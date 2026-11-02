//! Budget discovery through the actual authenticated v2 RPC and HTTP transport.
//! No exchange endpoint is contacted: the child receives only loopback URLs.
use serde_json::json;
use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod pb {
    tonic::include_proto!("execution_engine.v2");
}
mod next {
    tonic::include_proto!("execution_engine.vnext");
}
struct Engine(std::process::Child);
impl Engine {
    fn require_running(&mut self, log_path: &Path) -> Result<(), String> {
        match self.0.try_wait() {
            Ok(None) => Ok(()),
            Ok(Some(status)) => Err(format!(
                "owned rustcore pid={} exited during startup ({status}); see {}",
                self.0.id(),
                log_path.display()
            )),
            Err(error) => Err(format!("inspect owned rustcore failed: {error}")),
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn fixture_token() -> String {
    format!("budget-fixture-{}", ulid::Ulid::new())
}

fn logged_grpc_addr(log: &str) -> Result<Option<SocketAddr>, String> {
    for line in log
        .lines()
        .filter(|line| line.contains("rustcore starting"))
    {
        if let Some(value) = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("grpc_addr="))
        {
            let addr: SocketAddr = value
                .parse()
                .map_err(|error| format!("invalid owned listener address: {error}"))?;
            if !addr.ip().is_loopback() || addr.port() == 0 {
                return Err("owned listener must have a bound loopback address".into());
            }
            return Ok(Some(addr));
        }
    }
    Ok(None)
}

async fn authenticated_readiness(
    channel: tonic::transport::Channel,
    token: &str,
) -> Result<(), tonic::Status> {
    let mut readiness = tonic::client::Grpc::new(channel);
    readiness
        .ready()
        .await
        .map_err(|error| tonic::Status::unknown(format!("readiness transport: {error}")))?;
    let mut request = tonic::Request::new(pb::GetPositionStatesRequest {
        credential_id: "budget.wire".into(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let _: tonic::Response<pb::GetPositionStatesResponse> = readiness
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/GetPositionStates",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await?;
    Ok(())
}

async fn owned_channel(
    engine: &mut Engine,
    log_path: &Path,
    token: &str,
    deadline: Instant,
) -> Result<tonic::transport::Channel, String> {
    let startup = async {
        let addr = loop {
            let log = std::fs::read_to_string(log_path)
                .map_err(|error| format!("read owned child log: {error}"))?;
            if let Some(addr) = logged_grpc_addr(&log)? {
                break addr;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let channel = loop {
            match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
            {
                Ok(channel) => break channel,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        // Only this child's authenticated, store-backed RPC proves readiness.
        authenticated_readiness(channel.clone(), token)
            .await
            .map_err(|error| format!("owned child readiness RPC: {error}"))?;
        eprintln!("owned budget listener: {addr}; log={}", log_path.display());
        Ok(channel)
    };
    tokio::pin!(startup);
    loop {
        // Watch the child while waiting for its log, connecting, and polling
        // readiness; an available foreign listener must never mask its exit.
        engine.require_running(log_path)?;
        tokio::select! {
            result = &mut startup => {
                engine.require_running(log_path)?;
                return result;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                engine.require_running(log_path)?;
                return Err(format!("owned child startup exceeded the original 8s deadline; see {}", log_path.display()));
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}
#[derive(Clone, Copy, PartialEq)]
enum Metadata {
    Valid,
    Invalid,
    RateLimited,
    Banned,
    Timeout,
    SlowTransport,
    InitialCooldown,
    DelayedBan,
    RetryBudget,
}
#[derive(Clone, Copy, PartialEq)]
enum Entry {
    V2,
    Scheduled,
    Startup,
}

async fn exercise(metadata: Metadata, explicit_budget: Option<u64>, entry: Entry) {
    let token = fixture_token();
    let dir = std::env::temp_dir().join(format!("budget-rpc-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("engine.db");
    std::fs::File::create(&db).unwrap();
    if entry == Entry::Startup {
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO fill_recovery_scopes (credential_id,symbol,registered_at_ms,updated_at_ms,scope_known,scope_proved_at_ms) VALUES ('budget.wire','BTCUSDT',1,1,1,1)")
            .execute(&pool).await.unwrap();
        pool.close().await;
    }
    let log_path = dir.join("engine.log");
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let metadata_times = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = http.local_addr().unwrap().port();
    let calls = seen.clone();
    let info_times = metadata_times.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = http.accept().await {
            let calls = calls.clone();
            let info_times = info_times.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                    }
                    assert!(bytes.len() < 16384);
                }
                let text = String::from_utf8_lossy(&bytes);
                let first = text.lines().next().unwrap();
                let mut parts = first.split_whitespace();
                let method = parts.next().unwrap();
                let url =
                    url::Url::parse(&format!("http://localhost{}", parts.next().unwrap())).unwrap();
                let path = url.path();
                assert!(
                    method == "GET" || (method == "POST" && path == "/fapi/v1/listenKey"),
                    "no trading mutation permitted"
                );
                calls.lock().unwrap().push(path.to_string());
                let info_number = if path == "/fapi/v1/exchangeInfo" {
                    let mut times = info_times.lock().unwrap();
                    times.push(Instant::now());
                    times.len()
                } else {
                    0
                };
                let mut status = 200;
                let mut weight = 1;
                let body = match path {
                    "/fapi/v1/listenKey" => json!({"listenKey":"budget-fake-listen-key"}),
                    "/fapi/v1/exchangeInfo" => {
                        if metadata == Metadata::DelayedBan {
                            tokio::time::sleep(Duration::from_millis(2800)).await;
                        }
                        if matches!(metadata, Metadata::Timeout | Metadata::SlowTransport) {
                            tokio::time::sleep(Duration::from_secs(20)).await;
                            return;
                        }
                        status = match metadata { Metadata::RateLimited => 429, Metadata::Banned | Metadata::DelayedBan => 418, _ => 200 };
                        // Even an error body shaped like valid metadata cannot raise the budget.
                        json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,
                            "limit":if metadata == Metadata::Invalid || (metadata == Metadata::RetryBudget && info_number == 1) {0} else {6000}}]})
                    }
                    "/fapi/v1/openOrders" => {
                        weight = if matches!(metadata, Metadata::Timeout | Metadata::SlowTransport | Metadata::RetryBudget) { 1 } else { 3196 };
                        json!([])
                    }
                    "/fapi/v1/openAlgoOrders" | "/fapi/v2/positionRisk" => json!([]),
                    "/fapi/v2/balance" => json!([{"asset":"USDT","balance":"10000","availableBalance":"10000","crossWalletBalance":"10000"}]),
                    _ => panic!("unexpected path {path}"),
                }.to_string();
                let response = format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nX-MBX-USED-WEIGHT-1M: {weight}\r\nRetry-After: 120\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"));
    command
        .env_clear()
        .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
        .env("RUSTCORE_GRPC_ADDR", "127.0.0.1:0")
        .env("EXECUTION_TOKEN", &token)
        .env("ENGINE_LIVE_ENABLED", "1")
        .env(
            "BINANCE_FUTURES_BASE_URL",
            format!("http://127.0.0.1:{port}"),
        )
        .env("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:1")
        .env(
            "BINANCE_API_KEY_H6275646765742E77697265",
            "budget-fixture-key",
        )
        .env(
            "BINANCE_API_SECRET_H6275646765742E77697265",
            "budget-fixture-secret",
        )
        .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
        .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
        .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
        .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
        .env(
            "RECONCILE_INTERVAL_MS",
            if metadata == Metadata::RetryBudget {
                "10000"
            } else {
                "30000"
            },
        )
        .env(
            "BINANCE_HTTP_TIMEOUT_MS",
            if matches!(metadata, Metadata::SlowTransport | Metadata::DelayedBan) {
                "10000"
            } else {
                "800"
            },
        )
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    if let Some(limit) = explicit_budget {
        command.env("RUSTCORE_BINANCE_WEIGHT_BUDGET_1M", limit.to_string());
    }
    let mut engine = Engine(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(8);
    let channel = owned_channel(&mut engine, &log_path, &token, deadline)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    if entry != Entry::Startup {
        assert!(
            seen.lock().unwrap().is_empty(),
            "empty startup has no discovered exchange scope"
        );
    } else {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !seen
            .lock()
            .unwrap()
            .iter()
            .any(|p| p == "/fapi/v1/listenKey")
        {
            assert!(
                Instant::now() < deadline,
                "startup must reach stream creation"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            seen.lock().unwrap().first().map(String::as_str),
            Some("/fapi/v1/exchangeInfo"),
            "startup discovers budget before other REST traffic"
        );
    }
    if entry == Entry::Scheduled {
        let mut client = tonic::client::Grpc::new(channel.clone());
        client.ready().await.unwrap();
        let mut request = tonic::Request::new(next::ReconcileNowRequest {
            credential_id: "budget.wire".into(),
            reason: "schedule budget fixture".into(),
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let response: tonic::Response<next::ReconcileNowResponse> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.vnext.EngineExecution/ReconcileNow",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        assert!(response.into_inner().accepted);
        let terminal = if metadata == Metadata::Valid {
            "/fapi/v2/balance"
        } else {
            "/fapi/v1/openOrders"
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !seen.lock().unwrap().iter().any(|p| p == terminal) {
            assert!(
                Instant::now() < deadline,
                "scheduled scan must execute through production worker"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if metadata == Metadata::RetryBudget {
            // The one-shot manual scan has ended. Without another RPC or a
            // user stream, only the real periodic worker can discover again.
            // Ten-second scans leave the original default request budget
            // intact; one-second full-account scans would exhaust it here.
            let deadline = Instant::now() + Duration::from_secs(85);
            loop {
                let log = std::fs::read_to_string(&log_path).unwrap();
                if log.contains("outcome=\"observed\"") {
                    break;
                }
                assert!(Instant::now() < deadline, "periodic worker never rediscovered budget after the real 60s retry boundary; see {}", log_path.display());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let times = metadata_times.lock().unwrap();
            assert_eq!(times.len(), 2);
            assert!(
                times[1].duration_since(times[0]) >= Duration::from_millis(59_900),
                "sent retry slot must last the full minute"
            );
        }
    }
    let mut unlock = None;
    if matches!(metadata, Metadata::InitialCooldown | Metadata::DelayedBan) {
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        if metadata == Metadata::InitialCooldown {
            let now = chrono::Utc::now().timestamp_millis();
            sqlx::query("INSERT INTO binance_rate_limit_cooldowns (egress_scope,cooldown_until_ms,updated_at_ms) VALUES (?,?,?)")
                .bind(format!("binance-futures-rest:base-url:http://127.0.0.1:{port}"))
                .bind(now + 900).bind(now).execute(&pool).await.unwrap();
        } else {
            let mut writer = pool.acquire().await.unwrap();
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *writer)
                .await
                .unwrap();
            unlock = Some(tokio::spawn(async move {
                // The 418 arrives at 2.8s; durable publication cannot finish
                // until 4.4s. A timeout around the whole discovery drops it.
                tokio::time::sleep(Duration::from_millis(4400)).await;
                sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
            }));
        }
    }
    let normally_succeeds = explicit_budget.is_none()
        && matches!(
            metadata,
            Metadata::Valid | Metadata::Timeout | Metadata::SlowTransport | Metadata::RetryBudget
        );
    let mut rpc = tonic::client::Grpc::new(channel);
    for attempt in 0..2 {
        let should_succeed =
            normally_succeeds || (metadata == Metadata::InitialCooldown && attempt == 1);
        rpc.ready().await.unwrap();
        let mut request = tonic::Request::new(pb::ReconcileNowRequest {
            credential_id: "budget.wire".into(),
            schema_version: 1,
            reason: "budget fixture".into(),
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let response: Result<tonic::Response<pb::ReconcileNowResponse>, tonic::Status> =
            tokio::time::timeout(
                Duration::from_secs(6),
                rpc.unary(
                    request,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/execution_engine.v2.EngineExecution/ReconcileNow",
                    ),
                    tonic::codec::ProstCodec::default(),
                ),
            )
            .await
            .expect("bounded RPC");
        assert_eq!(
            response.is_ok(),
            should_succeed,
            "RPC verdict; see {}",
            log_path.display()
        );
        if let Ok(value) = response {
            let value = value.into_inner();
            assert!(
                value.completed && value.accepted && !value.has_diff && value.all_positions_zero
            );
        } else {
            assert_eq!(response.unwrap_err().code(), tonic::Code::Unavailable);
        }
        if metadata == Metadata::InitialCooldown && attempt == 0 {
            assert!(
                seen.lock().unwrap().is_empty(),
                "initial cooldown must dispatch nothing"
            );
            // Let the existing cooldown expire naturally; no state clearing.
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
    }
    if let Some(unlock) = unlock {
        unlock.await.unwrap();
    }
    let calls = seen.lock().unwrap().clone();
    let info_count = calls
        .iter()
        .filter(|p| p.as_str() == "/fapi/v1/exchangeInfo")
        .count();
    assert_eq!(
        info_count,
        if explicit_budget.is_some() {
            0
        } else if metadata == Metadata::RetryBudget {
            2
        } else {
            1
        },
        "discovery shared across real RPC clients"
    );
    let log = std::fs::read_to_string(&log_path).unwrap();
    if metadata == Metadata::DelayedBan {
        // SQLite may finish an already queued write even when its awaiting
        // future is dropped. A row found later alone cannot prove that the
        // HTTP transport gate stayed held until the write completed.
        let attempt = log
            .lines()
            .find(|line| {
                line.contains("event=\"http_attempt\"")
                    && line.contains("path=\"/fapi/v1/exchangeInfo\"")
            })
            .expect("discovery transport completion trace");
        let postprocess_ms: u64 = attempt
            .split_whitespace()
            .find_map(|field| field.strip_prefix("postprocess_ms="))
            .expect("discovery postprocess timing")
            .parse()
            .unwrap();
        assert!(
            postprocess_ms >= 1000,
            "discovery released transport before the delayed durable write: {attempt}"
        );
    }
    if matches!(metadata, Metadata::Valid | Metadata::InitialCooldown) && explicit_budget.is_none()
    {
        assert_eq!(
            calls.first().map(String::as_str),
            Some("/fapi/v1/exchangeInfo")
        );
        assert!(
            log.contains("budget_source=\"observed\"") && log.contains("effective_budget_1m=6000")
        );
        // vnext schedules a manual scan and starts the periodic owner's initial
        // scan. Both cold clients must share the single metadata attempt.
        let scans = if entry == Entry::Scheduled {
            4
        } else if metadata == Metadata::InitialCooldown {
            1
        } else {
            2
        };
        assert_eq!(
            calls
                .iter()
                .filter(|p| p.as_str() != "/fapi/v1/listenKey")
                .count(),
            1 + scans * 4,
            "one discovery plus expected truth scans"
        );
    }
    if matches!(
        metadata,
        Metadata::RateLimited | Metadata::Banned | Metadata::DelayedBan
    ) {
        assert_eq!(
            calls,
            vec!["/fapi/v1/exchangeInfo"],
            "published cooldown blocks all later network traffic"
        );
        assert!(!log.contains("outcome=\"observed\""));
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        let query = if matches!(metadata, Metadata::Banned | Metadata::DelayedBan) {
            "SELECT COUNT(*) FROM binance_ip_ban_cooldowns WHERE ban_until_ms > ?"
        } else {
            "SELECT COUNT(*) FROM binance_rate_limit_cooldowns WHERE cooldown_until_ms > ?"
        };
        let active: i64 = sqlx::query_scalar(query)
            .bind(chrono::Utc::now().timestamp_millis())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            active, 1,
            "cooldown is durably published before truth traffic"
        );
    }
    if metadata == Metadata::Invalid || explicit_budget.is_some() {
        assert!(!log.contains("outcome=\"observed\""));
        assert!(
            !calls.iter().any(|p| p == "/fapi/v1/openAlgoOrders"),
            "fallback hard park must remain enforced"
        );
    }
    eprintln!("budget RPC fixture: {} calls={calls:?}", dir.display());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_v2_discovers_budget_before_truth_and_reuses_it() {
    exercise(Metadata::Valid, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_budget_keeps_default_hard_park() {
    exercise(Metadata::Invalid, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_429_publishes_cooldown_before_truth() {
    exercise(Metadata::RateLimited, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_418_publishes_ban_without_retry() {
    exercise(Metadata::Banned, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_discovery_is_bounded_and_throttled() {
    exercise(Metadata::Timeout, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_deadline_cancels_slow_transport_and_throttles_next_attempt() {
    exercise(Metadata::SlowTransport, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cooldown_rejected_discovery_does_not_spend_the_retry_slot() {
    exercise(Metadata::InitialCooldown, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_ban_publication_survives_the_discovery_network_deadline() {
    exercise(Metadata::DelayedBan, None, Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_budget_remains_authoritative() {
    exercise(Metadata::Valid, Some(2400), Entry::V2).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_worker_discovers_budget_before_truth() {
    exercise(Metadata::Valid, None, Entry::Scheduled).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn periodic_worker_rediscovers_budget_without_another_rpc() {
    exercise(Metadata::RetryBudget, None, Entry::Scheduled).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_worker_invalid_metadata_cannot_raise_default_budget() {
    exercise(Metadata::Invalid, None, Entry::Scheduled).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_stream_discovers_budget_before_rest_recovery() {
    exercise(Metadata::Valid, None, Entry::Startup).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_child_exit_and_foreign_token_cannot_borrow_readiness() {
    let dir = std::env::temp_dir().join(format!("budget-owner-negative-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let spawn = |name: &str, addr: &str, token: &str| {
        let db = dir.join(format!("{name}.db"));
        std::fs::File::create(&db).unwrap();
        let log_path = dir.join(format!("{name}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"))
            .env_clear()
            .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
            .env("RUSTCORE_GRPC_ADDR", addr)
            .env("EXECUTION_TOKEN", token)
            .env("ENGINE_LIVE_ENABLED", "0")
            .env("BINANCE_FUTURES_BASE_URL", "http://127.0.0.1:1")
            .env("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:1")
            .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        (Engine(child), log_path)
    };
    let healthy_token = fixture_token();
    let foreign_token = fixture_token();
    let (mut healthy, healthy_log) = spawn("healthy", "127.0.0.1:0", &healthy_token);
    let channel = owned_channel(
        &mut healthy,
        &healthy_log,
        &healthy_token,
        Instant::now() + Duration::from_secs(8),
    )
    .await
    .unwrap();
    let wrong_owner = authenticated_readiness(channel.clone(), &foreign_token)
        .await
        .expect_err("a different fixture token must fail at the real authentication boundary");
    assert_eq!(wrong_owner.code(), tonic::Code::Unauthenticated);

    // Keep the healthy production listener alive while another real child
    // fails to bind it, reproducing the ownership precondition of the gate.
    let addr = logged_grpc_addr(&std::fs::read_to_string(&healthy_log).unwrap())
        .unwrap()
        .unwrap();
    let (mut failed, failed_log) = spawn("bind-failed", &addr.to_string(), &foreign_token);
    let error = owned_channel(
        &mut failed,
        &failed_log,
        &foreign_token,
        Instant::now() + Duration::from_secs(8),
    )
    .await
    .expect_err("an available foreign listener cannot hide the owned child's bind failure");
    assert!(
        error.contains("owned rustcore") && error.contains("exited"),
        "{error}"
    );
    assert!(!failed.0.try_wait().unwrap().unwrap().success());
    assert!(std::fs::read_to_string(&failed_log)
        .unwrap()
        .contains("reserve gRPC listener failed"));
    healthy.require_running(&healthy_log).unwrap();
    authenticated_readiness(channel, &healthy_token)
        .await
        .unwrap();
    eprintln!(
        "ownership negative control: foreign_token=Unauthenticated; failed_child={error}; healthy_child_still_ready=true"
    );
}
