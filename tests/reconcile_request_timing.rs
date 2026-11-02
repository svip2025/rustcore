//! Real authenticated v2 RPC, isolated SQLite and delayed loopback exchange.
//! Start with an empty database, then seed the cohort: unrelated startup
//! recovery must not explain either the measured requests or the RPC verdict.
use serde_json::{json, Value};
use sqlx::any::AnyPoolOptions;
use std::{
    path::PathBuf,
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
mod settlement_v1 {
    tonic::include_proto!("execution_engine.vnext");
}
const CRED: &str = "timing.wire";
const SYMBOL: &str = "binance|futures|BTC|USDT|USDT";
const TOKEN: &str = "timing-fake-rpc-token";

struct Engine(std::process::Child);
impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum QueryReply {
    Healthy,
    NotFound,
    Error,
    Timeout,
}

fn algo() -> Value {
    json!({"algoId":9001,"clientAlgoId":"timing-sl","symbol":"BTCUSDT","algoStatus":"NEW","orderType":"STOP_MARKET","updateTime":chrono::Utc::now().timestamp_millis()})
}

async fn upstream(mode: QueryReply, seen: Arc<Mutex<Vec<String>>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seen = seen.clone();
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
                let mut parts = text.lines().next().unwrap().split_whitespace();
                let method = parts.next().unwrap();
                let url =
                    url::Url::parse(&format!("http://127.0.0.1{}", parts.next().unwrap())).unwrap();
                let path = url.path();
                // Store only method/path. Signed query and authorization never leave this handler.
                seen.lock().unwrap().push(format!("{method} {path}"));
                assert_eq!(
                    method, "GET",
                    "this fixture must never submit or cancel orders"
                );
                let delay = if path == "/fapi/v1/algoOrder" {
                    240
                } else {
                    60
                };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let (status, body) = match path {
                    "/fapi/v1/exchangeInfo" => (
                        200,
                        json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000}]}),
                    ),
                    "/fapi/v1/openOrders" => (200, json!([])),
                    "/fapi/v1/openAlgoOrders" => (200, json!([algo()])),
                    "/fapi/v2/positionRisk" => (
                        200,
                        json!([{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1","entryPrice":"100","markPrice":"100","unRealizedProfit":"0","leverage":"1"}]),
                    ),
                    "/fapi/v2/balance" => (
                        200,
                        json!([{"asset":"USDT","balance":"10000","availableBalance":"9900","crossWalletBalance":"10000"}]),
                    ),
                    "/fapi/v1/algoOrder" => (200, algo()),
                    "/fapi/v1/order" if mode == QueryReply::NotFound => {
                        (400, json!({"code":-2013,"msg":"Order does not exist."}))
                    }
                    "/fapi/v1/order" if mode == QueryReply::Error => {
                        (500, json!({"code":-1000,"msg":"fixture unavailable"}))
                    }
                    "/fapi/v1/order" if mode == QueryReply::Timeout => {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        return;
                    }
                    "/fapi/v1/order" => {
                        let id = url
                            .query_pairs()
                            .find(|(k, _)| k == "orderId")
                            .unwrap()
                            .1
                            .parse::<i64>()
                            .unwrap();
                        (200, json!({"symbol":"BTCUSDT","orderId":id,"status":"NEW"}))
                    }
                    _ => panic!("unexpected loopback path {path}"),
                };
                let body = body.to_string();
                let response = format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    port
}

async fn seed(pool: &sqlx::AnyPool) {
    sqlx::query("INSERT INTO engine_credential_health (credential_id,updated_at_ms,reconcile_diff_open) VALUES (?,1,1)").bind(CRED).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,qty,entry_price,state,active_key,entry_epoch,opened_at_ms,updated_at_ms) VALUES ('timing-pos',?,'timing-bot',?,'long','1','100','protected','timing-active',1,1,1)")
        .bind(CRED).bind(SYMBOL).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO position_protection_plans (position_id,credential_id,bot_uid,exchange,market_kind,symbol_key,direction,stop_loss_price,take_profit_price,confirm_timeout_ms,generation,created_at_ms,updated_at_ms) VALUES ('timing-pos',?,'timing-bot','binance','futures',?,'long','90','',1500,1,1,1)")
        .bind(CRED).bind(SYMBOL).execute(pool).await.unwrap();
    // Two ordinary unknowns plus one active SL with an Algo identity. The SL
    // open-set truth is followed by a real point query in the recovery phase.
    for (id, role, status, exchange_id) in [
        ("first", "entry", "unknown", Some("8001")),
        ("second", "entry", "unknown", Some("8002")),
        ("sl", "protection_sl", "acked", None),
    ] {
        sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,order_role,status,exchange_order_id,position_id,created_at_ms,updated_at_ms) VALUES (?,?,'timing-bot',?,'binance','futures',?,'open_long','long','market','1',0,?,?,?,'timing-pos',1,1)")
            .bind(id).bind(CRED).bind(id).bind(SYMBOL).bind(role).bind(status).bind(exchange_id).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,trigger_price,status,verified_from_exchange,exchange_algo_id,client_algo_id,algo_status,created_at_ms,updated_at_ms) VALUES ('timing-protection','timing-pos',?,'sl',1,'sl','90','confirmed',1,'9001','timing-sl','NEW',1,1)")
        .bind(CRED).execute(pool).await.unwrap();
}

fn timing_lines(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| line.contains("reconcile timing"))
        .collect()
}
fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let key = format!("{key}=");
    line.split_whitespace()
        .find_map(|word| word.strip_prefix(&key))
        .unwrap_or_else(|| panic!("missing {key}: {line}"))
        .trim_matches('"')
}
fn millis(line: &str, key: &str) -> u64 {
    field(line, key).parse().unwrap()
}

async fn exercise(mode: QueryReply) {
    let base = std::env::var_os("RECONCILE_TIMING_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("reconcile-timing-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("fixture.db");
    let log_path = dir.join("engine.log");
    std::fs::File::create(&db).unwrap();
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let rest = upstream(mode, seen.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = listener.local_addr().unwrap().port();
    drop(listener);
    let locator = format!("H{}", hex::encode_upper(CRED));
    let log = std::fs::File::create(&log_path).unwrap();
    let _engine = Engine(
        std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"))
            .env_clear()
            .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
            .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc}"))
            .env("EXECUTION_TOKEN", TOKEN)
            .env(
                "BINANCE_FUTURES_BASE_URL",
                format!("http://127.0.0.1:{rest}"),
            )
            .env("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:1")
            .env(format!("BINANCE_API_KEY_{locator}"), "timing-fake-key")
            .env(
                format!("BINANCE_API_SECRET_{locator}"),
                "timing-fake-secret",
            )
            .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
            .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
            .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
            .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
            .env("BINANCE_HTTP_TIMEOUT_MS", "800")
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(6);
    let channel = loop {
        match tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => break channel,
            Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "engine startup; see {}",
                    log_path.display()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    // TCP admission precedes Store initialization. Only an authenticated,
    // store-backed readonly RPC proves readiness for fixture writes/timing.
    let mut readiness = tonic::client::Grpc::new(channel.clone());
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        readiness.ready().await.unwrap();
        let mut request = tonic::Request::new(pb::GetPositionStatesRequest {
            credential_id: CRED.into(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        let _: tonic::Response<pb::GetPositionStatesResponse> = readiness
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/GetPositionStates",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .expect("authenticated v2 startup readiness RPC");
    })
    .await
    .expect("store-backed RPC readiness within the original startup deadline");
    assert!(
        seen.lock().unwrap().is_empty(),
        "empty startup must not create background traffic"
    );
    seed(&pool).await;
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(pb::ReconcileNowRequest {
        credential_id: CRED.into(),
        reason: "private-reason-must-not-be-logged".into(),
        schema_version: 1,
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let started = Instant::now();
    let result: Result<tonic::Response<pb::ReconcileNowResponse>, tonic::Status> = client
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/ReconcileNow",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await;
    let elapsed = started.elapsed().as_millis() as u64;
    if let Err(error) = &result {
        eprintln!(
            "RPC error category: {}",
            error.message().split("http").next().unwrap()
        );
    }
    let requests = seen.lock().unwrap().clone();
    let query_count = requests
        .iter()
        .filter(|r| r.as_str() == "GET /fapi/v1/order")
        .count();
    let clean_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM engine_order_events WHERE event_type='reconcile_diff'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let diff_open: i64 = sqlx::query_scalar(
        "SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id=?",
    )
    .bind(CRED)
    .fetch_one(&pool)
    .await
    .unwrap();
    let unknowns: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM engine_orders WHERE status='unknown'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let summary = json!({"elapsed_ms":elapsed,"query_count":query_count,"clean_events":clean_events,"diff_open":diff_open,"unknowns":unknowns,"rpc_ok":result.is_ok(),"requests":requests});
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("reconcile fixture {}: {}", dir.display(), summary);
    if matches!(mode, QueryReply::Error | QueryReply::Timeout) {
        assert!(result.is_err(), "unavailable ordinary-order truth must fail the real RPC, not publish success: {result:?}; see {}", dir.display());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        assert_eq!(
            diff_open, 1,
            "failed truth must preserve the existing degradation"
        );
        assert_eq!(
            query_count, 1,
            "fail the incomplete round before accumulating per-order timeouts"
        );
        assert_eq!(
            unknowns, 2,
            "unknown orders must remain unknown on lookup failure"
        );
        assert_eq!(
            clean_events, 0,
            "failed truth must not publish a successful reconciliation"
        );
        assert!(
            !requests.iter().any(|r| r == "GET /fapi/v1/algoOrder"),
            "incomplete truth cannot advance into the successful protection recovery phase"
        );
    } else {
        let response = result.unwrap().into_inner();
        assert!(
            response.completed && !response.has_diff && !response.all_positions_zero,
            "{response:?}"
        );
        assert_eq!(query_count, 2);
        assert_eq!(clean_events, 1);
        assert_eq!(diff_open, 0);
        assert_eq!(
            unknowns,
            if mode == QueryReply::NotFound { 2 } else { 0 },
            "negative truth must preserve non-adjudicable orders"
        );
        let log = std::fs::read_to_string(&log_path).unwrap();
        let lines = timing_lines(&log);
        assert_eq!(
            lines.len(),
            10,
            "budget discovery plus seven HTTP attempts, one fullscan and one RPC; see {}",
            log_path.display()
        );
        let rpc = *lines
            .iter()
            .find(|line| field(line, "event") == "rpc")
            .unwrap();
        let trace = field(rpc, "trace_id");
        for line in &lines {
            assert_eq!(field(line, "trace_id"), trace);
            assert_eq!(field(line, "outcome"), "ok");
            for forbidden in [
                CRED,
                TOKEN,
                "timing-fake-key",
                "timing-fake-secret",
                "private-reason-must-not-be-logged",
                "signature=",
                "timestamp=",
                "http://",
                "clientAlgoId=",
            ] {
                assert!(
                    !line.contains(forbidden),
                    "unsafe diagnostic field: {forbidden}"
                );
            }
        }
        let http: Vec<_> = lines
            .iter()
            .filter(|line| field(line, "event") == "http_attempt")
            .collect();
        assert_eq!(http.len(), 8);
        assert!(
            http.iter().all(|line| millis(line, "http_ms") >= 50),
            "real upstream delay must appear as HTTP time"
        );
        let sl = http
            .iter()
            .find(|line| field(line, "path") == "/fapi/v1/algoOrder")
            .unwrap();
        assert!(millis(sl, "http_ms") >= 220);
        assert_eq!(field(sl, "request_class"), "event_truth");
        let scan = lines
            .iter()
            .find(|line| field(line, "event") == "fullscan")
            .unwrap();
        assert_eq!(millis(scan, "order_queries"), 2);
        assert_eq!(millis(scan, "local_orders"), 2);
        assert!(millis(rpc, "truth_ms") >= 330 && millis(rpc, "protection_ms") >= 220);
        assert!(
            millis(rpc, "elapsed_ms") <= elapsed + 20 && elapsed - millis(rpc, "elapsed_ms") < 150
        );
    }
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonflat_v2_rpc_correlates_fullscan_and_protection_http_timing() {
    exercise(QueryReply::Healthy).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_lookup_failure_cannot_publish_successful_reconcile() {
    exercise(QueryReply::Error).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_lookup_timeout_aborts_round_without_summing_all_inflight_timeouts() {
    exercise(QueryReply::Timeout).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authoritative_absence_completes_without_adjudicating_ineligible_orders() {
    exercise(QueryReply::NotFound).await;
}

// Separate production-process proof. The ample fixture budget isolates known
// response settlement; it does not claim to fix the default GetOrder budget.
async fn settlement_rpc(
    channel: tonic::transport::Channel,
    v2: bool,
    token: &str,
) -> Result<(), tonic::Status> {
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    if v2 {
        let mut request = tonic::Request::new(pb::ReconcileNowRequest {
            credential_id: CRED.into(),
            reason: "settlement-fixture".into(),
            schema_version: 1,
        });
        request.set_timeout(Duration::from_millis(400));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let result: Result<tonic::Response<pb::ReconcileNowResponse>, _> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/ReconcileNow",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        result.map(|_| ())
    } else {
        let mut request = tonic::Request::new(settlement_v1::GetOrderRequest {
            credential_id: CRED.into(),
            engine_order_id: "settlement-order".into(),
        });
        // This caller is actively cancelled after the known-response handoff.
        // A 400ms header instead lets GetOrder's legitimate 320ms refresh
        // fallback finish first. Its production 8s/9s caps remain unchanged.
        request.set_timeout(Duration::from_secs(10));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let result: Result<tonic::Response<settlement_v1::GetOrderResponse>, _> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.vnext.EngineExecution/GetOrder",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        result.map(|_| ())
    }
}

async fn settlement_start_engine(
    db: &std::path::Path,
    rest: u16,
    route: &str,
    log_path: &std::path::Path,
    fault_environment: bool,
) -> (Engine, tonic::transport::Channel) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = listener.local_addr().unwrap().port();
    drop(listener);
    let locator = format!("H{}", hex::encode_upper(CRED));
    let log = std::fs::File::create(log_path).unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"));
    command
        .env_clear()
        .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc}"))
        .env("EXECUTION_TOKEN", TOKEN)
        .env(
            "BINANCE_FUTURES_BASE_URL",
            format!("http://127.0.0.1:{rest}"),
        )
        .env("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:1")
        .env("BINANCE_EGRESS_ROUTE_ID", route)
        .env(format!("BINANCE_API_KEY_{locator}"), "timing-fake-key")
        .env(
            format!("BINANCE_API_SECRET_{locator}"),
            "timing-fake-secret",
        )
        .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
        .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
        .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
        .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
        .env("ENGINE_LIVE_ENABLED", "1")
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    if fault_environment {
        command
            .env("L4_FAULT_INJECTION_ENABLED", "1")
            .env("FAULT_INJECT", "settlement_service_error")
            .env("L4_FAULT_MARKER_DIR", log_path.parent().unwrap());
    }
    let engine = Engine(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(6);
    let channel = loop {
        if let Ok(channel) =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc}"))
                .unwrap()
                .connect()
                .await
        {
            break channel;
        }
        assert!(
            Instant::now() < deadline,
            "binary startup; see {}",
            log_path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut readiness = tonic::client::Grpc::new(channel.clone());
    readiness.ready().await.unwrap();
    let mut request = tonic::Request::new(pb::GetPositionStatesRequest {
        credential_id: CRED.into(),
        ..Default::default()
    });
    request.set_timeout(Duration::from_secs(5));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let _: tonic::Response<pb::GetPositionStatesResponse> = readiness
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/GetPositionStates",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .unwrap();
    (engine, channel)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SettlementExit {
    Signal(i32),
    ServiceError,
    DisabledServiceError,
    KillBeforeCommit,
}

#[cfg(unix)]
async fn settlement_binary_case(v2: bool, exit_case: SettlementExit) {
    let signal = match exit_case {
        SettlementExit::Signal(signal) => signal,
        _ => libc::SIGTERM,
    };
    let crash = exit_case == SettlementExit::KillBeforeCommit;
    let base = std::env::var_os("RECONCILE_TIMING_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("settlement-binary-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("fixture.db");
    std::fs::File::create(&db).unwrap();
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let locked_tx = Arc::new(Mutex::new(Some(locked_tx)));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let upstream_pool = pool.clone();
    let requests = seen.clone();
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        let mut shutdown = stopped.clone();
        loop {
            tokio::select! {
                _=shutdown.changed()=>break,
                Some(result)=tasks.join_next(), if !tasks.is_empty()=>{result.unwrap();},
                connection=listener.accept()=>{
                    let (mut socket,_)=connection.unwrap();let pool=upstream_pool.clone();let seen=requests.clone();let sender=locked_tx.clone();let mut stopped=stopped.clone();
                    tasks.spawn(async move {
                        let mut bytes=Vec::new();while !bytes.windows(4).any(|w|w==b"\r\n\r\n") {let mut chunk=[0;1024];let n=socket.read(&mut chunk).await.unwrap();if n==0{return;}bytes.extend_from_slice(&chunk[..n]);assert!(bytes.len()<16_384);}
                        let text=String::from_utf8_lossy(&bytes);let target=text.lines().next().unwrap().split_whitespace().nth(1).unwrap();
                        let path=url::Url::parse(&format!("http://127.0.0.1{target}")).unwrap().path().to_string();seen.lock().unwrap().push(path.clone());
                        if path=="/fapi/v1/exchangeInfo" {
                            let body=r#"{"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":600000}]}"#;
                            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();return;
                        }
                        let sender=sender.lock().unwrap().take();
                        if let Some(sender)=sender {
                            // This is after the actual request: the pre-send DB
                            // guards passed. Only persistence sees this writer lock.
                            let mut held=pool.acquire().await.unwrap();sqlx::query("BEGIN IMMEDIATE").execute(&mut *held).await.unwrap();
                            sender.send(held).unwrap();
                            let status=if v2 {429}else{418};
                            socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                            let _=stopped.changed().await;
                        } else if crash {
                            let body=r#"{"orderId":9001,"status":"NEW","executedQty":"0"}"#;
                            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                        } else {
                            panic!("unexpected exchange request after known cooldown: {path}");
                        }
                    });
                }
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    });
    let route = format!(
        "settlement-{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    let log_path = dir.join("engine.log");
    let fault_environment = matches!(
        exit_case,
        SettlementExit::ServiceError | SettlementExit::DisabledServiceError
    );
    let (mut engine, channel) =
        settlement_start_engine(&db, rest, &route, &log_path, fault_environment).await;
    sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,order_role,status,exchange_order_id,created_at_ms,updated_at_ms) VALUES ('settlement-order',?,'settlement-bot','settlement-idem','binance','futures',?,'open_long','long','limit','1',0,'entry','submitted','9001',1,1)")
        .bind(CRED).bind(SYMBOL).execute(&pool).await.unwrap();
    assert_eq!(
        settlement_rpc(channel.clone(), v2, "wrong-token")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert!(seen.lock().unwrap().is_empty());
    assert!(!std::fs::read_to_string(&log_path)
        .unwrap()
        .contains("binance response safety settlement"));
    let call_started = Instant::now();
    let call = tokio::spawn(settlement_rpc(channel, v2, TOKEN));
    let mut held = tokio::time::timeout(Duration::from_secs(3), locked_rx)
        .await
        .unwrap()
        .unwrap();
    let header_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let log = std::fs::read_to_string(&log_path).unwrap();
        if log.lines().any(|line| {
            line.contains("binance response safety settlement") && line.contains("writing")
        }) {
            break;
        }
        assert!(
            Instant::now() < header_deadline,
            "application header handoff must precede cancellation; see {}",
            log_path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let header_writing_observed_ms = call_started.elapsed().as_millis();
    let rpc_error = if v2 {
        let error = tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(error.message().contains("ReconcileNow budget exhausted"));
        Some(format!("{:?}", error.code()))
    } else {
        // The response body is still held by upstream, and `held` still owns
        // BEGIN IMMEDIATE. Neither release nor GetOrder's refresh budget may
        // finish this call before we actually cancel its real tonic future.
        assert!(
            !call.is_finished(),
            "GetOrder finished before active cancel"
        );
        assert!(call_started.elapsed() < Duration::from_secs(8));
        call.abort();
        let cancelled = tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap_err();
        assert!(cancelled.is_cancelled(), "actual client task cancellation");
        None
    };
    let caller_terminal_ms = call_started.elapsed().as_millis();
    let (table, column) = if v2 {
        ("binance_rate_limit_cooldowns", "cooldown_until_ms")
    } else {
        ("binance_ip_ban_cooldowns", "ban_until_ms")
    };
    // A separate connection proves no commit while the external writer lock
    // remains held. SQL may already be in SQLite busy handling: this does not
    // claim that the owner has not dispatched the statement.
    let independent = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    let before_commit: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&independent)
        .await
        .unwrap();
    assert_eq!(before_commit, 0);
    if fault_environment {
        std::fs::write(dir.join("settlement_service_error.trigger"), b"test-only").unwrap();
        if exit_case == SettlementExit::ServiceError {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !std::fs::read_to_string(&log_path)
                .unwrap()
                .contains("synthetic settlement service error requested")
            {
                assert!(
                    Instant::now() < deadline,
                    "feature service error trigger must be consumed"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let log = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(
            log.contains("synthetic settlement service error requested"),
            exit_case == SettlementExit::ServiceError
        );
    }
    if exit_case != SettlementExit::ServiceError {
        assert_eq!(
            unsafe {
                libc::kill(
                    engine.0.id() as libc::pid_t,
                    if crash { libc::SIGKILL } else { signal },
                )
            },
            0
        );
    }
    if !crash {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            engine.0.try_wait().unwrap().is_none(),
            "runtime must stay alive while a known safety write is waiting"
        );
    } else {
        use std::os::unix::process::ExitStatusExt;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = engine.0.try_wait().unwrap() {
                assert_eq!(status.signal(), Some(9));
                break;
            }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    sqlx::query("ROLLBACK").execute(&mut *held).await.unwrap();
    drop(held);
    let shutdown_deadline = Instant::now() + Duration::from_millis(6500);
    let exit = loop {
        if let Some(exit) = engine.0.try_wait().unwrap() {
            break exit;
        }
        assert!(
            Instant::now() < shutdown_deadline,
            "bounded owner drain; see {}",
            log_path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        exit.success(),
        !crash && exit_case != SettlementExit::ServiceError
    );
    let until: Option<i64> = sqlx::query_scalar(&format!("SELECT {column} FROM {table}"))
        .fetch_optional(&independent)
        .await
        .unwrap();
    if crash {
        assert_eq!(
            until, None,
            "SIGKILL before commit has no durable guarantee"
        );
    } else {
        assert!(until.unwrap() > chrono::Utc::now().timestamp_millis());
    }
    let before = seen.lock().unwrap().len();
    let order_reads_before = seen
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.as_str() == "/fapi/v1/order")
        .count();
    let restart_log = dir.join("restart.log");
    let (mut restarted, channel) =
        settlement_start_engine(&db, rest, &route, &restart_log, false).await;
    let _ = settlement_rpc(channel, v2, TOKEN).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    if crash {
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .filter(|path| path.as_str() == "/fapi/v1/order")
                .count()
                > order_reads_before,
            "fresh process exposes the uncommitted safety window by actually sending"
        );
    } else {
        assert_eq!(
            seen.lock().unwrap().len(),
            before,
            "fresh process consumes durable cooldown before exchange transport"
        );
    }
    assert_eq!(
        unsafe { libc::kill(restarted.0.id() as libc::pid_t, signal) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(exit) = restarted.0.try_wait().unwrap() {
            assert!(exit.success());
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.send(true).unwrap();
    server.await.unwrap();
    let log = std::fs::read_to_string(&log_path).unwrap();
    if v2 {
        let writing = log
            .lines()
            .find(|line| {
                line.contains("binance response safety settlement") && line.contains("writing")
            })
            .unwrap();
        let durable = log
            .lines()
            .find(|line| {
                line.contains("binance response safety settlement") && line.contains("durable")
            })
            .unwrap();
        // K08 returns its explicit 90% request deadline before the transport's
        // timer. The request-owned HTTP waiter drops while the owner continues.
        let terminal = log
            .lines()
            .find(|line| {
                line.contains("event=\"rpc\"") && line.contains("outcome=\"returned_error\"")
            })
            .unwrap();
        assert_eq!(field(writing, "trace_id"), field(terminal, "trace_id"));
        assert!(log.lines().any(|line| {
            line.contains("event=\"http_attempt\"")
                && line.contains("stage=\"postprocess\"")
                && line.contains("outcome=\"dropped\"")
                && field(line, "trace_id") == field(writing, "trace_id")
                && field(line, "attempt_id") == field(writing, "attempt_id")
        }));
        assert_eq!(field(writing, "trace_id"), field(durable, "trace_id"));
        assert_eq!(field(writing, "attempt_id"), field(durable, "attempt_id"));
        assert_eq!(
            field(writing, "settlement_id"),
            field(durable, "settlement_id")
        );
    }
    assert_eq!(
        log.lines()
            .any(|line| line.contains("binance response safety settlement")
                && line.contains("durable")),
        !crash
    );
    for secret in ["timing-fake-key", "timing-fake-secret", "signature="] {
        assert!(!log.contains(secret));
    }
    let summary = json!({"rpc":if v2{"ReconcileNow-v2"}else{"GetOrder-v1"},"status":if v2{429}else{418},"exit_case":format!("{exit_case:?}"),"signal":if crash{9}else{signal},"rpc_error":rpc_error,"cancellation_kind":if v2{"server_deadline"}else{"active_client_future_abort"},"client_future_cancelled":!v2,"header_writing_observed_ms":header_writing_observed_ms,"caller_terminal_ms":caller_terminal_ms,"exit_success":exit.success(),"until_ms":until,"requests":seen.lock().unwrap().clone(),"restart_blocked_transport":!crash,"feature_build":cfg!(feature="l4-fault-injection"),"before_commit_rows":before_commit});
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("settlement production process evidence: {}", dir.display());
    pool.close().await;
    independent.close().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s04_s13_s14_v1_active_cancel_sigterm_drain_restart() {
    settlement_binary_case(false, SettlementExit::Signal(libc::SIGTERM)).await;
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s04_s13_s14_v2_server_budget_ctrl_c_drain_restart() {
    settlement_binary_case(true, SettlementExit::Signal(libc::SIGINT)).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s14_sigkill_before_commit_exposes_unpersisted_window() {
    settlement_binary_case(false, SettlementExit::KillBeforeCommit).await;
}

#[cfg(all(unix, feature = "l4-fault-injection"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s13_service_error_drains_then_exits_unsuccessfully() {
    settlement_binary_case(false, SettlementExit::ServiceError).await;
}

#[cfg(all(unix, not(feature = "l4-fault-injection")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s13_normal_build_ignores_fault_environment() {
    settlement_binary_case(false, SettlementExit::DisabledServiceError).await;
}
