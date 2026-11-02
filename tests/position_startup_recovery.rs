//! A2c-R2: the deployed binary must resume a durable orphan position without
//! turning that management obligation into invented fill history or readiness.
//! Only the exchange is replaced by loopback HTTP. The positive predecessor is
//! produced through authenticated gRPC, then the same binary/DB is restarted.

use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{any::AnyPoolOptions, AnyPool, Row, TypeInfo, ValueRef};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{Arc, Mutex, Once},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod pb {
    tonic::include_proto!("execution_engine.v2");
}

const TOKEN: &str = "a2c-r2-loopback-rpc-token";
const SYMBOL: &str = "binance|futures|BTC|USDT|USDT";
const INTERVAL: Duration = Duration::from_secs(10);
const FIRST_ROUND: Duration = Duration::from_secs(8);
const STARTUP: Duration = Duration::from_secs(20);
static DRIVERS: Once = Once::new();

struct Engine {
    child: Child,
    log: PathBuf,
    grpc: u16,
    spawned_at: Instant,
}

impl Engine {
    fn assert_alive(&mut self) {
        assert_eq!(
            self.child.try_wait().expect("inspect owned engine"),
            None,
            "engine exited unexpectedly; see {}",
            self.log.display()
        );
    }

    fn stop(&mut self) {
        self.child.kill().expect("kill exact owned engine process");
        self.child.wait().expect("reap exact owned engine process");
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RequestSeen {
    credential: String,
    method: String,
    path: String,
}

struct Exchange {
    port: u16,
    qty: Arc<Mutex<String>>,
    seen: Arc<Mutex<Vec<RequestSeen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn key(credential: &str) -> String {
    format!("a2c-r2-local-key-{credential}")
}

async fn exchange(credentials: &[&str], qty: &str) -> Exchange {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind owned loopback exchange");
    let port = listener.local_addr().unwrap().port();
    let qty = Arc::new(Mutex::new(qty.to_string()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mappings = Arc::new(
        credentials
            .iter()
            .map(|credential| (key(credential), credential.to_string()))
            .collect::<Vec<_>>(),
    );
    let current = qty.clone();
    let requests = seen.clone();
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let current = current.clone();
            let requests = requests.clone();
            let mappings = mappings.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                    }
                    if bytes.len() > 16384 {
                        return;
                    }
                }
                let text = String::from_utf8_lossy(&bytes);
                let mut first = text.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap().to_string();
                let path = first.next().unwrap().split('?').next().unwrap().to_string();
                let supplied_key = text.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("x-mbx-apikey")
                        .then(|| value.trim())
                });
                let credential = supplied_key
                    .and_then(|supplied| mappings.iter().find(|(value, _)| value == supplied))
                    .map(|(_, credential)| credential.as_str())
                    .unwrap_or("public-or-unrecognized")
                    .to_string();
                // Do not retain signed query strings, tokens or key values.
                requests.lock().unwrap().push(RequestSeen {
                    credential,
                    method: method.clone(),
                    path: path.clone(),
                });
                let (status, body) = match (method.as_str(), path.as_str()) {
                    ("GET", "/fapi/v1/exchangeInfo") => (
                        200,
                        json!({
                            "rateLimits": [{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000}],
                            "symbols": []
                        }),
                    ),
                    ("GET", "/fapi/v1/openOrders" | "/fapi/v1/openAlgoOrders") => (200, json!([])),
                    ("GET", "/fapi/v2/positionRisk") => (
                        200,
                        json!([{
                            "symbol":"BTCUSDT", "positionSide":"BOTH",
                            "positionAmt":current.lock().unwrap().clone(),
                            "entryPrice":"100", "markPrice":"100",
                            "unRealizedProfit":"0", "leverage":"1"
                        }]),
                    ),
                    ("GET", "/fapi/v2/balance") => (
                        200,
                        json!([{
                            "asset":"USDT", "balance":"10000", "availableBalance":"9900",
                            "crossWalletBalance":"10000"
                        }]),
                    ),
                    _ => (
                        400,
                        json!({"code":-1000,"msg":"unexpected fixture request"}),
                    ),
                };
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    Exchange {
        port,
        qty,
        seen,
        task,
    }
}

fn directory(label: &str) -> PathBuf {
    let base = std::env::var_os("POSITION_STARTUP_RECOVERY_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("position-startup-{label}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    eprintln!("position startup evidence: {}", dir.display());
    dir
}

async fn start(dir: &Path, phase: &str, db: &Path, rest: u16, keys: &[&str]) -> Engine {
    start_with_env(dir, phase, db, rest, keys, &[]).await
}

async fn start_with_env(
    dir: &Path,
    phase: &str,
    db: &Path,
    rest: u16,
    keys: &[&str],
    extra_env: &[(String, String)],
) -> Engine {
    if !db.exists() {
        std::fs::File::create(db).expect("create empty owned SQLite database");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = listener.local_addr().unwrap().port();
    drop(listener);
    let log_path = dir.join(format!("{phase}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let mut binary = std::fs::File::open(env!("CARGO_BIN_EXE_rustcore")).unwrap();
    let mut digest = Sha256::new();
    let mut chunk = [0u8; 65536];
    loop {
        let count = std::io::Read::read(&mut binary, &mut chunk).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&chunk[..count]);
    }
    std::fs::write(
        dir.join(format!("{phase}.binary.json")),
        json!({
            "path": env!("CARGO_BIN_EXE_rustcore"),
            "sha256": hex::encode(digest.finalize()),
            "rest": format!("http://127.0.0.1:{rest}"),
            "ws": format!("ws://127.0.0.1:{rest}")
        })
        .to_string(),
    )
    .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustcore"));
    command
        .env_clear()
        .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc}"))
        .env("EXECUTION_TOKEN", TOKEN)
        // The established custom-loopback fixture must pass the production
        // gateway. No venue override, real key or external URL is inherited.
        .env("ENGINE_LIVE_ENABLED", "1")
        .env(
            "BINANCE_FUTURES_BASE_URL",
            format!("http://127.0.0.1:{rest}"),
        )
        .env(
            "BINANCE_FUTURES_WS_BASE_URL",
            format!("ws://127.0.0.1:{rest}"),
        )
        .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
        .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
        .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
        .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
        .env("BINANCE_HTTP_TIMEOUT_MS", "1500")
        .env("RECONCILE_INTERVAL_MS", INTERVAL.as_millis().to_string())
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    for credential in keys {
        let locator = format!("H{}", hex::encode_upper(credential.as_bytes()));
        command
            .env(format!("BINANCE_API_KEY_{locator}"), key(credential))
            .env(
                format!("BINANCE_API_SECRET_{locator}"),
                "a2c-r2-local-secret",
            );
    }
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let spawned_at = Instant::now();
    let mut engine = Engine {
        child: command.spawn().unwrap(),
        log: log_path,
        grpc,
        spawned_at,
    };
    let deadline = Instant::now() + STARTUP;
    loop {
        engine.assert_alive();
        if std::fs::read_to_string(&engine.log)
            .unwrap()
            .contains("rustcore starting")
        {
            return engine;
        }
        assert!(
            Instant::now() < deadline,
            "startup did not serve; see {}",
            engine.log.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn channel(engine: &Engine) -> tonic::transport::Channel {
    let deadline = Instant::now() + STARTUP;
    loop {
        if let Ok(channel) =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", engine.grpc))
                .unwrap()
                .connect_timeout(Duration::from_secs(1))
                .connect()
                .await
        {
            return channel;
        }
        assert!(
            Instant::now() < deadline,
            "connect owned production gRPC server; see {}",
            engine.log.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn authenticated_readiness(engine: &Engine, credential: &str) {
    let mut client = tonic::client::Grpc::new(channel(engine).await);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(pb::GetPositionStatesRequest {
        credential_id: credential.to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    tokio::time::timeout(Duration::from_secs(10), async {
        let _: tonic::Response<pb::GetPositionStatesResponse> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/GetPositionStates",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .expect("authenticated store-backed startup readiness");
    })
    .await
    .expect("bounded store-backed readiness response");
}

async fn reconcile(
    engine: &Engine,
    credential: &str,
    token: Option<&str>,
) -> Result<pb::ReconcileNowResponse, tonic::Status> {
    let mut client = tonic::client::Grpc::new(channel(engine).await);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(pb::ReconcileNowRequest {
        credential_id: credential.to_string(),
        reason: "A2c R2 orphan predecessor".into(),
        schema_version: 1,
    });
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        let response: tonic::Response<pb::ReconcileNowResponse> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/ReconcileNow",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await?;
        Ok(response.into_inner())
    })
    .await
    .expect("bounded production ReconcileNow response")
}

async fn pool(db: &Path) -> AnyPool {
    DRIVERS.call_once(sqlx::any::install_default_drivers);
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap()
}

async fn position(pool: &AnyPool, credential: &str) -> Option<(String, i64)> {
    // Any/SQLite can infer a 32-bit type for computed integer expressions.
    // Decode the persisted millisecond timestamp losslessly through text.
    sqlx::query("SELECT qty, CAST(COALESCE(last_reconciled_ms,0) AS TEXT) AS ts FROM position_state WHERE credential_id=? AND state <> 'closed'")
        .bind(credential).fetch_optional(pool).await.unwrap()
        .map(|row| (row.get("qty"), row.get::<String, _>("ts").parse().unwrap()))
}

async fn observe_qty(
    pool: &AnyPool,
    engine: &mut Engine,
    credential: &str,
    expected: &str,
    duration: Duration,
) -> Option<(String, i64)> {
    let deadline = Instant::now() + duration;
    loop {
        engine.assert_alive();
        let actual = position(pool, credential).await;
        if actual.as_ref().is_some_and(|(qty, _)| qty == expected) || Instant::now() >= deadline {
            return actual;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn assert_no_history(pool: &AnyPool) {
    for table in [
        "engine_orders",
        "engine_fills",
        "fill_recovery_scopes",
        "fill_replay_cursors",
    ] {
        let count: i64 = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap()
            .get("n");
        assert_eq!(count, 0, "management must not invent {table}");
    }
    let ready: i64 = sqlx::query("SELECT COUNT(*) AS n FROM fill_stream_state WHERE ready <> 0")
        .fetch_one(pool)
        .await
        .unwrap()
        .get("n");
    assert_eq!(ready, 0, "management must not manufacture stream readiness");
}

async fn assert_no_stream_state(pool: &AnyPool, credential: &str) {
    let count: i64 =
        sqlx::query("SELECT COUNT(*) AS n FROM fill_stream_state WHERE credential_id=?")
            .bind(credential)
            .fetch_one(pool)
            .await
            .unwrap()
            .get("n");
    assert_eq!(
        count, 0,
        "successful position management must not invent any stream state"
    );
}

fn assert_only_management_requests(exchange: &Exchange, allowed: &[&str]) {
    for request in exchange.seen.lock().unwrap().iter() {
        assert_eq!(
            request.method, "GET",
            "management may not submit/cancel: {request:?}"
        );
        assert!(
            matches!(
                request.path.as_str(),
                "/fapi/v1/exchangeInfo"
                    | "/fapi/v1/openOrders"
                    | "/fapi/v1/openAlgoOrders"
                    | "/fapi/v2/positionRisk"
                    | "/fapi/v2/balance"
            ),
            "unexpected management traffic: {request:?}"
        );
        if request.path != "/fapi/v1/exchangeInfo" {
            assert!(
                allowed.contains(&request.credential.as_str()),
                "excluded credential sent exchange traffic: {request:?}"
            );
        }
    }
}

async fn seed_position(pool: &AnyPool, credential: &str, state: &str, symbol: &str) {
    let position_id = format!("fixture-{credential}");
    sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,qty,entry_price,state,active_key,entry_epoch,opened_at_ms,updated_at_ms) VALUES (?,?,?,?,'long','1','100',?,?,1,1,1)")
        .bind(&position_id).bind(credential).bind(format!("bot-{credential}"))
        .bind(symbol).bind(state).bind((state != "closed").then_some(position_id.clone()))
        .execute(pool).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2c_r2_orphan_position_recovers_through_production_binary_restart() {
    let credential = "a2c-r2-orphan";
    let dir = directory("orphan");
    let db = dir.join("state.db");
    let exchange = exchange(&[credential], "1").await;
    let mut first = start(&dir, "before", &db, exchange.port, &[credential]).await;
    authenticated_readiness(&first, credential).await;
    let pool = pool(&db).await;
    assert_eq!(
        position(&pool, credential).await,
        None,
        "predecessor must begin with no local position"
    );
    assert_no_history(&pool).await;
    assert_eq!(
        reconcile(&first, credential, None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        exchange.seen.lock().unwrap().len(),
        0,
        "rejected RPC must start no management"
    );
    let reply = reconcile(&first, credential, Some(TOKEN)).await.unwrap();
    assert!(reply.accepted);
    let initial = observe_qty(&pool, &mut first, credential, "1", FIRST_ROUND).await;
    assert_eq!(
        initial.as_ref().map(|(qty, _)| qty.as_str()),
        Some("1"),
        "authenticated RPC must persist the exchange-created orphan"
    );
    assert_no_history(&pool).await;
    assert_no_stream_state(&pool, credential).await;
    std::fs::write(
        dir.join("predecessor.json"),
        json!({
            "authenticated_reconcile_accepted": reply.accepted,
            "persisted_position": initial.as_ref(),
            "four_history_tables_asserted_empty": true
        })
        .to_string(),
    )
    .unwrap();
    first.stop();
    exchange.seen.lock().unwrap().clear();
    *exchange.qty.lock().unwrap() = "2".into();

    // After this spawn there are no incoming RPCs, including readiness probes.
    let mut restarted = start(&dir, "restarted", &db, exchange.port, &[credential]).await;
    let before_binary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("before.binary.json")).unwrap())
            .unwrap();
    let after_binary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("restarted.binary.json")).unwrap())
            .unwrap();
    assert_eq!(
        before_binary["sha256"], after_binary["sha256"],
        "restart must use the same production binary bytes"
    );
    let first_budget = FIRST_ROUND.saturating_sub(restarted.spawned_at.elapsed());
    let recovered = observe_qty(&pool, &mut restarted, credential, "2", first_budget).await;
    let first_elapsed = restarted.spawned_at.elapsed();
    std::fs::write(
        dir.join("restart-first-observation.json"),
        json!({
            "persisted_position": recovered.as_ref(),
            "elapsed_since_spawn_ms": first_elapsed.as_millis(),
            "deadline_ms": FIRST_ROUND.as_millis(),
            "periodic_interval_ms": INTERVAL.as_millis()
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(
        recovered.as_ref().map(|(qty, _)| qty.as_str()),
        Some("2"),
        "A2c-R2 startup must resume the persisted orphan before a periodic-only first tick"
    );
    assert!(first_elapsed <= FIRST_ROUND, "first management observation must precede the first periodic tick: elapsed={first_elapsed:?}");
    assert!(
        recovered.as_ref().unwrap().1 > initial.unwrap().1,
        "restart must persist fresh position truth"
    );
    assert_no_history(&pool).await;
    assert_no_stream_state(&pool, credential).await;
    *exchange.qty.lock().unwrap() = "3".into();
    let periodic = observe_qty(
        &pool,
        &mut restarted,
        credential,
        "3",
        INTERVAL * 2 + Duration::from_secs(5),
    )
    .await;
    std::fs::write(
        dir.join("restart-periodic-observation.json"),
        json!({
            "persisted_position": periodic.as_ref(),
            "elapsed_since_spawn_ms": restarted.spawned_at.elapsed().as_millis()
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(
        periodic.as_ref().map(|(qty, _)| qty.as_str()),
        Some("3"),
        "A2c-R2 recovered manager must continue on its next periodic round"
    );
    assert!(periodic.unwrap().1 > recovered.unwrap().1);
    assert_no_history(&pool).await;
    assert_no_stream_state(&pool, credential).await;
    assert_only_management_requests(&exchange, &[credential]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2c_r2_excluded_durable_positions_start_no_exchange_management() {
    let credentials = [
        "a2c-r2-closed",
        "paper",
        "paper:00000000000000000000000000000001",
        "paper:bad",
        "a2c-r2-spot",
        "a2c-r2-inverse",
    ];
    let dir = directory("excluded");
    let db = dir.join("state.db");
    let exchange = exchange(&credentials, "2").await;
    let mut bootstrap = start(&dir, "empty", &db, exchange.port, &credentials).await;
    authenticated_readiness(&bootstrap, credentials[0]).await;
    let pool = pool(&db).await;
    bootstrap.stop();
    for credential in &credentials[..4] {
        seed_position(
            &pool,
            credential,
            if *credential == credentials[0] {
                "closed"
            } else {
                "open"
            },
            SYMBOL,
        )
        .await;
    }
    seed_position(&pool, credentials[4], "open", "binance|spot|BTC|USDT|USDT").await;
    seed_position(&pool, credentials[5], "open", "binance|futures|BTC|USD|BTC").await;
    let mut restarted = start(&dir, "excluded", &db, exchange.port, &credentials).await;
    // Cover a full periodic interval: a wrongly delayed manager must also be visible.
    let deadline = Instant::now() + INTERVAL + Duration::from_secs(2);
    loop {
        restarted.assert_alive();
        assert_eq!(
            exchange.seen.lock().unwrap().clone(),
            Vec::<RequestSeen>::new(),
            "excluded positions must never admit exchange management"
        );
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_no_history(&pool).await;
    let states: i64 = sqlx::query("SELECT COUNT(*) AS n FROM fill_stream_state")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("n");
    assert_eq!(
        states, 0,
        "excluded simulation/identity rows must not enter startup credential recovery"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2c_r2_missing_key_failure_does_not_block_another_durable_position() {
    let missing = "a2c-r2-0-missing";
    let half_key = "a2c-r2-1-half-key";
    let invalid = "a2c-r2-2-invalid-venue";
    let eligible = "a2c-r2-9-eligible";
    let dir = directory("missing-key");
    let db = dir.join("state.db");
    let exchange = exchange(&[eligible, half_key, invalid], "2").await;
    let mut bootstrap = start(&dir, "empty", &db, exchange.port, &[eligible]).await;
    authenticated_readiness(&bootstrap, eligible).await;
    let pool = pool(&db).await;
    bootstrap.stop();
    seed_position(&pool, missing, "open", SYMBOL).await;
    seed_position(&pool, half_key, "open", SYMBOL).await;
    seed_position(&pool, invalid, "open", SYMBOL).await;
    seed_position(&pool, eligible, "open", SYMBOL).await;
    for credential in [missing, half_key, invalid] {
        sqlx::query("INSERT INTO fill_stream_state (credential_id,generation,ready,phase,updated_at_ms) VALUES (?,41,1,'ready',1)")
            .bind(credential).execute(&pool).await.unwrap();
    }
    // Half a keypair returns Ok(None); only the invalid credential's venue
    // triggers the real Err path. Process-wide transport stays valid/loopback.
    let extra_env = [
        (
            format!("BINANCE_API_KEY_H{}", hex::encode_upper(half_key)),
            key(half_key),
        ),
        (
            format!("BINANCE_VENUE_H{}", hex::encode_upper(invalid)),
            "invalid-fixture-venue".to_string(),
        ),
    ];
    let mut restarted = start_with_env(
        &dir,
        "mixed",
        &db,
        exchange.port,
        &[eligible, invalid],
        &extra_env,
    )
    .await;
    let actual = observe_qty(&pool, &mut restarted, eligible, "2", FIRST_ROUND).await;
    assert_eq!(
        actual.as_ref().map(|(qty, _)| qty.as_str()),
        Some("2"),
        "a missing key must not block startup management of another credential"
    );
    for (credential, expected_reason) in [
        (missing, "missing Binance key pair"),
        (half_key, "missing Binance key pair"),
        (invalid, "invalid Binance credential configuration"),
    ] {
        let failure = sqlx::query(
        "SELECT ready, phase, failure_reason, generation FROM fill_stream_state WHERE credential_id=?",
    )
    .bind(credential)
    .fetch_optional(&pool)
    .await
    .unwrap()
    .map(|row| {
        (
            row.get::<i64, _>("ready"),
            row.get::<String, _>("phase"),
            if row
                .try_get_raw("failure_reason")
                .unwrap()
                .type_info()
                .name()
                .eq_ignore_ascii_case("NULL")
            {
                None
            } else {
                Some(row.get::<String, _>("failure_reason"))
            },
            row.get::<i64, _>("generation"),
        )
    });
        assert_eq!(
            failure
                .as_ref()
                .map(|(ready, phase, _, _)| (*ready, phase.as_str())),
            Some((0, "startup_position_credentials")),
            "missing or invalid key must invalidate stale readiness: {credential}"
        );
        assert_eq!(failure.as_ref().unwrap().2.as_deref(), Some(expected_reason), "the persisted reason must distinguish missing keys from the real configuration Err branch: {credential}");
        assert!(
            failure.unwrap().3 > 41,
            "credential failure must invalidate the stale recovery generation: {credential}"
        );
        assert_eq!(position(&pool, credential).await.unwrap().0, "1");
    }
    assert_no_history(&pool).await;
    assert_no_stream_state(&pool, eligible).await;
    assert_only_management_requests(&exchange, &[eligible]);
}
