//! Production-binary regression for WS fill protection attachment spawning a
//! second credential stream/reconciler. Only the exchange is replaced (loopback
//! HTTP/WS); no L4 shortcut, pre-verified SL, or internal service call is used.
//! Cold startup and a normal risk-rejected RPC must retain one periodic owner;
//! an actual fill must submit and confirm its SL without creating another owner.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sqlx::any::AnyPoolOptions;
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

mod pb {
    tonic::include_proto!("execution_engine.v2");
}

const CREDENTIAL: &str = "ownership.wire";
const BOT: &str = "bot_ownership";
const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";
const POSITION: &str = "pos_ownership";
const ORDER: i64 = 880101;
const ALGO: i64 = 990101;
const PROBE: &[u8] = b"after-real-protection";

#[derive(Default)]
struct Exchange {
    filled: AtomicBool,
    listen_keys: AtomicUsize,
    handshakes: AtomicUsize,
    active_sockets: AtomicUsize,
    original_reader_resumed: AtomicBool,
    algo_posts: AtomicUsize,
    algo_queries: AtomicUsize,
    algo_truth_reads: AtomicUsize,
    balance_reads_at: Mutex<Vec<Instant>>,
    algo: Mutex<Option<Value>>,
    requests: Mutex<Vec<String>>,
    errors: Mutex<Vec<String>>,
}

impl Exchange {
    fn reply(&self, method: &str, target: &str) -> (u16, Value) {
        let url = url::Url::parse(&format!("http://127.0.0.1{target}")).unwrap();
        let path = url.path();
        // Never retain a signed URL, headers, keys, or a response body in logs.
        self.requests
            .lock()
            .unwrap()
            .push(format!("{method} {path}"));
        let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let body = match (method, path) {
            ("POST", "/fapi/v1/listenKey") => {
                let n = self.listen_keys.fetch_add(1, Ordering::SeqCst) + 1;
                json!({"listenKey": format!("ownership-fake-{n}")})
            }
            ("PUT", "/fapi/v1/listenKey") => json!({}),
            ("GET", "/fapi/v1/userTrades") => json!([]),
            ("GET", "/fapi/v1/openOrders") => {
                if self.filled.load(Ordering::SeqCst) {
                    json!([])
                } else {
                    json!([{"symbol":"BTCUSDT", "orderId":ORDER,
                        "clientOrderId":"ownership-entry", "status":"NEW"}])
                }
            }
            ("GET", "/fapi/v1/order") => json!({"symbol":"BTCUSDT", "orderId":ORDER,
                "clientOrderId":"ownership-entry", "status":
                if self.filled.load(Ordering::SeqCst) {"FILLED"} else {"NEW"}}),
            ("GET", "/fapi/v1/openAlgoOrders") => match self.algo.lock().unwrap().clone() {
                Some(a) => {
                    self.algo_truth_reads.fetch_add(1, Ordering::SeqCst);
                    json!([a])
                }
                None => json!([]),
            },
            ("GET", "/fapi/v2/positionRisk") => json!([{"symbol":"BTCUSDT",
                "positionSide":"BOTH", "positionAmt":"1", "entryPrice":"100",
                "markPrice":"100", "unRealizedProfit":"0", "leverage":"1"}]),
            ("GET", "/fapi/v2/balance") => {
                self.balance_reads_at.lock().unwrap().push(Instant::now());
                json!([{"asset":"USDT", "balance":"10000",
                    "availableBalance":"9900", "crossWalletBalance":"10000"}])
            }
            ("GET", "/fapi/v1/exchangeInfo") => json!({"symbols":[{"symbol":"BTCUSDT",
            "filters":[
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},
                {"filterType":"MIN_NOTIONAL","notional":"1"},
                {"filterType":"PERCENT_PRICE","multiplierDown":"0.05","multiplierUp":"20"}
            ]}]}),
            ("GET", "/fapi/v1/premiumIndex") => json!({"symbol":"BTCUSDT", "markPrice":"100"}),
            ("POST", "/fapi/v1/algoOrder") => {
                if params.get("type").map(String::as_str) != Some("STOP_MARKET")
                    || params.get("side").map(String::as_str) != Some("SELL")
                    || params.get("symbol").map(String::as_str) != Some("BTCUSDT")
                    || !params.contains_key("clientAlgoId")
                {
                    self.errors
                        .lock()
                        .unwrap()
                        .push("invalid generated SL request".into());
                    return (400, json!({"code":-1100,"msg":"unexpected SL shape"}));
                }
                self.algo_posts.fetch_add(1, Ordering::SeqCst);
                let algo = json!({"algoId":ALGO, "clientAlgoId":params["clientAlgoId"],
                    "symbol":"BTCUSDT", "algoStatus":"NEW", "orderType":"STOP_MARKET",
                    "updateTime":chrono::Utc::now().timestamp_millis()});
                *self.algo.lock().unwrap() = Some(algo.clone());
                algo
            }
            ("GET", "/fapi/v1/algoOrder") => {
                self.algo_queries.fetch_add(1, Ordering::SeqCst);
                self.algo_truth_reads.fetch_add(1, Ordering::SeqCst);
                match self.algo.lock().unwrap().clone() {
                    Some(a)
                        if params
                            .get("algoId")
                            .is_some_and(|id| id == &ALGO.to_string())
                            || params
                                .get("clientAlgoId")
                                .is_some_and(|id| a["clientAlgoId"] == *id) =>
                    {
                        a
                    }
                    _ => return (400, json!({"code":-2013,"msg":"fake algo not found"})),
                }
            }
            _ => {
                self.errors
                    .lock()
                    .unwrap()
                    .push(format!("unexpected {method} {path}"));
                return (
                    400,
                    json!({"code":-1100,"msg":"unconfigured loopback endpoint"}),
                );
            }
        };
        (200, body)
    }
}

async fn rest_server(exchange: Arc<Exchange>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback REST is required");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let exchange = exchange.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                    }
                    if bytes.len() > 16384 {
                        return;
                    }
                }
                let line = String::from_utf8_lossy(&bytes);
                let mut parts = line.lines().next().unwrap().split_whitespace();
                let (status, body) = exchange.reply(parts.next().unwrap(), parts.next().unwrap());
                let body = body.to_string();
                let response = format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    port
}

struct SocketLifetime(Arc<Exchange>);
impl Drop for SocketLifetime {
    fn drop(&mut self) {
        self.0.active_sockets.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn ws_server(exchange: Arc<Exchange>, trigger: watch::Receiver<bool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback WS is required");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let exchange = exchange.clone();
            let mut trigger = trigger.clone();
            tokio::spawn(async move {
                let mut stream = tokio_tungstenite::accept_hdr_async(socket,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        assert!(request.uri().path().starts_with("/ws/ownership-fake-"),
                            "the WS path must consume the listenKey generated by the REST peer");
                        Ok(response)
                    }).await.unwrap();
                let number = exchange.handshakes.fetch_add(1, Ordering::SeqCst) + 1;
                exchange.active_sockets.fetch_add(1, Ordering::SeqCst);
                let _lifetime = SocketLifetime(exchange.clone());
                if number == 1 {
                    if trigger.wait_for(|go| *go).await.is_err() {
                        return;
                    }
                    exchange.filled.store(true, Ordering::SeqCst);
                    let now = chrono::Utc::now().timestamp_millis();
                    let frame = json!({"e":"ORDER_TRADE_UPDATE", "E":now, "T":now,
                        "o":{"s":"BTCUSDT", "c":"ownership-entry", "i":ORDER,
                        "x":"TRADE", "X":"FILLED", "t":660101, "l":"1", "L":"100", "z":"1"}});
                    stream.send(Message::Text(frame.to_string())).await.unwrap();
                    // The production reader processes this only after awaiting
                    // the complete fill handler, including temporary service drop.
                    stream.send(Message::Ping(PROBE.to_vec())).await.unwrap();
                }
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(Message::Pong(payload)) if number == 1 && payload == PROBE => {
                            exchange
                                .original_reader_resumed
                                .store(true, Ordering::SeqCst);
                        }
                        Ok(Message::Ping(payload)) => {
                            let _ = stream.send(Message::Pong(payload)).await;
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    });
    port
}

async fn seed(path: &Path) -> sqlx::Pool<sqlx::Any> {
    std::fs::File::create(path).unwrap();
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO fill_recovery_scopes (credential_id,symbol,registered_at_ms,updated_at_ms,scope_known,scope_proved_at_ms) VALUES (?,'BTCUSDT',1,1,1,1)")
        .bind(CREDENTIAL).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,order_role,status,exchange_order_id,client_order_id_short,position_id,created_at_ms,updated_at_ms) VALUES ('eng_ownership',?,?, 'ownership-entry','binance','futures',?,'open_long','long','market','1',0,'entry','acked',?,'ownership-entry',?,1,1)")
        .bind(CREDENTIAL).bind(BOT).bind(SYMBOL_KEY).bind(ORDER.to_string()).bind(POSITION)
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,qty,entry_price,state,active_key,entry_epoch,opened_at_ms,updated_at_ms) VALUES (?,?,?,?,'long','1','100','open',?,1,1,1)")
        .bind(POSITION).bind(CREDENTIAL).bind(BOT).bind(SYMBOL_KEY)
        .bind(format!("{BOT}|{SYMBOL_KEY}|long")).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO position_protection_plans (position_id,credential_id,bot_uid,exchange,market_kind,symbol_key,direction,stop_loss_price,take_profit_price,confirm_timeout_ms,generation,created_at_ms,updated_at_ms) VALUES (?,?,?,'binance','futures',?,'long','90','',3000,1,1,1)")
        .bind(POSITION).bind(CREDENTIAL).bind(BOT).bind(SYMBOL_KEY).execute(&pool).await.unwrap();
    pool
}

struct RustcoreProcess(std::process::Child);
impl Drop for RustcoreProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_engine(db: &Path, log: &Path, rest: u16, ws: u16, grpc: u16) -> RustcoreProcess {
    RustcoreProcess(engine_command(db, log, rest, ws, grpc).spawn().unwrap())
}

fn engine_command(db: &Path, log: &Path, rest: u16, ws: u16, grpc: u16) -> std::process::Command {
    let locator = format!("H{}", hex::encode_upper(CREDENTIAL));
    let output = std::fs::File::create(log).unwrap();
    // env_clear prevents inherited real credentials, proxy endpoints, or L4
    // switches. Legacy unconfigured venue permits the explicit loopback URLs;
    // exchange-backed "live" here is a fake-credential transport exercise only.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"));
    child
        .env_clear()
        .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc}"))
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "1")
        .env("ENGINE_STAGE_MAX", "live")
        .env("ENGINE_LIVE_ENABLED", "1")
        .env(
            "BINANCE_FUTURES_BASE_URL",
            format!("http://127.0.0.1:{rest}"),
        )
        .env(
            "BINANCE_FUTURES_WS_BASE_URL",
            format!("ws://127.0.0.1:{ws}"),
        )
        .env(format!("BINANCE_API_KEY_{locator}"), "ownership-fake-key")
        .env(
            format!("BINANCE_API_SECRET_{locator}"),
            "ownership-fake-secret",
        )
        .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
        .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
        .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
        .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
        .env("RECONCILE_INTERVAL_MS", "1000")
        .env("BINANCE_HTTP_TIMEOUT_MS", "2000")
        .env("PROTECTION_CONFIRM_TIMEOUT_MS", "1500")
        .env("RUST_LOG", "info")
        .stdout(output.try_clone().unwrap())
        .stderr(output);
    child
}

async fn eventually(label: &str, mut test: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(6);
    while !test() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn risk_rejected_entry(grpc: u16) -> pb::IntentAck {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let request = pb::ExecuteIntentsRequest {
        credential_id: CREDENTIAL.into(),
        run_mode: "live".into(),
        intents: vec![pb::OrderIntent {
            bot_uid: BOT.into(),
            run_id: "ownership_rpc".into(),
            cycle_id: "ownership_rpc".into(),
            idempotency_key: "ownership-rejected-entry".into(),
            client_order_id_short: "ownership-rejected".into(),
            exchange: "binance".into(),
            market_kind: "futures".into(),
            symbol_key: SYMBOL_KEY.into(),
            action: "open_long".into(),
            order_type: "limit".into(),
            limit_price: "100".into(),
            reduce_only: false,
            intent_id: "ownership_rpc_intent".into(),
            max_slippage_bps: 15,
            protection: Some(pb::Protection {
                required: true,
                stop_loss_price: "95".into(),
                take_profit_price: "110".into(),
                confirm_timeout_ms: 1500,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".into(),
            valid_until_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        }],
        data_snapshot: Some(pb::DataSnapshotDecision {
            snapshot_id: format!("snap_{}", "a".repeat(32)),
            quality_gate: "fail".into(),
            allow_open: false,
            allow_close: true,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
            schema_version: 1,
        }),
        schema_version: 1,
    };
    let codec =
        tonic::codec::ProstCodec::<pb::ExecuteIntentsRequest, pb::ExecuteIntentsResponse>::default(
        );
    let response = client
        .unary(
            tonic::Request::new(request),
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/ExecuteIntents",
            ),
            codec,
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.acks.len(), 1);
    let ack = response.acks.into_iter().next().unwrap();
    assert_eq!(ack.status, pb::Status::Rejected as i32, "{ack:?}");
    assert!(
        !ack.risk_verdict_id.is_empty(),
        "entry must reach the durable risk path: {ack:?}"
    );
    assert_eq!(ack.reject_reason_code, "STAGE_NOT_CERTIFIED", "{ack:?}");
    ack
}

async fn exercise(dir: &Path, warmup_rpc: bool) {
    let exchange = Arc::new(Exchange::default());
    let (trigger, rx) = watch::channel(false);
    let rest = rest_server(exchange.clone()).await;
    let ws = ws_server(exchange.clone(), rx).await;
    let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc = grpc_listener.local_addr().unwrap().port();
    drop(grpc_listener);
    let db = dir.join("fixture.db");
    let log = dir.join("engine.log");
    let pool = seed(&db).await;
    let _engine = spawn_engine(&db, &log, rest, ws, grpc);
    eventually("one original WS handshake", || {
        exchange.handshakes.load(Ordering::SeqCst) == 1
    })
    .await;
    let ready_deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let state = sqlx::query("SELECT ready,phase,COALESCE(failure_reason,'') AS failure FROM fill_stream_state WHERE credential_id=?")
            .bind(CREDENTIAL).fetch_optional(&pool).await.unwrap();
        if state
            .as_ref()
            .is_some_and(|r| r.get::<i64, _>("ready") == 1)
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "initial recovery did not converge: {:?}; see {}",
            state.map(|r| (r.get::<String, _>("phase"), r.get::<String, _>("failure"))),
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rpc_ack = if warmup_rpc {
        Some(risk_rejected_entry(grpc).await)
    } else {
        None
    };
    // Initial user-stream recovery has finished. A rejected ExecuteIntents RPC
    // can ensure the shared owner, but does not schedule a separate full scan.
    let prefill_started = Instant::now();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let prefill_rounds = exchange
        .balance_reads_at
        .lock()
        .unwrap()
        .iter()
        .filter(|at| {
            **at >= prefill_started && **at < prefill_started + Duration::from_millis(2300)
        })
        .count();
    let prefill_summary = json!({
        "warmup_rpc":warmup_rpc,
        "balance_reads_before_fill_in_2300ms":prefill_rounds,
        "ws_handshakes":exchange.handshakes.load(Ordering::SeqCst),
    });
    std::fs::write(
        dir.join("prefill-summary.json"),
        serde_json::to_vec_pretty(&prefill_summary).unwrap(),
    )
    .unwrap();
    eprintln!(
        "prefill ownership evidence: {}\n{}",
        dir.display(),
        prefill_summary
    );
    assert!(
        (2..=3).contains(&prefill_rounds),
        "exactly one periodic owner must run before fill (ordinary_rpc={warmup_rpc}); observed {prefill_rounds} balance reads in 2300ms; see {}",
        dir.display()
    );
    assert_eq!(
        exchange.handshakes.load(Ordering::SeqCst),
        1,
        "warmup itself duplicated streams"
    );
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM protection_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        before, 0,
        "fixture must not pre-confirm or even pre-create the SL"
    );
    trigger.send(true).unwrap();
    eventually("real SL POST and exact identity GET", || {
        exchange.algo_posts.load(Ordering::SeqCst) >= 1
            && exchange.algo_truth_reads.load(Ordering::SeqCst) >= 1
    })
    .await;
    eventually("original reader pong after full protection handler", || {
        exchange.original_reader_resumed.load(Ordering::SeqCst)
    })
    .await;
    let verified_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM protection_orders WHERE position_id=? AND kind='sl' AND verified_from_exchange=1 AND exchange_algo_id=?")
            .bind(POSITION).bind(ALGO.to_string()).fetch_one(&pool).await.unwrap();
        if count == 1 {
            break;
        }
        assert!(
            Instant::now() < verified_deadline,
            "real generated SL never became exchange-verified; see {}",
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The original reader has resumed, so its temporary service is gone. Keep
    // both transports healthy across >2 periodic ticks to observe detached work.
    let quiet_started = Instant::now();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let quiet_rounds = exchange
        .balance_reads_at
        .lock()
        .unwrap()
        .iter()
        .filter(|at| **at >= quiet_started && **at < quiet_started + Duration::from_millis(2300))
        .count();
    let engine_log = std::fs::read_to_string(&log).unwrap();
    let summary = json!({
        "warmup_rpc":warmup_rpc,
        "balance_reads_before_fill_in_2300ms":prefill_rounds,
        "risk_rejected_rpc_reason":rpc_ack.as_ref().map(|ack| &ack.reject_reason_code),
        "risk_rejected_rpc_verdict_id":rpc_ack.as_ref().map(|ack| &ack.risk_verdict_id),
        "initial_protection_rows":before,
        "algo_posts":exchange.algo_posts.load(Ordering::SeqCst),
        "algo_queries":exchange.algo_queries.load(Ordering::SeqCst),
        "algo_truth_reads":exchange.algo_truth_reads.load(Ordering::SeqCst),
        "exact_algo_verified_in_db":true,
        "original_reader_resumed_after_attachment":exchange.original_reader_resumed.load(Ordering::SeqCst),
        "listen_key_posts":exchange.listen_keys.load(Ordering::SeqCst),
        "ws_handshakes":exchange.handshakes.load(Ordering::SeqCst),
        "active_ws_after_handler_and_two_ticks":exchange.active_sockets.load(Ordering::SeqCst),
        "balance_reads_in_2300ms_quiet_window":quiet_rounds,
        "superseded_logs":engine_log.matches("superseded").count(),
        "parse_failure_logs":engine_log.matches("user stream could not parse a frame").count(),
        "unexpected_requests":*exchange.errors.lock().unwrap(),
        "requests":*exchange.requests.lock().unwrap(),
    });
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    eprintln!("ownership evidence: {}\n{}", dir.display(), summary);
    assert!(
        exchange.errors.lock().unwrap().is_empty(),
        "fake upstream saw unexpected actions"
    );
    assert_eq!(
        summary["parse_failure_logs"], 0,
        "a parse failure invalidates the no-reconnect control"
    );
    assert_eq!(
        exchange.algo_posts.load(Ordering::SeqCst),
        1,
        "SL must be really submitted exactly once"
    );
    assert_eq!(exchange.active_sockets.load(Ordering::SeqCst), 1,
        "WS fill attachment leaked an additional credential stream after its temporary service returned; see {}", dir.display());
    assert_eq!(
        exchange.listen_keys.load(Ordering::SeqCst),
        1,
        "healthy fill attachment must reuse the original credential manager"
    );
    assert!((2..=3).contains(&quiet_rounds),
        "one continuing 1000ms periodic owner must issue two or three balance reads in 2300ms; observed {quiet_rounds} after the fill callback");
    pool.close().await;
}

async fn run_fixture(warmup_rpc: bool) {
    let base = std::env::var_os("USERSTREAM_OWNERSHIP_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("ownership-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    tokio::time::timeout(Duration::from_secs(28), exercise(&dir, warmup_rpc))
        .await
        .expect("whole production ownership fixture must finish within 28s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_ws_fill_attachment_preserves_one_credential_background_owner() {
    run_fixture(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_start_owns_periodic_recovery_before_any_rpc_or_fill() {
    run_fixture(false).await;
}

// A2a reuses the same real upstream/production binary fixture. The original
// ownership cases above retain their original configuration and assertions.
mod pb_v1_startup {
    tonic::include_proto!("execution_engine.vnext");
}

fn startup_tls(dir: &Path) -> (PathBuf, PathBuf) {
    let certificate = dir.join("server.pem");
    let key = dir.join("server.key");
    let output = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=startup-loopback",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&certificate)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generate isolated gRPC TLS identity"
    );
    (certificate, key)
}

fn authenticated_engine(
    db: &Path,
    log: &Path,
    rest: u16,
    ws: u16,
    grpc: u16,
    cert: &Path,
    key: &Path,
) -> RustcoreProcess {
    let mut command = engine_command(db, log, rest, ws, grpc);
    command
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "0")
        .env("RUSTCORE_GRPC_AUTH_TOKEN", "startup-fixture-token")
        .env("RUSTCORE_GRPC_TLS_CERT_PATH", cert)
        .env("RUSTCORE_GRPC_TLS_KEY_PATH", key);
    RustcoreProcess(command.spawn().unwrap())
}

fn assert_startup_child_running(child: &mut RustcoreProcess, log: &Path) {
    let exit = child.0.try_wait().unwrap();
    assert_eq!(
        exit.is_none(),
        true,
        "legal startup exited: {exit:?}; {}",
        std::fs::read_to_string(log).unwrap()
    );
}

async fn startup_channel(
    child: &mut RustcoreProcess,
    log: &Path,
    grpc: u16,
    cert: &Path,
) -> tonic::transport::Channel {
    let tls = tonic::transport::ClientTlsConfig::new()
        .domain_name("127.0.0.1")
        .ca_certificate(tonic::transport::Certificate::from_pem(
            std::fs::read(cert).unwrap(),
        ));
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{grpc}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_millis(250));
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert_startup_child_running(child, log);
        match endpoint.clone().connect().await {
            Ok(channel) => return channel,
            Err(error) => {
                assert_startup_child_running(child, log);
                assert!(
                    Instant::now() < deadline,
                    "live child failed real TLS gRPC admission: {error}; {}",
                    std::fs::read_to_string(log).unwrap()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

async fn assert_authenticated_interfaces(channel: tonic::transport::Channel) {
    let mut client = tonic::client::Grpc::new(channel);
    for authorized in [false, true] {
        client.ready().await.unwrap();
        let mut request = tonic::Request::new(pb::GetPositionStatesRequest {
            credential_id: CREDENTIAL.into(),
            ..Default::default()
        });
        if authorized {
            request.metadata_mut().insert(
                "authorization",
                "Bearer startup-fixture-token".parse().unwrap(),
            );
        }
        let result = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/GetPositionStates",
                ),
                tonic::codec::ProstCodec::<
                    pb::GetPositionStatesRequest,
                    pb::GetPositionStatesResponse,
                >::default(),
            )
            .await;
        if authorized {
            result.expect("authenticated v2 must remain usable");
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        }
        client.ready().await.unwrap();
        let mut request = tonic::Request::new(pb_v1_startup::GetOpenOrdersRequest {
            credential_id: CREDENTIAL.into(),
            ..Default::default()
        });
        if authorized {
            request.metadata_mut().insert(
                "authorization",
                "Bearer startup-fixture-token".parse().unwrap(),
            );
        }
        let result = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.vnext.EngineExecution/GetOpenOrders",
                ),
                tonic::codec::ProstCodec::<
                    pb_v1_startup::GetOpenOrdersRequest,
                    pb_v1_startup::GetOpenOrdersResponse,
                >::default(),
            )
            .await;
        if authorized {
            result.expect("authenticated v1 must remain usable");
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        }
    }
}

async fn startup_recovery_ready(
    pool: &sqlx::Pool<sqlx::Any>,
    child: &mut RustcoreProcess,
    log: &Path,
    since: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert_startup_child_running(child, log);
        let row = sqlx::query("SELECT ready, CAST(updated_at_ms AS CHAR) AS updated FROM fill_stream_state WHERE credential_id=?")
            .bind(CREDENTIAL).fetch_optional(pool).await.unwrap();
        if row.as_ref().is_some_and(|row| {
            row.get::<i64, _>("ready") == 1
                && row.get::<String, _>("updated").parse::<i64>().unwrap() >= since
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "new startup has not restored persistent recovery; state={:?}",
            row.map(|row| (row.get::<i64, _>("ready"), row.get::<String, _>("updated")))
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn kill_and_reap_startup_child(child: &mut RustcoreProcess) {
    child
        .0
        .kill()
        .expect("terminate only the owned startup child");
    let status = child.0.wait().expect("reap the exact startup child");
    assert!(!status.success(), "forced child exit must be observed");
    assert_eq!(child.0.try_wait().unwrap(), Some(status));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_startup_preserves_incumbent_and_recovers_after_reaped_exit() {
    let root = std::env::var_os("STARTUP_ADMISSION_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = root.join(format!("startup-owner-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = startup_tls(&dir);
    let exchange = Arc::new(Exchange::default());
    let (trigger, rx) = watch::channel(false);
    let rest = rest_server(exchange.clone()).await;
    let ws = ws_server(exchange.clone(), rx).await;
    let available = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc = available.local_addr().unwrap().port();
    drop(available);

    // Legal startup retains the previous Store behavior of creating nested dirs.
    let fresh_db = dir.join("fresh/nested/engine.db");
    let fresh_log = dir.join("fresh.log");
    let mut fresh = authenticated_engine(&fresh_db, &fresh_log, rest, ws, grpc, &cert, &key);
    assert_authenticated_interfaces(startup_channel(&mut fresh, &fresh_log, grpc, &cert).await)
        .await;
    assert!(
        fresh_db.is_file(),
        "admitted startup must initialize its nested store"
    );
    kill_and_reap_startup_child(&mut fresh);
    assert_eq!(
        exchange.handshakes.load(Ordering::SeqCst),
        0,
        "empty store must not fabricate a recovery scope"
    );

    let db = dir.join("engine.db");
    let pool = seed(&db).await;
    let log = dir.join("incumbent.log");
    let started = chrono::Utc::now().timestamp_millis();
    let mut incumbent = authenticated_engine(&db, &log, rest, ws, grpc, &cert, &key);
    let channel = startup_channel(&mut incumbent, &log, grpc, &cert).await;
    startup_recovery_ready(&pool, &mut incumbent, &log, started).await;
    assert_authenticated_interfaces(channel).await;
    assert_eq!(exchange.handshakes.load(Ordering::SeqCst), 1);

    let duplicate_db = dir.join("duplicate/engine.db");
    let duplicate_log = dir.join("duplicate.log");
    let mut duplicate =
        authenticated_engine(&duplicate_db, &duplicate_log, rest, ws, grpc, &cert, &key);
    let deadline = Instant::now() + Duration::from_secs(5);
    let duplicate_status = loop {
        assert!(
            !duplicate_db.exists(),
            "duplicate reached Store initialization"
        );
        if let Some(status) = duplicate.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "duplicate still alive despite occupied listener"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(duplicate.0.wait().unwrap(), duplicate_status);
    assert!(!duplicate_status.success());
    assert!(std::fs::read_to_string(&duplicate_log)
        .unwrap()
        .contains("reserve gRPC listener failed"));
    assert!(!dir.join("duplicate").exists());
    assert_startup_child_running(&mut incumbent, &log);
    trigger.send(true).unwrap();
    eventually(
        "incumbent actually submits SL after duplicate rejection",
        || exchange.algo_posts.load(Ordering::SeqCst) == 1,
    )
    .await;
    eventually("incumbent reader finishes the real fill/SL path", || {
        exchange.original_reader_resumed.load(Ordering::SeqCst)
    })
    .await;
    let verified_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let verified: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM protection_orders WHERE position_id=? AND kind='sl' AND verified_from_exchange=1 AND exchange_algo_id=?")
            .bind(POSITION).bind(ALGO.to_string()).fetch_one(&pool).await.unwrap();
        if verified == 1 {
            break;
        }
        assert!(
            Instant::now() < verified_deadline,
            "incumbent SL must be persisted as exchange verified"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_authenticated_interfaces(startup_channel(&mut incumbent, &log, grpc, &cert).await).await;
    assert_eq!(exchange.active_sockets.load(Ordering::SeqCst), 1);
    kill_and_reap_startup_child(&mut incumbent);
    eventually("old reader has actually closed", || {
        exchange.active_sockets.load(Ordering::SeqCst) == 0
    })
    .await;

    let restart_log = dir.join("restarted.log");
    let restarted_at = chrono::Utc::now().timestamp_millis();
    let mut restarted = authenticated_engine(&db, &restart_log, rest, ws, grpc, &cert, &key);
    let channel = startup_channel(&mut restarted, &restart_log, grpc, &cert).await;
    startup_recovery_ready(&pool, &mut restarted, &restart_log, restarted_at).await;
    assert_authenticated_interfaces(channel).await;
    assert_eq!(exchange.handshakes.load(Ordering::SeqCst), 2);
    assert_eq!(exchange.active_sockets.load(Ordering::SeqCst), 1);
    assert_eq!(
        exchange.algo_posts.load(Ordering::SeqCst),
        1,
        "restart must not duplicate the incumbent's active SL"
    );
    assert!(exchange.errors.lock().unwrap().is_empty());
    kill_and_reap_startup_child(&mut restarted);
    eventually("new reader is reaped after the test", || {
        exchange.active_sockets.load(Ordering::SeqCst) == 0
    })
    .await;
    pool.close().await;
}
