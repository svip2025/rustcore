//! **不走 L4 假交易所的
//! fill-recovery 生产入口证据。**
//!
//! R15 的边界是：WS 通路 `run_once` **完全没有行为级
//! 证据**——R15M29「把 WS 双源判据整块删掉」在 L4 的 4 条进程级用例上 4/4 存活，
//! 因为那 4 条走的都是 `ensure_running` 里 `l4_fake_exchange` 的 early-return，
//! `run_once` 一行都没执行。
//!
//! 这条用例按 engine safety contract 的四条办：
//!
//! * **入口同一**：跑的是 `CARGO_BIN_EXE_rustcore`，即部署时执行的那个二进制，
//!   走 `main()` 全程（fault-proxy 校验 → 建库/迁移/启动复核 → 后台任务 →
//!   `start_fill_recovery_on_startup` → gRPC serve）。**默认 feature 下不编译
//!   `l4-fault-injection`**，`ensure_running` 的假交易所 early-return 在那个
//!   二进制里根本不存在；即便以 `--all-features` 编译，本用例也**不设**
//!   `L4_FAKE_EXCHANGE_DB`，`configured_db()` 为 `None` ⇒ 同样不走 early-return
//!   （`--all-features` 串行实跑同样通过）。两种编译下跑的都是 `run_once` 本体。
//! * **不跳层**：listenKey 的 REST 创建、WSS 握手与帧循环、REST 逐页回补、
//!   五段对账真值、readiness 落库、以及 `risk.rs` 的开仓闸门，一层都没绕；
//!   开仓走的是生产 gRPC 端口上的 `execution_engine.v2.EngineExecution/ExecuteIntents`。
//! * **两侧真值同源**：listenKey 由本用例的 REST 端产出、由引擎消费并用来拼
//!   WS 路径，用例在**握手请求里逐字读回**它；WS 帧里的 `orderId` 与库里种下的
//!   那张本地单同源；REST 回补的成交由同一个端产出、由引擎落进 `engine_fills`。
//! * **反向可断言**：同一条 intent、同一个进程——交易所侧**非归零**时开仓被
//!   `FILL_RECOVERY_NOT_READY` 挡住；把交易所侧改成全平之后（引擎自己重连、
//!   重走回补与对账）同一条 intent 不再被该判据挡。
//!
//! **替身缝的位置（逐条）**：只有**交易所本身**被换成本机监听端口——
//! REST 用生产同名开关 `BINANCE_FUTURES_BASE_URL`，WS 用
//! `BINANCE_FUTURES_WS_BASE_URL`（`startup_market_data_restore.rs` 同款）。
//! 这两个缝都在**被证明的链路之外**：链路本身（启动 → 起流 → listenKey →
//! 握手 → 帧 → 回补 → 对账 → readiness → 开仓闸门）一层都没替。
//!
//! **诚实边界**：本用例证明的是「这条链会执行、两端真值同源、反向能挡住」，
//! **不是**「与真实 Binance 的线上兼容性」——后者需要真实交易所连接与凭据，
//! 本测试不连接真实交易所，不能用本机替身证明线上兼容性。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::any::AnyPoolOptions;
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod pb2 {
    tonic::include_proto!("execution_engine.v2");
}

const CREDENTIAL: &str = "r16.wire";
const SYMBOL: &str = "BTCUSDT";
const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";
const LISTEN_KEY: &str = "r16wirelistenkey";
const WS_ORDER_ID: i64 = 8801;
const BACKFILL_TRADE_ID: i64 = 9901;

/// `BINANCE_API_KEY_<locator>` 的 locator 与生产同一份编码
/// （`venue::credential_secret_locator`：`H` + UTF-8 字节的十六进制）。
fn secret_locator(credential_id: &str) -> String {
    let mut locator = String::from("H");
    for byte in credential_id.as_bytes() {
        locator.push_str(&format!("{byte:02X}"));
    }
    locator
}

// ---------------------------------------------------------------------------
// 交易所替身（**链路之外**）：一个真实的 loopback HTTP 端与一个真实的 WS 端。
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExchangePosture {
    /// 交易所侧还挂着一张本地不认识的条件单：对账必然报分歧
    /// （`has_diff` 与 `all_open_algo_orders_empty` 两段同时说话），
    /// readiness 不得为 ready。
    Residual,
    /// 交易所侧收敛：对账五段全清，readiness 可以为 ready。
    Converged,
}

#[derive(Default)]
struct RestLog {
    paths: Vec<String>,
}

fn json_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn rest_body(path: &str, posture: ExchangePosture) -> String {
    match path {
        "/fapi/v1/listenKey" => format!("{{\"listenKey\":\"{LISTEN_KEY}\"}}"),
        "/fapi/v1/userTrades" => format!(
            "[{{\"id\":{BACKFILL_TRADE_ID},\"symbol\":\"{SYMBOL}\",\"orderId\":{WS_ORDER_ID},\
             \"side\":\"BUY\",\"price\":\"100\",\"qty\":\"1\",\"quoteQty\":\"100\",\
             \"commission\":\"0.04\",\"commissionAsset\":\"USDT\",\"realizedPnl\":\"0\",\
             \"time\":1700000000000}}]"
        ),
        "/fapi/v1/openOrders" | "/fapi/v2/positionRisk" | "/fapi/v2/balance" => "[]".to_string(),
        "/fapi/v1/openAlgoOrders" => match posture {
            ExchangePosture::Converged => "[]".to_string(),
            ExchangePosture::Residual => {
                "[{\"algoId\":7000000001,\"clientAlgoId\":\"algo-r16wire\",\"algoStatus\":\"NEW\"}]"
                    .to_string()
            }
        },
        _ => "{}".to_string(),
    }
}

/// 起一个真实的本机 HTTP 端，逐条应答生产 client 发来的签名请求。
async fn spawn_rest_upstream(
    posture: Arc<Mutex<ExchangePosture>>,
    log: Arc<Mutex<RestLog>>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in REST upstream");
    let port = listener.local_addr().expect("rest addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _peer)) = listener.accept().await else {
                return;
            };
            let posture = posture.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 2048];
                loop {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..read]);
                    if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&buffer).to_string();
                let target = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let path = target.split('?').next().unwrap_or("/").to_string();
                log.lock().expect("rest log").paths.push(path.clone());
                let current = *posture.lock().expect("posture");
                let body = rest_body(&path, current);
                let _ = socket.write_all(json_response(&body).as_bytes()).await;
                let _ = socket.flush().await;
            });
        }
    });
    port
}

struct WsObservation {
    handshake_targets: Vec<String>,
}

/// 起一个真实的本机 WebSocket 端：握手由 `tokio-tungstenite` 完成，
/// 用例逐字读回引擎请求的路径（listenKey 的两侧同源证据），随后推一帧
/// `ORDER_TRADE_UPDATE` 并把连接握住（reader 存活 = readiness 的第五段）。
async fn spawn_ws_upstream(observed: Arc<Mutex<WsObservation>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in WS upstream");
    let port = listener.local_addr().expect("ws addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _peer)) = listener.accept().await else {
                return;
            };
            let observed = observed.clone();
            tokio::spawn(async move {
                let target = Arc::new(Mutex::new(String::new()));
                let captured = target.clone();
                let stream = match tokio_tungstenite::accept_hdr_async(
                    socket,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        *captured.lock().expect("ws target") = request.uri().to_string();
                        Ok(response)
                    },
                )
                .await
                {
                    Ok(stream) => stream,
                    Err(_) => return,
                };
                observed
                    .lock()
                    .expect("ws observation")
                    .handshake_targets
                    .push(target.lock().expect("ws target").clone());
                let mut stream = stream;
                use futures_util::SinkExt;
                let frame = format!(
                    "{{\"e\":\"ORDER_TRADE_UPDATE\",\"E\":1,\"T\":2,\"o\":{{\"s\":\"{SYMBOL}\",\
                     \"c\":\"\",\"i\":{WS_ORDER_ID},\"x\":\"TRADE\",\"X\":\"FILLED\"}}}}"
                );
                let _ = stream
                    .send(tokio_tungstenite::tungstenite::Message::Text(frame.into()))
                    .await;
                // 连接握住：reader 存活正是 readiness 的第五段判据。
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            });
        }
    });
    port
}

// ---------------------------------------------------------------------------
// 库存：一条**已证明**的恢复域 + 一张本地单（WS 帧要落到它身上）。
// ---------------------------------------------------------------------------

async fn seeded_sqlite(name: &str) -> String {
    sqlx::any::install_default_drivers();
    let path =
        std::env::temp_dir().join(format!("rustcore-r16wire-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    std::fs::File::create(&path).expect("create sqlite file");
    let url = format!("sqlite://{}", path.display());
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect seed pool");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run rustcore migrations");
    sqlx::query(
        "INSERT INTO fill_recovery_scopes (credential_id, symbol, registered_at_ms, \
         updated_at_ms, scope_known, scope_proved_at_ms) VALUES (?, ?, 1, 1, 1, 1)",
    )
    .bind(CREDENTIAL)
    .bind(SYMBOL)
    .execute(&pool)
    .await
    .expect("seed a proved fill recovery scope");
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         exchange, market_kind, symbol_key, action, side, order_type, quantity, reduce_only, \
         order_role, status, exchange_order_id, client_order_id_short, created_at_ms, \
         updated_at_ms) VALUES ('eng_r16wire', ?, 'bot_r16wire', 'idem_r16wire', 'binance', \
         'futures', ?, 'open_long', 'long', 'market', '1', 0, 'entry', 'acked', ?, \
         'coid_r16wire', 1, 1)",
    )
    .bind(CREDENTIAL)
    .bind(SYMBOL_KEY)
    .bind(WS_ORDER_ID.to_string())
    .execute(&pool)
    .await
    .expect("seed the local order the WS frame belongs to");
    pool.close().await;
    url
}

struct RustcoreProcess(std::process::Child);

impl Drop for RustcoreProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_rustcore(db_url: &str, rest_port: u16, ws_port: u16, grpc_port: u16) -> RustcoreProcess {
    let (key_env, secret_env) = (
        format!("BINANCE_API_KEY_{}", secret_locator(CREDENTIAL)),
        format!("BINANCE_API_SECRET_{}", secret_locator(CREDENTIAL)),
    );
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"))
        .env("RUSTCORE_DB_URL", db_url)
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc_port}"))
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "1")
        .env("ENGINE_STAGE_MAX", "demo")
        .env("ENGINE_LIVE_ENABLED", "0")
        // 上游只能是本机监听端口，杜绝任何真实交易所出网。
        .env(
            "BINANCE_FUTURES_BASE_URL",
            format!("http://127.0.0.1:{rest_port}"),
        )
        .env(
            "BINANCE_FUTURES_WS_BASE_URL",
            format!("ws://127.0.0.1:{ws_port}"),
        )
        .env("BINANCE_FAULT_PROXY_REST_BASE_URL", "")
        .env("BINANCE_FAULT_PROXY_WS_BASE_URL", "")
        .env(key_env, "r16-wire-key")
        .env(secret_env, "r16-wire-secret")
        .env("RUST_LOG", "warn")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn the production rustcore binary");
    RustcoreProcess(child)
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let port = listener.local_addr().expect("reserved addr").port();
    drop(listener);
    port
}

async fn read_pool(url: &str) -> sqlx::Pool<sqlx::Any> {
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect the observation pool")
}

#[derive(Debug, Clone)]
struct StreamState {
    ready: i64,
    phase: String,
    failure_reason: String,
}

async fn stream_state(pool: &sqlx::Pool<sqlx::Any>) -> Option<StreamState> {
    sqlx::query(
        "SELECT ready, phase, COALESCE(failure_reason, '') AS failure_reason \
         FROM fill_stream_state WHERE credential_id = ?",
    )
    .bind(CREDENTIAL)
    .fetch_optional(pool)
    .await
    .expect("read fill stream state")
    .map(|row| StreamState {
        ready: row.get::<i64, _>("ready"),
        phase: row.get::<String, _>("phase"),
        failure_reason: row.get::<String, _>("failure_reason"),
    })
}

/// 轮询到判据成立为止；超时返回最后观察到的状态（断言里带出来，便于定位）。
async fn wait_for_state(
    pool: &sqlx::Pool<sqlx::Any>,
    within: Duration,
    mut done: impl FnMut(&StreamState) -> bool,
) -> Option<StreamState> {
    let deadline = std::time::Instant::now() + within;
    let mut last = None;
    loop {
        last = stream_state(pool).await.or(last);
        if last.as_ref().is_some_and(&mut done) {
            return last;
        }
        if std::time::Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// 生产 gRPC 端口上的 v2 开仓（`svc_v2::execute_intents`）。
// ---------------------------------------------------------------------------

async fn open_long_reject_code(grpc_port: u16, idempotency_key: &str) -> (i32, String, String) {
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("connect to the production gRPC port");
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("grpc service ready");
    let request = pb2::ExecuteIntentsRequest {
        credential_id: CREDENTIAL.to_string(),
        run_mode: "demo".to_string(),
        intents: vec![pb2::OrderIntent {
            bot_uid: "bot_r16wire".to_string(),
            run_id: "run_r16wire".to_string(),
            cycle_id: "cyc_r16wire".to_string(),
            idempotency_key: idempotency_key.to_string(),
            client_order_id_short: "coid_r16open".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "open_long".to_string(),
            order_type: "market".to_string(),
            limit_price: String::new(),
            reduce_only: false,
            intent_id: format!("int_{idempotency_key}"),
            max_slippage_bps: 10,
            protection: None,
            policy_version: String::new(),
            valid_until_ms: 0,
            config_hash: String::new(),
            prompt_hash: String::new(),
            code_commit: String::new(),
            schema_version: 1,
        }],
        data_snapshot: None,
        schema_version: 1,
    };
    let codec = tonic::codec::ProstCodec::<pb2::ExecuteIntentsRequest, pb2::ExecuteIntentsResponse>::default();
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(
        "/execution_engine.v2.EngineExecution/ExecuteIntents",
    );
    let response = client
        .unary(tonic::Request::new(request), path, codec)
        .await
        .expect("call the production ExecuteIntents entry point")
        .into_inner();
    let ack = response.acks.into_iter().next().expect("exactly one ack");
    (ack.status, ack.reject_reason_code, ack.message)
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_production_binary_runs_the_real_fill_recovery_chain_and_gates_new_entries() {
    let posture = Arc::new(Mutex::new(ExchangePosture::Residual));
    let rest_log = Arc::new(Mutex::new(RestLog::default()));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let rest_port = spawn_rest_upstream(posture.clone(), rest_log.clone()).await;
    let ws_port = spawn_ws_upstream(ws_observation.clone()).await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("chain").await;

    let started = std::time::Instant::now();
    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = read_pool(&db_url).await;
    eprintln!("[r16wire] {:?} spawned", started.elapsed());

    // ---- 反向：交易所侧未收敛 ⇒ 对账报分歧 ⇒ readiness 不 ready ----------
    //
    // 判据取**对账那一段真值自己写下的失败理由**，不是"随便一个 ready=0"——
    // 恢复周期一开始就是 ready=0，拿它当证据等于什么都没证。
    // 设计要求（A+B）：readiness 判据改为两段收敛（has_diff / local_position_zero_diff），
    // 失败文案不再输出 all_positions_zero / all_open_* 三段。本反向用例的分歧来自
    // Residual posture 的残留条件单 ⇒ has_diff=true，在新判据下仍然 not ready。
    let not_ready = wait_for_state(&pool, Duration::from_secs(60), |state| {
        state.ready == 0
            && state.failure_reason.contains(
                "post-backfill reconcile did not leave local and exchange truth converged",
            )
    })
    .await
    .expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.ready, 0,
        "a reconcile that reports a divergence must not declare the handoff ready: {not_ready:?}"
    );
    assert!(
        not_ready.failure_reason.contains("has_diff=true")
            && !not_ready
                .failure_reason
                .contains("all_open_algo_orders_empty"),
        "the recorded reason must be the exchange-side divergence itself, in the two-segment \
         converged form: {not_ready:?}"
    );

    eprintln!("[r16wire] {:?} not-ready observed", started.elapsed());
    // WS 与 REST 两条腿都真的跑过了（真值两侧同源）。
    let handshake = ws_observation
        .lock()
        .expect("ws observation")
        .handshake_targets
        .first()
        .cloned()
        .expect("the engine must have completed a real websocket handshake");
    assert_eq!(
        handshake,
        format!("/ws/{LISTEN_KEY}"),
        "the engine must connect with the listenKey this test's REST端 minted"
    );
    {
        let paths = &rest_log.lock().expect("rest log").paths;
        for expected in [
            "/fapi/v1/listenKey",
            "/fapi/v1/userTrades",
            "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders",
            "/fapi/v2/positionRisk",
            "/fapi/v2/balance",
        ] {
            assert!(
                paths.iter().any(|path| path == expected),
                "the real run_once must call {expected}; saw {paths:?}"
            );
        }
    }
    let ws_applied = sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
        .bind("eng_r16wire")
        .fetch_one(&pool)
        .await
        .expect("read the seeded local order")
        .get::<String, _>("status");
    assert_eq!(
        ws_applied, "filled",
        "the websocket frame must have reached the production sink"
    );
    let backfilled = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills WHERE credential_id=?")
        .bind(CREDENTIAL)
        .fetch_one(&pool)
        .await
        .expect("count backfilled fills")
        .get::<i64, _>("n");
    assert!(
        backfilled >= 1,
        "the REST backfill leg must have ingested the exchange trade"
    );

    eprintln!("[r16wire] {:?} legs observed", started.elapsed());
    // 开仓被 readiness 闸门挡住——**同一条生产入口**。
    let (status, reject_code, message) =
        open_long_reject_code(grpc_port, "idem_r16wire_blocked").await;
    assert_eq!(status, pb2::Status::Rejected as i32, "message={message}");
    assert_eq!(
        reject_code, "FILL_RECOVERY_NOT_READY",
        "an unconverged recovery must block new entries; message={message}"
    );

    eprintln!("[r16wire] {:?} blocked open observed", started.elapsed());
    // ---- 正向：交易所侧转为收敛 ⇒ 引擎自己重连、重走回补与对账 ⇒ ready ----
    *posture.lock().expect("posture") = ExchangePosture::Converged;
    let ready = wait_for_state(&pool, Duration::from_secs(90), |state| state.ready == 1)
        .await
        .expect("the fill stream state row must still exist");
    assert_eq!(
        ready.ready, 1,
        "a converged reconcile on a live stream must declare the handoff ready: {ready:?}"
    );

    eprintln!(
        "[r16wire] {:?} ready observed: {ready:?}",
        started.elapsed()
    );
    let (_status, reject_code, message) =
        open_long_reject_code(grpc_port, "idem_r16wire_ready").await;
    eprintln!("[r16wire] ready-side ack: code={reject_code:?} message={message:?}");
    assert_ne!(
        reject_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={message}"
    );

    pool.close().await;
}
