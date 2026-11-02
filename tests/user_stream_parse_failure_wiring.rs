//! **从真实生产
//! 二进制注入畸形帧，并且**开着 gRPC 鉴权**观察新开仓被挡。**
//!
//! R16 的 `user_stream_recovery_wiring.rs` 证明了这条链会执行，但第十三轮点出
//! 一条声明偏差：那条用例把 `RUSTCORE_GRPC_AUTH_DISABLED=1` 显式写进了进程
//! 环境——鉴权那一层被**关掉**了，而报告写的是「不跳层：鉴权一层都没绕」。
//! 这条用例把那一层补回来，并且换一个注入点：不是让交易所侧"不收敛"，
//! 而是**推一帧解析不了的真值**，观察 `ws_parse` / `ready=0` / 连接重建 /
//! 生产 gRPC 新开仓被挡。
//!
//! 按 engine safety contract 的四条：
//!
//! * **入口同一**：跑的是 `CARGO_BIN_EXE_rustcore`，即部署时执行的那个二进制，
//!   走 `main()` 全程；开仓走生产 gRPC 端口上的
//!   `execution_engine.v2.EngineExecution/ExecuteIntents`。
//! * **不跳层**：listenKey 的 REST 创建、WSS 握手与帧循环、**帧解析**、
//!   readiness 落库、**gRPC 鉴权拦截器**、`svc_v2` 的规范化、`risk.rs` 的开仓
//!   闸门，一层都没绕。鉴权**开着**（本进程不设 `RUSTCORE_GRPC_AUTH_DISABLED`，
//!   `RUSTCORE_GRPC_AUTH_TOKEN` 配一份测试 token），并且**反向**证明它真的在
//!   链路上：同一条 intent 不带 token 时拿到 `UNAUTHENTICATED`。
//! * **两侧真值同源**：listenKey 由本用例的 REST 端产出、由引擎消费并拼进 WS
//!   路径，用例在握手请求里逐字读回；恢复之后那一帧健康 `ORDER_TRADE_UPDATE`
//!   的 `orderId` 与库里种下的那张本地单同源。
//! * **反向可断言**：畸形帧期间同一条 intent 被 `FILL_RECOVERY_NOT_READY` 挡住；
//!   交易所侧改推健康帧之后（引擎自己重连、重走回补与对账）同一条 intent
//!   不再被该判据挡。
//!
//! **替身缝的位置**：只有**交易所本身**被换成本机监听端口（生产同名开关
//! `BINANCE_FUTURES_BASE_URL` / `BINANCE_FUTURES_WS_BASE_URL`）。被证明的链路
//! 本身一层都没替。
//!
//! **诚实边界**：本用例证明的是「这条链会执行、鉴权真的在链路上、反向能挡住」，
//! **不是**「与真实 Binance 的线上兼容性」——后者需要真实交易所端点、真实凭据
//! 与出网策略。本测试不连接真实交易所，不能用本机替身证明线上兼容性。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::any::AnyPoolOptions;
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod pb2 {
    tonic::include_proto!("execution_engine.v2");
}

const CREDENTIAL: &str = "r17.wire";
const SYMBOL: &str = "BTCUSDT";
const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";
const LISTEN_KEY: &str = "r17wirelistenkey";
const WS_ORDER_ID: i64 = 8901;
const BACKFILL_TRADE_ID: i64 = 9902;
/// 一份**只在本用例进程里存在**的假 token（不是任何真实凭据）。
const AUTH_TOKEN: &str = "r17-wire-token";

/// 畸形帧：事件名落在 `TRUTH_BEARING_WS_EVENT_TYPES` **之外**，但 `E` 的类型
/// 不符 ⇒ `BinanceWsEnvelope` 反序列化失败。这正是第十三轮 regression 给的
/// 反例：R16 会 warn+continue 并保持 `ready=1`，而这条帧的语义是
/// 「listenKey 失效、这条流已经死了」。
const MALFORMED_FRAME: &str = r#"{"e":"listenKeyExpired","E":"not-a-number"}"#;

/// ****：一条**非 UTF-8 的
/// Binary 帧**。R17 的帧循环用 `_ => {}` 把所有非 Text/Ping/Close 帧静默丢掉
/// ——不解析、不写 failure、不重连，`ready` 保持 1 继续放行新开仓。
const MALFORMED_BINARY_FRAME: [u8; 4] = [0xff, 0xfe, 0x00, 0x01];

/// **一次握手后推出去的 Ping 条数**。
///
/// 64 条 × 127 字节 ≈ 8KB，全部在连接关闭之前一次 flush 出去，因此它们
/// 落进引擎侧 tungstenite 的读缓冲里；引擎随后逐条从**缓冲**里取帧、
/// 逐条写 Pong，而不需要再碰 socket。第一条 Pong 触发对端回 RST，
/// 后面那几十条写入于是确定性地失败在**写侧**（`ws_pong_write`），
/// 而不是先在读侧撞上 `ws_transport`。
const PING_BURST: usize = 64;

fn secret_locator(credential_id: &str) -> String {
    let mut locator = String::from("H");
    for byte in credential_id.as_bytes() {
        locator.push_str(&format!("{byte:02X}"));
    }
    locator
}

// ---------------------------------------------------------------------------
// 交易所替身（链路之外）
// ---------------------------------------------------------------------------

fn json_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn rest_body(path: &str) -> String {
    match path {
        "/fapi/v1/listenKey" => format!("{{\"listenKey\":\"{LISTEN_KEY}\"}}"),
        "/fapi/v1/userTrades" => format!(
            "[{{\"id\":{BACKFILL_TRADE_ID},\"symbol\":\"{SYMBOL}\",\"orderId\":{WS_ORDER_ID},\
             \"side\":\"BUY\",\"price\":\"100\",\"qty\":\"1\",\"quoteQty\":\"100\",\
             \"commission\":\"0.04\",\"commissionAsset\":\"USDT\",\"realizedPnl\":\"0\",\
             \"time\":1700000000000}}]"
        ),
        "/fapi/v1/openOrders"
        | "/fapi/v2/positionRisk"
        | "/fapi/v2/balance"
        | "/fapi/v1/openAlgoOrders" => "[]".to_string(),
        _ => "{}".to_string(),
    }
}

/// 起一个真实的本机 HTTP 端。
///
/// `/fapi/v2/positionRisk` 刻意**慢**一拍：`run_once` 的主干要在 reader 之后
/// 跑完回补与对账才会去写 readiness，慢一点能让"reader 已经写下 `ws_parse`、
/// 主干还没回来"这个状态在库里稳定存在足够久，用例因此能确定性地观察到它，
/// 而不是靠采样运气。这只影响**交易所替身**的应答速度，不改任何被测判据。
async fn spawn_rest_upstream(broken_backfill: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in REST upstream");
    let port = listener.local_addr().expect("rest addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _peer)) = listener.accept().await else {
                return;
            };
            let broken_backfill = broken_backfill.clone();
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
                if path == "/fapi/v2/positionRisk" {
                    tokio::time::sleep(Duration::from_millis(900)).await;
                }
                // **回补可以被本用例掐断**：`ws_closed` 那条用例
                // 需要"引擎重连之后**再也回不到 ready**"，否则 2 秒退避之后
                // 新一轮恢复会把 `ready` 又写成 1，断言窗口变成撞运气。
                // 这只改**交易所替身**的应答，被测判据一层都没动。
                if path == "/fapi/v1/userTrades" && broken_backfill.load(Ordering::SeqCst) {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\
                              Connection: close\r\n\r\n",
                        )
                        .await;
                    let _ = socket.flush().await;
                    return;
                }
                let _ = socket
                    .write_all(json_response(&rest_body(&path)).as_bytes())
                    .await;
                let _ = socket.flush().await;
            });
        }
    });
    port
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WsPosture {
    /// 推一帧解析不了的帧。
    Malformed,
    /// 推一帧健康的 `ORDER_TRADE_UPDATE`（与库里那张本地单同源）。
    Healthy,
    /// **regression**：推一帧读不懂的 Binary。R17 会把它整条
    /// 吞掉、继续等下一帧，于是恢复照常走到 `ready=1`。
    MalformedBinary,
    /// 握完手立刻把连接关掉：reader 一个字节都没读到就结束。
    ///
    /// R17 时点上这条路径**不写**任何 recovery failure、也不推恢复代际
    /// （`Message::Close` / `StreamPoll::Ended` 都是裸 `break` + `Ok(())`），
    /// 因此它钉的是 `run_once` 那次 `tasks.reader_finished()` 采样。
    /// 之后断流本身也会在 reader 返回前落 not-ready
    /// 并推代际，两道判据于是都覆盖这条形态——用例的断言（**从来没有
    /// ready 过** + 新开仓一直被挡）两种实现下含义相同。
    EndImmediately,
    /// **regression**：握完手就把 TCP 连接掐掉，**不走关闭握手**。
    ///
    /// 这是生产里最常见的那种死法（网络抖动、LB 掉连接、TLS 断开）：
    /// tungstenite 读到 EOF 时按协议报
    /// `Protocol(ResetWithoutClosingHandshake)`，也就是帧循环里
    /// `let msg = next?` 那条**传输错误**路径。R18 在这里一行 recovery
    /// failure 都不写。
    TransportError,
    /// **regression**：握完手之后一帧都不发、连接一直开着。
    ///
    /// `next_frame_within` 在空闲上限到点时返回 `StreamPoll::IdleTimeout`
    /// ——R18 的这条分支同样只 `return Err(...)`，`ready` 一个字节没动。
    Silent,
    /// **regression**：一串**心跳**（RFC 6455 Ping），随后立刻
    /// 把 TCP 连接关掉。
    ///
    /// 引擎读到第一条 Ping 时连接还在，它写出的 Pong 因此落在一个**已经
    /// 关闭**的 socket 上 ⇒ 对端内核回 RST ⇒ 随后那几条 Pong 的写入以
    /// `ECONNRESET` 失败。这正是生产里最普通的一种写侧死法（LB 掉连接、
    /// 对端进程重启）。R19 的 `Message::Ping` 分支是裸
    /// `send_frame_within(...).await?`：`fill_stream_state` 一行都没写，
    /// `ready` 保持 1 继续放行新开仓。
    ///
    /// 一次发**一批** Ping（都落进 tungstenite 的读缓冲）是为了让"写失败"
    /// 确定性地发生在**读缓冲还没耗尽**的时候——否则读侧会先撞上 RST，
    /// 观察到的就成了 `ws_transport` 那条退出面。
    PingThenClose,
    /// ****：健康帧 + **心跳**（RFC 6455 Ping）。
    ///
    /// `Silent` 那条用例把空闲上限调到 400ms 才能在秒级里观察到
    /// `IdleTimeout`；同一个进程里的**恢复**那一半因此也受这个上限约束——
    /// 一条"发完一帧就再不说话"的流会在 400ms 后又被判死，`ready` 永远
    /// 到不了，正向断言变成不可能。真实的用户流不是这样：Binance 每 3 分钟
    /// 发一次 Ping，`next_frame_within` 的空闲计时被每一帧重置。
    /// 这条 posture 就是把那件事补上——发完健康帧之后按 150ms 发 Ping。
    /// 被测判据一层没动：Ping 走的是帧循环里既有的 `Message::Ping` 分支
    /// （回一个 Pong，不碰 readiness）。
    HealthyHeartbeat,
}

struct WsObservation {
    handshake_targets: Vec<String>,
}

async fn spawn_ws_upstream(
    posture: Arc<Mutex<WsPosture>>,
    observed: Arc<Mutex<WsObservation>>,
    connections: Arc<AtomicUsize>,
    close_requested: Arc<AtomicBool>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in WS upstream");
    let port = listener.local_addr().expect("ws addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _peer)) = listener.accept().await else {
                return;
            };
            let posture = posture.clone();
            let observed = observed.clone();
            let connections = connections.clone();
            let close_requested = close_requested.clone();
            tokio::spawn(async move {
                let target = Arc::new(Mutex::new(String::new()));
                let captured = target.clone();
                let Ok(mut stream) = tokio_tungstenite::accept_hdr_async(
                    socket,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        *captured.lock().expect("ws target") = request.uri().to_string();
                        Ok(response)
                    },
                )
                .await
                else {
                    return;
                };
                observed
                    .lock()
                    .expect("ws observation")
                    .handshake_targets
                    .push(target.lock().expect("ws target").clone());
                connections.fetch_add(1, Ordering::SeqCst);
                use futures_util::SinkExt;
                let current = *posture.lock().expect("posture");
                let frame = match current {
                    WsPosture::EndImmediately => {
                        let _ = stream.close(None).await;
                        return;
                    }
                    WsPosture::TransportError => {
                        // **不走关闭握手**：拿到底层 TCP 直接 shutdown 写侧再
                        // 丢掉整条连接。对端读到的是"连接没了"，
                        // tungstenite 按协议报 `ResetWithoutClosingHandshake`。
                        let _ = stream.get_mut().shutdown().await;
                        drop(stream);
                        return;
                    }
                    WsPosture::Silent => {
                        // 一帧都不发，连接一直开着：让引擎自己撞上空闲上限。
                        loop {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        }
                    }
                    WsPosture::MalformedBinary => {
                        let _ = stream
                            .send(tokio_tungstenite::tungstenite::Message::Binary(
                                MALFORMED_BINARY_FRAME.to_vec(),
                            ))
                            .await;
                        loop {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        }
                    }
                    WsPosture::PingThenClose => {
                        use futures_util::SinkExt as _;
                        // 125 字节是控制帧载荷的协议上限（RFC 6455 §5.5）。
                        let payload = vec![0x5au8; 125];
                        for _ in 0..PING_BURST {
                            if stream
                                .feed(tokio_tungstenite::tungstenite::Message::Ping(
                                    payload.clone(),
                                ))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        let _ = stream.flush().await;
                        // **不走关闭握手**：整条连接直接丢掉。对端此后写进来的
                        // 每一个 Pong 都撞在一个已关闭的 socket 上。
                        drop(stream);
                        return;
                    }
                    WsPosture::HealthyHeartbeat => {
                        let healthy = format!(
                            "{{\"e\":\"ORDER_TRADE_UPDATE\",\"E\":1,\"T\":2,\"o\":{{\"s\":\"{SYMBOL}\",\
                             \"c\":\"\",\"i\":{WS_ORDER_ID},\"x\":\"TRADE\",\"X\":\"FILLED\"}}}}"
                        );
                        let _ = stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(healthy))
                            .await;
                        loop {
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            if stream
                                .send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    WsPosture::Malformed => MALFORMED_FRAME.to_string(),
                    WsPosture::Healthy => format!(
                        "{{\"e\":\"ORDER_TRADE_UPDATE\",\"E\":1,\"T\":2,\"o\":{{\"s\":\"{SYMBOL}\",\
                         \"c\":\"\",\"i\":{WS_ORDER_ID},\"x\":\"TRADE\",\"X\":\"FILLED\"}}}}"
                    ),
                };
                let _ = stream
                    .send(tokio_tungstenite::tungstenite::Message::Text(frame.into()))
                    .await;
                // **这条连接可以被用例主动关掉**：
                // 关的时刻由用例挑——它先等引擎真的写下 `ready=1`，
                // 关闭之后观察到的每一件事因此都发生在 **ready 之后**。
                loop {
                    if close_requested.load(Ordering::SeqCst) {
                        let _ = stream.close(None).await;
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            });
        }
    });
    port
}

// ---------------------------------------------------------------------------

async fn seeded_sqlite(name: &str) -> String {
    sqlx::any::install_default_drivers();
    let path =
        std::env::temp_dir().join(format!("rustcore-r17wire-{name}-{}.db", std::process::id()));
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
         updated_at_ms) VALUES ('eng_r17wire', ?, 'bot_r17wire', 'idem_r17wire', 'binance', \
         'futures', ?, 'open_long', 'long', 'market', '1', 0, 'entry', 'acked', ?, \
         'coid_r17wire', 1, 1)",
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
    spawn_rustcore_with_idle_deadline(db_url, rest_port, ws_port, grpc_port, None)
}

/// 同一套生产装配，外加一个**生产同名开关** `BINANCE_WS_IDLE_TIMEOUT_MS`
/// 的覆写（`binance_futures::ws_timeouts_from_env`）。
///
/// 空闲上限的**生产默认值是 5 分钟**——一条按默认值跑的用例要等 300 秒才
/// 能观察到 `IdleTimeout`。调小的是**上限本身**（生产就是这么配的那个键），
/// 不是被测判据：帧循环、readiness 落库、gRPC 鉴权与开仓闸门一层都没动。
fn spawn_rustcore_with_idle_deadline(
    db_url: &str,
    rest_port: u16,
    ws_port: u16,
    grpc_port: u16,
    idle_timeout_ms: Option<u64>,
) -> RustcoreProcess {
    let (key_env, secret_env) = (
        format!("BINANCE_API_KEY_{}", secret_locator(CREDENTIAL)),
        format!("BINANCE_API_SECRET_{}", secret_locator(CREDENTIAL)),
    );
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"));
    let child = command
        .env("RUSTCORE_DB_URL", db_url)
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc_port}"))
        // **鉴权开着**：本进程刻意**不设** `RUSTCORE_GRPC_AUTH_DISABLED`，
        // 并配一份 token ⇒ `main()` 里 `require_auth` 为真、
        // `AuthInterceptor` 真的挂在四个服务前面。
        .env("RUSTCORE_GRPC_AUTH_TOKEN", AUTH_TOKEN)
        .env_remove("RUSTCORE_GRPC_AUTH_DISABLED")
        .env_remove("EXECUTION_TOKEN")
        .env("ENGINE_STAGE_MAX", "demo")
        .env("ENGINE_LIVE_ENABLED", "0")
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
        .env(key_env, "r17-wire-key")
        .env(secret_env, "r17-wire-secret")
        .env("RUST_LOG", "warn");
    let child = match idle_timeout_ms {
        Some(idle_timeout_ms) => {
            child.env("BINANCE_WS_IDLE_TIMEOUT_MS", idle_timeout_ms.to_string())
        }
        None => child.env_remove("BINANCE_WS_IDLE_TIMEOUT_MS"),
    };
    let child = child
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

#[derive(Debug, Clone)]
struct StreamState {
    ready: i64,
    phase: String,
    failure_reason: String,
    /// **恢复代际**（`fill_stream_state.generation`）。每一次
    /// `record_recovery_failure` 推一格；`finalize_recovery_readiness` 与
    /// `record_recovery_progress` 都不推。它因此是"这一轮里到底发生过几次
    /// 恢复失败写入"的**持久**计数——不像 `phase`，它不会被下一次写入抹掉。
    generation: i64,
}

async fn stream_state(pool: &sqlx::Pool<sqlx::Any>) -> Option<StreamState> {
    sqlx::query(
        "SELECT ready, phase, generation, COALESCE(failure_reason, '') AS failure_reason \
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
        generation: row.get::<i64, _>("generation"),
    })
}

/// 轮询到判据成立为止，并把**这段时间里观察到的整条状态轨迹**带出来
/// ——"有没有经过 `ws_parse`"正是判据之一，只看终态看不到它。
///
/// **只对「持续足够久」的状态下断言。** 引擎的一轮恢复里有些相位是
/// 几毫秒的过路状态（`disconnected_before_ready` 写完紧接着就被
/// `cycle_failed` 盖掉），跨进程按 25ms 采样注定时有时无——拿它当判据
/// 就是一条会偶发变红的用例（本轮实测：R17 起就在的
/// `a_stream_that_ends_without_frames_is_never_declared_ready` 6 次里红 1 次）。
/// 判据要落在**留得住**的那些状态上：`ws_parse` / `ws_binary` / `ws_closed`
/// 在整段回补+对账期间都留着（reader 的失败推走了恢复代际，主干后续的
/// `record_recovery_progress` 全部命中 0 行被丢弃），`cycle_failed` 则留满
/// 外层那 2 秒退避。
async fn wait_for_state(
    pool: &sqlx::Pool<sqlx::Any>,
    within: Duration,
    mut done: impl FnMut(&StreamState) -> bool,
) -> (Option<StreamState>, Vec<StreamState>) {
    let deadline = std::time::Instant::now() + within;
    let mut last: Option<StreamState> = None;
    let mut trail: Vec<StreamState> = Vec::new();
    loop {
        if let Some(state) = stream_state(pool).await {
            let changed = trail.last().is_none_or(|previous| {
                previous.phase != state.phase
                    || previous.ready != state.ready
                    || previous.failure_reason != state.failure_reason
            });
            if changed {
                trail.push(state.clone());
            }
            last = Some(state);
        }
        if last.as_ref().is_some_and(&mut done) {
            return (last, trail);
        }
        if std::time::Instant::now() >= deadline {
            return (last, trail);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// 轨迹里观察到的相位序列（相邻去重）。
fn phases_of(trail: &[StreamState]) -> Vec<String> {
    let mut phases: Vec<String> = Vec::new();
    for state in trail {
        if phases.last().map(String::as_str) != Some(state.phase.as_str()) {
            phases.push(state.phase.clone());
        }
    }
    phases
}

// ---------------------------------------------------------------------------
// 生产 gRPC 端口上的 v2 开仓（带/不带 token 两条）
// ---------------------------------------------------------------------------

fn open_long_request(idempotency_key: &str) -> pb2::ExecuteIntentsRequest {
    pb2::ExecuteIntentsRequest {
        credential_id: CREDENTIAL.to_string(),
        run_mode: "demo".to_string(),
        intents: vec![pb2::OrderIntent {
            bot_uid: "bot_r17wire".to_string(),
            run_id: "run_r17wire".to_string(),
            cycle_id: "cyc_r17wire".to_string(),
            idempotency_key: idempotency_key.to_string(),
            client_order_id_short: "coid_r17open".to_string(),
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
    }
}

async fn execute_intents(
    grpc_port: u16,
    idempotency_key: &str,
    token: Option<&str>,
) -> Result<pb2::ExecuteIntentsResponse, tonic::Status> {
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("grpc endpoint")
        .connect()
        .await
        .expect("connect to the production gRPC port");
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("grpc service ready");
    let mut request = tonic::Request::new(open_long_request(idempotency_key));
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("bearer metadata"),
        );
    }
    let codec = tonic::codec::ProstCodec::<pb2::ExecuteIntentsRequest, pb2::ExecuteIntentsResponse>::default();
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(
        "/execution_engine.v2.EngineExecution/ExecuteIntents",
    );
    client
        .unary(request, path, codec)
        .await
        .map(tonic::Response::into_inner)
}

/// A ready=1 observation and the production admission check can straddle one
/// more recovery generation under host load. Retry the same idempotent intent
/// until that bounded scheduler race clears; every caller still asserts that
/// the real production gate no longer reports FILL_RECOVERY_NOT_READY.
async fn wait_until_recovery_gate_allows(grpc_port: u16, idempotency_key: &str) -> pb2::IntentAck {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let allowed = execute_intents(grpc_port, idempotency_key, Some(AUTH_TOKEN))
            .await
            .expect("an authenticated call must reach the engine");
        let ack = allowed.acks.into_iter().next().expect("exactly one ack");
        if ack.reject_reason_code != "FILL_RECOVERY_NOT_READY" {
            return ack;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the healthy stream never produced an observable ready generation; message={}",
            ack.message
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_frame_from_the_real_binary_drops_readiness_and_blocks_new_entries() {
    // **feature 开启时拒绝记证。**
    //
    // `l4-fault-injection` 编译下 `ensure_running` 有一条假交易所 early-return，
    // 而 `l4_file_rpc::serve_if_configured` 还能整段替掉 gRPC server。这条用例
    // 声称的是「生产二进制跑的就是 `run_once` 与生产 gRPC 装配」——那个前提在
    // 该 feature 下**证不出来**，因此这次运行不得被当成 wiring proof。
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled: \
         that build carries a fake-exchange early return and a file-RPC server that can \
         replace the very layers this case claims are not bypassed. Run it on the default \
         feature set (`cargo test`), which is what the gate runs."
    );

    let posture = Arc::new(Mutex::new(WsPosture::Malformed));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let close_requested = Arc::new(AtomicBool::new(false));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        close_requested.clone(),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("parse").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- 畸形帧 ⇒ ws_parse / ready=0 -----------------------------------
    let (not_ready, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.ready == 0
            && state
                .failure_reason
                .contains("unknown event type listenKeyExpired")
    })
    .await;
    let phases = phases_of(&trail);
    let not_ready = not_ready.expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.ready, 0,
        "a frame the engine could not parse must not leave the handoff ready: {not_ready:?}"
    );
    assert!(
        not_ready
            .failure_reason
            .contains("unknown event type listenKeyExpired"),
        "the recorded reason must be the parse failure itself: {not_ready:?}"
    );
    assert!(
        phases.iter().any(|phase| phase == "ws_parse"),
        "the recovery cycle must pass through the ws_parse phase; saw {phases:?}"
    );

    // ---- 连接重建 --------------------------------------------------------
    let rebuilt = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = connections.load(Ordering::SeqCst);
            if seen >= 2 || std::time::Instant::now() >= deadline {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        rebuilt >= 2,
        "a stream that failed on an unparsable frame must be rebuilt; saw {rebuilt} connections"
    );
    {
        let targets = &ws_observation
            .lock()
            .expect("ws observation")
            .handshake_targets;
        assert!(
            targets
                .iter()
                .all(|target| target == &format!("/ws/{LISTEN_KEY}")),
            "every reconnect must carry the listenKey this test's REST端 minted: {targets:?}"
        );
    }

    // ---- 鉴权层真的在链路上（反向）--------------------------------------
    let unauthenticated = execute_intents(grpc_port, "idem_r17wire_noauth", None)
        .await
        .expect_err("a request without a token must not reach the engine");
    assert_eq!(
        unauthenticated.code(),
        tonic::Code::Unauthenticated,
        "the production gRPC entry point must be behind the auth interceptor: {unauthenticated:?}"
    );

    // ---- 带 token 的同一条 intent 被 readiness 闸门挡住 -------------------
    let blocked = execute_intents(grpc_port, "idem_r17wire_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.status,
        pb2::Status::Rejected as i32,
        "message={}",
        ack.message
    );
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "an unparsable truth frame must block new entries; message={}",
        ack.message
    );

    // ---- 正向：交易所侧改推健康帧 ⇒ 引擎自己恢复 -------------------------
    *posture.lock().expect("posture") = WsPosture::Healthy;
    let (ready, _) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    let ready = ready.expect("the fill stream state row must still exist");
    assert_eq!(
        ready.ready, 1,
        "a healthy stream plus a converged reconcile must declare the handoff ready: {ready:?}"
    );
    let applied = sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
        .bind("eng_r17wire")
        .fetch_one(&pool)
        .await
        .expect("read the seeded local order")
        .get::<String, _>("status");
    assert_eq!(
        applied, "filled",
        "the healthy websocket frame must have reached the production sink"
    );

    let ack = wait_until_recovery_gate_allows(grpc_port, "idem_r17wire_ready").await;
    assert_ne!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={}",
        ack.message
    );

    pool.close().await;
}

/// **一条握完手就断掉的流不得被宣布 ready。**
///
/// 这是 readiness 判据里**代际 CAS 挡不住**的那一半：reader 读到流正常结束
/// （`StreamPoll::Ended`）时返回 `Ok(())`，**不**写 recovery failure，
/// 于是恢复代际一格都没被推走——`finalize_recovery_readiness` 的 CAS 会命中。
/// 挡住它的是 `run_once` 里那次 `tasks.reader_finished()` 采样：第五段判据
/// 必须真的取自"reader 还活着吗"，不能写死成 `Succeeded`。
///
/// 两条判据一起才完整：**失败**的 reader 由代际 CAS 挡（上一条用例），
/// **静默结束**的 reader 由这次采样挡。少任何一条，一条已经断掉的用户流
/// 都会被写成 `ready=1` 并继续放行新开仓（engine safety contract「运行状态丢失」）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_that_ends_without_frames_is_never_declared_ready() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::EndImmediately));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("ended").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // **判据落在留得住的状态上**（见 `wait_for_state` 的说明）。
    // `disconnected_before_ready` 只在库里存在几毫秒——写完紧接着就被外层的
    // `cycle_failed` 盖掉——按 25ms 跨进程采样注定时有时无（R17 起这条断言
    // 就是偶发红：本轮实测 6 次里红 1 次）。同一件事有一个**留满外层 2 秒
    // 退避**的载体：`cycle_failed` 那一行的 `failure_reason` 就是
    // `run_once` 抛出的那句 "user stream disconnected before ready"。
    let (state, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.failure_reason.contains("disconnected before ready")
    })
    .await;
    let state = state.expect("the production binary must write a fill stream state row");
    let phases = phases_of(&trail);
    assert_eq!(
        state.ready, 0,
        "a user stream that ended without a single frame must never be ready: {state:?}"
    );
    assert!(
        trail
            .iter()
            .any(|state| state.failure_reason.contains("disconnected before ready")),
        "the cycle must report that the stream died before readiness; saw {trail:?}"
    );
    // **断流是 reader 自己
    // 记下来的。**
    //
    // R17 的 `Message::Close` / `StreamPoll::Ended` 是裸 `break`：库里从头到尾
    // 不会出现 `ws_closed` 这个相位，降 readiness 全靠外层 `run_once` 事后
    // 补写。这个相位是**留得住**的——reader 的失败写入推走了恢复代际，
    // 主干随后每一步 `record_recovery_progress` 都命中 0 行被丢弃
    // （`fill_replay::record_recovery_progress` 的绑代谓词），于是相位一直
    // 停在 `ws_closed` 直到这一轮收尾（整段回补 + 对账，本夹具里约 1 秒）。
    assert!(
        phases.iter().any(|phase| phase == "ws_closed"),
        "the reader itself must record the venue-initiated close before it returns; \
         saw {phases:?}"
    );
    // 反向：一次都没有 ready 过（不是"先 ready 又掉下来"）。
    let (still_not_ready, _) =
        wait_for_state(&pool, Duration::from_secs(8), |state| state.ready == 1).await;
    assert_eq!(
        still_not_ready.expect("state row").ready,
        0,
        "no recovery cycle may declare readiness while the stream keeps dying"
    );

    let blocked = execute_intents(grpc_port, "idem_r17ended_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "a dead user stream must keep blocking new entries; message={}",
        ack.message
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// 第十四轮 §9.4「WS 全 frame fail-closed」的生产入口 wiring proof
//
// 两条帧形态各一条，与上面两条用例共用同一套装配：真实生产二进制
// （`CARGO_BIN_EXE_rustcore`）、**鉴权开着**的生产 gRPC 入口、
// 同一份 listenKey 真值同源、以及"判据不通过时真的被挡住"的反向断言。
// ---------------------------------------------------------------------------

/// **regression（High）· 真实生产二进制收到一条读不懂的 Binary 帧时
/// 必须降 readiness、重连，并把新开仓挡在生产 gRPC 入口外。**
///
/// R17 的帧循环最后一臂是 `_ => {}`：Binary 不解析、不写 failure、不重连。
/// 于是这条流会安安静静地走完回补与对账、写下 `ready=1`，同一条 intent
/// 拿不到 `FILL_RECOVERY_NOT_READY`——一条**可能刚刚丢掉一笔成交真值**的
/// 用户流照样准入新敞口（engine safety contract「运行状态丢失」）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_binary_frame_from_the_real_binary_drops_readiness_and_blocks_new_entries() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::MalformedBinary));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("binary").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- 读不懂的 Binary ⇒ ws_binary / ready=0 --------------------------
    let (not_ready, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.ready == 0 && state.failure_reason.contains("not valid UTF-8")
    })
    .await;
    let phases = phases_of(&trail);
    let not_ready = not_ready.expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.ready, 0,
        "a binary frame the engine could not read must not leave the handoff ready: {not_ready:?}"
    );
    assert!(
        not_ready.failure_reason.contains("not valid UTF-8"),
        "the recorded reason must be the undecodable binary frame itself: {not_ready:?}"
    );
    assert!(
        phases.iter().any(|phase| phase == "ws_binary"),
        "the recovery cycle must pass through the ws_binary phase; saw {phases:?}"
    );

    // ---- 连接重建（fail-closed 不是"停在那儿"）--------------------------
    let rebuilt = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = connections.load(Ordering::SeqCst);
            if seen >= 2 || std::time::Instant::now() >= deadline {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        rebuilt >= 2,
        "a stream that failed on an unreadable binary frame must be rebuilt; saw {rebuilt} \
         connections"
    );

    // ---- 鉴权层真的在链路上（反向）--------------------------------------
    let unauthenticated = execute_intents(grpc_port, "idem_r18bin_noauth", None)
        .await
        .expect_err("a request without a token must not reach the engine");
    assert_eq!(
        unauthenticated.code(),
        tonic::Code::Unauthenticated,
        "the production gRPC entry point must be behind the auth interceptor: {unauthenticated:?}"
    );

    // ---- 带 token 的同一条 intent 被 readiness 闸门挡住 -------------------
    let blocked = execute_intents(grpc_port, "idem_r18bin_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "an unreadable binary frame must block new entries; message={}",
        ack.message
    );

    // ---- 正向：交易所侧改推健康 Text 帧 ⇒ 引擎自己恢复 -------------------
    *posture.lock().expect("posture") = WsPosture::Healthy;
    let (ready, _) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    assert_eq!(
        ready.expect("state row").ready,
        1,
        "a healthy stream plus a converged reconcile must declare the handoff ready"
    );
    let ack = wait_until_recovery_gate_allows(grpc_port, "idem_r18bin_ready").await;
    assert_ne!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={}",
        ack.message
    );

    pool.close().await;
}

/// **regression（High）· 一条**已经 ready** 的用户流被交易所关掉时，
/// not-ready 必须在 reader 返回之前落库。**
///
/// 这是四条帧形态里唯一一条**必须先 ready、再断**的：R17 的 Close/EOF 分支
/// 是裸 `break`，降 readiness 要等外层 `run_once` 走完 `join_reader()` 再写
/// `disconnected`——中间隔着一次任务 join、一次 `load_fill_stream_state`
/// 与一整个写事务。这段窗口里库里的 `ready` 仍是 1，而 `risk.rs` 正是拿它
/// 放行新开仓。
///
/// 用例因此不看终态（终态两种实现都会到 `ready=0`），而看**相位序列**：
/// 引擎必须先经过 `ready`，随后经过 `ws_closed`。`ws_closed` 只可能由
/// reader 自己在返回前写下——R17 的实现里这个相位一次都不会出现。
///
/// 关闭之后本用例把回补掐断（`broken_backfill`），重连的每一轮都停在
/// `backfill` 失败上：`ready` 不会被下一轮恢复重新写回，后面那条
/// "新开仓被挡"的断言因此是确定性的，不是撞运气。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_after_ready_drops_readiness_before_the_reader_returns() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::Healthy));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let close_requested = Arc::new(AtomicBool::new(false));
    let broken_backfill = Arc::new(AtomicBool::new(false));
    let rest_port = spawn_rest_upstream(broken_backfill.clone()).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        close_requested.clone(),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("closed").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- 先真的 ready：这条断言之后的一切都发生在 ready 之后 ------------
    let (ready, ready_trail) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    let ready_phases = phases_of(&ready_trail);
    let ready_state = ready.expect("state row");
    assert_eq!(
        ready_state.ready, 1,
        "the fixture must reach a real ready state, otherwise the window under test does not \
         exist; saw {ready_phases:?}"
    );

    // ---- 交易所关掉这条连接；重连之后回补一直失败，不会再 ready ---------
    broken_backfill.store(true, Ordering::SeqCst);
    close_requested.store(true, Ordering::SeqCst);

    let (disconnected, trail) = wait_for_state(&pool, Duration::from_secs(60), |state| {
        state.phase == "disconnected"
    })
    .await;
    let phases = phases_of(&trail);
    let disconnected = disconnected.expect("the fill stream state row must still exist");
    assert_eq!(
        disconnected.ready, 0,
        "a closed user stream must not stay ready: {disconnected:?}"
    );

    // **判据：那一格多出来的恢复代际。**
    //
    // `phase` 是单行状态，reader 写下 `ws_closed` 到外层补写 `disconnected`
    // 之间只隔着一次任务 join——正是本轮要消灭的那个窗口，它现在短到
    // 跨进程轮询采不到（采得到反而说明窗口还很宽）。**代际号不会被抹掉**：
    // 每一次 `record_recovery_failure` 推一格，而 `finalize_recovery_readiness`
    // 与 `record_recovery_progress` 一格都不推。
    //
    // 于是从 `ready` 到 `disconnected` 之间的代际增量就是"这段路上发生过
    // 几次恢复失败写入"：
    //
    // * R17（裸 `break`）：只有外层那一次 `disconnected` ⇒ **+1**；
    // * 修法后：reader 返回前先写 `ws_closed`，外层再写 `disconnected`
    //   ⇒ **+2**。
    //
    // 多出来的那一格就是"reader 在返回之前把 not-ready 落了库"这件事本身，
    // 而它同时推走了恢复代际——主干任何一次还在飞的 ready 写入都会 CAS 落空
    // （`fill_replay::finalize_recovery_readiness`）。相位名字由
    // `binance_user_stream::tests::a_close_frame_drops_readiness_before_the_reader_returns`
    // 在同一段生产代码上直接钉住。
    let ready_generation = ready_state.generation;
    assert_eq!(
        disconnected.generation - ready_generation,
        2,
        "a venue-initiated close must be recorded by the reader itself before it returns \
         (one recovery-failure write) and by the main line afterwards (a second one); \
         generations went {ready_generation} -> {}, phases {phases:?}",
        disconnected.generation
    );

    // 观察窗口内如果真的采到了那个相位，它必须就是 `ws_closed`。
    assert!(
        phases
            .iter()
            .all(|phase| phase != "ws_parse" && phase != "ws_binary"),
        "a clean venue close must not be reported as a parse or binary failure: {phases:?}"
    );

    // ---- 新开仓在生产 gRPC 入口上被挡（带 token）-------------------------
    let blocked = execute_intents(grpc_port, "idem_r18close_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "a closed user stream must block new entries; message={}",
        ack.message
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// 第十五轮 §8.4「WS 所有 reader 退出面先 not-ready」的生产入口
// wiring proof
//
// 两条**新**退出面（transport `Err` / IdleTimeout）各一条，与上面四条用例
// 共用同一套装配：真实生产二进制（`CARGO_BIN_EXE_rustcore`）、**鉴权开着**
// 的生产 gRPC 入口、同一份 listenKey 真值同源、以及"判据不通过时真的被
// 挡住"的反向断言（无 token ⇒ `UNAUTHENTICATED`；带 token ⇒
// `FILL_RECOVERY_NOT_READY`）。
// ---------------------------------------------------------------------------

/// **regression（High）· 真实生产二进制的用户流传输断掉时必须降 readiness、
/// 重连，并把新开仓挡在生产 gRPC 入口外。**
///
/// R18 的帧循环在这条路径上是一句裸 `let msg = next?`：TLS 断开、协议错、
/// 连接被中间设备掐掉——tungstenite 报的每一种传输故障都直接弹出去，
/// `fill_stream_state` 一行都不写。降 readiness 于是要等外层
/// `run_once` join 完 reader 才补写 `disconnected`，而那一步在
/// `reader_finished()` 采样与 ready CAS 之间根本来不及。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_failure_from_the_real_binary_drops_readiness_and_blocks_new_entries() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::TransportError));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("transport").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- 传输故障 ⇒ ws_transport / ready=0 ------------------------------
    let (not_ready, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.ready == 0 && state.phase == "ws_transport"
    })
    .await;
    let phases = phases_of(&trail);
    let not_ready = not_ready.expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.ready, 0,
        "a user stream whose transport failed must not leave the handoff ready: {not_ready:?}"
    );
    assert!(
        phases.iter().any(|phase| phase == "ws_transport"),
        "the reader itself must record the transport failure before it returns; saw {phases:?}"
    );
    assert!(
        trail
            .iter()
            .any(|state| state.failure_reason.contains("transport error")),
        "the recorded reason must be the transport failure itself; saw {trail:?}"
    );

    // ---- 从来没有 ready 过（不是"先 ready 又掉下来"）---------------------
    let (still_not_ready, _) =
        wait_for_state(&pool, Duration::from_secs(8), |state| state.ready == 1).await;
    assert_eq!(
        still_not_ready.expect("state row").ready,
        0,
        "no recovery cycle may declare readiness while the transport keeps failing"
    );

    // ---- 连接重建（fail-closed 不是"停在那儿"）--------------------------
    let rebuilt = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = connections.load(Ordering::SeqCst);
            if seen >= 2 || std::time::Instant::now() >= deadline {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        rebuilt >= 2,
        "a stream whose transport failed must be rebuilt; saw {rebuilt} connections"
    );

    // ---- 鉴权层真的在链路上（反向）--------------------------------------
    let unauthenticated = execute_intents(grpc_port, "idem_r19tr_noauth", None)
        .await
        .expect_err("a request without a token must not reach the engine");
    assert_eq!(
        unauthenticated.code(),
        tonic::Code::Unauthenticated,
        "the production gRPC entry point must be behind the auth interceptor: {unauthenticated:?}"
    );

    // ---- 带 token 的同一条 intent 被 readiness 闸门挡住 -------------------
    let blocked = execute_intents(grpc_port, "idem_r19tr_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "a user stream whose transport failed must block new entries; message={}",
        ack.message
    );

    // ---- 正向：交易所侧改推健康 Text 帧 ⇒ 引擎自己恢复 -------------------
    *posture.lock().expect("posture") = WsPosture::Healthy;
    let (ready, _) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    assert_eq!(
        ready.expect("state row").ready,
        1,
        "a healthy stream plus a converged reconcile must declare the handoff ready"
    );
    let ack = wait_until_recovery_gate_allows(grpc_port, "idem_r19tr_ready").await;
    assert_ne!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={}",
        ack.message
    );

    pool.close().await;
}

/// **regression（High）· 真实生产二进制的用户流静默超过空闲上限时必须降
/// readiness、重连，并把新开仓挡在生产 gRPC 入口外。**
///
/// 与上一条同根、不同退出面：R18 的 `StreamPoll::IdleTimeout` 分支只
/// `return Err(...)`。一条握完手就再也不说话的流在 R18 下会一路走到
/// `ready=1`（回补与对账都不依赖 WS 帧），直到空闲上限到点、reader 才报错
/// 退出——中间那段 `ready=1` 的窗口里 `risk.rs` 照常放行新开仓。
///
/// 空闲上限用生产同名开关 `BINANCE_WS_IDLE_TIMEOUT_MS` 调到 400ms
/// （生产默认 300_000ms）：reader 因此**必定**早于回补完成
/// （交易所替身的 `/fapi/v2/positionRisk` 慢 900ms）就失败，
/// "从来没有 ready 过"这条断言是确定性的，不是撞运气。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_stream_from_the_real_binary_drops_readiness_and_blocks_new_entries() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::Silent));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("idle").await;

    let _child =
        spawn_rustcore_with_idle_deadline(&db_url, rest_port, ws_port, grpc_port, Some(400));
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- 空闲超时 ⇒ ws_idle_timeout / ready=0 ---------------------------
    let (not_ready, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.ready == 0 && state.phase == "ws_idle_timeout"
    })
    .await;
    let phases = phases_of(&trail);
    let not_ready = not_ready.expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.ready, 0,
        "a user stream that went silent must not leave the handoff ready: {not_ready:?}"
    );
    assert!(
        phases.iter().any(|phase| phase == "ws_idle_timeout"),
        "the reader itself must record the idle deadline before it returns; saw {phases:?}"
    );
    assert!(
        trail
            .iter()
            .any(|state| state.failure_reason.contains("idle beyond 400ms")),
        "the recorded reason must name the deadline that was breached; saw {trail:?}"
    );

    // ---- 从来没有 ready 过 ----------------------------------------------
    let (still_not_ready, _) =
        wait_for_state(&pool, Duration::from_secs(8), |state| state.ready == 1).await;
    assert_eq!(
        still_not_ready.expect("state row").ready,
        0,
        "no recovery cycle may declare readiness while the stream keeps going silent"
    );

    // ---- 连接重建 --------------------------------------------------------
    let rebuilt = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = connections.load(Ordering::SeqCst);
            if seen >= 2 || std::time::Instant::now() >= deadline {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        rebuilt >= 2,
        "a stream that went silent past its idle deadline must be rebuilt; saw {rebuilt} \
         connections"
    );

    // ---- 鉴权层真的在链路上（反向）--------------------------------------
    let unauthenticated = execute_intents(grpc_port, "idem_r19idle_noauth", None)
        .await
        .expect_err("a request without a token must not reach the engine");
    assert_eq!(
        unauthenticated.code(),
        tonic::Code::Unauthenticated,
        "the production gRPC entry point must be behind the auth interceptor: {unauthenticated:?}"
    );

    // ---- 带 token 的同一条 intent 被 readiness 闸门挡住 -------------------
    let blocked = execute_intents(grpc_port, "idem_r19idle_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "a silent user stream must block new entries; message={}",
        ack.message
    );

    // ---- 正向：交易所侧改推健康帧 + 心跳 ⇒ 引擎自己恢复 ------------------
    *posture.lock().expect("posture") = WsPosture::HealthyHeartbeat;
    let (ready, _) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    assert_eq!(
        ready.expect("state row").ready,
        1,
        "a healthy stream plus a converged reconcile must declare the handoff ready"
    );
    let ack = wait_until_recovery_gate_allows(grpc_port, "idem_r19idle_ready").await;
    assert_ne!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={}",
        ack.message
    );

    pool.close().await;
}

/// **regression（High）· 真实生产二进制答不出 venue 的 Ping 时必须降
/// readiness、重连，并把新开仓挡在生产 gRPC 入口外。**
///
/// 与 `a_transport_failure_...` / `a_silent_stream_...` 同根、不同退出面：
/// R19 的 `Message::Ping` 分支是裸 `send_frame_within(...).await?`。写侧一旦
/// 废掉（对端把连接掐了），reader 报错退出而 `fill_stream_state` 一行都没写
/// ——`run_once` 主干那次 `tasks.reader_finished()` 采样若发生在失败之前，
/// 三段判据全是 `Succeeded`、恢复代际一格没被推走，那次 ready 写入的 CAS
/// **命中**，库里出现一段 `ready=1`，而 `risk.rs` 正是拿这一位放行新开仓。
///
/// 注入点是**心跳**本身：交易所替身握完手推一批 Ping，随后立刻关掉 TCP
/// 连接。引擎从读缓冲里逐条取 Ping、逐条回 Pong，第一条 Pong 撞上已关闭的
/// socket 触发 RST，之后的 Pong 写入以 `ECONNRESET` 失败——`ws_pong_write`。
///
/// 按 engine safety contract 的四条与本文件头部一致：入口是 `CARGO_BIN_EXE_rustcore`
/// 与生产 gRPC 端口上的 `ExecuteIntents`；鉴权开着并反向证明；listenKey 由
/// 本用例 REST 端产出、引擎消费并在握手里读回；反向断言是同一条 intent 先被
/// `FILL_RECOVERY_NOT_READY` 挡住、恢复之后不再被该判据挡。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pong_write_failure_from_the_real_binary_drops_readiness_and_blocks_new_entries() {
    assert!(
        !cfg!(feature = "l4-fault-injection"),
        "this wiring proof refuses to certify a build with `l4-fault-injection` enabled"
    );

    let posture = Arc::new(Mutex::new(WsPosture::PingThenClose));
    let ws_observation = Arc::new(Mutex::new(WsObservation {
        handshake_targets: Vec::new(),
    }));
    let connections = Arc::new(AtomicUsize::new(0));
    let rest_port = spawn_rest_upstream(Arc::new(AtomicBool::new(false))).await;
    let ws_port = spawn_ws_upstream(
        posture.clone(),
        ws_observation.clone(),
        connections.clone(),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("pongwrite").await;

    let _child = spawn_rustcore(&db_url, rest_port, ws_port, grpc_port);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");

    // ---- Pong 写失败 ⇒ ws_pong_write / ready=0 ---------------------------
    let (not_ready, trail) = wait_for_state(&pool, Duration::from_secs(90), |state| {
        state.ready == 0 && state.phase == "ws_pong_write"
    })
    .await;
    let phases = phases_of(&trail);
    let not_ready = not_ready.expect("the production binary must write a fill stream state row");
    assert_eq!(
        not_ready.phase, "ws_pong_write",
        "the reader itself must record the pong write failure before it returns; saw {phases:?}"
    );
    assert_eq!(
        not_ready.ready, 0,
        "a user stream whose pong could not be written must not leave the handoff ready: \
         {not_ready:?}"
    );
    assert!(
        trail.iter().any(|state| {
            state
                .failure_reason
                .contains("could not answer a venue ping")
        }),
        "the recorded reason must be the pong write failure itself; saw {trail:?}"
    );

    // ---- 从来没有 ready 过（不是"先 ready 又掉下来"）---------------------
    let (still_not_ready, _) =
        wait_for_state(&pool, Duration::from_secs(8), |state| state.ready == 1).await;
    assert_eq!(
        still_not_ready.expect("state row").ready,
        0,
        "no recovery cycle may declare readiness while the stream cannot be written to"
    );

    // ---- 连接重建（fail-closed 不是"停在那儿"）--------------------------
    let rebuilt = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let seen = connections.load(Ordering::SeqCst);
            if seen >= 2 || std::time::Instant::now() >= deadline {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        rebuilt >= 2,
        "a stream whose pong write failed must be rebuilt; saw {rebuilt} connections"
    );

    // ---- 鉴权层真的在链路上（反向）--------------------------------------
    let unauthenticated = execute_intents(grpc_port, "idem_r20pong_noauth", None)
        .await
        .expect_err("a request without a token must not reach the engine");
    assert_eq!(
        unauthenticated.code(),
        tonic::Code::Unauthenticated,
        "the production gRPC entry point must be behind the auth interceptor: {unauthenticated:?}"
    );

    // ---- 带 token 的同一条 intent 被 readiness 闸门挡住 -------------------
    let blocked = execute_intents(grpc_port, "idem_r20pong_blocked", Some(AUTH_TOKEN))
        .await
        .expect("an authenticated call must reach the engine");
    let ack = blocked.acks.into_iter().next().expect("exactly one ack");
    assert_eq!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "a user stream we cannot write to must block new entries; message={}",
        ack.message
    );

    // ---- 正向：交易所侧改推健康帧 ⇒ 引擎自己恢复 -------------------------
    *posture.lock().expect("posture") = WsPosture::Healthy;
    let (ready, _) =
        wait_for_state(&pool, Duration::from_secs(120), |state| state.ready == 1).await;
    assert_eq!(
        ready.expect("state row").ready,
        1,
        "a healthy stream plus a converged reconcile must declare the handoff ready"
    );
    let ack = wait_until_recovery_gate_allows(grpc_port, "idem_r20pong_ready").await;
    assert_ne!(
        ack.reject_reason_code, "FILL_RECOVERY_NOT_READY",
        "once recovery is ready the same intent must no longer hit this gate; message={}",
        ack.message
    );

    pool.close().await;
}
