//! **paper 凭据隔离（2026-09-06 · 生产缺陷，High）· 进程启动 wiring 证明。**
//!
//! 缺陷链：`fill_replay::discover_fill_recovery_scopes` 的 SQL 只排除字面量
//! `'paper'`，canonical 模拟凭据 `paper:<32位小写hex>` 从持久化 scopes /
//! cursors / fills / engine orders 一路通过，直达生产启动入口
//! `EngineExecutionService::start_fill_recovery_on_startup` →
//! `BinanceUserStreamManager::start_discovered`。`keys_from_env` 对模拟凭据
//! 必然 `Ok(None)`，于是**每次进程启动**都会为一条模拟凭据写
//! `startup_credentials` 失败并推进恢复代际——模拟数据在真实恢复域里制造
//! 持续的失败噪声与代际抖动（paper/live 混淆按 engine safety contract 属 High）。
//!
//! 按 engine safety contract 的四条办：
//!
//! * **入口同一**：跑的是 `CARGO_BIN_EXE_rustcore`——部署时执行的那个二进制，
//!   走 `main()` 全程（建库/迁移/启动复核 → `start_fill_recovery_on_startup`
//!   → gRPC serve）。不直接调 `discover_fill_recovery_scopes` 或任何内部 helper。
//! * **不跳层**：启动复核（`Store::connect`）、恢复域发现、`keys_from_env`、
//!   listenKey REST 创建、WSS 握手、REST 逐页回补、对账与 readiness 落库，
//!   一层都没绕。
//! * **两侧真值同源**：paper 记录在**启动之前**种进同一个库文件（引擎真实
//!   消费的就是这份库）；listenKey 由本用例的 REST 端产出、引擎消费，用例在
//!   WS 握手路径里逐字读回。
//! * **反向可断言**：把判据删掉（本用例提交前的形态），同样的夹具会让引擎
//!   为 paper 凭据写下 `fill_stream_state` 失败行——本用例断言该行**不存在**，
//!   且两次启动都不存在（无失败、无代际 churn）；同库同形状的交易所凭据
//!   （正域对照）照常完成真实启动并收敛 ready。
//!
//! **替身缝的位置**：只有交易所本身被换成本机监听端口（生产同名开关
//! `BINANCE_FUTURES_BASE_URL` / `BINANCE_FUTURES_WS_BASE_URL`），凭据是
//! 假密钥；这两条缝都在被证明的链路之外。存量 paper 行**原样保留**
//! （不删除、不迁移——用例末尾逐表断言仍在库里）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::any::AnyPoolOptions;
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 交易所身份的正域对照凭据（`credential_kind` = `ExchangeBacked`）。
const LIVE_CREDENTIAL: &str = "iso.exchange.v1";
/// canonical Product Demo 模拟凭据（与 `tests/fixtures/product-demo-credential-
/// identities.json` 同形）。
const CANONICAL_PAPER: &str = "paper:7b6f4495fde3706c999256af2985a8f4";
/// 保留命名空间下的畸形串：31 位 hex，差一位（`UnprovenIdentity`）。
const MALFORMED_PAPER: &str = "paper:7b6f4495fde3706c999256af2985a8f";
const SYMBOL: &str = "BTCUSDT";
const LISTEN_KEY: &str = "iso-paper-listen-key";

fn json_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// 收敛形态的交易所替身：listenKey 正常发放，userTrades / openOrders /
/// openAlgoOrders / positionRisk / balance 全部为空 ⇒ 首轮对账即可收敛 ready。
fn rest_body(method: &str, path: &str) -> String {
    match (method, path) {
        ("POST", "/fapi/v1/listenKey") | ("PUT", "/fapi/v1/listenKey") => {
            format!("{{\"listenKey\":\"{LISTEN_KEY}\"}}")
        }
        ("GET", "/fapi/v1/userTrades") => "[]".to_string(),
        ("GET", "/fapi/v1/openOrders") => "[]".to_string(),
        ("GET", "/fapi/v1/openAlgoOrders") => "[]".to_string(),
        ("GET", "/fapi/v2/positionRisk") => "[]".to_string(),
        ("GET", "/fapi/v2/balance") => "[]".to_string(),
        _ => "{}".to_string(),
    }
}

#[derive(Default)]
struct Upstream {
    listen_key_posts: AtomicUsize,
    handshakes: AtomicUsize,
    handshake_paths: Mutex<Vec<String>>,
}

async fn spawn_rest_upstream(upstream: Arc<Upstream>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the loopback REST upstream");
    let port = listener.local_addr().expect("rest addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _peer)) = listener.accept().await else {
                return;
            };
            let upstream = upstream.clone();
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
                let mut parts = request.lines().next().unwrap_or("").split_whitespace();
                let method = parts.next().unwrap_or("GET").to_string();
                let target = parts.next().unwrap_or("/").to_string();
                let path = target.split('?').next().unwrap_or("/").to_string();
                if method == "POST" && path == "/fapi/v1/listenKey" {
                    upstream.listen_key_posts.fetch_add(1, Ordering::SeqCst);
                }
                let body = rest_body(&method, &path);
                let _ = socket.write_all(json_response(&body).as_bytes()).await;
                let _ = socket.flush().await;
            });
        }
    });
    port
}

/// 握手路径逐字读回（listenKey 两侧同源证据），随后把连接握住——
/// reader 存活正是 readiness 的第五段判据。
async fn spawn_ws_upstream(upstream: Arc<Upstream>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the loopback WS upstream");
    let port = listener.local_addr().expect("ws addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _peer)) = listener.accept().await else {
                return;
            };
            let upstream = upstream.clone();
            tokio::spawn(async move {
                let captured = Arc::new(Mutex::new(String::new()));
                let target = captured.clone();
                let stream = match tokio_tungstenite::accept_hdr_async(
                    socket,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        *target.lock().expect("ws target") = request.uri().to_string();
                        Ok(response)
                    },
                )
                .await
                {
                    Ok(stream) => stream,
                    Err(_) => return,
                };
                upstream.handshakes.fetch_add(1, Ordering::SeqCst);
                upstream
                    .handshake_paths
                    .lock()
                    .expect("handshake paths")
                    .push(captured.lock().expect("ws target").clone());
                let mut stream = stream;
                use futures_util::StreamExt;
                loop {
                    // 不推任何行情帧：reader 存活本身就够了；只回 Pong。
                    match stream.next().await {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                            use futures_util::SinkExt;
                            let _ = stream
                                .send(tokio_tungstenite::tungstenite::Message::Pong(Vec::new()))
                                .await;
                        }
                        Some(Ok(_)) => {}
                        _ => return,
                    }
                }
            });
        }
    });
    port
}

/// **启动之前**把 paper 记录种进库：这正是生产缺陷的存量形态——已证明形状的
/// 恢复域行 + 运行期形状的 paper fills + paper 引擎单。另有一条同形状的
/// 交易所凭据行作为正域对照。
async fn seeded_sqlite(name: &str) -> String {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!(
        "rustcore-paper-iso-{name}-{}-{}.db",
        std::process::id(),
        ulid::Ulid::new()
    ));
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
    for credential_id in [LIVE_CREDENTIAL, "paper", CANONICAL_PAPER, MALFORMED_PAPER] {
        sqlx::query(
            "INSERT INTO fill_recovery_scopes (credential_id, symbol, registered_at_ms, \
             updated_at_ms, scope_known, scope_proved_at_ms) VALUES (?, ?, 1, 1, 1, 1)",
        )
        .bind(credential_id)
        .bind(SYMBOL)
        .execute(&pool)
        .await
        .expect("seed a proved fill recovery scope");
    }
    sqlx::query(
        "INSERT INTO fill_replay_cursors (credential_id, symbol, last_trade_id, \
         last_success_at_ms, updated_at_ms, scope_known, scope_proved_at_ms) \
         VALUES (?, 'ETHUSDT', 7, 1, 1, 1, 1)",
    )
    .bind(CANONICAL_PAPER)
    .execute(&pool)
    .await
    .expect("seed a proved paper replay cursor");
    for (credential_id, trade_id, symbol) in [
        ("paper", "paper-fill-1", "ETHUSDT"),
        (CANONICAL_PAPER, "paper-fill-2", "SOLUSDT"),
    ] {
        sqlx::query(
            "INSERT INTO engine_fills (credential_id, symbol_identity, symbol_scope_known, \
             symbol_scope_proved_at_ms, exchange_trade_id, engine_order_id, exchange, \
             market_kind, symbol_key, symbol, created_at_ms) \
             VALUES (?, ?, 1, 1, ?, 'paper-order', 'binance', 'futures', NULL, ?, 1)",
        )
        .bind(credential_id)
        .bind(symbol)
        .bind(trade_id)
        .bind(symbol)
        .execute(&pool)
        .await
        .expect("seed a born-proved paper fill");
    }
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         exchange, market_kind, symbol_key, action, side, order_type, quantity, reduce_only, \
         status, created_at_ms, updated_at_ms) VALUES ('eng_paper_iso', ?, 'bot_paper_iso', \
         'idem_paper_iso', 'binance', 'futures', 'binance|futures|SOL|USDT|USDT', 'open_long', \
         'long', 'market', '1', 0, 'acked', 1, 1)",
    )
    .bind(CANONICAL_PAPER)
    .execute(&pool)
    .await
    .expect("seed a paper engine order");
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

fn spawn_rustcore(
    db_url: &str,
    log_path: &std::path::Path,
    rest_port: u16,
    ws_port: u16,
    grpc_port: u16,
) -> RustcoreProcess {
    // env_clear：不继承任何真实凭据 / 代理端点 / L4 开关。凭据是假密钥，
    // 上游只能是本机监听端口，杜绝任何真实交易所出网。
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"));
    command.env_clear();
    let locator = format!("H{}", hex::encode_upper(LIVE_CREDENTIAL));
    let output = std::fs::File::create(log_path).expect("create engine log");
    let child = command
        .env("RUSTCORE_DB_URL", db_url)
        .env("RUSTCORE_GRPC_ADDR", format!("127.0.0.1:{grpc_port}"))
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "1")
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
        .env(format!("BINANCE_API_KEY_{locator}"), "iso-fake-key")
        .env(format!("BINANCE_API_SECRET_{locator}"), "iso-fake-secret")
        .env("RUSTCORE_BINANCE_WS_API_ENABLED", "0")
        .env("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "10000")
        .env("RUSTCORE_BINANCE_WEIGHT_BURST", "10000")
        .env("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "0")
        .env("RECONCILE_INTERVAL_MS", "1000")
        .env("BINANCE_HTTP_TIMEOUT_MS", "2000")
        .env("RUST_LOG", "info")
        .stdout(output.try_clone().expect("clone log handle"))
        .stderr(output)
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

async fn paper_stream_rows(pool: &sqlx::Pool<sqlx::Any>) -> Vec<(String, i64, String)> {
    sqlx::query(
        "SELECT credential_id, generation, phase FROM fill_stream_state \
         WHERE credential_id LIKE 'paper%' ORDER BY credential_id",
    )
    .fetch_all(pool)
    .await
    .expect("read paper fill stream state rows")
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("credential_id"),
            row.get::<i64, _>("generation"),
            row.get::<String, _>("phase"),
        )
    })
    .collect()
}

async fn live_ready(pool: &sqlx::Pool<sqlx::Any>) -> Option<(i64, String)> {
    sqlx::query("SELECT ready, phase FROM fill_stream_state WHERE credential_id = ?")
        .bind(LIVE_CREDENTIAL)
        .fetch_optional(pool)
        .await
        .expect("read the live credential fill stream state")
        .map(|row| (row.get::<i64, _>("ready"), row.get::<String, _>("phase")))
}

/// 轮询到判据成立为止；超时返回 None（断言里带出最后观察值，便于定位）。
async fn wait_until<T, F>(mut probe: impl FnMut() -> F, within: Duration) -> Option<T>
where
    F: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + within;
    loop {
        if let Some(value) = probe().await {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn count_where(pool: &sqlx::Pool<sqlx::Any>, sql: &str) -> i64 {
    sqlx::query(sql)
        .fetch_one(pool)
        .await
        .expect("run the count probe")
        .get::<i64, _>("n")
}

/// **两次真实进程启动**：第二次启动正是"每次启动都推进恢复代际"那条
/// 缺陷路径的复现面——修法之下，paper 凭据一个 `fill_stream_state` 行都
/// 不该出现，而交易所凭据两次都完成真实启动并收敛 ready。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_startup_never_starts_paper_fill_recovery_and_keeps_exchange_startup() {
    let upstream = Arc::new(Upstream::default());
    let rest_port = spawn_rest_upstream(upstream.clone()).await;
    let ws_port = spawn_ws_upstream(upstream.clone()).await;
    let grpc_port = free_port();
    let db_url = seeded_sqlite("startup").await;
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect the observation pool");
    let log_path =
        std::env::temp_dir().join(format!("rustcore-paper-iso-{}.log", ulid::Ulid::new()));

    for startup in 1..=2u32 {
        let child = spawn_rustcore(&db_url, &log_path, rest_port, ws_port, grpc_port);
        // 正域对照：交易所凭据必须真的起流——listenKey 由本用例 REST 端产出、
        // WS 握手路径逐字读回（两侧真值同源），reader 存活 + 空真值收敛 ready。
        let before_handshakes = upstream.handshakes.load(Ordering::SeqCst);
        let observed = wait_until(
            || async {
                (upstream.handshakes.load(Ordering::SeqCst) > before_handshakes
                    && live_ready(&pool).await == Some((1, "ready".to_string())))
                .then_some(())
            },
            Duration::from_secs(45),
        )
        .await;
        let ready = live_ready(&pool).await;
        assert!(
            observed.is_some(),
            "startup {startup}: the exchange-backed credential must complete real fill \
             recovery (ready=1); last state: {ready:?}; handshakes={}; log: {}",
            upstream.handshakes.load(Ordering::SeqCst),
            log_path.display()
        );
        assert_eq!(
            upstream
                .handshake_paths
                .lock()
                .expect("handshake paths")
                .iter()
                .filter(|path| *path == &format!("/ws/{LISTEN_KEY}"))
                .count(),
            upstream.handshakes.load(Ordering::SeqCst),
            "every WS handshake must consume the listenKey this test's REST peer minted"
        );
        // 判据本体：paper（legacy / canonical / 畸形保留串）不得有任何恢复
        // 失败行、不得推进任何恢复代际——没有行，就没有 churn。
        let paper_rows = paper_stream_rows(&pool).await;
        assert!(
            paper_rows.is_empty(),
            "startup {startup}: simulation credentials must never gain a fill stream state \
             row (failure or generation churn): {paper_rows:?}; log: {}",
            log_path.display()
        );
        // 留出重连/对账周期的时间窗再断言一次：失败路径若存在，会随周期反复出现。
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        let paper_rows_after_quiet = paper_stream_rows(&pool).await;
        assert!(
            paper_rows_after_quiet.is_empty(),
            "startup {startup}: no paper fill stream state row may appear across recovery \
             cycles: {paper_rows_after_quiet:?}"
        );
        let state_credential_ids: Vec<String> =
            sqlx::query("SELECT credential_id FROM fill_stream_state ORDER BY credential_id")
                .fetch_all(&pool)
                .await
                .expect("list fill stream state owners")
                .into_iter()
                .map(|row| row.get::<String, _>("credential_id"))
                .collect();
        assert_eq!(
            state_credential_ids,
            vec![LIVE_CREDENTIAL.to_string()],
            "only the exchange-backed credential may own a fill stream state row"
        );
        drop(child);
        // 给内核一点时间回收端口/子进程，再进入第二次启动。
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        upstream.listen_key_posts.load(Ordering::SeqCst) >= 2,
        "both startups must have really created listenKeys through the loopback REST peer"
    );

    // 存量 paper 行原样保留：修法靠分类放行，不靠删除/迁移历史数据遮丑。
    assert_eq!(
        count_where(
            &pool,
            "SELECT COUNT(*) AS n FROM fill_recovery_scopes WHERE credential_id LIKE 'paper%'",
        )
        .await,
        3,
        "the seeded paper/legacy/canonical/malformed scope rows must remain"
    );
    assert_eq!(
        count_where(
            &pool,
            "SELECT COUNT(*) AS n FROM fill_replay_cursors WHERE credential_id LIKE 'paper%'",
        )
        .await,
        1,
        "the seeded canonical paper cursor row must remain"
    );
    assert_eq!(
        count_where(
            &pool,
            "SELECT COUNT(*) AS n FROM engine_fills WHERE credential_id LIKE 'paper%'",
        )
        .await,
        2,
        "the seeded paper fill rows must remain"
    );
    assert_eq!(
        count_where(
            &pool,
            "SELECT COUNT(*) AS n FROM engine_orders WHERE credential_id LIKE 'paper%'",
        )
        .await,
        1,
        "the seeded paper engine order must remain"
    );
    pool.close().await;
}
