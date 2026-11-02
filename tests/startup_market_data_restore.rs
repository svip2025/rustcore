//! **`main` 的 restore 调用点必须有
//! 不可绕过的行为证据。**
//!
//! 段级终轮的变异「删除 `main` 里的 `restore_market_data_on_startup()` 调用」
//! **6/6 存活**：R9 的用例全部走 `EngineExecutionService::restore_market_data_on_startup`
//! 这个方法本身，没有任何一条从**进程启动**出发——于是"重启后行情流会被
//! 重建"这件事在生产入口上从未被证明过。
//!
//! 这条用例按 engine safety contract 的四条办：
//! * **入口同一**：跑的是 `CARGO_BIN_EXE_rustcore`，即部署时执行的那个二进制，
//!   走 `main()` 全程（fault-proxy 校验 → 建库 → 后台任务 → restore → serve）；
//!   用例一行内部函数都没调。
//! * **不跳层**：订阅真值来自库里的持久行，worker 的建立、`symbol_key` →
//!   交易所 symbol 的换算、ws URL 的拼装、TLS/transport 选路全部由生产代码做。
//! * **两侧真值同源**：种进库的 `symbol_key`/`timeframe` 由 rustcore 自己解析、
//!   自己拼成 `/ws/btcusdt@kline_1m`；用例只在**链路之外**（交易所那一端）
//!   放一条真实的 TCP 监听收下这次连接，逐字读它发来的 HTTP 升级请求。
//! * **反向可断言**：不种订阅行时，同一个二进制在同样的等待窗口内**不会**
//!   来连——证明这次连接确实是那条持久订阅驱动的。
//!
//! **替身缝的位置**：只有"交易所"被替换成本机监听端口
//! （`BINANCE_FUTURES_WS_BASE_URL`，生产同名开关）。被证明的链路本身
//! ——启动 → 读库 → 闭世界重验 → 起 worker → 连接——一层都没替。
//! REST 端点被指向一个死端口，用例因此**不可能**联到真实交易所。

use std::time::Duration;

use sqlx::any::AnyPoolOptions;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

const BTC_KEY: &str = "binance|futures|BTC|USDT|USDT";

async fn seeded_sqlite(name: &str, rows: &[(&str, &str, &str)]) -> String {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!(
        "rustcore-r10-startup-{name}-{}.db",
        std::process::id()
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
    for (credential_id, symbol_key, timeframe) in rows {
        sqlx::query(
            "INSERT INTO market_stream_subscriptions \
             (credential_id, symbol_key, timeframe, requested_at_ms) VALUES (?,?,?,?)",
        )
        .bind(*credential_id)
        .bind(*symbol_key)
        .bind(*timeframe)
        .bind(1_000i64)
        .execute(&pool)
        .await
        .expect("seed persisted subscription");
    }
    pool.close().await;
    url
}

/// 子进程守卫：用例无论正常结束还是 panic，都不留下一个还在跑的 rustcore。
struct RustcoreProcess(std::process::Child);

impl Drop for RustcoreProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// 起真实的 rustcore 二进制，上游指向 `upstream_port`。
fn spawn_rustcore(db_url: &str, upstream_port: u16) -> RustcoreProcess {
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_rustcore"))
        .env("RUSTCORE_DB_URL", db_url)
        .env("RUSTCORE_GRPC_ADDR", "127.0.0.1:0")
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "1")
        // 上游只能是本机监听端口；REST 指向死端口，杜绝任何真实交易所出网。
        .env(
            "BINANCE_FUTURES_WS_BASE_URL",
            format!("ws://127.0.0.1:{upstream_port}"),
        )
        .env("BINANCE_FUTURES_BASE_URL", "http://127.0.0.1:1")
        .env("BINANCE_FAULT_PROXY_REST_BASE_URL", "")
        .env("BINANCE_FAULT_PROXY_WS_BASE_URL", "")
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn the production rustcore binary");
    RustcoreProcess(child)
}

/// 收一条上游连接并读回它发来的请求行。
async fn first_upstream_request(listener: &TcpListener, within: Duration) -> Option<String> {
    let accepted = tokio::time::timeout(within, listener.accept()).await.ok()?;
    let (mut socket, _peer) = accepted.expect("accept upstream connection");
    let mut buffer = vec![0u8; 4096];
    let read = tokio::time::timeout(within, socket.read(&mut buffer))
        .await
        .ok()?
        .expect("read upstream request");
    Some(String::from_utf8_lossy(&buffer[..read]).to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_binary_reopens_persisted_market_data_streams_on_startup() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in upstream");
    let port = listener.local_addr().expect("upstream addr").port();
    let db_url = seeded_sqlite("restored", &[("startup.restore.cred", BTC_KEY, "1m")]).await;

    let child = spawn_rustcore(&db_url, port);
    let request = first_upstream_request(&listener, Duration::from_secs(30)).await;
    drop(child);

    let request = request.expect(
        "the production binary must reopen the persisted kline subscription on startup; \
         no upstream connection arrived",
    );
    let first_line = request.lines().next().unwrap_or_default().to_string();
    assert!(
        first_line.contains("/ws/btcusdt@kline_1m"),
        "the restored worker must stream exactly the persisted identity: {first_line}"
    );
    assert!(
        first_line.starts_with("GET "),
        "the upstream request must be a real websocket upgrade: {first_line}"
    );
}

/// **反向**：库里没有持久订阅时，同一个二进制在同样的窗口内不会来连。
/// 没有这一条，"启动会连上游"可能只是二进制无条件做的事。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_binary_opens_no_stream_without_a_persisted_subscription() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in upstream");
    let port = listener.local_addr().expect("upstream addr").port();
    let db_url = seeded_sqlite("empty", &[]).await;

    let child = spawn_rustcore(&db_url, port);
    let request = first_upstream_request(&listener, Duration::from_secs(10)).await;
    drop(child);

    assert!(
        request.is_none(),
        "no persisted subscription may produce an upstream stream: {request:?}"
    );
}
