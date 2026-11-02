#![expect(
    clippy::await_holding_lock,
    reason = "the shared synchronous guard protects process-global exchange env for each whole async test"
)]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Once;
use std::time::Duration;

use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codec::ProstCodec;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Endpoint;

use super::{EngineEventsService, EngineExecutionService};
use crate::proto::{GetOrderRequest, GetOrderResponse};
use crate::store::{mysql_test_support::MySqlScratch, Store};

static INSTALL_DRIVERS: Once = Once::new();
pub(super) const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";

pub(super) struct EnvGuard {
    name: String,
    old: Option<OsString>,
}

impl EnvGuard {
    pub(super) fn set(name: impl Into<String>, value: &str) -> Self {
        let name = name.into();
        let old = std::env::var_os(&name);
        std::env::set_var(&name, value);
        Self { name, old }
    }

    pub(super) fn unset(name: impl Into<String>) -> Self {
        let name = name.into();
        let old = std::env::var_os(&name);
        std::env::remove_var(&name);
        Self { name, old }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.old.take() {
            Some(value) => std::env::set_var(&self.name, value),
            None => std::env::remove_var(&self.name),
        }
    }
}

struct ExchangeLoopback {
    address: std::net::SocketAddr,
    seen: tokio::sync::oneshot::Receiver<()>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ExchangeLoopback {
    async fn start(status: &'static str, hold_response: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("owned GetOrder loopback listener");
        let address = listener.local_addr().unwrap();
        let (seen_tx, seen) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0, "incomplete GetOrder HTTP request");
                bytes.extend_from_slice(&chunk[..count]);
                assert!(bytes.len() <= 16_384, "oversized GetOrder HTTP request");
            }
            let headers = std::str::from_utf8(&bytes).unwrap();
            let mut first = headers.lines().next().unwrap().split_whitespace();
            assert_eq!(first.next(), Some("GET"));
            let target = first.next().expect("request target");
            let url = url::Url::parse(&format!("http://{address}{target}")).unwrap();
            assert_eq!(url.path(), "/fapi/v1/order");
            let query: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
            assert_eq!(query.get("symbol").map(String::as_str), Some("BTCUSDT"));
            assert_eq!(query.get("orderId").map(String::as_str), Some("9001"));
            assert!(query
                .get("timestamp")
                .is_some_and(|value| !value.is_empty()));
            assert!(query
                .get("signature")
                .is_some_and(|value| !value.is_empty()));
            assert!(headers.lines().skip(1).any(|line| {
                line.split_once(':').is_some_and(|(name, value)| {
                    name.eq_ignore_ascii_case("X-MBX-APIKEY")
                        && value.trim() == "local-get-order-key"
                })
            }));
            let _ = seen_tx.send(());
            if hold_response {
                let _ = release_rx.await;
            }
            let body = serde_json::json!({
                "symbol": "BTCUSDT",
                "orderId": 9001,
                "clientOrderId": "get-order-client",
                "status": status,
                "updateTime": 1_900_000_000_000_i64
            })
            .to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.shutdown().await.unwrap();
        });
        Self {
            address,
            seen,
            release: hold_response.then_some(release),
            task: Some(task),
        }
    }

    async fn wait_until_requested(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), &mut self.seen)
            .await
            .expect("production GetOrder reaches the owned HTTP boundary")
            .expect("HTTP request observation channel stays open");
    }

    fn release_response(&mut self) {
        self.release
            .take()
            .expect("the response is held")
            .send(())
            .expect("GetOrder still waits for the response");
    }

    async fn finish(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        let task = self.task.take().expect("owned HTTP task is present");
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("owned HTTP server finishes")
            .expect("owned HTTP server does not panic");
    }
}

impl Drop for ExchangeLoopback {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) struct GrpcHarness {
    pub(super) address: std::net::SocketAddr,
    pub(super) token: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl GrpcHarness {
    pub(super) async fn start(store: Store) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("owned v1 gRPC listener");
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let token = format!("local-get-order-{}", ulid::Ulid::new());
        let execution =
            EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
        let events = EngineEventsService::new(store.clone(), Duration::from_millis(800));
        let v2 = crate::svc_v2::EngineExecutionV2Service::with_shared_execution(
            store,
            execution.clone(),
        );
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            execution,
            events,
            v2,
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![token.clone()])),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            token,
            stop: Some(stop),
            task: Some(task),
        }
    }

    pub(super) async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let task = self.task.take().expect("owned gRPC task is present");
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("v1 gRPC server stops")
            .expect("v1 gRPC server does not panic");
    }
}

impl Drop for GrpcHarness {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn get_order_over_grpc(
    address: std::net::SocketAddr,
    token: String,
    credential_id: String,
    engine_order_id: String,
) -> Result<GetOrderResponse, tonic::Status> {
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(GetOrderRequest {
        credential_id,
        engine_order_id,
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.vnext.EngineExecution/GetOrder"),
            ProstCodec::<GetOrderRequest, GetOrderResponse>::default(),
        )
        .await
        .map(tonic::Response::into_inner)
}

pub(super) async fn test_store(label: &str, credential_id: &str) -> (Store, PathBuf, String) {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!(
        "get-order-refresh-{label}-{}.db",
        ulid::Ulid::new()
    ));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("real SQLite store with production migrations");
    let engine_order_id = seed_order(&store, label, credential_id, "submitted").await;
    (store, path, engine_order_id)
}

pub(super) async fn seed_order(
    store: &Store,
    label: &str,
    credential_id: &str,
    initial_status: &str,
) -> String {
    let engine_order_id = format!("get-order-{label}");
    sqlx::query(
        r#"
        INSERT INTO engine_orders (
          engine_order_id, credential_id, bot_uid, run_id, cycle_id,
          idempotency_key, client_order_id_short, exchange_order_id,
          exchange, market_kind, symbol_key, action, side, order_type,
          quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
        ) VALUES (?, ?, 'bot-get-order', 'run-get-order', 'cycle-get-order',
          ?, 'get-order-client', '9001', 'binance', 'futures', ?,
          'open_long', 'long', 'limit', '1', 0, ?, 'entry',
          1800000000000, 1800000000100)
        "#,
    )
    .bind(&engine_order_id)
    .bind(credential_id)
    .bind(format!("idem-{label}"))
    .bind(SYMBOL_KEY)
    .bind(initial_status)
    .execute(store.pool())
    .await
    .unwrap();
    engine_order_id
}

pub(super) fn exchange_env(credential_id: &str, address: std::net::SocketAddr) -> Vec<EnvGuard> {
    let (key, secret) = crate::binance_futures::key_env_names(credential_id);
    let route_id = format!(
        "getorder-{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    vec![
        EnvGuard::unset(crate::venue::venue_env_name(credential_id)),
        EnvGuard::set(key, "local-get-order-key"),
        EnvGuard::set(secret, "local-get-order-secret"),
        EnvGuard::set("ENGINE_LIVE_ENABLED", "1"),
        EnvGuard::set("BINANCE_FUTURES_BASE_URL", &format!("http://{address}")),
        // This suite proves GetOrder persistence, not process-global egress
        // contention. A unique production route scope and an ample real
        // limiter budget keep ambient/shared limiter state from turning the
        // best-effort REST refresh into a silent local-row fallback.
        EnvGuard::set("BINANCE_EGRESS_ROUTE_ID", &route_id),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "100"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_BURST", "100"),
        EnvGuard::set("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE", "1"),
        EnvGuard::unset(crate::venue::FAULT_PROXY_REST_BASE_URL_ENV),
        EnvGuard::unset(crate::venue::FAULT_PROXY_WS_BASE_URL_ENV),
        EnvGuard::unset(crate::venue::FAULT_PROXY_CA_PEM_PATH_ENV),
        EnvGuard::set("NO_PROXY", "127.0.0.1,localhost"),
        EnvGuard::set("no_proxy", "127.0.0.1,localhost"),
    ]
}

pub(super) async fn persisted_status(store: &Store, engine_order_id: &str) -> (String, i64) {
    let row = sqlx::query(
        "SELECT status, CAST(updated_at_ms AS CHAR) AS updated_at_ms_text \
         FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(engine_order_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    (
        row.get("status"),
        crate::store::try_get_i64_text(&row, "updated_at_ms_text").unwrap(),
    )
}

pub(super) async fn event_count(store: &Store, credential_id: &str) -> i64 {
    sqlx::query(
        "SELECT COUNT(*) AS n FROM engine_order_events \
         WHERE credential_id=? AND event_type='order_update'",
    )
    .bind(credential_id)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get("n")
}

async fn checkpoint_count(store: &Store, credential_id: &str) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM engine_checkpoints WHERE credential_id=?")
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("n")
}

async fn assert_mysql_get_order_preflight(
    store: &Store,
    credential_id: &str,
    engine_order_id: &str,
    exchange_address: std::net::SocketAddr,
) {
    assert_eq!(
        crate::venue::credential_kind(credential_id),
        crate::venue::CredentialKind::ExchangeBacked,
        "the MySQL fixture credential must reach the exchange-backed branch"
    );
    let keys = crate::binance_futures::keys_from_env(credential_id)
        .expect("the MySQL fixture key locator is valid")
        .expect("the MySQL fixture resolves its credential-scoped keys");
    assert_eq!(keys.api_key, "local-get-order-key");
    assert_eq!(keys.api_secret, "local-get-order-secret");
    assert!(
        super::is_binance_live_enabled(),
        "the MySQL fixture enables the live exchange gateway"
    );
    assert!(
        super::exchange_gateway_allowed(credential_id)
            .expect("the MySQL fixture has a valid venue configuration"),
        "the MySQL fixture credential must pass the production exchange gateway"
    );
    let transport = crate::venue::rest_transport_for_credential(credential_id)
        .expect("the MySQL fixture resolves its REST transport");
    assert_eq!(
        transport.base_url,
        format!("http://{exchange_address}"),
        "GetOrder must target the currently owned loopback"
    );

    let row = tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query(
            r#"
            SELECT eo.status,
                   COALESCE(po.protection_order_id, '') AS protection_order_id
            FROM engine_orders eo
            LEFT JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id
            WHERE eo.credential_id=? AND eo.engine_order_id=?
            LIMIT 1
            "#,
        )
        .bind(credential_id)
        .bind(engine_order_id)
        .fetch_one(store.pool()),
    )
    .await
    .expect("the GetOrder preflight JOIN is not blocked by MySQL trigger metadata")
    .expect("the GetOrder preflight JOIN finds its seeded row");
    assert_eq!(row.get::<String, _>("status"), "submitted");
    assert_eq!(row.get::<String, _>("protection_order_id"), "");
}

async fn require_mysql_pool_quiescent(store: &Store, label: &str) {
    let started = std::time::Instant::now();
    loop {
        let pool_size = store.pool().size() as usize;
        let pool_idle = store.pool().num_idle();
        if pool_idle == pool_size {
            return;
        }
        if started.elapsed() >= Duration::from_secs(5) {
            panic!(
                "{label}: MySQL pool did not quiesce within 5s; size={pool_size} \
                 idle={pool_idle} acquired={}; scratch database retained",
                pool_size.saturating_sub(pool_idle)
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn close_mysql_fixture(store: Store, scratch: MySqlScratch, label: &str) {
    require_mysql_pool_quiescent(&store, label).await;
    eprintln!("GET_ORDER_MYSQL_STAGE {label} pool_close_begin");
    if tokio::time::timeout(Duration::from_secs(5), store.pool().close())
        .await
        .is_err()
    {
        panic!("{label}: MySQL pool close exceeded 5s; scratch database retained");
    }
    eprintln!("GET_ORDER_MYSQL_STAGE {label} pool_close_done");
    drop(store);

    eprintln!("GET_ORDER_MYSQL_STAGE {label} scratch_cleanup_begin");
    if tokio::time::timeout(Duration::from_secs(10), scratch.cleanup())
        .await
        .is_err()
    {
        panic!("{label}: MySQL scratch cleanup exceeded 10s; database retained");
    }
    eprintln!("GET_ORDER_MYSQL_STAGE {label} scratch_cleanup_done");
}

pub(super) async fn cleanup(store: Store, path: PathBuf) {
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
}

#[test]
fn get_order_refresh_event_id_includes_the_complete_order_observation_identity() {
    let base = super::get_order_refresh_event_id(
        "credential-a",
        "engine-order-a",
        SYMBOL_KEY,
        "9001",
        "acked",
        1_900_000_000_000,
    );
    for different in [
        super::get_order_refresh_event_id(
            "credential-b",
            "engine-order-a",
            SYMBOL_KEY,
            "9001",
            "acked",
            1_900_000_000_000,
        ),
        super::get_order_refresh_event_id(
            "credential-a",
            "engine-order-b",
            SYMBOL_KEY,
            "9001",
            "acked",
            1_900_000_000_000,
        ),
        super::get_order_refresh_event_id(
            "credential-a",
            "engine-order-a",
            "binance|futures|ETH|USDT|USDT",
            "9001",
            "acked",
            1_900_000_000_000,
        ),
        super::get_order_refresh_event_id(
            "credential-a",
            "engine-order-a",
            SYMBOL_KEY,
            "9002",
            "acked",
            1_900_000_000_000,
        ),
        super::get_order_refresh_event_id(
            "credential-a",
            "engine-order-a",
            SYMBOL_KEY,
            "9001",
            "partial_filled",
            1_900_000_000_000,
        ),
        super::get_order_refresh_event_id(
            "credential-a",
            "engine-order-a",
            SYMBOL_KEY,
            "9001",
            "acked",
            1_900_000_000_001,
        ),
    ] {
        assert_ne!(different, base);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_does_not_regress_a_concurrent_fill() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-race";
    let (store, path, engine_order_id) = test_store("race", credential_id).await;
    sqlx::query("UPDATE engine_orders SET status='acked' WHERE engine_order_id=?")
        .bind(&engine_order_id)
        .execute(store.pool())
        .await
        .expect("the race starts from the exchange-acknowledged state");
    let mut exchange = ExchangeLoopback::start("NEW", true).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let call = tokio::spawn(get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    ));
    exchange.wait_until_requested().await;
    let filled_at = 1_900_000_000_123_i64;
    sqlx::query(
        "UPDATE engine_orders SET status='filled', updated_at_ms=? WHERE engine_order_id=?",
    )
    .bind(filled_at)
    .bind(&engine_order_id)
    .execute(store.pool())
    .await
    .expect("a concurrent fill commits while REST is pending");
    exchange.release_response();

    let response = call
        .await
        .expect("GetOrder task does not panic")
        .expect("GetOrder returns the persisted truth");
    let order = response.order.expect("order remains present");
    assert_eq!(
        (order.status.as_str(), order.updated_at_ms),
        ("filled", filled_at),
        "the RPC must return the stronger row and its matching persisted timestamp"
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await,
        ("filled".to_string(), filled_at),
        "a late REST NEW must not move the database backward"
    );
    assert_eq!(event_count(&store, credential_id).await, 0);

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_atomically_advances_status_and_event() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-advance-credential";
    let (store, path, engine_order_id) = test_store("advance-order", credential_id).await;
    sqlx::query("UPDATE engine_orders SET status='failed' WHERE engine_order_id=?")
        .bind(&engine_order_id)
        .execute(store.pool())
        .await
        .expect("seed a local failed adjudication before final exchange truth arrives");
    let exchange = ExchangeLoopback::start("FILLED", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let response = get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    )
    .await
    .expect("a stronger exchange truth commits");
    let order = response.order.expect("order remains present");
    let persisted = persisted_status(&store, &engine_order_id).await;
    assert_eq!(persisted.0, "filled");
    assert_ne!(
        persisted.1, 1_800_000_000_100,
        "a real advance must replace the old row timestamp"
    );
    assert!(persisted.1 > i64::from(i32::MAX));
    assert_eq!(
        (order.status, order.updated_at_ms),
        persisted,
        "the successful RPC projection must be the row that committed"
    );
    assert_eq!(
        event_count(&store, credential_id).await,
        1,
        "the failed-to-filled advance has exactly one order_update fact"
    );
    let event_id: String = sqlx::query(
        "SELECT event_id FROM engine_order_events \
         WHERE credential_id=? AND event_type='order_update'",
    )
    .bind(credential_id)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get("event_id");
    let expected_event_id = super::get_order_refresh_event_id(
        credential_id,
        &engine_order_id,
        SYMBOL_KEY,
        "9001",
        "filled",
        1_900_000_000_000,
    );
    assert_eq!(
        expected_event_id, "9f522ced46709c3b605b0b74361b80c48f07fe5ab18ee0d9f320e6766de2d0b4",
        "the get_order domain separator and complete observation shape are stable"
    );
    assert_eq!(
        event_id, expected_event_id,
        "the production call site must bind the complete exchange observation identity"
    );

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_keeps_an_equal_rank_terminal_truth_unchanged() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-equal-terminal";
    let (store, path, engine_order_id) = test_store("equal-terminal", credential_id).await;
    let canceled_at = 1_900_000_000_456_i64;
    sqlx::query(
        "UPDATE engine_orders SET status='canceled', updated_at_ms=? WHERE engine_order_id=?",
    )
    .bind(canceled_at)
    .bind(&engine_order_id)
    .execute(store.pool())
    .await
    .unwrap();
    let exchange = ExchangeLoopback::start("EXPIRED", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let response = get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    )
    .await
    .expect("an equal-rank observation returns the persisted order");
    let order = response.order.expect("order remains present");
    assert_eq!(
        (order.status.as_str(), order.updated_at_ms),
        ("canceled", canceled_at)
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await,
        ("canceled".to_string(), canceled_at)
    );
    assert_eq!(
        event_count(&store, credential_id).await,
        0,
        "an observation that did not advance state must not claim an order_update"
    );

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_propagates_order_update_failure() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-update-failure";
    let (store, path, engine_order_id) = test_store("update-failure", credential_id).await;
    sqlx::query(
        r#"
        CREATE TRIGGER fail_get_order_update
        BEFORE UPDATE OF status ON engine_orders
        WHEN NEW.engine_order_id = 'get-order-update-failure'
        BEGIN
          SELECT RAISE(ABORT, 'injected GetOrder engine_orders update failure');
        END
        "#,
    )
    .execute(store.pool())
    .await
    .unwrap();
    let exchange = ExchangeLoopback::start("NEW", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let error = get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    )
    .await
    .expect_err("a failed engine_orders write cannot be reported as a refreshed order");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("order refresh update failed"),
        "unexpected error: {error}"
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await.0,
        "submitted"
    );
    assert_eq!(event_count(&store, credential_id).await, 0);

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_rolls_back_everything_on_commit_failure() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-commit-failure";
    let (store, path, engine_order_id) = test_store("commit-failure", credential_id).await;
    let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
        .fetch_one(store.pool())
        .await
        .expect("read SQLite foreign-key enforcement")
        .get("foreign_keys");
    assert_eq!(
        foreign_keys, 1,
        "the commit-time proof requires real SQLite foreign-key enforcement"
    );
    sqlx::query("CREATE TABLE get_order_commit_parent (id VARCHAR(64) PRIMARY KEY)")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        r#"
        CREATE TABLE get_order_commit_probe (
          credential_id VARCHAR(64) PRIMARY KEY,
          missing_parent_id VARCHAR(64) NOT NULL,
          FOREIGN KEY (missing_parent_id)
            REFERENCES get_order_commit_parent(id)
            DEFERRABLE INITIALLY DEFERRED
        )
        "#,
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        CREATE TRIGGER defer_get_order_commit_failure
        AFTER INSERT ON engine_order_events
        WHEN NEW.credential_id = 'get-order-commit-failure'
             AND NEW.event_type = 'order_update'
        BEGIN
          INSERT INTO get_order_commit_probe (credential_id, missing_parent_id)
          VALUES (NEW.credential_id, 'missing-parent');
        END
        "#,
    )
    .execute(store.pool())
    .await
    .unwrap();
    let exchange = ExchangeLoopback::start("NEW", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let error = get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    )
    .await
    .expect_err("a deferred constraint failure must surface from the real commit boundary");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("db order refresh commit failed"),
        "the failure must occur at commit, not an earlier statement: {error}"
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await,
        ("submitted".to_string(), 1_800_000_000_100),
        "the status projection must roll back when commit fails"
    );
    assert_eq!(event_count(&store, credential_id).await, 0);
    assert_eq!(
        checkpoint_count(&store, credential_id).await,
        0,
        "checkpoint creation/advance is part of the failed transaction"
    );
    let deferred_probe_rows: i64 = sqlx::query("SELECT COUNT(*) AS n FROM get_order_commit_probe")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("n");
    assert_eq!(deferred_probe_rows, 0);

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_rolls_back_status_when_event_insert_fails() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "get-order-event-failure";
    let (store, path, engine_order_id) = test_store("event-failure", credential_id).await;
    sqlx::query(
        r#"
        CREATE TRIGGER fail_get_order_event
        BEFORE INSERT ON engine_order_events
        WHEN NEW.credential_id = 'get-order-event-failure'
             AND NEW.event_type = 'order_update'
        BEGIN
          SELECT RAISE(ABORT, 'injected GetOrder engine_order_events insert failure');
        END
        "#,
    )
    .execute(store.pool())
    .await
    .unwrap();
    let exchange = ExchangeLoopback::start("NEW", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let grpc = GrpcHarness::start(store.clone()).await;

    let error = get_order_over_grpc(
        grpc.address,
        grpc.token.clone(),
        credential_id.to_string(),
        engine_order_id.clone(),
    )
    .await
    .expect_err("an audit insert failure cannot leave or return a refreshed status");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("db event insert failed"),
        "unexpected error: {error}"
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await.0,
        "submitted",
        "the status update and its event must roll back together"
    );
    assert_eq!(event_count(&store, credential_id).await, 0);
    assert_eq!(
        checkpoint_count(&store, credential_id).await,
        0,
        "the failed event transaction must not leave or advance its checkpoint"
    );

    grpc.finish().await;
    exchange.finish().await;
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_mysql_commits_status_and_event() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let Some(scratch) = MySqlScratch::create(
        "getorder",
        "get_order_rest_refresh_mysql_commits_status_and_event",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    eprintln!("GET_ORDER_MYSQL_STAGE positive scratch_ready");

    let advance_credential_id = "get-order-mysql-advance-credential";
    let advance_engine_order_id = seed_order(
        &store,
        "mysql-advance-order",
        advance_credential_id,
        "failed",
    )
    .await;
    {
        let exchange = ExchangeLoopback::start("FILLED", false).await;
        let _env = exchange_env(advance_credential_id, exchange.address);
        let grpc = GrpcHarness::start(store.clone()).await;

        eprintln!("GET_ORDER_MYSQL_STAGE positive rpc_begin");
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            get_order_over_grpc(
                grpc.address,
                grpc.token.clone(),
                advance_credential_id.to_string(),
                advance_engine_order_id.clone(),
            ),
        )
        .await
        .expect("positive MySQL GetOrder RPC exceeded 10s")
        .expect("MySQL commits a failed-to-filled exchange truth");
        eprintln!("GET_ORDER_MYSQL_STAGE positive rpc_done");
        let order = response.order.expect("the MySQL order remains present");
        let persisted = persisted_status(&store, &advance_engine_order_id).await;
        assert_eq!(persisted.0, "filled");
        assert_ne!(persisted.1, 1_800_000_000_100);
        assert_eq!(
            (order.status, order.updated_at_ms),
            persisted,
            "the MySQL RPC returns the exact durable status/timestamp pair"
        );
        assert_eq!(event_count(&store, advance_credential_id).await, 1);
        assert_eq!(checkpoint_count(&store, advance_credential_id).await, 1);

        eprintln!("GET_ORDER_MYSQL_STAGE positive grpc_finish_begin");
        grpc.finish().await;
        eprintln!("GET_ORDER_MYSQL_STAGE positive grpc_finish_done");
        exchange.finish().await;
        eprintln!("GET_ORDER_MYSQL_STAGE positive exchange_finish_done");
    }

    close_mysql_fixture(store, scratch, "positive").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_mysql_treats_noncanonical_status_as_dirty() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let Some(scratch) = MySqlScratch::create(
        "getorder",
        "get_order_rest_refresh_mysql_treats_noncanonical_status_as_dirty",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let credential_id = "get-order-mysql-dirty-credential";
    let engine_order_id = seed_order(&store, "mysql-dirty-order", credential_id, "ACKED").await;
    {
        let exchange = ExchangeLoopback::start("PARTIALLY_FILLED", false).await;
        let _env = exchange_env(credential_id, exchange.address);
        let grpc = GrpcHarness::start(store.clone()).await;

        let response = tokio::time::timeout(
            Duration::from_secs(10),
            get_order_over_grpc(
                grpc.address,
                grpc.token.clone(),
                credential_id.to_string(),
                engine_order_id.clone(),
            ),
        )
        .await
        .expect("dirty-status MySQL GetOrder RPC exceeded 10s")
        .expect("dirty current status is returned without normalization");
        let order = response.order.expect("the dirty MySQL row remains present");
        assert_eq!(
            (order.status, order.updated_at_ms),
            ("ACKED".to_string(), 1_800_000_000_100),
            "a noncanonical current status must fail closed byte-for-byte"
        );
        assert_eq!(
            persisted_status(&store, &engine_order_id).await,
            ("ACKED".to_string(), 1_800_000_000_100)
        );
        assert_eq!(event_count(&store, credential_id).await, 0);
        assert_eq!(checkpoint_count(&store, credential_id).await, 0);

        grpc.finish().await;
        exchange.finish().await;
    }

    close_mysql_fixture(store, scratch, "dirty_status").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_rest_refresh_mysql_transaction_contract() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let Some(scratch) = MySqlScratch::create(
        "getorder",
        "get_order_rest_refresh_mysql_transaction_contract",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    eprintln!("GET_ORDER_MYSQL_STAGE negative scratch_ready");

    sqlx::raw_sql(
        r#"
        CREATE TRIGGER fail_get_order_mysql_event
        BEFORE INSERT ON engine_order_events
        FOR EACH ROW
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'injected GetOrder MySQL engine_order_events failure'
        "#,
    )
    .execute(store.pool())
    .await
    .expect("install a real MySQL event-insert failure boundary");
    eprintln!("GET_ORDER_MYSQL_STAGE negative trigger_installed");
    let installed_triggers: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM information_schema.TRIGGERS \
         WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME='fail_get_order_mysql_event'",
    )
    .fetch_one(store.pool())
    .await
    .expect("inspect the isolated MySQL trigger")
    .get("n");
    assert_eq!(installed_triggers, 1, "the injected trigger must be active");

    let rollback_credential_id = "get-order-mysql-rollback-credential";
    let rollback_engine_order_id = seed_order(
        &store,
        "mysql-rollback-order",
        rollback_credential_id,
        "submitted",
    )
    .await;
    {
        let mut exchange = ExchangeLoopback::start("NEW", true).await;
        let _env = exchange_env(rollback_credential_id, exchange.address);
        assert_mysql_get_order_preflight(
            &store,
            rollback_credential_id,
            &rollback_engine_order_id,
            exchange.address,
        )
        .await;
        eprintln!("GET_ORDER_MYSQL_STAGE negative preflight_done");
        let grpc = GrpcHarness::start(store.clone()).await;

        let mut call = tokio::spawn(get_order_over_grpc(
            grpc.address,
            grpc.token.clone(),
            rollback_credential_id.to_string(),
            rollback_engine_order_id.clone(),
        ));
        eprintln!("GET_ORDER_MYSQL_STAGE negative rpc_dispatched");
        tokio::select! {
            biased;
            observed = &mut exchange.seen => {
                observed.expect("the MySQL negative loopback remains alive until request arrival");
                eprintln!("GET_ORDER_MYSQL_STAGE negative rest_arrived");
            }
            early = &mut call => {
                match early {
                    Ok(Ok(response)) => panic!(
                        "MySQL negative GetOrder returned before REST arrival: found={} status={:?}",
                        response.found,
                        response.order.as_ref().map(|order| order.status.as_str())
                    ),
                    Ok(Err(status)) => panic!(
                        "MySQL negative GetOrder failed before REST arrival: code={:?} message={}",
                        status.code(),
                        status.message()
                    ),
                    Err(error) => panic!(
                        "MySQL negative GetOrder task ended before REST arrival: {error}"
                    ),
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                call.abort();
                let _ = tokio::time::timeout(Duration::from_secs(2), &mut call).await;
                panic!(
                    "MySQL negative GetOrder did not reach REST within 5s after identity, keys, \
                     gateway, base URL, trigger, and preflight JOIN were verified"
                );
            }
        }
        exchange.release_response();
        eprintln!("GET_ORDER_MYSQL_STAGE negative response_released");
        let joined = match tokio::time::timeout(Duration::from_secs(10), &mut call).await {
            Ok(joined) => joined,
            Err(_) => {
                call.abort();
                let _ = tokio::time::timeout(Duration::from_secs(2), &mut call).await;
                panic!(
                    "MySQL negative GetOrder RPC exceeded 10s after REST response release; \
                     client task aborted and scratch database retained"
                );
            }
        };
        let error = joined
            .expect("the MySQL negative GetOrder task does not panic")
            .expect_err("a MySQL event failure cannot return a refreshed order");
        eprintln!("GET_ORDER_MYSQL_STAGE negative rpc_done");
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(
            error.message().contains("db event insert failed"),
            "unexpected MySQL event failure: {error}"
        );
        assert_eq!(
            persisted_status(&store, &rollback_engine_order_id).await,
            ("submitted".to_string(), 1_800_000_000_100),
            "MySQL rolls back the status update with its failed event"
        );
        assert_eq!(event_count(&store, rollback_credential_id).await, 0);
        assert_eq!(checkpoint_count(&store, rollback_credential_id).await, 0);

        eprintln!("GET_ORDER_MYSQL_STAGE negative grpc_finish_begin");
        grpc.finish().await;
        eprintln!("GET_ORDER_MYSQL_STAGE negative grpc_finish_done");
        exchange.finish().await;
        eprintln!("GET_ORDER_MYSQL_STAGE negative exchange_finish_done");
    }

    close_mysql_fixture(store, scratch, "negative").await;
}

// Scope control: safety settlement does not repair the default queue/RPC budget.
async fn settlement_get_order_with_deadline(
    address: std::net::SocketAddr,
    token: String,
    credential_id: String,
    engine_order_id: String,
) -> Result<GetOrderResponse, tonic::Status> {
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(GetOrderRequest {
        credential_id,
        engine_order_id,
    });
    request.set_timeout(Duration::from_secs(10));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.vnext.EngineExecution/GetOrder"),
            ProstCodec::<GetOrderRequest, GetOrderResponse>::default(),
        )
        .await
        .map(tonic::Response::into_inner)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_s16_cold_default_get_order_returns_local_fallback_with_combined_budget() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "k03-default-budget-credential";
    let (store, path, engine_order_id) = test_store("k03-default-budget", credential_id).await;
    let mut exchange = ExchangeLoopback::start("NEW", false).await;
    let _env = exchange_env(credential_id, exchange.address);
    let _defaults: Vec<_> = [
        "RUSTCORE_BINANCE_WEIGHT_PER_SEC",
        "RUSTCORE_BINANCE_WEIGHT_BURST",
        "RUSTCORE_BINANCE_RPS",
        "RUSTCORE_BINANCE_BURST",
        "RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE",
        "RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M",
        "RUSTCORE_BINANCE_QUEUE_WAIT_MAX_MS",
        "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M",
    ]
    .into_iter()
    .map(EnvGuard::unset)
    .collect();
    for i in 0..5 {
        sqlx::query("INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, state, qty, updated_at_ms) VALUES (?, ?, ?, ?, 'long', 1, 'protected', '1', 1)")
            .bind(format!("k03-pos-{i}")).bind(credential_id).bind(format!("k03-bot-{i}"))
            .bind(SYMBOL_KEY).execute(store.pool()).await.unwrap();
    }
    let client = crate::binance_futures::BinanceFuturesClient::new_with_store(
        crate::binance_futures::keys_from_env(credential_id)
            .unwrap()
            .unwrap(),
        credential_id,
        store.clone(),
    )
    .unwrap();
    assert_eq!(client.admission_open_positions(&store).await.unwrap(), 5);
    let grpc = GrpcHarness::start(store.clone()).await;
    let unauth = settlement_get_order_with_deadline(
        grpc.address,
        "wrong-token".into(),
        credential_id.into(),
        engine_order_id.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(unauth.code(), tonic::Code::Unauthenticated);
    let original_state = persisted_status(&store, &engine_order_id).await;
    assert_eq!(original_state, ("submitted".into(), 1_800_000_000_100));
    let margin = Duration::from_millis(500);
    let start = std::time::Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(8) + margin,
        settlement_get_order_with_deadline(
            grpc.address,
            grpc.token.clone(),
            credential_id.into(),
            engine_order_id.clone(),
        ),
    )
    .await
    .expect("combined refresh budget plus the fixed scheduling margin")
    .expect("K03 returns confirmed local state before the original 10-second caller deadline");
    let elapsed = start.elapsed();
    assert!(response.found);
    let order = response.order.unwrap();
    assert_eq!((order.status, order.updated_at_ms), original_state);
    assert_eq!(
        persisted_status(&store, &engine_order_id).await,
        original_state
    );
    assert_eq!(event_count(&store, credential_id).await, 0);
    println!(
        "K03_PROOF rpc_code=OK elapsed_ms={} active_positions=5 grpc_timeout_s=10 local_fallback=true original_timestamp={}",
        elapsed.as_millis(), original_state.1
    );
    assert!(
        elapsed >= Duration::from_secs(8) - margin && elapsed <= Duration::from_secs(8) + margin
    );
    // Retain the old cancellation observation horizon: the original 10-second
    // caller deadline plus 1.5 seconds, including the old cold admission point.
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        start + Duration::from_millis(11_500),
    ))
    .await;
    assert!(
        matches!(
            exchange.seen.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "expired refresh must send no HTTP through the original caller deadline and observation window"
    );
    assert_eq!(
        persisted_status(&store, &engine_order_id).await,
        original_state
    );
    assert_eq!(event_count(&store, credential_id).await, 0);
    println!("K03_PROOF http_requests=0 persisted_status=submitted original_timestamp={} order_events=0 observation_ms={}", original_state.1, start.elapsed().as_millis());
    grpc.finish().await;
    drop(exchange);
    cleanup(store, path).await;
}
