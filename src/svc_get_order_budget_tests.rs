#![expect(
    clippy::await_holding_lock,
    reason = "serialize process-global exchange configuration"
)]

use super::get_order_refresh_tests::{
    cleanup, event_count, exchange_env, persisted_status, seed_order, test_store, EnvGuard,
    GrpcHarness, SYMBOL_KEY,
};
use crate::{
    binance_futures::{keys_from_env, BinanceFuturesClient},
    proto::{
        ExecuteIntentsRequest, ExecuteIntentsResponse, GetOrderRequest, GetOrderResponse,
        OrderIntent,
    },
    rate_limiter::{CredentialRateLimiter, TokenBucketConfig},
    store::Store,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Semaphore},
    time::Instant,
};
use tonic::{codec::ProstCodec, codegen::http::uri::PathAndQuery, transport::Endpoint, Code};

const ORIGINAL_TIME: i64 = 1_800_000_000_100;
const MARGIN: Duration = Duration::from_millis(500);

struct Reply {
    method: &'static str,
    path: &'static str,
    body: Value,
    delay: Duration,
    release: Option<Arc<Semaphore>>,
}
impl Reply {
    fn order(status: &str) -> Self {
        Self {
            method: "GET",
            path: "/fapi/v1/order",
            body: json!({"symbol":"BTCUSDT", "orderId":9001, "status":status, "updateTime":1_900_000_000_000_i64}),
            delay: Duration::ZERO,
            release: None,
        }
    }
}

#[derive(Clone, Debug)]
struct Observed {
    method: String,
    path: String,
    at: Instant,
    reduce_only: bool,
}

struct Venue {
    address: SocketAddr,
    observed: Arc<Mutex<Vec<Observed>>>,
    completed: Arc<std::sync::atomic::AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Venue {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let seen = observed.clone();
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done = completed.clone();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                let socket = tokio::select! {
                    _ = &mut stopped => break,
                    socket = listener.accept() => socket.unwrap().0,
                };
                let seen = seen.clone();
                let done = done.clone();
                let replies = replies.clone();
                handlers.spawn(async move {
                    let mut socket = socket;
                    let mut bytes = Vec::new();
                    while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                        let mut chunk = [0; 4096];
                        let count = socket.read(&mut chunk).await.unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&chunk[..count]);
                        assert!(bytes.len() < 16384);
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let mut fields = request.lines().next().unwrap().split_whitespace();
                    let method = fields.next().unwrap();
                    let url = url::Url::parse(&format!("http://{address}{}", fields.next().unwrap())).unwrap();
                    let query = url.query_pairs().collect::<std::collections::HashMap<_, _>>();
                    let reduce_only = query.get("reduceOnly").is_some_and(|v| v == "true");
                    seen.lock().unwrap().push(Observed { method: method.into(), path: url.path().into(), at: Instant::now(), reduce_only });
                    assert!(request.lines().any(|line| line.split_once(':').is_some_and(|(key,value)| key.eq_ignore_ascii_case("X-MBX-APIKEY") && value.trim() == "local-get-order-key")));
                    assert!(query.contains_key("timestamp") && query.contains_key("signature"));
                    let reply = replies.lock().unwrap().pop_front().expect("no unexpected exchange requests");
                    assert_eq!((method, url.path()), (reply.method, reply.path));
                    if method == "POST" { assert!(reduce_only, "fixture permits only reducing submissions"); }
                    tokio::time::sleep(reply.delay).await;
                    if let Some(release) = reply.release { release.acquire().await.unwrap().forget(); }
                    let body = reply.body.to_string();
                    // The client may have cancelled a held response; that is the
                    // behavior under test, not a fixture failure.
                    let _ = socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
                    done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
            }
            drop(listener);
            while let Some(result) = handlers.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            observed,
            completed,
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn wait_count(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.observed.lock().unwrap().len() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("production request reaches loopback");
    }

    async fn wait_completed(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.completed.load(std::sync::atomic::Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the observed loopback handlers drain");
    }

    async fn finish(mut self) -> Vec<Observed> {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        let observed = self.observed.lock().unwrap().clone();
        eprintln!(
            "K03_BUDGET venue_drained requests={} methods_paths={:?}",
            observed.len(),
            observed
                .iter()
                .map(|r| (&r.method, &r.path))
                .collect::<Vec<_>>()
        );
        observed
    }
}
impl Drop for Venue {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn defaults(credential: &str, address: SocketAddr) -> Vec<EnvGuard> {
    let mut env = exchange_env(credential, address);
    for name in [
        "RUSTCORE_BINANCE_WEIGHT_PER_SEC",
        "RUSTCORE_BINANCE_WEIGHT_BURST",
        "RUSTCORE_BINANCE_RPS",
        "RUSTCORE_BINANCE_BURST",
        "RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE",
        "RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M",
        "RUSTCORE_BINANCE_QUEUE_WAIT_MAX_MS",
        "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M",
        "BINANCE_HTTP_TIMEOUT_MS",
    ] {
        env.push(EnvGuard::unset(name));
    }
    env
}

async fn get(
    address: SocketAddr,
    token: String,
    credential: String,
    order: String,
    header: Option<&'static str>,
) -> Result<GetOrderResponse, tonic::Status> {
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(GetOrderRequest {
        credential_id: credential,
        engine_order_id: order,
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    if let Some(header) = header {
        request
            .metadata_mut()
            .insert("grpc-timeout", header.parse().unwrap());
    }
    client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.vnext.EngineExecution/GetOrder"),
            ProstCodec::<GetOrderRequest, GetOrderResponse>::default(),
        )
        .await
        .map(tonic::Response::into_inner)
}

async fn positions(store: &Store, credential: &str, count: usize) {
    for n in 0..count {
        sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,qty,entry_price,state,active_key,entry_epoch,opened_at_ms,updated_at_ms) VALUES (?,?,?,?,'long','1','100','protected',?,1,1,1)")
            .bind(format!("budget-pos-{n}")).bind(credential).bind(format!("budget-bot-{n}")).bind(SYMBOL_KEY).bind(format!("budget-active-{n}")).execute(store.pool()).await.unwrap();
    }
}

fn scope_and_queue(store: &Store, credential: &str) -> (String, CredentialRateLimiter) {
    let client = BinanceFuturesClient::new_with_store(
        keys_from_env(credential).unwrap().unwrap(),
        credential,
        store.clone(),
    )
    .unwrap();
    (
        client.egress_scope().to_string(),
        CredentialRateLimiter::shared(TokenBucketConfig::from_env(None)),
    )
}

async fn wait_queued(limiter: &CredentialRateLimiter, scope: &str) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while limiter.queued(scope) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual production admission waiter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_initial_database_wait_is_bounded_with_missing_short_invalid_and_tiny_headers(
) {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    for (label, header, total_ms) in [
        ("none", None, 9000),
        ("short", Some("1000m"), 900),
        ("invalid", Some("bogus"), 9000),
        ("tiny", Some("1n"), 0),
    ] {
        let credential = format!("budget-db-{label}");
        let (store, path, order) = test_store(label, &credential).await;
        let venue = Venue::start(vec![]).await;
        let _env = defaults(&credential, venue.address);
        let grpc = GrpcHarness::start(store.clone()).await;
        let held = store.pool().acquire().await.unwrap();
        if label == "short" {
            let denied = tokio::time::timeout(
                MARGIN,
                get(
                    grpc.address,
                    "wrong-local-token".into(),
                    credential.clone(),
                    order.clone(),
                    Some("1000m"),
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(denied.code(), Code::Unauthenticated);
            eprintln!("K03_BUDGET auth_negative rpc=Unauthenticated before_database=true");
        }
        let started = Instant::now();
        let error = tokio::time::timeout(
            Duration::from_millis(total_ms) + MARGIN,
            get(
                grpc.address,
                grpc.token.clone(),
                credential.clone(),
                order.clone(),
                header,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error.code(), Code::DeadlineExceeded | Code::Cancelled),
            "{error}"
        );
        if total_ms > 0 {
            assert!(
                error.message().contains("GetOrder total deadline"),
                "{error}"
            );
        }
        eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
        assert!(started.elapsed() <= Duration::from_millis(total_ms) + MARGIN);
        drop(held);
        assert_eq!(
            persisted_status(&store, &order).await,
            ("submitted".into(), ORIGINAL_TIME)
        );
        assert_eq!(event_count(&store, &credential).await, 0);
        grpc.finish().await;
        assert!(venue.finish().await.is_empty());
        cleanup(store, path).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_initial_wait_consumes_the_same_refresh_deadline() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-db-refresh";
    let (store, path, order) = test_store("db-refresh", credential).await;
    let venue = Venue::start(vec![]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let held = store.pool().acquire().await.unwrap();
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("2000m"),
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    drop(held);
    let response = call.await.unwrap().unwrap().order.unwrap();
    eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
    assert!(started.elapsed() <= Duration::from_millis(1600) + MARGIN);
    assert_eq!(
        (response.status, response.updated_at_ms),
        ("submitted".into(), ORIGINAL_TIME)
    );
    assert_eq!(event_count(&store, credential).await, 0);
    grpc.finish().await;
    assert!(venue.finish().await.is_empty());
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_held_http_has_no_header_and_short_header_caps() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    for (label, header, refresh_ms) in [("none", None, 8000), ("short", Some("2000m"), 1600)] {
        let credential = format!("budget-http-{label}");
        let (store, path, order) = test_store(label, &credential).await;
        let release = Arc::new(Semaphore::new(0));
        let mut reply = Reply::order("FILLED");
        reply.release = Some(release.clone());
        let venue = Venue::start(vec![reply, Reply::order("PARTIALLY_FILLED")]).await;
        let _env = defaults(&credential, venue.address);
        let grpc = GrpcHarness::start(store.clone()).await;
        let started = Instant::now();
        let call = tokio::spawn(get(
            grpc.address,
            grpc.token.clone(),
            credential.clone(),
            order.clone(),
            header,
        ));
        venue.wait_count(1).await;
        let response = call.await.unwrap().unwrap().order.unwrap();
        eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
        assert!(started.elapsed() <= Duration::from_millis(refresh_ms) + MARGIN);
        assert_eq!(
            (response.status, response.updated_at_ms),
            ("submitted".into(), ORIGINAL_TIME)
        );
        assert_eq!(
            persisted_status(&store, &order).await,
            ("submitted".into(), ORIGINAL_TIME)
        );
        assert_eq!(event_count(&store, &credential).await, 0);
        // The same credential/route must regain its transport permit while
        // the first FILLED response is still held at the HTTP boundary.
        let successor_started = Instant::now();
        let successor = tokio::time::timeout(
            Duration::from_secs(1) + MARGIN,
            get(
                grpc.address,
                grpc.token.clone(),
                credential.clone(),
                order.clone(),
                Some("2000m"),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .order
        .unwrap();
        venue.wait_count(2).await;
        venue.wait_completed(1).await;
        assert_eq!(venue.observed.lock().unwrap().len(), 2);
        assert_eq!(release.available_permits(), 0);
        assert_eq!(
            venue.completed.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the successor can complete while the original response is held"
        );
        let successor_state = (successor.status, successor.updated_at_ms);
        assert_eq!(successor_state.0, "partial_filled");
        assert_eq!(successor_state, persisted_status(&store, &order).await);
        assert_eq!(event_count(&store, &credential).await, 1);
        eprintln!(
            "K03_BUDGET transport_successor header={label} elapsed_ms={} old_reply_held=true http_requests=2 durable_status=partial_filled durable_time={} events=1",
            successor_started.elapsed().as_millis(), successor_state.1
        );
        // A detached late FILLED would defeat the successor's weaker durable
        // truth. Release only after the successor's status/time/event commit.
        release.add_permits(1);
        venue.wait_completed(2).await;
        tokio::time::sleep(MARGIN).await;
        assert_eq!(persisted_status(&store, &order).await, successor_state);
        assert_eq!(event_count(&store, &credential).await, 1);
        eprintln!("K03_BUDGET old_reply_drained header={label} late_observation_ms=500 durable_pair_unchanged=true events=1");
        grpc.finish().await;
        assert_eq!(venue.finish().await.len(), 2);
        cleanup(store, path).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_ordinary_persistence_wait_is_error_not_fallback() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-persistence";
    let (store, path, order) = test_store("persistence", credential).await;
    let release = Arc::new(Semaphore::new(0));
    let mut reply = Reply::order("FILLED");
    reply.release = Some(release.clone());
    let venue = Venue::start(vec![reply]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("1500m"),
    ));
    venue.wait_count(1).await;
    let held = store.pool().acquire().await.unwrap();
    release.add_permits(1);
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        error.message().contains("GetOrder total deadline"),
        "{error}"
    );
    eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
    assert!(started.elapsed() <= Duration::from_millis(1350) + MARGIN);
    drop(held);
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 1);
    // This fixture held the only connection before BEGIN. It proves this
    // controlled write did not occur, not that an arbitrary timeout rolled back.
    assert_eq!(
        persisted_status(&store, &order).await,
        ("submitted".into(), ORIGINAL_TIME)
    );
    assert_eq!(event_count(&store, credential).await, 0);
    cleanup(store, path).await;
}

async fn seed_algo(store: &Store, credential: &str, order: &str) {
    positions(store, credential, 1).await;
    sqlx::query("UPDATE engine_orders SET status='confirmed', exchange_order_id=NULL, order_role='protection_sl', position_id='budget-pos-0' WHERE engine_order_id=?").bind(order).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,trigger_price,status,verified_from_exchange,exchange_algo_id,client_algo_id,algo_status,created_at_ms,updated_at_ms) VALUES ('budget-protection','budget-pos-0',?,'sl',1,?,'90','confirmed',1,'9101','budget-algo','NEW',1,1)").bind(credential).bind(order).execute(store.pool()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_algo_then_ordinary_share_the_deadline_without_invented_terminal_truth() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-algo-chain";
    let (store, path, order) = test_store("algo-chain", credential).await;
    seed_algo(&store, credential, &order).await;
    let release = Arc::new(Semaphore::new(0));
    let mut ordinary = Reply::order("FILLED");
    ordinary.release = Some(release.clone());
    let venue = Venue::start(vec![Reply { method:"GET", path:"/fapi/v1/algoOrder", body:json!({"algoId":9101,"clientAlgoId":"budget-algo","symbol":"BTCUSDT","algoStatus":"TRIGGERED","actualOrderId":9001,"updateTime":1_900_000_000_000_i64}), delay:Duration::from_millis(300), release:None }, ordinary]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("2000m"),
    ));
    venue.wait_count(2).await;
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        error
            .message()
            .contains("refresh deadline after persistence began"),
        "{error}"
    );
    eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
    assert!(started.elapsed() <= Duration::from_millis(1600) + MARGIN);
    release.add_permits(1);
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 2);
    let state = persisted_status(&store, &order).await;
    eprintln!(
        "K03_BUDGET algo_then_ordinary durable_status={} durable_time={}",
        state.0, state.1
    );
    assert_eq!(state.0, "triggered");
    assert_ne!(state.1, ORIGINAL_TIME);
    assert_eq!(
        event_count(&store, credential).await,
        0,
        "held ordinary FILLED must never persist"
    );
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_algo_success_returns_the_confirmed_persistent_time_pair() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-algo-confirmed";
    let (store, path, order) = test_store("algo-confirmed", credential).await;
    seed_algo(&store, credential, &order).await;
    let venue = Venue::start(vec![Reply { method:"GET", path:"/fapi/v1/algoOrder", body:json!({"algoId":9101,"clientAlgoId":"budget-algo","symbol":"BTCUSDT","algoStatus":"NEW","updateTime":1_900_000_000_000_i64}), delay:Duration::ZERO, release:None }]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let result = get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        None,
    )
    .await
    .unwrap()
    .order
    .unwrap();
    let persisted = persisted_status(&store, &order).await;
    assert_eq!((result.status, result.updated_at_ms), persisted);
    eprintln!(
        "K03_BUDGET algo_confirmed durable_status={} durable_time={}",
        persisted.0, persisted.1
    );
    assert_eq!(persisted.0, "confirmed");
    assert_ne!(persisted.1, ORIGINAL_TIME);
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 1);
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_second_transport_timeout_after_algo_persistence_is_unavailable() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-algo-http-timeout";
    let (store, path, order) = test_store("algo-http-timeout", credential).await;
    seed_algo(&store, credential, &order).await;
    let release = Arc::new(Semaphore::new(0));
    let mut ordinary = Reply::order("FILLED");
    ordinary.release = Some(release.clone());
    let venue = Venue::start(vec![Reply {
        method: "GET", path: "/fapi/v1/algoOrder",
        body: json!({"algoId":9101,"clientAlgoId":"budget-algo","symbol":"BTCUSDT","algoStatus":"TRIGGERED","actualOrderId":9001,"updateTime":1_900_000_000_000_i64}),
        delay: Duration::ZERO, release: None,
    }, ordinary]).await;
    let _env = defaults(credential, venue.address);
    // Fault injection into the existing HTTP timeout, not an admission quota
    // override: the second HTTP times out before the 2.4s refresh deadline.
    let _http_timeout = EnvGuard::set("BINANCE_HTTP_TIMEOUT_MS", "500");
    let grpc = GrpcHarness::start(store.clone()).await;
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("3000m"),
    ));
    venue.wait_count(2).await;
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert!(error
        .message()
        .contains("refresh failed after persistence began"));
    assert!(started.elapsed() <= Duration::from_millis(1500) + MARGIN);
    eprintln!(
        "K03_BUDGET second_http_timeout elapsed_ms={} rpc=Unavailable",
        started.elapsed().as_millis()
    );
    release.add_permits(1);
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 2);
    assert_eq!(persisted_status(&store, &order).await.0, "triggered");
    assert_eq!(event_count(&store, credential).await, 0);
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_cancels_admission_waiter_and_successor_uses_the_same_queue() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-queue";
    let (store, path, order) = test_store("queue", credential).await;
    positions(&store, credential, 5).await;
    let venue = Venue::start(vec![Reply::order("FILLED")]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let (scope, limiter) = scope_and_queue(&store, credential);
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("1000m"),
    ));
    wait_queued(&limiter, &scope).await;
    let first = call.await.unwrap().unwrap().order.unwrap();
    assert_eq!(
        (first.status, first.updated_at_ms),
        ("submitted".into(), ORIGINAL_TIME)
    );
    assert_eq!(
        limiter.queued(&scope),
        0,
        "expired production waiter must unregister"
    );
    assert!(venue.observed.lock().unwrap().is_empty());
    // A real durable exposure change permits the successor. No quota or token
    // mutation is used, and the same route/credential/queue is retained.
    sqlx::query("UPDATE position_state SET state='closed',active_key=NULL WHERE position_id!='budget-pos-0'").execute(store.pool()).await.unwrap();
    let second = get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        None,
    )
    .await
    .unwrap()
    .order
    .unwrap();
    assert_eq!(second.status, "filled");
    assert_eq!(limiter.queued(&scope), 0);
    assert_eq!(event_count(&store, credential).await, 1);
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 1);
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_queued_query_preserves_exit_reserve_through_public_execute_intents() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-exit";
    let (store, path, order) = test_store("exit", credential).await;
    positions(&store, credential, 5).await;
    let venue = Venue::start(vec![Reply { method:"POST", path:"/fapi/v1/order", body:json!({"orderId":9201,"status":"NEW","clientOrderId":"budget-exit","symbol":"BTCUSDT","side":"SELL","type":"MARKET","origQty":"1","executedQty":"0"}), delay:Duration::ZERO, release:None }]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let (scope, limiter) = scope_and_queue(&store, credential);
    let query = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("2000m"),
    ));
    wait_queued(&limiter, &scope).await;
    let channel = Endpoint::from_shared(format!("http://{}", grpc.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(ExecuteIntentsRequest {
        credential_id: credential.into(),
        run_mode: "live".into(),
        intents: vec![OrderIntent {
            bot_uid: "budget-bot-0".into(),
            run_id: "budget-run".into(),
            cycle_id: "budget-close-cycle".into(),
            idempotency_key: "budget-close-idem".into(),
            client_order_id_short: "budget-exit".into(),
            exchange: "binance".into(),
            market_kind: "futures".into(),
            symbol_key: SYMBOL_KEY.into(),
            action: "close_long".into(),
            side: "long".into(),
            order_type: "market".into(),
            quantity: "1".into(),
            reduce_only: true,
            ..Default::default()
        }],
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", grpc.token).parse().unwrap(),
    );
    request.set_timeout(Duration::from_secs(3));
    let started = Instant::now();
    let response: tonic::Response<ExecuteIntentsResponse> = client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.vnext.EngineExecution/ExecuteIntents"),
            ProstCodec::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.get_ref().acks[0].status,
        "accepted",
        "{:?}",
        response.get_ref()
    );
    assert!(
        started.elapsed() <= MARGIN,
        "Exit must use reserved tokens while query waits"
    );
    eprintln!(
        "K03_BUDGET exit elapsed_ms={} query_still_running={}",
        started.elapsed().as_millis(),
        !query.is_finished()
    );
    assert!(
        !query.is_finished(),
        "exit must finish before the queued refresh deadline"
    );
    let result = query.await.unwrap().unwrap().order.unwrap();
    assert_eq!(
        (result.status, result.updated_at_ms),
        ("submitted".into(), ORIGINAL_TIME)
    );
    assert_eq!(limiter.queued(&scope), 0);
    grpc.finish().await;
    let seen = venue.finish().await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/fapi/v1/order");
    assert!(seen[0].reduce_only);
    assert!(seen[0].at >= started);
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_exposure_mapping_separates_real_routes_without_network() {
    // Mapping foundation, explicitly not an RPC/venue proof. Configured real
    // venue names are resolved locally; no client request is sent to either.
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "budget-route-local";
    let (store, path, _) = test_store("routes", credential).await;
    positions(&store, "budget-route-peer", 5).await;
    let _env = [
        EnvGuard::unset("BINANCE_FUTURES_BASE_URL"),
        EnvGuard::unset(crate::venue::FAULT_PROXY_REST_BASE_URL_ENV),
        EnvGuard::unset(crate::venue::FAULT_PROXY_WS_BASE_URL_ENV),
        EnvGuard::set(
            "BINANCE_EGRESS_ROUTE_ID",
            &format!("budget-routes-{}", ulid::Ulid::new()),
        ),
        EnvGuard::set(crate::venue::venue_env_name(credential), "testnet"),
        EnvGuard::set(crate::venue::venue_env_name("budget-route-peer"), "mainnet"),
    ];
    let local = crate::venue::rest_transport_for_credential(credential).unwrap();
    let scope =
        crate::binance_futures::egress_scope_for_transport(local.venue, &local.base_url).unwrap();
    assert_eq!(
        crate::binance_futures::open_positions_on_egress(&store, &scope)
            .await
            .unwrap(),
        0
    );
    let same = EnvGuard::set(crate::venue::venue_env_name("budget-route-peer"), "testnet");
    assert_eq!(
        crate::binance_futures::open_positions_on_egress(&store, &scope)
            .await
            .unwrap(),
        5
    );
    drop(same);
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_mysql_algo_postcommit_deadline_is_never_successful_fallback() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "getbudget",
        "get_order_budget_mysql_algo_postcommit_deadline_is_never_successful_fallback",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let credential = "budget-algo-postcommit";
    let order = seed_order(&store, "algo-postcommit", credential, "submitted").await;
    seed_algo(&store, credential, &order).await;
    sqlx::query("INSERT INTO engine_checkpoints (credential_id,last_seq_emitted,updated_at_ms) VALUES (?,0,1)")
        .bind(credential).execute(store.pool()).await.unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT credential_id FROM engine_checkpoints WHERE credential_id=? FOR UPDATE")
        .bind(credential)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let venue = Venue::start(vec![Reply {
        method: "GET", path: "/fapi/v1/algoOrder",
        body: json!({"algoId":9101,"clientAlgoId":"budget-algo","symbol":"BTCUSDT","algoStatus":"TRIGGERED","actualOrderId":9001,"updateTime":1_900_000_000_000_i64}),
        delay: Duration::ZERO, release: None,
    }]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("3000m"),
    ));
    venue.wait_count(1).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let waiting: i64 = sqlx::query(
                "SELECT COUNT(*) AS n FROM performance_schema.data_lock_waits w \
                 JOIN performance_schema.data_locks l ON w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID \
                 WHERE l.OBJECT_SCHEMA=DATABASE() AND l.OBJECT_NAME='engine_checkpoints'"
            ).fetch_one(store.pool()).await.unwrap().get("n");
            if waiting > 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("Algo audit transaction really waits on our checkpoint after state commit");
    let before_deadline = persisted_status(&store, &order).await;
    assert_eq!(before_deadline.0, "triggered");
    assert_ne!(before_deadline.1, ORIGINAL_TIME);
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(error.message().contains("GetOrder total deadline"));
    assert!(started.elapsed() <= Duration::from_millis(2700) + MARGIN);
    eprintln!("K03_BUDGET algo_postcommit elapsed_ms={} rpc=DeadlineExceeded durable_status={} durable_time={} audit_lock_wait_observed=true", started.elapsed().as_millis(), before_deadline.0, before_deadline.1);
    blocker.rollback().await.unwrap();
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.pool().num_idle() != store.pool().size() as usize {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled Algo audit transaction settles");
    assert_eq!(persisted_status(&store, &order).await, before_deadline);
    let events: i64 =
        sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE credential_id=?")
            .bind(credential)
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get("n");
    assert_eq!(
        events, 0,
        "bounded cancellation did not finish the held audit"
    );
    tokio::time::timeout(Duration::from_secs(5), store.pool().close())
        .await
        .unwrap();
    drop(store);
    tokio::time::timeout(Duration::from_secs(10), scratch.cleanup())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_order_budget_mysql_active_transaction_wait_returns_deadline_not_success() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "getbudget",
        "get_order_budget_mysql_active_transaction_wait_returns_deadline_not_success",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let credential = "budget-mysql-transaction";
    let order = seed_order(&store, "mysql-budget", credential, "submitted").await;
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT engine_order_id FROM engine_orders WHERE engine_order_id=? FOR UPDATE")
        .bind(&order)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let venue = Venue::start(vec![Reply::order("FILLED")]).await;
    let _env = defaults(credential, venue.address);
    let grpc = GrpcHarness::start(store.clone()).await;
    let started = Instant::now();
    let call = tokio::spawn(get(
        grpc.address,
        grpc.token.clone(),
        credential.into(),
        order.clone(),
        Some("3000m"),
    ));
    venue.wait_count(1).await;
    // Observe the actual UPDATE waiting on our fixture row. An HTTP request
    // alone would not prove that a database transaction had begun.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let waiting: i64 = sqlx::query(
                "SELECT COUNT(*) AS n FROM performance_schema.data_lock_waits w \
                 JOIN performance_schema.data_locks l ON w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID \
                 WHERE l.OBJECT_SCHEMA=DATABASE()"
            ).fetch_one(store.pool()).await.unwrap().get("n");
            if waiting > 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("GetOrder has a real MySQL transaction in LOCK WAIT");
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        error.message().contains("GetOrder total deadline"),
        "{error}"
    );
    eprintln!("K03_BUDGET elapsed_ms={}", started.elapsed().as_millis());
    assert!(started.elapsed() <= Duration::from_millis(2700) + MARGIN);
    eprintln!("K03_BUDGET mysql_active_transaction elapsed_ms={} rpc=DeadlineExceeded lock_wait_observed=true", started.elapsed().as_millis());
    blocker.rollback().await.unwrap();
    grpc.finish().await;
    assert_eq!(venue.finish().await.len(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.pool().num_idle() != store.pool().size() as usize {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled SQLx transaction settles after releasing fixture lock");
    assert_eq!(
        persisted_status(&store, &order).await,
        ("submitted".into(), ORIGINAL_TIME)
    );
    assert_eq!(event_count(&store, credential).await, 0);
    tokio::time::timeout(Duration::from_secs(5), store.pool().close())
        .await
        .unwrap();
    drop(store);
    tokio::time::timeout(Duration::from_secs(10), scratch.cleanup())
        .await
        .unwrap();
}
