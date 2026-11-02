//! Explicit real-MySQL production RPC proof. No environment means failure when
//! selected with --ignored; normal offline runs report this test as ignored.
use crate::{proto_v2 as p, store::Store};
use sqlx::{Acquire, AnyConnection, Row};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Point {
    Before,
    Configured,
    Orders,
    Finished,
}
#[derive(Default)]
struct Report {
    connection_id: i64,
    before: BTreeMap<String, u64>,
    after: BTreeMap<String, u64>,
}
struct Probe {
    boot: String,
    pause_at: Point,
    entered: Notify,
    resume: Notify,
    report: Mutex<Report>,
}
fn registry() -> &'static Mutex<Option<Arc<Probe>>> {
    static PROBE: OnceLock<Mutex<Option<Arc<Probe>>>> = OnceLock::new();
    PROBE.get_or_init(|| Mutex::new(None))
}
struct Installed(Arc<Probe>);
impl Drop for Installed {
    fn drop(&mut self) {
        let mut current = registry().lock().unwrap();
        if current.as_ref().is_some_and(|p| Arc::ptr_eq(p, &self.0)) {
            *current = None;
        }
    }
}
fn install(store: &Store, pause_at: Point) -> Installed {
    let probe = Arc::new(Probe {
        boot: store.boot_id().into(),
        pause_at,
        entered: Notify::new(),
        resume: Notify::new(),
        report: Mutex::new(Report::default()),
    });
    assert!(registry().lock().unwrap().replace(probe.clone()).is_none());
    Installed(probe)
}
async fn writes(connection: &mut AnyConnection) -> BTreeMap<String, u64> {
    sqlx::query("SHOW SESSION STATUS WHERE Variable_name IN ('Handler_write','Handler_update','Handler_delete')")
        .fetch_all(connection).await.expect("read this connection's write counters")
        .into_iter().map(|row| (
            row.get::<String,_>("Variable_name"),
            row.get::<String,_>("Value").parse().unwrap(),
        )).collect()
}
// Compiled out of production. Only the explicitly installed Store boot can
// pause; other parallel tests and every normal RPC skip all instrumentation.
pub(super) async fn checkpoint(store: &Store, connection: &mut AnyConnection, point: Point) {
    let probe = {
        registry()
            .lock()
            .unwrap()
            .as_ref()
            .filter(|p| p.boot == store.boot_id())
            .cloned()
    };
    let Some(probe) = probe else {
        return;
    };
    if point == Point::Before {
        let id = sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED) AS id")
            .fetch_one(&mut *connection)
            .await
            .unwrap()
            .get::<i64, _>("id");
        let before = writes(connection).await;
        let mut report = probe.report.lock().unwrap();
        report.connection_id = id;
        report.before = before;
    }
    if point == Point::Finished {
        let after = writes(connection).await;
        probe.report.lock().unwrap().after = after;
    }
    if point == probe.pause_at {
        probe.entered.notify_one();
        probe.resume.notified().await;
    }
}

struct Rpc {
    store: Store,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Rpc {
    async fn new(store: Store) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let execution = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks();
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            execution.clone(),
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
            crate::svc_v2::EngineExecutionV2Service::with_shared_execution(
                store.clone(),
                execution,
            ),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![
                "mysql-account-order-test-token".into(),
            ])),
        );
        let task = tokio::spawn(async move {
            router.serve_with_incoming(incoming).await.unwrap();
        });
        Self {
            store,
            address,
            task,
        }
    }
    async fn read(
        &self,
        credential: &str,
        limit: u32,
        timeout: Duration,
        token: &str,
    ) -> Result<p::GetAccountOrderStateResponse, tonic::Status> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let mut request = tonic::Request::new(p::GetAccountOrderStateRequest {
            credential_id: credential.into(),
            max_items_per_section: limit,
            expected_boot_id: Some(self.store.boot_id().into()),
            schema_version: 1,
        });
        request.set_timeout(timeout);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let response: tonic::Response<p::GetAccountOrderStateResponse> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/GetAccountOrderState",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await?;
        Ok(response.into_inner())
    }
    async fn get(&self, credential: &str, limit: u32) -> p::GetAccountOrderStateResponse {
        self.read(
            credential,
            limit,
            Duration::from_secs(5),
            "mysql-account-order-test-token",
        )
        .await
        .expect("authenticated production MySQL RPC")
    }
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        self.store.pool().close().await;
    }
}

fn mysql_environment() -> Vec<crate::binance_futures::tests::TestEnvVar> {
    use crate::binance_futures::tests::TestEnvVar;
    let mut guards = vec![];
    if let Some(path) = std::env::var_os("RUSTCORE_ACCOUNT_ORDER_MYSQL_URL_FILE") {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&path).expect("read MySQL URL file metadata");
            assert!(
                metadata.is_file() && metadata.permissions().mode() & 0o077 == 0,
                "MySQL URL file must be a private regular file (0600)"
            );
        }
        let value = std::fs::read_to_string(path).expect("read private MySQL URL file");
        guards.push(TestEnvVar::set(
            "RUSTCORE_TEST_MYSQL_URL".into(),
            value.trim(),
        ));
    }
    let value = std::env::var("RUSTCORE_TEST_MYSQL_URL").expect("explicit MySQL test needs RUSTCORE_TEST_MYSQL_URL or RUSTCORE_ACCOUNT_ORDER_MYSQL_URL_FILE");
    assert!(
        value.starts_with("mysql://"),
        "explicit MySQL test needs a mysql URL"
    );
    guards.push(TestEnvVar::set("RUSTCORE_REQUIRE_MYSQL_TESTS".into(), "1"));
    guards
}
const TABLES: [&str; 4] = [
    "engine_orders",
    "position_state",
    "position_protection_plans",
    "protection_orders",
];
async fn seed(store: &Store, credential: &str, prefix: &str, count: usize, marker: i64) {
    for n in 0..count {
        let id = format!("{prefix}-{n}");
        sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES (?,?,'mysql-bot','BTCUSDT','long',1,'protected','1.25',?)")
            .bind(&id).bind(credential).bind(marker).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,status,position_id,created_at_ms,updated_at_ms) VALUES (?,?,'mysql-bot',?,'binance','futures','BTCUSDT','open_long','buy','limit','1.25','unknown',?,1,?)")
            .bind(&id).bind(credential).bind(&id).bind(&id).bind(marker).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO position_protection_plans (position_id,credential_id,bot_uid,exchange,market_kind,symbol_key,direction,stop_loss_price,take_profit_price,confirm_timeout_ms,generation,created_at_ms,updated_at_ms) VALUES (?,?,'mysql-bot','binance','futures','BTCUSDT','long','48000','55000',5000,1,1,?)")
            .bind(&id).bind(credential).bind(marker).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,trigger_price,status,created_at_ms,updated_at_ms) VALUES (?,?,?,'sl',1,?,'48000','unknown',1,?)")
            .bind(&id).bind(&id).bind(credential).bind(&id).bind(marker).execute(store.pool()).await.unwrap();
    }
}
fn assert_sections(
    response: &p::GetAccountOrderStateResponse,
    credential: &str,
    count: usize,
    marker: i64,
) {
    assert_eq!(response.credential_id, credential);
    let l = response.local.as_ref().unwrap();
    let o = l.orders.as_ref().unwrap();
    let p = l.positions.as_ref().unwrap();
    let plans = l.protection_plans.as_ref().unwrap();
    let legs = l.protection_orders.as_ref().unwrap();
    for (meta, n) in [
        (&o.meta, o.rows.len()),
        (&p.meta, p.rows.len()),
        (&plans.meta, plans.rows.len()),
        (&legs.meta, legs.rows.len()),
    ] {
        let m = meta.as_ref().unwrap();
        assert_eq!(n, count);
        assert_eq!(m.returned_count as usize, count);
        assert_eq!(m.total_count, Some(count as u32));
        assert!(!m.overflow);
        assert_eq!(m.coverage, p::AccountOrderCoverage::AocCompleteLocal as i32);
    }
    assert!(o
        .rows
        .iter()
        .all(|r| r.credential_id == credential && r.updated_at_ms == marker));
    assert!(p
        .rows
        .iter()
        .all(|r| r.credential_id == credential && r.updated_at_ms == marker));
    assert!(plans
        .rows
        .iter()
        .all(|r| r.credential_id == credential && r.updated_at_ms == marker));
    assert!(legs
        .rows
        .iter()
        .all(|r| r.credential_id == credential && r.updated_at_ms == marker));
}
async fn next_borrower_is_writable(store: &Store, closed_id: Option<i64>) {
    let mut connection = store.pool().acquire().await.unwrap();
    let row=sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED) AS id, @@session.transaction_isolation AS session_iso, @@global.transaction_isolation AS global_iso, CAST(@@session.transaction_read_only AS SIGNED) AS read_only")
        .fetch_one(&mut *connection).await.unwrap();
    if let Some(id) = closed_id {
        assert_ne!(
            row.get::<i64, _>("id"),
            id,
            "configured canceled connection must be discarded"
        );
    }
    assert_eq!(
        row.get::<String, _>("session_iso"),
        row.get::<String, _>("global_iso")
    );
    assert_eq!(row.get::<i64, _>("read_only"), 0);
    let mut tx = connection.begin().await.unwrap();
    assert_eq!(
        sqlx::query("UPDATE engine_orders SET quantity='9' WHERE engine_order_id='own-0'")
            .execute(&mut *tx)
            .await
            .expect("next pool borrower retains write authority")
            .rows_affected(),
        1
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query("SELECT quantity FROM engine_orders WHERE engine_order_id='own-0'")
            .fetch_one(&mut *connection)
            .await
            .unwrap()
            .get::<String, _>("quantity"),
        "1.25"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly configured private MySQL scratch server; no skip fallback"]
async fn production_rpc_mysql_snapshot_scope_overflow_and_pool_restoration() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let _environment = mysql_environment();
    let scratch = crate::store::mysql_test_support::MySqlScratch::create(
        "aoread",
        "account-order MySQL production RPC",
    )
    .await
    .expect("explicit MySQL scratch setup must succeed");
    let store = scratch.connect_store().await;
    let rpc = Rpc::new(store.clone()).await;
    seed(&store, "7", "own", 2, 101).await;
    seed(&store, "8", "foreign", 1, 303).await;
    let paper = format!("paper:{}", "a".repeat(32));
    seed(&store, &paper, "lower", 1, 404).await;
    seed(&store, &paper.to_ascii_uppercase(), "upper", 1, 505).await;
    // The counterexample really is ambiguous under the MySQL collation, and
    // therefore relies on byte-exact credential checks, not request validation.
    let matches = sqlx::query(
        "SELECT CAST(COUNT(*) AS SIGNED) AS n FROM engine_orders WHERE credential_id=?",
    )
    .bind(&paper)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get::<i64, _>("n");
    assert_eq!(
        matches, 2,
        "MySQL scratch must exercise case-insensitive credential collation"
    );
    assert_eq!(
        rpc.read("7", 0, Duration::from_secs(5), "wrong")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_sections(&rpc.get(&paper, 0).await, &paper, 1, 404);
    assert_sections(&rpc.get("8", 0).await, "8", 1, 303);

    let probe = install(&store, Point::Orders);
    let read = rpc.get("7", 0);
    let update = async {
        tokio::time::timeout(Duration::from_secs(3), probe.0.entered.notified())
            .await
            .expect("RPC must reach first real SELECT");
        let mut tx = store.pool().begin().await.unwrap();
        for table in TABLES {
            sqlx::query(&format!(
                "UPDATE {table} SET updated_at_ms=202 WHERE credential_id='7'"
            ))
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        probe.0.resume.notify_one();
    };
    let (snapshot, ()) = tokio::join!(read, update);
    assert_sections(&snapshot, "7", 2, 101);
    assert_eq!(snapshot.engine_boot_id, store.boot_id());
    {
        let report = probe.0.report.lock().unwrap();
        assert_eq!(report.before.len(), 3);
        assert_eq!(
            report.before, report.after,
            "production read must perform no inserts, updates, or deletes on its connection"
        );
    }
    drop(probe);
    assert_sections(&rpc.get("7", 0).await, "7", 2, 202);
    let limited = rpc.get("7", 1).await.local.unwrap();
    for meta in [
        limited.orders.unwrap().meta,
        limited.positions.unwrap().meta,
        limited.protection_plans.unwrap().meta,
        limited.protection_orders.unwrap().meta,
    ] {
        let m = meta.unwrap();
        assert!(m.overflow);
        assert_eq!(m.returned_count, 1);
        assert_eq!(m.total_count, None);
        assert_eq!(m.coverage, p::AccountOrderCoverage::AocPartial as i32);
    }
    next_borrower_is_writable(&store, None).await;

    // Occupy every other connection so the configured connection cannot be
    // hidden by borrowing an unrelated idle one after cancellation.
    let mut held = Vec::new();
    for _ in 0..7 {
        held.push(store.pool().acquire().await.unwrap());
    }
    let probe = install(&store, Point::Configured);
    let read = rpc.read(
        "7",
        0,
        Duration::from_millis(500),
        "mysql-account-order-test-token",
    );
    let wait = async {
        tokio::time::timeout(Duration::from_secs(3), probe.0.entered.notified())
            .await
            .expect("RPC must pass SET TRANSACTION before being canceled");
    };
    let (result, ()) = tokio::join!(read, wait);
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    let configured_id = probe.0.report.lock().unwrap().connection_id;
    drop(probe);
    next_borrower_is_writable(&store, Some(configured_id)).await;
    drop(held);
    assert_sections(&rpc.get("7", 0).await, "7", 2, 202);
    rpc.stop().await;
    store.pool().close().await;
    scratch.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly configured private MySQL scratch server; no skip fallback"]
async fn production_rpc_mysql_closed_paper_cleanup_proof() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let _environment = mysql_environment();
    let scratch = crate::store::mysql_test_support::MySqlScratch::create(
        "aopaper",
        "closed paper account-order proof",
    )
    .await
    .expect("explicit MySQL scratch setup must succeed");
    let store = scratch.connect_store().await;
    let rpc = Rpc::new(store.clone()).await;
    let paper = "paper:0123456789abcdef0123456789abcdef";
    seed(&store, paper, "closed-paper", 1, 101).await;
    sqlx::query("UPDATE engine_orders SET status='acked',order_role='protection_sl'")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE protection_orders SET status='confirmed'")
        .execute(store.pool())
        .await
        .unwrap();
    let mut tx = store.pool().begin().await.unwrap();
    crate::position_state::mark_position_closed(&mut tx, "closed-paper-0", 202, None, "paper_fill")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let closed = rpc.get(paper, 0).await.local.unwrap();
    assert!(
        closed.orders.unwrap().rows.is_empty(),
        "no working simulation order survives close"
    );
    assert!(closed.positions.unwrap().rows.is_empty());
    assert!(closed.protection_orders.unwrap().rows.is_empty());
    let status: String = sqlx::query_scalar("SELECT status FROM protection_orders LIMIT 1")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(status, "canceled");
    // Deliberately restore the pre-fix residual for the existing passive-read
    // revision/late-ACK negative cases, after proving real cleanup above.
    sqlx::query("UPDATE protection_orders SET status='confirmed'")
        .execute(store.pool())
        .await
        .unwrap();
    let read = rpc.get(paper, 0).await;
    let local = read.local.unwrap();
    assert_eq!(
        local.positions.unwrap().rows[0].paper_cleanup_complete,
        Some(true)
    );
    assert_eq!(
        local.protection_orders.unwrap().rows[0].raw_status,
        "confirmed"
    );
    sqlx::query("UPDATE engine_orders SET exchange_order_id='late-ack'")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        rpc.get(paper, 0)
            .await
            .local
            .unwrap()
            .positions
            .unwrap()
            .rows[0]
            .paper_cleanup_complete,
        Some(false)
    );
    sqlx::query("UPDATE engine_orders SET exchange_order_id=NULL")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE protection_cleanup_obligations SET close_revision=0")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        rpc.get(paper, 0)
            .await
            .local
            .unwrap()
            .positions
            .unwrap()
            .rows[0]
            .paper_cleanup_complete,
        Some(false)
    );
    rpc.stop().await;
    store.pool().close().await;
    scratch.cleanup().await;
}
