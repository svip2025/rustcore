//! Production authenticated RPC, durable rows, no venue request or write on reads.
use crate::{proto_v2 as p, store::Store};
use sqlx::Row;
use std::time::Duration;
use tonic::{Request, Status};

pub(crate) struct Fixture {
    pub(crate) store: Store,
    path: std::path::PathBuf,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    pub(crate) async fn new() -> Self {
        sqlx::any::install_default_drivers();
        let path =
            std::env::temp_dir().join(format!("account-order-read-{}.db", ulid::Ulid::new()));
        Self::at(path).await
    }
    async fn at(path: std::path::PathBuf) -> Self {
        sqlx::any::install_default_drivers();
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let exec = crate::svc::EngineExecutionService::new(store.clone());
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            exec.clone(),
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
            crate::svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![
                "account-read-test-token".into()
            ])),
        );
        let task = tokio::spawn(async move {
            router.serve_with_incoming(incoming).await.unwrap();
        });
        Self {
            store,
            path,
            address,
            task,
        }
    }
    pub(crate) async fn rpc(
        &self,
        credential: &str,
        limit: u32,
        boot: Option<String>,
    ) -> Result<p::GetAccountOrderStateResponse, Status> {
        self.call(
            credential,
            limit,
            boot,
            "account-read-test-token",
            Duration::from_secs(5),
        )
        .await
    }
    async fn call(
        &self,
        credential: &str,
        limit: u32,
        boot: Option<String>,
        token: &str,
        deadline: Duration,
    ) -> Result<p::GetAccountOrderStateResponse, Status> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let mut req = Request::new(p::GetAccountOrderStateRequest {
            credential_id: credential.into(),
            max_items_per_section: limit,
            expected_boot_id: boot,
            schema_version: 1,
        });
        req.set_timeout(deadline);
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let response: tonic::Response<p::GetAccountOrderStateResponse> = client
            .unary(
                req,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.v2.EngineExecution/GetAccountOrderState",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await?;
        Ok(response.into_inner())
    }
    pub(crate) async fn cleanup(self) {
        self.task.abort();
        let _ = self.task.await;
        self.store.pool().close().await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

async fn order(f: &Fixture, id: &str, credential: &str, status: &str) {
    sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,status,created_at_ms,updated_at_ms) VALUES (?,?,?,?,'binance','futures','BTCUSDT','open_long','buy','limit','1.25',?,1788000000123,1788000000123)")
        .bind(id).bind(credential).bind(format!("bot-{id}")).bind(id).bind(status).execute(f.store.pool()).await.unwrap();
}

#[tokio::test]
async fn account_order_read_rpc_empty_local_is_not_venue_truth() {
    let f = Fixture::new().await;
    let r = f
        .rpc("7", 0, None)
        .await
        .expect("implemented production read RPC");
    assert_eq!(r.credential_id, "7");
    assert_eq!(r.engine_boot_id, f.store.boot_id());
    assert_eq!(r.account_kind, "exchange");
    let local = r.local.unwrap();
    let orders = local.orders.unwrap();
    assert!(orders.rows.is_empty());
    assert_eq!(
        orders.meta.unwrap().coverage,
        p::AccountOrderCoverage::AocCompleteLocal as i32
    );
    let venue = r.venue.unwrap();
    assert_eq!(
        venue.availability,
        p::AccountOrderAvailability::AoaUnknown as i32
    );
    assert_eq!(venue.reason_code, "not_measured");
    assert!(venue.source_as_of_ms.is_none());
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_rpc_scope_unknown_status_overflow_and_no_write() {
    let f = Fixture::new().await;
    for (id, cred, status) in [
        ("a", "7", "pending_submit"),
        ("b", "7", "unknown"),
        ("c", "7", "ACKED"),
        ("d", "7", "filled"),
        ("foreign", "8", "acked"),
    ] {
        order(&f, id, cred, status).await;
    }
    let before = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    let r = f.rpc("7", 2, None).await.unwrap();
    let section = r.local.unwrap().orders.unwrap();
    assert_eq!(section.rows.len(), 2);
    assert!(section.rows.iter().all(|r| r.credential_id == "7"));
    let meta = section.meta.unwrap();
    assert!(meta.overflow);
    assert_eq!(meta.coverage, p::AccountOrderCoverage::AocPartial as i32);
    assert!(meta.total_count.is_none());
    let r = f.rpc("7", 1000, None).await.unwrap();
    let section = r.local.unwrap().orders.unwrap();
    assert_eq!(
        section
            .rows
            .iter()
            .map(|r| r.engine_order_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    assert_eq!(section.rows[2].classified_state, "unknown");
    assert_eq!(section.rows[0].quantity, "1.25");
    assert_eq!(section.rows[0].updated_at_ms, 1788000000123);
    let after = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(before, after);
    assert_eq!(
        f.rpc("8", 0, None)
            .await
            .unwrap()
            .local
            .unwrap()
            .orders
            .unwrap()
            .rows[0]
            .engine_order_id,
        "foreign"
    );
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_rpc_rejects_bad_identity_boot_and_auth() {
    let f = Fixture::new().await;
    for bad in ["", " 7", "07", "cred:7", "cred_7", "paper", "paper:bad"] {
        assert_eq!(
            f.rpc(bad, 0, None).await.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{bad}"
        );
    }
    assert_eq!(
        f.rpc("7", 1001, None).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        f.rpc("7", 0, Some("old-boot".into()))
            .await
            .err()
            .map(|error| error.code()),
        Some(tonic::Code::FailedPrecondition)
    );
    assert_eq!(
        f.call("7", 0, None, "wrong", Duration::from_secs(1))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        f.rpc(&format!("paper:{}", "a".repeat(32)), 0, None)
            .await
            .unwrap()
            .account_kind,
        "simulation"
    );
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_rpc_multigeneration_orphans_and_wire_vector() {
    use prost::Message;
    let f = Fixture::new().await;
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../tests/fixtures/account-order-state-v1.sql.json"
    ))
    .unwrap();
    for query in vector["sql"].as_array().unwrap() {
        sqlx::query(query.as_str().unwrap())
            .execute(f.store.pool())
            .await
            .unwrap();
    }
    let r = f.rpc("7", 0, None).await.unwrap();
    let local = r.local.as_ref().unwrap();
    assert_eq!(local.orders.as_ref().unwrap().rows.len(), 1);
    assert_eq!(local.positions.as_ref().unwrap().rows.len(), 1);
    let plans = &local.protection_plans.as_ref().unwrap().rows;
    assert_eq!(plans.len(), 2);
    assert!(plans
        .iter()
        .any(|r| r.position_id == "orphan-plan" && r.association_status == "missing_position"));
    let legs = &local.protection_orders.as_ref().unwrap().rows;
    assert_eq!(legs.len(), 3);
    assert!(legs.iter().all(|r| r.credential_id == "7"));
    let old = legs
        .iter()
        .find(|r| r.protection_order_id == "sl-7-1")
        .unwrap();
    assert_eq!(old.current_plan_generation_match, Some(false));
    assert_eq!(old.legacy_exchange_order_id.as_deref(), Some("child-7-1"));
    assert!(old.actual_order_id.is_none());
    let new = legs
        .iter()
        .find(|r| r.protection_order_id == "sl-7-2")
        .unwrap();
    assert_eq!(new.current_plan_generation_match, Some(true));
    assert_eq!(new.actual_order_id.as_deref(), Some("child-7-2"));
    assert!(new.legacy_exchange_order_id.is_none());
    assert!(legs.iter().any(
        |r| r.protection_order_id == "orphan-leg" && r.association_status == "missing_position"
    ));
    let limited = f.rpc("7", 1, None).await.unwrap().local.unwrap();
    assert!(!limited.orders.unwrap().meta.unwrap().overflow);
    assert!(limited.protection_plans.unwrap().meta.unwrap().overflow);
    assert!(limited.protection_orders.unwrap().meta.unwrap().overflow);
    let foreign = f.rpc("8", 0, None).await.unwrap().local.unwrap();
    assert_eq!(foreign.protection_orders.unwrap().rows.len(), 2);
    // Optional durable one-sided vector: production RPC bytes, never a mocked envelope.
    if let Ok(path) = std::env::var("ACCOUNT_ORDER_WIRE_VECTOR") {
        std::fs::write(path, r.encode_to_vec()).unwrap();
    }
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_deadline_bounds_pool_wait_and_bad_decimal_is_not_empty() {
    let f = Fixture::new().await;
    let held = f.store.pool().acquire().await.unwrap();
    let began = tokio::time::Instant::now();
    let error = f
        .call(
            "7",
            0,
            None,
            "account-read-test-token",
            Duration::from_millis(150),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(began.elapsed() < Duration::from_secs(1));
    drop(held);
    order(&f, "bad", "7", "acked").await;
    sqlx::query("UPDATE engine_orders SET quantity='.' WHERE engine_order_id='bad'")
        .execute(f.store.pool())
        .await
        .unwrap();
    assert_eq!(
        f.rpc("7", 0, None).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_observation_real_restart_retains_rows_but_not_venue_proof() {
    let f = Fixture::new().await;
    order(&f, "durable-order", "7", "acked").await;
    let now = crate::svc::now_ms() - 5;
    let mut observation =
        crate::account_order_observation::Observation::begin(&f.store, "7", "full_account", now);
    observation.ordinary(&serde_json::json!([]), now, now + 1);
    observation.algo(&serde_json::json!([]), now + 1, now + 2);
    observation.finish(
        crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        },
        now + 2,
    );
    assert_eq!(
        f.rpc("7", 0, None)
            .await
            .unwrap()
            .venue
            .unwrap()
            .availability,
        p::AccountOrderAvailability::AoaAvailable as i32
    );
    f.store.pool().close().await;
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "account_order_read_tests::account_order_observation_restart_child",
            "--nocapture",
        ])
        .env("ACCOUNT_ORDER_RESTART_DATABASE", &f.path)
        .env("ACCOUNT_ORDER_PREVIOUS_BOOT", f.store.boot_id())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child restart failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
    f.cleanup().await;
}
#[tokio::test]
#[ignore = "child process invoked by restart regression"]
async fn account_order_observation_restart_child() {
    let path = std::env::var("ACCOUNT_ORDER_RESTART_DATABASE").expect("parent database");
    let old_boot = std::env::var("ACCOUNT_ORDER_PREVIOUS_BOOT").expect("parent boot");
    let f = Fixture::at(path.into()).await;
    assert_ne!(f.store.boot_id(), old_boot);
    assert_eq!(
        f.rpc("7", 0, Some(old_boot.clone()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let r = f.rpc("7", 0, None).await.unwrap();
    assert_eq!(r.venue.unwrap().reason_code, "not_measured");
    assert_eq!(
        r.local.unwrap().orders.unwrap().rows[0].engine_order_id,
        "durable-order"
    );
    println!(
        "restart proof: old_boot={} new_boot={} durable_row=durable-order venue=not_measured",
        old_boot,
        f.store.boot_id()
    );
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_terminal_orphan_missing_plan_is_retained() {
    let f = Fixture::new().await;
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES('closed','7','bot','BTCUSDT','long',1,'closed','0',100)").execute(f.store.pool()).await.unwrap();
    order(&f, "finished", "7", "filled").await;
    sqlx::query("UPDATE engine_orders SET position_id='closed' WHERE engine_order_id='finished'")
        .execute(f.store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO protection_orders(protection_order_id,position_id,credential_id,kind,generation,engine_order_id,trigger_price,status,created_at_ms,updated_at_ms) VALUES('missing-plan','closed','7','sl',1,'finished','48000','filled',100,100)").execute(f.store.pool()).await.unwrap();
    let r = f.rpc("7", 0, None).await.unwrap();
    let rows = r.local.unwrap().protection_orders.unwrap().rows;
    assert_eq!(
        rows.len(),
        1,
        "terminal history is excluded only with complete associations"
    );
    assert_eq!(rows[0].association_status, "missing_plan");
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_read_closed_paper_cleanup_proof_is_revision_bound_and_passive() {
    use prost::Message;
    let f = Fixture::new().await;
    let paper = "paper:0123456789abcdef0123456789abcdef";
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES('paper-closed',?,'bot-paper','BTCUSDT','long',1,'protected','1.25',1788000000123)")
        .bind(paper).execute(f.store.pool()).await.unwrap();
    order(&f, "paper-sl", paper, "acked").await;
    sqlx::query("UPDATE engine_orders SET position_id='paper-closed',order_role='protection_sl' WHERE engine_order_id='paper-sl'").execute(f.store.pool()).await.unwrap();
    sqlx::query("INSERT INTO position_protection_plans(position_id,credential_id,bot_uid,exchange,market_kind,symbol_key,direction,stop_loss_price,take_profit_price,confirm_timeout_ms,generation,created_at_ms,updated_at_ms) VALUES('paper-closed',?,'bot-paper','binance','futures','BTCUSDT','long','48000','55000',5000,1,1788000000123,1788000000123)")
        .bind(paper).execute(f.store.pool()).await.unwrap();
    sqlx::query("INSERT INTO protection_orders(protection_order_id,position_id,credential_id,kind,generation,engine_order_id,client_algo_id,trigger_price,status,verified_from_exchange,last_verified_at_ms,created_at_ms,updated_at_ms) VALUES('paper-sl','paper-closed',?,'sl',1,'paper-sl','local-request-only','48000','confirmed',1,1788000000123,1788000000123,1788000000123)")
        .bind(paper).execute(f.store.pool()).await.unwrap();
    // Produce the obligation through the actual close transaction, not a fixture
    // that invents a completion proof independently of its production writer.
    let mut tx = f.store.pool().begin().await.unwrap();
    crate::position_state::mark_position_closed(
        &mut tx,
        "paper-closed",
        1788000000223,
        None,
        "paper_fill",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let closed = f.rpc(paper, 0, None).await.unwrap().local.unwrap();
    assert!(
        closed.orders.unwrap().rows.is_empty(),
        "no working simulation order survives close"
    );
    assert!(closed.positions.unwrap().rows.is_empty());
    assert!(closed.protection_orders.unwrap().rows.is_empty());
    let status: String = sqlx::query_scalar(
        "SELECT status FROM protection_orders WHERE protection_order_id='paper-sl'",
    )
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    assert_eq!(status, "canceled");
    // Retain the legacy residual projection fixture for passive proof checks.
    // This deliberate test fault is not the just-proven production close state.
    sqlx::query(
        "UPDATE protection_orders SET status='confirmed' WHERE protection_order_id='paper-sl'",
    )
    .execute(f.store.pool())
    .await
    .unwrap();
    let r = f.rpc(paper, 0, None).await.unwrap();
    let local = r.local.as_ref().unwrap();
    assert_eq!(
        local.positions.as_ref().unwrap().rows[0].paper_cleanup_complete,
        Some(true)
    );
    assert_eq!(local.protection_plans.as_ref().unwrap().rows.len(), 1);
    assert_eq!(
        local.protection_orders.as_ref().unwrap().rows[0].raw_status,
        "confirmed",
        "raw protection history is preserved"
    );
    assert!(local.orders.as_ref().unwrap().rows.is_empty());
    if let Ok(path) = std::env::var("ACCOUNT_ORDER_PAPER_CLOSED_WIRE_VECTOR") {
        std::fs::write(path, r.encode_to_vec()).unwrap();
    }
    let unchanged: String = sqlx::query_scalar(
        "SELECT status FROM protection_orders WHERE protection_order_id='paper-sl'",
    )
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    assert_eq!(unchanged, "confirmed");
    // An ACK arriving after completion, including on a terminal engine order
    // omitted from local.orders, invalidates the claim on the next passive read.
    for (change, restore) in [
        ("UPDATE engine_orders SET exchange_order_id='late-ack' WHERE engine_order_id='paper-sl'", "UPDATE engine_orders SET exchange_order_id=NULL WHERE engine_order_id='paper-sl'"),
        ("UPDATE protection_orders SET exchange_algo_id='late-algo' WHERE protection_order_id='paper-sl'", "UPDATE protection_orders SET exchange_algo_id=NULL WHERE protection_order_id='paper-sl'"),
        ("UPDATE protection_orders SET exchange_order_id='late-child' WHERE protection_order_id='paper-sl'", "UPDATE protection_orders SET exchange_order_id=NULL WHERE protection_order_id='paper-sl'"),
        ("UPDATE protection_cleanup_obligations SET close_revision=0", "UPDATE protection_cleanup_obligations SET close_revision=1"),
        ("UPDATE protection_cleanup_obligations SET state='pending'", "UPDATE protection_cleanup_obligations SET state='complete'"),
        ("UPDATE protection_cleanup_obligations SET complete_reason='venue_terminal'", "UPDATE protection_cleanup_obligations SET complete_reason='paper_run_no_venue_protection'"),
        ("UPDATE protection_cleanup_obligations SET completed_at_ms=NULL", "UPDATE protection_cleanup_obligations SET completed_at_ms=1788000000223"),
        ("UPDATE protection_cleanup_obligations SET bot_uid='other-bot'", "UPDATE protection_cleanup_obligations SET bot_uid='bot-paper'"),
        ("UPDATE position_state SET state='protected',qty='1.25'", "UPDATE position_state SET state='closed',qty='0'"),
    ] {
        sqlx::query(change).execute(f.store.pool()).await.unwrap();
        let read = f.rpc(paper, 0, None).await.unwrap();
        assert_eq!(read.local.unwrap().positions.unwrap().rows[0].paper_cleanup_complete, Some(false), "{change}");
        sqlx::query(restore).execute(f.store.pool()).await.unwrap();
    }
    sqlx::query("DELETE FROM protection_cleanup_obligations")
        .execute(f.store.pool())
        .await
        .unwrap();
    assert_eq!(
        f.rpc(paper, 0, None)
            .await
            .unwrap()
            .local
            .unwrap()
            .positions
            .unwrap()
            .rows[0]
            .paper_cleanup_complete,
        Some(false)
    );
    f.cleanup().await;
}
