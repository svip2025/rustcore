//! B3-K1: one-sided fixtures enter through the production typed entry RPC.
use crate::{proto_v2 as p, store::Store, svc_v2::EngineExecutionV2Service};
use p::engine_execution_server::EngineExecution;
use sqlx::Row;
use tonic::Request;

pub(crate) struct Fixture {
    pub(crate) store: Store,
    service: EngineExecutionV2Service,
    path: std::path::PathBuf,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    pub(crate) async fn new() -> Self {
        sqlx::any::install_default_drivers();
        let path = std::env::temp_dir().join(format!("b3-close-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        Self::with_store(store, path).await
    }
    async fn with_store(store: Store, path: std::path::PathBuf) -> Self {
        let service = EngineExecutionV2Service::new(store.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let execution = crate::svc::EngineExecutionService::new(store.clone());
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            execution.clone(),
            crate::svc::EngineEventsService::new(
                store.clone(),
                std::time::Duration::from_millis(800),
            ),
            EngineExecutionV2Service::with_shared_execution(store.clone(), execution),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![
                "b3-kernel-test-token".into()
            ])),
        );
        let task = tokio::spawn(async move {
            router.serve_with_incoming(incoming).await.unwrap();
        });
        Self {
            store,
            service,
            path,
            address,
            task,
        }
    }
    pub(crate) async fn rpc<Q, R>(
        &self,
        path: &'static str,
        mut request: Request<Q>,
    ) -> Result<tonic::Response<R>, tonic::Status>
    where
        Q: prost::Message + Default + Send + Sync + 'static,
        R: prost::Message + Default + Send + Sync + 'static,
    {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        request.metadata_mut().insert(
            "authorization",
            "Bearer b3-kernel-test-token".parse().unwrap(),
        );
        client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::default(),
            )
            .await
    }
    pub(crate) async fn execute_entry(
        &self,
        credential: &str,
        label: &str,
        short: bool,
        symbol: &str,
        distance: i32,
        now: i64,
    ) -> p::IntentAck {
        self.rpc::<_, p::ExecuteIntentsResponse>(
            "/execution_engine.v2.EngineExecution/ExecuteIntents",
            Request::new(p::ExecuteIntentsRequest {
                credential_id: credential.into(),
                run_mode: "paper".into(),
                data_snapshot: Some(p::DataSnapshotDecision {
                    snapshot_id: format!("snap_{}", "a".repeat(32)),
                    quality_gate: "pass".into(),
                    allow_open: true,
                    allow_close: true,
                    created_at_ms: now,
                    schema_version: 1,
                }),
                intents: vec![p::OrderIntent {
                    bot_uid: format!("bot_{label}"),
                    run_id: format!("run_{label}"),
                    cycle_id: format!("cycle_{label}"),
                    idempotency_key: format!("b3_{label}"),
                    client_order_id_short: format!("b3co_{label}"),
                    exchange: "binance".into(),
                    market_kind: "futures".into(),
                    symbol_key: symbol.into(),
                    action: if short { "open_short" } else { "open_long" }.into(),
                    order_type: "limit".into(),
                    limit_price: "100".into(),
                    reduce_only: false,
                    intent_id: format!("int_{}", ulid::Ulid::new()),
                    max_slippage_bps: 15,
                    protection: Some(p::Protection {
                        required: false,
                        stop_loss_price: (if short {
                            100 + distance
                        } else {
                            100 - distance
                        })
                        .to_string(),
                        take_profit_price: (if short {
                            100 - 2 * distance
                        } else {
                            100 + 2 * distance
                        })
                        .to_string(),
                        confirm_timeout_ms: 3000,
                        schema_version: 1,
                    }),
                    policy_version: "risk_policy_v3".into(),
                    valid_until_ms: now + 30000,
                    config_hash: "c".repeat(64),
                    prompt_hash: format!("sha256:{}", "b".repeat(64)),
                    code_commit: "a".repeat(40),
                    schema_version: 1,
                }],
                schema_version: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .acks
        .remove(0)
    }
    pub(crate) async fn open(
        &self,
        credential: &str,
        label: &str,
        short: bool,
        symbol: &str,
    ) -> String {
        self.open_with_distance(credential, label, short, symbol, 5)
            .await
    }
    async fn open_with_distance(
        &self,
        credential: &str,
        label: &str,
        short: bool,
        symbol: &str,
        distance: i32,
    ) -> String {
        self.rpc::<_, p::FundSimulationAccountResponse>(
            "/execution_engine.v2.EngineExecution/FundSimulationAccount",
            Request::new(p::FundSimulationAccountRequest {
                credential_id: credential.into(),
                amount_usdt: "10000".into(),
                idempotency_identity: format!("b3fund:{credential}"),
                schema_version: 1,
            }),
        )
        .await
        .unwrap();
        let now = crate::svc::now_ms();
        self.rpc::<_, p::PutRiskBudgetResponse>(
            "/execution_engine.v2.EngineExecution/PutRiskBudget",
            Request::new(p::PutRiskBudgetRequest {
                credential_id: credential.into(),
                as_of_ms: now,
                equity_usdt: Some("10000".into()),
                available_usdt: Some("10000".into()),
                today_realized_pnl_usdt: Some("0".into()),
                today_realized_pnl_pct: Some(0.0),
                drawdown_pct: Some(0.0),
                total_exposure_pct: Some(0.0),
                open_positions_count: Some(0),
                group_exposure_json: Some(r#"{"majors":0,"l1_alts":0}"#.into()),
                kill_switch_level: Some("none".into()),
                remaining_credential_notional_usdt: Some("5000".into()),
                schema_version: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let ack = self
            .execute_entry(credential, label, short, symbol, distance, now)
            .await;
        assert_eq!(
            ack.status,
            p::Status::Accepted as i32,
            "entry: {}",
            ack.message
        );
        let mut tx = self.store.pool().begin().await.unwrap();
        crate::svc::emit_event(
            &mut tx,
            self.store.kind(),
            credential,
            "bar_closed",
            &crate::svc::EventContext {
                symbol_key: symbol.into(),
                exchange: "binance".into(),
                market_kind: "futures".into(),
                bot_uid: String::new(),
                run_id: String::new(),
                cycle_id: String::new(),
                idempotency_key: String::new(),
                client_order_id_short: String::new(),
            },
            "",
            None,
            Some(
                serde_json::json!({"close":"110","timeframe":"1m","closeTs":crate::svc::now_ms()}),
            ),
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        sqlx::query("SELECT position_id FROM engine_orders WHERE engine_order_id=?")
            .bind(ack.engine_order_id)
            .fetch_one(self.store.pool())
            .await
            .unwrap()
            .get("position_id")
    }
    pub(crate) async fn target(
        &self,
        credential: &str,
        position: &str,
    ) -> p::GetPositionCloseTargetResponse {
        self.rpc::<_, p::GetPositionCloseTargetResponse>(
            "/execution_engine.v2.EngineExecution/GetPositionCloseTarget",
            Request::new(p::GetPositionCloseTargetRequest {
                credential_id: credential.into(),
                position_id: position.into(),
                schema_version: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
    }
    pub(crate) async fn cleanup(self) {
        self.task.abort();
        let _ = self.task.await;
        self.store.pool().close().await;
        if !self.path.as_os_str().is_empty() {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
            }
        }
    }
}
pub(crate) const BTC: &str = "binance|futures|BTC|USDT|USDT";

#[tokio::test]
async fn b3_production_entry_exposes_proven_revision_bound_target() {
    let f = Fixture::new().await;
    let position = f.open("paper", "target", false, BTC).await;
    let target = f.target("paper", &position).await;
    assert!(
        target.available,
        "production entry must establish close provenance: {}",
        target.reason_code
    );
    let t = target.target.unwrap();
    assert_eq!(t.position_id, position);
    assert!(t.revision > 0);
    assert_eq!(t.execution_venue, "simulation");
    assert_eq!(t.run_mode, "paper");
    assert!(!t.entry_engine_order_id.is_empty());
    f.cleanup().await;
}

impl Fixture {
    pub(crate) async fn close_request(
        &self,
        credential: &str,
        position: &str,
    ) -> p::ClosePositionRequest {
        let target = self.target(credential, position).await;
        assert!(target.available, "{}", target.reason_code);
        p::ClosePositionRequest {
            credential_id: credential.into(),
            position_id: position.into(),
            expected_revision: target.target.unwrap().revision,
            expected_boot_id: target.engine_boot_id,
            request_ref: ulid::Ulid::new().to_string(),
            actor_member_id: 9,
            schema_version: 1,
        }
    }
    pub(crate) async fn close(&self, request: p::ClosePositionRequest) -> p::ClosePositionResponse {
        self.rpc::<_, p::ClosePositionResponse>(
            "/execution_engine.v2.EngineExecution/ClosePosition",
            Request::new(request),
        )
        .await
        .unwrap()
        .into_inner()
    }
    async fn receipt(&self, request: &p::ClosePositionRequest) -> p::ClosePositionResponse {
        self.rpc::<_, p::ClosePositionResponse>(
            "/execution_engine.v2.EngineExecution/GetPositionCloseReceipt",
            Request::new(p::GetPositionCloseReceiptRequest {
                credential_id: request.credential_id.clone(),
                request_ref: request.request_ref.clone(),
                actor_member_id: request.actor_member_id,
                schema_version: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
    }
    async fn ordinary_orders(&self) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM engine_orders WHERE order_role IN ('entry','exit')")
            .fetch_one(self.store.pool())
            .await
            .unwrap()
            .get("n")
    }
    async fn position(&self, id: &str) -> (String, String, i64) {
        let row =
            sqlx::query("SELECT state,qty,close_revision FROM position_state WHERE position_id=?")
                .bind(id)
                .fetch_one(self.store.pool())
                .await
                .unwrap();
        (row.get("state"), row.get("qty"), row.get("close_revision"))
    }
    async fn prepared(
        &self,
        request: &p::ClosePositionRequest,
    ) -> crate::position_close::ExactCloseContext {
        match crate::position_close::prepare(&self.store, request)
            .await
            .unwrap()
        {
            crate::position_close::Prepare::Dispatch(context) => context,
            crate::position_close::Prepare::Receipt(r) => {
                panic!("expected prepared, got {}", r.reason_code)
            }
        }
    }
    async fn entry_under_fence(&self, credential: &str, symbol: &str) -> p::IntentAck {
        self.rpc::<_, p::ExecuteIntentsResponse>(
            "/execution_engine.v2.EngineExecution/ExecuteIntents",
            Request::new(p::ExecuteIntentsRequest {
                credential_id: credential.into(),
                run_mode: "paper".into(),
                intents: vec![p::OrderIntent {
                    bot_uid: "bot_fenced_new".into(),
                    symbol_key: symbol.into(),
                    action: "open_long".into(),
                    idempotency_key: format!("probe_{}", ulid::Ulid::new()),
                    intent_id: format!("int_{}", ulid::Ulid::new()),
                    exchange: "binance".into(),
                    market_kind: "futures".into(),
                    order_type: "limit".into(),
                    limit_price: "100".into(),
                    schema_version: 1,
                    ..Default::default()
                }],
                schema_version: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .acks
        .remove(0)
    }
}

#[tokio::test]
async fn b3_wire_close_reporting_uses_only_the_original_entry_run() {
    let f = Fixture::new().await;
    let position = f.open("paper", "reporting_original", false, BTC).await;
    let request = f.close_request("paper", &position).await;
    // An already prepared request retains its persisted authority/target bytes.
    // Re-reading for dispatch resolves reporting from that same entry only.
    let first = f.prepared(&request).await;
    let retried = f.prepared(&request).await;
    let original = first.execution_request().into_inner();
    let replay = retried.execution_request().into_inner();
    assert_eq!(original.intents, replay.intents);
    assert_eq!(original.intents[0].run_id, "run_reporting_original");
    assert_eq!(
        original.intents[0].cycle_id,
        format!("cyc_{}", request.request_ref)
    );
    assert!(original.intents[0].idempotency_key.starts_with("b3_"));
    let receipt = f.close(request.clone()).await;
    assert_eq!(receipt.reason_code, "PAPER_FILLED");
    let again = f.close(request).await;
    assert_eq!(receipt.engine_order_id, again.engine_order_id);
    let order = sqlx::query(
        "SELECT run_id,cycle_id,idempotency_key FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(receipt.engine_order_id.as_ref().unwrap())
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    assert_eq!(order.get::<String, _>("run_id"), "run_reporting_original");
    assert!(order.get::<String, _>("cycle_id").starts_with("cyc_"));
    assert!(order
        .get::<String, _>("idempotency_key")
        .starts_with("idem_"));
    assert_eq!(f.position(&position).await.1, "0");
    f.cleanup().await;
}

#[tokio::test]
async fn b3_wire_close_without_historical_entry_run_remains_safe_and_unattributed() {
    let f = Fixture::new().await;
    let position = f.open("paper", "reporting_missing", false, BTC).await;
    // A historical entry can have missing reporting metadata. It still has
    // the independently verified execution provenance required to close.
    sqlx::query("UPDATE engine_orders SET run_id=NULL WHERE credential_id='paper' AND position_id=? AND order_role='entry'")
        .bind(&position).execute(f.store.pool()).await.unwrap();
    let request = f.close_request("paper", &position).await;
    let receipt = f.close(request).await;
    assert_eq!(receipt.reason_code, "PAPER_FILLED");
    assert_eq!(f.position(&position).await.1, "0");
    let order = sqlx::query("SELECT COALESCE(run_id,'') AS run_id,client_order_id_short FROM engine_orders WHERE engine_order_id=?")
        .bind(receipt.engine_order_id.unwrap()).fetch_one(f.store.pool()).await.unwrap();
    assert_eq!(order.get::<String, _>("run_id"), "");
    assert!(order
        .get::<String, _>("client_order_id_short")
        .starts_with("b3"));
    f.cleanup().await;
}

#[tokio::test]
async fn b3_wire_exact_close_is_idempotent_and_other_positions_are_unchanged() {
    let f = Fixture::new().await;
    exact_close_scenario(&f).await;
    f.cleanup().await;
}
async fn exact_close_scenario(f: &Fixture) -> serde_json::Value {
    let a = f.open("paper", "exact_a", false, BTC).await;
    let b = f
        .open(
            "paper:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "exact_b",
            true,
            BTC,
        )
        .await;
    let other = f
        .open(
            "paper",
            "exact_other",
            false,
            "binance|futures|ETH|USDT|USDT",
        )
        .await;
    let before_b = f.position(&b).await;
    let before_other = f.position(&other).await;
    let before = f.position(&a).await;
    let request = f.close_request("paper", &a).await;
    let target = f.target("paper", &a).await;
    let receipt = f.close(request.clone()).await;
    assert!(receipt.found);
    assert_eq!(
        receipt.request_status, "accepted",
        "{}",
        receipt.reason_code
    );
    assert_eq!(receipt.position_id, a);
    assert_eq!(
        receipt
            .execution_outcome
            .as_ref()
            .unwrap()
            .quantity
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        receipt.execution_outcome.as_ref().unwrap().source,
        "paper_transaction"
    );
    assert_eq!(
        receipt.protection_observation.as_ref().unwrap().status,
        "unknown"
    );
    assert_eq!(
        receipt.protection_observation.as_ref().unwrap().reason_code,
        "PROTECTION_CLEANUP_NOT_PROVEN"
    );
    assert_eq!(f.position(&b).await, before_b);
    assert_eq!(f.position(&other).await, before_other);
    assert_eq!(f.ordinary_orders().await, 4);
    let order = sqlx::query(
        "SELECT quantity,action,reduce_only,position_id FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(receipt.engine_order_id.as_ref().unwrap())
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    assert_eq!(order.get::<String, _>("quantity"), before.1);
    assert_eq!(order.get::<String, _>("action"), "close_long");
    assert_eq!(order.get::<i64, _>("reduce_only"), 1);
    assert_eq!(order.get::<String, _>("position_id"), a);
    let fill = sqlx::query("SELECT fill_price FROM engine_fills WHERE engine_order_id=?")
        .bind(receipt.engine_order_id.as_ref().unwrap())
        .fetch_one(f.store.pool())
        .await
        .unwrap();
    assert_eq!(fill.get::<String, _>("fill_price"), "110");
    let again = f.close(request.clone()).await;
    let read = f.receipt(&request).await;
    assert_eq!(again.engine_order_id, receipt.engine_order_id);
    assert_eq!(read.execution_outcome, receipt.execution_outcome);
    assert_eq!(f.ordinary_orders().await, 4);
    serde_json::json!({"schema":"b3-position-close-kernel-vectors.1","source":"authenticated production gRPC: FundSimulationAccount/PutRiskBudget/ExecuteIntents/GetPositionCloseTarget/ClosePosition/GetPositionCloseReceipt",
        "sources":vector_sources(),"target":target_json(&target),"request":request_json(&request),"receipt":receipt_json(&receipt),"replay":receipt_json(&again)})
}

#[tokio::test]
async fn b3_foreign_actor_and_changed_request_never_reveal_original_target() {
    let f = Fixture::new().await;
    let a = f.open("paper", "identity", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    let accepted = f.close(request.clone()).await;
    for change in 0..3 {
        let mut conflicting = request.clone();
        match change {
            0 => conflicting.actor_member_id = 10,
            1 => conflicting.position_id = "pos_foreign".into(),
            _ => conflicting.expected_revision += 1,
        };
        let receipt = f.close(conflicting).await;
        assert_eq!(receipt.reason_code, "REQUEST_IDENTITY_CONFLICT");
        assert!(!receipt.found);
        assert!(receipt.position_id.is_empty());
        assert!(receipt.engine_order_id.is_none());
    }
    let mut foreign = request.clone();
    foreign.actor_member_id = 10;
    let receipt = f.receipt(&foreign).await;
    assert_eq!(receipt.reason_code, "REQUEST_NOT_FOUND");
    assert!(receipt.position_id.is_empty());
    let foreign_target = f.target("paper:ffffffffffffffffffffffffffffffff", &a).await;
    let absent = f
        .target("paper:ffffffffffffffffffffffffffffffff", "pos_missing")
        .await;
    assert_eq!(foreign_target.reason_code, absent.reason_code);
    assert!(foreign_target.target.is_none());
    assert_eq!(f.ordinary_orders().await, 2);
    assert_eq!(accepted.request_status, "accepted");
    f.cleanup().await;
}

#[tokio::test]
async fn b3_actor_safeint_and_revision_reject_invalid_wire_without_effect() {
    let f = Fixture::new().await;
    let a = f.open("paper", "validation", false, BTC).await;
    let req = f.close_request("paper", &a).await;
    for (actor, revision, reference) in [
        (0, req.expected_revision, req.request_ref.clone()),
        (
            9_007_199_254_740_992,
            req.expected_revision,
            req.request_ref.clone(),
        ),
        (9, 0, req.request_ref.clone()),
        (9, u64::MAX, req.request_ref.clone()),
        (9, req.expected_revision, "not-a-ulid".into()),
    ] {
        let mut invalid = req.clone();
        invalid.actor_member_id = actor;
        invalid.expected_revision = revision;
        invalid.request_ref = reference;
        let error = f
            .rpc::<_, p::ClosePositionResponse>(
                "/execution_engine.v2.EngineExecution/ClosePosition",
                Request::new(invalid),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    assert_eq!(f.ordinary_orders().await, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_stale_quantity_direction_state_and_boot_do_not_execute() {
    let f = Fixture::new().await;
    let a = f.open("paper", "stale", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    let mut boot = request.clone();
    boot.expected_boot_id = ulid::Ulid::new().to_string();
    assert_eq!(f.close(boot).await.reason_code, "ENGINE_BOOT_MISMATCH");
    sqlx::query(
        "UPDATE position_state SET close_revision=close_revision+1,qty='7' WHERE position_id=?",
    )
    .bind(&a)
    .execute(f.store.pool())
    .await
    .unwrap();
    assert_eq!(
        f.close(request.clone()).await.reason_code,
        "POSITION_REVISION_MISMATCH"
    );
    // Corruption/mutation without a revision bump must still fail the frozen
    // exact target comparison inside T2, rather than retargeting from bot+symbol.
    let current = f.close_request("paper", &a).await;
    f.prepared(&current).await;
    sqlx::query("UPDATE position_state SET direction='short' WHERE position_id=?")
        .bind(&a)
        .execute(f.store.pool())
        .await
        .unwrap();
    let stale = f.close(current.clone()).await;
    assert_eq!(stale.request_status, "rejected");
    assert_eq!(stale.reason_code, "POSITION_REVISION_MISMATCH");
    assert_eq!(f.ordinary_orders().await, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_pending_fence_survives_restart_unknown_and_does_not_block_reductions() {
    let f = Fixture::new().await;
    let a = f.open("paper", "fence", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    f.prepared(&request).await;
    let entry = f.entry_under_fence("paper", BTC).await;
    assert_eq!(entry.reject_reason_code, "POSITION_CLOSE_PENDING");
    assert_eq!(f.ordinary_orders().await, 1);
    // Distinct symbol and direction remain usable through ordinary production entry.
    let other = f
        .open(
            "paper",
            "fence_other",
            false,
            "binance|futures|ETH|USDT|USDT",
        )
        .await;
    let other_request = f.close_request("paper", &other).await;
    assert_eq!(f.close(other_request).await.request_status, "accepted");
    assert_eq!(
        f.entry_under_fence("paper", BTC).await.reject_reason_code,
        "POSITION_CLOSE_PENDING"
    );
    // The opposite direction reaches its ordinary risk decision; the existing
    // per-symbol exposure limit may still reject it, so do not bypass that cap.
    let opposite = f
        .rpc::<_, p::ExecuteIntentsResponse>(
            "/execution_engine.v2.EngineExecution/ExecuteIntents",
            Request::new(p::ExecuteIntentsRequest {
                credential_id: "paper".into(),
                run_mode: "paper".into(),
                schema_version: 1,
                intents: vec![p::OrderIntent {
                    bot_uid: "opposite".into(),
                    idempotency_key: "opposite".into(),
                    intent_id: "int_opposite".into(),
                    symbol_key: BTC.into(),
                    action: "open_short".into(),
                    exchange: "binance".into(),
                    market_kind: "futures".into(),
                    order_type: "limit".into(),
                    limit_price: "100".into(),
                    schema_version: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        opposite.acks[0].reject_reason_code,
        "POSITION_CLOSE_PENDING"
    );
    // Existing member/emergency close paths remain callable while B3 is pending.
    let outcome = f
        .service
        .execute_intents(Request::new(p::ExecuteIntentsRequest {
            credential_id: "paper".into(),
            run_mode: "paper".into(),
            intents: vec![p::OrderIntent {
                bot_uid: "bot_fence".into(),
                symbol_key: BTC.into(),
                action: "close_long".into(),
                reduce_only: true,
                order_type: "market".into(),
                idempotency_key: "existing_reduce".into(),
                intent_id: "int_existing_reduce".into(),
                client_order_id_short: "existing_reduce".into(),
                exchange: "binance".into(),
                market_kind: "futures".into(),
                schema_version: 1,
                ..Default::default()
            }],
            schema_version: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(outcome.acks[0].status, p::Status::Accepted as i32);
    let stale = f.close(request.clone()).await;
    assert_eq!(stale.request_status, "rejected");
    assert_eq!(f.position(&a).await.1, "0");
    // Exact no-execution rejection releases the scope, enabling a new entry.
    f.open("paper", "fence_released", false, BTC).await;
    f.cleanup().await;
}

#[tokio::test]
async fn b3_prepared_old_boot_is_read_only_pending_and_stays_fenced() {
    let f = Fixture::new().await;
    let a = f.open("paper", "old_boot", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    f.prepared(&request).await;
    sqlx::query("UPDATE position_close_requests SET expected_boot_id=? WHERE credential_id='paper' AND request_ref=?")
        .bind(ulid::Ulid::new().to_string()).bind(&request.request_ref).execute(f.store.pool()).await.unwrap();
    let read = f.receipt(&request).await;
    assert_eq!(read.request_status, "pending_unknown");
    let replay = f.close(request.clone()).await;
    assert_eq!(replay.request_status, "pending_unknown");
    assert_eq!(
        f.entry_under_fence("paper", BTC).await.reject_reason_code,
        "POSITION_CLOSE_PENDING"
    );
    assert_eq!(f.ordinary_orders().await, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_same_and_distinct_reference_concurrency_fills_once() {
    let f = Fixture::new().await;
    let a = f.open("paper", "concurrent", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    let (first, second) = tokio::join!(f.close(request.clone()), f.close(request.clone()));
    assert_eq!(first.engine_order_id, second.engine_order_id);
    assert_eq!(f.ordinary_orders().await, 2);
    let b = f.open("paper", "concurrent_2", false, BTC).await;
    let req = f.close_request("paper", &b).await;
    let mut other = req.clone();
    other.request_ref = ulid::Ulid::new().to_string();
    let (first, second) = tokio::join!(f.close(req), f.close(other));
    assert_eq!(
        [first.request_status, second.request_status]
            .iter()
            .filter(|p| p.as_str() == "accepted")
            .count(),
        1
    );
    assert_eq!(f.ordinary_orders().await, 4);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_missing_stale_future_quote_rejects_without_fallback_zero_and_releases_fence() {
    let f = Fixture::new().await;
    let a = f.open("paper", "quote", false, BTC).await;
    for close_ts in [
        None,
        Some(crate::svc::now_ms() - 60_001),
        Some(crate::svc::now_ms() + 60_000),
    ] {
        sqlx::query("DELETE FROM engine_order_events WHERE event_type='bar_closed'")
            .execute(f.store.pool())
            .await
            .unwrap();
        if let Some(ts) = close_ts {
            let mut tx = f.store.pool().begin().await.unwrap();
            crate::svc::emit_event(
                &mut tx,
                f.store.kind(),
                "paper",
                "bar_closed",
                &crate::svc::EventContext {
                    bot_uid: String::new(),
                    run_id: String::new(),
                    cycle_id: String::new(),
                    idempotency_key: String::new(),
                    client_order_id_short: String::new(),
                    exchange: "binance".into(),
                    market_kind: "futures".into(),
                    symbol_key: BTC.into(),
                },
                "",
                None,
                Some(serde_json::json!({"timeframe":"1m","closeTs":ts,"close":"110"})),
                None,
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
        let request = f.close_request("paper", &a).await;
        let receipt = f.close(request.clone()).await;
        assert_eq!(receipt.request_status, "rejected");
        assert_eq!(receipt.reason_code, "PAPER_MARKET_PRICE_UNAVAILABLE");
        assert_eq!(f.ordinary_orders().await, 1);
        assert_ne!(f.position(&a).await.1, "0");
    }
    f.cleanup().await;
}

#[tokio::test]
async fn b3_unknown_legacy_and_exchange_positions_are_unavailable_without_backfill() {
    let f = Fixture::new().await;
    let a = f.open("paper", "provenance", false, BTC).await;
    sqlx::query("DELETE FROM position_execution_provenance WHERE position_id=?")
        .bind(&a)
        .execute(f.store.pool())
        .await
        .unwrap();
    assert_eq!(
        f.target("paper", &a).await.reason_code,
        "POSITION_PROVENANCE_UNKNOWN"
    );
    sqlx::query("UPDATE position_state SET credential_id='7' WHERE position_id=?")
        .bind(&a)
        .execute(f.store.pool())
        .await
        .unwrap();
    // WO-M1：交易所凭据的精确平仓已实现，但没有生产写下的执行来源仍然
    // 不可平——不补造、不回填。
    let exchange = f.target("7", &a).await;
    assert_eq!(exchange.reason_code, "POSITION_PROVENANCE_UNKNOWN");
    assert!(exchange.target.is_none());
    assert_eq!(f.ordinary_orders().await, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_old_accepted_outcome_never_masks_reopened_current_position() {
    let f = Fixture::new().await;
    let a = f.open("paper", "reopen", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    let first = f.close(request.clone()).await;
    sqlx::query("UPDATE position_state SET state='open',qty='7',close_revision=close_revision+1 WHERE position_id=?").bind(&a).execute(f.store.pool()).await.unwrap();
    let receipt = f.receipt(&request).await;
    assert_eq!(receipt.execution_outcome, first.execution_outcome);
    let current = receipt.position_observation.as_ref().unwrap();
    assert_eq!(current.quantity.as_deref(), Some("7"));
    assert!(current.revision > receipt.execution_outcome.as_ref().unwrap().revision);
    assert_eq!(
        f.close(request).await.engine_order_id,
        first.engine_order_id
    );
    assert_eq!(f.ordinary_orders().await, 2);
    f.cleanup().await;
}

fn target_json(response: &p::GetPositionCloseTargetResponse) -> serde_json::Value {
    assert_eq!(response.schema_version, 1);
    if let Some(target) = response.target.as_ref() {
        assert_eq!(target.schema_version, 1);
    }
    serde_json::json!({"available":response.available,"reason_code":response.reason_code,"observed_at_ms":response.observed_at_ms,
    "engine_boot_id":response.engine_boot_id,"schema_version":response.schema_version,"target":response.target.as_ref().map(|t|serde_json::json!({
        "position_id":t.position_id,"revision":t.revision.to_string(),"bot_uid":t.bot_uid,"symbol_key":t.symbol_key,"direction":t.direction,"quantity":t.quantity,
        "entry_epoch":t.entry_epoch.to_string(),"exit_epoch":t.exit_epoch.to_string(),"run_mode":t.run_mode,"execution_venue":t.execution_venue,"entry_engine_order_id":t.entry_engine_order_id,"schema_version":t.schema_version
    }))})
}
fn request_json(r: &p::ClosePositionRequest) -> serde_json::Value {
    serde_json::json!({"credential_id":r.credential_id,"position_id":r.position_id,
    "expected_revision":r.expected_revision.to_string(),"expected_boot_id":r.expected_boot_id,"request_ref":r.request_ref,"actor_member_id":r.actor_member_id.to_string(),"schema_version":r.schema_version})
}
fn observation_json(o: &Option<p::PositionCloseObservation>) -> serde_json::Value {
    if let Some(observation) = o.as_ref() {
        assert_eq!(observation.schema_version, 1);
    }
    o.as_ref().map_or(serde_json::Value::Null,|o|serde_json::json!({
    "status":o.status,"position_id":o.position_id,"revision":o.revision.to_string(),"quantity":o.quantity,"state":o.state,"observed_at_ms":o.observed_at_ms,"source":o.source,"schema_version":o.schema_version}))
}
fn receipt_json(r: &p::ClosePositionResponse) -> serde_json::Value {
    assert_eq!(r.schema_version, 1);
    assert_eq!(r.protection_observation.as_ref().unwrap().schema_version, 1);
    serde_json::json!({"found":r.found,"request_ref":r.request_ref,"position_id":r.position_id,
    "target_revision":r.target_revision.to_string(),"request_status":r.request_status,"reason_code":r.reason_code,"engine_order_id":r.engine_order_id,"risk_verdict_id":r.risk_verdict_id,
    "execution_outcome":observation_json(&r.execution_outcome),"position_observation":observation_json(&r.position_observation),"protection_observation":r.protection_observation.as_ref().map(|p|serde_json::json!({"status":p.status,"reason_code":p.reason_code,"schema_version":p.schema_version})),"schema_version":r.schema_version})
}

#[tokio::test]
async fn b3_sqlite_one_sided_contract_vector() {
    let f = Fixture::new().await;
    let mut vector = exact_close_scenario(&f).await;
    let mut conflict: p::ClosePositionRequest = p::ClosePositionRequest {
        credential_id: "paper".into(),
        position_id: vector["request"]["position_id"].as_str().unwrap().into(),
        expected_revision: vector["request"]["expected_revision"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        expected_boot_id: vector["request"]["expected_boot_id"]
            .as_str()
            .unwrap()
            .into(),
        request_ref: vector["request"]["request_ref"].as_str().unwrap().into(),
        actor_member_id: 10,
        schema_version: 1,
    };
    let receipt = f.close(conflict.clone()).await;
    let not_found = f.receipt(&conflict).await;
    conflict.actor_member_id = 9;
    let closed = f.target("paper", &conflict.position_id).await;
    vector["cases"] = serde_json::json!({"requestIdentityConflict":receipt_json(&receipt),"foreignActorReceipt":receipt_json(&not_found),"closedTarget":target_json(&closed)});

    // A controlled current-row reopen is an observation fixture, not proof of
    // a new production Entry. The response itself is captured from the real RPC.
    sqlx::query("UPDATE position_state SET state='open',qty='7',close_revision=close_revision+1 WHERE position_id=?")
        .bind(&conflict.position_id).execute(f.store.pool()).await.unwrap();
    let reopened = f.receipt(&conflict).await;
    assert_eq!(reopened.request_status, "accepted");
    assert_eq!(
        reopened
            .execution_outcome
            .as_ref()
            .unwrap()
            .quantity
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        reopened
            .position_observation
            .as_ref()
            .unwrap()
            .quantity
            .as_deref(),
        Some("7")
    );
    assert!(
        reopened.position_observation.as_ref().unwrap().revision
            > reopened.execution_outcome.as_ref().unwrap().revision
    );
    vector["cases"]["reopenedCurrentReceipt"] = receipt_json(&reopened);

    let pending_fixture = Fixture::new().await;
    let pending_position = pending_fixture
        .open("paper", "vector_pending", false, BTC)
        .await;
    let pending_target = pending_fixture.target("paper", &pending_position).await;
    let pending_request = pending_fixture
        .close_request("paper", &pending_position)
        .await;
    // Pin the real committed T1 helper without dispatch, then observe via RPC.
    pending_fixture.prepared(&pending_request).await;
    let prepared = pending_fixture.receipt(&pending_request).await;
    assert_eq!(prepared.request_status, "prepared");
    vector["cases"]["preparedTarget"] = target_json(&pending_target);
    vector["cases"]["preparedRequest"] = request_json(&pending_request);
    vector["cases"]["preparedReceipt"] = receipt_json(&prepared);
    let old_boot = pending_fixture.store.boot_id().to_owned();
    let database = pending_fixture.path.clone();
    pending_fixture.task.abort();
    pending_fixture.store.pool().close().await;
    drop(pending_fixture);
    // Store boot identity is process-wide. Reopening a Store in this process
    // cannot model a new boot; use a real child to capture the pure receipt RPC.
    let child_output = database.with_extension("pending-receipt.json");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "position_close_tests::b3_pending_vector_child",
            "--ignored",
            "--nocapture",
        ])
        .env("B3_VECTOR_DATABASE", &database)
        .env("B3_VECTOR_REFERENCE", &pending_request.request_ref)
        .env("B3_VECTOR_OUTPUT", &child_output)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let child_pid = child.id();
    let start = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > std::time::Duration::from_secs(30) {
            let _ = child.kill();
            let status = child.wait().unwrap();
            panic!("pending receipt child timed out: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert!(status.success(), "pending receipt child failed: {status}");
    let child_receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&child_output).unwrap()).unwrap();
    std::fs::remove_file(child_output).unwrap();
    assert_ne!(child_receipt["bootId"].as_str().unwrap(), old_boot);
    assert_eq!(
        child_receipt["receipt"]["request_status"],
        "pending_unknown"
    );
    assert_eq!(
        child_receipt["receipt"]["reason_code"],
        "PREPARED_PREVIOUS_BOOT"
    );
    eprintln!(
        "B3 vector restart evidence child={child_pid} boot_changed=true receipt=pending_unknown"
    );
    let restored = Fixture::with_store(
        Store::connect(&format!("sqlite://{}", database.display()))
            .await
            .unwrap(),
        database,
    )
    .await;
    assert_eq!(restored.ordinary_orders().await, 1);
    assert_eq!(
        restored
            .entry_under_fence("paper", BTC)
            .await
            .reject_reason_code,
        "POSITION_CLOSE_PENDING"
    );
    vector["cases"]["pendingUnknownReceipt"] = child_receipt["receipt"].clone();
    vector["caseSources"] = serde_json::json!({
        "reopenedCurrentReceipt": "production accepted close; controlled same-ID current-row reopen; authenticated GetPositionCloseReceipt; not a new-entry/reconcile wiring proof",
        "preparedReceipt": "production typed Entry; production prepare helper pins committed T1 without dispatch; authenticated GetPositionCloseReceipt",
        "pendingUnknownReceipt": {"source":"real child process opens the same persisted database through Store::connect; new process boot and authenticated gRPC service; pure GetPositionCloseReceipt; no fabricated response or expected_boot rewrite", "oldBootId":old_boot,"newBootId":child_receipt["bootId"]}
    });
    restored.cleanup().await;
    if let Ok(path) = std::env::var("B3_CLOSE_VECTOR_PATH") {
        std::fs::write(path, serde_json::to_vec_pretty(&vector).unwrap()).unwrap();
    }
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "subprocess only, invoked by the one-sided SQLite vector producer"]
async fn b3_pending_vector_child() {
    sqlx::any::install_default_drivers();
    let database = std::env::var("B3_VECTOR_DATABASE").unwrap();
    let reference = std::env::var("B3_VECTOR_REFERENCE").unwrap();
    let output = std::env::var("B3_VECTOR_OUTPUT").unwrap();
    let f = Fixture::with_store(
        Store::connect(&format!("sqlite://{database}"))
            .await
            .unwrap(),
        std::path::PathBuf::new(),
    )
    .await;
    let receipt = f
        .receipt(&p::ClosePositionRequest {
            credential_id: "paper".into(),
            request_ref: reference,
            actor_member_id: 9,
            ..Default::default()
        })
        .await;
    assert_eq!(receipt.request_status, "pending_unknown");
    let captured = serde_json::json!({"bootId":f.store.boot_id(),"receipt":receipt_json(&receipt)});
    std::fs::write(output, serde_json::to_vec_pretty(&captured).unwrap()).unwrap();
    f.cleanup().await;
}
// A failed real close keeps T1 prepared. Repair only the controlled corrupt
// sizing fixture, then replay the same RPC identity through durable target TEXT.
async fn prepared_retry_text_scenario(f: &Fixture) {
    let position = f.open("paper", "prepared_text", false, BTC).await;
    let request = f.close_request("paper", &position).await;
    let row = sqlx::query("SELECT sizing_json FROM risk_verdicts WHERE risk_verdict_id=(SELECT risk_verdict_id FROM engine_orders WHERE position_id=? AND order_role='entry')")
        .bind(&position).fetch_one(f.store.pool()).await.unwrap();
    let original = crate::store::try_get_text(&row, "sizing_json")
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE risk_verdicts SET sizing_json='{bad' WHERE risk_verdict_id=(SELECT risk_verdict_id FROM engine_orders WHERE position_id=? AND order_role='entry')")
        .bind(&position).execute(f.store.pool()).await.unwrap();
    let error = f
        .rpc::<_, p::ClosePositionResponse>(
            "/execution_engine.v2.EngineExecution/ClosePosition",
            Request::new(request.clone()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM position_close_requests WHERE request_ref=?")
            .bind(&request.request_ref)
            .fetch_one(f.store.pool())
            .await
            .unwrap();
    assert_eq!(phase, "prepared");
    assert_eq!(f.ordinary_orders().await, 1);
    sqlx::query("UPDATE risk_verdicts SET sizing_json=? WHERE risk_verdict_id=(SELECT risk_verdict_id FROM engine_orders WHERE position_id=? AND order_role='entry')")
        .bind(original).bind(&position).execute(f.store.pool()).await.unwrap();
    let accepted = f.close(request.clone()).await;
    assert_eq!(accepted.request_status, "accepted");
    assert_eq!(
        f.receipt(&request).await.execution_outcome,
        accepted.execution_outcome
    );
    let replay = f.close(request).await;
    assert_eq!(replay.engine_order_id, accepted.engine_order_id);
    assert_eq!(replay.execution_outcome, accepted.execution_outcome);
    assert_eq!(f.position(&position).await.0, "closed");
    assert_eq!(f.ordinary_orders().await, 2);
}

#[tokio::test]
async fn b3_sqlite_prepared_retry_reads_durable_target() {
    let f = Fixture::new().await;
    prepared_retry_text_scenario(&f).await;
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires owned MySQL RUSTCORE_TEST_MYSQL_URL; absence is not PASS"]
async fn b3_mysql_prepared_retry_reads_durable_target() {
    let scratch = crate::store::mysql_test_support::MySqlScratch::create(
        "b3close",
        "b3_mysql_prepared_retry_reads_durable_target",
    )
    .await
    .expect("owned MySQL required");
    let f = Fixture::with_store(scratch.connect_store().await, std::path::PathBuf::new()).await;
    prepared_retry_text_scenario(&f).await;
    f.cleanup().await;
    scratch.cleanup().await;
}

// A corrupt durable sizing row must reject the real close RPC without releasing
// notional or committing a close. Exercise the existing nullable TEXT shape.
async fn corrupt_close_sizing_scenario(f: &Fixture, invalid: Option<&str>) {
    let position = f.open("paper", "sizing_corrupt", false, BTC).await;
    let request = f.close_request("paper", &position).await;
    let before = f.position(&position).await;
    let orders = f.ordinary_orders().await;
    let reserved: String = sqlx::query_scalar(
        "SELECT reserved_usdt FROM notional_reservations WHERE credential_id='paper'",
    )
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE risk_verdicts SET sizing_json=? WHERE risk_verdict_id=(SELECT risk_verdict_id FROM engine_orders WHERE position_id=? AND order_role='entry')")
        .bind(invalid).bind(&position).execute(f.store.pool()).await.unwrap();
    let error = f
        .rpc::<_, p::ClosePositionResponse>(
            "/execution_engine.v2.EngineExecution/ClosePosition",
            Request::new(request.clone()),
        )
        .await
        .unwrap_err();
    if invalid.is_some() {
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error
            .message()
            .contains("decode durable entry sizing failed"));
    } else {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error
            .message()
            .contains("paper close has no durable entry sizing"));
    }
    assert_eq!(f.position(&position).await, before);
    assert_eq!(f.ordinary_orders().await, orders);
    let after: String = sqlx::query_scalar(
        "SELECT reserved_usdt FROM notional_reservations WHERE credential_id='paper'",
    )
    .fetch_one(f.store.pool())
    .await
    .unwrap();
    assert_eq!(after, reserved);
    assert_eq!(f.receipt(&request).await.request_status, "prepared");
}

#[tokio::test]
async fn b3_sqlite_corrupt_sizing_never_closes_or_releases_notional() {
    for invalid in [Some("{corrupt sizing JSON"), None] {
        let f = Fixture::new().await;
        corrupt_close_sizing_scenario(&f, invalid).await;
        f.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "requires owned MySQL RUSTCORE_TEST_MYSQL_URL; absence is not PASS"]
async fn b3_mysql_corrupt_sizing_never_closes_or_releases_notional() {
    for invalid in [Some("{corrupt sizing JSON"), None] {
        let scratch = crate::store::mysql_test_support::MySqlScratch::create(
            "b3close",
            "b3_mysql_corrupt_sizing_never_closes_or_releases_notional",
        )
        .await
        .expect("owned MySQL required");
        let f = Fixture::with_store(scratch.connect_store().await, std::path::PathBuf::new()).await;
        corrupt_close_sizing_scenario(&f, invalid).await;
        f.cleanup().await;
        scratch.cleanup().await;
    }
}

// The production budget publisher writes nonempty TEXT. Keep both its usable
// representation and a deliberately corrupt persisted value on the real RPC path.
async fn budget_group_text_scenario(f: &Fixture) {
    let position = f.open("paper", "group_text_valid", false, BTC).await;
    let account = f
        .rpc::<_, p::GetAccountStateResponse>(
            "/execution_engine.v2.EngineExecution/GetAccountState",
            Request::new(p::GetAccountStateRequest {
                credential_id: "paper".into(),
                bot_uid: String::new(),
                schema_version: 1,
            }),
        )
        .await
        .expect("GetAccountState must read existing durable entry sizing")
        .into_inner();
    assert!(account.available, "{}", account.unavailable_reason);
    assert_eq!(account.open_positions_count, Some(1));
    assert_eq!(account.positions.len(), 1);
    assert_eq!(account.positions[0].symbol_key, BTC);
    assert_eq!(account.positions[0].qty, f.position(&position).await.1);
    let sizing_row = sqlx::query("SELECT sizing_json FROM risk_verdicts WHERE risk_verdict_id=(SELECT risk_verdict_id FROM engine_orders WHERE position_id=? AND order_role='entry')")
        .bind(&position).fetch_one(f.store.pool()).await.unwrap();
    let sizing: serde_json::Value = serde_json::from_str(
        &crate::store::try_get_text(&sizing_row, "sizing_json")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        u64::from(account.positions[0].leverage),
        sizing["leverage"].as_u64().unwrap()
    );
    assert!(account.positions[0].leverage >= 1);
    let row =
        sqlx::query("SELECT group_exposure_json FROM risk_budget WHERE credential_id='paper'")
            .fetch_one(f.store.pool())
            .await
            .unwrap();
    assert_eq!(
        crate::store::try_get_text(&row, "group_exposure_json")
            .unwrap()
            .as_deref(),
        Some(r#"{"majors":0,"l1_alts":0}"#)
    );
    let before = f.ordinary_orders().await;
    // Controlled corruption only; no production writer is claimed to accept it.
    sqlx::query("UPDATE risk_budget SET group_exposure_json=? WHERE credential_id='paper'")
        .bind("{malformed JSON")
        .execute(f.store.pool())
        .await
        .unwrap();
    let ack = f
        .execute_entry(
            "paper",
            "group_text_invalid",
            false,
            "binance|futures|ETH|USDT|USDT",
            5,
            crate::svc::now_ms(),
        )
        .await;
    assert_eq!(ack.status, p::Status::Rejected as i32, "{}", ack.message);
    assert_eq!(ack.reject_reason_code, "STALE_RISK_BUDGET");
    assert_eq!(f.ordinary_orders().await, before);
}

#[tokio::test]
async fn b3_sqlite_group_text_budget_valid_and_corrupt() {
    let f = Fixture::new().await;
    budget_group_text_scenario(&f).await;
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires owned MySQL RUSTCORE_TEST_MYSQL_URL; absence is not PASS"]
async fn b3_mysql_group_text_budget_valid_and_corrupt() {
    let scratch = crate::store::mysql_test_support::MySqlScratch::create(
        "b3close",
        "b3_mysql_group_text_budget_valid_and_corrupt",
    )
    .await
    .expect("owned MySQL required");
    let f = Fixture::with_store(scratch.connect_store().await, std::path::PathBuf::new()).await;
    budget_group_text_scenario(&f).await;
    f.cleanup().await;
    scratch.cleanup().await;
}

#[tokio::test]
#[ignore = "requires owned MySQL RUSTCORE_TEST_MYSQL_URL; absence is not PASS"]
async fn b3_mysql_one_sided_contract_vector() {
    let scratch = crate::store::mysql_test_support::MySqlScratch::create(
        "b3close",
        "b3_mysql_one_sided_contract_vector",
    )
    .await
    .expect("owned MySQL required");
    let f = Fixture::with_store(scratch.connect_store().await, std::path::PathBuf::new()).await;
    let vector = exact_close_scenario(&f).await;
    if let Ok(path) = std::env::var("B3_CLOSE_VECTOR_PATH") {
        std::fs::write(path, serde_json::to_vec_pretty(&vector).unwrap()).unwrap();
    }
    f.cleanup().await;
    scratch.cleanup().await;
}

#[tokio::test]
async fn b3_failed_fill_rolls_back_cas_and_order_but_preserves_durable_fence_for_retry() {
    let f = Fixture::new().await;
    let a = f.open("paper", "rollback", false, BTC).await;
    let request = f.close_request("paper", &a).await;
    let before = f.position(&a).await;
    sqlx::query("CREATE TRIGGER b3_fill_failure BEFORE INSERT ON engine_fills BEGIN SELECT RAISE(ABORT,'b3 controlled fill failure'); END")
        .execute(f.store.pool()).await.unwrap();
    let error = f
        .rpc::<_, p::ClosePositionResponse>(
            "/execution_engine.v2.EngineExecution/ClosePosition",
            Request::new(request.clone()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(f.position(&a).await, before);
    assert_eq!(f.ordinary_orders().await, 1);
    assert_eq!(f.receipt(&request).await.request_status, "prepared");
    assert_eq!(
        f.entry_under_fence("paper", BTC).await.reject_reason_code,
        "POSITION_CLOSE_PENDING"
    );
    sqlx::query("DROP TRIGGER b3_fill_failure")
        .execute(f.store.pool())
        .await
        .unwrap();
    assert_eq!(f.close(request.clone()).await.request_status, "accepted");
    assert_eq!(f.ordinary_orders().await, 2);
    f.cleanup().await;
}

#[tokio::test]
async fn b3_uint64_revision_roundtrip_above_js_safe_integer_is_exact() {
    let f = Fixture::new().await;
    let a = f.open("paper", "big_revision", false, BTC).await;
    sqlx::query("UPDATE position_state SET close_revision=? WHERE position_id=?")
        .bind(9_007_199_254_740_993_i64)
        .bind(&a)
        .execute(f.store.pool())
        .await
        .unwrap();
    let target = f.target("paper", &a).await;
    assert_eq!(
        target.target.as_ref().unwrap().revision,
        9_007_199_254_740_993
    );
    assert_eq!(
        target_json(&target)["target"]["revision"],
        "9007199254740993"
    );
    let request = f.close_request("paper", &a).await;
    let receipt = f.close(request).await;
    assert_eq!(receipt.target_revision, 9_007_199_254_740_993);
    assert_eq!(
        receipt_json(&receipt)["target_revision"],
        "9007199254740993"
    );
    f.cleanup().await;
}

#[tokio::test]
async fn b3_upgrade_preserves_legacy_values_and_never_backfills_execution_proof() {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!("b3-upgrade-{}.db", ulid::Ulid::new()));
    std::fs::File::create(&path).unwrap();
    let url = format!("sqlite://{}", path.display());
    let pool = sqlx::AnyPool::connect(&url).await.unwrap();
    let mut old = crate::store::embedded_migrator_for_test();
    old.migrations =
        std::borrow::Cow::Owned(old.iter().filter(|m| m.version <= 38).cloned().collect());
    old.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES('legacy','paper','old',?,'long',1,'open','1.2500',1)").bind(BTC).execute(&pool).await.unwrap();
    pool.close().await;
    for _ in 0..2 {
        let store = Store::connect(&url).await.unwrap();
        let row = sqlx::query(
            "SELECT qty,state,close_revision FROM position_state WHERE position_id='legacy'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("qty"), "1.2500");
        assert_eq!(row.get::<String, _>("state"), "open");
        assert_eq!(row.get::<i64, _>("close_revision"), 0);
        let n = sqlx::query("SELECT COUNT(*) AS n FROM position_execution_provenance")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n");
        assert_eq!(n, 0);
        let target = EngineExecutionV2Service::new(store.clone())
            .get_position_close_target(Request::new(p::GetPositionCloseTargetRequest {
                credential_id: "paper".into(),
                position_id: "legacy".into(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(target.reason_code, "POSITION_PROVENANCE_UNKNOWN");
        store.pool().close().await;
    }
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test]
#[ignore = "subprocess only, invoked by b3_process_crash_boundaries"]
async fn b3_fault_child() {
    let path =
        std::path::PathBuf::from(std::env::var("B3_CHILD_DATABASE").expect("child database"));
    sqlx::any::install_default_drivers();
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let f = Fixture::with_store(store, path).await;
    let position = std::env::var("B3_CHILD_POSITION").unwrap();
    let mut request = f.close_request("paper", &position).await;
    request.request_ref = std::env::var("B3_CHILD_REFERENCE").unwrap();
    // The selected production checkpoint SIGSTOPs the whole child process.
    let _ = f.close(request).await;
    panic!("selected checkpoint did not stop the child");
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test]
async fn b3_process_crash_boundaries_preserve_unknown_fence_or_exact_terminal() {
    for point in ["b3_after_prepare", "b3_before_commit", "b3_after_commit"] {
        let f = Fixture::new().await;
        let position = f.open("paper", point, false, BTC).await;
        let reference = ulid::Ulid::new().to_string();
        let database = f.path.clone();
        f.task.abort();
        f.store.pool().close().await;
        let marker_dir = std::env::temp_dir().join(format!("b3-fault-{}", ulid::Ulid::new()));
        std::fs::create_dir(&marker_dir).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "position_close_tests::b3_fault_child",
                "--ignored",
                "--nocapture",
            ])
            .env("B3_CHILD_DATABASE", &database)
            .env("B3_CHILD_POSITION", &position)
            .env("B3_CHILD_REFERENCE", &reference)
            .env("L4_FAULT_INJECTION_ENABLED", "1")
            .env("FAULT_INJECT", point)
            .env("L4_FAULT_MARKER_DIR", &marker_dir)
            .env("L4_FAKE_EXCHANGE_DB", marker_dir.join("unused-exchange.db"))
            .env("L4_FILE_RPC_DIR", marker_dir.join("unused-rpc"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        let marker = marker_dir.join(format!("{point}.ready"));
        while !marker.exists()
            && start.elapsed() < std::time::Duration::from_secs(25)
            && child.try_wait().unwrap().is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let reached = marker.exists();
        let marker_text = std::fs::read_to_string(&marker).unwrap_or_default();
        let child_pid = child.id();
        let _ = child.kill();
        let status = child.wait().unwrap();
        assert!(reached, "{point} was not reached; child={status}");
        assert!(marker_text.contains(&format!("pid={child_pid}\n")));
        assert!(marker_text.contains(&format!("point={point}\n")));
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9),
            "the observed process must actually receive SIGKILL"
        );
        eprintln!(
            "B3 crash evidence point={point} child={child_pid} signal=9 marker_verified=true"
        );
        let restored = Fixture::with_store(
            Store::connect(&format!("sqlite://{}", database.display()))
                .await
                .unwrap(),
            database,
        )
        .await;
        let receipt = restored
            .receipt(&p::ClosePositionRequest {
                credential_id: "paper".into(),
                request_ref: reference,
                actor_member_id: 9,
                ..Default::default()
            })
            .await;
        if point == "b3_after_commit" {
            assert_eq!(receipt.request_status, "accepted");
            assert_eq!(restored.ordinary_orders().await, 2);
            assert_eq!(
                receipt
                    .execution_outcome
                    .as_ref()
                    .unwrap()
                    .quantity
                    .as_deref(),
                Some("0")
            );
            restored.open("paper", "post_terminal", false, BTC).await;
        } else {
            assert_eq!(receipt.request_status, "pending_unknown");
            assert_eq!(restored.ordinary_orders().await, 1);
            assert_ne!(restored.position(&position).await.1, "0");
            assert_eq!(
                restored
                    .entry_under_fence("paper", BTC)
                    .await
                    .reject_reason_code,
                "POSITION_CLOSE_PENDING"
            );
        }
        restored.cleanup().await;
        std::fs::remove_file(marker).unwrap();
        std::fs::remove_dir(marker_dir).unwrap();
    }
}

#[tokio::test]
async fn b3_same_account_same_symbol_direction_different_positions_close_only_exact_id() {
    let f = Fixture::new().await;
    let first = f
        .open_with_distance("paper", "same_scope_first", false, BTC, 10)
        .await;
    let second = f
        .open_with_distance("paper", "same_scope_second", false, BTC, 10)
        .await;
    let second_before = f.position(&second).await;
    let request = f.close_request("paper", &first).await;
    let receipt = f.close(request).await;
    assert_eq!(receipt.request_status, "accepted");
    assert_eq!(receipt.position_id, first);
    assert_eq!(f.position(&first).await.1, "0");
    assert_eq!(f.position(&second).await, second_before);
    assert_eq!(f.ordinary_orders().await, 3);
    f.cleanup().await;
}

fn vector_sources() -> serde_json::Value {
    let sources = [
        (
            "proto/execution_engine_v2.proto",
            include_str!("../proto/execution_engine_v2.proto"),
        ),
        ("src/position_close.rs", include_str!("position_close.rs")),
        (
            "src/position_close_tests.rs",
            include_str!("position_close_tests.rs"),
        ),
        ("src/svc.rs", include_str!("svc.rs")),
        ("src/svc_v2.rs", include_str!("svc_v2.rs")),
        ("src/position_state.rs", include_str!("position_state.rs")),
        ("src/reconcile.rs", include_str!("reconcile.rs")),
        ("src/store.rs", include_str!("store.rs")),
        (
            "migrations/0039_position_close_revision.sql",
            include_str!("../migrations/0039_position_close_revision.sql"),
        ),
        (
            "migrations/0040_position_execution_provenance.sql",
            include_str!("../migrations/0040_position_execution_provenance.sql"),
        ),
        (
            "migrations/0041_position_close_requests.sql",
            include_str!("../migrations/0041_position_close_requests.sql"),
        ),
    ];
    serde_json::Value::Array(sources.iter().map(|(path,source)|serde_json::json!({"file":path,"sha256":crate::svc::sha256_hex(source)})).collect())
}
