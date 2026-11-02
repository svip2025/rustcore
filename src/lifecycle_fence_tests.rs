//! B8 production tonic entry + persisted SQLite + one-sided native wire evidence.
use crate::{proto_v2 as p, store::Store};
use prost::Message;
use sqlx::Row;
use std::{path::PathBuf, time::Duration};
use tonic::{Request, Status};
pub(crate) const ARM: &str = "/execution_engine.v2.EngineExecution/ArmLifecycleFence";
pub(crate) const GET: &str = "/execution_engine.v2.EngineExecution/GetLifecycleFence";
const TOKEN: &str = "b8-owned-loopback-token";

pub(crate) struct Fixture {
    pub store: Store,
    pub path: PathBuf,
    pub address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    pub async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("b8-fence-{}.db", ulid::Ulid::new()));
        Self::at(path, None).await
    }
    pub async fn at(path: PathBuf, exec: Option<crate::svc::EngineExecutionService>) -> Self {
        sqlx::any::install_default_drivers();
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        Self::serve(store, path, exec).await
    }
    pub async fn serve(
        store: Store,
        path: PathBuf,
        exec: Option<crate::svc::EngineExecutionService>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let execution = exec.unwrap_or_else(|| {
            crate::svc::EngineExecutionService::new(store.clone())
                .without_background_exchange_tasks()
        });
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            execution.clone(),
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
            crate::svc_v2::EngineExecutionV2Service::with_shared_execution(
                store.clone(),
                execution,
            ),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![TOKEN.into()])),
        );
        let task = tokio::spawn(async move { router.serve_with_incoming(incoming).await.unwrap() });
        Self {
            store,
            path,
            address,
            task,
        }
    }
    pub async fn rpc<Q, R>(&self, path: &'static str, q: Q) -> Result<R, Status>
    where
        Q: Message + Default + Send + Sync + 'static,
        R: Message + Default + Send + Sync + 'static,
    {
        self.raw(path, q, TOKEN).await
    }
    async fn raw<Q, R>(&self, path: &'static str, q: Q, token: &str) -> Result<R, Status>
    where
        Q: Message + Default + Send + Sync + 'static,
        R: Message + Default + Send + Sync + 'static,
    {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready().await.unwrap();
        let mut req = Request::new(q);
        req.set_timeout(Duration::from_secs(10));
        if !token.is_empty() {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        let r: tonic::Response<R> = grpc
            .unary(
                req,
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::default(),
            )
            .await?;
        Ok(r.into_inner())
    }
    pub async fn replace_execution(self, exec: crate::svc::EngineExecutionService) -> Self {
        self.task.abort();
        Self::serve(self.store, self.path, Some(exec)).await
    }
    pub async fn stop(&self) {
        self.task.abort();
        self.store.pool().close().await;
    }
    pub async fn cleanup(self) {
        self.stop().await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}
pub(crate) fn arm_request(
    store: &Store,
    credential: &str,
    account: &str,
) -> p::ArmLifecycleFenceRequest {
    p::ArmLifecycleFenceRequest {
        request_ref: ulid::Ulid::new().to_string(),
        actor_member_id: 9,
        account_uid: account.into(),
        credential_id: credential.into(),
        purpose: "disable".into(),
        expected_boot_id: store.boot_id().into(),
        schema_version: 1,
    }
}
pub(crate) fn get_request(r: &p::ArmLifecycleFenceRequest) -> p::GetLifecycleFenceRequest {
    p::GetLifecycleFenceRequest {
        request_ref: r.request_ref.clone(),
        actor_member_id: r.actor_member_id,
        account_uid: r.account_uid.clone(),
        credential_id: r.credential_id.clone(),
        purpose: r.purpose.clone(),
        schema_version: 1,
    }
}
async fn count(store: &Store, table: &str) -> i64 {
    sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("n")
}

#[tokio::test]
async fn lifecycle_rpc_identity_auth_boot_replay_and_current_are_exact() {
    let f = Fixture::new().await;
    let req = arm_request(&f.store, "7", "tacct_fixture");
    for token in ["", "wrong"] {
        assert_eq!(
            f.raw::<_, p::LifecycleFenceResponse>(ARM, req.clone(), token)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            f.raw::<_, p::LifecycleFenceResponse>(GET, get_request(&req), token)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }
    for credential in [
        "cred_7",
        "07",
        " 7",
        "0",
        "9007199254740992",
        "paper",
        "paper:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "paper:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let mut bad = req.clone();
        bad.credential_id = credential.into();
        assert_eq!(
            f.rpc::<_, p::LifecycleFenceResponse>(ARM, bad)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let before: p::LifecycleFenceResponse = f.rpc(GET, get_request(&req)).await.unwrap();
    assert!(!before.found);
    assert_eq!(
        before.identity.as_ref().unwrap().request_ref,
        req.request_ref
    );
    assert!(
        !before
            .current_observation
            .as_ref()
            .unwrap()
            .admission_blocked
    );
    assert_eq!(
        before.current_observation.as_ref().unwrap().engine_boot_id,
        f.store.boot_id()
    );
    let mut wrong = req.clone();
    wrong.expected_boot_id = ulid::Ulid::new().to_string();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, wrong)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(count(&f.store, "lifecycle_credential_fences").await, 0);
    let first: p::LifecycleFenceResponse = f.rpc(ARM, req.clone()).await.unwrap();
    let repeat: p::LifecycleFenceResponse = f.rpc(ARM, req.clone()).await.unwrap();
    assert_eq!(first.original_arm, repeat.original_arm);
    assert!(first.found);
    assert_eq!(first.original_arm.as_ref().unwrap().fence_revision, 1);
    assert_eq!(
        first.current_observation.as_ref().unwrap().scope,
        "credential_on_current_executor"
    );
    assert_eq!(
        first
            .current_observation
            .as_ref()
            .unwrap()
            .entry_cancellation,
        "not_started"
    );
    assert_eq!(
        first.current_observation.as_ref().unwrap().settlement,
        "unknown"
    );
    for field in 0..5 {
        let mut bad = req.clone();
        match field {
            0 => bad.actor_member_id = 10,
            1 => bad.account_uid = "tacct_foreign".into(),
            2 => bad.credential_id = "8".into(),
            3 => bad.purpose = "rotate".into(),
            _ => bad.expected_boot_id = ulid::Ulid::new().to_string(),
        };
        assert_eq!(
            f.rpc::<_, p::LifecycleFenceResponse>(ARM, bad.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::AlreadyExists
        );
        if field < 4 {
            assert_eq!(
                f.rpc::<_, p::LifecycleFenceResponse>(GET, get_request(&bad))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::AlreadyExists
            );
        }
    }
    let mut next = req.clone();
    next.request_ref = ulid::Ulid::new().to_string();
    next.purpose = "rotate".into();
    let second: p::LifecycleFenceResponse = f.rpc(ARM, next).await.unwrap();
    assert_eq!(second.original_arm.unwrap().fence_revision, 2);
    let old: p::LifecycleFenceResponse = f.rpc(GET, get_request(&req)).await.unwrap();
    assert_eq!(old.original_arm, first.original_arm);
    assert_eq!(old.current_observation.unwrap().fence_revision, 2);
    assert_eq!(count(&f.store, "lifecycle_credential_fences").await, 1);
    assert_eq!(count(&f.store, "lifecycle_fence_requests").await, 2);
    if let Ok(dir) = std::env::var("B8_NATIVE_VECTOR_DIR") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in [
            ("arm-request", req.encode_to_vec()),
            ("get-request", get_request(&req).encode_to_vec()),
            ("not-found", before.encode_to_vec()),
            ("armed", first.encode_to_vec()),
            ("replayed", repeat.encode_to_vec()),
        ] {
            std::fs::write(dir.join(format!("{name}.bin")), bytes).unwrap();
        }
    }
    f.cleanup().await;
}

#[tokio::test]
async fn lifecycle_receipt_failure_rolls_back_fence_and_revision() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER b8_receipt_failure BEFORE INSERT ON lifecycle_fence_requests BEGIN SELECT RAISE(ABORT,'owned injected audit failure'); END").execute(f.store.pool()).await.unwrap();
    let req = arm_request(&f.store, "18", "rollback");
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, req.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(count(&f.store, "lifecycle_credential_fences").await, 0);
    sqlx::query("DROP TRIGGER b8_receipt_failure")
        .execute(f.store.pool())
        .await
        .unwrap();
    let good: p::LifecycleFenceResponse = f.rpc(ARM, req).await.unwrap();
    assert_eq!(good.original_arm.unwrap().fence_revision, 1);
    f.cleanup().await;
}

#[tokio::test]
async fn lifecycle_production_paper_entry_denied_but_exact_close_remains_usable() {
    let f = crate::position_close_tests::Fixture::new().await;
    let account = "tacct_b8_paper";
    let credential = format!("paper:{}", &crate::svc::sha256_hex(account)[..32]);
    let position = f
        .open(
            &credential,
            "b8_before",
            false,
            crate::position_close_tests::BTC,
        )
        .await;
    let req = arm_request(&f.store, &credential, account);
    let response = f
        .rpc::<_, p::LifecycleFenceResponse>(ARM, Request::new(req))
        .await
        .unwrap()
        .into_inner();
    assert!(response.current_observation.unwrap().admission_blocked);
    let ack = f
        .execute_entry(
            &credential,
            "b8_after",
            true,
            crate::position_close_tests::BTC,
            5,
            crate::svc::now_ms(),
        )
        .await;
    assert_eq!(ack.status, p::Status::Rejected as i32);
    assert_eq!(ack.reject_reason_code, crate::lifecycle_fence::DENIED);
    let verdict =
        sqlx::query("SELECT replay_input_json FROM risk_verdicts WHERE risk_verdict_id=?")
            .bind(&ack.risk_verdict_id)
            .fetch_one(f.store.pool())
            .await
            .unwrap();
    assert!(crate::store::try_get_text(&verdict, "replay_input_json")
        .unwrap()
        .unwrap()
        .contains("lifecycle_fence"));
    let request = f.close_request(&credential, &position).await;
    let closed = f.close(request).await;
    assert_eq!(closed.request_status, "accepted");
    assert_eq!(
        closed.position_observation.unwrap().quantity.as_deref(),
        Some("0")
    );
    f.cleanup().await;
}

#[tokio::test]
async fn lifecycle_restart_preserves_original_and_denies_new_entry() {
    let f = Fixture::new().await;
    let req = arm_request(&f.store, "37", "restart_account");
    let first: p::LifecycleFenceResponse = f.rpc(ARM, req.clone()).await.unwrap();
    let path = f.path.clone();
    let input = path.with_extension("request.bin");
    let output = path.with_extension("response.bin");
    std::fs::write(&input, req.encode_to_vec()).unwrap();
    f.stop().await;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lifecycle_fence_tests::lifecycle_restart_child",
            "--ignored",
            "--nocapture",
        ])
        .env("B8_RESTART_DB", &path)
        .env("B8_RESTART_INPUT", &input)
        .env("B8_RESTART_OUTPUT", &output)
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            assert!(s.success());
            break;
        }
        if started.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("owned restart child exceeded deadline");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let response =
        p::LifecycleFenceResponse::decode(std::fs::read(&output).unwrap().as_slice()).unwrap();
    assert_eq!(response.original_arm, first.original_arm);
    assert_ne!(
        response
            .current_observation
            .as_ref()
            .unwrap()
            .engine_boot_id,
        req.expected_boot_id
    );
    assert!(response.current_observation.unwrap().admission_blocked);
    std::fs::remove_file(input).unwrap();
    std::fs::remove_file(output).unwrap();
    f.cleanup().await;
}
#[tokio::test]
#[ignore = "explicit child process of lifecycle_restart_preserves_original_and_denies_new_entry"]
async fn lifecycle_restart_child() {
    let path = PathBuf::from(std::env::var("B8_RESTART_DB").expect("explicit restart DB required"));
    let req = p::ArmLifecycleFenceRequest::decode(
        std::fs::read(std::env::var("B8_RESTART_INPUT").unwrap())
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let f = Fixture::at(path, None).await;
    let reply: p::LifecycleFenceResponse = f.rpc(ARM, req.clone()).await.unwrap();
    assert_ne!(f.store.boot_id(), req.expected_boot_id);
    assert!(
        crate::lifecycle_fence::authorize_send(Some(&f.store), &req.credential_id)
            .await
            .unwrap_err()
            .to_string()
            .contains(crate::lifecycle_fence::DENIED)
    );
    std::fs::write(
        std::env::var("B8_RESTART_OUTPUT").unwrap(),
        reply.encode_to_vec(),
    )
    .unwrap();
    f.stop().await;
}

#[tokio::test]
async fn lifecycle_member_and_platform_reset_cannot_unfence_entry() {
    let f = Fixture::new().await;
    let credential = "71";
    let scope = "cred:71";
    let _: p::LifecycleFenceResponse = f
        .rpc(ARM, arm_request(&f.store, credential, "reset_account"))
        .await
        .unwrap();
    for member in [false, true] {
        let arm: p::SetKillSwitchResponse = f
            .rpc(
                "/execution_engine.v2.EngineExecution/SetKillSwitch",
                p::SetKillSwitchRequest {
                    scope_key: scope.into(),
                    level: "pause_new".into(),
                    reason: "owned reset independence fixture".into(),
                    trigger_source: if member { "member_api" } else { "manual_api" }.into(),
                    actor: "b8-local".into(),
                    authority_member_id: member.then_some(9),
                    request_ref: member.then(|| ulid::Ulid::new().to_string()),
                    schema_version: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(arm.accepted);
        let observed = crate::kill_switch::observe_flatten_generations(&f.store)
            .await
            .unwrap();
        crate::kill_switch::record_reconcile_for_scope(
            &f.store,
            scope,
            credential,
            crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            crate::svc::now_ms() + 1,
            &observed,
        )
        .await
        .unwrap();
        if member {
            let version =
                sqlx::query("SELECT admission_version FROM member_reset_admission WHERE id=1")
                    .fetch_one(f.store.pool())
                    .await
                    .unwrap()
                    .get::<i64, _>("admission_version");
            let admission: p::AdmitMemberResetResponse = f
                .rpc(
                    "/execution_engine.v2.EngineExecution/AdmitMemberReset",
                    p::AdmitMemberResetRequest {
                        basis: "clean_attestation:b8-isolated-test".into(),
                        admitted_by: "b8-local".into(),
                        demote_unknown_member_authority: false,
                        expected_boot_id: f.store.boot_id().into(),
                        expected_admission_version: Some(version),
                        schema_version: 1,
                    },
                )
                .await
                .unwrap();
            assert!(admission.admitted);
        }
        let generation =
            sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
                .bind(scope)
                .fetch_one(f.store.pool())
                .await
                .unwrap()
                .get::<i64, _>("flatten_generation");
        let reset: p::ResetKillSwitchResponse = f
            .rpc(
                "/execution_engine.v2.EngineExecution/ResetKillSwitch",
                p::ResetKillSwitchRequest {
                    scope_key: scope.into(),
                    reset_by: if member { "member:9" } else { "b8-operator" }.into(),
                    reset_authority: member.then_some(p::ResetAuthority::Member as i32),
                    expected_authority_revision: member.then_some(generation),
                    authority_member_id: member.then_some(9),
                    request_ref: Some(ulid::Ulid::new().to_string()),
                    schema_version: 1,
                },
            )
            .await
            .unwrap();
        assert!(reset.accepted, "{}", reset.message);
        assert!(
            crate::lifecycle_fence::authorize_send(Some(&f.store), credential)
                .await
                .unwrap_err()
                .to_string()
                .contains(crate::lifecycle_fence::DENIED)
        );
    }
    f.cleanup().await;
}

#[tokio::test]
async fn lifecycle_concurrent_duplicate_commits_one_original_and_read_is_no_write() {
    let f = Fixture::new().await;
    let req = arm_request(&f.store, "82", "concurrent_account");
    let (a, b) = tokio::join!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, req.clone()),
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, req.clone())
    );
    assert_eq!(a.unwrap().original_arm, b.unwrap().original_arm);
    assert_eq!(count(&f.store, "lifecycle_fence_requests").await, 1);
    let before = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    let _: p::LifecycleFenceResponse = f.rpc(GET, get_request(&req)).await.unwrap();
    let after = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(before, after);
    // Source failure is not an unarmed observation and cannot admit a sender.
    sqlx::query("DROP TABLE lifecycle_credential_fences")
        .execute(f.store.pool())
        .await
        .unwrap();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(GET, get_request(&req))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(crate::lifecycle_fence::authorize_send(Some(&f.store), "82")
        .await
        .is_err());
    assert!(crate::lifecycle_fence::authorize_send(None, "82")
        .await
        .is_err());
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "explicit private MySQL scratch gate; missing configuration is failure, never skip"]
async fn lifecycle_mysql_rpc_atomic_exact_scope_and_restart_store() {
    use crate::binance_futures::tests::TestEnvVar;
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let mut env = vec![TestEnvVar::set("RUSTCORE_REQUIRE_MYSQL_TESTS".into(), "1")];
    if let Some(path) = std::env::var_os("RUSTCORE_B8_MYSQL_URL_FILE") {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(&path).unwrap();
            assert!(
                m.is_file() && m.permissions().mode() & 0o077 == 0,
                "private regular MySQL URL file required"
            );
        }
        let value = std::fs::read_to_string(path).unwrap();
        env.push(TestEnvVar::set(
            "RUSTCORE_TEST_MYSQL_URL".into(),
            value.trim(),
        ));
    }
    assert!(std::env::var("RUSTCORE_TEST_MYSQL_URL")
        .expect("explicit MySQL URL required")
        .starts_with("mysql://"));
    let scratch =
        crate::store::mysql_test_support::MySqlScratch::create("b8fence", "B8 lifecycle MySQL")
            .await
            .expect("explicit scratch required");
    // Only the owned random schema name is logged, never a URL or credential.
    eprintln!(
        "B8_MYSQL_OWNED_SCHEMA={}",
        url::Url::parse(scratch.database_url())
            .unwrap()
            .path()
            .trim_start_matches('/')
    );
    let store = scratch.connect_store().await;
    let f = Fixture::serve(store, PathBuf::new(), None).await;
    let req = arm_request(&f.store, "97", "CaseSensitiveAccount");
    let (a, b) = tokio::join!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, req.clone()),
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, req.clone())
    );
    let first = a.unwrap();
    assert_eq!(first.original_arm, b.unwrap().original_arm);
    assert_eq!(count(&f.store, "lifecycle_fence_requests").await, 1);
    let mut foreign = get_request(&req);
    foreign.account_uid = foreign.account_uid.to_ascii_lowercase();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(GET, foreign)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    let mut other = get_request(&req);
    other.credential_id = "98".into();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(GET, other)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    let mut new = arm_request(&f.store, "98", "rollback_mysql");
    // MySQL trigger DDL uses the text protocol, not prepared statements.
    sqlx::raw_sql("CREATE TRIGGER b8_fail_receipt BEFORE INSERT ON lifecycle_fence_requests FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='owned B8 audit failure'").execute(f.store.pool()).await.unwrap();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, new.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(count(&f.store, "lifecycle_credential_fences").await, 1);
    sqlx::raw_sql("DROP TRIGGER b8_fail_receipt")
        .execute(f.store.pool())
        .await
        .unwrap();
    new.expected_boot_id = ulid::Ulid::new().to_string();
    assert_eq!(
        f.rpc::<_, p::LifecycleFenceResponse>(ARM, new)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let pool2 = scratch.connect_store().await;
    assert!(crate::lifecycle_fence::authorize_send(Some(&pool2), "97")
        .await
        .unwrap_err()
        .to_string()
        .contains(crate::lifecycle_fence::DENIED));
    // Another Store is not a new process/boot proof; this checks persisted reads
    // and pooled connections only. Actual process restart is tested separately.
    let another = Fixture::serve(pool2, PathBuf::new(), None).await;
    let replay: p::LifecycleFenceResponse = another.rpc(ARM, req).await.unwrap();
    assert_eq!(first.original_arm, replay.original_arm);
    another.stop().await;
    f.stop().await;
    if std::env::var("RUSTCORE_B8_PRESERVE_MYSQL").as_deref() != Ok("1") {
        scratch.cleanup().await;
    }
    // Failure panics before cleanup, preserving the owned schema for a full dump.
}
