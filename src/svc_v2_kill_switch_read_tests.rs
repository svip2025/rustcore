// P4b · svc 层验收（T2）：读投影 handler 映射、上界、闭世界拒绝、
// member set/reset 的字段纪律与准入端点映射（AM §1 的 P4b 服务腿）。

use super::*;

async fn p4b_store(label: &str) -> (Store, std::path::PathBuf) {
    static DRIVERS: std::sync::Once = std::sync::Once::new();
    DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!(
        "rustcore-t2p4b-{label}-{}.db",
        ulid::Ulid::new()
    ));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("t2 scratch store");
    (store, path)
}

fn read_request(keys: &[&str], refs: &[&str]) -> Request<GetKillSwitchStatesRequest> {
    Request::new(GetKillSwitchStatesRequest {
        scope_keys: keys.iter().map(|k| k.to_string()).collect(),
        request_refs: refs.iter().map(|r| r.to_string()).collect(),
        schema_version: 1,
    })
}

#[tokio::test]
async fn t2_read_maps_tri_state_presence_and_boot_fields() {
    let (store, path) = p4b_store("t2read").await;
    let service = EngineExecutionV2Service::new(store.clone());
    service
        .set_kill_switch(Request::new(SetKillSwitchRequest {
            scope_key: "cred:t2read".to_string(),
            level: "pause_new".to_string(),
            reason: "fixture".to_string(),
            trigger_source: "manual_api".to_string(),
            actor: "admin".to_string(),
            expected_authority_revision: None,
            authority_member_id: None,
            request_ref: None,
            schema_version: 1,
        }))
        .await
        .expect("platform arm");
    let response = service
        .get_kill_switch_states(read_request(&["cred:t2read", "cred:absent"], &[]))
        .await
        .expect("read rpc");
    let body = response.into_inner();
    assert_eq!(body.rows.len(), 2, "rows mirror the request order and count");
    let present = &body.rows[0];
    assert!(present.found);
    assert_eq!(present.level, "pause_new");
    // 从未投影：TriState=UNKNOWN(0)、时间字段缺席（不是 0）。
    assert_eq!(present.last_reconcile_clean, 0);
    assert!(present.last_reconcile_at_ms.is_none(), "absence is the never-projected signal");
    assert_eq!(present.authority_kind, 1, "manual arm writes platform authority");
    let absent = &body.rows[1];
    assert!(!absent.found, "no row is a deterministic fact, not an error");
    // 平台链字段恒填（FA2 §1.1）；会员面组装时剥除（B6 的职责，此处验证引擎侧存在）。
    assert!(!body.engine_boot_id.is_empty());
    assert_eq!(body.admission_version, 1, "boot registration bumped the seed version once");
    assert!(!body.snapshot_stale);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t2_read_ceilings_and_closed_world_rejections() {
    let (store, path) = p4b_store("t2ceil").await;
    let service = EngineExecutionV2Service::new(store.clone());
    let many: Vec<String> = (0..257).map(|i| format!("cred:t2c{i}")).collect();
    let error = service
        .get_kill_switch_states(Request::new(GetKillSwitchStatesRequest {
            scope_keys: many,
            request_refs: vec![],
            schema_version: 1,
        }))
        .await
        .expect_err("257 scopes must be INVALID_ARGUMENT");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let error = service
        .get_kill_switch_states(read_request(&["member:1"], &[]))
        .await
        .expect_err("non-canonical scope must be rejected wholesale");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let error = service
        .get_kill_switch_states(read_request(&["cred:t2c0"], &["not-a-ulid"]))
        .await
        .expect_err("non-canonical ref must be rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t2_member_set_field_discipline() {
    let (store, path) = p4b_store("t2set").await;
    let service = EngineExecutionV2Service::new(store.clone());
    let base = |member_id: Option<i32>, reference: Option<String>, revision: Option<i64>| {
        SetKillSwitchRequest {
            scope_key: "cred:t2set".to_string(),
            level: "pause_new".to_string(),
            reason: "fixture".to_string(),
            trigger_source: "member_api".to_string(),
            actor: "member:5501".to_string(),
            expected_authority_revision: revision,
            authority_member_id: member_id,
            request_ref: reference,
            schema_version: 1,
        }
    };
    // 缺身份/缺 ref → INVALID。
    let error = service
        .set_kill_switch(Request::new(base(None, Some("01A7XQZ90ZYY9VNEHNJQ000001".into()), None)))
        .await
        .expect_err("member_api without member id must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let error = service
        .set_kill_switch(Request::new(base(Some(5501), None, None)))
        .await
        .expect_err("member_api without request_ref must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    // 降险链不得带 revision（WO §3-1）。
    let error = service
        .set_kill_switch(Request::new(base(Some(5501), Some("01A7XQZ90ZYY9VNEHNJQ000001".into()), Some(3))))
        .await
        .expect_err("member arm with a revision must be refused");
    assert!(error.message().contains("no read gate"));
    // 非 member 源携带会员字段 → INVALID。
    let error = service
        .set_kill_switch(Request::new(SetKillSwitchRequest {
            scope_key: "cred:t2set".to_string(),
            level: "pause_new".to_string(),
            reason: "fixture".to_string(),
            trigger_source: "manual_api".to_string(),
            actor: "admin".to_string(),
            expected_authority_revision: None,
            authority_member_id: Some(5501),
            request_ref: None,
            schema_version: 1,
        }))
        .await
        .expect_err("platform source with member fields must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    // 正向：合法 member arm 生效并回显 ref。
    let response = service
        .set_kill_switch(Request::new(base(Some(5501), Some("01A7XQZ90ZYY9VNEHNJQ000001".into()), None)))
        .await
        .expect("member arm");
    assert!(response.into_inner().request_ref.unwrap().starts_with("01A7"));
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t2_member_reset_requires_the_full_constraint_set() {
    let (store, path) = p4b_store("t2reset").await;
    let service = EngineExecutionV2Service::new(store.clone());
    let mut request = ResetKillSwitchRequest {
        scope_key: "cred:t2reset".to_string(),
        reset_by: "member:5501".to_string(),
        reset_authority: Some(crate::proto_v2::ResetAuthority::Member as i32),
        expected_authority_revision: Some(0),
        authority_member_id: Some(0),
        request_ref: None,
        schema_version: 1,
    };
    let error = service
        .reset_kill_switch(Request::new(request.clone()))
        .await
        .expect_err("MEMBER reset without fields must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    request.request_ref = Some("01A7XQZ90ZYY9VNEHNJQ000002".to_string());
    request.expected_authority_revision = Some(1);
    // member_id=0 仍拒。
    let error = service
        .reset_kill_switch(Request::new(request.clone()))
        .await
        .expect_err("zero member id must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    // 字段齐但未准入 → FAILED_PRECONDITION MEMBER_RESET_NOT_ADMITTED。
    request.authority_member_id = Some(5501);
    let error = service
        .reset_kill_switch(Request::new(request))
        .await
        .expect_err("default-disabled admission must fail closed");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("MEMBER_RESET_NOT_ADMITTED"));
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t2_platform_reset_stays_unconstrained() {
    let (store, path) = p4b_store("t2preset").await;
    let service = EngineExecutionV2Service::new(store.clone());
    // 平台 arm + 平台 reset（缺省 reset_authority）必须走既有无约束语义：
    // 先 arm 成功，再 reset 成功（M10a 的击杀面：全量 MEMBER 化会让这组失败）。
    service
        .set_kill_switch(Request::new(SetKillSwitchRequest {
            scope_key: "cred:t2preset".to_string(),
            level: "pause_new".to_string(),
            reason: "platform fixture".to_string(),
            trigger_source: "manual_api".to_string(),
            actor: "admin".to_string(),
            expected_authority_revision: None,
            authority_member_id: None,
            request_ref: None,
            schema_version: 1,
        }))
        .await
        .expect("platform arm");
    // 未投影真值：reset 会被收敛前置拒绝——这本身证明它走的是**平台**
    // 判据链（member 路径会先撞 NotAdmitted）。断言错误信息是收敛前置而非
    // MEMBER_RESET_NOT_ADMITTED。
    let error = service
        .reset_kill_switch(Request::new(ResetKillSwitchRequest {
            scope_key: "cred:t2preset".to_string(),
            reset_by: "platform_admin:1".to_string(),
            reset_authority: None,
            expected_authority_revision: None,
            authority_member_id: None,
            request_ref: None,
            schema_version: 1,
        }))
        .await
        .expect_err("platform reset without a clean reconcile is refused");
    assert!(
        error.message().contains("zero diff"),
        "platform reset must run the platform reconcile precondition chain, got: {}",
        error.message()
    );
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t2_admit_maps_shape_and_binding_errors() {
    let (store, path) = p4b_store("t2admit").await;
    let service = EngineExecutionV2Service::new(store.clone());
    // 形状：basis 前缀。
    let error = service
        .admit_member_reset(Request::new(AdmitMemberResetRequest {
            basis: "bogus".to_string(),
            admitted_by: "op".to_string(),
            demote_unknown_member_authority: false,
            expected_boot_id: store.boot_id().to_string(),
            expected_admission_version: Some(1),
            schema_version: 1,
        }))
        .await
        .expect_err("bad basis prefix must be INVALID");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    // 绑定：版本不符（当前是 1，给 0 是合法初值但不是当前值）→ FAILED。
    let error = service
        .admit_member_reset(Request::new(AdmitMemberResetRequest {
            basis: "clean_attestation:t".to_string(),
            admitted_by: "op".to_string(),
            demote_unknown_member_authority: false,
            expected_boot_id: store.boot_id().to_string(),
            expected_admission_version: Some(0),
            schema_version: 1,
        }))
        .await
        .expect_err("stale version must be FAILED_PRECONDITION");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    // 正向：当前 boot/版本 → admitted，回显服务端盖章。
    let response = service
        .admit_member_reset(Request::new(AdmitMemberResetRequest {
            basis: "clean_attestation:t".to_string(),
            admitted_by: "op".to_string(),
            demote_unknown_member_authority: false,
            expected_boot_id: store.boot_id().to_string(),
            expected_admission_version: Some(1),
            schema_version: 1,
        }))
        .await
        .expect("admit");
    let body = response.into_inner();
    assert!(body.admitted);
    assert_eq!(body.admitted_boot_id, store.boot_id());
    assert_eq!(body.admission_version, 2);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}
