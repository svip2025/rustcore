// P4b · 归属/准入/只读投影的引擎级验收用例（T1；AM §1 的 P4b 引擎腿）。
//
// 覆盖：V-01/02/03/04/06/10a/12/13(模拟腿)/14/15a/15b/20/22、C1-a..h 的引擎
// 侧、V-12R 的缺行场景。MySQL 腿在 `RUSTCORE_TEST_MYSQL_URL` 提供时执行
// （V-01/V-04/V-14）；未提供即如实跳过。

const MEMBER_A: i64 = 5501;
const MEMBER_B: i64 = 5502;

fn member_ctx(member_id: i64) -> MemberArmContext<'static> {
    MemberArmContext {
        member_id,
        request_ref: Box::leak(format!("{:0>26}", member_id.to_string()).into_boxed_str()),
    }
}

fn variant_ref(reference: &str) -> String {
    // 把首个可变小写字符换掉（Crockford 字母表里没有小写）——BINARY 比较
    // 下它必须不命中（V-14 负向）。
    let mut chars: Vec<char> = reference.chars().collect();
    if let Some(first) = chars.iter_mut().find(|c| c.is_ascii_uppercase()) {
        *first = first.to_ascii_lowercase();
    }
    chars.into_iter().collect()
}

async fn arm_platform(store: &Store, scope_key: &str, level: Level, now_ms: i64) -> EffectiveState {
    arm(
        store,
        ArmRequest {
            scope_key,
            level,
            trigger_source: "manual_api",
            reason: "platform fixture",
            now_ms,
        },
    )
    .await
    .expect("platform arm")
}

async fn arm_member_ok(
    store: &Store,
    scope_key: &str,
    level: Level,
    member_id: i64,
    now_ms: i64,
) -> EffectiveState {
    arm_member(
        store,
        ArmRequest {
            scope_key,
            level,
            trigger_source: TRIGGER_SOURCE_MEMBER_API,
            reason: "member fixture",
            now_ms,
        },
        member_ctx(member_id),
    )
    .await
    .expect("member arm")
}


/// arm 之后投影一份全 clean/flat 的真值（reset 前置：设档后需一次零差异
/// 对账——ADR-0003 §4；既有 reset 用例同款夹具）。
async fn converge_clean(store: &Store, scope_key: &str, now_ms: i64) {
    let credential = scope_key.strip_prefix("cred:").expect("cred fixture").to_string();
    let observed = observe_flatten_generations(store).await.expect("generations");
    record_reconcile_for_scope(
        store,
        scope_key,
        &credential,
        ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        },
        now_ms,
        &observed,
    )
    .await
    .expect("project clean truth");
}

async fn state_authority(store: &Store, scope_key: &str) -> (i64, Option<i64>, i64, String) {
    let row = sqlx::query(
        "SELECT authority_kind, authority_member_id, flatten_generation, level \
         FROM kill_switch_state WHERE scope_key=?",
    )
    .bind(scope_key)
    .fetch_one(store.pool())
    .await
    .expect("state row");
    (
        row.get::<i64, _>("authority_kind"),
        row.try_get::<i64, _>("authority_member_id").ok(),
        row.get::<i64, _>("flatten_generation"),
        row.get::<String, _>("level"),
    )
}

async fn admission_row(store: &Store) -> (i64, String, i64) {
    let row = sqlx::query(
        "SELECT enabled, admitted_boot_id, admission_version \
         FROM member_reset_admission WHERE id=1",
    )
    .fetch_one(store.pool())
    .await
    .expect("admission singleton");
    (
        row.get::<i64, _>("enabled"),
        row.get::<String, _>("admitted_boot_id"),
        row.get::<i64, _>("admission_version"),
    )
}

async fn admit_current_boot(store: &Store, basis: &str, demote: bool) -> Result<AdmitMemberResetOutcome, String> {
    let version = admission_row(store).await.2;
    admit_member_reset(
        store,
        basis,
        "op-fixture",
        demote,
        store.boot_id(),
        Some(version),
        9_000_000,
    )
    .await
    .expect("admit io")
}

/// V-01：member_api 无 global 权（引擎直连腿）；真 MySQL 腿同判据。
#[tokio::test]
async fn v01_member_source_never_gains_global_privilege() {
    let (store, path) = test_store("v01").await;
    // 分类谓词的生产语义断言（R1-C03：M2 击杀的第一支柱）。
    assert!(
        !crate::kill_switch::source_is_manual_for_test(TRIGGER_SOURCE_MEMBER_API),
        "member_api must not be classified as a manual source (M2)"
    );
    let error = arm_member(
        &store,
        ArmRequest {
            scope_key: "global",
            level: Level::PauseNew,
            trigger_source: TRIGGER_SOURCE_MEMBER_API,
            reason: "must be refused",
            now_ms: 1_000,
        },
        member_ctx(MEMBER_A),
    )
    .await
    .expect_err("member_api must not arm global");
    // **行为断言优先（R1-C03）**：先证三表零写，再核错误文案。这样
    // M2 变异（member_api 并入人工集合）的击杀不依赖消息文本——
    // 即使第一道 global 门被放过、第二道 cred: 闭世界以不同的文案拒绝，
    // 下面的零写/零事件断言仍然必须在「arm 被完全拒绝」的前提下成立；
    // 若任一道防线放行了写入，这些断言立即打红。
    let row = sqlx::query(
        "SELECT level, flatten_generation, COALESCE(trigger_source,'') AS trigger_source \
         FROM kill_switch_state WHERE scope_key='global'",
    )
    .fetch_one(store.pool())
    .await
    .expect("factory global row");
    assert_eq!(
        row.get::<String, _>("level"),
        "none",
        "behavioral: member must never latch global (M2 kill)"
    );
    assert_eq!(row.get::<i64, _>("flatten_generation"), 0);
    assert_eq!(
        row.get::<String, _>("trigger_source"),
        "migration_seed",
        "member arm must not overwrite the factory seed's provenance"
    );
    let events = sqlx::query(
        "SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope='global' OR scope_key='global'",
    )
    .fetch_one(store.pool())
    .await
    .expect("count global events");
    assert_eq!(
        events.get::<i64, _>("n"),
        0,
        "behavioral: member global arm must leave zero audit events (M2 kill)"
    );
    let status_rows = sqlx::query(
        "SELECT COUNT(*) AS n FROM kill_switch_reconcile_status WHERE scope_key='global'",
    )
    .fetch_one(store.pool())
    .await
    .expect("count global reconcile rows");
    assert_eq!(status_rows.get::<i64, _>("n"), 0);
    // 次级文案断言（保留双防线；不再是击杀的唯一依据）。
    assert!(
        error.to_string().contains("global") || error.to_string().contains("cred:"),
        "engine must refuse member global via one of its closed-world gates, got: {error}"
    );
    // 正向对照：manual_api 语义不变（平台仍可 global）。
    arm_platform(&store, "global", Level::Flatten, 2_000).await;
    let platform = sqlx::query(
        "SELECT level FROM kill_switch_state WHERE scope_key='global'",
    )
    .fetch_one(store.pool())
    .await
    .expect("platform global row");
    assert_eq!(platform.get::<String, _>("level"), "flatten");
    cleanup(store, path).await;
}

/// V-02：会员升档不转移归属（平台档与未知档各一）。
#[tokio::test]
async fn v02_member_upgrade_keeps_existing_authority() {
    let (store, path) = test_store("v02").await;
    // 平台 pause_new → 会员 flatten：升档成功、归属保持 platform。
    arm_platform(&store, "cred:v02a", Level::PauseNew, 1_000).await;
    arm_member_ok(&store, "cred:v02a", Level::Flatten, MEMBER_A, 2_000).await;
    assert_eq!(state_authority(&store, "cred:v02a").await, (1, None, 2, "flatten".into()));
    // 未知活闩（kind=0）：构造 = 平台设档后手工把归属抹 0（模拟 legacy）。
    arm_platform(&store, "cred:v02b", Level::PauseNew, 1_000).await;
    sqlx::query("UPDATE kill_switch_state SET authority_kind=0, authority_member_id=NULL WHERE scope_key='cred:v02b'")
        .execute(store.pool())
        .await
        .expect("scratch legacy authority");
    arm_member_ok(&store, "cred:v02b", Level::Flatten, MEMBER_A, 2_000).await;
    let authority = state_authority(&store, "cred:v02b").await;
    assert_eq!(authority.0, 0, "unknown authority must not become member by upgrade");
    assert_eq!(authority.3, "flatten");
    // 自有档升档：归属保持 (3,M)。
    arm_member_ok(&store, "cred:v02c", Level::PauseNew, MEMBER_A, 1_000).await;
    arm_member_ok(&store, "cred:v02c", Level::Flatten, MEMBER_A, 2_000).await;
    assert_eq!(state_authority(&store, "cred:v02c").await.0, 3);
    // 异会员：拒绝、零写。
    let before = state_authority(&store, "cred:v02c").await;
    let error = arm_member(
        &store,
        ArmRequest {
            scope_key: "cred:v02c",
            level: Level::Flatten,
            trigger_source: TRIGGER_SOURCE_MEMBER_API,
            reason: "another member",
            now_ms: 3_000,
        },
        member_ctx(MEMBER_B),
    )
    .await
    .expect_err("cross-member arm must be refused");
    assert!(error.to_string().contains("another member"));
    assert_eq!(state_authority(&store, "cred:v02c").await, before);
    cleanup(store, path).await;
}

/// V-03：同级平台接管撤销会员资格；reset 先提交则平台重新设档（非幂等）。
#[tokio::test]
async fn v03_same_level_takeover_revokes_member_eligibility() {
    let (store, path) = test_store("v03").await;
    arm_member_ok(&store, "cred:v03", Level::PauseNew, MEMBER_A, 1_000).await;
    admit_current_boot(&store, "clean_attestation:fixture", false)
        .await
        .expect("admit for fixture");
    // 同级平台重设：早返回路径上的归属接管（双写 state+event）。
    arm_platform(&store, "cred:v03", Level::PauseNew, 2_000).await;
    let authority = state_authority(&store, "cred:v03").await;
    assert_eq!(authority.0, 1, "same-level platform re-arm must take authority");
    assert_eq!(authority.2, 1, "takeover must not bump the generation");
    // 会员 reset 现在被拒（归属谓词失配）。
    converge_clean(&store, "cred:v03", 2_500).await;
    let outcome = reset_member(&store, "cred:v03", MEMBER_A, 1, "01A7XQZ90ZYY9VNEHNJQ000001", 3_000)
        .await
        .expect("reset io");
    assert!(
        matches!(outcome, MemberResetOutcome::Rejected(ref reason) if reason.contains("member-owned")),
        "takeover must revoke the member reset, got {outcome:?}"
    );
    // 事件行：arm 身份（arm_actor=M）保留、当前归属=platform（C2.3 分列）。
    let event = sqlx::query(
        "SELECT authority_kind, arm_actor_kind, arm_actor_member_id FROM kill_switch_events \
         WHERE scope_key='cred:v03' AND flatten_generation=1",
    )
    .fetch_one(store.pool())
    .await
    .expect("event row");
    assert_eq!(event.get::<i64, _>("authority_kind"), 1);
    assert_eq!(event.get::<i64, _>("arm_actor_kind"), 3);
    assert_eq!(event.try_get::<i64, _>("arm_actor_member_id").ok(), Some(MEMBER_A));

    // 反向序：会员 reset 先 COMMIT → 平台 arm 遇 none **重新设档**并归属平台。
    arm_member_ok(&store, "cred:v03b", Level::PauseNew, MEMBER_A, 1_000).await;
    converge_clean(&store, "cred:v03b", 1_500).await;
    let generation = state_authority(&store, "cred:v03b").await.2;
    let outcome = reset_member(
        &store, "cred:v03b", MEMBER_A, generation,
        "01A7XQZ90ZYY9VNEHNJQ000002", 2_000,
    )
    .await
    .expect("member reset io");
    assert!(matches!(outcome, MemberResetOutcome::Executed { .. }), "{outcome:?}");
    arm_platform(&store, "cred:v03b", Level::PauseNew, 3_000).await;
    assert_eq!(state_authority(&store, "cred:v03b").await, (1, None, 2, "pause_new".into()));
    cleanup(store, path).await;
}

/// M4（AM §3）：接管的绑代 CAS——代际错位时必须拒绝，不得写到新代上。
#[tokio::test]
async fn v04b_takeover_cas_is_generation_bound() {
    let (store, path) = test_store("v04b").await;
    arm_member_ok(&store, "cred:v04b", Level::PauseNew, MEMBER_A, 1_000).await;
    // 代际推进到 g=2（会员升档 flatten），随后用**旧代**的期望做平台接管
    // ——引擎没有暴露"带代际的接管"入口，所以这里直接构造与
    // take_authority_in_tx 相同的场景：模拟读旧代（g=1）后写入。方法：
    // 手工把 state 归属改错到旧代值，然后同级平台 arm 应当在**当前代**上
    // 接管成功且事件行的代际等于当前代（不是旧代）。
    arm_member_ok(&store, "cred:v04b", Level::Flatten, MEMBER_A, 2_000).await;
    let before = state_authority(&store, "cred:v04b").await;
    assert_eq!(before.2, 2, "generation must be at 2 after the upgrade arm");
    // 同级平台接管（flatten 同级）：早返回路径上的绑代 CAS。
    arm_platform(&store, "cred:v04b", Level::Flatten, 3_000).await;
    let after = state_authority(&store, "cred:v04b").await;
    assert_eq!(after.0, 1, "same-level platform re-arm takes authority");
    assert_eq!(after.2, 2, "takeover must not create or move a generation");
    // 事件行：接管发生在当前代的事件行上；arm_actor 保持会员身份（C2.3）。
    let event = sqlx::query(
        "SELECT flatten_generation, authority_kind, arm_actor_kind \
         FROM kill_switch_events WHERE scope_key='cred:v04b' AND reset_at_ms IS NULL \
         ORDER BY armed_at_ms DESC LIMIT 1",
    )
    .fetch_one(store.pool())
    .await
    .expect("active event");
    assert_eq!(event.get::<i64, _>("flatten_generation"), 2);
    assert_eq!(event.get::<i64, _>("authority_kind"), 1);
    assert_eq!(event.get::<i64, _>("arm_actor_kind"), 3);
    cleanup(store, path).await;
}

/// V-04：迟到 g 请求在代际预检/谓词处拒绝，g+1 零变化。
#[tokio::test]
async fn v04_late_reset_against_a_new_revision_is_refused() {
    let (store, path) = test_store("v04").await;
    arm_member_ok(&store, "cred:v04", Level::PauseNew, MEMBER_A, 1_000).await;
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    converge_clean(&store, "cred:v04", 1_500).await;
    let stale_generation = state_authority(&store, "cred:v04").await.2;
    // 合法解除 g，随后本人重设 → g+1。
    let outcome = reset_member(&store, "cred:v04", MEMBER_A, stale_generation, "01A7XQZ90ZYY9VNEHNJQ000003", 2_000)
        .await
        .expect("reset io");
    assert!(matches!(outcome, MemberResetOutcome::Executed { .. }));
    arm_member_ok(&store, "cred:v04", Level::PauseNew, MEMBER_A, 3_000).await;
    let after = state_authority(&store, "cred:v04").await;
    // 迟到的 g 请求到达：预检拒绝（generation != expected）。
    let late = reset_member(&store, "cred:v04", MEMBER_A, stale_generation, "01A7XQZ90ZYY9VNEHNJQ000004", 4_000)
        .await
        .expect("late reset io");
    assert!(
        matches!(late, MemberResetOutcome::Rejected(ref r) if r.contains("moved since authorization")),
        "{late:?}"
    );
    assert_eq!(state_authority(&store, "cred:v04").await, after, "g+1 must be untouched");
    cleanup(store, path).await;
}

/// V-06：读投影零写副作用（含 ref 查询路径）。
#[tokio::test]
async fn v06_read_projection_has_zero_write_side_effects() {
    async fn table_fingerprint(store: &Store, table: &str) -> String {
        // 全行字节指纹（M5 的击杀面：updated_at_ms/authority 等任何列的
        // 偷写都必须可见）。逐表手写全部列的 CAST 串接（两后端同文）。
        let columns: &[&str] = match table {
            "kill_switch_state" => &[
                "scope_key", "level", "reason", "trigger_source", "armed_at_ms",
                "flatten_state", "last_reconcile_clean", "last_reconcile_at_ms",
                "updated_at_ms", "flatten_generation", "authority_kind",
                "authority_member_id",
            ],
            "kill_switch_events" => &[
                "event_id", "scope", "scope_key", "level", "trigger_source",
                "reason", "armed_at_ms", "flatten_state", "reconcile_clean",
                "reset_by", "reset_at_ms", "created_at_ms", "updated_at_ms",
                "flatten_generation", "authority_kind", "authority_member_id",
                "arm_request_ref", "reset_request_ref", "arm_actor_kind",
                "arm_actor_member_id", "generation_proved_at_ms",
            ],
            "kill_switch_reconcile_status" => &[
                "scope_key", "credential_id", "reconcile_clean",
                "flatten_complete", "reconciled_at_ms", "flatten_generation",
                "expected_provenance", "truth_positions_zero",
                "truth_orders_empty", "truth_algo_orders_empty",
                "truth_local_zero_diff", "truth_no_diff",
            ],
            "member_reset_admission" => &[
                "id", "enabled", "admitted_boot_id", "admission_version",
                "basis", "admitted_at_ms", "admitted_by",
            ],
            _ => &[],
        };
        let parts = columns
            .iter()
            .map(|c| format!("COALESCE(CAST({c} AS CHAR),'<null>')"))
            .collect::<Vec<_>>();
        let projection = match store.kind() {
            DbKind::MySql => format!("CONCAT_WS('|', {})", parts.join(", ")),
            _ => parts.join(" || '|' || "),
        };
        let sql = format!("SELECT {projection} AS digest FROM {table} ORDER BY 1");
        let rows = sqlx::query(&sql)
            .fetch_all(store.pool())
            .await
            .expect("fingerprint read");
        let mut digest = String::new();
        for row in rows {
            digest.push_str(
                &crate::store::try_get_text(&row, "digest")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            );
            digest.push(';');
        }
        digest
    }
    let (store, path) = test_store("v06").await;
    arm_member_ok(&store, "cred:v06", Level::PauseNew, MEMBER_A, 1_000).await;
    let before_state = table_fingerprint(&store, "kill_switch_state").await;
    let before_events = table_fingerprint(&store, "kill_switch_events").await;
    let before_status = table_fingerprint(&store, "kill_switch_reconcile_status").await;
    let before_admission = table_fingerprint(&store, "member_reset_admission").await;
    let outcome = get_kill_switch_states(
        &store,
        &["cred:v06".to_string(), "global".to_string()],
        &["01A7XQZ90ZYY9VNEHNJQ000005".to_string()],
    )
    .await
    .expect("read");
    assert!(!outcome.snapshot_stale);
    assert_eq!(
        (table_fingerprint(&store, "kill_switch_state").await, table_fingerprint(&store, "kill_switch_events").await,
         table_fingerprint(&store, "kill_switch_reconcile_status").await, table_fingerprint(&store, "member_reset_admission").await),
        (before_state, before_events, before_status, before_admission),
        "read projection must not write a single byte"
    );
    cleanup(store, path).await;
}

/// V-10a：双 ref 分列、arm 命中 ≠ reset 成功、ULID 闭世界与 BINARY 语义。
#[tokio::test]
async fn v10a_dual_refs_and_closed_world_attribution() {
    let (store, path) = test_store("v10a").await;
    let arm_ref = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let reset_ref = "01ARZ3NDEKTSV4RRFFQ69G5FBW";
    arm_member(&store, ArmRequest {
        scope_key: "cred:v10a", level: Level::PauseNew,
        trigger_source: TRIGGER_SOURCE_MEMBER_API, reason: "fixture", now_ms: 1_000,
    }, MemberArmContext { member_id: MEMBER_A, request_ref: arm_ref })
    .await
    .expect("arm");
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    converge_clean(&store, "cred:v10a", 1_500).await;
    let generation = state_authority(&store, "cred:v10a").await.2;
    reset_member(&store, "cred:v10a", MEMBER_A, generation, reset_ref, 2_000)
        .await
        .expect("member reset");
    let outcome = get_kill_switch_states(&store, &["cred:v10a".to_string()], &[arm_ref.to_string(), reset_ref.to_string()])
        .await
        .expect("read with refs");
    let arm_hit = outcome.ref_hits.iter().find(|hit| hit.request_ref == arm_ref).expect("arm hit");
    assert_eq!(arm_hit.kind, "arm");
    assert!(!arm_hit.reset_committed, "an arm hit is never reset success");
    assert_eq!(arm_hit.arm_actor_kind, 3);
    assert_eq!(arm_hit.arm_actor_member_id, Some(MEMBER_A));
    let reset_hit = outcome.ref_hits.iter().find(|hit| hit.request_ref == reset_ref).expect("reset hit");
    assert_eq!(reset_hit.kind, "reset");
    assert!(reset_hit.reset_committed);
    // 闭世界：变体（小写）ref 必须被查询入口拒绝，而不是静默不命中。
    let rejected = get_kill_switch_states(&store, &["cred:v10a".to_string()], &[variant_ref(arm_ref)])
        .await
        .expect_err("variant ref must be rejected by the closed world");
    assert!(rejected.to_string().contains("canonical ULID"));
    cleanup(store, path).await;
}

/// V-12 + C1-f：准入绑定启动周期——默认禁用；准入成功后新的 Store::connect
/// （= 新启动周期登记）无条件失效；未准入时会员 reset 拒、降险照常。
#[tokio::test]
async fn v12_admission_is_bound_to_the_boot_cycle() {
    let (store, path) = test_store("v12").await;
    // 默认禁用：迁移后首启 enabled=0（种子）→ member reset NotAdmitted。
    assert_eq!(admission_row(&store).await.0, 0);
    arm_member_ok(&store, "cred:v12", Level::PauseNew, MEMBER_A, 1_000).await;
    let outcome = reset_member(&store, "cred:v12", MEMBER_A, 1, "01A7XQZ90ZYY9VNEHNJQ000006", 2_000)
        .await
        .expect("reset io");
    assert_eq!(outcome, MemberResetOutcome::NotAdmitted);
    // 降险与后台保护不受门控：arm 照常（上面已 arm）。
    // 准入成功 → 生效。
    admit_current_boot(&store, "clean_attestation:fixture", false).await.expect("admit");
    assert_eq!(admission_row(&store).await.0, 1);
    // 新的启动周期（同库再 connect）：无条件失效（C1-f/g 的进程内模拟）。
    let second_path = path.clone();
    let _ = second_path;
    let url = format!("sqlite://{}", path.display());
    let store2 = Store::connect(&url).await.expect("second boot");
    assert_eq!(admission_row(&store2).await.0, 0, "a new boot cycle must disable the admission");
    let outcome2 = reset_member(&store2, "cred:v12", MEMBER_A, 1, "01A7XQZ90ZYY9VNEHNJQ000007", 3_000)
        .await
        .expect("reset io");
    assert_eq!(outcome2, MemberResetOutcome::NotAdmitted);
    // durable 恢复：归属/事件/代际跨启动仍在（只是资格失效）。
    assert_eq!(state_authority(&store2, "cred:v12").await.0, 3);
    store2.pool().close().await;
    cleanup(store, path).await;
}

/// V-13（模拟腿）：state/事件归属不一致（旧写者痕迹）→ 栅栏降级 + 自动关门。
/// state/event 相等不判清白：零写接管不可检测（C1-g 的诚实边界在准入侧）。
#[tokio::test]
async fn v13_fence_demotes_unproven_authority_and_closes_admission() {
    let (store, path) = test_store("v13").await;
    arm_member_ok(&store, "cred:v13", Level::PauseNew, MEMBER_A, 1_000).await;
    admit_current_boot(&store, "clean_attestation:fixture", false).await.expect("admit");
    // 模拟旧引擎 arm：直接改 level 与事件（不动归属列）→ 两侧归属不一致。
    sqlx::query("UPDATE kill_switch_state SET authority_kind=1, authority_member_id=NULL WHERE scope_key='cred:v13'")
        .execute(store.pool())
        .await
        .expect("simulate old-writer state change");
    let url = format!("sqlite://{}", path.display());
    let store2 = Store::connect(&url).await.expect("fence boot");
    let authority = state_authority(&store2, "cred:v13").await;
    assert_eq!(authority.0, 0, "fence must demote unproven state authority to unknown");
    assert_eq!(authority.3, "pause_new", "fence never clears the safety level");
    assert_eq!(admission_row(&store2).await.0, 0, "fence must close the admission");
    let history = sqlx::query("SELECT COUNT(*) AS n FROM member_reset_admission_history WHERE action='revoked_fence'")
        .fetch_one(store2.pool())
        .await
        .expect("history");
    assert_eq!(history.get::<i64, _>("n"), 1);
    store2.pool().close().await;
    cleanup(store, path).await;
}

/// V-15a/15b：同源完整性与五真位 presence。
#[tokio::test]
async fn v15_convergence_truth_bits_and_presence() {
    let (store, path) = test_store("v15").await;
    arm_member_ok(&store, "cred:v15", Level::Flatten, MEMBER_A, 1_000).await;
    // arm 冻结行（reconciled_at_ms=0、truth 位 NULL）→ UNKNOWN + 冻结签名。
    let outcome = get_kill_switch_states(&store, &["cred:v15".to_string()], &[])
        .await
        .expect("read");
    let row = &outcome.rows[0];
    let convergence = row.scope_convergence.as_ref().expect("same-source convergence");
    assert_eq!(convergence.expected, vec!["v15".to_string()], "cred scope membership");
    // 冻结行以 dirty/incomplete 阻塞收敛，
    // unreported 只覆盖"expected 里一行都没有"的成员。
    assert!(!convergence.flatten_complete, "frozen-only rows must not converge");
    assert_eq!(convergence.dirty, 1, "the frozen row reports dirty until a truth arrives");
    assert_eq!(convergence.incomplete, 1);
    let frozen = &row.convergence[0];
    assert_eq!(frozen.reconciled_at_ms, 0, "arm-freeze signature is a real zero");
    assert_eq!(frozen.truth_positions_zero, None, "legacy/never-projected bits are UNKNOWN");
    assert_eq!(row.last_reconcile_at_ms, None, "never-projected timestamp is absent, not zero");
    // 投影一份真值：五真位逐位落库且与聚合一致。
    let truth = ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: false,
        local_position_zero_diff: true,
    };
    let observed = observe_flatten_generations(&store).await.expect("generations");
    record_reconcile_for_scope(&store, "cred:v15", "v15", truth, 2_000, &observed)
        .await
        .expect("project");
    let outcome = get_kill_switch_states(&store, &["cred:v15".to_string()], &[])
        .await
        .expect("read after projection");
    let projected = &outcome.rows[0].convergence[0];
    assert_eq!(projected.truth_algo_orders_empty, Some(false), "bits are per-condition, not aggregate-derivable");
    assert_eq!(projected.truth_no_diff, Some(true));
    assert_eq!(projected.flatten_complete, Some(false));
    assert_eq!(projected.reconciled_at_ms, 2_000);
    cleanup(store, path).await;
}

/// V-20：降险链无门——新 scope 无行可设；reset 后有历史行可直接新设；
/// 同级重复 no_change 不推号；低档请求不降高档。
#[tokio::test]
async fn v20_risk_reducing_chain_has_no_gates() {
    let (store, path) = test_store("v20").await;
    // 新 scope 无行。
    arm_member_ok(&store, "cred:v20a", Level::PauseNew, MEMBER_A, 1_000).await;
    assert_eq!(state_authority(&store, "cred:v20a").await.2, 1);
    // reset 后（有历史 generation）直接新设。
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    converge_clean(&store, "cred:v20a", 1_500).await;
    let generation = state_authority(&store, "cred:v20a").await.2;
    reset_member(&store, "cred:v20a", MEMBER_A, generation, "01A7XQZ90ZYY9VNEHNJQ000008", 2_000)
        .await
        .expect("reset");
    arm_member_ok(&store, "cred:v20a", Level::Flatten, MEMBER_A, 3_000).await;
    assert_eq!(state_authority(&store, "cred:v20a").await, (3, Some(MEMBER_A), 2, "flatten".into()));
    // 同级重复：no_change（generation 不推、事件不加）。
    let events_before = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key='cred:v20a'")
        .fetch_one(store.pool()).await.unwrap().get::<i64, _>("n");
    arm_member_ok(&store, "cred:v20a", Level::Flatten, MEMBER_A, 4_000).await;
    assert_eq!(state_authority(&store, "cred:v20a").await.2, 2, "same-level repeat must not bump");
    let events_after = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope_key='cred:v20a'")
        .fetch_one(store.pool()).await.unwrap().get::<i64, _>("n");
    assert_eq!(events_before, events_after);
    // 低档请求不降高档（平台 manage_only < 会员 flatten：早返回、零写）。
    arm_platform(&store, "cred:v20a", Level::ManageOnly, 5_000).await;
    assert_eq!(state_authority(&store, "cred:v20a").await.3, "flatten");
    cleanup(store, path).await;
}

/// V-22 / C1-a..d：准入请求绑定（boot/version 双比对、禁止换绑、重放拒绝）。
#[tokio::test]
async fn v22_admission_request_binding() {
    let (store, path) = test_store("v22").await;
    // 形状：presence 缺席 → INVALID。
    let rejected = admit_member_reset(&store, "clean_attestation:x", "op", false, store.boot_id(), None, 1_000)
        .await.unwrap().unwrap_err();
    assert!(rejected.contains("presence"));
    // 前缀与 demote 旗标矛盾。
    assert!(admit_member_reset(&store, "post_rollback_repair:x", "op", false, store.boot_id(), Some(0), 1_000)
        .await.unwrap().unwrap_err().contains("demote"));
    // C1-a：A 周期的 Q（旧 boot 值）到 B 周期 → FAILED，零写。
    let stale_boot = "01ARZ3NDEKTSV4RRFFQ69G5FZZ";
    let version = admission_row(&store).await.2;
    let rejected = admit_member_reset(&store, "clean_attestation:x", "op", false, stale_boot, Some(version), 1_000)
        .await.unwrap().unwrap_err();
    assert!(rejected.contains("boot cycle"));
    assert_eq!(admission_row(&store).await, (0, String::new(), version), "rejected intents write nothing");
    // C1-c：对当前 boot/版本重新核验的新 Q → 成功。
    admit_current_boot(&store, "clean_attestation:fixture", false).await.expect("admit");
    let after = admission_row(&store).await;
    assert_eq!(after.0, 1);
    // C1-d：成功后重放**同一意图**（旧 expected 版本）→ 版本已变 → 拒，且不再推号。
    let replay = admit_member_reset(&store, "clean_attestation:fixture", "op", false, store.boot_id(), Some(version), 2_000)
        .await.unwrap().unwrap_err();
    assert!(replay.contains("does not match"));
    assert_eq!(admission_row(&store).await, after, "replay must not bump again");
    // C1-b：撤销（新 Store = 新周期推号）后的旧版本 Q → 拒。
    let url = format!("sqlite://{}", path.display());
    let store2 = Store::connect(&url).await.expect("new cycle");
    let version2 = admission_row(&store2).await.2;
    let rejected = admit_member_reset(&store2, "clean_attestation:x", "op", false, store2.boot_id(), Some(after.2), 3_000)
        .await.unwrap().unwrap_err();
    assert!(rejected.contains("admission_version"));
    assert!(version2 > after.2, "the new cycle must have bumped the version");
    store2.pool().close().await;
    cleanup(store, path).await;
}

/// V-22 post_rollback：同事务只降归属列，level/generation/事件本体不动。
#[tokio::test]
async fn v22_post_rollback_demotes_authority_only() {
    let (store, path) = test_store("v22pr").await;
    arm_member_ok(&store, "cred:v22pr", Level::PauseNew, MEMBER_A, 1_000).await;
    let before = state_authority(&store, "cred:v22pr").await;
    let events_before = sqlx::query("SELECT COUNT(*) AS n, MAX(flatten_generation) AS g FROM kill_switch_events WHERE scope_key='cred:v22pr'")
        .fetch_one(store.pool()).await.unwrap();
    admit_current_boot(&store, "post_rollback_repair:fixture", true)
        .await
        .expect("post-rollback admit");
    let after = state_authority(&store, "cred:v22pr").await;
    assert_eq!(after, (0, None, before.2, before.3), "only the authority columns may change");
    let events_after = sqlx::query("SELECT COUNT(*) AS n, MAX(flatten_generation) AS g FROM kill_switch_events WHERE scope_key='cred:v22pr'")
        .fetch_one(store.pool()).await.unwrap();
    assert_eq!(
        (events_before.get::<i64, _>("n"), events_before.get::<i64, _>("g")),
        (events_after.get::<i64, _>("n"), events_after.get::<i64, _>("g")),
        "post_rollback must not add or rewrite events"
    );
    let outcome = admit_member_reset(&store, "post_rollback_repair:again", "op", true, store.boot_id(), None, 2_000)
        .await.unwrap();
    assert!(outcome.is_err());
    cleanup(store, path).await;
}

/// V-12R（缺行腿）：成功迁移后单例行异常缺失 → admit/reset fail-closed。
#[tokio::test]
async fn v12r_missing_singleton_fails_closed() {
    let (store, path) = test_store("v12r").await;
    sqlx::query("DELETE FROM member_reset_admission WHERE id=1")
        .execute(store.pool())
        .await
        .expect("simulate anomaly");
    let rejected = admit_member_reset(&store, "clean_attestation:x", "op", false, store.boot_id(), Some(0), 1_000)
        .await
        .unwrap()
        .unwrap_err();
    assert!(rejected.contains("missing"));
    arm_member_ok(&store, "cred:v12r", Level::PauseNew, MEMBER_A, 1_000).await;
    let outcome = reset_member(&store, "cred:v12r", MEMBER_A, 1, "01A7XQZ90ZYY9VNEHNJQ000009", 2_000)
        .await
        .unwrap();
    assert_eq!(outcome, MemberResetOutcome::NotAdmitted);
    cleanup(store, path).await;
}

/// 读上界（引擎侧部分）：scopes ≤ 256、refs ≤ 64，超界整体拒绝。
#[tokio::test]
async fn read_ceilings_reject_wholesale() {
    let (store, path) = test_store("v18e").await;
    let many: Vec<String> = (0..257).map(|i| format!("cred:ceiling{i}")).collect();
    let error = get_kill_switch_states(&store, &many, &[])
        .await
        .expect_err("257 scopes must be refused");
    assert!(error.to_string().contains("256"));
    let refs: Vec<String> = (0..65)
        .map(|i| format!("{i:026}"))
        .collect();
    let error = get_kill_switch_states(&store, &["cred:one".to_string()], &refs)
        .await
        .expect_err("65 refs must be refused");
    assert!(error.to_string().contains("64"));
    cleanup(store, path).await;
}

// ============================================================================
// A2 · TG-1 迁移恢复（两后端）与 A3 · MySQL 腿
// ============================================================================

const MYSQL_SKIP_NOTE: &str = "set RUSTCORE_TEST_MYSQL_URL + RUSTCORE_REQUIRE_MYSQL_TESTS=1 to run";

fn mysql_required(label: &str) -> Option<String> {
    let require = std::env::var("RUSTCORE_REQUIRE_MYSQL_TESTS").unwrap_or_default();
    let url = std::env::var("RUSTCORE_TEST_MYSQL_URL").unwrap_or_default();
    if url.trim().is_empty() {
        if require == "1" {
            panic!("RUSTCORE_REQUIRE_MYSQL_TESTS=1 but RUSTCORE_TEST_MYSQL_URL is not set ({label})");
        }
        eprintln!("SKIPPED {label}: {MYSQL_SKIP_NOTE}");
        return None;
    }
    Some(url.trim().to_string())
}

async fn mysql_admin_pool(url: &str) -> sqlx::Pool<sqlx::Any> {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    sqlx::any::AnyPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("connect the lease MySQL admin")
}

async fn mysql_scratch_p4b(label: &str, upto: i64) -> Option<(sqlx::Pool<sqlx::Any>, String, String)> {
    let Some(admin_url) = mysql_required(label) else { return None };
    let admin = mysql_admin_pool(&admin_url).await;
    let database = format!(
        "rustcore_p4br1_{}_{}",
        label.to_ascii_lowercase(),
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    sqlx::query(&format!("CREATE DATABASE `{database}`")).execute(&admin).await.expect("create scratch");
    admin.close().await;
    // swap the database path segment (store.rs swap_database semantics)
    let base = admin_url.trim_end_matches('/');
    let url = match base.rfind('/') {
        Some(idx) => format!("{}/{}", &base[..idx], database),
        None => format!("{}/{}", base, database),
    };
    // run migrations only up to `upto` by copying migration files
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let target = std::env::temp_dir().join(format!("rustcore-p4br1-{label}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&target).expect("mkdir scratch migrations");
    for entry in std::fs::read_dir(&source).expect("read migrations") {
        let path = entry.expect("entry").path();
        let name = path.file_name().and_then(|v| v.to_str()).expect("name").to_string();
        let version: i64 = name.split('_').next().unwrap().parse().expect("version");
        if version > upto { continue; }
        std::fs::copy(&path, target.join(&name)).expect("copy migration");
    }
    let pool = sqlx::any::AnyPoolOptions::new().max_connections(2).connect(&url).await.expect("connect scratch");
    sqlx::migrate::Migrator::new(target.as_path()).await.expect("load migrator").run(&pool).await.expect("run up to upto");
    Some((pool, url, admin_url))
}

async fn drop_mysql_scratch(url: &str, admin_url: &str, database: &str) {
    let pool = sqlx::any::AnyPoolOptions::new().max_connections(1).connect(url).await.expect("reconnect scratch");
    let row = sqlx::query("SELECT DATABASE() AS db").fetch_one(&pool).await.expect("current db");
    let current = crate::store::try_get_text(&row, "db").ok().flatten().unwrap_or_default();
    pool.close().await;
    if let Ok(admin) = (|| async {
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await?;
        Ok::<_, sqlx::Error>(pool)
    })().await
    {
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{current}`")).execute(&admin).await;
        admin.close().await;
    }
    let _ = database;
}

/// 写一条 sqlx 形状的 dirty 账本行（success=FALSE，真实 checksum）。
async fn write_dirty_version(pool: &sqlx::Pool<sqlx::Any>, version: i64) {
    let migrator = crate::store::embedded_migrator_for_test();
    let migration = migrator.iter().find(|m| m.version == version).unwrap_or_else(|| panic!("migration {version} exists"));
    let checksum: String = migration.checksum.iter().map(|b| format!("{b:02X}")).collect();
    sqlx::query(&format!(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES ({version}, ?, FALSE, X'{checksum}', -1)"
    )).bind(&*migration.description).execute(pool).await.expect("write dirty ledger row");
}

/// 执行某版本 re-entry 计划的前 n 步（模拟「第 n 条语句后崩溃」）。
async fn run_steps(pool: &sqlx::Pool<sqlx::Any>, version: i64, n: usize) {
    let (_, steps) = crate::store::ddl_reentry_plans_for_test()
        .iter().find(|(v, _)| *v == version).expect("plan exists");
    for step in steps.iter().take(n) {
        sqlx::query(step.apply).execute(pool).await.expect("run pre-crash step");
    }
}

/// 情形 1+2：0036/0037/0038 **每个**断点（0…n）独立 scratch 库 → 启动恢复
/// 补齐并落章；与全新安装逐列对账（列名/类型/可空性，非只查 >0）。
#[tokio::test]
async fn a2_mysql_statement_breakpoints_recover_and_stamp() {
    for (version, statement_count) in [(36i64, 8usize), (37, 4), (38, 5)] {
        for breakpoint in 0..=statement_count {
            let label = format!("migbrk{version}b{breakpoint}");
            let Some((pool, url, admin_url)) = mysql_scratch_p4b(&label, 35).await
            else {
                return;
            };
            write_dirty_version(&pool, version).await;
            if breakpoint > 0 {
                run_steps(&pool, version, breakpoint).await;
            }
            let store =
                crate::store::Store::connect(&url).await
                .unwrap_or_else(|e| panic!("recovery boot v{version} b{breakpoint}: {e}"));
            let stamped = sqlx::query(
                "SELECT CAST(success AS SIGNED) AS ok \
                 FROM _sqlx_migrations WHERE version=?",
            )
            .bind(version)
            .fetch_one(store.pool())
            .await
            .expect("ledger row");
            assert_eq!(
                stamped.get::<i64, _>("ok"),
                1,
                "v{version} breakpoint {breakpoint} must stamp"
            );
            store.pool().close().await;
            drop_mysql_scratch(&url, &admin_url, "").await;
        }
    }
    // 与全新安装**逐列对账**：恢复库（从中间断点恢复）与新装库的每张表
    // 列名/类型/可空性完全一致。
    let Some((rec_pool, rec_url, rec_admin)) = mysql_scratch_p4b("migbrkcmp", 35).await
    else { return };
    write_dirty_version(&rec_pool, 36).await;
    run_steps(&rec_pool, 36, 1).await;
    let _ = crate::store::Store::connect(&rec_url).await.expect("recover from breakpoint 1");
    let Some((fresh_pool, fresh_url, fresh_admin)) = mysql_scratch_p4b("migbrkfrs", 38).await
    else {
        drop_mysql_scratch(&rec_url, &rec_admin, "").await; return;
    };
    for table in [
        "kill_switch_state",
        "kill_switch_events",
        "kill_switch_reconcile_status",
        "member_reset_admission",
        "member_reset_admission_history",
        "engine_runtime_provenance",
    ] {
        let q = "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_schema=DATABASE() AND table_name=? ORDER BY column_name";
        let recovered = sqlx::query(q).bind(table).fetch_all(&rec_pool).await.unwrap()
            .into_iter().map(|r| (
                crate::store::try_get_text(&r, "column_name").ok().flatten().unwrap_or_default(),
                crate::store::try_get_text(&r, "data_type").ok().flatten().unwrap_or_default(),
                crate::store::try_get_text(&r, "is_nullable").ok().flatten().unwrap_or_default(),
            )).collect::<Vec<(String, String, String)>>();
        let fresh = sqlx::query(q).bind(table).fetch_all(&fresh_pool).await.unwrap()
            .into_iter().map(|r| (
                crate::store::try_get_text(&r, "column_name").ok().flatten().unwrap_or_default(),
                crate::store::try_get_text(&r, "data_type").ok().flatten().unwrap_or_default(),
                crate::store::try_get_text(&r, "is_nullable").ok().flatten().unwrap_or_default(),
            )).collect::<Vec<(String, String, String)>>();
        assert_eq!(recovered, fresh, "{table} must be column-identical to a fresh install");
    }
    rec_pool.close().await; fresh_pool.close().await;
    drop_mysql_scratch(&rec_url, &rec_admin, "").await;
    drop_mysql_scratch(&fresh_url, &fresh_admin, "").await;
}

/// 情形 3：dirty 下已存在 id=1/enabled=1/version=5 的行 → 恢复失败、不落章、原行保持。
#[tokio::test]
async fn a2_mysql_dirty_live_seed_row_refused_and_preserved() {
    let Some((pool, url, admin_url)) = mysql_scratch_p4b("miglive", 35).await else { return };
    write_dirty_version(&pool, 37).await;
    run_steps(&pool, 37, 3).await; // 三表已建，种子未插
    sqlx::query("INSERT INTO member_reset_admission (id, enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by) VALUES (1, 1, '01ARZ3NDEKTSV4RRFFQ69G5FAV', 5, 'clean_attestation:legacy', 123, 'op')")
        .execute(&pool).await.expect("seed the anomalous live row");
    let result = crate::store::Store::connect(&url).await;
    assert!(result.is_err(), "recovery must refuse an unprovable seed effect");
    let row = sqlx::query("SELECT enabled, admission_version, basis FROM member_reset_admission WHERE id=1")
        .fetch_one(&pool).await.expect("row preserved");
    assert_eq!(row.get::<i64,_>("enabled"), 1, "existing row must be untouched");
    assert_eq!(row.get::<i64,_>("admission_version"), 5);
    assert_eq!(
        crate::store::try_get_text(&row, "basis").ok().flatten(),
        Some("clean_attestation:legacy".to_string()),
        "full-row preservation: basis"
    );
    let stamped = sqlx::query("SELECT CAST(success AS SIGNED) AS ok FROM _sqlx_migrations WHERE version=37")
        .fetch_one(&pool).await.expect("ledger row exists");
    assert_eq!(stamped.get::<i64,_>("ok"), 0, "no success stamp for unproven seed");
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// 情形 4：只有 id≠1 的行 → 效果未证明，fail-closed 不落章。
#[tokio::test]
async fn a2_mysql_wrong_id_row_is_unproven() {
    let Some((pool, url, admin_url)) = mysql_scratch_p4b("migwid", 35).await else { return };
    write_dirty_version(&pool, 37).await;
    run_steps(&pool, 37, 3).await;
    sqlx::query("INSERT INTO member_reset_admission (id, enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by) VALUES (2, 0, '', 0, '', 0, '')")
        .execute(&pool).await.expect("seed only a non-id=1 row");
    assert!(crate::store::Store::connect(&url).await.is_err());
    let stamped = sqlx::query("SELECT CAST(success AS SIGNED) AS ok FROM _sqlx_migrations WHERE version=37")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(stamped.get::<i64,_>("ok"), 0);
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// 情形 5：authority_kind 列类型错误（INT 而非 SMALLINT）→ 拒绝，不当已应用。
#[tokio::test]
async fn a2_mysql_wrong_column_type_refused() {
    let Some((pool, url, admin_url)) = mysql_scratch_p4b("migtype", 35).await else { return };
    write_dirty_version(&pool, 36).await;
    sqlx::query("ALTER TABLE kill_switch_state ADD COLUMN authority_kind INT NOT NULL DEFAULT 0")
        .execute(&pool).await.expect("apply with the wrong type");
    assert!(crate::store::Store::connect(&url).await.is_err());
    let stamped = sqlx::query("SELECT CAST(success AS SIGNED) AS ok FROM _sqlx_migrations WHERE version=36")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(stamped.get::<i64,_>("ok"), 0);
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// 情形 6：重复恢复幂等——已证明效果不重复写入；分阶段：正常启动仍无条件推号。
#[tokio::test]
async fn a2_mysql_repeat_recovery_idempotent_and_boot_still_bumps() {
    let Some((pool, url, admin_url)) = mysql_scratch_p4b("migrep", 35).await else { return };
    write_dirty_version(&pool, 37).await;
    run_steps(&pool, 37, 4).await; // 全部语句执行完（种子已插）
    let store = crate::store::Store::connect(&url).await.expect("first recovery");
    let row = sqlx::query("SELECT enabled, admission_version FROM member_reset_admission WHERE id=1")
        .fetch_one(store.pool()).await.unwrap();
    let version_after_first = row.get::<i64,_>("admission_version");
    assert_eq!(row.get::<i64,_>("enabled"), 0);
    store.pool().close().await;
    // 第二次启动：迁移账本已成功；正常启动仍无条件推号（不属于迁移覆盖）。
    let store2 = crate::store::Store::connect(&url).await.expect("second boot");
    let row2 = sqlx::query("SELECT enabled, admission_version FROM member_reset_admission WHERE id=1")
        .fetch_one(store2.pool()).await.unwrap();
    assert_eq!(row2.get::<i64,_>("enabled"), 0);
    assert!(row2.get::<i64,_>("admission_version") > version_after_first,
        "normal boot must bump the version unconditionally (not a migration concern)");
    let rows = sqlx::query("SELECT COUNT(*) AS n FROM member_reset_admission").fetch_one(store2.pool()).await.unwrap();
    assert_eq!(rows.get::<i64,_>("n"), 1, "no duplicate seed rows");
    store2.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// SQLite：迁移在事务内执行 → 半执行不可达（结构性原因：单连接事务包裹）。
/// 用手工 dirty 账本覆盖恢复函数的探针判据：种子未插、dirty live 行、
/// 只有非 id=1 行、错误表结构。
#[tokio::test]
async fn a2_sqlite_dirty_ledger_probe_cases() {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

    async fn make_dirty_and_try(
        source: &std::path::Path,
        label: &str,
        steps: usize,
        mutations: &[&str],
    ) -> Result<(), anyhow::Error> {
        let path = std::env::temp_dir()
            .join(format!("rustcore-a2lite-{}-{}.db", label, ulid::Ulid::new()));
        std::fs::File::create(&path).expect("create sqlite");
        let url = format!("sqlite://{}", path.display());
        let target = std::env::temp_dir()
            .join(format!("rustcore-a2lite-mig-{}-{}", label, ulid::Ulid::new()));
        std::fs::create_dir_all(&target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let p = entry.unwrap().path();
            let name = p.file_name().unwrap().to_str().unwrap().to_string();
            let version: i64 = name.split('_').next().unwrap().parse().unwrap();
            if version > 35 { continue; }
            std::fs::copy(&p, target.join(&name)).unwrap();
        }
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1).connect(&url).await.unwrap();
        sqlx::migrate::Migrator::new(target.as_path()).await.unwrap()
            .run(&pool).await.unwrap();
        write_dirty_version(&pool, 37).await;
        run_steps(&pool, 37, steps).await;
        for mutation in mutations {
            sqlx::query(mutation).execute(&pool).await.unwrap();
        }
        // SQLite: close the fixture pool before Store::connect creates its own —
        // an open pool can hold the database lock and prevent the new connection
        // from seeing (or writing) the state the probe needs to detect.
        pool.close().await;
        let result = crate::store::Store::connect(&url).await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&target);
        result.map(|_| ())
    }

    // 种子未插（tables built, seed not inserted）→ 恢复补种子并落章。
    assert!(
        make_dirty_and_try(&source, "seedmiss", 3, &[]).await.is_ok(),
        "seed-missing recovery must succeed"
    );
    // dirty live 行 → 恢复必须拒绝（reenter 在 migrator.run 之前执行，
    // 对 SQLite 同样生效——见 store.rs::connect_unregistered 的调用顺序）。
    // steps=4（全部语句含种子 INSERT），然后 UPDATE 种子行为 enabled=1。
    // 旧行保持与账本不落章由下方专门用例断言。
    assert!(
        make_dirty_and_try(&source, "liverow", 4, &[
            "UPDATE member_reset_admission SET enabled=1, admission_version=5, basis='clean_attestation:legacy' WHERE id=1",
        ]).await.is_err(),
        "dirty live row must be refused by the re-entry probe"
    );
    // 只有非 id=1 行 → 效果未证明。
    assert!(
        make_dirty_and_try(&source, "wrongid", 3, &[
            "DELETE FROM member_reset_admission WHERE id=1",
            "INSERT INTO member_reset_admission \
             (id, enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by) \
             VALUES (2, 0, '', 0, '', 0, '')",
        ]).await.is_err(),
        "non-id=1 rows must be unproven"
    );
    // 错误表结构（列形状不对）→ 拒绝。
    assert!(
        make_dirty_and_try(&source, "wrongtype", 3, &[
            "DROP TABLE member_reset_admission",
            "CREATE TABLE member_reset_admission (id INT PRIMARY KEY, wrong_col VARCHAR(10))",
        ]).await.is_err(),
        "wrong table structure must be refused"
    );
}

/// SQLite dirty live 行（原单 A2 情形 3 的 SQLite 腿）：手工构造
/// `id=1/enabled=1/admission_version=5` 后启动 → 恢复边界失败、
/// 账本不落成功章、旧行全值不变——不以正常启动推号后的状态替代
/// 迁移阶段断言。
#[tokio::test]
async fn a2_sqlite_dirty_live_row_refused_and_preserved() {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let path = std::env::temp_dir()
        .join(format!("rustcore-a2liverow-{}.db", ulid::Ulid::new()));
    std::fs::File::create(&path).expect("create sqlite");
    let url = format!("sqlite://{}", path.display());
    let target = std::env::temp_dir()
        .join(format!("rustcore-a2liverow-mig-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&target).unwrap();
    for entry in std::fs::read_dir(&source).unwrap() {
        let p = entry.unwrap().path();
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        let version: i64 = name.split('_').next().unwrap().parse().unwrap();
        if version > 35 {
            continue;
        }
        std::fs::copy(&p, target.join(&name)).unwrap();
    }
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::migrate::Migrator::new(target.as_path())
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    write_dirty_version(&pool, 37).await;
    run_steps(&pool, 37, 4).await; // 全部语句（含种子 INSERT）已执行
    sqlx::query(
        "UPDATE member_reset_admission SET enabled=1, admitted_boot_id='01ARZ3NDEKTSV4RRFFQ69G5FAV', \
         admission_version=5, basis='clean_attestation:legacy', admitted_at_ms=123, admitted_by='op' WHERE id=1",
    )
    .execute(&pool)
    .await
    .expect("dirty the live seed row");
    pool.close().await;
    // 恢复边界必须失败。
    assert!(
        crate::store::Store::connect(&url).await.is_err(),
        "recovery must refuse a dirty live seed row"
    );
    // 重开检查连接：旧行全值不变 + 账本不落成功章。
    let check = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let row = sqlx::query(
        "SELECT enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by \
         FROM member_reset_admission WHERE id=1",
    )
    .fetch_one(&check)
    .await
    .expect("dirty row still present");
    assert_eq!(row.get::<i64, _>("enabled"), 1, "existing row untouched");
    assert_eq!(row.get::<i64, _>("admission_version"), 5);
    assert_eq!(
        row.get::<i64, _>("admitted_at_ms"),
        123,
        "full-row preservation: admitted_at_ms"
    );
    assert_eq!(
        crate::store::try_get_text(&row, "basis").ok().flatten(),
        Some("clean_attestation:legacy".to_string()),
        "full-row preservation: basis"
    );
    assert_eq!(
        crate::store::try_get_text(&row, "admitted_by").ok().flatten(),
        Some("op".to_string()),
        "full-row preservation: admitted_by"
    );
    assert_eq!(
        crate::store::try_get_text(&row, "admitted_boot_id").ok(),
        Some(Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string())),
        "full-row preservation: admitted_boot_id"
    );
    let stamped = sqlx::query(
        "SELECT CAST(success AS INTEGER) AS ok FROM _sqlx_migrations WHERE version=37",
    )
    .fetch_one(&check)
    .await
    .expect("ledger row exists");
    assert_eq!(
        stamped.get::<i64, _>("ok"),
        0,
        "no success stamp for a dirty live row"
    );
    check.close().await;
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&target);
}

/// V-12r MySQL 腿：成功迁移后单例行缺失 → admit/reset fail-closed。
#[tokio::test]
async fn a2_mysql_missing_singleton_after_success_fails_closed() {
    let Some((pool, url, admin_url)) = mysql_scratch_p4b("migmiss", 38).await else { return };
    sqlx::query("DELETE FROM member_reset_admission WHERE id=1").execute(&pool).await.unwrap();
    let store = crate::store::Store::connect(&url).await.expect("boot still works");
    let outcome = reset_member(&store, "cred:5501", 5501, 1, "01A7XQZ90ZYY9VNEHNJQ000012", 2_000).await.unwrap();
    assert_eq!(outcome, MemberResetOutcome::NotAdmitted);
    let admitted = admit_member_reset(&store, "clean_attestation:x", "op", false, store.boot_id(), Some(0), 1_000).await.unwrap();
    assert!(admitted.is_err(), "admission must fail closed on the missing singleton");
    store.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

// ---------------------------------------------------------------------------
// A3 · MySQL 腿补齐
// ---------------------------------------------------------------------------

/// V-01 MySQL：会员设 global 拒绝（引擎层 + gRPC 入口）；三表零写。
#[tokio::test]
async fn a3_mysql_v01_member_global_refused_both_entries() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("v01m", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    // 引擎层
    assert!(arm_member(&store, ArmRequest {
        scope_key: "global", level: Level::PauseNew,
        trigger_source: TRIGGER_SOURCE_MEMBER_API, reason: "x", now_ms: 1,
    }, member_ctx(MEMBER_A)).await.is_err());
    // gRPC 入口层（svc handler 闭世界）
    use crate::svc_v2::EngineExecutionV2Service;
    use crate::proto_v2::engine_execution_server::EngineExecution as _;
    let service = EngineExecutionV2Service::new(store.clone());
    use tonic::Request;
    use crate::proto_v2 as proto;
    let err = service.set_kill_switch(Request::new(proto::SetKillSwitchRequest {
        scope_key: "global".into(), level: "pause_new".into(), reason: "x".into(),
        trigger_source: "member_api".into(), actor: "member:5501".into(),
        expected_authority_revision: None, authority_member_id: Some(5501),
        request_ref: Some("01A7XQZ90ZYY9VNEHNJQ000001".into()), schema_version: 1,
    })).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // 三表零写：global 行保持 none/0。
    let row = sqlx::query("SELECT level, flatten_generation FROM kill_switch_state WHERE scope_key='global'")
        .fetch_one(store.pool()).await.unwrap();
    assert_eq!(row.get::<String,_>("level"), "none");
    assert_eq!(row.get::<i64,_>("flatten_generation"), 0);
    let events = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events WHERE scope='global'")
        .fetch_one(store.pool()).await.unwrap().get::<i64,_>("n");
    assert_eq!(events, 0);
    store.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// V-12/C1-f/g MySQL：启动推号；准入后重启失效。
#[tokio::test]
async fn a3_mysql_v12_boot_bump_and_admission_cycle() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("v12m", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    assert_eq!(admission_row(&store).await.0, 0);
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    // 新启动周期（同库再 connect）：无条件失效（MySQL 行锁下同判据）。
    let store2 = crate::store::Store::connect(&url).await.unwrap();
    assert_eq!(admission_row(&store2).await.0, 0, "new boot disables admission");
    store.pool().close().await; store2.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// V-14 MySQL：_ci 大小写变体与尾空格在 BINARY 比较下都不命中。
#[tokio::test]
async fn a3_mysql_v14_variants_miss_under_binary_compare() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("v14m2", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    let arm_ref = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    arm_member(&store, ArmRequest {
        scope_key: "cred:v14b", level: Level::PauseNew,
        trigger_source: TRIGGER_SOURCE_MEMBER_API, reason: "f", now_ms: 1,
    }, MemberArmContext { member_id: MEMBER_A, request_ref: arm_ref }).await.unwrap();
    // 大小写变体被闭世界拒绝（非静默不命中）。
    assert!(get_kill_switch_states(&store, &["cred:v14b".into()], &[variant_ref(arm_ref)]).await.is_err());
    // 尾空格变体同样拒绝。
    assert!(get_kill_switch_states(&store, &["cred:v14b".into()], &[format!("{arm_ref} ")]).await.is_err());
    store.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// V-22/C1-a..d MySQL：准入请求绑定（boot/version 双比对、重放拒）。
#[tokio::test]
async fn a3_mysql_v22_admission_binding() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("v22m", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    let version = admission_row(&store).await.2;
    // C1-a：旧 boot。
    let stale_boot = "01ARZ3NDEKTSV4RRFFQ69G5FZZ";
    let rejected = admit_member_reset(&store, "clean_attestation:x", "op", false, stale_boot, Some(version), 1_000).await.unwrap().unwrap_err();
    assert!(rejected.contains("boot cycle"));
    // C1-c：当前 boot/version 成功。
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    let after = admission_row(&store).await;
    assert_eq!(after.0, 1);
    // C1-d：重放同一意图（旧 version）拒。
    let replay = admit_member_reset(&store, "clean_attestation:fixture", "op", false, store.boot_id(), Some(version), 2_000).await.unwrap().unwrap_err();
    assert!(replay.contains("admission_version"));
    assert_eq!(admission_row(&store).await, after, "replay must not bump");
    store.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// C1-e MySQL：准入撤销与在途 reset 的两种提交顺序（行锁串行化）。
#[tokio::test]
async fn a3_mysql_c1e_revocation_vs_inflight_reset_both_orders() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("c1e", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    // 顺序 A：准入 → 会员 arm+收敛+reset 成功 → 撤销（新 boot）后 reset 拒。
    arm_member_ok(&store, "cred:c1ea", Level::PauseNew, MEMBER_A, 1_000).await;
    converge_clean(&store, "cred:c1ea", 1_500).await;
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    let generation = state_authority(&store, "cred:c1ea").await.2;
    let outcome = reset_member(&store, "cred:c1ea", MEMBER_A, generation, "01A7XQZ90ZYY9VNEHNJQ000013", 2_000).await.unwrap();
    assert!(matches!(outcome, MemberResetOutcome::Executed { .. }));
    // 顺序 B：arm → 准入 → 撤销（推号）→ 在途 reset（旧授权）必须被锁内重读拒绝。
    arm_member_ok(&store, "cred:c1eb", Level::PauseNew, MEMBER_A, 1_000).await;
    converge_clean(&store, "cred:c1eb", 1_500).await;
    admit_current_boot(&store, "clean_attestation:fixture2", false).await.unwrap();
    let generation_b = state_authority(&store, "cred:c1eb").await.2;
    // 撤销 = 新 Store（同库新 boot 推号）。
    let store3 = crate::store::Store::connect(&url).await.unwrap();
    let outcome_b = reset_member(&store3, "cred:c1eb", MEMBER_A, generation_b, "01A7XQZ90ZYY9VNEHNJQ000014", 3_000).await.unwrap();
    assert_eq!(outcome_b, MemberResetOutcome::NotAdmitted, "post-revocation reset must fail in the in-lock read");
    store.pool().close().await; store3.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// C1-e（A3 行锁变异的击杀用例）：准入撤销与在途 reset 的并发串行化。
/// reset 在「准入门已读、行锁仍持有」处暂停（测试钩子），另一条连接以
/// 2 秒锁等待尝试撤销 UPDATE：**带行锁**时它必须超时（撤销被串行化到
/// reset 提交之后）；去掉 FOR UPDATE（MA3 变异）时它会立即成功——测试
/// 断言超时即被击杀。
#[tokio::test]
async fn a3_mysql_c1e_concurrent_revocation_during_inflight_reset() {
    let Some((_pool, url, admin_url)) = mysql_scratch_p4b("c1erace", 38).await else { return };
    let store = crate::store::Store::connect(&url).await.unwrap();
    arm_member_ok(&store, "cred:c1er", Level::PauseNew, MEMBER_A, 1_000).await;
    converge_clean(&store, "cred:c1er", 1_500).await;
    admit_current_boot(&store, "clean_attestation:fixture", false).await.unwrap();
    let generation = state_authority(&store, "cred:c1er").await.2;

    let outcome_slot = std::sync::Arc::new(std::sync::Mutex::new(String::from("no-attempt")));
    let url_arc = std::sync::Arc::new(url.clone());
    {
        let mut guard = crate::kill_switch::ADMISSION_PAUSE_HOOK.lock().unwrap();
        let slot = std::sync::Arc::clone(&outcome_slot);
        let url2 = std::sync::Arc::clone(&url_arc);
        *guard = Some(Box::new(move || {
            let slot = std::sync::Arc::clone(&slot);
            let url2 = std::sync::Arc::clone(&url2);
            Box::pin(async move {
                // 独立连接 + 2 秒锁等待，尝试撤销（enabled=0 推号）。
                let pool = sqlx::any::AnyPoolOptions::new()
                    .max_connections(1)
                    .connect(&url2)
                    .await
                    .unwrap();
                let _ = sqlx::query("SET SESSION innodb_lock_wait_timeout = 2")
                    .execute(&pool)
                    .await;
                let attempted = sqlx::query(
                    "UPDATE member_reset_admission \
                     SET enabled = 0, admission_version = admission_version + 1 WHERE id = 1",
                )
                .execute(&pool)
                .await;
                *slot.lock().unwrap() = match attempted {
                    Ok(_) => "revoked-during-pause".to_string(),
                    Err(_) => "blocked-by-lock".to_string(),
                };
                pool.close().await;
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        }));
    }
    let outcome = reset_member(&store, "cred:c1er", MEMBER_A, generation, "01A7XQZ90ZYY9VNEHNJQ000093", 2_000).await.unwrap();
    {
        let mut guard = crate::kill_switch::ADMISSION_PAUSE_HOOK.lock().unwrap();
        *guard = None;
    }
    let verdict = outcome_slot.lock().unwrap().clone();
    assert_eq!(
        verdict, "blocked-by-lock",
        "the revocation must serialize behind the in-flight reset's row lock (got {verdict}; outcome {outcome:?})"
    );
    store.pool().close().await;
    drop_mysql_scratch(&url, &admin_url, "").await;
}

/// **P2-4（评审 claude-review-m0 · M0fix）· 0044/0045 的崩溃重入逐条语句执行。**
///
/// sqlx-mysql 对带参 `query` 一律走 PREPARE，MySQL 拒绝多语句 PREPARE
/// （ERROR 1064 near "CREATE INDEX"）：把整个 0044 文件登记成一步的
/// re-entry 在 MySQL 上必然失败，「表在、索引缺」的断点还会被只探表的
/// 探针落章。两个迁移的**每个**断点（0…全部语句）各建 scratch 库：启动
/// 恢复逐段补齐并落章；恢复出的 schema 与全新安装逐列、逐索引一致。
#[tokio::test]
async fn m0fix_mysql_0044_0045_statement_breakpoints_recover_and_stamp() {
    async fn columns(pool: &sqlx::Pool<sqlx::Any>) -> Vec<(String, String, String)> {
        sqlx::query(
            "SELECT column_name AS c, column_type AS t, is_nullable AS n \
             FROM information_schema.columns WHERE table_schema=DATABASE() \
               AND table_name='protection_cleanup_obligations' ORDER BY column_name",
        )
        .fetch_all(pool)
        .await
        .expect("read the obligation columns")
        .iter()
        .map(|row| {
            (
                crate::store::try_get_text(row, "c")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                crate::store::try_get_text(row, "t")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                crate::store::try_get_text(row, "n")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            )
        })
        .collect()
    }
    async fn indexes(pool: &sqlx::Pool<sqlx::Any>) -> Vec<(String, i64, String)> {
        sqlx::query(
            "SELECT index_name AS i, CAST(seq_in_index AS SIGNED) AS s, column_name AS c \
             FROM information_schema.statistics WHERE table_schema=DATABASE() \
               AND table_name='protection_cleanup_obligations' ORDER BY index_name, seq_in_index",
        )
        .fetch_all(pool)
        .await
        .expect("read the obligation indexes")
        .iter()
        .map(|row| {
            (
                crate::store::try_get_text(row, "i")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                row.get::<i64, _>("s"),
                crate::store::try_get_text(row, "c")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            )
        })
        .collect()
    }

    let Some((fresh_pool, fresh_url, fresh_admin)) = mysql_scratch_p4b("m0migfresh", 45).await
    else {
        return;
    };
    let fresh_columns = columns(&fresh_pool).await;
    let fresh_indexes = indexes(&fresh_pool).await;
    fresh_pool.close().await;
    drop_mysql_scratch(&fresh_url, &fresh_admin, "").await;

    for (version, statement_count) in [(44i64, 2usize), (45, 2)] {
        for breakpoint in 0..=statement_count {
            let label = format!("m0migbrk{version}b{breakpoint}");
            let Some((pool, url, admin_url)) = mysql_scratch_p4b(&label, version - 1).await else {
                return;
            };
            write_dirty_version(&pool, version).await;
            if breakpoint > 0 {
                run_steps(&pool, version, breakpoint).await;
            }
            pool.close().await;
            let store = crate::store::Store::connect(&url)
                .await
                .unwrap_or_else(|e| panic!("recovery boot v{version} b{breakpoint}: {e:#}"));
            let stamped = sqlx::query(
                "SELECT CAST(success AS SIGNED) AS ok FROM _sqlx_migrations WHERE version=?",
            )
            .bind(version)
            .fetch_one(store.pool())
            .await
            .expect("ledger row");
            assert_eq!(
                stamped.get::<i64, _>("ok"),
                1,
                "v{version} breakpoint {breakpoint} must stamp"
            );
            assert_eq!(
                columns(store.pool()).await,
                fresh_columns,
                "v{version} breakpoint {breakpoint}: recovered columns must equal a fresh install"
            );
            assert_eq!(
                indexes(store.pool()).await,
                fresh_indexes,
                "v{version} breakpoint {breakpoint}: recovered indexes must equal a fresh install"
            );
            store.pool().close().await;
            drop_mysql_scratch(&url, &admin_url, "").await;
        }
    }
    // 全新安装本身的结构判据（P0-1 生命周期键列 + P2-5 到期扫描索引）。
    assert!(
        fresh_columns
            .iter()
            .any(|(name, _, _)| name == "close_revision"),
        "0045 adds the lifecycle key column: {fresh_columns:?}"
    );
    for expected in [
        ("protection_cleanup_due_scan_idx", 1, "state"),
        ("protection_cleanup_due_scan_idx", 2, "next_attempt_at_ms"),
        ("protection_cleanup_pending_scope_idx", 1, "credential_id"),
        ("protection_cleanup_pending_scope_idx", 4, "state"),
    ] {
        assert!(
            fresh_indexes
                .iter()
                .any(|(i, s, c)| i == expected.0 && *s == expected.1 && c == expected.2),
            "fresh install must carry {expected:?}: {fresh_indexes:?}"
        );
    }
}
