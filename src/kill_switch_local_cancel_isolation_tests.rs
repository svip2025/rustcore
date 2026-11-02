//! F-1 · kill-switch 本地撤单的**逐单错误隔离**回归。
//!
//! 被钉住的契约：
//! 1. 一张不可回收的 `pending_risk` 行不得挡住同一轮里其余合法候选；
//! 2. 只要有一张失败，整轮仍是 `Err`（重试/失败可见不被吞掉），多轮如此；
//! 3. 失败行本身与它名下的 reservation / position claim / entry_epoch /
//!    verdict / protection plan **逐字段不变**（不撤、不释放）；
//! 4. 合法兄弟照常处置，Entry 兄弟的资源**恰一次**释放，重试不重复释放；
//!    ManageOnly 仍只选 Entry。
//!
//! 候选 SELECT 没有 `ORDER BY`，行序未指定。这里不靠行序碰运气：
//! * SQLite 与 MySQL 的 `cancel_local_candidates` 用例都显式覆盖失败行在前、
//!   交错、在后的顺序，确定性区分早退与隔离；
//! * 公共入口与定时任务用例验证完整候选集合、处置结果和多轮资源不变，
//!   不把数据库允许的扫描顺序当作产品契约。早退反例由上面的显式顺序钉住。

use std::path::PathBuf;
use std::sync::Once;
use std::time::Duration;

use sqlx::Row;

use super::{
    apply_local_order_semantics, arm, cancel_local_candidates, local_cancel_candidates,
    spawn_action_retry_task, ArmRequest, Level, LocalCancelCandidate,
    LOCAL_CANCEL_ERROR_DETAIL_LIMIT,
};
use crate::store::{mysql_test_support::MySqlScratch, try_get_nullable_string, Store};

static INSTALL_DRIVERS: Once = Once::new();

const CREDENTIAL_ID: &str = "cred-isolate";
const BOT_UID: &str = "bot-isolate";
const SCOPE_KEY: &str = "cred:cred-isolate";
const BAD_ENTRY: &str = "ord-bad";
const BTC: &str = "binance|futures|BTC|USDT|USDT";
const ETH: &str = "binance|futures|ETH|USDT|USDT";
const SOL: &str = "binance|futures|SOL|USDT|USDT";

async fn sqlite_store(label: &str) -> (Store, PathBuf) {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!(
        "rustcore-local-cancel-isolation-{label}-{}.db",
        ulid::Ulid::new()
    ));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("connect sqlite test store and run migrations");
    (store, path)
}

async fn drop_sqlite(store: Store, path: PathBuf) {
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

async fn seed_reservation(store: &Store, reserved_usdt: &str) {
    sqlx::query(
        "INSERT INTO notional_reservations \
         (credential_id, reserved_usdt, cap_usdt, updated_at_ms) VALUES (?, ?, '5000', 1)",
    )
    .bind(CREDENTIAL_ID)
    .bind(reserved_usdt)
    .execute(store.pool())
    .await
    .expect("seed the shared notional reservation");
}

/// 一张权威完整的 `pending_risk` Entry：claim + verdict(100 USDT) + order + plan。
async fn seed_entry(store: &Store, engine_order_id: &str, symbol_key: &str) {
    let authority_key = crate::position_state::semantic_idempotency_key(&format!(
        "entry|{BOT_UID}|{symbol_key}|long|1"
    ));
    let position_id = format!("pos-{engine_order_id}");
    let verdict_id = format!("rv-{engine_order_id}");
    let intent_id = format!("intent-{engine_order_id}");
    let mut tx = store.pool().begin().await.expect("begin Entry fixture");
    sqlx::query(
        r#"
        INSERT INTO position_state (
          position_id, credential_id, bot_uid, symbol_key, direction,
          entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
        ) VALUES (?, ?, ?, ?, 'long', 1, 'pending', ?, ?, '0', 1)
        "#,
    )
    .bind(&position_id)
    .bind(CREDENTIAL_ID)
    .bind(BOT_UID)
    .bind(symbol_key)
    .bind(crate::position_state::active_key(
        BOT_UID, symbol_key, "long",
    ))
    .bind(&authority_key)
    .execute(&mut *tx)
    .await
    .expect("seed Entry position claim");
    sqlx::query(
        r#"
        INSERT INTO risk_verdicts (
          risk_verdict_id, intent_id, approved_intent_key, credential_id,
          bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
          decision, sizing_json, rules_json, evaluator_version
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 'test', 'hash', 'approve',
          '{"final_notional_usdt":"100"}', '[]', 'test')
        "#,
    )
    .bind(&verdict_id)
    .bind(&intent_id)
    .bind(&intent_id)
    .bind(CREDENTIAL_ID)
    .bind(BOT_UID)
    .bind(symbol_key)
    .execute(&mut *tx)
    .await
    .expect("seed Entry risk verdict");
    sqlx::query(
        r#"
        INSERT INTO engine_orders (
          engine_order_id, credential_id, bot_uid, idempotency_key,
          exchange, market_kind, symbol_key, action, side, order_type,
          quantity, reduce_only, order_role, status, risk_verdict_id,
          position_id, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, 'binance', 'futures', ?, 'open_long', 'long',
          'market', '1', 0, 'entry', 'pending_risk', ?, ?, 1, 1)
        "#,
    )
    .bind(engine_order_id)
    .bind(CREDENTIAL_ID)
    .bind(BOT_UID)
    .bind(&authority_key)
    .bind(symbol_key)
    .bind(&verdict_id)
    .bind(&position_id)
    .execute(&mut *tx)
    .await
    .expect("seed pending_risk Entry order");
    sqlx::query(
        r#"
        INSERT INTO position_protection_plans (
          position_id, credential_id, bot_uid, exchange, market_kind,
          symbol_key, direction, stop_loss_price, take_profit_price,
          required, confirm_timeout_ms, generation, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, 'binance', 'futures', ?, 'long', '95', '110',
          1, 3000, 1, 1, 1)
        "#,
    )
    .bind(&position_id)
    .bind(CREDENTIAL_ID)
    .bind(BOT_UID)
    .bind(symbol_key)
    .execute(&mut *tx)
    .await
    .expect("seed Entry protection plan");
    tx.commit().await.expect("commit Entry fixture");
}

/// 一张合法的 `pending_risk` 平仓兄弟单（cancel-only，不拥有任何资源）。
async fn seed_exit(store: &Store, engine_order_id: &str) {
    sqlx::query(
        r#"
        INSERT INTO engine_orders (
          engine_order_id, credential_id, bot_uid, idempotency_key,
          exchange, market_kind, symbol_key, action, side, order_type,
          quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, 'binance', 'futures', 'binance|futures|XRP|USDT|USDT',
          'close_long', 'short', 'market', '1', 1, 'exit', 'pending_risk', 1, 1)
        "#,
    )
    .bind(engine_order_id)
    .bind(CREDENTIAL_ID)
    .bind(BOT_UID)
    .bind(format!("idem-{engine_order_id}"))
    .execute(store.pool())
    .await
    .expect("seed pending_risk exit sibling");
}

#[derive(Clone, Copy, Debug)]
enum Corruption {
    /// 0009 迁移之前写入的形状：`order_role` / `position_id` 都是 NULL。
    LegacyNullRole,
    /// 分类阶段（任何锁之前）即失败。
    ExchangeIdentity,
    /// 取消 CAS **成功之后**才在事务内失败——必须整体回滚。
    RejectedVerdict,
}

impl Corruption {
    fn reason(self) -> &'static str {
        match self {
            Self::LegacyNullRole => "unknown or noncanonical durable role",
            Self::ExchangeIdentity => "already has an exchange order identity",
            Self::RejectedVerdict => "risk verdict does not match durable order ownership",
        }
    }
}

async fn corrupt(store: &Store, engine_order_id: &str, corruption: Corruption) {
    let (statement, key) = match corruption {
        Corruption::LegacyNullRole => (
            "UPDATE engine_orders SET order_role=NULL, position_id=NULL WHERE engine_order_id=?",
            engine_order_id.to_string(),
        ),
        Corruption::ExchangeIdentity => (
            "UPDATE engine_orders SET exchange_order_id='venue-1' WHERE engine_order_id=?",
            engine_order_id.to_string(),
        ),
        Corruption::RejectedVerdict => (
            "UPDATE risk_verdicts SET decision='reject' WHERE risk_verdict_id=?",
            format!("rv-{engine_order_id}"),
        ),
    };
    let affected = sqlx::query(statement)
        .bind(key)
        .execute(store.pool())
        .await
        .expect("corrupt the durable authority")
        .rows_affected();
    assert_eq!(affected, 1, "the corruption must hit exactly one row");
}

async fn reserved_usdt(store: &Store) -> f64 {
    sqlx::query("SELECT reserved_usdt FROM notional_reservations WHERE credential_id=?")
        .bind(CREDENTIAL_ID)
        .fetch_one(store.pool())
        .await
        .expect("load the shared reservation")
        .get::<String, _>("reserved_usdt")
        .parse::<f64>()
        .expect("numeric reservation")
}

async fn order_status(store: &Store, engine_order_id: &str) -> (String, i64) {
    let row =
        sqlx::query("SELECT status, updated_at_ms FROM engine_orders WHERE engine_order_id=?")
            .bind(engine_order_id)
            .fetch_one(store.pool())
            .await
            .expect("load order status");
    (row.get("status"), row.get("updated_at_ms"))
}

/// 一张 Entry 及其名下全部资源的逐字段指纹；「坏行不变」用前后相等来断言。
async fn entry_fingerprint(store: &Store, engine_order_id: &str) -> Vec<String> {
    let nullable = |row: &sqlx::any::AnyRow, column: &str| {
        try_get_nullable_string(row, column)
            .expect("read nullable column")
            .unwrap_or_else(|| "<NULL>".to_string())
    };
    let order = sqlx::query(
        "SELECT status, order_role, exchange_order_id, position_id, risk_verdict_id, \
                updated_at_ms FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(engine_order_id)
    .fetch_one(store.pool())
    .await
    .expect("load fingerprint order");
    let position = sqlx::query(
        "SELECT state, entry_epoch, active_key, pending_idem_key, qty, updated_at_ms \
         FROM position_state WHERE position_id=?",
    )
    .bind(format!("pos-{engine_order_id}"))
    .fetch_one(store.pool())
    .await
    .expect("load fingerprint position claim");
    let verdict = sqlx::query("SELECT decision FROM risk_verdicts WHERE risk_verdict_id=?")
        .bind(format!("rv-{engine_order_id}"))
        .fetch_one(store.pool())
        .await
        .expect("load fingerprint verdict");
    let plans =
        sqlx::query("SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?")
            .bind(format!("pos-{engine_order_id}"))
            .fetch_one(store.pool())
            .await
            .expect("count fingerprint plans")
            .get::<i64, _>("count");
    vec![
        format!("order.status={}", order.get::<String, _>("status")),
        format!("order.role={}", nullable(&order, "order_role")),
        format!(
            "order.exchange_order_id={}",
            nullable(&order, "exchange_order_id")
        ),
        format!("order.position_id={}", nullable(&order, "position_id")),
        format!(
            "order.risk_verdict_id={}",
            nullable(&order, "risk_verdict_id")
        ),
        format!(
            "order.updated_at_ms={}",
            order.get::<i64, _>("updated_at_ms")
        ),
        format!("claim.state={}", position.get::<String, _>("state")),
        format!(
            "claim.entry_epoch={}",
            position.get::<i64, _>("entry_epoch")
        ),
        format!("claim.active_key={}", nullable(&position, "active_key")),
        format!(
            "claim.pending_idem_key={}",
            nullable(&position, "pending_idem_key")
        ),
        format!("claim.qty={}", position.get::<String, _>("qty")),
        format!(
            "claim.updated_at_ms={}",
            position.get::<i64, _>("updated_at_ms")
        ),
        format!("verdict.decision={}", verdict.get::<String, _>("decision")),
        format!("plans={plans}"),
    ]
}

/// 合法 Entry 兄弟被处置后的终态：单已撤、claim 退休且 epoch 保留、plan 删除、
/// verdict 作为审计保留。
async fn assert_entry_released(store: &Store, engine_order_id: &str) {
    let fingerprint = entry_fingerprint(store, engine_order_id).await;
    for expected in [
        "order.status=canceled",
        "claim.state=closed",
        "claim.entry_epoch=1",
        "claim.active_key=<NULL>",
        "claim.pending_idem_key=<NULL>",
        "verdict.decision=approve",
        "plans=0",
    ] {
        assert!(
            fingerprint.iter().any(|field| field == expected),
            "{engine_order_id}: expected {expected} in {fingerprint:?}"
        );
    }
}

/// 公共入口必须选择完整的候选集合；未指定 ORDER BY 的查询不承诺行序。
async fn assert_candidate_set(store: &Store, level: Level, failing: &[&str], siblings: &[&str]) {
    let mut scan: Vec<String> = local_cancel_candidates(store, SCOPE_KEY, level)
        .await
        .expect("read the complete candidate set")
        .into_iter()
        .map(|candidate| candidate.engine_order_id)
        .collect();
    let mut expected: Vec<&str> = failing.iter().chain(siblings).copied().collect();
    scan.sort();
    expected.sort();
    assert_eq!(
        scan, expected,
        "all failing rows and legitimate siblings must be selected"
    );
}

fn assert_incomplete(error: &anyhow::Error, failed: u64, canceled: u64, mentions: &[&str]) {
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "local cancel incomplete: {failed} order(s) failed, {canceled} canceled, \
             0 not attempted;"
        )),
        "unexpected summary: {message}"
    );
    for fragment in mentions {
        assert!(
            message.contains(fragment),
            "missing {fragment:?} in: {message}"
        );
    }
}

/// Flatten：一张失败 Entry 夹在两张合法平仓兄弟之间，三轮调用。
async fn flatten_scenario(store: &Store, corruption: Corruption) {
    seed_reservation(store, "100").await;
    seed_exit(store, "aaa-exit").await;
    seed_entry(store, BAD_ENTRY, BTC).await;
    seed_exit(store, "zzz-exit").await;
    corrupt(store, BAD_ENTRY, corruption).await;
    let before = entry_fingerprint(store, BAD_ENTRY).await;
    assert_candidate_set(
        store,
        Level::Flatten,
        &[BAD_ENTRY],
        &["aaa-exit", "zzz-exit"],
    )
    .await;

    for pass in 1..=3_i64 {
        let error = apply_local_order_semantics(store, SCOPE_KEY, Level::Flatten, 10 + pass)
            .await
            .expect_err("a failing order must keep the whole round failed");
        let canceled = if pass == 1 { 2 } else { 0 };
        assert_incomplete(&error, 1, canceled, &[BAD_ENTRY, corruption.reason()]);
        for sibling in ["aaa-exit", "zzz-exit"] {
            assert_eq!(
                order_status(store, sibling).await,
                ("canceled".to_string(), 11),
                "pass={pass}: {sibling} must be canceled exactly in the first round"
            );
        }
        assert_eq!(
            entry_fingerprint(store, BAD_ENTRY).await,
            before,
            "pass={pass}: the failing row and everything it owns must stay untouched"
        );
        assert_eq!(reserved_usdt(store).await, 100.0, "pass={pass}");
    }
}

/// ManageOnly：失败 Entry（CAS 之后在事务内失败 → 回滚）夹在两张合法 Entry
/// 兄弟之间；平仓兄弟不在 ManageOnly 的选择面内。
async fn manage_only_scenario(store: &Store) {
    seed_reservation(store, "300").await;
    seed_entry(store, "aaa-entry", ETH).await;
    seed_entry(store, BAD_ENTRY, BTC).await;
    seed_exit(store, "mid-exit").await;
    seed_entry(store, "zzz-entry", SOL).await;
    corrupt(store, BAD_ENTRY, Corruption::RejectedVerdict).await;
    let before = entry_fingerprint(store, BAD_ENTRY).await;
    assert_candidate_set(
        store,
        Level::ManageOnly,
        &[BAD_ENTRY],
        &["aaa-entry", "zzz-entry"],
    )
    .await;

    for pass in 1..=3_i64 {
        let error = apply_local_order_semantics(store, SCOPE_KEY, Level::ManageOnly, 10 + pass)
            .await
            .expect_err("a failing Entry must keep the ManageOnly round failed");
        let canceled = if pass == 1 { 2 } else { 0 };
        assert_incomplete(
            &error,
            1,
            canceled,
            &[BAD_ENTRY, Corruption::RejectedVerdict.reason()],
        );
        assert_entry_released(store, "aaa-entry").await;
        assert_entry_released(store, "zzz-entry").await;
        assert_eq!(
            reserved_usdt(store).await,
            100.0,
            "pass={pass}: both siblings released exactly once; the failing Entry keeps its 100"
        );
        assert_eq!(
            entry_fingerprint(store, BAD_ENTRY).await,
            before,
            "pass={pass}: the rolled-back Entry must stay untouched"
        );
        assert_eq!(
            order_status(store, "mid-exit").await,
            ("pending_risk".to_string(), 1),
            "pass={pass}: ManageOnly must still select Entry orders only"
        );
    }

    let error = apply_local_order_semantics(store, SCOPE_KEY, Level::Flatten, 20)
        .await
        .expect_err("Flatten still reports the failing Entry");
    assert_incomplete(&error, 1, 1, &[BAD_ENTRY]);
    assert_eq!(order_status(store, "mid-exit").await.0, "canceled");
    assert_eq!(reserved_usdt(store).await, 100.0);
    assert_eq!(entry_fingerprint(store, BAD_ENTRY).await, before);
}

fn candidates(ids: &[&str]) -> Vec<LocalCancelCandidate> {
    ids.iter()
        .map(|id| LocalCancelCandidate {
            engine_order_id: (*id).to_string(),
            status: "pending_risk".to_string(),
        })
        .collect()
}

/// 显式顺序：两张失败行（NULL role + 损坏 Entry）之后仍要处置合法项。
async fn explicit_order_scenario(store: &Store, order: &[&str]) {
    seed_reservation(store, "200").await;
    seed_exit(store, "bad-null").await;
    corrupt(store, "bad-null", Corruption::LegacyNullRole).await;
    seed_entry(store, BAD_ENTRY, BTC).await;
    corrupt(store, BAD_ENTRY, Corruption::ExchangeIdentity).await;
    seed_entry(store, "good-entry", ETH).await;
    seed_exit(store, "good-exit").await;
    let before = entry_fingerprint(store, BAD_ENTRY).await;

    for pass in 1..=2_i64 {
        let error = cancel_local_candidates(store, SCOPE_KEY, &candidates(order), 10 + pass)
            .await
            .expect_err("two failing orders keep the round failed");
        let canceled = if pass == 1 { 2 } else { 0 };
        assert_incomplete(
            &error,
            2,
            canceled,
            &[
                "bad-null[pending_risk]",
                Corruption::LegacyNullRole.reason(),
                "ord-bad[pending_risk]",
                Corruption::ExchangeIdentity.reason(),
            ],
        );
        assert_eq!(
            order_status(store, "good-exit").await,
            ("canceled".to_string(), 11)
        );
        assert_entry_released(store, "good-entry").await;
        assert_eq!(reserved_usdt(store).await, 100.0, "pass={pass}");
        assert_eq!(
            order_status(store, "bad-null").await,
            ("pending_risk".to_string(), 1)
        );
        assert_eq!(
            entry_fingerprint(store, BAD_ENTRY).await,
            before,
            "pass={pass}"
        );
    }
}

#[tokio::test]
async fn local_cancel_isolation_flatten_failing_entry_does_not_block_exit_siblings() {
    for corruption in [Corruption::LegacyNullRole, Corruption::ExchangeIdentity] {
        let (store, path) = sqlite_store("flatten").await;
        flatten_scenario(&store, corruption).await;
        drop_sqlite(store, path).await;
    }
}

#[tokio::test]
async fn local_cancel_isolation_manage_only_releases_entry_siblings_exactly_once() {
    let (store, path) = sqlite_store("manage-only").await;
    manage_only_scenario(&store).await;
    drop_sqlite(store, path).await;
}

#[tokio::test]
async fn local_cancel_isolation_failures_first_in_explicit_order_still_reach_siblings() {
    for order in [
        ["bad-null", BAD_ENTRY, "good-entry", "good-exit"],
        [BAD_ENTRY, "good-entry", "bad-null", "good-exit"],
        ["good-exit", "good-entry", BAD_ENTRY, "bad-null"],
    ] {
        let (store, path) = sqlite_store("explicit-order").await;
        explicit_order_scenario(&store, &order).await;
        drop_sqlite(store, path).await;
    }
}

/// 对照臂：没有失败行时整轮 `Ok`，且第二轮不重复计数、不重复释放。
#[tokio::test]
async fn local_cancel_isolation_wellformed_scope_still_succeeds() {
    let (store, path) = sqlite_store("wellformed").await;
    seed_reservation(&store, "100").await;
    seed_exit(&store, "aaa-exit").await;
    seed_entry(&store, "good-entry", BTC).await;
    seed_exit(&store, "zzz-exit").await;
    assert_eq!(
        apply_local_order_semantics(&store, SCOPE_KEY, Level::Flatten, 11)
            .await
            .expect("a well-formed scope succeeds"),
        3
    );
    assert_entry_released(&store, "good-entry").await;
    assert_eq!(reserved_usdt(&store).await, 0.0);
    assert_eq!(
        apply_local_order_semantics(&store, SCOPE_KEY, Level::Flatten, 12)
            .await
            .expect("nothing is left to cancel"),
        0
    );
    assert_eq!(reserved_usdt(&store).await, 0.0);
    drop_sqlite(store, path).await;
}

/// 摘要有界：失败再多，明细也只留前 `LOCAL_CANCEL_ERROR_DETAIL_LIMIT` 条，计数如实。
#[tokio::test]
async fn local_cancel_isolation_error_summary_is_bounded_and_counts_every_failure() {
    let (store, path) = sqlite_store("bounded").await;
    let failing = LOCAL_CANCEL_ERROR_DETAIL_LIMIT + 4;
    let mut order = Vec::new();
    for index in 0..failing {
        let id = format!("bad-{index:02}");
        seed_exit(&store, &id).await;
        corrupt(&store, &id, Corruption::LegacyNullRole).await;
        order.push(id);
    }
    seed_exit(&store, "good-exit").await;
    order.push("good-exit".to_string());
    let order: Vec<&str> = order.iter().map(String::as_str).collect();

    let error = cancel_local_candidates(&store, SCOPE_KEY, &candidates(&order), 11)
        .await
        .expect_err("every failing order is counted");
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "{failing} order(s) failed, 1 canceled, 0 not attempted;"
        )),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "first {LOCAL_CANCEL_ERROR_DETAIL_LIMIT} failure(s)"
        )),
        "{message}"
    );
    assert_eq!(
        message.matches("[pending_risk]:").count(),
        LOCAL_CANCEL_ERROR_DETAIL_LIMIT,
        "{message}"
    );
    assert_eq!(order_status(&store, "good-exit").await.0, "canceled");
    drop_sqlite(store, path).await;
}

/// 数据库本身不可用不是「某一张单」的问题：候选查询失败整轮如实失败；
/// 循环内遇到连接级错误立即停手并报告未尝试数，不逐单空等。
#[tokio::test]
async fn local_cancel_isolation_store_unavailable_fails_the_round_without_draining() {
    let (store, path) = sqlite_store("unavailable").await;
    for id in ["exit-1", "exit-2", "exit-3"] {
        seed_exit(&store, id).await;
    }
    store.pool().close().await;

    let error = cancel_local_candidates(
        &store,
        SCOPE_KEY,
        &candidates(&["exit-1", "exit-2", "exit-3"]),
        11,
    )
    .await
    .expect_err("a closed pool cannot cancel anything");
    let message = error.to_string();
    assert!(
        message.contains("1 order(s) failed, 0 canceled, 2 not attempted (store unavailable)"),
        "{message}"
    );

    let error = apply_local_order_semantics(&store, SCOPE_KEY, Level::Flatten, 12)
        .await
        .expect_err("a failed candidate query fails the whole round");
    assert!(
        !error.to_string().contains("local cancel incomplete"),
        "the candidate query error must surface as-is: {error}"
    );
    let _ = std::fs::remove_file(path);
}

/// 生产定时入口：`spawn_action_retry_task`（`main` 起的就是它）在已闩锁的
/// scope 上逐轮重试。失败行在场时兄弟单仍被处置，失败行多轮之后依旧原样。
#[tokio::test]
async fn local_cancel_isolation_retry_task_progresses_siblings_despite_failing_row() {
    let (store, path) = sqlite_store("retry-task").await;
    seed_reservation(&store, "100").await;
    seed_exit(&store, "aaa-exit").await;
    seed_entry(&store, BAD_ENTRY, BTC).await;
    seed_exit(&store, "zzz-exit").await;
    corrupt(&store, BAD_ENTRY, Corruption::LegacyNullRole).await;
    let before = entry_fingerprint(&store, BAD_ENTRY).await;
    assert_candidate_set(
        &store,
        Level::Flatten,
        &[BAD_ENTRY],
        &["aaa-exit", "zzz-exit"],
    )
    .await;
    arm(
        &store,
        ArmRequest {
            scope_key: SCOPE_KEY,
            level: Level::Flatten,
            trigger_source: "manual_api",
            reason: "local cancel isolation",
            now_ms: 100,
        },
    )
    .await
    .expect("latch the scope");

    let task = spawn_action_retry_task(store.clone());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let done = order_status(&store, "aaa-exit").await.0 == "canceled"
            && order_status(&store, "zzz-exit").await.0 == "canceled";
        if done {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retry task never canceled the legitimate siblings"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // 再给它至少一轮重试机会：失败行不得被后续轮次改写或释放。
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    task.abort();
    let _ = task.await;
    assert_eq!(entry_fingerprint(&store, BAD_ENTRY).await, before);
    assert_eq!(reserved_usdt(&store).await, 100.0);
    drop_sqlite(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_local_cancel_isolation_flatten_legacy_null_role_and_malformed_entry() {
    const TEST: &str = "mysql_local_cancel_isolation_flatten_legacy_null_role_and_malformed_entry";
    for corruption in [Corruption::LegacyNullRole, Corruption::ExchangeIdentity] {
        let Some(scratch) = MySqlScratch::create("lcflatten", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;
        flatten_scenario(&store, corruption).await;
        store.pool().close().await;
        scratch.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_local_cancel_isolation_manage_only_releases_entry_siblings_exactly_once() {
    const TEST: &str =
        "mysql_local_cancel_isolation_manage_only_releases_entry_siblings_exactly_once";
    let Some(scratch) = MySqlScratch::create("lcmanage", TEST).await else {
        return;
    };
    let store = scratch.connect_store().await;
    manage_only_scenario(&store).await;
    store.pool().close().await;
    scratch.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_local_cancel_isolation_failures_first_in_explicit_order_still_reach_siblings() {
    const TEST: &str =
        "mysql_local_cancel_isolation_failures_first_in_explicit_order_still_reach_siblings";
    for order in [
        ["bad-null", BAD_ENTRY, "good-entry", "good-exit"],
        [BAD_ENTRY, "good-entry", "bad-null", "good-exit"],
        ["good-exit", "good-entry", BAD_ENTRY, "bad-null"],
    ] {
        let Some(scratch) = MySqlScratch::create("lcexplicit", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;
        explicit_order_scenario(&store, &order).await;
        store.pool().close().await;
        scratch.cleanup().await;
    }
}
