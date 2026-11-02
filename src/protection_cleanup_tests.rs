// D30 · 保护单善后义务的回归载体。
//
// 分两组：
// * 默认特性组：义务落库（同事务）、模拟凭据的平凡完成、入场闸门查询的
//   范围判别、重启续跑的持久性（SQLite 与真 MySQL 各一条）。
// * `l4-fault-injection` 组：仓内 SQLite 替身交易所（`l4_fake_exchange`）上的
//   端到端撤单 + 真值确认。**必须单独过滤运行**——它设置的 L4_* 进程级
//   环境变量会让同进程内其他走交易所客户端的用例也进替身（与 reconcile
//   的 h3 演练用例同一纪律）。

use super::*;
use crate::store::Store;
use sqlx::Row;

const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";

async fn scratch_store(prefix: &str) -> (Store, std::path::PathBuf) {
    static INSTALL_DRIVERS: std::sync::Once = std::sync::Once::new();
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let dir = std::env::temp_dir().join(format!("rustcore-{prefix}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).expect("create test directory");
    let store = Store::connect(&format!("sqlite://{}", dir.join("engine.db").display()))
        .await
        .expect("connect scratch store");
    (store, dir)
}

async fn cleanup_dir(store: &Store, dir: &std::path::PathBuf) {
    store.pool().close().await;
    let _ = std::fs::remove_dir_all(dir);
}

async fn seed_position(
    store: &Store,
    position_id: &str,
    credential_id: &str,
    bot_uid: &str,
    direction: &str,
    state: &str,
    qty: &str,
    symbol_key: &str,
) {
    let active_key = if state == "closed" {
        None
    } else {
        Some(format!("{bot_uid}|{symbol_key}|{direction}"))
    };
    sqlx::query(
        r#"
        INSERT INTO position_state (
          position_id, credential_id, bot_uid, symbol_key, direction,
          entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, 1)
        "#,
    )
    .bind(position_id)
    .bind(credential_id)
    .bind(bot_uid)
    .bind(symbol_key)
    .bind(direction)
    .bind(state)
    .bind(active_key)
    .bind(qty)
    .execute(store.pool())
    .await
    .expect("seed position");
}

async fn seed_protection(
    store: &Store,
    protection_order_id: &str,
    position_id: &str,
    credential_id: &str,
    kind: &str,
    engine_order_id: &str,
    exchange_algo_id: Option<&str>,
    client_algo_id: Option<&str>,
    status: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO protection_orders (
          protection_order_id, position_id, credential_id, kind, generation,
          engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id,
          trigger_price, reduce_only, close_position, status,
          verified_from_exchange, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, '95', 1, 1, ?, 0, 1, 1)
        "#,
    )
    .bind(protection_order_id)
    .bind(position_id)
    .bind(credential_id)
    .bind(kind)
    .bind(engine_order_id)
    .bind(exchange_algo_id)
    .bind(client_algo_id)
    .bind(status)
    .execute(store.pool())
    .await
    .expect("seed protection order");
}

async fn seed_engine_order(
    store: &Store,
    engine_order_id: &str,
    credential_id: &str,
    bot_uid: &str,
    position_id: &str,
    order_role: &str,
    status: &str,
    exchange_order_id: Option<&str>,
    client_order_id_short: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO engine_orders (
          engine_order_id, credential_id, bot_uid, idempotency_key,
          client_order_id_short, exchange_order_id, exchange, market_kind,
          symbol_key, action, side, order_type, quantity, reduce_only,
          status, order_role, position_id, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, 'binance', 'futures', ?, 'close_long', 'short',
                  'STOP_MARKET', '1', 1, ?, ?, ?, 1, 1)
        "#,
    )
    .bind(engine_order_id)
    .bind(credential_id)
    .bind(bot_uid)
    .bind(format!("idem_{engine_order_id}"))
    .bind(client_order_id_short)
    .bind(exchange_order_id)
    .bind(SYMBOL_KEY)
    .bind(status)
    .bind(order_role)
    .bind(position_id)
    .execute(store.pool())
    .await
    .expect("seed engine order");
}

/// agent 式平仓成交：与 WS fill 应用同一生产函数
/// （`apply_filled_order_truth` → `mark_position_closed`）。
async fn apply_exit_fill(
    store: &Store,
    credential_id: &str,
    bot_uid: &str,
    symbol_key: &str,
    qty: &str,
) {
    let mut tx = store.pool().begin().await.expect("begin exit fill tx");
    crate::position_state::apply_filled_order_truth(
        &mut tx,
        credential_id,
        bot_uid,
        symbol_key,
        "close_long",
        qty,
        Some("100"),
        1_000,
    )
    .await
    .expect("apply exit fill truth");
    tx.commit().await.expect("commit exit fill tx");
}

async fn obligation_state(store: &Store, position_id: &str) -> Option<(String, String, i64)> {
    sqlx::query(
        "SELECT state, COALESCE(complete_reason,'') AS complete_reason, attempts \
         FROM protection_cleanup_obligations WHERE position_id=?",
    )
    .bind(position_id)
    .fetch_optional(store.pool())
    .await
    .expect("load obligation")
    .map(|row| {
        (
            row.get("state"),
            row.get("complete_reason"),
            row.get("attempts"),
        )
    })
}

/// P0-1 生命周期键：义务行携带武装它的那次关闭写下的 `close_revision`。
async fn obligation_lifecycle(
    store: &Store,
    position_id: &str,
) -> Option<(String, String, i64, i64)> {
    sqlx::query(
        "SELECT state, COALESCE(complete_reason,'') AS complete_reason, attempts, close_revision \
         FROM protection_cleanup_obligations WHERE position_id=?",
    )
    .bind(position_id)
    .fetch_optional(store.pool())
    .await
    .expect("load obligation lifecycle")
    .map(|row| {
        (
            row.get("state"),
            row.get("complete_reason"),
            row.get("attempts"),
            row.get("close_revision"),
        )
    })
}

async fn position_state_revision(store: &Store, position_id: &str) -> i64 {
    sqlx::query("SELECT close_revision FROM position_state WHERE position_id=?")
        .bind(position_id)
        .fetch_one(store.pool())
        .await
        .expect("load position close_revision")
        .get("close_revision")
}

async fn obligation_row_count(store: &Store, position_id: &str) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM protection_cleanup_obligations WHERE position_id=?")
        .bind(position_id)
        .fetch_one(store.pool())
        .await
        .expect("count obligation rows")
        .get("n")
}

/// P0-1 生命周期键 + P2-3：义务绑定的是**一次**「非 closed → closed」的
/// 关闭生命周期，键 (position_id, close_revision)。
/// * 同一次关闭的重放（同 close_revision）是幂等 no-op；
/// * 对**已经 closed** 的行再执行一次关闭（MySQL 上 WS fill 快照早于
///   reconcile truth 关闭的竞态形状；`mark_position_closed` 本身无状态
///   守卫）不武装第二个生命周期、不报错、不重置第一个生命周期的进度。
/// 重开退役与重开后的再武装由 reconcile 的生产重开用例与 L4 回归用例覆盖。
#[tokio::test]
async fn replaying_a_close_or_reclosing_a_closed_position_keeps_one_lifecycle() {
    let (store, dir) = scratch_store("m0fixlife").await;
    let credential_id = "m0fix-life-cred";
    let bot_uid = "bot_m0fixlife";
    seed_position(
        &store,
        "pos_m0fixlife",
        credential_id,
        bot_uid,
        "long",
        "open",
        "1",
        SYMBOL_KEY,
    )
    .await;
    seed_protection(
        &store,
        "prot_m0fixlife_sl",
        "pos_m0fixlife",
        credential_id,
        "sl",
        "eo_m0fixlife_sl",
        Some("111"),
        Some("coid_m0fixlife_sl"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0fixlife_sl",
        credential_id,
        bot_uid,
        "pos_m0fixlife",
        "protection_sl",
        "confirmed",
        None,
        "coid_m0fixlife_sl",
    )
    .await;

    apply_exit_fill(&store, credential_id, bot_uid, SYMBOL_KEY, "1").await;
    let first_revision = position_state_revision(&store, "pos_m0fixlife").await;
    assert_eq!(
        obligation_lifecycle(&store, "pos_m0fixlife").await,
        Some(("pending".to_string(), String::new(), 0, first_revision)),
        "the close arms the obligation at the revision it wrote"
    );

    // 同一次关闭的重放：同 close_revision ⇒ no-op。
    let mut tx = store.pool().begin().await.expect("begin replay tx");
    crate::protection_cleanup::record_position_closed_in_tx(
        &mut tx,
        crate::store::DbKind::Sqlite,
        "pos_m0fixlife",
        "fill_truth",
        1_500,
    )
    .await
    .expect("replay the same close");
    tx.commit().await.expect("commit replay tx");
    assert_eq!(obligation_row_count(&store, "pos_m0fixlife").await, 1);
    assert_eq!(
        obligation_lifecycle(&store, "pos_m0fixlife").await,
        Some(("pending".to_string(), String::new(), 0, first_revision)),
        "a same-revision replay is an idempotent no-op"
    );

    // 让第一个生命周期带上进度（一次驱动尝试；无交易所密钥 ⇒ 如实挂起）。
    assert_eq!(drive_due_obligations(&store).await.expect("drive once"), 1);
    assert_eq!(
        obligation_lifecycle(&store, "pos_m0fixlife").await,
        Some(("pending".to_string(), String::new(), 1, first_revision))
    );

    // 再关一次已经 closed 的行（竞态里的第二个关闭写者）。
    let mut tx = store.pool().begin().await.expect("begin second close tx");
    crate::position_state::mark_position_closed(
        &mut tx,
        "pos_m0fixlife",
        2_000,
        None,
        "fill_truth",
    )
    .await
    .expect("re-closing an already-closed position must not fail");
    tx.commit().await.expect("commit second close tx");
    assert_eq!(obligation_row_count(&store, "pos_m0fixlife").await, 1);
    assert_eq!(
        obligation_lifecycle(&store, "pos_m0fixlife").await,
        Some(("pending".to_string(), String::new(), 1, first_revision)),
        "re-closing a closed position must not arm a second lifecycle or reset the first one"
    );
    cleanup_dir(&store, &dir).await;
}

/// **P0-1 预检的侧向映射**（与 `apply_exchange_position_truth` 同一份）：
/// 单向持仓 BOTH 按数量符号归属方向、零数量对两侧都成立；双向持仓
/// LONG/SHORT 各说各的；缺席、不可解析、未知 positionSide、非数组一律歧义；
/// 币本位同名合约不算这条 USDⓈ-M scope。
#[test]
fn venue_scope_exposure_uses_the_engine_side_mapping() {
    use serde_json::json;
    let row = |symbol: &str, qty: &str, side: Option<&str>| {
        let mut value = json!({"symbol": symbol, "positionAmt": qty});
        if let Some(side) = side {
            value["positionSide"] = json!(side);
        }
        value
    };
    let flat = VenueScopeExposure::Flat;
    let is_nonzero = |e: &VenueScopeExposure| matches!(e, VenueScopeExposure::NonZero(_));
    let is_ambiguous = |e: &VenueScopeExposure| matches!(e, VenueScopeExposure::Ambiguous(_));

    // 单向持仓。
    let one_way_flat = json!([
        row("BTCUSDT", "0", Some("BOTH")),
        row("ETHUSDT", "3", Some("BOTH"))
    ]);
    assert_eq!(
        venue_scope_exposure(&one_way_flat, SYMBOL_KEY, "long"),
        flat
    );
    assert_eq!(
        venue_scope_exposure(&one_way_flat, SYMBOL_KEY, "short"),
        flat
    );
    let one_way_long = json!([row("BTCUSDT", "0.010", Some("BOTH"))]);
    assert!(is_nonzero(&venue_scope_exposure(
        &one_way_long,
        SYMBOL_KEY,
        "long"
    )));
    assert_eq!(
        venue_scope_exposure(&one_way_long, SYMBOL_KEY, "short"),
        flat
    );
    let one_way_short = json!([row("BTCUSDT", "-2", None)]);
    assert!(is_nonzero(&venue_scope_exposure(
        &one_way_short,
        SYMBOL_KEY,
        "short"
    )));
    assert_eq!(
        venue_scope_exposure(&one_way_short, SYMBOL_KEY, "long"),
        flat
    );
    assert_eq!(
        venue_scope_exposure(&json!([row("BTCUSDT", "0.000", None)]), SYMBOL_KEY, "short"),
        flat,
        "a missing positionSide is BOTH"
    );

    // 双向持仓。
    let hedge_long = json!([
        row("BTCUSDT", "1", Some("LONG")),
        row("BTCUSDT", "0", Some("SHORT"))
    ]);
    assert!(is_nonzero(&venue_scope_exposure(
        &hedge_long,
        SYMBOL_KEY,
        "long"
    )));
    assert_eq!(venue_scope_exposure(&hedge_long, SYMBOL_KEY, "short"), flat);
    let hedge_short = json!([
        row("BTCUSDT", "0", Some("LONG")),
        row("BTCUSDT", "-1", Some("SHORT"))
    ]);
    assert_eq!(venue_scope_exposure(&hedge_short, SYMBOL_KEY, "long"), flat);
    assert!(is_nonzero(&venue_scope_exposure(
        &hedge_short,
        SYMBOL_KEY,
        "short"
    )));
    let only_long_row = json!([row("BTCUSDT", "0", Some("LONG"))]);
    assert!(
        is_ambiguous(&venue_scope_exposure(&only_long_row, SYMBOL_KEY, "short")),
        "no row describes the short side"
    );

    // 歧义。
    for (positions, why) in [
        (json!([row("ETHUSDT", "0", Some("BOTH"))]), "symbol absent"),
        (
            json!([row("BTCUSD_PERP", "0", Some("BOTH"))]),
            "coin-margined namesake",
        ),
        (
            json!([row("BTCUSDT", "abc", Some("BOTH"))]),
            "unparseable quantity",
        ),
        (json!([{"symbol": "BTCUSDT"}]), "missing quantity"),
        (
            json!([row("BTCUSDT", "0", Some("SIDEWAYS"))]),
            "unknown positionSide",
        ),
        (
            json!({"symbol": "BTCUSDT", "positionAmt": "0"}),
            "not an array",
        ),
        (json!([]), "empty response"),
    ] {
        assert!(
            is_ambiguous(&venue_scope_exposure(&positions, SYMBOL_KEY, "long")),
            "{why} must be ambiguous"
        );
    }
    assert!(is_ambiguous(&venue_scope_exposure(
        &one_way_flat,
        SYMBOL_KEY,
        ""
    )));
}

/// **P1-1 的宽限判据**：与 reconcile 裁决未确认订单同一常量、同一边界
/// （距上次状态推进 ≥ 宽限才可裁决）；有交易所单号或不在途的单不适用。
#[test]
fn request_identity_grace_boundary_matches_reconcile() {
    let grace = crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS;
    let advanced_at = 1_800_000_000_000_i64;
    assert!(request_identity_not_found_within_grace(
        false,
        true,
        advanced_at,
        advanced_at
    ));
    assert!(request_identity_not_found_within_grace(
        false,
        true,
        advanced_at,
        advanced_at + grace - 1
    ));
    assert!(!request_identity_not_found_within_grace(
        false,
        true,
        advanced_at,
        advanced_at + grace
    ));
    assert!(
        !request_identity_not_found_within_grace(true, true, advanced_at, advanced_at),
        "an exchange-acknowledged identity gets no grace"
    );
    assert!(
        !request_identity_not_found_within_grace(false, false, advanced_at, advanced_at),
        "a row that is not in flight gets no grace"
    );
    for status in ["pending_risk", "pending_submit", "unknown", "submitted"] {
        assert!(
            REQUEST_IN_FLIGHT_ENGINE_STATUSES.contains(&status),
            "{status}"
        );
    }
    for status in [
        "acked",
        "confirmed",
        "canceled",
        "filled",
        "failed",
        "rejected",
        "expired",
    ] {
        assert!(
            !REQUEST_IN_FLIGHT_ENGINE_STATUSES.contains(&status),
            "{status}"
        );
    }
}

/// SQLite 上「`EXPLAIN QUERY PLAN`」的 detail 列。
async fn sqlite_query_plan(
    store: &Store,
    sql: &str,
    text_params: usize,
    int_params: usize,
) -> Vec<String> {
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query(&explain);
    for _ in 0..text_params {
        query = query.bind("m0fix-plan");
    }
    for _ in 0..int_params {
        query = query.bind(0i64);
    }
    query
        .fetch_all(store.pool())
        .await
        .expect("explain the production query")
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect()
}

/// **P2-5**：入场闸门与 1 Hz 到期扫描的谓词必须**能用上**各自的索引
/// （此前 `HEX(列)` 包住了索引列，两条都是全表扫描，而义务表只增不删）。
/// 用的是生产代码执行的**同一份 SQL 常量**。
#[tokio::test]
async fn gate_and_due_scan_predicates_can_use_their_indexes_on_sqlite() {
    let (store, dir) = scratch_store("m0fixplan").await;
    let gate_params = ENTRY_SCOPE_PENDING_SQL.matches('?').count();
    let gate_plan = sqlite_query_plan(&store, ENTRY_SCOPE_PENDING_SQL, gate_params, 0).await;
    assert!(
        gate_plan
            .iter()
            .any(|detail| detail.contains("protection_cleanup_pending_scope_idx")),
        "the admission gate must be able to use the scope index: {gate_plan:?}"
    );
    let scan_plan = sqlite_query_plan(&store, DUE_SCAN_SQL, 0, 1).await;
    assert!(
        scan_plan
            .iter()
            .any(|detail| detail.contains("protection_cleanup_due_scan_idx")),
        "the due scan must be able to use the due-scan index: {scan_plan:?}"
    );
    cleanup_dir(&store, &dir).await;
}

/// ① 缺陷原样：平仓成交使仓位归零后，同一事务里必须已经落下善后义务。
///（接线前红：没有义务行。）
#[tokio::test]
async fn exit_fill_close_writes_a_cleanup_obligation_in_the_same_transaction() {
    let (store, dir) = scratch_store("m0create").await;
    let credential_id = "m0-create-cred";
    let bot_uid = "bot_m0create";
    seed_position(&store, "pos_m0create", credential_id, bot_uid, "long", "open", "1", SYMBOL_KEY)
        .await;
    seed_protection(
        &store,
        "prot_m0create_sl",
        "pos_m0create",
        credential_id,
        "sl",
        "eo_m0create_sl",
        Some("101"),
        Some("coid_m0create_sl"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0create_sl",
        credential_id,
        bot_uid,
        "pos_m0create",
        "protection_sl",
        "confirmed",
        None,
        "coid_m0create_sl",
    )
    .await;

    apply_exit_fill(&store, credential_id, bot_uid, SYMBOL_KEY, "1").await;

    let position: String = sqlx::query("SELECT state FROM position_state WHERE position_id='pos_m0create'")
        .fetch_one(store.pool())
        .await
        .expect("load position")
        .get("state");
    assert_eq!(position, "closed", "the exit fill must close the position");
    let obligation = obligation_state(&store, "pos_m0create").await;
    assert_eq!(
        obligation.as_ref().map(|(state, _, _)| state.as_str()),
        Some("pending"),
        "closing a position with live protection must durably record a cleanup obligation in the same transaction"
    );
    let (_, _, attempts) = obligation.expect("obligation row");
    assert_eq!(attempts, 0, "a fresh obligation has not been driven yet");
    cleanup_dir(&store, &dir).await;
}

/// 交易所仓位真值归零（reconcile / WS account 更新走的那条）同样落义务。
#[tokio::test]
async fn exchange_position_truth_zero_writes_a_cleanup_obligation() {
    let (store, dir) = scratch_store("m0truth").await;
    let credential_id = "m0-truth-cred";
    seed_position(&store, "pos_m0truth", credential_id, "bot_m0truth", "long", "open", "1", SYMBOL_KEY)
        .await;
    seed_protection(
        &store,
        "prot_m0truth_tp",
        "pos_m0truth",
        credential_id,
        "tp",
        "eo_m0truth_tp",
        Some("102"),
        Some("coid_m0truth_tp"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0truth_tp",
        credential_id,
        "bot_m0truth",
        "pos_m0truth",
        "protection_tp",
        "confirmed",
        None,
        "coid_m0truth_tp",
    )
    .await;

    let mut tx = store.pool().begin().await.expect("begin truth tx");
    crate::position_state::apply_exchange_position_truth(
        &mut tx,
        credential_id,
        "BTCUSDT",
        Some("LONG"),
        "0",
        None,
        2_000,
    )
    .await
    .expect("apply zero exchange truth");
    tx.commit().await.expect("commit truth tx");

    assert_eq!(
        obligation_state(&store, "pos_m0truth").await.as_ref().map(|(state, _, _)| state.as_str()),
        Some("pending"),
        "venue-zero truth must durably record the cleanup obligation"
    );
    cleanup_dir(&store, &dir).await;
}

/// 需求⑤：非 exchange-backed 档位（paper/simulation）没有任何 venue 侧
/// 保护单 ⇒ 义务**在关闭事务内**以 `paper_run_no_venue_protection` 平凡
/// 完成并记录原因——不静默跳过，也不把同 scope 的再开仓挡在门外（B3
/// 重开语义不受影响）。本例用无 provenance 的模拟凭据（回落分支）；
/// `paper` run_mode + 交易所形凭据 id 的主分支由既有 svc/B3 用例覆盖。
#[tokio::test]
async fn paper_close_completes_the_simulation_obligation_trivially_in_transaction() {
    let (store, dir) = scratch_store("m0paper").await;
    let credential_id = "paper:0123456789abcdef0123456789abcdef";
    seed_position(&store, "pos_m0paper", credential_id, "bot_m0paper", "long", "open", "1", SYMBOL_KEY)
        .await;
    // 本地保护行（requested、无任何 venue 身份）：模拟路径的真实形状。
    seed_protection(
        &store,
        "prot_m0paper_sl",
        "pos_m0paper",
        credential_id,
        "sl",
        "eo_m0paper_sl",
        None,
        None,
        "requested",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0paper_sl",
        credential_id,
        "bot_m0paper",
        "pos_m0paper",
        "protection_sl",
        "filled",
        None,
        "coid_m0paper_sl",
    )
    .await;

    // paper 平仓：mark_paper_filled 的 close 分支（同一生产函数）。
    let mut tx = store.pool().begin().await.expect("begin paper close tx");
    crate::position_state::mark_paper_filled(
        &mut tx,
        &crate::position_state::PreparedPositionIntent {
            authority_idem_key: "idem_m0paper_exit".to_string(),
            position_id: Some("pos_m0paper".to_string()),
            direction: Some("long".to_string()),
            orphan_recovery: false,
        },
        &crate::proto::OrderIntent {
            bot_uid: "bot_m0paper".to_string(),
            run_id: "run_m0paper".to_string(),
            cycle_id: "cycle_m0paper".to_string(),
            idempotency_key: "idem_m0paper_exit".to_string(),
            client_order_id_short: "coid_m0paper_exit".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "close_long".to_string(),
            side: "long".to_string(),
            order_type: "market".to_string(),
            price: "100".to_string(),
            quantity: "1".to_string(),
            reduce_only: true,
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        },
        "100",
        3_000,
    )
    .await
    .expect("apply paper close");
    tx.commit().await.expect("commit paper close");

    assert_eq!(
        obligation_state(&store, "pos_m0paper").await,
        Some(("complete".to_string(), "paper_run_no_venue_protection".to_string(), 0)),
        "a paper-run close with no venue-side protection must complete the obligation \
         in-transaction with a recorded reason"
    );

    // 再开仓不被挡：同 scope 的 entry 准入查询必须为 false。
    let mut tx = store.pool().begin().await.expect("begin entry probe tx");
    let pending = entry_scope_pending_in_tx(&mut tx, credential_id, SYMBOL_KEY, "long")
        .await
        .expect("probe entry scope");
    tx.commit().await.expect("commit entry probe");
    assert!(!pending, "a trivially-complete simulation obligation must not gate re-entry");
    cleanup_dir(&store, &dir).await;
}

/// 需求③的范围判别（闸门查询本身）：同 credential+symbol+direction 挡，
/// 反方向、别的 symbol、已完成的义务都不挡；同 scope 的平仓不受影响。
#[tokio::test]
async fn entry_scope_pending_matches_only_the_exact_pending_scope() {
    let (store, dir) = scratch_store("m0scope").await;
    let credential_id = "m0-scope-cred";
    seed_position(&store, "pos_m0scope", credential_id, "bot_m0scope", "long", "open", "1", SYMBOL_KEY)
        .await;
    seed_protection(
        &store,
        "prot_m0scope_sl",
        "pos_m0scope",
        credential_id,
        "sl",
        "eo_m0scope_sl",
        Some("103"),
        Some("coid_m0scope_sl"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0scope_sl",
        credential_id,
        "bot_m0scope",
        "pos_m0scope",
        "protection_sl",
        "confirmed",
        None,
        "coid_m0scope_sl",
    )
    .await;
    apply_exit_fill(&store, credential_id, "bot_m0scope", SYMBOL_KEY, "1").await;
    assert!(
        obligation_state(&store, "pos_m0scope").await.is_some(),
        "fixture: the close created an obligation"
    );
    // 仍 open 的第二仓位（在开事务**之前**种好：SQLite 单写者，别在持事务
    // 时再走池写）。
    seed_position(&store, "pos_m0scope2", credential_id, "bot_m0scope2", "long", "open", "1", SYMBOL_KEY)
        .await;

    let mut tx = store.pool().begin().await.expect("begin scope tx");
    for (symbol_key, direction, expected) in [
        (SYMBOL_KEY, "long", true),
        (SYMBOL_KEY, "short", false),
        ("binance|futures|ETH|USDT|USDT", "long", false),
    ] {
        let pending = entry_scope_pending_in_tx(&mut tx, credential_id, symbol_key, direction)
            .await
            .expect("probe scope");
        assert_eq!(pending, expected, "scope probe {symbol_key}/{direction}");
    }
    // 别的凭据同名 scope 不挡。
    let other = entry_scope_pending_in_tx(&mut tx, "m0-scope-other", SYMBOL_KEY, "long")
        .await
        .expect("probe other credential");
    assert!(!other, "another credential's scope is untouched");

    // 同 scope 的 reduce-only 平仓准备不受影响：prepare close 仍 Ready。
    let close2 = crate::proto::OrderIntent {
        bot_uid: "bot_m0scope2".to_string(),
        run_id: "run_m0scope".to_string(),
        cycle_id: "cycle_m0scope2".to_string(),
        idempotency_key: "idem_m0scope2_close".to_string(),
        client_order_id_short: "coid_m0scope2_close".to_string(),
        exchange: "binance".to_string(),
        market_kind: "futures".to_string(),
        symbol_key: SYMBOL_KEY.to_string(),
        action: "close_long".to_string(),
        side: "long".to_string(),
        order_type: "market".to_string(),
        price: "100".to_string(),
        quantity: String::new(),
        reduce_only: true,
        max_position_notional: 0.0,
        max_credential_notional: 0.0,
    };
    match crate::position_state::prepare_intent(&mut tx, credential_id, &close2, 4_000)
        .await
        .expect("prepare the reduce-only close")
    {
        crate::position_state::PrepareOutcome::Ready(_) => {}
        other => panic!("a reduce-only close must never be gated: {other:?}"),
    }
    tx.commit().await.expect("commit scope tx");
    cleanup_dir(&store, &dir).await;
}

/// 需求④（重启续跑，SQLite）：义务只活在库里。关掉连接池、从同一文件
/// 重开（等价于一次进程重启）后，扫描驱动继续把它做完。
#[tokio::test]
async fn pending_obligation_survives_a_store_restart_and_completes() {
    let (store, dir) = scratch_store("m0restart").await;
    let credential_id = "m0-restart-cred";
    seed_position(&store, "pos_m0restart", credential_id, "bot_m0restart", "long", "open", "1", SYMBOL_KEY)
        .await;
    // 没有任何保护单：重启后驱动应立即以 no_protection_orders 完成——
    // 完成状态必须由重启后的那次扫描写入，而不是创建时。
    apply_exit_fill(&store, credential_id, "bot_m0restart", SYMBOL_KEY, "1").await;
    assert_eq!(
        obligation_state(&store, "pos_m0restart").await.as_ref().map(|(s, _, _)| s.as_str()),
        Some("pending"),
        "fixture: the obligation starts pending"
    );

    let db_path = dir.join("engine.db");
    store.pool().close().await;
    let reopened = Store::connect(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("reopen the store after the simulated restart");
    let driven = drive_due_obligations(&reopened)
        .await
        .expect("drive after restart");
    assert_eq!(driven, 1, "the restarted scan must pick the durable obligation up");
    assert_eq!(
        obligation_state(&reopened, "pos_m0restart").await,
        Some(("complete".to_string(), "no_protection_orders".to_string(), 1)),
        "the obligation must complete only after the post-restart drive"
    );
    cleanup_dir(&reopened, &db_path.parent().expect("parent dir").to_path_buf()).await;
}

/// 需求④/②（真 MySQL）：义务持久化、同事务落库与闸门查询在 MySQL 方言下
/// 同样成立。**不拿 SQLite 冒充**（REQUIRE=1 时缺真库即失败）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_close_writes_and_gates_the_cleanup_obligation() {
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "m0cleanup",
        "mysql_close_writes_and_gates_the_cleanup_obligation",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let credential_id = "m0-mysql-cred";
    seed_position(&store, "pos_m0mysql", credential_id, "bot_m0mysql", "long", "open", "1", SYMBOL_KEY)
        .await;
    seed_protection(
        &store,
        "prot_m0mysql_sl",
        "pos_m0mysql",
        credential_id,
        "sl",
        "eo_m0mysql_sl",
        Some("104"),
        Some("coid_m0mysql_sl"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0mysql_sl",
        credential_id,
        "bot_m0mysql",
        "pos_m0mysql",
        "protection_sl",
        "confirmed",
        None,
        "coid_m0mysql_sl",
    )
    .await;

    apply_exit_fill(&store, credential_id, "bot_m0mysql", SYMBOL_KEY, "1").await;
    assert_eq!(
        obligation_state(&store, "pos_m0mysql").await.as_ref().map(|(s, _, _)| s.as_str()),
        Some("pending"),
        "MySQL: the close transaction must persist the obligation"
    );

    let mut tx = store.pool().begin().await.expect("begin mysql gate tx");
    let gated = entry_scope_pending_in_tx(&mut tx, credential_id, SYMBOL_KEY, "long")
        .await
        .expect("mysql gate probe");
    let other_direction =
        entry_scope_pending_in_tx(&mut tx, credential_id, SYMBOL_KEY, "short")
            .await
            .expect("mysql gate probe short");
    tx.commit().await.expect("commit mysql gate tx");
    assert!(gated, "MySQL: the same scope is gated");
    assert!(!other_direction, "MySQL: the opposite direction is not gated");

    // 驱动一次（无凭据环境 ⇒ 如实挂起并退避），验证 attempts/退避在 MySQL
    // 上持久，而不是内存态。
    let driven = drive_due_obligations(&store).await.expect("mysql drive");
    assert_eq!(driven, 1);
    let row = sqlx::query(
        "SELECT state, attempts, next_attempt_at_ms, attempt_outcomes_json \
         FROM protection_cleanup_obligations WHERE position_id='pos_m0mysql'",
    )
    .fetch_one(store.pool())
    .await
    .expect("load mysql obligation");
    assert_eq!(row.get::<String, _>("state"), "pending");
    assert_eq!(row.get::<i64, _>("attempts"), 1);
    assert!(
        row.get::<i64, _>("next_attempt_at_ms") > 0,
        "the failed attempt must durably schedule a retry"
    );
    let outcomes = crate::store::try_get_nullable_bytes(&row, "attempt_outcomes_json")
        .expect("decode attempt outcomes column");
    assert!(
        outcomes
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        "the attempt outcome must be recorded"
    );

    store.pool().close().await;
    scratch.cleanup().await;
}

/// **P2-3（真 MySQL）**：WS fill 事务的一致性快照早于一次 reconcile truth
/// 关闭——它读到的仍是 open 行，随后对已被关闭的行再执行一次关闭。此前
/// 义务存在性检查是一致性读、看不到 truth 事务已提交的义务行，INSERT 撞
/// 主键，fill 事务失败（WsApplyError → 断流回补）。修复后：第二个关闭
/// 写者成功提交，义务仍只有 truth 那一次关闭武装的那一行。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_a_fill_reclosing_a_position_closed_by_truth_keeps_one_obligation() {
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "m0fixrace",
        "mysql_a_fill_reclosing_a_position_closed_by_truth_keeps_one_obligation",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let credential_id = "m0fix-race-cred";
    let bot_uid = "bot_m0fixrace";
    seed_position(
        &store,
        "pos_m0fixrace",
        credential_id,
        bot_uid,
        "long",
        "open",
        "1",
        SYMBOL_KEY,
    )
    .await;
    seed_protection(
        &store,
        "prot_m0fixrace_sl",
        "pos_m0fixrace",
        credential_id,
        "sl",
        "eo_m0fixrace_sl",
        Some("121"),
        Some("coid_m0fixrace_sl"),
        "confirmed",
    )
    .await;
    seed_engine_order(
        &store,
        "eo_m0fixrace_sl",
        credential_id,
        bot_uid,
        "pos_m0fixrace",
        "protection_sl",
        "confirmed",
        None,
        "coid_m0fixrace_sl",
    )
    .await;

    // fill 事务先建立 REPEATABLE READ 快照（此刻仓位 open）。
    let mut fill_tx = store.pool().begin().await.expect("begin fill tx");
    let snapshot_state: String =
        sqlx::query("SELECT state FROM position_state WHERE position_id='pos_m0fixrace'")
            .fetch_one(&mut *fill_tx)
            .await
            .expect("establish the fill snapshot")
            .get("state");
    assert_eq!(snapshot_state, "open");

    // reconcile truth 事务把仓位写到零并武装义务，先提交。
    let mut truth_tx = store.pool().begin().await.expect("begin truth tx");
    crate::position_state::apply_exchange_position_truth(
        &mut truth_tx,
        credential_id,
        "BTCUSDT",
        Some("LONG"),
        "0",
        None,
        2_000,
    )
    .await
    .expect("apply zero truth");
    truth_tx.commit().await.expect("commit truth tx");
    let truth_revision = position_state_revision(&store, "pos_m0fixrace").await;

    // fill 事务继续：快照里仍是 open/qty=1 ⇒ 再关一次。
    crate::position_state::apply_filled_order_truth(
        &mut fill_tx,
        credential_id,
        bot_uid,
        SYMBOL_KEY,
        "close_long",
        "1",
        Some("100"),
        3_000,
    )
    .await
    .expect("a fill whose snapshot predates the truth close must not fail on the obligation key");
    fill_tx.commit().await.expect("commit fill tx");

    assert_eq!(obligation_row_count(&store, "pos_m0fixrace").await, 1);
    assert_eq!(
        obligation_lifecycle(&store, "pos_m0fixrace").await,
        Some(("pending".to_string(), String::new(), 0, truth_revision)),
        "the obligation stays the one armed by the close that actually closed the position"
    );
    store.pool().close().await;
    scratch.cleanup().await;
}

/// **P2-5（真 MySQL）**：同一份生产 SQL 的 `possible_keys` 必须含对应索引
/// （`HEX(列)` 谓词时为 NULL：优化器根本不能考虑索引）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_gate_and_due_scan_predicates_can_use_their_indexes() {
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "m0fixplan",
        "mysql_gate_and_due_scan_predicates_can_use_their_indexes",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    for index in 0..8 {
        seed_position(
            &store,
            &format!("pos_m0fixplan{index}"),
            "m0fix-plan-cred",
            &format!("bot_m0fixplan{index}"),
            "long",
            "open",
            "1",
            SYMBOL_KEY,
        )
        .await;
        apply_exit_fill(
            &store,
            "m0fix-plan-cred",
            &format!("bot_m0fixplan{index}"),
            SYMBOL_KEY,
            "1",
        )
        .await;
    }
    // EXPLAIN 走文本协议：预处理语句形式拿不到 EXPLAIN 的结果列元数据。
    // 占位符按位置换成固定字面量（值不影响 possible_keys）。
    async fn possible_keys(store: &Store, sql: &str, literal: &str) -> String {
        let explain = format!("EXPLAIN {}", sql.replace('?', literal));
        let rows = sqlx::raw_sql(&explain)
            .fetch_all(store.pool())
            .await
            .expect("explain the production query");
        rows.iter()
            .map(|row| {
                crate::store::try_get_text(row, "possible_keys")
                    .expect("decode possible_keys")
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join(",")
    }
    let gate_keys = possible_keys(&store, ENTRY_SCOPE_PENDING_SQL, "'m0fix-plan-cred'").await;
    assert!(
        gate_keys.contains("protection_cleanup_pending_scope_idx"),
        "MySQL: the admission gate must be able to use the scope index (possible_keys={gate_keys:?})"
    );
    let scan_keys = possible_keys(&store, DUE_SCAN_SQL, "0").await;
    assert!(
        scan_keys.contains("protection_cleanup_due_scan_idx"),
        "MySQL: the due scan must be able to use the due-scan index (possible_keys={scan_keys:?})"
    );
    // 索引等值之外仍字节精确：默认 _ci collation 下的大小写变体不命中闸门。
    let mut tx = store.pool().begin().await.expect("begin gate tx");
    for (credential, symbol_key, direction, expected) in [
        ("m0fix-plan-cred", SYMBOL_KEY, "long", true),
        ("M0FIX-PLAN-CRED", SYMBOL_KEY, "long", false),
        (
            "m0fix-plan-cred",
            "Binance|futures|BTC|USDT|USDT",
            "long",
            false,
        ),
        ("m0fix-plan-cred", SYMBOL_KEY, "LONG", false),
    ] {
        assert_eq!(
            entry_scope_pending_in_tx(&mut tx, credential, symbol_key, direction)
                .await
                .expect("gate probe"),
            expected,
            "MySQL gate must stay byte-exact for {credential}/{symbol_key}/{direction}"
        );
    }
    tx.commit().await.expect("commit gate tx");
    let due = sqlx::query(DUE_SCAN_SQL)
        .bind(i64::MAX)
        .fetch_all(store.pool())
        .await
        .expect("run the due scan");
    assert_eq!(
        due.len(),
        8,
        "the due scan still returns every pending obligation"
    );
    store.pool().close().await;
    scratch.cleanup().await;
}

#[cfg(all(test, feature = "l4-fault-injection"))]
mod fake_venue_tests {
    use super::*;
    use sqlx::SqlitePool;

    struct L4Env {
        names: Vec<String>,
    }

    fn set_l4_env(fake_path: &std::path::Path, root: &std::path::Path, credential_id: &str) -> L4Env {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let names = [
            "L4_FAULT_INJECTION_ENABLED".to_string(),
            "L4_FAKE_EXCHANGE_DB".to_string(),
            "L4_FILE_RPC_DIR".to_string(),
            crate::venue::venue_env_name(credential_id),
            key_env,
            secret_env,
        ]
        .into_iter()
        .collect::<Vec<_>>();
        for (name, value) in [
            ("L4_FAULT_INJECTION_ENABLED", "1".to_string()),
            ("L4_FAKE_EXCHANGE_DB", format!("{}", fake_path.display())),
            ("L4_FILE_RPC_DIR", format!("{}", root.display())),
            (names[3].as_str(), "testnet".to_string()),
            (names[4].as_str(), "m0-l4-key".to_string()),
            (names[5].as_str(), "m0-l4-secret".to_string()),
        ] {
            std::env::set_var(name, value);
        }
        L4Env { names }
    }

    impl Drop for L4Env {
        fn drop(&mut self) {
            for name in self.names.drain(..) {
                std::env::remove_var(name);
            }
        }
    }

    async fn seed_fake_venue(path: &std::path::Path) -> SqlitePool {
        std::fs::File::create(path).expect("create fake exchange database");
        let pool = SqlitePool::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect fake exchange database");
        for ddl in [
            "CREATE TABLE control (key TEXT PRIMARY KEY, value INTEGER NOT NULL)",
            "CREATE TABLE positions (symbol TEXT PRIMARY KEY, quantity TEXT NOT NULL)",
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY AUTOINCREMENT, client_order_id TEXT NOT NULL DEFAULT '', symbol TEXT NOT NULL DEFAULT 'BTCUSDT', status TEXT NOT NULL DEFAULT 'NEW', side TEXT NOT NULL DEFAULT 'BUY', orig_qty TEXT NOT NULL DEFAULT '0', executed_qty TEXT NOT NULL DEFAULT '0', order_type TEXT NOT NULL DEFAULT 'MARKET', update_time INTEGER NOT NULL DEFAULT 0, is_open INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE algo_orders (algo_id INTEGER PRIMARY KEY AUTOINCREMENT, client_algo_id TEXT NOT NULL DEFAULT '', symbol TEXT NOT NULL DEFAULT 'BTCUSDT', algo_status TEXT NOT NULL DEFAULT 'NEW', side TEXT NOT NULL DEFAULT 'SELL', orig_qty TEXT NOT NULL DEFAULT '0', executed_qty TEXT NOT NULL DEFAULT '0', order_type TEXT NOT NULL DEFAULT 'STOP_MARKET', update_time INTEGER NOT NULL DEFAULT 0, is_open INTEGER NOT NULL DEFAULT 0, actual_order_id TEXT)",
            "CREATE TABLE request_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, method TEXT NOT NULL, path TEXT NOT NULL, order_type TEXT NOT NULL DEFAULT '')",
        ] {
            sqlx::query(ddl).execute(&pool).await.expect("fake venue ddl");
        }
        sqlx::query("INSERT INTO control(key, value) VALUES ('next_order_id', 200), ('next_algo_id', 100), ('fail_next_sl', 0)")
            .execute(&pool)
            .await
            .expect("seed fake control");
        // P0-1：驱动器撤单前读 positionRisk，只有该 scope 精确为零才撤。
        // 默认替身账户在 BTCUSDT 上是平的；需要非零/缺席的用例自行改写。
        sqlx::query("INSERT INTO positions(symbol, quantity) VALUES ('BTCUSDT', '0')")
            .execute(&pool)
            .await
            .expect("seed the flat venue position");
        pool
    }

    async fn seed_fake_algo(pool: &SqlitePool, algo_id: i64, client_algo_id: &str) {
        sqlx::query(
            "INSERT INTO algo_orders(algo_id, client_algo_id, algo_status, is_open, update_time) \
             VALUES (?, ?, 'NEW', 1, 1)",
        )
        .bind(algo_id)
        .bind(client_algo_id)
        .execute(pool)
        .await
        .expect("seed fake algo order");
    }

    async fn seed_fake_order(pool: &SqlitePool, order_id: i64, client_order_id: &str) {
        sqlx::query(
            "INSERT INTO orders(order_id, client_order_id, status, is_open) VALUES (?, ?, 'NEW', 1)",
        )
        .bind(order_id)
        .bind(client_order_id)
        .execute(pool)
        .await
        .expect("seed fake ordinary order");
    }

    async fn fake_algo_status(pool: &SqlitePool, algo_id: i64) -> (String, i64) {
        let row = sqlx::query("SELECT algo_status, is_open FROM algo_orders WHERE algo_id=?")
            .bind(algo_id)
            .fetch_one(pool)
            .await
            .expect("load fake algo");
        (row.get("algo_status"), row.get("is_open"))
    }

    async fn fake_order_status(pool: &SqlitePool, order_id: i64) -> (String, i64) {
        let row = sqlx::query("SELECT status, is_open FROM orders WHERE order_id=?")
            .bind(order_id)
            .fetch_one(pool)
            .await
            .expect("load fake order");
        (row.get("status"), row.get("is_open"))
    }

    async fn request_log(pool: &SqlitePool) -> Vec<(String, String)> {
        sqlx::query("SELECT method, path FROM request_log ORDER BY seq")
            .fetch_all(pool)
            .await
            .expect("read fake request log")
            .into_iter()
            .map(|row: sqlx::sqlite::SqliteRow| (row.get("method"), row.get("path")))
            .collect()
    }

    /// 完整夹具：一个 exchange-backed 仓位 + Algo SL/TP + 一条普通域
    /// reduce-only 保护子单（无 po 链接的 protection_tp 引擎单，pre-0013 /
    /// actualOrderId 形状），全部在替身上活着。
    async fn seed_closed_position_with_live_protection(store: &Store, credential_id: &str) {
        let bot_uid = "bot_m0l4";
        seed_position(store, "pos_m0l4", credential_id, bot_uid, "long", "open", "1", SYMBOL_KEY)
            .await;
        seed_protection(
            store,
            "prot_m0l4_sl",
            "pos_m0l4",
            credential_id,
            "sl",
            "eo_m0l4_sl",
            Some("101"),
            Some("coid_m0l4_sl"),
            "confirmed",
        )
        .await;
        seed_engine_order(
            store,
            "eo_m0l4_sl",
            credential_id,
            bot_uid,
            "pos_m0l4",
            "protection_sl",
            "confirmed",
            None,
            "coid_m0l4_sl",
        )
        .await;
        seed_protection(
            store,
            "prot_m0l4_tp",
            "pos_m0l4",
            credential_id,
            "tp",
            "eo_m0l4_tp",
            Some("102"),
            Some("coid_m0l4_tp"),
            "confirmed",
        )
        .await;
        seed_engine_order(
            store,
            "eo_m0l4_tp",
            credential_id,
            bot_uid,
            "pos_m0l4",
            "protection_tp",
            "confirmed",
            None,
            "coid_m0l4_tp",
        )
        .await;
        // 普通域子单：无 protection_orders 链接（legacy/actualOrderId 形状）。
        seed_engine_order(
            store,
            "eo_m0l4_child",
            credential_id,
            bot_uid,
            "pos_m0l4",
            "protection_tp",
            "submitted",
            Some("201"),
            "coid_m0l4_child",
        )
        .await;
    }

    /// 需求①+②（必测 a）：agent 式平仓成交使仓位归零后，替身交易所上的
    /// Algo SL/TP 与普通域子单都被撤销，且义务**只在真值确认之后**完成。
    #[tokio::test]
    async fn exit_fill_close_cancels_algo_then_ordinary_protection_and_completes_via_truth() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0drive").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        seed_fake_algo(&venue, 102, "coid_m0l4_tp").await;
        seed_fake_order(&venue, 201, "coid_m0l4_child").await;

        let credential_id = "m0-drive-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);

        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await.as_ref().map(|(s, _, _)| s.as_str()),
            Some("pending"),
            "fixture: the close created a pending obligation"
        );

        let driven = drive_due_obligations(&store).await.expect("drive the obligation");
        assert_eq!(driven, 1);

        // 替身交易所：两张 Algo 与那条普通子单都终局撤销。
        assert_eq!(fake_algo_status(&venue, 101).await, ("CANCELED".to_string(), 0));
        assert_eq!(fake_algo_status(&venue, 102).await, ("CANCELED".to_string(), 0));
        assert_eq!(fake_order_status(&venue, 201).await, ("CANCELED".to_string(), 0));

        // 本地权威表同样收敛到终态（真值写入，不是撤单回执）。
        for po in ["prot_m0l4_sl", "prot_m0l4_tp"] {
            let status: String =
                sqlx::query("SELECT status FROM protection_orders WHERE protection_order_id=?")
                    .bind(po)
                    .fetch_one(store.pool())
                    .await
                    .expect("load protection row")
                    .get("status");
            assert_eq!(status, "canceled", "{po} must be truth-terminated");
        }
        for eo in ["eo_m0l4_sl", "eo_m0l4_tp", "eo_m0l4_child"] {
            let status: String =
                sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                    .bind(eo)
                    .fetch_one(store.pool())
                    .await
                    .expect("load engine order")
                    .get("status");
            assert_eq!(status, "canceled", "{eo} must be truth-terminated");
        }

        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 1)),
            "the obligation completes only after truth confirmation"
        );

        // L3 同序：Algo 撤在普通撤之前；且每次撤之后都有一次真值查询
        //（GET algoOrder/order）——「撤单请求被接受」绝不是完成。
        let log = request_log(&venue).await;
        let first_algo_delete = log
            .iter()
            .position(|(method, path)| method == "DELETE" && path == "/fapi/v1/algoOrder")
            .expect("an algo cancel must have been sent");
        let last_algo_delete = log
            .iter()
            .rposition(|(method, path)| method == "DELETE" && path == "/fapi/v1/algoOrder")
            .expect("both algo cancels must have been sent");
        let ordinary_delete = log
            .iter()
            .position(|(method, path)| method == "DELETE" && path == "/fapi/v1/order")
            .expect("an ordinary cancel must have been sent");
        assert!(
            last_algo_delete < ordinary_delete,
            "Algo protection must be cancelled before ordinary children: {log:?}"
        );
        let algo_truth_after_cancel = log[first_algo_delete..]
            .iter()
            .any(|(method, path)| method == "GET" && path == "/fapi/v1/algoOrder");
        assert!(
            algo_truth_after_cancel,
            "a truth read must follow the algo cancel before completion: {log:?}"
        );
        let ordinary_truth_after_cancel = log[ordinary_delete..]
            .iter()
            .any(|(method, path)| method == "GET" && path == "/fapi/v1/order");
        assert!(
            ordinary_truth_after_cancel,
            "a truth read must follow the ordinary cancel before completion: {log:?}"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// 需求②（必测 b）：撤单请求遭遇传输/未知错误 ⇒ 义务保持挂起并退避
    /// 重试；恢复后由真值确认完成。替身的 request_log 一撤就写，把它拆掉
    /// 即可让每次交易所调用以错误返回（等价于传输失败/超时——驱动器把
    /// 任何非 -2013 错误都当作「未完成」）。
    #[tokio::test]
    async fn cancel_transport_failure_keeps_the_obligation_pending_until_truth_confirms() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0retry").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;

        let credential_id = "m0-retry-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);

        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;

        // 让替身交易所的所有调用失败。
        let broken = SqlitePool::connect(&format!("sqlite://{}", fake_path.display()))
            .await
            .expect("reconnect fake venue");
        sqlx::query("DROP TABLE request_log")
            .execute(&broken)
            .await
            .expect("break the fake venue transport");
        broken.close().await;

        let driven = drive_due_obligations(&store).await.expect("drive against a failing venue");
        assert_eq!(driven, 1, "the attempt ran and failed");
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 1)),
            "a transport failure must never complete the obligation"
        );
        let deferred_to: i64 =
            sqlx::query("SELECT next_attempt_at_ms FROM protection_cleanup_obligations WHERE position_id='pos_m0l4'")
                .fetch_one(store.pool())
                .await
                .expect("load backoff")
                .get("next_attempt_at_ms");
        assert!(deferred_to > 0, "the retry must be durably scheduled with backoff");
        assert_eq!(
            fake_algo_status(&venue, 101).await,
            ("NEW".to_string(), 1),
            "nothing on the venue may be touched by a failed attempt"
        );

        // 恢复「传输」，强迫退避到期，重试并由真值确认。
        let healed = SqlitePool::connect(&format!("sqlite://{}", fake_path.display()))
            .await
            .expect("reconnect fake venue");
        sqlx::query(
            "CREATE TABLE request_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, method TEXT NOT NULL, path TEXT NOT NULL, order_type TEXT NOT NULL DEFAULT '')",
        )
        .execute(&healed)
        .await
        .expect("heal the fake venue transport");
        healed.close().await;
        sqlx::query("UPDATE protection_cleanup_obligations SET next_attempt_at_ms=0 WHERE position_id='pos_m0l4'")
            .execute(store.pool())
            .await
            .expect("force the backoff due");

        let driven = drive_due_obligations(&store).await.expect("retry the obligation");
        assert_eq!(driven, 1);
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 2)),
            "the retry must complete the obligation only after truth"
        );
        assert_eq!(fake_algo_status(&venue, 101).await, ("CANCELED".to_string(), 0));

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// 需求③（必测 e）：别的**仍开着**的仓位的保护单绝不被本次善后波及。
    #[tokio::test]
    async fn protection_of_a_different_open_position_is_never_cancelled() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0openpos").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        // 另一个仓位（同凭据、不同 bot/scope）的 Algo 保护，仍开着。
        seed_fake_algo(&venue, 105, "coid_m0other_sl").await;

        let credential_id = "m0-openpos-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let bot_uid = "bot_m0other";
        seed_position(&store, "pos_m0other", credential_id, bot_uid, "long", "open", "1", SYMBOL_KEY)
            .await;
        seed_protection(
            &store,
            "prot_m0other_sl",
            "pos_m0other",
            credential_id,
            "sl",
            "eo_m0other_sl",
            Some("105"),
            Some("coid_m0other_sl"),
            "confirmed",
        )
        .await;
        seed_engine_order(
            &store,
            "eo_m0other_sl",
            credential_id,
            bot_uid,
            "pos_m0other",
            "protection_sl",
            "confirmed",
            None,
            "coid_m0other_sl",
        )
        .await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);

        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;
        let driven = drive_due_obligations(&store).await.expect("drive the obligation");
        assert_eq!(driven, 1);

        assert_eq!(
            obligation_state(&store, "pos_m0l4").await.as_ref().map(|(s, _, _)| s.as_str()),
            Some("complete"),
            "fixture: the closed position's obligation completed"
        );
        assert_eq!(
            fake_algo_status(&venue, 105).await,
            ("NEW".to_string(), 1),
            "another open position's protection must stay live on the venue"
        );
        let other_row = sqlx::query(
            "SELECT po.status AS po_status, eo.status AS eo_status \
             FROM protection_orders po JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id \
             WHERE po.protection_order_id='prot_m0other_sl'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the other protection row");
        let other: (String, String) = (other_row.get("po_status"), other_row.get("eo_status"));
        assert_eq!(
            other,
            ("confirmed".to_string(), "confirmed".to_string()),
            "another open position's local protection must be untouched"
        );
        assert_eq!(
            sqlx::query("SELECT state FROM position_state WHERE position_id='pos_m0other'")
                .fetch_one(store.pool())
                .await
                .expect("load the other position")
                .get::<String, _>("state"),
            "open",
            "the other position must still be open"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// 需求④（必测 c，替身交易所上的完整重启链）：义务落下后「重启」
    ///（关池重开同一权威库），扫描驱动从持久行续跑并完成。
    #[tokio::test]
    async fn restart_with_a_pending_obligation_resumes_and_completes_on_the_fake_venue() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0restartvenue").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        seed_fake_algo(&venue, 102, "coid_m0l4_tp").await;
        seed_fake_order(&venue, 201, "coid_m0l4_child").await;

        let credential_id = "m0-restartvenue-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await.as_ref().map(|(s, _, _)| s.as_str()),
            Some("pending")
        );
        // 进程在驱动之前崩溃：义务只在库里。
        let db_path = dir.join("engine.db");
        store.pool().close().await;

        let reopened = Store::connect(&format!("sqlite://{}", db_path.display()))
            .await
            .expect("reopen the store after the crash");
        let driven = drive_due_obligations(&reopened).await.expect("resume after restart");
        assert_eq!(driven, 1, "the durable obligation must be resumed after restart");
        assert_eq!(
            obligation_state(&reopened, "pos_m0l4").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 1)),
            "the resumed drive must complete via truth"
        );
        assert_eq!(fake_algo_status(&venue, 101).await, ("CANCELED".to_string(), 0));
        assert_eq!(fake_order_status(&venue, 201).await, ("CANCELED".to_string(), 0));

        venue.close().await;
        cleanup_dir(&reopened, &dir).await;
    }

    /// 需求②的真值分支补充：venue 查无此单（-2013）也是终局——那张单
    /// 不可能再触发，义务据此完成（本地按 canceled 收敛并留痕）。
    #[tokio::test]
    async fn venue_unknown_order_is_terminal_for_the_cleanup_obligation() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0unknown").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        // 替身里**故意不种** algo 101：get_algo_order 返回逐字节真实的 -2013。

        let credential_id = "m0-unknown-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;

        let driven = drive_due_obligations(&store).await.expect("drive the obligation");
        assert_eq!(driven, 1);
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 1)),
            "a -2013 truth answer is terminal: nothing live can trigger"
        );
        let log = request_log(&venue).await;
        assert!(
            log.iter()
                .any(|(method, path)| method == "GET" && path == "/fapi/v1/algoOrder"),
            "the -2013 verdict must come from a real truth query: {log:?}"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// 替身请求日志里是否读过 positionRisk、是否发过任何撤单（DELETE）。
    async fn venue_call_kinds(pool: &SqlitePool) -> (bool, bool) {
        let log = request_log(pool).await;
        let has_position_risk = log
            .iter()
            .any(|(method, path)| method == "GET" && path == "/fapi/v2/positionRisk");
        let has_delete = log.iter().any(|(method, _)| method == "DELETE");
        (has_position_risk, has_delete)
    }

    /// **P0-1 回归（评审 claude-review-m0 附录探针固化为永久用例，断言修复后
    /// 行为）。** 生产 reconcile 应用一份陈旧的零仓快照，把所上仍开着的仓
    /// 投影成 closed：
    /// * 驱动器同轮读 positionRisk，发现该 symbol+direction 非零 ⇒ 一张单都
    ///   不撤，义务保持 pending 并记录尝试；
    /// * 下一轮 reconcile 以新鲜真值经 ReopenClosed 重开同一 position_id，
    ///   未完成义务在**同一事务**里退役（retired / position_reopened），不再
    ///   挡入场闸门，所上保护单仍活着；
    /// * 之后真正的平仓（所上同步归零）在新的 close_revision 上重新武装，
    ///   预检通过后照常撤单并以真值完成。
    #[tokio::test]
    async fn stale_zero_truth_never_cancels_live_venue_protection_and_reopen_retires_the_obligation(
    ) {
        use crate::rate_limiter::{CredentialRateLimiter, TokenBucketConfig};
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixstale").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        seed_fake_algo(&venue, 102, "coid_m0l4_tp").await;
        seed_fake_order(&venue, 201, "coid_m0l4_child").await;
        // 陈旧快照：入场成交**之前**的 positionRisk（BTCUSDT 0，替身默认）。

        let credential_id = "m0fix-stale-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        let keys = || crate::binance_futures::BinanceKeys {
            api_key: "m0-l4-key".to_string(),
            api_secret: "m0-l4-secret".to_string(),
        };

        crate::reconcile::reconcile_binance_futures(
            credential_id,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(200.0, 200.0)),
            keys(),
        )
        .await
        .expect("reconcile #1 applies the stale snapshot");
        assert_eq!(
            sqlx::query("SELECT state FROM position_state WHERE position_id='pos_m0l4'")
                .fetch_one(store.pool())
                .await
                .expect("load position after the stale reconcile")
                .get::<String, _>("state"),
            "closed",
            "fixture: the stale zero snapshot closed the position locally"
        );
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 0)),
            "fixture: the stale close armed a pending obligation"
        );

        // 所上真相：入场其实已成交，仓位一直是 1。
        sqlx::query("UPDATE positions SET quantity='1' WHERE symbol='BTCUSDT'")
            .execute(&venue)
            .await
            .expect("open the venue position");

        let driven = drive_due_obligations(&store)
            .await
            .expect("drive against a live venue position");
        assert_eq!(driven, 1, "the attempt ran");
        assert_eq!(
            fake_algo_status(&venue, 101).await,
            ("NEW".to_string(), 1),
            "the live SL must NOT be cancelled while the venue position is open"
        );
        assert_eq!(
            fake_algo_status(&venue, 102).await,
            ("NEW".to_string(), 1),
            "the live TP must NOT be cancelled while the venue position is open"
        );
        assert_eq!(
            fake_order_status(&venue, 201).await,
            ("NEW".to_string(), 1),
            "the live ordinary child must NOT be cancelled while the venue position is open"
        );
        let (precheck_read, any_delete) = venue_call_kinds(&venue).await;
        assert!(
            precheck_read,
            "the driver must read positionRisk before cancelling"
        );
        assert!(
            !any_delete,
            "no cancel may be sent while the venue position is non-zero"
        );
        let first_revision = position_state_revision(&store, "pos_m0l4").await;
        assert_eq!(
            obligation_lifecycle(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 1, first_revision)),
            "the obligation stays pending with a recorded attempt at the stale close's revision"
        );
        let outcome: String = sqlx::query(
            "SELECT COALESCE(attempt_outcomes_json,'') AS o FROM protection_cleanup_obligations \
             WHERE position_id='pos_m0l4'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the attempt outcome")
        .get("o");
        assert!(
            outcome.contains("venue_position") && outcome.contains("nonzero"),
            "the non-zero venue position must be recorded as the attempt outcome: {outcome}"
        );
        for po in ["prot_m0l4_sl", "prot_m0l4_tp"] {
            let status: String =
                sqlx::query("SELECT status FROM protection_orders WHERE protection_order_id=?")
                    .bind(po)
                    .fetch_one(store.pool())
                    .await
                    .expect("load local protection row")
                    .get("status");
            assert_eq!(
                status, "confirmed",
                "{po}: local protection must stay armed"
            );
        }

        // reconcile #2（新鲜真值 1）：ReopenClosed 重开同一 position_id。
        crate::reconcile::reconcile_binance_futures(
            credential_id,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(200.0, 200.0)),
            keys(),
        )
        .await
        .expect("reconcile #2 applies fresh truth");
        let row = sqlx::query("SELECT state, qty FROM position_state WHERE position_id='pos_m0l4'")
            .fetch_one(store.pool())
            .await
            .expect("load the reopened position");
        assert_eq!(row.get::<String, _>("state"), "open");
        assert_eq!(row.get::<String, _>("qty"), "1");
        assert_eq!(
            obligation_lifecycle(&store, "pos_m0l4").await,
            Some((
                "retired".to_string(),
                "position_reopened".to_string(),
                1,
                first_revision
            )),
            "the reopen transaction must retire the pending obligation"
        );
        let mut tx = store.pool().begin().await.expect("begin retired gate tx");
        let gated = entry_scope_pending_in_tx(&mut tx, credential_id, SYMBOL_KEY, "long")
            .await
            .expect("probe the retired obligation's gate");
        tx.commit().await.expect("commit retired gate tx");
        assert!(!gated, "a retired obligation must not gate the scope");
        // 退役的义务不再驱动任何东西。
        sqlx::query("UPDATE protection_cleanup_obligations SET next_attempt_at_ms=0 WHERE position_id='pos_m0l4'")
            .execute(store.pool())
            .await
            .expect("make the retired obligation look due");
        assert_eq!(
            drive_due_obligations(&store)
                .await
                .expect("drive after the reopen"),
            0,
            "a retired obligation is never driven"
        );
        assert_eq!(fake_algo_status(&venue, 101).await, ("NEW".to_string(), 1));
        assert_eq!(fake_algo_status(&venue, 102).await, ("NEW".to_string(), 1));
        assert_eq!(fake_order_status(&venue, 201).await, ("NEW".to_string(), 1));
        let (_, any_delete) = venue_call_kinds(&venue).await;
        assert!(
            !any_delete,
            "protection stays live on the venue after the reopen"
        );

        // 第二生命周期：真正的平仓（所上同步归零）重新武装并善后。
        sqlx::query("UPDATE positions SET quantity='0' WHERE symbol='BTCUSDT'")
            .execute(&venue)
            .await
            .expect("flat the venue position with the exit fill");
        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;
        let second_revision = position_state_revision(&store, "pos_m0l4").await;
        assert!(second_revision > first_revision);
        assert_eq!(
            obligation_lifecycle(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 0, second_revision)),
            "the later genuine close re-arms a fresh obligation at its own revision"
        );
        let mut tx = store.pool().begin().await.expect("begin re-armed gate tx");
        let gated = entry_scope_pending_in_tx(&mut tx, credential_id, SYMBOL_KEY, "long")
            .await
            .expect("probe the re-armed gate");
        tx.commit().await.expect("commit re-armed gate tx");
        assert!(gated, "the re-armed obligation gates the scope again");
        let driven = drive_due_obligations(&store)
            .await
            .expect("drive the re-armed obligation");
        assert_eq!(driven, 1);
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 1)),
            "the re-armed obligation completes via truth once the venue position is really zero"
        );
        assert_eq!(
            fake_algo_status(&venue, 101).await,
            ("CANCELED".to_string(), 0)
        );
        assert_eq!(
            fake_algo_status(&venue, 102).await,
            ("CANCELED".to_string(), 0)
        );
        assert_eq!(
            fake_order_status(&venue, 201).await,
            ("CANCELED".to_string(), 0)
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// **P0-1 的歧义分支**：positionRisk 里根本没有这个 symbol（缺席不是
    /// 零——与 reconcile「只凭缺席不改 position_state」同一原则）⇒ 不撤单、
    /// 义务挂起并记录原因。
    #[tokio::test]
    async fn an_absent_venue_position_is_ambiguous_and_never_cancels() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixabsent").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        seed_fake_algo(&venue, 102, "coid_m0l4_tp").await;
        seed_fake_order(&venue, 201, "coid_m0l4_child").await;
        sqlx::query("DELETE FROM positions")
            .execute(&venue)
            .await
            .expect("remove the venue position row");

        let credential_id = "m0fix-absent-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;

        assert_eq!(drive_due_obligations(&store).await.expect("drive"), 1);
        assert_eq!(fake_algo_status(&venue, 101).await, ("NEW".to_string(), 1));
        assert_eq!(fake_algo_status(&venue, 102).await, ("NEW".to_string(), 1));
        assert_eq!(fake_order_status(&venue, 201).await, ("NEW".to_string(), 1));
        let (precheck_read, any_delete) = venue_call_kinds(&venue).await;
        assert!(precheck_read, "the driver must have asked the venue");
        assert!(
            !any_delete,
            "an ambiguous venue position must never lead to a cancel"
        );
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 1))
        );
        let outcome: String = sqlx::query(
            "SELECT COALESCE(attempt_outcomes_json,'') AS o FROM protection_cleanup_obligations \
             WHERE position_id='pos_m0l4'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the attempt outcome")
        .get("o");
        assert!(
            outcome.contains("venue_position") && outcome.contains("ambiguous"),
            "the ambiguity must be recorded as the attempt outcome: {outcome}"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// **P0-1 撤单前的本地生命周期复核**：即便所上已平，只要该义务对应的
    /// 本地仓已不是 closed（重开了却没有退役义务——生产重开路径都会同事务
    /// 退役，此处用夹具直接改状态来钉住这道纵深防线），驱动器也一张不撤、
    /// 义务挂起并记录原因。
    #[tokio::test]
    async fn a_pending_obligation_whose_position_is_open_again_never_cancels() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixreopenlocal").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        seed_fake_algo(&venue, 101, "coid_m0l4_sl").await;
        seed_fake_algo(&venue, 102, "coid_m0l4_tp").await;
        seed_fake_order(&venue, 201, "coid_m0l4_child").await;

        let credential_id = "m0fix-reopenlocal-cred";
        seed_closed_position_with_live_protection(&store, credential_id).await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, "bot_m0l4", SYMBOL_KEY, "1").await;
        sqlx::query("UPDATE position_state SET state='open', qty='1' WHERE position_id='pos_m0l4'")
            .execute(store.pool())
            .await
            .expect("fixture: the local row is open again without retiring the obligation");

        assert_eq!(drive_due_obligations(&store).await.expect("drive"), 1);
        assert_eq!(fake_algo_status(&venue, 101).await, ("NEW".to_string(), 1));
        assert_eq!(fake_algo_status(&venue, 102).await, ("NEW".to_string(), 1));
        assert_eq!(fake_order_status(&venue, 201).await, ("NEW".to_string(), 1));
        let (_, any_delete) = venue_call_kinds(&venue).await;
        assert!(
            !any_delete,
            "nothing may be cancelled for a position that is locally open again"
        );
        assert_eq!(
            obligation_state(&store, "pos_m0l4").await,
            Some(("pending".to_string(), String::new(), 1))
        );
        let outcome: String = sqlx::query(
            "SELECT COALESCE(attempt_outcomes_json,'') AS o FROM protection_cleanup_obligations \
             WHERE position_id='pos_m0l4'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the attempt outcome")
        .get("o");
        assert!(
            outcome.contains("position_state") && outcome.contains("not_closed"),
            "the local re-open must be recorded as the attempt outcome: {outcome}"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// **P1-1**：只有请求身份（clientAlgoId / client_order_id_short，无交易所
    /// 单号）且仍在途（unknown 等）的单，venue 的 -2013 在宽限期内**不是**
    /// 终局——提交可能尚未落地，迟到的 ACK 会在所上留下活的 closePosition
    /// 单。宽限与 reconcile 裁决未确认订单的同一常量、同一边界（距上次
    /// 状态推进 ≥ 宽限才可裁决）。时钟经 `drive_due_obligations_at` 注入，
    /// 行时间戳与驱动时刻都是固定整数。Algo 域与普通域各一行。
    #[tokio::test]
    async fn request_identity_not_found_within_the_grace_stays_pending_then_resolves() {
        const T0: i64 = 1_800_000_000_000;
        let grace = crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS;
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixgrace").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        // **故意不种**任何单：两种查询都回逐字节真实的 -2013。

        let credential_id = "m0fix-grace-cred";
        let bot_uid = "bot_m0fixgrace";
        seed_position(
            &store,
            "pos_m0fixgrace",
            credential_id,
            bot_uid,
            "long",
            "open",
            "1",
            SYMBOL_KEY,
        )
        .await;
        // Algo 域请求身份：client_algo_id、无 exchange_algo_id；引擎单 unknown。
        seed_protection(
            &store,
            "prot_m0fixgrace_sl",
            "pos_m0fixgrace",
            credential_id,
            "sl",
            "eo_m0fixgrace_sl",
            None,
            Some("coid_m0fixgrace_sl"),
            "requested",
        )
        .await;
        seed_engine_order(
            &store,
            "eo_m0fixgrace_sl",
            credential_id,
            bot_uid,
            "pos_m0fixgrace",
            "protection_sl",
            "unknown",
            None,
            "coid_m0fixgrace_sl",
        )
        .await;
        // 普通域请求身份：无 protection_orders 链接、仅 client_order_id_short。
        seed_engine_order(
            &store,
            "eo_m0fixgrace_child",
            credential_id,
            bot_uid,
            "pos_m0fixgrace",
            "protection_tp",
            "pending_submit",
            None,
            "coid_m0fixgrace_child",
        )
        .await;
        sqlx::query("UPDATE engine_orders SET updated_at_ms=? WHERE position_id='pos_m0fixgrace'")
            .bind(T0)
            .execute(store.pool())
            .await
            .expect("stamp the engine orders' last state advance");
        sqlx::query(
            "UPDATE protection_orders SET updated_at_ms=? WHERE position_id='pos_m0fixgrace'",
        )
        .bind(T0)
        .execute(store.pool())
        .await
        .expect("stamp the protection row's last state advance");
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, bot_uid, SYMBOL_KEY, "1").await;

        // 宽限边界内侧（差 1 ms）。
        let driven = drive_due_obligations_at(&store, T0 + grace - 1)
            .await
            .expect("drive within the grace");
        assert_eq!(driven, 1);
        for (eo, expected) in [
            ("eo_m0fixgrace_sl", "unknown"),
            ("eo_m0fixgrace_child", "pending_submit"),
        ] {
            let status: String =
                sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                    .bind(eo)
                    .fetch_one(store.pool())
                    .await
                    .expect("load engine order")
                    .get("status");
            assert_eq!(
                status, expected,
                "{eo}: -2013 for a request-only in-flight identity must not be terminal within the grace"
            );
        }
        let po_status: String = sqlx::query(
            "SELECT status FROM protection_orders WHERE protection_order_id='prot_m0fixgrace_sl'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load protection row")
        .get("status");
        assert_eq!(
            po_status, "requested",
            "the protection row must stay non-terminal within the grace"
        );
        assert_eq!(
            obligation_state(&store, "pos_m0fixgrace").await,
            Some(("pending".to_string(), String::new(), 1)),
            "the obligation must stay pending within the grace"
        );

        // 宽限边界本身：距上次状态推进恰好等于宽限 ⇒ 可裁决（与 reconcile 同界）。
        sqlx::query("UPDATE protection_cleanup_obligations SET next_attempt_at_ms=0 WHERE position_id='pos_m0fixgrace'")
            .execute(store.pool())
            .await
            .expect("force the backoff due");
        let driven = drive_due_obligations_at(&store, T0 + grace)
            .await
            .expect("drive at the grace boundary");
        assert_eq!(driven, 1);
        for eo in ["eo_m0fixgrace_sl", "eo_m0fixgrace_child"] {
            let status: String =
                sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                    .bind(eo)
                    .fetch_one(store.pool())
                    .await
                    .expect("load engine order after the grace")
                    .get("status");
            assert_eq!(
                status, "canceled",
                "{eo}: once the grace has elapsed, venue-unknown is terminal for a request-only identity"
            );
        }
        assert_eq!(
            obligation_state(&store, "pos_m0fixgrace").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 2)),
            "past the grace the obligation completes on the -2013 truth"
        );
        let (_, any_delete) = venue_call_kinds(&venue).await;
        assert!(!any_delete, "nothing was ever live on the venue to cancel");

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// **P1-2**：本地 `failed`（`mark_submit_failed` 的 StartedUnknown 形状：
    /// 提交结果不明、venue 可能已经接受）的 Algo 保护单必须在 **Algo 域**
    /// 求证。此前它被 Algo 轮按终态跳过，又被普通域按 clientAlgoId 查出
    /// -2013 而「真值确认」——错域裁决把活着的 Algo 留在所上，义务却完成了。
    #[tokio::test]
    async fn locally_failed_algo_protection_is_adjudicated_in_the_algo_domain() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixfail").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        // 所上这张 Algo **活着**（提交其实已到达交易所）。
        seed_fake_algo(&venue, 111, "coid_m0fail_sl").await;

        let credential_id = "m0fix-fail-cred";
        let bot_uid = "bot_m0fixfail";
        seed_position(
            &store,
            "pos_m0fixfail",
            credential_id,
            bot_uid,
            "long",
            "open",
            "1",
            SYMBOL_KEY,
        )
        .await;
        seed_protection(
            &store,
            "prot_m0fixfail_sl",
            "pos_m0fixfail",
            credential_id,
            "sl",
            "eo_m0fixfail_sl",
            None,
            Some("coid_m0fail_sl"),
            "failed",
        )
        .await;
        seed_engine_order(
            &store,
            "eo_m0fixfail_sl",
            credential_id,
            bot_uid,
            "pos_m0fixfail",
            "protection_sl",
            "unknown",
            None,
            "coid_m0fail_sl",
        )
        .await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);

        apply_exit_fill(&store, credential_id, bot_uid, SYMBOL_KEY, "1").await;
        let driven = drive_due_obligations(&store)
            .await
            .expect("drive the failed-algo obligation");
        assert_eq!(driven, 1);
        assert_eq!(
            fake_algo_status(&venue, 111).await,
            ("CANCELED".to_string(), 0),
            "a locally-failed Algo identity that is live on the venue must be cancelled through the Algo domain"
        );
        assert_eq!(
            obligation_state(&store, "pos_m0fixfail").await,
            Some(("complete".to_string(), "truth_confirmed".to_string(), 1)),
            "completion must come from Algo-domain truth"
        );
        let log = request_log(&venue).await;
        assert!(
            log.iter()
                .any(|(method, path)| method == "GET" && path == "/fapi/v1/algoOrder"),
            "the Algo domain must be queried for an Algo-identity row: {log:?}"
        );
        assert!(
            !log.iter().any(|(_, path)| path == "/fapi/v1/order"),
            "the ordinary domain must never be asked about an Algo identity: {log:?}"
        );
        let po_row = sqlx::query(
            "SELECT status, COALESCE(exchange_algo_id,'') AS algo_id \
             FROM protection_orders WHERE protection_order_id='prot_m0fixfail_sl'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load the protection row");
        assert_eq!(po_row.get::<String, _>("status"), "canceled");
        assert_eq!(
            po_row.get::<String, _>("algo_id"),
            "111",
            "the Algo truth read binds the exchange algo identity"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }

    /// **P1-2 的完成判据**：本地 `failed`（不是交易所真值）且引擎单也已是
    /// 本地终态的 Algo 身份行，持久态看起来「已收敛」；只要它这一轮的
    /// Algo 域求证没有成功（此处替身的 algoOrder 查询本身出错，positionRisk
    /// 仍可读且为零），义务就不能完成。
    #[tokio::test]
    async fn a_failed_algo_row_without_algo_domain_truth_keeps_the_obligation_pending() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, dir) = scratch_store("m0fixfailtruth").await;
        let fake_path = dir.join("fake-exchange.sqlite");
        let venue = seed_fake_venue(&fake_path).await;
        sqlx::query("DROP TABLE algo_orders")
            .execute(&venue)
            .await
            .expect("break only the fake venue's Algo-domain query");

        let credential_id = "m0fix-failtruth-cred";
        let bot_uid = "bot_m0fixfailtruth";
        seed_position(
            &store,
            "pos_m0fixfailtruth",
            credential_id,
            bot_uid,
            "long",
            "open",
            "1",
            SYMBOL_KEY,
        )
        .await;
        seed_protection(
            &store,
            "prot_m0fixfailtruth_sl",
            "pos_m0fixfailtruth",
            credential_id,
            "sl",
            "eo_m0fixfailtruth_sl",
            None,
            Some("coid_m0failtruth_sl"),
            "failed",
        )
        .await;
        seed_engine_order(
            &store,
            "eo_m0fixfailtruth_sl",
            credential_id,
            bot_uid,
            "pos_m0fixfailtruth",
            "protection_sl",
            "failed",
            None,
            "coid_m0failtruth_sl",
        )
        .await;
        let _env = set_l4_env(&fake_path, &dir, credential_id);
        apply_exit_fill(&store, credential_id, bot_uid, SYMBOL_KEY, "1").await;

        assert_eq!(drive_due_obligations(&store).await.expect("drive"), 1);
        assert_eq!(
            obligation_state(&store, "pos_m0fixfailtruth").await,
            Some(("pending".to_string(), String::new(), 1)),
            "a locally-failed Algo row whose Algo-domain truth was not established must keep the obligation pending"
        );
        let log = request_log(&venue).await;
        assert!(
            log.iter()
                .any(|(method, path)| method == "GET" && path == "/fapi/v1/algoOrder"),
            "the Algo domain must have been asked: {log:?}"
        );
        assert!(
            !log.iter().any(|(_, path)| path == "/fapi/v1/order"),
            "the ordinary domain must never be asked about an Algo identity: {log:?}"
        );

        venue.close().await;
        cleanup_dir(&store, &dir).await;
    }
}
