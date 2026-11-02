//! D30（2026-10-02）· 平仓后的保护单善后义务。
//!
//! 缺陷（protection cleanup）：任一平仓使交易所仓位
//! 归零后，该仓的 SL/TP 保护单——Binance Algo 条件单（常为 closePosition=true）
//! 与普通 reduce-only 子单——在交易所侧**无人撤销**。reconcile 只观察它们；
//! 只有 kill-switch L3 与 `CancelOrder` RPC 会主动撤单。同标的同方向的后续
//! 开仓可能被残留保护单按旧触发价平掉（engine safety contract「保护单缺失/运行状态」
//! 高危类）。
//!
//! 本模块的职责边界：
//! * **义务落库**：仓位归零的同一事务里写一行 `protection_cleanup_obligations`
//!   （覆盖该 position_id 的**所有代**保护单，驱动时按权威表枚举，不做快照）。
//! * **入场闸门**：同 credential+symbol+direction 存在未确认完成的义务时，
//!   新开仓在 OMS 准入事务内以稳定类型码 [`REJECTION_CODE`] 拒绝。减仓/平仓、
//!   其他仓位的保护、reconcile 与 kill switch 不受影响（§3.11 只拦新增风险）。
//! * **撤单驱动**：boot 起的后台任务复用 `CancelOrder` / L3 的既有撤单原语
//!   （Algo 先、普通后），**撤单请求被接受绝不是完成**——与 kill_switch
//!   "acknowledgement is never completion" 同一条不变量；只有交易所真值
//!   （algoOrder/order 查询：`-2013` 或终态）确认后才标记完成。
//!
//! 崩溃边界：义务行与仓位 closed 状态写在**同一个数据库事务**里（提交前
//! 崩溃则两者都不存在，对账/WS 回放会重新推导并重新落义务；提交后崩溃则
//! 义务持久存在，重启后扫描继续驱动）。驱动器自身零内存状态，全部进度
//! （attempts / next_attempt_at_ms / 终态）都在义务行上。
//!
//! **M0fix（评审 claude-review-m0 P0-1）· 本地 closed 不等于交易所归零。**
//! reconcile 应用的 positionRisk 快照可能早于一次入场成交（快照 → 落库之间
//! 无新鲜度守卫），于是所上仍开着的仓会被本地投影成 closed；下一轮
//! reconcile 的 ReopenClosed 又会复用**同一个** position_id 把它重开。两条
//! 不变量因此成立：
//! * **撤单前必须同轮确认交易所该 scope 精确为零**：任何撤单之前，驱动器
//!   读一次该凭据的 positionRisk（既有客户端调用、既有限流器），按引擎应用
//!   仓位真值的同一份侧向映射判定该 symbol+direction；非零、读失败、歧义
//!   （缺席/无法解析）一律不撤、挂起并记录原因。
//! * **义务绑定一次关闭生命周期**：键 (position_id, close_revision)，
//!   close_revision 是那次「非 closed → closed」写下的 position_state 修订号。
//!   重开（ReopenClosed）在同一事务里把未完成义务退役（`retired` /
//!   `position_reopened`），退役义务不驱动、不挡闸门；之后真正的关闭在新的
//!   修订号上重新武装同一行。驱动器的每次 CAS 都带 close_revision。

use anyhow::Context as _;
use sqlx::Row;
use tonic::Status;

use crate::binance_futures::{BinanceAlgoId, BinanceClientAlgoId, BinanceFuturesClient};
use crate::rate_limiter::{CredentialRateLimiter, RequestClass};
use crate::store::{DbKind, Store};

/// 入场闸门的稳定类型码（v2 `IntentAck.reject_reason_code` 直接投影它）。
pub(crate) const REJECTION_CODE: &str = "PROTECTION_CLEANUP_PENDING";

/// 与 kill_switch L3 / reconcile 的保护单枚举同一非终态判据。
const PROTECTION_TERMINAL_STATUSES: [&str; 3] = ["canceled", "expired", "failed"];
/// engine_orders 的终态闭集（svc::TERMINAL_ORDER_STATUSES 同集）。
const ENGINE_TERMINAL_STATUSES: [&str; 5] = ["filled", "canceled", "expired", "rejected", "failed"];

/// 重试退避：与 kill_switch `ACTION_RETRY_BACKOFF_*` 同参数（1s 起步、60s 封顶）。
const RETRY_BACKOFF_BASE_MS: i64 = 1_000;
const RETRY_BACKOFF_CAP_MS: i64 = 60_000;
/// 扫描周期（与 kill_switch 动作重试任务同频）。
const SCAN_INTERVAL_MS: u64 = 1_000;

fn retry_backoff_ms(attempts: i64) -> i64 {
    let shift = attempts.clamp(1, 20) as u32;
    RETRY_BACKOFF_BASE_MS
        .saturating_mul(1i64.checked_shl(shift - 1).unwrap_or(i64::MAX / 2))
        .min(RETRY_BACKOFF_CAP_MS)
}

/// 交易所真值「查无此单」的判据——与 reconcile 的单查路径同串
/// （`reconcile.rs` 对 `get_algo_order_json` 错误的 -2013 判别）。
fn is_order_unknown_on_venue(error: &anyhow::Error) -> bool {
    error.to_string().contains("\"code\":-2013")
}

/// M0fix（评审 P1-1）· 请求身份的在途态（引擎单）：与 reconcile 裁决未确认
/// 订单的在途集合同族（`pending_risk` / `pending_submit` / `unknown`），外加
/// `submitted`——本地已标提交、却还没有任何交易所单号（回执未落库）。
const REQUEST_IN_FLIGHT_ENGINE_STATUSES: [&str; 4] =
    ["pending_risk", "pending_submit", "unknown", "submitted"];
/// 没有链接引擎单时保护行自身的在途态；`failed` 来自 `mark_submit_failed`
/// （StartedUnknown：提交结果不明，venue 可能已经或即将接受）。
const REQUEST_IN_FLIGHT_PROTECTION_STATUSES: [&str; 3] = ["requested", "submitted", "failed"];

/// M0fix（评审 P1-1）：只凭请求身份（clientAlgoId / client_order_id_short，
/// 没有任何交易所单号）认识、且仍在途的单，venue 的「查无此单（-2013）」在
/// 宽限期内**不是**终局——提交可能还没落地；`protection_dispatch` 已武装的
/// 授权在平仓后仍可能落地，迟到的 ACK 会在所上留下一张活的 closePosition
/// 单，而本地早已被写成终态、义务已完成。宽限与 reconcile 裁决未确认订单
/// 是同一常量、同一边界：距上次状态推进 ≥ 宽限才可把 -2013 当终局。
fn request_identity_not_found_within_grace(
    has_exchange_identity: bool,
    request_in_flight: bool,
    last_advance_ms: i64,
    now_ms: i64,
) -> bool {
    !has_exchange_identity
        && request_in_flight
        && now_ms.saturating_sub(last_advance_ms)
            < crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS
}

/// 平仓路径在**同一事务**内调用：为这一次关闭生命周期武装善后义务（幂等）。
///
/// `origin` 记录触发来源（`fill_truth` = 成交应用，`exchange_truth` = 仓位
/// 真值），只作审计，不影响语义。非 exchange-backed 档位（paper/simulation）
/// 的保护单从不派发到任何交易所——若无 ACK 级 venue 身份，义务在**本事务
/// 内**即以 `paper_run_no_venue_protection` 完成并记录原因（不静默跳过，
/// 也不把同 scope 的再开仓挡在门外）；exchange-backed 档位一律 pending，
/// 交驱动器撤单并以交易所真值确认。
///
/// M0fix（P0-1 / P2-3）· 生命周期键 (position_id, close_revision)：
/// * 同一修订号的重放（同一次关闭再落一次）是 no-op；
/// * 行里是更早的生命周期（已完成、已退役，或——防御性地——仍 pending）
///   ⇒ 原地改写成本次关闭的新生命周期（attempts/退避/结果清零）；
/// * 行里修订号更新 ⇒ no-op（不回退）。
///
/// 写法刻意避开 MySQL 的间隙锁：不对可能不存在的键做锁定读（两次不同
/// 仓位的并发关闭会在同一段 ULID 间隙上互锁），而是先「不存在才插入」
/// （只取插入意向锁），再对**已经存在**的那一行做锁定读（当前读、记录锁）
/// 决定 no-op 还是改写——一致性读看不到在本事务快照之后提交的义务行，
/// 那正是 P2-3 撞主键的来源。
pub(crate) async fn record_position_closed_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    position_id: &str,
    origin: &str,
    ts_ms: i64,
) -> Result<(), Status> {
    let (lock, insert_if_absent) = match db_kind {
        DbKind::MySql => (
            " FOR UPDATE",
            " ON DUPLICATE KEY UPDATE position_id=position_id",
        ),
        DbKind::Sqlite => ("", " ON CONFLICT(position_id) DO NOTHING"),
        DbKind::Unknown => {
            return Err(Status::internal(
                "protection cleanup obligation requires a known database dialect",
            ))
        }
    };
    let scope = format!(
        "SELECT credential_id, bot_uid, symbol_key, direction, close_revision FROM position_state \
         WHERE position_id=?{lock}"
    );
    let Some(scope) = sqlx::query(&scope)
        .bind(position_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db load closed position scope failed: {e}")))?
    else {
        // 没有仓行就没有可归属的保护义务范围；不为不存在的行造义务。
        return Ok(());
    };
    let credential_id: String = scope.get("credential_id");
    let bot_uid: String = scope.get("bot_uid");
    let symbol_key: String = scope.get("symbol_key");
    let direction: String = scope.get("direction");
    let close_revision: i64 = scope.get("close_revision");

    // 需求⑤（paper/simulation）：非 exchange-backed 档位的保护单**从不派发**
    // （protection dispatch 只对 exchange-backed 准备，svc.rs「if inserted &&
    // is_exchange_backed」），因此无可撤销的 venue 侧单：义务在本事务内以
    // 记录原因平凡完成。paper 与 exchange 的判别不猜命名空间、也不读可能
    // 缺席的 provenance——调用方就是引擎按 is_exchange_backed 切开的那两条
    // 生产路径本身（paper 分支走 mark_paper_filled，venue fill/truth 走另外
    // 两个入口），origin 即档位；模拟凭据再兜一层底。出现 ACK 级 venue 身份
    // （引擎正常路径不可达的异常数据）则如实留 pending。
    let paper_run = origin == "paper_fill"
        || matches!(
            crate::venue::credential_kind(&credential_id),
            crate::venue::CredentialKind::Simulation
        );
    let paper_trivially_complete =
        paper_run && !position_has_venue_protection_identity(tx, position_id).await?;

    if paper_trivially_complete {
        crate::svc::cancel_simulation_protection_in_tx(
            tx,
            db_kind,
            &credential_id,
            position_id,
            ts_ms,
        )
        .await?;
    }

    let (state, complete_reason, completed_at_ms) = if paper_trivially_complete {
        (
            "complete",
            Some("paper_run_no_venue_protection"),
            Some(ts_ms),
        )
    } else {
        ("pending", None, None)
    };
    sqlx::query(&format!(
        r#"
        INSERT INTO protection_cleanup_obligations (
          position_id, credential_id, bot_uid, symbol_key, direction,
          state, origin, attempts, next_attempt_at_ms,
          complete_reason, attempt_outcomes_json,
          created_at_ms, updated_at_ms, completed_at_ms, close_revision
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, NULL, ?, ?, ?, ?){insert_if_absent}
        "#
    ))
    .bind(position_id)
    .bind(&credential_id)
    .bind(&bot_uid)
    .bind(&symbol_key)
    .bind(&direction)
    .bind(state)
    .bind(origin)
    .bind(complete_reason)
    .bind(ts_ms)
    .bind(ts_ms)
    .bind(completed_at_ms)
    .bind(close_revision)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db insert cleanup obligation failed: {e}")))?;

    // 行此刻一定存在：锁定读拿到当前已提交版本（MySQL 记录锁）。
    let existing = sqlx::query(&format!(
        "SELECT close_revision, state FROM protection_cleanup_obligations \
         WHERE position_id=?{lock}"
    ))
    .bind(position_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load cleanup obligation failed: {e}")))?;
    let existing_revision: i64 = existing.get("close_revision");
    if existing_revision >= close_revision {
        // 本次关闭刚插入的那一行，或同一次关闭的重放，或更新的生命周期已在。
        return Ok(());
    }
    let previous_state: String = existing.get("state");
    if previous_state.trim() == "pending" {
        // 防御性分支：每条重开路径都在同一事务里退役未完成义务，走到这里
        // 说明有一条没有。新的关闭仍覆盖该仓所有代的保护单，改写成新
        // 生命周期（仍 pending）不丢任何撤单责任。
        tracing::warn!(
            position_id,
            previous_revision = existing_revision,
            close_revision,
            "protection cleanup obligation was still pending when the position closed again; \
             superseding it with the new close lifecycle"
        );
    }
    let rows = sqlx::query(
        r#"
        UPDATE protection_cleanup_obligations
        SET credential_id=?, bot_uid=?, symbol_key=?, direction=?,
            state=?, origin=?, attempts=0, next_attempt_at_ms=0,
            complete_reason=?, attempt_outcomes_json=NULL,
            created_at_ms=?, updated_at_ms=?, completed_at_ms=?, close_revision=?
        WHERE position_id=? AND close_revision=?
        "#,
    )
    .bind(&credential_id)
    .bind(&bot_uid)
    .bind(&symbol_key)
    .bind(&direction)
    .bind(state)
    .bind(origin)
    .bind(complete_reason)
    .bind(ts_ms)
    .bind(ts_ms)
    .bind(completed_at_ms)
    .bind(close_revision)
    .bind(position_id)
    .bind(existing_revision)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db re-arm cleanup obligation failed: {e}")))?
    .rows_affected();
    if rows != 1 {
        return Err(Status::internal(format!(
            "re-arming the cleanup obligation of {position_id} affected {rows} rows"
        )));
    }
    Ok(())
}

/// M0fix（P0-1）：一个**已 closed** 的仓被重开（reconcile 的 ReopenClosed）
/// 时，在**同一事务**里退役该仓尚未完成的善后义务：所上那些保护单属于
/// 仍开着的仓，旧义务从此不能再撤任何东西，也不再挡入场闸门（闸门只看
/// pending）。之后真正的关闭在新的 close_revision 上重新武装。
///
/// 先用一致性读确认有 pending 行，再按主键更新（记录锁）——不对不存在的
/// 键做锁定写，避开 MySQL 间隙锁。一致性读足够：调用方据以判定 ReopenClosed
/// 的本地快照里该仓已是 closed，而义务与那次关闭同事务提交，快照必然可见。
pub(crate) async fn retire_obligations_on_reopen_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
    ts_ms: i64,
) -> Result<u64, Status> {
    let pending = sqlx::query(
        "SELECT close_revision FROM protection_cleanup_obligations \
         WHERE position_id=? AND state='pending' AND HEX(state)=HEX('pending')",
    )
    .bind(position_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| {
        Status::internal(format!(
            "db load reopened position's cleanup obligation failed: {e}"
        ))
    })?;
    let Some(pending) = pending else {
        return Ok(0);
    };
    let close_revision: i64 = pending.get("close_revision");
    let rows = sqlx::query(
        "UPDATE protection_cleanup_obligations \
         SET state='retired', complete_reason='position_reopened', completed_at_ms=?, updated_at_ms=? \
         WHERE position_id=? AND close_revision=? AND state='pending' AND HEX(state)=HEX('pending')",
    )
    .bind(ts_ms)
    .bind(ts_ms)
    .bind(position_id)
    .bind(close_revision)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db retire reopened position's cleanup obligation failed: {e}")))?
    .rows_affected();
    if rows > 0 {
        tracing::warn!(
            position_id,
            close_revision,
            "position reopened: its pending protection cleanup obligation is retired and will not cancel anything"
        );
    }
    Ok(rows)
}

/// 该仓的保护单是否存在任何 **venue 已确认**身份：`protection_orders` 的
/// `exchange_algo_id`（venue 对 Algo 的 ack）/ `exchange_order_id`
///（actualOrderId 或 pre-0013 遗留条件单），或 engine_orders 的
/// `exchange_order_id`。
///
/// 刻意不含 `client_algo_id` 与 `client_order_id_short`——那是本地**请求**
/// 身份，insert_requested 在入场事务里就会写，纸面运行也有；它证明「想去
/// venue」，不证明「到过 venue」。平凡完成只认 ACK 级证据；驱动器的撤单
/// 目标枚举才把请求身份也算可撤身份（K04 中断提交形状）。
async fn position_has_venue_protection_identity(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
) -> Result<bool, Status> {
    let non_blank = |row: &sqlx::any::AnyRow, column: &str| -> Result<bool, Status> {
        Ok(crate::store::try_get_nullable_string(row, column)
            .map_err(|e| Status::internal(format!("decode protection identity failed: {e}")))?
            .is_some_and(|value| !value.trim().is_empty()))
    };
    let po = sqlx::query(
        "SELECT exchange_algo_id, exchange_order_id FROM protection_orders \
         WHERE position_id=?",
    )
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load protection identities failed: {e}")))?;
    for row in &po {
        if non_blank(row, "exchange_algo_id")? || non_blank(row, "exchange_order_id")? {
            return Ok(true);
        }
    }
    let eo = sqlx::query(
        "SELECT exchange_order_id FROM engine_orders \
         WHERE position_id=? AND order_role IN ('protection_sl','protection_tp','emergency')",
    )
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load protection order identities failed: {e}")))?;
    for row in &eo {
        if non_blank(row, "exchange_order_id")? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// 入场闸门查询（生产与索引可用性用例共用同一份 SQL）。
///
/// M0fix（评审 P2-5）：先用**直接等值**命中 scope 索引
/// `(credential_id, symbol_key, direction, state)`，再以 `HEX()` 作为同一批
/// 候选行上的残余过滤保留字节精确：MySQL 默认 `_ci` collation 下等值会
/// 折叠大小写/尾空格，候选集是字节精确匹配的超集（字节相等必然 collation
/// 相等），残余过滤把它收窄回字节精确。`HEX(列)` 单独出现时优化器根本
/// 不能考虑索引（此前闸门与 1 Hz 扫描都是全表扫描，而义务表只增不删）。
/// 参数顺序：credential_id, symbol_key, direction, credential_id,
/// symbol_key, direction。
pub(crate) const ENTRY_SCOPE_PENDING_SQL: &str =
    "SELECT position_id FROM protection_cleanup_obligations \
     WHERE credential_id=? AND symbol_key=? AND direction=? AND state='pending' \
       AND HEX(credential_id)=HEX(?) AND HEX(symbol_key)=HEX(?) \
       AND HEX(direction)=HEX(?) AND HEX(state)=HEX('pending') \
     LIMIT 1";

/// 驱动器到期扫描（生产与索引可用性用例共用同一份 SQL）。直接等值 +
/// 范围命中 0045 的 `(state, next_attempt_at_ms)` 索引，`HEX()` 同上只作
/// 残余的字节精确过滤（P2-5）。
pub(crate) const DUE_SCAN_SQL: &str =
    "SELECT position_id, credential_id, symbol_key, direction, close_revision, attempts \
     FROM protection_cleanup_obligations \
     WHERE state='pending' AND next_attempt_at_ms <= ? AND HEX(state)=HEX('pending') \
     ORDER BY position_id";

/// 入场闸门（OMS 准入事务内）：同 credential+symbol+direction 是否存在
/// 未确认完成的善后义务。索引等值 + HEX 残余过滤（字节精确，避开 MySQL
/// 默认 collation 的大小写折叠；见 [`ENTRY_SCOPE_PENDING_SQL`]）。
pub(crate) async fn entry_scope_pending_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
    direction: &str,
) -> Result<bool, Status> {
    let row = sqlx::query(ENTRY_SCOPE_PENDING_SQL)
        .bind(credential_id)
        .bind(symbol_key)
        .bind(direction)
        .bind(credential_id)
        .bind(symbol_key)
        .bind(direction)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db check protection cleanup scope failed: {e}")))?;
    Ok(row.is_some())
}

struct ObligationRow {
    position_id: String,
    credential_id: String,
    symbol_key: String,
    direction: String,
    /// 本义务所属关闭生命周期（M0fix P0-1）；驱动器的每次 CAS 都带它。
    close_revision: i64,
    attempts: i64,
}

/// 同一驱动轮内按凭据缓存的 positionRisk 读结果（同轮新鲜；错误按串缓存，
/// 同轮同凭据的其他义务同样挂起）。
type VenuePositionsRound = std::collections::HashMap<String, Result<serde_json::Value, String>>;

/// boot 时随其他后台任务一起启动（`main.rs`）。与
/// `kill_switch::spawn_action_retry_task` 同形：单任务、周期扫描、失败只记
/// 日志下一轮再来；所有进度持久在义务行上，重启后自然续跑。
pub fn spawn_cleanup_task(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(SCAN_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = drive_due_obligations(&store).await {
                tracing::error!(failure_reason = %format!("{error:#}"), "protection cleanup drive round failed");
            }
        }
    })
}

/// 驱动所有到期义务。测试直接调用它代替后台任务（驱动器无内存状态，
/// 「重启后续跑」等价于对同一持久库再次调用本函数）。
pub(crate) async fn drive_due_obligations(store: &Store) -> anyhow::Result<usize> {
    drive_due_obligations_at(store, crate::svc::now_ms()).await
}

/// 注入时钟的驱动入口：`now` 是本轮唯一的时间来源（退避到期判定、宽限
/// 窗口、落库时间戳）。生产只经 [`drive_due_obligations`] 传入墙钟。
pub(crate) async fn drive_due_obligations_at(store: &Store, now: i64) -> anyhow::Result<usize> {
    let due = sqlx::query(DUE_SCAN_SQL)
        .bind(now)
        .fetch_all(store.pool())
        .await
        .context("scan due protection cleanup obligations")?;
    let mut driven = 0usize;
    let mut venue_positions = VenuePositionsRound::new();
    for row in due {
        let obligation = ObligationRow {
            position_id: row.get("position_id"),
            credential_id: row.get("credential_id"),
            symbol_key: row.get("symbol_key"),
            direction: row.get("direction"),
            close_revision: row.get("close_revision"),
            attempts: row.get("attempts"),
        };
        let claimed = sqlx::query(
            "UPDATE protection_cleanup_obligations SET attempts=attempts+1, updated_at_ms=? \
             WHERE position_id=? AND close_revision=? AND HEX(state)=HEX('pending')",
        )
        .bind(now)
        .bind(&obligation.position_id)
        .bind(obligation.close_revision)
        .execute(store.pool())
        .await
        .context("claim protection cleanup attempt")?
        .rows_affected();
        if claimed != 1 {
            continue;
        }
        driven += 1;
        match drive_one(store, &obligation, now, &mut venue_positions).await {
            Ok(DriveOutcome::Complete { reason }) => {
                complete_obligation(store, &obligation, &reason, now).await?;
            }
            Ok(DriveOutcome::Pending { note, outcomes }) => {
                defer_obligation(
                    store,
                    &obligation,
                    obligation.attempts + 1,
                    &note,
                    &outcomes,
                    now,
                )
                .await?;
            }
            // 本轮途中该生命周期已退役/被改写：它的进度不归本轮写。
            Ok(DriveOutcome::Superseded { note }) => {
                tracing::info!(
                    position_id = %obligation.position_id,
                    close_revision = obligation.close_revision,
                    note,
                    "protection cleanup attempt stopped: the obligation lifecycle changed"
                );
            }
            // 单条义务的意外失败只影响它自己：记下原因并按退避重试。
            Err(error) => {
                tracing::error!(
                    position_id = %obligation.position_id,
                    credential_id = %obligation.credential_id,
                    failure_reason = %format!("{error:#}"),
                    "protection cleanup drive failed"
                );
                defer_obligation(
                    store,
                    &obligation,
                    obligation.attempts + 1,
                    &format!("drive error: {error:#}"),
                    &[],
                    now,
                )
                .await?;
            }
        }
    }
    Ok(driven)
}

enum DriveOutcome {
    Complete {
        reason: String,
    },
    Pending {
        note: String,
        outcomes: Vec<serde_json::Value>,
    },
    /// 本轮读到的生命周期已不再是 pending（重开退役、或被新关闭改写）。
    Superseded {
        note: String,
    },
}

/// 完成 CAS 带 (position_id, close_revision)。CAS 落空（同一轮里生命周期被
/// 重开退役/改写）只记日志，不中断本轮其余义务。
async fn complete_obligation(
    store: &Store,
    obligation: &ObligationRow,
    reason: &str,
    now: i64,
) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "UPDATE protection_cleanup_obligations \
         SET state='complete', complete_reason=?, completed_at_ms=?, updated_at_ms=? \
         WHERE position_id=? AND close_revision=? AND HEX(state)=HEX('pending')",
    )
    .bind(reason)
    .bind(now)
    .bind(now)
    .bind(&obligation.position_id)
    .bind(obligation.close_revision)
    .execute(store.pool())
    .await
    .context("complete protection cleanup obligation")?
    .rows_affected();
    if rows != 1 {
        tracing::warn!(
            position_id = %obligation.position_id,
            close_revision = obligation.close_revision,
            reason,
            "protection cleanup completion lost its lifecycle CAS row; the lifecycle changed during the round"
        );
        return Ok(());
    }
    tracing::info!(
        position_id = %obligation.position_id,
        close_revision = obligation.close_revision,
        reason,
        "protection cleanup obligation complete"
    );
    Ok(())
}

async fn defer_obligation(
    store: &Store,
    obligation: &ObligationRow,
    attempts: i64,
    note: &str,
    outcomes: &[serde_json::Value],
    now: i64,
) -> anyhow::Result<()> {
    let backoff = retry_backoff_ms(attempts);
    sqlx::query(
        "UPDATE protection_cleanup_obligations \
         SET next_attempt_at_ms=?, attempt_outcomes_json=?, updated_at_ms=? \
         WHERE position_id=? AND close_revision=? AND HEX(state)=HEX('pending')",
    )
    .bind(now + backoff)
    .bind(serde_json::to_string(outcomes).unwrap_or_else(|_| "[]".to_string()))
    .bind(now)
    .bind(&obligation.position_id)
    .bind(obligation.close_revision)
    .execute(store.pool())
    .await
    .context("defer protection cleanup obligation")?;
    tracing::warn!(
        position_id = %obligation.position_id,
        close_revision = obligation.close_revision,
        note,
        backoff_ms = backoff,
        "protection cleanup obligation still pending"
    );
    Ok(())
}

/// 交易所该 scope 仓位的判定结果（M0fix P0-1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VenueScopeExposure {
    /// 该 symbol+direction 精确为零：可以撤这条义务的保护单。
    Flat,
    /// 该 symbol+direction 非零：所上的仓仍开着，一张都不能撤。
    NonZero(String),
    /// 无法判定（缺席、数量不可解析、未知 positionSide、形状不对）。
    Ambiguous(String),
}

/// 从一份 positionRisk 应答判定 `symbol_key`+`direction` 是否精确为零。
///
/// 侧向映射与 `position_state::apply_exchange_position_truth` 同一份：
/// `LONG`/`SHORT` 行只说明各自方向；`BOTH`（单向持仓，缺省即 BOTH）行按
/// 数量符号归属方向，零数量对两个方向都成立——单向持仓下一侧非零时另一
/// 侧按构造为零。symbol 匹配用同一个 USDⓈ-M 身份判据（币本位/异 venue 的
/// 同名合约不算）。缺席不是零（与 reconcile「只凭缺席不改 position_state」
/// 同一原则）：没有任何一行说明该方向 ⇒ 歧义。
pub(crate) fn venue_scope_exposure(
    positions: &serde_json::Value,
    symbol_key: &str,
    direction: &str,
) -> VenueScopeExposure {
    let Some(entries) = positions.as_array() else {
        return VenueScopeExposure::Ambiguous("positionRisk response is not an array".to_string());
    };
    if !matches!(direction, "long" | "short") {
        return VenueScopeExposure::Ambiguous(format!(
            "obligation direction {direction:?} is neither long nor short"
        ));
    }
    let mut described = 0usize;
    for entry in entries {
        let symbol = entry
            .get("symbol")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .trim();
        if symbol.is_empty() || !crate::binance_futures::usdm_symbol_key_matches(symbol_key, symbol)
        {
            continue;
        }
        let qty = entry
            .get("positionAmt")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let Some(is_zero) = crate::reconcile::snapshot_decimal_is_zero(qty) else {
            return VenueScopeExposure::Ambiguous(format!(
                "{symbol} positionAmt {qty:?} is not a decimal"
            ));
        };
        let side = entry
            .get("positionSide")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("BOTH")
            .trim()
            .to_ascii_uppercase();
        let exposed_direction = match side.as_str() {
            "LONG" if direction != "long" => continue,
            "SHORT" if direction != "short" => continue,
            "LONG" => "long",
            "SHORT" => "short",
            "BOTH" if qty.trim().starts_with('-') => "short",
            "BOTH" => "long",
            other => {
                return VenueScopeExposure::Ambiguous(format!(
                    "{symbol} positionSide {other:?} is not LONG/SHORT/BOTH"
                ))
            }
        };
        described += 1;
        if !is_zero && exposed_direction == direction {
            return VenueScopeExposure::NonZero(format!(
                "{symbol} positionSide {side} positionAmt {qty}"
            ));
        }
    }
    if described == 0 {
        return VenueScopeExposure::Ambiguous(format!(
            "positionRisk has no row describing {direction} on {symbol_key}"
        ));
    }
    VenueScopeExposure::Flat
}

/// 同轮读该凭据的 positionRisk（既有客户端调用，经客户端的进程级限流器），
/// 同一轮内同凭据只读一次。
async fn venue_positions_this_round<'a>(
    client: &BinanceFuturesClient,
    credential_id: &str,
    round: &'a mut VenuePositionsRound,
) -> &'a Result<serde_json::Value, String> {
    if !round.contains_key(credential_id) {
        let read = client
            .positions(RequestClass::PeriodicReconcile)
            .await
            .map_err(|error| format!("{error:#}"));
        round.insert(credential_id.to_string(), read);
    }
    round
        .get(credential_id)
        .expect("the round cache holds the credential just read")
}

/// 撤单前的最后一道本地判据：本义务仍是这一生命周期的 pending，且仓位
/// 本地仍是 closed。重开（同事务退役）后两者都不成立。
enum LifecycleCheck {
    Armed,
    Superseded(String),
    PositionNotClosed(String),
}

async fn lifecycle_still_armed(
    store: &Store,
    obligation: &ObligationRow,
) -> anyhow::Result<LifecycleCheck> {
    let row = sqlx::query(
        "SELECT o.state AS o_state, COALESCE(p.state, '') AS p_state \
         FROM protection_cleanup_obligations o \
         LEFT JOIN position_state p ON p.position_id = o.position_id \
         WHERE o.position_id=? AND o.close_revision=?",
    )
    .bind(&obligation.position_id)
    .bind(obligation.close_revision)
    .fetch_optional(store.pool())
    .await
    .context("re-check the cleanup obligation lifecycle")?;
    let Some(row) = row else {
        return Ok(LifecycleCheck::Superseded(
            "the obligation was re-armed by a later close".to_string(),
        ));
    };
    let obligation_state: String = row.get("o_state");
    if obligation_state.trim() != "pending" {
        return Ok(LifecycleCheck::Superseded(format!(
            "the obligation is now {obligation_state}"
        )));
    }
    let position_state: String = row.get("p_state");
    if position_state.trim() != "closed" {
        return Ok(LifecycleCheck::PositionNotClosed(position_state));
    }
    Ok(LifecycleCheck::Armed)
}

struct ProtectionRow {
    protection_order_id: String,
    engine_order_id: Option<String>,
    po_status: String,
    exchange_algo_id: Option<String>,
    client_algo_id: Option<String>,
    exchange_order_id: Option<String>,
    eo_status: Option<String>,
    eo_exchange_order_id: Option<String>,
    #[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
    eo_client_order_id_short: Option<String>,
    /// 保护行与链接引擎单各自的最近状态推进时刻（P1-1 宽限的起点取两者
    /// 较晚者）；无引擎单时两者相同。
    po_updated_at_ms: i64,
    eo_updated_at_ms: i64,
}

impl ProtectionRow {
    fn non_blank(value: &Option<String>) -> Option<&str> {
        value
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
    fn algo_id(&self) -> Option<&str> {
        Self::non_blank(&self.exchange_algo_id)
    }
    fn client_algo_id(&self) -> Option<&str> {
        Self::non_blank(&self.client_algo_id)
    }
    fn venue_order_id(&self) -> Option<&str> {
        Self::non_blank(&self.eo_exchange_order_id)
            .or_else(|| Self::non_blank(&self.exchange_order_id))
    }
    #[allow(dead_code, reason = "Retained internal order identity accessor.")]
    fn client_order_id(&self) -> Option<&str> {
        Self::non_blank(&self.eo_client_order_id_short)
    }
    /// M0fix（评审 P1-2）：有 Algo 身份的行一律在 **Algo 域**求证，与本地
    /// 状态无关——本地终态并不都来自交易所真值：`mark_submit_failed` 写的
    /// `failed` 是本地判断（StartedUnknown，写它时 venue 可能已经接受了这张
    /// Algo）。唯一例外是「已触发并绑定 actualOrderId」的形态：Algo 本身已
    /// 终结，剩下的是普通域子单。clientAlgoId 不是普通域身份，拿它去普通域
    /// 查只会得到错域的 -2013。
    fn routed_to_algo_domain(&self) -> bool {
        (self.algo_id().is_some() || self.client_algo_id().is_some())
            && !(self.po_status.trim() == "triggered" && self.venue_order_id().is_some())
    }
    /// P1-1：Algo 域的 -2013 是否仍落在请求身份的提交宽限内。
    fn algo_not_found_within_submit_grace(&self, now_ms: i64) -> bool {
        let in_flight = match self.eo_status.as_deref() {
            Some(status) => REQUEST_IN_FLIGHT_ENGINE_STATUSES.contains(&status.trim()),
            None => REQUEST_IN_FLIGHT_PROTECTION_STATUSES.contains(&self.po_status.trim()),
        };
        request_identity_not_found_within_grace(
            self.algo_id().is_some(),
            in_flight,
            self.po_updated_at_ms.max(self.eo_updated_at_ms),
            now_ms,
        )
    }
}

struct EngineProtectionRow {
    engine_order_id: String,
    status: String,
    exchange_order_id: Option<String>,
    client_order_id_short: Option<String>,
    symbol_key: String,
    updated_at_ms: i64,
}

impl EngineProtectionRow {
    fn venue_order_id(&self) -> Option<&str> {
        ProtectionRow::non_blank(&self.exchange_order_id)
    }
    fn client_order_id(&self) -> Option<&str> {
        ProtectionRow::non_blank(&self.client_order_id_short)
    }
    /// P1-1：普通域的 -2013 是否仍落在请求身份的提交宽限内。
    fn not_found_within_submit_grace(&self, now_ms: i64) -> bool {
        request_identity_not_found_within_grace(
            self.venue_order_id().is_some(),
            REQUEST_IN_FLIGHT_ENGINE_STATUSES.contains(&self.status.trim()),
            self.updated_at_ms,
            now_ms,
        )
    }
}

fn optional_string(row: &sqlx::any::AnyRow, column: &str) -> anyhow::Result<Option<String>> {
    crate::store::try_get_nullable_string(row, column)
        .map_err(|error| anyhow::anyhow!("decode {column}: {error}"))
}

async fn load_protection_rows(
    store: &Store,
    position_id: &str,
) -> anyhow::Result<Vec<ProtectionRow>> {
    let rows = sqlx::query(
        r#"
        SELECT po.protection_order_id, po.status AS po_status,
               po.exchange_algo_id, po.client_algo_id, po.exchange_order_id,
               po.engine_order_id,
               eo.status AS eo_status, eo.exchange_order_id AS eo_exchange_order_id,
               eo.client_order_id_short AS eo_client_order_id_short,
               po.updated_at_ms AS po_updated_at_ms,
               COALESCE(eo.updated_at_ms, po.updated_at_ms) AS eo_updated_at_ms
        FROM protection_orders po
        LEFT JOIN engine_orders eo ON eo.engine_order_id = po.engine_order_id
        WHERE po.position_id = ?
        ORDER BY po.protection_order_id
        "#,
    )
    .bind(position_id)
    .fetch_all(store.pool())
    .await
    .context("load protection rows for cleanup")?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        parsed.push(ProtectionRow {
            protection_order_id: row.get("protection_order_id"),
            engine_order_id: optional_string(&row, "engine_order_id")?,
            po_status: row.get("po_status"),
            exchange_algo_id: optional_string(&row, "exchange_algo_id")?,
            client_algo_id: optional_string(&row, "client_algo_id")?,
            exchange_order_id: optional_string(&row, "exchange_order_id")?,
            eo_status: optional_string(&row, "eo_status")?,
            eo_exchange_order_id: optional_string(&row, "eo_exchange_order_id")?,
            eo_client_order_id_short: optional_string(&row, "eo_client_order_id_short")?,
            po_updated_at_ms: row.get("po_updated_at_ms"),
            eo_updated_at_ms: row.get("eo_updated_at_ms"),
        });
    }
    Ok(parsed)
}

async fn load_engine_protection_rows(
    store: &Store,
    position_id: &str,
) -> anyhow::Result<Vec<EngineProtectionRow>> {
    let rows = sqlx::query(
        r#"
        SELECT engine_order_id, status, exchange_order_id, client_order_id_short, symbol_key,
               updated_at_ms
        FROM engine_orders
        WHERE position_id = ?
          AND order_role IN ('protection_sl','protection_tp','emergency')
        ORDER BY engine_order_id
        "#,
    )
    .bind(position_id)
    .fetch_all(store.pool())
    .await
    .context("load protection-role engine orders for cleanup")?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        parsed.push(EngineProtectionRow {
            engine_order_id: row.get("engine_order_id"),
            status: row.get("status"),
            exchange_order_id: optional_string(&row, "exchange_order_id")?,
            client_order_id_short: optional_string(&row, "client_order_id_short")?,
            symbol_key: row.get("symbol_key"),
            updated_at_ms: row.get("updated_at_ms"),
        });
    }
    Ok(parsed)
}

async fn drive_one(
    store: &Store,
    obligation: &ObligationRow,
    now: i64,
    venue_positions: &mut VenuePositionsRound,
) -> anyhow::Result<DriveOutcome> {
    let protection_rows = load_protection_rows(store, &obligation.position_id).await?;
    let engine_rows = load_engine_protection_rows(store, &obligation.position_id).await?;

    // 平凡完成之一：该仓根本没有任何保护单（计划 required=false 或裸 v1 仓）。
    if protection_rows.is_empty() && engine_rows.is_empty() {
        return Ok(DriveOutcome::Complete {
            reason: "no_protection_orders".to_string(),
        });
    }

    match crate::venue::credential_kind(&obligation.credential_id) {
        crate::venue::CredentialKind::Simulation => {
            // 需求⑤的兜底：平凡完成在关闭事务内判定（见
            // record_position_closed_in_tx）。走到这里说明模拟凭据上出现了
            // ACK 级 venue 身份——引擎的正常路径不可达，如实挂起说明原因，
            // 不假装完成、也不静默跳过。
            return Ok(DriveOutcome::Pending {
                note: "simulation credential carries venue-side protection identities; \
                       manual adjudication required"
                    .to_string(),
                outcomes: Vec::new(),
            });
        }
        crate::venue::CredentialKind::UnprovenIdentity => {
            return Ok(DriveOutcome::Pending {
                note: "credential identity is unproven; protection cleanup cannot drive"
                    .to_string(),
                outcomes: Vec::new(),
            });
        }
        crate::venue::CredentialKind::ExchangeBacked => {}
    }

    let Some(keys) = crate::binance_futures::keys_from_env(&obligation.credential_id)
        .map_err(anyhow::Error::msg)?
    else {
        return Ok(DriveOutcome::Pending {
            note: "exchange credentials are not configured; the venue orders may still be open"
                .to_string(),
            outcomes: Vec::new(),
        });
    };
    let client =
        BinanceFuturesClient::new_with_store(keys, &obligation.credential_id, store.clone())?;
    let limiter = CredentialRateLimiter::shared(crate::rate_limiter::TokenBucketConfig::from_env(
        crate::venue::venue_for_credential(&obligation.credential_id)
            .map_err(anyhow::Error::msg)?,
    ));

    let mut outcomes: Vec<serde_json::Value> = Vec::new();
    let mut problems: Vec<String> = Vec::new();

    // ⓪ M0fix（P0-1）：本轮要向交易所发任何撤单/查询之前，先同轮确认该
    //    scope 的交易所仓位精确为零。本地 closed 可能来自一份早于入场成交的
    //    陈旧 positionRisk 快照——那时所上的仓仍开着，撤掉它的 SL/TP 就是把
    //    一个活仓变成裸仓。非零 / 读失败 / 歧义一律不撤、挂起并记录。
    let venue_work = protection_rows
        .iter()
        .any(ProtectionRow::routed_to_algo_domain)
        || engine_rows
            .iter()
            .any(|row| engine_row_needs_ordinary_venue_call(row, &protection_rows));
    if venue_work {
        let exposure =
            match venue_positions_this_round(&client, &obligation.credential_id, venue_positions)
                .await
            {
                Ok(positions) => {
                    venue_scope_exposure(positions, &obligation.symbol_key, &obligation.direction)
                }
                Err(error) => {
                    return Ok(DriveOutcome::Pending {
                        note: format!(
                            "venue position read failed; no protection order is cancelled: {error}"
                        ),
                        outcomes: vec![serde_json::json!({
                            "target": "venue_position",
                            "outcome": "unreadable",
                            "reason": error,
                        })],
                    });
                }
            };
        match exposure {
            VenueScopeExposure::Flat => {}
            VenueScopeExposure::NonZero(detail) => {
                return Ok(DriveOutcome::Pending {
                    note: format!(
                        "venue position for {} {} is non-zero ({detail}); the protection belongs \
                         to a live position and is not cancelled",
                        obligation.symbol_key, obligation.direction
                    ),
                    outcomes: vec![serde_json::json!({
                        "target": "venue_position",
                        "outcome": "nonzero",
                        "reason": detail,
                    })],
                });
            }
            VenueScopeExposure::Ambiguous(detail) => {
                return Ok(DriveOutcome::Pending {
                    note: format!(
                        "venue position for {} {} cannot be proven zero ({detail}); no protection \
                         order is cancelled",
                        obligation.symbol_key, obligation.direction
                    ),
                    outcomes: vec![serde_json::json!({
                        "target": "venue_position",
                        "outcome": "ambiguous",
                        "reason": detail,
                    })],
                });
            }
        }
        match lifecycle_still_armed(store, obligation).await? {
            LifecycleCheck::Armed => {}
            LifecycleCheck::Superseded(note) => return Ok(DriveOutcome::Superseded { note }),
            LifecycleCheck::PositionNotClosed(state) => {
                return Ok(DriveOutcome::Pending {
                    note: format!(
                        "the local position is {state} again while its obligation is still pending; \
                         nothing is cancelled"
                    ),
                    outcomes: vec![serde_json::json!({
                        "target": "position_state",
                        "outcome": "not_closed",
                        "reason": state,
                    })],
                });
            }
        }
    }

    // ① Algo 先撤（与 kill-switch L3 / CancelOrder 同序）。每一条：真值 →
    //    仍未终态才发撤单 → 撤单后必须再读真值确认；请求被接受从不算完成。
    //    有 Algo 身份的行不论本地状态都在这里求证（P1-2）。
    for row in &protection_rows {
        if !row.routed_to_algo_domain() {
            continue;
        }
        let target = format!(
            "algo {}",
            row.algo_id()
                .or_else(|| row.client_algo_id())
                .unwrap_or("<unidentified>")
        );
        match confirm_algo_terminal(
            store,
            &client,
            &limiter,
            &obligation.credential_id,
            row,
            now,
        )
        .await
        {
            Ok(ConfirmTarget::Resolved(already)) => outcomes.push(serde_json::json!({
                "target": target,
                "outcome": already,
            })),
            Ok(ConfirmTarget::Unresolved(reason)) => {
                problems.push(format!("{target}: {reason}"));
                outcomes.push(serde_json::json!({
                    "target": target,
                    "outcome": "pending",
                    "reason": reason,
                }));
            }
            Err(error) => {
                let reason = format!("{error:#}");
                problems.push(format!("{target}: {reason}"));
                outcomes.push(serde_json::json!({
                    "target": target,
                    "outcome": "error",
                    "reason": reason,
                }));
            }
        }
    }

    // ② 再撤普通域：无 Algo 身份的 reduce-only 子单、pre-0013 遗留条件单、
    //    以及 Algo 触发后绑定的 actualOrderId 子单。链接到 Algo 身份行的
    //    引擎单不进普通域（clientAlgoId 不是普通域身份，混用会撤错域/查错域）。
    for row in &engine_rows {
        if !engine_status_not_terminal(&row.status) {
            continue;
        }
        if row.venue_order_id().is_none() && row.client_order_id().is_none() {
            // 无 venue 身份又非终态：可能仍在提交机内（pending_*）。关闭后的
            // 仓位不会再被派发（protection_dispatch authorize 拒绝已平仓位），
            // reconcile 的 -2013 裁决会把它们收敛到终态；在那之前保持挂起。
            problems.push(format!(
                "engine order {} has no venue identity and is still {}",
                row.engine_order_id, row.status
            ));
            continue;
        }
        if engine_row_routed_to_algo_domain(row, &protection_rows) {
            continue;
        }
        let Some(symbol) = crate::binance_futures::usdm_symbol(&row.symbol_key) else {
            problems.push(format!(
                "engine order {} symbol_key {} has no USDⓈ-M identity",
                row.engine_order_id, row.symbol_key
            ));
            continue;
        };
        let target = format!(
            "order {} {}",
            row.engine_order_id,
            row.venue_order_id()
                .or_else(|| row.client_order_id())
                .unwrap_or("<unidentified>")
        );
        match confirm_ordinary_terminal(
            store,
            &client,
            &limiter,
            &obligation.credential_id,
            &symbol,
            row,
            now,
        )
        .await
        {
            Ok(ConfirmTarget::Resolved(already)) => outcomes.push(serde_json::json!({
                "target": target,
                "outcome": already,
            })),
            Ok(ConfirmTarget::Unresolved(reason)) => {
                problems.push(format!("{target}: {reason}"));
                outcomes.push(serde_json::json!({
                    "target": target,
                    "outcome": "pending",
                    "reason": reason,
                }));
            }
            Err(error) => {
                let reason = format!("{error:#}");
                problems.push(format!("{target}: {reason}"));
                outcomes.push(serde_json::json!({
                    "target": target,
                    "outcome": "error",
                    "reason": reason,
                }));
            }
        }
    }

    // ③ 完成判据：本轮每个交易所目标都已由真值收敛（`problems` 为空），
    //    **且**两份权威表里已持久的状态全部终态。只看持久态不够（P1-2 更正
    //    此前的注释）：本地终态并不都来自交易所真值——`mark_submit_failed`
    //    写的 `failed` 是本地判断——所以一条本轮 Algo 域求证失败的 `failed`
    //    行不能因为「库里是终态」就算完成。①②里撤销请求被接受但真值未确认
    //    的行此刻仍是非终态，同样保持挂起。
    let unresolved = unresolved_protection_rows(store, &obligation.position_id).await?;
    let unresolved_engine =
        unresolved_engine_protection_rows(store, &obligation.position_id).await?;
    if problems.is_empty() && unresolved.is_empty() && unresolved_engine.is_empty() {
        return Ok(DriveOutcome::Complete {
            reason: "truth_confirmed".to_string(),
        });
    }
    let mut note = format!(
        "protection still live for closed position: {}",
        unresolved
            .iter()
            .chain(unresolved_engine.iter())
            .cloned()
            .collect::<Vec<_>>()
            .join("; ")
    );
    if !problems.is_empty() {
        note.push_str("; attempt problems: ");
        note.push_str(&problems.join("; "));
    }
    Ok(DriveOutcome::Pending { note, outcomes })
}

fn engine_status_not_terminal(status: &str) -> bool {
    !ENGINE_TERMINAL_STATUSES.contains(&status.trim())
}

/// 链接到 Algo 身份保护行的引擎单只在 Algo 域求证，不进普通域（clientAlgoId
/// 不是普通域身份，混用会撤错域/查错域；P1-2）。
fn engine_row_routed_to_algo_domain(
    row: &EngineProtectionRow,
    protection_rows: &[ProtectionRow],
) -> bool {
    protection_rows.iter().any(|po| {
        po.engine_order_id.as_deref() == Some(row.engine_order_id.as_str())
            && po.routed_to_algo_domain()
    })
}

/// 普通域这一轮会不会为这条引擎单向交易所发请求（与普通域循环的分流
/// 判据同源：非终态、有 venue 身份、不归 Algo 域、symbol 有 USDⓈ-M 身份）。
fn engine_row_needs_ordinary_venue_call(
    row: &EngineProtectionRow,
    protection_rows: &[ProtectionRow],
) -> bool {
    engine_status_not_terminal(&row.status)
        && (row.venue_order_id().is_some() || row.client_order_id().is_some())
        && !engine_row_routed_to_algo_domain(row, protection_rows)
        && crate::binance_futures::usdm_symbol(&row.symbol_key).is_some()
}

/// 完成判据的持久态重算（protection_orders 侧）。
async fn unresolved_protection_rows(
    store: &Store,
    position_id: &str,
) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT po.protection_order_id, po.status AS po_status,
               po.exchange_algo_id, po.client_algo_id, po.exchange_order_id,
               eo.status AS eo_status
        FROM protection_orders po
        LEFT JOIN engine_orders eo ON eo.engine_order_id = po.engine_order_id
        WHERE po.position_id = ?
        "#,
    )
    .bind(position_id)
    .fetch_all(store.pool())
    .await
    .context("recompute protection cleanup resolution")?;
    let mut unresolved = Vec::new();
    for row in rows {
        let po_status: String = row.get("po_status");
        if PROTECTION_TERMINAL_STATUSES.contains(&po_status.trim()) {
            continue;
        }
        let eo_status: Option<String> = optional_string(&row, "eo_status")?;
        let eo_terminal = eo_status
            .as_deref()
            .is_some_and(|status| !engine_status_not_terminal(status));
        if eo_terminal {
            // 触发后的 actualOrderId 子单已终态，或本地引擎订单已被真值终态化。
            continue;
        }
        unresolved.push(row.get::<String, _>("protection_order_id"));
    }
    Ok(unresolved)
}

/// 完成判据的持久态重算（engine_orders 保护角色侧）。
async fn unresolved_engine_protection_rows(
    store: &Store,
    position_id: &str,
) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT engine_order_id, status, exchange_order_id, client_order_id_short
        FROM engine_orders
        WHERE position_id = ?
          AND order_role IN ('protection_sl','protection_tp','emergency')
        "#,
    )
    .bind(position_id)
    .fetch_all(store.pool())
    .await
    .context("recompute engine protection cleanup resolution")?;
    let mut unresolved = Vec::new();
    for row in rows {
        let status: String = row.get("status");
        if !engine_status_not_terminal(&status) {
            continue;
        }
        unresolved.push(row.get::<String, _>("engine_order_id"));
    }
    Ok(unresolved)
}

enum ConfirmTarget {
    /// 已终态；携带的字符串说明是「真值确认撤销」「真值本就终态」还是
    /// 「venue 查无此单（-2013）」。
    Resolved(&'static str),
    Unresolved(String),
}

/// Algo 域单条保护单的撤单 + 真值确认。撤单复用 `CancelOrder` RPC / L3
/// 的同一原语（`cancel_algo_order_by_identity_json`），真值复用 reconcile
/// 单查的同一读法与同一落库器（`apply_algo_truth_for_protection`）。
async fn confirm_algo_terminal(
    store: &Store,
    client: &BinanceFuturesClient,
    limiter: &CredentialRateLimiter,
    credential_id: &str,
    row: &ProtectionRow,
    now: i64,
) -> anyhow::Result<ConfirmTarget> {
    let query_algo_id = row
        .algo_id()
        .and_then(|value| value.parse::<i64>().ok())
        .map(BinanceAlgoId::new);
    let query_client_algo_id = (query_algo_id.is_none())
        .then(|| row.client_algo_id().map(BinanceClientAlgoId::new))
        .flatten();
    if query_algo_id.is_none() && query_client_algo_id.is_none() {
        return Ok(ConfirmTarget::Unresolved(
            "missing Algo identity; cannot cancel through the venue".to_string(),
        ));
    }

    // 真值先行：已终态就不用发撤单请求；venue 查无此单（-2013）同理——
    // 那侧不可能再有会触发的单，按本地终态 canceled 收敛（与 CancelOrder
    // RPC 的落库 SQL 同形）。
    match read_algo_truth(
        client,
        limiter,
        query_algo_id.as_ref(),
        query_client_algo_id.as_ref(),
    )
    .await?
    {
        None => {
            // P1-1：只有请求身份、仍在途、且还在提交宽限内 ⇒ 不是终局。
            if row.algo_not_found_within_submit_grace(now) {
                return Ok(ConfirmTarget::Unresolved(format!(
                    "venue reports the request-only Algo identity unknown (-2013) within the \
                     {}ms submit grace; a late-landing submit may still create it",
                    crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS
                )));
            }
            mark_algo_canceled_local(store, credential_id, row, now).await?;
            return Ok(ConfirmTarget::Resolved("venue_unknown_terminal"));
        }
        Some(truth) => {
            let disposition = crate::protection::apply_algo_truth_for_protection(
                store,
                credential_id,
                &row.protection_order_id,
                &truth,
                crate::protection::AlgoTruthSource::RestAlgoQuery,
                "protection_cleanup",
                now,
            )
            .await?;
            match disposition {
                crate::protection::AlgoTruthDisposition::Missing => {
                    return Ok(ConfirmTarget::Resolved("truth_terminal"));
                }
                crate::protection::AlgoTruthDisposition::Triggered => {
                    return Ok(ConfirmTarget::Unresolved(
                        "algo has triggered; its bound actual order moves to the ordinary pass"
                            .to_string(),
                    ));
                }
                crate::protection::AlgoTruthDisposition::Active => {}
                crate::protection::AlgoTruthDisposition::Unknown => {
                    return Ok(ConfirmTarget::Unresolved(
                        "algo truth is not decodable to a terminal state".to_string(),
                    ));
                }
            }
        }
    }

    // 仍在 venue 上活着：撤（请求被接受绝不是完成）。撤单请求失败不直接
    // 定论——真 Binance 对「查无此单」的撤单回 -2011/-2013，随后那次真值
    // 读才是判据；真值也读不到才留待退避重试。
    let cancel_error = client
        .cancel_algo_order_by_identity_json(
            limiter,
            query_algo_id.as_ref(),
            query_client_algo_id.as_ref(),
            RequestClass::Protection,
        )
        .await
        .err();

    // 撤单后的真值确认才算数。
    let truth = read_algo_truth(
        client,
        limiter,
        query_algo_id.as_ref(),
        query_client_algo_id.as_ref(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("algo truth disappeared after a failed cancel read"))?;
    let disposition = crate::protection::apply_algo_truth_for_protection(
        store,
        credential_id,
        &row.protection_order_id,
        &truth,
        crate::protection::AlgoTruthSource::RestAlgoQuery,
        "protection_cleanup",
        now,
    )
    .await?;
    match disposition {
        crate::protection::AlgoTruthDisposition::Missing => {
            Ok(ConfirmTarget::Resolved("canceled_and_truth_confirmed"))
        }
        _ => {
            let reason = match cancel_error {
                Some(error) => format!("algo cancel failed and truth is not terminal: {error:#}"),
                None => "algo still live after the cancel request".to_string(),
            };
            Ok(ConfirmTarget::Unresolved(reason))
        }
    }
}

/// Algo 域 -2013 的本地收敛：与 `CancelOrder` RPC 的两条 UPDATE 同形
/// （engine_orders 置 canceled；protection_orders 的 algo_status/status 同步）。
async fn mark_algo_canceled_local(
    store: &Store,
    credential_id: &str,
    row: &ProtectionRow,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE engine_orders SET status='canceled', updated_at_ms=? \
         WHERE credential_id=? AND engine_order_id=? AND status IN \
           ('pending_risk','pending_submit','unknown','submitted','acked','partial_filled',\
            'confirmed','triggering','triggered')",
    )
    .bind(now)
    .bind(credential_id)
    .bind(row.engine_order_id.clone().unwrap_or_default())
    .execute(store.pool())
    .await
    .context("persist algo protection terminal status")?;
    sqlx::query(
        "UPDATE protection_orders \
         SET algo_status='CANCELED', algo_updated_at_ms=?, status='canceled', \
             verified_from_exchange=0, updated_at_ms=? \
         WHERE credential_id=? AND protection_order_id=? \
           AND status NOT IN ('canceled','expired','failed')",
    )
    .bind(now)
    .bind(now)
    .bind(credential_id)
    .bind(&row.protection_order_id)
    .execute(store.pool())
    .await
    .context("persist protection terminal status")?;
    tracing::info!(
        credential_id,
        protection_order_id = %row.protection_order_id,
        "protection cleanup resolved an algo-domain order: venue reports unknown order"
    );
    Ok(())
}

async fn read_algo_truth(
    client: &BinanceFuturesClient,
    limiter: &CredentialRateLimiter,
    algo_id: Option<&BinanceAlgoId>,
    client_algo_id: Option<&BinanceClientAlgoId>,
) -> anyhow::Result<Option<crate::protection::AlgoOrderTruth>> {
    match client
        .get_algo_order_json(
            limiter,
            algo_id,
            client_algo_id,
            RequestClass::PeriodicReconcile,
        )
        .await
    {
        Ok(value) => Ok(Some(
            crate::protection::AlgoOrderTruth::from_json(value)
                .context("decode Binance algo truth for cleanup")?,
        )),
        Err(error) if is_order_unknown_on_venue(&error) => Ok(None),
        Err(error) => Err(error).context("query algo truth for cleanup"),
    }
}

/// 普通域单条保护子单的撤单 + 真值确认（`cancel_order_json` / `get_order_json`
/// 与 `CancelOrder` RPC、reconcile 同一读法）。`-2013` = venue 侧不存在该
/// 单 ⇒ 不可能再触发 ⇒ 按本地终态 `canceled` 收敛（与 CancelOrder RPC 的
/// 落库 SQL 同形）。
async fn confirm_ordinary_terminal(
    store: &Store,
    client: &BinanceFuturesClient,
    _limiter: &CredentialRateLimiter,
    credential_id: &str,
    symbol: &str,
    row: &EngineProtectionRow,
    now: i64,
) -> anyhow::Result<ConfirmTarget> {
    let read_truth = || async {
        match client
            .get_order_json(
                symbol,
                row.venue_order_id(),
                row.client_order_id(),
                RequestClass::PeriodicReconcile,
            )
            .await
        {
            Ok(value) => Ok(Some(value)),
            Err(error) if is_order_unknown_on_venue(&error) => Ok(None),
            Err(error) => Err(error).context("query order truth for cleanup"),
        }
    };

    let truth = read_truth().await?;
    if let Some(status) = truth
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(|value| value.as_str())
    {
        if let Some(resolved) = ordinary_terminal_status(status) {
            persist_ordinary_terminal(
                store,
                credential_id,
                &row.engine_order_id,
                &format!("venue status {status}"),
                resolved,
                now,
            )
            .await?;
            return Ok(ConfirmTarget::Resolved("truth_terminal"));
        }
    } else if truth.is_none() {
        // -2013：venue 查无此单。P1-1：只有请求身份、仍在途、且还在提交
        // 宽限内 ⇒ 不是终局（迟到的提交仍可能落地）。
        if row.not_found_within_submit_grace(now) {
            return Ok(ConfirmTarget::Unresolved(format!(
                "venue reports the request-only order identity unknown (-2013) within the \
                 {}ms submit grace; a late-landing submit may still create it",
                crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS
            )));
        }
        persist_ordinary_terminal(
            store,
            credential_id,
            &row.engine_order_id,
            "venue order unknown (-2013)",
            "canceled",
            now,
        )
        .await?;
        return Ok(ConfirmTarget::Resolved("venue_unknown_terminal"));
    }

    // 撤单请求失败不直接定论：真 Binance 对「查无此单」的撤单回
    // -2011/-2013，随后那次真值读才是判据（与 Algo 域同一不变量——
    // 请求被接受/被拒绝都不是完成，真值才是）。
    let cancel_error = client
        .cancel_order_json(
            symbol,
            row.venue_order_id(),
            row.client_order_id(),
            RequestClass::Exit,
        )
        .await
        .err();

    let truth = read_truth().await?;
    if truth.is_none() {
        // -2013：venue 查无此单 ⇒ 终局。走到这里说明本轮第一次真值读已经
        // 在所上看到过这张单（迟到提交的竞态已经落地），撤单后它不见了；
        // P1-1 的提交宽限只适用于「从未在所上见过」的请求身份。
        persist_ordinary_terminal(
            store,
            credential_id,
            &row.engine_order_id,
            "venue order unknown (-2013) after cancel",
            "canceled",
            now,
        )
        .await?;
        return Ok(ConfirmTarget::Resolved("venue_unknown_terminal"));
    }
    let status = truth
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("order truth absent after a failed cancel read"))?;
    if let Some(resolved) = ordinary_terminal_status(status) {
        persist_ordinary_terminal(
            store,
            credential_id,
            &row.engine_order_id,
            &format!("venue status {status} after cancel"),
            resolved,
            now,
        )
        .await?;
        return Ok(ConfirmTarget::Resolved("canceled_and_truth_confirmed"));
    }
    Ok(ConfirmTarget::Unresolved(match cancel_error {
        Some(error) => format!(
            "order cancel failed and truth is not terminal (venue status {status}): {error:#}"
        ),
        None => format!("order still live after the cancel request (venue status {status})"),
    }))
}

fn ordinary_terminal_status(status: &str) -> Option<&'static str> {
    match status.trim().to_ascii_uppercase().as_str() {
        "CANCELED" | "CANCELLED" => Some("canceled"),
        "EXPIRED" => Some("expired"),
        "REJECTED" => Some("rejected"),
        "FILLED" => Some("filled"),
        _ => None,
    }
}

/// 普通域终态落库：与 `CancelOrder` RPC 的两条 UPDATE 同形（engine_orders
/// 置终态；链接的 protection_orders 行同步置 canceled）。
async fn persist_ordinary_terminal(
    store: &Store,
    credential_id: &str,
    engine_order_id: &str,
    note: &str,
    status: &'static str,
    now: i64,
) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "UPDATE engine_orders SET status=?, updated_at_ms=? \
         WHERE credential_id=? AND engine_order_id=? AND status IN \
           ('pending_risk','pending_submit','unknown','submitted','acked','partial_filled',\
            'confirmed','triggering','triggered')",
    )
    .bind(status)
    .bind(now)
    .bind(credential_id)
    .bind(engine_order_id)
    .execute(store.pool())
    .await
    .context("persist ordinary protection terminal status")?
    .rows_affected();
    sqlx::query(
        "UPDATE protection_orders SET status='canceled', updated_at_ms=? \
         WHERE credential_id=? AND engine_order_id=? \
           AND status NOT IN ('canceled','expired','failed')",
    )
    .bind(now)
    .bind(credential_id)
    .bind(engine_order_id)
    .execute(store.pool())
    .await
    .context("persist linked protection terminal status")?;
    tracing::info!(
        credential_id,
        engine_order_id,
        status,
        note,
        updated_engine_rows = rows,
        "protection cleanup resolved an ordinary-domain protection order"
    );
    Ok(())
}

#[cfg(test)]
mod protection_cleanup_tests {
    include!("protection_cleanup_tests.rs");
}
