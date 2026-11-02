use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tokio::sync::Mutex;

use crate::binance_futures::{
    BinanceAlgoId, BinanceAlgoOrderResponse, BinanceClientAlgoId, BinanceFuturesClient, BinanceKeys,
};
use crate::binance_ws_api::BinanceWsApiClient;
use crate::rate_limiter::{CredentialRateLimiter, RequestClass};
use crate::store::Store;
use crate::svc::{binance_order_status_to_engine_status, now_ms, EventContext};

const ORPHAN_POSITION_BOT_PREFIX: &str = "reconcile_orphan_";
const DEFAULT_FULL_SCAN_INTERVAL_MS: u64 = 600_000;
const MIN_FULL_SCAN_INTERVAL_MS: u64 = 120_000;
const MAX_FULL_SCAN_INTERVAL_MS: u64 = 3_600_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReconcileTier {
    FullScan,
    SymbolNarrowed { symbols: Vec<String> },
    StreamOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "symbols")]
pub(crate) enum Coverage {
    Full,
    Symbols(Vec<String>),
}

impl Coverage {
    fn includes_symbol_key(&self, symbol_key: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Symbols(symbols) => crate::binance_futures::usdm_symbol(symbol_key)
                .is_some_and(|symbol| symbols.iter().any(|covered| covered == &symbol)),
        }
    }

    fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Clone)]
struct CoveredReconcileTruth {
    truth: crate::kill_switch::ReconcileTruth,
    coverage: Coverage,
}

impl CoveredReconcileTruth {
    fn flatten_complete(self) -> bool {
        matches!(self.coverage, Coverage::Full) && self.truth.flatten_complete()
    }
}

#[derive(Debug, Clone)]
struct LatestPeriodicReconcileTruth {
    observed_at_ms: i64,
    covered: CoveredReconcileTruth,
}

/// A process-local handoff from the periodic reconciler to user-stream recovery.
///
/// The pool address is the store identity: cloned `Store`s share the same pool,
/// while an isolated database using the same credential id cannot leak its truth
/// into this store. Process restart deliberately clears the handoff; startup fill
/// recovery then performs its normal backfill + full reconcile before anything can
/// become ready.
fn latest_periodic_truth_registry(
) -> &'static std::sync::Mutex<HashMap<(usize, String), LatestPeriodicReconcileTruth>> {
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<HashMap<(usize, String), LatestPeriodicReconcileTruth>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn store_truth_identity(store: &Store) -> usize {
    store.pool() as *const sqlx::Pool<sqlx::Any> as usize
}

fn remember_latest_periodic_reconcile_truth(
    store: &Store,
    credential_id: &str,
    observed_at_ms: i64,
    covered: CoveredReconcileTruth,
) {
    let key = (store_truth_identity(store), credential_id.to_string());
    let mut registry = latest_periodic_truth_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if registry
        .get(&key)
        .is_some_and(|existing| existing.observed_at_ms > observed_at_ms)
    {
        return;
    }
    registry.insert(
        key,
        LatestPeriodicReconcileTruth {
            observed_at_ms,
            covered,
        },
    );
}

#[cfg(test)]
pub(crate) fn remember_full_periodic_reconcile_truth_for_test(
    store: &Store,
    credential_id: &str,
    observed_at_ms: i64,
    truth: crate::kill_switch::ReconcileTruth,
) {
    remember_latest_periodic_reconcile_truth(
        store,
        credential_id,
        observed_at_ms,
        CoveredReconcileTruth {
            truth,
            coverage: Coverage::Full,
        },
    );
}

/// Return only a full-account periodic truth observed after the failed recovery
/// cycle. Symbol-narrowed truth cannot prove that uncovered order symbols are
/// converged and therefore cannot reopen the new-entry gate.
pub(crate) fn latest_full_periodic_reconcile_truth_since(
    store: &Store,
    credential_id: &str,
    not_before_ms: i64,
) -> Option<crate::kill_switch::ReconcileTruth> {
    let key = (store_truth_identity(store), credential_id.to_string());
    latest_periodic_truth_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
        .filter(|latest| {
            latest.observed_at_ms >= not_before_ms
                && matches!(latest.covered.coverage, Coverage::Full)
        })
        .map(|latest| latest.covered.truth)
}

fn credential_stagger_ms(credential_id: &str, stagger_window_ms: u64) -> u64 {
    if stagger_window_ms == 0 {
        return 0;
    }
    let digest = crate::svc::sha256_hex(credential_id);
    u64::from_str_radix(&digest[..16], 16).expect("sha256 prefix is hexadecimal")
        % stagger_window_ms
}

/// Pure three-way tier decision. `stagger_ms` is the size of the deterministic
/// hash window; production passes the full-scan interval, yielding exactly
/// `hash(credential_id) % full_scan_interval_ms`.
pub(crate) fn reconcile_tier(
    credential_id: &str,
    active_symbols: &[String],
    now_ms: i64,
    last_full_scan_ms: i64,
    full_scan_interval_ms: u64,
    stagger_ms: u64,
) -> ReconcileTier {
    let elapsed_ms = now_ms.saturating_sub(last_full_scan_ms).max(0) as u64;
    let due_after_ms =
        full_scan_interval_ms.saturating_add(credential_stagger_ms(credential_id, stagger_ms));
    if elapsed_ms >= due_after_ms {
        ReconcileTier::FullScan
    } else if active_symbols.is_empty() {
        ReconcileTier::StreamOnly
    } else {
        let mut symbols = active_symbols.to_vec();
        symbols.sort();
        symbols.dedup();
        ReconcileTier::SymbolNarrowed { symbols }
    }
}

fn reconcile_tier_with_safety_fallbacks(
    credential_id: &str,
    active_symbols: &[String],
    stream_ready: bool,
    l3_latched: bool,
    now_ms: i64,
    last_full_scan_ms: i64,
    full_scan_interval_ms: u64,
) -> ReconcileTier {
    if l3_latched {
        return ReconcileTier::FullScan;
    }
    let tier = reconcile_tier(
        credential_id,
        active_symbols,
        now_ms,
        last_full_scan_ms,
        full_scan_interval_ms,
        full_scan_interval_ms,
    );
    if matches!(tier, ReconcileTier::StreamOnly) && !stream_ready {
        ReconcileTier::FullScan
    } else {
        tier
    }
}

pub(crate) fn periodic_reconcile_full_scan_interval_ms() -> u64 {
    std::env::var("RUSTCORE_RECONCILE_FULL_SCAN_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_FULL_SCAN_INTERVAL_MS)
        .clamp(MIN_FULL_SCAN_INTERVAL_MS, MAX_FULL_SCAN_INTERVAL_MS)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalPositionObservation {
    position_id: String,
    bot_uid: String,
    symbol_key: String,
    direction: String,
    entry_epoch: i64,
    state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OrphanPositionDisposition {
    IgnoreZero,
    MatchedActive {
        position_id: String,
    },
    ReopenClosed {
        position_id: String,
        bot_uid: String,
        symbol_key: String,
        direction: String,
        entry_epoch: i64,
    },
    CreateUnowned {
        symbol_key: String,
        direction: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct OrphanPositionRecovery {
    position_id: String,
    symbol_key: String,
    direction: String,
    qty: String,
    recovery: String,
    #[serde(skip)]
    bot_uid: String,
    #[serde(skip)]
    entry_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PositionSnapshotOutcome {
    all_positions_zero: bool,
    active_local_positions: i64,
    local_position_zero_diff: bool,
    orphan_positions: Vec<OrphanPositionRecovery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconcileDiffCauses {
    missing_open_orders: bool,
    missing_open_algo_orders: bool,
    unresolved_algo_orders: bool,
    orphan_positions: bool,
    /// 本地在途单的 `symbol_key` 解析不出 USDⓈ-M 闭世界身份
    /// 。这条单**永远无法**与交易所真值对齐：
    /// 不能查、不能裁决、也不能被 open 集合认领。它是一条 diff，
    /// 不是"没事"——沉默等于让一条不可对账的单留在在途集合里。
    unidentified_local_orders: bool,
    terminal_local_remote_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReconcileDisposition {
    Clean,
    Degrade(ReconcileDiffCauses),
}

fn canonical_usdt_symbol_key(exchange_symbol: &str) -> Result<String, String> {
    let symbol = exchange_symbol.trim().to_uppercase();
    let Some(base) = symbol.strip_suffix("USDT") else {
        return Err(format!(
            "Binance USDT-M position symbol is not USDT quoted: {symbol}"
        ));
    };
    if base.is_empty() {
        return Err("Binance USDT-M position symbol has an empty base asset".to_string());
    }
    Ok(format!("binance|futures|{base}|USDT|USDT"))
}

fn exchange_position_direction(position_side: Option<&str>, qty: &str) -> Result<String, String> {
    let side = position_side.unwrap_or("BOTH").trim().to_uppercase();
    match side.as_str() {
        "LONG" => Ok("long".to_string()),
        "SHORT" => Ok("short".to_string()),
        "BOTH" => Ok(if qty.trim().starts_with('-') {
            "short".to_string()
        } else {
            "long".to_string()
        }),
        _ => Err(format!("unsupported Binance positionSide: {side}")),
    }
}

fn orphan_position_disposition(
    exchange_symbol: &str,
    position_side: Option<&str>,
    qty: &str,
    local_positions: &[LocalPositionObservation],
) -> Result<OrphanPositionDisposition, String> {
    if crate::position_state::is_decimal_zero(qty) {
        return Ok(OrphanPositionDisposition::IgnoreZero);
    }
    let direction = exchange_position_direction(position_side, qty)?;
    // **orphan 匹配也是身份判定。**
    // 裸 `base+quote` 会让一条币本位/异 venue 的本地仓"认领"USDⓈ-M 的
    // 交易所仓位，于是真实 orphan 被吸收成"已匹配"，而认领它的那条本地仓
    // 又拿到了别的合约的数量。判据与 WS/对账/提交同源。
    let matching = local_positions.iter().filter(|position| {
        position.direction == direction
            && crate::binance_futures::usdm_symbol_key_matches(
                &position.symbol_key,
                exchange_symbol,
            )
    });
    if let Some(active) = matching.clone().find(|position| position.state != "closed") {
        return Ok(OrphanPositionDisposition::MatchedActive {
            position_id: active.position_id.clone(),
        });
    }
    if let Some(closed) = matching
        .filter(|position| position.state == "closed")
        .max_by_key(|position| position.entry_epoch)
    {
        return Ok(OrphanPositionDisposition::ReopenClosed {
            position_id: closed.position_id.clone(),
            bot_uid: closed.bot_uid.clone(),
            symbol_key: closed.symbol_key.clone(),
            direction,
            entry_epoch: closed.entry_epoch,
        });
    }
    Ok(OrphanPositionDisposition::CreateUnowned {
        symbol_key: canonical_usdt_symbol_key(exchange_symbol)?,
        direction,
    })
}

fn reconcile_disposition(
    missing_open_orders: bool,
    missing_open_algo_orders: bool,
    unresolved_algo_orders: bool,
    orphan_positions: bool,
    unidentified_local_orders: bool,
    terminal_local_remote_active: bool,
) -> ReconcileDisposition {
    let causes = ReconcileDiffCauses {
        missing_open_orders,
        missing_open_algo_orders,
        unresolved_algo_orders,
        orphan_positions,
        unidentified_local_orders,
        terminal_local_remote_active,
    };
    if causes.missing_open_orders
        || causes.missing_open_algo_orders
        || causes.unresolved_algo_orders
        || causes.orphan_positions
        || causes.unidentified_local_orders
        || causes.terminal_local_remote_active
    {
        ReconcileDisposition::Degrade(causes)
    } else {
        ReconcileDisposition::Clean
    }
}

fn disposition_has_diff(disposition: &ReconcileDisposition) -> bool {
    match disposition {
        ReconcileDisposition::Clean => false,
        ReconcileDisposition::Degrade(_) => true,
    }
}

fn orphan_bot_uid(credential_id: &str) -> String {
    let digest = crate::svc::sha256_hex(&format!("orphan-position-owner|{credential_id}"));
    format!("{ORPHAN_POSITION_BOT_PREFIX}{}", &digest[..24])
}

async fn recover_orphan_position(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    qty: &str,
    entry_price: Option<&str>,
    ts: i64,
    disposition: OrphanPositionDisposition,
) -> anyhow::Result<Option<OrphanPositionRecovery>> {
    let entry_price = entry_price.filter(|value| !value.trim().is_empty());
    match disposition {
        OrphanPositionDisposition::IgnoreZero | OrphanPositionDisposition::MatchedActive { .. } => {
            Ok(None)
        }
        OrphanPositionDisposition::ReopenClosed {
            position_id,
            bot_uid,
            symbol_key,
            direction,
            entry_epoch,
        } => {
            let active_key = crate::position_state::active_key(&bot_uid, &symbol_key, &direction);
            let reopened = sqlx::query(
                r#"
                UPDATE position_state
                SET close_revision=close_revision+1, state='open', active_key=?, pending_intent_id=NULL,
                    pending_idem_key=NULL, qty=?, entry_price=COALESCE(?, entry_price),
                    opened_at_ms=COALESCE(opened_at_ms, ?), closed_at_ms=NULL,
                    last_reconciled_ms=?, updated_at_ms=?
                WHERE credential_id=? AND position_id=? AND state='closed'
                "#,
            )
            .bind(active_key)
            .bind(crate::position_state::decimal_abs(qty))
            .bind(entry_price)
            .bind(ts)
            .bind(ts)
            .bind(ts)
            .bind(credential_id)
            .bind(&position_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            // M0fix（评审 P0-1）：同一个 position_id 的新生命周期开始了。上一次
            // （可能来自陈旧零仓快照的）关闭武装的未完成善后义务在**本事务**
            // 里退役——所上那些保护单属于这个仍开着的仓，旧义务不能再撤它们。
            if reopened == 1 {
                crate::protection_cleanup::retire_obligations_on_reopen_in_tx(tx, &position_id, ts)
                    .await
                    .map_err(|status| anyhow::anyhow!(status.message().to_string()))?;
            }
            // A reconciled reopen is not a new admitted Entry transaction.
            // The previous source proof cannot authorize closing the new lifecycle.
            sqlx::query("DELETE FROM position_execution_provenance WHERE position_id=?")
                .bind(&position_id)
                .execute(&mut **tx)
                .await?;
            Ok(Some(OrphanPositionRecovery {
                position_id,
                symbol_key,
                direction,
                qty: crate::position_state::decimal_abs(qty).to_string(),
                recovery: "reopened_closed".to_string(),
                bot_uid,
                entry_epoch,
            }))
        }
        OrphanPositionDisposition::CreateUnowned {
            symbol_key,
            direction,
        } => {
            let bot_uid = orphan_bot_uid(credential_id);
            let row = sqlx::query(
                r#"
                SELECT COALESCE(MAX(entry_epoch), 0) AS max_epoch
                FROM position_state
                WHERE bot_uid=? AND symbol_key=? AND direction=?
                "#,
            )
            .bind(&bot_uid)
            .bind(&symbol_key)
            .bind(&direction)
            .fetch_one(&mut **tx)
            .await?;
            let entry_epoch = row.get::<i64, _>("max_epoch") + 1;
            let position_id = format!("pos_{}", ulid::Ulid::new());
            let active_key = crate::position_state::active_key(&bot_uid, &symbol_key, &direction);
            sqlx::query(
                r#"
                INSERT INTO position_state (
                  position_id, credential_id, bot_uid, symbol_key, direction,
                  entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
                  qty, entry_price, opened_at_ms, closed_at_ms,
                  last_reconciled_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, NULL, NULL, ?, ?, ?, NULL, ?, ?)
                "#,
            )
            .bind(&position_id)
            .bind(credential_id)
            .bind(&bot_uid)
            .bind(&symbol_key)
            .bind(&direction)
            .bind(entry_epoch)
            .bind(active_key)
            .bind(crate::position_state::decimal_abs(qty))
            .bind(entry_price)
            .bind(ts)
            .bind(ts)
            .bind(ts)
            .execute(&mut **tx)
            .await?;
            Ok(Some(OrphanPositionRecovery {
                position_id,
                symbol_key,
                direction,
                qty: crate::position_state::decimal_abs(qty).to_string(),
                recovery: "created_unowned".to_string(),
                bot_uid,
                entry_epoch,
            }))
        }
    }
}

/// 周期对账每一轮从哪里取交易所真值。
///
/// **生产构建里这个 enum 只有 `Exchange` 一个 variant**——`Stub` 带
/// `#[cfg(test)]`，编译不进生产二进制。`Exchange` 分支体与它替换掉的那次
/// 调用逐字相同（`reconcile_binance_futures(credential_id, store, limiter,
/// keys)`），所以生产路径的行为没有任何变化。
///
/// 为什么需要它：段③ R1 评审 SEG3-R1-001 要求从**生产入口**证明
/// 「开仓被风险闸门拒绝时，启动对账仍然真的执行过」。原来的
/// `disabled_for_tests` 把整条任务关掉，于是这条接线**零测试覆盖**——
/// 删掉 `svc.rs` 风控前那次 `ensure_running`，全量 683 项照样全绿。
/// 而周期对账会**联真实交易所**（REST），engine safety contract 要求交易路径
/// 测试默认 dry-run，所以出口不能是"让它联网"，只能是"同一条 spawn／
/// 注册／启动对账路径，交易所那一端换成无网络的响应队列桩"。
#[derive(Clone)]
enum ReconcileExchangeSource {
    /// 生产：每一轮用凭据密钥现构造一个真实的 Binance 客户端并联网。
    Exchange,
    /// 测试出口：`BinanceFuturesClient::new_with_mock_responses` 的内存队列。
    #[cfg(test)]
    Stub(BinanceFuturesClient),
}

/// The only seam that may switch the periodic account balance/position reads.
/// Order truth, kill-switch truth and every submission path remain REST-only.
enum AccountReadSource<'a> {
    WsApi(&'a BinanceWsApiClient),
    Rest,
}

struct AccountReadOutcome {
    positions: serde_json::Value,
    position_coverage: Coverage,
    balances: serde_json::Value,
    source: &'static str,
    fallback_reason: Option<String>,
}

fn validate_position_financial_fields(positions: &serde_json::Value) -> anyhow::Result<()> {
    let positions = positions
        .as_array()
        .context("position snapshot is not an array")?;
    for position in positions {
        // Zero-quantity rows still supply configured leverage for the next
        // entry. They may report explicit decimal zero prices, but missing
        // fields must never overwrite a valid prior observation with "".
        for field in ["entryPrice", "markPrice", "unRealizedProfit"] {
            let value = position
                .get(field)
                .and_then(serde_json::Value::as_str)
                .with_context(|| format!("position snapshot has missing or non-string {field}"))?;
            crate::svc_v2::validate_account_decimal(value, field)
                .with_context(|| format!("position snapshot has invalid {field}"))?;
        }
        let leverage = position
            .get("leverage")
            .and_then(serde_json::Value::as_str)
            .context("position snapshot has missing or non-string leverage")?;
        crate::svc_v2::parse_account_leverage(leverage)
            .context("position snapshot has invalid leverage")?;
    }
    Ok(())
}

async fn read_account_truth(
    source: AccountReadSource<'_>,
    rest: &BinanceFuturesClient,
    coverage: &Coverage,
) -> anyhow::Result<AccountReadOutcome> {
    if let AccountReadSource::WsApi(ws) = source {
        // One-symbol narrowed rounds preserve that scope on ws-fapi. For a
        // multi-symbol round the endpoint has no multi-symbol parameter, so an
        // account-wide read is the only single-request representation.
        let symbol = match coverage {
            Coverage::Symbols(symbols) if symbols.len() == 1 => symbols.first().map(String::as_str),
            Coverage::Full | Coverage::Symbols(_) => None,
        };
        let response = ws
            .account_reads(symbol)
            .await
            .and_then(|(positions, balances)| {
                validate_position_financial_fields(&positions)?;
                Ok((positions, balances))
            });
        match response {
            Ok((positions, balances)) => {
                return Ok(AccountReadOutcome {
                    positions,
                    position_coverage: symbol
                        .map(|s| Coverage::Symbols(vec![s.to_string()]))
                        .unwrap_or(Coverage::Full),
                    balances,
                    source: "ws",
                    fallback_reason: None,
                });
            }
            Err(error) => {
                let reason = format!("{error:#}");
                let positions = rest.positions(RequestClass::PeriodicReconcile).await?;
                let balances = rest.balances(RequestClass::PeriodicReconcile).await?;
                validate_position_financial_fields(&positions)?;
                return Ok(AccountReadOutcome {
                    positions,
                    position_coverage: Coverage::Full,
                    balances,
                    source: "rest",
                    fallback_reason: Some(reason),
                });
            }
        }
    }
    let positions = rest.positions(RequestClass::PeriodicReconcile).await?;
    let balances = rest.balances(RequestClass::PeriodicReconcile).await?;
    validate_position_financial_fields(&positions)?;
    Ok(AccountReadOutcome {
        positions,
        position_coverage: Coverage::Full,
        balances,
        source: "rest",
        fallback_reason: None,
    })
}

async fn last_full_scan_ms(store: &Store, credential_id: &str) -> anyhow::Result<i64> {
    let row = sqlx::query(
        "SELECT CAST(last_full_scan_ms AS CHAR) AS last_full_scan_ms_text \
         FROM reconcile_full_scan_state WHERE credential_id = ?",
    )
    .bind(credential_id)
    .fetch_optional(store.pool())
    .await
    .context("load last full reconcile scan timestamp")?;
    row.map(|row| crate::store::try_get_i64_text(&row, "last_full_scan_ms_text"))
        .transpose()
        .map(|value| value.unwrap_or_default())
        .context("decode last full reconcile scan timestamp")
}

async fn persist_last_full_scan_ms(
    store: &Store,
    credential_id: &str,
    completed_at_ms: i64,
) -> anyhow::Result<()> {
    let sql = match store.kind() {
        crate::store::DbKind::MySql => {
            "INSERT INTO reconcile_full_scan_state \
             (credential_id, last_full_scan_ms, updated_at_ms) VALUES (?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             last_full_scan_ms=VALUES(last_full_scan_ms), updated_at_ms=VALUES(updated_at_ms)"
        }
        _ => {
            "INSERT INTO reconcile_full_scan_state \
             (credential_id, last_full_scan_ms, updated_at_ms) VALUES (?, ?, ?) \
             ON CONFLICT(credential_id) DO UPDATE SET \
             last_full_scan_ms=excluded.last_full_scan_ms, updated_at_ms=excluded.updated_at_ms"
        }
    };
    sqlx::query(sql)
        .bind(credential_id)
        .bind(completed_at_ms)
        .bind(completed_at_ms)
        .execute(store.pool())
        .await
        .context("persist last full reconcile scan timestamp")?;
    Ok(())
}

async fn user_stream_ready(store: &Store, credential_id: &str) -> anyhow::Result<bool> {
    Ok(
        crate::fill_replay::load_fill_stream_state(store, credential_id)
            .await
            .context("load user stream readiness for reconcile tier")?
            .is_some_and(|state| state.ready),
    )
}

async fn credential_has_l3_latch(store: &Store, credential_id: &str) -> anyhow::Result<bool> {
    let bot_rows = sqlx::query(
        "SELECT DISTINCT bot_uid FROM position_state WHERE credential_id = ? \
         UNION SELECT DISTINCT bot_uid FROM engine_orders WHERE credential_id = ?",
    )
    .bind(credential_id)
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .context("load credential bot scopes for reconcile tier")?;
    let bot_scopes = bot_rows
        .into_iter()
        .map(|row| format!("bot:{}", row.get::<String, _>("bot_uid")))
        .collect::<HashSet<_>>();
    let rows = sqlx::query("SELECT scope_key FROM kill_switch_state WHERE level = 'flatten'")
        .fetch_all(store.pool())
        .await
        .context("load L3 latch state for reconcile tier")?;
    let credential_scope = format!("cred:{credential_id}");
    Ok(rows.into_iter().any(|row| {
        let scope: String = row.get("scope_key");
        scope == "global" || scope == credential_scope || bot_scopes.contains(&scope)
    }))
}

async fn selected_periodic_tier(
    store: &Store,
    credential_id: &str,
    now_ms: i64,
) -> anyhow::Result<(ReconcileTier, i64, u64)> {
    let full_scan_interval_ms = periodic_reconcile_full_scan_interval_ms();
    let last_full_scan_ms = last_full_scan_ms(store, credential_id).await?;
    let l3_latched = credential_has_l3_latch(store, credential_id).await?;
    if l3_latched {
        return Ok((
            ReconcileTier::FullScan,
            last_full_scan_ms,
            full_scan_interval_ms,
        ));
    }
    let active_symbols =
        match crate::position_state::active_usdm_symbols(store, credential_id).await {
            Ok(symbols) => symbols,
            Err(error) => {
                tracing::warn!(
                    credential_id,
                    error = %error,
                    "active symbol truth is unknown; forcing a full reconcile scan"
                );
                return Ok((
                    ReconcileTier::FullScan,
                    last_full_scan_ms,
                    full_scan_interval_ms,
                ));
            }
        };
    let stream_ready = user_stream_ready(store, credential_id).await?;
    Ok((
        reconcile_tier_with_safety_fallbacks(
            credential_id,
            &active_symbols,
            stream_ready,
            false,
            now_ms,
            last_full_scan_ms,
            full_scan_interval_ms,
        ),
        last_full_scan_ms,
        full_scan_interval_ms,
    ))
}

fn next_full_scan_in_ms(
    credential_id: &str,
    now_ms: i64,
    last_full_scan_ms: i64,
    full_scan_interval_ms: u64,
) -> u64 {
    let due = full_scan_interval_ms
        .saturating_add(credential_stagger_ms(credential_id, full_scan_interval_ms));
    due.saturating_sub(now_ms.saturating_sub(last_full_scan_ms).max(0) as u64)
}

fn tier_log_registry() -> &'static std::sync::Mutex<HashMap<String, i64>> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<HashMap<String, i64>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn log_periodic_tier(
    credential_id: &str,
    tier: &ReconcileTier,
    now_ms: i64,
    next_full_scan_in_ms: u64,
) {
    let mut registry = tier_log_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let last = registry.get(credential_id).copied().unwrap_or(i64::MIN);
    if now_ms.saturating_sub(last) < 30_000 {
        return;
    }
    registry.insert(credential_id.to_string(), now_ms);
    let (tier_name, symbol_count, round_weight) = match tier {
        ReconcileTier::FullScan => (
            "full_scan",
            0usize,
            crate::binance_futures::periodic_reconcile_round_weight(),
        ),
        ReconcileTier::SymbolNarrowed { symbols } => (
            "symbol_narrowed",
            symbols.len(),
            crate::binance_futures::symbol_narrowed_reconcile_round_weight(symbols.len()),
        ),
        ReconcileTier::StreamOnly => ("stream_only", 0usize, 0.0),
    };
    tracing::info!(
        credential_id,
        tier = tier_name,
        symbols = symbol_count,
        round_weight,
        next_full_scan_in_ms,
        "periodic reconcile tier selected"
    );
}

/// 跑一轮周期对账。生产构建里只有 `Exchange` 这一条交易所分支。
async fn run_reconcile_round(
    source: &ReconcileExchangeSource,
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    keys: BinanceKeys,
) -> anyhow::Result<()> {
    run_reconcile_round_with_ws(source, credential_id, store, limiter, keys, None).await
}

async fn run_reconcile_round_with_ws(
    source: &ReconcileExchangeSource,
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    keys: BinanceKeys,
    ws_api: Option<&BinanceWsApiClient>,
) -> anyhow::Result<()> {
    let selected_at_ms = now_ms();
    crate::account_order_observation::not_observed(&store, credential_id, selected_at_ms);
    let (tier, previous_full_scan_ms, full_scan_interval_ms) =
        selected_periodic_tier(&store, credential_id, selected_at_ms).await?;
    let next_ms = next_full_scan_in_ms(
        credential_id,
        selected_at_ms,
        previous_full_scan_ms,
        full_scan_interval_ms,
    );
    log_periodic_tier(credential_id, &tier, selected_at_ms, next_ms);

    if matches!(tier, ReconcileTier::StreamOnly) {
        tracing::debug!(
            credential_id,
            "stream-only reconcile round sends zero exchange requests"
        );
        return Ok(());
    }

    let client = match source {
        ReconcileExchangeSource::Exchange => {
            let client = BinanceFuturesClient::new_with_store(keys, credential_id, store.clone())?;
            client.discover_reconcile_budget().await;
            client
        }
        #[cfg(test)]
        ReconcileExchangeSource::Stub(client) => client.clone(),
    };
    let coverage = match &tier {
        ReconcileTier::FullScan => Coverage::Full,
        ReconcileTier::SymbolNarrowed { symbols } => Coverage::Symbols(symbols.clone()),
        ReconcileTier::StreamOnly => unreachable!("stream-only returned before client assembly"),
    };
    let covered = reconcile_binance_futures_truth_with_client_and_coverage_with_ws(
        credential_id,
        store.clone(),
        limiter,
        &client,
        coverage,
        ws_api,
    )
    .await?;
    remember_latest_periodic_reconcile_truth(&store, credential_id, now_ms(), covered);
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InitialReconcileOwner {
    Reconciler,
    UserStream,
}

#[derive(Clone)]
pub struct ReconcilerManager {
    inner: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// 只在测试里为 false。周期对账会**联真实交易所**（`reconcile_binance_futures`
    /// 走 REST），而 §4 要求交易路径测试默认 dry-run；驱动 `ExecuteIntents`
    /// 生产入口的接线用例因此需要一个不起后台网络任务的构造。
    enabled: bool,
    /// 见 [`ReconcileExchangeSource`]。生产恒为 `Exchange`。
    exchange: ReconcileExchangeSource,
    #[cfg(test)]
    ws_api_override: Option<BinanceWsApiClient>,
}

impl ReconcilerManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            enabled: true,
            exchange: ReconcileExchangeSource::Exchange,
            #[cfg(test)]
            ws_api_override: None,
        }
    }

    /// 测试专用：不起任何周期对账任务（**不联网**）。
    #[cfg(test)]
    pub(crate) fn disabled_for_tests() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            enabled: false,
            exchange: ReconcileExchangeSource::Exchange,
            #[cfg(test)]
            ws_api_override: None,
        }
    }

    /// **测试出口（SEG3-R1-001）**：起**真实**的周期对账任务——同一条
    /// `ensure_running` → `tokio::spawn` → 句柄登记 → 立刻跑一次启动对账
    /// 的路径，只把交易所那一端换成无网络的响应队列桩。
    ///
    /// 与 `disabled_for_tests` 的区别正是这条评审发现要的东西：任务真的起来、
    /// 启动对账真的跑完、真值真的落库，因此「风控前是否调过 `ensure_running`」
    /// 变成一个可观察的事实。
    #[cfg(test)]
    pub(crate) fn with_stubbed_exchange_for_tests(client: BinanceFuturesClient) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            enabled: true,
            exchange: ReconcileExchangeSource::Stub(client),
            ws_api_override: None,
        }
    }

    /// 任务句柄的登记状态——`ensure_running` 幂等性所依赖的那张表本身。
    #[cfg(test)]
    pub(crate) async fn is_running_for_tests(&self, credential_id: &str) -> bool {
        self.inner.lock().await.contains_key(credential_id)
    }

    /// 占位背后的句柄**是否真的已经结束**（正常返回 / 被取消 / panic 都算）。
    /// 注册表里有这个键不等于还有人在管这个凭据——A2c-R1 要区分的正是这两件事。
    #[cfg(test)]
    pub(crate) async fn task_finished_for_tests(&self, credential_id: &str) -> Option<bool> {
        self.inner
            .lock()
            .await
            .get(credential_id)
            .map(|handle| handle.is_finished())
    }

    /// 测试专用：收尾时停掉所有登记的任务，并**等它们真的退出**再返回——
    /// 否则调用方随后恢复环境变量、删库文件时，半死的任务还在跑，会把活动
    /// 残留给下一条用例。
    #[cfg(test)]
    pub(crate) async fn abort_all_for_tests(&self) {
        for (_, task) in self.inner.lock().await.drain() {
            task.abort();
            let _ = task.await;
        }
    }

    /// 测试专用：就地终止某个凭据的对账任务并**保留注册表占位**，等它真的
    /// 结束再返回——复现生产里任务因取消/panic 退出、而占位还在的那一刻。
    /// 它只制造前提条件，不替被测的 `ensure_running` 做任何判断。
    #[cfg(test)]
    pub(crate) async fn end_task_in_place_for_tests(&self, credential_id: &str) {
        let mut guard = self.inner.lock().await;
        let handle = guard
            .get_mut(credential_id)
            .expect("a registered reconcile task");
        handle.abort();
        let _ = (&mut *handle).await;
        assert!(
            handle.is_finished(),
            "the aborted reconcile task must have really finished"
        );
    }

    pub async fn ensure_running(
        &self,
        credential_id: &str,
        store: Store,
        limiter: CredentialRateLimiter,
        keys: BinanceKeys,
    ) {
        self.ensure_running_with_initial_owner(
            credential_id,
            store,
            limiter,
            keys,
            InitialReconcileOwner::Reconciler,
        )
        .await;
    }

    /// Startup fill recovery already owns backfill plus the initial full scan.
    /// Register the main periodic task now, but let its first normal tick run
    /// the next scan instead of competing for the same startup request budget.
    pub(crate) async fn ensure_running_after_stream_startup(
        &self,
        credential_id: &str,
        store: Store,
        limiter: CredentialRateLimiter,
        keys: BinanceKeys,
    ) {
        self.ensure_running_with_initial_owner(
            credential_id,
            store,
            limiter,
            keys,
            InitialReconcileOwner::UserStream,
        )
        .await;
    }

    async fn ensure_running_with_initial_owner(
        &self,
        credential_id: &str,
        store: Store,
        limiter: CredentialRateLimiter,
        keys: BinanceKeys,
        initial_owner: InitialReconcileOwner,
    ) {
        if !self.enabled {
            return;
        }
        let mut guard = self.inner.lock().await;
        // 占位不等于有人在管：任务正常返回、被取消或 panic 退出后，句柄仍然
        // 留在表里。判据只取 `JoinHandle` 自己的完成状态——不按时长、不按
        // 失联推定，因此**还在退出中**的任务不会被抢先顶替；确已结束的才让位
        // 给新任务。恢复靠的是下一次真实 ensure_running 调用，没有 watchdog。
        if guard
            .get(credential_id)
            .is_some_and(|handle| !handle.is_finished())
        {
            return;
        }
        let credential_id = credential_id.to_string();
        let task_credential_id = credential_id.clone();
        let exchange = self.exchange.clone();
        #[cfg(test)]
        let ws_api_override = self.ws_api_override.clone();

        let handle = tokio::spawn(async move {
            let interval_ms = periodic_reconcile_interval_ms();

            // Disabled is the literal baseline: no ws client, no task and no
            // handshake. A construction failure is also a REST-safe fallback.
            let ws_api = if crate::binance_ws_api::ws_api_enabled()
                && matches!(exchange, ReconcileExchangeSource::Exchange)
            {
                match BinanceWsApiClient::spawn(keys.clone(), &task_credential_id, store.clone()) {
                    Ok(client) => Some(client),
                    Err(error) => {
                        tracing::warn!(
                            credential_id = %task_credential_id,
                            %error,
                            "ws-fapi client unavailable; periodic account reads remain on REST"
                        );
                        None
                    }
                }
            } else {
                None
            };

            #[cfg(test)]
            let ws_api = ws_api_override.or(ws_api);

            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Tokio intervals tick immediately once. Consume that synthetic
            // first tick before the explicit startup round, otherwise startup
            // performs two full truth scans back-to-back.
            ticker.tick().await;

            if initial_owner == InitialReconcileOwner::Reconciler {
                // Startup reconcile.
                let startup = match ws_api.as_ref() {
                    Some(ws) => {
                        run_reconcile_round_with_ws(
                            &exchange,
                            &task_credential_id,
                            store.clone(),
                            limiter.clone(),
                            keys.clone(),
                            Some(ws),
                        )
                        .await
                    }
                    None => {
                        run_reconcile_round(
                            &exchange,
                            &task_credential_id,
                            store.clone(),
                            limiter.clone(),
                            keys.clone(),
                        )
                        .await
                    }
                };
                if let Err(e) = startup {
                    tracing::warn!(credential_id=%task_credential_id, error=%e, "startup reconcile failed");
                }
            }

            loop {
                ticker.tick().await;
                let periodic = match ws_api.as_ref() {
                    Some(ws) => {
                        run_reconcile_round_with_ws(
                            &exchange,
                            &task_credential_id,
                            store.clone(),
                            limiter.clone(),
                            keys.clone(),
                            Some(ws),
                        )
                        .await
                    }
                    None => {
                        run_reconcile_round(
                            &exchange,
                            &task_credential_id,
                            store.clone(),
                            limiter.clone(),
                            keys.clone(),
                        )
                        .await
                    }
                };
                if let Err(e) = periodic {
                    tracing::warn!(credential_id=%task_credential_id, error=%e, "periodic reconcile failed");
                }
            }
        });

        guard.insert(credential_id, handle);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BinanceOpenOrder {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    symbol: String,
    status: String,
    #[serde(rename = "updateTime")]
    update_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlgoTruthLookup {
    OpenSet,
    SingleOrderQuery,
    Unidentified,
}

fn algo_truth_lookup(
    present_in_open_set: bool,
    has_exchange_algo_id: bool,
    has_client_algo_id: bool,
) -> AlgoTruthLookup {
    match (
        present_in_open_set,
        has_exchange_algo_id || has_client_algo_id,
    ) {
        (true, _) => AlgoTruthLookup::OpenSet,
        (false, true) => AlgoTruthLookup::SingleOrderQuery,
        (false, false) => AlgoTruthLookup::Unidentified,
    }
}

fn truth_source_for_algo_lookup(
    lookup: AlgoTruthLookup,
) -> Option<crate::protection::AlgoTruthSource> {
    match lookup {
        AlgoTruthLookup::OpenSet => Some(crate::protection::AlgoTruthSource::RestOpenAlgoOrders),
        AlgoTruthLookup::SingleOrderQuery => {
            Some(crate::protection::AlgoTruthSource::RestAlgoQuery)
        }
        AlgoTruthLookup::Unidentified => None,
    }
}

/// 交易所侧从未确认过的订单，在被裁决为终态死亡前的宽限期。
///
/// `pending_risk` / `pending_submit` 可能正被一个**存活的**请求提交着
///（commit 之后、交易所回执之前）；`unknown` 可能是提交已到所而回执丢了。
/// 立刻裁决会与在途提交竞争：刚释放 closing、旧单随后成交，就成了误释放。
/// 因此先给一个宽限期，超期未被任何路径推进才允许向交易所求证并裁决。
pub(crate) const UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS: i64 = 60_000;

/// 这条订单是否**可以**被裁决为终态死亡（真正判死还要交易所答复 -2013）。
///
/// R5 FBL-R4-H3。三个条件缺一不可：
/// - 无 `exchange_order_id`：交易所从未确认过它。已有交易所单号的订单，其
///   -2013 含义不同（例如老订单被交易所清理），不在本条 scope 内。
/// - 状态属于崩溃残留态 `unknown` / `pending_risk` / `pending_submit`。
///   `submitted` / `acked` / `partial_filled` 是交易所已确认的在途单，不裁决。
/// - 距上次状态推进已过宽限期，避开与在途提交的竞争。
pub(crate) fn unconfirmed_order_is_adjudicable(
    status: &str,
    exchange_order_id: &str,
    updated_at_ms: i64,
    now_ms: i64,
) -> bool {
    if !exchange_order_id.trim().is_empty() {
        return false;
    }
    if !matches!(status, "unknown" | "pending_risk" | "pending_submit") {
        return false;
    }
    now_ms.saturating_sub(updated_at_ms) >= UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS
}

/// 把一条已被权威判死的订单转入终态 `failed` 并留下审计事件。
///
/// `failed` 在 R4 之前是死码（`exit_order_status_is_dead` 收它、但全仓无写入
/// 方）。R5 起它有且只有这一个写入方：交易所答复「查无此单」，或订单连交易所
/// 身份都没有。写入后 dead 集吸收 → `sweep_closing_positions` 释放 closing。
///
/// **CAS miss 与 DB error 不是一回事。**
///
/// R11 这里是 `.map(|r| r.rows_affected() > 0).unwrap_or(false)`：一次真正的
/// DB 故障被折成"这行在我们读它之后被推进过"，于是打出一条**事实错误**的
/// warn（`order advanced under the grace-window read`），本轮继续跑到底，
/// 最后还把 `ReconcileTruth` 当成一次成功的对账交给 kill-switch 的
/// `record_reconcile_for_scope` 与 `reconcile_diff` 降级解除——一次写失败
/// 被转换成了"一切正常"（engine safety contract）。
///
/// 现在：`rows_affected() == 0` 是 CAS miss（`Ok(false)`，本轮继续），
/// `Err` 是 DB error（原样上抛，**整轮失败**，ready/clean 投影一个都不更新）。
async fn adjudicate_order_dead(
    store: &Store,
    credential_id: &str,
    engine_order_id: &str,
    ctx: &EventContext,
    previous_status: &str,
    ts: i64,
    reason: &str,
) -> anyhow::Result<bool> {
    // **比较并置**，不是无条件写。
    //
    // 裁决依据（状态 + 无 exchange_order_id）是在本轮循环**开头**读到的；从那一刻
    // 到这里之间，一个滞留在 limiter 里的 exit 提交完全可能刚刚成功并把这行置成
    // `acked` + 交易所单号。无守卫的 UPDATE 会把一张**活单**改写成 `failed`，
    // 随后清扫还会释放 closing —— 仓位被放开而真实平仓单还挂在所上。
    //
    // 守卫把裁决变成 CAS：行在我们读它之后被推进过，就一行也不改、不裁决。
    let updated = sqlx::query(
        r#"
        UPDATE engine_orders
        SET status = 'failed', updated_at_ms = ?
        WHERE credential_id = ? AND engine_order_id = ?
          AND status = ?
          AND COALESCE(exchange_order_id, '') = ''
        "#,
    )
    .bind(ts)
    .bind(credential_id)
    .bind(engine_order_id)
    .bind(previous_status)
    .execute(store.pool())
    .await
    .context("reconcile could not adjudicate an unconfirmed order as dead")?
    .rows_affected()
        > 0;

    if !updated {
        tracing::warn!(
            credential_id = %credential_id,
            engine_order_id = %engine_order_id,
            previous_status = %previous_status,
            reason = %reason,
            "reconcile skipped adjudication: order advanced under the grace-window read"
        );
        return Ok(false);
    }

    tracing::warn!(
        credential_id = %credential_id,
        engine_order_id = %engine_order_id,
        previous_status = %previous_status,
        reason = %reason,
        "reconcile adjudicated unconfirmed order as dead per exchange authority"
    );

    emit_order_update(
        store,
        credential_id,
        engine_order_id,
        ctx,
        String::new(),
        ts,
        "reconcile_order_adjudicated_dead",
        serde_json::json!({
            "previousStatus": previous_status,
            "nextStatus": "failed",
            "reason": reason,
        }),
    )
    .await?;
    Ok(true)
}

/// **对账的状态写回**：与 WS 侧同一条 CAS/偏序纪律（第七轮
/// regression，High）。
///
/// R10 的两处写回是裸 `let _ = UPDATE ... WHERE credential AND engine_order_id`：
/// * **没有任何旧状态谓词**——一条交易所的迟到 `NEW` 能把本地 `partial_filled`
///   退回 `acked`，而 WS 侧同一件事已经用完整偏序挡住了。两条路径消费的是
///   同一份交易所真值，判据却不同源。
/// * **吞错**——`let _` 让 UPDATE 失败与成功不可区分，而调用方紧接着照常
///   `changed += 1` 并写下 `reconcile_open` / `reconcile_get_order` 审计事件：
///   库里留下"对账把它改成 X 了"的事实，状态却没动。
///
/// 现在：谓词写进 SQL 的 `status IN (...)`（不是先读后写的 TOCTOU），
/// 允许集合由 `statuses_weaker_than` 从闭世界枚举生成（认不出的当前状态
/// fail-closed 地不被覆盖）；错误原样上抛。
///
/// 返回**这一行是否真的被改写了**——调用方只在为真时计入 `changed`
/// 并追加审计事件。
async fn apply_reconciled_status(
    store: &Store,
    credential_id: &str,
    engine_order_id: &str,
    current_status: &str,
    next: &str,
    exchange_order_id: &str,
    ts: i64,
) -> anyhow::Result<bool> {
    let weaker = crate::svc::statuses_weaker_than(next);
    if !weaker.contains(&current_status) {
        tracing::warn!(
            credential_id,
            engine_order_id,
            current_status,
            rejected_status = next,
            "reconcile truth would regress the order status; keeping the stronger truth"
        );
        return Ok(false);
    }
    let placeholders = std::iter::repeat_n("?", weaker.len())
        .collect::<Vec<_>>()
        .join(",");
    // Persisted status bytes grant write authority. MySQL collation must not
    // equate malformed bytes with a canonical state after the earlier read.
    let status_lhs = crate::svc::exact_engine_order_status_lhs(store.kind())?;
    let sql = format!(
        r#"
        UPDATE engine_orders
        SET status = ?, exchange_order_id = ?, updated_at_ms = ?
        WHERE credential_id = ? AND engine_order_id = ?
          AND {status_lhs} IN ({placeholders})
        "#
    );
    let mut update = sqlx::query(&sql)
        .bind(next)
        .bind(exchange_order_id)
        .bind(ts)
        .bind(credential_id)
        .bind(engine_order_id);
    for status in &weaker {
        update = update.bind(*status);
    }
    let applied = update
        .execute(store.pool())
        .await
        .context("reconcile could not write back the order status")?;
    if applied.rows_affected() == 0 {
        tracing::warn!(
            credential_id,
            engine_order_id,
            current_status,
            next_status = next,
            "another path advanced this order between the reconcile read and the write; \
             keeping the status it now carries"
        );
        return Ok(false);
    }
    Ok(true)
}

/// **恢复流程的 readiness 判据：这次对账有没有留下任何未收敛的交易所真值。**
///
/// 把它从 `binance_user_stream` 的一条内联布尔里抽成
/// 生产函数，好让"重连之后凭什么敢 declare ready"可被用例直接观察。
///
/// **历史判据曾按需求收紧为完整五段。**
///
/// R14 的判据只消费 `has_diff` 与 `local_position_zero_diff` 两段，并明确
/// 写下"**不**判 `all_positions_zero` / `all_open_orders_empty`"，理由是
/// 正常持仓运行时那两段恒为假、会让重连之后永远无法 ready。统筹按台账 /
/// `docs/SAFETY_MODEL.md` 当时裁决：**Ok 但 non-flat → 不 ready**，判据因此
/// 曾是 `ReconcileTruth::flatten_complete()` 的补集——五段真值一段都不丢：
///
/// * `has_diff` —— 本地在途单/持仓与交易所真值对不上；
/// * `all_positions_zero` —— 交易所侧还有未平的持仓；
/// * `all_open_orders_empty` / `all_open_algo_orders_empty` —— 交易所侧还有
///   未终结的普通单 / 条件单；
/// * `local_position_zero_diff` —— 交易所说全平而本地还留着活仓。
///
/// **2026-09-05 设计要求 裁决 A + B 推翻 R15 的 readiness 部分**：readiness 的
/// 目的只是在本地与交易所真值收敛前关闭新增风险；该收敛由 `has_diff=false`
/// 与 `local_position_zero_diff=true` 完整表达。持仓及其 SL/TP 挂单是正常、
/// 安全的 non-flat 状态，若继续要求三段 emptiness，会让每次重连在持仓期间
/// 永久 not-ready，并以约 2 秒节拍重复回填和 FullScan（VM 实测约 1400
/// request weight/分钟）。因此 `all_positions_zero`、`all_open_orders_empty`
/// 与 `all_open_algo_orders_empty` 从本函数移除；它们仍逐字保留在
/// `ReconcileTruth::flatten_complete()`，只服务 L3 完成判据。
///
/// 这条判据只关掉 `risk.rs` 的**新开仓**闸门：`FILL_RECOVERY_NOT_READY` 对
/// close / reduce-only / SL·TP 保护 / L3 flatten 一律放行
/// （`risk::fill_recovery_guard_disposition`，`SAFETY_MODEL.md` §「用户流启动、
/// 重连或回补失败」同款表述）。既有仓位仍可被管理与平掉，被挡住的只有
/// "在交易所侧还有未收敛真值时再开一个新仓"。
///
/// 返回 `Some(reason)` 即"这次恢复不得 declare ready"，reason 直接落
/// `fill_stream_state.failure_reason`。
pub(crate) fn recovery_reconcile_divergence(
    truth: crate::kill_switch::ReconcileTruth,
) -> Option<String> {
    (truth.has_diff || !truth.local_position_zero_diff).then(|| {
        format!(
            "post-backfill reconcile did not leave local and exchange truth converged: \
             has_diff={} local_position_zero_diff={}",
            truth.has_diff, truth.local_position_zero_diff,
        )
    })
}

pub async fn reconcile_binance_futures(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    keys: BinanceKeys,
) -> anyhow::Result<()> {
    reconcile_binance_futures_truth(credential_id, store, limiter, keys)
        .await
        .map(|_| ())
}

pub async fn reconcile_binance_futures_truth(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    keys: BinanceKeys,
) -> anyhow::Result<crate::kill_switch::ReconcileTruth> {
    let client = BinanceFuturesClient::new_with_store(keys, credential_id, store.clone())?;
    client.discover_reconcile_budget().await;
    reconcile_binance_futures_truth_with_client(credential_id, store, limiter, &client).await
}

#[derive(Default)]
struct TerminalOrderConflicts {
    ordinary: Vec<serde_json::Value>,
    algos: Vec<serde_json::Value>,
    protection_ids: HashSet<String>,
}

// Identity recognition does not prove that a remotely active order is still
// managed locally. Preserve explicit terminal/active contradictions before the
// ordinary and Algo projectors run. A closed position alone is not a conflict:
// confirmed protection can legitimately be awaiting owned post-close cleanup.
async fn terminal_order_conflicts(
    store: &Store,
    credential_id: &str,
    coverage: &Coverage,
    open_by_order_id: &HashMap<(String, String), &BinanceOpenOrder>,
    open_by_client_id: &HashMap<(String, String), &BinanceOpenOrder>,
    open_algo_by_id: &HashMap<String, &BinanceAlgoOrderResponse>,
    open_algo_by_client: &HashMap<String, &BinanceAlgoOrderResponse>,
) -> anyhow::Result<TerminalOrderConflicts> {
    let mut conflicts = TerminalOrderConflicts::default();
    if open_by_order_id.is_empty() && open_algo_by_id.is_empty() && open_algo_by_client.is_empty() {
        return Ok(conflicts);
    }
    let rows = sqlx::query(
        r#"SELECT engine_order_id, symbol_key, status, order_role,
                  exchange_order_id, client_order_id_short
           FROM engine_orders WHERE credential_id=?
             AND status IN ('filled','canceled','expired','rejected','failed')"#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .context("load terminal ordinary identities for reconcile")?;
    for row in rows {
        let symbol_key: String = row.try_get("symbol_key")?;
        if !coverage.includes_symbol_key(&symbol_key) {
            continue;
        }
        let Some(symbol) = crate::binance_futures::usdm_symbol(&symbol_key) else {
            continue;
        };
        let exchange_id = crate::store::try_get_nullable_string(&row, "exchange_order_id")?;
        let client_id = crate::store::try_get_nullable_string(&row, "client_order_id_short")?;
        let role = crate::store::try_get_nullable_string(&row, "order_role")?;
        // Once acknowledged, exchange identity wins: a recycled client ID must
        // not bind a later remote order to an older terminal order.
        let remote = match exchange_id.as_deref().filter(|id| !id.trim().is_empty()) {
            Some(id) => open_by_order_id.get(&(symbol, id.trim().to_string())),
            None if !role.as_deref().unwrap_or("").starts_with("protection_") => client_id
                .as_deref()
                .and_then(|id| open_by_client_id.get(&(symbol, id.trim().to_string()))),
            None => None,
        };
        if let Some(remote) = remote {
            conflicts.ordinary.push(serde_json::json!({
                "engineOrderId": row.try_get::<String, _>("engine_order_id")?,
                "localStatus": row.try_get::<String, _>("status")?,
                "orderId": remote.order_id, "clientOrderId": remote.client_order_id,
                "symbol": remote.symbol, "remoteStatus": remote.status,
                "reason": "terminal_local_order_remote_active",
            }));
        }
    }
    let rows = sqlx::query(
        r#"SELECT po.protection_order_id, po.status, po.exchange_order_id,
                  po.exchange_algo_id, po.client_algo_id,
                  eo.status AS engine_status,
                  COALESCE(ps.symbol_key, eo.symbol_key) AS symbol_key
           FROM protection_orders po
           LEFT JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
             AND eo.credential_id=po.credential_id AND eo.position_id=po.position_id
           LEFT JOIN position_state ps ON ps.position_id=po.position_id
             AND ps.credential_id=po.credential_id
           WHERE po.credential_id=? AND (
             po.status IN ('canceled','expired','rejected','failed','missing')
             OR eo.status IN ('filled','canceled','expired','rejected','failed'))"#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .context("load terminal protection identities for reconcile")?;
    for row in rows {
        let symbol_key = crate::store::try_get_nullable_string(&row, "symbol_key")?;
        if !coverage.is_full()
            && !symbol_key
                .as_deref()
                .is_some_and(|key| coverage.includes_symbol_key(key))
        {
            continue;
        }
        let protection_id: String = row.try_get("protection_order_id")?;
        let exchange_id = crate::store::try_get_nullable_string(&row, "exchange_algo_id")?;
        let client_id = crate::store::try_get_nullable_string(&row, "client_algo_id")?;
        let remote = match exchange_id.as_deref().filter(|id| !id.trim().is_empty()) {
            Some(id) => open_algo_by_id.get(id.trim()),
            None => client_id
                .as_deref()
                .and_then(|id| open_algo_by_client.get(id.trim())),
        };
        if let Some(remote) = remote {
            conflicts.protection_ids.insert(protection_id.clone());
            conflicts.algos.push(serde_json::json!({
                "protectionOrderId": protection_id,
                "localStatus": row.try_get::<String, _>("status")?,
                "engineStatus": crate::store::try_get_nullable_string(&row, "engine_status")?,
                "symbolKey": symbol_key,
                "algoId": remote.algo_id,
                "clientAlgoId": remote.client_algo_id.as_ref().map(BinanceClientAlgoId::as_str),
                "remoteStatus": remote.algo_status,
                "reason": "terminal_local_protection_remote_active",
            }));
        }
        // A legacy protection or a triggered child retains its ordinary ID;
        // never reinterpret it as an Algo parent ID.
        if let (Some(symbol), Some(id)) = (
            symbol_key
                .as_deref()
                .and_then(crate::binance_futures::usdm_symbol),
            crate::store::try_get_nullable_string(&row, "exchange_order_id")?,
        ) {
            if let Some(remote) = open_by_order_id.get(&(symbol, id.trim().to_string())) {
                conflicts.protection_ids.insert(protection_id.clone());
                conflicts.ordinary.push(serde_json::json!({
                    "protectionOrderId": protection_id,
                    "localStatus": row.try_get::<String, _>("status")?,
                    "orderId": remote.order_id, "clientOrderId": remote.client_order_id,
                    "symbol": remote.symbol, "remoteStatus": remote.status,
                    "reason": "terminal_local_protection_remote_active",
                }));
            }
        }
    }
    Ok(conflicts)
}

/// Rows per protection reconciliation page (K04). The traversal itself is
/// complete: every page is read until one comes back short.
const PROTECTION_RECONCILE_PAGE_SIZE: i64 = 1000;

/// One keyset page of this credential's non-terminal protection rows, newest
/// first. `after` is the `(created_at_ms, protection_order_id)` of the last
/// row of the previous page; both columns are immutable once written.
async fn load_protection_reconcile_page(
    store: &Store,
    credential_id: &str,
    after: Option<&(i64, String)>,
) -> anyhow::Result<Vec<sqlx::any::AnyRow>> {
    let (has_cursor, cursor_ms, cursor_id) = match after {
        Some((created_at_ms, protection_order_id)) => {
            (1i64, *created_at_ms, protection_order_id.as_str())
        }
        None => (0i64, 0i64, ""),
    };
    Ok(sqlx::query(
        r#"
        SELECT po.protection_order_id, po.exchange_order_id, po.exchange_algo_id,
               po.client_algo_id, po.algo_status, ps.symbol_key,
               CAST(po.created_at_ms AS CHAR) AS created_at_ms_text,
               CASE WHEN ps.state <> 'closed' THEN 1 ELSE 0 END AS position_open
        FROM protection_orders po
        JOIN position_state ps ON ps.position_id=po.position_id
        WHERE po.credential_id=?
          AND po.status NOT IN ('canceled','expired','failed')
          AND (? = 0 OR po.created_at_ms < ?
               OR (po.created_at_ms = ? AND po.protection_order_id < ?))
        ORDER BY po.created_at_ms DESC, po.protection_order_id DESC
        LIMIT ?
        "#,
    )
    .bind(credential_id)
    .bind(has_cursor)
    .bind(cursor_ms)
    .bind(cursor_ms)
    .bind(cursor_id)
    .bind(PROTECTION_RECONCILE_PAGE_SIZE)
    .fetch_all(store.pool())
    .await?)
}

/// 生效的周期对账间隔。
///
/// 抽成函数是为了让启动时的「预算撑不撑得起这个节奏」自检与实际 ticker
/// **读同一个值**——各读各的 env 是这类自检失真的经典来源。
pub(crate) fn periodic_reconcile_interval_ms() -> u64 {
    std::env::var("RECONCILE_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30_000)
        .clamp(1_000, 600_000)
}

pub(crate) async fn reconcile_binance_futures_truth_with_client(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
) -> anyhow::Result<crate::kill_switch::ReconcileTruth> {
    reconcile_binance_futures_truth_with_client_and_coverage(
        credential_id,
        store,
        limiter,
        client,
        Coverage::Full,
    )
    .await
    .map(|covered| covered.truth)
}

async fn reconcile_binance_futures_truth_with_client_and_coverage(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
    coverage: Coverage,
) -> anyhow::Result<CoveredReconcileTruth> {
    reconcile_binance_futures_truth_with_client_and_coverage_with_ws(
        credential_id,
        store,
        limiter,
        client,
        coverage,
        None,
    )
    .await
}

async fn reconcile_binance_futures_truth_with_client_and_coverage_with_ws(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
    coverage: Coverage,
    ws_api: Option<&BinanceWsApiClient>,
) -> anyhow::Result<CoveredReconcileTruth> {
    reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
        credential_id,
        store,
        limiter,
        client,
        coverage,
        ws_api,
        now_ms,
    )
    .await
}

/// WO-M1 测试缝：与 [`reconcile_binance_futures_truth`] 同一套客户端 / 限流
/// 构造与同一条全量对账，只把本轮观察时刻换成注入值——跨过未确认订单的
/// 裁决宽限期不必真的等 60 秒（生产路径仍只用墙钟）。
#[cfg(test)]
pub(crate) async fn reconcile_full_at_for_tests(
    credential_id: &str,
    store: Store,
    observed_at_ms: i64,
) -> anyhow::Result<crate::kill_switch::ReconcileTruth> {
    let keys = crate::binance_futures::keys_from_env(credential_id)
        .map_err(anyhow::Error::msg)?
        .context("reconcile test seam requires exchange keys")?;
    let client = BinanceFuturesClient::new_with_store(keys, credential_id, store.clone())?;
    client.discover_reconcile_budget().await;
    let limiter = CredentialRateLimiter::shared(crate::rate_limiter::TokenBucketConfig::from_env(
        crate::venue::venue_for_credential(credential_id).map_err(anyhow::Error::msg)?,
    ));
    reconcile_binance_futures_truth_with_client_and_coverage_at(
        credential_id,
        store,
        limiter,
        &client,
        Coverage::Full,
        || observed_at_ms,
    )
    .await
    .map(|covered| covered.truth)
}

#[cfg(test)]
async fn reconcile_binance_futures_truth_with_client_and_coverage_at<F>(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
    coverage: Coverage,
    observed_at_ms: F,
) -> anyhow::Result<CoveredReconcileTruth>
where
    F: FnOnce() -> i64,
{
    reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
        credential_id,
        store,
        limiter,
        client,
        coverage,
        None,
        observed_at_ms,
    )
    .await
}

async fn reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws<F>(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
    coverage: Coverage,
    ws_api: Option<&BinanceWsApiClient>,
    observed_at_ms: F,
) -> anyhow::Result<CoveredReconcileTruth>
where
    F: FnOnce() -> i64,
{
    let mut timing = crate::reconcile_trace::Timer::fullscan();
    let result = reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws_timed(
        credential_id,
        store,
        limiter,
        client,
        coverage,
        ws_api,
        observed_at_ms,
        &mut timing,
    )
    .await;
    timing.finish(result.is_ok());
    result
}

#[allow(
    clippy::collapsible_if,
    reason = "Preserve the established reconciliation control flow."
)]
async fn reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws_timed<F>(
    credential_id: &str,
    store: Store,
    limiter: CredentialRateLimiter,
    client: &BinanceFuturesClient,
    coverage: Coverage,
    ws_api: Option<&BinanceWsApiClient>,
    observed_at_ms: F,
    timing: &mut crate::reconcile_trace::Timer,
) -> anyhow::Result<CoveredReconcileTruth>
where
    F: FnOnce() -> i64,
{
    let mut order_observation = crate::account_order_observation::Observation::begin(
        &store,
        credential_id,
        if coverage.is_full() {
            "full_account"
        } else {
            "symbols"
        },
        now_ms(),
    );
    // **观察点必须早于交易所读取。**
    //
    // 下面这一串是好几个网络往返：等它们回来的时候，本地完全可能已经因为
    // 一个**新的裸敞口**开了新一轮 flatten latch。那时再把这份真值投影
    // 进去，就是用"没看过这个敞口的观察"去宣布它收敛了。代际快照在这里取，
    // 与 `record_reconcile_for_scope` 的 CAS 配对。
    let observed_generations = crate::kill_switch::observe_flatten_generations(&store)
        .await
        .context("observe kill-switch flatten generations before reading exchange truth")?;

    // 1) Exchange truth. Full coverage keeps the historical four-call path.
    // Symbol coverage concatenates scoped order responses but reads account-wide
    // positionRisk once. Order truth outside the declared coverage remains unknown,
    // so full position truth does not upgrade the round to Coverage::Full.
    let (open_orders, open_algo_orders) = match &coverage {
        Coverage::Full => {
            let ordinary_started = now_ms();
            let open_orders_json = client
                .open_orders(None, RequestClass::PeriodicReconcile)
                .await?;
            order_observation.ordinary(&open_orders_json, ordinary_started, now_ms());
            let open_orders = serde_json::from_value(open_orders_json)
                .context("decode Binance openOrders truth")?;
            let algo_started = now_ms();
            let open_algo_orders_json = client
                .get_all_open_algo_orders_json(&limiter, RequestClass::PeriodicReconcile)
                .await
                .context("query Binance openAlgoOrders truth")?;
            order_observation.algo(&open_algo_orders_json, algo_started, now_ms());
            let open_algo_orders = serde_json::from_value(open_algo_orders_json)
                .context("decode Binance openAlgoOrders truth")?;
            (open_orders, open_algo_orders)
        }
        Coverage::Symbols(symbols) => {
            let mut open_orders: Vec<BinanceOpenOrder> = Vec::new();
            let mut open_algo_orders: Vec<BinanceAlgoOrderResponse> = Vec::new();
            for symbol in symbols {
                let ordinary_started = now_ms();
                let orders = client
                    .open_orders(Some(symbol), RequestClass::PeriodicReconcile)
                    .await?;
                order_observation.ordinary(&orders, ordinary_started, now_ms());
                open_orders.extend(
                    serde_json::from_value::<Vec<BinanceOpenOrder>>(orders)
                        .context("decode symbol-narrowed Binance openOrders truth")?,
                );
                let algo_started = now_ms();
                let algos = client
                    .get_open_algo_orders_json(&limiter, symbol, RequestClass::PeriodicReconcile)
                    .await
                    .context("query symbol-narrowed Binance openAlgoOrders truth")?;
                order_observation.algo(&algos, algo_started, now_ms());
                open_algo_orders.extend(
                    serde_json::from_value::<Vec<BinanceAlgoOrderResponse>>(algos)
                        .context("decode symbol-narrowed Binance openAlgoOrders truth")?,
                );
            }
            (open_orders, open_algo_orders)
        }
    };
    let account_source = match ws_api {
        Some(ws) if ws.is_healthy().await => AccountReadSource::WsApi(ws),
        Some(_) | None => AccountReadSource::Rest,
    };
    // WO-M1：仓位读数的起点。早于平仓单落库的读数不能证明「平仓之后」归零。
    let position_read_started_ms = now_ms();
    let account = read_account_truth(account_source, client, &coverage).await?;
    let positions_json = account.positions;
    if !positions_json.is_array() {
        anyhow::bail!("Binance positionRisk truth is not an array");
    }
    let balances_json = account.balances;

    if let Some(ws) = ws_api {
        let health = ws.health().await;
        tracing::info!(
            credential_id,
            source = account.source,
            ws_scope = ws.scope(),
            used_weight_1m = ?health.used_weight_1m,
            limit = ?health.limit_1m,
            connection_age_ms = ?health.connection_age_ms(now_ms()),
            reconnect_count = health.reconnect_count,
            fallback_reason = ?account.fallback_reason,
            "periodic reconcile account read source"
        );
    }

    timing.stage(crate::reconcile_trace::Stage::LocalLoad);
    let ts = observed_at_ms();
    let position_outcome = persist_positions_and_balances_with_coverage(
        &store,
        credential_id,
        ts,
        &positions_json,
        &balances_json,
        &account.position_coverage,
    )
    .await?;

    // 2) Local inflight orders.
    let inflight_rows = sqlx::query(
        r#"
        SELECT engine_order_id, exchange_order_id, client_order_id_short,
               bot_uid, run_id, cycle_id, idempotency_key,
               exchange, market_kind, symbol_key, status,
               CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
        FROM engine_orders
        WHERE credential_id = ?
          AND status IN ('pending_risk','pending_submit','submitted','acked','partial_filled','unknown')
          AND (order_role NOT LIKE 'protection_%' OR exchange_order_id IS NOT NULL)
        ORDER BY created_at_ms DESC
        LIMIT 500
        "#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    // **查询失败 ≠ 无在途单。**
    //
    // R10 这里是 `.unwrap_or_default()`：一次 DB 故障与"这个 credential
    // 没有任何在途单"产出**同一个空集合**，于是对账照跑到底、`checked=0`、
    // 零差异，最后还会把 `reconcile_diff` 的降级解除——一次查询失败被
    // 转换成了"一切正常"这条交易真值（engine safety contract 明令禁止，
    // 与 advance_then_submit 的 `.unwrap_or(0)` 是同型缺陷）。
    // 现在原样上抛：调用方（user stream 恢复链、周期对账）按失败处置。
    .context("load local inflight orders for reconcile")?;
    timing.local_orders(inflight_rows.len());

    // **open-order 索引必须带 symbol**（regression）。Binance 的 orderId
    // 只在 symbol 内唯一、clientOrderId 更是调用方自选：一张**另一个 symbol**
    // 的开放单会在这两张 map 里与本地单撞上，于是"交易所说它还开着"这条
    // 结论落到了错的单上。WS 侧用的是完整交易身份，对账侧此前只有裸 id——
    // 两个消费同一份交易所真值的路径用两个不同的身份判据。
    let mut open_by_order_id: HashMap<(String, String), &BinanceOpenOrder> = HashMap::new();
    let mut open_by_client_id: HashMap<(String, String), &BinanceOpenOrder> = HashMap::new();
    for o in &open_orders {
        let symbol = o.symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            tracing::error!(
                credential_id,
                exchange_order_id = o.order_id,
                "exchange open order carries no symbol; it cannot be bound to a local order"
            );
            continue;
        }
        open_by_order_id.insert((symbol.clone(), o.order_id.to_string()), o);
        if !o.client_order_id.trim().is_empty() {
            open_by_client_id.insert((symbol, o.client_order_id.trim().to_string()), o);
        }
    }

    let mut open_algo_by_id: HashMap<String, &BinanceAlgoOrderResponse> = HashMap::new();
    let mut open_algo_by_client: HashMap<String, &BinanceAlgoOrderResponse> = HashMap::new();
    for algo in &open_algo_orders {
        if let Some(algo_id) = algo.algo_id {
            open_algo_by_id.insert(algo_id.to_string(), algo);
        }
        if let Some(client_algo_id) = algo.client_algo_id.as_ref() {
            open_algo_by_client.insert(client_algo_id.as_str().to_string(), algo);
        }
    }

    let terminal_conflicts = terminal_order_conflicts(
        &store,
        credential_id,
        &coverage,
        &open_by_order_id,
        &open_by_client_id,
        &open_algo_by_id,
        &open_algo_by_client,
    )
    .await?;

    // Algo orders are a separate identity/status domain.  Reconcile them
    // before ordinary orders so a triggered actualOrderId is available when
    // the ordinary openOrders set is compared below.
    // K04: keyset pagination over (created_at_ms DESC, protection_order_id
    // DESC). Every page is bounded and older obligations are still visited;
    // the cursor is the immutable creation key of the last row read, so rows
    // retired while a page is processed cannot shift or skip the next page,
    // and equal created_at_ms values are split deterministically by id.
    timing.stage(crate::reconcile_trace::Stage::AlgoQueries);
    let mut checked_algos = 0usize;
    let mut changed_algos = 0usize;
    let mut unresolved_algos = Vec::new();
    let mut local_protections = 0usize;
    let mut page_cursor: Option<(i64, String)> = None;
    loop {
        let protection_rows =
            load_protection_reconcile_page(&store, credential_id, page_cursor.as_ref()).await?;
        local_protections += protection_rows.len();
        timing.local_protections(local_protections);
        let full_page = protection_rows.len() >= PROTECTION_RECONCILE_PAGE_SIZE as usize;
        if let Some(last) = protection_rows.last() {
            page_cursor = Some((
                crate::store::try_get_i64_text(last, "created_at_ms_text")?,
                last.try_get("protection_order_id")?,
            ));
        }
        for row in protection_rows {
            let protection_symbol_key: String = row.try_get("symbol_key")?;
            if !coverage.includes_symbol_key(&protection_symbol_key) {
                continue;
            }
            checked_algos += 1;
            let protection_order_id = row.get::<String, _>("protection_order_id");
            if terminal_conflicts
                .protection_ids
                .contains(&protection_order_id)
            {
                // Retain the contradiction and original terminal history on every
                // round; projecting NEW over it would let the next scan claim clean.
                continue;
            }
            let exchange_algo_id = crate::store::try_get_nullable_string(&row, "exchange_algo_id")?
                .unwrap_or_default();
            let client_algo_id =
                crate::store::try_get_nullable_string(&row, "client_algo_id")?.unwrap_or_default();
            let legacy_exchange_order_id =
                crate::store::try_get_nullable_string(&row, "exchange_order_id")?
                    .unwrap_or_default();
            // Rows predating migration 0013 may carry the old conditional
            // /fapi/v1/order id in exchange_order_id. They remain in the ordinary
            // reconciliation domain and must not be reinterpreted as Algo rows.
            if exchange_algo_id.is_empty()
                && client_algo_id.is_empty()
                && !legacy_exchange_order_id.is_empty()
            {
                continue;
            }
            let previous_status = crate::store::try_get_nullable_string(&row, "algo_status")?;
            let position_open = row.get::<i64, _>("position_open") == 1;
            let open_truth = if !exchange_algo_id.is_empty() {
                open_algo_by_id.get(&exchange_algo_id).copied()
            } else if !client_algo_id.is_empty() {
                open_algo_by_client.get(&client_algo_id).copied()
            } else {
                None
            };
            let truth_lookup = algo_truth_lookup(
                open_truth.is_some(),
                !exchange_algo_id.is_empty(),
                !client_algo_id.is_empty(),
            );
            let truth = match truth_lookup {
                AlgoTruthLookup::OpenSet => {
                    open_truth.map(crate::protection::AlgoOrderTruth::from_response)
                }
                AlgoTruthLookup::SingleOrderQuery => {
                    let parsed_algo_id = (!exchange_algo_id.is_empty())
                        .then(|| exchange_algo_id.parse::<i64>())
                        .transpose()
                        .context("stored protection exchange_algo_id is not an integer")?
                        .map(BinanceAlgoId::new);
                    let parsed_client_id = (!client_algo_id.is_empty())
                        .then(|| BinanceClientAlgoId::new(client_algo_id.clone()));
                    let (query_algo_id, query_client_algo_id) = if parsed_algo_id.is_some() {
                        (parsed_algo_id.as_ref(), None)
                    } else {
                        (None, parsed_client_id.as_ref())
                    };
                    timing.algo_query();
                    match client
                        .get_algo_order_json(
                            &limiter,
                            query_algo_id,
                            query_client_algo_id,
                            RequestClass::PeriodicReconcile,
                        )
                        .await
                    {
                        Ok(value) => Some(crate::protection::AlgoOrderTruth::from_json(value)?),
                        Err(error) if error.to_string().contains("\"code\":-2013") => {
                            if position_open {
                                unresolved_algos.push(serde_json::json!({
                                    "protectionOrderId": protection_order_id,
                                    "reason": "single_algo_query_not_found"
                                }));
                            }
                            None
                        }
                        Err(error) => {
                            return Err(error).context("query disappeared Binance algo order")
                        }
                    }
                }
                AlgoTruthLookup::Unidentified => {
                    if position_open {
                        unresolved_algos.push(serde_json::json!({
                            "protectionOrderId": protection_order_id,
                            "reason": "missing_algo_identity"
                        }));
                    }
                    None
                }
            };
            if let Some(truth) = truth {
                if previous_status.as_deref() != truth.algo_status.as_deref()
                    || truth.actual_order_id.is_some()
                {
                    changed_algos += 1;
                }
                let disposition = crate::protection::apply_algo_truth_for_protection(
                    &store,
                    credential_id,
                    &protection_order_id,
                    &truth,
                    truth_source_for_algo_lookup(truth_lookup)
                        .expect("a returned truth always has a REST source"),
                    "reconciliation",
                    ts,
                )
                .await?;
                if position_open
                    && matches!(
                        disposition,
                        crate::protection::AlgoTruthDisposition::Missing
                            | crate::protection::AlgoTruthDisposition::Unknown
                    )
                {
                    unresolved_algos.push(serde_json::json!({
                        "protectionOrderId": protection_order_id,
                        "reason": "terminal_or_incomplete_algo_truth_while_position_open"
                    }));
                }
            }
        }
        if !full_page {
            break;
        }
    }

    timing.stage(crate::reconcile_trace::Stage::OrderQueries);
    let mut changed = 0usize;
    let mut checked = 0usize;
    // 解析不出 USDⓈ-M 身份的本地在途单（regression）。它们既不能查、
    // 也不能被裁决，必须作为 diff 驱动降级/恢复，而不是被静默跳过。
    let mut unidentified_orders: Vec<serde_json::Value> = Vec::new();

    // 3) For each inflight local order, try to align status with exchange.
    for row in inflight_rows {
        // **列解码失败不是"这一列是空的"。**
        //
        // R11 这里整片是 `.try_get(...).ok().flatten().unwrap_or_default()`：
        // 一次解码错误与 `NULL` 产出同一个值。两条最危险的折叠——
        // `exchange_order_id` 折成空串（"交易所从未确认过它"）与
        // `updated_at_ms` 折成 `0`（`now - 0` 必然超过宽限期）——**合起来
        // 正好满足 `unconfirmed_order_is_adjudicable` 的全部条件**，
        // 于是一条解码不出来的活单会被裁决成 `failed` 并释放 closing。
        // 现在逐列原样上抛（`try_get_nullable_string` 负责 `sqlx::Any` 上
        // NULL 与错误的区分），整轮失败而不是编一行真值。
        let engine_order_id: String = row.try_get("engine_order_id")?;
        let exchange_order_id: String =
            crate::store::try_get_nullable_string(&row, "exchange_order_id")?.unwrap_or_default();
        let client_order_id_short: String =
            crate::store::try_get_nullable_string(&row, "client_order_id_short")?
                .unwrap_or_default();
        let symbol_key: String = row.try_get("symbol_key")?;
        if !coverage.includes_symbol_key(&symbol_key) {
            continue;
        }
        checked += 1;
        let current_status: String = row.try_get("status")?;
        // **sqlx-i64 核查批· 这一列必须走文本取真值。**
        //
        // 旧 sqlx 0.7.4 `Any` + SQLite 会把 `updated_at_ms` 截成低 32 位；
        // 精确锁定的 0.8.6 已修复。若将来回归，一条 2026 年的毫秒
        // 时间戳会读成 7.2e8，于是 `now_ms - updated_at_ms ≈ 1.78e12`
        // **恒**超过 `UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS`——这与上面
        // R12 regression 钉死的"折成 0"是**同一个**后果：一条刚下出去、
        // 交易所还没确认的活单立刻满足 `unconfirmed_order_is_adjudicable`
        // 的全部条件，被裁决成 `failed` 并释放 closing。
        let updated_at_ms: i64 = crate::store::try_get_i64_text(&row, "updated_at_ms_text")?;

        let ctx = EventContext {
            bot_uid: row.try_get("bot_uid")?,
            run_id: crate::store::try_get_nullable_string(&row, "run_id")?.unwrap_or_default(),
            cycle_id: crate::store::try_get_nullable_string(&row, "cycle_id")?.unwrap_or_default(),
            idempotency_key: row.try_get("idempotency_key")?,
            client_order_id_short: client_order_id_short.clone(),
            exchange: crate::store::try_get_nullable_string(&row, "exchange")?
                .unwrap_or_else(|| "binance".to_string()),
            market_kind: crate::store::try_get_nullable_string(&row, "market_kind")?
                .unwrap_or_else(|| "futures".to_string()),
            symbol_key: symbol_key.clone(),
        };

        // If exchange says it's still open, keep it open and emit reconcile_diff if local was weird.
        //
        // 身份判据与 WS 侧**同一个**（`usdm_symbol_key_matches` 背后的那套
        // venue/market/symbol/settle）：这里先把本地 `symbol_key` 换算成
        // USDⓈ-M 的交易所 symbol——换算不出来（非五段、异 venue/market、
        // 币本位 settle）就**不认领**任何交易所真值，而不是拿裸 id 去撞。
        let local_symbol = crate::binance_futures::usdm_identity(&symbol_key)
            .map(|identity| identity.symbol.to_ascii_uppercase());
        let open = match local_symbol.as_deref() {
            Some(symbol) if !exchange_order_id.is_empty() => open_by_order_id
                .get(&(symbol.to_string(), exchange_order_id.trim().to_string()))
                .copied(),
            Some(symbol) if !client_order_id_short.is_empty() => open_by_client_id
                .get(&(symbol.to_string(), client_order_id_short.trim().to_string()))
                .copied(),
            _ => None,
        };

        if let Some(open) = open {
            if let Some(next) = binance_order_status_to_engine_status(open.status.as_str()) {
                if next != current_status {
                    if apply_reconciled_status(
                        &store,
                        credential_id,
                        &engine_order_id,
                        &current_status,
                        next,
                        &open.order_id.to_string(),
                        ts,
                    )
                    .await?
                    {
                        changed += 1;
                        emit_order_update(
                            &store,
                            credential_id,
                            &engine_order_id,
                            &ctx,
                            open.order_id.to_string(),
                            ts,
                            "reconcile_open",
                            serde_json::json!({"binance":open}),
                        )
                        .await?;
                    }
                }
            }
            continue;
        }

        // Otherwise, ask exchange for final status (best-effort) and update.
        //
        // R5 FBL-R4-H3：交易所侧**从未确认过**的崩溃残留态（unknown /
        // pending_risk / pending_submit 且无 exchange_order_id）过了宽限期后
        // 允许被裁决为终态死亡；已被确认过的在途单（submitted/acked/
        // partial_filled，或已有 exchange_order_id）一律不裁决——它们的
        // -2013 另有含义（例如交易所对老订单的清理），不在本条 scope 内。
        let adjudicable = unconfirmed_order_is_adjudicable(
            &current_status,
            &exchange_order_id,
            updated_at_ms,
            ts,
        );

        // 连交易所身份都没有 = 从未提交过（claim 后、落单前崩溃）。没有可查
        // 的东西，宽限期到了即裁决；再等下去就是永久锁死。
        if adjudicable && client_order_id_short.is_empty() {
            if adjudicate_order_dead(
                &store,
                credential_id,
                &engine_order_id,
                &ctx,
                &current_status,
                ts,
                "no_exchange_identity",
            )
            .await?
            {
                changed += 1;
            }
            continue;
        }

        // **逐单回查不得退回裸身份。**
        //
        // 上面 open 集合的认领已经用完整身份了，这里却重新
        // `symbol_from_symbol_key(&symbol_key)` 拼一个裸 symbol 去查——
        // `okx|spot|BTC|USDT|BTC` 于是变成一次发往 Binance USDⓈ-M 的
        // `BTCUSDT` 查询，而它的 `-2013`（另一张合约上当然查无此单）会被
        // 当成权威判死，把一条本地单直接裁决成 `failed` 并释放 closing。
        //
        // 现在两处共用**同一次**解析结果 `local_symbol`；解析不出闭世界身份
        // 就一个字节都不问交易所，改为记成 diff 驱动降级与恢复。
        let Some(symbol) = local_symbol.clone() else {
            tracing::error!(
                credential_id,
                engine_order_id = %engine_order_id,
                %symbol_key,
                "reconcile cannot resolve a USDⓈ-M identity for this inflight order; \
                 it is neither queryable nor adjudicable and is reported as a diff"
            );
            if unidentified_orders.len() < 50 {
                unidentified_orders.push(serde_json::json!({
                    "engineOrderId": engine_order_id,
                    "symbolKey": symbol_key,
                    "status": current_status,
                }));
            }
            continue;
        };

        timing.order_query();
        let resp = match client
            .get_order_json(
                &symbol,
                if exchange_order_id.is_empty() {
                    None
                } else {
                    Some(exchange_order_id.as_str())
                },
                if client_order_id_short.is_empty() {
                    None
                } else {
                    Some(client_order_id_short.as_str())
                },
                RequestClass::PeriodicReconcile,
            )
            .await
        {
            Ok(v) => v,
            // An authoritative absence is a completed observation even while
            // the local order is too fresh for terminal adjudication. Preserve
            // that grace period so recovery can resolve durable pending intent.
            Err(error) if error.to_string().contains("\"code\":-2013") => {
                if adjudicable
                    && adjudicate_order_dead(
                        &store,
                        credential_id,
                        &engine_order_id,
                        &ctx,
                        &current_status,
                        ts,
                        "exchange_order_not_found",
                    )
                    .await?
                {
                    changed += 1;
                }
                continue;
            }
            // A failed lookup leaves this order unknown AND the round incomplete.
            // Continuing used to publish Clean and clear reconcile_diff_open after
            // every lookup timed out. Preserve order state, but stop before clean
            // health/kill-switch projection; do not accumulate one timeout per row.
            Err(error) => return Err(error).context("query inflight Binance order truth"),
        };

        if let Some(s) = resp.get("status").and_then(|v| v.as_str()) {
            if let Some(next) = binance_order_status_to_engine_status(s) {
                if next != current_status {
                    let exchange_id = resp
                        .get("orderId")
                        .and_then(|v| v.as_i64())
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| exchange_order_id.clone());

                    if apply_reconciled_status(
                        &store,
                        credential_id,
                        &engine_order_id,
                        &current_status,
                        next,
                        &exchange_id,
                        ts,
                    )
                    .await?
                    {
                        changed += 1;
                        emit_order_update(
                            &store,
                            credential_id,
                            &engine_order_id,
                            &ctx,
                            exchange_id,
                            ts,
                            "reconcile_get_order",
                            serde_json::json!({"binance":resp}),
                        )
                        .await?;
                    }
                }
            }
        }
    }

    timing.stage(crate::reconcile_trace::Stage::Finalize);
    // 3b) R5 FBL-R4-H3：订单裁决之后立刻跑一遍 closing 收敛清扫。
    //
    // `release_closing_if_exit_dead` 的原调用点在 `apply_exchange_position_truth`
    // 里，而仓位真值应用（步骤 1）发生在订单循环（步骤 3）**之前**——本轮刚被
    // 裁决为 `failed` 的订单，其 closing 仓位本轮不会被释放；交易所 positionRisk
    // 未返回该 symbol 时更是永远走不到。清扫按仓位枚举，与「本轮是否恰好收到
    // 该仓位的 truth」解耦。
    //
    // 放在真值应用之后：真正已平掉的仓位那时已被 mark_position_closed 移出
    // closing，不会被误释放回 open。判死仍只由 exit_order_status_is_dead 决定，
    // 本步不新增任何推定。
    {
        let mut tx = store.pool().begin().await?;
        let released =
            crate::position_state::sweep_closing_positions(&mut tx, credential_id, ts).await?;
        tx.commit().await?;
        if released > 0 {
            tracing::warn!(
                credential_id = %credential_id,
                released,
                "reconcile swept closing positions whose pending exit is dead per authority"
            );
        }
    }

    // 3c) WO-M1：交易所单仓平仓的真值终结。原单被判死 ⇒ rejected；原单成交
    // 且本轮 positionRisk（覆盖该 symbol、晚于原单落库）显示该 scope 精确
    // 归零 ⇒ filled。只消费本轮已读到的真值，不发任何请求；B3 fence 在
    // 终结的同一事务里释放，未终结则继续挂着。
    {
        let position_coverage = account.position_coverage.clone();
        let covers = move |symbol_key: &str| position_coverage.includes_symbol_key(symbol_key);
        let venue = crate::position_close::VenuePositions {
            positions: &positions_json,
            read_started_at_ms: position_read_started_ms,
            covers: &covers,
        };
        let finalized =
            crate::position_close::settle_exchange_closes(&store, credential_id, Some(&venue), ts)
                .await
                .map_err(|status| {
                    anyhow::anyhow!(
                        "settle exchange position closes failed: {}",
                        status.message()
                    )
                })?;
        if finalized > 0 {
            tracing::info!(
                credential_id = %credential_id,
                finalized,
                "reconcile finalized exchange position closes from exchange truth"
            );
        }
        // 3d) WO-M1b：前一 boot 在 prepare 与派发之间死亡留下的 prepared 请求。
        // 未发出由持久事实证明（订单行与相位推进同事务、出站在提交之后），
        // 不发任何请求；终结与 fence 释放同事务，绝不重新派发。
        let resolved =
            crate::position_close::resolve_stranded_prepared_closes(&store, credential_id).await;
        if resolved > 0 {
            tracing::warn!(
                credential_id = %credential_id,
                resolved,
                "reconcile resolved close requests a previous boot prepared but never dispatched"
            );
        }
    }

    // 4) Orders present on exchange but missing locally => reconcile_diff.
    //
    // **missing-local 集合必须带 symbol。**
    //
    // R11 把逐单认领的两张 map 换成了 `(symbol, id)`，这一处却仍然是一个
    // **裸 id 集合**：Binance 的 `orderId` 只在 symbol 内唯一、`clientOrderId`
    // 更是调用方自选，于是另一个 symbol 上的一张交易所开放单只要 id 撞上本地
    // 任意一张单，就被判成"本地已知"——一张**没人管的活单**从 diff 里消失，
    // 降级不触发、恢复不驱动。判据与本文件其余三处同源：完整交易身份先把
    // 本地 key 解析成 USDⓈ-M symbol，id 只在这个 symbol 内比对。
    let mut local_ids: HashSet<(String, String)> = HashSet::new();
    let local_rows = sqlx::query(
        r#"
        SELECT exchange_order_id, client_order_id_short, order_role, symbol_key
        FROM engine_orders
        WHERE credential_id = ?
        ORDER BY created_at_ms DESC
        LIMIT 2000
        "#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    // regression：一次 DB 故障与"这个 credential 一张本地单都没有"此前
    // 产出同一个空集合——于是**每一张**交易所开放单都被报成 missing，
    // `reconcile_diff` 里落下一份凭空捏造的差异清单。原样上抛。
    .context("load local order identities for the missing-local projection")?;

    for r in local_rows {
        // 解析不出闭世界身份的本地行不能给任何 symbol 背书；跳过它等于
        // 让对应的交易所开放单进入 missing（fail-closed 方向）。
        let Some(symbol) = crate::store::try_get_nullable_string(&r, "symbol_key")?
            .as_deref()
            .and_then(crate::binance_futures::usdm_symbol)
            .map(|symbol| symbol.to_ascii_uppercase())
        else {
            continue;
        };
        if let Some(v) = crate::store::try_get_nullable_string(&r, "exchange_order_id")? {
            if !v.trim().is_empty() {
                local_ids.insert((symbol.clone(), v.trim().to_string()));
            }
        }
        if !r.get::<String, _>("order_role").starts_with("protection_") {
            if let Some(v) = crate::store::try_get_nullable_string(&r, "client_order_id_short")? {
                if !v.trim().is_empty() {
                    local_ids.insert((symbol.clone(), v.trim().to_string()));
                }
            }
        }
    }
    // 保护单自己不存 `symbol_key`：它的交易身份来自所属仓位，仓位缺失时退到
    // 下单时那条 engine_order。两条来源都认不出身份就不给任何 symbol 背书。
    let local_protection_rows = sqlx::query(
        r#"
        SELECT po.exchange_order_id, po.exchange_algo_id, po.client_algo_id,
               ps.symbol_key AS position_symbol_key,
               eo.symbol_key AS order_symbol_key
        FROM protection_orders po
        LEFT JOIN position_state ps ON ps.position_id = po.position_id
        LEFT JOIN engine_orders eo ON eo.engine_order_id = po.engine_order_id
        WHERE po.credential_id=?
        "#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await?;
    let mut local_algo_ids: HashSet<String> = HashSet::new();
    for row in local_protection_rows {
        let symbol = crate::store::try_get_nullable_string(&row, "position_symbol_key")?
            .or(crate::store::try_get_nullable_string(
                &row,
                "order_symbol_key",
            )?)
            .as_deref()
            .and_then(crate::binance_futures::usdm_symbol)
            .map(|symbol| symbol.to_ascii_uppercase());
        if let (Some(symbol), Some(value)) = (
            symbol.as_ref(),
            crate::store::try_get_nullable_string(&row, "exchange_order_id")?,
        ) {
            if !value.trim().is_empty() {
                local_ids.insert((symbol.clone(), value.trim().to_string()));
            }
        }
        // algo id 侧**没有** symbol 可比：Binance 的 algo 开放单响应不带
        // `symbol` 字段（`BinanceAlgoOrderResponse`），所以这里保留全局
        // id 比对，并如实登记在报告的诚实边界里。
        for column in ["exchange_algo_id", "client_algo_id"] {
            if let Some(value) = crate::store::try_get_nullable_string(&row, column)? {
                if !value.trim().is_empty() {
                    local_algo_ids.insert(value.trim().to_string());
                }
            }
        }
    }

    let mut missing = Vec::new();
    for o in &open_orders {
        let symbol = o.symbol.trim().to_ascii_uppercase();
        let oid = o.order_id.to_string();
        if local_ids.contains(&(symbol.clone(), oid)) {
            continue;
        }
        if !o.client_order_id.trim().is_empty()
            && local_ids.contains(&(symbol.clone(), o.client_order_id.trim().to_string()))
        {
            continue;
        }
        missing.push(serde_json::json!({"orderId":o.order_id,"clientOrderId":o.client_order_id,"symbol":o.symbol,"status":o.status}));
        if missing.len() >= 50 {
            break;
        }
    }

    let mut missing_algos = Vec::new();
    for algo in &open_algo_orders {
        let algo_id = algo.algo_id.map(|value| value.to_string());
        let client_algo_id = algo
            .client_algo_id
            .as_ref()
            .map(BinanceClientAlgoId::as_str);
        if algo_id
            .as_deref()
            .is_some_and(|value| local_algo_ids.contains(value))
            || client_algo_id.is_some_and(|value| local_algo_ids.contains(value))
        {
            continue;
        }
        missing_algos.push(serde_json::json!({
            "algoId": algo_id,
            "clientAlgoId": client_algo_id,
            "algoStatus": algo.algo_status,
        }));
        if missing_algos.len() >= 50 {
            break;
        }
    }

    let disposition = reconcile_disposition(
        !missing.is_empty(),
        !missing_algos.is_empty(),
        !unresolved_algos.is_empty(),
        !position_outcome.orphan_positions.is_empty(),
        !unidentified_orders.is_empty(),
        !terminal_conflicts.ordinary.is_empty() || !terminal_conflicts.algos.is_empty(),
    );
    let has_diff = disposition_has_diff(&disposition);
    let all_positions_zero = position_outcome.all_positions_zero;
    let local_position_zero_diff = position_outcome.local_position_zero_diff;

    let orphan_positions_count = position_outcome.orphan_positions.len();
    let event_payload = serde_json::json!({
        "paper": false,
        "source": "reconcile",
        "coverage": coverage,
        "checkedInflight": checked,
        "changedOrders": changed,
        "openOrdersCount": open_orders.len(),
        "checkedAlgoOrders": checked_algos,
        "changedAlgoOrders": changed_algos,
        "openAlgoOrdersCount": open_algo_orders.len(),
        "missingLocalOpenOrders": missing,
        "missingLocalOpenAlgoOrders": missing_algos,
        "unresolvedAlgoOrders": unresolved_algos,
        "unidentifiedLocalOrders": unidentified_orders,
        "terminalLocalRemoteOpenOrders": terminal_conflicts.ordinary,
        "terminalLocalRemoteOpenAlgoOrders": terminal_conflicts.algos,
        "orphanPositions": position_outcome.orphan_positions,
        "summary": {
          "hasDiff": has_diff,
          "checkedInflight": checked,
          "changedOrders": changed,
          "missingLocalOpenOrdersCount": missing.len(),
          "openOrdersCount": open_orders.len(),
          "checkedAlgoOrders": checked_algos,
          "changedAlgoOrders": changed_algos,
          "missingLocalOpenAlgoOrdersCount": missing_algos.len(),
          "unresolvedAlgoOrdersCount": unresolved_algos.len(),
          "unidentifiedLocalOrdersCount": unidentified_orders.len(),
          "terminalLocalRemoteOpenOrdersCount": terminal_conflicts.ordinary.len(),
          "terminalLocalRemoteOpenAlgoOrdersCount": terminal_conflicts.algos.len(),
          "openAlgoOrdersCount": open_algo_orders.len(),
          "orphanPositionsCount": orphan_positions_count,
        },
    });
    publish_reconcile_disposition_with_coverage(
        &store,
        credential_id,
        ts,
        &disposition,
        event_payload,
        coverage.is_full(),
    )
    .await?;

    let truth = crate::kill_switch::ReconcileTruth {
        has_diff,
        all_positions_zero,
        all_open_orders_empty: open_orders.is_empty(),
        all_open_algo_orders_empty: open_algo_orders.is_empty(),
        local_position_zero_diff,
    };
    // A narrowed projection cannot prove account-wide emptiness.  Never feed it
    // to the existing L3 convergence predicate; the scheduler forces FullScan
    // whenever an L3 latch is active, and this guard is the second line.
    if coverage.is_full() {
        for scope_key in ["global".to_string(), format!("cred:{credential_id}")] {
            if let Err(error) = crate::kill_switch::record_reconcile_for_scope(
                &store,
                &scope_key,
                credential_id,
                truth,
                ts,
                &observed_generations,
            )
            .await
            {
                // **regression**：这是对账真值向 kill-switch 的**权威投影**，
                // 写失败之后本轮就不再是一次完整对账了。R11 只 warn 一行然后
                // 照常 `Ok(truth)`——L3 的 `flatten_complete()` 判据据此认为
                // "已经收敛"。现在原样上抛，整轮失败。
                return Err(error).context("record reconcile truth for kill-switch scope");
            }
        }
        let bot_rows =
            sqlx::query("SELECT DISTINCT bot_uid FROM position_state WHERE credential_id = ?")
                .bind(credential_id)
                .fetch_all(store.pool())
                .await
                // DB 故障与"这个 credential 没有任何 bot 仓位"此前折成同一个空集合：
                // 每个 bot scope 的对账投影**一个都不写**，而本轮仍然报成功。
                .context("enumerate bot scopes for the reconcile truth projection")?;
        for row in bot_rows {
            let scope_key = format!("bot:{}", row.try_get::<String, _>("bot_uid")?);
            crate::kill_switch::record_reconcile_for_scope(
                &store,
                &scope_key,
                credential_id,
                truth,
                ts,
                &observed_generations,
            )
            .await
            .context("record reconcile truth for bot scope")?;
        }
    }

    if coverage.is_full() {
        persist_last_full_scan_ms(&store, credential_id, ts).await?;
        tracing::info!(
            credential_id,
            completed_at_ms = ts,
            "unbiased full reconcile scan completed"
        );
    }
    order_observation.finish(truth, now_ms());
    let covered = CoveredReconcileTruth { truth, coverage };
    debug_assert_eq!(
        covered.clone().flatten_complete(),
        covered.coverage.is_full() && truth.flatten_complete()
    );
    Ok(covered)
}

// Validate decimal text without float rounding or treating malformed strings
// such as "." and "+" as zero. This predicate is also the deletion guard for
// obsolete zero projections; unknown or nonzero amounts must remain visible.
pub(crate) fn snapshot_decimal_is_zero(value: &str) -> Option<bool> {
    let value = value.trim();
    let magnitude = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    let mut digits = 0;
    let mut dots = 0;
    let mut zero = true;
    for byte in magnitude.bytes() {
        match byte {
            b'0'..=b'9' => {
                digits += 1;
                zero &= byte == b'0';
            }
            b'.' => dots += 1,
            _ => return None,
        }
    }
    (digits > 0 && dots <= 1).then_some(zero)
}

#[cfg(test)]
async fn persist_positions_and_balances(
    store: &Store,
    credential_id: &str,
    ts: i64,
    positions_json: &serde_json::Value,
    balances_json: &serde_json::Value,
) -> anyhow::Result<PositionSnapshotOutcome> {
    persist_positions_and_balances_with_coverage(
        store,
        credential_id,
        ts,
        positions_json,
        balances_json,
        &Coverage::Full,
    )
    .await
}

// A MySQL snapshot SELECT can precede another round's committed upsert.
// The DELETE is a current read: match the exact observed quantity so an old
// zero observation can never remove a newly nonzero position.
async fn retire_observed_zero_projection(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol: &str,
    observed_qty: &str,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM engine_positions WHERE credential_id=? AND symbol=? AND position_amt=?",
    )
    .bind(credential_id)
    .bind(symbol)
    .bind(observed_qty)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

async fn persist_positions_and_balances_with_coverage(
    store: &Store,
    credential_id: &str,
    ts: i64,
    positions_json: &serde_json::Value,
    balances_json: &serde_json::Value,
    position_coverage: &Coverage,
) -> anyhow::Result<PositionSnapshotOutcome> {
    // An account response is one observation, not a best-effort prefix. Reject
    // invalid identity/quantity or duplicate symbol rows before any mutation.
    // The current projection key cannot represent both hedge legs of a symbol;
    // silently overwriting one with the other would hide exposure.
    let positions = positions_json
        .as_array()
        .context("position snapshot is not an array")?;
    let balances = balances_json
        .as_array()
        .context("balance snapshot is not an array")?;
    anyhow::ensure!(
        positions.len() <= 5_000,
        "position snapshot exceeds supported entry count"
    );
    anyhow::ensure!(
        balances.len() <= 1_024,
        "balance snapshot exceeds supported entry count"
    );
    let mut seen_symbols = HashSet::new();
    for position in positions {
        let symbol = position
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        let qty = position
            .get("positionAmt")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        anyhow::ensure!(!symbol.is_empty(), "position snapshot has missing symbol");
        anyhow::ensure!(
            snapshot_decimal_is_zero(qty).is_some(),
            "position snapshot has invalid quantity"
        );
        anyhow::ensure!(
            seen_symbols.insert(symbol),
            "position snapshot has duplicate symbol"
        );
    }
    validate_position_financial_fields(positions_json)?;
    let mut seen_assets = HashSet::new();
    for balance in balances {
        let asset = balance
            .get("asset")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        anyhow::ensure!(!asset.is_empty(), "balance snapshot has missing asset");
        anyhow::ensure!(
            seen_assets.insert(asset),
            "balance snapshot has duplicate asset"
        );
    }
    let mut tx = store.pool().begin().await?;
    // A complete response supersedes old ZERO projections missing from its
    // actual position coverage. Keep
    // missing nonzero/invalid rows fail-closed, and never modify position_state,
    // orders or fills based only on absence. A symbol-scoped WS round cannot
    // remove any other symbol, whereas REST positionRisk is account-wide.
    let previous =
        sqlx::query("SELECT symbol, position_amt FROM engine_positions WHERE credential_id=?")
            .bind(credential_id)
            .fetch_all(&mut *tx)
            .await?;
    for row in previous {
        let symbol: String = row.try_get("symbol")?;
        let qty: String = row.try_get("position_amt")?;
        let covered = match position_coverage {
            Coverage::Full => true,
            Coverage::Symbols(symbols) => symbols.iter().any(|s| s == &symbol),
        };
        if covered
            && !seen_symbols.contains(symbol.as_str())
            && snapshot_decimal_is_zero(&qty) == Some(true)
        {
            retire_observed_zero_projection(&mut tx, credential_id, &symbol, &qty).await?;
        }
    }
    let local_rows = sqlx::query(
        r#"
        SELECT position_id, bot_uid, symbol_key, direction, entry_epoch, state
        FROM position_state WHERE credential_id = ?
        ORDER BY updated_at_ms DESC
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut *tx)
    .await?;
    let mut local_positions = local_rows
        .into_iter()
        .map(|row| LocalPositionObservation {
            position_id: row.get("position_id"),
            bot_uid: row.get("bot_uid"),
            symbol_key: row.get("symbol_key"),
            direction: row.get("direction"),
            entry_epoch: row.get("entry_epoch"),
            state: row.get("state"),
        })
        .collect::<Vec<_>>();
    let mut orphan_positions = Vec::new();
    // Persist every position in the validated response; never truncate exposure.
    if let Some(arr) = positions_json.as_array() {
        for p in arr {
            let symbol = p
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            if symbol.is_empty() {
                continue;
            }
            let position_amt = p.get("positionAmt").and_then(|v| v.as_str()).unwrap_or("");
            let entry_price = p.get("entryPrice").and_then(|v| v.as_str()).unwrap_or("");
            let mark_price = p.get("markPrice").and_then(|v| v.as_str()).unwrap_or("");
            let leverage = p.get("leverage").and_then(|v| v.as_str()).unwrap_or("");
            let unrealized = p
                .get("unRealizedProfit")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let liquidation_price = p
                .get("liquidationPrice")
                .and_then(|v| v.as_str())
                .filter(|value| !value.trim().is_empty());
            let disposition = orphan_position_disposition(
                symbol,
                p.get("positionSide").and_then(|value| value.as_str()),
                position_amt,
                &local_positions,
            )
            .map_err(anyhow::Error::msg)?;

            let raw = p.to_string().into_bytes();
            let q = match store.kind() {
                crate::store::DbKind::MySql => {
                    r#"
                    INSERT INTO engine_positions (
                      credential_id, exchange, market_kind, symbol, symbol_key,
                      position_amt, entry_price, mark_price, leverage, unrealized_pnl,
                      liquidation_price, updated_at_ms, raw_json
                    ) VALUES (?, 'binance', 'futures', ?, NULL,
                              ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                      position_amt=VALUES(position_amt),
                      entry_price=VALUES(entry_price),
                      mark_price=VALUES(mark_price),
                      leverage=VALUES(leverage),
                      unrealized_pnl=VALUES(unrealized_pnl),
                      liquidation_price=VALUES(liquidation_price),
                      updated_at_ms=VALUES(updated_at_ms),
                      raw_json=VALUES(raw_json)
                    "#
                }
                _ => {
                    r#"
                    INSERT INTO engine_positions (
                      credential_id, exchange, market_kind, symbol, symbol_key,
                      position_amt, entry_price, mark_price, leverage, unrealized_pnl,
                      liquidation_price, updated_at_ms, raw_json
                    ) VALUES (?, 'binance', 'futures', ?, NULL,
                              ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(credential_id, symbol) DO UPDATE SET
                      position_amt=excluded.position_amt,
                      entry_price=excluded.entry_price,
                      mark_price=excluded.mark_price,
                      leverage=excluded.leverage,
                      unrealized_pnl=excluded.unrealized_pnl,
                      liquidation_price=excluded.liquidation_price,
                      updated_at_ms=excluded.updated_at_ms,
                      raw_json=excluded.raw_json
                    "#
                }
            };

            sqlx::query(q)
                .bind(credential_id)
                .bind(symbol)
                .bind(position_amt)
                .bind(entry_price)
                .bind(mark_price)
                .bind(leverage)
                .bind(unrealized)
                .bind(liquidation_price)
                .bind(ts)
                .bind(raw)
                .execute(&mut *tx)
                .await?;
            if let Some(recovery) = recover_orphan_position(
                &mut tx,
                credential_id,
                position_amt,
                Some(entry_price),
                ts,
                disposition,
            )
            .await?
            {
                local_positions.push(LocalPositionObservation {
                    position_id: recovery.position_id.clone(),
                    bot_uid: recovery.bot_uid.clone(),
                    symbol_key: recovery.symbol_key.clone(),
                    direction: recovery.direction.clone(),
                    entry_epoch: recovery.entry_epoch,
                    state: "open".to_string(),
                });
                orphan_positions.push(recovery);
            }
            crate::position_state::apply_exchange_position_truth(
                &mut tx,
                credential_id,
                symbol,
                p.get("positionSide").and_then(|value| value.as_str()),
                position_amt,
                Some(entry_price),
                ts,
            )
            .await?;
        }
    }

    // Persist every asset in the validated balance response.
    if let Some(arr) = balances_json.as_array() {
        for b in arr {
            let asset = b.get("asset").and_then(|v| v.as_str()).unwrap_or("").trim();
            if asset.is_empty() {
                continue;
            }
            let wallet = b.get("balance").and_then(|v| v.as_str()).unwrap_or("");
            let available = b
                .get("availableBalance")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let raw = b.to_string().into_bytes();
            let q = match store.kind() {
                crate::store::DbKind::MySql => {
                    r#"
                    INSERT INTO engine_balances (
                      credential_id, exchange, market_kind, asset,
                      wallet_balance, available_balance, updated_at_ms, raw_json
                    ) VALUES (?, 'binance', 'futures', ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                      wallet_balance=VALUES(wallet_balance),
                      available_balance=VALUES(available_balance),
                      updated_at_ms=VALUES(updated_at_ms),
                      raw_json=VALUES(raw_json)
                    "#
                }
                _ => {
                    r#"
                    INSERT INTO engine_balances (
                      credential_id, exchange, market_kind, asset,
                      wallet_balance, available_balance, updated_at_ms, raw_json
                    ) VALUES (?, 'binance', 'futures', ?, ?, ?, ?, ?)
                    ON CONFLICT(credential_id, asset) DO UPDATE SET
                      wallet_balance=excluded.wallet_balance,
                      available_balance=excluded.available_balance,
                      updated_at_ms=excluded.updated_at_ms,
                      raw_json=excluded.raw_json
                    "#
                }
            };

            sqlx::query(q)
                .bind(credential_id)
                .bind(asset)
                .bind(wallet)
                .bind(available)
                .bind(ts)
                .bind(raw)
                .execute(&mut *tx)
                .await?;
        }
    }
    let all_positions_zero = positions_json
        .as_array()
        .map(|positions| {
            positions.iter().all(|position| {
                position
                    .get("positionAmt")
                    .and_then(|value| value.as_str())
                    .is_some_and(crate::position_state::is_decimal_zero)
            })
        })
        .unwrap_or(false);
    let active_local_positions = sqlx::query(
        "SELECT COUNT(*) AS n FROM position_state WHERE credential_id = ? AND state <> 'closed'",
    )
    .bind(credential_id)
    .fetch_one(&mut *tx)
    .await
    // **regression**：R11 把 DB 故障折成"有 1 个活仓"。方向看着保守，
    // 可它照样把一次**没查成**的结果当成一条本地真值送进 L3 的
    // `local_position_zero_diff`，而调用方对此一无所知。原样上抛。
    .context("count active local positions for the reconcile truth")?
    .try_get::<i64, _>("n")?;
    // Keep the L3 terminal predicate byte-for-byte equivalent to its pre-T3.10
    // meaning. Orphan detection is a current reconcile diff; it is not an
    // extra input to local/exchange zero-position convergence.
    let local_position_zero_diff = !all_positions_zero || active_local_positions == 0;
    tx.commit().await?;
    Ok(PositionSnapshotOutcome {
        all_positions_zero,
        active_local_positions,
        local_position_zero_diff,
        orphan_positions,
    })
}

/// **审计事件写失败也不许被吞。**
///
/// R11 这里是 `if let Ok(mut tx) = begin { let _ = emit_event; let _ = commit }`：
/// begin/emit/commit 三处失败一律不可见。调用方刚刚把一张单的状态改写成
/// 交易所真值并 `changed += 1`，审计侧却可能什么都没有——"改过什么"这条
/// 事实凭空消失，而本轮仍报成功。三处原样上抛，整轮失败。
async fn emit_order_update(
    store: &Store,
    credential_id: &str,
    engine_order_id: &str,
    ctx: &EventContext,
    exchange_order_id: String,
    ts: i64,
    reason: &str,
    extra: serde_json::Value,
) -> anyhow::Result<()> {
    let event_id = crate::svc::sha256_hex(&format!(
        "reconcile|order_update|{}|{}|{}",
        credential_id, exchange_order_id, ts
    ));
    let payload = serde_json::json!({
        "runMode": "live",
        "paper": false,
        "source": "reconcile",
        "reason": reason,
        "ts": ts,
        "extra": extra,
    });

    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin reconcile order_update audit event")?;
    crate::svc::emit_event(
        &mut tx,
        store.kind(),
        credential_id,
        "order_update",
        ctx,
        engine_order_id,
        Some(exchange_order_id.as_str()),
        Some(payload),
        Some(event_id),
    )
    .await
    .map_err(|error| anyhow::anyhow!("emit reconcile order_update audit event: {error}"))?;
    tx.commit()
        .await
        .context("commit reconcile order_update audit event")?;
    Ok(())
}

async fn emit_reconcile_diff(
    store: &Store,
    credential_id: &str,
    ts: i64,
    payload: serde_json::Value,
) -> anyhow::Result<()> {
    let event_id = crate::svc::sha256_hex(&format!("reconcile_diff|{}|{}", credential_id, ts));
    let ctx = EventContext {
        bot_uid: "".to_string(),
        run_id: "".to_string(),
        cycle_id: "".to_string(),
        idempotency_key: "".to_string(),
        client_order_id_short: "".to_string(),
        exchange: "binance".to_string(),
        market_kind: "futures".to_string(),
        symbol_key: "".to_string(),
    };
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin reconcile_diff audit event")?;
    crate::svc::emit_event(
        &mut tx,
        store.kind(),
        credential_id,
        "reconcile_diff",
        &ctx,
        "",
        None,
        Some(payload),
        Some(event_id),
    )
    .await
    .map_err(|error| anyhow::anyhow!("emit reconcile_diff audit event: {error}"))?;
    tx.commit()
        .await
        .context("commit reconcile_diff audit event")?;
    Ok(())
}

#[cfg(test)]
async fn publish_reconcile_disposition(
    store: &Store,
    credential_id: &str,
    ts: i64,
    disposition: &ReconcileDisposition,
    payload: serde_json::Value,
) -> anyhow::Result<()> {
    publish_reconcile_disposition_with_coverage(
        store,
        credential_id,
        ts,
        disposition,
        payload,
        true,
    )
    .await
}

async fn publish_reconcile_disposition_with_coverage(
    store: &Store,
    credential_id: &str,
    ts: i64,
    disposition: &ReconcileDisposition,
    payload: serde_json::Value,
    may_clear_existing_diff: bool,
) -> anyhow::Result<()> {
    let has_diff = disposition_has_diff(disposition);
    // Reuse the existing credential degrade and reconcile_diff event chain.
    // This function intentionally has no exchange client and cannot flatten.
    // A clean narrowed scan says only "no difference in these symbols"; it
    // cannot clear a difference previously found by an unbiased full scan.
    if has_diff || may_clear_existing_diff {
        apply_reconcile_diff_open(store, credential_id, ts, has_diff).await?;
    }
    emit_reconcile_diff(store, credential_id, ts, payload).await
}

/// **段③ 批 0（H-01 High / M-04 Medium）· 对账差异有了自己的列。**
///
/// 旧实现把"对账差异未消解"写进 `degraded` + `degraded_reason` 这个**唯一的
/// 降级槽位**，而槽位另有一个写者（熔断器）。置位守卫按**标志**
///（`WHEN degraded = 0`）、`clear_credential_errors` 的保护守卫按**原因**
///（`degraded_reason = 'reconcile_diff'`）——交易所错误先占住标志，差异就
/// 一个字都写不进去，随后保护守卫判的正是那个没被写进去的值。根因是槽位
/// 复用叠加置位/解除判据不对称（见迁移 `0029` 的文件头）。
///
/// 这个函数从此**只**写 `reconcile_diff_open` 一列，与 `degraded` /
/// `degraded_reason` 完全解耦：那两列专属熔断器，这一列专属对账。
///
/// 置位/解除都是**权威写**：置位失败等于"发现了差异但没人被降级"，解除失败
/// 方向保守但同样不可见。R11 把两处 `let _ =` 改成上抛（regression 同型），
/// 但上抛只覆盖 **DB 错误**——"SQL 成功、CASE 判假、零行被改"这种**语义空转**
/// 与成功逐字不可区分，因为函数丢弃了 `rows_affected`。这里补上：零行被改
/// 一律按权威写失败处置（回读确认过确实已是目标值的幂等重写除外，见下）。
async fn apply_reconcile_diff_open(
    store: &Store,
    credential_id: &str,
    ts: i64,
    has_diff: bool,
) -> anyhow::Result<()> {
    let target = i64::from(has_diff);

    // 1) 先保证主键行存在。**零差异同样是一条要被产出的真值**："有一轮对账
    //    真的跑完并观察到零差异"必须落成显式的 0，而不是留一个"没人写过"的
    //    空缺——空缺在读侧按 fail-closed 完成（engine safety contract）。
    let ensure = match store.kind() {
        crate::store::DbKind::MySql => {
            r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, ?)
                ON DUPLICATE KEY UPDATE credential_id=VALUES(credential_id)
                "#
        }
        _ => {
            r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, ?)
                ON CONFLICT(credential_id) DO NOTHING
                "#
        }
    };
    sqlx::query(ensure)
        .bind(credential_id)
        .bind(ts)
        .bind(target)
        .execute(store.pool())
        .await
        .context("ensure the credential health row before the reconcile_diff_open write")?;

    // 2) 权威写。**无条件**——不看 `degraded`、不看 `degraded_reason`，
    //    因此交易所错误串再也挡不住差异的置位，ACK 清零也再也抹不掉它。
    let updated = sqlx::query(
        "UPDATE engine_credential_health \
         SET reconcile_diff_open = ?, updated_at_ms = ? WHERE credential_id = ?",
    )
    .bind(target)
    .bind(ts)
    .bind(credential_id)
    .execute(store.pool())
    .await
    .context("write the authoritative reconcile_diff_open state")?;

    if updated.rows_affected() == 0 {
        // MySQL 在"每一列的新值与旧值逐字相同"时同样报 0 行被改，与"这一行
        // 已经不在了"不可区分。所以零行被改**既不能**直接当成成功、**也不能**
        // 直接当成失败：回读一次，用这一列**现在的值**判定。`-1` 是哨兵——
        // `NULL`（本列从未被写过）绝不等于任何合法目标值。
        let row = sqlx::query(
            "SELECT COALESCE(reconcile_diff_open, -1) AS reconcile_diff_open \
             FROM engine_credential_health WHERE credential_id = ? LIMIT 1",
        )
        .bind(credential_id)
        .fetch_optional(store.pool())
        .await
        .context("read back reconcile_diff_open after a zero-row authoritative write")?;
        let observed = row.map(|row| row.get::<i64, _>("reconcile_diff_open"));
        if observed != Some(target) {
            anyhow::bail!(
                "authoritative reconcile_diff_open write changed no row and the stored value is \
                 {observed:?}, not {target} (credential_id={credential_id})"
            );
        }
    }
    Ok(())
}

/// **段③ 批 0 · 迁移 0029 的存量行复核。**
///
/// 0029 零 DML：存量行的 `reconcile_diff_open` 是 `NULL`，语义是"本列从未被
/// 写过"。H-01 的本质决定了「历史上被掩盖的差异」**不可从库里恢复**——被掩盖
/// 的那一次按定义就是"一个字都没写进健康行"。所以对存量行诚实的值只有
/// "不知道"，而 engine safety contract 不许把"不知道"转换成"正常"。
///
/// 这一步在 `Store::connect` 里、迁移之后、任何对账任务启动之前跑，逐行给出
/// 结论并留证据，把 `NULL` 收敛成显式的 1——让这条状态可读可审计，而不是靠
/// 每一个读者都记得写 `COALESCE`（读侧的 fail-closed 仍然保留，两层都要）。
///
/// 两种结论都写 1，区别只在**理由**：
///
/// * `diff_open_proven` —— `degraded <> 0 AND degraded_reason = 'reconcile_diff'`：
///   差异被记录过、此后没被解除。这一行**今天就在拦**开仓，置 1 无行为变化。
/// * `unprovable` —— 其余存量行。被掩盖后又被 ACK 清零的行，其
///   `degraded=0 / reason=NULL / consecutive_errors=0 / first=last=0`
///   与"从未出过错"逐字不可区分，排除不了被掩盖的差异 ⇒ fail-closed。
///
/// 幂等：只处理 `reconcile_diff_open IS NULL` 的行，跑过一次之后是空集；
/// 健康库（0029 之后创建的行）上零写入。爆炸半径也是有界的——
/// `engine_credential_health` 的行只由实盘交易所路径创建，没有行的凭据
/// （paper、从未实盘下过单的）不受影响。
pub(crate) async fn revalidate_legacy_credential_reconcile_diff(
    store: &Store,
    now_ms: i64,
) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        "SELECT credential_id, degraded, degraded_reason \
         FROM engine_credential_health \
         WHERE reconcile_diff_open IS NULL ORDER BY credential_id",
    )
    .fetch_all(store.pool())
    .await
    .context("load legacy credential health rows for reconcile_diff revalidation")?;

    let mut written = 0usize;
    // ****：状态盖章与证据写入必须在同一事务。批 0 的
    // 两条独立语句之间存在崩溃窗口——状态已写、证据缺失，且下次启动的选取集
    // （`reconcile_diff_open IS NULL`）永远不再选中该行，审计链缺口不可自愈。
    // 证据写失败时整个事务回滚，让下一次启动重新选中并两写一起补齐。
    let mut tx = store
        .pool()
        .begin()
        .await
        .context("begin credential reconcile diff revalidation")?;
    for row in rows {
        let credential_id: String = row.get("credential_id");
        let degraded: i64 = row.get("degraded");
        // **真实升级演练抓到的缺陷（2026-08-28）**：`degraded_reason`
        // 是 TEXT 列，MySQL 按 BLOB 返回，`row.get::<String>` 必崩。        // 已有 `try_get_text` 归一 helper，批 0 的这个复核漏用——升级路径
        // 从未在 MySQL 上演练过的后果直接现形：真实升级库一启动就 panic。
        let reason_opt = crate::store::try_get_text(&row, "degraded_reason")
            .context("read degraded_reason for reconcile_diff revalidation")?;
        // **段① R1 · SEG1-RUST-2**：evidence 里 NULL（本列从未被写过）
        // 与真实空串是两个事实，`unwrap_or_default` 会把 NULL 伪装成 ''——
        // 证据文本按原值区分（<NULL> 哨兵），判定比较仍用解包值。
        let reason = reason_opt.clone().unwrap_or_default();
        let (verdict, evidence) = if degraded != 0 && reason == "reconcile_diff" {
            (
                "diff_open_proven",
                "degraded<>0 and degraded_reason='reconcile_diff': a reconcile diff was \
                 recorded and never observed resolved"
                    .to_string(),
            )
        } else {
            (
                "unprovable",
                format!(
                    "degraded={degraded} degraded_reason={}: the current row cannot \
                     exclude a reconcile diff masked by the shared degrade slot; \
                     fail closed",
                    match reason_opt.as_deref() {
                        Some(value) => format!("'{value}'"),
                        None => "<NULL>".to_string(),
                    }
                ),
            )
        };

        // `AND reconcile_diff_open IS NULL` 是并发守卫：真正跑过一轮的对账
        // 是这一列的权威写者。它若已经写下真值，复核**不覆盖**、也不记账。
        let stamped = sqlx::query(
            "UPDATE engine_credential_health SET reconcile_diff_open = 1 \
             WHERE credential_id = ? AND reconcile_diff_open IS NULL",
        )
        .bind(&credential_id)
        .execute(&mut *tx)
        .await
        .context("stamp the revalidated reconcile_diff_open value")?;
        if stamped.rows_affected() == 0 {
            continue;
        }

        let record = match store.kind() {
            crate::store::DbKind::MySql => {
                r#"
                INSERT INTO engine_credential_reconcile_diff_revalidations (
                  credential_id, verdict, evidence, revalidated_at_ms
                ) VALUES (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  verdict=VALUES(verdict), evidence=VALUES(evidence),
                  revalidated_at_ms=VALUES(revalidated_at_ms)
                "#
            }
            _ => {
                r#"
                INSERT INTO engine_credential_reconcile_diff_revalidations (
                  credential_id, verdict, evidence, revalidated_at_ms
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(credential_id) DO UPDATE SET
                  verdict=excluded.verdict, evidence=excluded.evidence,
                  revalidated_at_ms=excluded.revalidated_at_ms
                "#
            }
        };
        sqlx::query(record)
            .bind(&credential_id)
            .bind(verdict)
            .bind(&evidence)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .context("record the reconcile_diff_open revalidation verdict")?;
        written += 1;
    }
    tx.commit()
        .await
        .context("commit credential reconcile diff revalidation")?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    include!("reconcile_k05_tests.rs");
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Once};
    use std::time::Duration;

    use sqlx::Row;

    use super::{
        algo_truth_lookup, apply_reconciled_status, credential_stagger_ms, last_full_scan_ms,
        latest_full_periodic_reconcile_truth_since, orphan_position_disposition,
        persist_last_full_scan_ms, persist_positions_and_balances, publish_reconcile_disposition,
        read_account_truth, reconcile_binance_futures_truth_with_client,
        reconcile_binance_futures_truth_with_client_and_coverage,
        reconcile_binance_futures_truth_with_client_and_coverage_at,
        reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws, reconcile_disposition,
        reconcile_tier, reconcile_tier_with_safety_fallbacks, recovery_reconcile_divergence,
        remember_latest_periodic_reconcile_truth, revalidate_legacy_credential_reconcile_diff,
        run_reconcile_round, run_reconcile_round_with_ws, selected_periodic_tier,
        truth_source_for_algo_lookup, unconfirmed_order_is_adjudicable, AccountReadSource,
        AlgoTruthLookup, Coverage, CoveredReconcileTruth, LocalPositionObservation,
        OrphanPositionDisposition, ReconcileDiffCauses, ReconcileDisposition,
        ReconcileExchangeSource, ReconcileTier, UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS,
    };
    use crate::binance_futures::BinanceFuturesClient;
    use crate::rate_limiter::{CredentialRateLimiter, TokenBucketConfig};
    use crate::store::Store;
    use crate::svc::now_ms;

    static INSTALL_DRIVERS: Once = Once::new();

    async fn test_store(label: &str) -> (Store, std::path::PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-reconcile-{label}-{}.sqlite",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect reconcile test database");
        (store, path)
    }

    async fn cleanup(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    fn a9_r1_rest_client(credential_id: &str) -> BinanceFuturesClient {
        BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                ),
                (200, balances().to_string()),
            ],
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn startup_stream_owns_initial_scan_but_ordinary_rpc_reconciles_immediately() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        struct RestoreInterval(Option<std::ffi::OsString>);
        impl Drop for RestoreInterval {
            fn drop(&mut self) {
                match self.0.take() {
                    Some(value) => std::env::set_var("RECONCILE_INTERVAL_MS", value),
                    None => std::env::remove_var("RECONCILE_INTERVAL_MS"),
                }
            }
        }
        let _restore = RestoreInterval(std::env::var_os("RECONCILE_INTERVAL_MS"));
        std::env::set_var("RECONCILE_INTERVAL_MS", "1000");
        let (store, path) = test_store("initial-scan-owner").await;
        let delayed_client = a9_r1_rest_client("startup-owned");
        let immediate_client = a9_r1_rest_client("ordinary-owned");
        let delayed =
            super::ReconcilerManager::with_stubbed_exchange_for_tests(delayed_client.clone());
        let immediate =
            super::ReconcilerManager::with_stubbed_exchange_for_tests(immediate_client.clone());
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0));
        let keys = crate::binance_futures::BinanceKeys {
            api_key: "unused".into(),
            api_secret: "unused".into(),
        };
        delayed
            .ensure_running_after_stream_startup(
                "startup-owned",
                store.clone(),
                limiter.clone(),
                keys.clone(),
            )
            .await;
        assert!(
            delayed.is_running_for_tests("startup-owned").await,
            "the main owner must hold the periodic task even before its first tick"
        );
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        assert!(
            delayed_client.recorded_requests().is_empty(),
            "cold startup must not duplicate the user stream's initial full scan"
        );

        immediate
            .ensure_running("ordinary-owned", store.clone(), limiter, keys)
            .await;
        let immediate_deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        while immediate_client.recorded_requests().len() < 4 {
            assert!(
                std::time::Instant::now() < immediate_deadline,
                "ordinary RPC recovery must run before the first 1000ms periodic tick"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let delayed_deadline = std::time::Instant::now() + std::time::Duration::from_millis(1500);
        while delayed_client.recorded_requests().len() < 4 {
            assert!(
                std::time::Instant::now() < delayed_deadline,
                "cold startup must execute a real full scan when its normal tick arrives"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for owner in [&delayed, &immediate] {
            for (_, task) in owner.inner.lock().await.drain() {
                task.abort();
            }
        }
        cleanup(store, path).await;
    }

    /// A2c-R1 用的桩交易所：每一轮全量扫描消费四条响应
    /// （开放单 / 开放算法单 / 仓位 / 余额），按轮数重复。
    fn a2c_r1_rest_client(credential_id: &str, rounds: usize) -> BinanceFuturesClient {
        let mut responses = Vec::new();
        for _ in 0..rounds {
            responses.push((200, "[]".to_string()));
            responses.push((200, "[]".to_string()));
            responses.push((
                200,
                serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
            ));
            responses.push((200, balances().to_string()));
        }
        BinanceFuturesClient::new_with_mock_responses(credential_id, responses)
    }

    struct A2cR1IntervalEnv(Option<std::ffi::OsString>);

    impl A2cR1IntervalEnv {
        fn set(value: &str) -> Self {
            let previous = std::env::var_os("RECONCILE_INTERVAL_MS");
            std::env::set_var("RECONCILE_INTERVAL_MS", value);
            Self(previous)
        }
    }

    impl Drop for A2cR1IntervalEnv {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("RECONCILE_INTERVAL_MS", value),
                None => std::env::remove_var("RECONCILE_INTERVAL_MS"),
            }
        }
    }

    fn a2c_r1_test_keys() -> crate::binance_futures::BinanceKeys {
        crate::binance_futures::BinanceKeys {
            api_key: "unused".into(),
            api_secret: "unused".into(),
        }
    }

    fn a2c_r1_test_limiter() -> CredentialRateLimiter {
        CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0))
    }

    /// 用交易所那一端**真的收到过多少条请求**来等对账轮次，不睡固定时长。
    async fn a2c_r1_wait_for_requests(
        client: &BinanceFuturesClient,
        at_least: usize,
        budget: Duration,
    ) -> usize {
        let deadline = std::time::Instant::now() + budget;
        loop {
            let seen = client.recorded_requests().len();
            if seen >= at_least || std::time::Instant::now() >= deadline {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A2c-R1 的核心缺陷：任务真的结束以后，注册表里的占位让后续
    /// `ensure_running` 直接返回，管理再也起不来。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_finished_reconcile_task_is_replaced_by_the_next_ensure_running() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        // 只留启动那一轮，周期 tick 不会混进请求计数里。
        let _interval = A2cR1IntervalEnv::set("60000");
        let (store, path) = test_store("a2c-r1-finished-replaced").await;
        let credential_id = "a2c-r1-finished";
        let client = a2c_r1_rest_client(credential_id, 2);
        let manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(client.clone());
        manager
            .ensure_running(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 4, Duration::from_secs(5)).await,
            4,
            "the first admission must really run one startup reconcile round"
        );

        manager.end_task_in_place_for_tests(credential_id).await;
        assert_eq!(
            manager.task_finished_for_tests(credential_id).await,
            Some(true),
            "the terminated task must be really finished before the next admission"
        );
        assert!(
            manager.is_running_for_tests(credential_id).await,
            "the finished task still holds its registry slot — that is the premise of A2c-R1"
        );

        manager
            .ensure_running(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 8, Duration::from_secs(5)).await,
            8,
            "a finished reconcile task must be replaced and really reconcile again"
        );
        assert_eq!(
            manager.task_finished_for_tests(credential_id).await,
            Some(false),
            "the replacement task must be alive, not another finished placeholder"
        );
        manager.abort_all_for_tests().await;
        cleanup(store, path).await;
    }

    /// 反向：还活着的任务绝不能被顶掉，也不能被重复 spawn。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_live_reconcile_task_is_never_replaced_by_repeated_ensure_running() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = A2cR1IntervalEnv::set("60000");
        let (store, path) = test_store("a2c-r1-live-kept").await;
        let credential_id = "a2c-r1-live";
        let client = a2c_r1_rest_client(credential_id, 3);
        let manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(client.clone());
        manager
            .ensure_running(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 4, Duration::from_secs(5)).await,
            4
        );

        manager
            .ensure_running(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        manager
            .ensure_running_after_stream_startup(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            client.recorded_requests().len(),
            4,
            "a live task must stay the same instance: no second startup round, no duplicate spawn"
        );
        assert_eq!(
            manager.task_finished_for_tests(credential_id).await,
            Some(false),
            "the original task must still be the live one"
        );
        assert_eq!(manager.inner.lock().await.len(), 1);
        manager.abort_all_for_tests().await;
        cleanup(store, path).await;
    }

    /// 同一个凭据的并发准入：至多一个存活任务。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_concurrent_ensure_running_admits_exactly_one_live_reconcile_task() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = A2cR1IntervalEnv::set("60000");
        let (store, path) = test_store("a2c-r1-concurrent").await;
        let credential_id = "a2c-r1-concurrent";
        let client = a2c_r1_rest_client(credential_id, 4);
        let manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(client.clone());
        let mut admissions = Vec::new();
        for _ in 0..8 {
            let manager = manager.clone();
            let store = store.clone();
            admissions.push(tokio::spawn(async move {
                manager
                    .ensure_running(
                        credential_id,
                        store,
                        a2c_r1_test_limiter(),
                        a2c_r1_test_keys(),
                    )
                    .await;
            }));
        }
        for admission in admissions {
            admission.await.expect("concurrent admission");
        }
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 4, Duration::from_secs(5)).await,
            4
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            client.recorded_requests().len(),
            4,
            "eight concurrent admissions must leave exactly one reconciling task"
        );
        assert_eq!(manager.inner.lock().await.len(), 1);
        manager.abort_all_for_tests().await;
        cleanup(store, path).await;
    }

    /// 恢复只作用于结束的那个凭据，另一个凭据的活任务不受影响。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_finished_task_recovery_stays_isolated_per_credential() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = A2cR1IntervalEnv::set("60000");
        let (store, path) = test_store("a2c-r1-isolation").await;
        let client = a2c_r1_rest_client("a2c-r1-isolation", 4);
        let manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(client.clone());
        for credential_id in ["a2c-r1-ended", "a2c-r1-kept"] {
            manager
                .ensure_running(
                    credential_id,
                    store.clone(),
                    a2c_r1_test_limiter(),
                    a2c_r1_test_keys(),
                )
                .await;
        }
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 8, Duration::from_secs(5)).await,
            8,
            "both credentials must run their own startup round"
        );

        manager.end_task_in_place_for_tests("a2c-r1-ended").await;
        manager
            .ensure_running(
                "a2c-r1-ended",
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 12, Duration::from_secs(5)).await,
            12,
            "the ended credential must be readmitted"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            client.recorded_requests().len(),
            12,
            "the untouched credential must not be restarted along with it"
        );
        assert_eq!(
            manager.task_finished_for_tests("a2c-r1-kept").await,
            Some(false),
            "the other credential keeps its original live task"
        );
        assert_eq!(manager.inner.lock().await.len(), 2);
        manager.abort_all_for_tests().await;
        cleanup(store, path).await;
    }

    /// 关掉的管理器仍然什么都不起——恢复分支不能绕过 `enabled`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_disabled_manager_starts_no_task_before_or_after_a_finished_one() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = A2cR1IntervalEnv::set("60000");
        let (store, path) = test_store("a2c-r1-disabled").await;
        let credential_id = "a2c-r1-disabled";
        let manager = super::ReconcilerManager::disabled_for_tests();
        manager
            .ensure_running(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        manager
            .ensure_running_after_stream_startup(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        assert!(!manager.is_running_for_tests(credential_id).await);
        assert_eq!(manager.task_finished_for_tests(credential_id).await, None);
        assert_eq!(
            manager.inner.lock().await.len(),
            0,
            "a disabled manager must never spawn a reconcile task"
        );
        cleanup(store, path).await;
    }

    /// D3：被重新拉起的 user-stream 所有权任务，仍然不抢那次首轮完整扫描。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a2c_r1_restarted_stream_owned_task_still_defers_the_initial_full_scan() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = A2cR1IntervalEnv::set("1000");
        let (store, path) = test_store("a2c-r1-stream-owner").await;
        let credential_id = "a2c-r1-stream-owned";
        let client = a2c_r1_rest_client(credential_id, 2);
        let manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(client.clone());
        manager
            .ensure_running_after_stream_startup(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        // 标准比较断言：固定变异 runner 只认 assert_eq!/标准 assertion failed 诊断。
        assert_eq!(
            client.recorded_requests().is_empty(),
            true,
            "startup fill recovery still owns the first full scan"
        );

        manager.end_task_in_place_for_tests(credential_id).await;
        manager
            .ensure_running_after_stream_startup(
                credential_id,
                store.clone(),
                a2c_r1_test_limiter(),
                a2c_r1_test_keys(),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(
            client.recorded_requests().is_empty(),
            true,
            "the readmitted stream-owned task must not grab the initial scan either"
        );
        assert_eq!(
            a2c_r1_wait_for_requests(&client, 4, Duration::from_secs(3)).await,
            4,
            "the readmitted task must still reconcile when its normal tick arrives"
        );
        manager.abort_all_for_tests().await;
        cleanup(store, path).await;
    }

    fn assert_a9_r1_rest_account_selection(rest: &BinanceFuturesClient, ws_requests: &AtomicUsize) {
        let requests = rest.recorded_requests();
        assert_eq!(
            requests.len(),
            4,
            "an unhealthy ws client must leave balance and position reads on REST: {requests:?}"
        );
        assert!(requests[2].contains("/fapi/v2/positionRisk"));
        assert!(requests[3].contains("/fapi/v2/balance"));
        assert_eq!(
            ws_requests.load(Ordering::SeqCst),
            0,
            "an unhealthy ws client must receive zero account-read requests"
        );
    }

    #[tokio::test]
    async fn p1_reconcile_selects_rest_and_never_calls_ws_while_ws_scope_is_cooling() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("a9-r1-p1-reconcile").await;
            let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
            let credential_id = format!("cred_{identity}");
            let scope = format!("binance-futures-ws:route:{identity}:venue:testnet");
            let selected_at_ms = now_ms();
            for (table, deadline_column) in [
                ("binance_ip_ban_cooldowns", "ban_until_ms"),
                (
                    "binance_rate_limit_cooldowns",
                    "cooldown_until_ms",
                ),
            ] {
                sqlx::query(&format!(
                    "INSERT INTO {table} \
                     (egress_scope, {deadline_column}, updated_at_ms) VALUES (?, ?, ?)"
                ))
                .bind(&scope)
                .bind(selected_at_ms + 60_000)
                .bind(selected_at_ms)
                .execute(store.pool())
                .await
                .expect("seed active ws-scope cooldown");
            }
            let ws_requests = Arc::new(AtomicUsize::new(0));
            let ws = crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_and_health_for_test(
                store.clone(),
                scope,
                serde_json::json!([position("BTCUSDT", "0", "BOTH")]),
                balances(),
                crate::binance_ws_api::WsApiHealth {
                    connected: true,
                    connected_at_ms: Some(selected_at_ms - 1_000),
                    last_request_success_at_ms: Some(selected_at_ms),
                    ..crate::binance_ws_api::WsApiHealth::default()
                },
                ws_requests.clone(),
            );
            assert!(
                !ws.is_healthy().await,
                "active ws-scope cooldowns must make is_healthy false before source selection"
            );
            let rest = a9_r1_rest_client(&credential_id);
            run_reconcile_round_with_ws(
                &ReconcileExchangeSource::Stub(rest.clone()),
                &credential_id,
                store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                crate::binance_futures::BinanceKeys {
                    api_key: format!("key_{identity}"),
                    api_secret: format!("secret_{identity}"),
                },
                Some(&ws),
            )
            .await
            .expect("cooling ws scope must reconcile through REST");
            assert_a9_r1_rest_account_selection(&rest, &ws_requests);
            cleanup(store, path).await;
        })
        .await
        .expect("P1 reconcile source selection must not hang");
    }

    #[tokio::test]
    async fn p3_disconnected_ws_is_unhealthy_and_reconcile_uses_only_rest() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("a9-r1-p3-reconcile").await;
            let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
            let credential_id = format!("cred_{identity}");
            let selected_at_ms = now_ms();
            let ws_requests = Arc::new(AtomicUsize::new(0));
            let ws = crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_and_health_for_test(
                store.clone(),
                format!("binance-futures-ws:route:{identity}:venue:mainnet"),
                serde_json::json!([position("BTCUSDT", "0", "BOTH")]),
                balances(),
                crate::binance_ws_api::WsApiHealth {
                    connected: false,
                    connected_at_ms: Some(selected_at_ms - 1_000),
                    last_request_success_at_ms: Some(selected_at_ms),
                    ..crate::binance_ws_api::WsApiHealth::default()
                },
                ws_requests.clone(),
            );
            assert!(
                !ws.is_healthy().await,
                "connected=false must make is_healthy false even when every other field is fresh"
            );
            let rest = a9_r1_rest_client(&credential_id);
            run_reconcile_round_with_ws(
                &ReconcileExchangeSource::Stub(rest.clone()),
                &credential_id,
                store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                crate::binance_futures::BinanceKeys {
                    api_key: format!("key_{identity}"),
                    api_secret: format!("secret_{identity}"),
                },
                Some(&ws),
            )
            .await
            .expect("disconnected ws client must reconcile through REST");
            assert_a9_r1_rest_account_selection(&rest, &ws_requests);
            cleanup(store, path).await;
        })
        .await
        .expect("P3 reconcile source selection must not hang");
    }

    async fn seed_a8_active_position(store: &Store, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, entry_price,
              opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long',
                      1, 'open', ?, '1', '100', 1, 1)
            "#,
        )
        .bind(format!("pos_{}", ulid::Ulid::new()))
        .bind(credential_id)
        .bind(format!("bot_{}", ulid::Ulid::new()))
        .bind(format!(
            "bot_a8|binance|futures|BTC|USDT|USDT|long|{}",
            ulid::Ulid::new()
        ))
        .execute(store.pool())
        .await
        .expect("seed A8 active position");
    }

    async fn seed_a8_r2_position(
        store: &Store,
        credential_id: &str,
        bot_uid: &str,
        position_id: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, entry_price,
              opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, 'long', 1, 'open', ?, '1', '100', 1, 1)
            "#,
        )
        .bind(position_id)
        .bind(credential_id)
        .bind(bot_uid)
        .bind(symbol_key)
        .bind(format!("{bot_uid}|{symbol_key}|long|{}", ulid::Ulid::new()))
        .execute(store.pool())
        .await
        .expect("seed A8-R2 position");
    }

    async fn seed_a8_r2_uncovered_projection_fixture(
        store: &Store,
        credential_id: &str,
        identity: &str,
    ) -> (String, String) {
        let bot_uid = format!("bot_{identity}");
        let position_a = format!("pos_a_{identity}");
        let position_b = format!("pos_b_{identity}");
        let inflight_order = format!("order_b_{identity}");
        let protection_engine_order = format!("protection_order_b_{identity}");
        let protection_order = format!("prot_b_{identity}");
        seed_a8_r2_position(
            store,
            credential_id,
            &bot_uid,
            &position_a,
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        seed_a8_r2_position(
            store,
            credential_id,
            &bot_uid,
            &position_b,
            "binance|futures|ETH|USDT|USDT",
        )
        .await;

        seed_inflight_order(
            store,
            credential_id,
            &inflight_order,
            "acked",
            "2002",
            "binance|futures|ETH|USDT|USDT",
        )
        .await;
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id, exchange, market_kind,
              symbol_key, action, side, order_type, quantity, reduce_only,
              status, position_id, order_role, created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, NULL, 'binance', 'futures',
              'binance|futures|ETH|USDT|USDT', 'close_long', 'long',
              'stop_market', '1', 1, 'confirmed', ?, 'protection_sl', 1, 1)
            "#,
        )
        .bind(&protection_engine_order)
        .bind(credential_id)
        .bind(&bot_uid)
        .bind(format!("idem_{protection_engine_order}"))
        .bind(format!("coid_{protection_engine_order}"))
        .bind(&position_b)
        .execute(store.pool())
        .await
        .expect("seed A8-R2 protection engine order");
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              engine_order_id, exchange_algo_id, client_algo_id, algo_status,
              algo_updated_at_ms, trigger_price, reduce_only, close_position,
              status, verified_from_exchange, last_verified_at_ms,
              created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'sl', 1, ?, '7002', ?, 'NEW', 1, '90', 1, 1,
              'confirmed', 1, 1, 1, 1)
            "#,
        )
        .bind(&protection_order)
        .bind(&position_b)
        .bind(credential_id)
        .bind(&protection_engine_order)
        .bind(format!("algo_{identity}"))
        .execute(store.pool())
        .await
        .expect("seed A8-R2 protection projection");
        (inflight_order, protection_order)
    }

    async fn a8_r2_engine_order_snapshot(store: &Store, engine_order_id: &str) -> String {
        sqlx::query(
            r#"
            SELECT json_object(
              'engine_order_id', engine_order_id, 'credential_id', credential_id,
              'bot_uid', bot_uid, 'run_id', run_id, 'cycle_id', cycle_id,
              'idempotency_key', idempotency_key,
              'client_order_id_short', client_order_id_short,
              'exchange_order_id', exchange_order_id, 'exchange', exchange,
              'market_kind', market_kind, 'symbol_key', symbol_key,
              'action', action, 'side', side, 'order_type', order_type,
              'price', price, 'quantity', quantity, 'reduce_only', reduce_only,
              'status', status, 'created_at_ms', created_at_ms,
              'updated_at_ms', updated_at_ms, 'risk_verdict_id', risk_verdict_id,
              'position_id', position_id, 'order_role', order_role
            ) AS snapshot
            FROM engine_orders WHERE engine_order_id = ?
            "#,
        )
        .bind(engine_order_id)
        .fetch_one(store.pool())
        .await
        .expect("snapshot A8-R2 engine order")
        .get("snapshot")
    }

    async fn a8_r2_protection_snapshot(store: &Store, protection_order_id: &str) -> String {
        sqlx::query(
            r#"
            SELECT json_object(
              'protection_order_id', protection_order_id, 'position_id', position_id,
              'credential_id', credential_id, 'kind', kind, 'generation', generation,
              'engine_order_id', engine_order_id, 'exchange_order_id', exchange_order_id,
              'trigger_price', trigger_price, 'reduce_only', reduce_only,
              'close_position', close_position, 'status', status,
              'verified_from_exchange', verified_from_exchange,
              'verified_at_ms', verified_at_ms, 'created_at_ms', created_at_ms,
              'updated_at_ms', updated_at_ms, 'exchange_algo_id', exchange_algo_id,
              'client_algo_id', client_algo_id, 'algo_status', algo_status,
              'algo_updated_at_ms', algo_updated_at_ms,
              'last_verified_at_ms', last_verified_at_ms
            ) AS snapshot
            FROM protection_orders WHERE protection_order_id = ?
            "#,
        )
        .bind(protection_order_id)
        .fetch_one(store.pool())
        .await
        .expect("snapshot A8-R2 protection order")
        .get("snapshot")
    }

    fn a8_r2_projection_client(credential_id: &str, identity: &str) -> BinanceFuturesClient {
        BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([
                        position("BTCUSDT", "1", "BOTH"),
                        position("ETHUSDT", "1", "BOTH")
                    ])
                    .to_string(),
                ),
                (200, balances().to_string()),
                (
                    200,
                    serde_json::json!({
                        "algoId": 7002,
                        "clientAlgoId": format!("algo_{identity}"),
                        "algoStatus": "CANCELED",
                        "actualOrderId": "",
                        "updateTime": 20
                    })
                    .to_string(),
                ),
                (
                    200,
                    serde_json::json!({
                        "orderId": 2002,
                        "clientOrderId": format!("coid-order_b_{identity}"),
                        "symbol": "ETHUSDT",
                        "status": "CANCELED",
                        "updateTime": 20
                    })
                    .to_string(),
                ),
            ],
        )
    }

    fn assert_narrowed_request_shape(requests: &[String], symbols: &[&str]) {
        assert_eq!(
            requests.len(),
            symbols.len() * 2 + 2,
            "two scoped order reads per symbol plus one positionRisk and balance: {requests:?}"
        );
        for (endpoint, expected_count) in [
            ("/fapi/v1/openOrders", symbols.len()),
            ("/fapi/v1/openAlgoOrders", symbols.len()),
            ("/fapi/v2/positionRisk", 1),
            ("/fapi/v2/balance", 1),
        ] {
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.contains(endpoint))
                    .count(),
                expected_count,
                "unexpected {endpoint} count: {requests:?}"
            );
        }
        for symbol in symbols {
            for endpoint in ["/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"] {
                assert!(
                    requests.iter().any(|request| {
                        request.contains(endpoint) && request.contains(&format!("symbol={symbol}"))
                    }),
                    "missing scoped {endpoint} read for {symbol}: {requests:?}"
                );
            }
        }
        let position_risk = requests
            .iter()
            .find(|request| request.contains("/fapi/v2/positionRisk"))
            .expect("the single account-wide positionRisk read");
        assert!(
            !position_risk.contains("symbol="),
            "positionRisk must be account-wide: {position_risk}"
        );
    }

    #[test]
    fn a8_i1_tier_three_way_stagger_is_stable_and_not_ready_falls_back_full() {
        let interval = 600_000;
        let alpha = credential_stagger_ms("a8-credential-alpha", interval);
        let beta = credential_stagger_ms("a8-credential-beta", interval);
        assert_eq!(alpha, 180_633, "frozen SHA-256 prefix vector");
        assert_eq!(beta, 278_501, "frozen SHA-256 prefix vector");
        assert_eq!(
            alpha,
            credential_stagger_ms("a8-credential-alpha", interval),
            "the same credential must keep the same stagger"
        );
        assert_ne!(
            alpha, beta,
            "two frozen credential fixtures must be staggered"
        );

        let active = vec!["BTCUSDT".to_string()];
        assert_eq!(
            reconcile_tier("a8-credential-alpha", &active, 10, 0, interval, interval),
            ReconcileTier::SymbolNarrowed { symbols: active }
        );
        assert_eq!(
            reconcile_tier("a8-credential-alpha", &[], 10, 0, interval, interval),
            ReconcileTier::StreamOnly
        );
        assert_eq!(
            reconcile_tier_with_safety_fallbacks(
                "a8-credential-alpha",
                &[],
                false,
                false,
                10,
                0,
                interval,
            ),
            ReconcileTier::FullScan,
            "unknown stream readiness must never be treated as a healthy idle account"
        );
    }

    #[test]
    fn a8_i2_due_full_scan_wins_over_active_symbol_narrowing() {
        let credential = "a8-due-priority"; // gitleaks:allow -- Synthetic test credential identifier.
        let interval = 120_000;
        let due_at = interval + credential_stagger_ms(credential, interval);
        assert_eq!(
            reconcile_tier(
                credential,
                &["BTCUSDT".to_string()],
                due_at as i64,
                0,
                interval,
                interval,
            ),
            ReconcileTier::FullScan
        );
    }

    #[test]
    fn a8_r2_full_scan_due_boundary_includes_per_credential_stagger() {
        let interval = 120_000;
        let last_full_scan_ms = 1_700_000_000_000_i64;
        let first = format!("cred_{}", ulid::Ulid::new());
        let mut second = format!("cred_{}", ulid::Ulid::new());
        while credential_stagger_ms(&first, interval) == credential_stagger_ms(&second, interval) {
            second = format!("cred_{}", ulid::Ulid::new());
        }
        let active = ["BTCUSDT".to_string()];

        for credential in [&first, &second] {
            let stagger = credential_stagger_ms(credential, interval);
            let due_at = last_full_scan_ms
                .saturating_add(interval as i64)
                .saturating_add(stagger as i64);
            assert_eq!(
                reconcile_tier(
                    credential,
                    &active,
                    due_at - 1,
                    last_full_scan_ms,
                    interval,
                    interval,
                ),
                ReconcileTier::SymbolNarrowed {
                    symbols: active.to_vec()
                },
                "an active credential must remain narrowed one millisecond before interval + stagger"
            );
            assert_eq!(
                reconcile_tier(
                    credential,
                    &[],
                    due_at - 1,
                    last_full_scan_ms,
                    interval,
                    interval,
                ),
                ReconcileTier::StreamOnly,
                "an idle credential must remain stream-only one millisecond before interval + stagger"
            );
            for symbols in [&active[..], &[][..]] {
                assert_eq!(
                    reconcile_tier(
                        credential,
                        symbols,
                        due_at,
                        last_full_scan_ms,
                        interval,
                        interval,
                    ),
                    ReconcileTier::FullScan,
                    "FullScan must begin exactly at interval + this credential's stagger"
                );
            }
        }
        assert_ne!(
            credential_stagger_ms(&first, interval),
            credential_stagger_ms(&second, interval),
            "the two unique credential fixtures must exercise distinct stagger boundaries"
        );
    }

    #[tokio::test]
    async fn a8_i3_symbol_round_uses_only_scoped_order_reads_and_exact_base_weight() {
        let (store, path) = test_store("a8-i3-symbol").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        seed_a8_active_position(&store, &credential).await;
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                ),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0));
        let covered = reconcile_binance_futures_truth_with_client_and_coverage(
            &credential,
            store.clone(),
            limiter,
            &client,
            Coverage::Symbols(vec!["BTCUSDT".to_string()]),
        )
        .await
        .expect("symbol-narrowed reconcile");

        assert_eq!(
            covered.coverage,
            Coverage::Symbols(vec!["BTCUSDT".to_string()])
        );
        let requests = client.recorded_requests();
        assert_narrowed_request_shape(&requests, &["BTCUSDT"]);
        assert_eq!(
            crate::binance_futures::symbol_narrowed_reconcile_round_weight(1),
            22.0
        );
        assert_eq!(
            crate::binance_futures::symbol_narrowed_reconcile_round_weight(2),
            24.0
        );
        assert_eq!(
            crate::binance_futures::symbol_narrowed_reconcile_round_weight(5),
            30.0
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_r1_three_symbol_round_reads_position_risk_once_without_symbol() {
        let (store, path) = test_store("a8-r1-three-symbol-shape").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let covered = reconcile_binance_futures_truth_with_client_and_coverage(
            &credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &client,
            Coverage::Symbols(symbols.clone()),
        )
        .await
        .expect("three-symbol narrowed reconcile");

        assert_eq!(covered.coverage, Coverage::Symbols(symbols));
        assert_narrowed_request_shape(
            &client.recorded_requests(),
            &["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_i3_periodic_entry_selects_the_symbol_narrowed_request_shape() {
        let (store, path) = test_store("a8-i3-periodic-entry").await;
        let credential = "cred_a8_i3_periodic";
        seed_a8_active_position(&store, credential).await;
        persist_last_full_scan_ms(&store, credential, now_ms())
            .await
            .expect("seed recent full scan");
        let client = BinanceFuturesClient::new_with_mock_responses(
            credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                ),
                (200, balances().to_string()),
            ],
        );
        run_reconcile_round(
            &ReconcileExchangeSource::Stub(client.clone()),
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            crate::binance_futures::BinanceKeys {
                api_key: "unused".to_string(),
                api_secret: "unused".to_string(),
            },
        )
        .await
        .expect("periodic symbol-narrowed round");

        let requests = client.recorded_requests();
        assert_narrowed_request_shape(&requests, &["BTCUSDT"]);
        cleanup(store, path).await;
    }

    async fn seed_a8_r1_position_fixture(store: &Store, credential_id: &str, identity: &str) {
        for (base, state, qty) in [
            ("BTC", "open", "1"),
            ("ETH", "open", "2"),
            ("SOL", "open", "3"),
            ("XRP", "closed", "0"),
        ] {
            let symbol_key = format!("binance|futures|{base}|USDT|USDT");
            let bot_uid = format!("bot_{identity}_{base}");
            let active_key = (state != "closed").then(|| format!("{bot_uid}|{symbol_key}|long"));
            sqlx::query(
                r#"
                INSERT INTO position_state (
                  position_id, credential_id, bot_uid, symbol_key, direction,
                  entry_epoch, state, active_key, qty, entry_price,
                  opened_at_ms, closed_at_ms, last_reconciled_ms, updated_at_ms,
                  exit_epoch, pre_exit_state
                ) VALUES (?, ?, ?, ?, 'long', 1, ?, ?, ?, '100',
                          10, ?, 20, 20, 0, NULL)
                "#,
            )
            .bind(format!("pos_{identity}_{base}"))
            .bind(credential_id)
            .bind(bot_uid)
            .bind(symbol_key)
            .bind(state)
            .bind(active_key)
            .bind(qty)
            .bind((state == "closed").then_some(20_i64))
            .execute(store.pool())
            .await
            .expect("seed A8-R1 position fixture");
        }
    }

    async fn a8_r1_table_snapshot(store: &Store, table: &str) -> Vec<String> {
        let (columns, query) = match table {
            "position_state" => (
                vec![
                    "position_id",
                    "credential_id",
                    "bot_uid",
                    "symbol_key",
                    "direction",
                    "entry_epoch",
                    "state",
                    "active_key",
                    "pending_intent_id",
                    "pending_idem_key",
                    "qty",
                    "entry_price",
                    "opened_at_ms",
                    "closed_at_ms",
                    "last_reconciled_ms",
                    "updated_at_ms",
                    "exit_epoch",
                    "pre_exit_state",
                    "close_revision",
                ],
                "SELECT json_object(\
                   'position_id', position_id, 'credential_id', credential_id, \
                   'bot_uid', bot_uid, 'symbol_key', symbol_key, 'direction', direction, \
                   'entry_epoch', entry_epoch, 'state', state, 'active_key', active_key, \
                   'pending_intent_id', pending_intent_id, \
                   'pending_idem_key', pending_idem_key, 'qty', qty, \
                   'entry_price', entry_price, 'opened_at_ms', opened_at_ms, \
                   'closed_at_ms', closed_at_ms, 'last_reconciled_ms', last_reconciled_ms, \
                   'updated_at_ms', updated_at_ms, 'exit_epoch', exit_epoch, \
                   'pre_exit_state', pre_exit_state, 'close_revision', close_revision) AS row_json \
                 FROM position_state ORDER BY position_id",
            ),
            "engine_balances" => (
                vec![
                    "credential_id",
                    "exchange",
                    "market_kind",
                    "asset",
                    "wallet_balance",
                    "available_balance",
                    "updated_at_ms",
                    "raw_json",
                ],
                "SELECT json_object(\
                   'credential_id', credential_id, 'exchange', exchange, \
                   'market_kind', market_kind, 'asset', asset, \
                   'wallet_balance', wallet_balance, 'available_balance', available_balance, \
                   'updated_at_ms', updated_at_ms, 'raw_json_hex', hex(raw_json)) AS row_json \
                 FROM engine_balances ORDER BY asset",
            ),
            _ => panic!("unsupported A8-R1 snapshot table: {table}"),
        };
        let actual_columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(store.pool())
            .await
            .expect("read snapshot table columns")
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        assert_eq!(
            actual_columns, columns,
            "the field-for-field snapshot must cover every {table} column"
        );
        sqlx::query(query)
            .fetch_all(store.pool())
            .await
            .expect("read field-for-field snapshot")
            .into_iter()
            .map(|row| row.get::<String, _>("row_json"))
            .collect()
    }

    #[tokio::test]
    async fn a8_r1_narrowed_position_and_balance_rows_match_full_scan_field_for_field() {
        let (full_store, full_path) = test_store("a8-r1-full-persistence").await;
        let (narrow_store, narrow_path) = test_store("a8-r1-narrow-persistence").await;
        let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
        let credential = format!("cred_{identity}");
        seed_a8_r1_position_fixture(&full_store, &credential, &identity).await;
        seed_a8_r1_position_fixture(&narrow_store, &credential, &identity).await;
        let positions = serde_json::json!([
            position("BTCUSDT", "1", "BOTH"),
            position("ETHUSDT", "2", "BOTH"),
            position("SOLUSDT", "3", "BOTH"),
            position("XRPUSDT", "4", "BOTH")
        ]);
        let fixed_observation_ms = 1_725_000_000_123_i64;

        let full_client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, positions.to_string()),
                (200, balances().to_string()),
            ],
        );
        let full = reconcile_binance_futures_truth_with_client_and_coverage_at(
            &credential,
            full_store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &full_client,
            Coverage::Full,
            || fixed_observation_ms,
        )
        .await
        .expect("full-scan fixture reconcile");

        let narrowed_client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, positions.to_string()),
                (200, balances().to_string()),
            ],
        );
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        let narrowed = reconcile_binance_futures_truth_with_client_and_coverage_at(
            &credential,
            narrow_store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &narrowed_client,
            Coverage::Symbols(symbols.clone()),
            || fixed_observation_ms,
        )
        .await
        .expect("symbol-narrowed fixture reconcile");

        assert_eq!(full.coverage, Coverage::Full);
        assert_eq!(narrowed.coverage, Coverage::Symbols(symbols));
        assert!(!narrowed.flatten_complete());
        assert_eq!(
            a8_r1_table_snapshot(&narrow_store, "position_state").await,
            a8_r1_table_snapshot(&full_store, "position_state").await,
            "account-wide positionRisk must make narrowed position_state identical to FullScan"
        );
        assert_eq!(
            a8_r1_table_snapshot(&narrow_store, "engine_balances").await,
            a8_r1_table_snapshot(&full_store, "engine_balances").await,
            "the once-per-round balance projection must remain identical to FullScan"
        );
        assert_narrowed_request_shape(
            &narrowed_client.recorded_requests(),
            &["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        );

        cleanup(full_store, full_path).await;
        cleanup(narrow_store, narrow_path).await;
    }

    #[tokio::test]
    async fn a9_i4_ws_failure_falls_back_in_the_same_round_and_persists_rest_identical_rows() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (rest_store, rest_path) = test_store("a9-i4-rest").await;
            let (fallback_store, fallback_path) = test_store("a9-i4-fallback").await;
            let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
            let credential = format!("cred_{identity}");
            seed_a8_r1_position_fixture(&rest_store, &credential, &identity).await;
            seed_a8_r1_position_fixture(&fallback_store, &credential, &identity).await;
            let positions = serde_json::json!([
                position("BTCUSDT", "1", "BOTH"),
                position("ETHUSDT", "2", "BOTH"),
                position("SOLUSDT", "3", "BOTH"),
                position("XRPUSDT", "4", "BOTH")
            ]);
            let fixed_observation_ms = 1_725_000_000_123_i64;
            let rest_client = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![
                    (200, "[]".to_string()),
                    (200, "[]".to_string()),
                    (200, positions.to_string()),
                    (200, balances().to_string()),
                ],
            );
            reconcile_binance_futures_truth_with_client_and_coverage_at(
                &credential,
                rest_store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                &rest_client,
                Coverage::Full,
                || fixed_observation_ms,
            )
            .await
            .expect("pure REST reference round");

            let fallback_client = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![
                    (200, "[]".to_string()),
                    (200, "[]".to_string()),
                    (200, positions.to_string()),
                    (200, balances().to_string()),
                ],
            );
            let ws =
                crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_for_test(
                    fallback_store.clone(),
                    format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
                    Err("injected disconnect".to_string()),
                    Err("injected disconnect".to_string()),
                );
            reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
                &credential,
                fallback_store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                &fallback_client,
                Coverage::Full,
                Some(&ws),
                || fixed_observation_ms,
            )
            .await
            .expect("ws failure must complete through REST in the same round");

            let rest_positions = a8_r1_table_snapshot(&rest_store, "position_state").await;
            let fallback_positions = a8_r1_table_snapshot(&fallback_store, "position_state").await;
            let rest_balances = a8_r1_table_snapshot(&rest_store, "engine_balances").await;
            let fallback_balances = a8_r1_table_snapshot(&fallback_store, "engine_balances").await;
            println!("I4 position_state rows={fallback_positions:?}");
            println!("I4 engine_balances rows={fallback_balances:?}");
            assert_eq!(fallback_positions, rest_positions);
            assert_eq!(fallback_balances, rest_balances);
            assert_eq!(
                fallback_client
                    .recorded_requests()
                    .iter()
                    .map(|request| request.split('?').next().unwrap_or(request))
                    .collect::<Vec<_>>(),
                rest_client
                    .recorded_requests()
                    .iter()
                    .map(|request| request.split('?').next().unwrap_or(request))
                    .collect::<Vec<_>>(),
                "fallback must execute the exact baseline REST endpoint sequence"
            );
            cleanup(rest_store, rest_path).await;
            cleanup(fallback_store, fallback_path).await;
        })
        .await
        .expect("I4 must turn red rather than hang");
    }

    #[tokio::test]
    async fn a9_i4_timeout_disconnect_and_error_code_all_take_the_one_rest_fallback_seam() {
        tokio::time::timeout(Duration::from_secs(30), async {
            for reason in [
                "ws-fapi request exceeded 5000ms deadline",
                "ws-fapi connection ended",
                "ws-fapi status=429 code=Some(-1003)",
            ] {
                let (store, path) = test_store("a9-i4-failure-kinds").await;
                let rest = BinanceFuturesClient::new_with_mock_responses(
                    "cred-a9-failures",
                    vec![
                        (
                            200,
                            serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                        ),
                        (200, balances().to_string()),
                    ],
                );
                let ws =
                    crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_for_test(
                        store.clone(),
                        format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
                        Err(reason.to_string()),
                        Err(reason.to_string()),
                    );
                let outcome =
                    read_account_truth(AccountReadSource::WsApi(&ws), &rest, &Coverage::Full)
                        .await
                        .expect("REST fallback");
                assert_eq!(outcome.source, "rest");
                assert!(outcome
                    .fallback_reason
                    .as_deref()
                    .is_some_and(|actual| actual.contains(reason)));
                assert_eq!(rest.recorded_requests().len(), 2);
                cleanup(store, path).await;
            }
        })
        .await
        .expect("all fallback kinds must turn red rather than hang");
    }

    #[test]
    fn a9_observability_log_keys_are_pinned_at_the_account_read_seam() {
        let source = include_str!("reconcile.rs");
        for key in [
            "source = account.source",
            "ws_scope = ws.scope()",
            "used_weight_1m",
            "limit = ?health.limit_1m",
            "connection_age_ms",
            "reconnect_count",
            "fallback_reason",
        ] {
            assert!(source.contains(key), "missing structured log key: {key}");
        }
    }

    #[tokio::test]
    async fn a9_i7_healthy_ws_round_leaves_rest_account_reads_unspent() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("a9-i7-ws").await;
            let credential = format!("cred_{}", ulid::Ulid::new());
            let rest_client = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![(200, "[]".to_string()), (200, "[]".to_string())],
            );
            let ws =
                crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_for_test(
                    store.clone(),
                    format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
                    Ok(serde_json::json!([position("BTCUSDT", "0", "BOTH")])),
                    Ok(balances()),
                );
            reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
                &credential,
                store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                &rest_client,
                Coverage::Full,
                Some(&ws),
                || 1_725_000_000_123_i64,
            )
            .await
            .expect("healthy ws reconcile");
            let requests = rest_client.recorded_requests();
            assert_eq!(
                requests.len(),
                2,
                "only the two order truth reads remain on REST: {requests:?}"
            );
            assert!(requests[0].contains("/fapi/v1/openOrders"));
            assert!(requests[1].contains("/fapi/v1/openAlgoOrders"));
            cleanup(store, path).await;
        })
        .await
        .expect("I7 must turn red rather than hang");
    }

    #[tokio::test]
    async fn a9_i10_default_disabled_round_keeps_the_baseline_rest_sequence_byte_for_byte() {
        let _guard = crate::binance_futures::tests::env_serial_guard();
        std::env::remove_var("RUSTCORE_BINANCE_WS_API_ENABLED");
        assert!(!crate::binance_ws_api::ws_api_enabled());
        let (store, path) = test_store("a9-i10-disabled").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                ),
                (200, balances().to_string()),
            ],
        );
        reconcile_binance_futures_truth_with_client_and_coverage(
            &credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &client,
            Coverage::Full,
        )
        .await
        .expect("default REST reconcile");
        let endpoints = client
            .recorded_requests()
            .into_iter()
            .map(|request| request.split('?').next().unwrap_or(&request).to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            endpoints,
            vec![
                "GET https://local.stub.invalid/fapi/v1/openOrders",
                "GET https://local.stub.invalid/fapi/v1/openAlgoOrders",
                "GET https://local.stub.invalid/fapi/v2/positionRisk",
                "GET https://local.stub.invalid/fapi/v2/balance",
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_i4_stream_only_production_round_has_zero_outbound_requests() {
        let (store, path) = test_store("a8-i4-stream-only").await;
        let credential = "cred_a8_i4";
        let now = now_ms();
        persist_last_full_scan_ms(&store, credential, now)
            .await
            .expect("seed recent full scan");
        sqlx::query(
            "INSERT INTO fill_stream_state \
             (credential_id, generation, ready, phase, updated_at_ms) \
             VALUES (?, 1, 1, 'ready', ?)",
        )
        .bind(credential)
        .bind(now)
        .execute(store.pool())
        .await
        .expect("seed ready user stream");
        let client = BinanceFuturesClient::new_with_mock_responses(credential, Vec::new());
        run_reconcile_round(
            &ReconcileExchangeSource::Stub(client.clone()),
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            crate::binance_futures::BinanceKeys {
                api_key: "unused".to_string(),
                api_secret: "unused".to_string(),
            },
        )
        .await
        .expect("stream-only periodic round");
        assert!(client.recorded_requests().is_empty());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn i5_periodic_truth_handoff_is_fresh_full_and_store_scoped() {
        let (store, path) = test_store("periodic-truth-handoff").await;
        let (other_store, other_path) = test_store("periodic-truth-other-store").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        let dirty = crate::kill_switch::ReconcileTruth {
            has_diff: true,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        remember_latest_periodic_reconcile_truth(
            &store,
            &credential,
            100,
            CoveredReconcileTruth {
                truth: dirty,
                coverage: Coverage::Full,
            },
        );
        assert!(
            latest_full_periodic_reconcile_truth_since(&store, &credential, 101).is_none(),
            "a truth older than the recovery cycle must not be reused"
        );
        assert!(
            latest_full_periodic_reconcile_truth_since(&other_store, &credential, 1).is_none(),
            "the same credential in another store must not see this truth"
        );

        let clean = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        remember_latest_periodic_reconcile_truth(
            &store,
            &credential,
            110,
            CoveredReconcileTruth {
                truth: clean,
                coverage: Coverage::Symbols(vec!["BTCUSDT".to_string()]),
            },
        );
        assert!(
            latest_full_periodic_reconcile_truth_since(&store, &credential, 101).is_none(),
            "symbol-narrowed truth must not reopen an account-wide recovery gate"
        );
        remember_latest_periodic_reconcile_truth(
            &store,
            &credential,
            120,
            CoveredReconcileTruth {
                truth: clean,
                coverage: Coverage::Full,
            },
        );
        let published = latest_full_periodic_reconcile_truth_since(&store, &credential, 101)
            .expect("fresh full truth");
        assert!(!published.has_diff);
        assert!(published.local_position_zero_diff);
        assert!(!published.all_positions_zero);
        assert!(!published.all_open_orders_empty);
        assert!(!published.all_open_algo_orders_empty);
        cleanup(store, path).await;
        cleanup(other_store, other_path).await;
    }

    #[tokio::test]
    async fn i5_production_periodic_full_scan_publishes_the_recheck_truth() {
        let (store, path) = test_store("periodic-full-truth-wiring").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let before = now_ms();
        run_reconcile_round(
            &ReconcileExchangeSource::Stub(client),
            &credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            crate::binance_futures::BinanceKeys {
                api_key: "unused".to_string(),
                api_secret: "unused".to_string(),
            },
        )
        .await
        .expect("production periodic FullScan");
        let truth = latest_full_periodic_reconcile_truth_since(&store, &credential, before)
            .expect("production periodic entry must publish its full truth");
        assert_eq!(recovery_reconcile_divergence(truth), None);
        cleanup(store, path).await;
    }

    #[test]
    fn a8_i5_symbol_coverage_never_satisfies_flatten_and_l3_forces_full() {
        let clean = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        };
        assert!(!CoveredReconcileTruth {
            truth: clean,
            coverage: Coverage::Symbols(vec!["BTCUSDT".to_string()]),
        }
        .flatten_complete());
        assert_eq!(
            reconcile_tier_with_safety_fallbacks(
                "cred_a8_i5",
                &["BTCUSDT".to_string()],
                true,
                true,
                1,
                1,
                600_000,
            ),
            ReconcileTier::FullScan
        );
    }

    #[tokio::test]
    async fn a8_i5_persisted_credential_l3_latch_forces_the_periodic_full_path() {
        let (store, path) = test_store("a8-i5-l3-wiring").await;
        let credential = "cred_a8_i5_wiring";
        let now = now_ms();
        seed_a8_active_position(&store, credential).await;
        persist_last_full_scan_ms(&store, credential, now)
            .await
            .expect("seed recent full scan");
        crate::kill_switch::latch_flatten_pending(
            &store,
            &format!("cred:{credential}"),
            "manual_api",
            "A8 I5 production latch wiring",
            now,
        )
        .await
        .expect("persist the L3 latch through the production writer");

        let client = BinanceFuturesClient::new_with_mock_responses(
            credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        run_reconcile_round(
            &ReconcileExchangeSource::Stub(client.clone()),
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            crate::binance_futures::BinanceKeys {
                api_key: "unused".to_string(),
                api_secret: "unused".to_string(),
            },
        )
        .await
        .expect("drive the persisted L3 latch through the periodic production entry");
        let requests = client.recorded_requests();
        assert_eq!(requests.len(), 4, "full truth request set: {requests:?}");
        for endpoint in [
            "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders",
            "/fapi/v2/positionRisk",
        ] {
            let request = requests
                .iter()
                .find(|request| request.contains(endpoint))
                .unwrap_or_else(|| panic!("missing {endpoint}: {requests:?}"));
            assert!(
                !request.contains("symbol="),
                "L3 must use full rather than narrowed truth: {request}"
            );
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_i5_l3_is_checked_before_invalid_active_symbol_parsing() {
        let (store, path) = test_store("a8-i5-l3-before-symbol-parse").await;
        let credential = "cred_a8_i5_invalid_symbol";
        let now = now_ms();
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, entry_price,
              opened_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, 'okx|spot|BTC|USDT|BTC', 'long',
                      1, 'open', ?, '1', '100', 1, 1)
            "#,
        )
        .bind(format!("pos_{}", ulid::Ulid::new()))
        .bind(credential)
        .bind(format!("bot_{}", ulid::Ulid::new()))
        .bind(format!("a8_invalid_symbol_{}", ulid::Ulid::new()))
        .execute(store.pool())
        .await
        .expect("seed an active position outside the Binance USD-M symbol domain");
        persist_last_full_scan_ms(&store, credential, now)
            .await
            .expect("seed recent full scan");
        crate::kill_switch::latch_flatten_pending(
            &store,
            &format!("cred:{credential}"),
            "manual_api",
            "A8 I5 L3 precedes symbol parsing",
            now,
        )
        .await
        .expect("persist credential L3 latch");

        let (tier, _, _) = selected_periodic_tier(&store, credential, now)
            .await
            .expect("L3 must select a full scan without parsing active symbols");
        assert_eq!(tier, ReconcileTier::FullScan);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_i5_engine_order_only_bot_l3_membership_forces_full_scan() {
        let (store, path) = test_store("a8-i5-order-only-bot-l3").await;
        let credential = "cred_a8_i5_order_only";
        let bot_uid = "bot_a8_i5_order_only";
        let now = now_ms();
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              ?, ?, ?, ?, ?, NULL,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '1', 1, 'canceled', 'exit', 1, 1
            )
            "#,
        )
        .bind(format!("order_{}", ulid::Ulid::new()))
        .bind(credential)
        .bind(bot_uid)
        .bind(format!("idem_{}", ulid::Ulid::new()))
        .bind(format!("coid_{}", ulid::Ulid::new()))
        .execute(store.pool())
        .await
        .expect("seed historical engine-order-only bot membership");
        persist_last_full_scan_ms(&store, credential, now)
            .await
            .expect("seed recent full scan");
        sqlx::query(
            "INSERT INTO fill_stream_state \
             (credential_id, generation, ready, phase, updated_at_ms) \
             VALUES (?, 1, 1, 'ready', ?)",
        )
        .bind(credential)
        .bind(now)
        .execute(store.pool())
        .await
        .expect("seed ready user stream so a missed L3 would select StreamOnly");
        crate::kill_switch::latch_flatten_pending(
            &store,
            &format!("bot:{bot_uid}"),
            "manual_api",
            "A8 I5 engine-order-only bot membership",
            now,
        )
        .await
        .expect("persist bot L3 latch");

        let (tier, _, _) = selected_periodic_tier(&store, credential, now)
            .await
            .expect("select tier using the authoritative bot membership union");
        assert_eq!(tier, ReconcileTier::FullScan);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_i5_clean_symbol_projection_cannot_clear_prior_full_scan_diff() {
        let (store, path) = test_store("a8-i5-partial-preserves-diff").await;
        let credential = "cred_a8_i5_partial";
        super::apply_reconcile_diff_open(&store, credential, 1, true)
            .await
            .expect("seed a full-scan difference");
        let client = BinanceFuturesClient::new_with_mock_responses(
            credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    200,
                    serde_json::json!([position("BTCUSDT", "0", "BOTH")]).to_string(),
                ),
                (200, balances().to_string()),
            ],
        );
        reconcile_binance_futures_truth_with_client_and_coverage(
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &client,
            Coverage::Symbols(vec!["BTCUSDT".to_string()]),
        )
        .await
        .expect("clean narrowed projection");

        let open: i64 = sqlx::query(
            "SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id = ?",
        )
        .bind(credential)
        .fetch_one(store.pool())
        .await
        .expect("read preserved full-scan difference")
        .get("reconcile_diff_open");
        assert_eq!(
            open, 1,
            "unknown symbols cannot be collapsed into account-wide emptiness"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a8_r2_symbol_coverage_never_records_l3_convergence_projection() {
        async fn run(coverage: Coverage) -> (i64, Store, std::path::PathBuf) {
            let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
            let credential = format!("cred_{identity}");
            let (store, path) = test_store(&format!("a8-r2-l3-{identity}")).await;
            seed_a8_r2_position(
                &store,
                &credential,
                &format!("bot_{identity}"),
                &format!("pos_{identity}"),
                "binance|futures|BTC|USDT|USDT",
            )
            .await;
            for scope_key in ["global".to_string(), format!("cred:{credential}")] {
                crate::kill_switch::latch_flatten_pending(
                    &store,
                    &scope_key,
                    "manual_api",
                    "A8-R2 narrowed L3 projection guard",
                    10,
                )
                .await
                .expect("seed active L3 latch");
            }
            let client = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![
                    (200, "[]".to_string()),
                    (200, "[]".to_string()),
                    (
                        200,
                        serde_json::json!([position("BTCUSDT", "1", "BOTH")]).to_string(),
                    ),
                    (200, balances().to_string()),
                ],
            );
            tokio::time::timeout(
                Duration::from_secs(5),
                reconcile_binance_futures_truth_with_client_and_coverage_at(
                    &credential,
                    store.clone(),
                    CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                    &client,
                    coverage,
                    || 20,
                ),
            )
            .await
            .expect("A8-R2 L3 projection fixture exceeded its outer timeout")
            .expect("reconcile the A8-R2 L3 projection fixture");
            let recorded: i64 = sqlx::query(
                "SELECT COUNT(*) AS count FROM kill_switch_reconcile_status \
                 WHERE credential_id = ? AND scope_key IN ('global', ?) \
                   AND CAST(reconciled_at_ms AS CHAR) = '20'",
            )
            .bind(&credential)
            .bind(format!("cred:{credential}"))
            .fetch_one(store.pool())
            .await
            .expect("count A8-R2 kill-switch reconcile projections")
            .get("count");
            (recorded, store, path)
        }

        let (narrowed_records, narrowed_store, narrowed_path) =
            run(Coverage::Symbols(vec!["BTCUSDT".to_string()])).await;
        assert_eq!(
            narrowed_records, 0,
            "Coverage::Symbols must not call record_reconcile_for_scope for active L3 latches"
        );
        cleanup(narrowed_store, narrowed_path).await;

        let (full_records, full_store, full_path) = run(Coverage::Full).await;
        assert_eq!(
            full_records, 2,
            "the identical Full coverage fixture must record global and credential L3 truth"
        );
        cleanup(full_store, full_path).await;
    }

    #[tokio::test]
    async fn a8_r2_narrowed_truth_preserves_uncovered_order_and_protection_rows() {
        async fn run(
            coverage: Coverage,
        ) -> (
            (String, String),
            (String, String),
            Vec<String>,
            Store,
            std::path::PathBuf,
        ) {
            let identity = ulid::Ulid::new().to_string().to_ascii_lowercase();
            let credential = format!("cred_{identity}");
            let (store, path) = test_store(&format!("a8-r2-uncovered-{identity}")).await;
            let (inflight_order, protection_order) =
                seed_a8_r2_uncovered_projection_fixture(&store, &credential, &identity).await;
            let before = (
                a8_r2_engine_order_snapshot(&store, &inflight_order).await,
                a8_r2_protection_snapshot(&store, &protection_order).await,
            );
            let client = a8_r2_projection_client(&credential, &identity);
            tokio::time::timeout(
                Duration::from_secs(5),
                reconcile_binance_futures_truth_with_client_and_coverage_at(
                    &credential,
                    store.clone(),
                    CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                    &client,
                    coverage,
                    || 20,
                ),
            )
            .await
            .expect("A8-R2 uncovered-symbol fixture exceeded its outer timeout")
            .expect("reconcile the A8-R2 uncovered-symbol fixture");
            let after = (
                a8_r2_engine_order_snapshot(&store, &inflight_order).await,
                a8_r2_protection_snapshot(&store, &protection_order).await,
            );
            (before, after, client.recorded_requests(), store, path)
        }

        let (narrowed_before, narrowed_after, narrowed_requests, narrowed_store, narrowed_path) =
            run(Coverage::Symbols(vec!["BTCUSDT".to_string()])).await;
        assert_eq!(
            narrowed_after, narrowed_before,
            "uncovered inflight and protection rows must remain field-for-field unchanged"
        );
        assert_eq!(
            narrowed_requests.len(),
            4,
            "the narrowed A-only round must not query symbol B: {narrowed_requests:?}"
        );
        assert!(
            narrowed_requests
                .iter()
                .all(|request| !request.contains("symbol=ETHUSDT")),
            "the narrowed A-only round must not query symbol B: {narrowed_requests:?}"
        );
        cleanup(narrowed_store, narrowed_path).await;

        let (full_before, full_after, full_requests, full_store, full_path) =
            run(Coverage::Full).await;
        assert_ne!(
            full_after.0, full_before.0,
            "Full coverage must apply missing/terminal truth to symbol B's inflight order"
        );
        assert_ne!(
            full_after.1, full_before.1,
            "Full coverage must apply missing/terminal truth to symbol B's protection order"
        );
        assert!(
            full_requests
                .iter()
                .any(|request| request.contains("symbol=ETHUSDT")),
            "the Full coverage control must query symbol B's inflight order: {full_requests:?}"
        );
        cleanup(full_store, full_path).await;
    }

    #[tokio::test]
    async fn a8_i6_last_full_scan_timestamp_survives_store_restart() {
        let (store, path) = test_store("a8-i6-restart").await;
        persist_last_full_scan_ms(&store, "cred_a8_i6", 1_900_000_000_123)
            .await
            .expect("persist full scan timestamp");
        store.pool().close().await;
        let reopened = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("reopen the same engine database");
        assert_eq!(
            last_full_scan_ms(&reopened, "cred_a8_i6")
                .await
                .expect("reload full scan timestamp"),
            1_900_000_000_123
        );
        cleanup(reopened, path).await;
    }

    #[tokio::test]
    async fn a8_i6_full_scan_path_persists_its_completion_timestamp() {
        let (store, path) = test_store("a8-i6-full-path").await;
        let credential = "cred_a8_i6_full_path";
        let client = BinanceFuturesClient::new_with_mock_responses(
            credential,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        run_reconcile_round(
            &ReconcileExchangeSource::Stub(client),
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            crate::binance_futures::BinanceKeys {
                api_key: "unused".to_string(),
                api_secret: "unused".to_string(),
            },
        )
        .await
        .expect("full periodic round");
        assert!(
            last_full_scan_ms(&store, credential)
                .await
                .expect("read full scan timestamp")
                > 0,
            "the FullScan completion must be durable"
        );
        cleanup(store, path).await;
    }

    #[test]
    fn a8_i7_full_scan_interval_is_clamped_and_has_no_disable_switch() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        {
            let _high = crate::binance_futures::tests::TestEnvVar::set(
                "RUSTCORE_RECONCILE_FULL_SCAN_INTERVAL_MS".to_string(),
                "999999999",
            );
            assert_eq!(super::periodic_reconcile_full_scan_interval_ms(), 3_600_000);
        }
        {
            let _low = crate::binance_futures::tests::TestEnvVar::set(
                "RUSTCORE_RECONCILE_FULL_SCAN_INTERVAL_MS".to_string(),
                "1",
            );
            assert_eq!(super::periodic_reconcile_full_scan_interval_ms(), 120_000);
        }
        let source = include_str!("reconcile.rs");
        assert_eq!(
            source.matches("DISABLE_FULL_SCAN").count(),
            1,
            "the only occurrence must be this source-shape guard, never an env switch"
        );
    }

    #[tokio::test]
    async fn a8_i6_migration_0034_creates_durable_full_scan_state() {
        let (store, path) = test_store("a8-migration-0034").await;
        let columns = sqlx::query(
            "SELECT name FROM pragma_table_info('reconcile_full_scan_state') ORDER BY cid",
        )
        .fetch_all(store.pool())
        .await
        .expect("inspect migration 0034 table")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
        assert_eq!(
            columns,
            ["credential_id", "last_full_scan_ms", "updated_at_ms"]
        );
        cleanup(store, path).await;
    }

    async fn projection_seed(store: &Store, credential: &str, symbol: &str, qty: &str) {
        sqlx::query("INSERT INTO engine_positions (credential_id, exchange, market_kind, symbol, position_amt, entry_price, mark_price, leverage, unrealized_pnl, updated_at_ms) VALUES (?, 'binance', 'futures', ?, ?, '100', '101', '1', '0', 1)")
            .bind(credential).bind(symbol).bind(qty).execute(store.pool()).await.unwrap();
    }

    async fn projection_account(
        store: &Store,
        credential: &str,
    ) -> crate::proto_v2::GetAccountStateResponse {
        use crate::proto_v2::engine_execution_server::EngineExecution;
        crate::svc_v2::EngineExecutionV2Service::new(store.clone())
            .get_account_state(tonic::Request::new(
                crate::proto_v2::GetAccountStateRequest {
                    credential_id: credential.to_string(),
                    bot_uid: String::new(),
                    schema_version: 1,
                },
            ))
            .await
            .unwrap()
            .into_inner()
    }

    #[tokio::test]
    async fn account_projection_stale_zero_observation_cannot_delete_new_nonzero_truth() {
        let (store, path) = test_store("projection-stale-zero").await;
        projection_seed(&store, "cred_projection", "BTCUSDT", "0").await;
        let mut tx = store.pool().begin().await.unwrap();
        let old = sqlx::query("SELECT position_amt FROM engine_positions WHERE credential_id='cred_projection' AND symbol='BTCUSDT'").fetch_one(&mut *tx).await.unwrap().get::<String,_>("position_amt");
        // Same stale observation/current-row mismatch as a MySQL snapshot read
        // followed by another reconcile's committed update. This is a direct
        // SQL predicate test, not a claim of a two-session MySQL stress run.
        sqlx::query("UPDATE engine_positions SET position_amt='0.5' WHERE credential_id='cred_projection' AND symbol='BTCUSDT'").execute(&mut *tx).await.unwrap();
        let deleted =
            super::retire_observed_zero_projection(&mut tx, "cred_projection", "BTCUSDT", &old)
                .await
                .unwrap();
        assert_eq!(
            deleted, 0,
            "stale zero observation must not delete current exposure"
        );
        tx.commit().await.unwrap();
        let current = sqlx::query("SELECT position_amt FROM engine_positions WHERE credential_id='cred_projection' AND symbol='BTCUSDT'").fetch_one(store.pool()).await.unwrap().get::<String,_>("position_amt");
        assert_eq!(current, "0.5");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_actual_account_wide_sources_retire_zero_residue() {
        for source in ["rest", "ws-fallback", "ws-full"] {
            let (store, path) = test_store(&format!("projection-wide-{source}")).await;
            let credential = format!("cred_{}", ulid::Ulid::new());
            projection_seed(&store, &credential, "OLDUSDT", "0").await;
            let mut responses = vec![(200, "[]".to_string()), (200, "[]".to_string())];
            if source != "ws-full" {
                responses.extend([(200, "[]".to_string()), (200, balances().to_string())]);
            }
            let client = BinanceFuturesClient::new_with_mock_responses(&credential, responses);
            let ws = (source != "rest").then(|| {
                crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_for_test(
                    store.clone(),
                    format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
                    if source == "ws-fallback" {
                        Err("offline injected failure".to_string())
                    } else {
                        Ok(serde_json::json!([]))
                    },
                    Ok(balances()),
                )
            });
            let observed = now_ms();
            reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
                &credential,
                store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                &client,
                Coverage::Full,
                ws.as_ref(),
                || observed,
            )
            .await
            .unwrap();
            let rows = sqlx::query("SELECT symbol FROM engine_positions WHERE credential_id=?")
                .bind(&credential)
                .fetch_all(store.pool())
                .await
                .unwrap();
            assert!(
                rows.is_empty(),
                "actual {source} account-wide wiring must retire obsolete zero projection"
            );
            let account = projection_account(&store, &credential).await;
            assert!(account.available);
            assert_eq!(
                account.as_of_ms, observed,
                "actual {source} account scope must publish fresh observed facts"
            );
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn account_projection_preserves_position_501_and_balance_201() {
        let (store, path) = test_store("projection-tail").await;
        let observed = now_ms();
        let mut positions: Vec<_> = (0..500)
            .map(|i| position(&format!("ASSET{i}USDT"), "0", "BOTH"))
            .collect();
        positions.push(position("BTCUSDT", "0.01", "BOTH"));
        let mut assets: Vec<_> = (0..200).map(|i| serde_json::json!({"asset":format!("ASSET{i}"),"balance":"0","availableBalance":"0"})).collect();
        assets.push(balances()[0].clone());
        let outcome = persist_positions_and_balances(
            &store,
            "cred_projection",
            observed,
            &serde_json::json!(positions),
            &serde_json::json!(assets),
        )
        .await
        .unwrap();
        assert!(!outcome.all_positions_zero);
        assert_eq!(
            outcome.orphan_positions.len(),
            1,
            "tail exposure must be recovered, not silently truncated"
        );
        let row = sqlx::query("SELECT position_amt FROM engine_positions WHERE credential_id='cred_projection' AND symbol='BTCUSDT'").fetch_optional(store.pool()).await.unwrap();
        assert_eq!(row.unwrap().get::<String, _>("position_amt"), "0.01");
        let account = projection_account(&store, "cred_projection").await;
        assert!(
            account.available,
            "tail USDT balance must reach account truth: {}",
            account.unavailable_reason
        );
        assert_eq!(account.as_of_ms, observed);
        assert_eq!(account.open_positions_count, Some(1));
        assert_eq!(account.positions[0].qty, "0.01");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_sparse_full_snapshot_retires_only_obsolete_zero_rows() {
        let (store, path) = test_store("projection-sparse").await;
        projection_seed(&store, "cred_projection", "AIXBTUSDT", "0.000").await;
        projection_seed(&store, "cred_other", "AIXBTUSDT", "0").await;
        let observed = now_ms();
        persist_positions_and_balances(
            &store,
            "cred_projection",
            observed,
            &serde_json::json!([]),
            &balances(),
        )
        .await
        .unwrap();
        let own = sqlx::query(
            "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert!(
            own.is_empty(),
            "old flat rows must not pin account observation time"
        );
        let foreign =
            sqlx::query("SELECT symbol FROM engine_positions WHERE credential_id='cred_other'")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(foreign.len(), 1);
        let account = projection_account(&store, "cred_projection").await;
        assert!(account.available);
        assert_eq!(
            account.as_of_ms, observed,
            "freshness comes from actual received balance, never wall clock read"
        );
        assert_eq!(account.open_positions_count, Some(0));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_missing_nonzero_or_invalid_truth_remains_fail_closed() {
        let (store, path) = test_store("projection-unknown").await;
        for (symbol, qty) in [
            ("BTCUSDT", "0.01"),
            ("ETHUSDT", "."),
            ("SOLUSDT", "+"),
            ("ADAUSDT", "0.0"),
        ] {
            projection_seed(&store, "cred_projection", symbol, qty).await;
        }
        persist_positions_and_balances(
            &store,
            "cred_projection",
            now_ms(),
            &serde_json::json!([]),
            &balances(),
        )
        .await
        .unwrap();
        let rows = sqlx::query("SELECT symbol, position_amt FROM engine_positions WHERE credential_id='cred_projection' ORDER BY symbol").fetch_all(store.pool()).await.unwrap();
        assert_eq!(
            rows.len(),
            3,
            "only validated decimal zero is eligible for retirement"
        );
        assert_eq!(rows[0].get::<String, _>("position_amt"), "0.01");
        assert!(
            !projection_account(&store, "cred_projection")
                .await
                .available,
            "missing nonzero/invalid truth must not become a fresh clean account"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_symbol_scope_does_not_retire_uncovered_zero_rows() {
        let (store, path) = test_store("projection-scope").await;
        projection_seed(&store, "cred_projection", "BTCUSDT", "0").await;
        projection_seed(&store, "cred_projection", "ETHUSDT", "0").await;
        super::persist_positions_and_balances_with_coverage(
            &store,
            "cred_projection",
            now_ms(),
            &serde_json::json!([]),
            &balances(),
            &Coverage::Symbols(vec!["BTCUSDT".to_string()]),
        )
        .await
        .unwrap();
        let rows = sqlx::query(
            "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String, _>("symbol"), "ETHUSDT");
        assert_eq!(
            projection_account(&store, "cred_projection").await.as_of_ms,
            1,
            "narrow truth cannot refresh the whole account"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_oversized_snapshot_fails_before_mutation() {
        let (store, path) = test_store("projection-oversized").await;
        projection_seed(&store, "cred_projection", "OLDUSDT", "0").await;
        let positions = serde_json::json!(vec![position("BTCUSDT", "0", "BOTH"); 5_001]);
        let error = persist_positions_and_balances(
            &store,
            "cred_projection",
            now_ms(),
            &positions,
            &balances(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("exceeds supported entry count"));
        let rows = sqlx::query(
            "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String, _>("symbol"), "OLDUSDT");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_invalid_or_duplicate_snapshot_keeps_previous_rows() {
        let (store, path) = test_store("projection-invalid").await;
        projection_seed(&store, "cred_projection", "OLDUSDT", "0").await;
        for payload in [
            serde_json::json!({}),
            serde_json::json!([{"positionAmt":"0"}]),
            serde_json::json!([position("BTCUSDT", ".", "BOTH")]),
            serde_json::json!([
                position("BTCUSDT", "1", "LONG"),
                position("BTCUSDT", "-1", "SHORT")
            ]),
        ] {
            assert!(persist_positions_and_balances(
                &store,
                "cred_projection",
                now_ms(),
                &payload,
                &balances()
            )
            .await
            .is_err());
            let rows = sqlx::query(
                "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
            )
            .fetch_all(store.pool())
            .await
            .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<String, _>("symbol"), "OLDUSDT");
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_failure_after_retirement_rolls_back_whole_observation() {
        let (store, path) = test_store("projection-rollback").await;
        projection_seed(&store, "cred_projection", "OLDUSDT", "0").await;
        sqlx::query("CREATE TRIGGER fail_projection_balance BEFORE INSERT ON engine_balances BEGIN SELECT RAISE(ABORT, 'injected balance write failure'); END").execute(store.pool()).await.unwrap();
        assert!(persist_positions_and_balances(
            &store,
            "cred_projection",
            now_ms(),
            &serde_json::json!([]),
            &balances()
        )
        .await
        .is_err());
        let rows = sqlx::query(
            "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "projection retirement and balance persistence must roll back together"
        );
        assert_eq!(rows[0].get::<String, _>("symbol"), "OLDUSDT");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn account_projection_actual_ws_narrowed_round_keeps_uncovered_truth() {
        let (store, path) = test_store("projection-ws-scope").await;
        let credential = "cred_projection";
        projection_seed(&store, credential, "BTCUSDT", "0").await;
        projection_seed(&store, credential, "ETHUSDT", "0").await;
        let ws = crate::binance_ws_api::BinanceWsApiClient::new_with_mock_account_reads_for_test(
            store.clone(),
            format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
            Ok(serde_json::json!([])),
            Ok(balances()),
        );
        let client = BinanceFuturesClient::new_with_mock_responses(
            credential,
            vec![(200, "[]".to_string()), (200, "[]".to_string())],
        );
        reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            &client,
            Coverage::Symbols(vec!["BTCUSDT".to_string()]),
            Some(&ws),
            now_ms,
        )
        .await
        .unwrap();
        let rows = sqlx::query(
            "SELECT symbol FROM engine_positions WHERE credential_id='cred_projection'",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "actual narrowed WS round must pass its position coverage to persistence"
        );
        assert_eq!(rows[0].get::<String, _>("symbol"), "ETHUSDT");
        assert_eq!(projection_account(&store, credential).await.as_of_ms, 1);
        cleanup(store, path).await;
    }

    fn ws_contract_positions(qty: &str) -> serde_json::Value {
        let mut row = position("BTCUSDT", qty, "BOTH");
        row["leverage"] = serde_json::json!("20");
        row["unRealizedProfit"] = serde_json::json!("-0.125");
        serde_json::json!([row])
    }

    async fn ws_contract_seed(store: &Store, credential: &str) {
        persist_positions_and_balances(
            store,
            credential,
            now_ms() - 1_000,
            &ws_contract_positions("1"),
            &balances(),
        )
        .await
        .expect("seed a complete prior observation and durable ownership");
    }

    fn ws_contract_ws(
        store: &Store,
        positions: serde_json::Value,
        requests: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    ) -> crate::binance_ws_api::BinanceWsApiClient {
        crate::binance_ws_api::BinanceWsApiClient::new_with_recorded_account_reads_for_test(
            store.clone(),
            format!(
                "binance-futures-ws:route:{}:venue:testnet",
                ulid::Ulid::new()
            ),
            Ok(positions),
            Ok(balances()),
            requests,
        )
    }

    async fn ws_contract_projection_rows(store: &Store) -> Vec<String> {
        sqlx::query(
            "SELECT json_object('credential',credential_id,'symbol',symbol,\
             'symbol_key',symbol_key,'qty',position_amt,'entry',entry_price,\
             'mark',mark_price,'leverage',leverage,'pnl',unrealized_pnl,\
             'liquidation',liquidation_price,'at',updated_at_ms,'raw',hex(raw_json)) AS snapshot \
             FROM engine_positions ORDER BY credential_id,symbol",
        )
        .fetch_all(store.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("snapshot"))
        .collect()
    }

    async fn ws_contract_round(
        store: &Store,
        credential: &str,
        positions: serde_json::Value,
        rest: &BinanceFuturesClient,
    ) -> anyhow::Result<super::CoveredReconcileTruth> {
        let ws = ws_contract_ws(
            store,
            positions,
            Arc::new(std::sync::Mutex::new(Vec::new())),
        );
        reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
            credential,
            store.clone(),
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            rest,
            Coverage::Full,
            Some(&ws),
            now_ms,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_position_contract_real_periodic_tick_signs_complete_method_and_reaches_account_rpc()
    {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = crate::binance_futures::tests::TestEnvVar::set(
            "RECONCILE_INTERVAL_MS".to_string(),
            "1000",
        );
        let (store, path) = test_store("ws-contract-periodic").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        ws_contract_seed(&store, &credential).await;
        let previous_as_of = projection_account(&store, &credential).await.as_of_ms;
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut incoming = ws_contract_positions("1");
        incoming[0]["markPrice"] = serde_json::json!("105");
        let ws = ws_contract_ws(&store, incoming, requests.clone());
        let rest = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![(200, "[]".into()), (200, "[]".into())],
        );
        let mut manager = super::ReconcilerManager::with_stubbed_exchange_for_tests(rest.clone());
        manager.ws_api_override = Some(ws);
        manager
            .ensure_running_after_stream_startup(
                &credential,
                store.clone(),
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
                crate::binance_futures::BinanceKeys {
                    api_key: "key".into(),
                    api_secret: "secret".into(),
                },
            )
            .await;
        let finished = tokio::time::timeout(Duration::from_secs(10), async {
            while last_full_scan_ms(&store, &credential).await.unwrap() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        for (_, task) in manager.inner.lock().await.drain() {
            task.abort();
            let _ = task.await;
        }
        finished.expect("the real timer must publish a successful full scan");
        let account = projection_account(&store, &credential).await;
        assert!(account.available);
        assert_eq!(account.positions.len(), 1);
        assert_eq!(account.positions[0].leverage, 20);
        assert_eq!(account.positions[0].mark_price, "105");
        assert!(account.as_of_ms > previous_as_of);
        let recorded = requests.lock().unwrap().clone();
        assert_eq!(
            recorded.len(),
            2,
            "one WS position read and one balance read"
        );
        assert!(
            recorded
                .iter()
                .any(|request| request["method"] == "account.position"),
            "periodic projection needs the WS method that reports leverage: {recorded:?}"
        );
        assert!(recorded
            .iter()
            .any(|request| request["method"] == "v2/account.balance"));
        for request in recorded {
            assert!(request["id"].as_str().is_some_and(|id| !id.is_empty()));
            let params = request["params"].as_object().unwrap();
            assert_eq!(params["apiKey"], "key");
            assert_eq!(params["recvWindow"], "5000");
            assert!(
                params["timestamp"]
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
                    > 0
            );
            assert!(
                !params.contains_key("symbol"),
                "the actual first tick is account-wide"
            );
            let unsigned: Vec<(String, String)> = params
                .iter()
                .filter(|(name, _)| name.as_str() != "signature")
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_string()))
                .collect();
            let signature = crate::binance_futures::signed_query_hmac(b"secret", &unsigned)
                .unwrap()
                .1;
            assert_eq!(
                params["signature"], signature,
                "consume the actual signed request, not an independently fabricated payload"
            );
        }
        let rest_paths: Vec<_> = rest
            .recorded_requests()
            .into_iter()
            .map(|request| {
                reqwest::Url::parse(request.split_whitespace().nth(1).unwrap())
                    .unwrap()
                    .path()
                    .to_string()
            })
            .collect();
        assert_eq!(
            rest_paths,
            ["/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_missing_or_wrong_financial_fields_take_exactly_one_rest_fallback()
    {
        let (store, path) = test_store("ws-contract-fallback").await;
        for qty in ["1", "0"] {
            for field in ["entryPrice", "markPrice", "leverage", "unRealizedProfit"] {
                for invalid in [
                    None,
                    Some(serde_json::Value::Null),
                    Some(serde_json::json!("")),
                    Some(serde_json::json!(20)),
                ] {
                    let credential = format!("cred_{}", ulid::Ulid::new());
                    ws_contract_seed(&store, &credential).await;
                    let mut incoming = ws_contract_positions(qty);
                    match invalid {
                        None => {
                            incoming[0].as_object_mut().unwrap().remove(field);
                        }
                        Some(value) => incoming[0][field] = value,
                    }
                    let mut fallback = ws_contract_positions(qty);
                    fallback[0]["leverage"] = serde_json::json!("10");
                    fallback[0]["markPrice"] = serde_json::json!("107");
                    let rest = BinanceFuturesClient::new_with_mock_responses(
                        &credential,
                        vec![
                            (200, "[]".into()),
                            (200, "[]".into()),
                            (200, fallback.to_string()),
                            (200, balances().to_string()),
                        ],
                    );
                    ws_contract_round(&store, &credential, incoming, &rest)
                        .await
                        .unwrap();
                    let paths: Vec<_> = rest
                        .recorded_requests()
                        .into_iter()
                        .map(|request| {
                            reqwest::Url::parse(request.split_whitespace().nth(1).unwrap())
                                .unwrap()
                                .path()
                                .to_string()
                        })
                        .collect();
                    assert_eq!(paths, ["/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders", "/fapi/v2/positionRisk", "/fapi/v2/balance"],
                        "successful WS with invalid {field}, qty={qty}, must fall back exactly once");
                    let row = sqlx::query("SELECT leverage,mark_price FROM engine_positions WHERE credential_id=? AND symbol='BTCUSDT'")
                        .bind(&credential).fetch_one(store.pool()).await.unwrap();
                    assert_eq!(row.get::<String, _>("leverage"), "10");
                    assert_eq!(row.get::<String, _>("mark_price"), "107");
                    assert!(projection_account(&store, &credential).await.available);
                }
            }
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_invalid_rest_preserves_entire_previous_observation() {
        let (store, path) = test_store("ws-contract-invalid-rest").await;
        for qty in ["1", "0"] {
            for field in ["entryPrice", "markPrice", "leverage", "unRealizedProfit"] {
                let credential = format!("cred_{}", ulid::Ulid::new());
                ws_contract_seed(&store, &credential).await;
                projection_seed(&store, &credential, "OLDUSDT", "0").await;
                let before_positions = ws_contract_projection_rows(&store).await;
                let before_balances = a8_r1_table_snapshot(&store, "engine_balances").await;
                let before_local = a8_r1_table_snapshot(&store, "position_state").await;
                let mut incoming = ws_contract_positions(qty);
                incoming[0].as_object_mut().unwrap().remove("leverage");
                let mut fallback = ws_contract_positions(qty);
                fallback[0][field] = serde_json::json!("not-a-number");
                let rest = BinanceFuturesClient::new_with_mock_responses(
                    &credential,
                    vec![
                        (200, "[]".into()),
                        (200, "[]".into()),
                        (200, fallback.to_string()),
                        (200, balances().to_string()),
                    ],
                );
                let result = ws_contract_round(&store, &credential, incoming, &rest).await;
                assert!(
                    result.is_err(),
                    "invalid REST {field}, qty={qty}, cannot complete a round"
                );
                assert_eq!(
                    rest.recorded_requests().len(),
                    4,
                    "one bounded fallback, no retry loop"
                );
                assert_eq!(ws_contract_projection_rows(&store).await, before_positions,
                    "no empty overwrite, zero-residue retirement, raw replacement or timestamp refresh");
                assert_eq!(
                    a8_r1_table_snapshot(&store, "engine_balances").await,
                    before_balances
                );
                assert_eq!(
                    a8_r1_table_snapshot(&store, "position_state").await,
                    before_local
                );
                assert_eq!(last_full_scan_ms(&store, &credential).await.unwrap(), 0);
            }
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_persistence_rejects_incomplete_rows_before_any_mutation() {
        let (store, path) = test_store("ws-contract-persist-guard").await;
        for qty in ["1", "0"] {
            for field in ["entryPrice", "markPrice", "leverage", "unRealizedProfit"] {
                let credential = format!("cred_{}", ulid::Ulid::new());
                ws_contract_seed(&store, &credential).await;
                projection_seed(&store, &credential, "OLDUSDT", "0").await;
                let before = ws_contract_projection_rows(&store).await;
                let mut incoming = ws_contract_positions(qty);
                incoming[0].as_object_mut().unwrap().remove(field);
                assert!(
                    persist_positions_and_balances(
                        &store,
                        &credential,
                        now_ms(),
                        &incoming,
                        &balances(),
                    )
                    .await
                    .is_err(),
                    "persistence must independently reject missing {field}, qty={qty}"
                );
                assert_eq!(ws_contract_projection_rows(&store).await, before);
            }
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_explicit_zero_closes_position_without_rest_double_read() {
        let (store, path) = test_store("ws-contract-zero").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        ws_contract_seed(&store, &credential).await;
        let mut incoming = ws_contract_positions("0.000");
        incoming[0]["entryPrice"] = serde_json::json!("0.0");
        incoming[0]["markPrice"] = serde_json::json!("0");
        incoming[0]["unRealizedProfit"] = serde_json::json!("0");
        incoming[0]["liquidationPrice"] = serde_json::json!("0");
        let rest = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![(200, "[]".into()), (200, "[]".into())],
        );
        let truth = ws_contract_round(&store, &credential, incoming, &rest)
            .await
            .unwrap();
        assert!(truth.truth.all_positions_zero);
        assert_eq!(rest.recorded_requests().len(), 2);
        let account = projection_account(&store, &credential).await;
        assert!(account.available);
        assert_eq!(account.open_positions_count, Some(0));
        let local = sqlx::query("SELECT state,qty FROM position_state WHERE credential_id=?")
            .bind(&credential)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(local.get::<String, _>("state"), "closed");
        assert_eq!(local.get::<String, _>("qty"), "0");
        let projection = sqlx::query("SELECT entry_price,mark_price,leverage,liquidation_price FROM engine_positions WHERE credential_id=?")
            .bind(&credential).fetch_one(store.pool()).await.unwrap();
        assert_eq!(projection.get::<String, _>("entry_price"), "0.0");
        assert_eq!(projection.get::<String, _>("mark_price"), "0");
        assert_eq!(projection.get::<String, _>("leverage"), "20");
        assert_eq!(projection.get::<String, _>("liquidation_price"), "0");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_decimal_validation_matches_the_actual_account_consumer() {
        let (store, path) = test_store("ws-contract-decimal").await;
        for (field, invalid) in [
            ("entryPrice", "+1"),
            ("entryPrice", ".1"),
            ("entryPrice", "1."),
            ("markPrice", "1e3"),
            ("markPrice", "NaN"),
            (
                "unRealizedProfit",
                "170141183460469231731687303715884105728",
            ),
            ("leverage", "0"),
            ("leverage", "1.5"),
            ("leverage", "4294967296"),
        ] {
            let credential = format!("cred_{}", ulid::Ulid::new());
            ws_contract_seed(&store, &credential).await;
            let mut incoming = ws_contract_positions("1");
            incoming[0][field] = serde_json::json!(invalid);
            let rest = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![
                    (200, "[]".into()),
                    (200, "[]".into()),
                    (200, ws_contract_positions("1").to_string()),
                    (200, balances().to_string()),
                ],
            );
            ws_contract_round(&store, &credential, incoming, &rest)
                .await
                .unwrap();
            assert_eq!(
                rest.recorded_requests().len(),
                4,
                "invalid {field}={invalid} must fall back"
            );
            assert!(projection_account(&store, &credential).await.available);
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ws_position_contract_invalid_second_row_cannot_partially_publish_an_observation() {
        let (store, path) = test_store("ws-contract-invalid-tail").await;
        for mode in ["ws-valid-rest", "ws-invalid-rest", "direct-invalid"] {
            let credential = format!("cred_{}", ulid::Ulid::new());
            let original = serde_json::json!([
                position("BTCUSDT", "1", "BOTH"),
                position("ETHUSDT", "2", "BOTH"),
            ]);
            persist_positions_and_balances(
                &store,
                &credential,
                now_ms() - 1_000,
                &original,
                &balances(),
            )
            .await
            .unwrap();
            projection_seed(&store, &credential, "OLDUSDT", "0").await;
            let before_positions = ws_contract_projection_rows(&store).await;
            let before_balances = a8_r1_table_snapshot(&store, "engine_balances").await;
            let before_local = a8_r1_table_snapshot(&store, "position_state").await;
            let mut complete = original.clone();
            complete[0]["positionAmt"] = serde_json::json!("3");
            complete[0]["markPrice"] = serde_json::json!("107");
            complete[1]["positionAmt"] = serde_json::json!("4");
            complete[1]["markPrice"] = serde_json::json!("108");
            let mut malformed = complete.clone();
            malformed[1]["leverage"] = serde_json::json!("");
            let mut new_balances = balances();
            new_balances[0]["balance"] = serde_json::json!("12000");
            new_balances[0]["availableBalance"] = serde_json::json!("11000");

            if mode == "direct-invalid" {
                assert!(
                    persist_positions_and_balances(
                        &store,
                        &credential,
                        now_ms(),
                        &malformed,
                        &new_balances,
                    )
                    .await
                    .is_err(),
                    "persistence must inspect the second row before publishing the valid prefix"
                );
            } else {
                let fallback = if mode == "ws-valid-rest" {
                    &complete
                } else {
                    &malformed
                };
                let rest = BinanceFuturesClient::new_with_mock_responses(
                    &credential,
                    vec![
                        (200, "[]".into()),
                        (200, "[]".into()),
                        (200, fallback.to_string()),
                        (200, new_balances.to_string()),
                    ],
                );
                let result = ws_contract_round(&store, &credential, malformed, &rest).await;
                assert_eq!(rest.recorded_requests().len(), 4,
                    "a valid prefix must not hide the malformed second WS row; {mode} needs exactly one full REST fallback");
                if mode == "ws-valid-rest" {
                    result.expect(
                        "complete REST truth must replace the entire malformed WS observation",
                    );
                    let account = projection_account(&store, &credential).await;
                    assert!(account.available);
                    assert_eq!(account.positions.len(), 2);
                    assert_eq!(account.positions[0].qty, "3");
                    assert_eq!(account.positions[0].mark_price, "107");
                    assert_eq!(account.positions[1].qty, "4");
                    assert_eq!(account.positions[1].mark_price, "108");
                    assert_eq!(account.available_usdt.as_deref(), Some("11000"));
                    let old = sqlx::query("SELECT symbol FROM engine_positions WHERE credential_id=? AND symbol='OLDUSDT'")
                        .bind(&credential).fetch_optional(store.pool()).await.unwrap();
                    assert!(
                        old.is_none(),
                        "only complete fallback truth may retire old zero residue"
                    );
                    continue;
                }
                assert!(
                    result.is_err(),
                    "REST's malformed second row must fail the whole observation"
                );
            }
            assert_eq!(ws_contract_projection_rows(&store).await, before_positions,
                "{mode}: no first-row write, empty tail, raw replacement, zero retirement or timestamp refresh");
            assert_eq!(
                a8_r1_table_snapshot(&store, "engine_balances").await,
                before_balances,
                "{mode}: account balances must remain from the old observation"
            );
            assert_eq!(
                a8_r1_table_snapshot(&store, "position_state").await,
                before_local,
                "{mode}: the valid prefix must not advance local quantities or timestamps"
            );
            assert_eq!(last_full_scan_ms(&store, &credential).await.unwrap(), 0);
        }
        cleanup(store, path).await;
    }

    fn balances() -> serde_json::Value {
        serde_json::json!([{
            "asset": "USDT",
            "balance": "10000",
            "availableBalance": "9000"
        }])
    }

    fn position(symbol: &str, qty: &str, side: &str) -> serde_json::Value {
        serde_json::json!({
            "symbol": symbol,
            "positionAmt": qty,
            "entryPrice": "100",
            "markPrice": "101",
            "leverage": "1",
            "unRealizedProfit": "1",
            "liquidationPrice": "50",
            "positionSide": side
        })
    }

    fn local_position(
        position_id: &str,
        symbol_key: &str,
        direction: &str,
        state: &str,
        entry_epoch: i64,
    ) -> LocalPositionObservation {
        LocalPositionObservation {
            position_id: position_id.to_string(),
            bot_uid: "bot_local".to_string(),
            symbol_key: symbol_key.to_string(),
            direction: direction.to_string(),
            entry_epoch,
            state: state.to_string(),
        }
    }

    async fn persist_orphan(store: &Store, ts: i64) -> super::PositionSnapshotOutcome {
        persist_positions_and_balances(
            store,
            "cred_t310",
            ts,
            &serde_json::json!([position("BTCUSDT", "0.001", "BOTH")]),
            &balances(),
        )
        .await
        .expect("persist orphan position snapshot")
    }

    #[test]
    fn b3_algo_disappearing_from_open_set_requires_single_order_query() {
        assert_eq!(
            algo_truth_lookup(false, true, false),
            AlgoTruthLookup::SingleOrderQuery
        );
        assert_eq!(
            algo_truth_lookup(false, false, true),
            AlgoTruthLookup::SingleOrderQuery
        );
        assert_eq!(
            algo_truth_lookup(true, true, true),
            AlgoTruthLookup::OpenSet
        );
        assert_eq!(
            algo_truth_lookup(false, false, false),
            AlgoTruthLookup::Unidentified
        );
        assert_eq!(
            truth_source_for_algo_lookup(AlgoTruthLookup::OpenSet),
            Some(crate::protection::AlgoTruthSource::RestOpenAlgoOrders)
        );
        assert_eq!(
            truth_source_for_algo_lookup(AlgoTruthLookup::SingleOrderQuery),
            Some(crate::protection::AlgoTruthSource::RestAlgoQuery)
        );
        assert_eq!(
            truth_source_for_algo_lookup(AlgoTruthLookup::Unidentified),
            None
        );
    }

    #[test]
    fn t310_orphan_disposition_ignores_exact_zero_without_local_state() {
        assert_eq!(
            orphan_position_disposition("BTCUSDT", Some("BOTH"), "0.000", &[]),
            Ok(OrphanPositionDisposition::IgnoreZero)
        );
    }

    #[test]
    fn t310_orphan_disposition_matches_active_symbol_and_direction() {
        let local = [local_position(
            "pos_active",
            "binance|futures|BTC|USDT|USDT",
            "long",
            "protected",
            3,
        )];
        assert_eq!(
            orphan_position_disposition("BTCUSDT", Some("LONG"), "0.001", &local),
            Ok(OrphanPositionDisposition::MatchedActive {
                position_id: "pos_active".to_string(),
            })
        );
    }

    #[test]
    fn t310_orphan_disposition_reopens_latest_closed_truth_without_a_grace_window() {
        let local = [
            local_position(
                "pos_closed_old",
                "binance|futures|BTC|USDT|USDT",
                "long",
                "closed",
                1,
            ),
            local_position(
                "pos_closed_latest",
                "binance|futures|BTC|USDT|USDT",
                "long",
                "closed",
                2,
            ),
        ];
        assert_eq!(
            orphan_position_disposition("BTCUSDT", Some("BOTH"), "0.001", &local),
            Ok(OrphanPositionDisposition::ReopenClosed {
                position_id: "pos_closed_latest".to_string(),
                bot_uid: "bot_local".to_string(),
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                direction: "long".to_string(),
                entry_epoch: 2,
            })
        );
    }

    /// **orphan 匹配也是身份判定。**
    ///
    /// 一条币本位（`…|BTC|USDT|BTC`）或异 venue 的本地仓，其 `base+quote`
    /// 拼出来的 symbol 同样是 `BTCUSDT`。裸判据会让它"认领"USDⓈ-M 的孤儿
    /// 仓位：真实 orphan 被吸收成 `MatchedActive`（于是不进恢复），而认领它
    /// 的那条本地仓拿到了另一张合约的数量。
    #[test]
    fn orphan_matching_refuses_a_local_position_of_another_contract() {
        for foreign in [
            "binance|futures|BTC|USDT|BTC",
            "okx|futures|BTC|USDT|USDT",
            "binance|spot|BTC|USDT|USDT",
        ] {
            let local = [local_position("pos_foreign", foreign, "long", "open", 3)];
            assert_eq!(
                orphan_position_disposition("BTCUSDT", Some("LONG"), "0.001", &local),
                Ok(OrphanPositionDisposition::CreateUnowned {
                    symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                    direction: "long".to_string(),
                }),
                "{foreign} must not claim a USDⓈ-M orphan position"
            );
        }
    }

    #[test]
    fn t310_orphan_disposition_is_independent_per_symbol_and_direction() {
        let local = [local_position(
            "pos_btc_long",
            "binance|futures|BTC|USDT|USDT",
            "long",
            "open",
            1,
        )];
        assert_eq!(
            orphan_position_disposition("ETHUSDT", Some("LONG"), "0.25", &local),
            Ok(OrphanPositionDisposition::CreateUnowned {
                symbol_key: "binance|futures|ETH|USDT|USDT".to_string(),
                direction: "long".to_string(),
            })
        );
        assert_eq!(
            orphan_position_disposition("BTCUSDT", Some("SHORT"), "0.001", &local),
            Ok(OrphanPositionDisposition::CreateUnowned {
                symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
                direction: "short".to_string(),
            })
        );
    }

    #[test]
    fn t310_each_reconcile_diff_cause_independently_degrades() {
        assert_eq!(
            reconcile_disposition(false, false, false, false, false, false),
            ReconcileDisposition::Clean
        );
        for (actual, expected) in [
            (
                reconcile_disposition(true, false, false, false, false, false),
                ReconcileDiffCauses {
                    missing_open_orders: true,
                    missing_open_algo_orders: false,
                    unresolved_algo_orders: false,
                    orphan_positions: false,
                    unidentified_local_orders: false,
                    terminal_local_remote_active: false,
                },
            ),
            (
                reconcile_disposition(false, true, false, false, false, false),
                ReconcileDiffCauses {
                    missing_open_orders: false,
                    missing_open_algo_orders: true,
                    unresolved_algo_orders: false,
                    orphan_positions: false,
                    unidentified_local_orders: false,
                    terminal_local_remote_active: false,
                },
            ),
            (
                reconcile_disposition(false, false, true, false, false, false),
                ReconcileDiffCauses {
                    missing_open_orders: false,
                    missing_open_algo_orders: false,
                    unresolved_algo_orders: true,
                    orphan_positions: false,
                    unidentified_local_orders: false,
                    terminal_local_remote_active: false,
                },
            ),
            (
                reconcile_disposition(false, false, false, true, false, false),
                ReconcileDiffCauses {
                    missing_open_orders: false,
                    missing_open_algo_orders: false,
                    unresolved_algo_orders: false,
                    orphan_positions: true,
                    unidentified_local_orders: false,
                    terminal_local_remote_active: false,
                },
            ),
            (
                reconcile_disposition(false, false, false, false, true, false),
                ReconcileDiffCauses {
                    missing_open_orders: false,
                    missing_open_algo_orders: false,
                    unresolved_algo_orders: false,
                    orphan_positions: false,
                    unidentified_local_orders: true,
                    terminal_local_remote_active: false,
                },
            ),
            (
                reconcile_disposition(false, false, false, false, false, true),
                ReconcileDiffCauses {
                    missing_open_orders: false,
                    missing_open_algo_orders: false,
                    unresolved_algo_orders: false,
                    orphan_positions: false,
                    unidentified_local_orders: false,
                    terminal_local_remote_active: true,
                },
            ),
        ] {
            assert_eq!(actual, ReconcileDisposition::Degrade(expected));
        }
    }

    #[tokio::test]
    async fn t310_orphan_snapshot_creates_exact_active_position_state() {
        let (store, path) = test_store("orphan-position-state").await;
        let outcome = persist_orphan(&store, 10_000).await;
        assert_eq!(outcome.orphan_positions.len(), 1);
        let recovery = &outcome.orphan_positions[0];
        let row = sqlx::query(
            r#"
            SELECT position_id, credential_id, bot_uid, symbol_key, direction,
                   entry_epoch, state, active_key, qty, entry_price,
                   opened_at_ms,
                   CASE WHEN closed_at_ms IS NULL THEN 1 ELSE 0 END AS closed_at_is_null,
                   last_reconciled_ms, updated_at_ms
            FROM position_state WHERE credential_id='cred_t310'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load recovered orphan position");
        assert_eq!(
            serde_json::json!({
                "positionId": row.get::<String, _>("position_id"),
                "credentialId": row.get::<String, _>("credential_id"),
                "botUid": row.get::<String, _>("bot_uid"),
                "symbolKey": row.get::<String, _>("symbol_key"),
                "direction": row.get::<String, _>("direction"),
                "entryEpoch": row.get::<i64, _>("entry_epoch"),
                "state": row.get::<String, _>("state"),
                "activeKey": row.get::<String, _>("active_key"),
                "qty": row.get::<String, _>("qty"),
                "entryPrice": row.get::<String, _>("entry_price"),
                "openedAtMs": row.get::<i64, _>("opened_at_ms"),
                "closedAtIsNull": row.get::<i64, _>("closed_at_is_null"),
                "lastReconciledMs": row.get::<i64, _>("last_reconciled_ms"),
                "updatedAtMs": row.get::<i64, _>("updated_at_ms"),
            }),
            serde_json::json!({
                "positionId": recovery.position_id,
                "credentialId": "cred_t310",
                "botUid": recovery.bot_uid,
                "symbolKey": "binance|futures|BTC|USDT|USDT",
                "direction": "long",
                "entryEpoch": 1,
                "state": "open",
                "activeKey": format!("{}|binance|futures|BTC|USDT|USDT|long", recovery.bot_uid),
                "qty": "0.001",
                "entryPrice": "100",
                "openedAtMs": 10_000,
                "closedAtIsNull": 1,
                "lastReconciledMs": 10_000,
                "updatedAtMs": 10_000,
            })
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_orphan_recovery_never_arms_or_submits_flatten() {
        let (store, path) = test_store("orphan-no-flatten").await;
        let _ = persist_orphan(&store, 11_000).await;
        let disposition = reconcile_disposition(false, false, false, true, false, false);
        publish_reconcile_disposition(
            &store,
            "cred_t310",
            11_000,
            &disposition,
            serde_json::json!({"summary":{"hasDiff":true,"orphanPositionsCount":1}}),
        )
        .await
        .expect("publish the reconcile disposition");
        let order_count = sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n");
        let flatten_count =
            sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_state WHERE level='flatten'")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<i64, _>("n");
        assert_eq!(
            serde_json::json!({"engineOrders":order_count,"flattenScopes":flatten_count}),
            serde_json::json!({"engineOrders":0,"flattenScopes":0})
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_orphan_diff_emits_exact_reconcile_alert() {
        let (store, path) = test_store("orphan-alert").await;
        let disposition = reconcile_disposition(false, false, false, true, false, false);
        let payload = serde_json::json!({
            "source":"reconcile",
            "orphanPositions":[{"symbolKey":"binance|futures|BTC|USDT|USDT","direction":"long","qty":"0.001"}],
            "summary":{"hasDiff":true,"orphanPositionsCount":1}
        });
        publish_reconcile_disposition(&store, "cred_t310", 12_000, &disposition, payload.clone())
            .await
            .expect("publish the reconcile disposition");
        let row = sqlx::query(
            "SELECT event_type, payload_json FROM engine_order_events WHERE credential_id='cred_t310'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load reconcile alert");
        assert_eq!(row.get::<String, _>("event_type"), "reconcile_diff");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&row.get::<Vec<u8>, _>("payload_json"))
                .expect("decode reconcile alert"),
            payload
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_orphan_diff_sets_exact_reconcile_manage_only_health() {
        let (store, path) = test_store("orphan-manage-only").await;
        publish_reconcile_disposition(
            &store,
            "cred_t310",
            13_000,
            &reconcile_disposition(false, false, false, true, false, false),
            serde_json::json!({"summary":{"hasDiff":true,"orphanPositionsCount":1}}),
        )
        .await
        .expect("publish the reconcile disposition");
        let row = sqlx::query(
            r#"
            SELECT credential_id, degraded,
                   CASE WHEN degraded_reason IS NULL THEN 1 ELSE 0 END AS reason_is_null,
                   COALESCE(reconcile_diff_open, -1) AS reconcile_diff_open,
                   consecutive_errors, first_error_ms, last_error_ms, updated_at_ms
            FROM engine_credential_health WHERE credential_id='cred_t310'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load reconcile degradation");
        // **段③ 批 0（H-01/M-04）**：对账差异有了自己的列。这条用例此前
        // 钉的是 `degraded=1 / degraded_reason='reconcile_diff'`——那正是缺陷的
        // 形状：差异写在熔断器的槽位里。现在钉的是解耦后的形状：差异只写
        // `reconcile_diff_open`，熔断器那两列**一个字都不动**。
        assert_eq!(
            serde_json::json!({
                "credentialId":row.get::<String, _>("credential_id"),
                "degraded":row.get::<i64, _>("degraded"),
                "degradedReasonIsNull":row.get::<i64, _>("reason_is_null"),
                "reconcileDiffOpen":row.get::<i64, _>("reconcile_diff_open"),
                "consecutiveErrors":row.get::<i64, _>("consecutive_errors"),
                "firstErrorMs":row.get::<i64, _>("first_error_ms"),
                "lastErrorMs":row.get::<i64, _>("last_error_ms"),
                "updatedAtMs":row.get::<i64, _>("updated_at_ms"),
            }),
            serde_json::json!({
                "credentialId":"cred_t310",
                "degraded":0,
                "degradedReasonIsNull":1,
                "reconcileDiffOpen":1,
                "consecutiveErrors":0,
                "firstErrorMs":0,
                "lastErrorMs":0,
                "updatedAtMs":13_000,
            })
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_zero_snapshot_closes_recovered_orphan_and_preserves_l3_convergence() {
        let (store, path) = test_store("orphan-l3").await;
        let detected = persist_orphan(&store, 14_000).await;
        assert_eq!(
            serde_json::json!({
                "allPositionsZero":detected.all_positions_zero,
                "activeLocalPositions":detected.active_local_positions,
                "localPositionZeroDiff":detected.local_position_zero_diff,
                "orphanPositionsCount":detected.orphan_positions.len(),
            }),
            serde_json::json!({
                "allPositionsZero":false,
                "activeLocalPositions":1,
                "localPositionZeroDiff":true,
                "orphanPositionsCount":1,
            })
        );
        let flattened = persist_positions_and_balances(
            &store,
            "cred_t310",
            15_000,
            &serde_json::json!([position("BTCUSDT", "0.000", "BOTH")]),
            &balances(),
        )
        .await
        .expect("persist flattened truth");
        let truth = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: flattened.all_positions_zero,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: flattened.local_position_zero_diff,
        };
        assert_eq!(
            serde_json::json!({
                "allPositionsZero":flattened.all_positions_zero,
                "activeLocalPositions":flattened.active_local_positions,
                "localPositionZeroDiff":flattened.local_position_zero_diff,
                "orphanPositions":flattened.orphan_positions,
                "flattenComplete":truth.flatten_complete(),
            }),
            serde_json::json!({
                "allPositionsZero":true,
                "activeLocalPositions":0,
                "localPositionZeroDiff":true,
                "orphanPositions":[],
                "flattenComplete":true,
            })
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_multi_symbol_snapshot_recovers_each_orphan_independently() {
        let (store, path) = test_store("orphan-multi-symbol").await;
        let outcome = persist_positions_and_balances(
            &store,
            "cred_t310",
            16_000,
            &serde_json::json!([
                position("BTCUSDT", "0.001", "LONG"),
                position("ETHUSDT", "-0.25", "SHORT")
            ]),
            &balances(),
        )
        .await
        .expect("persist multi-symbol orphan truth");
        let actual = outcome
            .orphan_positions
            .iter()
            .map(|position| {
                serde_json::json!({
                    "symbolKey":position.symbol_key,
                    "direction":position.direction,
                    "qty":position.qty,
                    "recovery":position.recovery,
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                serde_json::json!({
                    "symbolKey":"binance|futures|BTC|USDT|USDT",
                    "direction":"long",
                    "qty":"0.001",
                    "recovery":"created_unowned",
                }),
                serde_json::json!({
                    "symbolKey":"binance|futures|ETH|USDT|USDT",
                    "direction":"short",
                    "qty":"0.25",
                    "recovery":"created_unowned",
                }),
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persist_positions_preserves_liquidation_price_and_maps_unknown_to_null() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let root = std::env::temp_dir().join(format!(
            "rustcore-reconcile-liquidation-{}",
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(&root).expect("create reconcile test directory");
        let balances = serde_json::json!([{
            "asset": "USDT",
            "balance": "10000",
            "availableBalance": "9000"
        }]);
        let engine_path = root.join("engine.sqlite");
        let store = Store::connect(&format!("sqlite://{}", engine_path.display()))
            .await
            .expect("connect engine database");

        let positions = serde_json::json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "1",
            "entryPrice": "100",
            "markPrice": "101",
            "leverage": "1",
            "unRealizedProfit": "1",
            "liquidationPrice": "50",
            "positionSide": "BOTH"
        }]);
        persist_positions_and_balances(&store, "cred_reconcile", 1_000, &positions, &balances)
            .await
            .expect("persist literal reconcile truth");
        let persisted = sqlx::query(
            "SELECT COALESCE(liquidation_price, '<NULL>') AS liquidation_price FROM engine_positions WHERE credential_id='cred_reconcile' AND symbol='BTCUSDT'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("liquidation_price");
        assert_eq!(persisted, "50");

        let missing = serde_json::json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "1",
            "entryPrice": "100",
            "markPrice": "101",
            "leverage": "1",
            "unRealizedProfit": "1",
            "positionSide": "BOTH"
        }]);
        persist_positions_and_balances(&store, "cred_reconcile", 2_000, &missing, &balances)
            .await
            .expect("persist missing liquidation truth");
        let persisted_missing = sqlx::query(
            "SELECT CASE WHEN liquidation_price IS NULL THEN 1 ELSE 0 END AS is_null FROM engine_positions WHERE credential_id='cred_reconcile' AND symbol='BTCUSDT'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("is_null");
        assert_eq!(persisted_missing, 1);

        for (ts, value) in [(3_000, ""), (4_000, "   \t")] {
            let blank = serde_json::json!([{
                "symbol": "BTCUSDT",
                "positionAmt": "1",
                "entryPrice": "100",
                "markPrice": "101",
                "leverage": "1",
                "unRealizedProfit": "1",
                "liquidationPrice": value,
                "positionSide": "BOTH"
            }]);
            persist_positions_and_balances(&store, "cred_reconcile", ts, &blank, &balances)
                .await
                .expect("persist blank liquidation truth");
            let persisted_blank = sqlx::query(
                "SELECT CASE WHEN liquidation_price IS NULL THEN 1 ELSE 0 END AS is_null FROM engine_positions WHERE credential_id='cred_reconcile' AND symbol='BTCUSDT'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("is_null");
            assert_eq!(persisted_blank, 1, "blank value {value:?} must be NULL");
        }

        store.pool().close().await;
        let _ = std::fs::remove_dir_all(root);
    }
    /// R5 FBL-R4-H3：`unconfirmed_order_is_adjudicable` 判别表。
    ///
    /// 三个条件缺一不可，三种放宽都会开出误释放面，故逐项钉住：
    /// ①已有 exchange_order_id（交易所确认过）不得裁决；
    /// ②`submitted`/`acked`/`partial_filled` 等已确认在途态不得裁决；
    /// ③宽限期内不得裁决（会与存活的在途提交竞争）。
    #[test]
    fn h3_only_unconfirmed_crash_residue_past_grace_is_adjudicable() {
        let now = 10_000_000i64;
        let stale = now - UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS;

        for status in ["unknown", "pending_risk", "pending_submit"] {
            assert!(
                unconfirmed_order_is_adjudicable(status, "", stale, now),
                "超期的崩溃残留态必须可裁决：{status}"
            );
            assert!(
                !unconfirmed_order_is_adjudicable(status, "", stale + 1, now),
                "宽限期内不得裁决：{status}"
            );
            assert!(
                !unconfirmed_order_is_adjudicable(status, "88991122", stale, now),
                "交易所已确认过（有单号）的订单不得按本路径裁决：{status}"
            );
        }
        for status in ["submitted", "acked", "partial_filled", "filled", "canceled"] {
            assert!(
                !unconfirmed_order_is_adjudicable(status, "", stale, now),
                "已确认/已终态的状态不属于本路径：{status}"
            );
        }
    }

    /// 裁决是**比较并置**。
    ///
    /// 裁决依据是在循环开头读到的（状态 + 无交易所单号）；到写入之间，一个滞留在
    /// limiter 里的 exit 提交可能刚好成功、把这行推进成 `acked` + 单号。无守卫的
    /// UPDATE 会把一张**活单**改写成 `failed`，清扫随后释放 closing——仓位被放开
    /// 而真实平仓单还挂在所上。本用例让交易所照常答 -2013，但在裁决前把行推进到
    /// `acked`，断言：不改写、不裁决、不释放。
    #[tokio::test]
    async fn h3_adjudication_is_compare_and_set_against_a_late_live_ack() {
        let (store, path) = test_store("h3-cas").await;

        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
              qty, entry_price, opened_at_ms, updated_at_ms, exit_epoch, pre_exit_state
            ) VALUES (
              'pos_h3cas', 'cred_h3cas', 'bot_h3cas', 'binance|futures|BTC|USDT|USDT', 'long',
              1, 'closing', 'bot_h3cas|binance|futures|BTC|USDT|USDT|long', 'int_h3cas', 'idem_h3cas_exit',
              '2', '100', 1, 1, 0, 'protected'
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed closing position");

        // 迟到的 live ack 已经落地：行已被推进为 acked 且带交易所单号。
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3cas', 'cred_h3cas', 'bot_h3cas', 'idem_h3cas_exit',
              'coid_h3cas', '778899',
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'acked', 'exit', 1, 1
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed late-acked exit order");

        // 直接驱动裁决函数，模拟「读到的是 unknown、写入时已变 acked」这一刻。
        let ctx = crate::svc::EventContext {
            bot_uid: "bot_h3cas".to_string(),
            run_id: String::new(),
            cycle_id: String::new(),
            idempotency_key: "idem_h3cas_exit".to_string(), // gitleaks:allow -- Synthetic test idempotency identifier.
            client_order_id_short: "coid_h3cas".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
        };
        let adjudicated = super::adjudicate_order_dead(
            &store,
            "cred_h3cas",
            "order_h3cas",
            &ctx,
            // 循环开头读到的是崩溃残留态
            "unknown",
            9_000,
            "exchange_order_not_found",
        )
        .await
        .expect("a CAS miss is not a DB error");
        assert!(
            !adjudicated,
            "行在读取之后被推进过，裁决必须落空（CAS 语义）"
        );

        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3cas'")
                .fetch_one(store.pool())
                .await
                .expect("load order")
                .get("status");
        assert_eq!(status, "acked", "活单不得被改写成 failed");

        // 清扫随后也不得释放：exit 仍在途。
        let mut tx = store.pool().begin().await.expect("begin sweep");
        let released = crate::position_state::sweep_closing_positions(&mut tx, "cred_h3cas", 9_100)
            .await
            .expect("sweep");
        tx.commit().await.expect("commit sweep");
        assert_eq!(released, 0, "活单在途时不得释放 closing");
        let state: String =
            sqlx::query("SELECT state FROM position_state WHERE position_id = 'pos_h3cas'")
                .fetch_one(store.pool())
                .await
                .expect("load position")
                .get("state");
        assert_eq!(state, "closing");

        cleanup(store, path).await;
    }

    /// CAS 的第二个条件
    /// 「仍无 exchange_order_id」独立于状态条件。
    ///
    /// 今天的 exit 路径不会造出「状态仍是崩溃残留态、却已带交易所单号」这一格：
    /// live ack 的 UPDATE 把 `exchange_order_id` 与 `status='acked'` 写在同一条语句里。
    /// 因此本用例**直接构造**该状态，钉的是守卫本身而不是某条生产路径——不把合成态
    /// 说成生产契约（R4 §5.1 的教训）。
    ///
    /// 之所以值得留这条守卫：仓内已存在只写单号、不动状态的 UPDATE 形状
    /// （`protection.rs` 的 `status = COALESCE(?, status)`）。哪天有路径先落单号
    /// 再落状态，无此守卫的裁决就会把一张活单改写成 `failed` 并让清扫放开仓位。
    #[tokio::test]
    async fn h3_adjudication_refuses_a_row_that_gained_an_exchange_id() {
        let (store, path) = test_store("h3-cas-eoid").await;
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3eo', 'cred_h3eo', 'bot_h3eo', 'idem_h3eo_exit',
              'coid_h3eo', '991122',
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'unknown', 'exit', 1, 1
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed order that gained an exchange id");

        let ctx = crate::svc::EventContext {
            bot_uid: "bot_h3eo".to_string(),
            run_id: String::new(),
            cycle_id: String::new(),
            idempotency_key: "idem_h3eo_exit".to_string(),
            client_order_id_short: "coid_h3eo".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
        };
        assert!(
            !super::adjudicate_order_dead(
                &store,
                "cred_h3eo",
                "order_h3eo",
                &ctx,
                "unknown",
                9_000,
                "exchange_order_not_found",
            )
            .await
            .expect("a CAS miss is not a DB error"),
            "已拿到交易所单号的订单不得被裁决为 failed（守卫的第二个条件）"
        );
        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3eo'")
                .fetch_one(store.pool())
                .await
                .expect("load order")
                .get("status");
        assert_eq!(status, "unknown", "状态不得被改写");

        cleanup(store, path).await;
    }

    /// CAS 守卫不得把**正常**裁决一起挡掉。
    /// 行仍停在读到的崩溃残留态且无交易所单号时，裁决必须照常发生。
    #[tokio::test]
    async fn h3_adjudication_still_fires_when_the_row_is_unchanged() {
        let (store, path) = test_store("h3-cas-ok").await;
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3ok', 'cred_h3ok', 'bot_h3ok', 'idem_h3ok_exit',
              'coid_h3ok', NULL,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'unknown', 'exit', 1, 1
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed unknown exit order");

        let ctx = crate::svc::EventContext {
            bot_uid: "bot_h3ok".to_string(),
            run_id: String::new(),
            cycle_id: String::new(),
            idempotency_key: "idem_h3ok_exit".to_string(),
            client_order_id_short: "coid_h3ok".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
        };
        assert!(
            super::adjudicate_order_dead(
                &store,
                "cred_h3ok",
                "order_h3ok",
                &ctx,
                "unknown",
                9_000,
                "exchange_order_not_found",
            )
            .await
            .expect("the adjudicating write must not fail"),
            "行未被推进时裁决必须照常发生（守卫不得误伤）"
        );
        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3ok'")
                .fetch_one(store.pool())
                .await
                .expect("load order")
                .get("status");
        assert_eq!(status, "failed");

        cleanup(store, path).await;
    }

    /// R11 反例用的本地在途单。
    async fn seed_inflight_order(
        store: &Store,
        credential_id: &str,
        engine_order_id: &str,
        status: &str,
        exchange_order_id: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r11', ?, ?, ?, 'binance', 'futures', ?,
              'open_long', 'long', 'market', '1', 0, ?, 'entry', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(credential_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(format!("coid-{engine_order_id}"))
        .bind(exchange_order_id)
        .bind(symbol_key)
        .bind(status)
        .execute(store.pool())
        .await
        .expect("seed inflight order");
    }

    async fn status_of(store: &Store, engine_order_id: &str) -> String {
        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = ?")
            .bind(engine_order_id)
            .fetch_one(store.pool())
            .await
            .expect("load order")
            .get::<String, _>("status")
    }

    fn open_order(symbol: &str, order_id: i64, client_order_id: &str, status: &str) -> String {
        serde_json::json!([{
            "orderId": order_id,
            "clientOrderId": client_order_id,
            "symbol": symbol,
            "status": status,
            "updateTime": 5
        }])
        .to_string()
    }

    /// **查询失败 ≠ 无在途单。**
    ///
    /// R10 这里是 `.unwrap_or_default()`：DB 故障与"没有在途单"产出同一个
    /// 空集合，对账因此照跑到底、零差异完成——一次失败被转换成了"一切正常"。
    #[tokio::test]
    async fn a_failed_inflight_lookup_is_not_an_empty_inflight_set() {
        let (store, path) = test_store("r11-inflight-error").await;
        sqlx::query("DROP TABLE engine_orders")
            .execute(store.pool())
            .await
            .expect("drop engine_orders to make the inflight lookup fail");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r11",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r11",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a failed inflight lookup must fail the reconcile, not report zero diff");
        assert!(
            format!("{error:#}").contains("inflight"),
            "the error must name the step that failed: {error:#}"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· open-order 索引必须带 symbol。**
    ///
    /// 交易所返回的是**另一个 symbol** 的开放单，只是 orderId 撞上了。
    /// R10 的裸 id map 会把它当成本地这张单的真值。
    #[tokio::test]
    async fn a_cross_symbol_open_order_must_not_claim_a_local_order() {
        let (store, path) = test_store("r11-open-symbol").await;
        seed_inflight_order(
            &store,
            "cred_r11",
            "order_x",
            "submitted",
            "5001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r11",
            vec![
                (
                    200,
                    open_order("ETHUSDT", 5001, "coid-other", "PARTIALLY_FILLED"),
                ),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                // 走到逐单求证时交易所权威地说"查无此单"；`submitted` 且已有
                // 交易所身份 → 不裁决，保持原状。
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_r11", store.clone(), limiter, &client)
            .await
            .expect("reconcile completes");

        assert_eq!(
            status_of(&store, "order_x").await,
            "submitted",
            "an open order of another symbol must not become this order's truth"
        );
        cleanup(store, path).await;
    }

    /// **放行面**：同 symbol 的开放单照常推进本地状态——
    /// 否则上一条用"永不认领"也能全绿。
    #[tokio::test]
    async fn a_same_symbol_open_order_still_advances_the_local_order() {
        let (store, path) = test_store("r11-open-same").await;
        seed_inflight_order(
            &store,
            "cred_r11",
            "order_x",
            "acked",
            "5001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r11",
            vec![
                (
                    200,
                    open_order("BTCUSDT", 5001, "coid-order_x", "PARTIALLY_FILLED"),
                ),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_r11", store.clone(), limiter, &client)
            .await
            .expect("reconcile completes");

        assert_eq!(status_of(&store, "order_x").await, "partial_filled");
        let events = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='order_update'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count events")
        .get::<i64, _>("n");
        assert_eq!(events, 1, "a real write-back must leave its audit event");
        cleanup(store, path).await;
    }

    /// **regression（High）· 写回失败不得被吞。**
    ///
    /// R10 的 `let _ = UPDATE` 让失败与成功不可区分，调用方紧接着照常
    /// `changed += 1` 并落审计事件：库里留下"对账把它改成 X 了"的事实，
    /// 而状态没动，且整轮对账仍报成功。
    #[tokio::test]
    async fn a_failed_status_write_back_fails_the_reconcile() {
        let (store, path) = test_store("r11-write-error").await;
        seed_inflight_order(
            &store,
            "cred_r11",
            "order_x",
            "acked",
            "5001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        sqlx::query(
            "CREATE TRIGGER r11_block_reconcile_status BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'partial_filled' \
             BEGIN SELECT RAISE(ABORT, 'injected reconcile write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r11",
            vec![
                (
                    200,
                    open_order("BTCUSDT", 5001, "coid-order_x", "PARTIALLY_FILLED"),
                ),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r11",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a swallowed write-back turns a failed reconcile into a successful one");
        assert!(format!("{error:#}").contains("write back the order status"));

        assert_eq!(status_of(&store, "order_x").await, "acked");
        let events = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='order_update'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count events")
        .get::<i64, _>("n");
        assert_eq!(events, 0);
        cleanup(store, path).await;
    }

    /// **regression（High）· 写回必须带旧状态 CAS 与完整偏序。**
    ///
    /// R10 的写回是裸 `UPDATE ... WHERE credential AND engine_order_id`：
    /// 交易所的一条迟到 `NEW` 能把本地 `partial_filled` 退回 `acked`，
    /// 而 WS 侧同一件事早已被完整偏序挡住——两条路径判据不同源。
    /// 审计事件也必须跟着不写：R10 无论写没写成都照样 `changed += 1` 并落事件。
    #[tokio::test]
    async fn reconcile_must_not_regress_a_stronger_local_status() {
        let (store, path) = test_store("r11-cas").await;
        seed_inflight_order(
            &store,
            "cred_r11",
            "order_x",
            "partial_filled",
            "5001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r11",
            vec![
                (200, open_order("BTCUSDT", 5001, "coid-order_x", "NEW")),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_r11", store.clone(), limiter, &client)
            .await
            .expect("reconcile completes");

        assert_eq!(
            status_of(&store, "order_x").await,
            "partial_filled",
            "a late NEW must not erase a partial fill that already happened"
        );
        let events = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='order_update'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count events")
        .get::<i64, _>("n");
        assert_eq!(
            events, 0,
            "a write that did not happen must not leave an audit trail saying it did"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 读到写之间被别的路径抢先时，不得谎报改写成功。**
    ///
    /// `rows_affected() == 0` 是 CAS 落空——状态没动。R10 无论写没写成都
    /// `changed += 1` 并落审计事件；改成 `Ok(true)` 的变异在上面三条用例下
    /// 489/489 全绿（实测存活），因为没有任何用例走过这条并发分支。
    ///
    /// 这里直接驱动写回函数本体，用一份**过期的** `current_status` 复现
    /// "读之后被抢先"：预检按过期值放行，SQL 里的 `status IN (...)` 落空。
    /// （接线由本模块另外三条对账用例覆盖，这一条只钉这条分支。）
    #[tokio::test]
    async fn a_lost_compare_and_set_is_not_reported_as_a_write() {
        let (store, path) = test_store("r11-cas-lost").await;
        seed_inflight_order(
            &store,
            "cred_r11",
            "order_x",
            "filled",
            "5001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let wrote = apply_reconciled_status(
            &store,
            "cred_r11",
            "order_x",
            // 过期读：对账开始时这一行还是 acked，写之前已被推进到 filled。
            "acked",
            "partial_filled",
            "5001",
            now_ms(),
        )
        .await
        .expect("a lost CAS is not an error");
        assert!(
            !wrote,
            "a compare-and-set that matched no row must not be reported as a write"
        );
        assert_eq!(
            status_of(&store, "order_x").await,
            "filled",
            "the stronger truth that won the race must survive"
        );
        cleanup(store, path).await;
    }

    /// R5 FBL-R4-H3 **复现契约**（端到端接线）：
    /// live close 提交失败 → `status='unknown'` 落库 → 释放事务前崩溃。
    /// 重启后 reconcile 必须：向交易所求证 → 拿到 `-2013`（查无此单）→ 裁决
    /// 为终态 `failed` → dead 集吸收 → 清扫释放 closing（epoch+1、清 pending 键）。
    ///
    /// R4 在这条路径上原样锁死：`unknown` 不在 dead 集、reconcile 对未上所
    /// 订单的 get_order 一律 `continue`，closing 永不释放、重试恒 duplicate。
    /// 本用例走的是**生产可达**的崩溃残留态，不是「订单行缺失」那种
    /// claim 与 insert 同事务因而生产不可达的合成态（评审 §5.1）。
    #[tokio::test]
    async fn h3_reconcile_adjudicates_unknown_exit_and_releases_closing() {
        let (store, path) = test_store("h3-adjudicate").await;
        let ts = now_ms();

        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
              qty, entry_price, opened_at_ms, updated_at_ms, exit_epoch, pre_exit_state
            ) VALUES (
              'pos_h3', 'cred_h3', 'bot_h3', 'binance|futures|BTC|USDT|USDT', 'long',
              1, 'closing', 'bot_h3|binance|futures|BTC|USDT|USDT|long', 'int_h3', 'idem_h3_exit',
              '2', '100', 1, 1, 0, 'open'
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed closing position");

        // 崩溃残留：提交失败写下 unknown（pool 自动提交），随后的释放事务没跑成。
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3', 'cred_h3', 'bot_h3', 'idem_h3_exit',
              'coid_h3', NULL,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'unknown', 'exit', 1, 1
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed unknown exit order");

        // 交易所应答序列：openOrders / openAlgoOrders / positionRisk / balance
        // 全空（刻意让仓位真值路径**不**参与释放，隔离出清扫这条通道），
        // 随后 get_order 返回权威的 -2013。
        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_h3",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));

        reconcile_binance_futures_truth_with_client("cred_h3", store.clone(), limiter, &client)
            .await
            .expect("reconcile with -2013 authority");

        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3'")
                .fetch_one(store.pool())
                .await
                .expect("load adjudicated order")
                .get("status");
        assert_eq!(
            status, "failed",
            "交易所查无此单 = 权威判死，订单必须转终态 failed"
        );

        let row = sqlx::query(
            "SELECT state, COALESCE(pending_idem_key, '') AS pending_idem_key, exit_epoch \
             FROM position_state WHERE position_id = 'pos_h3'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load swept position");
        assert_ne!(
            row.get::<String, _>("state"),
            "closing",
            "裁决之后必须在同一轮 reconcile 内释放 closing——否则锁死只是被推迟"
        );
        assert_eq!(row.get::<String, _>("pending_idem_key"), "");
        assert_eq!(
            row.get::<i64, _>("exit_epoch"),
            1,
            "释放必须自增 exit_epoch，下一次 close 才拿得到新键"
        );
        let _ = ts;

        cleanup(store, path).await;
    }

    /// R5 FBL-R4-H3（保守方向）：交易所**瞬断**不是「查无此单」。
    /// get_order 拿到 5xx/超时一类错误时，订单状态与 closing 必须原样不动——
    /// 把「问不到」当成「没有」就是误释放。
    #[tokio::test]
    async fn h3_transient_query_failure_never_adjudicates() {
        let (store, path) = test_store("h3-transient").await;

        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
              qty, entry_price, opened_at_ms, updated_at_ms, exit_epoch, pre_exit_state
            ) VALUES (
              'pos_h3t', 'cred_h3t', 'bot_h3t', 'binance|futures|BTC|USDT|USDT', 'long',
              1, 'closing', 'bot_h3t|binance|futures|BTC|USDT|USDT|long', 'int_h3t', 'idem_h3t_exit',
              '2', '100', 1, 1, 0, 'open'
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed closing position");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3t', 'cred_h3t', 'bot_h3t', 'idem_h3t_exit',
              'coid_h3t', NULL,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'unknown', 'exit', 1, 1
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("seed unknown exit order");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_h3t",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                // 交易所内部错误——不是权威的「查无此单」。
                (400, r#"{"code":-1001,"msg":"Internal error."}"#.to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));

        reconcile_binance_futures_truth_with_client("cred_h3t", store.clone(), limiter, &client)
            .await
            .expect_err("transient lookup failure must leave the round incomplete");

        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3t'")
                .fetch_one(store.pool())
                .await
                .expect("load order")
                .get("status");
        assert_eq!(status, "unknown", "瞬断不得被当成权威判死");
        let state: String =
            sqlx::query("SELECT state FROM position_state WHERE position_id = 'pos_h3t'")
                .fetch_one(store.pool())
                .await
                .expect("load position")
                .get("state");
        assert_eq!(state, "closing", "瞬断不得释放 closing");

        cleanup(store, path).await;
    }

    // ==================================================================
    // 返修用例
    // ==================================================================

    /// **regression（High）· 逐单回查不得退回裸身份。**
    ///
    /// 本地在途单声明的是 `okx|spot|BTC|USDT|USDT`。R11 的
    /// `symbol_from_symbol_key` 把它拼成 `BTCUSDT` 去问 **Binance USDⓈ-M**，
    /// 拿到的 `-2013`（另一个市场上当然查无此单）被当成权威判死，
    /// 于是这条本地单被裁决成 `failed`。
    ///
    /// 现在：一个字节都不问交易所（`recorded_requests` 里没有 `/order?`），
    /// 状态保持 `unknown`，并作为 `unidentifiedLocalOrders` 进 diff 驱动降级。
    #[tokio::test]
    async fn an_unidentifiable_symbol_key_is_never_queried_or_adjudicated() {
        let (store, path) = test_store("r12-unidentified").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_u1",
            "unknown",
            "",
            "okx|spot|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                // 这条**不该被消费**：一旦被消费，说明裸身份又回来了。
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let truth = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect("reconcile completes");

        assert_eq!(
            status_of(&store, "order_u1").await,
            "unknown",
            "an order of another market must never be adjudicated by USDⓈ-M truth"
        );
        assert!(
            !client
                .recorded_requests()
                .iter()
                .any(|request| request.contains("/fapi/v1/order?")),
            "the exchange must not be asked about a symbol this key does not own: {:?}",
            client.recorded_requests()
        );
        assert!(
            truth.has_diff,
            "an unreconcilable local order is a diff, not silence"
        );
        let payload = last_reconcile_diff_payload(&store, "cred_r12").await;
        assert_eq!(
            payload["summary"]["unidentifiedLocalOrdersCount"]
                .as_i64()
                .expect("count present"),
            1
        );
        assert_eq!(
            payload["unidentifiedLocalOrders"][0]["engineOrderId"]
                .as_str()
                .expect("engine order id present"),
            "order_u1"
        );
        cleanup(store, path).await;
    }

    /// **放行面（反向）**：同一形态但 key 是本引擎闭世界内的 USDⓈ-M 身份时，
    /// 逐单回查**照常发生**并按 `-2013` 裁决。没有这一条，上一条用
    /// "永远不查" 也能全绿。
    #[tokio::test]
    async fn a_usdm_symbol_key_is_still_queried_and_adjudicated() {
        let (store, path) = test_store("r12-identified").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_u1",
            "unknown",
            "",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_r12", store.clone(), limiter, &client)
            .await
            .expect("reconcile completes");

        assert_eq!(status_of(&store, "order_u1").await, "failed");
        assert!(
            client
                .recorded_requests()
                .iter()
                .any(|request| request.contains("symbol=BTCUSDT")),
            "the USDⓈ-M identity must still reach the exchange: {:?}",
            client.recorded_requests()
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· missing-local 集合必须带 symbol。**
    ///
    /// 交易所在 `ETHUSDT` 上有一张**本地根本不认识**的开放单，它的
    /// `orderId` 恰好与本地一张 `BTCUSDT` 单的交易所单号相同。R11 的裸 id
    /// 集合把它判成"本地已知" → 不进 missing → 零差异 → 降级不触发。
    #[tokio::test]
    async fn a_cross_symbol_id_collision_is_still_missing_locally() {
        let (store, path) = test_store("r12-missing-symbol").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_x",
            "acked",
            "6001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, open_order("ETHUSDT", 6001, "coid-order_x", "NEW")),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let truth = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect("reconcile completes");

        assert!(
            truth.has_diff,
            "an unowned exchange order must not be absorbed by a same-id order of another symbol"
        );
        let payload = last_reconcile_diff_payload(&store, "cred_r12").await;
        assert_eq!(
            payload["summary"]["missingLocalOpenOrdersCount"]
                .as_i64()
                .expect("count present"),
            1
        );
        assert_eq!(
            payload["missingLocalOpenOrders"][0]["symbol"]
                .as_str()
                .expect("symbol present"),
            "ETHUSDT"
        );
        cleanup(store, path).await;
    }

    /// **放行面（反向）**：同 symbol 同 id 时仍然算"本地已知"，
    /// 否则每一轮对账都会凭空报出一堆 missing。
    #[tokio::test]
    async fn a_same_symbol_id_is_still_known_locally() {
        let (store, path) = test_store("r12-missing-same").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_x",
            "acked",
            "6001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, open_order("BTCUSDT", 6001, "coid-order_x", "NEW")),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let truth = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect("reconcile completes");

        assert!(
            !truth.has_diff,
            "a local order that owns this symbol and id must still absorb the open order"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 裁决写的 DB 故障不是 CAS miss。**
    ///
    /// R11 的 `.execute(...).map(...).unwrap_or(false)` 把一次真正的写失败
    /// 折成"这行被别人推进过"，打出一条事实错误的 warn，本轮继续跑到底，
    /// 最后把 `ReconcileTruth` 当成一次成功对账交出去。
    #[tokio::test]
    async fn a_failed_adjudication_write_fails_the_whole_round() {
        let (store, path) = test_store("r12-adjudicate-error").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_u1",
            "unknown",
            "",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        sqlx::query(
            "CREATE TRIGGER r12_block_adjudication BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'failed' \
             BEGIN SELECT RAISE(ABORT, 'injected adjudication write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a DB error on the adjudicating write must fail the round");
        assert!(
            format!("{error:#}").contains("adjudicate"),
            "the error must name the step that failed: {error:#}"
        );

        assert_eq!(status_of(&store, "order_u1").await, "unknown");
        let diffs = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='reconcile_diff'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count reconcile_diff events")
        .get::<i64, _>("n");
        assert_eq!(
            diffs, 0,
            "a round that failed mid-way must not publish a clean/diff projection"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 权威写的 DB 故障不得被折成"一切正常"。**
    ///
    /// `reconcile_diff` 的降级置位是本轮结论的落点。R11 那里是 `let _ =`：
    /// 写失败之后本轮照样 `Ok(truth)`，调用方（kill-switch L3、user stream
    /// 恢复链）据此认为对账成功。
    #[tokio::test]
    async fn a_failed_degrade_write_fails_the_whole_round() {
        let (store, path) = test_store("r12-degrade-error").await;
        sqlx::query("DROP TABLE engine_credential_health")
            .execute(store.pool())
            .await
            .expect("drop the health table to make the degrade write fail");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, open_order("ETHUSDT", 6001, "coid-unknown", "NEW")),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a failed degrade write must fail the round");
        assert!(
            format!("{error:#}").contains("reconcile_diff_open"),
            "the error must name the step that failed: {error:#}"
        );
        cleanup(store, path).await;
    }

    /// **段③ 批 0（H-01）· "SQL 成功、零行被改"不得与成功不可区分。**
    ///
    /// `apply_reconcile_diff_open` 头上声明置位/解除是**权威写**，R11 把
    /// `let _ =` 改成上抛就是为了这句话。但上抛只覆盖 **DB 错误**——旧实现
    /// 丢弃 `rows_affected`，于是"SQL 成功、CASE 判假、一行都没改"这种语义
    /// 空转与成功逐字不可区分：一个已声明的不变量在自己的实现里不成立。
    ///
    /// 这里用 SQLite 的 `RAISE(IGNORE)` 触发器造出**恰好**那个状态：语句成功
    /// 返回、零行被改、库里没有这一行。本轮必须失败。
    #[tokio::test]
    async fn a_zero_row_authoritative_write_fails_the_whole_round() {
        let (store, path) = test_store("seg3b0-zero-row").await;
        for ddl in [
            "CREATE TRIGGER seg3b0_swallow_insert BEFORE INSERT ON engine_credential_health \
             BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER seg3b0_swallow_update BEFORE UPDATE ON engine_credential_health \
             BEGIN SELECT RAISE(IGNORE); END",
        ] {
            sqlx::query(ddl)
                .execute(store.pool())
                .await
                .expect("install the silently-swallowing trigger");
        }

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_seg3b0",
            vec![
                (200, open_order("ETHUSDT", 9101, "coid-unknown", "NEW")),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_seg3b0",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a write that changed no row must fail the round");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("changed no row") && rendered.contains("reconcile_diff_open"),
            "the error must name the authoritative write that silently did nothing: {rendered}"
        );
        let diffs = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='reconcile_diff'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count reconcile_diff events")
        .get::<i64, _>("n");
        assert_eq!(
            diffs, 0,
            "a round whose authoritative write did nothing must not publish a diff projection"
        );
        cleanup(store, path).await;
    }

    /// 上一条的**放行面**：零行被改也可能只是"新值与旧值逐字相同"
    /// （MySQL 的 `rows_affected` 在那种情况下同样报 0）。回读确认过确实
    /// 已是目标值时不得报错，否则这个不变量会把幂等重写变成故障。
    ///
    /// 判别力自证：同一套触发器下，库里的值**不是**目标值时必须报错。
    #[tokio::test]
    async fn a_zero_row_write_is_only_ok_when_the_stored_value_already_matches() {
        for (stored, has_diff, expect_ok) in [(1i64, true, true), (1i64, false, false)] {
            let (store, path) = test_store("seg3b0-idempotent-zero-row").await;
            sqlx::query(
                "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
                 consecutive_errors, first_error_ms, last_error_ms, updated_at_ms, \
                 reconcile_diff_open) VALUES ('cred_seg3b0', 0, NULL, 0, 0, 0, 1, ?)",
            )
            .bind(stored)
            .execute(store.pool())
            .await
            .expect("seed the health row");
            sqlx::query(
                "CREATE TRIGGER seg3b0_swallow_update BEFORE UPDATE ON engine_credential_health \
                 BEGIN SELECT RAISE(IGNORE); END",
            )
            .execute(store.pool())
            .await
            .expect("install the silently-swallowing update trigger");

            let exchange_open_orders = if has_diff {
                open_order("ETHUSDT", 9102, "coid-unknown", "NEW")
            } else {
                "[]".to_string()
            };
            let client = BinanceFuturesClient::new_with_mock_responses(
                "cred_seg3b0",
                vec![
                    (200, exchange_open_orders),
                    (200, "[]".to_string()),
                    (200, "[]".to_string()),
                    (200, balances().to_string()),
                ],
            );
            let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
            let outcome = reconcile_binance_futures_truth_with_client(
                "cred_seg3b0",
                store.clone(),
                limiter,
                &client,
            )
            .await;
            assert_eq!(
                outcome.is_ok(),
                expect_ok,
                "stored={stored} has_diff={has_diff}: {outcome:?}"
            );
            cleanup(store, path).await;
        }
    }

    /// **迁移 0029 的存量行复核**（`Store::connect` 里、迁移之后、任何对账
    /// 任务启动之前）。0029 零 DML：存量行本列是 `NULL`。被掩盖的差异不可从
    /// 库里恢复，所以两种结论都 fail-closed 置 1，区别只在**理由**。
    #[tokio::test]
    async fn legacy_null_reconcile_diff_rows_are_revalidated_fail_closed_with_verdicts() {
        let (store, path) = test_store("seg3b0-revalidate").await;
        for (credential_id, degraded, reason) in [
            ("cred_proven", 1i64, Some("reconcile_diff")),
            ("cred_cleared", 0i64, None),
            ("cred_other_reason", 1i64, Some("binance -1021 timestamp")),
        ] {
            sqlx::query(
                "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
                 consecutive_errors, first_error_ms, last_error_ms, updated_at_ms) \
                 VALUES (?, ?, ?, 0, 0, 0, 1)",
            )
            .bind(credential_id)
            .bind(degraded)
            .bind(reason)
            .execute(store.pool())
            .await
            .expect("seed a pre-0029 legacy health row");
        }
        // 对照组：0029 之后写下的行**不是**存量行，复核一个字都不许碰。
        sqlx::query(
            "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
             consecutive_errors, first_error_ms, last_error_ms, updated_at_ms, \
             reconcile_diff_open) VALUES ('cred_fresh', 0, NULL, 0, 0, 0, 1, 0)",
        )
        .execute(store.pool())
        .await
        .expect("seed a post-0029 row");

        assert_eq!(
            revalidate_legacy_credential_reconcile_diff(&store, 777)
                .await
                .expect("revalidate legacy rows"),
            3
        );
        assert_eq!(
            revalidated_state(&store).await,
            serde_json::json!([
                {"credentialId":"cred_cleared","reconcileDiffOpen":1,
                 "verdict":"unprovable","revalidatedAtMs":777},
                {"credentialId":"cred_fresh","reconcileDiffOpen":0,
                 "verdict":null,"revalidatedAtMs":null},
                {"credentialId":"cred_other_reason","reconcileDiffOpen":1,
                 "verdict":"unprovable","revalidatedAtMs":777},
                {"credentialId":"cred_proven","reconcileDiffOpen":1,
                 "verdict":"diff_open_proven","revalidatedAtMs":777},
            ])
        );

        // 幂等：跑过一次之后是空集，健康库上零写入。
        assert_eq!(
            revalidate_legacy_credential_reconcile_diff(&store, 888)
                .await
                .expect("re-run the revalidation"),
            0
        );
        assert_eq!(
            revalidated_state(&store).await,
            serde_json::json!([
                {"credentialId":"cred_cleared","reconcileDiffOpen":1,
                 "verdict":"unprovable","revalidatedAtMs":777},
                {"credentialId":"cred_fresh","reconcileDiffOpen":0,
                 "verdict":null,"revalidatedAtMs":null},
                {"credentialId":"cred_other_reason","reconcileDiffOpen":1,
                 "verdict":"unprovable","revalidatedAtMs":777},
                {"credentialId":"cred_proven","reconcileDiffOpen":1,
                 "verdict":"diff_open_proven","revalidatedAtMs":777},
            ])
        );
        cleanup(store, path).await;
    }

    /// **变异 M15 存活补测 · 复核不得越权覆盖对账已写下的真值。**
    ///
    /// `AND reconcile_diff_open IS NULL` 是 TOCTOU 守卫：外层 `SELECT` 取名单
    /// 与逐行 `UPDATE` 是两条语句，其间真正的权威写者（跑完一轮的对账）完全
    /// 可能已经把真值写进去了。守卫不在时，复核会把一个**刚刚被证明为 0**
    /// 的凭据重新按 fail-closed 抹成 1，白拦一轮开仓。
    ///
    /// 这里用 `AFTER UPDATE` 触发器**确定性地**造出那个窗口：复核盖章
    /// `cred_a` 的那一刻，`cred_b` 被"另一个写者"写成 0。
    #[tokio::test]
    async fn the_revalidation_never_overwrites_a_value_the_reconciler_already_wrote() {
        let (store, path) = test_store("seg3b0-revalidate-race").await;
        for credential_id in ["cred_a", "cred_b"] {
            sqlx::query(
                "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
                 consecutive_errors, first_error_ms, last_error_ms, updated_at_ms) \
                 VALUES (?, 0, NULL, 0, 0, 0, 1)",
            )
            .bind(credential_id)
            .execute(store.pool())
            .await
            .expect("seed a pre-0029 legacy health row");
        }
        sqlx::query(
            "CREATE TRIGGER seg3b0_concurrent_reconciler \
             AFTER UPDATE ON engine_credential_health \
             WHEN new.credential_id = 'cred_a' BEGIN \
               UPDATE engine_credential_health SET reconcile_diff_open = 0 \
               WHERE credential_id = 'cred_b'; END",
        )
        .execute(store.pool())
        .await
        .expect("install the concurrent-writer trigger");

        assert_eq!(
            revalidate_legacy_credential_reconcile_diff(&store, 999)
                .await
                .expect("revalidate legacy rows under a concurrent writer"),
            1,
            "the row the reconciler won must not be counted as revalidated"
        );
        assert_eq!(
            revalidated_state(&store).await,
            serde_json::json!([
                {"credentialId":"cred_a","reconcileDiffOpen":1,
                 "verdict":"unprovable","revalidatedAtMs":999},
                {"credentialId":"cred_b","reconcileDiffOpen":0,
                 "verdict":null,"revalidatedAtMs":null},
            ]),
            "real truth beats a fail-closed guess: cred_b must keep the reconciler's 0"
        );
        cleanup(store, path).await;
    }

    /// **状态盖章与证据写入同生共死。**
    ///
    /// 批 0 的实现把「盖章 `reconcile_diff_open`」与「写 revalidation 证据」
    /// 留在两条独立语句里：进程在两写之间崩溃会留下"已盖章、无证据"的终态，
    /// 且下次启动的选取集（`WHERE reconcile_diff_open IS NULL`）永远不再选中
    /// 该行——审计链缺口不可自愈（SEG3-REVIEW-R1 §3 的复现，此处固化为永久
    /// 回归）。用 `BEFORE INSERT` 触发器拒绝证据写入复现该窗口：失败后状态
    /// 必须回滚；故障清除后的下一次"启动"必须把状态与证据一起补齐。
    #[tokio::test]
    async fn the_status_stamp_rolls_back_when_the_evidence_write_fails() {
        let (store, path) = test_store("health-revalidation-atomicity").await;
        sqlx::query(
            "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
             consecutive_errors, first_error_ms, last_error_ms, updated_at_ms) \
             VALUES ('cred_x', 1, 'reconcile_diff', 0, 0, 0, 1)",
        )
        .execute(store.pool())
        .await
        .expect("seed a pre-0029 legacy health row");
        sqlx::query(
            "CREATE TRIGGER health_reject_evidence BEFORE INSERT ON \
             engine_credential_reconcile_diff_revalidations BEGIN \
               SELECT RAISE(ABORT, 'evidence write rejected'); END",
        )
        .execute(store.pool())
        .await
        .expect("install the evidence-rejecting trigger");

        let err = revalidate_legacy_credential_reconcile_diff(&store, 111)
            .await
            .expect_err("the injected evidence failure must fail the revalidation");
        assert!(
            format!("{err:#}").contains("evidence write rejected"),
            "unexpected error chain: {err:#}"
        );
        assert_eq!(
            revalidated_state(&store).await,
            serde_json::json!([
                {"credentialId":"cred_x","reconcileDiffOpen":-1,
                 "verdict":null,"revalidatedAtMs":null},
            ]),
            "the stamp must roll back with the failed evidence write — a stamped row \
             without evidence is an unproved terminal state"
        );

        // 模拟"下一次启动"：故障注入清除后重跑，状态与证据必须一起补齐。
        sqlx::query("DROP TRIGGER health_reject_evidence")
            .execute(store.pool())
            .await
            .expect("drop the evidence-rejecting trigger");
        assert_eq!(
            revalidate_legacy_credential_reconcile_diff(&store, 222)
                .await
                .expect("re-run after the injected fault cleared"),
            1
        );
        assert_eq!(
            revalidated_state(&store).await,
            serde_json::json!([
                {"credentialId":"cred_x","reconcileDiffOpen":1,
                 "verdict":"diff_open_proven","revalidatedAtMs":222},
            ]),
            "the next start must heal the stamp and the evidence together"
        );
        cleanup(store, path).await;
    }

    async fn revalidated_state(store: &Store) -> serde_json::Value {
        let rows = sqlx::query(
            "SELECT h.credential_id AS credential_id, \
                    COALESCE(h.reconcile_diff_open, -1) AS reconcile_diff_open, \
                    COALESCE(r.verdict, '') AS verdict, \
                    COALESCE(r.revalidated_at_ms, -1) AS revalidated_at_ms, \
                    COALESCE(LENGTH(r.evidence), 0) AS evidence_len \
             FROM engine_credential_health h \
             LEFT JOIN engine_credential_reconcile_diff_revalidations r \
               ON r.credential_id = h.credential_id \
             ORDER BY h.credential_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load revalidated state");
        serde_json::Value::Array(
            rows.into_iter()
                .map(|row| {
                    let verdict = row.get::<String, _>("verdict");
                    let at_ms = row.get::<i64, _>("revalidated_at_ms");
                    if !verdict.is_empty() {
                        assert!(
                            row.get::<i64, _>("evidence_len") > 0,
                            "every verdict must carry evidence"
                        );
                    }
                    serde_json::json!({
                        "credentialId": row.get::<String, _>("credential_id"),
                        "reconcileDiffOpen": row.get::<i64, _>("reconcile_diff_open"),
                        "verdict": if verdict.is_empty() {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(verdict)
                        },
                        "revalidatedAtMs": if at_ms < 0 {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(at_ms)
                        },
                    })
                })
                .collect(),
        )
    }

    /// **regression（High）· 列解码失败不是"这一列是空的/是 0"。**
    ///
    /// R11 这里整片是 `.try_get(...).ok().flatten().unwrap_or_default()` 与
    /// `.try_get("updated_at_ms").unwrap_or(0)`。两条折叠合起来正好满足
    /// `unconfirmed_order_is_adjudicable` 的全部条件（无交易所身份 +
    /// `now - 0` 必然超过宽限期），于是一条**解码不出来**的活单会被裁决成
    /// `failed` 并释放 closing。
    ///
    /// 故障是真的：SQLite 的列亲和性允许把一个 TEXT 值写进 `BIGINT` 列，
    /// 之后 `try_get::<i64>` 解码必然失败。
    #[tokio::test]
    async fn an_undecodable_inflight_row_fails_the_round_instead_of_adjudicating() {
        let (store, path) = test_store("r12-decode-error").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_u1",
            "unknown",
            "",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        sqlx::query(
            "UPDATE engine_orders SET updated_at_ms = 'not-a-timestamp' \
             WHERE engine_order_id = 'order_u1'",
        )
        .execute(store.pool())
        .await
        .expect("store a value this column cannot decode as i64");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
                (
                    400,
                    r#"{"code":-2013,"msg":"Order does not exist."}"#.to_string(),
                ),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_r12", store.clone(), limiter, &client)
            .await
            .expect_err("an undecodable inflight row must fail the round");
        assert_eq!(
            status_of(&store, "order_u1").await,
            "unknown",
            "a row we could not even decode must never be adjudicated dead"
        );
        cleanup(store, path).await;
    }

    /// **sqlx-i64 核查批· 宽限期内的在途单不得被裁决为死。**
    ///
    /// 与上一条同宗但**不是同一个**故障：上一条造的是"这一列解码不出来"，
    /// 这一条造的是"这一列解码得出来、但读回来的是另一个数"。
    /// 旧 sqlx 0.7.4 `Any` + SQLite 会把 `updated_at_ms` 截成低 32 位（毫秒 epoch
    /// → 7.2e8），`now - 7.2e8 ≈ 1.79e12` **恒**超过 60s 宽限期——
    /// 一张刚刚下出去、交易所还没确认的活单在**第一次**对账就满足
    /// `unconfirmed_order_is_adjudicable` 的全部条件，被裁决成 `failed`
    /// 并释放 closing。后果与 regression 逐字相同，只是成因换成了解码器。
    ///
    /// 本模块既有的在途单夹具一律写 `updated_at_ms = 1`，截断对 1 是恒等，
    /// 所以既有回归对这条缺陷完全失明。这里用**真实时钟**造夹具。
    #[tokio::test]
    async fn an_inflight_order_updated_one_second_ago_must_not_be_adjudicated_dead() {
        let (store, path) = test_store("i64-grace").await;
        seed_inflight_order(
            &store,
            "cred_i64",
            "order_fresh",
            "unknown",
            "",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        // 无交易所身份、无 client id = "claim 之后、落单之前崩了"这一类；
        // 它是否被判死**只**取决于宽限期，判别力不掺别的判据。
        let fresh_ms = crate::svc::now_ms() - 1_000;
        sqlx::query(
            "UPDATE engine_orders SET updated_at_ms = ?, client_order_id_short = '' \
             WHERE engine_order_id = 'order_fresh'",
        )
        .bind(fresh_ms)
        .execute(store.pool())
        .await
        .expect("stamp the order with a real millisecond epoch");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_i64",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        reconcile_binance_futures_truth_with_client("cred_i64", store.clone(), limiter, &client)
            .await
            .expect_err("a fresh order without query identity cannot prove a clean round");
        assert_eq!(
            status_of(&store, "order_fresh").await,
            "unknown",
            "an order updated 1s ago is inside the 60s grace window and must stay inflight"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 审计事件写失败也不得被吞。**
    ///
    /// R11 的 `emit_order_update` 是 `if let Ok(mut tx) = begin { let _ = …; let _ = commit }`：
    /// 调用方刚把一张单推进到交易所真值并 `changed += 1`，审计侧却可能什么都
    /// 没留下——"对账改过什么"这条事实凭空消失，而本轮照报成功。
    #[tokio::test]
    async fn a_failed_audit_event_write_fails_the_whole_round() {
        let (store, path) = test_store("r12-audit-error").await;
        seed_inflight_order(
            &store,
            "cred_r12",
            "order_x",
            "acked",
            "6001",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        sqlx::query(
            "CREATE TRIGGER r12_block_order_update_event BEFORE INSERT ON engine_order_events \
             FOR EACH ROW WHEN NEW.event_type = 'order_update' \
             BEGIN SELECT RAISE(ABORT, 'injected audit write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (
                    200,
                    open_order("BTCUSDT", 6001, "coid-order_x", "PARTIALLY_FILLED"),
                ),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a failed audit event write must fail the round");
        assert!(
            format!("{error:#}").contains("order_update audit event"),
            "the error must name the step that failed: {error:#}"
        );
        cleanup(store, path).await;
    }

    /// I4：readiness 只消费本地与交易所的两段收敛真值；L3 五段判据不变。
    #[test]
    fn i4_recovery_readiness_is_two_segment_convergence_not_flatten_completion() {
        let converged_non_flat = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        assert_eq!(
            recovery_reconcile_divergence(converged_non_flat),
            None,
            "a converged position with SL/TP orders must declare the handoff ready"
        );
        assert!(
            !converged_non_flat.flatten_complete(),
            "I4c: L3 flatten_complete must still require all five segments"
        );

        for (label, truth, marker) in [
            (
                "has_diff",
                crate::kill_switch::ReconcileTruth {
                    has_diff: true,
                    ..converged_non_flat
                },
                "has_diff=true",
            ),
            (
                "local_position_zero_diff",
                crate::kill_switch::ReconcileTruth {
                    local_position_zero_diff: false,
                    ..converged_non_flat
                },
                "local_position_zero_diff=false",
            ),
        ] {
            let reason = recovery_reconcile_divergence(truth).unwrap_or_else(|| {
                panic!("{label} must keep the fill handoff not-ready after a reconnect")
            });
            assert!(
                reason.contains(marker),
                "{label}: the failure reason must name the segment that blocked it: {reason}"
            );
            for removed in [
                "all_positions_zero",
                "all_open_orders_empty",
                "all_open_algo_orders_empty",
            ] {
                assert!(
                    !reason.contains(removed),
                    "readiness failure text must not contain removed segment {removed}: {reason}"
                );
            }
        }
    }

    /// I4b：新 criterion 接受 non-flat；既有 not-ready 风控分派原样保留。
    #[test]
    fn i4b_a_converged_non_flat_reconnect_is_ready_and_the_open_guard_is_unchanged() {
        let holding_a_position = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        assert_eq!(
            recovery_reconcile_divergence(holding_a_position),
            None,
            "non-flat is safe when local and exchange truth have converged"
        );

        use crate::protection::OrderRole;
        use crate::risk::{fill_recovery_guard_disposition, FillRecoveryGuardDisposition};
        assert_eq!(
            fill_recovery_guard_disposition("open_long", false, OrderRole::Entry),
            FillRecoveryGuardDisposition::RejectEntry
        );
        for (action, reduce_only, role) in [
            ("close_long", true, OrderRole::Exit),
            ("close_short", true, OrderRole::Exit),
            ("hold", false, OrderRole::Entry),
            ("open_long", false, OrderRole::ProtectionSl),
            ("open_long", false, OrderRole::ProtectionTp),
            ("open_long", true, OrderRole::Emergency),
        ] {
            assert_ne!(
                fill_recovery_guard_disposition(action, reduce_only, role),
                FillRecoveryGuardDisposition::RejectEntry,
                "not-ready must not block {action:?} reduce_only={reduce_only} role={role:?}"
            );
        }
    }

    /// **代际快照必须在**读交易所之前**
    /// 取——而且这条顺序要能被观察到。**
    ///
    /// TOCTOU 发生在"读完交易所"与"落库"之间：快照写在这一段之后，
    /// 它比较的就是自己跟自己，CAS 退化成恒等式。用例因此必须能把一次
    /// 并发 latch **插进这一段中间**——这里用 mock HTTP 的 per-request 钩子
    /// 做到，一步都不靠 sleep：
    ///
    /// 1. 先闩一轮（对账动身时看到的就是这一轮）；
    /// 2. 对账发出第一个交易所请求时，钩子**在响应之前**再闩一次
    ///    （新敞口出现了）；
    /// 3. 对账带着一份"全平"的真值回来落库。
    ///
    /// 这份真值观察的是第 1 轮，绝不能收敛第 2 轮。快照若被挪到交易所读取
    /// 之后，第 4 步就会把 `flatten_state` 写成 `flattened`，这条用例立刻变红。
    #[tokio::test]
    async fn the_generation_snapshot_is_taken_before_the_exchange_is_read() {
        let (store, path) = test_store("r14-toctou").await;
        let credential_id = "cred_r14toctou";
        let scope_key = format!("cred:{credential_id}");
        crate::kill_switch::latch_flatten_pending(
            &store,
            &scope_key,
            "auto_protection_timeout",
            "first exposure",
            10,
        )
        .await
        .expect("first latch");

        let hook_store = store.clone();
        let hook_scope = scope_key.clone();
        let latched_again = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_flag = latched_again.clone();
        let client = BinanceFuturesClient::new_with_mock_responses_and_hook(
            credential_id,
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
            std::sync::Arc::new(move |_path| {
                let store = hook_store.clone();
                let scope_key = hook_scope.clone();
                let flag = hook_flag.clone();
                Box::pin(async move {
                    // 只在第一个请求上闩一次：这一刻对账已经在路上了。
                    if flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    crate::kill_switch::latch_flatten_pending(
                        &store,
                        &scope_key,
                        "auto_protection_timeout",
                        "a new exposure while the reconcile is in flight",
                        11,
                    )
                    .await
                    .expect("second latch while the reconcile is in flight");
                })
            }),
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));

        let truth = reconcile_binance_futures_truth_with_client(
            credential_id,
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect("the reconcile itself succeeds");
        assert!(
            truth.flatten_complete(),
            "the fixture must return a flat truth, otherwise the assertion is vacuous"
        );
        assert!(
            latched_again.load(std::sync::atomic::Ordering::SeqCst),
            "the interleaved latch must really have run"
        );

        let flatten_state = sqlx::query(
            "SELECT COALESCE(flatten_state, '') AS flatten_state FROM kill_switch_state \
             WHERE scope_key=?",
        )
        .bind(&scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read the latch")
        .get::<String, _>("flatten_state");
        assert_eq!(
            flatten_state, "flattening",
            "a truth read before this exposure existed must not converge it"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 对账真值向 kill-switch 的投影是权威写。**
    ///
    /// R11 那里是 `tracing::warn!` 然后照常 `Ok(truth)`：L3 的
    /// `flatten_complete()` 会据此认为"已经收敛"，而 scope 里根本没写进去。
    #[tokio::test]
    async fn a_failed_kill_switch_projection_fails_the_whole_round() {
        let (store, path) = test_store("r12-scope-error").await;
        // ****：投影现在只写**有活 latch** 的 scope，
        // 而且只写本 revision 的 target。所以要先真的闩住 `cred:cred_r12`
        // ——否则这一轮走的是 `NoActiveLatch`，压根到不了那条权威写，
        // 用例测的就不再是"写失败要不要上抛"。
        // （`cred:` scope 的成员集合恒为它自己那个 credential，因此
        // `cred_r12` 一定是本 revision 的 target。）
        crate::kill_switch::latch_flatten_pending(
            &store,
            "cred:cred_r12",
            "auto_protection_timeout",
            "the exposure this round must converge",
            1,
        )
        .await
        .expect("latch the scope so the projection has a revision to write");
        // 投影现在带代际 CAS，读代际发生在**读交易所之前**。
        // 直接 `DROP TABLE kill_switch_state` 会让那一步先失败，测的就不再是
        // 投影写。改成只让**逐 credential 的投影写**必然 ABORT：
        // 代际快照照常取得到，失败点精确落在这一轮的权威写上。
        //
        // 触发器装在 latch **之后**：`arm_in_tx_with_round` 自己会在同一个
        // 事务里 INSERT 本 revision 的 target 行（`reset_reconcile_targets_in_tx`），
        // 先装触发器会把 latch 本身打红，失败点就跑偏了。
        sqlx::query(
            "CREATE TRIGGER r14_block_scope_projection \
             BEFORE UPDATE ON kill_switch_reconcile_status \
             FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install a trigger that makes the projection write fail");

        let client = BinanceFuturesClient::new_with_mock_responses(
            "cred_r12",
            vec![
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, balances().to_string()),
            ],
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(50.0, 50.0));
        let error = reconcile_binance_futures_truth_with_client(
            "cred_r12",
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect_err("a failed kill-switch projection must fail the round");
        assert!(
            format!("{error:#}").contains("kill-switch scope"),
            "the error must name the step that failed: {error:#}"
        );
        cleanup(store, path).await;
    }

    include!("reconcile_terminal_conflict_tests.rs");

    async fn last_reconcile_diff_payload(store: &Store, credential_id: &str) -> serde_json::Value {
        let payload = crate::store::try_get_nullable_bytes(
            &sqlx::query(
                "SELECT payload_json FROM engine_order_events \
                 WHERE credential_id = ? AND event_type = 'reconcile_diff' \
                 ORDER BY seq DESC LIMIT 1",
            )
            .bind(credential_id)
            .fetch_one(store.pool())
            .await
            .expect("load the reconcile_diff event"),
            "payload_json",
        )
        .expect("decode payload column")
        .expect("payload present");
        serde_json::from_slice(&payload).expect("payload is JSON")
    }
}

#[cfg(all(test, feature = "l4-fault-injection"))]
mod fault_injection_tests {
    #[tokio::test]
    async fn fake_exchange_position_risk_includes_liquidation_price() {
        let root =
            std::env::temp_dir().join(format!("rustcore-fake-position-risk-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).expect("create fake exchange test directory");
        let fake_path = root.join("fake.sqlite");
        std::fs::File::create(&fake_path).expect("create fake exchange database");
        let fake_pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", fake_path.display()))
            .await
            .expect("connect fake exchange database");
        sqlx::query("CREATE TABLE positions (symbol TEXT PRIMARY KEY, quantity TEXT NOT NULL)")
            .execute(&fake_pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE request_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, method TEXT NOT NULL, path TEXT NOT NULL, order_type TEXT NOT NULL DEFAULT '')",
        )
        .execute(&fake_pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO positions(symbol, quantity) VALUES ('BTCUSDT', '1')")
            .execute(&fake_pool)
            .await
            .unwrap();
        fake_pool.close().await;

        let positions = crate::l4_fake_exchange::positions(&fake_path)
            .await
            .expect("read socket-free fake positionRisk truth");
        assert_eq!(positions[0]["liquidationPrice"], "50");

        // Persistence semantics are covered by the default-feature test module above;
        // this opt-in test only owns the fake /fapi/v2/positionRisk response contract.
        let _ = std::fs::remove_dir_all(root);
    }

    /// R5 FBL-R4-H3 的**验收 VM 演练载体**（socket-free L4 故障注入）。
    ///
    /// 与默认特性下那条端到端用例的区别：这条走的是 `l4_fake_exchange` 这只
    /// 仓内既有的、由 SQLite 承载的交易所替身——`get_order` 查不到订单时返回
    /// **逐字节真实**的 `binance http 400: {"code":-2013,...}` 报文，因此
    /// `-2013 → failed → dead 集 → 清扫释放 closing` 整条链走的是生产同一份
    /// reconcile 代码，且落在持久 SQLite 上（验收 VM 上即 rustcore.db 的同构副本）。
    ///
    /// 边界（如实声明）：交易所侧是 fake exchange，不是真实 Binance；本用例
    /// 需 `--features l4-fault-injection` 且**必须单独过滤运行**——它设置的
    /// L4_* 进程级环境变量会让同进程内其他用例也走替身。
    #[tokio::test]
    async fn h3_vm_drill_unknown_exit_converges_via_fake_exchange_2013() {
        use crate::rate_limiter::{CredentialRateLimiter, TokenBucketConfig};
        use sqlx::Row as _;
        static INSTALL_DRIVERS: std::sync::Once = std::sync::Once::new();

        let root = std::env::temp_dir().join(format!("rustcore-h3-drill-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).expect("create drill directory");

        // ① 交易所替身：orders 表**故意不含**这把 client_order_id ⇒ get_order 返回 -2013。
        let fake_path = root.join("fake-exchange.sqlite");
        std::fs::File::create(&fake_path).expect("create fake exchange database");
        let fake_pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", fake_path.display()))
            .await
            .expect("connect fake exchange database");
        for ddl in [
            "CREATE TABLE positions (symbol TEXT PRIMARY KEY, quantity TEXT NOT NULL)",
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY AUTOINCREMENT, client_order_id TEXT NOT NULL DEFAULT '', symbol TEXT NOT NULL DEFAULT 'BTCUSDT', status TEXT NOT NULL DEFAULT 'NEW', side TEXT NOT NULL DEFAULT 'BUY', orig_qty TEXT NOT NULL DEFAULT '0', executed_qty TEXT NOT NULL DEFAULT '0', order_type TEXT NOT NULL DEFAULT 'MARKET', update_time INTEGER NOT NULL DEFAULT 0, is_open INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE algo_orders (algo_id INTEGER PRIMARY KEY AUTOINCREMENT, client_algo_id TEXT NOT NULL DEFAULT '', symbol TEXT NOT NULL DEFAULT 'BTCUSDT', algo_status TEXT NOT NULL DEFAULT 'NEW', side TEXT NOT NULL DEFAULT 'SELL', orig_qty TEXT NOT NULL DEFAULT '0', executed_qty TEXT NOT NULL DEFAULT '0', order_type TEXT NOT NULL DEFAULT 'STOP_MARKET', update_time INTEGER NOT NULL DEFAULT 0, is_open INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE request_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, method TEXT NOT NULL, path TEXT NOT NULL, order_type TEXT NOT NULL DEFAULT '')",
        ] {
            sqlx::query(ddl).execute(&fake_pool).await.expect("fake ddl");
        }
        // 仓位仍开着（qty>0）——truth 说仓位没平，closing 只可能靠清扫释放。
        sqlx::query("INSERT INTO positions(symbol, quantity) VALUES ('BTCUSDT', '1')")
            .execute(&fake_pool)
            .await
            .expect("seed fake position");
        fake_pool.close().await;

        // ② 权威库：与验收 VM 的 rustcore.db 同一套迁移建出的持久 SQLite。
        let store_path = root.join("rustcore-drill.db");
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let store = crate::store::Store::connect(&format!("sqlite://{}", store_path.display()))
            .await
            .expect("connect drill store");

        let credential_id = "l4h3drill";
        // 崩溃残留：live close 提交失败写下 unknown（pool 自动提交），随后的
        // 释放事务没跑成；updated_at_ms 远早于宽限期。
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
              qty, entry_price, opened_at_ms, updated_at_ms, exit_epoch, pre_exit_state
            ) VALUES (
              'pos_h3drill', ?, 'bot_h3drill', 'binance|futures|BTC|USDT|USDT', 'long',
              1, 'closing', 'bot_h3drill|binance|futures|BTC|USDT|USDT|long', 'int_h3drill', 'idem_h3drill_exit',
              '1', '100', 1, 1, 0, 'protected'
            )
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed closing position");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3drill', ?, 'bot_h3drill', 'idem_h3drill_exit',
              'coid_h3drill', NULL,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '1', 1, 'unknown', 'exit', 1, 1
            )
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed unknown exit order");

        // ③ 打开故障注入通道（进程级；本用例必须单独过滤运行）。
        std::env::set_var("L4_FAULT_INJECTION_ENABLED", "1");
        std::env::set_var("L4_FAKE_EXCHANGE_DB", &fake_path);
        std::env::set_var("L4_FILE_RPC_DIR", &root);
        std::env::set_var(crate::venue::venue_env_name(credential_id), "testnet");

        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(200.0, 200.0));
        super::reconcile_binance_futures(
            credential_id,
            store.clone(),
            limiter,
            crate::binance_futures::BinanceKeys {
                api_key: "l4-drill-key".to_string(),
                api_secret: "l4-drill-secret".to_string(),
            },
        )
        .await
        .expect("reconcile against fake exchange");

        // ④ 断言整条链。
        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = 'order_h3drill'")
                .fetch_one(store.pool())
                .await
                .expect("load order")
                .get("status");
        assert_eq!(
            status, "failed",
            "交易所答复 -2013（查无此单）必须把崩溃残留裁决为终态"
        );

        let row = sqlx::query(
            "SELECT state, COALESCE(pending_idem_key,'') AS pending_idem_key, exit_epoch \
             FROM position_state WHERE position_id = 'pos_h3drill'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load position");
        assert_eq!(
            row.get::<String, _>("state"),
            "protected",
            "释放必须据实回到 pre_exit_state（protected，不是一律回落 open）"
        );
        assert_eq!(row.get::<String, _>("pending_idem_key"), "");
        assert_eq!(row.get::<i64, _>("exit_epoch"), 1);

        // ⑤ 收敛证明：仓位可以**重新平**——锁死确实解除，而不只是状态字段变了。
        let reclose = crate::proto::OrderIntent {
            bot_uid: "bot_h3drill".to_string(),
            run_id: "run_h3drill".to_string(),
            cycle_id: "cycle_h3drill_reclose".to_string(),
            idempotency_key: "idem_h3drill_reclose".to_string(),
            client_order_id_short: "coid_h3drill_re".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: "close_long".to_string(),
            side: "long".to_string(),
            order_type: "market".to_string(),
            price: "100".to_string(),
            quantity: "1".to_string(),
            reduce_only: true,
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        };
        let mut tx = store.pool().begin().await.expect("begin re-close");
        let outcome =
            crate::position_state::prepare_intent(&mut tx, credential_id, &reclose, 9_000)
                .await
                .expect("re-prepare close");
        tx.commit().await.expect("commit re-close");
        assert!(
            matches!(outcome, crate::position_state::PrepareOutcome::Ready(_)),
            "释放后必须能重新提交平仓（锁死解除），实际 {outcome:?}"
        );

        for name in [
            "L4_FAULT_INJECTION_ENABLED",
            "L4_FAKE_EXCHANGE_DB",
            "L4_FILE_RPC_DIR",
        ] {
            std::env::remove_var(name);
        }
        std::env::remove_var(crate::venue::venue_env_name(credential_id));
        store.pool().close().await;
        let _ = std::fs::remove_dir_all(root);
    }
}

/// **批 0 §7.4「机制就绪、非实证」清单的两项真库补证（2026-08-28）。**
///
/// 批 0 报告 §7.4 登记的两个缺口，都只能在真实 MySQL 上闭：
/// ① `rows_affected == 0` 的**生产成因**是 MySQL 对"新旧值逐列相同"的
///   UPDATE 报 0 行——SQLite 上只能用 `RAISE(IGNORE)` 触发器模拟，那条
///   具体路径从未在 MySQL 上实测（M5/M16/M17 变异只证了分支可达）；
/// ② 启动复核的**升级路径**（一个装着 0028 终态存量行的真实库 `Store::connect`
///   启动一次）未在 MySQL 上演练——0029 加列后存量行是 `NULL`，复核要在
///   真实 DDL 隐式提交之后把它们盖章成显式 1 并留证。
///
/// 环境缺失时按 engine safety contract 如实跳过，不以 SQLite 冒充；
/// `RUSTCORE_REQUIRE_MYSQL_TESTS=1` 把跳过变成失败。
#[cfg(test)]
mod health_writeback_real_mysql_tests {
    use std::sync::Once;

    use sqlx::any::{install_default_drivers, AnyPoolOptions};
    use sqlx::Row;

    use super::{apply_reconcile_diff_open, revalidate_legacy_credential_reconcile_diff};
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
    const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";

    fn skip_without_mysql(test: &str, why: &str) {
        if std::env::var(REQUIRE_MYSQL_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false)
        {
            panic!("{test}: {REQUIRE_MYSQL_ENV}=1 要求真实 MySQL，但 {why}");
        }
        eprintln!(
            "SKIPPED {test}: 没有可用的真实 MySQL（{why}）。\
             本用例只以真实 MySQL 的成因路径为证据，不以降级替身冒充。\
             设 {MYSQL_URL_ENV} 后可实跑。"
        );
    }

    fn admin_url_or_skip(test: &str) -> Option<String> {
        INSTALL_DRIVERS.call_once(install_default_drivers);
        match std::env::var(MYSQL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
            _ => {
                skip_without_mysql(test, &format!("{MYSQL_URL_ENV} is not set"));
                None
            }
        }
    }

    fn swap_database(url: &str, database: &str) -> String {
        let (prefix, _) = url.rsplit_once('/').expect("url carries a database path");
        format!("{prefix}/{database}")
    }

    /// 建一个空 scratch 库并返回 (admin_url, database, app_url)。
    async fn scratch_database(label: &str) -> Option<(String, String, String)> {
        let test = label;
        let admin_url = admin_url_or_skip(test)?;
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("connect the scratch admin");
        let database = format!(
            "rustcore_health_writeback_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("create the scratch database");
        admin.close().await;
        let app_url = swap_database(&admin_url, &database);
        Some((admin_url, database, app_url))
    }

    /// 把仓内迁移目录复制成**只到 `max_version`** 的目录。
    ///
    /// R1-006 的教训：按文件名前缀排除会随迁移前移漂移（"只到 0026"的夹具
    /// 实际复制了 0028/0029）。这里按解析出的版本号截断，并断言目录里真的
    /// 存在 `max_version`，缺失即大声失败。
    fn migration_dir_up_to(label: &str, max_version: i64) -> std::path::PathBuf {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let target = std::env::temp_dir().join(format!(
            "rustcore-health-writeback-{label}-{}",
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(&target).expect("create the truncated migration directory");
        let mut copied_max: Option<i64> = None;
        for entry in std::fs::read_dir(&source).expect("read the migration directory") {
            let path = entry.expect("read a migration entry").path();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("migration file name")
                .to_string();
            if !name.ends_with(".sql") {
                continue;
            }
            let version: i64 = name
                .split_once('_')
                .expect("migration file name carries a version prefix")
                .0
                .parse()
                .unwrap_or_else(|_| panic!("unparseable migration version in {name}"));
            if version > max_version {
                continue;
            }
            std::fs::copy(&path, target.join(&name)).expect("copy a migration file");
            copied_max = Some(copied_max.map_or(version, |prior: i64| prior.max(version)));
        }
        assert_eq!(
            copied_max,
            Some(max_version),
            "the truncated migration directory must end exactly at {max_version}"
        );
        target
    }

    /// ① `rows_affected == 0` 的**生产栈真相**（批 0 §7.4 登记项的实跑结论）：
    ///
    /// raw MySQL 协议默认报 **changed** 行数——"新旧值逐列相同 ⇒ 0 行"，批 0
    /// 据此设计了回读分支。但 sqlx-mysql 0.7.4 与精确锁定的 0.8.6 都
    /// **无条件**协商 `CLIENT_FOUND_ROWS`（`connection/stream.rs`），UPDATE 的
    /// `rows_affected()` 从此报 **matched** 行数。因此：
    /// - 幂等重写（逐列同值）：matched=1 ≠ 0，回读分支不触发，函数直接 Ok——
    ///   结果正确（值本就等于目标），本用例在真实 MySQL 上钉住这条路径；
    /// - "matched=1 但存储值 ≠ 目标"（BEFORE UPDATE 触发器把 NEW 改回 OLD，
    ///   实测 `ROW_COUNT()`=0、两列未变）在生产栈上**同样返回 Ok**——语义
    ///   空转检测在 MySQL 上结构性不可达（改回 OLD 后 changed 信号在
    ///   FOUND_ROWS 握手下不传回）。生产库没有这类触发器，且无触发器时
    ///   UPDATE 的 matched ⟹ set 语义本身保证值已写；SQLite 侧（真实报
    ///   changed）继续钉回读分支逻辑。此边界登记台账。
    #[tokio::test]
    async fn mysql_production_stack_reports_matched_rows_and_the_noop_rewrite_stays_ok() {
        let test = "mysql_noop_update_zero_row_cause";
        let Some((admin_url, database, url)) = (scratch_database(test).await) else {
            return;
        };
        let store = Store::connect(&url)
            .await
            .expect("connect the scratch store");

        // 正向：同一 (ts, target) 的第二次权威写是逐列同值 UPDATE——
        // MySQL 报 0 行，回读确认 0 == 0，必须 Ok。
        apply_reconcile_diff_open(&store, "cred_noop", 777, false)
            .await
            .expect("first write inserts the row");
        apply_reconcile_diff_open(&store, "cred_noop", 777, false)
            .await
            .expect("the identical rewrite reports 0 rows on MySQL and the readback resolves it");
        let stored: i64 = sqlx::query(
            "SELECT reconcile_diff_open FROM engine_credential_health \
             WHERE credential_id = 'cred_noop'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load cred_noop")
        .get("reconcile_diff_open");
        assert_eq!(stored, 0);

        // 生产栈边界：BEFORE UPDATE 触发器把两个被写列都改回 OLD（docker mysql
        // 实测 `ROW_COUNT()`=0、两列未变——raw 协议的 changed 信号）。生产栈
        // FOUND_ROWS 握手下 sqlx 报 matched=1，回读分支不触发，函数返回 Ok。
        // 钉住这一现状，防止后人误以为该分支在 MySQL 上活着（见本测试 doc）。
        // MySQL 的 CREATE TRIGGER 不走 prepared 协议（error 1295），必须 raw_sql。
        sqlx::raw_sql(
            "CREATE TRIGGER health_revert_write BEFORE UPDATE ON engine_credential_health \
             FOR EACH ROW SET NEW.reconcile_diff_open = OLD.reconcile_diff_open, \
             NEW.updated_at_ms = OLD.updated_at_ms",
        )
        .execute(store.pool())
        .await
        .expect("install the revert trigger");
        apply_reconcile_diff_open(&store, "cred_noop", 888, true)
            .await
            .expect(
                "under sqlx FOUND_ROWS the semantic-no-op branch is structurally dead on MySQL",
            );
        let after_revert: i64 = sqlx::query(
            "SELECT reconcile_diff_open FROM engine_credential_health \
             WHERE credential_id = 'cred_noop'",
        )
        .fetch_one(store.pool())
        .await
        .expect("reload cred_noop after the reverted write")
        .get("reconcile_diff_open");
        assert_eq!(
            after_revert, 0,
            "the revert trigger keeps the stored value at the pre-write truth"
        );

        store.pool().close().await;
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("reconnect the scratch admin");
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
        admin.close().await;
    }

    /// ② 启动复核的升级路径：真实库推到 0028 终态、装上存量行（无
    /// `reconcile_diff_open` 列），`Store::connect` 一次完成 0029 迁移 +
    /// 复核——存量 NULL 被盖章成显式 1 并留下证据，健康新行不受影响。
    #[tokio::test]
    async fn mysql_upgrade_path_stamps_legacy_rows_through_real_store_connect() {
        let test = "mysql_upgrade_path_real_connect";
        let Some((admin_url, database, url)) = (scratch_database(test).await) else {
            return;
        };
        let dir = migration_dir_up_to(test, 28);
        let pre = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect the pre-0029 scratch database");
        sqlx::migrate::Migrator::new(dir.as_path())
            .await
            .expect("load the pre-0029 migrator")
            .run(&pre)
            .await
            .expect("run migrations up to 0028 on MySQL");
        for (credential_id, degraded, reason) in [
            ("cred_proven", 1i64, Some("reconcile_diff")),
            ("cred_unprovable", 0i64, None),
        ] {
            sqlx::query(
                "INSERT INTO engine_credential_health (credential_id, degraded, \
                 degraded_reason, consecutive_errors, first_error_ms, last_error_ms, \
                 updated_at_ms) VALUES (?, ?, ?, 0, 0, 0, 1)",
            )
            .bind(credential_id)
            .bind(degraded)
            .bind(reason)
            .execute(&pre)
            .await
            .expect("seed a legacy pre-0029 health row");
        }
        pre.close().await;

        // 真实启动：迁移 0029 + 启动复核一次完成（这是批 0 缺的那个"启动一次"）。
        let store = Store::connect(&url)
            .await
            .expect("connect through the upgrade path");

        let rows = sqlx::query(
            "SELECT h.credential_id AS credential_id, h.reconcile_diff_open AS diff_open, \
             r.verdict AS verdict, r.evidence AS evidence \
             FROM engine_credential_health h \
             LEFT JOIN engine_credential_reconcile_diff_revalidations r \
             ON r.credential_id = h.credential_id \
             WHERE h.credential_id IN ('cred_proven', 'cred_unprovable') \
             ORDER BY h.credential_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load the upgraded rows with their evidence");
        assert_eq!(rows.len(), 2, "both legacy rows must survive the upgrade");
        // evidence 是 TEXT 列：MySQL 按 BLOB 返回，必须走 try_get_text。
        let proven = &rows[0];
        assert_eq!(proven.get::<i64, _>("diff_open"), 1);
        assert_eq!(proven.get::<String, _>("verdict"), "diff_open_proven");
        let proven_evidence =
            crate::store::try_get_text(proven, "evidence").expect("read proven evidence");
        assert!(proven_evidence.is_some_and(|value| !value.is_empty()));
        let unprovable = &rows[1];
        assert_eq!(unprovable.get::<i64, _>("diff_open"), 1);
        assert_eq!(unprovable.get::<String, _>("verdict"), "unprovable");
        let unprovable_evidence = crate::store::try_get_text(unprovable, "evidence")
            .expect("read unprovable evidence")
            .expect("unprovable evidence must exist");
        // **SEG1-RUST-2**：该行 degraded_reason 是 NULL——证据必须显式携带
        // <NULL> 哨兵，不得伪装成空串。
        assert!(
            unprovable_evidence.contains("degraded_reason=<NULL>"),
            "NULL must stay distinguishable from an empty string, got: {unprovable_evidence}"
        );

        // 幂等（真库）：复核重跑是空集。
        assert_eq!(
            revalidate_legacy_credential_reconcile_diff(&store, 999)
                .await
                .expect("re-run the revalidation on the upgraded database"),
            0
        );

        store.pool().close().await;
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("reconnect the scratch admin");
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
        admin.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
#[path = "account_order_reconcile_tests.rs"]
mod account_order_observation_tests;

#[cfg(test)]
mod b3_exact_close_reopen_tests {
    use crate::position_close_tests::{Fixture, BTC};
    use sqlx::Row;
    #[tokio::test]
    async fn b3_production_reopen_increments_revision_and_removes_prior_entry_proof() {
        let f = Fixture::new().await;
        let position = f.open("paper", "reconcile_reopen", false, BTC).await;
        let target = f.target("paper", &position).await.target.unwrap();
        let request = f.close_request("paper", &position).await;
        let receipt = f.close(request).await;
        assert_eq!(receipt.request_status, "accepted");
        let before = receipt.execution_outcome.unwrap().revision;
        let mut tx = f.store.pool().begin().await.unwrap();
        let recovered = super::recover_orphan_position(
            &mut tx,
            "paper",
            "7",
            Some("110"),
            crate::svc::now_ms(),
            super::OrphanPositionDisposition::ReopenClosed {
                position_id: position.clone(),
                bot_uid: target.bot_uid,
                symbol_key: BTC.into(),
                direction: "long".into(),
                entry_epoch: target.entry_epoch as i64,
            },
        )
        .await
        .unwrap();
        assert!(recovered.is_some());
        tx.commit().await.unwrap();
        let row =
            sqlx::query("SELECT close_revision,qty,state FROM position_state WHERE position_id=?")
                .bind(&position)
                .fetch_one(f.store.pool())
                .await
                .unwrap();
        assert!(row.get::<i64, _>("close_revision") > before as i64);
        assert_eq!(row.get::<String, _>("qty"), "7");
        assert_eq!(
            f.target("paper", &position).await.reason_code,
            "POSITION_PROVENANCE_UNKNOWN"
        );
        f.cleanup().await;
    }

    /// **P0-1（M0fix）· ReopenClosed 在同一事务里退役未完成的善后义务。**
    ///
    /// 陈旧零仓真值把仓投影成 closed 并武装义务之后，reconcile 以新鲜真值
    /// 重开**同一个** position_id：那条义务不能再撤任何东西（所上保护单属于
    /// 仍开着的仓），也不能继续挡入场闸门；之后真正的平仓在新的
    /// close_revision 上重新武装一条 pending 义务。全部走生产函数。
    #[tokio::test]
    async fn reopen_closed_retires_a_pending_cleanup_obligation_and_a_later_close_rearms() {
        sqlx::any::install_default_drivers();
        let dir = std::env::temp_dir().join(format!("rustcore-m0fixreopen-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let store =
            crate::store::Store::connect(&format!("sqlite://{}", dir.join("engine.db").display()))
                .await
                .unwrap();
        let credential_id = "m0fix-reopen-cred";
        let symbol_key = "binance|futures|BTC|USDT|USDT";
        sqlx::query(
            "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, \
             entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms) \
             VALUES ('pos_m0fixreopen', ?, 'bot_m0fixreopen', ?, 'long', 1, 0, 'open', \
                     'bot_m0fixreopen|binance|futures|BTC|USDT|USDT|long', '1', 1)",
        )
        .bind(credential_id)
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO protection_orders (protection_order_id, position_id, credential_id, kind, \
             generation, engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id, \
             trigger_price, reduce_only, close_position, status, verified_from_exchange, \
             created_at_ms, updated_at_ms) VALUES ('prot_m0fixreopen_sl', 'pos_m0fixreopen', ?, 'sl', \
             1, 'eo_m0fixreopen_sl', NULL, '131', 'coid_m0fixreopen_sl', '95', 1, 1, 'confirmed', 1, 1, 1)",
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .unwrap();
        let obligation = |store: crate::store::Store| async move {
            let row = sqlx::query(
                "SELECT state, COALESCE(complete_reason,'') AS reason, attempts, close_revision \
                 FROM protection_cleanup_obligations WHERE position_id='pos_m0fixreopen'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap();
            (
                row.get::<String, _>("state"),
                row.get::<String, _>("reason"),
                row.get::<i64, _>("attempts"),
                row.get::<i64, _>("close_revision"),
            )
        };
        let revision = |store: crate::store::Store| async move {
            sqlx::query(
                "SELECT close_revision FROM position_state WHERE position_id='pos_m0fixreopen'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("close_revision")
        };
        let gated = |store: crate::store::Store| async move {
            let mut tx = store.pool().begin().await.unwrap();
            let pending = crate::protection_cleanup::entry_scope_pending_in_tx(
                &mut tx,
                "m0fix-reopen-cred",
                "binance|futures|BTC|USDT|USDT",
                "long",
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            pending
        };

        // 陈旧零仓真值：本地 closed + pending 义务（生产真值写入路径）。
        let mut tx = store.pool().begin().await.unwrap();
        crate::position_state::apply_exchange_position_truth(
            &mut tx,
            credential_id,
            "BTCUSDT",
            Some("BOTH"),
            "0",
            None,
            2_000,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let stale_revision = revision(store.clone()).await;
        assert_eq!(
            obligation(store.clone()).await,
            ("pending".to_string(), String::new(), 0, stale_revision)
        );
        assert!(
            gated(store.clone()).await,
            "fixture: the stale close gates the scope"
        );

        // reconcile 的 ReopenClosed（生产函数）。
        let mut tx = store.pool().begin().await.unwrap();
        let recovered = super::recover_orphan_position(
            &mut tx,
            credential_id,
            "1",
            Some("100"),
            3_000,
            super::OrphanPositionDisposition::ReopenClosed {
                position_id: "pos_m0fixreopen".into(),
                bot_uid: "bot_m0fixreopen".into(),
                symbol_key: symbol_key.into(),
                direction: "long".into(),
                entry_epoch: 1,
            },
        )
        .await
        .unwrap();
        assert!(recovered.is_some());
        tx.commit().await.unwrap();
        assert_eq!(
            obligation(store.clone()).await,
            (
                "retired".to_string(),
                "position_reopened".to_string(),
                0,
                stale_revision
            ),
            "the reopen must retire the pending obligation in its own transaction"
        );
        assert!(
            !gated(store.clone()).await,
            "a retired obligation no longer gates the scope"
        );

        // 真正的平仓：新 close_revision 上重新武装。
        let mut tx = store.pool().begin().await.unwrap();
        crate::position_state::apply_filled_order_truth(
            &mut tx,
            credential_id,
            "bot_m0fixreopen",
            symbol_key,
            "close_long",
            "1",
            Some("100"),
            4_000,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let closed_revision = revision(store.clone()).await;
        assert!(closed_revision > stale_revision);
        assert_eq!(
            obligation(store.clone()).await,
            ("pending".to_string(), String::new(), 0, closed_revision),
            "a later genuine close re-arms a fresh lifecycle"
        );
        assert!(
            gated(store.clone()).await,
            "the re-armed obligation gates the scope again"
        );

        store.pool().close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}
