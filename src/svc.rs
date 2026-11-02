use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sha2::Digest;
use tonic::{Request, Response, Status};

use crate::binance_futures::{
    keys_from_env, protectable_exit_weight_per_position, protectable_exposure_admission,
    BinanceAlgoConditionalOrderType, BinanceAlgoOrderResponse, BinanceClientAlgoId,
    BinanceFuturesClient, BinanceOrderAck, ExposureAdmission,
};
use crate::binance_user_stream::{
    BinanceAlgoUpdate, BinanceOrderTradeUpdate, BinanceUserStreamManager, BinanceWsEnvelope,
};
use crate::exchange_filters::{
    apply_exchange_filters, venue_for_filter_fetch, ExchangeConstraints, ExchangeFilterCache,
    OrderSide,
};
use crate::market_data::MarketDataManager;
use crate::position_state::{self, PrepareOutcome};
use crate::protection::{self, OrderRole};
use crate::proto;
use crate::proto::{
    engine_events_server::EngineEvents, engine_execution_server::EngineExecution,
    CancelOrderRequest, CancelOrderResponse, EngineEvent, EnsureMarketDataRequest,
    EnsureMarketDataResponse, ExecuteIntentsRequest, ExecuteIntentsResponse, GetOrderRequest,
    GetOrderResponse, IntentAck, ReconcileNowRequest, ReconcileNowResponse, StreamEventsRequest,
};
use crate::rate_limiter::{CredentialRateLimiter, RequestClass, TokenBucketConfig};
use crate::reconcile::{reconcile_binance_futures, ReconcilerManager};
use crate::risk;
use crate::store::{try_get_nullable_string, DbKind, Store};
use crate::venue::{venue_for_credential, Venue};
use risk_policy::Decision;

use sqlx::Row;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[path = "get_order_budget.rs"]
mod get_order_budget;

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Debug, Default)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    /// 生产装配里的**引擎时间观**。完成点在 `crate::clock`：默认构建下它逐字
    /// 就是 `SystemTime::now()`，`l4-fault-injection` 下 D7 harness 可以注入
    /// ±5s 漂移（promotion policy §3.3）。
    fn now_ms(&self) -> i64 {
        crate::clock::engine_now_ms()
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemClock.now_ms()
}

async fn latest_bar_close_price(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
) -> Option<String> {
    let row = sqlx::query(
        r#"
        SELECT payload_json
        FROM engine_order_events
        WHERE credential_id = ? AND event_type = 'bar_closed' AND symbol_key = ?
        ORDER BY ts_ms DESC
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .bind(symbol_key)
    .fetch_all(&mut **tx)
    .await
    .ok()?
    .into_iter()
    .next()?;

    let payload: Vec<u8> = row.get("payload_json");
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let close = v.get("close")?;
    let s = if let Some(s) = close.as_str() {
        s.trim().to_string()
    } else if close.is_number() {
        close.to_string()
    } else {
        return None;
    };
    let n = s.parse::<f64>().ok()?;
    if !n.is_finite() || n <= 0.0 {
        return None;
    }
    Some(s)
}

pub(crate) fn final_notional_from_sizing_json(sizing_json: &str) -> Result<String, Status> {
    let sizing: serde_json::Value = serde_json::from_str(sizing_json).map_err(|error| {
        Status::internal(format!("decode durable entry sizing failed: {error}"))
    })?;
    sizing
        .get("final_notional_usdt")
        .and_then(serde_json::Value::as_str)
        .filter(|value| {
            value
                .parse::<f64>()
                .is_ok_and(|number| number.is_finite() && number > 0.0)
        })
        .map(str::to_string)
        .ok_or_else(|| Status::internal("durable entry sizing has no positive final_notional_usdt"))
}

async fn release_paper_position_notional(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    position_id: &str,
    released_at_ms: i64,
) -> Result<(), Status> {
    let row = sqlx::query(
        r#"
        SELECT rv.sizing_json
        FROM engine_orders eo
        JOIN risk_verdicts rv
          ON rv.credential_id=eo.credential_id
         AND rv.risk_verdict_id=eo.risk_verdict_id
        WHERE eo.credential_id=?
          AND eo.position_id=?
          AND eo.order_role='entry'
        ORDER BY eo.created_at_ms ASC
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load paper entry sizing failed: {error}")))?
    .into_iter()
    .next()
    .ok_or_else(|| Status::failed_precondition("paper close has no durable entry sizing"))?;
    let sizing_json = crate::store::try_get_text(&row, "sizing_json")
        .map_err(|error| Status::internal(format!("decode durable entry sizing failed: {error}")))?
        .ok_or_else(|| Status::failed_precondition("paper close has no durable entry sizing"))?;
    let notional = final_notional_from_sizing_json(&sizing_json)?;
    risk::release_notional(tx, credential_id, &notional, released_at_ms).await
}

/// Exact decimal arithmetic for paper realized PnL truth.
///
/// These values are money truth consumed by the account projection and the
/// budget producer, so they are computed on an `i128` unit count with a
/// decimal scale — never through binary floating point. The accepted text
/// grammar mirrors the read side (`svc_v2::ExactDecimal`, which is private):
/// an optional `-`, ASCII digits, at most one fraction block.
#[derive(Clone, Copy)]
struct PaperMoney {
    units: i128,
    scale: u32,
}

impl PaperMoney {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.is_empty() || value.starts_with('+') {
            return None;
        }
        let (negative, unsigned) = value
            .strip_prefix('-')
            .map(|rest| (true, rest))
            .unwrap_or((false, value));
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if integer.is_empty()
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let scale = u32::try_from(fraction.len()).ok()?;
        let units = format!("{integer}{fraction}").parse::<i128>().ok()?;
        Some(Self {
            units: if negative { -units } else { units },
            scale,
        })
    }

    fn rescale(self, scale: u32) -> Option<i128> {
        if scale < self.scale {
            return None;
        }
        let factor = 10i128.checked_pow(scale - self.scale)?;
        self.units.checked_mul(factor)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        let scale = self.scale.max(other.scale);
        Some(Self {
            units: self.rescale(scale)?.checked_sub(other.rescale(scale)?)?,
            scale,
        })
    }

    fn checked_mul(self, other: Self) -> Option<Self> {
        Some(Self {
            units: self.units.checked_mul(other.units)?,
            scale: self.scale.checked_add(other.scale)?,
        })
    }

    fn numerically_equals(self, other: Self) -> bool {
        let scale = self.scale.max(other.scale);
        match (self.rescale(scale), other.rescale(scale)) {
            (Some(left), Some(right)) => left == right,
            _ => false,
        }
    }

    fn is_strictly_positive(self) -> bool {
        self.units > 0
    }

    fn to_text(self) -> String {
        if self.units == 0 {
            return "0".to_string();
        }
        let negative = self.units < 0;
        let magnitude = self.units.unsigned_abs().to_string();
        let mut rendered = if self.scale == 0 {
            magnitude
        } else if magnitude.len() <= self.scale as usize {
            format!(
                "0.{}{}",
                "0".repeat(self.scale as usize - magnitude.len()),
                magnitude
            )
        } else {
            let split = magnitude.len() - self.scale as usize;
            format!("{}.{}", &magnitude[..split], &magnitude[split..])
        };
        if rendered.contains('.') {
            while rendered.ends_with('0') {
                rendered.pop();
            }
            if rendered.ends_with('.') {
                rendered.pop();
            }
        }
        if negative {
            rendered.insert(0, '-');
        }
        rendered
    }
}

/// Realized PnL truth for a paper fill, derived only from simulation-owned
/// durable truth read inside the same order transaction.
///
/// - Entry fills open a position and close nothing: realized PnL is exactly
///   zero (paper charges no fee on fills, so gross equals net).
/// - Paper SL/TP submission only arms protection and never reaches this fill
///   path. Its ACK and durable protection confirmation do not realize PnL.
/// - Exit and emergency fills close the whole durable position: realized PnL
///   is the exact price difference on the durable quantity.
/// - Boundaries without defined economics keep `None` — unknown, never
///   defaulted to zero — so the account read side keeps rejecting them:
///   orphan-recovery closes (no local entry truth), fill quantities that do
///   not match the durable position quantity (partial closes), missing,
///   unparseable or non-positive entry/exit/qty truth (a zero/negative value
///   is not a financial price or trade size — a missing-price market close
///   falls back to the historical "0" fill-price placeholder), and any
///   unrecognized role.
async fn paper_fill_realized_pnl(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    order_role: OrderRole,
    prepared: &position_state::PreparedPositionIntent,
    intent: &proto::OrderIntent,
    fill_price: &str,
) -> Result<Option<String>, Status> {
    if matches!(
        order_role,
        OrderRole::Entry | OrderRole::ProtectionSl | OrderRole::ProtectionTp
    ) {
        return Ok(Some("0".to_string()));
    }
    if order_role != OrderRole::Exit && order_role != OrderRole::Emergency {
        return Ok(None);
    }
    if prepared.orphan_recovery {
        return Ok(None);
    }
    let Some(position_id) = prepared.position_id.as_deref() else {
        tracing::warn!(
            order_role = order_role.as_str(),
            "paper close without a durable position_id keeps realized PnL unknown"
        );
        return Ok(None);
    };
    // COALESCE pattern: the Any driver's NULL -> Option<String> decode is not
    // reliable; the empty string is "no truth" (see release_closing_if_exit_dead).
    let row = sqlx::query(
        "SELECT direction, qty, COALESCE(entry_price, '') AS entry_price_text \
         FROM position_state WHERE position_id = ?",
    )
    .bind(position_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load paper position truth failed: {error}")))?
    .ok_or_else(|| {
        Status::failed_precondition("paper close position truth disappeared before fill")
    })?;
    let direction: String = row.get("direction");
    let qty_text: String = row.get("qty");
    let entry_text: String = row.get("entry_price_text");
    let fill_qty_text = intent.quantity.trim();
    if entry_text.is_empty() || fill_qty_text.is_empty() {
        tracing::warn!(
            position_id,
            order_role = order_role.as_str(),
            "paper close has no durable entry price or fill quantity; realized PnL stays unknown"
        );
        return Ok(None);
    }
    let (Some(entry), Some(exit), Some(qty), Some(fill_qty)) = (
        PaperMoney::parse(&entry_text),
        PaperMoney::parse(fill_price),
        PaperMoney::parse(&qty_text),
        PaperMoney::parse(fill_qty_text),
    ) else {
        tracing::warn!(
            position_id,
            entry_price = %entry_text,
            fill_price,
            position_qty = %qty_text,
            fill_qty = fill_qty_text,
            "paper realized PnL inputs are not exact decimals; realized PnL stays unknown"
        );
        return Ok(None);
    };
    // Only strictly positive decimals are usable financial truth. The
    // missing-price market close falls back to the historical "0" fill-price
    // placeholder, and a zero/negative durable entry price is corrupt truth:
    // neither may be turned into a definite gain, loss or fake zero. Realized
    // PnL stays unknown and the account projection keeps failing closed.
    if !entry.is_strictly_positive()
        || !exit.is_strictly_positive()
        || !qty.is_strictly_positive()
        || !fill_qty.is_strictly_positive()
    {
        tracing::warn!(
            position_id,
            entry_price = %entry_text,
            fill_price,
            position_qty = %qty_text,
            fill_qty = fill_qty_text,
            "paper realized PnL needs strictly positive prices and quantities; \
             realized PnL stays unknown"
        );
        return Ok(None);
    }
    if !fill_qty.numerically_equals(qty) {
        tracing::warn!(
            position_id,
            position_qty = %qty_text,
            fill_qty = fill_qty_text,
            "paper close fill quantity does not match the durable position quantity; \
             partial closes are unsupported and realized PnL stays unknown"
        );
        return Ok(None);
    }
    if direction != "long" && direction != "short" {
        tracing::warn!(
            position_id,
            direction = %direction,
            "paper position has no recognizable direction; realized PnL stays unknown"
        );
        return Ok(None);
    }
    let signed_diff = if direction == "short" {
        entry.checked_sub(exit)
    } else {
        exit.checked_sub(entry)
    };
    match signed_diff.and_then(|diff| diff.checked_mul(qty)) {
        Some(realized) => Ok(Some(realized.to_text())),
        None => {
            tracing::warn!(
                position_id,
                direction = %direction,
                "paper realized PnL overflowed exact decimal range; realized PnL stays unknown"
            );
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EventContext {
    pub bot_uid: String,
    pub run_id: String,
    pub cycle_id: String,
    pub idempotency_key: String,
    pub client_order_id_short: String,
    pub exchange: String,
    pub market_kind: String,
    pub symbol_key: String,
}

/// 生产端口拒绝 v1 open 时的固定拒绝语（AUD-P2-D13）。
pub(crate) const V1_OPEN_REJECTION: &str =
    "rejected: V1_OPEN_NOT_ACCEPTED; frozen v1 wire cannot open positions on the \
     production port; use the v2 execution service";

/// 裸 v1（无 V2ExecutionContext）在生产端口的**闭世界**放行白名单。
///
/// R4 RV1-1 把判别键从 `!effective_reduce_only`（fail-closed：一切非 close
/// 语义一律拒）换成只拒 `open` 前缀——这是**开世界**判别：它枚举要拒的形态，
/// 于是任何未知 action 都落进放行侧。评审 FBL-R4-H2 逐环证实了后果：
/// `action="buy"` + `reduce_only=false` 时 `infer_v1_order_role` 兜底判
/// `Exit`（豁免开仓 stage rank 门与 demo 拒绝）→ 风控全规则 `!action.is_open()`
/// 跳过为 `not_open` → Approve → `OrderSide::for_action` 兜底 `BUY` →
/// `effective_reduce_only=false` 让实单带 `reduceOnly=false` 提交：一条调用方
/// 定量、无 sizing、无保护腿、绕过全部开仓风控的**真实开仓单**，比被 RV1-1
/// 修掉的 `open_*+reduce_only=true` 组合更宽。
///
/// 因此判别改为闭世界：裸 v1 只放行平仓语义的两个 action，其余一律拒绝——
/// 未知 action、大小写变体、`hold`、空串都落在拒绝侧（AUD-P2-D13 要求的
/// fail-closed 方向）。保护腿与 emergency 走的同样是 `close_*`
/// （见 `protection::infer_v1_order_role`：`reduce_only && action` 以 close
/// 开头才分流出 ProtectionSl/ProtectionTp/Emergency），不受影响。
///
/// 判别用 action 的**原字节**而非归一化形式，与下游一致：
/// `OrderSide::for_action` 与 `protection::infer_v1_order_role` 都做精确/前缀
/// 匹配，不 trim 也不降大小写。放宽这里而下游不放宽，只会让一条 wire 上被接受
/// 的 action 在下游拿到兜底语义——那正是本条 High 的成因。
pub(crate) fn v1_action_accepted_on_production_port(action: &str) -> bool {
    matches!(action, "close_long" | "close_short")
}

/// Per-request v2 metadata transported through tonic request extensions into
/// the single v1/v2 execution implementation. It is never stored in a global
/// map and is not an idempotency or protection authority.
#[derive(Clone, Debug, Default)]
pub(crate) struct V2ExecutionContext {
    pub intents: HashMap<String, V2IntentContext>,
}

#[derive(Clone, Debug)]
pub(crate) struct V2IntentContext {
    pub risk: risk::WireRiskContext,
    pub order_role: OrderRole,
    pub protection_required: bool,
    pub confirm_timeout_ms: u64,
    pub protection_generation: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeSubmissionRoute {
    Market,
    Limit,
    Algo(BinanceAlgoConditionalOrderType),
}

fn exchange_submission_route(
    order_role: OrderRole,
    order_type: &str,
) -> Result<ExchangeSubmissionRoute, String> {
    match (order_role, order_type.trim().to_ascii_lowercase().as_str()) {
        (OrderRole::ProtectionSl, "stop_market") => Ok(ExchangeSubmissionRoute::Algo(
            BinanceAlgoConditionalOrderType::StopMarket,
        )),
        (OrderRole::ProtectionTp, "take_profit_market") => Ok(ExchangeSubmissionRoute::Algo(
            BinanceAlgoConditionalOrderType::TakeProfitMarket,
        )),
        (_, "" | "market") => Ok(ExchangeSubmissionRoute::Market),
        (_, "limit") => Ok(ExchangeSubmissionRoute::Limit),
        (_, other) => Err(format!("unsupported order_type={other}")),
    }
}

#[derive(Debug)]
enum ExchangeSubmissionAck {
    Ordinary(BinanceOrderAck),
    Algo(BinanceAlgoOrderResponse),
}

fn inserted_protection_plan_candidate(
    order_role: OrderRole,
    context: Option<&V2IntentContext>,
) -> bool {
    order_role == OrderRole::Entry
        && context.is_some_and(|context| context.risk.protection.is_some())
}

impl EventContext {
    fn from_intent(intent: &proto::OrderIntent) -> Self {
        Self {
            bot_uid: intent.bot_uid.clone(),
            run_id: intent.run_id.clone(),
            cycle_id: intent.cycle_id.clone(),
            idempotency_key: intent.idempotency_key.clone(),
            client_order_id_short: intent.client_order_id_short.clone(),
            exchange: intent.exchange.clone(),
            market_kind: intent.market_kind.clone(),
            symbol_key: intent.symbol_key.clone(),
        }
    }
}

/// A simulation close retires only this position's local protection. The
/// caller has proven there is no venue identity and holds the close transaction.
pub(crate) async fn cancel_simulation_protection_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    credential_id: &str,
    position_id: &str,
    ts_ms: i64,
) -> Result<(), Status> {
    let rows = sqlx::query(
        "SELECT eo.*, COALESCE(rv.intent_id,'') AS intent_id \
         FROM engine_orders eo LEFT JOIN risk_verdicts rv \
         ON rv.risk_verdict_id=eo.risk_verdict_id AND rv.credential_id=eo.credential_id \
         WHERE eo.credential_id=? AND eo.position_id=? \
         AND eo.order_role IN ('protection_sl','protection_tp') \
         AND eo.status NOT IN ('filled','canceled','expired','rejected','failed')",
    )
    .bind(credential_id)
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("read simulation protection: {e}")))?;
    for row in rows {
        let order_id: String = row.get("engine_order_id");
        let changed = sqlx::query(
            "UPDATE engine_orders SET status='canceled',updated_at_ms=? \
             WHERE engine_order_id=? AND credential_id=? AND position_id=? \
             AND status NOT IN ('filled','canceled','expired','rejected','failed')",
        )
        .bind(ts_ms)
        .bind(&order_id)
        .bind(credential_id)
        .bind(position_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("cancel simulation protection: {e}")))?;
        if changed.rows_affected() == 0 {
            continue;
        }
        let ctx = EventContext {
            bot_uid: row.get("bot_uid"),
            run_id: row
                .try_get::<Option<String>, _>("run_id")
                .ok()
                .flatten()
                .unwrap_or_default(),
            cycle_id: row
                .try_get::<Option<String>, _>("cycle_id")
                .ok()
                .flatten()
                .unwrap_or_default(),
            idempotency_key: row.get("idempotency_key"),
            client_order_id_short: row
                .try_get::<Option<String>, _>("client_order_id_short")
                .ok()
                .flatten()
                .unwrap_or_default(),
            exchange: row.get("exchange"),
            market_kind: row.get("market_kind"),
            symbol_key: row.get("symbol_key"),
        };
        let payload = serde_json::json!({
            "paper": true, "runMode": "paper", "positionId": position_id,
            "action": row.get::<String,_>("action"), "side": row.get::<String,_>("side"),
            "orderType": row.get::<String,_>("order_type"),
            "requestedQty": row.get::<String,_>("quantity"),
            "orderRole": row.get::<String,_>("order_role"),
            "intentId": row.get::<String,_>("intent_id"),
            "riskVerdictId": row.try_get::<Option<String>,_>("risk_verdict_id").ok().flatten(),
            "orderStatus": "CANCELED", "source": "paper_position_close",
        });
        emit_event(
            tx,
            kind,
            credential_id,
            "cancel",
            &ctx,
            &order_id,
            None,
            Some(payload),
            Some(sha256_hex(&format!(
                "paper_protection_cancel|{credential_id}|{order_id}"
            ))),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE protection_orders SET status='canceled',updated_at_ms=? \
         WHERE credential_id=? AND position_id=? \
         AND status NOT IN ('filled','canceled','expired','rejected','failed')",
    )
    .bind(ts_ms)
    .bind(credential_id)
    .bind(position_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("retire simulation protection: {e}")))?;
    Ok(())
}

fn verdict_event_payload(
    intent: &proto::OrderIntent,
    run_mode: &str,
    verdict: &risk_policy::RiskVerdict,
    order_role: OrderRole,
    position_id: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "runMode": run_mode,
        "action": intent.action.as_str(),
        "side": intent.side.as_str(),
        "orderType": intent.order_type.as_str(),
        "requestedQty": intent.quantity.as_str(),
        "requestedPrice": if intent.price.is_empty() { None } else { Some(intent.price.as_str()) },
        "intentId": verdict.intent_id.as_str(),
        "riskVerdictId": verdict.risk_verdict_id.as_str(),
        "positionId": position_id,
        "orderRole": order_role.as_str(),
        "riskVerdict": risk::verdict_audit_payload(verdict),
    })
}

pub(crate) fn sha256_hex(input: &str) -> String {
    let mut h = sha2::Sha256::new();
    h.update(input.as_bytes());
    hex::encode(h.finalize())
}

pub(crate) fn binance_order_status_to_engine_status(status: &str) -> Option<&'static str> {
    match status.trim().to_uppercase().as_str() {
        "NEW" => Some("acked"),
        "PARTIALLY_FILLED" => Some("partial_filled"),
        "FILLED" => Some("filled"),
        "CANCELED" => Some("canceled"),
        "REJECTED" => Some("rejected"),
        "EXPIRED" => Some("expired"),
        _ => None,
    }
}

fn get_order_refresh_event_id(
    credential_id: &str,
    engine_order_id: &str,
    symbol_key: &str,
    exchange_order_id: &str,
    next_status: &str,
    exchange_update_time: i64,
) -> String {
    sha256_hex(
        &serde_json::json!([
            "get_order",
            credential_id,
            engine_order_id,
            symbol_key,
            exchange_order_id,
            next_status,
            exchange_update_time
        ])
        .to_string(),
    )
}

fn parse_engine_live_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
}

pub(crate) fn is_binance_live_enabled() -> bool {
    parse_engine_live_enabled(std::env::var("ENGINE_LIVE_ENABLED").ok().as_deref())
}

fn stage_rank(stage: &str) -> Option<u8> {
    match stage.trim().to_lowercase().as_str() {
        "paper" => Some(0),
        "demo" => Some(1),
        "live_small" => Some(2),
        "live" => Some(3),
        _ => None,
    }
}

fn stage_certification_venue_is_testnet(venue: Option<Venue>) -> bool {
    venue == Some(Venue::Testnet)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StageDenial {
    UnsupportedStage {
        requested: String,
    },
    LiveDisabled,
    ExceedsEffective {
        requested: String,
        effective: &'static str,
    },
    VenueConfiguration(String),
    SimulationCredentialNotExchangeBacked,
    UnprovenCredentialIdentity,
}

impl std::fmt::Display for StageDenial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedStage { requested } => {
                write!(formatter, "unsupported engine stage: {requested}")
            }
            Self::LiveDisabled => {
                formatter.write_str("live engine stage disabled by ENGINE_LIVE_ENABLED")
            }
            Self::ExceedsEffective {
                requested,
                effective,
            } => write!(
                formatter,
                "engine stage {requested} exceeds effective stage maximum {effective}",
            ),
            Self::VenueConfiguration(message) => formatter.write_str(message),
            Self::SimulationCredentialNotExchangeBacked => formatter.write_str(
                "simulation credential cannot be used with an exchange-backed stage \
                 (demo/live_small/live); it has no exchange account behind it",
            ),
            Self::UnprovenCredentialIdentity => formatter.write_str(
                "credential_id identity is not proven: it either uses the reserved `paper` \
                 namespace without matching the simulation grammar, or fails the exchange \
                 credential grammar (ASCII alphanumerics and ._:- only, 1..=64 bytes); \
                 refusing to treat it as exchange-backed",
            ),
        }
    }
}

fn stage_allowed_with(
    requested: &str,
    deployment: &str,
    live_enabled: bool,
) -> Result<(), StageDenial> {
    let requested = requested.trim().to_lowercase();
    let requested_rank = stage_rank(&requested).ok_or_else(|| StageDenial::UnsupportedStage {
        requested: requested.clone(),
    })?;
    if requested_rank >= stage_rank("live_small").expect("known stage") && !live_enabled {
        return Err(StageDenial::LiveDisabled);
    }
    let deployment_rank = stage_rank(deployment).unwrap_or(0);
    if requested_rank > deployment_rank {
        let effective = match deployment_rank {
            0 => "paper",
            1 => "demo",
            2 => "live_small",
            3 => "live",
            _ => unreachable!("stage_rank only returns known ranks"),
        };
        return Err(StageDenial::ExceedsEffective {
            requested,
            effective,
        });
    }
    Ok(())
}

fn stage_allowed_with_credential(
    requested: &str,
    credential_id: &str,
    deployment: &str,
    live_enabled: bool,
) -> Result<(), StageDenial> {
    match crate::venue::credential_kind(credential_id) {
        // 模拟凭据背后没有交易所账户；任何 exchange-backed 档位一律拒，
        // 且对所有 order_role 生效（见 stage_denial_applies 的豁免只覆盖
        // ExceedsEffective，本 variant 不在其中）。
        crate::venue::CredentialKind::Simulation => {
            if is_exchange_backed_run_mode(requested) {
                return Err(StageDenial::SimulationCredentialNotExchangeBacked);
            }
        }
        // 身份未被证明的一律拒（任何档位，含 paper）：拼错一位的 Product Demo
        // 凭据、以及含非 ASCII/零宽字符的 id，都不得掉进交易所分支去拿全局密钥。
        crate::venue::CredentialKind::UnprovenIdentity => {
            return Err(StageDenial::UnprovenCredentialIdentity);
        }
        crate::venue::CredentialKind::ExchangeBacked => {}
    }
    venue_for_credential(credential_id).map_err(StageDenial::VenueConfiguration)?;
    stage_allowed_with(requested, deployment, live_enabled)
}

fn stage_allowed(requested: &str, credential_id: &str) -> Result<(), StageDenial> {
    let deployment = std::env::var("ENGINE_STAGE_MAX").unwrap_or_else(|_| "paper".to_string());
    stage_allowed_with_credential(
        requested,
        credential_id,
        &deployment,
        is_binance_live_enabled(),
    )
}

fn is_exchange_backed_run_mode(run_mode: &str) -> bool {
    matches!(
        run_mode.trim().to_ascii_lowercase().as_str(),
        "demo" | "live_small" | "live"
    )
}

/// 档位闸门的一条拒因是否作用于这一条 intent。
///
/// * `ExceedsEffective`（部署上限）只拦 Entry（既有语义）；
/// * `LiveDisabled`（实盘总开关）只拦新增风险（D39 / engine safety contract）：只减不增
///   的退出——非 Entry 角色、action 属 close_long|close_short 闭集（交易所侧
///   一律 reduceOnly）——且该凭据的 reduce-only 退出可达交易所
///   （[`reduce_only_exit_reaches_exchange_for`]，与紧急 flatten 同一判据）时
///   不拦；开仓 / 增险、闭集外的 action 照拦；
/// * 其余拒因（未知档位、venue 配置、凭据身份）对所有角色生效。
fn stage_denial_applies(
    denial: &StageDenial,
    order_role: OrderRole,
    action: &str,
    reduce_only_exit_reachable: bool,
) -> bool {
    match denial {
        StageDenial::ExceedsEffective { .. } => order_role == OrderRole::Entry,
        StageDenial::LiveDisabled => {
            !(reduce_only_exit_reachable
                && order_role != OrderRole::Entry
                && matches!(action, "close_long" | "close_short"))
        }
        _ => true,
    }
}

fn demo_submission_reject_reason(
    run_mode: &str,
    venue: Option<Venue>,
    order_role: OrderRole,
) -> Option<&'static str> {
    if run_mode == "demo"
        && venue != Some(Venue::Testnet)
        && matches!(
            order_role,
            OrderRole::Entry | OrderRole::ProtectionSl | OrderRole::ProtectionTp
        )
    {
        Some("DEMO_REQUIRES_TESTNET_VENUE")
    } else {
        None
    }
}

/// 入参是 `CredentialKind` 而不是裸 bool：两个相邻的 bool 参数是可以被静默
/// 对调的（`credentials_to_act_on` 上方那条「no polarity to invert」注释就是
/// 为了防这类事），枚举让对调无法通过编译。
pub(crate) fn exchange_gateway_allowed_for(
    kind: crate::venue::CredentialKind,
    live_enabled: bool,
    venue: Option<Venue>,
) -> bool {
    match kind {
        crate::venue::CredentialKind::Simulation
        | crate::venue::CredentialKind::UnprovenIdentity => false,
        crate::venue::CredentialKind::ExchangeBacked => {
            live_enabled || venue == Some(Venue::Testnet)
        }
    }
}

/// D39 / engine safety contract：实盘总开关只拦新增风险。只减不增的退出动作（单仓
/// reduce-only 平仓、紧急 flatten 的撤单与只减市价单）能否到达交易所：交易所
/// 背书凭据且 venue 显式配置（testnet / mainnet）时，总开关关闭也可以——关
/// 总开关正是最需要能平仓的时刻；未配置 venue 的旧式凭据维持既有网关判据
/// （总开关关闭即不出站）。档位闸门的 LiveDisabled 豁免与 kill switch 的交易所
/// 动作共用这一个判据。
pub(crate) fn reduce_only_exit_reaches_exchange_for(
    kind: crate::venue::CredentialKind,
    live_enabled: bool,
    venue: Option<Venue>,
) -> bool {
    exchange_gateway_allowed_for(kind, live_enabled, venue)
        || (kind == crate::venue::CredentialKind::ExchangeBacked && venue.is_some())
}

fn exchange_gateway_allowed(credential_id: &str) -> Result<bool, String> {
    venue_for_credential(credential_id).map(|venue| {
        exchange_gateway_allowed_for(
            crate::venue::credential_kind(credential_id),
            is_binance_live_enabled(),
            venue,
        )
    })
}

fn protection_run_mode_for_venue(venue: Option<Venue>) -> &'static str {
    match venue {
        Some(Venue::Testnet) => "demo",
        Some(Venue::Mainnet) | None => "live",
    }
}

pub(crate) fn protection_run_mode_for_credential(credential_id: &str) -> &'static str {
    venue_for_credential(credential_id)
        .map(protection_run_mode_for_venue)
        .unwrap_or("live")
}

#[derive(Clone)]
pub struct EngineExecutionService {
    store: Store,
    clock: Arc<dyn Clock>,
    limiter: CredentialRateLimiter,
    binance_streams: BinanceUserStreamManager,
    reconcilers: ReconcilerManager,
    owns_background_exchange_tasks: bool,
    market_data: MarketDataManager,
    exchange_filters: ExchangeFilterCache,
    #[cfg(test)]
    protection_test_adapter: Option<Arc<dyn protection::ProtectionAdapter>>,
    #[cfg(test)]
    protection_test_submit_fails: bool,
    /// ****：提交路径的交易所客户端替身（无网络）。
    ///
    /// 段级复评四轮都在同一处失手：`ExchangeAttempt` 的分类被用例钉住了，
    /// 但**生产 `place` 闭包里那几个 `return` 没有**——把闭包里的
    /// `NotStarted` 改成 `StartedUnknown`，用例 1/1 照样通过，
    /// 因为用例自己构造 enum 喂给 helper，从不经过真实分类。
    ///
    /// 有了这个注入点，用例可以驱动**真实 `ExecuteIntents`**：
    /// 三个分类各自落到不同的库状态（`rejected` / `unknown` / `acked`），
    /// 分类改错就直接体现为状态不对——不需要探针，观察的就是生产结果。
    /// 客户端用 `new_with_mock_responses`，**不联网**（§4：交易路径测试默认 dry-run）。
    #[cfg(test)]
    submit_client_override: Option<BinanceFuturesClient>,
    // Test-only HTTP boundary; CancelOrder still resolves identity and venue/keys.
    #[cfg(test)]
    cancel_client_override: Option<BinanceFuturesClient>,
}

/// 提交**能不能发出去**的本地判定——`Err` 即「这次请求不会离开本进程」。
///
/// R7 把三个 `NotStarted` 的产生点
/// 散在 `place` 闭包里（路由不合法、limit 缺 price、algo 缺 triggerPrice）。
/// 用例自己构造 enum 喂给 helper、从不经过真实分类，于是**把闭包里的
/// `NotStarted` 改成 `StartedUnknown`，用例 1/1 照样通过**——
/// 与本段前三轮反复出现的形状完全一致：判据钉住了，判据的**产生点**没钉。
///
/// 现在三个产生点收敛成这一个函数：它是「请求会不会发出去」的**唯一判据**，
/// 闭包只剩一句把 `Err` 转成 `NotStarted` 的适配。函数本身可以被直接驱动，
/// 任何一条判据被改松都会在用例里变红。
#[allow(
    private_interfaces,
    reason = "Internal interface is preserved during packaging."
)]
pub(crate) fn plan_exchange_submission(
    order_role: OrderRole,
    order_type: &str,
    price: &str,
) -> anyhow::Result<ExchangeSubmissionRoute> {
    let route = exchange_submission_route(order_role, order_type).map_err(anyhow::Error::msg)?;
    match route {
        ExchangeSubmissionRoute::Limit if price.trim().is_empty() => {
            Err(anyhow::anyhow!("limit order missing price"))
        }
        ExchangeSubmissionRoute::Algo(_) if price.trim().is_empty() => Err(anyhow::anyhow!(
            "conditional algo order missing triggerPrice (use price field)"
        )),
        route => Ok(route),
    }
}

/// **一次交易所提交尝试的结果**——三态，不是 `Result`。
///
/// R6 用一个布尔
/// `exchange_request_attempted` 表达"有没有真的发过请求"，然后按它决定
/// 失败写回要不要带 CAS。**那个布尔从一开始就是错的**：它在**任何**
/// `place` 返回 `Err` 之后置位，而 `place` 的第一句是 `match route?`——
/// 路由不合法、limit 缺 price 这些**根本没发包**的本地错误也会置位。
/// 于是"没发过包"的失败照样走无条件 `unknown` 写回，
/// regression 换了扇门继续存在（复评变异 N1，93/93 存活）。
///
/// 根因是**用一个布尔去表达一个三分的事实**。现在让提交侧直接说清楚：
///
/// - `NotStarted`：请求**从未离开本进程**（路由不合法、缺必填参数）。
///   交易所侧不可能有这张单，因此这一行的当前状态就是真值——
///   写回必须带 CAS，绝不能把别的路径落下的终态盖掉，更不能写 `unknown`
///   （那是在为一张从未存在的单声称"结果不明"）。
/// - `StartedUnknown`：请求已经交给交易所客户端但结果不明（超时、连接中断、
///   5xx）。交易所侧**可能**挂着一张活单，必须转 `unknown` 让 reconcile
///   按真值复查——此时挡住写入才是危险的，活单会失去追踪。
/// - `Acked`：交易所明确应答。
///
/// **分类原则是保守的**：进入客户端调用之后的任何错误一律记 `StartedUnknown`，
/// 只有能证明发生在调用之前的才记 `NotStarted`。误判方向因此永远偏向
/// "多查一次"，而不是"漏掉一张活单"。
#[derive(Debug)]
#[allow(
    private_interfaces,
    reason = "Internal interface is preserved during packaging."
)]
pub(crate) enum ExchangeAttempt {
    NotStarted(anyhow::Error),
    ProtectionLocalFailure(anyhow::Error),
    StartedUnknown(anyhow::Error),
    Acked(ExchangeSubmissionAck),
}

/// 一次提交尝试对**写回语义**的影响。
///
/// 变异 R7-C 实证的缺口：`ExchangeAttempt` 的分类被用例钉住了，
/// 但**调用点拿这个分类去干什么**没有。把 `NotStarted` 那条臂的臂体整个换成
/// `StartedUnknown` 的处理方式（置 `live_error` + 走无条件 `unknown`），
/// 214 条测试全绿——与 R6 的 MUT-C 完全同形：判据钉住了，判据到控制流那一段没钉。
///
/// 处置：把「哪个分类 → 哪种写回」从**散在各臂的代码**变成**一个可测的映射**。
/// 生产的两个标志只能由它产生，调用点不再各写各的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteBackIntent {
    /// 走**带 CAS** 的本地拒绝写回：这一行不可能有交易所侧真值，
    /// 因此当前状态就是真值，绝不能盖掉别的路径落下的终态。
    pub local_reject: bool,
    /// 走**无条件** `unknown`：交易所侧真值不明，那边可能挂着活单，
    /// 挡住写入会让它失去追踪。
    pub outcome_unknown: bool,
}

/// 分类 → 写回语义的**唯一映射**。
pub(crate) fn write_back_intent(attempt: &ExchangeAttempt) -> WriteBackIntent {
    match attempt {
        // 请求从未离开本进程。
        ExchangeAttempt::NotStarted(_) => WriteBackIntent {
            local_reject: true,
            outcome_unknown: false,
        },
        ExchangeAttempt::ProtectionLocalFailure(_) => WriteBackIntent {
            local_reject: false,
            outcome_unknown: false,
        },
        // 已交给交易所客户端，结果不明。
        ExchangeAttempt::StartedUnknown(_) => WriteBackIntent {
            local_reject: false,
            outcome_unknown: true,
        },
        // 交易所明确应答：两条写回都不适用，由 ack 分支自己落库。
        ExchangeAttempt::Acked(_) => WriteBackIntent {
            local_reject: false,
            outcome_unknown: false,
        },
    }
}

/// 提交 ownership CAS 没有命中时的异常回执/审计代码。
pub(crate) const SUBMIT_OWNERSHIP_BLOCKED_CODE: &str = "ORDER_SUBMIT_OWNERSHIP_BLOCKED";

fn duplicate_ack_message(is_exchange_backed: bool, authority_idem_key: &str) -> String {
    let operation = if is_exchange_backed {
        "exchange not resubmitted"
    } else {
        "paper execution not repeated"
    };
    format!("duplicate; {operation}; authoritative_idempotency_key={authority_idem_key}")
}

/// **订单的终态集合**：交易所真值已经落定，任何更弱/更早的观测都不得改写它。
///
/// WS 的 `ORDER_TRADE_UPDATE`
/// 写回此前无状态谓词，一条迟到的 `NEW` 能把 `filled` 改回 `acked`。
/// 单列一份，避免"提交路径一套、WS 一套"两处各写各的终态定义。
pub(crate) const TERMINAL_ORDER_STATUSES: [&str; 5] =
    ["filled", "canceled", "expired", "rejected", "failed"];

pub(crate) fn is_terminal_order_status(status: &str) -> bool {
    TERMINAL_ORDER_STATUSES.contains(&status.trim().to_ascii_lowercase().as_str())
}

/// 引擎订单状态的**完整闭世界**。偏序判定要枚举"比 next 弱的状态"，
/// 因此这份清单必须是**穷举**的：漏掉一个，它就会被当成"认不出的状态"
/// 而 fail-closed 地拒绝写回（宁可不写，也不能猜）。
pub(crate) const ALL_ENGINE_ORDER_STATUSES: [&str; 11] = [
    "pending_risk",
    "pending_submit",
    "unknown",
    "submitted",
    "acked",
    "partial_filled",
    "filled",
    "canceled",
    "expired",
    "rejected",
    "failed",
];

/// **订单状态的进行度偏序**。
///
/// R9 只保护了终态：`is_terminal_order_status` 之外的一切都可以被 WS 任意
/// 改写。于是一条迟到的 `NEW` 能把 `partial_filled` 退回 `acked`
/// （评审实跑反例）——**已成交的那部分在状态上消失了**，而 `acked` 又在
/// reconcile 的 inflight 重查集里，等于把一张部分成交的单放回处理面。
/// WS 的送达顺序本来就不保证，这不是理论风险。
///
/// 名次的依据是「这条真值蕴含了多少已经发生的事实」：
/// * `pending_risk`(0)：本地刚落库，什么都没发生；
/// * `pending_submit` / `unknown`(1)：请求可能在路上，但没有任何交易所真值。
///   `unknown` 刻意与 `pending_submit` 同级——它表达的正是"没拿到真值"，
///   因此**任何**交易所真值都比它强，可以覆盖它；
/// * `submitted`(2)：本地已确认发出；
/// * `acked`(3)：交易所确认收单；
/// * `partial_filled`(4)：已经有成交；
/// * 终态(5)：不会再变。
///
/// **同级不覆盖**：两个终态之间（`filled` vs `canceled`）、
/// 两条 `partial_filled` 之间，WS 都不得改写——同级改写没有增量真值，
/// 只会让"哪一条先到"决定库里写什么。
/// **终态之间的权威度**。
///
/// R10 把**所有**终态压成同一个 rank 5。于是一条本地 `failed`
/// （从未拿到交易所真值、由 reconcile 宽限期裁决出来的那个 `failed`）
/// 能挡住后来真正的 `FILLED`——而同一帧里的 fill 行与 `position_state`
/// 敞口**照样落库**。库里因此留下「一张 failed 的单 + 一笔成交 + 一份敞口」
/// 这种状态与账务互相矛盾的记录，且没有任何地方报错。
/// 评审变异 R7-05 提示现有测试恰好把"终态同级"钉死了，那条语义按本裁决改写。
///
/// 三档的依据仍是「这条真值蕴含了多少已经发生的事实」：
/// * `failed`(5)：**本地**终态。它的语义就是"我们没能从交易所拿到真值"，
///   因此任何一条交易所权威终态都比它强。
/// * `canceled` / `expired` / `rejected`(6)：交易所权威终态，**无成交**。
///   三者互相同级——它们互斥，谁先到谁作数，晚到的不得改写。
/// * `filled`(7)：交易所权威终态，**有成交**。它是唯一蕴含"钱动了"的状态，
///   因此不被任何其它终态改写，也能改写任何其它终态。
fn terminal_authority_rank(status: &str) -> Option<u8> {
    match status {
        "failed" => Some(5),
        "canceled" | "expired" | "rejected" => Some(6),
        "filled" => Some(7),
        _ => None,
    }
}

pub(crate) fn order_status_progress_rank(status: &str) -> Option<u8> {
    let normalized = status.trim().to_ascii_lowercase();
    // 终态那一档**复用** `is_terminal_order_status` 判定"是不是终态"，
    // 不另抄一份清单：两份终态定义一旦走散，就会重现 regression
    // （提交闸门守住了、后处理又把终态盖掉）那种"各写各的谓词"的缺口。
    // 终态**之间**的权威度由 `terminal_authority_rank` 单独裁决。
    if is_terminal_order_status(&normalized) {
        return terminal_authority_rank(&normalized);
    }
    match normalized.as_str() {
        "pending_risk" => Some(0),
        "pending_submit" | "unknown" => Some(1),
        "submitted" => Some(2),
        "acked" => Some(3),
        "partial_filled" => Some(4),
        _ => None,
    }
}

/// 允许被 `next` 覆盖的当前状态集合——**严格更弱**的那些。
///
/// 用"允许集合"而不是"禁止集合"是刻意的 fail-closed：一个不在闭世界里的
/// 当前状态（脏数据、未来新增的状态）不会落进允许集合，因此不会被覆盖。
/// 谓词最终写进 SQL 的 `status IN (...)`，不是先读后写的 TOCTOU。
pub(crate) fn statuses_weaker_than(next: &str) -> Vec<&'static str> {
    let Some(next_rank) = order_status_progress_rank(next) else {
        return Vec::new();
    };
    ALL_ENGINE_ORDER_STATUSES
        .into_iter()
        .filter(|status| order_status_progress_rank(status).is_some_and(|rank| rank < next_rank))
        .collect()
}

/// 一条 WS 帧的**候选本地行**在两种 order identity 上的命中情况。
///
/// 只带布尔，不带行数据：裁决规则因此可以被逐条变异打红，而不必造一张库。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderIdentityCandidate {
    pub matches_exchange_order_id: bool,
    pub matches_client_order_id: bool,
}

/// 归属裁决的结果。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OrderIdentityAdjudication {
    /// 被判定为这条帧真正归属的候选下标。`None` = 不写任何一行。
    pub chosen: Option<usize>,
    /// 同一种**权威**身份命中了多行：不猜，整条丢弃。
    pub ambiguous: bool,
    /// 两种 identity 分别命中了**不同**的行（分裂命中）。
    /// 仍按权威身份归属，但必须告警——它意味着本地存在 id 复用。
    pub split: bool,
}

/// **WS 订单归属裁决**。
///
/// R10 的选择判据是 `WHERE credential AND (exchange_order_id=? OR
/// client_order_id_short=?) AND symbol_key IN (...) ORDER BY created_at_ms
/// DESC LIMIT 1`。评审构造的反例：同一 credential/symbol 下旧行 A 只匹配
/// `exchange_order_id`、新行 B 只匹配 `client_order_id`，一条同时带两者的
/// frame 经 `OR` 命中两行——`DESC` 选 B、`ASC` 选 A。
/// **排序改变了真实归属**，而排序不是身份。
///
/// 裁决规则（钉死，不再依赖任何排序）：
/// 1. `exchange_order_id` 是**权威**身份——它由交易所分配，本地不可自选。
///    只要有行按它命中，归属就在这些行里。
/// 2. 权威身份命中多行 = 本地 id 复用，**不猜**：整条丢弃并告警
///    （reconcile 会按交易所真值把它补回来）。
/// 3. 权威身份无命中时，才回落到调用方自选的 `client_order_id`；
///    它同样只接受唯一命中。
/// 4. 两种 identity 命中**不同**的行（分裂命中）：按权威身份归属，
///    并置 `split` 让调用方告警。
///
/// 函数对候选顺序**不敏感**：把 `candidates` 逆序传进来，结果里的
/// `chosen` 指向的仍是同一条候选（见 `ws_identity_adjudication_is_order_free`）。
pub(crate) fn adjudicate_order_identity(
    candidates: &[OrderIdentityCandidate],
) -> OrderIdentityAdjudication {
    let by_exchange: Vec<usize> = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.matches_exchange_order_id)
        .map(|(index, _)| index)
        .collect();
    let by_client: Vec<usize> = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.matches_client_order_id)
        .map(|(index, _)| index)
        .collect();
    if by_exchange.len() > 1 {
        return OrderIdentityAdjudication {
            chosen: None,
            ambiguous: true,
            split: false,
        };
    }
    if let Some(&chosen) = by_exchange.first() {
        return OrderIdentityAdjudication {
            chosen: Some(chosen),
            ambiguous: false,
            split: by_client.iter().any(|index| *index != chosen),
        };
    }
    if by_client.len() > 1 {
        return OrderIdentityAdjudication {
            chosen: None,
            ambiguous: true,
            split: false,
        };
    }
    OrderIdentityAdjudication {
        chosen: by_client.first().copied(),
        ambiguous: false,
        split: false,
    }
}

/// **不可能与一笔成交共存的终态**。
///
/// `canceled` / `expired` 刻意**不在**这里：Binance 的部分成交单最终就是
/// `CANCELED`/`EXPIRED`，"已撤单 + 有成交"是真实且自洽的账务。
/// 而 `rejected` 是交易所拒单（根本没进撮合）、`failed` 是本地在从未拿到
/// 交易所真值的情况下判死——这两种状态与"这笔单成交了"直接矛盾。
pub(crate) fn terminal_denies_execution(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "rejected" | "failed"
    )
}

/// 仍可被提交路径推进的状态集合——也是终态写回时**唯一允许被改写**的集合。
///
/// 单列一份是为了让「提交前」这个概念只有一处定义：CAS 谓词与终态写回谓词
/// 一旦各写各的，就会重现 regression（提交闸门守住了，后处理又把终态盖掉）。
const PRE_SUBMIT_STATUSES: [&str; 2] = ["pending_risk", "pending_submit"];

/// SQL 谓词里的状态左值必须与当前方言绑定。MySQL 的默认
/// `VARCHAR` collation 通常大小写不敏感；这里的状态是 authority boundary，
/// 所以不允许 `PENDING_RISK` 等脏字节获得提交或写回权。
pub(crate) fn exact_engine_order_status_lhs(db_kind: DbKind) -> Result<&'static str, sqlx::Error> {
    match db_kind {
        DbKind::MySql => Ok("BINARY status"),
        DbKind::Sqlite => Ok("status"),
        DbKind::Unknown => Err(sqlx::Error::Protocol(
            "engine-order status CAS requires a known database dialect".into(),
        )),
    }
}

/// 只有 `Advanced` 才表示本调用拿到了从 exact `pending_risk` 到
/// `pending_submit` 的线性化所有权，因而才可以 poll 交易所 future。
/// `Blocked` 里的原始状态只供审计，绝不是二次授权。
#[derive(Debug)]
pub(crate) enum AdvanceThenSubmitOutcome {
    Advanced(ExchangeAttempt),
    Blocked { observed_raw: Option<String> },
}

/// **提交闸门**：带 CAS 把订单推进到 `pending_submit`，只有推进成功才提交。
///
/// 。R5 把这两件事
/// 写成分开的两段（CAS 一段、"要不要提交"另一段），于是：
///
/// - `if advanced == 0` 这条控制流没有任何行为级用例，把它改成永不进入，
///   `kill_switch` 的 26 条测试全绿（MUT-C 存活）；
/// - 中止时用一个**合成的 `Err`** 跳过提交，那个 `Err` 又流进通用错误分支，
///   把 kill-switch 刚落下的 `canceled` 覆盖成 `rejected` 再覆盖成 `unknown`。
///
/// 合成一个函数之后，"影响 0 行就不提交"这条控制流可以被直接驱动并观察：
/// 返回 `Blocked` 时 `submit` 这个 future **一次都不会被 poll**（未被 poll 的
/// future 不产生任何副作用），因此"交易所有没有收到请求"是可断言的事实，
/// 而不是靠读代码推断。
///
/// - `Advanced(result)`：exact `pending_risk` 已推进到 `pending_submit`，`submit` 已执行；
/// - `Blocked`：本次没有取得提交所有权，原始状态仅供审计，调用方必须中止。
pub(crate) async fn advance_then_submit<Fut>(
    pool: &sqlx::Pool<sqlx::Any>,
    db_kind: DbKind,
    credential_id: &str,
    engine_order_id: &str,
    updated_at_ms: i64,
    submit: Fut,
) -> Result<AdvanceThenSubmitOutcome, sqlx::Error>
where
    Fut: std::future::Future<Output = ExchangeAttempt>,
{
    // ****：这里此前是 `.unwrap_or(0)`。
    // 于是**数据库故障**与**这一行已终态**返回同一个结果，调用方据此落
    // `ORDER_CANCELLED_BEFORE_SUBMIT` 审计事件——**把一次 DB 失败伪造成
    // 「这单被撤了」这条交易真值**，正是 engine safety contract 明令禁止的
    // 「缺失数据被转换为正常」。现在 DB 错误原样上抛，由调用方 fail-closed。
    let status_lhs = exact_engine_order_status_lhs(db_kind)?;
    let advance_sql = format!(
        r#"
        UPDATE engine_orders
        SET status = 'pending_submit', updated_at_ms = ?
        WHERE credential_id = ? AND engine_order_id = ?
          AND {status_lhs} = ?
        "#
    );
    let advanced = sqlx::query(&advance_sql)
        .bind(updated_at_ms)
        .bind(credential_id)
        .bind(engine_order_id)
        .bind("pending_risk")
        .execute(pool)
        .await?
        .rows_affected();
    match advanced {
        1 => Ok(AdvanceThenSubmitOutcome::Advanced(submit.await)),
        0 => {
            // 这次读只是 CAS=0 的审计快照。即使读到 canonical
            // `pending_risk`（例如竞态中另一路又改回），也绝不在这里重试、
            // 不 poll future；新一次授权必须由新的顶层调用获得。
            let observed_raw = sqlx::query(
                "SELECT status FROM engine_orders \
                 WHERE credential_id = ? AND engine_order_id = ?",
            )
            .bind(credential_id)
            .bind(engine_order_id)
            .fetch_all(pool)
            .await?
            .into_iter()
            .next()
            .map(|row| row.try_get::<String, _>("status"))
            .transpose()?;
            Ok(AdvanceThenSubmitOutcome::Blocked { observed_raw })
        }
        rows => Err(sqlx::Error::Protocol(format!(
            "engine-order submit ownership CAS affected {rows} rows; expected at most one"
        ))),
    }
}

/// 「不知道结果」的写回——**只允许改写仍在等提交结果的行**。
///
/// 与
/// `finalize_pre_submit_status` 的区别只在状态集。提交已经发出去之后，
/// 这一行可能已经被 WS / reconcile 按**交易所真值**写成 `acked` / `filled` 等；
/// 一条迟到的本地 timeout 不得把它降级成 `unknown`——那是用更弱的真值盖掉
/// 更强的真值。只有 `pending_submit`（还在等结果）才轮得到我们说"不知道"。
pub(crate) async fn finalize_status_if_in_flight<'c, E>(
    executor: E,
    db_kind: DbKind,
    credential_id: &str,
    engine_order_id: &str,
    new_status: &str,
    updated_at_ms: i64,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Any>,
{
    let status_lhs = exact_engine_order_status_lhs(db_kind)?;
    let sql = format!(
        "UPDATE engine_orders SET status = ?, updated_at_ms = ? \
         WHERE credential_id = ? AND engine_order_id = ? AND {status_lhs} = ?"
    );
    Ok(sqlx::query(&sql)
        .bind(new_status)
        .bind(updated_at_ms)
        .bind(credential_id)
        .bind(engine_order_id)
        .bind(IN_FLIGHT_STATUS)
        .execute(executor)
        .await?
        .rows_affected())
}

/// 「已经发出去、还在等结果」的那个状态。单列一份，避免谓词四处复制。
const IN_FLIGHT_STATUS: &str = "pending_submit";

/// 提交路径的**终态写回**——只允许改写仍处于提交前状态的行。
///
/// `live_reject` 分支此前是一条无 CAS 的
/// `UPDATE ... SET status='rejected'`，于是提交闸门虽然拦下了这次下单，
/// 紧随其后的后处理仍然把 kill-switch 落下的 `canceled` 盖掉了——
/// 撤单真值丢失，而 `rejected`/`unknown` 又把这行送回 reconcile 的处理面。
///
/// 返回受影响行数：**0 表示这行已经被别人推进到终态**，本次写回被正确地跳过。
pub(crate) async fn finalize_pre_submit_status<'c, E>(
    executor: E,
    db_kind: DbKind,
    credential_id: &str,
    engine_order_id: &str,
    new_status: &str,
    updated_at_ms: i64,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Any>,
{
    let [first, second] = PRE_SUBMIT_STATUSES;
    let status_lhs = exact_engine_order_status_lhs(db_kind)?;
    let sql = format!(
        "UPDATE engine_orders SET status = ?, updated_at_ms = ? \
         WHERE credential_id = ? AND engine_order_id = ? AND {status_lhs} IN (?, ?)"
    );
    Ok(sqlx::query(&sql)
        .bind(new_status)
        .bind(updated_at_ms)
        .bind(credential_id)
        .bind(engine_order_id)
        .bind(first)
        .bind(second)
        .execute(executor)
        .await?
        .rows_affected())
}

/// close-family 数量的权威来源是 position_state 里的 qty——这与 paper /
/// 交易所无关，普通 exit、SL、TP 与 emergency 读的是同一份权威仓位。
///
/// 本函数不按 venue/paper 分叉；`svc_v2` 的生产入口 wiring tests
/// `v2_close_derives_and_rounds_the_authoritative_position_quantity` 与
/// `planned_sl_and_tp_never_submit_an_empty_quantity` 会在真实适配层和提交请求上
/// 钉住这条不变量，重引入 paper-only call-site gate 会直接打红。
///
/// 返回 `None` 表示不需要推导（非 close-family 或调用方已定量）。
pub(crate) async fn resolve_close_family_quantity(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    order_role: OrderRole,
    provided_quantity: &str,
    position_id: Option<&str>,
) -> Result<Option<String>, Status> {
    if !matches!(
        order_role,
        OrderRole::Exit | OrderRole::ProtectionSl | OrderRole::ProtectionTp | OrderRole::Emergency
    ) || !provided_quantity.trim().is_empty()
    {
        return Ok(None);
    }
    let position_id = position_id.ok_or_else(|| {
        Status::failed_precondition("close requires an authoritative position_id")
    })?;
    let quantity = sqlx::query("SELECT qty FROM position_state WHERE position_id=?")
        .bind(position_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| Status::internal(format!("db load close quantity failed: {error}")))?
        .into_iter()
        .next()
        .ok_or_else(|| Status::failed_precondition("close authoritative position disappeared"))?
        .get::<String, _>("qty");
    crate::exchange_filters::validate_authoritative_position_quantity(&quantity).map_err(
        |error| {
            Status::failed_precondition(format!(
                "close authoritative position has no valid positive quantity: {error}"
            ))
        },
    )?;
    Ok(Some(quantity))
}

impl EngineExecutionService {
    pub fn new(store: Store) -> Self {
        let limiter = CredentialRateLimiter::shared(TokenBucketConfig::from_env(None));
        Self {
            store,
            clock: Arc::new(SystemClock),
            limiter,
            binance_streams: BinanceUserStreamManager::new(),
            reconcilers: ReconcilerManager::new(),
            owns_background_exchange_tasks: true,
            market_data: MarketDataManager::new(),
            exchange_filters: ExchangeFilterCache::shared(),
            #[cfg(test)]
            protection_test_adapter: None,
            #[cfg(test)]
            protection_test_submit_fails: false,
            #[cfg(test)]
            submit_client_override: None,
            #[cfg(test)]
            cancel_client_override: None,
        }
    }

    /// A fill callback already runs inside the credential's owned user stream.
    /// Its temporary execution context must submit protection through the same
    /// durable intent/risk path without creating detached stream/reconcile loops.
    fn for_fill_protection(store: Store) -> Self {
        Self {
            owns_background_exchange_tasks: false,
            ..Self::new(store)
        }
    }

    pub async fn start_fill_recovery_on_startup(&self) -> anyhow::Result<()> {
        self.binance_streams
            .start_discovered(self.store.clone(), self.limiter.clone(), &self.reconcilers)
            .await
    }

    #[cfg(test)]
    pub(crate) fn market_data_for_tests(&self) -> MarketDataManager {
        self.market_data.clone()
    }

    /// 启动时按**持久化的 required 集合**重建 kline worker（    /// 段级第五轮复评 regression，High）。
    ///
    /// 这是 `main` 唯一的调用点；`EnsureMarketData` 的路径不变。
    /// **只读订阅表**——`requested_at_ms` 不被刷新（R7 裁定：刷新会让一条
    /// 从未起来的流每次重试都把中断计时清零）。
    /// **`rejected` 必须被消费。**
    ///
    /// R10 让重建返回了 `rejected` 计数，但 `main` 拿到之后无视它继续 serving：
    /// 一个没人消费的计数与只写日志没有区别。现在这条**生产启动入口**
    /// （`main` 调的就是它，不跳任何一层）在拒绝发生时**先建立结构化
    /// not-ready 再返回**——建立失败则整条 `?` 上抛，启动失败。
    pub async fn restore_market_data_on_startup(
        &self,
    ) -> anyhow::Result<crate::market_data::RestoreOutcome> {
        let outcome = self
            .market_data
            .restore_required_streams(&self.store)
            .await?;
        if outcome.rejected > 0 {
            let armed = crate::auto_triggers::arm_dead_market_data_subscriptions(
                &self.store,
                &outcome.rejected_credential_ids,
                now_ms(),
            )
            .await?;
            tracing::error!(
                rejected = outcome.rejected,
                armed_scopes = armed.len(),
                "persisted market data subscriptions cannot start under the current closed \
                 world; armed pause_new for their credentials instead of serving as healthy"
            );
        }
        Ok(outcome)
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// 注入一个**无网络**的交易所客户端替身。
    #[cfg(test)]
    pub(crate) fn with_submit_client_override(mut self, client: BinanceFuturesClient) -> Self {
        self.submit_client_override = Some(client.with_test_store(self.store.clone()));
        self
    }

    /// 把过滤器来源换成固定真值（**无网络**）。
    #[cfg(test)]
    pub(crate) fn with_exchange_filters(mut self, filters: ExchangeFilterCache) -> Self {
        self.exchange_filters = filters;
        self
    }

    /// 关掉两条**会联真实交易所**的后台任务（user stream / 周期对账）。
    ///
    /// 它们不在被证明的提交链路上：本用例证明的是「submit client 的分类
    /// → 写回语义」，而这两条是 `ExecuteIntents` 顺带做的编排。
    /// §4 要求交易路径测试默认 dry-run，因此接线用例必须能不起它们。
    #[cfg(test)]
    pub(crate) fn without_background_exchange_tasks(mut self) -> Self {
        self.binance_streams = BinanceUserStreamManager::disabled_for_tests();
        self.reconcilers = ReconcilerManager::disabled_for_tests();
        self
    }

    /// **SEG3-R1-001 专用**：让周期对账**真的起来**，但交易所那一端换成
    /// 无网络的响应队列桩。
    ///
    /// 与 `without_background_exchange_tasks` 组合使用：user stream 仍然关掉
    /// （它会真的连交易所），对账则走完整的 `ensure_running` → spawn → 句柄
    /// 登记 → 启动对账 → 落库。风控**之前**那次 `ensure_running` 是否真的
    /// 执行，因此在库里可观察。
    #[cfg(test)]
    pub(crate) fn with_stubbed_reconcile_exchange(mut self, client: BinanceFuturesClient) -> Self {
        self.reconcilers = ReconcilerManager::with_stubbed_exchange_for_tests(client);
        self
    }

    /// 对账任务句柄注册表的只读视图（测试可观察面）。
    #[cfg(test)]
    pub(crate) fn reconcilers_for_tests(&self) -> &ReconcilerManager {
        &self.reconcilers
    }

    #[cfg(test)]
    pub(crate) fn with_protection_test_adapter(
        mut self,
        adapter: Arc<dyn protection::ProtectionAdapter>,
        submit_fails: bool,
    ) -> Self {
        self.protection_test_adapter = Some(adapter);
        self.protection_test_submit_fails = submit_fails;
        self
    }

    fn circuit_breaker_window_ms() -> i64 {
        let v = std::env::var("RUSTCORE_CIRCUIT_BREAKER_WINDOW_S")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(60)
            .clamp(10, 600);
        v * 1000
    }

    fn circuit_breaker_fails() -> i64 {
        std::env::var("RUSTCORE_CIRCUIT_BREAKER_FAILS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(5)
            .clamp(1, 100)
    }

    /// **段③ 批 0（H-01，High）· ACK 清零只清熔断器，碰不到对账差异。**
    ///
    /// 旧实现要在这里"保护" `reconcile_diff` 降级，守卫是
    /// `degraded_reason = 'reconcile_diff'`（按**原因**）；而对账那边的置位
    /// 守卫是 `WHEN degraded = 0`（按**标志**）。交易所错误先占住标志，差异
    /// 就一个字都没写进健康行——于是这里判的正是那个**没被写进去**的值，
    /// 保护不命中，走 `ELSE 0`。降级期间一张被允许的平仓/保护单 ACK 成功，
    /// 就把开仓闸门开了（`credential_degrade_guard_disposition` 只 `RejectEntry`，
    /// close / reduce-only / 保护单 / 应急单一律放行，而两处调用点挂在**任意**
    /// `Acked` 上、不分 order_role）。
    ///
    /// 修法不是把守卫写得更聪明，是把槽位还给对账：`reconcile_diff_open`
    /// 独立成列（迁移 `0029`），只有对账写它。这个函数从此**一个字都不碰**
    /// 那一列——它只清熔断器自己的 `degraded` / `degraded_reason` / 计数窗口。
    /// `RUSTCORE_CLEAR_RECONCILE_DIFF_DEGRADE` 这个开关随之删除：它守的那件事
    /// 已经不在这个函数的写入面里了（部署面影响见 `0029` 与批报告）。
    pub(crate) async fn clear_credential_errors(
        &self,
        credential_id: &str,
    ) -> Result<(), sqlx::Error> {
        let ts = now_ms();
        let q = match self.store.kind() {
            DbKind::MySql => {
                r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, 0)
                ON DUPLICATE KEY UPDATE
                  degraded=0,
                  degraded_reason=NULL,
                  consecutive_errors=0,
                  first_error_ms=0,
                  last_error_ms=0,
                  updated_at_ms=VALUES(updated_at_ms)
                "#
            }
            _ => {
                r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, 0)
                ON CONFLICT(credential_id) DO UPDATE SET
                  degraded=0,
                  degraded_reason=NULL,
                  consecutive_errors=0,
                  first_error_ms=0,
                  last_error_ms=0,
                  updated_at_ms=excluded.updated_at_ms
                "#
            }
        };
        // 新建行取 `reconcile_diff_open = 0`（不是 `NULL`）：健康行只由实盘
        // 交易所路径创建，**没有行**就没有过 `degraded=1`，也就不可能发生
        // 掩盖——"这个凭据此前没有任何对账差异"在这里是可证的，不是猜的。
        // 已存在的行**不**在 `ON CONFLICT` 分支里出现，一个字都不改。
        sqlx::query(q)
            .bind(credential_id)
            .bind(ts)
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub(crate) async fn record_credential_error(
        &self,
        credential_id: &str,
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        let ts = now_ms();
        let window_ms = Self::circuit_breaker_window_ms();
        let fails = Self::circuit_breaker_fails();

        // 先保证主键行存在，再用**一条数据库内 UPDATE**推进窗口。旧实现的
        // SELECT → Rust 计算 → UPSERT 是 read-modify-write：两个并发失败都读到
        // n，随后各自写 n+1，永久少记一次，熔断阈值因此可被并发流量绕过。
        // 这里每个失败只对应一条原子增量；数据库对同一主键行串行化 UPDATE。
        let insert = match self.store.kind() {
            DbKind::MySql => {
                r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, 0)
                ON DUPLICATE KEY UPDATE
                  credential_id=VALUES(credential_id)
                "#
            }
            _ => {
                r#"
                INSERT INTO engine_credential_health (
                  credential_id, degraded, degraded_reason, consecutive_errors,
                  first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
                ) VALUES (?, 0, NULL, 0, 0, 0, ?, 0)
                ON CONFLICT(credential_id) DO NOTHING
                "#
            }
        };
        sqlx::query(insert)
            .bind(credential_id)
            .bind(ts)
            .execute(self.store.pool())
            .await?;

        let cutoff_ms = ts.saturating_sub(window_ms);
        let active_window = format!(
            "first_error_ms > 0 AND last_error_ms > 0 \
             AND first_error_ms >= {cutoff_ms} AND last_error_ms >= {cutoff_ms}"
        );
        let next_count = format!(
            "CASE WHEN {active_window} THEN \
               CASE WHEN consecutive_errors < 9223372036854775807 \
                 THEN consecutive_errors + 1 ELSE consecutive_errors END \
             ELSE 1 END"
        );
        // `degraded_reason` 必须排在 `degraded` 前：MySQL 的单表 UPDATE 赋值按
        // 从左到右可见，反过来会在恰好跨阈值时看到新 degraded=1、却保留 NULL。
        let update = format!(
            "UPDATE engine_credential_health SET \
               degraded_reason = CASE \
                 WHEN degraded <> 0 THEN degraded_reason \
                 WHEN ({next_count}) >= ? THEN ? ELSE degraded_reason END, \
               degraded = CASE \
                 WHEN degraded <> 0 THEN degraded \
                 WHEN ({next_count}) >= ? THEN 1 ELSE 0 END, \
               consecutive_errors = {next_count}, \
               first_error_ms = CASE WHEN {active_window} THEN first_error_ms ELSE ? END, \
               last_error_ms = ?, updated_at_ms = ? \
             WHERE credential_id = ?"
        );
        let bounded_reason: String = reason.trim().chars().take(240).collect();
        let updated = sqlx::query(&update)
            .bind(fails)
            .bind(bounded_reason)
            .bind(fails)
            .bind(ts)
            .bind(ts)
            .bind(ts)
            .bind(credential_id)
            .execute(self.store.pool())
            .await?;
        if updated.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "credential health row disappeared during atomic error update".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn is_credential_degraded(
        &self,
        credential_id: &str,
    ) -> Result<bool, sqlx::Error> {
        let ts = now_ms();
        let window_ms = Self::circuit_breaker_window_ms();
        let fails = Self::circuit_breaker_fails();

        let row = sqlx::query(
            r#"
            SELECT degraded, consecutive_errors,
                   COALESCE(reconcile_diff_open, 1) AS reconcile_diff_open,
                   CAST(first_error_ms AS CHAR) AS first_error_ms_text,
                   CAST(last_error_ms AS CHAR) AS last_error_ms_text
            FROM engine_credential_health
            WHERE credential_id = ?
            LIMIT 1
            "#,
        )
        .bind(credential_id)
        .fetch_optional(self.store.pool())
        .await?;

        let Some(row) = row else {
            return Ok(false);
        };

        let degraded: i64 = row.try_get("degraded")?;
        if degraded != 0 {
            return Ok(true);
        }
        // **段③ 批 0（M-04）· 两个读侧不得再分叉。**
        // 段②完成评审实测到 `risk.rs` 的闸门与这里对同一个凭据给出**相反**的
        // 结论。这个方法的返回值只进审计 payload、不拦单，但一个不拦单的读侧
        // 更没有理由说反话。判据与闸门逐字同源：`degraded` 或对账差异未消解。
        // `COALESCE(..., 1)`：0029 之前的存量行本列是 `NULL`（从未被写过），
        // "证不出没有差异"在这个判据上等价于"有差异"（engine safety contract）。
        let reconcile_diff_open: i64 = row.try_get("reconcile_diff_open")?;
        if reconcile_diff_open != 0 {
            return Ok(true);
        }
        let consecutive: i64 = row.try_get("consecutive_errors")?;
        if consecutive < fails {
            return Ok(false);
        }
        // **sqlx-i64 核查批**：同 `record_credential_error`。
        // 判据是"降级"，所以解码失败不能变成 healthy=false。本方法保留
        // `Err`；生产调用方在上面显式把它解析为 degraded=true 并记录原错误。
        let first_ms = crate::store::try_get_i64_text(&row, "first_error_ms_text")?;
        let last_ms = crate::store::try_get_i64_text(&row, "last_error_ms_text")?;
        if first_ms <= 0 || last_ms <= 0 {
            return Ok(false);
        }
        if ts.saturating_sub(last_ms) > window_ms {
            return Ok(false);
        }
        Ok(ts.saturating_sub(first_ms) <= window_ms)
    }

    pub(crate) async fn submit_planned_protection(
        &self,
        position_id: &str,
        run_mode: &str,
    ) -> Result<Vec<IntentAck>, Status> {
        let confirmed_sl = sqlx::query(
            r#"
            SELECT 1 FROM protection_orders
            WHERE position_id=? AND kind='sl' AND verified_from_exchange=1
            LIMIT 1
            "#,
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load confirmed SL failed: {error}")))?
        .is_some();
        if confirmed_sl {
            sqlx::query(
                "UPDATE position_state SET close_revision=close_revision+1, state='protected', updated_at_ms=? WHERE position_id=? AND state IN ('open','protected')",
            )
            .bind(now_ms())
            .bind(position_id)
            .execute(self.store.pool())
            .await
            .map_err(|error| Status::internal(format!("db mark position protected failed: {error}")))?;
            return Ok(Vec::new());
        }

        let row = sqlx::query(
            r#"
            SELECT credential_id, bot_uid, COALESCE(run_id, '') AS run_id,
                   COALESCE(cycle_id, '') AS cycle_id, exchange, market_kind,
                   symbol_key, direction, stop_loss_price, take_profit_price,
                   confirm_timeout_ms, generation
            FROM position_protection_plans WHERE position_id=? LIMIT 1
            "#,
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load protection plan failed: {error}")))?
        .ok_or_else(|| {
            Status::failed_precondition("active position has no typed protection plan")
        })?;

        let credential_id = row.get::<String, _>("credential_id");
        let bot_uid = row.get::<String, _>("bot_uid");
        let run_id = row.get::<String, _>("run_id");
        let cycle_id = row.get::<String, _>("cycle_id");
        let exchange = row.get::<String, _>("exchange");
        let market_kind = row.get::<String, _>("market_kind");
        let symbol_key = row.get::<String, _>("symbol_key");
        let direction = row.get::<String, _>("direction");
        let generation = row.get::<i64, _>("generation").max(1);
        let timeout_ms = row.get::<i64, _>("confirm_timeout_ms").max(1) as u64;
        let action = if direction == "short" {
            "close_short"
        } else {
            "close_long"
        };
        let mut intents = Vec::new();
        let mut contexts = HashMap::new();
        for (kind, order_type, trigger_price) in [
            ("sl", "stop_market", row.get::<String, _>("stop_loss_price")),
            (
                "tp",
                "take_profit_market",
                row.get::<String, _>("take_profit_price"),
            ),
        ] {
            if trigger_price.trim().is_empty() {
                continue;
            }
            let idem = format!("prot|{position_id}|{kind}|{generation}");
            let digest = sha256_hex(&idem);
            let client_order_id_short = format!("p{}", &digest[..31]);
            intents.push(proto::OrderIntent {
                bot_uid: bot_uid.clone(),
                run_id: run_id.clone(),
                cycle_id: cycle_id.clone(),
                idempotency_key: idem.clone(),
                client_order_id_short,
                exchange: exchange.clone(),
                market_kind: market_kind.clone(),
                symbol_key: symbol_key.clone(),
                action: action.to_string(),
                side: direction.clone(),
                order_type: order_type.to_string(),
                price: trigger_price,
                quantity: String::new(),
                reduce_only: true,
                max_position_notional: 0.0,
                max_credential_notional: 0.0,
            });
            contexts.insert(
                idem.clone(),
                V2IntentContext {
                    risk: risk::WireRiskContext {
                        typed_protection_capable: true,
                        data_snapshot_capable: true,
                        data_snapshot: None,
                        intent_id: format!("{idem}:{}", ulid::Ulid::new()),
                        protection: None,
                        valid_until_ms: None,
                        max_slippage_bps: None,
                        config_hash: String::new(),
                        prompt_hash: String::new(),
                        policy_version: String::new(),
                        code_commit: String::new(),
                    },
                    order_role: if kind == "sl" {
                        OrderRole::ProtectionSl
                    } else {
                        OrderRole::ProtectionTp
                    },
                    protection_required: true,
                    confirm_timeout_ms: timeout_ms,
                    protection_generation: generation,
                },
            );
        }
        let mut all_acks = Vec::with_capacity(intents.len());
        for intent in intents {
            let idem = intent.idempotency_key.clone();
            let context = contexts
                .remove(&idem)
                .expect("typed protection context follows generated intent");
            let mut request = Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.clone(),
                run_mode: run_mode.to_string(),
                intents: vec![intent],
            });
            request.extensions_mut().insert(V2ExecutionContext {
                intents: HashMap::from([(idem, context)]),
            });
            let response = Box::pin(
                crate::proto::engine_execution_server::EngineExecution::execute_intents(
                    self, request,
                ),
            )
            .await?
            .into_inner();
            let failed = response
                .acks
                .iter()
                .any(|ack| matches!(ack.status.as_str(), "error" | "rejected"));
            all_acks.extend(response.acks);
            // SL is always first. Once it fails, flatten/pause is already in
            // progress and no TP child may be submitted into the naked window.
            if failed {
                break;
            }
        }
        if !is_exchange_backed_run_mode(run_mode)
            && all_acks
                .iter()
                .all(|ack| !matches!(ack.status.as_str(), "error" | "rejected"))
        {
            let ts = now_ms();
            sqlx::query(
                r#"
                UPDATE protection_orders
                SET status='confirmed', verified_from_exchange=1,
                    last_verified_at_ms=?, updated_at_ms=?
                WHERE position_id=? AND status IN ('requested','submitted')
                "#,
            )
            .bind(ts)
            .bind(ts)
            .bind(position_id)
            .execute(self.store.pool())
            .await
            .map_err(|error| {
                Status::internal(format!("db confirm paper protection failed: {error}"))
            })?;
            sqlx::query(
                "UPDATE position_state SET close_revision=close_revision+1, state='protected', updated_at_ms=? WHERE position_id=? AND state='open'",
            )
            .bind(ts)
            .bind(position_id)
            .execute(self.store.pool())
            .await
            .map_err(|error| Status::internal(format!("db protect paper position failed: {error}")))?;
        }
        Ok(all_acks)
    }

    pub(crate) async fn reconcile_planned_protection(
        &self,
        position_id: &str,
        run_mode: &str,
    ) -> Result<Vec<IntentAck>, Status> {
        let plan = sqlx::query(
            r#"
            SELECT credential_id, bot_uid, generation
            FROM position_protection_plans WHERE position_id=? LIMIT 1
            "#,
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load recovery plan failed: {error}")))?
        .ok_or_else(|| Status::failed_precondition("active position has no protection plan"))?;
        let credential_id = plan.get::<String, _>("credential_id");
        let bot_uid = plan.get::<String, _>("bot_uid");
        let venue = venue_for_credential(&credential_id).map_err(Status::failed_precondition)?;
        if let Some(reason) =
            demo_submission_reject_reason(run_mode, venue, OrderRole::ProtectionSl)
        {
            return Err(Status::failed_precondition(format!(
                "rejected: {reason}; demo protection requires an explicitly configured testnet venue"
            )));
        }

        #[cfg(test)]
        let test_adapter = self.protection_test_adapter.clone();
        #[cfg(not(test))]
        let test_adapter: Option<Arc<dyn protection::ProtectionAdapter>> = None;
        let adapter: Option<Arc<dyn protection::ProtectionAdapter>> = if test_adapter.is_some() {
            test_adapter
        } else if is_exchange_backed_run_mode(run_mode) {
            let keys = keys_from_env(&credential_id)
                .map_err(Status::failed_precondition)?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "missing exchange credentials for recovery protection check",
                    )
                })?;
            Some(Arc::new(
                protection::BinanceProtectionAdapter::new(
                    self.store.clone(),
                    keys,
                    &credential_id,
                    &bot_uid,
                    self.limiter.clone(),
                )
                .map_err(|error| {
                    Status::internal(format!("build recovery protection adapter failed: {error}"))
                })?,
            ))
        } else {
            None
        };

        let Some(adapter) = adapter else {
            return self.submit_planned_protection(position_id, run_mode).await;
        };
        let existing = sqlx::query(
            r#"
            SELECT po.protection_order_id, po.generation,
                   COALESCE(po.exchange_order_id, '') AS legacy_exchange_order_id,
                   COALESCE(po.exchange_algo_id, '') AS exchange_algo_id,
                   COALESCE(po.client_algo_id, '') AS client_algo_id
            FROM protection_orders po
            JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
            WHERE po.position_id=? AND po.kind='sl'
            ORDER BY po.generation DESC LIMIT 1
            "#,
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db load recovery SL truth key failed: {error}"))
        })?;
        let position_open = sqlx::query(
            "SELECT 1 FROM position_state WHERE position_id=? AND state <> 'closed' LIMIT 1",
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load recovery position failed: {error}")))?
        .is_some();
        if let Some(row) = existing.as_ref() {
            let exchange_algo_id = row.get::<String, _>("exchange_algo_id");
            let client_algo_id = row.get::<String, _>("client_algo_id");
            let legacy_exchange_order_id = row.get::<String, _>("legacy_exchange_order_id");
            if exchange_algo_id.is_empty()
                && client_algo_id.is_empty()
                && !legacy_exchange_order_id.is_empty()
            {
                if position_open {
                    return Err(Status::unavailable(
                        "legacy ordinary protection identity requires ordinary-order reconciliation; duplicate Algo protection suppressed",
                    ));
                }
                return Ok(Vec::new());
            }
            let (query_algo_id, query_client_algo_id) = if !client_algo_id.is_empty() {
                (None, Some(client_algo_id.as_str()))
            } else {
                (
                    (!exchange_algo_id.is_empty()).then_some(exchange_algo_id.as_str()),
                    None,
                )
            };
            let truth = adapter
                .query_algo_order(query_algo_id, query_client_algo_id)
                .await
                .map_err(|error| {
                    Status::unavailable(format!(
                        "exchange protection truth query by clientAlgoId failed: {error}"
                    ))
                })?;
            let disposition = if let Some(truth) = truth.as_ref() {
                let protection_order_id = row.get::<String, _>("protection_order_id");
                Some(
                    protection::apply_algo_truth_for_protection(
                        &self.store,
                        &credential_id,
                        &protection_order_id,
                        truth,
                        protection::AlgoTruthSource::RestAlgoQuery,
                        "recovery",
                        now_ms(),
                    )
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "db restore recovery algo identity failed: {error}"
                        ))
                    })?,
                )
            } else {
                None
            };
            match protection::recovery_protection_action(position_open, disposition) {
                protection::RecoveryProtectionAction::ReuseActive
                | protection::RecoveryProtectionAction::BindTriggered => {
                    sqlx::query(
                        "UPDATE position_state SET close_revision=close_revision+1, state='protected', updated_at_ms=? WHERE position_id=? AND state IN ('open','protected')",
                    )
                    .bind(now_ms())
                    .bind(position_id)
                    .execute(self.store.pool())
                    .await
                    .map_err(|error| {
                        Status::internal(format!("db restore protected state failed: {error}"))
                    })?;
                    return Ok(Vec::new());
                }
                protection::RecoveryProtectionAction::NoPosition => return Ok(Vec::new()),
                protection::RecoveryProtectionAction::HaltUnknown => {
                    return Err(Status::unavailable(
                        "recovery algo truth is incomplete; duplicate protection suppressed",
                    ));
                }
                protection::RecoveryProtectionAction::RepairMissing => {}
            }
        } else if !position_open {
            return Ok(Vec::new());
        }

        let next_generation = existing
            .as_ref()
            .map(|row| row.get::<i64, _>("generation") + 1)
            .unwrap_or_else(|| plan.get::<i64, _>("generation").max(1));
        let mut tx = self.store.pool().begin().await.map_err(|error| {
            Status::internal(format!("db begin recovery protection FIX failed: {error}"))
        })?;
        sqlx::query(
            "UPDATE protection_orders SET verified_from_exchange=0, status='missing_recovery', updated_at_ms=? WHERE position_id=? AND kind='sl'",
        )
        .bind(now_ms())
        .bind(position_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db mark missing recovery SL failed: {error}")))?;
        sqlx::query(
            "UPDATE position_protection_plans SET generation=?, updated_at_ms=? WHERE position_id=?",
        )
        .bind(next_generation)
        .bind(now_ms())
        .bind(position_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db advance protection generation failed: {error}")))?;
        tx.commit().await.map_err(|error| {
            Status::internal(format!("db commit recovery protection FIX failed: {error}"))
        })?;
        let acks = self
            .submit_planned_protection(position_id, run_mode)
            .await?;
        if !is_exchange_backed_run_mode(run_mode)
            || acks
                .iter()
                .any(|ack| matches!(ack.status.as_str(), "error" | "rejected"))
        {
            return Ok(acks);
        }

        // Recovery cannot report convergence merely because a replacement SL
        // was accepted by the venue. Prove the exact exchange order id is in
        // openOrders before allowing the orchestrator to leave `recovering`;
        // otherwise the existing fail-closed action flattens and pauses.
        let replacement = sqlx::query(
            r#"
            SELECT protection_order_id
            FROM protection_orders
            WHERE position_id=? AND kind='sl'
            ORDER BY generation DESC LIMIT 1
            "#,
        )
        .bind(position_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db load replacement recovery SL failed: {error}"))
        })?
        .ok_or_else(|| Status::failed_precondition("recovery FIX did not persist an SL"))?;
        let protection_order_id = replacement.get::<String, _>("protection_order_id");
        let verified =
            protection::verify_or_flatten(&self.store, &protection_order_id, adapter.as_ref())
                .await
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "recovery replacement SL verification failed: {error}"
                    ))
                })?;
        match verified {
            protection::VerificationOutcome::PositionClosed => return Ok(acks),
            protection::VerificationOutcome::FlattenStarted => {
                return Err(Status::failed_precondition(
                    "recovery replacement SL absent from exchange; flatten started",
                ));
            }
            protection::VerificationOutcome::Confirmed => {}
        }
        sqlx::query(
            "UPDATE position_state SET close_revision=close_revision+1, state='protected', updated_at_ms=? WHERE position_id=? AND state IN ('open','protected')",
        )
        .bind(now_ms())
        .bind(position_id)
        .execute(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db mark replacement SL protected failed: {error}"))
        })?;
        Ok(acks)
    }
}

#[tonic::async_trait]
impl EngineExecution for EngineExecutionService {
    async fn execute_intents(
        &self,
        request: Request<ExecuteIntentsRequest>,
    ) -> Result<Response<ExecuteIntentsResponse>, Status> {
        let exact_close = request
            .extensions()
            .get::<crate::position_close::ExactCloseContext>()
            .cloned();
        let v2_context = request.extensions().get::<V2ExecutionContext>().cloned();
        let req = request.into_inner();
        let credential_id = req.credential_id;
        let run_mode = req.run_mode;
        let run_mode_norm = run_mode.trim().to_lowercase();
        let is_exchange_backed = is_exchange_backed_run_mode(&run_mode_norm);
        let evaluation_venue = venue_for_credential(&credential_id).ok().flatten();
        let stage_denial = stage_allowed(&run_mode_norm, &credential_id).err();
        let reduce_only_exit_reachable = reduce_only_exit_reaches_exchange_for(
            crate::venue::credential_kind(&credential_id),
            is_binance_live_enabled(),
            evaluation_venue,
        );

        let mut acks: Vec<IntentAck> = Vec::with_capacity(req.intents.len());
        for intent in req.intents {
            let v2_intent = v2_context
                .as_ref()
                .and_then(|context| context.intents.get(&intent.idempotency_key));
            let requested_order_role = v2_intent
                .map(|context| context.order_role)
                .unwrap_or_else(|| protection::infer_v1_order_role(&intent));
            // This is the exact flag later sent to Binance. Exchange quantity
            // normalization must consume the same effective value instead of
            // inferring reduce-only semantics from the order role.
            let effective_reduce_only = intent.action.starts_with("close") || intent.reduce_only;

            // AUD-P2-D13 / Product Owner 2026-08-18 裁决：生产端口 fail-closed
            // 拒绝 v1 open。
            //
            // 冻结的 v1 wire 此前在生产端口接受**无 gate、无 typed protection、
            // 调用方定量**的开仓（legacy_client_quantity 路径）。它绕开的正是
            // §3.3 的确定性限额与 §3.5 的 entry/保护单原子化。
            //
            // 判别符是 V2ExecutionContext 是否存在：svc_v2 对**每条** v2 intent
            // 都 contexts.insert，故 v2_intent.is_none() 等价于裸 v1 调用。
            // 只拦新增风险——平仓与保护腿一律放行（§3.11 同一原则）。
            //
            // R4 RV1-1：判别键是 action，不是 reduce_only 旗标。原条件
            // `!effective_reduce_only` 会让 v1 `open_long` + `reduce_only=true`
            // 绕过拒绝——一个"只减仓的开仓"语义上自相矛盾，却仍以
            // legacy_client_quantity（调用方定量）进风控（评审 RV1-1）。
            //
            // R5 FBL-R4-H2：但 `starts_with("open")` 是**开世界**判别——它只
            // 枚举要拒的形态，于是任何未知 action 都被放行（fail-open）。
            // 判别键改为闭世界白名单，见 v1_action_accepted_on_production_port。
            if v2_intent.is_none() && !v1_action_accepted_on_production_port(&intent.action) {
                acks.push(IntentAck {
                    idempotency_key: intent.idempotency_key,
                    engine_order_id: "".to_string(),
                    status: "rejected".to_string(),
                    message: V1_OPEN_REJECTION.to_string(),
                });
                continue;
            }

            let applicable_stage_denial = stage_denial.as_ref().filter(|denial| {
                stage_denial_applies(
                    denial,
                    requested_order_role,
                    &intent.action,
                    reduce_only_exit_reachable,
                )
            });
            if let Some(denial) = applicable_stage_denial {
                acks.push(IntentAck {
                    idempotency_key: intent.idempotency_key,
                    engine_order_id: "".to_string(),
                    status: "rejected".to_string(),
                    message: denial.to_string(),
                });
                continue;
            }

            if is_exchange_backed
                && (intent.exchange.trim().to_lowercase() != "binance"
                    || intent.market_kind.trim().to_lowercase() != "futures")
            {
                acks.push(IntentAck {
                    idempotency_key: intent.idempotency_key,
                    engine_order_id: "".to_string(),
                    status: "rejected".to_string(),
                    message: "live only supports binance futures (v1)".to_string(),
                });
                continue;
            }

            // **提交侧必须校验完整身份。**
            //
            // 上面那一段只比了 `intent.exchange` / `intent.market_kind` 两个
            // **外层字段**；`intent.symbol_key` 自己声明的 venue/market_kind/
            // settle 一段都没人看。提交侧随后只调 `symbol_from_symbol_key`
            // 取裸 symbol——于是 `okx|spot|BTC|USDT|BTC` 带着一份合规的外层
            // 声明穿过整条链，变成一张发往 Binance USDⓈ-M 的 `BTCUSDT` 单
            //（币本位那条 key 更隐蔽：symbol 同样是 BTCUSDT，却是另一张合约）。
            //
            // 身份判据从此与 WS / 对账**同源**（`usdm_submission_identity`
            // 背后的 `usdm_identity`，全仓唯一一份）：两个身份来源逐段一致
            // 且落在 USDⓈ-M 闭世界内才放行，否则 fail-closed 拒单。
            let exchange_identity = if is_exchange_backed {
                match crate::binance_futures::usdm_submission_identity(
                    &intent.symbol_key,
                    &intent.exchange,
                    &intent.market_kind,
                ) {
                    Ok(identity) => Some(identity),
                    Err(reason) => {
                        acks.push(IntentAck {
                            idempotency_key: intent.idempotency_key,
                            engine_order_id: "".to_string(),
                            status: "rejected".to_string(),
                            message: reason,
                        });
                        continue;
                    }
                }
            } else {
                None
            };

            if is_exchange_backed && self.owns_background_exchange_tasks {
                let stream_symbol = exchange_identity
                    .as_ref()
                    .map(|identity| identity.symbol.clone());
                let stream_keys = keys_from_env(&credential_id);
                match (stream_keys, stream_symbol) {
                    (Ok(Some(keys)), Some(symbol)) => {
                        // **段③ 批 0（H-01）· 收敛不得依赖"先成功开一次仓"。**
                        //
                        // 周期对账此前只在**风控放行之后**的实盘提交分支里
                        // `ensure_running`。`reconcile_diff_open` 为 1（含 0029
                        // 存量行 fail-closed 的那一批）时开仓正是被拦的那一类，
                        // 于是"等下一轮对账用真值收敛"会变成一个死结：不开仓就
                        // 不起对账，不起对账就一直拦开仓。这里把它提到风控之前
                        // ——任何一条实盘意图（含随后被拦下的开仓）都会把对账
                        // 任务拉起来，而对账任务的**启动对账**是立刻跑一次的，
                        // 不等 `RECONCILE_INTERVAL_MS`。`ensure_running` 幂等，
                        // 已在跑就直接返回；提交分支那次调用原样保留。
                        self.reconcilers
                            .ensure_running(
                                &credential_id,
                                self.store.clone(),
                                self.limiter.clone(),
                                keys.clone(),
                            )
                            .await;
                        if let Err(error) = self
                            .binance_streams
                            .ensure_running(
                                &credential_id,
                                &symbol,
                                self.store.clone(),
                                self.limiter.clone(),
                                keys,
                                crate::fill_replay::RecoveryTrigger::Startup,
                            )
                            .await
                        {
                            tracing::error!(credential_id, %symbol, failure_reason=%error, "fill recovery stream orchestration failed");
                        }
                    }
                    (Err(error), symbol) => {
                        tracing::error!(credential_id, symbol=?symbol, failure_reason=%error, "fill recovery credentials unavailable");
                    }
                    (Ok(None), symbol) => {
                        tracing::error!(credential_id, symbol=?symbol, failure_reason="missing Binance key pair", "fill recovery credentials unavailable");
                    }
                    (Ok(Some(_)), None) => {
                        tracing::error!(
                            credential_id,
                            failure_reason = "invalid symbol_key",
                            "fill recovery scope unavailable"
                        );
                    }
                }
            }

            // Identity conflicts are configuration facts, independent of the
            // mutable ready/reconcile health projections. Management tasks above
            // still start; only a new-risk intent is refused here.
            let api_identity_denial =
                if is_exchange_backed && requested_order_role == OrderRole::Entry {
                    crate::account_identity::entry_denial(&self.store, &credential_id).await
                } else {
                    None
                };

            let engine_order_id = ulid::Ulid::new().to_string();
            let created_at_ms = self.clock.now_ms();
            // Resolve venue before either public market-data request. The
            // filter layer never consults the legacy process-wide base URL, so a
            // testnet credential cannot accidentally consume mainnet rules.
            let filter_constraints: Option<Result<ExchangeConstraints, String>> =
                if let Some(reason) = api_identity_denial.as_ref() {
                    Some(Err(reason.clone()))
                } else if is_exchange_backed {
                    let venue = venue_for_filter_fetch(&credential_id)
                        .map_err(|error| format!("venue resolution failed: {error}"));
                    let symbol = exchange_identity
                        .as_ref()
                        .map(|identity| identity.symbol.clone())
                        .ok_or_else(|| "invalid symbol_key".to_string());
                    Some(match (venue, symbol) {
                        (Ok(venue), Ok(symbol)) => {
                            if requested_order_role == OrderRole::Entry {
                                self.exchange_filters
                                    .constraints(venue, &symbol, created_at_ms)
                                    .await
                            } else {
                                self.exchange_filters
                                    .close_constraints(venue, &symbol, created_at_ms)
                                    .await
                            }
                        }
                        (Err(error), _) | (_, Err(error)) => Err(error),
                    })
                } else {
                    None
                };

            // Current-process linearization with Arm; acquire after market I/O,
            // before the OMS transaction. Exit/protection never take this gate.
            let lifecycle_guard = if requested_order_role == OrderRole::Entry {
                Some(crate::lifecycle_fence::lock(&credential_id).await?)
            } else {
                None
            };
            let mut tx = self
                .store
                .pool()
                .begin()
                .await
                .map_err(|e| Status::internal(format!("db begin failed: {e}")))?;

            let lifecycle_blocked = if lifecycle_guard.is_some() {
                crate::lifecycle_fence::entry_blocked(&mut tx, self.store.kind(), &credential_id)
                    .await?
            } else {
                false
            };

            // Lock order: lifecycle credential -> B3 scope -> reservation -> position -> order.
            // Only Entry is denied by a pending exact-close fence. Existing
            // safety reductions share the transaction lock but remain available.
            let entry_close_fenced = crate::position_close::lock_intent_scope(
                &mut tx,
                self.store.kind(),
                &credential_id,
                &intent,
            )
            .await?;
            // D30：同 credential+symbol+direction 存在未确认完成的保护单善后
            // 义务时，新开仓在此准入事务内拒绝（与 §3.11 同原则：只拦新增
            // 风险，减仓/平仓/其他仓位的保护/对账/kill switch 一律放行）。
            // 残留的 closePosition 止损会按旧触发价平掉新仓——先善后，再开仓。
            let protection_cleanup_pending = if requested_order_role == OrderRole::Entry {
                let direction = match intent.action.as_str() {
                    "open_long" => "long",
                    "open_short" => "short",
                    _ => "",
                };
                if direction.is_empty() {
                    false
                } else {
                    crate::protection_cleanup::entry_scope_pending_in_tx(
                        &mut tx,
                        &credential_id,
                        &intent.symbol_key,
                        direction,
                    )
                    .await?
                }
            } else {
                false
            };
            if let Some(context) = exact_close.as_ref() {
                if let Some(reason) =
                    crate::position_close::claim(&mut tx, &self.store, context).await?
                {
                    tx.commit().await.map_err(|error| {
                        Status::internal(format!("commit close claim: {error}"))
                    })?;
                    acks.push(IntentAck {
                        idempotency_key: intent.idempotency_key,
                        engine_order_id: String::new(),
                        status: "duplicate".into(),
                        message: reason.into(),
                    });
                    continue;
                }
            }

            #[cfg(feature = "l4-fault-injection")]
            let checkpoint_verdict_is_pending = std::cell::Cell::new(false);

            // 05-EXECUTION-PLANE §2.1 steps 1-3: load one transaction
            // snapshot, evaluate the pure policy, and append the verdict even
            // when it rejects. The frozen v1 ack carries the stable code in
            // its message prefix.
            // Only an exchange-backed v2 market entry consumes this quote.
            // Close constraints contain a sentinel price and must never enter
            // opening risk, and limit orders retain their limit-price contract.
            let uses_market_reference = is_exchange_backed
                && v2_intent.is_some()
                && requested_order_role == OrderRole::Entry
                && matches!(intent.action.as_str(), "open_long" | "open_short")
                && intent.order_type.trim().eq_ignore_ascii_case("market")
                && !intent.reduce_only;
            // Check age after network I/O AND after acquiring the transaction;
            // reusing the pre-fetch timestamp would hide both kinds of delay.
            let evaluated_at_ms = if uses_market_reference {
                self.clock.now_ms()
            } else {
                created_at_ms
            };
            let entry_market_reference = uses_market_reference.then(|| {
                risk::EntryMarketReference::from_constraints(
                    filter_constraints.as_ref(),
                    &intent.symbol_key,
                    evaluated_at_ms,
                )
            });
            let (mut verdict, reservation_cap, mut replay_input_json) = risk::evaluate_v1(
                &mut tx,
                self.store.kind(),
                &credential_id,
                &intent,
                evaluated_at_ms,
                v2_intent.map(|context| &context.risk),
                &run_mode_norm,
                stage_certification_venue_is_testnet(evaluation_venue),
                requested_order_role,
                entry_market_reference.as_ref(),
            )
            .await?;
            if let Some(reason) = api_identity_denial.as_ref() {
                // Record the observed membership even for a later RPC using
                // global fallback keys. Restart discovery reads this durable
                // rejected verdict; no health bit or fake order is required.
                (verdict, replay_input_json) = risk::replay_rejection(
                    &replay_input_json,
                    "binance_api_identity",
                    crate::account_identity::denial_code(reason),
                    reason,
                )?;
            }
            if lifecycle_blocked {
                (verdict, replay_input_json) = risk::replay_rejection(
                    &replay_input_json,
                    "lifecycle_fence",
                    crate::lifecycle_fence::DENIED,
                    "credential Entry admission is durably fenced on the current executor",
                )?;
            }
            if entry_close_fenced {
                (verdict, replay_input_json) = risk::replay_rejection(
                    &replay_input_json,
                    "position_close_fence",
                    "POSITION_CLOSE_PENDING",
                    "a durable exact close in this position scope has no terminal proof",
                )?;
            }
            if protection_cleanup_pending {
                (verdict, replay_input_json) = risk::replay_rejection(
                    &replay_input_json,
                    "protection_cleanup",
                    crate::protection_cleanup::REJECTION_CODE,
                    "a protection cleanup obligation in this credential+symbol+direction \
                     has no exchange-truth completion proof yet",
                )?;
            }
            // WO-M1：交易所凭据的精确平仓走 OMS 正常交易所路径（市价单由交易所
            // 成交），不需要也不读本地纸面价格；纸面价格闸门只属于 simulation。
            let exact_close_exchange = exact_close.is_some()
                && crate::venue::credential_kind(&credential_id)
                    == crate::venue::CredentialKind::ExchangeBacked;
            let exact_close_price = if exact_close.is_some() && !exact_close_exchange {
                crate::svc_v2::paper_close_price(
                    &mut tx,
                    &credential_id,
                    &intent.symbol_key,
                    self.clock.now_ms(),
                )
                .await?
            } else {
                None
            };
            if exact_close.is_some() && !exact_close_exchange && exact_close_price.is_none() {
                (verdict, replay_input_json) = risk::replay_rejection(
                    &replay_input_json,
                    "position_close_price",
                    "PAPER_MARKET_PRICE_UNAVAILABLE",
                    "no accepted positive 1m bar with non-future closeTs within 60 seconds",
                )?;
            }
            let mut execution_intent = intent.clone();
            if verdict.decision != Decision::Reject
                && matches!(execution_intent.action.as_str(), "open_long" | "open_short")
            {
                let Some(sizing) = verdict.sizing.as_ref() else {
                    return Err(Status::failed_precondition(
                        "approved opening verdict is missing deterministic sizing",
                    ));
                };
                // Exchange normalization is downstream of deterministic risk
                // sizing. The unrounded approved quantity remains in sizing;
                // the exchange_filters audit rule records the actual request.
                execution_intent.quantity = sizing.final_qty().to_string();
            }
            // V2 intentionally carries no quantity. Every close-family child
            // must therefore bind to the engine-owned position row before
            // exchange filters run. The former Exit-only derivation happened
            // after filters and left SL/TP on the empty closePosition shape;
            // Binance's algo endpoint rejected that request with -1102.
            //
            // Preparing inside this transaction also freezes the exact
            // position_id used by both the quantity lookup and the later
            // engine-order insert. Any downstream error rolls the transition
            // back with the transaction.
            let mut close_family_prepared = None;
            if verdict.decision != Decision::Reject
                && matches!(
                    execution_intent.action.as_str(),
                    "close_long" | "close_short"
                )
                && execution_intent.quantity.trim().is_empty()
            {
                let prepared = if let Some(context) = exact_close.as_ref() {
                    crate::position_close::prepare_exact_exit(&mut tx, context, created_at_ms)
                        .await?
                } else if let Some(kind) = requested_order_role.protection_kind() {
                    if v2_intent.is_some() {
                        position_state::prepare_typed_protection_intent(
                            &mut tx,
                            &execution_intent,
                            kind,
                            v2_intent
                                .map(|context| context.protection_generation)
                                .unwrap_or(1),
                        )
                        .await?
                    } else {
                        position_state::prepare_protection_intent(
                            &mut tx,
                            &execution_intent,
                            kind,
                            1,
                        )
                        .await?
                    }
                } else {
                    match position_state::prepare_intent(
                        &mut tx,
                        &credential_id,
                        &execution_intent,
                        created_at_ms,
                    )
                    .await?
                    {
                        PrepareOutcome::Ready(prepared) => prepared,
                        PrepareOutcome::DuplicateActiveEntry { .. } => {
                            return Err(Status::internal(
                                "close-family preparation returned an entry-only duplicate",
                            ));
                        }
                    }
                };
                if let Some(quantity) = resolve_close_family_quantity(
                    &mut tx,
                    requested_order_role,
                    execution_intent.quantity.as_str(),
                    prepared.position_id.as_deref(),
                )
                .await?
                {
                    execution_intent.quantity = quantity;
                }
                close_family_prepared = Some(prepared);
            }
            if verdict.decision != Decision::Reject {
                if let Some(constraints) = filter_constraints.as_ref() {
                    let constraints = constraints.as_ref().map_err(String::as_str);
                    let prepared = apply_exchange_filters(
                        requested_order_role,
                        effective_reduce_only,
                        OrderSide::for_action(&execution_intent.action),
                        &execution_intent.quantity,
                        &execution_intent.price,
                        constraints,
                    );
                    execution_intent.quantity = prepared.quantity.clone();
                    execution_intent.price = prepared.price.clone();
                    (verdict, replay_input_json) =
                        risk::replay_exchange_filter_result(&replay_input_json, &prepared)?;
                }
            }
            // WO-M1：交易所精确平仓只能是**全量**——过滤器取整一旦改变了权威
            // 数量，这张单就不是「平掉这一个仓位」，在同一裁决事务里拒绝。
            if verdict.decision != Decision::Reject && exact_close_exchange {
                if let Some(context) = exact_close.as_ref() {
                    if !crate::position_close::quantity_is_exact(
                        &execution_intent.quantity,
                        context,
                    ) {
                        (verdict, replay_input_json) = risk::replay_rejection(
                            &replay_input_json,
                            "position_close_quantity",
                            "CLOSE_QUANTITY_NOT_EXACT",
                            "the normalized close quantity differs from the exact target quantity",
                        )?;
                    }
                }
            }
            risk::insert_verdict(
                &mut tx,
                &credential_id,
                &intent,
                &verdict,
                &replay_input_json,
            )
            .await?;
            if verdict.decision == Decision::Reject {
                let code = verdict
                    .reject_reason_code
                    .as_deref()
                    .unwrap_or("RISK_POLICY_REJECTED");
                let ctx = EventContext::from_intent(&intent);
                let role = v2_intent
                    .map(|context| context.order_role)
                    .unwrap_or_else(|| protection::infer_v1_order_role(&intent));
                emit_event(
                    &mut tx,
                    self.store.kind(),
                    &credential_id,
                    "reject",
                    &ctx,
                    "",
                    None,
                    Some(verdict_event_payload(
                        &execution_intent,
                        &run_mode,
                        &verdict,
                        role,
                        None,
                    )),
                    Some(sha256_hex(&format!(
                        "risk_verdict|{}|{}",
                        credential_id, verdict.risk_verdict_id
                    ))),
                )
                .await?;
                if let Some(context) = exact_close.as_ref() {
                    // 裁决晚于 closing CAS 拒绝时（交易所精确平仓的数量闸门），
                    // 同一事务把仓位从 closing 按既有失败释放语义放回。
                    if let Some(prepared) = close_family_prepared.as_ref() {
                        if let Some(position_id) = prepared.position_id.as_deref() {
                            position_state::release_closing_after_failed_exit(
                                &mut tx,
                                position_id,
                                created_at_ms,
                            )
                            .await?;
                        }
                    }
                    crate::position_close::finish_rejected(
                        &mut tx,
                        context,
                        code,
                        Some(&verdict.risk_verdict_id),
                    )
                    .await?;
                }
                tx.commit()
                    .await
                    .map_err(|e| Status::internal(format!("db commit reject failed: {e}")))?;
                acks.push(IntentAck {
                    idempotency_key: intent.idempotency_key,
                    engine_order_id: String::new(),
                    status: "rejected".to_string(),
                    message: format!("rejected: {code}; deterministic risk policy"),
                });
                continue;
            }

            if execution_intent.action == "open_long" || execution_intent.action == "open_short" {
                let Some(sizing) = verdict.sizing.as_ref() else {
                    return Err(Status::failed_precondition(
                        "approved opening verdict is missing deterministic sizing",
                    ));
                };
                // Step 4: atomic, conditional reservation. A zero-row update
                // appends a second audit verdict and rejects before position or
                // order insertion.
                let reserved = risk::reserve_notional(
                    &mut tx,
                    &credential_id,
                    &reservation_cap,
                    sizing.final_notional_usdt(),
                    created_at_ms,
                )
                .await?;
                if !reserved {
                    let conflict_detail = "conditional update affected zero rows";
                    let (conflict, conflict_replay_input_json) = risk::replay_rejection(
                        &replay_input_json,
                        "notional_reservation_commit",
                        "RESERVATION_CONFLICT",
                        conflict_detail,
                    )?;
                    risk::insert_verdict(
                        &mut tx,
                        &credential_id,
                        &intent,
                        &conflict,
                        &conflict_replay_input_json,
                    )
                    .await?;
                    let ctx = EventContext::from_intent(&intent);
                    let role = v2_intent
                        .map(|context| context.order_role)
                        .unwrap_or_else(|| protection::infer_v1_order_role(&intent));
                    emit_event(
                        &mut tx,
                        self.store.kind(),
                        &credential_id,
                        "reject",
                        &ctx,
                        "",
                        None,
                        Some(verdict_event_payload(
                            &intent, &run_mode, &conflict, role, None,
                        )),
                        Some(sha256_hex(&format!(
                            "risk_verdict|{}|{}",
                            credential_id, conflict.risk_verdict_id
                        ))),
                    )
                    .await?;
                    tx.commit().await.map_err(|e| {
                        Status::internal(format!("db commit reservation reject failed: {e}"))
                    })?;
                    acks.push(IntentAck {
                        idempotency_key: intent.idempotency_key,
                        engine_order_id: String::new(),
                        status: "rejected".to_string(),
                        message: "rejected: RESERVATION_CONFLICT; conditional reservation update affected zero rows".to_string(),
                    });
                    continue;
                }
            }

            // Step 5: rustcore owns the durable active-position claim and
            // business-semantic idempotency key. The policy rule is auditable;
            // this unique index remains the concurrency backstop.
            // v2 carries an explicit typed role in its per-request adapter.
            // Only frozen v1 is allowed to use the compatibility inference.
            let order_role = v2_intent
                .map(|context| context.order_role)
                .unwrap_or_else(|| protection::infer_v1_order_role(&execution_intent));
            let prepared = if let Some(prepared) = close_family_prepared {
                prepared
            } else if let Some(kind) = order_role.protection_kind() {
                if v2_intent.is_some() {
                    position_state::prepare_typed_protection_intent(
                        &mut tx,
                        &execution_intent,
                        kind,
                        v2_intent
                            .map(|context| context.protection_generation)
                            .unwrap_or(1),
                    )
                    .await?
                } else {
                    position_state::prepare_protection_intent(&mut tx, &execution_intent, kind, 1)
                        .await?
                }
            } else {
                match position_state::prepare_intent(
                    &mut tx,
                    &credential_id,
                    &execution_intent,
                    created_at_ms,
                )
                .await?
                {
                    PrepareOutcome::Ready(prepared) => prepared,
                    PrepareOutcome::DuplicateActiveEntry {
                        authority_idem_key,
                        detail,
                    } => {
                        tx.rollback()
                            .await
                            .map_err(|e| Status::internal(format!("db rollback failed: {e}")))?;
                        let (conflict, conflict_replay_input_json) = risk::replay_rejection(
                            &replay_input_json,
                            "single_active_entry_commit",
                            "DUPLICATE_ACTIVE_ENTRY",
                            &detail,
                        )?;
                        let mut audit_tx = self.store.pool().begin().await.map_err(|e| {
                            Status::internal(format!("db begin duplicate audit failed: {e}"))
                        })?;
                        risk::insert_verdict(
                            &mut audit_tx,
                            &credential_id,
                            &intent,
                            &conflict,
                            &conflict_replay_input_json,
                        )
                        .await?;
                        let mut ctx = EventContext::from_intent(&intent);
                        ctx.idempotency_key = authority_idem_key.clone();
                        let role = v2_intent
                            .map(|context| context.order_role)
                            .unwrap_or_else(|| protection::infer_v1_order_role(&intent));
                        emit_event(
                            &mut audit_tx,
                            self.store.kind(),
                            &credential_id,
                            "reject",
                            &ctx,
                            "",
                            None,
                            Some(verdict_event_payload(
                                &intent, &run_mode, &conflict, role, None,
                            )),
                            Some(sha256_hex(&format!(
                                "risk_verdict|{}|{}",
                                credential_id, conflict.risk_verdict_id
                            ))),
                        )
                        .await?;
                        audit_tx.commit().await.map_err(|e| {
                            Status::internal(format!("db commit duplicate audit failed: {e}"))
                        })?;
                        acks.push(IntentAck {
                        idempotency_key: intent.idempotency_key,
                        engine_order_id: "".to_string(),
                        status: "rejected".to_string(),
                        message: format!(
                            "rejected: DUPLICATE_ACTIVE_ENTRY; {detail}; authoritative_idempotency_key={authority_idem_key}"
                        ),
                    });
                        continue;
                    }
                }
            };
            let authority_idem_key = prepared.authority_idem_key.clone();
            let intent = execution_intent;

            if inserted_protection_plan_candidate(order_role, v2_intent) {
                let position_id = prepared.position_id.as_deref().ok_or_else(|| {
                    Status::failed_precondition("v2 protected entry requires position_id")
                })?;
                let context = v2_intent.expect("candidate has v2 context");
                protection::persist_plan(
                    &mut tx,
                    position_id,
                    &credential_id,
                    &intent,
                    &context.risk,
                    verdict
                        .sizing
                        .as_ref()
                        .and_then(|sizing| sizing.effective_stop_loss_price()),
                    context.protection_required,
                    context.confirm_timeout_ms,
                    created_at_ms,
                )
                .await?;
            }

            #[cfg(feature = "l4-fault-injection")]
            if requested_order_role == OrderRole::Entry
                && crate::fault_injection::selected("in_oms_tx_after_verdict_before_order")
            {
                let pending_verdict_count =
                    sqlx::query("SELECT COUNT(*) AS n FROM risk_verdicts WHERE risk_verdict_id=?")
                        .bind(&verdict.risk_verdict_id)
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(|error| {
                            Status::internal(format!(
                                "L4 pending verdict observation failed: {error}"
                            ))
                        })?
                        .get::<i64, _>("n");
                checkpoint_verdict_is_pending.set(pending_verdict_count == 1);
            }

            #[cfg(feature = "l4-fault-injection")]
            if requested_order_role == OrderRole::Entry {
                crate::fault_injection::require_transaction_truth(
                    "in_oms_tx_after_verdict_before_order",
                    checkpoint_verdict_is_pending.get(),
                )
                .map_err(|error| {
                    Status::internal(format!("L4 fault checkpoint failed: {error}"))
                })?;
                crate::fault_injection::checkpoint("in_oms_tx_after_verdict_before_order")
                    .map_err(|error| {
                        Status::internal(format!("L4 fault checkpoint failed: {error}"))
                    })?;
            }

            // Simulation protection only arms a conditional order. It is not
            // a position fill; the durable protection confirmation follows the ACK.
            let paper_fill = !matches!(
                order_role,
                OrderRole::ProtectionSl | OrderRole::ProtectionTp
            );

            // Step 6: the only engine-order insertion helper requires a
            // non-empty verdict id before issuing SQL.
            let inserted = risk::insert_engine_order(
                &mut tx,
                &engine_order_id,
                &credential_id,
                &intent,
                &authority_idem_key,
                prepared.position_id.as_deref(),
                order_role.as_str(),
                Some(&verdict.risk_verdict_id),
                if is_exchange_backed {
                    "pending_risk"
                } else if paper_fill {
                    "filled"
                } else {
                    "acked"
                },
                created_at_ms,
            )
            .await?;

            if inserted && order_role == OrderRole::Entry {
                let position_id = prepared.position_id.as_deref().ok_or_else(|| {
                    Status::failed_precondition("entry provenance requires position")
                })?;
                crate::position_close::persist_entry_provenance(
                    &mut tx,
                    &credential_id,
                    &intent,
                    position_id,
                    &engine_order_id,
                    &run_mode_norm,
                    created_at_ms,
                )
                .await?;
            }

            let protection_order_id = if inserted {
                if let (Some(kind), Some(position_id)) = (
                    order_role.protection_kind(),
                    prepared.position_id.as_deref(),
                ) {
                    let protection_order_id = format!("prot_{}", ulid::Ulid::new());
                    protection::insert_requested(
                        &mut tx,
                        &protection_order_id,
                        position_id,
                        &credential_id,
                        kind,
                        v2_intent
                            .map(|context| context.protection_generation)
                            .unwrap_or(1),
                        &engine_order_id,
                        &intent.client_order_id_short,
                        &intent.price,
                        intent.quantity.trim().is_empty(),
                        created_at_ms,
                    )
                    .await?;
                    Some(protection_order_id)
                } else {
                    None
                }
            } else {
                None
            };

            let mut protection_dispatch = if inserted && is_exchange_backed {
                if let Some(protection_id) = protection_order_id.as_deref() {
                    let symbol = exchange_identity
                        .as_ref()
                        .ok_or_else(|| {
                            Status::internal("protection dispatch missing exchange identity")
                        })?
                        .symbol
                        .clone();
                    let close_position = effective_reduce_only && intent.quantity.trim().is_empty();
                    let wire = crate::protection_dispatch::AlgoWire {
                        symbol,
                        side: OrderSide::for_action(&intent.action)
                            .as_binance()
                            .to_owned(),
                        order_type: intent.order_type.to_ascii_uppercase(),
                        trigger_price: intent.price.trim().to_owned(),
                        quantity: if close_position {
                            None
                        } else {
                            Some(intent.quantity.trim().to_owned())
                        },
                        reduce_only: effective_reduce_only,
                        close_position,
                        client_algo_id: intent.client_order_id_short.clone(),
                    };
                    Some(
                        crate::protection_dispatch::prepare_in_tx(
                            &self.store,
                            &mut tx,
                            &engine_order_id,
                            protection_id,
                            wire,
                            created_at_ms,
                        )
                        .await
                        .map_err(|error| {
                            Status::internal(format!("prepare protection dispatch failed: {error}"))
                        })?,
                    )
                } else {
                    None
                }
            } else {
                None
            };

            let final_order_id = if inserted {
                engine_order_id.clone()
            } else {
                if let Some(sizing) = verdict.sizing.as_ref() {
                    risk::release_notional(
                        &mut tx,
                        &credential_id,
                        sizing.final_notional_usdt(),
                        created_at_ms,
                    )
                    .await?;
                }
                let row = sqlx::query(
                    r#"
                    SELECT engine_order_id
                    FROM engine_orders
                    WHERE bot_uid = ? AND idempotency_key = ?
                    LIMIT 1
                    "#,
                )
                .bind(&intent.bot_uid)
                .bind(&authority_idem_key)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Status::internal(format!("db select existing failed: {e}")))?;
                row.get::<String, _>("engine_order_id")
            };

            let mut ctx = EventContext::from_intent(&intent);
            ctx.idempotency_key = authority_idem_key.clone();

            // The authoritative verdict and its replay event commit with the
            // engine order. A later exchange submit/ack may fail or the
            // process may restart; neither may make the audit copy
            // permanently unreportable.
            emit_event(
                &mut tx,
                self.store.kind(),
                &credential_id,
                "risk_verdict",
                &ctx,
                "",
                None,
                Some(verdict_event_payload(
                    &intent,
                    &run_mode,
                    &verdict,
                    order_role,
                    prepared.position_id.as_deref(),
                )),
                Some(sha256_hex(&format!(
                    "risk_verdict|{}|{}",
                    credential_id, verdict.risk_verdict_id
                ))),
            )
            .await?;

            if inserted && prepared.orphan_recovery {
                let warning_code = "ORPHAN_CLOSE_WITHOUT_LOCAL_POSITION";
                tracing::warn!(
                    credential_id = %credential_id,
                    bot_uid = %intent.bot_uid,
                    symbol_key = %intent.symbol_key,
                    direction = %prepared.direction.as_deref().unwrap_or_default(),
                    warning_code,
                    "close request targets a position with no local position_state record"
                );
                emit_event(
                    &mut tx,
                    self.store.kind(),
                    &credential_id,
                    "error",
                    &ctx,
                    &final_order_id,
                    None,
                    Some(serde_json::json!({
                        "severity": "warning",
                        "code": warning_code,
                        "message": "close request targets a position with no local position_state record",
                        "source": "position_state_orphan_recovery",
                        "action": intent.action.as_str(),
                        "direction": prepared.direction.as_deref(),
                        "recoveryRecorded": true,
                    })),
                    Some(sha256_hex(&format!(
                        "orphan_close_warning|{}|{}",
                        credential_id, authority_idem_key
                    ))),
                )
                .await?;
            }

            if inserted && !is_exchange_backed {
                // Every accepted paper order acknowledges submission.
                let mut ack_payload = verdict_event_payload(
                    &intent,
                    &run_mode,
                    &verdict,
                    order_role,
                    prepared.position_id.as_deref(),
                );
                if let Some(payload) = ack_payload.as_object_mut() {
                    payload.insert("paper".to_string(), serde_json::Value::Bool(true));
                }
                emit_event(
                    &mut tx,
                    self.store.kind(),
                    &credential_id,
                    "order_ack",
                    &ctx,
                    &final_order_id,
                    None,
                    Some(ack_payload),
                    None,
                )
                .await?;
            }

            if inserted && !is_exchange_backed && paper_fill {
                let fill_price = if let Some(price) = exact_close_price.as_ref() {
                    price.clone()
                } else if intent.price.is_empty() {
                    // Paper market fills should use a reasonable market price; otherwise downstream UI/PNL become garbage.
                    // Prefer latest bar close from subscribed market-data stream; fallback to 0 only if market data unavailable.
                    latest_bar_close_price(&mut tx, &credential_id, intent.symbol_key.as_str())
                        .await
                        .unwrap_or_else(|| "0".to_string())
                } else {
                    intent.price.clone()
                };
                // The realized PnL truth must be read before the close zeroes
                // the durable position quantity in this same transaction.
                let paper_realized_pnl = paper_fill_realized_pnl(
                    &mut tx,
                    order_role,
                    &prepared,
                    &intent,
                    fill_price.as_str(),
                )
                .await?;
                if !matches!(
                    order_role,
                    OrderRole::ProtectionSl | OrderRole::ProtectionTp
                ) {
                    position_state::mark_paper_filled(
                        &mut tx,
                        &prepared,
                        &intent,
                        fill_price.as_str(),
                        created_at_ms,
                    )
                    .await?;
                    if order_role == OrderRole::Exit && !prepared.orphan_recovery {
                        let position_id = prepared.position_id.as_deref().ok_or_else(|| {
                            Status::failed_precondition(
                                "paper close requires an authoritative position_id",
                            )
                        })?;
                        release_paper_position_notional(
                            &mut tx,
                            &credential_id,
                            position_id,
                            created_at_ms,
                        )
                        .await?;
                    }
                }
                let exchange_trade_id = format!("paper:{}:1", final_order_id);
                let ts_exchange_ms = created_at_ms;
                // A failed fill-row insert must fail the whole order
                // transaction: silently dropping the fill while still emitting
                // a FILLED audit event would understate the account's
                // realized-PnL truth without any unknown marker.
                insert_fill_if_new(
                    &mut tx,
                    self.store.kind(),
                    &credential_id,
                    exchange_trade_id.as_str(),
                    &final_order_id,
                    None,
                    Some(intent.client_order_id_short.as_str()),
                    ctx.exchange.as_str(),
                    ctx.market_kind.as_str(),
                    Some(ctx.symbol_key.as_str()),
                    None,
                    Some(intent.side.as_str()),
                    Some(intent.quantity.as_str()),
                    Some(fill_price.as_str()),
                    None,
                    None,
                    paper_realized_pnl.as_deref(),
                    Some(ts_exchange_ms),
                    Some(created_at_ms),
                    Some(
                        serde_json::json!({
                            "paper": true,
                            "source": "paper_fill",
                            "exchange_trade_id": exchange_trade_id,
                            "ts_exchange_ms": ts_exchange_ms,
                        })
                        .to_string()
                        .into_bytes(),
                    ),
                )
                .await?;
                let payload = serde_json::json!({
                    "runMode": run_mode,
                    "paper": true,
                    "action": intent.action.as_str(),
                    "side": intent.side.as_str(),
                    "orderType": intent.order_type.as_str(),
                    "requestedQty": intent.quantity.as_str(),
                    "requestedPrice": if intent.price.is_empty() { None } else { Some(intent.price.as_str()) },
                    "orderStatus": "FILLED",
                    "order_status": "FILLED",
                    "exchangeTradeId": exchange_trade_id,
                    "exchange_trade_id": exchange_trade_id,
                    "tradeId": exchange_trade_id,
                    "tsExchangeMs": ts_exchange_ms,
                    "ts_exchange_ms": ts_exchange_ms,
                    "fillQty": intent.quantity.as_str(),
                    "fillPrice": fill_price,
                    "realizedPnl": paper_realized_pnl.as_deref(),
                    "intentId": verdict.intent_id.as_str(),
                    "riskVerdictId": verdict.risk_verdict_id.as_str(),
                    "positionId": prepared.position_id.as_deref(),
                    "orderRole": order_role.as_str(),
                });
                emit_event(
                    &mut tx,
                    self.store.kind(),
                    &credential_id,
                    "fill",
                    &ctx,
                    &final_order_id,
                    None,
                    Some(payload),
                    Some(sha256_hex(&format!(
                        "fill|{}|{}",
                        credential_id, exchange_trade_id
                    ))),
                )
                .await?;
            }

            if let Some(context) = exact_close.as_ref() {
                if exact_close_exchange {
                    // 交易所单：本事务只证明「这张单就是那张单」并持久为
                    // dispatched；结局由真值路径终结，fence 保持。
                    crate::position_close::finish_dispatched(
                        &mut tx,
                        context,
                        &final_order_id,
                        &verdict.risk_verdict_id,
                        inserted,
                    )
                    .await?;
                } else {
                    crate::position_close::finish_accepted(
                        &mut tx,
                        context,
                        &final_order_id,
                        &verdict.risk_verdict_id,
                        inserted,
                    )
                    .await?;
                }
            }

            #[cfg(feature = "l4-fault-injection")]
            if exact_close.is_some() {
                crate::fault_injection::checkpoint("b3_before_commit")
                    .map_err(|error| Status::internal(error.to_string()))?;
            }
            tx.commit()
                .await
                .map_err(|e| Status::internal(format!("db commit failed: {e}")))?;
            drop(lifecycle_guard); // Final HTTP attempts acquire the same gate again.
            #[cfg(feature = "l4-fault-injection")]
            if exact_close.is_some() {
                crate::fault_injection::checkpoint("b3_after_commit")
                    .map_err(|error| Status::internal(error.to_string()))?;
            }

            #[cfg(feature = "l4-fault-injection")]
            if inserted && is_exchange_backed && order_role == OrderRole::Entry {
                crate::fault_injection::checkpoint("after_order_insert_before_exchange").map_err(
                    |error| Status::internal(format!("L4 fault checkpoint failed: {error}")),
                )?;
            }

            #[cfg(feature = "l4-fault-injection")]
            if inserted && !is_exchange_backed && order_role == OrderRole::Entry {
                crate::fault_injection::checkpoint("after_paper_fill_commit").map_err(|error| {
                    Status::internal(format!("L4 fault checkpoint failed: {error}"))
                })?;
            }

            let mut live_error: Option<String> = None;
            let mut live_reject: Option<String> = None;
            let mut protection_dispatch_not_started = false;
            // **交易所侧真值是否不明**。只有 `ExchangeAttempt::StartedUnknown`
            // 会置位——即请求确实交给了交易所客户端、结果未知。
            //
            // R6 这里是 `exchange_request_attempted`，在**任何** `place`
            // 错误之后置位，包括根本没发包的本地参数错误（复评变异 N1 存活）。
            // 现在它只有**一个**赋值点，名字与含义一致：不明 → 必须写 `unknown`
            // 让 reconcile 按交易所真值复查，此时挡住写入才是危险的。
            // 其余失败（缺密钥、symbol_key 非法、路由不合法、缺 price）都
            // **没发过包**，写回一律带 CAS，不得盖掉别的路径落下的终态。
            let mut exchange_outcome_unknown = false;
            let mut live_exchange_ack_identity: Option<String> = None;
            // A known remote ACK with unresolved local state is not a transport
            // error: do not route it through live_error's unknown write-back.
            let mut live_ack_unresolved = false;

            #[cfg(test)]
            if inserted
                && !is_exchange_backed
                && self.protection_test_submit_fails
                && protection_order_id.is_some()
            {
                let protection_order_id = protection_order_id
                    .as_deref()
                    .expect("checked protection order id");
                live_error = Some("injected protection submit failure".to_string());
                protection::mark_submit_failed(&self.store, protection_order_id, now_ms())
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "db mark injected protection failure failed: {error}"
                        ))
                    })?;
                if let Some(adapter) = self.protection_test_adapter.as_deref() {
                    protection::flatten_and_pause(
                        &self.store,
                        protection_order_id,
                        &intent.bot_uid,
                        adapter,
                        "injected recovery protection submit failure",
                    )
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "injected recovery protection failure action failed: {error}"
                        ))
                    })?;
                }
            }

            if inserted && is_exchange_backed {
                // Defense in depth: this venue lookup is deliberately repeated
                // at the exchange boundary instead of trusting the earlier
                // stage decision.
                let submission_venue = venue_for_credential(&credential_id).ok().flatten();
                live_reject = demo_submission_reject_reason(
                    &run_mode_norm,
                    submission_venue,
                    order_role,
                )
                .map(|reason| {
                    format!(
                        "{reason}: demo exchange submission requires an explicitly configured testnet venue"
                    )
                });
                // This runs after the order transaction committed, so even a
                // one-connection Store cannot deadlock on the identity read.
                // A local refusal never enters StartedUnknown/submit-failure.
                if live_reject.is_none() && requested_order_role == OrderRole::Entry {
                    live_reject =
                        crate::account_identity::entry_denial(&self.store, &credential_id).await;
                }
                let keys = if live_reject.is_none() {
                    match keys_from_env(&credential_id) {
                        Ok(keys) => keys,
                        Err(error) => {
                            live_error = Some(error);
                            None
                        }
                    }
                } else {
                    None
                };
                if live_reject.is_none() && keys.is_none() {
                    live_error = Some("missing binance api keys in env".to_string());
                }

                // 交易所边界上用的 symbol 与上面放行这条 intent 的那次完整身份
                // 校验**出自同一次解析**（regression）：不可能一处认了
                // venue/market/settle、另一处又退回裸 `base+quote`。
                let symbol = exchange_identity
                    .as_ref()
                    .map(|identity| identity.symbol.clone());
                if live_reject.is_none() && symbol.is_none() {
                    live_error = Some("invalid symbol_key".to_string());
                }

                if let (None, Some(keys), Some(symbol)) = (live_reject.as_ref(), keys, symbol) {
                    if self.owns_background_exchange_tasks {
                        // Ensure reconcile loop is running (startup + periodic).
                        self.reconcilers
                            .ensure_running(
                                &credential_id,
                                self.store.clone(),
                                self.limiter.clone(),
                                keys.clone(),
                            )
                            .await;
                    }

                    // Reuse the same typed side that selected adverse price
                    // rounding; the submit call cannot silently invert it.
                    let side_order = OrderSide::for_action(&intent.action).as_binance();

                    if live_reject.is_none() {
                        #[cfg(test)]
                        let client = match self.submit_client_override.clone() {
                            Some(client) => client,
                            None => BinanceFuturesClient::new_with_clock(
                                keys.clone(),
                                &credential_id,
                                self.store.clone(),
                                self.clock.clone(),
                            )
                            .map_err(|e| {
                                Status::internal(format!("binance client init failed: {e}"))
                            })?,
                        };
                        #[cfg(not(test))]
                        let client = BinanceFuturesClient::new_with_clock(
                            keys.clone(),
                            &credential_id,
                            self.store.clone(),
                            self.clock.clone(),
                        )
                        .map_err(|e| {
                            Status::internal(format!("binance client init failed: {e}"))
                        })?;

                        // A7: this admission is entry-only by construction. Close,
                        // protection and emergency roles never query exposure and
                        // never consume this budget.
                        if requested_order_role == OrderRole::Entry && live_reject.is_none() {
                            let egress_scope = client.egress_scope();
                            match client.admission_open_positions(&self.store).await {
                                Ok(counted_open_positions) => {
                                    // The risk transaction has already inserted this entry's
                                    // pending position. Remove that one row before the pure
                                    // admission function adds the candidate back with `+ 1`.
                                    let open_positions = counted_open_positions.saturating_sub(1);
                                    match protectable_exposure_admission(
                                        open_positions,
                                        protectable_exit_weight_per_position(),
                                        client.protectable_exit_budget(),
                                    ) {
                                        ExposureAdmission::Admit { .. } => {}
                                        ExposureAdmission::Reject {
                                            reserved_after,
                                            budget_1m,
                                        } => {
                                            tracing::warn!(
                                                error_code = "EGRESS_PROTECTABLE_BUDGET_EXHAUSTED",
                                                scope = %egress_scope,
                                                open_positions,
                                                reserved_after,
                                                budget_1m,
                                                "entry rejected because its egress cannot reserve one protected exit per position"
                                            );
                                            live_reject = Some(format!(
                                                "EGRESS_PROTECTABLE_BUDGET_EXHAUSTED: scope={egress_scope} \
                                                 open_positions={open_positions} reserved_after={reserved_after} \
                                                 budget_1m={budget_1m}"
                                            ));
                                        }
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        error_code = "EGRESS_EXPOSURE_UNKNOWN",
                                        scope = %egress_scope,
                                        %error,
                                        "entry rejected because egress exposure could not be counted"
                                    );
                                    live_reject = Some(format!(
                                        "EGRESS_EXPOSURE_UNKNOWN: scope={egress_scope} error={error}"
                                    ));
                                }
                            }
                        }

                        if live_reject.is_none() {
                            // **「会不会发出去」由 `plan_exchange_submission` 一处判定**
                            //。闭包里只剩把 `Err` 转成
                            // `NotStarted` 的一句适配；进入客户端调用之后的任何错误
                            // 一律 `StartedUnknown`——保守方向是"多查一次"，
                            // 不是"漏掉一张活单"。
                            let plan = plan_exchange_submission(
                                order_role,
                                &intent.order_type,
                                &intent.price,
                            );
                            let place = async {
                                let route = match plan {
                                    Ok(route) => route,
                                    Err(error) => return ExchangeAttempt::NotStarted(error),
                                };
                                match route {
                                    ExchangeSubmissionRoute::Market => {
                                        match client
                                            .place_market_order(
                                                &symbol,
                                                side_order,
                                                intent.quantity.as_str(),
                                                effective_reduce_only,
                                                intent.client_order_id_short.as_str(),
                                            )
                                            .await
                                        {
                                            Ok(ack) => ExchangeAttempt::Acked(
                                                ExchangeSubmissionAck::Ordinary(ack),
                                            ),
                                            Err(error) => ExchangeAttempt::StartedUnknown(error),
                                        }
                                    }
                                    ExchangeSubmissionRoute::Limit => {
                                        match client
                                            .place_limit_order(
                                                &symbol,
                                                side_order,
                                                intent.quantity.as_str(),
                                                intent.price.as_str(),
                                                effective_reduce_only,
                                                intent.client_order_id_short.as_str(),
                                            )
                                            .await
                                        {
                                            Ok(ack) => ExchangeAttempt::Acked(
                                                ExchangeSubmissionAck::Ordinary(ack),
                                            ),
                                            Err(error) => ExchangeAttempt::StartedUnknown(error),
                                        }
                                    }
                                    ExchangeSubmissionRoute::Algo(algo_order_type) => {
                                        let close_position = effective_reduce_only
                                            && intent.quantity.trim().is_empty();
                                        let client_algo_id = BinanceClientAlgoId::new(
                                            intent.client_order_id_short.clone(),
                                        );
                                        let permit = match protection_dispatch.as_mut() {
                                            Some(permit) => permit,
                                            None => {
                                                return ExchangeAttempt::ProtectionLocalFailure(
                                                    anyhow::anyhow!(
                                                    "missing managed protection dispatch permit"
                                                ),
                                                )
                                            }
                                        };
                                        match client
                                        .place_algo_conditional(
                                            &self.limiter,
                                            permit,
                                            algo_order_type,
                                            &symbol,
                                            side_order,
                                            intent.price.as_str(),
                                            if close_position {
                                                None
                                            } else {
                                                Some(intent.quantity.as_str())
                                            },
                                            effective_reduce_only,
                                            close_position,
                                            &client_algo_id,
                                        )
                                        .await
                                    {
                                        Ok(ack) => ExchangeAttempt::Acked(
                                            ExchangeSubmissionAck::Algo(ack),
                                        ),
                                        Err(error) => match permit.disposition(&error) {
                                            crate::protection_dispatch::FailureDisposition::NotStarted => ExchangeAttempt::NotStarted(error),
                                            crate::protection_dispatch::FailureDisposition::LocalProtectionFailure => ExchangeAttempt::ProtectionLocalFailure(error),
                                            crate::protection_dispatch::FailureDisposition::MayHaveSent => ExchangeAttempt::StartedUnknown(error),
                                        },
                                    }
                                    }
                                }
                            };

                            // **提交闸门**
                            //
                            // R5 把 CAS 与"要不要提交"写成两段：CAS 影响 0 行时置
                            // `live_reject`，再在这里合成一个 `Err` 跳过提交。两个后果：
                            // ①合成的 `Err` 走进通用错误分支，把行写成 `unknown`——
                            //   kill-switch 刚落下的 `canceled` 先被覆盖成 `rejected`、
                            //   再被覆盖成 `unknown`，撤单真值丢失，终态还被放回
                            //   reconcile 的 inflight 集；
                            // ②`if advanced == 0` 这条**控制流**没有任何行为级用例——
                            //   复评把它改成永不进入，`kill_switch` 26 条全绿（MUT-C 存活）。
                            //
                            // 只有 exact `pending_risk` 才能取得这次提交所有权。
                            // CAS=0 返回独立的 `Blocked`，而不是伪造一个本地拒绝：
                            // 这样类型系统就能阻止它进入 live_reject/live_error finalizer。
                            let advance_outcome = advance_then_submit(
                                self.store.pool(),
                                self.store.kind(),
                                &credential_id,
                                &final_order_id,
                                now_ms(),
                                place,
                            )
                            .await
                            .map_err(|error| {
                                // **DB 故障不得被当成交易真值**（regression）：
                                // 这里 fail-closed 上抛，绝不把 DB 错误伪装成
                                // 一条正常的 ownership-blocked 审计事实。
                                Status::internal(format!(
                                    "db advance to pending_submit failed: {error}"
                                ))
                            })?;
                            let attempt = match advance_outcome {
                                AdvanceThenSubmitOutcome::Advanced(attempt) => attempt,
                                AdvanceThenSubmitOutcome::Blocked { observed_raw } => {
                                    // `place` 没被 poll，因此本轮没有向交易所提交。
                                    // Protection permit 仍须先核对持久仓位：closed+zero
                                    // 是安全的 NotStarted；active/不可判定则必须执行
                                    // mark-failed + flatten/pause，不能因为这里直接
                                    // `continue` 而绕过保护兜底。
                                    let mut blocked_ack_status = "error";
                                    let mut blocked_ack_message = format!(
                                    "{SUBMIT_OWNERSHIP_BLOCKED_CODE}: expected exact pending_risk, \
                                     observed raw status {:?}; exchange submit not polled; \
                                     authoritative_idempotency_key={authority_idem_key}",
                                    observed_raw
                                );
                                    let mut blocked_permit_outcome = "not_dispatched";
                                    let mut protection_not_started_detail: Option<String> = None;
                                    let mut protection_fallback_required = false;
                                    let mut protection_safety_authority = None;
                                    if let Some(permit) = protection_dispatch
                                        .as_mut()
                                        .filter(|permit| !permit.finished())
                                    {
                                        let (disposition, error) = permit.unpolled_denial().await;
                                        match disposition {
                                        crate::protection_dispatch::FailureDisposition::NotStarted => {
                                            let detail = crate::protection_dispatch::safe_error_detail(&error);
                                            blocked_ack_status = "rejected";
                                            blocked_ack_message = format!(
                                                "protection submission not started: {detail}; \
                                                 authoritative_idempotency_key={authority_idem_key}"
                                            );
                                            protection_not_started_detail = Some(detail);
                                        }
                                        crate::protection_dispatch::FailureDisposition::LocalProtectionFailure => {
                                            protection_fallback_required = true;
                                        }
                                        crate::protection_dispatch::FailureDisposition::MayHaveSent => {
                                            blocked_permit_outcome = "may_have_sent";
                                            protection_fallback_required = true;
                                        }
                                    }
                                    }

                                    // An active protection failure must have a durable recovery
                                    // authority before the permit can be declared closed.  Otherwise
                                    // a crash after the closure commit would strand an unprotected
                                    // position with no retry target.
                                    if protection_fallback_required {
                                        let protection_order_id = protection_order_id
                                            .as_deref()
                                            .expect("a protection permit has a protection order");
                                        let authority =
                                        protection::latch_flatten_after_protection_failure(
                                            &self.store,
                                            &intent.bot_uid,
                                            "protection submit ownership blocked while position remained active",
                                        )
                                        .await
                                        .map_err(|error| {
                                            Status::internal(format!(
                                                "latch ownership-blocked protection fallback failed: {error}"
                                            ))
                                        })?;
                                        #[cfg(feature = "l4-fault-injection")]
                                    crate::fault_injection::checkpoint(
                                        "after_ownership_blocked_flatten_latch_before_mark_submit_failed",
                                    )
                                    .map_err(|error| {
                                        Status::internal(format!(
                                            "ownership-blocked protection pre-mark checkpoint failed: {error}"
                                        ))
                                    })?;
                                        protection::mark_submit_failed(
                                            &self.store,
                                            protection_order_id,
                                            now_ms(),
                                        )
                                        .await
                                        .map_err(
                                            |error| {
                                                Status::internal(format!(
                                            "mark ownership-blocked protection failed: {error}"
                                        ))
                                            },
                                        )?;
                                        protection_safety_authority = Some(authority);
                                        #[cfg(feature = "l4-fault-injection")]
                                    crate::fault_injection::checkpoint(
                                        "after_ownership_blocked_flatten_latch_before_permit_close",
                                    )
                                    .map_err(|error| {
                                        Status::internal(format!(
                                            "ownership-blocked protection checkpoint failed: {error}"
                                        ))
                                    })?;
                                    }

                                    let (
                                        safety_authority_kind,
                                        safety_authority_scope,
                                        safety_authority_raw_level,
                                    ) = match protection_safety_authority.as_ref() {
                                        Some(
                                            protection::ProtectionFailureLatch::CanonicalFlatten,
                                        ) => (
                                            Some("canonical_flatten"),
                                            Some(format!("bot:{}", intent.bot_uid)),
                                            None,
                                        ),
                                        Some(
                                            protection::ProtectionFailureLatch::PreservedInvalid(
                                                invalid,
                                            ),
                                        ) => (
                                            Some("preserved_invalid_conservative_flatten"),
                                            Some(invalid.scope_key.clone()),
                                            Some(invalid.raw_level.clone()),
                                        ),
                                        None => (None, None, None),
                                    };
                                    tracing::error!(
                                        credential_id = %credential_id,
                                        engine_order_id = %final_order_id,
                                        observed_status_raw = ?observed_raw,
                                        error_code = SUBMIT_OWNERSHIP_BLOCKED_CODE,
                                        "exact pending_risk submit ownership was not acquired; exchange submit was not polled"
                                    );
                                    let mut blocked_tx =
                                        self.store.pool().begin().await.map_err(|e| {
                                            Status::internal(format!(
                                                "db begin submit-ownership audit failed: {e}"
                                            ))
                                        })?;
                                    let closed_protection_dispatch = if let Some(permit) =
                                        protection_dispatch
                                            .as_ref()
                                            .filter(|permit| !permit.finished())
                                    {
                                        permit
                                        .finish_in_tx(&mut blocked_tx, blocked_permit_outcome)
                                        .await
                                        .map_err(|error| {
                                            Status::internal(format!(
                                                "db close blocked protection dispatch failed: {error}"
                                            ))
                                        })?;
                                        true
                                    } else {
                                        false
                                    };
                                    emit_event(
                                    &mut blocked_tx,
                                    self.store.kind(),
                                    &credential_id,
                                    "order_submit_ownership_blocked",
                                    &ctx,
                                    &final_order_id,
                                    None,
                                    Some(serde_json::json!({
                                        "errorCode": SUBMIT_OWNERSHIP_BLOCKED_CODE,
                                        "expectedStatus": "pending_risk",
                                        "observedStatusRaw": observed_raw.clone(),
                                        "exchangeSubmitPolled": false,
                                        "orderRole": order_role.as_str(),
                                        "protectionFallbackRequired": protection_fallback_required,
                                        "safetyAuthorityKind": safety_authority_kind,
                                        "safetyAuthorityScopeKey": safety_authority_scope,
                                        "safetyAuthorityRawLevel": safety_authority_raw_level,
                                    })),
                                    Some(sha256_hex(&format!(
                                        "order_submit_ownership_blocked|{}|{}|{}",
                                        credential_id,
                                        final_order_id,
                                        observed_raw.as_deref().unwrap_or("<missing>")
                                    ))),
                                )
                                .await?;
                                    if let Some(detail) = protection_not_started_detail.as_deref() {
                                        emit_event(
                                        &mut blocked_tx,
                                        self.store.kind(),
                                        &credential_id,
                                        "reject",
                                        &ctx,
                                        &final_order_id,
                                        None,
                                        Some(serde_json::json!({
                                            "runMode": run_mode,
                                            "paper": false,
                                            "rejectReasonCode": "PROTECTION_DISPATCH_NOT_STARTED",
                                            "message": detail,
                                            "orderRole": order_role.as_str(),
                                        })),
                                        Some(sha256_hex(&format!(
                                            "protection_dispatch_not_started|{}|{}",
                                            credential_id, final_order_id
                                        ))),
                                    )
                                    .await?;
                                    }
                                    blocked_tx.commit().await.map_err(|e| {
                                        Status::internal(format!(
                                            "db commit submit-ownership audit failed: {e}"
                                        ))
                                    })?;
                                    if closed_protection_dispatch {
                                        protection_dispatch
                                            .as_mut()
                                            .expect("the committed blocked permit is still present")
                                            .acknowledge_committed_finish();
                                    }

                                    if protection_fallback_required {
                                        match protection::BinanceProtectionAdapter::new(
                                            self.store.clone(),
                                            keys.clone(),
                                            &credential_id,
                                            &intent.bot_uid,
                                            self.limiter.clone(),
                                        ) {
                                            Ok(adapter) => {
                                                if let Err(action_error) =
                                                    protection::dispatch_latched_flatten(&adapter)
                                                        .await
                                                {
                                                    tracing::error!(
                                                        protection_order_id = %protection_order_id
                                                            .as_deref()
                                                            .expect("fallback protection order remains present"),
                                                        error = %action_error,
                                                        "ownership-blocked immediate flatten failed; durable latch remains retryable"
                                                    );
                                                }
                                            }
                                            Err(action_error) => {
                                                tracing::error!(
                                                    protection_order_id = %protection_order_id
                                                        .as_deref()
                                                        .expect("fallback protection order remains present"),
                                                    error = %action_error,
                                                    "ownership-blocked flatten adapter unavailable; durable latch remains retryable"
                                                );
                                            }
                                        }
                                    }

                                    acks.push(IntentAck {
                                        idempotency_key: intent.idempotency_key,
                                        engine_order_id: final_order_id,
                                        status: blocked_ack_status.to_string(),
                                        message: blocked_ack_message,
                                    });
                                    continue;
                                }
                            };
                            // 两个写回标志**只能**由映射产生（见 `write_back_intent`）。
                            let intent_writeback = write_back_intent(&attempt);
                            exchange_outcome_unknown = intent_writeback.outcome_unknown;
                            match attempt {
                                ExchangeAttempt::Acked(ExchangeSubmissionAck::Ordinary(ack)) => {
                                    if let Err(error) =
                                        self.clear_credential_errors(&credential_id).await
                                    {
                                        // 交易所 ACK 已经发生；健康表清零失败不能让我们
                                        // 在保存 ACK 前提前返回，否则会制造一张本地未知的
                                        // 交易所活单。错误必须显式可见，但交易真值优先落库。
                                        tracing::error!(
                                            credential_id,
                                            %error,
                                            "credential error window clear failed after exchange ack"
                                        );
                                    }
                                    let exchange_order_id = ack.order_id.to_string();
                                    live_exchange_ack_identity = Some(exchange_order_id.clone());

                                    let updated_at_ms = now_ms();
                                    let mut tx2 = self.store.pool().begin().await.map_err(|e| {
                                        Status::internal(format!("db begin failed: {e}"))
                                    })?;

                                    // 宽限期竞态的另一侧。本次提交可能在
                                    // limiter 里滞留超过 60s，期间 reconcile 会按
                                    // 「无交易所身份的崩溃残留」把这单裁决为 `failed`
                                    // 并由清扫释放了 closing。现在交易所确认收单了。
                                    //
                                    // **这里刻意不按评审给的「加守卫挡住写入」实现**：
                                    // `failed` 不在 reconcile 的 inflight 重查状态集
                                    // （`pending_risk|pending_submit|submitted|acked|
                                    // partial_filled|unknown`）内，挡住写入会让这行永远
                                    // 停在 `failed`，而交易所侧挂着一张**活的**平仓单再
                                    // 也不会被任何路径重新追踪——那比留下审计矛盾更糟。
                                    // 因此：真值照写（保住对活单的追踪），但把这次
                                    // 「复活」显式记一笔，让矛盾可审计而不是静默。
                                    // 仓位侧的收敛由 reduce-only 与后续 truth 应用兜底
                                    // （单子成交 → qty=0 → mark_position_closed）。
                                    let acked_without_conflict = sqlx::query(
                                    r#"
                                    UPDATE engine_orders
                                    SET exchange_order_id = ?, status = 'acked', updated_at_ms = ?
                                    WHERE credential_id = ? AND engine_order_id = ?
                                      AND status NOT IN ('failed', 'canceled', 'expired', 'rejected', 'filled')
                                    "#,
                                )
                                .bind(&exchange_order_id)
                                .bind(updated_at_ms)
                                .bind(&credential_id)
                                .bind(&final_order_id)
                                .execute(&mut *tx2)
                                .await
                                .map_err(|e| Status::internal(format!("db update failed: {e}")))?
                                .rows_affected()
                                    > 0;

                                    if !acked_without_conflict {
                                        sqlx::query(
                                        r#"
                                        UPDATE engine_orders
                                        SET exchange_order_id = ?, status = 'acked', updated_at_ms = ?
                                        WHERE credential_id = ? AND engine_order_id = ?
                                        "#,
                                    )
                                    .bind(&exchange_order_id)
                                    .bind(updated_at_ms)
                                    .bind(&credential_id)
                                    .bind(&final_order_id)
                                    .execute(&mut *tx2)
                                    .await
                                    .map_err(|e| {
                                        Status::internal(format!("db update failed: {e}"))
                                    })?;
                                        tracing::error!(
                                            credential_id = %credential_id,
                                            engine_order_id = %final_order_id,
                                            %exchange_order_id,
                                            "live ack revived an order already adjudicated terminal: \
                                             closing may have been released while this order is live"
                                        );
                                        emit_event(
                                            &mut tx2,
                                            self.store.kind(),
                                            &credential_id,
                                            "order_ack_revived_terminal",
                                            &ctx,
                                            &final_order_id,
                                            Some(exchange_order_id.as_str()),
                                            Some(serde_json::json!({
                                                "reason": "grace_window_race",
                                                "nextStatus": "acked",
                                            })),
                                            None,
                                        )
                                        .await?;
                                    }

                                    let mut payload = verdict_event_payload(
                                        &intent,
                                        &run_mode,
                                        &verdict,
                                        order_role,
                                        prepared.position_id.as_deref(),
                                    );
                                    if let Some(object) = payload.as_object_mut() {
                                        object.insert(
                                            "paper".to_string(),
                                            serde_json::Value::Bool(false),
                                        );
                                        object.insert(
                                            "binance".to_string(),
                                            serde_json::to_value(&ack)
                                                .unwrap_or(serde_json::Value::Null),
                                        );
                                    }

                                    emit_event(
                                        &mut tx2,
                                        self.store.kind(),
                                        &credential_id,
                                        "order_ack",
                                        &ctx,
                                        &final_order_id,
                                        Some(exchange_order_id.as_str()),
                                        Some(payload),
                                        None,
                                    )
                                    .await?;

                                    tx2.commit().await.map_err(|e| {
                                        Status::internal(format!("db commit failed: {e}"))
                                    })?;
                                }
                                ExchangeAttempt::Acked(ExchangeSubmissionAck::Algo(ack)) => {
                                    if let Err(error) =
                                        self.clear_credential_errors(&credential_id).await
                                    {
                                        tracing::error!(
                                            credential_id,
                                            %error,
                                            "credential error window clear failed after exchange ack"
                                        );
                                    }
                                    let protection_order_id =
                                        protection_order_id.as_deref().ok_or_else(|| {
                                            Status::internal(
                                                "algo protection submission has no protection row",
                                            )
                                        })?;
                                    let requested_client_algo_id = BinanceClientAlgoId::new(
                                        intent.client_order_id_short.clone(),
                                    );
                                    let mut tx2 =
                                        self.store.pool().begin().await.map_err(|error| {
                                            Status::internal(format!(
                                                "db begin algo ack failed: {error}"
                                            ))
                                        })?;
                                    protection_dispatch
                                        .as_ref()
                                        .ok_or_else(|| {
                                            Status::internal("algo ack has no managed permit")
                                        })?
                                        .lock_for_ack(&mut tx2)
                                        .await
                                        .map_err(|error| {
                                            Status::internal(format!(
                                                "db lock acknowledged dispatch failed: {error}"
                                            ))
                                        })?;
                                    let ack_outcome = protection::mark_algo_submitted_in_tx(
                                        &mut tx2,
                                        protection_order_id,
                                        &requested_client_algo_id,
                                        &ack,
                                        now_ms(),
                                    )
                                    .await
                                    .map_err(|error| {
                                        Status::internal(format!(
                                            "db mark algo protection submitted failed: {error}"
                                        ))
                                    })?;
                                    live_exchange_ack_identity =
                                        Some(format!("algo:{}", ack_outcome.algo_id));
                                    live_ack_unresolved =
                                        ack_outcome.unresolved_local_state.is_some();
                                    let live_exchange_order_id = ack
                                        .actual_order_id
                                        .as_ref()
                                        .and_then(|value| value.normalized());

                                    let mut payload = verdict_event_payload(
                                        &intent,
                                        &run_mode,
                                        &verdict,
                                        order_role,
                                        prepared.position_id.as_deref(),
                                    );
                                    if let Some(object) = payload.as_object_mut() {
                                        object.insert(
                                            "paper".to_string(),
                                            serde_json::Value::Bool(false),
                                        );
                                        object.insert(
                                            "binanceAlgo".to_string(),
                                            serde_json::to_value(&ack)
                                                .unwrap_or(serde_json::Value::Null),
                                        );
                                        if let Some(raw) = &ack_outcome.unresolved_local_state {
                                            object.insert(
                                                "localStateOutcome".into(),
                                                serde_json::json!("unresolved"),
                                            );
                                            object.insert(
                                                "localStateReason".into(),
                                                serde_json::json!(
                                                    protection::ACK_LOCAL_STATE_UNRESOLVED
                                                ),
                                            );
                                            object.insert(
                                                "localStateRaw".into(),
                                                serde_json::json!(raw),
                                            );
                                        }
                                    }
                                    let permit = protection_dispatch.as_mut().ok_or_else(|| {
                                        Status::internal("algo ack has no managed permit")
                                    })?;
                                    permit
                                        .finish_in_tx(&mut tx2, "acknowledged")
                                        .await
                                        .map_err(|error| {
                                            Status::internal(format!(
                                                "db close acknowledged dispatch failed: {error}"
                                            ))
                                        })?;
                                    emit_event(
                                        &mut tx2,
                                        self.store.kind(),
                                        &credential_id,
                                        "order_ack",
                                        &ctx,
                                        &final_order_id,
                                        live_exchange_order_id.as_deref(),
                                        Some(payload),
                                        None,
                                    )
                                    .await?;
                                    tx2.commit().await.map_err(|error| {
                                        Status::internal(format!(
                                            "db commit algo ack failed: {error}"
                                        ))
                                    })?;
                                    permit.acknowledge_committed_finish();

                                    let timeout_ms = std::env::var("PROTECTION_CONFIRM_TIMEOUT_MS")
                                        .ok()
                                        .and_then(|value| value.parse::<u64>().ok())
                                        .unwrap_or(protection::DEFAULT_CONFIRM_TIMEOUT_MS)
                                        .clamp(1, 60_000);
                                    let adapter = protection::BinanceProtectionAdapter::new(
                                        self.store.clone(),
                                        keys.clone(),
                                        &credential_id,
                                        &intent.bot_uid,
                                        self.limiter.clone(),
                                    )
                                    .map_err(|error| {
                                        Status::internal(format!(
                                            "build protection adapter failed: {error}"
                                        ))
                                    })?;
                                    // Keep the existing test-only external filter source
                                    // for the independent confirmation/flatten adapter too.
                                    #[cfg(test)]
                                    let adapter = adapter
                                        .with_test_filter_cache(self.exchange_filters.clone());
                                    protection::spawn_confirmation_task(
                                        self.store.clone(),
                                        protection_order_id.to_string(),
                                        timeout_ms,
                                        Arc::new(adapter),
                                    );
                                }
                                // **请求从未离开本进程**：交易所侧不可能有这张单。
                                // 这不是「结果不明」，是一次**本地拒绝**——写回带 CAS，
                                // 也不记 credential 错误（那会让本地参数问题去拉
                                // 断路器，把整个凭据的后续下单一起拖下水）。
                                ExchangeAttempt::NotStarted(e) => {
                                    tracing::warn!(
                                        credential_id = %credential_id,
                                        engine_order_id = %final_order_id,
                                        error = %e,
                                        "order was never sent to the exchange; rejecting locally"
                                    );
                                    protection_dispatch_not_started = protection_dispatch.is_some();
                                    live_reject = Some(if protection_dispatch_not_started {
                                        format!(
                                            "protection submission not started: {}",
                                            crate::protection_dispatch::safe_error_detail(&e)
                                        )
                                    } else {
                                        format!("submission not started: {e}")
                                    });
                                }
                                // **请求已交给交易所客户端但结果不明**：交易所侧可能
                                // 挂着一张活单，必须转 `unknown` 让 reconcile 复查。
                                ExchangeAttempt::StartedUnknown(e)
                                | ExchangeAttempt::ProtectionLocalFailure(e) => {
                                    let error_detail = if protection_order_id.is_some() {
                                        crate::protection_dispatch::safe_error_detail(&e)
                                    } else {
                                        e.to_string()
                                    };
                                    live_error =
                                        Some(format!("binance submit failed: {error_detail}"));
                                    if exchange_outcome_unknown && protection_order_id.is_some() {
                                        // Preserve uncertainty before any protective action can wait.
                                        if let Err(error) = finalize_status_if_in_flight(
                                            self.store.pool(),
                                            self.store.kind(),
                                            &credential_id,
                                            &final_order_id,
                                            "unknown",
                                            now_ms(),
                                        )
                                        .await
                                        {
                                            tracing::error!(
                                                %error,
                                                "persist unknown protection dispatch outcome failed"
                                            );
                                            live_error = Some(format!(
                                                "binance submit failed: {e}; unknown persistence failed: {error}"
                                            ));
                                        }
                                    }
                                    if let Some(protection_order_id) =
                                        protection_order_id.as_deref()
                                    {
                                        let _ = protection::mark_submit_failed(
                                            &self.store,
                                            protection_order_id,
                                            now_ms(),
                                        )
                                        .await;
                                        if let Ok(adapter) =
                                            protection::BinanceProtectionAdapter::new(
                                                self.store.clone(),
                                                keys.clone(),
                                                &credential_id,
                                                &intent.bot_uid,
                                                self.limiter.clone(),
                                            )
                                        {
                                            if let Err(action_error) =
                                                protection::flatten_and_pause(
                                                    &self.store,
                                                    protection_order_id,
                                                    &intent.bot_uid,
                                                    &adapter,
                                                    "protection order submission failed",
                                                )
                                                .await
                                            {
                                                tracing::error!(
                                                    %protection_order_id,
                                                    error=%action_error,
                                                    "protection submit failure action incomplete"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(permit) = protection_dispatch.as_mut() {
                    if !permit.finished() {
                        permit
                            .finish(if exchange_outcome_unknown {
                                "may_have_sent"
                            } else {
                                "not_dispatched"
                            })
                            .await
                            .map_err(|error| {
                                Status::internal(format!(
                                    "close protection dispatch failed: {error}"
                                ))
                            })?;
                    }
                }

                if let Some(reason) = live_reject.as_deref() {
                    let updated_at_ms = now_ms();
                    let mut tx2 = self
                        .store
                        .pool()
                        .begin()
                        .await
                        .map_err(|e| Status::internal(format!("db begin failed: {e}")))?;
                    let entry_cleanup_claim = if order_role == OrderRole::Entry {
                        let position_id = prepared.position_id.as_deref().ok_or_else(|| {
                            Status::failed_precondition(
                                "rejected Entry has no durable position binding",
                            )
                        })?;
                        risk::lock_notional_reservation(
                            &mut tx2,
                            self.store.kind(),
                            &credential_id,
                        )
                        .await?;
                        Some(
                            position_state::lock_entry_claim(
                                &mut tx2,
                                self.store.kind(),
                                position_id,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    // ****：带 CAS 写回。
                    // 影响 0 行 = 这行已经被 kill-switch 或别的路径推进到终态，
                    // 本次 reject **不得**把它盖掉（`canceled` → `rejected` 就是
                    // 撤单真值丢失，且把终态放回 reconcile 的处理面）。
                    let rejected_rows = finalize_pre_submit_status(
                        &mut *tx2,
                        self.store.kind(),
                        &credential_id,
                        &final_order_id,
                        "rejected",
                        updated_at_ms,
                    )
                    .await
                    .map_err(|e| Status::internal(format!("db update demo reject failed: {e}")))?;
                    if rejected_rows == 0 {
                        tracing::warn!(
                            credential_id = %credential_id,
                            engine_order_id = %final_order_id,
                            "order already left the pre-submit state; keeping its current \
                             status instead of overwriting it with 'rejected'"
                        );
                    }
                    // Cleanup belongs to the same exact terminal-write ownership.
                    // CAS=0 means another path owns the order truth, so this
                    // request must neither release its reservation nor retire
                    // its position claim.
                    if order_role == OrderRole::Entry && rejected_rows == 1 {
                        if let Some(sizing) = verdict.sizing.as_ref() {
                            risk::release_notional(
                                &mut tx2,
                                &credential_id,
                                sizing.final_notional_usdt(),
                                updated_at_ms,
                            )
                            .await?;
                        }
                        if let Some(position_id) = prepared.position_id.as_deref() {
                            sqlx::query(
                                "DELETE FROM position_protection_plans WHERE position_id=? \
                                 AND credential_id=? AND bot_uid=? AND symbol_key=?",
                            )
                            .bind(position_id)
                            .bind(&credential_id)
                            .bind(&intent.bot_uid)
                            .bind(&intent.symbol_key)
                            .execute(&mut *tx2)
                            .await
                            .map_err(|e| {
                                Status::internal(format!(
                                    "db remove rejected demo protection plan failed: {e}"
                                ))
                            })?;
                            let direction = match intent.action.as_str() {
                                "open_long" => "long",
                                "open_short" => "short",
                                _ => {
                                    return Err(Status::failed_precondition(
                                        "rejected Entry has a noncanonical open action",
                                    ));
                                }
                            };
                            position_state::retire_locked_zero_pending_entry_claim(
                                &mut tx2,
                                self.store.kind(),
                                position_state::PendingEntryClaimRetirement {
                                    claim: entry_cleanup_claim.as_ref().ok_or_else(|| {
                                        Status::internal("rejected Entry cleanup lock is missing")
                                    })?,
                                    credential_id: &credential_id,
                                    bot_uid: &intent.bot_uid,
                                    symbol_key: &intent.symbol_key,
                                    direction,
                                    idempotency_key: &authority_idem_key,
                                    ts_ms: updated_at_ms,
                                },
                            )
                            .await?;
                        }
                    }
                    let reject_reason_code =
                        if reason.contains("EGRESS_PROTECTABLE_BUDGET_EXHAUSTED") {
                            "EGRESS_PROTECTABLE_BUDGET_EXHAUSTED"
                        } else if reason.contains("EGRESS_EXPOSURE_UNKNOWN") {
                            "EGRESS_EXPOSURE_UNKNOWN"
                        } else if reason.starts_with(crate::account_identity::CONFLICT_CODE)
                            || reason.starts_with(crate::account_identity::UNAVAILABLE_CODE)
                        {
                            crate::account_identity::denial_code(reason)
                        } else if protection_dispatch_not_started {
                            "PROTECTION_DISPATCH_NOT_STARTED"
                        } else {
                            "DEMO_REQUIRES_TESTNET_VENUE"
                        };
                    emit_event(
                        &mut tx2,
                        self.store.kind(),
                        &credential_id,
                        "reject",
                        &ctx,
                        &final_order_id,
                        None,
                        Some(serde_json::json!({
                            "runMode": run_mode,
                            "paper": false,
                            "rejectReasonCode": reject_reason_code,
                            "message": reason,
                            "orderRole": order_role.as_str(),
                        })),
                        Some(sha256_hex(&format!(
                            "demo_venue_reject|{}|{}",
                            credential_id, final_order_id
                        ))),
                    )
                    .await?;
                    tx2.commit()
                        .await
                        .map_err(|e| Status::internal(format!("db commit failed: {e}")))?;
                    if let Some(protection_order_id) = protection_order_id.as_deref() {
                        protection::mark_submit_failed(
                            &self.store,
                            protection_order_id,
                            updated_at_ms,
                        )
                        .await
                        .map_err(|error| {
                            Status::internal(format!(
                                "db mark venue-rejected protection failed: {error}"
                            ))
                        })?;
                        if let Ok(Some(keys)) = keys_from_env(&credential_id) {
                            if let Ok(adapter) = protection::BinanceProtectionAdapter::new(
                                self.store.clone(),
                                keys,
                                &credential_id,
                                &intent.bot_uid,
                                self.limiter.clone(),
                            ) {
                                if let Err(error) = protection::flatten_and_pause(
                                    &self.store,
                                    protection_order_id,
                                    &intent.bot_uid,
                                    &adapter,
                                    "demo protection rejected outside testnet venue",
                                )
                                .await
                                {
                                    tracing::error!(
                                        %protection_order_id,
                                        %error,
                                        "venue-rejected protection flatten action incomplete"
                                    );
                                }
                            }
                        }
                    }
                }

                if let Some(err) = live_error.as_deref() {
                    let updated_at_ms = now_ms();
                    // ****：这条写回此前无 CAS，
                    // 于是 `canceled` 会被盖成 `unknown`——撤单真值丢失，而且
                    // `unknown` 就在 reconcile 的 inflight 重查集里，等于把终态
                    // 放回处理面。
                    //
                    // 但这里**不能一律加"提交前"那道 CAS**：交易所侧真值不明时，
                    // 那边可能已经有一张活单，`unknown` 正是让 reconcile 去按真值
                    // 收敛的那个状态，用提交前状态集去挡会让活单失去追踪。
                    // 因此按**真值明不明**分流——不是按"有没有发过请求"
                    // （R6 那个布尔连自己的名字都对不上）。
                    //
                    // ****：
                    // 但"不能用提交前状态集"≠"可以无谓词"。R7 这条是**无状态谓词**
                    // 的 UPDATE，于是一次迟到的 HTTP timeout 能把 WS/reconcile
                    // 已经写下的 `filled` / `acked` 覆盖成 `unknown`——
                    // 那是**用更弱的真值盖掉更强的真值**。三态换了名字，CAS 没加。
                    //
                    // 正确的谓词是「这一行还停在**等提交结果**的状态」：
                    // 只有 `pending_submit` 才轮得到我们说"不知道"。
                    // 任何已经带上交易所真值的状态（acked / partial_filled /
                    // filled / canceled / expired / rejected / failed）都比
                    // 我们这条本地的"不知道"强，不得被它降级。
                    //
                    // 附带一条**刻意保守**的取舍：交易所明确 4xx 拒绝目前也归
                    // `StartedUnknown`。把它单列成"确定拒绝"需要从 anyhow 错误串
                    // 里认业务码，那种判断一旦认错方向就是把"可能有活单"记成
                    // "确定没单"——比多查一次危险得多。记 `unknown` 会让 reconcile
                    // 去交易所问一次并写回真实状态，**自我收敛**，代价只是一次查询。
                    //
                    // **DB 错误不得
                    // 被伪装成「0 行 / 已有更强真值」。**
                    //
                    // 这两个写回此前是 `.unwrap_or(0)` / `let _ =`：一次 DB 故障
                    // 与「这一行已经带着更强的交易所真值」**返回同一个结果**，
                    // 随后照常打印那条语义不真的 exchange-status 告警、
                    // 记 credential error、并把 exit 的 closing 释放掉。
                    // 于是**一次数据库故障就能让一张未知的活单失去追踪**
                    //（它没被写成 `unknown`，reconcile 的 inflight 集里没有它），
                    // 同时 exit 状态被释放——正是 §3.12 禁止的
                    // 「缺失数据被转换为正常」。
                    //
                    // 仓内先例是 regression（`advance_then_submit` 的
                    // `.unwrap_or(0)`）：`Result` 原样上抛，调用方 fail-closed。
                    // 这里照做——`?` 之后的 credential error / exit release
                    // 两段代码**都不会执行**。
                    // **exit 的释放闸门。**
                    //
                    // 只有一种情形可以在这里就把 closing 释放掉：**这张单确定
                    // 从未离开本进程**，且**这一行的状态确实是我们这次推进的**。
                    // 其余一律留给 reconcile 按交易所真值裁决
                    // （`release_closing_if_exit_dead`：订单被裁成
                    // canceled/expired/rejected/failed 后才释放）。
                    // 详见下面 exit release 那一段的注释。
                    let mut exit_release_proven_safe = false;
                    if exchange_outcome_unknown {
                        let unknown_rows = finalize_status_if_in_flight(
                            self.store.pool(),
                            self.store.kind(),
                            &credential_id,
                            &final_order_id,
                            "unknown",
                            updated_at_ms,
                        )
                        .await
                        .map_err(|error| {
                            Status::internal(format!(
                                "db finalize unknown submit outcome failed: {error}"
                            ))
                        })?;
                        if unknown_rows == 0 {
                            tracing::warn!(
                                credential_id = %credential_id,
                                engine_order_id = %final_order_id,
                                "order already carries an exchange-sourced status; \
                                 keeping it instead of downgrading to 'unknown'"
                            );
                        }
                    } else {
                        // 从未联到交易所（缺密钥、symbol_key 非法等）：这一行
                        // 不可能有交易所侧真值，写回带 CAS，撤单真值不被覆盖。
                        // 同一条纪律（regression）：DB 错误上抛，不得被吞成
                        // "0 行"再继续走 credential error / exit release。
                        let pre_submit_rows = finalize_pre_submit_status(
                            self.store.pool(),
                            self.store.kind(),
                            &credential_id,
                            &final_order_id,
                            "unknown",
                            updated_at_ms,
                        )
                        .await
                        .map_err(|error| {
                            Status::internal(format!(
                                "db finalize pre-submit unknown status failed: {error}"
                            ))
                        })?;
                        // **CAS=0 同样不得释放**（段级终轮点名）：影响 0 行意味着
                        // 这一行已经被别的路径推进了，我们并不知道它现在带着什么
                        // 真值——可能是 kill-switch 的 `canceled`，也可能是 WS
                        // 刚写下的 `acked`（那就有活单）。「不知道」不构成
                        // 「交易所没有活单」的证明。
                        exit_release_proven_safe = pre_submit_rows > 0;
                        if pre_submit_rows == 0 {
                            tracing::warn!(
                                credential_id = %credential_id,
                                engine_order_id = %final_order_id,
                                "order left the pre-submit state before the local failure \
                                 write-back; keeping the closing position for reconcile"
                            );
                        }
                    }

                    let credential_degraded = match self
                        .record_credential_error(&credential_id, err)
                        .await
                    {
                        Ok(()) => match self.is_credential_degraded(&credential_id).await {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::error!(
                                    credential_id,
                                    %error,
                                    "credential health read failed; reporting fail-closed degraded"
                                );
                                true
                            }
                        },
                        Err(error) => {
                            // 失败计数写不进去时，绝不能把这次交易所错误呈现成
                            // credential 健康。当前响应/审计按 degraded=true 完成；
                            // 数据库错误本身另行显式记录，不再折叠成一次成功写入。
                            tracing::error!(
                                credential_id,
                                %error,
                                "credential error record failed; reporting fail-closed degraded"
                            );
                            true
                        }
                    };

                    // AUD-P2-H6：提交失败后订单转 unknown，但 position_state 此前
                    // **仍停在 closing 且带着陈旧的 exit 幂等键**——重试恒 duplicate、
                    // 同方向新开仓又被 DUPLICATE_ACTIVE_ENTRY 拒绝，仓位既平不掉也
                    // 开不了。这里把它释放回 pre_exit_state 并自增 exit_epoch，
                    // 使下一次 close 得到一把新键、能真正重新提交。
                    //
                    // **对账证明无活单之前
                    // 不得释放 closing/epoch。**
                    //
                    // R9 这段对三种情形一视同仁地释放，其中**最危险的那种**正是
                    // 上面注释自己写明的那种：`StartedUnknown` = 请求已交给交易所
                    // 客户端、结果不明，**交易所侧可能挂着一张活单**。释放之后
                    // exit_epoch+1、pending_idem_key 被清空，下一次 close 拿到一把
                    // **新的幂等键**——它绕开 `engine_orders` 的唯一键，于是同一个
                    // 仓位被平两次（评审实跑：从 `execute_intents` 出发，
                    // StartedUnknown 后 exit 被释放）。§3.1 的"重复下单"。
                    //
                    // 三种情形分流（每一种在 R10R-REPORT 里都有对应用例）：
                    // ① **DB 错误**：上面的 `?` 已经上抛，根本走不到这里；
                    // ② **真值未知**（`StartedUnknown`，含 CAS=0 的那一支）：
                    //    不释放。订单已被写成 `unknown`，reconcile 会向交易所求证，
                    //    拿到 `-2013`（查无此单）才裁 `failed`，随后由
                    //    `release_closing_if_exit_dead` 在**同一事务**里释放
                    //    closing——那条路径才有"无活单"的证明；
                    // ③ **从未离开本进程**且**这一行确实由我们这次推进**
                    //    （CAS 影响 1 行）：交易所不可能有这张单，立即释放，
                    //    仓位不必等一整个 reconcile 周期才能重平。
                    if exit_release_proven_safe
                        && order_role == OrderRole::Exit
                        && !prepared.orphan_recovery
                    {
                        if let Some(position_id) = prepared.position_id.as_deref() {
                            let mut tx_release =
                                self.store.pool().begin().await.map_err(|e| {
                                    Status::internal(format!("db begin failed: {e}"))
                                })?;
                            let released = position_state::release_closing_after_failed_exit(
                                &mut tx_release,
                                position_id,
                                updated_at_ms,
                            )
                            .await?;
                            tx_release
                                .commit()
                                .await
                                .map_err(|e| Status::internal(format!("db commit failed: {e}")))?;
                            if released > 0 {
                                tracing::warn!(
                                    credential_id = %credential_id,
                                    position_id = %position_id,
                                    error = %err,
                                    "released closing position after failed exit submit"
                                );
                            }
                        }
                    }

                    let payload = serde_json::json!({
                        "runMode": run_mode,
                        "paper": false,
                        "error": err,
                        "degraded": credential_degraded,
                    });
                    let mut tx2 = self
                        .store
                        .pool()
                        .begin()
                        .await
                        .map_err(|e| Status::internal(format!("db begin failed: {e}")))?;
                    emit_event(
                        &mut tx2,
                        self.store.kind(),
                        &credential_id,
                        "error",
                        &ctx,
                        &final_order_id,
                        None,
                        Some(payload),
                        None,
                    )
                    .await?;
                    tx2.commit()
                        .await
                        .map_err(|e| Status::internal(format!("db commit failed: {e}")))?;

                    // Abnormal trigger: force a reconcile soon (best-effort).
                    let store = self.store.clone();
                    let limiter = self.limiter.clone();
                    let cred = credential_id.clone();
                    let keys = keys_from_env(&cred);
                    if let Ok(Some(keys)) = keys {
                        tokio::spawn(async move {
                            let _ = reconcile_binance_futures(&cred, store, limiter, keys).await;
                        });
                    }
                }
            }

            if inserted && !is_exchange_backed && order_role == OrderRole::Entry {
                if let Some(position_id) = prepared.position_id.as_deref() {
                    if v2_intent.is_some_and(|context| context.risk.protection.is_some()) {
                        let protection_acks = self
                            .submit_planned_protection(position_id, &run_mode)
                            .await?;
                        if protection_acks.iter().any(|ack| ack.status == "error") {
                            return Err(Status::failed_precondition(
                                "paper protection attachment returned an error",
                            ));
                        }
                    }
                }
            }

            if exact_close_exchange {
                if let Some(context) = exact_close.as_ref() {
                    // 提交已经有了结局（ACK / unknown / 本地拒绝）：已被交易所
                    // 判死的立即按真值终结；其余留给对账真值，不发任何请求。
                    // 落定失败不能盖住已经发生的提交结局——回执读取与对账会
                    // 按同一规则再落定一次。
                    if let Err(error) = crate::position_close::settle_request(
                        &self.store,
                        &credential_id,
                        &context.request_ref,
                        None,
                        now_ms(),
                    )
                    .await
                    {
                        tracing::error!(
                            credential_id = %credential_id,
                            request_ref = %context.request_ref,
                            %error,
                            "exchange close settlement after submit failed; truth paths will retry"
                        );
                    }
                }
            }

            let ack_status = if !inserted {
                "duplicate"
            } else if live_reject.is_some() {
                "rejected"
            } else if live_error.is_some() || live_ack_unresolved {
                "error"
            } else {
                "accepted"
            };

            let ack_message = if !inserted {
                duplicate_ack_message(is_exchange_backed, &authority_idem_key)
            } else if is_exchange_backed {
                if live_ack_unresolved {
                    format!(
                        "{}: remote ACK identity retained ({}); local state unresolved; authoritative_idempotency_key={authority_idem_key}",
                        protection::ACK_LOCAL_STATE_UNRESOLVED,
                        live_exchange_ack_identity.as_deref().unwrap_or("algo"),
                    )
                } else if let Some(id) = live_exchange_ack_identity.as_deref() {
                    format!(
                        "exchange submitted ({run_mode_norm}): {id}; authoritative_idempotency_key={authority_idem_key}"
                    )
                } else if let Some(r) = live_reject.as_deref() {
                    if r.starts_with(crate::account_identity::CONFLICT_CODE)
                        || r.starts_with(crate::account_identity::UNAVAILABLE_CODE)
                    {
                        format!("rejected: {}; {r}", crate::account_identity::denial_code(r))
                    } else {
                        format!(
                            "exchange rejected ({run_mode_norm}): {r}; authoritative_idempotency_key={authority_idem_key}"
                        )
                    }
                } else if let Some(err) = live_error.as_deref() {
                    format!("exchange error ({run_mode_norm}): {err}; authoritative_idempotency_key={authority_idem_key}")
                } else {
                    format!("exchange accepted ({run_mode_norm}); authoritative_idempotency_key={authority_idem_key}")
                }
            } else {
                format!("paper {run_mode}; authoritative_idempotency_key={authority_idem_key}")
            };

            acks.push(IntentAck {
                idempotency_key: intent.idempotency_key,
                engine_order_id: final_order_id,
                status: ack_status.to_string(),
                message: ack_message,
            });
        }

        Ok(Response::new(ExecuteIntentsResponse { acks }))
    }

    async fn get_order(
        &self,
        request: Request<GetOrderRequest>,
    ) -> Result<Response<GetOrderResponse>, Status> {
        let budget =
            get_order_budget::GetOrderBudget::new(tokio::time::Instant::now(), request.metadata());
        let mut refresh_outcome = get_order_budget::RefreshOutcome::Local;
        let operation = async {
            let mut persistence_started = false;
            let req = request.into_inner();
            let row = sqlx::query(
                r#"
            SELECT eo.engine_order_id, eo.credential_id, eo.bot_uid,
                   COALESCE(eo.run_id, '') AS run_id,
                   COALESCE(eo.cycle_id, '') AS cycle_id,
                   eo.idempotency_key,
                   COALESCE(eo.client_order_id_short, '') AS client_order_id_short,
                   COALESCE(eo.exchange_order_id, '') AS exchange_order_id,
                   COALESCE(po.protection_order_id, '') AS protection_order_id,
                   COALESCE(po.exchange_algo_id, '') AS exchange_algo_id,
                   COALESCE(po.client_algo_id, '') AS client_algo_id,
                   eo.exchange, eo.market_kind, eo.symbol_key, eo.status,
                   CAST(eo.created_at_ms AS CHAR) AS created_at_ms_text,
                   CAST(eo.updated_at_ms AS CHAR) AS updated_at_ms_text
            FROM engine_orders eo
            LEFT JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id
            WHERE eo.credential_id = ? AND eo.engine_order_id = ?
            LIMIT 1
            "#,
            )
            .bind(&req.credential_id)
            .bind(&req.engine_order_id)
            .fetch_optional(self.store.pool())
            .await
            .map_err(|e| Status::internal(format!("db select failed: {e}")))?;

            if let Some(row) = row {
                // Refresh is optional until persistence begins. All exchange reads share
                // the same entry-relative deadline; persistence has only the remaining
                // total budget and can never time out into a successful stale fallback.
                let mut persisted_rest_state: Option<(String, i64)> = None;
                let exchange: String = row.get("exchange");
                let market_kind: String = row.get("market_kind");
                let symbol_key: String = row.get("symbol_key");
                let decode_optional = |column| {
                    try_get_nullable_string(&row, column).map_err(|error| {
                        Status::internal(format!("db decode {column} failed: {error}"))
                    })
                };
                let mut exchange_order_id =
                    decode_optional("exchange_order_id")?.unwrap_or_default();
                let protection_order_id =
                    decode_optional("protection_order_id")?.unwrap_or_default();
                let exchange_algo_id = decode_optional("exchange_algo_id")?.unwrap_or_default();
                let client_algo_id = decode_optional("client_algo_id")?.unwrap_or_default();
                let client_order_id_short =
                    decode_optional("client_order_id_short")?.unwrap_or_default();
                let run_id = decode_optional("run_id")?.unwrap_or_default();
                let cycle_id = decode_optional("cycle_id")?.unwrap_or_default();

                if exchange.trim().eq_ignore_ascii_case("binance")
                    && market_kind.trim().eq_ignore_ascii_case("futures")
                    && exchange_gateway_allowed(&req.credential_id)
                        .map_err(Status::failed_precondition)?
                {
                    let keys =
                        keys_from_env(&req.credential_id).map_err(Status::failed_precondition)?;
                    // regression：交易所边界一律走闭世界身份，不接受裸 symbol。
                    let symbol = crate::binance_futures::usdm_symbol(&symbol_key);
                    if let (Some(keys), Some(symbol)) = (keys, symbol) {
                        if let Ok(client) = BinanceFuturesClient::new_with_clock(
                            keys,
                            &req.credential_id,
                            self.store.clone(),
                            self.clock.clone(),
                        ) {
                            let is_algo =
                                !exchange_algo_id.is_empty() || !client_algo_id.is_empty();
                            if is_algo {
                                let algo_id = exchange_algo_id
                                    .parse::<i64>()
                                    .ok()
                                    .map(crate::binance_futures::BinanceAlgoId::new);
                                let client_id = (!client_algo_id.is_empty())
                                    .then(|| BinanceClientAlgoId::new(client_algo_id.clone()));
                                let (query_algo_id, query_client_algo_id) = if algo_id.is_some() {
                                    (algo_id.as_ref(), None)
                                } else {
                                    (None, client_id.as_ref())
                                };
                                if let Some(value) = budget
                                    .read(
                                        client.get_algo_order_json(
                                            &self.limiter,
                                            query_algo_id,
                                            query_client_algo_id,
                                            RequestClass::EventTruth,
                                        ),
                                        persistence_started,
                                        &mut refresh_outcome,
                                    )
                                    .await?
                                {
                                    if let Ok(truth) = protection::AlgoOrderTruth::from_json(value)
                                    {
                                        if budget.begin_persistence(
                                            &mut persistence_started,
                                            &mut refresh_outcome,
                                        )? {
                                            protection::apply_algo_truth_for_protection(
                                            &self.store,
                                            &req.credential_id,
                                            &protection_order_id,
                                            &truth,
                                            protection::AlgoTruthSource::RestAlgoQuery,
                                            "get_order_refresh",
                                            now_ms(),
                                        )
                                        .await
                                        .map_err(|_| {
                                            Status::unavailable(
                                                "GetOrder Algo persistence was not confirmed; read durable state again",
                                            )
                                        })?;
                                            // The Algo writer can commit state before its audit write.
                                            // Only a confirmed return followed by this durable read may
                                            // supply a successful status/timestamp pair. Never combine
                                            // an in-memory Algo status with the initial row's timestamp.
                                            let persisted = sqlx::query(
                                            r#"
                                            SELECT status, COALESCE(exchange_order_id, '') AS exchange_order_id,
                                                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
                                            FROM engine_orders WHERE credential_id=? AND engine_order_id=?
                                            "#,
                                        )
                                        .bind(&req.credential_id)
                                        .bind(&req.engine_order_id)
                                        .fetch_one(self.store.pool())
                                        .await
                                        .map_err(|_| {
                                            Status::unavailable("GetOrder Algo durable projection failed")
                                        })?;
                                            exchange_order_id = try_get_nullable_string(
                                                &persisted,
                                                "exchange_order_id",
                                            )
                                            .map_err(|_| {
                                                Status::unavailable(
                                                    "GetOrder Algo identity decode failed",
                                                )
                                            })?
                                            .unwrap_or_default();
                                            persisted_rest_state = Some((
                                                persisted.get("status"),
                                                crate::store::try_get_i64_text(
                                                    &persisted,
                                                    "updated_at_ms_text",
                                                )
                                                .map_err(|_| {
                                                    Status::unavailable(
                                                        "GetOrder Algo timestamp decode failed",
                                                    )
                                                })?,
                                            ));
                                            refresh_outcome =
                                                get_order_budget::RefreshOutcome::Confirmed;
                                        }
                                    }
                                }
                            }

                            // An untriggered Algo order has no ordinary orderId and
                            // must never be queried through /fapi/v1/order. Once
                            // actualOrderId is bound, ordinary status/fills own it.
                            if !is_algo || !exchange_order_id.is_empty() {
                                if let Some(resp) = budget
                                    .read(
                                        client.get_order_json(
                                            &symbol,
                                            if exchange_order_id.is_empty() {
                                                None
                                            } else {
                                                Some(exchange_order_id.as_str())
                                            },
                                            if is_algo || client_order_id_short.is_empty() {
                                                None
                                            } else {
                                                Some(client_order_id_short.as_str())
                                            },
                                            RequestClass::EventTruth,
                                        ),
                                        persistence_started,
                                        &mut refresh_outcome,
                                    )
                                    .await?
                                {
                                    let ordinary_next = resp
                                        .get("status")
                                        .and_then(|v| v.as_str())
                                        .and_then(binance_order_status_to_engine_status);

                                    // Persist only a strict monotonic advance. The predicate lives in
                                    // the UPDATE so a WS/fill writer that wins while REST is awaited
                                    // cannot be overwritten by this older observation. The status and
                                    // its order_update audit fact are one transaction: either both
                                    // commit or neither does.
                                    if let Some(next) = ordinary_next {
                                        if budget.begin_persistence(
                                            &mut persistence_started,
                                            &mut refresh_outcome,
                                        )? {
                                            let weaker = statuses_weaker_than(next);
                                            let placeholders =
                                                std::iter::repeat_n("?", weaker.len())
                                                    .collect::<Vec<_>>()
                                                    .join(",");
                                            // MySQL's default VARCHAR collation is commonly
                                            // case/accent-insensitive. The closed-world status predicate
                                            // is an authority boundary, so dirty bytes such as `ACKED`
                                            // must not compare equal to canonical `acked` and become an
                                            // unaudited normalization write. SQLite's default comparison
                                            // is already byte-sensitive for these ASCII values.
                                            let status_column = match self.store.kind() {
                                                DbKind::MySql => "BINARY status",
                                                _ => "status",
                                            };
                                            let update_sql = format!(
                                                r#"
                                        UPDATE engine_orders
                                        SET status = ?, updated_at_ms = ?
                                        WHERE credential_id = ? AND engine_order_id = ?
                                          AND {status_column} IN ({placeholders})
                                        "#
                                            );
                                            let updated_at_ms = now_ms();
                                            let mut tx =
                                                self.store.pool().begin().await.map_err(|e| {
                                                    Status::internal(format!(
                                                        "db order refresh begin failed: {e}"
                                                    ))
                                                })?;
                                            let mut update = sqlx::query(&update_sql)
                                                .bind(next)
                                                .bind(updated_at_ms)
                                                .bind(&req.credential_id)
                                                .bind(&req.engine_order_id);
                                            for status in &weaker {
                                                update = update.bind(*status);
                                            }
                                            let applied =
                                                update.execute(&mut *tx).await.map_err(|e| {
                                                    Status::internal(format!(
                                                        "db order refresh update failed: {e}"
                                                    ))
                                                })?;

                                            if applied.rows_affected() == 1 {
                                                let ctx = EventContext {
                                                    bot_uid: row.get("bot_uid"),
                                                    run_id: run_id.clone(),
                                                    cycle_id: cycle_id.clone(),
                                                    idempotency_key: row.get("idempotency_key"),
                                                    client_order_id_short: client_order_id_short
                                                        .clone(),
                                                    exchange: exchange.clone(),
                                                    market_kind: market_kind.clone(),
                                                    symbol_key: symbol_key.clone(),
                                                };

                                                let order_id = resp
                                                    .get("orderId")
                                                    .and_then(|v| v.as_i64())
                                                    .map(|v| v.to_string())
                                                    .unwrap_or_else(|| exchange_order_id.clone());
                                                let update_time = resp
                                                    .get("updateTime")
                                                    .and_then(|v| v.as_i64())
                                                    .unwrap_or(updated_at_ms);
                                                let eid = get_order_refresh_event_id(
                                                    &req.credential_id,
                                                    &req.engine_order_id,
                                                    &symbol_key,
                                                    &order_id,
                                                    next,
                                                    update_time,
                                                );

                                                emit_event(
                                            &mut tx,
                                            self.store.kind(),
                                            &req.credential_id,
                                            "order_update",
                                            &ctx,
                                            &req.engine_order_id,
                                            if order_id.is_empty() { None } else { Some(order_id.as_str()) },
                                            Some(serde_json::json!({"runMode":"live","paper":false,"source":"binance_rest","binance":resp})),
                                            Some(eid),
                                        )
                                        .await?;
                                            }

                                            // A zero-row CAS means another producer already owns a
                                            // same-rank/stronger truth (or the row became unknown). Return
                                            // what is actually durable, including its matching timestamp,
                                            // rather than the REST observation paired with the stale row.
                                            // The read stays in this transaction so a successful commit is
                                            // the exact status/timestamp projection returned by the RPC.
                                            let persisted = sqlx::query(
                                                r#"
                                        SELECT status,
                                               CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
                                        FROM engine_orders
                                        WHERE credential_id = ? AND engine_order_id = ?
                                        LIMIT 1
                                        "#,
                                            )
                                            .bind(&req.credential_id)
                                            .bind(&req.engine_order_id)
                                            .fetch_one(&mut *tx)
                                            .await
                                            .map_err(|e| {
                                                Status::internal(format!(
                                                    "db order refresh select failed: {e}"
                                                ))
                                            })?;
                                            let persisted_updated_at_ms =
                                                crate::store::try_get_i64_text(
                                                    &persisted,
                                                    "updated_at_ms_text",
                                                )
                                                .map_err(|e| {
                                                    Status::internal(format!(
                                                        "db order refresh decode failed: {e}"
                                                    ))
                                                })?;
                                            let persisted_status = persisted.get("status");
                                            tx.commit().await.map_err(|e| {
                                                Status::internal(format!(
                                                    "db order refresh commit failed: {e}"
                                                ))
                                            })?;
                                            persisted_rest_state =
                                                Some((persisted_status, persisted_updated_at_ms));
                                            refresh_outcome =
                                                get_order_budget::RefreshOutcome::Confirmed;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let stored_updated_at_ms =
                    crate::store::try_get_i64_text(&row, "updated_at_ms_text")
                        .map_err(|error| Status::internal(format!("db decode failed: {error}")))?;
                let (response_status, response_updated_at_ms) = persisted_rest_state
                    .unwrap_or_else(|| (row.get("status"), stored_updated_at_ms));
                let order = proto::EngineOrder {
                    engine_order_id: row.get("engine_order_id"),
                    credential_id: row.get("credential_id"),
                    bot_uid: row.get("bot_uid"),
                    run_id,
                    cycle_id,
                    idempotency_key: row.get("idempotency_key"),
                    client_order_id_short,
                    exchange_order_id,
                    exchange: row.get("exchange"),
                    market_kind: row.get("market_kind"),
                    symbol_key: row.get("symbol_key"),
                    status: response_status,
                    // **sqlx-i64 核查批**：旧 sqlx 0.7.4
                    // `Any` + SQLite 会把这两列截成低 32 位；0.8.6 已修复。
                    // 文本投影暂留，防止 gRPC 向 control-plane 交付虚构时刻。
                    created_at_ms: crate::store::try_get_i64_text(&row, "created_at_ms_text")
                        .map_err(|error| Status::internal(format!("db decode failed: {error}")))?,
                    updated_at_ms: response_updated_at_ms,
                };
                return Ok(Response::new(GetOrderResponse {
                    found: true,
                    order: Some(order),
                }));
            }

            Ok(Response::new(GetOrderResponse {
                found: false,
                order: None,
            }))
        };
        let result = budget.finish(operation).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code() == tonic::Code::DeadlineExceeded)
            && tokio::time::Instant::now() >= budget.total
        {
            refresh_outcome = get_order_budget::RefreshOutcome::TotalDeadline;
        }
        tracing::info!(
            refresh_result = refresh_outcome.as_str(),
            elapsed_ms = budget.entered.elapsed().as_millis() as u64,
            rpc_ok = result.is_ok(),
            "GetOrder bounded refresh"
        );
        result
    }

    async fn get_open_orders(
        &self,
        request: Request<proto::GetOpenOrdersRequest>,
    ) -> Result<Response<proto::GetOpenOrdersResponse>, Status> {
        let req = request.into_inner();
        let credential_id = req.credential_id.trim();
        let symbol_key = req.symbol_key.trim();
        let cycle_id = req.cycle_id.trim();

        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id required"));
        }

        // Build query with optional filters
        let mut query = String::from(
            r#"
            SELECT engine_order_id, credential_id, bot_uid,
                   COALESCE(run_id, '') AS run_id,
                   COALESCE(cycle_id, '') AS cycle_id,
                   idempotency_key,
                   COALESCE(client_order_id_short, '') AS client_order_id_short,
                   COALESCE(exchange_order_id, '') AS exchange_order_id,
                   exchange, market_kind, symbol_key, status,
                   CAST(created_at_ms AS CHAR) AS created_at_ms_text,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
            FROM engine_orders
            WHERE credential_id = ?
              AND status IN ('acked', 'partial_filled', 'confirmed', 'triggering')
            "#,
        );

        if !symbol_key.is_empty() {
            query.push_str(" AND symbol_key = ?");
        }
        if !cycle_id.is_empty() {
            query.push_str(" AND cycle_id = ?");
        }
        query.push_str(" ORDER BY created_at_ms DESC LIMIT 100");

        let mut q = sqlx::query(&query).bind(credential_id);
        if !symbol_key.is_empty() {
            q = q.bind(symbol_key);
        }
        if !cycle_id.is_empty() {
            q = q.bind(cycle_id);
        }

        let rows = q
            .fetch_all(self.store.pool())
            .await
            .map_err(|e| Status::internal(format!("db query failed: {e}")))?;

        let mut orders = Vec::new();
        for row in rows {
            let order = proto::EngineOrder {
                engine_order_id: row.get("engine_order_id"),
                credential_id: row.get("credential_id"),
                bot_uid: row.get("bot_uid"),
                run_id: row.get("run_id"),
                cycle_id: row.get("cycle_id"),
                idempotency_key: row.get("idempotency_key"),
                client_order_id_short: row.get("client_order_id_short"),
                exchange_order_id: row.get("exchange_order_id"),
                exchange: row.get("exchange"),
                market_kind: row.get("market_kind"),
                symbol_key: row.get("symbol_key"),
                status: row.get("status"),
                // **sqlx-i64 核查批**：同 `get_order`。
                created_at_ms: crate::store::try_get_i64_text(&row, "created_at_ms_text")
                    .map_err(|error| Status::internal(format!("db decode failed: {error}")))?,
                updated_at_ms: crate::store::try_get_i64_text(&row, "updated_at_ms_text")
                    .map_err(|error| Status::internal(format!("db decode failed: {error}")))?,
            };
            orders.push(order);
        }

        Ok(Response::new(proto::GetOpenOrdersResponse { orders }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let row = sqlx::query(
            r#"
            SELECT eo.engine_order_id, eo.credential_id, eo.bot_uid,
                   COALESCE(eo.run_id, '') AS run_id,
                   COALESCE(eo.cycle_id, '') AS cycle_id,
                   eo.idempotency_key,
                   COALESCE(eo.client_order_id_short, '') AS client_order_id_short,
                   COALESCE(eo.exchange_order_id, '') AS exchange_order_id,
                   COALESCE(po.exchange_algo_id, '') AS exchange_algo_id,
                   COALESCE(po.client_algo_id, '') AS client_algo_id,
                   eo.exchange, eo.market_kind, eo.symbol_key, eo.status
            FROM engine_orders eo
            LEFT JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id
            WHERE eo.credential_id = ? AND eo.engine_order_id = ?
            LIMIT 1
            "#,
        )
        .bind(&req.credential_id)
        .bind(&req.engine_order_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|e| Status::internal(format!("db select failed: {e}")))?;

        let Some(row) = row else {
            return Ok(Response::new(CancelOrderResponse {
                accepted: false,
                message: "not_found".to_string(),
            }));
        };

        let exchange: String = row.get("exchange");
        let market_kind: String = row.get("market_kind");
        let symbol_key: String = row.get("symbol_key");
        let status: String = row.get("status");
        let decode_optional = |column| {
            try_get_nullable_string(&row, column)
                .map_err(|error| Status::internal(format!("db decode {column} failed: {error}")))
        };
        let exchange_order_id = decode_optional("exchange_order_id")?.unwrap_or_default();
        let client_order_id_short = decode_optional("client_order_id_short")?.unwrap_or_default();
        let exchange_algo_id = decode_optional("exchange_algo_id")?.unwrap_or_default();
        let client_algo_id = decode_optional("client_algo_id")?.unwrap_or_default();
        let run_id = decode_optional("run_id")?.unwrap_or_default();
        let cycle_id = decode_optional("cycle_id")?.unwrap_or_default();

        if status == "filled" {
            return Ok(Response::new(CancelOrderResponse {
                accepted: false,
                message: "already_filled".to_string(),
            }));
        }

        // Live cancel (Binance futures v1).
        if exchange.trim().eq_ignore_ascii_case("binance")
            && market_kind.trim().eq_ignore_ascii_case("futures")
            && exchange_gateway_allowed(&req.credential_id).map_err(Status::failed_precondition)?
        {
            let keys = keys_from_env(&req.credential_id).map_err(Status::failed_precondition)?;
            // regression：交易所边界一律走闭世界身份，不接受裸 symbol。
            //
            // **认不出身份不得落 local
            // 分支。** R12 这里是 `if let (Some(keys), Some(symbol))`：身份解析
            // 失败时整个 `if` 落空，控制流继续往下走到本方法末尾那条
            // **paper/dev 的 local cancel**——把 `engine_orders` 直接写成
            // `canceled` 并回 `accepted=true`，而**一个字节都没发给交易所**。
            // 能走到这里的前提是 `exchange_gateway_allowed` 为真，也就是
            // 交易所背书的凭据 + live/testnet 档位：那张单很可能仍然在
            // 交易所挂着。本地一句"已撤"之后，撤单真值就此丢失，
            // 没有任何东西会再去撤它（engine safety contract「运行状态丢失」「paper/live
            // 混淆」一律 High）。
            //
            // 现在身份失败在这里就 fail-closed 返回：不写库、不回
            // `accepted=true`、留一条 `error!` 告警，交由运维/对账处置。
            let Some(symbol) = crate::binance_futures::usdm_symbol(&symbol_key) else {
                tracing::error!(
                    credential_id = %req.credential_id,
                    engine_order_id = %req.engine_order_id,
                    %symbol_key,
                    "live cancel refused: the stored symbol_key is not a binance USDⓈ-M \
                     identity; the exchange order may still be open and must not be \
                     marked canceled locally"
                );
                return Ok(Response::new(CancelOrderResponse {
                    accepted: false,
                    message: format!(
                        "cancel_refused_unresolved_identity: symbol_key {symbol_key:?} is not a \
                         binance USDⓈ-M identity; the exchange order was not touched"
                    ),
                }));
            };
            // **。**
            //
            // R13 修掉了身份解析失败那条落 local 的路，`keys` 为 `None` 的
            // 那条**原样留着**：`if let Some(keys)` 落空之后控制流照样走到
            // 方法末尾那条 paper/dev 的 local cancel，把单写成 `canceled`
            // 并回 `accepted=true`。能走到这里的前提是
            // `exchange_gateway_allowed`——交易所背书的凭据 + live/testnet
            // 档位：那张单很可能仍挂在交易所上，而本地一句"已撤"之后
            // 没有任何东西会再去撤它（engine safety contract「运行状态丢失」
            // 「paper/live 混淆」一律 High）。
            //
            // 缺密钥不是"降级成本地撤单"的理由，是**运维事故**：
            // fail-closed 拒绝 + `error!` 告警，交由运维/对账处置。
            let Some(keys) = keys else {
                tracing::error!(
                    credential_id = %req.credential_id,
                    engine_order_id = %req.engine_order_id,
                    "live cancel refused: the exchange gateway is enabled for this credential \
                     but no api keys are configured; the exchange order may still be open and \
                     must not be marked canceled locally"
                );
                return Ok(Response::new(CancelOrderResponse {
                    accepted: false,
                    message: "cancel_refused_missing_credentials: the exchange gateway is \
                              enabled for this credential but no api keys are configured; \
                              the exchange order was not touched"
                        .to_string(),
                }));
            };
            let client = BinanceFuturesClient::new_with_clock(
                keys,
                &req.credential_id,
                self.store.clone(),
                self.clock.clone(),
            )
            .map_err(|e| Status::internal(format!("binance client init failed: {e}")))?;
            #[cfg(test)]
            let client = match self.cancel_client_override.as_ref() {
                Some(http) => client.with_mock_http_from(http),
                None => client,
            };
            let is_algo = !exchange_algo_id.is_empty() || !client_algo_id.is_empty();
            let cancel_result = if is_algo {
                let algo_id = (!exchange_algo_id.is_empty())
                    .then(|| exchange_algo_id.parse::<i64>())
                    .transpose()
                    .map_err(|error| {
                        Status::internal(format!("invalid stored exchange_algo_id: {error}"))
                    })?
                    .map(crate::binance_futures::BinanceAlgoId::new);
                let client_id = (!client_algo_id.is_empty())
                    .then(|| BinanceClientAlgoId::new(client_algo_id.clone()));
                let client_id = algo_id.is_none().then_some(client_id).flatten();
                client
                    .cancel_algo_order_by_identity_json(
                        &self.limiter,
                        algo_id.as_ref(),
                        client_id.as_ref(),
                        RequestClass::Protection,
                    )
                    .await
            } else {
                client
                    .cancel_order_json(
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
                        RequestClass::Protection,
                    )
                    .await
            };
            match cancel_result {
                Ok(resp) => {
                    let updated_at_ms = now_ms();
                    sqlx::query(
                        r#"
                        UPDATE engine_orders
                        SET status = 'canceled', updated_at_ms = ?
                        WHERE credential_id = ? AND engine_order_id = ?
                        "#,
                    )
                    .bind(updated_at_ms)
                    .bind(&req.credential_id)
                    .bind(&req.engine_order_id)
                    .execute(self.store.pool())
                    .await
                    .map_err(|e| Status::internal(format!("db update failed: {e}")))?;
                    if is_algo {
                        sqlx::query(
                            r#"
                            UPDATE protection_orders
                            SET algo_status='CANCELED', algo_updated_at_ms=?,
                                status='canceled', verified_from_exchange=0,
                                updated_at_ms=?
                            WHERE credential_id=? AND engine_order_id=?
                            "#,
                        )
                        .bind(updated_at_ms)
                        .bind(updated_at_ms)
                        .bind(&req.credential_id)
                        .bind(&req.engine_order_id)
                        .execute(self.store.pool())
                        .await
                        .map_err(|e| {
                            Status::internal(format!("db update algo cancel failed: {e}"))
                        })?;
                    }

                    // Emit an order_update event (best-effort; ignore duplicates).
                    let ctx = EventContext {
                        bot_uid: row.get("bot_uid"),
                        run_id,
                        cycle_id,
                        idempotency_key: row.get("idempotency_key"),
                        client_order_id_short,
                        exchange,
                        market_kind,
                        symbol_key,
                    };
                    let order_id = if is_algo {
                        exchange_order_id.clone()
                    } else {
                        resp.get("orderId")
                            .and_then(|v| v.as_i64())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| exchange_order_id.clone())
                    };
                    let update_time = resp
                        .get("updateTime")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(updated_at_ms);
                    let eid = sha256_hex(&format!(
                        "cancel|{}|{}|{}|{}",
                        req.credential_id, order_id, "CANCELED", update_time
                    ));

                    let mut tx = self
                        .store
                        .pool()
                        .begin()
                        .await
                        .map_err(|e| Status::internal(format!("db begin failed: {e}")))?;
                    let _ = emit_event(
                        &mut tx,
                        self.store.kind(),
                        &req.credential_id,
                        "order_update",
                        &ctx,
                        &req.engine_order_id,
                        if order_id.is_empty() { None } else { Some(order_id.as_str()) },
                        Some(serde_json::json!({"runMode":"live","paper":false,"source":"binance_rest","binance":resp})),
                        Some(eid),
                    )
                    .await;
                    let _ = tx.commit().await;

                    return Ok(Response::new(CancelOrderResponse {
                        accepted: true,
                        message: "canceled".to_string(),
                    }));
                }
                Err(e) => {
                    return Ok(Response::new(CancelOrderResponse {
                        accepted: false,
                        message: format!("cancel_failed: {e}"),
                    }));
                }
            }
        }

        // Fallback: local cancel (paper/dev).
        let updated = now_ms();
        sqlx::query(
            r#"
            UPDATE engine_orders
            SET status = 'canceled', updated_at_ms = ?
            WHERE credential_id = ? AND engine_order_id = ?
            "#,
        )
        .bind(updated)
        .bind(&req.credential_id)
        .bind(&req.engine_order_id)
        .execute(self.store.pool())
        .await
        .map_err(|e| Status::internal(format!("db update failed: {e}")))?;

        Ok(Response::new(CancelOrderResponse {
            accepted: true,
            message: req.reason,
        }))
    }

    async fn reconcile_now(
        &self,
        request: Request<ReconcileNowRequest>,
    ) -> Result<Response<ReconcileNowResponse>, Status> {
        let req = request.into_inner();
        let credential_id = req.credential_id.trim().to_string();
        if credential_id.is_empty() {
            return Ok(Response::new(ReconcileNowResponse {
                accepted: false,
                message: "credential_id required".to_string(),
            }));
        }
        if credential_id == "paper" {
            return Ok(Response::new(ReconcileNowResponse {
                accepted: false,
                message: "paper reconcile not supported".to_string(),
            }));
        }
        let gateway_allowed = match exchange_gateway_allowed(&credential_id) {
            Ok(allowed) => allowed,
            Err(error) => {
                return Ok(Response::new(ReconcileNowResponse {
                    accepted: false,
                    message: error,
                }))
            }
        };
        if !gateway_allowed {
            return Ok(Response::new(ReconcileNowResponse {
                accepted: false,
                message: "live gateway disabled by ENGINE_LIVE_ENABLED".to_string(),
            }));
        }

        let keys = match keys_from_env(&credential_id) {
            Ok(keys) => keys,
            Err(error) => {
                return Ok(Response::new(ReconcileNowResponse {
                    accepted: false,
                    message: error,
                }))
            }
        };
        let Some(keys) = keys else {
            return Ok(Response::new(ReconcileNowResponse {
                accepted: false,
                message: "missing binance api keys in env".to_string(),
            }));
        };

        // Ensure periodic reconcile stays alive.
        self.reconcilers
            .ensure_running(
                &credential_id,
                self.store.clone(),
                self.limiter.clone(),
                keys.clone(),
            )
            .await;

        let store = self.store.clone();
        let limiter = self.limiter.clone();
        let task_credential_id = credential_id.clone();
        tokio::spawn(async move {
            tracing::info!(credential_id=%task_credential_id, reason=%req.reason, "manual reconcile requested");
            let _ = reconcile_binance_futures(&task_credential_id, store, limiter, keys).await;
        });

        Ok(Response::new(ReconcileNowResponse {
            accepted: true,
            message: "reconcile scheduled".to_string(),
        }))
    }

    async fn ensure_market_data(
        &self,
        request: Request<EnsureMarketDataRequest>,
    ) -> Result<Response<EnsureMarketDataResponse>, Status> {
        let req = request.into_inner();
        let credential_id = req.credential_id.trim().to_string();
        if credential_id.is_empty() {
            return Ok(Response::new(EnsureMarketDataResponse {
                accepted: false,
                message: "credential_id required".to_string(),
                started: 0,
            }));
        }

        // ****
        //
        // R7 在这里逐条落库再逐条起流，两个洞：
        // ①**批量登记不是原子的**——任一条 DB 失败立即返回，已写的前缀不回滚；
        //   编排器 catch 掉整个 RPC 失败之后**仍把 bot 置 running**，
        //   于是未登记的后缀既没有 stream task、也不在 required 集合里，
        //   行情缺失**无法 fail-closed**（这正是本条判据要防的事）。
        // ②**未知 timeframe 被持久接受**——RPC 只校验非空，登记之后中断判据
        //   又因为映射不出阈值而 warn 后 skip：这条流永远不产 bar、
        //   也永远不触发 pause_new，等于一条对判据隐形的死流。
        //
        // **那道"闭世界"不是闭世界。**
        //
        // R8 的校验有三个洞，评审各实跑了一条反例：
        // ①unsupported venue（`okx`）/ market、空 symbol/timeframe 被逐条
        //   `continue` **静默丢弃**——丢掉之后既没有 stream、也不在 required
        //   集合里，判据对它完全失明，而 RPC 仍回 `accepted=true`；
        // ②timeframe 只过 `timeframe_to_ms` 这个**通用解析器**，Binance 根本
        //   不提供的 `7m` 能解析即被接受；
        // ③全部条目被丢光之后仍然 `accepted=true, started=0`，
        //   编排器只看 `accepted` 就把 bot 置 running。
        //
        // 处置：判据换成**真正的能力集合**（venue × market × symbol × timeframe，
        // 见 `market_data::validate_subscription`），并且**任何一条无效就整批
        // 拒绝**——不接受"丢掉坏的留下好的"。整批拒绝时一行都不落库，
        // 一条 stream 都不起：宁可这次 ensure 整体失败，也不留下
        // "半个订阅集合"这种判据看不见的状态。
        let validated = match crate::market_data::validate_subscription_batch(
            req.subscriptions.iter().map(|sub| {
                (
                    sub.exchange.as_str(),
                    sub.market_kind.as_str(),
                    sub.symbol_key.as_str(),
                    sub.timeframe.as_str(),
                )
            }),
        ) {
            Ok(validated) => validated,
            Err(message) => {
                tracing::error!(
                    credential_id = %credential_id,
                    reason = %message,
                    "ensure_market_data refused the whole batch"
                );
                return Ok(Response::new(EnsureMarketDataResponse {
                    accepted: false,
                    message,
                    started: 0,
                }));
            }
        };

        // 整批登记：一个事务，全成或全不成。
        let registrations: Vec<(String, String)> = validated
            .iter()
            .map(|subscription| {
                (
                    subscription.symbol_key.clone(),
                    subscription.timeframe.clone(),
                )
            })
            .collect();
        crate::auto_triggers::record_required_streams_atomically(
            &self.store,
            &credential_id,
            &registrations,
            now_ms(),
        )
        .await
        .map_err(|error| Status::internal(format!("db record required streams failed: {error}")))?;

        let mut started = 0u32;
        for subscription in &validated {
            if self
                .market_data
                .ensure_kline_running(
                    &credential_id,
                    self.store.clone(),
                    subscription.symbol_key.as_str(),
                    subscription.timeframe.as_str(),
                )
                .await
            {
                started = started.saturating_add(1);
            }
        }

        Ok(Response::new(EnsureMarketDataResponse {
            accepted: true,
            message: "ok".to_string(),
            started,
        }))
    }
}

/// **一帧 WS 更新没能被本地落定**。
///
/// R10 把「写失败整条回滚」做对了，但函数返回 `()`：reader 那一侧**看不到**
/// 失败，于是继续读下一帧、不重连、不降 readiness。库里少了一条交易所真值，
/// 而 stream 仍然 `ready=1`，新开仓照常放行——这正是 `SAFETY_MODEL.md`
/// 「用户流启动、重连或回补失败 → stream 保持 not-ready」那条恢复模型
/// 要排除的状态。
///
/// 现在这类失败沿 `handle_ws_envelope` 上抛到 reader，reader 记 recovery
/// failure（`ready=0`）并让整条流失败返回，由外层重连路径重新走
/// **回补 → reconcile → ready** 把这一帧的真值补回来。
#[derive(Clone, Debug)]
pub(crate) struct WsApplyError {
    message: String,
}

impl WsApplyError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for WsApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WsApplyError {}

#[derive(Clone)]
pub(crate) struct BinanceEventSink {
    store: Store,
    /// 测试专用的保护单失败处置替身（**不联网**）。生产恒为 `None`：
    /// 真实路径按 credential 现取交易所 adapter。
    #[cfg(test)]
    protection_failure_adapter: Option<Arc<dyn crate::protection::ProtectionAdapter>>,
}

impl BinanceEventSink {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            #[cfg(test)]
            protection_failure_adapter: None,
        }
    }

    /// 注入一个**无网络**的保护失败处置替身。
    #[cfg(test)]
    pub(crate) fn with_protection_failure_adapter(
        mut self,
        adapter: Arc<dyn crate::protection::ProtectionAdapter>,
    ) -> Self {
        self.protection_failure_adapter = Some(adapter);
        self
    }

    pub async fn handle_ws_envelope(
        &self,
        credential_id: &str,
        env: &BinanceWsEnvelope,
    ) -> Result<(), WsApplyError> {
        let t = env.event_type.trim();
        if t == "ORDER_TRADE_UPDATE" {
            if let Some(order) = env.order.as_ref() {
                self.handle_order_trade_update(credential_id, env, order)
                    .await?;
            }
        } else if t == "ALGO_UPDATE" {
            if let Some(algo) = env.algo_update.as_ref() {
                self.handle_algo_update(credential_id, env, algo).await?;
            }
        } else if t == "ACCOUNT_UPDATE" {
            self.handle_account_update(credential_id, env).await?;
        }
        Ok(())
    }

    /// **REST 回补侧的本地归属裁决——按完整交易身份，不按时间。**
    ///
    /// 。R12 这里是
    /// `ORDER BY eo.created_at_ms DESC LIMIT 1`：**先按裸 `orderId` 取最新
    /// 的那一行**，取到之后才去比 symbol。Binance 的 `orderId` 只在
    /// (账户, symbol) 内唯一——两张不同合约的本地单撞同一个 id 时，较新的
    /// 那一行会把正确的那一行整个遮住：后验一比 symbol 不符就整帧
    /// `failed_precondition`，正确的本地单永远轮不到被认领。评审把
    /// `DESC` 改成 `ASC`（R9M11）之后现有 3 条用例全绿，正说明"哪一行被选中"
    /// 从来没有被任何用例观察过。
    ///
    /// 现在裁决与顺序无关：先取**候选集合**（credential + exchange_order_id），
    /// 再用全仓唯一那份 USDⓈ-M 五段身份谓词 `usdm_symbol_key_matches`
    /// 筛出真正属于这条交易所消息的那张单。
    ///
    /// * 恰好一条 → 认领；
    /// * 本地压根没有这个 `orderId`（候选集为空）→ `None`，照旧登记成
    ///   `external:<id>` 的外部成交（这是 fill recovery 的完整性要求，
    ///   不是身份歧义）；
    /// * 有候选但**没有一条**身份相符 → fail-closed：这条真值不属于本地
    ///   任何一张单，绝不能落到撞号的那一张上；
    /// * 有**多条**身份相符 → 同样 fail-closed：本地自己就有歧义，
    ///   随便挑一条等于赌。
    ///
    /// 两种 fail-closed 都返回 `Err`，由 `binance_user_stream` 的回补循环
    /// 落成 `record_recovery_failure`（`ready=0`）并驱动重连恢复——
    /// **不得**返回健康。
    async fn claim_local_order_for_rest_fill(
        &self,
        credential_id: &str,
        exchange_order_id: &str,
        symbol: &str,
    ) -> Result<Option<sqlx::any::AnyRow>, Status> {
        let candidates = sqlx::query(
            r#"
            SELECT eo.engine_order_id, eo.bot_uid, eo.run_id, eo.cycle_id,
                   eo.idempotency_key, eo.client_order_id_short,
                   eo.exchange, eo.market_kind, eo.symbol_key, eo.action, eo.side,
                   COALESCE(rv.intent_id, '') AS intent_id,
                   COALESCE(eo.risk_verdict_id, '') AS risk_verdict_id,
                   COALESCE(eo.position_id, '') AS position_id,
                   COALESCE(eo.order_role, '') AS order_role
            FROM engine_orders eo
            LEFT JOIN risk_verdicts rv
              ON rv.risk_verdict_id = eo.risk_verdict_id
            WHERE eo.credential_id = ? AND eo.exchange_order_id = ?
            "#,
        )
        .bind(credential_id)
        .bind(exchange_order_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db match REST fill failed: {error}")))?;
        if candidates.is_empty() {
            return Ok(None);
        }
        // **候选行的两个身份来源必须一致。**
        //
        // R13 这里只信 `symbol_key` 一列。`engine_orders` 的同一行还存着
        // **外层** `exchange` / `market_kind`：一条 `exchange='okx'` /
        // `market_kind='spot'` 而 `symbol_key='binance|futures|BTC|USDT|USDT'`
        // 的矛盾行，只比内层就会被这条 Binance USDⓈ-M 的 REST 成交认领，
        // 于是别的市场的一张单被写上 Binance 的成交真值、并顺带播种恢复域。
        //
        // 现在逐行核对三段（外层两列 + 内层五段 key）。矛盾行不是"另一个市场
        // 的单"可以静默跳过——两个来源里必有一个是错的，本地在这条
        // `orderId` 上已经无法自证归属：**整帧 fail-closed + 告警**，
        // 由 `binance_user_stream` 的回补循环落成 `record_recovery_failure`。
        for row in &candidates {
            let symbol_key = row.get::<String, _>("symbol_key");
            let exchange = try_get_nullable_string(row, "exchange")
                .map_err(|error| Status::internal(format!("db decode exchange failed: {error}")))?
                .unwrap_or_default();
            let market_kind = try_get_nullable_string(row, "market_kind")
                .map_err(|error| {
                    Status::internal(format!("db decode market_kind failed: {error}"))
                })?
                .unwrap_or_default();
            if let Err(reason) = crate::binance_futures::trading_identity_sources_agree(
                &symbol_key,
                &exchange,
                &market_kind,
            ) {
                tracing::error!(
                    credential_id,
                    exchange_order_id,
                    %symbol,
                    engine_order_id = %row.get::<String, _>("engine_order_id"),
                    %reason,
                    "REST fill candidate carries two disagreeing trading identities;                      this exchange truth must not be claimed by any local order"
                );
                return Err(Status::failed_precondition(format!(
                    "REST fill candidate identity is inconsistent: credential={credential_id}                      symbol={symbol} order_id={exchange_order_id}: {reason}"
                )));
            }
        }
        let mut claimed = candidates
            .into_iter()
            .filter(|row| {
                crate::binance_futures::usdm_symbol_key_matches(
                    &row.get::<String, _>("symbol_key"),
                    symbol,
                )
            })
            .collect::<Vec<_>>();
        match claimed.len() {
            1 => Ok(claimed.pop()),
            0 => Err(Status::failed_precondition(format!(
                "REST fill symbol mismatch: credential={credential_id} symbol={symbol} \
                 order_id={exchange_order_id}"
            ))),
            count => Err(Status::failed_precondition(format!(
                "REST fill order identity is ambiguous: credential={credential_id} \
                 symbol={symbol} order_id={exchange_order_id} matched {count} local orders"
            ))),
        }
    }

    pub(crate) async fn handle_rest_user_trade(
        &self,
        credential_id: &str,
        trade: &crate::binance_futures::BinanceUserTrade,
    ) -> Result<bool, Status> {
        let symbol = trade.symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            return Err(Status::invalid_argument(
                "REST fill trade symbol must not be empty",
            ));
        }
        let order_id = trade.order_id.map(|value| value.to_string());
        let row = match order_id.as_deref() {
            Some(order_id) => {
                self.claim_local_order_for_rest_fill(credential_id, order_id, &symbol)
                    .await?
            }
            None => None,
        };

        let engine_order_id = row
            .as_ref()
            .map(|row| row.get::<String, _>("engine_order_id"))
            .unwrap_or_else(|| {
                format!(
                    "external:{}",
                    order_id.as_deref().unwrap_or("unknown_order")
                )
            });
        let ctx = if let Some(row) = row.as_ref() {
            EventContext {
                bot_uid: row.get::<String, _>("bot_uid"),
                run_id: try_get_nullable_string(row, "run_id")
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_default(),
                cycle_id: try_get_nullable_string(row, "cycle_id")
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_default(),
                idempotency_key: row.get::<String, _>("idempotency_key"),
                client_order_id_short: try_get_nullable_string(row, "client_order_id_short")
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_default(),
                exchange: try_get_nullable_string(row, "exchange")
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_else(|| "binance".to_string()),
                market_kind: try_get_nullable_string(row, "market_kind")
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_else(|| "futures".to_string()),
                symbol_key: row.get::<String, _>("symbol_key"),
            }
        } else {
            EventContext {
                bot_uid: String::new(),
                run_id: String::new(),
                cycle_id: String::new(),
                idempotency_key: String::new(),
                client_order_id_short: String::new(),
                exchange: "binance".to_string(),
                market_kind: "futures".to_string(),
                symbol_key: String::new(),
            }
        };
        let trade_id = trade.id.to_string();
        let ts_exchange_ms = trade.time.unwrap_or_else(now_ms);
        let mut tx = self
            .store
            .pool()
            .begin()
            .await
            .map_err(|error| Status::internal(format!("db begin REST fill failed: {error}")))?;
        let inserted = insert_fill_if_new(
            &mut tx,
            self.store.kind(),
            credential_id,
            &trade_id,
            &engine_order_id,
            order_id.as_deref(),
            (!ctx.client_order_id_short.is_empty()).then_some(ctx.client_order_id_short.as_str()),
            "binance",
            "futures",
            (!ctx.symbol_key.is_empty()).then_some(ctx.symbol_key.as_str()),
            Some(&symbol),
            trade.side.as_deref(),
            trade.qty.as_deref(),
            trade.price.as_deref(),
            trade.commission.as_deref(),
            trade.commission_asset.as_deref(),
            trade.realized_pnl.as_deref(),
            Some(ts_exchange_ms),
            Some(ts_exchange_ms),
            Some(
                serde_json::json!({"source":"binance_rest_backfill","trade":trade})
                    .to_string()
                    .into_bytes(),
            ),
        )
        .await?;
        if inserted {
            let fill_event_id = sha256_hex(&format!(
                "fill|{}|{}|{}",
                credential_id,
                order_id.as_deref().unwrap_or("unknown_order"),
                trade.id
            ));
            let intent_id = row
                .as_ref()
                .map(|row| row.get::<String, _>("intent_id"))
                .unwrap_or_default();
            let risk_verdict_id = row
                .as_ref()
                .map(|row| row.get::<String, _>("risk_verdict_id"))
                .unwrap_or_default();
            let position_id = row
                .as_ref()
                .map(|row| row.get::<String, _>("position_id"))
                .unwrap_or_default();
            let order_role = row
                .as_ref()
                .map(|row| row.get::<String, _>("order_role"))
                .unwrap_or_default();
            let mut payload = serde_json::json!({
                "runMode": "live",
                "paper": false,
                "source": "binance_rest_backfill",
                "exchangeTradeId": trade.id,
                "exchange_trade_id": trade.id,
                "tradeId": trade.id,
                "tsExchangeMs": ts_exchange_ms,
                "ts_exchange_ms": ts_exchange_ms,
                "fee": trade.commission,
                "feeAsset": trade.commission_asset,
                "fillQty": trade.qty,
                "fillPrice": trade.price,
                "realizedPnl": trade.realized_pnl,
                "intentId": intent_id,
                "riskVerdictId": risk_verdict_id,
                "positionId": position_id,
                "orderRole": order_role,
                "trade": trade,
            });
            if let Some(row) = row.as_ref() {
                // Match the WS producer: opening/closing intent and position
                // direction come from the uniquely claimed local order. The
                // exchange BUY/SELL side cannot identify either on its own.
                payload["action"] = serde_json::json!(row.get::<String, _>("action"));
                payload["side"] = serde_json::json!(row.get::<String, _>("side"));
            }
            emit_event(
                &mut tx,
                self.store.kind(),
                credential_id,
                "fill",
                &ctx,
                &engine_order_id,
                order_id.as_deref(),
                Some(payload),
                Some(fill_event_id),
            )
            .await?;
        }
        tx.commit()
            .await
            .map_err(|error| Status::internal(format!("db commit REST fill failed: {error}")))?;
        Ok(inserted)
    }

    /// **三类 frame 同一条纪律。**
    ///
    /// R11 只把 `ORDER_TRADE_UPDATE` 的失败上抛，`ALGO_UPDATE` 与
    /// `ACCOUNT_UPDATE` 仍返回 `()`（R11R §7.6 如实登记为批外项）。
    /// 于是保护单的交易所真值、仓位/余额的交易所真值落库失败之后，
    /// reader 照读下一帧、`ready` 仍是 1、新开仓照常放行——本地与交易所
    /// 已经脱节的正是**保护**与**仓位**这两类事实。
    ///
    /// 现在三类 frame 走同一条路：失败 → reader 失败返回 → `ready=0` →
    /// 外层重连按 `ConnectWebSocket → BackfillRest → Reconcile → DeclareReady`
    /// 把这一帧的真值补回来。
    async fn handle_algo_update(
        &self,
        credential_id: &str,
        env: &BinanceWsEnvelope,
        algo: &BinanceAlgoUpdate,
    ) -> Result<(), WsApplyError> {
        let observed_at_ms = env.transaction_time_ms.or(env.event_time_ms);
        let ts = now_ms();
        let truth = protection::AlgoOrderTruth {
            algo_id: algo.algo_id,
            client_algo_id: algo.client_algo_id.clone(),
            algo_status: algo.algo_status.clone(),
            actual_order_id: algo
                .actual_order_id
                .as_ref()
                .and_then(|value| value.normalized()),
            updated_at_ms: observed_at_ms,
        };
        if let Err(error) = protection::apply_algo_update_by_identity(
            &self.store,
            credential_id,
            &truth,
            protection::AlgoTruthSource::WsAlgoUpdate,
            "user_data_stream",
            ts,
        )
        .await
        {
            tracing::error!(credential_id, %error, "algo protection update failed");
            return Err(WsApplyError::new(format!(
                "WS algo update could not be applied: {error}"
            )));
        }
        Ok(())
    }

    async fn handle_account_update(
        &self,
        credential_id: &str,
        env: &BinanceWsEnvelope,
    ) -> Result<(), WsApplyError> {
        let ts = env
            .transaction_time_ms
            .or(env.event_time_ms)
            .unwrap_or_else(now_ms);
        let eid = sha256_hex(&format!("account_update|{credential_id}|{ts}"));
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

        let mut tx = self.store.pool().begin().await.map_err(|error| {
            tracing::error!(credential_id, %error, "account update could not open a transaction");
            WsApplyError::new(format!(
                "WS account update could not begin its transaction: {error}"
            ))
        })?;
        if let Some(positions) = env
            .account
            .as_ref()
            .and_then(|account| account.get("P"))
            .and_then(|positions| positions.as_array())
        {
            for position in positions {
                let symbol = position
                    .get("s")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let qty = position
                    .get("pa")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                if symbol.is_empty() || qty.is_empty() {
                    continue;
                }
                if let Err(error) = position_state::apply_exchange_position_truth(
                    &mut tx,
                    credential_id,
                    symbol,
                    position.get("ps").and_then(|value| value.as_str()),
                    qty,
                    position.get("ep").and_then(|value| value.as_str()),
                    ts,
                )
                .await
                {
                    tracing::error!(credential_id, %error, "account position truth update failed");
                    let _ = tx.rollback().await;
                    return Err(WsApplyError::new(format!(
                        "WS account position truth could not be applied: {error}"
                    )));
                }
            }
        }
        if let Err(error) = emit_event(
            &mut tx,
            self.store.kind(),
            credential_id,
            "account_update",
            &ctx,
            "",
            None,
            Some(serde_json::json!({"paper":false,"source":"binance_ws","eventTimeMs":env.event_time_ms,"transactionTimeMs":env.transaction_time_ms,"account":env.account.clone()})),
            Some(eid),
        )
        .await
        {
            tracing::error!(credential_id, %error, "account update audit event failed");
            let _ = tx.rollback().await;
            return Err(WsApplyError::new(format!(
                "WS account update audit event could not be written: {error}"
            )));
        }
        tx.commit().await.map_err(|error| {
            tracing::error!(credential_id, %error, "account update could not be committed; nothing from this frame was applied");
            WsApplyError::new(format!(
                "WS account update could not be committed: {error}"
            ))
        })?;
        Ok(())
    }

    async fn handle_order_trade_update(
        &self,
        credential_id: &str,
        env: &BinanceWsEnvelope,
        order: &BinanceOrderTradeUpdate,
    ) -> Result<(), WsApplyError> {
        let order_id = order.order_id.to_string();
        let client_order_id = order.client_order_id.trim().to_string();
        let upstream_symbol = order.symbol.trim().to_string();

        // **候选查询必须先选对 symbol。**
        //
        // R9 的候选查询只按 credential + (exchange_order_id | client_order_id)
        // 取**最新的一行**，symbol 留到后面再后验。可 Binance 的 orderId 只在
        // symbol 内唯一、clientOrderId 更是调用方自选：同一个 id 下如果存在
        // 一条**更新的、属于另一个 symbol** 的本地行，`ORDER BY created_at_ms
        // DESC LIMIT 1` 会先选中它，后验发现 symbol 不符就整条丢弃——
        // 真正该被更新的那张单**永远收不到这条更新**。
        // 先选错再拒绝 ≠ 选对：后验只能防止写错，防不了漏写。
        //
        // **归属不得由排序决定。**
        //
        // R10 把 symbol 放进了 WHERE，但选择仍是 `OR` + `ORDER BY
        // created_at_ms DESC LIMIT 1`。评审构造的反例：同 credential/symbol 下
        // 旧行 A 只匹配 `exchange_order_id`、新行 B 只匹配 `client_order_id`，
        // 一条同时带两者的 frame 命中两行——`DESC` 选 B、`ASC` 选 A。
        // **改一下排序就改变了真实归属**，而排序不是身份。
        //
        // 现在分两步，两步都不含排序：
        // 1. 用**完整交易身份**（`usdm_symbol_key_matches`：venue/market/
        //    symbol/settle 四段，与 reconcile 同一个判据）筛出这条 frame 的
        //    候选行；
        // 2. 用 `adjudicate_order_identity` 按「exchange_order_id 权威、
        //    client_order_id 回落、同一身份多命中不猜」裁决归属。
        if upstream_symbol.is_empty() {
            tracing::error!(
                credential_id,
                exchange_order_id = %order_id,
                "WS order update carries no symbol; it cannot be bound to a local order"
            );
            return Ok(());
        }
        // 候选集合上界。命中判据是两个 id 的精确相等，正常最多一两行；
        // 触到上界说明本地 id 复用到了不可裁决的程度，**不猜**。
        const CANDIDATE_LIMIT: usize = 64;
        let select_sql = format!(
            r#"
            SELECT eo.engine_order_id, eo.bot_uid, eo.run_id, eo.cycle_id,
                   eo.idempotency_key, eo.client_order_id_short, eo.exchange_order_id,
                   eo.exchange, eo.market_kind, eo.symbol_key, eo.status,
                   eo.action, eo.side, eo.order_type, eo.price, eo.quantity,
                   COALESCE(rv.intent_id, '') AS intent_id,
                   COALESCE(eo.risk_verdict_id, '') AS risk_verdict_id,
                   COALESCE(eo.position_id, '') AS position_id,
                   COALESCE(eo.order_role, '') AS order_role
            FROM engine_orders eo
            LEFT JOIN risk_verdicts rv
              ON rv.risk_verdict_id = eo.risk_verdict_id
            WHERE eo.credential_id = ?
              AND (
                eo.exchange_order_id = ?
                OR (eo.client_order_id_short IS NOT NULL AND eo.client_order_id_short = ?)
              )
            LIMIT {CANDIDATE_LIMIT}
            "#
        );
        let candidate_rows = sqlx::query(&select_sql)
            .bind(credential_id)
            .bind(&order_id)
            .bind(&client_order_id)
            .fetch_all(self.store.pool())
            .await;
        // **查询失败 ≠ 没有候选**（engine safety contract）。R10 这里是 `return;`：
        // 一次 DB 故障与"这条 frame 不属于本地任何一张单"产出同一个结果，
        // 而后者会让这条交易所真值被永久丢弃。现在上抛，由 reader 降 readiness。
        let mut candidate_rows = match candidate_rows {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(credential_id, %error, "WS order update candidate lookup failed");
                return Err(WsApplyError::new(format!(
                    "WS order update candidate lookup failed: {error}"
                )));
            }
        };
        if candidate_rows.len() >= CANDIDATE_LIMIT {
            tracing::error!(
                credential_id,
                exchange_order_id = %order_id,
                candidates = candidate_rows.len(),
                "WS order update matched more local orders than the engine will adjudicate; \
                 refusing to guess an owner"
            );
            return Ok(());
        }
        // **WS 侧同样要核对两个身份来源。**
        //
        // R14 只把双源判据接进了 REST 回补认领（`claim_local_order_for_rest_fill`）
        // 与恢复域发现（`discover_fill_recovery_scopes`）。WS 这条通路仍然只看
        // 内层 `symbol_key`：一条 `exchange='okx'` / `market_kind='spot'` 而
        // `symbol_key='binance|futures|BTC|USDT|USDT'` 的矛盾行，照样会被这条
        // Binance USDⓈ-M 的 `ORDER_TRADE_UPDATE` 认领——三条路径消费的是**同一份**
        // 交易所真值，判据却少了一处。
        //
        // 判据、处置都与 REST 侧逐字同一份：矛盾行不是"另一个市场的单"可以静默
        // 跳过——两个来源里必有一个是错的，本地在这条 id 上已经无法自证归属。
        // **整帧 fail-closed**：`Err(WsApplyError)` 由 reader 落成
        // `record_recovery_failure`（`ready=0`）并驱动重连恢复，而不是
        // `Ok(())`（那等于把一条交易所真值永久丢掉，engine safety contract）。
        for row in &candidate_rows {
            let symbol_key = try_get_nullable_string(row, "symbol_key")
                .ok()
                .flatten()
                .unwrap_or_default();
            let exchange = try_get_nullable_string(row, "exchange")
                .ok()
                .flatten()
                .unwrap_or_default();
            let market_kind = try_get_nullable_string(row, "market_kind")
                .ok()
                .flatten()
                .unwrap_or_default();
            if let Err(reason) = crate::binance_futures::trading_identity_sources_agree(
                &symbol_key,
                &exchange,
                &market_kind,
            ) {
                let engine_order_id = try_get_nullable_string(row, "engine_order_id")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                tracing::error!(
                    credential_id,
                    exchange_order_id = %order_id,
                    client_order_id = %client_order_id,
                    upstream_symbol = %upstream_symbol,
                    %engine_order_id,
                    %reason,
                    "WS order update candidate carries two disagreeing trading identities; \
                     this exchange truth must not be claimed by any local order"
                );
                return Err(WsApplyError::new(format!(
                    "WS order update candidate identity is inconsistent: \
                     credential={credential_id} symbol={upstream_symbol} \
                     order_id={order_id}: {reason}"
                )));
            }
        }
        // 完整交易身份：venue/market/symbol/settle 四段齐全才是这条 frame 的候选。
        candidate_rows.retain(|row| {
            let symbol_key = try_get_nullable_string(row, "symbol_key")
                .ok()
                .flatten()
                .unwrap_or_default();
            crate::binance_futures::usdm_symbol_key_matches(&symbol_key, &upstream_symbol)
        });
        let candidates: Vec<OrderIdentityCandidate> = candidate_rows
            .iter()
            .map(|row| {
                let row_exchange_order_id = try_get_nullable_string(row, "exchange_order_id")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                let row_client_order_id = try_get_nullable_string(row, "client_order_id_short")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                OrderIdentityCandidate {
                    matches_exchange_order_id: !order_id.trim().is_empty()
                        && row_exchange_order_id.trim() == order_id.trim(),
                    matches_client_order_id: !client_order_id.is_empty()
                        && row_client_order_id.trim() == client_order_id,
                }
            })
            .collect();
        let adjudication = adjudicate_order_identity(&candidates);
        if adjudication.split {
            tracing::error!(
                credential_id,
                exchange_order_id = %order_id,
                client_order_id = %client_order_id,
                upstream_symbol = %upstream_symbol,
                "WS order update matched one local order by exchange order id and a \
                 different one by client order id; binding it to the exchange-assigned \
                 identity and flagging the local id reuse"
            );
        }
        if adjudication.ambiguous {
            tracing::error!(
                credential_id,
                exchange_order_id = %order_id,
                client_order_id = %client_order_id,
                upstream_symbol = %upstream_symbol,
                "WS order update matched several local orders under one order identity; \
                 refusing to guess which one it belongs to"
            );
            return Ok(());
        }
        let Some(chosen) = adjudication.chosen else {
            return Ok(());
        };
        let row = candidate_rows.remove(chosen);

        let (
            existing_exchange_order_id,
            requested_price,
            run_id,
            cycle_id,
            client_order_id_short,
            exchange,
            market_kind,
            symbol_key,
        ) = match (|| -> Result<_, sqlx::Error> {
            Ok((
                try_get_nullable_string(&row, "exchange_order_id")?.unwrap_or_default(),
                try_get_nullable_string(&row, "price")?,
                try_get_nullable_string(&row, "run_id")?.unwrap_or_default(),
                try_get_nullable_string(&row, "cycle_id")?.unwrap_or_default(),
                try_get_nullable_string(&row, "client_order_id_short")?.unwrap_or_default(),
                try_get_nullable_string(&row, "exchange")?.unwrap_or_else(|| "binance".to_string()),
                try_get_nullable_string(&row, "market_kind")?
                    .unwrap_or_else(|| "futures".to_string()),
                try_get_nullable_string(&row, "symbol_key")?.unwrap_or_default(),
            ))
        })() {
            Ok(values) => values,
            Err(error) => {
                tracing::error!(
                    credential_id,
                    %error,
                    "order update optional column decode failed"
                );
                return Err(WsApplyError::new(format!(
                    "order update optional column decode failed: {error}"
                )));
            }
        };
        let engine_order_id: String = row.get("engine_order_id");
        let current_status: String = row.get("status");
        let action: String = row.get("action");
        let side: String = row.get("side");
        let order_type: String = row.get("order_type");
        let requested_qty: String = row.get("quantity");
        let intent_id: String = row.get("intent_id");
        let risk_verdict_id: String = row.get("risk_verdict_id");
        let position_id: String = row.get("position_id");
        let order_role: String = row.get("order_role");
        let desired_status = binance_order_status_to_engine_status(order.order_status.as_str());

        // **普通订单更新必须绑定
        // symbol 身份。**
        //
        // 上面那条 SELECT 只按 credential + (exchange_order_id | client_order_id)
        // 取本地行，**不校验 `order.symbol`**。Binance 的 orderId 只在
        // symbol 内唯一，clientOrderId 更是调用方自选：一条同 id 但属于
        // 另一个 symbol 的迟到 `ORDER_TRADE_UPDATE`，会被当成本地这张单的
        // 更新写回去（评审实跑 1 条：本地 BTC 单已 `filled`，注入 symbol=ETH
        // 的迟到 `NEW`，本地状态被改成 `acked`）。
        //
        // 处置：不匹配 → 告警并**整条丢弃**，一个字节都不写回。
        // REST 侧（`handle_rest_user_trade`）早已是这条纪律，WS 侧此前缺了。
        // 后验与选择判据**同一个函数**（`usdm_symbol_key_matches`）。
        // 两处各写各的比较正是身份分叉的入口。
        if !crate::binance_futures::usdm_symbol_key_matches(&symbol_key, &upstream_symbol) {
            tracing::error!(
                credential_id,
                engine_order_id = %engine_order_id,
                local_symbol_key = %symbol_key,
                upstream_symbol = %upstream_symbol,
                exchange_order_id = %order_id,
                "WS order update symbol does not match the local order; refusing to \
                 apply it (exchange order ids are only unique within a symbol)"
            );
            return Ok(());
        }

        let ts = env
            .transaction_time_ms
            .or(env.event_time_ms)
            .unwrap_or_else(now_ms);

        let ctx = EventContext {
            bot_uid: row.get("bot_uid"),
            run_id,
            cycle_id,
            idempotency_key: row.get("idempotency_key"),
            client_order_id_short,
            exchange,
            market_kind,
            symbol_key,
        };

        let mut tx = match self.store.pool().begin().await {
            Ok(v) => v,
            Err(error) => {
                tracing::error!(credential_id, %error, "WS order update could not open a transaction");
                return Err(WsApplyError::new(format!(
                    "WS order update could not open a transaction: {error}"
                )));
            }
        };

        // **写失败不得被吞。**
        //
        // 这个函数此前每一步写都是 `let _ = ...`：状态 UPDATE 的触发器失败之后
        // **照样**写 `order_update` 审计事件、照样 `insert_fill_if_new`、
        // 照样 commit（评审实跑红）。于是库里留下"这张单被更新过"的审计事实，
        // 而状态根本没动——审计与状态互相矛盾，且没有任何地方报错。
        //
        // 现在：**同一个事务里的任何一步写失败，整个事务回滚**，一个字节都不
        // 落库，并且带理由 `error!`。宁可这条 WS 更新整条丢失
        //（reconcile 会按交易所真值把它补回来），也不要一份自相矛盾的记录。
        macro_rules! in_tx {
            ($result:expr, $what:literal) => {
                match $result {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::error!(
                            credential_id,
                            engine_order_id = %engine_order_id,
                            %error,
                            concat!(
                                "WS order update failed while writing ",
                                $what,
                                "; rolling back the whole update instead of committing a \
                                 partially applied one"
                            )
                        );
                        let _ = tx.rollback().await;
                        return Err(WsApplyError::new(format!(
                            concat!("WS order update failed while writing ", $what, ": {}"),
                            error
                        )));
                    }
                }
            };
        }

        // Update order status (idempotent).
        //
        // **终态单调。**
        //
        // 这条写回此前没有任何状态谓词：一条迟到的 `NEW` 能把已经是 `filled`
        // 的本地行改回 `acked`——**用更早的观测盖掉更强的真值**，
        // 而 `acked` 又在 reconcile 的 inflight 重查集里，等于把一张已成交的
        // 单放回处理面。WS 的送达顺序本来就不保证，这不是理论风险。
        //
        // **只保护终态是不够的。**
        //
        // R9 的谓词是"当前状态不是终态"，于是**进行态之间**照样可以倒退：
        // 一条迟到的 `NEW`（`acked`，rank 3）把 `partial_filled`（rank 4）
        // 退回去（评审实跑红）——已经发生的成交在状态上消失，
        // 而 `acked` 又在 reconcile 的 inflight 重查集里。
        //
        // 现在用**完整偏序**（`order_status_progress_rank`）：只有严格更弱的
        // 当前状态才允许被覆盖，同级与更强一律不动。谓词仍写在 SQL 的
        // `status IN (...)` 里（不是先读后写的 TOCTOU），允许集合由
        // `statuses_weaker_than` 从闭世界枚举生成——认不出的当前状态
        // 不在允许集合里，因此 fail-closed 地不被覆盖。
        //
        // 唯一还允许发生的写是**补一个缺失的交易所身份**——它不改状态，
        // 只让一张单（含终态单）重新可被 id 追踪。
        // 本帧写完之后这一行**实际**处于什么状态。默认是读到的当前状态，
        // 只有状态 UPDATE 真的影响了行才推进。终态冲突守卫按它判定。
        let mut effective_status = current_status.clone();
        if let Some(next) = desired_status {
            if next != current_status {
                let weaker = statuses_weaker_than(next);
                if weaker.contains(&current_status.trim().to_ascii_lowercase().as_str()) {
                    let placeholders = std::iter::repeat_n("?", weaker.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    let update_sql = format!(
                        r#"
                        UPDATE engine_orders
                        SET status = ?, exchange_order_id = ?, updated_at_ms = ?
                        WHERE credential_id = ? AND engine_order_id = ?
                          AND status IN ({placeholders})
                        "#
                    );
                    let mut update = sqlx::query(&update_sql)
                        .bind(next)
                        .bind(&order_id)
                        .bind(now_ms())
                        .bind(credential_id)
                        .bind(&engine_order_id);
                    for status in &weaker {
                        update = update.bind(*status);
                    }
                    let applied = in_tx!(update.execute(&mut *tx).await, "the order status");
                    if applied.rows_affected() == 0 {
                        tracing::warn!(
                            credential_id,
                            engine_order_id = %engine_order_id,
                            current_status = %current_status,
                            next_status = %next,
                            "another path advanced this order between the read and the write; \
                             keeping the status it now carries"
                        );
                    } else {
                        effective_status = next.to_string();
                    }
                } else {
                    tracing::warn!(
                        credential_id,
                        engine_order_id = %engine_order_id,
                        current_status = %current_status,
                        rejected_status = %next,
                        "late WS order update would regress the order status; keeping the \
                         stronger truth"
                    );
                }
            }
            if existing_exchange_order_id.trim().is_empty() {
                in_tx!(
                    sqlx::query(
                        r#"
                        UPDATE engine_orders
                        SET exchange_order_id = ?, updated_at_ms = ?
                        WHERE credential_id = ? AND engine_order_id = ?
                          AND (exchange_order_id IS NULL OR exchange_order_id = '')
                        "#,
                    )
                    .bind(&order_id)
                    .bind(now_ms())
                    .bind(credential_id)
                    .bind(&engine_order_id)
                    .execute(&mut *tx)
                    .await,
                    "the missing exchange order id"
                );
            }
        }

        // Emit order_update.
        let order_update_eid = sha256_hex(&format!(
            "order_update|{}|{}|{}|{}|{}",
            credential_id, order_id, order.execution_type, order.order_status, ts
        ));
        in_tx!(
            emit_event(
                &mut tx,
                self.store.kind(),
                credential_id,
                "order_update",
                &ctx,
                &engine_order_id,
                Some(order_id.as_str()),
                Some(serde_json::json!({
                    "runMode": "live",
                    "paper": false,
                    "source": "binance_ws",
                    "action": action.clone(),
                    "side": side.clone(),
                    "orderType": order_type.clone(),
                    "requestedQty": requested_qty.clone(),
                    "requestedPrice": requested_price.clone(),
                    "eventTimeMs": env.event_time_ms,
                    "transactionTimeMs": env.transaction_time_ms,
                    "order": order,
                })),
                Some(order_update_eid),
            )
            .await,
            "the order_update audit event"
        );

        // Emit fill (dedupe by tradeId).
        if order.execution_type.trim().eq_ignore_ascii_case("TRADE") {
            if let Some(trade_id) = order.trade_id {
                // **状态与账务不得互相矛盾。**
                //
                // R10 让所有终态同 rank，于是一条本地 `failed` 挡住了后来真实的
                // `FILLED`——**而这一段照样把 fill 行与敞口写了下去**。库里因此
                // 留下「一张 failed 的单 + 一笔成交 + 一份敞口」。
                //
                // 权威度重定义（`terminal_authority_rank`）已经让 `FILLED` 能改写
                // `failed`/`rejected`，所以这条守卫在正常路径上不会触发；它守的是
                // **剩下的那条缝**：状态推进被并发抢先（`rows_affected()==0`）、
                // 或交易所状态本引擎认不出（`desired_status == None`）时，
                // 这一帧仍然带着一笔成交。那种情况下宁可整帧回滚——
                // reconcile 会按交易所真值把它补回来——也不落一份自相矛盾的账。
                if terminal_denies_execution(&effective_status) {
                    tracing::error!(
                        credential_id,
                        engine_order_id = %engine_order_id,
                        effective_status = %effective_status,
                        exchange_order_status = %order.order_status,
                        exchange_trade_id = trade_id,
                        "WS frame carries an executed trade for an order whose persisted \
                         status denies execution; rolling back rather than booking a fill \
                         under a contradictory status"
                    );
                    let _ = tx.rollback().await;
                    return Err(WsApplyError::new(format!(
                        "executed trade {trade_id} conflicts with persisted status \
                         {effective_status}"
                    )));
                }
                let exchange_trade_id = trade_id.to_string();
                let ts_exchange_ms = order
                    .trade_time_ms
                    .or(env.transaction_time_ms)
                    .or(env.event_time_ms)
                    .unwrap_or(ts);
                in_tx!(
                    insert_fill_if_new(
                        &mut tx,
                        self.store.kind(),
                        credential_id,
                        exchange_trade_id.as_str(),
                        engine_order_id.as_str(),
                        Some(order_id.as_str()),
                        Some(ctx.client_order_id_short.as_str()),
                        ctx.exchange.as_str(),
                        ctx.market_kind.as_str(),
                        Some(ctx.symbol_key.as_str()),
                        Some(order.symbol.as_str()),
                        Some(side.as_str()),
                        order.last_filled_qty.as_deref(),
                        order.last_filled_price.as_deref(),
                        order.commission.as_deref(),
                        order.commission_asset.as_deref(),
                        order.realized_pnl.as_deref(),
                        Some(ts_exchange_ms),
                        Some(ts),
                        Some(
                            serde_json::json!({
                                "source": "binance_ws",
                                "order": order,
                                "transactionTimeMs": env.transaction_time_ms,
                                "eventTimeMs": env.event_time_ms,
                            })
                            .to_string()
                            .into_bytes(),
                        ),
                    )
                    .await,
                    "the fill row"
                );
                if order.order_status.trim().eq_ignore_ascii_case("FILLED") {
                    in_tx!(
                        position_state::apply_filled_order_truth(
                            &mut tx,
                            credential_id,
                            &ctx.bot_uid,
                            &ctx.symbol_key,
                            &action,
                            order
                                .cumulative_filled_qty
                                .as_deref()
                                .or(order.last_filled_qty.as_deref())
                                .unwrap_or(""),
                            order.last_filled_price.as_deref(),
                            ts_exchange_ms,
                        )
                        .await,
                        "the position truth"
                    );
                }
                let fill_eid =
                    sha256_hex(&format!("fill|{}|{}|{}", credential_id, order_id, trade_id));
                in_tx!(
                    emit_event(
                        &mut tx,
                        self.store.kind(),
                        credential_id,
                        "fill",
                        &ctx,
                        &engine_order_id,
                        Some(order_id.as_str()),
                        Some(serde_json::json!({
                            "runMode": "live",
                            "paper": false,
                            "source": "binance_ws",
                            "action": action.clone(),
                            "side": side.clone(),
                            "orderType": order_type.clone(),
                            "requestedQty": requested_qty.clone(),
                            "requestedPrice": requested_price.clone(),
                            "eventTimeMs": env.event_time_ms,
                            "transactionTimeMs": env.transaction_time_ms,
                            "orderStatus": order.order_status,
                            "order_status": order.order_status,
                            "exchangeTradeId": trade_id,
                            "exchange_trade_id": trade_id,
                            "tradeId": trade_id,
                            "tsExchangeMs": ts_exchange_ms,
                            "ts_exchange_ms": ts_exchange_ms,
                            "fee": order.commission,
                            "feeAsset": order.commission_asset,
                            "fillQty": order.last_filled_qty,
                            "fillPrice": order.last_filled_price,
                            "cumulativeFilledQty": order.cumulative_filled_qty,
                            "realizedPnl": order.realized_pnl,
                            "intentId": intent_id,
                            "riskVerdictId": risk_verdict_id,
                            "positionId": position_id,
                            "orderRole": order_role,
                            "order": order,
                        })),
                        Some(fill_eid),
                    )
                    .await,
                    "the fill audit event"
                );
            }
        }

        // **entry fill 之后保护计划这一步
        // 失败，不能只写一行日志。**
        //
        // `docs/SAFETY_MODEL.md` §2 的既定安全模型是：「保护提交失败或
        // `PROTECTION_CONFIRM_TIMEOUT_MS` 到期仍未确认时，走既有 reduce-only
        // flatten 并自动锁存该 bot 的 `pause_new`」，engine safety contract 更直接——
        // **SL 未经交易所真值确认则立即 reduce-only flatten**。
        //
        // R10 这里两处都只 `error!`：查询失败 `.ok()` 折成 `None`（于是连
        // "有没有仓在等保护"都不知道就当没有），提交失败也只记日志——
        // 结果是**一个已经成交的入场敞口裸着**，而没有任何机制会去平它。
        let awaiting_protection = if order.execution_type.trim().eq_ignore_ascii_case("TRADE")
            && order.order_status.trim().eq_ignore_ascii_case("FILLED")
            && matches!(action.as_str(), "open_long" | "open_short")
        {
            // **缺 plan 是要处置的事实。**
            //
            // R11 这里是 `position_state ps JOIN position_protection_plans pp`
            //（INNER JOIN）：一条**有仓位、没有保护计划**的行不产出任何结果行，
            // 于是整件事被折成 `Known(None)`——"已知没有仓在等保护"，处置路径
            // 一步都不走。可"入场已成交、连保护计划都没有"恰恰是最该被处置的
            // 形态（engine safety contract：SL 未经交易所真值确认则立即 reduce-only flatten）。
            //
            // 改成 LEFT JOIN 只为保持"计划优先"的排序语义；判据变成**仓位在不在**。
            // 没有 plan 的仓位照样进 `Known(Some(position_id))`，
            // `submit_planned_protection` 会以
            // `active position has no typed protection plan` 失败，
            // 从而落进 flatten + pause 的处置路径。
            match sqlx::query(
                r#"
                SELECT ps.position_id
                FROM position_state ps
                LEFT JOIN position_protection_plans pp ON pp.position_id=ps.position_id
                WHERE ps.credential_id=? AND ps.bot_uid=? AND ps.symbol_key=?
                  AND ps.state IN ('open','protected')
                ORDER BY ps.entry_epoch DESC LIMIT 1
                "#,
            )
            .bind(credential_id)
            .bind(&ctx.bot_uid)
            .bind(&ctx.symbol_key)
            .fetch_all(&mut *tx)
            .await
            {
                Ok(rows) => AwaitingProtection::Known(
                    rows.into_iter()
                        .next()
                        .map(|row| row.get::<String, _>("position_id")),
                ),
                Err(error) => {
                    // 读失败不改变已写下的一致状态，因此不回滚；但**不得**折成
                    // "没有仓在等保护"（engine safety contract）——那正是让裸仓静默的那一步。
                    tracing::error!(
                        credential_id,
                        engine_order_id = %engine_order_id,
                        %error,
                        "WS fill could not look up the position awaiting typed protection"
                    );
                    AwaitingProtection::Unknown
                }
            }
        } else {
            AwaitingProtection::Known(None)
        };

        if let Err(error) = tx.commit().await {
            tracing::error!(
                credential_id,
                engine_order_id = %engine_order_id,
                %error,
                "WS order update could not be committed; nothing from this frame was applied"
            );
            return Err(WsApplyError::new(format!(
                "WS order update could not be committed: {error}"
            )));
        }
        match awaiting_protection {
            AwaitingProtection::Known(None) => {}
            AwaitingProtection::Unknown => {
                self.flatten_and_pause_after_entry_fill(
                    credential_id,
                    &ctx.bot_uid,
                    "entry fill could not determine whether a position awaits typed protection",
                )
                .await?;
            }
            AwaitingProtection::Known(Some(position_id)) => {
                let service = EngineExecutionService::for_fill_protection(self.store.clone());
                let protection_run_mode = protection_run_mode_for_credential(credential_id);
                // **两种失败形态都要接住**：`submit_planned_protection` 既可能
                // 返回 `Err`（查询/落库失败），也可能返回 `Ok(acks)` 而其中某条
                // ack 是 `error` / `rejected`（提交被交易所或闸门挡下）。
                // 只看 `Err` 的话，后一种——也是更常见的那一种——会被当成成功。
                let failure = match service
                    .submit_planned_protection(&position_id, protection_run_mode)
                    .await
                {
                    Err(error) => Some(error.to_string()),
                    Ok(acks) => acks
                        .iter()
                        .find(|ack| matches!(ack.status.as_str(), "error" | "rejected"))
                        .map(|ack| format!("protection ack {}: {}", ack.status, ack.message)),
                };
                if let Some(failure) = failure {
                    tracing::error!(credential_id, %position_id, %failure, "attach typed protection after entry fill failed");
                    self.flatten_and_pause_after_entry_fill(
                        credential_id,
                        &ctx.bot_uid,
                        &format!(
                            "typed protection could not be attached after an entry fill on \
                             position {position_id}: {failure}"
                        ),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// **入场成交后保护没挂上 → 持久 `flatten` latch + not-ready，直到交易所
    /// 真值证明仓位归零。**
    ///
    /// 建立了这条处置；
    /// **收紧了它的完成判据。**
    ///
    /// R12 的形状是：`flatten_and_pause_bot` 返回 `Ok(())` 就直接
    /// `Ok(())`——这一帧算成功，`ready` 保持 1，新开仓照常放行。可那个
    /// `Ok(())` 只代表 **place-order 请求被受理**（live adapter 下就是
    /// `submit_live_flatten` 下完 market order 就返回），既没有仓位归零的
    /// 交易所真值，也没有任何可重试的持久处置。「flatten 请求被受理但仓位
    /// 还在」这种最常见的形态，会被整条链记成健康。
    ///
    /// 现在**发现敞口就闩住**：无论这一次 flatten 请求成功与否，都走
    /// `latch_unprotected_entry_fill`——持久 `flatten` latch（未收敛）
    /// + fill stream not-ready + 这一帧失败上抛。收敛只有一条路：
    ///   `kill_switch::spawn_action_retry_task` 撤单/平仓/**对账**之后
    ///   `ReconcileTruth::flatten_complete()` 为真，latch 才写成 `flattened`。
    ///
    /// flatten 需要交易所凭据；拿不到凭据（模拟凭据、未配置 venue）时
    /// 如实把它记成处置失败，不假装平过。
    async fn flatten_and_pause_after_entry_fill(
        &self,
        credential_id: &str,
        bot_uid: &str,
        reason: &str,
    ) -> Result<(), WsApplyError> {
        let failure = self
            .request_reduce_only_flatten(credential_id, bot_uid, reason)
            .await
            .err()
            .map(|error| error.to_string());
        // **这一行就是「ACK 不解除 not-ready」本身**：`failure` 是 `None`
        // （交易所受理了这次 reduce-only 平仓请求）时**同样**走 latch。
        // R12 在这里是 `if failure.is_none() { return Ok(()); }`。
        self.latch_unprotected_entry_fill(credential_id, bot_uid, reason, failure.as_deref())
            .await
    }

    /// 只负责**把这一次 reduce-only flatten 请求发出去**（连同它的持久
    /// latch），不判定收敛——收敛判定在调用方，且只认交易所真值。
    ///
    /// 抽出来是为了让上面那条「ACK 不解除 not-ready」成为一条**单独可变异**
    /// 的生产语句：替身注入只影响"用哪个 adapter"，不再顺带绕过完成判定。
    async fn request_reduce_only_flatten(
        &self,
        credential_id: &str,
        bot_uid: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        #[cfg(test)]
        if let Some(adapter) = self.protection_failure_adapter.as_ref() {
            return crate::protection::flatten_and_pause_bot(
                &self.store,
                bot_uid,
                adapter.as_ref(),
                reason,
            )
            .await;
        }
        let adapter = match keys_from_env(credential_id) {
            Ok(Some(keys)) => crate::protection::BinanceProtectionAdapter::new(
                self.store.clone(),
                keys,
                credential_id,
                bot_uid,
                CredentialRateLimiter::shared(TokenBucketConfig::from_env(None)),
            )
            .map_err(|error| error.to_string()),
            Ok(None) => Err("no exchange credentials configured".to_string()),
            Err(error) => Err(error),
        };
        match adapter {
            Ok(adapter) => {
                crate::protection::flatten_and_pause_bot(&self.store, bot_uid, &adapter, reason)
                    .await
            }
            Err(unavailable) => Err(anyhow::anyhow!(unavailable)),
        }
    }

    /// **处置写失败必须持久、可重试。**
    /// **请求 ACK 不解除 not-ready。**
    ///
    /// R11 的 flatten/pause 失败只写一行 `error!`：这一帧照样 `success`，
    /// `ready` 照样是 1，进程重启之后连"曾经有过一个没保护的敞口"都不剩，
    /// 没有任何机制会再去平它。R12 把**失败**那一半接住了，
    /// R13 把**成功**那一半也接住：一次 place-order 的请求 ACK 不是仓位归零，
    /// 所以它同样走这里。`failure=None` 就是"这次请求被受理、但真值未证"。
    ///
    /// 落成两件持久的事：
    ///
    /// 1. **闩住 `bot:<uid>` 的 `flatten` 档且强制处于未收敛**
    ///    （`kill_switch::latch_flatten_pending`）。这是仓内既有的可重试处置——
    ///    `kill_switch::spawn_action_retry_task` 每秒扫
    ///    `level='flatten' AND flatten_state <> 'flattened'`，逐 credential
    ///    撤单 + reduce-only flatten + **对账**，只有
    ///    `ReconcileTruth::flatten_complete()`（交易所真值收敛）才会把它写成
    ///    `flattened`。一次请求回执永远不算数，与 `docs/SAFETY_MODEL.md`
    ///    的恢复模型同一条。`pause_new` 被 `flatten` 的严重度覆盖包含。
    ///    用 `latch_flatten_pending` 而不是裸 `arm`：上一轮已经收敛过的 scope
    ///    再出现新敞口时，`arm` 的同severity早返回会把旧的 `flattened` 原样留下。
    /// 2. **把用户流按 not-ready 记账**并让这一帧失败上抛：reader 断流重连，
    ///    走回补 → reconcile；在 flatten 收敛之前 kill-switch 闩仍然挡着新风险。
    ///
    /// latch 本身再失败就真的只剩日志了（它已经是一次纯 DB 写）——但那一帧
    /// **仍然**返回 `Err`，绝不把"没处置成"记成成功。
    async fn latch_unprotected_entry_fill(
        &self,
        credential_id: &str,
        bot_uid: &str,
        reason: &str,
        failure: Option<&str>,
    ) -> Result<(), WsApplyError> {
        let latched_reason = match failure {
            Some(failure) => format!("{reason}; disposition failed: {failure}"),
            None => format!(
                "{reason}; a reduce-only flatten was requested but no exchange truth has \
                 proven the position flat"
            ),
        };
        if let Err(error) = crate::kill_switch::latch_flatten_pending(
            &self.store,
            &format!("bot:{bot_uid}"),
            "auto_protection_timeout",
            &latched_reason,
            now_ms(),
        )
        .await
        {
            tracing::error!(credential_id, bot_uid, %error, "could not latch a retryable flatten after an unprotected entry fill");
        }
        if let Err(error) = crate::fill_replay::record_recovery_failure(
            &self.store,
            credential_id,
            "protection_disposition",
            None,
            None,
            &latched_reason,
            now_ms(),
        )
        .await
        {
            tracing::error!(credential_id, bot_uid, %error, "could not record the unprotected entry fill as a recovery failure");
        }
        tracing::error!(
            credential_id,
            bot_uid,
            failure = failure.unwrap_or("none: the flatten request was accepted"),
            "entry fill left a position without exchange-confirmed protection; a retryable \
             flatten is latched and the user stream is marked not-ready until exchange truth \
             converges"
        );
        Err(WsApplyError::new(format!(
            "entry fill protection disposition did not complete: {latched_reason}"
        )))
    }
}

/// 入场成交后「有没有仓在等保护」这件事的三种真值。
///
/// **`Unknown` 必须与 `Known(None)` 分开**——
/// R10 把查询失败 `.ok()` 折成 `None`，于是"问不到"与"没有"走同一条路，
/// 一个已经成交的敞口就此静默裸着。
enum AwaitingProtection {
    Known(Option<String>),
    Unknown,
}

#[derive(Clone)]
pub struct EngineEventsService {
    store: Store,
    poll_interval: Duration,
}

impl EngineEventsService {
    pub fn new(store: Store, poll_interval: Duration) -> Self {
        Self {
            store,
            poll_interval,
        }
    }
}

type EventStream = ReceiverStream<Result<EngineEvent, Status>>;

#[tonic::async_trait]
impl EngineEvents for EngineEventsService {
    type StreamEventsStream = EventStream;

    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let credential_id = req.credential_id;
        // **sqlx-i64 核查批· `u64 → i64` 不能回绕。**
        //
        // `req.since_seq` 是 proto 的 `uint64`。`as i64` 在 `> i64::MAX` 时
        // 回绕成负数，`WHERE seq > ?` 于是从头把**全部**历史事件重放一遍——
        // 一个越界的游标被静默翻译成"从头开始"。越界一律钉死在 `i64::MAX`：
        // 游标只会取不到新事件（fail-closed），不会变成全量回放。
        let mut since_seq = i64::try_from(req.since_seq).unwrap_or(i64::MAX);
        let pool = self.store.pool().clone();
        let poll = self.poll_interval;

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                let rows = match sqlx::query(
                    r#"
                    SELECT credential_id, seq, event_id,
                           CAST(ts_ms AS CHAR) AS ts_ms_text, event_type,
                           COALESCE(bot_uid, '') AS bot_uid,
                           COALESCE(run_id, '') AS run_id,
                           COALESCE(cycle_id, '') AS cycle_id,
                           COALESCE(idempotency_key, '') AS idempotency_key,
                           COALESCE(client_order_id_short, '') AS client_order_id_short,
                           COALESCE(exchange_order_id, '') AS exchange_order_id,
                           COALESCE(exchange, '') AS exchange,
                           COALESCE(market_kind, '') AS market_kind,
                           COALESCE(symbol_key, '') AS symbol_key,
                           COALESCE(payload_json, X'') AS payload_json
                    FROM engine_order_events
                    WHERE credential_id = ? AND seq > ?
                    ORDER BY seq ASC
                    LIMIT 500
                    "#,
                )
                .bind(&credential_id)
                .bind(since_seq)
                .fetch_all(&pool)
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("db query failed: {e}"))))
                            .await;
                        break;
                    }
                };

                if rows.is_empty() {
                    tokio::time::sleep(poll).await;
                    continue;
                }

                for row in rows {
                    let seq: i64 = row.get("seq");
                    since_seq = since_seq.max(seq);
                    // **sqlx-i64 核查批· 事件时刻走文本取真值。**
                    //
                    // 旧 sqlx 0.7.4 `Any` + SQLite 会把 `ts_ms` 截成低 32 位；
                    // 0.8.6 已修复。这条流是 control-plane 的
                    // 事件入口，截断会让**每一条**引擎事件带着一个从未存在过的
                    // 时刻进入证据链（intent→verdict→ack→fill 的逐行对账全靠它）。
                    // 解码不出来就把这条流按内部错误终止——不发一条编造时刻的事件。
                    let ts_ms = match crate::store::try_get_i64_text(&row, "ts_ms_text") {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = tx
                                .send(Err(Status::internal(format!("db decode failed: {error}"))))
                                .await;
                            return;
                        }
                    };
                    let ev = EngineEvent {
                        credential_id: row.get("credential_id"),
                        seq: seq as u64,
                        event_id: row.get("event_id"),
                        ts_ms,
                        event_type: row.get("event_type"),
                        bot_uid: row.get("bot_uid"),
                        run_id: row.get("run_id"),
                        cycle_id: row.get("cycle_id"),
                        idempotency_key: row.get("idempotency_key"),
                        client_order_id_short: row.get("client_order_id_short"),
                        exchange_order_id: row.get("exchange_order_id"),
                        exchange: row.get("exchange"),
                        market_kind: row.get("market_kind"),
                        symbol_key: row.get("symbol_key"),
                        payload_json: row.get("payload_json"),
                    };
                    if tx.send(Ok(ev)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub(crate) async fn emit_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    credential_id: &str,
    event_type: &str,
    ctx: &EventContext,
    _engine_order_id: &str,
    exchange_order_id: Option<&str>,
    payload: Option<serde_json::Value>,
    event_id_override: Option<String>,
) -> Result<(), Status> {
    let event_id = event_id_override.unwrap_or_else(|| ulid::Ulid::new().to_string());
    let ts_ms = now_ms();

    // Ensure checkpoint row exists (so we can UPDATE after seq allocation).
    match db_kind {
        DbKind::MySql => {
            sqlx::query(
                r#"
                INSERT INTO engine_checkpoints (credential_id, last_seq_emitted, updated_at_ms)
                VALUES (?, 0, ?)
                ON DUPLICATE KEY UPDATE credential_id = credential_id
                "#,
            )
            .bind(credential_id)
            .bind(ts_ms)
            .execute(&mut **tx)
            .await
            .map_err(|e| Status::internal(format!("db checkpoint ensure failed: {e}")))?;
        }
        _ => {
            sqlx::query(
                r#"
                INSERT INTO engine_checkpoints (credential_id, last_seq_emitted, updated_at_ms)
                VALUES (?, 0, ?)
                ON CONFLICT(credential_id) DO NOTHING
                "#,
            )
            .bind(credential_id)
            .bind(ts_ms)
            .execute(&mut **tx)
            .await
            .map_err(|e| Status::internal(format!("db checkpoint ensure failed: {e}")))?;
        }
    }

    // SQLite does not support `SELECT ... FOR UPDATE`. We lock the checkpoint row by doing a
    // no-op UPDATE so concurrent emitters can't read the same last_seq and "lose" events due to
    // `(credential_id, seq)` primary key conflicts.
    if db_kind != DbKind::MySql {
        sqlx::query(
            r#"
            UPDATE engine_checkpoints
            SET updated_at_ms = updated_at_ms
            WHERE credential_id = ?
            "#,
        )
        .bind(credential_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db checkpoint lock failed: {e}")))?;
    }

    let row = sqlx::query(match db_kind {
        DbKind::MySql => {
            r#"
                SELECT last_seq_emitted
                FROM engine_checkpoints
                WHERE credential_id = ?
                FOR UPDATE
                "#
        }
        _ => {
            r#"
                SELECT last_seq_emitted
                FROM engine_checkpoints
                WHERE credential_id = ?
                "#
        }
    })
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db checkpoint select failed: {e}")))?
    .into_iter()
    .next();

    let last_seq: i64 = row
        .and_then(|r| r.try_get::<i64, _>("last_seq_emitted").ok())
        .unwrap_or(0);
    let next_seq = last_seq + 1;

    let inserted = sqlx::query(
        r#"
        INSERT INTO engine_order_events (
          credential_id, seq, event_id, ts_ms, event_type,
          bot_uid, run_id, cycle_id, idempotency_key, client_order_id_short,
          exchange_order_id, exchange, market_kind, symbol_key, payload_json
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?
        )
        "#,
    )
    .bind(credential_id)
    .bind(next_seq)
    .bind(&event_id)
    .bind(ts_ms)
    .bind(event_type)
    .bind(&ctx.bot_uid)
    .bind(&ctx.run_id)
    .bind(&ctx.cycle_id)
    .bind(&ctx.idempotency_key)
    .bind(&ctx.client_order_id_short)
    .bind(exchange_order_id)
    .bind(&ctx.exchange)
    .bind(&ctx.market_kind)
    .bind(&ctx.symbol_key)
    .bind(payload.map(|v| v.to_string().into_bytes()))
    .execute(&mut **tx)
    .await;

    match inserted {
        Ok(_) => {}
        Err(e) if is_unique_violation(&e) => {
            // Could be:
            // - duplicate `(credential_id, event_id)` (true duplicate event, safe to ignore), or
            // - duplicate `(credential_id, seq)` (seq allocation bug / concurrency issue; must not drop).
            //
            // We treat only event_id duplicates as safe dedupe.
            let exists = sqlx::query(
                r#"
                SELECT 1
                FROM engine_order_events
                WHERE credential_id = ? AND event_id = ?
                LIMIT 1
                "#,
            )
            .bind(credential_id)
            .bind(&event_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|qe| Status::internal(format!("db event dedupe check failed: {qe}")))?
            .into_iter()
            .next();

            if exists.is_some() {
                return Ok(());
            }

            tracing::warn!(
                credential_id = %credential_id,
                event_id = %event_id,
                event_type = %event_type,
                error = %e,
                "engine event insert unique violation not caused by event_id (possible seq race)"
            );
            return Err(Status::internal(format!(
                "db event insert failed (unique violation; possible seq race): {e}"
            )));
        }
        Err(e) => {
            return Err(Status::internal(format!("db event insert failed: {e}")));
        }
    }

    sqlx::query(
        r#"
        UPDATE engine_checkpoints
        SET last_seq_emitted = ?, updated_at_ms = ?
        WHERE credential_id = ?
        "#,
    )
    .bind(next_seq)
    .bind(ts_ms)
    .bind(credential_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db checkpoint update failed: {e}")))?;

    Ok(())
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    let msg = e.to_string();
    if msg.contains("UNIQUE constraint failed") {
        return true;
    }
    if msg.contains("Duplicate entry") {
        return true;
    }
    false
}

pub(crate) async fn insert_fill_if_new(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    credential_id: &str,
    exchange_trade_id: &str,
    engine_order_id: &str,
    exchange_order_id: Option<&str>,
    client_order_id_short: Option<&str>,
    exchange: &str,
    market_kind: &str,
    symbol_key: Option<&str>,
    symbol: Option<&str>,
    side: Option<&str>,
    fill_qty: Option<&str>,
    fill_price: Option<&str>,
    fee: Option<&str>,
    fee_asset: Option<&str>,
    realized_pnl: Option<&str>,
    ts_exchange_ms: Option<i64>,
    ts_event_ms: Option<i64>,
    raw_json: Option<Vec<u8>>,
) -> Result<bool, Status> {
    let created_at_ms = now_ms();
    let symbol_identity = crate::fill_replay::fill_symbol_identity(symbol, symbol_key);
    let q = match db_kind {
        DbKind::MySql => {
            r#"
            INSERT IGNORE INTO engine_fills(
              credential_id, symbol_identity, symbol_scope_known, symbol_scope_proved_at_ms,
              exchange_trade_id, engine_order_id, exchange_order_id, client_order_id_short,
              exchange, market_kind, symbol_key, symbol, side,
              fill_qty, fill_price, fee, fee_asset, realized_pnl,
              ts_exchange_ms, ts_event_ms, raw_json, created_at_ms
            ) VALUES(?,?,?,?,?,?,?,?,
                     ?,?,?,?,?, ?,?,?,?,?,
                     ?,?,?,?)
            "#
        }
        _ => {
            r#"
            INSERT OR IGNORE INTO engine_fills(
              credential_id, symbol_identity, symbol_scope_known, symbol_scope_proved_at_ms,
              exchange_trade_id, engine_order_id, exchange_order_id, client_order_id_short,
              exchange, market_kind, symbol_key, symbol, side,
              fill_qty, fill_price, fee, fee_asset, realized_pnl,
              ts_exchange_ms, ts_event_ms, raw_json, created_at_ms
            ) VALUES(?,?,?,?,?,?,?,?,
                     ?,?,?,?,?, ?,?,?,?,?,
                     ?,?,?,?)
            "#
        }
    };

    let res = sqlx::query(q)
        .bind(credential_id)
        .bind(&symbol_identity.value)
        .bind(if symbol_identity.scope_known {
            1_i64
        } else {
            0_i64
        })
        // ****：写入侧的身份是**当场**用生产谓词
        // （`fill_symbol_identity` → `usdm_symbol`）判定的，所以这一行天生
        // 就是"已复核"的，无论判定结果是 known 还是 unknown。
        //
        // 这里必须落时间戳而不是留 NULL，原因不止是省一次扫描：REST 回补的
        // **外部成交**（本地没有对应 `engine_order` 的那些）没有 symbol_key，
        // 它的域身份来自**上游来源**——Binance USDⓈ-M userTrades 端点本身。
        // 启动复核看不到这层来源，只能判"证不出来"。留 NULL 等于让下一次
        // 启动把一条真实的 USDⓈ-M 成交降级掉。
        .bind(created_at_ms)
        .bind(exchange_trade_id)
        .bind(engine_order_id)
        .bind(exchange_order_id)
        .bind(client_order_id_short)
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .bind(symbol)
        .bind(side)
        .bind(fill_qty)
        .bind(fill_price)
        .bind(fee)
        .bind(fee_asset)
        .bind(realized_pnl)
        .bind(ts_exchange_ms)
        .bind(ts_event_ms)
        .bind(raw_json)
        .bind(created_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db insert engine_fills failed: {e}")))?;

    Ok(res.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Once};
    use std::time::Duration;

    use risk_policy::Decision;
    use sqlx::Row;
    use tokio::sync::Barrier;
    use tonic::Request;

    use super::{
        demo_submission_reject_reason, exchange_gateway_allowed_for, exchange_submission_route,
        is_exchange_backed_run_mode, parse_engine_live_enabled, protection_run_mode_for_venue,
        reduce_only_exit_reaches_exchange_for, stage_allowed_with, stage_allowed_with_credential,
        stage_certification_venue_is_testnet, stage_denial_applies, BinanceEventSink,
        EngineExecution, EngineExecutionService, ExchangeSubmissionRoute, StageDenial,
    };
    use crate::binance_futures::BinanceAlgoConditionalOrderType;
    use crate::position_state;
    use crate::protection::OrderRole;
    use crate::proto::{ExecuteIntentsRequest, OrderIntent};
    use crate::store::mysql_test_support::MySqlScratch;
    use crate::venue::Venue;

    #[test]
    fn b1_submission_dispatch_routes_only_typed_protection_to_algo() {
        assert_eq!(
            exchange_submission_route(OrderRole::ProtectionSl, "stop_market"),
            Ok(ExchangeSubmissionRoute::Algo(
                BinanceAlgoConditionalOrderType::StopMarket
            ))
        );
        assert_eq!(
            exchange_submission_route(OrderRole::ProtectionTp, "take_profit_market"),
            Ok(ExchangeSubmissionRoute::Algo(
                BinanceAlgoConditionalOrderType::TakeProfitMarket
            ))
        );
        assert_eq!(
            exchange_submission_route(OrderRole::Emergency, "market"),
            Ok(ExchangeSubmissionRoute::Market)
        );
    }

    #[test]
    fn live_stage_requires_switch_and_deployment_cap() {
        assert_eq!(
            stage_allowed_with("live", "live", false),
            Err(StageDenial::LiveDisabled)
        );
        assert_eq!(
            stage_allowed_with("live_small", "live_small", false),
            Err(StageDenial::LiveDisabled)
        );
        assert!(stage_allowed_with("live", "paper", true).is_err());
        assert!(stage_allowed_with("live", "live", true).is_ok());
        assert!(stage_allowed_with("paper", "paper", false).is_ok());
    }

    /// D39（WO-M6）：实盘总开关只拦新增风险。此前 LiveDisabled 对所有角色生效，
    /// live_small/live 既有仓位在总开关关闭时连 reduce-only 平仓都被拒。
    #[test]
    fn live_switch_denial_blocks_new_risk_but_not_reachable_reduce_only_exits() {
        for stage in ["live_small", "live"] {
            let denial = stage_allowed_with(stage, "demo", false)
                .expect_err("disabled live switch must reject live");
            assert_eq!(denial, StageDenial::LiveDisabled);
            // 开仓 / 增险：一律拦，可达与否都一样。
            for reachable in [true, false] {
                for action in ["open_long", "open_short"] {
                    assert!(stage_denial_applies(
                        &denial,
                        OrderRole::Entry,
                        action,
                        reachable
                    ));
                }
            }
            // 只减不增的退出（平仓 / 紧急 / 保护单）：可达交易所时不拦。
            for role in [
                OrderRole::Exit,
                OrderRole::Emergency,
                OrderRole::ProtectionSl,
                OrderRole::ProtectionTp,
            ] {
                for action in ["close_long", "close_short"] {
                    assert!(!stage_denial_applies(&denial, role, action, true));
                    // 该凭据的只减退出到不了交易所（未配置 venue）：照拦。
                    assert!(stage_denial_applies(&denial, role, action, false));
                }
            }
            // close 闭集之外的 action（hold / 未知 / 开仓字面量）不是只减退出：照拦。
            for action in ["hold", "noop", "open_long", "CLOSE_LONG", ""] {
                assert!(stage_denial_applies(&denial, OrderRole::Exit, action, true));
            }
        }
    }

    #[test]
    fn rank_denial_does_not_apply_to_exit_or_emergency() {
        let denial = stage_allowed_with("live", "paper", true)
            .expect_err("entry stage rank must remain bounded by deployment");
        assert!(matches!(denial, StageDenial::ExceedsEffective { .. }));
        for role in [OrderRole::Exit, OrderRole::Emergency] {
            for reachable in [true, false] {
                assert!(!stage_denial_applies(
                    &denial,
                    role,
                    "close_long",
                    reachable
                ));
            }
        }
        assert!(stage_denial_applies(
            &denial,
            OrderRole::Entry,
            "open_long",
            true
        ));
    }

    /// 其余拒因（venue 配置、凭据身份、未知档位）对只减退出照样生效：D39 只
    /// 豁免实盘总开关这一条。
    #[test]
    fn non_switch_stage_denials_still_apply_to_reduce_only_exits() {
        for denial in [
            StageDenial::UnsupportedStage {
                requested: "x".into(),
            },
            StageDenial::VenueConfiguration("bad venue".into()),
            StageDenial::SimulationCredentialNotExchangeBacked,
            StageDenial::UnprovenCredentialIdentity,
        ] {
            assert!(stage_denial_applies(
                &denial,
                OrderRole::Exit,
                "close_long",
                true
            ));
        }
    }

    /// D39 · 单仓 reduce-only 平仓（LiveDisabled 豁免）与紧急 flatten（kill switch
    /// 的凭据选择与本地真值判定）共用的那一个判据：交易所背书 ∧（总开关开启 ∨
    /// venue 显式配置）。模拟 / 身份未证明的凭据在任何开关状态下都不出站。
    #[test]
    fn reduce_only_exit_reach_is_exchange_backed_with_live_or_a_configured_venue() {
        use crate::venue::CredentialKind::{ExchangeBacked, Simulation, UnprovenIdentity};
        for live_enabled in [false, true] {
            for kind in [ExchangeBacked, Simulation, UnprovenIdentity] {
                for venue in [None, Some(Venue::Testnet), Some(Venue::Mainnet)] {
                    assert_eq!(
                        reduce_only_exit_reaches_exchange_for(kind, live_enabled, venue),
                        kind == ExchangeBacked && (live_enabled || venue.is_some()),
                        "kind={kind:?} live_enabled={live_enabled} venue={venue:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn demo_still_requires_explicit_deployment_stage_opt_in() {
        assert!(stage_allowed_with("demo", "paper", false).is_err());
        assert!(stage_allowed_with("demo", "demo", false).is_ok());
    }

    #[test]
    fn simulation_credential_ignores_mainnet_env_and_does_not_relax_stage() {
        let credential_id = "paper:0123456789abcdef0123456789abcdef";
        let env_name = crate::venue::venue_env_name(credential_id);
        let previous = std::env::var_os(&env_name);
        std::env::set_var(&env_name, "mainnet");

        let venue = crate::venue::venue_for_credential(credential_id);
        let stage = stage_allowed_with_credential("demo", credential_id, "paper", false);
        let gateway_allowed = venue.as_ref().map(|venue| {
            exchange_gateway_allowed_for(
                crate::venue::credential_kind(credential_id),
                false,
                *venue,
            )
        });

        if let Some(previous) = previous {
            std::env::set_var(&env_name, previous);
        } else {
            std::env::remove_var(&env_name);
        }

        assert_eq!(venue, Ok(None));
        assert_eq!(gateway_allowed, Ok(false));
        assert!(matches!(
            stage,
            Err(StageDenial::SimulationCredentialNotExchangeBacked)
        ));
    }

    #[test]
    fn stage_certification_venue_fact_is_true_only_for_explicit_testnet() {
        assert!(stage_certification_venue_is_testnet(Some(Venue::Testnet)));
        assert!(!stage_certification_venue_is_testnet(Some(Venue::Mainnet)));
        assert!(!stage_certification_venue_is_testnet(None));
    }

    #[test]
    fn demo_is_exchange_backed_but_paper_is_not() {
        assert!(is_exchange_backed_run_mode("demo"));
        assert!(is_exchange_backed_run_mode("live_small"));
        assert!(is_exchange_backed_run_mode("live"));
        assert!(!is_exchange_backed_run_mode("paper"));
    }

    #[test]
    fn demo_submission_guard_rejects_entry_and_protection_outside_testnet() {
        for role in [
            OrderRole::Entry,
            OrderRole::ProtectionSl,
            OrderRole::ProtectionTp,
        ] {
            assert_eq!(
                demo_submission_reject_reason("demo", Some(Venue::Mainnet), role),
                Some("DEMO_REQUIRES_TESTNET_VENUE")
            );
            assert_eq!(
                demo_submission_reject_reason("demo", None, role),
                Some("DEMO_REQUIRES_TESTNET_VENUE")
            );
            assert_eq!(
                demo_submission_reject_reason("demo", Some(Venue::Testnet), role),
                None
            );
        }
    }

    #[test]
    fn demo_venue_policy_does_not_block_exit_or_emergency_flatten() {
        for role in [OrderRole::Exit, OrderRole::Emergency] {
            assert_eq!(
                demo_submission_reject_reason("demo", Some(Venue::Mainnet), role),
                None
            );
            assert_eq!(demo_submission_reject_reason("demo", None, role), None);
        }
    }

    #[test]
    fn testnet_gateway_is_allowed_when_live_switch_is_off() {
        assert!(exchange_gateway_allowed_for(
            crate::venue::CredentialKind::ExchangeBacked,
            false,
            Some(Venue::Testnet)
        ));
    }

    #[test]
    fn mainnet_gateway_is_blocked_when_live_switch_is_off() {
        assert!(!exchange_gateway_allowed_for(
            crate::venue::CredentialKind::ExchangeBacked,
            false,
            Some(Venue::Mainnet)
        ));
    }

    #[test]
    fn unconfigured_gateway_is_blocked_when_live_switch_is_off() {
        assert!(!exchange_gateway_allowed_for(
            crate::venue::CredentialKind::ExchangeBacked,
            false,
            None
        ));
    }

    #[test]
    fn live_switch_allows_mainnet_gateway() {
        assert!(exchange_gateway_allowed_for(
            crate::venue::CredentialKind::ExchangeBacked,
            true,
            Some(Venue::Mainnet)
        ));
    }

    #[test]
    fn live_env_parser_preserves_existing_truthy_case_and_whitespace_behavior() {
        for value in ["1", "true", "yes", " TRUE ", "YeS\t", "\n1\r"] {
            assert!(
                parse_engine_live_enabled(Some(value)),
                "unexpected false for {value:?}"
            );
        }
        for value in ["", "0", "false", "no", "truthy", " true-ish "] {
            assert!(
                !parse_engine_live_enabled(Some(value)),
                "unexpected true for {value:?}"
            );
        }
        assert!(!parse_engine_live_enabled(None));
    }

    #[test]
    fn exchange_boundary_and_protection_branches_share_demo_semantics() {
        let source = include_str!("svc.rs");
        let boundary = source
            .split("// Defense in depth: this venue lookup is deliberately repeated")
            .nth(1)
            .expect("exchange boundary defense marker")
            .split("let keys =")
            .next()
            .expect("exchange boundary venue assertion block");
        assert!(boundary.contains("demo_submission_reject_reason("));

        let dangerous_not_equal = ["run_mode != ", "\"live\""].concat();
        let dangerous_equal = ["run_mode == ", "\"live\""].concat();
        assert!(!source.contains(&dangerous_not_equal));
        assert!(!source.contains(&dangerous_equal));
    }

    #[test]
    fn protection_mode_derivation_has_a_pure_testnet_mapping() {
        assert_eq!(protection_run_mode_for_venue(Some(Venue::Testnet)), "demo");
        assert_eq!(protection_run_mode_for_venue(Some(Venue::Mainnet)), "live");
        assert_eq!(protection_run_mode_for_venue(None), "live");

        // Keep the user-stream callback connected to the wrapper without
        // mutating process-global test env.
        let source = include_str!("svc.rs");
        assert!(source.contains("protection_run_mode_for_credential(credential_id)"));
    }
    use super::{resolve_close_family_quantity, V2ExecutionContext, V2IntentContext};
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    async fn test_store() -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-p1a-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        // Equity is engine truth. Factory policy deliberately allows v1's
        // absent typed protection while keeping the strategy-level
        // `per_trade.protection_required=true` for the v2 cutover.
        sqlx::query(
            r#"
            INSERT INTO engine_balances (
              credential_id, exchange, market_kind, asset,
              wallet_balance, available_balance, updated_at_ms
            ) VALUES ('cred_p1a', 'binance', 'futures', 'USDT', '10000', '10000', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .expect("insert engine balance truth");
        let budget_now = super::now_ms();
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES (
              'cred_p1a', ?, '10000', '10000', '0', 0,
              0, 0, 0, '{"majors":0,"l1_alts":0}', 'none', '5000'
            )
            "#,
        )
        // Keep this historical decimal-text fixture representation stable across
        // SQLite and MySQL; SQLx 0.8.6 preserves direct i64 values as well.
        .bind(budget_now.to_string())
        .execute(store.pool())
        .await
        .expect("insert fresh factory risk budget");
        (store, path)
    }

    /// 插一张处于给定状态的订单，用于提交闸门的交错用例。
    async fn seed_order(store: &Store, engine_order_id: &str, status: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
            ) VALUES (?, 'cred_p1a', 'bot_gate', ?, 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long',
              'market', '1', 0, 'entry', ?, 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(status)
        .execute(store.pool())
        .await
        .expect("insert gate fixture order");
    }

    async fn seed_cancelable_entry_authority(store: &Store, engine_order_id: &str) {
        let position_id = format!("pos_{engine_order_id}");
        let verdict_id = format!("rv_{engine_order_id}");
        let authority_key = position_state::semantic_idempotency_key(
            "entry|bot_p1a|binance|futures|BTC|USDT|USDT|long|1",
        );
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin cancelable Entry seed");
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (?, 'cred_p1a', 'bot_p1a',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'pending',
              'bot_p1a|binance|futures|BTC|USDT|USDT|long', ?, '0.000', 1)
            "#,
        )
        .bind(&position_id)
        .bind(&authority_key)
        .execute(&mut *tx)
        .await
        .expect("seed pending Entry claim");
        sqlx::query(
            r#"
            INSERT INTO risk_verdicts (
              risk_verdict_id, intent_id, approved_intent_key,
              credential_id, bot_uid, symbol_key,
              created_at_ms, policy_version, policy_hash, decision, sizing_json,
              rules_json, evaluator_version
            ) VALUES (?, ?, ?, 'cred_p1a', 'bot_p1a',
              'binance|futures|BTC|USDT|USDT', 1, 'test', 'hash', 'approve',
              '{"final_notional_usdt":"100"}', '[]', 'test')
            "#,
        )
        .bind(&verdict_id)
        .bind(format!("intent_{engine_order_id}"))
        .bind(format!("intent_{engine_order_id}"))
        .execute(&mut *tx)
        .await
        .expect("seed durable Entry verdict");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, risk_verdict_id,
              position_id, created_at_ms, updated_at_ms
            ) VALUES (?, 'cred_p1a', 'bot_p1a', ?, 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'market',
              '1', 0, 'entry', 'pending_risk', ?, ?, 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(&authority_key)
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
            ) VALUES (?, 'cred_p1a', 'bot_p1a', 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'long', '95', '110',
              1, 3000, 1, 1, 1)
            "#,
        )
        .bind(&position_id)
        .execute(&mut *tx)
        .await
        .expect("seed pending protection plan");
        sqlx::query(
            "INSERT INTO notional_reservations \
             (credential_id, reserved_usdt, cap_usdt, updated_at_ms) \
             VALUES ('cred_p1a', '100', '5000', 1)",
        )
        .execute(&mut *tx)
        .await
        .expect("seed pending Entry reservation");
        tx.commit().await.expect("commit cancelable Entry seed");
    }

    async fn order_status(store: &Store, engine_order_id: &str) -> String {
        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = ?")
            .bind(engine_order_id)
            .fetch_one(store.pool())
            .await
            .expect("read order status")
            .get::<String, _>("status")
    }

    async fn order_status_and_timestamp(store: &Store, engine_order_id: &str) -> (String, i64) {
        let row = sqlx::query(
            "SELECT status, CAST(updated_at_ms AS CHAR) AS updated_at_ms_text \
             FROM engine_orders WHERE engine_order_id = ?",
        )
        .bind(engine_order_id)
        .fetch_one(store.pool())
        .await
        .expect("read exact-status engine order");
        (
            row.get("status"),
            crate::store::try_get_i64_text(&row, "updated_at_ms_text")
                .expect("decode exact-status timestamp"),
        )
    }

    ///     /// **提交控制流**：CAS 影响 0 行时，交易所提交那一步一次都不能跑。
    ///
    /// 这条驱动的生产函数是 `svc::advance_then_submit`——生产的提交路径
    /// （`ExecuteIntents` 的 exchange-backed 分支）就是通过它把 `place`
    /// 这个 future 交出去的，没有第二条通往交易所的路。
    /// R5 把这条控制流写在调用点上、没有任何用例驱动，于是复评把
    /// `if advanced == 0` 改成永不进入，`kill_switch` 26 条全绿。
    #[tokio::test]
    async fn a_cancelled_order_must_never_reach_the_exchange_submit() {
        let (store, path) = test_store().await;
        seed_cancelable_entry_authority(&store, "eng_gate_cancelled").await;

        // L2 撤单走的是生产函数（读到的状态仍是 pending_risk）。
        let canceled = crate::kill_switch::cancel_order_if_status(
            &store,
            "eng_gate_cancelled",
            "pending_risk",
            10,
        )
        .await
        .expect("cancel");
        assert_eq!(canceled, 1);
        assert_eq!(order_status(&store, "eng_gate_cancelled").await, "canceled");

        let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = submitted.clone();
        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_gate_cancelled",
            20,
            async move {
                probe.store(true, std::sync::atomic::Ordering::SeqCst);
                super::ExchangeAttempt::StartedUnknown(anyhow::anyhow!(
                    "the exchange must not be reached"
                ))
            },
        )
        .await
        .expect("db must not fail in this fixture");

        assert!(
            matches!(
                outcome,
                super::AdvanceThenSubmitOutcome::Blocked {
                    observed_raw: Some(ref status)
                } if status == "canceled"
            ),
            "the blocked result must retain the raw status for audit"
        );
        assert!(
            !submitted.load(std::sync::atomic::Ordering::SeqCst),
            "the exchange submit future must never be polled after a cancel"
        );
        // 撤单真值必须原样保留——闸门自己不得改写这一行。
        assert_eq!(order_status(&store, "eng_gate_cancelled").await, "canceled");

        let reservation = sqlx::query(
            "SELECT reserved_usdt FROM notional_reservations WHERE credential_id='cred_p1a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load released reservation")
        .get::<String, _>("reserved_usdt")
        .parse::<f64>()
        .expect("numeric reservation");
        assert_eq!(reservation, 0.0);
        let claim = sqlx::query(
            "SELECT state, entry_epoch, active_key IS NULL AS active_key_is_null, \
                    pending_intent_id IS NULL AS pending_intent_is_null, \
                    pending_idem_key IS NULL AS pending_idem_is_null \
             FROM position_state WHERE position_id='pos_eng_gate_cancelled'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load retired Entry claim");
        assert_eq!(claim.get::<String, _>("state"), "closed");
        assert_eq!(claim.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(claim.get::<i64, _>("active_key_is_null"), 1);
        assert_eq!(claim.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(claim.get::<i64, _>("pending_idem_is_null"), 1);
        let plan_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans \
             WHERE position_id='pos_eng_gate_cancelled'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count retired protection plan")
        .get::<i64, _>("count");
        assert_eq!(plan_count, 0);

        let mut next_tx = store.pool().begin().await.expect("begin next Entry claim");
        let next = position_state::prepare_intent(
            &mut next_tx,
            "cred_p1a",
            &intent("open_long", "cycle_after_cancel", "client_after_cancel"),
            25,
        )
        .await
        .expect("prepare next Entry after canceled claim");
        let position_state::PrepareOutcome::Ready(next) = next else {
            panic!("retired claim must permit the next Entry epoch");
        };
        let epoch_2 = position_state::semantic_idempotency_key(
            "entry|bot_p1a|binance|futures|BTC|USDT|USDT|long|2",
        );
        assert_eq!(next.authority_idem_key, epoch_2);
        next_tx.rollback().await.expect("rollback next Entry probe");

        // **自证夹具**：同一个函数、同一个探针，换成仍在提交前状态的行就必须
        // 真的提交。少了这一半，上面那条红可以是"函数恒不提交"造成的假绿。
        seed_order(&store, "eng_gate_live", "pending_risk").await;
        let submitted_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe_ok = submitted_ok.clone();
        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_gate_live",
            30,
            async move {
                probe_ok.store(true, std::sync::atomic::Ordering::SeqCst);
                super::ExchangeAttempt::StartedUnknown(anyhow::anyhow!("reached the exchange"))
            },
        )
        .await
        .expect("db must not fail in this fixture");
        assert!(
            matches!(
                outcome,
                super::AdvanceThenSubmitOutcome::Advanced(super::ExchangeAttempt::StartedUnknown(
                    _
                ))
            ),
            "an exact pending_risk order must acquire submission ownership"
        );
        assert!(submitted_ok.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            order_status(&store, "eng_gate_live").await,
            "pending_submit"
        );

        cleanup(store, path).await;
    }

    /// 提交路径的**终态写回**不得
    /// 覆盖已经落下的撤单真值。驱动的生产函数是
    /// `svc::finalize_pre_submit_status`，`live_reject` 与"没联到交易所"的
    /// `live_error` 两条后处理分支都只经它写库。
    #[tokio::test]
    async fn post_submit_writeback_must_not_resurrect_a_cancelled_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_writeback_cancelled", "canceled").await;

        for status in ["rejected", "unknown"] {
            let rows = super::finalize_pre_submit_status(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_writeback_cancelled",
                status,
                40,
            )
            .await
            .expect("writeback");
            assert_eq!(rows, 0, "{status} must not overwrite a cancelled order");
        }
        assert_eq!(
            order_status(&store, "eng_writeback_cancelled").await,
            "canceled"
        );

        // 自证夹具：仍在提交前状态的行照常写回。
        seed_order(&store, "eng_writeback_open", "pending_risk").await;
        let rows = super::finalize_pre_submit_status(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_writeback_open",
            "rejected",
            50,
        )
        .await
        .expect("writeback");
        assert_eq!(rows, 1);
        assert_eq!(order_status(&store, "eng_writeback_open").await, "rejected");

        cleanup(store, path).await;
    }

    /// 把 regression 的复现契约整条串起来：撤单 → 提交闸门中止 →
    /// 两条后处理写回都影响 0 行 → 行仍是 `canceled`。
    ///
    /// 复评给的反例是 `canceled → rejected → unknown`；这条用例逐步断言那
    /// 三步都不再发生，且用的都是生产函数，不是测试自己重写的 SQL。
    #[tokio::test]
    async fn the_cancel_truth_survives_the_whole_submit_postprocessing() {
        let (store, path) = test_store().await;
        seed_cancelable_entry_authority(&store, "eng_chain").await;
        assert_eq!(
            crate::kill_switch::cancel_order_if_status(&store, "eng_chain", "pending_risk", 1)
                .await
                .expect("cancel"),
            1
        );

        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_chain",
            2,
            async { super::ExchangeAttempt::StartedUnknown(anyhow::anyhow!("unreachable")) },
        )
        .await
        .expect("db must not fail in this fixture");
        assert!(
            matches!(
                outcome,
                super::AdvanceThenSubmitOutcome::Blocked {
                    observed_raw: Some(ref status)
                } if status == "canceled"
            ),
            "the cancellation must surface as an audited ownership block"
        );
        for status in ["rejected", "unknown"] {
            assert_eq!(
                super::finalize_pre_submit_status(
                    store.pool(),
                    store.kind(),
                    "cred_p1a",
                    "eng_chain",
                    status,
                    3,
                )
                .await
                .expect("writeback"),
                0
            );
        }
        assert_eq!(order_status(&store, "eng_chain").await, "canceled");
        cleanup(store, path).await;
    }

    ///     /// **「没发过包」与「发了但不知道结果」是两件事**。
    ///
    /// R6 用一个布尔 `exchange_request_attempted` 表达这件事，而它在**任何**
    /// `place` 错误之后置位——路由不合法、limit 缺 price 这些根本没发包的
    /// 本地错误也会置位，于是它们照样走无条件 `unknown` 写回，
    /// 把别的路径落下的终态盖掉。复评变异 N1 在 93 条里全绿。
    ///
    /// 这条驱动的生产函数是 `svc::advance_then_submit` + `ExchangeAttempt`：
    /// 分类由**提交闭包自己**给出，调用方不再猜。
    #[tokio::test]
    async fn a_submission_that_never_left_the_process_must_not_claim_unknown() {
        let (store, path) = test_store().await;

        // ① 没发过包：本地拒绝，写回带 CAS —— 盖不掉已经落下的终态。
        seed_order(&store, "eng_not_started", "canceled").await;
        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_not_started",
            1,
            async { super::ExchangeAttempt::NotStarted(anyhow::anyhow!("bad route")) },
        )
        .await
        .expect("db must not fail");
        // 行已终态：闸门先拦下，连分类都用不上。
        assert!(matches!(
            outcome,
            super::AdvanceThenSubmitOutcome::Blocked {
                observed_raw: Some(ref status)
            } if status == "canceled"
        ));
        assert_eq!(order_status(&store, "eng_not_started").await, "canceled");

        // ② 同一条路径上，行仍在提交前：分类必须原样传出来，
        //    调用方据此走「本地拒绝」而不是「结果不明」。
        seed_order(&store, "eng_not_started_live", "pending_risk").await;
        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_not_started_live",
            2,
            async {
                super::ExchangeAttempt::NotStarted(anyhow::anyhow!("limit order missing price"))
            },
        )
        .await
        .expect("db must not fail");
        assert!(
            matches!(
                outcome,
                super::AdvanceThenSubmitOutcome::Advanced(super::ExchangeAttempt::NotStarted(_))
            ),
            "a local parameter failure must stay classified as NotStarted"
        );

        // ③ 发了但不知道结果：必须是另一个分类。两者不能塌成同一个值——
        //    塌了就回到 R6 那个布尔的处境。
        seed_order(&store, "eng_started_unknown", "pending_risk").await;
        let outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_started_unknown",
            3,
            async { super::ExchangeAttempt::StartedUnknown(anyhow::anyhow!("request timeout")) },
        )
        .await
        .expect("db must not fail");
        assert!(
            matches!(
                outcome,
                super::AdvanceThenSubmitOutcome::Advanced(super::ExchangeAttempt::StartedUnknown(
                    _
                ))
            ),
            "an in-flight failure must stay classified as StartedUnknown"
        );

        cleanup(store, path).await;
    }

    ///     /// **分类 → 写回语义的映射必须被钉住，不只是分类本身。**
    ///
    /// R7-C：把 `NotStarted` 那条臂的臂体整个换成 `StartedUnknown` 的处理方式，
    /// 214 条测试全绿——与 R6 的 MUT-C 完全同形：判据钉住了，
    /// **判据到控制流那一段没钉**。现在两个写回标志只能由 `write_back_intent`
    /// 产生，这条把四个分类的映射逐一断言。
    #[test]
    fn the_write_back_semantics_of_each_attempt_class_are_pinned() {
        use super::{write_back_intent, ExchangeAttempt};

        // 请求从未离开本进程 → 同样是本地拒绝。交易所侧不可能有这张单，
        // 写 `unknown` 等于为一张从未存在的单声称"结果不明"。
        let never_sent = ExchangeAttempt::NotStarted(anyhow::anyhow!("limit order missing price"));
        let not_started = write_back_intent(&never_sent);
        assert!(not_started.local_reject);
        assert!(
            !not_started.outcome_unknown,
            "a request that never left the process must not claim an unknown outcome"
        );

        // 已发出、结果不明 → 必须无条件写 unknown 让 reconcile 复查；
        // 此时**不得**走带 CAS 的本地拒绝，否则活单会失去追踪。
        let in_flight = ExchangeAttempt::StartedUnknown(anyhow::anyhow!("timeout"));
        let unknown = write_back_intent(&in_flight);
        assert!(!unknown.local_reject);
        assert!(unknown.outcome_unknown);

        // 两个标志**永远不能同时为真**：那等于同一行既"本地拒绝"又"结果不明"。
        for intent in [not_started, unknown] {
            assert!(!(intent.local_reject && intent.outcome_unknown));
        }
    }

    ///     /// **迟到的"不知道"不得盖掉交易所已经给出的真值。**
    ///
    /// R7 这条写回是**无状态谓词**的 UPDATE：一次迟到的 HTTP timeout 能把
    /// WS/reconcile 已经写下的 `filled` / `acked` 覆盖成 `unknown`——
    /// 用更弱的真值盖掉更强的真值。三态换了名字，CAS 没加。
    #[tokio::test]
    async fn a_late_unknown_must_not_downgrade_an_exchange_sourced_status() {
        let (store, path) = test_store().await;

        // 已经带交易所真值的状态，一条都不许被降级。
        for status in [
            "acked",
            "partial_filled",
            "filled",
            "canceled",
            "expired",
            "rejected",
            "failed",
        ] {
            let order_id = format!("eng_late_{status}");
            seed_order(&store, &order_id, status).await;
            let rows = super::finalize_status_if_in_flight(
                store.pool(),
                store.kind(),
                "cred_p1a",
                &order_id,
                "unknown",
                10,
            )
            .await
            .expect("writeback");
            assert_eq!(rows, 0, "{status} must not be downgraded to unknown");
            assert_eq!(order_status(&store, &order_id).await, status);
        }

        // 自证：还停在"等提交结果"的行必须照常转 unknown——
        // 挡住它才是危险的（交易所可能挂着活单，reconcile 靠 unknown 去复查）。
        seed_order(&store, "eng_late_inflight", "pending_submit").await;
        let rows = super::finalize_status_if_in_flight(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_late_inflight",
            "unknown",
            11,
        )
        .await
        .expect("writeback");
        assert_eq!(rows, 1);
        assert_eq!(order_status(&store, "eng_late_inflight").await, "unknown");

        // 提交**之前**的状态也不该走这条路（它有自己的 CAS 写回）——
        // 两个谓词分工必须清楚，否则又会变成"一个谓词管所有情况"。
        seed_order(&store, "eng_late_prerisk", "pending_risk").await;
        assert_eq!(
            super::finalize_status_if_in_flight(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_late_prerisk",
                "unknown",
                12,
            )
            .await
            .expect("writeback"),
            0
        );

        cleanup(store, path).await;
    }

    /// **闭世界校验要由真实 RPC 驱动。**
    ///
    /// 我先写的用例只驱动了 `timeframe_to_ms` 与中断判据，没有驱动
    /// `ensure_market_data` 这个**真实入口**——于是把入口那句
    /// `if timeframe_to_ms(..).is_none()` 改成 `if false`，221 条全绿。
    /// 又一次「判据钉住了，判据的执行点没钉」。
    ///
    /// 这条走真实 RPC：认不出的周期必须**整批拒绝**（`accepted=false`），
    /// 且**一条 required 行都不许落库**——半个订阅集合是判据看不见的状态。
    #[tokio::test]
    async fn the_subscription_rpc_refuses_unrecognised_timeframes() {
        use crate::proto::{EnsureMarketDataRequest, MarketDataSubscription};

        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        let sub = |timeframe: &str| MarketDataSubscription {
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            timeframe: timeframe.to_string(),
        };

        // 一条合法 + 一条认不出 → **整批**拒绝。
        let response = service
            .ensure_market_data(Request::new(EnsureMarketDataRequest {
                credential_id: "cred_p1a".to_string(),
                subscriptions: vec![sub("1m"), sub("banana")],
            }))
            .await
            .expect("rpc")
            .into_inner();
        assert!(
            !response.accepted,
            "an unmappable timeframe must be refused"
        );
        assert_eq!(response.started, 0);
        assert!(response.message.contains("banana"));

        // **一条都不许落库**：合法的那条也不能先登记（半个集合最危险）。
        let registered = crate::auto_triggers::required_streams(&store, "cred_p1a")
            .await
            .expect("read required");
        assert!(
            registered.is_empty(),
            "a refused batch must not leave a partial required-set behind, found {registered:?}"
        );

        // **放行面自证**：全部合法时照常接受并登记，收紧不得误伤。
        let response = service
            .ensure_market_data(Request::new(EnsureMarketDataRequest {
                credential_id: "cred_p1a".to_string(),
                subscriptions: vec![sub("1m"), sub("5m")],
            }))
            .await
            .expect("rpc")
            .into_inner();
        assert!(response.accepted);
        let registered = crate::auto_triggers::required_streams(&store, "cred_p1a")
            .await
            .expect("read required");
        assert_eq!(registered.len(), 2);

        cleanup(store, path).await;
    }

    ///     /// **「请求会不会离开本进程」这条判据必须被直接驱动。**
    ///
    /// R7 把三个 `NotStarted` 的产生点散在 `place` 闭包里，用例自己构造 enum
    /// 喂给 helper、从不经过真实分类，于是把闭包里的 `NotStarted` 改成
    /// `StartedUnknown`，用例 1/1 照样通过。现在三个产生点收敛成
    /// `plan_exchange_submission` 这一个函数，这条逐项驱动它。
    ///
    /// 链路：`plan_exchange_submission`（本条） → `NotStarted`
    /// → `write_back_intent` 判 `local_reject`（`the_write_back_semantics_…`）
    /// → `finalize_pre_submit_status` 带 CAS 写 `rejected`
    /// （`post_submit_writeback_…`）。三段各有行为用例。
    #[test]
    fn the_local_submittability_verdict_is_driven_directly() {
        use super::plan_exchange_submission;
        use crate::protection::OrderRole;

        // ① 路由不合法 → 不会发出去。
        assert!(plan_exchange_submission(OrderRole::Entry, "iceberg", "100").is_err());
        // ② limit 缺 price → 不会发出去（空串与纯空白都算缺）。
        assert!(plan_exchange_submission(OrderRole::Entry, "limit", "").is_err());
        assert!(plan_exchange_submission(OrderRole::Entry, "limit", "   ").is_err());
        // ③ 条件单缺 triggerPrice → 不会发出去。
        assert!(plan_exchange_submission(OrderRole::ProtectionSl, "stop_market", "").is_err());
        assert!(
            plan_exchange_submission(OrderRole::ProtectionTp, "take_profit_market", "").is_err()
        );

        // **放行面自证**：合法组合必须真的可提交，否则"永远不发"也能让上面全绿。
        assert!(plan_exchange_submission(OrderRole::Entry, "market", "").is_ok());
        assert!(plan_exchange_submission(OrderRole::Entry, "limit", "100").is_ok());
        assert!(plan_exchange_submission(OrderRole::ProtectionSl, "stop_market", "99").is_ok());

        // 源码守卫（第二层）：**生产段**里不得再出现第二个 `NotStarted` 产生点
        // ——散开正是 NEW-4 能存活的原因。
        //
        // 只扫生产段：整文件扫会把测试自己构造的 enum 也数进去（首版就是这么
        // 误报 8 处的）。守卫必须只看**被判对象**，这与"剥掉注释再扫"同一条纪律。
        let whole = code_without_comments(include_str!("svc.rs"));
        let production = whole
            .split_once("#[cfg(test)]\nmod tests {")
            .map(|(before, _)| before.to_string())
            .unwrap_or(whole);
        // 数的是**产生点**（`return ...NotStarted(`），不是模式匹配——
        // `Some(ExchangeAttempt::NotStarted(_)) =>` 是消费方，数进去就把
        // "在哪里产生"和"在哪里被处理"混成了一件事。
        let produced = production
            .matches("return ExchangeAttempt::NotStarted(")
            .count();
        assert_eq!(
            produced, 1,
            "NotStarted must be produced by exactly one adapter in production code"
        );
    }

    /// **四个分类都要被用例构造到。**
    ///
    /// R7 的 mapping 用例只喂了三个分类，`Acked` 从未构造——把它的
    /// `local_reject` 翻成 `true`，用例 1/1 照样通过。
    #[test]
    fn the_acked_class_is_covered_by_the_write_back_mapping_test() {
        use super::{write_back_intent, ExchangeAttempt, ExchangeSubmissionAck};
        use crate::binance_futures::{BinanceOrderAck, BinanceOrderId};

        let acked = ExchangeAttempt::Acked(ExchangeSubmissionAck::Ordinary(BinanceOrderAck {
            order_id: BinanceOrderId::new(7001),
            client_order_id: "coid_acked_cover".to_string(),
            status: "NEW".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "BUY".to_string(),
            orig_qty: "1".to_string(),
            executed_qty: "0".to_string(),
        }));
        let intent = write_back_intent(&acked);
        // 交易所明确应答：两条写回都不适用，由 ack 分支自己落库。
        assert!(!intent.local_reject);
        assert!(!intent.outcome_unknown);
    }

    #[test]
    fn duplicate_ack_never_claims_exchange_acceptance_or_reexecution() {
        let exchange = super::duplicate_ack_message(true, "authority-exchange");
        assert_eq!(
            exchange,
            "duplicate; exchange not resubmitted; authoritative_idempotency_key=authority-exchange"
        );
        assert!(!exchange.contains("accepted"));

        let paper = super::duplicate_ack_message(false, "authority-paper");
        assert_eq!(
            paper,
            "duplicate; paper execution not repeated; authoritative_idempotency_key=authority-paper"
        );
        assert!(!paper.contains("accepted"));
    }

    /// MySQL 的生产 `status` 列使用 `_ci` collation，因此普通 `=`/`IN`
    /// 会把大小写脏值当成 canonical 状态。本用例必须在真 MySQL 上证明三处
    /// authority CAS 都按字节精确比较；缺少真库时由共享夹具显式 SKIP。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_intents_status_cas_is_byte_exact_on_mysql_ci_collation() {
        const TEST: &str = "execute_intents_status_cas_is_byte_exact_on_mysql_ci_collation";
        let Some(scratch) = MySqlScratch::create("execstatus", TEST).await else {
            return;
        };
        let store = scratch.connect_store().await;

        let collation = sqlx::query(
            "SELECT COLLATION_NAME AS collation_name FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'engine_orders' \
             AND COLUMN_NAME = 'status'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the migrated MySQL status-column collation")
        .get::<String, _>("collation_name");
        assert!(
            collation.to_ascii_lowercase().ends_with("_ci"),
            "fixture must exercise a case-insensitive MySQL status column, got {collation}"
        );

        seed_order(&store, "eng_mysql_dirty_pending_risk", "PENDING_RISK").await;
        let dirty_polled = Arc::new(AtomicBool::new(false));
        let dirty_probe = dirty_polled.clone();
        let dirty_outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_mysql_dirty_pending_risk",
            10,
            async move {
                dirty_probe.store(true, Ordering::SeqCst);
                super::ExchangeAttempt::NotStarted(anyhow::anyhow!("must not be polled"))
            },
        )
        .await
        .expect("dirty pending-risk value must be an audited block");
        assert!(matches!(
            dirty_outcome,
            super::AdvanceThenSubmitOutcome::Blocked {
                observed_raw: Some(ref status)
            } if status == "PENDING_RISK"
        ));
        assert!(
            !dirty_polled.load(Ordering::SeqCst),
            "case-insensitive collation must not authorize the submit future"
        );
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_dirty_pending_risk").await,
            ("PENDING_RISK".to_string(), 1),
            "a blocked dirty value must remain byte-for-byte and timestamp unchanged"
        );

        // Canonical `pending_submit` is still not a submit-ownership source.
        // This negative control kills a mutation that broadens the gate from
        // exact `pending_risk` to `pending_risk OR pending_submit`.
        seed_order(
            &store,
            "eng_mysql_exact_pending_submit_gate",
            "pending_submit",
        )
        .await;
        let pending_submit_polled = Arc::new(AtomicBool::new(false));
        let pending_submit_probe = pending_submit_polled.clone();
        let pending_submit_outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_mysql_exact_pending_submit_gate",
            10,
            async move {
                pending_submit_probe.store(true, Ordering::SeqCst);
                super::ExchangeAttempt::NotStarted(anyhow::anyhow!("must not be resubmitted"))
            },
        )
        .await
        .expect("canonical pending_submit must remain blocked at the ownership gate");
        assert!(matches!(
            pending_submit_outcome,
            super::AdvanceThenSubmitOutcome::Blocked {
                observed_raw: Some(ref status)
            } if status == "pending_submit"
        ));
        assert!(
            !pending_submit_polled.load(Ordering::SeqCst),
            "pending_submit must never authorize a second exchange submit"
        );
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_exact_pending_submit_gate").await,
            ("pending_submit".to_string(), 1)
        );

        seed_order(&store, "eng_mysql_exact_pending_risk", "pending_risk").await;
        let exact_polled = Arc::new(AtomicBool::new(false));
        let exact_probe = exact_polled.clone();
        let exact_outcome = super::advance_then_submit(
            store.pool(),
            store.kind(),
            "cred_p1a",
            "eng_mysql_exact_pending_risk",
            11,
            async move {
                exact_probe.store(true, Ordering::SeqCst);
                super::ExchangeAttempt::NotStarted(anyhow::anyhow!("positive control"))
            },
        )
        .await
        .expect("canonical pending_risk must acquire submission ownership");
        assert!(matches!(
            exact_outcome,
            super::AdvanceThenSubmitOutcome::Advanced(super::ExchangeAttempt::NotStarted(_))
        ));
        assert!(exact_polled.load(Ordering::SeqCst));
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_exact_pending_risk").await,
            ("pending_submit".to_string(), 11)
        );

        seed_order(&store, "eng_mysql_dirty_pending_submit", "PENDING_SUBMIT").await;
        assert_eq!(
            super::finalize_status_if_in_flight(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_mysql_dirty_pending_submit",
                "unknown",
                20,
            )
            .await
            .expect("dirty in-flight finalizer"),
            0
        );
        assert_eq!(
            super::finalize_pre_submit_status(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_mysql_dirty_pending_submit",
                "rejected",
                21,
            )
            .await
            .expect("dirty pre-submit finalizer"),
            0
        );
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_dirty_pending_submit").await,
            ("PENDING_SUBMIT".to_string(), 1),
            "both finalizers must preserve the dirty bytes and timestamp"
        );

        seed_order(&store, "eng_mysql_exact_in_flight", "pending_submit").await;
        assert_eq!(
            super::finalize_status_if_in_flight(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_mysql_exact_in_flight",
                "unknown",
                30,
            )
            .await
            .expect("canonical in-flight positive control"),
            1
        );
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_exact_in_flight").await,
            ("unknown".to_string(), 30)
        );

        seed_order(&store, "eng_mysql_exact_pre_submit", "pending_submit").await;
        assert_eq!(
            super::finalize_pre_submit_status(
                store.pool(),
                store.kind(),
                "cred_p1a",
                "eng_mysql_exact_pre_submit",
                "rejected",
                31,
            )
            .await
            .expect("canonical pre-submit positive control"),
            1
        );
        assert_eq!(
            order_status_and_timestamp(&store, "eng_mysql_exact_pre_submit").await,
            ("rejected".to_string(), 31)
        );

        tokio::time::timeout(Duration::from_secs(5), store.pool().close())
            .await
            .expect("close MySQL test store before scratch cleanup");
        drop(store);
        tokio::time::timeout(Duration::from_secs(10), scratch.cleanup())
            .await
            .expect("drop the isolated MySQL scratch database");
    }

    #[tokio::test]
    async fn an_unknown_database_dialect_cannot_authorize_or_finalize_status() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_unknown_dialect", "pending_risk").await;
        let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = submitted.clone();
        let error = super::advance_then_submit(
            store.pool(),
            crate::store::DbKind::Unknown,
            "cred_p1a",
            "eng_unknown_dialect",
            1,
            async move {
                probe.store(true, std::sync::atomic::Ordering::SeqCst);
                super::ExchangeAttempt::NotStarted(anyhow::anyhow!("unreachable"))
            },
        )
        .await
        .expect_err("an unknown dialect must fail before submission");
        assert!(matches!(error, sqlx::Error::Protocol(_)));
        assert!(!submitted.load(std::sync::atomic::Ordering::SeqCst));

        for result in [
            super::finalize_status_if_in_flight(
                store.pool(),
                crate::store::DbKind::Unknown,
                "cred_p1a",
                "eng_unknown_dialect",
                "unknown",
                2,
            )
            .await,
            super::finalize_pre_submit_status(
                store.pool(),
                crate::store::DbKind::Unknown,
                "cred_p1a",
                "eng_unknown_dialect",
                "rejected",
                3,
            )
            .await,
        ] {
            assert!(matches!(result, Err(sqlx::Error::Protocol(_))));
        }
        assert_eq!(
            order_status(&store, "eng_unknown_dialect").await,
            "pending_risk"
        );
        cleanup(store, path).await;
    }

    /// **数据库故障不得被当成交易真值**。
    ///
    /// R6 这里是 `.unwrap_or(0)`：DB 一故障就与"这一行已终态"返回同一个结果，
    /// 调用方据此落 `ORDER_CANCELLED_BEFORE_SUBMIT` 审计事件——
    /// 把一次数据库失败伪造成「这单被撤了」这条交易真值（违反 §3.12）。
    #[tokio::test]
    async fn a_database_failure_must_not_be_reported_as_a_cancelled_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_db_fail", "pending_risk").await;

        // 关掉连接池，让 UPDATE 必然失败——这是"DB 故障"最直接的构造方式。
        let pool = store.pool().clone();
        pool.close().await;

        let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = submitted.clone();
        let outcome = super::advance_then_submit(
            &pool,
            store.kind(),
            "cred_p1a",
            "eng_db_fail",
            1,
            async move {
                probe.store(true, std::sync::atomic::Ordering::SeqCst);
                super::ExchangeAttempt::StartedUnknown(anyhow::anyhow!("unreachable"))
            },
        )
        .await;

        assert!(
            outcome.is_err(),
            "a database failure must surface as an error, not as `no rows advanced`"
        );
        assert!(
            !submitted.load(std::sync::atomic::Ordering::SeqCst),
            "nothing may be submitted when the pre-submit advance could not be evaluated"
        );
        let _ = std::fs::remove_file(path);
    }

    /// 源码守卫（**第二层**，行为证据见上面三条）：中止分支不得置 `live_error`，
    /// 失败写回不得绕过 `finalize_pre_submit_status`。
    /// 把 `//` 行注释剥掉——**源码守卫必须扫代码，不扫散文**。
    ///
    /// 现场教训：给守卫加「旧标志名不得回来」这条时，守卫立刻被
    /// **解释它为什么被废弃的那段注释**匹配到了。与 `pgrep -f` 自匹配、
    /// 与 R6 那条守卫匹配到自己所在行，是同一个形状：
    /// **判据把描述判据的文字当成了被判对象**。
    fn code_without_comments(source: &str) -> String {
        source
            .lines()
            .map(|line| match line.find("//") {
                Some(index) => &line[..index],
                None => line,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_submit_path_keeps_its_abort_branch_free_of_synthetic_errors() {
        let source = code_without_comments(include_str!("svc.rs"));
        let source = source.as_str();
        assert!(source.contains("advance_then_submit("));
        assert!(source.contains("finalize_pre_submit_status("));
        // 两个"不得回来"的串**运行时拼装**：写成字面量的话，守卫会匹配到
        // 它自己所在的这一行，于是永远为真——本仓已因自匹配 pattern 栽过
        // 多次（`pgrep -f` 那条同形）。
        let synthetic_err = format!(
            "{}{}",
            "submission skipped: order is no longer", " in a pre-submit state"
        );
        assert!(!source.contains(&synthetic_err));
        // reject 写回必须经带 CAS 的助手，不得回到裸 UPDATE。
        let bare_reject_update = format!(
            "{}{}",
            "UPDATE engine_orders SET status='rejected', ", "updated_at_ms=? WHERE"
        );
        assert!(!source.contains(&bare_reject_update));
        // R6 那个名不副实的布尔不得回来（它在任何 Err 之后置位，
        // 包括根本没发包的本地错误——复评变异 N1 据此存活）。
        let retired_flag = format!("{}{}", "exchange_request", "_attempted");
        assert!(!source.contains(&retired_flag));
        // 「DB 错误必须上抛」这条不写文本守卫：`.rows_affected()` 在别处是
        // 合法写法，扫文本只会写出一条既误伤又能被绕开的判据。
        // 它的证据是行为级的 —— `a_database_failure_must_not_be_reported_as_a_cancelled_order`
        // 关掉连接池后断言闸门返回 `Err` 且提交侧一次都没被 poll。
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn seed_algo_update_identity_fixture(store: &Store) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, updated_at_ms
            ) VALUES ('pos_algo_ws', 'cred_p1a', 'bot_algo_ws',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'open',
              'bot_algo_ws|symbol|long|1', '1', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
        for (engine_id, idem, client_id, exchange_id, role, order_type) in [
            (
                "eng_ordinary_collision",
                "idem_ordinary_collision",
                "ordinary-client",
                Some("7001"),
                "exit",
                "market",
            ),
            (
                "eng_algo_ws",
                "idem_algo_ws",
                "algo-client-ws",
                None,
                "protection_sl",
                "stop_market",
            ),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  client_order_id_short, exchange_order_id, exchange,
                  market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, status, position_id, order_role,
                  created_at_ms, updated_at_ms
                ) VALUES (?, 'cred_p1a', 'bot_algo_ws', ?, ?, ?, 'binance',
                  'futures', 'binance|futures|BTC|USDT|USDT', 'close_long',
                  'long', ?, '', 1, 'acked', 'pos_algo_ws', ?, 1, 1)
                "#,
            )
            .bind(engine_id)
            .bind(idem)
            .bind(client_id)
            .bind(exchange_id)
            .bind(order_type)
            .bind(role)
            .execute(store.pool())
            .await
            .unwrap();
        }
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind,
              generation, engine_order_id, exchange_algo_id, client_algo_id,
              algo_status, trigger_price, reduce_only, close_position,
              status, verified_from_exchange, created_at_ms, updated_at_ms
            ) VALUES ('prot_algo_ws', 'pos_algo_ws', 'cred_p1a', 'sl', 1,
              'eng_algo_ws', '7001', 'algo-client-ws', 'NEW', '95', 1, 1,
              'confirmed', 1, 1, 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn b8_algo_update_never_mutates_ordinary_order_with_same_numeric_id() {
        let (store, path) = test_store().await;
        seed_algo_update_identity_fixture(&store).await;
        let envelope = serde_json::from_str(
            r#"{"e":"ALGO_UPDATE","E":2,"T":3,"o":{"caid":"algo-client-ws","aid":7001,"X":"CANCELED","ai":""}}"#,
        )
        .unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope("cred_p1a", &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        let ordinary_status = sqlx::query(
            "SELECT status FROM engine_orders WHERE engine_order_id='eng_ordinary_collision'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("status");
        assert_eq!(ordinary_status, "acked");
        let algo_row = sqlx::query(
            "SELECT algo_status, verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_algo_ws'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(algo_row.get::<String, _>("algo_status"), "CANCELED");
        assert_eq!(
            algo_row.get::<i64, _>("verified_from_exchange"),
            0,
            "WS terminal truth must revoke the current protection projection"
        );
        let event = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type='protection_state'",
        )
        .fetch_one(store.pool())
        .await
        .expect("WS protection truth transition event");
        let payload: serde_json::Value =
            serde_json::from_slice(&event.get::<Vec<u8>, _>("payload_json")).unwrap();
        assert_eq!(payload["truth_source"], "ws_algo_update");
        assert_eq!(payload["algoStatus"], "CANCELED");
        assert_eq!(payload["algoId"], "7001");
        assert_eq!(payload["clientAlgoId"], "algo-client-ws");
        assert_eq!(payload["observed_at_ms"], 3);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b8_triggered_algo_update_binds_actual_order_id_only() {
        let (store, path) = test_store().await;
        seed_algo_update_identity_fixture(&store).await;
        let envelope = serde_json::from_str(
            r#"{"e":"ALGO_UPDATE","E":2,"T":3,"o":{"caid":"algo-client-ws","aid":7001,"X":"TRIGGERED","ai":8001}}"#,
        )
        .unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope("cred_p1a", &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        let row = sqlx::query(
            "SELECT exchange_algo_id, exchange_order_id, algo_status FROM protection_orders WHERE protection_order_id='prot_algo_ws'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("exchange_algo_id"), "7001");
        assert_eq!(row.get::<String, _>("exchange_order_id"), "8001");
        assert_eq!(row.get::<String, _>("algo_status"), "TRIGGERED");
        let engine_actual = sqlx::query(
            "SELECT exchange_order_id FROM engine_orders WHERE engine_order_id='eng_algo_ws'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("exchange_order_id");
        assert_eq!(engine_actual, "8001");
        cleanup(store, path).await;
    }

    fn intent(action: &str, cycle_id: &str, client_idem_key: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: "bot_p1a".to_string(),
            run_id: "run_p1a".to_string(),
            cycle_id: cycle_id.to_string(),
            idempotency_key: client_idem_key.to_string(),
            client_order_id_short: format!("coid_{cycle_id}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: action.to_string(),
            side: if action.ends_with("short") {
                "short".to_string()
            } else {
                "long".to_string()
            },
            order_type: "market".to_string(),
            price: "100".to_string(),
            quantity: "2".to_string(),
            reduce_only: action.starts_with("close"),
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        }
    }

    async fn execute_one(
        service: &EngineExecutionService,
        credential_id: &str,
        intent: OrderIntent,
    ) -> crate::proto::IntentAck {
        let response = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.to_string(),
                run_mode: "paper".to_string(),
                intents: vec![intent],
            }))
            .await
            .expect("execute intent")
            .into_inner();
        response.acks.into_iter().next().expect("one ack")
    }

    /// v2 驱动：把开仓走 v2 执行服务的等价路径。
    ///
    /// §7.4 之后生产端口 fail-closed 拒绝 v1 open，因此**以开仓为前置**的用例
    /// 必须改走 v2。这里刻意不复用 `execute_one` 再打补丁，而是另建一个驱动，
    /// 让「这条用例走的是哪条 wire」在调用点一眼可见——否则给共用 helper 挂上
    /// v2 上下文，会把所有 v1 用例的风控口径（typed_protection_capable、
    /// data_snapshot_capable，进而 legacy_sizing 是否激活）一并悄悄改掉。
    async fn execute_one_v2(
        service: &EngineExecutionService,
        credential_id: &str,
        intent: OrderIntent,
    ) -> crate::proto::IntentAck {
        let role = match intent.action.as_str() {
            "open_long" | "open_short" => OrderRole::Entry,
            _ => OrderRole::Exit,
        };
        let mut contexts = std::collections::HashMap::new();
        contexts.insert(
            intent.idempotency_key.clone(),
            V2IntentContext {
                risk: crate::risk::WireRiskContext {
                    typed_protection_capable: true,
                    data_snapshot_capable: true,
                    // 真实 v2 开仓携带数据快照；capable=true 而快照缺席会被
                    // require_data_snapshot 直接判 DATA_QUALITY_FAIL。
                    data_snapshot: matches!(role, OrderRole::Entry).then(|| {
                        risk_policy::DataSnapshot {
                            allow_open: true,
                            consistency_error: None,
                        }
                    }),
                    intent_id: format!("int_{}", intent.idempotency_key),
                    // 工厂策略对 v2 要求 typed protection（per_trade.
                    // protection_required=true 是 v2 cutover 的口径），而真实的
                    // v2 开仓本就携带 SL/TP。以 intent 的 price=100 为基准取
                    // 一组满足 min_rr 的保护价：多头 SL 95 / TP 110。
                    protection: matches!(role, OrderRole::Entry).then(|| {
                        let short = intent.action.ends_with("short");
                        crate::risk::TypedProtectionInput {
                            stop_loss_price: if short { "105".into() } else { "95".into() },
                            take_profit_price: if short { "90".into() } else { "110".into() },
                        }
                    }),
                    valid_until_ms: None,
                    max_slippage_bps: None,
                    config_hash: String::new(),
                    prompt_hash: String::new(),
                    policy_version: String::new(),
                    code_commit: String::new(),
                },
                order_role: role,
                // 风控门放行需要 protection 输入；是否**原子创建保护单**是另一件事。
                // 这些用例的开仓只是前置，保持与原 v1 路径一致的「不建保护单」，
                // 以免把 position_state 的终态从 open 悄悄改成 protected。
                protection_required: false,
                confirm_timeout_ms: crate::protection::DEFAULT_CONFIRM_TIMEOUT_MS,
                protection_generation: 1,
            },
        );
        let mut request = Request::new(ExecuteIntentsRequest {
            credential_id: credential_id.to_string(),
            run_mode: "paper".to_string(),
            intents: vec![intent],
        });
        request
            .extensions_mut()
            .insert(V2ExecutionContext { intents: contexts });
        let response = service
            .execute_intents(request)
            .await
            .expect("execute v2 intent")
            .into_inner();
        response.acks.into_iter().next().expect("one ack")
    }

    async fn insert_pending_engine_order(
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        intent: &OrderIntent,
        authority_idem_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (
              'order_pending_entry', 'cred_p1a', ?, ?,
              ?, ?, ?, ?, ?, ?,
              ?, 0, 'pending_submit', 1, 1
            )
            "#,
        )
        .bind(&intent.bot_uid)
        .bind(authority_idem_key)
        .bind(&intent.exchange)
        .bind(&intent.market_kind)
        .bind(&intent.symbol_key)
        .bind(&intent.action)
        .bind(&intent.side)
        .bind(&intent.order_type)
        .bind(&intent.quantity)
        .execute(&mut **tx)
        .await
        .expect("insert production-shaped pending engine order");
    }

    async fn insert_exchange_position(store: &Store, position_amt: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_positions (
              credential_id, exchange, market_kind, symbol, symbol_key,
              position_amt, entry_price, mark_price, leverage,
              unrealized_pnl, updated_at_ms
            ) VALUES (
              'cred_p1a', 'binance', 'futures', 'BTCUSDT',
              'binance|futures|BTC|USDT|USDT',
              ?, '100', '101', '1', '2', 1
            )
            "#,
        )
        .bind(position_amt)
        .execute(store.pool())
        .await
        .expect("insert exchange position truth without local position_state");
    }

    async fn insert_exchange_long_position(store: &Store) {
        insert_exchange_position(store, "2").await;
    }

    async fn assert_active_index_exists(store: &Store) {
        // Asserting the index *name* is not enough: downgrading the migration to a
        // plain `CREATE INDEX` keeps the name and would silently drop the only
        // database-level guard against duplicate active positions
        // (TRADE-SAFETY-2026-004). Assert the UNIQUE property itself.
        let row = sqlx::query(
            r#"
            SELECT sql FROM sqlite_master
            WHERE type = 'index' AND name = 'position_state_active_uniq'
            "#,
        )
        .fetch_optional(store.pool())
        .await
        .expect("query sqlite indexes");
        let row = row.expect("position_state_active_uniq must exist");
        let ddl = row.get::<String, _>("sql").to_uppercase();
        assert!(
            ddl.contains("UNIQUE"),
            "position_state_active_uniq must be UNIQUE, got: {ddl}"
        );
    }

    async fn insert_position(
        store: &Store,
        position_id: &str,
        epoch: i64,
        state: &str,
        active_key: Option<&str>,
        pending_idem_key: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (?, 'cred_p1a', 'bot_p1a',
                      'binance|futures|BTC|USDT|USDT', 'long',
                      ?, ?, ?, ?, '0', 1)
            "#,
        )
        .bind(position_id)
        .bind(epoch)
        .bind(state)
        .bind(active_key)
        .bind(pending_idem_key)
        .execute(store.pool())
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn migration_0005_runs_and_creates_active_unique_index() {
        let (store, path) = test_store().await;
        assert_active_index_exists(&store).await;
        let table = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'position_state'",
        )
        .fetch_optional(store.pool())
        .await
        .expect("query sqlite tables");
        assert!(table.is_some());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn migration_0006_runs_and_approved_verdict_unique_key_is_portable() {
        let (store, path) = test_store().await;
        for table_name in [
            "risk_verdicts",
            "notional_reservations",
            "risk_policies_snapshot",
        ] {
            let table =
                sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                    .bind(table_name)
                    .fetch_optional(store.pool())
                    .await
                    .expect("query migration table");
            assert!(table.is_some(), "missing table {table_name}");
        }
        let column = sqlx::query(
            "SELECT name FROM pragma_table_info('engine_orders') WHERE name = 'risk_verdict_id'",
        )
        .fetch_optional(store.pool())
        .await
        .expect("query engine_orders columns");
        assert!(column.is_some());

        async fn insert_verdict_row(
            store: &Store,
            verdict_id: &str,
            approved_key: Option<&str>,
            decision: &str,
        ) -> Result<(), sqlx::Error> {
            sqlx::query(
                r#"
                INSERT INTO risk_verdicts (
                  risk_verdict_id, intent_id, approved_intent_key,
                  credential_id, bot_uid, symbol_key, created_at_ms,
                  policy_version, policy_hash, decision, reject_reason_code,
                  sizing_json, rules_json, budget_as_of_ms, evaluator_version
                ) VALUES (?, 'same_intent', ?, 'cred', 'bot', 'symbol', 1,
                          'policy', 'sha256:test', ?, NULL, NULL, '[]', NULL, 'test')
                "#,
            )
            .bind(verdict_id)
            .bind(approved_key)
            .bind(decision)
            .execute(store.pool())
            .await
            .map(|_| ())
        }

        insert_verdict_row(&store, "rv_reject_1", None, "reject")
            .await
            .expect("first reject");
        insert_verdict_row(&store, "rv_reject_2", None, "reject")
            .await
            .expect("multiple NULL reject keys are append-only");
        insert_verdict_row(&store, "rv_approve_1", Some("same_intent"), "approve")
            .await
            .expect("first approval");
        let duplicate = insert_verdict_row(&store, "rv_approve_2", Some("same_intent"), "resize")
            .await
            .expect_err("second approving verdict must conflict");
        assert!(position_state::is_unique_violation(&duplicate));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn historical_risk_verdict_replays_exactly_from_persisted_input() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let ack = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_l5_s3", "idem_l5_s3"),
        )
        .await;
        assert_eq!(ack.status, "accepted", "{}", ack.message);

        let row = sqlx::query(
            r#"
            SELECT risk_verdict_id, intent_id,
                   CAST(created_at_ms AS TEXT) AS created_at_ms_text,
                   policy_version,
                   policy_hash, decision,
                   COALESCE(reject_reason_code, '') AS reject_reason_code,
                   sizing_json,
                   rules_json,
                   COALESCE(CAST(budget_as_of_ms AS TEXT), '') AS budget_as_of_ms_text,
                   evaluator_version,
                   replay_input_json
            FROM risk_verdicts
            -- §7.4 后生产端口只接受 v2 open，故本用例的裁决由 v2 wire 产生，
            -- intent_id 随之变化。主题不变：持久化裁决必须能从其持久化输入
            -- 逐字节重放；覆盖面由已关闭的 v1 wire 迁到当前生产 wire。
            WHERE intent_id = 'int_idem_l5_s3'
            LIMIT 1
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load historical verdict and replay input");
        let replay_input_json = row.get::<String, _>("replay_input_json");
        let replayed = crate::risk::replay_verdict(&replay_input_json)
            .expect("replay historical risk verdict");
        let replayed_json = serde_json::to_value(&replayed).expect("serialize replayed verdict");

        assert_eq!(
            replayed_json["risk_verdict_id"],
            row.get::<String, _>("risk_verdict_id")
        );
        assert_eq!(
            replayed_json["intent_id"],
            row.get::<String, _>("intent_id")
        );
        assert_eq!(
            replayed.created_at_ms,
            row.get::<String, _>("created_at_ms_text")
                .parse::<i64>()
                .expect("parse historical verdict timestamp")
        );
        assert_eq!(
            replayed_json["policy_version"],
            row.get::<String, _>("policy_version")
        );
        assert_eq!(
            replayed_json["policy_hash"],
            row.get::<String, _>("policy_hash")
        );
        assert_eq!(replayed_json["decision"], row.get::<String, _>("decision"));
        assert_eq!(
            replayed.reject_reason_code,
            match row.get::<String, _>("reject_reason_code") {
                value if value.is_empty() => None,
                value => Some(value),
            }
        );
        assert_eq!(
            replayed_json["sizing"],
            serde_json::from_str::<serde_json::Value>(&row.get::<String, _>("sizing_json"))
                .expect("decode historical sizing")
        );
        assert_eq!(
            replayed_json["rules"],
            serde_json::from_str::<serde_json::Value>(&row.get::<String, _>("rules_json"))
                .expect("decode historical rules")
        );
        assert_eq!(
            replayed.budget_as_of_ms,
            match row.get::<String, _>("budget_as_of_ms_text") {
                value if value.is_empty() => None,
                value => Some(
                    value
                        .parse::<i64>()
                        .expect("parse historical budget timestamp"),
                ),
            }
        );
        assert_eq!(
            replayed_json["evaluator_version"],
            row.get::<String, _>("evaluator_version")
        );

        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_same_version_old_factory_hash_rejects_open_but_executes_close() {
        let (store, path) = test_store().await;
        // A close with no local position is an orphan-recovery close. Its
        // quantity must come from credential-scoped exchange truth; the
        // policy-freshness rule under test must not be exercised with an
        // impossible, unquantifiable close fixture.
        insert_exchange_long_position(&store).await;
        let mut stale = risk_policy::factory_policy();
        stale.preconditions.require_data_snapshot = false;
        let stale_hash = risk_policy::policy_hash(&stale);
        let stale_json = serde_json::to_string(&stale).expect("serialize stale policy");
        sqlx::query(
            r#"
            INSERT INTO risk_policies_snapshot (
              policy_version, policy_hash, policy_json, schema_version,
              received_at_ms, activated_at_ms
            ) VALUES (?, ?, ?, 1, 1, 1)
            "#,
        )
        .bind(&stale.policy_version)
        .bind(stale_hash)
        .bind(stale_json)
        .execute(store.pool())
        .await
        .expect("persist old same-version policy");

        let service = EngineExecutionService::new(store.clone());
        let close = execute_one(
            &service,
            "cred_p1a",
            intent(
                "close_long",
                "cycle_stale_policy_close",
                "idem_stale_policy_close",
            ),
        )
        .await;
        assert_eq!(close.status, "accepted", "{}", close.message);

        let open = execute_one_v2(
            &service,
            "cred_p1a",
            intent(
                "open_long",
                "cycle_stale_policy_open",
                "idem_stale_policy_open",
            ),
        )
        .await;
        assert_eq!(open.status, "rejected");
        assert!(open.message.contains("STALE_FACTORY_POLICY"));

        let rows = sqlx::query(
            r#"
            SELECT intent_id, decision, reject_reason_code, rules_json
            FROM risk_verdicts
            ORDER BY created_at_ms, intent_id
            "#,
        )
        .fetch_all(store.pool())
        .await
        .expect("load stale policy verdict audit");
        assert_eq!(rows.len(), 2);
        let close_verdict = rows
            .iter()
            .find(|row| row.get::<String, _>("intent_id").contains("close"))
            .expect("close verdict");
        assert_eq!(close_verdict.get::<String, _>("decision"), "approve");
        let close_rules = close_verdict.get::<String, _>("rules_json");
        assert!(close_rules.contains("factory_policy_freshness"));
        assert!(close_rules.contains("not_applicable_to_close_or_hold"));
        let open_verdict = rows
            .iter()
            .find(|row| row.get::<String, _>("intent_id").contains("open"))
            .expect("open verdict");
        assert_eq!(open_verdict.get::<String, _>("decision"), "reject");
        assert_eq!(
            open_verdict.get::<Option<String>, _>("reject_reason_code"),
            Some("STALE_FACTORY_POLICY".to_string())
        );
        assert!(open_verdict
            .get::<String, _>("rules_json")
            .contains("factory_policy_freshness"));

        let order_count = sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count stale policy orders")
            .get::<i64, _>("n");
        assert_eq!(order_count, 1, "close must land; rejected open must not");
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn persisted_policy_hash_mismatch_rejects_open_but_executes_close() {
        let (store, path) = test_store().await;
        // Keep the close leg production-shaped under orphan recovery: policy
        // integrity is the variable under test, not whether a position exists.
        insert_exchange_long_position(&store).await;
        let policy = risk_policy::factory_policy();
        let policy_json = serde_json::to_string(&policy).expect("serialize policy");
        sqlx::query(
            r#"
            INSERT INTO risk_policies_snapshot (
              policy_version, policy_hash, policy_json, schema_version,
              received_at_ms, activated_at_ms
            ) VALUES (?, 'sha256:mismatched', ?, 1, 1, 1)
            "#,
        )
        .bind(&policy.policy_version)
        .bind(policy_json)
        .execute(store.pool())
        .await
        .expect("persist mismatched policy hash");

        let service = EngineExecutionService::new(store.clone());
        let close = execute_one(
            &service,
            "cred_p1a",
            intent(
                "close_long",
                "cycle_hash_mismatch_close",
                "idem_hash_mismatch_close",
            ),
        )
        .await;
        assert_eq!(close.status, "accepted", "{}", close.message);

        let open = execute_one_v2(
            &service,
            "cred_p1a",
            intent(
                "open_long",
                "cycle_hash_mismatch_open",
                "idem_hash_mismatch_open",
            ),
        )
        .await;
        assert_eq!(open.status, "rejected");
        assert!(open.message.contains("POLICY_HASH_MISMATCH"));

        let rows = sqlx::query(
            r#"
            SELECT intent_id, decision, reject_reason_code, rules_json
            FROM risk_verdicts
            ORDER BY created_at_ms, intent_id
            "#,
        )
        .fetch_all(store.pool())
        .await
        .expect("load hash mismatch verdict audit");
        assert_eq!(rows.len(), 2);
        let close_verdict = rows
            .iter()
            .find(|row| row.get::<String, _>("intent_id").contains("close"))
            .expect("close verdict");
        assert_eq!(close_verdict.get::<String, _>("decision"), "approve");
        let close_rules = close_verdict.get::<String, _>("rules_json");
        assert!(close_rules.contains("policy_hash_integrity"));
        assert!(close_rules.contains("not_applicable_to_close_or_hold"));
        let open_verdict = rows
            .iter()
            .find(|row| row.get::<String, _>("intent_id").contains("open"))
            .expect("open verdict");
        assert_eq!(open_verdict.get::<String, _>("decision"), "reject");
        assert_eq!(
            open_verdict.get::<Option<String>, _>("reject_reason_code"),
            Some("POLICY_HASH_MISMATCH".to_string())
        );

        let order_count = sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count mismatched policy orders")
            .get::<i64, _>("n");
        assert_eq!(order_count, 1, "close must land; rejected open must not");
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn current_factory_policy_executes_open_and_close_unchanged() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        let open = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_current_open", "idem_current_open"),
        )
        .await;
        assert_eq!(open.status, "accepted", "{}", open.message);

        let close = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_current_close", "idem_current_close"),
        )
        .await;
        assert_eq!(close.status, "accepted", "{}", close.message);

        // 开仓走 v2（§7.4 后生产端口只接受 v2 open），与 SL/TP 保护单原子化，
        // 故总行数为 4。本用例钉的是「当前工厂策略下开、平各恰好一张」，
        // 用按 role 计数表达比总行数更贴主题，也不受保护单腿数变化影响。
        let role_count = |role: &'static str| {
            let pool = store.pool().clone();
            async move {
                sqlx::query("SELECT COUNT(*) AS n FROM engine_orders WHERE order_role = ?")
                    .bind(role)
                    .fetch_one(&pool)
                    .await
                    .expect("count orders by role")
                    .get::<i64, _>("n")
            }
        };
        assert_eq!(role_count("entry").await, 1);
        assert_eq!(role_count("exit").await, 1);
        drop(service);
        cleanup(store, path).await;
    }

    async fn evaluate_at(
        store: &Store,
        intent: &OrderIntent,
        now_ms: i64,
    ) -> risk_policy::RiskVerdict {
        evaluate_at_for_credential(store, "cred_p1a", intent, now_ms).await
    }

    async fn evaluate_at_for_credential(
        store: &Store,
        credential_id: &str,
        intent: &OrderIntent,
        now_ms: i64,
    ) -> risk_policy::RiskVerdict {
        let mut tx = store.pool().begin().await.expect("begin risk evaluation");
        let (verdict, _, _) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            credential_id,
            intent,
            now_ms,
            None,
            "paper",
            false,
            crate::protection::infer_v1_order_role(intent),
            None,
        )
        .await
        .expect("evaluate v1 risk");
        tx.rollback().await.expect("rollback read-only evaluation");
        verdict
    }

    async fn evaluate_incomplete_fill_recovery_rule(
        store: &Store,
        action: &str,
        reduce_only: bool,
        order_role: OrderRole,
    ) -> serde_json::Value {
        let mut candidate = intent(action, "cycle_fill_guard", "idem_fill_guard");
        candidate.reduce_only = reduce_only;
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin fill guard evaluation");
        let (verdict, _, _) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_p1a",
            &candidate,
            super::now_ms(),
            None,
            "demo",
            true,
            order_role,
            None,
        )
        .await
        .expect("evaluate incomplete fill recovery guard");
        tx.rollback().await.expect("rollback fill guard evaluation");
        serde_json::from_str::<Vec<serde_json::Value>>(&verdict.rules_json())
            .expect("decode rule audit")
            .into_iter()
            .find(|rule| {
                rule.get("rule").and_then(|value| value.as_str())
                    == Some(crate::fill_replay::FILL_RECOVERY_RULE)
            })
            .expect("fill recovery rule must be audited")
    }

    fn expected_skipped_fill_recovery_rule(reason: &str) -> serde_json::Value {
        serde_json::json!({
            "rule": crate::fill_replay::FILL_RECOVERY_RULE,
            "result": "skipped",
            "reason": reason,
            "detail": "credential=cred_p1a symbol_key=binance|futures|BTC|USDT|USDT generation=0 phase=not_started",
        })
    }

    /// 以 **v1 wire 形状**（`wire: None`）直接跑风控评估。
    ///
    /// §7.4 之后生产端口 fail-closed 拒绝 v1 open，因此「v1 开仓的风控口径」
    /// 无法再经 execute_intents 取证。但该口径本身仍然存在且要紧——
    /// `legacy_sizing`（调用方定量）只在 `!typed_protection_capable` 且 open 时
    /// 激活，历史裁决的重放也走它。故把这些用例下沉到风控层，**主题不变**：
    /// 仍钉 legacy_client_quantity 的 resize-not-bypass 与 wire_not_capable 审计。
    /// 被关闭的是 wire，不是这条风控口径。
    async fn evaluate_v1_open(
        store: &Store,
        intent: &OrderIntent,
    ) -> (risk_policy::Decision, serde_json::Value, serde_json::Value) {
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin v1 open evaluation");
        let (verdict, _reservation_cap, _replay_input_json) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_p1a",
            intent,
            super::now_ms(),
            None,
            "paper",
            false,
            OrderRole::Entry,
            None,
        )
        .await
        .expect("evaluate v1 open");
        tx.commit().await.expect("commit v1 open evaluation");
        // 与持久化路径同源：落库的 sizing_json / rules_json 就取自 verdict 自身。
        let rules = serde_json::from_str(&verdict.rules_json()).expect("decode rules audit");
        let sizing = verdict
            .sizing_json()
            .map(|json| serde_json::from_str(&json).expect("decode sizing audit"))
            .unwrap_or(serde_json::Value::Null);
        (verdict.decision, sizing, rules)
    }

    /// 降级的**两个来源**。段③ 批 0 之前它们共用一个槽位，那正是
    /// H-01/M-04 的根因；现在各有一列，放行面必须在**两个**来源下逐条成立。
    #[derive(Clone, Copy, Debug)]
    enum DegradeSource {
        /// 熔断器：`degraded` + `degraded_reason`（`record_credential_error` 独占）。
        CircuitBreaker,
        /// 对账差异未消解：`reconcile_diff_open`（对账独占）。
        ReconcileDiffOpen,
    }

    async fn seed_degraded_credential_health(store: &Store, source: DegradeSource) {
        sqlx::query("DELETE FROM engine_credential_health WHERE credential_id='cred_p1a'")
            .execute(store.pool())
            .await
            .expect("clear credential health");
        let (degraded, reason, diff_open) = match source {
            DegradeSource::CircuitBreaker => (1i64, Some("binance -1021 timestamp"), 0i64),
            DegradeSource::ReconcileDiffOpen => (0i64, None, 1i64),
        };
        sqlx::query(
            r#"
            INSERT INTO engine_credential_health (
              credential_id, degraded, degraded_reason, consecutive_errors,
              first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
            ) VALUES ('cred_p1a', ?, ?, 0, 0, 0, 1, ?)
            "#,
        )
        .bind(degraded)
        .bind(reason)
        .bind(diff_open)
        .execute(store.pool())
        .await
        .expect("seed degraded credential health");
    }

    async fn evaluate_manage_only_behavior(
        store: &Store,
        action: &str,
        reduce_only: bool,
        order_role: OrderRole,
    ) -> (risk_policy::Decision, Option<String>, serde_json::Value) {
        evaluate_manage_only_behavior_from(
            store,
            DegradeSource::CircuitBreaker,
            action,
            reduce_only,
            order_role,
        )
        .await
    }

    async fn evaluate_manage_only_behavior_from(
        store: &Store,
        source: DegradeSource,
        action: &str,
        reduce_only: bool,
        order_role: OrderRole,
    ) -> (risk_policy::Decision, Option<String>, serde_json::Value) {
        seed_degraded_credential_health(store, source).await;
        let mut candidate = intent(action, "cycle_t310_manage_only", "idem_t310_manage_only");
        candidate.reduce_only = reduce_only;
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin manage_only evaluation");
        let (verdict, _, _) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_p1a",
            &candidate,
            super::now_ms(),
            None,
            "paper",
            false,
            order_role,
            None,
        )
        .await
        .expect("evaluate manage_only behavior");
        tx.rollback()
            .await
            .expect("rollback manage_only evaluation");
        let rule = serde_json::from_str::<Vec<serde_json::Value>>(&verdict.rules_json())
            .expect("decode manage_only audit")
            .into_iter()
            .find(|rule| {
                rule.get("rule").and_then(|value| value.as_str())
                    == Some("credential_degrade_order_guard")
            })
            .expect("manage_only order guard must be audited");
        (verdict.decision, verdict.reject_reason_code, rule)
    }

    fn expected_skipped_manage_only_rule(reason: &str) -> serde_json::Value {
        serde_json::json!({
            "rule": "credential_degrade_order_guard",
            "result": "skipped",
            "reason": reason,
            "detail": "credential=cred_p1a mode=manage_only",
        })
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_rejects_new_entry() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_manage_only_behavior(&store, "open_long", false, OrderRole::Entry).await,
            (
                risk_policy::Decision::Reject,
                Some("DEGRADED_CREDENTIAL".to_string()),
                serde_json::json!({
                    "rule": "credential_degrade_order_guard",
                    "result": "reject",
                    "reason_code": "DEGRADED_CREDENTIAL",
                    "detail": "credential=cred_p1a mode=manage_only",
                }),
            )
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_does_not_block_close() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_manage_only_behavior(&store, "close_long", false, OrderRole::Exit).await,
            (
                risk_policy::Decision::Approve,
                None,
                expected_skipped_manage_only_rule("close_must_remain_available"),
            )
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_does_not_block_hold() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_manage_only_behavior(&store, "hold", false, OrderRole::Entry).await,
            (
                risk_policy::Decision::Approve,
                None,
                expected_skipped_manage_only_rule("hold_has_no_exchange_exposure"),
            )
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_does_not_block_reduce_only() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_manage_only_behavior(&store, "open_long", true, OrderRole::Entry).await,
            (
                risk_policy::Decision::Approve,
                None,
                expected_skipped_manage_only_rule("reduce_only_must_remain_available"),
            )
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_does_not_block_sl_or_tp_protection() {
        let (store, path) = test_store().await;
        let mut actual = Vec::new();
        for role in [OrderRole::ProtectionSl, OrderRole::ProtectionTp] {
            actual.push(evaluate_manage_only_behavior(&store, "open_long", false, role).await);
        }
        assert_eq!(
            actual,
            vec![
                (
                    risk_policy::Decision::Approve,
                    None,
                    expected_skipped_manage_only_rule("protection_must_remain_available"),
                ),
                (
                    risk_policy::Decision::Approve,
                    None,
                    expected_skipped_manage_only_rule("protection_must_remain_available"),
                ),
            ]
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t310_reconcile_manage_only_does_not_block_l3_flatten() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_manage_only_behavior(&store, "open_long", false, OrderRole::Emergency).await,
            (
                risk_policy::Decision::Approve,
                None,
                expected_skipped_manage_only_rule("l3_flatten_must_remain_available"),
            )
        );
        cleanup(store, path).await;
    }

    // ------------------------------------------------------------------
    // 段③ 批 0 · H-01（High）/ M-04（Medium）行为回归
    //
    // 段②完成评审 §4「段①域移交」的两条同根因缺陷：`engine_credential_health`
    // 只有一个降级槽位，**熔断器**与**对账差异**抢它，而置位按标志、解除按
    // 原因的不对称让两条事实互相掩盖。下面几条用例把评审的 5 步（H-01）与
    // 4 步（M-04）实证场景钉成回归，并补上评审点名的测试盲区：
    // 「reconcile 降级与交易所失败计数叠在同一个凭据上」。
    //
    // 全部用**真实 SQLite + 真实迁移 + 真实 `EngineExecutionService` + 真实
    // 对账轮次**（`reconcile_binance_futures_truth_with_client`，交易所 REST
    // 由响应队列桩住），闸门读点走**生产函数** `crate::risk::evaluate_v1`
    // ——不复制 `risk.rs` 的 SQL：复制出来的读点证明不了生产读点会执行。
    // ------------------------------------------------------------------

    /// 一轮**真实**对账。`has_diff=true` 时交易所报一张本地不认识的开放单
    /// （`missing_open_orders`）；否则交易所侧全空 ⇒ 零差异。
    async fn run_reconcile_round(store: &Store, credential_id: &str, has_diff: bool) {
        let exchange_open_orders = if has_diff {
            serde_json::json!([{
                "orderId": 9001,
                "clientOrderId": "coid-unknown-to-us",
                "symbol": "ETHUSDT",
                "status": "NEW",
                "updateTime": 5
            }])
            .to_string()
        } else {
            "[]".to_string()
        };
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![
                (200, exchange_open_orders),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
                (200, "[]".to_string()),
            ],
        );
        let limiter = crate::rate_limiter::CredentialRateLimiter::new(
            crate::rate_limiter::TokenBucketConfig::for_test(50.0, 50.0),
        );
        crate::reconcile::reconcile_binance_futures_truth_with_client(
            credential_id,
            store.clone(),
            limiter,
            &client,
        )
        .await
        .expect("the reconcile round must complete");
    }

    /// 健康行的**形状**。`reconcileDiffOpen` 取 `-1` 表示 `NULL`
    /// （0029 之前的存量行语义：本列从未被写过）。
    async fn credential_health_shape(store: &Store, credential_id: &str) -> serde_json::Value {
        let row = sqlx::query(
            "SELECT degraded, consecutive_errors, \
             CASE WHEN degraded_reason IS NULL THEN 1 ELSE 0 END AS reason_is_null, \
             COALESCE(reconcile_diff_open, -1) AS reconcile_diff_open \
             FROM engine_credential_health WHERE credential_id = ? LIMIT 1",
        )
        .bind(credential_id)
        .fetch_optional(store.pool())
        .await
        .expect("load credential health shape");
        match row {
            None => serde_json::Value::Null,
            Some(row) => serde_json::json!({
                "degraded": row.get::<i64, _>("degraded"),
                "degradedReasonIsNull": row.get::<i64, _>("reason_is_null"),
                "consecutiveErrors": row.get::<i64, _>("consecutive_errors"),
                "reconcileDiffOpen": row.get::<i64, _>("reconcile_diff_open"),
            }),
        }
    }

    /// **生产读点**：`crate::risk::evaluate_v1` 里那一条 credential health 查询。
    async fn entry_verdict(store: &Store, action: &str, order_role: OrderRole) -> String {
        let candidate = intent(action, "cycle_seg3_b0", "idem_seg3_b0");
        let mut tx = store.pool().begin().await.expect("begin gate read");
        let (verdict, _, _) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_p1a",
            &candidate,
            super::now_ms(),
            None,
            "paper",
            false,
            order_role,
            None,
        )
        .await
        .expect("evaluate the production credential health gate");
        tx.rollback().await.expect("rollback gate read");
        match (verdict.decision, verdict.reject_reason_code) {
            (risk_policy::Decision::Approve, _) => "approve".to_string(),
            (risk_policy::Decision::Reject, Some(code)) => format!("reject:{code}"),
            (decision, code) => format!("{decision:?}:{code:?}"),
        }
    }

    async fn entry_blocked(store: &Store) -> bool {
        entry_verdict(store, "open_long", OrderRole::Entry).await == "reject:DEGRADED_CREDENTIAL"
    }

    /// **H-01（High，engine safety contract「对账失败后继续交易」）· 评审 5 步实证的回归。**
    ///
    /// 旧实现：交易所错误先把 `degraded` 打成 1 并占住唯一的 `degraded_reason`
    /// 槽位 ⇒ 对账差异到来时置位守卫 `WHEN degraded = 0` 不命中，差异**一个字
    /// 都没写进健康行** ⇒ `clear_credential_errors` 的保护守卫判
    /// `degraded_reason='reconcile_diff'`（判的正是那个没写进去的值）不命中
    /// ⇒ 一次成功 ACK 把整行清零 ⇒ **差异未消解却恢复放行**。
    ///
    /// 差异现在有自己的列，两条事实不再共用槽位。
    #[tokio::test]
    async fn h01_a_masked_reconcile_diff_survives_the_exchange_ack_clear() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        // step1 —— 连续 5 次交易所失败，熔断器跳闸。
        for _ in 0..5 {
            service
                .record_credential_error("cred_p1a", "binance -1021 timestamp for this request")
                .await
                .expect("record the exchange error");
        }
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 1,
                "degradedReasonIsNull": 0,
                "consecutiveErrors": 5,
                "reconcileDiffOpen": 0,
            }),
            "step1: the breaker owns degraded/degraded_reason and must not touch the diff column"
        );

        // step2 —— 对账发现差异。旧实现在这一步健康行**零痕迹**。
        run_reconcile_round(&store, "cred_p1a", true).await;
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 1,
                "degradedReasonIsNull": 0,
                "consecutiveErrors": 5,
                "reconcileDiffOpen": 1,
            }),
            "step2: an in-flight exchange error string must not swallow the reconcile diff"
        );

        // step3 —— 降级期间一张被允许的平仓/保护单 ACK 成功，清零熔断窗口。
        service
            .clear_credential_errors("cred_p1a")
            .await
            .expect("clear the credential error window after an exchange ack");
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": 1,
            }),
            "step3: an exchange ack clears the breaker and must NOT clear an unresolved diff"
        );

        // step4 —— 生产闸门读点：差异未消解 ⇒ 开仓仍然被拦。
        assert!(
            entry_blocked(&store).await,
            "step4: an unresolved reconcile diff must keep the entry gate closed"
        );

        // 放行面（engine safety contract / `credential_degrade_guard_disposition`）：
        // 降级只拦开仓，close / reduce-only / 保护单 / 应急单一律放行。
        assert_eq!(
            entry_verdict(&store, "close_long", OrderRole::Exit).await,
            "approve",
            "an unresolved reconcile diff must never block a close"
        );
        assert_eq!(
            entry_verdict(&store, "open_long", OrderRole::Emergency).await,
            "approve",
            "an unresolved reconcile diff must never block an L3 flatten"
        );

        // step5 —— 下一轮零差异对账用真值把它收敛成 0，开仓恢复。
        run_reconcile_round(&store, "cred_p1a", false).await;
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": 0,
            }),
            "step5: a round that observed zero diff writes an explicit 0, not an absence"
        );
        assert!(
            !entry_blocked(&store).await,
            "step5: a resolved diff on a healthy credential must let entries through again"
        );
        cleanup(store, path).await;
    }

    /// **M-04（Medium）· 评审 4 步实证的回归。**
    ///
    /// 旧实现：对账降级先占住 `degraded` 标志 ⇒ `record_credential_error` 的
    /// `WHEN degraded <> 0 THEN degraded` **永不**写 `degraded`，熔断器本该
    /// 跳闸却没跳 ⇒ 下一轮无差异的 auto-clear 只看 `degraded_reason`、不看
    /// 计数，把标志抹成 0。净效果：窗口内确实 5 次失败，闸门却是 `degraded=0`。
    ///
    /// 解耦之后对账再也不碰 `degraded`，熔断器的置位不可能被它挡住。
    #[tokio::test]
    async fn m04_a_reconcile_diff_no_longer_masks_the_circuit_breaker() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        // step1 —— 对账差异置位（**不**碰熔断器的两列）。
        run_reconcile_round(&store, "cred_p1a", true).await;
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": 1,
            }),
            "step1: a reconcile diff writes its own column only"
        );

        // step2 —— 期间 5 次提交失败：熔断器必须**真的**跳闸。
        for _ in 0..5 {
            service
                .record_credential_error("cred_p1a", "binance -1021 timestamp for this request")
                .await
                .expect("record the exchange error");
        }
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 1,
                "degradedReasonIsNull": 0,
                "consecutiveErrors": 5,
                "reconcileDiffOpen": 1,
            }),
            "step2: five failures inside the window must trip the breaker even while a diff is open"
        );

        // step3 —— 下一轮无差异：只解除差异，**不动**熔断器。
        run_reconcile_round(&store, "cred_p1a", false).await;
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 1,
                "degradedReasonIsNull": 0,
                "consecutiveErrors": 5,
                "reconcileDiffOpen": 0,
            }),
            "step3: clearing the diff must not clear a tripped breaker"
        );

        // step4 —— 两个读侧不得再分叉。
        assert!(
            entry_blocked(&store).await,
            "step4: the tripped breaker must keep the entry gate closed"
        );
        assert!(
            service
                .is_credential_degraded("cred_p1a")
                .await
                .expect("read credential health"),
            "step4: the audit read side must agree with the gate"
        );
        cleanup(store, path).await;
    }

    /// **评审点名的测试盲区**：reconcile 降级与交易所失败计数**叠在同一个
    /// 凭据上**。两条清除路径必须各清各的，且任意一条仍在时闸门保持关闭。
    #[tokio::test]
    async fn seg3_b0_overlaid_diff_and_breaker_clear_strictly_independently() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        run_reconcile_round(&store, "cred_p1a", true).await;
        for _ in 0..5 {
            service
                .record_credential_error("cred_p1a", "binance -1021 timestamp for this request")
                .await
                .expect("record the exchange error");
        }
        assert!(
            entry_blocked(&store).await,
            "both facts set: the gate is closed"
        );

        // 只清熔断器（一次交易所 ACK）——差异还在，闸门必须保持关闭。
        service
            .clear_credential_errors("cred_p1a")
            .await
            .expect("clear the breaker");
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": 1,
            })
        );
        assert!(
            entry_blocked(&store).await,
            "clearing only the breaker must leave the unresolved diff blocking"
        );

        // 反向：差异先消解、熔断器仍跳着——闸门同样必须保持关闭。
        run_reconcile_round(&store, "cred_p1a", true).await;
        for _ in 0..5 {
            service
                .record_credential_error("cred_p1a", "binance -1021 timestamp for this request")
                .await
                .expect("record the exchange error");
        }
        run_reconcile_round(&store, "cred_p1a", false).await;
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 1,
                "degradedReasonIsNull": 0,
                "consecutiveErrors": 5,
                "reconcileDiffOpen": 0,
            })
        );
        assert!(
            entry_blocked(&store).await,
            "clearing only the diff must leave the tripped breaker blocking"
        );

        // 两条都清干净了才放行——防"永远 degraded"的伪实现。
        service
            .clear_credential_errors("cred_p1a")
            .await
            .expect("clear the breaker");
        assert!(
            !entry_blocked(&store).await,
            "with both facts cleared the entry gate must open again"
        );
        cleanup(store, path).await;
    }

    /// **迁移 0029 的存量行在闸门上 fail-closed。**
    ///
    /// 0029 零 DML，存量行 `reconcile_diff_open IS NULL`（"本列从未被写过"）。
    /// 被掩盖的差异不可从库里恢复，所以"证不出没有差异"在开仓这个判据上等价
    /// 于"有差异"（engine safety contract）。放行面同样必须成立。
    #[tokio::test]
    async fn seg3_b0_a_legacy_null_reconcile_diff_column_fails_closed_on_entry_only() {
        let (store, path) = test_store().await;
        sqlx::query(
            "INSERT INTO engine_credential_health (credential_id, degraded, degraded_reason, \
             consecutive_errors, first_error_ms, last_error_ms, updated_at_ms) \
             VALUES ('cred_p1a', 0, NULL, 0, 0, 0, 1)",
        )
        .execute(store.pool())
        .await
        .expect("seed a pre-0029 legacy health row");
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": -1,
            }),
            "the legacy row must really be NULL, otherwise this case proves nothing"
        );
        assert!(
            entry_blocked(&store).await,
            "a legacy row whose diff state was never written must not open the entry gate"
        );
        assert_eq!(
            entry_verdict(&store, "close_long", OrderRole::Exit).await,
            "approve",
            "fail-closed on entry must never spill onto the close path"
        );
        assert!(
            EngineExecutionService::new(store.clone())
                .is_credential_degraded("cred_p1a")
                .await
                .expect("read credential health"),
            "the audit read side must fail closed on a legacy row too"
        );

        // 收敛：一轮真实的零差异对账写下显式 0，开仓恢复。
        run_reconcile_round(&store, "cred_p1a", false).await;
        assert!(
            !entry_blocked(&store).await,
            "the next reconcile round must converge the legacy unknown with real truth"
        );
        cleanup(store, path).await;
    }

    /// **放行面 · 变异 M9 存活补测。**
    ///
    /// 健康行有两个"建行者"——熔断器计数（`record_credential_error`）与 ACK
    /// 清零（`clear_credential_errors`）。两者都**不得**凭空声称"这个凭据有
    /// 未消解的对账差异"：没有行 ⇒ 这个凭据从来没有过 `degraded=1` ⇒ 不可能
    /// 发生 H-01 那种掩盖，所以 `reconcile_diff_open` 的诚实初值是显式的 0
    /// ——不是 1（会白拦开仓直到下一轮对账），也不是 `NULL`（那是存量行的
    /// "从未被写过"，同样 fail-closed）。
    ///
    /// 第一轮变异实测：把任一处建行值改成 1，全部现有用例都杀不掉它
    /// （它们的健康行都是先由对账轮次建的，两处 `ON CONFLICT` 走不到）。
    #[tokio::test]
    async fn seg3_b0_a_fresh_health_row_never_claims_an_unresolved_diff() {
        // (a) 这个凭据的第一件事是一次交易所失败（远未到熔断阈值）。
        let (store, path) = test_store().await;
        EngineExecutionService::new(store.clone())
            .record_credential_error("cred_p1a", "binance -1021 timestamp for this request")
            .await
            .expect("record the first ever exchange error");
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 1,
                "reconcileDiffOpen": 0,
            }),
            "the breaker creating the row must not invent a reconcile diff"
        );
        assert!(
            !entry_blocked(&store).await,
            "one exchange error below the breaker threshold must not close the entry gate"
        );
        cleanup(store, path).await;

        // (b) 这个凭据的第一件事是一次成功 ACK 清零。
        let (store, path) = test_store().await;
        EngineExecutionService::new(store.clone())
            .clear_credential_errors("cred_p1a")
            .await
            .expect("clear on the first ever exchange ack");
        assert_eq!(
            credential_health_shape(&store, "cred_p1a").await,
            serde_json::json!({
                "degraded": 0,
                "degradedReasonIsNull": 1,
                "consecutiveErrors": 0,
                "reconcileDiffOpen": 0,
            }),
            "the ack clear creating the row must not invent a reconcile diff"
        );
        assert!(
            !entry_blocked(&store).await,
            "a successful ack on a fresh credential must not close the entry gate"
        );
        cleanup(store, path).await;
    }

    /// 放行面（`reconcile_diff_open` 这一侧）：`credential_degrade_guard_disposition`
    /// 的六条处置在**新的降级来源**下逐条不被误伤。
    #[tokio::test]
    async fn seg3_b0_reconcile_diff_open_blocks_entry_only() {
        let (store, path) = test_store().await;
        let mut actual = Vec::new();
        for (action, reduce_only, role) in [
            ("open_long", false, OrderRole::Entry),
            ("close_long", false, OrderRole::Exit),
            ("hold", false, OrderRole::Entry),
            ("open_long", true, OrderRole::Entry),
            ("open_long", false, OrderRole::ProtectionSl),
            ("open_long", false, OrderRole::ProtectionTp),
            ("open_long", false, OrderRole::Emergency),
        ] {
            let (decision, code, rule) = evaluate_manage_only_behavior_from(
                &store,
                DegradeSource::ReconcileDiffOpen,
                action,
                reduce_only,
                role,
            )
            .await;
            actual.push(serde_json::json!({
                "decision": format!("{decision:?}"),
                "rejectReasonCode": code,
                "result": rule.get("result").and_then(|value| value.as_str()),
                "reason": rule
                    .get("reason")
                    .or_else(|| rule.get("reason_code"))
                    .and_then(|value| value.as_str()),
            }));
        }
        assert_eq!(
            serde_json::Value::Array(actual),
            serde_json::json!([
                {"decision":"Reject","rejectReasonCode":"DEGRADED_CREDENTIAL",
                 "result":"reject","reason":"DEGRADED_CREDENTIAL"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"close_must_remain_available"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"hold_has_no_exchange_exposure"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"reduce_only_must_remain_available"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"protection_must_remain_available"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"protection_must_remain_available"},
                {"decision":"Approve","rejectReasonCode":null,
                 "result":"skipped","reason":"l3_flatten_must_remain_available"},
            ]),
            "reconcile_diff_open must reject entries and nothing else"
        );

        // 放行面自证：差异消解且熔断器健康时，同一条开仓照常放行——
        // 否则这条用例用一个恒拒的实现也能通过。
        sqlx::query(
            "UPDATE engine_credential_health SET reconcile_diff_open = 0 \
             WHERE credential_id = 'cred_p1a'",
        )
        .execute(store.pool())
        .await
        .expect("resolve the reconcile diff");
        assert!(
            !entry_blocked(&store).await,
            "a resolved diff on a healthy credential must approve the very same entry"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_incomplete_fill_recovery_rejects_entry_with_audited_code() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(&store, "open_long", false, OrderRole::Entry)
                .await,
            serde_json::json!({
                "rule": crate::fill_replay::FILL_RECOVERY_RULE,
                "result": "reject",
                "reason_code": crate::fill_replay::FILL_RECOVERY_REASON_CODE,
                "detail": "credential=cred_p1a symbol_key=binance|futures|BTC|USDT|USDT generation=0 phase=not_started",
            })
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_successful_backfill_and_reconcile_remove_the_open_guard() {
        let (store, path) = test_store().await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            "cred_p1a",
            crate::fill_replay::RecoveryTrigger::Startup,
            10,
        )
        .await
        .expect("begin completed recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            &store,
            "cred_p1a",
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            cycle,
            20,
        )
        .await
        .expect("mark completed recovery ready");
        let candidate = intent("open_long", "cycle_fill_ready", "idem_fill_ready");
        let mut tx = store.pool().begin().await.expect("begin ready evaluation");
        let (verdict, _, _) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_p1a",
            &candidate,
            super::now_ms(),
            None,
            "demo",
            true,
            OrderRole::Entry,
            None,
        )
        .await
        .expect("evaluate ready fill recovery");
        tx.rollback().await.expect("rollback ready evaluation");
        let fill_rules = serde_json::from_str::<Vec<serde_json::Value>>(&verdict.rules_json())
            .expect("decode ready rules")
            .into_iter()
            .filter(|rule| {
                rule.get("rule").and_then(|value| value.as_str())
                    == Some(crate::fill_replay::FILL_RECOVERY_RULE)
            })
            .collect::<Vec<_>>();
        assert_eq!(fill_rules, Vec::<serde_json::Value>::new());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_fill_recovery_guard_does_not_reject_close_behavior() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(&store, "close_long", false, OrderRole::Exit)
                .await,
            expected_skipped_fill_recovery_rule("close_must_remain_available")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_fill_recovery_guard_does_not_reject_hold_behavior() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(&store, "hold", false, OrderRole::Entry).await,
            expected_skipped_fill_recovery_rule("hold_has_no_exchange_exposure")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_fill_recovery_guard_does_not_reject_reduce_only_behavior() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(&store, "open_long", true, OrderRole::Entry)
                .await,
            expected_skipped_fill_recovery_rule("reduce_only_must_remain_available")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_fill_recovery_guard_does_not_reject_protection_behavior() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(
                &store,
                "open_long",
                false,
                OrderRole::ProtectionSl
            )
            .await,
            expected_skipped_fill_recovery_rule("protection_must_remain_available")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn t39b_fill_recovery_guard_does_not_reject_l3_flatten_behavior() {
        let (store, path) = test_store().await;
        assert_eq!(
            evaluate_incomplete_fill_recovery_rule(
                &store,
                "open_long",
                false,
                OrderRole::Emergency
            )
            .await,
            expected_skipped_fill_recovery_rule("l3_flatten_must_remain_available")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn risk_budget_freshness_dod_rejects_stale_open_but_allows_reduce_only_close() {
        let (store, path) = test_store().await;
        let budget_as_of_ms = 1_000_000i64;
        sqlx::query("UPDATE risk_budget SET as_of_ms = ? WHERE credential_id = 'cred_p1a'")
            .bind(budget_as_of_ms)
            .execute(store.pool())
            .await
            .expect("pin budget clock");

        let open = intent("open_long", "cycle_budget_open", "idem_budget_open");
        let fresh = evaluate_at(&store, &open, budget_as_of_ms).await;
        assert_eq!(fresh.decision, risk_policy::Decision::Approve);

        // The crate contract is inclusive: age == max_age remains fresh.
        let boundary = evaluate_at(&store, &open, budget_as_of_ms + 30_000).await;
        assert_eq!(boundary.decision, risk_policy::Decision::Approve);

        let stale_now_ms = budget_as_of_ms + 30_001;
        let stale_open = evaluate_at(&store, &open, stale_now_ms).await;
        assert_eq!(stale_open.decision, risk_policy::Decision::Reject);
        assert_eq!(
            stale_open.reject_reason_code.as_deref(),
            Some("STALE_RISK_BUDGET")
        );

        let close = intent("close_long", "cycle_budget_close", "idem_budget_close");
        let stale_close = evaluate_at(&store, &close, stale_now_ms).await;
        assert_eq!(stale_close.decision, risk_policy::Decision::Approve);
        let freshness_rule = stale_close
            .rules
            .iter()
            .find(|rule| rule.rule == "risk_budget_freshness")
            .expect("freshness rule audit");
        assert_eq!(freshness_rule.result, risk_policy::RuleOutcome::Skipped);
        assert_eq!(
            freshness_rule.reason.as_deref(),
            Some("not_applicable_to_close_or_hold")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn missing_risk_budget_rejects_open_but_allows_reduce_only_close() {
        let (store, path) = test_store().await;
        sqlx::query("DELETE FROM risk_budget WHERE credential_id = 'cred_p1a'")
            .execute(store.pool())
            .await
            .expect("remove risk budget fixture");

        let open = evaluate_at(
            &store,
            &intent("open_long", "cycle_no_budget", "idem_no_budget"),
            1_000_000,
        )
        .await;
        assert_eq!(open.decision, risk_policy::Decision::Reject);
        assert_eq!(
            open.reject_reason_code.as_deref(),
            Some("STALE_RISK_BUDGET")
        );

        let close = evaluate_at(
            &store,
            &intent(
                "close_long",
                "cycle_no_budget_close",
                "idem_no_budget_close",
            ),
            1_000_000,
        )
        .await;
        assert_eq!(close.decision, risk_policy::Decision::Approve);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn product_demo_namespaced_credential_without_budget_rejects_open() {
        let (store, path) = test_store().await;
        let open = evaluate_at_for_credential(
            &store,
            "paper:7b6f4495fde3706c999256af2985a8f4",
            &intent("open_long", "cycle_demo_no_budget", "idem_demo_no_budget"),
            1_000_000,
        )
        .await;

        assert_eq!(open.decision, risk_policy::Decision::Reject);
        assert_eq!(
            open.reject_reason_code.as_deref(),
            Some("STALE_RISK_BUDGET")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn seeded_global_none_approves_open_and_pause_new_rejects_open_but_allows_close() {
        let (store, path) = test_store().await;
        let open = intent("open_long", "cycle_kill_none", "idem_kill_none");
        let none = evaluate_at(&store, &open, super::now_ms()).await;
        assert_eq!(
            none.decision,
            risk_policy::Decision::Approve,
            "{}",
            none.rules_json()
        );

        sqlx::query(
            "UPDATE kill_switch_state SET level='pause_new', reason='test', trigger_source='manual_api', armed_at_ms=1, updated_at_ms=1 WHERE scope_key='global'",
        )
        .execute(store.pool())
        .await
        .expect("arm global pause_new");
        let blocked = evaluate_at(&store, &open, super::now_ms()).await;
        assert_eq!(blocked.decision, risk_policy::Decision::Reject);
        assert_eq!(
            blocked.reject_reason_code.as_deref(),
            Some("KILL_SWITCH_ACTIVE")
        );
        assert!(blocked.rules_json().contains("scope_key=global"));

        let close = intent("close_long", "cycle_kill_close", "idem_kill_close");
        let close_verdict = evaluate_at(&store, &close, super::now_ms()).await;
        assert_eq!(close_verdict.decision, risk_policy::Decision::Approve);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn all_kill_switch_levels_allow_reduce_only_close_and_sl_tp_protection_orders() {
        let (store, path) = test_store().await;
        for level in ["pause_new", "manage_only", "flatten"] {
            sqlx::query(
                "UPDATE kill_switch_state SET level=?, reason='invariant test', trigger_source='manual_api', armed_at_ms=1, flatten_state=?, updated_at_ms=1 WHERE scope_key='global'",
            )
            .bind(level)
            .bind(if level == "flatten" { Some("flattening") } else { None })
            .execute(store.pool())
            .await
            .expect("set test level");

            let open = evaluate_at(
                &store,
                &intent(
                    "open_long",
                    &format!("cycle_{level}_open"),
                    &format!("idem_{level}_open"),
                ),
                super::now_ms(),
            )
            .await;
            assert_eq!(
                open.reject_reason_code.as_deref(),
                Some("KILL_SWITCH_ACTIVE")
            );

            for (order_type, suffix) in [
                ("market", "close"),
                ("stop_market", "sl"),
                ("take_profit_market", "tp"),
            ] {
                let mut protective = intent(
                    "close_long",
                    &format!("cycle_{level}_{suffix}"),
                    &format!("idem_{level}_{suffix}"),
                );
                protective.order_type = order_type.to_string();
                protective.reduce_only = true;
                if order_type != "market" {
                    protective.quantity.clear();
                }
                let verdict = evaluate_at(&store, &protective, super::now_ms()).await;
                assert_eq!(
                    verdict.decision,
                    risk_policy::Decision::Approve,
                    "level={level} order_type={order_type}: {}",
                    verdict.rules_json()
                );
            }
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn protection_intent_persists_typed_role_without_closing_position_under_pause_new() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let entry = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_role_entry", "idem_role_entry"),
        )
        .await;
        assert_eq!(entry.status, "accepted", "{}", entry.message);

        // 本用例的主题是「保护单 intent 的 typed role 在 pause_new 下仍被持久化」，
        // 开仓只是前置。§7.4 后开仓必须走 v2，而 v2 与保护单原子化（§3.5）会先挂
        // 上一组保护单，与本用例随后自己提交的那张在
        // (position_id, kind, generation) 上撞唯一键。清掉自动挂的那组，让用例
        // 回到它原本的起点：一个已开、尚无保护单的仓位。
        sqlx::query("DELETE FROM protection_orders")
            .execute(store.pool())
            .await
            .expect("clear protection orders created atomically with the v2 entry");

        crate::kill_switch::arm(
            &store,
            crate::kill_switch::ArmRequest {
                scope_key: "bot:bot_p1a",
                level: crate::kill_switch::Level::PauseNew,
                trigger_source: "auto_protection_timeout",
                reason: "test protection maintenance exemption",
                now_ms: super::now_ms(),
            },
        )
        .await
        .expect("arm bot pause_new");

        let mut stop = intent("close_long", "cycle_role_stop", "opaque_client_key");
        stop.order_type = "stop_market".to_string();
        stop.price = "95".to_string();
        stop.quantity.clear();
        stop.reduce_only = true;
        let stop_ack = execute_one(&service, "cred_p1a", stop).await;
        assert_eq!(stop_ack.status, "accepted");

        let order = sqlx::query(
            "SELECT order_role, position_id FROM engine_orders WHERE engine_order_id=?",
        )
        .bind(&stop_ack.engine_order_id)
        .fetch_one(store.pool())
        .await
        .expect("typed protection engine order");
        assert_eq!(order.get::<String, _>("order_role"), "protection_sl");
        assert!(!order.get::<String, _>("position_id").is_empty());

        let protection = sqlx::query(
            "SELECT kind, generation, verified_from_exchange FROM protection_orders WHERE engine_order_id=?",
        )
        .bind(&stop_ack.engine_order_id)
        .fetch_one(store.pool())
        .await
        .expect("typed protection row");
        assert_eq!(protection.get::<String, _>("kind"), "sl");
        assert_eq!(protection.get::<i64, _>("generation"), 1);
        assert_eq!(protection.get::<i64, _>("verified_from_exchange"), 0);

        let state = sqlx::query("SELECT state FROM position_state WHERE bot_uid='bot_p1a'")
            .fetch_one(store.pool())
            .await
            .expect("position remains active")
            .get::<String, _>("state");
        // 本用例钉的是「pause_new 下提交保护单**不会**把仓位关掉」。
        // v2 开仓与保护单原子化后终态是 protected（open 的下一级），
        // 二者都属活跃态；关键是它**没有**变成 closing/flattening/closed。
        assert!(
            matches!(state.as_str(), "open" | "protected"),
            "position must stay active, got {state}"
        );

        let explicit_close = execute_one(
            &service,
            "cred_p1a",
            intent(
                "close_long",
                "cycle_role_explicit_close",
                "idem_role_explicit_close",
            ),
        )
        .await;
        assert_eq!(
            explicit_close.status, "accepted",
            "pause_new must not block explicit reduce-only close"
        );
        let closed = sqlx::query("SELECT state FROM position_state WHERE bot_uid='bot_p1a'")
            .fetch_one(store.pool())
            .await
            .expect("explicit close updates position")
            .get::<String, _>("state");
        assert_eq!(closed, "closed");
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn strictest_scope_is_written_into_verdict_rules_json() {
        let (store, path) = test_store().await;
        for (scope_key, level, ts) in [
            ("global", "pause_new", 10),
            ("cred:cred_p1a", "flatten", 11),
            ("bot:bot_p1a", "manage_only", 12),
        ] {
            let flatten = (level == "flatten").then_some("flattening");
            let query = if scope_key == "global" {
                "UPDATE kill_switch_state SET level=?, reason='scope', trigger_source='manual_api', armed_at_ms=?, flatten_state=?, updated_at_ms=? WHERE scope_key='global'"
            } else {
                "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, armed_at_ms, flatten_state, updated_at_ms) VALUES (?, ?, 'scope', 'manual_api', ?, ?, ?)"
            };
            let mut statement = sqlx::query(query);
            if scope_key != "global" {
                statement = statement.bind(scope_key);
            }
            statement
                .bind(level)
                .bind(ts)
                .bind(flatten)
                .bind(ts)
                .execute(store.pool())
                .await
                .expect("set scope state");
        }
        let verdict = evaluate_at(
            &store,
            &intent("open_long", "cycle_scope_rules", "idem_scope_rules"),
            super::now_ms(),
        )
        .await;
        assert_eq!(
            verdict.reject_reason_code.as_deref(),
            Some("KILL_SWITCH_ACTIVE")
        );
        assert!(verdict.rules_json().contains("scope_key=cred:cred_p1a"));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn automatic_daily_loss_latches_manage_only_and_does_not_auto_recover() {
        let (store, path) = test_store().await;
        sqlx::query(
            "UPDATE risk_budget SET today_realized_pnl_pct=-0.03 WHERE credential_id='cred_p1a'",
        )
        .execute(store.pool())
        .await
        .expect("reach daily loss limit");
        let service = EngineExecutionService::new(store.clone());
        let first = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_auto_loss", "idem_auto_loss"),
        )
        .await;
        assert_eq!(first.status, "rejected");
        let state = sqlx::query(
            "SELECT level, trigger_source FROM kill_switch_state WHERE scope_key='cred:cred_p1a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("automatic state persisted");
        assert_eq!(state.get::<String, _>("level"), "manage_only");
        assert_eq!(state.get::<String, _>("trigger_source"), "auto_daily_loss");

        sqlx::query(
            "UPDATE risk_budget SET today_realized_pnl_pct=0 WHERE credential_id='cred_p1a'",
        )
        .execute(store.pool())
        .await
        .expect("restore healthy budget input");
        let still_latched = evaluate_at(
            &store,
            &intent("open_long", "cycle_auto_latched", "idem_auto_latched"),
            super::now_ms(),
        )
        .await;
        assert_eq!(
            still_latched.reject_reason_code.as_deref(),
            Some("KILL_SWITCH_ACTIVE")
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn automatic_drawdown_latches_flatten() {
        let (store, path) = test_store().await;
        sqlx::query("UPDATE risk_budget SET drawdown_pct=0.15 WHERE credential_id='cred_p1a'")
            .execute(store.pool())
            .await
            .expect("reach drawdown limit");
        let service = EngineExecutionService::new(store.clone());
        let ack = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_auto_drawdown", "idem_auto_drawdown"),
        )
        .await;
        assert_eq!(ack.status, "rejected");
        let state = sqlx::query(
            "SELECT level, flatten_state FROM kill_switch_state WHERE scope_key='cred:cred_p1a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("drawdown state persisted");
        assert_eq!(state.get::<String, _>("level"), "flatten");
        assert_eq!(state.get::<String, _>("flatten_state"), "flattening");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn risk_budget_kill_switch_columns_are_consumed_and_latched() {
        let (store, path) = test_store().await;
        sqlx::query(
            "UPDATE risk_budget SET kill_switch_level='pause_new', kill_switch_scope='credential', kill_switch_reason='control-plane latch' WHERE credential_id='cred_p1a'",
        )
        .execute(store.pool())
        .await
        .expect("set budget kill switch input");
        let service = EngineExecutionService::new(store.clone());
        let ack = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_budget_kill", "idem_budget_kill"),
        )
        .await;
        assert_eq!(ack.status, "rejected");
        assert!(ack.message.contains("KILL_SWITCH_ACTIVE"));

        sqlx::query(
            "UPDATE risk_budget SET kill_switch_level='none', kill_switch_scope=NULL, kill_switch_reason=NULL WHERE credential_id='cred_p1a'",
        )
        .execute(store.pool())
        .await
        .expect("clear budget snapshot without manual reset");
        let state = sqlx::query(
            "SELECT level, trigger_source FROM kill_switch_state WHERE scope_key='cred:cred_p1a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read latched state");
        assert_eq!(state.get::<String, _>("level"), "pause_new");
        assert_eq!(state.get::<String, _>("trigger_source"), "risk_budget");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn v1_factory_open_approves_without_synthetic_protection_and_does_not_mask_rr_too_low() {
        let (store, path) = test_store().await;
        // §7.4 之后 v1 open 在**生产端口**被 fail-closed 拒绝，因此本用例下沉到
        // 风控层：它钉的一直是「v1 形状的开仓不合成保护单、也不掩盖 RR_TOO_LOW」
        // 这条风控口径，而不是 wire 是否放行。wire 侧的新行为由
        // `v1_open_is_rejected_at_the_production_port` 单独钉住。
        let (decision, _sizing, rules) = evaluate_v1_open(
            &store,
            &intent("open_long", "cycle_v1_factory", "idem_v1_factory"),
        )
        .await;
        assert_eq!(decision, risk_policy::Decision::Approve);

        let find = |name: &str| {
            rules
                .as_array()
                .expect("rules audit array")
                .iter()
                .find(|rule| rule["rule"] == name)
                .unwrap_or_else(|| panic!("missing rule {name}"))
                .clone()
        };

        let data_quality = find("data_quality_gate");
        assert_eq!(data_quality["result"], "skipped");
        assert_eq!(data_quality["reason"], "wire_not_capable");

        for name in [
            "min_rr",
            "protection_required",
            "stop_distance_clamp",
            "cost_aware_tp",
        ] {
            let result = find(name);
            assert_eq!(result["result"], "skipped", "rule={name}");
            assert_eq!(
                result["reason"],
                if name == "protection_required" {
                    "wire_not_capable"
                } else {
                    "input_absent"
                },
                "rule={name}"
            );
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn v1_open_above_notional_cap_is_resized_not_bypassed() {
        let (store, path) = test_store().await;
        // 同上，下沉到风控层。主题不变：调用方定量（legacy_client_quantity）
        // 超过名义上限时必须被 **resize**，而不是原样放行——这正是审计
        // AUD-P2-D13 指出的 bypass 面，也是 §7.4 在 wire 侧关闭它的理由。
        let mut oversized = intent("open_long", "cycle_v1_notional_cap", "idem_v1_notional_cap");
        oversized.quantity = "20".to_string();
        let (decision, sizing, _rules) = evaluate_v1_open(&store, &oversized).await;

        assert_eq!(decision, risk_policy::Decision::Resize);
        assert_eq!(sizing["mode"], "legacy_client_quantity");
        assert_eq!(sizing["requested_qty"], "20");
        assert_eq!(
            sizing["final_qty"], "10",
            "超过上限的调用方定量必须被裁到上限，而不是被放行"
        );
        assert!(
            sizing["capped_by"]
                .as_array()
                .expect("capped_by audit")
                .iter()
                .any(|cap| cap.as_str().is_some()),
            "必须留下被哪条上限裁剪的审计"
        );
        cleanup(store, path).await;
    }

    /// §7.4：生产端口 fail-closed 拒绝 v1 open，且不留下任何执行痕迹。
    #[tokio::test]
    async fn v1_open_is_rejected_at_the_production_port() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let service = EngineExecutionService::new(store.clone());

        let ack = execute_one(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_v1_reject", "idem_v1_reject"),
        )
        .await;
        assert_eq!(ack.status, "rejected");
        assert_eq!(ack.message, crate::svc::V1_OPEN_REJECTION);

        // 拒绝必须发生在任何写入之前：既不下单，也不留裁决。
        for table in ["engine_orders", "risk_verdicts", "position_state"] {
            let n: i64 = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                .fetch_one(store.pool())
                .await
                .expect("count rows")
                .get("n");
            assert_eq!(n, 0, "table={table}");
        }

        // 只拦新增风险：v1 的平仓腿必须仍然放行（§3.11 同一原则）。
        let close = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_v1_close_ok", "idem_v1_close_ok"),
        )
        .await;
        assert_eq!(
            close.status, "accepted",
            "有同凭据仓位真值的平仓不得被 v1-open 拒绝规则误伤: {}",
            close.message
        );

        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn rejected_intent_keeps_append_only_risk_verdict() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO engine_credential_health (
              credential_id, degraded, consecutive_errors,
              first_error_ms, last_error_ms, updated_at_ms
            ) VALUES ('cred_p1a', 1, 1, 1, 1, 1)
            "#,
        )
        .execute(store.pool())
        .await
        .expect("degrade credential");
        let service = EngineExecutionService::new(store.clone());
        let ack = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_reject_audit", "idem_reject_audit"),
        )
        .await;
        assert_eq!(ack.status, "rejected");
        assert!(ack.message.starts_with("rejected: DEGRADED_CREDENTIAL;"));
        let verdicts: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM risk_verdicts WHERE decision = 'reject'")
                .fetch_one(store.pool())
                .await
                .expect("count rejected verdicts")
                .get("count");
        let orders: i64 = sqlx::query("SELECT COUNT(*) AS count FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count orders")
            .get("count");
        assert_eq!(verdicts, 1);
        assert_eq!(orders, 0);
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn engine_order_insert_path_rejects_missing_verdict() {
        let (store, path) = test_store().await;
        let mut tx = store.pool().begin().await.expect("begin");
        let error = crate::risk::insert_engine_order(
            &mut tx,
            "order_without_verdict",
            "cred_p1a",
            &intent("open_long", "cycle_no_verdict", "idem_no_verdict"),
            "authority_no_verdict",
            None,
            "entry",
            None,
            "filled",
            1,
        )
        .await
        .expect_err("insertion path must reject absent verdict");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        tx.rollback().await.expect("rollback");
        cleanup(store, path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_100_intents_reservation_never_exceeds_cap() {
        let (store, path) = test_store().await;
        let barrier = Arc::new(Barrier::new(100));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let store = store.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let mut tx = store
                    .pool()
                    .begin()
                    .await
                    .expect("begin reservation intent");
                let accepted =
                    crate::risk::reserve_notional(&mut tx, "cred_concurrent", "500", "100", 1)
                        .await
                        .expect("conditional reservation");
                tx.commit().await.expect("commit reservation intent");
                if accepted {
                    "accepted"
                } else {
                    "RESERVATION_CONFLICT"
                }
            }));
        }
        let mut accepted = 0;
        let mut rejected = 0;
        for task in tasks {
            match task.await.expect("join reservation intent") {
                "accepted" => accepted += 1,
                "RESERVATION_CONFLICT" => rejected += 1,
                status => panic!("unexpected reservation result: {status}"),
            }
        }
        let row = sqlx::query(
            "SELECT reserved_usdt, cap_usdt FROM notional_reservations WHERE credential_id = 'cred_concurrent'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load final reservation");
        let reserved = row
            .get::<String, _>("reserved_usdt")
            .parse::<f64>()
            .unwrap();
        let cap = row.get::<String, _>("cap_usdt").parse::<f64>().unwrap();
        assert_eq!(accepted, 5);
        assert_eq!(rejected, 95);
        assert!(reserved <= cap, "reserved={reserved} cap={cap}");
        assert_eq!(reserved, 500.0);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn position_state_active_unique_index_rejects_second_active_row() {
        let (store, path) = test_store().await;
        let key = "bot_p1a|binance|futures|BTC|USDT|USDT|long";
        insert_position(&store, "pos_active_1", 1, "open", Some(key), None)
            .await
            .expect("first active row");
        let error = insert_position(&store, "pos_active_2", 2, "pending", Some(key), None)
            .await
            .expect_err("second active row must conflict");
        assert!(position_state::is_unique_violation(&error));
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn position_state_unique_index_allows_multiple_closed_null_keys() {
        let (store, path) = test_store().await;
        insert_position(&store, "pos_closed_1", 1, "closed", None, None)
            .await
            .expect("first closed row");
        insert_position(&store, "pos_closed_2", 2, "closed", None, None)
            .await
            .expect("second closed row with NULL active_key");
        let count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM position_state")
            .fetch_one(store.pool())
            .await
            .expect("count closed rows")
            .get("count");
        assert_eq!(count, 2);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn execute_intents_rejects_duplicate_active_entry() {
        let (store, path) = test_store().await;
        assert_active_index_exists(&store).await;
        let service = EngineExecutionService::new(store.clone());
        let first = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_before_restart", "idem_client_first"),
        )
        .await;
        assert_eq!(first.status, "accepted");

        let ack = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_after_restart", "idem_client_new"),
        )
        .await;
        assert_eq!(ack.status, "rejected");
        assert!(
            ack.message.starts_with("rejected: DUPLICATE_ACTIVE_ENTRY;"),
            "unexpected message: {}",
            ack.message
        );
        let filled = sqlx::query(
            r#"
            SELECT state,
                   pending_intent_id IS NULL AS pending_intent_is_null,
                   pending_idem_key IS NULL AS pending_idem_is_null
            FROM position_state
            LIMIT 1
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load paper-filled position");
        // v2 开仓与保护单原子化（§3.5），故 paper fill 后的终态是 protected
        // 而不是 open——这是 wire 换代的必然结果，不是本用例主题的变化：
        // 它钉的仍是「同方向第二次开仓被拒为 DUPLICATE_ACTIVE_ENTRY」。
        assert_eq!(filled.get::<String, _>("state"), "protected");
        assert_eq!(filled.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(filled.get::<i64, _>("pending_idem_is_null"), 1);
        // 只数 entry：v2 开仓与 SL/TP 保护单原子化（§3.5），故总行数是 3。
        // 本用例钉的是「第二次同方向开仓不产生第二张 entry」，用 entry 计数
        // 表达比用总行数更贴主题，也不受保护单腿数变化影响。
        let entry_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM engine_orders WHERE order_role = 'entry'")
                .fetch_one(store.pool())
                .await
                .expect("count entry orders")
                .get("count");
        assert_eq!(entry_count, 1);
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn pending_entry_retry_uses_authoritative_epoch_key() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let claimed_intent = intent("open_long", "cycle_pending", "idem_client_first");
        let mut tx = store.pool().begin().await.expect("begin pending claim");
        let prepared = match position_state::prepare_intent(&mut tx, "cred_p1a", &claimed_intent, 1)
            .await
            .expect("claim pending entry")
        {
            position_state::PrepareOutcome::Ready(prepared) => prepared,
            other => panic!("expected ready pending claim, got {other:?}"),
        };
        insert_pending_engine_order(&mut tx, &claimed_intent, &prepared.authority_idem_key).await;
        sqlx::query(
            "INSERT INTO notional_reservations (credential_id, reserved_usdt, cap_usdt, updated_at_ms) VALUES ('cred_p1a', '100', '5000', 1)",
        )
        .execute(&mut *tx)
        .await
        .expect("seed original pending reservation");
        tx.commit()
            .await
            .expect("commit pending entry without fill");

        let retry_client_key = "idem_client_retry";
        let retry = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_retry", retry_client_key),
        )
        .await;
        assert_eq!(retry.status, "duplicate");
        assert_eq!(retry.idempotency_key, retry_client_key);

        let expected = position_state::semantic_idempotency_key(
            "entry|bot_p1a|binance|futures|BTC|USDT|USDT|long|1",
        );
        let rows = sqlx::query("SELECT idempotency_key FROM engine_orders")
            .fetch_all(store.pool())
            .await
            .expect("load engine orders");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String, _>("idempotency_key"), expected);
        assert!(retry.message.contains(&expected));

        let pending = sqlx::query("SELECT state, pending_idem_key FROM position_state LIMIT 1")
            .fetch_one(store.pool())
            .await
            .expect("load pending position");
        assert_eq!(pending.get::<String, _>("state"), "pending");
        assert_eq!(pending.get::<String, _>("pending_idem_key"), expected);
        let reserved = sqlx::query(
            "SELECT reserved_usdt FROM notional_reservations WHERE credential_id = 'cred_p1a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load reservation after duplicate retry")
        .get::<String, _>("reserved_usdt")
        .parse::<f64>()
        .unwrap();
        assert_eq!(reserved, 100.0, "duplicate retry must not reserve twice");
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn prepare_orphan_close_records_marked_closing_state() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let mut close = intent("close_long", "cycle_orphan_prepare", "idem_client_close");
        close.quantity = "999".to_string();
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin orphan close claim");
        let prepared = match position_state::prepare_intent(&mut tx, "cred_p1a", &close, 10)
            .await
            .expect("prepare orphan close")
        {
            position_state::PrepareOutcome::Ready(prepared) => prepared,
            other => panic!("expected ready orphan close, got {other:?}"),
        };
        assert!(prepared.orphan_recovery);
        let expected = position_state::semantic_idempotency_key(
            "exit|bot_p1a|binance|futures|BTC|USDT|USDT|long|orphan|manual_close",
        );
        assert_eq!(prepared.authority_idem_key, expected);

        let row = sqlx::query(
            r#"
            SELECT state, active_key, pending_intent_id, pending_idem_key, qty
            FROM position_state
            LIMIT 1
            "#,
        )
        .fetch_one(&mut *tx)
        .await
        .expect("load orphan recovery row");
        assert_eq!(row.get::<String, _>("state"), "closing");
        assert_eq!(
            row.get::<String, _>("active_key"),
            "bot_p1a|binance|futures|BTC|USDT|USDT|long"
        );
        assert_eq!(
            row.get::<String, _>("pending_intent_id"),
            position_state::ORPHAN_CLOSE_MARKER
        );
        assert_eq!(row.get::<String, _>("pending_idem_key"), expected);
        assert_eq!(
            row.get::<String, _>("qty"),
            "2",
            "orphan quantity must come from exchange truth, not intent.quantity"
        );
        tx.rollback()
            .await
            .expect("rollback inspection transaction");
        cleanup(store, path).await;
    }

    /// REVIEW-1102 Repro A: v2 hands the shared core an empty legacy
    /// quantity. An exchange position with no local position_state row must
    /// still produce a per-intent ack and an explicit authoritative order
    /// quantity; the RPC must not abort while reading its phantom row.
    #[tokio::test]
    async fn orphan_close_with_empty_quantity_submits_from_exchange_truth() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let service = EngineExecutionService::new(store.clone());
        let mut close = intent(
            "close_long",
            "cycle_orphan_empty_qty",
            "idem_orphan_empty_qty",
        );
        close.quantity.clear();

        let response = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: "cred_p1a".to_string(),
                run_mode: "paper".to_string(),
                intents: vec![close],
            }))
            .await
            .expect("orphan close must return an ack instead of aborting the RPC")
            .into_inner();
        let ack = response
            .acks
            .into_iter()
            .next()
            .expect("one orphan close ack");
        assert_eq!(ack.status, "accepted", "message={}", ack.message);

        let order = sqlx::query("SELECT quantity, reduce_only FROM engine_orders LIMIT 1")
            .fetch_one(store.pool())
            .await
            .expect("orphan close must persist an engine order");
        assert_eq!(order.get::<String, _>("quantity"), "2");
        assert_eq!(order.get::<i64, _>("reduce_only"), 1);

        drop(service);
        cleanup(store, path).await;
    }

    /// The positive orphan recovery leg must not turn into a cross-credential
    /// lookup. Absence of truth in the requested credential fails closed and
    /// rolls the phantom row back before an order is persisted.
    #[tokio::test]
    async fn orphan_close_does_not_borrow_exchange_truth_from_another_credential() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let service = EngineExecutionService::new(store.clone());
        let mut close = intent(
            "close_long",
            "cycle_orphan_wrong_credential",
            "idem_orphan_wrong_credential",
        );
        close.quantity.clear();

        let error = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: "cred_other".to_string(),
                run_mode: "paper".to_string(),
                intents: vec![close],
            }))
            .await
            .expect_err("cross-credential exchange truth must not authorize an orphan close");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error
                .message()
                .contains("no credential-scoped exchange position truth"),
            "unexpected error: {error}"
        );
        let order_count = sqlx::query("SELECT COUNT(*) AS count FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count engine orders")
            .get::<i64, _>("count");
        let position_count = sqlx::query("SELECT COUNT(*) AS count FROM position_state")
            .fetch_one(store.pool())
            .await
            .expect("count position rows")
            .get::<i64, _>("count");
        assert_eq!(order_count, 0);
        assert_eq!(position_count, 0);

        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn orphan_close_without_local_state_is_accepted_and_warns() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let service = EngineExecutionService::new(store.clone());
        let ack = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_orphan", "idem_client_orphan_close"),
        )
        .await;
        assert_eq!(ack.status, "accepted");

        let order_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count orphan close orders")
            .get("count");
        assert_eq!(order_count, 1);

        let position = sqlx::query(
            r#"
            SELECT state, active_key IS NULL AS active_key_is_null,
                   pending_intent_id IS NULL AS pending_intent_is_null,
                   pending_idem_key IS NULL AS pending_idem_is_null
            FROM position_state
            LIMIT 1
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load closed orphan recovery row");
        assert_eq!(position.get::<String, _>("state"), "closed");
        assert_eq!(position.get::<i64, _>("active_key_is_null"), 1);
        assert_eq!(position.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(position.get::<i64, _>("pending_idem_is_null"), 1);

        let warning = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type = 'error' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load orphan close warning event");
        let payload = warning.get::<Vec<u8>, _>("payload_json");
        let payload: serde_json::Value =
            serde_json::from_slice(&payload).expect("decode warning payload");
        assert_eq!(payload["severity"], "warning");
        assert_eq!(payload["code"], "ORPHAN_CLOSE_WITHOUT_LOCAL_POSITION");
        assert_eq!(
            payload["message"],
            "close request targets a position with no local position_state record"
        );
        assert_eq!(payload["recoveryRecorded"], true);
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn repeated_orphan_close_is_duplicate_without_extra_rows() {
        let (store, path) = test_store().await;
        insert_exchange_long_position(&store).await;
        let service = EngineExecutionService::new(store.clone());
        let first = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_orphan_1", "idem_client_orphan_1"),
        )
        .await;
        let second = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_orphan_2", "idem_client_orphan_2"),
        )
        .await;
        assert_eq!(first.status, "accepted");
        assert_eq!(second.status, "duplicate");

        let order_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count deduplicated orphan close orders")
            .get("count");
        let position_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM position_state")
            .fetch_one(store.pool())
            .await
            .expect("count orphan recovery rows")
            .get("count");
        let warning_count: i64 = sqlx::query(
            "SELECT COUNT(*) AS count FROM engine_order_events WHERE event_type = 'error'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count orphan close warnings")
        .get("count");
        assert_eq!(order_count, 1);
        assert_eq!(position_count, 1);
        assert_eq!(warning_count, 1);
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn exchange_open_and_protected_truth_clear_pending_claim_fields() {
        let (store, path) = test_store().await;
        let key = "bot_p1a|binance|futures|BTC|USDT|USDT|long";
        insert_position(
            &store,
            "pos_pending_truth",
            1,
            "pending",
            Some(key),
            Some("idem_pending_truth"),
        )
        .await
        .expect("insert pending position");
        sqlx::query(
            "UPDATE position_state SET pending_intent_id = 'intent_pending_truth' WHERE position_id = 'pos_pending_truth'",
        )
        .execute(store.pool())
        .await
        .expect("mark pending intent");

        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin exchange truth update");
        position_state::apply_exchange_position_truth(
            &mut tx,
            "cred_p1a",
            "BTCUSDT",
            Some("LONG"),
            "2.0",
            Some("100.5"),
            20,
        )
        .await
        .expect("apply open exchange truth");
        tx.commit().await.expect("commit exchange truth update");

        let row = sqlx::query(
            r#"
            SELECT state,
                   pending_intent_id IS NULL AS pending_intent_is_null,
                   pending_idem_key IS NULL AS pending_idem_is_null
            FROM position_state
            WHERE position_id = 'pos_pending_truth'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load truth-updated position");
        assert_eq!(row.get::<String, _>("state"), "open");
        assert_eq!(row.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(row.get::<i64, _>("pending_idem_is_null"), 1);

        sqlx::query(
            r#"
            UPDATE position_state
            SET state = 'protected', pending_intent_id = 'stale_protected_intent',
                pending_idem_key = 'stale_protected_idem'
            WHERE position_id = 'pos_pending_truth'
            "#,
        )
        .execute(store.pool())
        .await
        .expect("inject stale pending fields on protected position");
        let mut tx = store
            .pool()
            .begin()
            .await
            .expect("begin protected truth update");
        position_state::apply_exchange_position_truth(
            &mut tx,
            "cred_p1a",
            "BTCUSDT",
            Some("LONG"),
            "2.0",
            Some("100.5"),
            21,
        )
        .await
        .expect("apply protected exchange truth");
        tx.commit().await.expect("commit protected truth update");
        let protected = sqlx::query(
            r#"
            SELECT state,
                   pending_intent_id IS NULL AS pending_intent_is_null,
                   pending_idem_key IS NULL AS pending_idem_is_null
            FROM position_state
            WHERE position_id = 'pos_pending_truth'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("load protected truth-updated position");
        assert_eq!(protected.get::<String, _>("state"), "protected");
        assert_eq!(protected.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(protected.get::<i64, _>("pending_idem_is_null"), 1);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn confirmed_close_advances_next_entry_epoch() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let first = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_1", "idem_client_entry_1"),
        )
        .await;
        assert_eq!(first.status, "accepted");
        let close = execute_one(
            &service,
            "cred_p1a",
            intent("close_long", "cycle_close", "idem_client_close"),
        )
        .await;
        assert_eq!(close.status, "accepted");
        let closed = sqlx::query(
            "SELECT state, active_key IS NULL AS active_key_is_null, entry_epoch FROM position_state WHERE entry_epoch = 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load closed epoch");
        assert_eq!(closed.get::<String, _>("state"), "closed");
        assert_eq!(closed.get::<i64, _>("active_key_is_null"), 1);
        assert_eq!(closed.get::<i64, _>("entry_epoch"), 1);

        let next = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_2", "idem_client_entry_2"),
        )
        .await;
        assert_eq!(next.status, "accepted");
        let expected_epoch_1 = position_state::semantic_idempotency_key(
            "entry|bot_p1a|binance|futures|BTC|USDT|USDT|long|1",
        );
        let expected_epoch_2 = position_state::semantic_idempotency_key(
            "entry|bot_p1a|binance|futures|BTC|USDT|USDT|long|2",
        );
        let keys = sqlx::query(
            "SELECT idempotency_key FROM engine_orders WHERE action = 'open_long' ORDER BY created_at_ms",
        )
        .fetch_all(store.pool())
        .await
        .expect("load entry keys")
        .into_iter()
        .map(|row| row.get::<String, _>("idempotency_key"))
        .collect::<Vec<_>>();
        assert_eq!(keys, vec![expected_epoch_1, expected_epoch_2.clone()]);
        assert!(next.message.contains(&expected_epoch_2));
        drop(service);
        cleanup(store, path).await;
    }

    /// R4 RV1-1：`open_*` + `reduce_only=true` 的 v1 调用必须同样被拒绝。
    /// 原条件 `!effective_reduce_only` 让这对语义矛盾的组合（"只减仓的开仓"）
    /// 绕过 V1_OPEN_NOT_ACCEPTED，仍以 legacy_client_quantity（调用方定量）
    /// 进风控（评审 RV1-1）。开仓看 action，不看旗标。
    #[tokio::test]
    async fn v1_open_with_reduce_only_flag_is_still_rejected() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        let mut contradictory = intent("open_long", "cycle_v1_reduce_only", "idem_v1_reduce_only");
        contradictory.reduce_only = true;

        let ack = execute_one(&service, "cred_p1a", contradictory).await;
        assert_eq!(ack.status, "rejected");
        assert_eq!(ack.message, crate::svc::V1_OPEN_REJECTION);

        for table in ["engine_orders", "risk_verdicts", "position_state"] {
            let n: i64 = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                .fetch_one(store.pool())
                .await
                .expect("count rows")
                .get("n");
            assert_eq!(n, 0, "table={table}");
        }

        drop(service);
        cleanup(store, path).await;
    }

    /// R5 FBL-R4-H2（放行面 / fail-open 方向）：**未知 action** 的裸 v1 调用
    /// 必须同样被拒。RV1-1 的 `starts_with("open")` 判别是开世界的——它只枚举
    /// 要拒的形态，于是 `"buy"`、`"OPEN_LONG"`、`"entry"`、`"hold"`、空串
    /// 全部落进放行侧：role 兜底 Exit 豁免开仓 stage 门、风控按 `not_open`
    /// 跳过全规则直接 Approve、`OrderSide::for_action` 兜底 BUY、
    /// `reduce_only=false` 原样提交——一条绕过全部开仓风控的真实开仓单。
    ///
    /// 本用例钉的正是 RV1-1 只测了「收紧面」而漏掉的这一侧。
    #[tokio::test]
    async fn v1_unknown_action_is_rejected_by_the_closed_world_whitelist() {
        for (idx, action) in ["buy", "OPEN_LONG", "close", "entry", "hold", ""]
            .into_iter()
            .enumerate()
        {
            let (store, path) = test_store().await;
            let service = EngineExecutionService::new(store.clone());

            let mut unknown = intent(
                action,
                &format!("cycle_v1_unknown_{idx}"),
                &format!("idem_v1_unknown_{idx}"),
            );
            // 未知 action 的现实形态：调用方不置 reduce_only。这正是
            // `effective_reduce_only=false` → 实单 reduceOnly=false 的入口。
            unknown.reduce_only = false;

            let ack = execute_one(&service, "cred_p1a", unknown).await;
            assert_eq!(ack.status, "rejected", "action={action:?}");
            assert_eq!(
                ack.message,
                crate::svc::V1_OPEN_REJECTION,
                "action={action:?}"
            );

            // 拒绝必须发生在任何写入之前——「落单了但被风控 resize」不算修复。
            for table in ["engine_orders", "risk_verdicts", "position_state"] {
                let n: i64 = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                    .fetch_one(store.pool())
                    .await
                    .expect("count rows")
                    .get("n");
                assert_eq!(n, 0, "action={action:?} table={table}");
            }

            drop(service);
            cleanup(store, path).await;
        }
    }

    /// R5 FBL-R4-H2（收紧面 / 误伤方向）：白名单不得把平仓腿一起关掉。
    /// `close_long` / `close_short` 是裸 v1 在生产端口唯一合法的语义
    /// （保护腿与 emergency 也走它们，见 `protection::infer_v1_order_role`）。
    #[tokio::test]
    async fn v1_close_actions_stay_accepted_by_the_closed_world_whitelist() {
        for action in ["close_long", "close_short"] {
            let (store, path) = test_store().await;
            let position_amt = if action == "close_long" { "2" } else { "-2" };
            insert_exchange_position(&store, position_amt).await;
            let service = EngineExecutionService::new(store.clone());

            let ack = execute_one(
                &service,
                "cred_p1a",
                intent(
                    action,
                    &format!("cycle_v1_wl_{action}"),
                    &format!("idem_v1_wl_{action}"),
                ),
            )
            .await;
            assert_eq!(
                ack.status, "accepted",
                "有同凭据、同方向仓位真值的平仓腿不得被闭世界白名单误伤：action={action}; message={}",
                ack.message
            );

            drop(service);
            cleanup(store, path).await;
        }
    }

    /// 白名单集合本身的接线断言：把它当作**判别表**逐项钉住，
    /// 「放宽白名单」（收 `open_long`、收未知 action、按前缀而非精确匹配）
    /// 与「收窄白名单」（漏掉某个 close 腿）两种变异都在这里打红。
    #[test]
    fn v1_production_port_whitelist_is_closed_world() {
        for accepted in ["close_long", "close_short"] {
            assert!(
                crate::svc::v1_action_accepted_on_production_port(accepted),
                "平仓语义必须在白名单内：{accepted}"
            );
        }
        for rejected in [
            "open_long",
            "open_short",
            "buy",
            "sell",
            "OPEN_LONG",
            "CLOSE_LONG",
            "close",
            "close_",
            "close_longx",
            " close_long",
            "close_long ",
            "entry",
            "exit",
            "hold",
            "",
        ] {
            assert!(
                !crate::svc::v1_action_accepted_on_production_port(rejected),
                "白名单必须是闭世界的：{rejected:?} 不得被放行"
            );
        }
    }

    /// R4 RV1-2：closing 收敛由权威真值驱动——exit 订单行根本不存在
    ///（claim 后、落单前崩溃的窗口）时，truth 应用必须释放 closing，
    /// 且下一次 close 拿新键能重新提交（不锁仓）。
    #[tokio::test]
    async fn closing_released_when_pending_exit_order_missing() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let open = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_cnv_open", "idem_client_cnv_open"),
        )
        .await;
        assert_eq!(open.status, "accepted");

        let mut tx = store.pool().begin().await.expect("begin close claim");
        position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_cnv_close", "idem_client_cnv_close"),
            2,
        )
        .await
        .expect("prepare close");
        tx.commit().await.expect("commit closing");

        let before = sqlx::query(
            "SELECT position_id, COALESCE(pending_idem_key, '') AS pending_idem_key, exit_epoch FROM position_state LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load closing");
        let position_id: String = before.get("position_id");
        let pending_key: String = before.get("pending_idem_key");
        assert!(!pending_key.is_empty(), "closing 必须带着 pending 键");
        let epoch_before: i64 = before.get("exit_epoch");

        // truth：仓位仍开着（余量 1）。挂着的 exit 从未落单（engine_orders 无行）。
        let mut tx = store.pool().begin().await.expect("begin truth");
        position_state::apply_exchange_position_truth(
            &mut tx,
            "cred_p1a",
            "BTCUSDT",
            Some("LONG"),
            "1",
            None,
            3,
        )
        .await
        .expect("apply truth");
        tx.commit().await.expect("commit truth");

        let after = sqlx::query(
            "SELECT state, COALESCE(pending_idem_key, '') AS pending_idem_key, exit_epoch FROM position_state WHERE position_id = ?",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .expect("load released");
        assert_ne!(
            after.get::<String, _>("state"),
            "closing",
            "exit 死亡（订单行缺失=崩溃窗口）时 truth 应用必须释放 closing"
        );
        assert_eq!(
            after.get::<String, _>("pending_idem_key"),
            "",
            "释放必须清空 pending 键"
        );
        assert_eq!(
            after.get::<i64, _>("exit_epoch"),
            epoch_before + 1,
            "释放必须自增 exit_epoch，下一次 close 才有新键"
        );

        // 收敛证明：同一仓位可重新提交 close。
        let mut tx = store.pool().begin().await.expect("begin re-close");
        let outcome = position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_cnv_reclose", "idem_client_cnv_reclose"),
            4,
        )
        .await
        .expect("re-prepare close");
        tx.commit().await.expect("commit re-close");
        assert!(
            matches!(outcome, position_state::PrepareOutcome::Ready(_)),
            "释放后的重平必须拿到 Ready，实际 {outcome:?}"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// R4 RV1-2：exit 订单被交易所 canceled（部分成交后的典型终局）同样释放；
    /// exit 仍在途（submitted）时**不**释放——收敛只认权威终态。
    #[tokio::test]
    async fn closing_released_on_canceled_exit_but_not_while_live() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_cnv2_open", "idem_client_cnv2_open"),
        )
        .await;

        let mut tx = store.pool().begin().await.expect("begin close claim");
        position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_cnv2_close", "idem_client_cnv2_close"),
            2,
        )
        .await
        .expect("prepare close");
        tx.commit().await.expect("commit closing");

        let row = sqlx::query(
            "SELECT position_id, bot_uid, COALESCE(pending_idem_key, '') AS pending_idem_key FROM position_state LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load closing");
        let position_id: String = row.get("position_id");
        let bot_uid: String = row.get("bot_uid");
        let pending_key: String = row.get("pending_idem_key");
        assert!(!pending_key.is_empty(), "closing 必须带着 pending 键");

        // 在途：submitted → 不释放。
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (
              'order_cnv_exit', 'cred_p1a', ?, ?,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'submitted', 2, 2
            )
            "#,
        )
        .bind(&bot_uid)
        .bind(&pending_key)
        .execute(store.pool())
        .await
        .expect("insert live exit order");

        let mut tx = store.pool().begin().await.expect("begin truth live");
        position_state::apply_exchange_position_truth(
            &mut tx,
            "cred_p1a",
            "BTCUSDT",
            Some("LONG"),
            "2",
            None,
            3,
        )
        .await
        .expect("apply truth live");
        tx.commit().await.expect("commit truth live");
        let state: String = sqlx::query("SELECT state FROM position_state WHERE position_id = ?")
            .bind(&position_id)
            .fetch_one(store.pool())
            .await
            .expect("load live state")
            .get("state");
        assert_eq!(state, "closing", "exit 在途时不得释放");

        // 终态死亡：canceled → 释放（部分成交余量 1）。
        sqlx::query("UPDATE engine_orders SET status = 'canceled', updated_at_ms = 5 WHERE engine_order_id = 'order_cnv_exit'")
            .execute(store.pool())
            .await
            .expect("cancel exit order");

        let mut tx = store.pool().begin().await.expect("begin truth dead");
        position_state::apply_exchange_position_truth(
            &mut tx,
            "cred_p1a",
            "BTCUSDT",
            Some("LONG"),
            "1",
            None,
            6,
        )
        .await
        .expect("apply truth dead");
        tx.commit().await.expect("commit truth dead");
        let row = sqlx::query(
            "SELECT state, COALESCE(pending_idem_key, '') AS pending_idem_key, qty FROM position_state WHERE position_id = ?",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .expect("load released state");
        assert_ne!(row.get::<String, _>("state"), "closing");
        assert_eq!(row.get::<String, _>("pending_idem_key"), "");
        assert_eq!(
            row.get::<String, _>("qty"),
            "1",
            "truth 必须把仓位量刷新为部分成交后的余量"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// R5 FBL-R4-H3：`exit_order_status_is_dead` 判别表。
    ///
    /// R4 的 dead 集 = {行缺失, canceled, expired, failed}——`rejected`
    /// （WS 异步拒绝送达的交易所终态）漏在集外，那条 close 单再也不会成交
    /// 而 closing 永不释放。同时钉住**保守方向**：`unknown` / `pending_*`
    /// 不得被直接推定死亡（必须先由 reconcile 向交易所求证），把
    /// 「本地不知道」当成「交易所没有」正是误释放的入口。
    #[test]
    fn exit_order_dead_set_covers_exchange_terminals_only() {
        for dead in ["canceled", "expired", "failed", "rejected"] {
            assert!(
                position_state::exit_order_status_is_dead(Some(dead)),
                "交易所/本地终态必须判死：{dead}"
            );
        }
        assert!(
            position_state::exit_order_status_is_dead(None),
            "订单行缺失（claim 后落单前崩溃）必须判死"
        );
        for alive in [
            "unknown",
            "pending_risk",
            "pending_submit",
            "submitted",
            "acked",
            "partial_filled",
            "filled",
            "",
        ] {
            assert!(
                !position_state::exit_order_status_is_dead(Some(alive)),
                "未经交易所判死的状态不得被推定死亡：{alive:?}"
            );
        }
    }

    /// R5 FBL-R4-H3（行为级）：`rejected` 的 exit 单让 closing 永久锁死——
    /// 这是评审点名的 Medium-Low 面（WS 异步拒绝路径），与 H3 同族。
    /// 修复后清扫必须释放它，且下一次 close 拿新键能真正重提交。
    #[tokio::test]
    async fn closing_released_on_rejected_exit_by_sweep() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_h3rej_open", "idem_h3rej_open"),
        )
        .await;

        let mut tx = store.pool().begin().await.expect("begin close claim");
        position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_h3rej_close", "idem_h3rej_close"),
            2,
        )
        .await
        .expect("prepare close");
        tx.commit().await.expect("commit closing");

        let row = sqlx::query(
            "SELECT position_id, bot_uid, COALESCE(pending_idem_key, '') AS pending_idem_key, exit_epoch FROM position_state LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load closing");
        let position_id: String = row.get("position_id");
        let bot_uid: String = row.get("bot_uid");
        let pending_key: String = row.get("pending_idem_key");
        let epoch_before: i64 = row.get("exit_epoch");

        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, created_at_ms, updated_at_ms
            ) VALUES (
              'order_h3rej_exit', 'cred_p1a', ?, ?,
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'close_long', 'long', 'market',
              '2', 1, 'rejected', 2, 2
            )
            "#,
        )
        .bind(&bot_uid)
        .bind(&pending_key)
        .execute(store.pool())
        .await
        .expect("insert rejected exit order");

        // 清扫按**仓位**枚举，不依赖本轮是否恰好收到该仓位的交易所 truth。
        let mut tx = store.pool().begin().await.expect("begin sweep");
        let released = position_state::sweep_closing_positions(&mut tx, "cred_p1a", 7)
            .await
            .expect("sweep closing");
        tx.commit().await.expect("commit sweep");
        assert_eq!(released, 1, "rejected 的 exit 单必须让清扫释放 closing");

        let after = sqlx::query(
            "SELECT state, COALESCE(pending_idem_key, '') AS pending_idem_key, exit_epoch FROM position_state WHERE position_id = ?",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .expect("load released");
        assert_ne!(after.get::<String, _>("state"), "closing");
        assert_eq!(after.get::<String, _>("pending_idem_key"), "");
        assert_eq!(after.get::<i64, _>("exit_epoch"), epoch_before + 1);

        let mut tx = store.pool().begin().await.expect("begin re-close");
        let outcome = position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_h3rej_re", "idem_h3rej_re"),
            8,
        )
        .await
        .expect("re-prepare close");
        tx.commit().await.expect("commit re-close");
        assert!(
            matches!(outcome, position_state::PrepareOutcome::Ready(_)),
            "释放后的重平必须拿到 Ready，实际 {outcome:?}"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// R5 FBL-R4-H3（保守方向）：清扫不得把「本地不知道」当成「交易所没有」。
    /// `unknown` / `pending_submit` 的 exit 单**未经 reconcile 向交易所求证**
    /// 时，closing 必须原样保留——否则旧单随后成交就是误释放（评审确认误释放
    /// 方向此前不存在，本修复不得开出这个面）。
    #[tokio::test]
    async fn sweep_does_not_release_on_unresolved_exit_statuses() {
        for (idx, status) in ["unknown", "pending_risk", "pending_submit", "submitted"]
            .into_iter()
            .enumerate()
        {
            let (store, path) = test_store().await;
            let service = EngineExecutionService::new(store.clone());
            execute_one_v2(
                &service,
                "cred_p1a",
                intent(
                    "open_long",
                    &format!("cycle_h3cons_open_{idx}"),
                    &format!("idem_h3cons_open_{idx}"),
                ),
            )
            .await;

            let mut tx = store.pool().begin().await.expect("begin close claim");
            position_state::prepare_intent(
                &mut tx,
                "cred_p1a",
                &intent(
                    "close_long",
                    &format!("cycle_h3cons_close_{idx}"),
                    &format!("idem_h3cons_close_{idx}"),
                ),
                2,
            )
            .await
            .expect("prepare close");
            tx.commit().await.expect("commit closing");

            let row = sqlx::query(
                "SELECT position_id, bot_uid, COALESCE(pending_idem_key, '') AS pending_idem_key FROM position_state LIMIT 1",
            )
            .fetch_one(store.pool())
            .await
            .expect("load closing");
            let position_id: String = row.get("position_id");
            let bot_uid: String = row.get("bot_uid");
            let pending_key: String = row.get("pending_idem_key");

            sqlx::query(
                r#"
                INSERT INTO engine_orders (
                  engine_order_id, credential_id, bot_uid, idempotency_key,
                  exchange, market_kind, symbol_key, action, side, order_type,
                  quantity, reduce_only, status, created_at_ms, updated_at_ms
                ) VALUES (
                  'order_h3cons_exit', 'cred_p1a', ?, ?,
                  'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
                  'close_long', 'long', 'market',
                  '2', 1, ?, 2, 2
                )
                "#,
            )
            .bind(&bot_uid)
            .bind(&pending_key)
            .bind(status)
            .execute(store.pool())
            .await
            .expect("insert unresolved exit order");

            let mut tx = store.pool().begin().await.expect("begin sweep");
            let released = position_state::sweep_closing_positions(&mut tx, "cred_p1a", 7)
                .await
                .expect("sweep closing");
            tx.commit().await.expect("commit sweep");
            assert_eq!(released, 0, "status={status} 不得被清扫释放");

            let state: String =
                sqlx::query("SELECT state FROM position_state WHERE position_id = ?")
                    .bind(&position_id)
                    .fetch_one(store.pool())
                    .await
                    .expect("load state")
                    .get("state");
            assert_eq!(state, "closing", "status={status} 必须保留 closing");

            drop(service);
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn unconfirmed_close_does_not_advance_epoch() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let open = execute_one_v2(
            &service,
            "cred_p1a",
            intent("open_long", "cycle_open", "idem_client_open"),
        )
        .await;
        assert_eq!(open.status, "accepted");

        let mut tx = store.pool().begin().await.expect("begin close claim");
        let outcome = position_state::prepare_intent(
            &mut tx,
            "cred_p1a",
            &intent("close_long", "cycle_close", "idem_client_close"),
            2,
        )
        .await
        .expect("prepare unconfirmed close");
        assert!(matches!(outcome, position_state::PrepareOutcome::Ready(_)));
        tx.commit().await.expect("commit closing state");

        let row = sqlx::query(
            "SELECT state, active_key IS NOT NULL AS has_active_key, entry_epoch, closed_at_ms IS NULL AS closed_at_is_null FROM position_state LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("load unconfirmed position");
        assert_eq!(row.get::<String, _>("state"), "closing");
        assert_eq!(row.get::<i64, _>("has_active_key"), 1);
        assert_eq!(row.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(row.get::<i64, _>("closed_at_is_null"), 1);
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn exchange_filter_failure_is_auditable_and_open_only() {
        let (store, path) = test_store().await;

        let open = intent("open_long", "filter_open", "idem_filter_open");
        let mut open_tx = store.pool().begin().await.expect("begin open verdict");
        let (base_open, _, open_replay) = crate::risk::evaluate_v1(
            &mut open_tx,
            store.kind(),
            "cred_p1a",
            &open,
            super::now_ms(),
            None,
            "paper",
            false,
            OrderRole::Entry,
            None,
        )
        .await
        .expect("evaluate open");
        assert_ne!(base_open.decision, Decision::Reject);
        let open_filter = crate::exchange_filters::apply_exchange_filters(
            OrderRole::Entry,
            false,
            crate::exchange_filters::OrderSide::Buy,
            base_open.sizing.as_ref().unwrap().final_qty(),
            &open.price,
            Err("network down"),
        );
        let (filtered_open, _) =
            crate::risk::replay_exchange_filter_result(&open_replay, &open_filter)
                .expect("append open filter verdict");
        assert_eq!(filtered_open.decision, Decision::Reject);
        assert_eq!(
            filtered_open.reject_reason_code.as_deref(),
            Some("EXCHANGE_FILTERS_UNAVAILABLE")
        );
        assert!(filtered_open
            .rules_json()
            .contains("\"rule\":\"exchange_filters\""));
        open_tx.rollback().await.expect("rollback open snapshot");

        let close = intent("close_long", "filter_close", "idem_filter_close");
        let mut close_tx = store.pool().begin().await.expect("begin close verdict");
        let (base_close, _, close_replay) = crate::risk::evaluate_v1(
            &mut close_tx,
            store.kind(),
            "cred_p1a",
            &close,
            super::now_ms(),
            None,
            "paper",
            false,
            OrderRole::Exit,
            None,
        )
        .await
        .expect("evaluate close");
        assert_ne!(base_close.decision, Decision::Reject);
        let close_filter = crate::exchange_filters::apply_exchange_filters(
            OrderRole::Exit,
            true,
            crate::exchange_filters::OrderSide::Sell,
            &close.quantity,
            &close.price,
            Err("network down"),
        );
        let (filtered_close, replay_json) =
            crate::risk::replay_exchange_filter_result(&close_replay, &close_filter)
                .expect("append close filter verdict");
        assert_ne!(filtered_close.decision, Decision::Reject);
        assert!(filtered_close.rules_json().contains("exchange_filters"));
        assert!(filtered_close
            .rules_json()
            .contains("not_applicable_to_close_or_hold"));
        assert_eq!(
            crate::risk::replay_verdict(&replay_json)
                .expect("replay close verdict")
                .rules_json(),
            filtered_close.rules_json()
        );
        close_tx.rollback().await.expect("rollback close snapshot");
        cleanup(store, path).await;
    }

    // ---- AUD-P2-H6 / H7 回归 -------------------------------------------------

    /// H6：close 终态失败后必须把仓位从 closing 释放回可再平仓的状态，
    /// 并自增 exit_epoch 使下一次 close 得到一把**新**幂等键。
    ///
    /// 缺陷形态下 position_state 停在 closing 且带着陈旧键：重试恒 duplicate、
    /// 同方向新开仓被 DUPLICATE_ACTIVE_ENTRY 拒绝，仓位既平不掉也开不了。
    #[tokio::test]
    async fn h6_failed_exit_releases_closing_and_bumps_epoch() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, pre_exit_state,
              active_key, pending_intent_id, pending_idem_key, qty, updated_at_ms
            ) VALUES ('pos_h6', 'cred_p1a', 'bot_h6',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 0, 'closing', 'protected',
              'bot_h6|symbol|long|1', 'int_h6', 'stale_exit_key', '1', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();

        let mut tx = store.pool().begin().await.unwrap();
        let released =
            crate::position_state::release_closing_after_failed_exit(&mut tx, "pos_h6", 42)
                .await
                .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(released, 1);

        // sqlx 的 Any 驱动这版不能把 NULL 直接解进 Option<String>，故用 COALESCE。
        let row = sqlx::query(
            "SELECT state, exit_epoch, \
             COALESCE(pre_exit_state, '') AS pre_exit_state, \
             COALESCE(pending_idem_key, '') AS pending_idem_key, \
             COALESCE(pending_intent_id, '') AS pending_intent_id \
             FROM position_state WHERE position_id='pos_h6'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();

        // 恢复到进入 closing 之前的状态：protected 不能被抹成 open，
        // 否则「保护单已挂」这个事实就丢了。
        assert_eq!(row.get::<String, _>("state"), "protected");
        // 新 epoch 让下一次 close 得到一把新键，能真正重新提交。
        assert_eq!(row.get::<i64, _>("exit_epoch"), 1);
        // 陈旧的在途标记必须清干净，否则重试仍会命中 duplicate。
        assert_eq!(row.get::<String, _>("pending_idem_key"), "");
        assert_eq!(row.get::<String, _>("pending_intent_id"), "");
        assert_eq!(row.get::<String, _>("pre_exit_state"), "");

        cleanup(store, path).await;
    }

    /// H6：释放只作用于 closing 行——在途 close 之外的状态一律不动。
    #[tokio::test]
    async fn h6_release_does_not_touch_non_closing_positions() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms
            ) VALUES ('pos_h6_open', 'cred_p1a', 'bot_h6b',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 0, 'protected',
              'bot_h6b|symbol|long|1', '1', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();

        let mut tx = store.pool().begin().await.unwrap();
        let released =
            crate::position_state::release_closing_after_failed_exit(&mut tx, "pos_h6_open", 42)
                .await
                .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(released, 0);
        let row = sqlx::query(
            "SELECT state, exit_epoch FROM position_state WHERE position_id='pos_h6_open'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("state"), "protected");
        assert_eq!(row.get::<i64, _>("exit_epoch"), 0);

        cleanup(store, path).await;
    }

    /// H7：sizing 的入场参考价不得取用陈旧 bar。
    ///
    /// 缺陷形态下 bar 查询没有任何时间谓词，`ORDER BY ts_ms DESC LIMIT 1` 会
    /// 原样取回 N 小时前的收盘价并直接进 sizing / SL 距离 / 交易所数量。
    /// kline WS 断流后 bar 触发虽停，但 timer 与 admin 手动触发仍能以陈旧价
    /// 开出市价单——engine safety contract「行情陈旧仍开仓」是不可协商的 High。
    #[tokio::test]
    async fn h7_stale_bar_is_not_used_as_entry_reference() {
        let (store, path) = test_store().await;
        let now = 10_000_000_000i64;

        // 唯一可用的 bar 比允许窗口还老一小时。
        let stale_ts = now - 3_600_000;
        sqlx::query(
            r#"
            INSERT INTO engine_order_events (
              credential_id, seq, event_id, ts_ms, event_type,
              symbol_key, payload_json
            ) VALUES ('cred_p1a', 1, 'evt_stale_bar', ?, 'bar_closed',
              'binance|futures|BTC|USDT|USDT', ?)
            "#,
        )
        .bind(stale_ts)
        .bind(serde_json::to_vec(&serde_json::json!({ "close": "63850" })).unwrap())
        .execute(store.pool())
        .await
        .unwrap();

        let mut tx = store.pool().begin().await.unwrap();
        let stale = crate::risk::entry_reference_price_for_test(
            &mut tx,
            "cred_p1a",
            "binance|futures|BTC|USDT|USDT",
            now,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            stale.is_none(),
            "陈旧 bar 不得被当作入场参考价（缺陷形态下这里是 Some(63850.0)）"
        );

        // 同一条 bar 落在窗口内时必须可用——否则就不是「拒绝陈旧」而是「恒拒」。
        let mut tx = store.pool().begin().await.unwrap();
        let fresh = crate::risk::entry_reference_price_for_test(
            &mut tx,
            "cred_p1a",
            "binance|futures|BTC|USDT|USDT",
            stale_ts + 1_000,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(fresh, Some(63850.0));

        cleanup(store, path).await;
    }

    /// H6 的**接线**钉子：exit 幂等键必须真的把 exit_epoch 算进去。
    ///
    /// 上面两条只覆盖了释放函数本身——把 epoch 从键里去掉时它们仍全绿
    /// （变异存活）。守卫存在而接线无人钉，是本轮反复出现的缺口形态，
    /// 故这里直接比对「释放前后两次 prepare 得到的键」。
    #[tokio::test]
    async fn h6_exit_idempotency_key_changes_after_epoch_bump() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms
            ) VALUES ('pos_h6_key', 'cred_p1a', 'bot_h6_key',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 0, 'protected',
              'bot_h6_key|binance|futures|BTC|USDT|USDT|long|1', '1', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();

        let mut intent = crate::proto::OrderIntent::default();
        intent.bot_uid = "bot_h6_key".to_string();
        intent.symbol_key = "binance|futures|BTC|USDT|USDT".to_string();
        intent.action = "close_long".to_string();

        let key_of = |outcome: crate::position_state::PrepareOutcome| match outcome {
            crate::position_state::PrepareOutcome::Ready(prepared) => prepared.authority_idem_key,
            _ => panic!("expected a ready exit preparation"),
        };

        let mut tx = store.pool().begin().await.unwrap();
        let first = key_of(
            crate::position_state::prepare_intent(&mut tx, "cred_p1a", &intent, 1)
                .await
                .unwrap(),
        );
        tx.commit().await.unwrap();

        // 模拟提交失败后的解锁：状态回到 protected、epoch +1。
        let mut tx = store.pool().begin().await.unwrap();
        crate::position_state::release_closing_after_failed_exit(&mut tx, "pos_h6_key", 2)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut tx = store.pool().begin().await.unwrap();
        let second = key_of(
            crate::position_state::prepare_intent(&mut tx, "cred_p1a", &intent, 3)
                .await
                .unwrap(),
        );
        tx.commit().await.unwrap();

        // 缺陷形态下两把键逐字节相同 —— 重试命中 engine_orders 唯一键返回
        // duplicate、永不重新提交，仓位就此锁死。
        assert_ne!(
            first, second,
            "exit 幂等键必须随 exit_epoch 变化，否则失败后的重试永远撞唯一键"
        );

        cleanup(store, path).await;
    }

    /// H5：空 quantity 的 exit 必须从权威仓位推导数量。
    ///
    /// 缺陷形态是「只给 paper 推导」（call site 带 `!is_exchange_backed` 前置），
    /// 于是 v2 的交易所平仓恒发空 quantity → Binance -1102 → 平仓必失败，
    /// 失败后又落进 H6 的锁死态。`resolve_close_family_quantity` 的签名里
    /// **没有** is_exchange_backed，所以这条路径对 paper 与交易所是同一份实现，
    /// 并且 SL/TP/emergency 与普通 exit 共用同一权威来源。
    #[tokio::test]
    async fn h5_empty_exit_quantity_is_derived_from_authoritative_position() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms
            ) VALUES ('pos_h5', 'cred_p1a', 'bot_h5',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 0, 'protected',
              'bot_h5|symbol|long|1', '0.0157', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();

        let mut tx = store.pool().begin().await.unwrap();
        for role in [
            OrderRole::Exit,
            OrderRole::ProtectionSl,
            OrderRole::ProtectionTp,
            OrderRole::Emergency,
        ] {
            let derived = resolve_close_family_quantity(&mut tx, role, "", Some("pos_h5"))
                .await
                .unwrap();
            assert_eq!(derived.as_deref(), Some("0.0157"), "role={role:?}");
        }
        tx.commit().await.unwrap();

        // 调用方已定量时不覆写；entry 不推导。
        let mut tx = store.pool().begin().await.unwrap();
        assert!(
            resolve_close_family_quantity(&mut tx, OrderRole::Exit, "1.5", Some("pos_h5"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_close_family_quantity(&mut tx, OrderRole::Entry, "", Some("pos_h5"))
                .await
                .unwrap()
                .is_none()
        );
        tx.commit().await.unwrap();

        cleanup(store, path).await;
    }

    /// H5：权威仓位没有合法正数量时必须**拒绝**，不得发出空或畸形 quantity。
    #[tokio::test]
    async fn h5_invalid_authoritative_quantity_is_rejected() {
        let (store, path) = test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, updated_at_ms
            ) VALUES ('pos_h5_zero', 'cred_p1a', 'bot_h5_zero',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 0, 'protected',
              'bot_h5_zero|symbol|long|1', '0', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();

        for invalid in ["", "0", "-0.1", "not-a-decimal"] {
            sqlx::query("UPDATE position_state SET qty=? WHERE position_id='pos_h5_zero'")
                .bind(invalid)
                .execute(store.pool())
                .await
                .unwrap();
            let mut tx = store.pool().begin().await.unwrap();
            let outcome =
                resolve_close_family_quantity(&mut tx, OrderRole::Exit, "", Some("pos_h5_zero"))
                    .await;
            tx.commit().await.unwrap();
            assert!(
                outcome.is_err(),
                "invalid authoritative quantity {invalid:?} must fail before submission"
            );
        }

        cleanup(store, path).await;
    }
    // ---- V2-E Exit 1：模拟凭据不得被路由到交易所密钥 ----
    //
    // 三层防御各自独立断言，且每层都同时钉**收紧面**与**放行面**：
    // 只钉收紧面会放过「顺手把 paper 也拦了」，只钉放行面会放过 fail-open。

    const SIM_CRED: &str = "paper:0123456789abcdef0123456789abcdef";

    /// 收紧面：模拟凭据 + 三个 exchange-backed 档位逐个拒。
    #[test]
    fn b1_simulation_credential_denied_on_every_exchange_backed_stage() {
        for stage in ["demo", "live_small", "live"] {
            assert_eq!(
                stage_allowed_with_credential(stage, SIM_CRED, "live", true),
                Err(StageDenial::SimulationCredentialNotExchangeBacked),
                "stage={stage} 必须拒绝模拟凭据"
            );
        }
    }

    /// 放行面：模拟凭据走 paper 必须照常通过——安全面不得被顺手拦掉。
    #[test]
    fn b1_simulation_credential_still_allowed_on_paper_stage() {
        assert_eq!(
            stage_allowed_with_credential("paper", SIM_CRED, "live", true),
            Ok(())
        );
        assert_eq!(
            stage_allowed_with_credential("paper", "paper", "paper", false),
            Ok(())
        );
    }

    /// `stage_denial_applies` 的 order_role 豁免只覆盖 ExceedsEffective 与
    /// （D39 起、限可达交易所的只减退出）LiveDisabled。若有人把新 variant 加进
    /// 那个豁免，本测试变红。
    #[test]
    fn b1_simulation_denial_applies_to_every_order_role() {
        for role in [
            OrderRole::Entry,
            OrderRole::Exit,
            OrderRole::ProtectionSl,
            OrderRole::ProtectionTp,
        ] {
            assert!(
                stage_denial_applies(
                    &StageDenial::SimulationCredentialNotExchangeBacked,
                    role,
                    "close_long",
                    true
                ),
                "role={role:?} 不得豁免模拟凭据拒绝"
            );
            assert!(
                stage_denial_applies(
                    &StageDenial::UnprovenCredentialIdentity,
                    role,
                    "close_long",
                    true
                ),
                "role={role:?} 不得豁免畸形模拟凭据拒绝"
            );
        }
        // 对照：既有豁免仍然只对 Entry 生效，本次改动没有动它。
        let exceeds = StageDenial::ExceedsEffective {
            requested: "live".to_string(),
            effective: "paper",
        };
        assert!(stage_denial_applies(
            &exceeds,
            OrderRole::Entry,
            "open_long",
            true
        ));
        assert!(!stage_denial_applies(
            &exceeds,
            OrderRole::Exit,
            "close_long",
            true
        ));
    }

    /// `paper` 是保留命名空间：差一位的近似串既不是模拟凭据，
    /// 也**不得**掉进交易所分支（否则拼错一位就拿到全局 mainnet 密钥）。
    #[test]
    fn b1_paper_namespace_lookalikes_are_denied_not_treated_as_exchange() {
        let lookalikes = [
            "paper:0123456789abcdef0123456789abcde",   // 31 位
            "paper:0123456789abcdef0123456789abcdef0", // 33 位
            "paper:0123456789ABCDEF0123456789ABCDEF",  // hex 大写非文法
            // 前缀大小写：没有这一条，去掉 to_ascii_lowercase 的变异会存活
            // （评审独立抽验 R-A 实证）。
            "PAPER:0123456789abcdef0123456789abcdef",
            "Paper:0123456789abcdef0123456789abcdef",
            "paper:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // 非 hex
            "paper_0123456789abcdef0123456789abcdef", // 下划线
            "paper-trading",
            " paper:0123456789abcdef0123456789abcdef", // 前导空白
        ];
        for cred in lookalikes {
            assert!(
                !crate::venue::is_simulation_credential(cred),
                "{cred:?} 不应被判为合法模拟凭据"
            );
            assert_eq!(
                stage_allowed_with_credential("live", cred, "live", true),
                Err(StageDenial::UnprovenCredentialIdentity),
                "{cred:?} 必须被拒，而不是当成交易所凭据"
            );
            assert!(
                !exchange_gateway_allowed_for(
                    crate::venue::credential_kind(cred),
                    true,
                    Some(Venue::Mainnet)
                ),
                "{cred:?} 不得通过交易所网关"
            );
        }
    }

    /// 身份必须**被证明**，不能靠「排除已知 paper 拼法」。
    ///
    /// 评审实测：`p<U+0430>per:<32hex>`（西里尔 а）与 `pa<U+200B>per:<32hex>`
    /// （零宽空格）都不以 ASCII `paper` 开头，黑名单实现会把它们判成交易所凭据，
    /// 于是回落到全局 mainnet 密钥。闭世界文法把这类输入一律拒掉。
    #[test]
    fn b1_non_ascii_and_zero_width_credential_ids_are_denied() {
        let hostile = [
            "p\u{0430}per:0123456789abcdef0123456789abcdef", // 西里尔 а
            "pa\u{200b}per:0123456789abcdef0123456789abcdef", // 零宽空格
            "desk\u{200b}test",                              // 交易所侧同形注入
            "desk\u{00a0}test",                              // 不换行空格
            "desk\ttest",                                    // 控制字符
            "desk test",                                     // 空格
            "desk/test",                                     // 斜杠不在文法内
        ];
        for credential_id in hostile {
            assert_eq!(
                crate::venue::credential_kind(credential_id),
                crate::venue::CredentialKind::UnprovenIdentity,
                "{credential_id:?} 必须落 UnprovenIdentity"
            );
            assert_eq!(
                stage_allowed_with_credential("live", credential_id, "live", true),
                Err(StageDenial::UnprovenCredentialIdentity),
                "{credential_id:?} 必须被档位准入拒绝"
            );
            assert!(
                !exchange_gateway_allowed_for(
                    crate::venue::credential_kind(credential_id),
                    true,
                    Some(Venue::Mainnet)
                ),
                "{credential_id:?} 不得通过交易所网关"
            );
            assert!(
                crate::binance_futures::keys_from_env(credential_id)
                    .unwrap()
                    .is_none(),
                "{credential_id:?} 不得解析出交易所密钥"
            );
        }
    }

    /// 放行面：**合法**的交易所凭据 id 形状必须仍被接受。
    /// 只测拒绝会放过「把文法收得太紧、把真实凭据一起拒掉」的实现。
    #[test]
    fn b1_wellformed_exchange_credential_ids_are_still_accepted() {
        for credential_id in [
            "desk-test-v1",
            "b1_desk_test",
            "cred.42",
            "binance:sub-account-7",
            "A1",
            &"c".repeat(64),
        ] {
            assert_eq!(
                crate::venue::credential_kind(credential_id),
                crate::venue::CredentialKind::ExchangeBacked,
                "{credential_id:?} 是合法交易所凭据 id，不得被拒"
            );
        }
        // 超长（65）越界即拒
        assert_eq!(
            crate::venue::credential_kind(&"c".repeat(65)),
            crate::venue::CredentialKind::UnprovenIdentity
        );
    }

    /// 交易所凭据的既有真值表**逐格不变**——这条是「没把别人改坏」的证据。
    #[test]
    fn b1_exchange_backed_gateway_truth_table_unchanged() {
        use crate::venue::CredentialKind::ExchangeBacked;
        assert!(exchange_gateway_allowed_for(
            ExchangeBacked,
            false,
            Some(Venue::Testnet)
        ));
        assert!(!exchange_gateway_allowed_for(
            ExchangeBacked,
            false,
            Some(Venue::Mainnet)
        ));
        assert!(!exchange_gateway_allowed_for(ExchangeBacked, false, None));
        assert!(exchange_gateway_allowed_for(
            ExchangeBacked,
            true,
            Some(Venue::Mainnet)
        ));
        assert!(exchange_gateway_allowed_for(ExchangeBacked, true, None));
    }

    /// 收紧面：模拟凭据在 live 开关的**两种**状态下都不通过网关。
    /// 只测 live_enabled=true 会放过「关掉 live 时反而放行」的实现。
    #[test]
    fn b1_simulation_never_passes_gateway_under_either_live_switch() {
        use crate::venue::CredentialKind::Simulation;
        for live_enabled in [true, false] {
            for venue in [None, Some(Venue::Testnet), Some(Venue::Mainnet)] {
                assert!(
                    !exchange_gateway_allowed_for(Simulation, live_enabled, venue),
                    "live_enabled={live_enabled} venue={venue:?} 下模拟凭据仍通过了网关"
                );
            }
        }
    }

    /// 交易所凭据的档位准入行为不变（testnet 凭据三档皆过）。
    #[test]
    fn b1_exchange_credential_stage_admission_unchanged() {
        let cred = "b1-desk-test";
        let env_name = crate::venue::venue_env_name(cred);
        let previous = std::env::var_os(&env_name);
        std::env::set_var(&env_name, "testnet");

        let results = [
            stage_allowed_with_credential("paper", cred, "live", true),
            stage_allowed_with_credential("demo", cred, "live", true),
            stage_allowed_with_credential("live", cred, "live", true),
        ];

        match previous {
            Some(value) => std::env::set_var(&env_name, value),
            None => std::env::remove_var(&env_name),
        }

        for result in results {
            assert_eq!(result, Ok(()));
        }
    }
}

/// **/// 提交路径的完整生产入口 wiring proof。**
///
/// 这一组用例全部从 `EngineExecution::execute_intents`（gRPC 方法本体，
/// 外部实际调用的那个符号）出发，经真实的风控、规范化、提交闸门与**真实的
/// 交易所客户端分类**，直到写回落库为止；断言的是**库里的状态**，不是探针。
///
/// 为什么必须这样：R8 装好了 `with_submit_client_override` 这个注入口，
/// 却**一次都没被调用过**——`Acked` 用例仍然自己 `new` 一个 enum 喂给
/// `write_back_intent`。于是评审变异 M11（把真实 market ACK 错分成
/// `StartedUnknown`）1/1 存活：分类的**产生点**从来没有被驱动过。
///
/// **没有网络**（engine safety contract：交易路径测试默认 dry-run）：
/// - 交易所 HTTP 走 `BinanceFuturesClient::new_with_mock_responses` 的内存队列；
/// - symbol filters / mark price 走 `static_filter_cache` 的固定真值；
/// - user stream 与周期对账两条**后台**任务在用例里不起（它们不在被证明的
///   提交链路上，但会真的连交易所）。
///   这三处都是**同一条链路以外**的替身，链路本身一层都没绕。
#[cfg(test)]
mod r9_submit_wiring_tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;
    use tonic::Request;

    use super::{EngineExecution, EngineExecutionService};
    use crate::binance_futures::tests::{env_serial_guard, TestEnvVar};
    use crate::binance_futures::BinanceFuturesClient;
    use crate::proto::{ExecuteIntentsRequest, OrderIntent};
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    struct EnvGuard {
        name: String,
        previous: Option<OsString>,
    }

    impl EnvGuard {
        fn set(name: String, value: &str) -> Self {
            let previous = std::env::var_os(&name);
            std::env::set_var(&name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var(&self.name, value),
                None => std::env::remove_var(&self.name),
            }
        }
    }

    /// 装上一个交易所凭据的 venue 与密钥。
    ///
    /// 变量名由 `credential_secret_locator` 逐凭据生成（单射的 hex 编码），
    /// 所以每条用例用自己的 credential_id 就不会与并行用例抢同一个环境变量。
    /// API key 值也逐凭据区分，避免生产身份守卫把独立夹具识别成同 key 别名。
    /// 值是**本地假凭据**，不对应任何真实账户。
    fn exchange_credential_env(credential_id: &str) -> Vec<EnvGuard> {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        vec![
            EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet"),
            EnvGuard::set(key_env, &format!("local-test-key-{credential_id}")),
            EnvGuard::set(secret_env, "local-test-secret"),
        ]
    }

    async fn wiring_store(credential_id: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r9w-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        sqlx::query(
            r#"
            INSERT INTO engine_balances (
              credential_id, exchange, market_kind, asset,
              wallet_balance, available_balance, updated_at_ms
            ) VALUES (?, 'binance', 'futures', 'USDT', '10000', '10000', 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("insert engine balance truth");
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES (
              ?, ?, '10000', '10000', '0', 0,
              0, 0, 0, '{"majors":0,"l1_alts":0}', 'none', '5000'
            )
            "#,
        )
        .bind(credential_id)
        .bind(super::now_ms().to_string())
        .execute(store.pool())
        .await
        .expect("insert fresh factory risk budget");
        (store, path)
    }

    /// 一张可平的多头仓位（`state='open'`）。平仓路径据此拿到 position_id，
    /// 并把它推进到 `closing`——regression 的 exit release 副作用正是作用在
    /// 这一行上，所以它必须真实存在，否则"没有副作用"是废话。
    async fn insert_open_position(store: &Store, credential_id: &str, position_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, entry_price, updated_at_ms
            ) VALUES (?, ?, 'bot_r9w', 'binance|futures|BTC|USDT|USDT', 'long',
                      1, 0, 'open', 'bot_r9w|binance|futures|BTC|USDT|USDT|long|1',
                      '2', '100', 1)
            "#,
        )
        .bind(position_id)
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("insert closable long position");
    }

    async fn mark_fill_recovery_ready(store: &Store, credential_id: &str) {
        let cycle = crate::fill_replay::begin_recovery_cycle(
            store,
            credential_id,
            crate::fill_replay::RecoveryTrigger::Startup,
            10,
        )
        .await
        .expect("begin A7 fill recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            store,
            credential_id,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            cycle,
            20,
        )
        .await
        .expect("mark A7 fill recovery ready");
    }

    /// 平仓 intent。**用平仓腿而不是开仓**是刻意的：`ExceedsEffective` 这条
    /// stage 拒绝只对 Entry 生效（`stage_denial_applies`），因此平仓不需要动
    /// `ENGINE_STAGE_MAX` 这个**进程级**环境变量——用例之间不会互相污染。
    /// 提交路径（分类 → 写回）对两种 role 是同一段代码。
    fn close_intent(cycle_id: &str, order_type: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: "bot_r9w".to_string(),
            run_id: "run_r9w".to_string(),
            cycle_id: cycle_id.to_string(),
            idempotency_key: format!("idem_{cycle_id}"),
            client_order_id_short: format!("coid_{cycle_id}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: "close_long".to_string(),
            side: "long".to_string(),
            order_type: order_type.to_string(),
            price: "100".to_string(),
            quantity: "2".to_string(),
            reduce_only: true,
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        }
    }

    fn entry_intent(cycle_id: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: format!("bot_a7_{cycle_id}"),
            run_id: "run_a7".to_string(),
            cycle_id: cycle_id.to_string(),
            idempotency_key: format!("idem_{cycle_id}"),
            client_order_id_short: format!("coid_{cycle_id}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: "open_long".to_string(),
            side: "long".to_string(),
            order_type: "market".to_string(),
            price: "100".to_string(),
            quantity: String::new(),
            reduce_only: false,
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        }
    }

    async fn execute_entry_v2(
        service: &EngineExecutionService,
        credential_id: &str,
        intent: OrderIntent,
    ) -> Result<crate::proto::IntentAck, tonic::Status> {
        let mut contexts = std::collections::HashMap::new();
        contexts.insert(
            intent.idempotency_key.clone(),
            super::V2IntentContext {
                risk: crate::risk::WireRiskContext {
                    typed_protection_capable: true,
                    data_snapshot_capable: true,
                    data_snapshot: Some(risk_policy::DataSnapshot {
                        allow_open: true,
                        consistency_error: None,
                    }),
                    intent_id: format!("int_{}", intent.idempotency_key),
                    protection: Some(crate::risk::TypedProtectionInput {
                        stop_loss_price: "95".to_string(),
                        take_profit_price: "110".to_string(),
                    }),
                    valid_until_ms: None,
                    max_slippage_bps: None,
                    config_hash: String::new(),
                    prompt_hash: String::new(),
                    policy_version: String::new(),
                    code_commit: String::new(),
                },
                order_role: crate::protection::OrderRole::Entry,
                protection_required: false,
                confirm_timeout_ms: crate::protection::DEFAULT_CONFIRM_TIMEOUT_MS,
                protection_generation: 1,
            },
        );
        let mut request = Request::new(ExecuteIntentsRequest {
            credential_id: credential_id.to_string(),
            run_mode: "demo".to_string(),
            intents: vec![intent],
        });
        request
            .extensions_mut()
            .insert(super::V2ExecutionContext { intents: contexts });
        let response = service.execute_intents(request).await?.into_inner();
        Ok(response.acks.into_iter().next().expect("exactly one ack"))
    }

    fn wired_service(
        store: &Store,
        credential_id: &str,
        responses: Vec<(u16, String)>,
    ) -> EngineExecutionService {
        EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(BinanceFuturesClient::new_with_mock_responses(
                credential_id,
                responses,
            ))
    }

    fn market_ack_body(order_id: i64, client_order_id: &str) -> String {
        serde_json::json!({
            "orderId": order_id,
            "clientOrderId": client_order_id,
            "status": "NEW",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "origQty": "2",
            "executedQty": "0"
        })
        .to_string()
    }

    async fn execute_close(
        service: &EngineExecutionService,
        credential_id: &str,
        intent: OrderIntent,
    ) -> Result<crate::proto::IntentAck, tonic::Status> {
        let response = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.to_string(),
                run_mode: "demo".to_string(),
                intents: vec![intent],
            }))
            .await?
            .into_inner();
        Ok(response.acks.into_iter().next().expect("exactly one ack"))
    }

    struct ReferenceClock(std::sync::Arc<std::sync::atomic::AtomicI64>);

    impl super::Clock for ReferenceClock {
        fn now_ms(&self) -> i64 {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    struct ReferenceMarkSource {
        clock: std::sync::Arc<std::sync::atomic::AtomicI64>,
        shift_ms: i64,
        fail: bool,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::exchange_filters::ExchangeFilterSource for ReferenceMarkSource {
        fn fetch_filters<'a>(
            &'a self,
            _venue: crate::venue::Venue,
            _symbol: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::exchange_filters::SymbolFilters, String>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async { Ok(crate::exchange_filters::btcusdt_test_filters()) })
        }
        fn fetch_mark_price<'a>(
            &'a self,
            _venue: crate::venue::Venue,
            _symbol: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
        {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.clock
                    .fetch_add(self.shift_ms, std::sync::atomic::Ordering::SeqCst);
                if self.fail {
                    Err("test mark unavailable".to_string())
                } else {
                    Ok("100".to_string())
                }
            })
        }
    }

    #[tokio::test]
    async fn market_reference_guard_uses_post_fetch_clock_and_replays() {
        let _serial = env_serial_guard();
        for shift_ms in [45_000, -1, 0] {
            let credential = format!("market-ref-clock-{}", ulid::Ulid::new());
            let route = format!("market-ref-route-{}", ulid::Ulid::new());
            let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
            let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
            let _env = exchange_credential_env(&credential);
            let (store, path) = wiring_store(&credential).await;
            mark_fill_recovery_ready(&store, &credential).await;
            let before = super::now_ms();
            let time = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(before));
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let filters = crate::exchange_filters::ExchangeFilterCache::with_source(
                std::sync::Arc::new(ReferenceMarkSource {
                    clock: time.clone(),
                    shift_ms,
                    fail: false,
                    calls: calls.clone(),
                }),
                std::time::Duration::from_secs(300),
            );
            let client = BinanceFuturesClient::new_with_mock_responses(
                &credential,
                vec![(200, market_ack_body(987001, "coid_market_reference_clock"))],
            );
            let service = EngineExecutionService::new(store.clone())
                .without_background_exchange_tasks()
                .with_clock(std::sync::Arc::new(ReferenceClock(time)))
                .with_exchange_filters(filters)
                .with_submit_client_override(client.clone());
            let mut intent = entry_intent("market_reference_clock");
            intent.price.clear();
            let ack = execute_entry_v2(&service, &credential, intent)
                .await
                .unwrap();
            let row = sqlx::query(
                "SELECT risk_verdict_id,replay_input_json FROM risk_verdicts WHERE credential_id=?",
            )
            .bind(&credential)
            .fetch_one(store.pool())
            .await
            .unwrap();
            let raw = row.get::<String, _>("replay_input_json");
            let replay: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(replay["entry_market_reference"]["as_of_ms"], before);
            assert_eq!(
                replay["entry_market_reference"]["evaluated_at_ms"],
                before + shift_ms
            );
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "reuse the one pre-transaction mark request"
            );
            let repeated = crate::risk::replay_verdict(&raw).unwrap();
            assert_eq!(
                repeated.risk_verdict_id,
                row.get::<String, _>("risk_verdict_id")
            );
            if shift_ms == 0 {
                assert_eq!(ack.status, "accepted", "{ack:?}");
                assert_eq!(replay["input"]["intent"]["entry_ref_price"], 100.0);
                assert!(replay["entry_market_reference"]["unavailable_reason"].is_null());
                assert_eq!(client.recorded_requests().len(), 1);
            } else {
                assert_eq!(ack.status, "rejected", "{ack:?}");
                assert!(
                    ack.message.contains("EXCHANGE_FILTERS_UNAVAILABLE"),
                    "{ack:?}"
                );
                assert!(replay["input"]["intent"]["entry_ref_price"].is_null());
                assert_eq!(
                    replay["entry_market_reference"]["unavailable_reason"],
                    if shift_ms > 0 {
                        "venue_quote_observation_stale"
                    } else {
                        "venue_quote_observation_in_future"
                    }
                );
                assert!(
                    client.recorded_requests().is_empty(),
                    "unavailable quote must never submit"
                );
            }
            client.clear_egress_for_test();
            drop(service);
            cleanup(store, path).await;
        }
    }

    async fn native_market_v2(
        store: &Store,
        core: &EngineExecutionService,
        credential: &str,
        intent: OrderIntent,
    ) -> crate::proto_v2::IntentAck {
        use crate::proto_v2::engine_execution_server::EngineExecution as NativeV2;
        let service = crate::svc_v2::EngineExecutionV2Service::with_shared_execution(
            store.clone(),
            core.clone(),
        );
        let is_entry = intent.action.starts_with("open");
        let wire = crate::proto_v2::OrderIntent {
            bot_uid: intent.bot_uid,
            run_id: intent.run_id,
            cycle_id: intent.cycle_id,
            intent_id: format!("int_{}", intent.idempotency_key),
            idempotency_key: intent.idempotency_key,
            client_order_id_short: intent.client_order_id_short,
            exchange: intent.exchange,
            market_kind: intent.market_kind,
            symbol_key: intent.symbol_key,
            action: intent.action,
            order_type: intent.order_type,
            limit_price: intent.price,
            reduce_only: intent.reduce_only,
            protection: is_entry.then(|| crate::proto_v2::Protection {
                required: true,
                stop_loss_price: "99".to_string(),
                take_profit_price: "103".to_string(),
                confirm_timeout_ms: 3_000,
                schema_version: 1,
            }),
            max_slippage_bps: 15,
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: super::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        };
        NativeV2::execute_intents(
            &service,
            Request::new(crate::proto_v2::ExecuteIntentsRequest {
                credential_id: credential.to_string(),
                run_mode: "demo".to_string(),
                intents: vec![wire],
                data_snapshot: Some(crate::proto_v2::DataSnapshotDecision {
                    snapshot_id: format!("snap_{}", "a".repeat(32)),
                    quality_gate: "pass".to_string(),
                    allow_open: true,
                    allow_close: true,
                    created_at_ms: super::now_ms(),
                    schema_version: 1,
                }),
                schema_version: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .acks
        .remove(0)
    }

    async fn assert_native_market_entry_reference(account_row: bool, client_price: &str) {
        let _serial = env_serial_guard();
        let credential = format!("market-ref-native-{}", ulid::Ulid::new());
        let route = format!("market-ref-native-route-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential);
        let (store, path) = wiring_store(&credential).await;
        mark_fill_recovery_ready(&store, &credential).await;
        if account_row {
            sqlx::query("INSERT INTO engine_positions (credential_id,exchange,market_kind,symbol,symbol_key,position_amt,entry_price,mark_price,leverage,unrealized_pnl,updated_at_ms) VALUES (?,'binance','futures','BTCUSDT',NULL,'0','0','777','20','0',?)")
                .bind(&credential).bind(super::now_ms()).execute(store.pool()).await.unwrap();
        }
        let mark_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let filters = crate::exchange_filters::ExchangeFilterCache::with_source(
            std::sync::Arc::new(ReferenceMarkSource {
                clock: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(super::now_ms())),
                shift_ms: 0,
                fail: false,
                calls: mark_calls.clone(),
            }),
            std::time::Duration::from_secs(300),
        );
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![(200, market_ack_body(987002, "coid_market_reference_native"))],
        );
        let core = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(filters)
            .with_submit_client_override(client.clone());
        let mut intent = entry_intent("market_reference_native");
        intent.price = client_price.to_string();
        let ack = native_market_v2(&store, &core, &credential, intent).await;
        let row = sqlx::query("SELECT sizing_json,rules_json,replay_input_json FROM risk_verdicts WHERE credential_id=?")
            .bind(&credential).fetch_one(store.pool()).await.unwrap();
        let raw = row.get::<String, _>("replay_input_json");
        let replay: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(replay["input"]["intent"]["entry_ref_price"], 100.0, "{raw}");
        assert_eq!(
            replay["input"]["intent"]["requested_leverage"],
            if account_row { 20 } else { 1 }
        );
        assert_eq!(
            replay["entry_market_reference"]["source"],
            "binance_public_mark_price"
        );
        assert!(replay["entry_market_reference"]["unavailable_reason"].is_null());
        assert_eq!(mark_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let repeated = crate::risk::replay_verdict(&raw).unwrap();
        assert_eq!(repeated.risk_verdict_id, ack.risk_verdict_id);
        if client_price.is_empty() {
            assert_eq!(
                ack.status,
                crate::proto_v2::Status::Accepted as i32,
                "{ack:?}"
            );
            let requests = client.recorded_requests();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].contains("symbol=BTCUSDT"));
            let quantity: f64 = requests[0]
                .split("quantity=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                quantity > 0.0 && quantity * 100.0 <= 1000.0,
                "submitted notional exceeds cap: {quantity}"
            );
            let sizing: serde_json::Value =
                serde_json::from_str(&row.get::<String, _>("sizing_json")).unwrap();
            assert!(
                sizing["final_notional_usdt"]
                    .as_str()
                    .unwrap()
                    .parse::<f64>()
                    .unwrap()
                    <= 1000.0
            );
            if account_row {
                assert_eq!(sizing["leverage"], 10);
            }
            let rules: serde_json::Value =
                serde_json::from_str(&row.get::<String, _>("rules_json")).unwrap();
            assert!(rules
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["rule"] == "cost_aware_tp" && r["result"] == "pass"));
        } else {
            // Even a malformed market wire containing a limit-price value
            // cannot supply the risk reference. The existing order filter may
            // reject that extra price rather than silently altering the wire.
            assert_eq!(ack.status, crate::proto_v2::Status::Rejected as i32);
            assert!(client.recorded_requests().is_empty());
        }
        client.clear_egress_for_test();
        drop(core);
        cleanup(store, path).await;
    }

    // Independent fixtures may coexist in a parallel test process. Distinct
    // simulated credentials must not look like aliases of one API key to the
    // production identity guard before it reaches the market-reference check.
    #[tokio::test]
    async fn independent_submit_fixture_preserves_native_market_reference() {
        let other_credential = format!("market-ref-fixture-peer-{}", ulid::Ulid::new());
        let _other_env = exchange_credential_env(&other_credential);
        assert_native_market_entry_reference(true, "").await;
    }

    #[tokio::test]
    async fn market_reference_native_v2_first_entry_needs_no_account_binding() {
        assert_native_market_entry_reference(false, "").await;
    }

    #[tokio::test]
    async fn market_reference_native_v2_null_binding_retains_leverage_but_ignores_account_price() {
        assert_native_market_entry_reference(true, "").await;
    }

    #[tokio::test]
    async fn market_reference_native_v2_never_uses_client_market_price() {
        assert_native_market_entry_reference(false, "777").await;
    }

    #[tokio::test]
    async fn market_reference_native_v2_close_does_not_fetch_or_consume_quote() {
        let _serial = env_serial_guard();
        let credential = format!("market-ref-close-{}", ulid::Ulid::new());
        let route = format!("market-ref-close-route-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential);
        let (store, path) = wiring_store(&credential).await;
        insert_open_position(&store, &credential, "pos_market_reference_close").await;
        let mark_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let filters = crate::exchange_filters::ExchangeFilterCache::with_source(
            std::sync::Arc::new(ReferenceMarkSource {
                clock: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(super::now_ms())),
                shift_ms: 45_000,
                fail: false,
                calls: mark_calls.clone(),
            }),
            std::time::Duration::from_secs(300),
        );
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential,
            vec![(200, market_ack_body(987003, "coid_market_reference_close"))],
        );
        let core = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(filters)
            .with_submit_client_override(client.clone());
        let mut intent = close_intent("market_reference_close", "market");
        intent.price.clear();
        let ack = native_market_v2(&store, &core, &credential, intent).await;
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "{ack:?}"
        );
        assert_eq!(mark_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(client.recorded_requests().len(), 1);
        let raw: String =
            sqlx::query("SELECT replay_input_json FROM risk_verdicts WHERE credential_id=?")
                .bind(&credential)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get("replay_input_json");
        let replay: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(
            replay["entry_market_reference"].is_null(),
            "close sentinel must never be a quote"
        );
        assert!(replay["input"]["intent"]["entry_ref_price"].is_null());
        client.clear_egress_for_test();
        drop(core);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn market_reference_native_v2_unavailable_quote_never_falls_back_to_account_price() {
        let _serial = env_serial_guard();
        let credential = format!("market-ref-unavailable-{}", ulid::Ulid::new());
        let route = format!("market-ref-unavailable-route-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential);
        let (store, path) = wiring_store(&credential).await;
        mark_fill_recovery_ready(&store, &credential).await;
        sqlx::query("INSERT INTO engine_positions (credential_id,exchange,market_kind,symbol,symbol_key,position_amt,entry_price,mark_price,leverage,unrealized_pnl,updated_at_ms) VALUES (?,'binance','futures','BTCUSDT','binance|futures|BTC|USDT|USDT','0','0','777','20','0',?)")
            .bind(&credential).bind(super::now_ms()).execute(store.pool()).await.unwrap();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let filters = crate::exchange_filters::ExchangeFilterCache::with_source(
            std::sync::Arc::new(ReferenceMarkSource {
                clock: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(super::now_ms())),
                shift_ms: 0,
                fail: true,
                calls: calls.clone(),
            }),
            std::time::Duration::from_secs(300),
        );
        let client = BinanceFuturesClient::new_with_mock_responses(&credential, vec![]);
        let core = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(filters)
            .with_submit_client_override(client.clone());
        let mut intent = entry_intent("market_reference_unavailable");
        intent.price.clear();
        let ack = native_market_v2(&store, &core, &credential, intent).await;
        assert_eq!(ack.status, crate::proto_v2::Status::Rejected as i32);
        assert_eq!(ack.reject_reason_code, "EXCHANGE_FILTERS_UNAVAILABLE");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(client.recorded_requests().is_empty());
        let raw: String =
            sqlx::query("SELECT replay_input_json FROM risk_verdicts WHERE credential_id=?")
                .bind(&credential)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get("replay_input_json");
        let replay: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(replay["input"]["intent"]["entry_ref_price"].is_null());
        assert_eq!(
            replay["entry_market_reference"]["unavailable_reason"],
            "venue_constraints_unavailable"
        );
        assert_eq!(
            crate::risk::replay_verdict(&raw).unwrap().risk_verdict_id,
            ack.risk_verdict_id
        );
        client.clear_egress_for_test();
        drop(core);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn exhausted_protectable_scope_rejects_entry_without_transport_but_executes_close() {
        let _serial = env_serial_guard();
        let credential_id = format!("a7-wire-budget-{}", ulid::Ulid::new());
        let route = format!("a7-wire-budget-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _budget = TestEnvVar::set(
            "RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M".to_string(),
            "13",
        );
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential_id);
        let (store, path) = wiring_store(&credential_id).await;
        mark_fill_recovery_ready(&store, &credential_id).await;
        insert_open_position(&store, &credential_id, "pos_a7_budget_close").await;
        let scope = crate::binance_futures::egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
        )
        .expect("derive production egress scope");
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential_id,
            vec![(200, market_ack_body(970001, "coid_a7_budget_close"))],
        )
        .with_egress_scope_for_test(scope)
        .with_protectable_budget_for_test(13.0);
        let service = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(client.clone());

        let entry = execute_entry_v2(&service, &credential_id, entry_intent("a7_budget_entry"))
            .await
            .expect("ExecuteIntents entry response");
        assert_eq!(entry.status, "rejected", "entry: {entry:?}");
        assert!(entry
            .message
            .contains("EGRESS_PROTECTABLE_BUDGET_EXHAUSTED"));
        assert!(
            client.recorded_requests().is_empty(),
            "a rejected entry must make zero outbound requests"
        );
        let rejected_order = sqlx::query(
            "SELECT status, position_id, idempotency_key FROM engine_orders \
             WHERE engine_order_id=?",
        )
        .bind(&entry.engine_order_id)
        .fetch_one(store.pool())
        .await
        .expect("load live-rejected Entry order");
        assert_eq!(rejected_order.get::<String, _>("status"), "rejected");
        let rejected_position_id = rejected_order.get::<String, _>("position_id");
        let rejected_claim = sqlx::query(
            "SELECT state, entry_epoch, active_key IS NULL AS active_key_is_null, \
                    pending_intent_id IS NULL AS pending_intent_is_null, \
                    pending_idem_key IS NULL AS pending_idem_is_null \
             FROM position_state WHERE position_id=?",
        )
        .bind(&rejected_position_id)
        .fetch_one(store.pool())
        .await
        .expect("load retained live-rejected Entry epoch");
        assert_eq!(rejected_claim.get::<String, _>("state"), "closed");
        assert_eq!(rejected_claim.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(rejected_claim.get::<i64, _>("active_key_is_null"), 1);
        assert_eq!(rejected_claim.get::<i64, _>("pending_intent_is_null"), 1);
        assert_eq!(rejected_claim.get::<i64, _>("pending_idem_is_null"), 1);
        let rejected_plans = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?",
        )
        .bind(&rejected_position_id)
        .fetch_one(store.pool())
        .await
        .expect("count live-rejected protection plans")
        .get::<i64, _>("count");
        assert_eq!(rejected_plans, 0);
        let reserved =
            sqlx::query("SELECT reserved_usdt FROM notional_reservations WHERE credential_id=?")
                .bind(&credential_id)
                .fetch_one(store.pool())
                .await
                .expect("load live-rejected reservation")
                .get::<String, _>("reserved_usdt")
                .parse::<f64>()
                .expect("numeric live-rejected reservation");
        assert_eq!(reserved, 0.0);

        let mut next_intent = entry_intent("a7_budget_entry_next");
        next_intent.bot_uid = "bot_a7_a7_budget_entry".to_string();
        let next_entry = execute_entry_v2(&service, &credential_id, next_intent)
            .await
            .expect("next Entry after live rejection");
        assert_eq!(next_entry.status, "rejected", "next Entry: {next_entry:?}");
        let expected_epoch_2 = crate::position_state::semantic_idempotency_key(
            "entry|bot_a7_a7_budget_entry|binance|futures|BTC|USDT|USDT|long|2",
        );
        let next_key =
            sqlx::query("SELECT idempotency_key FROM engine_orders WHERE engine_order_id=?")
                .bind(&next_entry.engine_order_id)
                .fetch_one(store.pool())
                .await
                .expect("load next live-rejected Entry key")
                .get::<String, _>("idempotency_key");
        assert_eq!(next_key, expected_epoch_2);

        let close = execute_close(
            &service,
            &credential_id,
            close_intent("a7_budget_close", "market"),
        )
        .await
        .expect("ExecuteIntents close response");
        assert_eq!(close.status, "accepted", "close: {close:?}");
        assert_eq!(
            client.recorded_requests().len(),
            1,
            "close must reach transport"
        );
        client.clear_egress_for_test();
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn live_reject_cas_zero_preserves_the_pending_entry_authority() {
        let _serial = env_serial_guard();
        let credential_id = format!("live-reject-cas-zero-{}", ulid::Ulid::new());
        let route = format!("live-reject-cas-zero-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _budget = TestEnvVar::set(
            "RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M".to_string(),
            "13",
        );
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential_id);
        let (store, path) = wiring_store(&credential_id).await;
        mark_fill_recovery_ready(&store, &credential_id).await;
        insert_open_position(&store, &credential_id, "pos_live_reject_cas_zero_existing").await;
        sqlx::query(
            r#"
            CREATE TRIGGER live_reject_loses_status_cas
            BEFORE UPDATE OF status ON engine_orders
            WHEN OLD.status='pending_risk' AND NEW.status='rejected'
            BEGIN
              UPDATE engine_orders SET status='canceled' WHERE engine_order_id=OLD.engine_order_id;
              SELECT RAISE(IGNORE);
            END
            "#,
        )
        .execute(store.pool())
        .await
        .expect("install live-reject CAS interleaving");
        let scope = crate::binance_futures::egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
        )
        .expect("derive production egress scope");
        let client = BinanceFuturesClient::new_with_mock_responses(&credential_id, Vec::new())
            .with_egress_scope_for_test(scope)
            .with_protectable_budget_for_test(13.0);
        let service = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(client.clone());

        let ack = execute_entry_v2(
            &service,
            &credential_id,
            entry_intent("live_reject_cas_zero"),
        )
        .await
        .expect("ExecuteIntents live-reject CAS-zero response");
        assert_eq!(ack.status, "rejected", "ack={ack:?}");
        assert!(client.recorded_requests().is_empty());
        let order =
            sqlx::query("SELECT status, position_id FROM engine_orders WHERE engine_order_id=?")
                .bind(&ack.engine_order_id)
                .fetch_one(store.pool())
                .await
                .expect("load interleaved live-reject order");
        assert_eq!(order.get::<String, _>("status"), "canceled");
        let position_id = order.get::<String, _>("position_id");
        let reservation =
            sqlx::query("SELECT reserved_usdt FROM notional_reservations WHERE credential_id=?")
                .bind(&credential_id)
                .fetch_one(store.pool())
                .await
                .expect("load retained live-reject reservation")
                .get::<String, _>("reserved_usdt")
                .parse::<f64>()
                .expect("numeric retained reservation");
        assert!(reservation > 0.0);
        let claim = sqlx::query(
            "SELECT state, entry_epoch, active_key IS NOT NULL AS has_active_key, \
                    pending_idem_key IS NOT NULL AS has_pending_idem_key \
             FROM position_state WHERE position_id=?",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .expect("load retained live-reject claim");
        assert_eq!(claim.get::<String, _>("state"), "pending");
        assert_eq!(claim.get::<i64, _>("entry_epoch"), 1);
        assert_eq!(claim.get::<i64, _>("has_active_key"), 1);
        assert_eq!(claim.get::<i64, _>("has_pending_idem_key"), 1);
        let plans = sqlx::query(
            "SELECT COUNT(*) AS count FROM position_protection_plans WHERE position_id=?",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .expect("count retained live-reject plans")
        .get::<i64, _>("count");
        assert_eq!(plans, 1);

        client.clear_egress_for_test();
        drop(service);
        cleanup(store, path).await;
    }

    #[test]
    fn protectable_admission_source_guard_is_entry_only() {
        let source = include_str!("svc.rs");
        let gate = source
            .split_once("// A7: this admission is entry-only by construction.")
            .expect("A7 production gate marker exists")
            .1
            .split_once("if live_reject.is_none() {")
            .expect("submission planning follows A7 gate")
            .0;
        assert!(
            gate.contains("if requested_order_role == OrderRole::Entry && live_reject.is_none() {"),
            "A7 exposure admission must remain structurally limited to Entry"
        );
        for forbidden_role in [
            "OrderRole::Exit",
            "OrderRole::ProtectionSl",
            "OrderRole::ProtectionTp",
        ] {
            assert!(
                !gate.contains(forbidden_role),
                "A7 exposure admission must not mention {forbidden_role}"
            );
        }
    }

    #[tokio::test]
    async fn single_candidate_at_exact_budget_is_not_double_counted_after_pending_claim() {
        let _serial = env_serial_guard();
        let credential_id = format!("a7-wire-boundary-{}", ulid::Ulid::new());
        let route = format!("a7-wire-boundary-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _budget = TestEnvVar::set(
            "RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M".to_string(),
            "13",
        );
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential_id);
        let (store, path) = wiring_store(&credential_id).await;
        mark_fill_recovery_ready(&store, &credential_id).await;
        let scope = crate::binance_futures::egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
        )
        .expect("derive production egress scope");
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential_id,
            vec![(200, market_ack_body(970003, "coid_a7_boundary_entry"))],
        )
        .with_egress_scope_for_test(scope)
        .with_protectable_budget_for_test(13.0);
        let service = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(client.clone());

        let entry = execute_entry_v2(&service, &credential_id, entry_intent("a7_boundary_entry"))
            .await
            .expect("ExecuteIntents entry response");
        assert_eq!(entry.status, "accepted", "entry: {entry:?}");
        assert_eq!(client.recorded_requests().len(), 1);
        client.clear_egress_for_test();
        drop(service);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn unknown_egress_exposure_rejects_entry_but_never_blocks_close() {
        let _serial = env_serial_guard();
        let credential_id = format!("a7-wire-unknown-{}", ulid::Ulid::new());
        let route = format!("a7-wire-unknown-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _stage = TestEnvVar::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let _env = exchange_credential_env(&credential_id);
        let (store, path) = wiring_store(&credential_id).await;
        mark_fill_recovery_ready(&store, &credential_id).await;
        insert_open_position(&store, &credential_id, "pos_a7_unknown_close").await;
        let scope = crate::binance_futures::egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
        )
        .expect("derive production egress scope");
        let client = BinanceFuturesClient::new_with_mock_responses(
            &credential_id,
            vec![(200, market_ack_body(970002, "coid_a7_unknown_close"))],
        )
        .with_egress_scope_for_test(scope)
        .with_admission_exposure_error("injected exposure SELECT failure");
        let service = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(client.clone());

        let entry = execute_entry_v2(&service, &credential_id, entry_intent("a7_unknown_entry"))
            .await
            .expect("ExecuteIntents entry response");
        assert_eq!(entry.status, "rejected", "entry: {entry:?}");
        assert!(
            entry.message.contains("EGRESS_EXPOSURE_UNKNOWN"),
            "entry: {entry:?}"
        );
        assert!(client.recorded_requests().is_empty());

        let close = execute_close(
            &service,
            &credential_id,
            close_intent("a7_unknown_close", "market"),
        )
        .await
        .expect("ExecuteIntents close response");
        assert_eq!(close.status, "accepted", "close: {close:?}");
        assert_eq!(
            client.recorded_requests().len(),
            1,
            "close must reach transport"
        );
        client.clear_egress_for_test();
        drop(service);
        cleanup(store, path).await;
    }

    async fn order_row(store: &Store, credential_id: &str) -> (String, String) {
        let row = sqlx::query(
            "SELECT status, COALESCE(exchange_order_id, '') AS exchange_order_id \
             FROM engine_orders WHERE credential_id = ? ORDER BY created_at_ms DESC LIMIT 1",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("the production entry must have inserted an engine order");
        (
            row.get::<String, _>("status"),
            row.get::<String, _>("exchange_order_id"),
        )
    }

    async fn count(store: &Store, sql: &str, bind: &str) -> i64 {
        sqlx::query(sql)
            .bind(bind)
            .fetch_one(store.pool())
            .await
            .expect("count query")
            .get::<i64, _>("n")
    }

    async fn credential_error_count(store: &Store, credential_id: &str) -> i64 {
        count(
            store,
            "SELECT COUNT(*) AS n FROM engine_credential_health \
             WHERE credential_id = ? AND consecutive_errors > 0",
            credential_id,
        )
        .await
    }

    async fn event_count(store: &Store, credential_id: &str, event_type: &str) -> i64 {
        sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events \
             WHERE credential_id = ? AND event_type = ?",
        )
        .bind(credential_id)
        .bind(event_type)
        .fetch_one(store.pool())
        .await
        .expect("count events")
        .get::<i64, _>("n")
    }

    /// exit 释放的三个真值：状态、epoch、pending 幂等键。
    /// `exit_epoch` 与 `pending_idem_key` 才是"下一次 close 能不能拿到新键"
    /// 的决定项——只看 `state` 会漏掉重复 exit 的实际成因。
    async fn exit_release_state(store: &Store, position_id: &str) -> (String, i64, String) {
        let row = sqlx::query(
            "SELECT state, exit_epoch, COALESCE(pending_idem_key,'') AS pending_idem_key \
             FROM position_state WHERE position_id = ?",
        )
        .bind(position_id)
        .fetch_one(store.pool())
        .await
        .expect("read exit release state");
        (
            row.get::<String, _>("state"),
            row.get::<i64, _>("exit_epoch"),
            row.get::<String, _>("pending_idem_key"),
        )
    }

    async fn position_state_of(store: &Store, position_id: &str) -> String {
        sqlx::query("SELECT state FROM position_state WHERE position_id = ?")
            .bind(position_id)
            .fetch_one(store.pool())
            .await
            .expect("read position state")
            .get::<String, _>("state")
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **`Acked`**：交易所明确应答 → 库里必须是 `acked` + 交易所身份。
    ///
    /// 这条是变异 M11（`Ok(ack) => StartedUnknown`）的杀手：错分之后这一行
    /// 会变成 `unknown`，断言直接红。R8 的 `Acked` 用例做不到——它自己造
    /// enum，根本没经过 `place_market_order` 的 `Ok/Err` 分叉。
    #[tokio::test]
    async fn an_exchange_ack_reaches_the_database_as_acked_through_the_production_entry() {
        let credential_id = "r9w-acked";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r9w_acked").await;
        let service = wired_service(
            &store,
            credential_id,
            vec![(200, market_ack_body(880011, "coid_r9w_acked"))],
        );

        let ack = execute_close(&service, credential_id, close_intent("r9w_acked", "market"))
            .await
            .expect("execute_intents must succeed");
        assert_ne!(ack.engine_order_id, "", "ack: {ack:?}");

        let (status, exchange_order_id) = order_row(&store, credential_id).await;
        assert_eq!(status, "acked", "a real exchange ACK must land as `acked`");
        assert_eq!(exchange_order_id, "880011");
        assert_eq!(event_count(&store, credential_id, "order_ack").await, 1);
        assert_eq!(
            credential_error_count(&store, credential_id).await,
            0,
            "a successful ack must not leave a credential error"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **`StartedUnknown`**：请求交给了客户端但结果不明 → `unknown`
    /// （让 reconcile 按交易所真值复查），并记 credential error。
    ///
    /// 构造方式是**耗尽内存响应队列**：客户端在真实的 HTTP 发送点失败，
    /// 与超时/连接中断走的是同一条 `Err` 分支。
    #[tokio::test]
    async fn an_unreachable_exchange_reaches_the_database_as_unknown() {
        let credential_id = "r9w-unknown";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r9w_unknown").await;
        let service = wired_service(&store, credential_id, Vec::new());

        execute_close(
            &service,
            credential_id,
            close_intent("r9w_unknown", "market"),
        )
        .await
        .expect("execute_intents must succeed");

        let (status, _) = order_row(&store, credential_id).await;
        assert_eq!(
            status, "unknown",
            "an exchange outcome we cannot observe must stay trackable by reconcile"
        );
        assert_eq!(
            credential_error_count(&store, credential_id).await,
            1,
            "an unknown exchange outcome is a credential-level error"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **regression（High）· 真值未知时 exit 不得被释放。**
    ///
    /// 评审反例：从 `execute_intents` 出发，`StartedUnknown` 之后 exit 被释放
    /// ——`exit_epoch+1`、`pending_idem_key` 清空，于是下一次 close 拿到一把
    /// **新的幂等键**，绕开 `engine_orders` 的唯一键，同一个仓位被平第二次；
    /// 而交易所侧那张"结果不明"的单可能正活着（`svc.rs` 自己的注释写着
    /// "交易所可能有活单"）。
    ///
    /// 这里断言的是**库里的三件事**：仓位仍 `closing`、`exit_epoch` 未变、
    /// `pending_idem_key` 未被清空——第三件正是"下一次 close 会不会拿到新键"
    /// 的直接真值。
    #[tokio::test]
    async fn an_unknown_exchange_outcome_must_not_release_the_exit() {
        let credential_id = "r10w-unknown-exit";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r10w_unknown").await;
        let before = exit_release_state(&store, "pos_r10w_unknown").await;
        assert_eq!(before.0, "open");

        // 空响应队列 = 真实发送点失败 = `StartedUnknown`（与超时/连接中断同支）。
        let service = wired_service(&store, credential_id, Vec::new());
        execute_close(
            &service,
            credential_id,
            close_intent("r10w_unknown", "market"),
        )
        .await
        .expect("execute_intents must answer");

        let (status, _) = order_row(&store, credential_id).await;
        assert_eq!(
            status, "unknown",
            "the order must stay trackable by reconcile"
        );

        let (state, epoch, pending_key) = exit_release_state(&store, "pos_r10w_unknown").await;
        assert_eq!(
            state, "closing",
            "an exchange outcome we cannot observe is not a proof that no live order exists"
        );
        assert_eq!(
            epoch, before.1,
            "exit_epoch must not advance without that proof"
        );
        assert!(
            !pending_key.is_empty(),
            "clearing the pending idempotency key is what hands the next close a fresh key"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **regression · CAS=0 的那一支同样不得释放**（段级终轮点名）。
    ///
    /// 构造：一个 `AFTER INSERT` 触发器在订单落库的瞬间把它改成 `canceled`
    /// ——这就是 kill-switch 在提交前撤单的真实形态。随后"从未联到交易所"
    /// 的写回（缺密钥）带 CAS 执行，影响 **0 行**。
    /// 影响 0 行意味着"这一行现在带着什么真值我们并不知道"，
    /// 而"不知道"不是"交易所没有活单"的证明。
    #[tokio::test]
    async fn a_zero_row_cas_must_not_release_the_exit() {
        let credential_id = "r10w-cas0-exit";
        let _venue = EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet");
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let _no_key = EnvGuard::set(key_env, "");
        let _no_secret = EnvGuard::set(secret_env, "");
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r10w_cas0").await;
        let before = exit_release_state(&store, "pos_r10w_cas0").await;
        sqlx::query(
            "CREATE TRIGGER r10_cancel_on_insert AFTER INSERT ON engine_orders \
             FOR EACH ROW BEGIN UPDATE engine_orders SET status='canceled' \
             WHERE engine_order_id = NEW.engine_order_id; END",
        )
        .execute(store.pool())
        .await
        .expect("install the pre-submit cancellation trigger");

        let service = wired_service(&store, credential_id, Vec::new());
        execute_close(&service, credential_id, close_intent("r10w_cas0", "market"))
            .await
            .expect("execute_intents must answer");

        let (status, _) = order_row(&store, credential_id).await;
        assert_eq!(
            status, "canceled",
            "the CAS must not overwrite the truth another path already wrote"
        );
        let (state, epoch, pending_key) = exit_release_state(&store, "pos_r10w_cas0").await;
        assert_eq!(
            state, "closing",
            "a 0-row CAS proves nothing about the exchange"
        );
        assert_eq!(epoch, before.1);
        assert!(!pending_key.is_empty());

        drop(service);
        cleanup(store, path).await;
    }

    /// **放行面自证**：请求**确实从未离开本进程**、且这一行确实由我们这次
    /// 推进（CAS 影响 1 行）时，closing 必须**立刻**被释放——否则仓位要等一
    /// 整个 reconcile 周期才能重平，AUD-P2-H6 那条死锁就回来了。
    ///
    /// 没有这条，把释放整段删掉也能让上面两条全绿。
    #[tokio::test]
    async fn a_failure_that_never_reached_the_exchange_still_releases_the_exit() {
        let credential_id = "r10w-nokeys-exit";
        let _venue = EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet");
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let _no_key = EnvGuard::set(key_env, "");
        let _no_secret = EnvGuard::set(secret_env, "");
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r10w_nokeys").await;
        let before = exit_release_state(&store, "pos_r10w_nokeys").await;

        let service = wired_service(&store, credential_id, Vec::new());
        execute_close(
            &service,
            credential_id,
            close_intent("r10w_nokeys", "market"),
        )
        .await
        .expect("execute_intents must answer");

        let (state, epoch, pending_key) = exit_release_state(&store, "pos_r10w_nokeys").await;
        assert_eq!(
            state, "open",
            "a request that never left the process cannot have left a live order"
        );
        assert_eq!(epoch, before.1 + 1, "the next close must get a fresh key");
        assert!(pending_key.is_empty());

        drop(service);
        cleanup(store, path).await;
    }

    /// **`NotStarted`**：请求从未离开本进程 → 本地拒绝（带 CAS 的 `rejected`），
    /// **不记** credential error（本地参数问题不得去拉断路器）。
    ///
    /// 驱动方式是一个路由不合法的 order_type：判据在
    /// `plan_exchange_submission`，而这条用例走的是它在生产里的调用点。
    #[tokio::test]
    async fn a_request_that_never_left_the_process_reaches_the_database_as_rejected() {
        let credential_id = "r9w-notstarted";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r9w_notstarted").await;
        // 响应队列**故意留空**：这条路径一次 HTTP 都不该发。任何真的发出去的
        // 请求都会因为队列耗尽而变成 `StartedUnknown`，断言随之变红。
        let service = wired_service(&store, credential_id, Vec::new());

        let ack = execute_close(
            &service,
            credential_id,
            close_intent("r9w_notstarted", "iceberg"),
        )
        .await
        .expect("execute_intents must succeed");
        assert!(
            ack.message.contains("submission not started"),
            "ack message: {}",
            ack.message
        );

        let (status, exchange_order_id) = order_row(&store, credential_id).await;
        assert_eq!(status, "rejected");
        assert_eq!(exchange_order_id, "");
        assert_eq!(
            credential_error_count(&store, credential_id).await,
            0,
            "a purely local rejection must not trip the credential breaker"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **regression 的另一半**：连交易所都没联到（缺密钥）的那条写回同样
    /// 不得吞掉 DB 错误。
    ///
    /// 这一支走的是 `finalize_pre_submit_status`（带提交前状态集的 CAS）。
    /// 它此前是 `let _ = ...`：DB 一故障就静默继续，照样记 credential error、
    /// 照样把 closing 释放掉，而订单行仍停在 `pending_risk`——
    /// 与 unknown 那一支同一个形状。
    ///
    /// 只装 venue、**不装密钥**，让 `keys_from_env` 返回 None：
    /// 这是"从未联到交易所"的真实构造，不是合成的错误。
    #[tokio::test]
    async fn a_database_failure_on_the_pre_submit_writeback_must_not_release_exit_state() {
        let credential_id = "r9w-presubmit";
        let _venue = EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet");
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let _no_key = EnvGuard::set(key_env, "");
        let _no_secret = EnvGuard::set(secret_env, "");
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r9w_presubmit").await;
        sqlx::query(
            "CREATE TRIGGER r9_block_presubmit_writeback BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'unknown' \
             BEGIN SELECT RAISE(ABORT, 'injected pre-submit write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let service = wired_service(&store, credential_id, Vec::new());
        let outcome = execute_close(
            &service,
            credential_id,
            close_intent("r9w_presubmit", "market"),
        )
        .await;

        let error = outcome.expect_err("a database failure must surface as an RPC error");
        assert_eq!(error.code(), tonic::Code::Internal);
        let (status, _) = order_row(&store, credential_id).await;
        assert_eq!(status, "pending_risk");
        assert_eq!(credential_error_count(&store, credential_id).await, 0);
        assert_eq!(event_count(&store, credential_id, "error").await, 0);
        assert_eq!(
            position_state_of(&store, "pos_r9w_presubmit").await,
            "closing"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **regression**：`unknown` 写回的 DB 故障必须上抛，
    /// 且**不得**进入 credential error / exit release 语义。
    ///
    /// 故障是真的：一个只在 `status='unknown'` 时 ABORT 的 SQLite 触发器。
    /// 提交闸门那条 `pending_submit` 的 UPDATE 不受影响，因此故障被精确地
    /// 注入到被判对象上，而不是"把整个库弄坏"。
    ///
    /// 反例形态就是 R8 的实现：`.unwrap_or(0)` 会把这次故障读成"0 行 /
    /// 已有更强真值"，然后打印那条语义不真的告警、记 credential error、
    /// 并把 closing 释放回去——一张可能真实存在的活单就此失去追踪。
    #[tokio::test]
    async fn a_database_failure_on_the_unknown_writeback_must_not_release_exit_state() {
        let credential_id = "r9w-casfail";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position(&store, credential_id, "pos_r9w_casfail").await;
        sqlx::query(
            "CREATE TRIGGER r9_block_unknown_writeback BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'unknown' \
             BEGIN SELECT RAISE(ABORT, 'injected unknown write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let service = wired_service(&store, credential_id, Vec::new());
        let outcome = execute_close(
            &service,
            credential_id,
            close_intent("r9w_casfail", "market"),
        )
        .await;

        let error = outcome.expect_err("a database failure must surface as an RPC error");
        assert_eq!(error.code(), tonic::Code::Internal);

        let (status, _) = order_row(&store, credential_id).await;
        assert_eq!(
            status, "pending_submit",
            "the row must keep the state the failed write could not change"
        );
        assert_eq!(
            credential_error_count(&store, credential_id).await,
            0,
            "a database failure must not be reported as a credential error"
        );
        assert_eq!(
            event_count(&store, credential_id, "error").await,
            0,
            "a database failure must not emit the submit-error audit event"
        );
        assert_eq!(
            position_state_of(&store, "pos_r9w_casfail").await,
            "closing",
            "exit release must not run when the unknown write-back could not be evaluated"
        );

        drop(service);
        cleanup(store, path).await;
    }

    // ==================================================================
    // 提交侧完整身份的生产入口 wiring proof
    // ==================================================================

    async fn insert_open_position_with_key(
        store: &Store,
        credential_id: &str,
        position_id: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, entry_price, updated_at_ms
            ) VALUES (?, ?, 'bot_r9w', ?, 'long', 1, 0, 'open', ?, '2', '100', 1)
            "#,
        )
        .bind(position_id)
        .bind(credential_id)
        .bind(symbol_key)
        .bind(format!("bot_r9w|{symbol_key}|long|1"))
        .execute(store.pool())
        .await
        .expect("insert closable long position");
    }

    fn close_intent_with_symbol_key(cycle_id: &str, symbol_key: &str) -> OrderIntent {
        let mut intent = close_intent(cycle_id, "market");
        intent.symbol_key = symbol_key.to_string();
        intent
    }

    /// 提交侧身份用例的公共骨架。
    ///
    /// **每一层都真的到位**：真实凭据 env、真实余额/风控预算、一条与 intent
    /// **同 key** 的可平仓位、一个已排好成功回执的交易所替身。因此除了
    /// `symbol_key` 自己声明的身份，没有任何别的理由会挡下这条 intent——
    /// 断言到的拒绝只可能来自被测判据。
    async fn submit_with_symbol_key(
        credential_id: &str,
        cycle_id: &str,
        symbol_key: &str,
    ) -> (crate::proto::IntentAck, Vec<String>, i64) {
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        insert_open_position_with_key(&store, credential_id, "pos_r12w", symbol_key).await;
        let client = BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![(200, market_ack_body(990011, "coid_r12w"))],
        );
        let service = EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(client.clone());

        let ack = execute_close(
            &service,
            credential_id,
            close_intent_with_symbol_key(cycle_id, symbol_key),
        )
        .await
        .expect("execute_intents itself must not fail");
        let orders = count(
            &store,
            "SELECT COUNT(*) AS n FROM engine_orders WHERE credential_id = ?",
            credential_id,
        )
        .await;
        let requests = client.recorded_requests();
        drop(service);
        cleanup(store, path).await;
        (ack, requests, orders)
    }

    /// **regression（High）· key 自己声明的 venue/market 必须被看见。**
    ///
    /// R11 的提交侧只比 `intent.exchange` / `intent.market_kind` 两个外层字段，
    /// 然后 `symbol_from_symbol_key` 把 `okx|spot|BTC|USDT|USDT` 拼成
    /// `BTCUSDT` 交给 Binance USDⓈ-M 客户端——一张**没被任何人批准过的
    /// 市场**上的真实下单。
    #[tokio::test]
    async fn a_symbol_key_declaring_another_venue_never_reaches_the_exchange() {
        let (ack, requests, orders) =
            submit_with_symbol_key("r12w-okx", "r12w_okx", "okx|spot|BTC|USDT|USDT").await;
        assert_eq!(ack.status, "rejected", "ack: {ack:?}");
        assert!(
            ack.message.contains("declares venue"),
            "the rejection must name the identity that disagreed: {ack:?}"
        );
        assert_eq!(orders, 0, "a rejected identity must not insert an order");
        assert!(
            requests.is_empty(),
            "nothing may be sent to the exchange: {requests:?}"
        );
    }

    /// **regression（High）· settle 段同样是身份。**
    ///
    /// `binance|futures|BTC|USDT|BTC` 是币本位合约：外层 venue/market 完全
    /// 合规，`base+quote` 拼出来的 symbol 也照样是 `BTCUSDT`，但它是**另一张
    /// 合约**。裸身份判据对它一句话都说不出来。
    #[tokio::test]
    async fn a_coin_margined_symbol_key_never_reaches_the_usdm_exchange() {
        let (ack, requests, orders) =
            submit_with_symbol_key("r12w-coinm", "r12w_coinm", "binance|futures|BTC|USDT|BTC")
                .await;
        assert_eq!(ack.status, "rejected", "ack: {ack:?}");
        assert!(
            ack.message.contains("USDⓈ-M"),
            "the rejection must name the closed world: {ack:?}"
        );
        assert_eq!(orders, 0, "a rejected identity must not insert an order");
        assert!(
            requests.is_empty(),
            "nothing may be sent to the exchange: {requests:?}"
        );
    }

    /// **放行面（反向断言）**：闭世界内的 key 走的是同一条骨架，
    /// 照常落单并真的发出交易所请求。没有它，上面两条用"一律拒绝"也能全绿。
    #[tokio::test]
    async fn the_usdm_symbol_key_still_submits_through_the_same_entry() {
        let (ack, requests, orders) =
            submit_with_symbol_key("r12w-usdm", "r12w_usdm", "binance|futures|BTC|USDT|USDT").await;
        assert_eq!(ack.status, "accepted", "ack: {ack:?}");
        assert_eq!(orders, 1);
        assert!(
            requests
                .iter()
                .any(|request| request.contains("symbol=BTCUSDT")),
            "the accepted identity must reach the exchange: {requests:?}"
        );
    }
}

/// **行情订阅闭世界（regression）与跨重启重建（regression）的
/// 生产入口用例。**
///
/// 全部走 `EngineExecution::ensure_market_data`（gRPC 方法本体）与
/// `EngineExecutionService::restore_market_data_on_startup`（`main` 的启动
/// 调用点），断言的是**库里的 required 集合**与**执行体是否真的存在**。
#[cfg(test)]
mod r9_market_data_wiring_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;
    use tonic::Request;

    use super::{EngineExecution, EngineExecutionService};
    use crate::proto::{EnsureMarketDataRequest, MarketDataSubscription};
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const BTC_KEY: &str = "binance|futures|BTC|USDT|USDT";
    const CREDENTIAL: &str = "cred_r9md";

    async fn test_store() -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r9md-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    fn subscription(
        exchange: &str,
        market: &str,
        symbol_key: &str,
        timeframe: &str,
    ) -> MarketDataSubscription {
        MarketDataSubscription {
            exchange: exchange.to_string(),
            market_kind: market.to_string(),
            symbol_key: symbol_key.to_string(),
            timeframe: timeframe.to_string(),
        }
    }

    async fn ensure(
        service: &EngineExecutionService,
        subscriptions: Vec<MarketDataSubscription>,
    ) -> crate::proto::EnsureMarketDataResponse {
        service
            .ensure_market_data(Request::new(EnsureMarketDataRequest {
                credential_id: CREDENTIAL.to_string(),
                subscriptions,
            }))
            .await
            .expect("ensure_market_data must answer")
            .into_inner()
    }

    async fn required_row_count(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM market_stream_subscriptions")
            .fetch_one(store.pool())
            .await
            .expect("count required streams")
            .get::<i64, _>("n")
    }

    /// **每一条拒绝都必须留下"零落库"这个事实。**
    /// 只断言 `accepted=false` 不够：R8 的实现也会在丢弃条目之后返回
    /// `accepted=true`，而真正的危害是 required 集合与执行体的**状态**。
    async fn assert_refused(store: &Store, response: crate::proto::EnsureMarketDataResponse) {
        assert!(!response.accepted, "message={}", response.message);
        assert_eq!(response.started, 0);
        assert_eq!(
            required_row_count(store).await,
            0,
            "a refused batch must not leave a single required row"
        );
    }

    /// 评审实跑的反例①：可解析但 Binance 不提供的 `7m`。
    #[tokio::test]
    async fn the_rpc_refuses_a_parseable_but_unsupported_interval() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let response = ensure(
            &service,
            vec![subscription("binance", "futures", BTC_KEY, "7m")],
        )
        .await;
        assert!(
            response.message.contains("unsupported timeframe"),
            "{}",
            response.message
        );
        assert_refused(&store, response).await;
        drop(service);
        cleanup(store, path).await;
    }

    /// 评审实跑的反例②：unsupported venue。
    #[tokio::test]
    async fn the_rpc_refuses_an_unsupported_venue() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let response = ensure(
            &service,
            vec![subscription("okx", "futures", BTC_KEY, "1m")],
        )
        .await;
        assert!(
            response.message.contains("unsupported exchange"),
            "{}",
            response.message
        );
        assert_refused(&store, response).await;
        drop(service);
        cleanup(store, path).await;
    }

    /// **regression（High）· 评审反例：`okx|spot|…` 形状的 symbol_key
    /// 通过了整批校验。**
    ///
    /// 走的是生产 RPC 本体。外层 venue/market 写的是合法的 binance/futures，
    /// 只有 symbol_key 内部声明了另一个市场——两处身份不同源时必须整批拒，
    /// 且一行 required 都不留。
    #[tokio::test]
    async fn the_rpc_refuses_a_symbol_key_that_declares_another_market() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let response = ensure(
            &service,
            vec![
                subscription("binance", "futures", BTC_KEY, "1m"),
                subscription("binance", "futures", "okx|spot|BTC|USDT|USDT", "1m"),
            ],
        )
        .await;
        assert!(
            response.message.contains("declares"),
            "{}",
            response.message
        );
        assert_refused(&store, response).await;
        drop(service);
        cleanup(store, path).await;
    }

    /// 空 symbol_key / 空 timeframe：此前是逐条 `continue` 静默丢弃。
    #[tokio::test]
    async fn the_rpc_refuses_empty_entries_instead_of_dropping_them() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());

        let empty_symbol = ensure(
            &service,
            vec![subscription("binance", "futures", "  ", "1m")],
        )
        .await;
        assert_refused(&store, empty_symbol).await;

        let empty_timeframe = ensure(
            &service,
            vec![subscription("binance", "futures", BTC_KEY, "")],
        )
        .await;
        assert_refused(&store, empty_timeframe).await;

        // 一条订阅都没有的请求同样不得回 `accepted=true`：编排器只看这个字段。
        let empty_batch = ensure(&service, Vec::new()).await;
        assert_refused(&store, empty_batch).await;

        drop(service);
        cleanup(store, path).await;
    }

    /// **混合批整批拒绝**：不接受"丢掉坏的留下好的"。
    #[tokio::test]
    async fn the_rpc_refuses_a_mixed_batch_as_a_whole() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let response = ensure(
            &service,
            vec![
                subscription("binance", "futures", BTC_KEY, "1m"),
                subscription("binance", "futures", BTC_KEY, "7m"),
            ],
        )
        .await;
        assert_refused(&store, response).await;
        drop(service);
        cleanup(store, path).await;
    }

    /// **放行面自证**：全合法的批必须被接受、登记并起流。
    /// 没有它，"一律拒绝"也能让上面四条全绿。
    #[tokio::test]
    async fn the_rpc_accepts_and_registers_a_fully_valid_batch() {
        let (store, path) = test_store().await;
        let service = EngineExecutionService::new(store.clone());
        let response = ensure(
            &service,
            vec![
                subscription("binance", "futures", BTC_KEY, "1m"),
                subscription("BINANCE", "Futures", BTC_KEY, "1H"),
            ],
        )
        .await;
        assert!(response.accepted, "{}", response.message);
        assert_eq!(response.started, 2);
        assert_eq!(required_row_count(&store).await, 2);
        drop(service);
        cleanup(store, path).await;
    }

    /// **regression · 进程重启后 required 订阅必须重新有执行体。**
    ///
    /// 第一个服务实例走生产 RPC 登记并起流；随后**用生产构造路径**
    /// （`EngineExecutionService::new`，它每次都造一个空的 `MarketDataManager`）
    /// 新建第二个实例——这就是重启后的执行态——再调 `main` 用的那个启动方法。
    ///
    /// **诚实边界**：这是**实例级**重启，不是真正的进程级重启。真进程级需要
    /// 起一个 rustcore 子进程并给它一条可连的 websocket 上游，本用例做不到；
    /// 但被测的正是"新实例的执行态从哪里来"这一件事，而这件事在实例级是
    /// 完整的（新 manager、空表、只靠库里的 required 集合恢复）。
    #[tokio::test]
    async fn a_restarted_service_rebuilds_workers_and_keeps_the_trigger_fail_closed() {
        let (store, path) = test_store().await;
        let first = EngineExecutionService::new(store.clone());
        let response = ensure(
            &first,
            vec![
                subscription("binance", "futures", BTC_KEY, "1m"),
                subscription("binance", "futures", BTC_KEY, "5m"),
            ],
        )
        .await;
        assert!(response.accepted);
        assert_eq!(response.started, 2);
        let before = crate::auto_triggers::required_streams(&store, CREDENTIAL)
            .await
            .expect("required set before restart");
        drop(first);

        // 「重启」：全新的服务实例，执行态是空的。
        let restarted = EngineExecutionService::new(store.clone());
        let restored = restarted
            .restore_market_data_on_startup()
            .await
            .expect("restore market data on startup");
        assert_eq!(
            restored,
            crate::market_data::RestoreOutcome {
                started: 2,
                rejected: 0,
                rejected_credential_ids: Default::default(),
            },
            "every persisted required stream must get a worker again"
        );
        // 已经活着的不重复起。
        assert_eq!(
            restarted
                .restore_market_data_on_startup()
                .await
                .expect("restore is idempotent"),
            crate::market_data::RestoreOutcome::default()
        );

        // `requested_at_ms` 不得被重启刷新（R7 裁定）。
        let after = crate::auto_triggers::required_streams(&store, CREDENTIAL)
            .await
            .expect("required set after restart");
        assert_eq!(before, after, "restart must not rewrite the required set");

        // **中断判据仍能 fail-closed**：这两条流被要求存在、却从未产出过 bar，
        // 等够了阈值就必须 arm——用的是生产判据函数本体。
        let requested_at_ms = after[0].requested_at_ms;
        let facts = crate::auto_triggers::MarketDataFacts {
            credential_id: CREDENTIAL.to_string(),
            kind: crate::venue::CredentialKind::ExchangeBacked,
            streams: Vec::new(),
            required: after,
            has_exposure: true,
        };
        assert!(
            crate::auto_triggers::market_data_interruption_decision(
                &facts,
                requested_at_ms + 3_600_000,
                180_000,
            )
            .is_some(),
            "a required stream that never produced a bar must still arm after a restart"
        );

        drop(restarted);
        cleanup(store, path).await;
    }

    /// **`rejected > 0` 必须改变服务状态。**
    ///
    /// 反例：R10 之后 restore 会返回 `rejected=1`，但 `main` 拿到之后照常
    /// 继续 serving——库里那条永远起不来的订阅对外完全不可见。
    ///
    /// 这条是**生产入口 wiring proof**（engine safety contract）：调的是 `main` 调的
    /// 那个符号（`restore_market_data_on_startup`），不跳任何一层；
    /// 断言的是**引擎自己写进 `kill_switch_state` 的结构化真值**，
    /// 不是返回值回显。反向面在下一条。
    #[tokio::test]
    async fn a_rejected_persisted_subscription_puts_the_credential_into_not_ready() {
        let (store, path) = test_store().await;
        sqlx::query(
            "INSERT INTO market_stream_subscriptions              (credential_id, symbol_key, timeframe, requested_at_ms) VALUES (?,?,?,?)",
        )
        .bind(CREDENTIAL)
        .bind(BTC_KEY)
        .bind("7m")
        .bind(1_000i64)
        .execute(store.pool())
        .await
        .expect("seed a historical subscription row");

        let service = EngineExecutionService::new(store.clone());
        let restored = service
            .restore_market_data_on_startup()
            .await
            .expect("startup must answer");
        assert_eq!(restored.rejected, 1);
        assert_eq!(restored.started, 0);

        let latched = sqlx::query(
            "SELECT level, trigger_source, reason FROM kill_switch_state WHERE scope_key = ?",
        )
        .bind(format!("cred:{CREDENTIAL}"))
        .fetch_optional(store.pool())
        .await
        .expect("read kill switch state")
        .expect("startup must not serve as healthy after refusing a persisted subscription");
        assert_eq!(latched.get::<String, _>("level"), "pause_new");
        assert_eq!(
            latched.get::<String, _>("trigger_source"),
            crate::auto_triggers::TRIGGER_SOURCE_MARKET_DATA
        );
        assert!(
            latched.get::<String, _>("reason").contains("7m"),
            "the latched reason must name the row that can never start"
        );

        drop(service);
        cleanup(store, path).await;
    }

    /// **放行面**（同一条判据的反向）：全部持久行都合法时，启动**不得**
    /// latch 任何 pause_new——否则上一条用"一律 arm"也能全绿。
    #[tokio::test]
    async fn a_clean_restore_does_not_latch_pause_new() {
        let (store, path) = test_store().await;
        let first = EngineExecutionService::new(store.clone());
        let response = ensure(
            &first,
            vec![subscription("binance", "futures", BTC_KEY, "1m")],
        )
        .await;
        assert!(response.accepted, "{}", response.message);
        drop(first);

        let restarted = EngineExecutionService::new(store.clone());
        let restored = restarted
            .restore_market_data_on_startup()
            .await
            .expect("startup must answer");
        assert_eq!(restored.rejected, 0);
        assert_eq!(restored.started, 1);
        assert!(
            sqlx::query("SELECT 1 FROM kill_switch_state WHERE scope_key = ?")
                .bind(format!("cred:{CREDENTIAL}"))
                .fetch_optional(store.pool())
                .await
                .expect("read kill switch state")
                .is_none(),
            "a healthy restore must not pause new risk"
        );

        drop(restarted);
        cleanup(store, path).await;
    }
}

/// **WS 普通订单更新的身份绑定与终态单调（regression）。**
///
/// 驱动的是 `BinanceEventSink::handle_ws_envelope`——user stream reader
/// 收到一帧文本之后调用的**同一个**符号（见 `binance_user_stream::run_once`
/// 的 `Message::Text` 分支），入参是真实的 wire JSON，经真实的 envelope
/// 反序列化。断言的是库里的订单状态。
#[cfg(test)]
mod r9_ws_identity_tests {
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::BinanceEventSink;
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const CREDENTIAL: &str = "cred_r9ws";
    const BTC_KEY: &str = "binance|futures|BTC|USDT|USDT";

    async fn test_store() -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r9ws-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn seed_order(
        store: &Store,
        engine_order_id: &str,
        status: &str,
        exchange_order_id: &str,
    ) {
        seed_order_full(
            store,
            engine_order_id,
            status,
            exchange_order_id,
            BTC_KEY,
            1,
        )
        .await;
    }

    /// 同 id 但可指定 `symbol_key` 与 `created_at_ms` 的种子行——
    /// 候选查询的 shadow 反例（同 id、异 symbol、更新的行）需要它。
    /// `client_order_id_short` 刻意共用 `coid-eng_r9ws`：Binance 的
    /// clientOrderId 由调用方自选，跨 symbol 撞名是**真实可能**的形态。
    async fn seed_order_full(
        store: &Store,
        engine_order_id: &str,
        status: &str,
        exchange_order_id: &str,
        symbol_key: &str,
        created_at_ms: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r9ws', ?, 'binance', 'futures', ?,
              'open_long', 'long', 'market', '1', 0, 'entry', ?, ?, 'coid-eng_r9ws', ?, ?)
            "#,
        )
        .bind(engine_order_id)
        .bind(CREDENTIAL)
        .bind(format!("idem-{engine_order_id}"))
        .bind(symbol_key)
        .bind(status)
        .bind(exchange_order_id)
        .bind(created_at_ms)
        .bind(created_at_ms)
        .execute(store.pool())
        .await
        .expect("seed engine order");
    }

    fn order_update(symbol: &str, order_id: i64, status: &str) -> String {
        serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": symbol,
                "c": "coid-eng_r9ws",
                "i": order_id,
                "x": "NEW",
                "X": status
            }
        })
        .to_string()
    }

    async fn status_of(store: &Store, engine_order_id: &str) -> String {
        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = ?")
            .bind(engine_order_id)
            .fetch_one(store.pool())
            .await
            .expect("read order status")
            .get::<String, _>("status")
    }

    async fn event_count(store: &Store, event_type: &str) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(store.pool())
            .await
            .expect("count events")
            .get::<i64, _>("n")
    }

    /// 评审实跑的反例：本地 BTC 单已 `filled`，注入同 id 但 symbol=ETH 的
    /// 迟到 `NEW`——R8 的实现把本地状态改成了 `acked`。
    #[tokio::test]
    async fn a_cross_symbol_order_update_must_not_be_written_back() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "filled", "9001").await;
        let envelope = serde_json::from_str(&order_update("ETHUSDT", 9001, "NEW")).unwrap();

        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(status_of(&store, "eng_r9ws").await, "filled");
        assert_eq!(
            event_count(&store, "order_update").await,
            0,
            "a frame that does not belong to this order must not even be audited as its update"
        );
        cleanup(store, path).await;
    }

    /// 终态单调：同 symbol 的迟到 `NEW` 也不得把 `filled` 退回 `acked`。
    #[tokio::test]
    async fn a_late_update_must_not_regress_a_terminal_status() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "filled", "9001").await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "NEW")).unwrap();

        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(
            status_of(&store, "eng_r9ws").await,
            "filled",
            "WS ordering is not guaranteed; a terminal truth must never be downgraded"
        );
        cleanup(store, path).await;
    }

    /// **放行面自证**：非终态的行照常被真值推进，缺失的交易所身份照常补齐。
    /// 没有它，"WS 一律不写回"也能让上面两条全绿。
    #[tokio::test]
    async fn a_matching_update_still_advances_a_non_terminal_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "pending_submit", "").await;
        let envelope = serde_json::from_str(&order_update("btcusdt", 9001, "NEW")).unwrap();

        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(status_of(&store, "eng_r9ws").await, "acked");
        let exchange_order_id = sqlx::query(
            "SELECT COALESCE(exchange_order_id,'') AS v FROM engine_orders WHERE engine_order_id='eng_r9ws'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("v");
        assert_eq!(exchange_order_id, "9001");
        assert_eq!(event_count(&store, "order_update").await, 1);
        cleanup(store, path).await;
    }

    /// **regression（High）· 同 id 的更新不得被更新的异 symbol 行 shadow。**
    ///
    /// 评审实跑红：候选查询只按 credential + id 取**最新一行**，
    /// 于是一条更新的 ETH 行把 BTC 那张单挡住——后验发现 symbol 不符就整条
    /// 丢弃，**真正该被更新的 BTC 单永远收不到这条更新**。
    /// 先选错再拒绝防得住写错，防不住漏写。
    #[tokio::test]
    async fn a_newer_row_of_another_symbol_must_not_shadow_the_right_order() {
        let (store, path) = test_store().await;
        // 该被更新的那张：BTC，较早落库。
        seed_order_full(&store, "eng_r9ws", "acked", "9001", BTC_KEY, 1).await;
        // 干扰行：同一个 exchange_order_id / clientOrderId，另一个 symbol，**更新**。
        seed_order_full(
            &store,
            "eng_r9ws_eth",
            "acked",
            "9001",
            "binance|futures|ETH|USDT|USDT",
            9_999,
        )
        .await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(
            status_of(&store, "eng_r9ws").await,
            "filled",
            "the BTC order must receive its own update even though a newer ETH row shares the id"
        );
        assert_eq!(
            status_of(&store, "eng_r9ws_eth").await,
            "acked",
            "the ETH row must not be touched by a BTC update"
        );
        cleanup(store, path).await;
    }

    /// **regression（High）· 进行态也不得被回退。**
    ///
    /// 评审实跑红：R9 只保护终态，一条迟到的 `NEW` 把 `partial_filled`
    /// 退回 `acked`——已经发生的成交在状态上消失，而 `acked` 又在 reconcile
    /// 的 inflight 重查集里。
    #[tokio::test]
    async fn a_late_new_must_not_regress_a_partially_filled_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "partial_filled", "9001").await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "NEW")).unwrap();

        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(
            status_of(&store, "eng_r9ws").await,
            "partial_filled",
            "a partially filled order must not be regressed to acked by a late NEW"
        );
        cleanup(store, path).await;
    }

    /// 偏序的**放行面**：更强的真值仍必须写进去（`partial_filled` → `filled`，
    /// `unknown` → `acked`）。没有这条，"一律不写"也能让上面两条全绿。
    #[tokio::test]
    async fn a_stronger_truth_still_advances_the_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "partial_filled", "9001").await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");
        assert_eq!(status_of(&store, "eng_r9ws").await, "filled");
        cleanup(store, path).await;

        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws_unknown", "unknown", "9002").await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9002, "NEW")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");
        assert_eq!(
            status_of(&store, "eng_r9ws_unknown").await,
            "acked",
            "`unknown` carries no exchange truth; any exchange truth must be able to replace it"
        );
        cleanup(store, path).await;
    }

    /// 偏序表本身的闭世界自证：每个状态都必须有名次，
    /// 且"更弱集合"必须严格更弱（同级不得进集合）。
    #[test]
    fn the_status_partial_order_is_total_over_the_closed_world() {
        for status in super::ALL_ENGINE_ORDER_STATUSES {
            assert!(
                super::order_status_progress_rank(status).is_some(),
                "{status} must have a rank"
            );
        }
        assert!(super::order_status_progress_rank("no_such_status").is_none());
        assert!(super::statuses_weaker_than("no_such_status").is_empty());
        assert!(!super::statuses_weaker_than("acked").contains(&"partial_filled"));
        assert!(super::statuses_weaker_than("acked").contains(&"unknown"));
        assert!(super::statuses_weaker_than("partial_filled").contains(&"acked"));
    }

    /// **regression（High）· 终态之间的冲突裁决。**
    ///
    /// R10 把所有终态压成同一 rank，这条断言当时写的是
    /// `!statuses_weaker_than("filled").contains(&"canceled")`——
    /// 它恰好把缺陷语义钉死了（评审变异 R7-05 的提示）。
    /// 现在按权威度重定义，**正反两向都要有夹具**。
    #[test]
    fn terminal_conflicts_are_adjudicated_by_exchange_authority() {
        // 正向：后来的权威 FILLED 必须能改写本地判死的 failed。
        assert!(
            super::statuses_weaker_than("filled").contains(&"failed"),
            "a local `failed` must never block a later authoritative FILLED"
        );
        // 正向：交易所的无成交终态同样强于本地 failed。
        for authoritative in ["canceled", "expired", "rejected"] {
            assert!(
                super::statuses_weaker_than(authoritative).contains(&"failed"),
                "{authoritative} is exchange truth; `failed` is a local guess"
            );
        }
        // 正向：FILLED 强于任何无成交终态。
        for weaker in ["canceled", "expired", "rejected"] {
            assert!(
                super::statuses_weaker_than("filled").contains(&weaker),
                "an executed fill is the strongest truth an order can carry"
            );
        }
        // 反向：三条无成交终态互相同级，谁都不得改写谁。
        for (next, current) in [
            ("canceled", "rejected"),
            ("rejected", "canceled"),
            ("expired", "canceled"),
            ("canceled", "expired"),
            ("rejected", "expired"),
            ("expired", "rejected"),
        ] {
            assert!(
                !super::statuses_weaker_than(next).contains(&current),
                "{next} must not overwrite the mutually exclusive terminal {current}"
            );
        }
        // 反向：没有任何终态弱于 filled 之外的东西能改写 filled。
        for next in super::ALL_ENGINE_ORDER_STATUSES {
            assert!(
                !super::statuses_weaker_than(next).contains(&"filled"),
                "{next} must not overwrite a filled order"
            );
        }
        // 反向：进行态一律不得改写任何终态。
        for next in ["acked", "partial_filled", "submitted", "unknown"] {
            for terminal in super::TERMINAL_ORDER_STATUSES {
                assert!(
                    !super::statuses_weaker_than(next).contains(&terminal),
                    "{next} must not overwrite the terminal {terminal}"
                );
            }
        }
    }

    /// **regression（High）· 归属由完整交易身份决定，与排序无关。**
    ///
    /// 评审反例：同 credential/symbol 下旧行 A 只匹配 `exchange_order_id`、
    /// 新行 B 只匹配 `client_order_id`，一条同时带两者的 frame 经 `OR`
    /// 命中两行——`ORDER BY created_at_ms DESC` 选 B、`ASC` 选 A。
    #[test]
    fn ws_identity_adjudication_is_order_free() {
        use super::{adjudicate_order_identity, OrderIdentityCandidate};
        let only_exchange = OrderIdentityCandidate {
            matches_exchange_order_id: true,
            matches_client_order_id: false,
        };
        let only_client = OrderIdentityCandidate {
            matches_exchange_order_id: false,
            matches_client_order_id: true,
        };
        let both = OrderIdentityCandidate {
            matches_exchange_order_id: true,
            matches_client_order_id: true,
        };
        let neither = OrderIdentityCandidate {
            matches_exchange_order_id: false,
            matches_client_order_id: false,
        };

        // 分裂命中：无论候选顺序如何，归属都落在 exchange_order_id 那一行，
        // 且 `split` 必须置起（本地存在 id 复用，要告警）。
        let forward = adjudicate_order_identity(&[only_exchange, only_client]);
        assert_eq!(forward.chosen, Some(0));
        assert!(forward.split);
        assert!(!forward.ambiguous);
        let reversed = adjudicate_order_identity(&[only_client, only_exchange]);
        assert_eq!(
            reversed.chosen,
            Some(1),
            "reversing the candidate order must not move the ownership"
        );
        assert!(reversed.split);

        // 唯一命中（无分裂）不得误报 split。
        assert_eq!(
            adjudicate_order_identity(&[both]),
            super::OrderIdentityAdjudication {
                chosen: Some(0),
                ambiguous: false,
                split: false,
            }
        );
        assert_eq!(
            adjudicate_order_identity(&[only_client, neither]),
            super::OrderIdentityAdjudication {
                chosen: Some(0),
                ambiguous: false,
                split: false,
            }
        );

        // 权威身份命中多行 = 本地 id 复用：不猜，整条丢弃。
        let ambiguous = adjudicate_order_identity(&[only_exchange, only_exchange]);
        assert_eq!(ambiguous.chosen, None);
        assert!(ambiguous.ambiguous);
        // 回落身份命中多行同样不猜。
        let ambiguous_client = adjudicate_order_identity(&[only_client, only_client]);
        assert_eq!(ambiguous_client.chosen, None);
        assert!(ambiguous_client.ambiguous);

        // 无命中。
        assert_eq!(
            adjudicate_order_identity(&[neither, neither]),
            super::OrderIdentityAdjudication::default()
        );
        assert_eq!(
            adjudicate_order_identity(&[]),
            super::OrderIdentityAdjudication::default()
        );
    }

    /// **regression（High）· 与成交矛盾的终态清单。**
    #[test]
    fn only_non_execution_terminals_deny_a_fill() {
        for denying in ["failed", "rejected", "FAILED", " rejected "] {
            assert!(super::terminal_denies_execution(denying));
        }
        for compatible in ["canceled", "expired", "filled", "partial_filled", "acked"] {
            assert!(
                !super::terminal_denies_execution(compatible),
                "{compatible} can legitimately coexist with a booked fill"
            );
        }
    }

    /// **regression（High）· 状态写失败必须让整条更新回滚。**
    ///
    /// 评审实跑红：状态 UPDATE 的触发器失败被 `let _` 吞掉之后，
    /// `order_update` 审计事件照写、事务照 commit——库里于是留下"这张单被
    /// 更新过"的审计事实，而状态根本没动。
    ///
    /// 故障是真的：一个只在 `status='filled'` 时 ABORT 的 SQLite 触发器。
    #[tokio::test]
    async fn a_failed_status_write_rolls_back_the_whole_update() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        sqlx::query(
            "CREATE TRIGGER r10_block_ws_status BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'filled' \
             BEGIN SELECT RAISE(ABORT, 'injected ws status write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        // ****：写失败不再被吞在函数里——它上抛到
        // reader，由 reader 降 readiness 并重连。
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a write failure must reach the reader, not be swallowed");
        assert!(
            error.to_string().contains("the order status"),
            "the propagated error must name the step that failed: {error}"
        );

        assert_eq!(status_of(&store, "eng_r9ws").await, "acked");
        assert_eq!(
            event_count(&store, "order_update").await,
            0,
            "an update whose status write failed must not leave an audit trail saying it applied"
        );
        cleanup(store, path).await;
    }

    /// 同一条纪律作用在 **fill / position / audit** 侧：
    /// 成交写失败时，状态更新与 `order_update` 事件都不得留下。
    #[tokio::test]
    async fn a_failed_fill_write_rolls_back_the_status_and_the_audit() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        sqlx::query(
            "CREATE TRIGGER r10_block_ws_fill BEFORE INSERT ON engine_fills \
             FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'injected ws fill write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT",
                "c": "coid-eng_r9ws",
                "i": 9001,
                "x": "TRADE",
                "X": "FILLED",
                "t": 55_001,
                "l": "1",
                "L": "100",
                "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a write failure must reach the reader, not be swallowed");
        assert!(
            error.to_string().contains("the fill row"),
            "the propagated error must name the step that failed: {error}"
        );

        assert_eq!(
            status_of(&store, "eng_r9ws").await,
            "acked",
            "a fill that could not be persisted must not leave the order marked filled"
        );
        assert_eq!(event_count(&store, "order_update").await, 0);
        assert_eq!(event_count(&store, "fill").await, 0);
        cleanup(store, path).await;
    }

    /// 放行面：同一条成交帧在库健康时必须**全部**落地
    /// （状态 + order_update + fill 事件 + fill 行）。
    #[tokio::test]
    async fn a_healthy_fill_frame_persists_every_part_of_the_update() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT",
                "c": "coid-eng_r9ws",
                "i": 9001,
                "x": "TRADE",
                "X": "FILLED",
                "t": 55_001,
                "l": "1",
                "L": "100",
                "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        assert_eq!(status_of(&store, "eng_r9ws").await, "filled");
        assert_eq!(event_count(&store, "order_update").await, 1);
        assert_eq!(event_count(&store, "fill").await, 1);
        let fills = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills")
            .fetch_one(store.pool())
            .await
            .expect("count fills")
            .get::<i64, _>("n");
        assert_eq!(fills, 1);
        cleanup(store, path).await;
    }

    /// 同上，但可指定 `client_order_id_short`：分裂命中反例需要两行带**不同**
    /// 的 client id，才能让两种 identity 分别落在不同的行上。
    async fn seed_order_with_client_id(
        store: &Store,
        engine_order_id: &str,
        status: &str,
        exchange_order_id: &str,
        client_order_id_short: &str,
        created_at_ms: i64,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r9ws', ?, 'binance', 'futures', ?,
              'open_long', 'long', 'market', '1', 0, 'entry', ?, ?, ?, ?, ?)
            "#,
        )
        .bind(engine_order_id)
        .bind(CREDENTIAL)
        .bind(format!("idem-{engine_order_id}"))
        .bind(BTC_KEY)
        .bind(status)
        .bind(exchange_order_id)
        .bind(client_order_id_short)
        .bind(created_at_ms)
        .bind(created_at_ms)
        .execute(store.pool())
        .await
        .expect("seed engine order");
    }

    /// **分裂命中的归属不得由排序决定。**
    ///
    /// 评审构造的反例（统筹的「等价」判断被它推翻）：同 credential/symbol 下
    /// 旧行 A 只匹配 `exchange_order_id`、新行 B 只匹配 `client_order_id`，
    /// 一条同时带两者的 frame 经 `OR` 命中两行——`ORDER BY created_at_ms DESC`
    /// 选 B、`ASC` 选 A。**改一下排序就改变了真实归属。**
    ///
    /// 现在归属只由权威身份（交易所分配的 `exchange_order_id`）决定，
    /// 与 `created_at_ms` 无关：B 更新，但归属仍在 A。
    #[tokio::test]
    async fn a_split_identity_match_binds_to_the_exchange_assigned_identity() {
        let (store, path) = test_store().await;
        // A：**旧**行，只按 exchange_order_id 命中。
        seed_order_with_client_id(&store, "eng_by_exchange", "acked", "9001", "coid-A", 1).await;
        // B：**新**行，只按 client_order_id 命中。R10 的 `DESC LIMIT 1` 会选它。
        seed_order_with_client_id(&store, "eng_by_client", "acked", "9002", "coid-B", 999).await;

        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": { "s": "BTCUSDT", "c": "coid-B", "i": 9001, "x": "NEW", "X": "FILLED" }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied");

        assert_eq!(
            status_of(&store, "eng_by_exchange").await,
            "filled",
            "the exchange-assigned order id is the authoritative identity"
        );
        assert_eq!(
            status_of(&store, "eng_by_client").await,
            "acked",
            "the newer row matched only by a caller-chosen client id must not be touched"
        );
        cleanup(store, path).await;
    }

    /// **交易身份必须完整到 settle。**
    ///
    /// 本地这张单是**币本位**合约（`…|BTC|USDT|BTC`）：它的交易所 symbol
    /// 同样是 `BTCUSDT`，但它是另一张合约。只比 symbol（R10 的
    /// `symbol_from_symbol_key`）会把 USDⓈ-M 用户流的真值写到它头上。
    #[tokio::test]
    async fn a_coin_margined_order_must_not_receive_usdm_truth() {
        let (store, path) = test_store().await;
        seed_order_full(
            &store,
            "eng_coinm",
            "acked",
            "9001",
            "binance|futures|BTC|USDT|BTC",
            1,
        )
        .await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("a frame that belongs to nobody is dropped, not a write failure");

        assert_eq!(
            status_of(&store, "eng_coinm").await,
            "acked",
            "a USDS-M frame must not settle onto a coin-margined contract"
        );
        assert_eq!(event_count(&store, "order_update").await, 0);
        cleanup(store, path).await;
    }

    /// **同一条身份判据的另一半：候选**筛选（不是后验）必须完整到 settle。
    ///
    /// 上一条被**后验**那一层挡住了，因此把选择判据放宽也不会红
    /// （变异 M17 实测 488/488 存活——两处冗余守卫互相掩护）。
    /// 这一条让选择判据**自己**决定结局：一张正确的 USDⓈ-M 单与一张币本位
    /// 单带着同一个 `exchange_order_id`。判据完整时只有前者是候选、被推进；
    /// 判据折掉 settle 时两张都成候选、同一权威身份多命中 → 整条丢弃，
    /// 那张真正该被更新的单**永远收不到这条真值**。
    #[tokio::test]
    async fn the_candidate_filter_itself_must_carry_the_full_identity() {
        let (store, path) = test_store().await;
        seed_order_full(&store, "eng_usdm", "acked", "9001", BTC_KEY, 1).await;
        seed_order_full(
            &store,
            "eng_coinm",
            "acked",
            "9001",
            "binance|futures|BTC|USDT|BTC",
            999,
        )
        .await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied");

        assert_eq!(
            status_of(&store, "eng_usdm").await,
            "filled",
            "the coin-margined row must never have been a candidate at all"
        );
        assert_eq!(status_of(&store, "eng_coinm").await, "acked");
        cleanup(store, path).await;
    }

    /// 外层 `exchange` / `market_kind` 可指定的种子行——WS 双源判据的反例
    /// 需要一条"外层写着别的市场、内层写着 USDⓈ-M"的矛盾行。
    #[allow(clippy::too_many_arguments)]
    async fn seed_order_with_outer_identity(
        store: &Store,
        engine_order_id: &str,
        status: &str,
        exchange_order_id: &str,
        client_order_id_short: &str,
        exchange: &str,
        market_kind: &str,
        symbol_key: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r15ws', ?, ?, ?, ?,
              'open_long', 'long', 'market', '1', 0, 'entry', ?, ?, ?, 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(CREDENTIAL)
        .bind(format!("idem-{engine_order_id}"))
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .bind(status)
        .bind(exchange_order_id)
        .bind(client_order_id_short)
        .execute(store.pool())
        .await
        .expect("seed engine order with an explicit outer identity");
    }

    /// **WS 侧的两个身份来源必须逐段一致。**
    ///
    /// `engine_orders` 的同一行同时存着**外层** `exchange` / `market_kind`
    /// 两列与**内层** `symbol_key`。R14 把这条双源判据接进了 REST 回补认领与
    /// 恢复域发现，唯独漏了 WS：一条 `exchange='okx'` / `market_kind='spot'`
    /// 而 `symbol_key='binance|futures|BTC|USDT|USDT'` 的矛盾行，照样会被这条
    /// Binance USDⓈ-M 的 `ORDER_TRADE_UPDATE` 认领，于是别的市场的一张单被
    /// 写上 Binance 的成交真值。
    ///
    /// 处置与 REST 侧逐字同一条：**整帧 fail-closed**（`Err` → reader 落
    /// `record_recovery_failure` / `ready=0`），而不是 `Ok(())` ——后者会把
    /// 一条交易所真值永久丢掉（engine safety contract）。
    #[tokio::test]
    async fn a_ws_candidate_with_two_disagreeing_identities_fails_the_frame() {
        let (store, path) = test_store().await;
        seed_order_with_outer_identity(
            &store,
            "eng_r15_outer_okx",
            "acked",
            "9001",
            "coid-eng_r9ws",
            "okx",
            "spot",
            BTC_KEY,
        )
        .await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a candidate whose two identities disagree must fail the frame closed");
        assert!(
            error.to_string().contains("identity is inconsistent"),
            "the error must name the identity contradiction: {error}"
        );

        assert_eq!(
            status_of(&store, "eng_r15_outer_okx").await,
            "acked",
            "an order that cannot prove its own market must not receive binance USDⓈ-M truth"
        );
        assert_eq!(
            event_count(&store, "order_update").await,
            0,
            "nothing from a contradictory frame may be audited as this order's update"
        );
        cleanup(store, path).await;
    }

    /// **反向**：外层与内层一致时，同一条帧照常被认领并推进。
    /// 没有这一条，用「WS 永远 fail-closed」也能让上一条全绿。
    #[tokio::test]
    async fn a_ws_candidate_with_two_agreeing_identities_is_still_claimed() {
        let (store, path) = test_store().await;
        seed_order_with_outer_identity(
            &store,
            "eng_r15_outer_ok",
            "acked",
            "9001",
            "coid-eng_r9ws",
            "binance",
            "futures",
            BTC_KEY,
        )
        .await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("an agreeing candidate must still be claimed");

        assert_eq!(status_of(&store, "eng_r15_outer_ok").await, "filled");
        assert_eq!(event_count(&store, "order_update").await, 1);
        cleanup(store, path).await;
    }

    /// **矛盾行不是"另一个市场的单"可以静默跳过。**
    ///
    /// 反例的形状：同一个 `exchange_order_id` 下，一张身份自洽的 USDⓈ-M 单
    /// **和**一张矛盾行同时存在。判据若只是"把矛盾行从候选里 retain 掉"，
    /// 那张自洽的单会被照常推进，看起来一切正常——可本地在这条 id 上已经
    /// 无法自证归属（两个来源里必有一个是错的）。整帧 fail-closed 之后，
    /// 自洽的那张单也**不得**被推进。
    #[tokio::test]
    async fn one_contradictory_candidate_fails_the_whole_frame() {
        let (store, path) = test_store().await;
        seed_order_full(&store, "eng_r15_clean", "acked", "9001", BTC_KEY, 1).await;
        seed_order_with_outer_identity(
            &store,
            "eng_r15_dirty",
            "acked",
            "9001",
            "coid-r15-dirty",
            "okx",
            "spot",
            BTC_KEY,
        )
        .await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("one contradictory candidate poisons the whole order identity");

        assert_eq!(status_of(&store, "eng_r15_clean").await, "acked");
        assert_eq!(status_of(&store, "eng_r15_dirty").await, "acked");
        assert_eq!(event_count(&store, "order_update").await, 0);
        cleanup(store, path).await;
    }

    /// **regression（High）· 候选查询失败 ≠ 这条帧不属于本地任何一张单。**
    ///
    /// R10 这里是 `return;`：一次 DB 故障与"无归属"产出同一个结果，
    /// 而后者会让这条交易所真值被永久丢弃、reader 还以为一切正常。
    #[tokio::test]
    async fn a_failed_candidate_lookup_is_propagated_not_treated_as_no_match() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        // 真故障：候选查询 LEFT JOIN 的那张表没了。
        sqlx::query("DROP TABLE risk_verdicts")
            .execute(store.pool())
            .await
            .expect("drop risk_verdicts to fail the candidate lookup");

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a failed candidate lookup must reach the reader");
        assert!(error.to_string().contains("candidate lookup failed"));

        assert_eq!(status_of(&store, "eng_r9ws").await, "acked");
        cleanup(store, path).await;
    }

    /// **反向**：权威身份无命中时才回落到 client id——否则上一条用
    /// 「永远只按 exchange_order_id 找」也能全绿。
    #[tokio::test]
    async fn a_client_id_only_match_is_still_applied() {
        let (store, path) = test_store().await;
        seed_order_with_client_id(&store, "eng_by_client", "acked", "", "coid-B", 1).await;

        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": { "s": "BTCUSDT", "c": "coid-B", "i": 9001, "x": "NEW", "X": "FILLED" }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied");

        assert_eq!(status_of(&store, "eng_by_client").await, "filled");
        cleanup(store, path).await;
    }

    /// **同一权威身份命中多行 = 本地 id 复用：不猜。**
    #[tokio::test]
    async fn an_ambiguous_authoritative_match_writes_nothing() {
        let (store, path) = test_store().await;
        seed_order_with_client_id(&store, "eng_dup_a", "acked", "9001", "coid-A", 1).await;
        seed_order_with_client_id(&store, "eng_dup_b", "acked", "9001", "coid-B", 999).await;

        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "FILLED")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("an ambiguous frame is dropped, not a write failure");

        assert_eq!(status_of(&store, "eng_dup_a").await, "acked");
        assert_eq!(status_of(&store, "eng_dup_b").await, "acked");
        assert_eq!(
            event_count(&store, "order_update").await,
            0,
            "guessing an owner is worse than dropping the frame; reconcile restores it"
        );
        cleanup(store, path).await;
    }

    /// **后来的权威 FILLED 必须赢过本地
    /// 判死的 `failed`。**
    ///
    /// 评审反例：所有终态同 rank，于是 reconcile 宽限期裁出来的 `failed`
    /// 挡住了后来真正的 `FILLED`——**而同一帧的 fill 行与敞口照样落库**，
    /// 库里留下「failed 的单 + 一笔成交 + 一份敞口」。
    #[tokio::test]
    async fn a_later_authoritative_fill_overrides_a_locally_failed_order() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "failed", "9001").await;
        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT", "c": "coid-eng_r9ws", "i": 9001,
                "x": "TRADE", "X": "FILLED", "t": 77_001,
                "l": "1", "L": "100", "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied");

        assert_eq!(
            status_of(&store, "eng_r9ws").await,
            "filled",
            "a locally adjudicated `failed` must never outrank real exchange execution"
        );
        assert_eq!(event_count(&store, "fill").await, 1);
        let booked_fills = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills")
            .fetch_one(store.pool())
            .await
            .expect("count fills")
            .get::<i64, _>("n");
        assert_eq!(booked_fills, 1);
        // 缺陷的形状是「`failed` 的单 + 落账的成交」同时存在。断言它**不可能**
        // 再出现：库里没有任何一行既非终态权威、又背着一笔成交。
        let contradictions = sqlx::query(
            r#"
            SELECT COUNT(*) AS n
            FROM engine_orders eo
            JOIN engine_fills ef ON ef.engine_order_id = eo.engine_order_id
            WHERE eo.status IN ('failed','rejected')
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("count contradictions")
        .get::<i64, _>("n");
        assert_eq!(
            contradictions, 0,
            "no order may carry a booked fill under a status that denies execution"
        );
        cleanup(store, path).await;
    }

    /// **反向**：状态推不动、而帧又带着一笔成交时，宁可整帧回滚也不落矛盾账。
    ///
    /// 这里用一个本引擎**认不出**的交易所状态（`desired_status == None`），
    /// 于是状态一步都推不动，而 `x=TRADE` 仍带 `tradeId`。
    #[tokio::test]
    async fn a_fill_under_a_status_that_denies_execution_is_rolled_back() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "failed", "9001").await;
        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT", "c": "coid-eng_r9ws", "i": 9001,
                "x": "TRADE", "X": "PENDING_NEW", "t": 77_002,
                "l": "1", "L": "100", "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a fill that contradicts the persisted status must not be committed");
        assert!(error
            .to_string()
            .contains("conflicts with persisted status"));

        assert_eq!(status_of(&store, "eng_r9ws").await, "failed");
        assert_eq!(event_count(&store, "fill").await, 0);
        assert_eq!(event_count(&store, "order_update").await, 0);
        let fills = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills")
            .fetch_one(store.pool())
            .await
            .expect("count fills")
            .get::<i64, _>("n");
        assert_eq!(
            fills, 0,
            "no fill may be booked under a contradictory status"
        );
        cleanup(store, path).await;
    }

    /// **放行面**：`canceled` 与一笔成交是自洽的（部分成交后撤单），
    /// 不得被上一条守卫误伤。
    #[tokio::test]
    async fn a_fill_under_a_canceled_order_is_still_booked() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "canceled", "9001").await;
        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT", "c": "coid-eng_r9ws", "i": 9001,
                "x": "TRADE", "X": "PENDING_NEW", "t": 77_003,
                "l": "1", "L": "100", "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("a partial fill that preceded a cancel is real and must be booked");

        assert_eq!(status_of(&store, "eng_r9ws").await, "canceled");
        assert_eq!(event_count(&store, "fill").await, 1);
        cleanup(store, path).await;
    }

    /// **entry fill 之后保护没挂上
    /// 必须同步 reduce-only flatten。**
    /// **完成判据是交易所真值。**
    ///
    /// `docs/SAFETY_MODEL.md` §2「保护提交失败……走既有 reduce-only flatten
    /// 并自动锁存」、engine safety contract「SL 未经交易所真值确认则立即 reduce-only
    /// flatten」。R10 这里两处都只 `error!`——一个已经成交的入场敞口就此裸着，
    /// 没有任何机制会去平它。
    ///
    /// R13 把断言从「arm 了 `pause_new` 且这一帧成功」换成
    /// 「**未收敛**的 `flatten` latch + 这一帧 `Err`」：flatten 请求被受理
    /// 不等于仓位归零，`pause_new` 也不驱动任何重试。
    ///
    /// 两个方向各一条：查询失败（`.ok()` 折成 `None` 的那一半）与提交失败。
    #[tokio::test]
    async fn an_entry_fill_whose_protection_lookup_fails_latches_flatten_and_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        // 真故障：把保护计划表整张删掉，attach 查询必然报错。
        // 这一半正是 R10 用 `.ok()` 折成"没有仓在等保护"的那一半。
        sqlx::query("DROP TABLE position_protection_plans")
            .execute(store.pool())
            .await
            .expect("drop the protection plan table");

        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let frame = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT", "c": "coid-eng_r9ws", "i": 9001,
                "x": "TRADE", "X": "FILLED", "t": 66_001,
                "l": "1", "L": "100", "z": "1"
            }
        })
        .to_string();
        let envelope = serde_json::from_str(&frame).unwrap();
        let error = sink
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an unprotected exposure must not be reported as a healthy frame");
        assert!(
            error.to_string().contains("disposition did not complete"),
            "the error must name what failed: {error}"
        );

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an entry fill that cannot be proven protected must be flattened reduce-only"
        );
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string()),
            "an unprotected entry fill must latch a retryable, not-yet-converged flatten"
        );
        assert!(
            !stream_is_ready(&store).await,
            "an unprotected exposure must not keep the stream admitting new risk"
        );
        cleanup(store, path).await;
    }

    /// 同一条纪律的另一半：保护**提交**失败。
    #[tokio::test]
    async fn an_entry_fill_whose_protection_submit_fails_latches_flatten_and_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_awaiting_protection(&store).await;
        // 真故障：保护子单落库时 ABORT。
        sqlx::query(
            "CREATE TRIGGER r11_block_protection_order BEFORE INSERT ON engine_orders \
             FOR EACH ROW WHEN NEW.order_role LIKE 'protection%' \
             BEGIN SELECT RAISE(ABORT, 'injected protection submit failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        sink.handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an unprotected exposure must not be reported as a healthy frame");

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string())
        );
        assert!(!stream_is_ready(&store).await);
        cleanup(store, path).await;
    }

    /// **放行面**：保护顺利挂上时**不得** flatten、**不得** pause。
    /// 没有它，"一律 flatten" 也能让上面两条全绿。
    ///
    /// 这里用的是**已被交易所真值确认过的 SL**：`submit_planned_protection`
    /// 走 `confirmed_sl` 那条早返回，把仓位标成 `protected` 并返回空 ack——
    /// 一次不联网、但确实成功的保护附着。
    #[tokio::test]
    async fn a_successful_protection_attachment_neither_flattens_nor_pauses() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_awaiting_protection(&store).await;
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              trigger_price, status, verified_from_exchange, created_at_ms, updated_at_ms
            ) VALUES ('prot_r9ws', 'pos_r9ws', ?, 'sl', 1, '90', 'confirmed', 1, 1, 1)
            "#,
        )
        .bind(CREDENTIAL)
        .execute(store.pool())
        .await
        .expect("seed a confirmed SL");

        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        sink.handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("a healthy entry fill must apply");

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a protected entry must not be flattened"
        );
        assert!(
            sqlx::query("SELECT 1 FROM kill_switch_state WHERE scope_key='bot:bot_r9ws'")
                .fetch_optional(store.pool())
                .await
                .expect("read kill switch state")
                .is_none(),
            "a protected entry must not pause new risk"
        );
        assert_eq!(
            sqlx::query("SELECT state FROM position_state WHERE position_id='pos_r9ws'")
                .fetch_one(store.pool())
                .await
                .expect("read position state")
                .get::<String, _>("state"),
            "protected",
            "the release side must be a real successful attachment, not a no-op"
        );
        cleanup(store, path).await;
    }

    /// `bot:bot_r9ws` 的 latch：`(level, flatten_state)`。
    ///
    /// 只断言 `level` 是不够的——一条已经被写成
    /// `flattened`（= 交易所真值已证明归零）的 latch 同样是 `flatten`，
    /// 但它不再属于重试集合。裸敞口刚被发现时必须是**未收敛**的。
    async fn bot_latch(store: &Store) -> (String, String) {
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key='bot:bot_r9ws'",
        )
        .fetch_optional(store.pool())
        .await
        .expect("read kill switch state")
        .expect("an unprotected entry fill must leave a durable latch");
        (
            row.get::<String, _>("level"),
            row.get::<String, _>("flatten_state"),
        )
    }

    /// 用户流当前是否 ready（读的是 `fill_replay` 的生产投影）。
    async fn stream_is_ready(store: &Store) -> bool {
        crate::fill_replay::load_fill_stream_state(store, CREDENTIAL)
            .await
            .expect("load stream state")
            .is_some_and(|state| state.ready)
    }

    #[derive(Default)]
    struct CountingFlattenAdapter {
        flatten_calls: std::sync::atomic::AtomicUsize,
    }

    #[tonic::async_trait]
    impl crate::protection::ProtectionAdapter for CountingFlattenAdapter {
        async fn query_algo_order(
            &self,
            _exchange_algo_id: Option<&str>,
            _client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<crate::protection::AlgoOrderTruth>> {
            Ok(None)
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flatten_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn entry_fill_frame() -> String {
        serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": {
                "s": "BTCUSDT", "c": "coid-eng_r9ws", "i": 9001,
                "x": "TRADE", "X": "FILLED", "t": 66_002,
                "l": "1", "L": "100", "z": "1"
            }
        })
        .to_string()
    }

    /// 一个 open 仓 + 一份 typed 保护计划——`attach_position` 的查询要靠它命中。
    async fn seed_position_awaiting_protection(store: &Store) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES ('pos_r9ws', ?, 'bot_r9ws', ?, 'long', '1', '100', 'open', 1, 1, 1)
            "#,
        )
        .bind(CREDENTIAL)
        .bind(BTC_KEY)
        .execute(store.pool())
        .await
        .expect("seed position");
        sqlx::query(
            r#"
            INSERT INTO position_protection_plans (
              position_id, credential_id, bot_uid, exchange, market_kind, symbol_key,
              direction, stop_loss_price, take_profit_price, confirm_timeout_ms,
              generation, created_at_ms, updated_at_ms
            ) VALUES ('pos_r9ws', ?, 'bot_r9ws', 'binance', 'futures', ?,
              'long', '90', '', 5000, 1, 1, 1)
            "#,
        )
        .bind(CREDENTIAL)
        .bind(BTC_KEY)
        .execute(store.pool())
        .await
        .expect("seed protection plan");
    }

    // ==================================================================
    // 保护 fail-closed 全链
    // ==================================================================

    /// 一个 open 仓，**但没有任何 typed 保护计划**。
    async fn seed_position_without_plan(store: &Store) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES ('pos_r12noplan', ?, 'bot_r9ws', ?, 'long', '1', '100', 'open', 1, 1, 1)
            "#,
        )
        .bind(CREDENTIAL)
        .bind(BTC_KEY)
        .execute(store.pool())
        .await
        .expect("seed position without a protection plan");
    }

    #[derive(Default)]
    struct FailingFlattenAdapter {
        flatten_calls: std::sync::atomic::AtomicUsize,
    }

    #[tonic::async_trait]
    impl crate::protection::ProtectionAdapter for FailingFlattenAdapter {
        async fn query_algo_order(
            &self,
            _exchange_algo_id: Option<&str>,
            _client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<crate::protection::AlgoOrderTruth>> {
            Ok(None)
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flatten_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(anyhow::anyhow!("injected reduce-only flatten failure"))
        }
    }

    /// **第八轮 §9.6（High）· 「有仓、没 plan」是要处置的事实。**
    ///
    /// R11 的查询是 `position_state ps JOIN position_protection_plans pp`——
    /// 一条**有仓位、没有保护计划**的行不产出结果行，于是整件事折成
    /// `Known(None)`（"已知没有仓在等保护"），处置路径一步都不走。可
    /// 「入场已成交、连保护计划都没有」恰恰是最该被处置的形态。
    #[tokio::test]
    async fn an_entry_fill_on_a_position_without_a_plan_latches_flatten_and_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;

        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        sink.handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an unprotected exposure must not be reported as a healthy frame");

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a filled entry with no protection plan must be flattened reduce-only"
        );
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string()),
            "a filled entry with no protection plan must latch a retryable flatten"
        );
        assert!(!stream_is_ready(&store).await);
        cleanup(store, path).await;
    }

    /// **第八轮 §9.6（High）· 处置**写失败**必须持久、可重试、并保持 not-ready。**
    ///
    /// R11 的 flatten 失败只写一行 `error!`：这一帧照样 `success`，`ready`
    /// 照样是 1，重启之后连"曾经有过一个没保护的敞口"都不剩。
    ///
    /// 现在三件事必须同时成立：
    /// 1. `bot:` scope 被闩在 **`flatten`** 档（`kill_switch::spawn_action_retry_task`
    ///    的可重试处置，只有 `ReconcileTruth::flatten_complete()` 才解除）；
    /// 2. 用户流被记成 **not-ready**（`phase=protection_disposition`）；
    /// 3. 这一帧 **`Err`** 上抛，reader 断流重连走回补 → reconcile。
    #[tokio::test]
    async fn a_failed_protection_disposition_latches_a_retryable_flatten_and_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Startup,
            1,
        )
        .await
        .expect("begin recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            &store,
            CREDENTIAL,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            cycle,
            2,
        )
        .await
        .expect("mark the stream ready");

        let adapter = std::sync::Arc::new(FailingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        let error = sink
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a disposition that did not complete must not be reported as success");
        assert!(
            error.to_string().contains("disposition did not complete"),
            "the error must name what failed: {error}"
        );

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the immediate flatten must still have been attempted"
        );
        let latched =
            sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key='bot:bot_r9ws'")
                .fetch_optional(store.pool())
                .await
                .expect("read kill switch state")
                .expect("a failed disposition must leave a durable latch");
        assert_eq!(
            latched.get::<String, _>("level"),
            "flatten",
            "only a retryable flatten latch converges on exchange truth; pause_new alone \
             leaves the exposure open forever"
        );
        let state = crate::fill_replay::load_fill_stream_state(&store, CREDENTIAL)
            .await
            .expect("load stream state")
            .expect("stream state exists");
        assert!(
            !state.ready,
            "an unprotected exposure must not keep the stream admitting new risk"
        );
        assert_eq!(state.phase, "protection_disposition");
        cleanup(store, path).await;
    }

    // ==================================================================
    // 保护恢复以交易所真值完成
    // ==================================================================

    /// 把用户流推到 `ready=1`（走 fill_replay 的生产投影，不手写 SQL）。
    async fn mark_stream_ready(store: &Store) {
        let cycle = crate::fill_replay::begin_recovery_cycle(
            store,
            CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Startup,
            1,
        )
        .await
        .expect("begin recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            store,
            CREDENTIAL,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            cycle,
            2,
        )
        .await
        .expect("mark the stream ready");
    }

    /// 本地还有几条**未平**的仓位行。
    async fn open_position_rows(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM position_state WHERE state <> 'closed'")
            .fetch_one(store.pool())
            .await
            .expect("count open positions")
            .get::<i64, _>("n")
    }

    /// **regression（High）· 「flatten 请求被受理、仓位还在」必须保持
    /// not-ready，且闩住一条未收敛的 flatten。**
    ///
    /// R12 的形状：`submit_reduce_only_flatten()` 返回 `Ok(())` 就当处置完成，
    /// 这一帧成功、`ready` 保持 1、只 arm 了一个 `pause_new`。可那个 `Ok(())`
    /// 只是一次 **place-order 请求 ACK**（live adapter 下就是
    /// `kill_switch::submit_live_flatten` 下完 market order 就返回）——
    /// 部分成交、被交易所过滤规则挡下、下单后立刻反向成交，都会让 ACK 为真
    /// 而敞口还在。engine safety contract「保护单缺失或未经交易所真值确认」一律 High。
    ///
    /// 这条用例用的正是一个**成功**的 flatten 替身：它证明的是
    /// 「ACK 不解除 not-ready」，而不是"失败会被接住"（那是 R12 那条）。
    #[tokio::test]
    async fn a_flatten_accepted_but_not_proven_flat_keeps_the_stream_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;
        mark_stream_ready(&store).await;
        assert!(
            stream_is_ready(&store).await,
            "the fixture must really start from a ready stream"
        );

        // 这个替身**成功**：它就是"交易所受理了这次 reduce-only 平仓请求"。
        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        let error = sink
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an accepted place-order request is not proof the position is flat");
        assert!(
            error.to_string().contains("disposition did not complete"),
            "the error must name what did not complete: {error}"
        );

        assert_eq!(
            adapter
                .flatten_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the immediate reduce-only flatten must still have been requested"
        );
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string()),
            "an accepted request must leave the latch UN-converged"
        );
        assert!(
            !stream_is_ready(&store).await,
            "a position that is not proven flat must not keep admitting new risk"
        );
        assert_eq!(
            open_position_rows(&store).await,
            1,
            "the fixture's whole point is that the position really is still open"
        );
        cleanup(store, path).await;
    }

    /// **拿不到交易所凭据时，闩仍然由这条路径本身落下。**
    ///
    /// 变异自证暴露的缺口（MUT-16）：`latch_unprotected_entry_fill` 里那次
    /// `latch_flatten_pending` 在**注入了替身 adapter** 的用例里是冗余的——
    /// `flatten_and_pause_bot` 已经先闩过一次了。它唯一独自承担闩责的场合，
    /// 正是这条：**连 adapter 都构造不出来**（模拟凭据、未配置 venue、
    /// 环境里没有密钥）。这时 `request_reduce_only_flatten` 直接返回
    /// `no exchange credentials configured`，一次 flatten 请求都没发出去，
    /// 敞口完全没被碰过——却更需要闩住。
    ///
    /// 这条用例**不注入替身**，走的是生产的 `keys_from_env` 分支。
    #[tokio::test]
    async fn an_unprotected_fill_without_exchange_credentials_still_latches_and_goes_not_ready() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;
        mark_stream_ready(&store).await;

        // 不注入任何替身：adapter 只能从 `keys_from_env` 来，而本地没有密钥。
        let sink = BinanceEventSink::new(store.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        let error = sink
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an exposure nobody could even try to flatten must not report success");
        assert!(
            error.to_string().contains("disposition did not complete"),
            "the error must name what did not complete: {error}"
        );

        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string()),
            "with no adapter at all, this path is the only thing that can latch"
        );
        assert!(!stream_is_ready(&store).await);
        cleanup(store, path).await;
    }

    /// **重启之后仍然被重试。**
    ///
    /// 断言用的是**生产的扫描判据本身**
    /// （`kill_switch::scopes_needing_action_retry`，`spawn_action_retry_task`
    /// 每秒调用的那个符号），不是测试自己复写一遍那段 SQL。
    #[tokio::test]
    async fn a_latched_flatten_is_still_retried_after_a_restart() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;
        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        sink.handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an accepted place-order request is not proof the position is flat");
        drop(sink);
        store.pool().close().await;

        // 真正的重启：同一个 sqlite 文件，全新的连接池。
        let reopened = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("reopen the store the way a restarted process would");
        let scopes = crate::kill_switch::scopes_needing_action_retry(&reopened)
            .await
            .expect("read the retry scan set");
        assert!(
            scopes.contains(&(
                "bot:bot_r9ws".to_string(),
                crate::kill_switch::Level::Flatten
            )),
            "a restarted process must still pick this scope up for retry: {scopes:?}"
        );
        cleanup(reopened, path).await;
    }

    /// **只有交易所真值能收敛这条 latch。**
    ///
    /// 两个方向：仓位还没归零的对账真值不收敛（仍在重试集合里），
    /// 归零的对账真值才把 `flatten_state` 写成 `flattened` 并离开重试集合。
    #[tokio::test]
    async fn only_a_reconcile_truth_that_proves_zero_positions_converges_the_latch() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "acked", "9001").await;
        seed_position_without_plan(&store).await;
        let adapter = std::sync::Arc::new(CountingFlattenAdapter::default());
        let sink =
            BinanceEventSink::new(store.clone()).with_protection_failure_adapter(adapter.clone());
        let envelope = serde_json::from_str(&entry_fill_frame()).unwrap();
        sink.handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("an accepted place-order request is not proof the position is flat");

        // 交易所说仓位还在：不收敛。
        crate::kill_switch::record_reconcile_for_scope_now(
            &store,
            "bot:bot_r9ws",
            CREDENTIAL,
            crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: false,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            10,
        )
        .await
        .expect("record a non-flat reconcile truth");
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattening".to_string()),
            "a truth that still sees a position must not converge the latch"
        );

        // 交易所与本地都归零：收敛。
        sqlx::query("UPDATE position_state SET state='closed' WHERE position_id='pos_r12noplan'")
            .execute(store.pool())
            .await
            .expect("close the local position row");
        crate::kill_switch::record_reconcile_for_scope_now(
            &store,
            "bot:bot_r9ws",
            CREDENTIAL,
            crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            11,
        )
        .await
        .expect("record a flat reconcile truth");
        assert_eq!(
            bot_latch(&store).await,
            ("flatten".to_string(), "flattened".to_string()),
            "exchange truth that proves the position flat is what converges the latch"
        );
        let scopes = crate::kill_switch::scopes_needing_action_retry(&store)
            .await
            .expect("read the retry scan set");
        assert!(
            !scopes
                .iter()
                .any(|(scope_key, _)| scope_key == "bot:bot_r9ws"),
            "a converged latch must leave the retry set: {scopes:?}"
        );
        cleanup(store, path).await;
    }

    /// 终态行仍然可以**补一个缺失的交易所身份**——它不改状态，
    /// 只让这张单重新可被 id 追踪（否则"永不写回"会把身份也一起丢掉）。
    #[tokio::test]
    async fn a_terminal_order_may_still_gain_a_missing_exchange_identity() {
        let (store, path) = test_store().await;
        seed_order(&store, "eng_r9ws", "failed", "").await;
        let envelope = serde_json::from_str(&order_update("BTCUSDT", 9001, "NEW")).unwrap();

        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        let row = sqlx::query(
            "SELECT status, COALESCE(exchange_order_id,'') AS v FROM engine_orders WHERE engine_order_id='eng_r9ws'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "failed");
        assert_eq!(row.get::<String, _>("v"), "9001");
        cleanup(store, path).await;
    }

    // ==================================================================
    // ACCOUNT_UPDATE 帧的身份与失败传播
    // ==================================================================

    fn account_frame(symbol: &str, qty: &str) -> String {
        serde_json::json!({
            "e": "ACCOUNT_UPDATE",
            "E": 2,
            "T": 3,
            "a": { "P": [{ "s": symbol, "pa": qty, "ps": "LONG", "ep": "100" }] }
        })
        .to_string()
    }

    async fn seed_local_position(store: &Store, position_id: &str, symbol_key: &str) {
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r9ws', ?, 'long', '1', '100', 'open', 1, 1, 1)
            "#,
        )
        .bind(position_id)
        .bind(CREDENTIAL)
        .bind(symbol_key)
        .execute(store.pool())
        .await
        .expect("seed local position");
    }

    async fn position_qty(store: &Store, position_id: &str) -> String {
        sqlx::query("SELECT qty FROM position_state WHERE position_id = ?")
            .bind(position_id)
            .fetch_one(store.pool())
            .await
            .expect("read position qty")
            .get::<String, _>("qty")
    }

    /// **regression（High）· 仓位真值的落点也是完整身份。**
    ///
    /// 一条币本位（`…|BTC|USDT|BTC`）本地仓的 `base+quote` 同样拼出
    /// `BTCUSDT`。裸判据会把 USDⓈ-M 用户流的仓位数量写到它头上——
    /// 一份真值被应用到另一张合约。
    #[tokio::test]
    async fn a_coin_margined_position_never_receives_usdm_account_truth() {
        let (store, path) = test_store().await;
        seed_local_position(&store, "pos_coinm", "binance|futures|BTC|USDT|BTC").await;
        let envelope = serde_json::from_str(&account_frame("BTCUSDT", "5")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("the frame itself applies; it simply owns no local position here");
        assert_eq!(
            position_qty(&store, "pos_coinm").await,
            "1",
            "USDⓈ-M truth must not be written onto a coin-margined position"
        );
        cleanup(store, path).await;
    }

    /// **放行面（反向断言）**：同一条 USDⓈ-M 合约的本地仓照常收到真值。
    #[tokio::test]
    async fn a_usdm_position_still_receives_its_account_truth() {
        let (store, path) = test_store().await;
        seed_local_position(&store, "pos_usdm", BTC_KEY).await;
        let envelope = serde_json::from_str(&account_frame("BTCUSDT", "5")).unwrap();
        BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect("a healthy account frame must apply");
        assert_eq!(
            position_qty(&store, "pos_usdm").await,
            "5",
            "the release side must be a real write, not a no-op"
        );
        cleanup(store, path).await;
    }

    /// **第八轮 §9.5（High）· ACCOUNT_UPDATE 的仓位真值写失败必须整帧失败。**
    ///
    /// 这是 `handle_account_update` 三处失败点里的**中间那处**：
    /// R11 在这里 `tracing::warn!` + `rollback` + `return`——reader 拿到的是
    /// `()`，于是仓位真值丢了而 `ready` 还是 1。
    #[tokio::test]
    async fn an_account_position_truth_write_failure_fails_the_frame() {
        let (store, path) = test_store().await;
        seed_local_position(&store, "pos_usdm", BTC_KEY).await;
        sqlx::query(
            "CREATE TRIGGER r12_block_position_truth BEFORE UPDATE ON position_state \
             FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'injected position truth failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        let envelope = serde_json::from_str(&account_frame("BTCUSDT", "5")).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a position truth that could not be applied must fail the frame");
        assert!(
            error.to_string().contains("position truth"),
            "the error must name what failed: {error}"
        );
        assert_eq!(
            event_count(&store, "account_update").await,
            0,
            "nothing from a rolled-back frame may be visible"
        );
        cleanup(store, path).await;
    }

    /// **第八轮 §9.5（High）· `begin()` 失败也是三处失败点之一。**
    ///
    /// R11 的 `if let Ok(mut tx) = begin { … }` 在这里**连 else 都没有**：
    /// 事务开不出来时整个函数悄悄返回 `()`，这一帧的仓位真值一个字节都没落，
    /// 而 reader 什么都看不到。故障是真的：连接池已关闭。
    #[tokio::test]
    async fn an_account_frame_whose_transaction_cannot_begin_fails_the_frame() {
        let (store, path) = test_store().await;
        seed_local_position(&store, "pos_usdm", BTC_KEY).await;
        store.pool().close().await;

        let envelope = serde_json::from_str(&account_frame("BTCUSDT", "5")).unwrap();
        let error = BinanceEventSink::new(store.clone())
            .handle_ws_envelope(CREDENTIAL, &envelope)
            .await
            .expect_err("a frame whose transaction cannot begin must not report success");
        assert!(
            error.to_string().contains("begin its transaction"),
            "the error must name what failed: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    // ==================================================================
    // REST 回补侧的完整身份
    // ==================================================================

    fn rest_trade(
        id: i64,
        symbol: &str,
        order_id: i64,
    ) -> crate::binance_futures::BinanceUserTrade {
        crate::binance_futures::BinanceUserTrade {
            id,
            symbol: symbol.to_string(),
            order_id: Some(order_id),
            side: Some("BUY".to_string()),
            price: Some("100".to_string()),
            qty: Some("1".to_string()),
            quote_qty: Some("100".to_string()),
            commission: Some("0.1".to_string()),
            commission_asset: Some("USDT".to_string()),
            realized_pnl: Some("0".to_string()),
            time: Some(4_000),
            additional_fields: serde_json::Map::new(),
        }
    }

    /// **regression（High）· REST 回补的后验判据与 WS 同源。**
    ///
    /// R11R §7 登记过这一处：`symbol_from_symbol_key` 只比 `base+quote`，
    /// 于是一条币本位（`…|BTC|USDT|BTC`）的本地单被 orderId 撞上之后，
    /// 后验算出 `BTCUSDT` 与上游 `BTCUSDT` 相等而放行——USDⓈ-M 的成交真值
    /// 落到了另一张合约的单上。
    #[tokio::test]
    async fn a_coin_margined_local_order_never_claims_a_usdm_rest_fill() {
        let (store, path) = test_store().await;
        seed_order_full(
            &store,
            "eng_r12rest",
            "acked",
            "8801",
            "binance|futures|BTC|USDT|BTC",
            1,
        )
        .await;
        let sink = BinanceEventSink::new(store.clone());
        let error = sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect_err("a coin-margined local order must not absorb USDⓈ-M truth");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS n FROM engine_fills")
                .fetch_one(store.pool())
                .await
                .expect("count fills")
                .get::<i64, _>("n"),
            0,
            "a refused identity must not write a fill"
        );
        cleanup(store, path).await;
    }

    // ==================================================================
    // REST 回补的归属只按身份，不按时间
    // ==================================================================

    /// **regression（High）· 跨 symbol 撞 `orderId` 时，两个方向都必须
    /// 认领到自己那一张单。**
    ///
    /// Binance 的 `orderId` 只在 (账户, symbol) 内唯一。R12 的查询是
    /// `ORDER BY eo.created_at_ms DESC LIMIT 1`——**先取最新的那一行**，
    /// 取到之后才比 symbol。于是同一个 id 上两张不同合约的本地单里，
    /// 较新的那一行会把正确的那一行整个遮住：后验一比 symbol 不符就整帧
    /// `failed_precondition`，正确的单永远轮不到被认领，这条交易所真值
    /// 也就永远落不了库（engine safety contract「fill 必须以交易所真值对账」）。
    ///
    /// **两个方向都断言**是这条用例的关键：只测 BTC 那一半的话，把排序
    /// 改成 `ASC`（评审变异 R9M11）照样全绿——`ASC` 恰好先取到 BTC。
    /// 一条帧认领旧行、另一条帧认领新行，任何"按时间取一行"的实现
    /// 都至少在其中一边翻车。
    #[tokio::test]
    async fn a_cross_symbol_order_id_collision_claims_each_order_by_identity() {
        let (store, path) = test_store().await;
        // 同一个 credential 上同一个 exchange_order_id 的两张单：
        // BTCUSDT 是**较旧**的一张，ETHUSDT 是**较新**的一张。
        seed_order_full(&store, "eng_btc", "acked", "8801", BTC_KEY, 1).await;
        seed_order_full(
            &store,
            "eng_eth",
            "acked",
            "8801",
            "binance|futures|ETH|USDT|USDT",
            2,
        )
        .await;
        let sink = BinanceEventSink::new(store.clone());

        assert!(sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect("the BTCUSDT truth belongs to the BTCUSDT order"));
        assert!(sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(42, "ETHUSDT", 8801))
            .await
            .expect("the ETHUSDT truth belongs to the ETHUSDT order"));

        assert_eq!(
            fill_owners(&store).await,
            vec![
                ("41".to_string(), "eng_btc".to_string()),
                ("42".to_string(), "eng_eth".to_string()),
            ],
            "each exchange truth must land on the local order that really is that contract"
        );
        cleanup(store, path).await;
    }

    /// **本地自己有歧义时 fail-closed。**
    ///
    /// 同一个 `orderId` 上有两张**身份同样相符**的本地单：随便挑一条等于赌。
    /// 必须整帧拒绝，由回补循环记 recovery failure（`ready=0`）驱动恢复，
    /// 而不是挑一条写进去然后返回健康。
    #[tokio::test]
    async fn an_ambiguous_local_order_id_is_refused_instead_of_guessed() {
        let (store, path) = test_store().await;
        seed_order_full(&store, "eng_dup_a", "acked", "8801", BTC_KEY, 1).await;
        seed_order_full(&store, "eng_dup_b", "acked", "8801", BTC_KEY, 2).await;
        let sink = BinanceEventSink::new(store.clone());

        let error = sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect_err("two equally matching local orders must not be guessed between");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("ambiguous"),
            "the refusal must name the ambiguity: {}",
            error.message()
        );
        assert_eq!(
            fill_owners(&store).await,
            Vec::<(String, String)>::new(),
            "an ambiguous identity must not write a fill"
        );
        cleanup(store, path).await;
    }

    /// **放行面**：本地压根没有这个 `orderId` 时仍然登记成外部成交。
    ///
    /// 没有它，"一律 fail-closed" 也能让上面两条全绿——而那会让 fill
    /// recovery 丢掉所有非本引擎下的成交（`external:<id>` 是回补完整性的
    /// 既有约定，不是身份歧义）。
    #[tokio::test]
    async fn a_trade_with_no_local_order_at_all_still_books_an_external_fill() {
        let (store, path) = test_store().await;
        let sink = BinanceEventSink::new(store.clone());
        assert!(sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect("an unknown order id is an external fill, not an identity conflict"));
        assert_eq!(
            fill_owners(&store).await,
            vec![("41".to_string(), "external:8801".to_string())]
        );
        cleanup(store, path).await;
    }

    // ==================================================================
    // REST 认领要核对两个身份来源
    // ==================================================================

    /// 一张外层与内层**互相矛盾**的活单：`exchange`/`market_kind` 两列写着
    /// okx/spot，`symbol_key` 内部却声明自己是 binance USDⓈ-M。
    async fn seed_order_with_declared_market(
        store: &Store,
        engine_order_id: &str,
        exchange: &str,
        market_kind: &str,
        symbol_key: &str,
        exchange_order_id: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r14rest', ?, ?, ?, ?,
              'open_long', 'long', 'market', '1', 0, 'entry', 'acked', ?,
              'coid-r14rest', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(CREDENTIAL)
        .bind(format!("idem-{engine_order_id}"))
        .bind(exchange)
        .bind(market_kind)
        .bind(symbol_key)
        .bind(exchange_order_id)
        .execute(store.pool())
        .await
        .expect("seed engine order with an explicit declared market");
    }

    /// **regression（High）· 两个身份来源不一致的候选行不得认领交易所真值。**
    ///
    /// R13 的候选裁决只信 `symbol_key` 一列。同一行还存着**外层**
    /// `exchange` / `market_kind`：一条 `okx`/`spot` 的单只要内层 key 写成
    /// `binance|futures|BTC|USDT|USDT`，就能被这条 Binance USDⓈ-M 的 REST
    /// 成交认领——别的市场的一张单被写上 Binance 的成交真值。
    ///
    /// 矛盾行不是"另一个市场的单"可以静默跳过：两个来源里必有一个是错的，
    /// 本地在这条 `orderId` 上已经无法自证归属，只能整帧 fail-closed。
    #[tokio::test]
    async fn a_candidate_whose_two_identity_sources_disagree_is_refused() {
        let (store, path) = test_store().await;
        seed_order_with_declared_market(
            &store,
            "eng_r14_contradictory",
            "okx",
            "spot",
            BTC_KEY,
            "8801",
        )
        .await;
        let sink = BinanceEventSink::new(store.clone());

        let error = sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect_err("a row whose two identities disagree must not claim exchange truth");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("identity is inconsistent"),
            "the refusal must name the inconsistency: {}",
            error.message()
        );
        assert_eq!(
            fill_owners(&store).await,
            Vec::<(String, String)>::new(),
            "a contradictory candidate must not write a fill anywhere"
        );
        cleanup(store, path).await;
    }

    /// **放行面（反向断言）**：两个来源**一致**地写着另一个市场的那一行，
    /// 只是不参与认领——它不该把整帧带成 fail-closed。
    ///
    /// 没有它，"任何非 USDⓈ-M 候选一律拒绝"也能让上面那条全绿；而那会让
    /// 一条老老实实写着 okx/spot 的历史行，把同 `orderId` 的正确认领
    /// 永久挡死。这里同一个 `orderId` 上还有一张真正的 USDⓈ-M 单，
    /// 它必须照常拿到这条成交。
    #[tokio::test]
    async fn a_consistent_foreign_market_row_is_skipped_not_fatal() {
        let (store, path) = test_store().await;
        seed_order_with_declared_market(
            &store,
            "eng_r14_okx",
            "okx",
            "spot",
            "okx|spot|BTC|USDT|USDT",
            "8801",
        )
        .await;
        seed_order_full(&store, "eng_r14_usdm", "acked", "8801", BTC_KEY, 2).await;
        let sink = BinanceEventSink::new(store.clone());

        assert!(sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect("a self-consistent foreign row is not an identity conflict"));
        assert_eq!(
            fill_owners(&store).await,
            vec![("41".to_string(), "eng_r14_usdm".to_string())],
            "the USDⓈ-M order must still claim its own fill"
        );
        cleanup(store, path).await;
    }

    /// `(exchange_trade_id, engine_order_id)` 全表，按 trade id 排序。
    async fn fill_owners(store: &Store) -> Vec<(String, String)> {
        sqlx::query(
            "SELECT exchange_trade_id, engine_order_id FROM engine_fills \
             ORDER BY exchange_trade_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("read engine fills")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("exchange_trade_id"),
                row.get::<String, _>("engine_order_id"),
            )
        })
        .collect()
    }

    /// **放行面（反向断言）**：闭世界内的本地单照常认领它自己的 REST 成交。
    #[tokio::test]
    async fn a_usdm_local_order_still_claims_its_rest_fill() {
        let (store, path) = test_store().await;
        seed_order_full(&store, "eng_r12rest", "acked", "8801", BTC_KEY, 1).await;
        let sink = BinanceEventSink::new(store.clone());
        assert!(sink
            .handle_rest_user_trade(CREDENTIAL, &rest_trade(41, "BTCUSDT", 8801))
            .await
            .expect("a USDⓈ-M local order claims its own fill"));
        let engine_order_id: String =
            sqlx::query("SELECT engine_order_id FROM engine_fills LIMIT 1")
                .fetch_one(store.pool())
                .await
                .expect("the fill must exist")
                .get("engine_order_id");
        assert_eq!(engine_order_id, "eng_r12rest");
        cleanup(store, path).await;
    }
}

#[cfg(test)]
#[path = "svc_open_algo_tests.rs"]
mod open_algo_list_tests;

#[cfg(test)]
#[path = "svc_rest_fill_tests.rs"]
mod rest_fill_reporting_tests;

#[cfg(test)]
#[path = "svc_get_order_refresh_tests.rs"]
mod get_order_refresh_tests;

#[cfg(test)]
#[path = "svc_get_order_budget_tests.rs"]
mod get_order_budget_tests;

/// **live 撤单的完整生产入口 wiring proof。**
///
/// 这一组从 `EngineExecution::cancel_order`（gRPC 方法本体，外部实际调用的
/// 那个符号）出发，经真实的 venue/凭据档位判定、真实的 `symbol_key` 身份
/// 解析，直到写回落库为止；断言的是**库里的状态**与**返回给调用方的回执**，
/// 不是探针。
///
/// **没有网络**：被证明的两条分支都在构造交易所客户端**之前**就分流完毕——
/// 拒绝面根本不会走到 HTTP，放行面（paper 本地撤单）本来就不联网。
#[cfg(test)]
mod r13_cancel_identity_tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;
    use tonic::Request;

    use super::{EngineExecution, EngineExecutionService};
    use crate::proto::CancelOrderRequest;
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    /// 认不出闭世界身份的 `symbol_key`：venue/market/settle 三段全错。
    const ALIEN_KEY: &str = "okx|spot|BTC|USDT|BTC";

    struct EnvGuard {
        name: String,
        previous: Option<OsString>,
    }

    impl EnvGuard {
        fn set(name: String, value: &str) -> Self {
            let previous = std::env::var_os(&name);
            std::env::set_var(&name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var(&self.name, value),
                None => std::env::remove_var(&self.name),
            }
        }
    }

    /// 一个 testnet 档位的交易所凭据。变量名由 `credential_secret_locator`
    /// 逐凭据生成，所以并行用例之间不会抢同一个环境变量。值是本地假凭据。
    fn exchange_credential_env(credential_id: &str) -> Vec<EnvGuard> {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        vec![
            EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet"),
            EnvGuard::set(key_env, "local-test-key"),
            EnvGuard::set(secret_env, "local-test-secret"),
        ]
    }

    async fn test_store() -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r13cx-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 一张**声明为 binance/futures**、但 `symbol_key` 认不出身份的活单。
    ///
    /// 外层两段声明是合规的（否则连 live 分支都进不去），身份断裂只在
    /// `symbol_key` 自己那五段里——这正是 regression 的形状。
    async fn seed_alien_order(store: &Store, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES ('eng_r13cx', ?, 'bot_r13cx', 'idem-eng_r13cx',
              'binance', 'futures', ?, 'open_long', 'long', 'market',
              '1', 0, 'entry', 'acked', '7701', 'coid-r13cx', 1, 1)
            "#,
        )
        .bind(credential_id)
        .bind(ALIEN_KEY)
        .execute(store.pool())
        .await
        .expect("seed an order whose symbol_key has no closed-world identity");
    }

    async fn order_status(store: &Store) -> String {
        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='eng_r13cx'")
            .fetch_one(store.pool())
            .await
            .expect("read the order back")
            .get::<String, _>("status")
    }

    async fn cancel(
        service: &EngineExecutionService,
        credential_id: &str,
    ) -> crate::proto::CancelOrderResponse {
        service
            .cancel_order(Request::new(CancelOrderRequest {
                credential_id: credential_id.to_string(),
                engine_order_id: "eng_r13cx".to_string(),
                reason: "operator cancel".to_string(),
            }))
            .await
            .expect("the production entry must answer")
            .into_inner()
    }

    /// **拒绝面**：live 档位下认不出身份 → fail-closed，**不落 local cancel**。
    ///
    /// R12 的形状是 `if let (Some(keys), Some(symbol)) = …`：身份解析失败时
    /// 整个 `if` 落空，控制流继续走到方法末尾那条 **paper/dev 的 local
    /// cancel**——把 `engine_orders` 写成 `canceled` 并回 `accepted=true`，
    /// 而一个字节都没发给交易所。能走到这里的前提是
    /// `exchange_gateway_allowed` 为真（交易所背书的凭据 + testnet/live 档位），
    /// 也就是说那张单很可能仍然在交易所挂着：本地一句"已撤"之后，
    /// 撤单真值就此丢失（engine safety contract「运行状态丢失」「paper/live 混淆」）。
    #[tokio::test]
    async fn a_live_cancel_with_an_unresolvable_identity_is_refused_and_not_cancelled_locally() {
        let credential_id = "r13cx-desk-live";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = test_store().await;
        seed_alien_order(&store, credential_id).await;
        let service =
            EngineExecutionService::new(store.clone()).without_background_exchange_tasks();

        let response = cancel(&service, credential_id).await;

        assert!(
            !response.accepted,
            "an exchange order that was never cancelled must not be acknowledged as cancelled"
        );
        assert!(
            response
                .message
                .contains("cancel_refused_unresolved_identity"),
            "the refusal must name why it refused: {}",
            response.message
        );
        assert_eq!(
            order_status(&store).await,
            "acked",
            "the local row must not be marked canceled while the exchange order may be open"
        );
        drop(service);
        cleanup(store, path).await;
    }

    /// 一张**身份完全合法**的 USDⓈ-M 活单：身份解析不会失败，
    /// 于是唯一能让控制流落到末尾那条 local cancel 的就是"没有密钥"。
    async fn seed_usdm_order(store: &Store, credential_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES ('eng_r13cx', ?, 'bot_r13cx', 'idem-eng_r13cx',
              'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
              'open_long', 'long', 'market',
              '1', 0, 'entry', 'acked', '7701', 'coid-r13cx', 1, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed a well-formed USDⓈ-M order");
    }

    /// **拒绝面**：
    /// 网关放行但没有密钥 → fail-closed，**不落 local cancel**。
    ///
    /// R13 修掉的是"身份认不出"那条路；`keys` 为 `None` 的那条原样留着：
    /// `if let Some(keys)` 落空之后控制流照样走到方法末尾那条 paper/dev 的
    /// local cancel，把单写成 `canceled` 并回 `accepted=true`，而一个字节都
    /// 没发给交易所。这条用例把两个前提都钉死：档位是 testnet（网关放行）、
    /// `symbol_key` 完全合法（身份解析成功）——所以它观察的只能是缺密钥
    /// 这一条分支。
    #[tokio::test]
    async fn a_live_cancel_without_api_keys_is_refused_and_not_cancelled_locally() {
        let credential_id = "r14cx-desk-nokeys";
        // 只设 venue：这个 credential 的 per-key 环境变量不存在，且
        // `keys_from_env` 在 venue 已配时**不会**回落到全局主密钥。
        let _env = EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet");
        let (store, path) = test_store().await;
        seed_usdm_order(&store, credential_id).await;
        let service =
            EngineExecutionService::new(store.clone()).without_background_exchange_tasks();

        let response = cancel(&service, credential_id).await;

        assert!(
            !response.accepted,
            "an exchange order that was never cancelled must not be acknowledged as cancelled"
        );
        assert!(
            response
                .message
                .contains("cancel_refused_missing_credentials"),
            "the refusal must name why it refused: {}",
            response.message
        );
        assert_eq!(
            order_status(&store).await,
            "acked",
            "the local row must not be marked canceled while the exchange order may be open"
        );
        drop(service);
        cleanup(store, path).await;
    }

    /// **放行面（反向断言）**：同一张合法 USDⓈ-M 单，模拟凭据照旧本地撤。
    ///
    /// 与上一条只差档位：没有它，"缺密钥一律拒绝"会被写成"一律拒绝"
    /// 也全绿，而那会让 paper/dev 的撤单整条不可用。
    #[tokio::test]
    async fn a_simulation_credential_with_a_valid_identity_still_gets_its_local_cancel() {
        let (store, path) = test_store().await;
        seed_usdm_order(&store, "paper").await;
        let service =
            EngineExecutionService::new(store.clone()).without_background_exchange_tasks();

        let response = cancel(&service, "paper").await;

        assert!(
            response.accepted,
            "a simulation credential never reaches an exchange; local cancel stays available"
        );
        assert_eq!(order_status(&store).await, "canceled");
        drop(service);
        cleanup(store, path).await;
    }

    /// **放行面（反向断言）**：模拟凭据不经交易所网关，同一张单照旧本地撤。
    ///
    /// 没有它，"一律拒绝" 也能让上面那条全绿——而那会让 paper/dev 的撤单
    /// 整条不可用。两条用例的差别**只有档位**：同一个 `symbol_key`、
    /// 同一个生产入口、同一条 SQL。
    #[tokio::test]
    async fn a_simulation_credential_still_gets_its_local_cancel() {
        let (store, path) = test_store().await;
        seed_alien_order(&store, "paper").await;
        let service =
            EngineExecutionService::new(store.clone()).without_background_exchange_tasks();

        let response = cancel(&service, "paper").await;

        assert!(
            response.accepted,
            "a simulation credential never reaches an exchange; local cancel stays available"
        );
        assert_eq!(response.message, "operator cancel");
        assert_eq!(order_status(&store).await, "canceled");
        drop(service);
        cleanup(store, path).await;
    }
}

/// **sqlx-i64 核查批· `svc.rs` 四个读点的行为回归。**
///
/// 判别力全在夹具的**数量级**上：旧 sqlx 0.7.4 `Any` + SQLite 的宽整数
/// 截断对小值夹具不可见。这里一律用真实毫秒 epoch，防止依赖更新把已修复的
/// 直接 `i64` 读取重新退化。
#[cfg(test)]
mod sqlx_i64_width_tests {
    use std::path::PathBuf;
    use std::sync::Once;
    use std::time::Duration;

    use sqlx::any::AnyPoolOptions;
    use tokio_stream::StreamExt;
    use tonic::Request;

    use super::{now_ms, EngineEventsService, EngineExecutionService};
    use crate::proto::engine_events_server::EngineEvents;
    use crate::proto::engine_execution_server::EngineExecution;
    use crate::proto::{GetOpenOrdersRequest, GetOrderRequest, StreamEventsRequest};
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    /// 2026-08-25 附近的真实毫秒 epoch；低 32 位是 723097806。
    const WIDE_MS: i64 = 1_787_429_492_942;

    async fn test_store(label: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-i64-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 一张 `acked` 的现货单：`market_kind='spot'` 让 `get_order` 不走
    /// 交易所回查分支（本用例要断言的是**本地读侧**的真值）。
    async fn seed_order(store: &Store, engine_order_id: &str, created: i64, updated: i64) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES (?, 'cred_i64', 'bot_i64', ?, ?, '9001',
              'binance', 'spot', 'binance|spot|BTC|USDT|USDT',
              'open_long', 'long', 'market', '1', 0, 'acked', 'entry', ?, ?)
            "#,
        )
        .bind(engine_order_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(format!("coid-{engine_order_id}"))
        .bind(created)
        .bind(updated)
        .execute(store.pool())
        .await
        .expect("seed an order stamped at a real millisecond epoch");
    }

    /// **熔断窗口的两个端点被截断 = 熔断器永远数不满。**
    ///
    /// `first_error_ms` / `last_error_ms` 截成 7.2e8 之后，
    /// `ts - last_ms ≈ 1.79e12` 恒 > 60s 窗口，于是 `record_credential_error`
    /// 每一次都把 `consecutive` 重置成 1——连续失败计数永远到不了 5，
    /// 降级永远不会因连续失败触发。
    #[tokio::test]
    async fn five_consecutive_errors_degrade_the_credential_at_a_real_clock() {
        let (store, path) = test_store("degrade").await;
        let service = EngineExecutionService::new(store.clone());
        assert!(
            !service
                .is_credential_degraded("cred_i64")
                .await
                .expect("read empty credential health"),
            "a credential with no error history must not start degraded"
        );
        for _ in 0..5 {
            service
                .record_credential_error("cred_i64", "injected exchange error")
                .await
                .expect("record credential error");
        }
        let row = sqlx::query(
            "SELECT degraded, consecutive_errors, \
             CAST(first_error_ms AS CHAR) AS first_error_ms_text \
             FROM engine_credential_health WHERE credential_id='cred_i64'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load credential health");
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "consecutive_errors"),
            5,
            "five failures inside the 60s window must count as five, not restart at one"
        );
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "degraded"), 1);
        assert!(
            crate::store::try_get_i64_text(&row, "first_error_ms_text")
                .expect("decode first_error_ms")
                > i64::from(i32::MAX),
            "the fixture must really be at a real millisecond epoch, \
             otherwise this test has no discriminating power"
        );
        assert!(service
            .is_credential_degraded("cred_i64")
            .await
            .expect("read degraded credential"));
        drop(service);
        cleanup(store, path).await;
    }

    /// **变异 M08b 存活补测。**
    ///
    /// 上一条用例走不到 `is_credential_degraded` 的时间戳判据：它在
    /// `degraded != 0` 那一行就提前 `return true` 了，于是把那半边的读点
    /// 还原成直读 `i64` 也杀不掉它（第一轮变异实测 SURVIVED）。
    ///
    /// 那半边管的是「计数已满、`degraded` 标志却是 0」这个状态
    /// （降级被清过、或 `RUSTCORE_CIRCUIT_BREAKER_FAILS` 被调低）。
    /// 时间戳被截断时 `ts - last_ms ≈ 1.79e12` 恒 > 窗口，函数返回 `false`
    /// ——一个**本该被拒的**凭据被判成健康、继续下单。这里直接构造那个状态。
    #[tokio::test]
    async fn a_full_error_window_without_the_degraded_flag_still_reads_as_degraded() {
        let (store, path) = test_store("degrade-window").await;
        let fresh = now_ms() - 1_000;
        assert!(fresh > i64::from(i32::MAX));
        sqlx::query(
            // `reconcile_diff_open = 0` 必须显式写：这条用例钉的是**熔断器**
            // 那半边判据，不写就落 `NULL`（0029 之前的存量行语义），
            // `is_credential_degraded` 会 fail-closed 直接返回 true，
            // 放行面那半段就再也走不到时间戳判据了。
            "INSERT INTO engine_credential_health (credential_id, degraded, consecutive_errors, \
             first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open) \
             VALUES ('cred_i64', 0, 5, ?, ?, ?, 0)",
        )
        .bind(fresh)
        .bind(fresh)
        .bind(fresh)
        .execute(store.pool())
        .await
        .expect("seed a full error window whose degraded flag was cleared");

        let service = EngineExecutionService::new(store.clone());
        assert!(
            service
                .is_credential_degraded("cred_i64")
                .await
                .expect("read full error window"),
            "five failures 1s ago are inside the 60s window: the credential must read as degraded"
        );

        // 放行面自证：同一条判据在窗口**之外**必须放行，否则这条用例
        // 用一个恒 true 的实现也能通过。
        let stale = now_ms() - 10 * 60 * 1_000;
        sqlx::query("UPDATE engine_credential_health SET first_error_ms = ?, last_error_ms = ? WHERE credential_id='cred_i64'")
            .bind(stale)
            .bind(stale)
            .execute(store.pool())
            .await
            .expect("age the error window out");
        assert!(
            !service
                .is_credential_degraded("cred_i64")
                .await
                .expect("read aged error window"),
            "an error window that aged out of the 60s window must not keep the credential degraded"
        );
        drop(service);
        cleanup(store, path).await;
    }

    /// 同一凭据的并发失败必须一条不漏。旧的 SELECT→UPSERT read-modify-write
    /// 在 MySQL 多连接/多实例下会把两个 n+1 互相覆盖；这里从同一个生产方法
    /// 同时发出 24 个失败，并核对数据库中的精确计数。
    #[tokio::test]
    async fn concurrent_credential_errors_are_counted_atomically() {
        const FAILURES: usize = 24;
        let (store, path) = test_store("degrade-concurrent").await;
        let service = std::sync::Arc::new(EngineExecutionService::new(store.clone()));
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(FAILURES));
        let mut tasks = Vec::with_capacity(FAILURES);
        for _ in 0..FAILURES {
            let service = service.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                service
                    .record_credential_error("cred_atomic", "concurrent exchange error")
                    .await
            }));
        }
        for task in tasks {
            task.await
                .expect("credential error task must not panic")
                .expect("credential error write must succeed");
        }
        let row = sqlx::query(
            "SELECT degraded, consecutive_errors FROM engine_credential_health \
             WHERE credential_id='cred_atomic'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load atomically updated credential health");
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "consecutive_errors"),
            FAILURES as i64
        );
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "degraded"), 1);
        drop(service);
        cleanup(store, path).await;
    }

    /// DB 读写错误必须保留为错误，不能分别伪装成「无历史」「写成功」。
    #[tokio::test]
    async fn credential_health_database_errors_are_not_collapsed() {
        let (store, path) = test_store("degrade-errors").await;
        let service = EngineExecutionService::new(store.clone());
        store.pool().close().await;

        assert!(
            service
                .record_credential_error("cred_closed", "exchange error")
                .await
                .is_err(),
            "a failed counter write must remain an error"
        );
        assert!(
            service
                .clear_credential_errors("cred_closed")
                .await
                .is_err(),
            "a failed clear write must remain an error"
        );
        assert!(
            service.is_credential_degraded("cred_closed").await.is_err(),
            "a failed health read must not become healthy=false"
        );
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    /// SQLite 的 Store 刻意只有一个连接，无法证明多连接下没有 lost update。
    /// 这条在真实 MySQL scratch 库上让 32 个生产方法共享 8 连接并发推进同一行；
    /// 缺少真库时如实跳过，门禁可用 `RUSTCORE_REQUIRE_MYSQL_TESTS=1` 禁止跳过。
    #[tokio::test]
    async fn mysql_concurrent_credential_errors_are_counted_atomically() {
        const URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
        const REQUIRE_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";
        const FAILURES: usize = 32;
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let admin_url = match std::env::var(URL_ENV) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => {
                if std::env::var(REQUIRE_ENV).as_deref() == Ok("1") {
                    panic!("{REQUIRE_ENV}=1 requires {URL_ENV}");
                }
                eprintln!("SKIPPED mysql credential breaker concurrency: {URL_ENV} is not set");
                return;
            }
        };
        let admin = match AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                if std::env::var(REQUIRE_ENV).as_deref() == Ok("1") {
                    panic!("cannot connect required MySQL: {error}");
                }
                eprintln!("SKIPPED mysql credential breaker concurrency: {error}");
                return;
            }
        };
        let database = format!(
            "rustcore_credential_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("create credential breaker scratch database");
        admin.close().await;
        let (prefix, _) = admin_url
            .rsplit_once('/')
            .expect("the MySQL test URL must include a database path");
        let scratch_url = format!("{prefix}/{database}");
        let store = Store::connect(&scratch_url)
            .await
            .expect("migrate credential breaker MySQL scratch database");
        let service = std::sync::Arc::new(EngineExecutionService::new(store.clone()));
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(FAILURES));
        let mut tasks = Vec::with_capacity(FAILURES);
        for _ in 0..FAILURES {
            let service = service.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                service
                    .record_credential_error("cred_mysql_atomic", "concurrent exchange error")
                    .await
            }));
        }
        for task in tasks {
            task.await
                .expect("MySQL credential error task must not panic")
                .expect("MySQL credential error write must succeed");
        }
        let row = sqlx::query(
            "SELECT degraded,consecutive_errors FROM engine_credential_health \
             WHERE credential_id='cred_mysql_atomic'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read MySQL credential breaker row");
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "consecutive_errors"),
            FAILURES as i64
        );
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "degraded"), 1);

        // 放行面：成功清零仍必须恢复健康，防止“永远 degraded”伪实现。
        service
            .clear_credential_errors("cred_mysql_atomic")
            .await
            .expect("clear MySQL credential errors");
        assert!(!service
            .is_credential_degraded("cred_mysql_atomic")
            .await
            .expect("read cleared MySQL credential health"));
        drop(service);
        store.pool().close().await;
        let admin = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("reconnect to drop credential breaker scratch database");
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .expect("drop credential breaker scratch database");
        admin.close().await;
    }

    /// `GetOrder` / `GetOpenOrders` 是 v1 wire 的订单真值出口
    /// （`main.rs` 上线注册的就是它）。截断会把一个从未存在过的下单时刻
    /// 交给 control-plane。
    #[tokio::test]
    async fn get_order_and_get_open_orders_return_real_millisecond_epochs() {
        let (store, path) = test_store("get-order").await;
        seed_order(&store, "eng_i64_a", WIDE_MS - 5_000, WIDE_MS).await;
        let service = EngineExecutionService::new(store.clone());

        let response = service
            .get_order(Request::new(GetOrderRequest {
                credential_id: "cred_i64".to_string(),
                engine_order_id: "eng_i64_a".to_string(),
                ..Default::default()
            }))
            .await
            .expect("get_order succeeds")
            .into_inner();
        let order = response.order.expect("the order is found");
        assert_eq!(order.created_at_ms, WIDE_MS - 5_000);
        assert_eq!(order.updated_at_ms, WIDE_MS);

        let open = service
            .get_open_orders(Request::new(GetOpenOrdersRequest {
                credential_id: "cred_i64".to_string(),
                ..Default::default()
            }))
            .await
            .expect("get_open_orders succeeds")
            .into_inner();
        assert_eq!(open.orders.len(), 1);
        assert_eq!(open.orders[0].created_at_ms, WIDE_MS - 5_000);
        assert_eq!(open.orders[0].updated_at_ms, WIDE_MS);
        drop(service);
        cleanup(store, path).await;
    }

    /// `StreamEvents` 是 control-plane 的事件入口——截断会让**每一条**引擎
    /// 事件带着一个从未存在过的时刻进入跨库证据链。
    #[tokio::test]
    async fn stream_events_delivers_the_stored_event_timestamp() {
        let (store, path) = test_store("stream").await;
        sqlx::query(
            "INSERT INTO engine_order_events (credential_id, seq, event_id, ts_ms, event_type) \
             VALUES ('cred_i64', 1, 'evt_i64_1', ?, 'order_ack')",
        )
        .bind(WIDE_MS)
        .execute(store.pool())
        .await
        .expect("seed an event stamped at a real millisecond epoch");

        let service = EngineEventsService::new(store.clone(), Duration::from_millis(10));
        let mut stream = service
            .stream_events(Request::new(StreamEventsRequest {
                credential_id: "cred_i64".to_string(),
                since_seq: 0,
                ..Default::default()
            }))
            .await
            .expect("stream opens")
            .into_inner();
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the first event arrives")
            .expect("the stream is not closed")
            .expect("the first item is not an error");
        assert_eq!(event.event_id, "evt_i64_1");
        assert_eq!(
            event.ts_ms, WIDE_MS,
            "the delivered event timestamp must be the stored one, not its low 32 bits"
        );
        drop(stream);
        drop(service);
        cleanup(store, path).await;
    }

    /// **`u64 → i64` 的越界游标不得被静默翻译成"从头开始"。**
    ///
    /// `since_seq` 是 proto 的 `uint64`；`as i64` 在越过 `i64::MAX` 时回绕成
    /// 负数，`WHERE seq > <负数>` 会把**全部**历史事件重放一遍。
    /// 越界钉死在 `i64::MAX` 之后，方向变成"取不到新事件"（fail-closed）。
    #[tokio::test]
    async fn an_out_of_range_since_seq_replays_nothing_instead_of_everything() {
        let (store, path) = test_store("since-seq").await;
        sqlx::query(
            "INSERT INTO engine_order_events (credential_id, seq, event_id, ts_ms, event_type) \
             VALUES ('cred_i64', 1, 'evt_i64_x', ?, 'order_ack')",
        )
        .bind(WIDE_MS)
        .execute(store.pool())
        .await
        .expect("seed an event");

        let service = EngineEventsService::new(store.clone(), Duration::from_millis(10));
        let mut stream = service
            .stream_events(Request::new(StreamEventsRequest {
                credential_id: "cred_i64".to_string(),
                since_seq: u64::MAX,
                ..Default::default()
            }))
            .await
            .expect("stream opens")
            .into_inner();
        let first = tokio::time::timeout(Duration::from_millis(600), stream.next()).await;
        assert!(
            first.is_err(),
            "an out-of-range cursor must deliver nothing, not replay the whole history: {:?}",
            first.map(|item| item.map(|event| event.map(|event| event.event_id)))
        );
        drop(stream);
        drop(service);
        cleanup(store, path).await;
    }
}

/// Paper realized-PnL accounting（2026-09-20）：paper fill 的 realized PnL 真值回归。
///
/// 全部用例走生产入口符号：开仓经 `execute_intents` 携带 V2 执行上下文，
/// 平仓经裸 v1 wire（生产端口闭世界白名单放行 close_*）；账户真值经
/// `EngineExecutionV2Service::get_account_state`；模拟注资与预算经
/// `fund_simulation_account` / `put_risk_budget`。断言同时覆盖 RPC 响应与
/// SQLite 持久行（engine_fills / position_state / engine_order_events）。
#[cfg(test)]
mod paper_realized_pnl_tests {
    use std::path::PathBuf;

    use sqlx::Row;
    use tonic::Request;

    use super::{sha256_hex, EngineExecutionService, V2ExecutionContext, V2IntentContext};
    use crate::protection::OrderRole;
    use crate::proto::engine_execution_server::EngineExecution as V1EngineExecution;
    use crate::proto::{ExecuteIntentsRequest, IntentAck, OrderIntent};
    use crate::proto_v2::engine_execution_server::EngineExecution as V2EngineExecution;
    use crate::proto_v2::{
        FundSimulationAccountRequest, GetAccountStateRequest, GetAccountStateResponse,
        PutRiskBudgetRequest,
    };
    use crate::store::Store;
    use crate::svc_v2::EngineExecutionV2Service;

    const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";

    fn paper_intent(
        action: &str,
        price: &str,
        quantity: &str,
        cycle_id: &str,
        idem_key: &str,
        bot_uid: &str,
    ) -> OrderIntent {
        OrderIntent {
            bot_uid: bot_uid.to_string(),
            run_id: format!("run_{bot_uid}"),
            cycle_id: cycle_id.to_string(),
            idempotency_key: idem_key.to_string(),
            client_order_id_short: format!("c_{cycle_id}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: action.to_string(),
            side: if action.ends_with("short") {
                "short".to_string()
            } else {
                "long".to_string()
            },
            order_type: "market".to_string(),
            price: price.to_string(),
            quantity: quantity.to_string(),
            reduce_only: action.starts_with("close"),
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        }
    }

    /// v2 开仓（与生产 v2 wire 同一入口）：typed protection 以入场价为基准取
    /// 多空各自 RR=2 的 SL/TP，满足工厂策略 min_rr。
    async fn paper_open(
        service: &EngineExecutionService,
        credential_id: &str,
        action: &str,
        price: &str,
        bot_uid: &str,
        idem_key: &str,
    ) -> IntentAck {
        let short = action.ends_with("short");
        let entry: f64 = price.parse().expect("test entry price parses");
        let (stop_loss_price, take_profit_price) = if short {
            ((entry + 5.0).to_string(), (entry - 10.0).to_string())
        } else {
            ((entry - 5.0).to_string(), (entry + 10.0).to_string())
        };
        let intent = paper_intent(action, price, "2", "cycle_open", idem_key, bot_uid);
        let mut contexts = std::collections::HashMap::new();
        contexts.insert(
            intent.idempotency_key.clone(),
            V2IntentContext {
                risk: crate::risk::WireRiskContext {
                    typed_protection_capable: true,
                    data_snapshot_capable: true,
                    data_snapshot: Some(risk_policy::DataSnapshot {
                        allow_open: true,
                        consistency_error: None,
                    }),
                    intent_id: format!("int_{idem_key}"),
                    protection: Some(crate::risk::TypedProtectionInput {
                        stop_loss_price,
                        take_profit_price,
                    }),
                    valid_until_ms: None,
                    max_slippage_bps: None,
                    config_hash: String::new(),
                    prompt_hash: String::new(),
                    policy_version: String::new(),
                    code_commit: String::new(),
                },
                order_role: OrderRole::Entry,
                protection_required: false,
                confirm_timeout_ms: crate::protection::DEFAULT_CONFIRM_TIMEOUT_MS,
                protection_generation: 1,
            },
        );
        let mut request = Request::new(ExecuteIntentsRequest {
            credential_id: credential_id.to_string(),
            run_mode: "paper".to_string(),
            intents: vec![intent],
        });
        request
            .extensions_mut()
            .insert(V2ExecutionContext { intents: contexts });
        service
            .execute_intents(request)
            .await
            .expect("paper v2 open executes")
            .into_inner()
            .acks
            .into_iter()
            .next()
            .expect("one open ack")
    }

    /// 裸 v1 平仓（生产端口白名单放行的两个 action 之一）。
    async fn paper_close(
        service: &EngineExecutionService,
        credential_id: &str,
        action: &str,
        price: &str,
        quantity: &str,
        bot_uid: &str,
        idem_key: &str,
    ) -> IntentAck {
        let intent = paper_intent(action, price, quantity, "cycle_close", idem_key, bot_uid);
        service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.to_string(),
                run_mode: "paper".to_string(),
                intents: vec![intent],
            }))
            .await
            .expect("paper v1 close executes")
            .into_inner()
            .acks
            .into_iter()
            .next()
            .expect("one close ack")
    }

    /// 建库 + 生产入口注资 + 生产入口预算发布。credential 由 label 确定性派生
    /// （32 位小写 hex），每用例独立 SQLite 库。
    async fn funded_paper_fixture(label: &str) -> (Store, PathBuf, String) {
        static INSTALL_DRIVERS: std::sync::Once = std::sync::Once::new();
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let credential_id = format!("paper:{}", &sha256_hex(label)[..32]);
        let path = std::env::temp_dir().join(format!(
            "rustcore-paper-pnl-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect paper realized PnL fixture store");

        let funding = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(FundSimulationAccountRequest {
                credential_id: credential_id.clone(),
                amount_usdt: "10000".to_string(),
                idempotency_identity: format!("paper-pnl-fund:{credential_id}"),
                schema_version: 1,
            }))
            .await
            .expect("simulation funding via production RPC")
            .into_inner();
        assert!(funding.applied, "initial funding must apply once");

        let budget = EngineExecutionV2Service::new(store.clone())
            .put_risk_budget(Request::new(PutRiskBudgetRequest {
                credential_id: credential_id.clone(),
                as_of_ms: super::now_ms(),
                equity_usdt: Some("10000".to_string()),
                available_usdt: Some("10000".to_string()),
                today_realized_pnl_usdt: Some("0".to_string()),
                today_realized_pnl_pct: Some(0.0),
                drawdown_pct: Some(0.0),
                total_exposure_pct: Some(0.0),
                open_positions_count: Some(0),
                group_exposure_json: Some(r#"{"majors":0,"l1_alts":0}"#.to_string()),
                kill_switch_level: Some("none".to_string()),
                kill_switch_scope: None,
                kill_switch_reason: None,
                remaining_credential_notional_usdt: Some("5000".to_string()),
                schema_version: 1,
            }))
            .await
            .expect("risk budget via production RPC")
            .into_inner();
        assert!(budget.applied, "fresh budget must apply");
        (store, path, credential_id)
    }

    async fn paper_account(store: &Store, credential_id: &str) -> GetAccountStateResponse {
        EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_id.to_string(),
                bot_uid: String::new(),
                schema_version: 1,
            }))
            .await
            .expect("paper account state via production RPC")
            .into_inner()
    }

    async fn fill_rows(
        store: &Store,
        credential_id: &str,
    ) -> Vec<(String, String, Option<String>)> {
        sqlx::query(
            "SELECT fill_qty, fill_price, realized_pnl FROM engine_fills \
             WHERE credential_id=? ORDER BY ts_exchange_ms, exchange_trade_id",
        )
        .bind(credential_id)
        .fetch_all(store.pool())
        .await
        .expect("load engine_fills rows")
        .into_iter()
        .map(|row| {
            let realized = row
                .try_get::<Option<String>, _>("realized_pnl")
                .ok()
                .flatten();
            (row.get("fill_qty"), row.get("fill_price"), realized)
        })
        .collect()
    }

    async fn open_position_truth(store: &Store, credential_id: &str) -> (String, String, String) {
        let row = sqlx::query(
            "SELECT entry_price, qty, direction FROM position_state \
             WHERE credential_id=? AND state <> 'closed'",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("one open paper position");
        (
            row.get::<String, _>("entry_price"),
            row.get::<String, _>("qty"),
            row.get::<String, _>("direction"),
        )
    }

    /// 测试侧独立精确十进制 oracle：与实现不共享代码，用 i128 手算期望值。
    fn oracle_dec(value: &str) -> (i128, u32) {
        let value = value.trim();
        let (negative, unsigned) = value
            .strip_prefix('-')
            .map(|rest| (true, rest))
            .unwrap_or((false, value));
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let units: i128 = format!("{integer}{fraction}")
            .parse()
            .expect("oracle decimal digits parse");
        (if negative { -units } else { units }, fraction.len() as u32)
    }

    fn oracle_dec_text(units: i128, scale: u32) -> String {
        if units == 0 {
            return "0".to_string();
        }
        let negative = units < 0;
        let magnitude = units.unsigned_abs().to_string();
        let mut rendered = if scale == 0 {
            magnitude
        } else if magnitude.len() <= scale as usize {
            format!(
                "0.{}{}",
                "0".repeat(scale as usize - magnitude.len()),
                magnitude
            )
        } else {
            let split = magnitude.len() - scale as usize;
            format!("{}.{}", &magnitude[..split], &magnitude[split..])
        };
        if rendered.contains('.') {
            while rendered.ends_with('0') {
                rendered.pop();
            }
            if rendered.ends_with('.') {
                rendered.pop();
            }
        }
        if negative {
            rendered.insert(0, '-');
        }
        rendered
    }

    fn oracle_realized(entry: &str, exit: &str, qty: &str, direction: &str) -> String {
        let (entry_units, entry_scale) = oracle_dec(entry);
        let (exit_units, exit_scale) = oracle_dec(exit);
        let (qty_units, qty_scale) = oracle_dec(qty);
        let scale = entry_scale.max(exit_scale);
        let entry_scaled = entry_units * 10i128.pow(scale - entry_scale);
        let exit_scaled = exit_units * 10i128.pow(scale - exit_scale);
        let diff = if direction == "short" {
            entry_scaled - exit_scaled
        } else {
            exit_scaled - entry_scaled
        };
        oracle_dec_text(diff * qty_units, scale + qty_scale)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn paper_entry_fill_reports_exact_zero_realized_and_account_stays_available() {
        let (store, path, credential_id) = funded_paper_fixture("entry-zero-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let ack = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_zero",
        )
        .await;
        assert_eq!(ack.status, "accepted");
        drop(service);

        let fills = fill_rows(&store, &credential_id).await;
        // A protected paper entry produces one entry fill. Arming SL/TP
        // creates no executions and therefore no realized PnL.
        assert_eq!(
            fills.len(),
            1,
            "only the entry fills; protection is armed: {fills:?}"
        );
        assert_eq!(fills[0].1, "100");
        assert!(fills.iter().all(|fill| fill.2.as_deref() == Some("0")));
        let protection_orders = sqlx::query(
            "SELECT eo.status AS engine_status, po.status AS protection_status \
             FROM engine_orders eo JOIN protection_orders po \
             ON po.engine_order_id=eo.engine_order_id \
             WHERE eo.credential_id=? ORDER BY po.kind",
        )
        .bind(&credential_id)
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(
            protection_orders.len(),
            2,
            "SL and TP must both remain armed"
        );
        for order in protection_orders {
            assert_eq!(order.get::<String, _>("engine_status"), "acked");
            assert_eq!(order.get::<String, _>("protection_status"), "confirmed");
        }
        let position = sqlx::query("SELECT state, qty FROM position_state WHERE credential_id=?")
            .bind(&credential_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(position.get::<String, _>("state"), "protected");
        assert!(oracle_dec(&position.get::<String, _>("qty")).0 > 0);
        let fill_events = sqlx::query(
            "SELECT payload_json FROM engine_order_events \
             WHERE credential_id=? AND event_type='fill'",
        )
        .bind(&credential_id)
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(
            fill_events.len(),
            1,
            "arming protection must never report an execution"
        );
        let payload: serde_json::Value =
            serde_json::from_slice(&fill_events[0].get::<Vec<u8>, _>("payload_json")).unwrap();
        assert_eq!(payload["orderRole"], "entry");

        // A held position is valued from accepted market truth: record one
        // fresh closed bar at the entry price through the production ingest
        // path, so the account read proves availability under real marks.
        record_fresh_closed_bar(&store, &credential_id, "100", 1_000).await;
        let account = paper_account(&store, &credential_id).await;
        assert!(
            account.available,
            "entry-only paper account must stay available: {}",
            account.unavailable_reason
        );
        assert_eq!(account.today_realized_pnl_usdt.as_deref(), Some("0"));
        assert_eq!(account.open_positions_count, Some(1));
        let (_, qty, _) = open_position_truth(&store, &credential_id).await;
        let (_, notional) = entry_sizing_truth(&store, &credential_id).await;
        // Mark equals entry: unrealized is exactly zero, equity is the wallet
        // (funding, nothing realized yet), and the entry notional is the used
        // margin held against that wallet.
        assert_eq!(account.equity_usdt.as_deref(), Some("10000"));
        assert_eq!(
            account.available_usdt.as_deref(),
            Some(oracle_sub("10000", &notional).as_str())
        );
        assert_eq!(account.used_margin_usdt.as_deref(), Some(notional.as_str()));
        let position = &account.positions[0];
        assert_eq!(position.qty, qty);
        assert_eq!(position.unrealized_pnl_usdt, "0");
        assert_eq!(position.liquidation_price, None);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_long_close_realized_pnl_from_durable_truth_flows_to_account_state() {
        let (store, path, credential_id) = funded_paper_fixture("long-profit-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_long",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "long");
        assert_eq!(entry_price, "100");

        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "110",
            "",
            "bot_paper_pnl",
            "idem_paper_close_long",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        let expected = oracle_realized(&entry_price, "110", &qty, "long");
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2, "entry + close fills: {fills:?}");
        assert!(
            fills[0..1]
                .iter()
                .all(|fill| fill.2.as_deref() == Some("0")),
            "entry realizes zero: {fills:?}"
        );
        assert_eq!(
            fills[1].2.as_deref(),
            Some(expected.as_str()),
            "exit fill realized must derive from durable entry/qty truth"
        );

        // The fill audit event must carry the same truth, not a hardcoded value.
        let event = sqlx::query(
            "SELECT payload_json FROM engine_order_events \
             WHERE credential_id=? AND event_type='fill' ORDER BY seq",
        )
        .bind(&credential_id)
        .fetch_all(store.pool())
        .await
        .expect("load fill events");
        let exit_payload: serde_json::Value = serde_json::from_slice(
            &event
                .last()
                .expect("entry and close fill events")
                .get::<Vec<u8>, _>("payload_json"),
        )
        .expect("decode exit fill event payload");
        assert_eq!(
            exit_payload["realizedPnl"].as_str(),
            Some(expected.as_str())
        );

        let account = paper_account(&store, &credential_id).await;
        assert!(
            account.available,
            "closed paper account must stay available: {}",
            account.unavailable_reason
        );
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_short_close_realized_pnl_uses_entry_minus_exit() {
        let (store, path, credential_id) = funded_paper_fixture("short-profit-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_short",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_short",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "short");

        let close = paper_close(
            &service,
            &credential_id,
            "close_short",
            "90",
            "",
            "bot_paper_pnl",
            "idem_paper_close_short",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        // short 100 -> 90 realizes (entry - exit) * qty = +10 * qty
        let expected = oracle_realized(&entry_price, "90", &qty, "short");
        assert!(
            !expected.starts_with('-') && expected != "0",
            "short profit must be positive: {expected}"
        );
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[1].2.as_deref(), Some(expected.as_str()));
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_long_loss_close_keeps_negative_exact_sign() {
        let (store, path, credential_id) = funded_paper_fixture("long-loss-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_loss",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, _) = open_position_truth(&store, &credential_id).await;

        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "95",
            "",
            "bot_paper_pnl",
            "idem_paper_close_loss",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        let expected = oracle_realized(&entry_price, "95", &qty, "long");
        assert!(
            expected.starts_with('-'),
            "loss must be negative: {expected}"
        );
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[1].2.as_deref(), Some(expected.as_str()));
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_realized_pnl_uses_exact_decimal_arithmetic_not_float() {
        let (store, path, credential_id) = funded_paper_fixture("exact-decimal-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100.1",
            "bot_paper_pnl",
            "idem_paper_open_exact",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, _) = open_position_truth(&store, &credential_id).await;
        assert_eq!(entry_price, "100.1");

        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "100.2",
            "",
            "bot_paper_pnl",
            "idem_paper_close_exact",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        // 100.2 - 100.1 is exactly 0.1 in decimal but 0.10000000000000853 in
        // binary floating point; the fill truth must be the decimal result.
        let expected = oracle_realized(&entry_price, "100.2", &qty, "long");
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2);
        assert_eq!(
            fills[1].2.as_deref(),
            Some(expected.as_str()),
            "realized PnL must be exact decimal text"
        );
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_partial_close_quantity_mismatch_keeps_realized_unknown() {
        let (store, path, credential_id) = funded_paper_fixture("partial-mismatch-unknown").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_partial",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, _) = open_position_truth(&store, &credential_id).await;
        let (qty_units, qty_scale) = oracle_dec(&qty);
        assert!(qty_units > 1, "fixture needs a qty > 1 to halve: {qty}");
        let half = oracle_dec_text(qty_units / 2, qty_scale);

        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "110",
            &half,
            "bot_paper_pnl",
            "idem_paper_close_partial",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);
        let _ = (entry_price, &qty);

        // A partial close has no defined paper settlement truth: the fill row
        // must keep realized_pnl unknown and the account must fail closed.
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2);
        assert!(
            fills[0..1]
                .iter()
                .all(|fill| fill.2.as_deref() == Some("0")),
            "entry realizes zero: {fills:?}"
        );
        assert_eq!(
            fills[1].2, None,
            "partial close realized PnL must stay unknown, not default to zero"
        );
        let account = paper_account(&store, &credential_id).await;
        assert!(!account.available);
        assert!(
            account.unavailable_reason.contains("no realized PnL truth"),
            "unknown rejection reason must survive: {}",
            account.unavailable_reason
        );
        assert!(account.today_realized_pnl_usdt.is_none());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_close_retry_is_idempotent_without_double_realized() {
        let (store, path, credential_id) = funded_paper_fixture("retry-idempotent-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_retry",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, _) = open_position_truth(&store, &credential_id).await;

        // Duplicate entry retry while the position is still open must be the
        // durable idempotent rejection: no extra fill rows, no extra orders.
        let open_retry = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_retry",
        )
        .await;
        assert_eq!(open_retry.status, "rejected");
        assert!(
            open_retry.message.contains("DUPLICATE_ACTIVE_ENTRY"),
            "duplicate entry retry must cite the durable conflict: {}",
            open_retry.message
        );

        let first = paper_close(
            &service,
            &credential_id,
            "close_long",
            "110",
            "",
            "bot_paper_pnl",
            "idem_paper_close_retry",
        )
        .await;
        assert_eq!(first.status, "accepted");

        // Retrying a completed close on a simulation credential has no active
        // position and no exchange truth to recover from, so it must fail
        // closed (pre-existing paper semantics) — and the failure must leave
        // no fill, order or position rows behind.
        let retry_error = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.clone(),
                run_mode: "paper".to_string(),
                intents: vec![paper_intent(
                    "close_long",
                    "110",
                    "",
                    "cycle_close",
                    "idem_paper_close_retry",
                    "bot_paper_pnl",
                )],
            }))
            .await
            .expect_err("completed-close retry on paper must fail closed");
        assert_eq!(retry_error.code(), tonic::Code::FailedPrecondition);
        drop(service);

        let expected = oracle_realized(&entry_price, "110", &qty, "long");
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(
            fills.len(),
            2,
            "entry + close; retries must not add fill rows: {fills:?}"
        );
        assert_eq!(fills[1].2.as_deref(), Some(expected.as_str()));

        // A fresh service instance over the same durable store proves the
        // truth survives process restart without re-counting.
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_close_without_durable_entry_sizing_rolls_back_fill_atomically() {
        let (store, path, credential_id) = funded_paper_fixture("failed-close-atomic").await;
        // Seed a durable open position whose entry left no engine_orders row,
        // so the paper close's notional release fails inside the order
        // transaction. The fill row and its audit event must roll back with it.
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
              qty, entry_price, opened_at_ms, closed_at_ms,
              last_reconciled_ms, updated_at_ms
            ) VALUES (
              'pos_t7_atomic', ?, 'bot_t7_atomic',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'open',
              'bot_t7_atomic|binance|futures|BTC|USDT|USDT|long',
              NULL, NULL, '2', '100', 1, NULL, NULL, 1
            )
            "#,
        )
        .bind(&credential_id)
        .execute(store.pool())
        .await
        .expect("seed open position without entry order");

        let service = EngineExecutionService::new(store.clone());
        let error = service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.clone(),
                run_mode: "paper".to_string(),
                intents: vec![paper_intent(
                    "close_long",
                    "110",
                    "",
                    "cycle_t7",
                    "idem_t7_close",
                    "bot_t7_atomic",
                )],
            }))
            .await
            .expect_err("close without durable entry sizing must fail");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        drop(service);

        let fills = fill_rows(&store, &credential_id).await;
        assert!(
            fills.is_empty(),
            "failed close transaction must leave no fill rows: {fills:?}"
        );
        let events = sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events WHERE credential_id=? AND event_type='fill'",
        )
        .bind(&credential_id)
        .fetch_one(store.pool())
        .await
        .expect("count fill events");
        assert_eq!(
            events.get::<i64, _>("n"),
            0,
            "failed close transaction must leave no fill audit events"
        );
        let state =
            sqlx::query("SELECT state FROM position_state WHERE position_id='pos_t7_atomic'")
                .fetch_one(store.pool())
                .await
                .expect("seeded position survives rollback");
        assert_eq!(state.get::<String, _>("state"), "open");
        cleanup(store, path).await;
    }

    const EXIT_FILL_FAULT_TRIGGER: &str = "fable_pnl_fault_exit_fill_insert";
    const EXIT_FILL_FAULT_MARKER: &str = "fable_pnl_fault injected exit fill insert failure";

    /// 全库真值快照：逐表（`sqlite_master` 里的全部用户表）把每一行渲染成
    /// `quote(col)|...` 文本并排序。不挑表，所以订单、持仓、notional、verdict、
    /// 审计事件、保护单里任何一处漏出事务的写入都会让前后快照不等。
    async fn durable_truth_snapshot(
        store: &Store,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        let tables = sqlx::query(
            "SELECT name FROM sqlite_master \
             WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(store.pool())
        .await
        .expect("list fixture tables");
        let mut snapshot = std::collections::BTreeMap::new();
        for table in tables {
            let table: String = table.get("name");
            let columns = sqlx::query("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                .bind(&table)
                .fetch_all(store.pool())
                .await
                .expect("list fixture table columns")
                .into_iter()
                .map(|row| format!("quote(\"{}\")", row.get::<String, _>("name")))
                .collect::<Vec<_>>();
            assert!(!columns.is_empty(), "table {table} must expose columns");
            let rows = sqlx::query(&format!(
                "SELECT {} AS rendered FROM \"{table}\" ORDER BY 1",
                columns.join(" || '|' || ")
            ))
            .fetch_all(store.pool())
            .await
            .expect("render fixture table rows")
            .into_iter()
            .map(|row| row.get::<String, _>("rendered"))
            .collect::<Vec<_>>();
            snapshot.insert(table, rows);
        }
        snapshot
    }

    async fn count_rows(store: &Store, sql: &str, credential_id: &str) -> i64 {
        sqlx::query(sql)
            .bind(credential_id)
            .fetch_one(store.pool())
            .await
            .expect("count fixture rows")
            .get::<i64, _>("n")
    }

    async fn reserved_notional(store: &Store, credential_id: &str) -> String {
        sqlx::query("SELECT reserved_usdt FROM notional_reservations WHERE credential_id=?")
            .bind(credential_id)
            .fetch_one(store.pool())
            .await
            .expect("one notional reservation row")
            .get::<String, _>("reserved_usdt")
    }

    /// 实际边界（不多不少）：
    /// - 钉住的是 paper 平仓路径上 `insert_fill_if_new` 返回 `Err` 时，该错误
    ///   必须使整个下单事务失败并回滚——RPC 非成功、无新增 fill 行、无 FILLED
    ///   审计事件、订单/持仓/notional/verdict 全库真值与失败前逐行一致。
    /// - 故障是本用例私有 SQLite 库上的真实 `BEFORE INSERT` 触发器，只对
    ///   `order_role='exit'` 的订单的 fill 行 `RAISE(ABORT)`；不经过任何生产
    ///   fault 开关，也不制造 unknown/归零。`RAISE(ABORT)` 只回滚**该语句**，
    ///   事务本身仍然可提交——所以回滚只能来自生产代码对该错误的传播。
    /// - 抵达性：错误文本必须同时带 `insert_fill_if_new` 专属前缀与触发器
    ///   标记；移除触发器后**同一请求**重试被接受并落出精确真值，证明此前
    ///   挡住它的只有这条插入故障，而不是更早的前置阻断。
    /// - 不覆盖：MySQL 方言、entry/保护单 fill 的插入失败、交易所回报路径
    ///   （user stream / REST 回补）的 `insert_fill_if_new` 调用点、提交阶段
    ///   失败。开仓经生产 `ExecuteIntents`，没有手工播种任何真值行。
    #[tokio::test]
    async fn paper_close_fill_insert_failure_rolls_back_whole_order_transaction() {
        let (store, path, credential_id) =
            funded_paper_fixture("fable-fault-exit-fill-insert").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_fault",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "long");

        let fills_before = fill_rows(&store, &credential_id).await;
        assert_eq!(
            fills_before.len(),
            1,
            "only the entry fills; protection is armed: {fills_before:?}"
        );
        let orders_sql = "SELECT COUNT(*) AS n FROM engine_orders WHERE credential_id=?";
        let fill_events_sql = "SELECT COUNT(*) AS n FROM engine_order_events \
                               WHERE credential_id=? AND event_type='fill'";
        let orders_before = count_rows(&store, orders_sql, &credential_id).await;
        let fill_events_before = count_rows(&store, fill_events_sql, &credential_id).await;
        assert_eq!(fill_events_before, 1);
        let reserved_before = reserved_notional(&store, &credential_id).await;
        assert!(
            oracle_dec(&reserved_before).0 > 0,
            "the open must hold a durable notional reservation: {reserved_before}"
        );
        // The held position needs accepted market truth for its valuation.
        record_fresh_closed_bar(&store, &credential_id, "100", 1_000).await;
        let account_before = paper_account(&store, &credential_id).await;
        assert!(
            account_before.available,
            "{}",
            account_before.unavailable_reason
        );
        assert_eq!(account_before.today_realized_pnl_usdt.as_deref(), Some("0"));
        // Taken after the account read so a projection side effect, if any,
        // cannot be mistaken for a leak of the failed close.
        let snapshot_before = durable_truth_snapshot(&store).await;
        assert_eq!(snapshot_before["engine_fills"].len(), fills_before.len());
        assert_eq!(snapshot_before["engine_orders"].len() as i64, orders_before);

        sqlx::query(&format!(
            "CREATE TRIGGER {EXIT_FILL_FAULT_TRIGGER} BEFORE INSERT ON engine_fills \
             FOR EACH ROW WHEN (SELECT order_role FROM engine_orders \
                                WHERE engine_order_id = NEW.engine_order_id) = 'exit' \
             BEGIN SELECT RAISE(ABORT, '{EXIT_FILL_FAULT_MARKER}'); END"
        ))
        .execute(store.pool())
        .await
        .expect("install exit fill insert fault trigger");

        let close_request = || {
            Request::new(ExecuteIntentsRequest {
                credential_id: credential_id.clone(),
                run_mode: "paper".to_string(),
                intents: vec![paper_intent(
                    "close_long",
                    "110",
                    "",
                    "cycle_close",
                    "idem_paper_close_fault",
                    "bot_paper_pnl",
                )],
            })
        };
        let error = service
            .execute_intents(close_request())
            .await
            .expect_err("a failed exit fill insert must fail the close RPC");
        assert_eq!(error.code(), tonic::Code::Internal);
        // Reachability: both halves are required. The prefix is emitted only by
        // `insert_fill_if_new`; the marker only by this test's trigger.
        assert!(
            error.message().contains("db insert engine_fills failed")
                && error.message().contains(EXIT_FILL_FAULT_MARKER),
            "the failure must come from the injected fill insert fault: {}",
            error.message()
        );

        let fills_after_failure = fill_rows(&store, &credential_id).await;
        assert_eq!(
            fills_after_failure, fills_before,
            "failed close must not add or alter fill rows"
        );
        assert_eq!(
            count_rows(&store, fill_events_sql, &credential_id).await,
            fill_events_before,
            "failed close must not leave a FILLED audit event"
        );
        assert_eq!(
            count_rows(&store, orders_sql, &credential_id).await,
            orders_before,
            "failed close must not leave an exit order row"
        );
        assert_eq!(
            open_position_truth(&store, &credential_id).await,
            (entry_price.clone(), qty.clone(), direction.clone()),
            "failed close must leave the position open with its entry truth"
        );
        assert_eq!(
            reserved_notional(&store, &credential_id).await,
            reserved_before,
            "failed close must not release the notional reservation"
        );
        let snapshot_after_failure = durable_truth_snapshot(&store).await;
        for (table, rows) in &snapshot_before {
            assert_eq!(
                snapshot_after_failure.get(table),
                Some(rows),
                "failed close leaked a durable write into {table}"
            );
        }
        assert_eq!(snapshot_after_failure.len(), snapshot_before.len());
        let account_after_failure = paper_account(&store, &credential_id).await;
        assert!(
            account_after_failure.available,
            "{}",
            account_after_failure.unavailable_reason
        );
        assert_eq!(
            (
                account_after_failure.equity_usdt.as_deref(),
                account_after_failure.available_usdt.as_deref(),
                account_after_failure.used_margin_usdt.as_deref(),
                account_after_failure.today_realized_pnl_usdt.as_deref(),
                account_after_failure.open_positions_count,
            ),
            (
                account_before.equity_usdt.as_deref(),
                account_before.available_usdt.as_deref(),
                account_before.used_margin_usdt.as_deref(),
                account_before.today_realized_pnl_usdt.as_deref(),
                account_before.open_positions_count,
            ),
            "account truth must equal the pre-failure truth"
        );

        sqlx::query(&format!("DROP TRIGGER {EXIT_FILL_FAULT_TRIGGER}"))
            .execute(store.pool())
            .await
            .expect("remove exit fill insert fault trigger");

        // The identical request now succeeds: nothing earlier than the fill
        // insert was blocking it, and the rollback left no idempotency residue.
        let retry = service
            .execute_intents(close_request())
            .await
            .expect("retry after the fault is removed executes")
            .into_inner()
            .acks
            .into_iter()
            .next()
            .expect("one retry ack");
        assert_eq!(retry.status, "accepted", "{}", retry.message);
        drop(service);

        let expected = oracle_realized(&entry_price, "110", &qty, "long");
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2, "exactly one close fill: {fills:?}");
        assert_eq!(fills[0..1], fills_before[..]);
        assert_eq!(fills[1].2.as_deref(), Some(expected.as_str()));
        assert_eq!(
            count_rows(&store, fill_events_sql, &credential_id).await,
            fill_events_before + 1,
            "exactly one FILLED audit event for the successful retry"
        );
        assert_eq!(
            count_rows(&store, orders_sql, &credential_id).await,
            orders_before + 1
        );
        assert_eq!(
            count_rows(
                &store,
                "SELECT COUNT(*) AS n FROM position_state \
                 WHERE credential_id=? AND state <> 'closed'",
                &credential_id,
            )
            .await,
            0,
            "the retried close must close the position"
        );
        assert_eq!(
            oracle_dec(&reserved_notional(&store, &credential_id).await).0,
            0,
            "the retried close must release the reservation exactly once"
        );
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    // Missing-price accounting（2026-09-20）：缺价 market close 的 realized 真值边界。
    //
    // Regression reproduction: ExecuteIntents
    // 按 price=100 开仓（entry 100 / qty 10，无任何 bar_closed 行情），随后
    // 真实 market 平仓 intent.price 为空时，close 被接受且 engine_fills 落出
    // fill_price=0 / realized=-1000，账户投影 available=true——「没有定价」的
    // 0 占位被当成了确定亏损。以下用例钉住修复后的边界：没有任何价格真值的
    // 平仓保持 realized 未知并让既有账户投影 fail-closed unavailable；有真实
    // 已记录行情的同类平仓仍精确计算；零/负持久入场价保持未知。开仓与平仓
    // 均走与 probe 相同的生产入口符号。

    /// 无价格、无行情的 market 平仓：风险减少的平仓照常执行，但 realized
    /// 必须保持 NULL（不是假亏损也不是假零），账户投影必须 fail-closed。
    #[tokio::test]
    async fn paper_market_close_without_price_or_market_data_keeps_realized_unknown() {
        let (store, path, credential_id) =
            funded_paper_fixture("market-close-no-price-unknown").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_noprice",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (_, qty, _) = open_position_truth(&store, &credential_id).await;

        // This fixture records no bar_closed market data, so the empty-price
        // market close has no price truth from any source.
        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "",
            "",
            "bot_paper_pnl",
            "idem_paper_close_noprice",
        )
        .await;
        assert_eq!(
            close.status, "accepted",
            "the risk-reducing close itself must keep executing"
        );
        drop(service);

        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2, "entry + close fills: {fills:?}");
        assert!(
            fills[0..1]
                .iter()
                .all(|fill| fill.2.as_deref() == Some("0")),
            "entry realizes zero: {fills:?}"
        );
        assert_eq!(
            fills[1].0, qty,
            "close fills the durable position quantity: {fills:?}"
        );
        // The historical "0" fill-price placeholder may survive in the row,
        // but it must not be converted into a definite loss or a fake zero.
        assert_eq!(
            fills[1].2, None,
            "close without any price truth must keep realized PnL unknown: {fills:?}"
        );

        // The fill audit event must carry the same unknown, not a number.
        let event = sqlx::query(
            "SELECT payload_json FROM engine_order_events \
             WHERE credential_id=? AND event_type='fill' ORDER BY seq",
        )
        .bind(&credential_id)
        .fetch_all(store.pool())
        .await
        .expect("load fill events");
        let exit_payload: serde_json::Value = serde_json::from_slice(
            &event
                .last()
                .expect("entry and close fill events")
                .get::<Vec<u8>, _>("payload_json"),
        )
        .expect("decode exit fill event payload");
        assert!(
            exit_payload["realizedPnl"].is_null(),
            "fill event realizedPnl must serialize unknown as null: {exit_payload}"
        );

        let account = paper_account(&store, &credential_id).await;
        assert!(
            !account.available,
            "a fill without realized truth must fail the account closed"
        );
        assert!(
            account.unavailable_reason.contains("no realized PnL truth"),
            "unknown rejection reason must survive: {}",
            account.unavailable_reason
        );
        assert!(account.today_realized_pnl_usdt.is_none());
        cleanup(store, path).await;
    }

    /// 同类 market 平仓，但存在真实已记录的 bar_closed 行情：最新 bar close
    /// 就是价格真值，realized 仍必须精确计算——证明修复不是把所有 market
    /// exit 一刀切为 unknown。
    #[tokio::test]
    async fn paper_market_close_with_recorded_bar_close_still_computes_exact_realized() {
        let (store, path, credential_id) =
            funded_paper_fixture("market-close-bar-close-realized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_pnl",
            "idem_paper_open_barclose",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry_price, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "long");

        // Record one real closed bar for this credential/symbol: the durable
        // market-data truth the production market-close path consumes
        // (latest_bar_close_price). The seq sits far above the checkpoint-
        // allocated sequence so it cannot collide with emitted events.
        sqlx::query(
            r#"
            INSERT INTO engine_order_events (
              credential_id, seq, event_id, ts_ms, event_type, symbol_key, payload_json
            ) VALUES (?, 900000001, ?, ?, 'bar_closed', ?, ?)
            "#,
        )
        .bind(&credential_id)
        .bind(format!("bar_{credential_id}"))
        .bind(super::now_ms())
        .bind(SYMBOL_KEY)
        .bind(br#"{"close":"110"}"#.to_vec())
        .execute(store.pool())
        .await
        .expect("record one closed bar as market-data truth");

        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "",
            "",
            "bot_paper_pnl",
            "idem_paper_close_barclose",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        let expected = oracle_realized(&entry_price, "110", &qty, "long");
        let fills = fill_rows(&store, &credential_id).await;
        assert_eq!(fills.len(), 2, "entry + close fills: {fills:?}");
        assert_eq!(
            fills[1].1, "110",
            "fill price must be the recorded bar close: {fills:?}"
        );
        assert_eq!(
            fills[1].2.as_deref(),
            Some(expected.as_str()),
            "market close with real recorded market data must still compute \
             exact realized PnL: {fills:?}"
        );
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(expected.as_str())
        );
        cleanup(store, path).await;
    }

    /// 零/负持久入场价不是有效金融价格：即便平仓价格真值可用，realized 也
    /// 必须保持 unknown，账户 fail-closed。平仓本身仍照常执行。
    #[tokio::test]
    async fn paper_zero_or_negative_durable_entry_price_keeps_realized_unknown() {
        async fn corrupt_entry_close_case(corrupt_entry: &str, label: &str) {
            let (store, path, credential_id) = funded_paper_fixture(label).await;
            let service = EngineExecutionService::new(store.clone());
            let open = paper_open(
                &service,
                &credential_id,
                "open_long",
                "100",
                "bot_paper_pnl",
                &format!("idem_paper_open_{label}"),
            )
            .await;
            assert_eq!(open.status, "accepted");

            // The corrupt durable truth this boundary is about: the persistent
            // position row carries a zero/negative entry price.
            sqlx::query(
                "UPDATE position_state SET entry_price=? \
                 WHERE credential_id=? AND state <> 'closed'",
            )
            .bind(corrupt_entry)
            .bind(&credential_id)
            .execute(store.pool())
            .await
            .expect("seed corrupt durable entry price");

            let close = paper_close(
                &service,
                &credential_id,
                "close_long",
                "110",
                "",
                "bot_paper_pnl",
                &format!("idem_paper_close_{label}"),
            )
            .await;
            assert_eq!(
                close.status, "accepted",
                "the risk-reducing close itself must keep executing"
            );
            drop(service);

            let fills = fill_rows(&store, &credential_id).await;
            assert_eq!(fills.len(), 2, "entry + close fills: {fills:?}");
            assert_eq!(
                fills[1].2, None,
                "corrupt durable entry price must keep realized PnL unknown: {fills:?}"
            );
            let account = paper_account(&store, &credential_id).await;
            assert!(
                !account.available,
                "a fill without realized truth must fail the account closed"
            );
            assert!(
                account.unavailable_reason.contains("no realized PnL truth"),
                "unknown rejection reason must survive: {}",
                account.unavailable_reason
            );
            assert!(account.today_realized_pnl_usdt.is_none());
            cleanup(store, path).await;
        }

        corrupt_entry_close_case("0", "zero-entry-unknown").await;
        corrupt_entry_close_case("-5", "negative-entry-unknown").await;
    }

    // Paper account（2026-09-20）：账户投影真值边界。
    //
    // 真实反例（paper-account-review/EVIDENCE-RESULT.json，cg-ops-01 运行）：
    // open 后 position_state=protected/qty=0.016666666667，GetAccountState 却
    // available=true、open_positions_count=0；两轮 close 各自精确 realized
    // +10.0000000002，equity/available 恒为 funding 常量 1000。开仓经生产
    // v2 ExecuteIntents、平仓经生产 v1 close 白名单、账户经生产
    // GetAccountState、行情经生产 kline ingest——没有手工 seed 两侧真值。

    /// 经生产 market ingest 记录一根刚收盘的 1m bar（accepted bar_closed）。
    async fn record_fresh_closed_bar(
        store: &Store,
        credential_id: &str,
        close: &str,
        close_ago_ms: i64,
    ) {
        let close_ms = super::now_ms() - close_ago_ms;
        let open_ms = close_ms - 60_000;
        let close_value: f64 = close.parse().expect("test close price parses");
        let high = close_value + 1.0;
        let low = close_value - 1.0;
        let frame = serde_json::json!({
            "e": "kline",
            "E": close_ms,
            "s": "BTCUSDT",
            "k": {
                "t": open_ms,
                "T": close_ms,
                "i": "1m",
                "x": true,
                "o": close,
                "h": high.to_string(),
                "l": low.to_string(),
                "c": close,
                "v": "12",
                "n": 7
            }
        })
        .to_string();
        let outcome =
            crate::market_data::handle_kline_text(store, credential_id, SYMBOL_KEY, "1m", &frame)
                .await;
        assert!(
            matches!(outcome, crate::market_data::KlineIngest::Recorded),
            "fresh closed bar must be accepted: {outcome:?}"
        );
    }

    fn oracle_mul(left: &str, right: &str) -> String {
        let (left_units, left_scale) = oracle_dec(left);
        let (right_units, right_scale) = oracle_dec(right);
        oracle_dec_text(left_units * right_units, left_scale + right_scale)
    }

    fn oracle_add(left: &str, right: &str) -> String {
        let (left_units, left_scale) = oracle_dec(left);
        let (right_units, right_scale) = oracle_dec(right);
        let scale = left_scale.max(right_scale);
        oracle_dec_text(
            left_units * 10i128.pow(scale - left_scale)
                + right_units * 10i128.pow(scale - right_scale),
            scale,
        )
    }

    fn oracle_sub(left: &str, right: &str) -> String {
        let (left_units, left_scale) = oracle_dec(left);
        let (right_units, right_scale) = oracle_dec(right);
        let scale = left_scale.max(right_scale);
        oracle_dec_text(
            left_units * 10i128.pow(scale - left_scale)
                - right_units * 10i128.pow(scale - right_scale),
            scale,
        )
    }

    fn oracle_unrealized(entry: &str, mark: &str, qty: &str, direction: &str) -> String {
        let diff = if direction == "short" {
            oracle_sub(entry, mark)
        } else {
            oracle_sub(mark, entry)
        };
        oracle_mul(&diff, qty)
    }

    /// 读取入场 verdict 的 sizing 真值（杠杆与名义）——期望值独立取自持久行。
    async fn entry_sizing_truth(store: &Store, credential_id: &str) -> (u32, String) {
        let row = sqlx::query(
            r#"
            SELECT rv.sizing_json
            FROM engine_orders eo
            JOIN risk_verdicts rv
              ON rv.credential_id=eo.credential_id AND rv.risk_verdict_id=eo.risk_verdict_id
            WHERE eo.credential_id=? AND eo.order_role='entry'
            ORDER BY eo.created_at_ms ASC
            LIMIT 1
            "#,
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("load entry sizing truth");
        let sizing: serde_json::Value = serde_json::from_str(&row.get::<String, _>("sizing_json"))
            .expect("decode entry sizing truth");
        (
            sizing["leverage"].as_u64().expect("leverage in sizing") as u32,
            sizing["final_notional_usdt"]
                .as_str()
                .expect("final_notional_usdt in sizing")
                .to_string(),
        )
    }

    /// 反例主链：open → hold/read → close → 第二轮，账户资金/持仓/保证金
    /// 必须反映真实 durable 真值；funding 常量不得冒充 equity，空
    /// engine_positions 不得冒充空仓。
    #[tokio::test]
    async fn paper_account_projection_reflects_open_hold_close_and_second_round() {
        let (store, path, credential_id) = funded_paper_fixture("account-projection-rounds").await;
        let service = EngineExecutionService::new(store.clone());

        // Round 1: open at 100, hold with a fresh 110 mark.
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_r1",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "long");
        let (leverage, notional) = entry_sizing_truth(&store, &credential_id).await;
        assert_eq!(leverage, 1, "paper market entries size at venue default 1x");
        record_fresh_closed_bar(&store, &credential_id, "110", 1_000).await;

        let held = paper_account(&store, &credential_id).await;
        assert!(
            held.available,
            "held paper account with fresh mark must be available: {}",
            held.unavailable_reason
        );
        assert_eq!(held.open_positions_count, Some(1), "held position is real");
        assert_eq!(held.positions.len(), 1);
        let position = &held.positions[0];
        assert_eq!(position.symbol_key, SYMBOL_KEY);
        assert_eq!(position.side, "long");
        assert_eq!(position.qty, qty);
        assert_eq!(position.entry_price, entry);
        assert_eq!(position.mark_price, "110");
        assert_eq!(
            position.liquidation_price, None,
            "paper has no provable liquidation price; absent stays absent"
        );
        assert_eq!(position.leverage, leverage);
        let expected_unrealized = oracle_unrealized(&entry, "110", &qty, "long");
        assert_eq!(position.unrealized_pnl_usdt, expected_unrealized);
        assert_eq!(held.today_realized_pnl_usdt.as_deref(), Some("0"));
        let expected_equity = oracle_add("10000", &expected_unrealized);
        assert_eq!(held.equity_usdt.as_deref(), Some(expected_equity.as_str()));
        let expected_available = oracle_sub(&expected_equity, &notional);
        assert_eq!(
            held.available_usdt.as_deref(),
            Some(expected_available.as_str())
        );
        assert_eq!(held.used_margin_usdt.as_deref(), Some(notional.as_str()));

        // Round 1 close at 110 realizes exactly qty*(110-entry).
        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "110",
            "",
            "bot_paper_acct",
            "idem_acct_close_r1",
        )
        .await;
        assert_eq!(close.status, "accepted");
        let realized_r1 = oracle_realized(&entry, "110", &qty, "long");

        let flat = paper_account(&store, &credential_id).await;
        assert!(flat.available, "{}", flat.unavailable_reason);
        assert_eq!(flat.open_positions_count, Some(0));
        assert!(flat.positions.is_empty());
        assert_eq!(
            flat.today_realized_pnl_usdt.as_deref(),
            Some(realized_r1.as_str())
        );
        let wallet_r1 = oracle_add("10000", &realized_r1);
        assert_eq!(flat.equity_usdt.as_deref(), Some(wallet_r1.as_str()));
        assert_eq!(flat.available_usdt.as_deref(), Some(wallet_r1.as_str()));
        assert_eq!(flat.used_margin_usdt.as_deref(), Some("0"));

        // Round 2 on the same account: wallet must accumulate, not reset.
        let open2 = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_r2",
        )
        .await;
        assert_eq!(open2.status, "accepted");
        let (entry2, qty2, _) = open_position_truth(&store, &credential_id).await;
        assert_eq!(entry2, "100");
        let close2 = paper_close(
            &service,
            &credential_id,
            "close_long",
            "105",
            "",
            "bot_paper_acct",
            "idem_acct_close_r2",
        )
        .await;
        assert_eq!(close2.status, "accepted");
        drop(service);

        let realized_r2 = oracle_realized(&entry2, "105", &qty2, "long");
        let final_wallet = oracle_add(&wallet_r1, &realized_r2);
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(account.open_positions_count, Some(0));
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some(oracle_add(&realized_r1, &realized_r2).as_str())
        );
        assert_eq!(account.equity_usdt.as_deref(), Some(final_wallet.as_str()));
        assert_eq!(
            account.available_usdt.as_deref(),
            Some(final_wallet.as_str())
        );
        cleanup(store, path).await;
    }

    /// 持仓期间没有任何已接受行情：mark 缺失不是 0，也不是 entry 价——
    /// 账户必须 fail-closed，而不是 available=true 配虚构数值。
    #[tokio::test]
    async fn paper_open_position_without_any_mark_truth_fails_account_closed() {
        let (store, path, credential_id) = funded_paper_fixture("account-no-mark-unknown").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_nomark",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);

        let account = paper_account(&store, &credential_id).await;
        assert!(
            !account.available,
            "an open position without mark truth must fail the account closed"
        );
        assert!(
            account.unavailable_reason.contains("mark"),
            "the missing valuation truth must be explicit: {}",
            account.unavailable_reason
        );
        assert!(account.equity_usdt.is_none());
        assert!(account.available_usdt.is_none());
        assert!(account.positions.is_empty());
        cleanup(store, path).await;
    }

    /// 陈旧行情不能用 DB 到达时间续命：closeTs 超过 60s 账户估值窗口后
    /// 不再是可用估值，账户 fail-closed（新鲜度谓词钉在 bar 自身的
    /// closeTs；30s 只属于开仓参考价，不用于账户估值）。
    #[tokio::test]
    async fn paper_stale_mark_close_ts_is_not_recycled_as_fresh_valuation() {
        let (store, path, credential_id) = funded_paper_fixture("account-stale-mark-unknown").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_stale",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);

        record_fresh_closed_bar(&store, &credential_id, "110", 65_000).await;
        let account = paper_account(&store, &credential_id).await;
        assert!(
            !account.available,
            "a 1m bar closed 65s ago is beyond the 60s account valuation window"
        );
        assert!(
            account.unavailable_reason.contains("mark"),
            "stale rejection reason must be explicit: {}",
            account.unavailable_reason
        );
        cleanup(store, path).await;
    }

    /// 45s 前收盘的 1m bar（>30s、<60s）是可用账户估值：30s 开仓参考窗口
    /// 不得误用到账户估值，否则 1m 流每分钟有一半不可用。响应 as_of 绑定
    /// 底层 closeTs，重复读取不随读时刻前进。
    #[tokio::test]
    async fn paper_mark_45s_old_is_valuable_and_as_of_binds_to_close_ts() {
        let (store, path, credential_id) = funded_paper_fixture("account-mark-45s-as-of").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_45s",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);

        record_fresh_closed_bar(&store, &credential_id, "105", 45_000).await;
        let observed_before_ms = super::now_ms();
        let first = paper_account(&store, &credential_id).await;
        let observed_after_ms = super::now_ms();
        assert!(
            first.available,
            "a 45s-old closed 1m bar is inside the account valuation window: {}",
            first.unavailable_reason
        );
        let observed_at_ms = first
            .observed_at_ms
            .expect("native observation must be present");
        assert!((observed_before_ms..=observed_after_ms).contains(&observed_at_ms));
        assert!(observed_at_ms - first.as_of_ms >= 45_000);
        assert_eq!(first.open_positions_count, Some(1));
        assert_eq!(first.positions[0].mark_price, "105");

        // as_of must equal the recorded bar's own closeTs, read back from the
        // durable event rather than approximated by wall clock arithmetic.
        let bar_row = sqlx::query(
            "SELECT payload_json FROM engine_order_events \
             WHERE credential_id=? AND event_type='bar_closed' \
             ORDER BY ts_ms DESC LIMIT 1",
        )
        .bind(&credential_id)
        .fetch_one(store.pool())
        .await
        .expect("load the recorded bar event");
        let bar_payload: serde_json::Value =
            serde_json::from_slice(&bar_row.get::<Vec<u8>, _>("payload_json"))
                .expect("decode bar payload");
        let recorded_close_ts = bar_payload["closeTs"].as_i64().expect("closeTs in payload");
        assert_eq!(
            first.as_of_ms, recorded_close_ts,
            "as_of must be the underlying bar closeTs, not the read time"
        );

        // Repeat read with the same bar: as_of must not advance with the
        // observation clock (a wall-clock stamp would refresh a dead stream).
        std::thread::sleep(std::time::Duration::from_millis(1_100));
        let second = paper_account(&store, &credential_id).await;
        assert!(second.available, "{}", second.unavailable_reason);
        assert_eq!(
            second.as_of_ms, first.as_of_ms,
            "repeat reads must keep the truth-bound as_of"
        );
        assert_eq!(second.equity_usdt, first.equity_usdt);
        assert!(second.observed_at_ms.unwrap() > observed_at_ms);
        // One-sided production RPC wire for the real Python gRPC/HTTP adapter
        // and TypeScript reader. No separately invented consumer-side values.
        if let Ok(path) = std::env::var("ACCOUNT_STATE_OBSERVATION_WIRE_VECTOR") {
            let bytes = prost::Message::encode_to_vec(&first);
            let vector = serde_json::json!({
                "producer": "paper_mark_45s_old_is_valuable_and_as_of_binds_to_close_ts",
                "credentialId": credential_id,
                "protobufHex": bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                "asOfMs": first.as_of_ms,
                "observedAtMs": observed_at_ms,
                "quantity": first.positions[0].qty,
            });
            std::fs::write(path, serde_json::to_string_pretty(&vector).unwrap() + "\n").unwrap();
        }
        cleanup(store, path).await;
    }

    /// 最新收盘 bar 早于持仓开仓是正常时序（1m bar 先收盘、随后开仓）：
    /// 未来开仓守卫与 age 用观察时钟，不得因此把合法新仓判为不可用。
    #[tokio::test]
    async fn paper_position_opened_after_bar_close_stays_available() {
        let (store, path, credential_id) = funded_paper_fixture("account-bar-before-entry").await;
        // The bar closes first; the position opens afterwards.
        record_fresh_closed_bar(&store, &credential_id, "100", 45_000).await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_after_bar",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);

        let account = paper_account(&store, &credential_id).await;
        assert!(
            account.available,
            "a position opened after its price bar closed is legitimate: {}",
            account.unavailable_reason
        );
        assert_eq!(account.open_positions_count, Some(1));
        assert!(account.positions[0].age_minutes >= 0.0);
        cleanup(store, path).await;
    }

    /// 估值非未来是严格谓词：一根 closeTs 落在未来的 bar 即使骗过了 ingest
    /// 的时钟偏差容忍（±60s 内可落库）也不得成为估值——否则账户会以未来的
    /// as_of 报 available=true。
    #[tokio::test]
    async fn paper_future_close_ts_bar_is_not_a_valuation_quote() {
        let (store, path, credential_id) = funded_paper_fixture("account-future-bar-quote").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_future",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);

        // A bar whose closeTs is 5s in the future: inside the ingest skew
        // tolerance (recorded by the production ingest path), beyond the
        // valuation non-future guarantee.
        let close_ms = super::now_ms() + 5_000;
        let open_ms = close_ms - 60_000;
        let frame = serde_json::json!({
            "e": "kline",
            "E": close_ms,
            "s": "BTCUSDT",
            "k": {
                "t": open_ms,
                "T": close_ms,
                "i": "1m",
                "x": true,
                "o": "100",
                "h": "106",
                "l": "99",
                "c": "105",
                "v": "12",
                "n": 7
            }
        })
        .to_string();
        let outcome =
            crate::market_data::handle_kline_text(&store, &credential_id, SYMBOL_KEY, "1m", &frame)
                .await;
        assert!(
            matches!(outcome, crate::market_data::KlineIngest::Recorded),
            "the future bar is inside the ingest skew tolerance: {outcome:?}"
        );

        let account = paper_account(&store, &credential_id).await;
        assert!(
            !account.available,
            "a future closeTs must not back a valuation or an as_of"
        );
        assert!(
            account.unavailable_reason.contains("mark"),
            "future-quote rejection must be explicit: {}",
            account.unavailable_reason
        );
        assert!(account.positions.is_empty());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn paper_account_money_includes_positions_hidden_by_bot_filter() {
        let (store, path, credential_id) = funded_paper_fixture("account-bot-money-scope").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_account_bot_money_scope",
        )
        .await;
        assert_eq!(open.status, "accepted");
        drop(service);
        record_fresh_closed_bar(&store, &credential_id, "110", 1_000).await;
        let own = paper_account(&store, &credential_id).await;
        assert!(own.available);
        assert_eq!(own.open_positions_count, Some(1));

        let hidden = EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_id.clone(),
                bot_uid: "bot_without_positions".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("read sibling bot account")
            .into_inner();
        assert!(hidden.available);
        assert!(hidden.positions.is_empty());
        assert_eq!(hidden.open_positions_count, Some(0));
        assert_eq!(hidden.equity_usdt.as_deref(), Some("10100"));
        assert_eq!(hidden.used_margin_usdt.as_deref(), Some("1000"));
        assert_eq!(hidden.available_usdt.as_deref(), Some("9100"));
        assert_eq!(hidden.as_of_ms, own.as_of_ms);

        // Filtering out the only position cannot turn missing valuation into
        // a known flat account or refresh the account observation timestamp.
        sqlx::query(
            "DELETE FROM engine_order_events WHERE credential_id=? AND event_type='bar_closed'",
        )
        .bind(&credential_id)
        .execute(store.pool())
        .await
        .expect("remove mark truth");
        let unknown = EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_id.clone(),
                bot_uid: "bot_without_positions".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("read unvalued sibling account")
            .into_inner();
        assert!(
            !unknown.available,
            "hidden position still requires true account valuation"
        );
        assert!(unknown.equity_usdt.is_none());
        cleanup(store, path).await;
    }

    /// 钱包=全历史经济 exit 收益；今日 realized 只算 UTC 日内。跨日收益
    /// 必须进钱包但不进 today；跨日的 unknown realized 让钱包整体 unknown
    /// ——不允许过了 UTC 日就"恢复已知"。
    #[tokio::test]
    async fn paper_wallet_spans_history_while_today_is_utc_bounded() {
        let (store, path, credential_id) = funded_paper_fixture("account-wallet-vs-today").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_hist",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry, qty, _) = open_position_truth(&store, &credential_id).await;
        let close = paper_close(
            &service,
            &credential_id,
            "close_long",
            "110",
            "",
            "bot_paper_acct",
            "idem_acct_close_hist",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        let realized = oracle_realized(&entry, "110", &qty, "long");
        // Move the real exit fill (and its audit identity) to before today's
        // UTC midnight: the fill itself was produced by the real chain; only
        // its exchange timestamp is moved to pin the day boundary.
        let yesterday_ms =
            crate::clock::utc_day_start_ms(super::now_ms()).saturating_sub(3_600_000);
        sqlx::query("UPDATE engine_fills SET ts_exchange_ms=? WHERE credential_id=?")
            .bind(yesterday_ms)
            .bind(&credential_id)
            .execute(store.pool())
            .await
            .expect("move fills across the UTC day boundary");

        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(
            account.today_realized_pnl_usdt.as_deref(),
            Some("0"),
            "a fill from before UTC midnight is not today's realized PnL"
        );
        assert_eq!(
            account.equity_usdt.as_deref(),
            Some(oracle_add("10000", &realized).as_str()),
            "all-history realized PnL stays in the wallet"
        );

        // An unknown realized on a past-day exit fill keeps the whole wallet
        // unknown; crossing UTC midnight must not "recover" a definite value.
        sqlx::query(
            "UPDATE engine_fills SET realized_pnl=NULL \
             WHERE credential_id=? AND realized_pnl IS NOT NULL AND realized_pnl <> '0'",
        )
        .bind(&credential_id)
        .execute(store.pool())
        .await
        .expect("clear the past-day exit realized truth");
        let unknown_day = paper_account(&store, &credential_id).await;
        assert!(
            !unknown_day.available,
            "an unknown past-day realized PnL must fail the wallet closed"
        );
        assert!(
            unknown_day
                .unavailable_reason
                .contains("no realized PnL truth"),
            "unknown rejection must be explicit: {}",
            unknown_day.unavailable_reason
        );
        assert!(unknown_day.equity_usdt.is_none());
        cleanup(store, path).await;
    }

    /// 空头持仓的未实现盈亏方向必须按 entry-mark 计算；funding-only 账户
    /// （无持仓）不需要任何行情即可给出已知钱包真值。
    #[tokio::test]
    async fn paper_short_position_unrealized_uses_entry_minus_mark() {
        let (store, path, credential_id) = funded_paper_fixture("account-short-unrealized").await;
        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_id,
            "open_short",
            "100",
            "bot_paper_acct",
            "idem_acct_open_short_mark",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry, qty, direction) = open_position_truth(&store, &credential_id).await;
        assert_eq!(direction, "short");
        drop(service);

        // Price falls to 90: a short gains (entry - mark) * qty.
        record_fresh_closed_bar(&store, &credential_id, "90", 1_000).await;
        let account = paper_account(&store, &credential_id).await;
        assert!(account.available, "{}", account.unavailable_reason);
        assert_eq!(account.open_positions_count, Some(1));
        let position = &account.positions[0];
        assert_eq!(position.side, "short");
        assert_eq!(position.mark_price, "90");
        let expected_unrealized = oracle_unrealized(&entry, "90", &qty, "short");
        assert!(!expected_unrealized.starts_with('-') && expected_unrealized != "0");
        assert_eq!(position.unrealized_pnl_usdt, expected_unrealized);
        assert_eq!(
            account.equity_usdt.as_deref(),
            Some(oracle_add("10000", &expected_unrealized).as_str())
        );
        cleanup(store, path).await;
    }

    /// 资金不跨账户：A 的 funding/持仓/收益不得泄入 B 的投影；B 自己的
    /// funding 仍是 B 的钱包全部。
    #[tokio::test]
    async fn paper_account_wallet_and_positions_never_cross_credentials() {
        let (store, path, credential_a) = funded_paper_fixture("account-isolation-a").await;
        let credential_b = format!("paper:{}", &sha256_hex("account-isolation-b")[..32]);
        {
            let funding = EngineExecutionV2Service::new(store.clone())
                .fund_simulation_account(Request::new(FundSimulationAccountRequest {
                    credential_id: credential_b.clone(),
                    amount_usdt: "250".to_string(),
                    idempotency_identity: "paper-account-fund:isolation-b".to_string(),
                    schema_version: 1,
                }))
                .await
                .expect("fund the second simulation credential")
                .into_inner();
            assert!(funding.applied);
            assert_eq!(funding.wallet_balance_usdt, "250");
        }

        let service = EngineExecutionService::new(store.clone());
        let open = paper_open(
            &service,
            &credential_a,
            "open_long",
            "100",
            "bot_paper_acct",
            "idem_acct_open_isolated",
        )
        .await;
        assert_eq!(open.status, "accepted");
        let (entry, qty, _) = open_position_truth(&store, &credential_a).await;
        let close = paper_close(
            &service,
            &credential_a,
            "close_long",
            "110",
            "",
            "bot_paper_acct",
            "idem_acct_close_isolated",
        )
        .await;
        assert_eq!(close.status, "accepted");
        drop(service);

        let realized = oracle_realized(&entry, "110", &qty, "long");

        let account_b = EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_b.clone(),
                bot_uid: String::new(),
                schema_version: 1,
            }))
            .await
            .expect("read the sibling account")
            .into_inner();
        assert!(account_b.available, "{}", account_b.unavailable_reason);
        assert_eq!(account_b.open_positions_count, Some(0));
        assert_eq!(account_b.equity_usdt.as_deref(), Some("250"));
        assert_eq!(account_b.available_usdt.as_deref(), Some("250"));
        assert_eq!(account_b.today_realized_pnl_usdt.as_deref(), Some("0"));

        let account_a = paper_account(&store, &credential_a).await;
        assert!(account_a.available, "{}", account_a.unavailable_reason);
        let wallet_a = oracle_add("10000", &realized);
        assert_eq!(account_a.equity_usdt.as_deref(), Some(wallet_a.as_str()));
        assert_ne!(
            account_a.equity_usdt.as_deref(),
            account_b.equity_usdt.as_deref()
        );
        cleanup(store, path).await;
    }
}
