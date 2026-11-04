//! v2 service boundary. Trading execution adapts once into the shared v1/v2
//! implementation in `svc`; management and recovery responses stay typed v2.

use std::collections::HashMap;
use std::pin::Pin;

use futures_core::Stream;
use sqlx::Row;
use tonic::{Request, Response, Status};

use crate::proto_v2;
use crate::proto_v2::{
    engine_events_server::EngineEvents, engine_execution_server::EngineExecution, AccountPosition,
    AdmitMemberResetRequest, AdmitMemberResetResponse, CancelOrderRequest, CancelOrderResponse,
    EngineEvent, EnsureMarketDataRequest, EnsureMarketDataResponse, ExecuteIntentsRequest,
    ExecuteIntentsResponse, FundSimulationAccountRequest, FundSimulationAccountResponse,
    GetAccountStateRequest, GetAccountStateResponse, GetKillSwitchStatesRequest,
    GetKillSwitchStatesResponse, GetOpenOrdersRequest, GetOpenOrdersResponse,
    GetOrderByClientOrderIdRequest, GetOrderByClientOrderIdResponse, GetOrderRequest,
    GetOrderResponse, GetPositionStatesRequest, GetPositionStatesResponse, OrphanPosition,
    PositionState, PutPromotionCertificationRequest, PutPromotionCertificationResponse,
    PutRiskBudgetRequest, PutRiskBudgetResponse, ReconcileNowRequest, ReconcileNowResponse,
    ReleasePendingRequest, ReleasePendingResponse, ResetKillSwitchRequest, ResetKillSwitchResponse,
    SetKillSwitchRequest, SetKillSwitchResponse, StreamEventsRequest,
};
use crate::rate_limiter::RequestClass;
use crate::store::{DbKind, Store};

#[path = "reconcile_budget.rs"]
pub(crate) mod reconcile_budget;

const SKELETON_MESSAGE: &str =
    "execution_engine.v2 trading RPC is unimplemented; only management RPCs are available";
const ACCOUNT_DERIVED_FIELDS: [&str; 4] = [
    "equity_usdt",
    "used_margin_usdt",
    "today_realized_pnl_usdt",
    "age_minutes",
];
const UNFUNDED_ACCOUNT_STATE_REASON: &str = "USDT balance truth is unavailable";
// The simulation projection derives every monetary field from durable
// simulation-owned truth; nothing in it is exchange-reported.
const SIMULATION_ACCOUNT_DERIVED_FIELDS: [&str; 7] = [
    "equity_usdt",
    "available_usdt",
    "used_margin_usdt",
    "today_realized_pnl_usdt",
    "unrealized_pnl_usdt",
    "mark_price",
    "age_minutes",
];
/// Valuation bound for open simulation positions. This mirrors the data-plane
/// account freshness gate (`QualityThresholds.account_freshness_ms` = 60s),
/// which a 1m closed-bar stream must satisfy continuously; the tighter 30s
/// bound belongs to the opening entry reference only and would make half of
/// every minute unavailable.
const SIMULATION_ACCOUNT_VALUATION_AGE_MS: i64 = crate::simulation_quote::MAX_AGE_MS;
/// Only the 1m closed-bar stream backs account valuation. A slower timeframe
/// closing inside the window would mask a dead 1m stream.
const SIMULATION_VALUATION_TIMEFRAME: &str = "1m";

fn reconcile_limiter_for_credential(
    credential_id: &str,
) -> Result<crate::rate_limiter::CredentialRateLimiter, Status> {
    let venue =
        crate::venue::venue_for_credential(credential_id).map_err(Status::failed_precondition)?;
    Ok(crate::rate_limiter::CredentialRateLimiter::shared(
        crate::rate_limiter::TokenBucketConfig::from_env(venue),
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExactDecimal {
    units: i128,
    scale: u32,
}

impl ExactDecimal {
    pub(crate) fn zero() -> Self {
        Self { units: 0, scale: 0 }
    }

    pub(crate) fn parse(value: &str, field: &str) -> Result<Self, Status> {
        let value = value.trim();
        if value.is_empty() || value.starts_with('+') {
            return Err(Status::internal(format!(
                "stored {field} is not a decimal string"
            )));
        }
        let (negative, unsigned) = value
            .strip_prefix('-')
            .map(|rest| (true, rest))
            .unwrap_or((false, value));
        let mut parts = unsigned.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();
        if integer.is_empty()
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || fraction.is_some_and(|part| {
                part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit())
            })
            || parts.next().is_some()
        {
            return Err(Status::internal(format!(
                "stored {field} is not a decimal string"
            )));
        }
        let fraction = fraction.unwrap_or_default();
        let scale = u32::try_from(fraction.len())
            .map_err(|_| Status::internal(format!("stored {field} scale is too large")))?;
        let digits = format!("{integer}{fraction}");
        let magnitude = digits
            .parse::<i128>()
            .map_err(|_| Status::internal(format!("stored {field} is out of range")))?;
        Ok(Self {
            units: if negative { -magnitude } else { magnitude },
            scale,
        })
    }

    pub(crate) fn checked_add(self, other: Self, field: &str) -> Result<Self, Status> {
        let scale = self.scale.max(other.scale);
        let left = self.rescale(scale, field)?;
        let right = other.rescale(scale, field)?;
        Ok(Self {
            units: left
                .checked_add(right)
                .ok_or_else(|| Status::internal(format!("derived {field} is out of range")))?,
            scale,
        })
    }

    fn checked_sub(self, other: Self, field: &str) -> Result<Self, Status> {
        self.checked_add(
            Self {
                units: other
                    .units
                    .checked_neg()
                    .ok_or_else(|| Status::internal(format!("derived {field} is out of range")))?,
                scale: other.scale,
            },
            field,
        )
    }

    fn rescale(self, scale: u32, field: &str) -> Result<i128, Status> {
        let factor = 10i128
            .checked_pow(scale - self.scale)
            .ok_or_else(|| Status::internal(format!("derived {field} scale is too large")))?;
        self.units
            .checked_mul(factor)
            .ok_or_else(|| Status::internal(format!("derived {field} is out of range")))
    }

    fn checked_mul(self, other: Self, field: &str) -> Result<Self, Status> {
        Ok(Self {
            units: self
                .units
                .checked_mul(other.units)
                .ok_or_else(|| Status::internal(format!("derived {field} is out of range")))?,
            scale: self
                .scale
                .checked_add(other.scale)
                .ok_or_else(|| Status::internal(format!("derived {field} scale is too large")))?,
        })
    }

    /// Exact division by a positive integer multiplier (margin = notional /
    /// leverage). The quotient must stay a terminating decimal at a bounded
    /// scale; a non-terminating division is an explicit unknown, never a
    /// rounded or truncated money value.
    #[allow(
        clippy::explicit_counter_loop,
        clippy::absurd_extreme_comparisons,
        reason = "Preserve the established checked decimal arithmetic during packaging."
    )]
    fn checked_div_u32(self, divisor: u32, field: &str) -> Result<Self, Status> {
        let divisor = i128::from(divisor);
        if divisor < 1 {
            return Err(Status::internal(format!(
                "derived {field} divisor is invalid"
            )));
        }
        if divisor == 1 {
            return Ok(self);
        }
        let mut units = self.units;
        let mut scale = self.scale;
        for _ in 0..=18 {
            if scale >= u32::MAX {
                break;
            }
            let (quotient, remainder) = (units / divisor, units % divisor);
            if remainder == 0 {
                return Ok(Self {
                    units: quotient,
                    scale,
                });
            }
            let (scaled, overflow) = units.overflowing_mul(10);
            if overflow || scaled == i128::MIN {
                break;
            }
            units = scaled;
            scale += 1;
        }
        Err(Status::internal(format!(
            "derived {field} is not an exact decimal"
        )))
    }

    pub(crate) fn is_strictly_positive(self) -> bool {
        self.units > 0
    }

    fn is_zero(self) -> bool {
        self.units == 0
    }

    pub(crate) fn numerically_equals(self, other: Self, field: &str) -> Result<bool, Status> {
        Ok(self.checked_sub(other, field)?.is_zero())
    }

    fn abs_text(self) -> Result<String, Status> {
        Ok(Self {
            units: self
                .units
                .checked_abs()
                .ok_or_else(|| Status::internal("stored position_amt is out of range"))?,
            scale: self.scale,
        }
        .to_text())
    }

    pub(crate) fn to_text(self) -> String {
        if self.units == 0 {
            return "0".to_string();
        }
        let negative = self.units < 0;
        let magnitude = self.units.unsigned_abs().to_string();
        let mut rendered = if self.scale == 0 {
            magnitude
        } else {
            let scale = self.scale as usize;
            if magnitude.len() <= scale {
                format!("0.{}{}", "0".repeat(scale - magnitude.len()), magnitude)
            } else {
                let split = magnitude.len() - scale;
                format!("{}.{}", &magnitude[..split], &magnitude[split..])
            }
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

// Projection writers and this RPC share the exact accepted decimal grammar
// and range. A producer must not publish text that its consumer cannot parse.
pub(crate) fn validate_account_decimal(value: &str, field: &str) -> Result<(), Status> {
    ExactDecimal::parse(value, field).map(|_| ())
}

pub(crate) fn parse_account_leverage(value: &str) -> Result<u32, Status> {
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value >= 1)
        .ok_or_else(|| Status::internal("stored leverage is invalid"))
}

fn validate_simulation_funding_replay(
    stored_credential_id: &str,
    stored_amount_usdt: &str,
    requested_credential_id: &str,
    requested_amount: ExactDecimal,
) -> Result<(), Status> {
    if stored_credential_id != requested_credential_id {
        return Err(Status::already_exists(
            "idempotency_identity is already bound to another credential",
        ));
    }
    let stored_amount = ExactDecimal::parse(stored_amount_usdt, "simulation funding amount")?;
    if !stored_amount.numerically_equals(requested_amount, "simulation funding amount")? {
        return Err(Status::already_exists(
            "idempotency_identity is already bound to a different amount",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Simulation account projection (PAPER-ACCOUNT-PROJECTION-001).
//
// Paper execution writes position_state / engine_orders / engine_fills and
// never touches engine_balances / engine_positions, so an exchange-shaped
// account read reports the funding constant with zero positions forever.
// This projection rebuilds the account from simulation-owned durable truth
// only, inside one read-transaction snapshot:
//
// - funds        = simulation_account_funding_events (idempotent identities);
// - wallet       = funds + realized of every economic exit/emergency fill
//                  (entry and protection confirmations realize exactly zero);
// - today        = the same fills restricted to the UTC day;
// - positions    = non-zero non-closed position_state rows;
// - margin       = entry verdict final_notional_usdt / leverage;
// - mark         = freshest accepted bar_closed close (closeTs-bounded).
//
// Every unknown stays unknown: missing funding, unknown realized PnL,
// missing entry sizing, or a missing/stale mark fails the whole account
// closed with an explicit reason instead of synthesizing a number.
// ---------------------------------------------------------------------------

/// A projection read failed in a way the caller must surface as an
/// unavailable account (not as an RPC error).
#[derive(Debug)]
struct SimulationTruthUnavailable(String);

impl SimulationTruthUnavailable {
    fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }

    fn from_status(status: Status) -> Self {
        Self(status.message().to_string())
    }
}

struct SimulationPositionTruth {
    bot_uid: String,
    symbol_key: String,
    direction: String,
    qty: ExactDecimal,
    entry_price: ExactDecimal,
    opened_at_ms: i64,
    leverage: u32,
    used_margin: ExactDecimal,
    entry_notional: ExactDecimal,
}

struct SimulationAccountTruth {
    wallet: ExactDecimal,
    realized_today: ExactDecimal,
    positions: Vec<SimulationPositionTruth>,
}

/// One accepted bar_closed frame reduced to the facts a valuation may use.
struct SimulationMarkQuote {
    price: ExactDecimal,
    close_ts: i64,
    truth: proto_v2::MarkQuoteTruth,
}

/// Freshest accepted closed-bar close for this credential and symbol whose
/// own closeTs is inside the reference-quote age window. DB arrival order
/// only narrows the candidate set; freshness is judged on closeTs, so a
/// re-persisted old frame cannot extend a dead stream's life.
async fn simulation_mark_quote(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
    now_ms: i64,
) -> Result<Result<SimulationMarkQuote, SimulationTruthUnavailable>, Status> {
    if let Some(quote) = crate::simulation_quote::latest(tx, credential_id, symbol_key, now_ms)
        .await
        .map_err(|_| Status::internal("db load simulation current quote failed"))?
    {
        return Ok(match quote {
            Ok(truth) => Ok(SimulationMarkQuote {
                price: ExactDecimal::parse(&truth.raw_price, "quote_price")?,
                close_ts: truth.source_timestamp_ms,
                truth,
            }),
            Err(reason) => Err(SimulationTruthUnavailable::new(reason)),
        });
    }
    let rows = sqlx::query(
        r#"
        SELECT payload_json, CAST(ts_ms AS CHAR) AS received_at_ms_text
        FROM engine_order_events
        WHERE credential_id = ? AND event_type = 'bar_closed' AND symbol_key = ?
        ORDER BY ts_ms DESC
        LIMIT 32
        "#,
    )
    .bind(credential_id)
    .bind(symbol_key)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load simulation mark quotes failed: {error}")))?;

    let mut freshest: Option<SimulationMarkQuote> = None;
    for row in rows {
        let payload: Vec<u8> = row.get("payload_json");
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) else {
            continue;
        };
        // Only the 1m stream backs valuation; payload timeframe is the
        // per-frame identity the ingest bound at acceptance time.
        if value.get("timeframe").and_then(serde_json::Value::as_str)
            != Some(SIMULATION_VALUATION_TIMEFRAME)
        {
            continue;
        }
        let Some(close_ts) = value.get("closeTs").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        let Some(close_text) = value
            .get("close")
            .and_then(|close| {
                close
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| close.as_f64().map(|number| number.to_string()))
            })
            .filter(|text| ExactDecimal::parse(text, "bar close").is_ok())
        else {
            continue;
        };
        let Ok(price) = ExactDecimal::parse(&close_text, "bar close") else {
            continue;
        };
        // Valuation non-future is strict: a bar whose own closeTs is after
        // the observation time is not a usable quote here, even when it was
        // inside the ingest-side clock-skew tolerance. Acceptance tolerance
        // (market_data::MAX_BAR_CLOCK_SKEW_MS) governs ingestion only.
        if close_ts <= 0 || !price.is_strictly_positive() || close_ts > now_ms {
            continue;
        }
        if now_ms.saturating_sub(close_ts) > SIMULATION_ACCOUNT_VALUATION_AGE_MS {
            continue;
        }
        if freshest
            .as_ref()
            .is_none_or(|quote| quote.close_ts < close_ts)
        {
            freshest = Some(SimulationMarkQuote {
                price,
                close_ts,
                truth: proto_v2::MarkQuoteTruth {
                    source_kind: "accepted_closed_bar".into(),
                    source_fixture: String::new(),
                    run_mode: "paper".into(),
                    credential_id: credential_id.into(),
                    symbol_key: symbol_key.into(),
                    raw_price: close_text,
                    source_timestamp_ms: close_ts,
                    received_at_ms: crate::store::try_get_i64_text(&row, "received_at_ms_text")
                        .map_err(|_| Status::internal("stored bar receipt invalid"))?,
                    schema_version: 1,
                },
            });
        }
    }
    Ok(match freshest {
        Some(quote) => Ok(quote),
        None => Err(SimulationTruthUnavailable::new(format!(
            "no fresh mark truth for {symbol_key}"
        ))),
    })
}

/// Non-zero, non-closed durable positions with their entry sizing margin.
/// B3 close consumes the same accepted, source-time-bounded quote as paper
/// valuation (1m, positive exact decimal, non-future closeTs, at most 60s old).
/// The returned price is carried through the SAME OMS transaction to its fill.
pub(crate) async fn paper_close_price(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential: &str,
    symbol: &str,
    now: i64,
) -> Result<Option<String>, Status> {
    Ok(simulation_mark_quote(tx, credential, symbol, now)
        .await?
        .ok()
        .map(|quote| quote.price.to_text()))
}

async fn load_simulation_positions(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    bot_uid: &str,
) -> Result<Result<Vec<SimulationPositionTruth>, SimulationTruthUnavailable>, Status> {
    let rows = sqlx::query(
        r#"
        SELECT position_id, bot_uid, symbol_key, direction, state, qty,
               COALESCE(entry_price, '') AS entry_price_text,
               CAST(opened_at_ms AS CHAR) AS opened_at_ms_text
        FROM position_state
        WHERE credential_id = ? AND state <> 'closed' AND (? = '' OR bot_uid = ?)
        ORDER BY opened_at_ms, position_id
        "#,
    )
    .bind(credential_id)
    .bind(bot_uid)
    .bind(bot_uid)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Status::internal(format!("db load simulation position state failed: {error}"))
    })?;

    let mut positions = Vec::new();
    for row in rows {
        let position_id: String = row.get("position_id");
        let symbol_key: String = row.get("symbol_key");
        let direction: String = row.get("direction");
        let qty_text: String = row.get("qty");
        let entry_text: String = row.get("entry_price_text");
        let qty = match ExactDecimal::parse(&qty_text, "position qty") {
            Ok(value) => value,
            Err(error) => {
                return Ok(Err(SimulationTruthUnavailable::from_status(error)));
            }
        };
        if qty.is_zero() {
            continue;
        }
        let unavailable = |reason: String| SimulationTruthUnavailable::new(reason);
        if !qty.is_strictly_positive() {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} has a non-positive durable quantity"
            ))));
        }
        let entry_price = match ExactDecimal::parse(&entry_text, "position entry_price") {
            Ok(value) if value.is_strictly_positive() => value,
            _ => {
                return Ok(Err(unavailable(format!(
                    "paper position {position_id} has no positive entry price truth"
                ))))
            }
        };
        if direction != "long" && direction != "short" {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} has no recognizable direction"
            ))));
        }
        let opened_at_ms = match row.get::<String, _>("opened_at_ms_text").parse::<i64>() {
            Ok(value) if value > 0 => value,
            _ => {
                return Ok(Err(unavailable(format!(
                    "paper position {position_id} has no opened_at_ms truth"
                ))))
            }
        };

        let sizing_row = sqlx::query(
            r#"
            SELECT rv.sizing_json
            FROM engine_orders eo
            JOIN risk_verdicts rv
              ON rv.credential_id = eo.credential_id
             AND rv.risk_verdict_id = eo.risk_verdict_id
            WHERE eo.credential_id = ? AND eo.position_id = ? AND eo.order_role = 'entry'
            ORDER BY eo.created_at_ms ASC, eo.engine_order_id ASC
            LIMIT 1
            "#,
        )
        .bind(credential_id)
        .bind(&position_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Status::internal(format!("db load simulation entry sizing failed: {error}"))
        })?;
        let Some(sizing_row) = sizing_row else {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} has no durable entry sizing truth"
            ))));
        };
        let sizing_json =
            crate::store::try_get_text(&sizing_row, "sizing_json").map_err(|error| {
                Status::internal(format!("decode simulation entry sizing failed: {error}"))
            })?;
        let Some(sizing_json) = sizing_json else {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} has no durable entry sizing truth"
            ))));
        };
        let sizing: serde_json::Value = serde_json::from_str(&sizing_json).map_err(|_| {
            Status::internal(format!(
                "stored entry sizing for {position_id} is not valid JSON"
            ))
        })?;
        let Some(leverage) = sizing
            .get("leverage")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value >= 1)
        else {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} entry sizing has no usable leverage"
            ))));
        };
        let Some(notional_text) = sizing
            .get("final_notional_usdt")
            .and_then(serde_json::Value::as_str)
        else {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} entry sizing has no final_notional_usdt"
            ))));
        };
        let entry_notional = match ExactDecimal::parse(notional_text, "entry final_notional_usdt") {
            Ok(value) => value,
            Err(_) => {
                return Ok(Err(unavailable(format!(
                    "paper position {position_id} entry notional is not a decimal"
                ))))
            }
        };
        if !entry_notional.is_strictly_positive() {
            return Ok(Err(unavailable(format!(
                "paper position {position_id} entry notional is not positive"
            ))));
        }
        let sized_qty = sizing.get("final_qty").and_then(serde_json::Value::as_str);
        if let Some(sized_qty) = sized_qty {
            let sized_qty = match ExactDecimal::parse(sized_qty, "entry final_qty") {
                Ok(value) => value,
                Err(_) => {
                    return Ok(Err(unavailable(format!(
                        "paper position {position_id} entry sizing quantity is not a decimal"
                    ))))
                }
            };
            if !sized_qty.numerically_equals(qty, "entry sizing quantity")? {
                return Ok(Err(unavailable(format!(
                    "paper position {position_id} quantity does not match its entry sizing"
                ))));
            }
        }
        let used_margin = match entry_notional.checked_div_u32(leverage, "used_margin_usdt") {
            Ok(value) => value,
            Err(_) => {
                return Ok(Err(unavailable(format!(
                    "paper position {position_id} used margin is not an exact decimal"
                ))))
            }
        };

        positions.push(SimulationPositionTruth {
            bot_uid: row.get("bot_uid"),
            symbol_key,
            direction,
            qty,
            entry_price,
            opened_at_ms,
            leverage,
            used_margin,
            entry_notional,
        });
    }
    Ok(Ok(positions))
}

/// Funds plus realized PnL truth for the wallet. Any economic exit fill whose
/// realized PnL (or its exchange timestamp) is unknown makes the whole wallet
/// unknown — it must never silently "recover" a definite value later.
async fn load_simulation_wallet(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> Result<Result<(ExactDecimal, ExactDecimal), SimulationTruthUnavailable>, Status> {
    let funding_rows = sqlx::query(
        "SELECT amount_usdt FROM simulation_account_funding_events WHERE credential_id=?",
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Status::internal(format!("db load simulation funding events failed: {error}"))
    })?;
    if funding_rows.is_empty() {
        return Ok(Err(SimulationTruthUnavailable::new(
            "simulation funding truth is unavailable",
        )));
    }
    let mut funding_total = ExactDecimal::zero();
    for row in funding_rows {
        let amount = match ExactDecimal::parse(
            &row.get::<String, _>("amount_usdt"),
            "simulation funding amount",
        ) {
            Ok(value) => value,
            Err(error) => return Ok(Err(SimulationTruthUnavailable::from_status(error))),
        };
        if !amount.is_strictly_positive() {
            return Ok(Err(SimulationTruthUnavailable::new(
                "simulation funding event amount is not positive",
            )));
        }
        funding_total = funding_total.checked_add(amount, "simulation funding total")?;
    }

    let fill_rows = sqlx::query(
        r#"
        SELECT ef.exchange_trade_id, ef.realized_pnl, COALESCE(CAST(ef.ts_exchange_ms AS CHAR), '') AS ts_exchange_ms_text
        FROM engine_fills ef
        JOIN engine_orders eo
          ON eo.credential_id = ef.credential_id
         AND eo.engine_order_id = ef.engine_order_id
        WHERE ef.credential_id = ? AND eo.order_role IN ('exit','emergency')
        ORDER BY ef.ts_exchange_ms, ef.exchange_trade_id
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Status::internal(format!("db load simulation exit fills failed: {error}"))
    })?;

    let utc_day_start_ms = crate::clock::utc_day_start_ms(crate::svc::now_ms());
    let mut realized_total = ExactDecimal::zero();
    let mut realized_today = ExactDecimal::zero();
    for row in fill_rows {
        let trade_id: String = row.get("exchange_trade_id");
        let Some(realized_text) = row
            .try_get::<Option<String>, _>("realized_pnl")
            .ok()
            .flatten()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(Err(SimulationTruthUnavailable::new(format!(
                "economic exit fill {trade_id} has no realized PnL truth"
            ))));
        };
        let realized = match ExactDecimal::parse(&realized_text, "exit fill realized_pnl") {
            Ok(value) => value,
            Err(_) => {
                return Ok(Err(SimulationTruthUnavailable::new(format!(
                    "economic exit fill {trade_id} realized PnL is not a decimal"
                ))))
            }
        };
        realized_total = realized_total.checked_add(realized, "wallet_balance_usdt")?;
        let ts_text: String = row.get("ts_exchange_ms_text");
        let ts_exchange_ms = match ts_text.parse::<i64>() {
            Ok(value) => value,
            Err(_) => {
                return Ok(Err(SimulationTruthUnavailable::new(format!(
                    "economic exit fill {trade_id} has no exchange timestamp truth"
                ))))
            }
        };
        if ts_exchange_ms >= utc_day_start_ms {
            realized_today = realized_today.checked_add(realized, "today_realized_pnl_usdt")?;
        }
    }
    let wallet = funding_total.checked_add(realized_total, "wallet_balance_usdt")?;
    Ok(Ok((wallet, realized_today)))
}

/// Full projection backing GetAccountState for simulation credentials.
///
/// `as_of_ms` follows the underlying truth: with valued positions it is the
/// earliest original quote T / accepted closed-bar closeTs (a read must not refresh a dead
/// stream into looking fresh), while a known flat wallet uses the observation
/// time. Position age and the future-open guard always use the observation
/// clock, so a legitimate position whose price bar closed before entry stays
/// available.
async fn simulation_account_state(
    store: &Store,
    credential_id: &str,
    bot_uid: &str,
) -> Result<GetAccountStateResponse, Status> {
    let observed_at_ms = crate::svc::now_ms();
    let mut tx = store.pool().begin().await.map_err(|error| {
        Status::internal(format!("db begin simulation account read failed: {error}"))
    })?;
    // Account money includes every position owned by this credential. The
    // caller's bot filter applies only to the returned position list/count.
    let truth = match load_simulation_account_truth(&mut tx, credential_id, "").await? {
        Ok(truth) => truth,
        Err(unavailable) => {
            tx.rollback().await.map_err(|error| {
                Status::internal(format!(
                    "db rollback simulation account read failed: {error}"
                ))
            })?;
            return Ok(unavailable_account(unavailable.0));
        }
    };

    let mut positions = Vec::with_capacity(truth.positions.len());
    let mut unrealized_total = ExactDecimal::zero();
    let mut used_margin_total = ExactDecimal::zero();
    let mut earliest_mark_close_ts: Option<i64> = None;
    for position in truth.positions {
        let mark = match simulation_mark_quote(
            &mut tx,
            credential_id,
            &position.symbol_key,
            observed_at_ms,
        )
        .await?
        {
            Ok(quote) => quote,
            Err(unavailable) => {
                tx.rollback().await.map_err(|error| {
                    Status::internal(format!(
                        "db rollback simulation account read failed: {error}"
                    ))
                })?;
                return Ok(unavailable_account(unavailable.0));
            }
        };
        earliest_mark_close_ts = Some(match earliest_mark_close_ts {
            Some(current) => current.min(mark.close_ts),
            None => mark.close_ts,
        });
        let price_diff = if position.direction == "short" {
            position
                .entry_price
                .checked_sub(mark.price, "unrealized_pnl_usdt")?
        } else {
            mark.price
                .checked_sub(position.entry_price, "unrealized_pnl_usdt")?
        };
        let unrealized = price_diff.checked_mul(position.qty, "unrealized_pnl_usdt")?;
        unrealized_total = unrealized_total.checked_add(unrealized, "equity_usdt")?;
        used_margin_total =
            used_margin_total.checked_add(position.used_margin, "used_margin_usdt")?;
        if observed_at_ms < position.opened_at_ms {
            tx.rollback().await.map_err(|error| {
                Status::internal(format!(
                    "db rollback simulation account read failed: {error}"
                ))
            })?;
            return Ok(unavailable_account(format!(
                "paper position on {} has an invalid truth timestamp",
                position.symbol_key
            )));
        }
        if !bot_uid.is_empty() && position.bot_uid != bot_uid {
            continue;
        }
        positions.push(AccountPosition {
            symbol_key: position.symbol_key,
            side: position.direction,
            qty: position.qty.to_text(),
            entry_price: position.entry_price.to_text(),
            mark_price: mark.price.to_text(),
            leverage: position.leverage,
            unrealized_pnl_usdt: unrealized.to_text(),
            // The simulation has no provable liquidation model. Absent stays
            // absent; it is never "0" and never the entry price.
            liquidation_price: None,
            opened_at_ms: position.opened_at_ms,
            age_minutes: (observed_at_ms - position.opened_at_ms) as f64 / 60_000.0,
            mark_truth: Some(mark.truth),
            schema_version: 1,
        });
    }

    let equity = truth.wallet.checked_add(unrealized_total, "equity_usdt")?;
    let available = equity.checked_sub(used_margin_total, "available_usdt")?;
    tx.rollback().await.map_err(|error| {
        Status::internal(format!(
            "db rollback simulation account read failed: {error}"
        ))
    })?;

    Ok(GetAccountStateResponse {
        available: true,
        unavailable_reason: String::new(),
        as_of_ms: earliest_mark_close_ts.unwrap_or(observed_at_ms),
        observed_at_ms: Some(observed_at_ms),
        equity_usdt: Some(equity.to_text()),
        available_usdt: Some(available.to_text()),
        used_margin_usdt: Some(used_margin_total.to_text()),
        today_realized_pnl_usdt: Some(truth.realized_today.to_text()),
        open_positions_count: Some(positions.len() as u64),
        positions,
        derived_fields: SIMULATION_ACCOUNT_DERIVED_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        schema_version: 1,
    })
}

async fn load_simulation_account_truth(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    bot_uid: &str,
) -> Result<Result<SimulationAccountTruth, SimulationTruthUnavailable>, Status> {
    let (wallet, realized_today) = match load_simulation_wallet(tx, credential_id).await? {
        Ok(values) => values,
        Err(unavailable) => return Ok(Err(unavailable)),
    };
    let positions = match load_simulation_positions(tx, credential_id, bot_uid).await? {
        Ok(positions) => positions,
        Err(unavailable) => return Ok(Err(unavailable)),
    };
    Ok(Ok(SimulationAccountTruth {
        wallet,
        realized_today,
        positions,
    }))
}

/// Legacy risk-evaluator fallback facts for simulation credentials: the same
/// durable wallet/margin/exposure truth, without any mark precondition (the
/// legacy snapshot never valued unrealized PnL, and close/protection orders
/// must not gain an account-availability requirement). Any unknown fact maps
/// to `None`, which the existing require_risk_budget precondition turns into
/// a fail-closed open rejection.
pub(crate) struct SimulationLegacyFacts {
    pub(crate) equity_usdt: f64,
    pub(crate) available_margin_usdt: f64,
    pub(crate) total_exposure_usdt: f64,
}

pub(crate) async fn simulation_legacy_account_facts(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> Result<Option<SimulationLegacyFacts>, Status> {
    let Ok((wallet, _)) = load_simulation_wallet(tx, credential_id).await? else {
        return Ok(None);
    };
    let Ok(positions) = load_simulation_positions(tx, credential_id, "").await? else {
        return Ok(None);
    };
    let mut used_margin = ExactDecimal::zero();
    let mut exposure = ExactDecimal::zero();
    for position in &positions {
        used_margin = used_margin.checked_add(position.used_margin, "used_margin_usdt")?;
        exposure = exposure.checked_add(position.entry_notional, "total_exposure_usdt")?;
    }
    let available = wallet.checked_sub(used_margin, "available_usdt")?;
    let to_f64 = |value: ExactDecimal| value.to_text().parse::<f64>().unwrap_or(f64::NAN);
    Ok(Some(SimulationLegacyFacts {
        equity_usdt: to_f64(wallet),
        available_margin_usdt: to_f64(available),
        total_exposure_usdt: to_f64(exposure),
    }))
}

#[derive(Debug, PartialEq, Eq)]
struct PaperLedgerTruth {
    difference_count: i64,
    any_nonzero_position: bool,
}

async fn paper_ledger_truth(
    store: &Store,
    credential_id: &str,
) -> Result<PaperLedgerTruth, Status> {
    let position_rows = sqlx::query(
        "SELECT position_id, state, qty FROM position_state WHERE credential_id=? ORDER BY position_id",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .map_err(|error| Status::internal(format!("db load paper position state failed: {error}")))?;
    let fill_rows = sqlx::query(
        r#"
        SELECT eo.position_id, eo.order_role, ef.fill_qty
        FROM engine_orders eo
        JOIN engine_fills ef
          ON ef.credential_id=eo.credential_id
         AND ef.engine_order_id=eo.engine_order_id
        WHERE eo.credential_id=?
          AND eo.position_id IS NOT NULL
          AND eo.order_role IN ('entry','exit')
        ORDER BY eo.position_id, ef.exchange_trade_id
        "#,
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .map_err(|error| Status::internal(format!("db load paper fill ledger failed: {error}")))?;

    let mut ledger = HashMap::<String, ExactDecimal>::new();
    for row in fill_rows {
        let position_id = row.get::<String, _>("position_id");
        let role = row.get::<String, _>("order_role");
        let quantity_text = row
            .get::<Option<String>, _>("fill_qty")
            .ok_or_else(|| Status::internal("stored paper entry/exit fill_qty is missing"))?;
        let quantity = ExactDecimal::parse(&quantity_text, "paper fill_qty")?;
        if quantity.units <= 0 {
            return Err(Status::internal(
                "stored paper entry/exit fill_qty must be positive",
            ));
        }
        let current = ledger
            .get(&position_id)
            .copied()
            .unwrap_or_else(ExactDecimal::zero);
        let next = if role == "exit" {
            current.checked_sub(quantity, "paper fill ledger quantity")?
        } else {
            current.checked_add(quantity, "paper fill ledger quantity")?
        };
        ledger.insert(position_id, next);
    }

    let mut difference_count = 0i64;
    let mut any_nonzero_position = false;
    for row in position_rows {
        let position_id = row.get::<String, _>("position_id");
        let state = row.get::<String, _>("state");
        let quantity = ExactDecimal::parse(&row.get::<String, _>("qty"), "position qty")?;
        any_nonzero_position |= !quantity.is_zero();
        let net_fill = ledger
            .remove(&position_id)
            .unwrap_or_else(ExactDecimal::zero);
        let quantities_match = quantity.numerically_equals(net_fill, "paper ledger comparison")?;
        let state_matches = match state.as_str() {
            "pending" | "closed" => quantity.is_zero() && net_fill.is_zero(),
            "open" | "protected" => quantity.units > 0 && net_fill.units > 0,
            _ => false,
        };
        if !quantities_match || !state_matches {
            difference_count = difference_count.saturating_add(1);
        }
    }
    difference_count =
        difference_count.saturating_add(i64::try_from(ledger.len()).unwrap_or(i64::MAX));
    Ok(PaperLedgerTruth {
        difference_count,
        any_nonzero_position,
    })
}

/// 把一次 paper ledger 真值投影到本 credential 覆盖的 kill-switch scope。
///
/// scope 的取法与 `reconcile.rs` 的交易所分支同形（`global`、`cred:<id>`、
/// 本 credential 的各 `bot:<uid>`）；bot 一侧是 `kill_switch` 的 arm target
/// 映射（`position_state ∪ engine_orders`）按 credential 取反。成员资格、
/// 代际 CAS 与收敛聚合全部留在 `record_reconcile_for_scope` 里：这里不判断
/// 任何一个 scope 是否 clean，非本 revision 成员的 scope 一行都写不进去，
/// 多 credential 的 scope 缺一个成员就不收敛。
async fn project_paper_truth_to_kill_switch(
    store: &Store,
    credential_id: &str,
    truth: crate::kill_switch::ReconcileTruth,
    observed_generations: &crate::kill_switch::FlattenGenerations,
) -> Result<(), Status> {
    let bot_rows = sqlx::query(
        "SELECT bot_uid FROM position_state WHERE credential_id=? UNION SELECT bot_uid FROM engine_orders WHERE credential_id=?",
    )
    .bind(credential_id)
    .bind(credential_id)
    .fetch_all(store.pool())
    .await
    .map_err(|error| Status::internal(format!("db load paper bot scopes failed: {error}")))?;
    let mut scope_keys = vec!["global".to_string(), format!("cred:{credential_id}")];
    for row in bot_rows {
        let bot_uid = row.try_get::<String, _>("bot_uid").map_err(|error| {
            Status::internal(format!("db decode paper bot scope failed: {error}"))
        })?;
        scope_keys.push(format!("bot:{bot_uid}"));
    }
    let now_ms = crate::svc::now_ms();
    for scope_key in scope_keys {
        crate::kill_switch::record_reconcile_for_scope(
            store,
            &scope_key,
            credential_id,
            truth,
            now_ms,
            observed_generations,
        )
        .await
        .map_err(|error| {
            Status::internal(format!(
                "record paper reconcile truth for {scope_key} failed: {error}"
            ))
        })?;
    }
    Ok(())
}

/// 把**那一次**对账产出的 `ReconcileTruth` 整份投影成 v2 响应。
///
/// 这是 `reconcile_now` 唯一的出口：每一位都来自入参里的同一个 truth，
/// 函数内不再触碰库、交易所或时钟，因此"回给调用方的真值"与"这次对账
/// 观察到的真值"在结构上不可能是两份。
///
/// `all_open_orders_empty` 与 `all_open_algo_orders_empty` 在
/// `reconcile.rs` 里由**两份独立的交易所清单**（`open_orders` 与
/// `open_algo_orders`）各自算出，此处必须原样各带各的：把其中一位复用给
/// 另一位、或给算法单那位写死常量，都会让"常规挂单已清空"被读成"交易所
/// 侧已经没有任何挂单"，从而漏掉仍然挂着的止损/止盈条件单
/// （engine safety contract：保护单缺失或未经交易所真值确认按 High 处理）。
fn reconcile_now_response(truth: crate::kill_switch::ReconcileTruth) -> ReconcileNowResponse {
    ReconcileNowResponse {
        accepted: true,
        message: "reconcile completed".to_string(),
        completed: true,
        has_diff: truth.has_diff,
        all_positions_zero: truth.all_positions_zero,
        all_open_orders_empty: truth.all_open_orders_empty,
        all_open_algo_orders_empty: truth.all_open_algo_orders_empty,
        local_position_zero_diff: truth.local_position_zero_diff,
        schema_version: 1,
    }
}

fn unavailable_account(reason: impl Into<String>) -> GetAccountStateResponse {
    GetAccountStateResponse {
        available: false,
        unavailable_reason: reason.into(),
        as_of_ms: 0,
        observed_at_ms: None,
        equity_usdt: None,
        available_usdt: None,
        used_margin_usdt: None,
        today_realized_pnl_usdt: None,
        open_positions_count: None,
        positions: Vec::new(),
        derived_fields: ACCOUNT_DERIVED_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        schema_version: 1,
    }
}

fn required_decimal(value: Option<String>, field: &str) -> Result<String, Status> {
    let value = value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    let parsed = value
        .parse::<f64>()
        .map_err(|_| Status::invalid_argument(format!("{field} must be a decimal string")))?;
    if !parsed.is_finite() {
        return Err(Status::invalid_argument(format!("{field} must be finite")));
    }
    Ok(value)
}

fn required_finite(value: Option<f64>, field: &str) -> Result<f64, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    if !value.is_finite() {
        return Err(Status::invalid_argument(format!("{field} must be finite")));
    }
    Ok(value)
}

fn validate_group_exposure_json(value: Option<String>) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let groups: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&value)
        .map_err(|_| Status::invalid_argument("group_exposure_json must be a JSON object"))?;
    if groups.values().any(|value| {
        value
            .as_f64()
            .map(|number| !number.is_finite() || number < 0.0)
            .unwrap_or(true)
    }) {
        return Err(Status::invalid_argument(
            "group_exposure_json values must be finite non-negative numbers",
        ));
    }
    Ok(Some(value))
}

fn required_projection_text(value: String, field: &str, max_len: usize) -> Result<String, Status> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    if value.len() > max_len {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds {max_len} bytes"
        )));
    }
    Ok(value)
}

/// **sqlx-i64 核查批· 读一个毫秒 epoch 列的真值。**
///
/// 历史上的 sqlx 0.7.4 `Any` + SQLite 会在建行时把宽整数截成 `i32`。
/// 精确锁定的 0.8.6 已修复直接 `i64` 读取；现有 SQL 文本投影先作为纵深防御
/// 保留，后续与约 50 处同类投影一起单独机械清理。
/// 解析失败一律 `Status::internal`——v2 的这几个出口都是**持仓/订单真值**，
/// 宁可整次调用失败，也不把一个编造的时刻交给 control-plane。
fn position_ms(row: &sqlx::any::AnyRow, column: &str) -> Result<i64, Status> {
    crate::store::try_get_i64_text(row, column)
        .map_err(|error| Status::internal(format!("db decode {column} failed: {error}")))
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Clone)]
pub struct EngineExecutionV2Service {
    store: Store,
    shared_execution: crate::svc::EngineExecutionService,
}

impl EngineExecutionV2Service {
    #[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
    pub fn new(store: Store) -> Self {
        Self {
            shared_execution: crate::svc::EngineExecutionService::new(store.clone()),
            store,
        }
    }

    #[cfg(test)]
    fn with_protection_submit_failure(
        store: Store,
        adapter: std::sync::Arc<dyn crate::protection::ProtectionAdapter>,
    ) -> Self {
        Self {
            shared_execution: crate::svc::EngineExecutionService::new(store.clone())
                .with_protection_test_adapter(adapter, true),
            store,
        }
    }

    #[cfg(test)]
    fn with_protection_truth_adapter(
        store: Store,
        adapter: std::sync::Arc<dyn crate::protection::ProtectionAdapter>,
    ) -> Self {
        Self {
            shared_execution: crate::svc::EngineExecutionService::new(store.clone())
                .with_protection_test_adapter(adapter, false),
            store,
        }
    }

    /// **wiring proof 专用**：把 v2 服务架在一个**已经装好交易所替身**的
    /// v1 shared core 上。替身只换掉"交易所"这一端（内存 HTTP 队列 +
    /// 固定过滤器真值），v2 → v1 之间那两层规范化/映射一层都没绕。
    pub(crate) fn with_shared_execution(
        store: Store,
        shared_execution: crate::svc::EngineExecutionService,
    ) -> Self {
        Self {
            shared_execution,
            store,
        }
    }

    #[cfg(test)]
    fn with_execution_clock(store: Store, clock: std::sync::Arc<dyn crate::svc::Clock>) -> Self {
        Self {
            shared_execution: crate::svc::EngineExecutionService::new(store.clone())
                .with_clock(clock),
            store,
        }
    }
}

impl EngineExecutionV2Service {
    async fn reconcile_now_timed(
        &self,
        request: Request<ReconcileNowRequest>,
        timing: &mut crate::reconcile_trace::Timer,
    ) -> Result<Response<ReconcileNowResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.trim();
        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id is required"));
        }
        let mut paper_observed_generations = None;
        let truth = if crate::venue::is_simulation_credential(credential_id) {
            // 观察点必须早于 ledger 真值读取，与 `reconcile.rs` 的交易所分支同形：
            // 读真值期间重新 arm 的 latch 不认识这份真值，落库时由
            // `record_reconcile_for_scope` 的代际 CAS 丢弃。
            paper_observed_generations = Some(
                crate::kill_switch::observe_flatten_generations(&self.store)
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "observe kill-switch generations before paper truth failed: {error}"
                        ))
                    })?,
            );
            let ledger_truth = paper_ledger_truth(&self.store, credential_id).await?;
            let open_orders = sqlx::query(
                "SELECT COUNT(*) AS n FROM engine_orders WHERE credential_id=? AND status IN ('pending_risk','pending_submit','submitted','acked','partial_filled','unknown')",
            )
            .bind(credential_id)
            .fetch_one(self.store.pool())
            .await
            .map_err(|error| Status::internal(format!("db load paper orders failed: {error}")))?
            .get::<i64, _>("n");
            crate::kill_switch::ReconcileTruth {
                has_diff: ledger_truth.difference_count != 0,
                all_positions_zero: !ledger_truth.any_nonzero_position,
                all_open_orders_empty: open_orders == 0,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: ledger_truth.difference_count == 0,
            }
        } else {
            let keys = crate::binance_futures::keys_from_env(credential_id)
                .map_err(Status::failed_precondition)?
                .ok_or_else(|| {
                    Status::failed_precondition("missing exchange credentials for reconcile")
                })?;
            crate::reconcile::reconcile_binance_futures_truth(
                credential_id,
                self.store.clone(),
                reconcile_limiter_for_credential(credential_id)?,
                keys,
            )
            .await
            .map_err(|error| Status::unavailable(format!("reconcile failed: {error}")))?
        };
        timing.stage(crate::reconcile_trace::Stage::PlanLoad);
        let planned_positions = sqlx::query(
            r#"
            SELECT ps.position_id
            FROM position_state ps
            JOIN position_protection_plans pp ON pp.position_id=ps.position_id
            WHERE ps.credential_id=? AND ps.state IN ('open','protected')
            "#,
        )
        .bind(credential_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db load recovery protection plans failed: {error}"))
        })?;
        timing.stage(crate::reconcile_trace::Stage::Protection);
        for row in planned_positions {
            reconcile_budget::ensure_live()?;
            let position_id = row.get::<String, _>("position_id");
            let acks = self
                .shared_execution
                .reconcile_planned_protection(
                    &position_id,
                    if crate::venue::is_simulation_credential(credential_id) {
                        "paper"
                    } else {
                        crate::svc::protection_run_mode_for_credential(credential_id)
                    },
                )
                .await
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "recovery protection check failed for {position_id}: {error}"
                    ))
                })?;
            if acks
                .iter()
                .any(|ack| matches!(ack.status.as_str(), "error" | "rejected"))
            {
                return Err(Status::failed_precondition(format!(
                    "recovery protection repair failed for {position_id}"
                )));
            }
        }
        // 保护恢复全部成功之后才投影：上面任何一处早返回都不得留下一条
        // 可以让 `ResetKillSwitch` 放行的 post-arm 事实。
        if let Some(observed_generations) = paper_observed_generations.as_ref() {
            project_paper_truth_to_kill_switch(
                &self.store,
                credential_id,
                truth,
                observed_generations,
            )
            .await?;
        }
        Ok(Response::new(reconcile_now_response(truth)))
    }
}

#[tonic::async_trait]
impl EngineExecution for EngineExecutionV2Service {
    async fn get_funding_history(
        &self,
        request: Request<proto_v2::GetFundingHistoryRequest>,
    ) -> Result<Response<proto_v2::GetFundingHistoryResponse>, Status> {
        crate::funding::get_history(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }
    async fn get_runtime_diagnostics(
        &self,
        request: Request<proto_v2::GetRuntimeDiagnosticsRequest>,
    ) -> Result<Response<proto_v2::GetRuntimeDiagnosticsResponse>, Status> {
        crate::round2_diagnostics::read(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }
    async fn inject_user_stream_fault(
        &self,
        request: Request<proto_v2::InjectUserStreamFaultRequest>,
    ) -> Result<Response<proto_v2::InjectUserStreamFaultResponse>, Status> {
        crate::round2_diagnostics::inject(request.into_inner()).map(Response::new)
    }

    async fn get_position_close_target(
        &self,
        request: Request<proto_v2::GetPositionCloseTargetRequest>,
    ) -> Result<Response<proto_v2::GetPositionCloseTargetResponse>, Status> {
        crate::position_close::get_target(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn close_position(
        &self,
        request: Request<proto_v2::ClosePositionRequest>,
    ) -> Result<Response<proto_v2::ClosePositionResponse>, Status> {
        let request = request.into_inner();
        match crate::position_close::prepare(&self.store, &request).await? {
            // 既有请求的重放（WO-M1）：交易所请求经回执读取路径返回，与
            // GetPositionCloseReceipt 同一套真值落定；绝不重新派发。
            crate::position_close::Prepare::Receipt(receipt)
                if receipt.found
                    && crate::venue::credential_kind(&request.credential_id)
                        == crate::venue::CredentialKind::ExchangeBacked =>
            {
                crate::position_close::get_receipt(
                    &self.store,
                    proto_v2::GetPositionCloseReceiptRequest {
                        credential_id: request.credential_id,
                        request_ref: request.request_ref,
                        actor_member_id: request.actor_member_id,
                        schema_version: 1,
                    },
                )
                .await
                .map(Response::new)
            }
            crate::position_close::Prepare::Receipt(receipt) => Ok(Response::new(receipt)),
            crate::position_close::Prepare::Dispatch(context) => {
                #[cfg(feature = "l4-fault-injection")]
                crate::fault_injection::checkpoint("b3_after_prepare")
                    .map_err(|error| Status::internal(error.to_string()))?;
                crate::proto::engine_execution_server::EngineExecution::execute_intents(
                    &self.shared_execution,
                    context.execution_request(),
                )
                .await?;
                // 执行入口在取得派发权之前拒绝（零订单、零交易所请求）时，
                // 如实终结并释放 fence；已派发 / 已终结的请求不受影响。
                crate::position_close::reject_unclaimed(
                    &self.store,
                    &context,
                    "CLOSE_SUBMISSION_REFUSED",
                )
                .await?;
                crate::position_close::get_receipt(
                    &self.store,
                    proto_v2::GetPositionCloseReceiptRequest {
                        credential_id: request.credential_id,
                        request_ref: request.request_ref,
                        actor_member_id: request.actor_member_id,
                        schema_version: 1,
                    },
                )
                .await
                .map(Response::new)
            }
        }
    }

    async fn get_position_close_receipt(
        &self,
        request: Request<proto_v2::GetPositionCloseReceiptRequest>,
    ) -> Result<Response<proto_v2::ClosePositionResponse>, Status> {
        crate::position_close::get_receipt(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn execute_intents(
        &self,
        request: Request<ExecuteIntentsRequest>,
    ) -> Result<Response<ExecuteIntentsResponse>, Status> {
        let request = request.into_inner();
        let data_snapshot = crate::risk::map_v2_data_snapshot(request.data_snapshot.as_ref());
        let mut contexts = HashMap::new();
        let mut v1_intents = Vec::with_capacity(request.intents.len());
        let mut intent_ids = HashMap::new();

        for intent in request.intents {
            if intent.idempotency_key.trim().is_empty() || intent.intent_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "v2 intent_id and idempotency_key are required",
                ));
            }
            let role = match intent.action.as_str() {
                "open_long" | "open_short" => crate::protection::OrderRole::Entry,
                "close_long" | "close_short" => crate::protection::OrderRole::Exit,
                _ => crate::protection::OrderRole::Exit,
            };
            let protection =
                intent
                    .protection
                    .as_ref()
                    .map(|protection| crate::risk::TypedProtectionInput {
                        stop_loss_price: protection.stop_loss_price.clone(),
                        take_profit_price: protection.take_profit_price.clone(),
                    });
            let (protection_required, confirm_timeout_ms) = intent
                .protection
                .as_ref()
                .map(|protection| {
                    (
                        protection.required,
                        u64::from(protection.confirm_timeout_ms.max(1)),
                    )
                })
                .unwrap_or((false, crate::protection::DEFAULT_CONFIRM_TIMEOUT_MS));
            contexts.insert(
                intent.idempotency_key.clone(),
                crate::svc::V2IntentContext {
                    risk: crate::risk::WireRiskContext {
                        typed_protection_capable: true,
                        data_snapshot_capable: true,
                        data_snapshot: data_snapshot.clone(),
                        intent_id: intent.intent_id.clone(),
                        protection,
                        valid_until_ms: (intent.valid_until_ms > 0)
                            .then_some(intent.valid_until_ms),
                        max_slippage_bps: Some(intent.max_slippage_bps),
                        config_hash: intent.config_hash.clone(),
                        prompt_hash: intent.prompt_hash.clone(),
                        policy_version: intent.policy_version.clone(),
                        code_commit: intent.code_commit.clone(),
                    },
                    order_role: role,
                    protection_required,
                    confirm_timeout_ms,
                    protection_generation: 1,
                },
            );
            intent_ids.insert(intent.idempotency_key.clone(), intent.intent_id.clone());
            let side = match intent.action.as_str() {
                "open_short" | "close_short" => "short",
                _ => "long",
            };
            v1_intents.push(crate::proto::OrderIntent {
                bot_uid: intent.bot_uid,
                run_id: intent.run_id,
                cycle_id: intent.cycle_id,
                idempotency_key: intent.idempotency_key,
                client_order_id_short: intent.client_order_id_short,
                exchange: intent.exchange,
                market_kind: intent.market_kind,
                symbol_key: intent.symbol_key,
                action: intent.action,
                side: side.to_string(),
                order_type: intent.order_type,
                price: intent.limit_price,
                quantity: String::new(),
                reduce_only: intent.reduce_only,
                max_position_notional: 0.0,
                max_credential_notional: 0.0,
            });
        }

        let mut shared_request = Request::new(crate::proto::ExecuteIntentsRequest {
            credential_id: request.credential_id,
            run_mode: request.run_mode,
            intents: v1_intents,
        });
        shared_request
            .extensions_mut()
            .insert(crate::svc::V2ExecutionContext { intents: contexts });
        let shared = crate::proto::engine_execution_server::EngineExecution::execute_intents(
            &self.shared_execution,
            shared_request,
        )
        .await?
        .into_inner();

        let mut acks = Vec::with_capacity(shared.acks.len());
        for ack in shared.acks {
            let intent_id = intent_ids
                .get(&ack.idempotency_key)
                .cloned()
                .unwrap_or_default();
            let risk_verdict_id = if intent_id.is_empty() {
                String::new()
            } else {
                sqlx::query(
                    "SELECT risk_verdict_id FROM risk_verdicts WHERE intent_id=? ORDER BY created_at_ms DESC LIMIT 1",
                )
                .bind(&intent_id)
                .fetch_optional(self.store.pool())
                .await
                .map_err(|error| Status::internal(format!("db load v2 verdict failed: {error}")))?
                .map(|row| row.get::<String, _>("risk_verdict_id"))
                .unwrap_or_default()
            };
            let (status, reject_reason_code) = match ack.status.as_str() {
                "accepted" => (proto_v2::Status::Accepted as i32, String::new()),
                "duplicate" => (proto_v2::Status::Duplicate as i32, String::new()),
                "rejected" => (
                    proto_v2::Status::Rejected as i32,
                    ack.message
                        .strip_prefix("rejected: ")
                        .and_then(|value| value.split(';').next())
                        .unwrap_or_default()
                        .to_string(),
                ),
                _ => (proto_v2::Status::Error as i32, String::new()),
            };
            acks.push(proto_v2::IntentAck {
                idempotency_key: ack.idempotency_key,
                engine_order_id: ack.engine_order_id,
                message: ack.message,
                risk_verdict_id,
                status,
                reject_reason_code,
                schema_version: 1,
            });
        }
        Ok(Response::new(ExecuteIntentsResponse {
            acks,
            schema_version: 1,
        }))
    }

    async fn get_order(
        &self,
        _request: Request<GetOrderRequest>,
    ) -> Result<Response<GetOrderResponse>, Status> {
        Err(Status::unimplemented(SKELETON_MESSAGE))
    }

    async fn get_open_orders(
        &self,
        _request: Request<GetOpenOrdersRequest>,
    ) -> Result<Response<GetOpenOrdersResponse>, Status> {
        Err(Status::unimplemented(SKELETON_MESSAGE))
    }

    async fn arm_lifecycle_fence(
        &self,
        request: Request<proto_v2::ArmLifecycleFenceRequest>,
    ) -> Result<Response<proto_v2::LifecycleFenceResponse>, Status> {
        crate::lifecycle_fence::arm(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_lifecycle_fence(
        &self,
        request: Request<proto_v2::GetLifecycleFenceRequest>,
    ) -> Result<Response<proto_v2::LifecycleFenceResponse>, Status> {
        crate::lifecycle_fence::get(&self.store, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_account_order_state(
        &self,
        request: Request<proto_v2::GetAccountOrderStateRequest>,
    ) -> Result<Response<proto_v2::GetAccountOrderStateResponse>, Status> {
        crate::account_order_read::handle(&self.store, request)
            .await
            .map(Response::new)
    }

    async fn cancel_order(
        &self,
        _request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        Err(Status::unimplemented(SKELETON_MESSAGE))
    }

    async fn reconcile_now(
        &self,
        request: Request<ReconcileNowRequest>,
    ) -> Result<Response<ReconcileNowResponse>, Status> {
        let started = tokio::time::Instant::now();
        let deadline = reconcile_budget::from_metadata(started, request.metadata());
        crate::reconcile_trace::scope(async {
            let mut timing = crate::reconcile_trace::Timer::rpc();
            let result =
                reconcile_budget::run(deadline, self.reconcile_now_timed(request, &mut timing))
                    .await;
            timing.finish(result.is_ok());
            result
        })
        .await
    }

    async fn ensure_market_data(
        &self,
        _request: Request<EnsureMarketDataRequest>,
    ) -> Result<Response<EnsureMarketDataResponse>, Status> {
        Err(Status::unimplemented(SKELETON_MESSAGE))
    }

    async fn get_position_states(
        &self,
        request: Request<GetPositionStatesRequest>,
    ) -> Result<Response<GetPositionStatesResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.trim();
        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id is required"));
        }
        let bot_uid = request.bot_uid.trim();
        let rows = sqlx::query(
            r#"
            SELECT position_id, credential_id, bot_uid, symbol_key, direction,
                   entry_epoch, state, COALESCE(active_key, '') AS active_key,
                   COALESCE(pending_intent_id, '') AS pending_intent_id,
                   COALESCE(pending_idem_key, '') AS pending_idem_key,
                   qty, COALESCE(entry_price, '') AS entry_price,
                   COALESCE(CAST(opened_at_ms AS CHAR), '0') AS opened_at_ms_text,
                   COALESCE(CAST(closed_at_ms AS CHAR), '0') AS closed_at_ms_text,
                   COALESCE(CAST(last_reconciled_ms AS CHAR), '0') AS last_reconciled_ms_text,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text,
                   COALESCE((
                     SELECT eo.client_order_id_short FROM engine_orders eo
                     WHERE eo.position_id=position_state.position_id
                       AND eo.order_role='entry'
                     ORDER BY eo.created_at_ms DESC LIMIT 1
                   ), '') AS pending_client_order_id_short
            FROM position_state
            WHERE credential_id = ?
              AND (? = '' OR bot_uid = ?)
              AND (? = 1 OR HEX(state) <> HEX('closed'))
            ORDER BY bot_uid, symbol_key, direction, entry_epoch
            "#,
        )
        .bind(credential_id)
        .bind(bot_uid)
        .bind(bot_uid)
        .bind(if request.include_closed { 1i64 } else { 0i64 })
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db select position states failed: {error}")))?;

        // **sqlx-i64 核查批· 四条持仓时刻走文本取真值。**
        //
        // 旧驱动会把毫秒 epoch 截成低 32 位；0.8.6 已修复。文本投影暂留，
        // 继续保护 v2 持仓真值出口和跨库证据链的逐行一致性。
        let positions: Vec<PositionState> = rows
            .into_iter()
            .map(|row| {
                Ok::<_, Status>(PositionState {
                    position_id: row.get("position_id"),
                    credential_id: row.get("credential_id"),
                    bot_uid: row.get("bot_uid"),
                    symbol_key: row.get("symbol_key"),
                    direction: row.get("direction"),
                    entry_epoch: row.get("entry_epoch"),
                    state: row.get("state"),
                    active_key: row.get("active_key"),
                    pending_intent_id: row.get("pending_intent_id"),
                    pending_idem_key: row.get("pending_idem_key"),
                    qty: row.get("qty"),
                    entry_price: row.get("entry_price"),
                    opened_at_ms: position_ms(&row, "opened_at_ms_text")?,
                    closed_at_ms: position_ms(&row, "closed_at_ms_text")?,
                    last_reconciled_ms: position_ms(&row, "last_reconciled_ms_text")?,
                    updated_at_ms: position_ms(&row, "updated_at_ms_text")?,
                    pending_client_order_id_short: row.get("pending_client_order_id_short"),
                    schema_version: 1,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let active_keys = positions
            .iter()
            .filter(|position| position.state != "closed")
            .map(|position| (position.symbol_key.clone(), position.direction.clone()))
            .collect::<std::collections::HashSet<_>>();
        let exchange_rows = sqlx::query(
            r#"
            SELECT exchange, market_kind, symbol, symbol_key, position_amt,
                   entry_price, mark_price,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
            FROM engine_positions WHERE credential_id=?
            "#,
        )
        .bind(credential_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db select exchange positions failed: {error}"))
        })?;
        // **sqlx-i64 核查批**：孤儿持仓的 `observed_at_ms` 同样
        // 是被截断的毫秒 epoch；这里用 `map` 而不是 `filter_map`，让解码错误
        // 能上抛（孤儿持仓是"交易所有、本地没有"的高危真值，不能带着编造的
        // 观测时刻交出去，也不能因为解码失败被静默丢掉）。
        let orphan_positions = exchange_rows
            .into_iter()
            .map(|row| {
                let qty = row.get::<String, _>("position_amt");
                if crate::position_state::is_decimal_zero(&qty) {
                    return Ok(None);
                }
                let direction = if qty.trim().starts_with('-') {
                    "short"
                } else {
                    "long"
                };
                let symbol_key = row
                    .try_get::<Option<String>, _>("symbol_key")
                    .ok()
                    .flatten()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| {
                        format!("binance|futures|{}", row.get::<String, _>("symbol"))
                    });
                if active_keys.contains(&(symbol_key.clone(), direction.to_string())) {
                    return Ok(None);
                }
                Ok(Some(OrphanPosition {
                    credential_id: credential_id.to_string(),
                    exchange: row.get("exchange"),
                    market_kind: row.get("market_kind"),
                    symbol_key,
                    direction: direction.to_string(),
                    qty,
                    entry_price: row.get("entry_price"),
                    mark_price: row.get("mark_price"),
                    observed_at_ms: position_ms(&row, "updated_at_ms_text")?,
                    schema_version: 1,
                }))
            })
            .collect::<Result<Vec<_>, Status>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Response::new(GetPositionStatesResponse {
            positions,
            orphan_positions,
            schema_version: 1,
        }))
    }

    async fn get_account_state(
        &self,
        request: Request<GetAccountStateRequest>,
    ) -> Result<Response<GetAccountStateResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.trim();
        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id is required"));
        }
        let bot_uid = request.bot_uid.trim();
        // Simulation credentials have no exchange truth to project: their
        // account is rebuilt from simulation-owned durable facts (funding,
        // economic exit fills, position_state, entry sizing, accepted bars)
        // instead of the reconcile-maintained engine_balances/engine_positions
        // tables, which paper execution never writes.
        if crate::venue::is_simulation_credential(credential_id) {
            let response = simulation_account_state(&self.store, credential_id, bot_uid).await?;
            return Ok(Response::new(response));
        }
        let account_observed_at_ms = crate::svc::now_ms();

        let Some(balance) = sqlx::query(
            r#"
            SELECT wallet_balance, available_balance,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
            FROM engine_balances
            WHERE credential_id=? AND asset='USDT'
            LIMIT 1
            "#,
        )
        .bind(credential_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load USDT balance failed: {error}")))?
        else {
            // Missing reconcile truth is not a zero-balance account. Keep every
            // monetary optional absent so callers can fail the snapshot gate.
            return Ok(Response::new(unavailable_account(
                UNFUNDED_ACCOUNT_STATE_REASON,
            )));
        };

        let wallet_text = balance.get::<String, _>("wallet_balance");
        let available_text = balance.get::<String, _>("available_balance");
        let wallet = match ExactDecimal::parse(&wallet_text, "wallet_balance") {
            Ok(value) => value,
            Err(_) => {
                return Ok(Response::new(unavailable_account(
                    "USDT wallet balance truth is invalid",
                )))
            }
        };
        let available = match ExactDecimal::parse(&available_text, "available_balance") {
            Ok(value) => value,
            Err(_) => {
                return Ok(Response::new(unavailable_account(
                    "USDT available balance truth is invalid",
                )))
            }
        };
        let balance_as_of_ms = balance
            .get::<String, _>("updated_at_ms_text")
            .parse::<i64>()
            .map_err(|_| Status::internal("stored balance updated_at_ms is invalid"))?;

        let exchange_rows = sqlx::query(
            r#"
            SELECT symbol, COALESCE(symbol_key, '') AS symbol_key,
                   position_amt, entry_price, mark_price, leverage,
                   unrealized_pnl, liquidation_price,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text
            FROM engine_positions
            WHERE credential_id=?
            ORDER BY symbol
            "#,
        )
        .bind(credential_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load account positions failed: {error}")))?;

        let positions_as_of_ms = exchange_rows
            .iter()
            .map(|row| row.get::<String, _>("updated_at_ms_text").parse::<i64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Status::internal("stored position updated_at_ms is invalid"))?
            .into_iter()
            .min();
        let reconciled_as_of_ms = positions_as_of_ms
            .map(|position_ms| balance_as_of_ms.min(position_ms))
            .unwrap_or(balance_as_of_ms);
        let as_of_ms = if crate::venue::is_simulation_credential(credential_id) {
            account_observed_at_ms
        } else {
            reconciled_as_of_ms
        };

        let mut unrealized_total = ExactDecimal::zero();
        let mut positions = Vec::new();
        for row in exchange_rows {
            let qty_text = row.get::<String, _>("position_amt");
            let qty = match ExactDecimal::parse(&qty_text, "position_amt") {
                Ok(value) => value,
                Err(_) => {
                    return Ok(Response::new(unavailable_account(
                        "position quantity truth is invalid",
                    )))
                }
            };
            if qty.is_zero() {
                continue;
            }
            let unrealized_text = row.get::<String, _>("unrealized_pnl");
            let unrealized = match ExactDecimal::parse(&unrealized_text, "unrealized_pnl") {
                Ok(value) => value,
                Err(_) => {
                    return Ok(Response::new(unavailable_account(
                        "position unrealized PnL truth is invalid",
                    )))
                }
            };
            unrealized_total = unrealized_total.checked_add(unrealized, "equity_usdt")?;

            let direction = if qty.units < 0 { "short" } else { "long" };
            let symbol_key = row.get::<String, _>("symbol_key");
            if symbol_key.is_empty() {
                if bot_uid.is_empty() {
                    return Ok(Response::new(unavailable_account(format!(
                        "open exchange position {} has no durable position_state mapping",
                        row.get::<String, _>("symbol")
                    ))));
                }
                continue;
            }
            let state_rows = sqlx::query(
                r#"
                SELECT bot_uid, symbol_key, direction,
                       CAST(opened_at_ms AS CHAR) AS opened_at_ms_text
                FROM position_state
                WHERE credential_id=? AND symbol_key=? AND direction=?
                  AND state <> 'closed' AND (? = '' OR bot_uid = ?)
                ORDER BY updated_at_ms DESC
                LIMIT 2
                "#,
            )
            .bind(credential_id)
            .bind(&symbol_key)
            .bind(direction)
            .bind(bot_uid)
            .bind(bot_uid)
            .fetch_all(self.store.pool())
            .await
            .map_err(|error| {
                Status::internal(format!("db map account position state failed: {error}"))
            })?;
            if state_rows.is_empty() {
                if bot_uid.is_empty() {
                    return Ok(Response::new(unavailable_account(format!(
                        "open exchange position {symbol_key} has no active position_state"
                    ))));
                }
                continue;
            }
            if state_rows.len() != 1 {
                return Ok(Response::new(unavailable_account(format!(
                    "open exchange position {symbol_key} has ambiguous position_state ownership"
                ))));
            }
            let state = &state_rows[0];
            let Some(opened_at_text) = state
                .try_get::<Option<String>, _>("opened_at_ms_text")
                .ok()
                .flatten()
            else {
                return Ok(Response::new(unavailable_account(format!(
                    "open exchange position {symbol_key} has no opened_at_ms"
                ))));
            };
            let opened_at_ms = opened_at_text.parse::<i64>().map_err(|_| {
                Status::internal(format!(
                    "open exchange position {symbol_key} has invalid opened_at_ms"
                ))
            })?;
            if as_of_ms < opened_at_ms {
                return Ok(Response::new(unavailable_account(format!(
                    "open exchange position {symbol_key} has an invalid truth timestamp"
                ))));
            }
            let entry_price =
                ExactDecimal::parse(&row.get::<String, _>("entry_price"), "entry_price")?;
            let mark_price =
                ExactDecimal::parse(&row.get::<String, _>("mark_price"), "mark_price")?;
            let leverage = parse_account_leverage(&row.get::<String, _>("leverage"))?;
            let liquidation_price = row
                .try_get::<Option<String>, _>("liquidation_price")
                .ok()
                .flatten()
                .filter(|value| !value.trim().is_empty())
                .map(|value| ExactDecimal::parse(&value, "liquidation_price"))
                .transpose()?
                .map(ExactDecimal::to_text);
            positions.push(AccountPosition {
                symbol_key,
                side: state.get("direction"),
                qty: qty.abs_text()?,
                entry_price: entry_price.to_text(),
                mark_price: mark_price.to_text(),
                leverage,
                unrealized_pnl_usdt: unrealized.to_text(),
                // NULL stays absent. Zero, when explicitly reported, remains
                // the valid decimal string "0" and is never used as unknown.
                liquidation_price,
                opened_at_ms,
                age_minutes: (as_of_ms - opened_at_ms) as f64 / 60_000.0,
                mark_truth: None,
                schema_version: 1,
            });
        }

        let utc_day_start_ms = crate::clock::utc_day_start_ms(crate::svc::now_ms());
        let fill_rows = sqlx::query(
            r#"
            SELECT realized_pnl FROM engine_fills
            WHERE credential_id=? AND ts_exchange_ms >= ?
            "#,
        )
        .bind(credential_id)
        .bind(utc_day_start_ms)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load UTC-day fills failed: {error}")))?;
        let mut today_realized = ExactDecimal::zero();
        for row in fill_rows {
            let Some(value) = row
                .try_get::<Option<String>, _>("realized_pnl")
                .ok()
                .flatten()
            else {
                return Ok(Response::new(unavailable_account(
                    "UTC-day fill has no realized PnL truth",
                )));
            };
            today_realized = today_realized.checked_add(
                ExactDecimal::parse(&value, "realized_pnl")?,
                "today_realized_pnl_usdt",
            )?;
        }

        // These three account values and per-position age are deliberately
        // derived, not exchange-reported: equity = wallet + all unrealized PnL;
        // used margin = equity - available; UTC-day realized PnL is a fill sum;
        // age = (as_of_ms - opened_at_ms) / 60000. Exchange-backed as_of_ms is
        // the conservative reconciled truth timestamp; simulation as_of_ms is
        // this in-process truth observation's timestamp.
        let equity = wallet.checked_add(unrealized_total, "equity_usdt")?;
        let used_margin = equity.checked_sub(available, "used_margin_usdt")?;
        Ok(Response::new(GetAccountStateResponse {
            available: true,
            unavailable_reason: String::new(),
            as_of_ms,
            observed_at_ms: Some(account_observed_at_ms),
            equity_usdt: Some(equity.to_text()),
            available_usdt: Some(available.to_text()),
            used_margin_usdt: Some(used_margin.to_text()),
            today_realized_pnl_usdt: Some(today_realized.to_text()),
            open_positions_count: Some(positions.len() as u64),
            positions,
            derived_fields: ACCOUNT_DERIVED_FIELDS
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            schema_version: 1,
        }))
    }

    async fn get_order_by_client_order_id(
        &self,
        request: Request<GetOrderByClientOrderIdRequest>,
    ) -> Result<Response<GetOrderByClientOrderIdResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.trim();
        let client_id = request.client_order_id_short.trim();
        if credential_id.is_empty() || client_id.is_empty() {
            return Err(Status::invalid_argument(
                "credential_id and client_order_id_short are required",
            ));
        }
        let row = sqlx::query(
            r#"
            SELECT engine_order_id, credential_id, bot_uid,
                   COALESCE(run_id, '') AS run_id, COALESCE(cycle_id, '') AS cycle_id,
                   idempotency_key, COALESCE(client_order_id_short, '') AS client_order_id_short,
                   COALESCE(exchange_order_id, '') AS exchange_order_id,
                   COALESCE(exchange, '') AS exchange, COALESCE(market_kind, '') AS market_kind,
                   COALESCE(symbol_key, '') AS symbol_key, status,
                   CAST(created_at_ms AS CHAR) AS created_at_ms_text,
                   CAST(updated_at_ms AS CHAR) AS updated_at_ms_text,
                   COALESCE(risk_verdict_id, '') AS risk_verdict_id
            FROM engine_orders
            WHERE credential_id=? AND client_order_id_short=?
            ORDER BY created_at_ms DESC LIMIT 1
            "#,
        )
        .bind(credential_id)
        .bind(client_id)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db query client order failed: {error}")))?;
        let mut found = row.is_some();
        let mut confirmed_never_filled = false;
        let mut message = "local order lookup completed".to_string();
        let order = row
            .map(|row| {
                let status = row.get::<String, _>("status");
                confirmed_never_filled = matches!(
                    status.to_ascii_lowercase().as_str(),
                    "rejected" | "canceled" | "cancelled" | "expired"
                );
                Ok::<_, Status>(proto_v2::EngineOrder {
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
                    status,
                    // **sqlx-i64 核查批**：见 `store::try_get_i64_text`。
                    created_at_ms: position_ms(&row, "created_at_ms_text")?,
                    updated_at_ms: position_ms(&row, "updated_at_ms_text")?,
                    intent_id: String::new(),
                    risk_verdict_id: row.get("risk_verdict_id"),
                    schema_version: 1,
                })
            })
            .transpose()?;

        if !crate::venue::is_simulation_credential(credential_id) {
            let local = order.as_ref().ok_or_else(|| {
                Status::failed_precondition("local client order mapping is required for live query")
            })?;
            let keys = crate::binance_futures::keys_from_env(credential_id)
                .map_err(Status::failed_precondition)?
                .ok_or_else(|| {
                    Status::failed_precondition("missing exchange credentials for order query")
                })?;
            // regression：逐单回查也是交易所边界，走闭世界身份。
            let symbol = crate::binance_futures::usdm_symbol(&local.symbol_key)
                .ok_or_else(|| Status::failed_precondition("invalid symbol_key for order query"))?;
            let client = crate::binance_futures::BinanceFuturesClient::new_with_store(
                keys,
                credential_id,
                self.store.clone(),
            )
            .map_err(|error| Status::internal(format!("exchange client init failed: {error}")))?;
            match client
                .get_order_json(&symbol, None, Some(client_id), RequestClass::EventTruth)
                .await
            {
                Ok(value) => {
                    found = true;
                    let status = value
                        .get("status")
                        .and_then(|value| value.as_str())
                        .unwrap_or("");
                    let executed_zero = value
                        .get("executedQty")
                        .and_then(|value| value.as_str())
                        .is_some_and(crate::position_state::is_decimal_zero);
                    confirmed_never_filled =
                        executed_zero && matches!(status, "REJECTED" | "CANCELED" | "EXPIRED");
                    message = "exchange client-order lookup completed".to_string();
                }
                Err(error) if error.to_string().contains("-2013") => {
                    found = false;
                    confirmed_never_filled = true;
                    message = "exchange confirmed client order absent".to_string();
                }
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "exchange order query failed: {error}"
                    )));
                }
            }
        }
        let observed_at_ms = crate::svc::now_ms();
        sqlx::query(
            r#"
            INSERT INTO recovery_order_lookups (
              lookup_id, credential_id, client_order_id_short, completed,
              found, confirmed_never_filled, status, observed_at_ms
            ) VALUES (?, ?, ?, 1, ?, ?, ?, ?)
            "#,
        )
        .bind(format!("rol_{}", ulid::Ulid::new()))
        .bind(credential_id)
        .bind(client_id)
        .bind(if found { 1i64 } else { 0i64 })
        .bind(if confirmed_never_filled { 1i64 } else { 0i64 })
        .bind(order.as_ref().map(|order| order.status.as_str()))
        .bind(observed_at_ms)
        .execute(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db persist order lookup failed: {error}")))?;
        Ok(Response::new(GetOrderByClientOrderIdResponse {
            completed: true,
            found,
            order,
            confirmed_never_filled,
            message,
            schema_version: 1,
        }))
    }

    async fn release_pending(
        &self,
        request: Request<ReleasePendingRequest>,
    ) -> Result<Response<ReleasePendingResponse>, Status> {
        let request = request.into_inner();
        if request.credential_id.trim().is_empty()
            || request.position_id.trim().is_empty()
            || request.client_order_id_short.trim().is_empty()
        {
            return Err(Status::invalid_argument(
                "credential_id, position_id, and client_order_id_short are required",
            ));
        }
        let proof = sqlx::query(
            r#"
            SELECT confirmed_never_filled FROM recovery_order_lookups
            WHERE credential_id=? AND client_order_id_short=? AND completed=1
            ORDER BY observed_at_ms DESC LIMIT 1
            "#,
        )
        .bind(&request.credential_id)
        .bind(&request.client_order_id_short)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| {
            Status::internal(format!("db load pending release proof failed: {error}"))
        })?;
        if !proof.is_some_and(|row| row.get::<i64, _>("confirmed_never_filled") == 1) {
            return Err(Status::failed_precondition(
                "pending release requires a completed never-filled client-order lookup",
            ));
        }
        let mut tx = self.store.pool().begin().await.map_err(|error| {
            Status::internal(format!("db begin pending release failed: {error}"))
        })?;
        let position = sqlx::query(
            r#"
            SELECT ps.entry_epoch
            FROM position_state ps
            WHERE ps.credential_id=? AND ps.position_id=? AND ps.state='pending'
              AND EXISTS (
                SELECT 1 FROM engine_orders eo
                WHERE eo.position_id=ps.position_id
                  AND eo.order_role='entry'
                  AND eo.client_order_id_short=?
              )
            LIMIT 1
            "#,
        )
        .bind(&request.credential_id)
        .bind(&request.position_id)
        .bind(&request.client_order_id_short)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db load pending position failed: {error}")))?
        .into_iter()
        .next()
        .ok_or_else(|| Status::failed_precondition("pending position not found"))?;
        let entry_epoch = position.get::<i64, _>("entry_epoch");
        let released_at_ms = crate::svc::now_ms();
        sqlx::query(
            "DELETE FROM position_state WHERE credential_id=? AND position_id=? AND state='pending'",
        )
        .bind(&request.credential_id)
        .bind(&request.position_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db release pending position failed: {error}")))?;
        sqlx::query(
            r#"
            INSERT INTO recovery_pending_releases (
              release_id, credential_id, position_id, client_order_id_short,
              entry_epoch, released_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(format!("rpr_{}", ulid::Ulid::new()))
        .bind(&request.credential_id)
        .bind(&request.position_id)
        .bind(&request.client_order_id_short)
        .bind(entry_epoch)
        .bind(released_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db audit pending release failed: {error}")))?;
        tx.commit().await.map_err(|error| {
            Status::internal(format!("db commit pending release failed: {error}"))
        })?;
        Ok(Response::new(ReleasePendingResponse {
            released: true,
            entry_epoch,
            message: "pending released; entry_epoch unchanged".to_string(),
            schema_version: 1,
        }))
    }

    async fn fund_simulation_account(
        &self,
        request: Request<FundSimulationAccountRequest>,
    ) -> Result<Response<FundSimulationAccountResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.as_str();
        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id is required"));
        }
        if !crate::venue::is_simulation_credential(credential_id) {
            return Err(Status::permission_denied(
                "simulation funding rejects exchange-backed credentials",
            ));
        }
        let idempotency_identity = request.idempotency_identity.as_str();
        if idempotency_identity.is_empty()
            || idempotency_identity.len() > 128
            || idempotency_identity.trim() != idempotency_identity
        {
            return Err(Status::invalid_argument(
                "idempotency_identity must be 1..128 characters without surrounding whitespace",
            ));
        }
        if request.amount_usdt.is_empty() || request.amount_usdt.trim() != request.amount_usdt {
            return Err(Status::invalid_argument(
                "amount_usdt must be a positive decimal string without surrounding whitespace",
            ));
        }
        let amount =
            ExactDecimal::parse(&request.amount_usdt, "request amount_usdt").map_err(|_| {
                Status::invalid_argument("amount_usdt must be a positive decimal string")
            })?;
        if amount.units <= 0 {
            return Err(Status::invalid_argument(
                "amount_usdt must be a positive decimal string",
            ));
        }
        let amount_text = amount.to_text();

        let existing = sqlx::query(
            "SELECT credential_id, amount_usdt FROM simulation_account_funding_events WHERE idempotency_identity=? LIMIT 1",
        )
        .bind(idempotency_identity)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| Status::internal(format!("db load simulation funding event failed: {error}")))?;
        if let Some(existing) = existing {
            validate_simulation_funding_replay(
                &existing.get::<String, _>("credential_id"),
                &existing.get::<String, _>("amount_usdt"),
                credential_id,
                amount,
            )?;
            let balance = sqlx::query(
                "SELECT exchange, market_kind, wallet_balance FROM engine_balances WHERE credential_id=? AND asset='USDT' LIMIT 1",
            )
            .bind(credential_id)
            .fetch_optional(self.store.pool())
            .await
            .map_err(|error| Status::internal(format!("db load simulation balance failed: {error}")))?
            .ok_or_else(|| Status::internal("simulation funding event exists without USDT balance"))?;
            if balance.get::<String, _>("exchange") != "binance"
                || balance.get::<String, _>("market_kind") != "futures"
            {
                return Err(Status::internal(
                    "stored simulation balance namespace is invalid",
                ));
            }
            let wallet = ExactDecimal::parse(
                &balance.get::<String, _>("wallet_balance"),
                "simulation wallet_balance",
            )?;
            return Ok(Response::new(FundSimulationAccountResponse {
                accepted: true,
                applied: false,
                message: "duplicate".to_string(),
                wallet_balance_usdt: wallet.to_text(),
                schema_version: 1,
            }));
        }

        let now_ms = crate::svc::now_ms();
        let mut tx = self.store.pool().begin().await.map_err(|error| {
            Status::internal(format!("db begin simulation funding failed: {error}"))
        })?;
        let ensure_balance_query = match self.store.kind() {
            DbKind::Sqlite => {
                "INSERT OR IGNORE INTO engine_balances (credential_id, exchange, market_kind, asset, wallet_balance, available_balance, updated_at_ms, raw_json) VALUES (?, 'binance', 'futures', 'USDT', '0', '0', ?, NULL)"
            }
            DbKind::MySql => {
                "INSERT IGNORE INTO engine_balances (credential_id, exchange, market_kind, asset, wallet_balance, available_balance, updated_at_ms, raw_json) VALUES (?, 'binance', 'futures', 'USDT', '0', '0', ?, NULL)"
            }
            DbKind::Unknown => {
                return Err(Status::internal(
                    "simulation funding requires sqlite or mysql store",
                ))
            }
        };
        sqlx::query(ensure_balance_query)
            .bind(credential_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Status::internal(format!("db ensure simulation balance failed: {error}"))
            })?;

        let lock_balance_query = match self.store.kind() {
            DbKind::MySql => {
                "SELECT exchange, market_kind, wallet_balance, available_balance FROM engine_balances WHERE credential_id=? AND asset='USDT' LIMIT 1 FOR UPDATE"
            }
            _ => {
                "SELECT exchange, market_kind, wallet_balance, available_balance FROM engine_balances WHERE credential_id=? AND asset='USDT' LIMIT 1"
            }
        };
        let balance = sqlx::query(lock_balance_query)
            .bind(credential_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| {
                Status::internal(format!("db lock simulation balance failed: {error}"))
            })?;
        if balance.get::<String, _>("exchange") != "binance"
            || balance.get::<String, _>("market_kind") != "futures"
        {
            return Err(Status::failed_precondition(
                "existing USDT balance is outside the Product Demo namespace",
            ));
        }

        let raced_event = sqlx::query(
            "SELECT credential_id, amount_usdt FROM simulation_account_funding_events WHERE idempotency_identity=? LIMIT 1",
        )
        .bind(idempotency_identity)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db reload simulation funding event failed: {error}")))?.into_iter().next();
        if let Some(existing) = raced_event {
            validate_simulation_funding_replay(
                &existing.get::<String, _>("credential_id"),
                &existing.get::<String, _>("amount_usdt"),
                credential_id,
                amount,
            )?;
            let wallet = ExactDecimal::parse(
                &balance.get::<String, _>("wallet_balance"),
                "simulation wallet_balance",
            )?;
            tx.rollback().await.map_err(|error| {
                Status::internal(format!(
                    "db rollback duplicate simulation funding failed: {error}"
                ))
            })?;
            return Ok(Response::new(FundSimulationAccountResponse {
                accepted: true,
                applied: false,
                message: "duplicate".to_string(),
                wallet_balance_usdt: wallet.to_text(),
                schema_version: 1,
            }));
        }

        let wallet = ExactDecimal::parse(
            &balance.get::<String, _>("wallet_balance"),
            "simulation wallet_balance",
        )?;
        let available = ExactDecimal::parse(
            &balance.get::<String, _>("available_balance"),
            "simulation available_balance",
        )?;
        let funded_wallet = wallet.checked_add(amount, "simulation wallet_balance")?;
        let funded_available = available.checked_add(amount, "simulation available_balance")?;

        let persist_event_result = sqlx::query(
            "INSERT INTO simulation_account_funding_events (idempotency_identity, credential_id, amount_usdt, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(idempotency_identity)
        .bind(credential_id)
        .bind(&amount_text)
        .bind(now_ms)
        .execute(&mut *tx)
        .await;
        if let Err(persist_error) = persist_event_result {
            tx.rollback().await.map_err(|error| {
                Status::internal(format!(
                    "db rollback raced simulation funding failed: {error}"
                ))
            })?;
            let existing = sqlx::query(
                "SELECT credential_id, amount_usdt FROM simulation_account_funding_events WHERE idempotency_identity=? LIMIT 1",
            )
            .bind(idempotency_identity)
            .fetch_optional(self.store.pool())
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "db resolve raced simulation funding event failed: {error}"
                ))
            })?;
            if let Some(existing) = existing {
                validate_simulation_funding_replay(
                    &existing.get::<String, _>("credential_id"),
                    &existing.get::<String, _>("amount_usdt"),
                    credential_id,
                    amount,
                )?;
                let balance = sqlx::query(
                    "SELECT exchange, market_kind, wallet_balance FROM engine_balances WHERE credential_id=? AND asset='USDT' LIMIT 1",
                )
                .bind(credential_id)
                .fetch_optional(self.store.pool())
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "db load raced simulation balance failed: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    Status::internal("simulation funding event exists without USDT balance")
                })?;
                if balance.get::<String, _>("exchange") != "binance"
                    || balance.get::<String, _>("market_kind") != "futures"
                {
                    return Err(Status::internal(
                        "stored simulation balance namespace is invalid",
                    ));
                }
                let wallet = ExactDecimal::parse(
                    &balance.get::<String, _>("wallet_balance"),
                    "simulation wallet_balance",
                )?;
                return Ok(Response::new(FundSimulationAccountResponse {
                    accepted: true,
                    applied: false,
                    message: "duplicate".to_string(),
                    wallet_balance_usdt: wallet.to_text(),
                    schema_version: 1,
                }));
            }
            return Err(Status::internal(format!(
                "db persist simulation funding event failed: {persist_error}"
            )));
        }
        let update_result = sqlx::query(
            "UPDATE engine_balances SET wallet_balance=?, available_balance=?, updated_at_ms=? WHERE credential_id=? AND asset='USDT' AND exchange='binance' AND market_kind='futures'",
        )
        .bind(funded_wallet.to_text())
        .bind(funded_available.to_text())
        .bind(now_ms)
        .bind(credential_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db apply simulation funding failed: {error}")))?;
        if update_result.rows_affected() != 1 {
            return Err(Status::failed_precondition(
                "simulation USDT balance update did not affect exactly one row",
            ));
        }
        tx.commit().await.map_err(|error| {
            Status::internal(format!("db commit simulation funding failed: {error}"))
        })?;

        Ok(Response::new(FundSimulationAccountResponse {
            accepted: true,
            applied: true,
            message: "applied".to_string(),
            wallet_balance_usdt: funded_wallet.to_text(),
            schema_version: 1,
        }))
    }

    async fn put_risk_budget(
        &self,
        request: Request<PutRiskBudgetRequest>,
    ) -> Result<Response<PutRiskBudgetResponse>, Status> {
        let request = request.into_inner();
        let credential_id = request.credential_id.trim();
        if credential_id.is_empty() {
            return Err(Status::invalid_argument("credential_id is required"));
        }
        if request.as_of_ms <= 0 {
            return Err(Status::invalid_argument("as_of_ms must be positive"));
        }

        // These are the account/portfolio facts required by the current
        // evaluator. Reject an incomplete budget rather than converting an
        // absent fact to zero and accidentally opening fail-open.
        let equity_usdt = required_decimal(request.equity_usdt, "equity_usdt")?;
        let available_usdt = required_decimal(request.available_usdt, "available_usdt")?;
        let today_realized_pnl_usdt =
            required_decimal(request.today_realized_pnl_usdt, "today_realized_pnl_usdt")?;
        let today_realized_pnl_pct =
            required_finite(request.today_realized_pnl_pct, "today_realized_pnl_pct")?;
        let drawdown_pct = required_finite(request.drawdown_pct, "drawdown_pct")?;
        let total_exposure_pct = required_finite(request.total_exposure_pct, "total_exposure_pct")?;
        let open_positions_count = request
            .open_positions_count
            .filter(|count| *count >= 0)
            .ok_or_else(|| {
                Status::invalid_argument(
                    "open_positions_count is required and must be non-negative",
                )
            })?;
        if drawdown_pct < 0.0 || total_exposure_pct < 0.0 {
            return Err(Status::invalid_argument(
                "drawdown_pct and total_exposure_pct must be non-negative",
            ));
        }
        let group_exposure_json = validate_group_exposure_json(request.group_exposure_json)?;
        let kill_switch_level = request
            .kill_switch_level
            .filter(|level| !level.trim().is_empty())
            .unwrap_or_else(|| "none".to_string());
        if !matches!(
            kill_switch_level.as_str(),
            "none" | "pause_new" | "manage_only" | "flatten"
        ) {
            return Err(Status::invalid_argument("invalid kill_switch_level"));
        }
        if let Some(scope) = request.kill_switch_scope.as_deref() {
            if !matches!(scope, "global" | "credential" | "bot") {
                return Err(Status::invalid_argument("invalid kill_switch_scope"));
            }
            if kill_switch_level != "none" && scope != "credential" {
                return Err(Status::invalid_argument(
                    "active budget kill switch supports credential scope only; use SetKillSwitch for global/bot",
                ));
            }
        }
        let remaining_credential_notional_usdt = request
            .remaining_credential_notional_usdt
            .map(|value| required_decimal(Some(value), "remaining_credential_notional_usdt"))
            .transpose()?;

        let query = match self.store.kind() {
            DbKind::MySql => {
                r#"
                INSERT INTO risk_budget (
                  credential_id, as_of_ms, equity_usdt, available_usdt,
                  today_realized_pnl_usdt, today_realized_pnl_pct,
                  drawdown_pct, total_exposure_pct, open_positions_count,
                  group_exposure_json, kill_switch_level, kill_switch_scope,
                  kill_switch_reason, remaining_credential_notional_usdt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  equity_usdt=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(equity_usdt), equity_usdt),
                  available_usdt=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(available_usdt), available_usdt),
                  today_realized_pnl_usdt=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(today_realized_pnl_usdt), today_realized_pnl_usdt),
                  today_realized_pnl_pct=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(today_realized_pnl_pct), today_realized_pnl_pct),
                  drawdown_pct=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(drawdown_pct), drawdown_pct),
                  total_exposure_pct=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(total_exposure_pct), total_exposure_pct),
                  open_positions_count=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(open_positions_count), open_positions_count),
                  group_exposure_json=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(group_exposure_json), group_exposure_json),
                  kill_switch_level=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(kill_switch_level), kill_switch_level),
                  kill_switch_scope=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(kill_switch_scope), kill_switch_scope),
                  kill_switch_reason=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(kill_switch_reason), kill_switch_reason),
                  remaining_credential_notional_usdt=IF(VALUES(as_of_ms) >= as_of_ms, VALUES(remaining_credential_notional_usdt), remaining_credential_notional_usdt),
                  as_of_ms=GREATEST(as_of_ms, VALUES(as_of_ms))
                "#
            }
            _ => {
                r#"
                INSERT INTO risk_budget (
                  credential_id, as_of_ms, equity_usdt, available_usdt,
                  today_realized_pnl_usdt, today_realized_pnl_pct,
                  drawdown_pct, total_exposure_pct, open_positions_count,
                  group_exposure_json, kill_switch_level, kill_switch_scope,
                  kill_switch_reason, remaining_credential_notional_usdt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(credential_id) DO UPDATE SET
                  as_of_ms=excluded.as_of_ms,
                  equity_usdt=excluded.equity_usdt,
                  available_usdt=excluded.available_usdt,
                  today_realized_pnl_usdt=excluded.today_realized_pnl_usdt,
                  today_realized_pnl_pct=excluded.today_realized_pnl_pct,
                  drawdown_pct=excluded.drawdown_pct,
                  total_exposure_pct=excluded.total_exposure_pct,
                  open_positions_count=excluded.open_positions_count,
                  group_exposure_json=excluded.group_exposure_json,
                  kill_switch_level=excluded.kill_switch_level,
                  kill_switch_scope=excluded.kill_switch_scope,
                  kill_switch_reason=excluded.kill_switch_reason,
                  remaining_credential_notional_usdt=excluded.remaining_credential_notional_usdt
                WHERE excluded.as_of_ms >= risk_budget.as_of_ms
                "#
            }
        };
        sqlx::query(query)
            .bind(credential_id)
            // Preserve millisecond epoch precision through sqlx::Any on both
            // SQLite and MySQL; the BIGINT column coerces this decimal text.
            .bind(request.as_of_ms.to_string())
            .bind(equity_usdt)
            .bind(available_usdt)
            .bind(today_realized_pnl_usdt)
            .bind(today_realized_pnl_pct)
            .bind(drawdown_pct)
            .bind(total_exposure_pct)
            .bind(open_positions_count)
            .bind(group_exposure_json)
            .bind(kill_switch_level)
            .bind(request.kill_switch_scope)
            .bind(request.kill_switch_reason)
            .bind(remaining_credential_notional_usdt)
            .execute(self.store.pool())
            .await
            .map_err(|error| Status::internal(format!("db upsert risk budget failed: {error}")))?;

        let current_as_of_ms =
            sqlx::query("SELECT CAST(as_of_ms AS CHAR) AS as_of_ms_text FROM risk_budget WHERE credential_id = ?")
                .bind(credential_id)
                .fetch_one(self.store.pool())
                .await
                .map_err(|error| Status::internal(format!("db read risk budget failed: {error}")))?
                .get::<String, _>("as_of_ms_text")
                .parse::<i64>()
                .map_err(|_| Status::internal("stored risk budget as_of_ms is invalid"))?;
        let applied = current_as_of_ms == request.as_of_ms;
        Ok(Response::new(PutRiskBudgetResponse {
            accepted: true,
            applied,
            message: if applied {
                "applied".to_string()
            } else {
                "ignored_older_as_of".to_string()
            },
            current_as_of_ms,
            schema_version: 1,
        }))
    }

    async fn put_promotion_certification(
        &self,
        request: Request<PutPromotionCertificationRequest>,
    ) -> Result<Response<PutPromotionCertificationResponse>, Status> {
        let request = request.into_inner();
        let cert_id = required_projection_text(request.cert_id, "cert_id", 64)?;
        let bot_uid = required_projection_text(request.bot_uid, "bot_uid", 64)?;
        let stage = required_projection_text(request.stage, "stage", 16)?.to_ascii_lowercase();
        if !matches!(stage.as_str(), "paper" | "demo" | "live_small" | "live") {
            return Err(Status::invalid_argument("invalid certification stage"));
        }
        let config_hash = required_projection_text(request.config_hash, "config_hash", 80)?;
        if !is_lower_hex(&config_hash, 64) {
            return Err(Status::invalid_argument(
                "config_hash must be 64 lowercase hex characters",
            ));
        }
        let prompt_hash = required_projection_text(request.prompt_hash, "prompt_hash", 80)?;
        let prompt_digest = prompt_hash.strip_prefix("sha256:").unwrap_or(&prompt_hash);
        if !is_lower_hex(prompt_digest, 64) {
            return Err(Status::invalid_argument(
                "prompt_hash must be a lowercase sha256 digest",
            ));
        }
        let policy_version =
            required_projection_text(request.policy_version, "policy_version", 64)?;
        let code_commit = required_projection_text(request.code_commit, "code_commit", 40)?;
        if !is_lower_hex(&code_commit, 40) {
            return Err(Status::invalid_argument(
                "code_commit must be a full 40-character lowercase Git commit",
            ));
        }
        if request.certified_at_ms <= 0 {
            return Err(Status::invalid_argument("certified_at_ms must be positive"));
        }
        if request.invalidated_at_ms.is_some_and(|value| value <= 0) {
            return Err(Status::invalid_argument(
                "invalidated_at_ms must be positive when present",
            ));
        }

        let mut tx =
            self.store.pool().begin().await.map_err(|error| {
                Status::internal(format!("db begin projection failed: {error}"))
            })?;
        if let Some(existing) = sqlx::query(
            r#"
            SELECT bot_uid, stage, config_hash, prompt_hash, policy_version, code_commit
            FROM promotion_certifications_projection WHERE cert_id=? LIMIT 1
            "#,
        )
        .bind(&cert_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| Status::internal(format!("db inspect certification failed: {error}")))?
        .into_iter()
        .next()
        {
            let same_tuple = existing.get::<String, _>("bot_uid") == bot_uid
                && existing.get::<String, _>("stage") == stage
                && existing.get::<String, _>("config_hash") == config_hash
                && existing.get::<String, _>("prompt_hash") == prompt_hash
                && existing.get::<String, _>("policy_version") == policy_version
                && existing.get::<String, _>("code_commit") == code_commit;
            if !same_tuple {
                return Err(Status::invalid_argument(
                    "cert_id is already bound to a different certification tuple",
                ));
            }
        }

        let query = match self.store.kind() {
            DbKind::MySql => {
                r#"
                INSERT INTO promotion_certifications_projection (
                  cert_id, bot_uid, stage, config_hash, prompt_hash,
                  policy_version, code_commit, passed, invalidated_at_ms,
                  certified_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  cert_id=VALUES(cert_id), passed=VALUES(passed),
                  invalidated_at_ms=VALUES(invalidated_at_ms),
                  certified_at_ms=VALUES(certified_at_ms)
                "#
            }
            _ => {
                r#"
                INSERT INTO promotion_certifications_projection (
                  cert_id, bot_uid, stage, config_hash, prompt_hash,
                  policy_version, code_commit, passed, invalidated_at_ms,
                  certified_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bot_uid, stage, config_hash, prompt_hash, policy_version, code_commit)
                DO UPDATE SET cert_id=excluded.cert_id, passed=excluded.passed,
                  invalidated_at_ms=excluded.invalidated_at_ms,
                  certified_at_ms=excluded.certified_at_ms
                "#
            }
        };
        sqlx::query(query)
            .bind(&cert_id)
            .bind(&bot_uid)
            .bind(&stage)
            .bind(&config_hash)
            .bind(&prompt_hash)
            .bind(&policy_version)
            .bind(&code_commit)
            .bind(i64::from(request.passed))
            .bind(request.invalidated_at_ms.map(|value| value.to_string()))
            .bind(request.certified_at_ms.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Status::internal(format!("db upsert promotion certification failed: {error}"))
            })?;
        tx.commit().await.map_err(|error| {
            Status::internal(format!("db commit promotion certification failed: {error}"))
        })?;

        Ok(Response::new(PutPromotionCertificationResponse {
            accepted: true,
            applied: true,
            message: "applied".to_string(),
            schema_version: 1,
        }))
    }

    async fn set_kill_switch(
        &self,
        request: Request<SetKillSwitchRequest>,
    ) -> Result<Response<SetKillSwitchResponse>, Status> {
        let request = request.into_inner();
        let level = crate::kill_switch::Level::parse(&request.level)
            .filter(|level| *level != crate::kill_switch::Level::None)
            .ok_or_else(|| {
                Status::invalid_argument("level must be pause_new, manage_only, or flatten")
            })?;
        let trigger_source = request.trigger_source.trim();
        // ****：此前这里只 allowlist
        // 了 source **字符串**，没有绑定它允许的 scope 与档位。于是一个持有
        // 服务 token 的调用方可以发
        // `trigger_source=auto_llm_failures, scope_key=cred:*, level=flatten`，
        // 把"某个 bot 的 provider 连续失败"升级成**整个 credential 强平**——
        // 辅助触发源反向误伤了平仓/保护生命周期。
        //
        // 改成**闭世界映射**：每个自动源只能按它被裁定的 scope 与档位 arm。
        // 人工源保留自由度（那是人的判断），但仍要求 actor。
        if let Err(message) =
            validate_kill_switch_trigger_contract(trigger_source, request.scope_key.trim(), level)
        {
            return Err(Status::invalid_argument(message));
        }
        if trigger_source == "manual_api" && request.actor.trim().is_empty() {
            return Err(Status::invalid_argument("actor is required for manual_api"));
        }
        // **P4b · 会员降险链的字段纪律（WO §3-1）**：member set 载荷必带
        // 整数身份与 canonical ULID request_ref，且**不得**携带
        // expected_authority_revision（降险链无 revision 门——带上即拒，
        // 防止"门"被悄悄装回降险路径）；平台/自动路径不携带会员上下文
        // （member 字段与 member_api 源必须同现同缺）。
        let member_context = match (
            trigger_source == crate::kill_switch::TRIGGER_SOURCE_MEMBER_API,
            request.authority_member_id,
            request.request_ref.as_ref(),
        ) {
            (true, Some(member_id), Some(reference)) => {
                if member_id <= 0 {
                    return Err(Status::invalid_argument(
                        "authority_member_id must be a positive member id for member_api",
                    ));
                }
                if !crate::kill_switch::is_canonical_request_ref(reference) {
                    return Err(Status::invalid_argument(
                        "request_ref must be a canonical ULID",
                    ));
                }
                if request.expected_authority_revision.is_some() {
                    return Err(Status::invalid_argument(
                        "member arm carries no expected_authority_revision; the \
                         risk-reducing chain has no read gate (ruling #35)",
                    ));
                }
                Some(crate::kill_switch::MemberArmContext {
                    member_id: member_id as i64,
                    request_ref: reference.as_str(),
                })
            }
            (true, _, _) => {
                return Err(Status::invalid_argument(
                    "member_api requires authority_member_id and request_ref",
                ));
            }
            (false, None, None) => None,
            (false, _, _) => {
                return Err(Status::invalid_argument(
                    "authority_member_id/request_ref are only valid with member_api",
                ));
            }
        };
        let now_ms = crate::svc::now_ms();
        let arm_request = crate::kill_switch::ArmRequest {
            scope_key: request.scope_key.trim(),
            level,
            trigger_source,
            reason: request.reason.trim(),
            now_ms,
        };
        let state = match member_context {
            Some(context) => {
                crate::kill_switch::arm_member(&self.store, arm_request, context).await
            }
            None => crate::kill_switch::arm(&self.store, arm_request).await,
        }
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if !crate::svc::is_binance_live_enabled() {
            crate::kill_switch::apply_local_order_semantics(
                &self.store,
                &state.scope_key,
                state.level,
                now_ms,
            )
            .await
            .map_err(|error| {
                Status::internal(format!("apply kill switch actions failed: {error}"))
            })?;
        }
        Ok(Response::new(SetKillSwitchResponse {
            accepted: true,
            scope_key: state.scope_key,
            level: state.level.as_str().to_string(),
            flatten_state: state.flatten_state.unwrap_or_default(),
            message: "armed".to_string(),
            request_ref: request.request_ref,
            schema_version: 1,
        }))
    }

    async fn reset_kill_switch(
        &self,
        request: Request<ResetKillSwitchRequest>,
    ) -> Result<Response<ResetKillSwitchResponse>, Status> {
        let request = request.into_inner();
        // **P4b · 增险链的服务端固定 MEMBER 分流（FA C2.2 / WO §3-1）。**
        // `reset_authority=MEMBER` 必须伴随全部约束字段（g>0、整数身份、
        // canonical ULID）；缺省字段绝不落入平台语义（无隐式特权）。平台
        // 路径（UNCONSTRAINED）字节零变化：现有判据、现有 payload。
        let authority_raw = request.reset_authority.unwrap_or(0);
        let member_reset = authority_raw == crate::proto_v2::ResetAuthority::Member as i32;
        if member_reset {
            let member_id = request.authority_member_id.unwrap_or_default();
            let expected = request.expected_authority_revision.unwrap_or_default();
            let Some(reference) = request.request_ref.as_ref() else {
                return Err(Status::invalid_argument(
                    "a MEMBER reset requires request_ref",
                ));
            };
            if member_id <= 0 {
                return Err(Status::invalid_argument(
                    "a MEMBER reset requires a positive authority_member_id",
                ));
            }
            if expected <= 0 {
                return Err(Status::invalid_argument(
                    "a MEMBER reset requires a positive expected_authority_revision",
                ));
            }
            if !crate::kill_switch::is_canonical_request_ref(reference) {
                return Err(Status::invalid_argument(
                    "request_ref must be a canonical ULID",
                ));
            }
            if request.reset_by.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "reset_by is required (the member channel sends member:<id>)",
                ));
            }
            match crate::kill_switch::reset_member(
                &self.store,
                request.scope_key.trim(),
                member_id as i64,
                expected,
                reference.as_str(),
                crate::svc::now_ms(),
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            {
                crate::kill_switch::MemberResetOutcome::Executed { generation } => {
                    Ok(Response::new(ResetKillSwitchResponse {
                        accepted: true,
                        scope_key: request.scope_key.clone(),
                        level: "none".to_string(),
                        message: format!("member reset committed at generation {generation}"),
                        request_ref: request.request_ref.clone(),
                        schema_version: 1,
                    }))
                }
                crate::kill_switch::MemberResetOutcome::NotAdmitted => {
                    Err(Status::failed_precondition("MEMBER_RESET_NOT_ADMITTED"))
                }
                crate::kill_switch::MemberResetOutcome::Rejected(reason) => {
                    Err(Status::failed_precondition(reason))
                }
            }
        } else {
            let request_ref = match request.request_ref.as_deref() {
                None => None,
                Some("") => None,
                Some(reference) => {
                    if !crate::kill_switch::is_canonical_request_ref(reference) {
                        return Err(Status::invalid_argument(
                            "request_ref must be a canonical ULID when provided",
                        ));
                    }
                    Some(reference)
                }
            };
            crate::kill_switch::reset(
                &self.store,
                request.scope_key.trim(),
                request.reset_by.trim(),
                request_ref,
                crate::svc::now_ms(),
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
            Ok(Response::new(ResetKillSwitchResponse {
                accepted: true,
                scope_key: request.scope_key,
                level: "none".to_string(),
                message: "reset".to_string(),
                request_ref: None,
                schema_version: 1,
            }))
        }
    }

    /// **P4b · 只读投影（零写副作用）**。上界（256 scopes / 64 refs）在
    /// 引擎侧拒绝；本 handler 只做映射。
    async fn get_kill_switch_states(
        &self,
        request: Request<GetKillSwitchStatesRequest>,
    ) -> Result<Response<GetKillSwitchStatesResponse>, Status> {
        let request = request.into_inner();
        let outcome = crate::kill_switch::get_kill_switch_states(
            &self.store,
            &request.scope_keys,
            &request.request_refs,
        )
        .await
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observed_at_ms = crate::svc::now_ms();
        let rows = outcome
            .rows
            .into_iter()
            .map(|row| crate::proto_v2::KillSwitchAuthorityRow {
                scope_key: row.scope_key,
                found: row.found,
                level: row.level,
                raw_level: row.raw_level,
                level_canonical: row.level_canonical,
                trigger_source: row.trigger_source,
                authority_kind: row.authority_kind,
                authority_member_id: row.authority_member_id.map(|v| v as i32).unwrap_or(0),
                flatten_generation: row.flatten_generation,
                flatten_state: row.flatten_state,
                last_reconcile_clean: tri(row.last_reconcile_clean),
                last_reconcile_at_ms: row.last_reconcile_at_ms,
                armed_at_ms: row.armed_at_ms,
                observed_at_ms,
                convergence: row
                    .convergence
                    .into_iter()
                    .map(|entry| crate::proto_v2::KillSwitchConvergenceRow {
                        credential_id: entry.credential_id,
                        expected_provenance: entry.expected_provenance,
                        reconcile_clean: tri(entry.reconcile_clean),
                        flatten_complete: tri(entry.flatten_complete),
                        all_positions_zero: tri(entry.truth_positions_zero),
                        all_open_orders_empty: tri(entry.truth_orders_empty),
                        all_open_algo_orders_empty: tri(entry.truth_algo_orders_empty),
                        local_position_zero_diff: tri(entry.truth_local_zero_diff),
                        no_diff: tri(entry.truth_no_diff),
                        reconciled_at_ms: entry.reconciled_at_ms,
                        schema_version: 1,
                    })
                    .collect(),
                scope_convergence: row.scope_convergence.map(|scope| {
                    crate::proto_v2::KillSwitchScopeConvergence {
                        expected_credential_ids: scope.expected,
                        reported_credential_ids: scope.reported,
                        unreported_credential_ids: scope.unreported,
                        quarantined_expected_ids: scope.quarantined_expected,
                        quarantined_outsider_ids: scope.quarantined_outsider,
                        clean: scope.clean,
                        flatten_complete: scope.flatten_complete,
                        expected_total: scope.expected_total,
                        dirty_count: scope.dirty,
                        incomplete_count: scope.incomplete,
                        schema_version: 1,
                    }
                }),
                schema_version: 1,
            })
            .collect();
        let request_ref_hits = outcome
            .ref_hits
            .into_iter()
            .map(|hit| crate::proto_v2::KillSwitchRequestRefHit {
                request_ref: hit.request_ref,
                scope_key: hit.scope_key,
                flatten_generation: hit.flatten_generation,
                kind: hit.kind,
                trigger_source: hit.trigger_source,
                at_ms: hit.at_ms,
                arm_actor_kind: hit.arm_actor_kind,
                arm_actor_member_id: hit.arm_actor_member_id.map(|v| v as i32).unwrap_or(0),
                reset_committed: hit.reset_committed,
                current_authority_kind: hit.current_authority_kind,
                current_authority_member_id: hit
                    .current_authority_member_id
                    .map(|v| v as i32)
                    .unwrap_or(0),
                schema_version: 1,
            })
            .collect();
        Ok(Response::new(GetKillSwitchStatesResponse {
            rows,
            request_ref_hits,
            snapshot_stale: outcome.snapshot_stale,
            engine_boot_id: self.store.boot_id().to_string(),
            admission_version: outcome.admission_version,
            schema_version: 1,
        }))
    }

    /// **P4b · 准入动作**：锁内双比对（expected boot/version），只批当前
    /// boot；post_rollback 分支同事务只降归属列（D10：不做交易性修复）。
    async fn admit_member_reset(
        &self,
        request: Request<AdmitMemberResetRequest>,
    ) -> Result<Response<AdmitMemberResetResponse>, Status> {
        let request = request.into_inner();
        match crate::kill_switch::admit_member_reset(
            &self.store,
            request.basis.trim(),
            request.admitted_by.trim(),
            request.demote_unknown_member_authority,
            request.expected_boot_id.trim(),
            request.expected_admission_version,
            crate::svc::now_ms(),
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        {
            Ok(outcome) => Ok(Response::new(AdmitMemberResetResponse {
                admitted: outcome.admitted,
                admitted_boot_id: outcome.admitted_boot_id,
                admission_version: outcome.admission_version_after,
                demoted_scopes: outcome.demoted_scopes,
                message: outcome.message,
                schema_version: 1,
            })),
            // 形状类失败（basis 前缀/长度、expected 缺席或形状）映射 400；
            // 绑定类失败（boot/version 不符）映射 409 语义的
            // failed_precondition——调用方该重新核验，不是修请求参数。
            Err(reason) => {
                if reason.contains("must")
                    || reason.contains("required")
                    || reason.contains("prefix")
                {
                    Err(Status::invalid_argument(reason))
                } else {
                    Err(Status::failed_precondition(reason))
                }
            }
        }
    }
}

/// 读侧 `Option<bool>` → proto TriState 值（None=UNKNOWN=0，FALSE=1，TRUE=2；
/// prost 生成的字段就是 i32，值域与 proto 定义钉死）。
fn tri(value: Option<bool>) -> i32 {
    crate::kill_switch::read_tri_state(value)
}

#[derive(Clone, Default)]
pub struct EngineEventsV2Skeleton;

#[tonic::async_trait]
impl EngineEvents for EngineEventsV2Skeleton {
    type StreamEventsStream =
        Pin<Box<dyn Stream<Item = Result<EngineEvent, Status>> + Send + 'static>>;

    async fn stream_events(
        &self,
        _request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        Err(Status::unimplemented(SKELETON_MESSAGE))
    }
}

#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub fn execution_server(
    store: Store,
) -> proto_v2::engine_execution_server::EngineExecutionServer<EngineExecutionV2Service> {
    proto_v2::engine_execution_server::EngineExecutionServer::new(EngineExecutionV2Service::new(
        store,
    ))
}

#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub fn events_server() -> proto_v2::engine_events_server::EngineEventsServer<EngineEventsV2Skeleton>
{
    proto_v2::engine_events_server::EngineEventsServer::new(EngineEventsV2Skeleton)
}

/// `SetKillSwitch` 的 **source → scope → level 闭世界契约**。
///
/// 三条自动源的 scope 与档位由 `03-SAFETY-MODEL.md` §4.2 与各自的裁决固定：
/// - `auto_llm_failures`：`bot:*` + `pause_new`（只影响开仓，最小 scope）；
/// - `auto_reconcile_diff`：`cred:*` + `flatten`（连续对账差异是账户级事实）；
/// - 引擎侧自判的两个源（行情中断 / 订单拒绝率）**根本不走本 RPC**，
///   外部冒用一律拒绝。
///
/// 抽成纯函数是为了让"改一格就变红"成立：任何放宽都会被下面的用例逐条打红。
pub(crate) fn validate_kill_switch_trigger_contract(
    trigger_source: &str,
    scope_key: &str,
    level: crate::kill_switch::Level,
) -> Result<(), String> {
    let scope_is = |prefix: &str| scope_key.starts_with(prefix) && scope_key.len() > prefix.len();
    match trigger_source {
        "manual_api" => Ok(()),
        "auto_reconcile_diff" => {
            if !scope_is("cred:") {
                return Err("auto_reconcile_diff may only arm a cred: scope".to_string());
            }
            if level != crate::kill_switch::Level::Flatten {
                return Err("auto_reconcile_diff may only arm flatten".to_string());
            }
            Ok(())
        }
        source if source == crate::auto_triggers::TRIGGER_SOURCE_LLM_FAILURES => {
            if !scope_is("bot:") {
                return Err("auto_llm_failures may only arm a bot: scope".to_string());
            }
            if level != crate::kill_switch::Level::PauseNew {
                return Err("auto_llm_failures may only arm pause_new".to_string());
            }
            Ok(())
        }
        // **P4b（#35 D3/D1）· 会员源：仅 cred:、仅 pause_new/flatten。**
        // member_api 不进入 `source_is_manual`——global 的人工特权不向会员
        // 开放（W3 判据在 arm 层再核一次；变异 M2 的双防线之一）。
        crate::kill_switch::TRIGGER_SOURCE_MEMBER_API => {
            if !scope_is("cred:") {
                return Err("member_api may only arm a cred: scope".to_string());
            }
            if level != crate::kill_switch::Level::PauseNew
                && level != crate::kill_switch::Level::Flatten
            {
                return Err("member_api may only arm pause_new or flatten".to_string());
            }
            Ok(())
        }
        _ => Err(
            "trigger_source must be manual_api, auto_reconcile_diff, auto_llm_failures, or member_api"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    include!("svc_v2_kill_switch_read_tests.rs");
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::Once;
    use std::sync::{Arc, Mutex};

    use sqlx::Row;
    use tonic::Request;

    use super::{
        paper_ledger_truth, reconcile_now_response, unavailable_account, EngineExecution,
        EngineExecutionV2Service, UNFUNDED_ACCOUNT_STATE_REASON,
    };
    use crate::proto_v2::{
        DataSnapshotDecision, ExecuteIntentsRequest, FundSimulationAccountRequest,
        GetAccountStateRequest, GetOrderByClientOrderIdRequest, GetPositionStatesRequest,
        OrderIntent, Protection, PutPromotionCertificationRequest, PutRiskBudgetRequest,
        ReconcileNowRequest, ReleasePendingRequest, ResetKillSwitchRequest, SetKillSwitchRequest,
        Status,
    };
    use crate::store::Store;

    const F15_BASE_MS: i64 = 1_800_000_000_000;

    /// **常规挂单为空与算法挂单为空是两位互不蕴含的真值。**
    ///
    /// `reconcile.rs` 拉两份独立的交易所清单（`open_orders` 与
    /// `open_algo_orders`）分别算出这两位；`reconcile_now` 的投影必须原样
    /// 各带各的。四种组合逐一钉死，任何"把一位复用给另一位"（`a = b`）
    /// 或"给算法单那位写死常量"的改法都至少打红两条断言——写死 `true`
    /// 被后两例抓住，写死 `false` 被前两例抓住，互相赋值被中间两例抓住。
    #[test]
    fn reconcile_now_carries_normal_and_algo_emptiness_independently() {
        for (all_open_orders_empty, all_open_algo_orders_empty) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let response = reconcile_now_response(crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty,
                all_open_algo_orders_empty,
                local_position_zero_diff: true,
            });
            assert_eq!(
                response.all_open_orders_empty, all_open_orders_empty,
                "normal-open emptiness must survive the projection unchanged \
                 (normal={all_open_orders_empty}, algo={all_open_algo_orders_empty})"
            );
            assert_eq!(
                response.all_open_algo_orders_empty, all_open_algo_orders_empty,
                "algo-open emptiness must survive the projection unchanged and must not \
                 be derived from the normal-open bit \
                 (normal={all_open_orders_empty}, algo={all_open_algo_orders_empty})"
            );
        }
    }

    /// **五段真值一段都不丢，`false` 也照原样送出去。**
    ///
    /// 全 `false` 的一份 truth 是最危险的形状：任何 `unwrap_or_default` /
    /// `or` 兜底 / 漏赋值都会把它变成"看起来正常"。这里逐位比对，同时钉住
    /// `accepted`/`completed` 是常量 `true`——它们表达的是"这次 RPC 跑完了"，
    /// 不是"账对平了"，不得被拿来当零差异判据（engine safety contract）。
    #[test]
    fn reconcile_now_projection_preserves_every_false_truth_bit() {
        let response = reconcile_now_response(crate::kill_switch::ReconcileTruth {
            has_diff: true,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: false,
        });
        assert!(response.accepted);
        assert!(response.completed);
        assert!(response.has_diff);
        assert!(!response.all_positions_zero);
        assert!(!response.all_open_orders_empty);
        assert!(!response.all_open_algo_orders_empty);
        assert!(!response.local_position_zero_diff);
        assert_eq!(response.schema_version, 1);

        let clean = reconcile_now_response(crate::kill_switch::ReconcileTruth {
            has_diff: false,
            all_positions_zero: true,
            all_open_orders_empty: true,
            all_open_algo_orders_empty: true,
            local_position_zero_diff: true,
        });
        assert!(!clean.has_diff);
        assert!(clean.all_positions_zero);
        assert!(clean.all_open_orders_empty);
        assert!(clean.all_open_algo_orders_empty);
        assert!(clean.local_position_zero_diff);
    }

    #[test]
    fn unfunded_account_reason_matches_cross_language_fixture() {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Fixture {
            unfunded_account_state: String,
        }

        // Cross-language contract: TypeScript and Python load this same fixture path.
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../tests/fixtures/product-demo-account-state-reasons.json"
        ))
        .expect("account-state reason fixture must be valid JSON");
        let response = unavailable_account(UNFUNDED_ACCOUNT_STATE_REASON);

        assert_eq!(response.unavailable_reason, fixture.unfunded_account_state);
    }

    #[test]
    fn all_simulation_identity_call_sites_use_the_shared_allowlist() {
        let source = include_str!("svc_v2.rs");
        let shared_call = ["crate::venue::is_simulation_credential", "(credential_id)"].concat();
        let legacy_equal = ["credential_id == ", "\"paper\""].concat();
        let legacy_not_equal = ["credential_id != ", "\"paper\""].concat();

        assert_eq!(source.matches(&shared_call).count(), 6);
        assert!(!source.contains(&legacy_equal));
        assert!(!source.contains(&legacy_not_equal));
    }

    struct ManualClock {
        now_ms: AtomicI64,
    }

    impl ManualClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: AtomicI64::new(now_ms),
            }
        }

        fn set(&self, now_ms: i64) {
            self.now_ms.store(now_ms, Ordering::SeqCst);
        }
    }

    impl crate::svc::Clock for ManualClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }
    }

    #[derive(Default)]
    struct RecoveryFailureAdapter {
        flatten_calls: AtomicUsize,
        open_ids: Mutex<HashSet<String>>,
    }

    #[tonic::async_trait]
    impl crate::protection::ProtectionAdapter for RecoveryFailureAdapter {
        async fn query_algo_order(
            &self,
            exchange_algo_id: Option<&str>,
            client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<crate::protection::AlgoOrderTruth>> {
            let identity = exchange_algo_id.or(client_algo_id).unwrap_or_default();
            if !self.open_ids.lock().expect("open ids").contains(identity) {
                return Ok(None);
            }
            Ok(Some(crate::protection::AlgoOrderTruth {
                // A clientAlgoId lookup returns the venue's exact algoId.
                // Use a stable fake id when this test queried by client id.
                algo_id: Some(crate::binance_futures::BinanceAlgoId::new(
                    exchange_algo_id
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(42_001),
                )),
                client_algo_id: client_algo_id
                    .map(crate::binance_futures::BinanceClientAlgoId::new),
                algo_status: Some("NEW".to_string()),
                actual_order_id: None,
                updated_at_ms: Some(1),
            }))
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flatten_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    static INSTALL_DRIVERS: Once = Once::new();

    async fn empty_execution_fixture(label: &str) -> (Store, std::path::PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-v2-execute-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect v2 execution store");
        (store, path)
    }

    async fn execution_fixture(label: &str) -> (Store, std::path::PathBuf) {
        let (store, path) = empty_execution_fixture(label).await;
        let now = crate::svc::now_ms();
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES ('paper', ?, '10000', '10000', '0', 0, 0, 0, 0,
                      '{"majors":0,"l1_alts":0}', 'none', '5000')
            "#,
        )
        .bind(now.to_string())
        .execute(store.pool())
        .await
        .expect("fresh budget");
        (store, path)
    }

    fn protected_intent(label: &str, stop: &str, tp: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: format!("bot_{label}"),
            run_id: "run_v2".to_string(),
            cycle_id: format!("cycle_{label}"),
            idempotency_key: format!("client_{label}"),
            client_order_id_short: format!("coid_{label}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: "open_long".to_string(),
            order_type: "limit".to_string(),
            limit_price: "100".to_string(),
            reduce_only: false,
            intent_id: format!("int_{label}"),
            max_slippage_bps: 15,
            protection: Some(Protection {
                required: true,
                stop_loss_price: stop.to_string(),
                take_profit_price: tp.to_string(),
                confirm_timeout_ms: 3_000,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: crate::svc::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        }
    }

    async fn execute_v2(
        service: &EngineExecutionV2Service,
        intent: OrderIntent,
    ) -> crate::proto_v2::IntentAck {
        execute_v2_with_snapshot(service, intent, Some(snapshot("pass", true, true))).await
    }

    async fn seed_authoritative_long_position(store: &Store, bot_uid: &str) {
        let position_id = format!("pos_{bot_uid}");
        let verdict_id = format!("rv_{bot_uid}");
        let now = crate::svc::now_ms();
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, entry_price,
              updated_at_ms
            ) VALUES (?, 'paper', ?, 'binance|futures|BTC|USDT|USDT', 'long',
                      1, 0, 'open', ?, '1', '100', ?)
            "#,
        )
        .bind(&position_id)
        .bind(bot_uid)
        .bind(format!("{bot_uid}|binance|futures|BTC|USDT|USDT|long|1"))
        .bind(now.to_string())
        .execute(store.pool())
        .await
        .expect("seed authoritative close position");
        sqlx::query(
            r#"
            INSERT INTO risk_verdicts (
              risk_verdict_id, intent_id, approved_intent_key, credential_id,
              bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
              decision, reject_reason_code, sizing_json, rules_json,
              budget_as_of_ms, evaluator_version
            ) VALUES (?, ?, ?, 'paper', ?, 'binance|futures|BTC|USDT|USDT', ?,
                      'risk_policy_v3', 'sha256:fixture', 'approve', NULL,
                      '{"final_notional_usdt":"100"}', '[]', ?, 'fixture')
            "#,
        )
        .bind(&verdict_id)
        .bind(format!("int_entry_{bot_uid}"))
        .bind(format!("int_entry_{bot_uid}"))
        .bind(bot_uid)
        .bind(now.to_string())
        .bind(now.to_string())
        .execute(store.pool())
        .await
        .expect("seed durable entry sizing verdict");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, position_id,
              risk_verdict_id, created_at_ms, updated_at_ms
            ) VALUES (?, 'paper', ?, ?, 'binance', 'futures',
                      'binance|futures|BTC|USDT|USDT', 'open_long', 'long',
                      'market', '1', 0, 'entry', 'filled', ?, ?, ?, ?)
            "#,
        )
        .bind(format!("eng_entry_{bot_uid}"))
        .bind(bot_uid)
        .bind(format!("idem_entry_{bot_uid}"))
        .bind(&position_id)
        .bind(&verdict_id)
        .bind(now.to_string())
        .bind(now.to_string())
        .execute(store.pool())
        .await
        .expect("seed durable entry order");
        sqlx::query(
            r#"
            INSERT INTO notional_reservations (
              credential_id, reserved_usdt, cap_usdt, updated_at_ms
            ) VALUES ('paper', '100', '5000', ?)
            "#,
        )
        .bind(now.to_string())
        .execute(store.pool())
        .await
        .expect("seed durable paper notional reservation");
    }

    fn snapshot(gate: &str, allow_open: bool, allow_close: bool) -> DataSnapshotDecision {
        DataSnapshotDecision {
            snapshot_id: format!("snap_{}", "a".repeat(32)),
            quality_gate: gate.to_string(),
            allow_open,
            allow_close,
            created_at_ms: crate::svc::now_ms(),
            schema_version: 1,
        }
    }

    async fn execute_v2_with_snapshot(
        service: &EngineExecutionV2Service,
        intent: OrderIntent,
        data_snapshot: Option<DataSnapshotDecision>,
    ) -> crate::proto_v2::IntentAck {
        service
            .execute_intents(Request::new(ExecuteIntentsRequest {
                credential_id: "paper".to_string(),
                run_mode: "paper".to_string(),
                intents: vec![intent],
                data_snapshot,
                schema_version: 1,
            }))
            .await
            .expect("execute v2 intent")
            .into_inner()
            .acks
            .into_iter()
            .next()
            .expect("one v2 ack")
    }

    async fn seed_f15_budget(store: &Store, as_of_ms: i64) {
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES ('paper', ?, '10000', '10000', '0', 0, 0, 0, 0,
                      '{"majors":0,"l1_alts":0}', 'none', '5000')
            ON CONFLICT(credential_id) DO UPDATE SET as_of_ms=excluded.as_of_ms
            "#,
        )
        .bind(as_of_ms.to_string())
        .execute(store.pool())
        .await
        .expect("seed F15 risk budget");
    }

    fn f15_snapshot(label: &str, created_at_ms: i64) -> DataSnapshotDecision {
        DataSnapshotDecision {
            snapshot_id: format!("snap_{}", &crate::svc::sha256_hex(label)[..32]),
            quality_gate: "pass".to_string(),
            allow_open: true,
            allow_close: true,
            created_at_ms,
            schema_version: 1,
        }
    }

    fn f15_intent(label: &str, valid_until_ms: i64) -> OrderIntent {
        let mut intent = protected_intent(label, "95", "111");
        intent.valid_until_ms = valid_until_ms;
        intent
    }

    async fn f15_verdict_clock(store: &Store, intent_id: &str) -> i64 {
        let replay_input = sqlx::query(
            "SELECT replay_input_json FROM risk_verdicts WHERE intent_id=? ORDER BY created_at_ms DESC LIMIT 1",
        )
        .bind(intent_id)
        .fetch_one(store.pool())
        .await
        .expect("load F15 replay input")
        .get::<String, _>("replay_input_json");
        serde_json::from_str::<serde_json::Value>(&replay_input).expect("decode F15 replay input")
            ["input"]["now_ms"]
            .as_i64()
            .expect("F15 replay input carries integer now_ms")
    }

    async fn assert_f15_clock_drift(
        label: &str,
        injected_drift_ms: i64,
        expected_clock_ms: i64,
        expected_boundary_status: i32,
        expected_boundary_reject: &str,
    ) {
        let (decision_store, decision_path) =
            empty_execution_fixture(&format!("f15-{label}-decision")).await;
        seed_f15_budget(&decision_store, expected_clock_ms).await;
        let decision_clock = Arc::new(ManualClock::new(F15_BASE_MS));
        decision_clock.set(F15_BASE_MS.saturating_add(injected_drift_ms));
        let decision_service =
            EngineExecutionV2Service::with_execution_clock(decision_store.clone(), decision_clock);

        let stable = f15_intent(&format!("f15_{label}_stable"), F15_BASE_MS + 90_000);
        let stable_intent_id = stable.intent_id.clone();
        let stable_ack = execute_v2_with_snapshot(
            &decision_service,
            stable,
            Some(f15_snapshot(&format!("{label}_stable"), F15_BASE_MS)),
        )
        .await;
        assert_eq!(stable_ack.status, Status::Accepted as i32);
        assert_eq!(stable_ack.reject_reason_code, "");

        let expired = f15_intent(&format!("f15_{label}_expired"), F15_BASE_MS - 90_000);
        let expired_intent_id = expired.intent_id.clone();
        let expired_ack = execute_v2_with_snapshot(
            &decision_service,
            expired,
            Some(f15_snapshot(&format!("{label}_expired"), F15_BASE_MS)),
        )
        .await;
        assert_eq!(expired_ack.status, Status::Rejected as i32);
        assert_eq!(expired_ack.reject_reason_code, "INTENT_EXPIRED");

        let (boundary_store, boundary_path) =
            empty_execution_fixture(&format!("f15-{label}-boundary")).await;
        seed_f15_budget(&boundary_store, expected_clock_ms).await;
        let boundary_clock = Arc::new(ManualClock::new(F15_BASE_MS));
        boundary_clock.set(F15_BASE_MS.saturating_add(injected_drift_ms));
        let boundary_service =
            EngineExecutionV2Service::with_execution_clock(boundary_store.clone(), boundary_clock);
        let boundary = f15_intent(&format!("f15_{label}_boundary"), F15_BASE_MS + 30_000);
        let boundary_intent_id = boundary.intent_id.clone();
        let boundary_ack = execute_v2_with_snapshot(
            &boundary_service,
            boundary,
            Some(f15_snapshot(&format!("{label}_boundary"), F15_BASE_MS)),
        )
        .await;
        assert_eq!(boundary_ack.status, expected_boundary_status);
        assert_eq!(boundary_ack.reject_reason_code, expected_boundary_reject);

        let observed_clocks = vec![
            f15_verdict_clock(&decision_store, &stable_intent_id).await,
            f15_verdict_clock(&decision_store, &expired_intent_id).await,
            f15_verdict_clock(&boundary_store, &boundary_intent_id).await,
        ];
        assert_eq!(
            observed_clocks,
            vec![expected_clock_ms, expected_clock_ms, expected_clock_ms]
        );
        drop(decision_service);
        decision_store.pool().close().await;
        let _ = std::fs::remove_file(decision_path);
        drop(boundary_service);
        boundary_store.pool().close().await;
        let _ = std::fs::remove_file(boundary_path);

        let (idem_store, idem_path) =
            empty_execution_fixture(&format!("f15-{label}-idempotency")).await;
        seed_f15_budget(&idem_store, F15_BASE_MS).await;
        let idem_clock = Arc::new(ManualClock::new(F15_BASE_MS));
        let idem_service =
            EngineExecutionV2Service::with_execution_clock(idem_store.clone(), idem_clock.clone());
        let first = f15_intent(&format!("f15_{label}_idem_first"), F15_BASE_MS + 180_000);
        let bot_uid = first.bot_uid.clone();
        let symbol_key = first.symbol_key.clone();
        let first_ack = execute_v2_with_snapshot(
            &idem_service,
            first.clone(),
            Some(f15_snapshot(&format!("{label}_idem_first"), F15_BASE_MS)),
        )
        .await;
        assert_eq!(first_ack.status, Status::Accepted as i32);

        let authority_key = crate::position_state::semantic_idempotency_key(&format!(
            "entry|{bot_uid}|{symbol_key}|long|1"
        ));
        sqlx::query(
            r#"
            UPDATE position_state
            SET state='pending', pending_intent_id=?, pending_idem_key=?,
                qty='0', entry_price=NULL, opened_at_ms=NULL, updated_at_ms=?
            WHERE bot_uid=? AND symbol_key=? AND direction='long'
            "#,
        )
        .bind(&first.intent_id)
        .bind(&authority_key)
        .bind(F15_BASE_MS)
        .bind(&bot_uid)
        .bind(&symbol_key)
        .execute(idem_store.pool())
        .await
        .expect("restore the durable pre-fill retry boundary");

        seed_f15_budget(&idem_store, expected_clock_ms).await;
        idem_clock.set(F15_BASE_MS.saturating_add(injected_drift_ms));
        let mut replay = first;
        replay.run_id = format!("run_f15_{label}_replay");
        replay.cycle_id = format!("cycle_f15_{label}_replay");
        replay.idempotency_key = format!("client_f15_{label}_replay");
        replay.client_order_id_short = format!("coid_f15_{label}_replay");
        replay.intent_id = format!("int_f15_{label}_replay");
        let replay_intent_id = replay.intent_id.clone();
        let replay_client_key = replay.idempotency_key.clone();
        let replay_ack = execute_v2_with_snapshot(
            &idem_service,
            replay,
            Some(f15_snapshot(&format!("{label}_idem_replay"), F15_BASE_MS)),
        )
        .await;
        assert_eq!(replay_ack.status, Status::Duplicate as i32);
        assert_eq!(replay_ack.idempotency_key, replay_client_key);
        assert_eq!(
            replay_ack.message,
            format!(
                "duplicate; paper execution not repeated; \
                 authoritative_idempotency_key={authority_key}"
            )
        );

        let entry_keys = sqlx::query(
            "SELECT idempotency_key FROM engine_orders WHERE bot_uid=? AND order_role='entry' ORDER BY idempotency_key",
        )
        .bind(&bot_uid)
        .fetch_all(idem_store.pool())
        .await
        .expect("load F15 entry orders")
        .into_iter()
        .map(|row| row.get::<String, _>("idempotency_key"))
        .collect::<Vec<_>>();
        assert_eq!(entry_keys, vec![authority_key]);
        assert_eq!(
            f15_verdict_clock(&idem_store, &replay_intent_id).await,
            expected_clock_ms
        );
        drop(idem_service);
        idem_store.pool().close().await;
        let _ = std::fs::remove_file(idem_path);
    }

    #[tokio::test]
    async fn f15_clock_drift_plus_5s_uses_execution_clock_and_preserves_idempotency() {
        assert_f15_clock_drift(
            "plus_5s",
            5_000,
            F15_BASE_MS + 5_000,
            Status::Accepted as i32,
            "",
        )
        .await;
    }

    #[tokio::test]
    async fn f15_clock_drift_minus_5s_uses_execution_clock_and_preserves_idempotency() {
        assert_f15_clock_drift(
            "minus_5s",
            -5_000,
            F15_BASE_MS - 5_000,
            Status::Accepted as i32,
            "",
        )
        .await;
    }

    #[tokio::test]
    async fn f15_clock_drift_plus_60s_expires_boundary_and_preserves_idempotency() {
        assert_f15_clock_drift(
            "plus_60s",
            60_000,
            F15_BASE_MS + 60_000,
            Status::Rejected as i32,
            "INTENT_EXPIRED",
        )
        .await;
    }

    #[tokio::test]
    async fn f15_clock_drift_minus_60s_uses_execution_clock_and_preserves_idempotency() {
        assert_f15_clock_drift(
            "minus_60s",
            -60_000,
            F15_BASE_MS - 60_000,
            Status::Accepted as i32,
            "",
        )
        .await;
    }

    #[tokio::test]
    async fn v2_missing_snapshot_rejects_open_but_allows_close() {
        let (store, path) = execution_fixture("snapshot-missing").await;
        let service = EngineExecutionV2Service::new(store.clone());

        let open = execute_v2_with_snapshot(
            &service,
            protected_intent("snapshot_missing_open", "95", "111"),
            None,
        )
        .await;
        assert_eq!(open.status, Status::Rejected as i32);
        assert_eq!(open.reject_reason_code, "DATA_QUALITY_FAIL");

        let mut close = protected_intent("snapshot_missing_close", "95", "111");
        close.action = "close_long".to_string();
        close.reduce_only = true;
        close.protection = None;
        seed_authoritative_long_position(&store, &close.bot_uid).await;
        let close = execute_v2_with_snapshot(&service, close, None).await;
        assert_eq!(close.status, Status::Accepted as i32, "{}", close.message);

        let order_count = sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count accepted close order")
            .get::<i64, _>("n");
        assert_eq!(
            order_count, 2,
            "only the authoritative entry fixture and accepted close may exist"
        );
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_snapshot_gate_and_allow_open_mismatch_fails_closed() {
        let (store, path) = execution_fixture("snapshot-mismatch").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2_with_snapshot(
            &service,
            protected_intent("snapshot_mismatch", "95", "111"),
            Some(snapshot("pass", false, true)),
        )
        .await;
        assert_eq!(ack.status, Status::Rejected as i32);
        assert_eq!(ack.reject_reason_code, "DATA_SNAPSHOT_INCONSISTENT");
        let rules = sqlx::query(
            "SELECT rules_json FROM risk_verdicts WHERE intent_id='int_snapshot_mismatch'",
        )
        .fetch_one(store.pool())
        .await
        .expect("mismatch verdict")
        .get::<String, _>("rules_json");
        assert!(rules.contains("quality_gate=pass is inconsistent with allow_open=false"));

        let reverse = execute_v2_with_snapshot(
            &service,
            protected_intent("snapshot_mismatch_reverse", "95", "111"),
            Some(snapshot("fail", true, true)),
        )
        .await;
        assert_eq!(reverse.status, Status::Rejected as i32);
        assert_eq!(reverse.reject_reason_code, "DATA_SNAPSHOT_INCONSISTENT");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_allow_close_false_rejects_open_but_never_close() {
        let (store, path) = execution_fixture("allow-close-false").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let inconsistent = snapshot("pass", true, false);
        let open = execute_v2_with_snapshot(
            &service,
            protected_intent("allow_close_false_open", "95", "111"),
            Some(inconsistent.clone()),
        )
        .await;
        assert_eq!(open.status, Status::Rejected as i32);
        assert_eq!(open.reject_reason_code, "DATA_SNAPSHOT_INCONSISTENT");

        let mut close = protected_intent("allow_close_false_close", "95", "111");
        close.action = "close_long".to_string();
        close.reduce_only = true;
        close.protection = None;
        seed_authoritative_long_position(&store, &close.bot_uid).await;
        let close = execute_v2_with_snapshot(&service, close, Some(inconsistent)).await;
        assert_eq!(close.status, Status::Accepted as i32, "{}", close.message);
        let rules = sqlx::query(
            "SELECT rules_json FROM risk_verdicts WHERE intent_id='int_allow_close_false_close'",
        )
        .fetch_one(store.pool())
        .await
        .expect("close consistency audit")
        .get::<String, _>("rules_json");
        assert!(rules.contains("allow_close=false violates the close-always-open invariant"));
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_invalid_snapshot_id_is_treated_as_absent() {
        let (store, path) = execution_fixture("snapshot-id-invalid").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut invalid = snapshot("pass", true, true);
        invalid.snapshot_id = "snap_NOT_CONTENT_ADDRESSED".to_string();
        let ack = execute_v2_with_snapshot(
            &service,
            protected_intent("snapshot_id_invalid", "95", "111"),
            Some(invalid),
        )
        .await;
        assert_eq!(ack.status, Status::Rejected as i32);
        assert_eq!(ack.reject_reason_code, "DATA_QUALITY_FAIL");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_rejected_verdict_emits_auditable_replay_event() {
        let (store, path) = execution_fixture("reject-audit-event").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut invalid = snapshot("fail", false, true);
        invalid.snapshot_id = format!("snap_{}", "a".repeat(32));
        let ack = execute_v2_with_snapshot(
            &service,
            protected_intent("reject_audit_event", "95", "111"),
            Some(invalid),
        )
        .await;
        assert_eq!(ack.status, Status::Rejected as i32);

        let payload = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type='reject' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("rejected verdict event")
        .get::<Vec<u8>, _>("payload_json");
        let payload: serde_json::Value =
            serde_json::from_slice(&payload).expect("decode rejected verdict payload");
        assert_eq!(payload["riskVerdict"]["decision"], "reject");
        assert_eq!(payload["riskVerdict"]["intentId"], "int_reject_audit_event");
        assert_eq!(payload["intentId"], "int_reject_audit_event");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_approved_verdict_emits_transactional_replay_event() {
        let (store, path) = execution_fixture("approve-audit-event").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2(
            &service,
            protected_intent("approve_audit_event", "95", "111"),
        )
        .await;
        assert_eq!(ack.status, Status::Accepted as i32, "{}", ack.message);

        let payload = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type='risk_verdict' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("approved verdict ack event")
        .get::<Vec<u8>, _>("payload_json");
        let payload: serde_json::Value =
            serde_json::from_slice(&payload).expect("decode approved verdict payload");
        assert!(matches!(
            payload["riskVerdict"]["decision"].as_str(),
            Some("approve" | "resize")
        ));
        assert_eq!(payload["intentId"], "int_approve_audit_event");
        assert_eq!(payload["orderRole"], "entry");
        assert!(payload["positionId"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_typed_protection_uses_risk_budget_sizing_and_auto_attaches_once() {
        let (store, path) = execution_fixture("approve").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2(&service, protected_intent("approve", "95", "111")).await;
        assert_eq!(ack.status, Status::Accepted as i32, "{}", ack.message);
        assert!(!ack.risk_verdict_id.is_empty());
        let verdict = sqlx::query(
            "SELECT sizing_json FROM risk_verdicts WHERE intent_id='int_approve' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("typed verdict");
        let sizing: serde_json::Value =
            serde_json::from_str(&verdict.get::<String, _>("sizing_json")).expect("sizing json");
        assert_ne!(sizing["mode"], "legacy_client_quantity");
        assert!(sizing["capped_by"].is_array());

        let rows = sqlx::query(
            r#"
            SELECT eo.idempotency_key, po.kind, po.verified_from_exchange
            FROM protection_orders po
            JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
            ORDER BY po.kind
            "#,
        )
        .fetch_all(store.pool())
        .await
        .expect("protection rows");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| {
            row.get::<String, _>("kind") == "sl"
                && row
                    .get::<String, _>("idempotency_key")
                    .starts_with("prot|pos_")
                && row.get::<String, _>("idempotency_key").ends_with("|sl|1")
        }));
        assert!(rows
            .iter()
            .all(|row| row.get::<i64, _>("verified_from_exchange") == 1));
        let position_id = sqlx::query("SELECT position_id FROM position_state LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<String, _>("position_id");
        service
            .shared_execution
            .submit_planned_protection(&position_id, "paper")
            .await
            .expect("idempotent recovery trigger");
        let count = sqlx::query("SELECT COUNT(*) AS n FROM protection_orders")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n");
        assert_eq!(count, 2);
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn assert_v2_reject(label: &str, stop: &str, tp: &str, expected: &str) {
        let (store, path) = execution_fixture(label).await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2(&service, protected_intent(label, stop, tp)).await;
        assert_eq!(ack.status, Status::Rejected as i32, "label={label}");
        assert_eq!(ack.reject_reason_code, expected, "label={label}");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_missing_protection_is_rejected() {
        assert_v2_reject("missing", "", "", "MISSING_PROTECTION").await;
    }

    #[tokio::test]
    async fn v2_rr_too_low_is_rejected() {
        assert_v2_reject("rr", "95", "101", "RR_TOO_LOW").await;
    }

    #[tokio::test]
    async fn v2_market_cost_gate_uses_notional_units_and_records_new_evaluator() {
        let (store, path) = execution_fixture("cost-notional-units").await;
        let now = crate::svc::now_ms();
        sqlx::query(
            "INSERT INTO engine_positions (credential_id,exchange,market_kind,symbol,symbol_key,position_amt,entry_price,mark_price,leverage,unrealized_pnl,updated_at_ms) VALUES ('paper','binance','futures','BTCUSDT','binance|futures|BTC|USDT|USDT','0','0','100','20','0',?)",
        ).bind(now).execute(store.pool()).await.unwrap();
        let service = EngineExecutionV2Service::new(store.clone());
        let mut intent = protected_intent("cost_notional_units", "99", "103");
        // Force the actual market-intent path to use the persisted reference
        // and observed leverage; a limit-price test silently uses leverage 1.
        intent.order_type = "market".to_string();
        intent.limit_price.clear();
        let ack = execute_v2(&service, intent).await;
        assert_eq!(ack.status, Status::Accepted as i32, "{}", ack.message);
        let row = sqlx::query("SELECT sizing_json,rules_json,evaluator_version,replay_input_json FROM risk_verdicts WHERE intent_id='int_cost_notional_units'").fetch_one(store.pool()).await.unwrap();
        let sizing: serde_json::Value =
            serde_json::from_str(&row.get::<String, _>("sizing_json")).unwrap();
        let rules: serde_json::Value =
            serde_json::from_str(&row.get::<String, _>("rules_json")).unwrap();
        let replay: serde_json::Value =
            serde_json::from_str(&row.get::<String, _>("replay_input_json")).unwrap();
        assert_eq!(replay["input"]["intent"]["requested_leverage"], 20);
        assert_eq!(
            sizing["leverage"], 10,
            "the leverage cap still applies to sizing"
        );
        assert_eq!(
            sizing["final_notional_usdt"], "1000",
            "the position cap must remain unchanged"
        );
        assert!(rules
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["rule"] == "cost_aware_tp" && r["result"] == "pass"));
        assert_eq!(
            row.get::<String, _>("evaluator_version"),
            "risk-policy@1.1.0"
        );
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_tp_below_cost_is_rejected() {
        assert_v2_reject("cost", "99.99", "100.03", "TP_BELOW_COST").await;
    }

    #[tokio::test]
    async fn v2_expired_intent_is_rejected() {
        let (store, path) = execution_fixture("expired").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut expired = protected_intent("expired", "95", "111");
        expired.valid_until_ms = crate::svc::now_ms() - 1;
        let ack = execute_v2(&service, expired).await;
        assert_eq!(ack.status, Status::Rejected as i32);
        assert_eq!(ack.reject_reason_code, "INTENT_EXPIRED");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_stop_distance_is_capped_on_the_execution_path() {
        let (store, path) = execution_fixture("clamp").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2(&service, protected_intent("clamp", "50", "220")).await;
        assert_eq!(ack.status, Status::Accepted as i32, "{}", ack.message);
        let verdict = sqlx::query(
            "SELECT sizing_json, rules_json FROM risk_verdicts WHERE intent_id='int_clamp' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert!(verdict
            .get::<String, _>("sizing_json")
            .contains("stop_distance_clamp"));
        assert!(verdict
            .get::<String, _>("rules_json")
            .contains("\"result\":\"capped\""));
        let effective_stop =
            sqlx::query("SELECT stop_loss_price FROM position_protection_plans LIMIT 1")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<String, _>("stop_loss_price");
        assert_eq!(effective_stop, "94");
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn recovery_f3_keeps_confirmed_sl_and_f4_repairs_missing_sl() {
        let (store, path) = execution_fixture("f3-f4").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let ack = execute_v2(&service, protected_intent("f3-f4", "95", "111")).await;
        assert_eq!(ack.status, Status::Accepted as i32);
        let position_id = sqlx::query("SELECT position_id FROM position_state LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<String, _>("position_id");

        let truth_adapter = Arc::new(RecoveryFailureAdapter::default());
        let persisted_client_algo_id = sqlx::query(
            "SELECT client_algo_id FROM protection_orders WHERE position_id=? AND kind='sl' LIMIT 1",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("client_algo_id");
        truth_adapter
            .open_ids
            .lock()
            .expect("open ids")
            .insert(persisted_client_algo_id);
        let recovery_service = EngineExecutionV2Service::with_protection_truth_adapter(
            store.clone(),
            truth_adapter.clone(),
        );

        recovery_service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: "paper".to_string(),
                reason: "F3".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("F3 reconcile");
        let f3_count =
            sqlx::query("SELECT COUNT(*) AS n FROM protection_orders WHERE position_id=?")
                .bind(&position_id)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<i64, _>("n");
        assert_eq!(f3_count, 2, "F3 must not duplicate protection");
        let f3_state = sqlx::query("SELECT state FROM position_state WHERE position_id=?")
            .bind(&position_id)
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<String, _>("state");
        assert_eq!(f3_state, "protected");
        let recovery_truth = sqlx::query(
            "SELECT last_verified_at_ms FROM protection_orders WHERE position_id=? AND kind='sl' AND generation=1",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(recovery_truth.get::<i64, _>("last_verified_at_ms"), 1);
        let recovery_event = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type='protection_state' LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("recovery truth transition event");
        let recovery_payload: serde_json::Value =
            serde_json::from_slice(&recovery_event.get::<Vec<u8>, _>("payload_json")).unwrap();
        assert_eq!(recovery_payload["truth_source"], "rest_algo_query");
        assert_eq!(recovery_payload["observation_context"], "recovery");

        truth_adapter.open_ids.lock().expect("open ids").clear();

        sqlx::query("UPDATE position_state SET state='open' WHERE position_id=?")
            .bind(&position_id)
            .execute(store.pool())
            .await
            .unwrap();
        recovery_service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: "paper".to_string(),
                reason: "F4".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("F4 repair reconcile");
        let repaired = sqlx::query(
            "SELECT COUNT(*) AS n FROM protection_orders WHERE position_id=? AND kind='sl' AND generation=2 AND verified_from_exchange=1",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
        assert_eq!(repaired, 1, "F4 must immediately restore one confirmed SL");
        let old_status = sqlx::query(
            "SELECT status FROM protection_orders WHERE position_id=? AND kind='sl' AND generation=1",
        )
        .bind(&position_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<String, _>("status");
        assert_eq!(old_status, "missing_recovery");
        let state = sqlx::query("SELECT state FROM position_state WHERE position_id=?")
            .bind(&position_id)
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<String, _>("state");
        assert_eq!(state, "protected");

        sqlx::query("DELETE FROM protection_orders WHERE position_id=?")
            .bind(&position_id)
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query(
            "DELETE FROM engine_orders WHERE position_id=? AND order_role LIKE 'protection_%'",
        )
        .bind(&position_id)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE position_state SET state='open' WHERE position_id=?")
            .bind(&position_id)
            .execute(store.pool())
            .await
            .unwrap();
        let failure_adapter = Arc::new(RecoveryFailureAdapter::default());
        let failing_service = EngineExecutionV2Service::with_protection_submit_failure(
            store.clone(),
            failure_adapter.clone(),
        );
        let failure = failing_service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: "paper".to_string(),
                reason: "F4 injected repair failure".to_string(),
                schema_version: 1,
            }))
            .await;
        assert!(
            failure.is_err(),
            "F4 repair failure must fail reconciliation: {failure:?}"
        );
        assert_eq!(failure_adapter.flatten_calls.load(Ordering::SeqCst), 1);
        // 修复失败的处置不再是 `pause_new`，
        // 而是一条**未收敛**的 `flatten` latch——只有对账证明仓位归零
        // 才会把 `flatten_state` 写成 `flattened`。
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key='bot:bot_f3-f4'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("level"), "flatten");
        assert_eq!(row.get::<String, _>("flatten_state"), "flattening");
        drop(failing_service);
        drop(recovery_service);
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn paper_reconcile_compares_position_state_to_entry_exit_fill_ledger() {
        let (store, path) = execution_fixture("paper-ledger-truth").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let open = execute_v2(&service, protected_intent("paper_ledger_open", "95", "111")).await;
        assert_eq!(open.status, Status::Accepted as i32, "{}", open.message);

        let open_truth = paper_ledger_truth(&store, "paper")
            .await
            .expect("open paper ledger truth");
        assert_eq!(open_truth.difference_count, 0);
        assert!(open_truth.any_nonzero_position);

        let mut close = protected_intent("paper_ledger_close", "95", "111");
        close.bot_uid = "bot_paper_ledger_open".to_string();
        close.action = "close_long".to_string();
        close.reduce_only = true;
        close.protection = None;
        let close = execute_v2(&service, close).await;
        assert_eq!(close.status, Status::Accepted as i32, "{}", close.message);

        let closed_truth = paper_ledger_truth(&store, "paper")
            .await
            .expect("closed paper ledger truth");
        assert_eq!(closed_truth.difference_count, 0);
        assert!(!closed_truth.any_nonzero_position);
        let reserved = sqlx::query(
            "SELECT reserved_usdt FROM notional_reservations WHERE credential_id='paper'",
        )
        .fetch_one(store.pool())
        .await
        .expect("paper reservation after close")
        .get::<String, _>("reserved_usdt");
        assert!(crate::position_state::is_decimal_zero(&reserved));

        sqlx::query("UPDATE position_state SET qty='1' WHERE credential_id='paper'")
            .execute(store.pool())
            .await
            .expect("inject paper ledger mismatch");
        let mismatch = paper_ledger_truth(&store, "paper")
            .await
            .expect("mismatched paper ledger truth");
        assert_eq!(mismatch.difference_count, 1);
        assert!(mismatch.any_nonzero_position);
        let reconcile = service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: "paper".to_string(),
                reason: "paper ledger mismatch".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("paper mismatch reconciliation")
            .into_inner();
        assert!(reconcile.has_diff);
        assert!(!reconcile.local_position_zero_diff);
        assert!(!reconcile.all_positions_zero);

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    const SECOND_PAPER_CREDENTIAL: &str = "paper:448cc18e49c41178ad7bee0264b88f98";

    async fn arm_kill_switch_rpc(service: &EngineExecutionV2Service, scope_key: &str, level: &str) {
        let armed = service
            .set_kill_switch(Request::new(SetKillSwitchRequest {
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: scope_key.to_string(),
                level: level.to_string(),
                reason: "paper reset wiring".to_string(),
                trigger_source: "manual_api".to_string(),
                actor: "admin".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("arm kill switch through the v2 RPC")
            .into_inner();
        assert!(armed.accepted);
        assert_eq!(armed.level, level);
    }

    async fn reset_kill_switch_rpc(
        service: &EngineExecutionV2Service,
        scope_key: &str,
    ) -> Result<crate::proto_v2::ResetKillSwitchResponse, tonic::Status> {
        service
            .reset_kill_switch(Request::new(ResetKillSwitchRequest {
                reset_authority: None,
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: scope_key.to_string(),
                reset_by: "admin".to_string(),
                schema_version: 1,
            }))
            .await
            .map(|response| response.into_inner())
    }

    async fn assert_reset_refused(service: &EngineExecutionV2Service, scope_key: &str, why: &str) {
        let refused = reset_kill_switch_rpc(service, scope_key)
            .await
            .expect_err(why);
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{why}");
    }

    async fn reconcile_now_rpc(
        service: &EngineExecutionV2Service,
        credential_id: &str,
    ) -> Result<crate::proto_v2::ReconcileNowResponse, tonic::Status> {
        service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: credential_id.to_string(),
                reason: "paper reset wiring".to_string(),
                schema_version: 1,
            }))
            .await
            .map(|response| response.into_inner())
    }

    async fn kill_switch_level(store: &Store, scope_key: &str) -> String {
        sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(scope_key)
            .fetch_one(store.pool())
            .await
            .expect("kill switch state row")
            .get::<String, _>("level")
    }

    /// 本代逐 credential 行里**真的报过真值**的那些（arm 冻结行的
    /// `reconciled_at_ms` 恒为 0）。
    async fn reported_reconcile_rows(store: &Store, scope_key: &str) -> Vec<(String, i64)> {
        sqlx::query(
            "SELECT credential_id, reconcile_clean FROM kill_switch_reconcile_status \
             WHERE scope_key=? AND reconciled_at_ms <> 0 ORDER BY credential_id",
        )
        .bind(scope_key)
        .fetch_all(store.pool())
        .await
        .expect("reconcile status rows")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("credential_id"),
                row.get::<i64, _>("reconcile_clean"),
            )
        })
        .collect()
    }

    async fn seed_second_paper_budget(store: &Store) {
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES (?, ?, '10000', '10000', '0', 0, 0, 0, 0,
                      '{"majors":0,"l1_alts":0}', 'none', '5000')
            "#,
        )
        .bind(SECOND_PAPER_CREDENTIAL)
        .bind(crate::svc::now_ms().to_string())
        .execute(store.pool())
        .await
        .expect("second paper credential budget");
    }

    /// **完整生产入口 wiring proof（engine safety contract）**：`SetKillSwitch` →
    /// `ResetKillSwitch`（拒）→ `ReconcileNow` → `ResetKillSwitch`（过），
    /// 三个都是生产 gRPC 服务的方法，中间不调用任何 kill_switch helper。
    #[tokio::test]
    async fn paper_reconcile_now_records_the_post_arm_fact_reset_requires() {
        for level in ["pause_new", "manage_only"] {
            let (store, path) = execution_fixture("paper-reset-clean").await;
            let service = EngineExecutionV2Service::new(store.clone());
            arm_kill_switch_rpc(&service, "cred:paper", level).await;
            assert_reset_refused(
                &service,
                "cred:paper",
                "an armed paper scope that was never reconciled must not reset",
            )
            .await;

            let reconcile = reconcile_now_rpc(&service, "paper")
                .await
                .expect("clean paper reconcile");
            assert!(reconcile.completed && !reconcile.has_diff);
            assert_eq!(
                reported_reconcile_rows(&store, "cred:paper").await,
                vec![("paper".to_string(), 1)]
            );

            let reset = reset_kill_switch_rpc(&service, "cred:paper")
                .await
                .expect("reset after a same-generation clean paper reconcile");
            assert!(reset.accepted);
            assert_eq!(kill_switch_level(&store, "cred:paper").await, "none");

            drop(service);
            store.pool().close().await;
            let _ = std::fs::remove_file(path);
        }
    }

    /// GAP-B 的原始形状：`bot:<uid>` 在 arm 之前已经有仓位（target 冻结为
    /// 这个 paper credential），`manage_only` 下平仓、对账、reset。
    /// 同一条用例钉住反面：**arm 时一行都没有**的 bot scope target 集为空，
    /// 既有契约下它收敛不了——paper 对账不得替它盖一张无条件 clean 章。
    #[tokio::test]
    async fn paper_reconcile_now_converges_only_bot_scopes_this_credential_covers() {
        let (store, path) = execution_fixture("paper-reset-bot").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let open = execute_v2(&service, protected_intent("paper_reset_bot", "95", "111")).await;
        assert_eq!(open.status, Status::Accepted as i32, "{}", open.message);
        arm_kill_switch_rpc(&service, "bot:bot_paper_reset_bot", "manage_only").await;
        arm_kill_switch_rpc(&service, "bot:bot_never_traded", "pause_new").await;

        let mut close = protected_intent("paper_reset_bot_close", "95", "111");
        close.bot_uid = "bot_paper_reset_bot".to_string();
        close.action = "close_long".to_string();
        close.reduce_only = true;
        close.protection = None;
        let close = execute_v2(&service, close).await;
        assert_eq!(close.status, Status::Accepted as i32, "{}", close.message);
        assert_reset_refused(
            &service,
            "bot:bot_paper_reset_bot",
            "a bot latch must not reset before its credential reconciled",
        )
        .await;

        let reconcile = reconcile_now_rpc(&service, "paper")
            .await
            .expect("clean paper reconcile");
        assert!(!reconcile.has_diff && reconcile.all_positions_zero);
        reset_kill_switch_rpc(&service, "bot:bot_paper_reset_bot")
            .await
            .expect("reset the bot scope this paper credential covers");
        assert_eq!(
            kill_switch_level(&store, "bot:bot_paper_reset_bot").await,
            "none"
        );

        assert_reset_refused(
            &service,
            "bot:bot_never_traded",
            "a bot scope with an empty target set must stay latched",
        )
        .await;
        assert!(reported_reconcile_rows(&store, "bot:bot_never_traded")
            .await
            .is_empty());
        assert_eq!(
            kill_switch_level(&store, "bot:bot_never_traded").await,
            "pause_new"
        );

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn paper_reconcile_now_with_a_real_ledger_diff_keeps_the_latch() {
        let (store, path) = execution_fixture("paper-reset-diff").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let open = execute_v2(&service, protected_intent("paper_reset_diff", "95", "111")).await;
        assert_eq!(open.status, Status::Accepted as i32, "{}", open.message);
        let mut close = protected_intent("paper_reset_diff_close", "95", "111");
        close.bot_uid = "bot_paper_reset_diff".to_string();
        close.action = "close_long".to_string();
        close.reduce_only = true;
        close.protection = None;
        let close = execute_v2(&service, close).await;
        assert_eq!(close.status, Status::Accepted as i32, "{}", close.message);
        arm_kill_switch_rpc(&service, "cred:paper", "pause_new").await;

        sqlx::query("UPDATE position_state SET qty='1' WHERE credential_id='paper'")
            .execute(store.pool())
            .await
            .expect("inject paper ledger mismatch");
        let dirty = reconcile_now_rpc(&service, "paper")
            .await
            .expect("paper reconcile reporting a diff");
        assert!(dirty.has_diff);
        assert_eq!(
            reported_reconcile_rows(&store, "cred:paper").await,
            vec![("paper".to_string(), 0)],
            "the dirty truth itself must be the recorded post-arm fact"
        );
        assert_reset_refused(
            &service,
            "cred:paper",
            "a paper ledger diff must keep the latch",
        )
        .await;

        sqlx::query("UPDATE position_state SET qty='0' WHERE credential_id='paper'")
            .execute(store.pool())
            .await
            .expect("remove paper ledger mismatch");
        let clean = reconcile_now_rpc(&service, "paper")
            .await
            .expect("clean paper reconcile");
        assert!(!clean.has_diff);
        reset_kill_switch_rpc(&service, "cred:paper")
            .await
            .expect("reset once the diff is gone and reconciled again");

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 保护真值查询期间经生产 `SetKillSwitch` 把同一个 scope 升一档
    /// （revision +1）。它发生在这次对账取完 ledger 真值**之后**、投影**之前**。
    struct RearmDuringRecoveryAdapter {
        store: Store,
        inner: RecoveryFailureAdapter,
        rearmed: AtomicUsize,
    }

    #[tonic::async_trait]
    impl crate::protection::ProtectionAdapter for RearmDuringRecoveryAdapter {
        async fn query_algo_order(
            &self,
            exchange_algo_id: Option<&str>,
            client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<crate::protection::AlgoOrderTruth>> {
            if self.rearmed.fetch_add(1, Ordering::SeqCst) == 0 {
                let service = EngineExecutionV2Service::new(self.store.clone());
                arm_kill_switch_rpc(&service, "cred:paper", "manage_only").await;
            }
            self.inner
                .query_algo_order(exchange_algo_id, client_algo_id)
                .await
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.inner.submit_reduce_only_flatten().await
        }
    }

    #[tokio::test]
    async fn paper_reconcile_now_truth_from_before_a_rearm_cannot_release_the_new_latch() {
        let (store, path) = execution_fixture("paper-reset-rearm").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let open = execute_v2(&service, protected_intent("paper_reset_rearm", "95", "111")).await;
        assert_eq!(open.status, Status::Accepted as i32, "{}", open.message);
        let adapter = Arc::new(RearmDuringRecoveryAdapter {
            store: store.clone(),
            inner: RecoveryFailureAdapter::default(),
            rearmed: AtomicUsize::new(0),
        });
        for row in sqlx::query("SELECT client_algo_id FROM protection_orders")
            .fetch_all(store.pool())
            .await
            .expect("persisted paper protection ids")
        {
            adapter
                .inner
                .open_ids
                .lock()
                .expect("open ids")
                .insert(row.get::<String, _>("client_algo_id"));
        }
        let recovery_service =
            EngineExecutionV2Service::with_protection_truth_adapter(store.clone(), adapter.clone());
        arm_kill_switch_rpc(&service, "cred:paper", "pause_new").await;

        let stale = reconcile_now_rpc(&recovery_service, "paper")
            .await
            .expect("paper reconcile overlapped by a re-arm");
        assert!(!stale.has_diff);
        assert!(adapter.rearmed.load(Ordering::SeqCst) > 0);
        assert_eq!(kill_switch_level(&store, "cred:paper").await, "manage_only");
        assert!(
            reported_reconcile_rows(&store, "cred:paper")
                .await
                .is_empty(),
            "a truth observed before the re-arm must not be recorded for the new revision"
        );
        assert_reset_refused(
            &service,
            "cred:paper",
            "a truth observed before the re-arm must not release the new latch",
        )
        .await;

        reconcile_now_rpc(&recovery_service, "paper")
            .await
            .expect("paper reconcile after the re-arm");
        reset_kill_switch_rpc(&service, "cred:paper")
            .await
            .expect("reset after a reconcile that observed the current revision");

        drop(recovery_service);
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn paper_reconcile_now_one_credential_cannot_release_global_or_foreign_latches() {
        let (store, path) = execution_fixture("paper-reset-global").await;
        seed_second_paper_budget(&store).await;
        let service = EngineExecutionV2Service::new(store.clone());
        arm_kill_switch_rpc(&service, "global", "pause_new").await;
        let foreign_scope = format!("cred:{SECOND_PAPER_CREDENTIAL}");
        arm_kill_switch_rpc(&service, &foreign_scope, "pause_new").await;

        reconcile_now_rpc(&service, "paper")
            .await
            .expect("first paper credential reconcile");
        assert_eq!(
            reported_reconcile_rows(&store, "global").await,
            vec![("paper".to_string(), 1)]
        );
        assert_reset_refused(
            &service,
            "global",
            "one credential must not release a multi-credential global latch",
        )
        .await;
        assert!(reported_reconcile_rows(&store, &foreign_scope)
            .await
            .is_empty());
        assert_reset_refused(
            &service,
            &foreign_scope,
            "one credential must not release another credential's latch",
        )
        .await;

        reconcile_now_rpc(&service, SECOND_PAPER_CREDENTIAL)
            .await
            .expect("second paper credential reconcile");
        reset_kill_switch_rpc(&service, "global")
            .await
            .expect("global reset once every target credential reconciled clean");
        reset_kill_switch_rpc(&service, &foreign_scope)
            .await
            .expect("credential reset after its own reconcile");

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn paper_reconcile_now_failure_leaves_no_resettable_fact() {
        let (store, path) = execution_fixture("paper-reset-failure").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let open = execute_v2(&service, protected_intent("paper_reset_fail", "95", "111")).await;
        assert_eq!(open.status, Status::Accepted as i32, "{}", open.message);
        arm_kill_switch_rpc(&service, "cred:paper", "pause_new").await;

        // 保护修复失败：ledger 真值本身是 clean 的，但这次对账没有完成。
        sqlx::query("DELETE FROM protection_orders")
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM engine_orders WHERE order_role LIKE 'protection_%'")
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE position_state SET state='open'")
            .execute(store.pool())
            .await
            .unwrap();
        let failing_service = EngineExecutionV2Service::with_protection_submit_failure(
            store.clone(),
            Arc::new(RecoveryFailureAdapter::default()),
        );
        reconcile_now_rpc(&failing_service, "paper")
            .await
            .expect_err("a failed protection repair must fail the reconcile");
        assert!(reported_reconcile_rows(&store, "cred:paper")
            .await
            .is_empty());
        assert_reset_refused(
            &service,
            "cred:paper",
            "a reconcile whose protection repair failed must not make the latch resettable",
        )
        .await;

        // ledger 真值读不出来：同样不得留下任何事实。
        sqlx::query("UPDATE engine_fills SET fill_qty='not-a-decimal'")
            .execute(store.pool())
            .await
            .unwrap();
        reconcile_now_rpc(&service, "paper")
            .await
            .expect_err("an unreadable paper ledger must fail the reconcile");
        assert!(reported_reconcile_rows(&store, "cred:paper")
            .await
            .is_empty());
        assert_reset_refused(
            &service,
            "cred:paper",
            "an unreadable paper ledger must not make the latch resettable",
        )
        .await;
        assert_eq!(kill_switch_level(&store, "cred:paper").await, "pause_new");

        drop(failing_service);
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn non_simulation_reconcile_now_gains_no_paper_projection() {
        let (store, path) = execution_fixture("paper-reset-live").await;
        let service = EngineExecutionV2Service::new(store.clone());
        arm_kill_switch_rpc(&service, "cred:cred_not_paper", "pause_new").await;
        let refused = reconcile_now_rpc(&service, "cred_not_paper")
            .await
            .expect_err("a non-simulation credential without keys must not reconcile");
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
        // 另一个 credential 的 paper 对账也不得替它投票。
        reconcile_now_rpc(&service, "paper")
            .await
            .expect("paper reconcile");
        assert!(reported_reconcile_rows(&store, "cred:cred_not_paper")
            .await
            .is_empty());
        assert_reset_refused(
            &service,
            "cred:cred_not_paper",
            "a non-simulation latch must not be released by the paper path",
        )
        .await;

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn get_position_states_filters_credential_bot_and_closed_rows() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-v2-read-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect v2 test store");
        for (position_id, bot_uid, state, active_key) in [
            ("pos_open", "bot_open", "open", Some("active_open")),
            ("pos_closed", "bot_closed", "closed", None),
        ] {
            sqlx::query(
                r#"
                INSERT INTO position_state (
                  position_id, credential_id, bot_uid, symbol_key, direction,
                  entry_epoch, state, active_key, qty, updated_at_ms
                ) VALUES (?, 'cred_v2', ?, 'binance|futures|BTC|USDT|USDT',
                          'long', 1, ?, ?, '1', 10)
                "#,
            )
            .bind(position_id)
            .bind(bot_uid)
            .bind(state)
            .bind(active_key)
            .execute(store.pool())
            .await
            .expect("insert position state fixture");
        }
        sqlx::query(
            r#"
            INSERT INTO engine_positions (
              credential_id, exchange, market_kind, symbol, symbol_key,
              position_amt, entry_price, mark_price, leverage,
              unrealized_pnl, updated_at_ms
            ) VALUES ('cred_v2', 'binance', 'futures', 'ETHUSDT',
                      'binance|futures|ETH|USDT|USDT', '2', '50', '51',
                      '1', '2', 11)
            "#,
        )
        .execute(store.pool())
        .await
        .expect("insert orphan exchange truth");

        let service = EngineExecutionV2Service::new(store.clone());
        let open_only = service
            .get_position_states(Request::new(GetPositionStatesRequest {
                credential_id: "cred_v2".to_string(),
                bot_uid: "".to_string(),
                include_closed: false,
                schema_version: 1,
            }))
            .await
            .expect("get open position states")
            .into_inner();
        assert_eq!(open_only.positions.len(), 1);
        assert_eq!(open_only.positions[0].position_id, "pos_open");
        assert_eq!(open_only.orphan_positions.len(), 1);
        assert_eq!(open_only.orphan_positions[0].direction, "long");

        let closed_bot = service
            .get_position_states(Request::new(GetPositionStatesRequest {
                credential_id: "cred_v2".to_string(),
                bot_uid: "bot_closed".to_string(),
                include_closed: true,
                schema_version: 1,
            }))
            .await
            .expect("get closed bot state")
            .into_inner();
        assert_eq!(closed_bot.positions.len(), 1);
        assert_eq!(closed_bot.positions[0].state, "closed");
        assert!(closed_bot.positions[0].active_key.is_empty());

        let missing_credential = service
            .get_position_states(Request::new(GetPositionStatesRequest {
                credential_id: "".to_string(),
                bot_uid: "".to_string(),
                include_closed: false,
                schema_version: 1,
            }))
            .await
            .expect_err("credential_id must be required");
        assert_eq!(missing_credential.code(), tonic::Code::InvalidArgument);

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **sqlx-i64 核查批· v2 持仓/订单真值出口的毫秒 epoch。**
    ///
    /// 上一条用例的夹具写的是 `updated_at_ms = 10` / `11`（< `i32::MAX`），
    /// 截断对它们恒等，所以它对本缺陷失明。这里换成真实毫秒 epoch，
    /// 一次钉住 `get_position_states` 的四列 + 孤儿持仓的 `observed_at_ms`
    /// + `get_order_by_client_order_id` 的两列。
    #[tokio::test]
    async fn v2_read_paths_return_real_millisecond_epochs() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-v2-i64-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect v2 test store");
        // 2026-08-25 附近；低 32 位是 723097806。
        let wide_ms = 1_787_429_492_942_i64;
        assert!(wide_ms > i64::from(i32::MAX));
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, opened_at_ms, closed_at_ms,
              last_reconciled_ms, updated_at_ms
            ) VALUES ('pos_i64', 'cred_v2i', 'bot_i64', 'binance|futures|BTC|USDT|USDT',
                      'long', 1, 'open', 'active_i64', '1', ?, ?, ?, ?)
            "#,
        )
        .bind(wide_ms - 3_000)
        .bind(wide_ms - 2_000)
        .bind(wide_ms - 1_000)
        .bind(wide_ms)
        .execute(store.pool())
        .await
        .expect("insert a position stamped at a real millisecond epoch");
        sqlx::query(
            r#"
            INSERT INTO engine_positions (
              credential_id, exchange, market_kind, symbol, symbol_key,
              position_amt, entry_price, mark_price, leverage,
              unrealized_pnl, updated_at_ms
            ) VALUES ('cred_v2i', 'binance', 'futures', 'ETHUSDT',
                      'binance|futures|ETH|USDT|USDT', '2', '50', '51',
                      '1', '2', ?)
            "#,
        )
        .bind(wide_ms - 4_000)
        .execute(store.pool())
        .await
        .expect("insert orphan exchange truth");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, status, order_role, created_at_ms, updated_at_ms
            ) VALUES ('eng_v2i', 'paper', 'bot_i64', 'idem_v2i', 'coid_v2i', '7001',
                      'binance', 'futures', 'binance|futures|BTC|USDT|USDT',
                      'open_long', 'long', 'market', '1', 0, 'acked', 'entry', ?, ?)
            "#,
        )
        .bind(wide_ms - 9_000)
        .bind(wide_ms - 8_000)
        .execute(store.pool())
        .await
        .expect("insert an order stamped at a real millisecond epoch");

        let service = EngineExecutionV2Service::new(store.clone());
        let states = service
            .get_position_states(Request::new(GetPositionStatesRequest {
                credential_id: "cred_v2i".to_string(),
                bot_uid: String::new(),
                include_closed: true,
                schema_version: 1,
            }))
            .await
            .expect("get position states")
            .into_inner();
        assert_eq!(states.positions.len(), 1);
        let position = &states.positions[0];
        assert_eq!(position.opened_at_ms, wide_ms - 3_000);
        assert_eq!(position.closed_at_ms, wide_ms - 2_000);
        assert_eq!(position.last_reconciled_ms, wide_ms - 1_000);
        assert_eq!(position.updated_at_ms, wide_ms);
        assert_eq!(states.orphan_positions.len(), 1);
        assert_eq!(states.orphan_positions[0].observed_at_ms, wide_ms - 4_000);

        // `paper` 是模拟凭据：逐单回查不会走交易所边界，断言的是本地读侧。
        let order = service
            .get_order_by_client_order_id(Request::new(GetOrderByClientOrderIdRequest {
                credential_id: "paper".to_string(),
                client_order_id_short: "coid_v2i".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("get order by client order id")
            .into_inner()
            .order
            .expect("the order is found");
        assert_eq!(order.created_at_ms, wide_ms - 9_000);
        assert_eq!(order.updated_at_ms, wide_ms - 8_000);

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    fn simulation_funding_request(
        credential_id: &str,
        amount_usdt: &str,
        idempotency_identity: &str,
    ) -> FundSimulationAccountRequest {
        FundSimulationAccountRequest {
            credential_id: credential_id.to_string(),
            amount_usdt: amount_usdt.to_string(),
            idempotency_identity: idempotency_identity.to_string(),
            schema_version: 1,
        }
    }

    async fn simulation_funding_fixture(label: &str) -> (Store, std::path::PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-v2-simulation-funding-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect simulation funding fixture");
        (store, path)
    }

    #[tokio::test]
    async fn simulation_funding_is_durable_idempotent_and_additive_by_identity() {
        let (store, path) = simulation_funding_fixture("idempotent").await;
        let credential_id = "paper:7b6f4495fde3706c999256af2985a8f4";
        let first = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                credential_id,
                "1000.00000000",
                "pdfund:11111111111111111111111111111111",
            )))
            .await
            .expect("apply initial simulation funding")
            .into_inner();
        assert!(first.accepted);
        assert!(first.applied);
        assert_eq!(first.wallet_balance_usdt, "1000");

        // A new service instance proves idempotency is owned by migrated store
        // state rather than process-local memory.
        let replay = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                credential_id,
                "1000.0",
                "pdfund:11111111111111111111111111111111",
            )))
            .await
            .expect("replay initial simulation funding")
            .into_inner();
        assert!(replay.accepted);
        assert!(!replay.applied);
        assert_eq!(replay.wallet_balance_usdt, "1000");

        let conflict = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                credential_id,
                "999",
                "pdfund:11111111111111111111111111111111",
            )))
            .await
            .expect_err("different amount must not reuse an identity");
        assert_eq!(conflict.code(), tonic::Code::AlreadyExists);

        let other_credential = "paper:448cc18e49c41178ad7bee0264b88f98";
        let rebound = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                other_credential,
                "1000",
                "pdfund:11111111111111111111111111111111",
            )))
            .await
            .expect_err("one identity must not be rebound to another credential");
        assert_eq!(rebound.code(), tonic::Code::AlreadyExists);
        let other_balance_count =
            sqlx::query("SELECT COUNT(*) AS count FROM engine_balances WHERE credential_id=?")
                .bind(other_credential)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<i64, _>("count");
        assert_eq!(other_balance_count, 0);

        let second = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                credential_id,
                "0.125",
                "pdfund:22222222222222222222222222222222",
            )))
            .await
            .expect("apply independently identified funding")
            .into_inner();
        assert!(second.applied);
        assert_eq!(second.wallet_balance_usdt, "1000.125");

        let balance_count =
            sqlx::query("SELECT COUNT(*) AS count FROM engine_balances WHERE credential_id=?")
                .bind(credential_id)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<i64, _>("count");
        assert_eq!(balance_count, 1, "funding must only create the USDT row");
        let balance = sqlx::query(
            "SELECT exchange, market_kind, asset, wallet_balance, available_balance, raw_json FROM engine_balances WHERE credential_id=?",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(balance.get::<String, _>("exchange"), "binance");
        assert_eq!(balance.get::<String, _>("market_kind"), "futures");
        assert_eq!(balance.get::<String, _>("asset"), "USDT");
        assert_eq!(balance.get::<String, _>("wallet_balance"), "1000.125");
        assert_eq!(balance.get::<String, _>("available_balance"), "1000.125");
        assert!(crate::store::try_get_nullable_string(&balance, "raw_json")
            .unwrap()
            .is_none());
        let event_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM simulation_account_funding_events WHERE credential_id=?",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
        assert_eq!(event_count, 2);

        let account = EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_id.to_string(),
                bot_uid: String::new(),
                schema_version: 1,
            }))
            .await
            .expect("funded simulation truth must pass the account snapshot gate")
            .into_inner();
        assert!(account.available);
        assert_eq!(account.equity_usdt.as_deref(), Some("1000.125"));
        assert_eq!(account.available_usdt.as_deref(), Some("1000.125"));
        assert_eq!(account.open_positions_count, Some(0));

        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn simulation_funding_rejects_exchange_credentials_without_any_write() {
        let (store, path) = simulation_funding_fixture("reject-exchange").await;
        let error = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(simulation_funding_request(
                "credential_real_exchange",
                "1000",
                "pdfund:33333333333333333333333333333333",
            )))
            .await
            .expect_err("exchange credential must be rejected");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        let balance_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM engine_balances WHERE credential_id='credential_real_exchange'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
        let event_count = sqlx::query(
            "SELECT COUNT(*) AS count FROM simulation_account_funding_events WHERE credential_id='credential_real_exchange'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
        assert_eq!((balance_count, event_count), (0, 0));

        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn account_fixture(label: &str) -> (Store, std::path::PathBuf, i64) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-v2-account-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect account fixture");
        let now = crate::svc::now_ms();
        let position_as_of = now - 2_000;
        let opened_at = position_as_of - 60_000;
        sqlx::query(
            r#"
            INSERT INTO engine_balances (
              credential_id, exchange, market_kind, asset, wallet_balance,
              available_balance, updated_at_ms
            ) VALUES ('cred_account', 'binance', 'futures', 'USDT',
                      '100.25', '80.10', ?)
            "#,
        )
        .bind(now - 1_000)
        .execute(store.pool())
        .await
        .expect("insert account balance");
        for (symbol, symbol_key, qty, unrealized, liquidation, updated_at) in [
            (
                "BTCUSDT",
                "binance|futures|BTC|USDT|USDT",
                "2",
                "2.75",
                Some("50"),
                position_as_of,
            ),
            (
                "ETHUSDT",
                "binance|futures|ETH|USDT|USDT",
                "0",
                "999",
                None,
                now - 500,
            ),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_positions (
                  credential_id, exchange, market_kind, symbol, symbol_key,
                  position_amt, entry_price, mark_price, leverage,
                  unrealized_pnl, liquidation_price, updated_at_ms
                ) VALUES ('cred_account', 'binance', 'futures', ?, ?, ?,
                          '40', '42', '3', ?, ?, ?)
                "#,
            )
            .bind(symbol)
            .bind(symbol_key)
            .bind(qty)
            .bind(unrealized)
            .bind(liquidation)
            .bind(updated_at)
            .execute(store.pool())
            .await
            .expect("insert account position truth");
        }
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, entry_price, opened_at_ms,
              updated_at_ms
            ) VALUES ('pos_account', 'cred_account', 'bot_account',
                      'binance|futures|BTC|USDT|USDT', 'long', 1, 'open',
                      'bot_account|btc|long', '2', '40', ?, ?)
            "#,
        )
        .bind(opened_at)
        .bind(position_as_of)
        .execute(store.pool())
        .await
        .expect("insert durable position state");

        let day_start = chrono::Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("UTC midnight")
            .and_utc()
            .timestamp_millis();
        for (trade_id, pnl, exchange_ms) in [
            ("trade_before", "99", day_start - 1),
            ("trade_today_1", "1.25", day_start),
            ("trade_today_2", "0.75", day_start + 1),
        ] {
            sqlx::query(
                r#"
                INSERT INTO engine_fills (
                  credential_id, exchange_trade_id, engine_order_id, exchange,
                  market_kind, realized_pnl, ts_exchange_ms, created_at_ms
                ) VALUES ('cred_account', ?, 'eng_account', 'binance',
                          'futures', ?, ?, ?)
                "#,
            )
            .bind(trade_id)
            .bind(pnl)
            .bind(exchange_ms)
            .bind(now)
            .execute(store.pool())
            .await
            .expect("insert realized PnL fixture");
        }
        (store, path, position_as_of)
    }

    async fn get_account(
        service: &EngineExecutionV2Service,
    ) -> crate::proto_v2::GetAccountStateResponse {
        service
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: "cred_account".to_string(),
                bot_uid: "".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("get account state")
            .into_inner()
    }

    async fn close_account_fixture(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn get_account_state_missing_usdt_balance_is_unavailable_without_amounts() {
        let (store, path, _) = account_fixture("missing-balance").await;
        sqlx::query("DELETE FROM engine_balances WHERE credential_id='cred_account'")
            .execute(store.pool())
            .await
            .unwrap();
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert!(!response.available);
        assert!(!response.unavailable_reason.is_empty());
        assert_eq!(response.as_of_ms, 0);
        assert!(response.observed_at_ms.is_none());
        assert!(response.equity_usdt.is_none());
        assert!(response.available_usdt.is_none());
        assert!(response.used_margin_usdt.is_none());
        assert!(response.today_realized_pnl_usdt.is_none());
        assert!(response.open_positions_count.is_none());
        assert!(response.positions.is_empty());
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_null_liquidation_price_remains_unknown() {
        let (store, path, _) = account_fixture("null-liquidation").await;
        sqlx::query(
            "UPDATE engine_positions SET liquidation_price=NULL WHERE credential_id='cred_account' AND symbol='BTCUSDT'",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert!(response.available);
        assert_eq!(response.positions.len(), 1);
        assert_eq!(response.positions[0].liquidation_price, None);
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_realized_pnl_uses_utc_day_boundary() {
        let (store, path, _) = account_fixture("utc-realized").await;
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert_eq!(response.today_realized_pnl_usdt.as_deref(), Some("2"));
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_derives_equity_and_used_margin_exactly() {
        let (store, path, _) = account_fixture("derived-money").await;
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert_eq!(response.equity_usdt.as_deref(), Some("103"));
        assert_eq!(response.available_usdt.as_deref(), Some("80.1"));
        assert_eq!(response.used_margin_usdt.as_deref(), Some("22.9"));
        assert!(response
            .derived_fields
            .iter()
            .any(|field| field == "equity_usdt"));
        assert!(response
            .derived_fields
            .iter()
            .any(|field| field == "used_margin_usdt"));
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_exchange_backed_as_of_remains_conservative_truth_timestamp() {
        let (store, path, position_as_of) = account_fixture("as-of").await;
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert_eq!(response.as_of_ms, position_as_of);
        assert!(response.observed_at_ms.unwrap() >= position_as_of);
        assert!(response.observed_at_ms.unwrap() <= crate::svc::now_ms());
        assert_eq!(response.positions[0].age_minutes, 1.0);
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_simulation_as_of_reflects_observation_time() {
        let (store, path) = simulation_funding_fixture("account-observation-time").await;
        let credential_id = "paper:7b6f4495fde3706c999256af2985a8f4";
        // Fund through the production RPC; the stale engine_balances row below
        // is a different amount on purpose, proving the simulation projection
        // takes its funds from the durable funding ledger, not that table.
        let funding = EngineExecutionV2Service::new(store.clone())
            .fund_simulation_account(Request::new(FundSimulationAccountRequest {
                credential_id: credential_id.to_string(),
                amount_usdt: "512.5".to_string(),
                idempotency_identity: "simobs:as-of-observation".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("fund the simulation credential")
            .into_inner();
        assert!(funding.applied);
        sqlx::query(
            r#"
            UPDATE engine_balances
            SET wallet_balance='1000', available_balance='1000', updated_at_ms=1
            WHERE credential_id=? AND asset='USDT'
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("corrupt the simulation balance row to stale misleading values");
        let observed_before_ms = crate::svc::now_ms();

        let response = EngineExecutionV2Service::new(store.clone())
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: credential_id.to_string(),
                bot_uid: String::new(),
                schema_version: 1,
            }))
            .await
            .expect("observe simulation account state")
            .into_inner();
        let observed_after_ms = crate::svc::now_ms();

        assert!(response.available);
        assert!(response.as_of_ms >= observed_before_ms);
        assert!(response.as_of_ms <= observed_after_ms);
        assert_ne!(response.as_of_ms, 1);
        assert_eq!(response.observed_at_ms, Some(response.as_of_ms));
        assert_eq!(response.equity_usdt.as_deref(), Some("512.5"));
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_counts_only_nonzero_positions() {
        let (store, path, _) = account_fixture("open-count").await;
        let response = get_account(&EngineExecutionV2Service::new(store.clone())).await;
        assert_eq!(response.open_positions_count, Some(1));
        assert_eq!(response.positions.len(), 1);
        assert_eq!(response.positions[0].qty, "2");
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn get_account_state_fills_position_contract_fields_and_honors_bot_filter() {
        let (store, path, position_as_of) = account_fixture("contract-fields").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let response = service
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: "cred_account".to_string(),
                bot_uid: "bot_account".to_string(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.available);
        assert_eq!(response.equity_usdt.as_deref(), Some("103"));
        assert_eq!(response.available_usdt.as_deref(), Some("80.1"));
        assert_eq!(response.used_margin_usdt.as_deref(), Some("22.9"));
        assert_eq!(response.today_realized_pnl_usdt.as_deref(), Some("2"));
        assert_eq!(response.open_positions_count, Some(1));
        let position = &response.positions[0];
        assert_eq!(position.symbol_key, "binance|futures|BTC|USDT|USDT");
        assert_eq!(position.side, "long");
        assert_eq!(position.qty, "2");
        assert_eq!(position.entry_price, "40");
        assert_eq!(position.mark_price, "42");
        assert_eq!(position.leverage, 3);
        assert_eq!(position.unrealized_pnl_usdt, "2.75");
        assert_eq!(position.liquidation_price.as_deref(), Some("50"));
        assert_eq!(position.opened_at_ms, position_as_of - 60_000);
        assert_eq!(position.age_minutes, 1.0);

        let other_bot = service
            .get_account_state(Request::new(GetAccountStateRequest {
                credential_id: "cred_account".to_string(),
                bot_uid: "bot_other".to_string(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(other_bot.available);
        assert_eq!(other_bot.equity_usdt.as_deref(), Some("103"));
        assert_eq!(other_bot.open_positions_count, Some(0));
        assert!(other_bot.positions.is_empty());
        drop(service);
        close_account_fixture(store, path).await;
    }

    #[tokio::test]
    async fn recovery_rpcs_prove_reconcile_query_client_id_and_release_pending() {
        let (store, path) = execution_fixture("recovery-rpcs").await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES ('pos_pending_rpc', 'paper', 'bot_rpc',
                      'binance|futures|BTC|USDT|USDT', 'long', 7, 'pending',
                      'bot_rpc|btc|long', 'idem_rpc', '0', 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange, market_kind, symbol_key,
              action, side, order_type, quantity, reduce_only, status,
              position_id, order_role, created_at_ms, updated_at_ms
            ) VALUES ('eng_pending_rpc', 'paper', 'bot_rpc', 'idem_rpc',
                      'coid_pending_rpc', 'binance', 'futures',
                      'binance|futures|BTC|USDT|USDT', 'open_long', 'long',
                      'market', '1', 0, 'canceled', 'pos_pending_rpc', 'entry', 1, 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
        let service = EngineExecutionV2Service::new(store.clone());
        let reconcile = service
            .reconcile_now(Request::new(ReconcileNowRequest {
                credential_id: "paper".to_string(),
                reason: "test".to_string(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reconcile.accepted && reconcile.completed);
        assert!(!reconcile.has_diff);
        assert!(reconcile.local_position_zero_diff);
        // 真 RPC 出口也要带上算法挂单那一位：投影函数写对了、handler 没接上
        // 同样是"字段永远缺省"。这条 credential 只有一条 canceled 单，
        // 两份清单都空。
        assert!(reconcile.all_open_orders_empty);
        assert!(reconcile.all_open_algo_orders_empty);

        sqlx::query(
            r#"
            INSERT INTO recovery_order_lookups (
              lookup_id, credential_id, client_order_id_short, completed,
              found, confirmed_never_filled, status, observed_at_ms
            ) VALUES ('rol_wrong', 'paper', 'coid_wrong', 1, 0, 1, NULL, 1)
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
        let mismatched_release = service
            .release_pending(Request::new(ReleasePendingRequest {
                credential_id: "paper".to_string(),
                position_id: "pos_pending_rpc".to_string(),
                client_order_id_short: "coid_wrong".to_string(),
                schema_version: 1,
            }))
            .await
            .expect_err("proof for another client id must not release this pending row");
        assert_eq!(mismatched_release.code(), tonic::Code::FailedPrecondition);

        let lookup = service
            .get_order_by_client_order_id(Request::new(GetOrderByClientOrderIdRequest {
                credential_id: "paper".to_string(),
                client_order_id_short: "coid_pending_rpc".to_string(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(lookup.completed && lookup.found && lookup.confirmed_never_filled);
        let release = service
            .release_pending(Request::new(ReleasePendingRequest {
                credential_id: "paper".to_string(),
                position_id: "pos_pending_rpc".to_string(),
                client_order_id_short: "coid_pending_rpc".to_string(),
                schema_version: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(release.released);
        assert_eq!(release.entry_epoch, 7);
        let remaining = sqlx::query(
            "SELECT COUNT(*) AS n FROM position_state WHERE position_id='pos_pending_rpc'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
        assert_eq!(remaining, 0);
        let audited_epoch = sqlx::query(
            "SELECT entry_epoch FROM recovery_pending_releases WHERE position_id='pos_pending_rpc'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("entry_epoch");
        assert_eq!(audited_epoch, 7);
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    fn budget_request(as_of_ms: i64, equity_usdt: &str) -> PutRiskBudgetRequest {
        PutRiskBudgetRequest {
            credential_id: "cred_budget".to_string(),
            as_of_ms,
            equity_usdt: Some(equity_usdt.to_string()),
            available_usdt: Some("8000".to_string()),
            today_realized_pnl_usdt: Some("-25".to_string()),
            today_realized_pnl_pct: Some(-0.0025),
            drawdown_pct: Some(0.01),
            total_exposure_pct: Some(0.15),
            open_positions_count: Some(2),
            group_exposure_json: Some("{\"majors\":0.1,\"l1_alts\":0.05}".to_string()),
            kill_switch_level: Some("none".to_string()),
            kill_switch_scope: None,
            kill_switch_reason: None,
            remaining_credential_notional_usdt: Some("3500".to_string()),
            schema_version: 1,
        }
    }

    fn certification_request() -> PutPromotionCertificationRequest {
        PutPromotionCertificationRequest {
            cert_id: "cert_f16".to_string(),
            bot_uid: "bot_f16".to_string(),
            stage: "live".to_string(),
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            policy_version: "risk_policy_v3".to_string(),
            code_commit: "a".repeat(40),
            passed: true,
            invalidated_at_ms: None,
            certified_at_ms: 1_000_000,
            schema_version: 1,
        }
    }

    async fn evaluate_certified_stage(
        store: &Store,
        bot_uid: &str,
        requested_stage: &str,
        policy_version: &str,
        venue_is_testnet: bool,
    ) -> (risk_policy::RiskVerdict, String) {
        let now = crate::svc::now_ms();
        let intent = crate::proto::OrderIntent {
            bot_uid: bot_uid.to_string(),
            run_id: "run_stage_cert".to_string(),
            cycle_id: format!("cycle_{bot_uid}"),
            idempotency_key: format!("idem_{bot_uid}_{policy_version}"),
            client_order_id_short: format!("coid_{bot_uid}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: "binance|futures|BTC|USDT|USDT".to_string(),
            action: "open_long".to_string(),
            side: "long".to_string(),
            order_type: "limit".to_string(),
            price: "100".to_string(),
            quantity: String::new(),
            reduce_only: false,
            max_position_notional: 0.0,
            max_credential_notional: 0.0,
        };
        let wire = crate::risk::WireRiskContext {
            typed_protection_capable: true,
            data_snapshot_capable: true,
            data_snapshot: Some(risk_policy::DataSnapshot {
                allow_open: true,
                consistency_error: None,
            }),
            intent_id: format!("int_{bot_uid}_{policy_version}"),
            protection: Some(crate::risk::TypedProtectionInput {
                stop_loss_price: "95".to_string(),
                take_profit_price: "111".to_string(),
            }),
            valid_until_ms: Some(now + 30_000),
            max_slippage_bps: Some(15),
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            policy_version: policy_version.to_string(),
            code_commit: "a".repeat(40),
        };
        let cycle = crate::fill_replay::begin_recovery_cycle(
            store,
            "cred_f16",
            crate::fill_replay::RecoveryTrigger::Startup,
            now - 2,
        )
        .await
        .expect("begin fill-ready stage fixture");
        crate::fill_replay::finalize_recovery_readiness(
            store,
            "cred_f16",
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            crate::fill_replay::RecoveryPhaseResult::Succeeded,
            cycle,
            now - 1,
        )
        .await
        .expect("mark stage fixture fill-ready");
        let mut tx = store.pool().begin().await.expect("begin stage evaluation");
        let (verdict, _, replay_input) = crate::risk::evaluate_v1(
            &mut tx,
            store.kind(),
            "cred_f16",
            &intent,
            now,
            Some(&wire),
            requested_stage,
            venue_is_testnet,
            crate::protection::OrderRole::Entry,
            None,
        )
        .await
        .expect("evaluate stage certification");
        tx.rollback().await.expect("rollback stage evaluation");
        (verdict, replay_input)
    }

    #[tokio::test]
    async fn put_promotion_certification_upserts_invalidation_and_rejects_cert_id_rebinding() {
        let (store, path) = empty_execution_fixture("promotion-projection-rpc").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let applied = service
            .put_promotion_certification(Request::new(certification_request()))
            .await
            .expect("write certification projection")
            .into_inner();
        assert!(applied.accepted && applied.applied);

        let mut invalidated = certification_request();
        invalidated.invalidated_at_ms = Some(1_100_000);
        service
            .put_promotion_certification(Request::new(invalidated))
            .await
            .expect("update invalidation projection");
        let stored = sqlx::query(
            "SELECT passed, CAST(invalidated_at_ms AS CHAR) AS invalidated FROM promotion_certifications_projection WHERE cert_id='cert_f16'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read certification projection");
        assert_eq!(stored.get::<i64, _>("passed"), 1);
        assert_eq!(stored.get::<String, _>("invalidated"), "1100000");

        let mut rebound = certification_request();
        rebound.config_hash = "d".repeat(64);
        let error = service
            .put_promotion_certification(Request::new(rebound))
            .await
            .expect_err("cert_id rebinding must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn f16_policy_version_switch_downgrades_mainnet_bot_to_paper() {
        let (store, path) = empty_execution_fixture("f16-policy-switch").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut budget = budget_request(crate::svc::now_ms(), "10000");
        budget.credential_id = "cred_f16".to_string();
        service
            .put_risk_budget(Request::new(budget))
            .await
            .expect("seed F16 risk budget");
        service
            .put_promotion_certification(Request::new(certification_request()))
            .await
            .expect("seed matching live certification");

        // `venue_is_testnet=false` models F16's mainnet credential.  The
        // expected fallback must therefore be paper, never the demo allowance.
        let (certified, certified_replay) =
            evaluate_certified_stage(&store, "bot_f16", "live", "risk_policy_v3", false).await;
        assert_ne!(certified.decision, risk_policy::Decision::Reject);
        let certified_rule = certified
            .rules
            .iter()
            .find(|rule| rule.rule == "stage_certified")
            .expect("stage_certified verdict rule");
        assert_eq!(certified_rule.result, risk_policy::RuleOutcome::Pass);
        assert!(certified_rule
            .detail
            .as_deref()
            .unwrap()
            .contains("authorization_source=certification"));
        assert!(certified_replay.contains("cert_f16"));

        let (wrong_bot, _) =
            evaluate_certified_stage(&store, "bot_other", "live", "risk_policy_v3", false).await;
        assert_eq!(wrong_bot.decision, risk_policy::Decision::Reject);
        assert_eq!(
            wrong_bot.reject_reason_code.as_deref(),
            Some("STAGE_NOT_CERTIFIED")
        );

        let mut changed_policy = risk_policy::factory_policy();
        changed_policy.policy_version = "risk_policy_v4".to_string();
        let changed_hash = risk_policy::policy_hash(&changed_policy);
        sqlx::query(
            r#"
            INSERT INTO risk_policies_snapshot (
              policy_version, policy_hash, policy_json, schema_version,
              received_at_ms, activated_at_ms
            ) VALUES (?, ?, ?, 1, ?, ?)
            "#,
        )
        .bind(&changed_policy.policy_version)
        .bind(changed_hash)
        .bind(serde_json::to_string(&changed_policy).unwrap())
        .bind(crate::svc::now_ms().to_string())
        .bind(crate::svc::now_ms().to_string())
        .execute(store.pool())
        .await
        .expect("activate changed policy");

        let (downgraded, replay_input) =
            evaluate_certified_stage(&store, "bot_f16", "live", "risk_policy_v4", false).await;
        assert_eq!(downgraded.decision, risk_policy::Decision::Reject);
        assert_eq!(
            downgraded.reject_reason_code.as_deref(),
            Some("STAGE_NOT_CERTIFIED")
        );
        let rule = downgraded
            .rules
            .iter()
            .find(|rule| rule.rule == "stage_certified")
            .expect("downgrade verdict rule");
        assert!(rule
            .detail
            .as_deref()
            .unwrap()
            .contains("fallback_stage=paper"));
        let replayed = crate::risk::replay_verdict(&replay_input).expect("replay F16 verdict");
        assert_eq!(
            replayed.replay_fingerprint(),
            downgraded.replay_fingerprint()
        );

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn empty_projection_keeps_testnet_demo_authorized_with_distinct_verdict_source() {
        let (store, path) = empty_execution_fixture("demo-venue-stage-source").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut budget = budget_request(crate::svc::now_ms(), "10000");
        budget.credential_id = "cred_f16".to_string();
        service
            .put_risk_budget(Request::new(budget))
            .await
            .expect("seed demo risk budget");

        let (verdict, _) = evaluate_certified_stage(
            &store,
            "bot_demo_without_cert",
            "demo",
            "risk_policy_v3",
            true,
        )
        .await;
        assert_ne!(verdict.decision, risk_policy::Decision::Reject);
        let rule = verdict
            .rules
            .iter()
            .find(|rule| rule.rule == "stage_certified")
            .expect("demo stage verdict rule");
        assert_eq!(rule.result, risk_policy::RuleOutcome::Pass);
        assert!(rule
            .detail
            .as_deref()
            .unwrap()
            .contains("authorization_source=testnet_venue"));

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn put_risk_budget_null_optionals_remain_usable_on_real_evaluation_path() {
        let (store, path) = empty_execution_fixture("budget-null-optionals").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut request = budget_request(crate::svc::now_ms(), "10000");
        request.credential_id = "paper".to_string();
        request.group_exposure_json = None;
        request.kill_switch_scope = None;
        request.kill_switch_reason = None;
        request.remaining_credential_notional_usdt = None;

        let written = service
            .put_risk_budget(Request::new(request))
            .await
            .expect("PutRiskBudget must accept absent optional facts")
            .into_inner();
        assert!(written.accepted && written.applied);

        let stored = sqlx::query(
            r#"
            SELECT group_exposure_json IS NULL AS groups_null,
                   kill_switch_scope IS NULL AS scope_null,
                   kill_switch_reason IS NULL AS reason_null,
                   remaining_credential_notional_usdt IS NULL AS remaining_null
            FROM risk_budget WHERE credential_id='paper'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("read PutRiskBudget result");
        assert_eq!(stored.get::<i64, _>("groups_null"), 1);
        assert_eq!(stored.get::<i64, _>("scope_null"), 1);
        assert_eq!(stored.get::<i64, _>("reason_null"), 1);
        assert_eq!(stored.get::<i64, _>("remaining_null"), 1);
        let optional_values = sqlx::query(
            r#"
            SELECT group_exposure_json, remaining_credential_notional_usdt
            FROM risk_budget WHERE credential_id='paper'
            "#,
        )
        .fetch_one(store.pool())
        .await
        .expect("read nullable budget values");
        assert_eq!(
            crate::store::try_get_nullable_string(&optional_values, "group_exposure_json")
                .expect("decode NULL group exposure"),
            None
        );
        assert_eq!(
            crate::store::try_get_nullable_string(
                &optional_values,
                "remaining_credential_notional_usdt",
            )
            .expect("decode NULL remaining notional"),
            None
        );

        let execution_service = service.clone();
        let task = tokio::spawn(async move {
            execute_v2(
                &execution_service,
                protected_intent("budget_null_optionals", "95", "111"),
            )
            .await
        });
        let ack = task
            .await
            .expect("risk evaluation with SQL NULL optionals must not panic");
        assert_eq!(ack.status, Status::Accepted as i32, "{}", ack.message);

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn corrupted_non_null_optional_budget_value_fails_closed() {
        let (store, path) = empty_execution_fixture("budget-corrupt-optional").await;
        let service = EngineExecutionV2Service::new(store.clone());
        let mut request = budget_request(crate::svc::now_ms(), "10000");
        request.credential_id = "paper".to_string();
        request.group_exposure_json = None;
        request.remaining_credential_notional_usdt = None;
        service
            .put_risk_budget(Request::new(request))
            .await
            .expect("seed budget through PutRiskBudget");

        sqlx::query(
            "UPDATE risk_budget SET remaining_credential_notional_usdt=X'80' WHERE credential_id='paper'",
        )
        .execute(store.pool())
        .await
        .expect("inject non-NULL value that cannot decode as text");

        let execution_service = service.clone();
        let task = tokio::spawn(async move {
            execute_v2(
                &execution_service,
                protected_intent("budget_corrupt_optional", "95", "111"),
            )
            .await
        });
        let ack = task
            .await
            .expect("corrupt optional budget data must not panic the worker");
        assert_eq!(ack.status, Status::Rejected as i32);
        assert_eq!(ack.reject_reason_code, "STALE_RISK_BUDGET");

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn put_risk_budget_upserts_and_ignores_older_as_of_ms() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-v2-budget-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect v2 budget test store");
        let service = EngineExecutionV2Service::new(store.clone());

        let first = service
            .put_risk_budget(Request::new(budget_request(1_000, "10000")))
            .await
            .expect("first budget write")
            .into_inner();
        assert!(first.accepted);
        assert!(first.applied);

        let second = service
            .put_risk_budget(Request::new(budget_request(2_000, "12000")))
            .await
            .expect("newer budget write")
            .into_inner();
        assert!(second.applied);
        assert_eq!(second.current_as_of_ms, 2_000);

        let older = service
            .put_risk_budget(Request::new(budget_request(1_500, "1")))
            .await
            .expect("older budget is ignored")
            .into_inner();
        assert!(older.accepted);
        assert!(!older.applied);
        assert_eq!(older.message, "ignored_older_as_of");
        assert_eq!(older.current_as_of_ms, 2_000);

        let row = sqlx::query(
            "SELECT as_of_ms, equity_usdt FROM risk_budget WHERE credential_id = 'cred_budget'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read final budget row");
        assert_eq!(row.get::<i64, _>("as_of_ms"), 2_000);
        assert_eq!(row.get::<String, _>("equity_usdt"), "12000");

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_contract_binds_source_to_scope_and_level() {
        use super::validate_kill_switch_trigger_contract as check;
        use crate::kill_switch::Level;

        // 合法组合
        assert!(check("auto_llm_failures", "bot:bot_x", Level::PauseNew).is_ok());
        assert!(check("auto_reconcile_diff", "cred:c1", Level::Flatten).is_ok());
        assert!(check("manual_api", "global", Level::Flatten).is_ok());

        // **段级评审 regression 的复现**：LLM 源不得升级成 credential 强平。
        assert!(check("auto_llm_failures", "cred:c1", Level::Flatten).is_err());
        assert!(check("auto_llm_failures", "bot:bot_x", Level::Flatten).is_err());
        assert!(check("auto_llm_failures", "cred:c1", Level::PauseNew).is_err());
        assert!(check("auto_llm_failures", "global", Level::PauseNew).is_err());

        // 对账源同样被绑死
        assert!(check("auto_reconcile_diff", "bot:bot_x", Level::Flatten).is_err());
        assert!(check("auto_reconcile_diff", "cred:c1", Level::PauseNew).is_err());

        // 引擎侧自判的两个源不得从外部进来
        assert!(check("auto_market_data_interruption", "cred:c1", Level::PauseNew).is_err());
        assert!(check("auto_order_reject_rate", "cred:c1", Level::ManageOnly).is_err());

        // 空 scope 前缀不算合法 scope
        assert!(check("auto_llm_failures", "bot:", Level::PauseNew).is_err());
    }

    /// `SetKillSwitch` 的 trigger_source 是**闭世界 allowlist**。
    ///
    /// 新增的 `auto_llm_failures` 必须被接受（LLM 失败信号只有编排器看得到），
    /// 而**引擎侧自己判定的两个 source 不得被外部冒用**——它们的判据活在
    /// rustcore 的权威表里，外部若能顶着这两个名字 arm，审计里就会出现
    /// 一条"引擎自己判的"却其实来自网络的记录。
    #[tokio::test]
    async fn v2_set_kill_switch_trigger_source_allowlist_is_closed_world() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-v2-kill-src-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect trigger-source test store");
        let service = EngineExecutionV2Service::new(store.clone());

        let accepted = service
            .set_kill_switch(Request::new(SetKillSwitchRequest {
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: "bot:bot_llm".to_string(),
                level: "pause_new".to_string(),
                reason: "llm transport failures 3/3".to_string(),
                trigger_source: "auto_llm_failures".to_string(),
                // 自动源不要求 actor（人工源才要）。
                actor: String::new(),
                schema_version: 1,
            }))
            .await
            .expect("auto_llm_failures must be accepted")
            .into_inner();
        assert!(accepted.accepted);
        assert_eq!(accepted.level, "pause_new");

        for spoofed in [
            "auto_market_data_interruption",
            "auto_order_reject_rate",
            "auto_anything_else",
        ] {
            let rejected = service
                .set_kill_switch(Request::new(SetKillSwitchRequest {
                    expected_authority_revision: None,
                    authority_member_id: None,
                    request_ref: None,
                    scope_key: "bot:bot_llm".to_string(),
                    level: "flatten".to_string(),
                    reason: "spoof".to_string(),
                    trigger_source: spoofed.to_string(),
                    actor: "attacker".to_string(),
                    schema_version: 1,
                }))
                .await;
            assert!(
                rejected.is_err(),
                "{spoofed} must not be settable through the RPC"
            );
        }

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **结构化人工处置走的是
    /// 生产入口本身。**
    ///
    /// 0028 之前写下、来源在数据里答不出来的 reconcile status 行会被启动期
    /// 复核判成 `quarantined`：不投票、不阻塞，但这一轮的 expected 集合可能
    /// 是残缺的。运维必须有一条出口把它重新冻结——出口不是一个新 RPC，
    /// 而是**已经接线的那一个**：`SetKillSwitch(manual_api, flatten, actor)`。
    ///
    /// 这条用例因此从 `EngineExecutionV2Service::set_kill_switch` 进（生产
    /// gRPC 服务的那个方法），不直接调 `kill_switch::arm`：
    ///
    /// * 正向——本代带着 quarantine 行时，人工重新 arm **开新 revision**
    ///   （代际 +1，按当前成员资格重新冻结），而不是幂等早返回；
    /// * 反向——`auto_reconcile_diff`（同 scope、同档位的**自动**源）
    ///   走同一个方法时**不得**推代际；处置完之后同一条人工请求也不再推。
    #[tokio::test]
    async fn v2_manual_arm_refreezes_a_revision_whose_targets_cannot_be_proven() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-v2-refreeze-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect refreeze test store");
        let service = EngineExecutionV2Service::new(store.clone());
        let scope_key = "cred:r20refreeze";

        let arm_request = |trigger_source: &str, actor: &str| SetKillSwitchRequest {
            expected_authority_revision: None,
            authority_member_id: None,
            request_ref: None,
            scope_key: scope_key.to_string(),
            level: "flatten".to_string(),
            reason: "r20 refreeze".to_string(),
            trigger_source: trigger_source.to_string(),
            actor: actor.to_string(),
            schema_version: 1,
        };
        let generation = |store: Store| async move {
            sqlx::query("SELECT flatten_generation FROM kill_switch_state WHERE scope_key=?")
                .bind(scope_key)
                .fetch_one(store.pool())
                .await
                .expect("read the flatten generation")
                .get::<i64, _>("flatten_generation")
        };

        service
            .set_kill_switch(Request::new(arm_request("manual_api", "admin")))
            .await
            .expect("arm L3 through the production entry point");
        assert_eq!(generation(store.clone()).await, 1);

        // 0028 之前的存量：一条来源被抹掉、带着真实墙钟的 outsider 行。
        sqlx::query(
            "INSERT INTO kill_switch_reconcile_status (scope_key, credential_id, \
             reconcile_clean, flatten_complete, reconciled_at_ms, flatten_generation, \
             expected_provenance) VALUES (?, 'r20ghost', 1, 1, 2100, 1, NULL)",
        )
        .bind(scope_key)
        .execute(store.pool())
        .await
        .expect("plant a pre-0028 row the projection could have written");
        crate::kill_switch::revalidate_legacy_reconcile_targets(&store, 100)
            .await
            .expect("revalidate the legacy row");

        // 反向：自动源走同一个方法不得推代际。
        service
            .set_kill_switch(Request::new(arm_request("auto_reconcile_diff", "")))
            .await
            .expect("an automatic re-arm of an already latched scope is accepted");
        assert_eq!(
            generation(store.clone()).await,
            1,
            "an automatic source must not open a new revision through the refreeze door"
        );

        // 正向：人工重新 arm 开新 revision。
        service
            .set_kill_switch(Request::new(arm_request("manual_api", "admin")))
            .await
            .expect("an operator may refreeze through the production entry point");
        assert_eq!(
            generation(store.clone()).await,
            2,
            "a manual re-arm must refreeze a revision whose target set cannot be proven"
        );

        // 反向：处置完之后同一条人工请求恢复幂等。
        service
            .set_kill_switch(Request::new(arm_request("manual_api", "admin")))
            .await
            .expect("re-arming the refrozen scope is idempotent again");
        assert_eq!(
            generation(store.clone()).await,
            2,
            "the refreeze door must close once the revision's target set is provable"
        );

        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn v2_set_and_reset_kill_switch_enforce_clean_reconcile_precondition() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-v2-kill-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect v2 kill-switch test store");
        let service = EngineExecutionV2Service::new(store.clone());
        let armed = service
            .set_kill_switch(Request::new(SetKillSwitchRequest {
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: "cred:rpc".to_string(),
                level: "flatten".to_string(),
                reason: "offline rpc test".to_string(),
                trigger_source: "manual_api".to_string(),
                actor: "admin".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("set kill switch")
            .into_inner();
        assert!(armed.accepted);
        assert_eq!(armed.flatten_state, "flattening");

        let rejected = service
            .reset_kill_switch(Request::new(ResetKillSwitchRequest {
                reset_authority: None,
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: "cred:rpc".to_string(),
                reset_by: "admin".to_string(),
                schema_version: 1,
            }))
            .await
            .expect_err("reset without clean reconcile must fail");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);

        crate::kill_switch::record_reconcile_for_scope_now(
            &store,
            "cred:rpc",
            "rpc",
            crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: true,
                all_open_orders_empty: true,
                all_open_algo_orders_empty: true,
                local_position_zero_diff: true,
            },
            crate::svc::now_ms() + 1,
        )
        .await
        .expect("record clean reconcile");
        let reset = service
            .reset_kill_switch(Request::new(ResetKillSwitchRequest {
                reset_authority: None,
                expected_authority_revision: None,
                authority_member_id: None,
                request_ref: None,
                scope_key: "cred:rpc".to_string(),
                reset_by: "admin".to_string(),
                schema_version: 1,
            }))
            .await
            .expect("reset after clean reconcile")
            .into_inner();
        assert!(reset.accepted);
        assert_eq!(reset.level, "none");

        let event = sqlx::query(
            "SELECT reset_by, CAST(reset_at_ms AS CHAR) AS reset_at_ms_text FROM kill_switch_events WHERE scope_key='cred:rpc'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read reset audit event");
        assert_eq!(event.get::<String, _>("reset_by"), "admin");
        assert!(
            event
                .get::<String, _>("reset_at_ms_text")
                .parse::<i64>()
                .expect("reset timestamp")
                > 0
        );
        drop(service);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod loopback_test_support {
    use std::io;
    use std::net::SocketAddr;
    use std::time::Duration;

    use tonic::codec::ProstCodec;
    use tonic::codegen::http::uri::PathAndQuery;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Channel, Endpoint};
    use tonic::Request;

    use crate::proto_v2::{
        ExecuteIntentsRequest, ExecuteIntentsResponse, SetKillSwitchRequest, SetKillSwitchResponse,
    };

    const EPERM_RAW_OS_ERROR: i32 = 1;
    const EXECUTE_INTENTS_PATH: &str = "/execution_engine.v2.EngineExecution/ExecuteIntents";
    const SET_KILL_SWITCH_PATH: &str = "/execution_engine.v2.EngineExecution/SetKillSwitch";

    pub(super) struct BoundLoopback {
        pub(super) addr: SocketAddr,
        pub(super) incoming: TcpIncoming,
        pub(super) identity_token: String,
        pub(super) identity_label: String,
    }

    #[derive(Clone)]
    pub(super) struct BoundLoopbackClient {
        pub(super) addr: SocketAddr,
        pub(super) identity_token: String,
        pub(super) identity_label: String,
    }

    impl BoundLoopback {
        pub(super) fn client(&self) -> BoundLoopbackClient {
            BoundLoopbackClient {
                addr: self.addr,
                identity_token: self.identity_token.clone(),
                identity_label: self.identity_label.clone(),
            }
        }
    }

    fn is_eperm(error: &io::Error) -> bool {
        error.raw_os_error() == Some(EPERM_RAW_OS_ERROR)
    }

    fn require_local_socket_tests() -> bool {
        std::env::var("RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS").as_deref() == Ok("1")
    }

    fn may_skip_bind(error: &io::Error, require_local_socket_tests: bool) -> bool {
        is_eperm(error) && !require_local_socket_tests
    }

    /// Bind once and hand this exact listener to tonic. There is no
    /// reserve/drop/rebind window in which another process can claim the port.
    pub(super) async fn bind_owned_loopback(
        test_name: &str,
        token_prefix: &str,
    ) -> Option<BoundLoopback> {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if may_skip_bind(&error, require_local_socket_tests()) => {
                eprintln!(
                    "LOOPBACK TEST SKIP: {test_name}: bind 127.0.0.1:0 failed with \
                     EPERM/Operation not permitted (raw_os_error={:?}); \
                     RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS is not 1",
                    error.raw_os_error()
                );
                return None;
            }
            Err(error) => panic!(
                "{test_name}: bind 127.0.0.1:0 failed and is not skippable: {error} \
                 (raw_os_error={:?}, require_local_socket_tests={})",
                error.raw_os_error(),
                require_local_socket_tests()
            ),
        };
        let addr = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("{test_name}: read owned listener address: {error}"));
        let incoming = TcpIncoming::from_listener(listener, true, None)
            .unwrap_or_else(|error| panic!("{test_name}: hand owned listener to tonic: {error}"));
        let identity_label = format!("{test_name}-{}", ulid::Ulid::new());
        let identity_token = format!("{token_prefix}-{identity_label}");
        Some(BoundLoopback {
            addr,
            incoming,
            identity_token,
            identity_label,
        })
    }

    async fn unary(
        channel: Channel,
        token: Option<&str>,
        request: ExecuteIntentsRequest,
    ) -> Result<ExecuteIntentsResponse, tonic::Status> {
        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .expect("the owned production server must become ready");
        let mut request = Request::new(request);
        if let Some(token) = token {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .expect("valid metadata value"),
            );
        }
        let codec: ProstCodec<ExecuteIntentsRequest, ExecuteIntentsResponse> =
            ProstCodec::default();
        client
            .unary(
                request,
                PathAndQuery::from_static(EXECUTE_INTENTS_PATH),
                codec,
            )
            .await
            .map(|response| response.into_inner())
    }

    /// Connect, prove the peer owns this test's unique bearer identity, then
    /// issue the formal request over the same channel. The identity request has
    /// no intents, so it traverses the real interceptor and v2 adapter without
    /// creating an order or starting an exchange submission.
    pub(super) async fn execute_after_identity_probe(
        server: &BoundLoopbackClient,
        formal_token: Option<&str>,
        request: ExecuteIntentsRequest,
    ) -> Result<ExecuteIntentsResponse, tonic::Status> {
        let mut identity_request = request.clone();
        identity_request.intents.clear();
        let channel = connect_and_prove_identity(server, identity_request).await;
        unary(channel, formal_token, request).await
    }

    /// The same owned-listener identity proof, then one real `SetKillSwitch`
    /// unary (TCP, HTTP/2, protobuf, interceptor) over that proven channel.
    pub(super) async fn set_kill_switch_after_identity_probe(
        server: &BoundLoopbackClient,
        formal_token: Option<&str>,
        mut identity_request: ExecuteIntentsRequest,
        request: SetKillSwitchRequest,
    ) -> Result<SetKillSwitchResponse, tonic::Status> {
        identity_request.intents.clear();
        let channel = connect_and_prove_identity(server, identity_request).await;
        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .expect("the owned production server must become ready");
        let mut request = Request::new(request);
        if let Some(token) = formal_token {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .expect("valid metadata value"),
            );
        }
        let codec: ProstCodec<SetKillSwitchRequest, SetKillSwitchResponse> = ProstCodec::default();
        client
            .unary(
                request,
                PathAndQuery::from_static(SET_KILL_SWITCH_PATH),
                codec,
            )
            .await
            .map(|response| response.into_inner())
    }

    async fn connect_and_prove_identity(
        server: &BoundLoopbackClient,
        identity_request: ExecuteIntentsRequest,
    ) -> Channel {
        let channel = Endpoint::from_shared(format!("http://{}", server.addr))
            .expect("valid owned loopback endpoint")
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "loopback identity connection failed: identity={} \
                     expected_listener_addr={} client_target_addr=http://{}: {error}",
                    server.identity_label, server.addr, server.addr
                )
            });

        let identity_response = unary(
            channel.clone(),
            Some(&server.identity_token),
            identity_request,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "loopback identity mismatch: identity={} expected_listener_addr={} \
                 client_target_addr=http://{} status={error}",
                server.identity_label, server.addr, server.addr
            )
        });
        assert!(
            identity_response.acks.is_empty(),
            "loopback identity probe must be side-effect free: identity={} \
             expected_listener_addr={} client_target_addr=http://{} acks={:?}",
            server.identity_label,
            server.addr,
            server.addr,
            identity_response.acks
        );

        channel
    }

    #[test]
    fn only_eperm_is_skippable_and_required_host_mode_disables_skip() {
        let eperm = io::Error::from_raw_os_error(EPERM_RAW_OS_ERROR);
        let eacces = io::Error::from_raw_os_error(13);
        let address_in_use = io::Error::from_raw_os_error(98);

        assert!(may_skip_bind(&eperm, false));
        assert!(!may_skip_bind(&eperm, true));
        assert!(!may_skip_bind(&eacces, false));
        assert!(!may_skip_bind(&address_in_use, false));
    }

    #[test]
    fn wiring_servers_keep_owned_listeners_and_probe_unique_identity() {
        let source = include_str!("svc_v2.rs");
        let incoming_shape = [".serve_with_", "incoming_shutdown(bound.incoming"].concat();
        let identity_shape = [
            "execute_after_identity_",
            "probe(server, token, request).await",
        ]
        .concat();
        let bind_shape = [
            "let bound = bind_owned_",
            "loopback(test_name, AUTH_TOKEN_PREFIX).await?",
        ]
        .concat();
        let reserve_drop_shape = ["reserve an ephemeral", " port"].concat();
        let rebind_shape = [".serve_with_", "shutdown(addr"].concat();

        assert_eq!(
            source.matches(&incoming_shape).count(),
            2,
            "both loopback server helpers must hand tonic their already-bound listener"
        );
        assert_eq!(
            source.matches(&identity_shape).count(),
            2,
            "both wiring modules must probe the per-test identity before formal requests"
        );
        assert_eq!(
            source.matches(&bind_shape).count(),
            2,
            "both wiring modules must use the owned-listener helper"
        );
        assert!(
            !source.contains(&reserve_drop_shape),
            "reserve/drop/rebind must not return"
        );
        assert!(
            !source.contains(&rebind_shape),
            "a listener-dependent wiring server must not rebind a released address"
        );
    }
}

/// **v2 gRPC 生产入口的完整 wiring proof。**
///
/// R9 的五条提交用例走的是 v1 的 trait helper
/// （`EngineExecution::execute_intents(&exec, request)`，`svc.rs`），
/// 而 Python 编排器真实调用的是 **v2 的 gRPC 方法**。两者之间隔着
/// `svc_v2::execute_intents` 的整段规范化/映射（action→role/side、
/// `order_type`/`limit_price` 的转写、protection→`V2IntentContext`、
/// snapshot 映射、ack 分类与 `reject_reason_code` 解析），
/// 那一段**从未被任何用例执行过**——评审变异「破坏 v2 `order_type` 映射」
/// 5/5 存活。
///
/// 这一组用例按 engine safety contract 的四条办：
/// * **入口同一**：服务装配调 `main` 用的那个符号（`crate::add_engine_services`），
///   请求经**真实 TCP socket + 真实 HTTP/2 + 真实 protobuf 编解码**
///   （`ProstCodec`，与生成客户端同一个 codec）打到
///   `/execution_engine.v2.EngineExecution/ExecuteIntents` 这条真实路径上。
/// * **不跳层**：auth interceptor、v2 adapter、v1 shared core、
///   交易所提交客户端的分类，一层都没绕。
/// * **两侧真值同源**：intent 里的 `order_type`/`limit_price` 由用例一侧产出，
///   由**交易所那一端真实消费**——断言落在客户端实际发出的
///   `type=LIMIT&…&price=100` 那条 query 上，不是两边各调一次同一个函数。
/// * **反向可断言**：无 token 被 interceptor 挡在服务之外（库里一行都没有）；
///   非法 `order_type` 被判 `Rejected` 且**一个交易所请求都没发出**。
///
/// **替身缝在链路之外**：只有"交易所"这一端是内存 HTTP 队列
/// （`new_with_mock_responses`）+ 固定过滤器真值。transport 编解码与
/// interceptor **没有替**——它们正是被证明的对象。
#[cfg(test)]
mod r10_v2_production_entry_wiring_tests {
    include!("svc_v2_dispatch_tests.rs");
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::Once;
    use std::time::Duration;

    use sqlx::Row;
    use tonic::transport::Server;

    use super::loopback_test_support::{
        bind_owned_loopback, execute_after_identity_probe, BoundLoopbackClient,
    };
    use super::EngineExecutionV2Service;
    use crate::binance_futures::BinanceFuturesClient;
    use crate::proto_v2::{
        DataSnapshotDecision, ExecuteIntentsRequest, ExecuteIntentsResponse, OrderIntent,
        Protection,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const AUTH_TOKEN_PREFIX: &str = "r10-v2-wiring-token";
    const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";

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

    /// 交易所凭据的最小真实环境：显式 testnet venue + 一对密钥。
    fn exchange_credential_env(credential_id: &str) -> Vec<EnvGuard> {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        vec![
            EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet"),
            EnvGuard::set(key_env, "r10-v2-key"),
            EnvGuard::set(secret_env, "r10-v2-secret"),
        ]
    }

    async fn wiring_store(credential_id: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-r10v2-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES (?, ?, '10000', '10000', '0', 0, 0, 0, 0,
                      '{"majors":0,"l1_alts":0}', 'none', '5000')
            "#,
        )
        .bind(credential_id)
        .bind(crate::svc::now_ms().to_string())
        .execute(store.pool())
        .await
        .expect("insert fresh risk budget");
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, entry_price, updated_at_ms
            ) VALUES ('pos_r10v2', ?, 'bot_r10v2', ?, 'long',
                      1, 0, 'open', 'bot_r10v2|binance|futures|BTC|USDT|USDT|long|1',
                      '2', '100', 1)
            "#,
        )
        .bind(credential_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("insert closable long position");
        (store, path)
    }

    /// 起一台**生产装配**的 gRPC server（含 auth interceptor），
    /// 返回它的地址、交易所替身客户端（用于读回真实发出的请求）与关停句柄。
    async fn start_engine(
        test_name: &str,
        store: &Store,
        credential_id: &str,
        responses: Vec<(u16, String)>,
    ) -> Option<(
        BoundLoopbackClient,
        BinanceFuturesClient,
        tokio::sync::oneshot::Sender<()>,
    )> {
        let submit_client = BinanceFuturesClient::new_with_mock_responses(credential_id, responses);
        let exec = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(submit_client.clone());
        let events =
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800));
        let exec_v2 = EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
        let events_v2 = super::EngineEventsV2Skeleton;

        let bound = bind_owned_loopback(test_name, AUTH_TOKEN_PREFIX).await?;
        let client = bound.client();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let router = crate::add_engine_services(
            Server::builder(),
            exec,
            events,
            exec_v2,
            events_v2,
            // 生产的 token 校验路径（`RUSTCORE_GRPC_AUTH_TOKENS` 解析出来的就是
            // 这样一个 `Vec<String>`）。
            Some(crate::AuthInterceptor::new(vec![bound
                .identity_token
                .clone()])),
        );
        tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(bound.incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("owned loopback gRPC server must run");
        });
        Some((client, submit_client, shutdown_tx))
    }

    /// **真实客户端调用**：TCP → HTTP/2 → protobuf → 那条真实的方法路径。
    async fn execute_intents_over_grpc(
        server: &BoundLoopbackClient,
        token: Option<&str>,
        request: ExecuteIntentsRequest,
    ) -> Result<ExecuteIntentsResponse, tonic::Status> {
        execute_after_identity_probe(server, token, request).await
    }

    fn close_intent(label: &str, order_type: &str, limit_price: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: "bot_r10v2".to_string(),
            run_id: "run_r10v2".to_string(),
            cycle_id: format!("cycle_{label}"),
            idempotency_key: format!("idem_{label}"),
            client_order_id_short: format!("coid_{label}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "close_long".to_string(),
            order_type: order_type.to_string(),
            limit_price: limit_price.to_string(),
            reduce_only: true,
            intent_id: format!("int_{label}"),
            max_slippage_bps: 15,
            protection: None,
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: crate::svc::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        }
    }

    fn request_for(credential_id: &str, intent: OrderIntent) -> ExecuteIntentsRequest {
        ExecuteIntentsRequest {
            credential_id: credential_id.to_string(),
            run_mode: "demo".to_string(),
            intents: vec![intent],
            data_snapshot: Some(DataSnapshotDecision {
                snapshot_id: format!("snap_{}", "a".repeat(32)),
                quality_gate: "pass".to_string(),
                allow_open: true,
                allow_close: true,
                created_at_ms: crate::svc::now_ms(),
                schema_version: 1,
            }),
            schema_version: 1,
        }
    }

    fn ack_body(order_id: i64, client_order_id: &str, order_type: &str) -> String {
        serde_json::json!({
            "orderId": order_id,
            "clientOrderId": client_order_id,
            "status": "NEW",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": order_type,
            "origQty": "2",
            "executedQty": "0"
        })
        .to_string()
    }

    fn algo_ack_body(algo_id: i64, client_algo_id: &str, order_type: &str) -> String {
        serde_json::json!({
            "algoId": algo_id,
            "clientAlgoId": client_algo_id,
            "algoStatus": "NEW",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": order_type
        })
        .to_string()
    }

    fn protection_client_algo_id(position_id: &str, kind: &str) -> String {
        let digest = crate::svc::sha256_hex(&format!("prot|{position_id}|{kind}|1"));
        format!("p{}", &digest[..31])
    }

    async fn order_row(store: &Store) -> (String, String, String) {
        let row = sqlx::query(
            "SELECT status, order_type, COALESCE(price,'') AS price \
             FROM engine_orders ORDER BY created_at_ms DESC LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("the production entry must have inserted an engine order");
        (
            row.get::<String, _>("status"),
            row.get::<String, _>("order_type"),
            row.get::<String, _>("price"),
        )
    }

    async fn order_count(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
            .fetch_one(store.pool())
            .await
            .expect("count orders")
            .get::<i64, _>("n")
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **`order_type=limit` 必须一路变成交易所的 `type=LIMIT` + `price`。**
    ///
    /// 变异「破坏 v2 `order_type` 映射」（例如把 `intent.order_type` 写死成
    /// `"market"`、或把 `price: intent.limit_price` 换成空串）会让这条断言
    /// 直接红：交易所收到的 query 里不再有 `type=LIMIT`/`price=100`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_v2_limit_intent_reaches_the_exchange_as_a_limit_order() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-limit";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::a_v2_limit_intent_reaches_the_exchange_as_a_limit_order",
            &store,
            credential_id,
            vec![(200, ack_body(770011, "coid_r10v2_limit", "LIMIT"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, close_intent("r10v2_limit", "limit", "100")),
        )
        .await
        .expect("the v2 gRPC entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "ack: {ack:?}"
        );
        assert!(
            !ack.risk_verdict_id.is_empty(),
            "the v2 adapter must project the risk verdict it really wrote: {ack:?}"
        );

        let (status, order_type, price) = order_row(&store).await;
        assert_eq!(status, "acked");
        assert_eq!(
            order_type, "limit",
            "the v2 order_type must survive the mapping"
        );
        assert_eq!(price, "100");

        let requests = exchange.recorded_requests();
        assert_eq!(
            requests.len(),
            1,
            "exactly one exchange submission: {requests:?}"
        );
        assert!(
            requests[0].contains("type=LIMIT"),
            "the exchange must receive a LIMIT order: {}",
            requests[0]
        );
        assert!(
            requests[0].contains("price=100"),
            "the limit price must reach the exchange: {}",
            requests[0]
        );
        cleanup(store, path).await;
    }

    /// **`order_type=market` 必须变成 `type=MARKET`（且不带 limit 价）。**
    /// 与上一条成对：任何把 order_type 写死成某一个常量的变异，
    /// 两条里必有一条红。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_v2_market_intent_reaches_the_exchange_as_a_market_order() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-market";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::a_v2_market_intent_reaches_the_exchange_as_a_market_order",
            &store,
            credential_id,
            vec![(200, ack_body(770022, "coid_r10v2_market", "MARKET"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, close_intent("r10v2_market", "market", "")),
        )
        .await
        .expect("the v2 gRPC entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "ack: {ack:?}"
        );

        let (status, order_type, _price) = order_row(&store).await;
        assert_eq!(status, "acked");
        assert_eq!(order_type, "market");

        let requests = exchange.recorded_requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(
            requests[0].contains("type=MARKET"),
            "the exchange must receive a MARKET order: {}",
            requests[0]
        );
        assert!(
            !requests[0].contains("type=LIMIT"),
            "a market intent must never be submitted as a limit order: {}",
            requests[0]
        );
        cleanup(store, path).await;
    }

    /// Regression for Binance -1102: the v2 wire has no quantity field, so
    /// the production entry must load the full engine position, normalize it
    /// to LOT_SIZE precision, and submit an explicit reduce-only quantity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_close_derives_and_rounds_the_authoritative_position_quantity() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-close-qty";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        sqlx::query("UPDATE position_state SET qty='0.01571' WHERE position_id='pos_r10v2'")
            .execute(store.pool())
            .await
            .expect("seed a non-step-aligned authoritative position");
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::v2_close_derives_and_rounds_the_authoritative_position_quantity",
            &store,
            credential_id,
            vec![(200, ack_body(770044, "coid_r10v2_close_qty", "MARKET"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, close_intent("r10v2_close_qty", "market", "")),
        )
        .await
        .expect("the v2 production entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "{ack:?}"
        );

        let requests = exchange.recorded_requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(requests[0].contains("quantity=0.0158"), "{}", requests[0]);
        assert!(requests[0].contains("reduceOnly=true"), "{}", requests[0]);
        assert!(!requests[0].contains("quantity=&"), "{}", requests[0]);
        let stored = sqlx::query(
            "SELECT quantity, reduce_only FROM engine_orders ORDER BY created_at_ms DESC LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the exchange-bound order row");
        assert_eq!(stored.get::<String, _>("quantity"), "0.0158");
        assert_eq!(stored.get::<i64, _>("reduce_only"), 1);
        cleanup(store, path).await;
    }

    /// SL and TP used to preserve the v2 adapter's empty quantity and switch
    /// to closePosition=true. The live algo endpoint rejected that exact
    /// request shape with -1102. Both generated children must now carry the
    /// same normalized full-position quantity and remain reduce-only.
    #[tokio::test]
    async fn planned_sl_and_tp_never_submit_an_empty_quantity() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-protection-qty";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        sqlx::query("UPDATE position_state SET qty='0.01571' WHERE position_id='pos_r10v2'")
            .execute(store.pool())
            .await
            .expect("seed a non-step-aligned authoritative position");
        sqlx::query(
            r#"
            INSERT INTO position_protection_plans (
              position_id, credential_id, bot_uid, run_id, cycle_id,
              exchange, market_kind, symbol_key, direction,
              stop_loss_price, take_profit_price, required,
              confirm_timeout_ms, generation, created_at_ms, updated_at_ms
            ) VALUES (
              'pos_r10v2', ?, 'bot_r10v2', 'run_protection', 'cycle_protection',
              'binance', 'futures', ?, 'long', '90', '110', 1,
              300000, 1, 1, 1
            )
            "#,
        )
        .bind(credential_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed the typed protection plan");

        let sl_id = protection_client_algo_id("pos_r10v2", "sl");
        let tp_id = protection_client_algo_id("pos_r10v2", "tp");
        let exchange = BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![
                (200, algo_ack_body(880011, &sl_id, "STOP_MARKET")),
                (200, algo_ack_body(880012, &tp_id, "TAKE_PROFIT_MARKET")),
            ],
        );
        let service = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(exchange.clone());

        let acks = service
            .submit_planned_protection("pos_r10v2", "demo")
            .await
            .expect("both protection children must submit");
        assert_eq!(acks.len(), 2, "{acks:?}");
        assert!(acks.iter().all(|ack| ack.status == "accepted"), "{acks:?}");

        let requests = exchange.recorded_requests();
        assert_eq!(requests.len(), 2, "{requests:?}");
        for request in &requests {
            assert!(request.contains("quantity=0.0158"), "{request}");
            assert!(request.contains("reduceOnly=true"), "{request}");
            assert!(!request.contains("closePosition=true"), "{request}");
            assert!(!request.contains("quantity=&"), "{request}");
        }
        assert!(requests
            .iter()
            .any(|request| request.contains("type=STOP_MARKET")));
        assert!(requests
            .iter()
            .any(|request| request.contains("type=TAKE_PROFIT_MARKET")));
        cleanup(store, path).await;
    }

    /// **D30 wiring proof：待善后义务必须把同 scope 的新开仓挡在生产入口，
    /// 且只挡新增风险。** 通过 v2 的真实 gRPC 生产路径（TCP/HTTP2/protobuf，
    /// interceptor→v2 adapter→v1 shared core 一层不绕）：
    /// * 同 credential+symbol+long 的开仓被 `PROTECTION_CLEANUP_PENDING`
    ///   拒绝，**一个交易所请求都不发、一行订单都不落**，审计 verdict 带
    ///   同一类型码（判据在准入事务里，与订单插入同事务）；
    /// * 反方向（open_short）不受影响——照常提交并 ack；
    /// * 义务本身由生产平仓路径（`apply_filled_order_truth`）真实产生。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pending_protection_cleanup_obligation_rejects_same_scope_entry_at_admission() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-m0-cleanup";
        let _env = exchange_credential_env(credential_id);
        // 开仓是唯一会被 `StageDenial::ExceedsEffective` 拦住的 role；不抬
        // 档位，用例走不到被证明的善后闸门（seg3 同款处置，见其文档）。
        let _stage = EnvGuard::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let (store, path) = wiring_store(credential_id).await;
        // 开仓路径的健康前提：与 seg3 夹具同表，但种子是**健康**行——
        // 让拒绝只能来自被测的善后闸门，不被对账降级盖住。
        sqlx::query(
            r#"
            INSERT INTO engine_credential_health (
              credential_id, degraded, degraded_reason, consecutive_errors,
              first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
            ) VALUES (?, 0, NULL, 0, 0, 0, 1, 0)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed a healthy credential health row");
        sqlx::query(
            r#"
            INSERT INTO fill_stream_state (
              credential_id, generation, ready, phase, current_symbol,
              cursor_last_trade_id, failure_reason, updated_at_ms
            ) VALUES (?, 1, 1, 'streaming', 'BTCUSDT', NULL, NULL, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed a ready fill recovery stream state");

        // 生产平仓路径产生持久义务：wiring_store 种的 pos_r10v2（qty 2,
        // long）被 exit fill 归零 → protection_cleanup_obligations 出现
        // pending 一行（同事务）。
        let mut close_tx = store.pool().begin().await.expect("begin close tx");
        crate::position_state::apply_filled_order_truth(
            &mut close_tx,
            credential_id,
            "bot_r10v2",
            SYMBOL_KEY,
            "close_long",
            "2",
            Some("100"),
            1,
        )
        .await
        .expect("apply the exit fill through the production close path");
        close_tx.commit().await.expect("commit the close");
        let obligation = sqlx::query(
            "SELECT state FROM protection_cleanup_obligations WHERE position_id='pos_r10v2'",
        )
        .fetch_one(store.pool())
        .await
        .expect("the production close must have recorded the obligation")
        .get::<String, _>("state");
        assert_eq!(obligation, "pending");

        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::a_pending_protection_cleanup_obligation_rejects_same_scope_entry_at_admission",
            &store,
            credential_id,
            vec![(200, ack_body(770099, "coid_r10v2_m0_short", "MARKET"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let open_long = OrderIntent {
            bot_uid: "bot_r10v2".to_string(),
            run_id: "run_r10v2".to_string(),
            cycle_id: "cycle_r10v2_m0_long".to_string(),
            idempotency_key: "idem_r10v2_m0_long".to_string(), // gitleaks:allow -- Synthetic test idempotency identifier.
            client_order_id_short: "coid_r10v2_m0_long".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "open_long".to_string(),
            order_type: "limit".to_string(),
            limit_price: "100".to_string(),
            reduce_only: false,
            intent_id: "int_r10v2_m0_long".to_string(),
            max_slippage_bps: 15,
            protection: Some(Protection {
                required: true,
                stop_loss_price: "95".to_string(),
                take_profit_price: "110".to_string(),
                confirm_timeout_ms: 5_000,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: crate::svc::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        };
        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, open_long.clone()),
        )
        .await
        .expect("the v2 production entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Rejected as i32,
            "a pending cleanup obligation must reject the same-scope entry: {ack:?}"
        );
        assert_eq!(
            ack.reject_reason_code, "PROTECTION_CLEANUP_PENDING",
            "the rejection must carry the stable typed code: {ack:?}"
        );
        assert!(
            exchange.recorded_requests().is_empty(),
            "a cleanup-gated entry must never reach the exchange: {:?}",
            exchange.recorded_requests()
        );
        assert_eq!(
            order_count(&store).await,
            0,
            "admission runs in the OMS transaction: a gated entry inserts no order"
        );
        let verdict = sqlx::query(
            "SELECT decision, COALESCE(reject_reason_code,'') AS code FROM risk_verdicts \
             ORDER BY created_at_ms DESC LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("the gated entry must leave an audit verdict");
        assert_eq!(
            verdict.get::<String, _>("code"),
            "PROTECTION_CLEANUP_PENDING"
        );

        // 反方向不受影响：open_short 照常提交并 ack（只拦新增风险的同
        // scope 方向，不拦对冲/反手）。
        let open_short = OrderIntent {
            cycle_id: "cycle_r10v2_m0_short".to_string(),
            idempotency_key: "idem_r10v2_m0_short".to_string(), // gitleaks:allow -- Synthetic test idempotency identifier.
            client_order_id_short: "coid_r10v2_m0_short".to_string(),
            action: "open_short".to_string(),
            intent_id: "int_r10v2_m0_short".to_string(),
            ..open_long.clone()
        };
        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, open_short),
        )
        .await
        .expect("the v2 production entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "the opposite direction must not be gated: {ack:?}"
        );
        assert_eq!(
            exchange.recorded_requests().len(),
            1,
            "{:?}",
            exchange.recorded_requests()
        );
        assert_eq!(
            order_count(&store).await,
            1,
            "the opposite-direction entry inserts its order normally"
        );
        cleanup(store, path).await;
    }

    /// **P2-1（评审 claude-review-m0）· 闸门只拦新增风险：同 scope 存在
    /// pending 善后义务时，reduce-only 平仓与保护单提交必须照常走通生产
    /// 入口。** 把闸门的 direction 匹配扩到 close 动作的变异此前没有任何
    /// 用例会红。义务由生产平仓路径（`apply_filled_order_truth`）在同
    /// credential+symbol+long 的另一个 bot 仓上真实产生；对照断言：同
    /// scope 开仓仍被 `PROTECTION_CLEANUP_PENDING` 拒绝。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pending_cleanup_obligation_does_not_gate_same_scope_closes_or_protection() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-m0-ungated";
        let _env = exchange_credential_env(credential_id);
        // 对照臂需要开仓走到准入闸门（同 seg3/D30 闸门用例的处置）。
        let _stage = EnvGuard::set("ENGINE_STAGE_MAX".to_string(), "demo");
        let (store, path) = wiring_store(credential_id).await;

        // 同 scope 的另一个 bot 仓经生产平仓路径归零 ⇒ pending 义务。
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, exit_epoch, state, active_key, qty, entry_price, updated_at_ms
            ) VALUES ('pos_r10v2_prior', ?, 'bot_prior', ?, 'long',
                      1, 0, 'open', 'bot_prior|binance|futures|BTC|USDT|USDT|long|1',
                      '1', '100', 1)
            "#,
        )
        .bind(credential_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed the prior same-scope position");
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id,
              trigger_price, reduce_only, close_position, status,
              verified_from_exchange, created_at_ms, updated_at_ms
            ) VALUES ('prot_r10v2_prior_sl', 'pos_r10v2_prior', ?, 'sl', 1,
                      'eo_r10v2_prior_sl', NULL, '191', 'coid_r10v2_prior_sl',
                      '95', 1, 1, 'confirmed', 0, 1, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed the prior position's protection row");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange_order_id, exchange, market_kind,
              symbol_key, action, side, order_type, quantity, reduce_only,
              status, order_role, position_id, created_at_ms, updated_at_ms
            ) VALUES ('eo_r10v2_prior_sl', ?, 'bot_prior', 'idem_eo_r10v2_prior_sl',
                      'coid_r10v2_prior_sl', NULL, 'binance', 'futures',
                      ?, 'close_long', 'short', 'STOP_MARKET', '1', 1,
                      'confirmed', 'protection_sl', 'pos_r10v2_prior', 1, 1)
            "#,
        )
        .bind(credential_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed the prior protection engine order");
        let mut close_tx = store.pool().begin().await.expect("begin prior close tx");
        crate::position_state::apply_filled_order_truth(
            &mut close_tx,
            credential_id,
            "bot_prior",
            SYMBOL_KEY,
            "close_long",
            "1",
            Some("100"),
            1,
        )
        .await
        .expect("close the prior position through the production path");
        close_tx.commit().await.expect("commit the prior close");
        let obligation: String = sqlx::query(
            "SELECT state FROM protection_cleanup_obligations WHERE position_id='pos_r10v2_prior'",
        )
        .fetch_one(store.pool())
        .await
        .expect("the production close must record the obligation")
        .get("state");
        assert_eq!(obligation, "pending");

        // ① 保护单提交不被闸门波及：`submit_planned_protection` 是入场成交后
        //    引擎挂 SL/TP 的同一个生产方法（SL 先、TP 后），内部以保护角色
        //    的 close_* 意图经 execute_intents 提交——正是闸门所在的准入事务。
        sqlx::query(
            r#"
            INSERT INTO position_protection_plans (
              position_id, credential_id, bot_uid, run_id, cycle_id,
              exchange, market_kind, symbol_key, direction,
              stop_loss_price, take_profit_price, required,
              confirm_timeout_ms, generation, created_at_ms, updated_at_ms
            ) VALUES (
              'pos_r10v2', ?, 'bot_r10v2', 'run_ungated', 'cycle_ungated',
              'binance', 'futures', ?, 'long', '90', '110', 1,
              300000, 1, 1, 1
            )
            "#,
        )
        .bind(credential_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed the typed protection plan");
        let sl_id = protection_client_algo_id("pos_r10v2", "sl");
        let tp_id = protection_client_algo_id("pos_r10v2", "tp");
        let protection_exchange = BinanceFuturesClient::new_with_mock_responses(
            credential_id,
            vec![
                (200, algo_ack_body(880031, &sl_id, "STOP_MARKET")),
                (200, algo_ack_body(880032, &tp_id, "TAKE_PROFIT_MARKET")),
            ],
        );
        let service = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(protection_exchange.clone());
        let acks = service
            .submit_planned_protection("pos_r10v2", "demo")
            .await
            .expect("both protection children must submit despite the pending obligation");
        assert_eq!(acks.len(), 2, "{acks:?}");
        assert!(
            acks.iter().all(|ack| ack.status == "accepted"),
            "protection submissions must not be gated: {acks:?}"
        );
        assert_eq!(protection_exchange.recorded_requests().len(), 2);

        // ② 同 scope 的 reduce-only 平仓经真实 gRPC 生产入口照常提交。
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::a_pending_cleanup_obligation_does_not_gate_same_scope_closes_or_protection",
            &store,
            credential_id,
            vec![(200, ack_body(770041, "coid_r10v2_ungated_close", "MARKET"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };
        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(
                credential_id,
                close_intent("r10v2_ungated_close", "market", ""),
            ),
        )
        .await
        .expect("the v2 production entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Accepted as i32,
            "a same-scope reduce-only close must never be gated: {ack:?}"
        );
        assert!(
            ack.reject_reason_code.is_empty(),
            "no typed rejection may ride the close ack: {ack:?}"
        );
        assert_eq!(
            exchange.recorded_requests().len(),
            1,
            "{:?}",
            exchange.recorded_requests()
        );

        // 对照臂：同 scope 开仓仍被 PROTECTION_CLEANUP_PENDING 拒绝——闸门
        // 本身还在，只是不拦平仓/保护。
        let open_long = OrderIntent {
            bot_uid: "bot_r10v2".to_string(),
            run_id: "run_r10v2".to_string(),
            cycle_id: "cycle_r10v2_ungated_open".to_string(),
            idempotency_key: "idem_r10v2_ungated_open".to_string(),
            client_order_id_short: "coid_r10v2_ungated_open".to_string(),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "open_long".to_string(),
            order_type: "limit".to_string(),
            limit_price: "100".to_string(),
            reduce_only: false,
            intent_id: "int_r10v2_ungated_open".to_string(),
            max_slippage_bps: 15,
            protection: Some(Protection {
                required: true,
                stop_loss_price: "95".to_string(),
                take_profit_price: "110".to_string(),
                confirm_timeout_ms: 5_000,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: crate::svc::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        };
        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, open_long),
        )
        .await
        .expect("the v2 production entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Rejected as i32,
            "the control arm: the same-scope entry must still be gated: {ack:?}"
        );
        assert_eq!(
            ack.reject_reason_code, "PROTECTION_CLEANUP_PENDING",
            "the control arm must carry the stable typed code: {ack:?}"
        );
        cleanup(store, path).await;
    }

    /// **反向①：interceptor 真的在链路里。**
    /// 不带 token 的请求必须被挡在服务之外——不是"服务收下了再拒"，
    /// 而是库里连一行订单都不会出现。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unauthenticated_v2_call_never_reaches_the_engine() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-noauth";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::an_unauthenticated_v2_call_never_reaches_the_engine",
            &store,
            credential_id,
            vec![(200, ack_body(770033, "coid_r10v2_noauth", "MARKET"))],
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let error = execute_intents_over_grpc(
            &server,
            None,
            request_for(credential_id, close_intent("r10v2_noauth", "market", "")),
        )
        .await
        .expect_err("a call without a token must be refused");
        assert_eq!(error.code(), tonic::Code::Unauthenticated, "{error:?}");

        let wrong_token = execute_intents_over_grpc(
            &server,
            Some("not-the-token"),
            request_for(credential_id, close_intent("r10v2_noauth2", "market", "")),
        )
        .await
        .expect_err("a call with the wrong token must be refused");
        assert_eq!(wrong_token.code(), tonic::Code::Unauthenticated);

        assert_eq!(
            order_count(&store).await,
            0,
            "a refused call must not reach the execution core"
        );
        assert!(
            exchange.recorded_requests().is_empty(),
            "a refused call must not reach the exchange"
        );
        cleanup(store, path).await;
    }

    /// **反向②：错的 `order_type` 必须被拒，且一个交易所请求都不发。**
    /// 走的是同一条真实链路：v2 原样透传 `order_type`，v1 的
    /// `plan_exchange_submission` 判它路由不合法 → `NotStarted` → 本地拒绝。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unsupported_v2_order_type_is_rejected_before_the_exchange() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let credential_id = "r10v2-badtype";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        // 响应队列**故意留空**：任何真的发出去的请求都会变成 StartedUnknown，
        // 下面的断言随之变红。
        let Some((server, exchange, _shutdown)) = start_engine(
            "r10_v2_production_entry_wiring_tests::an_unsupported_v2_order_type_is_rejected_before_the_exchange",
            &store,
            credential_id,
            Vec::new(),
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, close_intent("r10v2_badtype", "iceberg", "")),
        )
        .await
        .expect("the v2 gRPC entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Rejected as i32,
            "ack: {ack:?}"
        );
        assert!(
            ack.message.contains("submission not started"),
            "the rejection must carry its reason: {}",
            ack.message
        );

        let (status, order_type, _price) = order_row(&store).await;
        assert_eq!(status, "rejected");
        assert_eq!(
            order_type, "iceberg",
            "the rejected order_type is recorded as sent"
        );
        assert!(
            exchange.recorded_requests().is_empty(),
            "an order that was never routable must never leave the process"
        );
        cleanup(store, path).await;
    }

    async fn seed_f1_pending_risk_exit(store: &Store, credential_id: &str, engine_order_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_f1_wiring', ?, 'binance', 'futures', ?,
              'close_long', 'short', 'market', '1', 1, 'exit', 'pending_risk', 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(credential_id)
        .bind(format!("idem-{engine_order_id}"))
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed a pending_risk exit order");
    }

    async fn f1_order_state(store: &Store, engine_order_id: &str) -> (String, i64) {
        let row =
            sqlx::query("SELECT status, updated_at_ms FROM engine_orders WHERE engine_order_id=?")
                .bind(engine_order_id)
                .fetch_one(store.pool())
                .await
                .expect("read the order state");
        (row.get("status"), row.get("updated_at_ms"))
    }

    /// **F-1 · kill-switch 本地逐单隔离的生产入口 wiring proof（engine safety contract）。**
    ///
    /// 入口是真实的 `/execution_engine.v2.EngineExecution/SetKillSwitch`：生产装配
    /// （`crate::add_engine_services` + auth interceptor）、真实 TCP/HTTP2/protobuf，
    /// 一路走到 `apply_local_order_semantics` 的逐单处置。
    ///
    /// * 正向：同 scope 里一张 0009 之前形状（`order_role=NULL`）的 `pending_risk`
    ///   滞留行**不再**挡住两张合法平仓兄弟单——它们真的被撤；
    /// * 失败可见：RPC 仍是 `internal`，错误里点名那张失败单；重复调用依旧失败，
    ///   失败行逐字段原样；闩锁本身已 durable；
    /// * 反向：无 token 被 interceptor 挡在外面，库里一行都没动；
    ///   没有失败行的对照 scope 走同一入口返回 `accepted`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn set_kill_switch_isolates_a_failing_local_cancel_and_still_reports_it() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        // `set_kill_switch` 只在 live 未开启时同步执行本地处置。
        let _live = EnvGuard::set("ENGINE_LIVE_ENABLED".to_string(), "false");
        let credential_id = "cred_f1_wiring";
        let control_credential_id = "cred_f1_control";
        let (store, path) = wiring_store(credential_id).await;
        let Some((server, exchange, shutdown)) = start_engine(
            "set_kill_switch_isolates_a_failing_local_cancel_and_still_reports_it",
            &store,
            credential_id,
            Vec::new(),
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        // 验证公共入口的完整候选集合；具体扫描顺序由数据库决定。
        seed_f1_pending_risk_exit(&store, credential_id, "aaa_f1_exit").await;
        seed_f1_pending_risk_exit(&store, credential_id, "mmm_f1_legacy").await;
        seed_f1_pending_risk_exit(&store, credential_id, "zzz_f1_exit").await;
        let legacy = sqlx::query(
            "UPDATE engine_orders SET order_role=NULL, position_id=NULL \
             WHERE engine_order_id='mmm_f1_legacy'",
        )
        .execute(store.pool())
        .await
        .expect("shape the pre-0009 legacy row")
        .rows_affected();
        assert_eq!(legacy, 1);
        seed_f1_pending_risk_exit(&store, control_credential_id, "ctl_f1_exit").await;

        let scope_key = format!("cred:{credential_id}");
        let mut scan: Vec<String> = crate::kill_switch::local_cancel_candidates(
            &store,
            &scope_key,
            crate::kill_switch::Level::Flatten,
        )
        .await
        .expect("read the complete candidate set")
        .into_iter()
        .map(|candidate| candidate.engine_order_id)
        .collect();
        scan.sort();
        assert_eq!(scan, ["aaa_f1_exit", "mmm_f1_legacy", "zzz_f1_exit"]);

        let arm_request = |credential_id: &str| crate::proto_v2::SetKillSwitchRequest {
            expected_authority_revision: None,
            authority_member_id: None,
            request_ref: None,
            scope_key: format!("cred:{credential_id}"),
            level: "flatten".to_string(),
            reason: "f1 wiring".to_string(),
            trigger_source: "manual_api".to_string(),
            actor: "admin".to_string(),
            schema_version: 1,
        };
        let probe = || request_for(credential_id, close_intent("f1_probe", "market", ""));

        // 反向：无 token 进不来，什么都没动。
        let refused = super::loopback_test_support::set_kill_switch_after_identity_probe(
            &server,
            None,
            probe(),
            arm_request(credential_id),
        )
        .await
        .expect_err("a call without a token must be refused");
        assert_eq!(refused.code(), tonic::Code::Unauthenticated, "{refused:?}");
        for id in ["aaa_f1_exit", "mmm_f1_legacy", "zzz_f1_exit"] {
            assert_eq!(
                f1_order_state(&store, id).await,
                ("pending_risk".to_string(), 1)
            );
        }

        for attempt in 1..=2 {
            let error = super::loopback_test_support::set_kill_switch_after_identity_probe(
                &server,
                Some(&server.identity_token),
                probe(),
                arm_request(credential_id),
            )
            .await
            .expect_err("a failing local cancel must stay visible to the operator");
            assert_eq!(error.code(), tonic::Code::Internal, "{error:?}");
            assert!(
                error.message().contains("apply kill switch actions failed")
                    && error
                        .message()
                        .contains("local cancel incomplete: 1 order(s) failed")
                    && error.message().contains("mmm_f1_legacy[pending_risk]"),
                "attempt={attempt}: {error:?}"
            );
            for sibling in ["aaa_f1_exit", "zzz_f1_exit"] {
                assert_eq!(
                    f1_order_state(&store, sibling).await.0,
                    "canceled",
                    "attempt={attempt}: {sibling} must be canceled despite the failing row"
                );
            }
            assert_eq!(
                f1_order_state(&store, "mmm_f1_legacy").await,
                ("pending_risk".to_string(), 1),
                "attempt={attempt}: the failing row must stay untouched"
            );
        }
        let latched = sqlx::query("SELECT level FROM kill_switch_state WHERE scope_key=?")
            .bind(&scope_key)
            .fetch_one(store.pool())
            .await
            .expect("the latch is durable even though the RPC reported failure")
            .get::<String, _>("level");
        assert_eq!(latched, "flatten");
        assert_eq!(
            f1_order_state(&store, "ctl_f1_exit").await,
            ("pending_risk".to_string(), 1),
            "another credential's scope must not be touched"
        );

        // 对照：同一入口、没有失败行的 scope 返回 accepted。
        let accepted = super::loopback_test_support::set_kill_switch_after_identity_probe(
            &server,
            Some(&server.identity_token),
            probe(),
            arm_request(control_credential_id),
        )
        .await
        .expect("a scope without failing rows is accepted");
        assert!(accepted.accepted);
        assert_eq!(f1_order_state(&store, "ctl_f1_exit").await.0, "canceled");
        assert!(
            exchange.recorded_requests().is_empty(),
            "local kill-switch semantics must not reach the exchange"
        );

        let _ = shutdown.send(());
        cleanup(store, path).await;
    }

    include!("svc_v2_k04_wiring_tests.rs");
    include!("svc_v2_k05_wiring_tests.rs");
}

/// **段③ R1 返修 · SEG3-R1-001（High）· 「风控之前拉起对账」的行为级反向契约。**
///
/// 段③ 批 0（`8c6be3b`）把 `ReconcilerManager::ensure_running` 提到了风险判定
/// **之前**（`svc.rs`），解除「`reconcile_diff_open=1` 的凭据开仓被拦 ⇒ 不开仓
/// ⇒ 对账不起 ⇒ 差异永远不收敛 ⇒ 继续拦」这个死结。R1 评审的变异证明这条接线
/// **零测试覆盖**：只删掉风控前那一次 `ensure_running`（不动任何内部 helper），
/// 全量 683 项照样全绿——没有任何用例钉住"被拦的开仓也必须把对账拉起来"。
///
/// 这一条按 engine safety contract 的四条办：
/// * **入口同一**：请求经**真实 TCP + 真实 HTTP/2 + 真实 protobuf**
///   （`ProstCodec`）打到 `/execution_engine.v2.EngineExecution/ExecuteIntents`
///   ——Python 编排器真实调用的那个符号，不是 `ensure_running` 本身，也不是
///   `ReconcilerManager` 的任何内部 helper。
/// * **不跳层**：auth interceptor → v2 adapter → v1 shared core → 风控闸门，
///   一层都没绕；开仓走的是完整的 typed-protection v2 意图。
/// * **两侧真值同源**：对账那一端的钱包余额由**交易所桩**一侧产出
///   （`RECONCILED_WALLET_BALANCE`），由**生产对账写入器**
///   （`persist_positions_and_balances`）真实消费并落进 `engine_balances`；
///   用例不在两侧各造一份。
/// * **反向可断言**：删掉 `svc.rs` 风控前那次 `ensure_running`，这条用例必红
///   ——开仓被闸门拒绝后提交分支永远不会执行，那次调用是本进程内**唯一**
///   会拉起对账的路径。
///
/// **没有网络**（§4：交易路径测试默认 dry-run）：交易所提交端与对账端各是一条
/// `BinanceFuturesClient::new_with_mock_responses` 的内存队列，user stream 关掉。
/// 对账任务本身**没有替**——`ensure_running` → `tokio::spawn` → 句柄登记 →
/// 立刻跑一次启动对账 → 落库，正是被证明的对象。
#[cfg(test)]
mod seg3_r1_reconcile_before_risk_wiring_tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::Once;
    use std::time::Duration;

    use sqlx::Row;
    use tonic::transport::Server;

    use super::loopback_test_support::{
        bind_owned_loopback, execute_after_identity_probe, BoundLoopbackClient,
    };
    use super::EngineExecutionV2Service;
    use crate::binance_futures::BinanceFuturesClient;
    use crate::proto_v2::{
        DataSnapshotDecision, ExecuteIntentsRequest, ExecuteIntentsResponse, OrderIntent,
        Protection,
    };
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();

    const AUTH_TOKEN_PREFIX: &str = "seg3-r1-reconcile-token";
    const SYMBOL_KEY: &str = "binance|futures|BTC|USDT|USDT";
    /// 只有**交易所那一端**知道的余额。它出现在 `engine_balances` 里，
    /// 等价于"启动对账真的跑完了一轮并把交易所真值写了回来"。
    const RECONCILED_WALLET_BALANCE: &str = "7777.77";
    const RECONCILED_AVAILABLE_BALANCE: &str = "6666.66";

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

    /// 交易所凭据的最小真实环境，外加把部署档位抬到 `demo`——**并握着进程 env 串行锁**。
    ///
    /// `ENGINE_STAGE_MAX` 是**进程级**变量：开仓（`OrderRole::Entry`）是唯一
    /// 会被 `StageDenial::ExceedsEffective` 拦住的 role，而那条拒绝发生在
    /// `ensure_running` **之前**——不抬档位，这条用例根本走不到被证明的代码。
    /// 真实的 demo 部署本就是 `ENGINE_STAGE_MAX=demo`。
    ///
    /// `svc.rs` 在**每次**开仓请求时重新读这个变量，而 `svc.rs` 里三条同样把它
    /// 抬到 demo 的用例只在 `env_serial_guard` 之下互斥。本模块若不持同一把锁，
    /// 它们的 `TestEnvVar` 复原会在本用例 gRPC 往返途中把上限打回 paper，ack 变成
    /// `ExceedsEffective`（`reject_reason_code` 为空）而不是被证明的
    /// `DEGRADED_CREDENTIAL`——2026-09-05 宿主全量 2/2 复现。
    /// 字段顺序即析构顺序：先复原 env，再放锁。
    struct ExchangeCredentialEnv {
        _guards: Vec<EnvGuard>,
        _serial: std::sync::MutexGuard<'static, ()>,
    }

    fn exchange_credential_env(credential_id: &str) -> ExchangeCredentialEnv {
        let serial = crate::binance_futures::tests::env_serial_guard();
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let guards = vec![
            EnvGuard::set("ENGINE_STAGE_MAX".to_string(), "demo"),
            EnvGuard::set(crate::venue::venue_env_name(credential_id), "testnet"),
            EnvGuard::set(key_env, "seg3-r1-key"),
            EnvGuard::set(secret_env, "seg3-r1-secret"),
        ];
        ExchangeCredentialEnv {
            _guards: guards,
            _serial: serial,
        }
    }

    /// 变异契约：让 `exchange_credential_env` 取到锁就放（`_serial: ()`），本用例必红。
    /// 探针失败只可能因为锁在别处被持有，而此刻持有者就是本用例自己。
    #[test]
    fn exchange_credential_env_holds_the_process_env_serial_lock_while_the_stage_is_raised() {
        let env = exchange_credential_env("seg3r1-lock-probe");
        assert_eq!(
            std::env::var("ENGINE_STAGE_MAX").as_deref(),
            Ok("demo"),
            "the helper must raise the process-level stage maximum"
        );
        assert!(
            crate::binance_futures::tests::env_serial_try_guard().is_none(),
            "ENGINE_STAGE_MAX is process-global and re-read by svc.rs on every entry; \
             the seg3 wiring env must hold env_serial_guard for as long as it is raised"
        );
        drop(env);
    }

    /// 真库 + 真迁移。健康行取 0029 fail-closed 存量行的形态：
    /// `reconcile_diff_open = 1`（差异未消解）⇒ 开仓必被闸门拦下。
    async fn wiring_store(credential_id: &str) -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-seg3r1-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level,
              remaining_credential_notional_usdt
            ) VALUES (?, ?, '10000', '10000', '0', 0, 0, 0, 0,
                      '{"majors":0,"l1_alts":0}', 'none', '5000')
            "#,
        )
        .bind(credential_id)
        .bind(crate::svc::now_ms().to_string())
        .execute(store.pool())
        .await
        .expect("insert fresh risk budget");
        sqlx::query(
            r#"
            INSERT INTO engine_credential_health (
              credential_id, degraded, degraded_reason, consecutive_errors,
              first_error_ms, last_error_ms, updated_at_ms, reconcile_diff_open
            ) VALUES (?, 0, NULL, 0, 0, 0, 1, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed the fail-closed reconcile diff row");
        // fill 恢复已就绪。不这样做，闸门会先在 `FILL_RECOVERY_NOT_READY`
        // 上拒绝（user stream 在本用例里是关掉的），把本条要证明的
        // `DEGRADED_CREDENTIAL` 判据盖住——那是夹具的产物，不是被证明的事实。
        sqlx::query(
            r#"
            INSERT INTO fill_stream_state (
              credential_id, generation, ready, phase, current_symbol,
              cursor_last_trade_id, failure_reason, updated_at_ms
            ) VALUES (?, 1, 1, 'streaming', 'BTCUSDT', NULL, NULL, 1)
            "#,
        )
        .bind(credential_id)
        .execute(store.pool())
        .await
        .expect("seed a ready fill recovery stream state");
        (store, path)
    }

    /// 对账那一端的交易所真值：一张本地不认识的开放单（差异**不收敛**，
    /// 于是闸门的结论与用例开始时逐字相同，与两个任务谁先跑无关），
    /// 外加一份只有交易所知道的余额。
    fn reconcile_exchange_responses() -> Vec<(u16, String)> {
        vec![
            (
                200,
                serde_json::json!([{
                    "orderId": 9001,
                    "clientOrderId": "coid-unknown-to-us",
                    "symbol": "ETHUSDT",
                    "status": "NEW",
                    "updateTime": 5
                }])
                .to_string(),
            ),
            (200, "[]".to_string()),
            (200, "[]".to_string()),
            (
                200,
                serde_json::json!([{
                    "asset": "USDT",
                    "balance": RECONCILED_WALLET_BALANCE,
                    "availableBalance": RECONCILED_AVAILABLE_BALANCE
                }])
                .to_string(),
            ),
        ]
    }

    /// 一条**实盘开仓**意图：typed protection 齐备、限价 100，
    /// 走的是 v2 生产 wire 上真实的那组字段。
    fn open_intent(label: &str) -> OrderIntent {
        OrderIntent {
            bot_uid: "bot_seg3r1".to_string(),
            run_id: "run_seg3r1".to_string(),
            cycle_id: format!("cycle_{label}"),
            idempotency_key: format!("idem_{label}"),
            client_order_id_short: format!("coid_{label}"),
            exchange: "binance".to_string(),
            market_kind: "futures".to_string(),
            symbol_key: SYMBOL_KEY.to_string(),
            action: "open_long".to_string(),
            order_type: "limit".to_string(),
            limit_price: "100".to_string(),
            reduce_only: false,
            intent_id: format!("int_{label}"),
            max_slippage_bps: 15,
            protection: Some(Protection {
                required: true,
                stop_loss_price: "95".to_string(),
                take_profit_price: "110".to_string(),
                confirm_timeout_ms: 5_000,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".to_string(),
            valid_until_ms: crate::svc::now_ms() + 30_000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        }
    }

    fn request_for(credential_id: &str, intent: OrderIntent) -> ExecuteIntentsRequest {
        ExecuteIntentsRequest {
            credential_id: credential_id.to_string(),
            run_mode: "demo".to_string(),
            intents: vec![intent],
            data_snapshot: Some(DataSnapshotDecision {
                snapshot_id: format!("snap_{}", "a".repeat(32)),
                quality_gate: "pass".to_string(),
                allow_open: true,
                allow_close: true,
                created_at_ms: crate::svc::now_ms(),
                schema_version: 1,
            }),
            schema_version: 1,
        }
    }

    /// 生产装配的 gRPC server。与 R10 那组唯一的差别：周期对账**不关掉**，
    /// 只把它的交易所那一端换成无网络的响应队列桩。
    async fn start_engine(
        test_name: &str,
        store: &Store,
        credential_id: &str,
        submit_responses: Vec<(u16, String)>,
        reconcile_responses: Vec<(u16, String)>,
    ) -> Option<(
        BoundLoopbackClient,
        BinanceFuturesClient,
        crate::svc::EngineExecutionService,
        tokio::sync::oneshot::Sender<()>,
    )> {
        let submit_client =
            BinanceFuturesClient::new_with_mock_responses(credential_id, submit_responses);
        let reconcile_client =
            BinanceFuturesClient::new_with_mock_responses(credential_id, reconcile_responses);
        let exec = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_stubbed_reconcile_exchange(reconcile_client)
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
            .with_submit_client_override(submit_client.clone());
        let events =
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800));
        let exec_v2 = EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
        let events_v2 = super::EngineEventsV2Skeleton;

        let bound = bind_owned_loopback(test_name, AUTH_TOKEN_PREFIX).await?;
        let client = bound.client();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let router = crate::add_engine_services(
            Server::builder(),
            exec.clone(),
            events,
            exec_v2,
            events_v2,
            Some(crate::AuthInterceptor::new(vec![bound
                .identity_token
                .clone()])),
        );
        tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(bound.incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("owned loopback gRPC server must run");
        });
        Some((client, submit_client, exec, shutdown_tx))
    }

    async fn execute_intents_over_grpc(
        server: &BoundLoopbackClient,
        token: Option<&str>,
        request: ExecuteIntentsRequest,
    ) -> Result<ExecuteIntentsResponse, tonic::Status> {
        execute_after_identity_probe(server, token, request).await
    }

    /// 启动对账在**另一个任务**里跑；用生产可观察的落库痕迹等它，不用睡固定时长。
    async fn wait_for<F, Fut>(label: &str, mut probe: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..400 {
            if probe().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tracing::warn!(probe = label, "observable never appeared");
        false
    }

    async fn reconciled_balance(store: &Store, credential_id: &str) -> Option<(String, String)> {
        sqlx::query(
            "SELECT wallet_balance, available_balance FROM engine_balances \
             WHERE credential_id = ? AND asset = 'USDT' LIMIT 1",
        )
        .bind(credential_id)
        .fetch_optional(store.pool())
        .await
        .expect("read the reconciled balance")
        .map(|row| {
            (
                row.get::<String, _>("wallet_balance"),
                row.get::<String, _>("available_balance"),
            )
        })
    }

    async fn reconcile_event_count(store: &Store, credential_id: &str) -> i64 {
        sqlx::query(
            "SELECT COUNT(*) AS n FROM engine_order_events \
             WHERE credential_id = ? AND event_type = 'reconcile_diff'",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("count reconcile_diff events")
        .get::<i64, _>("n")
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **被风险闸门拒绝的实盘开仓，也必须已经拉起并真的执行过启动对账。**
    ///
    /// 变异契约（评审 SEG3-R1-001 的关闭条件）：删除或旁路 `svc.rs` 风控**之前**
    /// 那一次 `self.reconcilers.ensure_running(...)`，本用例必红——ack 仍然是
    /// 那条 `DEGRADED_CREDENTIAL` 拒绝，但对账任务从未登记、`reconcile_diff`
    /// 审计行不存在、`engine_balances` 里也不会出现交易所那一端的余额。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_risk_rejected_live_entry_still_starts_and_runs_reconcile() {
        let credential_id = "seg3r1-blocked-entry";
        let _env = exchange_credential_env(credential_id);
        let (store, path) = wiring_store(credential_id).await;
        // 提交端的响应队列**故意留空**：这条意图必须在闸门处就被拒，
        // 一个交易所提交请求都不该发出去。
        let Some((server, submit_exchange, exec, _shutdown)) = start_engine(
            "seg3_r1_reconcile_before_risk_wiring_tests::a_risk_rejected_live_entry_still_starts_and_runs_reconcile",
            &store,
            credential_id,
            Vec::new(),
            reconcile_exchange_responses(),
        )
        .await
        else {
            cleanup(store, path).await;
            return;
        };

        let response = execute_intents_over_grpc(
            &server,
            Some(&server.identity_token),
            request_for(credential_id, open_intent("seg3r1_blocked")),
        )
        .await
        .expect("the v2 gRPC entry must answer");
        let ack = response.acks.into_iter().next().expect("exactly one ack");

        // ① 开仓确实被**风险闸门**拒了（不是被更早的档位/身份校验拦下——
        //    那两条发生在 ensure_running 之前，会让这条用例变成空转）。
        assert_eq!(
            ack.status,
            crate::proto_v2::Status::Rejected as i32,
            "an unresolved reconcile diff must reject the entry: {ack:?}"
        );
        assert_eq!(
            ack.reject_reason_code, "DEGRADED_CREDENTIAL",
            "the rejection must come from the credential-degrade gate, which sits \
             after the pre-risk ensure_running: {ack:?}"
        );
        assert!(
            submit_exchange.recorded_requests().is_empty(),
            "a gate-rejected entry must never reach the exchange: {:?}",
            submit_exchange.recorded_requests()
        );

        // ② **启动对账真的执行过**——判据落在生产可观察的库里，不是探针：
        //    交易所那一端产出的余额被生产对账写入器
        //    （`persist_positions_and_balances`）真实消费并落进 `engine_balances`。
        //    本进程内这一行没有第二个写者：提交端的响应队列是空的，开仓在闸门
        //    处就被拒了，`ReconcileNow` 也没人调。
        assert!(
            wait_for("reconciled balance", || async {
                reconciled_balance(&store, credential_id).await.is_some()
            })
            .await,
            "the startup reconcile must have persisted the exchange balance truth"
        );
        assert_eq!(
            reconciled_balance(&store, credential_id).await,
            Some((
                RECONCILED_WALLET_BALANCE.to_string(),
                RECONCILED_AVAILABLE_BALANCE.to_string()
            )),
            "the persisted balance must be the value the exchange side produced"
        );

        // ③ 这一轮对账跑到了尾，留下了它的审计行。
        assert!(
            wait_for("reconcile_diff event", || async {
                reconcile_event_count(&store, credential_id).await >= 1
            })
            .await,
            "a completed reconcile round must leave its reconcile_diff audit row"
        );

        // ④ 补充面：任务句柄进了 `ensure_running` 幂等性所依赖的那张注册表
        //    （这是生产状态本身——第二条实盘意图靠它不再重复起任务）。
        assert!(
            exec.reconcilers_for_tests()
                .is_running_for_tests(credential_id)
                .await,
            "the pre-risk ensure_running must have registered a reconcile task"
        );

        // ⑤ 死结的另一半：这一轮观察到的差异仍未消解，所以闸门保持关闭。
        //    收敛本身由段③批 0 的 H-01 用例（零差异轮次写显式 0）钉住。
        let diff_open = sqlx::query(
            "SELECT COALESCE(reconcile_diff_open, -1) AS reconcile_diff_open \
             FROM engine_credential_health WHERE credential_id = ? LIMIT 1",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("read credential health")
        .get::<i64, _>("reconcile_diff_open");
        assert_eq!(
            diff_open, 1,
            "the round observed a real diff; the gate must stay closed"
        );

        cleanup(store, path).await;
    }
    include!("svc_v2_terminal_conflict_tests.rs");
    include!("svc_v2_account_identity_tests.rs");
    include!("svc_v2_reconciler_recovery_tests.rs");
}
