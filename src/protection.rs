use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlx::Row;

use crate::binance_futures::{
    BinanceActualOrderId, BinanceAlgoId, BinanceAlgoOrderResponse, BinanceClientAlgoId,
    BinanceFuturesClient, BinanceKeys,
};
use crate::kill_switch;
use crate::proto;
use crate::rate_limiter::{CredentialRateLimiter, RequestClass};
use crate::store::Store;
use crate::svc::now_ms;

pub(crate) const DEFAULT_CONFIRM_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlgoTruthDisposition {
    Active,
    Triggered,
    Missing,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlgoTruthSource {
    RestAlgoQuery,
    RestOpenAlgoOrders,
    WsAlgoUpdate,
}

impl AlgoTruthSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::RestAlgoQuery => "rest_algo_query",
            Self::RestOpenAlgoOrders => "rest_open_algo_orders",
            Self::WsAlgoUpdate => "ws_algo_update",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProtectionTruthState {
    status: String,
    verified_from_exchange: i64,
    algo_status: Option<String>,
    exchange_algo_id: Option<String>,
    client_algo_id: Option<String>,
    actual_order_id: Option<String>,
}

impl ProtectionTruthState {
    fn event_value(&self) -> serde_json::Value {
        serde_json::json!({
            "status": self.status,
            "verified_from_exchange": self.verified_from_exchange,
            "algoStatus": self.algo_status,
            "algoId": self.exchange_algo_id,
            "clientAlgoId": self.client_algo_id,
            "actualOrderId": self.actual_order_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryProtectionAction {
    ReuseActive,
    BindTriggered,
    RepairMissing,
    NoPosition,
    HaltUnknown,
}

pub(crate) fn recovery_protection_action(
    position_open: bool,
    truth: Option<AlgoTruthDisposition>,
) -> RecoveryProtectionAction {
    if !position_open {
        return RecoveryProtectionAction::NoPosition;
    }
    match truth {
        Some(AlgoTruthDisposition::Active) => RecoveryProtectionAction::ReuseActive,
        Some(AlgoTruthDisposition::Triggered) => RecoveryProtectionAction::BindTriggered,
        Some(AlgoTruthDisposition::Missing) | None => RecoveryProtectionAction::RepairMissing,
        Some(AlgoTruthDisposition::Unknown) => RecoveryProtectionAction::HaltUnknown,
    }
}

pub(crate) fn classify_algo_truth(
    algo_status: Option<&str>,
    actual_order_id: Option<&str>,
) -> AlgoTruthDisposition {
    if actual_order_id.is_some_and(|value| !value.trim().is_empty() && value.trim() != "0") {
        return AlgoTruthDisposition::Triggered;
    }
    match algo_status
        .map(str::trim)
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("NEW" | "TRIGGERING") => AlgoTruthDisposition::Active,
        Some("CANCELED" | "EXPIRED" | "REJECTED" | "FAILED") => AlgoTruthDisposition::Missing,
        // TRIGGERED is itself exact exchange truth that the conditional order
        // reached the matching engine. actualOrderId is bound when present;
        // FINISHED without it remains ambiguous (filled vs canceled).
        Some("TRIGGERED") => AlgoTruthDisposition::Triggered,
        Some("FINISHED") | Some(_) | None => AlgoTruthDisposition::Unknown,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AlgoOrderTruth {
    pub algo_id: Option<BinanceAlgoId>,
    pub client_algo_id: Option<BinanceClientAlgoId>,
    pub algo_status: Option<String>,
    pub actual_order_id: Option<String>,
    pub updated_at_ms: Option<i64>,
}

impl AlgoOrderTruth {
    pub(crate) fn from_response(response: &BinanceAlgoOrderResponse) -> Self {
        Self {
            algo_id: response.algo_id,
            client_algo_id: response.client_algo_id.clone(),
            algo_status: response.algo_status.clone(),
            actual_order_id: response
                .actual_order_id
                .as_ref()
                .and_then(BinanceActualOrderId::normalized),
            updated_at_ms: response
                .additional_fields
                .get("updateTime")
                .and_then(serde_json::Value::as_i64),
        }
    }

    pub(crate) fn from_json(value: serde_json::Value) -> anyhow::Result<Self> {
        let response: BinanceAlgoOrderResponse =
            serde_json::from_value(value).context("decode Binance algo order truth")?;
        Ok(Self::from_response(&response))
    }

    pub(crate) fn disposition(&self) -> AlgoTruthDisposition {
        classify_algo_truth(self.algo_status.as_deref(), self.actual_order_id.as_deref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderRole {
    Entry,
    Exit,
    ProtectionSl,
    ProtectionTp,
    Emergency,
}

impl OrderRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::Exit => "exit",
            Self::ProtectionSl => "protection_sl",
            Self::ProtectionTp => "protection_tp",
            Self::Emergency => "emergency",
        }
    }

    pub(crate) fn protection_kind(self) -> Option<&'static str> {
        match self {
            Self::ProtectionSl => Some("sl"),
            Self::ProtectionTp => Some("tp"),
            _ => None,
        }
    }
}

/// Frozen v1 has no typed role. This is the single transitional adapter:
/// action + reduce_only + typed exchange order type are normalized once and
/// persisted. v2 never calls this adapter: it supplies `OrderRole` explicitly
/// in the request-local execution context. Every downstream decision reads
/// engine_orders.order_role.
pub(crate) fn infer_v1_order_role(intent: &proto::OrderIntent) -> OrderRole {
    let order_type = intent.order_type.trim().to_ascii_lowercase();
    if intent.reduce_only && intent.action.starts_with("close") {
        return match order_type.as_str() {
            "stop_market" => OrderRole::ProtectionSl,
            "take_profit_market" => OrderRole::ProtectionTp,
            _ if intent.idempotency_key.contains("emergency") => OrderRole::Emergency,
            _ => OrderRole::Exit,
        };
    }
    if matches!(intent.action.as_str(), "open_long" | "open_short") {
        OrderRole::Entry
    } else {
        OrderRole::Exit
    }
}

pub(crate) async fn persist_plan(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
    credential_id: &str,
    intent: &proto::OrderIntent,
    wire: &crate::risk::WireRiskContext,
    effective_stop_loss_price: Option<&str>,
    required: bool,
    confirm_timeout_ms: u64,
    ts_ms: i64,
) -> Result<(), tonic::Status> {
    let protection = wire
        .protection
        .as_ref()
        .ok_or_else(|| tonic::Status::failed_precondition("typed protection plan is absent"))?;
    let effective_stop = effective_stop_loss_price
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&protection.stop_loss_price);
    let timeout = i64::try_from(confirm_timeout_ms.max(1))
        .map_err(|_| tonic::Status::invalid_argument("confirm_timeout_ms is too large"))?;
    if let Some(row) = sqlx::query(
        r#"
        SELECT stop_loss_price, take_profit_price, confirm_timeout_ms
        FROM position_protection_plans WHERE position_id=? LIMIT 1
        "#,
    )
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| tonic::Status::internal(format!("db load protection plan failed: {error}")))?
    .into_iter()
    .next()
    {
        let same = row.get::<String, _>("stop_loss_price") == effective_stop
            && row.get::<String, _>("take_profit_price") == protection.take_profit_price
            && row.get::<i64, _>("confirm_timeout_ms") == timeout;
        return if same {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(
                "position already has a different typed protection plan",
            ))
        };
    }
    sqlx::query(
        r#"
        INSERT INTO position_protection_plans (
          position_id, credential_id, bot_uid, run_id, cycle_id,
          exchange, market_kind, symbol_key, direction,
          stop_loss_price, take_profit_price, required,
          confirm_timeout_ms, generation, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        "#,
    )
    .bind(position_id)
    .bind(credential_id)
    .bind(&intent.bot_uid)
    .bind(&intent.run_id)
    .bind(&intent.cycle_id)
    .bind(&intent.exchange)
    .bind(&intent.market_kind)
    .bind(&intent.symbol_key)
    .bind(if intent.action == "open_short" {
        "short"
    } else {
        "long"
    })
    .bind(effective_stop)
    .bind(&protection.take_profit_price)
    .bind(if required { 1i64 } else { 0i64 })
    .bind(timeout)
    .bind(ts_ms)
    .bind(ts_ms)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        tonic::Status::internal(format!("db insert protection plan failed: {error}"))
    })?;
    Ok(())
}

pub(crate) async fn insert_requested(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    protection_order_id: &str,
    position_id: &str,
    credential_id: &str,
    kind: &str,
    generation: i64,
    engine_order_id: &str,
    client_algo_id: &str,
    trigger_price: &str,
    close_position: bool,
    ts_ms: i64,
) -> Result<(), tonic::Status> {
    sqlx::query(
        r#"
        INSERT INTO protection_orders (
          protection_order_id, position_id, credential_id, kind, generation,
          engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id,
          algo_status, algo_updated_at_ms, trigger_price, reduce_only,
          close_position, status, verified_from_exchange, last_verified_at_ms,
          created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, ?, 1, ?,
                  'requested', 0, NULL, ?, ?)
        "#,
    )
    .bind(protection_order_id)
    .bind(position_id)
    .bind(credential_id)
    .bind(kind)
    .bind(generation.max(1))
    .bind(engine_order_id)
    .bind(client_algo_id)
    .bind(trigger_price)
    .bind(if close_position { 1i64 } else { 0i64 })
    .bind(ts_ms)
    .bind(ts_ms)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        tonic::Status::internal(format!("db insert protection order failed: {error}"))
    })?;
    Ok(())
}

pub(crate) const ACK_LOCAL_STATE_UNRESOLVED: &str = "PROTECTION_ACK_LOCAL_STATE_UNRESOLVED";

/// A validated remote identity survives even when local state cannot authorize
/// an ACK transition. The caller must commit identity + sender closure + audit
/// before reporting this unresolved result, and keep protection confirmation.
#[derive(Debug)]
pub(crate) struct AlgoSubmissionOutcome {
    pub algo_id: BinanceAlgoId,
    pub unresolved_local_state: Option<String>,
}

#[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
pub(crate) async fn mark_algo_submitted(
    store: &Store,
    protection_order_id: &str,
    requested_client_algo_id: &BinanceClientAlgoId,
    response: &BinanceAlgoOrderResponse,
    ts_ms: i64,
) -> anyhow::Result<AlgoSubmissionOutcome> {
    let mut tx = store.pool().begin().await?;
    let result = mark_algo_submitted_in_tx(
        &mut tx,
        protection_order_id,
        requested_client_algo_id,
        response,
        ts_ms,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub(crate) async fn mark_algo_submitted_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    protection_order_id: &str,
    requested_client_algo_id: &BinanceClientAlgoId,
    response: &BinanceAlgoOrderResponse,
    ts_ms: i64,
) -> anyhow::Result<AlgoSubmissionOutcome> {
    let algo_id = response
        .algo_id
        .ok_or_else(|| anyhow!("Binance algo submission response omitted algoId"))?;
    let returned_client_algo_id = response
        .client_algo_id
        .as_ref()
        .ok_or_else(|| anyhow!("Binance algo submission response omitted clientAlgoId"))?;
    if returned_client_algo_id.as_str() != requested_client_algo_id.as_str() {
        return Err(anyhow!(
            "Binance algo submission returned a different clientAlgoId"
        ));
    }
    let actual_order_id = response
        .actual_order_id
        .as_ref()
        .and_then(BinanceActualOrderId::normalized);
    let lock = match tx.as_mut().backend_name() {
        "MySQL" => " FOR UPDATE",
        "SQLite" => "",
        _ => return Err(anyhow!("algo ACK requires a known database dialect")),
    };
    let row = sqlx::query(&format!(
        "SELECT engine_order_id, exchange_algo_id, client_algo_id \
         FROM protection_orders WHERE protection_order_id=? LIMIT 1{lock}"
    ))
    .bind(protection_order_id)
    .fetch_one(&mut **tx)
    .await?;
    let engine_order_id = row.get::<String, _>("engine_order_id");
    let existing_algo_id = crate::store::try_get_nullable_string(&row, "exchange_algo_id")?;
    if existing_algo_id
        .as_deref()
        .is_some_and(|existing| existing != algo_id.to_string())
    {
        return Err(anyhow!(
            "REST ack algoId conflicts with earlier exchange truth"
        ));
    }
    let existing_client_id = crate::store::try_get_nullable_string(&row, "client_algo_id")?;
    if existing_client_id
        .as_deref()
        .is_some_and(|existing| existing != requested_client_algo_id.as_str())
    {
        return Err(anyhow!(
            "REST ack clientAlgoId conflicts with earlier exchange truth"
        ));
    }
    let state: String = sqlx::query_scalar(&format!(
        "SELECT status FROM engine_orders WHERE engine_order_id=?{lock}"
    ))
    .bind(&engine_order_id)
    .fetch_one(&mut **tx)
    .await?;
    let recognized = crate::svc::ALL_ENGINE_ORDER_STATUSES.contains(&state.as_str())
        || matches!(state.as_str(), "confirmed" | "triggering" | "triggered");
    let unresolved_local_state = (!recognized).then_some(state);

    sqlx::query(
        r#"
        UPDATE protection_orders
        SET exchange_order_id=CASE WHEN ? IS NOT NULL THEN ? ELSE exchange_order_id END,
            exchange_algo_id=COALESCE(exchange_algo_id, ?),
            client_algo_id=COALESCE(client_algo_id, ?),
            algo_status=CASE WHEN algo_updated_at_ms IS NULL THEN ? ELSE algo_status END,
            status=CASE WHEN algo_updated_at_ms IS NULL THEN 'submitted' ELSE status END,
            algo_updated_at_ms=COALESCE(algo_updated_at_ms, ?), updated_at_ms=?
        WHERE protection_order_id=?
        "#,
    )
    .bind(actual_order_id.as_deref())
    .bind(actual_order_id.as_deref())
    .bind(algo_id.to_string())
    .bind(requested_client_algo_id.as_str())
    .bind(response.algo_status.as_deref())
    .bind(ts_ms)
    .bind(ts_ms)
    .bind(protection_order_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        UPDATE engine_orders
        SET exchange_order_id=CASE WHEN ? IS NOT NULL THEN ? ELSE exchange_order_id END,
            status=CASE
              WHEN HEX(status) IN (HEX('pending_risk'), HEX('pending_submit'),
                                  HEX('unknown'), HEX('submitted'), HEX('acked')) THEN 'acked'
              ELSE status
            END,
            updated_at_ms=?
        WHERE engine_order_id=?
        "#,
    )
    .bind(actual_order_id.as_deref())
    .bind(actual_order_id.as_deref())
    .bind(ts_ms)
    .bind(engine_order_id)
    .execute(&mut **tx)
    .await?;
    Ok(AlgoSubmissionOutcome {
        algo_id,
        unresolved_local_state,
    })
}

pub(crate) async fn mark_submit_failed(
    store: &Store,
    protection_order_id: &str,
    ts_ms: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE protection_orders SET status='failed', updated_at_ms=? WHERE protection_order_id=? AND verified_from_exchange=0",
    )
    .bind(ts_ms)
    .bind(protection_order_id)
    .execute(store.pool())
    .await?;
    Ok(())
}

fn internal_status_for_algo_truth(
    disposition: AlgoTruthDisposition,
    exchange_status: Option<&str>,
) -> String {
    match disposition {
        AlgoTruthDisposition::Active => match exchange_status
            .map(str::trim)
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            Some("TRIGGERING") => "triggering".to_string(),
            _ => "confirmed".to_string(),
        },
        AlgoTruthDisposition::Triggered => "triggered".to_string(),
        AlgoTruthDisposition::Missing => exchange_status
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "missing".to_string()),
        AlgoTruthDisposition::Unknown => "algo_unknown".to_string(),
    }
}

/// Persist a GET /algoOrder or ALGO_UPDATE observation.  The observation must
/// carry an exact algoId; clientAlgoId may locate a pre-ack row, but never
/// substitutes for the exchange identity used to confirm it.  exchange_order_id
/// receives only a non-empty triggered actualOrderId.
///
/// `verified_from_exchange` is a current-state projection, not audit history.
/// Terminal exchange truth must revoke it from 1 to 0: making it monotonic
/// would treat a canceled protection order as valid and disguise a naked
/// position as protected.  Transition history is appended separately to
/// `engine_order_events`; an audit-write failure is visible but never rolls
/// back this safety-critical projection.
pub(crate) async fn apply_algo_truth_for_protection(
    store: &Store,
    credential_id: &str,
    protection_order_id: &str,
    truth: &AlgoOrderTruth,
    truth_source: AlgoTruthSource,
    observation_context: &'static str,
    ts_ms: i64,
) -> anyhow::Result<AlgoTruthDisposition> {
    let algo_id = truth
        .algo_id
        .ok_or_else(|| anyhow!("exchange algo truth omitted algoId"))?;
    let observed_at_ms = truth
        .updated_at_ms
        .ok_or_else(|| anyhow!("exchange algo truth omitted observation time"))?;
    let mut tx = store.pool().begin().await?;
    let row = sqlx::query(match store.kind() {
        crate::store::DbKind::MySql => {
            r#"
            SELECT COALESCE(po.engine_order_id, '') AS engine_order_id,
                   po.status, po.verified_from_exchange, po.algo_status,
                   po.exchange_algo_id, po.client_algo_id,
                   po.exchange_order_id AS actual_order_id,
                   COALESCE(eo.bot_uid, '') AS bot_uid,
                   COALESCE(eo.run_id, '') AS run_id,
                   COALESCE(eo.cycle_id, '') AS cycle_id,
                   COALESCE(eo.idempotency_key, '') AS idempotency_key,
                   COALESCE(eo.client_order_id_short, '') AS client_order_id_short,
                   COALESCE(eo.exchange, '') AS exchange,
                   COALESCE(eo.market_kind, '') AS market_kind,
                   COALESCE(eo.symbol_key, '') AS symbol_key
            FROM protection_orders po
            LEFT JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
            WHERE po.credential_id=? AND po.protection_order_id=?
            LIMIT 1 FOR UPDATE
            "#
        }
        _ => {
            r#"
            SELECT COALESCE(po.engine_order_id, '') AS engine_order_id,
                   po.status, po.verified_from_exchange, po.algo_status,
                   po.exchange_algo_id, po.client_algo_id,
                   po.exchange_order_id AS actual_order_id,
                   COALESCE(eo.bot_uid, '') AS bot_uid,
                   COALESCE(eo.run_id, '') AS run_id,
                   COALESCE(eo.cycle_id, '') AS cycle_id,
                   COALESCE(eo.idempotency_key, '') AS idempotency_key,
                   COALESCE(eo.client_order_id_short, '') AS client_order_id_short,
                   COALESCE(eo.exchange, '') AS exchange,
                   COALESCE(eo.market_kind, '') AS market_kind,
                   COALESCE(eo.symbol_key, '') AS symbol_key
            FROM protection_orders po
            LEFT JOIN engine_orders eo ON eo.engine_order_id=po.engine_order_id
            WHERE po.credential_id=? AND po.protection_order_id=? LIMIT 1
            "#
        }
    })
    .bind(credential_id)
    .bind(protection_order_id)
    .fetch_one(&mut *tx)
    .await?;
    let expected_algo_id = crate::store::try_get_nullable_string(&row, "exchange_algo_id")?;
    if expected_algo_id
        .as_deref()
        .is_some_and(|expected| expected != algo_id.to_string())
    {
        return Err(anyhow!("exchange algo truth algoId mismatch"));
    }
    let expected_client_id = crate::store::try_get_nullable_string(&row, "client_algo_id")?;
    if let Some(expected) = expected_client_id.as_deref() {
        let observed = truth
            .client_algo_id
            .as_ref()
            .ok_or_else(|| anyhow!("exchange algo truth omitted clientAlgoId"))?;
        if expected != observed.as_str() {
            return Err(anyhow!("exchange algo truth clientAlgoId mismatch"));
        }
    }

    let disposition = truth.disposition();
    let verified = matches!(
        disposition,
        AlgoTruthDisposition::Active | AlgoTruthDisposition::Triggered
    );
    let internal_status = internal_status_for_algo_truth(disposition, truth.algo_status.as_deref());
    let actual_order_id = truth.actual_order_id.as_deref();
    let client_algo_id = truth
        .client_algo_id
        .as_ref()
        .map(BinanceClientAlgoId::as_str)
        .or(expected_client_id.as_deref())
        .map(str::to_string);
    let engine_status = match disposition {
        AlgoTruthDisposition::Active | AlgoTruthDisposition::Triggered => {
            Some(internal_status.as_str())
        }
        AlgoTruthDisposition::Missing => Some("canceled"),
        AlgoTruthDisposition::Unknown => None,
    };
    let engine_order_id = row.get::<String, _>("engine_order_id");
    let before = ProtectionTruthState {
        status: row.get::<String, _>("status"),
        verified_from_exchange: row.get::<i64, _>("verified_from_exchange"),
        algo_status: crate::store::try_get_nullable_string(&row, "algo_status")?,
        exchange_algo_id: expected_algo_id,
        client_algo_id: expected_client_id,
        actual_order_id: crate::store::try_get_nullable_string(&row, "actual_order_id")?,
    };
    let event_context = crate::svc::EventContext {
        bot_uid: row.get::<String, _>("bot_uid"),
        run_id: row.get::<String, _>("run_id"),
        cycle_id: row.get::<String, _>("cycle_id"),
        idempotency_key: row.get::<String, _>("idempotency_key"),
        client_order_id_short: row.get::<String, _>("client_order_id_short"),
        exchange: row.get::<String, _>("exchange"),
        market_kind: row.get::<String, _>("market_kind"),
        symbol_key: row.get::<String, _>("symbol_key"),
    };
    sqlx::query(
        r#"
        UPDATE protection_orders
        SET exchange_algo_id=?, client_algo_id=?, algo_status=?,
            algo_updated_at_ms=?,
            exchange_order_id=CASE WHEN ? IS NOT NULL THEN ? ELSE exchange_order_id END,
            status=?,
            verified_from_exchange=?,
            last_verified_at_ms=CASE
              WHEN ?=1 THEN ? ELSE last_verified_at_ms
            END,
            updated_at_ms=?
        WHERE credential_id=? AND protection_order_id=?
        "#,
    )
    .bind(algo_id.to_string())
    .bind(client_algo_id.as_deref())
    .bind(truth.algo_status.as_deref())
    .bind(observed_at_ms)
    .bind(actual_order_id)
    .bind(actual_order_id)
    .bind(&internal_status)
    .bind(if verified { 1i64 } else { 0i64 })
    .bind(if verified { 1i64 } else { 0i64 })
    .bind(observed_at_ms)
    .bind(ts_ms)
    .bind(credential_id)
    .bind(protection_order_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        UPDATE engine_orders
        SET exchange_order_id=CASE WHEN ? IS NOT NULL THEN ? ELSE exchange_order_id END,
            status=COALESCE(?, status),
            updated_at_ms=?
        WHERE credential_id=? AND engine_order_id=?
        "#,
    )
    .bind(actual_order_id)
    .bind(actual_order_id)
    .bind(engine_status)
    .bind(ts_ms)
    .bind(credential_id)
    .bind(&engine_order_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let after = ProtectionTruthState {
        status: internal_status,
        verified_from_exchange: if verified { 1 } else { 0 },
        algo_status: truth.algo_status.clone(),
        exchange_algo_id: Some(algo_id.to_string()),
        client_algo_id,
        actual_order_id: actual_order_id
            .map(str::to_string)
            .or_else(|| before.actual_order_id.clone()),
    };
    if before != after {
        if let Err(error) = emit_protection_truth_transition(
            store,
            credential_id,
            protection_order_id,
            &engine_order_id,
            &event_context,
            truth_source,
            observation_context,
            observed_at_ms,
            &before,
            &after,
        )
        .await
        {
            tracing::warn!(
                credential_id,
                protection_order_id,
                truth_source = truth_source.as_str(),
                observation_context,
                %error,
                "protection truth transition audit event failed after state commit"
            );
        }
    }
    Ok(disposition)
}

#[allow(clippy::too_many_arguments)]
async fn emit_protection_truth_transition(
    store: &Store,
    credential_id: &str,
    protection_order_id: &str,
    engine_order_id: &str,
    event_context: &crate::svc::EventContext,
    truth_source: AlgoTruthSource,
    observation_context: &'static str,
    observed_at_ms: i64,
    before: &ProtectionTruthState,
    after: &ProtectionTruthState,
) -> anyhow::Result<()> {
    let payload = serde_json::json!({
        "schemaVersion": 1,
        "protectionOrderId": protection_order_id,
        "truth_source": truth_source.as_str(),
        "observation_context": observation_context,
        "observed_at_ms": observed_at_ms,
        "algoStatus": after.algo_status,
        "algoId": after.exchange_algo_id,
        "clientAlgoId": after.client_algo_id,
        "actualOrderId": after.actual_order_id,
        "from": before.event_value(),
        "to": after.event_value(),
    });
    let event_id = crate::svc::sha256_hex(&format!(
        "protection_truth_transition|{credential_id}|{protection_order_id}|{}",
        payload
    ));
    let mut event_tx = store.pool().begin().await?;
    crate::svc::emit_event(
        &mut event_tx,
        store.kind(),
        credential_id,
        "protection_state",
        event_context,
        engine_order_id,
        after.actual_order_id.as_deref(),
        Some(payload),
        Some(event_id),
    )
    .await
    .map_err(|error| anyhow!(error.to_string()))?;
    event_tx.commit().await?;
    Ok(())
}

pub(crate) async fn apply_algo_update_by_identity(
    store: &Store,
    credential_id: &str,
    truth: &AlgoOrderTruth,
    truth_source: AlgoTruthSource,
    observation_context: &'static str,
    ts_ms: i64,
) -> anyhow::Result<()> {
    let Some(algo_id) = truth.algo_id else {
        return Ok(());
    };
    let client_algo_id = truth
        .client_algo_id
        .as_ref()
        .map(BinanceClientAlgoId::as_str)
        .unwrap_or("");
    let row = sqlx::query(
        r#"
        SELECT protection_order_id FROM protection_orders
        WHERE credential_id=? AND (
          exchange_algo_id=? OR (client_algo_id IS NOT NULL AND client_algo_id=?)
        )
        ORDER BY generation DESC LIMIT 1
        "#,
    )
    .bind(credential_id)
    .bind(algo_id.to_string())
    .bind(client_algo_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    apply_algo_truth_for_protection(
        store,
        credential_id,
        &row.get::<String, _>("protection_order_id"),
        truth,
        truth_source,
        observation_context,
        ts_ms,
    )
    .await?;
    Ok(())
}

#[tonic::async_trait]
pub(crate) trait ProtectionAdapter: Send + Sync + 'static {
    async fn query_algo_order(
        &self,
        exchange_algo_id: Option<&str>,
        client_algo_id: Option<&str>,
    ) -> anyhow::Result<Option<AlgoOrderTruth>>;
    async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub(crate) struct BinanceProtectionAdapter {
    store: Store,
    client: BinanceFuturesClient,
    credential_id: String,
    scope_key: String,
    filter_cache: crate::exchange_filters::ExchangeFilterCache,
    limiter: CredentialRateLimiter,
}

impl BinanceProtectionAdapter {
    pub(crate) fn new(
        store: Store,
        keys: BinanceKeys,
        credential_id: &str,
        bot_uid: &str,
        limiter: CredentialRateLimiter,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            store,
            client: BinanceFuturesClient::new(keys, credential_id)?,
            credential_id: credential_id.to_string(),
            scope_key: format!("bot:{bot_uid}"),
            filter_cache: crate::exchange_filters::ExchangeFilterCache::shared(),
            limiter,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_filter_cache(
        mut self,
        filters: crate::exchange_filters::ExchangeFilterCache,
    ) -> Self {
        self.filter_cache = filters;
        self
    }
}

#[tonic::async_trait]
impl ProtectionAdapter for BinanceProtectionAdapter {
    async fn query_algo_order(
        &self,
        exchange_algo_id: Option<&str>,
        client_algo_id: Option<&str>,
    ) -> anyhow::Result<Option<AlgoOrderTruth>> {
        let parsed_algo_id = exchange_algo_id
            .filter(|value| !value.trim().is_empty())
            .map(str::parse::<i64>)
            .transpose()
            .context("stored exchange_algo_id is not an integer")?
            .map(BinanceAlgoId::new);
        let parsed_client_id = client_algo_id
            .filter(|value| !value.trim().is_empty())
            .map(BinanceClientAlgoId::new);
        let result = self
            .client
            .get_algo_order_json(
                &self.limiter,
                parsed_algo_id.as_ref(),
                parsed_client_id.as_ref(),
                RequestClass::EventTruth,
            )
            .await;
        match result {
            Ok(value) => Ok(Some(AlgoOrderTruth::from_json(value)?)),
            Err(error) if error.to_string().contains("\"code\":-2013") => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
        kill_switch::submit_live_flatten(
            &self.store,
            &self.client,
            &self.credential_id,
            &self.scope_key,
            &self.filter_cache,
        )
        .await
    }
}

struct ConfirmationContext {
    exchange_algo_id: String,
    client_algo_id: String,
    bot_uid: String,
    credential_id: String,
    position_id: String,
    verified: i64,
}

async fn protection_context(
    store: &Store,
    protection_order_id: &str,
) -> anyhow::Result<Option<ConfirmationContext>> {
    let row = sqlx::query(
        r#"
        SELECT po.exchange_algo_id, po.client_algo_id, po.verified_from_exchange,
               ps.bot_uid, po.credential_id, po.position_id
        FROM protection_orders po
        JOIN position_state ps ON ps.position_id=po.position_id
        WHERE po.protection_order_id=?
        LIMIT 1
        "#,
    )
    .bind(protection_order_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(ConfirmationContext {
        exchange_algo_id: crate::store::try_get_nullable_string(&row, "exchange_algo_id")?
            .unwrap_or_default(),
        client_algo_id: crate::store::try_get_nullable_string(&row, "client_algo_id")?
            .unwrap_or_default(),
        bot_uid: row.get("bot_uid"),
        credential_id: row.get("credential_id"),
        position_id: row.get("position_id"),
        verified: row.get("verified_from_exchange"),
    }))
}

/// **保护失败的既定处置**：先闩住 `bot:<uid>` 的 **`flatten` 档**，再发
/// reduce-only flatten——收敛只认交易所真值。
///
/// 从 `flatten_and_pause` 里提出来的**唯一一份**（第七轮
/// regression）：WS 侧「entry fill 之后保护没挂上」用的必须是同一条路，
/// 而不是另造一个处置。此处不碰 `protection_orders`——那一步只有
/// "确实存在一张保护单"时才有意义，由 `flatten_and_pause` 负责。
///
/// **请求 ACK 不是仓位归零。**
///
/// R11/R12 的形状是：先 `submit_reduce_only_flatten()`，成功之后 arm 一个
/// `pause_new` 就返回 `Ok(())`。两层都不成立：
///
/// * `submit_reduce_only_flatten` 在 live adapter 下就是
///   `kill_switch::submit_live_flatten`——**下完 market order 就返回**。
///   拿到的是一次 `place-order` 的**请求 ACK**，不是"仓位已经归零"。
///   部分成交、被交易所过滤规则挡下、下单后立刻反向成交，都会让 ACK 为真
///   而敞口还在（engine safety contract「SL 未经交易所真值确认则立即 reduce-only
///   flatten」，§3.6「必须以交易所真值对账」）。
/// * `pause_new` **只挡新开仓**，它不会驱动任何重试：进程重启之后
///   连"曾经有过一个没保护的敞口"都不剩，没有任何东西会再去平它。
///
/// 现在的顺序是**先闩后发**，且闩的是 `flatten` 档：
///
/// 1. `kill_switch::latch_flatten_pending` 持久化 `level='flatten'` +
///    `flatten_state='flattening'`（并清掉上一轮的收敛结论）。这条 latch
///    正是 `kill_switch::spawn_action_retry_task` 每秒扫描的集合——
///    它逐 credential 撤单 + reduce-only flatten + **对账**，
///    只有 `ReconcileTruth::flatten_complete()` 会把它写成 `flattened`。
///    latch 先于任何交易所动作落库，所以"发出去之后进程就崩了"也仍然可恢复。
/// 2. 再发这一次即时的 reduce-only flatten。它成功与否都**不解除** latch。
///
/// 返回 `Ok(())` 的含义随之收紧为「latch 已持久化 + 这一次 flatten 请求
/// 已被受理」，**不是**「已经平掉了」。调用方要判断敞口是否收敛，只能读
/// `kill_switch_state.flatten_state`。
pub(crate) async fn flatten_and_pause_bot<A: ProtectionAdapter + ?Sized>(
    store: &Store,
    bot_uid: &str,
    adapter: &A,
    reason: &str,
) -> anyhow::Result<()> {
    let _ = latch_flatten_after_protection_failure(store, bot_uid, reason).await?;
    dispatch_latched_flatten(adapter).await
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProtectionFailureLatch {
    CanonicalFlatten,
    PreservedInvalid(kill_switch::InvalidPersistedLevel),
}

/// Persist the crash-recoverable authority for a protection failure before
/// any permit is closed or any exchange action is attempted.
///
/// A malformed row is already included in the retry scanner as conservative
/// `flatten`; preserve its raw bytes and allow safety execution to continue.
/// Other persistence failures provide no durable recovery authority and must
/// remain fatal to the caller.
pub(crate) async fn latch_flatten_after_protection_failure(
    store: &Store,
    bot_uid: &str,
    reason: &str,
) -> anyhow::Result<ProtectionFailureLatch> {
    let scope_key = format!("bot:{bot_uid}");
    // **闩在前**：它只是一次 DB 写，不依赖交易所；先落库才能保证
    // 「请求发出去之后进程立刻崩」也仍然留着一条可重试的处置。
    match kill_switch::latch_flatten_pending(
        store,
        &scope_key,
        "auto_protection_timeout",
        reason,
        now_ms(),
    )
    .await
    {
        Ok(_) => Ok(ProtectionFailureLatch::CanonicalFlatten),
        Err(error)
            if error
                .downcast_ref::<kill_switch::InvalidPersistedLevel>()
                .is_some_and(|invalid| invalid.scope_key == scope_key) =>
        {
            let invalid = error
                .downcast_ref::<kill_switch::InvalidPersistedLevel>()
                .expect("matched typed invalid persisted level");
            tracing::error!(
                scope_key = %invalid.scope_key,
                raw_level = ?invalid.raw_level,
                conservative_level = kill_switch::Level::Flatten.as_str(),
                error_code = "KILL_SWITCH_INVALID_PERSISTED_LEVEL",
                "protection failure found an invalid durable latch; preserving it as conservative retry authority"
            );
            Ok(ProtectionFailureLatch::PreservedInvalid(invalid.clone()))
        }
        Err(error) => Err(error).context("latch retryable bot flatten after protection failure"),
    }
}

pub(crate) async fn dispatch_latched_flatten<A: ProtectionAdapter + ?Sized>(
    adapter: &A,
) -> anyhow::Result<()> {
    // **顺序本身要有一个崩溃观察面。**
    //
    // R13 登记过一条存活变异：把上面这次 latch 挪到 flatten 之后，
    // 23 条进程内用例全绿——因为在一个不会崩的进程里，两条语句谁先谁后
    // 观察不到。这个 checkpoint 把**两者之间**那一瞬变成一个可被外部
    // harness 抓住的持久边界：在这里 SIGSTOP，由 L4 harness 补 SIGKILL，
    // 重启后 latch 必须还在（`level='flatten'` + `flattening`）、
    // 交易所一张 flatten 单都不该收到过。
    //
    // 顺序若被调换，同一个 checkpoint 上的观察就会反过来：交易所已经收到
    // 单、而重启后没有任何 latch 驱动重试。普通构建里本模块根本不编译
    // （`l4-fault-injection` feature），无运行时分支。
    // **闩没落库就不许发单。**
    //
    // R14 把这次检查放在 `submit_reduce_only_flatten()` **之后**：DB 闩写失败
    // （磁盘满、连接断、约束冲突）时，函数确实返回 `Err`，可 reduce-only
    // flatten **已经发到交易所去了**——留下的正是"先闩后发"要消灭的那个状态：
    // 交易所收到过动作，而本地没有任何 latch 会在重启后驱动重试或对账收敛。
    // 闩是这条处置的**前置条件**，不是它的一个并列步骤：写不下去就立刻返回，
    // 一张单都不发（dispatch=0）。
    #[cfg(feature = "l4-fault-injection")]
    crate::fault_injection::checkpoint("after_flatten_latch_before_dispatch")?;
    adapter
        .submit_reduce_only_flatten()
        .await
        .context("reduce-only flatten after protection failure")
}

pub(crate) async fn flatten_and_pause<A: ProtectionAdapter + ?Sized>(
    store: &Store,
    protection_order_id: &str,
    bot_uid: &str,
    adapter: &A,
    reason: &str,
) -> anyhow::Result<()> {
    let outcome = flatten_and_pause_bot(store, bot_uid, adapter, reason).await;
    // 顺序刻意保留 R10 的形状：无论 flatten/arm 结果如何，这张保护单都要被
    // 标成 submit_failed，且它的失败不得盖掉上面两步的失败。
    let _ = mark_submit_failed(store, protection_order_id, now_ms()).await;
    outcome
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerificationOutcome {
    Confirmed,
    PositionClosed,
    FlattenStarted,
}

pub(crate) async fn verify_or_flatten<A: ProtectionAdapter + ?Sized>(
    store: &Store,
    protection_order_id: &str,
    adapter: &A,
) -> anyhow::Result<VerificationOutcome> {
    let Some(context) = protection_context(store, protection_order_id).await? else {
        return Err(anyhow!("protection order not found"));
    };
    if context.verified == 1 {
        return Ok(VerificationOutcome::Confirmed);
    }
    // Submission acknowledgement is not confirmation.  The deadline query is
    // made by the persisted exact algoId, and the response must echo that same
    // id before verified_from_exchange can move to 1.
    if !context.exchange_algo_id.is_empty() {
        if let Ok(Some(truth)) = adapter
            .query_algo_order(Some(&context.exchange_algo_id), None)
            .await
        {
            if truth.algo_id.map(|id| id.to_string()).as_deref()
                == Some(context.exchange_algo_id.as_str())
            {
                let credential_id = sqlx::query(
                    "SELECT credential_id FROM protection_orders WHERE protection_order_id=?",
                )
                .bind(protection_order_id)
                .fetch_one(store.pool())
                .await?
                .get::<String, _>("credential_id");
                let disposition = apply_algo_truth_for_protection(
                    store,
                    &credential_id,
                    protection_order_id,
                    &truth,
                    AlgoTruthSource::RestAlgoQuery,
                    "protection_confirmation",
                    now_ms(),
                )
                .await;
                match disposition {
                    Ok(AlgoTruthDisposition::Active | AlgoTruthDisposition::Triggered) => {
                        return Ok(VerificationOutcome::Confirmed);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        // Malformed or unpersistable truth is not confirmation.  It must
                        // fall through to the existing fail-open flatten + pause path.
                        tracing::warn!(
                            protection_order_id,
                            %error,
                            "exchange protection truth could not be applied; treating it as unconfirmed"
                        );
                    }
                }
            }
        }
    }

    let reason =
        format!(
        "protection algo order not confirmed by exact exchange truth at deadline (clientAlgoId={})",
        if context.client_algo_id.is_empty() { "absent" } else { "present" }
    );
    // Recheck after the venue await, under the same row lock and transaction
    // as arming. An expired obligation is distinct from confirmed protection.
    let latch = kill_switch::latch_flatten_unless_position_closed(
        store,
        &context.position_id,
        &context.credential_id,
        &context.bot_uid,
        &reason,
        now_ms(),
    )
    .await;
    let outcome = match latch {
        Ok(None) => return Ok(VerificationOutcome::PositionClosed),
        Ok(Some(_)) => dispatch_latched_flatten(adapter).await,
        Err(error) => Err(error).context("latch retryable bot flatten after protection failure"),
    };
    // Preserve the existing failure bookkeeping even when latch/dispatch fails.
    let _ = mark_submit_failed(store, protection_order_id, now_ms()).await;
    outcome?;
    Ok(VerificationOutcome::FlattenStarted)
}

pub(crate) fn spawn_confirmation_task<A: ProtectionAdapter>(
    store: Store,
    protection_order_id: String,
    timeout_ms: u64,
    adapter: Arc<A>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
        if let Err(error) = verify_or_flatten(&store, &protection_order_id, adapter.as_ref()).await
        {
            tracing::error!(%protection_order_id, %error, "protection confirmation/timeout action failed");
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Once};
    use std::time::Instant;

    use super::*;

    static INSTALL_DRIVERS: Once = Once::new();

    /// 读一个 scope 的 latch：`(level, flatten_state)`。
    ///
    /// 保护失败的处置从 `pause_new` 收紧成
    /// **未收敛的 `flatten` latch**，所以用例断言的也必须是这两段——
    /// 只看 `level` 的话，一个已经写成 `flattened`（= 认为已平）的 latch
    /// 也会让断言通过。
    async fn latch(store: &Store, scope_key: &str) -> (String, Option<String>) {
        let row = sqlx::query(
            "SELECT level, COALESCE(flatten_state, '') AS flatten_state \
             FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(scope_key)
        .fetch_one(store.pool())
        .await
        .expect("the disposition must have latched a kill-switch state");
        let flatten_state = row.get::<String, _>("flatten_state");
        (
            row.get::<String, _>("level"),
            (!flatten_state.is_empty()).then_some(flatten_state),
        )
    }

    #[derive(Clone, Default)]
    struct FakeAdapter {
        open_ids: Arc<Mutex<HashSet<String>>>,
        flatten_calls: Arc<AtomicUsize>,
        flatten_fails: bool,
    }

    #[derive(Clone)]
    struct ExactTruthAdapter {
        truth: AlgoOrderTruth,
        flatten_calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl ProtectionAdapter for ExactTruthAdapter {
        async fn query_algo_order(
            &self,
            _exchange_algo_id: Option<&str>,
            _client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<AlgoOrderTruth>> {
            Ok(Some(self.truth.clone()))
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flatten_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tonic::async_trait]
    impl ProtectionAdapter for FakeAdapter {
        async fn query_algo_order(
            &self,
            exchange_algo_id: Option<&str>,
            client_algo_id: Option<&str>,
        ) -> anyhow::Result<Option<AlgoOrderTruth>> {
            let identity = exchange_algo_id.or(client_algo_id).unwrap_or_default();
            if !self
                .open_ids
                .lock()
                .expect("open ids lock")
                .contains(identity)
            {
                return Ok(None);
            }
            Ok(Some(AlgoOrderTruth {
                algo_id: exchange_algo_id
                    .and_then(|value| value.parse::<i64>().ok())
                    .map(BinanceAlgoId::new),
                client_algo_id: Some(BinanceClientAlgoId::new(
                    client_algo_id.unwrap_or("algo-client-t17a"),
                )),
                algo_status: Some("NEW".to_string()),
                actual_order_id: None,
                updated_at_ms: Some(2),
            }))
        }

        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            self.flatten_calls.fetch_add(1, Ordering::SeqCst);
            if self.flatten_fails {
                Err(anyhow!("injected flatten failure"))
            } else {
                Ok(())
            }
        }
    }

    async fn fixture(label: &str) -> (Store, std::path::PathBuf, String) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-protection-{label}-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("test store");
        let position_id = format!("pos_{}", ulid::Ulid::new());
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, qty, updated_at_ms
            ) VALUES (?, 'cred_t17a', 'bot_t17a',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'open', ?, '1', 1)
            "#,
        )
        .bind(&position_id)
        .bind(format!("bot_t17a|symbol|long|{label}"))
        .execute(store.pool())
        .await
        .expect("position");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange, market_kind, symbol_key,
              action, side, order_type, quantity, reduce_only, status,
              position_id, order_role, created_at_ms, updated_at_ms
            ) VALUES (
              'eng_t17a', 'cred_t17a', 'bot_t17a', 'idem_t17a',
              'algo-client-t17a', 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'close_long', 'long',
              'stop_market', '', 1, 'pending_submit', ?, 'protection_sl', 1, 1
            )
            "#,
        )
        .bind(&position_id)
        .execute(store.pool())
        .await
        .expect("engine order");
        (store, path, position_id)
    }

    async fn insert_protection_row(
        store: &Store,
        position_id: &str,
        protection_id: &str,
        kind: &str,
        exchange_algo_id: Option<&str>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              engine_order_id, exchange_order_id, exchange_algo_id,
              client_algo_id, algo_status, trigger_price, reduce_only,
              close_position, status, verified_from_exchange, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'cred_t17a', ?, 1, 'eng_t17a', NULL, ?,
                      'algo-client-t17a', 'NEW', '99', 1, 1,
                      'submitted', 0, 1, 1)
            "#,
        )
        .bind(protection_id)
        .bind(position_id)
        .bind(kind)
        .bind(exchange_algo_id)
        .execute(store.pool())
        .await
        .expect("protection row");
    }

    async fn cleanup(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    async fn assert_bot_not_paused(store: &Store) {
        let pause = sqlx::query(
            "SELECT 1 FROM kill_switch_state WHERE scope_key='bot:bot_t17a' AND level='pause_new'",
        )
        .fetch_optional(store.pool())
        .await
        .unwrap();
        assert!(
            pause.is_none(),
            "successful exact truth must not latch pause_new"
        );
    }

    fn algo_truth(
        status: &str,
        algo_id: i64,
        actual_order_id: Option<&str>,
        ts: i64,
    ) -> AlgoOrderTruth {
        AlgoOrderTruth {
            algo_id: Some(BinanceAlgoId::new(algo_id)),
            client_algo_id: Some(BinanceClientAlgoId::new("algo-client-t17a")),
            algo_status: Some(status.to_string()),
            actual_order_id: actual_order_id.map(str::to_string),
            updated_at_ms: Some(ts),
        }
    }

    async fn protection_truth_events(store: &Store) -> Vec<(String, serde_json::Value)> {
        sqlx::query(
            r#"
            SELECT event_id, payload_json FROM engine_order_events
            WHERE event_type='protection_state' ORDER BY seq
            "#,
        )
        .fetch_all(store.pool())
        .await
        .expect("load protection truth events")
        .into_iter()
        .map(|row| {
            let event_id = row.get::<String, _>("event_id");
            let payload = row.get::<Vec<u8>, _>("payload_json");
            (
                event_id,
                serde_json::from_slice(&payload).expect("decode protection truth event"),
            )
        })
        .collect()
    }

    #[test]
    fn official_algo_statuses_keep_triggering_present_and_rejected_missing() {
        assert_eq!(
            classify_algo_truth(Some("NEW"), None),
            AlgoTruthDisposition::Active
        );
        assert_eq!(
            classify_algo_truth(Some("TRIGGERING"), None),
            AlgoTruthDisposition::Active
        );
        assert_eq!(
            classify_algo_truth(Some("REJECTED"), None),
            AlgoTruthDisposition::Missing
        );
        assert_eq!(
            classify_algo_truth(Some("TRIGGERED"), Some("88004")),
            AlgoTruthDisposition::Triggered
        );
        assert_eq!(
            classify_algo_truth(Some("TRIGGERED"), None),
            AlgoTruthDisposition::Triggered
        );
        assert_eq!(
            classify_algo_truth(Some("FINISHED"), Some("88004")),
            AlgoTruthDisposition::Triggered
        );
        assert_eq!(
            classify_algo_truth(Some("FINISHED"), None),
            AlgoTruthDisposition::Unknown
        );
    }

    #[tokio::test]
    async fn terminal_truth_revokes_current_verification_for_canceled_rejected_expired() {
        for (index, status) in ["CANCELED", "REJECTED", "EXPIRED"].into_iter().enumerate() {
            let (store, path, position_id) = fixture(&format!("terminal-{status}")).await;
            let protection_id = format!("prot_terminal_{index}");
            insert_protection_row(&store, &position_id, &protection_id, "sl", Some("42010")).await;
            sqlx::query(
                r#"
                UPDATE protection_orders
                SET status='confirmed', verified_from_exchange=1,
                    algo_status='NEW', last_verified_at_ms=10
                WHERE protection_order_id=?
                "#,
            )
            .bind(&protection_id)
            .execute(store.pool())
            .await
            .unwrap();

            let disposition = apply_algo_truth_for_protection(
                &store,
                "cred_t17a",
                &protection_id,
                &algo_truth(status, 42010, None, 20 + index as i64),
                AlgoTruthSource::RestAlgoQuery,
                "terminal_guard",
                30 + index as i64,
            )
            .await
            .unwrap();
            assert_eq!(disposition, AlgoTruthDisposition::Missing);
            let row = sqlx::query(
                "SELECT status, verified_from_exchange FROM protection_orders WHERE protection_order_id=?",
            )
            .bind(&protection_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
            assert_eq!(row.get::<String, _>("status"), status.to_ascii_lowercase());
            assert_eq!(
                row.get::<i64, _>("verified_from_exchange"),
                0,
                "{status} must revoke the current exchange-truth projection"
            );
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn truth_transition_event_contains_source_status_identity_and_exchange_time() {
        let (store, path, position_id) = fixture("truth-event-fields").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_truth_event",
            "sl",
            Some("42011"),
        )
        .await;

        apply_algo_truth_for_protection(
            &store,
            "cred_t17a",
            "prot_truth_event",
            &algo_truth("TRIGGERED", 42011, Some("88011"), 123_456),
            AlgoTruthSource::WsAlgoUpdate,
            "user_data_stream",
            999_999,
        )
        .await
        .unwrap();

        let events = protection_truth_events(&store).await;
        assert_eq!(
            events.len(),
            1,
            "every real truth transition needs one event"
        );
        let payload = &events[0].1;
        assert_eq!(payload["truth_source"], "ws_algo_update");
        assert_eq!(payload["observation_context"], "user_data_stream");
        assert_eq!(payload["algoStatus"], "TRIGGERED");
        assert_eq!(payload["algoId"], "42011");
        assert_eq!(payload["clientAlgoId"], "algo-client-t17a");
        assert_eq!(payload["actualOrderId"], "88011");
        assert_eq!(payload["observed_at_ms"], 123_456);
        assert_eq!(payload["from"]["status"], "submitted");
        assert_eq!(payload["from"]["verified_from_exchange"], 0);
        assert_eq!(payload["to"]["status"], "triggered");
        assert_eq!(payload["to"]["verified_from_exchange"], 1);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn repeated_same_state_is_quiet_but_changed_algo_status_is_a_transition() {
        let (store, path, position_id) = fixture("truth-event-change-key").await;
        insert_protection_row(&store, &position_id, "prot_change_key", "sl", Some("42012")).await;
        for (status, observed_at_ms) in [("NEW", 100), ("NEW", 200), ("new", 300)] {
            apply_algo_truth_for_protection(
                &store,
                "cred_t17a",
                "prot_change_key",
                &algo_truth(status, 42012, None, observed_at_ms),
                AlgoTruthSource::RestAlgoQuery,
                "dedupe_guard",
                observed_at_ms + 1,
            )
            .await
            .unwrap();
        }
        let events = protection_truth_events(&store).await;
        assert_eq!(
            events.len(),
            2,
            "identical observations stay quiet, but raw algoStatus participates in change detection"
        );
        let last_verified_at_ms = sqlx::query(
            "SELECT last_verified_at_ms FROM protection_orders WHERE protection_order_id='prot_change_key'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("last_verified_at_ms");
        assert_eq!(last_verified_at_ms, 300);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn replayed_observation_uses_deterministic_event_id_and_deduplicates() {
        let (store, path, position_id) = fixture("truth-event-replay").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_event_replay",
            "sl",
            Some("42013"),
        )
        .await;
        let truth = algo_truth("NEW", 42013, None, 400);
        for replay in 0..2 {
            if replay == 1 {
                sqlx::query(
                    r#"
                    UPDATE protection_orders
                    SET status='submitted', verified_from_exchange=0,
                        algo_status='NEW', last_verified_at_ms=NULL
                    WHERE protection_order_id='prot_event_replay'
                    "#,
                )
                .execute(store.pool())
                .await
                .unwrap();
                sqlx::query(
                    "UPDATE engine_orders SET status='pending_submit' WHERE engine_order_id='eng_t17a'",
                )
                .execute(store.pool())
                .await
                .unwrap();
            }
            apply_algo_truth_for_protection(
                &store,
                "cred_t17a",
                "prot_event_replay",
                &truth,
                AlgoTruthSource::RestAlgoQuery,
                "replay_guard",
                500 + replay,
            )
            .await
            .unwrap();
        }
        let events = protection_truth_events(&store).await;
        assert_eq!(
            events.len(),
            1,
            "replaying one observation from the same prior state must dedupe by event_id"
        );
        assert_eq!(events[0].0.len(), 64, "event_id is a sha256 hex digest");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn negative_truth_preserves_last_verified_time_and_positive_truth_advances_it() {
        let (store, path, position_id) = fixture("last-verified-polarity").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_last_verified",
            "sl",
            Some("42014"),
        )
        .await;
        for (status, observed_at_ms) in [("NEW", 100), ("CANCELED", 200), ("NEW", 300)] {
            apply_algo_truth_for_protection(
                &store,
                "cred_t17a",
                "prot_last_verified",
                &algo_truth(status, 42014, None, observed_at_ms),
                AlgoTruthSource::RestAlgoQuery,
                "timestamp_guard",
                observed_at_ms + 1,
            )
            .await
            .unwrap();
            let row = sqlx::query(
                "SELECT verified_from_exchange, last_verified_at_ms FROM protection_orders WHERE protection_order_id='prot_last_verified'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap();
            let expected_verified = if status == "CANCELED" { 0 } else { 1 };
            let expected_last_verified = if status == "CANCELED" {
                100
            } else {
                observed_at_ms
            };
            assert_eq!(
                row.get::<i64, _>("verified_from_exchange"),
                expected_verified
            );
            assert_eq!(
                row.get::<i64, _>("last_verified_at_ms"),
                expected_last_verified
            );
        }
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn truth_without_exchange_observation_time_is_rejected_without_mutation() {
        let (store, path, position_id) = fixture("missing-observation-time").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_missing_observation_time",
            "sl",
            Some("42016"),
        )
        .await;
        let mut truth = algo_truth("NEW", 42016, None, 100);
        truth.updated_at_ms = None;
        let error = apply_algo_truth_for_protection(
            &store,
            "cred_t17a",
            "prot_missing_observation_time",
            &truth,
            AlgoTruthSource::RestAlgoQuery,
            "observation_time_guard",
            999,
        )
        .await
        .expect_err("local processing time must not substitute for exchange observation time");
        assert!(error.to_string().contains("omitted observation time"));
        let row = sqlx::query(
            "SELECT status, verified_from_exchange, last_verified_at_ms FROM protection_orders WHERE protection_order_id='prot_missing_observation_time'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "submitted");
        assert_eq!(row.get::<i64, _>("verified_from_exchange"), 0);
        assert_eq!(
            crate::store::try_get_nullable_string(&row, "last_verified_at_ms").unwrap(),
            None
        );
        assert!(protection_truth_events(&store).await.is_empty());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn invalid_truth_at_confirmation_deadline_still_flattens_and_pauses() {
        let (store, path, position_id) = fixture("invalid-truth-flatten").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_invalid_truth_flatten",
            "sl",
            Some("42017"),
        )
        .await;
        let flatten_calls = Arc::new(AtomicUsize::new(0));
        let fake = ExactTruthAdapter {
            truth: AlgoOrderTruth {
                algo_id: Some(BinanceAlgoId::new(42017)),
                client_algo_id: Some(BinanceClientAlgoId::new("algo-client-t17a")),
                algo_status: Some("NEW".to_string()),
                actual_order_id: None,
                updated_at_ms: None,
            },
            flatten_calls: flatten_calls.clone(),
        };

        assert!(
            (verify_or_flatten(&store, "prot_invalid_truth_flatten", &fake)
                .await
                .expect("invalid truth must fall through to fail-open flatten")
                == VerificationOutcome::FlattenStarted)
        );
        assert_eq!(flatten_calls.load(Ordering::SeqCst), 1);
        let state = sqlx::query(
            "SELECT status, verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_invalid_truth_flatten'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(state.get::<String, _>("status"), "failed");
        assert_eq!(state.get::<i64, _>("verified_from_exchange"), 0);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
        );
        assert!(protection_truth_events(&store).await.is_empty());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn audit_event_failure_cannot_block_terminal_state_or_flatten() {
        let (store, path, position_id) = fixture("audit-failure-fail-open").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_audit_failure",
            "sl",
            Some("42015"),
        )
        .await;
        sqlx::query(
            r#"
            UPDATE protection_orders
            SET status='confirmed', verified_from_exchange=1,
                algo_status='NEW', last_verified_at_ms=10
            WHERE protection_order_id='prot_audit_failure'
            "#,
        )
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query("DROP TABLE engine_checkpoints")
            .execute(store.pool())
            .await
            .unwrap();

        let disposition = apply_algo_truth_for_protection(
            &store,
            "cred_t17a",
            "prot_audit_failure",
            &algo_truth("CANCELED", 42015, None, 20),
            AlgoTruthSource::WsAlgoUpdate,
            "audit_failure_guard",
            21,
        )
        .await
        .expect("audit failure must not escape after state commit");
        assert_eq!(disposition, AlgoTruthDisposition::Missing);
        let state = sqlx::query(
            "SELECT status, verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_audit_failure'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(state.get::<String, _>("status"), "canceled");
        assert_eq!(state.get::<i64, _>("verified_from_exchange"), 0);

        let fake = FakeAdapter::default();
        flatten_and_pause(
            &store,
            "prot_audit_failure",
            "bot_t17a",
            &fake,
            "terminal truth after audit failure",
        )
        .await
        .expect("audit failure must not block fail-open flatten/pause");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn active_get_algo_order_truth_confirms_without_flatten_or_pause_new() {
        let (store, path, position_id) = fixture("rest-active").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_rest_active",
            "sl",
            Some("42001"),
        )
        .await;
        let flatten_calls = Arc::new(AtomicUsize::new(0));
        let fake = ExactTruthAdapter {
            truth: AlgoOrderTruth::from_json(serde_json::json!({
                "algoId": 42001,
                "clientAlgoId": "algo-client-t17a",
                "algoStatus": "NEW",
                "actualOrderId": "",
                "updateTime": 2
            }))
            .unwrap(),
            flatten_calls: flatten_calls.clone(),
        };

        assert!(
            (verify_or_flatten(&store, "prot_rest_active", &fake)
                .await
                .unwrap()
                == VerificationOutcome::Confirmed)
        );
        let row = sqlx::query(
            "SELECT verified_from_exchange, status FROM protection_orders WHERE protection_order_id='prot_rest_active'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("verified_from_exchange"), 1);
        assert_eq!(row.get::<String, _>("status"), "confirmed");
        let engine_status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='eng_t17a'")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<String, _>("status");
        assert_eq!(engine_status, "confirmed");
        assert_eq!(flatten_calls.load(Ordering::SeqCst), 0);
        let events = protection_truth_events(&store).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1["truth_source"], "rest_algo_query");
        assert_eq!(
            events[0].1["observation_context"],
            "protection_confirmation"
        );
        assert_eq!(events[0].1["observed_at_ms"], 2);
        assert_bot_not_paused(&store).await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn active_algo_update_truth_confirms_without_flatten_or_pause_new() {
        let (store, path, position_id) = fixture("ws-active").await;
        insert_protection_row(&store, &position_id, "prot_ws_active", "sl", Some("42003")).await;
        let envelope: crate::binance_user_stream::BinanceWsEnvelope = serde_json::from_str(
            r#"{"e":"ALGO_UPDATE","E":2,"T":3,"o":{"caid":"algo-client-t17a","aid":42003,"X":"NEW","ai":""}}"#,
        )
        .unwrap();
        crate::svc::BinanceEventSink::new(store.clone())
            .handle_ws_envelope("cred_t17a", &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        let fake = FakeAdapter::default();
        assert!(
            (verify_or_flatten(&store, "prot_ws_active", &fake)
                .await
                .unwrap()
                == VerificationOutcome::Confirmed)
        );
        let row = sqlx::query(
            "SELECT verified_from_exchange, status FROM protection_orders WHERE protection_order_id='prot_ws_active'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("verified_from_exchange"), 1);
        assert_eq!(row.get::<String, _>("status"), "confirmed");
        let engine_status =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id='eng_t17a'")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<String, _>("status");
        assert_eq!(engine_status, "confirmed");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 0);
        assert_bot_not_paused(&store).await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn triggered_algo_update_truth_binds_actual_order_id_without_missing() {
        let (store, path, position_id) = fixture("ws-triggered").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_ws_triggered",
            "sl",
            Some("42004"),
        )
        .await;
        let envelope: crate::binance_user_stream::BinanceWsEnvelope = serde_json::from_str(
            r#"{"e":"ALGO_UPDATE","E":2,"T":3,"o":{"caid":"algo-client-t17a","aid":42004,"X":"TRIGGERED","ai":"88004"}}"#,
        )
        .unwrap();
        crate::svc::BinanceEventSink::new(store.clone())
            .handle_ws_envelope("cred_t17a", &envelope)
            .await
            .expect("the frame must be applied without a write failure");

        let fake = FakeAdapter::default();
        assert!(
            (verify_or_flatten(&store, "prot_ws_triggered", &fake)
                .await
                .unwrap()
                == VerificationOutcome::Confirmed)
        );
        let protection = sqlx::query(
            "SELECT exchange_order_id, algo_status, status, verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_ws_triggered'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(protection.get::<String, _>("exchange_order_id"), "88004");
        assert_eq!(protection.get::<String, _>("algo_status"), "TRIGGERED");
        assert_eq!(protection.get::<String, _>("status"), "triggered");
        assert_eq!(protection.get::<i64, _>("verified_from_exchange"), 1);
        let engine = sqlx::query(
            "SELECT exchange_order_id, status FROM engine_orders WHERE engine_order_id='eng_t17a'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(engine.get::<String, _>("exchange_order_id"), "88004");
        assert_eq!(engine.get::<String, _>("status"), "triggered");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 0);
        assert_bot_not_paused(&store).await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn submission_ack_alone_never_confirms_protection() {
        let (store, path, position_id) = fixture("ack-only").await;
        insert_protection_row(&store, &position_id, "prot_ack_only", "sl", None).await;
        let ack: BinanceAlgoOrderResponse = serde_json::from_value(serde_json::json!({
            "algoId": 42005,
            "clientAlgoId": "algo-client-t17a",
            "algoStatus": "NEW",
            "actualOrderId": ""
        }))
        .unwrap();
        mark_algo_submitted(
            &store,
            "prot_ack_only",
            &BinanceClientAlgoId::new("algo-client-t17a"),
            &ack,
            2,
        )
        .await
        .unwrap();
        let row = sqlx::query(
            "SELECT verified_from_exchange, status FROM protection_orders WHERE protection_order_id='prot_ack_only'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("verified_from_exchange"), 0);
        assert_eq!(row.get::<String, _>("status"), "submitted");

        let fake = FakeAdapter::default();
        assert!(
            (verify_or_flatten(&store, "prot_ack_only", &fake)
                .await
                .unwrap()
                == VerificationOutcome::FlattenStarted)
        );
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b7_exact_exchange_algo_id_truth_is_the_only_confirmation_key() {
        let (store, path, position_id) = fixture("exact").await;
        insert_protection_row(&store, &position_id, "prot_exact", "sl", Some("42001")).await;
        let fake = FakeAdapter::default();
        fake.open_ids
            .lock()
            .expect("ids")
            .insert("42001".to_string());

        assert!(
            (verify_or_flatten(&store, "prot_exact", &fake)
                .await
                .unwrap()
                == VerificationOutcome::Confirmed)
        );
        let row = sqlx::query(
            "SELECT verified_from_exchange, status FROM protection_orders WHERE protection_order_id='prot_exact'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<i64, _>("verified_from_exchange"), 1);
        assert_eq!(row.get::<String, _>("status"), "confirmed");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 0);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b2_algo_id_is_persisted_only_in_exchange_algo_id() {
        let (store, path, position_id) = fixture("algo-identity").await;
        insert_protection_row(&store, &position_id, "prot_algo_identity", "sl", None).await;
        let response: BinanceAlgoOrderResponse = serde_json::from_value(serde_json::json!({
            "algoId": 99101,
            "clientAlgoId": "algo-client-t17a",
            "algoStatus": "NEW",
            "actualOrderId": ""
        }))
        .unwrap();
        mark_algo_submitted(
            &store,
            "prot_algo_identity",
            &BinanceClientAlgoId::new("algo-client-t17a"),
            &response,
            2,
        )
        .await
        .unwrap();

        let protection = sqlx::query(
            "SELECT exchange_algo_id, exchange_order_id FROM protection_orders WHERE protection_order_id='prot_algo_identity'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(protection.get::<String, _>("exchange_algo_id"), "99101");
        assert_eq!(
            crate::store::try_get_nullable_string(&protection, "exchange_order_id").unwrap(),
            None
        );
        let engine_actual_row = sqlx::query(
            "SELECT exchange_order_id FROM engine_orders WHERE engine_order_id='eng_t17a'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        let engine_actual =
            crate::store::try_get_nullable_string(&engine_actual_row, "exchange_order_id").unwrap();
        assert_eq!(engine_actual, None);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn algo_update_arriving_before_rest_ack_keeps_newer_exchange_truth() {
        let (store, path, position_id) = fixture("ws-before-ack").await;
        insert_protection_row(&store, &position_id, "prot_ws_before_ack", "sl", None).await;
        let truth = AlgoOrderTruth {
            algo_id: Some(BinanceAlgoId::new(99102)),
            client_algo_id: Some(BinanceClientAlgoId::new("algo-client-t17a")),
            algo_status: Some("TRIGGERED".to_string()),
            actual_order_id: Some("88102".to_string()),
            updated_at_ms: Some(3),
        };
        apply_algo_truth_for_protection(
            &store,
            "cred_t17a",
            "prot_ws_before_ack",
            &truth,
            AlgoTruthSource::WsAlgoUpdate,
            "user_data_stream",
            3,
        )
        .await
        .unwrap();
        let ack: BinanceAlgoOrderResponse = serde_json::from_value(serde_json::json!({
            "algoId": 99102,
            "clientAlgoId": "algo-client-t17a",
            "algoStatus": "NEW",
            "actualOrderId": ""
        }))
        .unwrap();
        mark_algo_submitted(
            &store,
            "prot_ws_before_ack",
            &BinanceClientAlgoId::new("algo-client-t17a"),
            &ack,
            4,
        )
        .await
        .unwrap();

        let row = sqlx::query(
            "SELECT exchange_order_id, algo_status, status FROM protection_orders WHERE protection_order_id='prot_ws_before_ack'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("exchange_order_id"), "88102");
        assert_eq!(row.get::<String, _>("algo_status"), "TRIGGERED");
        assert_eq!(row.get::<String, _>("status"), "triggered");
        let engine = sqlx::query(
            "SELECT exchange_order_id, status FROM engine_orders WHERE engine_order_id='eng_t17a'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(engine.get::<String, _>("exchange_order_id"), "88102");
        assert_eq!(engine.get::<String, _>("status"), "triggered");
        cleanup(store, path).await;
    }

    #[derive(Clone, Default)]
    struct TerminalConfirmationAdapter {
        queried: Arc<AtomicUsize>,
        dispatched: Arc<AtomicUsize>,
        entered: Option<Arc<tokio::sync::Notify>>,
        resume: Option<Arc<tokio::sync::Notify>>,
        close_on_dispatch: Option<(Store, String)>,
    }

    #[tonic::async_trait]
    impl ProtectionAdapter for TerminalConfirmationAdapter {
        async fn query_algo_order(
            &self,
            _: Option<&str>,
            _: Option<&str>,
        ) -> anyhow::Result<Option<AlgoOrderTruth>> {
            self.queried.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = &self.entered {
                entered.notify_one();
            }
            if let Some(resume) = &self.resume {
                resume.notified().await;
            }
            Ok(None)
        }
        async fn submit_reduce_only_flatten(&self) -> anyhow::Result<()> {
            if let Some((store, position_id)) = &self.close_on_dispatch {
                assert_eq!(
                    latch(store, "bot:bot_t17a").await,
                    ("flatten".to_string(), Some("flattening".to_string())),
                    "the adapter may run only after its latch commits"
                );
                terminal_close_position(store, position_id).await;
            }
            self.dispatched.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn terminal_close_position(store: &Store, position_id: &str) {
        let mut tx = store.pool().begin().await.unwrap();
        crate::position_state::mark_position_closed(
            &mut tx,
            position_id,
            10,
            Some(10),
            "paper_fill",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_confirmation_close_during_query_retires_only_old_obligation() {
        let (store, path, position_id) = fixture("terminal-during-query").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_terminal_race",
            "sl",
            Some("42002"),
        )
        .await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let adapter = Arc::new(TerminalConfirmationAdapter {
            entered: Some(entered.clone()),
            resume: Some(resume.clone()),
            ..Default::default()
        });
        let timer = spawn_confirmation_task(
            store.clone(),
            "prot_terminal_race".into(),
            1,
            adapter.clone(),
        );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        terminal_close_position(&store, &position_id).await;
        resume.notify_one();
        tokio::time::timeout(Duration::from_secs(2), timer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(adapter.queried.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.dispatched.load(Ordering::SeqCst), 0);
        assert_bot_not_paused(&store).await;
        let row = sqlx::query("SELECT status, verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_terminal_race'")
            .fetch_one(store.pool()).await.unwrap();
        assert_eq!(row.get::<String, _>("status"), "submitted");
        assert_eq!(
            row.get::<i64, _>("verified_from_exchange"),
            0,
            "expired obligation is not fabricated venue confirmation"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn terminal_confirmation_only_strict_closed_zero_retires_the_obligation() {
        for (state, qty, retired) in [
            ("closed", "0.000", true),
            ("closed", "+0", true),
            ("closed", "-0.0", true),
            ("open", "0", false),
            ("closing", "0", false),
            ("protected", "1", false),
            ("closed", ".", false),
            ("closed", "+", false),
            ("closed", "0..0", false),
            ("closed", "", false),
            ("closed", "NaN", false),
            ("closed", "1e-999", false),
            (
                "closed",
                "0.000000000000000000000000000000000000000001",
                false,
            ),
        ] {
            let (store, path, position_id) = fixture("terminal-zero-boundary").await;
            insert_protection_row(&store, &position_id, "prot_terminal_quantity", "sl", None).await;
            sqlx::query(
                "UPDATE position_state SET state=?, qty=?, active_key=NULL WHERE position_id=?",
            )
            .bind(state)
            .bind(qty)
            .bind(&position_id)
            .execute(store.pool())
            .await
            .unwrap();
            let adapter = TerminalConfirmationAdapter::default();
            let result = verify_or_flatten(&store, "prot_terminal_quantity", &adapter)
                .await
                .unwrap();
            assert_eq!(
                result,
                if retired {
                    VerificationOutcome::PositionClosed
                } else {
                    VerificationOutcome::FlattenStarted
                },
                "state={state} qty={qty:?}"
            );
            assert_eq!(
                adapter.dispatched.load(Ordering::SeqCst),
                usize::from(!retired)
            );
            if retired {
                assert_bot_not_paused(&store).await;
            } else {
                assert_eq!(
                    latch(&store, "bot:bot_t17a").await,
                    ("flatten".into(), Some("flattening".into()))
                );
            }
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn terminal_confirmation_guard_requires_exact_position_ownership() {
        for (wrong_credential, wrong_bot, missing) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let (store, path, position_id) = fixture("terminal-identity").await;
            terminal_close_position(&store, &position_id).await;
            let result = crate::kill_switch::latch_flatten_unless_position_closed(
                &store,
                if missing { "pos_absent" } else { &position_id },
                if wrong_credential {
                    "cred_foreign"
                } else {
                    "cred_t17a"
                },
                if wrong_bot { "bot_foreign" } else { "bot_t17a" },
                "terminal identity test",
                20,
            )
            .await
            .unwrap();
            assert!(
                result.is_some(),
                "foreign or missing identity is not closed proof"
            );
            cleanup(store, path).await;
        }
    }

    #[tokio::test]
    async fn terminal_confirmation_sqlite_guard_waits_for_the_current_close() {
        let (store, path, position_id) = fixture("terminal-lock").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_terminal_lock",
            "sl",
            Some("42002"),
        )
        .await;
        let second = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let adapter = Arc::new(TerminalConfirmationAdapter {
            entered: Some(entered.clone()),
            resume: Some(resume.clone()),
            ..Default::default()
        });
        let cloned_store = store.clone();
        let cloned_adapter = adapter.clone();
        let task = tokio::spawn(async move {
            verify_or_flatten(&cloned_store, "prot_terminal_lock", cloned_adapter.as_ref()).await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let mut closing = second.pool().begin().await.unwrap();
        crate::position_state::mark_position_closed(
            &mut closing,
            &position_id,
            10,
            Some(10),
            "paper_fill",
        )
        .await
        .unwrap();
        resume.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !task.is_finished(),
            "guard must wait for the concurrent writer, not read an old open snapshot"
        );
        closing.commit().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result, VerificationOutcome::PositionClosed);
        assert_eq!(adapter.dispatched.load(Ordering::SeqCst), 0);
        assert_bot_not_paused(&store).await;
        second.pool().close().await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn terminal_confirmation_failed_guard_latch_never_dispatches() {
        let (store, path, position_id) = fixture("terminal-failed-latch").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_terminal_failed_latch",
            "sl",
            None,
        )
        .await;
        sqlx::query("CREATE TRIGGER terminal_block_latch BEFORE INSERT ON kill_switch_state BEGIN SELECT RAISE(ABORT, 'terminal latch storage failure'); END")
            .execute(store.pool()).await.unwrap();
        let adapter = TerminalConfirmationAdapter::default();
        let outcome = verify_or_flatten(&store, "prot_terminal_failed_latch", &adapter).await;
        assert!(outcome.is_err());
        assert_eq!(adapter.dispatched.load(Ordering::SeqCst), 0);
        assert_bot_not_paused(&store).await;
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn terminal_confirmation_active_failure_commits_latch_before_dispatch_and_close() {
        let (store, path, position_id) = fixture("terminal-arm-first").await;
        insert_protection_row(&store, &position_id, "prot_terminal_arm_first", "sl", None).await;
        let adapter = TerminalConfirmationAdapter {
            close_on_dispatch: Some((store.clone(), position_id.clone())),
            ..Default::default()
        };
        let outcome = tokio::time::timeout(
            Duration::from_secs(3),
            verify_or_flatten(&store, "prot_terminal_arm_first", &adapter),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outcome, VerificationOutcome::FlattenStarted);
        assert_eq!(adapter.dispatched.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".into(), Some("flattening".into()))
        );
        let row = sqlx::query("SELECT state, qty FROM position_state WHERE position_id=?")
            .bind(&position_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("state"), "closed");
        assert_eq!(row.get::<String, _>("qty"), "0");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn terminal_confirmation_mysql_guard_reads_the_committed_close() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let Ok(admin_url) = std::env::var("RUSTCORE_TEST_MYSQL_URL") else {
            assert_ne!(
                std::env::var("RUSTCORE_REQUIRE_MYSQL_TESTS").as_deref(),
                Ok("1"),
                "real MySQL is required for the terminal guard race"
            );
            eprintln!("SKIPPED real MySQL terminal guard race: RUSTCORE_TEST_MYSQL_URL is unset");
            return;
        };
        let admin = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .unwrap();
        let database = format!(
            "rustcore_terminal_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE `{database}`"))
            .execute(&admin)
            .await
            .unwrap();
        let (prefix, _) = admin_url.rsplit_once('/').unwrap();
        let url = format!("{prefix}/{database}");
        let store = Store::connect(&url).await.unwrap();
        let other = Store::connect(&url).await.unwrap();
        sqlx::query("INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, state, active_key, qty, updated_at_ms) VALUES ('pos_mysql_terminal', 'cred_t17a', 'bot_t17a', 'binance|futures|BTC|USDT|USDT', 'long', 1, 'open', 'mysql-terminal-active', '1', 1)")
            .execute(store.pool()).await.unwrap();
        let mut close_tx = other.pool().begin().await.unwrap();
        crate::position_state::mark_position_closed(
            &mut close_tx,
            "pos_mysql_terminal",
            10,
            Some(10),
            "paper_fill",
        )
        .await
        .unwrap();
        let task_store = store.clone();
        let task = tokio::spawn(async move {
            crate::kill_switch::latch_flatten_unless_position_closed(
                &task_store,
                "pos_mysql_terminal",
                "cred_t17a",
                "bot_t17a",
                "mysql terminal race",
                11,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let waited_for_close = !task.is_finished();
        close_tx.commit().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let no_latch: i64 = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_state WHERE scope_key='bot:bot_t17a' AND level<>'none'")
            .fetch_one(store.pool()).await.unwrap().get("n");
        other.pool().close().await;
        store.pool().close().await;
        sqlx::query(&format!("DROP DATABASE `{database}`"))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
        assert!(
            waited_for_close,
            "the MySQL guard must lock the current position row before deciding"
        );
        assert!(
            result.unwrap().is_none(),
            "a close committed before the guarded decision retires the obligation"
        );
        assert_eq!(no_latch, 0);
    }

    #[tokio::test]
    async fn terminal_confirmation_closed_position_never_rearms_or_flattens_next_position() {
        let (store, path, position_id) = fixture("terminal-confirmation-closed").await;
        insert_protection_row(
            &store,
            &position_id,
            "prot_terminal_closed",
            "sl",
            Some("42002"),
        )
        .await;
        sqlx::query("UPDATE position_state SET state='closed', qty='0', active_key=NULL, closed_at_ms=2 WHERE position_id=?")
            .bind(&position_id).execute(store.pool()).await.unwrap();
        sqlx::query("UPDATE protection_orders SET status='canceled', algo_status='CANCELED', verified_from_exchange=0 WHERE protection_order_id='prot_terminal_closed'")
            .execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, state, active_key, qty, updated_at_ms) VALUES ('pos_next_terminal', 'cred_t17a', 'bot_t17a', 'binance|futures|BTC|USDT|USDT', 'long', 2, 'open', 'terminal-next-active', '1', 3)")
            .execute(store.pool()).await.unwrap();
        let fake = Arc::new(FakeAdapter::default());
        spawn_confirmation_task(
            store.clone(),
            "prot_terminal_closed".to_string(),
            1,
            fake.clone(),
        )
        .await
        .expect("real scheduled confirmation completed");
        assert_eq!(
            fake.flatten_calls.load(Ordering::SeqCst),
            0,
            "an obsolete timer must not dispatch against the bot's next position"
        );
        assert_bot_not_paused(&store).await;
        let current: String =
            sqlx::query("SELECT state FROM position_state WHERE position_id='pos_next_terminal'")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get("state");
        assert_eq!(current, "open");
        let old_status: String = sqlx::query(
            "SELECT status FROM protection_orders WHERE protection_order_id='prot_terminal_closed'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("status");
        assert_eq!(
            old_status, "canceled",
            "terminal cleanup is not a protection submission failure"
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn b7_submission_ack_without_exact_exchange_truth_flattens_and_pauses_bot() {
        let (store, path, position_id) = fixture("missing").await;
        insert_protection_row(&store, &position_id, "prot_missing", "sl", None).await;
        sqlx::query(
            "UPDATE protection_orders SET exchange_algo_id='42002', client_algo_id='algo-42002', algo_status='NEW' WHERE protection_order_id='prot_missing'",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let fake = FakeAdapter::default();
        fake.open_ids
            .lock()
            .expect("ids")
            .insert("unrelated-order-id".to_string());

        assert!(
            (verify_or_flatten(&store, "prot_missing", &fake)
                .await
                .unwrap()
                == VerificationOutcome::FlattenStarted)
        );
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
        );
        let verified = sqlx::query(
            "SELECT verified_from_exchange FROM protection_orders WHERE protection_order_id='prot_missing'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("verified_from_exchange");
        assert_eq!(verified, 0);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn requested_protection_with_null_exchange_id_flattens_without_panicking() {
        let (store, path, position_id) = fixture("requested-null-exchange-id").await;
        let mut tx = store.pool().begin().await.expect("begin protection write");
        insert_requested(
            &mut tx,
            "prot_requested_null",
            &position_id,
            "cred_t17a",
            "sl",
            1,
            "eng_t17a",
            "algo-requested-null",
            "99",
            true,
            1,
        )
        .await
        .expect("write requested protection through production helper");
        tx.commit().await.expect("commit requested protection");

        let fake = FakeAdapter::default();
        assert!(
            (verify_or_flatten(&store, "prot_requested_null", &fake)
                .await
                .expect("NULL exchange id is absence, not a decode panic")
                == VerificationOutcome::FlattenStarted)
        );
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
            "missing exchange truth must latch a retryable flatten, not just pause_new",
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn injected_sl_submit_failure_always_flattens_and_pauses_bot() {
        let (store, path, position_id) = fixture("submit-fail").await;
        insert_protection_row(&store, &position_id, "prot_submit_fail", "sl", None).await;
        let fake = FakeAdapter::default();

        flatten_and_pause(
            &store,
            "prot_submit_fail",
            "bot_t17a",
            &fake,
            "injected SL submit failure",
        )
        .await
        .unwrap();
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
        );
        cleanup(store, path).await;
    }

    /// **闩没落库就一张单都不许发。**
    ///
    /// R14 把 latch 的错误检查放在 `submit_reduce_only_flatten()` **之后**：
    /// DB 闩写失败时函数确实返回 `Err`，可 reduce-only flatten **已经发到
    /// 交易所去了**——留下的正是"先闩后发"要消灭的那个状态：交易所收到过
    /// 动作，而本地没有任何 latch 会在重启后驱动重试或对账收敛。
    ///
    /// 这里用一条真实的持久化失败构造：`kill_switch_state` 上装一个 BEFORE
    /// UPDATE/INSERT 触发器直接 ABORT（等价于磁盘满 / 约束冲突 / 连接断）。
    /// 判据是 **adapter 调用数为 0**——不是"返回了 Err"（R14 也返回 Err）。
    #[tokio::test]
    async fn a_failed_latch_write_dispatches_no_flatten_at_all() {
        let (store, path, position_id) = fixture("latch-write-fails").await;
        insert_protection_row(&store, &position_id, "prot_latch_fail", "sl", None).await;
        for statement in [
            "CREATE TRIGGER l4_block_latch_update BEFORE UPDATE ON kill_switch_state \
             BEGIN SELECT RAISE(ABORT, 'injected latch write failure'); END",
            "CREATE TRIGGER l4_block_latch_insert BEFORE INSERT ON kill_switch_state \
             BEGIN SELECT RAISE(ABORT, 'injected latch write failure'); END",
        ] {
            sqlx::query(statement)
                .execute(store.pool())
                .await
                .expect("install the durable-write failure");
        }
        let fake = FakeAdapter::default();

        let error = flatten_and_pause_bot(&store, "bot_t17a", &fake, "injected SL submit failure")
            .await
            .expect_err("a latch that cannot be persisted must fail the whole disposition");
        assert!(
            format!("{error:#}").contains("latch retryable bot flatten"),
            "the error must name the step that failed: {error:#}"
        );
        assert_eq!(
            fake.flatten_calls.load(Ordering::SeqCst),
            0,
            "no reduce-only flatten may leave the process before its latch is durable"
        );
        let latched = sqlx::query(
            "SELECT COUNT(*) AS n FROM kill_switch_state WHERE scope_key=? AND level='flatten'",
        )
        .bind("bot:bot_t17a")
        .fetch_one(store.pool())
        .await
        .expect("count latches")
        .get::<i64, _>("n");
        assert_eq!(latched, 0, "the latch really did not land");
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn an_invalid_durable_latch_is_preserved_and_remains_flatten_retry_authority() {
        let (store, path, _) = fixture("invalid-latch-preserved").await;
        sqlx::query(
            "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
             armed_at_ms, flatten_state, last_reconcile_clean, last_reconcile_at_ms, \
             updated_at_ms, flatten_generation) VALUES ('bot:bot_t17a', ' FLATTEN ', \
             'invalid fixture', 'test_fixture', 1, 'flattened', 1, 1, 1, 7)",
        )
        .execute(store.pool())
        .await
        .expect("seed invalid durable authority");
        let fake = FakeAdapter::default();

        let authority = latch_flatten_after_protection_failure(
            &store,
            "bot_t17a",
            "protection failure with corrupt authority",
        )
        .await
        .expect("the exact invalid row is already conservative retry authority");
        assert_eq!(
            authority,
            ProtectionFailureLatch::PreservedInvalid(crate::kill_switch::InvalidPersistedLevel {
                scope_key: "bot:bot_t17a".to_string(),
                raw_level: " FLATTEN ".to_string(),
            })
        );
        dispatch_latched_flatten(&fake)
            .await
            .expect("safety dispatch remains available");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);

        let persisted: (String, i64) = sqlx::query_as(
            "SELECT level, flatten_generation FROM kill_switch_state \
             WHERE scope_key='bot:bot_t17a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("reload invalid authority");
        assert_eq!(persisted, (" FLATTEN ".to_string(), 7));
        let retry = crate::kill_switch::action_retry_targets(&store)
            .await
            .expect("scan retry authorities")
            .into_iter()
            .find(|target| target.scope_key == "bot:bot_t17a")
            .expect("invalid authority remains in the crash-recovery scan");
        assert_eq!(retry.level, crate::kill_switch::Level::Flatten);
        assert!(retry.invalid_level.is_some());
        cleanup(store, path).await;
    }

    /// **反向**：闩写得下去时，同一条路照常发单。没有这一条，用
    /// 「永远不 dispatch」也能让上一条全绿。
    #[tokio::test]
    async fn a_durable_latch_still_dispatches_exactly_one_flatten() {
        let (store, path, position_id) = fixture("latch-write-ok").await;
        insert_protection_row(&store, &position_id, "prot_latch_ok", "sl", None).await;
        let fake = FakeAdapter::default();

        flatten_and_pause_bot(&store, "bot_t17a", &fake, "injected SL submit failure")
            .await
            .expect("a durable latch lets the flatten through");
        assert_eq!(fake.flatten_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            latch(&store, "bot:bot_t17a").await,
            ("flatten".to_string(), Some("flattening".to_string())),
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn three_second_confirmation_window_does_not_delay_same_credential_events() {
        let (store, path, position_id) = fixture("latency").await;
        insert_protection_row(&store, &position_id, "prot_latency", "sl", Some("42003")).await;
        let fake = Arc::new(FakeAdapter::default());
        fake.open_ids
            .lock()
            .expect("ids")
            .insert("42003".to_string());
        let confirmation = spawn_confirmation_task(
            store.clone(),
            "prot_latency".to_string(),
            DEFAULT_CONFIRM_TIMEOUT_MS,
            fake,
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Instant>(16);
        let consumer = tokio::spawn(async move {
            let mut latencies = Vec::new();
            while let Some(sent_at) = rx.recv().await {
                latencies.push(sent_at.elapsed());
            }
            latencies
        });
        for _ in 0..8 {
            tx.send(Instant::now()).await.unwrap();
        }
        drop(tx);
        let latencies = consumer.await.unwrap();
        let max_latency = latencies.into_iter().max().unwrap();
        eprintln!(
            "T1.7-a same-credential max event latency: {:.3} ms",
            max_latency.as_secs_f64() * 1_000.0
        );
        assert!(
            max_latency < Duration::from_millis(100),
            "same-credential max event latency was {max_latency:?}"
        );

        confirmation.abort();
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn position_kind_generation_unique_constraint_is_enforced() {
        let (store, path, position_id) = fixture("unique").await;
        insert_protection_row(&store, &position_id, "prot_unique_1", "sl", Some("1")).await;
        let duplicate = sqlx::query(
            r#"
            INSERT INTO protection_orders (
              protection_order_id, position_id, credential_id, kind, generation,
              trigger_price, status, created_at_ms, updated_at_ms
            ) VALUES ('prot_unique_2', ?, 'cred_t17a', 'sl', 1, '98', 'requested', 1, 1)
            "#,
        )
        .bind(&position_id)
        .execute(store.pool())
        .await;
        assert!(
            duplicate.is_err(),
            "duplicate position+kind+generation must fail"
        );
        cleanup(store, path).await;
    }

    #[test]
    fn b4_recovery_reuses_active_and_binds_triggered_before_repairing() {
        assert_eq!(
            recovery_protection_action(true, Some(AlgoTruthDisposition::Active)),
            RecoveryProtectionAction::ReuseActive
        );
        assert_eq!(
            recovery_protection_action(true, Some(AlgoTruthDisposition::Triggered)),
            RecoveryProtectionAction::BindTriggered
        );
        assert_eq!(
            recovery_protection_action(true, Some(AlgoTruthDisposition::Missing)),
            RecoveryProtectionAction::RepairMissing
        );
        assert_eq!(
            recovery_protection_action(false, None),
            RecoveryProtectionAction::NoPosition
        );
    }
}
