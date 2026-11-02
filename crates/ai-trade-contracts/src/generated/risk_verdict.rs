// AUTO-GENERATED FROM schemas/risk-verdict.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskVerdict {
    pub schema_version: i64,
    pub risk_verdict_id: String,
    pub intent_id: String,
    pub created_at_ms: u64,
    pub policy_version: String,
    pub policy_hash: String,
    pub decision: RiskVerdictDecision,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub reject_reason_code: Option<RejectReasonCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<SizingResult>,
    pub rules: Vec<RuleResult>,
    pub budget_as_of_ms: u64,
    pub evaluator_version: String,
}

pub type DecimalString = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RejectReasonCode {
    #[serde(rename = "DATA_QUALITY_FAIL")]
    DataQualityFail,
    #[serde(rename = "STALE_RISK_BUDGET")]
    StaleRiskBudget,
    #[serde(rename = "KILL_SWITCH_ACTIVE")]
    KillSwitchActive,
    #[serde(rename = "INVALID_PERSISTED_KILL_SWITCH")]
    InvalidPersistedKillSwitch,
    #[serde(rename = "MISSING_PROTECTION")]
    MissingProtection,
    #[serde(rename = "RR_TOO_LOW")]
    RrTooLow,
    #[serde(rename = "TP_BELOW_COST")]
    TpBelowCost,
    #[serde(rename = "CONVICTION_TOO_LOW")]
    ConvictionTooLow,
    #[serde(rename = "LEVERAGE_EXCEEDED")]
    LeverageExceeded,
    #[serde(rename = "DUPLICATE_ACTIVE_ENTRY")]
    DuplicateActiveEntry,
    #[serde(rename = "NOTIONAL_EXCEEDED")]
    NotionalExceeded,
    #[serde(rename = "EXPOSURE_EXCEEDED")]
    ExposureExceeded,
    #[serde(rename = "TOO_MANY_POSITIONS")]
    TooManyPositions,
    #[serde(rename = "DAILY_LOSS_REACHED")]
    DailyLossReached,
    #[serde(rename = "DRAWDOWN_REACHED")]
    DrawdownReached,
    #[serde(rename = "EQUITY_BELOW_MIN")]
    EquityBelowMin,
    #[serde(rename = "INTENT_EXPIRED")]
    IntentExpired,
    #[serde(rename = "RESERVATION_CONFLICT")]
    ReservationConflict,
    #[serde(rename = "SYMBOL_DISABLED")]
    SymbolDisabled,
    #[serde(rename = "SLIPPAGE_EXCEEDED")]
    SlippageExceeded,
    #[serde(rename = "DEGRADED_CREDENTIAL")]
    DegradedCredential,
    #[serde(rename = "SIZE_BELOW_EXCHANGE_MIN")]
    SizeBelowExchangeMin,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SizingResult {
    pub equity_usdt: DecimalString,
    pub risk_per_trade_pct: f64,
    pub risk_budget_usdt: DecimalString,
    pub entry_ref_price: DecimalString,
    pub stop_loss_price: DecimalString,
    pub stop_distance_pct: f64,
    pub leverage: u64,
    pub computed_qty: DecimalString,
    pub capped_by: Vec<String>,
    pub final_qty: DecimalString,
    pub final_notional_usdt: DecimalString,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuleResult {
    pub rule: String,
    pub result: RuleResultResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RiskVerdictDecision {
    #[serde(rename = "approve")]
    Approve,
    #[serde(rename = "resize")]
    Resize,
    #[serde(rename = "reject")]
    Reject,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RuleResultResult {
    #[serde(rename = "pass")]
    Pass,
    #[serde(rename = "capped")]
    Capped,
    #[serde(rename = "reject")]
    Reject,
}
