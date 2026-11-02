// AUTO-GENERATED FROM schemas/member-decision-context.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemberDecisionContext {
    pub schema_version: i64,
    pub context_id: String,
    pub context_hash: String,
    pub created_at_ms: u64,
    pub as_of_ms: u64,
    pub cycle_id: String,
    pub member_id: String,
    pub trading_account_id: String,
    pub market_context_id: String,
    pub market_context_hash: String,
    pub position: PositionFact,
    pub unrealized_pnl: PrivateDecimalFact,
    pub exposure: PrivateDecimalFact,
    pub drawdown: PrivateDecimalFact,
    pub recent_trades: RecentTradesFact,
    pub runtime_eligibility: RuntimeEligibilityFact,
}

pub type DecimalString = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum FactStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionValue {
    pub position_id: String,
    pub symbol_key: String,
    pub side: PositionValueSide,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionFact {
    pub status: FactStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<PositionValue>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateDecimalFact {
    pub status: FactStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<DecimalString>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecentTradesSummary {
    pub closed_count: u64,
    pub realized_pnl: DecimalString,
    pub lookback_start_ms: u64,
    pub lookback_end_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecentTradesFact {
    pub status: FactStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub summary: Option<RecentTradesSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEligibilityValue {
    pub can_open: bool,
    pub can_close: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEligibilityFact {
    pub status: FactStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<RuntimeEligibilityValue>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum PositionValueSide {
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "short")]
    Short,
}
