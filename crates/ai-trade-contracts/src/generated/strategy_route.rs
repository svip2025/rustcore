// AUTO-GENERATED FROM schemas/strategy-route.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyRoute {
    pub schema_version: i64,
    pub route_id: String,
    pub route_hash: String,
    pub created_at_ms: u64,
    pub router_version: String,
    pub router_config_hash: String,
    pub effective_policy_hash: String,
    pub horizon: Horizon,
    pub regime_ref: RegimeReference,
    pub route: RouteContext,
    pub reasons: Vec<RouteReason>,
    pub fast_path: bool,
    pub route_state: RouteState,
}

pub type DecimalString = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Horizon {
    #[serde(rename = "intraday")]
    Intraday,
    #[serde(rename = "swing")]
    Swing,
    #[serde(rename = "position")]
    Position,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RouteContext {
    #[serde(rename = "TREND")]
    Trend,
    #[serde(rename = "RANGE")]
    Range,
    #[serde(rename = "NO_TRADE")]
    NoTrade,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegimeReference {
    pub regime_id: String,
    pub regime_hash: String,
    pub snapshot_id: String,
    pub cycle_id: String,
    pub algorithm_version: String,
    pub algorithm_config_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RouteReasonCode {
    #[serde(rename = "REGIME_TREND")]
    RegimeTrend,
    #[serde(rename = "REGIME_RANGE")]
    RegimeRange,
    #[serde(rename = "REGIME_CONFLICTED")]
    RegimeConflicted,
    #[serde(rename = "HYSTERESIS_HELD")]
    HysteresisHeld,
    #[serde(rename = "CONFIRMATION_PENDING")]
    ConfirmationPending,
    #[serde(rename = "MINIMUM_DWELL_ACTIVE")]
    MinimumDwellActive,
    #[serde(rename = "LIQUIDITY_UNSAFE")]
    LiquidityUnsafe,
    #[serde(rename = "VOLATILITY_EXTREME_NEW_RISK_DISABLED")]
    VolatilityExtremeNewRiskDisabled,
    #[serde(rename = "DATA_QUALITY_UNAVAILABLE")]
    DataQualityUnavailable,
    #[serde(rename = "DATA_QUALITY_STALE")]
    DataQualityStale,
    #[serde(rename = "DATA_QUALITY_DEGRADED")]
    DataQualityDegraded,
    #[serde(rename = "UNCERTAINTY_HIGH")]
    UncertaintyHigh,
    #[serde(rename = "REQUIRED_FACT_MISSING")]
    RequiredFactMissing,
    #[serde(rename = "REGIME_IDENTITY_MISMATCH")]
    RegimeIdentityMismatch,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ReasonSource {
    #[serde(rename = "trend")]
    Trend,
    #[serde(rename = "volatility")]
    Volatility,
    #[serde(rename = "liquidity")]
    Liquidity,
    #[serde(rename = "uncertainty")]
    Uncertainty,
    #[serde(rename = "data_quality")]
    DataQuality,
    #[serde(rename = "multi_timeframe")]
    MultiTimeframe,
    #[serde(rename = "route_state")]
    RouteState,
    #[serde(rename = "identity")]
    Identity,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteReason {
    pub code: RouteReasonCode,
    pub source: ReasonSource,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub threshold: Option<DecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingRouteCandidate {
    pub route: RouteContext,
    pub confirmations: u64,
    pub first_observed_at_ms: u64,
    pub last_observed_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteState {
    pub current_route: RouteContext,
    pub dwell_observations: u64,
    pub last_switch_at_ms: u64,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub pending_candidate: Option<PendingRouteCandidate>,
}
