// AUTO-GENERATED FROM schemas/market-regime.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRegime {
    pub schema_version: i64,
    pub regime_id: String,
    pub regime_hash: String,
    pub snapshot_id: String,
    pub cycle_id: String,
    pub snapshot_as_of_ms: u64,
    pub classified_at_ms: u64,
    pub symbol_key: String,
    pub market: String,
    pub horizon: Horizon,
    pub timeframes: Timeframes,
    pub algorithm_version: String,
    pub ruleset_version: String,
    pub parameters_version: String,
    pub algorithm_config_hash: String,
    pub data_quality: RegimeDataQuality,
    pub trend: TrendDimension,
    pub volatility: VolatilityDimension,
    pub liquidity: LiquidityDimension,
    pub uncertainty: UncertaintyDimension,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub multi_timeframe_relationship: Option<MultiTimeframeRelationship>,
}

pub type DecimalString = String;

pub type NonNegativeDecimalString = String;

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
#[serde(deny_unknown_fields)]
pub struct Timeframes {
    pub entry: TimeframesEntry,
    pub primary: TimeframesPrimary,
    pub context: TimeframesContext,
    pub background: TimeframesBackground,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum MissingRequiredFact {
    #[serde(rename = "trend_score")]
    TrendScore,
    #[serde(rename = "trend_confidence")]
    TrendConfidence,
    #[serde(rename = "realized_volatility_pct")]
    RealizedVolatilityPct,
    #[serde(rename = "spread_bps")]
    SpreadBps,
    #[serde(rename = "depth_ratio")]
    DepthRatio,
    #[serde(rename = "entry_direction")]
    EntryDirection,
    #[serde(rename = "primary_direction")]
    PrimaryDirection,
    #[serde(rename = "context_direction")]
    ContextDirection,
    #[serde(rename = "background_direction")]
    BackgroundDirection,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegimeDataQuality {
    pub status: RegimeDataQualityStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub snapshot_quality: Option<crate::generated::data_snapshot::DataQuality>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub snapshot_age_ms: Option<u64>,
    pub max_snapshot_age_ms: u64,
    pub facts_complete: bool,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub missing_required_facts: Option<Vec<MissingRequiredFact>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TrendDirection {
    #[serde(rename = "up")]
    Up,
    #[serde(rename = "down")]
    Down,
    #[serde(rename = "neutral")]
    Neutral,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrendDimension {
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub direction: Option<TrendDirection>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub strength: Option<NonNegativeDecimalString>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub confidence: Option<NonNegativeDecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum VolatilityState {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "extreme")]
    Extreme,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolatilityDimension {
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub state: Option<VolatilityState>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub realized_pct: Option<NonNegativeDecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LiquidityState {
    #[serde(rename = "healthy")]
    Healthy,
    #[serde(rename = "degraded")]
    Degraded,
    #[serde(rename = "unsafe")]
    Unsafe,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiquidityDimension {
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub state: Option<LiquidityState>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub spread_bps: Option<NonNegativeDecimalString>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub depth_ratio: Option<NonNegativeDecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum UncertaintyState {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "high")]
    High,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UncertaintyDimension {
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub state: Option<UncertaintyState>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub confidence: Option<NonNegativeDecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum MultiTimeframeRelationship {
    #[serde(rename = "aligned")]
    Aligned,
    #[serde(rename = "pullback")]
    Pullback,
    #[serde(rename = "transition")]
    Transition,
    #[serde(rename = "conflicted")]
    Conflicted,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TimeframesEntry {
    #[serde(rename = "5m")]
    Value5m,
    #[serde(rename = "15m")]
    Value15m,
    #[serde(rename = "1h")]
    Value1h,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TimeframesPrimary {
    #[serde(rename = "15m")]
    Value15m,
    #[serde(rename = "1h")]
    Value1h,
    #[serde(rename = "4h")]
    Value4h,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TimeframesContext {
    #[serde(rename = "1h")]
    Value1h,
    #[serde(rename = "4h")]
    Value4h,
    #[serde(rename = "1d")]
    Value1d,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TimeframesBackground {
    #[serde(rename = "4h")]
    Value4h,
    #[serde(rename = "1d")]
    Value1d,
    #[serde(rename = "1w")]
    Value1w,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RegimeDataQualityStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "unavailable")]
    Unavailable,
}
