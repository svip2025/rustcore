// AUTO-GENERATED FROM schemas/effective-policy-artifact.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectivePolicyArtifact {
    pub schema_version: i64,
    pub policy_version: String,
    pub compiler_version: String,
    pub normalization_version: String,
    pub policy_hash: String,
    pub runtime_policy_hash: String,
    pub horizon: Horizon,
    pub risk_style: RiskStyle,
    pub timeframes: Timeframes,
    pub risk_style_template: RiskStyleTemplate,
    pub horizon_overlay: HorizonOverlay,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub strategy_parameters: Option<StrategyParameters>,
    pub hard_limits: HardLimits,
    pub runtime_policy: RuntimePolicy,
    pub protection_plan_compiler: ProtectionPlanCompiler,
}

pub type DecimalString = String;

pub type NonNegativeDecimalString = String;

pub type Hash256 = String;

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
pub enum RiskStyle {
    #[serde(rename = "conservative")]
    Conservative,
    #[serde(rename = "balanced")]
    Balanced,
    #[serde(rename = "aggressive")]
    Aggressive,
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
#[serde(deny_unknown_fields)]
pub struct HardLimitValues {
    pub risk_per_trade_pct: NonNegativeDecimalString,
    pub max_stop_distance_pct: NonNegativeDecimalString,
    pub max_leverage: u64,
    pub max_slippage_bps: u64,
    pub max_position_notional_usdt: NonNegativeDecimalString,
    pub max_credential_notional_usdt: NonNegativeDecimalString,
    pub max_total_exposure_pct: NonNegativeDecimalString,
    pub max_pair_exposure_pct: NonNegativeDecimalString,
    pub max_concurrent_positions: u64,
    pub max_same_direction_positions: u64,
    pub max_correlated_group_exposure_pct: NonNegativeDecimalString,
    pub max_daily_loss_pct: NonNegativeDecimalString,
    pub max_drawdown_pct: NonNegativeDecimalString,
    pub max_managed_capital_usdt: NonNegativeDecimalString,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedDecimalLimit {
    pub value: NonNegativeDecimalString,
    pub source_layer: SourceLayer,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedIntegerLimit {
    pub value: u64,
    pub source_layer: SourceLayer,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SourceLayer {
    #[serde(rename = "platform_deployment")]
    PlatformDeployment,
    #[serde(rename = "venue_account")]
    VenueAccount,
    #[serde(rename = "member")]
    Member,
    #[serde(rename = "horizon")]
    Horizon,
    #[serde(rename = "risk_style")]
    RiskStyle,
    #[serde(rename = "strategy")]
    Strategy,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedLimits {
    pub risk_per_trade_pct: ResolvedDecimalLimit,
    pub max_stop_distance_pct: ResolvedDecimalLimit,
    pub max_leverage: ResolvedIntegerLimit,
    pub max_slippage_bps: ResolvedIntegerLimit,
    pub max_position_notional_usdt: ResolvedDecimalLimit,
    pub max_credential_notional_usdt: ResolvedDecimalLimit,
    pub max_total_exposure_pct: ResolvedDecimalLimit,
    pub max_pair_exposure_pct: ResolvedDecimalLimit,
    pub max_concurrent_positions: ResolvedIntegerLimit,
    pub max_same_direction_positions: ResolvedIntegerLimit,
    pub max_correlated_group_exposure_pct: ResolvedDecimalLimit,
    pub max_daily_loss_pct: ResolvedDecimalLimit,
    pub max_drawdown_pct: ResolvedDecimalLimit,
    pub max_managed_capital_usdt: ResolvedDecimalLimit,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformDeploymentLimits {
    pub source_version: String,
    pub values: HardLimitValues,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VenueAccountLimits {
    pub capability_hash: Hash256,
    pub values: HardLimitValues,
    pub trading_account_id: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemberLimits {
    pub scope: String,
    pub subject_ref_hash: Hash256,
    pub confirmation_version: String,
    pub values: HardLimitValues,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HardLimits {
    pub platform_deployment: PlatformDeploymentLimits,
    pub venue_account: VenueAccountLimits,
    pub member: MemberLimits,
    pub resolved: ResolvedLimits,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskStyleParameters {
    pub limits: HardLimitValues,
    pub min_conviction_open: DecimalString,
    pub min_conviction_open_short: DecimalString,
    pub min_stop_distance_pct: NonNegativeDecimalString,
    pub min_rr: NonNegativeDecimalString,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskStyleTemplate {
    pub id: RiskStyle,
    pub template_version: String,
    pub parameters: RiskStyleParameters,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HorizonRiskParameters {
    pub max_stop_distance_pct: NonNegativeDecimalString,
    pub max_slippage_bps: u64,
    pub min_stop_distance_pct: NonNegativeDecimalString,
    pub min_hold_minutes: u64,
    pub entry_cooldown_minutes_after_stop: u64,
    pub cost_aware_tp_multiple: NonNegativeDecimalString,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HorizonOverlay {
    pub horizon: Horizon,
    pub overlay_version: String,
    pub risk_parameters: HorizonRiskParameters,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyParameters {
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub max_slippage_bps: Option<u64>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub max_concurrent_positions: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePolicy {
    pub policy_version: String,
    pub schema_version: u64,
    pub preconditions: RuntimePreconditions,
    pub per_trade: RuntimePerTrade,
    pub portfolio: RuntimePortfolio,
    pub account: RuntimeAccount,
    pub churn: RuntimeChurn,
    pub context_filters: RuntimeContextFilters,
    pub symbol_controls: RuntimeSymbolControls,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePreconditions {
    pub require_data_quality_pass: bool,
    pub risk_budget_max_age_ms: u64,
    pub reject_expired_intent: bool,
    pub require_risk_budget: bool,
    pub require_kill_switch: bool,
    pub require_data_snapshot: bool,
    pub require_exchange_filters: bool,
    pub require_typed_protection: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePerTrade {
    pub risk_per_trade_pct: f64,
    pub min_conviction_open: f64,
    pub min_conviction_open_short: f64,
    pub protection_required: bool,
    pub min_stop_distance_pct: f64,
    pub max_stop_distance_pct: f64,
    pub min_rr: f64,
    pub fee_per_side: f64,
    pub slippage_round_trip: f64,
    pub cost_aware_tp_multiple: f64,
    pub max_leverage: u64,
    pub max_slippage_bps: u64,
    pub min_notional_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePortfolio {
    pub max_position_notional_usdt: f64,
    pub max_credential_notional_usdt: f64,
    pub max_total_exposure_pct: f64,
    pub max_pair_exposure_pct: f64,
    pub max_concurrent_positions: u64,
    pub max_same_direction_positions: u64,
    pub max_correlated_group_exposure_pct: f64,
    pub correlation_groups: BTreeMap<String, Vec<String>>,
    pub max_managed_capital_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAccount {
    pub max_daily_loss_pct: f64,
    pub max_drawdown_pct: f64,
    pub min_equity_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeChurn {
    pub min_hold_minutes: u64,
    pub min_conviction_close: f64,
    pub entry_cooldown_minutes_after_stop: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeContextFilters {
    pub enabled: bool,
    pub long_max_range_pos: f64,
    pub short_min_range_pos: f64,
    pub min_trend_score_long: i64,
    pub max_trend_score_short: i64,
    pub min_vol_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSymbolControls {
    pub disabled_for_open: Vec<String>,
    pub disabled_for_short: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionPlanCompilerParameters {
    pub mandatory_protection: bool,
    pub require_stop_loss: bool,
    pub require_take_profit: bool,
    pub fail_closed_on_missing_protection: bool,
    pub min_stop_distance_pct: NonNegativeDecimalString,
    pub max_stop_distance_pct: NonNegativeDecimalString,
    pub min_risk_reward: NonNegativeDecimalString,
    pub rounding_mode: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionPlanCompiler {
    pub rules_version: String,
    pub parameter_schema_version: String,
    pub parameters: ProtectionPlanCompilerParameters,
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
