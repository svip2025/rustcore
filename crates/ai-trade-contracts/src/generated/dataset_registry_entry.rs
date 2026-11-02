// AUTO-GENERATED FROM schemas/dataset-registry-entry.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetRegistryEntry {
    pub schema_version: i64,
    pub dataset_id: String,
    pub dataset_hash: String,
    pub dataset_version: String,
    pub dataset_type: DatasetType,
    pub manifest_hash: String,
    pub created_at_ms: u64,
    pub created_by: String,
    pub coverage_status: CoverageStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub market_episode_coverage: Option<MarketEpisodeCoverage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DatasetType {
    #[serde(rename = "research")]
    Research,
    #[serde(rename = "promotion_evaluation")]
    PromotionEvaluation,
    #[serde(rename = "locked_holdout")]
    LockedHoldout,
    #[serde(rename = "frozen_benchmark")]
    FrozenBenchmark,
    #[serde(rename = "rolling_evaluation")]
    RollingEvaluation,
    #[serde(rename = "live_out_of_sample")]
    LiveOutOfSample,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum CoverageStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TrendDirection {
    #[serde(rename = "up")]
    Up,
    #[serde(rename = "down")]
    Down,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum VolatilityBand {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "extreme")]
    Extreme,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DataQualityScenario {
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "degraded")]
    Degraded,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEpisodeCoverage {
    pub trend_directions: Vec<TrendDirection>,
    pub range: bool,
    pub volatility_bands: Vec<VolatilityBand>,
    pub liquidity_degraded: bool,
    pub transition: bool,
    pub event_shock: bool,
    pub data_quality_scenarios: Vec<DataQualityScenario>,
    pub supported_horizons: Vec<String>,
    pub supported_assets: Vec<String>,
}
