// AUTO-GENERATED FROM schemas/market-analysis-context.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketAnalysisContext {
    pub schema_version: i64,
    pub context_id: String,
    pub context_hash: String,
    pub created_at_ms: u64,
    pub cycle_id: String,
    pub snapshot_identity: SnapshotIdentity,
    pub horizon: crate::generated::market_regime::Horizon,
    pub strategy_context: crate::generated::strategy_route::RouteContext,
    pub regime: crate::generated::market_regime::MarketRegime,
    pub strategy_route: crate::generated::strategy_route::StrategyRoute,
    pub public_facts: Vec<PublicMarketFact>,
    pub data_quality: crate::generated::market_regime::RegimeDataQuality,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentity {
    pub snapshot_id: String,
    pub created_at_ms: u64,
    pub symbol_key: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum PublicFactStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "degraded")]
    Degraded,
    #[serde(rename = "unavailable")]
    Unavailable,
}

pub type PublicFactValue = Option<serde_json::Value>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicMarketFact {
    pub fact_path: String,
    pub value: PublicFactValue,
    pub status: PublicFactStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub source_ref: Option<String>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_at_ms: Option<u64>,
}
