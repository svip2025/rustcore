// AUTO-GENERATED FROM schemas/analysis-evidence.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnalysisEvidence {
    pub schema_version: i64,
    pub evidence_id: String,
    pub evidence_hash: String,
    pub market_context_id: String,
    pub market_context_hash: String,
    pub created_at_ms: u64,
    pub analysis_agent_id: String,
    pub model_id: String,
    pub prompt_hash: String,
    pub market_bias: MarketBiasAssessment,
    pub trend_assessment: TextAssessment,
    pub volatility_assessment: TextAssessment,
    pub derivatives_assessment: TextAssessment,
    pub liquidity_assessment: TextAssessment,
    pub multi_timeframe_interpretation: TextAssessment,
    pub risks: SignalList,
    pub conflicting_signals: SignalList,
    pub confidence: ConfidenceAssessment,
    pub opportunity_type: OpportunityAssessment,
    pub evidence_refs: Vec<EvidenceReference>,
    pub ui_summary: String,
    pub status: AnalysisEvidenceStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub abstain_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum AssessmentStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TextAssessment {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketBiasAssessment {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<MarketBiasAssessmentValue>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SignalList {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub items: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfidenceAssessment {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpportunityAssessment {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<OpportunityAssessmentValue>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceReference {
    pub context_id: String,
    pub fact_path: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum AnalysisEvidenceStatus {
    #[serde(rename = "valid")]
    Valid,
    #[serde(rename = "abstain")]
    Abstain,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum MarketBiasAssessmentValue {
    #[serde(rename = "bullish")]
    Bullish,
    #[serde(rename = "bearish")]
    Bearish,
    #[serde(rename = "neutral")]
    Neutral,
    #[serde(rename = "avoid")]
    Avoid,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum OpportunityAssessmentValue {
    #[serde(rename = "trend_continuation")]
    TrendContinuation,
    #[serde(rename = "pullback")]
    Pullback,
    #[serde(rename = "range_reversion")]
    RangeReversion,
    #[serde(rename = "breakout_watch")]
    BreakoutWatch,
    #[serde(rename = "none")]
    None,
}
