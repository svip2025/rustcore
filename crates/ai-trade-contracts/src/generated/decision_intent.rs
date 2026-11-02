// AUTO-GENERATED FROM schemas/decision-intent.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionIntent {
    pub schema_version: i64,
    pub intent_id: String,
    pub intent_hash: String,
    pub created_at_ms: u64,
    pub decision_agent_id: String,
    pub model_id: String,
    pub prompt_hash: String,
    pub member_context_id: String,
    pub member_context_hash: String,
    pub analysis_evidence_id: String,
    pub analysis_evidence_hash: String,
    pub logical_action: DecisionIntentLogicalAction,
    pub confidence: DecisionConfidence,
    pub thesis: DecisionStatement,
    pub invalidation: DecisionStatement,
    pub referenced_evidence: Vec<ReferencedEvidence>,
    pub expected_horizon: crate::generated::market_regime::Horizon,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub abstain_reason: Option<String>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub entry_preference: Option<DecisionIntentEntryPreference>,
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
pub struct DecisionConfidence {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionStatement {
    pub status: AssessmentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencedEvidence {
    pub analysis_evidence_id: String,
    pub analysis_evidence_hash: String,
    pub evidence_ref: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DecisionIntentLogicalAction {
    #[serde(rename = "OPEN_LONG")]
    OpenLong,
    #[serde(rename = "OPEN_SHORT")]
    OpenShort,
    #[serde(rename = "CLOSE")]
    Close,
    #[serde(rename = "HOLD")]
    Hold,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DecisionIntentEntryPreference {
    #[serde(rename = "market")]
    Market,
    #[serde(rename = "pullback")]
    Pullback,
    #[serde(rename = "breakout_wait")]
    BreakoutWait,
}
