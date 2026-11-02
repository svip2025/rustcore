// AUTO-GENERATED FROM schemas/evidence.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub schema_version: i64,
    pub evidence_id: String,
    pub snapshot_id: String,
    pub agent_id: String,
    pub model_id: String,
    pub prompt_hash: String,
    pub created_at_ms: u64,
    pub latency_ms: u64,
    pub token_usage: TokenUsage,
    pub claims: Vec<EvidenceClaim>,
    pub abstained: bool,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub abstain_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceClaim {
    pub claim: String,
    pub stance: EvidenceClaimStance,
    pub confidence: f64,
    pub refs: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvidenceClaimStance {
    #[serde(rename = "bullish")]
    Bullish,
    #[serde(rename = "bearish")]
    Bearish,
    #[serde(rename = "neutral")]
    Neutral,
}
