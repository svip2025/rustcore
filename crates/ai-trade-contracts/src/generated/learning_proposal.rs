// AUTO-GENERATED FROM schemas/learning-proposal.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LearningProposal {
    pub schema_version: i64,
    pub proposal_id: String,
    pub proposal_hash: String,
    pub proposal_version: String,
    pub evidence_scope: EvidenceScope,
    pub affected_component: String,
    pub observed_pattern: String,
    pub hypothesis: String,
    pub proposed_change: String,
    pub expected_benefit: String,
    pub known_risks: Vec<String>,
    pub author: ProposalAuthor,
    pub confidence_status: LearningProposalConfidenceStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub confidence: Option<f64>,
    pub deployment_authorization: bool,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceScope {
    pub range_start_ms: u64,
    pub range_end_ms: u64,
    pub episode_set_hash: String,
    pub episode_count: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalAuthor {
    pub model: String,
    pub version: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LearningProposalConfidenceStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unavailable")]
    Unavailable,
}
