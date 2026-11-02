// AUTO-GENERATED FROM schemas/experiment-lineage.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentLineage {
    pub schema_version: i64,
    pub lineage_id: String,
    pub lineage_hash: String,
    pub experiment_id: String,
    pub family_id: String,
    pub candidate_id: String,
    pub candidate_parent_status: ExperimentLineageCandidateParentStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub candidate_parent_id: Option<String>,
    pub hypothesis: String,
    pub change_summary: String,
    pub dataset_assignment_id: String,
    pub datasets_used: Vec<DatasetReference>,
    pub evaluation_result_status: ExperimentLineageEvaluationResultStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub evaluation_result: Option<EvaluationResult>,
    pub rejected_reason_status: ExperimentLineageRejectedReasonStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub rejected_reason: Option<String>,
    pub attempt_count: u64,
    pub family_lineage_count: u64,
    pub lineage_status: ExperimentLineageLineageStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetReference {
    pub dataset_id: String,
    pub dataset_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationResult {
    pub summary: String,
    pub result_manifest_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExperimentLineageCandidateParentStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "root")]
    Root,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExperimentLineageEvaluationResultStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExperimentLineageRejectedReasonStatus {
    #[serde(rename = "not_applicable")]
    NotApplicable,
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ExperimentLineageLineageStatus {
    #[serde(rename = "registered")]
    Registered,
    #[serde(rename = "evaluated")]
    Evaluated,
    #[serde(rename = "rejected")]
    Rejected,
}
