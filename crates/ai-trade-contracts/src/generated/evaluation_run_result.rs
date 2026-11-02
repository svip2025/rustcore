// AUTO-GENERATED FROM schemas/evaluation-run-result.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationRunResult {
    pub schema_version: i64,
    pub evaluation_run_id: String,
    pub evaluation_run_hash: String,
    pub candidate_id: String,
    pub champion_id: String,
    pub dataset_assignment_id: String,
    pub datasets_evaluated: Vec<DatasetReference>,
    pub lifecycle_mapping: LifecycleMapping,
    pub safety_verdict: SafetyVerdict,
    pub quality_metrics_status: EvaluationRunResultQualityMetricsStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub baseline_quality_metrics: Option<QualityMetrics>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub challenger_quality_metrics: Option<QualityMetrics>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub quality_comparison: Option<QualityComparison>,
    pub sufficiency_verdict: SufficiencyVerdict,
    pub recommended_action: RecommendedAction,
    pub deployment_authorization: bool,
    pub evaluated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetReference {
    pub dataset_id: String,
    pub dataset_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleMapping {
    pub challenger_step: LifecycleMappingChallengerStep,
    pub stage_relation: LifecycleMappingStageRelation,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub formal_stage: Option<LifecycleMappingFormalStage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyRegression {
    pub category: SafetyRegressionCategory,
    pub reason: String,
    pub evidence_status: SafetyRegressionEvidenceStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub evidence_manifest_hash: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyVerdict {
    pub status: SafetyVerdictStatus,
    pub regressions: Vec<SafetyRegression>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricValue {
    pub status: MetricValueStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub value: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualityMetrics {
    pub net_return: MetricValue,
    pub max_drawdown: MetricValue,
    pub risk_adjusted_return: MetricValue,
    pub robustness: MetricValue,
    pub decision_quality: MetricValue,
    pub cost_latency: MetricValue,
    pub abstention_quality: MetricValue,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualityComparison {
    pub status: QualityComparisonStatus,
    pub improved_dimensions: Vec<QualityDimension>,
    pub regressed_dimensions: Vec<QualityDimension>,
    pub unavailable_dimensions: Vec<QualityDimension>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum QualityDimension {
    #[serde(rename = "net_return")]
    NetReturn,
    #[serde(rename = "max_drawdown")]
    MaxDrawdown,
    #[serde(rename = "risk_adjusted_return")]
    RiskAdjustedReturn,
    #[serde(rename = "robustness")]
    Robustness,
    #[serde(rename = "decision_quality")]
    DecisionQuality,
    #[serde(rename = "cost_latency")]
    CostLatency,
    #[serde(rename = "abstention_quality")]
    AbstentionQuality,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceGap {
    pub dimension: EvidenceGapDimension,
    pub gap_kind: EvidenceGapGapKind,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub required_minimum: Option<u64>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_count: Option<u64>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub required_status: Option<EvidenceGapRequiredStatus>,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub observed_status: Option<EvidenceGapObservedStatus>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SufficiencyVerdict {
    pub status: SufficiencyVerdictStatus,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub gaps: Option<Vec<EvidenceGap>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecommendedAction {
    pub path: RecommendedActionPath,
    pub action: RecommendedActionAction,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvaluationRunResultQualityMetricsStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "not_evaluated_safety_veto")]
    NotEvaluatedSafetyVeto,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LifecycleMappingChallengerStep {
    #[serde(rename = "research")]
    Research,
    #[serde(rename = "replay")]
    Replay,
    #[serde(rename = "shadow")]
    Shadow,
    #[serde(rename = "paper_simulation")]
    PaperSimulation,
    #[serde(rename = "engineering_demo_testnet")]
    EngineeringDemoTestnet,
    #[serde(rename = "controlled_live_small")]
    ControlledLiveSmall,
    #[serde(rename = "live")]
    Live,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LifecycleMappingStageRelation {
    #[serde(rename = "no_stage")]
    NoStage,
    #[serde(rename = "evaluation_evidence")]
    EvaluationEvidence,
    #[serde(rename = "formal_stage")]
    FormalStage,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum LifecycleMappingFormalStage {
    #[serde(rename = "paper")]
    Paper,
    #[serde(rename = "demo")]
    Demo,
    #[serde(rename = "live_small")]
    LiveSmall,
    #[serde(rename = "live")]
    Live,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SafetyRegressionCategory {
    #[serde(rename = "risk_bypass")]
    RiskBypass,
    #[serde(rename = "orphan_order")]
    OrphanOrder,
    #[serde(rename = "identity_mismatch")]
    IdentityMismatch,
    #[serde(rename = "protection_reconcile_regression")]
    ProtectionReconcileRegression,
    #[serde(rename = "contract_replay_integrity_failure")]
    ContractReplayIntegrityFailure,
    #[serde(rename = "cross_member_privacy_issue")]
    CrossMemberPrivacyIssue,
    #[serde(rename = "fabricated_or_missing_evidence")]
    FabricatedOrMissingEvidence,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SafetyRegressionEvidenceStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "fabricated")]
    Fabricated,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SafetyVerdictStatus {
    #[serde(rename = "passed")]
    Passed,
    #[serde(rename = "rejected")]
    Rejected,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum MetricValueStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum QualityComparisonStatus {
    #[serde(rename = "pareto_improvement")]
    ParetoImprovement,
    #[serde(rename = "equivalent")]
    Equivalent,
    #[serde(rename = "tradeoff")]
    Tradeoff,
    #[serde(rename = "dominated")]
    Dominated,
    #[serde(rename = "insufficient_metrics")]
    InsufficientMetrics,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvidenceGapDimension {
    #[serde(rename = "decision_cycles")]
    DecisionCycles,
    #[serde(rename = "trade_episodes")]
    TradeEpisodes,
    #[serde(rename = "trend_coverage")]
    TrendCoverage,
    #[serde(rename = "range_coverage")]
    RangeCoverage,
    #[serde(rename = "no_trade_coverage")]
    NoTradeCoverage,
    #[serde(rename = "degraded_failure_cases")]
    DegradedFailureCases,
    #[serde(rename = "forward_evidence")]
    ForwardEvidence,
    #[serde(rename = "sample_sufficiency")]
    SampleSufficiency,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvidenceGapGapKind {
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "coverage")]
    Coverage,
    #[serde(rename = "sample_sufficiency")]
    SampleSufficiency,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvidenceGapRequiredStatus {
    #[serde(rename = "covered")]
    Covered,
    #[serde(rename = "sufficient")]
    Sufficient,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EvidenceGapObservedStatus {
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "insufficient")]
    Insufficient,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SufficiencyVerdictStatus {
    #[serde(rename = "sufficient")]
    Sufficient,
    #[serde(rename = "insufficient")]
    Insufficient,
    #[serde(rename = "not_assessed_safety_veto")]
    NotAssessedSafetyVeto,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RecommendedActionPath {
    #[serde(rename = "safety")]
    Safety,
    #[serde(rename = "quality")]
    Quality,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum RecommendedActionAction {
    #[serde(rename = "immediate_stop_and_rollback")]
    ImmediateStopAndRollback,
    #[serde(rename = "stop_rollout_expansion")]
    StopRolloutExpansion,
    #[serde(rename = "return_to_review")]
    ReturnToReview,
    #[serde(rename = "require_more_evidence")]
    RequireMoreEvidence,
    #[serde(rename = "recommend_evaluation_advancement")]
    RecommendEvaluationAdvancement,
}
