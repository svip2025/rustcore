use std::{collections::BTreeMap, fs, path::PathBuf};

use ai_trade_contracts::{
    canonical_json, content_addressed_id,
    generated::{
        AnalysisEvidence, DataSnapshot, DatasetRegistryEntry, DecisionIntent,
        EffectivePolicyArtifact, EvaluationRunResult, Evidence, ExperimentLineage,
        LearningProposal, MarketAnalysisContext, MarketRegime, MemberDecisionContext, RiskVerdict,
        StrategyRoute, TradingIntent,
    },
    sha256_hex,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn round_trip<T>(filename: &str) -> Value
where
    T: DeserializeOwned + Serialize,
{
    let raw = fs::read_to_string(fixture_dir().join(filename)).unwrap();
    let original: Value = serde_json::from_str(&raw).unwrap();
    let model: T = serde_json::from_value(original.clone())
        .unwrap_or_else(|error| panic!("{filename}: {error}"));
    let serialized = serde_json::to_value(model).unwrap();
    assert_eq!(
        canonical_json(&serialized),
        canonical_json(&original),
        "{filename}"
    );
    serialized
}

#[test]
fn rust_round_trip_matches_shared_canonical_hashes_and_ids() {
    let expected: BTreeMap<String, Value> = serde_json::from_str(
        &fs::read_to_string(fixture_dir().join("expected-hashes.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(expected.len(), 30);

    for (filename, expectation) in expected {
        let serialized = if filename.starts_with("analysis-evidence") {
            round_trip::<AnalysisEvidence>(&filename)
        } else if filename.starts_with("data-snapshot") {
            round_trip::<DataSnapshot>(&filename)
        } else if filename.starts_with("dataset-registry-entry") {
            round_trip::<DatasetRegistryEntry>(&filename)
        } else if filename.starts_with("decision-intent") {
            round_trip::<DecisionIntent>(&filename)
        } else if filename.starts_with("experiment-lineage") {
            round_trip::<ExperimentLineage>(&filename)
        } else if filename.starts_with("evaluation-run-result") {
            round_trip::<EvaluationRunResult>(&filename)
        } else if filename.starts_with("learning-proposal") {
            round_trip::<LearningProposal>(&filename)
        } else if filename.starts_with("evidence") {
            round_trip::<Evidence>(&filename)
        } else if filename.starts_with("trading-intent") {
            round_trip::<TradingIntent>(&filename)
        } else if filename.starts_with("effective-policy") {
            round_trip::<EffectivePolicyArtifact>(&filename)
        } else if filename.starts_with("market-regime") {
            round_trip::<MarketRegime>(&filename)
        } else if filename.starts_with("market-analysis-context") {
            round_trip::<MarketAnalysisContext>(&filename)
        } else if filename.starts_with("member-decision-context") {
            round_trip::<MemberDecisionContext>(&filename)
        } else if filename.starts_with("strategy-route") {
            round_trip::<StrategyRoute>(&filename)
        } else {
            round_trip::<RiskVerdict>(&filename)
        };

        let canonical = canonical_json(&serialized);
        assert_eq!(
            sha256_hex(&canonical),
            expectation["canonical_sha256"].as_str().unwrap(),
            "{filename}"
        );
        let id_field = expectation["id_field"].as_str().unwrap();
        assert_eq!(
            serialized[id_field].as_str().unwrap(),
            expectation["contract_id"].as_str().unwrap(),
            "{filename}"
        );

        if filename.starts_with("data-snapshot") {
            assert_eq!(
                content_addressed_id("snap_", &serialized, id_field),
                serialized[id_field].as_str().unwrap(),
                "{filename}"
            );
        }
    }
}

#[test]
fn rust_rejects_missing_required_field() {
    let raw = fs::read_to_string(fixture_dir().join("data-snapshot-minimal.json")).unwrap();
    let mut malformed: Value = serde_json::from_str(&raw).unwrap();
    malformed.as_object_mut().unwrap().remove("schema_version");
    let error = serde_json::from_value::<DataSnapshot>(malformed).unwrap_err();
    assert!(error.to_string().contains("schema_version"));
}
