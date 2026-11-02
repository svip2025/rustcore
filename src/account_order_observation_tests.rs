use super::*;
use crate::account_order_read_tests::Fixture;

fn truth() -> crate::kill_switch::ReconcileTruth {
    crate::kill_switch::ReconcileTruth {
        has_diff: false,
        all_positions_zero: true,
        all_open_orders_empty: true,
        all_open_algo_orders_empty: true,
        local_position_zero_diff: true,
    }
}
fn complete(store: &Store, credential: &str, started: i64, rows: Value) -> Observation {
    let mut observation = Observation::begin(store, credential, "full_account", started);
    observation.ordinary(&rows, started, started + 1);
    observation.algo(&serde_json::json!([]), started + 1, started + 2);
    observation
}
#[tokio::test]
async fn account_order_observation_freshness_boundaries_and_pool_scope() {
    let f = Fixture::new().await;
    let other = Fixture::new().await;
    complete(&f.store, "7", 100000, serde_json::json!([])).finish(truth(), 100002);
    assert_eq!(
        read(&f.store, "7", 1000, 130000).availability,
        p::AccountOrderAvailability::AoaAvailable as i32
    );
    assert_eq!(
        read(&f.store, "7", 1000, 130001).reason_code,
        "stale_measurement"
    );
    assert_eq!(
        read(&f.store, "7", 1000, 99999).reason_code,
        "invalid_observation_time"
    );
    assert_eq!(
        read(&other.store, "7", 1000, 100003).reason_code,
        "not_measured"
    );
    complete(&f.store, "7", 100000, serde_json::json!([])).finish(truth(), 130001);
    assert_eq!(
        read(&f.store, "7", 1000, 130002).reason_code,
        "source_inconsistent"
    );
    assert_eq!(
        read(
            &f.store,
            "paper:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1000,
            130002
        )
        .reason_code,
        "simulation_local_only"
    );
    f.cleanup().await;
    other.cleanup().await;
}
#[tokio::test]
async fn account_order_observation_failure_in_progress_and_late_completion_do_not_revive_proof() {
    let f = Fixture::new().await;
    complete(&f.store, "7", 100000, serde_json::json!([])).finish(truth(), 100002);
    let older = complete(&f.store, "7", 100003, serde_json::json!([]));
    assert_eq!(
        read(&f.store, "7", 1000, 100006).reason_code,
        "observation_in_progress"
    );
    let newer = Observation::begin(&f.store, "7", "symbols", 100007);
    let newer_id = newer.attempt.observation_id.clone();
    drop(newer);
    older.finish(truth(), 100010);
    let result = read(&f.store, "7", 1000, 100011);
    assert_eq!(result.reason_code, "latest_attempt_failed");
    assert_eq!(result.latest_attempt.unwrap().observation_id, newer_id);
    assert_eq!(
        result.availability,
        p::AccountOrderAvailability::AoaUnknown as i32
    );
    assert_eq!(result.source_as_of_ms, Some(100000));
    assert!(
        result.ordinary.is_some(),
        "last full rows remain only for stale display"
    );
    not_observed(&f.store, "7", 100012);
    assert_eq!(
        read(&f.store, "7", 1000, 100013).reason_code,
        "not_observed"
    );
    f.cleanup().await;
}
#[tokio::test]
async fn account_order_observation_overflow_malformed_and_exact_typed_fields() {
    let f = Fixture::new().await;
    let values: Vec<Value> = (1..=1001)
        .map(|i| serde_json::json!({"orderId":i,"symbol":"BTCUSDT","status":"NEW"}))
        .collect();
    complete(&f.store, "7", 100000, Value::Array(values)).finish(truth(), 100002);
    let result = read(&f.store, "7", 1000, 100003);
    assert_eq!(result.reason_code, "overflow");
    let overflow_section = result.ordinary.unwrap();
    assert_eq!(overflow_section.rows.len(), 1000);
    assert!(overflow_section.meta.as_ref().unwrap().overflow);
    assert!(overflow_section.meta.unwrap().total_count.is_none());
    complete(&f.store,"7",100000,serde_json::json!([{ "orderId":1,"symbol":"BTCUSDT","status":"NEW"},{"orderId":2,"symbol":"ETHUSDT","status":"NEW"}])).finish(truth(),100002);
    assert_eq!(read(&f.store, "7", 1, 100003).reason_code, "overflow");
    assert_eq!(
        read(&f.store, "7", 1000, 100003).availability,
        p::AccountOrderAvailability::AoaAvailable as i32,
        "reader truncation cannot mutate shared cache"
    );
    complete(&f.store,"7",100000,serde_json::json!([{"orderId":1,"symbol":"BTCUSDT","status":"NEW"},{"orderId":1,"symbol":"BTCUSDT","status":"NEW"}])).finish(truth(),100002);
    assert_eq!(
        read(&f.store, "7", 1000, 100003).reason_code,
        "source_inconsistent"
    );
    complete(&f.store, "7", 100000, serde_json::json!({"not":"array"})).finish(truth(), 100002);
    assert_eq!(
        read(&f.store, "7", 1000, 100003).reason_code,
        "source_inconsistent"
    );
    let fields = section(
        &serde_json::json!([{"algoId":9007199254740993u64,"symbol":"BTCUSDT","algoStatus":"NEW","triggerPrice":".","reduceOnly":"TRUE","closePosition":1,"positionSide":"BOTH"}]),
        true,
        100000,
        100001,
    );
    assert_eq!(fields.rows[0].algo_id.as_deref(), Some("9007199254740993"));
    assert!(fields.rows[0].trigger_price.is_none());
    assert!(fields.rows[0].reduce_only.is_none());
    assert!(fields.rows[0].close_position.is_none());
    f.cleanup().await;
}

#[tokio::test]
async fn account_order_observation_missing_algo_is_unknown() {
    let f = Fixture::new().await;
    let mut missing = Observation::begin(&f.store, "7", "full_account", 100000);
    missing.ordinary(&serde_json::json!([]), 100000, 100001);
    missing.finish(truth(), 100002);
    let result = read(&f.store, "7", 1000, 100003);
    assert_eq!(
        result.availability,
        p::AccountOrderAvailability::AoaUnknown as i32
    );
    assert_eq!(result.reason_code, "not_measured");
    let mut malformed = Observation::begin(&f.store, "7", "full_account", 100004);
    malformed.ordinary(&serde_json::json!([]), 100004, 100005);
    malformed.algo(&serde_json::json!({"not":"an array"}), 100005, 100006);
    malformed.finish(truth(), 100007);
    let result = read(&f.store, "7", 1000, 100008);
    assert_eq!(
        result.availability,
        p::AccountOrderAvailability::AoaUnknown as i32
    );
    assert_eq!(result.reason_code, "source_inconsistent");
    f.cleanup().await;
}
