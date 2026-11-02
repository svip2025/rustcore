use super::*;
use crate::account_order_read_tests::Fixture;
use crate::proto_v2 as p;
use crate::rate_limiter::TokenBucketConfig;

fn client(
    credential: &str,
    orders: serde_json::Value,
    algos: serde_json::Value,
) -> BinanceFuturesClient {
    BinanceFuturesClient::new_with_mock_responses(credential,vec![
        (200,orders.to_string()),(200,algos.to_string()),(200,"[]".into()),
        (200,r#"[{"asset":"USDT","balance":"5000","availableBalance":"5000","crossWalletBalance":"5000","crossUnPnl":"0"}]"#.into())])
}
async fn round(
    f: &Fixture,
    client: &BinanceFuturesClient,
    coverage: Coverage,
) -> anyhow::Result<CoveredReconcileTruth> {
    reconcile_binance_futures_truth_with_client_and_coverage_at_with_ws(
        "7",
        f.store.clone(),
        CredentialRateLimiter::new(TokenBucketConfig::for_test(10000.0, 10000.0)),
        client,
        coverage,
        None,
        now_ms,
    )
    .await
}
#[tokio::test]
async fn account_order_observation_production_reconcile_to_rpc_no_extra_requests() {
    let f = Fixture::new().await;
    let client = client(
        "7",
        serde_json::json!([{"orderId":31,"clientOrderId":"external-order","symbol":"BTCUSDT","status":"NEW","side":"BUY","type":"LIMIT","origQty":"0.123456789123456789","price":"45000","updateTime":1788000000123i64}]),
        serde_json::json!([{"algoId":9007199254740993i64,"clientAlgoId":"external-algo","algoStatus":"NEW","symbol":"BTCUSDT","side":"SELL","orderType":"STOP_MARKET","quantity":"1.25","triggerPrice":"48000.125","reduceOnly":true,"closePosition":"false","positionSide":"LONG","updateTime":1788000000123i64}]),
    );
    let truth = round(&f, &client, Coverage::Full).await.unwrap().truth;
    let requests = client.recorded_requests();
    assert_eq!(requests.len(), 4, "observer must not add venue requests");
    let before = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    let r = f.rpc("7", 0, None).await.unwrap();
    let venue = r.venue.as_ref().unwrap();
    assert_eq!(
        venue.availability,
        p::AccountOrderAvailability::AoaAvailable as i32,
        "production producer is connected"
    );
    assert_eq!(venue.source_as_of_ms, venue.capture_started_at_ms);
    assert!(venue.source_as_of_ms.unwrap() <= r.observed_at_ms);
    let ordinary = &venue.ordinary.as_ref().unwrap().rows;
    assert_eq!(ordinary.len(), 1);
    assert_eq!(
        ordinary[0].quantity.as_deref(),
        Some("0.123456789123456789")
    );
    assert_eq!(ordinary[0].order_id.as_deref(), Some("31"));
    assert!(
        r.local
            .as_ref()
            .unwrap()
            .orders
            .as_ref()
            .unwrap()
            .rows
            .is_empty(),
        "unmapped venue rows survive"
    );
    let algo = &venue.algo.as_ref().unwrap().rows[0];
    assert_eq!(algo.algo_id.as_deref(), Some("9007199254740993"));
    assert_eq!(algo.trigger_price.as_deref(), Some("48000.125"));
    assert_eq!(
        (algo.reduce_only, algo.close_position),
        (Some(true), Some(false))
    );
    assert_eq!(algo.position_side.as_deref(), Some("LONG"));
    assert_eq!(
        venue.reconcile_truth.as_ref().unwrap().has_diff,
        truth.has_diff
    );
    assert_eq!(venue.latest_attempt.as_ref().unwrap().status, "succeeded");
    assert_eq!(
        venue.latest_attempt.as_ref().unwrap().coverage,
        "full_account"
    );
    assert_eq!(
        f.rpc("8", 0, None)
            .await
            .unwrap()
            .venue
            .unwrap()
            .reason_code,
        "not_measured"
    );
    assert_eq!(client.recorded_requests(), requests);
    let after = sqlx::query("SELECT total_changes() AS n")
        .fetch_one(f.store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(before, after, "authenticated read changes no SQLite row");
    if let Ok(path) = std::env::var("ACCOUNT_ORDER_VENUE_WIRE_VECTOR") {
        use prost::Message;
        std::fs::write(path, r.encode_to_vec()).unwrap();
    }
    f.cleanup().await;
}

fn retained_client() -> BinanceFuturesClient {
    client(
        "7",
        serde_json::json!([{"orderId":77,"clientOrderId":"retained-order","symbol":"BTCUSDT","status":"NEW"}]),
        serde_json::json!([{"algoId":88,"clientAlgoId":"retained-algo","algoStatus":"NEW","symbol":"BTCUSDT"}]),
    )
}
fn assert_stale_rows_and_time(
    result: &p::VenueAccountOrderObservation,
    original: &p::VenueAccountOrderObservation,
) {
    assert_eq!(
        result.availability,
        p::AccountOrderAvailability::AoaUnknown as i32
    );
    assert_eq!(result.observation_id, original.observation_id);
    assert_eq!(result.source_as_of_ms, original.source_as_of_ms);
    assert_eq!(result.capture_started_at_ms, original.capture_started_at_ms);
    assert_eq!(
        result.capture_completed_at_ms,
        original.capture_completed_at_ms
    );
    assert_eq!(
        result.ordinary.as_ref().unwrap().rows,
        original.ordinary.as_ref().unwrap().rows
    );
    assert_eq!(
        result.algo.as_ref().unwrap().rows,
        original.algo.as_ref().unwrap().rows
    );
    assert_eq!(
        result.ordinary.as_ref().unwrap().rows[0]
            .order_id
            .as_deref(),
        Some("77")
    );
    assert_eq!(
        result.algo.as_ref().unwrap().rows[0].algo_id.as_deref(),
        Some("88")
    );
    assert_ne!(
        result.latest_attempt.as_ref().unwrap().observation_id,
        original.observation_id.as_deref().unwrap()
    );
}
#[tokio::test]
async fn account_order_observation_whole_round_failure_and_narrowed_replace_full() {
    let f = Fixture::new().await;
    round(&f, &retained_client(), Coverage::Full).await.unwrap();
    let original = f.rpc("7", 0, None).await.unwrap().venue.unwrap();
    assert_eq!(
        original.availability,
        p::AccountOrderAvailability::AoaAvailable as i32
    );
    let empty = serde_json::json!([]);
    let narrowed = client("7", empty.clone(), empty);
    round(&f, &narrowed, Coverage::Symbols(vec!["BTCUSDT".into()]))
        .await
        .unwrap();
    let result = f.rpc("7", 0, None).await.unwrap().venue.unwrap();
    assert_eq!(result.reason_code, "partial_coverage");
    assert_eq!(result.latest_attempt.as_ref().unwrap().coverage, "symbols");
    assert_stale_rows_and_time(&result, &original);
    let failed = BinanceFuturesClient::new_with_mock_responses(
        "7",
        vec![
            (200, "[]".into()),
            (200, "[]".into()),
            (200, "{}".into()),
            (200, "[]".into()),
        ],
    );
    assert!(round(&f, &failed, Coverage::Full).await.is_err());
    let result = f.rpc("7", 0, None).await.unwrap().venue.unwrap();
    assert_eq!(result.reason_code, "latest_attempt_failed");
    assert_eq!(result.latest_attempt.as_ref().unwrap().status, "failed");
    assert_stale_rows_and_time(&result, &original);
    f.cleanup().await;
}
#[tokio::test]
async fn account_order_observation_stream_only_invalidates_full_without_outbound() {
    let f = Fixture::new().await;
    round(&f, &retained_client(), Coverage::Full).await.unwrap();
    let original = f.rpc("7", 0, None).await.unwrap().venue.unwrap();
    assert_eq!(
        original.availability,
        p::AccountOrderAvailability::AoaAvailable as i32
    );
    sqlx::query("INSERT INTO fill_stream_state(credential_id,generation,ready,phase,updated_at_ms) VALUES('7',1,1,'ready',?)").bind(now_ms()).execute(f.store.pool()).await.unwrap();
    let no_requests = BinanceFuturesClient::new_with_mock_responses("7", Vec::new());
    run_reconcile_round(
        &ReconcileExchangeSource::Stub(no_requests.clone()),
        "7",
        f.store.clone(),
        CredentialRateLimiter::new(TokenBucketConfig::for_test(10000.0, 10000.0)),
        BinanceKeys {
            api_key: "unused".into(),
            api_secret: "unused".into(),
        },
    )
    .await
    .unwrap();
    assert!(no_requests.recorded_requests().is_empty());
    let result = f.rpc("7", 0, None).await.unwrap().venue.unwrap();
    assert_eq!(result.reason_code, "not_observed");
    assert_eq!(
        result.latest_attempt.as_ref().unwrap().coverage,
        "stream_only"
    );
    assert_stale_rows_and_time(&result, &original);
    f.cleanup().await;
}
