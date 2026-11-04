use super::*;

async fn fixture() -> (Store, std::path::PathBuf) {
    sqlx::any::install_default_drivers();
    let path =
        std::env::temp_dir().join(format!("rustcore-round2-funding-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    (store, path)
}
fn request() -> p::GetFundingHistoryRequest {
    let end = crate::svc::now_ms() - 1_000;
    p::GetFundingHistoryRequest {
        credential_id: "round2-funding-fixture".into(),
        symbol: "BTCUSDT".into(),
        start_time_ms: end - 60_000,
        end_time_ms: end,
        schema_version: 1,
    }
}
fn income(id: u64, time: i64, amount: &str) -> serde_json::Value {
    serde_json::json!({"symbol":"BTCUSDT","asset":"USDT","incomeType":"FUNDING_FEE","income":amount,"time":time,"tranId":id})
}
fn client(rows: serde_json::Value) -> BinanceFuturesClient {
    BinanceFuturesClient::new_with_mock_responses(
        "round2-funding-fixture",
        vec![(200, rows.to_string())],
    )
}
async fn count(store: &Store, table: &str) -> i64 {
    sqlx::query(&format!("SELECT COUNT(*) n FROM {table}"))
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("n")
}

#[tokio::test]
async fn round2_funding_exact_idempotent_restart_and_no_double_equity_credit() {
    let (store, path) = fixture().await;
    let req = request();
    sqlx::query("INSERT INTO engine_balances(credential_id,exchange,market_kind,asset,wallet_balance,available_balance,updated_at_ms) VALUES (?,'binance','futures','USDT','997.87499999','997.87499999',?)")
        .bind(&req.credential_id).bind(req.end_time_ms).execute(store.pool()).await.unwrap();
    let rows = serde_json::json!([
        income(1, req.start_time_ms, "-2.12500001"),
        income(2, req.end_time_ms, "0.0000000001")
    ]);
    let first = refresh(&store, &client(rows.clone()), &req).await.unwrap();
    assert_eq!(first.total_funding_usdt, "-2.1250000099");
    assert_eq!(first.entries.len(), 2);
    let repeated = refresh(&store, &client(rows.clone()), &req).await.unwrap();
    assert_eq!(first.entries, repeated.entries);
    assert_eq!(count(&store, "exchange_funding_ledger").await, 2);
    let account = crate::proto_v2::engine_execution_server::EngineExecution::get_account_state(
        &crate::svc_v2::EngineExecutionV2Service::new(store.clone()),
        tonic::Request::new(p::GetAccountStateRequest {
            credential_id: req.credential_id.clone(),
            bot_uid: String::new(),
            schema_version: 1,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        account.equity_usdt.as_deref(),
        Some("997.87499999"),
        "wallet already includes funding; never credit again"
    );
    store.pool().close().await;
    let restarted = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    refresh(&restarted, &client(rows), &req).await.unwrap();
    assert_eq!(count(&restarted, "exchange_funding_ledger").await, 2);
    restarted.pool().close().await;
    let _ = std::fs::remove_file(path);
}
#[tokio::test]
async fn round2_funding_partial_unknown_conflict_and_transaction_rollback() {
    let (store, path) = fixture().await;
    let req = request();
    // First complete observation persists one identity.
    refresh(
        &store,
        &client(serde_json::json!([income(2, req.end_time_ms, "1")])),
        &req,
    )
    .await
    .unwrap();
    let conflict = refresh(
        &store,
        &client(serde_json::json!([
            income(1, req.start_time_ms, "3"),
            income(2, req.end_time_ms, "2")
        ])),
        &req,
    )
    .await;
    assert!(conflict.is_err());
    assert_eq!(
        count(&store, "exchange_funding_ledger").await,
        1,
        "earlier insert must roll back on conflict"
    );
    assert_eq!(count(&store, "exchange_funding_observations").await, 1);
    let failed = BinanceFuturesClient::new_with_mock_responses(
        "round2-funding-fixture",
        vec![(503, "{}".into())],
    );
    assert!(refresh(&store, &failed, &req).await.is_err());
    assert!(refresh(
        &store,
        &client(serde_json::json!([income(4, req.start_time_ms, "NaN")])),
        &req
    )
    .await
    .is_err());
    assert_eq!(count(&store, "exchange_funding_observations").await, 1);
    // A capped page is not complete; subsequent read fails and nothing imports.
    let full = vec![income(9, req.start_time_ms, "1"); LIMIT];
    let partial = BinanceFuturesClient::new_with_mock_responses(
        "round2-funding-fixture",
        vec![
            (200, serde_json::json!(full).to_string()),
            (503, "{}".into()),
        ],
    );
    assert!(refresh(&store, &partial, &req).await.is_err());
    assert_eq!(count(&store, "exchange_funding_ledger").await, 1);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round2_funding_mysql_duplicate_concurrent_and_atomic_marker() {
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "r2funding",
        "round2_funding_mysql_duplicate_concurrent_and_atomic_marker",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let req = request();
    let rows = serde_json::json!([
        income(1, req.start_time_ms, "-0.125"),
        income(2, req.end_time_ms, "0.025")
    ]);
    let a = client(rows.clone());
    let b = client(rows.clone());
    let (left, right) = tokio::join!(refresh(&store, &a, &req), refresh(&store, &b, &req));
    assert_eq!(left.unwrap().total_funding_usdt, "-0.1");
    assert_eq!(right.unwrap().total_funding_usdt, "-0.1");
    assert_eq!(count(&store, "exchange_funding_ledger").await, 2);
    assert_eq!(count(&store, "exchange_funding_observations").await, 2);
    let conflict = client(serde_json::json!([
        income(0, req.start_time_ms, "5"),
        income(2, req.end_time_ms, "2")
    ]));
    assert!(refresh(&store, &conflict, &req).await.is_err());
    assert_eq!(count(&store, "exchange_funding_ledger").await, 2);
    assert_eq!(count(&store, "exchange_funding_observations").await, 2);
    store.pool().close().await;
    scratch.cleanup().await;
}
#[test]
fn round2_funding_rejects_wrong_window_symbol_and_schema() {
    let now = crate::svc::now_ms();
    let mut req = request();
    assert!(validate_window(&req, now).is_ok());
    req.end_time_ms = now + 1;
    assert!(validate_window(&req, now).is_err());
    req = request();
    req.start_time_ms = now - MAX_WINDOW_MS - 10_000;
    assert!(validate_window(&req, now).is_err());
    req = request();
    req.symbol = "ETHUSDT".into();
    assert!(validate_window(&req, now).is_err());
    req = request();
    req.schema_version = 2;
    assert!(validate_window(&req, now).is_err());
}

#[tokio::test]
async fn round2_funding_capped_pages_split_inclusive_windows_and_reject_lost_rows() {
    let (store, path) = fixture().await;
    let req = request();
    let middle = req.start_time_ms + (req.end_time_ms - req.start_time_ms) / 2;
    let row_a = income(1001, req.start_time_ms, "-0.5");
    let row_b = income(1002, req.end_time_ms, "0.25");
    let mut capped = vec![row_a.clone(); LIMIT];
    capped[LIMIT - 1] = row_b.clone();
    let script = BinanceFuturesClient::new_with_mock_responses(
        "round2-funding-fixture",
        vec![
            (200, serde_json::json!(capped).to_string()),
            (200, serde_json::json!([row_a.clone()]).to_string()),
            (200, serde_json::json!([row_b]).to_string()),
        ],
    );
    let result = refresh(&store, &script, &req).await.unwrap();
    assert_eq!(result.total_funding_usdt, "-0.25");
    assert_eq!(result.entries.len(), 2);
    let sent = script.recorded_requests();
    assert_eq!(sent.len(), 3);
    let lower = url::Url::parse(sent[1].strip_prefix("GET ").unwrap()).unwrap();
    let upper = url::Url::parse(sent[2].strip_prefix("GET ").unwrap()).unwrap();
    assert_eq!(
        lower.query_pairs().find(|(k, _)| k == "endTime").unwrap().1,
        middle.to_string()
    );
    assert_eq!(
        upper
            .query_pairs()
            .find(|(k, _)| k == "startTime")
            .unwrap()
            .1,
        (middle + 1).to_string()
    );
    assert!(!sent.iter().any(|r| r.contains("page=")));
    let script = BinanceFuturesClient::new_with_mock_responses(
        "round2-funding-fixture",
        vec![
            (200, serde_json::json!(capped).to_string()),
            (200, serde_json::json!([row_a]).to_string()),
            (200, "[]".into()),
        ],
    );
    assert!(
        refresh(&store, &script, &req).await.is_err(),
        "known parent-row disappearing from subranges cannot prove complete"
    );
    assert_eq!(count(&store, "exchange_funding_observations").await, 1);
    let mut instant = req.clone();
    instant.start_time_ms = instant.end_time_ms;
    let saturated = client(serde_json::json!(vec![
        income(7, req.end_time_ms, "1");
        LIMIT
    ]));
    assert!(refresh(&store, &saturated, &instant).await.is_err());
    assert_eq!(count(&store, "exchange_funding_observations").await, 1);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}
