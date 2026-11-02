// Included in reconcile::tests: real SQLite migrations + complete adapter pipeline.
const TERMINAL_TEST_SYMBOL: &str = "binance|futures|BTC|USDT|USDT";

async fn terminal_seed_protection(
    store: &Store,
    credential: &str,
    label: &str,
    po_status: &str,
    eo_status: &str,
) {
    let position_id = format!("pos-{label}");
    seed_inflight_order(
        store,
        credential,
        label,
        eo_status,
        "",
        TERMINAL_TEST_SYMBOL,
    )
    .await;
    sqlx::query("UPDATE engine_orders SET exchange_order_id=NULL, position_id=?, order_role='protection_sl', action='close_long', reduce_only=1 WHERE engine_order_id=?")
        .bind(&position_id).bind(label).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES (?,?,'bot_r11',?,'long',1,'closed','0',1)")
        .bind(&position_id).bind(credential).bind(TERMINAL_TEST_SYMBOL).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,exchange_algo_id,client_algo_id,trigger_price,status,created_at_ms,updated_at_ms) VALUES (?, ?, ?, 'sl', 1, ?, '7001', 'algo-terminal', '90', ?, 1, 1)")
        .bind(format!("po-{label}")).bind(position_id).bind(credential).bind(label).bind(po_status).execute(store.pool()).await.unwrap();
}

async fn terminal_round(
    store: &Store,
    credential: &str,
    ordinary: String,
    algos: &str,
    coverage: Coverage,
) -> super::CoveredReconcileTruth {
    let client = BinanceFuturesClient::new_with_mock_responses(
        credential,
        vec![
            (200, ordinary),
            (200, algos.to_string()),
            (200, "[]".to_string()),
            (200, balances().to_string()),
        ],
    );
    let covered = reconcile_binance_futures_truth_with_client_and_coverage(
        credential,
        store.clone(),
        CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0)),
        &client,
        coverage,
    )
    .await
    .expect("complete production reconcile pipeline");
    assert!(
        client
            .recorded_requests()
            .iter()
            .all(|request| request.starts_with("GET ")),
        "reconcile must remain read-only at the exchange: {:?}",
        client.recorded_requests()
    );
    covered
}

#[tokio::test]
async fn terminal_remote_ordinary_conflicts_cover_every_terminal_status_and_identity() {
    for status in ["filled", "canceled", "expired", "rejected", "failed"] {
        for has_exchange_id in [false, true] {
            let (store, path) = test_store("terminal-ordinary").await;
            seed_inflight_order(
                &store,
                "terminal-ordinary",
                "terminal-order",
                status,
                if has_exchange_id { "6001" } else { "" },
                TERMINAL_TEST_SYMBOL,
            )
            .await;
            let truth = terminal_round(
                &store,
                "terminal-ordinary",
                open_order("BTCUSDT", 6001, "coid-terminal-order", "NEW"),
                "[]",
                Coverage::Full,
            )
            .await
            .truth;
            assert!(truth.has_diff, "{status}, exchange_id={has_exchange_id}");
            assert!(!truth.all_open_orders_empty);
            let payload = last_reconcile_diff_payload(&store, "terminal-ordinary").await;
            assert_eq!(payload["summary"]["missingLocalOpenOrdersCount"], 0);
            assert_eq!(payload["summary"]["terminalLocalRemoteOpenOrdersCount"], 1);
            assert_eq!(
                payload["terminalLocalRemoteOpenOrders"][0]["localStatus"],
                status
            );
            assert_eq!(status_of(&store, "terminal-order").await, status);
            cleanup(store, path).await;
        }
    }
}

#[tokio::test]
async fn terminal_remote_algo_conflict_preserves_history_across_repeated_rounds() {
    for (po_status, eo_status) in [
        ("failed", "unknown"),
        ("canceled", "acked"),
        ("expired", "acked"),
        ("rejected", "acked"),
        ("confirmed", "failed"),
    ] {
        for has_exchange_id in [false, true] {
            let (store, path) = test_store("terminal-algo").await;
            terminal_seed_protection(
                &store,
                "terminal-algo",
                "terminal-algo-order",
                po_status,
                eo_status,
            )
            .await;
            if !has_exchange_id {
                sqlx::query("UPDATE protection_orders SET exchange_algo_id=NULL")
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
            for _ in 0..2 {
                let truth = terminal_round(
                    &store,
                    "terminal-algo",
                    "[]".to_string(),
                    r#"[{"algoId":7001,"clientAlgoId":"algo-terminal","algoStatus":"NEW","updateTime":5}]"#,
                    Coverage::Full,
                )
                .await
                .truth;
                assert!(truth.has_diff, "po={po_status} eo={eo_status}");
                assert!(!truth.all_open_algo_orders_empty);
                let payload = last_reconcile_diff_payload(&store, "terminal-algo").await;
                assert_eq!(payload["summary"]["missingLocalOpenAlgoOrdersCount"], 0);
                assert_eq!(
                    payload["summary"]["terminalLocalRemoteOpenAlgoOrdersCount"],
                    1
                );
                let saved: String = sqlx::query("SELECT status FROM protection_orders")
                    .fetch_one(store.pool())
                    .await
                    .unwrap()
                    .get("status");
                assert_eq!(
                    saved, po_status,
                    "an active remote parent must not erase contradictory local history"
                );
                assert_eq!(status_of(&store, "terminal-algo-order").await, eo_status);
            }
            cleanup(store, path).await;
        }
    }
}

#[tokio::test]
async fn terminal_remote_closed_position_with_current_protection_is_a_normal_cleanup_transition() {
    for (local, remote) in [("confirmed", "NEW"), ("triggering", "TRIGGERING")] {
        let (store, path) = test_store("terminal-normal-cleanup").await;
        terminal_seed_protection(&store, "terminal-cleanup", "cleanup-order", local, "acked").await;
        let algos =
            serde_json::json!([{"algoId":7001,"clientAlgoId":"algo-terminal","algoStatus":remote,"updateTime":5}])
                .to_string();
        let truth = terminal_round(
            &store,
            "terminal-cleanup",
            "[]".to_string(),
            &algos,
            Coverage::Full,
        )
        .await
        .truth;
        assert!(
            !truth.has_diff,
            "closed-only must permit the existing owned cleanup path"
        );
        assert!(
            !truth.all_open_algo_orders_empty,
            "normal cleanup is still pending"
        );
        let payload = last_reconcile_diff_payload(&store, "terminal-cleanup").await;
        assert_eq!(
            payload["summary"]["terminalLocalRemoteOpenAlgoOrdersCount"],
            0
        );
        cleanup(store, path).await;
    }
}

#[tokio::test]
async fn terminal_remote_conflicts_obey_credential_symbol_and_acknowledged_identity() {
    let (store, path) = test_store("terminal-scope").await;
    seed_inflight_order(
        &store,
        "terminal-scope",
        "current",
        "acked",
        "6001",
        TERMINAL_TEST_SYMBOL,
    )
    .await;
    seed_inflight_order(
        &store,
        "foreign-credential",
        "foreign",
        "failed",
        "6001",
        TERMINAL_TEST_SYMBOL,
    )
    .await;
    seed_inflight_order(
        &store,
        "terminal-scope",
        "foreign-symbol",
        "failed",
        "6001",
        "binance|futures|ETH|USDT|USDT",
    )
    .await;
    seed_inflight_order(
        &store,
        "terminal-scope",
        "recycled",
        "filled",
        "6002",
        TERMINAL_TEST_SYMBOL,
    )
    .await;
    sqlx::query("UPDATE engine_orders SET client_order_id_short='coid-current' WHERE engine_order_id='recycled'").execute(store.pool()).await.unwrap();
    terminal_seed_protection(
        &store,
        "terminal-scope",
        "current-protection",
        "confirmed",
        "acked",
    )
    .await;
    terminal_seed_protection(
        &store,
        "foreign-credential",
        "foreign-protection",
        "failed",
        "unknown",
    )
    .await;
    let truth = terminal_round(
        &store,
        "terminal-scope",
        open_order("BTCUSDT", 6001, "coid-current", "NEW"),
        r#"[{"algoId":7001,"clientAlgoId":"algo-terminal","algoStatus":"NEW","updateTime":5}]"#,
        Coverage::Full,
    )
    .await
    .truth;
    assert!(
        !truth.has_diff,
        "foreign terminal IDs and recycled clients cannot contaminate current active identities"
    );
    let payload = last_reconcile_diff_payload(&store, "terminal-scope").await;
    assert_eq!(payload["summary"]["terminalLocalRemoteOpenOrdersCount"], 0);
    assert_eq!(
        payload["summary"]["terminalLocalRemoteOpenAlgoOrdersCount"],
        0
    );
    cleanup(store, path).await;
}

#[tokio::test]
async fn terminal_remote_narrow_scan_sets_diff_but_uncovered_empty_scan_cannot_clear_it() {
    let (store, path) = test_store("terminal-coverage").await;
    terminal_seed_protection(
        &store,
        "terminal-coverage",
        "coverage-order",
        "failed",
        "unknown",
    )
    .await;
    let truth = terminal_round(
        &store,
        "terminal-coverage",
        "[]".to_string(),
        r#"[{"algoId":7001,"clientAlgoId":"algo-terminal","algoStatus":"NEW","updateTime":5}]"#,
        Coverage::Symbols(vec!["BTCUSDT".to_string()]),
    )
    .await
    .truth;
    assert!(truth.has_diff);
    let truth = terminal_round(
        &store,
        "terminal-coverage",
        "[]".to_string(),
        "[]",
        Coverage::Symbols(vec!["ETHUSDT".to_string()]),
    )
    .await
    .truth;
    assert!(
        !truth.has_diff,
        "empty scoped observation is scoped truth only"
    );
    let health: i64 = sqlx::query("SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id='terminal-coverage'").fetch_one(store.pool()).await.unwrap().get("reconcile_diff_open");
    assert_eq!(health, 1, "uncovered truth must not reopen entry");
    let truth = terminal_round(
        &store,
        "terminal-coverage",
        "[]".to_string(),
        "[]",
        Coverage::Full,
    )
    .await
    .truth;
    assert!(!truth.has_diff);
    let health: i64 = sqlx::query("SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id='terminal-coverage'").fetch_one(store.pool()).await.unwrap().get("reconcile_diff_open");
    assert_eq!(
        health, 0,
        "fresh complete absence can clear this remote-active conflict, without retiring history"
    );
    assert_eq!(status_of(&store, "coverage-order").await, "unknown");
    cleanup(store, path).await;
}
