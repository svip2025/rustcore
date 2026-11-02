// K05-A: SQL CAS and Rust precheck are independent authority boundaries.
#[tokio::test]
async fn k05_reconcile_status_cas_rejects_noncanonical_database_bytes_on_mysql() {
    const TEST: &str = "k05_reconcile_status_cas_rejects_noncanonical_database_bytes_on_mysql";
    let Some(scratch) =
        crate::store::mysql_test_support::MySqlScratch::create("k05cas", TEST).await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    let mut observed = Vec::new();
    for (c, collation) in ["utf8mb4_general_ci", "utf8mb4_bin", "utf8mb4_0900_ai_ci"]
        .iter()
        .enumerate()
    {
        // All identifiers are fixed fixture literals; no production schema changes.
        sqlx::query(&format!("ALTER TABLE engine_orders MODIFY status VARCHAR(32) CHARACTER SET utf8mb4 COLLATE {collation} NOT NULL"))
            .execute(store.pool()).await.expect("configure scratch collation");
        for (i, state) in [
            "submitted",
            "Submitted",
            "submitted ",
            "submitted\t",
            "future_state",
            "filled",
        ]
        .iter()
        .enumerate()
        {
            let id = format!("k05-cas-{c}-{i}");
            seed_inflight_order(
                &store,
                "k05-cas",
                &id,
                state,
                "9701",
                "binance|futures|BTC|USDT|USDT",
            )
            .await;
            // The read was canonical. Another writer's stored bytes are the
            // SQL predicate's responsibility, independently of the Rust precheck.
            let wrote =
                apply_reconciled_status(&store, "k05-cas", &id, "submitted", "acked", "9701", 9001)
                    .await
                    .expect("apply monotone truth");
            let row = sqlx::query("SELECT status, CAST(updated_at_ms AS CHAR) AS timestamp_text FROM engine_orders WHERE engine_order_id=?")
                .bind(&id).fetch_one(store.pool()).await.expect("read persisted CAS result");
            observed.push((
                collation.to_string(),
                state.to_string(),
                wrote,
                row.get::<String, _>("status"),
                row.get::<String, _>("timestamp_text"),
            ));
        }
    }
    store.pool().close().await;
    scratch.cleanup().await;
    for (collation, before, wrote, after, timestamp) in observed {
        let canonical = before == "submitted";
        assert_eq!(
            wrote, canonical,
            "SQL CAS authority for {before:?} under {collation}"
        );
        assert_eq!(after, if canonical { "acked" } else { &before });
        assert_eq!(timestamp, if canonical { "9001" } else { "1" });
    }
}

#[tokio::test]
async fn k05_reconcile_status_precheck_does_not_normalize_an_old_observation() {
    let (store, path) = test_store("k05-observation").await;
    let mut observed = Vec::new();
    for (i, read_state) in [
        "Submitted",
        "submitted ",
        "submitted\t",
        "future_state",
        "submitted",
    ]
    .iter()
    .enumerate()
    {
        let id = format!("k05-observed-{i}");
        // Another path may have repaired the row since the observation. The
        // stale malformed observation itself must not become authority.
        seed_inflight_order(
            &store,
            "k05-observed",
            &id,
            "submitted",
            "9702",
            "binance|futures|BTC|USDT|USDT",
        )
        .await;
        let wrote = apply_reconciled_status(
            &store,
            "k05-observed",
            &id,
            read_state,
            "acked",
            "9702",
            9002,
        )
        .await
        .expect("precheck decision");
        observed.push((read_state.to_string(), wrote, status_of(&store, &id).await));
    }
    cleanup(store, path).await;
    for (read_state, wrote, after) in observed {
        let canonical = read_state == "submitted";
        assert_eq!(wrote, canonical, "precheck authority for {read_state:?}");
        assert_eq!(after, if canonical { "acked" } else { "submitted" });
    }
}
