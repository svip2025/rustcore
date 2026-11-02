// K05-A regression: real authenticated ReconcileNow and owned MySQL scratch.
// Fault values are deliberately seeded; no claim that normal writers emit them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k05_reconcile_now_preserves_noncanonical_stored_states() {
    use crate::k04_fake_venue::{open_order, FakeVenue};
    const CRED: &str = "k05-status-rpc";
    let venue = FakeVenue::start().await;
    let _env = k04_env(&venue.base_url(), &[CRED]);
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "k05rpc",
        "k05_reconcile_now_preserves_noncanonical_stored_states",
    )
    .await
    else {
        return;
    };
    let store = scratch.connect_store().await;
    // Explicitly exercise a production-compatible case-insensitive PAD SPACE
    // column. This is test-only persisted-data fault setup, not a migration.
    sqlx::query("ALTER TABLE engine_orders MODIFY status VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL")
        .execute(store.pool()).await.expect("set scratch status comparison semantics");
    let (server, shutdown) = k04_start_engine(
        "k05_reconcile_now_preserves_noncanonical_stored_states",
        &store,
        CRED,
    )
    .await
    .expect("required loopback");
    let cases = [
        "submitted",
        "acked",
        "Submitted",
        "submitted ",
        "future_state",
    ];
    let mut orders = Vec::new();
    for (i, state) in cases.iter().enumerate() {
        let id = format!("k05-{i}");
        let venue_id = 9700 + i as i64;
        sqlx::query("INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, side, order_type, quantity, reduce_only, order_role, status, created_at_ms, updated_at_ms) VALUES (?, ?, 'k05-bot', ?, ?, ?, 'binance', 'futures', ?, 'open_long', 'long', 'limit', '1', 0, 'entry', ?, 1, 1)")
            .bind(&id).bind(CRED).bind(&id).bind(&id).bind(venue_id.to_string()).bind(SYMBOL_KEY).bind(*state)
            .execute(store.pool()).await.expect("seed owned persisted state fault");
        orders.push(open_order("BTCUSDT", venue_id, &id));
    }
    venue.with_book(|book| book.open_orders = orders);
    let refusal = k04_reconcile_now(&server, None, CRED)
        .await
        .expect_err("auth negative");
    assert_eq!(refusal.code(), tonic::Code::Unauthenticated);
    assert!(venue.requests().is_empty(), "refused call sends no HTTP");
    let result = k04_reconcile_now(&server, Some(&server.identity_token), CRED).await;
    let mut observed = Vec::new();
    for (i, before) in cases.iter().enumerate() {
        let id = format!("k05-{i}");
        let row = sqlx::query(
            "SELECT status, HEX(status) AS status_hex FROM engine_orders WHERE engine_order_id=?",
        )
        .bind(&id)
        .fetch_one(store.pool())
        .await
        .expect("read actual committed state");
        let event = sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE idempotency_key=? AND event_type='order_update'")
            .bind(&id).fetch_one(store.pool()).await.expect("read actual events").get::<i64,_>("n");
        observed.push(serde_json::json!({"id":id,"before":before,"after":row.get::<String,_>("status"),"after_hex":row.get::<String,_>("status_hex"),"events":event}));
    }
    let requests: Vec<_> = venue
        .requests()
        .into_iter()
        .map(|q| serde_json::json!({"method":q.method,"path":q.path}))
        .collect();
    println!(
        "K05_OBSERVED {}",
        serde_json::json!({"rows":observed,"http":requests,"rpc_ok":result.is_ok()})
    );
    let _ = shutdown.send(());
    store.pool().close().await;
    scratch.cleanup().await;
    assert!(
        result.is_ok(),
        "ordinary canonical scenario RPC must complete: {result:?}"
    );
    assert!(requests.iter().any(|q| q["path"] == "/fapi/v1/openOrders"));
    assert_eq!(
        observed[0]["after"], "acked",
        "canonical submitted must advance"
    );
    assert_eq!(
        observed[0]["events"], 1,
        "canonical transition emits one actual event"
    );
    assert_eq!(observed[1]["after"], "acked");
    assert_eq!(observed[1]["events"], 0);
    assert_eq!(
        observed[4]["after"], "future_state",
        "unrecognized control stays intact"
    );
    assert_eq!(observed[4]["events"], 0);
    let bad: Vec<_> = [2usize, 3]
        .into_iter()
        .filter(|i| observed[*i]["after"] != cases[*i] || observed[*i]["events"] != 0)
        .collect();
    assert!(bad.is_empty(), "noncanonical persisted states must not be normalized and authorized by collation: {observed:?}");
}
