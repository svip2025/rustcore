// K04 · production-entry wiring proofs (engine safety contract) for the L3 cancellation
// and reconciliation-traversal fixes.
//
// * Entry: the real `/execution_engine.v2.EngineExecution/SetKillSwitch` and
//   `/ReconcileNow` methods over TCP/HTTP2/protobuf through the production
//   service assembly (`crate::add_engine_services` + auth interceptor). The
//   L3 exchange action is driven by `kill_switch::spawn_action_retry_task`,
//   the exact task `main` spawns; nothing calls an internal helper directly.
// * Only the exchange HTTP boundary is local: the production Binance client
//   signs and sends real HTTP to a loopback fake (`k04_fake_venue`).
// * Both sides are observed from their real products: requests the venue
//   actually received, and durable rows the production writers committed.
// * Negative: an unauthenticated call is refused with zero venue traffic;
//   another bot's / another credential's obligations are never sent; L3 stays
//   unconverged while exchange truth still has working orders.

struct K04Env {
    _vars: Vec<EnvGuard>,
    _serial: std::sync::MutexGuard<'static, ()>,
}

fn k04_env(base_url: &str, credentials: &[&str]) -> K04Env {
    let serial = crate::binance_futures::tests::env_serial_guard();
    let mut vars = vec![
        EnvGuard::set("BINANCE_FUTURES_BASE_URL".to_string(), base_url),
        EnvGuard::set("ENGINE_LIVE_ENABLED".to_string(), "1"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(), "100000"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_BURST".to_string(), "100000"),
    ];
    for credential_id in credentials {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        let venue_env = crate::venue::venue_env_name(credential_id);
        let previous = std::env::var_os(&venue_env);
        std::env::remove_var(&venue_env);
        vars.push(EnvGuard {
            name: venue_env,
            previous,
        });
        vars.push(EnvGuard::set(key_env, &format!("{credential_id}-api-key")));
        vars.push(EnvGuard::set(secret_env, "k04-wiring-secret"));
    }
    K04Env {
        _vars: vars,
        _serial: serial,
    }
}

async fn k04_store(label: &str) -> (Store, PathBuf) {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("rustcore-k04-{label}-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("connect sqlite test store and run migrations");
    (store, path)
}

async fn k04_start_engine(
    test_name: &str,
    store: &Store,
    credential_id: &str,
) -> Option<(BoundLoopbackClient, tokio::sync::oneshot::Sender<()>)> {
    let (server, _submit, shutdown) =
        start_engine(test_name, store, credential_id, Vec::new()).await?;
    Some((server, shutdown))
}

async fn k04_reconcile_now(
    server: &BoundLoopbackClient,
    token: Option<&str>,
    credential_id: &str,
) -> Result<crate::proto_v2::ReconcileNowResponse, tonic::Status> {
    use crate::proto_v2::{ReconcileNowRequest, ReconcileNowResponse};
    use tonic::codec::ProstCodec;
    use tonic::codegen::http::uri::PathAndQuery;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", server.addr))
        .expect("valid owned loopback endpoint")
        .connect()
        .await
        .expect("connect the owned production server");
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("server ready");
    let mut request = tonic::Request::new(ReconcileNowRequest {
        credential_id: credential_id.to_string(),
        reason: "k04 traversal proof".to_string(),
        schema_version: 1,
    });
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("metadata"),
        );
    }
    client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.v2.EngineExecution/ReconcileNow"),
            ProstCodec::<ReconcileNowRequest, ReconcileNowResponse>::default(),
        )
        .await
        .map(|response| response.into_inner())
}

#[allow(clippy::too_many_arguments)]
async fn k04_insert_protection(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    protection_order_id: &str,
    position_id: &str,
    credential_id: &str,
    generation: i64,
    exchange_order_id: Option<&str>,
    exchange_algo_id: Option<&str>,
    client_algo_id: Option<&str>,
    status: &str,
    created_at_ms: i64,
) {
    sqlx::query(
        "INSERT INTO protection_orders (protection_order_id, position_id, credential_id, kind, \
         generation, engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id, \
         trigger_price, reduce_only, close_position, status, verified_from_exchange, \
         created_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, 'sl', ?, NULL, ?, ?, ?, '90', 1, 1, ?, 0, ?, ?)",
    )
    .bind(protection_order_id)
    .bind(position_id)
    .bind(credential_id)
    .bind(generation)
    .bind(exchange_order_id)
    .bind(exchange_algo_id)
    .bind(client_algo_id)
    .bind(status)
    .bind(created_at_ms)
    .bind(created_at_ms)
    .execute(&mut **tx)
    .await
    .expect("seed protection order");
}

async fn k04_seed_position(
    store: &Store,
    position_id: &str,
    credential_id: &str,
    bot_uid: &str,
    symbol_key: &str,
) {
    sqlx::query(
        "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, \
         qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, ?, 'long', '1', '100', 'open', 1, 1, 1)",
    )
    .bind(position_id)
    .bind(credential_id)
    .bind(bot_uid)
    .bind(symbol_key)
    .execute(store.pool())
    .await
    .expect("seed position");
}

async fn k04_row(store: &Store, protection_order_id: &str) -> (String, Option<String>) {
    let row = sqlx::query(
        "SELECT status, exchange_order_id FROM protection_orders WHERE protection_order_id=?",
    )
    .bind(protection_order_id)
    .fetch_one(store.pool())
    .await
    .expect("read protection row");
    (
        row.get::<String, _>("status"),
        crate::store::try_get_nullable_string(&row, "exchange_order_id").expect("decode"),
    )
}

/// More than one page (1041 active rows for this credential) with a tie of
/// 80 equal `created_at_ms` values straddling the page boundary. The first
/// page's tie rows are retired by exact venue truth while being processed (a
/// status-shifting OFFSET cursor would then skip rows), the second page's tie
/// rows and the single oldest obligation carry only a clientAlgoId and must
/// each be queried exactly once and bound to their triggered actualOrderId.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k04_reconcile_now_visits_every_protection_page_including_ties_and_the_oldest() {
    use crate::k04_fake_venue::{algo_order, FakeVenue};

    const CRED: &str = "k04-recon-rpc";
    const OTHER_CRED: &str = "k04-recon-other";
    const PAGE_ONE_BULK: i64 = 960;
    const TIES: i64 = 80;
    const TIE_TS: i64 = 50_000;

    let venue = FakeVenue::start().await;
    let _env = k04_env(&venue.base_url(), &[CRED, OTHER_CRED]);
    let (store, path) = k04_store("reconcile-pages").await;
    let Some((server, shutdown)) = k04_start_engine(
        "k04_reconcile_now_visits_every_protection_page_including_ties_and_the_oldest",
        &store,
        CRED,
    )
    .await
    else {
        cleanup(store, path).await;
        return;
    };
    k04_seed_position(&store, "k04r-pos", CRED, "bot-k04r", SYMBOL_KEY).await;
    k04_seed_position(&store, "k04r-pos-other", OTHER_CRED, "bot-k04r", SYMBOL_KEY).await;

    let mut history = Vec::new();
    let mut tx = store.pool().begin().await.expect("seed tx");
    let mut generation = 0;
    let mut next_generation = || {
        generation += 1;
        generation
    };
    // Newest: pre-0013 rows owned by the ordinary domain. They fill page one.
    for i in 0..PAGE_ONE_BULK {
        k04_insert_protection(
            &mut tx,
            &format!("k04r-a-{i:04}"),
            "k04r-pos",
            CRED,
            next_generation(),
            Some(&format!("legacy-{i}")),
            None,
            None,
            "confirmed",
            100_000 + i,
        )
        .await;
    }
    for j in 0..TIES {
        let id = format!("k04r-t-{j:03}");
        let client = format!("{id}-c");
        if j >= TIES / 2 {
            // Sorted first inside the tie → page one. Venue truth: CANCELED.
            let algo_id = 700_000 + j;
            k04_insert_protection(
                &mut tx,
                &id,
                "k04r-pos",
                CRED,
                next_generation(),
                None,
                Some(&algo_id.to_string()),
                Some(&client),
                "confirmed",
                TIE_TS,
            )
            .await;
            history.push(algo_order("BTCUSDT", algo_id, &client, "CANCELED"));
        } else {
            // Page two, same created_at_ms: interrupted submit, clientAlgoId only.
            k04_insert_protection(
                &mut tx,
                &id,
                "k04r-pos",
                CRED,
                next_generation(),
                None,
                None,
                Some(&client),
                "requested",
                TIE_TS,
            )
            .await;
            let mut triggered = algo_order("BTCUSDT", 750_000 + j, &client, "TRIGGERED");
            triggered["actualOrderId"] = serde_json::json!((800_000 + j).to_string());
            history.push(triggered);
        }
    }
    // A terminal row inside the tie keeps the status filter observable.
    k04_insert_protection(
        &mut tx,
        "k04r-t-zzz-excluded",
        "k04r-pos",
        CRED,
        next_generation(),
        None,
        None,
        Some("k04r-excluded"),
        "canceled",
        TIE_TS,
    )
    .await;
    history.push(algo_order("BTCUSDT", 790_001, "k04r-excluded", "NEW"));
    // The oldest active obligation.
    k04_insert_protection(
        &mut tx,
        "k04r-0-oldest",
        "k04r-pos",
        CRED,
        next_generation(),
        None,
        None,
        Some("k04r-oldest"),
        "requested",
        1,
    )
    .await;
    let mut oldest = algo_order("BTCUSDT", 899_998, "k04r-oldest", "TRIGGERED");
    oldest["actualOrderId"] = serde_json::json!("899999");
    history.push(oldest);
    // Another credential's obligation is outside this credential's traversal.
    k04_insert_protection(
        &mut tx,
        "k04r-foreign",
        "k04r-pos-other",
        OTHER_CRED,
        1,
        None,
        None,
        Some("k04r-foreign"),
        "requested",
        2,
    )
    .await;
    history.push(algo_order("BTCUSDT", 790_002, "k04r-foreign", "NEW"));
    tx.commit().await.expect("commit seed");
    venue.with_book(|book| {
        book.algo_history = history;
        book.positions = vec![crate::k04_fake_venue::position("BTCUSDT", "1")];
    });

    // Negative: no token, no traversal, no venue traffic.
    let refused = k04_reconcile_now(&server, None, CRED)
        .await
        .expect_err("unauthenticated ReconcileNow must be refused");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    assert!(venue.requests().is_empty(), "{:?}", venue.requests());

    let response = k04_reconcile_now(&server, Some(&server.identity_token), CRED)
        .await
        .expect("authenticated ReconcileNow");
    assert!(response.completed, "{response:?}");

    let queries: Vec<_> = venue
        .requests()
        .into_iter()
        .filter(|request| request.is("GET", "/fapi/v1/algoOrder"))
        .collect();
    let mut by_client: std::collections::BTreeMap<String, usize> = Default::default();
    let mut by_algo: std::collections::BTreeMap<String, usize> = Default::default();
    for query in &queries {
        if let Some(id) = query.param("clientAlgoId") {
            *by_client.entry(id.to_string()).or_default() += 1;
        }
        if let Some(id) = query.param("algoId") {
            *by_algo.entry(id.to_string()).or_default() += 1;
        }
    }
    for j in 0..TIES / 2 {
        let id = format!("k04r-t-{j:03}");
        assert_eq!(
            by_client.get(&format!("{id}-c")),
            Some(&1),
            "second-page tie row {id} must be queried exactly once"
        );
        assert_eq!(
            k04_row(&store, &id).await,
            ("triggered".to_string(), Some((800_000 + j).to_string())),
            "second-page tie row {id} must bind its triggered actualOrderId"
        );
    }
    for j in TIES / 2..TIES {
        let id = format!("k04r-t-{j:03}");
        assert_eq!(by_algo.get(&(700_000 + j).to_string()), Some(&1), "{id}");
        assert_eq!(k04_row(&store, &id).await.0, "canceled", "{id}");
    }
    assert_eq!(
        by_client.get("k04r-oldest"),
        Some(&1),
        "the oldest obligation is visited"
    );
    assert_eq!(
        k04_row(&store, "k04r-0-oldest").await,
        ("triggered".to_string(), Some("899999".to_string()))
    );
    assert_eq!(
        by_client.get("k04r-excluded"),
        None,
        "terminal rows stay filtered"
    );
    assert_eq!(
        by_client.get("k04r-foreign"),
        None,
        "other credential stays out"
    );
    assert_eq!(
        queries.len() as i64,
        TIES + 1,
        "every active Algo-domain row is visited exactly once: {by_client:?} {by_algo:?}"
    );
    assert_eq!(k04_row(&store, "k04r-foreign").await.0, "requested");

    let _ = shutdown.send(());
    cleanup(store, path).await;
}

/// L3 through the production SetKillSwitch → durable latch → retry task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k04_set_kill_switch_l3_cancels_triggered_child_and_client_algo_only_obligation() {
    use crate::k04_fake_venue::{algo_order, open_order, FakeVenue};

    const CRED: &str = "k04-ks-rpc";
    const BOT: &str = "bot-k04w";
    const OTHER_BOT: &str = "bot-k04w-other";
    const ETH_KEY: &str = "binance|futures|ETH|USDT|USDT";
    let scope_key = format!("bot:{BOT}");

    let mut venue = FakeVenue::start().await;
    const CRED_B: &str = "k04-ks-rpc-foreign";
    let _env = k04_env(&venue.base_url(), &[CRED, CRED_B]);
    let (store, path) = k04_store("set-kill-switch").await;
    let Some((server, shutdown)) = k04_start_engine(
        "k04_set_kill_switch_l3_cancels_triggered_child_and_client_algo_only_obligation",
        &store,
        CRED,
    )
    .await
    else {
        cleanup(store, path).await;
        return;
    };

    k04_seed_position(&store, "k04w-pos", CRED, BOT, SYMBOL_KEY).await;
    k04_seed_position(&store, "k04w-pos-other", CRED, OTHER_BOT, ETH_KEY).await;
    for (engine_order_id, bot_uid, symbol_key, position_id, status, actual, client) in [
        (
            "k04w-sl-eng",
            BOT,
            SYMBOL_KEY,
            "k04w-pos",
            "triggered",
            Some("7201"),
            "k04w-sl-algo",
        ),
        (
            "k04w-co-eng",
            BOT,
            SYMBOL_KEY,
            "k04w-pos",
            "acked",
            None,
            "k04w-client-only",
        ),
        (
            "k04w-fail-eng",
            BOT,
            SYMBOL_KEY,
            "k04w-pos",
            "acked",
            None,
            "k04w-fail",
        ),
        (
            "k04w-other-eng",
            OTHER_BOT,
            ETH_KEY,
            "k04w-pos-other",
            "triggered",
            Some("7202"),
            "k04w-other-algo",
        ),
    ] {
        sqlx::query(
            "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
             client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, \
             side, order_type, quantity, reduce_only, order_role, status, position_id, \
             created_at_ms, updated_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, 'binance', 'futures', ?, 'close_long', 'long', \
             'stop_market', '1', 1, 'protection_sl', ?, ?, 1, 1)",
        )
        .bind(engine_order_id)
        .bind(CRED)
        .bind(bot_uid)
        .bind(format!("idem-{engine_order_id}"))
        .bind(client)
        .bind(actual)
        .bind(symbol_key)
        .bind(status)
        .bind(position_id)
        .execute(store.pool())
        .await
        .expect("seed protection engine order");
    }
    // K04-R1: a bot can retain orders under both its current and prior credential.
    k04_seed_position(&store, "k04w-pos-b", CRED_B, BOT, SYMBOL_KEY).await;
    sqlx::query("INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, side, order_type, quantity, reduce_only, order_role, status, position_id, created_at_ms, updated_at_ms) SELECT '000-k04w-b-eng', ?, bot_uid, 'idem-k04w-b', 'k04w-b-algo', '7301', exchange, market_kind, symbol_key, action, side, order_type, quantity, reduce_only, order_role, status, 'k04w-pos-b', created_at_ms, updated_at_ms FROM engine_orders WHERE engine_order_id='k04w-sl-eng'")
        .bind(CRED_B).execute(store.pool()).await.expect("seed second credential triggered child");
    let mut tx = store.pool().begin().await.expect("seed tx");
    k04_insert_protection(
        &mut tx,
        "k04w-b",
        "k04w-pos-b",
        CRED_B,
        1,
        Some("7301"),
        Some("5401"),
        Some("k04w-b-algo"),
        "triggered",
        10,
    )
    .await;
    sqlx::query("UPDATE protection_orders SET engine_order_id='000-k04w-b-eng' WHERE protection_order_id='k04w-b'").execute(&mut *tx).await.expect("link second credential protection");
    k04_insert_protection(
        &mut tx,
        "k04w-sl",
        "k04w-pos",
        CRED,
        1,
        Some("7201"),
        Some("5301"),
        Some("k04w-sl-algo"),
        "triggered",
        10,
    )
    .await;
    k04_insert_protection(
        &mut tx,
        "k04w-co",
        "k04w-pos",
        CRED,
        2,
        None,
        None,
        Some("k04w-client-only"),
        "requested",
        11,
    )
    .await;
    k04_insert_protection(
        &mut tx,
        "k04w-fail",
        "k04w-pos",
        CRED,
        3,
        None,
        Some("5303"),
        Some("k04w-fail"),
        "confirmed",
        12,
    )
    .await;
    k04_insert_protection(
        &mut tx,
        "k04w-other",
        "k04w-pos-other",
        CRED,
        1,
        Some("7202"),
        Some("5304"),
        Some("k04w-other-algo"),
        "triggered",
        13,
    )
    .await;
    tx.commit().await.expect("commit seed");
    for (id, engine_order_id) in [
        ("k04w-sl", "k04w-sl-eng"),
        ("k04w-co", "k04w-co-eng"),
        ("k04w-fail", "k04w-fail-eng"),
        ("k04w-other", "k04w-other-eng"),
    ] {
        sqlx::query("UPDATE protection_orders SET engine_order_id=? WHERE protection_order_id=?")
            .bind(engine_order_id)
            .bind(id)
            .execute(store.pool())
            .await
            .expect("link protection to its engine order");
    }
    venue.with_book(|book| {
        book.open_orders = vec![
            open_order("BTCUSDT", 7201, "autoclose-k04w-sl"),
            open_order("ETHUSDT", 7202, "autoclose-k04w-other"),
            open_order("BTCUSDT", 7301, "autoclose-k04w-b"),
        ];
        book.open_algos = vec![
            algo_order("BTCUSDT", 5302, "k04w-client-only", "NEW"),
            algo_order("BTCUSDT", 5303, "k04w-fail", "NEW"),
        ];
        let mut sl = algo_order("BTCUSDT", 5301, "k04w-sl-algo", "TRIGGERED");
        sl["actualOrderId"] = serde_json::json!("7201");
        let mut other = algo_order("ETHUSDT", 5304, "k04w-other-algo", "TRIGGERED");
        other["actualOrderId"] = serde_json::json!("7202");
        let mut child_b = algo_order("BTCUSDT", 5401, "k04w-b-algo", "TRIGGERED");
        child_b["actualOrderId"] = serde_json::json!("7301");
        book.algo_history = vec![sl, other, child_b];
        book.failing_algo_cancels.insert("algoId:5303".to_string());
        book.positions = vec![
            crate::k04_fake_venue::position("BTCUSDT", "1"),
            crate::k04_fake_venue::position("ETHUSDT", "1"),
        ];
    });

    // The production background driver, exactly as `main` spawns it.
    let task = crate::kill_switch::spawn_action_retry_task(store.clone());
    let arm = || crate::proto_v2::SetKillSwitchRequest {
        scope_key: scope_key.clone(),
        level: "flatten".to_string(),
        reason: "k04 wiring".to_string(),
        trigger_source: "manual_api".to_string(),
        actor: "k04-operator".to_string(),
        schema_version: 1,
        authority_member_id: None,
        expected_authority_revision: None,
        request_ref: None,
    };
    let probe = || request_for(CRED, close_intent("k04_probe", "market", ""));

    // Negative: unauthenticated → refused, no latch, no venue traffic.
    let refused = super::loopback_test_support::set_kill_switch_after_identity_probe(
        &server,
        None,
        probe(),
        arm(),
    )
    .await
    .expect_err("a call without a token must be refused");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated, "{refused:?}");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let latches: i64 = sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_state WHERE scope_key=?")
        .bind(&scope_key)
        .fetch_one(store.pool())
        .await
        .expect("count latches")
        .get("n");
    assert_eq!(latches, 0);
    assert!(venue.requests().is_empty(), "{:?}", venue.requests());

    let accepted = super::loopback_test_support::set_kill_switch_after_identity_probe(
        &server,
        Some(&server.identity_token),
        probe(),
        arm(),
    )
    .await
    .expect("authenticated SetKillSwitch");
    assert!(accepted.accepted, "{accepted:?}");

    let latch_state = || async {
        sqlx::query(
            "SELECT COALESCE(flatten_state, '') AS s FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(&scope_key)
        .fetch_one(store.pool())
        .await
        .expect("read latch")
        .get::<String, _>("s")
    };
    // Phase 1: the first L3 round cancels what it owns, then reads truth.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let requests = venue.requests();
        let child = requests
            .iter()
            .any(|r| r.is("DELETE", "/fapi/v1/order") && r.param("orderId") == Some("7201"));
        let client_only = requests.iter().any(|r| {
            r.is("DELETE", "/fapi/v1/algoOrder")
                && r.param("clientAlgoId") == Some("k04w-client-only")
        });
        let truth_read = requests.iter().any(|r| r.is("GET", "/fapi/v2/balance"));
        if child && client_only && truth_read {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retry task never reached the owned obligations: {requests:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Require the second credential's positive before progressing. The final
    // ownership assertion runs only after convergence and task/handler shutdown.
    let owner_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let requests = venue.requests();
        if requests.iter().any(|r| {
            r.is("DELETE", "/fapi/v1/order")
                && r.param("orderId") == Some("7301")
                && r.api_key == format!("{CRED_B}-api-key")
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < owner_deadline,
            "second credential owner-key dispatch not observed: {requests:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(
        latch_state().await,
        "flattened",
        "a working Algo order and another bot's open order keep L3 unconverged"
    );

    // Phase 2: the venue recovers and the other bot's exposure is gone; the
    // same production retry task converges only on clean exchange truth.
    venue.with_book(|book| {
        book.failing_algo_cancels.clear();
        book.open_orders
            .retain(|order| order["orderId"] != serde_json::json!(7202));
        for position in book.positions.iter_mut() {
            position["positionAmt"] = serde_json::json!("0");
        }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while latch_state().await != "flattened" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "clean exchange truth must converge the latch: state={} requests={:?}",
            latch_state().await,
            venue.requests()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    venue.with_book(|book| {
        assert!(book.open_algos.is_empty(), "{:?}", book.open_algos);
        assert!(book.open_orders.is_empty(), "{:?}", book.open_orders);
    });

    // Both credentials' positive dispatches and clean convergence were observed
    // above. End the actual producer, then drain every accepted HTTP handler.
    // The negative assertion covers this entire finite run, not just the first
    // owner's dispatch snapshot and not any hypothetical future task run.
    task.abort();
    let stopped = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("production retry task must stop within five seconds")
        .expect_err("the infinite retry task must stop by cancellation");
    assert!(stopped.is_cancelled(), "{stopped:?}");
    let _ = shutdown.send(());
    let all_requests = venue.stop_and_drain().await;
    assert!(
        all_requests.iter().any(|r| r.is("DELETE", "/fapi/v1/order")
            && r.param("orderId") == Some("7201")
            && r.api_key == format!("{CRED}-api-key")),
        "credential A positive owner case must dispatch"
    );
    assert!(
        !all_requests
            .iter()
            .any(|r| r.is("DELETE", "/fapi/v1/order") && r.param("orderId") == Some("7202")),
        "different bot child must remain excluded"
    );
    assert!(!all_requests.iter().any(|r| r.is("DELETE", "/fapi/v1/order") && r.param("orderId") == Some("7301") && r.api_key != format!("{CRED_B}-api-key")), "credential B triggered child must never be sent with credential A key; actual ordinary requests: {:?}", all_requests.iter().filter(|r| r.is("DELETE", "/fapi/v1/order")).collect::<Vec<_>>());
    let child_cancel = all_requests
        .iter()
        .find(|r| r.is("DELETE", "/fapi/v1/order") && r.param("orderId") == Some("7201"))
        .expect("child cancel");
    assert_eq!(child_cancel.param("symbol"), Some("BTCUSDT"));
    assert_eq!(child_cancel.param("origClientOrderId"), None);
    assert_eq!(child_cancel.api_key, format!("{CRED}-api-key"));
    assert!(
        !all_requests.iter().any(|r| r.is("DELETE", "/fapi/v1/order")
            && r.param("orderId") == Some("7201")
            && r.api_key != format!("{CRED}-api-key")),
        "credential A child must only be cancelled using its own key"
    );
    let client_only_cancel = all_requests
        .iter()
        .find(|r| {
            r.is("DELETE", "/fapi/v1/algoOrder")
                && r.param("clientAlgoId") == Some("k04w-client-only")
        })
        .expect("clientAlgo-only cancel");
    assert_eq!(client_only_cancel.param("algoId"), None);
    assert!(all_requests
        .iter()
        .any(|r| r.is("DELETE", "/fapi/v1/algoOrder") && r.param("algoId") == Some("5303")));
    for request in &all_requests {
        assert!(
            !(request.is("DELETE", "/fapi/v1/order") && request.param("orderId") == Some("7202")),
            "another bot's triggered child must not be cancelled: {request:?}"
        );
        assert!(
            !(request.is("DELETE", "/fapi/v1/algoOrder")
                && request.param("algoId") == Some("5304")),
            "another bot's Algo parent must not be cancelled: {request:?}"
        );
        if request.is("DELETE", "/fapi/v1/order") {
            let client = request.param("origClientOrderId").unwrap_or_default();
            assert!(
                !client.starts_with("k04w-"),
                "an Algo identity must never become an ordinary clientOrderId: {request:?}"
            );
        }
    }
    cleanup(store, path).await;
}

/// Real authenticated RPC -> durable latch -> production retry task -> signed
/// HTTP. Two completed reconciliation projections establish a bounded retry
/// interval; the RPC acknowledgement itself is not the action result.
async fn k04_b_assert_rpc_open_orders_failure_isolation(global: bool) {
    use crate::k04_fake_venue::{open_order, FakeVenue};
    const CRED: &str = "k04b-rpc-a";
    const OTHER_CRED: &str = "k04b-rpc-b";
    let scope_key = if global {
        "global".to_string()
    } else {
        format!("cred:{CRED}")
    };
    let mut venue = FakeVenue::start().await;
    let _env = k04_env(&venue.base_url(), &[CRED, OTHER_CRED]);
    let (store, path) = k04_store("ordinary-failure-rpc").await;
    let Some((server, shutdown)) =
        k04_start_engine("k04_b_rpc_open_orders_failure_isolation", &store, CRED).await
    else {
        cleanup(store, path).await;
        return;
    };
    k04_seed_position(&store, "k04b-rpc-pos", CRED, "bot-k04b", SYMBOL_KEY).await;
    if !global {
        k04_seed_position(
            &store,
            "k04b-rpc-foreign",
            OTHER_CRED,
            "bot-k04b",
            SYMBOL_KEY,
        )
        .await;
    }
    sqlx::query("UPDATE position_state SET state='closed', qty='0'")
        .execute(store.pool())
        .await
        .expect("no exchange position remains; only ordinary orders are outstanding");
    for (id, order_id) in [("first", "8201"), ("next", "8202"), ("last", "8203")] {
        sqlx::query(
            "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
             client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, \
             side, order_type, quantity, reduce_only, order_role, status, position_id, \
             created_at_ms, updated_at_ms) \
             VALUES (?, ?, 'bot-k04b', ?, ?, ?, 'binance', 'futures', ?, 'close_long', 'long', \
             'limit', '1', 1, 'exit', 'acked', 'k04b-rpc-pos', 1, 1)",
        )
        .bind(format!("k04b-rpc-{id}"))
        .bind(CRED)
        .bind(format!("idem-k04b-rpc-{id}"))
        .bind(format!("k04b-rpc-{id}"))
        .bind(order_id)
        .bind(SYMBOL_KEY)
        .execute(store.pool())
        .await
        .expect("seed known working ordinary exit");
    }
    venue.with_book(|book| {
        book.open_orders = vec![
            open_order("BTCUSDT", 8201, "k04b-rpc-first"),
            open_order("BTCUSDT", 8202, "k04b-rpc-next"),
            open_order("BTCUSDT", 8203, "k04b-rpc-last"),
        ];
        book.failing_order_cancels
            .extend(["8201".to_string(), "8203".to_string()]);
    });
    let arm = || crate::proto_v2::SetKillSwitchRequest {
        scope_key: scope_key.clone(),
        level: "flatten".to_string(),
        reason: "k04-b ordinary failure isolation".to_string(),
        trigger_source: "manual_api".to_string(),
        actor: "k04-b-operator".to_string(),
        schema_version: 1,
        authority_member_id: None,
        expected_authority_revision: None,
        request_ref: None,
    };
    let probe = || request_for(CRED, close_intent("k04b_probe", "market", ""));
    let refused = super::loopback_test_support::set_kill_switch_after_identity_probe(
        &server,
        None,
        probe(),
        arm(),
    )
    .await
    .expect_err("unauthenticated SetKillSwitch must be refused");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    // Migration 0008 seeds global=none; an unauthenticated call must not activate it.
    let latch_count: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM kill_switch_state WHERE scope_key=? AND level <> 'none'",
    )
    .bind(&scope_key)
    .fetch_one(store.pool())
    .await
    .expect("count unauthenticated latches")
    .get("n");
    assert_eq!(latch_count, 0);
    assert!(venue.requests().is_empty(), "{:?}", venue.requests());

    let task = crate::kill_switch::spawn_action_retry_task(store.clone());
    let accepted = super::loopback_test_support::set_kill_switch_after_identity_probe(
        &server,
        Some(&server.identity_token),
        probe(),
        arm(),
    )
    .await
    .expect("authenticated SetKillSwitch");
    assert!(accepted.accepted, "{accepted:?}");
    let completed_rounds = tokio::time::timeout(Duration::from_secs(30), async {
        let mut first_projection = None;
        loop {
            let row = sqlx::query("SELECT reconciled_at_ms FROM kill_switch_reconcile_status WHERE scope_key=? AND credential_id=?")
                .bind(&scope_key).bind(CRED).fetch_optional(store.pool()).await.expect("read production reconcile projection");
            let at_ms = row.map(|row| row.get::<i64, _>("reconciled_at_ms")).unwrap_or(0);
            let first_attempts = venue.requests().iter().filter(|r| r.is("DELETE", "/fapi/v1/order") && r.param("orderId") == Some("8201")).count();
            if at_ms > 0 {
                if let Some(first) = first_projection {
                    if at_ms > first && first_attempts >= 2 {
                        break (first, at_ms);
                    }
                } else {
                    first_projection = Some(at_ms);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await;
    // End the production task and all accepted venue requests before assertions
    // so even the deliberately failing old candidate has a finite complete log.
    task.abort();
    let stopped = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("production retry task must stop")
        .expect_err("retry loop stops by cancellation");
    assert!(stopped.is_cancelled(), "{stopped:?}");
    let _ = shutdown.send(());
    let requests = venue.stop_and_drain().await;
    let latch = sqlx::query("SELECT level, COALESCE(flatten_state, '') AS state FROM kill_switch_state WHERE scope_key=?")
        .bind(&scope_key).fetch_one(store.pool()).await.expect("read final persistent latch");
    let level: String = latch.get("level");
    let state: String = latch.get("state");
    let ordinary: Vec<_> = requests
        .iter()
        .filter(|r| r.is("DELETE", "/fapi/v1/order"))
        .collect();
    println!("K04-B stopped RPC interval scope={scope_key} accepted={} projections={completed_rounds:?} latch={level}/{state} ordinary={ordinary:?}", accepted.accepted);
    cleanup(store, path).await;
    completed_rounds.expect("two complete production truth projections must precede shutdown");
    assert_eq!(level, "flatten");
    assert_eq!(
        state, "flattening",
        "failed venue orders must keep the accepted L3 request unconverged"
    );
    assert!(
        ordinary
            .iter()
            .any(|r| r.param("orderId") == Some("8202") && r.api_key == format!("{CRED}-api-key")),
        "a rejected first order must not starve the following ordinary order: {ordinary:?}"
    );
    assert!(
        ordinary.iter().any(|r| r.param("orderId") == Some("8203")),
        "the last rejected order must also be visited"
    );
    assert!(
        ordinary
            .iter()
            .all(|r| r.api_key == format!("{CRED}-api-key")),
        "{ordinary:?}"
    );
    assert!(
        !requests
            .iter()
            .any(|r| r.api_key == format!("{OTHER_CRED}-api-key")),
        "the credential scope must not visit the other credential: {requests:?}"
    );
    venue.with_book(|book| {
        assert_eq!(
            book.open_orders
                .iter()
                .map(|o| o["orderId"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![8201, 8203],
            "HTTP acceptance must not retire rejected venue orders"
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k04_b_credential_rpc_retries_failed_order_and_cancels_later_orders() {
    k04_b_assert_rpc_open_orders_failure_isolation(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k04_b_global_rpc_retries_failed_order_and_cancels_later_orders() {
    k04_b_assert_rpc_open_orders_failure_isolation(true).await;
}
