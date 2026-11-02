//! K04 · L3 exchange cancellation must reach every owned obligation that the
//! engine can identify, through the production scope driver
//! (`drive_live_scope_once`) and a loopback fake venue.
//!
//! Contracts pinned here:
//! 1. an Algo protection whose truth projected `triggered` + actualOrderId is
//!    an ordinary child that may still be working: bot-scope L3 cancels it by
//!    that ordinary `orderId`, never by the parent's clientAlgoId; another
//!    bot's child and terminal local rows are untouched; L2 stays entry-only;
//! 2. an obligation that only carries `client_algo_id` (interrupted submit)
//!    is cancelled through the typed clientAlgoId identity even when its
//!    position is locally closed; one failing / unknown / identity-less
//!    obligation neither starves its siblings nor is retired locally, and the
//!    round stays a visible failure while exchange truth is not flat;
//! 3. ownership: another bot's and another credential's obligations are never
//!    sent with this credential's key.

use std::path::PathBuf;
use std::sync::Once;

use sqlx::Row;

use super::{drive_live_scope_once, Level};
use crate::binance_futures::tests::{env_serial_guard, TestEnvVar};
use crate::k04_fake_venue::{algo_order, open_order, FakeVenue, RecordedRequest};
use crate::store::Store;

static INSTALL_DRIVERS: Once = Once::new();

const CRED_A: &str = "cred-k04a";
const CRED_B: &str = "cred-k04b";
const BOT: &str = "bot-k04";
const OTHER_BOT: &str = "bot-k04-other";
const BTC: &str = "binance|futures|BTC|USDT|USDT";
const ETH: &str = "binance|futures|ETH|USDT|USDT";

fn api_key(credential_id: &str) -> String {
    format!("{credential_id}-api-key")
}

/// Process-level env for a live-enabled, venue-unset credential whose REST
/// base URL is the loopback fake. Field order is drop order: env first, lock last.
struct LiveEnv {
    _vars: Vec<TestEnvVar>,
    _serial: std::sync::MutexGuard<'static, ()>,
}

fn live_env(venue: &FakeVenue) -> LiveEnv {
    let serial = env_serial_guard();
    let mut vars = vec![
        TestEnvVar::set("BINANCE_FUTURES_BASE_URL".to_string(), &venue.base_url()),
        TestEnvVar::set("ENGINE_LIVE_ENABLED".to_string(), "1"),
        TestEnvVar::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(), "100000"),
        TestEnvVar::set("RUSTCORE_BINANCE_WEIGHT_BURST".to_string(), "100000"),
        TestEnvVar::set("KILL_SWITCH_CREDENTIAL_DEADLINE_MS".to_string(), "60000"),
    ];
    for credential_id in [CRED_A, CRED_B] {
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
        vars.push(TestEnvVar::unset(crate::venue::venue_env_name(
            credential_id,
        )));
        vars.push(TestEnvVar::set(key_env, &api_key(credential_id)));
        vars.push(TestEnvVar::set(secret_env, "k04-secret"));
    }
    LiveEnv {
        _vars: vars,
        _serial: serial,
    }
}

async fn sqlite_store(label: &str) -> (Store, PathBuf) {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("rustcore-k04-{label}-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("connect sqlite test store and run migrations");
    (store, path)
}

async fn drop_sqlite(store: Store, path: PathBuf) {
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

async fn seed_position(
    store: &Store,
    position_id: &str,
    credential_id: &str,
    bot_uid: &str,
    symbol_key: &str,
    open: bool,
) {
    sqlx::query(
        "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, \
         qty, entry_price, state, entry_epoch, opened_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, ?, 'long', ?, '100', ?, 1, 1, 1)",
    )
    .bind(position_id)
    .bind(credential_id)
    .bind(bot_uid)
    .bind(symbol_key)
    .bind(if open { "1" } else { "0" })
    .bind(if open { "open" } else { "closed" })
    .execute(store.pool())
    .await
    .expect("seed position");
}

struct OrderSeed<'a> {
    engine_order_id: &'a str,
    credential_id: &'a str,
    bot_uid: &'a str,
    symbol_key: &'a str,
    role: &'a str,
    status: &'a str,
    exchange_order_id: Option<&'a str>,
    client_order_id_short: &'a str,
    position_id: &'a str,
}

async fn seed_order(store: &Store, seed: OrderSeed<'_>) {
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, \
         side, order_type, quantity, reduce_only, order_role, status, position_id, \
         created_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, 'binance', 'futures', ?, 'close_long', 'long', 'market', \
         '1', 1, ?, ?, ?, 1, 1)",
    )
    .bind(seed.engine_order_id)
    .bind(seed.credential_id)
    .bind(seed.bot_uid)
    .bind(format!("idem-{}", seed.engine_order_id))
    .bind(seed.client_order_id_short)
    .bind(seed.exchange_order_id)
    .bind(seed.symbol_key)
    .bind(seed.role)
    .bind(seed.status)
    .bind(seed.position_id)
    .execute(store.pool())
    .await
    .expect("seed engine order");
}

struct ProtectionSeed<'a> {
    protection_order_id: &'a str,
    position_id: &'a str,
    credential_id: &'a str,
    engine_order_id: &'a str,
    generation: i64,
    exchange_algo_id: Option<&'a str>,
    client_algo_id: Option<&'a str>,
    exchange_order_id: Option<&'a str>,
    status: &'a str,
}

async fn seed_protection(store: &Store, seed: ProtectionSeed<'_>) {
    sqlx::query(
        "INSERT INTO protection_orders (protection_order_id, position_id, credential_id, kind, \
         generation, engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id, \
         trigger_price, reduce_only, close_position, status, verified_from_exchange, \
         created_at_ms, updated_at_ms) \
         VALUES (?, ?, ?, 'sl', ?, ?, ?, ?, ?, '90', 1, 1, ?, 0, 1, 1)",
    )
    .bind(seed.protection_order_id)
    .bind(seed.position_id)
    .bind(seed.credential_id)
    .bind(seed.generation)
    .bind(seed.engine_order_id)
    .bind(seed.exchange_order_id)
    .bind(seed.exchange_algo_id)
    .bind(seed.client_algo_id)
    .bind(seed.status)
    .execute(store.pool())
    .await
    .expect("seed protection order");
}

/// A protection obligation owned by `bot_uid`: its engine order carries no
/// ordinary exchange id, so only the Algo domain can reach it.
async fn seed_algo_obligation(
    store: &Store,
    id: &str,
    credential_id: &str,
    bot_uid: &str,
    position_id: &str,
    generation: i64,
    exchange_algo_id: Option<&str>,
    client_algo_id: Option<&str>,
    status: &str,
) {
    let engine_order_id = format!("{id}-eng");
    seed_order(
        store,
        OrderSeed {
            engine_order_id: &engine_order_id,
            credential_id,
            bot_uid,
            symbol_key: BTC,
            role: "protection_sl",
            status: "acked",
            exchange_order_id: None,
            client_order_id_short: &format!("{id}-coid"),
            position_id,
        },
    )
    .await;
    seed_protection(
        store,
        ProtectionSeed {
            protection_order_id: id,
            position_id,
            credential_id,
            engine_order_id: &engine_order_id,
            generation,
            exchange_algo_id,
            client_algo_id,
            exchange_order_id: None,
            status,
        },
    )
    .await;
}

async fn protection_status(store: &Store, id: &str) -> String {
    sqlx::query("SELECT status FROM protection_orders WHERE protection_order_id=?")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .expect("read protection status")
        .get::<String, _>("status")
}

fn deletes(requests: &[RecordedRequest], path: &str) -> Vec<RecordedRequest> {
    requests
        .iter()
        .filter(|request| request.is("DELETE", path))
        .cloned()
        .collect()
}

/// Triggered-child fixture shared by the L3 and L2 proofs.
async fn seed_triggered_child_book(store: &Store, venue: &FakeVenue) {
    seed_position(store, "k04-pos-btc", CRED_A, BOT, BTC, true).await;
    seed_position(store, "k04-pos-eth-other", CRED_A, OTHER_BOT, ETH, true).await;
    // Owned entry: the ordinary sibling that has always been selected.
    seed_order(
        store,
        OrderSeed {
            engine_order_id: "k04-entry",
            credential_id: CRED_A,
            bot_uid: BOT,
            symbol_key: BTC,
            role: "entry",
            status: "acked",
            exchange_order_id: Some("7103"),
            client_order_id_short: "k04-entry-coid",
            position_id: "k04-pos-btc",
        },
    )
    .await;
    // Owned triggered SL: actualOrderId 7101 was projected by Algo truth.
    for (engine_order_id, bot_uid, symbol_key, position_id, actual, algo_id, client_algo, gen) in [
        (
            "k04-sl-eng",
            BOT,
            BTC,
            "k04-pos-btc",
            Some("7101"),
            "5101",
            "k04-sl-algo",
            1,
        ),
        // TRIGGERED without an actualOrderId: no authoritative ordinary id.
        (
            "k04-tp-eng",
            BOT,
            BTC,
            "k04-pos-btc",
            None,
            "5102",
            "k04-tp-algo",
            2,
        ),
        // Triggered child that already finished on the venue (sorts first):
        // its "unknown order" answer must not starve the siblings after it.
        (
            "k04-a-finished-eng",
            BOT,
            BTC,
            "k04-pos-btc",
            Some("7106"),
            "5105",
            "k04-finished-algo",
            3,
        ),
        // Another bot's triggered child on the same credential.
        (
            "k04-other-eng",
            OTHER_BOT,
            ETH,
            "k04-pos-eth-other",
            Some("7102"),
            "5103",
            "k04-other-algo",
            1,
        ),
    ] {
        seed_order(
            store,
            OrderSeed {
                engine_order_id,
                credential_id: CRED_A,
                bot_uid,
                symbol_key,
                role: "protection_sl",
                status: "triggered",
                exchange_order_id: actual,
                client_order_id_short: client_algo,
                position_id,
            },
        )
        .await;
        seed_protection(
            store,
            ProtectionSeed {
                protection_order_id: &format!("{engine_order_id}-po"),
                position_id,
                credential_id: CRED_A,
                engine_order_id,
                generation: gen,
                exchange_algo_id: Some(algo_id),
                client_algo_id: Some(client_algo),
                exchange_order_id: actual,
                status: "triggered",
            },
        )
        .await;
    }
    // Terminal ordinary protection rows keep their exchange id but must not
    // be broadened into the cancellable set.
    for (engine_order_id, status) in [
        ("k04-filled-eng", "filled"),
        ("k04-canceled-eng", "canceled"),
    ] {
        seed_order(
            store,
            OrderSeed {
                engine_order_id,
                credential_id: CRED_A,
                bot_uid: BOT,
                symbol_key: BTC,
                role: "protection_sl",
                status,
                exchange_order_id: Some(if status == "filled" { "7104" } else { "7105" }),
                client_order_id_short: engine_order_id,
                position_id: "k04-pos-btc",
            },
        )
        .await;
    }
    venue.with_book(|book| {
        book.open_orders = vec![
            open_order("BTCUSDT", 7101, "autoclose-k04-sl"),
            open_order("BTCUSDT", 7103, "k04-entry-coid"),
            open_order("ETHUSDT", 7102, "autoclose-k04-other"),
        ];
        let mut triggered = algo_order("BTCUSDT", 5101, "k04-sl-algo", "TRIGGERED");
        triggered["actualOrderId"] = serde_json::json!("7101");
        let mut finished = algo_order("BTCUSDT", 5105, "k04-finished-algo", "TRIGGERED");
        finished["actualOrderId"] = serde_json::json!("7106");
        let mut other = algo_order("ETHUSDT", 5103, "k04-other-algo", "TRIGGERED");
        other["actualOrderId"] = serde_json::json!("7102");
        book.algo_history = vec![
            triggered,
            algo_order("BTCUSDT", 5102, "k04-tp-algo", "TRIGGERED"),
            finished,
            other,
        ];
        book.positions = vec![
            crate::k04_fake_venue::position("BTCUSDT", "1"),
            crate::k04_fake_venue::position("ETHUSDT", "1"),
        ];
    });
}

fn assert_no_client_algo_id_as_ordinary(requests: &[RecordedRequest]) {
    for request in deletes(requests, "/fapi/v1/order") {
        let client = request.param("origClientOrderId").unwrap_or_default();
        assert!(
            ![
                "k04-sl-algo",
                "k04-tp-algo",
                "k04-other-algo",
                "k04-finished-algo"
            ]
            .contains(&client),
            "a clientAlgoId must never become an ordinary clientOrderId: {request:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k04_l3_bot_scope_cancels_triggered_child_by_actual_order_id_only() {
    let venue = FakeVenue::start().await;
    let _env = live_env(&venue);
    let (store, path) = sqlite_store("triggered-l3").await;
    seed_triggered_child_book(&store, &venue).await;

    let error = drive_live_scope_once(
        &store,
        "bot:bot-k04",
        Level::Flatten,
        &crate::exchange_filters::ExchangeFilterCache::new(),
    )
    .await
    .expect_err("another bot's child and position remain: L3 must not converge");

    let requests = venue.requests();
    let ordinary = deletes(&requests, "/fapi/v1/order");
    let child: Vec<_> = ordinary
        .iter()
        .filter(|request| request.param("orderId") == Some("7101"))
        .collect();
    assert_eq!(
        child.len(),
        1,
        "the owned triggered child must be cancelled by its actualOrderId: {ordinary:?}"
    );
    assert_eq!(child[0].param("symbol"), Some("BTCUSDT"));
    assert_eq!(
        child[0].param("origClientOrderId"),
        None,
        "the child cancel carries the authoritative orderId only"
    );
    assert_eq!(child[0].api_key, api_key(CRED_A));
    assert!(
        ordinary
            .iter()
            .any(|request| request.param("orderId") == Some("7103")),
        "the owned entry sibling is still cancelled: {ordinary:?}"
    );
    for foreign in ["7102", "7104", "7105"] {
        assert!(
            !ordinary
                .iter()
                .any(|request| request.param("orderId") == Some(foreign)),
            "orderId {foreign} is another bot's or terminal locally: {ordinary:?}"
        );
    }
    assert_no_client_algo_id_as_ordinary(&requests);
    venue.with_book(|book| {
        let open: Vec<i64> = book
            .open_orders
            .iter()
            .filter_map(|order| order["orderId"].as_i64())
            .collect();
        assert_eq!(
            open,
            vec![7102],
            "only the non-owned child is still working"
        );
    });
    assert!(
        ordinary
            .iter()
            .any(|request| request.param("orderId") == Some("7106")),
        "the finished child is attempted first by its actualOrderId: {ordinary:?}"
    );
    let message = format!("{error:#}");
    assert!(
        message.contains("k04-a-finished-eng")
            && message.contains("ordinary cancellation failures"),
        "the unknown-order answer stays visible: {message}"
    );
    assert!(
        message.contains("positions_zero=false"),
        "a remaining exchange position keeps L3 unconverged: {message}"
    );
    assert!(
        message.contains("orders_empty=false"),
        "remaining exchange open orders keep L3 unconverged: {message}"
    );
    drop_sqlite(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k04_l2_bot_scope_stays_entry_only_and_leaves_triggered_protection() {
    let venue = FakeVenue::start().await;
    let _env = live_env(&venue);
    let (store, path) = sqlite_store("triggered-l2").await;
    seed_triggered_child_book(&store, &venue).await;

    drive_live_scope_once(
        &store,
        "bot:bot-k04",
        Level::ManageOnly,
        &crate::exchange_filters::ExchangeFilterCache::new(),
    )
    .await
    .expect("L2 cancels the entry only");

    let requests = venue.requests();
    let ordinary = deletes(&requests, "/fapi/v1/order");
    assert_eq!(
        ordinary
            .iter()
            .map(|request| request.param("orderId").unwrap_or_default().to_string())
            .collect::<Vec<_>>(),
        vec!["7103".to_string()],
        "L2 must stay entry-only: {requests:?}"
    );
    assert!(deletes(&requests, "/fapi/v1/algoOrder").is_empty());
    assert!(
        !requests.iter().any(|request| request.method == "POST"),
        "L2 never flattens: {requests:?}"
    );
    drop_sqlite(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k04_l3_bot_scope_cancels_client_algo_only_obligations_and_isolates_failures() {
    let venue = FakeVenue::start().await;
    let _env = live_env(&venue);
    let (store, path) = sqlite_store("client-algo-only").await;
    seed_position(&store, "k04-pos-a", CRED_A, BOT, BTC, true).await;
    seed_position(&store, "k04-pos-closed", CRED_A, BOT, BTC, false).await;
    seed_position(&store, "k04-pos-other", CRED_A, OTHER_BOT, ETH, true).await;
    seed_position(&store, "k04-pos-b", CRED_B, BOT, BTC, true).await;
    // Sorted first: a venue failure here must not starve the rest.
    seed_algo_obligation(
        &store,
        "k04-a-fail",
        CRED_A,
        BOT,
        "k04-pos-a",
        1,
        Some("5201"),
        Some("k04-fail-algo"),
        "confirmed",
    )
    .await;
    // Interrupted submit; its position is locally closed, which is not authority to skip it.
    seed_algo_obligation(
        &store,
        "k04-b-client-only",
        CRED_A,
        BOT,
        "k04-pos-closed",
        1,
        None,
        Some("k04-client-only"),
        "requested",
    )
    .await;
    seed_algo_obligation(
        &store,
        "k04-c-sibling",
        CRED_A,
        BOT,
        "k04-pos-a",
        2,
        None,
        Some("k04-sibling"),
        "requested",
    )
    .await;
    // Unknown to the venue: an unknown-order answer is not authority to retire it.
    seed_algo_obligation(
        &store,
        "k04-d-gone",
        CRED_A,
        BOT,
        "k04-pos-a",
        3,
        None,
        Some("k04-gone"),
        "requested",
    )
    .await;
    // No typed identity at all: must stay a visible failure, never a success.
    seed_algo_obligation(
        &store,
        "k04-e-noid",
        CRED_A,
        BOT,
        "k04-pos-a",
        4,
        None,
        None,
        "requested",
    )
    .await;
    seed_algo_obligation(
        &store,
        "k04-other",
        CRED_A,
        OTHER_BOT,
        "k04-pos-other",
        1,
        None,
        Some("k04-other-client"),
        "requested",
    )
    .await;
    seed_algo_obligation(
        &store,
        "k04-credb",
        CRED_B,
        BOT,
        "k04-pos-b",
        1,
        None,
        Some("k04-credb-client"),
        "requested",
    )
    .await;
    venue.with_book(|book| {
        book.open_algos = vec![
            algo_order("BTCUSDT", 5201, "k04-fail-algo", "NEW"),
            algo_order("BTCUSDT", 5202, "k04-client-only", "NEW"),
            algo_order("BTCUSDT", 5203, "k04-sibling", "NEW"),
            algo_order("ETHUSDT", 5204, "k04-other-client", "NEW"),
            algo_order("BTCUSDT", 5205, "k04-credb-client", "NEW"),
        ];
        book.failing_algo_cancels.insert("algoId:5201".to_string());
    });

    let error = drive_live_scope_once(
        &store,
        "bot:bot-k04",
        Level::Flatten,
        &crate::exchange_filters::ExchangeFilterCache::new(),
    )
    .await
    .expect_err("an Algo order is still working: L3 must stay a visible failure");

    let requests = venue.requests();
    let algo_deletes = deletes(&requests, "/fapi/v1/algoOrder");
    let sent = |credential_id: &str, name: &str, value: &str| {
        algo_deletes.iter().any(|request| {
            request.api_key == api_key(credential_id) && request.param(name) == Some(value)
        })
    };
    assert!(
        sent(CRED_A, "algoId", "5201"),
        "the failing obligation is attempted: {algo_deletes:?}"
    );
    for client in ["k04-client-only", "k04-sibling", "k04-gone"] {
        assert!(
            sent(CRED_A, "clientAlgoId", client),
            "clientAlgo-only obligation {client} must be cancelled by its typed identity: {algo_deletes:?}"
        );
    }
    assert!(
        algo_deletes
            .iter()
            .filter(|request| request.param("clientAlgoId").is_some())
            .all(|request| request.param("algoId").is_none()),
        "a clientAlgoId cancel must not invent an algoId: {algo_deletes:?}"
    );
    assert!(
        !algo_deletes
            .iter()
            .any(|request| request.param("clientAlgoId") == Some("k04-other-client")),
        "another bot's obligation is never cancelled from this bot scope: {algo_deletes:?}"
    );
    assert!(
        !algo_deletes.iter().any(|request| {
            request.api_key == api_key(CRED_A)
                && request.param("clientAlgoId") == Some("k04-credb-client")
        }),
        "credential B's obligation is never sent with credential A's key: {algo_deletes:?}"
    );
    assert!(
        sent(CRED_B, "clientAlgoId", "k04-credb-client"),
        "the bot's obligation under credential B is cancelled with credential B: {algo_deletes:?}"
    );
    assert!(
        !deletes(&requests, "/fapi/v1/order")
            .iter()
            .any(|request| request
                .param("origClientOrderId")
                .is_some_and(|id| id.starts_with("k04-"))),
        "no Algo identity may reach ordinary cancellation: {requests:?}"
    );
    venue.with_book(|book| {
        let mut open: Vec<i64> = book
            .open_algos
            .iter()
            .filter_map(|algo| algo["algoId"].as_i64())
            .collect();
        open.sort();
        assert_eq!(
            open,
            vec![5201, 5204],
            "failed and non-owned Algo orders remain"
        );
    });
    // Unknown / identity-less obligations are not retired locally.
    assert_eq!(protection_status(&store, "k04-d-gone").await, "requested");
    assert_eq!(protection_status(&store, "k04-e-noid").await, "requested");
    let message = format!("{error:#}");
    assert!(message.contains("algo_orders_empty=false"), "{message}");
    assert!(message.contains("cancel_failed=true"), "{message}");
    for visible in ["5201", "k04-gone", "k04-e-noid"] {
        assert!(
            message.contains(visible),
            "unresolved obligation {visible} must stay visible in the failure: {message}"
        );
    }
    drop_sqlite(store, path).await;
}

/// Completed production scope-driver attempt: ordinary failures remain in its
/// returned error, while every item from the successful openOrders read runs.
async fn k04_b_assert_open_orders_failure_isolation(global: bool) {
    let venue = FakeVenue::start().await;
    let _env = live_env(&venue);
    let (store, path) = sqlite_store("ordinary-failures").await;
    seed_position(&store, "k04b-pos", CRED_A, BOT, BTC, false).await;
    if !global {
        seed_position(&store, "k04b-foreign", CRED_B, BOT, BTC, false).await;
    }
    for (id, order_id) in [("first", "8101"), ("next", "8102"), ("last", "8103")] {
        seed_order(
            &store,
            OrderSeed {
                engine_order_id: &format!("k04b-{id}"),
                credential_id: CRED_A,
                bot_uid: BOT,
                symbol_key: BTC,
                role: "exit",
                status: "acked",
                exchange_order_id: Some(order_id),
                client_order_id_short: &format!("k04b-{id}"),
                position_id: "k04b-pos",
            },
        )
        .await;
    }
    venue.with_book(|book| {
        book.open_orders = vec![
            open_order("BTCUSDT", 8101, "k04b-first"),
            open_order("BTCUSDT", 8102, "k04b-next"),
            open_order("BTCUSDT", 8103, "k04b-last"),
        ];
        book.failing_order_cancels
            .extend(["8101".to_string(), "8103".to_string()]);
    });
    let scope_key = if global {
        "global".to_string()
    } else {
        format!("cred:{CRED_A}")
    };
    let error = drive_live_scope_once(
        &store,
        &scope_key,
        Level::Flatten,
        &crate::exchange_filters::ExchangeFilterCache::new(),
    )
    .await
    .expect_err("fixed venue failures and still-working ordinary orders must stay visible");
    let requests = venue.requests();
    let ordinary = deletes(&requests, "/fapi/v1/order");
    let sent: Vec<_> = ordinary
        .iter()
        .map(|r| r.param("orderId").unwrap_or_default())
        .collect();
    let message = format!("{error:#}");
    println!("K04-B completed scope attempt scope={scope_key} orderIds={sent:?} error={message}");
    // Preserve cleanup even on the deliberately red old-candidate assertions.
    drop_sqlite(store, path).await;
    assert_eq!(
        sent,
        vec!["8101", "8102", "8103"],
        "a rejected first order must not starve later ordinary orders"
    );
    assert!(
        ordinary.iter().all(|r| r.api_key == api_key(CRED_A)),
        "{ordinary:?}"
    );
    assert!(
        !requests.iter().any(|r| r.api_key == api_key(CRED_B)),
        "credential scope must not visit the other credential: {requests:?}"
    );
    for expected in [
        "ordinary cancellation failures",
        "8101",
        "8103",
        "cancel_failed=true",
        "orders_empty=false",
    ] {
        assert!(
            message.contains(expected),
            "missing {expected:?} from completed attempt error: {message}"
        );
    }
    venue.with_book(|book| {
        assert_eq!(
            book.open_orders
                .iter()
                .map(|o| o["orderId"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![8101, 8103]
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k04_b_credential_open_orders_continues_and_reports_all_errors() {
    k04_b_assert_open_orders_failure_isolation(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k04_b_global_open_orders_continues_and_reports_all_errors() {
    k04_b_assert_open_orders_failure_isolation(true).await;
}
