//! K07 real-MySQL pool lifecycle proofs through the application's SQLx graph (the vendored,
//! patched sqlx-core). A loopback proxy withholds one connection's release COM_PING OK reply (or
//! a new connection's greeting); the oracle checks pool counters, the proxy's client EOF / relay
//! exit, and the server PROCESSLIST separately. Every key observation happens *before* any held
//! reply is released, unless the scenario is precisely about a release before the deadline.
//!
//! Requires `RUSTCORE_TEST_MYSQL_URL` (task-owned MySQL on 127.0.0.1). With
//! `RUSTCORE_REQUIRE_MYSQL_TESTS=1` a missing fixture fails instead of skipping.

#[path = "support/mysql_ping_proxy.rs"]
mod mysql_ping_proxy;

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Once;
use std::time::{Duration, Instant};

use mysql_ping_proxy::PingProxy;
use sqlx::any::AnyPoolOptions;
use sqlx::pool::PoolConnection;
use sqlx::{Any, Pool, Row};

const MYSQL_URL_ENV: &str = "RUSTCORE_TEST_MYSQL_URL";
const REQUIRE_MYSQL_ENV: &str = "RUSTCORE_REQUIRE_MYSQL_TESTS";

/// Frozen K07 budgets and observation windows (not widened after failures).
const T_RELEASE: Duration = Duration::from_millis(5000);
const MARGIN: Duration = Duration::from_millis(1500);
const SERVER_WINDOW: Duration = Duration::from_millis(2000);
const SETTLE: Duration = Duration::from_millis(300);

static INSTALL_DRIVERS: Once = Once::new();

struct Fixture {
    test: &'static str,
    admin_url: url::Url,
    proxy: PingProxy,
    observer: Pool<Any>,
}

async fn fixture(test: &'static str) -> Option<Fixture> {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let required = std::env::var(REQUIRE_MYSQL_ENV).is_ok_and(|v| v.trim() == "1");
    let raw = match std::env::var(MYSQL_URL_ENV) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_owned(),
        _ => {
            assert!(
                !required,
                "{test}: {REQUIRE_MYSQL_ENV}=1 but {MYSQL_URL_ENV} is unset"
            );
            eprintln!("SKIPPED {test}: {MYSQL_URL_ENV} is unset; no substitute backend is used");
            return None;
        }
    };
    let admin_url = url::Url::parse(&raw).expect("MySQL test URL must parse");
    assert_eq!(admin_url.scheme(), "mysql", "{test}: fixture must be MySQL");
    let host: IpAddr = admin_url
        .host_str()
        .and_then(|h| h.parse().ok())
        .expect("fixture host must be an IP literal");
    assert!(
        host.is_loopback(),
        "{test}: fixture must be a loopback server"
    );
    let upstream = SocketAddr::new(host, admin_url.port().unwrap_or(3306));
    let observer = match AnyPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(admin_url.as_str())
        .await
    {
        Ok(pool) => pool,
        Err(_) if required => {
            panic!("{test}: {REQUIRE_MYSQL_ENV}=1 but the fixture is unreachable")
        }
        Err(_) => {
            eprintln!("SKIPPED {test}: the configured MySQL fixture is unreachable");
            return None;
        }
    };
    let proxy = PingProxy::start(upstream)
        .await
        .expect("start loopback proxy");
    Some(Fixture {
        test,
        admin_url,
        proxy,
        observer,
    })
}

impl Fixture {
    fn proxied_url(&self) -> String {
        let mut url = self.admin_url.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(self.proxy.addr().port())).unwrap();
        url.set_query(Some("ssl-mode=disabled"));
        url.to_string()
    }

    fn pool(&self, max: u32) -> Pool<Any> {
        AnyPoolOptions::new()
            .max_connections(max)
            .min_connections(0)
            .acquire_timeout(Duration::from_millis(500))
            .connect_lazy(&self.proxied_url())
            .expect("lazy pool")
    }

    async fn server_has(&self, id: u32) -> bool {
        let row = sqlx::query(
            "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.PROCESSLIST WHERE ID = ?",
        )
        .bind(i64::from(id))
        .fetch_one(&self.observer)
        .await
        .expect("PROCESSLIST probe");
        row.get::<i64, _>(0) > 0
    }

    /// Fixed remote window: the server thread must disappear within SERVER_WINDOW of `from`.
    async fn server_gone(&self, id: u32, from: Instant) -> Duration {
        loop {
            if !self.server_has(id).await {
                return from.elapsed();
            }
            assert!(
                from.elapsed() <= SERVER_WINDOW,
                "{}: server thread {id} still present {SERVER_WINDOW:?} after local release",
                self.test
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn finish(self) {
        let cleanup = self.proxy.shutdown().await;
        self.observer.close().await;
        println!(
            "K07_EVIDENCE {}",
            serde_json::json!({
                "test": self.test,
                "proxy_accepted": cleanup.accepted,
                "proxy_relays_active_after_wait": cleanup.relays_active_after_wait,
                "proxy_relays_force_aborted": cleanup.relays_force_aborted,
                "proxy_listener_closed": cleanup.listener_closed,
            })
        );
        assert_eq!(
            cleanup.relays_active_after_wait, 0,
            "{}: proxy relays left",
            self.test
        );
        assert_eq!(
            cleanup.relays_force_aborted, 0,
            "{}: proxy relays forced",
            self.test
        );
        assert!(cleanup.listener_closed);
    }
}

fn evidence(test: &str, data: serde_json::Value) {
    println!(
        "K07_EVIDENCE {}",
        serde_json::json!({ "test": test, "data": data })
    );
}

async fn within<F: Future>(what: &str, bound: Duration, f: F) -> F::Output {
    match tokio::time::timeout(bound, f).await {
        Ok(output) => output,
        Err(_) => panic!("{what}: not finished within {bound:?}"),
    }
}

async fn until(what: &str, deadline: Instant, cond: impl Fn() -> bool) -> Instant {
    loop {
        if cond() {
            return Instant::now();
        }
        assert!(
            Instant::now() <= deadline,
            "{what}: not reached by its deadline"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn mysql_id(conn: &mut PoolConnection<Any>) -> u32 {
    let row = sqlx::query("SELECT CAST(CONNECTION_ID() AS SIGNED)")
        .fetch_one(&mut **conn)
        .await
        .expect("CONNECTION_ID");
    u32::try_from(row.get::<i64, _>(0)).expect("thread id fits u32")
}

async fn select_one(conn: &mut PoolConnection<Any>) -> i64 {
    sqlx::query("SELECT CAST(1 AS SIGNED)")
        .fetch_one(&mut **conn)
        .await
        .expect("SELECT 1")
        .get::<i64, _>(0)
}

/// Healthy SELECT, release ping and same-id reuse, then an explicitly held connection whose
/// release ping reply the proxy withholds. Returns (held id, drop instant).
async fn hold_release_ping(fx: &Fixture, pool: &Pool<Any>) -> (u32, Instant) {
    let mut conn = pool.acquire().await.expect("acquire");
    assert_eq!(select_one(&mut conn).await, 1);
    let id = mysql_id(&mut conn).await;
    drop(conn);
    until("healthy release", Instant::now() + MARGIN, || {
        pool.num_idle() == 1
    })
    .await;
    let mut conn = pool.acquire().await.expect("reacquire");
    assert_eq!(
        mysql_id(&mut conn).await,
        id,
        "healthy connection is reused"
    );
    fx.proxy.hold_next_ping_reply(id);
    let dropped_at = Instant::now();
    drop(conn);
    fx.proxy
        .wait_for("release ping reply withheld", MARGIN, |c| {
            c.iter().any(|c| c.mysql_id == Some(id) && c.held_now == 1)
        })
        .await;
    assert_eq!((pool.size(), pool.num_idle()), (1, 0));
    (id, dropped_at)
}

/// T01
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t01_healthy_release_pings_and_reuses_the_server_connection() {
    const TEST: &str = "k07_real_t01_healthy_release_pings_and_reuses_the_server_connection";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(1);
    let mut ids = Vec::new();
    for _ in 0..3 {
        let mut conn = within("acquire", MARGIN, pool.acquire())
            .await
            .expect("acquire");
        assert_eq!(select_one(&mut conn).await, 1);
        let id = mysql_id(&mut conn).await;
        ids.push(id);
        let pings_before = fx.proxy.by_mysql_id(id).pings;
        drop(conn);
        until("release ping + idle", Instant::now() + MARGIN, || {
            pool.num_idle() == 1 && fx.proxy.by_mysql_id(id).pings == pings_before + 1
        })
        .await;
    }
    assert!(
        ids.windows(2).all(|w| w[0] == w[1]),
        "same server connection: {ids:?}"
    );
    let snap = fx.proxy.snapshot();
    assert_eq!(snap.len(), 1, "no reconnect on healthy returns");
    assert_eq!(snap[0].pings, 5, "3 release pings + 2 acquire pings");
    assert_eq!((pool.size(), pool.num_idle()), (1, 1));
    within("close", MARGIN, pool.close()).await;
    let gone = fx.server_gone(ids[0], Instant::now()).await;
    evidence(
        TEST,
        serde_json::json!({"ids": ids, "pings": snap[0].pings, "server_gone_ms": gone.as_millis()}),
    );
    fx.finish().await;
}

/// T02: withheld release ping, no close: the pool drops the connection at T_release, the server
/// thread disappears, and a new connection works. The held reply is never forwarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t02_withheld_release_ping_is_dropped_and_capacity_recovers() {
    const TEST: &str = "k07_real_t02_withheld_release_ping_is_dropped_and_capacity_recovers";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(1);
    let (id, dropped_at) = hold_release_ping(&fx, &pool).await;
    let acquire_started = Instant::now();
    assert!(
        matches!(pool.acquire().await, Err(sqlx::Error::PoolTimedOut)),
        "capacity is held while the return is pending"
    );
    let acquire_ms = acquire_started.elapsed().as_millis();
    let zero_at = until("size back to zero", dropped_at + T_RELEASE + MARGIN, || {
        pool.size() == 0
    })
    .await;
    assert!(
        zero_at.duration_since(dropped_at) >= T_RELEASE,
        "dropped before the deadline"
    );
    let relay_done = fx
        .proxy
        .wait_for("client EOF and relay exit", MARGIN, |c| {
            c.iter().any(|c| {
                c.mysql_id == Some(id) && c.client_eof_at.is_some() && c.relay_done_at.is_some()
            })
        })
        .await;
    let old = fx.proxy.by_mysql_id(id);
    assert_eq!(
        (old.held_total, old.released_total),
        (1, 0),
        "held reply never forwarded"
    );
    let gone = fx.server_gone(id, zero_at).await;
    let mut fresh = within("acquire after recovery", MARGIN, pool.acquire())
        .await
        .expect("acquire");
    let fresh_id = mysql_id(&mut fresh).await;
    assert_ne!(fresh_id, id);
    assert_eq!(select_one(&mut fresh).await, 1);
    drop(fresh);
    until("fresh idle", Instant::now() + MARGIN, || {
        pool.num_idle() == 1
    })
    .await;
    within("close", MARGIN, pool.close()).await;
    fx.server_gone(fresh_id, Instant::now()).await;
    assert_eq!(fx.proxy.by_mysql_id(id).released_total, 0);
    evidence(
        TEST,
        serde_json::json!({
            "held_id": id, "fresh_id": fresh_id,
            "acquire_timed_out_ms": acquire_ms,
            "size_zero_after_drop_ms": zero_at.duration_since(dropped_at).as_millis(),
            "relay_done_after_drop_ms": relay_done.duration_since(dropped_at).as_millis(),
            "server_gone_after_size_zero_ms": gone.as_millis(),
        }),
    );
    fx.finish().await;
}

/// T03: one close started while the release ping reply is withheld returns by T_release+margin
/// with everything released; the held reply is still withheld when this is observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t03_first_close_during_withheld_ping_releases_everything() {
    const TEST: &str = "k07_real_t03_first_close_during_withheld_ping_releases_everything";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(1);
    let (id, dropped_at) = hold_release_ping(&fx, &pool).await;
    let close_started = Instant::now();
    let closer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.close().await;
            Instant::now()
        })
    };
    tokio::time::sleep(SETTLE).await;
    assert!(
        !closer.is_finished(),
        "close waits for the connection being returned"
    );
    let bound = (dropped_at + T_RELEASE + MARGIN).saturating_duration_since(Instant::now());
    let returned = within("first close", bound, closer).await.unwrap();
    assert!(returned.duration_since(dropped_at) >= T_RELEASE);
    assert!(pool.is_closed());
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    let old = fx.proxy.by_mysql_id(id);
    assert_eq!(
        (old.held_total, old.released_total),
        (1, 0),
        "observed before any release"
    );
    let relay_done = fx
        .proxy
        .wait_for("client EOF and relay exit", MARGIN, |c| {
            c.iter().any(|c| {
                c.mysql_id == Some(id) && c.client_eof_at.is_some() && c.relay_done_at.is_some()
            })
        })
        .await;
    let gone = fx.server_gone(id, returned).await;
    evidence(
        TEST,
        serde_json::json!({
            "held_id": id,
            "close_started_after_drop_ms": close_started.duration_since(dropped_at).as_millis(),
            "first_close_returned_after_drop_ms": returned.duration_since(dropped_at).as_millis(),
            "relay_done_after_close_ms": relay_done.saturating_duration_since(returned).as_millis(),
            "server_gone_after_close_ms": gone.as_millis(),
            "second_close_called": false,
        }),
    );
    fx.finish().await;
}

/// T04: the withheld reply is released before the deadline while close waits; the same close
/// completes promptly and nothing is left idle or on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t04_release_before_deadline_overlapping_close() {
    const TEST: &str = "k07_real_t04_release_before_deadline_overlapping_close";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(1);
    let (id, dropped_at) = hold_release_ping(&fx, &pool).await;
    let closer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.close().await;
            Instant::now()
        })
    };
    tokio::time::sleep(SETTLE).await;
    assert!(!closer.is_finished());
    let released_at = Instant::now();
    fx.proxy.release_held();
    let returned = within("close after release", MARGIN, closer).await.unwrap();
    assert!(
        returned.duration_since(dropped_at) < T_RELEASE,
        "healthy late return is not held to the deadline"
    );
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    assert_eq!(fx.proxy.by_mysql_id(id).released_total, 1);
    let gone = fx.server_gone(id, returned).await;
    evidence(
        TEST,
        serde_json::json!({
            "held_id": id,
            "released_after_drop_ms": released_at.duration_since(dropped_at).as_millis(),
            "close_returned_after_release_ms": returned.duration_since(released_at).as_millis(),
            "server_gone_after_close_ms": gone.as_millis(),
        }),
    );
    fx.finish().await;
}

/// T06: one idle and one held connection; close stays pending while the business holds its
/// connection and completes once it is returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t06_close_pending_while_business_holds_a_connection() {
    const TEST: &str = "k07_real_t06_close_pending_while_business_holds_a_connection";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(2);
    let mut a = pool.acquire().await.expect("a");
    let mut b = pool.acquire().await.expect("b");
    let (id_a, id_b) = (mysql_id(&mut a).await, mysql_id(&mut b).await);
    assert_ne!(id_a, id_b);
    drop(a);
    until("a idle", Instant::now() + MARGIN, || pool.num_idle() == 1).await;
    let closer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.close().await;
            Instant::now()
        })
    };
    let close_started = Instant::now();
    fx.server_gone(id_a, close_started).await;
    tokio::time::sleep(SETTLE).await;
    assert!(
        !closer.is_finished(),
        "close must wait for the held connection"
    );
    assert_eq!((pool.size(), pool.num_idle()), (1, 0));
    assert!(fx.server_has(id_b).await);
    assert_eq!(select_one(&mut b).await, 1, "held connection keeps working");
    let released = Instant::now();
    drop(b);
    let returned = within("close after held returns", MARGIN, closer)
        .await
        .unwrap();
    let gone = fx.server_gone(id_b, returned).await;
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    evidence(
        TEST,
        serde_json::json!({
            "idle_id": id_a, "held_id": id_b,
            "close_returned_after_held_release_ms": returned.duration_since(released).as_millis(),
            "held_server_gone_after_close_ms": gone.as_millis(),
        }),
    );
    fx.finish().await;
}

/// T07 (real part): two closers while a returned connection's ping reply is withheld; neither
/// returns before that connection is dropped at its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t07_two_closers_wait_for_the_withheld_return() {
    const TEST: &str = "k07_real_t07_two_closers_wait_for_the_withheld_return";
    let Some(fx) = fixture(TEST).await else {
        return;
    };
    let pool = fx.pool(2);
    let mut a = pool.acquire().await.expect("a");
    let id_a = mysql_id(&mut a).await;
    let (id_b, dropped_at) = {
        // Hold `a` so that the held connection is a distinct server thread.
        let result = hold_release_ping_distinct(&fx, &pool).await;
        drop(a);
        result
    };
    until("a idle", Instant::now() + MARGIN, || pool.num_idle() == 1).await;
    let spawn_close = || {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.close().await;
            Instant::now()
        })
    };
    let (c1, c2) = (spawn_close(), spawn_close());
    tokio::time::sleep(SETTLE).await;
    assert!(!c1.is_finished() && !c2.is_finished());
    let bound = (dropped_at + T_RELEASE + MARGIN).saturating_duration_since(Instant::now());
    let r1 = within("closer 1", bound, c1).await.unwrap();
    let r2 = within("closer 2", bound + SETTLE, c2).await.unwrap();
    for r in [r1, r2] {
        assert!(
            r.duration_since(dropped_at) >= T_RELEASE,
            "a closer returned before the held raw was dropped"
        );
    }
    assert_eq!(fx.proxy.by_mysql_id(id_b).released_total, 0);
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    let first = r1.min(r2);
    fx.server_gone(id_a, first).await;
    fx.server_gone(id_b, first).await;
    evidence(
        TEST,
        serde_json::json!({
            "idle_id": id_a, "held_id": id_b,
            "closer1_after_drop_ms": r1.duration_since(dropped_at).as_millis(),
            "closer2_after_drop_ms": r2.duration_since(dropped_at).as_millis(),
        }),
    );
    fx.finish().await;
}

/// Like `hold_release_ping` but for a pool where another connection is already checked out.
async fn hold_release_ping_distinct(fx: &Fixture, pool: &Pool<Any>) -> (u32, Instant) {
    let mut conn = pool.acquire().await.expect("acquire second");
    assert_eq!(select_one(&mut conn).await, 1);
    let id = mysql_id(&mut conn).await;
    fx.proxy.hold_next_ping_reply(id);
    let dropped_at = Instant::now();
    drop(conn);
    fx.proxy
        .wait_for("release ping reply withheld", MARGIN, |c| {
            c.iter().any(|c| c.mysql_id == Some(id) && c.held_now == 1)
        })
        .await;
    (id, dropped_at)
}

/// T13 (loopback): close while a counted connection is still being created (greeting withheld).
async fn in_flight_connect(test: &'static str, cancel: bool) {
    let Some(fx) = fixture(test).await else {
        return;
    };
    fx.proxy.hold_greetings(true);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(20))
        .connect_lazy(&fx.proxied_url())
        .expect("lazy pool");
    let acquirer = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.acquire().await })
    };
    fx.proxy
        .wait_for("greeting withheld", MARGIN, |c| {
            c.len() == 1 && c[0].held_now == 1
        })
        .await;
    let id = fx.proxy.snapshot()[0].mysql_id.expect("greeting parsed");
    assert_eq!(pool.size(), 1, "the in-flight creation is counted");
    let closer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.close().await;
            Instant::now()
        })
    };
    tokio::time::sleep(SETTLE).await;
    assert!(
        !closer.is_finished(),
        "close waits for the counted creation"
    );
    fx.proxy.hold_greetings(false);
    let outcome_at = Instant::now();
    if cancel {
        acquirer.abort();
        assert!(acquirer.await.unwrap_err().is_cancelled());
    } else {
        fx.proxy.release_held();
        let mut conn = within("acquire", MARGIN, acquirer)
            .await
            .unwrap()
            .expect("creation accepted before close reaches the caller");
        assert_eq!(mysql_id(&mut conn).await, id);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !closer.is_finished(),
            "close waits while the business holds it"
        );
        assert_eq!((pool.size(), pool.num_idle()), (1, 0));
        drop(conn);
    }
    let returned = within("close", MARGIN, closer).await.unwrap();
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    fx.proxy
        .wait_for("relay exit", MARGIN, |c| c[0].relay_done_at.is_some())
        .await;
    let gone = fx.server_gone(id, returned).await;
    evidence(
        test,
        serde_json::json!({
            "id": id, "cancelled": cancel,
            "close_returned_after_outcome_ms": returned.duration_since(outcome_at).as_millis(),
            "server_gone_after_close_ms": gone.as_millis(),
        }),
    );
    fx.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t13a_close_with_connect_in_flight_that_succeeds() {
    in_flight_connect(
        "k07_real_t13a_close_with_connect_in_flight_that_succeeds",
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_real_t13b_close_with_connect_in_flight_that_is_cancelled() {
    in_flight_connect(
        "k07_real_t13b_close_with_connect_in_flight_that_is_cancelled",
        true,
    )
    .await;
}

/// T17 (SQLite part): file-backed pool read/write, return, reuse and close through the patched
/// pool. This does not substitute for the MySQL Any regressions in store.rs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k07_t17_sqlite_file_pool_read_write_return_close() {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("k07-sqlite-{}.db", ulid::Ulid::new()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("sqlite pool");
    sqlx::query("CREATE TABLE k07 (v INTEGER NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("INSERT INTO k07 (v) VALUES (7)")
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    until("sqlite idle", Instant::now() + MARGIN, || {
        pool.num_idle() == 1
    })
    .await;
    let row = sqlx::query("SELECT v FROM k07")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>(0), 7);
    // The return runs in a spawned task; wait for it rather than sampling mid-flight.
    until("sqlite reused idle", Instant::now() + MARGIN, || {
        pool.num_idle() == 1
    })
    .await;
    assert_eq!(
        (pool.size(), pool.num_idle()),
        (1, 1),
        "healthy connection reused"
    );
    within("sqlite close", MARGIN, pool.close()).await;
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    let reopened = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let row = sqlx::query("SELECT COUNT(*) FROM k07")
        .fetch_one(&reopened)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>(0), 1);
    reopened.close().await;
    let _ = std::fs::remove_file(&path);
}
