//! K07 local patch: deterministic pool lifecycle tests (see PATCHES.md).
//!
//! `cfg(test)` only. The application never compiles this module; the independent root
//! `tests/gate/k07-pool-lifecycle/internal-harness` builds the vendored crate as a test target.
//! A scripted `Any` backend stands in for the driver so connects, pings and closes can be held
//! Pending, gated or failed on demand, and hooks at the lifecycle gate force both orders of each
//! race. Every wait is bounded; a hang is reported as a failed assertion.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, PoisonError, Weak};
use std::thread;
use std::time::{Duration, Instant};

use either::Either;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use once_cell::sync::Lazy;

use super::{PoolInner, CLOSE_IDLE_TIMEOUT};
use crate::any::driver::{install_drivers, AnyDriver};
use crate::any::{
    Any, AnyArguments, AnyConnectOptions, AnyConnection, AnyConnectionBackend, AnyQueryResult,
    AnyRow, AnyStatement, AnyTypeInfo,
};
use crate::common::DebugFn;
use crate::describe::Describe;
use crate::error::Error;
use crate::pool::connection::RETURN_TO_POOL_TIMEOUT;
use crate::pool::{Pool, PoolOptions};

// ---------------------------------------------------------------------------------------------
// Hooks (fired from `#[cfg(test)]` statements in inner.rs)
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    CloseLoopTop,
    CloseAfterListen,
    CloseBeforeSizeCheck,
    CloseBeforeWait,
    ReleaseBeforePublish,
    ReleaseAfterPush,
    SizeBeforeIncrement,
}

type Hook = Arc<dyn Fn(Point) + Send + Sync>;

#[derive(Default)]
pub(super) struct Hooks(Mutex<Option<Hook>>);

impl Hooks {
    pub(super) fn fire(&self, point: Point) {
        let hook = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(point);
        }
    }

    fn set(&self, hook: impl Fn(Point) + Send + Sync + 'static) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(hook));
    }

    fn clear(&self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// One-shot pause: the first thread reaching `point` reports arrival and blocks (bounded)
/// until resumed. Used while the lifecycle gate is held to prove who waits for whom.
struct Pause {
    point: Point,
    armed: AtomicBool,
    reached: Mutex<mpsc::Sender<()>>,
    resume: Mutex<mpsc::Receiver<()>>,
}

struct PauseCtl {
    reached: mpsc::Receiver<()>,
    resume: mpsc::Sender<()>,
}

impl Pause {
    fn new(point: Point) -> (Arc<Self>, PauseCtl) {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let pause = Arc::new(Self {
            point,
            armed: AtomicBool::new(true),
            reached: Mutex::new(reached_tx),
            resume: Mutex::new(resume_rx),
        });
        (
            pause,
            PauseCtl {
                reached: reached_rx,
                resume: resume_tx,
            },
        )
    }

    fn hit(&self, point: Point) {
        if point == self.point && self.armed.swap(false, Ordering::SeqCst) {
            let _ = self.reached.lock().unwrap().send(());
            let _ = self.resume.lock().unwrap().recv_timeout(BOUND);
        }
    }
}

impl PauseCtl {
    fn wait_reached(&self) {
        self.reached
            .recv_timeout(BOUND)
            .expect("hooked thread never reached the pause point");
    }

    fn resume(&self) {
        let _ = self.resume.send(());
    }
}

// ---------------------------------------------------------------------------------------------
// Scripted Any driver
// ---------------------------------------------------------------------------------------------

/// Frozen K07 budgets; the tests assert against these values, never against widened copies.
const T_RELEASE: Duration = Duration::from_millis(5000);
const T_IDLE_CLOSE: Duration = Duration::from_millis(5000);
/// Frozen scheduling margin for "finished by" bounds.
const MARGIN: Duration = Duration::from_millis(1500);
/// Observation window used to show that something is still pending.
const SETTLE: Duration = Duration::from_millis(300);
/// Upper bound for any wait that is expected to finish promptly.
const BOUND: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Ok,
    Err,
    Pending,
    /// Wait for `Script::open_gate`, then succeed or fail per `Script::gate_ok`.
    Gated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    ConnectStart,
    Connected,
    PingStart,
    PingDone,
    CloseStart,
    CloseHard,
    RawDrop,
}

#[derive(Clone, Copy, Debug)]
struct Record {
    ev: Ev,
    conn: u64,
    /// Pool size when the event happened (`None` once the pool is gone).
    size: Option<u32>,
    /// Whether the lifecycle gate was held by anyone at that moment.
    gate_held: bool,
    at: Instant,
}

struct Script {
    name: String,
    next_id: AtomicU64,
    connect: Mutex<Mode>,
    ping: Mutex<Mode>,
    close: Mutex<Mode>,
    gate: tokio::sync::Semaphore,
    gate_ok: AtomicBool,
    log: Mutex<Vec<Record>>,
    pool: Mutex<Weak<PoolInner<Any>>>,
}

static SCRIPTS: Lazy<Mutex<HashMap<String, Arc<Script>>>> = Lazy::new(Default::default);

impl Script {
    fn new(name: &str) -> Arc<Self> {
        let script = Arc::new(Self {
            name: name.to_owned(),
            next_id: AtomicU64::new(0),
            connect: Mutex::new(Mode::Ok),
            ping: Mutex::new(Mode::Ok),
            close: Mutex::new(Mode::Ok),
            gate: tokio::sync::Semaphore::new(0),
            gate_ok: AtomicBool::new(true),
            log: Mutex::new(Vec::new()),
            pool: Mutex::new(Weak::new()),
        });
        let previous = SCRIPTS
            .lock()
            .unwrap()
            .insert(name.to_owned(), script.clone());
        assert!(previous.is_none(), "duplicate script name {name}");
        script
    }

    fn set(slot: &Mutex<Mode>, mode: Mode) {
        *slot.lock().unwrap() = mode;
    }

    fn open_gate(&self, ok: bool) {
        self.gate_ok.store(ok, Ordering::SeqCst);
        self.gate.add_permits(1);
    }

    fn record(&self, ev: Ev, conn: u64) {
        // Take the snapshot and release the strong pool reference before locking the log: if this
        // happened to be the last reference, dropping the pool records more events.
        let (size, gate_held) = {
            let pool = self.pool.lock().unwrap().upgrade();
            let snapshot = pool
                .as_ref()
                .map(|p| (p.size(), p.lifecycle.try_lock().is_err()));
            drop(pool);
            snapshot.map_or((None, false), |(size, held)| (Some(size), held))
        };
        self.log.lock().unwrap().push(Record {
            ev,
            conn,
            size,
            gate_held,
            at: Instant::now(),
        });
    }

    fn records(&self, ev: Ev) -> Vec<Record> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.ev == ev)
            .copied()
            .collect()
    }

    fn count(&self, ev: Ev) -> usize {
        self.records(ev).len()
    }

    fn one(&self, ev: Ev, conn: u64) -> Record {
        let found: Vec<_> = self
            .records(ev)
            .into_iter()
            .filter(|r| r.conn == conn)
            .collect();
        assert_eq!(
            found.len(),
            1,
            "expected exactly one {ev:?} for connection {conn}: {found:?}"
        );
        found[0]
    }

    async fn wait(&self, mode: Mode) -> Result<(), Error> {
        match mode {
            Mode::Ok => Ok(()),
            Mode::Err => Err(Error::Protocol("k07 scripted failure".into())),
            Mode::Pending => std::future::pending().await,
            Mode::Gated => {
                self.gate.acquire().await.expect("gate").forget();
                if self.gate_ok.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(Error::Protocol("k07 scripted gated failure".into()))
                }
            }
        }
    }
}

struct MockConn {
    id: u64,
    script: Arc<Script>,
}

impl fmt::Debug for MockConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockConn").field("id", &self.id).finish()
    }
}

impl Drop for MockConn {
    fn drop(&mut self) {
        self.script.record(Ev::RawDrop, self.id);
    }
}

fn unsupported<'a, T: Send + 'a>() -> BoxFuture<'a, Result<T, Error>> {
    Box::pin(async { Err(Error::Protocol("k07 mock: unsupported".into())) })
}

impl AnyConnectionBackend for MockConn {
    fn name(&self) -> &str {
        "K07Mock"
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            // The future owns the raw connection until it completes or is dropped, like a driver.
            self.script.record(Ev::CloseStart, self.id);
            let mode = *self.script.close.lock().unwrap();
            let result = self.script.clone().wait(mode).await;
            drop(self);
            result
        })
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, Result<(), Error>> {
        self.script.record(Ev::CloseHard, self.id);
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.script.record(Ev::PingStart, self.id);
            let mode = *self.script.ping.lock().unwrap();
            let result = self.script.wait(mode).await;
            self.script.record(Ev::PingDone, self.id);
            result
        })
    }

    fn begin(&mut self, _statement: Option<Cow<'static, str>>) -> BoxFuture<'_, Result<(), Error>> {
        unsupported()
    }

    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        unsupported()
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        unsupported()
    }

    fn start_rollback(&mut self) {}

    fn get_transaction_depth(&self) -> usize {
        0
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    fn should_flush(&self) -> bool {
        false
    }

    fn fetch_many<'q>(
        &'q mut self,
        _query: &'q str,
        _persistent: bool,
        _arguments: Option<AnyArguments<'q>>,
    ) -> BoxStream<'q, Result<Either<AnyQueryResult, AnyRow>, Error>> {
        Box::pin(futures_util::stream::empty())
    }

    fn fetch_optional<'q>(
        &'q mut self,
        _query: &'q str,
        _persistent: bool,
        _arguments: Option<AnyArguments<'q>>,
    ) -> BoxFuture<'q, Result<Option<AnyRow>, Error>> {
        Box::pin(async { Ok(None) })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        _sql: &'q str,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, Result<AnyStatement<'q>, Error>> {
        unsupported()
    }

    fn describe<'q>(&'q mut self, _sql: &'q str) -> BoxFuture<'q, Result<Describe<Any>, Error>> {
        unsupported()
    }
}

fn mock_connect(options: &AnyConnectOptions) -> BoxFuture<'_, Result<AnyConnection, Error>> {
    Box::pin(async move {
        let name = options
            .database_url
            .host_str()
            .unwrap_or_default()
            .to_owned();
        let script = SCRIPTS
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .expect("unknown k07 script");
        let id = script.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        script.record(Ev::ConnectStart, id);
        let mode = *script.connect.lock().unwrap();
        script.wait(mode).await?;
        script.record(Ev::Connected, id);
        Ok(AnyConnection {
            backend: Box::new(MockConn { id, script }),
        })
    })
}

static DRIVERS: [AnyDriver; 1] = [AnyDriver {
    name: "K07Mock",
    url_schemes: &["k07mock"],
    connect: DebugFn(mock_connect),
    migrate_database: None,
}];

fn install() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| install_drivers(&DRIVERS).expect("install the k07 mock driver"));
}

fn opts() -> PoolOptions<Any> {
    PoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(3))
}

/// Must be called inside a runtime (the pool may spawn maintenance).
fn lazy(script: &Arc<Script>, options: PoolOptions<Any>) -> Pool<Any> {
    install();
    let pool = options
        .connect_lazy(&format!("k07mock://{}/", script.name))
        .expect("lazy mock pool");
    *script.pool.lock().unwrap() = Arc::downgrade(&pool.0);
    pool
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
}

fn run<F: Future>(f: F) -> F::Output {
    rt().block_on(f)
}

async fn within<F: Future>(what: &str, bound: Duration, f: F) -> F::Output {
    match tokio::time::timeout(bound, f).await {
        Ok(output) => output,
        Err(_) => panic!("{what}: not finished within {bound:?}"),
    }
}

async fn until(what: &str, bound: Duration, mut cond: impl FnMut() -> bool) -> Duration {
    let start = Instant::now();
    loop {
        if cond() {
            return start.elapsed();
        }
        assert!(
            start.elapsed() <= bound,
            "{what}: not reached within {bound:?}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn permits(pool: &Pool<Any>) -> usize {
    pool.0.semaphore.permits()
}

fn spawn_close(pool: &Pool<Any>) -> tokio::task::JoinHandle<Instant> {
    let pool = pool.clone();
    tokio::spawn(async move {
        pool.close().await;
        Instant::now()
    })
}

async fn idle_one(pool: &Pool<Any>, script: &Script) -> u64 {
    let conn = pool.acquire().await.expect("acquire");
    let id = script.next_id.load(Ordering::SeqCst);
    drop(conn);
    until("healthy release to idle", MARGIN, || pool.num_idle() == 1).await;
    id
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[test]
fn k07_frozen_budgets() {
    assert_eq!(RETURN_TO_POOL_TIMEOUT, T_RELEASE);
    assert_eq!(CLOSE_IDLE_TIMEOUT, T_IDLE_CLOSE);
}

/// T01: a healthy return still pings and the same connection is reused.
#[test]
fn k07_t01_healthy_return_pings_and_reuses_the_connection() {
    run(async {
        let s = Script::new("t01");
        let pool = lazy(&s, opts());
        for round in 0..3 {
            let conn = pool.acquire().await.expect("acquire");
            let pings_before = s.count(Ev::PingDone);
            drop(conn);
            until("release ping + idle", MARGIN, || {
                pool.num_idle() == 1 && s.count(Ev::PingDone) == pings_before + 1
            })
            .await;
            assert_eq!((pool.size(), pool.num_idle()), (1, 1), "round {round}");
        }
        assert_eq!(
            s.count(Ev::ConnectStart),
            1,
            "healthy returns reuse one connection"
        );
        assert_eq!(s.count(Ev::RawDrop), 0);
        within("close", MARGIN, pool.close()).await;
        assert_eq!(s.count(Ev::RawDrop), 1);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    })
}

/// T02: a release ping that never answers loses the connection at T_release; capacity recovers
/// without calling close, and a new connection is opened afterwards.
#[test]
fn k07_t02_pending_release_ping_is_dropped_at_deadline_and_capacity_recovers() {
    run(async {
        let s = Script::new("t02");
        let pool = lazy(&s, opts());
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Pending);
        let dropped_at = Instant::now();
        drop(conn);
        until("release ping started", MARGIN, || {
            s.count(Ev::PingStart) == 1
        })
        .await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            (pool.size(), pool.num_idle()),
            (1, 0),
            "owned by the return future"
        );
        assert_eq!(s.count(Ev::RawDrop), 0);

        until("size back to zero", T_RELEASE + MARGIN, || pool.size() == 0).await;
        let elapsed = dropped_at.elapsed();
        assert!(
            elapsed >= T_RELEASE,
            "dropped before the deadline: {elapsed:?}"
        );
        let drop_rec = s.one(Ev::RawDrop, 1);
        assert_eq!(
            drop_rec.size,
            Some(1),
            "raw connection must be dropped before size"
        );
        assert_eq!(s.count(Ev::CloseHard) + s.count(Ev::CloseStart), 0);
        assert_eq!(permits(&pool), 1, "the permit comes back exactly once");

        Script::set(&s.ping, Mode::Ok);
        let conn = within("acquire after recovery", MARGIN, pool.acquire())
            .await
            .expect("acquire after recovery");
        assert_eq!(
            s.count(Ev::ConnectStart),
            2,
            "a new connection replaces the dropped one"
        );
        drop(conn);
        within("close", MARGIN, pool.close()).await;
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    })
}

/// T03: a single close started while the release ping is blocked finishes by T_release.
#[test]
fn k07_t03_close_during_blocked_release_ping_finishes_by_deadline() {
    run(async {
        let s = Script::new("t03");
        let pool = lazy(&s, opts());
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Pending);
        let dropped_at = Instant::now();
        drop(conn);
        until("release ping started", MARGIN, || {
            s.count(Ev::PingStart) == 1
        })
        .await;
        let closer = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !closer.is_finished(),
            "close must wait for the connection being returned"
        );
        let returned = within("first close", T_RELEASE + MARGIN, closer)
            .await
            .unwrap();
        assert!(returned.duration_since(dropped_at) >= T_RELEASE);
        let drop_rec = s.one(Ev::RawDrop, 1);
        assert!(
            drop_rec.at <= returned,
            "close returned before the raw connection was dropped"
        );
        assert_eq!(drop_rec.size, Some(1));
        assert!(pool.is_closed());
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    })
}

/// T04 / T05 (normal return): the ping answers before the deadline while close is waiting; the
/// closed pool refuses the connection, which is gracefully closed by the same return future.
#[test]
fn k07_t04_late_ping_ok_during_close_is_refused_and_closed() {
    run(async {
        let s = Script::new("t04");
        let pool = lazy(&s, opts());
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Gated);
        drop(conn);
        until("release ping started", MARGIN, || {
            s.count(Ev::PingStart) == 1
        })
        .await;
        let closer = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(!closer.is_finished());
        let opened = Instant::now();
        s.open_gate(true);
        let returned = within("close after late ping", MARGIN, closer)
            .await
            .unwrap();
        assert!(
            returned.duration_since(opened) < MARGIN,
            "a healthy late return must not wait for the deadline"
        );
        let close_rec = s.one(Ev::CloseStart, 1);
        let drop_rec = s.one(Ev::RawDrop, 1);
        assert!(close_rec.at >= opened && drop_rec.at <= returned);
        assert!(!drop_rec.gate_held);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert!(
            pool.0.idle_conns.is_empty(),
            "no idle connection published after close"
        );
    })
}

/// T05a: release wins the lifecycle gate; close waits for it and then drains the new idle.
#[test]
fn k07_t05a_release_inside_gate_is_drained_by_close() {
    let runtime = rt();
    let s = Script::new("t05a");
    let pool = runtime.block_on(async { lazy(&s, opts()) });
    let inner = pool.0.clone();
    let floating = runtime.block_on(inner.acquire()).expect("acquire");
    let (pause, ctl) = Pause::new(Point::ReleaseBeforePublish);
    inner.k07_hooks.set(move |p| pause.hit(p));

    let releaser = {
        let inner = inner.clone();
        thread::spawn(move || inner.try_release(floating).is_ok())
    };
    ctl.wait_reached();
    let marked = Arc::new(AtomicBool::new(false));
    let closer = {
        let inner = inner.clone();
        let marked = marked.clone();
        thread::spawn(move || {
            let rt = rt();
            let close = inner.close();
            marked.store(true, Ordering::SeqCst);
            rt.block_on(async { tokio::time::timeout(BOUND, close).await })
                .is_ok()
        })
    };
    thread::sleep(SETTLE);
    assert!(
        !marked.load(Ordering::SeqCst),
        "mark_closed must wait for a release already inside the gate"
    );
    ctl.resume();
    assert!(
        releaser.join().unwrap(),
        "release that won the gate is accepted"
    );
    assert!(
        closer.join().unwrap(),
        "close must drain the published idle connection"
    );
    inner.k07_hooks.clear();
    assert_eq!((inner.size(), inner.num_idle()), (0, 0));
    assert!(inner.idle_conns.is_empty());
    assert_eq!(s.count(Ev::RawDrop), 1);
    assert_eq!(inner.semaphore.permits(), 1);
}

/// T05b: close wins the gate; the central release refuses and hands the connection back, and it
/// is dropped outside the gate.
#[test]
fn k07_t05b_release_after_close_is_refused_outside_the_gate() {
    run(async {
        let s = Script::new("t05b");
        let pool = lazy(&s, opts());
        let inner = pool.0.clone();
        let floating = inner.acquire().await.expect("acquire");
        let close = pool.close();
        let rejected = inner
            .try_release(floating)
            .err()
            .expect("closed pool refuses");
        assert_eq!(inner.num_idle(), 0);
        assert!(inner.idle_conns.is_empty());
        drop(rejected);
        let drop_rec = s.one(Ev::RawDrop, 1);
        assert!(
            !drop_rec.gate_held,
            "refused connection dropped outside the gate"
        );
        assert_eq!(drop_rec.size, Some(1));
        within("close", MARGIN, close).await;
        assert_eq!((inner.size(), inner.num_idle()), (0, 0));
        assert_eq!(inner.semaphore.permits(), 1);
    })
}

/// T05c: `num_idle` is published before a consumer can pop the idle connection (the pop happens
/// right after the push, while the releasing thread is still inside the gate).
#[test]
fn k07_t05c_idle_count_is_published_before_the_connection_can_be_popped() {
    run(async {
        let s = Script::new("t05c");
        let pool = lazy(&s, opts().max_connections(2));
        let inner = pool.0.clone();
        let floating = inner.acquire().await.expect("acquire");
        let observed = Arc::new(Mutex::new(None));
        let stash = Arc::new(Mutex::new(None));
        {
            let weak = Arc::downgrade(&inner);
            let observed = observed.clone();
            let stash = stash.clone();
            inner.k07_hooks.set(move |p| {
                if p != Point::ReleaseAfterPush {
                    return;
                }
                let inner = weak.upgrade().expect("pool alive");
                let popped = inner
                    .try_acquire()
                    .expect("free permit pops the pushed idle");
                *observed.lock().unwrap() = Some(inner.num_idle());
                *stash.lock().unwrap() = Some(popped.into_live());
            });
        }
        assert!(inner.try_release(floating).is_ok(), "open pool accepts");
        inner.k07_hooks.clear();
        assert_eq!(
            *observed.lock().unwrap(),
            Some(0),
            "num_idle must already count the pushed connection when it is popped"
        );
        drop(stash.lock().unwrap().take());
        within("close", MARGIN, pool.close()).await;
        assert_eq!((inner.size(), inner.num_idle()), (0, 0));
        assert_eq!(inner.semaphore.permits(), 2);
    })
}

/// T05d / T13 (central caller): a `min_connections` creation that completes after close is
/// refused by the central release and dropped, never published as idle.
#[test]
fn k07_t05d_min_connections_creation_after_close_is_refused() {
    run(async {
        let s = Script::new("t05d");
        Script::set(&s.connect, Mode::Gated);
        let pool = lazy(&s, opts().min_connections(1));
        until("maintenance connect started", MARGIN, || {
            s.count(Ev::ConnectStart) == 1
        })
        .await;
        assert_eq!(pool.size(), 1, "the in-flight creation is counted");
        let closer = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !closer.is_finished(),
            "close waits for the counted creation"
        );
        s.open_gate(true);
        within("close", MARGIN, closer).await.unwrap();
        assert_eq!(s.count(Ev::Connected), 1);
        let drop_rec = s.one(Ev::RawDrop, 1);
        assert!(!drop_rec.gate_held);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert!(pool.0.idle_conns.is_empty());
    })
}

/// T06: one idle, one checked out. Close stays pending while the business connection is held and
/// closing the idle one does not add a permit.
#[test]
fn k07_t06_close_waits_for_held_connection_without_permit_inflation() {
    run(async {
        let s = Script::new("t06");
        let pool = lazy(&s, opts().max_connections(2));
        let a = pool.acquire().await.expect("a");
        let b = pool.acquire().await.expect("b");
        drop(a);
        until("a idle", MARGIN, || pool.num_idle() == 1).await;
        assert_eq!(permits(&pool), 1);
        let closer = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !closer.is_finished(),
            "close must wait for the held connection"
        );
        assert_eq!((pool.size(), pool.num_idle()), (1, 0));
        assert_eq!(s.count(Ev::RawDrop), 1);
        assert_eq!(
            permits(&pool),
            1,
            "closing an idle connection must not add a permit"
        );
        let released = Instant::now();
        drop(b);
        let returned = within("close after held returns", MARGIN, closer)
            .await
            .unwrap();
        assert!(returned.duration_since(released) < MARGIN);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(permits(&pool), 2);
    })
}

/// T07: two closers; an idle graceful close hangs while a held connection's return overlaps.
/// Neither closer may return while a raw connection still belongs to the pool.
#[test]
fn k07_t07_two_closers_wait_for_every_raw_connection() {
    run(async {
        let s = Script::new("t07");
        let pool = lazy(&s, opts().max_connections(2));
        let a = pool.acquire().await.expect("a");
        let b = pool.acquire().await.expect("b");
        drop(a);
        until("a idle", MARGIN, || pool.num_idle() == 1).await;
        Script::set(&s.close, Mode::Pending);
        let started = Instant::now();
        let c1 = spawn_close(&pool);
        let c2 = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(!c1.is_finished() && !c2.is_finished());
        let b_dropped = Instant::now();
        drop(b);
        let r1 = within("closer 1", T_RELEASE + MARGIN + SETTLE, c1)
            .await
            .unwrap();
        let r2 = within("closer 2", T_RELEASE + MARGIN + SETTLE, c2)
            .await
            .unwrap();
        let drops = s.records(Ev::RawDrop);
        assert_eq!(drops.len(), 2);
        for d in &drops {
            assert!(
                d.at <= r1 && d.at <= r2,
                "a closer returned before raw {} dropped",
                d.conn
            );
            assert!(
                d.size.unwrap_or(0) >= 1,
                "raw {} dropped after its size unit",
                d.conn
            );
        }
        assert!(r1.duration_since(started) >= T_IDLE_CLOSE);
        assert!(
            r1.duration_since(b_dropped) >= T_RELEASE || r2.duration_since(b_dropped) >= T_RELEASE
        );
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(permits(&pool), 2, "no permit inflation or double decrement");
    })
}

/// T08a: a return future that is never polled still drops the raw connection and gives the size
/// unit and the permit back exactly once.
#[test]
fn k07_t08a_unpolled_return_future_releases_once() {
    run(async {
        let s = Script::new("t08a");
        let pool = lazy(&s, opts());
        let mut conn = pool.acquire().await.expect("acquire");
        let fut = conn.return_to_pool();
        drop(fut);
        drop(conn);
        assert_eq!(s.count(Ev::RawDrop), 1);
        assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
        assert_eq!(s.count(Ev::PingStart), 0, "no release ping was started");
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(permits(&pool), 1);
        let again = within("reacquire", MARGIN, pool.acquire())
            .await
            .expect("reacquire");
        drop(again);
        within("close", MARGIN, pool.close()).await;
    })
}

/// T08b: cancelling the owned return future while its ping is pending drops the raw connection
/// before the size unit, once, and capacity recovers.
#[test]
fn k07_t08b_cancelled_return_future_releases_once() {
    run(async {
        let s = Script::new("t08b");
        let pool = lazy(&s, opts());
        let mut conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Pending);
        let task = tokio::spawn(conn.return_to_pool());
        drop(conn);
        until("release ping started", MARGIN, || {
            s.count(Ev::PingStart) == 1
        })
        .await;
        assert_eq!(pool.size(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(s.count(Ev::RawDrop), 1);
        assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(permits(&pool), 1);
        Script::set(&s.ping, Mode::Ok);
        let again = within("reacquire", MARGIN, pool.acquire())
            .await
            .expect("reacquire");
        drop(again);
        within("close", MARGIN, pool.close()).await;
    })
}

/// T09: cancelling a close future while it gracefully closes one idle connection releases that
/// connection's size unit once (and no permit); a later close finishes the rest.
#[test]
fn k07_t09_cancelled_close_releases_taken_idle_once() {
    run(async {
        let s = Script::new("t09");
        let pool = lazy(&s, opts().max_connections(2));
        let a = pool.acquire().await.expect("a");
        let b = pool.acquire().await.expect("b");
        drop(a);
        drop(b);
        until("both idle", MARGIN, || pool.num_idle() == 2).await;
        assert_eq!(permits(&pool), 2);
        Script::set(&s.close, Mode::Pending);
        {
            let first = pool.close();
            tokio::pin!(first);
            assert!(tokio::time::timeout(SETTLE, &mut first).await.is_err());
            assert_eq!(
                s.count(Ev::CloseStart),
                1,
                "first close took exactly one idle"
            );
            assert_eq!(pool.num_idle(), 1);
        } // cancelled here
        let taken = s.records(Ev::CloseStart)[0].conn;
        let drop_rec = s.one(Ev::RawDrop, taken);
        assert_eq!(drop_rec.size, Some(2), "raw dropped before its size unit");
        assert_eq!((pool.size(), pool.num_idle()), (1, 1));
        assert_eq!(permits(&pool), 2, "a closing idle guard owns no permit");
        Script::set(&s.close, Mode::Ok);
        within("second close", MARGIN, pool.close()).await;
        assert_eq!(s.count(Ev::RawDrop), 2);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        assert_eq!(permits(&pool), 2);
    })
}

async fn assert_dropped_at_deadline(s: &Script, pool: &Pool<Any>, dropped_at: Instant) {
    until("size back to zero", T_RELEASE + MARGIN, || pool.size() == 0).await;
    let elapsed = dropped_at.elapsed();
    assert!(
        elapsed >= T_RELEASE,
        "dropped before the deadline: {elapsed:?}"
    );
    assert_eq!(s.count(Ev::RawDrop), 1);
    assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
    assert_eq!(pool.num_idle(), 0);
    assert_eq!(permits(pool), 1);
}

/// T10a: `after_release` Pending is bounded by the same deadline.
#[test]
fn k07_t10a_after_release_pending_is_bounded() {
    run(async {
        let s = Script::new("t10a");
        let pool = lazy(
            &s,
            opts().after_release(|_, _| Box::pin(std::future::pending())),
        );
        let conn = pool.acquire().await.expect("acquire");
        let dropped_at = Instant::now();
        drop(conn);
        assert_dropped_at_deadline(&s, &pool, dropped_at).await;
        assert_eq!(s.count(Ev::PingStart), 0, "no release ping was started");
    })
}

/// T10b: `after_release` Err drops the connection immediately without any driver close.
#[test]
fn k07_t10b_after_release_error_drops_without_close() {
    run(async {
        let s = Script::new("t10b");
        let pool = lazy(
            &s,
            opts().after_release(|_, _| {
                Box::pin(async { Err(Error::Protocol("k07 after_release".into())) })
            }),
        );
        let conn = pool.acquire().await.expect("acquire");
        drop(conn);
        until("dropped", MARGIN, || pool.size() == 0).await;
        assert_eq!(s.count(Ev::CloseHard) + s.count(Ev::CloseStart), 0);
        assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
        assert_eq!((pool.num_idle(), permits(&pool)), (0, 1));
    })
}

/// T10c: `after_release` Ok(false) gracefully closes and does not pool.
#[test]
fn k07_t10c_after_release_false_closes_gracefully() {
    run(async {
        let s = Script::new("t10c");
        let pool = lazy(
            &s,
            opts().after_release(|_, _| Box::pin(async { Ok(false) })),
        );
        let conn = pool.acquire().await.expect("acquire");
        drop(conn);
        until("closed", MARGIN, || pool.size() == 0).await;
        assert_eq!(s.count(Ev::CloseStart), 1);
        assert_eq!(s.count(Ev::CloseHard), 0);
        assert_eq!((pool.num_idle(), permits(&pool)), (0, 1));
    })
}

/// T10d: a failing release ping drops the connection without awaiting a driver close.
#[test]
fn k07_t10d_ping_error_drops_without_close() {
    run(async {
        let s = Script::new("t10d");
        let pool = lazy(&s, opts());
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Err);
        drop(conn);
        until("dropped", MARGIN, || pool.size() == 0).await;
        assert_eq!(s.count(Ev::CloseHard) + s.count(Ev::CloseStart), 0);
        assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
        assert_eq!((pool.num_idle(), permits(&pool)), (0, 1));
    })
}

/// T10e: the closed-pool branch's graceful close Pending is dropped at the deadline.
#[test]
fn k07_t10e_closed_branch_graceful_close_pending_is_bounded() {
    run(async {
        let s = Script::new("t10e");
        let pool = lazy(&s, opts());
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.close, Mode::Pending);
        let closer = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        let dropped_at = Instant::now();
        drop(conn);
        assert_dropped_at_deadline(&s, &pool, dropped_at).await;
        assert_eq!(s.count(Ev::CloseStart), 1);
        within("close", MARGIN, closer).await.unwrap();
    })
}

/// T10f: the max_lifetime branch's graceful close Pending is dropped at the deadline.
#[test]
fn k07_t10f_max_lifetime_close_pending_is_bounded() {
    run(async {
        let s = Script::new("t10f");
        let pool = lazy(&s, opts().max_lifetime(Duration::from_millis(1)));
        let conn = pool.acquire().await.expect("acquire");
        tokio::time::sleep(Duration::from_millis(10)).await;
        Script::set(&s.close, Mode::Pending);
        let dropped_at = Instant::now();
        drop(conn);
        assert_dropped_at_deadline(&s, &pool, dropped_at).await;
        assert_eq!(s.count(Ev::CloseStart), 1);
        assert_eq!(s.count(Ev::PingStart), 0, "no release ping was started");
    })
}

/// T10g: one budget for the whole return, not one per stage: a 3 s `after_release` followed by a
/// Pending ping still ends at T_release.
#[test]
fn k07_t10g_single_budget_across_stages() {
    run(async {
        let s = Script::new("t10g");
        let pool = lazy(
            &s,
            opts().after_release(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    Ok(true)
                })
            }),
        );
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Pending);
        let dropped_at = Instant::now();
        drop(conn);
        assert_dropped_at_deadline(&s, &pool, dropped_at).await;
        assert!(dropped_at.elapsed() < T_RELEASE + MARGIN);
        assert_eq!(
            s.count(Ev::PingStart),
            1,
            "the release ping started after after_release"
        );
    })
}

/// T10h: the deadline is fixed at creation; a return future first polled after it drops the
/// connection without pinging.
#[test]
fn k07_t10h_deadline_fixed_at_creation() {
    run(async {
        let s = Script::new("t10h");
        let pool = lazy(&s, opts());
        let mut conn = pool.acquire().await.expect("acquire");
        let fut = conn.return_to_pool();
        drop(conn);
        tokio::time::sleep(T_RELEASE + Duration::from_millis(100)).await;
        assert_eq!(pool.size(), 1, "not polled yet");
        within("expired return", MARGIN, fut).await;
        assert_eq!(s.count(Ev::PingStart), 0, "no release ping was started");
        assert_eq!(s.one(Ev::RawDrop, 1).size, Some(1));
        assert_eq!((pool.size(), pool.num_idle(), permits(&pool)), (0, 0, 1));
    })
}

/// T11: an idle graceful close that never finishes is dropped at T_idle_close, raw before size,
/// and every closer wakes.
#[test]
fn k07_t11_idle_close_pending_is_dropped_and_wakes_all_closers() {
    run(async {
        let s = Script::new("t11");
        let pool = lazy(&s, opts());
        let id = idle_one(&pool, &s).await;
        Script::set(&s.close, Mode::Pending);
        let started = Instant::now();
        let c1 = spawn_close(&pool);
        let c2 = spawn_close(&pool);
        let r1 = within("closer 1", T_IDLE_CLOSE + MARGIN, c1).await.unwrap();
        let r2 = within("closer 2", T_IDLE_CLOSE + MARGIN, c2).await.unwrap();
        let drop_rec = s.one(Ev::RawDrop, id);
        assert_eq!(drop_rec.size, Some(1), "raw dropped before the size unit");
        assert!(drop_rec.at <= r1 && drop_rec.at <= r2);
        assert!(r1.duration_since(started) >= T_IDLE_CLOSE);
        assert_eq!((pool.size(), pool.num_idle(), permits(&pool)), (0, 0, 1));
    })
}

/// T12a: an increment already inside the gate is accepted first; close waits for its guard and
/// the size never rebounds after close returns.
#[test]
fn k07_t12a_increment_inside_gate_is_waited_for() {
    let runtime = rt();
    let s = Script::new("t12a");
    let pool = runtime.block_on(async { lazy(&s, opts()) });
    let inner = pool.0.clone();
    let (pause, ctl) = Pause::new(Point::SizeBeforeIncrement);
    inner.k07_hooks.set(move |p| pause.hit(p));

    let incrementer = {
        let inner = inner.clone();
        thread::spawn(move || {
            let permit = inner.semaphore.try_acquire(1).expect("free permit");
            inner.try_increment_size(permit).ok()
        })
    };
    ctl.wait_reached();
    let marked = Arc::new(AtomicBool::new(false));
    let closer = {
        let inner = inner.clone();
        let marked = marked.clone();
        thread::spawn(move || {
            let rt = rt();
            let close = inner.close();
            marked.store(true, Ordering::SeqCst);
            let done = rt
                .block_on(async { tokio::time::timeout(BOUND, close).await })
                .is_ok();
            (done, inner.size())
        })
    };
    thread::sleep(SETTLE);
    assert!(
        !marked.load(Ordering::SeqCst),
        "mark_closed must wait for an increment already inside the gate"
    );
    ctl.resume();
    let guard = incrementer
        .join()
        .unwrap()
        .expect("increment that won the gate is accepted");
    runtime.block_on(until("closed", BOUND, || marked.load(Ordering::SeqCst)));
    thread::sleep(SETTLE);
    assert!(
        !closer.is_finished(),
        "close must wait for the accepted increment"
    );
    assert_eq!(inner.size(), 1);
    drop(guard);
    let (done, size_at_return) = closer.join().unwrap();
    assert!(done);
    assert_eq!(size_at_return, 0);
    inner.k07_hooks.clear();
    assert_eq!(inner.size(), 0);
    assert_eq!(inner.semaphore.permits(), 1);
}

/// T12b: close wins the gate; a later increment is refused.
#[test]
fn k07_t12b_increment_after_close_is_refused() {
    run(async {
        let s = Script::new("t12b");
        let pool = lazy(&s, opts());
        let inner = pool.0.clone();
        let close = pool.close();
        let permit = inner.semaphore.try_acquire(1).expect("free permit");
        assert!(
            inner.try_increment_size(permit).is_err(),
            "closed pool refuses to grow"
        );
        assert_eq!(inner.size(), 0);
        within("close", MARGIN, close).await;
        assert_eq!(inner.size(), 0);
        assert_eq!(inner.semaphore.permits(), 1);
    })
}

enum Connect {
    Succeeds,
    Fails,
    Cancelled,
}

async fn in_flight_connect(name: &str, outcome: Connect) {
    let s = Script::new(name);
    Script::set(&s.connect, Mode::Gated);
    let pool = lazy(&s, opts());
    let acquirer = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.acquire().await })
    };
    until("connect started", MARGIN, || s.count(Ev::ConnectStart) == 1).await;
    assert_eq!(pool.size(), 1, "the in-flight creation is counted");
    let closer = spawn_close(&pool);
    tokio::time::sleep(SETTLE).await;
    assert!(
        !closer.is_finished(),
        "close waits for the counted creation"
    );
    match outcome {
        Connect::Succeeds => {
            s.open_gate(true);
            let conn = within("acquire", MARGIN, acquirer)
                .await
                .unwrap()
                .expect("creation accepted before close is handed to the caller");
            tokio::time::sleep(SETTLE).await;
            assert!(
                !closer.is_finished(),
                "close waits while the business holds it"
            );
            assert_eq!((pool.size(), pool.num_idle()), (1, 0));
            drop(conn);
            within("close", MARGIN, closer).await.unwrap();
            assert_eq!(s.count(Ev::RawDrop), 1);
        }
        Connect::Fails => {
            s.open_gate(false);
            assert!(within("acquire", MARGIN, acquirer).await.unwrap().is_err());
            within("close", MARGIN, closer).await.unwrap();
            assert_eq!(s.count(Ev::Connected), 0);
        }
        Connect::Cancelled => {
            acquirer.abort();
            assert!(acquirer.await.unwrap_err().is_cancelled());
            within("close", MARGIN, closer).await.unwrap();
            assert_eq!(s.count(Ev::Connected), 0);
        }
    }
    assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    assert!(pool.0.idle_conns.is_empty());
    assert_eq!(permits(&pool), 1);
}

/// T13: close with a counted creation in flight: success (business holds it), failure, cancel.
#[test]
fn k07_t13a_in_flight_connect_succeeds() {
    run(in_flight_connect("t13a", Connect::Succeeds))
}

#[test]
fn k07_t13b_in_flight_connect_fails() {
    run(in_flight_connect("t13b", Connect::Fails))
}

#[test]
fn k07_t13c_in_flight_connect_cancelled() {
    run(in_flight_connect("t13c", Connect::Cancelled))
}

/// T14: the last size unit is released at each point of a closer's loop while another closer is
/// already waiting; nobody misses the wake-up.
fn last_guard_dropped_at(name: &str, point: Point) {
    run(async {
        let s = Script::new(name);
        let pool = lazy(&s, opts());
        let inner = pool.0.clone();
        let permit = inner.semaphore.try_acquire(1).expect("free permit");
        let guard = inner.try_increment_size(permit).ok().expect("size unit");
        assert_eq!(inner.size(), 1);

        let waiting = spawn_close(&pool);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !waiting.is_finished(),
            "first closer is parked on the size event"
        );

        let slot = Arc::new(Mutex::new(Some(guard)));
        let fired = Arc::new(AtomicUsize::new(0));
        {
            let slot = slot.clone();
            let fired = fired.clone();
            inner.k07_hooks.set(move |p| {
                if p == point {
                    if let Some(guard) = slot.lock().unwrap().take() {
                        fired.fetch_add(1, Ordering::SeqCst);
                        drop(guard);
                    }
                }
            });
        }
        let dropping = spawn_close(&pool);
        within("closer that released the last unit", MARGIN, dropping)
            .await
            .unwrap();
        within("closer parked before the release", MARGIN, waiting)
            .await
            .unwrap();
        inner.k07_hooks.clear();
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "hook point {point:?} reached"
        );
        assert_eq!(inner.size(), 0);
        assert_eq!(inner.semaphore.permits(), 1);
    })
}

#[test]
fn k07_t14a_wake_when_released_before_listen() {
    last_guard_dropped_at("t14a", Point::CloseLoopTop)
}

#[test]
fn k07_t14b_wake_when_released_after_listen() {
    last_guard_dropped_at("t14b", Point::CloseAfterListen)
}

#[test]
fn k07_t14c_wake_when_released_before_size_check() {
    last_guard_dropped_at("t14c", Point::CloseBeforeSizeCheck)
}

#[test]
fn k07_t14d_wake_when_released_before_wait() {
    last_guard_dropped_at("t14d", Point::CloseBeforeWait)
}

/// T15a: min=0 (production) opens nothing extra on return.
#[test]
fn k07_t15a_min0_opens_nothing_extra() {
    run(async {
        let s = Script::new("t15a");
        let pool = lazy(&s, opts());
        idle_one(&pool, &s).await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(s.count(Ev::ConnectStart), 1);
        Script::set(&s.ping, Mode::Err);
        let conn = pool.acquire().await;
        // The idle connection failed its acquire ping; a replacement was opened for the caller.
        assert_eq!(
            s.count(Ev::ConnectStart),
            2,
            "only the replacement for the caller"
        );
        drop(conn);
        tokio::time::sleep(SETTLE).await;
        assert_eq!(pool.size(), 0, "min=0: the failed return is not backfilled");
        assert_eq!(s.count(Ev::ConnectStart), 2);
        within("close", MARGIN, pool.close()).await;
    })
}

/// T15b: min=1 backfills after a failed return.
#[test]
fn k07_t15b_min1_backfills_after_failed_return() {
    run(async {
        let s = Script::new("t15b");
        let pool = lazy(&s, opts().min_connections(1));
        until("initial min connection", MARGIN, || pool.num_idle() == 1).await;
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Err);
        drop(conn);
        until("backfilled", MARGIN, || {
            s.count(Ev::Connected) == 2 && pool.num_idle() == 1
        })
        .await;
        assert_eq!(pool.size(), 1);
        within("close", MARGIN, pool.close()).await;
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    })
}

/// T15c: the backfill started by a return shares that return's deadline.
#[test]
fn k07_t15c_min1_backfill_pending_is_bounded_by_the_return_deadline() {
    run(async {
        let s = Script::new("t15c");
        let pool = lazy(&s, opts().min_connections(1));
        until("initial min connection", MARGIN, || pool.num_idle() == 1).await;
        let conn = pool.acquire().await.expect("acquire");
        Script::set(&s.ping, Mode::Err);
        Script::set(&s.connect, Mode::Pending);
        let dropped_at = Instant::now();
        drop(conn);
        until("backfill started", MARGIN, || {
            s.count(Ev::ConnectStart) == 2
        })
        .await;
        assert_eq!(pool.size(), 1, "the backfill holds a size unit");
        until("backfill cut", T_RELEASE + MARGIN, || pool.size() == 0).await;
        assert!(dropped_at.elapsed() >= T_RELEASE);
        assert_eq!(permits(&pool), 1);
        within("close", MARGIN, pool.close()).await;
    })
}

/// T15d: nothing is backfilled after close.
#[test]
fn k07_t15d_no_backfill_after_close() {
    run(async {
        let s = Script::new("t15d");
        let pool = lazy(&s, opts().min_connections(1));
        until("initial min connection", MARGIN, || pool.num_idle() == 1).await;
        let conn = pool.acquire().await.expect("acquire");
        let closer = spawn_close(&pool);
        drop(conn);
        within("close", MARGIN, closer).await.unwrap();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(s.count(Ev::ConnectStart), 1);
        assert_eq!((pool.size(), pool.num_idle()), (0, 0));
    })
}

/// T15e: child pool (max=2) that borrowed one parent permit; two closers while its idle close
/// hangs. Neither closer waits for an impossible second permit, and the parent gets back exactly
/// the one permit that was borrowed.
#[test]
fn k07_t15e_child_pool_close_conserves_parent_permits() {
    run(async {
        let ps = Script::new("t15e-parent");
        let parent = lazy(&ps, opts().max_connections(2));
        let cs = Script::new("t15e-child");
        let child = lazy(&cs, opts().max_connections(2).parent(parent.clone()));
        let id = idle_one(&child, &cs).await;
        assert_eq!(permits(&parent), 1, "one permit borrowed by the child");
        assert_eq!(
            permits(&child),
            1,
            "the child's idle connection returned it locally"
        );
        Script::set(&cs.close, Mode::Pending);
        let started = Instant::now();
        let c1 = spawn_close(&child);
        let c2 = spawn_close(&child);
        let r1 = within("child closer 1", T_IDLE_CLOSE + MARGIN, c1)
            .await
            .unwrap();
        let r2 = within("child closer 2", T_IDLE_CLOSE + MARGIN, c2)
            .await
            .unwrap();
        assert!(r1.duration_since(started) >= T_IDLE_CLOSE);
        let drop_rec = cs.one(Ev::RawDrop, id);
        assert!(drop_rec.at <= r1 && drop_rec.at <= r2);
        assert_eq!((child.size(), child.num_idle()), (0, 0));
        assert_eq!(
            permits(&child),
            1,
            "no permit added by the closing idle guard"
        );
        drop(child);
        assert_eq!(permits(&parent), 2, "parent permits conserved");
        within("parent close", MARGIN, parent.close()).await;
    })
}
