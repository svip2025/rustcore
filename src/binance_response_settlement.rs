//! Owns only safety facts already received from a signed HTTP attempt.
//! Caller cancellation never owns this write. This is not a durable outbox:
//! one bounded write may end unresolved, and pre-commit process death is unknown.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures_util::FutureExt;
use tokio::sync::{oneshot, watch, OwnedMutexGuard};
use tokio::time::Instant;
use tracing::instrument::WithSubscriber;

use crate::binance_futures::{persist_cooldown, CooldownTable};
use crate::reconcile_trace::Correlation;
use crate::store::Store;

pub(crate) const SETTLE_BUDGET: Duration = Duration::from_millis(5_000);

#[derive(Clone, Debug)]
pub(crate) struct Fact {
    pub scope: String,
    pub table: CooldownTable,
    pub until_ms: i64,
    pub received_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Durable,
    LocalOnly,
    DatabaseError,
    TimedOut,
    Panicked,
    Abandoned,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::LocalOnly => "local_only",
            Self::DatabaseError => "database_error",
            Self::TimedOut => "timeout_unknown",
            Self::Panicked => "panic_unknown",
            Self::Abandoned => "abandoned_unknown",
        }
    }
}

pub(crate) struct Settled<T> {
    pub payload: T,
    pub outcome: Outcome,
}

struct Pending {
    fact: Fact,
    generation: u64,
    outcome: Outcome,
    requires_durable: bool,
}

#[derive(Default)]
struct State {
    closing: bool,
    flights: usize,
    next_id: u64,
    active: HashMap<u64, Fact>,
    pending: HashMap<(String, CooldownTable), Pending>,
}

pub(crate) struct Settlements {
    state: Mutex<State>,
    closing: watch::Sender<bool>,
    changed: watch::Sender<u64>,
    #[cfg(test)]
    pub(crate) panic_next_write: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) panic_after_write: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pause_completion: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    #[cfg(test)]
    pub(crate) lost_replies: std::sync::atomic::AtomicUsize,
}

impl Settlements {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            closing: watch::channel(false).0,
            changed: watch::channel(0).0,
            #[cfg(test)]
            panic_next_write: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            panic_after_write: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            pause_completion: Mutex::new(None),
            #[cfg(test)]
            lost_replies: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn notify(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(crate) fn flight(self: &Arc<Self>) -> anyhow::Result<Flight> {
        let mut state = self.lock();
        if state.closing {
            return Err(anyhow!("BINANCE_SETTLEMENTS_CLOSING"));
        }
        state.flights += 1;
        Ok(Flight {
            owner: self.clone(),
            live: true,
        })
    }

    pub(crate) fn closing_receiver(&self) -> watch::Receiver<bool> {
        self.closing.subscribe()
    }

    pub(crate) fn begin_close(&self) {
        {
            let mut state = self.lock();
            state.closing = true;
            self.closing.send_replace(true);
        }
        self.notify();
    }

    /// Cancelling a waiter does not cancel accepted flights or registered jobs.
    /// Subscribe before inspecting the state: any number of closers can wait.
    pub(crate) async fn close(&self) -> anyhow::Result<()> {
        let mut changed = self.changed.subscribe();
        self.begin_close();
        loop {
            {
                let state = self.lock();
                if state.flights == 0 && state.active.is_empty() {
                    if state.pending.is_empty() {
                        return Ok(());
                    }
                    return Err(anyhow!(
                        "BINANCE_SETTLEMENTS_UNRESOLVED count={}",
                        state.pending.len()
                    ));
                }
            }
            changed
                .changed()
                .await
                .context("settlement completion channel closed")?;
        }
    }

    /// Registration consumes an already accepted flight even during Closing.
    /// The completion guard exists *before* spawn, so unpolled/panicking jobs
    /// cannot silently remove their obligation. Completed task state is reaped
    /// on completion, rather than accumulating JoinHandles until shutdown.
    pub(crate) fn settle<T: Send + 'static>(
        self: &Arc<Self>,
        mut flight: Flight,
        fact: Fact,
        received_at: Instant,
        store: Option<Store>,
        payload: T,
        gate: OwnedMutexGuard<()>,
        correlation: Correlation,
    ) -> oneshot::Receiver<Settled<T>> {
        assert!(Arc::ptr_eq(self, &flight.owner));
        let id = {
            let mut state = self.lock();
            state.next_id = state
                .next_id
                .checked_add(1)
                .expect("settlement generation exhausted");
            let id = state.next_id;
            let key = (fact.scope.clone(), fact.table);
            let pending = state.pending.entry(key).or_insert_with(|| Pending {
                fact: fact.clone(),
                generation: id,
                outcome: Outcome::Abandoned,
                requires_durable: store.is_some(),
            });
            if fact.until_ms >= pending.fact.until_ms {
                pending.fact = fact.clone();
            }
            pending.generation = id;
            pending.requires_durable |= store.is_some();
            state.active.insert(id, fact.clone());
            state.flights -= 1;
            flight.live = false;
            id
        };
        let completion = Completion {
            owner: self.clone(),
            id,
            done: false,
        };
        let (reply, receiver) = oneshot::channel();
        let deadline = received_at + SETTLE_BUDGET;
        let dispatch = correlation.dispatcher.clone();
        #[cfg(test)]
        let panic_write = self
            .panic_next_write
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        #[cfg(test)]
        let panic_after_write = self
            .panic_after_write
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        #[cfg(test)]
        let pause_completion = self.pause_completion.lock().unwrap().take();
        tokio::spawn(
            async move {
                let mut completion = completion;
                correlation.record(id, fact.table, "writing");
                let work = async {
                    #[cfg(test)]
                    if panic_write {
                        panic!("synthetic settlement writer failure");
                    }
                    let outcome = match store {
                        Some(_) if Instant::now() >= deadline => Outcome::TimedOut,
                        Some(store) => match tokio::time::timeout_at(
                            deadline,
                            persist_cooldown(
                                &store,
                                fact.table,
                                &fact.scope,
                                fact.until_ms,
                                fact.received_at_ms,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(())) => Outcome::Durable,
                            Ok(Err(_)) => Outcome::DatabaseError,
                            Err(_) => Outcome::TimedOut,
                        },
                        None => Outcome::LocalOnly,
                    };
                    #[cfg(test)]
                    if panic_after_write {
                        panic!("synthetic failure after safety write acknowledgement");
                    }
                    outcome
                };
                let outcome = std::panic::AssertUnwindSafe(work)
                    .catch_unwind()
                    .await
                    .unwrap_or(Outcome::Panicked);
                // No body await and no exchange retry. A failed write drops its DB
                // future; pool-owned return cleanup is the separate K07 contract.
                drop(gate);
                #[cfg(test)]
                if let Some((entered, resume)) = pause_completion {
                    let _ = entered.send(());
                    let _ = resume.await;
                }
                correlation.record(id, fact.table, outcome.label());
                // No raw response/connection/key is retained by unresolved records.
                if reply.send(Settled { payload, outcome }).is_err() {
                    #[cfg(test)]
                    completion
                        .owner
                        .lost_replies
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                completion.finish(outcome);
            }
            .with_subscriber(dispatch),
        );
        receiver
    }

    #[cfg(test)]
    pub(crate) fn pause_next_completion(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        assert!(self
            .pause_completion
            .lock()
            .unwrap()
            .replace((entered, resumed))
            .is_none());
        (observed, resume)
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let state = self.lock();
        (state.flights, state.active.len(), state.pending.len())
    }

    #[cfg(test)]
    pub(crate) fn unresolved(&self) -> Vec<(Fact, Outcome)> {
        self.lock()
            .pending
            .values()
            .map(|p| (p.fact.clone(), p.outcome))
            .collect()
    }
}

pub(crate) struct Flight {
    owner: Arc<Settlements>,
    live: bool,
}

impl Drop for Flight {
    fn drop(&mut self) {
        if self.live {
            self.owner.lock().flights -= 1;
            self.owner.notify();
        }
    }
}

struct Completion {
    owner: Arc<Settlements>,
    id: u64,
    done: bool,
}

impl Completion {
    fn finish(&mut self, outcome: Outcome) {
        {
            let mut state = self.owner.lock();
            if let Some(fact) = state.active.remove(&self.id) {
                let key = (fact.scope.clone(), fact.table);
                if let Some(pending) = state.pending.get_mut(&key) {
                    let covers =
                        pending.generation <= self.id && fact.until_ms >= pending.fact.until_ms;
                    if covers
                        && (outcome == Outcome::Durable
                            || (outcome == Outcome::LocalOnly && !pending.requires_durable))
                    {
                        state.pending.remove(&key);
                    } else if pending.generation <= self.id {
                        // A successful smaller write cannot discharge an older
                        // larger unresolved deadline.
                        pending.outcome =
                            if matches!(outcome, Outcome::Durable | Outcome::LocalOnly) {
                                Outcome::Abandoned
                            } else {
                                outcome
                            };
                    }
                }
            }
            self.done = true;
        }
        self.owner.notify();
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.done {
            self.finish(Outcome::Abandoned);
        }
    }
}

fn registry() -> &'static Mutex<Weak<Settlements>> {
    static OWNER: OnceLock<Mutex<Weak<Settlements>>> = OnceLock::new();
    OWNER.get_or_init(|| Mutex::new(Weak::new()))
}

pub(crate) struct Registration(Arc<Settlements>);

pub(crate) fn install(owner: Arc<Settlements>) -> anyhow::Result<Registration> {
    let mut registered = registry().lock().unwrap_or_else(|e| e.into_inner());
    if registered.upgrade().is_some() {
        return Err(anyhow!(
            "Binance response settlement owner already registered"
        ));
    }
    *registered = Arc::downgrade(&owner);
    Ok(Registration(owner))
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut registered = registry().lock().unwrap_or_else(|e| e.into_inner());
        if registered.ptr_eq(&Arc::downgrade(&self.0)) {
            *registered = Weak::new();
        }
    }
}

pub(crate) fn for_client() -> anyhow::Result<Arc<Settlements>> {
    let owner = registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .upgrade();
    if let Some(owner) = owner {
        return Ok(owner);
    }
    // Unit tests instantiate clients without running main. Each such fixture
    // owns an isolated manager; production never silently falls back.
    #[cfg(test)]
    {
        Ok(Settlements::new())
    }
    #[cfg(not(test))]
    {
        Err(anyhow!(
            "Binance response settlement owner is not registered"
        ))
    }
}
