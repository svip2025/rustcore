//! Bounded, task-local diagnostics for an explicit v2 ReconcileNow call.
//! Only closed-world labels and elapsed times enter these events. In particular,
//! no exchange error, credential, URL, query, header or body is retained here.

use std::cell::Cell;
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

struct Context {
    id: String,
    next_attempt: Cell<u64>,
}

tokio::task_local! {
    static TRACE: Context;
}

pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    TRACE
        .scope(
            Context {
                id: ulid::Ulid::new().to_string(),
                next_attempt: Cell::new(0),
            },
            future,
        )
        .await
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Admission,
    TransportGate,
    Http,
    Postprocess,
    Truth,
    PlanLoad,
    Protection,
    ExchangeRead,
    LocalLoad,
    AlgoQueries,
    OrderQueries,
    Finalize,
}

impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::TransportGate => "transport_gate",
            Self::Http => "http",
            Self::Postprocess => "postprocess",
            Self::Truth => "truth",
            Self::PlanLoad => "plan_load",
            Self::Protection => "protection",
            Self::ExchangeRead => "exchange_read",
            Self::LocalLoad => "local_load",
            Self::AlgoQueries => "algo_queries",
            Self::OrderQueries => "order_queries",
            Self::Finalize => "finalize",
        }
    }
}

enum Kind {
    Rpc,
    Fullscan,
    Http {
        method: &'static str,
        path: &'static str,
        class: &'static str,
        attempt_id: u64,
    },
}

struct Active {
    // Drop must work even after the task-local scope itself has been removed.
    trace_id: String,
    // WithSubscriber enters its dispatcher during poll, but not when an
    // aborted future is dropped. Keep that observer for the terminal event.
    dispatcher: tracing::Dispatch,
    kind: Kind,
    started: Instant,
    phase_started: Instant,
    stage: Stage,
    phases: [Duration; 12],
    outcome: &'static str,
    complete: bool,
    local_orders: Option<u64>,
    local_protections: Option<u64>,
    algo_queries: u64,
    order_queries: u64,
    http_status: Option<u16>,
}

pub(crate) struct Timer(Option<Active>);

/// Copied before spawning; Tokio task-local context is not inherited by tasks.
pub(crate) struct Correlation {
    trace_id: Option<String>,
    attempt_id: Option<u64>,
    pub(crate) dispatcher: tracing::Dispatch,
}

impl Correlation {
    pub(crate) fn record(
        &self,
        settlement_id: u64,
        table: crate::binance_futures::CooldownTable,
        outcome: &'static str,
    ) {
        tracing::info!(
            trace_id = self.trace_id.as_deref(),
            attempt_id = self.attempt_id,
            settlement_id,
            http_status = if table == crate::binance_futures::CooldownTable::IpBan {
                418_u16
            } else {
                429_u16
            },
            outcome,
            "binance response safety settlement"
        );
    }
}

impl Timer {
    pub(crate) fn correlation(&self) -> Correlation {
        Correlation {
            trace_id: self.0.as_ref().map(|active| active.trace_id.clone()),
            attempt_id: self.0.as_ref().and_then(|active| match active.kind {
                Kind::Http { attempt_id, .. } => Some(attempt_id),
                _ => None,
            }),
            dispatcher: self
                .0
                .as_ref()
                .map(|active| active.dispatcher.clone())
                .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone)),
        }
    }
    fn new(kind: Kind, stage: Stage) -> Self {
        Self(
            TRACE
                .try_with(|ctx| {
                    let now = Instant::now();
                    Active {
                        trace_id: ctx.id.clone(),
                        dispatcher: tracing::dispatcher::get_default(Clone::clone),
                        kind,
                        started: now,
                        phase_started: now,
                        stage,
                        phases: [Duration::ZERO; 12],
                        outcome: "dropped",
                        complete: false,
                        local_orders: None,
                        local_protections: None,
                        algo_queries: 0,
                        order_queries: 0,
                        http_status: None,
                    }
                })
                .ok(),
        )
    }

    pub(crate) fn rpc() -> Self {
        Self::new(Kind::Rpc, Stage::Truth)
    }
    pub(crate) fn fullscan() -> Self {
        Self::new(Kind::Fullscan, Stage::ExchangeRead)
    }

    pub(crate) fn http(method: &str, path: &str, class: crate::rate_limiter::RequestClass) -> Self {
        let attempt_id = TRACE
            .try_with(|ctx| {
                let next = ctx.next_attempt.get().saturating_add(1);
                ctx.next_attempt.set(next);
                next
            })
            .unwrap_or(0);
        let method = match method {
            "GET" => "GET",
            "POST" => "POST",
            "PUT" => "PUT",
            "DELETE" => "DELETE",
            _ => "other",
        };
        // Accept only fixed endpoint names, never an arbitrary caller string.
        let path = match path {
            "/fapi/v1/order" => "/fapi/v1/order",
            "/fapi/v1/algoOrder" => "/fapi/v1/algoOrder",
            "/fapi/v1/openOrders" => "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders" => "/fapi/v1/openAlgoOrders",
            "/fapi/v1/allOpenOrders" => "/fapi/v1/allOpenOrders",
            "/fapi/v1/listenKey" => "/fapi/v1/listenKey",
            "/fapi/v1/userTrades" => "/fapi/v1/userTrades",
            "/fapi/v1/exchangeInfo" => "/fapi/v1/exchangeInfo",
            "/fapi/v1/premiumIndex" => "/fapi/v1/premiumIndex",
            "/fapi/v2/positionRisk" => "/fapi/v2/positionRisk",
            "/fapi/v2/balance" => "/fapi/v2/balance",
            _ => "other",
        };
        Self::new(
            Kind::Http {
                method,
                path,
                class: class.as_str(),
                attempt_id,
            },
            Stage::Admission,
        )
    }

    pub(crate) fn stage(&mut self, stage: Stage) {
        if let Some(active) = &mut self.0 {
            active.advance();
            active.stage = stage;
        }
    }

    /// Called only after the wrapped future actually returns. A `?` error is
    /// returned_error; a future dropped while pending keeps the dropped outcome.
    pub(crate) fn finish(&mut self, ok: bool) {
        if let Some(active) = &mut self.0 {
            active.advance();
            active.outcome = if ok { "ok" } else { "returned_error" };
            active.complete = ok;
        }
    }

    pub(crate) fn http_status(&mut self, status: u16) {
        if let Some(active) = &mut self.0 {
            active.http_status = Some(status);
        }
    }
    pub(crate) fn local_orders(&mut self, count: usize) {
        if let Some(active) = &mut self.0 {
            active.local_orders = Some(count as u64);
        }
    }
    pub(crate) fn local_protections(&mut self, count: usize) {
        if let Some(active) = &mut self.0 {
            active.local_protections = Some(count as u64);
        }
    }
    pub(crate) fn algo_query(&mut self) {
        if let Some(active) = &mut self.0 {
            active.algo_queries += 1;
        }
    }
    pub(crate) fn order_query(&mut self) {
        if let Some(active) = &mut self.0 {
            active.order_queries += 1;
        }
    }
}

impl Active {
    fn advance(&mut self) {
        let now = Instant::now();
        self.phases[self.stage as usize] += now.duration_since(self.phase_started);
        self.phase_started = now;
    }
    fn ms(&self, stage: Stage) -> u64 {
        self.phases[stage as usize]
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let Some(a) = &mut self.0 else {
            return;
        };
        if a.outcome == "dropped" {
            a.advance();
        }
        let elapsed_ms = a
            .phase_started
            .duration_since(a.started)
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let stage = if a.complete {
            "complete"
        } else {
            a.stage.label()
        };
        tracing::dispatcher::with_default(&a.dispatcher, || match &a.kind {
            Kind::Rpc => tracing::info!(
                event = "rpc", trace_id = %a.trace_id, stage, outcome = a.outcome, elapsed_ms,
                truth_ms = a.ms(Stage::Truth), plan_load_ms = a.ms(Stage::PlanLoad),
                protection_ms = a.ms(Stage::Protection), "reconcile timing"
            ),
            Kind::Fullscan => tracing::info!(
                event = "fullscan", trace_id = %a.trace_id, stage, outcome = a.outcome, elapsed_ms,
                exchange_read_ms = a.ms(Stage::ExchangeRead), local_load_ms = a.ms(Stage::LocalLoad),
                algo_queries_ms = a.ms(Stage::AlgoQueries), order_queries_ms = a.ms(Stage::OrderQueries),
                finalize_ms = a.ms(Stage::Finalize), local_orders = a.local_orders,
                local_protections = a.local_protections, algo_queries = a.algo_queries,
                order_queries = a.order_queries, "reconcile timing"
            ),
            Kind::Http {
                method,
                path,
                class,
                attempt_id,
            } => tracing::info!(
                event = "http_attempt", trace_id = %a.trace_id, method, path,
                request_class = class, attempt_id, stage, outcome = a.outcome, elapsed_ms,
                admission_ms = a.ms(Stage::Admission), transport_gate_ms = a.ms(Stage::TransportGate),
                http_ms = a.ms(Stage::Http), postprocess_ms = a.ms(Stage::Postprocess),
                http_status = a.http_status, "reconcile timing"
            ),
        });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    pub(crate) type Records = Arc<Mutex<Vec<BTreeMap<String, String>>>>;
    struct Capture(Records);
    struct Fields(BTreeMap<String, String>);
    impl tracing::field::Visit for Fields {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            if fields
                .0
                .get("message")
                .is_some_and(|v| v == "reconcile timing")
            {
                self.0.lock().unwrap().push(fields.0);
            }
        }
    }
    pub(crate) fn capture() -> (tracing::Dispatch, Records) {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Capture(records.clone()));
        (tracing::Dispatch::new(subscriber), records)
    }

    #[tokio::test]
    async fn concurrent_scopes_keep_distinct_ids_and_unscoped_work_emits_nothing() {
        let (dispatch, records) = capture();
        async {
            let mut outside = Timer::rpc();
            outside.finish(true);
            drop(outside);
            let first = scope(async {
                let mut timer = Timer::rpc();
                tokio::task::yield_now().await;
                timer.finish(false);
            });
            let second = scope(async {
                let mut timer = Timer::rpc();
                tokio::task::yield_now().await;
                timer.finish(true);
            });
            tokio::join!(first, second);
            drop(Timer::fullscan());
        }
        .with_subscriber(dispatch)
        .await;
        let events = records.lock().unwrap();
        assert_eq!(
            events.len(),
            2,
            "only the two explicitly scoped calls may emit"
        );
        assert_ne!(events[0]["trace_id"], events[1]["trace_id"]);
        assert!(events
            .iter()
            .any(|e| e["outcome"] == "returned_error" && e["stage"] == "truth"));
        assert!(events
            .iter()
            .any(|e| e["outcome"] == "ok" && e["stage"] == "complete"));
    }

    #[tokio::test]
    async fn dropping_pending_scope_keeps_id_and_reports_the_last_stage() {
        let (dispatch, records) = capture();
        let mut pending = Box::pin(
            scope(async {
                let mut timer = Timer::rpc();
                timer.stage(Stage::Protection);
                std::future::pending::<()>().await;
                timer.finish(true);
            })
            .with_subscriber(dispatch),
        );
        assert!(futures_util::poll!(&mut pending).is_pending());
        drop(pending);
        let events = records.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(!events[0]["trace_id"].is_empty());
        assert_eq!(events[0]["outcome"], "dropped");
        assert_eq!(events[0]["stage"], "protection");
    }
}
