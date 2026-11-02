//! Passive bounded observations of existing reconcile requests, never authority.
//! Pool Weak identity prevents address reuse and does not retain a closed Store.
use crate::{proto_v2 as p, store::Store};
use serde_json::Value;
use sqlx::{Any, Pool};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, Weak};

const MAX_ENTRIES: usize = 128;
const MAX_ROWS: usize = 1000;
const WINDOW_MS: i64 = 30_000;
const SOURCE: &str = "rustcore_reconcile_cache";

struct Entry {
    pool: Weak<Pool<Any>>,
    credential: String,
    boot: String,
    attempt: p::AccountOrderReadAttempt,
    snapshot: Option<Arc<p::VenueAccountOrderObservation>>,
    last_good: Option<Arc<p::VenueAccountOrderObservation>>,
}
fn registry() -> &'static Mutex<Vec<Entry>> {
    static REGISTRY: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}
fn matching(e: &Entry, pool: &Weak<Pool<Any>>, credential: &str, boot: &str) -> bool {
    e.pool.ptr_eq(pool) && e.credential == credential && e.boot == boot
}

pub(crate) struct Observation {
    pool: Weak<Pool<Any>>,
    credential: String,
    boot: String,
    attempt: p::AccountOrderReadAttempt,
    ordinary: Option<p::VenueAccountOrderSection>,
    algo: Option<p::VenueAccountOrderSection>,
    finished: bool,
}
impl Observation {
    pub(crate) fn begin(store: &Store, credential: &str, coverage: &str, now: i64) -> Self {
        let pool = store.weak_pool_identity();
        let attempt = p::AccountOrderReadAttempt {
            schema_version: 1,
            observation_id: ulid::Ulid::new().to_string(),
            status: "in_progress".into(),
            coverage: coverage.into(),
            started_at_ms: now,
            completed_at_ms: None,
        };
        let mut entries = registry().lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|e| e.pool.strong_count() > 0);
        // A new attempt invalidates current completeness, but last_good remains
        // available for stale display with its original source time and identity.
        if let Some(e) = entries
            .iter_mut()
            .find(|e| matching(e, &pool, credential, store.boot_id()))
        {
            e.attempt = attempt.clone();
            e.snapshot = None;
        } else {
            if entries.len() >= MAX_ENTRIES {
                let oldest = entries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, e)| e.attempt.started_at_ms)
                    .map(|(i, _)| i)
                    .unwrap();
                entries.swap_remove(oldest);
            }
            entries.push(Entry {
                pool: pool.clone(),
                credential: credential.into(),
                boot: store.boot_id().into(),
                attempt: attempt.clone(),
                snapshot: None,
                last_good: None,
            });
        }
        Self {
            pool,
            credential: credential.into(),
            boot: store.boot_id().into(),
            attempt,
            ordinary: None,
            algo: None,
            finished: false,
        }
    }
    pub(crate) fn ordinary(&mut self, raw: &Value, started: i64, completed: i64) {
        append_section(&mut self.ordinary, section(raw, false, started, completed));
    }
    pub(crate) fn algo(&mut self, raw: &Value, started: i64, completed: i64) {
        append_section(&mut self.algo, section(raw, true, started, completed));
    }
    pub(crate) fn finish(mut self, truth: crate::kill_switch::ReconcileTruth, now: i64) {
        self.finished = true;
        self.attempt.status = "succeeded".into();
        self.attempt.completed_at_ms = Some(now);
        let snapshot = p::VenueAccountOrderObservation {
            schema_version: 1,
            availability: p::AccountOrderAvailability::AoaUnknown as i32,
            reason_code: "not_measured".into(),
            source_kind: SOURCE.into(),
            observation_id: Some(self.attempt.observation_id.clone()),
            credential_id: self.credential.clone(),
            engine_boot_id: self.boot.clone(),
            capture_started_at_ms: Some(self.attempt.started_at_ms),
            capture_completed_at_ms: Some(now),
            source_as_of_ms: Some(self.attempt.started_at_ms),
            ordinary: self.ordinary.take(),
            algo: self.algo.take(),
            reconcile_truth: Some(p::AccountOrderReconcileTruth {
                schema_version: 1,
                has_diff: truth.has_diff,
                all_positions_zero: truth.all_positions_zero,
                all_open_orders_empty: truth.all_open_orders_empty,
                all_open_algo_orders_empty: truth.all_open_algo_orders_empty,
                local_position_zero_diff: truth.local_position_zero_diff,
            }),
            latest_attempt: Some(self.attempt.clone()),
        };
        self.publish(Some(snapshot));
    }
    fn publish(&self, snapshot: Option<p::VenueAccountOrderObservation>) {
        let mut entries = registry().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = entries
            .iter_mut()
            .find(|e| matching(e, &self.pool, &self.credential, &self.boot))
        {
            // An older slow completion must not replace a newer failed attempt.
            if e.attempt.observation_id == self.attempt.observation_id {
                e.attempt = self.attempt.clone();
                e.snapshot = snapshot.map(Arc::new);
                if let Some(snapshot) = e
                    .snapshot
                    .as_ref()
                    .filter(|s| full_candidate(s, &e.attempt))
                {
                    e.last_good = Some(snapshot.clone());
                }
            }
        }
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        if !self.finished {
            self.attempt.status = "failed".into();
            self.attempt.completed_at_ms = Some(crate::svc::now_ms());
            self.publish(None);
        }
    }
}

// A last-good entry is a complete same-round source observation. This is not
// a closure or protection-health verdict; has_diff and all truth bits stay raw.
fn full_candidate(
    snapshot: &p::VenueAccountOrderObservation,
    attempt: &p::AccountOrderReadAttempt,
) -> bool {
    if attempt.status != "succeeded" || attempt.coverage != "full_account" {
        return false;
    }
    let Some((start, end)) = snapshot
        .capture_started_at_ms
        .zip(snapshot.capture_completed_at_ms)
    else {
        return false;
    };
    if start <= 0 || end < start || end - start > WINDOW_MS {
        return false;
    }
    [&snapshot.ordinary, &snapshot.algo]
        .into_iter()
        .all(|section| {
            section.as_ref().is_some_and(|section| {
                section.meta.as_ref().is_some_and(|meta| {
                    !meta.overflow
                        && meta.coverage == p::AccountOrderCoverage::AocCompleteVenue as i32
                        && meta.returned_count as usize == section.rows.len()
                        && meta.total_count == Some(section.rows.len() as u32)
                }) && section.rows.iter().all(|row| row.identity_valid)
                    && section
                        .started_at_ms
                        .is_some_and(|n| n >= start && n <= end)
                    && section
                        .completed_at_ms
                        .is_some_and(|n| n >= section.started_at_ms.unwrap_or(i64::MAX) && n <= end)
            })
        })
}

/// Call before selection/client construction; failures and StreamOnly are not
/// allowed to leave an earlier full round apparently fresh. No REST is added.
pub(crate) fn not_observed(store: &Store, credential: &str, now: i64) {
    let mut guard = Observation::begin(store, credential, "stream_only", now);
    guard.finished = true;
    guard.attempt.status = "not_observed".into();
    guard.attempt.completed_at_ms = Some(now);
    guard.publish(None);
}
fn append_section(
    target: &mut Option<p::VenueAccountOrderSection>,
    mut next: p::VenueAccountOrderSection,
) {
    let Some(current) = target.as_mut() else {
        *target = Some(next);
        return;
    };
    let current_meta = current.meta.as_mut().expect("constructed section meta");
    let next_meta = next.meta.take().expect("constructed section meta");
    let total = current_meta
        .total_count
        .zip(next_meta.total_count)
        .and_then(|(a, b)| a.checked_add(b));
    current_meta.overflow |= next_meta.overflow || total.is_none_or(|n| n as usize > MAX_ROWS);
    if next_meta.coverage != p::AccountOrderCoverage::AocCompleteVenue as i32
        || current_meta.overflow
    {
        current_meta.coverage = p::AccountOrderCoverage::AocPartial as i32;
    }
    let mut ids: HashSet<String> = current
        .rows
        .iter()
        .filter_map(|r| r.order_id.as_ref().or(r.algo_id.as_ref()).cloned())
        .collect();
    for row in &mut next.rows {
        if row
            .order_id
            .as_ref()
            .or(row.algo_id.as_ref())
            .is_some_and(|id| !ids.insert(id.clone()))
        {
            row.identity_valid = false;
            current_meta.coverage = p::AccountOrderCoverage::AocPartial as i32;
        }
    }
    current
        .rows
        .extend(next.rows.into_iter().take(MAX_ROWS - current.rows.len()));
    current_meta.returned_count = current.rows.len() as u32;
    current_meta.total_count = if current_meta.overflow { None } else { total };
    current.completed_at_ms = next.completed_at_ms;
}

fn string(v: &Value, key: &str) -> Option<String> {
    let s = v.get(key)?.as_str()?;
    (!s.is_empty() && s.len() <= 128).then(|| s.to_owned())
}
fn id(v: &Value, key: &str) -> Option<String> {
    let v = v.get(key)?;
    let text = if let Some(s) = v.as_str() {
        s.to_owned()
    } else {
        let n = v.as_u64()?;
        n.to_string()
    };
    (!text.is_empty()
        && text.len() <= 64
        && text.bytes().all(|b| b.is_ascii_digit())
        && text.bytes().any(|b| b != b'0'))
    .then_some(text)
}
fn timestamp(v: &Value) -> Option<i64> {
    let value = v.get("updateTime").or_else(|| v.get("time"))?;
    let n = if let Some(s) = value.as_str() {
        s.parse().ok()?
    } else {
        value.as_i64()?
    };
    (n > 0 && n <= 9_007_199_254_740_991).then_some(n)
}
fn decimal(v: &Value, key: &str) -> Option<String> {
    let value = string(v, key)?;
    crate::reconcile::snapshot_decimal_is_zero(&value)
        .is_some()
        .then_some(value)
}
fn boolean(v: &Value, key: &str) -> Option<bool> {
    match v.get(key)? {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s == "true" => Some(true),
        Value::String(s) if s == "false" => Some(false),
        _ => None,
    }
}
fn section(raw: &Value, algo: bool, started: i64, completed: i64) -> p::VenueAccountOrderSection {
    let Some(values) = raw.as_array() else {
        return p::VenueAccountOrderSection {
            schema_version: 1,
            meta: Some(p::AccountOrderSectionMeta {
                schema_version: 1,
                source_kind: SOURCE.into(),
                ..Default::default()
            }),
            rows: vec![],
            started_at_ms: Some(started),
            completed_at_ms: Some(completed),
        };
    };
    let mut identities = HashSet::new();
    let mut valid = true;
    let rows = values
        .iter()
        .take(MAX_ROWS)
        .map(|value| {
            let order_id = if algo { None } else { id(value, "orderId") };
            let algo_id = if algo { id(value, "algoId") } else { None };
            let symbol = string(value, "symbol");
            let status = string(value, if algo { "algoStatus" } else { "status" });
            let identity = if algo {
                algo_id.as_ref()
            } else {
                order_id.as_ref()
            };
            let identity_valid = identity.is_some_and(|id| identities.insert(id.clone()))
                && symbol.is_some()
                && status.is_some();
            valid &= identity_valid;
            p::VenueAccountOrder {
                schema_version: 1,
                order_id,
                client_order_id: if algo {
                    None
                } else {
                    string(value, "clientOrderId")
                },
                algo_id,
                client_algo_id: if algo {
                    string(value, "clientAlgoId")
                } else {
                    None
                },
                actual_order_id: if algo {
                    id(value, "actualOrderId")
                } else {
                    None
                },
                symbol,
                status,
                side: string(value, "side"),
                order_type: string(value, "type").or_else(|| string(value, "orderType")),
                quantity: decimal(value, "origQty").or_else(|| decimal(value, "quantity")),
                price: decimal(value, "price"),
                updated_at_ms: timestamp(value),
                identity_valid,
                trigger_price: decimal(value, "triggerPrice")
                    .or_else(|| decimal(value, "stopPrice")),
                reduce_only: boolean(value, "reduceOnly"),
                close_position: boolean(value, "closePosition"),
                position_side: string(value, "positionSide"),
            }
        })
        .collect();
    let mut meta = crate::account_order_read::meta(
        values.len(),
        MAX_ROWS,
        SOURCE,
        p::AccountOrderCoverage::AocCompleteVenue,
    );
    if !valid || completed < started || started <= 0 {
        meta.coverage = p::AccountOrderCoverage::AocPartial as i32;
    }
    p::VenueAccountOrderSection {
        schema_version: 1,
        meta: Some(meta),
        rows,
        started_at_ms: Some(started),
        completed_at_ms: Some(completed),
    }
}

#[allow(
    clippy::collapsible_if,
    reason = "Preserve the established account-observation control flow."
)]
pub(crate) fn read(
    store: &Store,
    credential: &str,
    limit: usize,
    now: i64,
) -> p::VenueAccountOrderObservation {
    let mut result = p::VenueAccountOrderObservation {
        schema_version: 1,
        availability: p::AccountOrderAvailability::AoaUnknown as i32,
        reason_code: if credential.starts_with("paper:") {
            "simulation_local_only"
        } else {
            "not_measured"
        }
        .into(),
        source_kind: SOURCE.into(),
        credential_id: credential.into(),
        engine_boot_id: store.boot_id().into(),
        ..Default::default()
    };
    if credential.starts_with("paper:") {
        return result;
    }
    let pool = store.weak_pool_identity();
    let entries = registry().lock().unwrap_or_else(|e| e.into_inner());
    let Some(entry) = entries
        .iter()
        .find(|e| matching(e, &pool, credential, store.boot_id()))
    else {
        return result;
    };
    result.latest_attempt = Some(entry.attempt.clone());
    let current_full = entry
        .snapshot
        .as_ref()
        .filter(|s| full_candidate(s, &entry.attempt));
    let Some(snapshot) = current_full
        .or(entry.last_good.as_ref())
        .or(entry.snapshot.as_ref())
    else {
        result.reason_code = match entry.attempt.status.as_str() {
            "failed" => "latest_attempt_failed",
            "in_progress" => "observation_in_progress",
            _ => "not_observed",
        }
        .into();
        return result;
    };
    result = snapshot.as_ref().clone();
    let snapshot_full = snapshot
        .latest_attempt
        .as_ref()
        .is_some_and(|a| a.coverage == "full_account");
    result.latest_attempt = Some(entry.attempt.clone());
    let same_observation =
        result.observation_id.as_deref() == Some(entry.attempt.observation_id.as_str());
    let started = result.capture_started_at_ms.unwrap_or(0);
    let completed = result.capture_completed_at_ms.unwrap_or(0);
    let mut reason = if entry.attempt.status != "succeeded" {
        match entry.attempt.status.as_str() {
            "failed" => "latest_attempt_failed",
            "in_progress" => "observation_in_progress",
            _ => "not_observed",
        }
    } else if entry.attempt.coverage != "full_account" {
        "partial_coverage"
    } else if !same_observation {
        "source_inconsistent"
    } else if started <= 0
        || started > now
        || completed < started
        || completed > now
        || completed - started > WINDOW_MS
    {
        "invalid_observation_time"
    } else if now - started > WINDOW_MS {
        "stale_measurement"
    } else {
        ""
    };
    for section in [&mut result.ordinary, &mut result.algo] {
        if let Some(section) = section {
            if let Some(meta) = &mut section.meta {
                if !snapshot_full {
                    meta.coverage = p::AccountOrderCoverage::AocPartial as i32;
                }
                if meta.overflow || section.rows.len() > limit {
                    if reason.is_empty() {
                        reason = "overflow";
                    }
                    meta.overflow = true;
                    meta.total_count = None;
                    meta.coverage = p::AccountOrderCoverage::AocPartial as i32;
                } else if meta.coverage != p::AccountOrderCoverage::AocCompleteVenue as i32
                    && reason.is_empty()
                {
                    reason = "source_inconsistent";
                }
                section.rows.truncate(limit);
                meta.returned_count = section.rows.len() as u32;
            } else if reason.is_empty() {
                reason = "source_inconsistent";
            }
            if section
                .started_at_ms
                .is_none_or(|n| n < started || n > completed)
                || section
                    .completed_at_ms
                    .is_none_or(|n| n < started || n > completed)
            {
                if reason.is_empty() {
                    reason = "invalid_observation_time";
                }
            }
        } else if reason.is_empty() {
            reason = "not_measured";
        }
    }
    if reason.is_empty() {
        result.availability = p::AccountOrderAvailability::AoaAvailable as i32;
        result.reason_code = "available".into();
    } else {
        result.availability = p::AccountOrderAvailability::AoaUnknown as i32;
        result.reason_code = reason.into();
    }
    result
}

#[cfg(test)]
#[path = "account_order_observation_tests.rs"]
mod tests;
