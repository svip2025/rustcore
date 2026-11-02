use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

pub(crate) const BINANCE_IP_BAN_COOLDOWN_MS: u64 = 120_000;

const RETRY_BASE_MS: u64 = 100;
const RETRY_DELAY_CAP_MS: u64 = 2_000;
const NORMAL_MAX_RETRIES: u32 = 3;
const NORMAL_TOTAL_BUDGET_MS: u64 = 10_000;
const EXIT_MAX_RETRIES: u32 = 1;
const EXIT_TOTAL_BUDGET_MS: u64 = 750;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryClass {
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
}

impl RetryClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::IdempotentWrite => "idempotent_write",
            Self::NonIdempotentWrite => "non_idempotent_write",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryScope {
    Normal,
    Exit,
}

impl RetryScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Exit => "exit",
        }
    }

    fn limits(self) -> (u32, u64) {
        match self {
            Self::Normal => (NORMAL_MAX_RETRIES, NORMAL_TOTAL_BUDGET_MS),
            Self::Exit => (EXIT_MAX_RETRIES, EXIT_TOTAL_BUDGET_MS),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    Retry { after_ms: u64, retry_number: u32 },
    VerifyTruth { after_ms: u64 },
    GiveUp { reason: &'static str },
    IpBan { cooldown_ms: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetryInput {
    pub status: u16,
    pub class: RetryClass,
    pub scope: RetryScope,
    pub completed_retries: u32,
    pub elapsed_ms: u64,
    pub retry_after_ms: Option<u64>,
    pub jitter_ms: u64,
}

/// Returns the complete disposition for a rate-limit response. Callers only
/// match on the outcome; there is no boolean polarity to invert.
pub(crate) fn retry_decision(input: RetryInput) -> RetryDecision {
    if input.status == 418 {
        return RetryDecision::IpBan {
            cooldown_ms: input
                .retry_after_ms
                .unwrap_or_default()
                .max(BINANCE_IP_BAN_COOLDOWN_MS),
        };
    }
    if input.status != 429 {
        return RetryDecision::GiveUp {
            reason: "http_status_not_retryable",
        };
    }

    let (max_retries, total_budget_ms) = input.scope.limits();
    let exponential_ms = RETRY_BASE_MS
        .saturating_mul(
            1_u64
                .checked_shl(input.completed_retries)
                .unwrap_or(u64::MAX),
        )
        .min(RETRY_DELAY_CAP_MS);
    // Retry-After is authoritative when present. It is never shortened by the
    // local cap or jitter. If it does not fit the bounded exit/normal budget,
    // the safe outcome is an explicit failure, not a more aggressive retry.
    let after_ms = input.retry_after_ms.unwrap_or_else(|| {
        exponential_ms
            .saturating_add(input.jitter_ms)
            .min(RETRY_DELAY_CAP_MS)
    });
    let remaining_ms = total_budget_ms.saturating_sub(input.elapsed_ms);
    if after_ms > remaining_ms {
        return RetryDecision::GiveUp {
            reason: "retry_budget_exhausted",
        };
    }

    if input.class == RetryClass::NonIdempotentWrite {
        return RetryDecision::VerifyTruth { after_ms };
    }
    if input.completed_retries >= max_retries {
        return RetryDecision::GiveUp {
            reason: "retry_limit_exhausted",
        };
    }
    RetryDecision::Retry {
        after_ms,
        retry_number: input.completed_retries + 1,
    }
}

pub(crate) fn deterministic_jitter_ms(seed: &str, retry_number: u32) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in seed
        .as_bytes()
        .iter()
        .copied()
        .chain(retry_number.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let exponential_ms = RETRY_BASE_MS
        .saturating_mul(
            1_u64
                .checked_shl(retry_number.saturating_sub(1))
                .unwrap_or(u64::MAX),
        )
        .min(RETRY_DELAY_CAP_MS);
    hash % (exponential_ms / 2 + 1)
}

pub(crate) fn parse_retry_after_ms(raw: Option<&str>, now_ms: i64) -> Option<u64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(seconds) = raw.parse::<u64>() {
        return seconds.checked_mul(1_000);
    }
    let at = chrono::DateTime::parse_from_rfc2822(raw).ok()?;
    Some(at.timestamp_millis().saturating_sub(now_ms).max(0) as u64)
}

#[derive(Clone)]
pub struct CredentialRateLimiter {
    inner: Arc<Mutex<HashMap<String, TokenBucket>>>,
    queues: Arc<std::sync::Mutex<HashMap<String, EgressQueue>>>,
    meter: EgressMeter,
    default: TokenBucketConfig,
    governor: EgressGovernorConfig,
    clock: Arc<dyn crate::svc::Clock>,
}

/// 进程内**唯一**的出口桶注册表。
///
/// 之前它带着 `#[cfg(not(test))]`，于是"所有 limiter 收敛到同一个注册表"这条
/// 头号修复（RL-2 / A-05）**被编译出了每一个测试二进制**：把构造器整个回退成
/// 每实例 HashMap 仍然全绿。生产结构必须在测试里可达，否则它没有被任何东西钉住。
fn shared_egress_buckets() -> Arc<Mutex<HashMap<String, TokenBucket>>> {
    static BUCKETS: OnceLock<Arc<Mutex<HashMap<String, TokenBucket>>>> = OnceLock::new();
    BUCKETS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

fn shared_egress_queues() -> Arc<std::sync::Mutex<HashMap<String, EgressQueue>>> {
    static QUEUES: OnceLock<Arc<std::sync::Mutex<HashMap<String, EgressQueue>>>> = OnceLock::new();
    QUEUES
        .get_or_init(|| Arc::new(std::sync::Mutex::new(HashMap::new())))
        .clone()
}

#[cfg(test)]
pub(crate) async fn clear_shared_egress_bucket(egress_scope: &str) {
    shared_egress_buckets().lock().await.remove(egress_scope);
    shared_egress_queues()
        .lock()
        .expect("Binance egress queue registry lock poisoned")
        .remove(egress_scope);
    EgressMeter::shared(EgressGovernorConfig::from_env()).clear(egress_scope);
}

const MINUTE_MS: i64 = 60_000;
const BUDGET_MISMATCH_WARN_INTERVAL_MS: i64 = 60 * 60 * 1_000;
const REST_WEIGHT_BUDGET_ENV: &str = "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M";
const WS_WEIGHT_BUDGET_ENV: &str = "RUSTCORE_BINANCE_WS_WEIGHT_BUDGET_1M";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BudgetSource {
    Default,
    Observed,
    Env,
}

impl BudgetSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Observed => "observed",
            Self::Env => "env",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EffectiveEgressBudget {
    pub budget_1m: u64,
    pub source: BudgetSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EgressGovernorConfig {
    pub budget_1m: u64,
    pub queue_wait_max_ms: u64,
    pub fair_share_burst: u32,
    budget_env_base: &'static str,
    fallback_budget_source: BudgetSource,
}

impl EgressGovernorConfig {
    pub(crate) fn from_env() -> Self {
        Self::from_raw_with_budget_env(
            REST_WEIGHT_BUDGET_ENV,
            std::env::var(REST_WEIGHT_BUDGET_ENV).ok().as_deref(),
            true,
            std::env::var("RUSTCORE_BINANCE_QUEUE_WAIT_MAX_MS")
                .ok()
                .as_deref(),
            std::env::var("RUSTCORE_BINANCE_FAIR_SHARE_BURST")
                .ok()
                .as_deref(),
        )
    }

    /// ws-fapi owns a Binance-documented IP weight pool that is independent
    /// from REST.  Its published generic page does not state a numeric limit,
    /// so this deliberately uses the operator override and an explicitly
    /// unverified 2400/minute default until a response supplies `rateLimits`.
    pub(crate) fn from_ws_env() -> Self {
        Self::from_raw_with_budget_env(
            WS_WEIGHT_BUDGET_ENV,
            std::env::var(WS_WEIGHT_BUDGET_ENV).ok().as_deref(),
            true,
            std::env::var("RUSTCORE_BINANCE_QUEUE_WAIT_MAX_MS")
                .ok()
                .as_deref(),
            std::env::var("RUSTCORE_BINANCE_FAIR_SHARE_BURST")
                .ok()
                .as_deref(),
        )
    }

    #[cfg(test)]
    fn from_raw(
        budget_1m: Option<&str>,
        queue_wait_ms: Option<&str>,
        fair_burst: Option<&str>,
    ) -> Self {
        Self::from_raw_with_budget_env(
            REST_WEIGHT_BUDGET_ENV,
            budget_1m,
            false,
            queue_wait_ms,
            fair_burst,
        )
    }

    fn from_raw_with_budget_env(
        budget_env_base: &'static str,
        budget_1m: Option<&str>,
        budget_is_from_env: bool,
        queue_wait_ms: Option<&str>,
        fair_burst: Option<&str>,
    ) -> Self {
        let parsed_budget_1m = parse_positive_u64(budget_1m);
        Self {
            budget_1m: parsed_budget_1m.unwrap_or(2_400).clamp(100, 6_000),
            queue_wait_max_ms: parse_positive_u64(queue_wait_ms)
                .unwrap_or(15_000)
                .clamp(1_000, 60_000),
            fair_share_burst: parse_positive_u64(fair_burst).unwrap_or(3).clamp(1, 100) as u32,
            budget_env_base,
            fallback_budget_source: if budget_is_from_env && parsed_budget_1m.is_some() {
                BudgetSource::Env
            } else {
                BudgetSource::Default
            },
        }
    }

    pub(crate) fn scoped_budget_env_name(&self, egress_scope: &str) -> String {
        format!("{}__{egress_scope}", self.budget_env_base)
    }

    fn scoped_budget(&self, egress_scope: &str) -> Option<u64> {
        std::env::var(self.scoped_budget_env_name(egress_scope))
            .ok()
            .as_deref()
            .and_then(|value| parse_positive_u64(Some(value)))
            .map(|value| value.clamp(100, 6_000))
    }

    fn configured_budget(&self, egress_scope: &str) -> EffectiveEgressBudget {
        self.scoped_budget(egress_scope)
            .map(|budget_1m| EffectiveEgressBudget {
                budget_1m,
                source: BudgetSource::Env,
            })
            .unwrap_or(EffectiveEgressBudget {
                budget_1m: self.budget_1m,
                source: self.fallback_budget_source,
            })
    }

    pub(crate) fn effective_budget(
        &self,
        egress_scope: &str,
        observed_budget_1m: Option<u64>,
    ) -> EffectiveEgressBudget {
        if let Some(budget_1m) = self.scoped_budget(egress_scope) {
            return EffectiveEgressBudget {
                budget_1m,
                source: BudgetSource::Env,
            };
        }
        observed_budget_1m
            .filter(|value| *value > 0)
            .map(|budget_1m| EffectiveEgressBudget {
                budget_1m,
                source: BudgetSource::Observed,
            })
            .unwrap_or(EffectiveEgressBudget {
                budget_1m: self.budget_1m,
                source: self.fallback_budget_source,
            })
    }

    #[cfg(test)]
    pub(crate) fn for_test(budget_1m: u64, queue_wait_max_ms: u64, fair_share_burst: u32) -> Self {
        Self {
            budget_1m,
            queue_wait_max_ms,
            fair_share_burst,
            budget_env_base: REST_WEIGHT_BUDGET_ENV,
            fallback_budget_source: BudgetSource::Default,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Headroom {
    pub budget_1m: u64,
    pub last_used: u64,
    pub remaining: u64,
    pub observed_at_ms: i64,
    pub window_start_ms: i64,
    pub window_until_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParkState {
    Open,
    Soft,
    Hard,
}

impl ParkState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Soft => "soft",
            Self::Hard => "hard",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct EgressObservation {
    observed_budget_1m: Option<u64>,
    budget_1m: u64,
    budget_source: BudgetSource,
    last_used: u64,
    observed_at_ms: i64,
    window_start_ms: i64,
    has_usage_observation: bool,
    admitted_after_observation: u64,
    state: ParkState,
    last_info_at_ms: i64,
    last_budget_mismatch_warn_at_ms: i64,
    budget_mismatch_warning_count: u64,
    budget_exceeded_without_429_count: u64,
}

fn shared_egress_meter_registry() -> Arc<std::sync::Mutex<HashMap<String, EgressObservation>>> {
    static REGISTRY: OnceLock<Arc<std::sync::Mutex<HashMap<String, EgressObservation>>>> =
        OnceLock::new();
    REGISTRY
        .get_or_init(|| Arc::new(std::sync::Mutex::new(HashMap::new())))
        .clone()
}

#[derive(Clone)]
pub(crate) struct EgressMeter {
    inner: Arc<std::sync::Mutex<HashMap<String, EgressObservation>>>,
    config: EgressGovernorConfig,
}

impl EgressMeter {
    pub(crate) fn shared(config: EgressGovernorConfig) -> Self {
        Self {
            inner: shared_egress_meter_registry(),
            config,
        }
    }

    #[allow(dead_code)]
    fn isolated(config: EgressGovernorConfig) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
            config,
        }
    }

    pub(crate) fn observe(&self, egress_scope: &str, used_weight_1m: u64, now_ms: i64) {
        self.observe_usage(egress_scope, used_weight_1m, now_ms);
    }

    pub(crate) fn observe_with_limit(
        &self,
        egress_scope: &str,
        used_weight_1m: u64,
        limit_1m: u64,
        now_ms: i64,
    ) {
        self.observe_budget(egress_scope, limit_1m, now_ms);
        self.observe_usage(egress_scope, used_weight_1m, now_ms);
    }

    pub(crate) fn observe_budget(&self, egress_scope: &str, observed_budget_1m: u64, now_ms: i64) {
        if observed_budget_1m == 0 {
            return;
        }
        let window_start_ms = minute_window_start(now_ms);
        let configured = self.config.configured_budget(egress_scope);
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let entry = registry
            .entry(egress_scope.to_string())
            .or_insert_with(|| EgressObservation {
                observed_budget_1m: None,
                budget_1m: configured.budget_1m,
                budget_source: configured.source,
                last_used: 0,
                observed_at_ms: now_ms,
                window_start_ms,
                has_usage_observation: false,
                admitted_after_observation: 0,
                state: ParkState::Open,
                last_info_at_ms: i64::MIN / 2,
                last_budget_mismatch_warn_at_ms: i64::MIN / 2,
                budget_mismatch_warning_count: 0,
                budget_exceeded_without_429_count: 0,
            });
        let previous_state = entry.state;
        entry.observed_budget_1m = Some(observed_budget_1m);
        let effective = self
            .config
            .effective_budget(egress_scope, entry.observed_budget_1m);
        entry.budget_1m = effective.budget_1m;
        entry.budget_source = effective.source;
        if entry.has_usage_observation && entry.window_start_ms == window_start_ms {
            entry.state = park_state(entry.last_used, entry.budget_1m);
        }
        let current_state = entry.state;
        let last_used = entry.last_used;
        let warn_mismatch = observed_budget_1m != configured.budget_1m
            && now_ms.saturating_sub(entry.last_budget_mismatch_warn_at_ms)
                >= BUDGET_MISMATCH_WARN_INTERVAL_MS;
        if warn_mismatch {
            entry.last_budget_mismatch_warn_at_ms = now_ms;
            entry.budget_mismatch_warning_count =
                entry.budget_mismatch_warning_count.saturating_add(1);
        }
        drop(registry);
        if warn_mismatch {
            tracing::warn!(
                egress_scope = %egress_scope,
                observed_budget_1m,
                configured_budget_1m = configured.budget_1m,
                effective_budget_1m = effective.budget_1m,
                budget_source = effective.source.as_str(),
                "observed Binance request-weight limit differs from the configured budget"
            );
        }
        log_park_transition(egress_scope, previous_state, current_state, last_used);
    }

    fn observe_usage(&self, egress_scope: &str, used_weight_1m: u64, now_ms: i64) {
        let window_start_ms = minute_window_start(now_ms);
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let previous_state = registry
            .get(egress_scope)
            .filter(|value| value.has_usage_observation && value.window_start_ms == window_start_ms)
            .map(|value| value.state)
            .unwrap_or(ParkState::Open);
        if registry.get(egress_scope).is_some_and(|value| {
            value.has_usage_observation
                && value.window_start_ms == window_start_ms
                && value.observed_at_ms > now_ms
        }) {
            return;
        }
        let previous = registry.get(egress_scope).filter(|value| {
            value.has_usage_observation && value.window_start_ms == window_start_ms
        });
        let observed_budget_1m = registry
            .get(egress_scope)
            .and_then(|value| value.observed_budget_1m);
        let effective = self
            .config
            .effective_budget(egress_scope, observed_budget_1m);
        let last_used = previous
            .map(|value| value.last_used.max(used_weight_1m))
            .unwrap_or(used_weight_1m);
        let admitted_after_observation = previous
            .filter(|value| used_weight_1m <= value.last_used)
            .map(|value| value.admitted_after_observation)
            .unwrap_or_default();
        let state = park_state(last_used, effective.budget_1m);
        let last_info_at_ms = registry
            .get(egress_scope)
            .map(|value| value.last_info_at_ms)
            .unwrap_or(i64::MIN / 2);
        let last_budget_mismatch_warn_at_ms = registry
            .get(egress_scope)
            .map(|value| value.last_budget_mismatch_warn_at_ms)
            .unwrap_or(i64::MIN / 2);
        let budget_mismatch_warning_count = registry
            .get(egress_scope)
            .map(|value| value.budget_mismatch_warning_count)
            .unwrap_or_default();
        let budget_exceeded_without_429_count = registry
            .get(egress_scope)
            .map(|value| value.budget_exceeded_without_429_count)
            .unwrap_or_default();
        registry.insert(
            egress_scope.to_string(),
            EgressObservation {
                observed_budget_1m,
                budget_1m: effective.budget_1m,
                budget_source: effective.source,
                last_used,
                observed_at_ms: now_ms,
                window_start_ms,
                has_usage_observation: true,
                admitted_after_observation,
                state,
                last_info_at_ms,
                last_budget_mismatch_warn_at_ms,
                budget_mismatch_warning_count,
                budget_exceeded_without_429_count,
            },
        );
        drop(registry);
        log_park_transition(egress_scope, previous_state, state, last_used);
    }

    pub(crate) fn headroom(&self, egress_scope: &str, now_ms: i64) -> Option<Headroom> {
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let expired = registry.get(egress_scope).is_some_and(|value| {
            value.has_usage_observation && value.window_start_ms != minute_window_start(now_ms)
        });
        if expired {
            if let Some(previous) = registry.get_mut(egress_scope) {
                let previous_state = previous.state;
                let previous_used = previous.last_used;
                previous.has_usage_observation = false;
                previous.admitted_after_observation = 0;
                previous.state = ParkState::Open;
                drop(registry);
                log_park_transition(egress_scope, previous_state, ParkState::Open, previous_used);
            }
            return None;
        }
        registry
            .get(egress_scope)
            .filter(|value| value.has_usage_observation)
            .map(|value| Headroom {
                budget_1m: value.budget_1m,
                last_used: value.last_used,
                remaining: value.budget_1m.saturating_sub(value.last_used),
                observed_at_ms: value.observed_at_ms,
                window_start_ms: value.window_start_ms,
                window_until_ms: value.window_start_ms.saturating_add(MINUTE_MS),
            })
    }

    fn effective_remaining(&self, egress_scope: &str, now_ms: i64) -> Option<f64> {
        let headroom = self.headroom(egress_scope, now_ms)?;
        let registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let projected = registry
            .get(egress_scope)
            .map(|value| value.admitted_after_observation)
            .unwrap_or_default();
        Some(headroom.remaining.saturating_sub(projected) as f64)
    }

    #[cfg(test)]
    fn effective_budget(&self, egress_scope: &str) -> EffectiveEgressBudget {
        let observed_budget_1m = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned")
            .get(egress_scope)
            .and_then(|value| value.observed_budget_1m);
        self.config
            .effective_budget(egress_scope, observed_budget_1m)
    }

    fn record_budget_exceeded_without_429(
        &self,
        egress_scope: &str,
        http_status: u16,
    ) -> Option<(u64, u64, u64, BudgetSource)> {
        if http_status == 429 {
            return None;
        }
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let value = registry.get_mut(egress_scope)?;
        if !value.has_usage_observation || value.last_used <= value.budget_1m {
            return None;
        }
        value.budget_exceeded_without_429_count =
            value.budget_exceeded_without_429_count.saturating_add(1);
        Some((
            value.budget_exceeded_without_429_count,
            value.last_used,
            value.budget_1m,
            value.budget_source,
        ))
    }

    fn reserve(&self, egress_scope: &str, cost: f64, now_ms: i64) {
        if cost <= 0.0 || !cost.is_finite() {
            return;
        }
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        if let Some(value) = registry.get_mut(egress_scope).filter(|value| {
            value.has_usage_observation && value.window_start_ms == minute_window_start(now_ms)
        }) {
            value.admitted_after_observation = value
                .admitted_after_observation
                .saturating_add(cost.ceil() as u64);
        }
    }

    fn release(&self, egress_scope: &str, cost: f64, now_ms: i64) {
        if cost <= 0.0 || !cost.is_finite() {
            return;
        }
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        if let Some(value) = registry.get_mut(egress_scope).filter(|value| {
            value.has_usage_observation && value.window_start_ms == minute_window_start(now_ms)
        }) {
            value.admitted_after_observation = value
                .admitted_after_observation
                .saturating_sub(cost.ceil() as u64);
        }
    }

    fn snapshot_for_log(&self, egress_scope: &str, now_ms: i64) -> GovernorLogSnapshot {
        let _ = self.headroom(egress_scope, now_ms);
        let mut registry = self
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned");
        let Some(value) = registry.get_mut(egress_scope) else {
            let effective = self.config.effective_budget(egress_scope, None);
            return GovernorLogSnapshot {
                last_used: None,
                headroom: None,
                observed_budget_1m: None,
                effective_budget_1m: effective.budget_1m,
                budget_source: effective.source,
                state: ParkState::Open,
                should_log: false,
            };
        };
        let should_log = now_ms.saturating_sub(value.last_info_at_ms) >= 30_000;
        if should_log {
            value.last_info_at_ms = now_ms;
        }
        GovernorLogSnapshot {
            last_used: value.has_usage_observation.then_some(value.last_used),
            headroom: value.has_usage_observation.then_some(
                value
                    .budget_1m
                    .saturating_sub(value.last_used)
                    .saturating_sub(value.admitted_after_observation),
            ),
            observed_budget_1m: value.observed_budget_1m,
            effective_budget_1m: value.budget_1m,
            budget_source: value.budget_source,
            state: value.state,
            should_log,
        }
    }

    #[cfg(test)]
    fn clear(&self, egress_scope: &str) {
        self.inner
            .lock()
            .expect("Binance egress meter registry lock poisoned")
            .remove(egress_scope);
    }
}

#[derive(Clone, Copy, Debug)]
struct GovernorLogSnapshot {
    last_used: Option<u64>,
    headroom: Option<u64>,
    observed_budget_1m: Option<u64>,
    effective_budget_1m: u64,
    budget_source: BudgetSource,
    state: ParkState,
    should_log: bool,
}

fn minute_window_start(now_ms: i64) -> i64 {
    now_ms.div_euclid(MINUTE_MS).saturating_mul(MINUTE_MS)
}

fn threshold_reached(used: u64, budget: u64, percent: u64) -> bool {
    used.saturating_mul(100) >= budget.saturating_mul(percent)
}

fn park_state(used: u64, budget: u64) -> ParkState {
    if threshold_reached(used, budget, 97) {
        ParkState::Hard
    } else if threshold_reached(used, budget, 90) {
        ParkState::Soft
    } else {
        ParkState::Open
    }
}

fn log_park_transition(scope: &str, previous: ParkState, current: ParkState, last_used: u64) {
    if previous == current {
        return;
    }
    if previous != ParkState::Open {
        let error_code = match previous {
            ParkState::Soft => "BINANCE_EGRESS_SOFT_PARK_CLEARED",
            ParkState::Hard => "BINANCE_EGRESS_HARD_PARK_CLEARED",
            ParkState::Open => unreachable!(),
        };
        tracing::warn!(egress_scope = %scope, last_used, error_code, "binance egress park cleared");
    }
    if current != ParkState::Open {
        let error_code = match current {
            ParkState::Soft => "BINANCE_EGRESS_SOFT_PARK",
            ParkState::Hard => "BINANCE_EGRESS_HARD_PARK",
            ParkState::Open => unreachable!(),
        };
        tracing::warn!(egress_scope = %scope, last_used, error_code, "binance egress park entered");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RequestClass {
    Exit,
    Protection,
    EventTruth,
    PeriodicReconcile,
    Entry,
}

impl RequestClass {
    fn priority(self) -> u8 {
        match self {
            Self::Exit => 5,
            Self::Protection => 4,
            Self::EventTruth => 3,
            Self::Entry => 2,
            Self::PeriodicReconcile => 1,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Exit => "exit",
            Self::Protection => "protection",
            Self::EventTruth => "event_truth",
            Self::PeriodicReconcile => "periodic_reconcile",
            Self::Entry => "entry",
        }
    }

    fn queues(self) -> bool {
        !matches!(self, Self::Exit | Self::Protection)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    Admitted,
    TimedOut,
    HardRejected,
    ReserveExhausted,
}

#[derive(Clone)]
struct QueueWaiter {
    id: u64,
    credential_id: String,
    class: RequestClass,
    notify: Arc<Notify>,
}

/// The waiting future owns its registration. Dropping that future (including
/// timeout/select/abort) synchronously removes it and wakes its successors.
/// Admission spends budget independently; removing a waiter never refunds it.
struct QueueRegistration<'a> {
    limiter: &'a CredentialRateLimiter,
    egress_scope: &'a str,
    waiter_id: u64,
}

impl Drop for QueueRegistration<'_> {
    fn drop(&mut self) {
        self.limiter
            .remove_waiter(self.egress_scope, self.waiter_id);
    }
}

#[derive(Default)]
struct FairTurn {
    last_credential: Option<String>,
    consecutive: u32,
}

#[derive(Default)]
struct EgressQueue {
    next_id: u64,
    waiters: VecDeque<QueueWaiter>,
    turns: HashMap<RequestClass, FairTurn>,
}

impl EgressQueue {
    fn selected_waiter(&self, fair_share_burst: u32) -> Option<u64> {
        let priority = self
            .waiters
            .iter()
            .map(|waiter| waiter.class.priority())
            .max()?;
        let candidates = self
            .waiters
            .iter()
            .filter(|waiter| waiter.class.priority() == priority)
            .collect::<Vec<_>>();
        let class = candidates.first()?.class;
        let turn = self.turns.get(&class);
        if let Some(turn) = turn {
            if turn.consecutive >= fair_share_burst
                && candidates.iter().any(|waiter| {
                    turn.last_credential.as_deref() != Some(waiter.credential_id.as_str())
                })
            {
                return candidates
                    .into_iter()
                    .find(|waiter| {
                        turn.last_credential.as_deref() != Some(waiter.credential_id.as_str())
                    })
                    .map(|waiter| waiter.id);
            }
        }
        candidates.first().map(|waiter| waiter.id)
    }

    fn record_admission(&mut self, waiter: &QueueWaiter) {
        let turn = self.turns.entry(waiter.class).or_default();
        if turn.last_credential.as_deref() == Some(waiter.credential_id.as_str()) {
            turn.consecutive = turn.consecutive.saturating_add(1);
        } else {
            turn.last_credential = Some(waiter.credential_id.clone());
            turn.consecutive = 1;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderCountConfig {
    pub one_minute: u64,
    pub ten_seconds: u64,
}

impl OrderCountConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            one_minute: positive_u64_env("RUSTCORE_BINANCE_ORDER_COUNT_1M", 1_200),
            ten_seconds: positive_u64_env("RUSTCORE_BINANCE_ORDER_COUNT_10S", 300),
        }
    }

    pub(crate) fn describe_from_env() -> OrderCountBudgetProvenance {
        Self::from_raw(
            std::env::var("RUSTCORE_BINANCE_ORDER_COUNT_1M")
                .ok()
                .as_deref(),
            std::env::var("RUSTCORE_BINANCE_ORDER_COUNT_10S")
                .ok()
                .as_deref(),
        )
    }

    fn from_raw(one_minute: Option<&str>, ten_seconds: Option<&str>) -> OrderCountBudgetProvenance {
        OrderCountBudgetProvenance {
            config: Self {
                one_minute: parse_positive_u64(one_minute).unwrap_or(1_200),
                ten_seconds: parse_positive_u64(ten_seconds).unwrap_or(300),
            },
            one_minute_source: one_minute
                .map(|_| "RUSTCORE_BINANCE_ORDER_COUNT_1M")
                .unwrap_or("built-in default"),
            ten_seconds_source: ten_seconds
                .map(|_| "RUSTCORE_BINANCE_ORDER_COUNT_10S")
                .unwrap_or("built-in default"),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(one_minute: u64, ten_seconds: u64) -> Self {
        Self {
            one_minute,
            ten_seconds,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderCountBudgetProvenance {
    pub config: OrderCountConfig,
    pub one_minute_source: &'static str,
    pub ten_seconds_source: &'static str,
}

fn positive_u64_env(name: &str, fallback: u64) -> u64 {
    parse_positive_u64(std::env::var(name).ok().as_deref()).unwrap_or(fallback)
}

fn parse_positive_u64(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderAdmission {
    Admitted,
    Parked { until_ms: i64, window: &'static str },
}

#[derive(Default)]
struct CredentialOrderCounts {
    one_minute: VecDeque<i64>,
    ten_seconds: VecDeque<i64>,
    observed_one_minute_until_ms: i64,
    observed_ten_seconds_until_ms: i64,
}

fn shared_order_count_registry() -> Arc<std::sync::Mutex<HashMap<String, CredentialOrderCounts>>> {
    static REGISTRY: OnceLock<Arc<std::sync::Mutex<HashMap<String, CredentialOrderCounts>>>> =
        OnceLock::new();
    REGISTRY
        .get_or_init(|| Arc::new(std::sync::Mutex::new(HashMap::new())))
        .clone()
}

/// Binance ORDER COUNT is a per-account count in two simultaneous sliding
/// windows. It is deliberately separate from the IP request-weight bucket.
#[derive(Clone)]
pub(crate) struct OrderCountLimiter {
    inner: Arc<std::sync::Mutex<HashMap<String, CredentialOrderCounts>>>,
    config: OrderCountConfig,
}

impl OrderCountLimiter {
    /// Production constructor. Independently constructed clients share the
    /// same per-credential registry, just as they share the IP egress bucket.
    pub(crate) fn shared(config: OrderCountConfig) -> Self {
        Self {
            inner: shared_order_count_registry(),
            config,
        }
    }

    #[cfg(test)]
    pub(crate) fn isolated(config: OrderCountConfig) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
            config,
        }
    }

    pub(crate) fn admit_entry(&self, credential_id: &str, now_ms: i64) -> OrderAdmission {
        const ONE_MINUTE_MS: i64 = 60_000;
        const TEN_SECONDS_MS: i64 = 10_000;
        let mut registry = self
            .inner
            .lock()
            .expect("Binance order-count registry lock poisoned");
        let counts = registry.entry(credential_id.to_string()).or_default();
        prune_window(&mut counts.one_minute, now_ms, ONE_MINUTE_MS);
        prune_window(&mut counts.ten_seconds, now_ms, TEN_SECONDS_MS);

        let mut parked: Option<(i64, &'static str)> = None;
        consider_park(
            &mut parked,
            counts.observed_one_minute_until_ms,
            "1m",
            now_ms,
        );
        consider_park(
            &mut parked,
            counts.observed_ten_seconds_until_ms,
            "10s",
            now_ms,
        );
        if counts.one_minute.len() as u64 >= self.config.one_minute {
            consider_park(
                &mut parked,
                counts
                    .one_minute
                    .front()
                    .copied()
                    .unwrap_or(now_ms)
                    .saturating_add(ONE_MINUTE_MS),
                "1m",
                now_ms,
            );
        }
        if counts.ten_seconds.len() as u64 >= self.config.ten_seconds {
            consider_park(
                &mut parked,
                counts
                    .ten_seconds
                    .front()
                    .copied()
                    .unwrap_or(now_ms)
                    .saturating_add(TEN_SECONDS_MS),
                "10s",
                now_ms,
            );
        }
        if let Some((until_ms, window)) = parked {
            return OrderAdmission::Parked { until_ms, window };
        }

        counts.one_minute.push_back(now_ms);
        counts.ten_seconds.push_back(now_ms);
        OrderAdmission::Admitted
    }

    pub(crate) fn observe_headers(
        &self,
        credential_id: &str,
        order_count_1m: Option<u64>,
        order_count_10s: Option<u64>,
        now_ms: i64,
    ) {
        let mut registry = self
            .inner
            .lock()
            .expect("Binance order-count registry lock poisoned");
        let counts = registry.entry(credential_id.to_string()).or_default();
        if order_count_1m.is_some_and(|value| value >= ninety_percent(self.config.one_minute)) {
            counts.observed_one_minute_until_ms = counts
                .observed_one_minute_until_ms
                .max(next_aligned_boundary(now_ms, 60_000));
        }
        if order_count_10s.is_some_and(|value| value >= ninety_percent(self.config.ten_seconds)) {
            counts.observed_ten_seconds_until_ms = counts
                .observed_ten_seconds_until_ms
                .max(next_aligned_boundary(now_ms, 10_000));
        }
    }
}

fn prune_window(values: &mut VecDeque<i64>, now_ms: i64, window_ms: i64) {
    let cutoff = now_ms.saturating_sub(window_ms);
    while values.front().is_some_and(|value| *value <= cutoff) {
        values.pop_front();
    }
}

fn consider_park(
    parked: &mut Option<(i64, &'static str)>,
    until_ms: i64,
    window: &'static str,
    now_ms: i64,
) {
    if until_ms <= now_ms {
        return;
    }
    if parked.is_none_or(|(current, _)| until_ms > current) {
        *parked = Some((until_ms, window));
    }
}

fn ninety_percent(capacity: u64) -> u64 {
    capacity.saturating_mul(9).saturating_add(9) / 10
}

fn next_aligned_boundary(now_ms: i64, window_ms: i64) -> i64 {
    now_ms
        .div_euclid(window_ms)
        .saturating_add(1)
        .saturating_mul(window_ms)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TokenBucketConfig {
    pub capacity: f64,
    pub refill_per_sec: f64,
}

impl TokenBucketConfig {
    pub fn from_env(venue: Option<crate::venue::Venue>) -> Self {
        // New names describe the actual Binance accounting unit. Keep the
        // historical RPS variables as a compatibility fallback so existing
        // deployments become conservative weighted limiters on upgrade.
        let generic_rps = std::env::var("RUSTCORE_BINANCE_WEIGHT_PER_SEC")
            .ok()
            .or_else(|| std::env::var("RUSTCORE_BINANCE_RPS").ok());
        let generic_burst = std::env::var("RUSTCORE_BINANCE_WEIGHT_BURST")
            .ok()
            .or_else(|| std::env::var("RUSTCORE_BINANCE_BURST").ok());
        let testnet_rps = std::env::var("RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC")
            .ok()
            .or_else(|| std::env::var("RUSTCORE_BINANCE_TESTNET_RPS").ok());
        let testnet_burst = std::env::var("RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST")
            .ok()
            .or_else(|| std::env::var("RUSTCORE_BINANCE_TESTNET_BURST").ok());
        Self::from_raw(
            venue,
            generic_rps.as_deref(),
            generic_burst.as_deref(),
            testnet_rps.as_deref(),
            testnet_burst.as_deref(),
        )
    }

    pub(crate) fn binance_ws_from_env() -> Self {
        let budget = std::env::var("RUSTCORE_BINANCE_WS_WEIGHT_BUDGET_1M")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(2_400.0)
            .clamp(100.0, 6_000.0);
        Self {
            capacity: budget,
            refill_per_sec: budget / 60.0,
        }
    }

    fn from_raw(
        venue: Option<crate::venue::Venue>,
        generic_rps: Option<&str>,
        generic_burst: Option<&str>,
        testnet_rps: Option<&str>,
        testnet_burst: Option<&str>,
    ) -> Self {
        let generic_rps = parse_f64(generic_rps).unwrap_or(5.0);
        let generic_burst = parse_f64(generic_burst).unwrap_or(10.0);
        let rps = if venue == Some(crate::venue::Venue::Testnet) {
            parse_f64(testnet_rps).unwrap_or(generic_rps)
        } else {
            generic_rps
        };
        let burst = if venue == Some(crate::venue::Venue::Testnet) {
            parse_f64(testnet_burst).unwrap_or(generic_burst)
        } else {
            generic_burst
        };
        Self {
            capacity: burst.max(1.0),
            refill_per_sec: rps.max(0.1),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            capacity,
            refill_per_sec,
        }
    }

    /// 生效预算 + **它是从哪个 env 名来的**。
    ///
    /// 取值仍然由未改动的 [`TokenBucketConfig::from_env`] 决定，这里只报告来源，
    /// 所以日志与行为不可能分叉。存在的理由是 `f79a7c8` 改掉了一个**已经上线的
    /// 数字的含义**：`RUSTCORE_BINANCE_RPS` 从"请求/秒"变成"权重/秒"，而部署里
    /// 只配了旧名字。没有任何一条日志说过生效预算是多少，于是 1.5 被当成
    /// 1.5 权重/秒、一轮 100 权重的对账被拉成 60 秒一轮，看起来像引擎坏了。
    pub fn describe_from_env(venue: Option<crate::venue::Venue>) -> WeightBudgetProvenance {
        let testnet = venue == Some(crate::venue::Venue::Testnet);
        let rate_source = first_set_env(if testnet {
            &[
                "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC",
                "RUSTCORE_BINANCE_TESTNET_RPS",
                "RUSTCORE_BINANCE_WEIGHT_PER_SEC",
                "RUSTCORE_BINANCE_RPS",
            ]
        } else {
            &["RUSTCORE_BINANCE_WEIGHT_PER_SEC", "RUSTCORE_BINANCE_RPS"]
        });
        let burst_source = first_set_env(if testnet {
            &[
                "RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST",
                "RUSTCORE_BINANCE_TESTNET_BURST",
                "RUSTCORE_BINANCE_WEIGHT_BURST",
                "RUSTCORE_BINANCE_BURST",
            ]
        } else {
            &["RUSTCORE_BINANCE_WEIGHT_BURST", "RUSTCORE_BINANCE_BURST"]
        });
        let legacy_only = [rate_source.as_deref(), burst_source.as_deref()]
            .into_iter()
            .flatten()
            .any(|name| name.ends_with("_RPS") || name.ends_with("_BURST"));
        WeightBudgetProvenance {
            config: Self::from_env(venue),
            rate_source: rate_source.unwrap_or_else(|| "built-in default".to_string()),
            burst_source: burst_source.unwrap_or_else(|| "built-in default".to_string()),
            legacy_names_only: legacy_only,
        }
    }
}

/// 第一个**被设置**的 env 名（不看是否可解析，与 `from_env` 的优先级一致）。
fn first_set_env(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find(|name| std::env::var_os(name).is_some())
        .map(|name| (*name).to_string())
}

/// 生效的出口权重预算及其来源，只用于启动时的可观测性。
#[derive(Debug, Clone, PartialEq)]
pub struct WeightBudgetProvenance {
    pub config: TokenBucketConfig,
    pub rate_source: String,
    pub burst_source: String,
    /// 只配了历史 `*_RPS` / `*_BURST` 名字——**它们的单位含义已经变了**。
    pub legacy_names_only: bool,
}

/// 这份预算撑不撑得起配置的对账节奏。
///
/// 返回「一轮实际需要多少毫秒」——仅当它**超过**配置周期时返回 `Some`。
/// 超过意味着对账轮次会排队、持仓真值在有杠杆仓位时静默变陈旧，而且**不会有 418
/// 伴随**：这是本地限流器自己造成的，不是交易所拒绝。实测过一次：继承来的 1.5
/// 权重/秒 × 一轮 100 权重 = 60.0 秒下限，对着配置的 30 秒周期。
pub fn reconcile_cadence_shortfall_ms(
    round_weight: f64,
    refill_per_sec: f64,
    interval_ms: u64,
) -> Option<u64> {
    if !round_weight.is_finite() || !refill_per_sec.is_finite() || refill_per_sec <= 0.0 {
        return None;
    }
    let required_ms = (round_weight / refill_per_sec * 1_000.0).ceil();
    if !required_ms.is_finite() || required_ms <= interval_ms as f64 {
        return None;
    }
    Some(required_ms as u64)
}

fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw.and_then(|value| value.trim().parse::<f64>().ok())
}

#[derive(Clone, Copy)]
struct TokenBucket {
    tokens: f64,
    last: Instant,
    config: TokenBucketConfig,
}

impl CredentialRateLimiter {
    /// **生产路径的构造器。**
    ///
    /// Binance 的请求权重记在公网出口 IP 上，不记在 API key 上，所以本进程里每一个
    /// limiter 都必须收敛到同一个注册表——执行、恢复、kill-switch 三条路径各自
    /// 构造的 client 花的是同一份预算。key 由调用方传入的 egress scope 决定
    /// （历史参数名 `credential_id` 为兼容保留）。
    ///
    /// 与 `new` 分开是**为了让这条性质可被测试观察**：它此前藏在
    /// `#[cfg(not(test))]` 里，撤销它的变异体能全绿通过。
    pub fn shared(default: TokenBucketConfig) -> Self {
        let governor = EgressGovernorConfig::from_env();
        Self {
            inner: shared_egress_buckets(),
            queues: shared_egress_queues(),
            meter: EgressMeter::shared(governor),
            default,
            governor,
            clock: Arc::new(crate::svc::SystemClock),
        }
    }

    /// Production constructor for the independent ws-fapi IP pool.  It shares
    /// the process registries, but the `binance-futures-ws:*` scope prefix keeps
    /// every bucket, queue and meter row disjoint from REST.
    pub(crate) fn shared_ws(default: TokenBucketConfig) -> Self {
        let governor = EgressGovernorConfig::from_ws_env();
        Self {
            inner: shared_egress_buckets(),
            queues: shared_egress_queues(),
            meter: EgressMeter::shared(governor),
            default,
            governor,
            clock: Arc::new(crate::svc::SystemClock),
        }
    }

    /// **隔离**的 limiter：每个实例自带一套桶。
    ///
    /// 给用例用，好让并发用例不会互相花掉对方的合成令牌。clone 仍然共享同一套桶
    /// （`Arc`），对应生产里"一个执行核被到处 clone"的所有权模型。
    /// 生产构造请用 [`CredentialRateLimiter::shared`]。
    #[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
    pub fn new(default: TokenBucketConfig) -> Self {
        let governor = EgressGovernorConfig::from_env();
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            queues: Arc::new(std::sync::Mutex::new(HashMap::new())),
            meter: EgressMeter::isolated(governor),
            default,
            governor,
            clock: Arc::new(crate::svc::SystemClock),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_governor_test(
        default: TokenBucketConfig,
        governor: EgressGovernorConfig,
        clock: Arc<dyn crate::svc::Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            queues: Arc::new(std::sync::Mutex::new(HashMap::new())),
            meter: EgressMeter::isolated(governor),
            default,
            governor,
            clock,
        }
    }

    pub(crate) fn with_clock(mut self, clock: Arc<dyn crate::svc::Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn observe_egress_weight(
        &self,
        egress_scope: &str,
        used_weight_1m: u64,
        now_ms: i64,
    ) {
        self.meter.observe(egress_scope, used_weight_1m, now_ms);
        self.log_governor(egress_scope, now_ms);
        self.notify_scope(egress_scope);
    }

    pub(crate) fn observe_egress_budget(
        &self,
        egress_scope: &str,
        observed_budget_1m: u64,
        now_ms: i64,
    ) {
        self.meter
            .observe_budget(egress_scope, observed_budget_1m, now_ms);
        self.log_governor(egress_scope, now_ms);
        self.notify_scope(egress_scope);
    }

    pub(crate) fn needs_budget_discovery(&self, egress_scope: &str) -> bool {
        let observed = self
            .meter
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned")
            .get(egress_scope)
            .and_then(|entry| entry.observed_budget_1m);
        observed.is_none()
            && self
                .governor
                .effective_budget(egress_scope, observed)
                .source
                == BudgetSource::Default
    }

    pub(crate) fn observe_egress_weight_with_limit(
        &self,
        egress_scope: &str,
        used_weight_1m: u64,
        limit_1m: u64,
        now_ms: i64,
    ) {
        self.meter
            .observe_with_limit(egress_scope, used_weight_1m, limit_1m, now_ms);
        self.log_governor(egress_scope, now_ms);
        self.notify_scope(egress_scope);
    }

    pub(crate) fn record_budget_exceeded_without_429(&self, egress_scope: &str, http_status: u16) {
        if let Some((count, used_weight_1m, effective_budget_1m, budget_source)) = self
            .meter
            .record_budget_exceeded_without_429(egress_scope, http_status)
        {
            tracing::info!(
                scope = %egress_scope,
                http_status,
                used_weight_1m,
                effective_budget_1m,
                budget_source = budget_source.as_str(),
                budget_exceeded_without_429_count = count,
                "budget exceeded without 429"
            );
        }
    }

    fn notify_scope(&self, egress_scope: &str) {
        let waiters = self
            .queues
            .lock()
            .expect("Binance egress queue registry lock poisoned")
            .get(egress_scope)
            .map(|queue| {
                queue
                    .waiters
                    .iter()
                    .map(|waiter| waiter.notify.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for waiter in waiters {
            waiter.notify_one();
        }
    }

    fn queue_counts(&self, egress_scope: &str) -> HashMap<&'static str, usize> {
        let mut counts = HashMap::new();
        if let Some(queue) = self
            .queues
            .lock()
            .expect("Binance egress queue registry lock poisoned")
            .get(egress_scope)
        {
            for waiter in &queue.waiters {
                *counts.entry(waiter.class.as_str()).or_insert(0) += 1;
            }
        }
        counts
    }

    fn log_governor(&self, egress_scope: &str, now_ms: i64) {
        let snapshot = self.meter.snapshot_for_log(egress_scope, now_ms);
        if !snapshot.should_log {
            return;
        }
        let counts = self.queue_counts(egress_scope);
        tracing::info!(
            scope = %egress_scope,
            last_used = ?snapshot.last_used,
            headroom = ?snapshot.headroom,
            observed_budget_1m = ?snapshot.observed_budget_1m,
            effective_budget_1m = snapshot.effective_budget_1m,
            budget_source = snapshot.budget_source.as_str(),
            event_truth_queue_len = counts.get("event_truth").copied().unwrap_or_default(),
            entry_queue_len = counts.get("entry").copied().unwrap_or_default(),
            periodic_reconcile_queue_len = counts
                .get("periodic_reconcile")
                .copied()
                .unwrap_or_default(),
            park_state = snapshot.state.as_str(),
            "binance egress governor"
        );
    }

    pub(crate) async fn acquire_prioritized(
        &self,
        egress_scope: &str,
        credential_id: &str,
        cost: f64,
        class: RequestClass,
        reserve: f64,
    ) -> Admission {
        let cost = if cost < 0.0 || !cost.is_finite() {
            1.0
        } else {
            cost
        };
        let reserve = reserve.max(0.0);
        if !class.queues() {
            if cost == 0.0 {
                return Admission::Admitted;
            }
            return if self.try_acquire_reserved(egress_scope, cost).await {
                Admission::Admitted
            } else {
                Admission::ReserveExhausted
            };
        }

        // Only explicit v2 ReconcileNow installs this task local. Background
        // callers keep their configured 15s queue; reserve and selection do not change.
        let request_deadline = crate::svc_v2::reconcile_budget::current_deadline();
        let started_at_ms = self.clock.now_ms();
        if self
            .meter
            .headroom(egress_scope, started_at_ms)
            .is_some_and(|headroom| {
                park_state(headroom.last_used, headroom.budget_1m) == ParkState::Hard
            })
        {
            self.log_governor(egress_scope, started_at_ms);
            return Admission::HardRejected;
        }

        let waiter = {
            let mut queues = self
                .queues
                .lock()
                .expect("Binance egress queue registry lock poisoned");
            let queue = queues.entry(egress_scope.to_string()).or_default();
            queue.next_id = queue.next_id.saturating_add(1);
            let waiter = QueueWaiter {
                id: queue.next_id,
                credential_id: credential_id.to_string(),
                class,
                notify: Arc::new(Notify::new()),
            };
            queue.waiters.push_back(waiter.clone());
            waiter
        };
        // No await may separate insertion from installing this owner.
        let _registration = QueueRegistration {
            limiter: self,
            egress_scope,
            waiter_id: waiter.id,
        };
        self.notify_scope(egress_scope);

        loop {
            let now_ms = self.clock.now_ms();
            self.log_governor(egress_scope, now_ms);
            let elapsed_ms = now_ms.saturating_sub(started_at_ms).max(0) as u64;
            let remaining_wait_ms = match request_deadline {
                Some(deadline) => deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_nanos()
                    .div_ceil(1_000_000) as u64,
                None => self.governor.queue_wait_max_ms.saturating_sub(elapsed_ms),
            };
            if remaining_wait_ms == 0 {
                return Admission::TimedOut;
            }

            let headroom = self.meter.headroom(egress_scope, now_ms);
            if headroom.is_some_and(|value| {
                park_state(value.last_used, value.budget_1m) == ParkState::Hard
            }) {
                return Admission::HardRejected;
            }

            let selected = self
                .queues
                .lock()
                .expect("Binance egress queue registry lock poisoned")
                .get(egress_scope)
                .and_then(|queue| queue.selected_waiter(self.governor.fair_share_burst))
                == Some(waiter.id);
            if !selected {
                tokio::select! {
                    _ = waiter.notify.notified() => {},
                    _ = self.clock.sleep(Duration::from_millis(remaining_wait_ms)) => {},
                }
                continue;
            }

            if let Some(headroom) = headroom
                .filter(|value| park_state(value.last_used, value.budget_1m) == ParkState::Soft)
            {
                let until_boundary_ms =
                    headroom.window_until_ms.saturating_sub(now_ms).max(0) as u64;
                let wait_ms = until_boundary_ms.min(remaining_wait_ms);
                tokio::select! {
                    _ = waiter.notify.notified() => {},
                    _ = self.clock.sleep(Duration::from_millis(wait_ms)) => {},
                }
                continue;
            }

            if cost == 0.0 {
                {
                    let mut queues = self
                        .queues
                        .lock()
                        .expect("Binance egress queue registry lock poisoned");
                    let queue = queues.entry(egress_scope.to_string()).or_default();
                    queue.record_admission(&waiter);
                }
                return Admission::Admitted;
            }
            let real_cap = self.meter.effective_remaining(egress_scope, now_ms);
            let wait_for = {
                let mut buckets = self.inner.lock().await;
                let bucket = buckets
                    .entry(egress_scope.to_string())
                    .or_insert_with(|| TokenBucket::new(self.default));
                bucket.acquire_reserving_with_cap(cost, reserve, real_cap)
            };
            if wait_for.is_none() {
                self.meter.reserve(egress_scope, cost, now_ms);
                {
                    let mut queues = self
                        .queues
                        .lock()
                        .expect("Binance egress queue registry lock poisoned");
                    let queue = queues.entry(egress_scope.to_string()).or_default();
                    queue.record_admission(&waiter);
                }
                return Admission::Admitted;
            }
            let wait_for = wait_for
                .unwrap_or_else(|| Duration::from_millis(1))
                .min(Duration::from_millis(remaining_wait_ms));
            tokio::select! {
                _ = waiter.notify.notified() => {},
                _ = self.clock.sleep(wait_for) => {},
            }
        }
    }

    fn remove_waiter(&self, egress_scope: &str, waiter_id: u64) {
        let notifies = {
            let mut queues = self
                .queues
                .lock()
                .expect("Binance egress queue registry lock poisoned");
            let Some(queue) = queues.get_mut(egress_scope) else {
                return;
            };
            queue.waiters.retain(|waiter| waiter.id != waiter_id);
            queue
                .waiters
                .iter()
                .map(|waiter| waiter.notify.clone())
                .collect::<Vec<_>>()
        };
        for notify in notifies {
            notify.notify_one();
        }
    }

    #[cfg(test)]
    pub async fn acquire(&self, credential_id: &str, cost: f64) {
        let cost = if cost <= 0.0 { 1.0 } else { cost };
        loop {
            let sleep_for = {
                let mut guard = self.inner.lock().await;
                let bucket = guard
                    .entry(credential_id.to_string())
                    .or_insert_with(|| TokenBucket::new(self.default));
                bucket.acquire(cost)
            };
            if let Some(dur) = sleep_for {
                tokio::time::sleep(dur).await;
                continue;
            }
            return;
        }
    }

    pub async fn acquire_reserving(&self, egress_scope: &str, cost: f64, reserve: f64) {
        let cost = if cost <= 0.0 { 1.0 } else { cost };
        let reserve = reserve.max(0.0);
        loop {
            let sleep_for = {
                let mut guard = self.inner.lock().await;
                let bucket = guard
                    .entry(egress_scope.to_string())
                    .or_insert_with(|| TokenBucket::new(self.default));
                bucket.acquire_reserving(cost, reserve)
            };
            if let Some(dur) = sleep_for {
                tokio::time::sleep(dur).await;
                continue;
            }
            return;
        }
    }

    pub async fn try_acquire_reserved(&self, egress_scope: &str, cost: f64) -> bool {
        let cost = if cost <= 0.0 { 1.0 } else { cost };
        let mut guard = self.inner.lock().await;
        let bucket = guard
            .entry(egress_scope.to_string())
            .or_insert_with(|| TokenBucket::new(self.default));
        bucket.try_acquire(cost)
    }

    /// Return a reservation that never reached transport. The capacity clamp is
    /// load-bearing: a concurrent refill must not let a refund mint tokens.
    pub async fn refund(&self, egress_scope: &str, cost: f64) {
        let cost = if cost <= 0.0 { 1.0 } else { cost };
        let mut guard = self.inner.lock().await;
        let bucket = guard
            .entry(egress_scope.to_string())
            .or_insert_with(|| TokenBucket::new(self.default));
        bucket.refill();
        bucket.tokens = (bucket.tokens + cost).min(bucket.config.capacity);
        drop(guard);
        self.notify_scope(egress_scope);
    }

    pub(crate) async fn refund_prioritized(&self, egress_scope: &str, cost: f64) {
        self.refund(egress_scope, cost).await;
        self.meter.release(egress_scope, cost, self.clock.now_ms());
    }

    #[cfg(test)]
    pub(crate) async fn stored_tokens(&self, credential_id: &str) -> Option<f64> {
        self.inner
            .lock()
            .await
            .get(credential_id)
            .map(|bucket| bucket.tokens)
    }

    #[cfg(test)]
    pub(crate) async fn set_stored_tokens(&self, egress_scope: &str, tokens: f64) {
        let mut buckets = self.inner.lock().await;
        let bucket = buckets
            .entry(egress_scope.to_string())
            .or_insert_with(|| TokenBucket::new(self.default));
        bucket.tokens = tokens.max(0.0).min(bucket.config.capacity);
        drop(buckets);
        self.notify_scope(egress_scope);
    }

    #[cfg(test)]
    pub(crate) fn observed_headroom_for_test(
        &self,
        egress_scope: &str,
        now_ms: i64,
    ) -> Option<Headroom> {
        self.meter.headroom(egress_scope, now_ms)
    }

    #[cfg(test)]
    pub(crate) fn effective_budget_for_test(&self, egress_scope: &str) -> EffectiveEgressBudget {
        self.meter.effective_budget(egress_scope)
    }

    #[cfg(test)]
    fn budget_diagnostics_for_test(&self, egress_scope: &str) -> Option<(Option<u64>, u64, u64)> {
        self.meter
            .inner
            .lock()
            .expect("Binance egress meter registry lock poisoned")
            .get(egress_scope)
            .map(|value| {
                (
                    value.observed_budget_1m,
                    value.budget_mismatch_warning_count,
                    value.budget_exceeded_without_429_count,
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn queued(&self, egress_scope: &str) -> usize {
        self.queues
            .lock()
            .expect("Binance egress queue registry lock poisoned")
            .get(egress_scope)
            .map(|queue| queue.waiters.len())
            .unwrap_or_default()
    }
}

impl TokenBucket {
    fn new(config: TokenBucketConfig) -> Self {
        Self {
            tokens: config.capacity,
            last: Instant::now(),
            config,
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * self.config.refill_per_sec).min(self.config.capacity);
    }

    #[cfg(test)]
    fn acquire(&mut self, cost: f64) -> Option<Duration> {
        // A few truth endpoints have a documented weight of 40. The old
        // bucket capacity defaulted to 10, so charging the real cost could
        // never complete: refill was clamped below the requested amount.
        // Grow the burst ceiling to the largest single legitimate request;
        // this makes the caller wait for the full weight instead of silently
        // undercharging it as one request.
        if cost > self.config.capacity {
            self.config.capacity = cost;
        }
        self.refill();
        if self.tokens >= cost {
            self.tokens -= cost;
            return None;
        }
        let missing = cost - self.tokens;
        let secs = missing / self.config.refill_per_sec.max(1e-6);
        Some(Duration::from_secs_f64(secs).min(Duration::from_secs(5)))
    }

    fn acquire_reserving(&mut self, cost: f64, reserve: f64) -> Option<Duration> {
        let required = cost + reserve;
        if required > self.config.capacity {
            self.config.capacity = required;
        }
        self.refill();
        if self.tokens >= required {
            self.tokens -= cost;
            return None;
        }
        let missing = required - self.tokens;
        let secs = missing / self.config.refill_per_sec.max(1e-6);
        Some(Duration::from_secs_f64(secs).min(Duration::from_secs(5)))
    }

    fn acquire_reserving_with_cap(
        &mut self,
        cost: f64,
        reserve: f64,
        real_headroom: Option<f64>,
    ) -> Option<Duration> {
        let required = cost + reserve;
        if required > self.config.capacity {
            self.config.capacity = required;
        }
        self.refill();
        let available = real_headroom
            .map(|headroom| self.tokens.min(headroom.max(0.0)))
            .unwrap_or(self.tokens);
        if available >= required {
            self.tokens -= cost;
            return None;
        }
        let missing = required - available;
        let secs = missing / self.config.refill_per_sec.max(1e-6);
        Some(Duration::from_secs_f64(secs).min(Duration::from_secs(5)))
    }

    fn try_acquire(&mut self, cost: f64) -> bool {
        if cost > self.config.capacity {
            self.config.capacity = cost;
        }
        self.refill();
        if self.tokens < cost {
            return false;
        }
        self.tokens -= cost;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;

    struct ManualGovernorClock {
        now_ms: AtomicI64,
    }

    impl ManualGovernorClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: AtomicI64::new(now_ms),
            }
        }
    }

    impl crate::svc::Clock for ManualGovernorClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.now_ms.fetch_add(
                    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                    Ordering::SeqCst,
                );
                tokio::task::yield_now().await;
            })
        }
    }

    // Cancellation tests must not consume the governor's own queue deadline.
    // Only the caller's timeout/cancellation may remove these waiters.
    struct FrozenGovernorClock;

    impl crate::svc::Clock for FrozenGovernorClock {
        fn now_ms(&self) -> i64 {
            1_000
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(std::future::pending())
        }
    }

    async fn cancellation_limiter() -> (CredentialRateLimiter, String, String) {
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(100, 15_000, 3),
            Arc::new(FrozenGovernorClock),
        );
        let scope = format!("a6-r3-{}", ulid::Ulid::new());
        let credential = ulid::Ulid::new().to_string();
        limiter.observe_egress_weight(&scope, 0, 1_000);
        limiter.set_stored_tokens(&scope, 0.0).await;
        (limiter, scope, credential)
    }

    async fn poll_waiting_once<F: Future>(future: Pin<&mut F>) {
        let mut future = future;
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn a6_r3_timeout_drops_waiter_synchronously_and_next_request_is_admitted() {
        let (limiter, scope, credential) = cancellation_limiter().await;
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            limiter.acquire_prioritized(
                &scope,
                &credential,
                1.0,
                RequestClass::PeriodicReconcile,
                0.0,
            ),
        )
        .await;
        assert!(
            result.is_err(),
            "the caller's deadline must cancel the wait"
        );
        assert_eq!(
            limiter.queued(&scope),
            0,
            "cancelled waiter must be removed synchronously"
        );
        assert_eq!(limiter.queue_counts(&scope).get("periodic_reconcile"), None);
        limiter.set_stored_tokens(&scope, 1.0).await;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                limiter.acquire_prioritized(
                    &scope,
                    &credential,
                    1.0,
                    RequestClass::PeriodicReconcile,
                    0.0,
                ),
            )
            .await
            .expect("cancelled head must not block its successor"),
            Admission::Admitted
        );
    }

    #[tokio::test]
    async fn a6_r3_select_loser_is_removed_without_advancing_governor_time() {
        let (limiter, scope, credential) = cancellation_limiter().await;
        let wait = limiter.acquire_prioritized(
            &scope,
            &credential,
            1.0,
            RequestClass::PeriodicReconcile,
            0.0,
        );
        tokio::select! {
            biased;
            _ = wait => panic!("empty bucket must not admit a waiter"),
            _ = std::future::ready(()) => {
                // select! drops its owned losing futures before this handler.
                assert_eq!(limiter.queued(&scope), 0);
                assert_eq!(limiter.queue_counts(&scope).get("periodic_reconcile"), None);
            }
        }
    }

    #[tokio::test]
    async fn a6_r3_aborted_recovery_removes_waiter_before_join_completes() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (limiter, scope, credential) = cancellation_limiter().await;
            let task_limiter = limiter.clone();
            let task_scope = scope.clone();
            let task = tokio::spawn(async move {
                task_limiter
                    .acquire_prioritized(
                        &task_scope,
                        &credential,
                        1.0,
                        RequestClass::EventTruth,
                        0.0,
                    )
                    .await
            });
            while limiter.queued(&scope) == 0 {
                tokio::task::yield_now().await;
            }
            task.abort();
            assert!(task
                .await
                .expect_err("recovery task must be cancelled")
                .is_cancelled());
            assert_eq!(limiter.queued(&scope), 0);
            assert_eq!(limiter.queue_counts(&scope).get("event_truth"), None);
        })
        .await
        .expect("task cancellation must finish within the test deadline");
    }

    #[tokio::test]
    async fn a6_r3_dropped_head_wakes_existing_same_and_lower_priority_waiters() {
        for class in [RequestClass::EventTruth, RequestClass::PeriodicReconcile] {
            tokio::time::timeout(Duration::from_secs(1), async {
                let (limiter, scope, credential) = cancellation_limiter().await;
                let mut head = Box::pin(limiter.acquire_prioritized(
                    &scope,
                    &credential,
                    1.0,
                    RequestClass::EventTruth,
                    0.0,
                ));
                poll_waiting_once(head.as_mut()).await;
                let follower_credential = ulid::Ulid::new().to_string();
                // Zero-cost work needs no refill/notification from the test;
                // removing the head must itself wake an already sleeping task.
                let follower_limiter = limiter.clone();
                let follower_scope = scope.clone();
                let follower = tokio::spawn(async move {
                    follower_limiter
                        .acquire_prioritized(&follower_scope, &follower_credential, 0.0, class, 0.0)
                        .await
                });
                while limiter.queued(&scope) != 2 {
                    tokio::task::yield_now().await;
                }
                tokio::task::yield_now().await;
                drop(head);
                assert_eq!(limiter.queued(&scope), 1);
                assert_eq!(follower.await.expect("follower task"), Admission::Admitted);
                assert_eq!(limiter.queued(&scope), 0);
            })
            .await
            .expect("head cancellation must wake queued successors immediately");
        }
    }

    #[tokio::test]
    async fn a6_r3_cancel_while_waiting_for_bucket_lock_cleans_queue() {
        let (limiter, scope, credential) = cancellation_limiter().await;
        let bucket_lock = limiter.inner.lock().await;
        let mut wait = Box::pin(limiter.acquire_prioritized(
            &scope,
            &credential,
            1.0,
            RequestClass::PeriodicReconcile,
            0.0,
        ));
        poll_waiting_once(wait.as_mut()).await;
        assert_eq!(limiter.queued(&scope), 1);
        drop(wait);
        assert_eq!(limiter.queued(&scope), 0);
        drop(bucket_lock);
    }

    #[tokio::test]
    async fn a6_r3_admitted_charge_survives_caller_cancellation_without_refund() {
        let (limiter, scope, credential) = cancellation_limiter().await;
        limiter.set_stored_tokens(&scope, 100.0).await;
        let mut caller = Box::pin(async {
            assert_eq!(
                limiter
                    .acquire_prioritized(&scope, &credential, 10.0, RequestClass::EventTruth, 0.0,)
                    .await,
                Admission::Admitted
            );
            std::future::pending::<()>().await;
        });
        poll_waiting_once(caller.as_mut()).await;
        assert_eq!(
            limiter.queued(&scope),
            0,
            "normal admission also removes its waiter"
        );
        drop(caller);
        assert_eq!(limiter.stored_tokens(&scope).await, Some(90.0));
        assert_eq!(limiter.meter.effective_remaining(&scope, 1_000), Some(90.0));
    }

    fn rust_files_under(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
        {
            let path = entry
                .unwrap_or_else(|error| {
                    panic!("failed to read an entry under {}: {error}", dir.display())
                })
                .path();
            if path.is_dir() {
                rust_files_under(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    #[test]
    fn token_bucket_config_literals_are_centralized_in_rate_limiter() {
        let src_root = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let allowed_file = src_root.join("rate_limiter.rs");
        let mut rust_files = Vec::new();
        rust_files_under(&src_root, &mut rust_files);

        let files_scanned = rust_files.len();
        assert!(
            files_scanned > 0,
            "no Rust files scanned under {}",
            src_root.display()
        );

        let mut violations = Vec::new();
        for path in rust_files {
            if path == allowed_file {
                continue;
            }
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            for (index, line) in source.lines().enumerate() {
                if line.contains("TokenBucketConfig {") {
                    let relative = path.strip_prefix(&src_root).unwrap_or(&path);
                    violations.push(format!("{}:{}", relative.display(), index + 1));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "TokenBucketConfig literals must be centralized in rate_limiter.rs; found: {}",
            violations.join(", ")
        );
    }

    #[tokio::test]
    async fn limiter_allows() {
        let lim = CredentialRateLimiter::new(TokenBucketConfig {
            capacity: 1.0,
            refill_per_sec: 10.0,
        });
        lim.acquire("c1", 1.0).await;
        lim.acquire("c1", 1.0).await;
    }

    /// 复现真实停摆的那笔算术，以及运维当时的修法。
    #[test]
    fn the_cadence_check_reproduces_the_measured_outage_and_its_fix() {
        // 继承来的 1.5（旧名字本意是"请求/秒"）× 一轮 100 权重 = 66.7 秒下限，
        // 对着配置的 30 秒周期。实测轮次间隔 60.0 秒，战役连续 80 分钟起不来。
        assert_eq!(
            reconcile_cadence_shortfall_ms(100.0, 1.5, 30_000),
            Some(66_667)
        );
        // 运维改成 6.0 权重/秒之后：16.7 秒 < 30 秒，撑得住。
        assert_eq!(reconcile_cadence_shortfall_ms(100.0, 6.0, 30_000), None);
        // 恰好相等不算短缺——判据是**超过**才告警。
        assert_eq!(reconcile_cadence_shortfall_ms(30.0, 1.0, 30_000), None);
        assert_eq!(
            reconcile_cadence_shortfall_ms(30.001, 1.0, 30_000),
            Some(30_001)
        );
        // 非法输入不得伪造出一条告警。
        assert_eq!(reconcile_cadence_shortfall_ms(100.0, 0.0, 30_000), None);
        assert_eq!(reconcile_cadence_shortfall_ms(100.0, -1.0, 30_000), None);
        assert_eq!(reconcile_cadence_shortfall_ms(f64::NAN, 1.0, 30_000), None);
    }

    /// clone 共享同一套桶——这是 `#[derive(Clone)]` 的 `Arc` 语义，**旧代码本来就有**。
    /// 改名是因为它此前叫 `limiter_clones_share_one_egress_bucket`，读起来像是在保证
    /// "所有 limiter 收敛到一个出口桶"，而那条性质由下面那个用例负责。
    #[tokio::test]
    async fn cloning_a_limiter_keeps_its_exact_bucket_set() {
        let scope = "rate-limiter-clone-test";
        let first = CredentialRateLimiter::new(TokenBucketConfig::for_test(2.0, 0.0));
        let second = first.clone();

        first.acquire(scope, 1.0).await;

        assert_eq!(second.stored_tokens(scope).await, Some(1.0));
    }

    /// **两个各自独立构造**（不是 clone）的生产 limiter 必须花同一份出口预算。
    ///
    /// 这才是 RL-2 / A-05 的头号修复：权重记在出口 IP 上，而执行、恢复、kill-switch
    /// 三条路径各自 `shared(...)` 出自己的 client。此前该性质只存在于
    /// `#[cfg(not(test))]` 分支里，于是把构造器整个回退成每实例 HashMap 也能全绿。
    #[tokio::test]
    async fn two_independently_built_shared_limiters_spend_one_egress_budget() {
        let scope = format!("rate-limiter-shared-registry-{}", ulid::Ulid::new());
        let execution = CredentialRateLimiter::shared(TokenBucketConfig::for_test(5.0, 0.0));
        let kill_switch = CredentialRateLimiter::shared(TokenBucketConfig::for_test(5.0, 0.0));

        execution.acquire_reserving(&scope, 3.0, 0.0).await;

        // 第二个 limiter 从未被 clone 过，却必须看到第一个花掉的额度。
        assert_eq!(kill_switch.stored_tokens(&scope).await, Some(2.0));

        // 反向也成立：隔离构造器不得看见共享注册表里的开销。
        let isolated = CredentialRateLimiter::new(TokenBucketConfig::for_test(5.0, 0.0));
        assert_eq!(isolated.stored_tokens(&scope).await, None);

        shared_egress_buckets().lock().await.remove(&scope);
    }

    #[test]
    fn one_minute_order_window_parks_normal_entries_without_sleeping() {
        let limiter = OrderCountLimiter::isolated(OrderCountConfig::for_test(2, 100));
        let credential = format!("order-1m-{}", ulid::Ulid::new());
        assert_eq!(
            limiter.admit_entry(&credential, 1_000),
            OrderAdmission::Admitted
        );
        assert_eq!(
            limiter.admit_entry(&credential, 2_000),
            OrderAdmission::Admitted
        );
        assert_eq!(
            limiter.admit_entry(&credential, 2_000),
            OrderAdmission::Parked {
                until_ms: 61_000,
                window: "1m",
            }
        );
    }

    #[test]
    fn ten_second_order_window_is_independent_from_the_one_minute_window() {
        let limiter = OrderCountLimiter::isolated(OrderCountConfig::for_test(100, 2));
        let credential = format!("order-10s-{}", ulid::Ulid::new());
        assert_eq!(
            limiter.admit_entry(&credential, 1_000),
            OrderAdmission::Admitted
        );
        assert_eq!(
            limiter.admit_entry(&credential, 2_000),
            OrderAdmission::Admitted
        );
        assert_eq!(
            limiter.admit_entry(&credential, 2_000),
            OrderAdmission::Parked {
                until_ms: 11_000,
                window: "10s",
            }
        );
        assert_eq!(
            limiter.admit_entry(&credential, 11_000),
            OrderAdmission::Admitted
        );
    }

    #[test]
    fn observed_ninety_percent_parks_until_the_aligned_window_boundary() {
        let limiter = OrderCountLimiter::isolated(OrderCountConfig::for_test(1_200, 300));
        let credential = format!("order-header-{}", ulid::Ulid::new());
        limiter.observe_headers(&credential, Some(1_080), None, 12_345);
        assert_eq!(
            limiter.admit_entry(&credential, 12_345),
            OrderAdmission::Parked {
                until_ms: 60_000,
                window: "1m",
            }
        );
        assert_eq!(
            limiter.admit_entry(&credential, 60_000),
            OrderAdmission::Admitted,
            "the header-derived park must release exactly at the minute boundary"
        );
    }

    #[test]
    fn order_count_is_keyed_by_credential_not_process_global() {
        let limiter = OrderCountLimiter::isolated(OrderCountConfig::for_test(1, 1));
        let credential_a = format!("order-a-{}", ulid::Ulid::new());
        let credential_b = format!("order-b-{}", ulid::Ulid::new());
        assert_eq!(
            limiter.admit_entry(&credential_a, 1_000),
            OrderAdmission::Admitted
        );
        assert!(matches!(
            limiter.admit_entry(&credential_a, 1_000),
            OrderAdmission::Parked { .. }
        ));
        assert_eq!(
            limiter.admit_entry(&credential_b, 1_000),
            OrderAdmission::Admitted,
            "credential A exhausting its ORDER budget must not block credential B"
        );
    }

    #[test]
    fn independently_constructed_order_limiters_share_the_production_registry() {
        let credential = format!("order-shared-{}", ulid::Ulid::new());
        let first = OrderCountLimiter::shared(OrderCountConfig::for_test(1, 1));
        let second = OrderCountLimiter::shared(OrderCountConfig::for_test(1, 1));
        assert_eq!(
            first.admit_entry(&credential, 1_000),
            OrderAdmission::Admitted
        );
        assert!(matches!(
            second.admit_entry(&credential, 1_000),
            OrderAdmission::Parked { .. }
        ));
        shared_order_count_registry()
            .lock()
            .expect("order-count registry")
            .remove(&credential);
    }

    #[test]
    fn one_heavy_request_can_grow_the_bucket_and_eventually_acquire() {
        let mut bucket = TokenBucket::new(TokenBucketConfig::for_test(10.0, 10.0));
        assert!(bucket.acquire(40.0).is_some());
        assert_eq!(bucket.config.capacity, 40.0);

        bucket.last = Instant::now() - Duration::from_secs(4);
        assert_eq!(bucket.acquire(40.0), None);
        assert_eq!(bucket.tokens, 0.0);
    }

    /// 普通流量不得侵占留给平仓的余量。
    ///
    /// 余额必须**显式**落在 `cost` 与 `cost + reserve` 之间——那是唯一能区分
    /// `tokens >= required` 与 `tokens >= cost` 的区间。此前这个用例让容量恰好长到
    /// `cost + reserve = 50`、余额也是 50，两个谓词取同一分支，**算术上是空断言**：
    /// 把 `required` 改成 `cost` 它照样绿。`TokenBucket::new` 从满桶开始，所以要手设。
    #[test]
    fn normal_weight_cannot_eat_into_the_exit_reserve() {
        let mut bucket = TokenBucket::new(TokenBucketConfig::for_test(50.0, 0.0));
        bucket.tokens = 45.0;

        // 45 < 40 + 10：普通流量必须等，且**等待不得扣款**。
        assert!(bucket.acquire_reserving(40.0, 10.0).is_some());
        assert_eq!(bucket.tokens, 45.0);

        // 同一个余额，退出请求（不为自己再留 reserve）必须能过。
        assert!(bucket.try_acquire(40.0));
        assert_eq!(bucket.tokens, 5.0);
    }

    /// reserve 用尽时，退出请求得到的是**显式失败**，不是排队。
    /// 排队会顶穿 750ms 的退出预算，那比失败更糟——调用方至少能对失败做处置。
    #[test]
    fn an_exhausted_reserve_refuses_the_exit_instead_of_queueing_it() {
        let mut bucket = TokenBucket::new(TokenBucketConfig::for_test(50.0, 0.0));
        bucket.tokens = 3.0;
        assert!(!bucket.try_acquire(40.0));
        assert_eq!(bucket.tokens, 3.0, "被拒的请求不得扣款");
    }

    #[test]
    fn config_defaults_match_existing_limits() {
        assert_eq!(
            TokenBucketConfig::from_raw(None, None, None, None, None),
            TokenBucketConfig {
                capacity: 10.0,
                refill_per_sec: 5.0,
            }
        );
    }

    #[test]
    fn config_uses_generic_overrides_and_existing_lower_bounds() {
        assert_eq!(
            TokenBucketConfig::from_raw(None, Some("0.01"), Some("0.5"), None, None),
            TokenBucketConfig {
                capacity: 1.0,
                refill_per_sec: 0.1,
            }
        );
        assert_eq!(
            TokenBucketConfig::from_raw(None, Some("7.5"), Some("12"), None, None),
            TokenBucketConfig {
                capacity: 12.0,
                refill_per_sec: 7.5,
            }
        );
    }

    #[test]
    fn testnet_overrides_fall_back_individually_to_generic_values() {
        assert_eq!(
            TokenBucketConfig::from_raw(
                Some(crate::venue::Venue::Testnet),
                Some("6"),
                Some("14"),
                Some("2.5"),
                None,
            ),
            TokenBucketConfig {
                capacity: 14.0,
                refill_per_sec: 2.5,
            }
        );
    }

    #[test]
    fn retry_after_is_authoritative_and_not_capped_or_jittered() {
        assert_eq!(
            retry_decision(RetryInput {
                status: 429,
                class: RetryClass::ReadOnly,
                scope: RetryScope::Normal,
                completed_retries: 0,
                elapsed_ms: 0,
                retry_after_ms: Some(3_000),
                jitter_ms: 49,
            }),
            RetryDecision::Retry {
                after_ms: 3_000,
                retry_number: 1,
            }
        );
    }

    #[test]
    fn ip_ban_and_rate_limit_have_independent_dispositions() {
        let base = RetryInput {
            status: 418,
            class: RetryClass::ReadOnly,
            scope: RetryScope::Normal,
            completed_retries: 0,
            elapsed_ms: 0,
            retry_after_ms: Some(100),
            jitter_ms: 0,
        };
        assert_eq!(
            retry_decision(base),
            RetryDecision::IpBan {
                cooldown_ms: BINANCE_IP_BAN_COOLDOWN_MS,
            }
        );
        assert_eq!(
            retry_decision(RetryInput {
                status: 429,
                ..base
            }),
            RetryDecision::Retry {
                after_ms: 100,
                retry_number: 1,
            }
        );
        assert_eq!(
            retry_decision(RetryInput {
                status: 418,
                retry_after_ms: Some(300_000),
                ..base
            }),
            RetryDecision::IpBan {
                cooldown_ms: 300_000,
            }
        );
    }

    #[test]
    fn non_idempotent_write_returns_truth_lookup_not_retry() {
        assert_eq!(
            retry_decision(RetryInput {
                status: 429,
                class: RetryClass::NonIdempotentWrite,
                scope: RetryScope::Exit,
                completed_retries: 0,
                elapsed_ms: 0,
                retry_after_ms: Some(250),
                jitter_ms: 0,
            }),
            RetryDecision::VerifyTruth { after_ms: 250 }
        );
    }

    #[test]
    fn retry_limit_and_budget_fail_explicitly() {
        assert_eq!(
            retry_decision(RetryInput {
                status: 429,
                class: RetryClass::IdempotentWrite,
                scope: RetryScope::Normal,
                completed_retries: NORMAL_MAX_RETRIES,
                elapsed_ms: 0,
                retry_after_ms: Some(1),
                jitter_ms: 0,
            }),
            RetryDecision::GiveUp {
                reason: "retry_limit_exhausted",
            }
        );
        assert_eq!(
            retry_decision(RetryInput {
                status: 429,
                class: RetryClass::IdempotentWrite,
                scope: RetryScope::Exit,
                completed_retries: 0,
                elapsed_ms: 0,
                retry_after_ms: Some(EXIT_TOTAL_BUDGET_MS + 1),
                jitter_ms: 0,
            }),
            RetryDecision::GiveUp {
                reason: "retry_budget_exhausted",
            }
        );
    }

    #[test]
    fn exit_scope_is_strictly_shorter_than_entry_scope() {
        let input = RetryInput {
            status: 429,
            class: RetryClass::IdempotentWrite,
            scope: RetryScope::Normal,
            completed_retries: 0,
            elapsed_ms: 0,
            retry_after_ms: Some(1_000),
            jitter_ms: 0,
        };
        assert_eq!(
            retry_decision(input),
            RetryDecision::Retry {
                after_ms: 1_000,
                retry_number: 1,
            }
        );
        assert_eq!(
            retry_decision(RetryInput {
                scope: RetryScope::Exit,
                ..input
            }),
            RetryDecision::GiveUp {
                reason: "retry_budget_exhausted",
            }
        );
    }

    #[test]
    fn parses_retry_after_delta_seconds_and_http_date() {
        assert_eq!(parse_retry_after_ms(Some("2"), 0), Some(2_000));
        assert_eq!(
            parse_retry_after_ms(Some("Thu, 01 Jan 1970 00:00:03 GMT"), 1_000),
            Some(2_000)
        );
        assert_eq!(parse_retry_after_ms(Some("invalid"), 0), None);
    }

    #[test]
    fn a6_i1_latest_observation_wins_only_inside_its_aligned_minute() {
        let meter = EgressMeter::isolated(EgressGovernorConfig::for_test(100, 15_000, 3));
        let scope = format!("a6-i1-{}", ulid::Ulid::new());
        meter.observe(&scope, 20, 1_000);
        meter.observe(&scope, 35, 2_000);
        assert_eq!(
            meter.headroom(&scope, 2_000).map(|value| value.remaining),
            Some(65)
        );
        assert_eq!(meter.headroom(&scope, 60_000), None);
    }

    #[test]
    fn a6_i1_shared_meter_registry_is_keyed_by_unique_egress_scope() {
        let config = EgressGovernorConfig::for_test(100, 15_000, 3);
        let writer = EgressMeter::shared(config);
        let reader = EgressMeter::shared(config);
        let scope = format!("a6-i1-shared-{}", ulid::Ulid::new());
        writer.observe(&scope, 41, 1_000);
        assert_eq!(
            reader.headroom(&scope, 1_001).map(|value| value.remaining),
            Some(59)
        );
        writer.clear(&scope);
    }

    #[test]
    fn a6_config_defaults_and_required_clamps_are_pinned_without_process_env() {
        assert_eq!(
            EgressGovernorConfig::from_raw(None, None, None),
            EgressGovernorConfig::for_test(2_400, 15_000, 3)
        );
        assert_eq!(
            EgressGovernorConfig::from_raw(Some("1"), Some("1"), Some("0")),
            EgressGovernorConfig::for_test(100, 1_000, 3)
        );
        assert_eq!(
            EgressGovernorConfig::from_raw(Some("9999"), Some("999999"), Some("999")),
            EgressGovernorConfig::for_test(6_000, 60_000, 100)
        );
    }

    #[test]
    fn a6_r2_i2_budget_priority_is_scope_env_then_observed_then_global_default() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let scope = format!("a6-r2-priority-{}", ulid::Ulid::new());
        let config = EgressGovernorConfig::from_raw(Some("2400"), None, None);
        let scoped_name = config.scoped_budget_env_name(&scope);
        let _scoped_unset = crate::binance_futures::tests::TestEnvVar::unset(scoped_name.clone());

        assert_eq!(
            config.effective_budget(&scope, None),
            EffectiveEgressBudget {
                budget_1m: 2_400,
                source: BudgetSource::Default,
            }
        );
        assert_eq!(
            config.effective_budget(&scope, Some(6_000)),
            EffectiveEgressBudget {
                budget_1m: 6_000,
                source: BudgetSource::Observed,
            }
        );

        let _scoped = crate::binance_futures::tests::TestEnvVar::set(scoped_name, "3500");
        assert_eq!(
            config.effective_budget(&scope, Some(6_000)),
            EffectiveEgressBudget {
                budget_1m: 3_500,
                source: BudgetSource::Env,
            }
        );

        let global_scope = format!("a6-r2-global-{}", ulid::Ulid::new());
        let _global = crate::binance_futures::tests::TestEnvVar::set(
            REST_WEIGHT_BUDGET_ENV.to_string(),
            "3100",
        );
        let from_env = EgressGovernorConfig::from_env();
        let _global_scope_unset = crate::binance_futures::tests::TestEnvVar::unset(
            from_env.scoped_budget_env_name(&global_scope),
        );
        assert_eq!(
            from_env.effective_budget(&global_scope, None),
            EffectiveEgressBudget {
                budget_1m: 3_100,
                source: BudgetSource::Env,
            }
        );
        assert_eq!(
            from_env.effective_budget(&global_scope, Some(6_000)),
            EffectiveEgressBudget {
                budget_1m: 6_000,
                source: BudgetSource::Observed,
            },
            "the global env value remains a fallback below an observed exchange limit"
        );
        assert_eq!(BudgetSource::Default.as_str(), "default");
        assert_eq!(BudgetSource::Observed.as_str(), "observed");
        assert_eq!(BudgetSource::Env.as_str(), "env");
    }

    #[test]
    fn a6_r2_i2_observed_6000_moves_soft_and_hard_thresholds() {
        let meter = EgressMeter::isolated(EgressGovernorConfig::for_test(2_400, 15_000, 3));
        let soft_scope = format!("a6-r2-soft-{}", ulid::Ulid::new());
        meter.observe_budget(&soft_scope, 6_000, 1_000);
        meter.observe(&soft_scope, 5_399, 1_000);
        assert_eq!(
            meter.snapshot_for_log(&soft_scope, 1_000).state,
            ParkState::Open
        );
        meter.observe(&soft_scope, 5_400, 1_001);
        let soft = meter.snapshot_for_log(&soft_scope, 1_001);
        assert_eq!(soft.effective_budget_1m, 6_000);
        assert_eq!(soft.budget_source, BudgetSource::Observed);
        assert_eq!(soft.state, ParkState::Soft);

        let hard_scope = format!("a6-r2-hard-{}", ulid::Ulid::new());
        meter.observe_budget(&hard_scope, 6_000, 1_000);
        meter.observe(&hard_scope, 5_819, 1_000);
        assert_eq!(
            meter.snapshot_for_log(&hard_scope, 1_000).state,
            ParkState::Soft
        );
        meter.observe(&hard_scope, 5_820, 1_001);
        assert_eq!(
            meter.snapshot_for_log(&hard_scope, 1_001).state,
            ParkState::Hard
        );
    }

    #[test]
    fn a6_r2_warns_at_most_hourly_per_scope_for_budget_mismatch() {
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 15_000, 3),
            Arc::new(ManualGovernorClock::new(1_000)),
        );
        let scope = format!("a6-r2-warn-{}", ulid::Ulid::new());
        limiter.observe_egress_budget(&scope, 6_000, 1_000);
        limiter.observe_egress_budget(&scope, 6_000, 3_600_999);
        assert_eq!(
            limiter.budget_diagnostics_for_test(&scope),
            Some((Some(6_000), 1, 0))
        );
        limiter.observe_egress_budget(&scope, 6_000, 3_601_000);
        assert_eq!(
            limiter.budget_diagnostics_for_test(&scope),
            Some((Some(6_000), 2, 0))
        );
    }

    #[test]
    fn a6_r2_counts_budget_excess_without_429_without_changing_park_state() {
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(100, 15_000, 3),
            Arc::new(ManualGovernorClock::new(1_000)),
        );
        let scope = format!("a6-r2-excess-{}", ulid::Ulid::new());
        limiter.observe_egress_weight(&scope, 101, 1_000);
        let state_before = limiter.meter.snapshot_for_log(&scope, 1_000).state;
        limiter.record_budget_exceeded_without_429(&scope, 200);
        limiter.record_budget_exceeded_without_429(&scope, 429);
        limiter.record_budget_exceeded_without_429(&scope, 418);
        assert_eq!(
            limiter.budget_diagnostics_for_test(&scope),
            Some((None, 0, 2))
        );
        assert_eq!(
            limiter.meter.snapshot_for_log(&scope, 1_000).state,
            state_before,
            "the diagnostic counter must not alter parking semantics"
        );
    }

    #[tokio::test]
    async fn a6_i2_soft_parks_normal_to_boundary_while_exit_is_immediate() {
        let clock = Arc::new(ManualGovernorClock::new(59_500));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(100, 15_000, 3),
            clock.clone(),
        );
        let scope = format!("a6-i2-{}", ulid::Ulid::new());
        limiter.observe_egress_weight(&scope, 90, 59_500);
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "exit", 1.0, RequestClass::Exit, 0.0)
                .await,
            Admission::Admitted
        );
        assert_eq!(clock.now_ms.load(Ordering::SeqCst), 59_500);
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "normal", 1.0, RequestClass::PeriodicReconcile, 0.0,)
                .await,
            Admission::Admitted
        );
        assert_eq!(clock.now_ms.load(Ordering::SeqCst), 60_000);
    }

    #[tokio::test]
    async fn a6_i3_hard_rejects_normal_without_queue_but_exit_spends_reserve() {
        let clock = Arc::new(ManualGovernorClock::new(10_000));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(100, 15_000, 3),
            clock,
        );
        let scope = format!("a6-i3-{}", ulid::Ulid::new());
        limiter.observe_egress_weight(&scope, 97, 10_000);
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "entry", 1.0, RequestClass::Entry, 0.0)
                .await,
            Admission::HardRejected
        );
        assert_eq!(limiter.queued(&scope), 0);
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "exit", 1.0, RequestClass::Exit, 0.0)
                .await,
            Admission::Admitted
        );
    }

    #[test]
    fn a6_i4_exchange_headroom_caps_the_local_token_balance_with_min() {
        let mut bucket = TokenBucket::new(TokenBucketConfig::for_test(100.0, 0.0));
        assert!(
            bucket
                .acquire_reserving_with_cap(10.0, 0.0, Some(5.0))
                .is_some(),
            "100 local tokens must not override five units of exchange headroom"
        );
        assert_eq!(bucket.tokens, 100.0);
    }

    #[tokio::test]
    async fn a6_i4_refund_releases_local_and_observed_headroom_reservations() {
        let clock = Arc::new(ManualGovernorClock::new(1_000));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(100.0, 0.0),
            EgressGovernorConfig::for_test(100, 15_000, 3),
            clock,
        );
        let scope = format!("a6-i4-refund-{}", ulid::Ulid::new());
        limiter.observe_egress_weight(&scope, 0, 1_000);
        assert_eq!(
            limiter
                .acquire_prioritized(&scope, "normal", 10.0, RequestClass::EventTruth, 0.0)
                .await,
            Admission::Admitted
        );
        assert_eq!(limiter.meter.effective_remaining(&scope, 1_000), Some(90.0));
        limiter.refund_prioritized(&scope, 10.0).await;
        assert_eq!(
            limiter.meter.effective_remaining(&scope, 1_000),
            Some(100.0)
        );
        assert_eq!(limiter.stored_tokens(&scope).await, Some(100.0));
    }

    #[tokio::test]
    async fn a6_i5_waiters_release_event_truth_then_entry_then_periodic_reconcile() {
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(3.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 15_000, 3),
            Arc::new(crate::svc::SystemClock),
        );
        let scope = format!("a6-i5-{}", ulid::Ulid::new());
        limiter.set_stored_tokens(&scope, 0.0).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for (label, class) in [
            ("periodic", RequestClass::PeriodicReconcile),
            ("entry", RequestClass::Entry),
            ("truth", RequestClass::EventTruth),
        ] {
            let limiter = limiter.clone();
            let scope = scope.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let result = limiter
                    .acquire_prioritized(&scope, label, 1.0, class, 0.0)
                    .await;
                tx.send((label, result)).expect("priority result receiver");
            });
        }
        while limiter.queued(&scope) != 3 {
            tokio::task::yield_now().await;
        }
        let mut order = Vec::new();
        for _ in 0..3 {
            limiter.refund(&scope, 1.0).await;
            let (label, result) = rx.recv().await.expect("priority admission");
            assert_eq!(result, Admission::Admitted);
            order.push(label);
        }
        assert_eq!(order, vec!["truth", "entry", "periodic"]);
    }

    #[tokio::test]
    async fn a6_i6_fair_share_yields_to_b_after_three_a_admissions() {
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(5.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 15_000, 3),
            Arc::new(crate::svc::SystemClock),
        );
        let scope = format!("a6-i6-{}", ulid::Ulid::new());
        limiter.set_stored_tokens(&scope, 0.0).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for credential in ["A", "A", "A", "A", "B"] {
            let limiter = limiter.clone();
            let scope = scope.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let result = limiter
                    .acquire_prioritized(&scope, credential, 1.0, RequestClass::EventTruth, 0.0)
                    .await;
                tx.send((credential, result)).expect("fair-share receiver");
            });
        }
        while limiter.queued(&scope) != 5 {
            tokio::task::yield_now().await;
        }
        let mut order = Vec::new();
        for _ in 0..4 {
            limiter.refund(&scope, 1.0).await;
            let (credential, result) = rx.recv().await.expect("fair-share admission");
            assert_eq!(result, Admission::Admitted);
            order.push(credential);
        }
        assert_eq!(order, vec!["A", "A", "A", "B"]);
    }

    #[tokio::test]
    async fn a6_i7_queue_wait_limit_returns_timed_out_and_removes_waiter() {
        let clock = Arc::new(ManualGovernorClock::new(1_000));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(1.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 1_000, 3),
            clock,
        );
        let scope = format!("a6-i7-{}", ulid::Ulid::new());
        limiter.set_stored_tokens(&scope, 0.0).await;
        // 外层超时只为让「等待上限失效」这类变异体**变红而不是挂死**
        //（统筹变异 a6m7 曾挂 2 h 24 min）；正常路径由总督自己的 1 s 上限返回。
        let admission = tokio::time::timeout(
            Duration::from_secs(30),
            limiter.acquire_prioritized(&scope, "stalled", 1.0, RequestClass::Entry, 0.0),
        )
        .await
        .expect("the governor's own queue wait cap must return TimedOut long before the test's outer timeout");
        assert_eq!(admission, Admission::TimedOut);
        assert_eq!(limiter.queued(&scope), 0);
    }

    #[tokio::test]
    async fn a6_i8_normal_queue_keeps_the_a7_dynamic_exit_reserve() {
        let clock = Arc::new(ManualGovernorClock::new(1_000));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(10.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 1_000, 3),
            clock,
        );
        let scope = format!("a6-i8-{}", ulid::Ulid::new());
        let admission = tokio::time::timeout(
            Duration::from_secs(30),
            limiter.acquire_prioritized(&scope, "normal", 1.0, RequestClass::PeriodicReconcile, 10.0),
        )
        .await
        .expect("the governor's own queue wait cap must return TimedOut long before the test's outer timeout");
        assert_eq!(
            admission,
            Admission::TimedOut,
            "ten local tokens cannot pay cost one while preserving the two-position reserve ten"
        );
        assert_eq!(limiter.stored_tokens(&scope).await, Some(10.0));
    }

    #[test]
    fn a6_i10_governor_log_and_transition_error_codes_are_source_pinned() {
        let source = include_str!("rate_limiter.rs")
            .split("#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for needle in [
            "binance egress governor",
            "budget_source = snapshot.budget_source.as_str()",
            "budget exceeded without 429",
            "BINANCE_EGRESS_SOFT_PARK",
            "BINANCE_EGRESS_HARD_PARK",
            "BINANCE_EGRESS_SOFT_PARK_CLEARED",
            "BINANCE_EGRESS_HARD_PARK_CLEARED",
        ] {
            assert!(
                source.contains(needle),
                "missing governor log shape: {needle}"
            );
        }
        let startup_source = include_str!("binance_futures.rs")
            .split("#[cfg(test)]\npub(crate) mod tests")
            .next()
            .expect("production Binance source");
        assert!(
            startup_source.contains("budget_source = effective_budget.source.as_str()"),
            "startup diagnostics must log the resolved budget source"
        );
    }
}
