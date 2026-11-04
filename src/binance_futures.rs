use anyhow::{anyhow, Context};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::binance_response_settlement::{
    self, Fact as CooldownFact, Outcome as SettlementOutcome, Settlements,
};
use crate::rate_limiter::{
    deterministic_jitter_ms, parse_retry_after_ms, retry_decision, Admission,
    CredentialRateLimiter, EgressGovernorConfig, OrderAdmission, OrderCountConfig,
    OrderCountLimiter, RequestClass, RetryClass, RetryDecision, RetryInput, RetryScope,
    TokenBucketConfig,
};
use crate::store::{DbKind, Store};
use crate::svc::{Clock, SystemClock};

type HmacSha256 = Hmac<Sha256>;

#[cfg(test)]
#[path = "lifecycle_send_tests.rs"]
mod lifecycle_send_tests;

#[cfg(test)]
#[path = "binance_response_settlement_tests.rs"]
mod response_settlement_tests;

#[cfg(test)]
#[path = "binance_unsigned_settlement_tests.rs"]
mod unsigned_settlement_tests;

#[derive(Clone)]
pub struct BinanceKeys {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Clone)]
pub struct BinanceFuturesClient {
    base_url: String,
    egress_scope: String,
    credential_id: String,
    request_limiter: CredentialRateLimiter,
    order_count_limiter: OrderCountLimiter,
    exit_weight_reserve: f64,
    protectable_exit_budget_1m: f64,
    exposure_cache: Arc<tokio::sync::Mutex<Option<EgressExposureCache>>>,
    http: reqwest::Client,
    keys: BinanceKeys,
    recv_window_ms: u64,
    clock: Arc<dyn Clock>,
    cooldown_store: Option<Store>,
    settlements: Arc<Settlements>,
    #[cfg(test)]
    admission_exposure_error: Option<String>,
    #[cfg(test)]
    mock_http: Option<MockHttp>,
    #[cfg(test)]
    header_observer: Option<Arc<dyn Fn(u16) + Send + Sync>>,
    #[cfg(test)]
    gate_wait_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    observation_audit: Arc<Mutex<Vec<(&'static str, i64)>>>,
}

enum SignedAuthority<'a> {
    Ordinary,
    Protection {
        permit: &'a mut crate::protection_dispatch::ProtectionDispatchPermit,
        wire: &'a crate::protection_dispatch::AlgoWire,
    },
}
impl SignedAuthority<'_> {
    async fn authorize(
        &mut self,
        credential: &str,
        method: &str,
        path: &str,
        params: &[(String, String)],
    ) -> anyhow::Result<()> {
        match (method, path, self) {
            ("POST", "/fapi/v1/algoOrder", Self::Protection { permit, wire }) => {
                wire.validate_params(params)?;
                permit.authorize(credential, wire).await
            }
            ("POST", "/fapi/v1/algoOrder", _) => Err(anyhow!(
                "Algo POST requires a managed protection dispatch permit"
            )),
            (_, _, Self::Ordinary) => Ok(()),
            _ => Err(anyhow!(
                "protection dispatch permit cannot authorize another endpoint"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct EgressExposureCache {
    open_positions: u64,
    checked_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ExposureAdmission {
    Admit { reserved_after: f64 },
    Reject { reserved_after: f64, budget_1m: f64 },
}

#[cfg(test)]
#[derive(Clone)]
struct MockHttp {
    requests: Arc<Mutex<Vec<String>>>,
    responses: Arc<Mutex<std::collections::VecDeque<MockResponse>>>,
    retry_records: Arc<Mutex<Vec<RetryRecord>>>,
    order_count_headers: Option<(Option<u64>, Option<u64>)>,
    /// **在每一次请求真正取到响应之前**运行的钩子。
    ///
    /// 一次对账"读交易所"与"落库"之间
    /// 隔着好几个往返，TOCTOU 就发生在那中间。没有这个钩子，用例只能在
    /// 调用前后做事，"中间"是观察不到的——而观察点写早还是写晚，正是
    /// 那条修法的全部内容。钩子让"对账在路上时又来了一次 latch"成为
    /// 一条**确定性**的用例，而不是靠 sleep 赌时序。
    on_request: Option<MockRequestHook>,
}

/// 钩子签名：拿到请求路径，返回一个可等待的动作（例如再闩一次 flatten）。
#[cfg(test)]
type MockRequestHook = Arc<
    dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync,
>;

#[cfg(test)]
#[derive(Clone, Debug)]
struct MockResponse {
    status: u16,
    retry_after: Option<String>,
    body: String,
}

#[derive(Clone, Debug)]
struct HttpAttemptResponse {
    status: u16,
    retry_after: Option<String>,
    used_weight_1m: Option<u64>,
    order_count_1m: Option<u64>,
    order_count_10s: Option<u64>,
    body: String,
}

enum HttpReply {
    Network(reqwest::Response),
    #[cfg(test)]
    Mock(HttpAttemptResponse),
}

impl HttpReply {
    fn headers(&self) -> HttpAttemptResponse {
        match self {
            Self::Network(response) => {
                let (order_count_1m, order_count_10s) = order_count_headers(response.headers());
                HttpAttemptResponse {
                    status: response.status().as_u16(),
                    retry_after: response
                        .headers()
                        .get(RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                    used_weight_1m: used_weight_1m(response.headers()),
                    order_count_1m,
                    order_count_10s,
                    body: String::new(),
                }
            }
            #[cfg(test)]
            Self::Mock(response) => HttpAttemptResponse {
                body: String::new(),
                ..response.clone()
            },
        }
    }

    async fn body(self) -> String {
        match self {
            Self::Network(response) => response.text().await.unwrap_or_default(),
            #[cfg(test)]
            Self::Mock(response) => response.body,
        }
    }
}

async fn unsigned_step_before_deadline<T>(
    deadline: Option<tokio::time::Instant>,
    future: impl std::future::Future<Output = T>,
) -> anyhow::Result<T> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| anyhow!("BINANCE_BUDGET_DISCOVERY_TIMEOUT")),
        None => Ok(future.await),
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct RetryRecord {
    credential_id: String,
    endpoint: String,
    status: u16,
    retry_after: Option<String>,
    retry_number: u32,
    next_wait_ms: u64,
    request_class: &'static str,
    scope: &'static str,
    decision: &'static str,
}

#[derive(Debug)]
enum NonIdempotentWriteOutcome {
    Response(HttpAttemptResponse),
    VerifyTruth,
}

fn ip_ban_registry() -> &'static Mutex<HashMap<String, i64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rate_limit_registry() -> &'static Mutex<HashMap<String, i64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Floor after an Exit probe itself receives 429/418. Successful probes do not
/// start this interval: a minimum exit sequence needs to run without 15 seconds
/// of artificial pauses between its three steps.
const EXIT_PROBE_MIN_INTERVAL_MS: i64 = 5_000;

#[derive(Clone, Copy, Debug)]
struct ExitProbeState {
    window_until_ms: i64,
    probes_spent: u32,
    last_probe_failed_at_ms: Option<i64>,
}

impl Default for ExitProbeState {
    fn default() -> Self {
        Self {
            window_until_ms: i64::MIN,
            probes_spent: 0,
            last_probe_failed_at_ms: None,
        }
    }
}

/// Process-local fallback when no Store exists, plus a fast rejection cache.
/// With a Store, the SQL CAS remains authoritative across processes.
fn exit_probe_registry() -> &'static Mutex<HashMap<String, ExitProbeState>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, ExitProbeState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Which of the two egress guard calls is running. The pre-gate call is a cheap
/// fail-fast so a parked request neither waits for tokens nor queues on the
/// single-flight transport gate. The authoritative call runs while holding that
/// gate, which is the only place the exit probe can be spent without racing
/// every sibling request that has not reached the gate yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EgressGuardPhase {
    Fast,
    Authoritative,
}

fn egress_transport_gates() -> &'static Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> {
    static GATES: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Return the process-shared single-flight gate for one real egress boundary.
/// Public and credentialed REST clients must use this exact registry so a
/// cooldown observed by either side cannot cross the other's final send edge.
pub(crate) fn transport_gate_for_scope(egress_scope: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut gates = egress_transport_gates()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    gates
        .entry(egress_scope.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn blocked_log_registry() -> &'static Mutex<HashMap<String, i64>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 冷却的两张持久表。抽成枚举是为了让**方言分支只有一份**：凭据 client 与公共端点
/// 发布器共用同一段 upsert，否则修了一边、另一边继续保留旧语义。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CooldownTable {
    IpBan,
    RateLimit,
}

impl CooldownTable {
    fn table(self) -> &'static str {
        match self {
            Self::IpBan => "binance_ip_ban_cooldowns",
            Self::RateLimit => "binance_rate_limit_cooldowns",
        }
    }

    fn column(self) -> &'static str {
        match self {
            Self::IpBan => "ban_until_ms",
            Self::RateLimit => "cooldown_until_ms",
        }
    }

    fn registry(self) -> &'static Mutex<HashMap<String, i64>> {
        match self {
            Self::IpBan => ip_ban_registry(),
            Self::RateLimit => rate_limit_registry(),
        }
    }
}

/// 只增不减地把冷却写进持久表。表名与列名来自上面的闭集，不含外部输入。
pub(crate) async fn persist_cooldown(
    store: &Store,
    table: CooldownTable,
    egress_scope: &str,
    until_ms: i64,
    now_ms: i64,
) -> anyhow::Result<()> {
    let (name, column) = (table.table(), table.column());
    let query = match store.kind() {
        DbKind::MySql => format!(
            "INSERT INTO {name} (egress_scope, {column}, updated_at_ms) VALUES (?, ?, ?) \
             ON DUPLICATE KEY UPDATE {column} = GREATEST({column}, VALUES({column})), \
             updated_at_ms = VALUES(updated_at_ms)"
        ),
        _ => format!(
            "INSERT INTO {name} (egress_scope, {column}, updated_at_ms) VALUES (?, ?, ?) \
             ON CONFLICT(egress_scope) DO UPDATE SET \
             {column} = MAX({name}.{column}, excluded.{column}), \
             updated_at_ms = excluded.updated_at_ms"
        ),
    };
    sqlx::query(&query)
        .bind(egress_scope)
        .bind(until_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .with_context(|| format!("persist Binance cooldown into {name}"))?;
    Ok(())
}

#[cfg(not(test))]
fn cooldown_store_registry() -> &'static Mutex<Option<Store>> {
    static STORE: OnceLock<Mutex<Option<Store>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(None))
}

#[cfg(not(test))]
pub(crate) fn install_cooldown_store(store: Store) {
    *cooldown_store_registry()
        .lock()
        .expect("Binance cooldown store registry lock poisoned") = Some(store);
}

#[cfg(test)]
pub(crate) fn install_cooldown_store(_store: Store) {
    // Test binaries use the process-lifetime Store below. Per-test Store::connect
    // calls must never replace it with a database that the test later deletes.
}

#[cfg(test)]
fn test_cooldown_store() -> &'static OnceLock<Store> {
    static STORE: OnceLock<Store> = OnceLock::new();
    &STORE
}

#[cfg(test)]
pub(crate) fn test_cooldown_store_initialized() -> bool {
    test_cooldown_store().get().is_some()
}

#[cfg(test)]
pub(crate) fn install_test_cooldown_store(store: Store) {
    let _ = test_cooldown_store().set(store);
}

#[cfg(not(test))]
pub(crate) fn registered_cooldown_store() -> anyhow::Result<Store> {
    cooldown_store_registry()
        .lock()
        .map_err(|_| anyhow!("Binance cooldown store registry lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow!("Binance cooldown store is not initialized"))
}

#[cfg(test)]
pub(crate) fn registered_cooldown_store() -> anyhow::Result<Store> {
    test_cooldown_store()
        .get()
        .cloned()
        .ok_or_else(|| anyhow!("process-lifetime Binance cooldown test store is not initialized"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EgressCooldown {
    pub ban_until_ms: i64,
    pub rate_limit_until_ms: i64,
}

/// Read the process registry and the shared database as one egress truth.
/// BIGINT values always cross sqlx Any as text so SQLite cannot truncate the
/// millisecond epoch to its low 32 bits.
pub(crate) async fn shared_egress_cooldown(
    egress_scope: &str,
    store: Option<&Store>,
    now_ms: i64,
) -> anyhow::Result<EgressCooldown> {
    let memory_ban = ip_ban_registry()
        .lock()
        .map_err(|_| anyhow!("binance IP-ban registry lock poisoned"))?
        .get(egress_scope)
        .copied()
        .unwrap_or_default();
    let memory_rate_limit = rate_limit_registry()
        .lock()
        .map_err(|_| anyhow!("binance rate-limit registry lock poisoned"))?
        .get(egress_scope)
        .copied()
        .unwrap_or_default();
    let (persisted_ban, persisted_rate_limit) = if let Some(store) = store {
        // The SQLx Any empty-result invariant is documented in `store.rs`.
        // Keep these at-most-one-row reads fully drained as defense in depth.
        let ban = sqlx::query(
            "SELECT CAST(ban_until_ms AS CHAR) AS until_ms_text \
             FROM binance_ip_ban_cooldowns WHERE egress_scope = ?",
        )
        .bind(egress_scope)
        .fetch_all(store.pool())
        .await
        .context("load persisted Binance IP-ban cooldown")?
        .into_iter()
        .next()
        .map(|row| {
            crate::store::try_get_i64_text(&row, "until_ms_text")
                .context("decode persisted Binance IP-ban cooldown")
        })
        .transpose()?
        .unwrap_or_default();
        let rate_limit = sqlx::query(
            "SELECT CAST(cooldown_until_ms AS CHAR) AS until_ms_text \
             FROM binance_rate_limit_cooldowns WHERE egress_scope = ?",
        )
        .bind(egress_scope)
        .fetch_all(store.pool())
        .await
        .context("load persisted Binance rate-limit cooldown")?
        .into_iter()
        .next()
        .map(|row| {
            crate::store::try_get_i64_text(&row, "until_ms_text")
                .context("decode persisted Binance rate-limit cooldown")
        })
        .transpose()?
        .unwrap_or_default();
        (ban, rate_limit)
    } else {
        (0, 0)
    };
    let cooldown = EgressCooldown {
        ban_until_ms: memory_ban.max(persisted_ban),
        rate_limit_until_ms: memory_rate_limit.max(persisted_rate_limit),
    };
    tracing::trace!(
        %egress_scope,
        now_ms,
        ban_until_ms = cooldown.ban_until_ms,
        rate_limit_until_ms = cooldown.rate_limit_until_ms,
        "resolved shared Binance egress cooldown"
    );
    Ok(cooldown)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveBanDecision {
    Allow,
    Reject { remaining_ms: u64 },
}

fn active_ban_decision(now_ms: i64, ban_until_ms: i64) -> ActiveBanDecision {
    if now_ms < ban_until_ms {
        ActiveBanDecision::Reject {
            remaining_ms: ban_until_ms.saturating_sub(now_ms) as u64,
        }
    } else {
        ActiveBanDecision::Allow
    }
}

pub(crate) fn egress_scope_for_transport(
    venue: Option<crate::venue::Venue>,
    base_url: &str,
) -> anyhow::Result<String> {
    egress_scope_for_transport_and_route(
        venue,
        base_url,
        std::env::var("BINANCE_EGRESS_ROUTE_ID").ok().as_deref(),
    )
}

/// 把一次**公共端点**（exchangeInfo / premiumIndex）观察到的 418/429 发布到
/// 进程内共享的出口冷却表。
///
/// 公共端点走的是 `exchange_filters.rs` 里另一个 `reqwest::Client`，带**每实例**的
/// 封禁状态，而 `ExchangeFilterCache` 在每次保护操作里都会被重新构造——于是它的
/// 封禁记忆每次都清零，是全仓唯一一条结构上无法退避的路径。但权重和封禁都按
/// **IP** 计量：公共端点撞到的 418，凭据路径一样被封。把它发布到共享表，
/// 治理器与战役守卫才能看见。
///
/// **只做写侧。** 读侧（在公共路径上加 `ensure_not_ip_banned`）故意不做：
/// `kill_switch.rs` 的 close_constraints 会在一个**永远是冷的**缓存上走到
/// `fetch_filters`，在那里加闸等于在封禁期挡住 reduce-only 平仓——拿限流问题
/// 换未保护敞口，方向反了。
/// 生产入口：从进程级注册表取 store 后委派。
///
/// 拆出 `_with_store` 是为了让**落库这一步可被确定性地断言**——它是这条链上真正
/// 载重的一半（VM 守卫只读 SQLite），而经全局注册表的版本会与并发用例里的
/// `Store::connect` 抢同一个格子，写不出稳定用例。
pub(crate) async fn publish_public_endpoint_cooldown(
    venue: Option<crate::venue::Venue>,
    status: u16,
    cooldown_ms: u64,
    now_ms: i64,
) -> anyhow::Result<String> {
    let store = registered_cooldown_store().ok();
    publish_public_endpoint_cooldown_with_store(store.as_ref(), venue, status, cooldown_ms, now_ms)
        .await
}

pub(crate) async fn publish_public_endpoint_cooldown_with_store(
    store: Option<&Store>,
    venue: Option<crate::venue::Venue>,
    status: u16,
    cooldown_ms: u64,
    now_ms: i64,
) -> anyhow::Result<String> {
    let egress_scopes = public_endpoint_egress_scopes(venue)?;
    let egress_scope = egress_scopes
        .first()
        .cloned()
        .expect("public endpoint scope set is never empty");
    let until_ms = now_ms.saturating_add(i64::try_from(cooldown_ms).unwrap_or(i64::MAX));
    let table = match status {
        418 => CooldownTable::IpBan,
        429 => CooldownTable::RateLimit,
        other => return Err(anyhow!("not a Binance rate-limit status: {other}")),
    };
    for scope in &egress_scopes {
        {
            let mut guard = table
                .registry()
                .lock()
                .map_err(|_| anyhow!("binance egress cooldown registry lock poisoned"))?;
            let entry = guard.entry(scope.clone()).or_default();
            // 只增不减：并发的两次观察里，短的那个截止时间不得覆盖长的。
            *entry = (*entry).max(until_ms);
        }
        // **必须落库**：VM 上的战役守卫只读 SQLite，进程内 registry 它一行也看不到。
        // 未安装 store（Store::connect 之前）时退回内存，并如实说明。
        match store {
            Some(store) => persist_cooldown(store, table, scope, until_ms, now_ms).await?,
            None => tracing::debug!(
                egress_scope = %scope,
                "public endpoint cooldown stayed in-process; no cooldown store installed yet"
            ),
        }
    }
    Ok(egress_scope)
}

fn public_endpoint_egress_scopes(
    venue: Option<crate::venue::Venue>,
) -> anyhow::Result<Vec<String>> {
    let primary = public_endpoint_egress_scope(venue)?;
    let route_is_set = std::env::var("BINANCE_EGRESS_ROUTE_ID")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    let mut scopes = vec![primary];
    if !route_is_set {
        if let Some(venue) = venue {
            let legacy_unconfigured_scope =
                egress_scope_for_transport(None, venue.rest_base_url())?;
            if !scopes.contains(&legacy_unconfigured_scope) {
                scopes.push(legacy_unconfigured_scope);
            }
        }
    }
    Ok(scopes)
}

pub(crate) fn validate_egress_route_id(route_id: &str) -> Result<(), String> {
    if route_id.is_empty()
        || route_id.len() > 96
        || !route_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(
            "BINANCE_EGRESS_ROUTE_ID must be 1..=96 ASCII letters, digits, '-', '_' or '.'"
                .to_string(),
        );
    }
    Ok(())
}

fn egress_scope_for_transport_and_route(
    venue: Option<crate::venue::Venue>,
    base_url: &str,
    route_id: Option<&str>,
) -> anyhow::Result<String> {
    egress_scope_for_transport_and_route_with_prefix(
        "binance-futures-rest",
        venue,
        base_url,
        route_id,
    )
}

pub(crate) fn egress_scope_for_transport_and_route_with_prefix(
    prefix: &str,
    venue: Option<crate::venue::Venue>,
    base_url: &str,
    route_id: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(route_id) = route_id.map(str::trim).filter(|value| !value.is_empty()) {
        validate_egress_route_id(route_id).map_err(anyhow::Error::msg)?;
        let venue = venue
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unconfigured".to_string());
        return Ok(format!("{prefix}:route:{route_id}:venue:{venue}"));
    }
    match venue {
        Some(venue) => Ok(format!("{prefix}:venue:{venue}")),
        None => {
            let mut url = url::Url::parse(base_url).context("parse Binance REST base URL")?;
            url.set_query(None);
            url.set_fragment(None);
            let normalized = url.as_str().trim_end_matches('/');
            Ok(format!("{prefix}:base-url:{normalized}"))
        }
    }
}

/// Publish a cooldown observed on a non-REST Binance transport.  The caller
/// supplies the fully-derived scope, so ws-fapi can persist exactly the same
/// conservative semantics without ever touching the REST key.
pub(crate) async fn publish_scoped_transport_cooldown(
    store: &Store,
    egress_scope: &str,
    status: u16,
    retry_after: Option<&str>,
    now_ms: i64,
) -> anyhow::Result<()> {
    let (table, cooldown_ms) = match status {
        418 => (
            CooldownTable::IpBan,
            parse_retry_after_ms(retry_after, now_ms)
                .unwrap_or_default()
                .max(crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS),
        ),
        429 => (
            CooldownTable::RateLimit,
            rate_limit_cooldown_ms(retry_after, now_ms),
        ),
        _ => return Ok(()),
    };
    let until_ms = now_ms.saturating_add(i64::try_from(cooldown_ms).unwrap_or(i64::MAX));
    {
        let mut registry = table
            .registry()
            .lock()
            .map_err(|_| anyhow!("binance cooldown registry lock poisoned"))?;
        let current = registry.entry(egress_scope.to_string()).or_default();
        *current = (*current).max(until_ms);
    }
    persist_cooldown(store, table, egress_scope, until_ms, now_ms).await
}

const ORDER_ENDPOINTS: [&str; 2] = ["/fapi/v1/order", "/fapi/v1/algoOrder"];

fn signed_request_weight(method: &str, path: &str, params: &[(String, String)]) -> f64 {
    let has_symbol = params
        .iter()
        .any(|(name, value)| name == "symbol" && !value.trim().is_empty());
    match path {
        path if method == "POST" && ORDER_ENDPOINTS.contains(&path) => 0.0,
        "/fapi/v1/openOrders" | "/fapi/v1/openAlgoOrders" if !has_symbol => 40.0,
        "/fapi/v1/userTrades" => 5.0,
        "/fapi/v1/income" => 30.0,
        // Binance 的 2024-08-07 changelog（2024-09-03 生效）把这两条从 5 提到 10，
        // 而 endpoint 页至今仍渲染 5。冲突未解，且只有读一次真实
        // `X-MBX-USED-WEIGHT-1M` 差值才能定论。**按高的那个记**：低估的后果是
        // 治理器以为还有余量而继续发，正是 418 的成因；高估只损失一点吞吐。
        "/fapi/v2/balance" | "/fapi/v2/positionRisk" => 10.0,
        // 兜底 1.0 是**刻意保守**，不是"未知即 1"。只有上面的两个 POST
        // 下单端点能绕过 IP 桶；其它未知或非正开销仍由 limiter 抬回 1。
        _ => 1.0,
    }
}

/// 公共端点发布冷却时用的 egress scope。与凭据路径同一个派生函数——
/// 接线用例要按同一个 key 断言，否则测的是"写进了某个 key"而非"写进了对的 key"。
pub(crate) fn public_endpoint_egress_scope(
    venue: Option<crate::venue::Venue>,
) -> anyhow::Result<String> {
    egress_scope_for_transport(venue, "https://fapi.binance.com")
}

/// 只读地取某个 scope 的 429 冷却截止时间（供跨模块接线用例断言）。
#[cfg(test)]
pub(crate) fn rate_limit_cooldown_until_for_tests(egress_scope: &str) -> i64 {
    rate_limit_registry()
        .lock()
        .expect("rate-limit registry lock")
        .get(egress_scope)
        .copied()
        .unwrap_or_default()
}

/// 启动时打印生效的出口权重预算、它的来源，以及两类静默错配。
///
/// **绝不因此拒绝启动。** 起不来的引擎会让交易所侧的杠杆仓位失去对账、失去保护挂单、
/// 失去 kill switch——严格劣于一个配错的桶。所以这里只告警。
///
/// 之所以需要它：`f79a7c8` 改掉了一个**已经上线的数字的含义**，而没有任何一条日志
/// 说过生效预算是多少、来自哪个变量。于是继承来的 `RUSTCORE_BINANCE_TESTNET_RPS=1.5`
/// 被当成 1.5 权重/秒，一轮 100 权重的对账被拉成 60 秒下限、对着配置的 30 秒周期，
/// 表现出来像是引擎坏了，实际是配置语义变了。
pub fn log_effective_egress_budget() {
    let round_weight = periodic_reconcile_round_weight();
    let interval_ms = crate::reconcile::periodic_reconcile_interval_ms();
    let exit_reserve = exit_weight_reserve_from_env();
    let governor = EgressGovernorConfig::from_env();
    for venue in [None, Some(crate::venue::Venue::Testnet)] {
        let resolved = crate::rate_limiter::TokenBucketConfig::describe_from_env(venue);
        let protectable_exit_weight_per_position = protectable_exit_weight_per_position();
        let protectable_exit_budget_1m = protectable_exit_budget_1m(
            &resolved.config,
            std::env::var("RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M")
                .ok()
                .as_deref(),
        );
        let max_protectable_positions =
            (protectable_exit_budget_1m / protectable_exit_weight_per_position).floor() as u64;
        let venue_label = venue
            .map(|value| value.to_string())
            .unwrap_or_else(|| "generic".to_string());
        let egress_scope = public_endpoint_egress_scope(venue).unwrap_or_else(|error| {
            tracing::warn!(
                venue = %venue_label,
                %error,
                "could not derive Binance egress scope for startup budget diagnostics"
            );
            format!("unresolved:{venue_label}")
        });
        let effective_budget = governor.effective_budget(&egress_scope, None);
        tracing::info!(
            venue = %venue_label,
            egress_scope = %egress_scope,
            weight_per_sec = resolved.config.refill_per_sec,
            weight_burst = resolved.config.capacity,
            rate_source = %resolved.rate_source,
            burst_source = %resolved.burst_source,
            exit_weight_reserve = exit_reserve,
            budget_1m = effective_budget.budget_1m,
            budget_source = effective_budget.source.as_str(),
            queue_wait_max_ms = governor.queue_wait_max_ms,
            fair_share_burst = governor.fair_share_burst,
            protectable_exit_budget_1m,
            protectable_exit_weight_per_position,
            max_protectable_positions,
            reconcile_round_weight = round_weight,
            reconcile_interval_ms = interval_ms,
            "binance egress weight budget in effect"
        );
        if resolved.legacy_names_only {
            tracing::warn!(
                venue = %venue_label,
                rate_source = %resolved.rate_source,
                burst_source = %resolved.burst_source,
                weight_per_sec = resolved.config.refill_per_sec,
                "legacy *_RPS/*_BURST names are in effect and their UNIT has changed: the value \
                 is now weight per second, not requests per second. One full reconcile round \
                 costs the reconcile_round_weight logged above (the FullScan upper bound; A8-R1 \
                 narrowed rounds cost 2n+20 and StreamOnly costs 0)."
            );
        }
        if let Some(required_ms) = crate::rate_limiter::reconcile_cadence_shortfall_ms(
            round_weight,
            resolved.config.refill_per_sec,
            interval_ms,
        ) {
            tracing::warn!(
                venue = %venue_label,
                required_ms,
                reconcile_interval_ms = interval_ms,
                weight_per_sec = resolved.config.refill_per_sec,
                "the configured weight budget cannot sustain the configured reconcile cadence; \
                 rounds will queue and position truth will go stale WITHOUT any 418 to explain it"
            );
        }
    }
    let order_count = OrderCountConfig::describe_from_env();
    tracing::info!(
        order_count_1m = order_count.config.one_minute,
        order_count_10s = order_count.config.ten_seconds,
        order_count_1m_source = order_count.one_minute_source,
        order_count_10s_source = order_count.ten_seconds_source,
        "binance order-count budget in effect"
    );
}

/// 一轮周期对账要花的 IP 权重。
///
/// **由权重表本身算出，不写死**——写死的常数会与表脱节，而这个数字正是用来判断
/// 「配置的预算撑不撑得起配置的对账节奏」的，脱节就等于判据失真。四个调用见
/// `reconcile.rs` 的 openOrders / openAlgoOrders / positionRisk / balance。
pub(crate) fn periodic_reconcile_round_weight() -> f64 {
    let no_params: Vec<(String, String)> = Vec::new();
    signed_request_weight("GET", "/fapi/v1/openOrders", &no_params)
        + signed_request_weight("GET", "/fapi/v1/openAlgoOrders", &no_params)
        + signed_request_weight("GET", "/fapi/v2/positionRisk", &no_params)
        + signed_request_weight("GET", "/fapi/v2/balance", &no_params)
}

/// Base IP weight of one symbol-narrowed reconcile round. Each covered
/// symbol has two scoped order reads; positionRisk and balance are each read
/// once for the credential. Tail lookups for disappeared local orders are
/// deliberately outside this base figure and remain metered individually.
pub(crate) fn symbol_narrowed_reconcile_round_weight(symbol_count: usize) -> f64 {
    let symbol = vec![("symbol".to_string(), "BTCUSDT".to_string())];
    let per_symbol = signed_request_weight("GET", "/fapi/v1/openOrders", &symbol)
        + signed_request_weight("GET", "/fapi/v1/openAlgoOrders", &symbol);
    per_symbol * symbol_count as f64
        + signed_request_weight("GET", "/fapi/v2/positionRisk", &[])
        + signed_request_weight("GET", "/fapi/v2/balance", &[])
}

/// Weight needed to protect and, if necessary, flatten one position on one
/// egress. Derive it from the endpoint table so weight-table changes cannot
/// silently leave admission using an obsolete literal.
pub(crate) fn protectable_exit_weight_per_position() -> f64 {
    let symbol = vec![("symbol".to_string(), "BTCUSDT".to_string())];
    signed_request_weight("DELETE", "/fapi/v1/allOpenOrders", &symbol)
        + signed_request_weight("DELETE", "/fapi/v1/algoOrder", &symbol)
        + signed_request_weight("POST", "/fapi/v1/order", &symbol)
        + signed_request_weight("GET", "/fapi/v2/positionRisk", &symbol)
        + signed_request_weight("GET", "/fapi/v1/openAlgoOrders", &symbol)
}

pub(crate) fn protectable_exit_budget_1m(config: &TokenBucketConfig, raw: Option<&str>) -> f64 {
    let per_position_weight = protectable_exit_weight_per_position();
    let maximum = config.refill_per_sec * 60.0;
    let default = maximum * 0.25;
    raw.and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(default)
        .clamp(per_position_weight, maximum.max(per_position_weight))
}

pub(crate) fn protectable_exposure_admission(
    open_positions: u64,
    per_position_weight: f64,
    budget_1m: f64,
) -> ExposureAdmission {
    let reserved_after = (open_positions.saturating_add(1) as f64) * per_position_weight;
    if reserved_after <= budget_1m {
        ExposureAdmission::Admit { reserved_after }
    } else {
        ExposureAdmission::Reject {
            reserved_after,
            budget_1m,
        }
    }
}

/// Count active, non-simulation positions whose credential resolves to this
/// real egress. An unresolvable credential is charged to the queried scope:
/// unknown routing must never create apparently free protection capacity.
pub(crate) async fn open_positions_on_egress(
    store: &Store,
    egress_scope: &str,
) -> anyhow::Result<u64> {
    let sql = format!(
        "SELECT credential_id, COUNT(*) AS position_count FROM position_state \
         WHERE state IN ({}) GROUP BY credential_id",
        crate::position_state::ACTIVE_STATES
    );
    let rows = sqlx::query(&sql).fetch_all(store.pool()).await?;
    let mut total = 0_u64;
    for row in rows {
        let credential_id = row.get::<String, _>("credential_id");
        if crate::venue::is_simulation_credential(&credential_id) {
            continue;
        }
        let count = row.get::<i64, _>("position_count").max(0) as u64;
        let mapped_scope = crate::venue::rest_transport_for_credential(&credential_id)
            .map_err(anyhow::Error::msg)
            .and_then(|transport| egress_scope_for_transport(transport.venue, &transport.base_url));
        match mapped_scope {
            Ok(mapped_scope) if mapped_scope == egress_scope => {
                total = total.saturating_add(count);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    credential_id = %credential_id,
                    %egress_scope,
                    %error,
                    position_count = count,
                    "credential egress mapping failed; charging exposure to the queried scope"
                );
                total = total.saturating_add(count);
            }
        }
    }
    Ok(total)
}

/// 按账户的下单计量表读数（`X-MBX-ORDER-COUNT-1M` / `-10S`）。
///
/// 与 `X-MBX-USED-WEIGHT-1M` 是**两个不同的表**：权重按 IP，下单按账户（UID）。
/// 仓内此前一处都没有解析过它，于是唯一能被单个重度用户独占的那个计量表完全不可见。
/// 它不是当前的绑定约束（每用户 1200/分钟，约 300 个完整生命周期/分钟才撞上），
/// 但它是**随付费活动增长**的那一个，多用户上线前必须先能看见。
fn order_count_headers(headers: &HeaderMap) -> (Option<u64>, Option<u64>) {
    let read = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    (read("x-mbx-order-count-1m"), read("x-mbx-order-count-10s"))
}

fn used_weight_1m(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-mbx-used-weight-1m")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

// Logging only: never echo arbitrary header content (including RFC2822
// comments). Preserve numeric seconds or a normalized date, and distinguish
// a missing header from one that cannot be interpreted. Retry policy continues
// to parse the original value independently.
fn retry_after_log_value(retry_after: Option<&str>) -> String {
    let Some(raw) = retry_after else {
        return "absent".to_string();
    };
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return seconds.to_string();
    }
    if let Ok(at) = chrono::DateTime::parse_from_rfc2822(raw) {
        return at.to_rfc2822();
    }
    "invalid".to_string()
}

pub(crate) fn rate_limit_cooldown_ms(retry_after: Option<&str>, now_ms: i64) -> u64 {
    parse_retry_after_ms(retry_after, now_ms)
        .map(|value| value.max(100))
        .unwrap_or(2_000)
}

pub(crate) fn exit_weight_reserve_from_env() -> f64 {
    std::env::var("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(10.0)
        .clamp(1.0, 100.0)
}

/// One minimal L3 exit sequence spends three probes: cancel Algo protection,
/// cancel an ordinary order, then submit the reduce-only flatten. Four leaves
/// one bounded margin. Binance escalates only when requests continue after a
/// 429; four probes per window is far below the measured escalation traffic,
/// while one makes any such sequence structurally impossible.
pub(crate) fn exit_probes_per_window(raw: Option<&str>) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(4)
        .clamp(1, 8)
}

fn configured_exit_probes_per_window() -> u32 {
    exit_probes_per_window(
        std::env::var("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW")
            .ok()
            .as_deref(),
    )
}

#[cfg(test)]
fn egress_scope_for(credential_id: &str, base_url: &str) -> anyhow::Result<String> {
    let venue = crate::venue::venue_for_credential(credential_id).map_err(anyhow::Error::msg)?;
    egress_scope_for_transport(venue, base_url)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct BinanceOrderId(i64);

#[allow(dead_code)]
impl BinanceOrderId {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn as_i64(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for BinanceOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
#[allow(dead_code)]
pub struct BinanceAlgoId(i64);

#[allow(dead_code)]
impl BinanceAlgoId {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn as_i64(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for BinanceAlgoId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
#[allow(dead_code)]
pub struct BinanceClientAlgoId(String);

#[allow(dead_code)]
impl BinanceClientAlgoId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum BinanceActualOrderId {
    Numeric(i64),
    Text(String),
}

impl BinanceActualOrderId {
    pub fn normalized(&self) -> Option<String> {
        match self {
            Self::Numeric(value) if *value > 0 => Some(value.to_string()),
            Self::Text(value) => {
                let value = value.trim();
                (!value.is_empty() && value != "0").then(|| value.to_string())
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for BinanceActualOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric(value) => value.fmt(formatter),
            Self::Text(value) => value.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum BinanceAlgoConditionalOrderType {
    StopMarket,
    TakeProfitMarket,
    Stop,
    TakeProfit,
    TrailingStopMarket,
}

#[allow(dead_code)]
impl BinanceAlgoConditionalOrderType {
    fn as_binance(self) -> &'static str {
        match self {
            Self::StopMarket => "STOP_MARKET",
            Self::TakeProfitMarket => "TAKE_PROFIT_MARKET",
            Self::Stop => "STOP",
            Self::TakeProfit => "TAKE_PROFIT",
            Self::TrailingStopMarket => "TRAILING_STOP_MARKET",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct BinanceAlgoOrderResponse {
    #[serde(rename = "algoId", skip_serializing_if = "Option::is_none")]
    pub algo_id: Option<BinanceAlgoId>,
    #[serde(
        rename = "clientAlgoId",
        default,
        deserialize_with = "deserialize_optional_client_algo_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_algo_id: Option<BinanceClientAlgoId>,
    #[serde(
        rename = "algoStatus",
        default,
        deserialize_with = "deserialize_optional_nonempty_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub algo_status: Option<String>,
    #[serde(
        rename = "actualOrderId",
        default,
        deserialize_with = "deserialize_optional_actual_order_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub actual_order_id: Option<BinanceActualOrderId>,
    #[serde(flatten)]
    pub additional_fields: serde_json::Map<String, serde_json::Value>,
}

fn deserialize_optional_nonempty_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_string())
    }))
}

fn deserialize_optional_client_algo_id<'de, D>(
    deserializer: D,
) -> Result<Option<BinanceClientAlgoId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(deserialize_optional_nonempty_string(deserializer)?.map(BinanceClientAlgoId::new))
}

fn deserialize_optional_actual_order_id<'de, D>(
    deserializer: D,
) -> Result<Option<BinanceActualOrderId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let parsed = match value {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .filter(|value| *value > 0)
            .map(BinanceActualOrderId::Numeric),
        Some(serde_json::Value::String(value)) => {
            let value = value.trim();
            (!value.is_empty() && value != "0")
                .then(|| BinanceActualOrderId::Text(value.to_string()))
        }
        Some(other) => {
            return Err(serde::de::Error::custom(format!(
                "actualOrderId must be a positive integer or non-empty string, got {other}"
            )))
        }
    };
    Ok(parsed)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceOrderAck {
    #[serde(rename = "orderId")]
    pub order_id: BinanceOrderId,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub status: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct BinanceUserTrade {
    pub id: i64,
    pub symbol: String,
    #[serde(rename = "orderId", default)]
    pub order_id: Option<i64>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub qty: Option<String>,
    #[serde(rename = "quoteQty", default)]
    pub quote_qty: Option<String>,
    #[serde(default)]
    pub commission: Option<String>,
    #[serde(rename = "commissionAsset", default)]
    pub commission_asset: Option<String>,
    #[serde(rename = "realizedPnl", default)]
    pub realized_pnl: Option<String>,
    #[serde(default)]
    pub time: Option<i64>,
    #[serde(flatten)]
    pub additional_fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum UserTradesPageDisposition {
    Complete,
    Continue { next_from_id: i64 },
}

#[allow(dead_code)]
fn first_user_trades_from_id(last_trade_id: i64) -> anyhow::Result<i64> {
    last_trade_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("fill replay cursor cannot advance beyond i64::MAX"))
}

#[allow(dead_code)]
fn user_trades_page_disposition(
    page: &[BinanceUserTrade],
    page_limit: usize,
) -> anyhow::Result<UserTradesPageDisposition> {
    if !(1..=1_000).contains(&page_limit) {
        return Err(anyhow!("Binance userTrades page limit must be in 1..=1000"));
    }
    if page.len() > page_limit {
        return Err(anyhow!(
            "Binance userTrades returned {} rows above requested limit {page_limit}",
            page.len()
        ));
    }
    if page.len() < page_limit {
        return Ok(UserTradesPageDisposition::Complete);
    }
    let max_id = page
        .iter()
        .map(|trade| trade.id)
        .max()
        .ok_or_else(|| anyhow!("full Binance userTrades page unexpectedly had no rows"))?;
    let next_from_id = max_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("Binance userTrades id cannot advance beyond i64::MAX"))?;
    Ok(UserTradesPageDisposition::Continue { next_from_id })
}

impl BinanceFuturesClient {
    pub fn new(keys: BinanceKeys, credential_id: &str) -> anyhow::Result<Self> {
        Self::build(keys, credential_id, Some(registered_cooldown_store()?))
    }

    pub(crate) fn new_with_store(
        keys: BinanceKeys,
        credential_id: &str,
        store: Store,
    ) -> anyhow::Result<Self> {
        Self::build(keys, credential_id, Some(store))
    }

    /// 同 `new_with_mock_responses`，外加一个"每次请求前"的钩子。
    /// 用于把并发事件**确定性地**插进一次对账的中间。
    #[cfg(test)]
    pub(crate) fn new_with_mock_responses_and_hook(
        credential_id: &str,
        responses: Vec<(u16, String)>,
        on_request: MockRequestHook,
    ) -> Self {
        let mut client = Self::new_with_mock_responses(credential_id, responses);
        if let Some(mock) = client.mock_http.as_mut() {
            mock.on_request = Some(on_request);
        }
        client
    }

    /// 测试专用：带一条**按调用顺序出队**的 HTTP 响应队列的客户端，让
    /// reconcile 这类跨层流程能在不联网的前提下被端到端驱动（R5 FBL-R4-H3
    /// 的 `-2013` 裁决链就靠它整链落测——评审对 H3 的定性正是「契约条款被
    /// 生产不可达的合成态测试顶替」，接线必须有人钉）。
    ///
    /// 仅 `#[cfg(test)]`，不构成生产面。
    #[cfg(test)]
    pub(crate) fn new_with_mock_responses(
        credential_id: &str,
        responses: Vec<(u16, String)>,
    ) -> Self {
        Self {
            base_url: "https://local.stub.invalid".to_string(),
            egress_scope: format!("test-egress:{credential_id}"),
            credential_id: credential_id.to_string(),
            request_limiter: CredentialRateLimiter::new(TokenBucketConfig::for_test(
                10_000.0, 10_000.0,
            )),
            order_count_limiter: OrderCountLimiter::isolated(OrderCountConfig::for_test(
                1_200, 300,
            )),
            exit_weight_reserve: 0.0,
            protectable_exit_budget_1m: protectable_exit_weight_per_position(),
            exposure_cache: Arc::new(tokio::sync::Mutex::new(Some(EgressExposureCache {
                open_positions: 0,
                checked_at_ms: 0,
            }))),
            http: reqwest::Client::new(),
            keys: BinanceKeys {
                api_key: "local-test-key".to_string(),
                api_secret: "local-test-secret".to_string(),
            },
            recv_window_ms: 5_000,
            clock: Arc::new(crate::svc::SystemClock),
            cooldown_store: None,
            settlements: Settlements::new(),
            admission_exposure_error: None,
            header_observer: None,
            gate_wait_observer: None,
            observation_audit: Arc::new(Mutex::new(Vec::new())),
            mock_http: Some(MockHttp {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(
                    responses
                        .into_iter()
                        .map(|(status, body)| MockResponse {
                            status,
                            retry_after: None,
                            body,
                        })
                        .collect::<std::collections::VecDeque<_>>(),
                )),
                retry_records: Arc::new(Mutex::new(Vec::new())),
                order_count_headers: None,
                on_request: None,
            }),
        }
    }

    /// 内存 HTTP 队列**实际收到**的请求（`"{METHOD} {url}"`，含签名后的完整
    /// query）。wiring proof 用它断言"链路末端真的发出了这样一条交易所请求"
    /// ——例如 v2 的 `order_type=limit` 是否真的变成了 `type=LIMIT&price=…`。
    #[cfg(test)]
    pub(crate) fn recorded_requests(&self) -> Vec<String> {
        self.mock_http
            .as_ref()
            .map(|mock| {
                mock.requests
                    .lock()
                    .expect("lock in-memory HTTP request log")
                    .clone()
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn with_test_store(mut self, store: Store) -> Self {
        self.cooldown_store = Some(store);
        self
    }

    /// Replace only HTTP responses/capture; keep the production client identity,
    /// signing keys, clock, venue, store and admission configuration intact.
    #[cfg(test)]
    pub(crate) fn with_mock_http_from(mut self, source: &Self) -> Self {
        self.mock_http = Some(source.mock_http.clone().expect("HTTP fixture required"));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_admission_exposure_error(mut self, error: &str) -> Self {
        self.admission_exposure_error = Some(error.to_string());
        self
    }

    #[cfg(test)]
    pub(crate) fn with_egress_scope_for_test(mut self, egress_scope: String) -> Self {
        self.egress_scope = egress_scope;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_protectable_budget_for_test(mut self, budget_1m: f64) -> Self {
        self.protectable_exit_budget_1m = budget_1m;
        self
    }

    #[cfg(test)]
    pub(crate) fn park_429_for_test(&self, until_ms: i64) {
        rate_limit_registry()
            .lock()
            .expect("lock rate-limit registry")
            .insert(self.egress_scope.clone(), until_ms);
    }

    #[cfg(test)]
    pub(crate) fn park_418_for_test(&self, until_ms: i64) {
        ip_ban_registry()
            .lock()
            .expect("lock IP-ban registry")
            .insert(self.egress_scope.clone(), until_ms);
    }

    #[cfg(test)]
    pub(crate) fn clear_egress_for_test(&self) {
        rate_limit_registry()
            .lock()
            .expect("lock rate-limit registry")
            .remove(&self.egress_scope);
        ip_ban_registry()
            .lock()
            .expect("lock IP-ban registry")
            .remove(&self.egress_scope);
        exit_probe_registry()
            .lock()
            .expect("lock exit-probe registry")
            .remove(&self.egress_scope);
    }
}

/// 交易所 HTTP 的有界 deadline（请求上限，连接上限）。
///
/// 抽成纯函数 + 独立的 builder 施加点，是为了让"deadline 到底有没有被装上"
/// 可以被测试直接钉住——只写在 `Client::builder()` 链里，删掉它没有任何用例会红
/// 。
pub(crate) fn http_timeouts_from_env() -> (std::time::Duration, std::time::Duration) {
    let request_ms = std::env::var("BINANCE_HTTP_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000);
    let connect_ms = std::env::var("BINANCE_HTTP_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5_000);
    (
        std::time::Duration::from_millis(request_ms),
        std::time::Duration::from_millis(connect_ms),
    )
}

/// 交易所 **WebSocket** 的有界 deadline（连接上限，空闲上限）。
///
/// ****：
/// REST 侧在 R4 就装了 deadline，但两条关键 WebSocket——kline 行情流与
/// user-stream——的 `connect` 与 `read` **一直没有应用级上限**。
/// 对端完成 TCP/TLS 握手之后不再发任何 frame、也不 close，
/// `read.next()` 就永远停在 await 上：行情静默停滞、fill recovery 与真值对账
/// 再也不推进，而外层重连循环**永远拿不到返回**，看起来一切正常。
///
/// 空闲上限的取值依据：Binance user-stream 每 ~3 分钟发一次 ping，
/// kline 流按订阅周期持续推送。默认 300s 给了足够余量，
/// 超过它只能解释为**这条流已经死了**，必须重连而不是继续等。
///
/// 与 HTTP 那一对同样抽成纯函数：deadline 到底有没有被装上，可以被直接钉住。
pub(crate) fn ws_timeouts_from_env() -> (std::time::Duration, std::time::Duration) {
    let connect_ms = std::env::var("BINANCE_WS_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000);
    let idle_ms = std::env::var("BINANCE_WS_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300_000);
    (
        std::time::Duration::from_millis(connect_ms),
        std::time::Duration::from_millis(idle_ms),
    )
}

/// 一次「在 deadline 内取下一帧」的结果。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StreamPoll<T> {
    Frame(T),
    /// 对端正常结束。
    Ended,
    /// **空闲超过上限**——这条流按死流处置，不得与「正常结束」混为一谈。
    IdleTimeout,
}

/// 在 `idle` 上限内取流的下一帧。**两条 WebSocket 的唯一读取入口。**
///
/// 抽成函数是为了让「空闲上限到底有没有生效」可以**不依赖监听端口**被钉住：
/// 喂一个永不产出的流（`futures_util::stream::pending`）进来，
/// 断言它在上限内返回 `IdleTimeout` 而不是挂住。这条判据因此在拒绝 bind 的
/// 沙箱里同样可测——与 HTTP deadline 那两条慢服务用例不同，
/// 它不需要"接受连接但永不响应"的真实对端。
pub(crate) async fn next_frame_within<S>(
    idle: std::time::Duration,
    stream: &mut S,
) -> StreamPoll<S::Item>
where
    S: futures_util::Stream + Unpin,
{
    match tokio::time::timeout(idle, futures_util::StreamExt::next(stream)).await {
        Ok(Some(item)) => StreamPoll::Frame(item),
        Ok(None) => StreamPoll::Ended,
        Err(_elapsed) => StreamPoll::IdleTimeout,
    }
}

/// 在 `deadline` 内把一帧写出去。**两条 WebSocket 的唯一写入入口。**
///
/// R7 只给 read/connect 装了
/// 上界，**Pong 的 `send(...).await` 仍无界**——对端不读时 TCP 写缓冲填满，
/// `send` 就永远挂在 await 上。表现与 read 挂住完全一样：流静默停滞、
/// 外层重连循环拿不到返回，而我们已经"修过"了 read，更不容易再想到这里。
///
/// 与读侧同一条纪律：超时**必须带理由报出去**，不得静默忽略。
/// 忽略等于让一条写不动的连接继续假装健康。
pub(crate) async fn send_frame_within<S, T>(
    deadline: std::time::Duration,
    sink: &mut S,
    frame: T,
) -> anyhow::Result<()>
where
    S: futures_util::Sink<T> + Unpin,
    S::Error: std::fmt::Display,
{
    match tokio::time::timeout(deadline, futures_util::SinkExt::send(sink, frame)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(anyhow::anyhow!("websocket write failed: {error}")),
        Err(_elapsed) => Err(anyhow::anyhow!(
            "websocket write exceeded {}ms deadline",
            deadline.as_millis()
        )),
    }
}

/// 把 deadline 装到 client builder 上。**唯一的装配点**——生产与测试共用它。
pub(crate) fn apply_http_deadlines(
    builder: reqwest::ClientBuilder,
    timeouts: (std::time::Duration, std::time::Duration),
) -> reqwest::ClientBuilder {
    builder.timeout(timeouts.0).connect_timeout(timeouts.1)
}

impl BinanceFuturesClient {
    #[cfg(test)]
    pub(crate) fn http_deadlines_for_test() -> (std::time::Duration, std::time::Duration) {
        http_timeouts_from_env()
    }
}

impl BinanceFuturesClient {
    /// Discover a missing REST budget on the same transport and scope as truth
    /// reads. One bounded attempt per scope/minute; failure retains the
    /// conservative default. Existing cooldown, queue and transport gates apply.
    pub(crate) async fn discover_reconcile_budget(&self) {
        // The socket-free L4 exchange has no REST budget. Keep its configured
        // (or invalid) adapter from ever falling through to a real endpoint.
        #[cfg(feature = "l4-fault-injection")]
        if !matches!(crate::l4_fake_exchange::configured_db(), Ok(None)) {
            return;
        }
        if !self
            .request_limiter
            .needs_budget_discovery(&self.egress_scope)
        {
            return;
        }
        type Attempt = Arc<tokio::sync::Mutex<Option<std::time::Instant>>>;
        static ATTEMPTS: OnceLock<Mutex<HashMap<String, Attempt>>> = OnceLock::new();
        let gate = ATTEMPTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("budget discovery registry lock poisoned")
            .entry(self.egress_scope.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let work = async {
            let mut last_attempt =
                unsigned_step_before_deadline(Some(deadline), gate.lock()).await?;
            if !self
                .request_limiter
                .needs_budget_discovery(&self.egress_scope)
                || last_attempt.is_some_and(|at| at.elapsed() < Duration::from_secs(60))
            {
                return Ok::<(), anyhow::Error>(());
            }
            // Spend the retry slot immediately before dispatch. A local park
            // or cooldown sends nothing and must not phase-lock recovery out
            // of the next available minute. Cancellation after dispatch still
            // leaves the shared slot occupied.
            let response = self
                .send_unsigned_once_with_deadline(
                    "GET",
                    "/fapi/v1/exchangeInfo",
                    &[],
                    RequestClass::PeriodicReconcile,
                    Some(deadline),
                    || *last_attempt = Some(std::time::Instant::now()),
                )
                .await;
            let outcome = match response {
                Ok(response) if (200..300).contains(&response.status) => {
                    match crate::exchange_filters::parse_request_weight_limit_1m(&response.body) {
                        Some(limit) => {
                            self.request_limiter.observe_egress_budget(
                                &self.egress_scope,
                                limit,
                                self.clock.now_ms(),
                            );
                            "observed"
                        }
                        None => "invalid_metadata",
                    }
                }
                Ok(_) => "upstream_error",
                Err(error) => return Err(error),
            };
            tracing::info!(egress_scope = %self.egress_scope, outcome,
                "reconcile REST budget discovery");
            Ok(())
        };
        if let Err(error) = work.await {
            let error_code = if error.to_string() == "BINANCE_BUDGET_DISCOVERY_TIMEOUT" {
                "BINANCE_BUDGET_DISCOVERY_TIMEOUT"
            } else {
                "BINANCE_BUDGET_DISCOVERY_UNAVAILABLE"
            };
            tracing::warn!(egress_scope = %self.egress_scope,
                error_code,
                "reconcile REST budget discovery kept the configured fallback");
        }
    }

    fn build(
        keys: BinanceKeys,
        credential_id: &str,
        cooldown_store: Option<Store>,
    ) -> anyhow::Result<Self> {
        let transport = crate::venue::rest_transport_for_credential(credential_id)
            .map_err(anyhow::Error::msg)?;
        let base_url = transport.base_url.clone();
        let egress_scope = egress_scope_for_transport(transport.venue, &base_url)?;
        let request_limiter =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(transport.venue));
        let protectable_exit_budget_1m = protectable_exit_budget_1m(
            &TokenBucketConfig::from_env(transport.venue),
            std::env::var("RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M")
                .ok()
                .as_deref(),
        );
        let order_count_limiter = OrderCountLimiter::shared(OrderCountConfig::from_env());
        let recv_window_ms = std::env::var("BINANCE_RECV_WINDOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5_000);

        let mut headers = HeaderMap::new();
        headers.insert(
            "X-MBX-APIKEY",
            HeaderValue::from_str(keys.api_key.as_str())
                .context("invalid BINANCE api key header")?,
        );

        // ****
        //
        // 此前这个 client **既没有 request timeout 也没有 connect timeout**：
        // 一个永久 pending 的交易所请求会把调用方无限挂住。kill-switch 的
        // scope 循环是**串行** await 每个 credential 的撤单→平仓→对账，
        // 于是第一个挂住的账户会让**其后所有账户永远轮不到撤单与平仓**。
        // 设计要求 裁决原文：所有关键交易所 I/O 必须有**有界 deadline**；
        // timeout 只能记 failure/unknown 并**继续处理其他 credential**，
        // **不得伪造撤单/平仓成功**。
        //
        // 这里给出**传输层**的有界 deadline（第一道）；调用侧的每账户 deadline
        // 在 `kill_switch::drive_live_scope_once` 里（第二道，防的是"单个请求没超时
        // 但一个账户的整条链路慢到拖垮其它账户"）。
        let timeouts = http_timeouts_from_env();
        let http_builder = apply_http_deadlines(
            reqwest::Client::builder().default_headers(headers),
            timeouts,
        );
        #[cfg(test)]
        let http_builder =
            crate::k04_fake_venue::route_fixture_http(credential_id, &base_url, http_builder);
        let http = transport
            .apply_test_root(http_builder)
            .map_err(anyhow::Error::msg)?
            .build()
            .context("build reqwest client failed")?;

        let trust = transport.trust_profile();
        tracing::info!(
            credential_id,
            egress_scope = %egress_scope,
            upstream_venue = transport
                .venue
                .map(|value| value.to_string())
                .as_deref()
                .unwrap_or("unconfigured"),
            transport = transport.mode.as_str(),
            built_in_roots_preserved = trust.built_in_roots_preserved,
            added_test_roots = trust.added_test_roots,
            "binance REST transport configured"
        );

        Ok(Self {
            base_url,
            egress_scope,
            credential_id: credential_id.to_string(),
            request_limiter,
            order_count_limiter,
            exit_weight_reserve: exit_weight_reserve_from_env(),
            protectable_exit_budget_1m,
            exposure_cache: Arc::new(tokio::sync::Mutex::new(None)),
            http,
            keys,
            recv_window_ms,
            clock: Arc::new(SystemClock),
            cooldown_store,
            settlements: binance_response_settlement::for_client()?,
            #[cfg(test)]
            admission_exposure_error: None,
            #[cfg(test)]
            mock_http: None,
            #[cfg(test)]
            header_observer: None,
            #[cfg(test)]
            gate_wait_observer: None,
            #[cfg(test)]
            observation_audit: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub(crate) fn new_with_clock(
        keys: BinanceKeys,
        credential_id: &str,
        store: Store,
        clock: Arc<dyn Clock>,
    ) -> anyhow::Result<Self> {
        let mut client = Self::new_with_store(keys, credential_id, store)?;
        client.request_limiter = client.request_limiter.with_clock(clock.clone());
        client.clock = clock;
        Ok(client)
    }

    pub(crate) fn egress_scope(&self) -> &str {
        &self.egress_scope
    }

    pub(crate) fn protectable_exit_budget(&self) -> f64 {
        self.protectable_exit_budget_1m
    }

    pub(crate) async fn admission_open_positions(&self, store: &Store) -> anyhow::Result<u64> {
        #[cfg(test)]
        if let Some(error) = self.admission_exposure_error.as_deref() {
            return Err(anyhow!(error.to_string()));
        }
        open_positions_on_egress(store, &self.egress_scope).await
    }

    async fn cached_open_positions_on_egress(&self) -> u64 {
        let now_ms = self.clock.now_ms();
        let mut cache = self.exposure_cache.lock().await;
        if let Some(cached) = *cache {
            let age_ms = now_ms.saturating_sub(cached.checked_at_ms);
            if now_ms >= cached.checked_at_ms && age_ms < 1_000 {
                return cached.open_positions;
            }
        }

        let result = match self.cooldown_store.as_ref() {
            Some(store) => open_positions_on_egress(store, &self.egress_scope).await,
            None => Err(anyhow!("no exposure store attached to Binance client")),
        };
        match result {
            Ok(open_positions) => {
                *cache = Some(EgressExposureCache {
                    open_positions,
                    checked_at_ms: now_ms,
                });
                open_positions
            }
            Err(error) => {
                if let Some(cached) = *cache {
                    tracing::warn!(
                        egress_scope = %self.egress_scope,
                        %error,
                        cached_open_positions = cached.open_positions,
                        "egress exposure refresh failed; retaining the last known reserve"
                    );
                    cached.open_positions
                } else {
                    tracing::warn!(
                        egress_scope = %self.egress_scope,
                        %error,
                        reserve = self.protectable_exit_budget_1m,
                        "egress exposure has never been observed; reserving the full protectable budget"
                    );
                    u64::MAX
                }
            }
        }
    }

    async fn normal_exit_weight_reserve(&self) -> f64 {
        let open_positions = self.cached_open_positions_on_egress().await;
        let exposure_reserve = if open_positions == u64::MAX {
            self.protectable_exit_budget_1m
        } else {
            (open_positions as f64 * protectable_exit_weight_per_position())
                .min(self.protectable_exit_budget_1m)
        };
        self.exit_weight_reserve.max(exposure_reserve)
    }

    #[cfg(test)]
    fn new_without_cooldown_store(keys: BinanceKeys, credential_id: &str) -> anyhow::Result<Self> {
        Self::build(keys, credential_id, None)
    }

    async fn persisted_ban_until_ms(&self) -> anyhow::Result<i64> {
        // **sqlx-i64 核查批· 走文本取真值。**
        //
        // 旧 sqlx 0.7.4 `Any` + SQLite 会把 `ban_until_ms` 截成低 32 位；
        // 精确锁定的 0.8.6 已修复。若将来回归，截断后的值会恒远小于
        // `now_ms`，`active_ban_decision` 于是判 `Allow`，而 `Allow` 分支还会
        // `delete_expired_ban` 把这行**删掉**：一次重启就能让一条真实的
        // IP 封禁冷却**静默消失**，进程接着往被封的出口打请求。
        // 失败方向必须是"当封禁还在"，所以这里解析失败一律上抛。
        Ok(shared_egress_cooldown(
            &self.egress_scope,
            self.cooldown_store.as_ref(),
            self.clock.now_ms(),
        )
        .await?
        .ban_until_ms)
    }

    async fn persisted_rate_limit_until_ms(&self) -> anyhow::Result<i64> {
        let Some(store) = self.cooldown_store.as_ref() else {
            return Ok(0);
        };
        // See the SQLx Any empty-result invariant in `store.rs`; retain the
        // full-stream drain here as defense in depth.
        let row = sqlx::query(
            "SELECT CAST(cooldown_until_ms AS CHAR) AS cooldown_until_ms_text \
             FROM binance_rate_limit_cooldowns WHERE egress_scope = ?",
        )
        .bind(&self.egress_scope)
        .fetch_all(store.pool())
        .await
        .context("load persisted Binance rate-limit cooldown")?
        .into_iter()
        .next();
        match row {
            None => Ok(0),
            Some(row) => Ok(
                crate::store::try_get_i64_text(&row, "cooldown_until_ms_text")
                    .context("decode persisted Binance rate-limit cooldown")?,
            ),
        }
    }

    pub(crate) async fn active_429_cooldown_until_ms(&self) -> anyhow::Result<Option<i64>> {
        let now_ms = self.clock.now_ms();
        let memory_until_ms = rate_limit_registry()
            .lock()
            .map_err(|_| anyhow!("binance rate-limit registry lock poisoned"))?
            .get(&self.egress_scope)
            .copied()
            .unwrap_or_default();
        let persisted_until_ms = self.persisted_rate_limit_until_ms().await?;
        let until_ms = memory_until_ms.max(persisted_until_ms);
        Ok((until_ms > now_ms).then_some(until_ms))
    }

    async fn delete_expired_ban(&self, now_ms: i64) -> anyhow::Result<()> {
        if let Some(store) = self.cooldown_store.as_ref() {
            sqlx::query(
                "DELETE FROM binance_ip_ban_cooldowns WHERE egress_scope = ? AND ban_until_ms <= ?",
            )
            .bind(&self.egress_scope)
            .bind(now_ms)
            .execute(store.pool())
            .await
            .context("delete expired Binance IP-ban cooldown")?;
        }
        ip_ban_registry()
            .lock()
            .map_err(|_| anyhow!("binance IP-ban registry lock poisoned"))?
            .remove(&self.egress_scope);
        Ok(())
    }

    async fn delete_expired_rate_limit(&self, now_ms: i64) -> anyhow::Result<()> {
        if let Some(store) = self.cooldown_store.as_ref() {
            sqlx::query(
                "DELETE FROM binance_rate_limit_cooldowns \
                 WHERE egress_scope = ? AND cooldown_until_ms <= ?",
            )
            .bind(&self.egress_scope)
            .bind(now_ms)
            .execute(store.pool())
            .await
            .context("delete expired Binance rate-limit cooldown")?;
        }
        rate_limit_registry()
            .lock()
            .map_err(|_| anyhow!("binance rate-limit registry lock poisoned"))?
            .remove(&self.egress_scope);
        Ok(())
    }

    fn should_log_local_block(&self, cause: &str, now_ms: i64) -> bool {
        const LOG_INTERVAL_MS: i64 = 30_000;
        let key = format!("{}|{cause}", self.egress_scope);
        let Ok(mut registry) = blocked_log_registry().lock() else {
            return true;
        };
        let last_ms = registry.get(&key).copied().unwrap_or(i64::MIN / 2);
        if now_ms.saturating_sub(last_ms) < LOG_INTERVAL_MS {
            return false;
        }
        registry.insert(key, now_ms);
        true
    }

    fn record_meter_observations(&self, path: &str, response: &HttpAttemptResponse) {
        self.record_meter_observations_at(path, response, self.clock.now_ms());
    }

    #[allow(
        clippy::collapsible_if,
        reason = "Preserve the established meter-observation control flow."
    )]
    fn record_meter_observations_at(
        &self,
        path: &str,
        response: &HttpAttemptResponse,
        received_at_ms: i64,
    ) {
        if let Some(used_weight_1m) = response.used_weight_1m {
            self.request_limiter.observe_egress_weight(
                &self.egress_scope,
                used_weight_1m,
                received_at_ms,
            );
            #[cfg(test)]
            self.observation_audit
                .lock()
                .unwrap()
                .push(("weight", received_at_ms));
            self.request_limiter
                .record_budget_exceeded_without_429(&self.egress_scope, response.status);
            tracing::debug!(
                credential = %self.credential_id,
                egress_scope = %self.egress_scope,
                %path,
                used_weight_1m,
                "binance request-weight observation"
            );
            if self.should_log_local_block("meter_observation_weight", self.clock.now_ms()) {
                tracing::info!(
                    credential = %self.credential_id,
                    egress_scope = %self.egress_scope,
                    used_weight_1m,
                    "binance request-weight meter"
                );
            }
        }
        if response.order_count_1m.is_some() || response.order_count_10s.is_some() {
            if self.should_log_local_block("meter_observation_order_count", self.clock.now_ms()) {
                tracing::info!(
                    credential = %self.credential_id,
                    egress_scope = %self.egress_scope,
                    order_count_1m = ?response.order_count_1m,
                    order_count_10s = ?response.order_count_10s,
                    "binance order-count meters"
                );
            }
        }
    }

    fn exit_probe_failed_too_recently(&self, now_ms: i64) -> anyhow::Result<bool> {
        let registry = exit_probe_registry()
            .lock()
            .map_err(|_| anyhow!("binance exit-probe registry lock poisoned"))?;
        Ok(registry
            .get(&self.egress_scope)
            .and_then(|state| state.last_probe_failed_at_ms)
            .is_some_and(|failed_at| now_ms.saturating_sub(failed_at) < EXIT_PROBE_MIN_INTERVAL_MS))
    }

    fn record_exit_probe_response(&self, status: u16, now_ms: i64) -> anyhow::Result<()> {
        if status != 418 && status != 429 {
            return Ok(());
        }
        let mut registry = exit_probe_registry()
            .lock()
            .map_err(|_| anyhow!("binance exit-probe registry lock poisoned"))?;
        registry
            .entry(self.egress_scope.clone())
            .or_default()
            .last_probe_failed_at_ms = Some(now_ms);
        Ok(())
    }

    async fn persisted_probe_spent(&self) -> anyhow::Result<u32> {
        let Some(store) = self.cooldown_store.as_ref() else {
            return Ok(0);
        };
        let row = sqlx::query(
            "SELECT probes_spent FROM binance_exit_probe_claims WHERE egress_scope = ?",
        )
        .bind(&self.egress_scope)
        .fetch_optional(store.pool())
        .await
        .context("load persisted Binance exit-probe spend")?;
        Ok(row
            .map(|row| row.get::<i32, _>("probes_spent").max(0) as u32)
            .unwrap_or(0))
    }

    /// Atomically claim one of this window's N probes. With a Store, all
    /// decisions are SQL CAS decisions; process memory is only a fast cache and
    /// the no-Store fallback.
    async fn claim_exit_probe(&self, window_until_ms: i64, now_ms: i64) -> anyhow::Result<bool> {
        if self.exit_probe_failed_too_recently(now_ms)? {
            return Ok(false);
        }
        let budget = configured_exit_probes_per_window();
        let Some(store) = self.cooldown_store.as_ref() else {
            let mut registry = exit_probe_registry()
                .lock()
                .map_err(|_| anyhow!("binance exit-probe registry lock poisoned"))?;
            let state = registry.entry(self.egress_scope.clone()).or_default();
            if state.window_until_ms != window_until_ms {
                state.window_until_ms = window_until_ms;
                state.probes_spent = 0;
            }
            if state.probes_spent >= budget {
                return Ok(false);
            }
            state.probes_spent += 1;
            return Ok(true);
        };

        let increment = sqlx::query(
            "UPDATE binance_exit_probe_claims \
             SET probes_spent = probes_spent + 1, claimed_at_ms = ? \
             WHERE egress_scope = ? AND window_until_ms = ? AND probes_spent < ?",
        )
        .bind(now_ms)
        .bind(&self.egress_scope)
        .bind(window_until_ms)
        .bind(i64::from(budget))
        .execute(store.pool())
        .await
        .context("increment Binance exit-probe budget")?;
        let granted = if increment.rows_affected() == 1 {
            true
        } else {
            let reset = sqlx::query(
                "UPDATE binance_exit_probe_claims \
                 SET window_until_ms = ?, probes_spent = 1, claimed_at_ms = ? \
                 WHERE egress_scope = ? AND window_until_ms < ?",
            )
            .bind(window_until_ms)
            .bind(now_ms)
            .bind(&self.egress_scope)
            .bind(window_until_ms)
            .execute(store.pool())
            .await
            .context("reset Binance exit-probe budget for a new window")?;
            if reset.rows_affected() == 1 {
                true
            } else {
                let insert_sql = match store.kind() {
                    DbKind::MySql => {
                        "INSERT IGNORE INTO binance_exit_probe_claims \
                         (egress_scope, window_until_ms, claimed_at_ms, probes_spent) \
                         VALUES (?, ?, ?, 1)"
                    }
                    _ => {
                        "INSERT INTO binance_exit_probe_claims \
                         (egress_scope, window_until_ms, claimed_at_ms, probes_spent) \
                         VALUES (?, ?, ?, 1) ON CONFLICT(egress_scope) DO NOTHING"
                    }
                };
                let inserted = sqlx::query(insert_sql)
                    .bind(&self.egress_scope)
                    .bind(window_until_ms)
                    .bind(now_ms)
                    .execute(store.pool())
                    .await
                    .context("insert first Binance exit-probe budget row")?;
                if inserted.rows_affected() == 1 {
                    true
                } else {
                    sqlx::query(
                        "UPDATE binance_exit_probe_claims \
                         SET probes_spent = probes_spent + 1, claimed_at_ms = ? \
                         WHERE egress_scope = ? AND window_until_ms = ? AND probes_spent < ?",
                    )
                    .bind(now_ms)
                    .bind(&self.egress_scope)
                    .bind(window_until_ms)
                    .bind(i64::from(budget))
                    .execute(store.pool())
                    .await
                    .context("retry Binance exit-probe CAS after insert conflict")?
                    .rows_affected()
                        == 1
                }
            }
        };

        let probes_spent = self.persisted_probe_spent().await?;
        let mut registry = exit_probe_registry()
            .lock()
            .map_err(|_| anyhow!("binance exit-probe registry lock poisoned"))?;
        let state = registry.entry(self.egress_scope.clone()).or_default();
        state.window_until_ms = window_until_ms;
        state.probes_spent = probes_spent;
        Ok(granted)
    }

    async fn ensure_not_ip_banned(
        &self,
        endpoint: &str,
        scope: RetryScope,
        phase: EgressGuardPhase,
    ) -> anyhow::Result<()> {
        let now_ms = self.clock.now_ms();
        let memory_until_ms = ip_ban_registry()
            .lock()
            .map_err(|_| anyhow!("binance IP-ban registry lock poisoned"))?
            .get(&self.egress_scope)
            .copied()
            .unwrap_or_default();
        let persisted_until_ms = self.persisted_ban_until_ms().await?;
        let ban_until_ms = memory_until_ms.max(persisted_until_ms);
        match active_ban_decision(now_ms, ban_until_ms) {
            ActiveBanDecision::Allow => {
                if ban_until_ms > 0 {
                    self.delete_expired_ban(now_ms).await?;
                }
                Ok(())
            }
            ActiveBanDecision::Reject { remaining_ms } => {
                if self.should_log_local_block("ip_ban", now_ms) {
                    tracing::warn!(
                        credential = %self.credential_id,
                        egress_scope = %self.egress_scope,
                        %endpoint,
                        error_code = "BINANCE_EGRESS_COOLDOWN_ACTIVE",
                        cooldown_cause = "upstream_418_ip_ban",
                        remaining_ms,
                        "binance request stopped locally by egress cooldown"
                    );
                }
                Err(anyhow!(
                    "binance IP ban cooldown active for egress_scope {} credential {} endpoint {} until_ms={}",
                    self.egress_scope,
                    self.credential_id,
                    endpoint,
                    ban_until_ms
                ))
            }
        }?;

        let memory_until_ms = rate_limit_registry()
            .lock()
            .map_err(|_| anyhow!("binance rate-limit registry lock poisoned"))?
            .get(&self.egress_scope)
            .copied()
            .unwrap_or_default();
        let persisted_until_ms = self.persisted_rate_limit_until_ms().await?;
        let cooldown_until_ms = memory_until_ms.max(persisted_until_ms);
        match active_ban_decision(now_ms, cooldown_until_ms) {
            ActiveBanDecision::Allow => {
                if cooldown_until_ms > 0 {
                    self.delete_expired_rate_limit(now_ms).await?;
                }
                Ok(())
            }
            ActiveBanDecision::Reject { remaining_ms } => {
                // A 429 is a rate-limit park, not a ban: the exchange still
                // answers. Refusing every scope here would mean no cancel, no
                // reduce-only flatten and no SL/TP arming for the whole window,
                // which is a worse outcome than the park — and it contradicts
                // the invariant this engine registered for itself
                // (只拦开仓不拦平仓/保护). Exits therefore get a bounded
                // carve-out; entries and routine polling do not. A 418 is a
                // real ban and is never carved out — see the branch above.
                if scope == RetryScope::Exit {
                    match phase {
                        EgressGuardPhase::Fast => return Ok(()),
                        EgressGuardPhase::Authoritative => {
                            if self.exit_probe_failed_too_recently(now_ms)? {
                                return Err(anyhow!(
                                    "binance exit request stopped locally: prior exit probe failed less than {}ms ago for egress_scope {} credential {} endpoint {}",
                                    EXIT_PROBE_MIN_INTERVAL_MS,
                                    self.egress_scope,
                                    self.credential_id,
                                    endpoint,
                                ));
                            }
                            if self.claim_exit_probe(cooldown_until_ms, now_ms).await? {
                                tracing::warn!(
                                    credential = %self.credential_id,
                                    egress_scope = %self.egress_scope,
                                    %endpoint,
                                    error_code = "BINANCE_EGRESS_EXIT_PROBE_ADMITTED",
                                    cooldown_cause = "upstream_429_rate_limit",
                                    remaining_ms,
                                    "exit request admitted through 429 egress cooldown on this window's probe"
                                );
                                return Ok(());
                            }
                            let probes_spent = if self.cooldown_store.is_some() {
                                self.persisted_probe_spent().await?
                            } else {
                                exit_probe_registry()
                                    .lock()
                                    .map_err(|_| {
                                        anyhow!("binance exit-probe registry lock poisoned")
                                    })?
                                    .get(&self.egress_scope)
                                    .map(|state| state.probes_spent)
                                    .unwrap_or(0)
                            };
                            tracing::error!(
                                credential = %self.credential_id,
                                egress_scope = %self.egress_scope,
                                %endpoint,
                                window_until = cooldown_until_ms,
                                probes_spent,
                                error_code = "BINANCE_EGRESS_EXIT_BUDGET_EXHAUSTED",
                                "exit request stopped locally because the 429-window budget is exhausted"
                            );
                            return Err(anyhow!(
                                "binance exit request stopped locally: 429 egress cooldown probe budget exhausted for egress_scope {} credential {} endpoint {} until_ms={} probes_spent={}",
                                self.egress_scope,
                                self.credential_id,
                                endpoint,
                                cooldown_until_ms,
                                probes_spent,
                            ));
                        }
                    }
                }
                if self.should_log_local_block("rate_limit", now_ms) {
                    tracing::warn!(
                        credential = %self.credential_id,
                        egress_scope = %self.egress_scope,
                        %endpoint,
                        error_code = "BINANCE_EGRESS_COOLDOWN_ACTIVE",
                        cooldown_cause = "upstream_429_rate_limit",
                        remaining_ms,
                        "binance request stopped locally by egress cooldown"
                    );
                }
                Err(anyhow!(
                    "binance rate-limit cooldown active for egress_scope {} credential {} endpoint {} until_ms={}",
                    self.egress_scope,
                    self.credential_id,
                    endpoint,
                    cooldown_until_ms
                ))
            }
        }
    }

    async fn impose_rate_limit_cooldown(
        &self,
        endpoint: &str,
        cooldown_ms: u64,
    ) -> anyhow::Result<()> {
        let now_ms = self.clock.now_ms();
        let until_ms = now_ms.saturating_add(i64::try_from(cooldown_ms).unwrap_or(i64::MAX));
        {
            let mut registry = rate_limit_registry()
                .lock()
                .map_err(|_| anyhow!("binance rate-limit registry lock poisoned"))?;
            let memory_until_ms = registry.entry(self.egress_scope.clone()).or_default();
            *memory_until_ms = (*memory_until_ms).max(until_ms);
        }
        if let Some(store) = self.cooldown_store.as_ref() {
            persist_cooldown(
                store,
                CooldownTable::RateLimit,
                &self.egress_scope,
                until_ms,
                now_ms,
            )
            .await?;
        }
        tracing::warn!(
            credential = %self.credential_id,
            egress_scope = %self.egress_scope,
            %endpoint,
            upstream_status = 429_u16,
            cooldown_ms,
            "binance egress-wide rate-limit cooldown engaged"
        );
        Ok(())
    }

    async fn impose_ip_ban(
        &self,
        endpoint: &str,
        cooldown_ms: u64,
        retry_after: Option<&str>,
    ) -> anyhow::Result<()> {
        let now_ms = self.clock.now_ms();
        let until_ms = now_ms.saturating_add(i64::try_from(cooldown_ms).unwrap_or(i64::MAX));
        {
            let mut registry = ip_ban_registry()
                .lock()
                .map_err(|_| anyhow!("binance IP-ban registry lock poisoned"))?;
            let memory_until_ms = registry.entry(self.egress_scope.clone()).or_default();
            *memory_until_ms = (*memory_until_ms).max(until_ms);
        }
        if let Some(store) = self.cooldown_store.as_ref() {
            persist_cooldown(
                store,
                CooldownTable::IpBan,
                &self.egress_scope,
                until_ms,
                now_ms,
            )
            .await?;
        }
        tracing::error!(
            credential = %self.credential_id,
            egress_scope = %self.egress_scope,
            %endpoint,
            http_status = 418_u16,
            retry_after = %retry_after_log_value(retry_after),
            retry_number = 0_u32,
            next_wait_ms = cooldown_ms,
            request_class = "ip_ban",
            "binance IP ban received; automatic retry disabled and hard cooldown engaged"
        );
        Ok(())
    }

    fn retry_input(
        &self,
        response: &HttpAttemptResponse,
        class: RetryClass,
        scope: RetryScope,
        completed_retries: u32,
        started_at_ms: i64,
        endpoint: &str,
    ) -> RetryInput {
        let elapsed_ms = self.clock.now_ms().saturating_sub(started_at_ms).max(0) as u64;
        RetryInput {
            status: response.status,
            class,
            scope,
            completed_retries,
            elapsed_ms,
            retry_after_ms: {
                let parsed =
                    parse_retry_after_ms(response.retry_after.as_deref(), self.clock.now_ms());
                if response.status == 429 {
                    Some(rate_limit_cooldown_ms(
                        response.retry_after.as_deref(),
                        self.clock.now_ms(),
                    ))
                } else {
                    parsed
                }
            },
            jitter_ms: deterministic_jitter_ms(
                &format!("{}|{endpoint}", self.credential_id),
                completed_retries + 1,
            ),
        }
    }

    fn record_retry(
        &self,
        endpoint: &str,
        response: &HttpAttemptResponse,
        class: RetryClass,
        scope: RetryScope,
        retry_number: u32,
        next_wait_ms: u64,
        decision: &'static str,
    ) {
        tracing::warn!(
            credential = %self.credential_id,
            %endpoint,
            http_status = response.status,
            retry_after = %retry_after_log_value(response.retry_after.as_deref()),
            retry_number,
            next_wait_ms,
            request_class = class.as_str(),
            retry_scope = scope.as_str(),
            %decision,
            "binance rate-limit disposition"
        );
        #[cfg(test)]
        if let Some(mock) = self.mock_http.as_ref() {
            mock.retry_records
                .lock()
                .expect("lock retry record log")
                .push(RetryRecord {
                    credential_id: self.credential_id.clone(),
                    endpoint: endpoint.to_string(),
                    status: response.status,
                    retry_after: response.retry_after.clone(),
                    retry_number,
                    next_wait_ms,
                    request_class: class.as_str(),
                    scope: scope.as_str(),
                    decision,
                });
        }
    }

    fn fresh_signed_url(&self, path: &str, params: &[(String, String)]) -> anyhow::Result<String> {
        let mut params = params.to_vec();
        if let Some((_, timestamp)) = params
            .iter_mut()
            .find(|(name, _)| name.as_str() == "timestamp")
        {
            *timestamp = self.clock.now_ms().max(0).to_string();
        }
        let (query, sig) = signed_query_hmac(self.keys.api_secret.as_bytes(), &params)?;
        Ok(format!(
            "{}{}?{}&signature={}",
            self.base_url, path, query, sig
        ))
    }

    fn transport_gate(&self) -> anyhow::Result<Arc<tokio::sync::Mutex<()>>> {
        Ok(transport_gate_for_scope(&self.egress_scope))
    }

    fn log_upstream_cooldown(&self, endpoint: &str, response: &HttpAttemptResponse) {
        if matches!(response.status, 418 | 429) {
            // Failed persistence must not erase the received headers. Body is
            // optional and stays empty on signed settlement failure; when it
            // is available only the numeric code is allowed into this log.
            #[derive(Deserialize)]
            struct ErrorCode {
                code: i32,
            }
            let upstream_error_code = serde_json::from_str::<ErrorCode>(&response.body)
                .ok()
                .map(|error| error.code);
            tracing::warn!(
                credential = %self.credential_id,
                egress_scope = %self.egress_scope,
                %endpoint,
                http_status = response.status,
                retry_after = %retry_after_log_value(response.retry_after.as_deref()),
                retry_after_ms = parse_retry_after_ms(
                    response.retry_after.as_deref(), self.clock.now_ms()),
                upstream_error_code,
                "binance upstream rate-limit response"
            );
        }
    }

    async fn record_upstream_cooldown(
        &self,
        endpoint: &str,
        response: &HttpAttemptResponse,
    ) -> anyhow::Result<()> {
        self.log_upstream_cooldown(endpoint, response);
        match response.status {
            418 => {
                let cooldown_ms =
                    parse_retry_after_ms(response.retry_after.as_deref(), self.clock.now_ms())
                        .unwrap_or_default()
                        .max(crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS);
                self.impose_ip_ban(endpoint, cooldown_ms, response.retry_after.as_deref())
                    .await
            }
            429 => {
                // Binance normally supplies Retry-After. If an intermediary
                // strips it, two seconds is the minimum quiet period; the
                // request-level exponential retry remains bounded separately.
                let cooldown_ms =
                    rate_limit_cooldown_ms(response.retry_after.as_deref(), self.clock.now_ms());
                self.impose_rate_limit_cooldown(endpoint, cooldown_ms).await
            }
            _ => Ok(()),
        }
    }

    /// Synchronous publication at the header observation point. The returned
    /// fixed deadline is the fact transferred to the runtime-owned writer.
    fn publish_response_safety(
        &self,
        endpoint: &str,
        response: &HttpAttemptResponse,
        now_ms: i64,
        scope: RetryScope,
    ) -> anyhow::Result<(CooldownFact, u64)> {
        self.record_meter_observations_at(endpoint, response, now_ms);
        self.record_order_count_observations_at(response, now_ms);
        if scope == RetryScope::Exit {
            self.record_exit_probe_response(response.status, now_ms)?;
        }
        let (table, cooldown_ms) = if response.status == 418 {
            (
                CooldownTable::IpBan,
                parse_retry_after_ms(response.retry_after.as_deref(), now_ms)
                    .unwrap_or_default()
                    .max(crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS),
            )
        } else {
            (
                CooldownTable::RateLimit,
                rate_limit_cooldown_ms(response.retry_after.as_deref(), now_ms),
            )
        };
        let until_ms = now_ms.saturating_add(i64::try_from(cooldown_ms).unwrap_or(i64::MAX));
        {
            let mut registry = table
                .registry()
                .lock()
                .map_err(|_| anyhow!("Binance cooldown registry lock poisoned"))?;
            let memory = registry.entry(self.egress_scope.clone()).or_default();
            *memory = (*memory).max(until_ms);
        }
        Ok((
            CooldownFact {
                scope: self.egress_scope.clone(),
                table,
                until_ms,
                received_at_ms: now_ms,
            },
            cooldown_ms,
        ))
    }

    fn record_order_count_observations_at(&self, response: &HttpAttemptResponse, now_ms: i64) {
        self.order_count_limiter.observe_headers(
            &self.credential_id,
            response.order_count_1m,
            response.order_count_10s,
            now_ms,
        );
        #[cfg(test)]
        if response.order_count_1m.is_some() || response.order_count_10s.is_some() {
            self.observation_audit
                .lock()
                .unwrap()
                .push(("orders", now_ms));
        }
    }

    fn log_cooldown_committed(
        &self,
        endpoint: &str,
        response: &HttpAttemptResponse,
        cooldown_ms: u64,
    ) {
        if response.status == 418 {
            tracing::error!(credential = %self.credential_id, egress_scope = %self.egress_scope, %endpoint,
                http_status = 418_u16, retry_after = %retry_after_log_value(response.retry_after.as_deref()),
                retry_number = 0_u32, next_wait_ms = cooldown_ms,
                "binance IP ban received; automatic retry disabled and hard cooldown engaged");
        } else {
            tracing::warn!(credential = %self.credential_id, egress_scope = %self.egress_scope, %endpoint,
                upstream_status = 429_u16, cooldown_ms, "binance egress-wide rate-limit cooldown engaged");
        }
    }

    async fn send_signed_once(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        scope: RetryScope,
        request_class: RequestClass,
    ) -> anyhow::Result<HttpAttemptResponse> {
        self.send_signed_once_authorized(
            method,
            path,
            params,
            scope,
            request_class,
            &mut SignedAuthority::Ordinary,
        )
        .await
    }

    async fn send_signed_once_authorized(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        scope: RetryScope,
        request_class: RequestClass,
        authority: &mut SignedAuthority<'_>,
    ) -> anyhow::Result<HttpAttemptResponse> {
        let mut timing = crate::reconcile_trace::Timer::http(method, path, request_class);
        let result = self
            .send_signed_once_timed(
                method,
                path,
                params,
                scope,
                request_class,
                &mut timing,
                authority,
            )
            .await;
        if let Ok(response) = &result {
            timing.http_status(response.status);
        }
        timing.finish(result.is_ok());
        result
    }

    async fn send_signed_once_timed(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        scope: RetryScope,
        request_class: RequestClass,
        timing: &mut crate::reconcile_trace::Timer,
        authority: &mut SignedAuthority<'_>,
    ) -> anyhow::Result<HttpAttemptResponse> {
        let is_order_post = method == "POST" && ORDER_ENDPOINTS.contains(&path);
        self.ensure_not_ip_banned(path, scope, EgressGuardPhase::Fast)
            .await?;
        let weight = signed_request_weight(method, path, params);
        // Order POSTs cost 0 on the IP table (official weight); they are governed by
        // the per-credential ORDER COUNT windows below instead of the IP bucket.
        let skip_ip_bucket = weight == 0.0 && is_order_post;
        if matches!(
            request_class,
            RequestClass::EventTruth | RequestClass::PeriodicReconcile | RequestClass::Entry
        ) {
            let reserve = if skip_ip_bucket {
                0.0
            } else {
                self.normal_exit_weight_reserve().await
            };
            match self
                .request_limiter
                .acquire_prioritized(
                    &self.egress_scope,
                    &self.credential_id,
                    weight,
                    request_class,
                    reserve,
                )
                .await
            {
                Admission::Admitted => {}
                Admission::TimedOut => {
                    return Err(anyhow!(
                        "binance egress queue timed out: credential={} scope={} endpoint={} class={}",
                        self.credential_id,
                        self.egress_scope,
                        path,
                        request_class.as_str()
                    ));
                }
                Admission::HardRejected => {
                    return Err(anyhow!(
                        "EGRESS_WEIGHT_BUDGET_EXHAUSTED credential={} scope={} endpoint={} class={}",
                        self.credential_id,
                        self.egress_scope,
                        path,
                        request_class.as_str()
                    ));
                }
                Admission::ReserveExhausted => unreachable!("normal requests queue"),
            }
        }
        timing.stage(crate::reconcile_trace::Stage::TransportGate);
        let gate = self.transport_gate()?;
        #[cfg(test)]
        if let Some(observe) = &self.gate_wait_observer {
            observe();
        }
        let transport_guard = gate.lock_owned().await;
        let flight = self.settlements.flight()?;
        let mut closing = self.settlements.closing_receiver();
        let before_headers = async {
            timing.stage(crate::reconcile_trace::Stage::Admission);
            // Exit owns the reserved slice, but claiming a 429-window probe is a
            // scarcer authority decision than taking a local token. Charge after
            // acquiring the transport gate and before the authoritative cooldown
            // read so reserve exhaustion cannot consume a probe without sending.
            if matches!(request_class, RequestClass::Exit | RequestClass::Protection)
                && !skip_ip_bucket
                && self
                    .request_limiter
                    .acquire_prioritized(
                        &self.egress_scope,
                        &self.credential_id,
                        weight,
                        request_class,
                        0.0,
                    )
                    .await
                    != Admission::Admitted
            {
                return Err(anyhow!(
                "binance exit request stopped locally: egress reserve exhausted for scope {} endpoint {}",
                self.egress_scope,
                path
            ));
            }
            // Another request may have received 429/418 while this one waited for
            // tokens or the single-flight transport gate.
            if let Err(error) = self
                .ensure_not_ip_banned(path, scope, EgressGuardPhase::Authoritative)
                .await
            {
                if !skip_ip_bucket {
                    self.request_limiter
                        .refund_prioritized(&self.egress_scope, weight)
                        .await;
                }
                return Err(error);
            }
            if is_order_post && request_class == RequestClass::Entry {
                if let OrderAdmission::Parked { until_ms, window } = self
                    .order_count_limiter
                    .admit_entry(&self.credential_id, self.clock.now_ms())
                {
                    return Err(anyhow!(
                    "ORDER_COUNT_BUDGET_EXHAUSTED credential={} endpoint={} window={} until_ms={}",
                    self.credential_id,
                    path,
                    window,
                    until_ms
                ));
                }
            }
            // Acquire AFTER the egress queue, and re-read on EVERY retry. The
            // guard lives until send returns headers (or failure/cancellation),
            // but no SQL transaction spans the HTTP request. It is dropped before
            // response safety settlement can need the single SQLite connection.
            let _lifecycle_send = if is_order_post && request_class == RequestClass::Entry {
                Some(
                    crate::lifecycle_fence::authorize_send(
                        self.cooldown_store.as_ref(),
                        &self.credential_id,
                    )
                    .await?,
                )
            } else {
                None
            };
            let url = self.fresh_signed_url(path, params)?;
            authority
                .authorize(&self.credential_id, method, path, params)
                .await?;
            crate::svc_v2::reconcile_budget::ensure_live()?;
            timing.stage(crate::reconcile_trace::Stage::Http);
            #[cfg(test)]
            if let Some(mock) = self.mock_http.as_ref() {
                mock.requests
                    .lock()
                    .expect("lock in-memory HTTP request log")
                    .push(format!("{method} {url}"));
                if let Some(hook) = mock.on_request.clone() {
                    hook(path.to_string()).await;
                }
                let order_count_headers = mock.order_count_headers;
                let response = mock
                    .responses
                    .lock()
                    .expect("lock in-memory HTTP responses")
                    .pop_front()
                    .map(|response| HttpAttemptResponse {
                        status: response.status,
                        retry_after: response.retry_after,
                        used_weight_1m: None,
                        order_count_1m: order_count_headers.and_then(|value| value.0),
                        order_count_10s: order_count_headers.and_then(|value| value.1),
                        body: response.body,
                    })
                    .ok_or_else(|| anyhow!("in-memory HTTP response queue exhausted"))?;
                return Ok(HttpReply::Mock(response));
            }

            let request = match method {
                "GET" => self.http.get(url),
                "POST" => self.http.post(url),
                "DELETE" => self.http.delete(url),
                other => return Err(anyhow!("unsupported Binance HTTP method {other}")),
            };
            let response = request.send().await.with_context(|| {
                format!("binance signed {} failed", method.to_ascii_lowercase())
            })?;
            Ok(HttpReply::Network(response))
        };
        // Closing may cancel admission/authorization/transport before headers;
        // an already accepted flight can hand off a known fact while Closing.
        let reply = tokio::select! {
            result = before_headers => result?,
            _ = closing.wait_for(|closed| *closed) => return Err(anyhow!("BINANCE_SETTLEMENTS_CLOSING")),
        };
        let received_at = tokio::time::Instant::now();
        let received_at_ms = self.clock.now_ms();
        let mut response = reply.headers();
        timing.http_status(response.status);
        #[cfg(test)]
        if let Some(observe) = &self.header_observer {
            observe(response.status);
        }
        if matches!(response.status, 418 | 429) {
            timing.stage(crate::reconcile_trace::Stage::Postprocess);
            let (fact, cooldown_ms) =
                self.publish_response_safety(path, &response, received_at_ms, scope)?;
            let table = fact.table;
            // No await between observing headers, local publication and this
            // registration. Neither caller cancellation nor body owns the write.
            let settlement = self.settlements.settle(
                flight,
                fact,
                received_at,
                self.cooldown_store.clone(),
                reply,
                transport_guard,
                timing.correlation(),
            );
            let settled = settlement
                .await
                .map_err(|_| anyhow!("BINANCE_SETTLEMENT_OWNER_LOST"))?;
            let success = matches!(
                settled.outcome,
                SettlementOutcome::Durable | SettlementOutcome::LocalOnly
            );
            if !success {
                // A failed/unknown safety write must release the response and
                // report its outcome without waiting for optional error text.
                drop(settled.payload);
                self.log_upstream_cooldown(path, &response);
                return Err(anyhow!(
                    "persist Binance cooldown into {}: safety settlement {:?}",
                    table.table(),
                    settled.outcome
                ));
            }
            self.log_cooldown_committed(path, &response, cooldown_ms);
            // Successful settlement retains the existing optional diagnostics.
            // Neither persistence nor the gate depends on this body await.
            response.body = settled.payload.body().await;
            self.log_upstream_cooldown(path, &response);
            return Ok(response);
        }
        drop(flight);
        response.body = reply.body().await;
        timing.stage(crate::reconcile_trace::Stage::Postprocess);
        self.record_meter_observations(path, &response);
        self.record_order_count_observations_at(&response, self.clock.now_ms());
        if scope == RetryScope::Exit {
            self.record_exit_probe_response(response.status, self.clock.now_ms())?;
        }
        drop(transport_guard);
        Ok(response)
    }

    async fn send_unsigned_once(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        request_class: RequestClass,
    ) -> anyhow::Result<HttpAttemptResponse> {
        self.send_unsigned_once_with_deadline(method, path, params, request_class, None, || {})
            .await
    }

    async fn send_unsigned_once_with_deadline<F: FnOnce()>(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        request_class: RequestClass,
        deadline: Option<tokio::time::Instant>,
        on_dispatch: F,
    ) -> anyhow::Result<HttpAttemptResponse> {
        let mut timing = crate::reconcile_trace::Timer::http(method, path, request_class);
        let result = self
            .send_unsigned_once_timed(
                method,
                path,
                params,
                request_class,
                &mut timing,
                deadline,
                on_dispatch,
            )
            .await;
        if let Ok(response) = &result {
            timing.http_status(response.status);
        }
        timing.finish(result.is_ok());
        result
    }

    async fn send_unsigned_once_timed<F: FnOnce()>(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        request_class: RequestClass,
        timing: &mut crate::reconcile_trace::Timer,
        deadline: Option<tokio::time::Instant>,
        on_dispatch: F,
    ) -> anyhow::Result<HttpAttemptResponse> {
        // listenKey maintenance is infrastructure, never an exit primitive, so
        // it takes no part in the Exit carve-out.
        unsigned_step_before_deadline(
            deadline,
            self.ensure_not_ip_banned(path, RetryScope::Normal, EgressGuardPhase::Fast),
        )
        .await??;
        // Keep the source shape used by the request-class regression assertion.
        #[rustfmt::skip]
        let reserve = unsigned_step_before_deadline(deadline, self.normal_exit_weight_reserve()).await?;
        match unsigned_step_before_deadline(
            deadline,
            self.request_limiter.acquire_prioritized(
                &self.egress_scope,
                &self.credential_id,
                1.0,
                request_class,
                reserve,
            ),
        )
        .await?
        {
            Admission::Admitted => {}
            Admission::TimedOut => {
                return Err(anyhow!(
                    "binance egress queue timed out: credential={} scope={} endpoint={} class={}",
                    self.credential_id,
                    self.egress_scope,
                    path,
                    request_class.as_str()
                ));
            }
            Admission::HardRejected => {
                return Err(anyhow!(
                    "EGRESS_WEIGHT_BUDGET_EXHAUSTED credential={} scope={} endpoint={} class={}",
                    self.credential_id,
                    self.egress_scope,
                    path,
                    request_class.as_str()
                ));
            }
            Admission::ReserveExhausted => unreachable!("normal requests queue"),
        }
        timing.stage(crate::reconcile_trace::Stage::TransportGate);
        let gate = self.transport_gate()?;
        #[cfg(test)]
        if let Some(observe) = &self.gate_wait_observer {
            observe();
        }
        let transport_guard = unsigned_step_before_deadline(deadline, gate.lock_owned()).await?;
        let flight = self.settlements.flight()?;
        let mut closing = self.settlements.closing_receiver();
        let before_headers = async {
            timing.stage(crate::reconcile_trace::Stage::Admission);
            if let Err(error) = unsigned_step_before_deadline(
                deadline,
                self.ensure_not_ip_banned(
                    path,
                    RetryScope::Normal,
                    EgressGuardPhase::Authoritative,
                ),
            )
            .await?
            {
                self.request_limiter
                    .refund_prioritized(&self.egress_scope, 1.0)
                    .await;
                return Err(error);
            }
            let query = serde_urlencoded::to_string(params).context("encode query failed")?;
            let url = if query.is_empty() {
                format!("{}{}", self.base_url, path)
            } else {
                format!("{}{}?{}", self.base_url, path, query)
            };
            crate::svc_v2::reconcile_budget::ensure_live()?;
            timing.stage(crate::reconcile_trace::Stage::Http);
            #[cfg(test)]
            if let Some(mock) = self.mock_http.as_ref() {
                on_dispatch();
                mock.requests
                    .lock()
                    .expect("lock in-memory HTTP request log")
                    .push(format!("{method} {url}"));
                if let Some(hook) = mock.on_request.clone() {
                    unsigned_step_before_deadline(deadline, hook(path.to_string())).await?;
                }
                let response = mock
                    .responses
                    .lock()
                    .expect("lock in-memory HTTP responses")
                    .pop_front()
                    .map(|response| HttpAttemptResponse {
                        status: response.status,
                        retry_after: response.retry_after,
                        used_weight_1m: None,
                        order_count_1m: None,
                        order_count_10s: None,
                        body: response.body,
                    })
                    .ok_or_else(|| anyhow!("in-memory HTTP response queue exhausted"))?;
                return Ok(HttpReply::Mock(response));
            }
            let request = match method {
                "GET" => self.http.get(url),
                "POST" => self.http.post(url),
                "PUT" => self.http.put(url),
                other => return Err(anyhow!("unsupported Binance HTTP method {other}")),
            };
            on_dispatch();
            let response = unsigned_step_before_deadline(deadline, request.send())
                .await?
                .with_context(|| {
                    format!("binance unsigned {} failed", method.to_ascii_lowercase())
                })?;
            Ok(HttpReply::Network(response))
        };
        let reply = tokio::select! {
            result = before_headers => result?,
            _ = closing.wait_for(|closed| *closed) => return Err(anyhow!("BINANCE_SETTLEMENTS_CLOSING")),
        };
        let received_at = tokio::time::Instant::now();
        let received_at_ms = self.clock.now_ms();
        let mut response = reply.headers();
        timing.http_status(response.status);
        #[cfg(test)]
        if let Some(observe) = &self.header_observer {
            observe(response.status);
        }
        if matches!(response.status, 418 | 429) {
            timing.stage(crate::reconcile_trace::Stage::Postprocess);
            let (fact, cooldown_ms) =
                self.publish_response_safety(path, &response, received_at_ms, RetryScope::Normal)?;
            let table = fact.table;
            // The same owner as signed responses accepts the fact synchronously
            // before any await, and owns this exact response and transport gate.
            let settlement = self.settlements.settle(
                flight,
                fact,
                received_at,
                self.cooldown_store.clone(),
                reply,
                transport_guard,
                timing.correlation(),
            );
            let settled = settlement
                .await
                .map_err(|_| anyhow!("BINANCE_SETTLEMENT_OWNER_LOST"))?;
            if !matches!(
                settled.outcome,
                SettlementOutcome::Durable | SettlementOutcome::LocalOnly
            ) {
                drop(settled.payload);
                self.log_upstream_cooldown(path, &response);
                return Err(anyhow!(
                    "persist Binance cooldown into {}: safety settlement {:?}",
                    table.table(),
                    settled.outcome,
                ));
            }
            self.log_cooldown_committed(path, &response, cooldown_ms);
            response.body = unsigned_step_before_deadline(deadline, settled.payload.body())
                .await
                .unwrap_or_default();
            self.log_upstream_cooldown(path, &response);
            return Ok(response);
        }
        drop(flight);
        response.body = unsigned_step_before_deadline(deadline, reply.body()).await?;
        timing.stage(crate::reconcile_trace::Stage::Postprocess);
        self.record_meter_observations(path, &response);
        self.record_upstream_cooldown(path, &response).await?;
        drop(transport_guard);
        Ok(response)
    }

    async fn unsigned_request_with_retry(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        class: RetryClass,
        request_class: RequestClass,
    ) -> anyhow::Result<HttpAttemptResponse> {
        let started_at_ms = self.clock.now_ms();
        let mut completed_retries = 0_u32;
        loop {
            let response = self
                .send_unsigned_once(method, path, params, request_class)
                .await?;
            if (200..300).contains(&response.status) {
                return Ok(response);
            }
            if response.status != 418 && response.status != 429 {
                return Err(http_error(&response));
            }
            match retry_decision(self.retry_input(
                &response,
                class,
                RetryScope::Normal,
                completed_retries,
                started_at_ms,
                path,
            )) {
                RetryDecision::Retry {
                    after_ms,
                    retry_number,
                } => {
                    self.record_retry(
                        path,
                        &response,
                        class,
                        RetryScope::Normal,
                        retry_number,
                        after_ms,
                        "retry",
                    );
                    self.clock.sleep(Duration::from_millis(after_ms)).await;
                    completed_retries = retry_number;
                }
                RetryDecision::VerifyTruth { .. } => {
                    self.record_retry(
                        path,
                        &response,
                        class,
                        RetryScope::Normal,
                        0,
                        0,
                        "truth_lookup_unavailable",
                    );
                    return Err(anyhow!(
                        "binance 429 non-idempotent write not retried: credential={} endpoint={} truth lookup unavailable",
                        self.credential_id,
                        path
                    ));
                }
                RetryDecision::IpBan { cooldown_ms } => {
                    self.record_retry(
                        path,
                        &response,
                        class,
                        RetryScope::Normal,
                        0,
                        cooldown_ms,
                        "ip_ban",
                    );
                    return Err(anyhow!(
                        "binance http 418 IP ban: credential={} endpoint={} cooldown_ms={cooldown_ms}",
                        self.credential_id,
                        path
                    ));
                }
                RetryDecision::GiveUp { reason } => {
                    self.record_retry(
                        path,
                        &response,
                        class,
                        RetryScope::Normal,
                        completed_retries,
                        0,
                        reason,
                    );
                    return Err(anyhow!(
                        "binance 429 retry abandoned: credential={} endpoint={} retries={} reason={reason}",
                        self.credential_id,
                        path,
                        completed_retries
                    ));
                }
            }
        }
    }

    async fn signed_request_with_retry(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        class: RetryClass,
        scope: RetryScope,
        request_class: RequestClass,
    ) -> anyhow::Result<HttpAttemptResponse> {
        self.signed_request_with_retry_authorized(
            method,
            path,
            params,
            class,
            scope,
            request_class,
            SignedAuthority::Ordinary,
        )
        .await
    }

    async fn signed_request_with_retry_authorized(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        class: RetryClass,
        scope: RetryScope,
        request_class: RequestClass,
        mut authority: SignedAuthority<'_>,
    ) -> anyhow::Result<HttpAttemptResponse> {
        let started_at_ms = self.clock.now_ms();
        let mut completed_retries = 0_u32;
        loop {
            let response = self
                .send_signed_once_authorized(
                    method,
                    path,
                    params,
                    scope,
                    request_class,
                    &mut authority,
                )
                .await?;
            if (200..300).contains(&response.status) {
                return Ok(response);
            }
            if response.status != 418 && response.status != 429 {
                return Err(http_error(&response));
            }
            match retry_decision(self.retry_input(
                &response,
                class,
                scope,
                completed_retries,
                started_at_ms,
                path,
            )) {
                RetryDecision::Retry {
                    after_ms,
                    retry_number,
                } => {
                    self.record_retry(
                        path,
                        &response,
                        class,
                        scope,
                        retry_number,
                        after_ms,
                        "retry",
                    );
                    self.clock.sleep(Duration::from_millis(after_ms)).await;
                    completed_retries = retry_number;
                }
                RetryDecision::IpBan { cooldown_ms } => {
                    self.record_retry(path, &response, class, scope, 0, cooldown_ms, "ip_ban");
                    return Err(anyhow!(
                        "binance http 418 IP ban: credential={} endpoint={} cooldown_ms={cooldown_ms}",
                        self.credential_id,
                        path
                    ));
                }
                RetryDecision::GiveUp { reason } => {
                    self.record_retry(path, &response, class, scope, completed_retries, 0, reason);
                    return Err(anyhow!(
                        "binance 429 retry abandoned: credential={} endpoint={} retries={} reason={reason}",
                        self.credential_id,
                        path,
                        completed_retries
                    ));
                }
                RetryDecision::VerifyTruth { .. } => {
                    return Err(anyhow!(
                        "internal retry classification error: truth lookup requested for retryable request"
                    ));
                }
            }
        }
    }

    async fn signed_non_idempotent_delete(
        &self,
        path: &str,
        params: &[(String, String)],
        request_class: RequestClass,
    ) -> anyhow::Result<NonIdempotentWriteOutcome> {
        let started_at_ms = self.clock.now_ms();
        let response = self
            .send_signed_once("DELETE", path, params, RetryScope::Exit, request_class)
            .await?;
        if (200..300).contains(&response.status) {
            return Ok(NonIdempotentWriteOutcome::Response(response));
        }
        if response.status != 418 && response.status != 429 {
            return Err(http_error(&response));
        }
        match retry_decision(self.retry_input(
            &response,
            RetryClass::NonIdempotentWrite,
            RetryScope::Exit,
            0,
            started_at_ms,
            path,
        )) {
            RetryDecision::VerifyTruth { after_ms } => {
                self.record_retry(
                    path,
                    &response,
                    RetryClass::NonIdempotentWrite,
                    RetryScope::Exit,
                    0,
                    after_ms,
                    "verify_truth",
                );
                self.clock.sleep(Duration::from_millis(after_ms)).await;
                Ok(NonIdempotentWriteOutcome::VerifyTruth)
            }
            RetryDecision::IpBan { cooldown_ms } => {
                self.record_retry(
                    path,
                    &response,
                    RetryClass::NonIdempotentWrite,
                    RetryScope::Exit,
                    0,
                    cooldown_ms,
                    "ip_ban",
                );
                Err(anyhow!(
                    "binance http 418 IP ban: credential={} endpoint={} cooldown_ms={cooldown_ms}",
                    self.credential_id,
                    path
                ))
            }
            RetryDecision::GiveUp { reason } => {
                self.record_retry(
                    path,
                    &response,
                    RetryClass::NonIdempotentWrite,
                    RetryScope::Exit,
                    0,
                    0,
                    reason,
                );
                Err(anyhow!(
                    "binance 429 non-idempotent write unresolved: credential={} endpoint={} reason={reason}",
                    self.credential_id,
                    path
                ))
            }
            RetryDecision::Retry { .. } => Err(anyhow!(
                "internal retry classification error: non-idempotent write selected automatic retry"
            )),
        }
    }

    #[allow(dead_code)]
    pub async fn place_algo_conditional(
        &self,
        limiter: &CredentialRateLimiter,
        permit: &mut crate::protection_dispatch::ProtectionDispatchPermit,
        order_type: BinanceAlgoConditionalOrderType,
        symbol: &str,
        side: &str,
        trigger_price: &str,
        quantity: Option<&str>,
        reduce_only: bool,
        close_position: bool,
        client_algo_id: &BinanceClientAlgoId,
    ) -> anyhow::Result<BinanceAlgoOrderResponse> {
        let trigger_price = trigger_price.trim();
        if trigger_price.is_empty() {
            return Err(anyhow!("{} missing triggerPrice", order_type.as_binance()));
        }
        if client_algo_id.as_str().trim().is_empty() {
            return Err(anyhow!("conditional algo order missing clientAlgoId"));
        }
        if close_position && quantity.is_some() {
            return Err(anyhow!(
                "{} closePosition cannot set quantity",
                order_type.as_binance()
            ));
        }
        if !close_position && quantity.unwrap_or("").trim().is_empty() {
            return Err(anyhow!(
                "{} missing quantity (or set closePosition)",
                order_type.as_binance()
            ));
        }

        let wire = crate::protection_dispatch::AlgoWire {
            symbol: symbol.to_owned(),
            side: side.to_owned(),
            order_type: order_type.as_binance().to_owned(),
            trigger_price: trigger_price.to_owned(),
            quantity: quantity.map(|v| v.trim().to_owned()),
            reduce_only,
            close_position,
            client_algo_id: client_algo_id.as_str().to_owned(),
        };
        let _ = limiter;
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::place_algo_conditional(
                &path,
                permit,
                &self.credential_id,
                &wire,
                order_type.as_binance(),
                symbol,
                client_algo_id,
            )
            .await;
        }

        let ts = self.clock.now_ms().max(0) as u64;
        let mut params: Vec<(String, String)> = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("algoType".to_string(), "CONDITIONAL".to_string()),
            ("type".to_string(), order_type.as_binance().to_string()),
            ("triggerPrice".to_string(), trigger_price.to_string()),
            ("workingType".to_string(), "MARK_PRICE".to_string()),
            ("priceProtect".to_string(), "true".to_string()),
            (
                "clientAlgoId".to_string(),
                client_algo_id.as_str().to_string(),
            ),
            ("recvWindow".to_string(), self.recv_window_ms.to_string()),
            ("timestamp".to_string(), ts.to_string()),
        ];

        if close_position {
            params.push(("closePosition".to_string(), "true".to_string()));
        } else {
            params.push((
                "quantity".to_string(),
                quantity.expect("validated quantity").trim().to_string(),
            ));
            params.push((
                "reduceOnly".to_string(),
                if reduce_only { "true" } else { "false" }.to_string(),
            ));
        }

        self.signed_post_json(
            "/fapi/v1/algoOrder",
            params,
            if reduce_only || close_position {
                RetryScope::Exit
            } else {
                RetryScope::Normal
            },
            RequestClass::Protection,
            SignedAuthority::Protection {
                permit,
                wire: &wire,
            },
        )
        .await
        .context("decode binance algo order response failed")
    }

    #[allow(dead_code)]
    pub async fn get_algo_order_json(
        &self,
        limiter: &CredentialRateLimiter,
        algo_id: Option<&BinanceAlgoId>,
        client_algo_id: Option<&BinanceClientAlgoId>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        if algo_id.is_none() && client_algo_id.is_none() {
            return Err(anyhow!("algoId or clientAlgoId must be provided"));
        }
        let _ = limiter;
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::get_algo_order(&path, algo_id, client_algo_id).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let mut params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        if let Some(id) = algo_id {
            params.push(("algoId", id.to_string()));
        }
        if let Some(id) = client_algo_id {
            if !id.as_str().trim().is_empty() {
                params.push(("clientAlgoId", id.as_str().trim().to_string()));
            }
        }
        self.signed_get_json("/fapi/v1/algoOrder", params, request_class)
            .await
    }

    #[allow(dead_code)]
    pub async fn get_open_algo_orders_json(
        &self,
        limiter: &CredentialRateLimiter,
        symbol: &str,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        let _ = limiter;
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::open_algo_orders(&path, Some(symbol)).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let params = vec![
            ("symbol", symbol.to_string()),
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        self.signed_get_json("/fapi/v1/openAlgoOrders", params, request_class)
            .await
    }

    pub async fn get_all_open_algo_orders_json(
        &self,
        limiter: &CredentialRateLimiter,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        // Weight is charged centrally in `send_signed_once`; without a symbol
        // this request consumes 40 units, not one request token.
        let _ = limiter;
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::open_algo_orders(&path, None).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        self.signed_get_json("/fapi/v1/openAlgoOrders", params, request_class)
            .await
    }

    #[allow(dead_code)]
    pub async fn cancel_algo_order_json(
        &self,
        limiter: &CredentialRateLimiter,
        algo_id: &BinanceAlgoId,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        self.cancel_algo_order_by_identity_json(limiter, Some(algo_id), None, request_class)
            .await
    }

    pub async fn cancel_algo_order_by_identity_json(
        &self,
        limiter: &CredentialRateLimiter,
        algo_id: Option<&BinanceAlgoId>,
        client_algo_id: Option<&BinanceClientAlgoId>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        if algo_id.is_none() && client_algo_id.is_none() {
            return Err(anyhow!("algoId or clientAlgoId must be provided"));
        }
        let _ = limiter;
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::cancel_algo_order(&path, algo_id, client_algo_id)
                .await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let mut params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        if let Some(algo_id) = algo_id {
            params.push(("algoId", algo_id.to_string()));
        }
        if let Some(client_algo_id) = client_algo_id {
            params.push(("clientAlgoId", client_algo_id.as_str().trim().to_string()));
        }
        let params = params
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<Vec<_>>();
        match self
            .signed_non_idempotent_delete("/fapi/v1/algoOrder", &params, request_class)
            .await?
        {
            NonIdempotentWriteOutcome::Response(response) => {
                serde_json::from_str(&response.body).context("decode binance response failed")
            }
            NonIdempotentWriteOutcome::VerifyTruth => {
                let truth = self
                    .signed_get_json_with_scope(
                        "/fapi/v1/algoOrder",
                        params,
                        RetryScope::Exit,
                        RequestClass::EventTruth,
                    )
                    .await?;
                let status = truth
                    .get("algoStatus")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                if status.eq_ignore_ascii_case("CANCELED")
                    || status.eq_ignore_ascii_case("CANCELLED")
                {
                    Ok(truth)
                } else {
                    Err(anyhow!(
                        "binance algo cancel ambiguous after 429; exchange truth status={status}"
                    ))
                }
            }
        }
    }

    pub async fn create_listen_key(&self) -> anyhow::Result<String> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::create_listen_key(&path).await;
        }
        let response = self
            .unsigned_request_with_retry(
                "POST",
                "/fapi/v1/listenKey",
                &[],
                RetryClass::NonIdempotentWrite,
                RequestClass::PeriodicReconcile,
            )
            .await?;
        let v: serde_json::Value =
            serde_json::from_str(&response.body).context("decode listenKey failed")?;
        let key = v
            .get("listenKey")
            .and_then(|k| k.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();
        if key.is_empty() {
            return Err(anyhow!("binance listenKey missing in response"));
        }
        Ok(key)
    }

    pub async fn keepalive_listen_key(&self, listen_key: &str) -> anyhow::Result<()> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            let _ = listen_key;
            return crate::l4_fake_exchange::keepalive_listen_key(&path).await;
        }
        self.unsigned_request_with_retry(
            "PUT",
            "/fapi/v1/listenKey",
            &[("listenKey".to_string(), listen_key.to_string())],
            RetryClass::IdempotentWrite,
            RequestClass::PeriodicReconcile,
        )
        .await?;
        Ok(())
    }

    pub async fn place_market_order(
        &self,
        symbol: &str,
        side: &str, // BUY|SELL
        quantity: &str,
        reduce_only: bool,
        client_order_id: &str,
    ) -> anyhow::Result<BinanceOrderAck> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            let _lifecycle_send = if !reduce_only {
                Some(
                    crate::lifecycle_fence::authorize_send(
                        self.cooldown_store.as_ref(),
                        &self.credential_id,
                    )
                    .await?,
                )
            } else {
                None
            };
            return crate::l4_fake_exchange::place_order(
                &path,
                "MARKET",
                symbol,
                side,
                quantity,
                reduce_only,
                client_order_id,
            )
            .await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let params: Vec<(String, String)> = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("type".to_string(), "MARKET".to_string()),
            ("quantity".to_string(), quantity.to_string()),
            (
                "reduceOnly".to_string(),
                if reduce_only { "true" } else { "false" }.to_string(),
            ),
            ("newClientOrderId".to_string(), client_order_id.to_string()),
            ("recvWindow".to_string(), self.recv_window_ms.to_string()),
            ("timestamp".to_string(), ts.to_string()),
        ];

        let response = self
            .signed_request_with_retry(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryClass::IdempotentWrite,
                if reduce_only {
                    RetryScope::Exit
                } else {
                    RetryScope::Normal
                },
                if reduce_only {
                    RequestClass::Exit
                } else {
                    RequestClass::Entry
                },
            )
            .await?;
        serde_json::from_str::<BinanceOrderAck>(&response.body)
            .context("decode binance order ack failed")
    }

    pub async fn place_limit_order(
        &self,
        symbol: &str,
        side: &str, // BUY|SELL
        quantity: &str,
        price: &str,
        reduce_only: bool,
        client_order_id: &str,
    ) -> anyhow::Result<BinanceOrderAck> {
        let p = price.trim();
        if p.is_empty() {
            return Err(anyhow!("limit order missing price"));
        }
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            let _lifecycle_send = if !reduce_only {
                Some(
                    crate::lifecycle_fence::authorize_send(
                        self.cooldown_store.as_ref(),
                        &self.credential_id,
                    )
                    .await?,
                )
            } else {
                None
            };
            return crate::l4_fake_exchange::place_order(
                &path,
                "LIMIT",
                symbol,
                side,
                quantity,
                reduce_only,
                client_order_id,
            )
            .await;
        }

        let ts = self.clock.now_ms().max(0) as u64;
        let params: Vec<(String, String)> = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("type".to_string(), "LIMIT".to_string()),
            ("timeInForce".to_string(), "GTC".to_string()),
            ("quantity".to_string(), quantity.to_string()),
            ("price".to_string(), p.to_string()),
            (
                "reduceOnly".to_string(),
                if reduce_only { "true" } else { "false" }.to_string(),
            ),
            ("newClientOrderId".to_string(), client_order_id.to_string()),
            ("recvWindow".to_string(), self.recv_window_ms.to_string()),
            ("timestamp".to_string(), ts.to_string()),
        ];

        let response = self
            .signed_request_with_retry(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryClass::IdempotentWrite,
                if reduce_only {
                    RetryScope::Exit
                } else {
                    RetryScope::Normal
                },
                if reduce_only {
                    RequestClass::Exit
                } else {
                    RequestClass::Entry
                },
            )
            .await?;
        serde_json::from_str::<BinanceOrderAck>(&response.body)
            .context("decode binance order ack failed")
    }

    pub async fn get_order_json(
        &self,
        symbol: &str,
        exchange_order_id: Option<&str>,
        client_order_id: Option<&str>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            let _ = symbol;
            return crate::l4_fake_exchange::get_order(&path, exchange_order_id, client_order_id)
                .await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let mut params = vec![
            ("symbol", symbol.to_string()),
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        if let Some(id) = exchange_order_id {
            if !id.trim().is_empty() {
                params.push(("orderId", id.trim().to_string()));
            }
        }
        if let Some(id) = client_order_id {
            if !id.trim().is_empty() {
                params.push(("origClientOrderId", id.trim().to_string()));
            }
        }
        self.signed_get_json("/fapi/v1/order", params, request_class)
            .await
    }

    pub async fn cancel_order_json(
        &self,
        symbol: &str,
        exchange_order_id: Option<&str>,
        client_order_id: Option<&str>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            let _ = symbol;
            return crate::l4_fake_exchange::cancel_order(
                &path,
                exchange_order_id,
                client_order_id,
            )
            .await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let mut params = vec![
            ("symbol", symbol.to_string()),
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        if let Some(id) = exchange_order_id {
            if !id.trim().is_empty() {
                params.push(("orderId", id.trim().to_string()));
            }
        }
        if let Some(id) = client_order_id {
            if !id.trim().is_empty() {
                params.push(("origClientOrderId", id.trim().to_string()));
            }
        }
        let params = params
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<Vec<_>>();
        match self
            .signed_non_idempotent_delete("/fapi/v1/order", &params, request_class)
            .await?
        {
            NonIdempotentWriteOutcome::Response(response) => {
                serde_json::from_str(&response.body).context("decode binance response failed")
            }
            NonIdempotentWriteOutcome::VerifyTruth => {
                let truth = self
                    .signed_get_json_with_scope(
                        "/fapi/v1/order",
                        params,
                        RetryScope::Exit,
                        RequestClass::EventTruth,
                    )
                    .await?;
                let status = truth
                    .get("status")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                if status.eq_ignore_ascii_case("CANCELED")
                    || status.eq_ignore_ascii_case("CANCELLED")
                {
                    Ok(truth)
                } else {
                    Err(anyhow!(
                        "binance cancel ambiguous after 429; exchange truth status={status}"
                    ))
                }
            }
        }
    }

    pub async fn open_orders(
        &self,
        symbol: Option<&str>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::open_orders(&path, symbol).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let mut params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        if let Some(s) = symbol {
            params.push(("symbol", s.to_string()));
        }
        self.signed_get_json("/fapi/v1/openOrders", params, request_class)
            .await
    }

    pub async fn balances(&self, request_class: RequestClass) -> anyhow::Result<serde_json::Value> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::balances(&path).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        self.signed_get_json("/fapi/v2/balance", params, request_class)
            .await
    }

    pub async fn positions(
        &self,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        #[cfg(feature = "l4-fault-injection")]
        if let Some(path) = crate::l4_fake_exchange::configured_db()? {
            return crate::l4_fake_exchange::positions(&path).await;
        }
        let ts = self.clock.now_ms().max(0) as u64;
        let params = vec![
            ("recvWindow", self.recv_window_ms.to_string()),
            ("timestamp", ts.to_string()),
        ];
        self.signed_get_json("/fapi/v2/positionRisk", params, request_class)
            .await
    }

    #[allow(dead_code)]
    pub(crate) async fn user_trades_after(
        &self,
        limiter: &CredentialRateLimiter,
        symbol: &str,
        last_trade_id: i64,
        page_limit: usize,
        request_class: RequestClass,
    ) -> anyhow::Result<Vec<BinanceUserTrade>> {
        let symbol = symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            return Err(anyhow!("Binance userTrades symbol must not be empty"));
        }
        if !(1..=1_000).contains(&page_limit) {
            return Err(anyhow!("Binance userTrades page limit must be in 1..=1000"));
        }
        let mut from_id = first_user_trades_from_id(last_trade_id)?;
        let mut trades = Vec::new();
        loop {
            let _ = limiter;
            let ts = self.clock.now_ms().max(0) as u64;
            let page_value = self
                .signed_get_json(
                    "/fapi/v1/userTrades",
                    vec![
                        ("symbol", symbol.clone()),
                        ("fromId", from_id.to_string()),
                        ("limit", page_limit.to_string()),
                        ("recvWindow", self.recv_window_ms.to_string()),
                        ("timestamp", ts.to_string()),
                    ],
                    request_class,
                )
                .await?;
            let page: Vec<BinanceUserTrade> =
                serde_json::from_value(page_value).context("decode Binance userTrades response")?;
            let disposition = user_trades_page_disposition(&page, page_limit)?;
            trades.extend(page);
            match disposition {
                UserTradesPageDisposition::Complete => return Ok(trades),
                UserTradesPageDisposition::Continue { next_from_id } => {
                    if next_from_id <= from_id {
                        return Err(anyhow!(
                            "Binance userTrades page did not advance: fromId={from_id} next={next_from_id}"
                        ));
                    }
                    from_id = next_from_id;
                }
            }
        }
    }

    pub(crate) async fn funding_income_window(
        &self,
        symbol: &str,
        start_time_ms: i64,
        end_time_ms: i64,
    ) -> anyhow::Result<serde_json::Value> {
        self.signed_get_json(
            "/fapi/v1/income",
            vec![
                ("symbol", symbol.to_string()),
                ("incomeType", "FUNDING_FEE".to_string()),
                ("startTime", start_time_ms.to_string()),
                ("endTime", end_time_ms.to_string()),
                ("limit", "1000".to_string()),
                ("recvWindow", self.recv_window_ms.to_string()),
                ("timestamp", self.clock.now_ms().max(0).to_string()),
            ],
            RequestClass::EventTruth,
        )
        .await
    }

    async fn signed_get_json(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        self.signed_get_json_with_scope(path, params, RetryScope::Normal, request_class)
            .await
    }

    async fn signed_get_json_with_scope<N: Into<String>>(
        &self,
        path: &str,
        params: Vec<(N, String)>,
        scope: RetryScope,
        request_class: RequestClass,
    ) -> anyhow::Result<serde_json::Value> {
        let params = params
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect::<Vec<_>>();
        let response = self
            .signed_request_with_retry(
                "GET",
                path,
                &params,
                RetryClass::ReadOnly,
                scope,
                request_class,
            )
            .await?;
        serde_json::from_str(&response.body).context("decode binance response failed")
    }

    #[allow(dead_code)]
    async fn signed_post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: Vec<(String, String)>,
        scope: RetryScope,
        request_class: RequestClass,
        authority: SignedAuthority<'_>,
    ) -> anyhow::Result<T> {
        let response = self
            .signed_request_with_retry_authorized(
                "POST",
                path,
                &params,
                RetryClass::IdempotentWrite,
                scope,
                request_class,
                authority,
            )
            .await?;
        serde_json::from_str(&response.body).context("decode binance response failed")
    }
}

fn http_error(response: &HttpAttemptResponse) -> anyhow::Error {
    anyhow!(
        "binance http {}: {}",
        response.status,
        response.body.chars().take(300).collect::<String>()
    )
}

pub fn sign_hmac_sha256_hex(secret: &[u8], message: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac can take key of any size");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

pub(crate) fn signed_query_hmac(
    secret: &[u8],
    ordered_params: &[(String, String)],
) -> anyhow::Result<(String, String)> {
    let query = serde_urlencoded::to_string(ordered_params).context("encode query failed")?;
    let signature = sign_hmac_sha256_hex(secret, query.as_bytes());
    Ok((query, signature))
}

/// `symbol_key` 的**完整身份**：`venue|market_kind|base|quote|settle`。
///
/// **闭世界不得在这里分叉。**
///
/// 此前只有 `symbol_from_symbol_key`：它取 `parts[2]`/`parts[3]` 拼出交易所
/// symbol，**把 key 内的 venue/market 段整段丢掉**。于是订阅入口虽然校验了
/// 外层 venue/market，`okx|spot|BTC|USDT|USDT` 这种 key 仍能整批通过——
/// 两个身份来源（外层字段与 key 内部段）互相不认，闭世界从中间裂开
/// （评审实跑：`okx|spot|…` 形状的 symbol_key 通过了整批校验）。
///
/// 身份从此**只有一个解析器**：调用方要么拿完整身份、要么只拿 symbol，
/// 但两者出自同一次解析，不可能各说各话。
///
/// **形状必须精确，settle 必须留下。**
///
/// R10 的判据是 `parts.len() < 4 → None`，也就是「**至少**四段，多余的段
/// 一律忽略」。两个后果：
///
/// * 四段的 `binance|futures|BTC|USDT` 被当成完整身份接受——可它**没有
///   settle**。同一个 `BTCUSDT` 在 USDⓈ-M（settle=USDT）与币本位
///   （settle=BTC）是两张不同的合约，把 settle 未知的 key 当成完整身份，
///   等于允许一份真值被应用到另一张合约上。
/// * 六段及以上被静默截断——那说明写这条 key 的生产者用的是**另一套语法**，
///   忽略多出来的段会让两套语法共用一个身份空间。
///
/// 评审变异 R7-01（「至少四段」收紧为「恰好五段」）23/23 存活、
/// R7-02（settle 投影改用最后一段）23/23 存活，正是因为整套夹具里没有
/// 任何一条段数分叉或 settle 分叉的向量：判据没被观察过，也就谈不上被钉住。
///
/// 现在：**恰好五段、每段非空**，且 `settle` 作为独立字段被带出来参与身份。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolKeyIdentity {
    pub venue: String,
    pub market_kind: String,
    pub base: String,
    pub quote: String,
    /// 结算资产。**不是冗余信息**：它区分 USDⓈ-M 与币本位合约。
    pub settle: String,
    /// 交易所 symbol（`base` + `quote`）。派生字段，与上面四段同源同一次解析。
    pub symbol: String,
}

/// `symbol_key` 的段数。语法是 `venue|market_kind|base|quote|settle`，
/// 单列一份常量，免得判据与文档各写各的。
pub const SYMBOL_KEY_SEGMENTS: usize = 5;

pub fn symbol_key_identity(symbol_key: &str) -> Option<SymbolKeyIdentity> {
    let parts: Vec<&str> = symbol_key.split('|').map(|s| s.trim()).collect();
    if parts.len() != SYMBOL_KEY_SEGMENTS {
        return None;
    }
    let venue = parts[0];
    let market_kind = parts[1];
    let base = parts[2];
    let quote = parts[3];
    let settle = parts[4];
    if venue.is_empty()
        || market_kind.is_empty()
        || base.is_empty()
        || quote.is_empty()
        || settle.is_empty()
    {
        return None;
    }
    Some(SymbolKeyIdentity {
        venue: venue.to_string(),
        market_kind: market_kind.to_string(),
        base: base.to_string(),
        quote: quote.to_string(),
        settle: settle.to_string(),
        symbol: format!("{}{}", base, quote),
    })
}

/// 本引擎唯一交易的市场：Binance USDⓈ-M 永续。**唯一一份**，
/// WS 归属判据、reconcile 归属判据与订阅闭世界共用它。
pub(crate) const USDM_VENUE: &str = "binance";
pub(crate) const USDM_MARKET_KIND: &str = "futures";

/// **一条本地 `symbol_key` 是否就是这条 USDⓈ-M 交易所消息说的那个市场。**
///
/// WS 与 reconcile
/// 此前各用各的归属判据——WS 比 `symbol_from_symbol_key`（venue/market/settle
/// 全被折掉），reconcile 的 open-order map **连 symbol 都不含**。两个消费同一份
/// 交易所真值的路径用两个不同的身份，必然在某个方向上把 A 的真值写到 B 上。
///
/// 判据是**完整交易身份**：venue、market_kind、symbol 三段与本引擎的闭世界
/// 一致，且 `settle == quote`。最后一条是 USDⓈ-M 的定义：币本位合约
/// （`…|BTC|USDT|BTC`）的 symbol 同样是 `BTCUSDT`，但它是另一张合约，
/// 交易所 USDⓈ-M 用户流/对账的真值绝不能落到它头上。
pub(crate) fn usdm_symbol_key_matches(symbol_key: &str, upstream_symbol: &str) -> bool {
    usdm_identity(symbol_key)
        .is_some_and(|identity| identity.symbol.eq_ignore_ascii_case(upstream_symbol.trim()))
}

/// **本引擎闭世界内的 USDⓈ-M 交易身份——唯一一份解析 + 唯一一份判据。**
///
/// 。R11 把 WS 的候选裁决
/// 换成了完整身份，但**提交侧、逐单回查、missing-local 集合、orphan 匹配**
/// 仍在用裸 `symbol_from_symbol_key`：它把 `venue|market_kind|…|settle` 四段
/// 里的三段整段丢掉，只留 `base+quote`。于是
/// `okx|spot|BTC|USDT|BTC` 这条 key 会被换算成 `BTCUSDT`，然后
///
/// * 被送去 Binance USDⓈ-M 下单（提交侧）；
/// * 被拿去向 Binance USDⓈ-M 逐单回查、并按 `-2013` 裁决本地单死亡（对账）；
/// * 与另一张合约的开放单撞 id 而被当成"本地已知"（missing-local）。
///
/// 三条路径消费/生产的都是**同一个交易所的真值**，判据却各说各话。本函数是
/// 那个唯一判据：五段形状（`symbol_key_identity`）+ venue/market_kind 属于
/// 本引擎闭世界 + `settle == quote`（USDⓈ-M 的定义，把币本位排除掉）。
/// 认不出就是 `None`——**没有降级成裸 symbol 的那条路**。
pub(crate) fn usdm_identity(symbol_key: &str) -> Option<SymbolKeyIdentity> {
    let identity = symbol_key_identity(symbol_key)?;
    if identity.venue.eq_ignore_ascii_case(USDM_VENUE)
        && identity.market_kind.eq_ignore_ascii_case(USDM_MARKET_KIND)
        && identity.settle.eq_ignore_ascii_case(&identity.quote)
    {
        Some(identity)
    } else {
        None
    }
}

/// **一个交易所 symbol 字符串是否落在本引擎的 USDⓈ-M 闭世界内。**
///
/// 。`fill_recovery_scopes` /
/// `fill_replay_cursors` 两张旧表**只存 `(credential_id, symbol)`**，一列身份
/// 都没有：拿它们直接播种恢复域，等于相信一个来源不明的字符串就是一张
/// Binance USDⓈ-M 合约。这里把那个字符串放回**同一套闭世界判据**里过一遍：
/// 先按 USDT 计价还原出规范五段 key，再用 `usdm_identity` 判定，最后要求
/// 还原出来的 symbol 与输入逐字相同。认不出就是 `None`——没有降级路径。
///
/// 这**不是**归属证明（`BTCUSDT` 形状对不代表这个 credential 真的在这张
/// 合约上有过成交），归属由 `fill_replay::fill_scope_origin_proof` 用
/// 已证明的 fill / 双源一致的 engine_order 单独证明。
pub(crate) fn usdm_scope_symbol(symbol: &str) -> Option<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    let base = symbol
        .strip_suffix("USDT")
        .map(str::trim)
        .filter(|base| !base.is_empty())?;
    let key = format!("{USDM_VENUE}|{USDM_MARKET_KIND}|{base}|USDT|USDT");
    usdm_identity(&key).and_then(|identity| {
        identity
            .symbol
            .eq_ignore_ascii_case(&symbol)
            .then_some(identity.symbol)
    })
}

/// 一条本地 `symbol_key` 对应的 USDⓈ-M 交易所 symbol。
///
/// **取代**在交易所边界上直接调 `symbol_from_symbol_key`：认不出闭世界身份
/// 的 key 在这里没有 symbol，调用方因此只能 fail-closed，而不是拿着一个
/// 拼出来的 `BTCUSDT` 去别的市场执行。
pub(crate) fn usdm_symbol(symbol_key: &str) -> Option<String> {
    usdm_identity(symbol_key).map(|identity| identity.symbol)
}

/// **提交侧的完整身份判据**。
///
/// 提交路径此前只做了两件半事：外层比 `intent.exchange` / `intent.market_kind`
/// 是不是 binance/futures，然后用 `symbol_from_symbol_key` 把 key 拼成 symbol。
/// key **自己声明的** venue/market_kind/settle 一段都没人看——
/// `okx|spot|BTC|USDT|BTC` 因此能带着一个合规的外层声明穿过整条链，
/// 变成一张发往 Binance USDⓈ-M 的 `BTCUSDT` 单。
///
/// 这里把两个身份来源钉在一起：key 的 venue/market_kind 必须与 intent 声明的
/// 逐段一致，且整条 key 必须落在 USDⓈ-M 闭世界内。任何一段不一致即
/// fail-closed 拒单，错误串直接进 `IntentAck.message`。
///
/// ****：提交侧不是唯一有两个身份
/// 来源的地方——`engine_orders` 的每一行同时存着**外层** `exchange` /
/// `market_kind` 两列与**内层** `symbol_key`，而 REST 回补认领与恢复域发现
/// 此前只看内层。判据只有一份（`usdm_identity_agreeing_with`），
/// 提交侧只是它的一个入口。
pub(crate) fn usdm_submission_identity(
    symbol_key: &str,
    declared_exchange: &str,
    declared_market_kind: &str,
) -> Result<SymbolKeyIdentity, String> {
    usdm_identity_agreeing_with(symbol_key, declared_exchange, declared_market_kind)
}

/// **双源交易身份判据**：一条 `symbol_key` 与它**同一行/同一条消息**上的
/// 外层 `exchange` / `market_kind` 声明必须逐段一致，且整条 key 必须落在
/// 本引擎的 USDⓈ-M 闭世界内。
///
/// 两个来源任意一段不一致，这一行就是**矛盾行**：它既不能被当作 Binance
/// USDⓈ-M 的身份使用（内层可能是伪造/漂移的），也不能被静默当作别的市场
/// 忽略掉（外层同样可能是错的）。调用方一律 fail-closed。
pub(crate) fn usdm_identity_agreeing_with(
    symbol_key: &str,
    declared_exchange: &str,
    declared_market_kind: &str,
) -> Result<SymbolKeyIdentity, String> {
    trading_identity_sources_agree(symbol_key, declared_exchange, declared_market_kind)?;
    usdm_identity(symbol_key).ok_or_else(|| {
        format!(
            "symbol_key {symbol_key:?} is not a {USDM_VENUE} {USDM_MARKET_KIND} USDⓈ-M \
             identity (settle must equal quote); this engine trades nowhere else"
        )
    })
}

/// **两个身份来源是否逐段一致**——只判一致性，不判闭世界归属。
///
/// 。把一致性从闭世界归属里拆出来是因为
/// 两者的处置**不同**：
///
/// * 两源一致但不是 USDⓈ-M（例如整行都老老实实写着 okx/spot）——它只是
///   别的市场的一行，跳过即可；
/// * 两源**不一致**（外层 okx/spot + 内层 `binance|futures|…`）——这一行
///   自己就在撒谎，两个来源里必有一个是错的。它既不能被 Binance 的真值
///   认领，也不能被当成"别的市场"静默忽略：一律 fail-closed + 告警。
pub(crate) fn trading_identity_sources_agree(
    symbol_key: &str,
    declared_exchange: &str,
    declared_market_kind: &str,
) -> Result<(), String> {
    let Some(identity) = symbol_key_identity(symbol_key) else {
        return Err(format!(
            "symbol_key {symbol_key:?} is not a {SYMBOL_KEY_SEGMENTS}-segment \
             venue|market_kind|base|quote|settle identity; it cannot be submitted"
        ));
    };
    let declared_exchange = declared_exchange.trim();
    let declared_market_kind = declared_market_kind.trim();
    if !identity.venue.eq_ignore_ascii_case(declared_exchange) {
        return Err(format!(
            "symbol_key {symbol_key:?} declares venue {:?} but the intent declares \
             {declared_exchange:?}; an order whose two identities disagree would be \
             routed to a market it was never approved for",
            identity.venue
        ));
    }
    if !identity
        .market_kind
        .eq_ignore_ascii_case(declared_market_kind)
    {
        return Err(format!(
            "symbol_key {symbol_key:?} declares market_kind {:?} but the intent declares \
             {declared_market_kind:?}; an order whose two identities disagree would be \
             routed to a market it was never approved for",
            identity.market_kind
        ));
    }
    Ok(())
}

pub fn symbol_from_symbol_key(symbol_key: &str) -> Option<String> {
    symbol_key_identity(symbol_key).map(|identity| identity.symbol)
}

pub(crate) fn key_env_names(credential_id: &str) -> (String, String) {
    let suffix = crate::venue::credential_secret_locator(credential_id);
    (
        format!("BINANCE_API_KEY_{suffix}"),
        format!("BINANCE_API_SECRET_{suffix}"),
    )
}

fn keys_from_values(
    venue: Option<crate::venue::Venue>,
    per_key: Option<&str>,
    per_secret: Option<&str>,
    global_key: Option<&str>,
    global_secret: Option<&str>,
) -> Option<BinanceKeys> {
    let api_key = if venue.is_some() {
        per_key?
    } else {
        per_key.or(global_key)?
    };
    let api_secret = if venue.is_some() {
        per_secret?
    } else {
        per_secret.or(global_secret)?
    };
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return None;
    }
    Some(BinanceKeys {
        api_key: api_key.trim().to_string(),
        api_secret: api_secret.trim().to_string(),
    })
}

pub fn keys_from_env(credential_id: &str) -> Result<Option<BinanceKeys>, String> {
    // 模拟凭据与 `paper` 保留命名空间下的畸形串都不得解析出任何交易所密钥，
    // 尤其不得回落到全局 BINANCE_API_KEY/SECRET（那是 mainnet 主密钥）。
    if !matches!(
        crate::venue::credential_kind(credential_id),
        crate::venue::CredentialKind::ExchangeBacked
    ) {
        return Ok(None);
    }
    let venue = crate::venue::venue_for_credential(credential_id)?;
    let (key_env, secret_env) = key_env_names(credential_id);
    let per_key = std::env::var(key_env).ok();
    let per_secret = std::env::var(secret_env).ok();
    let global_key = std::env::var("BINANCE_API_KEY").ok();
    let global_secret = std::env::var("BINANCE_API_SECRET").ok();
    Ok(keys_from_values(
        venue,
        per_key.as_deref(),
        per_secret.as_deref(),
        global_key.as_deref(),
        global_secret.as_deref(),
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex, Once};

    use super::*;
    use crate::rate_limiter::TokenBucketConfig;
    use crate::store::{mysql_test_support::MySqlScratch, Store};
    use sqlx::Row;

    static INSTALL_TEST_DRIVERS: Once = Once::new();

    mod rate_limit_observation {
        use super::*;
        use std::collections::BTreeMap;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        type Events = Vec<BTreeMap<String, String>>;
        struct Capture(Arc<Mutex<Events>>);
        struct Fields(BTreeMap<String, String>);

        impl tracing::field::Visit for Fields {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
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
                if event.metadata().target() == module_path!().split("::tests").next().unwrap() {
                    let mut fields = Fields(BTreeMap::new());
                    event.record(&mut fields);
                    self.0.lock().unwrap().push(fields.0);
                }
            }
        }

        struct Observation {
            events: Events,
            error: String,
            until_ms: Option<i64>,
        }

        fn event<'a>(events: &'a Events, message: &str) -> &'a BTreeMap<String, String> {
            let matching: Vec<_> = events
                .iter()
                .filter(|event| event.get("message").is_some_and(|value| value == message))
                .collect();
            assert_eq!(matching.len(), 1, "exactly one {message}: {events:?}");
            matching[0]
        }

        // Only the exchange is replaced: public client methods traverse real
        // signing/headers, reqwest, cooldown persistence and retry disposition.
        // A scoped subscriber captures actual production tracing events.
        async fn observe(
            status: u16,
            retry_after: Option<&str>,
            body: &str,
            signed: bool,
            fail_persistence: bool,
        ) -> Observation {
            let (store, path) = retry_test_store().await;
            let clock = Arc::new(ManualRetryClock::new(1_000));
            let credential = format!("observation-{}", ulid::Ulid::new());
            let (mut client, _, _) = test_client_responses_with_cooldown_store(
                vec![],
                &credential,
                &credential,
                clock.clone(),
                store.clone(),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("response observation requires a local HTTP socket");
            client.base_url = format!("http://{}", listener.local_addr().unwrap());
            client.mock_http = None;
            let mut headers = HeaderMap::new();
            headers.insert("X-MBX-APIKEY", HeaderValue::from_static("local-test-key"));
            client.http = reqwest::Client::builder()
                .no_proxy()
                .default_headers(headers)
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap();
            let header = retry_after
                .map(|value| format!("Retry-After: {value}\r\n"))
                .unwrap_or_default();
            let reply = format!(
                "HTTP/1.1 {status} Limited\r\n{header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            let server_store = store.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut chunk = [0_u8; 1_024];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert!(count > 0 && request.len() < 16_384);
                    request.extend_from_slice(&chunk[..count]);
                }
                // The response arrives after the admission guard has read the
                // DB. Fail only the subsequent durable publication, not entry.
                if fail_persistence {
                    sqlx::query("DROP TABLE binance_ip_ban_cooldowns")
                        .execute(server_store.pool())
                        .await
                        .unwrap();
                }
                stream.write_all(reply.as_bytes()).await.unwrap();
                String::from_utf8(request).unwrap()
            });
            let events = Arc::new(Mutex::new(Vec::new()));
            let subscriber = tracing_subscriber::registry().with(Capture(events.clone()));
            let response = async {
                let result = if signed {
                    client
                        .get_all_open_algo_orders_json(&test_limiter(), RequestClass::EventTruth)
                        .await
                        .map(|_| ())
                } else {
                    client.create_listen_key().await.map(|_| ())
                };
                let error = result.expect_err("rate-limited response must fail");
                if status == 418 && !fail_persistence {
                    let blocked = client
                        .create_listen_key()
                        .await
                        .expect_err("second call must be stopped locally");
                    assert!(blocked.to_string().contains("cooldown active"));
                }
                format!("{error:#}")
            }
            .with_subscriber(subscriber);
            let error = tokio::time::timeout(Duration::from_secs(5), response)
                .await
                .expect("public response path must finish within the test deadline");
            let request = tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .expect("the real HTTP endpoint must have been called")
                .unwrap();
            assert!(request
                .to_ascii_lowercase()
                .contains("x-mbx-apikey: local-test-key"));
            if signed {
                assert!(request.starts_with("GET /fapi/v1/openAlgoOrders?"));
                assert!(request.contains("&signature="));
            } else {
                assert!(request.starts_with("POST /fapi/v1/listenKey "));
            }
            assert!(
                clock.sleeps().is_empty(),
                "observability must not enable retry"
            );
            let until_ms = if fail_persistence {
                None
            } else {
                let (table, column) = if status == 418 {
                    ("binance_ip_ban_cooldowns", "ban_until_ms")
                } else {
                    ("binance_rate_limit_cooldowns", "cooldown_until_ms")
                };
                Some(
                    sqlx::query(&format!(
                        "SELECT {column} FROM {table} WHERE egress_scope=?"
                    ))
                    .bind(&credential)
                    .fetch_one(store.pool())
                    .await
                    .unwrap()
                    .get::<i64, _>(0),
                )
            };
            let events = events.lock().unwrap().clone();
            let rendered = format!("{events:?}");
            for forbidden in [
                "local-test-key",
                "local-test-secret",
                "signature=",
                "SENSITIVE_SENTINEL",
            ] {
                assert!(
                    !rendered.contains(forbidden),
                    "upstream data must not leak: {rendered}"
                );
            }
            clear_egress_test_state(&credential);
            cleanup_retry_test_store(store, path).await;
            Observation {
                events,
                error,
                until_ms,
            }
        }

        const UPSTREAM: &str = "binance upstream rate-limit response";
        const BAN: &str =
            "binance IP ban received; automatic retry disabled and hard cooldown engaged";

        #[tokio::test]
        async fn signed_418_reports_actual_header_and_numeric_code_without_changing_cooldown() {
            let seen = observe(
                418,
                Some("5"),
                r#"{"code":-1003,"msg":"SENSITIVE_SENTINEL api_secret=local-test-secret"}"#,
                true,
                false,
            )
            .await;
            let upstream = event(&seen.events, UPSTREAM);
            assert_eq!(upstream["http_status"], "418");
            assert_eq!(upstream["endpoint"], "/fapi/v1/openAlgoOrders");
            assert_eq!(upstream["retry_after"], "5");
            assert_eq!(upstream["retry_after_ms"], "5000");
            assert_eq!(upstream["upstream_error_code"], "-1003");
            let ban = event(&seen.events, BAN);
            assert_eq!(ban["retry_after"], "5");
            assert_eq!(ban["next_wait_ms"], "120000");
            assert_eq!(
                event(&seen.events, "binance rate-limit disposition")["retry_after"],
                "5"
            );
            assert_eq!(seen.until_ms, Some(121_000));
            assert!(seen.error.contains("418 IP ban"));
        }

        #[tokio::test]
        async fn unsigned_418_missing_header_and_non_json_body_remain_unknown() {
            let seen = observe(418, None, "<html>SENSITIVE_SENTINEL</html>", false, false).await;
            let upstream = event(&seen.events, UPSTREAM);
            assert_eq!(upstream["endpoint"], "/fapi/v1/listenKey");
            assert_eq!(upstream["retry_after"], "absent");
            assert!(!upstream.contains_key("retry_after_ms"));
            assert!(!upstream.contains_key("upstream_error_code"));
            assert_eq!(event(&seen.events, BAN)["retry_after"], "absent");
            assert_eq!(seen.until_ms, Some(121_000));
        }

        #[tokio::test]
        async fn malformed_header_and_non_numeric_code_are_not_echoed_or_fabricated() {
            for body in [
                r#"{"code":"SENSITIVE_SENTINEL","msg":"SENSITIVE_SENTINEL"}"#,
                r#"{"code":2147483648}"#,
                r#"{"code":-1003.5}"#,
            ] {
                let seen = observe(418, Some("SENSITIVE_SENTINEL"), body, true, false).await;
                let upstream = event(&seen.events, UPSTREAM);
                assert_eq!(upstream["retry_after"], "invalid");
                assert!(!upstream.contains_key("retry_after_ms"));
                assert!(!upstream.contains_key("upstream_error_code"));
                assert_eq!(event(&seen.events, BAN)["retry_after"], "invalid");
                assert_eq!(seen.until_ms, Some(121_000));
            }
        }

        #[tokio::test]
        async fn valid_date_is_normalized_without_echoing_comments() {
            let seen = observe(
                418,
                Some("Thu, 01 Jan 1970 00:00:06 GMT (SENSITIVE_SENTINEL)"),
                r#"{"code":-1003}"#,
                true,
                false,
            )
            .await;
            let upstream = event(&seen.events, UPSTREAM);
            assert_eq!(upstream["retry_after"], "Thu, 1 Jan 1970 00:00:06 +0000");
            assert_eq!(upstream["retry_after_ms"], "5000");
            assert_eq!(seen.until_ms, Some(121_000));
        }

        #[tokio::test]
        async fn longer_header_keeps_existing_ban_duration() {
            let seen = observe(418, Some("240"), r#"{"code":-1003}"#, true, false).await;
            assert_eq!(event(&seen.events, UPSTREAM)["retry_after_ms"], "240000");
            assert_eq!(event(&seen.events, BAN)["next_wait_ms"], "240000");
            assert_eq!(seen.until_ms, Some(241_000));
        }

        #[tokio::test]
        async fn unsigned_429_also_observes_numeric_code_without_retrying_non_idempotent_write() {
            let seen = observe(
                429,
                Some("5"),
                r#"{"code":-1003,"msg":"SENSITIVE_SENTINEL"}"#,
                false,
                false,
            )
            .await;
            let upstream = event(&seen.events, UPSTREAM);
            assert_eq!(upstream["http_status"], "429");
            assert_eq!(upstream["upstream_error_code"], "-1003");
            assert_eq!(seen.until_ms, Some(6_000));
            assert!(seen.error.contains("not retried"));
        }

        #[tokio::test]
        async fn upstream_observation_survives_cooldown_persistence_failure() {
            let seen = observe(418, Some("5"), r#"{"code":-1003}"#, true, true).await;
            let upstream = event(&seen.events, UPSTREAM);
            assert_eq!(upstream["http_status"], "418");
            assert_eq!(upstream["retry_after"], "5");
            // Failed safety settlement does not await optional body diagnostics.
            assert!(!upstream.contains_key("upstream_error_code"));
            assert!(!seen
                .events
                .iter()
                .any(|event| event.get("message").is_some_and(|v| v == BAN)));
            assert!(seen.error.contains("binance_ip_ban_cooldowns"));
        }
    }

    pub(crate) struct TestEnvVar {
        name: String,
        previous: Option<OsString>,
    }

    impl TestEnvVar {
        pub(crate) fn set(name: String, value: &str) -> Self {
            let previous = std::env::var_os(&name);
            std::env::set_var(&name, value);
            Self { name, previous }
        }

        /// 临时**清掉**一个变量，`Drop` 时按原值复原。断言默认值的用例需要它：
        /// 从外部继承一个 `BINANCE_HTTP_TIMEOUT_MS` 就会把"默认是 10s"测成别的数。
        pub(crate) fn unset(name: String) -> Self {
            let previous = std::env::var_os(&name);
            std::env::remove_var(&name);
            Self { name, previous }
        }
    }

    /// 进程级 env 的**串行锁**。改 `BINANCE_HTTP_*` 这类进程级变量的用例
    /// 必须互斥，否则同一个测试二进制里的并发用例会互相擦配置
    /// （段级复评的测试缺口 #9：R5 的 deadline 用例与默认值用例正好抢同一组）。
    ///
    /// 用毒化后仍可用的取法：上一个持锁用例 panic 不应把其余用例全带红。
    fn env_serial_lock() -> &'static std::sync::Mutex<()> {
        static ENV_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        ENV_LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    pub(crate) fn env_serial_guard() -> std::sync::MutexGuard<'static, ()> {
        env_serial_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `env_serial_guard` 的**非阻塞探针**：`None` 表示锁正被别处持有。只供
    /// 「持锁形状」用例断言自己拿到的环境确实握着这把锁；不要用它代替
    /// `env_serial_guard` 去改 env。
    pub(crate) fn env_serial_try_guard() -> Option<std::sync::MutexGuard<'static, ()>> {
        match env_serial_lock().try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }

    impl Drop for TestEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.as_ref() {
                std::env::set_var(&self.name, previous);
            } else {
                std::env::remove_var(&self.name);
            }
        }
    }

    #[derive(Default)]
    struct ManualRetryClock {
        now_ms: AtomicI64,
        sleeps_ms: Mutex<Vec<u64>>,
    }

    impl ManualRetryClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: AtomicI64::new(now_ms),
                sleeps_ms: Mutex::new(Vec::new()),
            }
        }

        fn sleeps(&self) -> Vec<u64> {
            self.sleeps_ms.lock().expect("lock sleeps").clone()
        }

        fn advance_ms(&self, millis: i64) {
            self.now_ms.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl Clock for ManualRetryClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
                self.sleeps_ms.lock().expect("lock sleeps").push(millis);
                self.now_ms
                    .fetch_add(i64::try_from(millis).unwrap_or(i64::MAX), Ordering::SeqCst);
            })
        }
    }

    fn test_client(
        response_body: &str,
        credential_id: &str,
    ) -> (BinanceFuturesClient, Arc<Mutex<Vec<String>>>) {
        let (client, requests, _) = test_client_responses(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: response_body.to_string(),
            }],
            credential_id,
            Arc::new(ManualRetryClock::new(1_000)),
        );
        (client, requests)
    }

    fn test_client_responses(
        responses: Vec<MockResponse>,
        credential_id: &str,
        clock: Arc<dyn Clock>,
    ) -> (
        BinanceFuturesClient,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Vec<RetryRecord>>>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let retry_records = Arc::new(Mutex::new(Vec::new()));
        (
            BinanceFuturesClient {
                base_url: "https://local.stub.invalid".to_string(),
                egress_scope: format!("test-egress:{credential_id}"),
                credential_id: credential_id.to_string(),
                request_limiter: CredentialRateLimiter::new(TokenBucketConfig::for_test(
                    10_000.0, 10_000.0,
                ))
                .with_clock(clock.clone()),
                order_count_limiter: OrderCountLimiter::isolated(OrderCountConfig::for_test(
                    1_200, 300,
                )),
                exit_weight_reserve: 0.0,
                protectable_exit_budget_1m: protectable_exit_weight_per_position(),
                exposure_cache: Arc::new(tokio::sync::Mutex::new(Some(EgressExposureCache {
                    open_positions: 0,
                    checked_at_ms: 0,
                }))),
                http: reqwest::Client::new(),
                keys: BinanceKeys {
                    api_key: "local-test-key".to_string(),
                    api_secret: "local-test-secret".to_string(),
                },
                recv_window_ms: 5_000,
                clock,
                cooldown_store: None,
                settlements: Settlements::new(),
                admission_exposure_error: None,
                header_observer: None,
                gate_wait_observer: None,
                observation_audit: Arc::new(Mutex::new(Vec::new())),
                mock_http: Some(MockHttp {
                    requests: requests.clone(),
                    responses: Arc::new(Mutex::new(responses.into())),
                    retry_records: retry_records.clone(),
                    order_count_headers: None,
                    on_request: None,
                }),
            },
            requests,
            retry_records,
        )
    }

    /// Build the in-memory transport around the same generic bucket and
    /// governor defaults used by production.  The returned limiter lets a
    /// fake-clock test apply the refill implied by elapsed fake time without
    /// exposing a production-only setter on `BinanceFuturesClient`.
    pub(crate) fn l3_governor_default_test_client(
        responses: Vec<(u16, String)>,
        credential_id: &str,
        clock: Arc<dyn Clock>,
    ) -> (
        BinanceFuturesClient,
        CredentialRateLimiter,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Vec<(String, i64)>>>,
    ) {
        let responses = responses
            .into_iter()
            .map(|(status, body)| MockResponse {
                status,
                retry_after: None,
                body,
            })
            .collect();
        let (mut client, requests, _) =
            test_client_responses(responses, credential_id, clock.clone());
        let request_times = Arc::new(Mutex::new(Vec::new()));
        let hook_times = request_times.clone();
        let hook_clock = clock.clone();
        client
            .mock_http
            .as_mut()
            .expect("L3 governor test client has mock transport")
            .on_request = Some(Arc::new(move |path| {
            hook_times
                .lock()
                .expect("lock timestamped request log")
                .push((path, hook_clock.now_ms()));
            Box::pin(async {})
        }));
        let limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::from_env(None),
            EgressGovernorConfig::from_env(),
            clock,
        );
        client.request_limiter = limiter.clone();
        client.exit_weight_reserve = exit_weight_reserve_from_env();
        (client, limiter, requests, request_times)
    }

    fn test_client_responses_with_cooldown_store(
        responses: Vec<MockResponse>,
        credential_id: &str,
        egress_scope: &str,
        clock: Arc<dyn Clock>,
        store: Store,
    ) -> (
        BinanceFuturesClient,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Vec<RetryRecord>>>,
    ) {
        let (mut client, requests, records) =
            test_client_responses(responses, credential_id, clock);
        client.egress_scope = egress_scope.to_string();
        client.cooldown_store = Some(store);
        (client, requests, records)
    }

    fn test_limiter() -> CredentialRateLimiter {
        CredentialRateLimiter::new(TokenBucketConfig::for_test(10.0, 10.0))
    }

    fn test_limiter_without_refill() -> CredentialRateLimiter {
        CredentialRateLimiter::new(TokenBucketConfig::for_test(10.0, 0.0))
    }

    async fn retry_test_store() -> (Store, std::path::PathBuf) {
        INSTALL_TEST_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-t38-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect retry test store");
        (store, path)
    }

    async fn cleanup_retry_test_store(store: Store, path: std::path::PathBuf) {
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    async fn assert_concurrent_cooldown_upsert_keeps_later_deadline(
        store_a: Store,
        store_b: Store,
        table: CooldownTable,
        scope: &str,
    ) {
        let read_store = store_a.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let short = {
            let barrier = barrier.clone();
            let scope = scope.to_string();
            tokio::spawn(async move {
                barrier.wait().await;
                persist_cooldown(&store_a, table, &scope, 120_000, 100_000).await
            })
        };
        let long = {
            let barrier = barrier.clone();
            let scope = scope.to_string();
            tokio::spawn(async move {
                barrier.wait().await;
                persist_cooldown(&store_b, table, &scope, 180_000, 100_001).await
            })
        };
        short
            .await
            .expect("join the short cooldown writer")
            .expect("persist the short cooldown");
        long.await
            .expect("join the long cooldown writer")
            .expect("persist the long cooldown");

        let name = table.table();
        let column = table.column();
        let query = format!(
            "SELECT CAST({column} AS CHAR) AS deadline_text FROM {name} WHERE egress_scope = ?"
        );
        let row = sqlx::query(&query)
            .bind(scope)
            .fetch_one(read_store.pool())
            .await
            .expect("read the persisted cooldown deadline");
        assert_eq!(
            crate::store::try_get_i64_text(&row, "deadline_text")
                .expect("decode the persisted cooldown deadline"),
            180_000,
            "{name} must retain the later concurrent deadline"
        );
    }

    async fn insert_active_exposures(store: &Store, credential_id: &str, count: u64) {
        for index in 0..count {
            let nonce = ulid::Ulid::new();
            let position_id = format!("pos-a7-{credential_id}-{index}-{nonce}");
            let bot_uid = format!("bot-a7-{credential_id}-{index}-{nonce}");
            let active_key = format!("{bot_uid}|binance|futures|BTC|USDT|USDT|long");
            sqlx::query(
                "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, \
                 direction, entry_epoch, state, active_key, qty, updated_at_ms) \
                 VALUES (?, ?, ?, 'binance|futures|BTC|USDT|USDT', 'long', 1, 'open', ?, '1', 1)",
            )
            .bind(position_id)
            .bind(credential_id)
            .bind(bot_uid)
            .bind(active_key)
            .execute(store.pool())
            .await
            .expect("insert active A7 exposure");
        }
    }

    #[tokio::test]
    async fn exposure_count_is_scope_local_skips_simulation_and_charges_mapping_failure() {
        let _serial = env_serial_guard();
        let route = format!("a7-count-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let testnet_credential = format!("a7-testnet-{}", ulid::Ulid::new());
        let mainnet_credential = format!("a7-mainnet-{}", ulid::Ulid::new());
        let broken_credential = format!("a7-broken-{}", ulid::Ulid::new());
        let _testnet =
            TestEnvVar::set(crate::venue::venue_env_name(&testnet_credential), "testnet");
        let _mainnet =
            TestEnvVar::set(crate::venue::venue_env_name(&mainnet_credential), "mainnet");
        let _broken = TestEnvVar::set(
            crate::venue::venue_env_name(&broken_credential),
            "not-a-venue",
        );
        let (store, path) = retry_test_store().await;
        insert_active_exposures(&store, &testnet_credential, 2).await;
        insert_active_exposures(&store, &mainnet_credential, 3).await;
        insert_active_exposures(&store, &broken_credential, 4).await;
        insert_active_exposures(&store, "paper", 5).await;

        let testnet_scope = egress_scope_for_transport_and_route(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
            Some(&route),
        )
        .expect("testnet route scope");
        let mainnet_scope = egress_scope_for_transport_and_route(
            Some(crate::venue::Venue::Mainnet),
            "https://unused.invalid",
            Some(&route),
        )
        .expect("mainnet route scope");
        let simulation_scope =
            egress_scope_for_transport_and_route(None, "https://unused.invalid", Some(&route))
                .expect("simulation-derived route scope");
        assert_eq!(
            open_positions_on_egress(&store, &testnet_scope)
                .await
                .unwrap(),
            6
        );
        assert_eq!(
            open_positions_on_egress(&store, &mainnet_scope)
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            open_positions_on_egress(&store, &simulation_scope)
                .await
                .unwrap(),
            4,
            "only the fail-closed unmappable credential, never paper, is charged here"
        );

        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn two_open_positions_raise_normal_reserve_but_exit_still_spends_immediately() {
        let clock = Arc::new(ManualRetryClock::new(1_000));
        let (mut client, requests, _) = test_client_responses(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            }],
            "a7-dynamic-reserve",
            clock,
        );
        client.egress_scope = format!("a7-dynamic-reserve:{}", ulid::Ulid::new());
        client.request_limiter =
            CredentialRateLimiter::new(TokenBucketConfig::for_test(20.0, 100.0));
        client.exit_weight_reserve = 10.0;
        client.protectable_exit_budget_1m = 100.0;
        *client.exposure_cache.lock().await = Some(EgressExposureCache {
            open_positions: 2,
            checked_at_ms: 1_000,
        });

        let normal = tokio::time::timeout(
            Duration::from_millis(20),
            client.send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::PeriodicReconcile,
            ),
        )
        .await;
        assert!(
            normal.is_err(),
            "20 tokens cover cost+fixed floor but not cost+two-position reserve"
        );
        assert!(requests.lock().expect("request log").is_empty());

        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect("Exit must use the unchanged reserved-token path immediately");
        assert_eq!(requests.lock().expect("request log").len(), 1);
    }

    #[tokio::test]
    async fn a6_i2_soft_parking_delays_normal_transport_but_not_exit_transport() {
        let clock = Arc::new(ManualRetryClock::new(59_500));
        let responses = vec![
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
        ];
        let (mut client, requests, _) =
            test_client_responses(responses, "a6-soft-transport", clock.clone());
        client.request_limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(10_000.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 15_000, 3),
            clock.clone(),
        );
        let scope = client.egress_scope.clone();
        client
            .request_limiter
            .observe_egress_weight(&scope, 2_160, 59_500);

        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect("Exit transport remains immediate during Soft");
        assert_eq!(clock.now_ms(), 59_500);
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
            .expect("Normal transport releases at the next minute boundary");
        assert_eq!(clock.now_ms(), 60_000);
        assert_eq!(requests.lock().expect("request log").len(), 2);
    }

    #[tokio::test]
    async fn a6_i3_hard_rejection_has_zero_normal_transport_while_exit_still_sends() {
        let clock = Arc::new(ManualRetryClock::new(10_000));
        let (mut client, requests, _) = test_client_responses(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            }],
            "a6-hard-transport",
            clock,
        );
        client.request_limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(10_000.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 15_000, 3),
            client.clock.clone(),
        );
        let scope = client.egress_scope.clone();
        client
            .request_limiter
            .observe_egress_weight(&scope, 2_328, 10_000);
        let error = client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &[("symbol".to_string(), "BTCUSDT".to_string())],
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect_err("Hard must reject even a zero-IP-weight Entry locally");
        assert!(
            error.to_string().contains("EGRESS_WEIGHT_BUDGET_EXHAUSTED"),
            "{error:#}"
        );
        assert!(requests.lock().expect("request log").is_empty());
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect("Exit still uses the protected local reserve during Hard");
        assert_eq!(requests.lock().expect("request log").len(), 1);
    }

    #[tokio::test]
    async fn a6_i7_queue_timeout_uses_the_existing_error_path_with_zero_transport() {
        let clock = Arc::new(ManualRetryClock::new(10_000));
        let (mut client, requests, _) = test_client_responses(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            }],
            "a6-timeout-transport",
            clock,
        );
        client.request_limiter = CredentialRateLimiter::new_for_governor_test(
            TokenBucketConfig::for_test(1.0, 0.0),
            EgressGovernorConfig::for_test(2_400, 1_000, 3),
            client.clock.clone(),
        );
        let scope = client.egress_scope.clone();
        client.request_limiter.set_stored_tokens(&scope, 0.0).await;

        let error = client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
            .expect_err("a timed-out Normal request must return the existing error path");
        assert!(
            error.to_string().contains("egress queue timed out"),
            "{error:#}"
        );
        assert!(requests.lock().expect("request log").is_empty());
    }

    #[tokio::test]
    async fn production_build_wires_the_env_reserve_into_normal_but_not_exit() {
        let _serial = env_serial_guard();
        let route = format!("r2b-reserve-wire-{}", ulid::Ulid::new());
        let credential = format!("r2b-reserve-wire-cred-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _venue = TestEnvVar::set(crate::venue::venue_env_name(&credential), "testnet");
        let _reserve = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE".to_string(), "25");
        let _rate = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(), "1");
        let _burst = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST".to_string(), "20");
        let (store, path) = retry_test_store().await;
        let mut client = BinanceFuturesClient::build(
            BinanceKeys {
                api_key: "local-test-key".to_string(),
                api_secret: "local-test-secret".to_string(),
            },
            &credential,
            Some(store.clone()),
        )
        .expect("production build path");
        client.clock = Arc::new(ManualRetryClock::new(100_000));
        let requests = Arc::new(Mutex::new(Vec::new()));
        client.mock_http = Some(MockHttp {
            requests: requests.clone(),
            responses: Arc::new(Mutex::new(
                vec![MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "{}".to_string(),
                }]
                .into(),
            )),
            retry_records: Arc::new(Mutex::new(Vec::new())),
            order_count_headers: None,
            on_request: None,
        });
        assert_eq!(
            open_positions_on_egress(&store, &client.egress_scope)
                .await
                .expect("zero-position production query"),
            0
        );
        assert_eq!(client.normal_exit_weight_reserve().await, 25.0);

        // GET order costs 1. The production-built bucket starts at 20: above
        // cost, below cost + the env floor (26). Normal must wait, while Exit
        // spends immediately from the same production limiter and reaches the
        // mock transport. A five-second cap in acquire_reserving bounds a bad
        // implementation; this observation needs only a short scheduling turn.
        let normal_client = client.clone();
        let normal = tokio::spawn(async move {
            normal_client
                .send_signed_once(
                    "GET",
                    "/fapi/v1/order",
                    &[],
                    RetryScope::Normal,
                    RequestClass::PeriodicReconcile,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            requests.lock().expect("request log").is_empty(),
            "Normal must not reach transport while only the reserve remains"
        );
        assert!(!normal.is_finished(), "Normal must still be waiting");

        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect("Exit spends immediately from the reserved slice");
        assert_eq!(requests.lock().expect("request log").len(), 1);

        normal.abort();
        let _ = normal.await;
        clear_egress_test_state(&client.egress_scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn exposure_cache_requeries_at_one_thousand_milliseconds() {
        let _serial = env_serial_guard();
        let route = format!("a7-cache-{}", ulid::Ulid::new());
        let credential = format!("a7-cache-cred-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let _venue = TestEnvVar::set(crate::venue::venue_env_name(&credential), "testnet");
        let (store, path) = retry_test_store().await;
        insert_active_exposures(&store, &credential, 1).await;
        let clock = Arc::new(ManualRetryClock::new(10_000));
        let (mut client, _, _) = test_client_responses(Vec::new(), &credential, clock.clone());
        client.egress_scope = egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://unused.invalid",
        )
        .expect("derive unique cache scope");
        client.cooldown_store = Some(store.clone());
        *client.exposure_cache.lock().await = None;

        assert_eq!(client.cached_open_positions_on_egress().await, 1);
        insert_active_exposures(&store, &credential, 1).await;
        assert_eq!(
            client.cached_open_positions_on_egress().await,
            1,
            "a still-fresh cache must avoid a second query"
        );
        clock.advance_ms(1_000);
        assert_eq!(
            client.cached_open_positions_on_egress().await,
            2,
            "the cache must be invalid at age 1000ms, not live forever"
        );

        cleanup_retry_test_store(store, path).await;
    }

    #[test]
    fn t311_egress_scope_same_venue_ignores_credential_identity() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let credential_a = "t311-egress-same-venue-a";
        let credential_b = "t311-egress-same-venue-b";
        let _venue_a = TestEnvVar::set(crate::venue::venue_env_name(credential_a), "testnet");
        let _venue_b = TestEnvVar::set(crate::venue::venue_env_name(credential_b), "testnet");

        let scope_a = egress_scope_for(credential_a, "https://unused-a.example.test")
            .expect("configured venue A derives an egress scope");
        let scope_b = egress_scope_for(credential_b, "https://unused-b.example.test")
            .expect("configured venue B derives an egress scope");

        assert_eq!(scope_a, "binance-futures-rest:venue:testnet");
        assert_eq!(scope_b, "binance-futures-rest:venue:testnet");
    }

    #[test]
    fn fault_proxy_transport_keeps_upstream_testnet_egress_scope() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let scope = egress_scope_for_transport(
            Some(crate::venue::Venue::Testnet),
            "https://127.0.0.1:8443",
        )
        .expect("fault proxy endpoint must not replace upstream venue identity");

        assert_eq!(scope, "binance-futures-rest:venue:testnet");
    }

    #[test]
    fn explicit_route_id_names_the_real_public_egress_boundary() {
        let scope = egress_scope_for_transport_and_route(
            Some(crate::venue::Venue::Mainnet),
            "https://fapi.binance.com",
            Some("nat-gateway-hkg-01"),
        )
        .expect("operator route id must be accepted");
        assert_eq!(
            scope,
            "binance-futures-rest:route:nat-gateway-hkg-01:venue:mainnet"
        );
        assert!(egress_scope_for_transport_and_route(
            Some(crate::venue::Venue::Mainnet),
            "https://fapi.binance.com",
            Some("bad route/id"),
        )
        .is_err());
    }

    #[test]
    fn the_reconcile_round_weight_tracks_the_table_instead_of_a_literal() {
        // 四个调用：openOrders 40 + openAlgoOrders 40 + positionRisk 10 + balance 10。
        // 改权重表就必须让这条一起动——脱节会让启动自检的判据失真。
        assert_eq!(periodic_reconcile_round_weight(), 100.0);
    }

    #[test]
    fn protectable_exit_weight_tracks_all_five_endpoint_table_entries() {
        let symbol = vec![("symbol".to_string(), "BTCUSDT".to_string())];
        let table_sum = signed_request_weight("DELETE", "/fapi/v1/allOpenOrders", &symbol)
            + signed_request_weight("DELETE", "/fapi/v1/algoOrder", &symbol)
            + signed_request_weight("POST", "/fapi/v1/order", &symbol)
            + signed_request_weight("GET", "/fapi/v2/positionRisk", &symbol)
            + signed_request_weight("GET", "/fapi/v1/openAlgoOrders", &symbol);
        assert_eq!(protectable_exit_weight_per_position(), table_sum);
        assert_eq!(table_sum, 13.0, "the current table is 1+1+0+10+1");

        let source = include_str!("binance_futures.rs");
        let function_body = source
            .split_once("pub(crate) fn protectable_exit_weight_per_position() -> f64 {")
            .expect("protectable weight function exists")
            .1
            .split_once("\npub(crate) fn protectable_exit_budget_1m")
            .expect("protectable budget function follows weight function")
            .0;
        for table_lookup in [
            "signed_request_weight(\"DELETE\", \"/fapi/v1/allOpenOrders\"",
            "signed_request_weight(\"DELETE\", \"/fapi/v1/algoOrder\"",
            "signed_request_weight(\"POST\", \"/fapi/v1/order\"",
            "signed_request_weight(\"GET\", \"/fapi/v2/positionRisk\"",
            "signed_request_weight(\"GET\", \"/fapi/v1/openAlgoOrders\"",
        ] {
            assert!(
                function_body.contains(table_lookup),
                "per-position weight must retain table lookup {table_lookup}"
            );
        }
        assert!(
            !function_body.contains("13.0"),
            "per-position weight must not regress to today's literal total"
        );
    }

    #[test]
    fn protectable_admission_includes_the_new_position_and_keeps_the_exact_boundary() {
        let weight = 13.0;
        let exact = 3.0 * weight;
        assert_eq!(
            protectable_exposure_admission(2, weight, exact),
            ExposureAdmission::Admit {
                reserved_after: exact
            }
        );
        assert_eq!(
            protectable_exposure_admission(2, weight, exact - f64::EPSILON * exact),
            ExposureAdmission::Reject {
                reserved_after: exact,
                budget_1m: exact - f64::EPSILON * exact,
            }
        );
        assert_eq!(
            protectable_exposure_admission(3, weight, exact),
            ExposureAdmission::Reject {
                reserved_after: 4.0 * weight,
                budget_1m: exact,
            },
            "forgetting the +1 would incorrectly admit this position"
        );
    }

    #[test]
    fn protectable_budget_defaults_to_one_quarter_minute_and_clamps_both_ends() {
        let config = TokenBucketConfig::for_test(500.0, 4.0);
        let weight = protectable_exit_weight_per_position();
        assert_eq!(protectable_exit_budget_1m(&config, None), 60.0);
        assert_eq!(protectable_exit_budget_1m(&config, Some("0")), weight);
        assert_eq!(protectable_exit_budget_1m(&config, Some("9999")), 240.0);
        assert_eq!(
            protectable_exit_budget_1m(&config, Some("not-a-number")),
            60.0
        );
    }

    #[test]
    fn the_budget_provenance_names_which_env_var_is_in_effect() {
        use crate::rate_limiter::TokenBucketConfig;
        let _serial = env_serial_guard();
        let names = [
            "RUSTCORE_BINANCE_WEIGHT_PER_SEC",
            "RUSTCORE_BINANCE_WEIGHT_BURST",
            "RUSTCORE_BINANCE_RPS",
            "RUSTCORE_BINANCE_BURST",
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC",
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST",
            "RUSTCORE_BINANCE_TESTNET_RPS",
            "RUSTCORE_BINANCE_TESTNET_BURST",
        ];
        let _cleared: Vec<_> = names
            .iter()
            .map(|name| TestEnvVar::unset((*name).to_string()))
            .collect();

        // 一个都没配：来源是内建默认，且不该报"legacy"。
        let plain = TokenBucketConfig::describe_from_env(None);
        assert_eq!(plain.rate_source, "built-in default");
        assert!(!plain.legacy_names_only);

        // 只配了旧名字——这正是出事的那个部署形态，必须被标出来。
        {
            let _legacy = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_RPS".to_string(), "1.5");
            let testnet = TokenBucketConfig::describe_from_env(Some(crate::venue::Venue::Testnet));
            assert_eq!(testnet.rate_source, "RUSTCORE_BINANCE_TESTNET_RPS");
            assert!(testnet.legacy_names_only, "只配旧名字必须触发单位变更告警");
            assert_eq!(testnet.config.refill_per_sec, 1.5);

            // 新名字优先于旧名字，且优先之后不再算 legacy。
            let _new = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(), "6");
            let retuned = TokenBucketConfig::describe_from_env(Some(crate::venue::Venue::Testnet));
            assert_eq!(
                retuned.rate_source,
                "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC"
            );
            assert_eq!(retuned.config.refill_per_sec, 6.0);
            assert!(!retuned.legacy_names_only);

            // testnet 名字优先于通用名字。
            let _generic = TestEnvVar::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(), "99");
            let still_testnet =
                TokenBucketConfig::describe_from_env(Some(crate::venue::Venue::Testnet));
            assert_eq!(
                still_testnet.rate_source,
                "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC"
            );
            assert_eq!(still_testnet.config.refill_per_sec, 6.0);
        }
    }

    #[test]
    fn exit_weight_reserve_defaults_and_clamps() {
        let _serial = env_serial_guard();
        let name = "RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE".to_string();
        {
            let _unset = TestEnvVar::unset(name.clone());
            assert_eq!(exit_weight_reserve_from_env(), 10.0);
        }
        // 下限 1.0：预留 0 等于取消预留，平仓要和普通流量抢同一份余额。
        {
            let _v = TestEnvVar::set(name.clone(), "0");
            assert_eq!(exit_weight_reserve_from_env(), 1.0);
        }
        {
            let _v = TestEnvVar::set(name.clone(), "-5");
            assert_eq!(exit_weight_reserve_from_env(), 1.0);
        }
        // 上限 100：预留超过整桶会让普通流量永远等不到令牌。
        {
            let _v = TestEnvVar::set(name.clone(), "1000");
            assert_eq!(exit_weight_reserve_from_env(), 100.0);
        }
        // 畸形值回落到默认值，**不是** 0——静默取消预留是最坏的失败方式。
        {
            let _v = TestEnvVar::set(name, "not-a-number");
            assert_eq!(exit_weight_reserve_from_env(), 10.0);
        }
    }

    #[tokio::test]
    async fn an_exit_is_refused_explicitly_rather_than_queued_when_tokens_run_out() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "4");
        let scope = format!("test-egress:exit-reserve:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let (mut client, requests, _) = test_client_responses(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r##"{"orderId":9201,"clientOrderId":"coid_reserve","status":"NEW","symbol":"BTCUSDT","side":"SELL","origQty":"0.001","executedQty":"0"}"##
                    .to_string(),
            }],
            "cred-exit-reserve",
            clock,
        );
        client.egress_scope = scope.clone();
        client.exit_weight_reserve = 10.0;
        // 桶小到连一次权重 1 的请求都付不起；refill 不为零，好让"改成排队"那种
        // 变异体在毫秒级跑完，而不是把整个套件挂住。
        client.request_limiter =
            CredentialRateLimiter::new(TokenBucketConfig::for_test(0.5, 100.0));

        // 退出路径走 try_acquire_reserved：拿不到令牌就**立刻显式失败**。排队会顶穿
        // 750ms 的退出预算，而调用方对失败还能处置，对挂起不能。
        let refused = client
            .send_signed_once(
                "DELETE",
                "/fapi/v1/order",
                &[("symbol".to_string(), "BTCUSDT".to_string())],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect_err("an exit must fail fast when the egress reserve is gone");
        assert!(
            refused.to_string().contains("egress reserve exhausted"),
            "{refused:#}"
        );
        assert_eq!(
            requests.lock().expect("lock requests").len(),
            0,
            "the refused exit must not reach transport"
        );
        assert_eq!(
            exit_probe_registry()
                .lock()
                .expect("lock exit-probe registry")
                .get(&scope)
                .map(|state| state.probes_spent)
                .unwrap_or(0),
            0,
            "reserve exhaustion must happen before the in-memory probe claim"
        );
        clear_egress_test_state(&scope);
    }

    #[tokio::test]
    async fn reserve_exhaustion_with_a_store_does_not_spend_a_probe() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "4");
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-store-reserve:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);
        let (mut client, requests, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-exit-store-reserve",
            &scope,
            clock,
            store.clone(),
        );
        client.request_limiter =
            CredentialRateLimiter::new(TokenBucketConfig::for_test(0.5, 100.0));

        let refused = client
            .send_signed_once(
                "DELETE",
                "/fapi/v1/order",
                &[("symbol".to_string(), "BTCUSDT".to_string())],
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect_err("reserve exhaustion must reject before the authoritative gate");
        assert!(
            refused.to_string().contains("egress reserve exhausted"),
            "{refused:#}"
        );
        assert_eq!(requests.lock().expect("lock requests").len(), 0);
        assert_eq!(
            client.persisted_probe_spent().await.expect("probe spend"),
            0,
            "reserve exhaustion must not create or increment a probe row"
        );
        assert_eq!(
            exit_probe_registry()
                .lock()
                .expect("lock exit-probe registry")
                .get(&scope)
                .map(|state| state.probes_spent)
                .unwrap_or(0),
            0,
            "the store-backed fast cache must remain untouched too"
        );

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn two_exit_budget_rejections_refund_their_pre_authority_charge() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "1");
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-no-charge:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);
        let (mut client, requests, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-exit-no-charge",
            &scope,
            clock,
            store.clone(),
        );
        client.request_limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(3.0, 0.0));
        client.request_limiter.refund(&scope, 1.0).await;
        assert!(client
            .claim_exit_probe(160_000, 100_000)
            .await
            .expect("spend budget"));
        let before = client.request_limiter.stored_tokens(&scope).await;
        for index in 0..2 {
            let error = client
                .send_signed_once(
                    "DELETE",
                    "/fapi/v1/algoOrder",
                    &[("algoId".to_string(), format!("{index}"))],
                    RetryScope::Exit,
                    RequestClass::Protection,
                )
                .await
                .expect_err("exhausted probe budget must reject locally");
            assert!(error.to_string().contains("budget exhausted"), "{error:#}");
            assert_eq!(
                client.request_limiter.stored_tokens(&scope).await,
                before,
                "rejection {index} must refund the exact pre-authority charge"
            );
        }
        assert_eq!(client.request_limiter.stored_tokens(&scope).await, before);
        assert_eq!(requests.lock().expect("lock requests").len(), 0);
        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn an_admitted_exit_spends_one_token_and_one_probe_exactly_once() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "4");
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-single-charge:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);
        let (mut client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            }],
            "cred-exit-single-charge",
            &scope,
            clock,
            store.clone(),
        );
        client.request_limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(3.0, 0.0));
        client.request_limiter.refund(&scope, 1.0).await;
        let weight = signed_request_weight(
            "DELETE",
            "/fapi/v1/algoOrder",
            &[("algoId".to_string(), "7".to_string())],
        );
        let before = client
            .request_limiter
            .stored_tokens(&scope)
            .await
            .expect("initialized bucket");

        client
            .send_signed_once(
                "DELETE",
                "/fapi/v1/algoOrder",
                &[("algoId".to_string(), "7".to_string())],
                RetryScope::Exit,
                RequestClass::Protection,
            )
            .await
            .expect("in-budget exit reaches transport");

        assert_eq!(requests.lock().expect("lock requests").len(), 1);
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            Some(before - weight),
            "the successful path must charge exactly the endpoint weight once"
        );
        assert_eq!(
            client.persisted_probe_spent().await.expect("probe spend"),
            1,
            "the successful path must claim exactly one probe"
        );

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn normal_request_refunds_tokens_when_a_concurrent_418_wins_the_authoritative_gate() {
        let (mut client, requests, _) = test_client_responses(
            vec![],
            "cred-normal-refund",
            Arc::new(ManualRetryClock::new(100_000)),
        );
        let scope = format!("test-egress:normal-refund:{}", ulid::Ulid::new());
        client.egress_scope = scope.clone();
        client.request_limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(3.0, 0.0));
        client.request_limiter.refund(&scope, 1.0).await;
        let gate = client.transport_gate().expect("transport gate");
        let held = gate.lock().await;
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .send_signed_once(
                    "GET",
                    "/fapi/v1/order",
                    &[],
                    RetryScope::Normal,
                    RequestClass::PeriodicReconcile,
                )
                .await
        });
        tokio::task::yield_now().await;
        ip_ban_registry()
            .lock()
            .expect("lock IP ban")
            .insert(scope.clone(), 160_000);
        drop(held);
        let error = request.await.expect("join").expect_err("418 must reject");
        assert!(
            error.to_string().contains("IP ban cooldown active"),
            "{error:#}"
        );
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            Some(3.0)
        );
        assert_eq!(requests.lock().expect("lock requests").len(), 0);
        clear_egress_test_state(&scope);
    }

    #[tokio::test]
    async fn admitted_requests_wait_at_transport_gate_beyond_fifteen_seconds_with_honest_timing() {
        use tracing::instrument::WithSubscriber;

        async fn exercise(class: RequestClass, cancel: bool) {
            let (mut client, requests) = test_client("{}", "gate-timing-local");
            client.egress_scope = format!("test-egress:timing:{}", ulid::Ulid::new());
            let scope = client.egress_scope.clone();
            let gate = client.transport_gate().expect("production transport gate");
            let held = gate.lock().await;
            let (dispatch, records) = crate::reconcile_trace::tests::capture();
            let request_client = client.clone();
            let task = tokio::spawn(
                crate::reconcile_trace::scope(async move {
                    request_client
                        .send_signed_once("GET", "/fapi/v1/order", &[], RetryScope::Normal, class)
                        .await
                })
                .with_subscriber(dispatch),
            );

            // Observe real token consumption before measuring the blocked
            // transport. This excludes a future that has never been polled.
            tokio::time::timeout(Duration::from_secs(2), async {
                while client.request_limiter.stored_tokens(&scope).await != Some(9_999.0) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("request must first be admitted");
            assert_eq!(client.request_limiter.queued(&scope), 0);
            assert!(requests.lock().unwrap().is_empty());
            tokio::time::sleep(Duration::from_millis(15_050)).await;
            assert!(
                !task.is_finished(),
                "admission deadline must not remove the shared transport gate"
            );
            assert_eq!(client.request_limiter.queued(&scope), 0);
            assert!(
                requests.lock().unwrap().is_empty(),
                "mock transport must not run while the gate is held"
            );
            assert!(
                records.lock().unwrap().is_empty(),
                "one attempt emits only when it returns or drops"
            );

            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                drop(held);
            } else {
                drop(held);
                let response = task
                    .await
                    .expect("join request")
                    .expect("released request succeeds");
                assert_eq!(response.status, 200);
            }
            assert_eq!(requests.lock().unwrap().len(), usize::from(!cancel));
            let events = records.lock().unwrap();
            assert_eq!(events.len(), 1, "class={class:?}, cancel={cancel}");
            let event = &events[0];
            assert_eq!(event["event"], "http_attempt");
            assert_eq!(event["request_class"], class.as_str());
            assert_eq!(event["path"], "/fapi/v1/order");
            assert_eq!(event["attempt_id"], "1");
            assert!(event["transport_gate_ms"].parse::<u64>().unwrap() >= 15_000);
            assert_eq!(event["outcome"], if cancel { "dropped" } else { "ok" });
            assert_eq!(
                event["stage"],
                if cancel { "transport_gate" } else { "complete" }
            );
            assert!(!event
                .values()
                .any(|value| value.contains("local-test-secret") || value.contains("signature=")));
            clear_egress_test_state(&scope);
        }

        // Independent scopes run concurrently, keeping the test's real wall
        // time near 15s while checking release and cancellation for both classes.
        tokio::join!(
            exercise(RequestClass::PeriodicReconcile, false),
            exercise(RequestClass::PeriodicReconcile, true),
            exercise(RequestClass::EventTruth, false),
            exercise(RequestClass::EventTruth, true),
        );
    }

    #[test]
    fn order_count_only_observation_takes_its_info_window_without_taking_weight_window() {
        let (mut client, _) = test_client("{}", "cred-meter-order-only");
        client.egress_scope = format!("test-egress:meter:{}", ulid::Ulid::new());
        client.record_meter_observations(
            "/fapi/v1/order",
            &HttpAttemptResponse {
                status: 200,
                retry_after: None,
                used_weight_1m: None,
                order_count_1m: Some(7),
                order_count_10s: Some(2),
                body: "{}".to_string(),
            },
        );
        assert!(
            !client.should_log_local_block("meter_observation_order_count", 1_000),
            "the ORDER-only response must have consumed the ORDER info window"
        );
        assert!(
            client.should_log_local_block("meter_observation_weight", 1_000),
            "ORDER visibility must not consume the independent weight window"
        );
        let source = include_str!("binance_futures.rs")
            .split("#[cfg(test)]\npub(crate) mod tests")
            .next()
            .expect("production source");
        assert_eq!(
            source
                .matches("self.record_meter_observations(path, &response);")
                .count(),
            2,
            "signed and unsigned production paths must both record parsed meters"
        );
    }

    #[tokio::test]
    async fn public_cooldown_production_entry_keeps_using_the_process_lifetime_store_after_two_connects(
    ) {
        let _serial = env_serial_guard();
        let route = format!("lifecycle-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let (first, first_path) = retry_test_store().await;
        let (second, second_path) = retry_test_store().await;
        cleanup_retry_test_store(first, first_path).await;
        cleanup_retry_test_store(second, second_path).await;

        let scope = publish_public_endpoint_cooldown(
            Some(crate::venue::Venue::Testnet),
            418,
            120_000,
            1_788_000_000_000,
        )
        .await
        .expect("production entry writes through the lifecycle store");
        let store = registered_cooldown_store().expect("lifecycle store");
        let row =
            sqlx::query("SELECT egress_scope FROM binance_ip_ban_cooldowns WHERE egress_scope=?")
                .bind(&scope)
                .fetch_one(store.pool())
                .await
                .expect("cooldown row remains queryable");
        assert_eq!(row.get::<String, _>("egress_scope"), scope);
        clear_egress_test_state(&scope);
    }

    #[test]
    fn store_connect_source_keeps_the_production_cooldown_install_call() {
        let source = include_str!("store.rs");
        let connect_body = source
            .split("pub async fn connect(db_url: &str)")
            .nth(1)
            .expect("Store::connect body")
            .split("async fn connect_unregistered")
            .next()
            .expect("Store::connect end");
        assert!(
            connect_body.contains("install_cooldown_store(store.clone())"),
            "production Store::connect must install the cooldown store"
        );
    }

    #[tokio::test]
    /// 这条只证明 **key 与两张表的语义**（落在凭据路径会读的同一个 scope、只增不减、
    /// 418/429 分表、非限流状态被拒）。这条用显式 store 隔离验证表语义；
    /// 生产入口与进程生命周 store 的行为接线由 lifecycle 用例钉住。
    async fn a_public_endpoint_ban_lands_where_the_credentialed_path_reads_it() {
        let _serial = env_serial_guard();
        let route_id = format!("pub-cooldown-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route_id);
        let now = 1_788_000_000_000_i64;

        // 公共端点的 418 必须落在凭据路径**会去读的那个 key** 上，否则等于没写。
        let scope = publish_public_endpoint_cooldown_with_store(
            None,
            Some(crate::venue::Venue::Testnet),
            418,
            120_000,
            now,
        )
        .await
        .expect("a public 418 must be publishable");
        assert_eq!(
            scope,
            egress_scope_for_transport(Some(crate::venue::Venue::Testnet), "https://unused")
                .expect("credentialed scope")
        );
        assert_eq!(
            ip_ban_registry().lock().expect("lock").get(&scope).copied(),
            Some(now + 120_000)
        );

        // 只增不减：并发观察里短的截止时间不得覆盖长的。
        publish_public_endpoint_cooldown_with_store(
            None,
            Some(crate::venue::Venue::Testnet),
            418,
            1_000,
            now,
        )
        .await
        .expect("second publish");
        assert_eq!(
            ip_ban_registry().lock().expect("lock").get(&scope).copied(),
            Some(now + 120_000)
        );

        // 429 走另一张表，两者不得互相覆盖。
        let rate_limit_scope = publish_public_endpoint_cooldown_with_store(
            None,
            Some(crate::venue::Venue::Testnet),
            429,
            5_000,
            now,
        )
        .await
        .expect("a public 429 must be publishable");
        assert_eq!(rate_limit_scope, scope);
        assert_eq!(
            rate_limit_registry()
                .lock()
                .expect("lock")
                .get(&scope)
                .copied(),
            Some(now + 5_000)
        );
        assert_eq!(
            ip_ban_registry().lock().expect("lock").get(&scope).copied(),
            Some(now + 120_000)
        );

        // 非限流状态必须被拒绝，而不是悄悄记成一次封禁。
        assert!(publish_public_endpoint_cooldown_with_store(
            None,
            Some(crate::venue::Venue::Testnet),
            500,
            1_000,
            now
        )
        .await
        .is_err());

        ip_ban_registry().lock().expect("lock").remove(&scope);
        rate_limit_registry().lock().expect("lock").remove(&scope);
    }

    #[tokio::test]
    async fn public_ban_reaches_an_unconfigured_credential_through_its_base_url_key() {
        let _serial = env_serial_guard();
        let credential = format!("unconfigured-public-ban-{}", ulid::Ulid::new());
        let _route = TestEnvVar::unset("BINANCE_EGRESS_ROUTE_ID".to_string());
        let _venue = TestEnvVar::unset(crate::venue::venue_env_name(&credential));
        let _base_url = TestEnvVar::unset("BINANCE_FUTURES_BASE_URL".to_string());
        let (store, path) = retry_test_store().await;
        let now_ms = crate::svc::now_ms();
        let primary = public_endpoint_egress_scope(Some(crate::venue::Venue::Mainnet))
            .expect("public venue scope");
        let credential_scope =
            egress_scope_for_transport(None, crate::venue::Venue::Mainnet.rest_base_url())
                .expect("legacy unconfigured credential scope");
        assert_ne!(primary, credential_scope);

        publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Mainnet),
            418,
            120_000,
            now_ms,
        )
        .await
        .expect("publish public mainnet ban");
        // A shorter observation must not reduce either memory key or either
        // persisted row.
        publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Mainnet),
            418,
            1_000,
            now_ms,
        )
        .await
        .expect("publish shorter public mainnet ban");
        for scope in [&primary, &credential_scope] {
            assert_eq!(
                ip_ban_registry().lock().expect("lock").get(scope).copied(),
                Some(now_ms + 120_000)
            );
            let row = sqlx::query(
                "SELECT CAST(ban_until_ms AS CHAR) AS until_text \
                 FROM binance_ip_ban_cooldowns WHERE egress_scope = ?",
            )
            .bind(scope)
            .fetch_one(store.pool())
            .await
            .expect("both public 418 keys must be persisted");
            assert_eq!(
                crate::store::try_get_i64_text(&row, "until_text").expect("decode 418 cutoff"),
                now_ms + 120_000
            );
        }

        publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Mainnet),
            429,
            5_000,
            now_ms,
        )
        .await
        .expect("publish public mainnet rate limit");
        publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Mainnet),
            429,
            1_000,
            now_ms,
        )
        .await
        .expect("publish shorter public mainnet rate limit");
        for scope in [&primary, &credential_scope] {
            assert_eq!(
                rate_limit_registry()
                    .lock()
                    .expect("lock")
                    .get(scope)
                    .copied(),
                Some(now_ms + 5_000)
            );
            let row = sqlx::query(
                "SELECT CAST(cooldown_until_ms AS CHAR) AS until_text \
                 FROM binance_rate_limit_cooldowns WHERE egress_scope = ?",
            )
            .bind(scope)
            .fetch_one(store.pool())
            .await
            .expect("both public 429 keys must be persisted");
            assert_eq!(
                crate::store::try_get_i64_text(&row, "until_text").expect("decode 429 cutoff"),
                now_ms + 5_000
            );
        }

        let client = BinanceFuturesClient::new_with_store(
            BinanceKeys {
                api_key: "local-test-key".to_string(),
                api_secret: "local-test-secret".to_string(),
            },
            &credential,
            store.clone(),
        )
        .expect("build legacy unconfigured credential client");
        assert_eq!(client.egress_scope, credential_scope);
        let error = client
            .ensure_not_ip_banned(
                "/fapi/v2/balance",
                RetryScope::Normal,
                EgressGuardPhase::Authoritative,
            )
            .await
            .expect_err("the public 418 must stop the base-url credential path");
        assert!(error.to_string().contains("IP ban cooldown active"));

        clear_egress_test_state(&primary);
        clear_egress_test_state(&credential_scope);
        drop(client);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn explicit_route_publishes_exactly_one_cooldown_key() {
        let _serial = env_serial_guard();
        let route = format!("single-public-key-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let (store, path) = retry_test_store().await;
        let now_ms = 1_788_000_000_000_i64;
        let scopes = public_endpoint_egress_scopes(Some(crate::venue::Venue::Testnet))
            .expect("derive published scopes");
        assert_eq!(
            scopes.len(),
            1,
            "a route id is the complete egress identity"
        );
        let scope = publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Testnet),
            418,
            120_000,
            now_ms,
        )
        .await
        .expect("publish route-scoped ban");
        let rows =
            sqlx::query("SELECT egress_scope FROM binance_ip_ban_cooldowns ORDER BY egress_scope")
                .fetch_all(store.pool())
                .await
                .expect("read persisted public scopes");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String, _>("egress_scope"), scope);
        assert_eq!(
            ip_ban_registry()
                .lock()
                .expect("lock")
                .keys()
                .filter(|key| key.contains(&route))
                .count(),
            1,
            "the in-memory registry must use the same one-key route identity"
        );

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    /// **接线证明（d）**：生产入口必须从进程级注册槽位取 store 再委派给
    /// `_with_store`。行为接线已由稳定的进程生命周 store 用例覆盖；
    /// 本源码形状守卫保留为真实调用点的独立检查。
    #[test]
    fn the_public_cooldown_entry_reads_the_registered_store() {
        let source = include_str!("binance_futures.rs");
        let entry_start = source
            .find("pub(crate) async fn publish_public_endpoint_cooldown(")
            .expect("the production publish entry exists");
        let entry_end = source[entry_start..]
            .find("pub(crate) async fn publish_public_endpoint_cooldown_with_store(")
            .map(|offset| entry_start + offset)
            .expect("the _with_store variant follows the entry");
        let entry = &source[entry_start..entry_end];
        assert!(
            entry.contains("registered_cooldown_store().ok()"),
            "the production entry must take the store from the process-level registry: {entry}"
        );
        assert!(
            entry.contains("publish_public_endpoint_cooldown_with_store(store.as_ref()"),
            "the production entry must hand that store to the _with_store variant: {entry}"
        );
    }

    #[test]
    fn endpoint_weights_charge_full_account_truth_queries() {
        let no_params = Vec::<(String, String)>::new();
        let symbol = vec![("symbol".to_string(), "BTCUSDT".to_string())];
        assert_eq!(
            signed_request_weight("GET", "/fapi/v1/openOrders", &no_params),
            40.0
        );
        assert_eq!(
            signed_request_weight("GET", "/fapi/v1/openAlgoOrders", &no_params),
            40.0
        );
        assert_eq!(
            signed_request_weight("GET", "/fapi/v1/openOrders", &symbol),
            1.0
        );
        assert_eq!(
            signed_request_weight("GET", "/fapi/v1/userTrades", &symbol),
            5.0
        );
        // 冲突未解时取高值：低估让治理器以为还有余量而继续发，正是 418 的成因。
        assert_eq!(
            signed_request_weight("GET", "/fapi/v2/positionRisk", &no_params),
            10.0
        );
        assert_eq!(
            signed_request_weight("GET", "/fapi/v2/balance", &no_params),
            10.0
        );
        assert_eq!(
            signed_request_weight("POST", "/fapi/v1/order", &symbol),
            0.0
        );
        assert_eq!(
            signed_request_weight("POST", "/fapi/v1/algoOrder", &symbol),
            0.0
        );
        assert_eq!(signed_request_weight("GET", "/fapi/v1/order", &symbol), 1.0);
        assert_eq!(
            signed_request_weight("DELETE", "/fapi/v1/order", &symbol),
            1.0
        );
        assert_eq!(
            signed_request_weight("GET", "/fapi/v1/algoOrder", &symbol),
            1.0
        );
        assert_eq!(
            signed_request_weight("DELETE", "/fapi/v1/algoOrder", &symbol),
            1.0
        );
    }

    #[tokio::test]
    async fn order_posts_touch_no_ip_tokens_but_reads_and_deletes_keep_their_weight() {
        let (lifecycle_store, lifecycle_path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(1_000));
        let responses = (0..5)
            .map(|_| MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            })
            .collect();
        let (mut client, requests, _) =
            test_client_responses(responses, "weight-zero-order", clock);
        client.egress_scope = format!("weight-zero-order:{}", ulid::Ulid::new());
        client.request_limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(20.0, 0.0));
        let scope = client.egress_scope.clone();
        let symbol = vec![("symbol".to_string(), "BTCUSDT".to_string())];
        let wire = crate::protection_dispatch::AlgoWire {
            symbol: "BTCUSDT".to_owned(),
            side: "SELL".to_owned(),
            order_type: "STOP_MARKET".to_owned(),
            trigger_price: "90".to_owned(),
            quantity: Some("1".to_owned()),
            reduce_only: true,
            close_position: false,
            client_algo_id: "weight-zero-order-algo".to_owned(),
        };
        let mut permit =
            crate::protection_dispatch::tests::fixture_permit("weight-zero-order", wire.clone())
                .await;
        let algo_params = vec![
            ("symbol".to_owned(), wire.symbol.clone()),
            ("side".to_owned(), wire.side.clone()),
            ("algoType".to_owned(), "CONDITIONAL".to_owned()),
            ("type".to_owned(), wire.order_type.clone()),
            ("triggerPrice".to_owned(), wire.trigger_price.clone()),
            ("quantity".to_owned(), wire.quantity.clone().unwrap()),
            ("reduceOnly".to_owned(), "true".to_owned()),
            ("workingType".to_owned(), "MARK_PRICE".to_owned()),
            ("priceProtect".to_owned(), "true".to_owned()),
            ("clientAlgoId".to_owned(), wire.client_algo_id.clone()),
        ];

        client.cooldown_store = Some(lifecycle_store.clone());
        client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &symbol,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect("standard order POST");
        client
            .send_signed_once_authorized(
                "POST",
                "/fapi/v1/algoOrder",
                &algo_params,
                RetryScope::Exit,
                RequestClass::Protection,
                &mut SignedAuthority::Protection {
                    permit: &mut permit,
                    wire: &wire,
                },
            )
            .await
            .expect("algo order POST");
        {
            let sent = requests.lock().expect("captured order POSTs");
            assert_eq!(sent.len(), 2, "both POSTs must reach the transport");
            assert_eq!(request_path(&sent[0]), "/fapi/v1/order");
            assert_eq!(request_path(&sent[1]), "/fapi/v1/algoOrder");
        }
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            None,
            "both zero-weight order POSTs must leave the IP bucket untouched"
        );

        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &symbol,
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
            .expect("order query");
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            Some(19.0)
        );
        client
            .send_signed_once(
                "DELETE",
                "/fapi/v1/algoOrder",
                &symbol,
                RetryScope::Exit,
                RequestClass::Protection,
            )
            .await
            .expect("algo cancel");
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            Some(18.0)
        );
        client
            .send_signed_once(
                "GET",
                "/fapi/v2/balance",
                &[],
                RetryScope::Normal,
                RequestClass::PeriodicReconcile,
            )
            .await
            .expect("balance truth query");
        assert_eq!(
            client.request_limiter.stored_tokens(&scope).await,
            Some(8.0)
        );
        cleanup_retry_test_store(lifecycle_store, lifecycle_path).await;
    }

    #[tokio::test]
    async fn exhausted_order_budget_parks_normal_without_transport_but_never_blocks_exit() {
        let (lifecycle_store, lifecycle_path) = retry_test_store().await;
        let now_ms = 1_000;
        let clock = Arc::new(ManualRetryClock::new(now_ms));
        let responses = vec![
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
        ];
        let (mut client, requests, _) =
            test_client_responses(responses, "order-normal-vs-exit", clock);
        client.order_count_limiter = OrderCountLimiter::isolated(OrderCountConfig::for_test(1, 1));
        let params = vec![("symbol".to_string(), "BTCUSDT".to_string())];

        client.cooldown_store = Some(lifecycle_store.clone());
        client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect("first normal order is admitted");
        let parked = client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect_err("the second normal order must park immediately");
        assert!(
            parked.to_string().contains("ORDER_COUNT_BUDGET_EXHAUSTED"),
            "{parked:#}"
        );
        assert_eq!(requests.lock().expect("requests").len(), 1);

        client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Exit,
                RequestClass::Exit,
            )
            .await
            .expect("an Exit order must bypass the ORDER admission gate");
        assert_eq!(
            requests.lock().expect("requests").len(),
            2,
            "Exit must reach transport at the same instant Normal is parked"
        );
        cleanup_retry_test_store(lifecycle_store, lifecycle_path).await;
    }

    #[tokio::test]
    async fn response_order_count_headers_park_the_next_normal_order_until_boundary() {
        let (lifecycle_store, lifecycle_path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(12_345));
        let responses = vec![
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
            MockResponse {
                status: 200,
                retry_after: None,
                body: "{}".to_string(),
            },
        ];
        let (mut client, requests, _) =
            test_client_responses(responses, "order-header-loop", clock.clone());
        client.order_count_limiter =
            OrderCountLimiter::isolated(OrderCountConfig::for_test(1_200, 300));
        client
            .mock_http
            .as_mut()
            .expect("mock HTTP")
            .order_count_headers = Some((Some(1_080), None));
        let params = vec![("symbol".to_string(), "BTCUSDT".to_string())];

        client.cooldown_store = Some(lifecycle_store.clone());
        client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect("the response carrying the threshold is observed");
        let parked = client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect_err("the observed threshold must park the next order");
        assert!(parked.to_string().contains("until_ms=60000"), "{parked:#}");
        assert_eq!(requests.lock().expect("requests").len(), 1);

        clock.now_ms.store(60_000, Ordering::SeqCst);
        client
            .send_signed_once(
                "POST",
                "/fapi/v1/order",
                &params,
                RetryScope::Normal,
                RequestClass::Entry,
            )
            .await
            .expect("the aligned minute boundary releases the park");
        assert_eq!(requests.lock().expect("requests").len(), 2);
        cleanup_retry_test_store(lifecycle_store, lifecycle_path).await;
    }

    #[test]
    fn order_count_headers_read_the_per_account_meter() {
        let mut headers = HeaderMap::new();
        headers.insert("x-mbx-order-count-1m", "137".parse().expect("header value"));
        headers.insert("x-mbx-order-count-10s", "4".parse().expect("header value"));
        assert_eq!(order_count_headers(&headers), (Some(137), Some(4)));

        // 权重表与下单表是两个表：只有权重头在场时，下单读数必须是 None，
        // 而不是被当成 0——「没读到」不等于「用了 0」。
        let mut only_weight = HeaderMap::new();
        only_weight.insert("x-mbx-used-weight-1m", "900".parse().expect("header value"));
        assert_eq!(order_count_headers(&only_weight), (None, None));
        assert_eq!(used_weight_1m(&only_weight), Some(900));

        // 畸形值不得被当成 0 悄悄吞掉。
        let mut malformed = HeaderMap::new();
        malformed.insert("x-mbx-order-count-1m", "not-a-number".parse().expect("v"));
        assert_eq!(order_count_headers(&malformed), (None, None));
    }

    #[test]
    fn t311_egress_scope_same_base_url_ignores_credential_identity() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let expected = "binance-futures-rest:base-url:https://same-egress.example.test/api";
        let scope_a = egress_scope_for(
            "t311-egress-same-url-a",
            "https://same-egress.example.test/api",
        )
        .expect("unconfigured credential A derives a base-URL scope");
        let scope_b = egress_scope_for(
            "t311-egress-same-url-b",
            "https://same-egress.example.test/api",
        )
        .expect("unconfigured credential B derives a base-URL scope");

        assert_eq!(scope_a, expected);
        assert_eq!(scope_b, expected);
    }

    #[test]
    fn t311_egress_scope_distinguishes_venues() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let testnet_credential = "t311-egress-venue-testnet";
        let mainnet_credential = "t311-egress-venue-mainnet"; // gitleaks:allow -- Synthetic test credential identifier.
        let _testnet = TestEnvVar::set(crate::venue::venue_env_name(testnet_credential), "testnet");
        let _mainnet = TestEnvVar::set(crate::venue::venue_env_name(mainnet_credential), "mainnet");

        let testnet_scope = egress_scope_for(testnet_credential, "https://same.example.test")
            .expect("testnet venue derives an egress scope");
        let mainnet_scope = egress_scope_for(mainnet_credential, "https://same.example.test")
            .expect("mainnet venue derives an egress scope");

        assert_eq!(testnet_scope, "binance-futures-rest:venue:testnet");
        assert_eq!(mainnet_scope, "binance-futures-rest:venue:mainnet");
    }

    #[test]
    fn t311_egress_scope_distinguishes_base_urls() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let scope_a = egress_scope_for(
            "t311-egress-different-url-a",
            "https://egress-a.example.test/path",
        )
        .expect("base URL A derives an egress scope");
        let scope_b = egress_scope_for(
            "t311-egress-different-url-b",
            "https://egress-b.example.test/path",
        )
        .expect("base URL B derives an egress scope");

        assert_eq!(
            scope_a,
            "binance-futures-rest:base-url:https://egress-a.example.test/path"
        );
        assert_eq!(
            scope_b,
            "binance-futures-rest:base-url:https://egress-b.example.test/path"
        );
    }

    #[test]
    fn t311_egress_scope_normalizes_url_variants() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let expected = "binance-futures-rest:base-url:https://x";

        assert_eq!(
            egress_scope_for("t311-egress-url-slash", "https://x/")
                .expect("trailing-slash URL derives an egress scope"),
            expected
        );
        assert_eq!(
            egress_scope_for("t311-egress-url-plain", "https://x")
                .expect("plain URL derives an egress scope"),
            expected
        );
        assert_eq!(
            egress_scope_for("t311-egress-url-query", "https://x?a=1")
                .expect("query URL derives an egress scope"),
            expected
        );
        assert_eq!(
            egress_scope_for("t311-egress-url-fragment", "https://x#f")
                .expect("fragment URL derives an egress scope"),
            expected
        );
    }

    #[test]
    fn t311_egress_scope_rejects_invalid_base_url() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let error = egress_scope_for("t311-egress-invalid-url", "not a URL")
            .expect_err("invalid base URL must not fall back to a default scope");

        assert_eq!(
            format!("{error:#}"),
            "parse Binance REST base URL: relative URL without a base"
        );
    }

    fn request_target(request: &str) -> &str {
        request
            .split_whitespace()
            .nth(1)
            .expect("HTTP request target")
    }

    fn request_query(request: &str) -> HashMap<String, String> {
        let url = url::Url::parse(request_target(request)).expect("parse HTTP request target");
        url.query_pairs().into_owned().collect()
    }

    fn request_path(request: &str) -> String {
        url::Url::parse(request_target(request))
            .expect("parse HTTP request target")
            .path()
            .to_string()
    }

    fn user_trade(id: i64, symbol: &str) -> BinanceUserTrade {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "symbol": symbol,
            "orderId": id + 10_000,
            "price": "100.0",
            "qty": "0.01",
            "time": 1_700_000_000_000_i64,
        }))
        .expect("construct Binance user trade fixture")
    }

    fn user_trades_body(ids: &[i64], symbol: &str) -> String {
        serde_json::to_string(
            &ids.iter()
                .map(|id| user_trade(*id, symbol))
                .collect::<Vec<_>>(),
        )
        .expect("serialize Binance user trade fixtures")
    }

    #[test]
    fn t39_first_user_trades_from_id_is_cursor_plus_one() {
        assert_eq!(first_user_trades_from_id(40).expect("derive fromId"), 41);
        assert_eq!(
            format!(
                "{:#}",
                first_user_trades_from_id(i64::MAX).expect_err("cursor overflow must fail")
            ),
            "fill replay cursor cannot advance beyond i64::MAX"
        );
    }

    #[test]
    fn t39_full_user_trades_page_derives_next_from_max_id() {
        let page = vec![user_trade(13, "BTCUSDT"), user_trade(12, "BTCUSDT")];
        assert_eq!(
            user_trades_page_disposition(&page, 2).expect("classify full page"),
            UserTradesPageDisposition::Continue { next_from_id: 14 }
        );
    }

    #[test]
    fn t39_partial_user_trades_page_is_complete() {
        assert_eq!(
            user_trades_page_disposition(&[user_trade(12, "BTCUSDT")], 2)
                .expect("classify partial page"),
            UserTradesPageDisposition::Complete
        );
    }

    #[tokio::test]
    async fn t39_exactly_full_user_trades_page_fetches_the_following_page() {
        let (mut client, requests, _) = test_client_responses(
            vec![
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: user_trades_body(&[11, 12], "BTCUSDT"),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: user_trades_body(&[], "BTCUSDT"),
                },
            ],
            "t39-full-page",
            Arc::new(ManualRetryClock::new(1_000)),
        );
        let limiter = test_limiter_without_refill();
        client.request_limiter = limiter.clone();
        let limiter_scope = client.egress_scope.clone();
        let trades = client
            .user_trades_after(&limiter, "BTCUSDT", 10, 2, RequestClass::EventTruth)
            .await
            .expect("paginate exact full userTrades page");
        assert_eq!(
            trades.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![11, 12]
        );
        let captured = requests.lock().expect("lock userTrades requests").clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(request_path(&captured[0]), "/fapi/v1/userTrades");
        assert_eq!(
            request_query(&captured[0]).get("fromId"),
            Some(&"11".to_string())
        );
        assert_eq!(
            request_query(&captured[1]).get("fromId"),
            Some(&"13".to_string())
        );
        assert_eq!(limiter.stored_tokens(&limiter_scope).await, Some(0.0));
    }

    #[tokio::test]
    async fn t39_partial_user_trades_page_stops_without_an_extra_request() {
        let (mut client, requests) =
            test_client(&user_trades_body(&[21], "ETHUSDT"), "t39-partial-page");
        let limiter = test_limiter_without_refill();
        client.request_limiter = limiter.clone();
        let limiter_scope = client.egress_scope.clone();
        let trades = client
            .user_trades_after(&limiter, "ETHUSDT", 20, 2, RequestClass::EventTruth)
            .await
            .expect("stop at partial userTrades page");
        assert_eq!(
            trades.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![21]
        );
        let captured = requests.lock().expect("lock userTrades requests").clone();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            request_query(&captured[0]).get("fromId"),
            Some(&"21".to_string())
        );
        assert_eq!(limiter.stored_tokens(&limiter_scope).await, Some(5.0));
    }

    #[tokio::test]
    async fn t39_user_trades_requires_a_cursor_and_never_defaults_to_recent_history() {
        let (client, requests) = test_client("[]", "t39-cursor-required");
        client
            .user_trades_after(
                &test_limiter(),
                " btcusdt ",
                987,
                100,
                RequestClass::EventTruth,
            )
            .await
            .expect("fetch from mandatory cursor");
        let captured = requests.lock().expect("lock userTrades request").clone();
        assert_eq!(captured.len(), 1);
        let query = request_query(&captured[0]);
        assert_eq!(query.get("symbol"), Some(&"BTCUSDT".to_string()));
        assert_eq!(query.get("fromId"), Some(&"988".to_string()));
        assert_eq!(query.get("limit"), Some(&"100".to_string()));
    }

    #[tokio::test]
    async fn t39_user_trades_returns_duplicates_for_database_constraint_to_resolve() {
        let (client, _) = test_client(
            &user_trades_body(&[31, 31], "BTCUSDT"),
            "t39-no-client-dedupe",
        );
        let trades = client
            .user_trades_after(&test_limiter(), "BTCUSDT", 30, 3, RequestClass::EventTruth)
            .await
            .expect("return raw page ordering and multiplicity");
        assert_eq!(
            trades.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![31, 31]
        );
    }

    /// **接线证明（a）**：两个各自经生产构造器建出来的 client，在同一个真实出口上
    /// 必须花同一份预算。
    ///
    /// 上一版只测了 `shared()` 这个函数本身，于是把生产调用点从 `shared()` 改回
    /// `new()` 仍然全绿——测的是 helper，不是接线。这条用例走 `build()` 的真实路径。
    #[tokio::test]
    async fn two_production_clients_on_one_egress_spend_one_weight_budget() {
        let _serial = env_serial_guard();
        let route = format!("seam-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        // 桶要可预测：容量 100、补充 1/秒，两次调用之间的补充可以忽略。
        let _rate = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(), "1");
        let _burst = TestEnvVar::set("RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST".to_string(), "100");
        let cred_a = "egress-seam-client-a";
        let cred_b = "egress-seam-client-b";
        let _venue_a = TestEnvVar::set(crate::venue::venue_env_name(cred_a), "testnet");
        let _venue_b = TestEnvVar::set(crate::venue::venue_env_name(cred_b), "testnet");
        let keys = BinanceKeys {
            api_key: "local-test-key".to_string(),
            api_secret: "local-test-secret".to_string(),
        };
        let a = BinanceFuturesClient::new_without_cooldown_store(keys.clone(), cred_a)
            .expect("build client a through the production constructor");
        let b = BinanceFuturesClient::new_without_cooldown_store(keys, cred_b)
            .expect("build client b through the production constructor");
        assert_eq!(
            a.egress_scope, b.egress_scope,
            "同一 route id 必须派生同一个出口 scope"
        );

        a.request_limiter
            .acquire_reserving(&a.egress_scope, 30.0, 0.0)
            .await;
        assert_eq!(
            b.request_limiter.stored_tokens(&b.egress_scope).await,
            Some(70.0),
            "第二个 client 必须看到第一个花掉的权重；看不到就说明它自带了一个桶"
        );
    }

    /// **接线证明（b）**：启动确实会打印生效预算。
    ///
    /// `main` 起不来测试进程，仓内既有做法是对源码形状断言
    /// （`venue.rs` 的 `transport_call_sites_keep_startup_...` 同款）。
    /// 这条能杀掉"把 main.rs 里那行调用删掉"的变异。
    #[test]
    fn startup_logs_the_effective_egress_weight_budget() {
        let main_source = include_str!("main.rs");
        let client_source = include_str!("binance_futures.rs");
        let production_client_source = client_source
            .split_once("\n#[cfg(test)]\npub(crate) mod tests {")
            .expect("binance_futures test module boundary")
            .0;
        assert!(
            main_source.contains("binance_futures::log_effective_egress_budget()"),
            "启动必须打印生效的出口权重预算，否则改一个已上线旋钮的语义无人可见"
        );
        assert!(
            production_client_source.contains("binance order-count budget in effect"),
            "startup budget logging must expose both per-credential ORDER windows"
        );
        for field_line in [
            "            egress_scope = %egress_scope,\n",
            "            budget_1m = effective_budget.budget_1m,\n",
            "            budget_source = effective_budget.source.as_str(),\n",
            "            queue_wait_max_ms = governor.queue_wait_max_ms,\n",
            "            protectable_exit_budget_1m,\n",
            "            protectable_exit_weight_per_position,\n",
            "            max_protectable_positions,\n",
        ] {
            assert!(
                production_client_source.contains(field_line),
                "startup budget logging must expose {field_line:?}"
            );
        }
    }

    #[test]
    fn a6_i9_production_send_paths_and_callers_pin_request_classes() {
        fn production(source: &str) -> &str {
            [
                "#[cfg(test)]\npub(crate) mod tests",
                "#[cfg(test)]\nmod tests",
            ]
            .into_iter()
            .filter_map(|boundary| source.find(boundary))
            .min()
            .map(|index| &source[..index])
            .unwrap_or(source)
        }
        let binance = production(include_str!("binance_futures.rs"));
        assert!(
            !binance.contains(".acquire_reserving(&self.egress_scope"),
            "send_signed_once/send_unsigned_once must not regress to sleep-poll"
        );
        assert_eq!(
            binance.matches(".acquire_prioritized(").count(),
            3,
            "signed Normal, signed Exit/Protection, and unsigned Normal are the three admission seams"
        );
        let unsigned = binance
            .split("async fn send_unsigned_once(")
            .nth(1)
            .and_then(|source| source.split("async fn unsigned_request_with_retry").next())
            .expect("unsigned production admission source");
        assert!(
            unsigned.contains("let reserve = unsigned_step_before_deadline(deadline, self.normal_exit_weight_reserve()).await?;")
                && unsigned.contains("request_class,\n                reserve,"),
            "unsigned Normal must inherit the A7 dynamic exit reserve"
        );
        let expected = [
            (
                include_str!("reconcile.rs"),
                "RequestClass::PeriodicReconcile",
                10,
            ),
            (
                include_str!("binance_user_stream.rs"),
                "RequestClass::EventTruth",
                1,
            ),
            (include_str!("kill_switch.rs"), "RequestClass::Exit", 2),
            (
                include_str!("kill_switch.rs"),
                "RequestClass::EventTruth",
                3,
            ),
            (
                include_str!("kill_switch.rs"),
                "RequestClass::Protection",
                1,
            ),
            (include_str!("protection.rs"), "RequestClass::EventTruth", 1),
            (include_str!("svc.rs"), "RequestClass::EventTruth", 2),
            (include_str!("svc.rs"), "RequestClass::Protection", 2),
            (include_str!("svc_v2.rs"), "RequestClass::EventTruth", 1),
        ];
        for (source, class, count) in expected {
            assert_eq!(
                production(source).matches(class).count(),
                count,
                "production caller classification count changed for {class}"
            );
        }
    }

    #[test]
    fn every_production_rate_limiter_seam_uses_the_shared_constructor() {
        fn production_before_test_module(source: &str) -> &str {
            [
                "\n#[cfg(test)]\npub(crate) mod tests {",
                "\n#[cfg(test)]\nmod tests {",
            ]
            .into_iter()
            .filter_map(|boundary| source.find(boundary))
            .min()
            .map(|index| &source[..index])
            .unwrap_or(source)
        }

        let expected = [
            (
                "binance_futures.rs",
                include_str!("binance_futures.rs"),
                1_usize,
            ),
            (
                "exchange_filters.rs",
                include_str!("exchange_filters.rs"),
                1,
            ),
            ("rate_limiter.rs", include_str!("rate_limiter.rs"), 0),
            ("kill_switch.rs", include_str!("kill_switch.rs"), 1),
            ("svc.rs", include_str!("svc.rs"), 2),
            ("svc_v2.rs", include_str!("svc_v2.rs"), 1),
        ];
        let mut shared_total = 0;
        let mut new_total = 0;
        for (name, source, expected_shared) in expected {
            let production = production_before_test_module(source);
            let shared = production.matches("CredentialRateLimiter::shared(").count();
            // binance_futures keeps one pre-module `#[cfg(test)]` mock
            // constructor. Its `for_test` argument is itself test-only, so it
            // is excluded from the production count without hiding a
            // shared(from_env) -> new(from_env) seam mutation.
            let all_new = production.matches("CredentialRateLimiter::new(").count();
            let cfg_test_new = production
                .matches("CredentialRateLimiter::new(TokenBucketConfig::for_test(")
                .count();
            let new = all_new.saturating_sub(cfg_test_new);
            assert_eq!(
                shared, expected_shared,
                "production shared-constructor seam count changed in {name}"
            );
            assert_eq!(
                new, 0,
                "production code in {name} must never construct an isolated limiter"
            );
            shared_total += shared;
            new_total += new;
        }
        assert_eq!(shared_total, 6, "the six production seams are a closed set");
        assert_eq!(new_total, 0);
    }

    /// **接线证明（c）**：公共端点冷却必须**落库**，不能只写进程内 registry。
    ///
    /// VM 上的战役守卫只读 SQLite。上一版只断言了内存 registry，于是"保留内存更新、
    /// 跳过持久化"的变异全绿——而被跳过的恰恰是载重的那一半。
    #[tokio::test]
    async fn a_public_endpoint_cooldown_is_persisted_where_the_campaign_guard_reads_it() {
        // 发布器**同时**写进程内 registry。不给自己一个独占 scope 的话，这里会把
        // now+5s（一个远未来时刻）留在共享的 venue:testnet 上，任何用小假时钟跑在
        // 同一 scope 的用例（t311 用 80_000）从此都被"停车"——顺序相关的抖动。
        let _serial = env_serial_guard();
        let route = format!("persist-{}", ulid::Ulid::new());
        let _route = TestEnvVar::set("BINANCE_EGRESS_ROUTE_ID".to_string(), &route);
        let (store, path) = retry_test_store().await;
        let now = 1_788_000_000_000_i64;
        let scope = publish_public_endpoint_cooldown_with_store(
            Some(&store),
            Some(crate::venue::Venue::Testnet),
            429,
            5_000,
            now,
        )
        .await
        .expect("a public 429 must be publishable");

        let row = sqlx::query(
            "SELECT CAST(cooldown_until_ms AS CHAR) AS until_text \
             FROM binance_rate_limit_cooldowns WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store.pool())
        .await
        .expect("守卫读的是 SQLite，所以这一行必须存在");
        assert_eq!(
            crate::store::try_get_i64_text(&row, "until_text").expect("decode persisted cooldown"),
            now + 5_000
        );
        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[test]
    fn t39_user_trades_client_uses_credential_venue_base_url() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let credential = "t39-user-trades-testnet-venue";
        let _venue = TestEnvVar::set(crate::venue::venue_env_name(credential), "testnet");
        let client = BinanceFuturesClient::new_without_cooldown_store(
            BinanceKeys {
                api_key: "local-test-key".to_string(),
                api_secret: "local-test-secret".to_string(),
            },
            credential,
        )
        .expect("derive REST client from credential venue");
        assert_eq!(client.base_url, crate::venue::BINANCE_TESTNET_REST_BASE_URL);
    }

    #[tokio::test]
    async fn t39_user_trades_reuses_t38_read_backoff_for_429() {
        let clock = Arc::new(ManualRetryClock::new(1_000));
        let (client, requests, records) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("0".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
            ],
            "t39-user-trades-429",
            clock,
        );
        client
            .user_trades_after(
                &test_limiter(),
                "BTCUSDT",
                50,
                100,
                RequestClass::EventTruth,
            )
            .await
            .expect("retry read-only userTrades after 429");
        assert_eq!(requests.lock().expect("lock retry requests").len(), 2);
        assert_eq!(
            records.lock().expect("lock retry records").clone(),
            vec![RetryRecord {
                credential_id: "t39-user-trades-429".to_string(),
                endpoint: "/fapi/v1/userTrades".to_string(),
                status: 429,
                retry_after: Some("0".to_string()),
                retry_number: 1,
                next_wait_ms: 100,
                request_class: "read_only",
                scope: "normal",
                decision: "retry",
            }]
        );
    }

    #[tokio::test]
    async fn t39_user_trades_reuses_t311_418_ip_ban_without_retry() {
        let (client, requests, records) = test_client_responses(
            vec![MockResponse {
                status: 418,
                retry_after: Some("1".to_string()),
                body: r#"{"code":-1003}"#.to_string(),
            }],
            "t39-user-trades-418",
            Arc::new(ManualRetryClock::new(1_000)),
        );
        let error = client
            .user_trades_after(
                &test_limiter(),
                "BTCUSDT",
                60,
                100,
                RequestClass::EventTruth,
            )
            .await
            .expect_err("418 must fail without retrying userTrades");
        assert_eq!(requests.lock().expect("lock IP-ban requests").len(), 1);
        assert_eq!(
            format!("{error:#}"),
            "binance http 418 IP ban: credential=t39-user-trades-418 endpoint=/fapi/v1/userTrades cooldown_ms=120000"
        );
        assert_eq!(records.lock().expect("lock IP-ban records").len(), 1);
        assert_eq!(
            records.lock().expect("lock IP-ban records")[0].decision,
            "ip_ban"
        );
    }

    #[tokio::test]
    async fn dispatch_algo_post_cannot_use_the_ordinary_transport_authority() {
        let (client, requests) = test_client("{}", "dispatch-no-permit");
        let result = client
            .signed_request_with_retry(
                "POST",
                "/fapi/v1/algoOrder",
                &[],
                RetryClass::IdempotentWrite,
                RetryScope::Exit,
                RequestClass::Protection,
            )
            .await;
        assert!(result.is_err());
        assert!(
            requests.lock().unwrap().is_empty(),
            "unmanaged Algo POST must not reach even mock transport"
        );
    }

    async fn place_close_position_algo_and_capture(
    ) -> (BinanceAlgoOrderResponse, String, Option<f64>) {
        let (mut client, requests) = test_client(
            r#"{"algoId":987654321,"clientAlgoId":"algo-client-1","algoStatus":"NEW","symbol":"BTCUSDT"}"#,
            "algo-contract-test",
        );
        let credential_id = "algo-contract-test";
        let limiter = test_limiter();
        client.egress_scope = credential_id.to_string();
        client.request_limiter = limiter.clone();
        let mut permit = crate::protection_dispatch::tests::fixture_permit(
            credential_id,
            crate::protection_dispatch::AlgoWire {
                symbol: "BTCUSDT".into(),
                side: "SELL".into(),
                order_type: "STOP_MARKET".into(),
                trigger_price: "95000.25".into(),
                quantity: None,
                reduce_only: true,
                close_position: true,
                client_algo_id: "algo-client-1".into(),
            },
        )
        .await;
        let response = client
            .place_algo_conditional(
                &limiter,
                &mut permit,
                BinanceAlgoConditionalOrderType::StopMarket,
                "BTCUSDT",
                "SELL",
                "95000.25",
                None,
                true,
                true,
                &BinanceClientAlgoId::new("algo-client-1"),
            )
            .await
            .expect("place conditional algo order against local stub");
        let request = requests
            .lock()
            .expect("lock captured request")
            .pop()
            .expect("captured in-memory HTTP request");
        let tokens = limiter.stored_tokens(credential_id).await;
        (response, request, tokens)
    }

    #[test]
    fn signs() {
        let sig = sign_hmac_sha256_hex(b"secret", b"symbol=BTCUSDT&timestamp=1");
        assert_eq!(sig.len(), 64);
    }

    #[tokio::test]
    async fn a1_algo_submit_uses_trigger_price_not_stop_price() {
        let (response, request, tokens) = place_close_position_algo_and_capture().await;
        let query = request_query(&request);
        assert!(request_target(&request).starts_with("https://local.stub.invalid/"));
        assert_eq!(request_path(&request), "/fapi/v1/algoOrder");
        assert_eq!(
            query.get("triggerPrice").map(String::as_str),
            Some("95000.25")
        );
        assert!(!query.contains_key("stopPrice"));
        assert_eq!(response.algo_id, Some(BinanceAlgoId::new(987654321)));
        assert_eq!(tokens, None, "POST algoOrder has zero IP request weight");
    }

    #[tokio::test]
    async fn a2_algo_submit_requires_conditional_algo_type() {
        let (response, request, _) = place_close_position_algo_and_capture().await;
        let query = request_query(&request);
        assert_eq!(
            query.get("algoType").map(String::as_str),
            Some("CONDITIONAL")
        );
        assert_eq!(query.get("type").map(String::as_str), Some("STOP_MARKET"));
        assert_eq!(response.algo_status.as_deref(), Some("NEW"));
    }

    #[tokio::test]
    async fn a3_algo_submit_uses_client_algo_id_not_standard_client_order_id() {
        let (response, request, _) = place_close_position_algo_and_capture().await;
        let query = request_query(&request);
        assert_eq!(
            query.get("clientAlgoId").map(String::as_str),
            Some("algo-client-1")
        );
        assert!(!query.contains_key("newClientOrderId"));
        assert_eq!(
            response
                .client_algo_id
                .as_ref()
                .map(BinanceClientAlgoId::as_str),
            Some("algo-client-1")
        );
    }

    #[test]
    fn a4_algo_and_standard_order_ids_are_distinct_types() {
        assert_ne!(
            std::any::TypeId::of::<BinanceAlgoId>(),
            std::any::TypeId::of::<BinanceOrderId>()
        );
        assert_ne!(
            std::any::type_name::<BinanceAlgoId>(),
            std::any::type_name::<BinanceOrderId>()
        );
        let ordinary = BinanceOrderAck {
            order_id: BinanceOrderId::new(7),
            client_order_id: "ordinary-client".to_string(),
            status: "NEW".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "BUY".to_string(),
            orig_qty: "0.001".to_string(),
            executed_qty: "0".to_string(),
        };
        let algo = BinanceAlgoId::new(7);
        assert_eq!(ordinary.order_id.as_i64(), algo.as_i64());
    }

    #[tokio::test]
    async fn a5_missing_actual_order_id_stays_absent() {
        let (response, _, _) = place_close_position_algo_and_capture().await;
        assert_eq!(response.actual_order_id, None);
        let encoded = serde_json::to_value(response).expect("serialize parsed algo response");
        assert!(
            encoded.get("actualOrderId").is_none(),
            "an absent actualOrderId must not be synthesized as zero or an empty string"
        );
    }

    #[test]
    fn missing_algo_response_fields_remain_absent_individually() {
        let response: BinanceAlgoOrderResponse =
            serde_json::from_str(r#"{"symbol":"BTCUSDT"}"#).expect("parse sparse algo response");
        assert_eq!(response.algo_id, None);
        assert_eq!(response.client_algo_id, None);
        assert_eq!(response.algo_status, None);
        assert_eq!(response.actual_order_id, None);
        let encoded = serde_json::to_value(response).expect("serialize sparse algo response");
        for absent in ["algoId", "clientAlgoId", "algoStatus", "actualOrderId"] {
            assert!(encoded.get(absent).is_none(), "{absent} must remain absent");
        }
        assert_eq!(
            encoded.get("symbol").and_then(serde_json::Value::as_str),
            Some("BTCUSDT")
        );
    }

    #[test]
    fn b10_blank_actual_order_id_and_algo_status_remain_absent() {
        let response: BinanceAlgoOrderResponse = serde_json::from_str(
            r#"{"algoId":12,"clientAlgoId":"algo-12","algoStatus":"","actualOrderId":""}"#,
        )
        .expect("parse blank optional algo fields");
        assert_eq!(response.algo_status, None);
        assert_eq!(response.actual_order_id, None);

        let zero: BinanceAlgoOrderResponse =
            serde_json::from_str(r#"{"algoId":12,"clientAlgoId":"algo-12","actualOrderId":0}"#)
                .expect("parse zero actualOrderId as absent");
        assert_eq!(zero.actual_order_id, None);
    }

    #[test]
    fn conditional_algo_order_type_mapping_covers_the_exchange_contract() {
        assert_eq!(
            [
                BinanceAlgoConditionalOrderType::StopMarket,
                BinanceAlgoConditionalOrderType::TakeProfitMarket,
                BinanceAlgoConditionalOrderType::Stop,
                BinanceAlgoConditionalOrderType::TakeProfit,
                BinanceAlgoConditionalOrderType::TrailingStopMarket,
            ]
            .map(BinanceAlgoConditionalOrderType::as_binance),
            [
                "STOP_MARKET",
                "TAKE_PROFIT_MARKET",
                "STOP",
                "TAKE_PROFIT",
                "TRAILING_STOP_MARKET",
            ]
        );
    }

    #[test]
    fn a6_algo_client_base_url_is_venue_derived() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        const CHILD_MARKER: &str = "BINANCE_ALGO_VENUE_PROBE_CHILD";
        const CHILD_TEST: &str = "binance_futures::tests::a6_algo_client_base_url_is_venue_derived";
        if std::env::var(CHILD_MARKER).as_deref() == Ok("1") {
            let client = BinanceFuturesClient::new_without_cooldown_store(
                BinanceKeys {
                    api_key: "local-test-key".to_string(),
                    api_secret: "local-test-secret".to_string(),
                },
                "algo-venue-probe",
            )
            .expect("construct venue-derived Binance client");
            assert_eq!(client.base_url, crate::venue::BINANCE_TESTNET_REST_BASE_URL);
            return;
        }

        let status = std::process::Command::new(
            std::env::current_exe().expect("locate current Rust test executable"),
        )
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(CHILD_MARKER, "1")
        // 从 venue_env_name 派生而不是写死后缀：把 secret locator 从有损
        // 归一改成单射编码后，写死的 BINANCE_VENUE_ALGO_VENUE_PROBE 会指向一个
        // 谁也不读的名字，子进程于是拿不到 venue——测试红得毫无线索。
        .env(crate::venue::venue_env_name("algo-venue-probe"), "testnet")
        .env(
            "BINANCE_FUTURES_BASE_URL",
            "https://TESTNET.binancefuture.com/custom/path",
        )
        .status()
        .expect("run isolated venue derivation probe");
        assert!(status.success(), "venue derivation child test failed");
    }

    #[tokio::test]
    async fn algo_query_open_and_cancel_use_exact_contract_endpoints() {
        let limiter = test_limiter();
        let credential_id = "algo-query-test";

        let (mut client, requests) = test_client(r#"{"algoId":42}"#, credential_id);
        client.egress_scope = credential_id.to_string();
        client.request_limiter = limiter.clone();
        let value = client
            .get_algo_order_json(
                &limiter,
                Some(&BinanceAlgoId::new(42)),
                None,
                RequestClass::EventTruth,
            )
            .await
            .expect("query algo order against local stub");
        assert_eq!(
            value.get("algoId").and_then(serde_json::Value::as_i64),
            Some(42)
        );
        let request = requests
            .lock()
            .expect("lock algo query request")
            .pop()
            .expect("captured algo query request");
        assert!(request.starts_with("GET "));
        assert_eq!(request_path(&request), "/fapi/v1/algoOrder");
        assert_eq!(
            request_query(&request).get("algoId").map(String::as_str),
            Some("42")
        );

        let (mut client, requests) = test_client("[]", credential_id);
        client.egress_scope = credential_id.to_string();
        client.request_limiter = limiter.clone();
        let value = client
            .get_open_algo_orders_json(&limiter, "BTCUSDT", RequestClass::PeriodicReconcile)
            .await
            .expect("query open algo orders against local stub");
        assert_eq!(value, serde_json::json!([]));
        let request = requests
            .lock()
            .expect("lock open algo request")
            .pop()
            .expect("captured open algo request");
        assert!(request.starts_with("GET "));
        assert_eq!(request_path(&request), "/fapi/v1/openAlgoOrders");
        assert_eq!(
            request_query(&request).get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );

        let (mut client, requests) = test_client(r#"{"algoId":42}"#, credential_id);
        client.egress_scope = credential_id.to_string();
        client.request_limiter = limiter.clone();
        let value = client
            .cancel_algo_order_json(&limiter, &BinanceAlgoId::new(42), RequestClass::Protection)
            .await
            .expect("cancel algo order against local stub");
        assert_eq!(
            value.get("algoId").and_then(serde_json::Value::as_i64),
            Some(42)
        );
        let request = requests
            .lock()
            .expect("lock algo cancel request")
            .pop()
            .expect("captured algo cancel request");
        assert!(request.starts_with("DELETE "));
        assert_eq!(request_path(&request), "/fapi/v1/algoOrder");
        assert_eq!(
            request_query(&request).get("algoId").map(String::as_str),
            Some("42")
        );
        let remaining = limiter
            .stored_tokens(credential_id)
            .await
            .expect("all algo query methods use the supplied limiter");
        assert!(
            (7.0..7.2).contains(&remaining),
            "three requests must consume three tokens; got {remaining}"
        );
    }

    #[tokio::test]
    async fn b1_protection_submit_uses_algo_endpoint_and_trigger_price() {
        let limiter = test_limiter();
        let (client, requests) = test_client(
            r#"{"algoId":101,"clientAlgoId":"deterministic-protection","algoStatus":"NEW","symbol":"BTCUSDT","side":"SELL","actualOrderId":""}"#,
            "algo-protection-test",
        );
        let mut permit = crate::protection_dispatch::tests::fixture_permit(
            "algo-protection-test",
            crate::protection_dispatch::AlgoWire {
                symbol: "BTCUSDT".into(),
                side: "SELL".into(),
                order_type: "STOP_MARKET".into(),
                trigger_price: "95000.25".into(),
                quantity: None,
                reduce_only: true,
                close_position: true,
                client_algo_id: "deterministic-protection".into(),
            },
        )
        .await;
        let ack = client
            .place_algo_conditional(
                &limiter,
                &mut permit,
                BinanceAlgoConditionalOrderType::StopMarket,
                "BTCUSDT",
                "SELL",
                "95000.25",
                None,
                true,
                true,
                &BinanceClientAlgoId::new("deterministic-protection"),
            )
            .await
            .expect("algo protection call against local stub");
        assert_eq!(ack.algo_id, Some(BinanceAlgoId::new(101)));
        let request = requests
            .lock()
            .expect("lock algo protection request")
            .pop()
            .expect("captured algo protection request");
        let query = request_query(&request);
        assert_eq!(request_path(&request), "/fapi/v1/algoOrder");
        assert_eq!(
            query.get("triggerPrice").map(String::as_str),
            Some("95000.25")
        );
        assert_eq!(
            query.get("clientAlgoId").map(String::as_str),
            Some("deterministic-protection")
        );
        assert_eq!(
            query.get("algoType").map(String::as_str),
            Some("CONDITIONAL")
        );
        assert!(!query.contains_key("stopPrice"));
        assert!(!query.contains_key("newClientOrderId"));

        let submit_source = include_str!("svc.rs");
        assert!(submit_source.contains(".place_algo_conditional("));
        assert!(!submit_source.contains(".place_stop_market_order("));
        assert!(!submit_source.contains(".place_take_profit_market_order("));
    }

    fn order_ack(client_order_id: &str) -> String {
        format!(
            r#"{{"orderId":7001,"clientOrderId":"{client_order_id}","status":"NEW","symbol":"BTCUSDT","side":"BUY","origQty":"0.001","executedQty":"0"}}"#
        )
    }

    #[tokio::test]
    async fn t310_recovered_orphan_is_flattened_by_l3_and_reconcile_converges() {
        let credential_id = "cred_t310_l3_closure";
        let orphan_position = r#"[{"symbol":"BTCUSDT","positionAmt":"0.001","entryPrice":"100","markPrice":"101","leverage":"1","unRealizedProfit":"1","liquidationPrice":"50","positionSide":"BOTH"}]"#;
        let zero_position = r#"[{"symbol":"BTCUSDT","positionAmt":"0.000","entryPrice":"0","markPrice":"101","leverage":"1","unRealizedProfit":"0","liquidationPrice":"0","positionSide":"BOTH"}]"#;
        let balance = r#"[{"asset":"USDT","balance":"10000","availableBalance":"9000"}]"#;
        let flatten_client_id = "ks_t310_recovered_orphan";
        let (store, path) = retry_test_store().await;
        let owner_digest =
            crate::svc::sha256_hex(&format!("orphan-position-owner|{credential_id}"));
        let expected_bot_uid = format!("reconcile_orphan_{}", &owner_digest[..24]);
        let scope_key = format!("bot:{expected_bot_uid}");
        crate::kill_switch::arm(
            &store,
            crate::kill_switch::ArmRequest {
                scope_key: &scope_key,
                level: crate::kill_switch::Level::Flatten,
                trigger_source: "reconcile_diff",
                reason: "pre-existing T3.10 L3",
                now_ms: 309_999,
            },
        )
        .await
        .expect("L3 is already latched before orphan detection");
        let (client, requests, _) = test_client_responses_with_cooldown_store(
            vec![
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: orphan_position.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: balance.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: format!(
                        r#"{{"orderId":7310,"clientOrderId":"{flatten_client_id}","status":"FILLED","symbol":"BTCUSDT","side":"SELL","origQty":"0.001","executedQty":"0.001"}}"#
                    ),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: zero_position.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: balance.to_string(),
                },
            ],
            credential_id,
            &format!("test-egress:t310-l3-closure:{}", ulid::Ulid::new()),
            Arc::new(ManualRetryClock::new(310_000)),
            store.clone(),
        );

        let detected = crate::reconcile::reconcile_binance_futures_truth_with_client(
            credential_id,
            store.clone(),
            test_limiter(),
            &client,
        )
        .await
        .expect("reconcile detects and durably recovers the orphan");
        assert!(
            detected.has_diff,
            "an unexplained exchange position is a diff"
        );
        assert!(
            detected.local_position_zero_diff,
            "the derived local zero predicate must reach ReconcileTruth unchanged"
        );
        assert!(!detected.flatten_complete());

        let recovered = sqlx::query(
            "SELECT bot_uid, state, symbol_key, direction, qty FROM position_state WHERE credential_id=?",
        )
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("load recovered orphan position_state");
        let bot_uid = recovered.get::<String, _>("bot_uid");
        assert_eq!(bot_uid, expected_bot_uid);
        assert_eq!(
            (
                recovered.get::<String, _>("state"),
                recovered.get::<String, _>("symbol_key"),
                recovered.get::<String, _>("direction"),
                recovered.get::<String, _>("qty"),
            ),
            (
                "open".to_string(),
                "binance|futures|BTC|USDT|USDT".to_string(),
                "long".to_string(),
                "0.001".to_string(),
            )
        );
        let blocked = sqlx::query(
            "SELECT flatten_state, last_reconcile_clean FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(&scope_key)
        .fetch_one(store.pool())
        .await
        .expect("orphan diff keeps the pre-existing L3 in flattening");
        assert_eq!(
            (
                blocked.get::<String, _>("flatten_state"),
                blocked.get::<i64, _>("last_reconcile_clean"),
            ),
            ("flattening".to_string(), 0)
        );

        crate::kill_switch::submit_live_flatten(
            &store,
            &client,
            credential_id,
            &scope_key,
            &crate::exchange_filters::ExchangeFilterCache::new(),
        )
        .await
        .expect("L3 treats recovered orphan as an ordinary open position");

        let converged = crate::reconcile::reconcile_binance_futures_truth_with_client(
            credential_id,
            store.clone(),
            test_limiter(),
            &client,
        )
        .await
        .expect("forced reconcile observes the filled flatten");
        assert!(!converged.has_diff);
        assert!(converged.local_position_zero_diff);
        assert!(converged.flatten_complete());

        let captured = requests.lock().expect("lock T3.10 requests").clone();
        let flatten_requests = captured
            .iter()
            .filter(|request| {
                request.starts_with("POST ") && request_path(request) == "/fapi/v1/order"
            })
            .collect::<Vec<_>>();
        assert_eq!(flatten_requests.len(), 1);
        let flatten_query = request_query(flatten_requests[0]);
        assert_eq!(
            flatten_query.get("symbol").map(String::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(flatten_query.get("side").map(String::as_str), Some("SELL"));
        assert_eq!(
            flatten_query.get("quantity").map(String::as_str),
            Some("0.001")
        );
        assert_eq!(
            flatten_query.get("reduceOnly").map(String::as_str),
            Some("true")
        );

        let status = sqlx::query(
            r#"
            SELECT reconcile_clean, flatten_complete
            FROM kill_switch_reconcile_status
            WHERE scope_key=? AND credential_id=?
            "#,
        )
        .bind(&scope_key)
        .bind(credential_id)
        .fetch_one(store.pool())
        .await
        .expect("load final per-credential L3 reconcile status");
        let state = sqlx::query(
            "SELECT flatten_state, last_reconcile_clean FROM kill_switch_state WHERE scope_key=?",
        )
        .bind(&scope_key)
        .fetch_one(store.pool())
        .await
        .expect("load final L3 state");
        assert_eq!(
            (
                status.get::<i64, _>("reconcile_clean"),
                status.get::<i64, _>("flatten_complete"),
                state.get::<String, _>("flatten_state"),
                state.get::<i64, _>("last_reconcile_clean"),
            ),
            (1, 1, "flattened".to_string(), 1)
        );

        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn rate_limit_429_retries_read_but_418_never_retries_and_engages_cooldown() {
        let read_clock = Arc::new(ManualRetryClock::new(10_000));
        let (client, requests, records) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("1".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: r#"{"status":"NEW"}"#.to_string(),
                },
            ],
            "t38-read-429",
            read_clock.clone(),
        );
        let value = client
            .get_order_json("BTCUSDT", None, Some("coid_read"), RequestClass::EventTruth)
            .await
            .expect("429 read retries against in-memory transport");
        assert_eq!(value, serde_json::json!({"status":"NEW"}));
        assert_eq!(requests.lock().expect("lock requests").len(), 2);
        assert_eq!(read_clock.sleeps(), vec![1_000]);
        assert_eq!(
            records.lock().expect("lock records").as_slice(),
            [RetryRecord {
                credential_id: "t38-read-429".to_string(),
                endpoint: "/fapi/v1/order".to_string(),
                status: 429,
                retry_after: Some("1".to_string()),
                retry_number: 1,
                next_wait_ms: 1_000,
                request_class: "read_only",
                scope: "normal",
                decision: "retry",
            }]
        );

        let ban_clock = Arc::new(ManualRetryClock::new(20_000));
        let (client, requests, records) = test_client_responses(
            vec![
                MockResponse {
                    status: 418,
                    retry_after: Some("1".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: r#"{"status":"NEW"}"#.to_string(),
                },
            ],
            "t38-read-418",
            ban_clock.clone(),
        );
        let error = client
            .get_order_json("BTCUSDT", None, Some("coid_ban"), RequestClass::EventTruth)
            .await
            .expect_err("418 must fail without retry");
        assert!(error.to_string().contains("418 IP ban"), "{error:#}");
        let blocked = client
            .get_order_json("BTCUSDT", None, Some("coid_ban"), RequestClass::EventTruth)
            .await
            .expect_err("active ban must block a later request before transport");
        assert!(
            blocked.to_string().contains("cooldown active"),
            "{blocked:#}"
        );
        assert_eq!(requests.lock().expect("lock requests").len(), 1);
        assert_eq!(ban_clock.sleeps(), Vec::<u64>::new());
        assert_eq!(records.lock().expect("lock records").len(), 1);
        assert_eq!(records.lock().expect("lock records")[0].decision, "ip_ban");
    }

    #[tokio::test]
    async fn t311_ip_ban_is_shared_across_credentials_in_one_egress_scope() {
        // 派生 egress scope 会读 BINANCE_EGRESS_ROUTE_ID；不持串行锁就会看见并发用例设的路由。
        let _serial = env_serial_guard();
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(80_000));
        let credential_a = "cred-t311-shared-a";
        let credential_b = "cred-t311-shared-b";
        let _venue_a = TestEnvVar::set(crate::venue::venue_env_name(credential_a), "testnet");
        let _venue_b = TestEnvVar::set(crate::venue::venue_env_name(credential_b), "testnet");
        let scope_a = egress_scope_for(credential_a, "https://unused-a.example.test")
            .expect("credential A derives its configured venue scope");
        let scope_b = egress_scope_for(credential_b, "https://unused-b.example.test")
            .expect("credential B derives its configured venue scope");
        assert_eq!(scope_a, "binance-futures-rest:venue:testnet");
        assert_eq!(scope_b, "binance-futures-rest:venue:testnet");
        let (client_a, requests_a, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 418,
                retry_after: None,
                body: r#"{"code":-1003}"#.to_string(),
            }],
            credential_a,
            &scope_a,
            clock.clone(),
            store.clone(),
        );
        let (client_b, requests_b, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            credential_b,
            &scope_b,
            clock,
            store.clone(),
        );

        let first = client_a
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_a"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("credential A must impose the shared 418 cooldown");
        assert!(first.to_string().contains("418 IP ban"), "{first:#}");
        let second = client_b
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_b"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("credential B in the same egress scope must be blocked");
        assert!(second.to_string().contains("cooldown active"), "{second:#}");
        assert_eq!(requests_a.lock().expect("lock A requests").len(), 1);
        assert_eq!(requests_b.lock().expect("lock B requests").len(), 0);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn first_429_closes_the_shared_egress_before_another_credential_sends() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(85_000));
        let scope = format!("test-egress:429-shared:{}", ulid::Ulid::new());
        let (client_a, requests_a, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 429,
                retry_after: Some("1".to_string()),
                body: r#"{"code":-1003}"#.to_string(),
            }],
            "cred-429-shared-a",
            &scope,
            clock.clone(),
            store.clone(),
        );
        let (client_b, requests_b, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-429-shared-b",
            &scope,
            clock,
            store.clone(),
        );

        let first = client_a
            .place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_429_a")
            .await
            .expect_err("exit budget must not wait through the shared 429 cooldown");
        assert!(
            first.to_string().contains("retry_budget_exhausted"),
            "{first:#}"
        );

        // Clear only process memory to prove that a peer/restarted process
        // sharing the database still observes the egress-wide quiet period.
        rate_limit_registry()
            .lock()
            .expect("lock rate-limit registry")
            .remove(&scope);
        let second = client_b
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_429_b"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("credential B must be stopped before transport");
        assert!(
            second.to_string().contains("rate-limit cooldown active"),
            "{second:#}"
        );
        assert_eq!(requests_a.lock().expect("lock A requests").len(), 1);
        assert_eq!(requests_b.lock().expect("lock B requests").len(), 0);

        let persisted = sqlx::query(
            "SELECT CAST(cooldown_until_ms AS CHAR) AS until_text \
             FROM binance_rate_limit_cooldowns WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store.pool())
        .await
        .expect("load persisted 429 cooldown");
        assert_eq!(
            crate::store::try_get_i64_text(&persisted, "until_text")
                .expect("decode persisted cooldown"),
            86_000
        );
        cleanup_retry_test_store(store, path).await;
    }

    /// Parks the shared egress on a 429 window without sending anything, so the
    /// carve-out can be exercised without also exercising the retry path.
    fn park_429_window(scope: &str, until_ms: i64) {
        rate_limit_registry()
            .lock()
            .expect("lock rate-limit registry")
            .insert(scope.to_string(), until_ms);
    }

    pub(crate) fn clear_egress_test_state(scope: &str) {
        rate_limit_registry()
            .lock()
            .expect("lock rate-limit registry")
            .remove(scope);
        ip_ban_registry()
            .lock()
            .expect("lock IP-ban registry")
            .remove(scope);
        exit_probe_registry()
            .lock()
            .expect("lock exit-probe registry")
            .remove(scope);
    }

    #[test]
    fn exit_probe_budget_defaults_to_four_and_clamps_to_the_closed_range() {
        assert_eq!(exit_probes_per_window(None), 4);
        assert_eq!(exit_probes_per_window(Some("0")), 1);
        assert_eq!(exit_probes_per_window(Some("9")), 8);
        assert_eq!(exit_probes_per_window(Some("6")), 6);
        assert_eq!(exit_probes_per_window(Some("nonsense")), 4);
    }

    #[tokio::test]
    async fn no_store_fallback_caps_the_same_window_at_n_and_resets_a_new_window() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "3");
        let scope = format!("test-egress:probe-memory:{}", ulid::Ulid::new());
        let (mut client, _, _) = test_client_responses(
            vec![],
            "cred-probe-memory",
            Arc::new(ManualRetryClock::new(100_000)),
        );
        client.egress_scope = scope.clone();
        for _ in 0..3 {
            assert!(client
                .claim_exit_probe(160_000, 100_000)
                .await
                .expect("claim"));
        }
        assert!(!client
            .claim_exit_probe(160_000, 100_000)
            .await
            .expect("cap"));
        assert!(client
            .claim_exit_probe(170_000, 100_000)
            .await
            .expect("new window"));
        clear_egress_test_state(&scope);
    }

    #[tokio::test]
    async fn probe_interval_is_zero_after_success_but_five_seconds_after_429() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "4");
        let scope = format!("test-egress:probe-interval:{}", ulid::Ulid::new());
        let (mut client, _, _) = test_client_responses(
            vec![],
            "cred-probe-interval",
            Arc::new(ManualRetryClock::new(100_000)),
        );
        client.egress_scope = scope.clone();
        assert!(client
            .claim_exit_probe(160_000, 100_000)
            .await
            .expect("first"));
        client
            .record_exit_probe_response(200, 100_000)
            .expect("success");
        assert!(client
            .claim_exit_probe(160_000, 100_000)
            .await
            .expect("second"));
        client
            .record_exit_probe_response(429, 100_000)
            .expect("failure");
        assert!(!client
            .claim_exit_probe(160_000, 104_999)
            .await
            .expect("floor"));
        assert!(client
            .claim_exit_probe(160_000, 105_000)
            .await
            .expect("floor elapsed"));
        clear_egress_test_state(&scope);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_clients_concurrently_claim_exactly_n_sql_cas_slots_then_new_window_starts_at_one()
    {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::set("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string(), "4");
        let (store, path) = retry_test_store().await;
        let scope = format!("test-egress:probe-cas:{}", ulid::Ulid::new());
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let (a, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-probe-cas-a",
            &scope,
            clock.clone(),
            store.clone(),
        );
        let (b, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-probe-cas-b",
            &scope,
            clock,
            store.clone(),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for index in 0..8 {
            let client = if index % 2 == 0 { a.clone() } else { b.clone() };
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                client
                    .claim_exit_probe(160_000, 100_000)
                    .await
                    .expect("CAS")
            }));
        }
        let mut granted = 0;
        for task in tasks {
            granted += usize::from(task.await.expect("join"));
        }
        assert_eq!(
            granted, 4,
            "the shared SQLite row must hand out exactly N slots"
        );
        let spent = sqlx::query(
            "SELECT probes_spent FROM binance_exit_probe_claims WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store.pool())
        .await
        .expect("load the exhausted SQLite probe budget");
        assert_eq!(spent.get::<i32, _>("probes_spent"), 4);
        assert!(!a.claim_exit_probe(160_000, 100_000).await.expect("N+1"));
        assert!(a
            .claim_exit_probe(170_000, 100_000)
            .await
            .expect("new window"));
        let row = sqlx::query(
            "SELECT window_until_ms, probes_spent FROM binance_exit_probe_claims WHERE egress_scope=?",
        )
                .bind(&scope)
                .fetch_one(store.pool())
                .await
                .expect("load spend");
        assert_eq!(row.get::<i64, _>("window_until_ms"), 170_000);
        assert_eq!(row.get::<i32, _>("probes_spent"), 1);
        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mysql_exit_probe_budget_cas_admits_exactly_n_under_concurrency() {
        const TEST: &str = "mysql_exit_probe_budget_cas_admits_exactly_n_under_concurrency";
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::unset("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string());
        let Some(scratch) = MySqlScratch::create("m1probe", TEST).await else {
            return;
        };
        let store_a = scratch.connect_store().await;
        let store_b = scratch.connect_store().await;
        let scope = format!("test-egress:mysql-probe-cas:{}", ulid::Ulid::new());
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let (a, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-mysql-probe-cas-a",
            &scope,
            clock.clone(),
            store_a.clone(),
        );
        let (b, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-mysql-probe-cas-b",
            &scope,
            clock,
            store_b.clone(),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::with_capacity(8);
        for index in 0..8 {
            let client = if index % 2 == 0 { a.clone() } else { b.clone() };
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                client
                    .claim_exit_probe(160_000, 100_000)
                    .await
                    .expect("claim a MySQL probe CAS slot")
            }));
        }
        let mut granted = 0;
        for task in tasks {
            granted += usize::from(task.await.expect("join a MySQL probe claimant"));
        }
        assert_eq!(granted, 4, "the default budget must admit exactly N=4");

        let spent = sqlx::query(
            "SELECT probes_spent FROM binance_exit_probe_claims WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store_a.pool())
        .await
        .expect("load the exhausted MySQL probe budget");
        assert_eq!(
            spent.get::<i32, _>("probes_spent"),
            4,
            "the MySQL row truth must equal the admitted total"
        );
        assert!(!a
            .claim_exit_probe(160_000, 100_000)
            .await
            .expect("refuse N+1 in the same MySQL window"));
        assert!(b
            .claim_exit_probe(170_000, 100_001)
            .await
            .expect("claim the first slot in a new MySQL window"));
        let reset = sqlx::query(
            "SELECT CAST(window_until_ms AS CHAR) AS window_text, probes_spent \
             FROM binance_exit_probe_claims WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store_b.pool())
        .await
        .expect("load the reset MySQL probe budget");
        assert_eq!(
            crate::store::try_get_i64_text(&reset, "window_text")
                .expect("decode the MySQL probe window"),
            170_000
        );
        assert_eq!(reset.get::<i32, _>("probes_spent"), 1);

        clear_egress_test_state(&scope);
        drop(a);
        drop(b);
        store_a.pool().close().await;
        store_b.pool().close().await;
        scratch.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_cooldown_upsert_keeps_the_later_deadline() {
        let (store, path) = retry_test_store().await;
        for table in [CooldownTable::IpBan, CooldownTable::RateLimit] {
            let scope = format!("test-egress:sqlite-cooldown:{}", ulid::Ulid::new());
            assert_concurrent_cooldown_upsert_keeps_later_deadline(
                store.clone(),
                store.clone(),
                table,
                &scope,
            )
            .await;
        }
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mysql_cooldown_upsert_keeps_the_later_deadline() {
        const TEST: &str = "mysql_cooldown_upsert_keeps_the_later_deadline";
        let Some(scratch) = MySqlScratch::create("m1cooldown", TEST).await else {
            return;
        };
        let store_a = scratch.connect_store().await;
        let store_b = scratch.connect_store().await;
        for table in [CooldownTable::IpBan, CooldownTable::RateLimit] {
            let scope = format!("test-egress:mysql-cooldown:{}", ulid::Ulid::new());
            assert_concurrent_cooldown_upsert_keeps_later_deadline(
                store_a.clone(),
                store_b.clone(),
                table,
                &scope,
            )
            .await;
        }
        store_a.pool().close().await;
        store_b.pool().close().await;
        scratch.cleanup().await;
    }

    #[tokio::test]
    async fn exit_takes_the_window_probe_through_a_429_cooldown_while_reads_stay_blocked() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::unset("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string());
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-probe:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);

        let (exit_client, exit_requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r##"{"orderId":9101,"clientOrderId":"coid_exit_probe","status":"NEW","symbol":"BTCUSDT","side":"SELL","origQty":"0.001","executedQty":"0"}"##.to_string(),
            }],
            "cred-exit-probe",
            &scope,
            clock.clone(),
            store.clone(),
        );
        let (read_client, read_requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-read-probe",
            &scope,
            clock.clone(),
            store.clone(),
        );

        // reduce_only == RetryScope::Exit: the flatten path must still reach
        // transport, otherwise a 429 park leaves a position uncloseable.
        exit_client
            .place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_exit_probe")
            .await
            .expect("a reduce-only exit must be admitted on the window probe");
        assert_eq!(exit_requests.lock().expect("lock exit requests").len(), 1);

        // Routine truth reads on the same parked egress are still refused.
        let blocked = read_client
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_read_probe"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("a normal-scope read must stay blocked by the 429 cooldown");
        assert!(
            blocked.to_string().contains("rate-limit cooldown active"),
            "{blocked:#}"
        );
        assert_eq!(read_requests.lock().expect("lock read requests").len(), 0);

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn the_n_plus_one_exit_in_the_same_429_window_is_refused_before_transport() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::unset("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string());
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-probe-spent:{}", ulid::Ulid::new());
        park_429_window(&scope, 160_000);

        let (client, requests, _) = test_client_responses_with_cooldown_store(
            (0..4)
                .map(|index| MockResponse {
                    status: 200,
                    retry_after: None,
                    body: order_ack(&format!("coid_exit_{index}")),
                })
                .collect::<Vec<_>>(),
            "cred-exit-probe-spent",
            &scope,
            clock.clone(),
            store.clone(),
        );

        for index in 0..4 {
            client
                .place_market_order(
                    "BTCUSDT",
                    "SELL",
                    "0.001",
                    true,
                    &format!("coid_exit_{index}"),
                )
                .await
                .expect("an in-budget exit reaches transport");
        }
        assert_eq!(requests.lock().expect("lock requests").len(), 4);
        let refused = client
            .place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_exit_fifth")
            .await
            .expect_err("the window's probe budget is exhausted");
        assert!(
            refused.to_string().contains("budget exhausted"),
            "{refused:#}"
        );
        assert_eq!(
            requests.lock().expect("lock requests").len(),
            4,
            "the refused exit must not reach transport"
        );

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    /// **跨重启有界**：冷却窗在库里，探针领取记录也必须在库里。
    /// 用"清空进程内 registry、保留 store"模拟重启：第二个进程只能领取
    /// 同窗剩余预算，不能把计数重置。
    #[tokio::test]
    async fn an_exit_probe_claim_survives_a_restart_through_the_store() {
        let _serial = env_serial_guard();
        let _budget = TestEnvVar::unset("RUSTCORE_BINANCE_EXIT_PROBES_PER_WINDOW".to_string());
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:probe-restart:{}", ulid::Ulid::new());
        let (before, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-probe-restart-a",
            &scope,
            clock.clone(),
            store.clone(),
        );
        assert!(
            before
                .claim_exit_probe(160_000, 100_000)
                .await
                .expect("claim"),
            "first process takes the window probe"
        );

        // 重启：进程内记录消失，库还在。
        clear_egress_test_state(&scope);
        let (after, _, _) = test_client_responses_with_cooldown_store(
            vec![],
            "cred-probe-restart-b",
            &scope,
            clock,
            store.clone(),
        );
        for _ in 0..3 {
            assert!(after
                .claim_exit_probe(160_000, 130_000)
                .await
                .expect("claim"));
        }
        assert!(!after.claim_exit_probe(160_000, 130_000).await.expect("cap"));
        // 新窗口仍可领：有界是按窗口，不是永久封死。
        assert!(
            after
                .claim_exit_probe(200_000, 100_000 + 30_000)
                .await
                .expect("claim"),
            "a later window gets its own probe"
        );
        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn a_418_ip_ban_is_never_carved_out_for_exits() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:exit-ban:{}", ulid::Ulid::new());
        ip_ban_registry()
            .lock()
            .expect("lock IP-ban registry")
            .insert(scope.clone(), 160_000);

        let (client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-exit-ban",
            &scope,
            clock.clone(),
            store.clone(),
        );

        // A 418 means the IP itself is banned: the request would fail anyway and
        // touching during the ban extends it. No scope is exempt.
        let blocked = client
            .place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_exit_ban")
            .await
            .expect_err("an exit must not be carved out of a 418 IP ban");
        assert!(
            blocked.to_string().contains("IP ban cooldown active"),
            "{blocked:#}"
        );
        assert_eq!(requests.lock().expect("lock requests").len(), 0);

        clear_egress_test_state(&scope);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t311_ip_ban_write_is_persisted_independently_of_memory() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(90_000));
        let scope = format!("test-egress:write:{}", ulid::Ulid::new());
        let (client, _, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 418,
                retry_after: None,
                body: r#"{"code":-1003}"#.to_string(),
            }],
            "cred-t311-write",
            &scope,
            clock,
            store.clone(),
        );

        client
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_write"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("418 must be persisted before returning");
        let rows = sqlx::query(
            "SELECT egress_scope, ban_until_ms FROM binance_ip_ban_cooldowns ORDER BY egress_scope",
        )
        .fetch_all(store.pool())
        .await
        .expect("load persisted cooldown set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("egress_scope"),
                row.get::<i64, _>("ban_until_ms"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(rows, vec![(scope, 210_000)]);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t311_restart_loads_active_ip_ban_from_engine_store() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(100_000));
        let scope = format!("test-egress:restart:{}", ulid::Ulid::new());
        sqlx::query(
            "INSERT INTO binance_ip_ban_cooldowns (egress_scope, ban_until_ms, updated_at_ms) VALUES (?, 160000, 100000)",
        )
        .bind(&scope)
        .execute(store.pool())
        .await
        .expect("seed cooldown written by the prior process");
        ip_ban_registry()
            .lock()
            .expect("lock registry")
            .remove(&scope);
        let (restarted_client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-t311-restarted",
            &scope,
            clock,
            store.clone(),
        );

        let error = restarted_client
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_restart"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("restarted process must load the active persisted cooldown");
        assert!(error.to_string().contains("cooldown active"), "{error:#}");
        assert_eq!(requests.lock().expect("lock requests").len(), 0);
        cleanup_retry_test_store(store, path).await;
    }

    /// **sqlx-i64 核查批· 真实毫秒量级的持久化封禁必须还在。**
    ///
    /// 上一条用的是 `100_000` 一类小值（< `i32::MAX`），截断对它是恒等，
    /// 所以它对本缺陷失明。真实进程里 `ban_until_ms` 是毫秒 epoch：
    /// 旧 sqlx 0.7.4 `Any` + SQLite 会读回低 32 位（7.2e8）→ 恒 < `now_ms` →
    /// `active_ban_decision` 判 `Allow`，而 `Allow` 分支还会
    /// `delete_expired_ban` 把这一行**删掉**。于是一次重启就能让一条真实的
    /// 币安 IP 封禁冷却静默消失，进程接着往被封的出口打请求（418 会升级）。
    #[tokio::test]
    async fn t311_restart_loads_an_active_ip_ban_stamped_at_a_real_millisecond_epoch() {
        let (store, path) = retry_test_store().await;
        let now_ms = 1_787_429_492_942_i64;
        let ban_until_ms = now_ms + 120_000;
        let clock = Arc::new(ManualRetryClock::new(now_ms));
        let scope = format!("test-egress:restart-wide:{}", ulid::Ulid::new());
        sqlx::query(
            "INSERT INTO binance_ip_ban_cooldowns (egress_scope, ban_until_ms, updated_at_ms) \
             VALUES (?, ?, ?)",
        )
        .bind(&scope)
        .bind(ban_until_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .expect("seed a cooldown written by the prior process");
        ip_ban_registry()
            .lock()
            .expect("lock registry")
            .remove(&scope);
        let (restarted_client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-t311-restarted-wide",
            &scope,
            clock,
            store.clone(),
        );

        let error = restarted_client
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_restart_wide"),
                RequestClass::EventTruth,
            )
            .await
            .expect_err("a cooldown 120s in the future must still block after restart");
        assert!(error.to_string().contains("cooldown active"), "{error:#}");
        assert_eq!(
            requests.lock().expect("lock requests").len(),
            0,
            "not one byte may go out to a banned egress"
        );
        // 而且那一行必须**还在**——`Allow` 分支会顺手删掉它。
        let remaining = sqlx::query(
            "SELECT COUNT(*) AS n FROM binance_ip_ban_cooldowns WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(store.pool())
        .await
        .expect("count the persisted cooldown")
        .get::<i64, _>("n");
        assert_eq!(remaining, 1, "an active cooldown must not be deleted");
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t311_expired_ip_ban_is_released_and_removed() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(170_000));
        let scope = format!("test-egress:expired:{}", ulid::Ulid::new());
        sqlx::query(
            "INSERT INTO binance_ip_ban_cooldowns (egress_scope, ban_until_ms, updated_at_ms) VALUES (?, 169999, 100000)",
        )
        .bind(&scope)
        .execute(store.pool())
        .await
        .expect("seed expired cooldown");
        ip_ban_registry()
            .lock()
            .expect("lock registry")
            .insert(scope.clone(), 169_999);
        let (client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: r#"{"status":"NEW"}"#.to_string(),
            }],
            "cred-t311-expired",
            &scope,
            clock,
            store.clone(),
        );

        let value = client
            .get_order_json(
                "BTCUSDT",
                None,
                Some("coid_t311_expired"),
                RequestClass::EventTruth,
            )
            .await
            .expect("expired cooldown must release the request");
        assert_eq!(value, serde_json::json!({"status":"NEW"}));
        assert_eq!(requests.lock().expect("lock requests").len(), 1);
        let rows = sqlx::query(
            "SELECT egress_scope, ban_until_ms FROM binance_ip_ban_cooldowns ORDER BY egress_scope",
        )
        .fetch_all(store.pool())
        .await
        .expect("load cooldown set after expiry");
        assert_eq!(rows.len(), 0);
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t311_exit_during_ip_ban_fails_explicitly_without_waiting_or_transport() {
        let (store, path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(180_000));
        let scope = format!("test-egress:exit:{}", ulid::Ulid::new());
        sqlx::query(
            "INSERT INTO binance_ip_ban_cooldowns (egress_scope, ban_until_ms, updated_at_ms) VALUES (?, 240000, 180000)",
        )
        .bind(&scope)
        .execute(store.pool())
        .await
        .expect("seed active exit cooldown");
        let (client, requests, _) = test_client_responses_with_cooldown_store(
            vec![MockResponse {
                status: 200,
                retry_after: None,
                body: order_ack("coid_t311_exit"),
            }],
            "cred-t311-exit",
            &scope,
            clock.clone(),
            store.clone(),
        );

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            client.place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_t311_exit"),
        )
        .await
        .expect("exit cooldown handling must be bounded")
        .expect_err("active IP ban must fail the exit explicitly");
        assert!(result.to_string().contains("cooldown active"), "{result:#}");
        assert_eq!(requests.lock().expect("lock requests").len(), 0);
        assert_eq!(clock.sleeps(), Vec::<u64>::new());
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t38_order_429_retry_reuses_exact_client_id_and_preserves_complete_db_sets() {
        let authority_key = "entry|bot_t38|binance|futures|BTC|USDT|USDT|long|1";
        let client_order_id = "coid_t38_stable";
        let (store, path) = retry_test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (
              'pos_t38', 'cred_t38', 'bot_t38',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'pending',
              'bot_t38|binance|futures|BTC|USDT|USDT|long', ?, '0', 1
            )
            "#,
        )
        .bind(authority_key)
        .execute(store.pool())
        .await
        .expect("insert production-shaped pending position");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange, market_kind, symbol_key,
              action, side, order_type, quantity, reduce_only, order_role, status,
              created_at_ms, updated_at_ms
            ) VALUES (
              'eng_t38', 'cred_t38', 'bot_t38', ?, ?, 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'market',
              '0.001', 0, 'entry', 'pending_submit', 1, 1
            )
            "#,
        )
        .bind(authority_key)
        .bind(client_order_id)
        .execute(store.pool())
        .await
        .expect("insert production-shaped engine order");

        let clock = Arc::new(ManualRetryClock::new(30_000));
        let (client, requests, _) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("0".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: order_ack(client_order_id),
                },
            ],
            "cred_t38",
            clock,
        );
        let client = client.with_test_store(store.clone());
        let ack = client
            .place_market_order("BTCUSDT", "BUY", "0.001", false, client_order_id)
            .await
            .expect("idempotent order retry succeeds");
        assert_eq!(ack.client_order_id, client_order_id);
        sqlx::query(
            "UPDATE engine_orders SET status='acked', exchange_order_id=? WHERE engine_order_id='eng_t38'",
        )
        .bind(ack.order_id.to_string())
        .execute(store.pool())
        .await
        .expect("apply ack to the existing engine order only");

        let request_keys = requests
            .lock()
            .expect("lock requests")
            .iter()
            .map(|request| {
                request_query(request)
                    .get("newClientOrderId")
                    .cloned()
                    .expect("every submit carries client id")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            request_keys,
            vec![client_order_id.to_string(), client_order_id.to_string()]
        );

        let order_rows = sqlx::query(
            "SELECT engine_order_id, idempotency_key, client_order_id_short, status, exchange_order_id FROM engine_orders ORDER BY engine_order_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load complete engine order set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("engine_order_id"),
                row.get::<String, _>("idempotency_key"),
                row.get::<String, _>("client_order_id_short"),
                row.get::<String, _>("status"),
                row.get::<String, _>("exchange_order_id"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            order_rows,
            vec![(
                "eng_t38".to_string(),
                authority_key.to_string(),
                client_order_id.to_string(),
                "acked".to_string(),
                "7001".to_string(),
            )]
        );
        let position_rows = sqlx::query(
            "SELECT position_id, state, pending_idem_key FROM position_state ORDER BY position_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load complete position set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("position_id"),
                row.get::<String, _>("state"),
                row.get::<String, _>("pending_idem_key"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            position_rows,
            vec![(
                "pos_t38".to_string(),
                "pending".to_string(),
                authority_key.to_string(),
            )]
        );
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t311_duplicate_client_id_unknown_reconciles_to_one_complete_order_and_position_set() {
        let authority_key = "entry|bot_t311|binance|futures|BTC|USDT|USDT|long|1";
        let client_order_id = "coid_t311_ambiguous";
        let (store, path) = retry_test_store().await;
        sqlx::query(
            r#"
            INSERT INTO position_state (
              position_id, credential_id, bot_uid, symbol_key, direction,
              entry_epoch, state, active_key, pending_idem_key, qty, updated_at_ms
            ) VALUES (
              'pos_t311', 'cred_t311', 'bot_t311',
              'binance|futures|BTC|USDT|USDT', 'long', 1, 'pending',
              'bot_t311|binance|futures|BTC|USDT|USDT|long', ?, '0', 1
            )
            "#,
        )
        .bind(authority_key)
        .execute(store.pool())
        .await
        .expect("insert ambiguous pending position");
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              client_order_id_short, exchange, market_kind, symbol_key,
              action, side, order_type, quantity, reduce_only, order_role, status,
              created_at_ms, updated_at_ms
            ) VALUES (
              'eng_t311', 'cred_t311', 'bot_t311', ?, ?, 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'market',
              '0.001', 0, 'entry', 'pending_submit', 1, 1
            )
            "#,
        )
        .bind(authority_key)
        .bind(client_order_id)
        .execute(store.pool())
        .await
        .expect("insert ambiguous engine order");

        let submit_clock = Arc::new(ManualRetryClock::new(250_000));
        let (submit_client, submit_requests, _) = test_client_responses_with_cooldown_store(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("0".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 400,
                    retry_after: None,
                    body: r#"{"code":-4111,"msg":"Duplicated clientOrderId"}"#.to_string(),
                },
            ],
            "cred_t311",
            &format!("test-egress:ambiguous:{}", ulid::Ulid::new()),
            submit_clock,
            store.clone(),
        );
        let submit_error = submit_client
            .place_market_order("BTCUSDT", "BUY", "0.001", false, client_order_id)
            .await
            .expect_err("duplicate client id after a 429 is an ambiguous submit error");
        assert!(
            submit_error.to_string().contains("-4111"),
            "{submit_error:#}"
        );
        let submitted_client_ids = submit_requests
            .lock()
            .expect("lock submit requests")
            .iter()
            .map(|request| {
                request_query(request)
                    .get("newClientOrderId")
                    .cloned()
                    .expect("every submit carries the stable client id")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            submitted_client_ids,
            vec![client_order_id.to_string(), client_order_id.to_string()]
        );

        sqlx::query(
            "UPDATE engine_orders SET status='unknown', updated_at_ms=250000 WHERE engine_order_id='eng_t311'",
        )
        .execute(store.pool())
        .await
        .expect("production error path records the ambiguity as unknown");

        let reconcile_clock = Arc::new(ManualRetryClock::new(251_000));
        let open_truth = format!(
            r#"[{{"orderId":8111,"clientOrderId":"{client_order_id}","symbol":"BTCUSDT","status":"NEW","updateTime":251000}}]"#
        );
        let (reconcile_client, reconcile_requests, _) = test_client_responses_with_cooldown_store(
            vec![
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: open_truth,
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: "[]".to_string(),
                },
            ],
            "cred_t311",
            &format!("test-egress:reconcile:{}", ulid::Ulid::new()),
            reconcile_clock,
            store.clone(),
        );
        crate::reconcile::reconcile_binance_futures_truth_with_client(
            "cred_t311",
            store.clone(),
            test_limiter(),
            &reconcile_client,
        )
        .await
        .expect("reconcile finds exchange truth by the exact client id");
        let reconcile_targets = reconcile_requests
            .lock()
            .expect("lock reconcile requests")
            .iter()
            .map(|request| request_path(request))
            .collect::<Vec<_>>();
        assert_eq!(
            reconcile_targets,
            vec![
                "/fapi/v1/openOrders".to_string(),
                "/fapi/v1/openAlgoOrders".to_string(),
                "/fapi/v2/positionRisk".to_string(),
                "/fapi/v2/balance".to_string(),
            ]
        );

        let order_rows = sqlx::query(
            "SELECT engine_order_id, idempotency_key, client_order_id_short, status, exchange_order_id FROM engine_orders ORDER BY engine_order_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load complete converged order set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("engine_order_id"),
                row.get::<String, _>("idempotency_key"),
                row.get::<String, _>("client_order_id_short"),
                row.get::<String, _>("status"),
                row.get::<String, _>("exchange_order_id"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            order_rows,
            vec![(
                "eng_t311".to_string(),
                authority_key.to_string(),
                client_order_id.to_string(),
                "acked".to_string(),
                "8111".to_string(),
            )]
        );
        let position_rows = sqlx::query(
            "SELECT position_id, state, pending_idem_key FROM position_state ORDER BY position_id",
        )
        .fetch_all(store.pool())
        .await
        .expect("load complete converged position set")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("position_id"),
                row.get::<String, _>("state"),
                row.get::<String, _>("pending_idem_key"),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            position_rows,
            vec![(
                "pos_t311".to_string(),
                "pending".to_string(),
                authority_key.to_string(),
            )]
        );
        cleanup_retry_test_store(store, path).await;
    }

    #[tokio::test]
    async fn t38_non_idempotent_cancel_429_queries_truth_without_repeating_delete() {
        let clock = Arc::new(ManualRetryClock::new(40_000));
        let (client, requests, records) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("0".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: r#"{"orderId":7001,"status":"CANCELED"}"#.to_string(),
                },
            ],
            "t38-cancel-truth",
            clock,
        );
        let truth = client
            .cancel_order_json("BTCUSDT", Some("7001"), None, RequestClass::Protection)
            .await
            .expect("429 cancellation resolves from exchange truth");
        assert_eq!(truth["status"], "CANCELED");
        let methods = requests
            .lock()
            .expect("lock requests")
            .iter()
            .map(|request| request.split_whitespace().next().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(methods, vec!["DELETE".to_string(), "GET".to_string()]);
        assert_eq!(
            records.lock().expect("lock records")[0].decision,
            "verify_truth"
        );
    }

    #[tokio::test]
    async fn t38_retry_limit_returns_auditable_error_and_never_consumes_late_success() {
        let (lifecycle_store, lifecycle_path) = retry_test_store().await;
        let clock = Arc::new(ManualRetryClock::new(50_000));
        let mut responses = (0..4)
            .map(|_| MockResponse {
                status: 429,
                retry_after: Some("0".to_string()),
                body: r#"{"code":-1003}"#.to_string(),
            })
            .collect::<Vec<_>>();
        responses.push(MockResponse {
            status: 200,
            retry_after: None,
            body: order_ack("coid_limit"),
        });
        let (client, requests, records) =
            test_client_responses(responses, "t38-limit", clock.clone());
        let client = client.with_test_store(lifecycle_store.clone());
        let error = client
            .place_market_order("BTCUSDT", "BUY", "0.001", false, "coid_limit")
            .await
            .expect_err("retry exhaustion must not become success");
        assert!(
            error.to_string().contains("retry_limit_exhausted"),
            "{error:#}"
        );
        assert_eq!(requests.lock().expect("lock requests").len(), 4);
        assert_eq!(clock.sleeps(), vec![100, 100, 100]);
        assert_eq!(records.lock().expect("lock records").len(), 4);
        assert_eq!(
            records
                .lock()
                .expect("lock records")
                .last()
                .unwrap()
                .decision,
            "retry_limit_exhausted"
        );
        cleanup_retry_test_store(lifecycle_store, lifecycle_path).await;
    }

    #[tokio::test]
    async fn t38_exit_retry_budget_is_bounded_and_shorter_than_entry() {
        let (lifecycle_store, lifecycle_path) = retry_test_store().await;
        let exit_clock = Arc::new(ManualRetryClock::new(60_000));
        let (exit_client, exit_requests, _) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("1".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: order_ack("coid_exit"),
                },
            ],
            "t38-exit",
            exit_clock.clone(),
        );
        let exit_error = exit_client
            .place_market_order("BTCUSDT", "SELL", "0.001", true, "coid_exit")
            .await
            .expect_err("exit must fail explicitly instead of waiting beyond its budget");
        assert!(
            exit_error.to_string().contains("retry_budget_exhausted"),
            "{exit_error:#}"
        );
        assert_eq!(exit_requests.lock().expect("lock requests").len(), 1);
        assert_eq!(exit_clock.sleeps(), Vec::<u64>::new());

        let entry_clock = Arc::new(ManualRetryClock::new(70_000));
        let (entry_client, entry_requests, _) = test_client_responses(
            vec![
                MockResponse {
                    status: 429,
                    retry_after: Some("1".to_string()),
                    body: r#"{"code":-1003}"#.to_string(),
                },
                MockResponse {
                    status: 200,
                    retry_after: None,
                    body: order_ack("coid_entry"),
                },
            ],
            "t38-entry",
            entry_clock.clone(),
        );
        let entry_client = entry_client.with_test_store(lifecycle_store.clone());
        entry_client
            .place_market_order("BTCUSDT", "BUY", "0.001", false, "coid_entry")
            .await
            .expect("entry can use the longer normal retry budget");
        assert_eq!(entry_requests.lock().expect("lock requests").len(), 2);
        assert_eq!(entry_clock.sleeps(), vec![1_000]);
        cleanup_retry_test_store(lifecycle_store, lifecycle_path).await;
    }

    #[test]
    fn maps_symbol_key() {
        assert_eq!(
            symbol_from_symbol_key("binance|futures|BTC|USDT|USDT").as_deref(),
            Some("BTCUSDT")
        );
    }

    /// **段数是身份的一部分。**
    ///
    /// R10 的判据是「至少四段」，于是评审的 R7-01 变异（收紧为「恰好五段」）
    /// 23/23 存活——整套夹具里**没有一条**四段或六段的向量，段数根本没被测过。
    /// 这一条与下面的 settle 条都是**正交夹具**：先让形状可被观察，
    /// 判据才谈得上被钉住。
    #[test]
    fn symbol_key_shape_is_exact_five_segments() {
        assert!(
            symbol_key_identity("binance|futures|BTC").is_none(),
            "three segments carry no settle and no quote; it is not an identity"
        );
        assert!(
            symbol_key_identity("binance|futures|BTC|USDT").is_none(),
            "four segments drop the settle asset; accepting them lets a key whose \
             settle is unknown pass as a complete identity"
        );
        assert!(
            symbol_key_identity("binance|futures|BTC|USDT|USDT").is_some(),
            "five segments are the canonical shape"
        );
        assert!(
            symbol_key_identity("binance|futures|BTC|USDT|USDT|extra").is_none(),
            "a sixth segment means the producer used a different key grammar; \
             silently ignoring it makes two grammars share one identity space"
        );
    }

    /// **settle 段参与身份，不得被投影折掉**（regression）。
    ///
    /// 评审变异 R7-02（settle 投影改用最后一段）23/23 存活，因为没有任何
    /// 夹具让 `parts[4]` 与 `parts.last()` 不同、也没有任何夹具读过 settle。
    #[test]
    fn symbol_key_identity_carries_every_segment() {
        let identity =
            symbol_key_identity("binance|futures|BTC|USDT|USDC").expect("five segments parse");
        assert_eq!(identity.venue, "binance");
        assert_eq!(identity.market_kind, "futures");
        assert_eq!(identity.base, "BTC");
        assert_eq!(identity.quote, "USDT");
        assert_eq!(
            identity.settle, "USDC",
            "settle must be read from its own segment, not folded away"
        );
        assert_eq!(identity.symbol, "BTCUSDT");
        assert!(
            symbol_key_identity("binance|futures|BTC|USDT| ").is_none(),
            "a blank settle segment is not a settle asset"
        );
    }

    /// **提交侧的完整身份判据**。
    ///
    /// 两个身份来源（intent 的外层字段与 key 内部段）必须逐段一致，
    /// 且整条 key 必须落在 USDⓈ-M 闭世界内。四条拒绝面 + 一条放行面。
    #[test]
    fn usdm_submission_identity_pins_both_identity_sources() {
        let ok = usdm_submission_identity("binance|futures|BTC|USDT|USDT", "binance", "futures")
            .expect("the canonical USDⓈ-M identity submits");
        assert_eq!(ok.symbol, "BTCUSDT");
        assert_eq!(ok.settle, "USDT");

        let venue = usdm_submission_identity("okx|spot|BTC|USDT|USDT", "binance", "futures")
            .expect_err("a key that declares another venue must be refused");
        assert!(venue.contains("declares venue"), "{venue}");

        let market = usdm_submission_identity("binance|spot|BTC|USDT|USDT", "binance", "futures")
            .expect_err("a key that declares another market_kind must be refused");
        assert!(market.contains("declares market_kind"), "{market}");

        let settle = usdm_submission_identity("binance|futures|BTC|USDT|BTC", "binance", "futures")
            .expect_err("a coin-margined key is a different contract");
        assert!(settle.contains("USDⓈ-M"), "{settle}");

        let shape = usdm_submission_identity("binance|futures|BTC|USDT", "binance", "futures")
            .expect_err("a four-segment key has no settle asset");
        assert!(shape.contains("5-segment"), "{shape}");

        // 大小写与空白不是身份差异——否则这条判据会在真实调用方那里误伤。
        assert!(usdm_submission_identity(
            "BINANCE|FUTURES|BTC|USDT|USDT",
            " binance ",
            " Futures "
        )
        .is_ok());
    }

    /// **一致性与闭世界归属是两件事。**
    ///
    /// 两者的处置不同，判据也必须分得开：
    ///
    /// * 整行老老实实写着 okx/spot —— 两源一致，只是不属于本引擎的市场，
    ///   调用方跳过即可；
    /// * 外层 okx/spot + 内层 `binance|futures|…` —— 两源不一致，这一行
    ///   自己在撒谎，调用方必须 fail-closed。
    ///
    /// 把它们混成一个 `Result` 会让"别的市场的历史行"把整帧带成失败，
    /// 或者反过来让矛盾行被当成"别的市场"静默跳过。
    #[test]
    fn identity_source_agreement_is_separate_from_closed_world_membership() {
        // 两源一致 + USDⓈ-M：两条判据都通过。
        assert!(trading_identity_sources_agree(
            "binance|futures|BTC|USDT|USDT",
            "binance",
            "futures"
        )
        .is_ok());
        assert!(
            usdm_identity_agreeing_with("binance|futures|BTC|USDT|USDT", "binance", "futures")
                .is_ok()
        );

        // 两源一致但不是 USDⓈ-M：一致性通过，闭世界归属不通过。
        assert!(trading_identity_sources_agree("okx|spot|BTC|USDT|USDT", "okx", "spot").is_ok());
        let membership = usdm_identity_agreeing_with("okx|spot|BTC|USDT|USDT", "okx", "spot")
            .expect_err("another venue is not this engine's closed world");
        assert!(membership.contains("USDⓈ-M"), "{membership}");

        // 两源不一致：一致性判据本身必须拒绝，两个方向都要。
        let outer_lies =
            trading_identity_sources_agree("binance|futures|BTC|USDT|USDT", "okx", "spot")
                .expect_err("an outer declaration that contradicts the key must be refused");
        assert!(outer_lies.contains("declares venue"), "{outer_lies}");
        let inner_lies =
            trading_identity_sources_agree("okx|spot|BTC|USDT|USDT", "binance", "futures")
                .expect_err("a key that contradicts its row must be refused");
        assert!(inner_lies.contains("declares venue"), "{inner_lies}");
        let market_lies =
            trading_identity_sources_agree("binance|spot|BTC|USDT|USDT", "binance", "futures")
                .expect_err("a market_kind mismatch is a mismatch");
        assert!(
            market_lies.contains("declares market_kind"),
            "{market_lies}"
        );
    }

    /// `usdm_symbol` 是交易所边界上唯一的 symbol 来源：闭世界之外没有 symbol。
    #[test]
    fn usdm_symbol_has_no_bare_fallback() {
        assert_eq!(
            usdm_symbol("binance|futures|BTC|USDT|USDT").as_deref(),
            Some("BTCUSDT")
        );
        for refused in [
            "okx|spot|BTC|USDT|USDT",
            "okx|futures|BTC|USDT|USDT",
            "binance|spot|BTC|USDT|USDT",
            "binance|futures|BTC|USDT|BTC",
            "binance|futures|BTC|USDT",
        ] {
            assert!(
                usdm_symbol(refused).is_none(),
                "{refused} must not resolve to a USDⓈ-M symbol"
            );
            assert!(
                symbol_from_symbol_key(refused).is_some() || refused == "binance|futures|BTC|USDT",
                "fixture sanity: {refused} is exactly the shape the bare projection accepted"
            );
        }
    }

    /// **USDⓈ-M 的完整交易身份**：venue/market/symbol/settle 四者齐全才算命中。
    ///
    /// 这是 WS 与 reconcile 共用的那一个判据（regression / regression）。
    #[test]
    fn usdm_identity_match_requires_every_segment_to_agree() {
        assert!(usdm_symbol_key_matches(
            "binance|futures|BTC|USDT|USDT",
            "BTCUSDT"
        ));
        assert!(usdm_symbol_key_matches(
            "binance|futures|BTC|USDT|USDT",
            "btcusdt"
        ));
        assert!(
            !usdm_symbol_key_matches("okx|futures|BTC|USDT|USDT", "BTCUSDT"),
            "venue must participate"
        );
        assert!(
            !usdm_symbol_key_matches("binance|spot|BTC|USDT|USDT", "BTCUSDT"),
            "market kind must participate"
        );
        assert!(
            !usdm_symbol_key_matches("binance|futures|BTC|USDT|BTC", "BTCUSDT"),
            "a coin-margined settle asset is a different contract; USDS-M truth \
             must not be applied to it"
        );
        assert!(
            !usdm_symbol_key_matches("binance|futures|ETH|USDT|USDT", "BTCUSDT"),
            "symbol must participate"
        );
        assert!(
            !usdm_symbol_key_matches("binance|futures|BTC|USDT", "BTCUSDT"),
            "an incomplete key is not a complete identity"
        );
    }

    #[test]
    fn explicit_venue_does_not_fall_back_to_global_keys() {
        assert!(keys_from_values(
            Some(crate::venue::Venue::Testnet),
            None,
            None,
            Some("global-key"),
            Some("global-secret"),
        )
        .is_none());
    }

    #[test]
    fn explicit_venue_missing_per_key_does_not_fall_back_to_global_key() {
        assert!(keys_from_values(
            Some(crate::venue::Venue::Testnet),
            None,
            Some("testnet-secret"),
            Some("global-key"),
            Some("global-secret"),
        )
        .is_none());
    }

    #[test]
    fn explicit_venue_missing_per_secret_does_not_fall_back_to_global_secret() {
        assert!(keys_from_values(
            Some(crate::venue::Venue::Testnet),
            Some("testnet-key"),
            None,
            Some("global-key"),
            Some("global-secret"),
        )
        .is_none());
    }

    #[test]
    fn unconfigured_venue_preserves_global_key_fallback() {
        let keys = keys_from_values(None, None, None, Some("global-key"), Some("global-secret"))
            .expect("legacy global fallback must remain available without a venue");
        assert_eq!(keys.api_key, "global-key");
        assert_eq!(keys.api_secret, "global-secret");
    }

    ///     /// **WebSocket 空闲上限的行为级证明，不需要监听端口。**
    ///
    /// 驱动的生产函数是 `binance_futures::next_frame_within`——kline 与
    /// user-stream 两条读取循环**唯一**的取帧入口。喂一个永不产出的流
    /// （对端握手成功后再不发 frame、也不 close 的等价物），
    /// 断言它在上限内返回 `IdleTimeout` 而不是挂住。
    ///
    /// 与两条 HTTP 慢服务用例不同，这条**在拒绝 bind 的沙箱里同样可测**。
    /// **写侧也要有上界。**
    ///
    /// R7 只给 read/connect 装了上界，Pong 的 `send(...).await` 仍无界——
    /// 对端不读时 TCP 写缓冲填满，`send` 永远挂在 await 上，
    /// 表现与 read 挂住一模一样。
    ///
    /// 与读侧同样**不需要监听端口**：喂一个永远收不下的 sink 即可。
    #[tokio::test]
    async fn a_websocket_write_that_never_completes_must_hit_the_deadline() {
        // 永远 pending 的 sink：`poll_ready` 从不就绪，等价于写缓冲满且对端不读。
        struct NeverReady;
        impl futures_util::Sink<u8> for NeverReady {
            type Error = std::io::Error;
            fn poll_ready(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }
            fn start_send(self: std::pin::Pin<&mut Self>, _item: u8) -> Result<(), Self::Error> {
                Ok(())
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }
            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }
        }

        let mut stuck = NeverReady;
        // 用例自带上限：deadline 被摘掉时必须**快速失败**而不是挂住。
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::send_frame_within(std::time::Duration::from_millis(200), &mut stuck, 1u8),
        )
        .await
        .expect("the write must be bounded by its own deadline, not by the harness");
        let error = outcome.expect_err("a write that never completes must not report success");
        assert!(
            error.to_string().contains("deadline"),
            "the failure must say it was a deadline, not just 'failed': {error}"
        );

        // 自证（放行面）：能收的 sink 必须照常成功，收紧不得误伤。
        let mut ok: Vec<u8> = Vec::new();
        super::send_frame_within(std::time::Duration::from_millis(200), &mut ok, 9u8)
            .await
            .expect("a healthy sink must still accept the frame");
        assert_eq!(ok, vec![9u8]);
    }

    #[tokio::test]
    async fn a_websocket_that_never_sends_a_frame_must_hit_the_idle_deadline() {
        let mut dead = futures_util::stream::pending::<u8>();
        let started = std::time::Instant::now();
        // 用例自带上限：deadline 一旦被摘掉，这里必须**快速失败**而不是挂住。
        let poll = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::next_frame_within(std::time::Duration::from_millis(200), &mut dead),
        )
        .await
        .expect("the read must be bounded by its own deadline, not by the harness");
        assert_eq!(poll, super::StreamPoll::IdleTimeout);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));

        // **自证夹具（放行面）**：有帧就必须原样取出来，不能被超时误伤。
        let mut alive = futures_util::stream::iter(vec![7u8, 8u8]);
        assert_eq!(
            super::next_frame_within(std::time::Duration::from_millis(200), &mut alive).await,
            super::StreamPoll::Frame(7u8)
        );

        // **空闲超时与"对端正常结束"必须是两个值**：塌成一个，
        // 上层就分不出"流死了"和"流关了"，只会静默重连并把中断当正常。
        let mut ended = futures_util::stream::empty::<u8>();
        assert_eq!(
            super::next_frame_within(std::time::Duration::from_millis(200), &mut ended).await,
            super::StreamPoll::Ended
        );
    }

    #[test]
    fn ws_deadlines_have_bounded_defaults_and_reject_nonsense_overrides() {
        let _serial = env_serial_guard();
        let _connect_unset = TestEnvVar::unset("BINANCE_WS_CONNECT_TIMEOUT_MS".to_string());
        let _idle_unset = TestEnvVar::unset("BINANCE_WS_IDLE_TIMEOUT_MS".to_string());
        let (connect, idle) = super::ws_timeouts_from_env();
        assert_eq!(connect, std::time::Duration::from_millis(10_000));
        // 空闲上限必须**大于** Binance 的 ~3 分钟 ping 间隔，否则健康的流会被
        // 反复误杀；同时必须有限，否则等于没装。
        assert_eq!(idle, std::time::Duration::from_millis(300_000));
        assert!(idle > std::time::Duration::from_secs(180));
        assert!(connect > std::time::Duration::ZERO);
    }

    /// 源码守卫（**第二层**，行为证据见上面那条）：两条 WebSocket 的读取都必须
    /// 经过共享入口，不得有人再写一个裸 `read.next().await` 循环。
    #[test]
    fn both_websocket_read_loops_go_through_the_bounded_entry() {
        for source in [
            include_str!("market_data.rs"),
            include_str!("binance_user_stream.rs"),
        ] {
            let code = source
                .lines()
                .map(|line| match line.find("//") {
                    Some(index) => &line[..index],
                    None => line,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(code.contains("next_frame_within("));
            let unbounded = format!("{}{}", "while let Some(msg) = read.", "next().await");
            assert!(!code.contains(&unbounded));
        }
    }

    #[test]
    fn http_deadlines_have_bounded_defaults_and_reject_nonsense_overrides() {
        // 设计要求 已裁定的 B3 hard blocker：关键交易所 I/O 必须有**有界 deadline**。
        // 与改写 `BINANCE_HTTP_*` 的用例互斥：这条读的正是那两个变量的默认值，
        // 并发下会读到别人临时设的 300ms（段级复评的测试缺口 #9）。
        let _serial = env_serial_guard();
        let _timeout_unset = TestEnvVar::unset("BINANCE_HTTP_TIMEOUT_MS".to_string());
        let _connect_unset = TestEnvVar::unset("BINANCE_HTTP_CONNECT_TIMEOUT_MS".to_string());
        let (request, connect) = super::http_timeouts_from_env();
        assert_eq!(request, std::time::Duration::from_millis(10_000));
        assert_eq!(connect, std::time::Duration::from_millis(5_000));
        assert!(request > std::time::Duration::ZERO);
        assert!(connect > std::time::Duration::ZERO);
    }

    /// 环境变量：置为 `1` 时，**拿不到本地监听端口就是失败**，不是跳过。
    ///
    /// 默认（未设置）行为是跳过并打印原因——见
    /// `try_spawn_never_responding_server` 的说明。开发机与能开 socket 的 CI
    /// 应当把它设成 `1`，让"永远在跳过"这种退化被看见。
    const REQUIRE_LOCAL_SOCKET_TESTS: &str = "RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS";

    /// 拿不到本地监听端口时的统一处置：要么按显式要求打红，要么明确跳过。
    ///
    /// ****：R5 的两条慢服务
    /// 用例直接 `expect("bind local slow server")`，在拒绝 bind 的受限沙箱里
    /// **直接变红**，并把 `gate:l4`（rc=3）与 `gate:l5`（rc=1）一起带红。
    /// 也就是说它们**不可移植**——不只是"测不出"，而是会让门禁在别的环境里假红。
    ///
    /// 这里的取舍是明写的：没有本地 socket 就**产生不出**"deadline 真的把一个
    /// 永久 pending 的请求结束掉"这条行为级证据，本仓不接受用源码守卫冒充它
    /// （"存在≠生效"）。因此默认跳过并**大声说明原因**，同时提供
    /// `RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS=1` 让"能 bind 的环境"把跳过变成失败。
    fn skip_without_local_socket(test: &str, error: &std::io::Error) {
        if std::env::var(REQUIRE_LOCAL_SOCKET_TESTS)
            .map(|value| value.trim() == "1")
            .unwrap_or(false)
        {
            panic!(
                "{test}: {REQUIRE_LOCAL_SOCKET_TESTS}=1 要求本地监听端口，但 bind 失败: {error}"
            );
        }
        eprintln!(
            "SKIPPED {test}: 本环境不允许 bind 127.0.0.1:0（{error}）。\n\
             deadline 的行为级证据需要一个「接受连接但永不响应」的本地服务；\n\
             拿不到 socket 就产生不出该证据，本用例不以源码守卫冒充它。\n\
             在允许 bind 的环境里设 {REQUIRE_LOCAL_SOCKET_TESTS}=1 可把跳过变成失败。"
        );
    }

    /// 启一个**接受连接但永不响应**的本地服务；拿不到监听端口时返回 `None`。
    ///
    /// R4 用 `10.255.255.1` 黑洞地址，段级复评实测该用例
    /// **因为错误的理由通过**——沙箱对该地址立刻返回网络错误，0.00s 就绿了，
    /// 删掉 deadline 装配也照样绿。改用本地慢服务：连接一定成功，
    /// 之后永远不回数据，**只有 request timeout 能把它结束**。
    ///
    /// bind 失败不再 `expect` 打红（见 `skip_without_local_socket`）。
    async fn try_spawn_never_responding_server() -> Result<std::net::SocketAddr, std::io::Error> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        // 握手完成后既不读也不写，只把连接握在手里。
                        tokio::spawn(async move {
                            let _held = stream;
                            futures_util::future::pending::<()>().await;
                        });
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(addr)
    }

    #[tokio::test]
    async fn a_client_built_through_the_shared_assembly_point_actually_times_out() {
        let addr = match try_spawn_never_responding_server().await {
            Ok(addr) => addr,
            Err(error) => {
                skip_without_local_socket(
                    "a_client_built_through_the_shared_assembly_point_actually_times_out",
                    &error,
                );
                return;
            }
        };
        let client = super::apply_http_deadlines(
            reqwest::Client::builder(),
            (
                std::time::Duration::from_millis(300),
                std::time::Duration::from_millis(300),
            ),
        )
        .build()
        .expect("build client");

        let started = std::time::Instant::now();
        // **用例自带上限**：deadline 一旦被摘掉，这里必须**快速失败**而不是
        // 无限挂住——挂住的测试在 CI 里等于卡死流水线，而且看起来像环境问题。
        //
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.get(format!("http://{addr}/")).send(),
        )
        .await
        .expect("client must be bounded by its own deadline, not by the test harness");
        let elapsed = started.elapsed();
        let error = result.expect_err("a never-responding server must not yield a response");
        // 必须是**超时**，不是别的网络错误——这条区分正是 R4 缺的那一步。
        assert!(error.is_timeout(), "expected a timeout, got: {error}");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "request must be bounded by the deadline, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn the_production_build_path_carries_the_same_deadlines() {
        // 段级复评的 RCHK-N6：生产 `build` 绕开 helper 时测试全绿——
        // 因为用例只调用 helper，没观察生产装配边。这条从**生产入口**
        // 建出 client 再打同一个慢服务，绕开 helper 即变红。
        let addr = match try_spawn_never_responding_server().await {
            Ok(addr) => addr,
            Err(error) => {
                skip_without_local_socket(
                    "the_production_build_path_carries_the_same_deadlines",
                    &error,
                );
                return;
            }
        };
        // **进程级 env 必须串行且可复原**（段级复评的测试缺口 #9）：
        // R5 这里直接 `set_var` / `remove_var`，既没串行也没恢复旧值，
        // 会与同进程的默认 deadline 用例竞态，还会擦掉调用者的配置。
        let _serial = env_serial_guard();
        let _timeout = TestEnvVar::set("BINANCE_HTTP_TIMEOUT_MS".to_string(), "300");
        let _connect = TestEnvVar::set("BINANCE_HTTP_CONNECT_TIMEOUT_MS".to_string(), "300");
        let _base_url = TestEnvVar::set(
            "BINANCE_FUTURES_BASE_URL".to_string(),
            &format!("http://{addr}"),
        );

        // 用 `new_with_store` 而不是 `new`：后者要求进程级 cooldown store 已注册，
        // 那是另一条判据；这里只观察**生产 build 的 deadline 装配边**。
        // 两者共用同一个 `build`，因此这条仍然钉的是生产装配路径。
        static PROBE_DRIVERS: std::sync::Once = std::sync::Once::new();
        PROBE_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let db_path =
            std::env::temp_dir().join(format!("rustcore-deadline-probe-{}.db", ulid::Ulid::new()));
        let store = crate::store::Store::connect(&format!("sqlite://{}", db_path.display()))
            .await
            .expect("connect probe store");
        let client = BinanceFuturesClient::new_with_store(
            BinanceKeys {
                api_key: "k".to_string(),
                api_secret: "s".to_string(),
            },
            "cred_deadline_probe",
            store.clone(),
        );

        drop(_base_url);
        // **不留逃生口**：构造失败就是失败。R5 首版在这里 `return`，
        // 于是 N6 变异（生产 build 绕开 helper）走到这条早退而**假绿**——
        // "跳过"在变异扫描下与"通过"无法区分，正是空跑绿的经典形态。
        let client = client.expect("production client must be constructible in this fixture");

        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.create_listen_key(),
        )
        .await
        .expect("production client must be bounded by its own deadline");
        let elapsed = started.elapsed();
        // env 由 `TestEnvVar` 的 `Drop` 复原（含"原本就没设"这一侧）。
        // **必须是超时**——"任意错误"会让这条用例因为错误的理由通过
        // （R5 首版就是那样：base URL env 名字写错，请求打到真实 Binance 被
        // 沙箱瞬间拒绝，于是 N6 变异照样全绿）。
        let error = result.expect_err("a never-responding server must not succeed");
        // `create_listen_key` 把底层错误包成 anyhow，这里顺链找 reqwest 的超时标记。
        let is_timeout = error.chain().any(|cause| {
            cause
                .downcast_ref::<reqwest::Error>()
                .map(|err| err.is_timeout())
                .unwrap_or(false)
        });
        assert!(
            is_timeout,
            "production client must fail with a timeout, got: {error:#}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "production client must be bounded too, took {elapsed:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn b1_keys_from_env_never_falls_back_to_global_for_paper_namespace() {
        // `BINANCE_API_KEY`/`BINANCE_API_SECRET` 是进程级全局名：不持串行锁，
        // 并发用例会在本用例窗口里读到这对假密钥。
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _key = TestEnvVar::set("BINANCE_API_KEY".to_string(), "global-mainnet-key");
        let _secret = TestEnvVar::set("BINANCE_API_SECRET".to_string(), "global-mainnet-secret");

        // 先自证夹具有效：一个普通交易所凭据在同样环境下**确实**拿得到全局密钥。
        let control = super::keys_from_env("b1-control-cred").unwrap();
        assert!(
            control.is_some(),
            "夹具失效：全局密钥没生效，后面的 None 断言将是空断言"
        );

        for cred in [
            "paper",
            "paper:12345678901234567890123456789012",
            "paper:1234567890123456789012345678901", // 31 位
            "paper:123456789012345678901234567890123", // 33 位
            "paper-trading",
        ] {
            assert!(
                super::keys_from_env(cred).unwrap().is_none(),
                "{cred:?} 解析出了交易所密钥"
            );
        }
    }
}
