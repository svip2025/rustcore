use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

use crate::protection::OrderRole;
use crate::rate_limiter::{
    deterministic_jitter_ms, parse_retry_after_ms, retry_decision, CredentialRateLimiter,
    RetryClass, RetryDecision, RetryInput, RetryScope, TokenBucketConfig,
};
use crate::svc::{Clock, SystemClock};
use crate::venue::Venue;

const DEFAULT_FILTER_TTL: Duration = Duration::from_secs(300);
const EXCHANGE_INFO_PATH: &str = "/fapi/v1/exchangeInfo";
const MARK_PRICE_PATH: &str = "/fapi/v1/premiumIndex";

type FetchFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SymbolFilters {
    min_qty: ExactDecimal,
    max_qty: ExactDecimal,
    step_size: ExactDecimal,
    min_price: ExactDecimal,
    max_price: ExactDecimal,
    tick_size: ExactDecimal,
    min_notional: ExactDecimal,
    multiplier_down: ExactDecimal,
    multiplier_up: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExchangeConstraints {
    pub filters: SymbolFilters,
    pub mark_price: String,
    // Conservative observation time: start of the filter/quote lookup that supplied this
    // quote, retained across cache hits. Close-only sentinel prices have none.
    pub mark_price_as_of_ms: Option<i64>,
    pub stale_filters: bool,
}

pub(crate) trait ExchangeFilterSource: Send + Sync {
    fn fetch_filters<'a>(&'a self, venue: Venue, symbol: &'a str)
        -> FetchFuture<'a, SymbolFilters>;

    fn fetch_mark_price<'a>(&'a self, venue: Venue, symbol: &'a str) -> FetchFuture<'a, String>;
}

#[derive(Clone)]
struct BinanceExchangeFilterSource {
    http: reqwest::Client,
    clock: Arc<dyn Clock>,
    ban_until_ms: Arc<Mutex<i64>>,
    cooldown_store: Option<crate::store::Store>,
    #[cfg(test)]
    mock_responses: Option<Arc<Mutex<std::collections::VecDeque<PublicHttpResponse>>>>,
}

#[derive(Clone, Debug)]
struct PublicHttpResponse {
    status: u16,
    retry_after: Option<String>,
    body: String,
}

/// 发布一次公共端点观察到的封禁/限流到共享出口表。
///
/// 发布失败**绝不能**让取过滤器本身失败——它只是让别的路径看得见，不是这次请求的
/// 前置条件。所以这里吞掉错误并记一条 warn。
async fn publish_public_cooldown(venue: Venue, status: u16, cooldown_ms: u64, now_ms: i64) {
    match crate::binance_futures::publish_public_endpoint_cooldown(
        Some(venue),
        status,
        cooldown_ms,
        now_ms,
    )
    .await
    {
        Ok(egress_scope) => tracing::warn!(
            credential = "public_market_data",
            %egress_scope,
            upstream_status = status,
            cooldown_ms,
            "public endpoint cooldown published to the shared egress registry"
        ),
        Err(error) => tracing::warn!(
            credential = "public_market_data",
            upstream_status = status,
            %error,
            "could not publish the public endpoint cooldown; credentialed paths stay unaware"
        ),
    }
}

impl BinanceExchangeFilterSource {
    fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .expect("static exchange-filter HTTP client configuration is valid"),
            clock: Arc::new(SystemClock),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: crate::binance_futures::registered_cooldown_store().ok(),
            #[cfg(test)]
            mock_responses: None,
        }
    }

    async fn fetch_public(&self, venue: Venue, path: &str, symbol: &str) -> Result<String, String> {
        let started_at_ms = self.clock.now_ms();
        let mut completed_retries = 0_u32;
        loop {
            let now_ms = self.clock.now_ms();
            let egress_scope = crate::binance_futures::public_endpoint_egress_scope(Some(venue))
                .map_err(|error| format!("derive public Binance egress scope failed: {error}"))?;
            let shared = crate::binance_futures::shared_egress_cooldown(
                &egress_scope,
                self.cooldown_store.as_ref(),
                now_ms,
            )
            .await
            .map_err(|error| format!("read shared Binance egress cooldown failed: {error}"))?;
            let ban_until_ms = {
                let mut mirror = self
                    .ban_until_ms
                    .lock()
                    .map_err(|_| "public Binance IP-ban lock poisoned".to_string())?;
                *mirror = (*mirror).max(shared.ban_until_ms);
                *mirror
            };
            if ban_until_ms > now_ms {
                return Err(format!(
                    "public Binance IP ban cooldown active for {path} until_ms={ban_until_ms}"
                ));
            }
            if shared.rate_limit_until_ms > now_ms {
                return Err(format!(
                    "public Binance rate-limit cooldown active for {path} until_ms={}",
                    shared.rate_limit_until_ms
                ));
            }
            let response = self.fetch_public_once(venue, path, symbol).await?;
            if (200..300).contains(&response.status) {
                return Ok(response.body);
            }
            if response.status != 418 && response.status != 429 {
                return Err(format!("{} http {}", endpoint_name(path), response.status));
            }
            let elapsed_ms = self.clock.now_ms().saturating_sub(started_at_ms).max(0) as u64;
            let decision = retry_decision(RetryInput {
                status: response.status,
                class: RetryClass::ReadOnly,
                scope: RetryScope::Normal,
                completed_retries,
                elapsed_ms,
                retry_after_ms: parse_retry_after_ms(
                    response.retry_after.as_deref(),
                    self.clock.now_ms(),
                ),
                jitter_ms: deterministic_jitter_ms(
                    &format!("public_market_data|{venue}|{path}|{symbol}"),
                    completed_retries + 1,
                ),
            });
            match decision {
                RetryDecision::Retry {
                    after_ms,
                    retry_number,
                } => {
                    tracing::warn!(
                        credential = "public_market_data",
                        endpoint = path,
                        http_status = response.status,
                        retry_after = response.retry_after.as_deref().unwrap_or("absent"),
                        retry_number,
                        next_wait_ms = after_ms,
                        request_class = "read_only",
                        "Binance public market-data rate-limit retry"
                    );
                    self.clock.sleep(Duration::from_millis(after_ms)).await;
                    completed_retries = retry_number;
                }
                RetryDecision::IpBan { cooldown_ms } => {
                    tracing::error!(
                        credential = "public_market_data",
                        endpoint = path,
                        http_status = 418_u16,
                        retry_after = response.retry_after.as_deref().unwrap_or("absent"),
                        retry_number = 0_u32,
                        next_wait_ms = cooldown_ms,
                        request_class = "ip_ban",
                        "Binance public endpoint IP ban; retry disabled"
                    );
                    return Err(format!(
                        "{} http 418 IP ban; cooldown_ms={cooldown_ms}",
                        endpoint_name(path)
                    ));
                }
                RetryDecision::GiveUp { reason } => {
                    tracing::warn!(
                        credential = "public_market_data",
                        endpoint = path,
                        http_status = response.status,
                        retry_after = response.retry_after.as_deref().unwrap_or("absent"),
                        retry_number = completed_retries,
                        next_wait_ms = 0_u64,
                        request_class = "read_only",
                        %reason,
                        "Binance public market-data retry abandoned"
                    );
                    return Err(format!(
                        "{} 429 retry abandoned after {} retries: {reason}",
                        endpoint_name(path),
                        completed_retries
                    ));
                }
                RetryDecision::VerifyTruth { .. } => {
                    return Err("public read unexpectedly requested truth lookup".to_string());
                }
            }
        }
    }

    async fn fetch_public_once(
        &self,
        venue: Venue,
        path: &str,
        symbol: &str,
    ) -> Result<PublicHttpResponse, String> {
        let egress_scope = crate::binance_futures::public_endpoint_egress_scope(Some(venue))
            .map_err(|error| format!("derive public Binance egress scope failed: {error}"))?;
        let request_limiter =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(Some(venue)));
        request_limiter
            .acquire_reserving(
                &egress_scope,
                match path {
                    EXCHANGE_INFO_PATH | MARK_PRICE_PATH => 1.0,
                    _ => 1.0,
                },
                crate::binance_futures::exit_weight_reserve_from_env(),
            )
            .await;
        let transport_gate = crate::binance_futures::transport_gate_for_scope(&egress_scope);
        let _transport_guard = transport_gate.lock().await;
        // The pre-bucket read in `fetch_public` is only a fast rejection. A
        // sibling may publish 429/418 while this request waits for tokens or
        // the shared transport gate, so the in-gate read is authoritative.
        let now_ms = self.clock.now_ms();
        let shared = crate::binance_futures::shared_egress_cooldown(
            &egress_scope,
            self.cooldown_store.as_ref(),
            now_ms,
        )
        .await
        .map_err(|error| format!("read authoritative Binance egress cooldown failed: {error}"))?;
        if shared.ban_until_ms > now_ms {
            return Err(format!(
                "public Binance IP ban cooldown active for {path} until_ms={}",
                shared.ban_until_ms
            ));
        }
        if shared.rate_limit_until_ms > now_ms {
            return Err(format!(
                "public Binance rate-limit cooldown active for {path} until_ms={}",
                shared.rate_limit_until_ms
            ));
        }

        let response = self.send_public_transport(venue, path, symbol).await?;
        // Publish before releasing the transport gate. Credentialed requests
        // take this same gate, so none can pass its final check and send in the
        // response-to-publication gap.
        match response.status {
            429 => {
                let cooldown_ms = crate::binance_futures::rate_limit_cooldown_ms(
                    response.retry_after.as_deref(),
                    self.clock.now_ms(),
                );
                publish_public_cooldown(venue, 429, cooldown_ms, self.clock.now_ms()).await;
            }
            418 => {
                let cooldown_ms =
                    parse_retry_after_ms(response.retry_after.as_deref(), self.clock.now_ms())
                        .unwrap_or_default()
                        .max(crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS);
                publish_public_cooldown(venue, 418, cooldown_ms, self.clock.now_ms()).await;
            }
            _ => {}
        }
        if path == EXCHANGE_INFO_PATH {
            if let Some(observed_budget_1m) = parse_request_weight_limit_1m(&response.body) {
                request_limiter.observe_egress_budget(
                    &egress_scope,
                    observed_budget_1m,
                    self.clock.now_ms(),
                );
            }
        }
        Ok(response)
    }

    async fn send_public_transport(
        &self,
        venue: Venue,
        path: &str,
        symbol: &str,
    ) -> Result<PublicHttpResponse, String> {
        #[cfg(test)]
        if let Some(responses) = self.mock_responses.as_ref() {
            return responses
                .lock()
                .map_err(|_| "public response queue lock poisoned".to_string())?
                .pop_front()
                .ok_or_else(|| "public response queue exhausted".to_string());
        }
        let response = self
            .http
            .get(public_endpoint_url(venue, path))
            .query(&[("symbol", symbol)])
            .send()
            .await
            .map_err(|error| format!("{} request failed: {error}", endpoint_name(path)))?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response
            .text()
            .await
            .map_err(|error| format!("{} body read failed: {error}", endpoint_name(path)))?;
        Ok(PublicHttpResponse {
            status,
            retry_after,
            body,
        })
    }
}

fn endpoint_name(path: &str) -> &'static str {
    if path == EXCHANGE_INFO_PATH {
        "exchangeInfo"
    } else {
        "mark price"
    }
}

impl ExchangeFilterSource for BinanceExchangeFilterSource {
    fn fetch_filters<'a>(
        &'a self,
        venue: Venue,
        symbol: &'a str,
    ) -> FetchFuture<'a, SymbolFilters> {
        Box::pin(async move {
            #[cfg(feature = "l4-fault-injection")]
            if crate::l4_fake_exchange::configured_db()
                .map_err(|error| format!("fake exchange filter config failed: {error}"))?
                .is_some()
            {
                return l4_fake_symbol_filters();
            }
            let body = self.fetch_public(venue, EXCHANGE_INFO_PATH, symbol).await?;
            parse_exchange_info(&body, symbol)
        })
    }

    fn fetch_mark_price<'a>(&'a self, venue: Venue, symbol: &'a str) -> FetchFuture<'a, String> {
        Box::pin(async move {
            #[cfg(feature = "l4-fault-injection")]
            if crate::l4_fake_exchange::configured_db()
                .map_err(|error| format!("fake exchange mark-price config failed: {error}"))?
                .is_some()
            {
                return Ok("100".to_string());
            }
            let body = self.fetch_public(venue, MARK_PRICE_PATH, symbol).await?;
            let response: MarkPriceResponse = serde_json::from_str(&body)
                .map_err(|error| format!("decode mark price failed: {error}"))?;
            ExactDecimal::parse_positive(&response.mark_price, "markPrice")?;
            Ok(response.mark_price)
        })
    }
}

#[cfg(feature = "l4-fault-injection")]
fn l4_fake_symbol_filters() -> Result<SymbolFilters, String> {
    parse_exchange_info(
        r#"{
          "symbols": [{
            "symbol": "BTCUSDT",
            "filters": [
              {"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},
              {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},
              {"filterType":"MIN_NOTIONAL","notional":"1"},
              {"filterType":"PERCENT_PRICE","multiplierDown":"0.0500","multiplierUp":"20.0000"}
            ]
          }]
        }"#,
        "BTCUSDT",
    )
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    venue: Venue,
    symbol: String,
}

#[derive(Clone)]
struct CacheEntry {
    filters: SymbolFilters,
    fetched_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Clone)]
struct MarkPriceEntry {
    mark_price: String,
    as_of_ms: i64,
    expires_at_ms: i64,
}

#[derive(Clone)]
pub(crate) struct ExchangeFilterCache {
    source: Arc<dyn ExchangeFilterSource>,
    entries: Arc<Mutex<HashMap<CacheKey, CacheEntry>>>,
    filter_load_gates: Arc<Mutex<HashMap<CacheKey, Arc<tokio::sync::Mutex<()>>>>>,
    mark_prices: Arc<Mutex<HashMap<CacheKey, MarkPriceEntry>>>,
    ttl: Duration,
    mark_price_ttl: Duration,
}

impl ExchangeFilterCache {
    fn production() -> Self {
        Self::with_source_and_mark_ttl(
            Arc::new(BinanceExchangeFilterSource::new()),
            DEFAULT_FILTER_TTL,
            mark_price_cache_ttl_from_env(),
        )
    }

    pub(crate) fn shared() -> Self {
        static CACHE: OnceLock<ExchangeFilterCache> = OnceLock::new();
        CACHE.get_or_init(ExchangeFilterCache::production).clone()
    }

    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::production()
    }

    #[cfg(test)]
    pub(crate) fn with_source(source: Arc<dyn ExchangeFilterSource>, ttl: Duration) -> Self {
        Self::with_source_and_mark_ttl(source, ttl, Duration::from_millis(5_000))
    }

    fn with_source_and_mark_ttl(
        source: Arc<dyn ExchangeFilterSource>,
        ttl: Duration,
        mark_price_ttl: Duration,
    ) -> Self {
        Self {
            source,
            entries: Arc::new(Mutex::new(HashMap::new())),
            filter_load_gates: Arc::new(Mutex::new(HashMap::new())),
            mark_prices: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            mark_price_ttl,
        }
    }

    pub(crate) async fn constraints(
        &self,
        venue: Venue,
        symbol: &str,
        now_ms: i64,
    ) -> Result<ExchangeConstraints, String> {
        let (filters, stale_filters) = self.filters(venue, symbol, now_ms, false).await?;
        let symbol = symbol.trim().to_ascii_uppercase();
        let mark = self.mark_price(venue, &symbol, now_ms).await?;
        Ok(ExchangeConstraints {
            filters,
            mark_price: mark.mark_price,
            mark_price_as_of_ms: Some(mark.as_of_ms),
            stale_filters,
        })
    }

    pub(crate) async fn close_constraints(
        &self,
        venue: Venue,
        symbol: &str,
        now_ms: i64,
    ) -> Result<ExchangeConstraints, String> {
        let (filters, stale_filters) = self.filters(venue, symbol, now_ms, true).await?;
        Ok(ExchangeConstraints {
            filters,
            // Close-only normalization never evaluates notional or percent
            // bounds, so it deliberately needs no live mark-price request.
            mark_price: "1".to_string(),
            mark_price_as_of_ms: None,
            stale_filters,
        })
    }

    async fn filters(
        &self,
        venue: Venue,
        symbol: &str,
        now_ms: i64,
        allow_stale: bool,
    ) -> Result<(SymbolFilters, bool), String> {
        let symbol = symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            return Err("exchange filter symbol is empty".to_string());
        }
        let key = CacheKey {
            venue,
            symbol: symbol.clone(),
        };
        let cached = self
            .entries
            .lock()
            .map_err(|_| "exchange filter cache lock poisoned".to_string())?
            .get(&key)
            .cloned();
        if let Some(entry) = cached.as_ref().filter(|entry| now_ms < entry.expires_at_ms) {
            return Ok((entry.filters.clone(), false));
        }
        let load_gate = {
            let mut gates = self
                .filter_load_gates
                .lock()
                .map_err(|_| "exchange filter load-gate registry lock poisoned".to_string())?;
            gates
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _load_guard = load_gate.lock().await;
        // Another cold miss may have populated this exact venue+symbol while
        // we waited. Rechecking under the per-key gate is the load single-flight.
        if let Some(entry) = self
            .entries
            .lock()
            .map_err(|_| "exchange filter cache lock poisoned".to_string())?
            .get(&key)
            .filter(|entry| now_ms < entry.expires_at_ms)
            .cloned()
        {
            return Ok((entry.filters, false));
        }
        let filters = match self.source.fetch_filters(venue, &symbol).await {
            Ok(filters) => filters,
            Err(error) if allow_stale => {
                if let Some(entry) = cached.filter(|entry| {
                    now_ms.saturating_sub(entry.fetched_at_ms) <= stale_filter_max_ms_from_env()
                }) {
                    return Ok((entry.filters, true));
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let ttl_ms = i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX);
        self.entries
            .lock()
            .map_err(|_| "exchange filter cache lock poisoned".to_string())?
            .insert(
                key,
                CacheEntry {
                    filters: filters.clone(),
                    fetched_at_ms: now_ms,
                    expires_at_ms: now_ms.saturating_add(ttl_ms),
                },
            );
        Ok((filters, false))
    }

    async fn mark_price(
        &self,
        venue: Venue,
        symbol: &str,
        now_ms: i64,
    ) -> Result<MarkPriceEntry, String> {
        let key = CacheKey {
            venue,
            symbol: symbol.to_string(),
        };
        let cached = self
            .mark_prices
            .lock()
            .map_err(|_| "mark-price cache lock poisoned".to_string())?
            .get(&key)
            .filter(|entry| now_ms >= entry.as_of_ms && now_ms < entry.expires_at_ms)
            .cloned();
        if let Some(entry) = cached {
            return Ok(entry);
        }
        let mark_price = self.source.fetch_mark_price(venue, symbol).await?;
        let ttl_ms = i64::try_from(self.mark_price_ttl.as_millis()).unwrap_or(i64::MAX);
        let entry = MarkPriceEntry {
            mark_price,
            as_of_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl_ms),
        };
        self.mark_prices
            .lock()
            .map_err(|_| "mark-price cache lock poisoned".to_string())?
            .insert(key, entry.clone());
        Ok(entry)
    }
}

fn stale_filter_max_ms_from_env() -> i64 {
    std::env::var("RUSTCORE_FILTER_STALE_MAX_MS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(86_400_000)
        .max(300_000)
}

fn mark_price_cache_ttl_from_env() -> Duration {
    let millis = std::env::var("RUSTCORE_MARK_PRICE_CACHE_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(5_000)
        .clamp(1_000, 60_000);
    Duration::from_millis(millis)
}

/// **无网络的过滤器来源**（测试专用）。
///
/// `ExecuteIntents` 的交易所分支在提交之前必须先取 symbol filters 与 mark
/// price，生产实现走公共 REST。驱动生产入口的接线用例不得联真实交易所
/// （engine safety contract），因此把这一层换成一个**固定真值**的来源：
/// 规范化、过滤、风控 replay 全部照常执行，只有 HTTP 那一步是夹具。
#[cfg(test)]
pub(crate) struct StaticExchangeFilterSource {
    pub filters: SymbolFilters,
    pub mark_price: String,
}

#[cfg(test)]
impl ExchangeFilterSource for StaticExchangeFilterSource {
    fn fetch_filters<'a>(
        &'a self,
        _venue: Venue,
        _symbol: &'a str,
    ) -> FetchFuture<'a, SymbolFilters> {
        let filters = self.filters.clone();
        Box::pin(async move { Ok(filters) })
    }

    fn fetch_mark_price<'a>(&'a self, _venue: Venue, _symbol: &'a str) -> FetchFuture<'a, String> {
        let mark_price = self.mark_price.clone();
        Box::pin(async move { Ok(mark_price) })
    }
}

/// BTCUSDT 的一组真实形状的 filters（数值取自 Binance exchangeInfo 的字段形态）。
#[cfg(test)]
pub(crate) fn btcusdt_test_filters() -> SymbolFilters {
    parse_exchange_info(
        r#"{
          "symbols": [{
            "symbol": "BTCUSDT",
            "filters": [
              {"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},
              {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},
              {"filterType":"MIN_NOTIONAL","notional":"5"},
              {"filterType":"PERCENT_PRICE","multiplierDown":"0.9500","multiplierUp":"1.0500"}
            ]
          }]
        }"#,
        "BTCUSDT",
    )
    .expect("valid BTCUSDT filter fixture")
}

/// 一个只认这组固定 filters 的 cache（TTL 足够长，用例期间不再取值）。
#[cfg(test)]
pub(crate) fn static_filter_cache(mark_price: &str) -> ExchangeFilterCache {
    ExchangeFilterCache::with_source(
        Arc::new(StaticExchangeFilterSource {
            filters: btcusdt_test_filters(),
            mark_price: mark_price.to_string(),
        }),
        Duration::from_secs(3600),
    )
}

fn public_endpoint_url(venue: Venue, path: &str) -> String {
    format!("{}{path}", venue.rest_base_url())
}

pub(crate) fn venue_for_filter_fetch(credential_id: &str) -> Result<Venue, String> {
    #[cfg(feature = "l4-fault-injection")]
    if crate::l4_fake_exchange::configured_db()
        .map_err(|error| format!("fake exchange filter config failed: {error}"))?
        .is_some()
    {
        // The filesystem fake has no external venue. A typed value is still
        // required by the source interface so tests exercise the same cache.
        return Ok(Venue::Testnet);
    }
    crate::venue::venue_for_credential(credential_id)?
        .ok_or_else(|| "credential venue is not configured".to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub(crate) fn for_action(action: &str) -> Self {
        if action == "open_short" || action.starts_with("close_long") {
            Self::Sell
        } else {
            Self::Buy
        }
    }

    pub(crate) fn as_binance(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoundDirection {
    Down,
    Up,
}

fn quantity_round_direction(role: OrderRole, reduce_only: bool) -> RoundDirection {
    if role == OrderRole::Entry {
        RoundDirection::Down
    } else if reduce_only {
        // Ceil is safe only after the submit-time effective reduce-only flag is
        // verified: it avoids an untradeable sub-step tail without reversing
        // the position. Role alone is not proof; v2 derives it from the
        // caller-supplied wire action, and compatibility paths can classify a
        // non-entry without making the submitted order reduce-only.
        RoundDirection::Up
    } else {
        // A non-entry role without effective reduce-only can still add exposure.
        RoundDirection::Down
    }
}

fn price_round_direction(side: OrderSide) -> RoundDirection {
    match side {
        OrderSide::Buy => RoundDirection::Down,
        OrderSide::Sell => RoundDirection::Up,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FilterAuditOutcome {
    Pass,
    Capped,
    Skipped,
    Reject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedOrder {
    pub quantity: String,
    pub price: String,
    pub outcome: FilterAuditOutcome,
    pub reason_code: Option<&'static str>,
    pub reason: Option<&'static str>,
    pub detail: String,
}

impl PreparedOrder {
    fn reject(code: &'static str, detail: String, quantity: String, price: String) -> Self {
        Self {
            quantity,
            price,
            outcome: FilterAuditOutcome::Reject,
            reason_code: Some(code),
            reason: None,
            detail,
        }
    }
}

pub(crate) fn apply_exchange_filters(
    role: OrderRole,
    reduce_only: bool,
    side: OrderSide,
    quantity: &str,
    price: &str,
    constraints: Result<&ExchangeConstraints, &str>,
) -> PreparedOrder {
    let filters = match constraints {
        Ok(filters) => filters,
        Err(detail) if role != OrderRole::Entry => {
            return PreparedOrder {
                quantity: quantity.to_string(),
                price: price.to_string(),
                outcome: FilterAuditOutcome::Skipped,
                reason_code: None,
                reason: Some("not_applicable_to_close_or_hold"),
                detail: format!("exchange filters unavailable: {detail}"),
            };
        }
        Err(detail) => {
            return PreparedOrder::reject(
                "EXCHANGE_FILTERS_UNAVAILABLE",
                format!("exchange filters unavailable: {detail}"),
                quantity.to_string(),
                price.to_string(),
            );
        }
    };

    let parsed_quantity = match ExactDecimal::parse_nonnegative(quantity, "quantity") {
        Ok(value) => value,
        Err(_error) if role != OrderRole::Entry && quantity.trim().is_empty() => {
            let rounded_price = match round_optional_price(price, filters, side) {
                Ok(value) => value,
                Err(error) => {
                    return PreparedOrder {
                        quantity: quantity.to_string(),
                        price: price.to_string(),
                        outcome: FilterAuditOutcome::Skipped,
                        reason_code: None,
                        reason: Some("not_applicable_to_close_or_hold"),
                        detail: format!("closePosition price normalization skipped: {error}"),
                    };
                }
            };
            let changed =
                !price.trim().is_empty() && rounded_price != normalized_decimal_text(price);
            return PreparedOrder {
                quantity: quantity.to_string(),
                price: rounded_price,
                outcome: if changed {
                    FilterAuditOutcome::Capped
                } else {
                    FilterAuditOutcome::Pass
                },
                reason_code: None,
                reason: None,
                detail: "closePosition order has no explicit quantity".to_string(),
            };
        }
        Err(error) if role != OrderRole::Entry => {
            return PreparedOrder {
                quantity: quantity.to_string(),
                price: price.to_string(),
                outcome: FilterAuditOutcome::Skipped,
                reason_code: None,
                reason: Some("not_applicable_to_close_or_hold"),
                detail: format!("close normalization skipped: {error}"),
            };
        }
        Err(error) => {
            return PreparedOrder::reject(
                "INVALID_EXCHANGE_FILTER_INPUT",
                error,
                quantity.to_string(),
                price.to_string(),
            );
        }
    };
    let rounded_quantity = parsed_quantity.round_to_increment(
        filters.filters.step_size,
        quantity_round_direction(role, reduce_only),
    );
    let rounded_price = match round_optional_price(price, filters, side) {
        Ok(value) => value,
        Err(error) if role != OrderRole::Entry => {
            return PreparedOrder {
                quantity: quantity.to_string(),
                price: price.to_string(),
                outcome: FilterAuditOutcome::Skipped,
                reason_code: None,
                reason: Some("not_applicable_to_close_or_hold"),
                detail: format!("close price normalization skipped: {error}"),
            };
        }
        Err(error) => {
            return PreparedOrder::reject(
                "INVALID_EXCHANGE_FILTER_INPUT",
                error,
                rounded_quantity.to_string(),
                price.to_string(),
            );
        }
    };
    let changed = rounded_quantity.to_string() != normalized_decimal_text(quantity)
        || (!price.trim().is_empty() && rounded_price != normalized_decimal_text(price));

    // All exchange bounds below are open-only. Close, reduce-only,
    // protection and emergency intents keep the close-always-open invariant.
    if role != OrderRole::Entry {
        return PreparedOrder {
            quantity: rounded_quantity.to_string(),
            price: rounded_price.clone(),
            outcome: if changed {
                FilterAuditOutcome::Capped
            } else {
                FilterAuditOutcome::Pass
            },
            reason_code: None,
            reason: None,
            detail: format!(
                "close-safe normalization quantity={quantity}->{} price={price}->{}",
                rounded_quantity, rounded_price
            ),
        };
    }

    if rounded_quantity.is_zero()
        || rounded_quantity < filters.filters.min_qty
        || (!filters.filters.max_qty.is_zero() && rounded_quantity > filters.filters.max_qty)
    {
        return PreparedOrder::reject(
            "LOT_SIZE_VIOLATION",
            format!(
                "rounded_quantity={} min_qty={} max_qty={}",
                rounded_quantity, filters.filters.min_qty, filters.filters.max_qty
            ),
            rounded_quantity.to_string(),
            rounded_price,
        );
    }

    let parsed_price = if rounded_price.is_empty() {
        None
    } else {
        match ExactDecimal::parse_positive(&rounded_price, "price") {
            Ok(value) => Some(value),
            Err(error) => {
                return PreparedOrder::reject(
                    "INVALID_EXCHANGE_FILTER_INPUT",
                    error,
                    rounded_quantity.to_string(),
                    rounded_price,
                );
            }
        }
    };
    if let Some(order_price) = parsed_price {
        if (!filters.filters.min_price.is_zero() && order_price < filters.filters.min_price)
            || (!filters.filters.max_price.is_zero() && order_price > filters.filters.max_price)
        {
            return PreparedOrder::reject(
                "PRICE_FILTER_VIOLATION",
                format!(
                    "rounded_price={} min_price={} max_price={}",
                    order_price, filters.filters.min_price, filters.filters.max_price
                ),
                rounded_quantity.to_string(),
                rounded_price,
            );
        }
    }

    let mark_price = match ExactDecimal::parse_positive(&filters.mark_price, "markPrice") {
        Ok(value) => value,
        Err(error) => {
            return PreparedOrder::reject(
                "EXCHANGE_FILTERS_UNAVAILABLE",
                error,
                rounded_quantity.to_string(),
                rounded_price,
            );
        }
    };
    let notional_price = parsed_price.unwrap_or(mark_price);
    let notional = match rounded_quantity.multiply(notional_price) {
        Ok(value) => value,
        Err(error) => {
            return PreparedOrder::reject(
                "INVALID_EXCHANGE_FILTER_INPUT",
                error,
                rounded_quantity.to_string(),
                rounded_price,
            );
        }
    };
    if notional < filters.filters.min_notional {
        return PreparedOrder::reject(
            "SIZE_BELOW_EXCHANGE_MIN",
            format!(
                "rounded_quantity={} notional={} minimum={}",
                rounded_quantity, notional, filters.filters.min_notional
            ),
            rounded_quantity.to_string(),
            rounded_price,
        );
    }

    if let Some(order_price) = parsed_price {
        let boundary = match side {
            OrderSide::Buy => mark_price.multiply(filters.filters.multiplier_up),
            OrderSide::Sell => mark_price.multiply(filters.filters.multiplier_down),
        };
        let boundary = match boundary {
            Ok(value) => value,
            Err(error) => {
                return PreparedOrder::reject(
                    "INVALID_EXCHANGE_FILTER_INPUT",
                    error,
                    rounded_quantity.to_string(),
                    rounded_price,
                );
            }
        };
        let outside = match side {
            OrderSide::Buy => order_price > boundary,
            OrderSide::Sell => order_price < boundary,
        };
        if outside {
            return PreparedOrder::reject(
                "PERCENT_PRICE_VIOLATION",
                format!(
                    "rounded_price={} mark_price={} multiplier_down={} multiplier_up={}",
                    order_price,
                    mark_price,
                    filters.filters.multiplier_down,
                    filters.filters.multiplier_up
                ),
                rounded_quantity.to_string(),
                rounded_price,
            );
        }
    }

    PreparedOrder {
        quantity: rounded_quantity.to_string(),
        price: rounded_price.clone(),
        outcome: if changed {
            FilterAuditOutcome::Capped
        } else {
            FilterAuditOutcome::Pass
        },
        reason_code: None,
        reason: None,
        detail: format!(
            "quantity={quantity}->{} price={price}->{rounded_price} mark_price={} notional={notional}",
            rounded_quantity, mark_price
        ),
    }
}

fn round_optional_price(
    price: &str,
    constraints: &ExchangeConstraints,
    side: OrderSide,
) -> Result<String, String> {
    if price.trim().is_empty() {
        return Ok(String::new());
    }
    Ok(ExactDecimal::parse_nonnegative(price, "price")?
        .round_to_increment(constraints.filters.tick_size, price_round_direction(side))
        .to_string())
}

fn normalized_decimal_text(value: &str) -> String {
    ExactDecimal::parse_nonnegative(value, "decimal")
        .map(|value| value.to_string())
        .unwrap_or_else(|_| value.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExactDecimal {
    units: i128,
    scale: u32,
}

impl ExactDecimal {
    fn parse_positive(value: &str, field: &str) -> Result<Self, String> {
        let parsed = Self::parse_nonnegative(value, field)?;
        if parsed.is_zero() {
            return Err(format!("{field} must be positive"));
        }
        Ok(parsed)
    }

    fn parse_nonnegative(value: &str, field: &str) -> Result<Self, String> {
        let value = value.trim();
        if value.is_empty() || value.starts_with('+') || value.starts_with('-') {
            return Err(format!("{field} is not a non-negative decimal"));
        }
        let mut parts = value.split('.');
        let whole = parts.next().unwrap_or_default();
        let fraction = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || whole.is_empty()
            || whole.len() > 18
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            || fraction.len() > 18
        {
            return Err(format!("{field} is not a supported decimal"));
        }
        let scale = fraction.len() as u32;
        let factor = checked_pow10(scale).ok_or_else(|| format!("{field} scale is too large"))?;
        let whole = whole
            .parse::<i128>()
            .map_err(|_| format!("{field} is too large"))?;
        let fraction = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse::<i128>()
                .map_err(|_| format!("{field} is too large"))?
        };
        let units = whole
            .checked_mul(factor)
            .and_then(|value| value.checked_add(fraction))
            .ok_or_else(|| format!("{field} is too large"))?;
        Ok(Self { units, scale }.normalized())
    }

    fn normalized(mut self) -> Self {
        while self.scale > 0 && self.units % 10 == 0 {
            self.units /= 10;
            self.scale -= 1;
        }
        self
    }

    fn is_zero(self) -> bool {
        self.units == 0
    }

    fn round_to_increment(self, increment: Self, direction: RoundDirection) -> Self {
        let scale = self.scale.max(increment.scale);
        let value = self.rescale_units(scale);
        let step = increment.rescale_units(scale);
        debug_assert!(step > 0);
        let quotient = value / step;
        let remainder = value % step;
        let units = match direction {
            RoundDirection::Down => quotient * step,
            RoundDirection::Up if remainder == 0 => value,
            RoundDirection::Up => (quotient + 1) * step,
        };
        Self { units, scale }.normalized()
    }

    fn multiply(self, other: Self) -> Result<Self, String> {
        Ok(Self {
            units: self
                .units
                .checked_mul(other.units)
                .ok_or_else(|| "exchange decimal multiplication overflow".to_string())?,
            scale: self.scale + other.scale,
        }
        .normalized())
    }

    fn rescale_units(self, scale: u32) -> i128 {
        self.units
            .checked_mul(checked_pow10(scale - self.scale).expect("validated decimal scale"))
            .expect("validated Binance decimal rescale overflow")
    }
}

/// Validate a quantity that came from an engine-owned position row before it
/// is allowed to reach a close-family exchange submission. Close filters are
/// deliberately fail-open when venue metadata is unavailable, so this source
/// validation cannot be delegated to `apply_exchange_filters`.
pub(crate) fn validate_authoritative_position_quantity(quantity: &str) -> Result<(), String> {
    ExactDecimal::parse_positive(quantity, "authoritative position quantity").map(|_| ())
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let scale = self.scale.max(other.scale);
        self.rescale_units(scale).cmp(&other.rescale_units(scale))
    }
}

impl std::fmt::Display for ExactDecimal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.scale == 0 {
            return write!(formatter, "{}", self.units);
        }
        let factor = checked_pow10(self.scale).expect("validated decimal scale");
        let whole = self.units / factor;
        let fraction = self.units % factor;
        write!(
            formatter,
            "{whole}.{:0width$}",
            fraction,
            width = self.scale as usize
        )
    }
}

fn checked_pow10(scale: u32) -> Option<i128> {
    10i128.checked_pow(scale)
}

#[derive(Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<ExchangeInfoSymbol>,
}

#[derive(Deserialize)]
struct ExchangeInfoSymbol {
    symbol: String,
    filters: Vec<RawFilter>,
}

#[derive(Deserialize)]
struct RawFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "minQty")]
    min_qty: Option<String>,
    #[serde(rename = "maxQty")]
    max_qty: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "minPrice")]
    min_price: Option<String>,
    #[serde(rename = "maxPrice")]
    max_price: Option<String>,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    notional: Option<String>,
    #[serde(rename = "multiplierDown")]
    multiplier_down: Option<String>,
    #[serde(rename = "multiplierUp")]
    multiplier_up: Option<String>,
}

#[derive(Deserialize)]
struct MarkPriceResponse {
    #[serde(rename = "markPrice")]
    mark_price: String,
}

pub(crate) fn parse_request_weight_limit_1m(body: &str) -> Option<u64> {
    let response: serde_json::Value = serde_json::from_str(body).ok()?;
    response
        .get("rateLimits")?
        .as_array()?
        .iter()
        .find_map(|entry| {
            let matches_request_weight = entry
                .get("rateLimitType")
                .and_then(serde_json::Value::as_str)
                == Some("REQUEST_WEIGHT");
            let matches_minute =
                entry.get("interval").and_then(serde_json::Value::as_str) == Some("MINUTE");
            let matches_one =
                entry.get("intervalNum").and_then(serde_json::Value::as_u64) == Some(1);
            if !matches_request_weight || !matches_minute || !matches_one {
                return None;
            }
            entry
                .get("limit")
                .and_then(serde_json::Value::as_u64)
                .filter(|limit| *limit > 0)
        })
}

fn parse_exchange_info(body: &str, symbol: &str) -> Result<SymbolFilters, String> {
    let response: ExchangeInfoResponse = serde_json::from_str(body)
        .map_err(|error| format!("decode exchangeInfo failed: {error}"))?;
    let symbol = response
        .symbols
        .into_iter()
        .find(|candidate| candidate.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| format!("exchangeInfo missing symbol {symbol}"))?;
    let get = |filter_type: &str, field: fn(&RawFilter) -> Option<&String>| {
        symbol
            .filters
            .iter()
            .find(|filter| filter.filter_type == filter_type)
            .and_then(field)
            .cloned()
            .ok_or_else(|| format!("{filter_type} missing required field"))
    };
    Ok(SymbolFilters {
        min_qty: ExactDecimal::parse_nonnegative(
            &get("LOT_SIZE", |filter| filter.min_qty.as_ref())?,
            "LOT_SIZE.minQty",
        )?,
        max_qty: ExactDecimal::parse_nonnegative(
            &get("LOT_SIZE", |filter| filter.max_qty.as_ref())?,
            "LOT_SIZE.maxQty",
        )?,
        step_size: ExactDecimal::parse_positive(
            &get("LOT_SIZE", |filter| filter.step_size.as_ref())?,
            "LOT_SIZE.stepSize",
        )?,
        min_price: ExactDecimal::parse_nonnegative(
            &get("PRICE_FILTER", |filter| filter.min_price.as_ref())?,
            "PRICE_FILTER.minPrice",
        )?,
        max_price: ExactDecimal::parse_nonnegative(
            &get("PRICE_FILTER", |filter| filter.max_price.as_ref())?,
            "PRICE_FILTER.maxPrice",
        )?,
        tick_size: ExactDecimal::parse_positive(
            &get("PRICE_FILTER", |filter| filter.tick_size.as_ref())?,
            "PRICE_FILTER.tickSize",
        )?,
        min_notional: ExactDecimal::parse_nonnegative(
            &get("MIN_NOTIONAL", |filter| filter.notional.as_ref())?,
            "MIN_NOTIONAL.notional",
        )?,
        multiplier_down: ExactDecimal::parse_positive(
            &get("PERCENT_PRICE", |filter| filter.multiplier_down.as_ref())?,
            "PERCENT_PRICE.multiplierDown",
        )?,
        multiplier_up: ExactDecimal::parse_positive(
            &get("PERCENT_PRICE", |filter| filter.multiplier_up.as_ref())?,
            "PERCENT_PRICE.multiplierUp",
        )?,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;

    async fn cooldown_test_store(label: &str) -> (crate::store::Store, std::path::PathBuf) {
        static INSTALL_DRIVERS: std::sync::Once = std::sync::Once::new();
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-a3-{label}-{}.db", ulid::Ulid::new()));
        let store = crate::store::Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect cooldown test store");
        (store, path)
    }

    async fn cleanup_cooldown_test_store(store: crate::store::Store, path: std::path::PathBuf) {
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    struct FilterClock {
        now_ms: AtomicI64,
        sleeps_ms: Mutex<Vec<u64>>,
    }

    impl Clock for FilterClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
                self.sleeps_ms.lock().unwrap().push(millis);
                self.now_ms.fetch_add(millis as i64, Ordering::SeqCst);
            })
        }
    }

    fn filters() -> SymbolFilters {
        parse_exchange_info(
            r#"{
              "symbols": [{
                "symbol": "BTCUSDT",
                "filters": [
                  {"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},
                  {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},
                  {"filterType":"MIN_NOTIONAL","notional":"50"},
                  {"filterType":"PERCENT_PRICE","multiplierDown":"0.9500","multiplierUp":"1.0500"}
                ]
              }]
            }"#,
            "BTCUSDT",
        )
        .expect("valid fixture")
    }

    fn constraints(mark_price: &str) -> ExchangeConstraints {
        ExchangeConstraints {
            filters: filters(),
            mark_price: mark_price.to_string(),
            mark_price_as_of_ms: Some(1_000),
            stale_filters: false,
        }
    }

    fn exchange_info_with_rate_limits(rate_limits: &str) -> String {
        format!(
            r#"{{
              "rateLimits": {rate_limits},
              "symbols": [{{
                "symbol": "BTCUSDT",
                "filters": [
                  {{"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"}},
                  {{"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"}},
                  {{"filterType":"MIN_NOTIONAL","notional":"50"}},
                  {{"filterType":"PERCENT_PRICE","multiplierDown":"0.9500","multiplierUp":"1.0500"}}
                ]
              }}]
            }}"#
        )
    }

    #[test]
    fn a6_r2_i1_parser_accepts_only_request_weight_minute_one() {
        let body = r#"{
          "rateLimits": [
            {"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":100},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"SECOND","intervalNum":1,"limit":200},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":5,"limit":300},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":"bad"},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000}
          ]
        }"#;
        assert_eq!(parse_request_weight_limit_1m(body), Some(6_000));
        for malformed in [
            "not-json",
            r#"{}"#,
            r#"{"rateLimits":{}}"#,
            r#"{"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1}]}"#,
            r#"{"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":0}]}"#,
        ] {
            assert_eq!(parse_request_weight_limit_1m(malformed), None);
        }
    }

    #[tokio::test]
    async fn a6_r2_existing_exchange_info_read_publishes_observed_budget_by_scope() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("a6-r2-observed-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let _global_budget = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M".to_string(),
        );
        let _scoped_budget = crate::binance_futures::tests::TestEnvVar::unset(format!(
            "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M__{scope}"
        ));
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(1_000),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![PublicHttpResponse {
                    status: 200,
                    retry_after: None,
                    body: exchange_info_with_rate_limits(
                        r#"[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000}]"#,
                    ),
                }]
                .into(),
            ))),
        };

        tokio::time::timeout(
            Duration::from_secs(30),
            source.fetch_filters(Venue::Testnet, "BTCUSDT"),
        )
        .await
        .expect("the existing public read must not hang")
        .expect("valid exchangeInfo");
        let observer =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(Some(Venue::Testnet)));
        assert_eq!(
            observer.effective_budget_for_test(&scope),
            crate::rate_limiter::EffectiveEgressBudget {
                budget_1m: 6_000,
                source: crate::rate_limiter::BudgetSource::Observed,
            }
        );

        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[tokio::test]
    async fn a6_r2_malformed_rate_limits_do_not_override_or_break_filters() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("a6-r2-malformed-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let _global_budget = crate::binance_futures::tests::TestEnvVar::unset(
            "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M".to_string(),
        );
        let _scoped_budget = crate::binance_futures::tests::TestEnvVar::unset(format!(
            "RUSTCORE_BINANCE_WEIGHT_BUDGET_1M__{scope}"
        ));
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(1_000),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![PublicHttpResponse {
                    status: 200,
                    retry_after: None,
                    body: exchange_info_with_rate_limits(
                        r#"[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":"bad"}]"#,
                    ),
                }]
                .into(),
            ))),
        };

        let actual = tokio::time::timeout(
            Duration::from_secs(30),
            source.fetch_filters(Venue::Testnet, "BTCUSDT"),
        )
        .await
        .expect("the existing public read must not hang")
        .expect("malformed rateLimits must not break valid filters");
        assert_eq!(actual, filters());
        let observer =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(Some(Venue::Testnet)));
        assert_eq!(
            observer.effective_budget_for_test(&scope),
            crate::rate_limiter::EffectiveEgressBudget {
                budget_1m: 2_400,
                source: crate::rate_limiter::BudgetSource::Default,
            }
        );

        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[test]
    fn a6_r2_i3_existing_exchange_request_call_site_counts_do_not_increase() {
        let exchange_filters = include_str!("exchange_filters.rs")
            .rsplit_once("#[cfg(test)]\nmod tests")
            .map(|(production, _)| production)
            .expect("exchange_filters production source");
        assert_eq!(
            exchange_filters.matches(".get(public_endpoint_url").count(),
            1
        );

        let binance_futures = include_str!("binance_futures.rs")
            .split_once("#[cfg(test)]\npub(crate) mod tests")
            .map(|(production, _)| production)
            .expect("binance_futures production source");
        assert_eq!(binance_futures.matches("signed_get_json(").count(), 9);
    }

    /// **接线证明**：驱动真实的 `fetch_public` 分支，而不是直接调发布 helper。
    ///
    /// 上一版只测了 helper 本身，于是把 `exchange_filters.rs` 里两个真实调用点
    /// 整个删掉，全量套件仍然全绿（MUT-WIRE-01 存活）。纯函数用例替代不了接线证明。
    #[tokio::test]
    async fn a_public_429_parks_the_shared_egress_from_the_first_response() {
        let clock = Arc::new(FilterClock {
            now_ms: AtomicI64::new(1_000),
            sleeps_ms: Mutex::new(Vec::new()),
        });
        // 独占一个 route id：发布器会写进程内共享 registry，落在公共的 venue:testnet
        // 上就是留给别的用例的一颗雷。与发布路径用同一个派生函数取 key。
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("pub429-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        assert_eq!(
            crate::binance_futures::rate_limit_cooldown_until_for_tests(&scope),
            0,
            "fresh route must start clean"
        );

        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: clock.clone(),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![PublicHttpResponse {
                    status: 429,
                    retry_after: Some("2".to_string()),
                    body: String::new(),
                }]
                .into(),
            ))),
        };

        // 队列里只有一条 429：重试会因为响应耗尽而失败。关键不是它最终失败，
        // 而是**第一条 429 当场**就把共享出口停车了。
        let _ = source.fetch_filters(Venue::Testnet, "BTCUSDT").await;

        // 独占 scope 之下可以断言精确值：now(1_000) + Retry-After(2 s)。
        assert_eq!(
            crate::binance_futures::rate_limit_cooldown_until_for_tests(&scope),
            1_000 + 2_000,
            "第一条 429 必须当场把共享 429 冷却写到 now+Retry-After"
        );
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[tokio::test]
    async fn a_public_418_publishes_to_memory_and_the_lifecycle_store() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("pub418-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        // Any test Store::connect initializes the process-lifetime store used
        // by the real publication entry. The source itself deliberately has no
        // injected store, so this assertion follows the production lifecycle.
        let (bootstrap, bootstrap_path) = cooldown_test_store("public-418-lifecycle").await;
        let lifecycle =
            crate::binance_futures::registered_cooldown_store().expect("lifecycle cooldown store");
        let now_ms = 1_788_000_000_000_i64;
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(now_ms),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![PublicHttpResponse {
                    status: 418,
                    retry_after: None,
                    body: String::new(),
                }]
                .into(),
            ))),
        };

        let error = source
            .fetch_filters(Venue::Testnet, "BTCUSDT")
            .await
            .expect_err("a public 418 is never retried");
        assert!(error.contains("http 418 IP ban"));
        let shared = crate::binance_futures::shared_egress_cooldown(&scope, None, now_ms)
            .await
            .expect("read in-memory shared cooldown");
        assert_eq!(
            shared.ban_until_ms,
            now_ms + crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS as i64
        );
        let row = sqlx::query(
            "SELECT CAST(ban_until_ms AS CHAR) AS until_text \
             FROM binance_ip_ban_cooldowns WHERE egress_scope = ?",
        )
        .bind(&scope)
        .fetch_one(lifecycle.pool())
        .await
        .expect("the lifecycle store must contain the public 418 row");
        assert_eq!(
            crate::store::try_get_i64_text(&row, "until_text").expect("decode ban cutoff"),
            now_ms + crate::rate_limiter::BINANCE_IP_BAN_COOLDOWN_MS as i64
        );

        sqlx::query("DELETE FROM binance_ip_ban_cooldowns WHERE egress_scope = ?")
            .bind(&scope)
            .execute(lifecycle.pool())
            .await
            .expect("clean lifecycle row");
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
        cleanup_cooldown_test_store(bootstrap, bootstrap_path).await;
    }

    #[tokio::test]
    async fn concurrent_cold_filter_misses_make_one_exchange_info_request() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-single-flight-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let _rate = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(),
            "100",
        );
        let _burst = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST".to_string(),
            "100",
        );
        let body = r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},{"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"50"},{"filterType":"PERCENT_PRICE","multiplierDown":"0.95","multiplierUp":"1.05"}]}]}"#;
        let responses = Arc::new(Mutex::new(
            vec![
                PublicHttpResponse {
                    status: 200,
                    retry_after: None,
                    body: body.to_string(),
                },
                PublicHttpResponse {
                    status: 200,
                    retry_after: None,
                    body: body.to_string(),
                },
            ]
            .into(),
        ));
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(1_000),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(responses.clone()),
        };
        let cache = ExchangeFilterCache::with_source(Arc::new(source), Duration::from_secs(60));
        let key = CacheKey {
            venue: Venue::Testnet,
            symbol: "BTCUSDT".to_string(),
        };
        let load_gate = {
            let mut gates = cache.filter_load_gates.lock().expect("load gates");
            gates
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let held = load_gate.lock().await;
        let first_cache = cache.clone();
        let first = tokio::spawn(async move {
            first_cache
                .close_constraints(Venue::Testnet, "BTCUSDT", 1_000)
                .await
        });
        let second_cache = cache.clone();
        let second = tokio::spawn(async move {
            second_cache
                .close_constraints(Venue::Testnet, "BTCUSDT", 1_000)
                .await
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        drop(held);
        first.await.expect("first join").expect("first filters");
        second.await.expect("second join").expect("second filters");
        assert_eq!(
            responses.lock().expect("responses").len(),
            1,
            "the waiter must re-read the cache instead of sending exchangeInfo again"
        );

        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[tokio::test]
    async fn authoritative_public_recheck_stops_a_send_parked_while_waiting_for_the_gate() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-authoritative-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let _rate = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(),
            "100",
        );
        let _burst = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST".to_string(),
            "100",
        );
        let now_ms = 1_788_000_000_000_i64;
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let responses = Arc::new(Mutex::new(
            vec![PublicHttpResponse {
                status: 200,
                retry_after: None,
                body: "must stay queued".to_string(),
            }]
            .into(),
        ));
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(now_ms),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(responses.clone()),
        };
        let gate = crate::binance_futures::transport_gate_for_scope(&scope);
        let held = gate.lock().await;
        let request =
            tokio::spawn(async move { source.fetch_filters(Venue::Testnet, "BTCUSDT").await });
        let observer =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(Some(Venue::Testnet)));
        for _ in 0..100 {
            if observer
                .stored_tokens(&scope)
                .await
                .is_some_and(|tokens| tokens < 100.0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            observer
                .stored_tokens(&scope)
                .await
                .is_some_and(|tokens| tokens < 100.0),
            "the request must have passed its fast read and bucket before publication"
        );
        crate::binance_futures::publish_public_endpoint_cooldown_with_store(
            None,
            Some(Venue::Testnet),
            429,
            5_000,
            now_ms,
        )
        .await
        .expect("publish concurrent 429");
        drop(held);

        let error = request
            .await
            .expect("request join")
            .expect_err("the authoritative recheck must stop transport");
        assert!(error.contains("rate-limit cooldown active"));
        assert_eq!(responses.lock().expect("responses").len(), 1);
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[tokio::test]
    async fn active_shared_418_stops_public_fetch_before_the_mock_transport() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-read-418-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let (store, path) = cooldown_test_store("shared-418").await;
        let now_ms = 1_788_000_000_000_i64;
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        sqlx::query(
            "INSERT INTO binance_ip_ban_cooldowns \
             (egress_scope, ban_until_ms, updated_at_ms) VALUES (?, ?, ?)",
        )
        .bind(&scope)
        .bind(now_ms + 120_000)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .expect("seed active shared 418");
        let responses = Arc::new(Mutex::new(
            vec![PublicHttpResponse {
                status: 200,
                retry_after: None,
                body: "should stay queued".to_string(),
            }]
            .into(),
        ));
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(now_ms),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: Some(store.clone()),
            mock_responses: Some(responses.clone()),
        };

        let error = source
            .fetch_filters(Venue::Testnet, "BTCUSDT")
            .await
            .expect_err("the shared 418 must stop public transport");
        assert!(error.contains("IP ban cooldown active"), "{error}");
        assert_eq!(responses.lock().expect("responses").len(), 1);
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
        cleanup_cooldown_test_store(store, path).await;
    }

    #[tokio::test]
    async fn active_shared_429_stops_public_fetch_then_expiry_releases_it() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-read-429-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let (store, path) = cooldown_test_store("shared-429").await;
        let now_ms = 1_788_000_000_000_i64;
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        sqlx::query(
            "INSERT INTO binance_rate_limit_cooldowns \
             (egress_scope, cooldown_until_ms, updated_at_ms) VALUES (?, ?, ?)",
        )
        .bind(&scope)
        .bind(now_ms + 2_000)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .expect("seed active shared 429");
        let responses = Arc::new(Mutex::new(
            vec![PublicHttpResponse {
                status: 200,
                retry_after: None,
                body: r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},{"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"50"},{"filterType":"PERCENT_PRICE","multiplierDown":"0.95","multiplierUp":"1.05"}]}]}"#.to_string(),
            }]
            .into(),
        ));
        let clock = Arc::new(FilterClock {
            now_ms: AtomicI64::new(now_ms),
            sleeps_ms: Mutex::new(Vec::new()),
        });
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: clock.clone(),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: Some(store.clone()),
            mock_responses: Some(responses.clone()),
        };

        let error = source
            .fetch_filters(Venue::Testnet, "BTCUSDT")
            .await
            .expect_err("the active shared 429 must stop public transport");
        assert!(error.contains("rate-limit cooldown active"), "{error}");
        assert_eq!(responses.lock().expect("responses").len(), 1);
        clock.now_ms.store(now_ms + 2_000, Ordering::SeqCst);
        assert_eq!(
            source
                .fetch_filters(Venue::Testnet, "BTCUSDT")
                .await
                .expect("expiry releases public transport"),
            filters()
        );
        assert_eq!(responses.lock().expect("responses").len(), 0);
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
        cleanup_cooldown_test_store(store, path).await;
    }

    #[tokio::test]
    async fn public_exchange_info_spends_one_token_from_the_shared_egress_bucket() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-weight-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let _burst = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_BURST".to_string(),
            "100",
        );
        let _rate = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_BINANCE_TESTNET_WEIGHT_PER_SEC".to_string(),
            "100",
        );
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: Arc::new(FilterClock {
                now_ms: AtomicI64::new(1_000),
                sleeps_ms: Mutex::new(Vec::new()),
            }),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![PublicHttpResponse {
                    status: 200,
                    retry_after: None,
                    body: r#"{"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},{"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"50"},{"filterType":"PERCENT_PRICE","multiplierDown":"0.95","multiplierUp":"1.05"}]}]}"#.to_string(),
                }]
                .into(),
            ))),
        };
        source
            .fetch_filters(Venue::Testnet, "BTCUSDT")
            .await
            .expect("public fetch");
        let observer =
            CredentialRateLimiter::shared(TokenBucketConfig::from_env(Some(Venue::Testnet)));
        assert_eq!(observer.stored_tokens(&scope).await, Some(99.0));
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[tokio::test]
    async fn t38_exchange_info_read_retries_429_with_retry_after() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let route = format!("public-retry-{}", ulid::Ulid::new());
        let _route = crate::binance_futures::tests::TestEnvVar::set(
            "BINANCE_EGRESS_ROUTE_ID".to_string(),
            &route,
        );
        let scope = crate::binance_futures::public_endpoint_egress_scope(Some(Venue::Testnet))
            .expect("scope");
        let clock = Arc::new(FilterClock {
            now_ms: AtomicI64::new(1_000),
            sleeps_ms: Mutex::new(Vec::new()),
        });
        let source = BinanceExchangeFilterSource {
            http: reqwest::Client::new(),
            clock: clock.clone(),
            ban_until_ms: Arc::new(Mutex::new(0)),
            cooldown_store: None,
            mock_responses: Some(Arc::new(Mutex::new(
                vec![
                    PublicHttpResponse {
                        status: 429,
                        retry_after: Some("2".to_string()),
                        body: String::new(),
                    },
                    PublicHttpResponse {
                        status: 200,
                        retry_after: None,
                        body: r#"{
                          "symbols": [{
                            "symbol": "BTCUSDT",
                            "filters": [
                              {"filterType":"LOT_SIZE","minQty":"0.0001","maxQty":"1000","stepSize":"0.0001"},
                              {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"},
                              {"filterType":"MIN_NOTIONAL","notional":"50"},
                              {"filterType":"PERCENT_PRICE","multiplierDown":"0.9500","multiplierUp":"1.0500"}
                            ]
                          }]
                        }"#
                        .to_string(),
                    },
                ]
                .into(),
            ))),
        };
        let actual = source
            .fetch_filters(Venue::Testnet, "BTCUSDT")
            .await
            .expect("exchangeInfo 429 read retries");
        assert_eq!(actual, filters());
        assert_eq!(*clock.sleeps_ms.lock().unwrap(), vec![2_000]);
        crate::binance_futures::tests::clear_egress_test_state(&scope);
        crate::rate_limiter::clear_shared_egress_bucket(&scope).await;
    }

    #[test]
    fn f1_entry_quantity_is_floored_and_never_exceeds_risk_sizing() {
        assert_eq!(
            quantity_round_direction(OrderRole::Entry, false),
            RoundDirection::Down
        );
        let result = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Buy,
            "0.007956107745",
            "",
            Ok(&constraints("62844.8")),
        );
        assert_eq!(result.quantity, "0.0079");
        assert!(
            ExactDecimal::parse_positive(&result.quantity, "result").unwrap()
                <= ExactDecimal::parse_positive("0.007956107745", "approved").unwrap()
        );
        assert_eq!(result.outcome, FilterAuditOutcome::Capped);
    }

    #[test]
    fn f2_below_min_notional_rejects_without_raising_quantity() {
        let result = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Buy,
            "0.00079",
            "",
            Ok(&constraints("62844.8")),
        );
        assert_eq!(result.outcome, FilterAuditOutcome::Reject);
        assert_eq!(result.reason_code, Some("SIZE_BELOW_EXCHANGE_MIN"));
        assert_eq!(result.quantity, "0.0007");
    }

    #[test]
    fn f3_percent_price_outside_band_is_rejected_not_clamped() {
        let buy = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Buy,
            "1",
            "105.1",
            Ok(&constraints("100")),
        );
        assert_eq!(buy.outcome, FilterAuditOutcome::Reject);
        assert_eq!(buy.reason_code, Some("PERCENT_PRICE_VIOLATION"));
        assert_eq!(buy.price, "105.1");

        let sell = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Sell,
            "1",
            "94.9",
            Ok(&constraints("100")),
        );
        assert_eq!(sell.outcome, FilterAuditOutcome::Reject);
        assert_eq!(sell.reason_code, Some("PERCENT_PRICE_VIOLATION"));
        assert_eq!(sell.price, "94.9");
    }

    #[test]
    fn f4_unavailable_filters_reject_entry_but_skip_every_close_role() {
        let entry = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Buy,
            "1",
            "100",
            Err("network down"),
        );
        assert_eq!(entry.outcome, FilterAuditOutcome::Reject);
        assert_eq!(entry.reason_code, Some("EXCHANGE_FILTERS_UNAVAILABLE"));

        for role in [
            OrderRole::Exit,
            OrderRole::ProtectionSl,
            OrderRole::ProtectionTp,
            OrderRole::Emergency,
        ] {
            let close = apply_exchange_filters(
                role,
                true,
                OrderSide::Sell,
                "1",
                "100",
                Err("network down"),
            );
            assert_eq!(close.outcome, FilterAuditOutcome::Skipped, "role={role:?}");
            assert_eq!(
                close.reason,
                Some("not_applicable_to_close_or_hold"),
                "role={role:?}"
            );

            let out_of_bounds = apply_exchange_filters(
                role,
                true,
                OrderSide::Sell,
                "0.00001",
                "200",
                Ok(&constraints("100")),
            );
            assert_ne!(
                out_of_bounds.outcome,
                FilterAuditOutcome::Reject,
                "exchange bounds must remain open-only for role={role:?}"
            );
            for (quantity, price) in [("bad-quantity", "100"), ("1", "bad-price")] {
                let malformed = apply_exchange_filters(
                    role,
                    true,
                    OrderSide::Sell,
                    quantity,
                    price,
                    Ok(&constraints("100")),
                );
                assert_eq!(
                    malformed.outcome,
                    FilterAuditOutcome::Skipped,
                    "malformed close normalization input must fail open for role={role:?}"
                );
            }
        }
    }

    #[test]
    fn f5_public_filter_urls_are_derived_from_the_selected_venue() {
        assert_eq!(
            public_endpoint_url(Venue::Testnet, EXCHANGE_INFO_PATH),
            "https://demo-fapi.binance.com/fapi/v1/exchangeInfo"
        );
        assert_eq!(
            public_endpoint_url(Venue::Mainnet, EXCHANGE_INFO_PATH),
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        );
        assert_eq!(
            public_endpoint_url(Venue::Testnet, MARK_PRICE_PATH),
            "https://demo-fapi.binance.com/fapi/v1/premiumIndex"
        );
        let source = include_str!("exchange_filters.rs");
        let legacy_process_base_url = ["BINANCE_FUTURES_", "BASE_URL"].concat();
        assert!(!source.contains(&legacy_process_base_url));
    }

    #[test]
    fn f6_price_rounding_is_always_adverse_to_the_order_side() {
        assert_eq!(price_round_direction(OrderSide::Buy), RoundDirection::Down);
        assert_eq!(price_round_direction(OrderSide::Sell), RoundDirection::Up);
        let fixture = constraints("100");
        let buy = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Buy,
            "1",
            "100.09",
            Ok(&fixture),
        );
        let sell = apply_exchange_filters(
            OrderRole::Entry,
            false,
            OrderSide::Sell,
            "1",
            "99.91",
            Ok(&fixture),
        );
        assert_eq!(buy.price, "100");
        assert_eq!(sell.price, "100");
        assert_eq!(OrderSide::for_action("open_long").as_binance(), "BUY");
        assert_eq!(OrderSide::for_action("close_short").as_binance(), "BUY");
        assert_eq!(OrderSide::for_action("open_short").as_binance(), "SELL");
        assert_eq!(OrderSide::for_action("close_long").as_binance(), "SELL");
    }

    #[test]
    fn r1_non_entry_without_effective_reduce_only_rounds_down() {
        assert_eq!(
            quantity_round_direction(OrderRole::Exit, false),
            RoundDirection::Down
        );
        let result = apply_exchange_filters(
            OrderRole::Exit,
            false,
            OrderSide::Sell,
            "0.007956107745",
            "",
            Ok(&constraints("62844.8")),
        );
        assert_eq!(result.quantity, "0.0079");
        assert_eq!(result.outcome, FilterAuditOutcome::Capped);
    }

    #[test]
    fn r2_verified_reduce_only_non_entry_rounds_up_to_avoid_a_sub_step_tail() {
        assert_eq!(
            quantity_round_direction(OrderRole::Exit, true),
            RoundDirection::Up
        );
        let result = apply_exchange_filters(
            OrderRole::Exit,
            true,
            OrderSide::Sell,
            "0.007956107745",
            "",
            Ok(&constraints("62844.8")),
        );
        assert_eq!(result.quantity, "0.008");
        assert_eq!(result.outcome, FilterAuditOutcome::Capped);

        let close_position = apply_exchange_filters(
            OrderRole::ProtectionSl,
            true,
            OrderSide::Sell,
            "",
            "99.91",
            Ok(&constraints("100")),
        );
        assert_eq!(close_position.quantity, "");
        assert_eq!(close_position.price, "100");
        assert_eq!(close_position.outcome, FilterAuditOutcome::Capped);
    }

    #[test]
    fn r3_entry_with_effective_reduce_only_still_rounds_down() {
        assert_eq!(
            quantity_round_direction(OrderRole::Entry, true),
            RoundDirection::Down
        );
        let result = apply_exchange_filters(
            OrderRole::Entry,
            true,
            OrderSide::Buy,
            "0.007956107745",
            "",
            Ok(&constraints("62844.8")),
        );
        assert_eq!(result.quantity, "0.0079");
        assert_eq!(result.outcome, FilterAuditOutcome::Capped);
    }

    #[derive(Clone)]
    struct StubSource {
        filter_calls: Arc<AtomicUsize>,
        mark_calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct FailsAfterFirstFilterSource {
        filter_calls: Arc<AtomicUsize>,
    }

    impl ExchangeFilterSource for FailsAfterFirstFilterSource {
        fn fetch_filters<'a>(
            &'a self,
            _venue: Venue,
            _symbol: &'a str,
        ) -> FetchFuture<'a, SymbolFilters> {
            let call = self.filter_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call == 0 {
                    Ok(filters())
                } else {
                    Err("shared cooldown blocks refresh".to_string())
                }
            })
        }

        fn fetch_mark_price<'a>(
            &'a self,
            _venue: Venue,
            _symbol: &'a str,
        ) -> FetchFuture<'a, String> {
            Box::pin(async { Ok("62844.8".to_string()) })
        }
    }

    impl ExchangeFilterSource for StubSource {
        fn fetch_filters<'a>(
            &'a self,
            _venue: Venue,
            _symbol: &'a str,
        ) -> FetchFuture<'a, SymbolFilters> {
            self.filter_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(filters()) })
        }

        fn fetch_mark_price<'a>(
            &'a self,
            _venue: Venue,
            _symbol: &'a str,
        ) -> FetchFuture<'a, String> {
            self.mark_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok("62844.8".to_string()) })
        }
    }

    #[tokio::test]
    async fn filter_and_mark_price_caches_are_per_venue_symbol_and_obey_their_ttls() {
        let filter_calls = Arc::new(AtomicUsize::new(0));
        let mark_calls = Arc::new(AtomicUsize::new(0));
        let cache = ExchangeFilterCache::with_source(
            Arc::new(StubSource {
                filter_calls: filter_calls.clone(),
                mark_calls: mark_calls.clone(),
            }),
            Duration::from_millis(100),
        );
        cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_000)
            .await
            .unwrap();
        cache
            .constraints(Venue::Testnet, "btcusdt", 1_050)
            .await
            .unwrap();
        cache
            .constraints(Venue::Mainnet, "BTCUSDT", 1_050)
            .await
            .unwrap();
        cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_100)
            .await
            .unwrap();
        assert_eq!(filter_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            mark_calls.load(Ordering::SeqCst),
            2,
            "the same venue+symbol mark price must be fetched only once inside its TTL"
        );
    }

    #[tokio::test]
    async fn market_reference_cache_keeps_original_observation_and_close_fetches_no_quote() {
        let filter_calls = Arc::new(AtomicUsize::new(0));
        let mark_calls = Arc::new(AtomicUsize::new(0));
        let cache = ExchangeFilterCache::with_source_and_mark_ttl(
            Arc::new(StubSource {
                filter_calls,
                mark_calls: mark_calls.clone(),
            }),
            Duration::from_secs(300),
            Duration::from_secs(60),
        );
        let first = cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_000)
            .await
            .unwrap();
        assert_eq!(first.mark_price_as_of_ms, Some(1_000));
        let old = cache
            .constraints(Venue::Testnet, "BTCUSDT", 46_000)
            .await
            .unwrap();
        assert_eq!(
            old.mark_price_as_of_ms,
            Some(1_000),
            "a cache hit must not refresh provenance"
        );
        assert_eq!(mark_calls.load(Ordering::SeqCst), 1);
        let risk_reference = crate::risk::EntryMarketReference::from_constraints(
            Some(&Ok(old)),
            "binance|futures|BTC|USDT|USDT",
            46_000,
        );
        let json = serde_json::to_value(risk_reference).unwrap();
        assert_eq!(json["unavailable_reason"], "venue_quote_observation_stale");
        let close = cache
            .close_constraints(Venue::Testnet, "BTCUSDT", 61_000)
            .await
            .unwrap();
        assert_eq!(close.mark_price_as_of_ms, None);
        assert_eq!(
            mark_calls.load(Ordering::SeqCst),
            1,
            "close needs no mark-price request"
        );
        let refreshed = cache
            .constraints(Venue::Testnet, "BTCUSDT", 61_000)
            .await
            .unwrap();
        assert_eq!(refreshed.mark_price_as_of_ms, Some(61_000));
        assert_eq!(mark_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn close_uses_expired_known_good_filters_but_entry_still_fails_closed() {
        let filter_calls = Arc::new(AtomicUsize::new(0));
        let cache = ExchangeFilterCache::with_source(
            Arc::new(FailsAfterFirstFilterSource {
                filter_calls: filter_calls.clone(),
            }),
            Duration::from_millis(100),
        );
        let fresh = cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_000)
            .await
            .expect("seed known-good filters");
        assert!(!fresh.stale_filters);

        let close = cache
            .close_constraints(Venue::Testnet, "BTCUSDT", 1_100)
            .await
            .expect("close may reuse known-good filters when refresh is blocked");
        assert!(close.stale_filters);
        assert_eq!(close.filters, filters());
        let entry = cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_100)
            .await
            .expect_err("entry must never reuse an expired filter value");
        assert!(entry.contains("shared cooldown blocks refresh"));
    }

    #[tokio::test]
    async fn close_refuses_a_known_good_filter_older_than_the_stale_maximum() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _stale_max = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_FILTER_STALE_MAX_MS".to_string(),
            "300000",
        );
        let cache = ExchangeFilterCache::with_source(
            Arc::new(FailsAfterFirstFilterSource {
                filter_calls: Arc::new(AtomicUsize::new(0)),
            }),
            Duration::from_millis(100),
        );
        cache
            .constraints(Venue::Testnet, "BTCUSDT", 1_000)
            .await
            .expect("seed known-good filters");
        let error = cache
            .close_constraints(Venue::Testnet, "BTCUSDT", 301_001)
            .await
            .expect_err("stale fallback must have a hard age ceiling");
        assert!(error.contains("shared cooldown blocks refresh"));
    }

    #[test]
    fn filter_stale_and_mark_price_cache_env_bounds_are_pinned() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _stale = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_FILTER_STALE_MAX_MS".to_string(),
            "1",
        );
        let _mark = crate::binance_futures::tests::TestEnvVar::set(
            "RUSTCORE_MARK_PRICE_CACHE_MS".to_string(),
            "999999",
        );
        assert_eq!(stale_filter_max_ms_from_env(), 300_000);
        assert_eq!(
            mark_price_cache_ttl_from_env(),
            Duration::from_millis(60_000)
        );
    }

    #[test]
    fn shared_cache_clones_observe_the_same_entries_and_production_never_calls_new() {
        let first = ExchangeFilterCache::shared();
        let second = ExchangeFilterCache::shared();
        assert!(Arc::ptr_eq(&first.entries, &second.entries));
        let key = CacheKey {
            venue: Venue::Testnet,
            symbol: format!("UNIT{}", ulid::Ulid::new()),
        };
        first.entries.lock().expect("entries").insert(
            key.clone(),
            CacheEntry {
                filters: filters(),
                fetched_at_ms: 1_000,
                expires_at_ms: 2_000,
            },
        );
        assert!(second.entries.lock().expect("entries").contains_key(&key));
        second.entries.lock().expect("entries").remove(&key);

        for (name, source) in [
            ("exchange_filters.rs", include_str!("exchange_filters.rs")),
            ("protection.rs", include_str!("protection.rs")),
            ("kill_switch.rs", include_str!("kill_switch.rs")),
            ("svc.rs", include_str!("svc.rs")),
        ] {
            let production = source
                .rsplit_once("#[cfg(test)]\nmod tests")
                .map(|(production, _)| production)
                .unwrap_or(source);
            assert_eq!(
                production.matches("ExchangeFilterCache::new()").count(),
                0,
                "production code in {name} must use the process-shared cache"
            );
        }
    }

    #[tokio::test]
    async fn close_filter_lookup_does_not_depend_on_mark_price_io() {
        let filter_calls = Arc::new(AtomicUsize::new(0));
        let mark_calls = Arc::new(AtomicUsize::new(0));
        let cache = ExchangeFilterCache::with_source(
            Arc::new(StubSource {
                filter_calls: filter_calls.clone(),
                mark_calls: mark_calls.clone(),
            }),
            Duration::from_secs(1),
        );
        let constraints = cache
            .close_constraints(Venue::Testnet, "BTCUSDT", 1_000)
            .await
            .unwrap();
        assert_eq!(constraints.mark_price, "1");
        assert!(!constraints.stale_filters);
        assert_eq!(filter_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mark_calls.load(Ordering::SeqCst), 0);
    }
}
