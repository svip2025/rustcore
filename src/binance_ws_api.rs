use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::{self, Message};

use crate::binance_futures::{
    egress_scope_for_transport_and_route_with_prefix, publish_scoped_transport_cooldown,
    shared_egress_cooldown, signed_query_hmac, BinanceKeys,
};
use crate::rate_limiter::{Admission, CredentialRateLimiter, RequestClass, TokenBucketConfig};
use crate::store::Store;
use crate::svc::{Clock, SystemClock};

const WS_API_MAINNET: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
const WS_API_TESTNET: &str = "wss://testnet.binancefuture.com/ws-fapi/v1";
const WS_HANDSHAKE_WEIGHT: f64 = 5.0;
const WS_ACCOUNT_READ_WEIGHT: f64 = 5.0;
const MAX_CONNECTION_AGE_MS: i64 = 23 * 60 * 60 * 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const RECENT_SUCCESS_MAX_AGE_MS: i64 = 120_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WsApiHealth {
    pub connected: bool,
    pub connected_at_ms: Option<i64>,
    pub last_request_success_at_ms: Option<i64>,
    pub reconnect_count: u64,
    pub used_weight_1m: Option<u64>,
    pub limit_1m: Option<u64>,
    pub last_failure: Option<String>,
}

impl WsApiHealth {
    pub(crate) fn connection_age_ms(&self, now_ms: i64) -> Option<i64> {
        self.connected_at_ms
            .map(|connected| now_ms.saturating_sub(connected).max(0))
    }
}

pub(crate) struct WsCommand {
    id: String,
    payload: Message,
    result: oneshot::Sender<Result<Value, String>>,
}

fn ws_egress_scope(
    venue: Option<crate::venue::Venue>,
    endpoint: &str,
    route_id: Option<&str>,
) -> anyhow::Result<String> {
    egress_scope_for_transport_and_route_with_prefix(
        "binance-futures-ws",
        venue,
        endpoint,
        route_id,
    )
}

#[derive(Clone)]
pub(crate) struct BinanceWsApiClient {
    commands: mpsc::Sender<WsCommand>,
    health: Arc<Mutex<WsApiHealth>>,
    limiter: CredentialRateLimiter,
    store: Store,
    scope: String,
    credential_id: String,
    keys: BinanceKeys,
    recv_window_ms: u64,
    request_timeout: Duration,
    clock: Arc<dyn Clock>,
}

impl BinanceWsApiClient {
    pub(crate) fn spawn(
        keys: BinanceKeys,
        credential_id: &str,
        store: Store,
    ) -> anyhow::Result<Self> {
        let venue =
            crate::venue::venue_for_credential(credential_id).map_err(anyhow::Error::msg)?;
        let endpoint = match venue {
            Some(crate::venue::Venue::Mainnet) => WS_API_MAINNET,
            Some(crate::venue::Venue::Testnet) => WS_API_TESTNET,
            None => return Err(anyhow!("ws-fapi requires an explicit Binance venue")),
        };
        let route_id = std::env::var("BINANCE_EGRESS_ROUTE_ID").ok();
        let scope = ws_egress_scope(venue, endpoint, route_id.as_deref())?;
        let limiter = CredentialRateLimiter::shared_ws(TokenBucketConfig::binance_ws_from_env());
        let (commands, receiver) = mpsc::channel(32);
        let health = Arc::new(Mutex::new(WsApiHealth::default()));
        let request_timeout = request_timeout_from_env();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let client = Self {
            commands,
            health: health.clone(),
            limiter: limiter.clone(),
            store: store.clone(),
            scope: scope.clone(),
            credential_id: credential_id.to_string(),
            keys,
            recv_window_ms: 5_000,
            request_timeout,
            clock: clock.clone(),
        };
        let task_keys = client.keys.clone();
        let task_credential = client.credential_id.clone();
        tokio::spawn(run_connection_loop(
            endpoint.to_string(),
            task_keys,
            task_credential,
            store,
            scope,
            limiter,
            health,
            receiver,
            clock,
        ));
        Ok(client)
    }

    #[cfg(test)]
    fn with_channel_for_test(
        keys: BinanceKeys,
        credential_id: &str,
        store: Store,
        scope: String,
        limiter: CredentialRateLimiter,
        commands: mpsc::Sender<WsCommand>,
        health: Arc<Mutex<WsApiHealth>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            commands,
            health,
            limiter,
            store,
            scope,
            credential_id: credential_id.to_string(),
            keys,
            recv_window_ms: 5_000,
            request_timeout: Duration::from_millis(500),
            clock,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_mock_account_reads_for_test(
        store: Store,
        scope: String,
        positions: Result<Value, String>,
        balances: Result<Value, String>,
    ) -> Self {
        Self::new_with_recorded_account_reads_for_test(
            store,
            scope,
            positions,
            balances,
            Arc::new(std::sync::Mutex::new(Vec::new())),
        )
    }

    // Only the exchange response channel is fake. Callers still execute the
    // production admission, signing, serialization and account-read methods.
    #[cfg(test)]
    pub(crate) fn new_with_recorded_account_reads_for_test(
        store: Store,
        scope: String,
        positions: Result<Value, String>,
        balances: Result<Value, String>,
        requests: Arc<std::sync::Mutex<Vec<Value>>>,
    ) -> Self {
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0));
        let (commands, mut receiver) = mpsc::channel::<WsCommand>(8);
        let health = Arc::new(Mutex::new(WsApiHealth {
            connected: true,
            connected_at_ms: Some(crate::svc::now_ms()),
            ..WsApiHealth::default()
        }));
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let method = match &command.payload {
                    Message::Text(text) => {
                        serde_json::from_str::<Value>(text).ok().and_then(|value| {
                            requests
                                .lock()
                                .expect("capture WS request")
                                .push(value.clone());
                            value
                                .get("method")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                    }
                    _ => None,
                };
                let result = match method.as_deref() {
                    // Accept both documented methods at the fake exchange so
                    // a method regression fails the captured-request assertion,
                    // not an unrelated mock "unexpected method" error.
                    Some("account.position" | "v2/account.position") => positions.clone(),
                    Some("v2/account.balance") => balances.clone(),
                    _ => Err("unexpected ws-fapi method".to_string()),
                };
                let _ = command.result.send(result);
            }
        });
        Self {
            commands,
            health,
            limiter,
            store,
            scope,
            credential_id: "cred-test".to_string(),
            keys: BinanceKeys {
                api_key: "key".into(),
                api_secret: "secret".into(),
            },
            recv_window_ms: 5_000,
            request_timeout: Duration::from_millis(500),
            clock: Arc::new(SystemClock),
        }
    }

    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    pub(crate) async fn health(&self) -> WsApiHealth {
        self.health.lock().await.clone()
    }

    pub(crate) async fn is_healthy(&self) -> bool {
        let now_ms = self.clock.now_ms();
        let health = self.health().await;
        if !health.connected {
            return false;
        }
        if health
            .last_request_success_at_ms
            .is_some_and(|success| now_ms.saturating_sub(success) > RECENT_SUCCESS_MAX_AGE_MS)
        {
            return false;
        }
        match shared_egress_cooldown(&self.scope, Some(&self.store), now_ms).await {
            Ok(cooldown) => {
                now_ms >= cooldown.ban_until_ms && now_ms >= cooldown.rate_limit_until_ms
            }
            Err(error) => {
                tracing::warn!(
                    credential_id = %self.credential_id,
                    ws_scope = %self.scope,
                    %error,
                    "ws-fapi health could not read its cooldown; selecting REST"
                );
                false
            }
        }
    }

    pub(crate) async fn account_reads(
        &self,
        position_symbol: Option<&str>,
    ) -> anyhow::Result<(Value, Value)> {
        let balance = self.request("v2/account.balance", Vec::new());
        // The projection needs leverage, which account.position reports.
        // v2/account.position omits it; both endpoints have request weight 5.
        let position = self.request(
            "account.position",
            position_symbol
                .map(|symbol| vec![("symbol".to_string(), symbol.to_string())])
                .unwrap_or_default(),
        );
        let (balances, positions) = tokio::try_join!(balance, position)?;
        Ok((positions, balances))
    }

    async fn request(&self, method: &str, extra: Vec<(String, String)>) -> anyhow::Result<Value> {
        match self
            .limiter
            .acquire_prioritized(
                &self.scope,
                &self.credential_id,
                WS_ACCOUNT_READ_WEIGHT,
                RequestClass::PeriodicReconcile,
                0.0,
            )
            .await
        {
            Admission::Admitted => {}
            other => {
                let reason = format!("ws-fapi request admission rejected: {other:?}");
                self.health.lock().await.last_failure = Some(reason.clone());
                return Err(anyhow!(reason));
            }
        }
        let id = ulid::Ulid::new().to_string();
        let payload = signed_request_payload(
            &id,
            method,
            &self.keys,
            self.clock.now_ms().max(0),
            self.recv_window_ms,
            extra,
        )?;
        let (result_tx, result_rx) = oneshot::channel();
        let command = WsCommand {
            id,
            payload: Message::Text(payload.to_string()),
            result: result_tx,
        };
        let exchange = async {
            self.commands
                .send(command)
                .await
                .map_err(|_| anyhow!("ws-fapi connection task is not running"))?;
            match result_rx.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => Err(anyhow!(error)),
                Err(_closed) => Err(anyhow!("ws-fapi response channel closed")),
            }
        };
        match tokio::time::timeout(self.request_timeout, exchange).await {
            Ok(result) => result,
            Err(_elapsed) => {
                let reason = format!(
                    "ws-fapi request exceeded {}ms deadline",
                    self.request_timeout.as_millis()
                );
                let mut health = self.health.lock().await;
                health.last_failure = Some(reason.clone());
                Err(anyhow!(reason))
            }
        }
    }
}

pub(crate) fn ws_api_enabled() -> bool {
    std::env::var("RUSTCORE_BINANCE_WS_API_ENABLED")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
}

fn request_timeout_from_env() -> Duration {
    Duration::from_millis(
        std::env::var("RUSTCORE_BINANCE_WS_REQUEST_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
            .clamp(100, 60_000),
    )
}

fn ws_reconnect_delay_ms(consecutive_short_cycles: u32, cap_ms: u64) -> u64 {
    crate::binance_user_stream::user_stream_retry_delay_ms(consecutive_short_cycles, cap_ms)
}

async fn wait_before_reconnect(
    clock: &dyn Clock,
    previous_short_cycles: u32,
    cycle_ms: u64,
    cap_ms: u64,
) -> u32 {
    let short_cycles =
        crate::binance_user_stream::next_short_cycle_count(previous_short_cycles, cycle_ms);
    clock
        .sleep(Duration::from_millis(ws_reconnect_delay_ms(
            short_cycles,
            cap_ms,
        )))
        .await;
    short_cycles
}

fn signed_request_payload(
    id: &str,
    method: &str,
    keys: &BinanceKeys,
    timestamp_ms: i64,
    recv_window_ms: u64,
    extra: Vec<(String, String)>,
) -> anyhow::Result<Value> {
    let mut params = BTreeMap::<String, String>::new();
    for (name, value) in extra {
        params.insert(name, value);
    }
    params.insert("apiKey".to_string(), keys.api_key.clone());
    params.insert("recvWindow".to_string(), recv_window_ms.to_string());
    params.insert("timestamp".to_string(), timestamp_ms.max(0).to_string());
    let ordered = params
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let (_query, signature) = signed_query_hmac(keys.api_secret.as_bytes(), &ordered)
        .context("encode ws-fapi signature input")?;
    params.insert("signature".to_string(), signature);
    Ok(serde_json::json!({"id": id, "method": method, "params": params}))
}

async fn charge_handshake(
    limiter: &CredentialRateLimiter,
    scope: &str,
    credential_id: &str,
) -> anyhow::Result<()> {
    match limiter
        .acquire_prioritized(
            scope,
            credential_id,
            WS_HANDSHAKE_WEIGHT,
            RequestClass::PeriodicReconcile,
            0.0,
        )
        .await
    {
        Admission::Admitted => Ok(()),
        other => Err(anyhow!("ws-fapi handshake admission rejected: {other:?}")),
    }
}

/// 这条后台任务是 `commands` 接收端的唯一持有者，而**每个还能发请求的
/// owner 都持有一个 `Sender`**（`BinanceWsApiClient` 及其克隆）。所有 owner
/// 消失后再重连只会替一个已经不存在的管理任务继续消耗共享请求预算，因此
/// worker 必须停下来。判据只看命令通道：父任务的 `JoinHandle` 结束**不**代表
/// 这条子连接已经结束，也不能反过来用它来推定 owner 还在。
fn last_request_owner_left(commands: &mpsc::Receiver<WsCommand>) -> bool {
    commands.is_closed()
}

/// worker 退出时对**还排在命令队列里**的请求给出明确结果。已经写进 socket、
/// 正在等交易所回包的请求由 `run_ws_api_session` 的 `fail_pending` 完成；本函数
/// 不声称把所有已进入网络的请求都排空。
async fn stop_worker(
    mut commands: mpsc::Receiver<WsCommand>,
    health: &Arc<Mutex<WsApiHealth>>,
    credential_id: &str,
) {
    commands.close();
    while let Ok(command) = commands.try_recv() {
        let _ = command.result.send(Err(
            "ws-fapi worker stopped: request owner is gone".to_string()
        ));
    }
    health.lock().await.connected = false;
    tracing::info!(
        credential_id,
        "ws-fapi connection loop stopped: its last request owner is gone"
    );
}

#[allow(clippy::too_many_arguments)]
async fn run_connection_loop(
    endpoint: String,
    _keys: BinanceKeys,
    credential_id: String,
    store: Store,
    scope: String,
    limiter: CredentialRateLimiter,
    health: Arc<Mutex<WsApiHealth>>,
    mut commands: mpsc::Receiver<WsCommand>,
    clock: Arc<dyn Clock>,
) {
    let mut short_cycles = 0_u32;
    let mut attempts = 0_u64;
    loop {
        if last_request_owner_left(&commands) {
            break;
        }
        if let Err(error) = charge_handshake(&limiter, &scope, &credential_id).await {
            health.lock().await.last_failure = Some(error.to_string());
            if last_request_owner_left(&commands) {
                break;
            }
            short_cycles = wait_before_reconnect(
                clock.as_ref(),
                short_cycles,
                0,
                crate::reconcile::periodic_reconcile_interval_ms(),
            )
            .await;
        } else {
            if let Err(error) = crate::round2_diagnostics::record_connection_attempt(
                &store,
                &credential_id,
                "TRADE_WS_API",
                attempts > 0,
                clock.now_ms(),
            )
            .await
            {
                health.lock().await.last_failure = Some(error.to_string());
                short_cycles = wait_before_reconnect(
                    clock.as_ref(),
                    short_cycles,
                    0,
                    crate::reconcile::periodic_reconcile_interval_ms(),
                )
                .await;
                continue;
            }
            attempts = attempts.saturating_add(1);
            let connector = match crate::venue::ws_transport_for_credential(&credential_id)
                .map_err(anyhow::Error::msg)
                .and_then(|transport| transport.connector().map_err(anyhow::Error::msg))
            {
                Ok(connector) => connector,
                Err(error) => {
                    health.lock().await.last_failure = Some(error.to_string());
                    if last_request_owner_left(&commands) {
                        break;
                    }
                    short_cycles = wait_before_reconnect(
                        clock.as_ref(),
                        short_cycles,
                        0,
                        crate::reconcile::periodic_reconcile_interval_ms(),
                    )
                    .await;
                    continue;
                }
            };
            let connect_deadline = crate::binance_futures::ws_timeouts_from_env().0;
            let outcome = tokio::time::timeout(
                connect_deadline,
                tokio_tungstenite::connect_async_tls_with_config(
                    endpoint.clone(),
                    None,
                    false,
                    connector,
                ),
            )
            .await;
            let cycle_ms = match outcome {
                Ok(Ok((mut socket, _response))) => {
                    let connected_at = clock.now_ms();
                    {
                        let mut state = health.lock().await;
                        state.connected = true;
                        state.connected_at_ms = Some(connected_at);
                        state.reconnect_count = attempts.saturating_sub(1);
                        state.last_failure = None;
                    }
                    let session = run_ws_api_session(
                        &mut socket,
                        &mut commands,
                        &health,
                        &limiter,
                        &store,
                        &scope,
                        &credential_id,
                        clock.as_ref(),
                    )
                    .await;
                    let cycle_ms = clock.now_ms().saturating_sub(connected_at).max(0) as u64;
                    let mut state = health.lock().await;
                    state.connected = false;
                    state.last_failure = session.err().map(|error| error.to_string());
                    cycle_ms
                }
                Ok(Err(error)) => {
                    health.lock().await.last_failure = Some(error.to_string());
                    0
                }
                Err(_elapsed) => {
                    health.lock().await.last_failure = Some(format!(
                        "ws-fapi connect exceeded {}ms deadline",
                        connect_deadline.as_millis()
                    ));
                    0
                }
            };
            if last_request_owner_left(&commands) {
                break;
            }
            short_cycles = wait_before_reconnect(
                clock.as_ref(),
                short_cycles,
                cycle_ms,
                crate::reconcile::periodic_reconcile_interval_ms(),
            )
            .await;
        }
    }
    stop_worker(commands, &health, &credential_id).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_ws_api_session<S>(
    socket: &mut S,
    commands: &mut mpsc::Receiver<WsCommand>,
    health: &Arc<Mutex<WsApiHealth>>,
    limiter: &CredentialRateLimiter,
    store: &Store,
    scope: &str,
    credential_id: &str,
    clock: &dyn Clock,
) -> anyhow::Result<()>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut pending = HashMap::<String, oneshot::Sender<Result<Value, String>>>::new();
    let connected_at = clock.now_ms();
    loop {
        let age_ms = clock.now_ms().saturating_sub(connected_at).max(0);
        if age_ms >= MAX_CONNECTION_AGE_MS {
            fail_pending(
                &mut pending,
                "ws-fapi connection reached the 23h rotation age",
            );
            return Err(anyhow!("ws-fapi proactive 23h reconnect"));
        }
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    fail_pending(&mut pending, "ws-fapi request owner closed");
                    return Ok(());
                };
                pending.insert(command.id, command.result);
                if let Err(error) = socket.send(command.payload).await {
                    fail_pending(&mut pending, &format!("ws-fapi write failed: {error}"));
                    return Err(anyhow!("ws-fapi write failed: {error}"));
                }
            }
            frame = socket.next() => {
                let Some(frame) = frame else {
                    fail_pending(&mut pending, "ws-fapi connection ended");
                    return Err(anyhow!("ws-fapi connection ended"));
                };
                match frame.context("read ws-fapi frame")? {
                    Message::Ping(payload) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| anyhow!("answer ws-fapi ping failed: {error}"))?;
                    }
                    Message::Text(text) => {
                        handle_response(
                            &text,
                            &mut pending,
                            health,
                            limiter,
                            store,
                            scope,
                            credential_id,
                            clock.now_ms(),
                        ).await?;
                    }
                    Message::Binary(payload) => {
                        let text = std::str::from_utf8(&payload)
                            .context("ws-fapi binary response is not UTF-8")?;
                        handle_response(
                            text,
                            &mut pending,
                            health,
                            limiter,
                            store,
                            scope,
                            credential_id,
                            clock.now_ms(),
                        ).await?;
                    }
                    Message::Close(frame) => {
                        fail_pending(&mut pending, "ws-fapi close frame received");
                        return Err(anyhow!("ws-fapi closed: {frame:?}"));
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            _ = clock.sleep(Duration::from_millis((MAX_CONNECTION_AGE_MS - age_ms).max(1) as u64)) => {
                fail_pending(&mut pending, "ws-fapi connection reached the 23h rotation age");
                return Err(anyhow!("ws-fapi proactive 23h reconnect"));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_response(
    text: &str,
    pending: &mut HashMap<String, oneshot::Sender<Result<Value, String>>>,
    health: &Arc<Mutex<WsApiHealth>>,
    limiter: &CredentialRateLimiter,
    store: &Store,
    scope: &str,
    credential_id: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    let envelope: Value = serde_json::from_str(text).context("decode ws-fapi response")?;
    let id = envelope
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ws-fapi response omitted string id"))?;
    let mut used = None;
    let mut limit = None;
    if let Some(rate_limits) = envelope.get("rateLimits").and_then(Value::as_array) {
        for item in rate_limits {
            if item.get("rateLimitType").and_then(Value::as_str) == Some("REQUEST_WEIGHT") {
                used = item.get("count").and_then(Value::as_u64);
                limit = item.get("limit").and_then(Value::as_u64);
                if let (Some(count), Some(response_limit)) = (used, limit) {
                    limiter.observe_egress_weight_with_limit(scope, count, response_limit, now_ms);
                } else if let Some(count) = used {
                    limiter.observe_egress_weight(scope, count, now_ms);
                }
            }
        }
    }
    let status = envelope
        .get("status")
        .and_then(Value::as_u64)
        .unwrap_or(200) as u16;
    let code = envelope
        .get("code")
        .or_else(|| envelope.get("error").and_then(|error| error.get("code")))
        .and_then(Value::as_i64);
    let cooldown_status = if status == 418 {
        Some(418)
    } else if status == 429 || code == Some(-1003) {
        Some(429)
    } else {
        None
    };
    if let Some(cooldown_status) = cooldown_status {
        let retry_after = envelope.get("retryAfter").and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| value.as_u64().map(|v| v.to_string()))
        });
        publish_scoped_transport_cooldown(
            store,
            scope,
            cooldown_status,
            retry_after.as_deref(),
            now_ms,
        )
        .await?;
    }
    {
        let mut state = health.lock().await;
        state.used_weight_1m = match (state.used_weight_1m, used) {
            (Some(previous), Some(current)) => Some(previous.max(current)),
            (previous, current) => current.or(previous),
        };
        state.limit_1m = match (state.limit_1m, limit) {
            (Some(previous), Some(current)) => Some(previous.min(current)),
            (previous, current) => current.or(previous),
        };
        if status < 400 && code.is_none() && envelope.get("result").is_some() {
            state.last_request_success_at_ms = Some(now_ms);
            state.last_failure = None;
        } else {
            state.last_failure = Some(format!("ws-fapi status={status} code={code:?}"));
        }
    }
    let outcome = if status < 400 && code.is_none() {
        envelope
            .get("result")
            .cloned()
            .ok_or_else(|| "ws-fapi success response omitted result".to_string())
    } else {
        Err(format!("ws-fapi status={status} code={code:?}"))
    };
    if let Some(sender) = pending.remove(id) {
        let _ = sender.send(outcome);
    } else {
        tracing::warn!(
            credential_id,
            response_id = id,
            "late or unknown ws-fapi response"
        );
    }
    Ok(())
}

fn fail_pending(
    pending: &mut HashMap<String, oneshot::Sender<Result<Value, String>>>,
    reason: &str,
) {
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(reason.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
    use std::sync::Once;
    use std::task::{Context as TaskContext, Poll};

    static INSTALL_DRIVERS: Once = Once::new();

    impl BinanceWsApiClient {
        pub(crate) fn new_with_mock_account_reads_and_health_for_test(
            store: Store,
            scope: String,
            positions: Value,
            balances: Value,
            health_state: WsApiHealth,
            request_count: Arc<AtomicUsize>,
        ) -> Self {
            let limiter =
                CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0));
            let (commands, mut receiver) = mpsc::channel::<WsCommand>(8);
            let health = Arc::new(Mutex::new(health_state));
            tokio::spawn(async move {
                while let Some(command) = receiver.recv().await {
                    request_count.fetch_add(1, Ordering::SeqCst);
                    let method = match &command.payload {
                        Message::Text(text) => {
                            serde_json::from_str::<Value>(text).ok().and_then(|value| {
                                value
                                    .get("method")
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            })
                        }
                        _ => None,
                    };
                    let result = match method.as_deref() {
                        Some("account.position") => Ok(positions.clone()),
                        Some("v2/account.balance") => Ok(balances.clone()),
                        _ => Err("unexpected ws-fapi method".to_string()),
                    };
                    let _ = command.result.send(result);
                }
            });
            Self {
                commands,
                health,
                limiter,
                store,
                scope,
                credential_id: format!("cred_{}", ulid::Ulid::new()),
                keys: BinanceKeys {
                    api_key: "key".into(),
                    api_secret: "secret".into(),
                },
                recv_window_ms: 5_000,
                request_timeout: Duration::from_millis(500),
                clock: Arc::new(SystemClock),
            }
        }
    }

    async fn test_store(label: &str) -> (Store, std::path::PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-ws-api-{label}-{}.sqlite",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect ws-api test store");
        (store, path)
    }

    async fn cleanup(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    struct ChannelSocket {
        incoming: mpsc::UnboundedReceiver<Result<Message, tungstenite::Error>>,
        outgoing: mpsc::UnboundedSender<Message>,
    }

    impl Stream for ChannelSocket {
        type Item = Result<Message, tungstenite::Error>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.incoming.poll_recv(cx)
        }
    }

    impl Sink<Message> for ChannelSocket {
        type Error = tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.get_mut()
                .outgoing
                .send(item)
                .map_err(|_| tungstenite::Error::ConnectionClosed)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct SessionFixture {
        client: BinanceWsApiClient,
        incoming: mpsc::UnboundedSender<Result<Message, tungstenite::Error>>,
        outgoing: mpsc::UnboundedReceiver<Message>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
        limiter: CredentialRateLimiter,
        store: Store,
        path: std::path::PathBuf,
        scope: String,
    }

    async fn session_fixture(label: &str) -> SessionFixture {
        let (store, path) = test_store(label).await;
        let scope = format!(
            "binance-futures-ws:base-url:https://{}.invalid",
            ulid::Ulid::new()
        );
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0));
        limiter.set_stored_tokens(&scope, 100.0).await;
        let (command_tx, mut command_rx) = mpsc::channel(8);
        let health = Arc::new(Mutex::new(WsApiHealth {
            connected: true,
            connected_at_ms: Some(crate::svc::now_ms()),
            ..WsApiHealth::default()
        }));
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let mut socket = ChannelSocket {
            incoming: incoming_rx,
            outgoing: outgoing_tx,
        };
        let task_health = health.clone();
        let task_limiter = limiter.clone();
        let task_store = store.clone();
        let task_scope = scope.clone();
        let task = tokio::spawn(async move {
            run_ws_api_session(
                &mut socket,
                &mut command_rx,
                &task_health,
                &task_limiter,
                &task_store,
                &task_scope,
                "cred-test",
                &SystemClock,
            )
            .await
        });
        let client = BinanceWsApiClient::with_channel_for_test(
            BinanceKeys {
                api_key: "key".into(),
                api_secret: "secret".into(),
            },
            "cred-test",
            store.clone(),
            scope.clone(),
            limiter.clone(),
            command_tx,
            health,
            Arc::new(SystemClock),
        );
        SessionFixture {
            client,
            incoming: incoming_tx,
            outgoing: outgoing_rx,
            task,
            limiter,
            store,
            path,
            scope,
        }
    }

    fn request_id_and_method(message: Message) -> (String, String) {
        let Message::Text(text) = message else {
            panic!("expected text request")
        };
        let value: Value = serde_json::from_str(&text).expect("request JSON");
        (
            value["id"].as_str().expect("id").to_string(),
            value["method"].as_str().expect("method").to_string(),
        )
    }

    fn success(id: &str, result: Value, count: u64, limit: u64) -> Message {
        Message::Text(
            serde_json::json!({
                "id": id,
                "status": 200,
                "result": result,
                "rateLimits": [{
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": limit,
                    "count": count
                }]
            })
            .to_string(),
        )
    }

    struct JumpClock {
        now_ms: AtomicI64,
        sleeps: AtomicUsize,
    }

    fn health_client_with_clock(
        store: Store,
        scope: String,
        health_state: WsApiHealth,
        clock: Arc<dyn Clock>,
    ) -> BinanceWsApiClient {
        let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0));
        let (commands, mut receiver) = mpsc::channel::<WsCommand>(1);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let _ = command
                    .result
                    .send(Err("health-only test must not issue a request".to_string()));
            }
        });
        BinanceWsApiClient::with_channel_for_test(
            BinanceKeys {
                api_key: "key".into(),
                api_secret: "secret".into(),
            },
            &format!("cred_{}", ulid::Ulid::new()),
            store,
            scope,
            limiter,
            commands,
            Arc::new(Mutex::new(health_state)),
            clock,
        )
    }

    #[tokio::test]
    async fn p1_active_ws_scope_cooldowns_make_health_false_and_expired_rows_restore_true() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("a9-r1-p1-health").await;
            let scope = format!(
                "binance-futures-ws:route:{}:venue:testnet",
                ulid::Ulid::new()
            );
            let now_ms = 1_725_000_000_123_i64;
            let client = health_client_with_clock(
                store.clone(),
                scope.clone(),
                WsApiHealth {
                    connected: true,
                    connected_at_ms: Some(now_ms - 1_000),
                    last_request_success_at_ms: Some(now_ms),
                    ..WsApiHealth::default()
                },
                Arc::new(JumpClock {
                    now_ms: AtomicI64::new(now_ms),
                    sleeps: AtomicUsize::new(0),
                }),
            );

            sqlx::query(
                "INSERT INTO binance_ip_ban_cooldowns \
                 (egress_scope, ban_until_ms, updated_at_ms) VALUES (?, ?, ?)",
            )
            .bind(&scope)
            .bind(now_ms + 1)
            .bind(now_ms)
            .execute(store.pool())
            .await
            .expect("seed active ws IP-ban cooldown");
            assert!(
                !client.is_healthy().await,
                "an unexpired ws-scope IP ban must make is_healthy false"
            );

            sqlx::query(
                "UPDATE binance_ip_ban_cooldowns SET ban_until_ms = ? \
                 WHERE egress_scope = ?",
            )
            .bind(now_ms)
            .bind(&scope)
            .execute(store.pool())
            .await
            .expect("expire ws IP-ban cooldown");
            sqlx::query(
                "INSERT INTO binance_rate_limit_cooldowns \
                 (egress_scope, cooldown_until_ms, updated_at_ms) VALUES (?, ?, ?)",
            )
            .bind(&scope)
            .bind(now_ms + 1)
            .bind(now_ms)
            .execute(store.pool())
            .await
            .expect("seed active ws rate-limit cooldown");
            assert!(
                !client.is_healthy().await,
                "an unexpired ws-scope rate-limit cooldown must make is_healthy false"
            );

            sqlx::query(
                "UPDATE binance_rate_limit_cooldowns SET cooldown_until_ms = ? \
                 WHERE egress_scope = ?",
            )
            .bind(now_ms)
            .bind(&scope)
            .execute(store.pool())
            .await
            .expect("expire ws rate-limit cooldown");
            assert!(
                client.is_healthy().await,
                "is_healthy must return true once both ws-scope cooldowns have expired"
            );
            cleanup(store, path).await;
        })
        .await
        .expect("P1 cooldown boundaries must not hang");
    }

    #[tokio::test]
    async fn p2_recent_success_age_boundary_is_strictly_greater_than_the_limit() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("a9-r1-p2-health").await;
            let now_ms = 1_725_000_000_123_i64;
            let client = health_client_with_clock(
                store.clone(),
                format!(
                    "binance-futures-ws:route:{}:venue:mainnet",
                    ulid::Ulid::new()
                ),
                WsApiHealth {
                    connected: true,
                    connected_at_ms: Some(now_ms - 1_000),
                    last_request_success_at_ms: Some(now_ms - RECENT_SUCCESS_MAX_AGE_MS - 1),
                    ..WsApiHealth::default()
                },
                Arc::new(JumpClock {
                    now_ms: AtomicI64::new(now_ms),
                    sleeps: AtomicUsize::new(0),
                }),
            );
            assert!(
                !client.is_healthy().await,
                "a ws success older than RECENT_SUCCESS_MAX_AGE_MS must make is_healthy false"
            );

            client.health.lock().await.last_request_success_at_ms =
                Some(now_ms - RECENT_SUCCESS_MAX_AGE_MS);
            assert!(
                client.is_healthy().await,
                "a ws success exactly RECENT_SUCCESS_MAX_AGE_MS old must remain healthy"
            );
            cleanup(store, path).await;
        })
        .await
        .expect("P2 freshness boundaries must not hang");
    }

    #[test]
    fn p4_production_ws_scope_derivation_matches_rest_except_for_its_prefix() {
        let route_id = format!("route-{}", ulid::Ulid::new());
        let base_url = format!(
            "https://{}.example.invalid/path/?q=1#fragment",
            ulid::Ulid::new()
        );
        for (venue, endpoint, route) in [
            (
                Some(crate::venue::Venue::Testnet),
                WS_API_TESTNET,
                Some(route_id.as_str()),
            ),
            (Some(crate::venue::Venue::Mainnet), WS_API_MAINNET, None),
            (None, base_url.as_str(), None),
        ] {
            let ws = ws_egress_scope(venue, endpoint, route).expect("derive ws egress scope");
            let rest = egress_scope_for_transport_and_route_with_prefix(
                "binance-futures-rest",
                venue,
                endpoint,
                route,
            )
            .expect("derive REST egress scope with the production REST prefix");
            assert!(ws.starts_with("binance-futures-ws:"));
            assert_eq!(
                ws.strip_prefix("binance-futures-ws"),
                rest.strip_prefix("binance-futures-rest"),
                "ws and REST scope derivation may differ only by transport prefix"
            );
        }

        let source = include_str!("binance_ws_api.rs");
        let production = source
            .split("    #[cfg(test)]\n    fn with_channel_for_test")
            .next()
            .expect("production source segment");
        let spawn = production
            .split("pub(crate) fn spawn(")
            .nth(1)
            .expect("spawn production function");
        assert!(
            spawn.contains("let scope = ws_egress_scope("),
            "spawn must consume the tested ws_egress_scope derivation"
        );
        assert_eq!(
            production.matches("\"binance-futures-ws\"").count(),
            1,
            "the ws transport prefix must have one production source of truth"
        );
    }

    impl Clock for JumpClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.sleeps.fetch_add(1, Ordering::SeqCst);
                self.now_ms.fetch_add(
                    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                    Ordering::SeqCst,
                );
            })
        }
    }

    #[test]
    fn i1_sorted_params_timestamp_and_signature_match_the_rest_hmac_byte_for_byte() {
        let keys = BinanceKeys {
            api_key: "key".into(),
            api_secret: "secret".into(),
        };
        let payload = signed_request_payload(
            "01TEST",
            "account.position",
            &keys,
            1_725_000_000_123,
            5_000,
            vec![("symbol".into(), "BTCUSDT".into())],
        )
        .expect("signed payload");
        let params = payload["params"].as_object().expect("params");
        assert_eq!(params["timestamp"], "1725000000123");
        assert_eq!(
            params["signature"],
            signed_query_hmac(
                keys.api_secret.as_bytes(),
                &[
                    ("apiKey".to_string(), "key".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("timestamp".to_string(), "1725000000123".to_string()),
                ]
            )
            .expect("REST signing seam")
            .1
        );
        assert_eq!(payload["method"], "account.position");
    }

    #[test]
    fn i10_ws_api_is_disabled_when_the_enable_variable_is_absent() {
        let _guard = crate::binance_futures::tests::env_serial_guard();
        std::env::remove_var("RUSTCORE_BINANCE_WS_API_ENABLED");
        assert!(!ws_api_enabled());
    }

    #[tokio::test]
    async fn request_timeout_covers_a_saturated_command_queue() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (store, path) = test_store("queue-timeout").await;
            let scope = format!(
                "binance-futures-ws:base-url:https://{}.invalid",
                ulid::Ulid::new()
            );
            let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0));
            let (commands, _receiver) = mpsc::channel(1);
            let (blocked_result, _blocked_receiver) = oneshot::channel();
            commands
                .send(WsCommand {
                    id: ulid::Ulid::new().to_string(),
                    payload: Message::Text("{}".to_string()),
                    result: blocked_result,
                })
                .await
                .expect("fill command queue");
            let health = Arc::new(Mutex::new(WsApiHealth {
                connected: true,
                connected_at_ms: Some(crate::svc::now_ms()),
                ..WsApiHealth::default()
            }));
            let client = BinanceWsApiClient::with_channel_for_test(
                BinanceKeys {
                    api_key: "key".into(),
                    api_secret: "secret".into(),
                },
                "cred-queue-timeout",
                store.clone(),
                scope,
                limiter,
                commands,
                health,
                Arc::new(SystemClock),
            );
            let error = client
                .request("v2/account.balance", Vec::new())
                .await
                .expect_err("a full command queue must obey the request deadline");
            assert!(error.to_string().contains("exceeded 500ms deadline"));
            drop(client);
            drop(_receiver);
            cleanup(store, path).await;
        })
        .await
        .expect("queue timeout mutation must turn red rather than hang");
    }

    #[tokio::test]
    async fn i11_five_disconnects_use_monotonic_capped_nonzero_reconnect_delays() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let cap = 30_000;
            let clock = JumpClock {
                now_ms: AtomicI64::new(0),
                sleeps: AtomicUsize::new(0),
            };
            let mut short_cycles = 0;
            let mut reconnect_at_ms = Vec::new();
            for _ in 0..5 {
                short_cycles = wait_before_reconnect(&clock, short_cycles, 0, cap).await;
                reconnect_at_ms.push(clock.now_ms());
            }
            assert_eq!(reconnect_at_ms, vec![4_000, 12_000, 28_000, 58_000, 88_000]);
            assert_eq!(clock.sleeps.load(Ordering::SeqCst), 5);
        })
        .await
        .expect("I11 reconnect schedule must not hang");
    }

    #[tokio::test]
    async fn i3_each_handshake_charges_five_and_n_reconnects_charge_five_n() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 0.1));
            let scope = format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new());
            limiter.set_stored_tokens(&scope, 100.0).await;
            for _ in 0..5 {
                charge_handshake(&limiter, &scope, "cred-handshake")
                    .await
                    .expect("admit handshake");
            }
            let remaining = limiter.stored_tokens(&scope).await.expect("bucket");
            assert!(
                (remaining - 75.0).abs() < 0.1,
                "five handshakes must cost 25: {remaining}"
            );
        })
        .await
        .expect("I3 must not hang");
    }

    #[tokio::test]
    async fn i5_ping_is_answered_while_a_request_is_pending() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut fixture = session_fixture("ping").await;
            let client = fixture.client.clone();
            let request =
                tokio::spawn(async move { client.request("v2/account.balance", Vec::new()).await });
            let outbound = fixture.outgoing.recv().await.expect("request frame");
            let (id, _) = request_id_and_method(outbound);
            fixture
                .incoming
                .send(Ok(Message::Ping(vec![7, 8])))
                .expect("ping");
            assert_eq!(
                fixture.outgoing.recv().await,
                Some(Message::Pong(vec![7, 8]))
            );
            fixture
                .incoming
                .send(Ok(success(&id, serde_json::json!([]), 5, 2400)))
                .expect("response");
            assert_eq!(
                request.await.expect("join").expect("request"),
                serde_json::json!([])
            );
            drop(fixture.client);
            fixture.task.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("I5 must not hang");
    }

    #[tokio::test]
    async fn i5_fake_clock_rotates_a_connection_at_23h_exactly_once() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = test_store("rotation").await;
            let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0));
            let scope = format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new());
            let (_command_tx, mut command_rx) = mpsc::channel(1);
            let health = Arc::new(Mutex::new(WsApiHealth::default()));
            let (_incoming_tx, incoming_rx) = mpsc::unbounded_channel();
            let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
            let mut socket = ChannelSocket {
                incoming: incoming_rx,
                outgoing: outgoing_tx,
            };
            let clock = JumpClock {
                now_ms: AtomicI64::new(0),
                sleeps: AtomicUsize::new(0),
            };
            let error = run_ws_api_session(
                &mut socket,
                &mut command_rx,
                &health,
                &limiter,
                &store,
                &scope,
                "cred-rotation",
                &clock,
            )
            .await
            .expect_err("23h must rotate the session");
            assert!(error.to_string().contains("proactive 23h reconnect"));
            assert_eq!(
                clock.sleeps.load(Ordering::SeqCst),
                1,
                "one session emits one reconnect signal"
            );
            cleanup(store, path).await;
        })
        .await
        .expect("I5 rotation must not hang");
    }

    #[tokio::test]
    async fn t1_only_loopback_ws_path_executes_or_skips_only_on_sandbox_eperm() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                Ok(listener) => listener,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    assert_ne!(
                        std::env::var("RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS")
                            .ok()
                            .as_deref(),
                        Some("1"),
                        "loopback EPERM is forbidden when local socket tests are required"
                    );
                    eprintln!("SKIP: sandbox denied loopback bind with EPERM");
                    return;
                }
                Err(error) => panic!("loopback bind failed for a non-EPERM reason: {error}"),
            };
            let address = listener.local_addr().expect("loopback address");
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.expect("accept loopback");
                let mut ws = tokio_tungstenite::accept_async(tcp)
                    .await
                    .expect("accept websocket");
                let request = ws
                    .next()
                    .await
                    .expect("request frame")
                    .expect("request frame ok");
                let (id, method) = request_id_and_method(request);
                assert_eq!(method, "v2/account.balance");
                ws.send(success(&id, serde_json::json!([{"asset":"USDT"}]), 5, 2400))
                    .await
                    .expect("send response");
            });
            let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
                .await
                .expect("connect loopback websocket");
            let (store, path) = test_store("loopback").await;
            let scope = format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new());
            let limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(100.0, 100.0));
            let (commands, mut command_rx) = mpsc::channel(2);
            let health = Arc::new(Mutex::new(WsApiHealth {
                connected: true,
                connected_at_ms: Some(crate::svc::now_ms()),
                ..WsApiHealth::default()
            }));
            let client = BinanceWsApiClient::with_channel_for_test(
                BinanceKeys {
                    api_key: "key".into(),
                    api_secret: "secret".into(),
                },
                "cred-loopback",
                store.clone(),
                scope.clone(),
                limiter.clone(),
                commands,
                health.clone(),
                Arc::new(SystemClock),
            );
            let session_store = store.clone();
            let session_scope = scope.clone();
            let session = tokio::spawn(async move {
                run_ws_api_session(
                    &mut socket,
                    &mut command_rx,
                    &health,
                    &limiter,
                    &session_store,
                    &session_scope,
                    "cred-loopback",
                    &SystemClock,
                )
                .await
            });
            let result = client
                .request("v2/account.balance", Vec::new())
                .await
                .expect("loopback request");
            assert_eq!(result[0]["asset"], "USDT");
            server.await.expect("server join");
            session.abort();
            cleanup(store, path).await;
        })
        .await
        .expect("T1 must not hang");
    }

    #[tokio::test]
    async fn i8_two_concurrent_requests_pair_out_of_order_responses_by_ulid() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut fixture = session_fixture("out-of-order").await;
            let client = fixture.client.clone();
            let reads = tokio::spawn(async move { client.account_reads(Some("BTCUSDT")).await });
            let first =
                request_id_and_method(fixture.outgoing.recv().await.expect("first request"));
            let second =
                request_id_and_method(fixture.outgoing.recv().await.expect("second request"));
            let by_method = [
                (first.1.clone(), first.0.clone()),
                (second.1.clone(), second.0.clone()),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();
            let position_id = by_method["account.position"].clone();
            let balance_id = by_method["v2/account.balance"].clone();
            fixture
                .incoming
                .send(Ok(success(
                    &position_id,
                    serde_json::json!([{"symbol":"BTCUSDT"}]),
                    100,
                    100,
                )))
                .expect("position response");
            fixture
                .incoming
                .send(Ok(success(
                    &balance_id,
                    serde_json::json!([{"asset":"USDT"}]),
                    95,
                    100,
                )))
                .expect("balance response");
            let (positions, balances) = reads.await.expect("join").expect("reads");
            assert_eq!(positions[0]["symbol"], "BTCUSDT");
            assert_eq!(balances[0]["asset"], "USDT");
            let diagnostics = crate::round2_diagnostics::read(
                &fixture.store,
                crate::proto_v2::GetRuntimeDiagnosticsRequest {
                    credential_id: "paper".into(),
                    schema_version: 1,
                },
            )
            .await
            .unwrap();
            let metric = diagnostics
                .websocket_scopes
                .iter()
                .find(|m| m.scope == "TRADE_WS_API")
                .unwrap();
            assert_eq!(metric.status, "OBSERVED");
            assert_eq!(
                metric.reconnect_count,
                Some(1),
                "the real connection loop must persist its reconnect"
            );
            let now_ms = crate::svc::now_ms();
            let observed = fixture
                .limiter
                .observed_headroom_for_test(&fixture.scope, now_ms)
                .expect("ws meter observation");
            assert_eq!((observed.last_used, observed.budget_1m), (100, 100));
            assert_eq!(fixture.client.health().await.used_weight_1m, Some(100));
            fixture.task.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("I8 must not hang");
    }

    #[tokio::test]
    async fn i2_i7_ws_meter_and_tokens_never_mutate_the_rest_scope() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut fixture = session_fixture("scope-isolation").await;
            let rest_scope = format!("binance-futures-rest:venue:testnet:{}", ulid::Ulid::new());
            fixture.limiter.set_stored_tokens(&rest_scope, 77.0).await;
            let client = fixture.client.clone();
            let request =
                tokio::spawn(async move { client.request("v2/account.balance", Vec::new()).await });
            let (id, _) = request_id_and_method(fixture.outgoing.recv().await.expect("request"));
            fixture
                .incoming
                .send(Ok(success(&id, serde_json::json!([]), 98, 100)))
                .expect("response");
            request.await.expect("join").expect("success");
            assert_eq!(fixture.limiter.stored_tokens(&rest_scope).await, Some(77.0));
            let ws_remaining = fixture
                .limiter
                .stored_tokens(&fixture.scope)
                .await
                .expect("ws bucket");
            assert!(ws_remaining < 100.0, "ws request must spend only ws tokens");
            let health = fixture.client.health().await;
            assert_eq!(
                (health.used_weight_1m, health.limit_1m),
                (Some(98), Some(100))
            );
            let observed_at = crate::svc::now_ms();
            let headroom = fixture
                .limiter
                .observed_headroom_for_test(&fixture.scope, observed_at)
                .expect("response limit must configure ws meter");
            assert_eq!((headroom.last_used, headroom.budget_1m), (98, 100));
            assert_eq!(headroom.remaining, 2);
            assert!(
                fixture
                    .limiter
                    .observed_headroom_for_test(&rest_scope, observed_at)
                    .is_none(),
                "a ws response must not create a REST meter row"
            );
            fixture.task.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("I2/I7 must not hang");
    }

    #[tokio::test]
    async fn i9_429_minus_1003_and_418_persist_only_the_ws_scope() {
        tokio::time::timeout(Duration::from_secs(30), async {
            for (status, code, table) in [
                (429_u16, None, "binance_rate_limit_cooldowns"),
                (400_u16, Some(-1003_i64), "binance_rate_limit_cooldowns"),
                (418_u16, None, "binance_ip_ban_cooldowns"),
            ] {
                let mut fixture = session_fixture("cooldowns").await;
                let rest_scope =
                    format!("binance-futures-rest:venue:testnet:{}", ulid::Ulid::new());
                let client = fixture.client.clone();
                let request =
                    tokio::spawn(
                        async move { client.request("v2/account.balance", Vec::new()).await },
                    );
                let (id, _) =
                    request_id_and_method(fixture.outgoing.recv().await.expect("request"));
                fixture
                    .incoming
                    .send(Ok(Message::Text(
                        serde_json::json!({
                            "id": id, "status": status, "code": code, "msg": "limited"
                        })
                        .to_string(),
                    )))
                    .expect("limited response");
                assert!(request.await.expect("join").is_err());
                let sql = format!("SELECT egress_scope FROM {table} ORDER BY egress_scope");
                let rows = sqlx::query(&sql)
                    .fetch_all(fixture.store.pool())
                    .await
                    .expect("cooldown rows");
                assert!(rows
                    .iter()
                    .any(|row| row.get::<String, _>("egress_scope") == fixture.scope));
                assert!(!rows
                    .iter()
                    .any(|row| row.get::<String, _>("egress_scope") == rest_scope));
                fixture.task.abort();
                cleanup(fixture.store, fixture.path).await;
            }
        })
        .await
        .expect("I9 must not hang");
    }

    // ---- A2c-R1 D6：WS 后台 worker 的生命周期边界 ----
    //
    // 下面几条都从**生产 worker 入口** `run_connection_loop` 起跑（`spawn` 托给
    // tokio 的就是它），只把端点指向本地回环、时钟换成固定假时钟。没有替身
    // bool、没有源码文本断言。

    /// 后台 worker 用的固定假时钟：重连退避被压缩成一个有界的固定实时延迟
    /// （既不忙等也不真等 4s+），而 23h 轮换定时器默认永不到期；只有测试显式
    /// 打开 `rotate` 时长睡眠才立刻返回并把逻辑时间推进整整 23h。
    struct WorkerClock {
        now_ms: AtomicI64,
        sleeps: AtomicUsize,
        rotate: AtomicBool,
    }

    const WORKER_CLOCK_LONG_SLEEP_MS: u128 = 3_600_000;
    const WORKER_BACKOFF_REAL_MS: u64 = 20;

    impl WorkerClock {
        fn new() -> Self {
            Self {
                now_ms: AtomicI64::new(1_725_000_000_000),
                sleeps: AtomicUsize::new(0),
                rotate: AtomicBool::new(false),
            }
        }
    }

    impl Clock for WorkerClock {
        fn now_ms(&self) -> i64 {
            self.now_ms.load(Ordering::SeqCst)
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.sleeps.fetch_add(1, Ordering::SeqCst);
                if duration.as_millis() >= WORKER_CLOCK_LONG_SLEEP_MS
                    && !self.rotate.load(Ordering::SeqCst)
                {
                    std::future::pending::<()>().await;
                }
                self.now_ms.fetch_add(
                    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                    Ordering::SeqCst,
                );
                tokio::time::sleep(Duration::from_millis(WORKER_BACKOFF_REAL_MS)).await;
            })
        }
    }

    struct WorkerFixture {
        commands: Option<mpsc::Sender<WsCommand>>,
        worker: tokio::task::JoinHandle<()>,
        health: Arc<Mutex<WsApiHealth>>,
        clock: Arc<WorkerClock>,
        store: Store,
        path: std::path::PathBuf,
    }

    async fn worker_fixture(
        label: &str,
        endpoint: String,
        limiter: CredentialRateLimiter,
        scope: String,
    ) -> WorkerFixture {
        let (store, path) = test_store(label).await;
        let health = Arc::new(Mutex::new(WsApiHealth::default()));
        let clock = Arc::new(WorkerClock::new());
        let (commands, receiver) = mpsc::channel::<WsCommand>(8);
        let worker = tokio::spawn(run_connection_loop(
            endpoint,
            BinanceKeys {
                api_key: "key".into(),
                api_secret: "secret".into(),
            },
            // 模拟凭据：venue 为 None、直连传输、connector 为 None。生产的
            // 传输选择照常执行，只是这条连接落在本地回环上，不触外网。
            "paper".to_string(),
            store.clone(),
            scope,
            limiter,
            health.clone(),
            receiver,
            clock.clone(),
        ));
        WorkerFixture {
            commands: Some(commands),
            worker,
            health,
            clock,
            store,
            path,
        }
    }

    fn ws_test_limiter() -> (CredentialRateLimiter, String) {
        (
            CredentialRateLimiter::new(TokenBucketConfig::for_test(10_000.0, 10_000.0)),
            format!("binance-futures-ws:venue:testnet:{}", ulid::Ulid::new()),
        )
    }

    async fn loopback_listener_or_skip() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                assert_ne!(
                    std::env::var("RUSTCORE_REQUIRE_LOCAL_SOCKET_TESTS")
                        .ok()
                        .as_deref(),
                    Some("1"),
                    "loopback EPERM is forbidden when local socket tests are required"
                );
                eprintln!("SKIP: sandbox denied loopback bind with EPERM");
                None
            }
            Err(error) => panic!("loopback bind failed for a non-EPERM reason: {error}"),
        }
    }

    async fn wait_for_at_least(counter: &AtomicUsize, target: usize, label: &str) {
        for _ in 0..600 {
            if counter.load(Ordering::SeqCst) >= target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 标准比较断言（而非裸 panic）：固定的变异 runner 只认
        // assert_eq!/标准 assertion failed 诊断，这里保留同样的计数/目标信息。
        assert_eq!(
            counter.load(Ordering::SeqCst) >= target,
            true,
            "{label}: stalled at {} of {target}",
            counter.load(Ordering::SeqCst)
        );
    }

    async fn wait_for_connected(health: &Arc<Mutex<WsApiHealth>>, label: &str) {
        for _ in 0..600 {
            if health.lock().await.connected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{label}: the ws worker never reported a connected session");
    }

    async fn assert_worker_stopped(fixture: &mut WorkerFixture, message: String) {
        let stopped = tokio::time::timeout(Duration::from_secs(5), &mut fixture.worker).await;
        // 标准比较断言，让固定变异 runner 能把超时分支归类为已证实失败。
        assert_eq!(stopped.is_ok(), true, "{message}");
        stopped
            .expect("bounded worker exit")
            .expect("the ws worker must exit without panicking");
    }

    #[tokio::test]
    async fn ws1_connected_worker_stops_after_its_last_request_owner_disappears() {
        // 本用例调用完整生产 worker，会读进程环境；环境串行锁在超时窗口外获取并
        // 持有到用例结束，排队等其他用例放锁不消耗 60s 截止时间。
        let _env_serial = crate::binance_futures::tests::env_serial_guard();
        tokio::time::timeout(Duration::from_secs(60), async {
            let Some(listener) = loopback_listener_or_skip().await else {
                return;
            };
            let address = listener.local_addr().expect("loopback address");
            let handshakes = Arc::new(AtomicUsize::new(0));
            let server_handshakes = handshakes.clone();
            let server = tokio::spawn(async move {
                let mut held = Vec::new();
                loop {
                    let (tcp, _) = listener.accept().await.expect("accept loopback");
                    let socket = tokio_tungstenite::accept_async(tcp)
                        .await
                        .expect("accept websocket");
                    server_handshakes.fetch_add(1, Ordering::SeqCst);
                    // 只保留最近几条连接，避免基线红态的重连风暴耗尽 fd。
                    if held.len() >= 8 {
                        held.remove(0);
                    }
                    held.push(socket);
                }
            });
            let (limiter, scope) = ws_test_limiter();
            limiter.set_stored_tokens(&scope, 10_000.0).await;
            let mut fixture =
                worker_fixture("ws1-owner-gone", format!("ws://{address}"), limiter, scope).await;
            wait_for_at_least(&handshakes, 1, "the worker must open its first ws session").await;
            wait_for_connected(&fixture.health, "ws1").await;

            // 最后一个能发请求的 owner 消失。
            drop(fixture.commands.take().expect("the only request owner"));

            assert_worker_stopped(
                &mut fixture,
                format!(
                    "an already connected ws worker must exit once its last request owner is \
                     gone; it was still running after {} handshakes",
                    handshakes.load(Ordering::SeqCst)
                ),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(
                handshakes.load(Ordering::SeqCst),
                1,
                "no ws handshake may follow the last owner's disappearance"
            );
            assert!(
                !fixture.health.lock().await.connected,
                "a stopped ws worker must not still report a connected session"
            );
            server.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("WS1 must not hang");
    }

    #[tokio::test]
    async fn ws2_connect_failure_backoff_stops_dialing_after_its_owner_disappears() {
        // 同 ws1：锁在超时窗口外获取，排队放锁不占 60s 预算。
        let _env_serial = crate::binance_futures::tests::env_serial_guard();
        tokio::time::timeout(Duration::from_secs(60), async {
            let Some(listener) = loopback_listener_or_skip().await else {
                return;
            };
            let address = listener.local_addr().expect("loopback address");
            let dials = Arc::new(AtomicUsize::new(0));
            let server_dials = dials.clone();
            // 接了就断，不做 WebSocket 握手：worker 停在「连接失败 → 退避」这一支。
            let server = tokio::spawn(async move {
                loop {
                    let (tcp, _) = listener.accept().await.expect("accept loopback");
                    server_dials.fetch_add(1, Ordering::SeqCst);
                    drop(tcp);
                }
            });
            let (limiter, scope) = ws_test_limiter();
            limiter.set_stored_tokens(&scope, 10_000.0).await;
            let mut fixture = worker_fixture(
                "ws2-connect-failure",
                format!("ws://{address}"),
                limiter,
                scope,
            )
            .await;
            wait_for_at_least(
                &dials,
                2,
                "a failing endpoint must still be retried while an owner exists",
            )
            .await;
            assert!(
                fixture.health.lock().await.last_failure.is_some(),
                "a failed ws connect must be recorded before the backoff"
            );

            let before = dials.load(Ordering::SeqCst);
            drop(fixture.commands.take().expect("the only request owner"));

            assert_worker_stopped(
                &mut fixture,
                format!(
                    "a ws worker in connect-failure backoff must exit once its last request \
                     owner is gone; dials went {before} → {}",
                    dials.load(Ordering::SeqCst)
                ),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let after = dials.load(Ordering::SeqCst);
            assert!(
                after <= before + 1,
                "only the dial already in flight may finish after the owner left: {before} → {after}"
            );
            server.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("WS2 must not hang");
    }

    #[tokio::test]
    async fn ws3_handshake_admission_backoff_exits_without_dialing_once_owners_are_gone() {
        // 同 ws1：锁在超时窗口外获取，排队放锁不占 60s 预算。
        let _env_serial = crate::binance_futures::tests::env_serial_guard();
        tokio::time::timeout(Duration::from_secs(60), async {
            let (limiter, scope) = ws_test_limiter();
            // Keep admission's minute window fixed while the SQLite fixture starts.
            // The reconnect clock advances independently; a wall-clock rollover must
            // not expire the hard-stop precondition before the worker is polled.
            let admission_clock = Arc::new(JumpClock {
                now_ms: AtomicI64::new(1_725_000_059_999),
                sleeps: AtomicUsize::new(0),
            });
            let limiter = limiter.with_clock(admission_clock.clone());
            limiter.observe_egress_weight_with_limit(
                &scope,
                1_000,
                1_000,
                admission_clock.now_ms(),
            );
            // 端口 1 只是一个绝不会被拨的目标：本用例期望一次连接都不发起。
            let mut fixture = worker_fixture(
                "ws3-admission-backoff",
                "ws://127.0.0.1:1".to_string(),
                limiter,
                scope,
            )
            .await;
            for _ in 0..600 {
                let parked = fixture
                    .health
                    .lock()
                    .await
                    .last_failure
                    .as_deref()
                    .is_some_and(|failure| failure.contains("handshake admission rejected"));
                if parked {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let failure = fixture.health.lock().await.last_failure.clone();
            assert!(
                failure
                    .as_deref()
                    .is_some_and(|failure| failure.contains("handshake admission rejected")),
                "expected the pre-connect admission backoff, got {failure:?}"
            );
            assert!(
                fixture.clock.sleeps.load(Ordering::SeqCst) >= 1,
                "a rejected handshake must back off before retrying"
            );

            drop(fixture.commands.take().expect("the only request owner"));

            assert_worker_stopped(
                &mut fixture,
                "a ws worker parked in handshake-admission backoff must exit once its last \
                 request owner is gone"
                    .to_string(),
            )
            .await;
            assert!(!fixture.health.lock().await.connected);
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("WS3 must not hang");
    }

    #[tokio::test]
    async fn ws4_live_owner_keeps_its_reconnect_and_is_still_served() {
        // 同 ws1：锁在超时窗口外获取，排队放锁不占 60s 预算。
        let _env_serial = crate::binance_futures::tests::env_serial_guard();
        tokio::time::timeout(Duration::from_secs(60), async {
            let Some(listener) = loopback_listener_or_skip().await else {
                return;
            };
            let address = listener.local_addr().expect("loopback address");
            let handshakes = Arc::new(AtomicUsize::new(0));
            let server_handshakes = handshakes.clone();
            let server = tokio::spawn(async move {
                let mut held = Vec::new();
                let mut index = 0_usize;
                loop {
                    let (tcp, _) = listener.accept().await.expect("accept loopback");
                    let mut socket = tokio_tungstenite::accept_async(tcp)
                        .await
                        .expect("accept websocket");
                    index += 1;
                    server_handshakes.fetch_add(1, Ordering::SeqCst);
                    if index == 1 {
                        // 服务端主动断开第一条连接。
                        drop(socket);
                        continue;
                    }
                    if index == 2 {
                        let request = socket
                            .next()
                            .await
                            .expect("request frame")
                            .expect("request frame ok");
                        let (id, method) = request_id_and_method(request);
                        assert_eq!(method, "v2/account.balance");
                        socket
                            .send(success(&id, serde_json::json!([{"asset":"USDT"}]), 5, 2400))
                            .await
                            .expect("send response");
                    }
                    held.push(socket);
                }
            });
            let (limiter, scope) = ws_test_limiter();
            limiter.set_stored_tokens(&scope, 10_000.0).await;
            let mut fixture = worker_fixture(
                "ws4-live-owner",
                format!("ws://{address}"),
                limiter.clone(),
                scope.clone(),
            )
            .await;
            wait_for_at_least(
                &handshakes,
                2,
                "a live owner must get a reconnect after a server-side close",
            )
            .await;
            wait_for_connected(&fixture.health, "ws4").await;

            let client = BinanceWsApiClient::with_channel_for_test(
                BinanceKeys {
                    api_key: "key".into(),
                    api_secret: "secret".into(),
                },
                "paper",
                fixture.store.clone(),
                scope,
                limiter,
                fixture.commands.clone().expect("the live request owner"),
                fixture.health.clone(),
                Arc::new(SystemClock),
            );
            let balances = client
                .request("v2/account.balance", Vec::new())
                .await
                .expect("the reconnected session must still serve its owner");
            assert_eq!(balances[0]["asset"], "USDT");
            assert!(
                !fixture.worker.is_finished(),
                "the ws worker must keep running while a request owner still exists"
            );
            drop(client);
            drop(fixture.commands.take());
            server.abort();
            fixture.worker.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("WS4 must not hang");
    }

    #[tokio::test]
    async fn ws5_proactive_rotation_reconnects_for_a_live_owner_then_stops_without_one() {
        // 同 ws1：锁在超时窗口外获取，排队放锁不占 60s 预算。
        let _env_serial = crate::binance_futures::tests::env_serial_guard();
        tokio::time::timeout(Duration::from_secs(60), async {
            let Some(listener) = loopback_listener_or_skip().await else {
                return;
            };
            let address = listener.local_addr().expect("loopback address");
            let handshakes = Arc::new(AtomicUsize::new(0));
            let server_handshakes = handshakes.clone();
            let server = tokio::spawn(async move {
                let mut held = Vec::new();
                loop {
                    let (tcp, _) = listener.accept().await.expect("accept loopback");
                    let socket = tokio_tungstenite::accept_async(tcp)
                        .await
                        .expect("accept websocket");
                    server_handshakes.fetch_add(1, Ordering::SeqCst);
                    // 只保留最近几条连接，避免基线红态的重连风暴耗尽 fd。
                    if held.len() >= 8 {
                        held.remove(0);
                    }
                    held.push(socket);
                }
            });
            let (limiter, scope) = ws_test_limiter();
            limiter.set_stored_tokens(&scope, 10_000.0).await;
            let mut fixture =
                worker_fixture("ws5-rotation", format!("ws://{address}"), limiter, scope).await;
            // 让 23h 主动轮换立刻到期：每条会话一连上就轮换。
            fixture.clock.rotate.store(true, Ordering::SeqCst);
            wait_for_at_least(
                &handshakes,
                3,
                "proactive rotation must keep reconnecting while an owner exists",
            )
            .await;

            let before = handshakes.load(Ordering::SeqCst);
            drop(fixture.commands.take().expect("the only request owner"));

            assert_worker_stopped(
                &mut fixture,
                format!(
                    "rotation must not keep a ws worker alive after its last request owner \
                     left; handshakes went {before} → {}",
                    handshakes.load(Ordering::SeqCst)
                ),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let after = handshakes.load(Ordering::SeqCst);
            assert!(
                after <= before + 1,
                "only the rotation already in flight may finish after the owner left: \
                 {before} → {after}"
            );
            server.abort();
            cleanup(fixture.store, fixture.path).await;
        })
        .await
        .expect("WS5 must not hang");
    }

    #[test]
    fn i6_source_shape_forbids_ws_order_methods_and_keeps_kill_switch_unwired() {
        let source = include_str!("binance_ws_api.rs");
        for forbidden in [
            concat!("order", ".place"),
            concat!("order", ".modify"),
            concat!("order", ".cancel"),
            concat!("algo", "Order."),
        ] {
            assert!(
                !source.contains(forbidden),
                "forbidden ws method: {forbidden}"
            );
        }
        let kill_switch = include_str!("kill_switch.rs");
        assert!(!kill_switch.contains("ws-fapi"));
        assert!(!kill_switch.contains("WsApi"));
    }
}
