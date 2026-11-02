//! Real loopback HTTP at the production client's last send, with RPC arming.
use super::*;
use crate::{
    lifecycle_fence_tests::{arm_request, Fixture, ARM},
    proto_v2 as p,
};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Http {
    url: String,
    calls: Arc<Mutex<Vec<(String, String, bool)>>>,
    arrived: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Http {
    async fn new(hold_entry: bool, retry_first: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let log = calls.clone();
        let arrived = Arc::new(tokio::sync::Notify::new());
        let seen = arrived.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let resume = release.clone();
        let posts = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let log = log.clone();
                let seen = seen.clone();
                let resume = resume.clone();
                let posts = posts.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut buf = [0; 4096];
                    loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buf[..n]);
                        if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let raw = String::from_utf8(bytes).unwrap();
                    let line = raw.lines().next().unwrap();
                    let mut parts = line.split_whitespace();
                    let method = parts.next().unwrap();
                    let u = url::Url::parse(&format!("http://loopback{}", parts.next().unwrap()))
                        .unwrap();
                    let q = u.query_pairs().collect::<HashMap<_, _>>();
                    let reduce = q.get("reduceOnly").is_some_and(|v| v == "true");
                    log.lock()
                        .unwrap()
                        .push((method.into(), u.path().into(), reduce));
                    let entry = method == "POST" && u.path() == "/fapi/v1/order" && !reduce;
                    let n = if entry {
                        posts.fetch_add(1, Ordering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    seen.notify_one();
                    if entry && hold_entry {
                        resume.notified().await;
                    }
                    let retry = entry && retry_first && n == 0;
                    let (status, extra, body) = if retry {
                        (
                            429,
                            "Retry-After: 0\r\n",
                            serde_json::json!({"code":-1003}).to_string(),
                        )
                    } else if method == "POST" {
                        (200,"",serde_json::json!({"orderId":891,"clientOrderId":q.get("newClientOrderId").map(|v|v.as_ref()).unwrap_or("exit"),"status":"NEW","symbol":"BTCUSDT","side":q.get("side").map(|v|v.as_ref()).unwrap_or("SELL"),"origQty":"0.1","executedQty":"0"}).to_string())
                    } else {
                        (200, "", "[]".into())
                    };
                    let msg=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                    let _ = socket.write_all(msg.as_bytes()).await;
                });
            }
        });
        Self {
            url,
            calls,
            arrived,
            release,
            task,
        }
    }
    fn client(&self, store: &Store, credential: &str) -> BinanceFuturesClient {
        let mut c = BinanceFuturesClient::new_with_mock_responses(credential, vec![]);
        c.base_url = self.url.clone();
        c.mock_http = None;
        c.cooldown_store = Some(store.clone());
        c.egress_scope = format!("b8-loopback-{}", ulid::Ulid::new());
        c.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        c
    }
    async fn seen(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.arrived.notified())
            .await
            .expect("actual HTTP arrival");
    }
}
impl Drop for Http {
    fn drop(&mut self) {
        self.release.notify_waiters();
        self.task.abort();
    }
}

#[tokio::test]
async fn lifecycle_last_send_after_egress_queue_blocks_old_admitted_entry_and_preserves_exit_reads()
{
    let f = Fixture::new().await;
    let http = Http::new(false, false).await;
    let c = http.client(&f.store, "601");
    let held = transport_gate_for_scope(c.egress_scope())
        .lock_owned()
        .await;
    let waiting = Arc::new(tokio::sync::Notify::new());
    let observed = waiting.clone();
    let mut entry = c.clone();
    entry.gate_wait_observer = Some(Arc::new(move || observed.notify_one()));
    let task = tokio::spawn(async move {
        entry
            .place_market_order("BTCUSDT", "BUY", "0.1", false, "queued")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), waiting.notified())
        .await
        .unwrap();
    let r: p::LifecycleFenceResponse = f
        .rpc(ARM, arm_request(&f.store, "601", "queue_account"))
        .await
        .unwrap();
    assert!(r.current_observation.unwrap().admission_blocked);
    drop(held);
    assert!(task
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains(crate::lifecycle_fence::DENIED));
    assert!(http.calls.lock().unwrap().is_empty());
    c.place_market_order("BTCUSDT", "SELL", "0.1", true, "safe_exit")
        .await
        .unwrap();
    c.signed_request_with_retry(
        "GET",
        "/fapi/v1/openOrders",
        &[],
        RetryClass::ReadOnly,
        RetryScope::Normal,
        RequestClass::PeriodicReconcile,
    )
    .await
    .unwrap();
    let calls = http.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], ("POST".into(), "/fapi/v1/order".into(), true));
    assert_eq!(calls[1].0, "GET");
    f.cleanup().await;
}

#[tokio::test]
async fn lifecycle_arm_waits_for_real_send_headers_not_only_prior_authorization() {
    let f = Arc::new(Fixture::new().await);
    let http = Http::new(true, false).await;
    let c = http.client(&f.store, "602");
    let send = tokio::spawn(async move {
        c.place_limit_order("BTCUSDT", "BUY", "0.1", "100", false, "already_sending")
            .await
    });
    http.seen().await;
    let reader = f.clone();
    let req = arm_request(&f.store, "602", "sending_account");
    let mut arm =
        tokio::spawn(async move { reader.rpc::<_, p::LifecycleFenceResponse>(ARM, req).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(80), &mut arm)
            .await
            .is_err(),
        "arm must wait until actual headers"
    );
    assert_eq!(
        sqlx::query("SELECT COUNT(*) AS n FROM lifecycle_fence_requests")
            .fetch_one(f.store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n"),
        0
    );
    http.release.notify_one();
    assert!(send.await.unwrap().is_ok());
    let armed = arm.await.unwrap().unwrap();
    assert!(armed.found);
    assert_eq!(http.calls.lock().unwrap().len(), 1);
    Arc::try_unwrap(f).ok().unwrap().cleanup().await;
}
struct RetryClock {
    now: AtomicI64,
    sleeping: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl Clock for RetryClock {
    fn now_ms(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.sleeping.notify_one();
            self.resume.notified().await;
            self.now.fetch_add(6000, Ordering::SeqCst);
        })
    }
}
#[tokio::test]
async fn lifecycle_each_retry_rechecks_durable_fence_after_first_real_429() {
    let f = Fixture::new().await;
    let http = Http::new(false, true).await;
    let clock = Arc::new(RetryClock {
        now: AtomicI64::new(crate::svc::now_ms()),
        sleeping: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let mut c = http.client(&f.store, "603");
    c.clock = clock.clone();
    c.request_limiter = c.request_limiter.with_clock(clock.clone());
    let send = tokio::spawn(async move {
        c.place_market_order("BTCUSDT", "BUY", "0.1", false, "retry_entry")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), clock.sleeping.notified())
        .await
        .expect("first response settled; retry is sleeping");
    let _: p::LifecycleFenceResponse = f
        .rpc(ARM, arm_request(&f.store, "603", "retry_account"))
        .await
        .unwrap();
    clock.resume.notify_one();
    assert!(send
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains(crate::lifecycle_fence::DENIED));
    assert_eq!(
        http.calls.lock().unwrap().len(),
        1,
        "no second physical POST after Arm"
    );
    f.cleanup().await;
}

fn entry_request(credential: &str, label: &str) -> p::ExecuteIntentsRequest {
    let now = crate::svc::now_ms();
    p::ExecuteIntentsRequest {
        credential_id: credential.into(),
        run_mode: "demo".into(),
        schema_version: 1,
        data_snapshot: Some(p::DataSnapshotDecision {
            snapshot_id: format!("snap_{}", "a".repeat(32)),
            quality_gate: "pass".into(),
            allow_open: true,
            allow_close: true,
            created_at_ms: now,
            schema_version: 1,
        }),
        intents: vec![p::OrderIntent {
            bot_uid: format!("bot_{label}"),
            run_id: "b8_run".into(),
            cycle_id: label.into(),
            idempotency_key: format!("b8_{label}"),
            client_order_id_short: format!("b8co_{label}"),
            exchange: "binance".into(),
            market_kind: "futures".into(),
            symbol_key: crate::position_close_tests::BTC.into(),
            action: "open_long".into(),
            order_type: "limit".into(),
            limit_price: "100".into(),
            reduce_only: false,
            intent_id: format!("int_{}", ulid::Ulid::new()),
            max_slippage_bps: 15,
            protection: Some(p::Protection {
                required: false,
                stop_loss_price: "95".into(),
                take_profit_price: "110".into(),
                confirm_timeout_ms: 3000,
                schema_version: 1,
            }),
            policy_version: "risk_policy_v3".into(),
            valid_until_ms: now + 30000,
            config_hash: "c".repeat(64),
            prompt_hash: format!("sha256:{}", "b".repeat(64)),
            code_commit: "a".repeat(40),
            schema_version: 1,
        }],
    }
}
#[tokio::test]
async fn lifecycle_real_authenticated_v2_oms_old_verdict_cannot_outbound_after_arm() {
    let _serial = super::tests::env_serial_guard();
    let credential = "604";
    let (key, secret) = key_env_names(credential);
    let _env = [
        super::tests::TestEnvVar::set(key, "b8-local-604-key"),
        super::tests::TestEnvVar::set(secret, "b8-local-604-secret"),
        super::tests::TestEnvVar::set(crate::venue::venue_env_name(credential), "testnet"),
        super::tests::TestEnvVar::set("ENGINE_STAGE_MAX".into(), "demo"),
    ];
    let f = Fixture::new().await;
    let http = Http::new(false, false).await;
    let mut c = http.client(&f.store, credential);
    sqlx::query("INSERT INTO engine_balances(credential_id,exchange,market_kind,asset,wallet_balance,available_balance,updated_at_ms) VALUES(?,'binance','futures','USDT','10000','10000',1)").bind(credential).execute(f.store.pool()).await.unwrap();
    sqlx::query("INSERT INTO risk_budget(credential_id,as_of_ms,equity_usdt,available_usdt,today_realized_pnl_usdt,today_realized_pnl_pct,drawdown_pct,total_exposure_pct,open_positions_count,group_exposure_json,kill_switch_level,remaining_credential_notional_usdt) VALUES(?,?,'10000','10000','0',0,0,0,0,'{\"majors\":0,\"l1_alts\":0}','none','5000')").bind(credential).bind(crate::svc::now_ms()).execute(f.store.pool()).await.unwrap();
    let cycle = crate::fill_replay::begin_recovery_cycle(
        &f.store,
        credential,
        crate::fill_replay::RecoveryTrigger::Startup,
        10,
    )
    .await
    .unwrap();
    crate::fill_replay::finalize_recovery_readiness(
        &f.store,
        credential,
        crate::fill_replay::RecoveryPhaseResult::Succeeded,
        crate::fill_replay::RecoveryPhaseResult::Succeeded,
        crate::fill_replay::RecoveryPhaseResult::Succeeded,
        cycle,
        20,
    )
    .await
    .unwrap();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let observed = waiting.clone();
    c.gate_wait_observer = Some(Arc::new(move || observed.notify_one()));
    let gate = transport_gate_for_scope(c.egress_scope());
    let core = crate::svc::EngineExecutionService::new(f.store.clone())
        .without_background_exchange_tasks()
        .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
        .with_submit_client_override(c);
    let f = Arc::new(f.replace_execution(core).await);
    // Positive authenticated production v2 -> OMS -> real HTTP first.
    let positive: p::ExecuteIntentsResponse = f
        .rpc(
            "/execution_engine.v2.EngineExecution/ExecuteIntents",
            entry_request(credential, "positive"),
        )
        .await
        .unwrap();
    assert_eq!(
        positive.acks[0].status,
        p::Status::Accepted as i32,
        "{:?}",
        positive.acks
    );
    assert_eq!(http.calls.lock().unwrap().len(), 1);
    // Consume the positive call's queue notification, then queue a different bot.
    waiting.notified().await;
    let held = gate.lock_owned().await;
    let executor = f.clone();
    let pending = tokio::spawn(async move {
        executor
            .rpc::<_, p::ExecuteIntentsResponse>(
                "/execution_engine.v2.EngineExecution/ExecuteIntents",
                entry_request(credential, "old_verdict"),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), waiting.notified())
        .await
        .expect("old accepted OMS row reached actual egress queue");
    let rows = sqlx::query(
        "SELECT status FROM engine_orders WHERE credential_id=? AND bot_uid='bot_old_verdict'",
    )
    .bind(credential)
    .fetch_all(f.store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("status"), "pending_submit");
    let _: p::LifecycleFenceResponse = f
        .rpc(ARM, arm_request(&f.store, credential, "oms_account"))
        .await
        .unwrap();
    drop(held);
    let result = pending.await.unwrap().unwrap();
    assert_ne!(result.acks[0].status, p::Status::Accepted as i32);
    assert!(
        result.acks[0]
            .message
            .contains(crate::lifecycle_fence::DENIED),
        "{:?}",
        result.acks
    );
    assert_eq!(
        http.calls.lock().unwrap().len(),
        1,
        "arm blocked old authorized Entry at last send"
    );
    Arc::try_unwrap(f).ok().unwrap().cleanup().await;
}
