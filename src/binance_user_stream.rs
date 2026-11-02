use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_futures::{keys_from_env, BinanceFuturesClient, BinanceKeys};
use crate::fill_replay::{
    self, BackfillAttempt, BackfillCompletion, RecoveryPhaseResult, RecoveryStep, RecoveryTrigger,
};
use crate::rate_limiter::{CredentialRateLimiter, RequestClass};
use crate::store::Store;
use crate::svc::BinanceEventSink;

#[derive(Clone)]
pub struct BinanceUserStreamManager {
    inner: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// 只在测试里为 false。user stream 会**联真实交易所**（create listenKey +
    /// websocket），而 §4 要求交易路径测试默认 dry-run；驱动 `ExecuteIntents`
    /// 生产入口的接线用例因此需要一个不起后台网络任务的构造。
    enabled: bool,
}

impl BinanceUserStreamManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            enabled: true,
        }
    }

    /// 测试专用：不起任何 user stream 任务（**不联网**）。
    #[cfg(test)]
    pub(crate) fn disabled_for_tests() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            enabled: false,
        }
    }

    pub async fn ensure_running(
        &self,
        credential_id: &str,
        symbol: &str,
        store: Store,
        limiter: CredentialRateLimiter,
        keys: BinanceKeys,
        trigger: RecoveryTrigger,
    ) -> anyhow::Result<()> {
        crate::account_identity::diagnose(&store, Some(credential_id)).await;
        if !self.enabled {
            return Ok(());
        }
        let inserted = fill_replay::register_fill_recovery_scope(
            &store,
            credential_id,
            symbol,
            crate::svc::now_ms(),
        )
        .await?;
        #[cfg(feature = "l4-fault-injection")]
        if crate::l4_fake_exchange::configured_db()?.is_some() {
            // The socket-free L4 adapter uses synchronous REST-shaped truth;
            // it must never fall through to the production Binance websocket.
            let cycle = fill_replay::begin_recovery_cycle(
                &store,
                credential_id,
                trigger,
                crate::svc::now_ms(),
            )
            .await?;
            // ****：这条 L4 通路与
            // `run_once` 犯的是同一个错——`reconcile_binance_futures` 把真值
            // `.map(|_| ())` 丢掉，然后给 `finalize_recovery_readiness` 固定传
            // 三个 `Succeeded`。一次**跑完了、但报告本地与交易所有分歧**的
            // 对账照样把凭据写成 `ready`，而 `ready` 正是 `risk.rs` 放行新开仓
            // 的判据。判据与 WS 通路同源：`recovery_reconcile_divergence`。
            let truth = crate::reconcile::reconcile_binance_futures_truth(
                credential_id,
                store.clone(),
                limiter,
                keys,
            )
            .await
            .context("L4 fake fill recovery reconcile")?;
            let reconcile_phase = match crate::reconcile::recovery_reconcile_divergence(truth) {
                None => RecoveryPhaseResult::Succeeded,
                Some(reason) => {
                    fill_replay::record_recovery_failure(
                        &store,
                        credential_id,
                        "reconcile",
                        None,
                        None,
                        &reason,
                        crate::svc::now_ms(),
                    )
                    .await?;
                    tracing::error!(
                        credential_id,
                        failure_reason = %reason,
                        "recovery reconcile completed but reported a divergence; the fill \
                         handoff stays not-ready"
                    );
                    RecoveryPhaseResult::Failed
                }
            };
            fill_replay::finalize_recovery_readiness(
                &store,
                credential_id,
                RecoveryPhaseResult::Succeeded,
                reconcile_phase,
                RecoveryPhaseResult::Succeeded,
                cycle,
                crate::svc::now_ms(),
            )
            .await?;
            return Ok(());
        }
        let mut guard = self.inner.lock().await;
        if guard
            .get(credential_id)
            .is_some_and(|handle| !handle.is_finished())
            && !inserted
        {
            return Ok(());
        }
        if let Some(old) = guard.remove(credential_id) {
            old.abort();
        }
        let credential_id = credential_id.to_string();
        let task_credential_id = credential_id.clone();
        let handle = tokio::spawn(async move {
            let sink = BinanceEventSink::new(store.clone());
            let mut next_trigger = trigger;
            let mut short_cycles: u32 = 0;
            // 上限与对账节拍绑定，每轮重读：运维改 RECONCILE_INTERVAL_MS 不必重启。
            loop {
                let cap_ms = user_stream_retry_cap_ms();
                let cycle_started_ms = crate::svc::now_ms();
                let outcome = run_once(
                    &task_credential_id,
                    keys.clone(),
                    store.clone(),
                    sink.clone(),
                    limiter.clone(),
                    next_trigger,
                )
                .await;
                // 存活时长而非 Ok/Err 决定退避：连上就掉的流走的是 Ok 分支，
                // 而它每轮都要付一次全量回补＋全量对账。
                let cycle_ms = u64::try_from(crate::svc::now_ms().saturating_sub(cycle_started_ms))
                    .unwrap_or(0);
                short_cycles = next_short_cycle_count(short_cycles, cycle_ms);
                if let Err(e) = outcome {
                    let _ = fill_replay::record_recovery_failure(
                        &store,
                        &task_credential_id,
                        "cycle_failed",
                        None,
                        None,
                        &format!("{e:#}"),
                        crate::svc::now_ms(),
                    )
                    .await;
                    tracing::warn!(credential_id=%task_credential_id, failure_reason=%e, cycle_ms, short_cycles, "binance fill recovery and user stream cycle failed; retrying");
                } else {
                    tracing::warn!(credential_id=%task_credential_id, cycle_ms, short_cycles, "binance user stream disconnected; retrying");
                }
                let delay_ms = user_stream_retry_sleep_ms(
                    short_cycles,
                    cap_ms,
                    crate::rate_limiter::deterministic_jitter_ms(&task_credential_id, short_cycles),
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                next_trigger = RecoveryTrigger::Reconnect;
            }
        });
        guard.insert(credential_id, handle);
        Ok(())
    }

    pub async fn start_discovered(
        &self,
        store: Store,
        limiter: CredentialRateLimiter,
        reconcilers: &crate::reconcile::ReconcilerManager,
    ) -> anyhow::Result<()> {
        crate::account_identity::diagnose(&store, None).await;
        let discovered = fill_replay::discover_fill_recovery_scopes(&store).await?;
        let mut by_credential = BTreeMap::<String, Vec<String>>::new();
        for scope in discovered {
            fill_replay::register_fill_recovery_scope(
                &store,
                &scope.credential_id,
                &scope.symbol,
                crate::svc::now_ms(),
            )
            .await?;
            by_credential
                .entry(scope.credential_id)
                .or_default()
                .push(scope.symbol);
        }
        for (credential_id, symbols) in &by_credential {
            let Some(first_symbol) = symbols.first() else {
                continue;
            };
            match keys_from_env(credential_id) {
                Ok(Some(keys)) => {
                    // Persisted scopes can receive fills before the first RPC.
                    // The main execution service owns their periodic recovery;
                    // the temporary fill-protection service must not create it.
                    reconcilers
                        .ensure_running_after_stream_startup(
                            credential_id,
                            store.clone(),
                            limiter.clone(),
                            keys.clone(),
                        )
                        .await;
                    self.ensure_running(
                        credential_id,
                        first_symbol,
                        store.clone(),
                        limiter.clone(),
                        keys,
                        RecoveryTrigger::Startup,
                    )
                    .await?;
                }
                Ok(None) => {
                    let reason = "configured fill recovery scope has no Binance key pair";
                    fill_replay::record_recovery_failure(
                        &store,
                        credential_id,
                        "startup_credentials",
                        Some(first_symbol),
                        None,
                        reason,
                        crate::svc::now_ms(),
                    )
                    .await?;
                    tracing::error!(%credential_id, symbol=%first_symbol, cursor_last_trade_id=?Option::<i64>::None, failure_reason=reason, "startup fill recovery cannot start");
                }
                Err(error) => {
                    fill_replay::record_recovery_failure(
                        &store,
                        credential_id,
                        "startup_credentials",
                        Some(first_symbol),
                        None,
                        &error,
                        crate::svc::now_ms(),
                    )
                    .await?;
                    tracing::error!(%credential_id, symbol=%first_symbol, cursor_last_trade_id=?Option::<i64>::None, failure_reason=%error, "startup fill recovery cannot start");
                }
            }
        }
        for credential_id in crate::position_state::active_usdm_credentials(&store).await? {
            if by_credential.contains_key(&credential_id) {
                // Proven fill recovery retains its UserStream-owned first scan.
                continue;
            }
            let failure_reason = match keys_from_env(&credential_id) {
                Ok(Some(keys)) => {
                    // A durable position is a management obligation, not proof
                    // of fill history. Start only the periodic reconciler, whose
                    // normal admission owns an immediate first management scan.
                    reconcilers
                        .ensure_running(&credential_id, store.clone(), limiter.clone(), keys)
                        .await;
                    continue;
                }
                Ok(None) => "missing Binance key pair",
                Err(_) => "invalid Binance credential configuration",
            };
            // Persist the failure and revoke stale readiness without claiming a
            // fill scope or history cursor. Configuration values stay out of it.
            fill_replay::record_recovery_failure(
                &store,
                &credential_id,
                "startup_position_credentials",
                None,
                None,
                failure_reason,
                crate::svc::now_ms(),
            )
            .await?;
            tracing::error!(
                %credential_id,
                failure_reason,
                "startup position management cannot start"
            );
        }
        Ok(())
    }
}

/// **user-stream 两条后台任务的结构化所有权**（段级第五轮复评
/// regression，Medium）。
///
/// R8 只给 keepalive 挂了 `AbortOnDrop`：reader 是一个裸 `JoinHandle`，
/// 而 `run_once` 在 spawn reader 之后还有一长串 `?;`（backfill、cursor 读写、
/// reconcile）。任意一条提前返回都会**跳过下面所有显式的 `.abort()`**——
/// keepalive 被 guard 收掉了，reader 没有 owner：它继续读那条 socket、
/// 继续把事件喂给 sink，而外层已经在起一条新的连接了。每次重连再泄漏一个。
/// 评审对此的变异（把 `Drop` 里的 abort 删掉）3/3 存活：
/// **当时根本没有任何生命周期行为证据**。
///
/// 处置：两条 handle 由**同一个所有者**持有，`Drop` 收掉尚未被交出的那些。
/// 这不依赖任何人记得在新的 `?` 之前补一句 abort——新增提前返回路径
/// **无法**绕过它。
///
/// `join_reader` 是唯一把 reader 交出去的出口，它消费 `self`：
/// keepalive 在同一步被收掉，之后 `Drop` 里已经没有 reader 可收。
pub(crate) struct UserStreamTasks {
    keepalive: Option<tokio::task::JoinHandle<()>>,
    reader: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl UserStreamTasks {
    pub(crate) fn new(
        keepalive: tokio::task::JoinHandle<()>,
        reader: tokio::task::JoinHandle<anyhow::Result<()>>,
    ) -> Self {
        Self {
            keepalive: Some(keepalive),
            reader: Some(reader),
        }
    }

    /// reader 是否已经结束（连接已断）。**不消费所有权**。
    pub(crate) fn reader_finished(&self) -> bool {
        self.reader
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
    }

    /// Wait for either the fixed divergence-recheck deadline or the live WS
    /// reader to finish. Keeping the reader owned here means logical reconcile
    /// divergence does not tear down a healthy stream and create another
    /// backfill cycle; a real transport failure still returns immediately to
    /// the existing reconnect/backoff path.
    async fn wait_for_reader_or_delay(
        &mut self,
        delay: Duration,
    ) -> Option<Result<anyhow::Result<()>, tokio::task::JoinError>> {
        let outcome = {
            let reader = self
                .reader
                .as_mut()
                .expect("user stream reader remains owned while recovery waits");
            tokio::select! {
                result = reader => Some(result),
                _ = tokio::time::sleep(delay) => None,
            }
        };
        if outcome.is_some() {
            if let Some(keepalive) = self.keepalive.take() {
                keepalive.abort();
            }
            let _ = self.reader.take();
        }
        outcome
    }

    /// 收掉 keepalive 并 join reader。消费 `self`——调用之后这两条任务
    /// 不再有第二个所有者。
    ///
    /// **取消窗口里 reader 必须仍有
    /// owner。**
    ///
    /// R9 这里是 `self.reader.take()` 之后再 `reader.await`：handle 已经被移出
    /// `self`，`Drop` 里那句 `reader.abort()` 从此够不到它。外层 future 一旦在
    /// 这个 await 上被取消（`tokio::select!` 的另一条臂先完成、
    /// `timeout` 到期、任务被 abort），局部变量 `reader` 只是被 **drop**——
    /// 而 drop 一个 `JoinHandle` 是**分离**任务，不是取消它。于是 reader 继续
    /// 读那条 socket、继续把事件喂给 sink，外层却已经在起新连接了
    /// （评审实跑：取消外层 join 后 reader 仍在运行）。
    ///
    /// 现在**不 take**：借用 `self.reader` 来 await，所有权自始至终留在 `self`
    /// 里，取消时 `self` 被 drop → `Drop` 的 `abort()` 收掉 reader。
    /// 只有在 join 真正返回之后才把它取出来（此时任务已结束，abort 无意义）。
    pub(crate) async fn join_reader(
        mut self,
    ) -> Result<anyhow::Result<()>, tokio::task::JoinError> {
        if let Some(keepalive) = self.keepalive.take() {
            keepalive.abort();
        }
        let reader = self
            .reader
            .as_mut()
            .expect("user stream reader handle is joined exactly once");
        let outcome = reader.await;
        // 已经结束的任务不需要再 abort；取出来只是让 `Drop` 无事可做。
        let _ = self.reader.take();
        outcome
    }
}

impl Drop for UserStreamTasks {
    fn drop(&mut self) {
        if let Some(keepalive) = self.keepalive.take() {
            keepalive.abort();
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}

/// **user stream reader 的帧循环。**
///
/// 从 `run_once` 里提出来是为了让两件事**不依赖真实 websocket** 就能被钉住
/// ：
///
/// 1. **帧消费的连续性**。评审变异 R7-06（"第一条可解析 Text 之后正常结束、
///    不消费 sink"）8/8 存活——整套用例里没有任何一条驱动过这个循环本体，
///    于是"每一帧都会被交给 sink"这条性质根本没被观察过。
/// 2. **写失败的传播**。sink 落库失败时 reader 此前继续读下一帧、不重连、
///    不降 readiness；现在它记 recovery failure（`ready=0`）并让整条流失败
///    返回，由外层重连路径重新走 **回补 → reconcile → ready**
///    （`docs/SAFETY_MODEL.md` §3「用户流启动、重连或回补失败」那一行）。
///
/// 泛型参数就是生产里的那两半（`ws.split()`），因此测试喂 `stream::iter`
/// 与一个记账 sink，跑的是**同一段代码**。
pub(crate) async fn run_user_stream_reader_loop<R, W>(
    store: &Store,
    credential_id: &str,
    sink: &BinanceEventSink,
    read: &mut R,
    write: &mut W,
    idle_deadline: Duration,
    write_deadline: Duration,
) -> anyhow::Result<()>
where
    R: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    W: futures_util::Sink<Message> + Unpin,
    W::Error: std::fmt::Display,
{
    loop {
        let next = match crate::binance_futures::next_frame_within(idle_deadline, read).await {
            crate::binance_futures::StreamPoll::Frame(msg) => msg,
            crate::binance_futures::StreamPoll::Ended => {
                // **EOF 必须在
                // reader 返回**之前**落 not-ready。**
                //
                // R17 这里是一句裸 `break`：reader 返回 `Ok(())`，`ready` 那一行
                // 一个字节都没动。降 readiness 要等外层 `run_once` 走完
                // `join_reader()` 再写 `disconnected`——中间隔着一次任务 join、
                // 一次 `load_fill_stream_state` 与一整个写事务。这段窗口里
                // 库里的 `ready` 仍是 1，而 `risk.rs` 正是拿它放行新开仓：
                // 一条**已经断掉**的用户流在这期间照样准入新敞口
                // （engine safety contract「运行状态丢失」）。更糟的是 reader 若以
                // `JoinError`（panic / abort）回来，`join_reader()` 那一步的
                // `?` 会直接把 `run_once` 弹出去，那条 `disconnected` 写入
                // **一次都不会发生**，`ready=1` 一直挂到下一轮 `begin_recovery_cycle`。
                //
                // 现在 reader 自己在返回前落 failure：`record_recovery_failure`
                // 同时**推恢复代际**，于是主干那条早已采样完成、正在飞的
                // `finalize_recovery_readiness` 的 CAS 也一并落空
                // （`fill_replay::finalize_recovery_readiness`）。
                let reason = "user stream ended without a close frame";
                tracing::warn!(
                    credential_id = %credential_id,
                    "binance user stream ended; dropping readiness before the reader returns \
                     so no new entry is admitted on a dead stream"
                );
                drop_readiness_before_returning(store, credential_id, "ws_ended", reason).await?;
                break;
            }
            crate::binance_futures::StreamPoll::IdleTimeout => {
                // **空闲超时也走
                // 同一条 failure-before-return。**
                //
                // R18 这里只是 `return Err(...)`：理由报出去了，可 `ready` 一个
                // 字节都没动。降 readiness 要等外层 `run_once` 走完
                // `join_reader()` 再写 `disconnected`——而那一步在采样
                // （`tasks.reader_finished()`）之后、ready CAS 之前根本来不及：
                // 采样时 reader 还活着 ⇒ `stream_phase = Succeeded`，恢复代际
                // 一格都没被推走 ⇒ CAS 命中 ⇒ 一条**已经静默 5 分钟**的用户流
                // 被写成 `ready=1` 继续放行新开仓（engine safety contract「运行状态丢失」）。
                let reason = format!(
                    "user stream idle beyond {}ms deadline",
                    idle_deadline.as_millis()
                );
                tracing::error!(
                    credential_id = %credential_id,
                    idle_deadline_ms = idle_deadline.as_millis() as u64,
                    "binance user stream produced no frame within its idle \
                     deadline; treating the stream as dead and reconnecting"
                );
                drop_readiness_before_returning(store, credential_id, "ws_idle_timeout", &reason)
                    .await?;
                return Err(anyhow!(reason));
            }
        };
        // **transport 错误是
        // reader 最后一条不落 not-ready 的退出面。**
        //
        // R18 这里是一句裸 `let msg = next?`：TLS 断开、协议错、非法 opcode、
        // 读到一半的帧——tungstenite 报的每一种传输故障都从这里直接弹出去，
        // `fill_stream_state` 一行都没写。它与 IdleTimeout 同根，也与
        // Binary/Close/EOF 同类：**这条流已经死了，而 `ready` 还是 1**。
        // 现在四类退出面共用 `drop_readiness_before_returning`——同一个
        // helper、同一次 `record_recovery_failure`、同一次代际推进，
        // 于是"哪一种死法"不再决定"要不要降 readiness"。
        let msg = match next {
            Ok(msg) => msg,
            Err(error) => {
                let reason = format!("user stream transport error before the next frame: {error}");
                tracing::error!(
                    credential_id = %credential_id,
                    failure_reason = %error,
                    "binance user stream transport failed; dropping readiness before the \
                     reader returns so no new entry is admitted on a dead stream"
                );
                drop_readiness_before_returning(store, credential_id, "ws_transport", &reason)
                    .await?;
                return Err(anyhow!(reason));
            }
        };
        match msg {
            // 两类 **data frame** 走同一条 envelope 路径（下同）。
            Message::Text(text) => {
                handle_ws_data_frame(store, credential_id, sink, WsDataFrameKind::Text, &text)
                    .await?;
            }
            // **Binary 帧不再
            // 落进 wildcard。**
            //
            // R17 的 `_ => {}` 把 Binary 静默丢掉：不解析、不写 failure、
            // 不重连。Binary 与 Text 一样是 **data frame**——它承载的是应用
            // 载荷，也就是我们**只能**从交易所拿到、丢了补不回来的那部分真值。
            // 一条 `ORDER_TRADE_UPDATE` 换成 Binary 送来，R17 的引擎会当作
            // 什么都没发生，`ready` 保持 1 继续放行新开仓
            // （engine safety contract「运行状态丢失」、§3.12「缺失数据不得被写成正常」）。
            //
            // 处置取的是「**按同一 envelope 解析**」那一支：能读懂就照常消费
            // （一个字节都不丢），读不懂就与 Text 走**同一条** fail-closed
            // 路径。UTF-8 都解不出来的载荷没有第二种解释——它就是一份我们
            // 读不懂的真值，`ws_binary` 记下这件事并断流。
            Message::Binary(payload) => {
                let text = match std::str::from_utf8(&payload) {
                    Ok(text) => text,
                    Err(error) => {
                        let reason = format!(
                            "user stream binary frame of {} bytes is not valid UTF-8 and \
                             therefore cannot be read as an event envelope: {error}",
                            payload.len()
                        );
                        tracing::error!(
                            credential_id = %credential_id,
                            frame_bytes = payload.len(),
                            failure_reason = %error,
                            "user stream received a binary frame it cannot decode; dropping \
                             readiness and reconnecting so backfill and reconcile can restore \
                             the exchange truth"
                        );
                        drop_readiness_before_returning(store, credential_id, "ws_binary", &reason)
                            .await?;
                        return Err(anyhow!(reason));
                    }
                };
                handle_ws_data_frame(store, credential_id, sink, WsDataFrameKind::Binary, text)
                    .await?;
            }
            Message::Ping(payload) => {
                // 写侧同样有界。
                //
                // **Pong 写失败
                // 是第七条退出面，它此前是**唯一**一条不落 not-ready 就走的。**
                //
                // R19 这里是一句裸 `send_frame_within(...).await?`：写失败
                // （对端已经把连接掐了、TLS 断了）与写超时（对端不读、TCP 写
                // 缓冲填满）都从这里直接弹出去，`fill_stream_state` 一行都没写。
                // 它与 transport `Err`、IdleTimeout 同根——**这条流已经死了，
                // 而 `ready` 还是 1**：`run_once` 主干那次 `tasks.reader_finished()`
                // 采样发生在 reader 失败**之前**时，三段判据全是 `Succeeded`、
                // 恢复代际一格没被推走 ⇒ `finalize_recovery_readiness` 的 CAS
                // 命中 ⇒ 库里出现一段 `ready=1`，`risk.rs` 照常放行新开仓
                // （engine safety contract「运行状态丢失」）。
                //
                // Ping 尤其不能例外：Binance 每 ~3 分钟发一次心跳，它是这条流
                // 上**最频繁**的写触点，也因此是写侧故障最先暴露的地方。
                if let Err(error) = crate::binance_futures::send_frame_within(
                    write_deadline,
                    write,
                    Message::Pong(payload),
                )
                .await
                {
                    let reason = format!("user stream could not answer a venue ping: {error}");
                    tracing::error!(
                        credential_id = %credential_id,
                        failure_reason = %error,
                        "binance user stream could not write the pong a venue ping requires; \
                         dropping readiness before the reader returns so no new entry is \
                         admitted on a stream we can no longer write to"
                    );
                    drop_readiness_before_returning(store, credential_id, "ws_pong_write", &reason)
                        .await?;
                    return Err(anyhow!(reason));
                }
            }
            // **控制帧，没有应用载荷**：RFC 6455 §5.5.3 允许服务端主动发
            // 未经请求的 Pong 当心跳。它按定义带不了交易所真值，因此是这段
            // 匹配里**唯一**一个"什么都不做"是安全的分支——判据是帧类型的
            // 协议语义，不是"我们不认识它"。
            Message::Pong(_) => {
                tracing::debug!(
                    credential_id = %credential_id,
                    "binance user stream sent an unsolicited pong"
                );
            }
            Message::Close(frame) => {
                // ****：与 EOF
                // 同一条理由——not-ready 必须在 reader 返回之前落库。
                tracing::warn!(credential_id=%credential_id, ?frame, "binance user stream closed");
                let reason = match &frame {
                    Some(frame) => format!(
                        "user stream closed by the venue: code={} reason={}",
                        frame.code, frame.reason
                    ),
                    None => "user stream closed by the venue without a close reason".to_string(),
                };
                drop_readiness_before_returning(store, credential_id, "ws_closed", &reason).await?;
                break;
            }
            // `Message::Frame(_)` 只由 tungstenite 的低层写侧构造，读侧的
            // `Stream` 实现不产出它。**它仍然要显式列在这里**：这段匹配
            // 一个 wildcard 都不留，将来 tungstenite 新增一个帧变体时是
            // **编译失败**，而不是又一条静默忽略的真值路径（regression
            // 的根因就是那个 `_ => {}`）。
            Message::Frame(frame) => {
                let reason = format!(
                    "user stream yielded a raw websocket frame the reader cannot interpret: \
                     {} payload bytes",
                    frame.payload().len()
                );
                tracing::error!(
                    credential_id = %credential_id,
                    "user stream yielded a raw frame; dropping readiness and reconnecting"
                );
                drop_readiness_before_returning(store, credential_id, "ws_frame", &reason).await?;
                return Err(anyhow!(reason));
            }
        }
    }
    Ok(())
}

/// **reader 的每一条退出面在返回之前先把 readiness 落成 0**
/// 。
///
/// 帧循环有七条退出面：`StreamPoll::Ended`（EOF）、`StreamPoll::IdleTimeout`、
/// **transport `Err`**、读不懂的 Binary、venue Close、原始 `Frame`、
/// 以及 **Ping 的 Pong 写失败**。
/// R18 只给其中四条写了 recovery failure，IdleTimeout 与 transport `Err`
/// 各自 `return Err(...)` 就走了；R19 收了那两条，却把 Pong 写侧那条
/// 漏在外面——**同一件事（这条流已经死了）按死法分成了两种处置**。
///
/// 为什么"报个错回去"不够：`run_once` 的 readiness 判据里，reader 的死活是
/// 一次**性采样**（`tasks.reader_finished()`），采样点与 ready 真的落库之间
/// 隔着一次数据库往返。reader 在这段窗口里失败时，采样看到的还是"活着"
/// ⇒ `stream_phase = Succeeded` ⇒ 那次 ready 写入照样命中，
/// 库里于是出现一段 `ready=1`；要等外层 join 完 reader 再写 `disconnected`
/// 才降下来。`risk.rs` 正是拿这一位放行新开仓——窗口里进来的每一条 intent
/// 都是在一条已经与交易所脱节的流上开的新敞口
/// （engine safety contract「运行状态丢失」）。
///
/// 挡住那次在飞的写入的**唯一**机制是恢复代际 CAS：
/// `record_recovery_failure` 每次都推一格代际，而
/// `finalize_recovery_readiness` 的 `UPDATE ... WHERE generation = ?`
/// 于是命中 0 行（`fill_replay::finalize_recovery_readiness`）。也就是说
/// "在返回之前写一次 failure"不只是记账，它就是那道 CAS 的**推手**。
/// 七条退出面共用这一个 helper，正是为了让"哪一种死法"再也不能决定
/// "要不要降 readiness"。
///
/// `phase` 只用来把死法说清楚（`ws_ended` / `ws_idle_timeout` /
/// `ws_transport` / `ws_binary` / `ws_closed` / `ws_frame` /
/// `ws_pong_write`），它不参与任何判据。
async fn drop_readiness_before_returning(
    store: &Store,
    credential_id: &str,
    phase: &str,
    reason: &str,
) -> anyhow::Result<()> {
    fill_replay::record_recovery_failure(
        store,
        credential_id,
        phase,
        None,
        None,
        reason,
        crate::svc::now_ms(),
    )
    .await
}

/// **两类 data frame 的类别**——只用来把失败说清楚（`Text` / `Binary` 走的是
/// 同一条 envelope 解析与消费路径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WsDataFrameKind {
    Text,
    Binary,
}

impl WsDataFrameKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Binary => "binary",
        }
    }
}

/// **一条 data frame 的解析 + 消费**。
///
/// 从帧循环里提出来是因为 Binary 现在与 Text 共用它：两类 data frame 必须
/// 走**逐字相同**的判据，否则"Binary 也 fail-closed"就会退化成第二份实现
/// 并各自漂移。
async fn handle_ws_data_frame(
    store: &Store,
    credential_id: &str,
    sink: &BinanceEventSink,
    kind: WsDataFrameKind,
    text: &str,
) -> anyhow::Result<()> {
    let parsed = match serde_json::from_str::<BinanceWsEnvelope>(text) {
        Ok(env) => env,
        Err(error) => {
            // **解析失败一律
            // fail-closed，「事件名不在白名单里」不是安全跳过的理由。**
            //
            // R16 在这里留了一条前向兼容的逃生口：`"e"` 读得出来、
            // 且不在 `TRUTH_BEARING_WS_EVENT_TYPES` 里时 warn+continue，
            // `ready` 一点不降。它建立在一个**没有真值支撑**的推断上
            // ——「事件名未知 ⇒ 这一帧没承载我们需要的真值」。可是这一帧
            // **根本没解析成功**：那个名字是从一份我们已经证明自己读不懂
            // 的字节流里抠出来的，它证明不了任何事。评审给的反例正是
            // `listenKeyExpired`：一条畸形的 listenKey 失效通知被静默吞掉，
            // 而 `ready` 继续放行新开仓，直到 idle deadline 才发现流早死了。
            //
            // **前向兼容不在这条路径上。** `BinanceWsEnvelope` 只强制
            // 一个 `"e"`（`E`/`T`/`o`/`a` 全是 `Option`），交易所新增的
            // 事件类型**解析得动**，随后由 `handle_ws_envelope` 的
            // 闭世界分发跳过并留下 warn——那条路径一个字节都没丢。
            // 能走到这里的只有**格式本身坏掉**的帧，它一定丢了真值：
            // 要么这一帧是已知事件而我们没读懂，要么我们连它是什么
            // 都不知道。两种都必须降 readiness 并重连，由
            // **回补 → reconcile → ready** 把这一段真值补回来
            // （engine safety contract「运行状态丢失」「对账失败后继续交易」，
            // §3.12「缺失数据不得被转换成正常」）。
            //
            // 安全跳过要成立，判据必须来自**真值本身**——schema/version
            // 协商出来的、可验证的"这一帧与本地无关"，而不是一个从坏
            // 字节里读出的名字。本仓当前没有这样的真值来源，因此没有
            // 任何一条解析失败的帧可以被跳过。
            let class = classify_unparsed_ws_frame(text);
            let reason = format!(
                "user stream {} frame with {} could not be parsed: {error}",
                kind.as_str(),
                class.describe()
            );
            tracing::error!(
                credential_id = %credential_id,
                frame_kind = kind.as_str(),
                frame_class = class.describe(),
                failure_reason = %error,
                "user stream could not parse a frame; dropping readiness and \
                 reconnecting so backfill and reconcile can restore the exchange \
                 truth"
            );
            fill_replay::record_recovery_failure(
                store,
                credential_id,
                "ws_parse",
                None,
                None,
                &reason,
                crate::svc::now_ms(),
            )
            .await?;
            return Err(anyhow!(reason));
        }
    };
    if let Err(error) = sink.handle_ws_envelope(credential_id, &parsed).await {
        // **本地已经与交易所真值脱节**：这一帧一个字节都没落库。
        // 继续读下一帧等于把 stream 当健康的用，而新开仓正是靠
        // `ready` 放行的。降 readiness 并断流，交给重连路径回补。
        tracing::error!(
            credential_id = %credential_id,
            frame_kind = kind.as_str(),
            failure_reason = %error,
            "user stream could not apply an order update; dropping \
             readiness and reconnecting so backfill and reconcile \
             can restore the exchange truth"
        );
        fill_replay::record_recovery_failure(
            store,
            credential_id,
            "ws_apply",
            None,
            None,
            &error.to_string(),
            crate::svc::now_ms(),
        )
        .await?;
        return Err(anyhow!("user stream could not apply a frame: {error}"));
    }
    Ok(())
}

/// 恢复循环的重试节奏。
///
/// `run_once` 是一条**定死的工作流**（ConnectWebSocket → BackfillRest → Reconcile →
/// DeclareReady，见本函数开头的断言），每一轮都要重跑整段 REST 回补与整轮真值对账
/// ——按 `dd99c36` 复算约 95 权重。所以"多久重来一次"就是这条链的权重开销本身：
/// 2 秒定值下一个抖动的流是 2850 权重/分钟，而 Binance 公布的 per-IP 额度是
/// 2400/分钟——**单个凭据的一条坏流就能吃掉整个出口**。
const USER_STREAM_RETRY_BASE_MS: u64 = 2_000;

/// 退避上限**不是常量**：它等于周期对账的间隔（`RECONCILE_INTERVAL_MS`）。
///
/// 裁决记录（2026-09-05，设计要求 授权自主裁决，理由见 recovery scheduling constraints）：
/// 简报要求"只退 REST 重同步、socket 重连保持快速"。但 `run_once` 是被断言钉死的
/// 四步工作流，拆开它属于改恢复契约，不该塞进限流批次；且一轮 ~106 权重里
/// socket 连接只占 1，简报的经济学成立，但它要的收益依赖"连上但未 ready"这个
/// 状态，而在本仓 `DeclareReady` 之前应用流事件正是那条断言在防的事。
/// 因此保留整循环退避，但把上限压到等于独立真值路径的节拍：流中断窗口
/// **永远不会超过一次周期对账**——这是可陈述、可测的不变量，60 秒魔数不是。
fn user_stream_retry_cap_ms() -> u64 {
    crate::reconcile::periodic_reconcile_interval_ms().max(USER_STREAM_RETRY_BASE_MS)
}

/// 一次「健康」的连接至少要活这么久。
///
/// 判据**不是 Ok/Err**。干净断开会走 Ok 分支，而一条连上就立刻掉的流正是最危险的
/// 形态：每 2 秒付一次全量回补＋全量对账，计数器却因为"成功"永不递增。用**本轮存活
/// 时长**判别，才能把"挂了几小时后正常断开"（应立即重连）与"连上就掉"（应退避）
/// 分开——前者是 socket 的正常生命周期，后者是自我 DoS。
const USER_STREAM_HEALTHY_CYCLE_MS: u64 = 60_000;

/// 连续短命轮次达到 `consecutive_short_cycles` 次之后，下一轮要等多久。
///
/// 抽成纯函数是为了让**上限**与**基线**各自可被钉住：没有上限会退成小时级，等于放弃
/// fill recovery；没有退避就是上面那条自我 DoS。第 0 次（刚健康过）返回基线，
/// 也就是维持既有的 2 秒重连速度——socket 重连本身只花一次权重 1 的 listenKey，
/// **不该为了省权重去拖慢它**。
pub(crate) fn user_stream_retry_delay_ms(consecutive_short_cycles: u32, cap_ms: u64) -> u64 {
    USER_STREAM_RETRY_BASE_MS
        .saturating_mul(
            1_u64
                .checked_shl(consecutive_short_cycles)
                .unwrap_or(u64::MAX),
        )
        .min(cap_ms)
}

/// The cap applies to the final sleep, including jitter. Capping before jitter
/// made a saturated recovery loop exceed the reconcile interval by about one
/// second, violating the very cadence bound this backoff is meant to preserve.
fn user_stream_retry_sleep_ms(consecutive_short_cycles: u32, cap_ms: u64, jitter_ms: u64) -> u64 {
    user_stream_retry_delay_ms(consecutive_short_cycles, cap_ms)
        .saturating_add(jitter_ms)
        .min(cap_ms)
}

/// 本轮结束后的连续短命计数。存活够久即清零。
pub(crate) fn next_short_cycle_count(previous: u32, cycle_duration_ms: u64) -> u32 {
    if cycle_duration_ms >= USER_STREAM_HEALTHY_CYCLE_MS {
        0
    } else {
        previous.saturating_add(1)
    }
}

/// Fixed cadence for a logical reconcile divergence. The next check is always
/// at least one complete reconcile interval after both the previous check and
/// the current clock, so a long preceding cycle cannot collapse it back to the
/// normal 2-second reconnect baseline.
fn next_divergence_recheck_at_ms(last_check_ms: i64, now_ms: i64, interval_ms: u64) -> i64 {
    last_check_ms
        .max(now_ms)
        .saturating_add(i64::try_from(interval_ms).unwrap_or(i64::MAX))
}

fn next_divergence_retry_count(previous: u32) -> u32 {
    previous.saturating_add(1)
}

#[derive(Debug, PartialEq, Eq)]
enum PeriodicDivergenceRecheck {
    NoFreshFullTruth,
    Diverged(String),
    Converged,
}

fn periodic_divergence_recheck(
    store: &Store,
    credential_id: &str,
    not_before_ms: i64,
) -> PeriodicDivergenceRecheck {
    let Some(truth) = crate::reconcile::latest_full_periodic_reconcile_truth_since(
        store,
        credential_id,
        not_before_ms,
    ) else {
        return PeriodicDivergenceRecheck::NoFreshFullTruth;
    };
    match crate::reconcile::recovery_reconcile_divergence(truth) {
        Some(reason) => PeriodicDivergenceRecheck::Diverged(reason),
        None => PeriodicDivergenceRecheck::Converged,
    }
}

/// Persist a logical reconcile divergence without advancing the recovery
/// generation. The WS reader is still live and the backfill in this generation
/// already succeeded, so advancing the generation would make a later periodic
/// truth unable to complete the same honest handoff. The generation predicate
/// preserves the existing reader-failure CAS: if the reader failed first, this
/// write changes zero rows and cannot erase that transport failure.
async fn record_recovery_divergence(
    store: &Store,
    credential_id: &str,
    cycle_generation: i64,
    reason: &str,
    now_ms: i64,
) -> anyhow::Result<bool> {
    let rows = sqlx::query(
        "UPDATE fill_stream_state \
         SET ready=0, phase='reconcile_divergence', failure_reason=?, updated_at_ms=? \
         WHERE credential_id=? AND generation=?",
    )
    .bind(reason)
    .bind(now_ms)
    .bind(credential_id)
    .bind(cycle_generation)
    .execute(store.pool())
    .await
    .context("record recovery reconcile divergence without advancing its generation")?
    .rows_affected();
    Ok(rows == 1)
}

async fn run_once(
    credential_id: &str,
    keys: BinanceKeys,
    store: Store,
    sink: BinanceEventSink,
    limiter: CredentialRateLimiter,
    trigger: RecoveryTrigger,
) -> anyhow::Result<()> {
    let workflow = fill_replay::recovery_workflow(trigger);
    if workflow
        != [
            RecoveryStep::ConnectWebSocket,
            RecoveryStep::BackfillRest,
            RecoveryStep::Reconcile,
            RecoveryStep::DeclareReady,
        ]
    {
        return Err(anyhow!("fill recovery workflow omitted a required step"));
    }
    let cycle =
        fill_replay::begin_recovery_cycle(&store, credential_id, trigger, crate::svc::now_ms())
            .await?;
    // Capture this before the reader exists. A later reader failure advances
    // the generation, and every divergence write below is conditional on this
    // exact value so it cannot overwrite the transport failure.
    let cycle_generation = fill_replay::load_fill_stream_state(&store, credential_id)
        .await?
        .ok_or_else(|| anyhow!("fill stream state disappeared after recovery cycle start"))?
        .generation;
    let ws_transport =
        crate::venue::ws_transport_for_credential(credential_id).map_err(anyhow::Error::msg)?;

    let client = BinanceFuturesClient::new_with_store(keys, credential_id, store.clone())?;
    client.discover_reconcile_budget().await;
    let listen_key = match client.create_listen_key().await {
        Ok(value) => value,
        Err(error) => {
            fill_replay::record_recovery_failure(
                &store,
                credential_id,
                "connect",
                None,
                None,
                &format!("{error:#}"),
                crate::svc::now_ms(),
            )
            .await?;
            return Err(error);
        }
    };

    let ws_url = format!("{}/ws/{listen_key}", ws_transport.base_url);
    let trust = ws_transport.trust_profile();
    tracing::info!(
        credential_id,
        upstream_venue = ws_transport
            .venue
            .map(|value| value.to_string())
            .as_deref()
            .unwrap_or("unconfigured"),
        transport = ws_transport.mode.as_str(),
        built_in_roots_preserved = trust.built_in_roots_preserved,
        added_test_roots = trust.added_test_roots,
        "binance user stream connecting"
    );

    let connector = ws_transport.connector().map_err(anyhow::Error::msg)?;
    // ****：见
    // `binance_futures::ws_timeouts_from_env` 的说明。user-stream 挂住的后果
    // 比行情更重：fill recovery 与交易所真值对账都不再推进。
    let (connect_deadline, idle_deadline) = crate::binance_futures::ws_timeouts_from_env();
    let connect_outcome = match tokio::time::timeout(
        connect_deadline,
        tokio_tungstenite::connect_async_tls_with_config(ws_url, None, false, connector),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_elapsed) => {
            let reason = format!(
                "user stream connect exceeded {}ms deadline",
                connect_deadline.as_millis()
            );
            fill_replay::record_recovery_failure(
                &store,
                credential_id,
                "connect",
                None,
                None,
                &reason,
                crate::svc::now_ms(),
            )
            .await?;
            return Err(anyhow::anyhow!(reason));
        }
    };
    let (ws, _resp) = match connect_outcome {
        Ok(value) => value,
        Err(error) => {
            fill_replay::record_recovery_failure(
                &store,
                credential_id,
                "connect",
                None,
                None,
                &error.to_string(),
                crate::svc::now_ms(),
            )
            .await?;
            return Err(error.into());
        }
    };
    let (mut write, mut read) = ws.split();

    // Keepalive every 30 minutes (Binance recommends <= 30m).
    let keepalive = tokio::spawn({
        let client = client.clone();
        let listen_key = listen_key.clone();
        let credential_id = credential_id.to_string();
        async move {
            let interval = Duration::from_secs(30 * 60);
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = client.keepalive_listen_key(&listen_key).await {
                    tracing::warn!(credential_id, error=%e, "binance listenKey keepalive failed");
                }
            }
        }
    });

    let task_credential_id = credential_id.to_string();
    let ws_sink = sink.clone();
    let task_store = store.clone();
    let reader = tokio::spawn(async move {
        run_user_stream_reader_loop(
            &task_store,
            &task_credential_id,
            &ws_sink,
            &mut read,
            &mut write,
            idle_deadline,
            connect_deadline,
        )
        .await
    });

    // **两条后台任务的所有权从这里起是结构化的**（段级第五轮复评
    // regression）。构造之后，函数体里任何一条提前返回都会把它们一起收掉。
    let mut tasks = UserStreamTasks::new(keepalive, reader);

    let scopes = fill_replay::scopes_for_credential(&store, credential_id).await?;
    for symbol in scopes {
        let cursor = fill_replay::load_fill_replay_cursor(&store, credential_id, &symbol)
            .await?
            .unwrap_or(0);
        fill_replay::record_recovery_progress(
            &store,
            credential_id,
            cycle,
            "backfill",
            Some(&symbol),
            Some(cursor),
            crate::svc::now_ms(),
        )
        .await?;
        let trades = match client
            .user_trades_after(&limiter, &symbol, cursor, 1_000, RequestClass::EventTruth)
            .await
        {
            Ok(trades) => trades,
            Err(error) => {
                let _ = fill_replay::finalize_fill_backfill(
                    &store,
                    credential_id,
                    &symbol,
                    BackfillAttempt {
                        attempted_last_trade_id: cursor,
                        completed_at_ms: crate::svc::now_ms(),
                        completion: BackfillCompletion::Failed,
                    },
                )
                .await?;
                fill_replay::record_recovery_failure(
                    &store,
                    credential_id,
                    "backfill",
                    Some(&symbol),
                    Some(cursor),
                    &format!("{error:#}"),
                    crate::svc::now_ms(),
                )
                .await?;
                tracing::error!(credential_id, %symbol, cursor_last_trade_id=cursor, failure_reason=%error, "REST fill backfill failed");
                return Err(error);
            }
        };
        let mut completed_cursor = cursor;
        for trade in &trades {
            if let Err(error) = sink.handle_rest_user_trade(credential_id, trade).await {
                let _ = fill_replay::finalize_fill_backfill(
                    &store,
                    credential_id,
                    &symbol,
                    BackfillAttempt {
                        attempted_last_trade_id: completed_cursor,
                        completed_at_ms: crate::svc::now_ms(),
                        completion: BackfillCompletion::Failed,
                    },
                )
                .await?;
                fill_replay::record_recovery_failure(
                    &store,
                    credential_id,
                    "backfill_ingest",
                    Some(&symbol),
                    Some(cursor),
                    &error.to_string(),
                    crate::svc::now_ms(),
                )
                .await?;
                tracing::error!(credential_id, %symbol, cursor_last_trade_id=cursor, failure_reason=%error, "REST fill ingest failed");
                return Err(anyhow!(error));
            }
            completed_cursor = fill_replay::next_backfill_cursor(completed_cursor, trade.id);
        }
        fill_replay::finalize_fill_backfill(
            &store,
            credential_id,
            &symbol,
            BackfillAttempt {
                attempted_last_trade_id: completed_cursor,
                completed_at_ms: crate::svc::now_ms(),
                completion: BackfillCompletion::Succeeded,
            },
        )
        .await?;
    }

    fill_replay::record_recovery_progress(
        &store,
        credential_id,
        cycle,
        "reconcile",
        None,
        None,
        crate::svc::now_ms(),
    )
    .await?;
    // **重连要消费完整的对账真值。**
    //
    // R13 这里只区分 `Err` / `Ok`，把 `Ok` 里那五段真值整个丢掉，然后给
    // `finalize_recovery_readiness` 固定传 `Succeeded`。于是一次**跑完了、
    // 但报告本地与交易所有分歧**的对账，照样把这条凭据写成 `ready`——
    // `ready` 正是 `risk.rs` 里放行新开仓的那个判据（engine safety contract
    // 「对账失败后继续交易」一律 High）。
    //
    // 判据取的是**分歧**那两段（`has_diff` 与 `local_position_zero_diff`），
    // 不是 `flatten_complete()`：后者还要求仓位归零、挂单为空——那是
    // kill-switch L3 的收敛判据，不是"恢复完成"的判据。正常持仓运行时
    // 它恒为假，拿它当 readiness 会让引擎在**任何**重连之后永远不 ready，
    // 把一条安全判据变成一条可用性故障。分歧真值则不同：它意味着这次恢复
    // 没能把本地与交易所对齐，此时放行新开仓正是 §3.1 点名的那条 High。
    let reconcile_truth = match crate::reconcile::reconcile_binance_futures_truth_with_client(
        credential_id,
        store.clone(),
        limiter,
        &client,
    )
    .await
    {
        Ok(truth) => truth,
        Err(error) => {
            fill_replay::record_recovery_failure(
                &store,
                credential_id,
                "reconcile",
                None,
                None,
                &format!("{error:#}"),
                crate::svc::now_ms(),
            )
            .await?;
            tracing::error!(credential_id, failure_reason=%error, "post-backfill reconcile failed");
            return Err(error);
        }
    };
    if let Some(mut reason) = crate::reconcile::recovery_reconcile_divergence(reconcile_truth) {
        let divergence_observed_at_ms = crate::svc::now_ms();
        if !record_recovery_divergence(
            &store,
            credential_id,
            cycle_generation,
            &reason,
            divergence_observed_at_ms,
        )
        .await?
        {
            return Err(anyhow!(
                "recovery reconcile divergence was superseded before it could be recorded"
            ));
        }
        tracing::error!(
            credential_id,
            failure_reason = %reason,
            "post-backfill reconcile completed but reported a divergence; the fill handoff \
             stays not-ready and waits for periodic reconcile truth"
        );

        // Periodic divergence retries must not restart full recovery.
        // The healthy reader remains active; each retry waits a full periodic
        // reconcile interval and consumes only a newer FullScan truth published
        // by the periodic reconciler. This branch owns no Binance client and
        // therefore cannot issue userTrades/openOrders/openAlgoOrders/
        // positionRisk/balance requests.
        let mut last_check_ms = divergence_observed_at_ms;
        let mut divergence_failures = 1_u32;
        loop {
            let interval_ms = crate::reconcile::periodic_reconcile_interval_ms();
            let now_ms = crate::svc::now_ms();
            let next_check_ms = next_divergence_recheck_at_ms(last_check_ms, now_ms, interval_ms);
            let delay_ms = u64::try_from(next_check_ms.saturating_sub(now_ms)).unwrap_or(u64::MAX);
            if let Some(reader_outcome) = tasks
                .wait_for_reader_or_delay(Duration::from_millis(delay_ms))
                .await
            {
                let reader_result = reader_outcome
                    .context("join user stream reader while waiting for reconcile convergence")?;
                reader_result?;
                return Ok(());
            }
            last_check_ms = crate::svc::now_ms();
            match periodic_divergence_recheck(&store, credential_id, divergence_observed_at_ms) {
                PeriodicDivergenceRecheck::Converged => break,
                PeriodicDivergenceRecheck::Diverged(next_reason) => {
                    reason = next_reason;
                    divergence_failures = next_divergence_retry_count(divergence_failures);
                    if !record_recovery_divergence(
                        &store,
                        credential_id,
                        cycle_generation,
                        &reason,
                        last_check_ms,
                    )
                    .await?
                    {
                        return Err(anyhow!(
                            "recovery divergence recheck was superseded by a newer stream failure"
                        ));
                    }
                    tracing::warn!(
                        credential_id,
                        failure_reason = %reason,
                        interval_ms,
                        divergence_failures,
                        "periodic reconcile truth is still divergent; keeping the fill handoff \
                         not-ready without repeating backfill or a full recovery scan"
                    );
                }
                PeriodicDivergenceRecheck::NoFreshFullTruth => {
                    divergence_failures = next_divergence_retry_count(divergence_failures);
                    tracing::warn!(
                        credential_id,
                        interval_ms,
                        divergence_failures,
                        "no fresh periodic full reconcile truth is available; keeping the fill \
                         handoff not-ready without manufacturing a replacement truth"
                    );
                }
            }
        }
    }
    // Recovery readiness sample: this only detects failure earlier,
    // 不是判据。**
    //
    // `reader_finished()` 是**一次性采样**，采样点与 ready 真的落库之间还隔着
    // 一次数据库往返；reader 完全可以在这中间才失败。R16 的形状因此是：
    // reader 写下 `ready=0 / phase=ws_parse`，这里一条无谓词的 upsert 把它
    // 整行覆盖成 `ready=1`——一条已经与交易所脱节的流重新放行新开仓。
    //
    // 真正的判据在 `finalize_recovery_readiness` 里：写入绑**本周期的恢复
    // 代际**，而 `record_recovery_failure` 每次都推代际。采样保留是因为它便宜
    // ——能提前一步走 not-ready 分支就不必等 CAS 落空。
    let stream_phase = if tasks.reader_finished() {
        RecoveryPhaseResult::Failed
    } else {
        RecoveryPhaseResult::Succeeded
    };
    let ready_write = fill_replay::finalize_recovery_readiness(
        &store,
        credential_id,
        RecoveryPhaseResult::Succeeded,
        RecoveryPhaseResult::Succeeded,
        stream_phase,
        cycle,
        crate::svc::now_ms(),
    )
    .await?;
    if ready_write != fill_replay::ReadinessWrite::Ready {
        // `join_reader` 消费掉 tasks：keepalive 立即被收，reader 被 join。
        let reader_result = tasks
            .join_reader()
            .await
            .context("join disconnected user stream reader")?;
        fill_replay::record_recovery_failure(
            &store,
            credential_id,
            "disconnected_before_ready",
            None,
            None,
            "user stream disconnected before fill handoff became ready",
            crate::svc::now_ms(),
        )
        .await?;
        reader_result?;
        return Err(anyhow!("user stream disconnected before ready"));
    }
    tracing::info!(
        credential_id,
        "fill backfill and reconcile succeeded; user stream ready"
    );

    let reader_result = tasks
        .join_reader()
        .await
        .context("join user stream reader")?;

    fill_replay::record_recovery_failure(
        &store,
        credential_id,
        "disconnected",
        None,
        None,
        "user stream disconnected",
        crate::svc::now_ms(),
    )
    .await?;
    reader_result
}

/// **承载交易所真值的 user-stream 事件类型**（与
/// `BinanceEventSink::handle_ws_envelope` 的分发同源）。
///
/// 这三类事件里带着成交、订单终态与账户变化——也就是本地**只能**从交易所拿到、
/// 丢了就再也补不回来的那部分真值。解析失败时它们必须降 readiness 并重连，
/// 由 REST 回补 + 对账把这一段补回来。
pub(crate) const TRUTH_BEARING_WS_EVENT_TYPES: [&str; 3] =
    ["ORDER_TRADE_UPDATE", "ALGO_UPDATE", "ACCOUNT_UPDATE"];

/// 一条**没能反序列化成 `BinanceWsEnvelope`** 的 Text 帧的归类
/// 。
///
/// **归类只用于把失败说清楚，
/// 不再决定处置。** R16 让 `ForwardCompatible` 这一类跳过并保持 `ready`；
/// 那个判据的输入是从一份**已经证明读不懂**的字节流里抠出来的 `"e"`，
/// 它证明不了"这一帧没承载真值"。现在三类一律 fail-closed，归类的唯一
/// 作用是让 `fill_stream_state.failure_reason` 与日志能说出这一帧长什么样。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnparsedWsFrameClass {
    /// `"e"` 读得出来且落在 `TRUTH_BEARING_WS_EVENT_TYPES` 里：已知事件读不懂。
    TruthBearing(String),
    /// `"e"` 读得出来但不在已知集合里。**这不构成安全跳过的理由**：
    /// 名字来自一份没解析成功的帧，而畸形的 `listenKeyExpired` 恰恰是
    /// "流已经死了"的通知（第十三轮 regression 的反例）。
    ForwardCompatible(String),
    /// 连 `"e"` 都读不出来（不是 JSON 对象 / 没有 `"e"` / `"e"` 不是字符串）。
    Unclassifiable,
}

impl UnparsedWsFrameClass {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::TruthBearing(event_type) => {
                format!("truth-bearing event type {event_type}")
            }
            Self::ForwardCompatible(event_type) => {
                format!("unknown event type {event_type}")
            }
            Self::Unclassifiable => "no readable event type".to_string(),
        }
    }
}

/// 从一条**解析失败**的原始帧里尽力读出 `"e"` 并归类。
///
/// 归一化与 `handle_ws_envelope` 一致（`trim`），免得判据与分发各写各的。
pub(crate) fn classify_unparsed_ws_frame(text: &str) -> UnparsedWsFrameClass {
    let Some(event_type) = serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .as_ref()
        .and_then(|value| value.get("e"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    else {
        return UnparsedWsFrameClass::Unclassifiable;
    };
    if TRUTH_BEARING_WS_EVENT_TYPES.contains(&event_type.as_str()) {
        UnparsedWsFrameClass::TruthBearing(event_type)
    } else {
        UnparsedWsFrameClass::ForwardCompatible(event_type)
    }
}

#[derive(Debug, Clone)]
pub struct BinanceWsEnvelope {
    pub event_type: String,
    pub event_time_ms: Option<i64>,
    pub transaction_time_ms: Option<i64>,
    pub order: Option<BinanceOrderTradeUpdate>,
    pub algo_update: Option<BinanceAlgoUpdate>,
    pub account: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct BinanceWsEnvelopeRaw {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time_ms: Option<i64>,
    #[serde(rename = "T")]
    transaction_time_ms: Option<i64>,
    #[serde(rename = "o")]
    order_payload: Option<serde_json::Value>,
    #[serde(rename = "a")]
    account: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for BinanceWsEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = BinanceWsEnvelopeRaw::deserialize(deserializer)?;
        let (order, algo_update) = match (raw.event_type.as_str(), raw.order_payload) {
            ("ORDER_TRADE_UPDATE", Some(payload)) => (
                Some(serde_json::from_value(payload).map_err(serde::de::Error::custom)?),
                None,
            ),
            ("ALGO_UPDATE", Some(payload)) => (
                None,
                Some(serde_json::from_value(payload).map_err(serde::de::Error::custom)?),
            ),
            _ => (None, None),
        };
        Ok(Self {
            event_type: raw.event_type,
            event_time_ms: raw.event_time_ms,
            transaction_time_ms: raw.transaction_time_ms,
            order,
            algo_update,
            account: raw.account,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceAlgoUpdate {
    #[serde(rename = "caid")]
    pub client_algo_id: Option<crate::binance_futures::BinanceClientAlgoId>,
    #[serde(rename = "aid")]
    pub algo_id: Option<crate::binance_futures::BinanceAlgoId>,
    #[serde(rename = "at")]
    pub algo_type: Option<String>,
    #[serde(rename = "o")]
    pub order_type: Option<String>,
    #[serde(rename = "s")]
    pub symbol: Option<String>,
    #[serde(rename = "S")]
    pub side: Option<String>,
    #[serde(rename = "ps")]
    pub position_side: Option<String>,
    #[serde(rename = "f")]
    pub time_in_force: Option<String>,
    #[serde(rename = "q")]
    pub quantity: Option<String>,
    #[serde(
        rename = "X",
        default,
        deserialize_with = "deserialize_optional_nonempty_string"
    )]
    pub algo_status: Option<String>,
    #[serde(
        rename = "ai",
        default,
        deserialize_with = "deserialize_optional_actual_order_id"
    )]
    pub actual_order_id: Option<crate::binance_futures::BinanceActualOrderId>,
    #[serde(rename = "ap")]
    pub actual_price: Option<String>,
    #[serde(rename = "aq")]
    pub actual_quantity: Option<String>,
    #[serde(rename = "act")]
    pub actual_order_type: Option<String>,
    #[serde(rename = "tp")]
    pub trigger_price: Option<String>,
    #[serde(rename = "p")]
    pub price: Option<String>,
    #[serde(rename = "V")]
    pub self_trade_prevention_mode: Option<String>,
    #[serde(rename = "wt")]
    pub working_type: Option<String>,
    #[serde(rename = "pm")]
    pub price_match: Option<String>,
    #[serde(rename = "cp")]
    pub close_position: Option<bool>,
    #[serde(rename = "pP")]
    pub price_protect: Option<bool>,
    #[serde(rename = "R")]
    pub reduce_only: Option<bool>,
    #[serde(rename = "tt")]
    pub trigger_time_ms: Option<i64>,
    #[serde(rename = "gtd")]
    pub good_till_date_ms: Option<i64>,
    #[serde(rename = "rm")]
    pub failed_reason: Option<String>,
    #[serde(rename = "ia")]
    pub activated: Option<bool>,
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

fn deserialize_optional_actual_order_id<'de, D>(
    deserializer: D,
) -> Result<Option<crate::binance_futures::BinanceActualOrderId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let parsed = match value {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Number(value)) => value
            .as_i64()
            .filter(|value| *value > 0)
            .map(crate::binance_futures::BinanceActualOrderId::Numeric),
        Some(serde_json::Value::String(value)) => {
            let value = value.trim();
            (!value.is_empty() && value != "0")
                .then(|| crate::binance_futures::BinanceActualOrderId::Text(value.to_string()))
        }
        Some(other) => {
            return Err(serde::de::Error::custom(format!(
                "actual order id must be positive or absent, got {other}"
            )))
        }
    };
    Ok(parsed)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceOrderTradeUpdate {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "t")]
    pub trade_id: Option<i64>,
    #[serde(rename = "T")]
    pub trade_time_ms: Option<i64>,
    #[serde(rename = "l")]
    pub last_filled_qty: Option<String>,
    #[serde(rename = "L")]
    pub last_filled_price: Option<String>,
    #[serde(rename = "z")]
    pub cumulative_filled_qty: Option<String>,
    // Commission (fee)
    #[serde(rename = "n")]
    pub commission: Option<String>,
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "rp")]
    pub realized_pnl: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Instant;

    use futures_util::SinkExt;
    use sqlx::Row;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tracing::field::{Field, Visit};
    use tracing::instrument::WithSubscriber;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context as LayerContext, Layer};
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Default)]
    struct FUserstreamR1Log(Arc<StdMutex<Vec<(u32, Instant)>>>);

    #[derive(Default)]
    struct FUserstreamR1Visitor {
        divergence_failures: Option<u32>,
    }

    impl Visit for FUserstreamR1Visitor {
        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "divergence_failures" {
                self.divergence_failures = u32::try_from(value).ok();
            }
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            if field.name() == "divergence_failures" {
                self.divergence_failures = u32::try_from(value).ok();
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "divergence_failures" {
                self.divergence_failures = format!("{value:?}").parse().ok();
            }
        }
    }

    impl<Registry> Layer<Registry> for FUserstreamR1Log
    where
        Registry: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, Registry>) {
            let mut visitor = FUserstreamR1Visitor::default();
            event.record(&mut visitor);
            if let Some(count) = visitor.divergence_failures {
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((count, Instant::now()));
            }
        }
    }

    impl FUserstreamR1Log {
        fn observations(&self) -> Vec<(u32, Instant)> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    #[derive(Debug)]
    struct FUserstreamR1State {
        ready: i64,
        phase: String,
        failure_reason: String,
    }

    fn f_userstream_r1_http_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn f_userstream_r1_rest_body(path: &str) -> &'static str {
        match path {
            "/fapi/v1/listenKey" => r#"{"listenKey":"f-userstream-r1"}"#,
            "/fapi/v1/userTrades" => "[]",
            "/fapi/v1/openOrders" => "[]",
            "/fapi/v1/openAlgoOrders" => {
                r#"[{"algoId":7000000001,"clientAlgoId":"f-r1-orphan","algoStatus":"NEW"}]"#
            }
            "/fapi/v2/positionRisk" | "/fapi/v2/balance" => "[]",
            _ => "{}",
        }
    }

    async fn spawn_f_userstream_r1_rest(
        requests: Arc<StdMutex<Vec<String>>>,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind F-R1 fake REST transport");
        let port = listener.local_addr().expect("F-R1 REST address").port();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut chunk = [0_u8; 2_048];
                    loop {
                        let Ok(read) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        raw.extend_from_slice(&chunk[..read]);
                        if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8_lossy(&raw);
                    let target = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let path = target.split('?').next().unwrap_or("/").to_string();
                    requests
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(path.clone());
                    let response = f_userstream_r1_http_response(f_userstream_r1_rest_body(&path));
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                });
            }
        });
        (port, task)
    }

    async fn spawn_f_userstream_r1_ws(
        handshakes: Arc<StdMutex<Vec<String>>>,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind F-R1 fake WebSocket transport");
        let port = listener.local_addr().expect("F-R1 WS address").port();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let handshakes = handshakes.clone();
                tokio::spawn(async move {
                    let target = Arc::new(StdMutex::new(String::new()));
                    let captured = target.clone();
                    let Ok(mut stream) = tokio_tungstenite::accept_hdr_async(
                        socket,
                        move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                              response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            *captured
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                request.uri().to_string();
                            Ok(response)
                        },
                    )
                    .await
                    else {
                        return;
                    };
                    handshakes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(
                            target
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .clone(),
                        );
                    loop {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        if stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                r#"{"e":"ACCOUNT_CONFIG_UPDATE","E":1,"T":1}"#.into(),
                            ))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        (port, task)
    }

    async fn f_userstream_r1_state(
        store: &Store,
        credential_id: &str,
    ) -> Option<FUserstreamR1State> {
        sqlx::query(
            "SELECT ready, phase, COALESCE(failure_reason, '') AS failure_reason \
             FROM fill_stream_state WHERE credential_id=?",
        )
        .bind(credential_id)
        .fetch_optional(store.pool())
        .await
        .expect("read F-R1 fill stream state")
        .map(|row| FUserstreamR1State {
            ready: row.get("ready"),
            phase: row.get("phase"),
            failure_reason: row.get("failure_reason"),
        })
    }

    async fn wait_for_f_userstream_r1_state(
        store: &Store,
        credential_id: &str,
        within: Duration,
        mut matches: impl FnMut(&FUserstreamR1State) -> bool,
    ) -> Option<FUserstreamR1State> {
        let deadline = Instant::now() + within;
        loop {
            let state = f_userstream_r1_state(store, credential_id).await;
            if state.as_ref().is_some_and(&mut matches) {
                return state;
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn start_f_userstream_r1_run_once(
        store: &Store,
        credential_id: &str,
    ) -> (
        tokio::task::JoinHandle<anyhow::Result<()>>,
        Arc<StdMutex<Vec<String>>>,
        Arc<StdMutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        Vec<crate::binance_futures::tests::TestEnvVar>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let handshakes = Arc::new(StdMutex::new(Vec::new()));
        let (rest_port, rest_task) = spawn_f_userstream_r1_rest(requests.clone()).await;
        let (ws_port, ws_task) = spawn_f_userstream_r1_ws(handshakes.clone()).await;
        let env = vec![
            crate::binance_futures::tests::TestEnvVar::set(
                "BINANCE_FUTURES_BASE_URL".to_string(),
                &format!("http://127.0.0.1:{rest_port}"),
            ),
            crate::binance_futures::tests::TestEnvVar::set(
                "BINANCE_FUTURES_WS_BASE_URL".to_string(),
                &format!("ws://127.0.0.1:{ws_port}"),
            ),
            crate::binance_futures::tests::TestEnvVar::unset(crate::venue::venue_env_name(
                credential_id,
            )),
            crate::binance_futures::tests::TestEnvVar::unset(
                crate::venue::FAULT_PROXY_REST_BASE_URL_ENV.to_string(),
            ),
            crate::binance_futures::tests::TestEnvVar::unset(
                crate::venue::FAULT_PROXY_WS_BASE_URL_ENV.to_string(),
            ),
            crate::binance_futures::tests::TestEnvVar::set(
                "RECONCILE_INTERVAL_MS".to_string(),
                "1000",
            ),
            crate::binance_futures::tests::TestEnvVar::set(
                "RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(),
                "10000",
            ),
            crate::binance_futures::tests::TestEnvVar::set(
                "RUSTCORE_BINANCE_WEIGHT_BURST".to_string(),
                "10000",
            ),
            crate::binance_futures::tests::TestEnvVar::set(
                "RUSTCORE_BINANCE_EXIT_WEIGHT_RESERVE".to_string(),
                "0",
            ),
        ];
        crate::fill_replay::register_fill_recovery_scope(
            store,
            credential_id,
            "BTCUSDT",
            crate::svc::now_ms(),
        )
        .await
        .expect("register F-R1 recovery scope");
        let task_credential_id = credential_id.to_string();
        let task_store = store.clone();
        let run = tokio::spawn(
            async move {
                run_once(
                    &task_credential_id,
                    BinanceKeys {
                        api_key: "f-r1-key".to_string(),
                        api_secret: "f-r1-secret".to_string(),
                    },
                    task_store.clone(),
                    BinanceEventSink::new(task_store),
                    CredentialRateLimiter::new(crate::rate_limiter::TokenBucketConfig::for_test(
                        10_000.0, 10_000.0,
                    )),
                    RecoveryTrigger::Reconnect,
                )
                .await
            }
            .with_current_subscriber(),
        );
        (run, requests, handshakes, rest_task, ws_task, env)
    }

    fn assert_f_userstream_r1_zero_recovery_requests(requests: &[String]) {
        for forbidden in [
            "/fapi/v1/userTrades",
            "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders",
            "/fapi/v2/positionRisk",
            "/fapi/v2/balance",
        ] {
            assert!(
                requests.iter().all(|request| request != forbidden),
                "divergence recheck must make zero {forbidden} requests: {requests:?}"
            );
        }
    }

    /// F-R1 P1：从生产 `run_once` 入口进入逻辑发散循环，观察调用点自己的
    /// `divergence_failures` 字段。旧纯函数用例只能证明加法器会递增；这里把
    /// 两处调用点改成 `divergence_failures = 1` 时，序列会直接退化为常数。
    #[test]
    fn p1_production_rechecks_report_a_monotonic_failure_count_and_fixed_cadence() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let captured = FUserstreamR1Log::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build F-R1 P1 runtime");

        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(25), async {
                    let (store, path) = reader_test_store("f-r1-p1").await;
                    let credential_id = format!("cred_{}", ulid::Ulid::new());
                    let (run, requests, handshakes, rest_task, ws_task, _env) =
                        start_f_userstream_r1_run_once(&store, &credential_id).await;
                    let divergence = match wait_for_f_userstream_r1_state(
                        &store,
                        &credential_id,
                        Duration::from_secs(10),
                        |state| {
                            state.phase == "reconcile_divergence"
                                && state.failure_reason.contains("has_diff=true")
                        },
                    )
                    .await {
                        Some(state) => state,
                        None => panic!(
                            "production entry must reach its divergence branch; state={:?}; requests={:?}",
                            f_userstream_r1_state(&store, &credential_id).await,
                            requests
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                        ),
                    };
                    assert_eq!(divergence.ready, 0);
                    assert_eq!(crate::reconcile::periodic_reconcile_interval_ms(), 1_000);
                    assert!(!run.is_finished(), "the production run exited before P1 rechecks");
                    assert_eq!(
                        handshakes
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .as_slice(),
                        ["/ws/f-userstream-r1"]
                    );

                    requests
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clear();
                    let divergent = crate::kill_switch::ReconcileTruth {
                        has_diff: true,
                        all_positions_zero: false,
                        all_open_orders_empty: false,
                        all_open_algo_orders_empty: false,
                        local_position_zero_diff: true,
                    };
                    crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
                        &store,
                        &credential_id,
                        crate::svc::now_ms().saturating_add(1),
                        divergent,
                    );
                    assert!(matches!(
                        periodic_divergence_recheck(
                            &store,
                            &credential_id,
                            crate::svc::now_ms().saturating_sub(1_000),
                        ),
                        PeriodicDivergenceRecheck::Diverged(_)
                    ));

                    let deadline = Instant::now() + Duration::from_secs(8);
                    while captured.observations().len() < 3 && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let observations = captured.observations();
                    let observed_counts = observations
                        .iter()
                        .map(|(count, _)| *count)
                        .collect::<Vec<_>>();
                    let final_state = f_userstream_r1_state(&store, &credential_id).await;
                    assert_eq!(
                        observed_counts.get(..3),
                        Some([2, 3, 4].as_slice()),
                        "the production call sites must monotonically count the initial failure plus three rechecks; run_finished={}; state={final_state:?}",
                        run.is_finished()
                    );
                    assert!(
                        observations[2].1.duration_since(observations[0].1)
                            >= Duration::from_millis(2_000),
                        "the third recheck must be at least two intervals after the first: {observations:?}"
                    );
                    assert_f_userstream_r1_zero_recovery_requests(
                        &requests
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()),
                    );

                    run.abort();
                    rest_task.abort();
                    ws_task.abort();
                    store.pool().close().await;
                    let _ = std::fs::remove_file(path);
                })
                .await
                .expect("P1 must turn red within its bounded outer timeout");
            });
        });
    }

    /// F-R1 P2：T 前的 Full 收敛真值不能穿过调用点的 `not_before_ms`。
    #[tokio::test]
    async fn p2_periodic_recheck_rejects_pre_divergence_truth_then_accepts_newer_full_truth() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (store, path) = reader_test_store("f-r1-p2").await;
            let credential_id = format!("cred_{}", ulid::Ulid::new());
            let observed_at_ms = 20_000_i64;
            let converged = crate::kill_switch::ReconcileTruth {
                has_diff: false,
                all_positions_zero: false,
                all_open_orders_empty: false,
                all_open_algo_orders_empty: false,
                local_position_zero_diff: true,
            };
            crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
                &store,
                &credential_id,
                observed_at_ms - 1,
                converged,
            );
            crate::fill_replay::begin_recovery_cycle(
                &store,
                &credential_id,
                RecoveryTrigger::Reconnect,
                observed_at_ms,
            )
            .await
            .expect("begin P2 recovery cycle");
            let generation = crate::fill_replay::load_fill_stream_state(&store, &credential_id)
                .await
                .expect("load P2 cycle")
                .expect("P2 cycle exists")
                .generation;
            assert!(record_recovery_divergence(
                &store,
                &credential_id,
                generation,
                "P2 divergence",
                observed_at_ms,
            )
            .await
            .expect("record P2 divergence"));

            assert_eq!(
                periodic_divergence_recheck(&store, &credential_id, observed_at_ms),
                PeriodicDivergenceRecheck::NoFreshFullTruth,
                "a converged Full truth from before T must not reopen the gate"
            );
            let state = f_userstream_r1_state(&store, &credential_id)
                .await
                .expect("P2 state remains present");
            assert_eq!(state.ready, 0);
            assert_eq!(state.phase, "reconcile_divergence");

            crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
                &store,
                &credential_id,
                observed_at_ms + 1,
                converged,
            );
            assert_eq!(
                periodic_divergence_recheck(&store, &credential_id, observed_at_ms),
                PeriodicDivergenceRecheck::Converged,
                "a converged Full truth newer than T must be consumable"
            );
            store.pool().close().await;
            let _ = std::fs::remove_file(path);
        })
        .await
        .expect("P2 must turn red within its bounded outer timeout");
    }

    /// F-R1 P3：reader 始终存活且没有第二次恢复请求；唯一能把同一 generation
    /// 推进到 ready 的事件，是复查消费到一条新的收敛 Full 真值。
    #[test]
    fn p3_recheck_convergence_reaches_finalize_without_reconnect_or_recovery_requests() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build F-R1 P3 runtime");
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(25), async {
                let (store, path) = reader_test_store("f-r1-p3").await;
                let credential_id = format!("cred_{}", ulid::Ulid::new());
                let (run, requests, handshakes, rest_task, ws_task, _env) =
                    start_f_userstream_r1_run_once(&store, &credential_id).await;
                let divergence = match wait_for_f_userstream_r1_state(
                    &store,
                    &credential_id,
                    Duration::from_secs(10),
                    |state| {
                        state.phase == "reconcile_divergence"
                            && state.failure_reason.contains("has_diff=true")
                    },
                )
                .await {
                    Some(state) => state,
                    None => panic!(
                        "production entry must record its initial divergence; state={:?}; requests={:?}",
                        f_userstream_r1_state(&store, &credential_id).await,
                        requests
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                    ),
                };
                assert_eq!(divergence.ready, 0);
                let generation = crate::fill_replay::load_fill_stream_state(
                    &store,
                    &credential_id,
                )
                .await
                .expect("load P3 divergence generation")
                .expect("P3 stream state exists")
                .generation;

                requests
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clear();
                crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
                    &store,
                    &credential_id,
                    crate::svc::now_ms().saturating_add(1),
                    crate::kill_switch::ReconcileTruth {
                        has_diff: false,
                        all_positions_zero: false,
                        all_open_orders_empty: false,
                        all_open_algo_orders_empty: false,
                        local_position_zero_diff: true,
                    },
                );

                let ready = wait_for_f_userstream_r1_state(
                    &store,
                    &credential_id,
                    Duration::from_secs(10),
                    |state| state.ready == 1,
                )
                .await
                .expect("fresh convergence must reach the existing finalize path");
                assert_eq!(ready.ready, 1);
                assert_eq!(ready.phase, "ready");
                assert_eq!(
                    crate::fill_replay::load_fill_stream_state(&store, &credential_id)
                        .await
                        .expect("reload P3 ready state")
                        .expect("P3 ready state exists")
                        .generation,
                    generation,
                    "periodic convergence must finalize the same recovery generation"
                );
                assert_eq!(
                    handshakes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .as_slice(),
                    ["/ws/f-userstream-r1"],
                    "the reader must stay on its original connection"
                );
                assert_f_userstream_r1_zero_recovery_requests(
                    &requests
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                );

                run.abort();
                rest_task.abort();
                ws_task.abort();
                store.pool().close().await;
                let _ = std::fs::remove_file(path);
            })
            .await
            .expect("P3 must turn red within its bounded outer timeout");
        });
    }

    #[test]
    fn user_stream_retry_climbs_from_the_reconnect_baseline_to_a_cap() {
        // 刚健康过：维持 2 秒重连。socket 重连只花一次权重 1 的 listenKey，
        // 不该为了省权重去拖慢它。
        let cap = 60_000;
        assert_eq!(
            user_stream_retry_delay_ms(0, cap),
            USER_STREAM_RETRY_BASE_MS
        );
        assert_eq!(user_stream_retry_delay_ms(1, cap), 4_000);
        assert_eq!(user_stream_retry_delay_ms(2, cap), 8_000);
        assert_eq!(user_stream_retry_delay_ms(4, cap), 32_000);
        // 上限存在：没有它，长时间不可达会把 fill recovery 退成小时级。
        assert_eq!(user_stream_retry_delay_ms(5, cap), cap);
        assert_eq!(user_stream_retry_delay_ms(64, cap), cap);
        assert_eq!(user_stream_retry_delay_ms(u32::MAX, cap), cap);
    }

    #[test]
    fn a_long_lived_stream_that_drops_reconnects_at_full_speed() {
        // 挂了很久之后正常断开，是 socket 的正常生命周期，不是抖动：
        // 计数清零，下一轮仍按基线重连。
        assert_eq!(next_short_cycle_count(7, USER_STREAM_HEALTHY_CYCLE_MS), 0);
        assert_eq!(next_short_cycle_count(7, 3_600_000), 0);
        assert_eq!(
            user_stream_retry_delay_ms(next_short_cycle_count(7, 3_600_000), 60_000),
            USER_STREAM_RETRY_BASE_MS
        );
    }

    #[test]
    fn a_flapping_stream_backs_off_even_though_every_cycle_returns_ok() {
        // 连上就掉走的是 Ok 分支。若判据用 Ok/Err，计数永不递增，于是每 2 秒
        // 付一次全量回补＋全量对账＝2850 权重/分钟，超过整个 per-IP 额度。
        let mut count = 0_u32;
        for _ in 0..6 {
            count = next_short_cycle_count(count, 800);
        }
        assert_eq!(count, 6);
        assert_eq!(user_stream_retry_delay_ms(count, 60_000), 60_000);
        // 恰好差一毫秒也算短命——阈值是闭区间的下界。
        assert_eq!(
            next_short_cycle_count(0, USER_STREAM_HEALTHY_CYCLE_MS - 1),
            1
        );
    }

    /// 上限**等于**周期对账间隔：流中断窗口永远不超过一次独立真值路径的节拍。
    #[test]
    fn the_retry_cap_is_the_periodic_reconcile_interval() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _interval = crate::binance_futures::tests::TestEnvVar::set(
            "RECONCILE_INTERVAL_MS".to_string(),
            "45000",
        );
        assert_eq!(user_stream_retry_cap_ms(), 45_000);
        assert_eq!(
            user_stream_retry_delay_ms(64, user_stream_retry_cap_ms()),
            45_000
        );
        // 上限不得低于基线，否则退避会把重连压得比健康重连还快。
        let _tiny = crate::binance_futures::tests::TestEnvVar::set(
            "RECONCILE_INTERVAL_MS".to_string(),
            "1000",
        );
        assert_eq!(user_stream_retry_cap_ms(), USER_STREAM_RETRY_BASE_MS);
    }

    /// **接线证明**：spawn 出去的恢复循环真的用了耦合上限，不是某个常量。
    #[test]
    fn the_recovery_loop_uses_the_coupled_cap() {
        let source = include_str!("binance_user_stream.rs");
        let body = source
            .split("let mut short_cycles: u32 = 0;")
            .nth(1)
            .expect("recovery loop follows the counter");
        let body = body
            .split("next_trigger = RecoveryTrigger::Reconnect;")
            .next()
            .expect("loop end");
        assert!(
            body.contains("let cap_ms = user_stream_retry_cap_ms();"),
            "cap must be read from the reconcile interval"
        );
        assert!(
            body.contains("let delay_ms = user_stream_retry_sleep_ms(")
                && body.contains("short_cycles,")
                && body.contains("cap_ms,"),
            "the production sleep, including jitter, must use that cap"
        );
    }

    #[test]
    fn the_short_cycle_counter_saturates_instead_of_wrapping() {
        // 回绕会把计数打回 0，也就是把退避悄悄取消。
        assert_eq!(next_short_cycle_count(u32::MAX, 0), u32::MAX);
    }

    #[test]
    fn saturated_backoff_plus_maximum_jitter_never_exceeds_the_final_cap() {
        let cap_ms = 30_000;
        assert_eq!(
            user_stream_retry_sleep_ms(u32::MAX, cap_ms, u64::MAX),
            cap_ms
        );
    }

    #[test]
    fn i2_five_divergence_rechecks_never_shorten_below_the_reconcile_interval() {
        let interval_ms = 30_000;
        let mut last_check_ms = 1_000;
        let mut fake_now_ms = 1_000;
        let mut failures = 0_u32;
        let mut checks = Vec::new();
        for _ in 0..5 {
            let next = next_divergence_recheck_at_ms(last_check_ms, fake_now_ms, interval_ms);
            assert!(
                next - fake_now_ms >= interval_ms as i64,
                "a logical divergence retry must wait a complete reconcile interval"
            );
            checks.push(next);
            failures = next_divergence_retry_count(failures);
            last_check_ms = next;
            // A long failed cycle used to reset the transport counter. It must
            // not reset or shorten this independent logical-divergence cadence.
            fake_now_ms = next + USER_STREAM_HEALTHY_CYCLE_MS as i64;
        }
        assert_eq!(checks, vec![31_000, 121_000, 211_000, 301_000, 391_000]);
        assert_eq!(failures, 5);
        assert!(
            checks
                .windows(2)
                .all(|pair| pair[1] - pair[0] >= interval_ms as i64),
            "five consecutive divergence failures must never contract: {checks:?}"
        );
    }

    #[test]
    fn i3_transport_retry_counter_keeps_its_existing_duration_semantics() {
        assert_eq!(next_short_cycle_count(4, 0), 5);
        assert_eq!(
            next_short_cycle_count(4, USER_STREAM_HEALTHY_CYCLE_MS - 1),
            5
        );
        assert_eq!(next_short_cycle_count(4, USER_STREAM_HEALTHY_CYCLE_MS), 0);
        assert_eq!(
            next_short_cycle_count(4, USER_STREAM_HEALTHY_CYCLE_MS + 1),
            0
        );
    }

    #[tokio::test]
    async fn i1_i6_five_divergence_rechecks_make_zero_recovery_requests() {
        let (store, path) = reader_test_store("nonflat-recheck-weight").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        let client = BinanceFuturesClient::new_with_mock_responses(&credential, Vec::new());
        let dirty = crate::kill_switch::ReconcileTruth {
            has_diff: true,
            all_positions_zero: false,
            all_open_orders_empty: false,
            all_open_algo_orders_empty: false,
            local_position_zero_diff: true,
        };
        for observed_at_ms in 101..=105 {
            crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
                &store,
                &credential,
                observed_at_ms,
                dirty,
            );
            assert!(matches!(
                periodic_divergence_recheck(&store, &credential, 100),
                PeriodicDivergenceRecheck::Diverged(_)
            ));
        }
        let requests = client.recorded_requests();
        for forbidden in [
            "/fapi/v1/userTrades",
            "/fapi/v1/openOrders",
            "/fapi/v1/openAlgoOrders",
            "/fapi/v2/positionRisk",
            "/fapi/v2/balance",
        ] {
            assert!(
                requests.iter().all(|request| !request.contains(forbidden)),
                "divergence rechecks must not call {forbidden}: {requests:?}"
            );
        }
        assert!(requests.is_empty());

        let full_recovery_weight = 100_u32;
        let old_user_stream_recovery_weight_per_minute = 14 * full_recovery_weight;
        let new_user_stream_recovery_weight_per_minute = 0_u32;
        assert_eq!(old_user_stream_recovery_weight_per_minute, 1_400);
        assert_eq!(new_user_stream_recovery_weight_per_minute, 0);
        assert!(
            new_user_stream_recovery_weight_per_minute <= 100,
            "logical retries themselves must stay within the <=100 budget"
        );

        let converged_non_flat = crate::kill_switch::ReconcileTruth {
            has_diff: false,
            ..dirty
        };
        crate::reconcile::remember_full_periodic_reconcile_truth_for_test(
            &store,
            &credential,
            106,
            converged_non_flat,
        );
        assert_eq!(
            periodic_divergence_recheck(&store, &credential, 100),
            PeriodicDivergenceRecheck::Converged
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn i1_production_divergence_branch_cannot_reenter_full_recovery() {
        let source = include_str!("binance_user_stream.rs");
        let branch = source
            .split("// Periodic divergence retries must not restart full recovery.")
            .nth(1)
            .expect("logical divergence retry branch marker")
            .split("// Recovery readiness sample:")
            .next()
            .expect("logical divergence retry branch end");
        assert!(branch.contains("periodic_divergence_recheck("));
        for forbidden in [
            "user_trades_after(",
            "reconcile_binance_futures_truth_with_client(",
            "reconcile_binance_futures_truth(",
        ] {
            assert!(
                !branch.contains(forbidden),
                "logical divergence retry re-entered an expensive recovery call: {forbidden}"
            );
        }
    }

    #[tokio::test]
    async fn i2_divergence_keeps_generation_and_cannot_overwrite_transport_failure() {
        let (store, path) = reader_test_store("divergence-generation-cas").await;
        let credential = format!("cred_{}", ulid::Ulid::new());
        crate::fill_replay::begin_recovery_cycle(
            &store,
            &credential,
            RecoveryTrigger::Reconnect,
            10,
        )
        .await
        .expect("begin recovery cycle");
        let generation = crate::fill_replay::load_fill_stream_state(&store, &credential)
            .await
            .expect("load cycle")
            .expect("cycle state")
            .generation;
        assert!(
            record_recovery_divergence(&store, &credential, generation, "dirty", 20)
                .await
                .expect("record logical divergence")
        );
        let state = crate::fill_replay::load_fill_stream_state(&store, &credential)
            .await
            .expect("load divergence")
            .expect("divergence state");
        assert_eq!(state.generation, generation);
        assert!(!state.ready);
        assert_eq!(state.phase, "reconcile_divergence");

        crate::fill_replay::record_recovery_failure(
            &store,
            &credential,
            "ws_auth",
            None,
            None,
            "transport failed",
            30,
        )
        .await
        .expect("record transport failure");
        assert!(
            !record_recovery_divergence(&store, &credential, generation, "stale dirty", 40)
                .await
                .expect("stale logical divergence CAS")
        );
        let state = crate::fill_replay::load_fill_stream_state(&store, &credential)
            .await
            .expect("load transport failure")
            .expect("transport state");
        assert_eq!(state.generation, generation + 1);
        assert_eq!(state.phase, "ws_auth");
        assert_eq!(state.failure_reason.as_deref(), Some("transport failed"));
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 起两条**永不结束**的任务，模拟 keepalive 与 reader。
    fn pending_pair() -> (
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        (
            tokio::spawn(async { futures_util::future::pending::<()>().await }),
            tokio::spawn(async {
                futures_util::future::pending::<()>().await;
                Ok(())
            }),
        )
    }

    /// **regression（存活变异 M8）· 任意提前返回都必须把两条任务一起收掉。**
    ///
    /// 这条驱动的是 `run_once` 里那个**结构化所有权**本体：函数体内构造
    /// `UserStreamTasks`，随后走一条**裸 `?` 的提前返回**——与生产里
    /// backfill / cursor / reconcile 那一串 `?;` 同形。返回之后两条任务
    /// 都必须已经被 abort。
    ///
    /// R8 只给 keepalive 挂了 guard，reader 是裸 handle：同样的提前返回会把
    /// reader 留在后台继续读那条 socket、继续喂 sink，而外层已经在重连了。
    /// 把 `Drop` 里的 `reader.abort()` 删掉（M8 的变异形状），这条用例变红。
    #[tokio::test]
    async fn an_early_return_aborts_both_stream_tasks() {
        let (keepalive, reader) = pending_pair();
        let keepalive_probe = keepalive.abort_handle();
        let reader_probe = reader.abort_handle();

        async fn body(
            keepalive: tokio::task::JoinHandle<()>,
            reader: tokio::task::JoinHandle<anyhow::Result<()>>,
        ) -> anyhow::Result<()> {
            let _tasks = UserStreamTasks::new(keepalive, reader);
            // 生产里这一句是 `fill_replay::load_fill_replay_cursor(...).await?`
            // 之类的裸 `?`——它**跳过**函数末尾所有显式的 abort。
            Err::<(), _>(anyhow!("backfill failed")).map_err(anyhow::Error::from)?;
            unreachable!("the early return above owns this test")
        }

        let outcome = body(keepalive, reader).await;
        assert!(outcome.is_err(), "the test drives the early-return path");

        // `abort_handle` 只是观察句柄，不影响所有权。
        for _ in 0..200 {
            if keepalive_probe.is_finished() && reader_probe.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            keepalive_probe.is_finished(),
            "keepalive must not outlive the early return"
        );
        assert!(
            reader_probe.is_finished(),
            "the reader must not be detached by an early return"
        );
    }

    /// **join 出口**：把 reader 交出去 join 的**同一步**必须先收掉 keepalive，
    /// 而不是等整个 join 结束后靠 `Drop` 兜底。
    ///
    /// 差别是可观察的：reader 可以挂很久（生产里它就是一条长连接的读循环），
    /// 那段时间里 keepalive 还在按 30 分钟节奏拿着 client 打 listenKey。
    /// 因此这条用例在 **join 尚未返回**的时刻断言 keepalive 已经停了——
    /// 把 `join_reader` 里那句 abort 删掉（靠 Drop 兜底）会让断言变红。
    #[tokio::test]
    async fn joining_the_reader_stops_the_keepalive_before_the_join_returns() {
        let keepalive = tokio::spawn(async { futures_util::future::pending::<()>().await });
        let keepalive_probe = keepalive.abort_handle();
        let reader = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(800)).await;
            Ok::<(), anyhow::Error>(())
        });

        let tasks = UserStreamTasks::new(keepalive, reader);
        let join = tokio::spawn(async move { tasks.join_reader().await });

        let mut keepalive_stopped_while_joining = false;
        for _ in 0..40 {
            if keepalive_probe.is_finished() {
                keepalive_stopped_while_joining = !join.is_finished();
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            keepalive_stopped_while_joining,
            "the keepalive must be stopped by `join_reader` itself, not by the later Drop"
        );

        let joined = join
            .await
            .expect("join task")
            .expect("reader joins cleanly");
        assert!(joined.is_ok(), "the reader returned its own Ok result");
    }

    /// **regression（Medium）· 取消窗口：外层被取消时 reader 不得被分离。**
    ///
    /// 评审反例实跑：`join_reader` 先 `take()` 出 reader 再 await，
    /// 外层 join 被取消后 **reader 仍在运行**——因为 drop 一个 `JoinHandle`
    /// 是分离任务而不是取消它，而 handle 已经不在 `self` 里，
    /// `Drop` 的 abort 够不到它。生产形态就是 `run_once` 的调用点被
    /// `tokio::select!` / `timeout` / 外层 abort 取消的那一刻。
    ///
    /// 断言方式是**行为**而不是 API：reader 每 5ms 给计数器加一，
    /// 取消之后计数必须停住（分离的任务会继续加）。
    #[tokio::test]
    async fn cancelling_the_join_still_aborts_the_reader() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let ticks = Arc::new(AtomicU64::new(0));
        let reader_ticks = ticks.clone();
        let keepalive = tokio::spawn(async { futures_util::future::pending::<()>().await });
        let reader = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                reader_ticks.fetch_add(1, Ordering::SeqCst);
            }
        });

        let tasks = UserStreamTasks::new(keepalive, reader);
        // 外层被取消：`timeout` 到期会 drop 这个 future，与 `select!` 另一条臂
        // 先完成、或整个任务被 abort 是同一件事。
        let cancelled = tokio::time::timeout(Duration::from_millis(60), tasks.join_reader()).await;
        assert!(cancelled.is_err(), "the join must really be cancelled here");

        // 取消之前它确实在跑（否则"停住"是废话）。
        assert!(
            ticks.load(Ordering::SeqCst) > 0,
            "the reader must have been running before the cancellation"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        let after_cancel = ticks.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            after_cancel,
            "a cancelled join must leave the reader aborted, not detached and still reading"
        );
    }

    /// **放行面自证**：只要所有者还活着，两条任务都不得被收掉。
    /// 没有这条，"构造即 abort" 的实现也能让上面两条全绿。
    #[tokio::test]
    async fn tasks_keep_running_while_the_owner_is_alive() {
        let (keepalive, reader) = pending_pair();
        let tasks = UserStreamTasks::new(keepalive, reader);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!tasks.reader_finished(), "the reader must still be running");
        drop(tasks);
    }

    /// `reader_finished` 是 `run_once` 判 stream_phase 的依据，必须真的反映
    /// reader 是否已经结束。
    #[tokio::test]
    async fn reader_finished_reflects_a_dead_reader() {
        let keepalive = tokio::spawn(async { futures_util::future::pending::<()>().await });
        let reader = tokio::spawn(async { Ok::<(), anyhow::Error>(()) });
        let probe_keepalive = keepalive.abort_handle();
        let tasks = UserStreamTasks::new(keepalive, reader);
        for _ in 0..200 {
            if tasks.reader_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(tasks.reader_finished());
        drop(tasks);
        let _ = probe_keepalive;
    }

    #[test]
    fn a7_algo_update_is_not_parsed_as_order_trade_update() {
        let payload = r#"{
            "e":"ALGO_UPDATE",
            "E":1750515742303,
            "T":1750515742297,
            "o":{
                "caid":"algo-client-7",
                "aid":7000000001,
                "at":"CONDITIONAL",
                "o":"STOP_MARKET",
                "s":"BTCUSDT",
                "S":"SELL",
                "ps":"BOTH",
                "f":"GTE_GTC",
                "q":"0.001",
                "X":"NEW",
                "tp":"95000.25",
                "p":"0",
                "V":"NONE",
                "wt":"MARK_PRICE",
                "pm":"NONE",
                "cp":true,
                "pP":true,
                "R":true,
                "tt":0,
                "gtd":0
            }
        }"#;
        let envelope: BinanceWsEnvelope =
            serde_json::from_str(payload).expect("deserialize ALGO_UPDATE envelope");
        assert_eq!(envelope.event_type, "ALGO_UPDATE");
        assert!(
            envelope.order.is_none(),
            "ALGO_UPDATE must not enter the ORDER_TRADE_UPDATE payload slot"
        );
        let algo = envelope
            .algo_update
            .expect("ALGO_UPDATE must use its dedicated payload type");
        assert_eq!(
            algo.algo_id,
            Some(crate::binance_futures::BinanceAlgoId::new(7000000001))
        );
        assert_eq!(
            algo.client_algo_id
                .as_ref()
                .map(crate::binance_futures::BinanceClientAlgoId::as_str),
            Some("algo-client-7")
        );
        assert_eq!(algo.algo_status.as_deref(), Some("NEW"));
        assert_eq!(algo.actual_order_id, None);
    }

    #[test]
    fn ordinary_and_unknown_event_handling_is_preserved() {
        let ordinary: BinanceWsEnvelope = serde_json::from_str(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1,"T":2,"o":{"s":"BTCUSDT","c":"ordinary-client","i":19,"x":"NEW","X":"NEW"}}"#,
        )
        .expect("deserialize ordinary order update");
        assert!(ordinary.order.is_some());
        assert!(ordinary.algo_update.is_none());

        let unknown: BinanceWsEnvelope =
            serde_json::from_str(r#"{"e":"FUTURE_UNKNOWN_EVENT","E":3,"o":{"unexpected":true}}"#)
                .expect("unknown event remains ignorable");
        assert!(unknown.order.is_none());
        assert!(unknown.algo_update.is_none());
    }

    /// **reader 的帧消费连续性。**
    ///
    /// 评审变异 R7-06（"第一条可解析 Text 之后正常结束、不消费 sink"）8/8
    /// 存活——整套用例里没有任何一条驱动过这个循环本体。
    ///
    /// 这里喂一串真实 `Message` 进**生产的**帧循环
    /// （`run_user_stream_reader_loop`，`run_once` 里 spawn 的就是它），
    /// 断言每一帧都真的到达了 sink：三条 `ORDER_TRADE_UPDATE` 分别命中三张
    /// 本地单，跑完之后三张都必须被推进。少消费任何一帧都会红。
    #[tokio::test]
    async fn the_reader_loop_delivers_every_frame_to_the_sink() {
        let (store, path) = reader_test_store("frames").await;
        for (engine_order_id, exchange_order_id) in
            [("eng_f1", "8001"), ("eng_f2", "8002"), ("eng_f3", "8003")]
        {
            seed_reader_order(&store, engine_order_id, exchange_order_id).await;
        }

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Text(order_frame(8001).into())),
            // 中间夹一条**已知集合之外、但解析得动**的事件帧：这才是真正的
            // 前向兼容路径——`BinanceWsEnvelope` 只强制一个 `"e"`，交易所
            // 新增的事件类型解析成功、由 `handle_ws_envelope` 的闭世界分发
            // 跳过并留 warn，一个字节都没丢，消费也不中断。
            //
            // （regression：**解析失败**的帧无论事件名叫什么都
            // 一律 fail-closed，见
            // `a_malformed_frame_outside_the_allowlist_ends_the_stream`；
            // R16 这里放的是一条畸形帧，那条现在会终止这条流。）
            Ok(Message::Text(
                r#"{"e":"FUTURE_UNKNOWN_EVENT","E":1,"T":2}"#.into(),
            )),
            Ok(Message::Text(order_frame(8002).into())),
            Ok(Message::Text(order_frame(8003).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect("a stream that ends normally is not a failure");

        for engine_order_id in ["eng_f1", "eng_f2", "eng_f3"] {
            assert_eq!(
                reader_status_of(&store, engine_order_id).await,
                "filled",
                "{engine_order_id} never received its frame; the loop stopped consuming"
            );
        }
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **regression（High）· sink 落库失败必须降 readiness 并断流。**
    ///
    /// R10 的 reader 拿到的是 `()`：写失败之后它继续读下一帧、不重连、
    /// 不降 readiness，而 `ready=1` 正是新开仓的放行条件。
    #[tokio::test]
    async fn a_sink_write_failure_drops_readiness_and_ends_the_stream() {
        let (store, path) = reader_test_store("apply-fail").await;
        seed_reader_order(&store, "eng_f1", "8001").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Startup,
            1,
        )
        .await
        .expect("begin recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            &store,
            READER_CREDENTIAL,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            cycle,
            2,
        )
        .await
        .expect("mark the stream ready");
        assert!(
            crate::fill_replay::load_fill_stream_state(&store, READER_CREDENTIAL)
                .await
                .expect("load stream state")
                .expect("stream state exists")
                .ready,
            "the fixture must start from a ready stream, otherwise the assertion is vacuous"
        );

        // 真故障：一个让状态写必然 ABORT 的 SQLite 触发器。
        sqlx::query(
            "CREATE TRIGGER r11_block_reader_status BEFORE UPDATE ON engine_orders \
             FOR EACH ROW WHEN NEW.status = 'filled' \
             BEGIN SELECT RAISE(ABORT, 'injected reader write failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        // 第二帧属于另一张单：它**不得**被消费——流在第一帧就该结束。
        seed_reader_order(&store, "eng_f2", "8002").await;
        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Text(order_frame(8001).into())),
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a frame that could not be applied must fail the stream");
        assert!(error.to_string().contains("could not apply a frame"));

        let state = crate::fill_replay::load_fill_stream_state(&store, READER_CREDENTIAL)
            .await
            .expect("load stream state")
            .expect("stream state exists");
        assert!(
            !state.ready,
            "a stream that lost an exchange truth must not keep admitting new risk"
        );
        assert_eq!(state.phase, "ws_apply");
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "acked",
            "the stream must stop at the failed frame, not keep consuming"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 把一条流推到 `ready=1`，供"解析失败是否降 readiness"的用例做起点。
    async fn make_reader_stream_ready(store: &Store) {
        let cycle = crate::fill_replay::begin_recovery_cycle(
            store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Startup,
            1,
        )
        .await
        .expect("begin recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            store,
            READER_CREDENTIAL,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            cycle,
            2,
        )
        .await
        .expect("mark the stream ready");
        assert!(
            crate::fill_replay::load_fill_stream_state(store, READER_CREDENTIAL)
                .await
                .expect("load stream state")
                .expect("stream state exists")
                .ready,
            "the fixture must start from a ready stream, otherwise the assertion is vacuous"
        );
    }

    async fn reader_stream_state(store: &Store) -> crate::fill_replay::FillStreamState {
        crate::fill_replay::load_fill_stream_state(store, READER_CREDENTIAL)
            .await
            .expect("load stream state")
            .expect("stream state exists")
    }

    /// **已知事件解析失败必须降
    /// readiness 并断流。**
    ///
    /// R15 的 Text 分支是 `if let Ok(env) = ...` 且**没有 else**：一条缺必填
    /// 字段的 `ORDER_TRADE_UPDATE`（这里缺 `i`/`x`/`X`）解析失败之后 reader
    /// 一声不响读下一帧——那一帧承载的成交/终态真值就此丢失，而 `ready`
    /// 仍然为 1、新开仓照样放行。
    ///
    /// 这条用例喂的是**生产的**帧循环（`run_once` 里 spawn 的就是它），
    /// 观察三件事：流以失败返回、`fill_stream_state` 变成 `ready=0` 且
    /// `phase='ws_parse'`、**后面那一帧不再被消费**。
    #[tokio::test]
    async fn a_truth_bearing_frame_that_cannot_be_parsed_drops_readiness_and_ends_the_stream() {
        let (store, path) = reader_test_store("parse-fail-known").await;
        make_reader_stream_ready(&store).await;
        seed_reader_order(&store, "eng_p2", "8102").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            // 已知事件，但 `o` 里缺了 `i` / `x` / `X` 三个必填字段。
            Ok(Message::Text(
                r#"{"e":"ORDER_TRADE_UPDATE","E":2,"T":3,"o":{"s":"BTCUSDT","c":""}}"#.into(),
            )),
            Ok(Message::Text(order_frame(8102).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a truth-bearing frame that cannot be parsed must fail the stream");
        assert!(
            error.to_string().contains("truth-bearing event type"),
            "the failure must name the class of the lost frame: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a stream that lost an exchange truth must not keep admitting new risk"
        );
        assert_eq!(state.phase, "ws_parse");
        assert_eq!(
            reader_status_of(&store, "eng_p2").await,
            "acked",
            "the stream must stop at the unparsable frame, not keep consuming"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **无法归类的帧同样按真值丢失处置。**
    ///
    /// 连 `"e"` 都读不出来时我们**无从证明**它不是一条已知事件；把它当成
    /// "没什么可做的"就是把缺失数据转换成"正常"（engine safety contract）。
    #[tokio::test]
    async fn a_frame_without_a_readable_event_type_ends_the_stream() {
        let (store, path) = reader_test_store("parse-fail-opaque").await;
        make_reader_stream_ready(&store).await;
        seed_reader_order(&store, "eng_p3", "8103").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Text("not json".into())),
            Ok(Message::Text(order_frame(8103).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("an unclassifiable frame must fail the stream");
        assert!(
            error.to_string().contains("no readable event type"),
            "the failure must say the frame could not be classified: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(!state.ready);
        assert_eq!(state.phase, "ws_parse");
        assert_eq!(
            reader_status_of(&store, "eng_p3").await,
            "acked",
            "the stream must stop at the unclassifiable frame"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **allowlist 之外的畸形帧
    /// 同样必须终止这条流。**
    ///
    /// R16 的用例把反例钉成了绿：`{"e":"listenKeyExpired","E":"not-a-number"}`
    /// 被跳过、`ready` 保持 1、后一帧照常消费。可这条帧的语义正是
    /// **"listenKey 失效、这条流已经死了"**——它被静默吞掉之后，引擎会一直
    /// 拿着 `ready=1` 放行新开仓，直到 idle deadline 才发现流早就不推事件了。
    ///
    /// 判据换成"任何解析不了的帧一律 fail-closed"之后，这条用例的方向翻过来：
    /// 同一帧必须写下 `ws_parse` 的 recovery failure、`ready=0`、整条流失败返回，
    /// 并且**后一帧不得被消费**（那张本地单停在 `acked`）。
    ///
    /// 失败理由里必须带着这一帧的归类（`unknown event type listenKeyExpired`）
    /// ——fail-closed 不等于放弃说明它长什么样。
    #[tokio::test]
    async fn a_malformed_frame_outside_the_allowlist_ends_the_stream() {
        let (store, path) = reader_test_store("parse-fail-unknown").await;
        make_reader_stream_ready(&store).await;
        seed_reader_order(&store, "eng_p4", "8104").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            // 已知集合之外，且 `E` 类型不符 ⇒ 反序列化失败。
            Ok(Message::Text(
                r#"{"e":"listenKeyExpired","E":"not-a-number"}"#.into(),
            )),
            Ok(Message::Text(order_frame(8104).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("an unparsable frame must fail the stream whatever its event name says");
        assert!(
            error
                .to_string()
                .contains("unknown event type listenKeyExpired"),
            "the failure must still describe the frame: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a frame we could not parse must drop readiness even when its event name is \
             outside the truth-bearing set"
        );
        assert_eq!(state.phase, "ws_parse");
        assert_eq!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("unknown event type listenKeyExpired"),
            true,
            "the recorded reason must name the frame class: {state:?}"
        );
        assert_eq!(
            reader_status_of(&store, "eng_p4").await,
            "acked",
            "the stream must stop at the unparsable frame, not read past it"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **reader 的 parse failure 不得被
    /// 主干那次陈旧采样覆盖成 ready。**
    ///
    /// 交错完全按 `run_once`：
    ///
    /// 1. `begin_recovery_cycle` 拿到本周期的号（主干持有它）；
    /// 2. **生产的** `run_user_stream_reader_loop` 读到一帧解析不了的真值，
    ///    写下 `ready=0 / phase=ws_parse` 并失败返回（reader 任务的行为）；
    /// 3. 主干这时才走到 `finalize_recovery_readiness`——它手里的三段判据
    ///    是**更早**采样的 `Succeeded`（`reader_finished()` 那一刻 reader 还活着）。
    ///
    /// R16 的写入没有谓词，第 3 步会把第 2 步整行盖掉：`ready=1`、
    /// `phase='ready'`、`failure_reason=NULL`——一条已经与交易所脱节的流
    /// 重新放行新开仓，库里连失败痕迹都不剩。绑代之后第 3 步必须
    /// `PreserveNotReady`。
    #[tokio::test]
    async fn a_reader_parse_failure_survives_the_main_lines_stale_ready_write() {
        let (store, path) = reader_test_store("stale-ready").await;
        seed_reader_order(&store, "eng_p5", "8105").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Reconnect,
            1,
        )
        .await
        .expect("begin the recovery cycle the main line holds");

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![Ok(
            Message::Text(r#"{"e":"ORDER_TRADE_UPDATE","E":"not-a-number"}"#.into()),
        )];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("the reader must fail on a truth-bearing frame it cannot parse");

        // 主干手里的三段判据是**更早**采样的结果。
        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                READER_CREDENTIAL,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                9,
            )
            .await
            .expect("a superseded cycle is not an error"),
            crate::fill_replay::ReadinessWrite::PreserveNotReady,
            "a cycle whose reader already failed must not be able to declare readiness"
        );
        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "the reader's ready=0 must survive the main line's stale write: {state:?}"
        );
        assert_eq!(state.phase, "ws_parse");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("could not be parsed"),
            "the parse failure reason must survive: {state:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// 归类判据本身的三个方向（判据与 reader 循环共用同一份）。
    #[test]
    fn unparsed_frames_are_classified_by_their_event_type() {
        // 闭世界本身也要被钉住：集合悄悄少一类 = 那一类的真值丢失重新变成静默。
        // 三类与 `BinanceEventSink::handle_ws_envelope` 的三条分发分支一一对应
        // （分发侧各自有既有用例：`the_reader_loop_delivers_every_frame_to_the_sink`
        // 与 `healthy_algo_and_account_frames_keep_the_stream_ready`）。
        assert_eq!(
            TRUTH_BEARING_WS_EVENT_TYPES,
            ["ORDER_TRADE_UPDATE", "ALGO_UPDATE", "ACCOUNT_UPDATE"]
        );
        for event_type in TRUTH_BEARING_WS_EVENT_TYPES {
            let frame = format!(r#"{{"e":"{event_type}","E":"not-a-number"}}"#);
            assert_eq!(
                classify_unparsed_ws_frame(&frame),
                UnparsedWsFrameClass::TruthBearing(event_type.to_string()),
                "{event_type} carries exchange truth"
            );
        }
        assert_eq!(
            classify_unparsed_ws_frame(r#"{"e":"listenKeyExpired","E":"x"}"#),
            UnparsedWsFrameClass::ForwardCompatible("listenKeyExpired".to_string())
        );
        for opaque in [
            "not json",
            "[1,2,3]",
            r#"{"E":1}"#,
            r#"{"e":123}"#,
            r#"{"e":"   "}"#,
        ] {
            assert_eq!(
                classify_unparsed_ws_frame(opaque),
                UnparsedWsFrameClass::Unclassifiable,
                "{opaque} has no readable event type"
            );
        }
        // 归一化与 `handle_ws_envelope` 的 `trim` 同源。
        assert_eq!(
            classify_unparsed_ws_frame(r#"{"e":" ORDER_TRADE_UPDATE ","E":"x"}"#),
            UnparsedWsFrameClass::TruthBearing("ORDER_TRADE_UPDATE".to_string())
        );
    }

    const READER_CREDENTIAL: &str = "cred_r11reader";

    async fn reader_test_store(tag: &str) -> (Store, std::path::PathBuf) {
        sqlx::any::install_default_drivers();
        let path =
            std::env::temp_dir().join(format!("rustcore-r11rd-{tag}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn seed_reader_order(store: &Store, engine_order_id: &str, exchange_order_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO engine_orders (
              engine_order_id, credential_id, bot_uid, idempotency_key,
              exchange, market_kind, symbol_key, action, side, order_type,
              quantity, reduce_only, order_role, status, exchange_order_id,
              client_order_id_short, created_at_ms, updated_at_ms
            ) VALUES (?, ?, 'bot_r11rd', ?, 'binance', 'futures',
              'binance|futures|BTC|USDT|USDT', 'open_long', 'long', 'market',
              '1', 0, 'entry', 'acked', ?, ?, 1, 1)
            "#,
        )
        .bind(engine_order_id)
        .bind(READER_CREDENTIAL)
        .bind(format!("idem-{engine_order_id}"))
        .bind(exchange_order_id)
        .bind(format!("coid-{engine_order_id}"))
        .execute(store.pool())
        .await
        .expect("seed engine order");
    }

    fn order_frame(order_id: i64) -> String {
        serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 2,
            "T": 3,
            "o": { "s": "BTCUSDT", "c": "", "i": order_id, "x": "NEW", "X": "FILLED" }
        })
        .to_string()
    }

    async fn reader_status_of(store: &Store, engine_order_id: &str) -> String {
        use sqlx::Row;
        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id = ?")
            .bind(engine_order_id)
            .fetch_one(store.pool())
            .await
            .expect("read order status")
            .get::<String, _>("status")
    }

    #[test]
    fn b10_algo_update_blank_status_and_actual_id_remain_absent() {
        let envelope: BinanceWsEnvelope = serde_json::from_str(
            r#"{"e":"ALGO_UPDATE","E":1,"T":2,"o":{"caid":"algo-blank","aid":44,"X":"","ai":""}}"#,
        )
        .expect("deserialize sparse ALGO_UPDATE");
        let algo = envelope.algo_update.expect("dedicated algo payload");
        assert_eq!(algo.algo_status, None);
        assert_eq!(algo.actual_order_id, None);
    }

    // ==================================================================
    // 三类 frame 的失败传播，全部经生产 reader
    // ==================================================================

    fn algo_frame(algo_id: i64) -> String {
        serde_json::json!({
            "e": "ALGO_UPDATE",
            "E": 2,
            "T": 3,
            "o": { "caid": "algo-client-r12", "aid": algo_id, "X": "NEW" }
        })
        .to_string()
    }

    fn account_frame(symbol: &str) -> String {
        serde_json::json!({
            "e": "ACCOUNT_UPDATE",
            "E": 2,
            "T": 3,
            "a": { "P": [{ "s": symbol, "pa": "1", "ps": "LONG", "ep": "100" }] }
        })
        .to_string()
    }

    /// 已 ready 的流 + 一条**在第一帧之后**的 ORDER 帧，用来观察
    /// "reader 有没有在失败那一帧停下"。
    async fn ready_reader_fixture(tag: &str) -> (Store, std::path::PathBuf) {
        let (store, path) = reader_test_store(tag).await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Startup,
            1,
        )
        .await
        .expect("begin recovery cycle");
        crate::fill_replay::finalize_recovery_readiness(
            &store,
            READER_CREDENTIAL,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            RecoveryPhaseResult::Succeeded,
            cycle,
            2,
        )
        .await
        .expect("mark the stream ready");
        assert!(
            crate::fill_replay::load_fill_stream_state(&store, READER_CREDENTIAL)
                .await
                .expect("load stream state")
                .expect("stream state exists")
                .ready,
            "the fixture must start from a ready stream, otherwise the assertion is vacuous"
        );
        seed_reader_order(&store, "eng_f2", "8002").await;
        (store, path)
    }

    async fn assert_frame_failure_drops_readiness(store: &Store, frames: Vec<Message>) {
        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            frames.into_iter().map(Ok).collect();
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a frame that could not be applied must fail the stream");
        assert!(error.to_string().contains("could not apply a frame"));

        let state = crate::fill_replay::load_fill_stream_state(store, READER_CREDENTIAL)
            .await
            .expect("load stream state")
            .expect("stream state exists");
        assert!(
            !state.ready,
            "a stream that lost an exchange truth must not keep admitting new risk"
        );
        assert_eq!(state.phase, "ws_apply");
        assert_eq!(
            reader_status_of(store, "eng_f2").await,
            "acked",
            "the stream must stop at the failed frame, not keep consuming"
        );
    }

    /// **第八轮 §9.5（High）· ALGO_UPDATE 的落库失败同样必须传播。**
    ///
    /// R11 只把 `ORDER_TRADE_UPDATE` 的失败上抛，`handle_algo_update` 仍返回
    /// `()`（R11R §7.6 如实登记为批外项）。保护单的交易所真值写不进去之后，
    /// reader 照读下一帧、`ready` 仍是 1——而"保护单到底还在不在"正是
    /// engine safety contract 的那条红线。
    #[tokio::test]
    async fn an_algo_frame_that_cannot_be_applied_drops_readiness_and_ends_the_stream() {
        let (store, path) = ready_reader_fixture("algo-fail").await;
        // 真故障：保护单表整张不见了，身份查询必然报错。
        sqlx::query("DROP TABLE protection_orders")
            .execute(store.pool())
            .await
            .expect("drop protection_orders to make the algo lookup fail");

        assert_frame_failure_drops_readiness(
            &store,
            vec![
                Message::Text(algo_frame(9001).into()),
                Message::Text(order_frame(8002).into()),
            ],
        )
        .await;
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **第八轮 §9.5（High）· ACCOUNT_UPDATE 的落库失败同样必须传播。**
    ///
    /// 这一帧携带的是**仓位与余额的交易所真值**。R11 的
    /// `handle_account_update` 三处（begin / write / commit）全部返回 `()`：
    /// 仓位真值丢了，`ready` 却还是 1。
    #[tokio::test]
    async fn an_account_frame_that_cannot_be_applied_drops_readiness_and_ends_the_stream() {
        let (store, path) = ready_reader_fixture("account-fail").await;
        // 真故障：审计事件写入被触发器 ABORT，整帧回滚。
        sqlx::query(
            "CREATE TRIGGER r12_block_account_event BEFORE INSERT ON engine_order_events \
             FOR EACH ROW WHEN NEW.event_type = 'account_update' \
             BEGIN SELECT RAISE(ABORT, 'injected account update failure'); END",
        )
        .execute(store.pool())
        .await
        .expect("install injected failure trigger");

        assert_frame_failure_drops_readiness(
            &store,
            vec![
                Message::Text(account_frame("BTCUSDT").into()),
                Message::Text(order_frame(8002).into()),
            ],
        )
        .await;
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **放行面（反向断言）**：三类 frame 在**能落库**时都不降 readiness，
    /// 且循环继续消费后面的帧。没有它，上面两条用"ALGO/ACCOUNT 一律失败"
    /// 也能全绿。
    ///
    /// **这条流必须保持「还活着」。**
    ///
    /// R17 这里喂的是一个 `stream::iter`：三帧消费完之后它**结束**，而
    /// 「EOF ⇒ 降 readiness」正是本轮要修的判据。用一条会结束的流断言
    /// `ready` 仍是 1，等于把"一条已经断掉的流照样 ready"钉成绿。
    /// 现在三帧之后接 `stream::pending()`——流**没有结束**，循环还在等下一帧，
    /// 断言问的于是是它真正要问的那件事：**帧本身**落库成功时 readiness
    /// 不受影响。idle deadline 取得远大于观察窗口，免得把 IdleTimeout
    /// 混进这条判据。
    #[tokio::test]
    async fn healthy_algo_and_account_frames_keep_the_stream_ready() {
        let (store, path) = ready_reader_fixture("three-classes-ok").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Text(algo_frame(9001).into())),
            Ok(Message::Text(account_frame("BTCUSDT").into())),
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames).chain(futures_util::stream::pending());
        let mut write: Vec<Message> = Vec::new();
        let still_reading = tokio::time::timeout(
            Duration::from_millis(400),
            run_user_stream_reader_loop(
                &store,
                READER_CREDENTIAL,
                &BinanceEventSink::new(store.clone()),
                &mut read,
                &mut write,
                Duration::from_secs(600),
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            still_reading.is_err(),
            "a stream that has not ended must leave the reader loop waiting for the next              frame; it returned {still_reading:?}"
        );

        assert!(
            crate::fill_replay::load_fill_stream_state(&store, READER_CREDENTIAL)
                .await
                .expect("load stream state")
                .expect("stream state exists")
                .ready,
            "applying three healthy frames must not drop readiness"
        );
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "filled",
            "the loop must keep consuming past ALGO and ACCOUNT frames"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
    // ---------------------------------------------------------------------
    // 第十四轮 §9.4「WS 全 frame fail-closed」
    // ---------------------------------------------------------------------

    /// **Binary 帧承载得动真值时必须被真的消费**（regression 的正向面）。
    ///
    /// 处置取的是「按同一 envelope 解析」那一支，因此这条断言不是可有可无的
    /// 装饰：少了它，把 Binary 一律写成"记 failure 然后断流"也能让下面两条
    /// 负向用例全绿——而那等于把一条**读得懂**的交易所真值扔掉并强制重连。
    #[tokio::test]
    async fn a_binary_frame_carrying_a_truth_event_is_applied_like_a_text_frame() {
        let (store, path) = ready_reader_fixture("binary-ok").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Binary(order_frame(8002).into_bytes()))];
        let mut read = futures_util::stream::iter(frames).chain(futures_util::stream::pending());
        let mut write: Vec<Message> = Vec::new();
        let still_reading = tokio::time::timeout(
            Duration::from_millis(400),
            run_user_stream_reader_loop(
                &store,
                READER_CREDENTIAL,
                &BinanceEventSink::new(store.clone()),
                &mut read,
                &mut write,
                Duration::from_secs(600),
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            still_reading.is_err(),
            "a binary frame the engine can read must not end the stream; got {still_reading:?}"
        );

        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "filled",
            "a binary frame carrying an ORDER_TRADE_UPDATE must reach the sink"
        );
        assert!(
            reader_stream_state(&store).await.ready,
            "a binary frame that applied cleanly must not drop readiness"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **regression（High）· 读不懂的 Binary 帧必须 fail-closed。**
    ///
    /// R17 的 `_ => {}` 把它整条吞掉：不解析、不写 failure、不重连，
    /// `ready` 保持 1 继续放行新开仓。这里的载荷连 UTF-8 都不是——它没有
    /// 第二种解释，就是一份我们读不懂的真值。
    #[tokio::test]
    async fn a_binary_frame_that_is_not_utf8_drops_readiness_and_ends_the_stream() {
        let (store, path) = ready_reader_fixture("binary-not-utf8").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Binary(vec![0xff, 0xfe, 0x00, 0x01])),
            // 后一帧属于另一张单：流必须停在坏帧，不得读过去。
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a binary frame the engine cannot decode must fail the stream");
        assert!(
            error.to_string().contains("not valid UTF-8"),
            "the failure must say why the binary payload could not be read: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a binary frame that may have carried exchange truth must drop readiness: {state:?}"
        );
        assert_eq!(state.phase, "ws_binary");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("not valid UTF-8"),
            "the recorded reason must describe the undecodable frame: {state:?}"
        );
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "acked",
            "the stream must stop at the undecodable binary frame, not keep consuming"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **Binary 与 Text 走同一条解析判据**：UTF-8 解得出来但解析不动时，
    /// 归类、`ws_parse` 与断流行为与 Text 逐字相同，只是理由里多一个
    /// `binary` 标签（免得两类 data frame 各长一份判据后悄悄漂移）。
    #[tokio::test]
    async fn a_binary_frame_that_cannot_be_parsed_drops_readiness_and_ends_the_stream() {
        let (store, path) = ready_reader_fixture("binary-parse-fail").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Binary(
                br#"{"e":"ORDER_TRADE_UPDATE","E":"not-a-number"}"#.to_vec(),
            )),
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a binary frame that cannot be parsed must fail the stream");
        assert!(
            error.to_string().contains("binary frame")
                && error.to_string().contains("truth-bearing event type"),
            "the failure must name both the frame kind and its class: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_parse");
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "acked",
            "the stream must stop at the unparsable binary frame"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **regression（High）· Close 帧要在 reader **返回之前**落 not-ready。**
    ///
    /// R17 的 Close 分支是一句裸 `break`：reader 返回 `Ok(())` 时库里
    /// `ready` 还是 1，要等外层 `run_once` join 完 reader 再写 `disconnected`。
    /// 这条用例**直接调帧循环**，因此它观察到的就是那个窗口本身——循环返回
    /// 之后立刻读库，`ready` 必须已经是 0 且 `phase='ws_closed'`。
    /// 没有 reader 侧的那次写入，这里读到的只会是夹具留下的 `ready=1`。
    #[tokio::test]
    async fn a_close_frame_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-closed").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![Ok(
            Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away,
                reason: "listen key expired".into(),
            })),
        )];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect("a venue-initiated close is not a cycle failure");

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a closed user stream must be not-ready by the time the reader returns: {state:?}"
        );
        assert_eq!(state.phase, "ws_closed");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("listen key expired"),
            "the venue's close reason must be recorded: {state:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **regression（High）· EOF 同样要在 reader 返回之前落 not-ready。**
    ///
    /// 没有 Close 帧、连接直接断掉是生产里更常见的那一半
    /// （`StreamPoll::Ended`）。R17 在这里也是一句裸 `break`。
    #[tokio::test]
    async fn a_stream_that_ends_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-ended").await;

        // 先消费一帧健康真值再断，确保断流本身是唯一的 readiness 触发因素。
        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Text(order_frame(8002).into()))];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect("a stream that ends normally is not a cycle failure");

        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "filled",
            "the healthy frame before the EOF must still have been applied"
        );
        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a stream that ended must be not-ready by the time the reader returns: {state:?}"
        );
        assert_eq!(state.phase, "ws_ended");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **regression（High）· 断流也必须推恢复代际。**
    ///
    /// `run_once` 的 `finalize_recovery_readiness` 采样点早于落库；一条在
    /// 那之后才断掉的流，只有靠**代际 CAS** 才能挡住那次已经在飞的 ready
    /// 写入。交错与 `a_reader_parse_failure_survives_the_main_lines_stale_ready_write`
    /// 同形，只是触发因素换成 Close：
    ///
    /// 1. 主干 `begin_recovery_cycle` 拿到本周期的号；
    /// 2. reader 收到 Close，落 `ws_closed` 并返回；
    /// 3. 主干这时才带着**更早**采样的三个 `Succeeded` 去写 ready。
    ///
    /// 第 3 步必须 `PreserveNotReady`。只在 reader 返回后由外层补写
    /// `disconnected`（R17 的形状）救不了这一步：那次写入发生在 ready
    /// **之后**，库里会先出现一段 `ready=1`。
    #[tokio::test]
    async fn a_closed_stream_supersedes_the_main_lines_stale_ready_write() {
        let (store, path) = reader_test_store("ws-closed-cas").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Reconnect,
            1,
        )
        .await
        .expect("begin the recovery cycle the main line holds");

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Close(None))];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect("a venue-initiated close is not a cycle failure");

        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                READER_CREDENTIAL,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                9,
            )
            .await
            .expect("a superseded cycle is not an error"),
            crate::fill_replay::ReadinessWrite::PreserveNotReady,
            "a cycle whose stream already closed must not be able to declare readiness"
        );
        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_closed");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **transport 错误也必须在
    /// reader 返回之前落 not-ready。**
    ///
    /// R18 这条路径是一句裸 `let msg = next?`。TLS 断开、协议错、非法 opcode
    /// ——tungstenite 报的每一种传输故障都从这里直接弹出去，
    /// `fill_stream_state` 一行都不动。判据与 Close/EOF 逐字相同：
    /// reader **返回时**库里必须已经是 `ready=0`。
    #[tokio::test]
    async fn a_transport_error_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-transport").await;

        // 真实的传输故障：对端没走关闭握手就把连接断了。
        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Err(tokio_tungstenite::tungstenite::Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
            ))];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a transport failure must fail the stream");
        assert!(
            error.to_string().contains("transport error"),
            "the transport failure must be reported as such: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a stream whose transport failed must be not-ready by the time the reader \
             returns: {state:?}"
        );
        assert_eq!(state.phase, "ws_transport");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("without closing handshake"),
            "the transport error itself must be recorded: {state:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **空闲超时同根。**
    ///
    /// R18 的 IdleTimeout 分支只 `return Err(...)`：理由报出去了，`ready`
    /// 一个字节都没动。一条**静默到超过空闲上限**的流与一条被 Close 的流
    /// 在安全上没有区别——两者都已经与交易所脱节。
    #[tokio::test]
    async fn an_idle_timeout_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-idle").await;

        // 永不产出的流 + 一个很短的空闲上限：`next_frame_within` 必然返回
        // `IdleTimeout`（判据是上限本身，不依赖任何监听端口）。
        let mut read = futures_util::stream::pending::<
            Result<Message, tokio_tungstenite::tungstenite::Error>,
        >();
        let mut write: Vec<Message> = Vec::new();
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_millis(60),
            Duration::from_secs(5),
        )
        .await
        .expect_err("an idle stream must fail the stream");
        assert!(
            error.to_string().contains("idle beyond"),
            "the idle deadline must be reported as such: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a stream that went silent past its idle deadline must be not-ready by the time \
             the reader returns: {state:?}"
        );
        assert_eq!(state.phase, "ws_idle_timeout");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("idle beyond 60ms"),
            "the idle deadline that was breached must be recorded: {state:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **「采样之后、ready CAS 之前」的确定性竞态（transport `Err`）。**
    ///
    /// 这是 regression 真正的危害面，也是 `a_transport_error_drops_readiness_\
    /// before_the_reader_returns` 单独证不了的那一半。交错逐字如下：
    ///
    /// 1. 主干 `begin_recovery_cycle` 拿到本周期的号；
    /// 2. 主干采样 `tasks.reader_finished()` —— reader 这时**还活着**，
    ///    于是第五段判据是 `Succeeded`；
    /// 3. reader 的传输断了；
    /// 4. 主干这才带着第 2 步采样的三个 `Succeeded` 去写 ready。
    ///
    /// 第 4 步必须 `PreserveNotReady`。R18 的形状里第 3 步一行都不写，
    /// 恢复代际一格没动 ⇒ 第 4 步的 CAS **命中** ⇒ 库里出现 `ready=1`，
    /// 直到外层 join 完 reader 补写 `disconnected` 才降下来；那段窗口里
    /// `risk.rs` 照常放行新开仓。用例里第 2 步用一个显式的
    /// `RecoveryPhaseResult::Succeeded` 表达——它就是"采样发生在失败之前"
    /// 这件事本身。
    #[tokio::test]
    async fn a_transport_error_supersedes_the_main_lines_stale_ready_write() {
        let (store, path) = reader_test_store("ws-transport-cas").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Reconnect,
            1,
        )
        .await
        .expect("begin the recovery cycle the main line holds");

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Err(tokio_tungstenite::tungstenite::Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
            ))];
        let mut read = futures_util::stream::iter(frames);
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a transport failure must fail the stream");

        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                READER_CREDENTIAL,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                9,
            )
            .await
            .expect("a superseded cycle is not an error"),
            crate::fill_replay::ReadinessWrite::PreserveNotReady,
            "a cycle whose transport already failed must not be able to declare readiness"
        );
        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_transport");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **「采样之后、ready CAS 之前」的确定性竞态（IdleTimeout）。**
    ///
    /// 与上一条同形，触发因素换成空闲超时。两条各证一个退出面：
    /// 只留一条时，另一条退出面退回裸 `return Err(...)` 仍然全绿。
    #[tokio::test]
    async fn an_idle_timeout_supersedes_the_main_lines_stale_ready_write() {
        let (store, path) = reader_test_store("ws-idle-cas").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Reconnect,
            1,
        )
        .await
        .expect("begin the recovery cycle the main line holds");

        let mut read = futures_util::stream::pending::<
            Result<Message, tokio_tungstenite::tungstenite::Error>,
        >();
        let mut write: Vec<Message> = Vec::new();
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_millis(60),
            Duration::from_secs(5),
        )
        .await
        .expect_err("an idle stream must fail the stream");

        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                READER_CREDENTIAL,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                9,
            )
            .await
            .expect("a superseded cycle is not an error"),
            crate::fill_replay::ReadinessWrite::PreserveNotReady,
            "a cycle whose stream went silent must not be able to declare readiness"
        );
        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_idle_timeout");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **反向：控制帧 Pong 既不降 readiness 也不断流。**
    ///
    /// 全 frame fail-closed 不等于"见到没见过的帧就断"。RFC 6455 §5.5.3
    /// 允许服务端主动发未经请求的 Pong 当心跳，它按定义带不了应用载荷
    /// ——把它也当成真值丢失会让引擎在每一次心跳上重连。没有这条反向断言，
    /// 「Pong 一律 fail-closed」这种过度处置在其余用例上一样全绿。
    #[tokio::test]
    async fn an_unsolicited_pong_neither_drops_readiness_nor_ends_the_stream() {
        let (store, path) = ready_reader_fixture("ws-pong").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Pong(vec![1, 2, 3])),
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames).chain(futures_util::stream::pending());
        let mut write: Vec<Message> = Vec::new();
        let still_reading = tokio::time::timeout(
            Duration::from_millis(400),
            run_user_stream_reader_loop(
                &store,
                READER_CREDENTIAL,
                &BinanceEventSink::new(store.clone()),
                &mut read,
                &mut write,
                Duration::from_secs(600),
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            still_reading.is_err(),
            "an unsolicited pong must not end the stream; got {still_reading:?}"
        );
        assert!(
            reader_stream_state(&store).await.ready,
            "a protocol-level pong carries no exchange truth and must not drop readiness"
        );
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "filled",
            "the loop must keep consuming past a pong"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **一个写不出去的 sink**——对端已经把连接掐了的等价物。
    ///
    /// `poll_ready` 就绪（写请求真的被受理），失败发生在 `start_send`：
    /// 这正是 TCP 侧 `ECONNRESET` 从 tungstenite 弹回来的形状，与
    /// `NeverReady`（`binance_futures.rs`，写**挂住**⇒ deadline）互补。
    /// 两个形状最后都由 `send_frame_within` 归一成一个 `Err`。
    struct BrokenSink;

    impl futures_util::Sink<Message> for BrokenSink {
        type Error = std::io::Error;
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            ))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// **Pong 写失败必须在
    /// reader 返回之前落 not-ready。**
    ///
    /// R19 这条分支是裸 `send_frame_within(...).await?`：理由报出去了，
    /// `ready` 一个字节都没动。一条**写不出去**的用户流与一条传输已断的流
    /// 在安全上没有区别——两者都已经与交易所脱节，而 `risk.rs` 正是拿
    /// `ready` 放行新开仓。
    #[tokio::test]
    async fn a_pong_write_failure_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-pong-write").await;

        // 心跳来了，写侧却已经废了。
        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Ping(vec![9, 9]))];
        let mut read = futures_util::stream::iter(frames);
        let mut write = BrokenSink;
        let error = run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a stream we cannot write to must fail the stream");
        assert!(
            error.to_string().contains("could not answer a venue ping"),
            "the pong write failure must be reported as such: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(
            !state.ready,
            "a stream whose pong could not be written must be not-ready by the time the \
             reader returns: {state:?}"
        );
        assert_eq!(state.phase, "ws_pong_write");
        assert!(
            state
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("connection reset by peer"),
            "the write failure itself must be recorded: {state:?}"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **写**超时**也是同一条退出面。**
    ///
    /// 写失败与写超时是 `send_frame_within` 的两个分支（`Ok(Err)` 与
    /// `Err(_elapsed)`）。只钉住其中一个，另一个退回裸 `?` 仍然全绿。
    #[tokio::test]
    async fn a_pong_write_deadline_drops_readiness_before_the_reader_returns() {
        let (store, path) = ready_reader_fixture("ws-pong-deadline").await;

        /// 永远收不下的 sink：对端不读、TCP 写缓冲填满的等价物。
        struct NeverReady;
        impl futures_util::Sink<Message> for NeverReady {
            type Error = std::io::Error;
            fn poll_ready(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }
            fn start_send(
                self: std::pin::Pin<&mut Self>,
                _item: Message,
            ) -> Result<(), Self::Error> {
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

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Ping(Vec::new()))];
        let mut read = futures_util::stream::iter(frames);
        let mut write = NeverReady;
        // 用例自带上限：写 deadline 一旦被摘掉，这里必须**快速失败**而不是挂住。
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            run_user_stream_reader_loop(
                &store,
                READER_CREDENTIAL,
                &BinanceEventSink::new(store.clone()),
                &mut read,
                &mut write,
                Duration::from_secs(600),
                Duration::from_millis(60),
            ),
        )
        .await
        .expect("the pong write must be bounded by its own deadline, not by the harness")
        .expect_err("a pong that never completes must fail the stream");
        assert!(
            error.to_string().contains("60ms deadline"),
            "the breached write deadline must be reported: {error}"
        );

        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_pong_write");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **「采样之后、ready CAS 之前」的确定性竞态（Pong 写失败）。**
    ///
    /// 与 transport / idle 两条同形：主干先取号、再采样（reader 这时还活着
    /// ⇒ 三段判据 `Succeeded`），随后 reader 的写侧断了，主干这才带着旧采样
    /// 去写 ready。第四步必须 `PreserveNotReady`——挡住它的是
    /// `record_recovery_failure` 推走的那一格恢复代际。裸 `?` 的形状里
    /// 第三步一行都不写，那次 CAS 会**命中**。
    #[tokio::test]
    async fn a_pong_write_failure_supersedes_the_main_lines_stale_ready_write() {
        let (store, path) = reader_test_store("ws-pong-cas").await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            READER_CREDENTIAL,
            crate::fill_replay::RecoveryTrigger::Reconnect,
            1,
        )
        .await
        .expect("begin the recovery cycle the main line holds");

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Ping(vec![1]))];
        let mut read = futures_util::stream::iter(frames);
        let mut write = BrokenSink;
        run_user_stream_reader_loop(
            &store,
            READER_CREDENTIAL,
            &BinanceEventSink::new(store.clone()),
            &mut read,
            &mut write,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a stream we cannot write to must fail the stream");

        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                READER_CREDENTIAL,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                RecoveryPhaseResult::Succeeded,
                cycle,
                9,
            )
            .await
            .expect("a superseded cycle is not an error"),
            crate::fill_replay::ReadinessWrite::PreserveNotReady,
            "a cycle whose pong write already failed must not be able to declare readiness"
        );
        let state = reader_stream_state(&store).await;
        assert!(!state.ready, "{state:?}");
        assert_eq!(state.phase, "ws_pong_write");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    /// **反向：写得出去的 Pong 既不降 readiness 也不断流。**
    ///
    /// 全写失败 fail-closed 不等于"见到 Ping 就断"。Binance 每 ~3 分钟发一次
    /// 心跳，把正常心跳也当成真值丢失会让引擎在每一次 Ping 上重连。
    /// 没有这条反向断言，「Ping 一律 fail-closed」这种过度处置在其余用例上
    /// 一样全绿。
    #[tokio::test]
    async fn a_venue_ping_that_can_be_answered_keeps_the_stream_ready() {
        let (store, path) = ready_reader_fixture("ws-ping-ok").await;

        let frames: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Ping(vec![7, 7, 7])),
            Ok(Message::Text(order_frame(8002).into())),
        ];
        let mut read = futures_util::stream::iter(frames).chain(futures_util::stream::pending());
        let mut write: Vec<Message> = Vec::new();
        let still_reading = tokio::time::timeout(
            Duration::from_millis(400),
            run_user_stream_reader_loop(
                &store,
                READER_CREDENTIAL,
                &BinanceEventSink::new(store.clone()),
                &mut read,
                &mut write,
                Duration::from_secs(600),
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            still_reading.is_err(),
            "an answered ping must not end the stream; got {still_reading:?}"
        );
        assert_eq!(
            write,
            vec![Message::Pong(vec![7, 7, 7])],
            "the venue ping must be answered with its own payload"
        );
        assert!(
            reader_stream_state(&store).await.ready,
            "a ping we could answer carries no failure and must not drop readiness"
        );
        assert_eq!(
            reader_status_of(&store, "eng_f2").await,
            "filled",
            "the loop must keep consuming past an answered ping"
        );
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}
