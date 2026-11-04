use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_futures::symbol_from_symbol_key;
use crate::store::Store;
use crate::svc::{now_ms, sha256_hex, EventContext};

/// **交易所侧的官方全集**：Binance USDⓈ-M `GET /fapi/v1/klines` 的 `interval`
/// 枚举，逐字照抄，**不是**本引擎的能力集合。
///
/// 此前只有一个数组，注释把它叫做
/// "Binance 实际支持的闭世界集合"，而它其实是**引擎自己提供的子集**
/// （官方枚举含 `1s` 与 `1M`，数组里没有）。于是拒绝信息在说
/// "Binance 只提供这些"——把**本引擎的自身受限**说成了交易所的限制。
/// 两个集合从此分开表达：错误信息据实区分"交易所没有"与"我们不提供"。
///
/// 它同时是能力集合的**外层护栏**：引擎集合必须是它的子集，
/// 任何往 allowlist 里偷偷加一个交易所根本没有的档位（评审变异：`2m`）
/// 都会被 `the_engine_interval_set_is_a_subset_of_the_official_enumeration`
/// 直接打红。
pub(crate) const BINANCE_USDM_OFFICIAL_KLINE_INTERVALS: [&str; 16] = [
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
    "1M",
];

/// **本引擎提供的 kline interval 闭世界集合**——官方全集的**真子集**。
///
/// `EnsureMarketData` 此前的
/// "闭世界"只调 `timeframe_to_ms`——那是一个**通用解析器**，`7m` / `11h`
/// 这类 Binance 根本不提供的档位一样能解析出毫秒数，于是被登记成 required、
/// 起一条永远收不到 bar 的 kline task，行情中断判据反而被这条死流拖着。
///
/// 与官方全集的两处差，都是**本引擎自身的选择**，与交易所能力无关：
/// * `1M`（月线）不提供：订阅与心跳两张表都按 lowercase 存取，`1M` 会与
///   `1m`（分钟）折叠成同一个键——两个语义相反的周期共用一行订阅与一行心跳。
///   宁可少一个能用的档位，也不要一个会把月线当分钟线判"中断"的映射。
/// * `1s` 不提供：秒级 bar 的中断阈值落在本仓的扫描周期以下，登记它等于
///   登记一条判据永远来不及评的流。
pub(crate) const ENGINE_KLINE_INTERVALS: [&str; 14] = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
];

/// 官方有、但**本引擎不提供**的档位（诚实措辞的素材）。
fn intervals_the_engine_declines() -> Vec<&'static str> {
    BINANCE_USDM_OFFICIAL_KLINE_INTERVALS
        .iter()
        .copied()
        .filter(|interval| !ENGINE_KLINE_INTERVALS.contains(interval))
        .collect()
}

/// 本引擎唯一支持的行情来源（venue × market）。
///
/// 与 WS / reconcile 的归属判据**同一份**常量
/// （`binance_futures::USDM_*`）。此前订阅闭世界与成交归属各写各的字面量，
/// 正是"同一语义两族开关"的形状。
pub(crate) const SUPPORTED_EXCHANGE: &str = crate::binance_futures::USDM_VENUE;
pub(crate) const SUPPORTED_MARKET_KIND: &str = crate::binance_futures::USDM_MARKET_KIND;

/// 一条**通过闭世界校验**的订阅。构造它的唯一途径是 `validate_subscription`，
/// 因此"这条订阅可执行"是类型层面的事实，而不是调用方记得校验过。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValidatedSubscription {
    pub symbol_key: String,
    pub timeframe: String,
    /// 由 `symbol_key` 解析出的交易所 symbol（`BTCUSDT`）。
    pub symbol: String,
}

/// **真闭世界校验**：venue × market × symbol × timeframe 四项**全部**要在
/// 能力集合里，任何一项不在就返回 `Err`。
///
/// 判据落在这里而不是调用方，是因为 regression 的形态正是
/// 「调用方逐条 `continue` 把不认识的条目丢掉」——丢掉之后既没有 stream、
/// 也不在 required 集合里，行情缺失对判据隐形。
pub(crate) fn validate_subscription(
    exchange: &str,
    market_kind: &str,
    symbol_key: &str,
    timeframe: &str,
) -> Result<ValidatedSubscription, String> {
    let exchange_norm = exchange.trim().to_lowercase();
    if exchange_norm != SUPPORTED_EXCHANGE {
        return Err(format!(
            "unsupported exchange {exchange:?}: this engine only streams {SUPPORTED_EXCHANGE}"
        ));
    }
    let market_norm = market_kind.trim().to_lowercase();
    if market_norm != SUPPORTED_MARKET_KIND {
        return Err(format!(
            "unsupported market_kind {market_kind:?}: this engine only streams              {SUPPORTED_EXCHANGE} {SUPPORTED_MARKET_KIND}"
        ));
    }
    let symbol_key_norm = symbol_key.trim().to_string();
    if symbol_key_norm.is_empty() {
        return Err("symbol_key must not be empty".to_string());
    }
    // **身份必须同源。**
    //
    // 此前这里只调 `symbol_from_symbol_key`——它把 key 内的 venue/market 两段
    // 整段丢掉，只拼 base+quote。于是上面刚校验完的 outer venue/market 与
    // key 自己声明的 venue/market **互不相干**：评审实跑
    // `("binance","futures","okx|spot|BTC|USDT|USDT","1m")` 整批通过，
    // 落库的 required 行带着 `okx|spot` 前缀，而起的流连的是 Binance——
    // 订阅集合与执行体从此指向两个不同的市场，判据比对的是错的东西。
    //
    // 现在两处身份出自**同一次解析**，任何一段不一致就整批拒。
    let Some(identity) = crate::binance_futures::symbol_key_identity(&symbol_key_norm) else {
        return Err(format!(
            "symbol_key {symbol_key_norm:?} does not resolve to a Binance symbol;              a kline stream for it could never connect"
        ));
    };
    if !identity.venue.eq_ignore_ascii_case(&exchange_norm) {
        return Err(format!(
            "symbol_key {symbol_key_norm:?} declares venue {:?} but the subscription \
             declares {exchange_norm:?}; a subscription whose two identities disagree \
             would register one market and stream another",
            identity.venue
        ));
    }
    if !identity.market_kind.eq_ignore_ascii_case(&market_norm) {
        return Err(format!(
            "symbol_key {symbol_key_norm:?} declares market_kind {:?} but the subscription \
             declares {market_norm:?}; a subscription whose two identities disagree \
             would register one market and stream another",
            identity.market_kind
        ));
    }
    // **settle 也是闭世界的一段。**
    //
    // R10 收紧了 venue/market 两段，settle 仍然没人看：`binance|futures|BTC|USDT|BTC`
    // （币本位结算）此前整批通过，落库成 required，起的却是 USDⓈ-M 的 kline 流，
    // 而 WS/reconcile 的归属判据又会把 USDⓈ-M 的真值往它头上写。
    // 本引擎只做 USDⓈ-M：settle 必须等于 quote。
    if !identity.settle.eq_ignore_ascii_case(&identity.quote) {
        return Err(format!(
            "symbol_key {symbol_key_norm:?} settles in {:?} while it quotes in {:?}; this \
             engine streams only Binance USDS-M contracts, whose settle asset is the quote \
             asset. A coin-margined key shares the exchange symbol but is a different \
             contract",
            identity.settle, identity.quote
        ));
    }
    let symbol = identity.symbol;
    let timeframe_raw = timeframe.trim();
    if timeframe_raw.is_empty() {
        return Err("timeframe must not be empty".to_string());
    }
    // **大小写折叠的歧义先拒**：Binance 用 `M` 表示月线、`m` 表示分钟线，
    // 而本仓在订阅与心跳两张表上都按 lowercase 存取。`1M` 一旦被 lowercase
    // 就与 `1m` 共用同一行——两个语义相反的周期共享订阅与心跳。
    // 这道判据必须在 lowercase **之前**做，否则它永远看不到区别。
    if timeframe_raw.contains('M') {
        return Err(format!(
            "timeframe {timeframe_raw:?} uses Binance's month unit `M`, which this engine \
             does not support: subscriptions and heartbeats are keyed case-insensitively, \
             so `1M` would fold onto the minute interval `1m`"
        ));
    }
    let timeframe_norm = timeframe_raw.to_lowercase();
    if !ENGINE_KLINE_INTERVALS.contains(&timeframe_norm.as_str()) {
        // **措辞必须如实**：此前这句写的是
        // "Binance USDⓈ-M klines only serve {allowlist}"，而 allowlist 是
        // **本引擎的子集**——官方枚举里明明有 `1s`/`1M`。把自身受限说成交易所
        // 限制，会让运维照着这句去查交易所文档，然后发现两边对不上。
        let declined = intervals_the_engine_declines();
        return Err(format!(
            "unsupported timeframe {timeframe:?}: this engine streams only              {ENGINE_KLINE_INTERVALS:?}; an unsupported interval would be registered as              required and then never produce a bar. Binance USDⓈ-M additionally serves              {declined:?}, which this engine deliberately does not provide"
        ));
    }
    // 冗余但刻意保留的一道：能力集合里的每个档位都必须能被中断判据换算成
    // 阈值。两张表一旦走散，登记的流又会变成判据评不了的死流。
    if crate::auto_triggers::timeframe_to_ms(&timeframe_norm).is_none() {
        return Err(format!(
            "timeframe {timeframe_norm:?} is in the venue capability set but the              interruption trigger cannot derive a threshold for it; refusing"
        ));
    }
    Ok(ValidatedSubscription {
        symbol_key: symbol_key_norm,
        timeframe: timeframe_norm,
        symbol,
    })
}

/// **一条已持久化的 required 订阅行是否仍然通过当前闭世界。**
///
/// R10 让**启动重建**重验持久行，
/// 但重建的判据与行情中断判据是两套——重建拒了它、判据看不见它，
/// 那条流于是既不产 bar、也永远不 arm 任何东西（"隔离"退化成"静默"）。
///
/// 现在三个消费方共用这**一份**判据：
/// * `restore_required_streams`：拒绝起 worker；
/// * `auto_triggers::market_data_interruption_decision`：立刻判为可处置故障；
/// * `main` 启动路径：`rejected > 0` 时进入结构化 not-ready。
///
/// 返回 `Some(reason)` 表示这一行**在当前闭世界下不可能变成活流**；
/// `None` 表示它合法。
pub(crate) fn persisted_subscription_rejection(
    symbol_key: &str,
    timeframe: &str,
) -> Option<String> {
    let Some(identity) = crate::binance_futures::symbol_key_identity(symbol_key) else {
        return Some(format!(
            "symbol_key {symbol_key:?} does not resolve to a Binance symbol"
        ));
    };
    // 身份只能来自这一行自己：订阅表没有 venue/market 列，
    // `symbol_key` 的头两段就是它声明的市场。
    validate_subscription(
        &identity.venue,
        &identity.market_kind,
        symbol_key,
        timeframe,
    )
    .err()
}

/// **整批**校验：任何一条不合法 → 整批 `Err`（fail-closed 方向）。
///
/// 空批同样拒绝：`accepted=true, started=0` 是编排器唯一会看的字段组合，
/// 一个"什么都没订阅却被接受"的请求会让 bot 以零行情流进 running。
/// 生产调用方（`admin_api.start_bot`）的 `parse_subscribe_timeframes` 恒返回
/// 非空列表，因此拒空批不会拒掉任何真实请求。
pub(crate) fn validate_subscription_batch<'a, I>(
    subscriptions: I,
) -> Result<Vec<ValidatedSubscription>, String>
where
    I: IntoIterator<Item = (&'a str, &'a str, &'a str, &'a str)>,
{
    let mut validated = Vec::new();
    for (index, (exchange, market_kind, symbol_key, timeframe)) in
        subscriptions.into_iter().enumerate()
    {
        match validate_subscription(exchange, market_kind, symbol_key, timeframe) {
            Ok(subscription) => validated.push(subscription),
            Err(error) => {
                return Err(format!(
                    "subscription #{index} rejected: {error} \
                     (the whole batch is refused: a partially accepted batch leaves \
                     streams that the interruption trigger cannot see)"
                ))
            }
        }
    }
    if validated.is_empty() {
        return Err(
            "no market data subscriptions requested; refusing to report success for a \
             bot that would run with zero streams"
                .to_string(),
        );
    }
    Ok(validated)
}

/// 一次启动重建的结果。`started` 与 `rejected` **分开报**：把非法行折进
/// "没新起"里，正是评审反例（`started=1`）能存在的原因。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RestoreOutcome {
    /// 本次真正新起的 worker 数（已经活着的不计）。
    pub started: u32,
    /// 未通过当前闭世界、因而**没有**被起成 worker 的持久行数。
    pub rejected: u32,
    /// 至少有一条被拒行的 credential。启动路径按它进入结构化 not-ready
    /// ——`rejected` 只是个计数，
    /// 光有计数没有身份，调用方就只能"记个日志"。
    pub rejected_credential_ids: std::collections::BTreeSet<String>,
}

#[derive(Clone)]
pub struct MarketDataManager {
    inner: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
}

impl MarketDataManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn ensure_kline_running(
        &self,
        credential_id: &str,
        store: Store,
        symbol_key: &str,
        timeframe: &str,
    ) -> bool {
        let key = format!(
            "kline|{}|{}|{}",
            credential_id.trim(),
            symbol_key.trim(),
            timeframe.trim().to_lowercase()
        );
        let credential_id = credential_id.trim().to_string();
        let symbol_key = symbol_key.trim().to_string();
        let timeframe = timeframe.trim().to_lowercase();

        self.ensure_task(key, move || {
            tokio::spawn(async move {
                let mut reconnect = false;
                loop {
                    if let Err(error) = crate::round2_diagnostics::record_connection_attempt(&store, &credential_id, "MARKET_DATA", reconnect, now_ms()).await {
                        tracing::warn!(%error, "market websocket diagnostic write failed; retrying before connecting");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    reconnect = true;
                    if let Err(e) =
                        run_kline_once(&credential_id, store.clone(), &symbol_key, &timeframe).await
                    {
                        tracing::warn!(
                            credential_id = %credential_id,
                            symbol_key = %symbol_key,
                            timeframe = %timeframe,
                            error = %e,
                            "kline stream failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    } else {
                        tracing::warn!(
                            credential_id = %credential_id,
                            symbol_key = %symbol_key,
                            timeframe = %timeframe,
                            "kline stream disconnected; retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            })
        })
        .await
    }

    /// **跨 rustcore 重启把 required 订阅重新变成执行体**（    /// 段级第五轮复评 regression，High）。
    ///
    /// `EngineExecutionService::new` 每次都造一个**空**的 `MarketDataManager`，
    /// 而进程启动只调 user-stream 的 `start_discovered`。于是重启之后：
    /// required 集合还在库里（判据看得见），kline worker 一个都没有
    /// （执行体没了）。表现是"行情永远不来"，而**判据要过一段时间才会
    /// 意识到**——期间决策循环照跑。
    ///
    /// 这里按持久化的 required 集合重建 worker。**刻意不写库**：
    /// `requested_at_ms` 是"首次被要求的时刻"，R7 已裁定重复 ensure 不得刷新
    /// （刷新会让一条从未起来的流每次重试都把计时清零，永远够不到阈值）；
    /// 重启是最典型的"重复 ensure"，更不能在这里把计时清零。
    ///
    /// 返回本次的重建结果（见 `RestoreOutcome`）。
    ///
    /// **持久行必须重验当前闭世界。**
    ///
    /// 此前这里直接消费库行：`ensure_kline_running` 只做 trim/lowercase，
    /// 校验全在 `EnsureMarketData` 那一侧。于是**任何**绕过入口进到表里的行
    /// ——旧版本写下的、闭世界收紧之前合法的、DBA 手工插的——在重启后
    /// 都会被原样起成 worker（评审实跑：种入历史 `7m` 行后 restore 返回
    /// `started=1`）。入口的闭世界因此只挡"新的"，挡不住"已经在库里的"。
    ///
    /// 现在每一行都按**当前**闭世界重验，身份取自 `symbol_key` 自身声明的
    /// venue/market（与入口同一个解析器、同一个判据）。非法行 fail-closed：
    /// **不起 worker**、逐行 `error!` 告警，并计入 `rejected`。
    ///
    /// 刻意**不删库行**：required 集合是"我们被要求让它活着"的真值，删掉它
    /// 会让行情中断判据连这条流都看不见（`required − observed` 的差集里
    /// 少一条 = 静默）。留着它，判据到点仍会 arm——这就是隔离而非静默。
    pub async fn restore_required_streams(
        &self,
        store: &Store,
    ) -> Result<RestoreOutcome, sqlx::Error> {
        let required = crate::auto_triggers::all_required_streams(store).await?;
        let mut outcome = RestoreOutcome::default();
        for stream in &required {
            // 判据与行情中断触发源、启动 not-ready 判定**同一份**。
            if let Some(reason) =
                persisted_subscription_rejection(&stream.symbol_key, &stream.timeframe)
            {
                outcome.rejected = outcome.rejected.saturating_add(1);
                outcome
                    .rejected_credential_ids
                    .insert(stream.credential_id.clone());
                tracing::error!(
                    credential_id = %stream.credential_id,
                    symbol_key = %stream.symbol_key,
                    timeframe = %stream.timeframe,
                    reason = %reason,
                    "persisted market data subscription fails the current closed-world \
                     check; refusing to start a worker for it (the required row is kept \
                     so the interruption trigger still sees the gap)"
                );
                continue;
            }
            if self
                .ensure_kline_running(
                    &stream.credential_id,
                    store.clone(),
                    stream.symbol_key.trim(),
                    stream.timeframe.trim(),
                )
                .await
            {
                outcome.started = outcome.started.saturating_add(1);
            }
        }
        tracing::info!(
            required = required.len(),
            started = outcome.started,
            rejected = outcome.rejected,
            "restored persisted market data subscriptions after start"
        );
        Ok(outcome)
    }

    /// **登记不等于活着**。
    ///
    /// 此前 `ensure_kline_running` 只判 `contains_key`：kline 任务内部虽然是
    /// 死循环，但 **panic 会让 `JoinHandle` 结束**，而条目仍留在表里，
    /// 于是它永远直接返回、行情流再也起不来。行情流静默停掉之后，
    /// B3b 的「行情中断」触发源反而会把账户按停——**触发机制本身会死**，
    /// 正是本子批要解决的问题。
    ///
    /// 抽成带 `spawn` 闭包的小函数是为了能被测试直接钉住：
    /// 生产路径里 `spawn` 起的是真实 websocket 循环，测试里可以起一个
    /// 立刻结束的任务来模拟"worker 早死"。
    async fn ensure_task<F>(&self, key: String, spawn: F) -> bool
    where
        F: FnOnce() -> tokio::task::JoinHandle<()>,
    {
        let mut guard = self.inner.lock().await;
        if let Some(handle) = guard.get(&key) {
            if !handle.is_finished() {
                return false;
            }
            tracing::warn!(key = %key, "market data task was dead; restarting");
            guard.remove(&key);
        }
        guard.insert(key, spawn());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Once;
    use std::task::{Context, Poll};

    use futures_util::SinkExt;
    use sqlx::Row;

    static INSTALL_DRIVERS: Once = Once::new();

    async fn test_store() -> (Store, PathBuf) {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!("rustcore-md-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        (store, path)
    }

    async fn cleanup(store: Store, path: PathBuf) {
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    include!("market_data_transport_tests.rs");

    /// 读心跳表。历史上的 sqlx 0.7.4 `Any` + SQLite 会把宽整数截成 `i32`；
    /// 精确锁定的 0.8.6 已修复直接 `i64` 读取。这里保留文本投影作为纵深防御，
    /// 并继续让用例断言落库真值；驱动宽度契约由 `store` 的双后端测试钉住。
    async fn heartbeat_rows(store: &Store) -> Vec<(String, String, i64)> {
        sqlx::query(
            "SELECT symbol_key, timeframe, CAST(last_bar_ms AS TEXT) AS last_bar_ms \
             FROM market_stream_health ORDER BY symbol_key, timeframe",
        )
        .fetch_all(store.pool())
        .await
        .expect("read heartbeats")
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("symbol_key"),
                row.get::<String, _>("timeframe"),
                row.get::<String, _>("last_bar_ms")
                    .parse::<i64>()
                    .expect("last_bar_ms is stored as an integer"),
            )
        })
        .collect()
    }

    async fn bar_event_count(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='bar_closed'")
            .fetch_one(store.pool())
            .await
            .expect("count bar events")
            .get::<i64, _>("n")
    }

    /// 一条**永远不 ready** 的 sink：模拟对端不读、写缓冲填满的连接。
    struct StalledSink;

    impl futures_util::Sink<Message> for StalledSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("a stalled sink is never ready to send")
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    /// 一条什么都吞下的 sink（文本帧用例不关心写侧）。
    struct NullSink;

    impl futures_util::Sink<Message> for NullSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn closed_kline_frame(symbol: &str, interval: &str, open_ms: i64, close_ms: i64) -> String {
        serde_json::json!({
            "e": "kline",
            "E": close_ms,
            "s": symbol,
            "k": {
                "t": open_ms,
                "T": close_ms,
                "i": interval,
                "x": true,
                "o": "100",
                "h": "101",
                "l": "99",
                "c": "100.5",
                "v": "12",
                "n": 7
            }
        })
        .to_string()
    }

    const BTC_KEY: &str = "binance|futures|BTC|USDT|USDT";

    /// **regression**：`7m` 能被通用解析器解析，但 Binance 不提供它。
    #[test]
    fn a_parseable_but_unsupported_interval_is_refused() {
        // 反例：R8 只判 `timeframe_to_ms(...).is_some()`，`7m` 因此过关。
        assert!(crate::auto_triggers::timeframe_to_ms("7m").is_some());
        let error = validate_subscription("binance", "futures", BTC_KEY, "7m")
            .expect_err("7m is not a Binance USDⓈ-M kline interval");
        assert!(error.contains("unsupported timeframe"), "{error}");
    }

    /// 放行面自证：能力集合里的每一个档位都必须被接受，
    /// 否则"一律拒绝"也能让上面那条全绿。
    #[test]
    fn every_supported_interval_is_accepted() {
        for timeframe in ENGINE_KLINE_INTERVALS {
            let validated = validate_subscription("binance", "futures", BTC_KEY, timeframe)
                .unwrap_or_else(|error| panic!("{timeframe} must be accepted: {error}"));
            assert_eq!(validated.timeframe, timeframe);
            assert_eq!(validated.symbol, "BTCUSDT");
            assert!(crate::auto_triggers::timeframe_to_ms(timeframe).is_some());
        }
    }

    /// venue / market / 空值 / 非法 symbol_key 一律拒绝（此前是逐条静默丢弃）。
    #[test]
    fn unsupported_venue_market_and_empty_entries_are_refused() {
        assert!(validate_subscription("okx", "futures", BTC_KEY, "1m").is_err());
        assert!(validate_subscription("binance", "spot", BTC_KEY, "1m").is_err());
        assert!(validate_subscription("binance", "futures", "", "1m").is_err());
        assert!(validate_subscription("binance", "futures", BTC_KEY, "").is_err());
        assert!(validate_subscription("binance", "futures", BTC_KEY, "   ").is_err());
        assert!(validate_subscription("binance", "futures", "not-a-symbol-key", "1m").is_err());
        // 月线与分钟线在 lowercase 之后会折叠成同一个键，宁可拒绝。
        assert!(validate_subscription("binance", "futures", BTC_KEY, "1M").is_err());
    }

    /// **整批拒绝**：一条无效就整批不收；空批同样不收。
    #[test]
    fn one_invalid_entry_refuses_the_whole_batch() {
        let mixed = vec![
            ("binance", "futures", BTC_KEY, "1m"),
            ("binance", "futures", BTC_KEY, "7m"),
        ];
        let error = validate_subscription_batch(mixed)
            .expect_err("a mixed batch must be refused as a whole");
        assert!(error.contains("subscription #1"), "{error}");

        assert!(validate_subscription_batch(Vec::new()).is_err());

        let all_valid = vec![
            ("binance", "futures", BTC_KEY, "1m"),
            ("binance", "futures", BTC_KEY, "1H"),
        ];
        let validated = validate_subscription_batch(all_valid).expect("valid batch must pass");
        assert_eq!(validated.len(), 2);
        assert_eq!(validated[1].timeframe, "1h");
    }

    /// **regression**：错配 symbol 的收盘帧不得被本流当成心跳。
    #[tokio::test]
    async fn a_frame_from_another_symbol_is_rejected_and_never_recorded() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let frame = closed_kline_frame("ETHUSDT", "1m", now - 120_000, now - 60_000);

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert!(
            matches!(outcome, KlineIngest::Rejected(ref reason) if reason.contains("ETHUSDT")),
            "{outcome:?}"
        );
        assert!(heartbeat_rows(&store).await.is_empty());
        assert_eq!(bar_event_count(&store).await, 0);
        cleanup(store, path).await;
    }

    /// 错配 interval 同理：BTC/1m 的 manager 收到 BTC/5m 的收盘帧不得落库。
    #[tokio::test]
    async fn a_frame_from_another_interval_is_rejected_and_never_recorded() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "5m", now - 600_000, now - 300_000);

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert!(
            matches!(outcome, KlineIngest::Rejected(ref reason) if reason.contains("5m")),
            "{outcome:?}"
        );
        assert!(heartbeat_rows(&store).await.is_empty());
        assert_eq!(bar_event_count(&store).await, 0);
        cleanup(store, path).await;
    }

    /// 未来 bar 不收：收下它会把心跳推到未来，让"陈旧"判据永远看着新鲜。
    #[tokio::test]
    async fn a_bar_that_closes_in_the_future_is_rejected() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let frame = closed_kline_frame(
            "BTCUSDT",
            "1m",
            now + MAX_BAR_CLOCK_SKEW_MS + 60_000,
            now + MAX_BAR_CLOCK_SKEW_MS + 120_000,
        );

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert!(
            matches!(outcome, KlineIngest::Rejected(ref reason) if reason.contains("clock-skew")),
            "{outcome:?}"
        );
        assert!(heartbeat_rows(&store).await.is_empty());
        cleanup(store, path).await;
    }

    /// **放行面自证**：身份一致、时间合理的收盘帧必须真的落库并记心跳。
    /// 没有这条，"一律拒绝"也能让上面三条全绿。
    ///
    /// 这条用例此前把上游 interval 写成
    /// `1M` 喂给 `1m` 流并断言 `Recorded`——**它把缺陷编码进了断言**。
    /// symbol 仍按大小写不敏感（`btcusdt` 与 `BTCUSDT` 是同一个标的），
    /// interval 改为逐字节相符（`m`=分钟 / `M`=月）。
    #[tokio::test]
    async fn a_matching_closed_bar_is_recorded_with_its_own_identity() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let close_ms = now - 60_000;
        let frame = closed_kline_frame("btcusdt", "1m", close_ms - 60_000, close_ms);

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert_eq!(outcome, KlineIngest::Recorded, "{outcome:?}");
        assert_eq!(
            heartbeat_rows(&store).await,
            vec![(BTC_KEY.to_string(), "1m".to_string(), close_ms)]
        );
        assert_eq!(bar_event_count(&store).await, 1);
        cleanup(store, path).await;
    }

    /// **DB 故障不得被伪装成健康**：落库失败要报出来，且心跳不得出现。
    #[tokio::test]
    async fn a_persistence_failure_is_surfaced_instead_of_being_swallowed() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "1m", now - 120_000, now - 60_000);
        store.pool().close().await;

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert!(
            matches!(outcome, KlineIngest::PersistFailed(_)),
            "a closed pool must surface as PersistFailed, got {outcome:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    /// **存活变异 M7 的生产调用点**：Pong 必须走**有界**写。
    ///
    /// 对端不读时 sink 永远不 ready。绕过 `send_frame_within` 的实现会挂在
    /// await 上，外层的 5s 上限因此超时，用例变红；有界写在 50ms 上限内
    /// 带着理由返回 `Err`。
    #[tokio::test]
    async fn a_ping_reply_to_a_stalled_peer_is_bounded() {
        let (store, path) = test_store().await;
        let mut sink = StalledSink;

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            handle_kline_frame(
                &store,
                "cred_md",
                BTC_KEY,
                "1m",
                Duration::from_millis(50),
                &mut sink,
                Message::Ping(Default::default()),
            ),
        )
        .await
        .expect("the pong write must be bounded; an unbounded send would hang here");
        let error = outcome.expect_err("a stalled write must fail, not silently succeed");
        assert!(
            error.to_string().contains("exceeded"),
            "the failure must carry its reason: {error}"
        );
        cleanup(store, path).await;
    }

    /// 帧派发的放行面：文本帧照常被消费，Close 帧让循环收。
    #[tokio::test]
    async fn the_frame_dispatch_still_consumes_text_and_stops_on_close() {
        let (store, path) = test_store().await;
        let mut sink = NullSink;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "1m", now - 120_000, now - 60_000);

        let step = handle_kline_frame(
            &store,
            "cred_md",
            BTC_KEY,
            "1m",
            Duration::from_millis(500),
            &mut sink,
            Message::Text(frame.into()),
        )
        .await
        .expect("text frames must not fail the loop");
        assert_eq!(step, KlineFrameStep::Continue);
        assert_eq!(bar_event_count(&store).await, 1);

        let step = handle_kline_frame(
            &store,
            "cred_md",
            BTC_KEY,
            "1m",
            Duration::from_millis(500),
            &mut sink,
            Message::Close(None),
        )
        .await
        .expect("close frames end the loop without an error");
        assert_eq!(step, KlineFrameStep::Stop);
        cleanup(store, path).await;
    }

    /// **regression**：`restore_required_streams` 按持久集合重建 worker，
    /// 且**不刷新** `requested_at_ms`。
    #[tokio::test]
    async fn a_fresh_manager_rebuilds_workers_from_the_persisted_required_set() {
        let (store, path) = test_store().await;
        crate::auto_triggers::record_required_streams_atomically(
            &store,
            "cred_md_restart",
            &[
                (BTC_KEY.to_string(), "1m".to_string()),
                (BTC_KEY.to_string(), "5m".to_string()),
            ],
            1_000,
        )
        .await
        .expect("register required streams");

        // 全新的 manager = 重启之后的执行态（`EngineExecutionService::new`
        // 每次都造一个空的）。
        let manager = MarketDataManager::new();
        let started = manager
            .restore_required_streams(&store)
            .await
            .expect("restore required streams");
        assert_eq!(
            started,
            RestoreOutcome {
                started: 2,
                rejected: 0,
                rejected_credential_ids: Default::default(),
            },
            "both persisted subscriptions must get a worker"
        );

        // 已经活着的不重复起——第二次 restore 一条都不新起。
        let again = manager
            .restore_required_streams(&store)
            .await
            .expect("restore is idempotent");
        assert_eq!(again, RestoreOutcome::default());

        // 重建**不得**刷新首次被要求的时刻（R7 裁定）。
        let required = crate::auto_triggers::required_streams(&store, "cred_md_restart")
            .await
            .expect("read required streams");
        assert_eq!(required.len(), 2);
        for stream in required {
            assert_eq!(stream.requested_at_ms, 1_000);
        }
        cleanup(store, path).await;
    }

    /// **regression（High）· 月线帧不得被分钟流收下。**
    ///
    /// 评审反例：上游 `1M`（月线）收盘帧喂给 `1m`（分钟）流，
    /// `eq_ignore_ascii_case` 把它当成本流的 bar 收下并刷新心跳——
    /// 于是"这条分钟流健康"由一根月线撑着。
    /// 入口 (`validate_subscription`) 对 `1M` 是拒的，消费侧必须同一条理由。
    #[tokio::test]
    async fn a_month_interval_frame_is_never_accepted_by_a_minute_stream() {
        let (store, path) = test_store().await;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "1M", now - 120_000, now - 60_000);

        let outcome = handle_kline_text(&store, "cred_md", BTC_KEY, "1m", &frame).await;
        assert!(
            matches!(outcome, KlineIngest::Rejected(ref reason) if reason.contains("1M")),
            "{outcome:?}"
        );
        assert!(
            heartbeat_rows(&store).await.is_empty(),
            "a month bar must never refresh a minute stream's heartbeat"
        );
        assert_eq!(bar_event_count(&store).await, 0);

        // 入口侧的理由必须一致：`1M` 在订阅时就被拒。
        assert!(validate_subscription("binance", "futures", BTC_KEY, "1M").is_err());
        cleanup(store, path).await;
    }

    /// **regression（High）· 落库失败必须把这条流的读循环打断。**
    ///
    /// 驱动的是**生产帧消费函数** `handle_kline_frame`（读循环里唯一的
    /// `match msg`），不是只看 `KlineIngest` 这个 enum：反例形态正是
    /// "`report` 只写日志、调用方照样 `Ok(Continue)`"——那样一条每根 bar 都
    /// 落库失败的流会一直被当成正常在跑。
    #[tokio::test]
    async fn a_persistence_failure_breaks_the_production_frame_loop() {
        let (store, path) = test_store().await;
        let mut sink = NullSink;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "1m", now - 120_000, now - 60_000);
        store.pool().close().await;

        let error = handle_kline_frame(
            &store,
            "cred_md",
            BTC_KEY,
            "1m",
            Duration::from_millis(500),
            &mut sink,
            Message::Text(frame.into()),
        )
        .await
        .expect_err("a bar that cannot be persisted must not be reported as Continue");
        assert!(
            error.to_string().contains("persistence failed"),
            "the failure must carry its reason: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    /// 放行面：能落库的帧仍然让循环继续（否则上一条用"一律 Err"也能全绿）。
    #[tokio::test]
    async fn a_persistable_frame_keeps_the_production_frame_loop_running() {
        let (store, path) = test_store().await;
        let mut sink = NullSink;
        let now = now_ms();
        let frame = closed_kline_frame("BTCUSDT", "1m", now - 120_000, now - 60_000);

        let step = handle_kline_frame(
            &store,
            "cred_md",
            BTC_KEY,
            "1m",
            Duration::from_millis(500),
            &mut sink,
            Message::Text(frame.into()),
        )
        .await
        .expect("a persistable bar must not break the loop");
        assert_eq!(step, KlineFrameStep::Continue);
        assert_eq!(bar_event_count(&store).await, 1);
        cleanup(store, path).await;
    }

    /// **regression（High）· symbol_key 内的 venue/market 必须与 outer 同源。**
    ///
    /// 评审反例：`("binance","futures","okx|spot|BTC|USDT|USDT","1m")`
    /// 通过了整批校验——outer 身份被校验了，key 里的身份整段没人看。
    #[test]
    fn a_symbol_key_that_declares_another_market_is_refused() {
        for symbol_key in [
            "okx|spot|BTC|USDT|USDT",
            "okx|futures|BTC|USDT|USDT",
            "binance|spot|BTC|USDT|USDT",
        ] {
            let error = validate_subscription("binance", "futures", symbol_key, "1m")
                .expect_err("a symbol_key from another market must be refused");
            assert!(
                error.contains("declares"),
                "{symbol_key} must be refused for identity mismatch: {error}"
            );
        }
        // 整批同样拒（评审跑的就是整批那条路）。
        let batch = vec![
            ("binance", "futures", BTC_KEY, "1m"),
            ("binance", "futures", "okx|spot|BTC|USDT|USDT", "1m"),
        ];
        let error = validate_subscription_batch(batch)
            .expect_err("one forged symbol_key must refuse the whole batch");
        assert!(error.contains("subscription #1"), "{error}");
    }

    /// 放行面自证：同源的 key 必须仍被接受，且大小写归一不影响判定。
    #[test]
    fn a_same_source_symbol_key_is_still_accepted() {
        let validated =
            validate_subscription("BINANCE", "Futures", "Binance|FUTURES|BTC|USDT|USDT", "1m")
                .expect("the two identities agree; this must pass");
        assert_eq!(validated.symbol, "BTCUSDT");
    }

    /// **regression · 能力集合的两层表达。**
    ///
    /// 引擎集合必须是官方枚举的**子集**：评审变异「allowlist 偷偷加 `2m`」
    /// 14/14 存活，因为当时没有任何用例能看见能力集合被放宽。
    #[test]
    fn the_engine_interval_set_is_a_subset_of_the_official_enumeration() {
        for interval in ENGINE_KLINE_INTERVALS {
            assert!(
                BINANCE_USDM_OFFICIAL_KLINE_INTERVALS.contains(&interval),
                "{interval:?} is not in Binance's official kline enumeration; \
                 the engine must never offer an interval the exchange does not serve"
            );
        }
        // 真子集：官方有而引擎不提供的档位必须存在，否则"两层表达"是空话。
        assert_eq!(intervals_the_engine_declines(), vec!["1s", "1M"]);
    }

    /// **不在官方集合内的固定反例**：`2m` 交易所根本不提供，
    /// 无论 allowlist 怎么改都必须被拒。
    #[test]
    fn an_interval_the_exchange_does_not_serve_is_always_refused() {
        assert!(
            !BINANCE_USDM_OFFICIAL_KLINE_INTERVALS.contains(&"2m"),
            "the fixed counter-example must stay outside the official enumeration"
        );
        assert!(crate::auto_triggers::timeframe_to_ms("2m").is_some());
        let error = validate_subscription("binance", "futures", BTC_KEY, "2m")
            .expect_err("2m is not a Binance USDⓈ-M kline interval");
        assert!(error.contains("unsupported timeframe"), "{error}");
    }

    /// **措辞如实**：拒绝信息不得声称 allowlist 就是交易所全集。
    #[test]
    fn the_refusal_message_separates_engine_limits_from_exchange_limits() {
        let error =
            validate_subscription("binance", "futures", BTC_KEY, "7m").expect_err("7m is refused");
        assert!(
            error.contains("this engine streams only"),
            "the refusal must own the engine-side limit: {error}"
        );
        assert!(
            error.contains("1s") && error.contains("1M"),
            "the refusal must disclose what the exchange serves but the engine declines: {error}"
        );
    }

    /// **regression（High）· restore 必须重验持久行。**
    ///
    /// 评审反例：直接往订阅表里种一条历史 `7m` 行（模拟闭世界收紧之前落库、
    /// 或绕过入口写入的行），restore 返回 `started=1`——入口的闭世界只挡新的，
    /// 挡不住已经在库里的。
    #[tokio::test]
    async fn restore_refuses_persisted_rows_that_fail_the_current_closed_world() {
        let (store, path) = test_store().await;
        for (symbol_key, timeframe) in [
            (BTC_KEY, "7m"),                  // 交易所不提供的档位
            ("okx|spot|BTC|USDT|USDT", "1m"), // 另一个市场的 symbol_key
            ("not-a-symbol-key", "1m"),       // 解析不出 symbol
        ] {
            sqlx::query(
                "INSERT INTO market_stream_subscriptions \
                 (credential_id, symbol_key, timeframe, requested_at_ms) VALUES (?,?,?,?)",
            )
            .bind("cred_md_legacy")
            .bind(symbol_key)
            .bind(timeframe)
            .bind(1_000i64)
            .execute(store.pool())
            .await
            .expect("seed a historical subscription row");
        }

        let manager = MarketDataManager::new();
        let outcome = manager
            .restore_required_streams(&store)
            .await
            .expect("restore must answer");
        assert_eq!(
            outcome,
            RestoreOutcome {
                started: 0,
                rejected: 3,
                rejected_credential_ids: ["cred_md_legacy".to_string()].into_iter().collect(),
            },
            "a persisted row that fails the current closed world must not become a worker"
        );
        assert!(
            manager.inner.lock().await.is_empty(),
            "no worker may be registered for a refused row"
        );

        // **隔离不等于删除**：required 行留着，中断判据仍看得见这条缺口。
        let still_required = crate::auto_triggers::all_required_streams(&store)
            .await
            .expect("read required streams");
        assert_eq!(still_required.len(), 3);
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn ensure_task_does_not_respawn_a_live_task() {
        let manager = MarketDataManager::new();
        let first = manager
            .ensure_task("live".to_string(), || {
                tokio::spawn(async {
                    // 永不结束：模拟正常运行中的行情流。
                    futures_util::future::pending::<()>().await;
                })
            })
            .await;
        let second = manager
            .ensure_task("live".to_string(), || tokio::spawn(async {}))
            .await;
        assert!(first, "first ensure must spawn");
        assert!(!second, "a live task must not be respawned");
    }

    #[tokio::test]
    async fn ensure_task_restarts_a_dead_task() {
        let manager = MarketDataManager::new();
        assert!(
            manager
                .ensure_task("dead".to_string(), || tokio::spawn(async {}))
                .await
        );
        // 让第一个任务真正结束（模拟 panic 或提前返回后的死 worker）。
        tokio::task::yield_now().await;
        for _ in 0..50 {
            if manager
                .inner
                .lock()
                .await
                .get("dead")
                .map(|handle| handle.is_finished())
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let restarted = manager
            .ensure_task("dead".to_string(), || {
                tokio::spawn(async {
                    futures_util::future::pending::<()>().await;
                })
            })
            .await;
        // 只判 contains_key 的旧实现会在这里返回 false —— 行情流永不恢复。
        assert!(restarted, "a dead task must be restarted");
    }
}

#[derive(Debug, Deserialize)]
struct BinanceKlineEnvelope {
    #[serde(rename = "e")]
    event_type: Option<String>,
    #[serde(rename = "E")]
    event_time_ms: Option<i64>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "k")]
    kline: Option<BinanceKline>,
}

#[derive(Debug, Deserialize)]
struct BinanceCombinedEnvelope {
    #[allow(dead_code)]
    stream: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct BinanceKline {
    #[serde(rename = "t")]
    open_time_ms: i64,
    #[serde(rename = "T")]
    close_time_ms: i64,
    /// 上游自报的周期。**必须与本 manager 请求的 timeframe 逐字相符**
    /// （regression）——此前它带着 `#[allow(dead_code)]`，
    /// 正说明"消费侧从不校验上游身份"。
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "x")]
    closed: bool,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trade_count: Option<i64>,
}

async fn run_kline_once(
    credential_id: &str,
    store: Store,
    symbol_key: &str,
    timeframe: &str,
) -> anyhow::Result<()> {
    let ws_transport =
        crate::venue::ws_transport_for_credential(credential_id).map_err(anyhow::Error::msg)?;

    let symbol = symbol_from_symbol_key(symbol_key)
        .ok_or_else(|| anyhow::anyhow!("invalid symbol_key: {symbol_key}"))?;
    let stream_symbol = symbol.to_lowercase();
    let interval = timeframe.to_lowercase();

    let legacy_ws_url = format!(
        "{}/ws/{stream_symbol}@kline_{interval}",
        ws_transport.base_url
    );
    let quote_permit =
        crate::simulation_quote::prepare(&store, credential_id, symbol_key, timeframe).await?;
    let ws_url = quote_permit
        .as_ref()
        .map(|permit| permit.url.clone())
        .unwrap_or(legacy_ws_url);
    let trust = ws_transport.trust_profile();
    tracing::info!(
        credential_id,
        symbol_key,
        timeframe,
        upstream_venue = ws_transport
            .venue
            .map(|value| value.to_string())
            .as_deref()
            .unwrap_or("unconfigured"),
        transport = ws_transport.mode.as_str(),
        built_in_roots_preserved = trust.built_in_roots_preserved,
        added_test_roots = trust.added_test_roots,
        "kline stream connecting"
    );

    let connector = ws_transport.connector().map_err(anyhow::Error::msg)?;
    // ****：connect 与
    // read 都要有应用级上限。此前两者都没有——对端握手之后不再发 frame、
    // 也不 close，`read.next()` 就永远停在 await 上：行情静默停滞，
    // 而外层重连循环永远拿不到返回，看起来一切正常。
    let (connect_deadline, idle_deadline) = crate::binance_futures::ws_timeouts_from_env();
    let (ws, resp) = tokio::time::timeout(
        connect_deadline,
        tokio_tungstenite::connect_async_tls_with_config(ws_url, None, false, connector),
    )
    .await
    .map_err(|_elapsed| {
        anyhow::anyhow!(
            "kline stream connect exceeded {}ms deadline",
            connect_deadline.as_millis()
        )
    })??;
    if quote_permit.is_some()
        && resp
            .headers()
            .get("X-Fixture-Label")
            .and_then(|value| value.to_str().ok())
            != Some(crate::simulation_quote::FIXTURE)
    {
        anyhow::bail!("simulation quote handshake source label mismatch");
    }
    let (mut write, mut read) = ws.split();

    loop {
        let next = match crate::binance_futures::next_frame_within(idle_deadline, &mut read).await {
            crate::binance_futures::StreamPoll::Frame(msg) => msg,
            // 对端正常结束：交给外层重连。
            crate::binance_futures::StreamPoll::Ended => break,
            crate::binance_futures::StreamPoll::IdleTimeout => {
                // **不得**在这里静默 break：静默 break 与"流正常结束"无法区分，
                // 上层看不出这是一次超时。失败必须带理由地报出去。
                tracing::error!(
                    credential_id,
                    symbol_key,
                    timeframe,
                    idle_deadline_ms = idle_deadline.as_millis() as u64,
                    "kline stream produced no frame within its idle deadline; \
                     treating the stream as dead and reconnecting"
                );
                anyhow::bail!(
                    "kline stream idle beyond {}ms deadline",
                    idle_deadline.as_millis()
                );
            }
        };
        let msg = next?;
        if let Message::Text(text) = &msg {
            if crate::simulation_quote::is_quote(text) {
                // Freeze the native receipt before any validation/DB waiting.
                let received_at_ms = now_ms();
                tokio::time::timeout(
                    market_data_db_deadline(),
                    crate::simulation_quote::ingest(
                        &store,
                        quote_permit.as_ref(),
                        text,
                        received_at_ms,
                    ),
                )
                .await
                .map_err(|_| anyhow::anyhow!("simulation quote persistence deadline exceeded"))??;
                continue;
            }
        }
        match handle_kline_frame(
            &store,
            credential_id,
            symbol_key,
            timeframe,
            connect_deadline,
            &mut write,
            msg,
        )
        .await?
        {
            KlineFrameStep::Continue => {}
            KlineFrameStep::Stop => break,
        }
    }

    Ok(())
}

/// 一帧处理完之后，读循环该继续还是该收。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KlineFrameStep {
    Continue,
    Stop,
}

/// **kline 读循环的帧派发**——生产循环里唯一的 `match msg`。
///
/// `Pong` 的有界写此前直接写在
/// `run_kline_once` 的循环体里，而那个循环需要一条真的 websocket 才能进入，
/// 于是**没有任何用例驱动过它**——把 `send_frame_within` 换成裸 `send`，
/// 变异 1/1 存活。把派发抽成这一个函数（生产循环唯一的调用点），
/// 它就可以被一个"永远 pending 的 sink"直接驱动：绕过有界写的实现会挂住，
/// 用例因此变红。
pub(crate) async fn handle_kline_frame<S>(
    store: &Store,
    credential_id: &str,
    symbol_key: &str,
    timeframe: &str,
    write_deadline: Duration,
    write: &mut S,
    msg: Message,
) -> anyhow::Result<KlineFrameStep>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    match msg {
        Message::Text(text) => {
            let outcome =
                handle_kline_text(store, credential_id, symbol_key, timeframe, &text).await;
            // **报出去 + 变成控制流**：
            // 此前这里是 `outcome.report(...); Ok(Continue)`——两条语句之间
            // 没有任何依赖，删掉 `report` 这一整行 14/14 变异存活，
            // 因为落库失败**不改变任何可观测行为**。现在结果本身决定循环怎么走。
            outcome.report(credential_id, symbol_key, timeframe)
        }
        Message::Ping(payload) => {
            // 写侧同样有界：对端不读时写缓冲填满，
            // `send` 会永远挂住，表现与 read 挂住一模一样。
            crate::binance_futures::send_frame_within(
                write_deadline,
                write,
                Message::Pong(payload),
            )
            .await?;
            Ok(KlineFrameStep::Continue)
        }
        Message::Close(frame) => {
            tracing::warn!(credential_id, ?frame, "kline stream closed");
            Ok(KlineFrameStep::Stop)
        }
        _ => Ok(KlineFrameStep::Continue),
    }
}

/// 一帧 kline 文本被消费之后的**可观测结果**。
///
/// 此前这条路径**没有返回值**，
/// 落库两步都是 `let _ = ...`。于是三件事无法区分：写成功、写失败、
/// 这帧压根不该被采信。DB 故障因此静默通过，而心跳表里没有新行——
/// 上层只会看到"这条流很久没 bar 了"，看不到"库在报错"。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KlineIngest {
    /// 与本流身份无关、也不构成真值的帧（非 kline 事件、未收盘的 bar）。
    Ignored(&'static str),
    /// **身份或时间不可信**——一律不落库，也不驱动任何健康判定。
    Rejected(String),
    /// bar 事件与心跳都已持久化。
    Recorded,
    /// 落库失败或超时。**不得**被当成健康：既没有心跳，也必须报出去。
    PersistFailed(String),
}

impl KlineIngest {
    /// 把结果**报出去，并把它变成读循环的控制流**。
    ///
    /// **`PersistFailed` 不得只写日志。**
    ///
    /// 此前这个函数只 `tracing::error!` 就返回 `()`，调用方随后无条件
    /// `Ok(Continue)`：一条**每根 bar 都落库失败**的流会一直被当成正常在跑的
    /// 流——socket 活着、循环在转、进程健康，只有日志里在刷错误。
    /// 唯一的兜底是行情中断判据 3×bar 之后 arm，期间决策循环照跑。
    ///
    /// **本轮的处置：上抛，触发重连**（两个候选里选它，理由写在 R10R-REPORT）：
    /// * 「结构化健康故障」需要把故障记到某处让判据读——而**能记的地方正是
    ///   刚刚失败的那个库**，一次 DB 故障会同时吃掉故障记录本身；
    /// * 上抛让 `run_kline_once` 带着理由返回，外层重连循环 warn + 2s 退避后
    ///   重建连接。落库失败若是瞬时的（连接被回收、超时），重连即自愈；
    ///   若是持续的，这条流就会持续处在"连上→失败→退避"的可见循环里，
    ///   同时心跳不刷新，中断判据照常 arm——两条路都不静默。
    ///
    /// 一根落库失败的 bar **不会**被上游补发，所以这里不试图重放：
    /// 缺口由心跳陈旧如实暴露，而不是被一句日志盖过去。
    pub(crate) fn report(
        self,
        credential_id: &str,
        symbol_key: &str,
        timeframe: &str,
    ) -> anyhow::Result<KlineFrameStep> {
        match self {
            Self::Ignored(_) | Self::Recorded => Ok(KlineFrameStep::Continue),
            Self::Rejected(reason) => {
                tracing::warn!(
                    credential_id,
                    symbol_key,
                    timeframe,
                    reason = %reason,
                    "kline frame rejected: it does not belong to this stream identity"
                );
                Ok(KlineFrameStep::Continue)
            }
            Self::PersistFailed(error) => {
                tracing::error!(
                    credential_id,
                    symbol_key,
                    timeframe,
                    error = %error,
                    "kline bar could not be persisted; this stream has no fresh heartbeat \
                     and must not be treated as healthy; dropping the connection so the \
                     failure cannot masquerade as a running stream"
                );
                Err(anyhow::anyhow!(
                    "kline bar persistence failed for {symbol_key} {timeframe}: {error}"
                ))
            }
        }
    }
}

/// bar 落库的上界。挂住的 DB 与失败的 DB 一样，不得让读循环无限期停在 await 上。
fn market_data_db_deadline() -> Duration {
    let ms = std::env::var("RUSTCORE_MARKET_DATA_DB_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5_000)
        .clamp(100, 60_000);
    Duration::from_millis(ms)
}

/// 未来 bar 的容忍上界：只给时钟偏差留余量，不给"上游发来的另一条流"留余量。
pub(crate) const MAX_BAR_CLOCK_SKEW_MS: i64 = 60_000;

pub(crate) async fn handle_kline_text(
    store: &Store,
    credential_id: &str,
    symbol_key: &str,
    timeframe: &str,
    text: &str,
) -> KlineIngest {
    // Binance sends either:
    // - direct: { e: "kline", s: "...", k: {...} }
    // - combined: { stream: "...", data: {...} }
    let direct: Option<BinanceKlineEnvelope> = serde_json::from_str(text).ok();
    let env = if let Some(d) = direct {
        d
    } else {
        let combined: Option<BinanceCombinedEnvelope> = serde_json::from_str(text).ok();
        let Some(c) = combined else {
            return KlineIngest::Ignored("frame is not a Binance envelope");
        };
        let Some(data) = c.data else {
            return KlineIngest::Ignored("combined envelope carries no data");
        };
        let parsed = serde_json::from_value::<BinanceKlineEnvelope>(data).ok();
        let Some(parsed) = parsed else {
            return KlineIngest::Ignored("combined payload is not a kline envelope");
        };
        parsed
    };

    let t = env.event_type.as_deref().unwrap_or("").trim();
    if t != "kline" {
        return KlineIngest::Ignored("not a kline event");
    }
    let Some(k) = env.kline else {
        return KlineIngest::Ignored("kline event without a kline body");
    };

    // **逐字绑定上游身份。**
    //
    // 此前这里完全不比对 `env.symbol` / `k.interval`：一条 BTC/1m 的 manager
    // 收到 ETH/5m 的收盘帧，会照原样按**本 manager 的** symbol_key/timeframe
    // 落 `bar_closed` 并写心跳——错误的 symbol 因此被判成"行情健康"，
    // 而真正的 BTC/1m 可能已经死了（评审实跑 1 条反例证实）。
    //
    // 身份判定放在 `closed` 之前：一条不属于本流的帧，无论收没收盘都不该
    // 被本流看见。
    let Some(expected_symbol) = symbol_from_symbol_key(symbol_key) else {
        return KlineIngest::Rejected(format!(
            "stream symbol_key {symbol_key:?} does not resolve to a Binance symbol"
        ));
    };
    match env.symbol.as_deref().map(str::trim) {
        Some(upstream) if upstream.eq_ignore_ascii_case(&expected_symbol) => {}
        Some(upstream) => {
            return KlineIngest::Rejected(format!(
                "upstream symbol {upstream:?} does not match this stream's {expected_symbol:?}"
            ))
        }
        None => {
            return KlineIngest::Rejected(
                "upstream frame carries no symbol; it cannot be bound to this stream".to_string(),
            )
        }
    }
    // **interval 的比较必须区分大小写。**
    //
    // 此前这里用 `eq_ignore_ascii_case`：Binance 用 `M` 表示**月线**、`m` 表示
    // **分钟线**，折叠之后一条 `1M`（月线）收盘帧会被 `1m`（分钟线）这条流
    // 当成自己的 bar 收下——落 `bar_closed`、刷新心跳，于是"分钟线健康"这个
    // 判断由一根月线撑着，而真正的分钟流可能早就死了。
    //
    // 订阅入口对 `1M` 是**拒绝**的（`validate_subscription` 在 lowercase 之前
    // 就挡下带 `M` 的 timeframe）。入口拒、消费侧收，两边理由不一致——
    // 消费侧于是成了那道判据的绕行路。现在两侧同一条纪律：m/M 语义保留。
    //
    // symbol 那一侧仍是大小写不敏感的：`BTCUSDT`/`btcusdt` 是同一个标的，
    // 没有任何一对交易所 symbol 靠大小写区分语义。interval 有，所以只有它按
    // 逐字节比。
    if k.interval.trim() != timeframe.trim() {
        return KlineIngest::Rejected(format!(
            "upstream interval {:?} does not match this stream's {timeframe:?} \
             (`m` is minutes and `M` is months: the comparison is case-sensitive)",
            k.interval.trim()
        ));
    }

    if !k.closed {
        return KlineIngest::Ignored("bar is not closed yet");
    }

    // 时间合理性：**未来 bar 不收**。一根还没结束的 bar 不可能已经收盘，
    // 收下它会把心跳推到未来，让"陈旧"判据在真正断流之后仍然看着新鲜。
    let close_ts = k.close_time_ms;
    if close_ts <= 0 || k.open_time_ms <= 0 || k.open_time_ms >= close_ts {
        return KlineIngest::Rejected(format!(
            "implausible bar window openTs={} closeTs={close_ts}",
            k.open_time_ms
        ));
    }
    let now = now_ms();
    if close_ts > now.saturating_add(MAX_BAR_CLOCK_SKEW_MS) {
        return KlineIngest::Rejected(format!(
            "bar closes {close_ts} which is beyond now={now} plus the \
             {MAX_BAR_CLOCK_SKEW_MS}ms clock-skew allowance"
        ));
    }

    // Authoritative bar close event.
    let eid = sha256_hex(&format!(
        "bar_closed|{}|{}|{}|{}",
        credential_id, symbol_key, timeframe, close_ts
    ));

    let ctx = EventContext {
        bot_uid: "".to_string(),
        run_id: "".to_string(),
        cycle_id: "".to_string(),
        idempotency_key: "".to_string(),
        client_order_id_short: "".to_string(),
        exchange: "binance".to_string(),
        market_kind: "futures".to_string(),
        symbol_key: symbol_key.to_string(),
    };

    let payload = serde_json::json!({
        "paper": false,
        "source": "binance_ws_kline",
        "symbolKey": symbol_key,
        "timeframe": timeframe,
        "eventTimeMs": env.event_time_ms,
        "symbol": env.symbol,
        "openTs": k.open_time_ms,
        "closeTs": k.close_time_ms,
        "open": k.open,
        "high": k.high,
        "low": k.low,
        "close": k.close,
        "volume": k.volume,
        "tradeCount": k.trade_count,
        "emittedAtMs": now,
    });

    // **frame → DB 的每一步都有上界**：没有上界时一次挂住的 DB 会把整条读
    // 循环停在 await 上，而外层重连循环永远拿不到返回——与 R7 修掉的 WS
    // 读侧无界是同一个形状，只是换到了 sink 这一端。
    let deadline = market_data_db_deadline();
    match tokio::time::timeout(
        deadline,
        persist_closed_bar(
            store,
            credential_id,
            symbol_key,
            timeframe,
            &ctx,
            payload,
            eid,
            close_ts,
        ),
    )
    .await
    {
        Ok(Ok(())) => KlineIngest::Recorded,
        Ok(Err(error)) => KlineIngest::PersistFailed(format!("{error:#}")),
        Err(_elapsed) => KlineIngest::PersistFailed(format!(
            "bar persistence exceeded {}ms deadline",
            deadline.as_millis()
        )),
    }
}

/// bar 事件 + 心跳的落库。**两步都上抛错误**——调用方据此判 `PersistFailed`。
#[allow(clippy::too_many_arguments)]
async fn persist_closed_bar(
    store: &Store,
    credential_id: &str,
    symbol_key: &str,
    timeframe: &str,
    ctx: &EventContext,
    payload: serde_json::Value,
    eid: String,
    close_ts: i64,
) -> anyhow::Result<()> {
    let mut tx = store.pool().begin().await?;
    crate::svc::emit_event(
        &mut tx,
        store.kind(),
        credential_id,
        "bar_closed",
        ctx,
        "",
        None,
        Some(payload),
        Some(eid),
    )
    .await
    .map_err(|status| anyhow::anyhow!("emit bar_closed failed: {status}"))?;
    tx.commit().await?;

    // 按**真实 stream identity**（credential + symbol + timeframe）记心跳。
    // 行情中断触发源据此逐流判定——事件表里 timeframe 只活在 payload JSON 里，
    // SQL 够不到，靠 MAX(ts_ms) 会让健康的流掩盖已死的流。
    record_market_stream_heartbeat(store, credential_id, symbol_key, timeframe, close_ts).await?;
    Ok(())
}

/// 记录一条 market stream 的最近收盘时间。**只在真的收到 bar 时写**——
/// 表里没有的流 = 从未见过的流，判据据此天然满足"从未有过 bar ≠ 中断"。
pub(crate) async fn record_market_stream_heartbeat(
    store: &Store,
    credential_id: &str,
    symbol_key: &str,
    timeframe: &str,
    last_bar_ms: i64,
) -> Result<(), sqlx::Error> {
    let now_ms = now_ms();
    let statement = match store.kind() {
        crate::store::DbKind::MySql => {
            "INSERT INTO market_stream_health (credential_id, symbol_key, timeframe, last_bar_ms, updated_at_ms) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE last_bar_ms=GREATEST(last_bar_ms, VALUES(last_bar_ms)), updated_at_ms=VALUES(updated_at_ms)"
        }
        _ => {
            "INSERT INTO market_stream_health (credential_id, symbol_key, timeframe, last_bar_ms, updated_at_ms) VALUES (?,?,?,?,?) ON CONFLICT(credential_id, symbol_key, timeframe) DO UPDATE SET last_bar_ms=MAX(last_bar_ms, excluded.last_bar_ms), updated_at_ms=excluded.updated_at_ms"
        }
    };
    sqlx::query(statement)
        .bind(credential_id.trim())
        .bind(symbol_key.trim())
        .bind(timeframe.trim().to_lowercase())
        .bind(last_bar_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await?;
    Ok(())
}
