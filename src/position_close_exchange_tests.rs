//! WO-M1（D29 / D29a）· 交易所账户单仓平仓。
//!
//! 生产入口：真实 gRPC（`add_engine_services` + auth interceptor）上的
//! GetPositionCloseTarget / ClosePosition / GetPositionCloseReceipt /
//! ReconcileNow；保护单善后是 boot 后台任务每轮调用的同一个
//! `protection_cleanup::drive_due_obligations`；成交经用户流 reader 落库所用的
//! 同一个 `BinanceEventSink::handle_ws_envelope`（帧按交易所冻结形状解析）。
//! 交易所端只有一处替身：TLS 回环上的 [`TlsFakeVenue`]——凭据配置显式
//! testnet venue，生产客户端只经 fault-proxy 传输（https + 固定 CA）出站，
//! 签名、限流与 HTTP 全是生产路径。仓位的执行来源（provenance）由生产写入方
//! `position_close::persist_entry_provenance` 写下，不手写同形 JSON。
//!
//! 两侧真值同源：替身只回答它自己账本里的事实；断言读的是生产写入方落下
//! 的持久行与替身真实收到的请求。

use std::path::PathBuf;
use std::time::Duration;

use sqlx::Row;
use tonic::Request;

use crate::k04_fake_venue::{algo_order, position as venue_position, OrderBehavior, TlsFakeVenue};
use crate::proto_v2 as p;
use crate::store::Store;

const CRED: &str = "61";
const OTHER_CRED: &str = "62";
const BTC: &str = "binance|futures|BTC|USDT|USDT";
const ETH: &str = "binance|futures|ETH|USDT|USDT";
const TOKEN: &str = "wo-m1-close-test-token";
const BOT: &str = "bot_wom1";
const POS: &str = "pos_wom1_btc";
const POS_ETH: &str = "pos_wom1_eth";
const POS_OTHER: &str = "pos_wom1_other";
const SL_ALGO: i64 = 7101;
const TP_ALGO: i64 = 7102;
const ETH_SL_ALGO: i64 = 7201;

struct EnvGuard {
    name: String,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn remove(name: &str) -> Self {
        let previous = std::env::var_os(name);
        std::env::remove_var(name);
        Self {
            name: name.into(),
            previous,
        }
    }

    fn set(name: impl Into<String>, value: &str) -> Self {
        let name = name.into();
        let previous = std::env::var_os(&name);
        std::env::set_var(&name, value);
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(&self.name, value),
            None => std::env::remove_var(&self.name),
        }
    }
}

/// 交易所凭据的真实配置形状：显式 testnet venue + 密钥 + fault-proxy 传输。
/// 进程级 env 只在 env 串行锁内改写（与仓内所有 env 写入方同一把锁）。
struct ExchangeEnv {
    _vars: Vec<EnvGuard>,
    _routes: Vec<crate::k04_fake_venue::FixtureRouteGuard>,
    _serial: std::sync::MutexGuard<'static, ()>,
}

fn exchange_env(venue: &TlsFakeVenue, http_timeout_ms: Option<u64>) -> ExchangeEnv {
    exchange_env_with_venues(
        venue,
        http_timeout_ms,
        &[
            (CRED, crate::venue::Venue::Testnet),
            (OTHER_CRED, crate::venue::Venue::Testnet),
        ],
    )
}

fn exchange_env_with_venues(
    venue: &TlsFakeVenue,
    http_timeout_ms: Option<u64>,
    credentials: &[(&str, crate::venue::Venue)],
) -> ExchangeEnv {
    let serial = crate::binance_futures::tests::env_serial_guard();
    let direct = credentials
        .iter()
        .any(|(_, venue)| *venue == crate::venue::Venue::Mainnet);
    let mut routes = Vec::new();
    let mut vars = vec![
        if direct {
            EnvGuard::remove("BINANCE_FUTURES_BASE_URL")
        } else {
            EnvGuard::set("BINANCE_FUTURES_BASE_URL", "")
        },
        EnvGuard::set(
            "BINANCE_FAULT_PROXY_REST_BASE_URL",
            &if direct {
                String::new()
            } else {
                venue.rest_base_url()
            },
        ),
        EnvGuard::set(
            "BINANCE_FAULT_PROXY_WS_BASE_URL",
            &if direct {
                String::new()
            } else {
                venue.ws_base_url()
            },
        ),
        EnvGuard::set(
            "BINANCE_FAULT_PROXY_CA_PEM_PATH",
            venue.ca_path.to_str().expect("utf-8 CA path"),
        ),
        EnvGuard::set("ENGINE_LIVE_ENABLED", "false"),
        EnvGuard::set("ENGINE_STAGE_MAX", "demo"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC", "100000"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_BURST", "100000"),
        EnvGuard::set(
            "BINANCE_HTTP_TIMEOUT_MS",
            &http_timeout_ms.unwrap_or(5_000).to_string(),
        ),
    ];
    for &(credential, credential_venue) in credentials {
        if direct {
            routes.push(venue.route_direct_credential(credential, credential_venue));
        }
        let (key_env, secret_env) = crate::binance_futures::key_env_names(credential);
        vars.push(EnvGuard::set(
            crate::venue::venue_env_name(credential),
            &credential_venue.to_string(),
        ));
        vars.push(EnvGuard::set(key_env, &format!("{credential}-api-key")));
        vars.push(EnvGuard::set(secret_env, "wo-m1-secret"));
    }
    ExchangeEnv {
        _vars: vars,
        _serial: serial,
        _routes: routes,
    }
}

pub(crate) struct Engine {
    pub(crate) store: Store,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
    pub(crate) market_data: crate::market_data::MarketDataManager,
}

/// 引擎时间观领先墙钟固定毫秒数（生产 `Clock` 注入缝）：派发事务写下的
/// 原单 `created_at_ms` 落在未来，而对账按墙钟取 positionRisk 读数时刻。
struct AheadClock(i64);

impl crate::svc::Clock for AheadClock {
    fn now_ms(&self) -> i64 {
        crate::svc::now_ms() + self.0
    }
}

impl Engine {
    pub(crate) async fn start(store: Store) -> Self {
        Self::start_with_clock(store, None).await
    }

    async fn start_with_clock(
        store: Store,
        clock: Option<std::sync::Arc<dyn crate::svc::Clock>>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the owned engine listener");
        let addr = listener.local_addr().expect("engine address");
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .expect("engine incoming");
        // 不起会主动联网的后台流 / 周期对账：对账由用例经 ReconcileNow 驱动。
        // 交易所过滤器用固定真值（stepSize 0.0001）：公开 exchangeInfo 读取
        // 不走 fault-proxy 传输，绝不能从测试里打到真实交易所。
        let mut execution = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"));
        if let Some(clock) = clock {
            execution = execution.with_clock(clock);
        }
        let market_data = execution.market_data_for_tests();
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            execution.clone(),
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
            crate::svc_v2::EngineExecutionV2Service::with_shared_execution(
                store.clone(),
                execution,
            ),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![TOKEN.into()])),
        );
        let task = tokio::spawn(async move {
            router
                .serve_with_incoming(incoming)
                .await
                .expect("owned engine server");
        });
        Self {
            store,
            addr,
            task,
            market_data,
        }
    }

    pub(crate) async fn rpc<Q, R>(&self, path: &'static str, request: Q) -> Result<R, tonic::Status>
    where
        Q: prost::Message + Default + Send + Sync + 'static,
        R: prost::Message + Default + Send + Sync + 'static,
    {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.addr))
            .expect("engine endpoint")
            .connect()
            .await
            .expect("connect the owned engine");
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.expect("engine ready");
        let mut request = Request::new(request);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TOKEN}").parse().expect("metadata"),
        );
        client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .map(tonic::Response::into_inner)
    }

    pub(crate) async fn target(
        &self,
        credential: &str,
        position: &str,
    ) -> p::GetPositionCloseTargetResponse {
        self.rpc(
            "/execution_engine.v2.EngineExecution/GetPositionCloseTarget",
            p::GetPositionCloseTargetRequest {
                credential_id: credential.into(),
                position_id: position.into(),
                schema_version: 1,
            },
        )
        .await
        .expect("GetPositionCloseTarget")
    }

    async fn close_request(&self, credential: &str, position: &str) -> p::ClosePositionRequest {
        let target = self.target(credential, position).await;
        assert!(
            target.available,
            "exchange target must be available: {}",
            target.reason_code
        );
        p::ClosePositionRequest {
            credential_id: credential.into(),
            position_id: position.into(),
            expected_revision: target.target.expect("target").revision,
            expected_boot_id: target.engine_boot_id,
            request_ref: ulid::Ulid::new().to_string(),
            actor_member_id: 41,
            schema_version: 1,
        }
    }

    pub(crate) async fn close(
        &self,
        request: &p::ClosePositionRequest,
    ) -> p::ClosePositionResponse {
        self.rpc(
            "/execution_engine.v2.EngineExecution/ClosePosition",
            request.clone(),
        )
        .await
        .expect("ClosePosition")
    }

    pub(crate) async fn receipt(
        &self,
        request: &p::ClosePositionRequest,
    ) -> p::ClosePositionResponse {
        self.rpc(
            "/execution_engine.v2.EngineExecution/GetPositionCloseReceipt",
            p::GetPositionCloseReceiptRequest {
                credential_id: request.credential_id.clone(),
                request_ref: request.request_ref.clone(),
                actor_member_id: request.actor_member_id,
                schema_version: 1,
            },
        )
        .await
        .expect("GetPositionCloseReceipt")
    }

    async fn execute_v2(
        &self,
        credential: &str,
        run_mode: &str,
        intent: p::OrderIntent,
    ) -> p::ExecuteIntentsResponse {
        self.rpc(
            "/execution_engine.v2.EngineExecution/ExecuteIntents",
            p::ExecuteIntentsRequest {
                credential_id: credential.into(),
                run_mode: run_mode.into(),
                intents: vec![intent],
                schema_version: 1,
                ..Default::default()
            },
        )
        .await
        .expect("ExecuteIntents")
    }

    async fn set_kill_switch(&self, scope: &str, level: &str) -> p::SetKillSwitchResponse {
        self.rpc(
            "/execution_engine.v2.EngineExecution/SetKillSwitch",
            p::SetKillSwitchRequest {
                scope_key: scope.into(),
                level: level.into(),
                reason: "wo-m6 emergency flatten with live disabled".into(),
                trigger_source: "manual_api".into(),
                actor: "wo-m6-test".into(),
                schema_version: 1,
                ..Default::default()
            },
        )
        .await
        .expect("SetKillSwitch")
    }

    pub(crate) async fn reconcile_now(&self, credential: &str) {
        self.rpc::<_, p::ReconcileNowResponse>(
            "/execution_engine.v2.EngineExecution/ReconcileNow",
            p::ReconcileNowRequest {
                credential_id: credential.into(),
                reason: "wo-m1 exchange close truth".into(),
                schema_version: 1,
            },
        )
        .await
        .expect("ReconcileNow must complete against the fake venue");
    }

    pub(crate) async fn stop(self) -> Store {
        self.task.abort();
        let _ = self.task.await;
        self.store
    }
}

enum Database {
    Sqlite(PathBuf),
    MySql(crate::store::mysql_test_support::MySqlScratch),
}

struct Harness {
    venue: TlsFakeVenue,
    _env: ExchangeEnv,
    engine: Engine,
    database: Database,
}

async fn sqlite_store() -> (Store, PathBuf) {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!("wo-m1-close-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("connect sqlite store");
    (store, path)
}

impl Harness {
    async fn sqlite(behaviors: Vec<OrderBehavior>, http_timeout_ms: Option<u64>) -> Self {
        let venue = TlsFakeVenue::start();
        let env = exchange_env(&venue, http_timeout_ms);
        let (store, path) = sqlite_store().await;
        Self::finish(venue, env, store, Database::Sqlite(path), behaviors, None).await
    }

    async fn sqlite_with_venues(credentials: &[(&str, crate::venue::Venue)]) -> Self {
        let venue = TlsFakeVenue::start();
        let env = exchange_env_with_venues(&venue, None, credentials);
        let (store, path) = sqlite_store().await;
        Self::finish(venue, env, store, Database::Sqlite(path), vec![], None).await
    }

    /// 同一套夹具，但引擎时间观领先墙钟 `ahead_ms`（见 [`AheadClock`]）。
    async fn sqlite_with_engine_ahead(behaviors: Vec<OrderBehavior>, ahead_ms: i64) -> Self {
        let venue = TlsFakeVenue::start();
        let env = exchange_env(&venue, None);
        let (store, path) = sqlite_store().await;
        let clock: std::sync::Arc<dyn crate::svc::Clock> =
            std::sync::Arc::new(AheadClock(ahead_ms));
        Self::finish(
            venue,
            env,
            store,
            Database::Sqlite(path),
            behaviors,
            Some(clock),
        )
        .await
    }

    async fn mysql(test: &str, behaviors: Vec<OrderBehavior>) -> Option<Self> {
        let scratch =
            crate::store::mysql_test_support::MySqlScratch::create("wom1close", test).await?;
        let venue = TlsFakeVenue::start();
        let env = exchange_env(&venue, None);
        let store = scratch.connect_store().await;
        Some(Self::finish(venue, env, store, Database::MySql(scratch), behaviors, None).await)
    }

    async fn finish(
        venue: TlsFakeVenue,
        env: ExchangeEnv,
        store: Store,
        database: Database,
        behaviors: Vec<OrderBehavior>,
        clock: Option<std::sync::Arc<dyn crate::svc::Clock>>,
    ) -> Self {
        venue.with_book(|book| {
            book.order_behaviors = behaviors.into_iter().collect();
            book.positions = vec![
                venue_position("BTCUSDT", "1"),
                venue_position("ETHUSDT", "-2"),
            ];
            book.open_algos = vec![
                algo_order("BTCUSDT", SL_ALGO, "wom1slbtc", "NEW"),
                algo_order("BTCUSDT", TP_ALGO, "wom1tpbtc", "NEW"),
                algo_order("ETHUSDT", ETH_SL_ALGO, "wom1sleth", "NEW"),
            ];
        });
        seed_exchange_position(&store, CRED, BOT, POS, BTC, "long", "1").await;
        seed_stop(&store, CRED, BOT, POS, BTC, "sl", SL_ALGO, "wom1slbtc").await;
        seed_stop(&store, CRED, BOT, POS, BTC, "tp", TP_ALGO, "wom1tpbtc").await;
        seed_exchange_position(&store, CRED, "bot_wom1_eth", POS_ETH, ETH, "short", "2").await;
        seed_stop(
            &store,
            CRED,
            "bot_wom1_eth",
            POS_ETH,
            ETH,
            "sl",
            ETH_SL_ALGO,
            "wom1sleth",
        )
        .await;
        seed_exchange_position(
            &store,
            OTHER_CRED,
            "bot_wom1_other",
            POS_OTHER,
            BTC,
            "long",
            "1",
        )
        .await;
        seed_stop(
            &store,
            OTHER_CRED,
            "bot_wom1_other",
            POS_OTHER,
            BTC,
            "sl",
            7301,
            "wom1slother",
        )
        .await;
        let engine = Engine::start_with_clock(store, clock).await;
        // 生产真值把三张 Algo 保护单确认为「交易所上活着」。
        engine.reconcile_now(CRED).await;
        Self {
            venue,
            _env: env,
            engine,
            database,
        }
    }

    fn store(&self) -> &Store {
        &self.engine.store
    }

    fn order_posts(&self) -> Vec<crate::k04_fake_venue::RecordedRequest> {
        self.venue
            .requests()
            .into_iter()
            .filter(|request| request.is("POST", "/fapi/v1/order"))
            .collect()
    }

    fn deletes(&self) -> Vec<crate::k04_fake_venue::RecordedRequest> {
        self.venue
            .requests()
            .into_iter()
            .filter(|request| request.method == "DELETE")
            .collect()
    }

    fn algo_status(&self, algo_id: i64) -> String {
        self.venue.with_book(|book| {
            book.open_algos
                .iter()
                .chain(book.algo_history.iter())
                .find(|algo| algo["algoId"].as_i64() == Some(algo_id))
                .and_then(|algo| algo["algoStatus"].as_str().map(str::to_string))
                .expect("fake venue algo")
        })
    }

    fn venue_amount(&self, symbol: &str) -> String {
        self.venue.with_book(|book| {
            book.positions
                .iter()
                .find(|position| position["symbol"].as_str() == Some(symbol))
                .and_then(|position| position["positionAmt"].as_str().map(str::to_string))
                .expect("fake venue position")
        })
    }

    async fn cleanup(self) {
        let store = self.engine.stop().await;
        store.pool().close().await;
        match self.database {
            Database::Sqlite(path) => {
                for suffix in ["", "-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
                }
            }
            Database::MySql(scratch) => scratch.cleanup().await,
        }
    }
}

/// 一个已经开着的交易所仓位。执行来源由生产写入方
/// `persist_entry_provenance` 在本凭据的 venue 配置下写入（venue=testnet、
/// run_mode=demo），与生产 Entry 事务内的那次调用同一个函数。
async fn seed_exchange_position(
    store: &Store,
    credential: &str,
    bot: &str,
    position_id: &str,
    symbol_key: &str,
    direction: &str,
    qty: &str,
) {
    seed_exchange_position_in(
        store,
        credential,
        bot,
        position_id,
        symbol_key,
        direction,
        qty,
        "demo",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_exchange_position_in(
    store: &Store,
    credential: &str,
    bot: &str,
    position_id: &str,
    symbol_key: &str,
    direction: &str,
    qty: &str,
    run_mode: &str,
) {
    let entry_order = format!("entry_{position_id}");
    let mut tx = store.pool().begin().await.expect("begin seed");
    sqlx::query(
        "INSERT INTO position_state (position_id, credential_id, bot_uid, symbol_key, direction, \
         entry_epoch, exit_epoch, state, active_key, qty, entry_price, opened_at_ms, updated_at_ms, \
         close_revision) VALUES (?, ?, ?, ?, ?, 1, 0, 'protected', ?, ?, '100', 1, 1, 1)",
    )
    .bind(position_id)
    .bind(credential)
    .bind(bot)
    .bind(symbol_key)
    .bind(direction)
    .bind(format!("{bot}|{symbol_key}|{direction}"))
    .bind(qty)
    .execute(&mut *tx)
    .await
    .expect("seed exchange position");
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, side, \
         order_type, quantity, reduce_only, status, order_role, position_id, created_at_ms, \
         updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, 'binance', 'futures', ?, ?, ?, 'market', ?, 0, \
         'filled', 'entry', ?, 1, 1)",
    )
    .bind(&entry_order)
    .bind(credential)
    .bind(bot)
    .bind(format!("idem_{entry_order}"))
    .bind(format!("co{position_id}"))
    .bind(format!("88{}", position_id.len()))
    .bind(symbol_key)
    .bind(format!("open_{direction}"))
    .bind(direction)
    .bind(qty)
    .bind(position_id)
    .execute(&mut *tx)
    .await
    .expect("seed filled entry order");
    let intent = crate::proto::OrderIntent {
        bot_uid: bot.into(),
        symbol_key: symbol_key.into(),
        action: format!("open_{direction}"),
        ..Default::default()
    };
    crate::position_close::persist_entry_provenance(
        &mut tx,
        credential,
        &intent,
        position_id,
        &entry_order,
        run_mode,
        1,
    )
    .await
    .expect("production provenance writer");
    // 生产 Entry 在同一事务里经同一个函数占用名义额度（首次即建行）。
    assert!(
        crate::risk::reserve_notional(&mut tx, credential, "5000", "100", 1)
            .await
            .expect("production notional reservation writer")
    );
    tx.commit().await.expect("commit seed");
}

/// 一张已经发到交易所的 Algo 保护单（本地 `submitted`，待生产对账确认）。
#[allow(clippy::too_many_arguments)]
async fn seed_stop(
    store: &Store,
    credential: &str,
    bot: &str,
    position_id: &str,
    symbol_key: &str,
    kind: &str,
    algo_id: i64,
    client_algo_id: &str,
) {
    let engine_order = format!("eo_{client_algo_id}");
    sqlx::query(
        "INSERT INTO protection_orders (protection_order_id, position_id, credential_id, kind, \
         generation, engine_order_id, exchange_order_id, exchange_algo_id, client_algo_id, \
         trigger_price, reduce_only, close_position, status, verified_from_exchange, \
         created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, '90', 1, 1, \
         'submitted', 0, 1, 1)",
    )
    .bind(format!("prot_{client_algo_id}"))
    .bind(position_id)
    .bind(credential)
    .bind(kind)
    .bind(&engine_order)
    .bind(algo_id.to_string())
    .bind(client_algo_id)
    .execute(store.pool())
    .await
    .expect("seed protection order");
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         client_order_id_short, exchange_order_id, exchange, market_kind, symbol_key, action, side, \
         order_type, quantity, reduce_only, status, order_role, position_id, created_at_ms, \
         updated_at_ms) VALUES (?, ?, ?, ?, ?, NULL, 'binance', 'futures', ?, 'close_long', 'long', \
         'STOP_MARKET', '', 1, 'submitted', ?, ?, 1, 1)",
    )
    .bind(&engine_order)
    .bind(credential)
    .bind(bot)
    .bind(format!("idem_{engine_order}"))
    .bind(client_algo_id)
    .bind(symbol_key)
    .bind(if kind == "sl" { "protection_sl" } else { "protection_tp" })
    .bind(position_id)
    .execute(store.pool())
    .await
    .expect("seed protection engine order");
}

/// 用户流 reader 收到的 ORDER_TRADE_UPDATE 帧（交易所冻结字段名），经 reader
/// 落库所用的同一个解析与 sink 应用。
async fn apply_trade_frame(
    store: &Store,
    order_id: i64,
    client_order_id: &str,
    status: &str,
    last_qty: &str,
    cumulative_qty: &str,
    trade_id: i64,
) {
    apply_symbol_trade_frame(
        store,
        "BTCUSDT",
        order_id,
        client_order_id,
        status,
        last_qty,
        cumulative_qty,
        trade_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn apply_symbol_trade_frame(
    store: &Store,
    symbol: &str,
    order_id: i64,
    client_order_id: &str,
    status: &str,
    last_qty: &str,
    cumulative_qty: &str,
    trade_id: i64,
) {
    let now = crate::svc::now_ms();
    let frame = serde_json::json!({
        "e": "ORDER_TRADE_UPDATE", "E": now, "T": now,
        "o": {
            "s": symbol, "c": client_order_id, "i": order_id, "x": "TRADE", "X": status,
            "t": trade_id, "T": now, "l": last_qty, "L": "100", "z": cumulative_qty,
            "n": "0.01", "N": "USDT", "rp": "0"
        }
    })
    .to_string();
    let envelope: crate::binance_user_stream::BinanceWsEnvelope =
        serde_json::from_str(&frame).expect("frozen ORDER_TRADE_UPDATE frame");
    crate::svc::BinanceEventSink::new(store.clone())
        .handle_ws_envelope(CRED, &envelope)
        .await
        .expect("production fill sink");
}

async fn close_order(store: &Store, engine_order_id: &str) -> (String, String, String) {
    let row = sqlx::query(
        "SELECT status, COALESCE(exchange_order_id,'') AS exchange_order_id, \
         client_order_id_short FROM engine_orders WHERE engine_order_id=?",
    )
    .bind(engine_order_id)
    .fetch_one(store.pool())
    .await
    .expect("close order row");
    (
        row.get("status"),
        row.get("exchange_order_id"),
        row.get("client_order_id_short"),
    )
}

async fn position_row(store: &Store, position_id: &str) -> (String, String, i64) {
    let row =
        sqlx::query("SELECT state, qty, close_revision FROM position_state WHERE position_id=?")
            .bind(position_id)
            .fetch_one(store.pool())
            .await
            .expect("position row");
    (row.get("state"), row.get("qty"), row.get("close_revision"))
}

async fn protection_row(store: &Store, client_algo_id: &str) -> (String, i64) {
    let row = sqlx::query(
        "SELECT status, verified_from_exchange FROM protection_orders WHERE client_algo_id=?",
    )
    .bind(client_algo_id)
    .fetch_one(store.pool())
    .await
    .expect("protection row");
    (row.get("status"), row.get("verified_from_exchange"))
}

async fn fence(store: &Store, request_ref: &str) -> bool {
    sqlx::query("SELECT COUNT(*) AS n FROM position_close_scope_locks WHERE pending_request_ref=?")
        .bind(request_ref)
        .fetch_one(store.pool())
        .await
        .expect("scope fence")
        .get::<i64, _>("n")
        == 1
}

/// 生产 OMS 事务里 Entry 取的同一把 scope 锁：交易所凭据的新开仓是否被
/// 未终结的单仓平仓挡住。
async fn exchange_entry_fenced(store: &Store, symbol_key: &str) -> bool {
    let mut tx = store.pool().begin().await.expect("begin probe");
    let fenced = crate::position_close::lock_intent_scope(
        &mut tx,
        store.kind(),
        CRED,
        &crate::proto::OrderIntent {
            symbol_key: symbol_key.into(),
            action: "open_long".into(),
            ..Default::default()
        },
    )
    .await
    .expect("scope probe");
    tx.rollback().await.expect("rollback probe");
    fenced
}

fn protection(receipt: &p::ClosePositionResponse) -> (String, String) {
    let observation = receipt
        .protection_observation
        .as_ref()
        .expect("protection observation");
    (observation.status.clone(), observation.reason_code.clone())
}

fn assert_receipt(receipt: &p::ClosePositionResponse, status: &str, reason: &str, label: &str) {
    assert_eq!(
        (
            receipt.request_status.as_str(),
            receipt.reason_code.as_str()
        ),
        (status, reason),
        "{label}: receipt {receipt:?}"
    );
}

/// 成交全部到达、交易所真值归零之后，平仓才由真值路径终结；终结之前 B3
/// fence 一直挂着、回执不写已成交；保护单在交易所确认归零之后才由 M0 善后
/// 撤销（D29a）。同账户异 symbol / 异方向与异账户同 symbol 的兄弟仓位一字
/// 不动。
async fn full_fill_scenario(h: &Harness) {
    let store = h.store();
    assert_eq!(
        protection_row(store, "wom1slbtc").await,
        ("confirmed".into(), 1)
    );
    let eth_before = position_row(store, POS_ETH).await;
    let other_before = position_row(store, POS_OTHER).await;

    let target = h.engine.target(CRED, POS).await;
    assert!(target.available, "{}", target.reason_code);
    let t = target.target.clone().expect("target");
    assert_eq!(
        (
            t.run_mode.as_str(),
            t.execution_venue.as_str(),
            t.quantity.as_str()
        ),
        ("demo", "testnet", "1")
    );
    let request = h.engine.close_request(CRED, POS).await;
    let first = h.engine.close(&request).await;
    assert_receipt(&first, "working", "EXCHANGE_WORKING", "after submit");
    assert_eq!(first.target_quantity.as_deref(), Some("1"));
    assert_eq!(first.filled_quantity.as_deref(), Some("0"));
    assert!(first.engine_order_id.is_some() && first.risk_verdict_id.is_some());
    assert_eq!(
        protection(&first),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    let order_id = first.engine_order_id.clone().unwrap();
    let (status, exchange_id, client_id) = close_order(store, &order_id).await;
    assert_eq!(status, "acked");
    assert!(client_id.starts_with("b3"), "client order id {client_id}");

    // 交易所收到的正是一张全量、只减仓的市价单，且只有一张。
    let posts = h.order_posts();
    assert_eq!(posts.len(), 1, "exactly one close order: {posts:?}");
    let post = &posts[0];
    assert_eq!(post.param("symbol"), Some("BTCUSDT"));
    assert_eq!(post.param("side"), Some("SELL"));
    assert_eq!(post.param("type"), Some("MARKET"));
    assert_eq!(post.param("quantity"), Some("1"));
    assert_eq!(post.param("reduceOnly"), Some("true"));
    assert_eq!(post.param("newClientOrderId"), Some(client_id.as_str()));
    // D29a：平仓前与平仓同时都不撤保护单。
    assert!(
        h.deletes().is_empty(),
        "no protection cancel before the close fills"
    );
    assert!(
        fence(store, &request.request_ref).await,
        "fence held while working"
    );
    assert!(exchange_entry_fenced(store, BTC).await);
    assert!(
        !exchange_entry_fenced(store, ETH).await,
        "other scope is not fenced"
    );

    // 成交到达：本地仓位因成交归零，但交易所仓位真值尚未被读到——不终结。
    apply_trade_frame(
        store,
        exchange_id.parse().expect("exchange order id"),
        &client_id,
        "FILLED",
        "1",
        "1",
        51_001,
    )
    .await;
    let filled_unconfirmed = h.engine.receipt(&request).await;
    assert_receipt(
        &filled_unconfirmed,
        "working",
        "EXCHANGE_FILL_CONFIRMING",
        "fills recorded, venue truth not read",
    );
    assert_eq!(filled_unconfirmed.filled_quantity.as_deref(), Some("1"));
    assert_eq!(
        filled_unconfirmed
            .execution_outcome
            .as_ref()
            .map(|o| o.status.as_str()),
        Some("unknown"),
        "no frozen outcome before truth finalization"
    );
    assert!(
        fence(store, &request.request_ref).await,
        "fence held until truth"
    );

    // M0 善后：同轮读到交易所该 scope 归零之后才撤保护单（Algo 先）。
    let before_cleanup = h.venue.requests().len();
    crate::protection_cleanup::drive_due_obligations(store)
        .await
        .expect("M0 cleanup round");
    let cleanup_requests = h.venue.requests()[before_cleanup..].to_vec();
    let first_delete = cleanup_requests
        .iter()
        .position(|request| request.method == "DELETE")
        .expect("M0 cancels the closed position's protection");
    assert!(
        cleanup_requests[..first_delete]
            .iter()
            .any(|request| request.is("GET", "/fapi/v2/positionRisk")),
        "venue flatness is read before any cancel: {cleanup_requests:?}"
    );
    assert_eq!(h.algo_status(SL_ALGO), "CANCELED");
    assert_eq!(h.algo_status(TP_ALGO), "CANCELED");
    assert_eq!(
        h.algo_status(ETH_SL_ALGO),
        "NEW",
        "sibling protection untouched"
    );

    // 交易所真值路径：ReconcileNow 读到归零 + 累计成交=目标 ⇒ 终结。
    h.engine.reconcile_now(CRED).await;
    let done = h.engine.receipt(&request).await;
    assert_receipt(&done, "filled", "EXCHANGE_FILLED", "after truth");
    let outcome = done.execution_outcome.clone().expect("frozen outcome");
    assert_eq!(
        (
            outcome.status.as_str(),
            outcome.source.as_str(),
            outcome.state.as_deref(),
            outcome.quantity.as_deref()
        ),
        ("observed", "exchange_truth", Some("closed"), Some("0"))
    );
    assert!(outcome.revision > request.expected_revision);
    assert_eq!(done.filled_quantity.as_deref(), Some("1"));
    assert_eq!(
        protection(&done),
        ("cleaned".into(), "PROTECTION_CLEANUP_CONFIRMED".into())
    );
    assert!(
        !fence(store, &request.request_ref).await,
        "fence released at terminal"
    );
    assert!(!exchange_entry_fenced(store, BTC).await);

    // 兄弟仓位：状态 / 数量与保护单不变；异账户行逐字节不变且零交易所流量。
    let eth_after = position_row(store, POS_ETH).await;
    assert_eq!((&eth_after.0, &eth_after.1), (&eth_before.0, &eth_before.1));
    assert_eq!(
        protection_row(store, "wom1sleth").await,
        ("confirmed".into(), 1)
    );
    assert_eq!(position_row(store, POS_OTHER).await, other_before);
    assert_eq!(
        protection_row(store, "wom1slother").await,
        ("submitted".into(), 0)
    );
    assert!(
        h.venue
            .requests()
            .iter()
            .all(|request| request.api_key != format!("{OTHER_CRED}-api-key")),
        "the other account is never sent anything"
    );
    assert_eq!(h.venue_amount("ETHUSDT"), "-2");

    // 同一请求重放只返回原回执，不再下第二张单。
    let replay = h.engine.close(&request).await;
    assert_receipt(&replay, "filled", "EXCHANGE_FILLED", "replay");
    assert_eq!(h.order_posts().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_full_fill_finalizes_from_truth_and_m0_cleans_after_venue_flat() {
    let h = Harness::sqlite(vec![OrderBehavior::FillNow], None).await;
    full_fill_scenario(&h).await;
    h.cleanup().await;
}

/// M1 评审 P2 #1：终结判据里「同轮 positionRisk 读数晚于原单落库」这一合取项。
/// 其余合取项全部成立（原单 filled、累计成交 = 目标、本地仓位 closed/0、
/// 交易所该 scope 精确归零且读数覆盖该 symbol），只有读数时刻早于原单
/// `created_at_ms`：这样的读数证明不了「平仓之后」归零，绝不终结、fence
/// 继续挂着。引擎时间观经生产 `Clock` 缝领先墙钟，原单落库时刻因此落在
/// 未来；对账走真实 ReconcileNow（读数时刻按墙钟）。读数一旦晚于原单，
/// 同一份真值立即终结——正向对照证明拦住它的正是这一项。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_never_finalizes_from_a_position_read_older_than_the_close_order() {
    const ENGINE_AHEAD_MS: i64 = 8_000;
    let h = Harness::sqlite_with_engine_ahead(vec![OrderBehavior::FillNow], ENGINE_AHEAD_MS).await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let first = h.engine.close(&request).await;
    assert_receipt(&first, "working", "EXCHANGE_WORKING", "after submit");
    let order_id = first.engine_order_id.clone().unwrap();
    let (_, exchange_id, client_id) = close_order(store, &order_id).await;
    let order_created_at_ms: i64 =
        sqlx::query("SELECT created_at_ms FROM engine_orders WHERE engine_order_id=?")
            .bind(&order_id)
            .fetch_one(store.pool())
            .await
            .expect("close order creation time")
            .get("created_at_ms");
    apply_trade_frame(
        store,
        exchange_id.parse().expect("exchange order id"),
        &client_id,
        "FILLED",
        "1",
        "1",
        56_001,
    )
    .await;
    // 除读数时刻外的每一项都已成立。
    assert_eq!(close_order(store, &order_id).await.0, "filled");
    let local = position_row(store, POS).await;
    assert_eq!((local.0.as_str(), local.1.as_str()), ("closed", "0"));
    assert_eq!(h.venue_amount("BTCUSDT"), "0");

    h.engine.reconcile_now(CRED).await;
    let read_bound_ms = crate::svc::now_ms();
    assert!(
        read_bound_ms < order_created_at_ms,
        "precondition: the whole ReconcileNow (its positionRisk read included) must predate \
         the close order's creation ({read_bound_ms} >= {order_created_at_ms})"
    );
    let stale = h.engine.receipt(&request).await;
    assert_receipt(
        &stale,
        "working",
        "EXCHANGE_FILL_CONFIRMING",
        "flat truth read before the close order existed",
    );
    assert_eq!(
        stale.execution_outcome.as_ref().map(|o| o.status.as_str()),
        Some("unknown")
    );
    assert!(
        fence(store, &request.request_ref).await,
        "a pre-order read never releases the fence"
    );
    assert!(exchange_entry_fenced(store, BTC).await);

    // 正向对照：读数晚于原单落库之后，同一份真值终结。
    while crate::svc::now_ms() <= order_created_at_ms + 50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    h.engine.reconcile_now(CRED).await;
    let done = h.engine.receipt(&request).await;
    assert_receipt(&done, "filled", "EXCHANGE_FILLED", "read after the order");
    assert!(!fence(store, &request.request_ref).await);
    assert_eq!(h.order_posts().len(), 1);
    h.cleanup().await;
}

/// M1 评审 P2 #2：回执的「止损仍在」只认**经交易所真值确认**的止损
/// （`verified_from_exchange=1 ∧ status='confirmed'`）。`status` 与
/// `verified_from_exchange` 是两列独立投影；当前生产写入方总是成对写下
/// (confirmed, 1)，但库里任何一行「状态写着 confirmed、却没有交易所核验」
/// （核验前的存量行、未来的写入方）都不得被读成止损仍在。同一行上翻转
/// 核验位：未核验 ⇒ STOP_LOSS_NOT_CONFIRMED；恢复核验 ⇒ STOP_LOSS_ON_VENUE。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_receipt_reports_stop_loss_on_venue_only_when_venue_verified() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let working = h.engine.close(&request).await;
    assert_receipt(&working, "working", "EXCHANGE_WORKING", "resting");
    assert_eq!(
        protection(&working),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );

    let set_verified = |verified: i64| {
        sqlx::query("UPDATE protection_orders SET verified_from_exchange=? WHERE client_algo_id='wom1slbtc'")
            .bind(verified)
            .execute(store.pool())
    };
    set_verified(0).await.expect("revoke venue verification");
    assert_eq!(
        protection_row(store, "wom1slbtc").await,
        ("confirmed".into(), 0),
        "confirmed status without venue verification"
    );
    let unverified = h.engine.receipt(&request).await;
    assert_receipt(&unverified, "working", "EXCHANGE_WORKING", "unverified SL");
    assert_eq!(
        protection(&unverified),
        ("unknown".into(), "STOP_LOSS_NOT_CONFIRMED".into())
    );

    set_verified(1).await.expect("restore venue verification");
    let verified = h.engine.receipt(&request).await;
    assert_eq!(
        protection(&verified),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    h.cleanup().await;
}

/// 部分成交：回执如实为 partially_filled（成交量 / 目标量），保护单仍在、
/// M0 不撤；余量成交后由交易所真值终结。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_partial_fill_then_fill_keeps_protection_until_truth() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let working = h.engine.close(&request).await;
    assert_receipt(&working, "working", "EXCHANGE_WORKING", "resting");
    let order_id = working.engine_order_id.clone().unwrap();
    let (_, exchange_id, client_id) = close_order(store, &order_id).await;
    let exchange_id: i64 = exchange_id.parse().expect("acked exchange id");

    h.venue.with_book(|book| book.fill_order(&client_id, "0.4"));
    apply_trade_frame(
        store,
        exchange_id,
        &client_id,
        "PARTIALLY_FILLED",
        "0.4",
        "0.4",
        52_001,
    )
    .await;
    let partial = h.engine.receipt(&request).await;
    assert_receipt(
        &partial,
        "partially_filled",
        "EXCHANGE_PARTIALLY_FILLED",
        "partial",
    );
    assert_eq!(
        (
            partial.filled_quantity.as_deref(),
            partial.target_quantity.as_deref()
        ),
        (Some("0.4"), Some("1"))
    );
    assert_eq!(
        protection(&partial),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    // 仓位仍开在交易所上：M0 没有义务可驱动，一张保护单都不撤。
    let driven = crate::protection_cleanup::drive_due_obligations(store)
        .await
        .expect("M0 round");
    assert_eq!(driven, 0);
    h.engine.reconcile_now(CRED).await;
    let still_partial = h.engine.receipt(&request).await;
    assert_receipt(
        &still_partial,
        "partially_filled",
        "EXCHANGE_PARTIALLY_FILLED",
        "reconciled",
    );
    assert_eq!(
        position_row(store, POS).await.1,
        "0.6",
        "venue residual applied"
    );
    assert!(
        h.deletes().is_empty(),
        "protection stays while the venue is open"
    );
    assert!(fence(store, &request.request_ref).await);
    assert_eq!(h.algo_status(SL_ALGO), "NEW");

    h.venue.with_book(|book| book.fill_order(&client_id, "0.6"));
    apply_trade_frame(store, exchange_id, &client_id, "FILLED", "0.6", "1", 52_002).await;
    h.engine.reconcile_now(CRED).await;
    let done = h.engine.receipt(&request).await;
    assert_receipt(&done, "filled", "EXCHANGE_FILLED", "after residual");
    assert_eq!(done.filled_quantity.as_deref(), Some("1"));
    assert!(!fence(store, &request.request_ref).await);
    crate::protection_cleanup::drive_due_obligations(store)
        .await
        .expect("M0 round after flat");
    assert_eq!(h.algo_status(SL_ALGO), "CANCELED");
    assert_eq!(h.order_posts().len(), 1);
    h.cleanup().await;
}

/// 拒单：提交结果按仓内保守口径记 unknown；裁决宽限内不终结、fence 挂着、
/// 不能再发新请求；交易所查无此单后终结为 rejected——仓位仍在、交易所侧
/// 止损仍在（服务端如实说明），零撤单。
async fn reject_scenario(h: &Harness) {
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let first = h.engine.close(&request).await;
    assert_receipt(&first, "unknown", "EXCHANGE_OUTCOME_UNKNOWN", "venue 400");
    assert_eq!(
        protection(&first),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    assert_eq!(first.filled_quantity.as_deref(), Some("0"));
    let (_, _, client_id) = close_order(store, first.engine_order_id.as_ref().unwrap()).await;

    // 宽限期内：对账只能重查原请求，不终结；该仓也不能再发起新平仓。
    h.engine.reconcile_now(CRED).await;
    let within_grace = h.engine.receipt(&request).await;
    assert_receipt(
        &within_grace,
        "unknown",
        "EXCHANGE_OUTCOME_UNKNOWN",
        "within grace",
    );
    assert!(fence(store, &request.request_ref).await);
    let retry_target = h.engine.target(CRED, POS).await;
    assert!(
        !retry_target.available,
        "an unknown close never admits a second request: {}",
        retry_target.reason_code
    );
    assert_eq!(h.order_posts().len(), 1, "unknown never re-submits");

    // 跨过裁决宽限：交易所对原 clientOrderId 答「查无此单」⇒ rejected。
    crate::reconcile::reconcile_full_at_for_tests(
        CRED,
        store.clone(),
        crate::svc::now_ms() + crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS + 1_000,
    )
    .await
    .expect("reconcile past the adjudication grace");
    let requery = h
        .venue
        .requests()
        .into_iter()
        .filter(|r| r.is("GET", "/fapi/v1/order"))
        .collect::<Vec<_>>();
    assert!(!requery.is_empty(), "unknown is re-queried");
    assert!(
        requery
            .iter()
            .all(|r| r.param("origClientOrderId") == Some(client_id.as_str())
                && r.param("orderId").is_none()),
        "re-query only by the original client order id: {requery:?}"
    );
    let rejected = h.engine.receipt(&request).await;
    assert_receipt(
        &rejected,
        "rejected",
        "EXCHANGE_ORDER_NOT_FOUND",
        "adjudicated",
    );
    assert_eq!(
        protection(&rejected),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    let current = rejected.position_observation.clone().expect("current");
    assert_eq!(
        (current.state.as_deref(), current.quantity.as_deref()),
        (Some("protected"), Some("1")),
        "the position is still open"
    );
    assert_eq!(h.venue_amount("BTCUSDT"), "1");
    assert_eq!(h.algo_status(SL_ALGO), "NEW");
    assert_eq!(
        protection_row(store, "wom1slbtc").await,
        ("confirmed".into(), 1)
    );
    assert!(h.deletes().is_empty(), "a failed close cancels nothing");
    assert!(
        !fence(store, &request.request_ref).await,
        "terminal releases the fence"
    );
    let again = h.engine.target(CRED, POS).await;
    assert!(again.available, "{}", again.reason_code);
    assert!(again.target.unwrap().revision > request.expected_revision);
    assert_eq!(h.order_posts().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_reject_keeps_position_and_protection() {
    let h = Harness::sqlite(
        vec![OrderBehavior::Reject {
            code: -2022,
            msg: "ReduceOnly Order is rejected.".into(),
        }],
        None,
    )
    .await;
    reject_scenario(&h).await;
    h.cleanup().await;
}

/// 超时：交易所已成交但应答丢在客户端超时之后 ⇒ unknown；只按原
/// clientOrderId 重查、绝不重发；之后的读取由真值收敛到 filled。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_timeout_is_unknown_then_resolves_without_resubmit() {
    let h = Harness::sqlite(
        vec![OrderBehavior::FillButHoldResponse { hold_ms: 2_500 }],
        Some(400),
    )
    .await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let first = h.engine.close(&request).await;
    assert_receipt(&first, "unknown", "EXCHANGE_OUTCOME_UNKNOWN", "timed out");
    let order_id = first.engine_order_id.clone().unwrap();
    let (_, _, client_id) = close_order(store, &order_id).await;
    // 同一请求重放：只读原回执。
    let replay = h.engine.close(&request).await;
    assert_eq!(replay.engine_order_id.as_deref(), Some(order_id.as_str()));
    assert_eq!(h.order_posts().len(), 1);

    h.engine.reconcile_now(CRED).await;
    let resolved = h.engine.receipt(&request).await;
    assert_receipt(
        &resolved,
        "working",
        "EXCHANGE_FILL_CONFIRMING",
        "requeried",
    );
    let (status, exchange_id, _) = close_order(store, &order_id).await;
    assert_eq!(status, "filled");
    assert!(h
        .venue
        .requests()
        .iter()
        .filter(|r| r.is("GET", "/fapi/v1/order"))
        .all(|r| r.param("origClientOrderId") == Some(client_id.as_str())));
    apply_trade_frame(
        store,
        exchange_id.parse().expect("exchange id from the re-query"),
        &client_id,
        "FILLED",
        "1",
        "1",
        53_001,
    )
    .await;
    h.engine.reconcile_now(CRED).await;
    let done = h.engine.receipt(&request).await;
    assert_receipt(&done, "filled", "EXCHANGE_FILLED", "after truth");
    assert_eq!(h.order_posts().len(), 1, "never re-submitted");
    h.cleanup().await;
}

/// 过期 revision 的请求在任何交易所写入之前被拒（revision 绑定）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_with_a_stale_revision_never_reaches_the_venue() {
    let h = Harness::sqlite(vec![OrderBehavior::FillNow], None).await;
    let mut request = h.engine.close_request(CRED, POS).await;
    // 一次真实对账把仓位真值再应用一次（revision 前移）。
    h.engine.reconcile_now(CRED).await;
    let current = h.engine.target(CRED, POS).await.target.unwrap().revision;
    assert!(current > request.expected_revision);
    request.request_ref = ulid::Ulid::new().to_string();
    let stale = h.engine.close(&request).await;
    assert_receipt(&stale, "rejected", "POSITION_REVISION_MISMATCH", "stale");
    assert!(
        h.order_posts().is_empty(),
        "no venue write for a stale target"
    );
    assert_eq!(position_row(h.store(), POS).await.0, "protected");
    h.cleanup().await;
}

/// 精确目标：执行来源必须由生产写入方在**当前** venue 配置下写下；同
/// scope 有别的活仓时不提供可平目标（交易所真值无法证明「这一个」归零）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_target_is_bound_to_venue_provenance_and_a_private_scope() {
    let h = Harness::sqlite(vec![], None).await;
    let store = h.store();
    let available = h.engine.target(CRED, POS).await;
    assert!(available.available, "{}", available.reason_code);

    let venue_env = crate::venue::venue_env_name(CRED);
    std::env::set_var(&venue_env, "mainnet");
    let switched = h.engine.target(CRED, POS).await;
    std::env::set_var(&venue_env, "testnet");
    assert_eq!(switched.reason_code, "POSITION_PROVENANCE_UNSUPPORTED");
    assert!(switched.target.is_none());

    seed_exchange_position(
        store,
        CRED,
        "bot_wom1_twin",
        "pos_wom1_twin",
        BTC,
        "long",
        "1",
    )
    .await;
    assert_eq!(
        h.engine.target(CRED, POS).await.reason_code,
        "POSITION_SCOPE_SHARED"
    );
    sqlx::query(
        "UPDATE position_state SET state='closed', qty='0' WHERE position_id='pos_wom1_twin'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(h.engine.target(CRED, POS).await.available);

    sqlx::query("DELETE FROM position_execution_provenance WHERE position_id=?")
        .bind(POS)
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(
        h.engine.target(CRED, POS).await.reason_code,
        "POSITION_PROVENANCE_UNKNOWN"
    );
    assert!(h.order_posts().is_empty());
    h.cleanup().await;
}

/// 重启：真实子进程以新 boot 打开同一个库、起同一套生产服务，回执继续读到
/// 原请求（不是 pending_unknown），之后成交与真值在新进程里把它终结。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_survives_an_engine_restart_and_finalizes_in_the_new_boot() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let request = h.engine.close_request(CRED, POS).await;
    let working = h.engine.close(&request).await;
    assert_receipt(&working, "working", "EXCHANGE_WORKING", "before restart");
    let order_id = working.engine_order_id.clone().unwrap();
    let (_, exchange_id, client_id) = close_order(h.store(), &order_id).await;
    let old_boot = h.store().boot_id().to_string();
    let Harness {
        venue,
        _env,
        engine,
        database,
    } = h;
    let store = engine.stop().await;
    store.pool().close().await;
    let Database::Sqlite(path) = database else {
        unreachable!("sqlite harness")
    };
    // 引擎停着的时候交易所把单子成交完。
    venue.with_book(|book| book.fill_order(&client_id, "1"));
    let output = path.with_extension("restart.json");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "position_close_exchange_tests::exchange_close_restart_child",
            "--ignored",
            "--nocapture",
        ])
        .env("WO_M1_DATABASE", &path)
        .env(
            "WO_M1_REQUEST",
            serde_json::json!({
                "position": request.position_id, "revision": request.expected_revision.to_string(),
                "boot": request.expected_boot_id, "ref": request.request_ref,
                "actor": request.actor_member_id, "exchangeId": exchange_id, "clientId": client_id,
            })
            .to_string(),
        )
        .env("WO_M1_OUTPUT", &output)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            panic!("restart child timed out");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(status.success(), "restart child failed: {status}");
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let _ = std::fs::remove_file(&output);
    assert_ne!(
        result["bootId"].as_str().unwrap(),
        old_boot,
        "a real new boot"
    );
    assert_eq!(result["before"]["status"], "working");
    assert_eq!(result["before"]["reason"], "EXCHANGE_WORKING");
    assert_eq!(result["after"]["status"], "filled");
    assert_eq!(result["after"]["reason"], "EXCHANGE_FILLED");
    assert_eq!(
        venue
            .requests()
            .iter()
            .filter(|r| r.is("POST", "/fapi/v1/order"))
            .count(),
        1,
        "the restart never re-submits"
    );
    drop(_env);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "subprocess only, invoked by the restart scenario"]
async fn exchange_close_restart_child() {
    sqlx::any::install_default_drivers();
    let database = std::env::var("WO_M1_DATABASE").unwrap();
    let input: serde_json::Value =
        serde_json::from_str(&std::env::var("WO_M1_REQUEST").unwrap()).unwrap();
    let output = std::env::var("WO_M1_OUTPUT").unwrap();
    let store = Store::connect(&format!("sqlite://{database}"))
        .await
        .unwrap();
    let engine = Engine::start(store).await;
    let request = p::ClosePositionRequest {
        credential_id: CRED.into(),
        position_id: input["position"].as_str().unwrap().into(),
        expected_revision: input["revision"].as_str().unwrap().parse().unwrap(),
        expected_boot_id: input["boot"].as_str().unwrap().into(),
        request_ref: input["ref"].as_str().unwrap().into(),
        actor_member_id: input["actor"].as_u64().unwrap(),
        schema_version: 1,
    };
    let before = engine.receipt(&request).await;
    apply_trade_frame(
        &engine.store,
        input["exchangeId"].as_str().unwrap().parse().unwrap(),
        input["clientId"].as_str().unwrap(),
        "FILLED",
        "1",
        "1",
        54_001,
    )
    .await;
    engine.reconcile_now(CRED).await;
    let after = engine.receipt(&request).await;
    std::fs::write(
        output,
        serde_json::to_vec(&serde_json::json!({
            "bootId": engine.store.boot_id(),
            "before": {"status": before.request_status, "reason": before.reason_code},
            "after": {"status": after.request_status, "reason": after.reason_code},
        }))
        .unwrap(),
    )
    .unwrap();
    let store = engine.stop().await;
    store.pool().close().await;
}

// ---------------------------------------------------------------------------
// WO-M1b（M1 评审 P2 #3）· prepare 已提交、派发事务未提交时进程死亡
// ---------------------------------------------------------------------------

/// 崩溃窗口留下的那条请求：由已死进程的 boot 写下的 prepared。
struct StrandedClose {
    request: p::ClosePositionRequest,
    child_boot: String,
    client_order_id: String,
}

async fn wait_child(mut child: std::process::Child, label: &str) {
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            panic!("{label} timed out");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(status.success(), "{label} failed: {status}");
}

/// 真实子进程以自己的 boot 打开同一个库、经真实 gRPC 读目标，再跑生产
/// ClosePosition 在派发之前提交的同一个 T1（`position_close::prepare`），随即
/// 进程退出——派发事务从未开始。父进程（另一个 boot）随后以同一个库重启
/// 引擎：这就是「prepare 已提交、派发未提交」的崩溃窗口。
async fn strand_prepared_close(h: Harness, position: &str) -> (Harness, StrandedClose) {
    let Harness {
        venue,
        _env,
        engine,
        database,
    } = h;
    let store = engine.stop().await;
    store.pool().close().await;
    let Database::Sqlite(path) = database else {
        unreachable!("sqlite harness")
    };
    let output = path.with_extension("stranded.json");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "position_close_exchange_tests::exchange_close_stranded_prepare_child",
            "--ignored",
            "--nocapture",
        ])
        .env("WO_M1B_DATABASE", &path)
        .env("WO_M1B_POSITION", position)
        .env("WO_M1B_OUTPUT", &output)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    wait_child(child, "stranded-prepare child").await;
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let _ = std::fs::remove_file(&output);
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("reopen the same database in the new boot");
    let engine = Engine::start(store).await;
    let request = p::ClosePositionRequest {
        credential_id: CRED.into(),
        position_id: result["position"].as_str().unwrap().into(),
        expected_revision: result["revision"].as_str().unwrap().parse().unwrap(),
        expected_boot_id: result["bootId"].as_str().unwrap().into(),
        request_ref: result["ref"].as_str().unwrap().into(),
        actor_member_id: result["actor"].as_u64().unwrap(),
        schema_version: 1,
    };
    let stranded = StrandedClose {
        child_boot: request.expected_boot_id.clone(),
        client_order_id: result["clientOrderId"].as_str().unwrap().into(),
        request,
    };
    (
        Harness {
            venue,
            _env,
            engine,
            database: Database::Sqlite(path),
        },
        stranded,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "subprocess only, invoked by the stranded-prepare scenarios"]
async fn exchange_close_stranded_prepare_child() {
    sqlx::any::install_default_drivers();
    let database = std::env::var("WO_M1B_DATABASE").unwrap();
    let position = std::env::var("WO_M1B_POSITION").unwrap();
    let output = std::env::var("WO_M1B_OUTPUT").unwrap();
    let store = Store::connect(&format!("sqlite://{database}"))
        .await
        .unwrap();
    let engine = Engine::start(store).await;
    let request = engine.close_request(CRED, &position).await;
    assert_eq!(request.expected_boot_id, engine.store.boot_id());
    let crate::position_close::Prepare::Dispatch(context) =
        crate::position_close::prepare(&engine.store, &request)
            .await
            .expect("production prepare")
    else {
        panic!("prepare must commit a dispatchable request");
    };
    let client_order_id: String = sqlx::query(
        "SELECT client_order_id_short FROM position_close_requests WHERE request_ref=?",
    )
    .bind(&context.request_ref)
    .fetch_one(engine.store.pool())
    .await
    .unwrap()
    .get("client_order_id_short");
    std::fs::write(
        output,
        serde_json::to_vec(&serde_json::json!({
            "bootId": engine.store.boot_id(), "position": request.position_id,
            "revision": request.expected_revision.to_string(), "ref": request.request_ref,
            "actor": request.actor_member_id, "clientOrderId": client_order_id,
        }))
        .unwrap(),
    )
    .unwrap();
    // 进程就此死去：不停服务、不派发、不做任何收尾。
    std::process::exit(0);
}

async fn request_state(store: &Store, request_ref: &str) -> (String, String, String, bool) {
    let row = sqlx::query(
        "SELECT phase, reason_code, expected_boot_id, engine_order_id IS NULL AS no_order \
         FROM position_close_requests WHERE request_ref=?",
    )
    .bind(request_ref)
    .fetch_one(store.pool())
    .await
    .expect("close request row");
    (
        row.get("phase"),
        row.get("reason_code"),
        row.get("expected_boot_id"),
        row.get::<i64, _>("no_order") == 1,
    )
}

async fn orders_with_client_id(store: &Store, client_order_id: &str) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM engine_orders WHERE client_order_id_short=?")
        .bind(client_order_id)
        .fetch_one(store.pool())
        .await
        .expect("order rows by client order id")
        .get("n")
}

/// 崩溃窗口留下的现场：前一 boot 的 prepared、无订单行、fence 挂着、零出站。
async fn assert_stranded(h: &Harness, stranded: &StrandedClose) {
    let store = h.store();
    assert_ne!(stranded.child_boot, store.boot_id(), "a real other boot");
    let (phase, _, boot, no_order) = request_state(store, &stranded.request.request_ref).await;
    assert_eq!(
        (phase.as_str(), boot.as_str(), no_order),
        ("prepared", stranded.child_boot.as_str(), true)
    );
    assert!(stranded.client_order_id.starts_with("b3"));
    assert_eq!(
        orders_with_client_id(store, &stranded.client_order_id).await,
        0
    );
    assert!(fence(store, &stranded.request.request_ref).await);
    assert!(exchange_entry_fenced(store, BTC).await);
    assert!(h.order_posts().is_empty());
}

/// 终结之后：如实 rejected（未下达），fence 释放，仓位与交易所侧止损原样，
/// 同一请求重放只读这张终态回执、绝不重新派发；会员可以重新发起平仓。
async fn assert_resolved_not_dispatched(h: &Harness, stranded: &StrandedClose) {
    let store = h.store();
    let receipt = h.engine.receipt(&stranded.request).await;
    assert_receipt(
        &receipt,
        "rejected",
        "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED",
        "resolved stranded prepare",
    );
    assert!(!fence(store, &stranded.request.request_ref).await);
    assert!(!exchange_entry_fenced(store, BTC).await);
    assert!(receipt.engine_order_id.is_none() && receipt.risk_verdict_id.is_none());
    assert_eq!(
        (
            receipt.filled_quantity.as_deref(),
            receipt.target_quantity.as_deref()
        ),
        (None, Some("1"))
    );
    assert_eq!(
        receipt
            .execution_outcome
            .as_ref()
            .map(|o| o.status.as_str()),
        Some("unknown")
    );
    let current = receipt.position_observation.clone().expect("current");
    assert_eq!(
        (current.state.as_deref(), current.quantity.as_deref()),
        (Some("protected"), Some("1"))
    );
    assert_eq!(
        protection(&receipt),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    let replay = h.engine.close(&stranded.request).await;
    assert_receipt(
        &replay,
        "rejected",
        "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED",
        "replay of the resolved request",
    );
    assert!(h.order_posts().is_empty(), "never re-dispatched");
    assert_eq!(
        orders_with_client_id(store, &stranded.client_order_id).await,
        0
    );
    assert!(h.deletes().is_empty());

    let again = h.engine.close_request(CRED, POS).await;
    let working = h.engine.close(&again).await;
    assert_receipt(&working, "working", "EXCHANGE_WORKING", "fresh close");
    assert_eq!(
        h.order_posts().len(),
        1,
        "exactly the fresh close reached the venue"
    );
}

/// 新 boot 的对账轮（真实 ReconcileNow）证明未发出后终结并释放 fence——不需要
/// 任何会员动作，该 scope 的开仓随之解封。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_stranded_before_dispatch_is_rejected_by_reconcile_and_releases_the_fence() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let (h, stranded) = strand_prepared_close(h, POS).await;
    assert_stranded(&h, &stranded).await;
    h.engine.reconcile_now(CRED).await;
    let (phase, reason, _, no_order) =
        request_state(h.store(), &stranded.request.request_ref).await;
    assert_eq!(
        (phase.as_str(), reason.as_str(), no_order),
        ("rejected", "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED", true),
        "the reconcile round itself resolves the stranded request"
    );
    assert!(
        !fence(h.store(), &stranded.request.request_ref).await,
        "released in the reconcile round, before any receipt read"
    );
    assert!(!exchange_entry_fenced(h.store(), BTC).await);
    assert_resolved_not_dispatched(&h, &stranded).await;
    h.cleanup().await;
}

/// 控制面读回执（真实 GetPositionCloseReceipt）时同样按未发出证明终结：
/// 会员看到的是终态，而不是永远的 pending_unknown。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_stranded_before_dispatch_is_rejected_on_the_receipt_read() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let (h, stranded) = strand_prepared_close(h, POS).await;
    assert_stranded(&h, &stranded).await;
    assert_resolved_not_dispatched(&h, &stranded).await;
    h.cleanup().await;
}

/// 未发出证明不成立就绝不推定：库里已经有一行带着本请求 b3 clientOrderId
/// 的订单（派发事务本身产生不出「有订单行、请求仍 prepared」，这里构造它
/// 正是为了钉住证明本身）⇒ 回执仍是 pending_unknown，对账也不终结，fence
/// 挂着，零出站。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_stranded_prepare_with_an_order_row_is_never_presumed_unsent() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let (h, stranded) = strand_prepared_close(h, POS).await;
    assert_stranded(&h, &stranded).await;
    let store = h.store();
    sqlx::query(
        "INSERT INTO engine_orders (engine_order_id, credential_id, bot_uid, idempotency_key, \
         client_order_id_short, exchange, market_kind, symbol_key, action, side, order_type, \
         quantity, reduce_only, status, order_role, position_id, created_at_ms, updated_at_ms) \
         VALUES ('eo_wom1b_inflight', ?, ?, 'idem_wom1b_inflight', ?, 'binance', 'futures', ?, \
         'close_long', 'long', 'market', '1', 1, 'pending_submit', 'exit', ?, 1, 1)",
    )
    .bind(CRED)
    .bind(BOT)
    .bind(&stranded.client_order_id)
    .bind(BTC)
    .bind(POS)
    .execute(store.pool())
    .await
    .expect("an order row carrying the request's client order id");

    let read = h.engine.receipt(&stranded.request).await;
    assert_receipt(
        &read,
        "pending_unknown",
        "PREPARED_PREVIOUS_BOOT",
        "order row present",
    );
    h.engine.reconcile_now(CRED).await;
    let (phase, _, _, _) = request_state(store, &stranded.request.request_ref).await;
    assert_eq!(phase, "prepared");
    let replay = h.engine.close(&stranded.request).await;
    assert_receipt(
        &replay,
        "pending_unknown",
        "PREPARED_PREVIOUS_BOOT",
        "replay with an order row present",
    );
    assert!(fence(store, &stranded.request.request_ref).await);
    assert!(exchange_entry_fenced(store, BTC).await);
    assert!(h.order_posts().is_empty());
    h.cleanup().await;
}

/// 本 boot 的 prepared（派发可能正在进行）不是崩溃遗留：读回执与对账都
/// 不碰它，fence 继续挂着。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_prepared_in_the_current_boot_is_left_to_its_dispatch() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let crate::position_close::Prepare::Dispatch(_) =
        crate::position_close::prepare(store, &request)
            .await
            .expect("production prepare")
    else {
        panic!("prepare must commit a dispatchable request");
    };
    let read = h.engine.receipt(&request).await;
    assert_receipt(&read, "prepared", "PREPARED", "current-boot prepare");
    h.engine.reconcile_now(CRED).await;
    let (phase, reason, boot, _) = request_state(store, &request.request_ref).await;
    assert_eq!(
        (phase.as_str(), reason.as_str(), boot.as_str()),
        ("prepared", "PREPARED", store.boot_id())
    );
    assert!(fence(store, &request.request_ref).await);
    assert!(h.order_posts().is_empty());
    h.cleanup().await;
}

/// 故障注入构建下的同一崩溃窗口，但崩溃发生在**真实 ClosePosition RPC 内部**：
/// 子进程在生产检查点 `b3_after_prepare`（prepare 已提交、派发尚未开始）
/// 自停，父进程对它发 SIGKILL；新 boot 的读回执按未发出证明终结。
#[cfg(feature = "l4-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_sigkill_after_prepare_is_resolved_as_not_dispatched() {
    let h = Harness::sqlite(vec![OrderBehavior::Rest], None).await;
    let Harness {
        venue,
        _env,
        engine,
        database,
    } = h;
    let store = engine.stop().await;
    store.pool().close().await;
    let Database::Sqlite(path) = database else {
        unreachable!("sqlite harness")
    };
    let output = path.with_extension("sigkill.json");
    let marker_dir = std::env::temp_dir().join(format!("wo-m1b-fault-{}", ulid::Ulid::new()));
    std::fs::create_dir(&marker_dir).unwrap();
    let point = "b3_after_prepare";
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "position_close_exchange_tests::exchange_close_sigkill_child",
            "--ignored",
            "--nocapture",
        ])
        .env("WO_M1B_DATABASE", &path)
        .env("WO_M1B_POSITION", POS)
        .env("WO_M1B_OUTPUT", &output)
        .env("L4_FAULT_INJECTION_ENABLED", "1")
        .env("FAULT_INJECT", point)
        .env("L4_FAULT_MARKER_DIR", &marker_dir)
        .env("L4_FAKE_EXCHANGE_DB", marker_dir.join("unused-exchange.db"))
        .env("L4_FILE_RPC_DIR", marker_dir.join("unused-rpc"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let marker = marker_dir.join(format!("{point}.ready"));
    let started = std::time::Instant::now();
    while !marker.exists()
        && started.elapsed() < Duration::from_secs(30)
        && child.try_wait().unwrap().is_none()
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let reached = marker.exists();
    let marker_text = std::fs::read_to_string(&marker).unwrap_or_default();
    let child_pid = child.id();
    let _ = child.kill();
    let status = child.wait().unwrap();
    assert!(reached, "{point} was not reached; child={status}");
    assert!(marker_text.contains(&format!("pid={child_pid}\n")));
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(9),
        "the child must actually die by SIGKILL"
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let _ = std::fs::remove_file(&output);
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let engine = Engine::start(store).await;
    let request = p::ClosePositionRequest {
        credential_id: CRED.into(),
        position_id: POS.into(),
        expected_revision: result["revision"].as_str().unwrap().parse().unwrap(),
        expected_boot_id: result["bootId"].as_str().unwrap().into(),
        request_ref: result["ref"].as_str().unwrap().into(),
        actor_member_id: result["actor"].as_u64().unwrap(),
        schema_version: 1,
    };
    let client_order_id: String = sqlx::query(
        "SELECT client_order_id_short FROM position_close_requests WHERE request_ref=?",
    )
    .bind(&request.request_ref)
    .fetch_one(engine.store.pool())
    .await
    .expect("the killed RPC committed its prepare")
    .get("client_order_id_short");
    let h = Harness {
        venue,
        _env,
        engine,
        database: Database::Sqlite(path),
    };
    let stranded = StrandedClose {
        child_boot: request.expected_boot_id.clone(),
        client_order_id,
        request,
    };
    assert_stranded(&h, &stranded).await;
    assert_resolved_not_dispatched(&h, &stranded).await;
    h.cleanup().await;
    std::fs::remove_file(marker).unwrap();
    std::fs::remove_dir(marker_dir).unwrap();
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "subprocess only, invoked by the SIGKILL-after-prepare scenario"]
async fn exchange_close_sigkill_child() {
    sqlx::any::install_default_drivers();
    let database = std::env::var("WO_M1B_DATABASE").unwrap();
    let position = std::env::var("WO_M1B_POSITION").unwrap();
    let output = std::env::var("WO_M1B_OUTPUT").unwrap();
    let store = Store::connect(&format!("sqlite://{database}"))
        .await
        .unwrap();
    let engine = Engine::start(store).await;
    let request = engine.close_request(CRED, &position).await;
    std::fs::write(
        output,
        serde_json::to_vec(&serde_json::json!({
            "bootId": request.expected_boot_id, "revision": request.expected_revision.to_string(),
            "ref": request.request_ref, "actor": request.actor_member_id,
        }))
        .unwrap(),
    )
    .unwrap();
    // 生产 ClosePosition：prepare 提交后在 b3_after_prepare 自停，等待 SIGKILL。
    let _ = engine.close(&request).await;
    panic!("selected checkpoint did not stop the child");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_exchange_close_full_fill_finalizes_from_truth() {
    let Some(h) = Harness::mysql(
        "mysql_exchange_close_full_fill_finalizes_from_truth",
        vec![OrderBehavior::FillNow],
    )
    .await
    else {
        return;
    };
    full_fill_scenario(&h).await;
    h.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_exchange_close_reject_keeps_position_and_protection() {
    let Some(h) = Harness::mysql(
        "mysql_exchange_close_reject_keeps_position_and_protection",
        vec![OrderBehavior::Reject {
            code: -2022,
            msg: "ReduceOnly Order is rejected.".into(),
        }],
    )
    .await
    else {
        return;
    };
    reject_scenario(&h).await;
    h.cleanup().await;
}

/// WO-M1b 的真库方言覆盖（scope 锁 FOR UPDATE、未发出证明、同事务终结 +
/// 释放 fence）。崩溃窗口本身由 SQLite 子进程用例真实产生；进程内 boot 是
/// 全进程唯一的，这里在生产 prepare 提交之后把请求的 boot 改写为另一个
/// （已死）boot，只为在 MySQL 上走同一条恢复路径。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_exchange_close_stranded_prepare_is_resolved_by_reconcile() {
    let Some(h) = Harness::mysql(
        "mysql_exchange_close_stranded_prepare_is_resolved_by_reconcile",
        vec![OrderBehavior::Rest],
    )
    .await
    else {
        return;
    };
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let crate::position_close::Prepare::Dispatch(_) =
        crate::position_close::prepare(store, &request)
            .await
            .expect("production prepare")
    else {
        panic!("prepare must commit a dispatchable request");
    };
    let dead_boot = ulid::Ulid::new().to_string();
    sqlx::query("UPDATE position_close_requests SET expected_boot_id=? WHERE request_ref=?")
        .bind(&dead_boot)
        .bind(&request.request_ref)
        .execute(store.pool())
        .await
        .expect("attribute the prepare to a dead boot");
    let client_order_id: String = sqlx::query(
        "SELECT client_order_id_short FROM position_close_requests WHERE request_ref=?",
    )
    .bind(&request.request_ref)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get("client_order_id_short");
    // 请求身份（params 承诺）保持原样：重放仍是同一个请求。
    let stranded = StrandedClose {
        request,
        child_boot: dead_boot,
        client_order_id,
    };
    assert_stranded(&h, &stranded).await;
    h.engine.reconcile_now(CRED).await;
    let (phase, reason, _, _) = request_state(store, &stranded.request.request_ref).await;
    assert_eq!(
        (phase.as_str(), reason.as_str()),
        ("rejected", "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED")
    );
    assert!(!fence(store, &stranded.request.request_ref).await);
    assert_resolved_not_dispatched(&h, &stranded).await;
    h.cleanup().await;
}

fn vector_target(response: &p::GetPositionCloseTargetResponse) -> serde_json::Value {
    serde_json::json!({
        "available": response.available, "reason_code": response.reason_code,
        "observed_at_ms": response.observed_at_ms, "engine_boot_id": response.engine_boot_id,
        "schema_version": response.schema_version,
        "target": response.target.as_ref().map(|t| serde_json::json!({
            "position_id": t.position_id, "revision": t.revision.to_string(), "bot_uid": t.bot_uid,
            "symbol_key": t.symbol_key, "direction": t.direction, "quantity": t.quantity,
            "entry_epoch": t.entry_epoch.to_string(), "exit_epoch": t.exit_epoch.to_string(),
            "run_mode": t.run_mode, "execution_venue": t.execution_venue,
            "entry_engine_order_id": t.entry_engine_order_id, "schema_version": t.schema_version,
        })),
    })
}

fn vector_request(r: &p::ClosePositionRequest) -> serde_json::Value {
    serde_json::json!({
        "credential_id": r.credential_id, "position_id": r.position_id,
        "expected_revision": r.expected_revision.to_string(), "expected_boot_id": r.expected_boot_id,
        "request_ref": r.request_ref, "actor_member_id": r.actor_member_id.to_string(),
        "schema_version": r.schema_version,
    })
}

fn vector_observation(o: &Option<p::PositionCloseObservation>) -> serde_json::Value {
    o.as_ref().map_or(serde_json::Value::Null, |o| {
        serde_json::json!({
            "status": o.status, "position_id": o.position_id, "revision": o.revision.to_string(),
            "quantity": o.quantity, "state": o.state, "observed_at_ms": o.observed_at_ms,
            "source": o.source, "schema_version": o.schema_version,
        })
    })
}

fn vector_receipt(r: &p::ClosePositionResponse) -> serde_json::Value {
    serde_json::json!({
        "found": r.found, "request_ref": r.request_ref, "position_id": r.position_id,
        "target_revision": r.target_revision.to_string(), "request_status": r.request_status,
        "reason_code": r.reason_code, "engine_order_id": r.engine_order_id,
        "risk_verdict_id": r.risk_verdict_id,
        "execution_outcome": vector_observation(&r.execution_outcome),
        "position_observation": vector_observation(&r.position_observation),
        "protection_observation": r.protection_observation.as_ref().map(|p| serde_json::json!({
            "status": p.status, "reason_code": p.reason_code, "schema_version": p.schema_version,
        })),
        "filled_quantity": r.filled_quantity, "target_quantity": r.target_quantity,
        "schema_version": r.schema_version,
    })
}

/// 跨层冻结向量的唯一生产者：每一条都是真实 gRPC 对 TLS 回环交易所跑出来
/// 的原生回执（Python / 控制面 / 页面只消费它，不各写一份同形 DTO）。
/// `WO_M1_EXCHANGE_VECTOR_PATH` 设置时写出，否则只校验流程。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_contract_vector() {
    let h = Harness::sqlite(
        vec![
            OrderBehavior::Rest,
            OrderBehavior::Reject {
                code: -2022,
                msg: "ReduceOnly Order is rejected.".into(),
            },
        ],
        None,
    )
    .await;
    let store = h.store();
    let target = h.engine.target(CRED, POS).await;
    let btc = h.engine.close_request(CRED, POS).await;
    let working = h.engine.close(&btc).await;
    assert_receipt(&working, "working", "EXCHANGE_WORKING", "vector working");
    let (_, exchange_id, client_id) =
        close_order(store, working.engine_order_id.as_ref().unwrap()).await;
    let exchange_id: i64 = exchange_id.parse().unwrap();
    h.venue.with_book(|book| book.fill_order(&client_id, "0.4"));
    apply_trade_frame(
        store,
        exchange_id,
        &client_id,
        "PARTIALLY_FILLED",
        "0.4",
        "0.4",
        55_001,
    )
    .await;
    let partial = h.engine.receipt(&btc).await;
    assert_receipt(
        &partial,
        "partially_filled",
        "EXCHANGE_PARTIALLY_FILLED",
        "vector partial",
    );
    h.venue.with_book(|book| book.fill_order(&client_id, "0.6"));
    apply_trade_frame(store, exchange_id, &client_id, "FILLED", "0.6", "1", 55_002).await;
    let confirming = h.engine.receipt(&btc).await;
    assert_receipt(
        &confirming,
        "working",
        "EXCHANGE_FILL_CONFIRMING",
        "vector confirming",
    );
    h.engine.reconcile_now(CRED).await;
    let filled = h.engine.receipt(&btc).await;
    assert_receipt(&filled, "filled", "EXCHANGE_FILLED", "vector filled");
    crate::protection_cleanup::drive_due_obligations(store)
        .await
        .expect("M0 round");
    let cleaned = h.engine.receipt(&btc).await;
    assert_eq!(
        protection(&cleaned),
        ("cleaned".into(), "PROTECTION_CLEANUP_CONFIRMED".into())
    );

    let eth = h.engine.close_request(CRED, POS_ETH).await;
    let unknown = h.engine.close(&eth).await;
    assert_receipt(
        &unknown,
        "unknown",
        "EXCHANGE_OUTCOME_UNKNOWN",
        "vector unknown",
    );
    crate::reconcile::reconcile_full_at_for_tests(
        CRED,
        store.clone(),
        crate::svc::now_ms() + crate::reconcile::UNCONFIRMED_ORDER_ADJUDICATION_GRACE_MS + 1_000,
    )
    .await
    .expect("reconcile past the adjudication grace");
    let rejected = h.engine.receipt(&eth).await;
    assert_receipt(
        &rejected,
        "rejected",
        "EXCHANGE_ORDER_NOT_FOUND",
        "vector rejected",
    );
    assert_eq!(
        protection(&rejected),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );

    // WO-M1b：真实子进程在 prepare 提交后死亡（派发从未开始），新 boot 读回执
    // 时按未发出证明终结。
    let (h, stranded) = strand_prepared_close(h, POS_ETH).await;
    let not_dispatched = h.engine.receipt(&stranded.request).await;
    assert_receipt(
        &not_dispatched,
        "rejected",
        "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED",
        "vector not dispatched",
    );
    assert_eq!(h.order_posts().len(), 2);

    let vector = serde_json::json!({
        "schema": "wo-m1-exchange-position-close-native-source.1",
        "source": "authenticated production gRPC over a TLS loopback Binance fake: GetPositionCloseTarget/ClosePosition/GetPositionCloseReceipt/ReconcileNow; fills via the production user-stream sink; M0 cleanup via drive_due_obligations; notDispatched: a real child process (its own boot) commits the production prepare and dies before dispatch, the receipt is read in the next boot",
        "requests": {
            "filledClose": vector_request(&btc),
            "rejectedClose": vector_request(&eth),
            "strandedClose": vector_request(&stranded.request),
        },
        "target": vector_target(&target),
        "cases": {
            "working": vector_receipt(&working),
            "partiallyFilled": vector_receipt(&partial),
            "fillConfirming": vector_receipt(&confirming),
            "filled": vector_receipt(&filled),
            "filledCleaned": vector_receipt(&cleaned),
            "unknown": vector_receipt(&unknown),
            "rejected": vector_receipt(&rejected),
            "notDispatched": vector_receipt(&not_dispatched),
        },
    });
    if let Ok(path) = std::env::var("WO_M1_EXCHANGE_VECTOR_PATH") {
        std::fs::write(path, serde_json::to_vec_pretty(&vector).unwrap()).unwrap();
    }
    h.cleanup().await;
}

// ---------------------------------------------------------------------------
// WO-M6 / D39 · 实盘总开关只拦新增风险
// ---------------------------------------------------------------------------

const SOL: &str = "binance|futures|SOL|USDT|USDT";
const POS_LIVE: &str = "pos_wom6_live";
const LIVE_SL_ALGO: i64 = 7401;

/// 一个 live_small 档位的既有仓位（执行来源由生产写入方在本凭据的 venue
/// 配置下写下），交易所侧带一张生产对账确认过的止损。
async fn seed_live_small_position(h: &Harness) {
    let store = h.store();
    seed_exchange_position_in(
        store,
        CRED,
        "bot_wom6_live",
        POS_LIVE,
        SOL,
        "long",
        "3",
        "live_small",
    )
    .await;
    seed_stop(
        store,
        CRED,
        "bot_wom6_live",
        POS_LIVE,
        SOL,
        "sl",
        LIVE_SL_ALGO,
        "wom6slsol",
    )
    .await;
    h.venue.with_book(|book| {
        book.positions.push(venue_position("SOLUSDT", "3"));
        book.open_algos
            .push(algo_order("SOLUSDT", LIVE_SL_ALGO, "wom6slsol", "NEW"));
    });
    h.engine.reconcile_now(CRED).await;
    assert_eq!(
        protection_row(store, "wom6slsol").await,
        ("confirmed".into(), 1)
    );
}

fn assert_live_switch_off() {
    assert!(!crate::svc::is_binance_live_enabled());
}

/// D39（WO-M6）：`ENGINE_LIVE_ENABLED` 关闭时，既有 live_small 仓位的单仓
/// reduce-only 平仓照常受理、下单，并由交易所真值终结——总开关关闭正是最需要
/// 能平仓的时刻（engine safety contract）。此前被档位闸门以 LiveDisabled 拒绝
/// （CLOSE_SUBMISSION_REFUSED）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_small_close_with_live_disabled_is_accepted_and_finalized_from_truth() {
    live_small_close_with_live_disabled_is_accepted_and_finalized_from_truth_for_venue(
        crate::venue::Venue::Testnet,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainnet_live_small_close_with_live_disabled_is_accepted_and_finalized_from_truth() {
    live_small_close_with_live_disabled_is_accepted_and_finalized_from_truth_for_venue(
        crate::venue::Venue::Mainnet,
    )
    .await;
}

async fn live_small_close_with_live_disabled_is_accepted_and_finalized_from_truth_for_venue(
    venue: crate::venue::Venue,
) {
    let h =
        Harness::sqlite_with_venues(&[(CRED, venue), (OTHER_CRED, crate::venue::Venue::Testnet)])
            .await;
    assert_eq!(crate::venue::venue_for_credential(CRED), Ok(Some(venue)));
    assert_live_switch_off();
    seed_live_small_position(&h).await;
    let store = h.store();
    let target = h.engine.target(CRED, POS_LIVE).await;
    assert!(target.available, "{}", target.reason_code);
    assert_eq!(target.target.as_ref().unwrap().run_mode, "live_small");

    let request = h.engine.close_request(CRED, POS_LIVE).await;
    let first = h.engine.close(&request).await;
    assert_receipt(
        &first,
        "working",
        "EXCHANGE_WORKING",
        "live_small close, live off",
    );
    assert_eq!(
        protection(&first),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    let posts = h.order_posts();
    assert_eq!(posts.len(), 1, "exactly one close order: {posts:?}");
    let post = &posts[0];
    assert_eq!(post.param("symbol"), Some("SOLUSDT"));
    assert_eq!(post.param("side"), Some("SELL"));
    assert_eq!(post.param("type"), Some("MARKET"));
    assert_eq!(post.param("quantity"), Some("3"));
    assert_eq!(post.param("reduceOnly"), Some("true"));
    let order_id = first.engine_order_id.clone().unwrap();
    let (_, exchange_id, client_id) = close_order(store, &order_id).await;
    assert_eq!(post.param("newClientOrderId"), Some(client_id.as_str()));

    apply_symbol_trade_frame(
        store,
        "SOLUSDT",
        exchange_id.parse().expect("exchange order id"),
        &client_id,
        "FILLED",
        "3",
        "3",
        57_001,
    )
    .await;
    h.engine.reconcile_now(CRED).await;
    let done = h.engine.receipt(&request).await;
    assert_receipt(&done, "filled", "EXCHANGE_FILLED", "finalized from truth");
    let outcome = done.execution_outcome.clone().expect("frozen outcome");
    assert_eq!(
        (
            outcome.source.as_str(),
            outcome.state.as_deref(),
            outcome.quantity.as_deref()
        ),
        ("exchange_truth", Some("closed"), Some("0"))
    );
    assert_eq!(h.venue_amount("SOLUSDT"), "0");
    assert!(!fence(store, &request.request_ref).await);
    assert_eq!(h.order_posts().len(), 1);
    h.cleanup().await;
}

/// D39：只拦新增风险——live_small 档位的开仓在总开关关闭时仍由 LiveDisabled
/// 拒绝；未配置 venue 的旧式凭据上的平仓也仍被拒（它的只减退出到不了交易所，
/// 紧急 flatten 同样跳过它：同一判据）。两条都经真实 v2 ExecuteIntents，零出站。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_disabled_still_refuses_new_risk_and_exits_that_cannot_reach_the_exchange() {
    live_disabled_still_refuses_new_risk_and_exits_that_cannot_reach_the_exchange_for_venue(
        crate::venue::Venue::Testnet,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainnet_live_disabled_still_refuses_new_risk_and_exits_that_cannot_reach_the_exchange() {
    live_disabled_still_refuses_new_risk_and_exits_that_cannot_reach_the_exchange_for_venue(
        crate::venue::Venue::Mainnet,
    )
    .await;
}

async fn live_disabled_still_refuses_new_risk_and_exits_that_cannot_reach_the_exchange_for_venue(
    venue: crate::venue::Venue,
) {
    let h =
        Harness::sqlite_with_venues(&[(CRED, venue), (OTHER_CRED, crate::venue::Venue::Testnet)])
            .await;
    assert_eq!(crate::venue::venue_for_credential(CRED), Ok(Some(venue)));
    assert_live_switch_off();
    let intent = |action: &str, key: &str| p::OrderIntent {
        bot_uid: "bot_wom6_entry".into(),
        symbol_key: BTC.into(),
        action: action.into(),
        order_type: "market".into(),
        reduce_only: action.starts_with("close_"),
        idempotency_key: key.into(),
        intent_id: format!("int_{key}"),
        client_order_id_short: key.into(),
        exchange: "binance".into(),
        market_kind: "futures".into(),
        schema_version: 1,
        ..Default::default()
    };
    let open = h
        .engine
        .execute_v2(CRED, "live_small", intent("open_long", "wom6open"))
        .await;
    assert_eq!(open.acks.len(), 1);
    assert_eq!(open.acks[0].status, p::Status::Rejected as i32, "{open:?}");
    assert!(
        open.acks[0]
            .message
            .contains("live engine stage disabled by ENGINE_LIVE_ENABLED"),
        "{open:?}"
    );

    let unconfigured = "6301";
    assert_eq!(crate::venue::venue_for_credential(unconfigured), Ok(None));
    let close = h
        .engine
        .execute_v2(
            unconfigured,
            "live_small",
            intent("close_long", "wom6unreach"),
        )
        .await;
    assert_eq!(
        close.acks[0].status,
        p::Status::Rejected as i32,
        "{close:?}"
    );
    assert!(
        close.acks[0]
            .message
            .contains("live engine stage disabled by ENGINE_LIVE_ENABLED"),
        "{close:?}"
    );
    assert!(h.order_posts().is_empty(), "no new risk reached the venue");
    h.cleanup().await;
}

/// D39：紧急 flatten（kill switch L3）在总开关关闭时同样平掉 live_small 仓位。
/// 经真实 SetKillSwitch 锁存，再由生产 scope 驱动（后台重试任务每轮调用的同一个
/// `drive_live_scope_once`，交易所过滤器用固定真值以免触网）跑一轮：只减市价单
/// 只带本凭据的密钥，交易所该 symbol 归零。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emergency_flatten_with_live_disabled_flattens_a_live_small_position() {
    emergency_flatten_with_live_disabled_flattens_a_live_small_position_for_venue(
        crate::venue::Venue::Testnet,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainnet_emergency_flatten_with_live_disabled_flattens_a_live_small_position() {
    emergency_flatten_with_live_disabled_flattens_a_live_small_position_for_venue(
        crate::venue::Venue::Mainnet,
    )
    .await;
}

async fn emergency_flatten_with_live_disabled_flattens_a_live_small_position_for_venue(
    venue: crate::venue::Venue,
) {
    let h =
        Harness::sqlite_with_venues(&[(CRED, venue), (OTHER_CRED, crate::venue::Venue::Testnet)])
            .await;
    assert_eq!(crate::venue::venue_for_credential(CRED), Ok(Some(venue)));
    assert_live_switch_off();
    seed_live_small_position(&h).await;
    let armed = h.engine.set_kill_switch("cred:61", "flatten").await;
    assert!(armed.accepted, "{armed:?}");
    let filters = crate::exchange_filters::static_filter_cache("100");
    let mut rounds = 0;
    while h.venue_amount("SOLUSDT") != "0" && rounds < 3 {
        rounds += 1;
        let _ = crate::kill_switch::drive_live_scope_once_for_tests(
            h.store(),
            "cred:61",
            crate::kill_switch::Level::Flatten,
            &filters,
        )
        .await;
    }
    assert_eq!(
        h.venue_amount("SOLUSDT"),
        "0",
        "the live_small scope is flat"
    );
    let sol_posts = h
        .order_posts()
        .into_iter()
        .filter(|post| post.param("symbol") == Some("SOLUSDT"))
        .collect::<Vec<_>>();
    assert!(!sol_posts.is_empty());
    assert!(sol_posts
        .iter()
        .all(|post| post.param("reduceOnly") == Some("true")
            && post.param("type") == Some("MARKET")
            && post.param("side") == Some("SELL")));
    assert!(
        h.venue
            .requests()
            .iter()
            .all(|request| request.api_key != format!("{OTHER_CRED}-api-key")),
        "the other account is never sent anything"
    );
    h.cleanup().await;
}

/// D39 之后单仓平仓不再被档位闸门拒绝；执行入口在取得派发权之前的其余拒绝
/// （这里：派发那一刻凭据的 venue 配置不可读——VenueConfiguration 对所有角色
/// 生效）仍须如实终结并释放 fence，而不是永远停在 prepared。按 ClosePosition
/// 的生产顺序调用同一组生产函数：prepare → execute_intents → reject_unclaimed；
/// 配置在 prepare 之后、派发之前损坏（一次 RPC 之内无法从外部插入这一刻）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_refused_before_claiming_dispatch_is_rejected_and_releases_the_fence() {
    let h = Harness::sqlite(vec![], None).await;
    let store = h.store();
    let request = h.engine.close_request(CRED, POS).await;
    let crate::position_close::Prepare::Dispatch(context) =
        crate::position_close::prepare(store, &request)
            .await
            .expect("production prepare")
    else {
        panic!("prepare must commit a dispatchable request");
    };
    let venue_env = crate::venue::venue_env_name(CRED);
    std::env::set_var(&venue_env, "not-a-venue");
    let execution = crate::svc::EngineExecutionService::new(store.clone())
        .without_background_exchange_tasks()
        .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"));
    let outcome = crate::proto::engine_execution_server::EngineExecution::execute_intents(
        &execution,
        context.execution_request(),
    )
    .await;
    std::env::set_var(&venue_env, "testnet");
    let acks = outcome.expect("execute_intents").into_inner().acks;
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].status, "rejected", "{acks:?}");
    assert!(acks[0].message.contains(&venue_env), "{acks:?}");
    assert!(
        crate::position_close::reject_unclaimed(store, &context, "CLOSE_SUBMISSION_REFUSED")
            .await
            .expect("reject unclaimed")
    );
    let refused = h.engine.receipt(&request).await;
    assert_receipt(
        &refused,
        "rejected",
        "CLOSE_SUBMISSION_REFUSED",
        "refused before claiming dispatch",
    );
    assert!(refused.engine_order_id.is_none());
    assert_eq!(
        protection(&refused),
        ("in_place".into(), "STOP_LOSS_ON_VENUE".into())
    );
    assert!(
        !fence(store, &request.request_ref).await,
        "no fence left behind"
    );
    assert_eq!(position_row(store, POS).await.0, "protected");
    assert!(h.order_posts().is_empty(), "nothing reached the venue");
    h.cleanup().await;
}

/// 过滤器步长取整会改变权威数量时，这张单就不是「全部」：同一裁决事务里
/// 拒绝，仓位按既有失败释放语义回到原状态，零交易所写入、零残留 fence。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_close_whose_normalized_quantity_is_not_the_full_position_is_rejected() {
    let h = Harness::sqlite(vec![], None).await;
    let store = h.store();
    seed_exchange_position(
        store,
        CRED,
        "bot_wom1_step",
        "pos_wom1_step",
        "binance|futures|XRP|USDT|USDT",
        "long",
        "1.00005",
    )
    .await;
    let request = h.engine.close_request(CRED, "pos_wom1_step").await;
    let refused = h.engine.close(&request).await;
    assert_receipt(
        &refused,
        "rejected",
        "CLOSE_QUANTITY_NOT_EXACT",
        "step rounding",
    );
    assert!(refused.engine_order_id.is_none());
    assert!(
        h.order_posts().is_empty(),
        "no partial-quantity order reaches the venue"
    );
    assert!(!fence(store, &request.request_ref).await);
    let (state, qty, revision) = position_row(store, "pos_wom1_step").await;
    assert_eq!((state.as_str(), qty.as_str()), ("protected", "1.00005"));
    assert!(revision as u64 > request.expected_revision);
    h.cleanup().await;
}
