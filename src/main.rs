// Production RPC futures and the fault-injection server exceed rustc's
// default layout-query depth. This only raises the compile-time limit.
#![recursion_limit = "256"]
// Preserve established RPC signatures, SQL bindings and protocol enum names.
// Packaging does not change the execution contract to satisfy style suggestions.
#![allow(
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::enum_variant_names
)]

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use tonic::transport::Server;
use tonic::{Request, Status};
use tracing_subscriber::EnvFilter;

mod proto {
    tonic::include_proto!("execution_engine.vnext");
}

mod proto_v2 {
    tonic::include_proto!("execution_engine.v2");
}

mod account_identity;
mod account_order_observation;
mod account_order_read;
#[cfg(test)]
mod account_order_read_tests;
mod auto_triggers;
mod binance_futures;
mod binance_response_settlement;
mod binance_user_stream;
mod binance_ws_api;
mod clock;
mod egress_identity;
mod exchange_filters;
#[cfg(feature = "l4-fault-injection")]
mod fault_injection;
#[allow(dead_code)]
mod fill_replay;
#[cfg(test)]
mod k04_fake_venue;
mod kill_switch;
#[cfg(feature = "l4-fault-injection")]
mod l4_fake_exchange;
#[cfg(feature = "l4-fault-injection")]
mod l4_file_rpc;
mod lifecycle_fence;
#[cfg(test)]
mod lifecycle_fence_tests;
mod market_data;
mod position_close;
#[cfg(test)]
mod position_close_exchange_tests;
#[cfg(test)]
mod position_close_tests;
mod position_state;
mod protection;
mod protection_cleanup;
mod protection_dispatch;
mod rate_limiter;
mod reconcile;
mod reconcile_trace;
mod risk;
mod simulation_quote;
mod store;
mod svc;
mod svc_v2;
mod venue;

use svc::{EngineEventsService, EngineExecutionService};

#[derive(Clone)]
struct AuthInterceptor {
    tokens: Vec<String>,
}

impl AuthInterceptor {
    fn new(tokens: Vec<String>) -> Self {
        Self { tokens }
    }

    fn check(&self, req: &Request<()>) -> Result<(), Status> {
        let Some(v) = req.metadata().get("authorization") else {
            return Err(Status::unauthenticated("missing authorization metadata"));
        };
        let Ok(s) = v.to_str() else {
            return Err(Status::unauthenticated("invalid authorization metadata"));
        };
        let token = s.strip_prefix("Bearer ").unwrap_or("");
        if token.is_empty() {
            return Err(Status::unauthenticated("invalid authorization scheme"));
        }
        if self.tokens.iter().any(|t| t == token) {
            return Ok(());
        }
        Err(Status::unauthenticated("invalid token"))
    }
}

/// **生产 gRPC server 的服务装配**——`main` 与 wiring proof 用例共用同一份。
///
/// ****：R9 的提交 wiring proof 全部直调 v1 的
/// trait helper（`EngineExecution::execute_intents(&exec, req)`），而 Python
/// 编排器真实走的是 **v2 gRPC 方法**：tonic transport → auth interceptor →
/// `svc_v2` 的规范化/映射 → v1 shared core → 交易所客户端分类。中间那两层
/// 从未被任何用例执行过，评审变异「破坏 v2 `order_type` 映射」5/5 存活。
///
/// 装配抽成这一个函数，用例才能**调 `main` 调的那个符号**去搭同一台 server：
/// interceptor 的挂载方式、四个服务的注册、v2 与 v1 的共存关系都不再有
/// 第二份实现可以走样。
fn add_engine_services(
    mut server: Server,
    exec: EngineExecutionService,
    events: EngineEventsService,
    exec_v2: svc_v2::EngineExecutionV2Service,
    events_v2: svc_v2::EngineEventsV2Skeleton,
    auth: Option<AuthInterceptor>,
) -> tonic::transport::server::Router {
    if let Some(interceptor) = auth {
        let interceptor_exec = interceptor.clone();
        let interceptor_events = interceptor.clone();
        let interceptor_exec_v2 = interceptor.clone();
        let interceptor_events_v2 = interceptor;
        let exec_svc = proto::engine_execution_server::EngineExecutionServer::with_interceptor(
            exec,
            move |req: Request<()>| {
                interceptor_exec.check(&req)?;
                Ok(req)
            },
        );

        let events_svc = proto::engine_events_server::EngineEventsServer::with_interceptor(
            events,
            move |req: Request<()>| {
                interceptor_events.check(&req)?;
                Ok(req)
            },
        );

        let exec_v2_svc =
            proto_v2::engine_execution_server::EngineExecutionServer::with_interceptor(
                exec_v2,
                move |req: Request<()>| {
                    interceptor_exec_v2.check(&req)?;
                    Ok(req)
                },
            );

        let events_v2_svc = proto_v2::engine_events_server::EngineEventsServer::with_interceptor(
            events_v2,
            move |req: Request<()>| {
                interceptor_events_v2.check(&req)?;
                Ok(req)
            },
        );

        server
            .add_service(exec_svc)
            .add_service(events_svc)
            .add_service(exec_v2_svc)
            .add_service(events_v2_svc)
    } else {
        server
            .add_service(proto::engine_execution_server::EngineExecutionServer::new(
                exec,
            ))
            .add_service(proto::engine_events_server::EngineEventsServer::new(events))
            .add_service(proto_v2::engine_execution_server::EngineExecutionServer::new(exec_v2))
            .add_service(proto_v2::engine_events_server::EngineEventsServer::new(
                events_v2,
            ))
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim().to_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => default,
    }
}

fn parse_tokens() -> Vec<String> {
    let raw = std::env::var("RUSTCORE_GRPC_AUTH_TOKENS")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("RUSTCORE_GRPC_AUTH_TOKEN").ok())
        .or_else(|| std::env::var("EXECUTION_TOKEN").ok())
        .unwrap_or_default();

    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Admission is completed before opening the store or starting any task.
/// The original listening descriptor remains here while tonic serves a clone.
struct GrpcAdmission {
    addr: SocketAddr,
    listener: std::net::TcpListener,
    server: Server,
    auth: Option<AuthInterceptor>,
    tls_enabled: bool,
}

enum StartupTransport {
    Grpc(GrpcAdmission),
    #[cfg(feature = "l4-fault-injection")]
    FileRpc(l4_file_rpc::PreparedFileRpc),
}

struct StartupAdmission {
    transport: StartupTransport,
    db_url: String,
}

fn validate_tls_certificates(
    pem: &[u8],
) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .context("parse configured TLS certificate PEM failed")?;
    anyhow::ensure!(
        !certificates.is_empty(),
        "configured TLS certificate chain is empty"
    );
    let mut parsed = rustls::RootCertStore::empty();
    for certificate in &certificates {
        parsed
            .add(certificate.clone())
            .context("parse configured TLS certificate DER failed")?;
    }
    Ok(certificates)
}

fn validate_tls_identity(certificate: &[u8], private_key: &[u8]) -> anyhow::Result<()> {
    let certificates = validate_tls_certificates(certificate)?;
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(private_key))
        .context("parse configured TLS private key failed")?
        .ok_or_else(|| anyhow::anyhow!("configured TLS private key is missing"))?;
    let identity = rustls::sign::CertifiedKey::from_der(
        certificates,
        key,
        &rustls::crypto::ring::default_provider(),
    )
    .context("load configured TLS identity failed")?;
    // from_der permits Unknown consistency. Admission requires an actual match.
    identity
        .keys_match()
        .context("configured TLS certificate/private key do not match")?;
    Ok(())
}

impl StartupAdmission {
    fn from_env() -> anyhow::Result<Self> {
        venue::validate_fault_proxy_startup_config()
            .map_err(anyhow::Error::msg)
            .context("fault-proxy startup validation failed")?;
        let db_url = std::env::var("RUSTCORE_DB_URL")
            .unwrap_or_else(|_| "sqlite://./data/rustcore.db".to_string());

        #[cfg(feature = "l4-fault-injection")]
        if let Some(file_rpc) = l4_file_rpc::prepare_if_configured()? {
            return Ok(Self {
                transport: StartupTransport::FileRpc(file_rpc),
                db_url,
            });
        }

        let addr: SocketAddr = std::env::var("RUSTCORE_GRPC_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()
            .context("invalid RUSTCORE_GRPC_ADDR")?;
        let tokens = parse_tokens();
        let require_auth = !env_bool("RUSTCORE_GRPC_AUTH_DISABLED", false)
            && (!tokens.is_empty() || !addr.ip().is_loopback());
        if require_auth && tokens.is_empty() {
            anyhow::bail!("gRPC auth required (non-loopback bind) but no token configured; set RUSTCORE_GRPC_AUTH_TOKEN (or EXECUTION_TOKEN) or set RUSTCORE_GRPC_AUTH_DISABLED=1 for local dev only");
        }
        let tls_path = |name: &str| -> anyhow::Result<Option<String>> {
            match std::env::var(name) {
                Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
                Ok(_) => anyhow::bail!("configured gRPC TLS path must not be empty"),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(_) => anyhow::bail!("configured gRPC TLS path is not valid Unicode"),
            }
        };
        let cert = tls_path("RUSTCORE_GRPC_TLS_CERT_PATH")?;
        let key = tls_path("RUSTCORE_GRPC_TLS_KEY_PATH")?;
        let client_ca = tls_path("RUSTCORE_GRPC_TLS_CLIENT_CA_PATH")?;
        let tls_enabled = cert.is_some() || key.is_some() || client_ca.is_some();
        let mut server = Server::builder();
        if tls_enabled {
            let (Some(cert), Some(key)) = (cert, key) else {
                anyhow::bail!("gRPC TLS requires both certificate and private key paths");
            };
            let certificate = std::fs::read(cert).context("read gRPC TLS certificate failed")?;
            let private_key = std::fs::read(key).context("read gRPC TLS private key failed")?;
            // tonic 0.11 accepts an empty PEM chain and does not check key match.
            validate_tls_identity(&certificate, &private_key)
                .context("validate gRPC TLS identity failed")?;
            let identity = tonic::transport::Identity::from_pem(certificate, private_key);
            let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            if let Some(ca) = client_ca {
                let ca = std::fs::read(ca).context("read gRPC TLS client CA failed")?;
                validate_tls_certificates(&ca).context("validate gRPC TLS client CA failed")?;
                tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca));
            }
            server = server
                .tls_config(tls)
                .context("apply gRPC TLS config failed")?;
        }
        let listener = std::net::TcpListener::bind(addr).context("reserve gRPC listener failed")?;
        listener
            .set_nonblocking(true)
            .context("set gRPC listener nonblocking failed")?;
        Ok(Self {
            transport: StartupTransport::Grpc(GrpcAdmission {
                addr: listener
                    .local_addr()
                    .context("read gRPC listener address failed")?,
                listener,
                server,
                auth: require_auth.then(|| AuthInterceptor::new(tokens)),
                tls_enabled,
            }),
            db_url,
        })
    }
}

/// Fields drop in declaration order, including unwinding: background work ends
/// before the admission descriptor is released. Keeping it only in async main
/// would release it before the macro-owned Runtime finishes shutting down.
struct AdmittedRuntime {
    runtime: tokio::runtime::Runtime,
    admission: StartupAdmission,
}

impl AdmittedRuntime {
    fn new(admission: StartupAdmission) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("build Rust runtime failed")?;
        Ok(Self { runtime, admission })
    }

    fn run(self) -> anyhow::Result<()> {
        self.runtime.block_on(run_admitted(&self.admission))
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();
    // Apply safe process defaults before constructing any threads or clients.
    // Explicit venue/endpoint settings continue to use the existing resolver.
    let mainnet = egress_identity::mainnet_configured(std::env::vars())
        || ["BINANCE_FUTURES_BASE_URL", "BINANCE_FUTURES_WS_BASE_URL"]
            .iter()
            .filter_map(|name| std::env::var(name).ok())
            .filter_map(|value| url::Url::parse(&value).ok())
            .any(|value| {
                matches!(
                    value.host_str(),
                    Some("fapi.binance.com" | "fstream.binance.com")
                )
            });
    for (name, testnet, mainnet_url) in [
        (
            "BINANCE_FUTURES_BASE_URL",
            venue::BINANCE_TESTNET_REST_BASE_URL,
            venue::BINANCE_MAINNET_REST_BASE_URL,
        ),
        (
            "BINANCE_FUTURES_WS_BASE_URL",
            venue::BINANCE_TESTNET_WS_BASE_URL,
            venue::BINANCE_MAINNET_WS_BASE_URL,
        ),
    ] {
        if std::env::var_os(name).is_none() {
            std::env::set_var(name, if mainnet { mainnet_url } else { testnet });
        }
    }
    if mainnet {
        tracing::warn!("*** MAINNET CONFIGURATION ENABLED: real exchange access is possible; order admission gates still apply ***");
    } else {
        tracing::info!("safe startup defaults: paper execution; Binance testnet endpoints unless explicitly overridden");
    }
    AdmittedRuntime::new(StartupAdmission::from_env()?)?.run()
}

async fn run_admitted(admission: &StartupAdmission) -> anyhow::Result<()> {
    let settlements = binance_response_settlement::Settlements::new();
    let _registration = binance_response_settlement::install(settlements.clone())?;
    // Keep the runtime/admission descriptor alive until all known safety facts
    // reach a terminal result, including service errors and startup early `?`.
    let result = tokio::select! {
        result = run_admitted_service(admission) => result,
        result = shutdown_requested() => result,
    };
    let settlement_result = settlements.close().await;
    if let Err(error) = settlement_result {
        tracing::error!("rustcore shutdown has unresolved Binance safety writes");
        return Err(error).context("drain Binance response settlements");
    }
    result
}

async fn shutdown_requested() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
    }
}

async fn run_admitted_service(admission: &StartupAdmission) -> anyhow::Result<()> {
    // Convert the reserved socket without closing/rebinding it. Keep the
    // Server::builder defaults (TCP_NODELAY=true and no TCP keepalive override).
    let incoming = match &admission.transport {
        StartupTransport::Grpc(grpc) => Some(
            tonic::transport::server::TcpIncoming::from_listener(
                tokio::net::TcpListener::from_std(
                    grpc.listener
                        .try_clone()
                        .context("clone admitted gRPC listener failed")?,
                )
                .context("register admitted gRPC listener failed")?,
                true,
                None,
            )
            .map_err(|_| anyhow::anyhow!("prepare admitted gRPC incoming failed"))?,
        ),
        #[cfg(feature = "l4-fault-injection")]
        StartupTransport::FileRpc(_) => None,
    };
    sqlx::any::install_default_drivers();
    let store = store::Store::connect(&admission.db_url).await?;

    // 生效的出口权重预算必须在启动时可见：f79a7c8 改掉了一个**已上线数字的含义**，
    // 而没有任何一条日志说过生效预算是多少、来自哪个变量。只告警，绝不拒绝启动。
    binance_futures::log_effective_egress_budget();
    let egress_identity_config = egress_identity::EgressIdentityConfig::from_env();
    egress_identity::log_startup_identity(
        &egress_identity_config,
        egress_identity::mainnet_configured(std::env::vars()),
    );

    #[cfg(feature = "l4-fault-injection")]
    fault_injection::startup_checkpoint(&store).await?;

    let kill_signal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    rustcore::kill_signal::install_sigusr1_handler()
        .context("install SIGUSR1 kill-switch handler")?;
    let _kill_signal_task = rustcore::kill_signal::spawn_handoff_task(kill_signal.clone());
    let _kill_file_task = kill_switch::spawn_manual_trigger_task(
        store.clone(),
        kill_switch::default_kill_file(),
        kill_signal,
    );
    let _egress_identity_task =
        egress_identity::spawn_egress_identity_task(store.clone(), egress_identity_config);
    let _kill_action_task = kill_switch::spawn_action_retry_task(store.clone());
    // D30：平仓后的保护单善后驱动（义务持久在库，重启自然续跑）。
    let _protection_cleanup_task = protection_cleanup::spawn_cleanup_task(store.clone());
    // 行情中断 / 订单拒绝率两条自动触发源的周期扫描。
    // LLM 连续失败由编排器侧 latch 经 SetKillSwitch RPC 接入（rustcore 不调 LLM）。
    let _auto_trigger_task = auto_triggers::spawn_auto_trigger_task(store.clone());

    let exec = EngineExecutionService::new(store.clone());
    exec.start_fill_recovery_on_startup()
        .await
        .context("start persisted fill recovery scopes")?;
    // **required 订阅必须跨重启
    // 重新变成执行体。**
    //
    // `EngineExecutionService::new` 每次都造一个空的 `MarketDataManager`，
    // 而这里此前只调 user-stream 的 `start_discovered`：重启之后 required
    // 集合还在库里（判据看得见），kline worker 一个都没有（执行体没了）。
    // 行情因此永远不来，而决策循环照跑。
    exec.restore_market_data_on_startup()
        .await
        .context("restore persisted market data subscriptions")?;
    let events = EngineEventsService::new(store.clone(), Duration::from_millis(800));
    // V2 implements recovery/risk/kill-switch management RPCs. Every v2
    // trading RPC and the v2 event service remains UNIMPLEMENTED.
    // V1 and V2 are two RPC façades over one execution core. Constructing a
    // second core here used to duplicate limiter/reconcile managers and could
    // multiply exchange traffic for the same credential.
    let exec_v2 =
        svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
    let events_v2 = svc_v2::EngineEventsV2Skeleton;

    match &admission.transport {
        #[cfg(feature = "l4-fault-injection")]
        StartupTransport::FileRpc(file_rpc) => {
            l4_file_rpc::serve_prepared(file_rpc, exec, exec_v2).await?
        }
        StartupTransport::Grpc(grpc) => {
            tracing::info!(
                grpc_addr = %grpc.addr,
                auth = if grpc.auth.is_some() { "token" } else { "disabled" },
                tls = if grpc.tls_enabled { "enabled" } else { "disabled" },
                "rustcore starting"
            );
            let serving = add_engine_services(
                grpc.server.clone(),
                exec,
                events,
                exec_v2,
                events_v2,
                grpc.auth.clone(),
            )
            .serve_with_incoming(incoming.expect("gRPC admission owns its incoming socket"));
            #[cfg(feature = "l4-fault-injection")]
            {
                tokio::select! {
                    result = serving => result.context("grpc serve failed")?,
                    result = settlement_service_error_injection() => result?,
                }
            }
            #[cfg(not(feature = "l4-fault-injection"))]
            serving.await.context("grpc serve failed")?;
        }
    }

    Ok(())
}

/// Test-build-only injection of a service error, after the external fixture has
/// observed a real signed response. It returns through the ordinary main drain.
#[cfg(feature = "l4-fault-injection")]
async fn settlement_service_error_injection() -> anyhow::Result<()> {
    if !fault_injection::selected("settlement_service_error") {
        return std::future::pending().await;
    }
    let directory = std::path::PathBuf::from(
        std::env::var("L4_FAULT_MARKER_DIR")
            .context("settlement fault marker directory required")?,
    );
    anyhow::ensure!(
        directory.is_absolute(),
        "settlement fault marker directory must be absolute"
    );
    let marker = directory.join("settlement_service_error.trigger");
    loop {
        if marker.is_file() {
            tracing::info!("synthetic settlement service error requested");
            anyhow::bail!("synthetic settlement service error");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use tonic::server::NamedService;
    use tonic::transport::Server;

    use super::{proto_v2, svc_v2};

    #[tokio::test]
    async fn v2_services_are_registered_and_server_is_constructible() {
        type ExecutionServer = proto_v2::engine_execution_server::EngineExecutionServer<
            svc_v2::EngineExecutionV2Service,
        >;
        type EventsServer =
            proto_v2::engine_events_server::EngineEventsServer<svc_v2::EngineEventsV2Skeleton>;

        assert_eq!(
            <ExecutionServer as NamedService>::NAME,
            "execution_engine.v2.EngineExecution"
        );
        assert_eq!(
            <EventsServer as NamedService>::NAME,
            "execution_engine.v2.EngineEvents"
        );

        sqlx::any::install_default_drivers();
        let path = std::env::temp_dir().join(format!("rustcore-v2-{}.db", ulid::Ulid::new()));
        let store = super::store::Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect test store");
        let router = Server::builder()
            .add_service(svc_v2::execution_server(store.clone()))
            .add_service(svc_v2::events_server());
        drop(router);
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }

    fn assert_listener_outlives_runtime(unwind: bool) {
        use std::sync::mpsc;
        use std::time::Duration;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let admitted = super::AdmittedRuntime::new(super::StartupAdmission {
            transport: super::StartupTransport::Grpc(super::GrpcAdmission {
                addr,
                listener,
                server: Server::builder(),
                auth: None,
                tls_enabled: false,
            }),
            db_url: "unused: lifecycle test never opens a store".into(),
        })
        .unwrap();
        let (running_tx, running_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        admitted.runtime.spawn_blocking(move || {
            running_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        running_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        struct ShutdownNotice(mpsc::Sender<()>);
        impl Drop for ShutdownNotice {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let (started_tx, started_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        admitted.runtime.spawn(async move {
            let _notice = ShutdownNotice(shutdown_tx);
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let shutdown = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _owned = admitted;
                if unwind {
                    panic!("expected startup unwinding test");
                }
            }));
            assert_eq!(result.is_err(), unwind);
        });
        // A runtime-owned future is destroyed only after shutdown starts.
        // Blocking work remains alive until this thread explicitly releases it.
        let teardown_started = shutdown_rx.recv_timeout(Duration::from_secs(3)).is_ok();
        let reservation_held = std::net::TcpListener::bind(addr).is_err();
        let runtime_was_waiting = !shutdown.is_finished();
        release_tx.send(()).unwrap();
        shutdown.join().unwrap();
        assert_eq!(teardown_started, true, "runtime teardown must be observed");
        assert_eq!(
            runtime_was_waiting, true,
            "runtime must still own its blocked work during the probe"
        );
        assert_eq!(
            reservation_held, true,
            "listener reservation released while admitted runtime still had live work"
        );
        std::net::TcpListener::bind(addr)
            .expect("listener must be released after runtime shutdown");
    }

    #[test]
    fn startup_listener_reservation_outlives_runtime_shutdown() {
        assert_listener_outlives_runtime(false);
    }

    #[test]
    fn startup_listener_reservation_outlives_runtime_unwind() {
        assert_listener_outlives_runtime(true);
    }
}

#[cfg(test)]
mod k05_b_tests;
