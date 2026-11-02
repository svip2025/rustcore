// A2c-R1：真实鉴权 gRPC `ReconcileNow`（生产 vnext 服务，Python 编排器调的那个
// 符号）→ auth interceptor → `svc.rs` 的 `ensure_running` → 已经**真的结束**的
// 周期对账任务必须被重新拉起。
//
// 交易所两端都在本机，且**分成两个 sink**，因此"RPC 自己那一次对账"与"周期
// 任务又活过来并落盘"不会被混为一谈：
// * RPC 自带的那一次 `reconcile_binance_futures` 走**真实 HTTP 客户端**
//   （`BINANCE_FUTURES_BASE_URL` 指向本地假交易所），它只报 `USDC` 余额；
// * 周期任务走 `with_stubbed_reconcile_exchange` 的内存响应队列，它报 `USDT`
//   余额，且**每一轮的数值都不同**——`engine_balances` 里的 USDT 值因此逐字
//   说明"第几轮周期对账真的跑完并落库了"。
//
// 任务的结束不是测试直接掀桌子：桩交易所的请求钩子让**生产任务自己在第 5 条
// 读取上 panic**，与生产里对账任务因 panic 退出、注册表占位仍在的形态一致。
const RECONCILE_NOW_PATH: &str = "/execution_engine.vnext.EngineExecution/ReconcileNow";

/// 生产 vnext `ReconcileNow`：真实 TCP + HTTP/2 + protobuf + auth interceptor。
async fn a2c_r1_reconcile_now(
    addr: std::net::SocketAddr,
    token: Option<&str>,
    credential_id: &str,
    reason: &str,
) -> Result<crate::proto::ReconcileNowResponse, tonic::Status> {
    use tonic::codec::ProstCodec;
    use tonic::codegen::http::uri::PathAndQuery;
    use tonic::transport::Endpoint;

    use crate::proto::{ReconcileNowRequest, ReconcileNowResponse};

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(ReconcileNowRequest {
        credential_id: credential_id.to_string(),
        reason: reason.to_string(),
    });
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    client
        .unary(
            request,
            PathAndQuery::from_static(RECONCILE_NOW_PATH),
            ProstCodec::<ReconcileNowRequest, ReconcileNowResponse>::default(),
        )
        .await
        .map(|response| response.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2c_r1_ended_reconcile_task_recovers_through_authenticated_reconcile_now_rpc() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // 只有桩队列知道的余额，一轮一个值。
    const ROUND1_WALLET: &str = "7777.77";
    const ROUND2_WALLET: &str = "8888.88";
    const ROUND3_WALLET: &str = "9999.99";
    // 只有本地假交易所（RPC 自带那次对账）知道的余额，落在另一个 asset 上。
    const RPC_HTTP_WALLET: &str = "1111.11";

    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential_id = "a2c-r1-reconcile-now";
    let Some(bound) = bind_owned_loopback(credential_id, AUTH_TOKEN_PREFIX).await else {
        return;
    };

    // 本地假交易所：RPC 自带的那一次对账真的走 HTTP 打到这里。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("owned local exchange HTTP listener");
    let http_addr = listener.local_addr().unwrap();
    let http_requests = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let observed_http = http_requests.clone();
    let http_task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let requests = observed_http.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0u8; 2048];
                    let read = socket.read(&mut chunk).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    assert!(bytes.len() < 16384);
                }
                let text = String::from_utf8(bytes).unwrap();
                let first = text.lines().next().unwrap();
                let mut parts = first.split_whitespace();
                let method = parts.next().unwrap();
                let path = parts.next().unwrap().split('?').next().unwrap();
                requests.lock().unwrap().push(format!("{method} {path}"));
                let body = match path {
                    "/fapi/v1/exchangeInfo" => serde_json::json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400}],"symbols":[]}),
                    "/fapi/v2/balance" => serde_json::json!([
                        {"asset":"USDC","balance":RPC_HTTP_WALLET,"availableBalance":RPC_HTTP_WALLET}
                    ]),
                    _ => serde_json::json!([]),
                }
                .to_string();
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
            });
        }
    });

    let (key_env, secret_env) = crate::binance_futures::key_env_names(credential_id);
    let _key = EnvGuard::set(key_env, "a2c-r1-local-key");
    let _secret = EnvGuard::set(secret_env, "a2c-r1-local-secret");
    // venue 留空，REST 因此用下面这个本地地址（venue 一旦设定，生产会强制用
    // 交易所真实域名——本用例不联任何外网）。venue 为空时 `ReconcileNow` 的
    // 网关判据落在 ENGINE_LIVE_ENABLED 上。
    let _base_url = EnvGuard::set(
        "BINANCE_FUTURES_BASE_URL".to_string(),
        &format!("http://{http_addr}"),
    );
    let _live = EnvGuard::set("ENGINE_LIVE_ENABLED".to_string(), "1");
    let _interval = EnvGuard::set("RECONCILE_INTERVAL_MS".to_string(), "1000");
    // RPC 自带那一次对账用的是**真实限流器**（`new_with_store` 按
    // `TokenBucketConfig::from_env` 建桶）。生产默认 burst=10/per-sec=5，而无
    // symbol 的 openOrders/openAlgoOrders 单条权重就是 40：`acquire_prioritized`
    // 要等桶攒到 cost+退出预留才放行，oneoff 在本用例的等待窗口内根本走不到
    // positionRisk。本用例测的是**任务恢复**，不是默认预算；沿用仓库既有的
    // 环境变量把本地假交易所夹具的桶放宽到足够跑完两轮完整扫描（每轮
    // openOrders 40 + openAlgoOrders 40 + positionRisk 10 + balance 10 ≈ 100
    // 权重），与 binance_user_stream.rs 的既有夹具同一套变量。
    let _weight_per_sec = EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC".to_string(), "10000");
    let _weight_burst = EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_BURST".to_string(), "10000");

    let mut reconcile_responses = Vec::new();
    for wallet in [
        ROUND1_WALLET,
        ROUND2_WALLET,
        ROUND3_WALLET,
        ROUND3_WALLET,
        ROUND3_WALLET,
    ] {
        reconcile_responses.push((200, "[]".to_string()));
        reconcile_responses.push((200, "[]".to_string()));
        reconcile_responses.push((200, "[]".to_string()));
        reconcile_responses.push((
            200,
            serde_json::json!([{"asset":"USDT","balance":wallet,"availableBalance":wallet}])
                .to_string(),
        ));
    }
    let stub_requests = Arc::new(AtomicUsize::new(0));
    let hook_counter = stub_requests.clone();
    let reconcile_client = BinanceFuturesClient::new_with_mock_responses_and_hook(
        credential_id,
        reconcile_responses,
        Arc::new(
            move |_path: String| -> std::pin::Pin<
                Box<dyn std::future::Future<Output = ()> + Send>,
            > {
                let counter = hook_counter.clone();
                Box::pin(async move {
                    // 第 5 条读取＝原任务第一次周期 tick 的开头：让这个生产
                    // 任务在自己的执行里 panic 结束。
                    if counter.fetch_add(1, Ordering::SeqCst) == 4 {
                        panic!("A2c-R1 injected reconcile task death");
                    }
                })
            },
        ),
    );

    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("rustcore-a2c-r1-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("connect sqlite test store and run migrations");

    let exec = crate::svc::EngineExecutionService::new(store.clone())
        .without_background_exchange_tasks()
        .with_stubbed_reconcile_exchange(reconcile_client.clone());
    let events = crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800));
    let exec_v2 = EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
    let server = bound.client();
    let (shutdown, receiver) = tokio::sync::oneshot::channel::<()>();
    let router = crate::add_engine_services(
        Server::builder(),
        exec.clone(),
        events,
        exec_v2,
        super::EngineEventsV2Skeleton,
        Some(crate::AuthInterceptor::new(vec![bound
            .identity_token
            .clone()])),
    );
    let grpc_task = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(bound.incoming, async move {
                let _ = receiver.await;
            })
            .await
            .expect("owned loopback gRPC server must run");
    });
    // ① 真实鉴权：没有令牌的同一个入口进不来，也不许起任何管理任务。
    let unauthorized =
        a2c_r1_reconcile_now(server.addr, None, credential_id, "unauthenticated").await;
    assert_eq!(
        unauthorized.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert!(
        !exec
            .reconcilers_for_tests()
            .is_running_for_tests(credential_id)
            .await
    );

    // ② 带令牌的生产 RPC：周期对账任务被真的拉起，第一轮真值落库。
    let accepted = a2c_r1_reconcile_now(
        server.addr,
        Some(&server.identity_token),
        credential_id,
        "a2c-r1 first admission",
    )
    .await
    .expect("authenticated ReconcileNow");
    assert!(accepted.accepted, "{accepted:?}");
    assert!(
        wait_for("round 1 wallet", || async {
            reconciled_balance(&store, credential_id).await
                == Some((ROUND1_WALLET.to_string(), ROUND1_WALLET.to_string()))
        })
        .await,
        "the admitted periodic task must persist its first round: {:?}",
        reconciled_balance(&store, credential_id).await
    );

    // ③ 这个任务在它第一次周期 tick 里 panic 结束了，注册表占位还留着。
    assert!(
        wait_for("task death", || async {
            exec.reconcilers_for_tests()
                .task_finished_for_tests(credential_id)
                .await
                == Some(true)
        })
        .await,
        "the injected panic must really finish the reconcile task"
    );
    assert!(
        exec.reconcilers_for_tests()
            .is_running_for_tests(credential_id)
            .await,
        "the finished task keeps its registry slot — the premise of A2c-R1"
    );
    assert_eq!(
        reconcile_client.recorded_requests().len(),
        5,
        "the dead task must stop after the read it died on: {:?}",
        reconcile_client.recorded_requests()
    );

    // ④ 下一次真实生产 RPC 必须让管理重新活过来。
    let readmitted = a2c_r1_reconcile_now(
        server.addr,
        Some(&server.identity_token),
        credential_id,
        "a2c-r1 readmission",
    )
    .await
    .expect("authenticated ReconcileNow after the task died");
    assert!(readmitted.accepted, "{readmitted:?}");

    // ⑤ 判据不是 RPC 的 accepted，也不是注册表长度：重新准入的**周期任务**
    //    自己又跑完了一轮，并且**继续按周期跑**——第三轮的真值落进了库。
    //    标准比较断言（而非裸 assert!）：固定变异 runner 只认 assert_eq!/标准
    //    assertion failed 诊断，消息与判据保持不变。
    assert_eq!(
        wait_for("round 2 wallet", || async {
            reconciled_balance(&store, credential_id).await
                == Some((ROUND2_WALLET.to_string(), ROUND2_WALLET.to_string()))
        })
        .await,
        true,
        "the readmitted task must run its own reconcile round: stub requests={:?}",
        reconcile_client.recorded_requests().len()
    );
    assert!(
        wait_for("round 3 wallet", || async {
            reconciled_balance(&store, credential_id).await
                == Some((ROUND3_WALLET.to_string(), ROUND3_WALLET.to_string()))
        })
        .await,
        "the readmitted task must keep reconciling periodically: stub requests={:?}",
        reconcile_client.recorded_requests().len()
    );
    assert!(
        reconcile_client.recorded_requests().len() >= 13,
        "a startup round plus at least one periodic tick must have really run: {:?}",
        reconcile_client.recorded_requests().len()
    );
    assert_eq!(
        exec.reconcilers_for_tests()
            .task_finished_for_tests(credential_id)
            .await,
        Some(false),
        "the readmitted task must be alive"
    );

    // ⑥ RPC 自带的那一次对账是**另一条**真实 HTTP 链路，它的真值落在别的
    //    asset 上——上面那些断言与它无关。oneoff 是异步 spawn 的，HTTP 到达
    //    与落盘都要有界等待，不能读一次赌时序。
    assert!(
        wait_for("rpc-side positionRisk", || async {
            http_requests
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry == "GET /fapi/v2/positionRisk")
        })
        .await,
        "the RPC's own reconcile must really go through production HTTP: {:?}",
        http_requests.lock().unwrap().clone()
    );
    assert!(
        wait_for("rpc-side USDC balance", || async {
            sqlx::query(
                "SELECT wallet_balance FROM engine_balances \
                 WHERE credential_id=? AND asset='USDC' LIMIT 1",
            )
            .bind(credential_id)
            .fetch_optional(store.pool())
            .await
            .expect("read the RPC-side balance")
            .map(|row| row.get::<String, _>("wallet_balance"))
                == Some(RPC_HTTP_WALLET.to_string())
        })
        .await,
        "the RPC's own reconcile has its own truth and is not what proves periodic recovery"
    );

    exec.reconcilers_for_tests().abort_all_for_tests().await;
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), grpc_task)
        .await
        .expect("gRPC server shuts down")
        .unwrap();
    http_task.abort();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}
