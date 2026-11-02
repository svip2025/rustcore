// Authenticated production v2 RPCs and a real local HTTP exchange. No socket
// skip is permitted, and no exchange write leaves the owned loopback server.
struct A1Http {
    address: std::net::SocketAddr,
    requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String, bool)>>>,
    task: tokio::task::JoinHandle<()>,
}

async fn a1_http() -> A1Http {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("A1 HTTP loopback is mandatory");
    let address = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let observed = observed.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0u8; 2048];
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                    assert!(bytes.len() < 16384);
                }
                let request = String::from_utf8(bytes).unwrap();
                let mut fields = request.lines().next().unwrap().split_whitespace();
                let method = fields.next().unwrap();
                let url = url::Url::parse(&format!("http://localhost{}", fields.next().unwrap()))
                    .unwrap();
                let path = url.path();
                let query = url
                    .query_pairs()
                    .collect::<std::collections::HashMap<_, _>>();
                let reduce_only = query.get("reduceOnly").is_some_and(|v| v == "true");
                observed
                    .lock()
                    .unwrap()
                    .push((method.to_owned(), path.to_owned(), reduce_only));
                let body = match (method,path) {
                    ("GET", "/fapi/v1/exchangeInfo") => serde_json::json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400}],"symbols":[]}),
                    ("GET", "/fapi/v1/openOrders" | "/fapi/v1/openAlgoOrders") => serde_json::json!([]),
                    ("GET", "/fapi/v2/positionRisk") => serde_json::json!([{"symbol":"BTCUSDT","positionAmt":"2","entryPrice":"100","markPrice":"100","leverage":"1","unRealizedProfit":"0","liquidationPrice":"0","positionSide":"BOTH"}]),
                    ("GET", "/fapi/v2/balance") => serde_json::json!([{"asset":"USDT","balance":"10000","availableBalance":"10000"}]),
                    ("POST", "/fapi/v1/order") => {
                        assert!(reduce_only, "A1 proof may only submit a close");
                        serde_json::json!({"orderId":88001,"clientOrderId":query.get("newClientOrderId").unwrap(),"status":"NEW","symbol":"BTCUSDT","side":"SELL","type":"LIMIT","origQty":"2","executedQty":"0"})
                    }
                    _ => panic!("unexpected A1 HTTP method/path {method} {path}"),
                }.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    A1Http {
        address,
        requests,
        task,
    }
}

struct A1Server {
    client: BoundLoopbackClient,
    exec: crate::svc::EngineExecutionService,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
async fn a1_server(store: &Store) -> A1Server {
    let exec = crate::svc::EngineExecutionService::new(store.clone())
        .without_background_exchange_tasks()
        .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"));
    a1_server_with_execution(store, exec).await
}
async fn a1_server_with_execution(
    store: &Store,
    exec: crate::svc::EngineExecutionService,
) -> A1Server {
    let bound = bind_owned_loopback("a1-account-identity", "a1-auth")
        .await
        .expect("A1 gRPC loopback must not be skipped");
    let client = bound.client();
    let router = crate::add_engine_services(
        Server::builder(),
        exec.clone(),
        crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
        EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone()),
        super::EngineEventsV2Skeleton,
        Some(crate::AuthInterceptor::new(vec![bound
            .identity_token
            .clone()])),
    );
    let (shutdown, receiver) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(bound.incoming, async {
                let _ = receiver.await;
            })
            .await
            .expect("A1 gRPC server");
    });
    A1Server {
        client,
        exec,
        shutdown,
        task,
    }
}
async fn a1_stop(server: A1Server) {
    server.shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server.task)
        .await
        .unwrap()
        .unwrap();
}
async fn a1_reconcile(server: &A1Server, credential: &str) {
    use crate::proto_v2::{ReconcileNowRequest, ReconcileNowResponse};
    use tonic::codec::ProstCodec;
    use tonic::codegen::http::uri::PathAndQuery;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", server.client.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(ReconcileNowRequest {
        credential_id: credential.into(),
        reason: "A1 configuration is independent of clean reconcile".into(),
        schema_version: 1,
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", server.client.identity_token)
            .parse()
            .unwrap(),
    );
    let response: tonic::Response<ReconcileNowResponse> = client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.v2.EngineExecution/ReconcileNow"),
            ProstCodec::<ReconcileNowRequest, ReconcileNowResponse>::default(),
        )
        .await
        .unwrap();
    let response = response.into_inner();
    assert!(
        !response.has_diff,
        "controlled reconcile must actually be clean: {response:?}"
    );
}
async fn a1_assert_refused(server: &A1Server, credential: &str, label: &str) {
    let response = execute_intents_over_grpc(
        &server.client,
        Some(&server.client.identity_token),
        request_for(credential, open_intent(label)),
    )
    .await
    .unwrap();
    assert_eq!(response.acks.len(), 1);
    assert_eq!(
        response.acks[0].status,
        crate::proto_v2::Status::Rejected as i32,
        "{:?}",
        response.acks[0]
    );
    assert_eq!(
        response.acks[0].reject_reason_code,
        crate::account_identity::CONFLICT_CODE,
        "{:?}",
        response.acks[0]
    );
}
async fn a1_position(store: &Store, credential: &str) {
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,exit_epoch,state,active_key,qty,entry_price,updated_at_ms) VALUES('a1-rpc-position',?,'bot_seg3r1',?,'long',1,0,'open','a1-rpc-active','2','100',1)")
        .bind(credential).bind(SYMBOL_KEY).execute(store.pool()).await.unwrap();
}
fn a1_keys(credential: &str, key: &str, secret: &str) -> Vec<EnvGuard> {
    let (key_name, secret_name) = crate::binance_futures::key_env_names(credential);
    vec![
        EnvGuard::set(key_name, key),
        EnvGuard::set(secret_name, secret),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a1_rpc_configured_conflict_survives_clean_reconcile_ready_and_allows_close() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "a1-rpc-configured-a";
    let peer = "a1-rpc-configured-b";
    let _a = a1_keys(credential, "a1-rpc-private-key", "a1-rpc-private-secret");
    let _b = a1_keys(peer, "a1-rpc-private-key", "a1-rpc-private-secret");
    let _stage = EnvGuard::set("ENGINE_STAGE_MAX".into(), "demo");
    let http = a1_http().await;
    let _url = EnvGuard::set(
        "BINANCE_FUTURES_BASE_URL".into(),
        &format!("http://{}", http.address),
    );
    let (store, path) = wiring_store(credential).await;
    a1_position(&store, credential).await;
    let server = a1_server(&store).await;
    let unauthorized = execute_intents_over_grpc(
        &server.client,
        None,
        request_for(credential, open_intent("a1-unauthorized")),
    )
    .await
    .unwrap_err();
    assert_eq!(unauthorized.code(), tonic::Code::Unauthenticated);
    assert!(http.requests.lock().unwrap().is_empty());
    for (index, member) in [credential, peer].iter().enumerate() {
        a1_assert_refused(&server, member, &format!("a1-config-{index}")).await;
    }
    for round in 0..2 {
        a1_reconcile(&server, credential).await;
        let cycle = crate::fill_replay::begin_recovery_cycle(
            &store,
            credential,
            crate::fill_replay::RecoveryTrigger::Startup,
            crate::svc::now_ms(),
        )
        .await
        .unwrap();
        let success = crate::fill_replay::RecoveryPhaseResult::Succeeded;
        assert_eq!(
            crate::fill_replay::finalize_recovery_readiness(
                &store,
                credential,
                success,
                success,
                success,
                cycle,
                crate::svc::now_ms()
            )
            .await
            .unwrap(),
            crate::fill_replay::ReadinessWrite::Ready
        );
        server
            .exec
            .clear_credential_errors(credential)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query(
                "SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id=?"
            )
            .bind(credential)
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("reconcile_diff_open"),
            0
        );
        a1_assert_refused(&server, credential, &format!("a1-after-clean-{round}")).await;
        a1_assert_refused(&server, peer, &format!("a1-peer-after-clean-{round}")).await;
    }
    assert!(http
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(method, _, _)| method == "GET"));
    let mut close = open_intent("a1-close");
    close.action = "close_long".into();
    close.reduce_only = true;
    close.protection = None;
    let response = execute_intents_over_grpc(
        &server.client,
        Some(&server.client.identity_token),
        request_for(credential, close),
    )
    .await
    .unwrap();
    assert_eq!(
        response.acks[0].status,
        crate::proto_v2::Status::Accepted as i32,
        "{:?}",
        response.acks[0]
    );
    let posts = http
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _, _)| method == "POST")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(posts, vec![("POST".into(), "/fapi/v1/order".into(), true)]);
    let verdict = sqlx::query(
        "SELECT replay_input_json FROM risk_verdicts WHERE intent_id='int_a1-after-clean-1'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get::<String, _>("replay_input_json");
    assert_eq!(
        crate::risk::replay_verdict(&verdict)
            .unwrap()
            .reject_reason_code
            .as_deref(),
        Some(crate::account_identity::CONFLICT_CODE)
    );
    assert!(!verdict.contains("a1-rpc-private-key") && !verdict.contains("a1-rpc-private-secret"));
    a1_stop(server).await;
    http.task.abort();
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a1_rpc_later_global_alias_persists_across_restart_and_config_rotation() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "a1-rpc-existing";
    let peer = "a1-rpc-added-later";
    let _a = a1_keys(credential, "a1-rpc-global-key", "a1-rpc-global-secret");
    let _global_key = EnvGuard::set("BINANCE_API_KEY".into(), "a1-rpc-global-key");
    let _global_secret = EnvGuard::set("BINANCE_API_SECRET".into(), "a1-rpc-global-secret");
    let _stage = EnvGuard::set("ENGINE_STAGE_MAX".into(), "demo");
    let http = a1_http().await;
    let _url = EnvGuard::set(
        "BINANCE_FUTURES_BASE_URL".into(),
        &format!("http://{}", http.address),
    );
    let (store, path) = wiring_store(credential).await;
    assert!(crate::account_identity::entry_denial(&store, credential)
        .await
        .is_none());
    let server = a1_server(&store).await;
    // No env locator, durable scope, or task-map entry exists for the peer. Its
    // first authenticated request supplies that second identity to admission.
    a1_assert_refused(&server, peer, "a1-later-first").await;
    assert_eq!(
        sqlx::query("SELECT COUNT(*) AS n FROM risk_verdicts WHERE credential_id=?")
            .bind(peer)
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n"),
        1
    );
    a1_assert_refused(&server, credential, "a1-existing-after-peer").await; // gitleaks:allow -- Synthetic test request label.
    a1_stop(server).await;
    store.pool().close().await;
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let server = a1_server(&store).await;
    a1_assert_refused(&server, credential, "a1-after-restart").await;
    a1_assert_refused(&server, peer, "a1-peer-after-restart").await;
    std::env::set_var(
        crate::binance_futures::key_env_names(credential).0,
        "a1-rpc-rotated-distinct",
    );
    assert!(crate::account_identity::entry_denial(&store, credential)
        .await
        .is_none());
    let response = execute_intents_over_grpc(
        &server.client,
        Some(&server.client.identity_token),
        request_for(credential, open_intent("a1-after-rotation")),
    )
    .await
    .unwrap();
    assert_ne!(
        response.acks[0].reject_reason_code,
        crate::account_identity::CONFLICT_CODE,
        "{:?}",
        response.acks[0]
    );
    assert_ne!(
        response.acks[0].reject_reason_code,
        crate::account_identity::UNAVAILABLE_CODE,
        "{:?}",
        response.acks[0]
    );
    std::env::set_var(
        crate::binance_futures::key_env_names(credential).0,
        "a1-rpc-global-key",
    );
    a1_assert_refused(&server, credential, "a1-conflict-restored").await;
    assert!(http
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(method, _, _)| method != "POST"));
    a1_stop(server).await;
    http.task.abort();
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a1_rpc_submit_boundary_rechecks_after_admission_without_unknown_or_flatten() {
    struct RotateAtCreatedTime {
        peer_key_name: String,
        fired: std::sync::atomic::AtomicBool,
    }
    impl crate::svc::Clock for RotateAtCreatedTime {
        fn now_ms(&self) -> i64 {
            if !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                // The service's first clock call is after the initial identity
                // read and before its order transaction. No production hook or
                // assumed cancellation of an in-flight request is introduced.
                std::env::set_var(&self.peer_key_name, "a1-boundary-shared");
            }
            crate::svc::now_ms()
        }
    }
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = "a1-boundary-a";
    let peer = "a1-boundary-b";
    let _a = a1_keys(credential, "a1-boundary-shared", "a1-boundary-secret");
    let _b = a1_keys(peer, "a1-boundary-other", "a1-boundary-secret");
    let _venue_a = EnvGuard::set(crate::venue::venue_env_name(credential), "testnet");
    let _venue_b = EnvGuard::set(crate::venue::venue_env_name(peer), "testnet");
    let _stage = EnvGuard::set("ENGINE_STAGE_MAX".into(), "demo");
    let (store, path) = wiring_store(credential).await;
    sqlx::query("UPDATE engine_credential_health SET reconcile_diff_open=0 WHERE credential_id=?")
        .bind(credential)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(crate::account_identity::entry_denial(&store, credential)
        .await
        .is_none());
    let exchange = BinanceFuturesClient::new_with_mock_responses(credential, Vec::new());
    let clock = std::sync::Arc::new(RotateAtCreatedTime {
        peer_key_name: crate::binance_futures::key_env_names(peer).0,
        fired: std::sync::atomic::AtomicBool::new(false),
    });
    let exec = crate::svc::EngineExecutionService::new(store.clone())
        .without_background_exchange_tasks()
        .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
        .with_clock(clock.clone())
        .with_submit_client_override(exchange.clone());
    let server = a1_server_with_execution(&store, exec).await;
    a1_assert_refused(&server, credential, "a1-at-submit").await;
    assert!(clock.fired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        exchange.recorded_requests().is_empty(),
        "local conflict must not enter the exchange client"
    );
    let order = sqlx::query(
        "SELECT status, position_id FROM engine_orders WHERE credential_id=?",
    )
        .bind(credential)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        order.get::<String, _>("status"),
        "rejected",
        "boundary refusal is a known non-submission, not unknown"
    );
    let position_id = order.get::<String, _>("position_id");
    let claim = sqlx::query(
        "SELECT state, entry_epoch, active_key IS NULL AS active_key_is_null, \
                pending_intent_id IS NULL AS pending_intent_is_null, \
                pending_idem_key IS NULL AS pending_idem_is_null \
         FROM position_state WHERE position_id=?",
    )
    .bind(&position_id)
    .fetch_one(store.pool())
    .await
    .expect("load retained boundary-rejected Entry epoch");
    assert_eq!(claim.get::<String, _>("state"), "closed");
    assert_eq!(claim.get::<i64, _>("entry_epoch"), 1);
    assert_eq!(claim.get::<i64, _>("active_key_is_null"), 1);
    assert_eq!(claim.get::<i64, _>("pending_intent_is_null"), 1);
    assert_eq!(claim.get::<i64, _>("pending_idem_is_null"), 1);
    let position_count = sqlx::query("SELECT COUNT(*) AS n FROM position_state")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(
        position_count, 1,
        "local refusal must retain exactly one closed Entry epoch"
    );
    for table in ["position_protection_plans", "protection_orders"] {
        let count = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n");
        assert_eq!(
            count, 0,
            "local refusal must not leave pending protection or flatten: {table}"
        );
    }
    let switches = sqlx::query("SELECT scope_key,level FROM kill_switch_state ORDER BY scope_key")
        .fetch_all(store.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("scope_key"),
                row.get::<String, _>("level"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        switches,
        vec![("global".into(), "none".into())],
        "only the factory default may remain; no configuration error may arm flatten"
    );
    let payload=sqlx::query("SELECT payload_json FROM engine_order_events WHERE credential_id=? AND event_type='reject' ORDER BY seq DESC LIMIT 1").bind(credential).fetch_one(store.pool()).await.unwrap().get::<Vec<u8>,_>("payload_json");
    let payload: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(
        payload["rejectReasonCode"],
        crate::account_identity::CONFLICT_CODE
    );
    a1_stop(server).await;
    cleanup(store, path).await;
}
