// Production gRPC/auth/protobuf -> ReconcileNow -> real HTTP adapter -> SQLite
// -> production ExecuteIntents/risk. Only the exchange HTTP boundary is local.
async fn terminal_remote_rpc_proof(algo: bool) {
    use crate::proto_v2::{ReconcileNowRequest, ReconcileNowResponse};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::codec::ProstCodec;
    use tonic::codegen::http::uri::PathAndQuery;
    use tonic::transport::Endpoint;

    let _serial = crate::binance_futures::tests::env_serial_guard();
    let credential = if algo {
        "terminal-rpc-algo"
    } else {
        "terminal-rpc-ordinary"
    };
    let Some(bound) = bind_owned_loopback(credential, "terminal-rpc-token").await else {
        return;
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("owned local exchange HTTP listener");
    let http_addr = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let observed_requests = requests.clone();
    let http_task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let requests = observed_requests.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0u8; 2048];
                    let read = socket.read(&mut chunk).await.unwrap();
                    assert!(read > 0, "HTTP request headers must arrive");
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
                assert_eq!(
                    method, "GET",
                    "ReconcileNow cannot dispatch exchange writes"
                );
                let body = match path {
                    "/fapi/v1/exchangeInfo" => serde_json::json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400}],"symbols":[]}),
                    "/fapi/v1/openOrders" if !algo => serde_json::json!([{"symbol":"ETHUSDT","orderId":6001,"clientOrderId":"terminal-rpc-client","status":"NEW","updateTime":1}]),
                    "/fapi/v1/openOrders" => serde_json::json!([]),
                    "/fapi/v1/openAlgoOrders" if algo => serde_json::json!([{"algoId":7001,"clientAlgoId":"terminal-rpc-algo-client","algoStatus":"NEW","updateTime":5}]),
                    "/fapi/v1/openAlgoOrders" => serde_json::json!([]),
                    "/fapi/v2/positionRisk" => serde_json::json!([{"symbol":"BTCUSDT","positionAmt":"2","entryPrice":"100","markPrice":"100","leverage":"1","unRealizedProfit":"0","liquidationPrice":"0","positionSide":"BOTH"}]),
                    "/fapi/v2/balance" => serde_json::json!([{"asset":"USDT","balance":"10000","availableBalance":"10000"}]),
                    other => panic!("unexpected exchange endpoint {other}"),
                }.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    let (key_env, secret_env) = crate::binance_futures::key_env_names(credential);
    let _key = EnvGuard::set(key_env, "local-terminal-key");
    let _secret = EnvGuard::set(secret_env, "local-terminal-secret");
    let _stage = EnvGuard::set("ENGINE_STAGE_MAX".to_string(), "demo");
    // The existing legacy URL seam exercises production HTTP/signing without
    // weakening venue validation or adding an alternate reconcile API.
    let _url = EnvGuard::set(
        "BINANCE_FUTURES_BASE_URL".to_string(),
        &format!("http://{http_addr}"),
    );
    let (store, path) = wiring_store(credential).await;
    sqlx::query("UPDATE engine_credential_health SET reconcile_diff_open=0 WHERE credential_id=?")
        .bind(credential)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,exit_epoch,state,active_key,qty,entry_price,updated_at_ms) VALUES ('terminal-rpc-live',?,'bot_seg3r1',?,'long',1,0,'open','terminal-rpc-live-key','2','100',1)")
        .bind(credential).bind(SYMBOL_KEY).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,client_order_id_short,exchange_order_id,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,status,order_role,position_id,created_at_ms,updated_at_ms) VALUES ('terminal-rpc-order',?,'terminal-rpc-old-bot','terminal-rpc-idem','terminal-rpc-client',?,'binance','futures','binance|futures|ETH|USDT|USDT','close_long','long','market','1',1,?,?,'terminal-rpc-closed',1,1)")
        .bind(credential).bind(if algo {None} else {Some("6001")}).bind(if algo {"unknown"} else {"failed"}).bind(if algo {"protection_sl"} else {"exit"}).execute(store.pool()).await.unwrap();
    if algo {
        sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES ('terminal-rpc-closed',?,'terminal-rpc-old-bot','binance|futures|ETH|USDT|USDT','long',1,'closed','0',1)")
            .bind(credential).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,client_algo_id,trigger_price,status,created_at_ms,updated_at_ms) VALUES ('terminal-rpc-po','terminal-rpc-closed',?,'sl',1,'terminal-rpc-order','terminal-rpc-algo-client','90','failed',1,1)")
            .bind(credential).execute(store.pool()).await.unwrap();
    }
    let submit = BinanceFuturesClient::new_with_mock_responses(
        credential,
        vec![(200, r#"{"orderId":8001,"clientOrderId":"coid_terminal_close","status":"NEW","symbol":"BTCUSDT","side":"SELL","type":"LIMIT","origQty":"2","executedQty":"0"}"#.to_string())],
    );
    let execution = crate::svc::EngineExecutionService::new(store.clone())
        .without_background_exchange_tasks()
        .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"))
        .with_submit_client_override(submit.clone());
    let events = crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800));
    let v2 = EngineExecutionV2Service::with_shared_execution(store.clone(), execution.clone());
    let server = bound.client();
    let (shutdown, receiver) = tokio::sync::oneshot::channel::<()>();
    let router = crate::add_engine_services(
        Server::builder(),
        execution,
        events,
        v2,
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
            .unwrap();
    });
    let channel = Endpoint::from_shared(format!("http://{}", server.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let make_request = || ReconcileNowRequest {
        credential_id: credential.to_string(),
        reason: "terminal conflict proof".to_string(),
        schema_version: 1,
    };
    let unauthorized: Result<tonic::Response<ReconcileNowResponse>, _> = client
        .unary(
            tonic::Request::new(make_request()),
            PathAndQuery::from_static("/execution_engine.v2.EngineExecution/ReconcileNow"),
            ProstCodec::<ReconcileNowRequest, ReconcileNowResponse>::default(),
        )
        .await;
    assert_eq!(
        unauthorized.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert!(requests.lock().unwrap().is_empty());
    client.ready().await.unwrap();
    let mut request = tonic::Request::new(make_request());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", server.identity_token).parse().unwrap(),
    );
    let truth = client
        .unary(
            request,
            PathAndQuery::from_static("/execution_engine.v2.EngineExecution/ReconcileNow"),
            ProstCodec::<ReconcileNowRequest, ReconcileNowResponse>::default(),
        )
        .await
        .expect("real authenticated ReconcileNow")
        .into_inner();
    assert!(
        truth.has_diff,
        "known terminal identity must remain a real conflict: {truth:?}"
    );
    let health: i64 = sqlx::query(
        "SELECT reconcile_diff_open FROM engine_credential_health WHERE credential_id=?",
    )
    .bind(credential)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get("reconcile_diff_open");
    assert_eq!(
        health, 1,
        "the RPC-produced conflict must persist into the risk consumer"
    );
    let payload: Vec<u8> = sqlx::query("SELECT payload_json FROM engine_order_events WHERE credential_id=? AND event_type='reconcile_diff' ORDER BY seq DESC LIMIT 1").bind(credential).fetch_one(store.pool()).await.unwrap().get("payload_json");
    let payload: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(
        payload["summary"][if algo {
            "terminalLocalRemoteOpenAlgoOrdersCount"
        } else {
            "terminalLocalRemoteOpenOrdersCount"
        }],
        1
    );
    assert_eq!(payload["summary"]["missingLocalOpenOrdersCount"], 0);
    assert_eq!(payload["summary"]["missingLocalOpenAlgoOrdersCount"], 0);
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .any(|path| path == "GET /fapi/v1/openAlgoOrders"));
    // Use the normal testnet execution contract for ExecuteIntents. HTTP is
    // already finished; its truth is consumed from the same durable store.
    let _venue = EnvGuard::set(crate::venue::venue_env_name(credential), "testnet");
    let _testnet_url = EnvGuard::set(
        "BINANCE_FUTURES_BASE_URL".to_string(),
        crate::venue::BINANCE_TESTNET_REST_BASE_URL,
    );
    let open = execute_intents_over_grpc(
        &server,
        Some(&server.identity_token),
        request_for(credential, open_intent("terminal_blocked")),
    )
    .await
    .unwrap();
    assert_eq!(
        open.acks[0].status,
        crate::proto_v2::Status::Rejected as i32
    );
    assert_eq!(
        open.acks[0].reject_reason_code, "DEGRADED_CREDENTIAL",
        "{:?}",
        open.acks[0]
    );
    assert!(submit.recorded_requests().is_empty());
    let mut close = open_intent("terminal_close");
    close.action = "close_long".to_string();
    close.reduce_only = true;
    close.protection = None;
    let close = execute_intents_over_grpc(
        &server,
        Some(&server.identity_token),
        request_for(credential, close),
    )
    .await
    .unwrap();
    assert_eq!(
        close.acks[0].status,
        crate::proto_v2::Status::Accepted as i32,
        "{:?}",
        close.acks[0]
    );
    assert_eq!(submit.recorded_requests().len(), 1);
    assert!(submit.recorded_requests()[0].contains("reduceOnly=true"));
    drop(client);
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), grpc_task)
        .await
        .expect("gRPC server shuts down")
        .unwrap();
    http_task.abort();
    cleanup(store, path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_remote_reconcile_now_ordinary_conflict_blocks_entry_but_allows_close() {
    terminal_remote_rpc_proof(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_remote_reconcile_now_algo_conflict_blocks_entry_but_allows_close() {
    terminal_remote_rpc_proof(true).await;
}
