mod dispatch_rpc_proof {
    use super::*;
    use crate::protection_dispatch::fence;
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    struct RetryClock {
        now: std::sync::atomic::AtomicI64,
        sleeping: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    impl crate::svc::Clock for RetryClock {
        fn now_ms(&self) -> i64 {
            self.now.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn sleep<'a>(
            &'a self,
            _: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.sleeping.notify_one();
                self.resume.notified().await;
                // A scheduler may resume later than the minimum Retry-After. This
                // bounded clock advance clears the independent 429 probe interval,
                // so the positive control really reaches a second physical POST.
                self.now
                    .fetch_add(6000, std::sync::atomic::Ordering::SeqCst);
            })
        }
    }
    struct Http {
        address: std::net::SocketAddr,
        log: Arc<Mutex<Vec<String>>>,
        task: Option<tokio::task::JoinHandle<()>>,
        stop: Arc<std::sync::atomic::AtomicBool>,
        ca_path: PathBuf,
        tls_dir: PathBuf,
    }
    impl Drop for Http {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    async fn http(label: &str) -> Http {
        http_with_ack_hold(label, None).await
    }
    async fn http_with_ack_hold(label: &str, ack_hold: Option<Arc<K05AckHold>>) -> Http {
        let missing_confirmation = label == "k05_b_missing";
        let retry = label.starts_with("retry");
        let lose_response = label == "lost_response";
        let tls_dir = std::env::temp_dir().join(format!("dispatch-tls-{}", ulid::Ulid::new()));
        std::fs::create_dir(&tls_dir).unwrap();
        let ca_path = tls_dir.join("loopback.pem");
        let key_path = tls_dir.join("loopback.key");
        let generated = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=dispatch-loopback",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-addext",
                "extendedKeyUsage=serverAuth",
                "-keyout",
            ])
            .arg(&key_path)
            .arg("-out")
            .arg(&ca_path)
            .output()
            .unwrap();
        assert!(
            generated.status.success(),
            "generate isolated test TLS certificate"
        );
        let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(&ca_path).unwrap(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
            std::fs::File::open(&key_path).unwrap(),
        ))
        .unwrap()
        .unwrap();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let observed = log.clone();
        let posts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let task = tokio::task::spawn_blocking(move || {
            while !stopping.load(std::sync::atomic::Ordering::SeqCst) {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(e) => panic!("TLS listener: {e}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut socket = rustls::StreamOwned::new(
                    rustls::ServerConnection::new(config.clone()).unwrap(),
                    socket,
                );
                let observed = observed.clone();
                let posts = posts.clone();
                let ack_hold = ack_hold.clone();
                std::thread::spawn(move || {
                    let mut bytes = Vec::new();
                    let mut buf = [0; 2048];
                    loop {
                        let n = socket.read(&mut buf).unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&buf[..n]);
                        if bytes.windows(4).any(|x| x == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let raw = String::from_utf8(bytes).unwrap();
                    let first = raw.lines().next().unwrap();
                    observed.lock().unwrap().push(first.into());
                    let mut words = first.split_whitespace();
                    let method = words.next().unwrap();
                    let url =
                        url::Url::parse(&format!("http://localhost{}", words.next().unwrap()))
                            .unwrap();
                    let q = url
                        .query_pairs()
                        .collect::<std::collections::HashMap<_, _>>();
                    if lose_response && method == "POST" && url.path() == "/fapi/v1/algoOrder" {
                        return;
                    }
                    if let Some(hold) = &ack_hold {
                        if url.path() == "/fapi/v1/algoOrder" {
                            if method == "POST" { *hold.client_id.lock().unwrap() = Some(q["clientAlgoId"].to_string()); hold.wait(false); }
                            if method == "GET" { hold.wait(true); }
                        }
                    }
                    let mut status = 200;
                    let mut extra = "";
                    let body=match(method,url.path()){
                    ("POST","/fapi/v1/algoOrder")=>{let n=posts.fetch_add(1,std::sync::atomic::Ordering::SeqCst);if retry&&n==0 {status=429;extra="Retry-After: 0\r\n";serde_json::json!({"code":-1003})}else{serde_json::json!({"algoId":800+n,"clientAlgoId":q["clientAlgoId"],"algoStatus":"NEW","symbol":"BTCUSDT","actualOrderId":if ack_hold.is_some(){Some("88001")}else{None}})}},
                    ("GET","/fapi/v1/algoOrder")=>if missing_confirmation {status=400;serde_json::json!({"code":-2013,"msg":"Order does not exist"})}else{serde_json::json!({"algoId":800,"clientAlgoId":ack_hold.as_ref().and_then(|h|h.client_id.lock().unwrap().clone()).or_else(||q.get("clientAlgoId").map(|v|v.to_string())),"algoStatus":"NEW","symbol":"BTCUSDT","updateTime":crate::svc::now_ms()})},
                    ("GET","/fapi/v1/openOrders"|"/fapi/v1/openAlgoOrders"|"/fapi/v2/positionRisk")=>serde_json::json!([]),
                    ("GET","/fapi/v2/balance")=>serde_json::json!([{"asset":"USDT","balance":"10000","availableBalance":"10000"}]),
                    ("POST","/fapi/v1/order")=>{assert_eq!(q["reduceOnly"],"true");serde_json::json!({"orderId":991,"clientOrderId":q["newClientOrderId"],"status":"FILLED","symbol":"BTCUSDT","origQty":q["quantity"],"executedQty":q["quantity"]})},
                    ("DELETE",_)=>serde_json::json!({}),
                    _=>panic!("unexpected dispatch HTTP {first}"),
                }.to_string();
                    socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).unwrap();
                });
            }
        });
        Http {
            address,
            log,
            task: Some(task),
            stop,
            ca_path,
            tls_dir,
        }
    }
    fn request(credential: &str, label: &str) -> crate::proto::ExecuteIntentsRequest {
        crate::proto::ExecuteIntentsRequest {
            credential_id: credential.into(),
            run_mode: "demo".into(),
            intents: vec![crate::proto::OrderIntent {
                bot_uid: "bot_r10v2".into(),
                run_id: "run_dispatch".into(),
                cycle_id: format!("cycle_{label}"),
                idempotency_key: format!("idem_{label}"),
                client_order_id_short: format!("dispatch_{label}"),
                exchange: "binance".into(),
                market_kind: "futures".into(),
                symbol_key: SYMBOL_KEY.into(),
                action: "close_long".into(),
                side: "long".into(),
                order_type: if label == "positive_tp" {
                    "take_profit_market"
                } else {
                    "stop_market"
                }
                .into(),
                price: if label == "positive_tp" { "110" } else { "90" }.into(),
                quantity: "2".into(),
                reduce_only: true,
                max_position_notional: 0.,
                max_credential_notional: 0.,
            }],
        }
    }
    async fn rpc(
        bound: BoundLoopbackClient,
        request: crate::proto::ExecuteIntentsRequest,
        authenticated: bool,
    ) -> Result<crate::proto::ExecuteIntentsResponse, tonic::Status> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", bound.addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let mut request = tonic::Request::new(request);
        if authenticated {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", bound.identity_token).parse().unwrap(),
            );
        }
        client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/execution_engine.vnext.EngineExecution/ExecuteIntents",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .map(|r: tonic::Response<crate::proto::ExecuteIntentsResponse>| r.into_inner())
    }
    async fn attempt(store: &Store) -> (String, i64) {
        tokio::time::timeout(Duration::from_secs(5),async{loop{
            if let Some(r)=sqlx::query("SELECT attempt_id,CAST(revision AS CHAR) AS rev FROM protection_dispatch_attempts").fetch_all(store.pool()).await.unwrap().into_iter().next(){return(r.get("attempt_id"),crate::store::try_get_i64_text(&r,"rev").unwrap());}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }}).await.expect("production T0 must create a managed attempt")
    }
    #[tokio::test]
    async fn dispatch_v1_authenticated_rpc_reaches_real_http_and_blocks_waiting_sends() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        for label in [
            "positive_sl",
            "positive_tp",
            "positive_lifecycle",
            "closed_wait",
            "fence_wait",
            "identity_wait",
            "sql_wait",
            "ack_rollback",
            "retry_allow",
            "retry_fence",
            "prepare_rollback",
            "paper",
            "lost_response",
            "pre_cancel_active",
            "pre_cancel_invalid_active",
            "pre_cancel_closed",
        ] {
            eprintln!("DISPATCH_RPC_SCENARIO_START={label}");
            let mut http = http(label).await;
            let credential = if label == "paper" {
                "paper".to_owned()
            } else if label == "positive_lifecycle" {
                "605".to_owned()
            } else {
                format!("dispatch-rpc-{label}")
            };
            let mut env = exchange_credential_env(&credential);
            env.push(EnvGuard::set("BINANCE_FUTURES_BASE_URL".into(), ""));
            env.push(EnvGuard::set(
                "BINANCE_FAULT_PROXY_REST_BASE_URL".into(),
                &format!("https://{}", http.address),
            ));
            env.push(EnvGuard::set(
                "BINANCE_FAULT_PROXY_WS_BASE_URL".into(),
                &format!("wss://{}", http.address),
            ));
            env.push(EnvGuard::set(
                "BINANCE_FAULT_PROXY_CA_PEM_PATH".into(),
                http.ca_path.to_str().unwrap(),
            ));
            env.push(EnvGuard::set("ENGINE_LIVE_ENABLED".into(), "false"));
            env.push(EnvGuard::set("ENGINE_STAGE_MAX".into(), "demo"));
            env.push(EnvGuard::set(
                "PROTECTION_CONFIRM_TIMEOUT_MS".into(),
                "60000",
            ));
            let (store, path) = wiring_store(&credential).await;
            if label == "pre_cancel_invalid_active" {
                sqlx::query(
                    "INSERT INTO kill_switch_state (scope_key, level, reason, trigger_source, \
                     armed_at_ms, flatten_state, last_reconcile_clean, last_reconcile_at_ms, \
                     updated_at_ms, flatten_generation) VALUES ('bot:bot_r10v2', \
                     ' FLATTEN ', 'invalid exact-scope fixture', 'test_fixture', 1, \
                     'flattened', 1, 1, 1, 7)",
                )
                .execute(store.pool())
                .await
                .expect("seed exact bot-scoped invalid safety authority");
            }
            let retry_clock = Arc::new(RetryClock {
                now: std::sync::atomic::AtomicI64::new(crate::svc::now_ms()),
                sleeping: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            });
            let clock: Arc<dyn crate::svc::Clock> = if label.starts_with("retry") {
                retry_clock.clone()
            } else {
                Arc::new(crate::svc::SystemClock)
            };
            let exchange = if label == "paper" {
                None
            } else {
                Some(
                    BinanceFuturesClient::new_with_clock(
                        crate::binance_futures::keys_from_env(&credential)
                            .unwrap()
                            .unwrap(),
                        &credential,
                        store.clone(),
                        clock,
                    )
                    .unwrap(),
                )
            };
            let waiting = label.ends_with("wait");
            let held = if waiting {
                Some(
                    crate::binance_futures::transport_gate_for_scope(
                        &exchange.as_ref().unwrap().egress_scope(),
                    )
                    .lock_owned()
                    .await,
                )
            } else {
                None
            };
            if label.starts_with("pre_cancel") {
                let close = if label == "pre_cancel_closed" {
                    "UPDATE position_state SET state='closed',qty='0',active_key=NULL WHERE position_id='pos_r10v2';"
                } else {
                    ""
                };
                sqlx::query(&format!("CREATE TRIGGER stop_before_order_cas AFTER INSERT ON protection_dispatch_attempts BEGIN UPDATE engine_orders SET status='canceled' WHERE engine_order_id=NEW.engine_order_id; {close} END")).execute(store.pool()).await.unwrap();
            }
            if label == "prepare_rollback" {
                sqlx::query("CREATE TRIGGER block_dispatch_prepare BEFORE INSERT ON protection_dispatch_attempts BEGIN SELECT RAISE(FAIL,'reject prepared transaction'); END").execute(store.pool()).await.unwrap();
            }
            if label == "ack_rollback" {
                sqlx::query("CREATE TRIGGER block_ack_event BEFORE INSERT ON engine_order_events WHEN NEW.event_type='order_ack' BEGIN SELECT RAISE(FAIL,'reject ack transaction'); END").execute(store.pool()).await.unwrap();
            }
            let exec = crate::svc::EngineExecutionService::new(store.clone())
                .without_background_exchange_tasks()
                .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"));
            let exec = if let Some(exchange) = exchange {
                exec.with_submit_client_override(exchange)
            } else {
                exec
            };
            let bound = bind_owned_loopback(label, "dispatch-rpc-auth")
                .await
                .expect("owned gRPC listener required");
            let client = bound.client();
            let router = crate::add_engine_services(
                Server::builder(),
                exec.clone(),
                crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
                EngineExecutionV2Service::with_shared_execution(store.clone(), exec),
                super::super::EngineEventsV2Skeleton,
                Some(crate::AuthInterceptor::new(vec![bound
                    .identity_token
                    .clone()])),
            );
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                router
                    .serve_with_incoming_shutdown(bound.incoming, async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
            if label == "positive_lifecycle" {
                // Same authenticated server, a durable lifecycle fence first;
                // real managed Algo protection must remain reachable afterwards.
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", client.addr)).unwrap().connect().await.unwrap();
                let mut grpc = tonic::client::Grpc::new(channel);
                grpc.ready().await.unwrap();
                let mut arm = tonic::Request::new(crate::lifecycle_fence_tests::arm_request(&store, &credential, "protection_account"));
                arm.metadata_mut().insert("authorization", format!("Bearer {}",client.identity_token).parse().unwrap());
                let response: tonic::Response<crate::proto_v2::LifecycleFenceResponse> = grpc.unary(arm,
                    tonic::codegen::http::uri::PathAndQuery::from_static(crate::lifecycle_fence_tests::ARM), tonic::codec::ProstCodec::default()).await.unwrap();
                assert!(response.into_inner().current_observation.unwrap().admission_blocked);
            }
            let mut req = request(&credential, label);
            if label == "paper" {
                req.run_mode = "paper".into();
            }
            assert_eq!(
                rpc(client.clone(), req.clone(), false)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
            let running = tokio::spawn(rpc(client.clone(), req.clone(), true));
            if label.starts_with("retry") {
                tokio::time::timeout(Duration::from_secs(5), retry_clock.sleeping.notified())
                    .await
                    .expect("actual 429 must enter retry sleep");
                let (id, rev) = attempt(&store).await;
                if label == "retry_fence" {
                    assert!(fence(&store, &id, rev).await.unwrap());
                }
                retry_clock.resume.notify_one();
            }
            if waiting {
                let (id, rev) = attempt(&store).await;
                match label {
                    "closed_wait" => {
                        sqlx::query("UPDATE position_state SET state='closed',qty='0',active_key=NULL WHERE position_id='pos_r10v2'").execute(store.pool()).await.unwrap();
                    }
                    "fence_wait" => {
                        assert!(fence(&store, &id, rev).await.unwrap());
                    }
                    "identity_wait" => {
                        sqlx::query("UPDATE protection_orders SET client_algo_id='foreign' WHERE engine_order_id IS NOT NULL").execute(store.pool()).await.unwrap();
                    }
                    "sql_wait" => {
                        sqlx::query("CREATE TRIGGER fail_actual_arm BEFORE UPDATE OF authorized_post_count ON protection_dispatch_attempts BEGIN SELECT RAISE(FAIL,'actual arm SQL failure'); END").execute(store.pool()).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            drop(held);
            let response = tokio::time::timeout(Duration::from_secs(20), running)
                .await
                .unwrap()
                .unwrap();
            if label == "lost_response" {
                let rendered = format!("{response:?}");
                for forbidden in ["signature=", "r10-v2-secret", "https://127.0.0.1"] {
                    assert!(
                        !rendered.contains(forbidden),
                        "transport diagnostic leaked {forbidden}"
                    );
                }
                let events = sqlx::query(
                    "SELECT payload_json FROM engine_order_events WHERE event_type='error'",
                )
                .fetch_all(store.pool())
                .await
                .unwrap();
                assert!(
                    !events.is_empty(),
                    "unknown transport outcome must emit an error fact"
                );
                for event in events {
                    let bytes = event.get::<Vec<u8>, _>("payload_json");
                    let text = String::from_utf8(bytes).unwrap();
                    for forbidden in ["signature=", "r10-v2-secret", "https://127.0.0.1"] {
                        assert!(
                            !text.contains(forbidden),
                            "persisted diagnostic leaked {forbidden}"
                        );
                    }
                }
            }
            eprintln!("DISPATCH_RPC_SCENARIO_RESPONSE={label}: {response:?}");
            if label == "prepare_rollback" || label == "paper" {
                let (orders, protections) = if label == "prepare_rollback" {
                    assert!(response.is_err());
                    (0, 0)
                } else {
                    assert!(response.is_ok());
                    (1, 1)
                };
                for (table, expected) in [
                    ("engine_orders", orders),
                    ("protection_orders", protections),
                    ("protection_dispatch_attempts", 0),
                ] {
                    let n = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                        .fetch_one(store.pool())
                        .await
                        .unwrap()
                        .get::<i64, _>("n");
                    assert_eq!(n, expected, "{label}: {table}");
                }
                assert!(http.log.lock().unwrap().is_empty());
                let _ = stop.send(());
                server.await.unwrap();
                http.stop.store(true, std::sync::atomic::Ordering::SeqCst);
                http.task.take().unwrap().await.unwrap();
                std::fs::remove_dir_all(&http.tls_dir).unwrap();
                cleanup(store, path).await;
                drop(env);
                continue;
            }
            let row=sqlx::query("SELECT phase,CAST(authorized_post_count AS CHAR) AS n,sender_finished,dispatch_closed,outcome FROM protection_dispatch_attempts").fetch_one(store.pool()).await.unwrap();
            let posts = http
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.starts_with("POST /fapi/v1/algoOrder?"))
                .count();
            if label.starts_with("positive") || label == "retry_allow" {
                let expected_posts = if label == "retry_allow" { 2 } else { 1 };
                assert_eq!(
                    response.as_ref().unwrap().acks[0].status,
                    "accepted",
                    "{label}: {response:?}"
                );
                assert_eq!(posts, expected_posts);
                assert_eq!(
                    crate::store::try_get_i64_text(&row, "n").unwrap(),
                    expected_posts as i64
                );
                assert_eq!(row.get::<i64, _>("sender_finished"), 1);
                let duplicate = rpc(client.clone(), req, true).await.unwrap_err();
                assert_eq!(duplicate.code(), tonic::Code::Internal);
                assert!(duplicate
                    .message()
                    .contains("risk_verdicts.approved_intent_key"));
                for table in [
                    "engine_orders",
                    "protection_orders",
                    "protection_dispatch_attempts",
                ] {
                    assert_eq!(
                        sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                            .fetch_one(store.pool())
                            .await
                            .unwrap()
                            .get::<i64, _>("n"),
                        1,
                        "duplicate {table}"
                    );
                }
                assert_eq!(
                    http.log
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|r| r.starts_with("POST /fapi/v1/algoOrder?"))
                        .count(),
                    expected_posts
                );
            } else if label == "retry_fence" || label == "lost_response" {
                assert_eq!(posts, 1);
                assert_eq!(crate::store::try_get_i64_text(&row, "n").unwrap(), 1);
                assert_eq!(row.get::<i64, _>("sender_finished"), 1);
                assert_eq!(
                    crate::store::try_get_text(&row, "outcome")
                        .unwrap()
                        .as_deref(),
                    Some("may_have_sent")
                );
                assert_eq!(response.as_ref().unwrap().acks[0].status, "error");
            } else if label == "ack_rollback" {
                assert!(response.is_err());
                assert_eq!(posts, 1);
                assert_eq!(row.get::<i64, _>("sender_finished"), 0);
                assert_eq!(row.get::<String, _>("phase"), "armed");
                let po = sqlx::query("SELECT exchange_algo_id,status FROM protection_orders")
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
                assert!(
                    crate::store::try_get_nullable_string(&po, "exchange_algo_id")
                        .unwrap()
                        .is_none()
                );
                assert_eq!(po.get::<String, _>("status"), "requested");
            } else {
                assert_eq!(posts, 0, "{label}");
                assert_eq!(crate::store::try_get_i64_text(&row, "n").unwrap(), 0);
                assert_eq!(row.get::<i64, _>("sender_finished"), 1);
                assert_eq!(row.get::<i64, _>("dispatch_closed"), 1);
                if label.starts_with("pre_cancel") {
                    assert_eq!(
                        crate::store::try_get_text(&row, "outcome")
                            .unwrap()
                            .as_deref(),
                        Some("not_dispatched"),
                        "{label}: an unpolled submit must close with exact outcome"
                    );
                }
                assert_eq!(
                    response.as_ref().unwrap().acks[0].status,
                    if label == "closed_wait" || label == "pre_cancel_closed" {
                        "rejected"
                    } else {
                        "error"
                    },
                    "{label}: {response:?}"
                );
                if label.starts_with("pre_cancel") {
                    let response = response
                        .as_ref()
                        .expect("ownership-blocked RPC returns one explicit ack");
                    assert_eq!(
                        response.acks.len(),
                        1,
                        "{label}: blocked intent must produce exactly one ack"
                    );
                    let ack = &response.acks[0];
                    assert_eq!(
                        sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?",)
                            .bind(&ack.engine_order_id)
                            .fetch_one(store.pool())
                            .await
                            .unwrap()
                            .get::<String, _>("status"),
                        "canceled",
                        "{label}: ownership denial must preserve the pre-existing terminal status"
                    );
                    assert!(
                        !ack.message.to_ascii_lowercase().contains("accepted"),
                        "{label}: blocked ack must not imply acceptance: {}",
                        ack.message
                    );
                    assert!(
                        ack.message.contains("authoritative_idempotency_key="),
                        "{label}: blocked ack must identify the existing authority: {}",
                        ack.message
                    );
                    if label == "pre_cancel_closed" {
                        assert!(
                            ack.message.contains("protection submission not started"),
                            "{label}: closed protection must report that execution was not repeated: {}",
                            ack.message
                        );
                    } else {
                        assert!(
                            ack.message.starts_with("ORDER_SUBMIT_OWNERSHIP_BLOCKED:"),
                            "{label}: active ownership denial must expose the stable error code: {}",
                            ack.message
                        );
                        assert!(
                            ack.message.contains("exchange submit not polled"),
                            "{label}: active protection must report that exchange submission was not repeated: {}",
                            ack.message
                        );
                    }
                    assert_eq!(
                        sqlx::query(
                            "SELECT COUNT(*) AS n FROM engine_order_events \
                             WHERE event_type='order_submit_ownership_blocked'",
                        )
                        .fetch_one(store.pool())
                        .await
                        .unwrap()
                        .get::<i64, _>("n"),
                        1,
                        "{label}: ownership denial must emit exactly one audit event"
                    );
                    let event = sqlx::query(
                        "SELECT payload_json FROM engine_order_events \
                         WHERE event_type='order_submit_ownership_blocked' \
                         ORDER BY seq DESC LIMIT 1",
                    )
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
                    let payload: serde_json::Value =
                        serde_json::from_slice(&event.get::<Vec<u8>, _>("payload_json")).unwrap();
                    assert_eq!(payload["errorCode"], "ORDER_SUBMIT_OWNERSHIP_BLOCKED");
                    assert_eq!(payload["expectedStatus"], "pending_risk");
                    assert_eq!(payload["observedStatusRaw"], "canceled");
                    assert_eq!(payload["exchangeSubmitPolled"], false);
                    assert_eq!(payload["orderRole"], "protection_sl");
                    if label == "pre_cancel_active" || label == "pre_cancel_invalid_active" {
                        assert_eq!(payload["protectionFallbackRequired"], true);
                        assert_eq!(payload["safetyAuthorityScopeKey"], "bot:bot_r10v2");
                        if label == "pre_cancel_invalid_active" {
                            assert_eq!(
                                payload["safetyAuthorityKind"],
                                "preserved_invalid_conservative_flatten"
                            );
                            assert_eq!(payload["safetyAuthorityRawLevel"], " FLATTEN ");
                            let latch = sqlx::query(
                                "SELECT level, flatten_state, trigger_source, \
                                 CAST(flatten_generation AS CHAR) AS generation_text \
                                 FROM kill_switch_state WHERE scope_key='bot:bot_r10v2'",
                            )
                            .fetch_one(store.pool())
                            .await
                            .unwrap();
                            assert_eq!(
                                latch.get::<String, _>("level"),
                                " FLATTEN ",
                                "typed-invalid authority must remain byte-for-byte intact"
                            );
                            assert_eq!(latch.get::<String, _>("flatten_state"), "flattened");
                            assert_eq!(latch.get::<String, _>("trigger_source"), "test_fixture");
                            assert_eq!(
                                crate::store::try_get_i64_text(&latch, "generation_text").unwrap(),
                                7,
                                "preserving invalid authority must not forge a new generation"
                            );
                        } else {
                            assert_eq!(payload["safetyAuthorityKind"], "canonical_flatten");
                            assert!(payload["safetyAuthorityRawLevel"].is_null());
                            let latch = sqlx::query(
                                "SELECT level, flatten_state, trigger_source \
                                 FROM kill_switch_state WHERE scope_key='bot:bot_r10v2'",
                            )
                            .fetch_one(store.pool())
                            .await
                            .unwrap();
                            assert_eq!(latch.get::<String, _>("level"), "flatten");
                            assert_eq!(latch.get::<String, _>("flatten_state"), "flattening");
                            assert_eq!(
                                latch.get::<String, _>("trigger_source"),
                                "auto_protection_timeout"
                            );
                        }
                        assert_eq!(
                            sqlx::query("SELECT status FROM protection_orders")
                                .fetch_one(store.pool())
                                .await
                                .unwrap()
                                .get::<String, _>("status"),
                            "failed"
                        );
                        assert_eq!(
                            http.log
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|request| request.starts_with("POST /fapi/v1/order?"))
                                .count(),
                            1,
                            "the latched fallback should make one immediate reduce-only attempt"
                        );
                    } else {
                        assert_eq!(payload["protectionFallbackRequired"], false);
                        assert!(payload["safetyAuthorityKind"].is_null());
                        assert!(payload["safetyAuthorityScopeKey"].is_null());
                        assert!(payload["safetyAuthorityRawLevel"].is_null());
                        assert_eq!(
                            http.log
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|request| request.starts_with("POST /fapi/v1/order?"))
                                .count(),
                            0,
                            "closed+zero must not dispatch a flatten"
                        );
                        for table in ["kill_switch_state", "kill_switch_events"] {
                            assert_eq!(
                                sqlx::query(&format!(
                                    "SELECT COUNT(*) AS n FROM {table} \
                                     WHERE scope_key='bot:bot_r10v2'"
                                ))
                                .fetch_one(store.pool())
                                .await
                                .unwrap()
                                .get::<i64, _>("n"),
                                0,
                                "closed+zero must not create bot safety authority in {table}"
                            );
                        }
                    }
                }
                if label == "closed_wait" || label == "pre_cancel_closed" {
                    let event=sqlx::query("SELECT payload_json FROM engine_order_events WHERE event_type='reject' ORDER BY seq DESC LIMIT 1").fetch_one(store.pool()).await.unwrap();
                    let payload: serde_json::Value =
                        serde_json::from_slice(&event.get::<Vec<u8>, _>("payload_json")).unwrap();
                    assert_eq!(
                        payload["rejectReasonCode"],
                        "PROTECTION_DISPATCH_NOT_STARTED"
                    );
                    if label == "closed_wait" {
                        assert_eq!(
                            sqlx::query("SELECT status FROM engine_orders")
                                .fetch_one(store.pool())
                                .await
                                .unwrap()
                                .get::<String, _>("status"),
                            "rejected"
                        );
                    }
                }
                if label != "closed_wait"
                    && label != "pre_cancel_closed"
                    && label != "pre_cancel_invalid_active"
                {
                    assert!(
                        sqlx::query("SELECT COUNT(*) AS n FROM kill_switch_events")
                            .fetch_one(store.pool())
                            .await
                            .unwrap()
                            .get::<i64, _>("n")
                            > 0,
                        "{label}: active SL failure must retain safety action"
                    );
                }
            }
            let _ = stop.send(());
            server.await.unwrap();
            http.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            http.task.take().unwrap().await.unwrap();
            std::fs::remove_dir_all(&http.tls_dir).unwrap();
            cleanup(store, path).await;
            drop(env);
        }
    }
    include!("svc_v2_k05_b_ack_tests.rs");

}
