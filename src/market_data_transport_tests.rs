mod transport_tests {
    use super::*;
    struct TransportEnv(Vec<(String, Option<std::ffi::OsString>)>);

    impl TransportEnv {
        fn remove(&mut self, name: String) {
            self.0.push((name.clone(), std::env::var_os(&name)));
            std::env::remove_var(name);
        }

        fn set(&mut self, name: impl Into<String>, value: &str) {
            let name = name.into();
            self.0.push((name.clone(), std::env::var_os(&name)));
            std::env::set_var(name, value);
        }
    }

    impl Drop for TransportEnv {
        fn drop(&mut self) {
            for (name, old) in self.0.drain(..).rev() {
                match old {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    const PAPER_TRANSPORT_CRED: &str = "paper:0123456789abcdef0123456789abcdef";
    const EXCHANGE_TRANSPORT_CRED: &str = "wom7-exchange";

    fn transport_env(proxy: &crate::k04_fake_venue::TlsFakeVenue) -> TransportEnv {
        let mut env = TransportEnv(Vec::new());
        env.set("BINANCE_FAULT_PROXY_REST_BASE_URL", &proxy.rest_base_url());
        env.set("BINANCE_FAULT_PROXY_WS_BASE_URL", &proxy.ws_base_url());
        env.set(
            "BINANCE_FAULT_PROXY_CA_PEM_PATH",
            proxy.ca_path.to_str().unwrap(),
        );
        env.set("ENGINE_LIVE_ENABLED", "false");
        env.set("ENGINE_STAGE_MAX", "demo");
        env.set(
            crate::venue::venue_env_name(EXCHANGE_TRANSPORT_CRED),
            "testnet",
        );
        env.remove(crate::venue::venue_env_name(PAPER_TRANSPORT_CRED));
        env
    }

    async fn stop_transport_workers(manager: &MarketDataManager) {
        for (_, task) in manager.inner.lock().await.drain() {
            task.abort();
            let _ = task.await;
        }
    }

    /// WO-M7：鉴权 gRPC EnsureMarketData 起两条真实 WS；bar 与心跳来自各自夹具
    /// 发出的同一冻结帧。TLS 代理与 paper listener 的请求分别断言，不能靠
    /// transport helper 的返回值冒充并存接线。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m7_paper_and_exchange_kline_coexist_through_production_market_data() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let proxy = crate::k04_fake_venue::TlsFakeVenue::start();
        let mut env = transport_env(&proxy);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        env.set(
            "BINANCE_FUTURES_WS_BASE_URL",
            &format!("ws://{}", listener.local_addr().unwrap()),
        );
        crate::venue::validate_fault_proxy_startup_config().expect("mixed startup is valid");
        let close_ms = crate::svc::now_ms() - 1_000;
        let frame = closed_kline_frame("BTCUSDT", "1m", close_ms - 60_000, close_ms);
        proxy.with_book(|book| book.kline_frame = Some(frame.clone()));
        let (path_tx, mut path_rx) = tokio::sync::oneshot::channel();
        let paper_server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut observed_path = String::new();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                socket,
                |req: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    observed_path = req.uri().path().to_string();
                    Ok(response)
                },
            )
            .await
            .unwrap();
            path_tx.send(observed_path).unwrap();
            ws.send(Message::Text(frame)).await.unwrap();
            let _ = ws.close(None).await;
        });
        let (store, path) = test_store().await;
        let engine = crate::position_close_exchange_tests::Engine::start(store.clone()).await;
        let manager = engine.market_data.clone();
        for credential in [PAPER_TRANSPORT_CRED, EXCHANGE_TRANSPORT_CRED] {
            let response: crate::proto::EnsureMarketDataResponse = engine
                .rpc(
                    "/execution_engine.vnext.EngineExecution/EnsureMarketData",
                    crate::proto::EnsureMarketDataRequest {
                        credential_id: credential.into(),
                        subscriptions: vec![crate::proto::MarketDataSubscription {
                            exchange: "binance".into(),
                            market_kind: "futures".into(),
                            symbol_key: BTC_KEY.into(),
                            timeframe: "1m".into(),
                        }],
                    },
                )
                .await
                .expect("authenticated production EnsureMarketData");
            assert!(response.accepted, "{}", response.message);
            assert_eq!(response.started, 1);
        }
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let rows = sqlx::query("SELECT credential_id, CAST(last_bar_ms AS TEXT) AS ts FROM market_stream_health ORDER BY credential_id")
                    .fetch_all(store.pool()).await.unwrap();
                if rows.len() == 2 {
                    return rows.into_iter().map(|row| (row.get::<String, _>("credential_id"), row.get::<String, _>("ts"))).collect::<Vec<_>>();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await;
        stop_transport_workers(&manager).await;
        paper_server.abort();
        let _ = engine.stop().await;
        let bars = sqlx::query("SELECT credential_id FROM engine_order_events WHERE event_type='bar_closed' ORDER BY credential_id")
            .fetch_all(store.pool()).await.unwrap();
        let bar_credentials = bars
            .into_iter()
            .map(|row| row.get::<String, _>("credential_id"))
            .collect::<Vec<_>>();
        cleanup(store, path).await;
        assert_eq!(
            observed.unwrap_or_else(|error| panic!("both credentials must receive durable heartbeats: {error}; observed bar credentials: {bar_credentials:?}")),
            vec![
                (PAPER_TRANSPORT_CRED.into(), close_ms.to_string()),
                (EXCHANGE_TRANSPORT_CRED.into(), close_ms.to_string()),
            ]
        );
        assert_eq!(path_rx.try_recv().unwrap(), "/ws/btcusdt@kline_1m");
        assert!(proxy
            .requests()
            .iter()
            .any(|request| request.is("GET", "/ws/btcusdt@kline_1m")));
        assert_eq!(
            bar_credentials,
            vec![PAPER_TRANSPORT_CRED, EXCHANGE_TRANSPORT_CRED]
        );
        // 启动守卫仍拒 mainnet / live / 不完整端点。
        env.set(
            crate::venue::venue_env_name(EXCHANGE_TRANSPORT_CRED),
            "mainnet",
        );
        assert_eq!(
            crate::venue::validate_fault_proxy_startup_config().unwrap_err(),
            "fault-proxy transport requires an explicit testnet credential venue"
        );
        env.set(
            crate::venue::venue_env_name(EXCHANGE_TRANSPORT_CRED),
            "testnet",
        );
        env.set("ENGINE_LIVE_ENABLED", "true");
        assert_eq!(
            crate::venue::validate_fault_proxy_startup_config().unwrap_err(),
            "fault-proxy transport is forbidden while ENGINE_LIVE_ENABLED is true"
        );
        env.set("ENGINE_LIVE_ENABLED", "false");
        env.set("BINANCE_FAULT_PROXY_WS_BASE_URL", "");
        assert_eq!(
            crate::venue::validate_fault_proxy_startup_config().unwrap_err(),
            "fault-proxy transport requires both REST and WebSocket endpoints"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn m7_simulation_exchange_venue_and_unknown_identity_remain_refused() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let proxy = crate::k04_fake_venue::TlsFakeVenue::start();
        let mut env = transport_env(&proxy);
        env.set("BINANCE_FUTURES_WS_BASE_URL", "ws://127.0.0.1:9");
        let expected = "fault-proxy transport requires an explicit testnet credential venue";
        for venue in ["testnet", "mainnet"] {
            env.set(crate::venue::venue_env_name(PAPER_TRANSPORT_CRED), venue);
            assert_eq!(
                crate::venue::ws_transport_for_credential(PAPER_TRANSPORT_CRED)
                    .err()
                    .expect("simulation misconfiguration refused"),
                expected
            );
        }
        env.remove(crate::venue::venue_env_name(PAPER_TRANSPORT_CRED));
        for unknown in ["wom7-unconfigured", "paper:invalid"] {
            assert_eq!(
                crate::venue::ws_transport_for_credential(unknown)
                    .err()
                    .expect("unknown credential refused"),
                expected
            );
            let (store, path) = test_store().await;
            let manager = MarketDataManager::new();
            assert!(
                manager
                    .ensure_kline_running(unknown, store.clone(), BTC_KEY, "1m")
                    .await
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            stop_transport_workers(&manager).await;
            assert_eq!(bar_event_count(&store).await, 0);
            assert!(heartbeat_rows(&store).await.is_empty());
            cleanup(store, path).await;
        }
        assert!(proxy.requests().is_empty());
    }

    fn quote_frame(timestamp: i64) -> serde_json::Value {
        serde_json::json!({"e":"offlineMarkQuote","source_fixture":"OFFLINE_MARKET_FIXTURE","run_mode":"paper","credential_id":PAPER_TRANSPORT_CRED,"symbol_key":BTC_KEY,"symbol":"BTCUSDT","price":"60001.25","source_timestamp_ms":timestamp,"schema_version":1})
    }

    async fn quote_event_count(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='simulation_mark_quote'").fetch_one(store.pool()).await.unwrap().get("n")
    }

    async fn quote_account(
        engine: &crate::position_close_exchange_tests::Engine,
    ) -> crate::proto_v2::GetAccountStateResponse {
        engine
            .rpc(
                "/execution_engine.v2.EngineExecution/GetAccountState",
                crate::proto_v2::GetAccountStateRequest {
                    credential_id: PAPER_TRANSPORT_CRED.into(),
                    bot_uid: String::new(),
                    schema_version: 1,
                },
            )
            .await
            .unwrap()
    }

    async fn funded_quote_position(
        store: &Store,
        engine: &crate::position_close_exchange_tests::Engine,
    ) {
        let funded: crate::proto_v2::FundSimulationAccountResponse = engine
            .rpc(
                "/execution_engine.v2.EngineExecution/FundSimulationAccount",
                crate::proto_v2::FundSimulationAccountRequest {
                    credential_id: PAPER_TRANSPORT_CRED.into(),
                    amount_usdt: "1000".into(),
                    idempotency_identity: "quote-test-fund".into(),
                    schema_version: 1,
                },
            )
            .await
            .unwrap();
        assert!(funded.applied);
        // Existing durable entry truth is a fixture; quote ingress and valuation
        // use the authenticated production server, with no AI/bot scheduler.
        sqlx::query("INSERT INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,active_key,qty,entry_price,opened_at_ms,updated_at_ms) VALUES ('quote-pos',?,'paused-quote-bot',?,'long',1,'open','quote-active','0.01','60000',?,?)").bind(PAPER_TRANSPORT_CRED).bind(BTC_KEY).bind(now_ms()-300000).bind(now_ms()).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO risk_verdicts (risk_verdict_id,intent_id,credential_id,bot_uid,symbol_key,created_at_ms,policy_version,policy_hash,decision,sizing_json,rules_json,evaluator_version) VALUES ('quote-rv','quote-intent',?,'paused-quote-bot',?,?,'fixture','fixture','allow',?,'[]','fixture')").bind(PAPER_TRANSPORT_CRED).bind(BTC_KEY).bind(now_ms()).bind(r#"{"leverage":1,"final_notional_usdt":"600","final_qty":"0.01"}"#).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,status,position_id,order_role,risk_verdict_id,created_at_ms,updated_at_ms) VALUES ('quote-entry',?,'paused-quote-bot','quote-idem','binance','futures',?,'open_long','long','market','0.01','filled','quote-pos','entry','quote-rv',?,?)").bind(PAPER_TRANSPORT_CRED).bind(BTC_KEY).bind(now_ms()).bind(now_ms()).execute(store.pool()).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quote_wire_persists_get_account_state_and_restores_without_ai() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let mut env = TransportEnv(Vec::new());
        env.set("SIMULATION_MARK_QUOTE_FIXTURE", "OFFLINE_MARKET_FIXTURE");
        env.set("ENGINE_STAGE_MAX", "paper");
        env.set("ENGINE_LIVE_ENABLED", "0");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        env.set(
            "BINANCE_FUTURES_WS_BASE_URL",
            &format!("ws://{}", listener.local_addr().unwrap()),
        );
        let frame = Arc::new(std::sync::Mutex::new(quote_frame(now_ms() - 10)));
        let server_frame = frame.clone();
        let (paths_tx, mut paths_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut observed = String::new();
                let mut ws=tokio_tungstenite::accept_hdr_async(socket,|req: &tokio_tungstenite::tungstenite::handshake::server::Request,mut response: tokio_tungstenite::tungstenite::handshake::server::Response| { observed=req.uri().to_string(); response.headers_mut().insert("X-Fixture-Label","OFFLINE_MARKET_FIXTURE".parse().unwrap()); Ok(response) }).await.unwrap();
                paths_tx.send(observed).unwrap();
                let text = server_frame.lock().unwrap().to_string();
                let _ = ws.send(Message::Text(text)).await;
                // No forming/closed bar or decision tick supplies this price.
                while ws.next().await.is_some() {}
            }
        });
        let (store, path) = test_store().await;
        let engine = crate::position_close_exchange_tests::Engine::start(store.clone()).await;
        funded_quote_position(&store, &engine).await;
        let ensured: crate::proto::EnsureMarketDataResponse = engine
            .rpc(
                "/execution_engine.vnext.EngineExecution/EnsureMarketData",
                crate::proto::EnsureMarketDataRequest {
                    credential_id: PAPER_TRANSPORT_CRED.into(),
                    subscriptions: vec![crate::proto::MarketDataSubscription {
                        exchange: "binance".into(),
                        market_kind: "futures".into(),
                        symbol_key: BTC_KEY.into(),
                        timeframe: "1m".into(),
                    }],
                },
            )
            .await
            .unwrap();
        assert!(ensured.accepted);
        tokio::time::timeout(Duration::from_secs(5), async {
            while quote_event_count(&store).await != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let account = quote_account(&engine).await;
        assert!(account.available, "{}", account.unavailable_reason);
        let truth = account.positions[0].mark_truth.clone().unwrap();
        assert_eq!(account.as_of_ms, truth.source_timestamp_ms);
        assert_eq!(account.positions[0].mark_price, "60001.25");
        assert_eq!(account.equity_usdt.as_deref(), Some("1000.0125"));
        assert_eq!(truth.raw_price, "60001.25");
        assert_eq!(truth.credential_id, PAPER_TRANSPORT_CRED);
        assert_eq!(truth.source_fixture, "OFFLINE_MARKET_FIXTURE");
        assert_eq!(bar_event_count(&store).await, 0);
        let bound = paths_rx.recv().await.unwrap();
        let parsed = url::Url::parse(&format!("ws://127.0.0.1{bound}")).unwrap();
        assert!(parsed
            .query_pairs()
            .any(|(k, v)| k == "credentialId" && v == PAPER_TRANSPORT_CRED));
        assert!(parsed
            .query_pairs()
            .any(|(k, v)| k == "symbolKey" && v == BTC_KEY));
        stop_transport_workers(&engine.market_data).await;
        engine.stop().await;
        let restarted = crate::position_close_exchange_tests::Engine::start(store.clone()).await;
        // Re-reading after restart cannot create a new receipt or source T.
        assert_eq!(
            quote_account(&restarted).await.positions[0]
                .mark_truth
                .as_ref(),
            Some(&truth)
        );
        let before_restore_count = quote_event_count(&store).await;
        let restored = restarted
            .market_data
            .restore_required_streams(&store)
            .await
            .unwrap();
        assert_eq!(restored.started, 1);
        paths_rx.recv().await.unwrap(); // exact old frame is replayed and rejected
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(quote_event_count(&store).await, before_restore_count);
        assert_eq!(
            quote_account(&restarted).await.positions[0]
                .mark_truth
                .as_ref(),
            Some(&truth)
        );
        *frame.lock().unwrap() = quote_frame(now_ms());
        tokio::time::timeout(Duration::from_secs(5), async {
            while quote_event_count(&store).await != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let new_truth = quote_account(&restarted).await.positions[0]
            .mark_truth
            .clone()
            .unwrap();
        assert!(new_truth.source_timestamp_ms > truth.source_timestamp_ms);
        assert!(new_truth.received_at_ms >= new_truth.source_timestamp_ms);
        stop_transport_workers(&restarted.market_data).await;
        restarted.stop().await;
        server.abort();
        cleanup(store, path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quote_bad_frames_fail_through_actual_ws_reader_without_persistence() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let mut env = TransportEnv(Vec::new());
        env.set("SIMULATION_MARK_QUOTE_FIXTURE", "OFFLINE_MARKET_FIXTURE");
        env.set("ENGINE_STAGE_MAX", "paper");
        env.set("ENGINE_LIVE_ENABLED", "0");
        let (store, path) = test_store().await;
        sqlx::query("INSERT INTO market_stream_subscriptions (credential_id,symbol_key,timeframe,requested_at_ms) VALUES (?,?,'1m',1)").bind(PAPER_TRANSPORT_CRED).bind(BTC_KEY).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO simulation_account_funding_events (idempotency_identity,credential_id,amount_usdt,created_at_ms) VALUES ('bad-quote-fund',?,'1000',1)").bind(PAPER_TRANSPORT_CRED).execute(store.pool()).await.unwrap();
        for patch in [
            serde_json::json!({"source_fixture":"binance"}),
            serde_json::json!({"run_mode":"live"}),
            serde_json::json!({"credential_id":"paper:ffffffffffffffffffffffffffffffff"}),
            serde_json::json!({"symbol":"ETHUSDT"}),
            serde_json::json!({"symbol_key":"binance|futures|ETH|USDT|USDT"}),
            serde_json::json!({"price":"0"}),
            serde_json::json!({"price":"NaN"}),
            serde_json::json!({"price":"Infinity"}),
            serde_json::json!({"source_timestamp_ms":0}),
            serde_json::json!({"source_timestamp_ms":now_ms()+60000}),
            serde_json::json!({"source_timestamp_ms":now_ms()-60001}),
            serde_json::json!({"received_at_ms":now_ms()}),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            env.set(
                "BINANCE_FUTURES_WS_BASE_URL",
                &format!("ws://{}", listener.local_addr().unwrap()),
            );
            let mut frame = quote_frame(now_ms());
            for (key, value) in patch.as_object().unwrap() {
                frame[key] = value.clone();
            }
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws=tokio_tungstenite::accept_hdr_async(socket,|_req: &tokio_tungstenite::tungstenite::handshake::server::Request,mut response: tokio_tungstenite::tungstenite::handshake::server::Response| { response.headers_mut().insert("X-Fixture-Label","OFFLINE_MARKET_FIXTURE".parse().unwrap());Ok(response) }).await.unwrap();
                let _ = ws.send(Message::Text(frame.to_string())).await;
                let _ = ws.close(None).await;
            });
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(3),
                    run_kline_once(PAPER_TRANSPORT_CRED, store.clone(), BTC_KEY, "1m")
                )
                .await
                .unwrap()
                .is_err(),
                "{patch}"
            );
            server.await.unwrap();
            assert_eq!(quote_event_count(&store).await, 0);
        }
        for label in [None, Some("OFFLINE_PAPER_FIXTURE")] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            env.set(
                "BINANCE_FUTURES_WS_BASE_URL",
                &format!("ws://{}", listener.local_addr().unwrap()),
            );
            let frame = quote_frame(now_ms());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(socket, |_req: &tokio_tungstenite::tungstenite::handshake::server::Request, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    if let Some(label) = label { response.headers_mut().insert("X-Fixture-Label", label.parse().unwrap()); }
                    Ok(response)
                }).await.unwrap();
                let _ = ws.send(Message::Text(frame.to_string())).await;
                let _ = ws.close(None).await;
            });
            assert!(tokio::time::timeout(
                Duration::from_secs(3),
                run_kline_once(PAPER_TRANSPORT_CRED, store.clone(), BTC_KEY, "1m")
            )
            .await
            .unwrap()
            .is_err());
            server.await.unwrap();
            assert_eq!(quote_event_count(&store).await, 0);
        }
        // Explicit paper quote activation does not authorize an unfunded stream.
        sqlx::query("DELETE FROM simulation_account_funding_events")
            .execute(store.pool())
            .await
            .unwrap();
        assert!(
            run_kline_once(PAPER_TRANSPORT_CRED, store.clone(), BTC_KEY, "1m")
                .await
                .is_err()
        );
        assert_eq!(quote_event_count(&store).await, 0);
        cleanup(store, path).await;
    }
}
