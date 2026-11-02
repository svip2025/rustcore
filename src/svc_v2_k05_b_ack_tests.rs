// The real TLS peer holds the remote POST reply while this test injects bad
// persisted bytes. No submit-client mock or synthesized helper ACK is used.
#[derive(Default)]
struct K05AckHold {
    client_id: Mutex<Option<String>>,
    reached: [std::sync::atomic::AtomicBool; 2],
    released: Mutex<[bool; 2]>,
    signal: std::sync::Condvar,
}
impl K05AckHold {
    fn wait(&self, confirmation: bool) {
        let index = usize::from(confirmation);
        self.reached[index].store(true, std::sync::atomic::Ordering::SeqCst);
        let guard = self.released.lock().unwrap();
        let (released, timed) = self
            .signal
            .wait_timeout_while(guard, Duration::from_secs(20), |released| !released[index])
            .unwrap();
        drop(released);
        assert!(!timed.timed_out(), "test must release its held HTTP reply");
    }
    fn release(&self, confirmation: bool) {
        self.released.lock().unwrap()[usize::from(confirmation)] = true;
        self.signal.notify_all();
    }
    async fn reached(&self, confirmation: bool) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while !self.reached[usize::from(confirmation)].load(std::sync::atomic::Ordering::SeqCst)
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("production HTTP must reach the owned peer");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k05_b_authenticated_submit_ack_retains_identity_and_confirmation() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let mut gaps = Vec::new();
    for (label, state) in [
        ("k05_b_unknown", "unknown"),
        ("k05_b_mystery", "mystery"),
        ("k05_b_missing", "CANCELED"),
    ] {
        let hold = Arc::new(K05AckHold::default());
        let mut http = http_with_ack_hold(label, Some(hold.clone())).await;
        let credential = format!("dispatch-{label}");
        let mut env = exchange_credential_env(&credential);
        for (name, value) in [
            ("BINANCE_FUTURES_BASE_URL", String::new()),
            (
                "BINANCE_FAULT_PROXY_REST_BASE_URL",
                format!("https://{}", http.address),
            ),
            (
                "BINANCE_FAULT_PROXY_WS_BASE_URL",
                format!("wss://{}", http.address),
            ),
            (
                "BINANCE_FAULT_PROXY_CA_PEM_PATH",
                http.ca_path.to_str().unwrap().into(),
            ),
            ("ENGINE_LIVE_ENABLED", "false".into()),
            ("ENGINE_STAGE_MAX", "demo".into()),
            ("PROTECTION_CONFIRM_TIMEOUT_MS", "30".into()),
        ] {
            env.push(EnvGuard::set(name.into(), &value));
        }
        let (store, path) = wiring_store(&credential).await;
        let exec = crate::svc::EngineExecutionService::new(store.clone())
            .without_background_exchange_tasks()
            .with_exchange_filters(crate::exchange_filters::static_filter_cache("100"));
        let bound = bind_owned_loopback(label, "k05-b-ack-rpc")
            .await
            .expect("owned authenticated RPC required");
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
        let req = request(&credential, label);
        assert_eq!(
            rpc(client.clone(), req.clone(), false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert!(
            http.log.lock().unwrap().is_empty(),
            "auth refusal sends no HTTP"
        );
        let running = tokio::spawn(rpc(client.clone(), req.clone(), true));
        hold.reached(false).await;
        assert!(!running.is_finished(), "ACK still held by actual peer");
        // Manual injection only after the real dispatch authorized and POST arrived.
        sqlx::query("UPDATE engine_orders SET status=?")
            .bind(state)
            .execute(store.pool())
            .await
            .unwrap();
        hold.release(false);
        let response = tokio::time::timeout(Duration::from_secs(8), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.acks.len(), 1);
        let ack = &response.acks[0];
        let unresolved = state != "unknown";
        let expected_state = if unresolved { state } else { "acked" };
        let row=sqlx::query("SELECT eo.status,eo.exchange_order_id,po.exchange_algo_id,po.client_algo_id,po.exchange_order_id AS actual,po.verified_from_exchange FROM engine_orders eo JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id").fetch_one(store.pool()).await.unwrap();
        let status: String = row.get("status");
        assert_eq!(row.get::<String, _>("exchange_algo_id"), "800");
        assert_eq!(
            row.get::<String, _>("client_algo_id"),
            req.intents[0].client_order_id_short
        );
        assert_eq!(row.get::<String, _>("actual"), "88001");
        assert_eq!(row.get::<String, _>("exchange_order_id"), "88001");
        assert_eq!(
            row.get::<i64, _>("verified_from_exchange"),
            0,
            "an ACK is not confirmation"
        );
        let attempt=sqlx::query("SELECT CAST(authorized_post_count AS CHAR) AS n,sender_finished,dispatch_closed FROM protection_dispatch_attempts").fetch_one(store.pool()).await.unwrap();
        assert_eq!(crate::store::try_get_i64_text(&attempt, "n").unwrap(), 1);
        assert_eq!(attempt.get::<i64, _>("sender_finished"), 1);
        assert_eq!(attempt.get::<i64, _>("dispatch_closed"), 1);
        let event = sqlx::query(
            "SELECT payload_json FROM engine_order_events WHERE event_type='order_ack'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(&event.get::<Vec<u8>, _>("payload_json")).unwrap();
        if status != expected_state
            || ack.status != if unresolved { "error" } else { "accepted" }
            || (unresolved
                && (!ack
                    .message
                    .contains("PROTECTION_ACK_LOCAL_STATE_UNRESOLVED")
                    || payload["localStateOutcome"] != "unresolved"
                    || payload["localStateRaw"] != state))
        {
            gaps.push(format!(
                "{label}: state={status:?}, ack={ack:?}, event={payload}"
            ));
        }
        // Duplicate may be refused by the existing risk idempotency constraint;
        // regardless of its RPC representation it must not acquire another send.
        let duplicate = rpc(client.clone(), req, true).await;
        assert!(
            duplicate.is_err()
                || duplicate
                    .as_ref()
                    .unwrap()
                    .acks
                    .iter()
                    .all(|a| a.status != "accepted")
        );
        assert_eq!(
            http.log
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.starts_with("POST /fapi/v1/algoOrder?"))
                .count(),
            1
        );
        hold.reached(true).await; // Real post-ACK task remains active even on unresolved result.
        hold.release(true);
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let n = sqlx::query_scalar::<_, i64>(if label == "k05_b_missing" {
                    "SELECT COUNT(*) FROM kill_switch_state WHERE level='flatten'"
                } else {
                    "SELECT COUNT(*) FROM protection_orders WHERE verified_from_exchange=1"
                })
                .fetch_one(store.pool())
                .await
                .unwrap();
                let flattened = http
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|r| r.starts_with("POST /fapi/v1/order?"));
                if n == 1 && (label != "k05_b_missing" || flattened) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("confirmation or necessary reduce-only flatten continues after ACK response");
        let after_confirmation: String =
            sqlx::query_scalar("SELECT status FROM engine_orders WHERE engine_order_id=?")
                .bind(&ack.engine_order_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        eprintln!(
            "K05_B {}",
            serde_json::json!({"surface":"actual-authenticated-submit-tls-ack","label":label,"manual_state_after_post":state,"persisted_state_before_confirmation":status,"state_after_independent_confirmation":after_confirmation,"caller_status":ack.status,"identity_retained":true,"sender_closed":true,"post_count":1,"duplicate_refused":true,"confirmation_http":true,"flatten_http":label=="k05_b_missing","unresolved_event":payload.get("localStateOutcome")})
        );
        // Drain the task's final database bookkeeping before dropping the owned store.
        if label == "k05_b_missing" {
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let n = sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM protection_orders WHERE status='failed'",
                    )
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
                    if n == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        let _ = stop.send(());
        server.await.unwrap();
        http.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        http.task.take().unwrap().await.unwrap();
        std::fs::remove_dir_all(&http.tls_dir).unwrap();
        cleanup(store, path).await;
        drop(env);
    }
    assert!(
        gaps.is_empty(),
        "external ACK unresolved contract: {gaps:?}"
    );
}
