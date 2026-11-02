//! Actual unsigned HTTP headers/body and SQLite, sharing the signed owner.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Keep this fixture's client/governor observations in one minute. Socket I/O,
// sleeps and the settlement owner's five-second timeout still use real time.
struct ObservationClock;
impl Clock for ObservationClock {
    fn now_ms(&self) -> i64 {
        55_000
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    Cancel,
    SqlError,
    Timeout,
    ClosingAtHeaders,
}

async fn exercise(status: u16, discovery: bool, fault: Fault) {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!("k08-unsigned-{}.sqlite", ulid::Ulid::new()));
    let url = format!("sqlite://{}", path.display());
    let store = Store::connect(&url).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    let clock = Arc::new(ObservationClock);
    client.request_limiter = client.request_limiter.with_clock(clock.clone());
    client.clock = clock;
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.cooldown_store = Some(store.clone());
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let scope = client.egress_scope.clone();
    let table = if status == 418 {
        CooldownTable::IpBan
    } else {
        CooldownTable::RateLimit
    };
    let owner = client.settlements.clone();
    let closing_owner = owner.clone();
    let (headers_tx, headers_rx) = tokio::sync::oneshot::channel();
    let headers_tx = Mutex::new(Some(headers_tx));
    client.header_observer = Some(Arc::new(move |actual| {
        assert_eq!(actual, status);
        if fault == Fault::ClosingAtHeaders {
            closing_owner.begin_close();
        }
        let _ = headers_tx
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(tokio::time::Instant::now());
    }));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let server_store = store.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0 && request.len() < 16_384);
            request.extend_from_slice(&buf[..n]);
        }
        let text = String::from_utf8(request).unwrap();
        assert!(text.starts_with(if discovery {
            "GET /fapi/v1/exchangeInfo "
        } else {
            "POST /fapi/v1/listenKey "
        }));
        assert!(!text.contains("signature="));
        let held = if fault == Fault::SqlError {
            sqlx::query(&format!("DROP TABLE {}", table.table()))
                .execute(server_store.pool())
                .await
                .unwrap();
            None
        } else {
            Some(server_store.pool().acquire().await.unwrap())
        };
        let _ = held_tx.send(held);
        // Never finish the body. Persistence and close must not need it.
        socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nX-MBX-USED-WEIGHT-1M: 1170\r\nX-MBX-ORDER-COUNT-1M: 1080\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        let mut b = [0; 1];
        let eof = tokio::time::timeout(Duration::from_secs(7), socket.read(&mut b)).await;
        assert!(
            matches!(eof, Ok(Ok(0)) | Ok(Err(_))),
            "response must release before fixture cleanup: {eof:?}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "no second request may escape the cooldown"
        );
    });
    let observer = client.clone();
    let caller = tokio::spawn(async move {
        if discovery {
            client
                .send_unsigned_once_with_deadline(
                    "GET",
                    "/fapi/v1/exchangeInfo",
                    &[],
                    RequestClass::PeriodicReconcile,
                    Some(tokio::time::Instant::now() + Duration::from_secs(3)),
                    || {},
                )
                .await
        } else {
            client
                .send_unsigned_once("POST", "/fapi/v1/listenKey", &[], RequestClass::EventTruth)
                .await
        }
    });
    let held = held_rx.await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), headers_rx)
        .await
        .unwrap()
        .unwrap();
    if matches!(fault, Fault::Cancel | Fault::ClosingAtHeaders) {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    } else {
        let error = tokio::time::timeout(Duration::from_secs(6), caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains(if fault == Fault::Timeout {
                "TimedOut"
            } else {
                "DatabaseError"
            }),
            "{error:#}"
        );
    }
    let memory = table
        .registry()
        .lock()
        .unwrap()
        .get(&scope)
        .copied()
        .unwrap();
    assert_eq!(
        observer
            .request_limiter
            .observed_headroom_for_test(&scope, observer.clock.now_ms())
            .unwrap()
            .last_used,
        1170
    );
    assert!(matches!(
        observer
            .order_count_limiter
            .admit_entry(&observer.credential_id, observer.clock.now_ms()),
        OrderAdmission::Parked { .. }
    ));
    drop(held);
    if fault != Fault::ClosingAtHeaders {
        let mut peer = observer.clone();
        peer.credential_id = ulid::Ulid::new().to_string();
        peer.header_observer = None;
        let refused = peer.create_listen_key().await.unwrap_err();
        assert!(
            refused.to_string().contains("cooldown active")
                || (fault == Fault::SqlError
                    && refused.to_string().contains("load persisted Binance")),
            "{refused:#}"
        );
    }
    let closed = tokio::time::timeout(Duration::from_secs(2), owner.close())
        .await
        .unwrap();
    let bad = matches!(fault, Fault::SqlError | Fault::Timeout);
    assert_eq!(closed.is_err(), bad);
    assert_eq!(owner.counts(), (0, 0, usize::from(bad)));
    assert!(observer.transport_gate().unwrap().try_lock().is_ok());
    if fault == Fault::Timeout {
        assert!(received.elapsed() >= Duration::from_secs(5));
        assert_eq!(owner.unresolved()[0].1, SettlementOutcome::TimedOut);
        eprintln!(
            "unsigned timeout: status={status} real_elapsed_ms={} observation_ms={} unresolved={:?}",
            received.elapsed().as_millis(),
            observer.clock.now_ms(),
            owner.unresolved()[0].1
        );
    }
    server.await.unwrap();
    store.pool().close().await;
    if !bad {
        // Reopen from disk with a new pool after every original caller is gone.
        let fresh = Store::connect(&url).await.unwrap();
        let row: i64 = sqlx::query_scalar(&format!(
            "SELECT {} FROM {} WHERE egress_scope=?",
            table.column(),
            table.table()
        ))
        .bind(&scope)
        .fetch_one(fresh.pool())
        .await
        .unwrap();
        assert_eq!(row, memory);
        fresh.pool().close().await;
    }
    observer.clear_egress_for_test();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn unsigned_u01_u02_cancel_discovery_and_listen_key_418_429() {
    for status in [418, 429] {
        for discovery in [true, false] {
            exercise(status, discovery, Fault::Cancel).await;
        }
    }
}

#[tokio::test]
async fn unsigned_u03_sql_error_drops_real_response_and_gate() {
    for status in [418, 429] {
        exercise(status, false, Fault::SqlError).await;
    }
}

#[tokio::test]
async fn unsigned_u03_timeout_keeps_unresolved_without_body_wait() {
    for status in [418, 429] {
        exercise(status, true, Fault::Timeout).await;
    }
}

#[tokio::test]
async fn unsigned_u03_accepted_headers_register_during_closing() {
    for status in [418, 429] {
        exercise(status, false, Fault::ClosingAtHeaders).await;
    }
}

#[tokio::test]
async fn unsigned_u03_closing_before_dispatch_has_zero_http() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.settlements.begin_close();
    assert!(client
        .create_listen_key()
        .await
        .unwrap_err()
        .to_string()
        .contains("CLOSING"));
    client.settlements.close().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err()
    );
    client.clear_egress_for_test();
}

#[tokio::test]
async fn k08_r03_transport_gate_deadline_removes_request_without_late_send() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    let gate = client.transport_gate().unwrap();
    let held = gate.lock().await;
    let error = crate::svc_v2::reconcile_budget::run(
        tokio::time::Instant::now() + Duration::from_millis(100),
        async {
            client
                .get_order_json("BTCUSDT", Some("1"), None, RequestClass::PeriodicReconcile)
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    drop(held);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(client.settlements.counts(), (0, 0, 0));
    assert_eq!(client.request_limiter.queued(&client.egress_scope), 0);
    client.clear_egress_for_test();
}

#[tokio::test]
async fn k08_r04_default_reserve_allows_concurrent_public_exit() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.exit_weight_reserve = 10.0;
    client.request_limiter = CredentialRateLimiter::new(TokenBucketConfig::for_test(10.0, 5.0));
    let normal = client.clone();
    let waiting = tokio::spawn(async move {
        crate::svc_v2::reconcile_budget::run(
            tokio::time::Instant::now() + Duration::from_millis(200),
            async {
                normal
                    .get_order_json(
                        "BTCUSDT",
                        Some("normal"),
                        None,
                        RequestClass::PeriodicReconcile,
                    )
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.request_limiter.queued(&client.egress_scope) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("DELETE /fapi/v1/order?"));
        assert!(text.contains("orderId=exit"));
        let body = r#"{"orderId":1,"status":"CANCELED"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err()
        );
    });
    let started = tokio::time::Instant::now();
    let response = client
        .cancel_order_json("BTCUSDT", Some("exit"), None, RequestClass::Exit)
        .await
        .unwrap();
    assert_eq!(response["status"], "CANCELED");
    assert!(started.elapsed() < Duration::from_millis(150));
    assert_eq!(
        waiting.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    server.await.unwrap();
    assert_eq!(client.request_limiter.queued(&client.egress_scope), 0);
    client.clear_egress_for_test();
}

#[tokio::test]
async fn unsigned_u03_closing_cancels_real_preheader_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        tx.send(()).unwrap();
        let mut byte = [0; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let owner = client.settlements.clone();
    let observer = client.clone();
    let caller = tokio::spawn(async move { client.create_listen_key().await });
    rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), owner.close())
        .await
        .unwrap()
        .unwrap();
    assert!(caller
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("CLOSING"));
    assert_eq!(owner.counts(), (0, 0, 0));
    server.await.unwrap();
    observer.clear_egress_for_test();
}

#[tokio::test]
async fn unsigned_u03_real_listen_key_create_keepalive_and_429_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    let server = tokio::spawn(async move {
        for (method, status) in [("POST", 200), ("PUT", 429), ("PUT", 200)] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut buf = [0; 1024];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.starts_with(&format!("{method} /fapi/v1/listenKey")));
            if method == "PUT" {
                assert!(text.contains("listenKey=k08-owned-listen-key"));
            }
            let body = if method == "POST" {
                r#"{"listenKey":"k08-owned-listen-key"}"#
            } else {
                "{}"
            };
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status} Reply\r\nRetry-After: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let key = client.create_listen_key().await.unwrap();
    assert_eq!(key, "k08-owned-listen-key");
    client.keepalive_listen_key(&key).await.unwrap();
    client.settlements.close().await.unwrap();
    assert_eq!(client.settlements.counts(), (0, 0, 0));
    server.await.unwrap();
    client.clear_egress_for_test();
}
