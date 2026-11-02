//! Real reqwest response boundaries; no production await is introduced by the hook.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Reuse the fixed K07 observer unchanged. The inner ACK proxy below never
// forwards the upsert OK, so return maintenance is stuck in wait_until_ready;
// this scenario does not claim that a COM_PING has already been sent.
#[path = "../tests/support/mysql_ping_proxy.rs"]
mod settlement_k07_ping_proxy;

async fn cancellation_case(status: u16, hold_database: bool) {
    let (store, path) = owner_store().await;
    cancellation_case_on_store(status, hold_database, store).await;
    let _ = std::fs::remove_file(path);
}

async fn cancellation_case_on_store(status: u16, hold_database: bool, store: Store) {
    let id = ulid::Ulid::new().to_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = BinanceFuturesClient::new_with_mock_responses(&id, vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.cooldown_store = Some(store.clone());
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let egress = client.egress_scope.clone();
    let table = if status == 418 {
        CooldownTable::IpBan
    } else {
        CooldownTable::RateLimit
    };
    let (header_tx, header_rx) = tokio::sync::oneshot::channel();
    let header_tx = Mutex::new(Some(header_tx));
    client.header_observer = Some(Arc::new(move |seen| {
        assert_eq!(seen, status);
        if let Some(sender) = header_tx.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server_store = store.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|x| x == b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0 && request.len() < 16_384);
            request.extend_from_slice(&buffer[..n]);
        }
        // Admission's DB reads have finished: only the subsequent safety write is blocked.
        let mut held = Vec::new();
        if hold_database {
            let slots = if server_store.kind() == DbKind::MySql {
                8
            } else {
                1
            };
            for _ in 0..slots {
                held.push(server_store.pool().acquire().await.unwrap());
            }
        }
        let _ = held_tx.send(held);
        let body = if hold_database { "{}" } else { "" };
        socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nX-MBX-USED-WEIGHT-1M: 1170\r\nX-MBX-ORDER-COUNT-1M: 1080\r\nX-MBX-ORDER-COUNT-10S: 270\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{body}").as_bytes()).await.unwrap();
        let _ = stop_rx.await;
    });
    let observer = client.clone();
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[("symbol".into(), "BTCUSDT".into())],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    let held = tokio::time::timeout(Duration::from_secs(2), held_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), header_rx)
        .await
        .expect("application actually consumed headers")
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(held);
    let memory_until = table.registry().lock().unwrap().get(&egress).copied();
    let observed_headroom = observer
        .request_limiter
        .observed_headroom_for_test(&egress, observer.clock.now_ms());
    let observed_orders = observer
        .order_count_limiter
        .admit_entry(&observer.credential_id, observer.clock.now_ms());
    let sql = format!(
        "SELECT {} FROM {} WHERE egress_scope = ?",
        table.column(),
        table.table()
    );
    let mut durable = None;
    for _ in 0..50 {
        durable = sqlx::query(&sql)
            .bind(&egress)
            .fetch_optional(store.pool())
            .await
            .unwrap()
            .map(|row| row.get::<i64, _>(0));
        if durable.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = stop_tx.send(());
    server.await.unwrap();
    observer.clear_egress_for_test();
    store.pool().close().await;
    assert!(
        memory_until.is_some(),
        "known rate-limit headers must be published before body/caller cancellation"
    );
    assert_eq!(
        durable, memory_until,
        "caller cancellation must not own the known cooldown write"
    );
    assert_eq!(
        observed_headroom.unwrap().last_used,
        1170,
        "known usage survives cancellation"
    );
    assert!(
        matches!(observed_orders, OrderAdmission::Parked { .. }),
        "actual order-count limiter consumes the header"
    );
}

#[tokio::test]
async fn settlement_s01_418_header_body_cancel() {
    cancellation_case(418, false).await;
}
#[tokio::test]
async fn settlement_s01_429_header_body_cancel() {
    cancellation_case(429, false).await;
}
#[tokio::test]
async fn settlement_s02_418_database_wait_cancel() {
    cancellation_case(418, true).await;
}
#[tokio::test]
async fn settlement_s02_429_database_wait_cancel() {
    cancellation_case(429, true).await;
}

async fn owner_store() -> (Store, std::path::PathBuf) {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!("settlement-owner-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    (store, path)
}

fn fact(until_ms: i64) -> CooldownFact {
    CooldownFact {
        scope: "private-owner-fixture".into(),
        table: CooldownTable::RateLimit,
        until_ms,
        received_at_ms: 1000,
    }
}

fn correlation() -> crate::reconcile_trace::Correlation {
    crate::reconcile_trace::Timer::http("GET", "/fapi/v1/order", RequestClass::EventTruth)
        .correlation()
}

#[tokio::test]
async fn settlement_s08_timeout_retains_unknown_and_releases_gate() {
    let (store, path) = owner_store().await;
    let held = store.pool().acquire().await.unwrap();
    let owner = Settlements::new();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let guard = gate.clone().lock_owned().await;
    let started = tokio::time::Instant::now();
    let rx = owner.settle(
        owner.flight().unwrap(),
        fact(121_000),
        started,
        Some(store.clone()),
        (),
        guard,
        correlation(),
    );
    let reply = tokio::time::timeout(Duration::from_millis(6500), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.outcome, SettlementOutcome::TimedOut);
    assert!(started.elapsed() >= Duration::from_millis(5000));
    assert_eq!(owner.counts(), (0, 0, 1));
    assert_eq!(owner.unresolved()[0].0.until_ms, 121_000);
    assert_eq!(owner.unresolved()[0].1, SettlementOutcome::TimedOut);
    assert!(
        gate.try_lock().is_ok(),
        "body and DB failure cannot retain HTTP permit"
    );
    assert!(owner
        .close()
        .await
        .unwrap_err()
        .to_string()
        .contains("UNRESOLVED"));
    drop(held);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM binance_rate_limit_cooldowns")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "this controlled acquire-before-send case never dispatched SQL"
    );
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s08_expired_before_first_poll_does_not_start_write() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    let rx = owner.settle(
        owner.flight().unwrap(),
        fact(121_000),
        tokio::time::Instant::now() - Duration::from_secs(6),
        Some(store.clone()),
        (),
        gate,
        correlation(),
    );
    assert_eq!(rx.await.unwrap().outcome, SettlementOutcome::TimedOut);
    assert!(owner.close().await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM binance_rate_limit_cooldowns")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s11_closing_accepts_held_flight_and_multiple_waiters() {
    let owner = Settlements::new();
    let flight = owner.flight().unwrap();
    owner.begin_close();
    assert!(owner.flight().is_err());
    let a = owner.clone();
    let first = tokio::spawn(async move { a.close().await });
    let b = owner.clone();
    let second = tokio::spawn(async move { b.close().await });
    tokio::task::yield_now().await;
    assert!(!first.is_finished() && !second.is_finished());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    let rx = owner.settle(
        flight,
        fact(121_000),
        tokio::time::Instant::now(),
        None,
        (),
        gate,
        correlation(),
    );
    assert_eq!(rx.await.unwrap().outcome, SettlementOutcome::LocalOnly);
    tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(owner.counts(), (0, 0, 0));
}

#[tokio::test]
async fn settlement_s12_panic_is_unresolved_and_does_not_strand_a_closer() {
    let owner = Settlements::new();
    owner
        .panic_next_write
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let rx = owner.settle(
        owner.flight().unwrap(),
        fact(121_000),
        tokio::time::Instant::now(),
        None,
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    assert_eq!(rx.await.unwrap().outcome, SettlementOutcome::Panicked);
    assert!(gate.try_lock().is_ok());
    assert!(tokio::time::timeout(Duration::from_secs(1), owner.close())
        .await
        .unwrap()
        .is_err());
    assert_eq!(owner.unresolved()[0].1, SettlementOutcome::Panicked);
}

#[tokio::test]
async fn settlement_s10_only_durable_covering_write_clears_an_obligation() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    for (until, persistence, inject_panic, expected) in [
        (150_000, true, true, SettlementOutcome::Panicked),
        (120_000, true, false, SettlementOutcome::Durable),
        (160_000, false, false, SettlementOutcome::LocalOnly),
        (170_000, true, false, SettlementOutcome::Durable),
    ] {
        owner
            .panic_next_write
            .store(inject_panic, std::sync::atomic::Ordering::SeqCst);
        let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
        let rx = owner.settle(
            owner.flight().unwrap(),
            fact(until),
            tokio::time::Instant::now(),
            persistence.then(|| store.clone()),
            (),
            gate,
            correlation(),
        );
        assert_eq!(rx.await.unwrap().outcome, expected);
        assert_eq!(
            owner.counts().2,
            usize::from(until < 170_000),
            "smaller or local-only success cannot erase a durable obligation"
        );
    }
    owner.close().await.unwrap();
    let until: i64 = sqlx::query_scalar("SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns WHERE egress_scope='private-owner-fixture'").fetch_one(store.pool()).await.unwrap();
    assert_eq!(until, 170_000);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s05_received_time_controls_both_usage_windows() {
    let client = BinanceFuturesClient::new_with_mock_responses("header-window-fixture", vec![]);
    let response = HttpAttemptResponse {
        status: 429,
        retry_after: Some("120".into()),
        used_weight_1m: Some(1170),
        order_count_1m: Some(1080),
        order_count_10s: Some(270),
        body: String::new(),
    };
    let (fact, _) = client
        .publish_response_safety("/fapi/v1/order", &response, 59_999, RetryScope::Exit)
        .unwrap();
    assert_eq!(fact.until_ms, 179_999);
    let headroom = client
        .request_limiter
        .observed_headroom_for_test(&client.egress_scope, 59_999)
        .unwrap();
    assert_eq!(
        (
            headroom.last_used,
            headroom.observed_at_ms,
            headroom.window_start_ms
        ),
        (1170, 59_999, 0)
    );
    assert!(client
        .request_limiter
        .observed_headroom_for_test(&client.egress_scope, 60_000)
        .is_none());
    assert!(matches!(
        client
            .order_count_limiter
            .admit_entry(&client.credential_id, 59_999),
        OrderAdmission::Parked {
            until_ms: 60_000,
            ..
        }
    ));
    assert!(matches!(
        client
            .order_count_limiter
            .admit_entry(&client.credential_id, 60_000),
        OrderAdmission::Admitted
    ));
    assert!(client.exit_probe_failed_too_recently(60_000).unwrap());
    client.clear_egress_for_test();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settlement_mysql_s02_known_header_cancellation_uses_production_pool() {
    use futures_util::FutureExt;
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "s02settlement",
        "settlement_mysql_s02_known_header_cancellation_uses_production_pool",
    )
    .await
    else {
        return;
    };
    for status in [418, 429] {
        let store = scratch.connect_store().await;
        let result = std::panic::AssertUnwindSafe(cancellation_case_on_store(status, true, store))
            .catch_unwind()
            .await;
        if let Err(error) = result {
            scratch.cleanup().await;
            std::panic::resume_unwind(error);
        }
    }
    scratch.cleanup().await;
}

#[tokio::test]
async fn settlement_s03_s07_durable_and_gate_free_while_caller_body_stalls() {
    let (store, path) = owner_store().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.cooldown_store = Some(store.clone());
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let observer = client.clone();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|x| x == b"\r\n\r\n") {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        socket.write_all(b"HTTP/1.1 429 Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").await.unwrap();
        released.await.unwrap();
        socket.write_all(b"{}").await.unwrap();
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    let until = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(row) = sqlx::query(
                "SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns WHERE egress_scope=?",
            )
            .bind(&observer.egress_scope)
            .fetch_optional(store.pool())
            .await
            .unwrap()
            {
                break row.get::<i64, _>(0);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(until > observer.clock.now_ms());
    assert!(!caller.is_finished(), "optional body is still pending");
    assert!(
        observer.transport_gate().unwrap().try_lock_owned().is_ok(),
        "HTTP gate lifetime does not depend on optional body"
    );
    observer.settlements.close().await.unwrap();
    release.send(()).unwrap();
    server.await.unwrap();
    assert_eq!(caller.await.unwrap().unwrap().status, 429);
    observer.clear_egress_for_test();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s11_close_cancels_real_transport_before_headers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let owner = client.settlements.clone();
    let gate = client.transport_gate().unwrap();
    let (seen, requested) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 1024];
        assert!(socket.read(&mut buf).await.unwrap() > 0);
        seen.send(()).unwrap();
        let _ = stopped.await;
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), requested)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.counts(), (1, 0, 0));
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
    assert!(gate.try_lock_owned().is_ok());
    assert_eq!(owner.counts(), (0, 0, 0));
    let _ = stop.send(());
    server.await.unwrap();
}

// The fixture observes the server's actual OK packet for this one prepared
// safety upsert. It never logs authentication packets, SQL values, or URLs.
// Holding the packet is different from delaying SQL before the commit.
#[derive(Default)]
struct MysqlSettlementWire {
    preparing: bool,
    statement_id: Option<u32>,
    executing: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MysqlSettlementFault {
    None,
    HoldAck,
    DisconnectBeforeExecute,
}

struct MysqlSettlementProxy {
    address: std::net::SocketAddr,
    stopped: tokio::sync::watch::Sender<bool>,
    ack: tokio::sync::watch::Receiver<bool>,
    executions: Arc<std::sync::atomic::AtomicUsize>,
    task: Option<tokio::task::JoinHandle<()>>,
}

async fn mysql_settlement_packet(
    socket: &mut (impl tokio::io::AsyncRead + Unpin),
) -> std::io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    socket.read_exact(&mut header).await?;
    let len =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    if len == 0 || len > 1024 * 1024 {
        return Err(std::io::Error::other("unexpected test MySQL packet length"));
    }
    let mut packet = vec![0; len + 4];
    packet[..4].copy_from_slice(&header);
    socket.read_exact(&mut packet[4..]).await?;
    Ok(packet)
}

impl MysqlSettlementProxy {
    async fn start(upstream: std::net::SocketAddr, fault: MysqlSettlementFault) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stopped, mut stop) = tokio::sync::watch::channel(false);
        let (acknowledged, ack) = tokio::sync::watch::channel(false);
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executed = executions.clone();
        let task = tokio::spawn(async move {
            let mut sockets = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stop.changed() => break,
                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        let mut stop = stop.clone();
                        let acknowledged = acknowledged.clone();
                        let executions = executed.clone();
                        sockets.spawn(async move {
                            let relay = async {
                                let server = tokio::net::TcpStream::connect(upstream).await?;
                                let (mut client_read, mut client_write) = client.into_split();
                                let (mut server_read, mut server_write) = server.into_split();
                                let state = Arc::new(Mutex::new(MysqlSettlementWire::default()));
                                let client_state = state.clone();
                                let send = async {
                                    loop {
                                        let packet = mysql_settlement_packet(&mut client_read).await?;
                                        if packet[3] == 0 {
                                            let mut state = client_state.lock().unwrap();
                                            if packet[4] == 0x16 {
                                                let sql = &packet[5..];
                                                state.preparing = sql.starts_with(b"INSERT INTO binance_rate_limit_cooldowns ")
                                                    || sql.starts_with(b"INSERT INTO binance_ip_ban_cooldowns ");
                                            } else if packet[4] == 0x17 && packet.len() >= 9 {
                                                let statement_id = u32::from_le_bytes(packet[5..9].try_into().unwrap());
                                                if state.statement_id == Some(statement_id) {
                                                    if fault == MysqlSettlementFault::DisconnectBeforeExecute {
                                                        acknowledged.send_replace(true);
                                                        return Err(std::io::Error::other("test disconnect before safety SQL dispatch"));
                                                    }
                                                    state.executing = true;
                                                    executions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                }
                                            }
                                        }
                                        server_write.write_all(&packet).await?;
                                    }
                                    #[allow(unreachable_code)]
                                    Ok::<(), std::io::Error>(())
                                };
                                let receive = async {
                                    loop {
                                        let packet = mysql_settlement_packet(&mut server_read).await?;
                                        let is_ack = {
                                            let mut state = state.lock().unwrap();
                                            if state.preparing && packet[3] == 1 {
                                                state.preparing = false;
                                                if packet[4] != 0 || packet.len() < 9 {
                                                    return Err(std::io::Error::other("prepare safety upsert did not return OK"));
                                                }
                                                state.statement_id = Some(u32::from_le_bytes(packet[5..9].try_into().unwrap()));
                                            }
                                            let is_ack = state.executing && packet[3] == 1;
                                            if is_ack {
                                                state.executing = false;
                                                if packet[4] != 0 {
                                                    return Err(std::io::Error::other("safety upsert did not return OK"));
                                                }
                                            }
                                            is_ack
                                        };
                                        if is_ack {
                                            acknowledged.send_replace(true);
                                            if fault == MysqlSettlementFault::HoldAck {
                                                // This future owns the received OK bytes until
                                                // the fixture closes its socket pair below.
                                                std::future::pending::<()>().await;
                                            }
                                        }
                                        client_write.write_all(&packet).await?;
                                    }
                                    #[allow(unreachable_code)]
                                    Ok::<(), std::io::Error>(())
                                };
                                tokio::try_join!(send, receive)?;
                                Ok::<(), std::io::Error>(())
                            };
                            tokio::select! {
                                _ = stop.wait_for(|value| *value) => {},
                                _ = relay => {},
                            }
                        });
                    },
                    Some(result) = sockets.join_next(), if !sockets.is_empty() => { result.unwrap(); }
                }
            }
            sockets.abort_all();
            while let Some(result) = sockets.join_next().await {
                assert!(result.is_ok() || result.unwrap_err().is_cancelled());
            }
        });
        Self {
            address,
            stopped,
            ack,
            executions,
            task: Some(task),
        }
    }

    async fn close(&mut self) {
        self.stopped.send_replace(true);
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

impl Drop for MysqlSettlementProxy {
    fn drop(&mut self) {
        self.stopped.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort(); // Dropping its JoinSet also aborts every owned relay.
        }
    }
}

async fn mysql_ack_case(direct: &Store, status: u16, fault: MysqlSettlementFault) {
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(direct.pool())
        .await
        .unwrap();
    let autocommit: i64 = sqlx::query_scalar("SELECT CAST(@@autocommit AS SIGNED)")
        .fetch_one(direct.pool())
        .await
        .unwrap();
    assert_eq!(autocommit, 1, "the upsert is one autocommit statement");
    let mut url = url::Url::parse(&std::env::var("RUSTCORE_TEST_MYSQL_URL").unwrap()).unwrap();
    let upstream: std::net::SocketAddr =
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(3306))
            .parse()
            .expect("the owned fixture uses a numeric loopback endpoint");
    assert!(upstream.ip().is_loopback());
    let mut proxy = MysqlSettlementProxy::start(upstream, fault).await;
    url.set_host(Some("127.0.0.1")).unwrap();
    url.set_port(Some(proxy.address.port())).unwrap();
    url.set_path(&format!("/{database}"));
    let query: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key != "ssl-mode")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(query)
        .append_pair("ssl-mode", "disabled");
    let store = Store::connect(url.as_str())
        .await
        .expect("connect via the owned ACK proxy");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.cooldown_store = Some(store.clone());
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let observer = client.clone();
    let (received, observed) = tokio::sync::oneshot::channel();
    let received = Mutex::new(Some(received));
    client.header_observer = Some(Arc::new(move |_| {
        if let Some(received) = received.lock().unwrap().take() {
            let _ = received.send(tokio::time::Instant::now());
        }
    }));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut part = [0; 1024];
            let size = socket.read(&mut part).await.unwrap();
            assert!(size > 0 && bytes.len() < 16_384);
            bytes.extend_from_slice(&part[..size]);
        }
        socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    let observed = tokio::time::timeout(Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), proxy.ack.wait_for(|value| *value))
        .await
        .expect("proxy reached the exact targeted safety SQL boundary")
        .unwrap();
    let table = if status == 418 {
        CooldownTable::IpBan
    } else {
        CooldownTable::RateLimit
    };
    let expected = table.registry().lock().unwrap()[&observer.egress_scope];
    // This connection bypasses the proxy and cannot consume its held ACK.
    let sql = format!(
        "SELECT {} FROM {} WHERE egress_scope=?",
        table.column(),
        table.table()
    );
    let durable: Option<i64> = sqlx::query_scalar(&sql)
        .bind(&observer.egress_scope)
        .fetch_optional(direct.pool())
        .await
        .unwrap();
    assert_eq!(
        durable,
        (fault != MysqlSettlementFault::DisconnectBeforeExecute).then_some(expected),
        "independent query distinguishes committed/unknown from this controlled non-dispatch"
    );
    if fault == MysqlSettlementFault::HoldAck {
        assert!(
            !caller.is_finished(),
            "server commit is not client acknowledgement"
        );
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let result = tokio::time::timeout_at(
            observed + Duration::from_millis(6500),
            observer.settlements.close(),
        )
        .await
        .expect("fixed 5000ms settlement plus 1500ms margin");
        assert!(result.unwrap_err().to_string().contains("UNRESOLVED"));
        assert!(observed.elapsed() >= Duration::from_millis(4950));
        assert_eq!(observer.settlements.counts(), (0, 0, 1));
        let unresolved = observer.settlements.unresolved();
        assert_eq!(unresolved[0].1, SettlementOutcome::TimedOut);
        assert_eq!(unresolved[0].0.until_ms, expected);
    } else if fault == MysqlSettlementFault::DisconnectBeforeExecute {
        let error = tokio::time::timeout_at(observed + Duration::from_millis(6500), caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("DatabaseError"));
        assert!(observer
            .settlements
            .close()
            .await
            .unwrap_err()
            .to_string()
            .contains("UNRESOLVED"));
        assert_eq!(observer.settlements.counts(), (0, 0, 1));
        assert_eq!(
            observer.settlements.unresolved()[0].1,
            SettlementOutcome::DatabaseError
        );
    } else {
        let response = tokio::time::timeout_at(observed + Duration::from_millis(6500), caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status, status);
        observer.settlements.close().await.unwrap();
        assert_eq!(observer.settlements.counts(), (0, 0, 0));
    }
    assert!(observer.transport_gate().unwrap().try_lock_owned().is_ok());
    assert_eq!(
        proxy.executions.load(std::sync::atomic::Ordering::SeqCst),
        usize::from(fault != MysqlSettlementFault::DisconnectBeforeExecute),
        "no compensation/retry write"
    );
    println!("SETTLEMENT_S09 status={status} fault={fault:?} committed_row={} unresolved={} elapsed_ms={} sql_executions={}", durable.is_some(), fault != MysqlSettlementFault::None, observed.elapsed().as_millis(), proxy.executions.load(std::sync::atomic::Ordering::SeqCst));
    // Test-owned sockets are deliberately closed before pool cleanup. This is
    // not S15's evidence for production K07 physical return/drain behavior.
    proxy.close().await;
    tokio::time::timeout(Duration::from_secs(2), store.pool().close())
        .await
        .unwrap();
    server.await.unwrap();
    // Explicit test-side recovery only: the frozen until is idempotent, a
    // smaller value cannot shorten it, and neither operation auto-discharges
    // the owner's unknown obligation. Production performs no compensation.
    for until in [expected, expected.saturating_sub(1000), expected] {
        persist_cooldown(
            direct,
            table,
            &observer.egress_scope,
            until,
            observer.clock.now_ms(),
        )
        .await
        .unwrap();
    }
    let restored: i64 = sqlx::query_scalar(&sql)
        .bind(&observer.egress_scope)
        .fetch_one(direct.pool())
        .await
        .unwrap();
    assert_eq!(restored, expected);
    assert_eq!(
        observer.settlements.counts().2,
        usize::from(fault != MysqlSettlementFault::None)
    );
    observer.clear_egress_for_test();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_mysql_s09_committed_but_unacknowledged_remains_unknown() {
    use futures_util::FutureExt;
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "s09settlement",
        "settlement_mysql_s09_committed_but_unacknowledged_remains_unknown",
    )
    .await
    else {
        return;
    };
    let direct = scratch.connect_store().await;
    let result = std::panic::AssertUnwindSafe(async {
        for status in [429, 418] {
            for fault in [
                MysqlSettlementFault::None,
                MysqlSettlementFault::HoldAck,
                MysqlSettlementFault::DisconnectBeforeExecute,
            ] {
                mysql_ack_case(&direct, status, fault).await;
            }
        }
    })
    .catch_unwind()
    .await;
    direct.pool().close().await;
    scratch.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

struct SettlementTestTask<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for SettlementTestTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn s15_until(
    what: &str,
    deadline: std::time::Instant,
    predicate: impl Fn() -> bool,
) -> std::time::Instant {
    loop {
        if predicate() {
            return std::time::Instant::now();
        }
        assert!(std::time::Instant::now() < deadline, "S15: {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn s15_mysql_id(connection: &mut sqlx::pool::PoolConnection<sqlx::Any>) -> u32 {
    let id: i64 = sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS SIGNED)")
        .fetch_one(&mut **connection)
        .await
        .unwrap();
    u32::try_from(id).unwrap()
}

async fn s15_server_count(direct: &Store, ids: &[u32]) -> i64 {
    let mut count = 0;
    for id in ids {
        count += sqlx::query_scalar::<_, i64>(
            "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.PROCESSLIST WHERE ID=?",
        )
        .bind(i64::from(*id))
        .fetch_one(direct.pool())
        .await
        .unwrap();
    }
    count
}

async fn s15_server_gone(direct: &Store, ids: &[u32], from: std::time::Instant) -> u128 {
    loop {
        if s15_server_count(direct, ids).await == 0 {
            return from.elapsed().as_millis();
        }
        assert!(
            from.elapsed() <= Duration::from_secs(2),
            "S15 server connections remain"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn s15_mysql_return_case(direct: &Store, status: u16, close_during_return: bool) {
    use futures_util::FutureExt;
    use settlement_k07_ping_proxy::PingProxy;
    use std::time::Instant;

    const MARGIN: Duration = Duration::from_millis(1500);
    const T_RELEASE: Duration = Duration::from_millis(5000);
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(direct.pool())
        .await
        .unwrap();
    let mut url = url::Url::parse(&std::env::var("RUSTCORE_TEST_MYSQL_URL").unwrap()).unwrap();
    let upstream: std::net::SocketAddr =
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(3306))
            .parse()
            .unwrap();
    assert!(upstream.ip().is_loopback());
    // Store -> unchanged K07 EOF observer -> settlement ACK holder -> MySQL.
    // Neither proxy is released or closed until all success evidence is saved.
    let mut ack_proxy = MysqlSettlementProxy::start(upstream, MysqlSettlementFault::HoldAck).await;
    let proxy = PingProxy::start(ack_proxy.address).await.unwrap();
    url.set_host(Some("127.0.0.1")).unwrap();
    url.set_port(Some(proxy.addr().port())).unwrap();
    url.set_path(&format!("/{database}"));
    url.set_query(Some("ssl-mode=disabled"));
    let store = Store::connect(url.as_str()).await.unwrap();
    let pool = store.pool().clone();
    assert_eq!(store.kind(), DbKind::MySql);
    assert_eq!(pool.options().get_max_connections(), 8);
    assert_eq!(pool.options().get_min_connections(), 0);

    let result = std::panic::AssertUnwindSafe(async {
        // Real production pool, with its eight slots. Keep seven business
        // leases so the sole available connection is unambiguous throughout
        // preflight and the actual safety upsert.
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(tokio::time::timeout(MARGIN, pool.acquire()).await.unwrap().unwrap());
        }
        let mut ids = Vec::new();
        for connection in &mut held {
            ids.push(s15_mysql_id(connection).await);
        }
        assert_eq!(ids.iter().collect::<std::collections::HashSet<_>>().len(), 8);
        let id = *ids.last().unwrap();
        let before_pings = proxy.by_mysql_id(id).pings;
        drop(held.pop().unwrap());
        s15_until("healthy return before fault", Instant::now() + MARGIN, || {
            pool.num_idle() == 1 && proxy.by_mysql_id(id).pings > before_pings
        }).await;
        let mut healthy = tokio::time::timeout(MARGIN, pool.acquire()).await.unwrap().unwrap();
        assert_eq!(s15_mysql_id(&mut healthy).await, id);
        let one: i64 = sqlx::query_scalar("SELECT CAST(1 AS SIGNED)")
            .fetch_one(&mut *healthy).await.unwrap();
        assert_eq!(one, 1);
        drop(healthy);
        s15_until("healthy connection idle", Instant::now() + MARGIN, || pool.num_idle() == 1).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
        let (received, observed) = tokio::sync::oneshot::channel();
        let received = Mutex::new(Some(received));
        client.header_observer = Some(Arc::new(move |actual| {
            assert_eq!(actual, status);
            if let Some(sender) = received.lock().unwrap().take() {
                let _ = sender.send(Instant::now());
            }
        }));
        let observer = client.clone();
        let (request_seen, request_arrived) = tokio::sync::oneshot::channel();
        let (publish_headers, publish) = tokio::sync::oneshot::channel();
        let mut http = SettlementTestTask(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_headers(&mut socket).await;
            request_seen.send(()).unwrap();
            publish.await.unwrap();
            socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
        }));
        let preflight_at = Instant::now();
        let mut caller = SettlementTestTask(tokio::spawn(async move {
            client.send_signed_once("GET", "/fapi/v1/order", &[], RetryScope::Normal,
                RequestClass::EventTruth).await
        }));
        // The serial preflight reads cross two protocol observers and one
        // available pool slot. This bounded fixture setup precedes headers and
        // therefore neither starts nor enlarges either frozen safety budget.
        tokio::time::timeout(Duration::from_secs(10), request_arrived)
            .await.expect("S15 real request must reach the fixture before headers").unwrap();
        let preflight_ms = preflight_at.elapsed().as_millis();
        publish_headers.send(()).unwrap();
        let headers_at = tokio::time::timeout(MARGIN, observed).await.unwrap().unwrap();
        tokio::time::timeout(MARGIN, ack_proxy.ack.wait_for(|seen| *seen))
            .await.unwrap().unwrap();
        assert_eq!(pool.size(), 8);
        assert_eq!(pool.num_idle(), 0);
        assert_eq!(s15_server_count(direct, &[id]).await, 1);
        let table = if status == 418 { CooldownTable::IpBan } else { CooldownTable::RateLimit };
        let expected = table.registry().lock().unwrap()[&observer.egress_scope];
        let read = format!("SELECT {} FROM {} WHERE egress_scope=?", table.column(), table.table());
        let durable: i64 = sqlx::query_scalar(&read).bind(&observer.egress_scope)
            .fetch_one(direct.pool()).await.unwrap();
        assert_eq!(durable, expected, "independent connection proves commit before unknown outcome");
        let pings_at_write = proxy.by_mysql_id(id).pings;
        caller.0.abort();
        assert!((&mut caller.0).await.unwrap_err().is_cancelled());
        let close = tokio::time::timeout_at(
            tokio::time::Instant::from_std(headers_at + Duration::from_millis(6500)),
            observer.settlements.close(),
        ).await.unwrap();
        let owner_done = Instant::now();
        assert!(close.unwrap_err().to_string().contains("UNRESOLVED"));
        assert!(owner_done.duration_since(headers_at) >= Duration::from_millis(4950));
        assert_eq!(observer.settlements.counts(), (0, 0, 1));
        assert_eq!(observer.settlements.unresolved()[0].1, SettlementOutcome::TimedOut);
        assert_eq!(observer.settlements.unresolved()[0].0.until_ms, expected);
        assert!(observer.transport_gate().unwrap().try_lock_owned().is_ok());
        assert_eq!((pool.size(), pool.num_idle()), (8, 0));
        let raw = proxy.by_mysql_id(id);
        assert!(raw.client_eof_at.is_none() && raw.relay_done_at.is_none());
        assert_eq!(raw.pings, pings_at_write, "unconsumed upsert ACK blocks ready before COM_PING");
        assert_eq!(s15_server_count(direct, &[id]).await, 1);

        let mut fresh_id = None;
        let released_at;
        let first_close_at;
        if close_during_return {
            // This is the first pool.close, while the ACK is still held. The
            // seven ordinary business leases are released explicitly; K07
            // does not impose a deadline on business-held connections.
            let close_pool = pool.clone();
            let mut closer = SettlementTestTask(tokio::spawn(async move {
                close_pool.close().await;
                Instant::now()
            }));
            drop(held);
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!closer.0.is_finished(), "first close must wait for the pending return");
            let at = tokio::time::timeout_at(
                tokio::time::Instant::from_std(owner_done + T_RELEASE + MARGIN),
                &mut closer.0,
            ).await.unwrap().unwrap();
            assert!(at.duration_since(owner_done) >= Duration::from_millis(4950));
            assert!(pool.is_closed());
            assert_eq!((pool.size(), pool.num_idle()), (0, 0));
            released_at = at;
            first_close_at = at;
        } else {
            assert!(tokio::time::timeout(Duration::from_millis(500), pool.acquire()).await.is_err(),
                "all eight slots remain occupied before return cleanup");
            released_at = s15_until("failed return releases the eighth slot",
                owner_done + T_RELEASE + MARGIN, || pool.size() == 7).await;
            assert!(released_at.duration_since(owner_done) >= Duration::from_millis(4950));
            let mut fresh = tokio::time::timeout(MARGIN, pool.acquire()).await.unwrap().unwrap();
            let new_id = s15_mysql_id(&mut fresh).await;
            assert!(!ids.contains(&new_id));
            assert_eq!(pool.size(), 8, "production capacity is again fully usable");
            let one: i64 = sqlx::query_scalar("SELECT CAST(1 AS SIGNED)")
                .fetch_one(&mut *fresh).await.unwrap();
            assert_eq!(one, 1);
            drop(fresh);
            s15_until("healthy return after fault", Instant::now() + MARGIN, || pool.num_idle() == 1).await;
            let mut again = tokio::time::timeout(MARGIN, pool.acquire()).await.unwrap().unwrap();
            assert_eq!(s15_mysql_id(&mut again).await, new_id);
            drop(again);
            fresh_id = Some(new_id);
            drop(held);
            s15_until("all eight slots healthy and idle", Instant::now() + MARGIN, || pool.num_idle() == 8).await;
            tokio::time::timeout(MARGIN, pool.close()).await.unwrap();
            first_close_at = Instant::now();
            assert_eq!((pool.size(), pool.num_idle()), (0, 0));
        }

        // These are observed with the ACK still permanently withheld. Closing
        // the proxies in cleanup below cannot manufacture this evidence.
        proxy.wait_for("actual client EOF and relay exit", MARGIN, |rows| {
            rows.iter().all(|row| row.relay_done_at.is_some())
                && rows.iter().any(|row| row.mysql_id == Some(id) && row.client_eof_at.is_some())
        }).await;
        let raw = proxy.by_mysql_id(id);
        let eof = raw.client_eof_at.unwrap();
        assert!(eof.duration_since(owner_done) <= T_RELEASE + MARGIN);
        assert!(eof.duration_since(owner_done) >= Duration::from_millis(4950));
        assert_eq!(raw.pings, pings_at_write);
        let all_ids: Vec<_> = proxy.snapshot().iter().map(|row| row.mysql_id.unwrap()).collect();
        let server_gone_ms = s15_server_gone(direct, &all_ids, first_close_at).await;
        assert_eq!(ack_proxy.executions.load(std::sync::atomic::Ordering::SeqCst), 1,
            "physical cleanup must not issue a compensating upsert");
        let still_durable: i64 = sqlx::query_scalar(&read).bind(&observer.egress_scope)
            .fetch_one(direct.pool()).await.unwrap();
        assert_eq!(still_durable, expected, "timeout is not rollback");
        assert_eq!(observer.settlements.counts(), (0, 0, 1));
        assert_eq!(observer.settlements.unresolved()[0].1, SettlementOutcome::TimedOut);
        tokio::time::timeout(MARGIN, &mut http.0).await.unwrap().unwrap();
        println!("SETTLEMENT_S15 {}", serde_json::json!({
            "status": status, "close_during_return": close_during_return,
            "held_connection_id": id, "all_initial_ids": ids, "fresh_id": fresh_id,
            "pre_header_fixture_setup_ms": preflight_ms,
            "headers_to_owner_timeout_ms": owner_done.duration_since(headers_at).as_millis(),
            "owner_timeout_to_capacity_or_first_close_ms": released_at.duration_since(owner_done).as_millis(),
            "owner_timeout_to_raw_eof_ms": eof.duration_since(owner_done).as_millis(),
            "server_gone_after_first_close_ms": server_gone_ms,
            "maintenance_stage": "wait_until_ready", "maintenance_com_ping_sent": false,
            "sql_executions": 1, "ack_released": false, "committed_row": true,
            "unresolved_retained": true, "first_close_complete": true,
            "pool_size": pool.size(), "pool_idle": pool.num_idle(),
        }));
        observer.clear_egress_for_test();
    }).catch_unwind().await;

    // Bounded failure cleanup is never used as positive physical evidence.
    if result.is_err() {
        let _ = tokio::time::timeout(Duration::from_millis(6500), pool.close()).await;
    }
    let cleanup = proxy.shutdown().await;
    ack_proxy.close().await;
    println!(
        "SETTLEMENT_S15_CLEANUP {}",
        serde_json::json!({
            "status": status, "close_during_return": close_during_return,
            "accepted": cleanup.accepted, "active_after_wait": cleanup.relays_active_after_wait,
            "forced_relays": cleanup.relays_force_aborted, "listener_closed": cleanup.listener_closed,
        })
    );
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
    assert_eq!(cleanup.relays_active_after_wait, 0);
    assert_eq!(cleanup.relays_force_aborted, 0);
    assert!(cleanup.listener_closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_mysql_s15_owner_timeout_bounds_real_return_and_first_close() {
    use futures_util::FutureExt;
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "s15settlement",
        "settlement_mysql_s15_owner_timeout_bounds_real_return_and_first_close",
    )
    .await
    else {
        return;
    };
    let direct = scratch.connect_store().await;
    let result = std::panic::AssertUnwindSafe(async {
        for status in [429, 418] {
            for close_during_return in [false, true] {
                s15_mysql_return_case(&direct, status, close_during_return).await;
            }
        }
    })
    .catch_unwind()
    .await;
    direct.pool().close().await;
    scratch.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

struct PublicationClock(std::sync::atomic::AtomicI64);
impl Clock for PublicationClock {
    fn now_ms(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

async fn read_http_headers(socket: &mut tokio::net::TcpStream) {
    let mut bytes = Vec::new();
    while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
        let mut part = [0; 1024];
        let size = socket.read(&mut part).await.unwrap();
        assert!(size > 0 && bytes.len() < 16_384);
        bytes.extend_from_slice(&part[..size]);
    }
}

fn real_client(address: std::net::SocketAddr, store: Option<Store>) -> BinanceFuturesClient {
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{address}");
    client.mock_http = None;
    client.cooldown_store = store;
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    client
}

#[tokio::test]
async fn settlement_s05_real_headers_observed_once_across_sql_and_body_boundary() {
    for status in [418, 429] {
        for usage_headers in [true, false] {
            let (store, path) = owner_store().await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
            let clock = Arc::new(PublicationClock(std::sync::atomic::AtomicI64::new(59_999)));
            client.clock = clock.clone();
            let (received, observed) = tokio::sync::oneshot::channel();
            let received = Mutex::new(Some(received));
            client.header_observer = Some(Arc::new(move |_| {
                let _ = received.lock().unwrap().take().unwrap().send(());
            }));
            let observer = client.clone();
            let (locked, held) = tokio::sync::oneshot::channel();
            let (body, release_body) = tokio::sync::oneshot::channel();
            let server_store = store.clone();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_http_headers(&mut socket).await;
                locked
                    .send(server_store.pool().acquire().await.unwrap())
                    .unwrap();
                let headers = if usage_headers {
                    "Retry-After: 120\r\nX-MBX-USED-WEIGHT-1M: 1170\r\nX-MBX-ORDER-COUNT-1M: 1080\r\nX-MBX-ORDER-COUNT-10S: 270\r\n"
                } else {
                    ""
                };
                socket.write_all(format!("HTTP/1.1 {status} Limited\r\n{headers}Content-Length: 2\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                release_body.await.unwrap();
                socket.write_all(b"{}").await.unwrap();
            });
            let caller = tokio::spawn(async move {
                client
                    .send_signed_once(
                        "GET",
                        "/fapi/v1/order",
                        &[],
                        RetryScope::Exit,
                        RequestClass::Exit,
                    )
                    .await
            });
            let held = tokio::time::timeout(Duration::from_secs(2), held)
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), observed)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                *observer.observation_audit.lock().unwrap(),
                if usage_headers {
                    vec![("weight", 59_999), ("orders", 59_999)]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                observer
                    .request_limiter
                    .observed_headroom_for_test(&observer.egress_scope, 59_999)
                    .map(|x| x.last_used),
                usage_headers.then_some(1170)
            );
            let admission = observer
                .order_count_limiter
                .admit_entry(&observer.credential_id, 59_999);
            assert_eq!(
                matches!(admission, OrderAdmission::Parked { .. }),
                usage_headers
            );
            assert!(observer.exit_probe_failed_too_recently(60_000).unwrap());
            clock.0.store(60_001, std::sync::atomic::Ordering::SeqCst);
            drop(held);
            tokio::time::timeout(Duration::from_secs(2), observer.settlements.close())
                .await
                .unwrap()
                .unwrap();
            assert!(
                !caller.is_finished(),
                "SQL finished across the boundary while body remains withheld"
            );
            let table = if status == 418 {
                CooldownTable::IpBan
            } else {
                CooldownTable::RateLimit
            };
            let until: i64 = sqlx::query_scalar(&format!(
                "SELECT {} FROM {} WHERE egress_scope=?",
                table.column(),
                table.table()
            ))
            .bind(&observer.egress_scope)
            .fetch_one(store.pool())
            .await
            .unwrap();
            assert_eq!(
                until,
                if status == 418 || usage_headers {
                    179_999
                } else {
                    61_999
                },
                "fixed header-time deadline, not body or SQL completion time"
            );
            body.send(()).unwrap();
            assert_eq!(caller.await.unwrap().unwrap().status, status);
            assert_eq!(
                *observer.observation_audit.lock().unwrap(),
                if usage_headers {
                    vec![("weight", 59_999), ("orders", 59_999)]
                } else {
                    vec![]
                },
                "optional postprocess must not observe a second time"
            );
            assert!(observer
                .request_limiter
                .observed_headroom_for_test(&observer.egress_scope, 60_001)
                .is_none());
            assert!(matches!(
                observer
                    .order_count_limiter
                    .admit_entry(&observer.credential_id, 60_001),
                OrderAdmission::Admitted
            ));
            server.await.unwrap();
            observer.clear_egress_for_test();
            store.pool().close().await;
            let _ = std::fs::remove_file(path);
        }
    }
}

#[tokio::test]
async fn settlement_s03_truncated_body_preserves_safety_and_200_does_not_create_it() {
    for status in [418, 429, 200] {
        let (store, path) = owner_store().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
        let observer = client.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_headers(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status} Reply\r\nRetry-After: 120\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
        });
        let response = client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
            .unwrap();
        assert_eq!(response.status, status);
        assert!(response.body.is_empty());
        observer.settlements.close().await.unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM binance_ip_ban_cooldowns) + (SELECT COUNT(*) FROM binance_rate_limit_cooldowns)").fetch_one(store.pool()).await.unwrap();
        assert_eq!(rows, i64::from(status != 200));
        assert_eq!(observer.settlements.counts(), (0, 0, 0));
        assert!(observer.transport_gate().unwrap().try_lock_owned().is_ok());
        server.await.unwrap();
        observer.clear_egress_for_test();
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn settlement_s03_200_body_stall_creates_no_safety_obligation() {
    let (store, path) = owner_store().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
    let (headers, observed) = tokio::sync::oneshot::channel();
    let headers = Mutex::new(Some(headers));
    client.header_observer = Some(Arc::new(move |_| {
        let _ = headers.lock().unwrap().take().unwrap().send(());
    }));
    let observer = client.clone();
    let (body, release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        release.await.unwrap();
        socket.write_all(b"{}").await.unwrap();
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!caller.is_finished());
    assert_eq!(observer.settlements.counts(), (0, 0, 0));
    let rows: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM binance_ip_ban_cooldowns) + (SELECT COUNT(*) FROM binance_rate_limit_cooldowns)").fetch_one(store.pool()).await.unwrap();
    assert_eq!(rows, 0);
    body.send(()).unwrap();
    assert_eq!(caller.await.unwrap().unwrap().body, "{}");
    server.await.unwrap();
    observer.settlements.close().await.unwrap();
    observer.clear_egress_for_test();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s07_queued_peer_waits_for_cancelled_callers_owned_gate() {
    let (store, path) = owner_store().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
    let (header, observed) = tokio::sync::oneshot::channel();
    let header = Mutex::new(Some(header));
    client.header_observer = Some(Arc::new(move |_| {
        let _ = header.lock().unwrap().take().unwrap().send(());
    }));
    let observer = client.clone();
    let mut peer = client.clone();
    peer.credential_id.push_str("-peer");
    peer.header_observer = None;
    let (waiting, at_gate) = tokio::sync::oneshot::channel();
    let waiting = Mutex::new(Some(waiting));
    peer.gate_wait_observer = Some(Arc::new(move || {
        let _ = waiting.lock().unwrap().take().unwrap().send(());
    }));
    let (requested, request) = tokio::sync::oneshot::channel();
    let (headers, release_headers) = tokio::sync::oneshot::channel();
    let (locked, held) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let (eof, eof_seen) = tokio::sync::oneshot::channel();
    let server_store = store.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_headers(&mut socket).await;
        requested.send(()).unwrap();
        release_headers.await.unwrap();
        locked
            .send(server_store.pool().acquire().await.unwrap())
            .unwrap();
        socket.write_all(b"HTTP/1.1 429 Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut byte = [0; 1];
        tokio::select! {
            _=stopped=>{},
            _=listener.accept()=>panic!("queued peer sent through known cooldown"),
            result=socket.read(&mut byte)=>{ assert_eq!(result.unwrap(),0); let _=eof.send(()); }
        }
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap();
    let peer = tokio::spawn(async move {
        peer.send_signed_once(
            "GET",
            "/fapi/v1/order",
            &[],
            RetryScope::Normal,
            RequestClass::EventTruth,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), at_gate)
        .await
        .unwrap()
        .unwrap();
    headers.send(()).unwrap();
    let held = held.await.unwrap();
    observed.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(observer.transport_gate().unwrap().try_lock_owned().is_err());
    assert!(!peer.is_finished());
    // A distinct physical egress/client continues while this one holds its gate.
    let other_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let other = real_client(other_listener.local_addr().unwrap(), None);
    let other_server = tokio::spawn(async move {
        let (mut socket, _) = other_listener.accept().await.unwrap();
        read_http_headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
    });
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            other.send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth
            )
        )
        .await
        .unwrap()
        .unwrap()
        .status,
        200
    );
    other_server.await.unwrap();
    other.clear_egress_for_test();
    drop(held);
    let error = tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cooldown active"));
    observer.settlements.close().await.unwrap();
    assert_eq!(
        observer
            .settlements
            .lost_replies
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(observer.settlements.counts(), (0, 0, 0));
    assert!(observer.transport_gate().unwrap().try_lock_owned().is_ok());
    tokio::time::timeout(Duration::from_secs(2), eof_seen)
        .await
        .unwrap()
        .unwrap();
    let _ = stop.send(());
    server.await.unwrap();
    observer.clear_egress_for_test();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s07_receiver_disappears_at_response_handoff() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    let (arrived, resume) = owner.pause_next_completion();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let reply = owner.settle(
        owner.flight().unwrap(),
        fact(150_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    tokio::time::timeout(Duration::from_secs(2), arrived)
        .await
        .unwrap()
        .unwrap();
    assert!(gate.try_lock().is_ok());
    assert_eq!(owner.counts(), (0, 1, 1));
    drop(reply);
    resume.send(()).unwrap();
    owner.close().await.unwrap();
    assert_eq!(
        owner.lost_replies.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(owner.counts(), (0, 0, 0));
    let until: i64 =
        sqlx::query_scalar("SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(until, 150_000);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s10_older_completion_cannot_erase_newer_larger_fact() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let (arrived, resume) = owner.pause_next_completion();
    let older = owner.settle(
        owner.flight().unwrap(),
        fact(150_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    tokio::time::timeout(Duration::from_secs(2), arrived)
        .await
        .unwrap()
        .unwrap();
    let held = store.pool().acquire().await.unwrap();
    let newer = owner.settle(
        owner.flight().unwrap(),
        fact(170_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    assert_eq!(owner.counts(), (0, 2, 1));
    resume.send(()).unwrap();
    assert_eq!(older.await.unwrap().outcome, SettlementOutcome::Durable);
    assert_eq!(owner.unresolved()[0].0.until_ms, 170_000);
    assert_eq!(owner.counts(), (0, 1, 1));
    drop(held);
    assert_eq!(newer.await.unwrap().outcome, SettlementOutcome::Durable);
    owner.close().await.unwrap();
    assert_eq!(owner.counts(), (0, 0, 0));
    let until: i64 =
        sqlx::query_scalar("SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(until, 170_000);
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s10_older_failure_after_newer_success_and_distinct_kinds() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    owner
        .panic_next_write
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (arrived, resume) = owner.pause_next_completion();
    let older = owner.settle(
        owner.flight().unwrap(),
        fact(150_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    tokio::time::timeout(Duration::from_secs(2), arrived)
        .await
        .unwrap()
        .unwrap();
    let newer = owner.settle(
        owner.flight().unwrap(),
        fact(170_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    assert_eq!(newer.await.unwrap().outcome, SettlementOutcome::Durable);
    resume.send(()).unwrap();
    assert_eq!(older.await.unwrap().outcome, SettlementOutcome::Panicked);
    assert_eq!(owner.counts(), (0, 0, 0));
    for table in [CooldownTable::RateLimit, CooldownTable::IpBan] {
        owner
            .panic_next_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut failed = fact(190_000);
        failed.table = table;
        let r = owner.settle(
            owner.flight().unwrap(),
            failed,
            tokio::time::Instant::now(),
            Some(store.clone()),
            (),
            gate.clone().lock_owned().await,
            correlation(),
        );
        assert_eq!(r.await.unwrap().outcome, SettlementOutcome::Panicked);
    }
    assert_eq!(owner.counts().2, 2);
    for (table, remaining) in [(CooldownTable::RateLimit, 1), (CooldownTable::IpBan, 0)] {
        let mut recovered = fact(200_000);
        recovered.table = table;
        let r = owner.settle(
            owner.flight().unwrap(),
            recovered,
            tokio::time::Instant::now(),
            Some(store.clone()),
            (),
            gate.clone().lock_owned().await,
            correlation(),
        );
        assert_eq!(r.await.unwrap().outcome, SettlementOutcome::Durable);
        assert_eq!(owner.counts().2, remaining);
    }
    owner.close().await.unwrap();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s12_post_write_panic_retains_unknown_despite_real_row() {
    let (store, path) = owner_store().await;
    let owner = Settlements::new();
    owner
        .panic_after_write
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let r = owner.settle(
        owner.flight().unwrap(),
        fact(150_000),
        tokio::time::Instant::now(),
        Some(store.clone()),
        (),
        gate.clone().lock_owned().await,
        correlation(),
    );
    assert_eq!(r.await.unwrap().outcome, SettlementOutcome::Panicked);
    assert!(owner.close().await.is_err());
    assert!(gate.try_lock().is_ok());
    let until: i64 =
        sqlx::query_scalar("SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(until, 150_000);
    assert_eq!(owner.counts(), (0, 0, 1));
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[test]
fn settlement_s12_unpolled_job_drop_is_unresolved_and_releases_resources() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let owner = Settlements::new();
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let reply = {
        let _entered = runtime.enter();
        owner.settle(
            owner.flight().unwrap(),
            fact(150_000),
            tokio::time::Instant::now(),
            None,
            (),
            gate.clone().try_lock_owned().unwrap(),
            correlation(),
        )
    };
    assert_eq!(owner.counts(), (0, 1, 1));
    drop(runtime);
    drop(reply);
    assert_eq!(owner.counts(), (0, 0, 1));
    assert!(gate.try_lock().is_ok());
    assert_eq!(owner.unresolved()[0].1, SettlementOutcome::Abandoned);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            assert!(owner.close().await.is_err());
        });
}

// Permanent form of the independently observed 0a9d S08 counterexample.
#[tokio::test]
async fn independent_s08_failed_write_releases_response_without_body() {
    let (store, path) = owner_store().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client =
        BinanceFuturesClient::new_with_mock_responses(&ulid::Ulid::new().to_string(), vec![]);
    client.base_url = format!("http://{}", listener.local_addr().unwrap());
    client.mock_http = None;
    client.cooldown_store = Some(store.clone());
    client.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let observer = client.clone();
    let (header_tx, header_rx) = tokio::sync::oneshot::channel();
    let header_tx = Mutex::new(Some(header_tx));
    client.header_observer = Some(Arc::new(move |status| {
        assert_eq!(status, 429);
        if let Some(tx) = header_tx.lock().unwrap().take() {
            tx.send(tokio::time::Instant::now()).unwrap();
        }
    }));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let eof_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_eof = eof_seen.clone();
    let server_store = store.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut part = [0; 1024];
            let n = socket.read(&mut part).await.unwrap();
            assert!(n > 0 && bytes.len() < 16384);
            bytes.extend_from_slice(&part[..n]);
        }
        // Production pre-send DB guards completed. Only the received fact's
        // actual upsert now fails; this is not an admission/fixture failure.
        sqlx::query("DROP TABLE binance_rate_limit_cooldowns")
            .execute(server_store.pool())
            .await
            .unwrap();
        socket.write_all(b"HTTP/1.1 429 Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut byte = [0; 1];
        tokio::select! {
            result = socket.read(&mut byte) => {
                let n = result.unwrap();
                assert_eq!(n, 0, "no second HTTP request");
                server_eof.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            _ = stop_rx => {}
        }
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    let received = tokio::time::timeout(Duration::from_secs(2), header_rx)
        .await
        .expect("application consumed real headers")
        .unwrap();
    tokio::time::timeout(Duration::from_millis(6500), async {
        loop {
            if observer.settlements.counts() == (0, 0, 1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("writer reaches an unresolved terminal state");
    assert_eq!(
        observer.settlements.unresolved()[0].1,
        SettlementOutcome::DatabaseError
    );
    let gate_free = observer.transport_gate().unwrap().try_lock_owned().is_ok();
    let close_unresolved = observer.settlements.close().await.is_err();
    tokio::time::sleep_until(received + Duration::from_millis(6500)).await;
    let caller_finished_before_cleanup = caller.is_finished();
    let eof_before_cleanup = eof_seen.load(std::sync::atomic::Ordering::SeqCst);
    println!("INDEPENDENT_S08 state=DatabaseError gate_free={gate_free} close_unresolved={close_unresolved} caller_finished_before_cleanup={caller_finished_before_cleanup} eof_before_cleanup={eof_before_cleanup} elapsed_ms={}", received.elapsed().as_millis());
    // Preserve the failed observation before cleanup; cancellation is never
    // credited as the owner's resource release or error delivery.
    let caller_error = if caller_finished_before_cleanup {
        caller.await.unwrap().unwrap_err().to_string()
    } else {
        caller.abort();
        let _ = caller.await;
        String::new()
    };
    let _ = stop_tx.send(());
    server.await.unwrap();
    observer.clear_egress_for_test();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
    assert!(gate_free && close_unresolved);
    assert!(caller_finished_before_cleanup && caller_error.contains("DatabaseError"));
    assert!(eof_before_cleanup,
        "failed settlement must release its response before cleanup even when optional body stays withheld");
}

#[tokio::test]
async fn settlement_s11_real_headers_win_before_closing_and_still_persist() {
    let (store, path) = owner_store().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
    let owner = client.settlements.clone();
    let closing = owner.clone();
    let (header, observed) = tokio::sync::oneshot::channel();
    let header = Mutex::new(Some(header));
    client.header_observer = Some(Arc::new(move |_| {
        // This callback is reached only after the real reqwest response won
        // the select. Closing is forced before local publication/registration.
        closing.begin_close();
        let _ = header.lock().unwrap().take().unwrap().send(());
    }));
    let observer = client.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_headers(&mut socket).await;
        socket.write_all(b"HTTP/1.1 429 Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
    });
    let caller = tokio::spawn(async move {
        client
            .send_signed_once(
                "GET",
                "/fapi/v1/order",
                &[],
                RetryScope::Normal,
                RequestClass::EventTruth,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(owner.flight().is_err());
    tokio::time::timeout(Duration::from_secs(2), owner.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(caller.await.unwrap().unwrap().status, 429);
    assert_eq!(owner.counts(), (0, 0, 0));
    let until: i64 = sqlx::query_scalar(
        "SELECT cooldown_until_ms FROM binance_rate_limit_cooldowns WHERE egress_scope=?",
    )
    .bind(&observer.egress_scope)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        Some(until),
        rate_limit_registry()
            .lock()
            .unwrap()
            .get(&observer.egress_scope)
            .copied()
    );
    server.await.unwrap();
    observer.clear_egress_for_test();
    store.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn settlement_s08_timed_out_write_drops_real_response_before_cleanup() {
    for status in [429, 418] {
        let (store, path) = owner_store().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = real_client(listener.local_addr().unwrap(), Some(store.clone()));
        let (header, observed) = tokio::sync::oneshot::channel();
        let header = Mutex::new(Some(header));
        client.header_observer = Some(Arc::new(move |_| {
            let _ = header
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(tokio::time::Instant::now());
        }));
        let observer = client.clone();
        let (locked, held) = tokio::sync::oneshot::channel();
        let (eof, seen_eof) = tokio::sync::oneshot::channel();
        let server_store = store.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_headers(&mut socket).await;
            locked
                .send(server_store.pool().acquire().await.unwrap())
                .unwrap();
            socket.write_all(format!("HTTP/1.1 {status} Limited\r\nRetry-After: 120\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            let mut byte = [0; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            let _ = eof.send(());
        });
        let mut caller = tokio::spawn(async move {
            client
                .send_signed_once(
                    "GET",
                    "/fapi/v1/order",
                    &[],
                    RetryScope::Normal,
                    RequestClass::EventTruth,
                )
                .await
        });
        let held = tokio::time::timeout(Duration::from_secs(2), held)
            .await
            .unwrap()
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), observed)
            .await
            .unwrap()
            .unwrap();
        let deadline = received + Duration::from_millis(6500);
        let result = tokio::time::timeout_at(deadline, &mut caller).await;
        let error = match result {
            Ok(Ok(Err(error))) => Some(error.to_string()),
            _ => None,
        };
        let eof_before_cleanup = matches!(
            tokio::time::timeout_at(deadline, seen_eof).await,
            Ok(Ok(()))
        );
        let gate_free = observer.transport_gate().unwrap().try_lock_owned().is_ok();
        let close_unresolved = tokio::time::timeout_at(deadline, observer.settlements.close())
            .await
            .is_ok_and(|result| result.is_err());
        let outcome = observer.settlements.unresolved().first().map(|x| x.1);
        println!("SETTLEMENT_S08 status={status} timed_out={} eof_before_cleanup={eof_before_cleanup} gate_free={gate_free} close_unresolved={close_unresolved} elapsed_ms={}",error.as_ref().is_some_and(|x|x.contains("TimedOut")),received.elapsed().as_millis());
        // Capture all resource/error evidence before releasing the DB lease or
        // cancelling fixture tasks. Cleanup can never turn the observation green.
        caller.abort();
        drop(held);
        server.abort();
        let _ = server.await;
        let rows:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM binance_ip_ban_cooldowns) + (SELECT COUNT(*) FROM binance_rate_limit_cooldowns)").fetch_one(store.pool()).await.unwrap();
        observer.clear_egress_for_test();
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
        assert!(error.is_some_and(|x| x.contains("TimedOut")));
        assert!(eof_before_cleanup && gate_free && close_unresolved);
        assert_eq!(outcome, Some(SettlementOutcome::TimedOut));
        assert_eq!(rows, 0);
    }
}
