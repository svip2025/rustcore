//! K05-B: deliberately injected persisted bytes; no assertion about their writer.
//! Actual production functions, startup, authenticated TCP RPC and owned databases.
use crate::store::{mysql_test_support::MySqlScratch, Store};
use sqlx::Row;
use std::time::Duration;

pub(crate) const BACKENDS: [&str; 4] = [
    "sqlite",
    "utf8mb4_general_ci",
    "utf8mb4_bin",
    "utf8mb4_0900_ai_ci",
];
pub(crate) struct Fixture {
    pub store: Store,
    scratch: Option<MySqlScratch>,
    path: Option<std::path::PathBuf>,
}
impl Fixture {
    pub async fn new(backend: &str, prefix: &str) -> Option<Self> {
        sqlx::any::install_default_drivers();
        if backend == "sqlite" {
            let path = std::env::temp_dir().join(format!("k05b-{}.db", ulid::Ulid::new()));
            let store = Store::connect(&format!("sqlite://{}", path.display()))
                .await
                .unwrap();
            Some(Self {
                store,
                scratch: None,
                path: Some(path),
            })
        } else {
            let scratch = MySqlScratch::create(prefix, "k05_b_contract").await?;
            let store = scratch.connect_store().await;
            for (table, column) in [("position_state", "state"), ("engine_orders", "status")] {
                // Owned fixture semantics, not a migration or production schema change.
                sqlx::query(&format!("ALTER TABLE {table} MODIFY {column} VARCHAR(32) CHARACTER SET utf8mb4 COLLATE {backend} NOT NULL"))
                    .execute(store.pool()).await.unwrap();
            }
            Some(Self {
                store,
                scratch: Some(scratch),
                path: None,
            })
        }
    }
    pub async fn cleanup(self) {
        self.store.pool().close().await;
        if let Some(scratch) = self.scratch {
            scratch.cleanup().await;
        }
        if let Some(path) = self.path {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        }
    }
}

async fn seed(store: &Store, id: &str, state: &str) {
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,active_key,pending_intent_id,pending_idem_key,qty,entry_price,opened_at_ms,updated_at_ms) VALUES (?,?,?,'binance|futures|BTC|USDT|USDT','long',1,?,?,?,?,'1','100',100,100)")
        .bind(id).bind(id).bind(id).bind(state).bind(id).bind(id).bind(id).execute(store.pool()).await.unwrap();
}
async fn snapshot(store: &Store, id: &str) -> serde_json::Value {
    let r = sqlx::query("SELECT state,qty,COALESCE(active_key,'') AS ak,COALESCE(pending_intent_id,'') AS pi,COALESCE(pending_idem_key,'') AS pk,COALESCE(entry_price,'') AS ep,CAST(updated_at_ms AS CHAR) AS ts,COALESCE(CAST(closed_at_ms AS CHAR),'') AS ct,COALESCE(CAST(last_reconciled_ms AS CHAR),'') AS rt FROM position_state WHERE position_id=?")
        .bind(id).fetch_one(store.pool()).await.unwrap();
    serde_json::json!({"state":r.get::<String,_>("state"),"qty":r.get::<String,_>("qty"),"active":r.get::<String,_>("ak"),"pending_intent":r.get::<String,_>("pi"),"pending_key":r.get::<String,_>("pk"),"price":r.get::<String,_>("ep"),"updated":r.get::<String,_>("ts"),"closed":r.get::<String,_>("ct"),"reconciled":r.get::<String,_>("rt")})
}
async fn positions(
    address: std::net::SocketAddr,
    token: &str,
    credential: &str,
    include_closed: bool,
) -> Result<crate::proto_v2::GetPositionStatesResponse, tonic::Status> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let mut req = tonic::Request::new(crate::proto_v2::GetPositionStatesRequest {
        credential_id: credential.into(),
        bot_uid: String::new(),
        include_closed,
        schema_version: 1,
    });
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let response: tonic::Response<crate::proto_v2::GetPositionStatesResponse> = client
        .unary(
            req,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/GetPositionStates",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await?;
    Ok(response.into_inner())
}

#[tokio::test]
async fn k05_b_discovery_startup_authenticated_rpc_exact_closed_only() {
    let mut gaps = Vec::new();
    for backend in BACKENDS {
        let Some(f) = Fixture::new(backend, "k05bdiscover").await else {
            continue;
        };
        let store = &f.store;
        let cases = ["closed", "CLOSED", "closed ", "unknown", "mystery", "open"];
        let mut credentials = Vec::new();
        for (i, state) in cases.iter().enumerate() {
            let id = format!("k05-b-discover-{i}");
            assert!(crate::binance_futures::keys_from_env(&id)
                .unwrap()
                .is_none());
            seed(store, &id, state).await;
            sqlx::query("INSERT INTO fill_stream_state(credential_id,ready,phase,updated_at_ms,generation) VALUES (?,1,'ready',100,7)").bind(&id).execute(store.pool()).await.unwrap();
            credentials.push(id);
        }
        seed(store, "paper", "CLOSED").await;
        seed(store, "k05-b-foreign", "CLOSED").await;
        sqlx::query("UPDATE position_state SET symbol_key='binance|delivery|BTC|USD|BTC' WHERE position_id='k05-b-foreign'").execute(store.pool()).await.unwrap();
        let discovered = crate::position_state::active_usdm_credentials(store)
            .await
            .unwrap();
        if discovered != credentials[1..] {
            gaps.push(format!("{backend} discovery: {discovered:?}"));
        }
        let exec = crate::svc::EngineExecutionService::new(store.clone());
        exec.start_fill_recovery_on_startup().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("required real loopback");
        let address = listener.local_addr().unwrap();
        let incoming =
            tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
        let token = format!("k05-b-local-{}", ulid::Ulid::new());
        let router = crate::add_engine_services(
            tonic::transport::Server::builder(),
            exec.clone(),
            crate::svc::EngineEventsService::new(store.clone(), Duration::from_millis(800)),
            crate::svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec),
            crate::svc_v2::EngineEventsV2Skeleton,
            Some(crate::AuthInterceptor::new(vec![token.clone()])),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        assert_eq!(
            positions(address, "wrong-token", &credentials[5], false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        for (i, state) in cases.iter().enumerate() {
            let view = positions(address, &token, &credentials[i], false)
                .await
                .unwrap();
            let inclusive = positions(address, &token, &credentials[i], true)
                .await
                .unwrap();
            let readiness = crate::fill_replay::load_fill_stream_state(store, &credentials[i])
                .await
                .unwrap()
                .unwrap();
            let expected_visible = *state != "closed";
            assert_eq!(inclusive.positions.len(), 1);
            assert_eq!(
                inclusive.positions[0].state, *state,
                "original bytes in management view"
            );
            if view.positions.len() != usize::from(expected_visible)
                || readiness.ready == expected_visible
                || readiness.generation != if expected_visible { 8 } else { 7 }
            {
                gaps.push(format!(
                    "{backend}/{state:?}: visible={},ready={},generation={}",
                    view.positions.len(),
                    readiness.ready,
                    readiness.generation
                ));
            }
            eprintln!(
                "K05_B {}",
                serde_json::json!({"surface":"startup/auth-rpc","backend":backend,"injected_state":state,"visible":view.positions.len(),"ready":readiness.ready,"generation":readiness.generation,"reason":readiness.failure_reason})
            );
        }
        for table in [
            "fill_recovery_scopes",
            "fill_replay_cursors",
            "engine_orders",
            "engine_fills",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(store.pool())
                    .await
                    .unwrap(),
                0,
                "discovery grants no {table}"
            );
        }
        stop.send(()).unwrap();
        task.await.unwrap();
        f.cleanup().await;
    }
    assert!(gaps.is_empty(), "exact closed/readiness contract: {gaps:?}");
}

#[tokio::test]
async fn k05_b_position_truth_exact_states_and_zero_quantity() {
    let mut gaps = Vec::new();
    for backend in BACKENDS {
        let Some(f) = Fixture::new(backend, "k05btruth").await else {
            continue;
        };
        let store = &f.store;
        for (i, state) in [
            "pending",
            "open",
            "protected",
            "closing",
            "flattening",
            "OPEN",
            "PROTECTED",
            "open ",
            "closed ",
            "unknown",
            "mystery",
            "closed",
        ]
        .iter()
        .enumerate()
        {
            for qty in ["2", "0"] {
                let id = format!("k05-b-truth-{i}-{qty}");
                seed(store, &id, state).await;
                // Keep the ordinary closing-release guard live; this is not an exit-death test.
                sqlx::query("INSERT INTO engine_orders(engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,status,created_at_ms,updated_at_ms) VALUES (?,?,?,?,'binance','futures','binance|futures|BTC|USDT|USDT','close_long','long','market','1',1,'acked',1,1)")
                    .bind(&id).bind(&id).bind(&id).bind(&id).execute(store.pool()).await.unwrap();
                sqlx::query("INSERT INTO engine_positions(credential_id,exchange,market_kind,symbol,position_amt,entry_price,mark_price,leverage,unrealized_pnl,updated_at_ms) VALUES (?,'binance','futures','BTCUSDT','1','100','101','1','0',100)").bind(&id).execute(store.pool()).await.unwrap();
                let before = snapshot(store, &id).await;
                let mut tx = store.pool().begin().await.unwrap();
                // Same transaction boundary as the reconciler: known raw exchange truth is
                // not local write authority and must roll back with an unresolved projection.
                sqlx::query("UPDATE engine_positions SET position_amt=?,updated_at_ms=200 WHERE credential_id=?").bind(qty).bind(&id).execute(&mut *tx).await.unwrap();
                let result = crate::position_state::apply_exchange_position_truth(
                    &mut tx,
                    &id,
                    "BTCUSDT",
                    Some("LONG"),
                    qty,
                    Some("101"),
                    200,
                )
                .await;
                let ok = result.is_ok();
                let code = result.as_ref().err().map(|e| format!("{:?}", e.code()));
                if ok {
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                }
                if ok
                    && matches!(
                        *state,
                        "pending" | "open" | "protected" | "closing" | "flattening"
                    )
                {
                    let mut repeat = store.pool().begin().await.unwrap();
                    crate::position_state::apply_exchange_position_truth(
                        &mut repeat,
                        &id,
                        "BTCUSDT",
                        Some("LONG"),
                        qty,
                        Some("101"),
                        200,
                    )
                    .await
                    .expect("identical canonical truth remains idempotent");
                    repeat.commit().await.unwrap();
                }
                let after = snapshot(store, &id).await;
                let raw=sqlx::query("SELECT position_amt,COALESCE(symbol_key,'') AS mapping FROM engine_positions WHERE credential_id=?").bind(&id).fetch_one(store.pool()).await.unwrap();
                let active = matches!(
                    *state,
                    "pending" | "open" | "protected" | "closing" | "flattening"
                );
                if active {
                    let expected_state = if qty == "0" {
                        "closed"
                    } else if matches!(*state, "pending" | "open") {
                        "open"
                    } else {
                        state
                    };
                    if !ok
                        || after["state"] != expected_state
                        || after["qty"] != qty
                        || after["updated"] != "200"
                        || raw.get::<String, _>("mapping") != "binance|futures|BTC|USDT|USDT"
                    {
                        gaps.push(format!("{backend}/{state}/{qty} legal: {after}"));
                    }
                    if qty == "0" || !matches!(*state, "closing" | "flattening") {
                        assert_eq!(after["pending_key"], "");
                    } else {
                        assert_eq!(after["pending_key"], id);
                    }
                } else if *state == "closed" {
                    if !ok || before != after {
                        gaps.push(format!("{backend} canonical closed changed: {after}"));
                    }
                } else if ok
                    || before != after
                    || raw.get::<String, _>("position_amt") != "1"
                    || raw.get::<String, _>("mapping") != ""
                    || code.as_deref() != Some("FailedPrecondition")
                {
                    gaps.push(format!(
                        "{backend}/{state:?}/{qty} unresolved: ok={ok},code={code:?},after={after}"
                    ));
                }
                eprintln!(
                    "K05_B {}",
                    serde_json::json!({"surface":"position-truth","backend":backend,"injected_state":state,"truth_qty":qty,"ok":ok,"code":code,"after":after,"raw_qty":raw.get::<String,_>("position_amt"),"mapping":raw.get::<String,_>("mapping")})
                );
            }
        }
        f.cleanup().await;
    }
    assert!(
        gaps.is_empty(),
        "position write authority contract: {gaps:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k05_b_position_truth_current_locked_state_beats_old_snapshot() {
    let mut gaps = Vec::new();
    for backend in BACKENDS.into_iter().filter(|b| *b != "sqlite") {
        let Some(f) = Fixture::new(backend, "k05brace").await else {
            continue;
        };
        for (i, (next, qty)) in [
            ("mystery", "2"),
            ("OPEN", "0"),
            ("closed", "2"),
            ("real_close_writer", "2"),
            ("protected", "2"),
            ("flattening", "2"),
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("k05-b-race-{i}");
            seed(&f.store, &id, "open").await;
            let mut truth_tx = f.store.pool().begin().await.unwrap();
            // Establish an earlier repeatable-read snapshot before another writer.
            let seen: String =
                sqlx::query_scalar("SELECT state FROM position_state WHERE position_id=?")
                    .bind(&id)
                    .fetch_one(&mut *truth_tx)
                    .await
                    .unwrap();
            assert_eq!(seen, "open");
            let mut writer = f.store.pool().begin().await.unwrap();
            let real_close_writer = next == "real_close_writer";
            let next = if real_close_writer { "closed" } else { next };
            if real_close_writer {
                crate::position_state::mark_position_closed(
                    &mut writer,
                    &id,
                    150,
                    Some(150),
                    "paper_fill",
                )
                .await
                .unwrap();
            } else {
                sqlx::query(
                "UPDATE position_state SET state=?,qty='3',updated_at_ms=150 WHERE position_id=?",
            )
            .bind(next)
            .bind(&id)
            .execute(&mut *writer)
            .await
            .unwrap();
            }
            let task_id = id.clone();
            let applying = tokio::spawn(async move {
                let result = crate::position_state::apply_exchange_position_truth(
                    &mut truth_tx,
                    &task_id,
                    "BTCUSDT",
                    Some("LONG"),
                    qty,
                    Some("101"),
                    200,
                )
                .await;
                if result.is_ok() {
                    truth_tx.commit().await.unwrap();
                } else {
                    truth_tx.rollback().await.unwrap();
                }
                result
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !applying.is_finished(),
                "actual function must wait on the competing row writer"
            );
            writer.commit().await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(5), applying)
                .await
                .unwrap()
                .unwrap();
            let after = snapshot(&f.store, &id).await;
            let bad = matches!(next, "mystery" | "OPEN");
            let expected_qty = if real_close_writer {
                "0"
            } else if bad || next == "closed" {
                "3"
            } else {
                qty
            };
            let expected_ts = if bad || next == "closed" {
                "150"
            } else {
                "200"
            };
            if result.is_err() != bad
                || after["state"] != next
                || after["qty"] != expected_qty
                || after["updated"] != expected_ts
            {
                gaps.push(format!(
                    "{backend}/{next}/{qty}: result={result:?}, after={after}"
                ));
            }
            if real_close_writer
                && (after["active"] != ""
                    || after["pending_key"] != ""
                    || after["pending_intent"] != ""
                    || after["closed"] != "150"
                    || after["reconciled"] != "150")
            {
                gaps.push(format!(
                    "{backend}/real_close_writer obligation fields regressed: {after}"
                ));
            }
            eprintln!(
                "K05_B {}",
                serde_json::json!({"surface":"held-writer-and-old-snapshot","backend":backend,"snapshot":"open","writer":if real_close_writer { "production mark_position_closed transaction" } else { "manual UPDATE control" },"committed_state":next,"truth_qty":qty,"result":format!("{result:?}"),"after":after})
            );
        }
        // Conversely, a writer arriving after truth has locked/updated must wait
        // through that transaction's commit; its later canonical close wins.
        let id = "k05-b-lock-lifetime";
        seed(&f.store, id, "open").await;
        let mut tx = f.store.pool().begin().await.unwrap();
        crate::position_state::apply_exchange_position_truth(
            &mut tx,
            id,
            "BTCUSDT",
            Some("LONG"),
            "2",
            None,
            200,
        )
        .await
        .unwrap();
        let store = f.store.clone();
        let later = tokio::spawn(async move {
            sqlx::query("UPDATE position_state SET state='closed',qty='0',updated_at_ms=300 WHERE position_id=?").bind(id).execute(store.pool()).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!later.is_finished());
        tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), later)
            .await
            .unwrap()
            .unwrap();
        let after = snapshot(&f.store, id).await;
        assert_eq!(after["state"], "closed");
        assert_eq!(after["updated"], "300");
        f.cleanup().await;
    }
    assert!(gaps.is_empty(), "current atomic position truth: {gaps:?}");
}

#[tokio::test]
async fn k05_b_sqlite_competing_writer_cannot_be_overwritten() {
    let f = Fixture::new("sqlite", "k05bsqlite").await.unwrap();
    // Store's SQLite pool deliberately has one connection. Use another real
    // Store for the cross-connection race, not a second lease from that pool.
    let writer_store = Store::connect(&format!("sqlite://{}", f.path.as_ref().unwrap().display()))
        .await
        .unwrap();
    for (i, next) in ["mystery", "closed"].into_iter().enumerate() {
        let id = format!("k05-b-sqlite-race-{i}");
        seed(&f.store, &id, "open").await;
        let mut tx = f.store.pool().begin().await.unwrap();
        let seen: String =
            sqlx::query_scalar("SELECT state FROM position_state WHERE position_id=?")
                .bind(&id)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(seen, "open");
        let mut writer = writer_store.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE position_state SET state=?,qty='3',updated_at_ms=150 WHERE position_id=?",
        )
        .bind(next)
        .bind(&id)
        .execute(&mut *writer)
        .await
        .unwrap();
        let result = crate::position_state::apply_exchange_position_truth(
            &mut tx,
            &id,
            "BTCUSDT",
            Some("LONG"),
            "2",
            None,
            200,
        )
        .await;
        let error = result.expect_err("SQLite must refuse this competing write transaction");
        assert!(
            error.message().contains("locked"),
            "real SQLite write conflict: {error}"
        );
        tx.rollback().await.unwrap();
        writer.commit().await.unwrap();
        let after = snapshot(&f.store, &id).await;
        assert_eq!(after["state"], next);
        assert_eq!(after["qty"], "3");
        eprintln!(
            "K05_B {}",
            serde_json::json!({"surface":"sqlite-competing-writer","writer_state":next,"result_code":format!("{:?}",error.code()),"after":after})
        );
    }
    writer_store.pool().close().await;
    f.cleanup().await;
}
