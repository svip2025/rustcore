use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::Duration;

use serde_json::{json, Value};
use sqlx::Row;

use super::{BinanceEventSink, EngineEventsService, EngineExecutionService};
use crate::binance_futures::BinanceUserTrade;
use crate::store::Store;

static INSTALL_DRIVERS: Once = Once::new();
const VECTOR: &str = include_str!("../tests/fixtures/rest_fill_reporting_v1.json");

/// The fixture drives actual v2 ExecuteIntents and revision-bound ClosePosition
/// over the production authenticated router, then consumes its original v1
/// events through Python's persistent reporting path. No report is synthesized.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn paper_protection_arm_and_manual_close_reach_python_reporting() {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("paper-arm-wire-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let credential = "paper:1234567890abcdef1234567890abcdef";
    let closed_at = super::now_ms() - 1_000;
    let frame = json!({
        "e": "kline", "E": closed_at, "s": "BTCUSDT",
        "k": {"t": closed_at - 60_000, "T": closed_at, "i": "1m", "x": true,
              "o": "100", "h": "101", "l": "99", "c": "100", "v": "12", "n": 7}
    })
    .to_string();
    assert!(matches!(
        crate::market_data::handle_kline_text(
            &store,
            credential,
            "binance|futures|BTC|USDT|USDT",
            "1m",
            &frame,
        )
        .await,
        crate::market_data::KlineIngest::Recorded
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let token = format!("local-paper-arm-{}", ulid::Ulid::new());
    let exec = EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
    let events = EngineEventsService::new(store.clone(), Duration::from_millis(10));
    let v2 =
        crate::svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
    let router = crate::add_engine_services(
        tonic::transport::Server::builder(),
        exec,
        events,
        v2,
        crate::svc_v2::EngineEventsV2Skeleton,
        Some(crate::AuthInterceptor::new(vec![token.clone()])),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::path::PathBuf::from(
        std::env::var_os("AI_TRADE_PROTOTYPE_ROOT")
            .expect("AI_TRADE_PROTOTYPE_ROOT must point at an ai-trade-prototype checkout"),
    );
    let python = std::env::var_os("RUSTCORE_TEST_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".venv-gate/bin/python"));
    let script = manifest.join("tests/fixtures/paper_protection_reporting_client.py");
    let python_path = std::env::var_os("PATH").unwrap();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(python)
            .arg(script)
            .arg(address.to_string())
            .arg(credential)
            .env_clear()
            .env("PATH", python_path)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("RUSTCORE_GRPC_AUTH_TOKEN", token)
            .output()
            .expect("run original reporting consumer")
    })
    .await
    .unwrap();
    let _ = stop.send(());
    server.await.unwrap();
    // Preserve actual rejected reports too: a failed bridge is evidence, not
    // permission to rewrite IDs or to hide the failing HTTP response.
    let fills = sqlx::query("SELECT eo.order_role FROM engine_fills ef JOIN engine_orders eo ON ef.credential_id=eo.credential_id AND ef.engine_order_id=eo.engine_order_id WHERE ef.credential_id=? ORDER BY ef.ts_exchange_ms")
        .bind(credential).fetch_all(store.pool()).await.unwrap();
    let fill_roles: Vec<String> = fills.iter().map(|row| row.get("order_role")).collect();
    let final_state = sqlx::query("SELECT state,qty FROM position_state WHERE credential_id=?")
        .bind(credential)
        .fetch_optional(store.pool())
        .await
        .unwrap();
    let protection =
        sqlx::query("SELECT status FROM protection_orders WHERE credential_id=? ORDER BY kind")
            .bind(credential)
            .fetch_all(store.pool())
            .await
            .unwrap();
    let protection_states: Vec<String> = protection.iter().map(|row| row.get("status")).collect();
    let cleanup = sqlx::query("SELECT pc.state,pc.complete_reason,pc.close_revision,ps.close_revision AS position_revision FROM protection_cleanup_obligations pc JOIN position_state ps ON ps.position_id=pc.position_id AND ps.credential_id=pc.credential_id WHERE pc.credential_id=?")
        .bind(credential).fetch_optional(store.pool()).await.unwrap();
    let cleanup_evidence = cleanup.as_ref().map(|row| {
        json!({
            "state": row.get::<String,_>("state"),
            "reason": row.get::<String,_>("complete_reason"),
            "revision": row.get::<i64,_>("close_revision"),
            "positionRevision": row.get::<i64,_>("position_revision"),
            "protectionStates": protection_states,
        })
    });
    if let Some(out) = std::env::var_os("RUSTCORE_PAPER_PROTECTION_WIRE_OUTPUT") {
        if let Ok(mut observation) = serde_json::from_slice::<Value>(&output.stdout) {
            observation["cleanupLedger"] = json!(cleanup_evidence);
            std::fs::write(out, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
        } else {
            std::fs::write(out, &output.stdout).unwrap();
        }
    }
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
    assert!(
        output.status.success(),
        "Python paper reporting bridge failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(protection_states, vec!["canceled", "canceled"]);
    let cleanup = cleanup_evidence.expect("real close must retain cleanup proof");
    assert_eq!(cleanup["state"], "complete");
    assert_eq!(cleanup["reason"], "paper_run_no_venue_protection");
    assert_eq!(cleanup["revision"], cleanup["positionRevision"]);
    assert_eq!(
        fill_roles,
        vec!["entry", "exit"],
        "only actual executions belong in the fill ledger"
    );
    let final_state = final_state.expect("original durable position is retained");
    assert_eq!(final_state.get::<String, _>("state"), "closed");
    assert_eq!(final_state.get::<String, _>("qty"), "0");
}

/// Exercise the actual REST fill producer, persistent event table, authenticated
/// StreamEvents service, Python protobuf consumer, mapper, and durable outbox.
/// Exchange transport/reconnect orchestration are deliberately outside this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn rest_fill_actions_reach_python_reporting_over_authenticated_grpc() {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("rest-fill-wire-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let vector: Value = serde_json::from_str(VECTOR).unwrap();
    let credential = vector["credential_id"].as_str().unwrap();
    let sink = BinanceEventSink::new(store.clone());
    for case in vector["cases"].as_array().unwrap() {
        let order_id = case["order_id"].as_i64().unwrap();
        if case["external"] != true {
            sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,run_id,cycle_id,idempotency_key,exchange_order_id,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,status,order_role,created_at_ms,updated_at_ms) VALUES (?,?,?,'run-rest-fill',?,?,?,'binance','futures',?,?,?,'market','0.01',?,'filled',?,1,1)")
                .bind(format!("engine-{order_id}"))
                .bind(credential)
                .bind(vector["bot_uid"].as_str().unwrap())
                .bind(vector["cycle_id"].as_str().unwrap())
                .bind(format!("idem_{order_id:064x}"))
                .bind(order_id.to_string())
                .bind(vector["symbol_key"].as_str().unwrap())
                .bind(case["action"].as_str().unwrap())
                .bind(case["side"].as_str().unwrap())
                .bind(i64::from(case["order_role"] == "exit"))
                .bind(case["order_role"].as_str().unwrap())
                .execute(store.pool()).await.unwrap();
        }
        let trade: BinanceUserTrade = serde_json::from_value(json!({
            "id": case["trade_id"], "symbol": vector["symbol"], "orderId": order_id,
            "side": case["exchange_side"], "price": "100", "qty": "0.01",
            "commission": "0.001", "commissionAsset": "USDT",
            "realizedPnl": "0", "time": 1_788_700_000_000_i64
        }))
        .unwrap();
        assert!(sink
            .handle_rest_user_trade(credential, &trade)
            .await
            .unwrap());
        assert!(!sink
            .handle_rest_user_trade(credential, &trade)
            .await
            .unwrap());
    }
    let row = sqlx::query("SELECT COUNT(*) AS n FROM engine_fills")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        row.get::<i64, _>("n"),
        vector["cases"].as_array().unwrap().len() as i64
    );
    let row = sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type='fill'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        row.get::<i64, _>("n"),
        vector["cases"].as_array().unwrap().len() as i64
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a real loopback socket is required; never skip the reporting bridge");
    let address = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let token = format!("local-rest-fill-{}", ulid::Ulid::new());
    let exec = EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
    let events = EngineEventsService::new(store.clone(), Duration::from_millis(10));
    let v2 =
        crate::svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
    let router = crate::add_engine_services(
        tonic::transport::Server::builder(),
        exec,
        events,
        v2,
        crate::svc_v2::EngineEventsV2Skeleton,
        Some(crate::AuthInterceptor::new(vec![token.clone()])),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::path::PathBuf::from(
        std::env::var_os("AI_TRADE_PROTOTYPE_ROOT")
            .expect("AI_TRADE_PROTOTYPE_ROOT must point at an ai-trade-prototype checkout"),
    );
    let python = std::env::var_os("RUSTCORE_TEST_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let local = root.join(".venv-gate/bin/python");
            if local.is_file() {
                local
            } else {
                PathBuf::from("python3")
            }
        });
    let python_path =
        std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/local/bin:/usr/bin:/bin"));
    let script = manifest.join("tests/fixtures/rest_fill_reporting_client.py");
    let fixture = manifest.join("tests/fixtures/rest_fill_reporting_v1.json");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(python)
            .arg(script)
            .arg(address.to_string())
            .arg(fixture)
            .env_clear()
            .env("PATH", python_path)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("RUSTCORE_GRPC_AUTH_TOKEN", token)
            .output()
            .expect("run real Python consumer")
    })
    .await
    .unwrap();
    let _ = stop.send(());
    server.await.unwrap();
    assert!(
        output.status.success(),
        "Python reporting bridge failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let observation: Value = serde_json::from_slice(&output.stdout).unwrap();
    if let Some(out) = std::env::var_os("RUSTCORE_REST_FILL_WIRE_OUTPUT") {
        std::fs::write(out, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
    }
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
}
