use super::*;
use std::path::{Path, PathBuf};

pub(crate) async fn fixture_permit(credential: &str, wire: AlgoWire) -> ProtectionDispatchPermit {
    sqlx::any::install_default_drivers();
    let path = std::env::temp_dir().join(format!("dispatch-unit-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    fixture_on_store(&store, credential, wire).await
}
async fn fixture_on_store(
    store: &Store,
    credential: &str,
    wire: AlgoWire,
) -> ProtectionDispatchPermit {
    let id = ulid::Ulid::new().to_string();
    let engine = format!("eo_{id}");
    let protection = format!("po_{id}");
    let position = format!("pos_{id}");
    let bot = format!("bot_{id}");
    let kind = if wire.order_type == "STOP_MARKET" {
        "sl"
    } else {
        "tp"
    };
    let action = if wire.side == "SELL" {
        "close_long"
    } else {
        "close_short"
    };
    let direction = if wire.side == "SELL" { "long" } else { "short" };
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES (?,?,?,'binance|futures|BTC|USDT|USDT',?,1788000000000,'open','1',1)")
        .bind(&position).bind(credential).bind(&bot).bind(direction).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO engine_orders(engine_order_id,credential_id,bot_uid,idempotency_key,client_order_id_short,exchange,market_kind,symbol_key,action,side,order_type,price,quantity,reduce_only,status,created_at_ms,updated_at_ms,position_id,order_role) VALUES (?,?,?,?,?,'binance','futures','binance|futures|BTC|USDT|USDT',?,?,?,?,?,1,'pending_submit',1,1,?,?)")
        .bind(&engine).bind(credential).bind(&bot).bind(&id).bind(&wire.client_algo_id).bind(action).bind(direction).bind(wire.order_type.to_ascii_lowercase()).bind(&wire.trigger_price).bind(wire.quantity.as_deref().unwrap_or("")).bind(&position).bind(format!("protection_{kind}")).execute(&mut *tx).await.unwrap();
    crate::protection::insert_requested(
        &mut tx,
        &protection,
        &position,
        credential,
        kind,
        1,
        &engine,
        &wire.client_algo_id,
        &wire.trigger_price,
        wire.close_position,
        1,
    )
    .await
    .unwrap();
    let permit = prepare_in_tx(store, &mut tx, &engine, &protection, wire, 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    permit
}
fn wire() -> AlgoWire {
    AlgoWire {
        symbol: "BTCUSDT".into(),
        side: "SELL".into(),
        order_type: "STOP_MARKET".into(),
        trigger_price: "90".into(),
        quantity: Some("1".into()),
        reduce_only: true,
        close_position: false,
        client_algo_id: format!("d{}", ulid::Ulid::new()),
    }
}
async fn count(permit: &ProtectionDispatchPermit) -> (i64, i64, i64) {
    let r = sqlx::query("SELECT CAST(authorized_post_count AS CHAR) AS n,dispatch_closed,sender_finished FROM protection_dispatch_attempts WHERE attempt_id=?").bind(&permit.attempt_id).fetch_one(permit.store.pool()).await.unwrap();
    (
        crate::store::try_get_i64_text(&r, "n").unwrap(),
        r.get("dispatch_closed"),
        r.get("sender_finished"),
    )
}
#[tokio::test]
async fn dispatch_positive_arm_and_normal_finish_preserve_may_have_sent() {
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-positive", wire.clone()).await;
    assert_eq!(count(&p).await, (0, 0, 0));
    p.authorize("dispatch-positive", &wire).await.unwrap();
    p.authorize("dispatch-positive", &wire).await.unwrap();
    assert_eq!(count(&p).await, (2, 0, 0));
    p.finish("may_have_sent").await.unwrap();
    assert_eq!(count(&p).await, (2, 1, 1));
    assert!(matches!(
        p.disposition(&anyhow!("later failure")),
        FailureDisposition::MayHaveSent
    ));
}
#[tokio::test]
async fn dispatch_fence_is_exact_revision_and_does_not_erase_prior_arm() {
    for arm_first in [false, true] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-fence", wire.clone()).await;
        if arm_first {
            p.authorize("dispatch-fence", &wire).await.unwrap();
        }
        assert!(!fence(&p.store, &p.attempt_id, p.revision + 1)
            .await
            .unwrap());
        assert!(fence(&p.store, &p.attempt_id, p.revision).await.unwrap());
        assert!(!fence(&p.store, &p.attempt_id, p.revision).await.unwrap());
        let error = p.authorize("dispatch-fence", &wire).await.unwrap_err();
        assert!(if arm_first {
            matches!(p.disposition(&error), FailureDisposition::MayHaveSent)
        } else {
            matches!(
                p.disposition(&error),
                FailureDisposition::LocalProtectionFailure
            )
        });
        p.finish("fenced").await.unwrap();
        assert_eq!(count(&p).await, (i64::from(arm_first), 1, 1));
    }
}
#[tokio::test]
async fn dispatch_current_terminal_and_exact_closed_zero_deny_last_gate() {
    for (state, qty, deny) in [
        ("closed", "0.000", true),
        ("closed", "+0", true),
        ("open", "0", false),
        ("closed", "1e-999", false),
        ("closed", "1", false),
    ] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-zero", wire.clone()).await;
        sqlx::query("UPDATE position_state SET state=?,qty=? WHERE position_id=?")
            .bind(state)
            .bind(qty)
            .bind(p.binding.value("position_id"))
            .execute(p.store.pool())
            .await
            .unwrap();
        let result = p.authorize("dispatch-zero", &wire).await;
        assert_eq!(result.is_err(), deny, "{state}/{qty}");
        assert_eq!(count(&p).await.0, i64::from(!deny));
    }
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-terminal", wire.clone()).await;
    sqlx::query("UPDATE engine_orders SET status='canceled' WHERE engine_order_id=?")
        .bind(p.binding.value("engine_order_id"))
        .execute(p.store.pool())
        .await
        .unwrap();
    assert!(p.authorize("dispatch-terminal", &wire).await.is_err());
    assert_eq!(count(&p).await.0, 0);
}
#[tokio::test]
async fn dispatch_changed_business_identity_is_never_authority() {
    for (table, column, value) in [
        ("engine_orders", "credential_id", "DISPATCH-EXACT"),
        ("engine_orders", "credential_id", "dispatch-exact "),
        ("engine_orders", "bot_uid", "different"),
        ("engine_orders", "quantity", "3"),
        ("protection_orders", "generation", "2"),
        ("protection_orders", "client_algo_id", "different"),
        ("position_state", "entry_epoch", "1788000000001"),
        ("position_state", "symbol_key", "foreign"),
    ] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-exact", wire.clone()).await;
        let (key, id) = match table {
            "engine_orders" => ("engine_order_id", p.binding.value("engine_order_id")),
            "protection_orders" => (
                "protection_order_id",
                p.binding.value("protection_order_id"),
            ),
            _ => ("position_id", p.binding.value("position_id")),
        };
        sqlx::query(&format!("UPDATE {table} SET {column}=? WHERE {key}=?"))
            .bind(value)
            .bind(id)
            .execute(p.store.pool())
            .await
            .unwrap();
        let error = p.authorize("dispatch-exact", &wire).await.unwrap_err();
        assert!(
            matches!(
                p.disposition(&error),
                FailureDisposition::LocalProtectionFailure
            ),
            "{table}.{column}"
        );
        assert_eq!(count(&p).await.0, 0);
    }
}
#[tokio::test]
async fn dispatch_wire_and_caller_mismatch_do_not_arm() {
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-wire", wire.clone()).await;
    let mut changed = wire.clone();
    changed.trigger_price = "1".into();
    assert!(p.authorize("dispatch-wire", &changed).await.is_err());
    assert!(p.authorize("DISPATCH-WIRE", &wire).await.is_err());
    assert_eq!(count(&p).await.0, 0);
    p.authorize("dispatch-wire", &wire).await.unwrap();
    assert_eq!(count(&p).await.0, 1);
}
#[tokio::test]
async fn dispatch_drop_does_not_reclaim_prepared_or_armed() {
    for arm in [false, true] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-drop", wire.clone()).await;
        if arm {
            p.authorize("dispatch-drop", &wire).await.unwrap();
        }
        let store = p.store.clone();
        let id = p.attempt_id.clone();
        drop(p);
        let r=sqlx::query("SELECT phase,sender_finished,dispatch_closed FROM protection_dispatch_attempts WHERE attempt_id=?").bind(id).fetch_one(store.pool()).await.unwrap();
        assert_eq!(
            r.get::<String, _>("phase"),
            if arm { "armed" } else { "prepared" }
        );
        assert_eq!(r.get::<i64, _>("sender_finished"), 0);
        assert_eq!(r.get::<i64, _>("dispatch_closed"), 0);
    }
}
#[tokio::test]
async fn dispatch_sql_failure_never_becomes_safe_terminal_denial() {
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-sql", wire.clone()).await;
    sqlx::query("CREATE TRIGGER fail_arm BEFORE UPDATE OF authorized_post_count ON protection_dispatch_attempts BEGIN SELECT RAISE(FAIL,'arm denied'); END").execute(p.store.pool()).await.unwrap();
    let error = p.authorize("dispatch-sql", &wire).await.unwrap_err();
    assert!(matches!(
        p.disposition(&error),
        FailureDisposition::LocalProtectionFailure
    ));
    assert_eq!(count(&p).await, (0, 0, 0));
}
#[tokio::test]
async fn dispatch_ack_and_closure_rollback_together() {
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-ack", wire.clone()).await;
    p.authorize("dispatch-ack", &wire).await.unwrap();
    let ack = serde_json::from_value(
        serde_json::json!({"algoId":456,"clientAlgoId":wire.client_algo_id,"algoStatus":"NEW"}),
    )
    .unwrap();
    let mut tx = p.store.pool().begin().await.unwrap();
    crate::protection::mark_algo_submitted_in_tx(
        &mut tx,
        p.binding.value("protection_order_id"),
        &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
        &ack,
        3,
    )
    .await
    .unwrap();
    p.finish_in_tx(&mut tx, "acknowledged").await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count(&p).await, (1, 0, 0));
    let r = sqlx::query(
        "SELECT status,exchange_algo_id FROM protection_orders WHERE protection_order_id=?",
    )
    .bind(p.binding.value("protection_order_id"))
    .fetch_one(p.store.pool())
    .await
    .unwrap();
    assert_eq!(r.get::<String, _>("status"), "requested");
    assert!(
        crate::store::try_get_nullable_string(&r, "exchange_algo_id")
            .unwrap()
            .is_none()
    );
}

// Test-only entry for VM's isolated, consistent copy. No main/background launch.
#[tokio::test]
async fn migration_copy_uses_production_store() {
    sqlx::any::install_default_drivers();
    let supplied = std::env::var_os("RUSTCORE_TEST_MIGRATION_COPY");
    let path = if let Some(raw) = supplied.as_ref() {
        let path = PathBuf::from(raw);
        assert!(path.is_absolute() && path.is_file());
        for ancestor in path.ancestors() {
            assert!(!std::fs::symlink_metadata(ancestor)
                .unwrap()
                .file_type()
                .is_symlink());
        }
        assert!(!path.starts_with("/srv/rustcore/app"));
        let marker: serde_json::Value = serde_json::from_slice(
            &std::fs::read(path.parent().unwrap().join("migration-copy-authority.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(marker["schema"], "protection-dispatch-a-migration-copy-v1");
        assert_eq!(marker["database_path"], path.to_str().unwrap());
        assert_eq!(
            marker["intended_operation"],
            "Store::connect_then_pool_close"
        );
        assert_eq!(marker["original_application_must_remain_untouched"], true);
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let head = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(head.status.success());
        let commit = String::from_utf8(head.stdout).unwrap();
        let commit = commit.trim();
        assert_eq!(marker["candidate"], commit);
        let attempt = marker["attempt_id"].as_str().unwrap();
        assert!((3..=48).contains(&attempt.len()));
        assert!(attempt
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'));
        assert!(attempt.as_bytes()[0].is_ascii_alphanumeric());
        let base = PathBuf::from(format!(
            "/srv/rustcore/candidates/protection-dispatch-{}-{attempt}",
            &commit[..12]
        ));
        assert_eq!(path, base.join("migration-copy/rustcore.copy.db"));
        assert_eq!(repo, base.join("source"));
        assert_eq!(
            marker["original_database_path"],
            "/srv/rustcore/app/data/rustcore.db"
        );
        let marker_path = path.parent().unwrap().join("migration-copy-authority.json");
        assert!(!std::fs::symlink_metadata(marker_path)
            .unwrap()
            .file_type()
            .is_symlink());
        use sha2::Digest;
        let actual = hex::encode(sha2::Sha256::digest(std::fs::read(&path).unwrap()));
        if marker["database_sha256_before"] != actual {
            use sqlx::Connection;
            let mut connection =
                sqlx::AnyConnection::connect(&format!("sqlite://{}?mode=ro", path.display()))
                    .await
                    .unwrap();
            let n = sqlx::query(
                "SELECT COUNT(*) AS n FROM _sqlx_migrations WHERE version=35 AND success=1",
            )
            .fetch_one(&mut connection)
            .await
            .unwrap()
            .get::<i64, _>("n");
            assert_eq!(n, 1, "first migration requires exact copy hash");
            connection.close().await.unwrap();
        }
        path
    } else {
        std::env::temp_dir().join(format!("dispatch-migration-{}.db", ulid::Ulid::new()))
    };
    let url = format!("sqlite://{}", path.display());
    let store = Store::connect(&url).await.unwrap();
    let n = sqlx::query("SELECT COUNT(*) AS n FROM protection_dispatch_attempts")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(n, 0);
    let migrated =
        sqlx::query("SELECT COUNT(*) AS n FROM _sqlx_migrations WHERE version=35 AND success=1")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n");
    assert_eq!(migrated, 1);
    store.pool().close().await;
    let store = Store::connect(&url).await.unwrap();
    assert_eq!(
        sqlx::query("SELECT COUNT(*) AS n FROM protection_dispatch_attempts")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n"),
        0
    );
    store.pool().close().await;
    if supplied.is_none() {
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn dispatch_canceled_future_after_arm_leaves_unresolved_authority() {
    let wire = self::wire();
    let mut p = fixture_permit("dispatch-abort", wire.clone()).await;
    let arrived = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    p.after_arm = Some((arrived.clone(), resume));
    let store = p.store.clone();
    let id = p.attempt_id.clone();
    let task = tokio::spawn(async move {
        p.authorize("dispatch-abort", &wire).await.unwrap();
        panic!("canceled future must not reach wire");
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), arrived.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let row=sqlx::query("SELECT phase,sender_finished,CAST(authorized_post_count AS CHAR) AS n FROM protection_dispatch_attempts WHERE attempt_id=?").bind(id).fetch_one(store.pool()).await.unwrap();
    assert_eq!(row.get::<String, _>("phase"), "armed");
    assert_eq!(row.get::<i64, _>("sender_finished"), 0);
    assert_eq!(crate::store::try_get_i64_text(&row, "n").unwrap(), 1);
}

#[tokio::test]
async fn dispatch_mysql_contract() {
    let Some(scratch) = crate::store::mysql_test_support::MySqlScratch::create(
        "dispatch",
        "dispatch_mysql_contract",
    )
    .await
    else {
        let p = fixture_permit("dispatch-mysql-local-control", wire()).await;
        assert_eq!(count(&p).await, (0, 0, 0));
        println!("DISPATCH_MYSQL_NOT_REQUESTED_LOCAL_CONTROL_ONLY");
        return;
    };
    let store = scratch.connect_store().await;
    for variation in ["DISPATCH-MYSQL", "dispatch-mysql ", "dispatch-mysql"] {
        let wire = self::wire();
        let mut p = fixture_on_store(&store, "dispatch-mysql", wire.clone()).await;
        sqlx::query("UPDATE engine_orders SET credential_id=? WHERE engine_order_id=?")
            .bind(variation)
            .bind(p.binding.value("engine_order_id"))
            .execute(store.pool())
            .await
            .unwrap();
        let result = p.authorize("dispatch-mysql", &wire).await;
        assert_eq!(result.is_ok(), variation == "dispatch-mysql");
        assert_eq!(count(&p).await.0, i64::from(variation == "dispatch-mysql"));
        assert!(
            !fence(&store, &p.attempt_id.to_ascii_uppercase(), p.revision)
                .await
                .unwrap()
        );
        assert!(!fence(&store, &format!("{} ", p.attempt_id), p.revision)
            .await
            .unwrap());
        assert!(fence(&store, &p.attempt_id, p.revision).await.unwrap());
        p.finish("mysql-proof").await.unwrap();
        assert_eq!(count(&p).await.1, 1);
    }
    let wire = self::wire();
    let mut p = fixture_on_store(&store, "dispatch-mysql-race", wire.clone()).await;
    let id = p.attempt_id.clone();
    let store2 = store.clone();
    let (authorized, fenced) = tokio::join!(
        p.authorize("dispatch-mysql-race", &wire),
        fence(&store2, &id, 0)
    );
    assert_ne!(
        authorized.is_ok(),
        fenced.unwrap(),
        "one exact revision wins"
    );
    let state = count(&p).await;
    assert_eq!(state.0, i64::from(authorized.is_ok()));
    let old_owner = p.owner_token.clone();
    let old_id = p.attempt_id.clone();
    drop(p);
    let wire = self::wire();
    let mut ack_permit = fixture_on_store(&store, "dispatch-mysql-ack", wire.clone()).await;
    ack_permit
        .authorize("dispatch-mysql-ack", &wire)
        .await
        .unwrap();
    let ack = serde_json::from_value(
        serde_json::json!({"algoId":900,"clientAlgoId":wire.client_algo_id,"algoStatus":"NEW"}),
    )
    .unwrap();
    let mut tx = store.pool().begin().await.unwrap();
    ack_permit.lock_for_ack(&mut tx).await.unwrap();
    crate::protection::mark_algo_submitted_in_tx(
        &mut tx,
        ack_permit.binding.value("protection_order_id"),
        &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
        &ack,
        5,
    )
    .await
    .unwrap();
    ack_permit
        .finish_in_tx(&mut tx, "acknowledged")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count(&ack_permit).await, (1, 0, 0));
    let row = sqlx::query(
        "SELECT exchange_algo_id,status FROM protection_orders WHERE protection_order_id=?",
    )
    .bind(ack_permit.binding.value("protection_order_id"))
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(
        crate::store::try_get_nullable_string(&row, "exchange_algo_id")
            .unwrap()
            .is_none()
    );
    assert_eq!(row.get::<String, _>("status"), "requested");
    // Exercise the production single-DDL/ledger recovery on real MySQL too.
    sqlx::query("UPDATE _sqlx_migrations SET success=0 WHERE version=35")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    let reopened = scratch.connect_store().await;
    let row = sqlx::query(
        "SELECT owner_token,sender_finished FROM protection_dispatch_attempts WHERE attempt_id=?",
    )
    .bind(old_id)
    .fetch_one(reopened.pool())
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("owner_token"), old_owner);
    assert_eq!(row.get::<i64, _>("sender_finished"), 0);
    reopened.pool().close().await;
    scratch.cleanup().await;
    println!("DISPATCH_MYSQL_REAL_CONTRACT_PASS");
}

#[tokio::test]
async fn dispatch_migration_recovers_ddl_ledger_gap_without_reclaiming_rows() {
    let wire = self::wire();
    let p = fixture_permit("dispatch-migration-gap", wire).await;
    let store = p.store.clone();
    // A successful DDL followed by a lost success stamp is the recoverable
    // shape. The exact compiled migration checksum remains bound in ledger.
    sqlx::query("UPDATE _sqlx_migrations SET success=0 WHERE version=35")
        .execute(store.pool())
        .await
        .unwrap();
    let row = sqlx::query("PRAGMA database_list")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let path = row.get::<String, _>("file");
    store.pool().close().await;
    let reopened = Store::connect(&format!("sqlite://{path}")).await.unwrap();
    let r=sqlx::query("SELECT owner_token,phase,sender_finished FROM protection_dispatch_attempts WHERE attempt_id=?").bind(&p.attempt_id).fetch_one(reopened.pool()).await.unwrap();
    assert_eq!(r.get::<String, _>("owner_token"), p.owner_token);
    assert_eq!(r.get::<String, _>("phase"), "prepared");
    assert_eq!(r.get::<i64, _>("sender_finished"), 0);
    reopened.pool().close().await;
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test]
async fn dispatch_l4_fake_requires_the_same_last_send_authority() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    struct Env(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Env {
        fn drop(&mut self) {
            for (name, old) in self.0.drain(..) {
                match old {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
    let mut env = Env(Vec::new());
    let fake_path = std::env::temp_dir().join(format!("dispatch-l4-{}.db", ulid::Ulid::new()));
    sqlx::any::install_default_drivers();
    let fake = Store::connect(&format!("sqlite://{}", fake_path.display()))
        .await
        .unwrap();
    for sql in ["CREATE TABLE request_log(method TEXT,path TEXT,order_type TEXT)","CREATE TABLE control(key TEXT PRIMARY KEY,value BIGINT)","INSERT INTO control VALUES ('next_algo_id',0),('fail_next_sl',0)","CREATE TABLE algo_orders(algo_id BIGINT,client_algo_id TEXT,algo_status TEXT,actual_order_id TEXT,symbol TEXT,order_type TEXT,is_open BIGINT,update_time BIGINT)"] {sqlx::query(sql).execute(fake.pool()).await.unwrap();}
    for (name, value) in [
        ("L4_FAULT_INJECTION_ENABLED", "1".to_owned()),
        (
            "L4_FAKE_EXCHANGE_DB",
            fake_path.to_str().unwrap().to_owned(),
        ),
        (
            "L4_FILE_RPC_DIR",
            std::env::temp_dir().to_str().unwrap().to_owned(),
        ),
    ] {
        env.0.push((name, std::env::var_os(name)));
        std::env::set_var(name, value);
    }
    let limiter = crate::rate_limiter::CredentialRateLimiter::shared(
        crate::rate_limiter::TokenBucketConfig::for_test(10.0, 10.0),
    );
    for allowed in [false, true] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-l4", wire.clone()).await;
        if !allowed {
            assert!(fence(&p.store, &p.attempt_id, 0).await.unwrap());
        }
        let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
            "dispatch-l4",
            Vec::new(),
        );
        let result = client
            .place_algo_conditional(
                &limiter,
                &mut p,
                crate::binance_futures::BinanceAlgoConditionalOrderType::StopMarket,
                &wire.symbol,
                &wire.side,
                &wire.trigger_price,
                wire.quantity.as_deref(),
                true,
                false,
                &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
            )
            .await;
        assert_eq!(result.is_ok(), allowed);
        assert_eq!(count(&p).await.0, i64::from(allowed));
        for table in ["request_log", "algo_orders"] {
            assert_eq!(
                sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
                    .fetch_one(fake.pool())
                    .await
                    .unwrap()
                    .get::<i64, _>("n"),
                i64::from(allowed)
            );
        }
    }
    drop(env);
    fake.pool().close().await;
    std::fs::remove_file(fake_path).unwrap();
}

#[tokio::test]
async fn dispatch_persisted_owner_or_counter_cannot_restore_authority() {
    for field in [
        "owner_token",
        "engine_order_id",
        "protection_order_id",
        "revision",
        "authorized_post_count",
    ] {
        let wire = self::wire();
        let mut p = fixture_permit("dispatch-tamper", wire.clone()).await;
        let value = if matches!(
            field,
            "owner_token" | "engine_order_id" | "protection_order_id"
        ) {
            "foreign"
        } else {
            "1"
        };
        sqlx::query(&format!(
            "UPDATE protection_dispatch_attempts SET {field}=? WHERE attempt_id=?"
        ))
        .bind(value)
        .bind(&p.attempt_id)
        .execute(p.store.pool())
        .await
        .unwrap();
        let error = p.authorize("dispatch-tamper", &wire).await.unwrap_err();
        assert!(if field == "authorized_post_count" {
            matches!(p.disposition(&error), FailureDisposition::MayHaveSent)
        } else {
            matches!(
                p.disposition(&error),
                FailureDisposition::LocalProtectionFailure
            )
        });
        assert_eq!(
            count(&p).await.0,
            i64::from(field == "authorized_post_count")
        );
    }
}

#[test]
fn dispatch_actual_signed_parameters_are_bound_without_defaulting() {
    let wire = self::wire();
    let params = vec![
        ("symbol", "BTCUSDT"),
        ("side", "SELL"),
        ("algoType", "CONDITIONAL"),
        ("type", "STOP_MARKET"),
        ("triggerPrice", "90"),
        ("workingType", "MARK_PRICE"),
        ("priceProtect", "true"),
        ("quantity", "1"),
        ("reduceOnly", "true"),
        ("clientAlgoId", wire.client_algo_id.as_str()),
        ("timestamp", "1"),
        ("recvWindow", "5000"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect::<Vec<_>>();
    wire.validate_params(&params).unwrap();
    for changed in [
        "symbol",
        "side",
        "algoType",
        "type",
        "triggerPrice",
        "workingType",
        "priceProtect",
        "quantity",
        "reduceOnly",
        "clientAlgoId",
    ] {
        let mut mutant = params.clone();
        mutant.iter_mut().find(|(k, _)| k == changed).unwrap().1 = "changed".to_owned();
        assert!(wire.validate_params(&mutant).is_err(), "{changed}");
    }
}

#[tokio::test]
async fn dispatch_duplicate_prepare_cannot_create_a_second_sender() {
    let wire = wire();
    let p = fixture_permit("dispatch-duplicate", wire.clone()).await;
    let mut tx = p.store.pool().begin().await.unwrap();
    assert!(prepare_in_tx(
        &p.store,
        &mut tx,
        p.binding.value("engine_order_id"),
        p.binding.value("protection_order_id"),
        wire,
        2
    )
    .await
    .is_err());
    tx.rollback().await.unwrap();
    assert_eq!(count(&p).await, (0, 0, 0));
    assert_eq!(
        sqlx::query("SELECT COUNT(*) AS n FROM protection_dispatch_attempts")
            .fetch_one(p.store.pool())
            .await
            .unwrap()
            .get::<i64, _>("n"),
        1
    );
}

#[tokio::test]
async fn dispatch_canceled_future_before_arm_leaves_prepared_unresolved() {
    let wire = wire();
    let mut p = fixture_permit("dispatch-abort-before", wire.clone()).await;
    let client = crate::binance_futures::BinanceFuturesClient::new_with_mock_responses(
        "dispatch-abort-before",
        Vec::new(),
    );
    let held = crate::binance_futures::transport_gate_for_scope(&client.egress_scope())
        .lock_owned()
        .await;
    let store = p.store.clone();
    let id = p.attempt_id.clone();
    let observed = client.clone();
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let entered = started.clone();
    let task = tokio::spawn(async move {
        entered.notify_one();
        client
            .place_algo_conditional(
                &crate::rate_limiter::CredentialRateLimiter::shared(
                    crate::rate_limiter::TokenBucketConfig::for_test(10.0, 10.0),
                ),
                &mut p,
                crate::binance_futures::BinanceAlgoConditionalOrderType::StopMarket,
                &wire.symbol,
                &wire.side,
                &wire.trigger_price,
                wire.quantity.as_deref(),
                true,
                false,
                &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
            )
            .await
    });
    started.notified().await;
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(held);
    assert!(observed.recorded_requests().is_empty());
    let row=sqlx::query("SELECT phase,sender_finished,CAST(authorized_post_count AS CHAR) AS n FROM protection_dispatch_attempts WHERE attempt_id=?").bind(id).fetch_one(store.pool()).await.unwrap();
    assert_eq!(row.get::<String, _>("phase"), "prepared");
    assert_eq!(row.get::<i64, _>("sender_finished"), 0);
    assert_eq!(crate::store::try_get_i64_text(&row, "n").unwrap(), 0);
}

// Bounded state-corruption evidence. Uses production dispatch authorization,
// ACK locks and identity checks, but a synthetic remote ACK, not an HTTP race.
#[tokio::test]
async fn k05_b_ack_with_dispatch_and_identity_guards() {
    let mut gaps = Vec::new();
    for backend in crate::k05_b_tests::BACKENDS {
        let Some(fixture) = crate::k05_b_tests::Fixture::new(backend, "k05back").await else {
            continue;
        };
        let store = &fixture.store;
        for (i, state) in [
            "pending_risk",
            "pending_submit",
            "submitted",
            "acked",
            "partial_filled",
            "filled",
            "expired",
            "rejected",
            "failed",
            "canceled",
            "unknown",
            "confirmed",
            "triggering",
            "triggered",
            "CANCELED",
            "canceled ",
            "mystery",
        ]
        .iter()
        .enumerate()
        {
            let wire = self::wire();
            let credential = format!("k05-ack-{i}");
            let mut p = fixture_on_store(&store, &credential, wire.clone()).await;
            p.authorize(&credential, &wire).await.unwrap();
            assert_eq!(count(&p).await, (1, 0, 0));
            let algo_id = 9000 + i as i64;
            if *state == "canceled" {
                let truth=crate::protection::AlgoOrderTruth::from_json(serde_json::json!({"algoId":algo_id,"clientAlgoId":wire.client_algo_id,"algoStatus":"CANCELED","updateTime":2})).unwrap();
                crate::protection::apply_algo_truth_for_protection(
                    &store,
                    &credential,
                    p.binding.value("protection_order_id"),
                    &truth,
                    crate::protection::AlgoTruthSource::WsAlgoUpdate,
                    "k05-canonical-control",
                    2,
                )
                .await
                .unwrap();
            } else if *state != "pending_submit" {
                // Deliberate corruption after successful authorization, not a normal writer.
                sqlx::query("UPDATE engine_orders SET status=? WHERE engine_order_id=?")
                    .bind(state)
                    .bind(p.binding.value("engine_order_id"))
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
            let ack=serde_json::from_value(serde_json::json!({"algoId":algo_id,"clientAlgoId":wire.client_algo_id,"algoStatus":"NEW","actualOrderId":"88002"})).unwrap();
            let mut tx = store.pool().begin().await.unwrap();
            p.lock_for_ack(&mut tx).await.unwrap();
            let outcome = crate::protection::mark_algo_submitted_in_tx(
                &mut tx,
                p.binding.value("protection_order_id"),
                &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
                &ack,
                3,
            )
            .await
            .unwrap();
            p.finish_in_tx(&mut tx, "acknowledged").await.unwrap();
            tx.commit().await.unwrap();
            p.acknowledge_committed_finish();
            let row=sqlx::query("SELECT eo.status AS engine_status,po.status AS protection_status,po.exchange_algo_id,po.client_algo_id FROM engine_orders eo JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id WHERE eo.engine_order_id=?").bind(p.binding.value("engine_order_id")).fetch_one(store.pool()).await.unwrap();
            let actual: String = row.get("engine_status");
            let stored_algo: String = row.get("exchange_algo_id");
            let stored_client: String = row.get("client_algo_id");
            let protection_status: String = row.get("protection_status");
            let expected = if matches!(
                *state,
                "pending_risk" | "pending_submit" | "submitted" | "acked" | "unknown"
            ) {
                "acked"
            } else {
                state
            };
            if actual != expected {
                gaps.push(format!("{backend}/{state:?} was normalized to {actual:?}"));
            }
            let unresolved = matches!(*state, "CANCELED" | "canceled " | "mystery");
            assert_eq!(outcome.algo_id.to_string(), algo_id.to_string());
            assert_eq!(
                outcome.unresolved_local_state.as_deref(),
                if unresolved { Some(*state) } else { None }
            );
            let actual_id: String = sqlx::query_scalar(
                "SELECT exchange_order_id FROM engine_orders WHERE engine_order_id=?",
            )
            .bind(p.binding.value("engine_order_id"))
            .fetch_one(store.pool())
            .await
            .unwrap();
            assert_eq!(actual_id, "88002");
            assert_eq!(stored_algo, algo_id.to_string());
            assert_eq!(stored_client, wire.client_algo_id);
            assert_eq!(count(&p).await, (1, 1, 1));
            assert!(
                p.authorize(&credential, &wire).await.is_err(),
                "finished sender must not acquire duplicate permission"
            );
            eprintln!(
                "K05_B {}",
                serde_json::json!({"surface":"actual-ack-with-dispatch","origin":if *state=="canceled" {"production Algo truth writer with synthetic exchange truth"} else if *state=="pending_submit" {"canonical fixture"} else if unresolved {"manual corruption after authorize"} else {"manual injection of a known canonical engine state after authorize"},"backend":backend,"unresolved":unresolved,"state_before":state,"state_after":actual,"protection_status":protection_status,"exchange_algo_id":stored_algo,"identity_preserved":true,"authorized_post_count":1,"dispatch_closed":true,"sender_finished":true,"rearm_denied":true})
            );
        }
        // Corruption before dispatch is rejected by the real exact submission guard.
        let wire = self::wire();
        let mut denied = fixture_on_store(&store, "k05-ack-deny", wire.clone()).await;
        sqlx::query("UPDATE engine_orders SET status='PENDING_SUBMIT' WHERE engine_order_id=?")
            .bind(denied.binding.value("engine_order_id"))
            .execute(store.pool())
            .await
            .unwrap();
        assert!(denied
            .authorize("k05-ack-deny", &wire)
            .await
            .unwrap_err()
            .to_string()
            .contains("permitted submission state"));
        assert_eq!(count(&denied).await, (0, 0, 0));
        denied.finish("test_pre_send_denied").await.unwrap();
        // A bad remote client identity does not commit its purported ACK.
        let wire = self::wire();
        let mut rejected = fixture_on_store(&store, "k05-ack-identity", wire.clone()).await;
        rejected.authorize("k05-ack-identity", &wire).await.unwrap();
        let ack = serde_json::from_value(
            serde_json::json!({"algoId":9999,"clientAlgoId":"different-client","algoStatus":"NEW"}),
        )
        .unwrap();
        let mut tx = store.pool().begin().await.unwrap();
        rejected.lock_for_ack(&mut tx).await.unwrap();
        assert!(crate::protection::mark_algo_submitted_in_tx(
            &mut tx,
            rejected.binding.value("protection_order_id"),
            &crate::binance_futures::BinanceClientAlgoId::new(wire.client_algo_id.clone()),
            &ack,
            3
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("different clientAlgoId"));
        tx.rollback().await.unwrap();
        let row = sqlx::query(
            "SELECT status,exchange_algo_id FROM protection_orders WHERE protection_order_id=?",
        )
        .bind(rejected.binding.value("protection_order_id"))
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "requested");
        assert!(
            crate::store::try_get_nullable_string(&row, "exchange_algo_id")
                .unwrap()
                .is_none()
        );
        rejected.finish("test_bad_remote_identity").await.unwrap();
        eprintln!(
            "K05_B {}",
            serde_json::json!({"surface":"ack-negative-controls","dirty_pre_send_denied":true,"authorized_post_count":0,"bad_remote_identity_denied":true,"no_remote_identity_committed":true})
        );
        fixture.cleanup().await;
    }
    assert!(gaps.is_empty(), "ACK state byte contract: {gaps:?}");
}
