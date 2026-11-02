//! 0042: real legacy rows, ledger checksums and crash boundaries; no trading authority.
use super::*;
use std::borrow::Cow;

async fn before_42(pool: &Pool<Any>) {
    let mut migrator = embedded_migrator();
    migrator.migrations = Cow::Owned(
        migrator
            .iter()
            .filter(|m| m.version < 42)
            .cloned()
            .collect(),
    );
    migrator.run(pool).await.unwrap();
}

async fn seed_rows(pool: &Pool<Any>) {
    // Explicit historical-row fixtures, not claims of a production entry.
    for (id, kind, role) in [
        ("historical-entry", "limit", "entry"),
        ("historical-sl", "stop_market", "protection_sl"),
    ] {
        sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,idempotency_key,exchange,market_kind,symbol_key,action,side,order_type,quantity,status,created_at_ms,updated_at_ms,order_role) VALUES (?,'paper','historical-bot',?,'binance','futures','binance|futures|BTC|USDT|USDT','open_long','BUY',?,'7','accepted',1700000000000,1700000000000,?)")
            .bind(id).bind(id).bind(kind).bind(role).execute(pool).await.unwrap();
    }
}

async fn row_bytes(pool: &Pool<Any>) -> Vec<Vec<Option<String>>> {
    // Every persisted field, with numeric values rendered losslessly for Any.
    let names = [
        "engine_order_id",
        "credential_id",
        "bot_uid",
        "run_id",
        "cycle_id",
        "idempotency_key",
        "client_order_id_short",
        "exchange_order_id",
        "exchange",
        "market_kind",
        "symbol_key",
        "action",
        "side",
        "order_type",
        "price",
        "quantity",
        "reduce_only",
        "status",
        "created_at_ms",
        "updated_at_ms",
        "risk_verdict_id",
        "position_id",
        "order_role",
    ];
    sqlx::query("SELECT engine_order_id,credential_id,bot_uid,run_id,cycle_id,idempotency_key,client_order_id_short,exchange_order_id,exchange,market_kind,symbol_key,action,side,order_type,price,quantity,CAST(reduce_only AS CHAR) AS reduce_only,status,CAST(created_at_ms AS CHAR) AS created_at_ms,CAST(updated_at_ms AS CHAR) AS updated_at_ms,risk_verdict_id,position_id,order_role FROM engine_orders ORDER BY engine_order_id")
        .fetch_all(pool).await.unwrap().iter().map(|row| names.iter().map(|name| try_get_text(row,name).unwrap()).collect()).collect()
}

async fn old_checksums(pool: &Pool<Any>) -> Vec<(i64, Vec<u8>)> {
    sqlx::query("SELECT version,checksum FROM _sqlx_migrations WHERE version < 42 ORDER BY version")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get("version"), r.get("checksum")))
        .collect()
}

async fn index_bytes(pool: &Pool<Any>, kind: DbKind) -> Vec<Option<String>> {
    let query = if kind == DbKind::MySql {
        "SELECT CONCAT(index_name,':',seq_in_index,':',column_name,':',non_unique,':',index_type,':',IFNULL(sub_part,-1),':',IFNULL(collation,'')) AS definition FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='engine_orders' ORDER BY index_name,seq_in_index"
    } else {
        "SELECT name || ':' || IFNULL(sql,'') AS definition FROM sqlite_master WHERE type='index' AND tbl_name='engine_orders' ORDER BY name"
    };
    sqlx::query(query)
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| try_get_text(r, "definition").unwrap())
        .collect()
}

async fn stamp(pool: &Pool<Any>, corrupt_checksum: bool) {
    let migrator = embedded_migrator();
    let migration = migrator.iter().find(|m| m.version == 42).unwrap();
    let mut checksum = migration.checksum.to_vec();
    if corrupt_checksum {
        checksum[0] ^= 0xff;
    }
    sqlx::query("INSERT INTO _sqlx_migrations (version,description,success,checksum,execution_time) VALUES (42,?,0,?,0)")
        .bind(migration.description.as_ref()).bind(checksum).execute(pool).await.unwrap();
}

async fn state_42(pool: &Pool<Any>) -> Vec<String> {
    sqlx::query("SELECT CAST(success AS CHAR) AS success FROM _sqlx_migrations WHERE version=42")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get("success"))
        .collect()
}

async fn mysql_legacy(label: &str) -> (mysql_test_support::MySqlScratch, Pool<Any>) {
    let scratch = mysql_test_support::MySqlScratch::create("b3close", label)
        .await
        .expect("owned MySQL required");
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(scratch.database_url())
        .await
        .unwrap();
    before_42(&pool).await;
    seed_rows(&pool).await;
    (scratch, pool)
}

#[tokio::test]
#[ignore = "requires owned MySQL; absence is not PASS"]
async fn mysql_upgrade_and_repeat_preserve_rows_indexes_and_checksums() {
    let (scratch, pool) = mysql_legacy("width-clean").await;
    let rows = row_bytes(&pool).await;
    let indexes = index_bytes(&pool, DbKind::MySql).await;
    let checksums = old_checksums(&pool).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        probe_ddl_effect(&pool, DbKind::MySql, &DdlEffect::MySqlOrderTypeWidth)
            .await
            .unwrap(),
        DdlEffectState::Absent
    );
    pool.close().await;
    for _ in 0..2 {
        let store = Store::connect(scratch.database_url()).await.unwrap();
        assert_eq!(
            probe_ddl_effect(store.pool(), DbKind::MySql, &DdlEffect::MySqlOrderTypeWidth)
                .await
                .unwrap(),
            DdlEffectState::Present
        );
        assert_eq!(row_bytes(store.pool()).await, rows);
        assert_eq!(index_bytes(store.pool(), DbKind::MySql).await, indexes);
        assert_eq!(old_checksums(store.pool()).await, checksums);
        assert_eq!(state_42(store.pool()).await, vec!["1"]);
        store.pool().close().await;
    }
    scratch.cleanup().await;
}

#[tokio::test]
#[ignore = "requires owned MySQL; absence is not PASS"]
async fn mysql_dirty_before_and_after_ddl_reenter_without_losing_rows() {
    for after in [false, true] {
        let (scratch, pool) =
            mysql_legacy(if after { "width-after" } else { "width-before" }).await;
        let rows = row_bytes(&pool).await;
        let indexes = index_bytes(&pool, DbKind::MySql).await;
        let checksums = old_checksums(&pool).await;
        // Controlled crash-boundary fixture: exact embedded checksum, dirty row,
        // and optionally the committed DDL. Recovery itself uses production Store.
        stamp(&pool, false).await;
        if after {
            sqlx::raw_sql(include_str!(
                "../migrations/0042_engine_order_type_width.sql"
            ))
            .execute(&pool)
            .await
            .unwrap();
        }
        assert_eq!(state_42(&pool).await, vec!["0"]);
        pool.close().await;
        let store = Store::connect(scratch.database_url()).await.unwrap();
        assert_eq!(row_bytes(store.pool()).await, rows);
        assert_eq!(index_bytes(store.pool(), DbKind::MySql).await, indexes);
        assert_eq!(old_checksums(store.pool()).await, checksums);
        assert_eq!(state_42(store.pool()).await, vec!["1"]);
        assert_eq!(
            probe_ddl_effect(store.pool(), DbKind::MySql, &DdlEffect::MySqlOrderTypeWidth)
                .await
                .unwrap(),
            DdlEffectState::Present
        );
        store.pool().close().await;
        scratch.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "requires owned MySQL; absence is not PASS"]
async fn mysql_wrong_shape_or_checksum_never_stamps_or_changes_rows() {
    let (scratch, pool) = mysql_legacy("width-refusal").await;
    let rows = row_bytes(&pool).await;
    let checksums = old_checksums(&pool).await;
    sqlx::query("ALTER TABLE engine_orders MODIFY COLUMN order_type VARCHAR(16) NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert!(Store::connect(scratch.database_url())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("0042 preflight"));
    assert_eq!(state_42(&pool).await, Vec::<String>::new());
    assert_eq!(row_bytes(&pool).await, rows);
    // Restore only this owned test's injected wrong shape before the checksum case.
    sqlx::query("ALTER TABLE engine_orders MODIFY COLUMN order_type VARCHAR(16) NOT NULL")
        .execute(&pool)
        .await
        .unwrap();
    stamp(&pool, true).await;
    let error = Store::connect(scratch.database_url()).await.err().unwrap();
    assert!(format!("{error:#}").contains("checksum"));
    assert_eq!(state_42(&pool).await, vec!["0"]);
    assert_eq!(row_bytes(&pool).await, rows);
    let migrator = embedded_migrator();
    let checksum = migrator
        .iter()
        .find(|m| m.version == 42)
        .unwrap()
        .checksum
        .to_vec();
    sqlx::query("UPDATE _sqlx_migrations SET checksum=? WHERE version=42")
        .bind(checksum)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE engine_orders MODIFY COLUMN order_type VARCHAR(24) NOT NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert!(Store::connect(scratch.database_url())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("0042 preflight"));
    assert_eq!(state_42(&pool).await, vec!["0"]);
    assert_eq!(row_bytes(&pool).await, rows);
    assert_eq!(old_checksums(&pool).await, checksums);
    pool.close().await;
    scratch.cleanup().await;
}

#[tokio::test]
async fn sqlite_fresh_and_upgrade_keep_declared_width_and_all_old_bytes() {
    sqlx::any::install_default_drivers();
    for old in [false, true] {
        let file = std::env::temp_dir().join(format!("b3-width-{}.db", ulid::Ulid::new()));
        std::fs::File::create(&file).unwrap();
        let url = format!("sqlite://{}", file.display());
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        if old {
            before_42(&pool).await;
        } else {
            embedded_migrator().run(&pool).await.unwrap();
        }
        seed_rows(&pool).await;
        let rows = row_bytes(&pool).await;
        let indexes = index_bytes(&pool, DbKind::Sqlite).await;
        let checksums = old_checksums(&pool).await;
        pool.close().await;
        for _ in 0..2 {
            let store = Store::connect(&url).await.unwrap();
            let columns = observed_columns(store.pool(), DbKind::Sqlite, "engine_orders")
                .await
                .unwrap();
            let column = columns.iter().find(|c| c.name == "order_type").unwrap();
            assert_eq!(column.max_length, Some(16));
            assert!(!column.nullable);
            assert_eq!(row_bytes(store.pool()).await, rows);
            assert_eq!(index_bytes(store.pool(), DbKind::Sqlite).await, indexes);
            assert_eq!(old_checksums(store.pool()).await, checksums);
            assert_eq!(state_42(store.pool()).await, vec!["1"]);
            store.pool().close().await;
        }
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", file.display()));
        }
    }
}
