//! SQLite-backed Binance truth stub for socket-free L4 process tests.
//!
//! The production binary cannot compile this module without the
//! `l4-fault-injection` feature. Even feature builds require the explicit L4
//! enable flag and absolute filesystem paths before this adapter is selected.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, Row, SqlitePool};

use crate::binance_futures::{
    BinanceAlgoId, BinanceAlgoOrderResponse, BinanceClientAlgoId, BinanceOrderAck, BinanceOrderId,
};

pub(crate) fn configured_db() -> anyhow::Result<Option<PathBuf>> {
    let Some(raw) = std::env::var("L4_FAKE_EXCHANGE_DB")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    crate::fault_injection::validate_file_transport()?;
    Ok(Some(PathBuf::from(raw.trim())))
}

async fn connect(path: &PathBuf) -> anyhow::Result<SqlitePool> {
    let options = path
        .to_string_lossy()
        .parse::<SqliteConnectOptions>()
        .context("parse L4 fake exchange SQLite path")?
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5))
        .disable_statement_logging();
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("connect L4 fake exchange SQLite")
}

async fn log(pool: &SqlitePool, method: &str, path: &str, order_type: &str) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO request_log(method, path, order_type) VALUES (?, ?, ?)")
        .bind(method)
        .bind(path)
        .bind(order_type)
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn create_listen_key(path: &PathBuf) -> anyhow::Result<String> {
    let pool = connect(path).await?;
    log(&pool, "POST", "/fapi/v1/listenKey", "").await?;
    pool.close().await;
    Ok("local-l4-listen-key".to_string())
}

pub(crate) async fn keepalive_listen_key(path: &PathBuf) -> anyhow::Result<()> {
    let pool = connect(path).await?;
    log(&pool, "PUT", "/fapi/v1/listenKey", "").await?;
    pool.close().await;
    Ok(())
}

pub(crate) async fn place_order(
    path: &PathBuf,
    order_type: &str,
    symbol: &str,
    side: &str,
    quantity: &str,
    reduce_only: bool,
    client_order_id: &str,
) -> anyhow::Result<BinanceOrderAck> {
    let pool = connect(path).await?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO request_log(method, path, order_type) VALUES ('POST', '/fapi/v1/order', ?)",
    )
    .bind(order_type)
    .execute(&mut *tx)
    .await?;
    if order_type == "STOP_MARKET" {
        let fail = sqlx::query("SELECT value FROM control WHERE key='fail_next_sl'")
            .fetch_one(&mut *tx)
            .await?
            .get::<i64, _>("value");
        if fail == 1 {
            sqlx::query("UPDATE control SET value=0 WHERE key='fail_next_sl'")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            pool.close().await;
            return Err(anyhow!("binance http 500: injected SL failure"));
        }
    }
    let next = sqlx::query("SELECT value FROM control WHERE key='next_order_id'")
        .fetch_one(&mut *tx)
        .await?
        .get::<i64, _>("value")
        + 1;
    sqlx::query("UPDATE control SET value=? WHERE key='next_order_id'")
        .bind(next)
        .execute(&mut *tx)
        .await?;
    let is_flatten = order_type == "MARKET" && reduce_only;
    let status = if is_flatten { "FILLED" } else { "NEW" };
    let executed_qty = if is_flatten { quantity } else { "0" };
    if is_flatten {
        sqlx::query(
            "INSERT INTO positions(symbol, quantity) VALUES (?, '0') ON CONFLICT(symbol) DO UPDATE SET quantity='0'",
        )
        .bind(symbol)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query(
            r#"
            INSERT INTO orders(
              order_id, client_order_id, status, symbol, side, orig_qty,
              executed_qty, is_open
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            "#,
        )
        .bind(next)
        .bind(client_order_id)
        .bind(status)
        .bind(symbol)
        .bind(side)
        .bind(quantity)
        .bind(executed_qty)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    pool.close().await;
    Ok(BinanceOrderAck {
        order_id: BinanceOrderId::new(next),
        client_order_id: client_order_id.to_string(),
        status: status.to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        orig_qty: quantity.to_string(),
        executed_qty: executed_qty.to_string(),
    })
}

#[allow(dead_code)]
pub(crate) async fn place_algo_conditional(
    path: &PathBuf,
    permit: &mut crate::protection_dispatch::ProtectionDispatchPermit,
    credential: &str,
    wire: &crate::protection_dispatch::AlgoWire,
    order_type: &str,
    symbol: &str,
    client_algo_id: &BinanceClientAlgoId,
) -> anyhow::Result<BinanceAlgoOrderResponse> {
    let pool = connect(path).await?;
    let mut tx = pool.begin().await?;
    if wire.order_type != order_type
        || wire.symbol != symbol
        || wire.client_algo_id != client_algo_id.as_str()
    {
        return Err(anyhow!(
            "L4 actual protection parameters differ from managed binding"
        ));
    }
    permit.authorize(credential, wire).await?;
    sqlx::query(
        "INSERT INTO request_log(method, path, order_type) VALUES ('POST', '/fapi/v1/algoOrder', ?)",
    )
    .bind(order_type)
    .execute(&mut *tx)
    .await?;
    if order_type == "STOP_MARKET" {
        let fail = sqlx::query("SELECT value FROM control WHERE key='fail_next_sl'")
            .fetch_one(&mut *tx)
            .await?
            .get::<i64, _>("value");
        if fail == 1 {
            sqlx::query("UPDATE control SET value=0 WHERE key='fail_next_sl'")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            pool.close().await;
            return Err(anyhow!("binance http 500: injected SL failure"));
        }
    }
    let next = sqlx::query("SELECT value FROM control WHERE key='next_algo_id'")
        .fetch_one(&mut *tx)
        .await?
        .get::<i64, _>("value")
        + 1;
    sqlx::query("UPDATE control SET value=? WHERE key='next_algo_id'")
        .bind(next)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO algo_orders(
          algo_id, client_algo_id, algo_status, actual_order_id,
          symbol, order_type, is_open, update_time
        ) VALUES (?, ?, 'NEW', NULL, ?, ?, 1, 1)
        "#,
    )
    .bind(next)
    .bind(client_algo_id.as_str())
    .bind(symbol)
    .bind(order_type)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    pool.close().await;
    Ok(BinanceAlgoOrderResponse {
        algo_id: Some(BinanceAlgoId::new(next)),
        client_algo_id: Some(client_algo_id.clone()),
        algo_status: Some("NEW".to_string()),
        actual_order_id: None,
        additional_fields: serde_json::Map::new(),
    })
}

#[allow(dead_code)]
pub(crate) async fn get_algo_order(
    path: &PathBuf,
    algo_id: Option<&BinanceAlgoId>,
    client_algo_id: Option<&BinanceClientAlgoId>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v1/algoOrder", "").await?;
    let row = if let Some(client_algo_id) = client_algo_id {
        sqlx::query("SELECT * FROM algo_orders WHERE client_algo_id=? LIMIT 1")
            .bind(client_algo_id.as_str())
            .fetch_optional(&pool)
            .await?
    } else if let Some(algo_id) = algo_id {
        sqlx::query("SELECT * FROM algo_orders WHERE algo_id=? LIMIT 1")
            .bind(algo_id.as_i64())
            .fetch_optional(&pool)
            .await?
    } else {
        None
    };
    pool.close().await;
    let Some(row) = row else {
        return Err(anyhow!(
            "binance http 400: {{\"code\":-2013,\"msg\":\"Order does not exist.\"}}"
        ));
    };
    Ok(algo_order_json(&row))
}

#[allow(dead_code)]
pub(crate) async fn open_algo_orders(
    path: &PathBuf,
    symbol: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v1/openAlgoOrders", "").await?;
    let rows = if let Some(symbol) = symbol.filter(|value| !value.trim().is_empty()) {
        sqlx::query("SELECT * FROM algo_orders WHERE is_open=1 AND symbol=? ORDER BY algo_id")
            .bind(symbol)
            .fetch_all(&pool)
            .await?
    } else {
        sqlx::query("SELECT * FROM algo_orders WHERE is_open=1 ORDER BY algo_id")
            .fetch_all(&pool)
            .await?
    };
    pool.close().await;
    Ok(serde_json::Value::Array(
        rows.iter().map(algo_order_json).collect(),
    ))
}

#[allow(dead_code)]
pub(crate) async fn cancel_algo_order(
    path: &PathBuf,
    algo_id: Option<&BinanceAlgoId>,
    client_algo_id: Option<&BinanceClientAlgoId>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "DELETE", "/fapi/v1/algoOrder", "").await?;
    let row = if let Some(algo_id) = algo_id {
        sqlx::query("SELECT algo_id, client_algo_id FROM algo_orders WHERE algo_id=? LIMIT 1")
            .bind(algo_id.as_i64())
            .fetch_optional(&pool)
            .await?
    } else if let Some(client_algo_id) = client_algo_id {
        sqlx::query(
            "SELECT algo_id, client_algo_id FROM algo_orders WHERE client_algo_id=? LIMIT 1",
        )
        .bind(client_algo_id.as_str())
        .fetch_optional(&pool)
        .await?
    } else {
        None
    };
    let Some(row) = row else {
        pool.close().await;
        return Err(anyhow!(
            "binance http 400: {{\"code\":-2013,\"msg\":\"Order does not exist.\"}}"
        ));
    };
    let returned_algo_id = row.get::<i64, _>("algo_id");
    let returned_client_algo_id = row.get::<String, _>("client_algo_id");
    sqlx::query("UPDATE algo_orders SET is_open=0, algo_status='CANCELED' WHERE algo_id=?")
        .bind(returned_algo_id)
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(serde_json::json!({
        "algoId": returned_algo_id,
        "clientAlgoId": returned_client_algo_id,
        "code": 200,
        "msg": "success"
    }))
}

fn algo_order_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    let actual_order_id = row
        .try_get::<Option<String>, _>("actual_order_id")
        .ok()
        .flatten();
    serde_json::json!({
        "algoId": row.get::<i64, _>("algo_id"),
        "clientAlgoId": row.get::<String, _>("client_algo_id"),
        "algoType": "CONDITIONAL",
        "algoStatus": row.get::<String, _>("algo_status"),
        "actualOrderId": actual_order_id.unwrap_or_default(),
        "symbol": row.get::<String, _>("symbol"),
        "orderType": row.get::<String, _>("order_type"),
        "updateTime": row.get::<i64, _>("update_time"),
    })
}

pub(crate) async fn get_order(
    path: &PathBuf,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v1/order", "").await?;
    let row = if let Some(client_id) = client_order_id.filter(|value| !value.trim().is_empty()) {
        sqlx::query("SELECT * FROM orders WHERE client_order_id=? ORDER BY order_id DESC LIMIT 1")
            .bind(client_id)
            .fetch_optional(&pool)
            .await?
    } else if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        sqlx::query("SELECT * FROM orders WHERE order_id=? LIMIT 1")
            .bind(order_id.parse::<i64>().unwrap_or(-1))
            .fetch_optional(&pool)
            .await?
    } else {
        None
    };
    pool.close().await;
    let Some(row) = row else {
        return Err(anyhow!(
            "binance http 400: {{\"code\":-2013,\"msg\":\"Order does not exist.\"}}"
        ));
    };
    Ok(order_json(&row))
}

pub(crate) async fn cancel_order(
    path: &PathBuf,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "DELETE", "/fapi/v1/order", "").await?;
    if let Some(client_id) = client_order_id.filter(|value| !value.trim().is_empty()) {
        sqlx::query("UPDATE orders SET is_open=0, status='CANCELED' WHERE client_order_id=?")
            .bind(client_id)
            .execute(&pool)
            .await?;
    } else if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        sqlx::query("UPDATE orders SET is_open=0, status='CANCELED' WHERE order_id=?")
            .bind(order_id.parse::<i64>().unwrap_or(-1))
            .execute(&pool)
            .await?;
    }
    pool.close().await;
    Ok(serde_json::json!({}))
}

fn order_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    serde_json::json!({
        "orderId": row.get::<i64, _>("order_id"),
        "clientOrderId": row.get::<String, _>("client_order_id"),
        "status": row.get::<String, _>("status"),
        "symbol": row.get::<String, _>("symbol"),
        "side": row.get::<String, _>("side"),
        "origQty": row.get::<String, _>("orig_qty"),
        "executedQty": row.get::<String, _>("executed_qty"),
    })
}

pub(crate) async fn open_orders(
    path: &PathBuf,
    symbol: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v1/openOrders", "").await?;
    let rows = if let Some(symbol) = symbol.filter(|value| !value.trim().is_empty()) {
        sqlx::query("SELECT * FROM orders WHERE is_open=1 AND symbol=? ORDER BY order_id")
            .bind(symbol)
            .fetch_all(&pool)
            .await?
    } else {
        sqlx::query("SELECT * FROM orders WHERE is_open=1 ORDER BY order_id")
            .fetch_all(&pool)
            .await?
    };
    pool.close().await;
    Ok(serde_json::Value::Array(
        rows.iter().map(order_json).collect(),
    ))
}

pub(crate) async fn balances(path: &PathBuf) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v2/balance", "").await?;
    pool.close().await;
    Ok(serde_json::json!([{
        "asset": "USDT",
        "balance": "10000",
        "availableBalance": "10000",
        "crossWalletBalance": "10000"
    }]))
}

pub(crate) async fn positions(path: &PathBuf) -> anyhow::Result<serde_json::Value> {
    let pool = connect(path).await?;
    log(&pool, "GET", "/fapi/v2/positionRisk", "").await?;
    let rows = sqlx::query("SELECT symbol, quantity FROM positions ORDER BY symbol")
        .fetch_all(&pool)
        .await?;
    pool.close().await;
    Ok(serde_json::Value::Array(
        rows.into_iter()
            .map(|row| {
                serde_json::json!({
                    "symbol": row.get::<String, _>("symbol"),
                    "positionAmt": row.get::<String, _>("quantity"),
                    "entryPrice": "100",
                    "markPrice": "100",
                    "leverage": "1",
                    "unRealizedProfit": "0",
                    "liquidationPrice": "50",
                    "positionSide": "BOTH"
                })
            })
            .collect(),
    ))
}
