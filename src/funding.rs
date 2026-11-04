//! Testnet funding-income evidence. The exchange wallet remains the equity
//! source; importing an income row must never credit that wallet a second time.
use crate::binance_futures::BinanceFuturesClient;
use crate::proto_v2 as p;
use crate::store::{DbKind, Store};
use crate::svc_v2::ExactDecimal;
use sqlx::Row;
use std::collections::BTreeMap;
use tonic::Status;

const LIMIT: usize = 1_000;
const MAX_QUERIES: usize = 63;
const MAX_WINDOW_MS: i64 = 48 * 60 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Income {
    transaction_id: String,
    time: i64,
    amount: String,
}
fn db(error: sqlx::Error) -> Status {
    tracing::error!(%error, "funding evidence database operation failed");
    Status::internal("funding evidence database operation failed")
}
fn invalid() -> Status {
    Status::failed_precondition("funding-income truth has invalid fields")
}

pub(crate) fn validate_window(
    request: &p::GetFundingHistoryRequest,
    now: i64,
) -> Result<(), Status> {
    if request.schema_version != 1
        || request.symbol != "BTCUSDT"
        || request.credential_id.trim().is_empty()
        || request.credential_id.trim() != request.credential_id
        || request.credential_id.len() > 64
        || request.start_time_ms < 0
        || request.end_time_ms < request.start_time_ms
        || request.end_time_ms > now
        || request.end_time_ms - request.start_time_ms > MAX_WINDOW_MS
        || request.start_time_ms < now.saturating_sub(89 * 24 * 60 * 60 * 1_000)
    {
        return Err(Status::invalid_argument(
            "funding query requires schema 1, BTCUSDT and a closed recent window of at most 48h",
        ));
    }
    Ok(())
}
fn decode(value: &serde_json::Value, from: i64, to: i64) -> Result<Vec<Income>, Status> {
    let rows = value.as_array().ok_or_else(invalid)?;
    if rows.len() > LIMIT {
        return Err(invalid());
    }
    rows.iter()
        .map(|row| {
            if row["symbol"].as_str() != Some("BTCUSDT")
                || row["asset"].as_str() != Some("USDT")
                || row["incomeType"].as_str() != Some("FUNDING_FEE")
            {
                return Err(invalid());
            }
            let time = row["time"].as_i64().ok_or_else(invalid)?;
            if time < from || time > to {
                return Err(invalid());
            }
            let transaction_id = match &row["tranId"] {
                serde_json::Value::Number(n) => n.as_u64().map(|v| v.to_string()),
                serde_json::Value::String(s)
                    if !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_digit()) =>
                {
                    Some(s.clone())
                }
                _ => None,
            }
            .ok_or_else(invalid)?;
            let amount = ExactDecimal::parse(
                row["income"].as_str().ok_or_else(invalid)?,
                "funding income",
            )
            .map_err(|_| invalid())?
            .to_text();
            Ok(Income {
                transaction_id,
                time,
                amount,
            })
        })
        .collect()
}

pub(crate) async fn refresh(
    store: &Store,
    client: &BinanceFuturesClient,
    request: &p::GetFundingHistoryRequest,
) -> Result<p::GetFundingHistoryResponse, Status> {
    validate_window(request, crate::svc::now_ms())?;
    let mut intervals = vec![(request.start_time_ms, request.end_time_ms)];
    let mut incomes = BTreeMap::<String, Income>::new();
    let mut observed = BTreeMap::<String, Income>::new();
    let mut queries = 0;
    while let Some((from, to)) = intervals.pop() {
        queries += 1;
        if queries > MAX_QUERIES {
            return Err(Status::resource_exhausted(
                "funding history window is too dense to prove complete",
            ));
        }
        let value = client.funding_income_window("BTCUSDT", from, to).await.map_err(|error| {
            tracing::warn!(%error, "funding-income exchange read failed; no completeness marker written");
            Status::unavailable("funding-income exchange read failed")
        })?;
        let rows = decode(&value, from, to)?;
        for row in &rows {
            if observed
                .insert(row.transaction_id.clone(), row.clone())
                .is_some_and(|prior| prior != *row)
            {
                return Err(Status::failed_precondition(
                    "funding transaction identity has conflicting query truth",
                ));
            }
        }
        if rows.len() == LIMIT {
            // No undocumented page parameter or ordering assumption. Split the
            // inclusive time window. At 1ms saturation completeness is unknown.
            if from == to {
                return Err(Status::resource_exhausted(
                    "funding history saturated a single timestamp",
                ));
            }
            let middle = from + (to - from) / 2;
            intervals.push((middle + 1, to));
            intervals.push((from, middle));
            continue;
        }
        for row in rows {
            if incomes
                .insert(row.transaction_id.clone(), row.clone())
                .is_some_and(|prior| prior != row)
            {
                return Err(Status::failed_precondition(
                    "funding transaction identity has conflicting truth",
                ));
            }
        }
    }
    if observed
        .iter()
        .any(|(identity, row)| incomes.get(identity) != Some(row))
    {
        return Err(Status::failed_precondition(
            "funding interval reads disagreed; complete coverage is unknown",
        ));
    }
    persist(
        store,
        request,
        incomes.into_values().collect(),
        crate::svc::now_ms(),
    )
    .await
}

async fn persist(
    store: &Store,
    request: &p::GetFundingHistoryRequest,
    incomes: Vec<Income>,
    observed: i64,
) -> Result<p::GetFundingHistoryResponse, Status> {
    let mut tx = store.pool().begin().await.map_err(db)?;
    let mut total = ExactDecimal::zero();
    let mut entries = Vec::new();
    for row in incomes {
        let ledger_id = crate::svc::sha256_hex(&format!(
            "funding|{}|FUNDING_FEE|{}",
            request.credential_id, row.transaction_id
        ));
        let insert = match store.kind() {
            DbKind::MySql => "INSERT IGNORE INTO exchange_funding_ledger (ledger_id,credential_id,exchange_transaction_id,symbol,asset,amount_usdt,transaction_time_ms,observed_at_ms) VALUES (?,?,?,'BTCUSDT','USDT',?,?,?)",
            DbKind::Unknown => return Err(Status::failed_precondition("funding ledger requires SQLite or MySQL")),
            DbKind::Sqlite => "INSERT OR IGNORE INTO exchange_funding_ledger (ledger_id,credential_id,exchange_transaction_id,symbol,asset,amount_usdt,transaction_time_ms,observed_at_ms) VALUES (?,?,?,'BTCUSDT','USDT',?,?,?)",
        };
        sqlx::query(insert)
            .bind(&ledger_id)
            .bind(&request.credential_id)
            .bind(&row.transaction_id)
            .bind(&row.amount)
            .bind(row.time)
            .bind(observed)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let stored = sqlx::query("SELECT credential_id,exchange_transaction_id,symbol,asset,amount_usdt,CAST(transaction_time_ms AS CHAR) AS transaction_time FROM exchange_funding_ledger WHERE ledger_id=?")
            .bind(&ledger_id).fetch_one(&mut *tx).await.map_err(db)?;
        if stored.get::<String, _>("credential_id") != request.credential_id
            || stored.get::<String, _>("exchange_transaction_id") != row.transaction_id
            || stored.get::<String, _>("symbol") != "BTCUSDT"
            || stored.get::<String, _>("asset") != "USDT"
            || stored.get::<String, _>("amount_usdt") != row.amount
            || stored.get::<String, _>("transaction_time") != row.time.to_string()
        {
            return Err(Status::failed_precondition(
                "funding transaction identity has conflicting persisted truth",
            ));
        }
        total = total.checked_add(
            ExactDecimal::parse(&row.amount, "funding income")?,
            "funding total",
        )?;
        entries.push(p::FundingIncomeEntry {
            public_id: format!("funding_{ledger_id}"),
            transaction_time_ms: row.time,
            symbol: "BTCUSDT".into(),
            asset: "USDT".into(),
            amount_usdt: row.amount,
        });
    }
    entries.sort_by(|a, b| {
        (a.transaction_time_ms, &a.public_id).cmp(&(b.transaction_time_ms, &b.public_id))
    });
    let observation_id = ulid::Ulid::new().to_string();
    sqlx::query("INSERT INTO exchange_funding_observations (observation_id,credential_id,symbol,start_time_ms,end_time_ms,source_as_of_ms,entry_count,total_funding_usdt) VALUES (?,?,'BTCUSDT',?,?,?,?,?)")
        .bind(&observation_id).bind(&request.credential_id).bind(request.start_time_ms).bind(request.end_time_ms)
        .bind(observed).bind(entries.len() as i64).bind(total.to_text()).execute(&mut *tx).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    Ok(p::GetFundingHistoryResponse { complete: true, reason: "Complete bounded exchange FUNDING_FEE income observation; live settlement format remains pending testnet verification".into(), source_as_of_ms: observed,
        start_time_ms: request.start_time_ms, end_time_ms: request.end_time_ms, total_funding_usdt: total.to_text(), entries, schema_version: 1 })
}

pub(crate) async fn get_history(
    store: &Store,
    request: p::GetFundingHistoryRequest,
) -> Result<p::GetFundingHistoryResponse, Status> {
    validate_window(&request, crate::svc::now_ms())?;
    if crate::venue::venue_for_credential(&request.credential_id)
        .map_err(|_| Status::failed_precondition("funding venue unavailable"))?
        != Some(crate::venue::Venue::Testnet)
    {
        return Err(Status::failed_precondition(
            "round2 funding evidence requires explicit testnet venue",
        ));
    }
    let keys = crate::binance_futures::keys_from_env(&request.credential_id)
        .map_err(|_| Status::failed_precondition("funding credentials unavailable"))?
        .ok_or_else(|| Status::failed_precondition("funding credentials unavailable"))?;
    let client = BinanceFuturesClient::new_with_store(keys, &request.credential_id, store.clone())
        .map_err(|_| Status::failed_precondition("funding exchange transport unavailable"))?;
    refresh(store, &client, &request).await
}

#[cfg(test)]
#[path = "funding_tests.rs"]
mod tests;
