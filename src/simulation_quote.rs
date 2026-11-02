//! Explicit, isolated-paper quote input on an already-owned market stream.
//! Quotes never become closed bars, orders, funding or entry admission.
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::proto_v2::MarkQuoteTruth;
use crate::store::{DbKind, Store};
use crate::svc::{emit_event, sha256_hex, EventContext};

pub(crate) const FIXTURE: &str = "OFFLINE_MARKET_FIXTURE";
pub(crate) const EVENT: &str = "simulation_mark_quote";
pub(crate) const MAX_AGE_MS: i64 = 60_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    e: String,
    source_fixture: String,
    run_mode: String,
    credential_id: String,
    symbol_key: String,
    symbol: String,
    price: String,
    source_timestamp_ms: i64,
    schema_version: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    raw: Frame,
    received_at_ms: i64,
}

pub(crate) struct Permit {
    credential: String,
    symbol_key: String,
    pub(crate) url: String,
}

/// The default is disabled. The only enabled transport is the explicitly
/// labelled loopback fixture on a paper-only, live-disabled executing process.
fn configured_base() -> Result<Option<url::Url>, &'static str> {
    configured_base_from(
        std::env::var("SIMULATION_MARK_QUOTE_FIXTURE")
            .ok()
            .as_deref(),
        std::env::var("ENGINE_STAGE_MAX").ok().as_deref(),
        std::env::var("ENGINE_LIVE_ENABLED").ok().as_deref(),
        std::env::var("BINANCE_FUTURES_WS_BASE_URL").ok().as_deref(),
    )
}

fn configured_base_from(
    fixture: Option<&str>,
    stage: Option<&str>,
    live: Option<&str>,
    base: Option<&str>,
) -> Result<Option<url::Url>, &'static str> {
    if fixture.is_none() {
        return Ok(None);
    }
    if fixture != Some(FIXTURE) || stage != Some("paper") || live != Some("0") {
        return Err("quote_fixture_requires_explicit_paper_live_off");
    }
    let parsed = url::Url::parse(base.ok_or("quote_fixture_base_missing")?)
        .map_err(|_| "quote_fixture_base_invalid")?;
    if parsed.scheme() != "ws"
        || parsed.host_str() != Some("127.0.0.1")
        || parsed.port().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err("quote_fixture_base_not_exact_loopback");
    }
    Ok(Some(parsed))
}

fn account_identity(credential: &str) -> bool {
    credential.starts_with("paper:") && crate::venue::is_simulation_credential(credential)
}

pub(crate) async fn prepare(
    store: &Store,
    credential: &str,
    symbol_key: &str,
    timeframe: &str,
) -> anyhow::Result<Option<Permit>> {
    let Some(mut base) = configured_base().map_err(anyhow::Error::msg)? else {
        return Ok(None);
    };
    if !account_identity(credential) || timeframe != "1m" {
        return Ok(None);
    }
    let subscription =
        crate::market_data::validate_subscription("binance", "futures", symbol_key, timeframe)
            .map_err(anyhow::Error::msg)?;
    let owned = sqlx::query("SELECT 1 FROM market_stream_subscriptions s WHERE s.credential_id=? AND s.symbol_key=? AND s.timeframe='1m' AND EXISTS (SELECT 1 FROM simulation_account_funding_events f WHERE f.credential_id=s.credential_id) LIMIT 1")
        .bind(credential).bind(symbol_key).fetch_optional(store.pool()).await?;
    if owned.is_none() {
        // A stream requested before funding must retry until it owns the
        // scope, rather than staying on a legacy connection without quotes.
        anyhow::bail!("quote_owned_stream_missing");
    }
    base.set_path(&format!(
        "/ws/{}@kline_1m",
        subscription.symbol.to_lowercase()
    ));
    base.query_pairs_mut()
        .append_pair("credentialId", credential)
        .append_pair("symbolKey", symbol_key);
    Ok(Some(Permit {
        credential: credential.into(),
        symbol_key: symbol_key.into(),
        url: base.into(),
    }))
}

pub(crate) fn is_quote(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| value.get("e").and_then(|v| v.as_str()).map(str::to_string))
        .as_deref()
        == Some("offlineMarkQuote")
}

fn validate(
    raw: &Frame,
    credential: &str,
    symbol_key: &str,
    receipt_ms: i64,
) -> Result<(), &'static str> {
    if raw.e != "offlineMarkQuote"
        || raw.source_fixture != FIXTURE
        || raw.run_mode != "paper"
        || raw.schema_version != 1
    {
        return Err("quote_source_or_mode_invalid");
    }
    if !account_identity(credential)
        || raw.credential_id != credential
        || raw.symbol_key != symbol_key
    {
        return Err("quote_account_or_symbol_mismatch");
    }
    let subscription =
        crate::market_data::validate_subscription("binance", "futures", symbol_key, "1m")
            .map_err(|_| "quote_symbol_invalid")?;
    if raw.symbol != subscription.symbol {
        return Err("quote_upstream_symbol_mismatch");
    }
    let price = crate::svc_v2::ExactDecimal::parse(&raw.price, "quote_price")
        .map_err(|_| "quote_price_invalid")?;
    if !price.is_strictly_positive() {
        return Err("quote_price_nonpositive");
    }
    if raw.source_timestamp_ms <= 0
        || receipt_ms <= 0
        || raw.source_timestamp_ms > receipt_ms
        || receipt_ms.saturating_sub(raw.source_timestamp_ms) > MAX_AGE_MS
    {
        return Err("quote_original_timestamp_invalid");
    }
    Ok(())
}

/// Serializes against the same per-credential event checkpoint as emit_event.
/// The original source timestamp is the watermark, including across restart.
pub(crate) async fn ingest(
    store: &Store,
    permit: Option<&Permit>,
    text: &str,
    receipt_ms: i64,
) -> anyhow::Result<()> {
    let permit =
        permit.ok_or_else(|| anyhow::anyhow!("quote_input_not_enabled_for_owned_stream"))?;
    configured_base()
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("quote_input_disabled"))?;
    if text.len() > 4096 {
        anyhow::bail!("quote_frame_too_large");
    }
    let raw: Frame =
        serde_json::from_str(text).map_err(|_| anyhow::anyhow!("quote_frame_invalid"))?;
    validate(&raw, &permit.credential, &permit.symbol_key, receipt_ms)
        .map_err(anyhow::Error::msg)?;
    let mut tx = store.pool().begin().await?;
    let ensure = match store.kind() {
        DbKind::MySql => "INSERT INTO engine_checkpoints (credential_id,last_seq_emitted,updated_at_ms) VALUES (?,0,?) ON DUPLICATE KEY UPDATE credential_id=credential_id",
        _ => "INSERT INTO engine_checkpoints (credential_id,last_seq_emitted,updated_at_ms) VALUES (?,0,?) ON CONFLICT(credential_id) DO NOTHING",
    };
    sqlx::query(ensure)
        .bind(&permit.credential)
        .bind(receipt_ms)
        .execute(&mut *tx)
        .await?;
    // A write acquires the row lock on both native backends before inspecting
    // the quote watermark. emit_event's own lock is re-entrant in this tx.
    sqlx::query("UPDATE engine_checkpoints SET updated_at_ms=updated_at_ms WHERE credential_id=?")
        .bind(&permit.credential)
        .execute(&mut *tx)
        .await?;
    let owned = sqlx::query("SELECT 1 FROM market_stream_subscriptions s WHERE s.credential_id=? AND s.symbol_key=? AND s.timeframe='1m' AND EXISTS (SELECT 1 FROM simulation_account_funding_events f WHERE f.credential_id=s.credential_id) LIMIT 1")
        .bind(&permit.credential).bind(&permit.symbol_key).fetch_optional(&mut *tx).await?;
    if owned.is_none() {
        anyhow::bail!("quote_owned_stream_missing");
    }
    if let Some(row) = sqlx::query("SELECT payload_json FROM engine_order_events WHERE credential_id=? AND symbol_key=? AND event_type=? ORDER BY seq DESC LIMIT 1")
        .bind(&permit.credential).bind(&permit.symbol_key).bind(EVENT).fetch_optional(&mut *tx).await? {
        let bytes: Vec<u8> = row.get("payload_json");
        let previous: Record = serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("stored_quote_invalid"))?;
        validate(&previous.raw, &permit.credential, &permit.symbol_key, previous.received_at_ms).map_err(anyhow::Error::msg)?;
        if raw.source_timestamp_ms <= previous.raw.source_timestamp_ms {
            anyhow::bail!("quote_timestamp_not_strictly_new");
        }
    }
    let event_id = sha256_hex(&format!(
        "{EVENT}|{}|{}|{}",
        raw.credential_id, raw.symbol_key, raw.source_timestamp_ms
    ));
    let record = Record {
        raw,
        received_at_ms: receipt_ms,
    };
    let ctx = EventContext {
        bot_uid: String::new(),
        run_id: String::new(),
        cycle_id: String::new(),
        idempotency_key: String::new(),
        client_order_id_short: String::new(),
        exchange: "binance".into(),
        market_kind: "futures".into(),
        symbol_key: permit.symbol_key.clone(),
    };
    emit_event(
        &mut tx,
        store.kind(),
        &permit.credential,
        EVENT,
        &ctx,
        "",
        None,
        Some(serde_json::to_value(record)?),
        Some(event_id),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Once this scope has quote truth, an expired/malformed quote is unavailable;
/// a closed bar must not silently take over and be relabelled as a new quote.
pub(crate) async fn latest(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential: &str,
    symbol_key: &str,
    now: i64,
) -> Result<Option<Result<MarkQuoteTruth, &'static str>>, sqlx::Error> {
    let row = sqlx::query("SELECT payload_json FROM engine_order_events WHERE credential_id=? AND symbol_key=? AND event_type=? ORDER BY seq DESC LIMIT 1")
        .bind(credential).bind(symbol_key).bind(EVENT).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let check = || {
        configured_base()?.ok_or("stored_quote_source_disabled")?;
        let bytes: Vec<u8> = row.get("payload_json");
        let record: Record = serde_json::from_slice(&bytes).map_err(|_| "stored_quote_invalid")?;
        validate(&record.raw, credential, symbol_key, record.received_at_ms)?;
        if record.received_at_ms > now
            || record.raw.source_timestamp_ms > now
            || now.saturating_sub(record.raw.source_timestamp_ms) > MAX_AGE_MS
        {
            return Err("stored_quote_expired_or_future");
        }
        Ok(MarkQuoteTruth {
            source_kind: "offline_fixture_quote".into(),
            source_fixture: record.raw.source_fixture,
            run_mode: record.raw.run_mode,
            credential_id: record.raw.credential_id,
            symbol_key: record.raw.symbol_key,
            raw_price: record.raw.price,
            source_timestamp_ms: record.raw.source_timestamp_ms,
            received_at_ms: record.received_at_ms,
            schema_version: 1,
        })
    };
    Ok(Some(check()))
}

#[cfg(test)]
mod tests {
    use super::*;
    struct SavedEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for SavedEnv {
        fn drop(&mut self) {
            for (name, value) in self.0.drain(..).rev() {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
    const C: &str = "paper:0123456789abcdef0123456789abcdef";
    const S: &str = "binance|futures|BTC|USDT|USDT";
    fn raw() -> Frame {
        Frame {
            e: "offlineMarkQuote".into(),
            source_fixture: FIXTURE.into(),
            run_mode: "paper".into(),
            credential_id: C.into(),
            symbol_key: S.into(),
            symbol: "BTCUSDT".into(),
            price: "60001.25".into(),
            source_timestamp_ms: 100_000,
            schema_version: 1,
        }
    }
    #[test]
    fn quote_source_is_opt_in_and_loopback_paper_only() {
        assert!(
            configured_base_from(None, Some("paper"), Some("0"), Some("ws://127.0.0.1:19205"))
                .unwrap()
                .is_none()
        );
        assert!(configured_base_from(
            Some(FIXTURE),
            Some("paper"),
            Some("0"),
            Some("ws://127.0.0.1:19205")
        )
        .unwrap()
        .is_some());
        for base in [
            "wss://127.0.0.1:19205",
            "ws://localhost:19205",
            "ws://example.com:19205",
            "ws://127.0.0.1:19205/?x=1",
        ] {
            assert!(
                configured_base_from(Some(FIXTURE), Some("paper"), Some("0"), Some(base)).is_err()
            );
        }
        assert!(configured_base_from(
            Some(FIXTURE),
            Some("live"),
            Some("0"),
            Some("ws://127.0.0.1:19205")
        )
        .is_err());
        assert!(configured_base_from(
            Some(FIXTURE),
            Some("paper"),
            Some("1"),
            Some("ws://127.0.0.1:19205")
        )
        .is_err());
    }
    #[test]
    fn quote_original_timestamp_scope_and_price_are_strict() {
        assert!(validate(&raw(), C, S, 160_000).is_ok());
        assert!(validate(&raw(), C, S, 160_001).is_err());
        assert!(validate(&raw(), C, S, 99_999).is_err());
        assert!(validate(&raw(), "paper", S, 100_001).is_err());
        for price in ["", "0", "-1", "NaN", "Infinity", "1e999"] {
            let mut r = raw();
            r.price = price.into();
            assert!(validate(&r, C, S, 100_001).is_err());
        }
        for mutate in 0..7 {
            let mut r = raw();
            match mutate {
                0 => r.source_fixture = "binance".into(),
                1 => r.run_mode = "live".into(),
                2 => r.credential_id = "paper:ffffffffffffffffffffffffffffffff".into(),
                3 => r.symbol = "ETHUSDT".into(),
                4 => r.source_timestamp_ms = 0,
                5 => r.schema_version = 2,
                _ => r.symbol_key = "binance|spot|BTC|USDT|USDT".into(),
            };
            assert!(validate(&r, C, S, 100_001).is_err());
        }
        let mut value = serde_json::to_value(raw()).unwrap();
        value["received_at_ms"] = serde_json::json!(100_001);
        assert!(
            serde_json::from_value::<Frame>(value).is_err(),
            "source cannot invent native receipt"
        );
    }

    #[tokio::test]
    async fn quote_sqlite_reopen_retains_receipt_and_exact_expiry() {
        use std::sync::Once;
        static DRIVERS: Once = Once::new();
        DRIVERS.call_once(sqlx::any::install_default_drivers);
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let names = [
            "SIMULATION_MARK_QUOTE_FIXTURE",
            "ENGINE_STAGE_MAX",
            "ENGINE_LIVE_ENABLED",
            "BINANCE_FUTURES_WS_BASE_URL",
        ];
        let _saved = SavedEnv(
            names
                .into_iter()
                .map(|name| (name, std::env::var_os(name)))
                .collect(),
        );
        for (name, value) in names
            .into_iter()
            .zip([FIXTURE, "paper", "0", "ws://127.0.0.1:19205"])
        {
            std::env::set_var(name, value);
        }
        let path = std::env::temp_dir().join(format!("quote-persistence-{}.db", ulid::Ulid::new()));
        let db_url = format!("sqlite://{}", path.display());
        let store = Store::connect(&db_url).await.unwrap();
        sqlx::query("INSERT INTO market_stream_subscriptions (credential_id,symbol_key,timeframe,requested_at_ms) VALUES (?,?,'1m',1)").bind(C).bind(S).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO simulation_account_funding_events (idempotency_identity,credential_id,amount_usdt,created_at_ms) VALUES ('quote-fixture-fund',?,'1000',1)").bind(C).execute(store.pool()).await.unwrap();
        let permit = prepare(&store, C, S, "1m").await.unwrap().unwrap();
        let text = serde_json::to_string(&raw()).unwrap();
        ingest(&store, Some(&permit), &text, 100_001).await.unwrap();
        store.pool().close().await;
        let reopened = Store::connect(&db_url).await.unwrap();
        let mut tx = reopened.pool().begin().await.unwrap();
        let recovered = latest(&mut tx, C, S, 160_000)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(recovered.source_timestamp_ms, 100_000);
        assert_eq!(recovered.received_at_ms, 100_001);
        assert_eq!(recovered.raw_price, "60001.25");
        assert!(latest(&mut tx, C, S, 160_001)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        tx.rollback().await.unwrap();
        assert!(ingest(&reopened, Some(&permit), &text, 100_002)
            .await
            .is_err());
        let mut older = raw();
        older.source_timestamp_ms -= 1;
        assert!(ingest(
            &reopened,
            Some(&permit),
            &serde_json::to_string(&older).unwrap(),
            100_002
        )
        .await
        .is_err());
        let row = sqlx::query("SELECT COUNT(*) AS n FROM engine_order_events WHERE event_type=?")
            .bind(EVENT)
            .fetch_one(reopened.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("n"), 1);
        reopened.pool().close().await;
        std::fs::remove_file(path).unwrap();
    }
}
