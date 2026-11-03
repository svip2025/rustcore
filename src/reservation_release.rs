//! Return an exchange Entry's original allocation only after fresh venue-flat
//! truth. This also repairs closed positions left by older engine versions.
//! No account-wide reset, price-based estimate, or locally inferred flatness.

use sqlx::Row;
use tonic::Status;

use crate::position_close::VenuePositions;
use crate::store::{DbKind, Store};
use crate::svc_v2::ExactDecimal;

fn db(error: sqlx::Error) -> Status {
    Status::internal(format!(
        "reservation release database operation failed: {error}"
    ))
}

fn unresolved(position: &str, reason: &str) -> bool {
    tracing::warn!(
        position_id = position,
        reason,
        "closed Entry reservation retained"
    );
    false
}

/// The only production caller supplies the current successful reconcile's
/// PositionRisk response and coverage, after its order truth has been applied.
pub(crate) async fn sweep_confirmed_flat_reservations(
    store: &Store,
    credential: &str,
    venue: &VenuePositions<'_>,
    now: i64,
) -> Result<usize, Status> {
    // Legacy positions without an allocation must not turn reconciliation into
    // an error. Never manufacture a reservation row for them.
    if sqlx::query("SELECT 1 FROM notional_reservations WHERE HEX(credential_id)=HEX(?)")
        .bind(credential)
        .fetch_optional(store.pool())
        .await
        .map_err(db)?
        .is_none()
    {
        return Ok(0);
    }
    let candidates = sqlx::query(
        "SELECT DISTINCT ps.position_id FROM position_state ps \
         JOIN engine_orders eo ON HEX(eo.position_id)=HEX(ps.position_id) \
           AND HEX(eo.credential_id)=HEX(ps.credential_id) \
         LEFT JOIN notional_reservation_releases rr \
           ON HEX(rr.entry_engine_order_id)=HEX(eo.engine_order_id) \
         WHERE HEX(ps.credential_id)=HEX(?) AND HEX(ps.state)=HEX('closed') \
           AND HEX(eo.order_role)=HEX('entry') AND eo.risk_verdict_id IS NOT NULL \
           AND rr.entry_engine_order_id IS NULL ORDER BY ps.position_id",
    )
    .bind(credential)
    .fetch_all(store.pool())
    .await
    .map_err(db)?;
    let mut released = 0;
    for candidate in candidates {
        let position: String = candidate.get("position_id");
        let mut tx = store.pool().begin().await.map_err(db)?;
        // Same global lock order as Entry admission and zero-fill retirement.
        crate::risk::lock_notional_reservation(&mut tx, store.kind(), credential).await?;
        if release_one(&mut tx, store.kind(), credential, &position, venue, now).await? {
            released += 1;
        }
        tx.commit().await.map_err(db)?;
    }
    Ok(released)
}

async fn release_one(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    kind: DbKind,
    credential: &str,
    position: &str,
    venue: &VenuePositions<'_>,
    now: i64,
) -> Result<bool, Status> {
    let lock = if kind == DbKind::MySql {
        " FOR UPDATE"
    } else {
        ""
    };
    let Some(ps) = sqlx::query(&format!(
        "SELECT credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty \
         FROM position_state WHERE position_id=?{lock}"
    ))
    .bind(position)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?
    else {
        return Ok(false);
    };
    let symbol: String = ps.get("symbol_key");
    let direction: String = ps.get("direction");
    if ps.get::<String, _>("credential_id") != credential
        || ps.get::<String, _>("state") != "closed"
        || crate::reconcile::snapshot_decimal_is_zero(&ps.get::<String, _>("qty")) != Some(true)
        || !(venue.covers)(&symbol)
        || crate::protection_cleanup::venue_scope_exposure(venue.positions, &symbol, &direction)
            != crate::protection_cleanup::VenueScopeExposure::Flat
    {
        return Ok(false);
    }
    let bot: String = ps.get("bot_uid");
    let epoch: i64 = ps.get("entry_epoch");
    // Do not lock an Entry order after the position: the fill sink locks its
    // order first. This first consistent read is taken after the current-read
    // position lock. A terminal Entry older than the venue read cannot gain new
    // exposure; an in-flight or newly terminal Entry is retained for next round.
    let entries = sqlx::query(
        "SELECT engine_order_id,bot_uid,symbol_key,action,idempotency_key,status, \
         COALESCE(exchange_order_id,'') AS exchange_id,exchange,market_kind, \
         COALESCE(risk_verdict_id,'') AS verdict,created_at_ms,updated_at_ms \
         FROM engine_orders WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?) \
           AND HEX(order_role)=HEX('entry')",
    )
    .bind(credential)
    .bind(position)
    .fetch_all(&mut **tx)
    .await
    .map_err(db)?;
    if entries.len() != 1 {
        return Ok(unresolved(position, "ENTRY_ALLOCATION_NOT_UNIQUE"));
    }
    let entry = &entries[0];
    let order: String = entry.get("engine_order_id");
    let verdict: String = entry.get("verdict");
    if sqlx::query(
        "SELECT 1 FROM notional_reservation_releases WHERE HEX(entry_engine_order_id)=HEX(?)",
    )
    .bind(&order)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?
    .is_some()
    {
        return Ok(false);
    }
    let status: String = entry.get("status");
    if !matches!(
        status.as_str(),
        "filled" | "canceled" | "expired" | "rejected" | "failed"
    ) || venue.read_started_at_ms <= entry.get::<i64, _>("created_at_ms")
        || venue.read_started_at_ms <= entry.get::<i64, _>("updated_at_ms")
    {
        return Ok(false);
    }
    let expected_action = match direction.as_str() {
        "long" => "open_long",
        "short" => "open_short",
        _ => return Ok(unresolved(position, "ENTRY_DIRECTION_UNPROVEN")),
    };
    let exchange_id: String = entry.get("exchange_id");
    if entry.get::<String, _>("bot_uid") != bot
        || entry.get::<String, _>("symbol_key") != symbol
        || entry.get::<String, _>("action") != expected_action
        || entry.get::<String, _>("exchange") != "binance"
        || entry.get::<String, _>("market_kind") != "futures"
        || exchange_id.trim().is_empty()
        || exchange_id.starts_with("paper")
        || verdict.is_empty()
    {
        return Ok(unresolved(position, "ENTRY_SCOPE_UNPROVEN"));
    }
    let Some(rv) = sqlx::query(
        "SELECT credential_id,bot_uid,symbol_key,intent_id, \
         COALESCE(approved_intent_key,'') AS approved_key,decision, \
         COALESCE(sizing_json,'') AS sizing,COALESCE(replay_input_json,'') AS replay \
         FROM risk_verdicts WHERE HEX(risk_verdict_id)=HEX(?)",
    )
    .bind(&verdict)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?
    else {
        return Ok(unresolved(position, "ENTRY_VERDICT_MISSING"));
    };
    let references =
        sqlx::query("SELECT COUNT(*) AS n FROM engine_orders WHERE HEX(risk_verdict_id)=HEX(?)")
            .bind(&verdict)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?
            .get::<i64, _>("n");
    if rv.get::<String, _>("credential_id") != credential
        || rv.get::<String, _>("bot_uid") != bot
        || rv.get::<String, _>("symbol_key") != symbol
        || rv.get::<String, _>("approved_key") != rv.get::<String, _>("intent_id")
        || !matches!(
            rv.get::<String, _>("decision").as_str(),
            "approve" | "resize"
        )
        || references != 1
    {
        return Ok(unresolved(position, "ENTRY_VERDICT_SCOPE_UNPROVEN"));
    }
    let proof = sqlx::query(
        "SELECT credential_id,bot_uid,symbol_key,direction,entry_epoch, \
         entry_engine_order_id,run_mode,execution_venue \
         FROM position_execution_provenance WHERE HEX(position_id)=HEX(?)",
    )
    .bind(position)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;
    let has_proof = proof.is_some();
    if let Some(proof) = proof {
        if proof.get::<String, _>("credential_id") != credential
            || proof.get::<String, _>("bot_uid") != bot
            || proof.get::<String, _>("symbol_key") != symbol
            || proof.get::<String, _>("direction") != direction
            || proof.get::<i64, _>("entry_epoch") != epoch
            || proof.get::<String, _>("entry_engine_order_id") != order
            || !matches!(
                proof.get::<String, _>("run_mode").as_str(),
                "demo" | "live_small" | "live"
            )
            || !matches!(
                proof.get::<String, _>("execution_venue").as_str(),
                "testnet" | "mainnet"
            )
        {
            return Ok(unresolved(position, "ENTRY_PROVENANCE_CONFLICT"));
        }
    } else {
        // A reconciled reopen intentionally deletes close authority. Recover
        // only allocation ownership; never restore that close authority.
        let replay = crate::store::try_get_text(&rv, "replay")
            .map_err(db)?
            .unwrap_or_default();
        let stage = serde_json::from_str::<serde_json::Value>(&replay)
            .ok()
            .and_then(|v| {
                v.pointer("/input/stage_certification/requested_stage")
                    .and_then(|x| x.as_str())
                    .map(str::to_owned)
            });
        if entry.get::<String, _>("idempotency_key")
            != crate::position_state::entry_authority_key(&bot, &symbol, &direction, epoch)
            || !matches!(stage.as_deref(), Some("demo" | "live_small" | "live"))
        {
            return Ok(unresolved(position, "LEGACY_ENTRY_EPOCH_UNPROVEN"));
        }
    }
    if !has_proof || status != "filled" {
        let fills = sqlx::query(
            "SELECT COALESCE(fill_qty,'') AS qty,exchange,market_kind, \
             COALESCE(symbol,'') AS symbol,COALESCE(exchange_order_id,'') AS exchange_id, \
             symbol_scope_known FROM engine_fills \
             WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)",
        )
        .bind(credential)
        .bind(&order)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        let proven = fills.iter().any(|f| {
            f.get::<i64, _>("symbol_scope_known") == 1
                && f.get::<String, _>("exchange") == "binance"
                && f.get::<String, _>("market_kind") == "futures"
                && f.get::<String, _>("exchange_id") == exchange_id
                && crate::binance_futures::usdm_symbol_key_matches(
                    &symbol,
                    &f.get::<String, _>("symbol"),
                )
                && ExactDecimal::parse(&f.get::<String, _>("qty"), "Entry fill quantity")
                    .is_ok_and(ExactDecimal::is_strictly_positive)
        });
        if !proven {
            return Ok(unresolved(position, "ENTRY_EXCHANGE_FILL_UNPROVEN"));
        }
    }
    let sizing = crate::store::try_get_text(&rv, "sizing")
        .map_err(db)?
        .unwrap_or_default();
    let amount = crate::svc::final_notional_from_sizing_json(&sizing)?;
    let amount_decimal = ExactDecimal::parse(&amount, "Entry reserved notional")?;
    if !amount_decimal.is_strictly_positive() {
        return Err(Status::failed_precondition(
            "Entry reserved notional is not positive",
        ));
    }
    let reserved = sqlx::query(
        "SELECT reserved_usdt FROM notional_reservations WHERE HEX(credential_id)=HEX(?)",
    )
    .bind(credential)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?
    .get::<String, _>("reserved_usdt");
    let after = ExactDecimal::parse(&reserved, "reserved notional")?
        .checked_add(
            ExactDecimal::parse(&format!("-{amount}"), "released notional")?,
            "remaining reservation",
        )?
        .to_text();
    if after.starts_with('-') {
        return Err(Status::failed_precondition(
            "reservation release exceeds durable reserved amount",
        ));
    }
    sqlx::query(
        "INSERT INTO notional_reservation_releases(entry_engine_order_id,risk_verdict_id, \
         credential_id,position_id,notional_usdt,truth_read_started_at_ms,released_at_ms) \
         VALUES(?,?,?,?,?,?,?)",
    )
    .bind(&order)
    .bind(&verdict)
    .bind(credential)
    .bind(position)
    .bind(&amount)
    .bind(venue.read_started_at_ms)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    let changed = sqlx::query(
        "UPDATE notional_reservations SET reserved_usdt=?,updated_at_ms=? \
         WHERE HEX(credential_id)=HEX(?) AND HEX(reserved_usdt)=HEX(?)",
    )
    .bind(after)
    .bind(now)
    .bind(credential)
    .bind(reserved)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    if changed.rows_affected() != 1 {
        return Err(Status::failed_precondition(
            "reservation release lost allocation ownership",
        ));
    }
    Ok(true)
}
