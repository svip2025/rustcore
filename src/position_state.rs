use sqlx::Row;
use tonic::Status;

use crate::proto;
use crate::svc::sha256_hex;

pub(crate) const ACTIVE_STATES: &str = "'pending','open','protected','closing','flattening'";
pub(crate) const ORPHAN_CLOSE_MARKER: &str = "recovery:orphan_close";

#[derive(Debug, Clone)]
pub(crate) struct PreparedPositionIntent {
    pub authority_idem_key: String,
    pub position_id: Option<String>,
    pub direction: Option<String>,
    pub orphan_recovery: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PrepareOutcome {
    Ready(PreparedPositionIntent),
    DuplicateActiveEntry {
        authority_idem_key: String,
        detail: String,
    },
}

#[derive(Debug)]
struct ActivePosition {
    position_id: String,
    entry_epoch: i64,
    /// AUD-P2-H6：exit 键必须含 epoch，否则一次失败的 close 会永久锁仓。
    exit_epoch: i64,
    /// 进入 closing 前的状态，终态失败解锁时据实恢复。
    #[allow(dead_code, reason = "Retained internal API of the standalone engine.")]
    current_state: String,
    pending_intent_id: Option<String>,
    pending_idem_key: Option<String>,
}

pub(crate) fn semantic_idempotency_key(seed: &str) -> String {
    format!("idem_{}", sha256_hex(seed))
}

pub(crate) fn active_key(bot_uid: &str, symbol_key: &str, direction: &str) -> String {
    format!("{bot_uid}|{symbol_key}|{direction}")
}

pub(crate) struct LockedEntryClaim {
    position_id: String,
    credential_id: String,
    bot_uid: String,
    symbol_key: String,
    direction: String,
    entry_epoch: i64,
    state: String,
    active_key: Option<String>,
    pending_idem_key: Option<String>,
    quantity: String,
}

pub(crate) struct PendingEntryClaimRetirement<'a> {
    pub(crate) claim: &'a LockedEntryClaim,
    pub(crate) credential_id: &'a str,
    pub(crate) bot_uid: &'a str,
    pub(crate) symbol_key: &'a str,
    pub(crate) direction: &'a str,
    pub(crate) idempotency_key: &'a str,
    pub(crate) ts_ms: i64,
}

/// Lock a claim in the same reservation -> position -> order sequence used by
/// Entry creation. Interpretation happens only after the caller wins its exact
/// engine-order CAS, so CAS=0 never grants cleanup authority.
pub(crate) async fn lock_entry_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: crate::store::DbKind,
    position_id: &str,
) -> Result<LockedEntryClaim, Status> {
    let select = if db_kind == crate::store::DbKind::MySql {
        "SELECT position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, \
                state, active_key, pending_idem_key, qty FROM position_state \
         WHERE position_id=? FOR UPDATE"
    } else {
        "SELECT position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, \
                state, active_key, pending_idem_key, qty FROM position_state \
         WHERE position_id=?"
    };
    let row = match sqlx::query(select)
        .bind(position_id)
        .fetch_one(&mut **tx)
        .await
    {
        Ok(row) => row,
        Err(sqlx::Error::RowNotFound) => {
            return Err(Status::failed_precondition(
                "pending entry claim is missing",
            ));
        }
        Err(error) => {
            return Err(Status::internal(format!(
                "db lock pending entry claim failed: {error}"
            )));
        }
    };
    Ok(LockedEntryClaim {
        position_id: row.get("position_id"),
        credential_id: row.get("credential_id"),
        bot_uid: row.get("bot_uid"),
        symbol_key: row.get("symbol_key"),
        direction: row.get("direction"),
        entry_epoch: row.get("entry_epoch"),
        state: row.get("state"),
        active_key: crate::store::try_get_nullable_string(&row, "active_key").map_err(|error| {
            Status::internal(format!("decode pending Entry active key failed: {error}"))
        })?,
        pending_idem_key: crate::store::try_get_nullable_string(&row, "pending_idem_key").map_err(
            |error| {
                Status::internal(format!(
                    "decode pending Entry idempotency key failed: {error}"
                ))
            },
        )?,
        quantity: row.get("qty"),
    })
}

/// Retire one exact, still-empty Entry claim without deleting its epoch history.
pub(crate) async fn retire_locked_zero_pending_entry_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: crate::store::DbKind,
    retirement: PendingEntryClaimRetirement<'_>,
) -> Result<(), Status> {
    let PendingEntryClaimRetirement {
        claim,
        credential_id,
        bot_uid,
        symbol_key,
        direction,
        idempotency_key,
        ts_ms,
    } = retirement;
    let expected_active_key = active_key(bot_uid, symbol_key, direction);
    let expected_authority_key =
        entry_authority_key(bot_uid, symbol_key, direction, claim.entry_epoch);
    if claim.credential_id != credential_id
        || claim.bot_uid != bot_uid
        || claim.symbol_key != symbol_key
        || claim.direction != direction
        || claim.active_key.as_deref() != Some(expected_active_key.as_str())
        || claim.pending_idem_key.as_deref() != Some(idempotency_key)
        || idempotency_key != expected_authority_key
    {
        return Err(Status::failed_precondition(
            "pending entry claim does not match durable order ownership",
        ));
    }
    if claim.state != "pending"
        || crate::reconcile::snapshot_decimal_is_zero(&claim.quantity) != Some(true)
    {
        return Err(Status::failed_precondition(
            "pending entry claim is not exact pending with numeric zero quantity",
        ));
    }

    let state_lhs = match db_kind {
        crate::store::DbKind::MySql => "BINARY state",
        crate::store::DbKind::Sqlite => "state",
        crate::store::DbKind::Unknown => {
            return Err(Status::internal(
                "pending entry claim retirement requires a known database dialect",
            ));
        }
    };
    let update = format!(
        "UPDATE position_state SET close_revision=close_revision+1, state='closed', active_key=NULL, \
         pending_intent_id=NULL, pending_idem_key=NULL, closed_at_ms=?, updated_at_ms=? \
         WHERE position_id=? AND {state_lhs}='pending' AND qty=?"
    );
    let rows = sqlx::query(&update)
        .bind(ts_ms)
        .bind(ts_ms)
        .bind(&claim.position_id)
        .bind(&claim.quantity)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Status::internal(format!("db retire pending entry claim failed: {error}"))
        })?
        .rows_affected();
    if rows != 1 {
        return Err(Status::failed_precondition(format!(
            "pending entry claim retirement affected {rows} rows"
        )));
    }
    Ok(())
}

/// Return the exchange symbols whose local position lifecycle is still active
/// for one credential. The query is deliberately read-only: tier selection
/// consumes the durable position projection rather than creating another
/// source of position truth in the reconciler.
pub(crate) async fn active_usdm_symbols(
    store: &crate::store::Store,
    credential_id: &str,
) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT DISTINCT symbol_key FROM position_state \
         WHERE credential_id = ? AND state <> 'closed' ORDER BY symbol_key",
    )
    .bind(credential_id)
    .fetch_all(store.pool())
    .await?;

    rows.into_iter()
        .map(|row| {
            let symbol_key: String = row.try_get("symbol_key")?;
            crate::binance_futures::usdm_symbol(&symbol_key).ok_or_else(|| {
                anyhow::anyhow!(
                    "active position has no Binance USD-M identity: credential={credential_id} \
                     symbol_key={symbol_key}"
                )
            })
        })
        .collect()
}

/// Discover durable position-management obligations without granting fill
/// history authority. In particular, an orphan recovered by ReconcileNow can
/// exist without any local order, fill or proved recovery scope.
pub(crate) async fn active_usdm_credentials(
    store: &crate::store::Store,
) -> anyhow::Result<Vec<String>> {
    // Do not use SQL DISTINCT/GROUP BY: a database collation may fold distinct
    // credential bytes. Deduplicate only after reading the original identities.
    let rows = sqlx::query(
        "SELECT credential_id, symbol_key FROM position_state WHERE HEX(state) <> HEX('closed')",
    )
    .fetch_all(store.pool())
    .await?;
    let mut credentials = std::collections::BTreeSet::new();
    for row in rows {
        let credential_id: String = row.try_get("credential_id")?;
        if !matches!(
            crate::venue::credential_kind(&credential_id),
            crate::venue::CredentialKind::ExchangeBacked
        ) {
            continue;
        }
        let symbol_key: String = row.try_get("symbol_key")?;
        if crate::binance_futures::usdm_symbol(&symbol_key).is_none() {
            tracing::warn!(
                %credential_id,
                "active position has no Binance USD-M identity; it grants no startup management scope"
            );
            continue;
        }
        credentials.insert(credential_id);
    }
    Ok(credentials.into_iter().collect())
}

fn action_direction(action: &str) -> Option<(&'static str, bool)> {
    match action {
        "open_long" => Some(("long", true)),
        "open_short" => Some(("short", true)),
        "close_long" => Some(("long", false)),
        "close_short" => Some(("short", false)),
        _ => None,
    }
}

pub(crate) async fn prepare_intent(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    intent: &proto::OrderIntent,
    ts_ms: i64,
) -> Result<PrepareOutcome, Status> {
    let Some((direction, is_entry)) = action_direction(intent.action.as_str()) else {
        return Ok(PrepareOutcome::Ready(PreparedPositionIntent {
            authority_idem_key: intent.idempotency_key.clone(),
            position_id: None,
            direction: None,
            orphan_recovery: false,
        }));
    };

    if is_entry {
        prepare_entry(tx, credential_id, intent, direction, ts_ms).await
    } else {
        prepare_exit(tx, credential_id, intent, direction, ts_ms).await
    }
}

pub(crate) async fn prepare_protection_intent(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    intent: &proto::OrderIntent,
    kind: &str,
    generation: i64,
) -> Result<PreparedPositionIntent, Status> {
    let Some((direction, is_entry)) = action_direction(intent.action.as_str()) else {
        return Err(Status::invalid_argument(
            "protection intent action must close a known direction",
        ));
    };
    if is_entry {
        return Err(Status::invalid_argument(
            "protection intent action must be reduce-only close",
        ));
    }
    let Some(active) = active_position(tx, &intent.bot_uid, &intent.symbol_key, direction).await?
    else {
        return Err(Status::failed_precondition(
            "protection intent requires an active local position",
        ));
    };
    Ok(PreparedPositionIntent {
        authority_idem_key: semantic_idempotency_key(&format!(
            "protection|{}|{kind}|{generation}",
            active.position_id
        )),
        position_id: Some(active.position_id),
        direction: Some(direction.to_string()),
        orphan_recovery: false,
    })
}

pub(crate) async fn prepare_typed_protection_intent(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    intent: &proto::OrderIntent,
    kind: &str,
    generation: i64,
) -> Result<PreparedPositionIntent, Status> {
    let Some((direction, is_entry)) = action_direction(intent.action.as_str()) else {
        return Err(Status::invalid_argument(
            "typed protection action must close a known direction",
        ));
    };
    if is_entry || !intent.reduce_only {
        return Err(Status::invalid_argument(
            "typed protection must be a reduce-only close",
        ));
    }
    let Some(active) = active_position(tx, &intent.bot_uid, &intent.symbol_key, direction).await?
    else {
        return Err(Status::failed_precondition(
            "typed protection requires an active local position",
        ));
    };
    Ok(PreparedPositionIntent {
        authority_idem_key: format!("prot|{}|{kind}|{generation}", active.position_id),
        position_id: Some(active.position_id),
        direction: Some(direction.to_string()),
        orphan_recovery: false,
    })
}

async fn active_position(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    bot_uid: &str,
    symbol_key: &str,
    direction: &str,
) -> Result<Option<ActivePosition>, Status> {
    let query = format!(
        r#"
        SELECT position_id, entry_epoch, exit_epoch, state,
               COALESCE(pending_intent_id, '') AS pending_intent_id,
               COALESCE(pending_idem_key, '') AS pending_idem_key
        FROM position_state
        WHERE bot_uid = ? AND symbol_key = ? AND direction = ?
          AND state IN ({ACTIVE_STATES})
        LIMIT 1
        "#
    );
    let row = sqlx::query(&query)
        .bind(bot_uid)
        .bind(symbol_key)
        .bind(direction)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db select active position failed: {e}")))?
        .into_iter()
        .next();

    Ok(row.map(|row| {
        let pending_idem_key: String = row.get("pending_idem_key");
        let pending_intent_id: String = row.get("pending_intent_id");
        ActivePosition {
            position_id: row.get("position_id"),
            entry_epoch: row.get("entry_epoch"),
            exit_epoch: row.get("exit_epoch"),
            current_state: row.get("state"),
            pending_intent_id: if pending_intent_id.is_empty() {
                None
            } else {
                Some(pending_intent_id)
            },
            pending_idem_key: if pending_idem_key.is_empty() {
                None
            } else {
                Some(pending_idem_key)
            },
        }
    }))
}

fn entry_authority_key(
    bot_uid: &str,
    symbol_key: &str,
    direction: &str,
    entry_epoch: i64,
) -> String {
    semantic_idempotency_key(&format!(
        "entry|{bot_uid}|{symbol_key}|{direction}|{entry_epoch}"
    ))
}

async fn prepare_entry(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    intent: &proto::OrderIntent,
    direction: &str,
    ts_ms: i64,
) -> Result<PrepareOutcome, Status> {
    if let Some(active) =
        active_position(tx, &intent.bot_uid, &intent.symbol_key, direction).await?
    {
        let authority_idem_key = entry_authority_key(
            &intent.bot_uid,
            &intent.symbol_key,
            direction,
            active.entry_epoch,
        );
        if active.pending_idem_key.as_deref() == Some(authority_idem_key.as_str()) {
            return Ok(PrepareOutcome::Ready(PreparedPositionIntent {
                authority_idem_key,
                position_id: Some(active.position_id),
                direction: Some(direction.to_string()),
                orphan_recovery: false,
            }));
        }
        return Ok(PrepareOutcome::DuplicateActiveEntry {
            authority_idem_key,
            detail: format!(
                "active position {} already exists for bot={} symbol={} direction={direction}",
                active.position_id, intent.bot_uid, intent.symbol_key
            ),
        });
    }

    let row = sqlx::query(
        r#"
        SELECT COALESCE(MAX(entry_epoch), 0) AS max_epoch
        FROM position_state
        WHERE bot_uid = ? AND symbol_key = ? AND direction = ?
        "#,
    )
    .bind(&intent.bot_uid)
    .bind(&intent.symbol_key)
    .bind(direction)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db select position epoch failed: {e}")))?;
    let entry_epoch = row.get::<i64, _>("max_epoch") + 1;
    let authority_idem_key =
        entry_authority_key(&intent.bot_uid, &intent.symbol_key, direction, entry_epoch);
    let position_id = format!("pos_{}", ulid::Ulid::new());
    let key = active_key(&intent.bot_uid, &intent.symbol_key, direction);

    let insert = sqlx::query(
        r#"
        INSERT INTO position_state (
          position_id, credential_id, bot_uid, symbol_key, direction,
          entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
          qty, entry_price, opened_at_ms, closed_at_ms,
          last_reconciled_ms, updated_at_ms
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, 'pending', ?, NULL, ?,
          '0', NULL, NULL, NULL,
          NULL, ?
        )
        "#,
    )
    .bind(&position_id)
    .bind(credential_id)
    .bind(&intent.bot_uid)
    .bind(&intent.symbol_key)
    .bind(direction)
    .bind(entry_epoch)
    .bind(&key)
    .bind(&authority_idem_key)
    .bind(ts_ms)
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(PrepareOutcome::Ready(PreparedPositionIntent {
            authority_idem_key,
            position_id: Some(position_id),
            direction: Some(direction.to_string()),
            orphan_recovery: false,
        })),
        Err(error) if is_unique_violation(&error) => {
            let existing =
                active_position(tx, &intent.bot_uid, &intent.symbol_key, direction).await?;
            if let Some(existing) = existing {
                let existing_authority = entry_authority_key(
                    &intent.bot_uid,
                    &intent.symbol_key,
                    direction,
                    existing.entry_epoch,
                );
                if existing.pending_idem_key.as_deref() == Some(existing_authority.as_str()) {
                    return Ok(PrepareOutcome::Ready(PreparedPositionIntent {
                        authority_idem_key: existing_authority,
                        position_id: Some(existing.position_id),
                        direction: Some(direction.to_string()),
                        orphan_recovery: false,
                    }));
                }
                return Ok(PrepareOutcome::DuplicateActiveEntry {
                    authority_idem_key: existing_authority,
                    detail: format!(
                        "active position {} won the concurrent claim for bot={} symbol={} direction={direction}",
                        existing.position_id, intent.bot_uid, intent.symbol_key
                    ),
                });
            }
            Err(Status::internal(format!(
                "position claim unique conflict without active row: {error}"
            )))
        }
        Err(error) => Err(Status::internal(format!(
            "db claim active position failed: {error}"
        ))),
    }
}

async fn prepare_exit(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    intent: &proto::OrderIntent,
    direction: &str,
    ts_ms: i64,
) -> Result<PrepareOutcome, Status> {
    // TODO(P1/T1.6,T1.7): replace manual_close with the typed exit reason.
    let exit_reason = "manual_close";
    if let Some(active) =
        active_position(tx, &intent.bot_uid, &intent.symbol_key, direction).await?
    {
        let authority_idem_key = if active.pending_intent_id.as_deref() == Some(ORPHAN_CLOSE_MARKER)
        {
            active.pending_idem_key.unwrap_or_else(|| {
                semantic_idempotency_key(&format!(
                    "exit|{}|{}|{}|orphan|{exit_reason}",
                    intent.bot_uid, intent.symbol_key, direction
                ))
            })
        } else {
            // AUD-P2-H6：epoch 参与键。终态失败会自增它，于是重试得到一把
            // 新键、能真正重新提交；在途重试则沿用同一把键，幂等不变。
            semantic_idempotency_key(&format!(
                "exit|{}|{}|{exit_reason}|{}",
                intent.bot_uid, active.position_id, active.exit_epoch
            ))
        };
        let key = active_key(&intent.bot_uid, &intent.symbol_key, direction);
        sqlx::query(
            r#"
            UPDATE position_state
            SET close_revision=close_revision+1, state = 'closing',
                pre_exit_state = CASE
                  WHEN state = 'closing' THEN pre_exit_state
                  ELSE state
                END,
                active_key = ?, pending_idem_key = ?, updated_at_ms = ?
            WHERE position_id = ?
            "#,
        )
        .bind(&key)
        .bind(&authority_idem_key)
        .bind(ts_ms)
        .bind(&active.position_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db mark position closing failed: {e}")))?;

        return Ok(PrepareOutcome::Ready(PreparedPositionIntent {
            authority_idem_key,
            position_id: Some(active.position_id),
            direction: Some(direction.to_string()),
            orphan_recovery: false,
        }));
    }

    let authority_idem_key = semantic_idempotency_key(&format!(
        "exit|{}|{}|{}|orphan|{exit_reason}",
        intent.bot_uid, intent.symbol_key, direction
    ));

    // A completed orphan close leaves no active position_state row. Check the
    // durable order key before recreating the recovery row so retries remain
    // duplicate without leaving a new phantom closing position behind.
    let prior_order = sqlx::query(
        r#"
        SELECT 1 FROM engine_orders
        WHERE bot_uid = ? AND idempotency_key = ?
        LIMIT 1
        "#,
    )
    .bind(&intent.bot_uid)
    .bind(&authority_idem_key)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db select prior orphan close failed: {e}")))?
    .into_iter()
    .next();
    if prior_order.is_some() {
        return Ok(PrepareOutcome::Ready(PreparedPositionIntent {
            authority_idem_key,
            position_id: None,
            direction: Some(direction.to_string()),
            orphan_recovery: false,
        }));
    }

    let row = sqlx::query(
        r#"
        SELECT COALESCE(MAX(entry_epoch), 0) AS max_epoch
        FROM position_state
        WHERE bot_uid = ? AND symbol_key = ? AND direction = ?
        "#,
    )
    .bind(&intent.bot_uid)
    .bind(&intent.symbol_key)
    .bind(direction)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db select orphan position epoch failed: {e}")))?;
    let entry_epoch = row.get::<i64, _>("max_epoch") + 1;
    let position_id = format!("pos_{}", ulid::Ulid::new());
    let key = active_key(&intent.bot_uid, &intent.symbol_key, direction);
    // An orphan close has no local position_state quantity to bind. Seed the
    // phantom closing row from the credential-scoped exchange snapshot that
    // caused the orphan to be observable, never from the caller's intent.
    // `engine_positions` stores one-way short positions as a negative amount;
    // position_state always stores the positive magnitude.
    let exchange_symbol = crate::binance_futures::usdm_symbol(&intent.symbol_key)
        .ok_or_else(|| Status::failed_precondition("orphan close has an invalid symbol_key"))?;
    let exchange_position = sqlx::query(
        r#"
        SELECT position_amt
        FROM engine_positions
        WHERE credential_id = ? AND exchange = 'binance'
          AND market_kind = 'futures' AND symbol = ?
        "#,
    )
    .bind(credential_id)
    .bind(exchange_symbol)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load orphan exchange position failed: {e}")))?
    .ok_or_else(|| {
        Status::failed_precondition("orphan close has no credential-scoped exchange position truth")
    })?;
    let position_amt = exchange_position.get::<String, _>("position_amt");
    let exchange_direction = if position_amt.trim().starts_with('-') {
        "short"
    } else {
        "long"
    };
    if exchange_direction != direction {
        return Err(Status::failed_precondition(format!(
            "orphan close direction {direction} does not match exchange position direction {exchange_direction}"
        )));
    }
    let authoritative_qty = decimal_abs(&position_amt).to_string();
    crate::exchange_filters::validate_authoritative_position_quantity(&authoritative_qty).map_err(
        |error| {
            Status::failed_precondition(format!(
                "orphan close exchange position has no valid positive quantity: {error}"
            ))
        },
    )?;
    sqlx::query(
        r#"
        INSERT INTO position_state (
          position_id, credential_id, bot_uid, symbol_key, direction,
          entry_epoch, state, active_key, pending_intent_id, pending_idem_key,
          qty, entry_price, opened_at_ms, closed_at_ms,
          last_reconciled_ms, updated_at_ms
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, 'closing', ?, ?, ?,
          ?, NULL, NULL, NULL,
          NULL, ?
        )
        "#,
    )
    .bind(&position_id)
    .bind(credential_id)
    .bind(&intent.bot_uid)
    .bind(&intent.symbol_key)
    .bind(direction)
    .bind(entry_epoch)
    .bind(&key)
    .bind(ORPHAN_CLOSE_MARKER)
    .bind(&authority_idem_key)
    .bind(&authoritative_qty)
    .bind(ts_ms)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db record orphan closing position failed: {e}")))?;

    Ok(PrepareOutcome::Ready(PreparedPositionIntent {
        authority_idem_key,
        position_id: Some(position_id),
        direction: Some(direction.to_string()),
        orphan_recovery: true,
    }))
}

pub(crate) async fn mark_paper_filled(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    prepared: &PreparedPositionIntent,
    intent: &proto::OrderIntent,
    fill_price: &str,
    ts_ms: i64,
) -> Result<(), Status> {
    let Some(position_id) = prepared.position_id.as_deref() else {
        return Ok(());
    };
    match intent.action.as_str() {
        "open_long" | "open_short" => {
            let direction = prepared.direction.as_deref().unwrap_or_default();
            let key = active_key(&intent.bot_uid, &intent.symbol_key, direction);
            sqlx::query(
                r#"
                UPDATE position_state
                SET close_revision=close_revision+1, state = 'open', active_key = ?, qty = ?, entry_price = ?,
                    pending_intent_id = NULL, pending_idem_key = NULL,
                    opened_at_ms = COALESCE(opened_at_ms, ?), updated_at_ms = ?
                WHERE position_id = ?
                "#,
            )
            .bind(&key)
            .bind(&intent.quantity)
            .bind(fill_price)
            .bind(ts_ms)
            .bind(ts_ms)
            .bind(position_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| Status::internal(format!("db mark paper position open failed: {e}")))?;
        }
        "close_long" | "close_short" => {
            // paper 分支的归零：D30 善后义务按 paper 档位平凡完成（见
            // record_position_closed_in_tx 的 origin 判别）。
            mark_position_closed(tx, position_id, ts_ms, Some(ts_ms), "paper_fill").await?;
        }
        _ => {}
    }
    Ok(())
}

/// AUD-P2-H6：close 终态失败后把仓位从 `closing` 释放回可再平仓的状态。
///
/// 不释放的话：position_state 停在 closing 且带着那把陈旧的 exit 幂等键，
/// 重试命中 engine_orders 唯一键返回 duplicate、永不重新提交；而 closing
/// ∈ ACTIVE_STATES，同方向新开仓又被 DUPLICATE_ACTIVE_ENTRY 拒绝——仓位既
/// 平不掉也开不了，唯一恢复手段是 L3 flatten。
///
/// 释放同时自增 exit_epoch，使下一次 close 得到一把**新键**能真正重新提交；
/// 状态按 pre_exit_state 据实恢复（open 与 protected 语义不同，不能一律回落
/// 到 open，否则会把「保护单已挂」这个事实抹掉）。
///
/// 仅作用于 state='closing' 的行：在途的 close 与已完成的平仓都不受影响。
pub(crate) async fn release_closing_after_failed_exit(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
    ts_ms: i64,
) -> Result<u64, Status> {
    let result = sqlx::query(
        r#"
        UPDATE position_state
        SET close_revision=close_revision+1, state = COALESCE(pre_exit_state, 'open'),
            pre_exit_state = NULL,
            pending_intent_id = NULL,
            pending_idem_key = NULL,
            exit_epoch = exit_epoch + 1,
            updated_at_ms = ?
        WHERE position_id = ? AND state = 'closing'
        "#,
    )
    .bind(ts_ms)
    .bind(position_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db release closing position failed: {e}")))?;
    Ok(result.rows_affected())
}

/// 挂着的 exit 订单是否已**终态死亡**——即它再也不会成交，closing 可以释放。
///
/// R5 FBL-R4-H3：R4 的 dead 集是 {行缺失, canceled, expired, failed}，其中
/// `failed` 当时是死码（全仓无写入方）、`行缺失` 生产不可达（claim 与
/// insert_engine_order 同一事务提交），于是真正的崩溃残留态**全部漏裁**：
/// `rejected`（WS 异步拒绝送达的终态）不在集内，`unknown` / `pending_risk` /
/// `pending_submit` 在交易所侧从未确认过却也不在集内——closing 永久锁死。
///
/// 本轮的划线：**交易所权威已把它判死**才算死。
/// - `canceled` / `expired` / `rejected`：交易所终态，不会再成交。
/// - `failed`：本地裁决的终态。R5 起由 reconcile 的 `-2013`（交易所查无此单）
///   裁决写入——「已裁决」这条 设计要求 契约条款的兑现点。
/// - 行缺失（`None`）：claim 后、落单前崩溃，根本没有这把键的订单行。
///
/// 刻意**不在**集内：`unknown` / `pending_risk` / `pending_submit` 语义暧昧
///（可能已在交易所成交，只是本地没拿到回执），必须先由 reconcile 向交易所
/// 求证、拿到 -2013 才转 `failed` 再进本集——不得在此直接推定死亡，否则
/// 就是把「本地不知道」当成「交易所没有」，开出误释放面。
pub(crate) fn exit_order_status_is_dead(order_status: Option<&str>) -> bool {
    matches!(
        order_status,
        None | Some("canceled") | Some("expired") | Some("failed") | Some("rejected")
    )
}

/// R4 RV1-2：closing 的收敛清扫（权威真值驱动，供 apply_exchange_position_truth
/// 在**同一事务**内调用）。truth 说仓位仍开着，但挂着的 exit 单已终态死亡
///（canceled/expired/failed，或 claim 后、落单前崩溃导致 engine_orders 里
/// 根本没有这把键的行）时释放 closing：state 回 pre_exit_state、清 pending 键、
/// exit_epoch+1——下一次 close 拿新键可真正重提交（部分成交后的余量重平同理：
/// qty 已被同事务的 truth 应用刷新为余量）。
/// exit 仍在途（submitted/acked/partial_filled/filled）不动作；`unknown` 与
/// `pending_*` 语义暧昧（可能已在交易所成交，只是本地没拿到回执），不在此
/// 推定死亡——由 reconcile 向交易所求证、拿到 `-2013`（查无此单）后裁决为
/// `failed`，再经本函数释放（R5 FBL-R4-H3：这才是 设计要求 裁决③「unknown 已裁决
/// → 释放」的兑现路径；svc.rs live_error 分支的内联释放只覆盖进程存活的那
/// 一瞬，双写间隙崩溃后原样锁死）。
pub(crate) async fn release_closing_if_exit_dead(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
    ts_ms: i64,
) -> Result<u64, Status> {
    // COALESCE 模式（与 load_active_position 一致）：Any 驱动对 NULL 的
    // Option 解码不可靠，空串即"无键"。
    let row = sqlx::query(
        "SELECT COALESCE(pending_idem_key, '') AS pending_idem_key, bot_uid \
         FROM position_state WHERE position_id = ? AND state = 'closing'",
    )
    .bind(position_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load closing pending key failed: {e}")))?
    .into_iter()
    .next();
    let row = match row {
        Some(row) => row,
        None => return Ok(0),
    };
    let pending_key: String = row.get("pending_idem_key");
    // 没有 pending 键的 closing 不是本清扫的形态（保守：不动）。
    if pending_key.trim().is_empty() {
        return Ok(0);
    }
    let bot_uid: String = row.get("bot_uid");
    let order_status: Option<String> = sqlx::query(
        "SELECT COALESCE(status, '') AS status FROM engine_orders \
         WHERE bot_uid = ? AND idempotency_key = ?",
    )
    .bind(&bot_uid)
    .bind(&pending_key)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load pending exit order failed: {e}")))?
    .into_iter()
    .next()
    .map(|row| {
        let status: String = row.get("status");
        status
    })
    .filter(|status| !status.is_empty());
    if !exit_order_status_is_dead(order_status.as_deref()) {
        return Ok(0);
    }
    let released = release_closing_after_failed_exit(tx, position_id, ts_ms).await?;
    if released > 0 {
        tracing::warn!(
            position_id = %position_id,
            bot_uid = %bot_uid,
            pending_idem_key = %pending_key,
            order_status = order_status.as_deref().unwrap_or("<missing>"),
            "reconcile released closing position: pending exit is dead per authority"
        );
    }
    Ok(released)
}

/// R5 FBL-R4-H3：对一个 credential 下**所有** `closing` 仓位跑一遍收敛清扫。
///
/// `release_closing_if_exit_dead` 此前只有一个调用点——`apply_exchange_position_truth`
/// 的 qty>0 分支，即「交易所报了这个仓位、且数量非零」时才会被走到。于是两类
/// 真实残留完全够不着它：
/// ① reconcile 在**本轮**才把某条订单裁决为死亡（-2013 → `failed`），而仓位
///    真值应用发生在订单循环**之前**，本轮不会再释放；
/// ② 交易所 positionRisk 未返回该 symbol（或返回但被 direction 过滤掉）时，
///    truth 分支根本不进入。
/// 清扫按仓位枚举而不是按订单枚举，因此与「本轮是否恰好收到该仓位的 truth」
/// 解耦——收敛不再依赖某条特定成功路径。
///
/// 保守方向不变：真正的判死仍只由 `exit_order_status_is_dead` 决定，本函数
/// 不新增任何推定。调用点须在仓位真值应用**之后**——已经平掉的仓位那时已被
/// `mark_position_closed` 移出 `closing`，不会被误释放回 open。
pub(crate) async fn sweep_closing_positions(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    ts_ms: i64,
) -> Result<u64, Status> {
    let rows = sqlx::query(
        "SELECT position_id FROM position_state \
         WHERE credential_id = ? AND state = 'closing'",
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load closing positions failed: {e}")))?;

    let mut released = 0u64;
    for row in rows {
        let position_id: String = row.get("position_id");
        released += release_closing_if_exit_dead(tx, &position_id, ts_ms).await?;
    }
    Ok(released)
}

pub(crate) async fn mark_position_closed(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    position_id: &str,
    ts_ms: i64,
    reconciled_at_ms: Option<i64>,
    // 归零来源（D30）：`paper_fill` = paper 分支成交；`fill_truth` = 交易所
    // exit fill 应用。来源即档位——引擎按 is_exchange_backed 切开的两条
    // 生产路径分别落这两种 origin。
    origin: &str,
) -> Result<(), Status> {
    // M0fix（评审 P2-3）：善后义务绑定的是「非 closed → closed」这一次转换。
    // 本函数的 UPDATE 不带状态守卫，而 `apply_filled_order_truth` 的选仓是
    // 一致性读：MySQL 上一条快照早于 reconcile truth 关闭的 fill 事务会对
    // **已经 closed** 的行再关一次。先以当前读（MySQL 锁定读、记录锁——本行
    // 随后的 UPDATE 本来就要拿同一把锁）取转换前状态；已经 closed 的行不再
    // 武装第二个生命周期（也就不会撞义务主键、不会重置第一个生命周期的
    // 进度）。
    let db_kind = dialect_kind(tx).await?;
    let lock = if db_kind == crate::store::DbKind::MySql {
        " FOR UPDATE"
    } else {
        ""
    };
    let prior_state = sqlx::query(&format!(
        "SELECT state FROM position_state WHERE position_id = ?{lock}"
    ))
    .bind(position_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db load position state before close failed: {e}")))?
    .map(|row| row.get::<String, _>("state"));
    sqlx::query(
        r#"
        UPDATE position_state
        SET close_revision=close_revision+1, state = 'closed', active_key = NULL,
            pending_intent_id = NULL, pending_idem_key = NULL,
            qty = '0', closed_at_ms = ?,
            last_reconciled_ms = COALESCE(?, last_reconciled_ms), updated_at_ms = ?
        WHERE position_id = ?
        "#,
    )
    .bind(ts_ms)
    .bind(reconciled_at_ms)
    .bind(ts_ms)
    .bind(position_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| Status::internal(format!("db close position state failed: {e}")))?;
    if prior_state.as_deref().map(str::trim) == Some("closed") {
        return Ok(());
    }
    // D30：仓位归零的**同一事务**里落保护单善后义务——提交前崩溃则两者
    // 都不存在（成交/真值会重新推导），提交后崩溃则义务持久。模拟凭据
    // 的义务在此事务内即以记录原因平凡完成（见 record_position_closed_in_tx）。
    crate::protection_cleanup::record_position_closed_in_tx(
        tx,
        db_kind,
        position_id,
        origin,
        ts_ms,
    )
    .await?;
    Ok(())
}

/// 从事务连接判方言（与 `apply_exchange_position_truth` 的 lock 推导同源）。
async fn dialect_kind(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
) -> Result<crate::store::DbKind, Status> {
    match tx.as_mut().backend_name() {
        "MySQL" => Ok(crate::store::DbKind::MySql),
        "SQLite" => Ok(crate::store::DbKind::Sqlite),
        other => Err(Status::internal(format!(
            "position close requires a known database dialect, got {other}"
        ))),
    }
}

#[allow(
    clippy::if_same_then_else,
    reason = "Preserve the established position-truth control flow during packaging."
)]
pub(crate) async fn apply_exchange_position_truth(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    exchange_symbol: &str,
    position_side: Option<&str>,
    qty: &str,
    entry_price: Option<&str>,
    ts_ms: i64,
) -> Result<(), Status> {
    // Management obligations are discovered conservatively. Only the exact
    // canonical closed state can be ignored; an unrecognized matched row must
    // fail explicitly instead of silently appearing reconciled.
    // MySQL's locking read sees the current committed row, not an earlier
    // REPEATABLE READ snapshot. Keep that lock through all projection writes.
    let lock = match tx.as_mut().backend_name() {
        "MySQL" => " FOR UPDATE",
        "SQLite" => "",
        _ => {
            return Err(Status::internal(
                "position truth requires a known database dialect",
            ))
        }
    };
    let query = format!(
        "SELECT position_id, bot_uid, symbol_key, direction, state \
         FROM position_state WHERE credential_id = ? AND HEX(state) <> HEX('closed'){lock}"
    );
    let rows = sqlx::query(&query)
        .bind(credential_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db load active positions for truth failed: {e}")))?;

    let side = position_side.unwrap_or("BOTH").trim().to_uppercase();
    let truth_direction = match side.as_str() {
        "LONG" => Some("long"),
        "SHORT" => Some("short"),
        _ if qty.trim().starts_with('-') && !is_decimal_zero(qty) => Some("short"),
        _ if !is_decimal_zero(qty) => Some("long"),
        _ => None,
    };

    let mut matched = Vec::new();
    for row in rows {
        let symbol_key: String = row.get("symbol_key");
        // **regression**：交易所仓位真值只能落到确实是这条 USDⓈ-M 合约的
        // 本地仓上。裸 `base+quote` 会把 USDⓈ-M 的 `BTCUSDT` 真值写到币本位
        // 或异 venue 的同名本地仓上——同一份真值应用到另一张合约。
        if !crate::binance_futures::usdm_symbol_key_matches(&symbol_key, exchange_symbol) {
            continue;
        }
        let direction: String = row.get("direction");
        if let Some(truth_direction) = truth_direction {
            if direction != truth_direction {
                continue;
            }
        } else if side == "LONG" && direction != "long" {
            continue;
        } else if side == "SHORT" && direction != "short" {
            continue;
        }

        let state: String = row.get("state");
        if !matches!(
            state.as_str(),
            "pending" | "open" | "protected" | "closing" | "flattening"
        ) {
            return Err(Status::failed_precondition(
                "POSITION_TRUTH_STATE_UNRESOLVED: matched obligation has a noncanonical active state",
            ));
        }
        matched.push((row, symbol_key, direction, state));
    }

    for (row, symbol_key, direction, state) in matched {
        let position_id: String = row.get("position_id");
        if is_decimal_zero(qty) {
            // Do not use the general unconditional close helper: a truth write
            // must retain its exact state authorization at the final UPDATE.
            let changed = sqlx::query(
                "UPDATE position_state SET close_revision=close_revision+1, state='closed', active_key=NULL, \
                 pending_intent_id=NULL, pending_idem_key=NULL, qty='0', \
                 closed_at_ms=?, last_reconciled_ms=?, updated_at_ms=? \
                 WHERE position_id=? AND HEX(state)=HEX(?)",
            )
            .bind(ts_ms)
            .bind(ts_ms)
            .bind(ts_ms)
            .bind(&position_id)
            .bind(&state)
            .execute(&mut **tx)
            .await
            .map_err(|e| Status::internal(format!("db apply closed position truth failed: {e}")))?;
            if changed.rows_affected() != 1 {
                return Err(Status::failed_precondition(
                    "POSITION_TRUTH_STATE_UNRESOLVED: close authority changed",
                ));
            }
            // D30：交易所真值把仓位写到零的同一事务里落保护单善后义务。
            // 与 mark_position_closed 内那次同形、幂等（同 position_id 只落
            // 一行）；崩溃边界与成交路径一致。
            let db_kind = dialect_kind(tx).await?;
            crate::protection_cleanup::record_position_closed_in_tx(
                tx,
                db_kind,
                &position_id,
                "exchange_truth",
                ts_ms,
            )
            .await?;
        } else {
            let bot_uid: String = row.get("bot_uid");
            let key = active_key(&bot_uid, &symbol_key, &direction);
            let changed = sqlx::query(
            r#"
            UPDATE position_state
            SET close_revision=close_revision+1, state = CASE
                  WHEN HEX(state) IN (HEX('protected'), HEX('closing'), HEX('flattening')) THEN state
                  ELSE 'open'
                END,
                active_key = ?, qty = ?, entry_price = COALESCE(?, entry_price),
                pending_intent_id = CASE
                  WHEN HEX(state) IN (HEX('closing'), HEX('flattening')) THEN pending_intent_id
                  ELSE NULL
                END,
                pending_idem_key = CASE
                  WHEN HEX(state) IN (HEX('closing'), HEX('flattening')) THEN pending_idem_key
                  ELSE NULL
                END,
                opened_at_ms = COALESCE(opened_at_ms, ?),
                last_reconciled_ms = ?, updated_at_ms = ?
            WHERE position_id = ? AND HEX(state) = HEX(?)
            "#,
        )
        .bind(&key)
        .bind(decimal_abs(qty))
        .bind(entry_price.filter(|value| !value.trim().is_empty()))
        .bind(ts_ms)
        .bind(ts_ms)
        .bind(ts_ms)
        .bind(&position_id)
        .bind(&state)
        .execute(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db apply open position truth failed: {e}")))?;

            if changed.rows_affected() != 1 {
                return Err(Status::failed_precondition(
                    "POSITION_TRUTH_STATE_UNRESOLVED: open authority changed",
                ));
            }

            // R4 RV1-2：truth 说仓位仍开着（qty>0）——若挂着的 exit 已终态死亡
            //（canceled/expired/failed，或 claim 后、落单前崩溃根本无订单行），
            // 在同一事务内释放 closing，下一次 close 可重提交。reconcile 周期与
            // orchestrator 启动恢复（reconcile_now）都经过本函数，closing 的收敛
            // 由此不再依赖进程内存活（评审 RV1-2：原修复只在 live_error 分支内联
            // 释放，canceled-after-partial 与释放双写间隙崩溃仍永久锁仓）。
            release_closing_if_exit_dead(tx, &position_id, ts_ms).await?;
        }
        // Map raw exchange truth only after the local projection was authorized.
        // Both writes still belong to the caller's transaction; an unresolved
        // projection makes the caller roll back its raw-truth write as well.
        sqlx::query("UPDATE engine_positions SET symbol_key=? WHERE credential_id=? AND symbol=?")
            .bind(&symbol_key)
            .bind(credential_id)
            .bind(exchange_symbol)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                Status::internal(format!("db map exchange position symbol failed: {e}"))
            })?;
    }
    Ok(())
}

pub(crate) async fn apply_filled_order_truth(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    bot_uid: &str,
    symbol_key: &str,
    action: &str,
    cumulative_filled_qty: &str,
    fill_price: Option<&str>,
    ts_ms: i64,
) -> Result<(), Status> {
    let Some((direction, is_entry)) = action_direction(action) else {
        return Ok(());
    };
    if cumulative_filled_qty.trim().is_empty() || is_decimal_zero(cumulative_filled_qty) {
        return Ok(());
    }
    if is_entry {
        let symbol = crate::binance_futures::usdm_symbol(symbol_key)
            .ok_or_else(|| Status::internal("invalid symbol_key on filled entry"))?;
        return apply_exchange_position_truth(
            tx,
            credential_id,
            &symbol,
            Some(if direction == "long" { "LONG" } else { "SHORT" }),
            cumulative_filled_qty,
            fill_price,
            ts_ms,
        )
        .await;
    }

    let query = format!(
        r#"
        SELECT position_id, qty
        FROM position_state
        WHERE credential_id = ? AND bot_uid = ? AND symbol_key = ? AND direction = ?
          AND state IN ({ACTIVE_STATES})
        LIMIT 1
        "#
    );
    let row = sqlx::query(&query)
        .bind(credential_id)
        .bind(bot_uid)
        .bind(symbol_key)
        .bind(direction)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| Status::internal(format!("db load position for close fill failed: {e}")))?
        .into_iter()
        .next();
    let Some(row) = row else {
        return Ok(());
    };
    let known_qty: String = row.get("qty");
    if decimal_strings_equal(&known_qty, cumulative_filled_qty) {
        let position_id: String = row.get("position_id");
        mark_position_closed(tx, &position_id, ts_ms, None, "fill_truth").await?;
    }
    Ok(())
}

pub(crate) fn is_decimal_zero(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value)
        .chars()
        .all(|ch| ch == '0' || ch == '.')
}

pub(crate) fn decimal_abs(value: &str) -> &str {
    value.trim().strip_prefix('-').unwrap_or(value.trim())
}

fn decimal_strings_equal(left: &str, right: &str) -> bool {
    fn normalize(value: &str) -> String {
        let unsigned = decimal_abs(value);
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let integer = integer.trim_start_matches('0');
        let fraction = fraction.trim_end_matches('0');
        format!(
            "{}.{}",
            if integer.is_empty() { "0" } else { integer },
            fraction
        )
    }
    normalize(left) == normalize(right)
}

pub(crate) fn is_unique_violation(error: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(database) = error {
        if database.is_unique_violation() {
            return true;
        }
    }
    let message = error.to_string();
    message.contains("UNIQUE constraint failed") || message.contains("Duplicate entry")
}

#[cfg(test)]
mod startup_discovery_tests {
    use super::active_usdm_credentials;
    use crate::store::Store;

    const USDM: &str = "binance|futures|BTC|USDT|USDT";

    async fn store() -> (Store, std::path::PathBuf) {
        sqlx::any::install_default_drivers();
        let dir = std::env::temp_dir().join(format!("position-startup-{}", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", dir.join("engine.db").display()))
            .await
            .unwrap();
        (store, dir)
    }

    async fn seed(store: &Store, credential_id: &str, symbol_key: &str, state: &str) {
        let id = ulid::Ulid::new().to_string();
        sqlx::query(
            "INSERT INTO position_state \
             (position_id, credential_id, bot_uid, symbol_key, direction, entry_epoch, \
              state, active_key, qty, updated_at_ms) \
             VALUES (?, ?, ?, ?, 'long', 1, ?, ?, '1', 1)",
        )
        .bind(&id)
        .bind(credential_id)
        .bind(&id)
        .bind(symbol_key)
        .bind(state)
        .bind(if state == "closed" { None } else { Some(&id) })
        .execute(store.pool())
        .await
        .unwrap();
    }

    async fn assert_no_fill_authority(store: &Store) {
        for table in [
            "engine_orders",
            "engine_fills",
            "fill_recovery_scopes",
            "fill_replay_cursors",
            "fill_stream_state",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(store.pool())
                .await
                .unwrap();
            assert_eq!(count, 0, "position discovery must not write {table}");
        }
    }

    #[tokio::test]
    async fn a2c_r2_discovers_active_positions_with_exact_credential_bytes_without_fill_authority()
    {
        let (store, dir) = store().await;
        let mut expected = Vec::new();
        for state in [
            "pending",
            "open",
            "protected",
            "closing",
            "flattening",
            "unknown",
        ] {
            let credential = format!("management.{state}");
            seed(&store, &credential, USDM, state).await;
            expected.push(credential);
        }
        for credential in ["Case.Managed", "case.managed", "case.managed "] {
            seed(&store, credential, USDM, "open").await;
            seed(&store, credential, "binance|futures|ETH|USDC|USDC", "open").await;
            expected.push(credential.to_string());
        }
        // One invalid row cannot hide a separate, proven USD-M obligation.
        seed(&store, "case.managed", "not-a-symbol-key", "open").await;
        expected.sort();
        assert_eq!(active_usdm_credentials(&store).await.unwrap(), expected);
        assert_no_fill_authority(&store).await;
        store.pool().close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a2c_r2_rejects_closed_simulation_and_unproven_position_identities() {
        let (store, dir) = store().await;
        seed(&store, "closed.only", USDM, "closed").await;
        for credential in [
            "paper",
            "paper:0123456789abcdef0123456789abcdef",
            "paper:malformed",
            "p\u{0430}per:0123456789abcdef0123456789abcdef",
            "unknown\u{200b}credential",
            "",
        ] {
            seed(&store, credential, USDM, "open").await;
        }
        for (credential, symbol_key) in [
            ("spot.only", "binance|spot|BTC|USDT"),
            ("coin.margin", "binance|futures|BTC|USD|BTC"),
            ("foreign.exchange", "kraken|futures|BTC|USDT|USDT"),
            ("malformed.symbol", "BTCUSDT"),
        ] {
            seed(&store, credential, symbol_key, "open").await;
        }
        assert_eq!(
            active_usdm_credentials(&store).await.unwrap(),
            Vec::<String>::new()
        );
        assert_no_fill_authority(&store).await;
        store.pool().close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
