//! Bounded passive local account projection. No exchange client, repair, or write.
use crate::{proto_v2 as p, store::Store};
use sqlx::{any::AnyRow, Acquire, Row};
use std::time::Duration;
use tonic::{Request, Status};

pub(crate) const DEFAULT_LIMIT: usize = 500;
pub(crate) const MAX_LIMIT: usize = 1000;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

#[cfg(test)]
#[path = "account_order_read_mysql_tests.rs"]
mod mysql_tests;

fn text(row: &AnyRow, field: &str) -> Result<String, Status> {
    row.try_get(field)
        .map_err(|_| Status::internal("account-order stored text invalid"))
}
fn optional(row: &AnyRow, field: &str) -> Result<Option<String>, Status> {
    crate::store::try_get_nullable_string(row, field)
        .map(|v| v.filter(|s| !s.is_empty()))
        .map_err(|_| Status::internal("account-order stored optional text invalid"))
}
fn decimal(row: &AnyRow, field: &str) -> Result<String, Status> {
    let value = text(row, field)?;
    if crate::reconcile::snapshot_decimal_is_zero(&value).is_none() {
        return Err(Status::internal("account-order stored decimal invalid"));
    }
    Ok(value)
}
fn optional_decimal(row: &AnyRow, field: &str) -> Result<Option<String>, Status> {
    let value = optional(row, field)?;
    if value
        .as_ref()
        .is_some_and(|v| crate::reconcile::snapshot_decimal_is_zero(v).is_none())
    {
        return Err(Status::internal(
            "account-order stored optional decimal invalid",
        ));
    }
    Ok(value)
}
fn integer(row: &AnyRow, field: &str) -> Result<i64, Status> {
    let n = text(row, field)?
        .parse::<i64>()
        .map_err(|_| Status::internal("account-order stored integer invalid"))?;
    if n.abs_diff(0) > MAX_SAFE_INTEGER as u64 {
        return Err(Status::internal(
            "account-order stored integer exceeds safe range",
        ));
    }
    Ok(n)
}
fn optional_integer(row: &AnyRow, field: &str) -> Result<Option<i64>, Status> {
    optional(row, field)?
        .map(|v| {
            let n = v
                .parse::<i64>()
                .map_err(|_| Status::internal("account-order optional integer invalid"))?;
            if n.abs_diff(0) > MAX_SAFE_INTEGER as u64 {
                return Err(Status::internal(
                    "account-order optional integer exceeds safe range",
                ));
            }
            Ok(n)
        })
        .transpose()
}
fn boolean(row: &AnyRow, field: &str) -> Result<Option<bool>, Status> {
    Ok(match optional(row, field)?.as_deref() {
        Some("0") => Some(false),
        Some("1") => Some(true),
        _ => None,
    })
}
pub(crate) fn meta(
    count: usize,
    limit: usize,
    source: &str,
    complete: p::AccountOrderCoverage,
) -> p::AccountOrderSectionMeta {
    let overflow = count > limit;
    p::AccountOrderSectionMeta {
        schema_version: 1,
        coverage: if overflow {
            p::AccountOrderCoverage::AocPartial as i32
        } else {
            complete as i32
        },
        overflow,
        returned_count: count.min(limit) as u32,
        total_count: (!overflow).then_some(count as u32),
        source_kind: source.into(),
    }
}
fn scope_valid(value: &str) -> bool {
    if let Some(uid) = value.strip_prefix("paper:") {
        return uid.len() == 32
            && uid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    }
    !value.is_empty()
        && value.len() <= 10
        && value.as_bytes()[0] != b'0'
        && value.bytes().all(|b| b.is_ascii_digit())
}
fn budget(request: &Request<p::GetAccountOrderStateRequest>) -> Duration {
    let cap = Duration::from_secs(5);
    let Some(raw) = request
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
    else {
        return cap;
    };
    let bytes = raw.as_bytes();
    if !(2..=9).contains(&bytes.len()) {
        return Duration::ZERO;
    }
    let Ok(n) = raw[..raw.len() - 1].parse::<u64>() else {
        return Duration::ZERO;
    };
    let unit = match bytes[bytes.len() - 1] {
        b'H' => 3_600_000_000_000u128,
        b'M' => 60_000_000_000,
        b'S' => 1_000_000_000,
        b'm' => 1_000_000,
        b'u' => 1_000,
        b'n' => 1,
        _ => return Duration::ZERO,
    };
    Duration::from_nanos(((n as u128 * unit * 9 / 10).min(cap.as_nanos())) as u64)
}

pub(crate) async fn handle(
    store: &Store,
    request: Request<p::GetAccountOrderStateRequest>,
) -> Result<p::GetAccountOrderStateResponse, Status> {
    let deadline = tokio::time::Instant::now() + budget(&request);
    let req = request.into_inner();
    if req.schema_version != 1
        || !scope_valid(&req.credential_id)
        || req.max_items_per_section > MAX_LIMIT as u32
    {
        return Err(Status::invalid_argument(
            "invalid account-order read scope, limit, or schema",
        ));
    }
    if req
        .expected_boot_id
        .as_deref()
        .is_some_and(|v| v != store.boot_id())
    {
        return Err(Status::failed_precondition("engine boot changed"));
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(Status::deadline_exceeded(
            "account-order read budget exhausted",
        ));
    }
    let limit = if req.max_items_per_section == 0 {
        DEFAULT_LIMIT
    } else {
        req.max_items_per_section as usize
    };
    tokio::time::timeout_at(deadline, read(store, &req.credential_id, limit))
        .await
        .map_err(|_| Status::deadline_exceeded("account-order read budget exhausted"))?
}

async fn read(
    store: &Store,
    credential: &str,
    limit: usize,
) -> Result<p::GetAccountOrderStateResponse, Status> {
    let observed_at_ms = crate::svc::now_ms();
    // Set only the next transaction characteristics, never pool/session state.
    // MySQL defaults can be changed by operators: request a consistent read-only
    // snapshot explicitly. SQLite establishes its snapshot at the first SELECT.
    let mut connection = store
        .pool()
        .acquire()
        .await
        .map_err(|_| Status::unavailable("account-order read database unavailable"))?;
    #[cfg(test)]
    mysql_tests::checkpoint(store, &mut connection, mysql_tests::Point::Before).await;
    if store.kind() == crate::store::DbKind::MySql {
        // Cancellation between SET TRANSACTION and BEGIN must not leak the
        // next-transaction characteristics to an unrelated pool borrower.
        connection.close_on_drop();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *connection)
            .await
            .map_err(|_| Status::unavailable("account-order read transaction unavailable"))?;
        #[cfg(test)]
        mysql_tests::checkpoint(store, &mut connection, mysql_tests::Point::Configured).await;
    }
    let mut tx = connection
        .begin()
        .await
        .map_err(|_| Status::unavailable("account-order read database unavailable"))?;
    let query_limit = (limit + 1) as i64;
    let rows=sqlx::query(r#"
        SELECT engine_order_id,credential_id,bot_uid,position_id,order_role,symbol_key,
        action,side,order_type,quantity,price,CAST(reduce_only AS CHAR) AS reduce_only_text,
        status,client_order_id_short,exchange_order_id,
        CAST(created_at_ms AS CHAR) AS created_text,CAST(updated_at_ms AS CHAR) AS updated_text
        FROM engine_orders WHERE credential_id=? AND HEX(credential_id)=HEX(?)
        AND (status IS NULL OR HEX(status) NOT IN(HEX('filled'),HEX('canceled'),HEX('expired'),HEX('rejected'),HEX('failed')))
        ORDER BY updated_at_ms,engine_order_id LIMIT ?
    "#).bind(credential).bind(credential).bind(query_limit).fetch_all(&mut *tx).await.map_err(|_|Status::unavailable("account-order orders read failed"))?;
    #[cfg(test)]
    mysql_tests::checkpoint(store, &mut tx, mysql_tests::Point::Orders).await;
    let orders_meta = meta(
        rows.len(),
        limit,
        "rustcore_local_store",
        p::AccountOrderCoverage::AocCompleteLocal,
    );
    let orders = rows
        .iter()
        .take(limit)
        .map(|r| {
            let status = text(r, "status")?;
            let classified = match status.as_str() {
                "pending_risk" | "pending_submit" | "submitted" => "pending",
                "acked" | "partial_filled" | "confirmed" | "triggering" => "working",
                _ => "unknown",
            };
            Ok(p::LocalAccountOrder {
                schema_version: 1,
                engine_order_id: text(r, "engine_order_id")?,
                credential_id: text(r, "credential_id")?,
                bot_uid: text(r, "bot_uid")?,
                position_id: optional(r, "position_id")?,
                order_role: optional(r, "order_role")?,
                symbol_key: text(r, "symbol_key")?,
                action: text(r, "action")?,
                side: text(r, "side")?,
                order_type: text(r, "order_type")?,
                quantity: decimal(r, "quantity")?,
                price: optional_decimal(r, "price")?,
                reduce_only: boolean(r, "reduce_only_text")?,
                raw_status: status,
                classified_state: classified.into(),
                client_order_id: optional(r, "client_order_id_short")?,
                exchange_order_id: optional(r, "exchange_order_id")?,
                created_at_ms: integer(r, "created_text")?,
                updated_at_ms: integer(r, "updated_text")?,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let rows=sqlx::query(r#"
        SELECT ps.position_id,ps.credential_id,ps.bot_uid,ps.symbol_key,ps.direction,ps.qty,ps.state,
        CAST(ps.updated_at_ms AS CHAR) AS updated_text,
        CAST(CASE WHEN HEX(ps.state)=HEX('closed') AND HEX(ps.qty)=HEX('0')
          AND EXISTS(SELECT 1 FROM protection_cleanup_obligations pc
            WHERE pc.position_id=ps.position_id AND HEX(pc.position_id)=HEX(ps.position_id) AND HEX(pc.credential_id)=HEX(ps.credential_id)
              AND HEX(pc.bot_uid)=HEX(ps.bot_uid) AND HEX(pc.symbol_key)=HEX(ps.symbol_key)
              AND HEX(pc.direction)=HEX(ps.direction) AND pc.close_revision=ps.close_revision
              AND HEX(pc.state)=HEX('complete') AND HEX(pc.complete_reason)=HEX('paper_run_no_venue_protection')
              AND pc.completed_at_ms IS NOT NULL)
          AND NOT EXISTS(SELECT 1 FROM protection_orders ack
            WHERE ack.position_id=ps.position_id AND HEX(ack.position_id)=HEX(ps.position_id)
              AND (TRIM(COALESCE(ack.exchange_algo_id,''))<>'' OR TRIM(COALESCE(ack.exchange_order_id,''))<>''))
          AND NOT EXISTS(SELECT 1 FROM engine_orders ack
            WHERE ack.position_id=ps.position_id AND HEX(ack.position_id)=HEX(ps.position_id)
              AND HEX(ack.order_role) IN(HEX('protection_sl'),HEX('protection_tp'),HEX('emergency'))
              AND TRIM(COALESCE(ack.exchange_order_id,''))<>'')
          THEN 1 ELSE 0 END AS CHAR) AS paper_cleanup_text
        FROM position_state ps WHERE ps.credential_id=? AND HEX(ps.credential_id)=HEX(?) AND (
          NOT(HEX(ps.state)=HEX('closed') AND HEX(ps.qty)=HEX('0')) OR EXISTS(
            SELECT 1 FROM protection_orders po WHERE HEX(po.credential_id)=HEX(ps.credential_id)
            AND HEX(po.position_id)=HEX(ps.position_id) AND HEX(po.status) NOT IN(HEX('filled'),HEX('canceled'),HEX('expired'),HEX('rejected'),HEX('failed'))))
        ORDER BY ps.updated_at_ms,ps.position_id LIMIT ?
    "#).bind(credential).bind(credential).bind(query_limit).fetch_all(&mut *tx).await.map_err(|_|Status::unavailable("account-order positions read failed"))?;
    let positions_meta = meta(
        rows.len(),
        limit,
        "rustcore_local_store",
        p::AccountOrderCoverage::AocCompleteLocal,
    );
    let positions = rows
        .iter()
        .take(limit)
        .map(|r| {
            Ok(p::LocalProtectionPosition {
                schema_version: 1,
                position_id: text(r, "position_id")?,
                credential_id: text(r, "credential_id")?,
                bot_uid: text(r, "bot_uid")?,
                symbol_key: text(r, "symbol_key")?,
                direction: text(r, "direction")?,
                quantity: decimal(r, "qty")?,
                raw_state: text(r, "state")?,
                updated_at_ms: integer(r, "updated_text")?,
                // Preserve all historical rows. Consumers may retire only positively
                // proved paper obligations; missing evidence is not a measured flat.
                paper_cleanup_complete: if credential.starts_with("paper:") {
                    boolean(r, "paper_cleanup_text")?
                } else {
                    None
                },
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let rows=sqlx::query(r#"
        SELECT pp.position_id,pp.credential_id,pp.bot_uid,pp.symbol_key,pp.direction,
        CAST(pp.required AS CHAR) AS required_text,CAST(pp.generation AS CHAR) AS generation_text,
        pp.stop_loss_price,pp.take_profit_price,CAST(pp.updated_at_ms AS CHAR) AS updated_text,
        ps.position_id AS joined_position,ps.bot_uid AS position_bot,ps.symbol_key AS position_symbol,ps.direction AS position_direction
        FROM position_protection_plans pp LEFT JOIN position_state ps ON HEX(ps.position_id)=HEX(pp.position_id) AND HEX(ps.credential_id)=HEX(pp.credential_id)
        WHERE pp.credential_id=? AND HEX(pp.credential_id)=HEX(?) AND (ps.position_id IS NULL
          OR NOT(HEX(ps.state)=HEX('closed') AND HEX(ps.qty)=HEX('0')) OR EXISTS(
            SELECT 1 FROM protection_orders po WHERE HEX(po.credential_id)=HEX(pp.credential_id)
            AND HEX(po.position_id)=HEX(pp.position_id) AND HEX(po.status) NOT IN(HEX('filled'),HEX('canceled'),HEX('expired'),HEX('rejected'),HEX('failed'))))
        ORDER BY pp.updated_at_ms,pp.position_id LIMIT ?
    "#).bind(credential).bind(credential).bind(query_limit).fetch_all(&mut *tx).await.map_err(|_|Status::unavailable("account-order plans read failed"))?;
    let plans_meta = meta(
        rows.len(),
        limit,
        "rustcore_local_store",
        p::AccountOrderCoverage::AocCompleteLocal,
    );
    let plans = rows
        .iter()
        .take(limit)
        .map(|r| {
            let association = if optional(r, "joined_position")?.is_none() {
                "missing_position"
            } else if optional(r, "position_bot")?.as_deref() != Some(text(r, "bot_uid")?.as_str())
                || optional(r, "position_symbol")?.as_deref()
                    != Some(text(r, "symbol_key")?.as_str())
                || optional(r, "position_direction")?.as_deref()
                    != Some(text(r, "direction")?.as_str())
            {
                "inconsistent"
            } else {
                "matched"
            };
            Ok(p::LocalProtectionPlan {
                schema_version: 1,
                position_id: text(r, "position_id")?,
                credential_id: text(r, "credential_id")?,
                bot_uid: text(r, "bot_uid")?,
                symbol_key: text(r, "symbol_key")?,
                direction: text(r, "direction")?,
                required: boolean(r, "required_text")?,
                generation: integer(r, "generation_text")?,
                stop_loss_price: decimal(r, "stop_loss_price")?,
                take_profit_price: optional_decimal(r, "take_profit_price")?,
                updated_at_ms: integer(r, "updated_text")?,
                association_status: association.into(),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let rows=sqlx::query(r#"
        SELECT po.protection_order_id,po.credential_id,po.position_id,po.kind,
        CAST(po.generation AS CHAR) AS generation_text,po.engine_order_id,po.exchange_algo_id,
        po.client_algo_id,po.exchange_order_id,po.algo_status,po.trigger_price,
        CAST(po.reduce_only AS CHAR) AS reduce_text,CAST(po.close_position AS CHAR) AS close_text,
        po.status,CAST(po.verified_from_exchange AS CHAR) AS verified_text,
        CAST(po.last_verified_at_ms AS CHAR) AS verified_at_text,CAST(po.algo_updated_at_ms AS CHAR) AS algo_at_text,
        CAST(po.updated_at_ms AS CHAR) AS updated_text,ps.position_id AS joined_position,
        eo.engine_order_id AS joined_engine,pp.position_id AS joined_plan,CAST(pp.generation AS CHAR) AS plan_generation_text
        FROM protection_orders po
        LEFT JOIN position_state ps ON HEX(ps.position_id)=HEX(po.position_id) AND HEX(ps.credential_id)=HEX(po.credential_id)
        LEFT JOIN engine_orders eo ON HEX(eo.engine_order_id)=HEX(po.engine_order_id) AND HEX(eo.credential_id)=HEX(po.credential_id) AND HEX(eo.position_id)=HEX(po.position_id)
        LEFT JOIN position_protection_plans pp ON HEX(pp.position_id)=HEX(po.position_id) AND HEX(pp.credential_id)=HEX(po.credential_id)
        WHERE po.credential_id=? AND HEX(po.credential_id)=HEX(?) AND (
          HEX(po.status) NOT IN(HEX('filled'),HEX('canceled'),HEX('expired'),HEX('rejected'),HEX('failed'))
          OR ps.position_id IS NULL OR eo.engine_order_id IS NULL OR pp.position_id IS NULL
          OR NOT(HEX(ps.state)=HEX('closed') AND HEX(ps.qty)=HEX('0')))
        ORDER BY po.updated_at_ms,po.protection_order_id LIMIT ?
    "#).bind(credential).bind(credential).bind(query_limit).fetch_all(&mut *tx).await.map_err(|_|Status::unavailable("account-order protection read failed"))?;
    let protection_meta = meta(
        rows.len(),
        limit,
        "rustcore_local_store",
        p::AccountOrderCoverage::AocCompleteLocal,
    );
    let protection = rows
        .iter()
        .take(limit)
        .map(|r| {
            let generation = integer(r, "generation_text")?;
            let algo = optional(r, "exchange_algo_id")?;
            let client_algo = optional(r, "client_algo_id")?;
            let exchange_id = optional(r, "exchange_order_id")?;
            let modern = algo.is_some();
            let association = if optional(r, "joined_position")?.is_none() {
                "missing_position"
            } else if optional(r, "joined_engine")?.is_none() {
                "missing_engine"
            } else if optional(r, "joined_plan")?.is_none() {
                "missing_plan"
            } else {
                "matched"
            };
            Ok(p::LocalProtectionOrder {
                schema_version: 1,
                protection_order_id: text(r, "protection_order_id")?,
                credential_id: text(r, "credential_id")?,
                position_id: text(r, "position_id")?,
                kind: text(r, "kind")?,
                generation,
                engine_order_id: optional(r, "engine_order_id")?,
                exchange_algo_id: algo,
                client_algo_id: client_algo,
                actual_order_id: if modern { exchange_id.clone() } else { None },
                legacy_exchange_order_id: if modern { None } else { exchange_id },
                algo_status: optional(r, "algo_status")?,
                trigger_price: decimal(r, "trigger_price")?,
                reduce_only: boolean(r, "reduce_text")?,
                close_position: boolean(r, "close_text")?,
                raw_status: text(r, "status")?,
                verified_projection: boolean(r, "verified_text")?,
                last_verified_at_ms: optional_integer(r, "verified_at_text")?,
                algo_updated_at_ms: optional_integer(r, "algo_at_text")?,
                updated_at_ms: integer(r, "updated_text")?,
                association_status: association.into(),
                current_plan_generation_match: optional_integer(r, "plan_generation_text")?
                    .map(|v| v == generation),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    tx.rollback()
        .await
        .map_err(|_| Status::unavailable("account-order read transaction close failed"))?;
    #[cfg(test)]
    mysql_tests::checkpoint(store, &mut connection, mysql_tests::Point::Finished).await;
    let kind = if credential.starts_with("paper:") {
        "simulation"
    } else {
        "exchange"
    };
    Ok(p::GetAccountOrderStateResponse {
        credential_id: credential.into(),
        engine_boot_id: store.boot_id().into(),
        account_kind: kind.into(),
        observed_at_ms,
        local: Some(p::LocalAccountOrderProjection {
            schema_version: 1,
            orders: Some(p::LocalAccountOrderSection {
                schema_version: 1,
                meta: Some(orders_meta),
                rows: orders,
            }),
            positions: Some(p::LocalProtectionPositionSection {
                schema_version: 1,
                meta: Some(positions_meta),
                rows: positions,
            }),
            protection_plans: Some(p::LocalProtectionPlanSection {
                schema_version: 1,
                meta: Some(plans_meta),
                rows: plans,
            }),
            protection_orders: Some(p::LocalProtectionOrderSection {
                schema_version: 1,
                meta: Some(protection_meta),
                rows: protection,
            }),
        }),
        venue: Some(crate::account_order_observation::read(
            store,
            credential,
            limit,
            observed_at_ms,
        )),
        schema_version: 1,
    })
}
