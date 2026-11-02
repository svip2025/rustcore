//! B3-K1 durable, exact-target close. This module coordinates authority;
//! risk, order insertion and paper fills remain in the shared OMS transaction.
//!
//! WO-M1（D29/D29a）· 交易所凭据同一套持久请求 / 幂等 / fence：派发事务只把
//! reduce-only 市价单与风控裁决一起落库（`dispatched`），结局由交易所真值
//! 路径终结（[`settle_exchange_closes`]：累计成交=目标 ∧ 同轮 positionRisk
//! 显示该 scope 归零 ∧ 本地仓位已 closed）；终结之前 fence 一直挂着。该仓的
//! SL/TP 不在平仓路径上撤——交易所确认归零之后由 D30 善后（WO-M0）撤销。
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tonic::{Request, Status};

use crate::{
    position_state::PreparedPositionIntent,
    proto, proto_v2 as p,
    store::{DbKind, Store},
};
type Tx<'a> = sqlx::Transaction<'a, sqlx::Any>;
fn db(error: sqlx::Error) -> Status {
    Status::internal(format!("position close store: {error}"))
}
fn hash(value: &serde_json::Value) -> String {
    crate::svc::sha256_hex(&value.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    position_id: String,
    revision: u64,
    bot_uid: String,
    symbol_key: String,
    direction: String,
    quantity: String,
    entry_epoch: u64,
    exit_epoch: u64,
    run_mode: String,
    execution_venue: String,
    entry_engine_order_id: String,
}
impl Target {
    fn wire(&self) -> p::PositionCloseTarget {
        p::PositionCloseTarget {
            position_id: self.position_id.clone(),
            revision: self.revision,
            bot_uid: self.bot_uid.clone(),
            symbol_key: self.symbol_key.clone(),
            direction: self.direction.clone(),
            quantity: self.quantity.clone(),
            entry_epoch: self.entry_epoch,
            exit_epoch: self.exit_epoch,
            run_mode: self.run_mode.clone(),
            execution_venue: self.execution_venue.clone(),
            entry_engine_order_id: self.entry_engine_order_id.clone(),
            schema_version: 1,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct ExactCloseContext {
    pub credential_id: String,
    pub request_ref: String,
    pub params_hash: String,
    pub scope_key: String,
    pub target: Target,
    idempotency_key: String,
    client_order_id_short: String,
    // Reporting attribution comes only from this target's immutable entry order.
    // It is not part of the close authority or persisted request identity.
    reporting_run_id: Option<String>,
}
struct RowTarget {
    target: Target,
    state: String,
    proof_valid: bool,
    /// 交易所凭据：同 credential + 同 USDⓈ-M symbol + 同方向还有别的活仓
    /// （单向持仓下交易所只有一个聚合仓位，「这一个」归零无法被真值证明）。
    scope_shared: bool,
}
fn read_u64(row: &sqlx::any::AnyRow, key: &str) -> Result<u64, Status> {
    u64::try_from(row.try_get::<i64, _>(key).map_err(db)?)
        .map_err(|_| Status::failed_precondition("POSITION_REVISION_INVALID"))
}
fn schema(version: u32) -> Result<(), Status> {
    if version != 1 {
        return Err(Status::invalid_argument("SCHEMA_VERSION_UNSUPPORTED"));
    }
    Ok(())
}
fn identity(credential: &str, reference: &str, actor: u64) -> Result<(), Status> {
    if credential.is_empty()
        || credential.len() > 64
        || credential.trim() != credential
        || actor == 0
        || actor > 9_007_199_254_740_991
        || ulid::Ulid::from_string(reference)
            .ok()
            .is_none_or(|id| id.to_string() != reference)
    {
        return Err(Status::invalid_argument("REQUEST_IDENTITY_INVALID"));
    }
    Ok(())
}
fn params(request: &p::ClosePositionRequest) -> String {
    hash(&serde_json::json!([
        1,
        request.credential_id,
        request.request_ref,
        request.actor_member_id.to_string(),
        request.position_id,
        request.expected_revision.to_string(),
        request.expected_boot_id
    ]))
}
fn scope(credential: &str, symbol: &str, direction: &str) -> String {
    let symbol = crate::binance_futures::usdm_symbol(symbol).unwrap_or_else(|| symbol.to_owned());
    hash(&serde_json::json!([
        "position-close-scope.1",
        credential,
        symbol,
        direction
    ]))
}

/// Both production Entry and B3 prepare/dispatch take this lock FIRST. The
/// durable pointer remains set across process death; it is not a time lease.
pub(crate) async fn lock_scope(
    tx: &mut Tx<'_>,
    kind: DbKind,
    key: &str,
) -> Result<Option<String>, Status> {
    let insert = match kind {
        DbKind::MySql => "INSERT INTO position_close_scope_locks(scope_key,pending_request_ref) VALUES(?,NULL) ON DUPLICATE KEY UPDATE scope_key=scope_key",
        DbKind::Sqlite => "INSERT INTO position_close_scope_locks(scope_key,pending_request_ref) VALUES(?,NULL) ON CONFLICT(scope_key) DO NOTHING",
        DbKind::Unknown => return Err(Status::internal("unknown scope-lock dialect")),
    };
    sqlx::query(insert)
        .bind(key)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    let query = if kind == DbKind::MySql {
        "SELECT COALESCE(pending_request_ref,'') AS pending FROM position_close_scope_locks WHERE scope_key=? FOR UPDATE"
    } else {
        // INSERT above already owns SQLite's writer lock.
        "SELECT COALESCE(pending_request_ref,'') AS pending FROM position_close_scope_locks WHERE scope_key=?"
    };
    let row = sqlx::query(query)
        .bind(key)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    let pending: String = row.get("pending");
    Ok((!pending.is_empty()).then_some(pending))
}

/// Simulation and exchange entries/close-family share the scope lock (WO-M1
/// extends it to exchange credentials). A pending exact close rejects ONLY
/// entry; all existing reduce-only channels remain usable.
pub(crate) async fn lock_intent_scope(
    tx: &mut Tx<'_>,
    kind: DbKind,
    credential: &str,
    intent: &proto::OrderIntent,
) -> Result<bool, Status> {
    if crate::venue::credential_kind(credential) == crate::venue::CredentialKind::UnprovenIdentity {
        return Ok(false);
    }
    let direction = match intent.action.as_str() {
        "open_long" | "close_long" => "long",
        "open_short" | "close_short" => "short",
        _ => return Ok(false),
    };
    let pending = lock_scope(tx, kind, &scope(credential, &intent.symbol_key, direction)).await?;
    Ok(intent.action.starts_with("open_") && pending.is_some())
}

async fn read_target(
    tx: &mut Tx<'_>,
    credential: &str,
    position: &str,
    lock: bool,
    kind: DbKind,
) -> Result<Option<RowTarget>, Status> {
    let suffix = if lock && kind == DbKind::MySql {
        " FOR UPDATE"
    } else {
        ""
    };
    let query = format!("SELECT position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,exit_epoch,close_revision,state,qty FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?){suffix}");
    let Some(row) = sqlx::query(&query)
        .bind(credential)
        .bind(position)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
    else {
        return Ok(None);
    };
    let mut target = Target {
        position_id: row.get("position_id"),
        revision: read_u64(&row, "close_revision")?,
        bot_uid: row.get("bot_uid"),
        symbol_key: row.get("symbol_key"),
        direction: row.get("direction"),
        quantity: row.get("qty"),
        entry_epoch: read_u64(&row, "entry_epoch")?,
        exit_epoch: read_u64(&row, "exit_epoch")?,
        run_mode: String::new(),
        execution_venue: String::new(),
        entry_engine_order_id: String::new(),
    };
    let proof = sqlx::query("SELECT credential_id,bot_uid,symbol_key,direction,entry_epoch,entry_engine_order_id,run_mode,execution_venue FROM position_execution_provenance WHERE HEX(position_id)=HEX(?)")
        .bind(position).fetch_optional(&mut **tx).await.map_err(db)?;
    let mut proof_valid = false;
    if let Some(proof) = proof {
        target.run_mode = proof.get("run_mode");
        target.execution_venue = proof.get("execution_venue");
        target.entry_engine_order_id = proof.get("entry_engine_order_id");
        proof_valid = proof.get::<String, _>("credential_id") == credential
            && proof.get::<String, _>("bot_uid") == target.bot_uid
            && proof.get::<String, _>("symbol_key") == target.symbol_key
            && proof.get::<String, _>("direction") == target.direction
            && read_u64(&proof, "entry_epoch")? == target.entry_epoch;
        let order = sqlx::query("SELECT credential_id,position_id,order_role,status,bot_uid,symbol_key,action FROM engine_orders WHERE HEX(engine_order_id)=HEX(?)")
            .bind(&target.entry_engine_order_id).fetch_optional(&mut **tx).await.map_err(db)?;
        proof_valid &= order.is_some_and(|o| {
            o.get::<String, _>("credential_id") == credential
                && o.get::<String, _>("position_id") == position
                && o.get::<String, _>("order_role") == "entry"
                && o.get::<String, _>("status") == "filled"
                && o.get::<String, _>("bot_uid") == target.bot_uid
                && o.get::<String, _>("symbol_key") == target.symbol_key
                && o.get::<String, _>("action") == format!("open_{}", target.direction)
        });
    }
    let scope_shared = if crate::venue::credential_kind(credential)
        == crate::venue::CredentialKind::ExchangeBacked
    {
        let others = sqlx::query("SELECT symbol_key FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(direction)=HEX(?) AND HEX(position_id)<>HEX(?) AND HEX(state)<>HEX('closed') LIMIT 1001")
            .bind(credential).bind(&target.direction).bind(position)
            .fetch_all(&mut **tx).await.map_err(db)?;
        let own = crate::binance_futures::usdm_symbol(&target.symbol_key);
        // 上界溢出不是「没有兄弟仓」的证据。
        others.len() > 1000
            || others.iter().any(|other| {
                own.is_some()
                    && crate::binance_futures::usdm_symbol(&other.get::<String, _>("symbol_key"))
                        == own
            })
    } else {
        false
    };
    Ok(Some(RowTarget {
        target,
        state: row.get("state"),
        proof_valid,
        scope_shared,
    }))
}
/// Missing historical reporting identity must not block a reduce-only close.
/// Never borrow an active bot run: the original entry is the only attribution.
async fn entry_reporting_run(
    tx: &mut Tx<'_>,
    credential: &str,
    target: &Target,
) -> Result<Option<String>, Status> {
    let row = sqlx::query(
        "SELECT COALESCE(run_id,'') AS run_id FROM engine_orders \
         WHERE HEX(engine_order_id)=HEX(?) AND HEX(credential_id)=HEX(?) \
         AND HEX(bot_uid)=HEX(?) AND HEX(position_id)=HEX(?) \
         AND order_role='entry' AND status='filled'",
    )
    .bind(&target.entry_engine_order_id)
    .bind(credential)
    .bind(&target.bot_uid)
    .bind(&target.position_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;
    Ok(row
        .map(|row| row.get::<String, _>("run_id"))
        .filter(|run| !run.is_empty() && run.trim() == run))
}

fn is_exchange(credential: &str) -> bool {
    crate::venue::credential_kind(credential) == crate::venue::CredentialKind::ExchangeBacked
}

/// 交易所仓位的执行来源必须是生产 Entry 在**当前** venue 配置下写下的：
/// exchange-backed 档位 + 与凭据现配置相同的 venue（venue 被改配或撤配后，
/// 旧仓位的平仓会发到另一个交易所环境，拒绝而不是猜）。
fn exchange_provenance_supported(target: &Target, credential: &str) -> bool {
    matches!(target.run_mode.as_str(), "demo" | "live_small" | "live")
        && crate::venue::venue_for_credential(credential)
            .ok()
            .flatten()
            .is_some_and(|venue| venue.to_string() == target.execution_venue)
}

fn unavailable(row: &RowTarget, credential: &str) -> Option<&'static str> {
    let exchange = match crate::venue::credential_kind(credential) {
        crate::venue::CredentialKind::Simulation => false,
        crate::venue::CredentialKind::ExchangeBacked => true,
        crate::venue::CredentialKind::UnprovenIdentity => {
            return Some("POSITION_PROVENANCE_UNSUPPORTED")
        }
    };
    if !row.proof_valid || row.target.revision == 0 {
        return Some("POSITION_PROVENANCE_UNKNOWN");
    }
    let provenance_supported = if exchange {
        exchange_provenance_supported(&row.target, credential)
    } else {
        row.target.run_mode == "paper" && row.target.execution_venue == "simulation"
    };
    if !provenance_supported {
        return Some("POSITION_PROVENANCE_UNSUPPORTED");
    }
    if row.scope_shared {
        return Some("POSITION_SCOPE_SHARED");
    }
    if !matches!(row.state.as_str(), "open" | "protected") {
        return Some("POSITION_NOT_CLOSABLE");
    }
    if !matches!(row.target.direction.as_str(), "long" | "short")
        || crate::exchange_filters::validate_authoritative_position_quantity(&row.target.quantity)
            .is_err()
        || crate::binance_futures::usdm_symbol(&row.target.symbol_key).is_none()
    {
        return Some("POSITION_TARGET_INVALID");
    }
    None
}

pub(crate) async fn get_target(
    store: &Store,
    request: p::GetPositionCloseTargetRequest,
) -> Result<p::GetPositionCloseTargetResponse, Status> {
    schema(request.schema_version)?;
    let mut tx = store.pool().begin().await.map_err(db)?;
    let row = read_target(
        &mut tx,
        &request.credential_id,
        &request.position_id,
        false,
        store.kind(),
    )
    .await?;
    let reason = row.as_ref().map_or(Some("POSITION_NOT_FOUND"), |r| {
        unavailable(r, &request.credential_id)
    });
    let target = if reason.is_none() {
        row.map(|r| r.target.wire())
    } else {
        None
    };
    Ok(p::GetPositionCloseTargetResponse {
        available: reason.is_none(),
        reason_code: reason.unwrap_or("AVAILABLE").into(),
        observed_at_ms: crate::svc::now_ms(),
        engine_boot_id: store.boot_id().into(),
        target,
        schema_version: 1,
    })
}

fn unknown() -> p::PositionCloseObservation {
    p::PositionCloseObservation {
        status: "unknown".into(),
        schema_version: 1,
        ..Default::default()
    }
}
fn rejected(reason: &str) -> p::ClosePositionResponse {
    p::ClosePositionResponse {
        found: false,
        request_status: "rejected".into(),
        reason_code: reason.into(),
        execution_outcome: Some(unknown()),
        position_observation: Some(unknown()),
        protection_observation: Some(p::PositionCloseProtectionObservation {
            status: "unknown".into(),
            reason_code: "PROTECTION_CLEANUP_NOT_PROVEN".into(),
            schema_version: 1,
        }),
        schema_version: 1,
        ..Default::default()
    }
}
async fn request_row(
    tx: &mut Tx<'_>,
    credential: &str,
    reference: &str,
    kind: DbKind,
    lock: bool,
) -> Result<Option<sqlx::any::AnyRow>, Status> {
    let suffix = if lock && kind == DbKind::MySql {
        " FOR UPDATE"
    } else {
        ""
    };
    sqlx::query(&format!("SELECT *,COALESCE(engine_order_id,'') AS order_id,COALESCE(risk_verdict_id,'') AS verdict_id,COALESCE(outcome_json,'') AS outcome FROM position_close_requests WHERE credential_id=? AND request_ref=?{suffix}"))
        .bind(credential).bind(reference).fetch_optional(&mut **tx).await.map_err(db)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Outcome {
    position_id: String,
    revision: u64,
    quantity: String,
    state: String,
    observed_at_ms: i64,
}
impl Outcome {
    fn wire(self, source: &str) -> p::PositionCloseObservation {
        p::PositionCloseObservation {
            status: "observed".into(),
            position_id: self.position_id,
            revision: self.revision,
            quantity: Some(self.quantity),
            state: Some(self.state),
            observed_at_ms: Some(self.observed_at_ms),
            source: source.into(),
            schema_version: 1,
        }
    }
}
async fn receipt_tx(
    tx: &mut Tx<'_>,
    store: &Store,
    row: sqlx::any::AnyRow,
) -> Result<p::ClosePositionResponse, Status> {
    let credential: String = row.get("credential_id");
    if is_exchange(&credential) {
        return exchange_receipt_tx(tx, store, row).await;
    }
    let position: String = row.get("position_id");
    let phase: String = row.get("phase");
    let boot: String = row.get("expected_boot_id");
    let current = sqlx::query("SELECT position_id,close_revision,qty,state FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?)")
        .bind(&credential).bind(&position).fetch_optional(&mut **tx).await.map_err(db)?;
    let observation = if let Some(current) = current {
        Outcome {
            position_id: current.get("position_id"),
            revision: read_u64(&current, "close_revision")?,
            quantity: current.get("qty"),
            state: current.get("state"),
            observed_at_ms: crate::svc::now_ms(),
        }
        .wire("position_store")
    } else {
        unknown()
    };
    let outcome = crate::store::try_get_text(&row, "outcome")
        .map_err(db)?
        .ok_or_else(|| Status::internal("missing frozen close outcome"))?;
    let frozen = if outcome.is_empty() {
        unknown()
    } else {
        serde_json::from_str::<Outcome>(&outcome)
            .map_err(|_| Status::internal("invalid frozen close outcome"))?
            .wire("paper_transaction")
    };
    let order: String = row.get("order_id");
    let verdict: String = row.get("verdict_id");
    Ok(p::ClosePositionResponse {
        found: true,
        request_ref: row.get("request_ref"),
        position_id: position,
        target_revision: read_u64(&row, "expected_revision")?,
        request_status: if phase == "prepared" && boot != store.boot_id() {
            "pending_unknown".into()
        } else {
            phase
        },
        reason_code: if boot != store.boot_id() && row.get::<String, _>("phase") == "prepared" {
            "PREPARED_PREVIOUS_BOOT".into()
        } else {
            row.get("reason_code")
        },
        engine_order_id: (!order.is_empty()).then_some(order),
        risk_verdict_id: (!verdict.is_empty()).then_some(verdict),
        execution_outcome: Some(frozen),
        position_observation: Some(observation),
        protection_observation: rejected("").protection_observation,
        filled_quantity: None,
        target_quantity: None,
        schema_version: 1,
    })
}
pub(crate) async fn get_receipt(
    store: &Store,
    request: p::GetPositionCloseReceiptRequest,
) -> Result<p::ClosePositionResponse, Status> {
    schema(request.schema_version)?;
    identity(
        &request.credential_id,
        &request.request_ref,
        request.actor_member_id,
    )?;
    let mut tx = store.pool().begin().await.map_err(db)?;
    let Some(row) = request_row(
        &mut tx,
        &request.credential_id,
        &request.request_ref,
        store.kind(),
        false,
    )
    .await?
    else {
        return Ok(rejected("REQUEST_NOT_FOUND"));
    };
    if row.get::<i64, _>("actor_member_id") != request.actor_member_id as i64
        || row.get::<String, _>("credential_id") != request.credential_id
    {
        return Ok(rejected("REQUEST_NOT_FOUND"));
    }
    let stranded = stranded_prepared(&row, store);
    if row.get::<String, _>("phase") != PHASE_DISPATCHED && !stranded {
        return receipt_tx(&mut tx, store, row).await;
    }
    drop(tx);
    if stranded {
        // WO-M1b：前一 boot 未派发的交易所 prepared 请求，可证明未发出时在读
        // 回执前如实终结（不发任何请求）；证明不成立则原样只读。
        resolve_stranded_prepared(store, &request.credential_id, &request.request_ref).await?;
    } else {
        // WO-M1：已记录的交易所终态（拒单 / 撤销 / 过期 / 查无此单）不需要任何
        // 交易所调用即可终结；在读回执前按同一套真值规则落定（不发任何请求）。
        // 「已成交」的终结仍只属于带 positionRisk 真值的对账路径。
        settle_request(
            store,
            &request.credential_id,
            &request.request_ref,
            None,
            crate::svc::now_ms(),
        )
        .await?;
    }
    let mut tx = store.pool().begin().await.map_err(db)?;
    let row = request_row(
        &mut tx,
        &request.credential_id,
        &request.request_ref,
        store.kind(),
        false,
    )
    .await?
    .ok_or_else(|| Status::internal("close request disappeared during settlement"))?;
    receipt_tx(&mut tx, store, row).await
}

pub(crate) enum Prepare {
    Dispatch(ExactCloseContext),
    Receipt(p::ClosePositionResponse),
}
pub(crate) async fn prepare(
    store: &Store,
    request: &p::ClosePositionRequest,
) -> Result<Prepare, Status> {
    schema(request.schema_version)?;
    identity(
        &request.credential_id,
        &request.request_ref,
        request.actor_member_id,
    )?;
    if request.position_id.is_empty()
        || request.position_id.len() > 64
        || request.expected_revision == 0
        || request.expected_revision > i64::MAX as u64
    {
        return Err(Status::invalid_argument("POSITION_TARGET_INVALID"));
    }
    let commitment = params(request);
    let mut tx = store.pool().begin().await.map_err(db)?;
    // Existing request identity is resolved before the current target. It must
    // stay retryable even after that target has closed or been reopened.
    if let Some(row) = request_row(
        &mut tx,
        &request.credential_id,
        &request.request_ref,
        store.kind(),
        false,
    )
    .await?
    {
        if row.get::<String, _>("params_hash") != commitment {
            return Ok(Prepare::Receipt(rejected("REQUEST_IDENTITY_CONFLICT")));
        }
        if row.get::<String, _>("phase") != "prepared"
            || row.get::<String, _>("expected_boot_id") != store.boot_id()
        {
            return Ok(Prepare::Receipt(receipt_tx(&mut tx, store, row).await?));
        }
        let target_json = crate::store::try_get_text(&row, "target_json")
            .map_err(db)?
            .ok_or_else(|| Status::internal("missing close target receipt"))?;
        let target: Target = serde_json::from_str(&target_json)
            .map_err(|_| Status::internal("invalid close target receipt"))?;
        let reporting_run_id =
            entry_reporting_run(&mut tx, &request.credential_id, &target).await?;
        return Ok(Prepare::Dispatch(ExactCloseContext {
            reporting_run_id,
            credential_id: request.credential_id.clone(),
            request_ref: request.request_ref.clone(),
            params_hash: commitment,
            scope_key: row.get("scope_key"),
            idempotency_key: row.get("idempotency_key"),
            client_order_id_short: row.get("client_order_id_short"),
            target,
        }));
    }
    if request.expected_boot_id != store.boot_id() {
        return Ok(Prepare::Receipt(rejected("ENGINE_BOOT_MISMATCH")));
    }
    let Some(initial) = read_target(
        &mut tx,
        &request.credential_id,
        &request.position_id,
        false,
        store.kind(),
    )
    .await?
    else {
        return Ok(Prepare::Receipt(rejected("POSITION_NOT_FOUND")));
    };
    if let Some(reason) = unavailable(&initial, &request.credential_id) {
        return Ok(Prepare::Receipt(rejected(reason)));
    }
    let key = scope(
        &request.credential_id,
        &initial.target.symbol_key,
        &initial.target.direction,
    );
    let pending = lock_scope(&mut tx, store.kind(), &key).await?;
    // The same request may have won while this transaction waited for scope.
    if let Some(row) = request_row(
        &mut tx,
        &request.credential_id,
        &request.request_ref,
        store.kind(),
        true,
    )
    .await?
    {
        if row.get::<String, _>("params_hash") != commitment {
            return Ok(Prepare::Receipt(rejected("REQUEST_IDENTITY_CONFLICT")));
        }
        return Ok(Prepare::Receipt(receipt_tx(&mut tx, store, row).await?));
    }
    if pending.is_some() {
        return Ok(Prepare::Receipt(rejected("POSITION_CLOSE_PENDING")));
    }
    crate::risk::lock_notional_reservation(&mut tx, store.kind(), &request.credential_id).await?;
    let Some(current) = read_target(
        &mut tx,
        &request.credential_id,
        &request.position_id,
        true,
        store.kind(),
    )
    .await?
    else {
        return Ok(Prepare::Receipt(rejected("POSITION_NOT_FOUND")));
    };
    if current.target.revision != request.expected_revision {
        return Ok(Prepare::Receipt(rejected("POSITION_REVISION_MISMATCH")));
    }
    if let Some(reason) = unavailable(&current, &request.credential_id) {
        return Ok(Prepare::Receipt(rejected(reason)));
    }
    if current.target.symbol_key != initial.target.symbol_key
        || current.target.direction != initial.target.direction
    {
        return Ok(Prepare::Receipt(rejected("POSITION_REVISION_MISMATCH")));
    }
    let unresolved = sqlx::query("SELECT symbol_key FROM engine_orders WHERE HEX(credential_id)=HEX(?) AND HEX(action)=HEX(?) AND HEX(order_role)=HEX('entry') AND HEX(status) NOT IN (HEX('filled'),HEX('rejected'),HEX('canceled'),HEX('cancelled'),HEX('expired')) LIMIT 1001")
        .bind(&request.credential_id).bind(format!("open_{}",current.target.direction)).fetch_all(&mut *tx).await.map_err(db)?;
    // Symbol aliases share the same venue position scope. Unknown overflow is
    // not evidence that there is no pending Entry in that scope.
    if unresolved.len() > 1000
        || unresolved.iter().any(|row| {
            scope(
                &request.credential_id,
                &row.get::<String, _>("symbol_key"),
                &current.target.direction,
            ) == key
        })
    {
        return Ok(Prepare::Receipt(rejected("ENTRY_EXECUTION_UNRESOLVED")));
    }
    let identity = hash(&serde_json::json!([
        request.credential_id,
        request.request_ref
    ]));
    let idempotency_key = format!("b3_{identity}");
    let client_order_id_short = format!("b3{}", &identity[..32]);
    let now = crate::svc::now_ms();
    sqlx::query("INSERT INTO position_close_requests(credential_id,request_ref,actor_member_id,position_id,expected_revision,expected_boot_id,params_hash,scope_key,target_json,idempotency_key,client_order_id_short,phase,reason_code,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,'prepared','PREPARED',?,?)")
        .bind(&request.credential_id).bind(&request.request_ref).bind(request.actor_member_id as i64).bind(&request.position_id)
        .bind(request.expected_revision as i64).bind(&request.expected_boot_id).bind(&commitment).bind(&key)
        .bind(serde_json::to_string(&current.target).map_err(|_|Status::internal("serialize close target"))?).bind(&idempotency_key).bind(&client_order_id_short).bind(now).bind(now)
        .execute(&mut *tx).await.map_err(db)?;
    let fenced=sqlx::query("UPDATE position_close_scope_locks SET pending_request_ref=? WHERE scope_key=? AND pending_request_ref IS NULL")
        .bind(&request.request_ref).bind(&key).execute(&mut *tx).await.map_err(db)?;
    if fenced.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FENCE_MISMATCH"));
    }
    let reporting_run_id =
        entry_reporting_run(&mut tx, &request.credential_id, &current.target).await?;
    tx.commit().await.map_err(db)?;
    Ok(Prepare::Dispatch(ExactCloseContext {
        reporting_run_id,
        credential_id: request.credential_id.clone(),
        request_ref: request.request_ref.clone(),
        params_hash: commitment,
        scope_key: key,
        target: current.target,
        idempotency_key,
        client_order_id_short,
    }))
}

impl ExactCloseContext {
    pub(crate) fn execution_request(&self) -> Request<proto::ExecuteIntentsRequest> {
        let mut request = Request::new(proto::ExecuteIntentsRequest {
            credential_id: self.credential_id.clone(),
            run_mode: self.target.run_mode.clone(),
            intents: vec![proto::OrderIntent {
                bot_uid: self.target.bot_uid.clone(),
                run_id: self.reporting_run_id.clone().unwrap_or_default(),
                cycle_id: format!("cyc_{}", self.request_ref),
                idempotency_key: self.idempotency_key.clone(),
                client_order_id_short: self.client_order_id_short.clone(),
                exchange: "binance".into(),
                market_kind: "futures".into(),
                symbol_key: self.target.symbol_key.clone(),
                action: format!("close_{}", self.target.direction),
                side: self.target.direction.clone(),
                order_type: "market".into(),
                price: String::new(),
                quantity: String::new(),
                reduce_only: true,
                max_position_notional: 0.0,
                max_credential_notional: 0.0,
            }],
        });
        request.extensions_mut().insert(self.clone());
        request
    }
}

/// Called after scope lock, before risk/position/order writes. A replay sees
/// terminal request state under the same lock and never enters the executor.
pub(crate) async fn claim(
    tx: &mut Tx<'_>,
    store: &Store,
    ctx: &ExactCloseContext,
) -> Result<Option<&'static str>, Status> {
    crate::risk::lock_notional_reservation(tx, store.kind(), &ctx.credential_id).await?;
    let row = request_row(tx, &ctx.credential_id, &ctx.request_ref, store.kind(), true)
        .await?
        .ok_or_else(|| Status::failed_precondition("REQUEST_NOT_FOUND"))?;
    if row.get::<String, _>("params_hash") != ctx.params_hash {
        return Err(Status::failed_precondition("REQUEST_IDENTITY_CONFLICT"));
    }
    if row.get::<String, _>("phase") != "prepared" {
        return Ok(Some("REQUEST_ALREADY_TERMINAL"));
    }
    if row.get::<String, _>("expected_boot_id") != store.boot_id() {
        return Ok(Some("PREPARED_PREVIOUS_BOOT"));
    }
    let pending=sqlx::query("SELECT COALESCE(pending_request_ref,'') AS pending FROM position_close_scope_locks WHERE scope_key=?")
        .bind(&ctx.scope_key).fetch_one(&mut **tx).await.map_err(db)?.get::<String,_>("pending");
    if pending != ctx.request_ref {
        return Err(Status::failed_precondition("CLOSE_FENCE_MISMATCH"));
    }
    let current = read_target(
        tx,
        &ctx.credential_id,
        &ctx.target.position_id,
        true,
        store.kind(),
    )
    .await?;
    let reason = match current {
        None => Some("POSITION_NOT_FOUND"),
        Some(row) if row.target != ctx.target => Some("POSITION_REVISION_MISMATCH"),
        Some(row) => unavailable(&row, &ctx.credential_id),
    };
    if let Some(reason) = reason {
        finish_rejected(tx, ctx, reason, None).await?;
        return Ok(Some(reason));
    }
    Ok(None)
}

pub(crate) async fn prepare_exact_exit(
    tx: &mut Tx<'_>,
    ctx: &ExactCloseContext,
    now: i64,
) -> Result<PreparedPositionIntent, Status> {
    let target = &ctx.target;
    let authority = crate::position_state::semantic_idempotency_key(&format!(
        "exit|{}|{}|manual_close|{}",
        target.bot_uid, target.position_id, target.exit_epoch
    ));
    let changed=sqlx::query("UPDATE position_state SET close_revision=close_revision+1,pre_exit_state=state,state='closing',pending_idem_key=?,updated_at_ms=? WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?) AND close_revision=? AND HEX(state) IN (HEX('open'),HEX('protected'))")
        .bind(&authority).bind(now).bind(&ctx.credential_id).bind(&target.position_id).bind(target.revision as i64).execute(&mut **tx).await.map_err(db)?;
    if changed.rows_affected() != 1 {
        return Err(Status::failed_precondition("POSITION_REVISION_MISMATCH"));
    }
    Ok(PreparedPositionIntent {
        authority_idem_key: authority,
        position_id: Some(target.position_id.clone()),
        direction: Some(target.direction.clone()),
        orphan_recovery: false,
    })
}
async fn clear_fence(tx: &mut Tx<'_>, ctx: &ExactCloseContext) -> Result<(), Status> {
    clear_fence_for(tx, &ctx.scope_key, &ctx.request_ref).await
}
async fn clear_fence_for(tx: &mut Tx<'_>, scope_key: &str, reference: &str) -> Result<(), Status> {
    let result=sqlx::query("UPDATE position_close_scope_locks SET pending_request_ref=NULL WHERE scope_key=? AND pending_request_ref=?")
        .bind(scope_key).bind(reference).execute(&mut **tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FENCE_MISMATCH"));
    }
    Ok(())
}
pub(crate) async fn finish_rejected(
    tx: &mut Tx<'_>,
    ctx: &ExactCloseContext,
    reason: &str,
    verdict: Option<&str>,
) -> Result<(), Status> {
    let now = crate::svc::now_ms();
    let result=sqlx::query("UPDATE position_close_requests SET phase='rejected',reason_code=?,risk_verdict_id=?,updated_at_ms=?,finalized_at_ms=? WHERE credential_id=? AND request_ref=? AND phase='prepared' AND engine_order_id IS NULL")
        .bind(reason).bind(verdict).bind(now).bind(now).bind(&ctx.credential_id).bind(&ctx.request_ref).execute(&mut **tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FINALIZATION_CONFLICT"));
    }
    clear_fence(tx, ctx).await
}
pub(crate) async fn finish_accepted(
    tx: &mut Tx<'_>,
    ctx: &ExactCloseContext,
    order: &str,
    verdict: &str,
    inserted: bool,
) -> Result<(), Status> {
    if !inserted {
        return Err(Status::failed_precondition("CLOSE_ORDER_IDENTITY_CONFLICT"));
    }
    let order_row=sqlx::query("SELECT position_id,status,order_role,reduce_only,quantity,action,risk_verdict_id FROM engine_orders WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)")
        .bind(&ctx.credential_id).bind(order).fetch_one(&mut **tx).await.map_err(db)?;
    if order_row.get::<String, _>("position_id") != ctx.target.position_id
        || order_row.get::<String, _>("status") != "filled"
        || order_row.get::<String, _>("order_role") != "exit"
        || order_row.get::<i64, _>("reduce_only") != 1
        || order_row.get::<String, _>("quantity") != ctx.target.quantity
        || order_row.get::<String, _>("action") != format!("close_{}", ctx.target.direction)
        || order_row.get::<String, _>("risk_verdict_id") != verdict
    {
        return Err(Status::failed_precondition("PAPER_ORDER_NOT_PROVEN"));
    }
    let position=sqlx::query("SELECT close_revision,qty,state FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?)")
        .bind(&ctx.credential_id).bind(&ctx.target.position_id).fetch_one(&mut **tx).await.map_err(db)?;
    let revision = read_u64(&position, "close_revision")?;
    if position.get::<String, _>("state") != "closed"
        || crate::reconcile::snapshot_decimal_is_zero(&position.get::<String, _>("qty"))
            != Some(true)
        || revision <= ctx.target.revision
    {
        return Err(Status::failed_precondition("PAPER_ZERO_NOT_PROVEN"));
    }
    let fill=sqlx::query("SELECT fill_qty FROM engine_fills WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)")
        .bind(&ctx.credential_id).bind(order).fetch_all(&mut **tx).await.map_err(db)?;
    if fill.len() != 1 || fill[0].get::<String, _>("fill_qty") != ctx.target.quantity {
        return Err(Status::failed_precondition("PAPER_FILL_NOT_PROVEN"));
    }
    let now = crate::svc::now_ms();
    let frozen = serde_json::to_string(&Outcome {
        position_id: ctx.target.position_id.clone(),
        revision,
        quantity: position.get("qty"),
        state: position.get("state"),
        observed_at_ms: now,
    })
    .map_err(|_| Status::internal("serialize paper outcome"))?;
    let result=sqlx::query("UPDATE position_close_requests SET phase='accepted',reason_code='PAPER_FILLED',engine_order_id=?,risk_verdict_id=?,outcome_json=?,updated_at_ms=?,finalized_at_ms=? WHERE credential_id=? AND request_ref=? AND phase='prepared'")
        .bind(order).bind(verdict).bind(frozen).bind(now).bind(now).bind(&ctx.credential_id).bind(&ctx.request_ref).execute(&mut **tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FINALIZATION_CONFLICT"));
    }
    clear_fence(tx, ctx).await
}

// ---------------------------------------------------------------------------
// WO-M1 · 交易所凭据的单仓平仓（D29 / D29a）
// ---------------------------------------------------------------------------

/// 派发事务已把 reduce-only 市价单与裁决一起落库、尚无真值终结（fence 挂着）。
const PHASE_DISPATCHED: &str = "dispatched";
/// 交易所真值终结：累计成交 = 目标，且交易所该 scope 归零。
const PHASE_FILLED: &str = "filled";

fn stored_target(row: &sqlx::any::AnyRow) -> Result<Target, Status> {
    let target_json = crate::store::try_get_text(row, "target_json")
        .map_err(db)?
        .ok_or_else(|| Status::internal("missing close target receipt"))?;
    serde_json::from_str(&target_json).map_err(|_| Status::internal("invalid close target receipt"))
}

fn decimal(value: &str, field: &str) -> Result<crate::svc_v2::ExactDecimal, Status> {
    crate::svc_v2::ExactDecimal::parse(value, field)
}

/// 派发数量必须**数值上**等于精确目标数量（交易所过滤器的步长取整一旦
/// 改了它，这张单就不再是「全部」）。
pub(crate) fn quantity_is_exact(quantity: &str, ctx: &ExactCloseContext) -> bool {
    match (
        decimal(quantity, "close quantity"),
        decimal(&ctx.target.quantity, "target quantity"),
    ) {
        (Ok(left), Ok(right)) => left
            .numerically_equals(right, "close quantity")
            .unwrap_or(false),
        _ => false,
    }
}

/// 交易所凭据的平仓在派发事务里只证明「这张单就是那张单」：全量、只减仓、
/// 市价、本仓、本裁决、本请求的 clientOrderId，仓位已按 revision CAS 进入
/// closing。请求转 `dispatched`；fence **不清**——只有真值终结才清。
pub(crate) async fn finish_dispatched(
    tx: &mut Tx<'_>,
    ctx: &ExactCloseContext,
    order: &str,
    verdict: &str,
    inserted: bool,
) -> Result<(), Status> {
    if !inserted {
        return Err(Status::failed_precondition("CLOSE_ORDER_IDENTITY_CONFLICT"));
    }
    let order_row=sqlx::query("SELECT position_id,status,order_role,reduce_only,quantity,action,order_type,risk_verdict_id,client_order_id_short FROM engine_orders WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)")
        .bind(&ctx.credential_id).bind(order).fetch_one(&mut **tx).await.map_err(db)?;
    if order_row.get::<String, _>("position_id") != ctx.target.position_id
        || order_row.get::<String, _>("status") != "pending_risk"
        || order_row.get::<String, _>("order_role") != "exit"
        || order_row.get::<i64, _>("reduce_only") != 1
        || !quantity_is_exact(&order_row.get::<String, _>("quantity"), ctx)
        || order_row.get::<String, _>("action") != format!("close_{}", ctx.target.direction)
        || !order_row
            .get::<String, _>("order_type")
            .eq_ignore_ascii_case("market")
        || order_row.get::<String, _>("risk_verdict_id") != verdict
        || order_row.get::<String, _>("client_order_id_short") != ctx.client_order_id_short
    {
        return Err(Status::failed_precondition(
            "EXCHANGE_CLOSE_ORDER_NOT_PROVEN",
        ));
    }
    let position=sqlx::query("SELECT close_revision,state FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?)")
        .bind(&ctx.credential_id).bind(&ctx.target.position_id).fetch_one(&mut **tx).await.map_err(db)?;
    if position.get::<String, _>("state") != "closing"
        || read_u64(&position, "close_revision")? <= ctx.target.revision
    {
        return Err(Status::failed_precondition(
            "EXCHANGE_CLOSE_POSITION_NOT_PROVEN",
        ));
    }
    let now = crate::svc::now_ms();
    let result=sqlx::query("UPDATE position_close_requests SET phase=?,reason_code='EXCHANGE_SUBMITTED',engine_order_id=?,risk_verdict_id=?,updated_at_ms=? WHERE credential_id=? AND request_ref=? AND phase='prepared' AND engine_order_id IS NULL")
        .bind(PHASE_DISPATCHED).bind(order).bind(verdict).bind(now).bind(&ctx.credential_id).bind(&ctx.request_ref).execute(&mut **tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FINALIZATION_CONFLICT"));
    }
    Ok(())
}

/// 原请求那张单的持久真值：状态、累计成交（成交行逐笔求和，不是单笔
/// 全量证明）、落库时刻。
struct OrderTruth {
    status: String,
    filled: crate::svc_v2::ExactDecimal,
    created_at_ms: i64,
}

async fn load_order_truth(
    tx: &mut Tx<'_>,
    credential: &str,
    order: &str,
) -> Result<OrderTruth, Status> {
    let row = sqlx::query("SELECT status,created_at_ms FROM engine_orders WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)")
        .bind(credential).bind(order).fetch_one(&mut **tx).await.map_err(db)?;
    let fills = sqlx::query("SELECT fill_qty FROM engine_fills WHERE HEX(credential_id)=HEX(?) AND HEX(engine_order_id)=HEX(?)")
        .bind(credential).bind(order).fetch_all(&mut **tx).await.map_err(db)?;
    let mut filled = crate::svc_v2::ExactDecimal::zero();
    for fill in fills {
        let quantity = crate::store::try_get_nullable_string(&fill, "fill_qty")
            .map_err(db)?
            .ok_or_else(|| Status::internal("close order fill has no quantity"))?;
        filled = filled.checked_add(decimal(&quantity, "fill quantity")?, "filled quantity")?;
    }
    Ok(OrderTruth {
        status: row.get::<String, _>("status").trim().to_ascii_lowercase(),
        filled,
        created_at_ms: row.get("created_at_ms"),
    })
}

/// 交易所已判死、再也不会成交的订单（与 `exit_order_status_is_dead` 同一
/// 闭集；`unknown` / `pending_*` 不在其中——只能重查，不能推定）。
fn exchange_order_is_dead(status: &str) -> bool {
    matches!(
        status,
        "rejected" | "canceled" | "cancelled" | "expired" | "failed"
    )
}

fn dead_reason(order: &OrderTruth) -> &'static str {
    if order.filled.is_strictly_positive() {
        return "EXCHANGE_ENDED_PARTIALLY_FILLED";
    }
    match order.status.as_str() {
        "rejected" => "EXCHANGE_REJECTED",
        "canceled" | "cancelled" => "EXCHANGE_CANCELED",
        "expired" => "EXCHANGE_EXPIRED",
        _ => "EXCHANGE_ORDER_NOT_FOUND",
    }
}

/// `dispatched` 请求的在途回执状态（只读推导，不写任何东西）。
fn dispatched_status(order: &OrderTruth, same_boot: bool) -> (&'static str, &'static str) {
    match order.status.as_str() {
        "pending_risk" | "pending_submit" | "submitted" if same_boot => {
            ("accepted", "EXCHANGE_SUBMITTED")
        }
        // 派发进程已不在：这一刻是否已经送达交易所不可知，只能等重查。
        "pending_risk" | "pending_submit" | "submitted" => ("unknown", "DISPATCHED_PREVIOUS_BOOT"),
        "acked" if order.filled.is_strictly_positive() => {
            ("partially_filled", "EXCHANGE_PARTIALLY_FILLED")
        }
        "acked" => ("working", "EXCHANGE_WORKING"),
        "partial_filled" => ("partially_filled", "EXCHANGE_PARTIALLY_FILLED"),
        // 交易所说单子成交了；成交行与交易所仓位归零尚未都被真值确认。
        "filled" => ("working", "EXCHANGE_FILL_CONFIRMING"),
        _ => ("unknown", "EXCHANGE_OUTCOME_UNKNOWN"),
    }
}

async fn current_observation(
    tx: &mut Tx<'_>,
    credential: &str,
    position: &str,
) -> Result<p::PositionCloseObservation, Status> {
    let current = sqlx::query("SELECT position_id,close_revision,qty,state FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?)")
        .bind(credential).bind(position).fetch_optional(&mut **tx).await.map_err(db)?;
    Ok(match current {
        Some(current) => Outcome {
            position_id: current.get("position_id"),
            revision: read_u64(&current, "close_revision")?,
            quantity: current.get("qty"),
            state: current.get("state"),
            observed_at_ms: crate::svc::now_ms(),
        }
        .wire("position_store"),
        None => unknown(),
    })
}

fn protection_observation(status: &str, reason: &str) -> p::PositionCloseProtectionObservation {
    p::PositionCloseProtectionObservation {
        status: status.into(),
        reason_code: reason.into(),
        schema_version: 1,
    }
}

/// 回执里的保护单状态只来自服务端持久真值，从不推断：仓位仍开着时，只有
/// 经交易所真值确认、仍在挂的止损才报「止损仍在」；仓位已 closed 时报
/// D30 善后义务的状态（确认前如实为 PROTECTION_CLEANUP_PENDING）。
async fn exchange_protection_observation(
    tx: &mut Tx<'_>,
    position: &str,
) -> Result<p::PositionCloseProtectionObservation, Status> {
    let state = sqlx::query("SELECT state FROM position_state WHERE HEX(position_id)=HEX(?)")
        .bind(position)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .map(|row| row.get::<String, _>("state"));
    if state.as_deref() == Some("closed") {
        let obligation = sqlx::query(
            "SELECT state FROM protection_cleanup_obligations WHERE HEX(position_id)=HEX(?)",
        )
        .bind(position)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .map(|row| row.get::<String, _>("state"));
        return Ok(match obligation.as_deref().map(str::trim) {
            Some("pending") => {
                protection_observation("cleanup_pending", "PROTECTION_CLEANUP_PENDING")
            }
            Some("complete") => protection_observation("cleaned", "PROTECTION_CLEANUP_CONFIRMED"),
            _ => protection_observation("unknown", "PROTECTION_CLEANUP_NOT_PROVEN"),
        });
    }
    let standing = sqlx::query("SELECT COUNT(*) AS n FROM protection_orders WHERE HEX(position_id)=HEX(?) AND kind='sl' AND verified_from_exchange=1 AND status='confirmed'")
        .bind(position).fetch_one(&mut **tx).await.map_err(db)?.get::<i64, _>("n");
    Ok(if standing > 0 {
        protection_observation("in_place", "STOP_LOSS_ON_VENUE")
    } else {
        protection_observation("unknown", "STOP_LOSS_NOT_CONFIRMED")
    })
}

async fn exchange_receipt_tx(
    tx: &mut Tx<'_>,
    store: &Store,
    row: sqlx::any::AnyRow,
) -> Result<p::ClosePositionResponse, Status> {
    let credential: String = row.get("credential_id");
    let position: String = row.get("position_id");
    let phase: String = row.get("phase");
    let boot: String = row.get("expected_boot_id");
    let target = stored_target(&row)?;
    let order: String = row.get("order_id");
    let verdict: String = row.get("verdict_id");
    let reason: String = row.get("reason_code");
    let order_truth = if order.is_empty() {
        None
    } else {
        Some(load_order_truth(tx, &credential, &order).await?)
    };
    let (request_status, reason_code, execution_outcome) = match phase.as_str() {
        "prepared" if boot != store.boot_id() => (
            "pending_unknown".to_string(),
            "PREPARED_PREVIOUS_BOOT".to_string(),
            unknown(),
        ),
        "prepared" | "rejected" => (phase.clone(), reason, unknown()),
        PHASE_FILLED => {
            let outcome = crate::store::try_get_text(&row, "outcome")
                .map_err(db)?
                .filter(|outcome| !outcome.is_empty())
                .ok_or_else(|| Status::internal("filled exchange close has no frozen outcome"))?;
            let frozen = serde_json::from_str::<Outcome>(&outcome)
                .map_err(|_| Status::internal("invalid frozen close outcome"))?
                .wire("exchange_truth");
            (phase.clone(), reason, frozen)
        }
        PHASE_DISPATCHED => {
            let order_truth = order_truth
                .as_ref()
                .ok_or_else(|| Status::internal("dispatched exchange close has no order"))?;
            let (status, reason) = dispatched_status(order_truth, boot == store.boot_id());
            (status.to_string(), reason.to_string(), unknown())
        }
        other => {
            return Err(Status::internal(format!(
                "exchange close request has an unknown phase {other:?}"
            )))
        }
    };
    Ok(p::ClosePositionResponse {
        found: true,
        request_ref: row.get("request_ref"),
        position_id: position.clone(),
        target_revision: read_u64(&row, "expected_revision")?,
        request_status,
        reason_code,
        engine_order_id: (!order.is_empty()).then_some(order),
        risk_verdict_id: (!verdict.is_empty()).then_some(verdict),
        execution_outcome: Some(execution_outcome),
        position_observation: Some(current_observation(tx, &credential, &position).await?),
        protection_observation: Some(exchange_protection_observation(tx, &position).await?),
        filled_quantity: order_truth.map(|truth| truth.filled.to_text()),
        target_quantity: Some(target.quantity),
        schema_version: 1,
    })
}

/// 同一轮对账读到的交易所 positionRisk 真值。`covers` 是本轮仓位读数的
/// 覆盖面（symbol 收窄的读数不能为别的 symbol 作证）；`read_started_at_ms`
/// 早于原单落库的读数不能证明「平仓之后」归零。
pub(crate) struct VenuePositions<'a> {
    pub positions: &'a serde_json::Value,
    pub read_started_at_ms: i64,
    pub covers: &'a (dyn Fn(&str) -> bool + Send + Sync),
}

/// 交易所真值路径：终结本凭据全部 `dispatched` 平仓请求中已有终局的那些。
/// 只读持久真值与调用方给的同轮读数，**从不**向交易所发送任何请求。
pub(crate) async fn settle_exchange_closes(
    store: &Store,
    credential: &str,
    venue: Option<&VenuePositions<'_>>,
    now: i64,
) -> Result<usize, Status> {
    let pending = sqlx::query("SELECT request_ref FROM position_close_requests WHERE credential_id=? AND phase=? ORDER BY created_at_ms LIMIT 200")
        .bind(credential).bind(PHASE_DISPATCHED).fetch_all(store.pool()).await.map_err(db)?;
    let mut finalized = 0;
    for row in pending {
        let reference: String = row.get("request_ref");
        if settle_request(store, credential, &reference, venue, now).await? {
            finalized += 1;
        }
    }
    Ok(finalized)
}

/// 终结一条 `dispatched` 请求（若真值已足够）：
/// * 原单被交易所判死（拒单 / 撤销 / 过期 / 查无此单）⇒ `rejected`，同事务
///   按既有判死规则释放 closing；
/// * 原单 `filled` ∧ 累计成交 = 目标 ∧ 本地仓位 closed/零 ∧ 同轮
///   positionRisk（晚于原单落库、覆盖该 symbol）显示该 scope 精确归零 ⇒
///   `filled`，冻结真值终态。
///
/// 两者都在同一事务里清 fence。其余一律不动（fence 继续挂着）。
pub(crate) async fn settle_request(
    store: &Store,
    credential: &str,
    reference: &str,
    venue: Option<&VenuePositions<'_>>,
    now: i64,
) -> Result<bool, Status> {
    let mut tx = store.pool().begin().await.map_err(db)?;
    let Some(row) = request_row(&mut tx, credential, reference, store.kind(), false).await? else {
        return Ok(false);
    };
    if row.get::<String, _>("phase") != PHASE_DISPATCHED {
        return Ok(false);
    }
    let scope_key: String = row.get("scope_key");
    // Lock order: B3 scope -> request -> position (same as dispatch).
    lock_scope(&mut tx, store.kind(), &scope_key).await?;
    let Some(row) = request_row(&mut tx, credential, reference, store.kind(), true).await? else {
        return Ok(false);
    };
    if row.get::<String, _>("phase") != PHASE_DISPATCHED {
        return Ok(false);
    }
    let target = stored_target(&row)?;
    let order: String = row.get("order_id");
    let order = load_order_truth(&mut tx, credential, &order).await?;
    let (phase, reason, outcome) = if exchange_order_is_dead(&order.status) {
        crate::position_state::release_closing_if_exit_dead(&mut tx, &target.position_id, now)
            .await?;
        ("rejected", dead_reason(&order), None)
    } else if order.status == "filled"
        && order.filled.numerically_equals(
            decimal(&target.quantity, "target quantity")?,
            "filled quantity",
        )?
        && venue.is_some_and(|venue| venue_flat_after(venue, &target, order.created_at_ms))
    {
        let position=sqlx::query("SELECT close_revision,qty,state FROM position_state WHERE HEX(credential_id)=HEX(?) AND HEX(position_id)=HEX(?)")
            .bind(credential).bind(&target.position_id).fetch_one(&mut *tx).await.map_err(db)?;
        if position.get::<String, _>("state") != "closed"
            || crate::reconcile::snapshot_decimal_is_zero(&position.get::<String, _>("qty"))
                != Some(true)
        {
            return Ok(false);
        }
        let frozen = serde_json::to_string(&Outcome {
            position_id: target.position_id.clone(),
            revision: read_u64(&position, "close_revision")?,
            quantity: position.get("qty"),
            state: position.get("state"),
            observed_at_ms: now,
        })
        .map_err(|_| Status::internal("serialize exchange close outcome"))?;
        (PHASE_FILLED, "EXCHANGE_FILLED", Some(frozen))
    } else {
        return Ok(false);
    };
    let result=sqlx::query("UPDATE position_close_requests SET phase=?,reason_code=?,outcome_json=?,updated_at_ms=?,finalized_at_ms=? WHERE credential_id=? AND request_ref=? AND phase=?")
        .bind(phase).bind(reason).bind(outcome).bind(now).bind(now).bind(credential).bind(reference).bind(PHASE_DISPATCHED)
        .execute(&mut *tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FINALIZATION_CONFLICT"));
    }
    clear_fence_for(&mut tx, &scope_key, reference).await?;
    tx.commit().await.map_err(db)?;
    Ok(true)
}

fn venue_flat_after(venue: &VenuePositions<'_>, target: &Target, order_created_at_ms: i64) -> bool {
    venue.read_started_at_ms > order_created_at_ms
        && (venue.covers)(&target.symbol_key)
        && crate::protection_cleanup::venue_scope_exposure(
            venue.positions,
            &target.symbol_key,
            &target.direction,
        ) == crate::protection_cleanup::VenueScopeExposure::Flat
}

/// 派发调用返回后请求仍是本 boot 的 `prepared`：执行入口在取得派发权之前
/// 就拒绝了（例如档位闸门），没有任何订单落库、也没有任何交易所请求。
/// 如实终结为 rejected 并释放 fence——否则一条什么都没做的请求会永久挡住
/// 该 scope 的开仓。派发事务把订单插入与相位推进放在同一事务里，所以
/// 「仍是 prepared」本身就是「从未派发」的证明。
pub(crate) async fn reject_unclaimed(
    store: &Store,
    ctx: &ExactCloseContext,
    reason: &str,
) -> Result<bool, Status> {
    let mut tx = store.pool().begin().await.map_err(db)?;
    lock_scope(&mut tx, store.kind(), &ctx.scope_key).await?;
    let Some(row) = request_row(
        &mut tx,
        &ctx.credential_id,
        &ctx.request_ref,
        store.kind(),
        true,
    )
    .await?
    else {
        return Ok(false);
    };
    if row.get::<String, _>("phase") != "prepared"
        || row.get::<String, _>("expected_boot_id") != store.boot_id()
        || !row.get::<String, _>("order_id").is_empty()
        || row.get::<String, _>("params_hash") != ctx.params_hash
    {
        return Ok(false);
    }
    finish_rejected(&mut tx, ctx, reason, None).await?;
    tx.commit().await.map_err(db)?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// WO-M1b（M1 评审 P2 #3）· 崩溃窗口遗留的 prepared 请求
// ---------------------------------------------------------------------------

/// 前一 boot 未派发、经证明从未发出的请求的终结理由。
const PREPARED_NOT_DISPATCHED: &str = "PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED";

/// prepare 已提交、派发事务未提交时进程死亡留下的请求：交易所凭据、仍是
/// prepared、没有订单 id、`expected_boot_id` 不是本 boot。本 boot 的
/// prepared（派发可能正在进行）不在此列；模拟凭据保持 B3-K1 既有语义
/// （前一 boot 的 prepared 只读、fence 保留）。
fn stranded_prepared(row: &sqlx::any::AnyRow, store: &Store) -> bool {
    is_exchange(&row.get::<String, _>("credential_id"))
        && row.get::<String, _>("phase") == "prepared"
        && row.get::<String, _>("expected_boot_id") != store.boot_id()
        && row.get::<String, _>("order_id").is_empty()
}

/// 终结一条崩溃窗口遗留的 prepared 请求——**仅当可证明什么都没发出**。
///
/// 证明链（svc.rs `execute_intents`）：派发事务把 `engine_orders` 插入与
/// `finish_dispatched`（prepared→dispatched、写 engine_order_id）放在**同一个**
/// 事务里；交易所提交只发生在该事务 `commit` 之后（之后才取密钥、出站），
/// 不存在「先发包、后落行」的窗口。因此「请求仍 prepared ∧ 无 engine_order_id
/// ∧ 没有任何订单行带着本请求的 b3 clientOrderId」⇒ 这张单从未落库，也就
/// 从未发到交易所——不需要、也不发任何交易所查询。
///
/// 终结为 rejected（`PREPARED_PREVIOUS_BOOT_NOT_DISPATCHED`），与释放 fence 在
/// **同一事务**里；绝不重新派发。证明不成立时原样不动（fence 继续挂着）。
pub(crate) async fn resolve_stranded_prepared(
    store: &Store,
    credential: &str,
    reference: &str,
) -> Result<bool, Status> {
    let mut tx = store.pool().begin().await.map_err(db)?;
    let Some(row) = request_row(&mut tx, credential, reference, store.kind(), false).await? else {
        return Ok(false);
    };
    if !stranded_prepared(&row, store) {
        return Ok(false);
    }
    let scope_key: String = row.get("scope_key");
    // Lock order: B3 scope -> request（与派发、真值终结同序）。
    lock_scope(&mut tx, store.kind(), &scope_key).await?;
    let Some(row) = request_row(&mut tx, credential, reference, store.kind(), true).await? else {
        return Ok(false);
    };
    if !stranded_prepared(&row, store) {
        return Ok(false);
    }
    // 未发出的证明：任何一行订单带着本请求的 b3 clientOrderId 都推翻它。
    // 比较不区分大小写的排序规则只会多匹配（多拒），不会漏。
    let orders = sqlx::query(
        "SELECT COUNT(*) AS n FROM engine_orders WHERE credential_id=? AND client_order_id_short=?",
    )
    .bind(credential)
    .bind(row.get::<String, _>("client_order_id_short"))
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?
    .get::<i64, _>("n");
    if orders != 0 {
        return Ok(false);
    }
    let dead_boot: String = row.get("expected_boot_id");
    let now = crate::svc::now_ms();
    let result=sqlx::query("UPDATE position_close_requests SET phase='rejected',reason_code=?,updated_at_ms=?,finalized_at_ms=? WHERE credential_id=? AND request_ref=? AND phase='prepared' AND engine_order_id IS NULL AND expected_boot_id=?")
        .bind(PREPARED_NOT_DISPATCHED).bind(now).bind(now).bind(credential).bind(reference).bind(&dead_boot)
        .execute(&mut *tx).await.map_err(db)?;
    if result.rows_affected() != 1 {
        return Err(Status::failed_precondition("CLOSE_FINALIZATION_CONFLICT"));
    }
    clear_fence_for(&mut tx, &scope_key, reference).await?;
    tx.commit().await.map_err(db)?;
    Ok(true)
}

/// 对账轮的清扫（不需要任何会员动作即可解封该 scope 的开仓）：逐条尝试
/// [`resolve_stranded_prepared`]。单条失败只记录、不让整轮对账失败——
/// 失败的那条原样保留 fence（安全方向），其余真值照常应用。
pub(crate) async fn resolve_stranded_prepared_closes(store: &Store, credential: &str) -> usize {
    let candidates = match sqlx::query("SELECT request_ref FROM position_close_requests WHERE credential_id=? AND phase='prepared' AND expected_boot_id<>? ORDER BY created_at_ms LIMIT 200")
        .bind(credential).bind(store.boot_id()).fetch_all(store.pool()).await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::error!(credential_id = %credential, %error, "stranded close request scan failed");
            return 0;
        }
    };
    let mut resolved = 0;
    for row in candidates {
        let reference: String = row.get("request_ref");
        match resolve_stranded_prepared(store, credential, &reference).await {
            Ok(true) => resolved += 1,
            Ok(false) => {}
            Err(status) => tracing::error!(
                credential_id = %credential,
                request_ref = %reference,
                failure_reason = %status.message(),
                "stranded close request was not resolved; its scope fence stays held"
            ),
        }
    }
    resolved
}

/// Provenance is established only from a newly inserted admitted Entry order,
/// inside that order's original transaction. Reads/close never backfill it.
pub(crate) async fn persist_entry_provenance(
    tx: &mut Tx<'_>,
    credential: &str,
    intent: &proto::OrderIntent,
    position: &str,
    order: &str,
    run_mode: &str,
    now: i64,
) -> Result<(), Status> {
    let venue = if crate::venue::is_simulation_credential(credential) {
        Some("simulation".to_string())
    } else {
        crate::venue::venue_for_credential(credential)
            .ok()
            .flatten()
            .map(|v| v.to_string())
    };
    let Some(venue) = venue else {
        return Ok(());
    };
    let epoch=sqlx::query("SELECT entry_epoch FROM position_state WHERE HEX(position_id)=HEX(?) AND HEX(credential_id)=HEX(?)")
        .bind(position).bind(credential).fetch_one(&mut **tx).await.map_err(db)?.get::<i64,_>("entry_epoch");
    let direction = if intent.action == "open_long" {
        "long"
    } else {
        "short"
    };
    sqlx::query("INSERT INTO position_execution_provenance(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,entry_engine_order_id,run_mode,execution_venue,created_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?)")
        .bind(position).bind(credential).bind(&intent.bot_uid).bind(&intent.symbol_key).bind(direction).bind(epoch).bind(order).bind(run_mode).bind(venue).bind(now)
        .execute(&mut **tx).await.map_err(db)?;
    Ok(())
}
