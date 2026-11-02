//! B8 monotonic Entry admission for one credential on the current executor.
//!
//! The mutex linearizes OMS commit and each actual send with Arm in THIS process.
//! It is deliberately not a cross-process owner/lease. Persistent rows deny Entry
//! after restart, but do not cancel previously sent orders or establish settlement.
//! Machine authentication is supplied by the production tonic assembly; member
//! ownership and a stable single-account binding remain the trusted caller's duty.
use crate::{
    proto_v2 as p,
    store::{DbKind, Store},
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tonic::Status;

type Tx<'a> = sqlx::Transaction<'a, sqlx::Any>;
const SAFE: u64 = 9_007_199_254_740_991;
pub(crate) const DENIED: &str = "LIFECYCLE_ENTRY_FENCED";
fn db(_: sqlx::Error) -> Status {
    Status::unavailable("LIFECYCLE_STORE_UNAVAILABLE")
}
fn corrupt() -> Status {
    Status::unavailable("LIFECYCLE_STORE_INCONSISTENT")
}
fn conflict() -> Status {
    Status::already_exists("LIFECYCLE_IDENTITY_CONFLICT")
}
fn schema(value: u32) -> Result<(), Status> {
    if value == 1 {
        Ok(())
    } else {
        Err(Status::invalid_argument("SCHEMA_VERSION_UNSUPPORTED"))
    }
}
fn canonical_ulid(s: &str) -> bool {
    ulid::Ulid::from_string(s)
        .ok()
        .is_some_and(|v| v.to_string() == s)
}
fn key(credential: &str) -> String {
    crate::svc::sha256_hex(credential)
}

/// Process-wide, not per service instance/pool. Weak entries bound idle memory;
/// an owned guard keeps the mutex alive for the complete critical section.
pub(crate) async fn lock(credential: &str) -> Result<OwnedMutexGuard<()>, Status> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>> = OnceLock::new();
    let mutex = {
        let mut locks = LOCKS
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| corrupt())?;
        locks.retain(|_, v| v.strong_count() > 0);
        let slot = locks.entry(credential.to_owned()).or_default();
        if let Some(mutex) = slot.upgrade() {
            mutex
        } else {
            let mutex = Arc::new(AsyncMutex::new(()));
            *slot = Arc::downgrade(&mutex);
            mutex
        }
    };
    Ok(mutex.lock_owned().await)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    request_ref: String,
    actor_member_id: u64,
    account_uid: String,
    credential_id: String,
    purpose: String,
}
impl Identity {
    fn validate(&self) -> Result<(), Status> {
        let numeric = self
            .credential_id
            .parse::<u64>()
            .ok()
            .is_some_and(|v| v > 0 && v <= SAFE && v.to_string() == self.credential_id);
        let paper = self.credential_id == format!("paper:{}", &key(&self.account_uid)[..32]);
        if !canonical_ulid(&self.request_ref)
            || !(1..=SAFE).contains(&self.actor_member_id)
            || self.account_uid.is_empty()
            || self.account_uid.len() > 32
            || !self
                .account_uid
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || v == b'_' || v == b'-')
            || !(numeric || paper)
            || !matches!(
                self.purpose.as_str(),
                "disable" | "rotate" | "account_closure"
            )
        {
            return Err(Status::invalid_argument("LIFECYCLE_IDENTITY_INVALID"));
        }
        Ok(())
    }
    fn wire(&self) -> p::LifecycleFenceIdentity {
        p::LifecycleFenceIdentity {
            request_ref: self.request_ref.clone(),
            actor_member_id: self.actor_member_id,
            account_uid: self.account_uid.clone(),
            credential_id: self.credential_id.clone(),
            purpose: self.purpose.clone(),
            schema_version: 1,
        }
    }
}
fn get_identity(r: p::GetLifecycleFenceRequest) -> Result<Identity, Status> {
    schema(r.schema_version)?;
    let id = Identity {
        request_ref: r.request_ref,
        actor_member_id: r.actor_member_id,
        account_uid: r.account_uid,
        credential_id: r.credential_id,
        purpose: r.purpose,
    };
    id.validate()?;
    Ok(id)
}
fn text(row: &sqlx::any::AnyRow, col: &str) -> Result<String, Status> {
    crate::store::try_get_text(row, col)
        .map_err(db)?
        .ok_or_else(corrupt)
}
fn positive(row: &sqlx::any::AnyRow, col: &str) -> Result<u64, Status> {
    let n = row.try_get::<i64, _>(col).map_err(db)?;
    if n <= 0 {
        Err(corrupt())
    } else {
        Ok(n as u64)
    }
}

/// Same exact credential mutex must be held by the caller. SQL hashes are only
/// lookup keys: every retrieved credential/identity is also compared byte-for-byte.
async fn fence_row(
    tx: &mut Tx<'_>,
    kind: DbKind,
    credential: &str,
    for_update: bool,
) -> Result<Option<sqlx::any::AnyRow>, Status> {
    let suffix = if for_update && kind == DbKind::MySql {
        " FOR UPDATE"
    } else {
        ""
    };
    let sql = format!("SELECT credential_id,actor_member_id,account_uid,revision FROM lifecycle_credential_fences WHERE credential_key=?{suffix}");
    let row = sqlx::query(&sql)
        .bind(key(credential))
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
    if let Some(r) = &row {
        if text(r, "credential_id")? != credential {
            return Err(corrupt());
        }
        positive(r, "revision")?;
    }
    Ok(row)
}
pub(crate) async fn entry_blocked(
    tx: &mut Tx<'_>,
    kind: DbKind,
    credential: &str,
) -> Result<bool, Status> {
    Ok(fence_row(tx, kind, credential, true).await?.is_some())
}
/// Caller retains this guard through request.send().await (headers). Never keep
/// the SQL transaction across HTTP; release the mutex before response settlement.
pub(crate) async fn authorize_send(
    store: Option<&Store>,
    credential: &str,
) -> anyhow::Result<OwnedMutexGuard<()>> {
    let store = store.ok_or_else(|| anyhow::anyhow!("LIFECYCLE_STORE_REQUIRED"))?;
    let guard = lock(credential).await?;
    let mut tx = store.pool().begin().await?;
    let blocked = entry_blocked(&mut tx, store.kind(), credential).await?;
    tx.commit().await?;
    if blocked {
        return Err(anyhow::anyhow!(DENIED));
    }
    Ok(guard)
}

async fn original(
    tx: &mut Tx<'_>,
    id: &Identity,
    expected: Option<&str>,
) -> Result<Option<p::LifecycleFenceArmFact>, Status> {
    let row = sqlx::query("SELECT request_ref,identity_json,expected_boot_id,armed_boot_id,fence_revision,armed_at_ms,params_hash FROM lifecycle_fence_requests WHERE request_ref=?")
        .bind(&id.request_ref).fetch_optional(&mut **tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let raw = text(&row, "identity_json")?;
    let recorded: Identity = serde_json::from_str(&raw).map_err(|_| corrupt())?;
    let expected_boot = text(&row, "expected_boot_id")?;
    let armed_boot = text(&row, "armed_boot_id")?;
    if recorded != *id
        || text(&row, "request_ref")? != id.request_ref
        || expected.is_some_and(|v| v != expected_boot)
    {
        return Err(conflict());
    }
    if !canonical_ulid(&expected_boot)
        || expected_boot != armed_boot
        || text(&row, "params_hash")? != key(&format!("{raw}\n{expected_boot}"))
    {
        return Err(corrupt());
    }
    let at = row.try_get::<i64, _>("armed_at_ms").map_err(db)?;
    if at <= 0 {
        return Err(corrupt());
    }
    Ok(Some(p::LifecycleFenceArmFact {
        expected_boot_id: expected_boot,
        armed_boot_id: armed_boot,
        fence_revision: positive(&row, "fence_revision")?,
        armed_at_ms: at,
        schema_version: 1,
    }))
}
async fn response(
    tx: &mut Tx<'_>,
    store: &Store,
    id: &Identity,
    fact: Option<p::LifecycleFenceArmFact>,
) -> Result<p::LifecycleFenceResponse, Status> {
    let row = fence_row(tx, store.kind(), &id.credential_id, false).await?;
    let revision = if let Some(r) = row {
        if positive(&r, "actor_member_id")? != id.actor_member_id
            || text(&r, "account_uid")? != id.account_uid
        {
            return Err(conflict());
        }
        positive(&r, "revision")?
    } else {
        0
    };
    if fact.as_ref().is_some_and(|v| v.fence_revision > revision) {
        return Err(corrupt());
    }
    Ok(p::LifecycleFenceResponse {
        identity: Some(id.wire()),
        found: fact.is_some(),
        original_arm: fact,
        current_observation: Some(p::LifecycleFenceObservation {
            credential_id: id.credential_id.clone(),
            engine_boot_id: store.boot_id().into(),
            observed_at_ms: crate::svc::now_ms(),
            scope: "credential_on_current_executor".into(),
            admission_blocked: revision > 0,
            fence_revision: revision,
            entry_cancellation: "not_started".into(),
            settlement: "unknown".into(),
            schema_version: 1,
        }),
        schema_version: 1,
    })
}
pub(crate) async fn get(
    store: &Store,
    r: p::GetLifecycleFenceRequest,
) -> Result<p::LifecycleFenceResponse, Status> {
    let id = get_identity(r)?;
    let _guard = lock(&id.credential_id).await?;
    let mut tx = store.pool().begin().await.map_err(db)?;
    let fact = original(&mut tx, &id, None).await?;
    let result = response(&mut tx, store, &id, fact).await?;
    tx.commit().await.map_err(db)?;
    Ok(result)
}
pub(crate) async fn arm(
    store: &Store,
    r: p::ArmLifecycleFenceRequest,
) -> Result<p::LifecycleFenceResponse, Status> {
    let id = get_identity(p::GetLifecycleFenceRequest {
        request_ref: r.request_ref,
        actor_member_id: r.actor_member_id,
        account_uid: r.account_uid,
        credential_id: r.credential_id,
        purpose: r.purpose,
        schema_version: r.schema_version,
    })?;
    if !canonical_ulid(&r.expected_boot_id) {
        return Err(Status::invalid_argument("LIFECYCLE_BOOT_INVALID"));
    }
    let _guard = lock(&id.credential_id).await?;
    let mut tx = store.pool().begin().await.map_err(db)?;
    // Original request identity takes precedence over current boot; never re-arm
    // a committed request after restart, or reinterpret changed parameters.
    if let Some(fact) = original(&mut tx, &id, Some(&r.expected_boot_id)).await? {
        let result = response(&mut tx, store, &id, Some(fact)).await?;
        tx.commit().await.map_err(db)?;
        return Ok(result);
    }
    if r.expected_boot_id != store.boot_id() {
        return Err(Status::failed_precondition("LIFECYCLE_BOOT_MISMATCH"));
    }
    let old = fence_row(&mut tx, store.kind(), &id.credential_id, true).await?;
    let revision = if let Some(row) = &old {
        if positive(row, "actor_member_id")? != id.actor_member_id
            || text(row, "account_uid")? != id.account_uid
        {
            return Err(conflict());
        }
        positive(row, "revision")?
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(corrupt)?
    } else {
        1
    };
    let now = crate::svc::now_ms();
    if old.is_none() {
        sqlx::query("INSERT INTO lifecycle_credential_fences(credential_key,credential_id,actor_member_id,account_uid,revision,first_request_ref,updated_at_ms) VALUES(?,?,?,?,?,?,?)")
            .bind(key(&id.credential_id)).bind(&id.credential_id).bind(id.actor_member_id as i64).bind(&id.account_uid)
            .bind(revision as i64).bind(&id.request_ref).bind(now).execute(&mut *tx).await.map_err(db)?;
    } else {
        sqlx::query("UPDATE lifecycle_credential_fences SET revision=?,updated_at_ms=? WHERE credential_key=?")
            .bind(revision as i64).bind(now).bind(key(&id.credential_id)).execute(&mut *tx).await.map_err(db)?;
    }
    let raw = serde_json::to_string(&id).map_err(|_| corrupt())?;
    sqlx::query("INSERT INTO lifecycle_fence_requests(request_ref,credential_key,identity_json,expected_boot_id,armed_boot_id,fence_revision,armed_at_ms,params_hash) VALUES(?,?,?,?,?,?,?,?)")
        .bind(&id.request_ref).bind(key(&id.credential_id)).bind(&raw).bind(&r.expected_boot_id).bind(store.boot_id())
        .bind(revision as i64).bind(now).bind(key(&format!("{raw}\n{}",r.expected_boot_id))).execute(&mut *tx).await.map_err(db)?;
    let fact = Some(p::LifecycleFenceArmFact {
        expected_boot_id: r.expected_boot_id,
        armed_boot_id: store.boot_id().into(),
        fence_revision: revision,
        armed_at_ms: now,
        schema_version: 1,
    });
    let result = response(&mut tx, store, &id, fact).await?;
    tx.commit().await.map_err(db)?;
    Ok(result)
}
