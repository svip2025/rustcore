//! Authority for one newly inserted protection submission, never an account lease.
//! A durable arm means may-have-sent. It cannot establish that packets were sent.
//! Dropping this value has no database side effect; old permits are not restored.
use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use sqlx::{Any, Row, Transaction};

use crate::store::{DbKind, Store};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlgoWire {
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub trigger_price: String,
    pub quantity: Option<String>,
    pub reduce_only: bool,
    pub close_position: bool,
    pub client_algo_id: String,
}

impl AlgoWire {
    pub(crate) fn validate_params(&self, params: &[(String, String)]) -> anyhow::Result<()> {
        let mut expected = std::collections::BTreeMap::from([
            ("symbol".to_owned(), self.symbol.clone()),
            ("side".to_owned(), self.side.clone()),
            ("algoType".to_owned(), "CONDITIONAL".to_owned()),
            ("type".to_owned(), self.order_type.clone()),
            ("triggerPrice".to_owned(), self.trigger_price.clone()),
            ("workingType".to_owned(), "MARK_PRICE".to_owned()),
            ("priceProtect".to_owned(), "true".to_owned()),
            ("clientAlgoId".to_owned(), self.client_algo_id.clone()),
        ]);
        if self.close_position {
            expected.insert("closePosition".into(), "true".into());
        } else {
            expected.insert(
                "quantity".into(),
                self.quantity
                    .clone()
                    .ok_or_else(|| anyhow!("dispatch quantity missing"))?,
            );
            expected.insert("reduceOnly".into(), self.reduce_only.to_string());
        }
        let mut observed = std::collections::BTreeMap::new();
        for (key, value) in params {
            if key != "timestamp"
                && key != "recvWindow"
                && observed.insert(key.clone(), value.clone()).is_some()
            {
                bail!("duplicate fixed protection wire field");
            }
        }
        if observed != expected {
            bail!("actual signed protection parameters differ from prepared wire binding");
        }
        Ok(())
    }
}

// This closed list captures exact persisted bytes, including every join owner.
const FIXED: &[&str] = &[
    "engine_order_id",
    "eo_credential",
    "bot_uid",
    "eo_position",
    "symbol_key",
    "exchange_name",
    "market_kind",
    "action",
    "side",
    "order_type",
    "order_role",
    "price",
    "quantity",
    "eo_reduce_only",
    "eo_client",
    "protection_order_id",
    "po_position",
    "po_credential",
    "kind",
    "generation",
    "po_engine",
    "po_client",
    "trigger_price",
    "po_reduce_only",
    "close_position",
    "position_id",
    "position_credential",
    "position_bot",
    "position_symbol",
    "direction",
    "entry_epoch",
];
const BUSINESS_QUERY: &str = r#"
SELECT eo.engine_order_id, eo.credential_id AS eo_credential, eo.bot_uid,
 eo.position_id AS eo_position, eo.symbol_key, eo.exchange AS exchange_name,
 eo.market_kind, eo.action, eo.side, eo.order_type, eo.order_role, eo.price, eo.quantity,
 CAST(eo.reduce_only AS CHAR) AS eo_reduce_only, eo.client_order_id_short AS eo_client,
 po.protection_order_id, po.position_id AS po_position, po.credential_id AS po_credential,
 po.kind, CAST(po.generation AS CHAR) AS generation, po.engine_order_id AS po_engine,
 po.client_algo_id AS po_client, po.trigger_price,
 CAST(po.reduce_only AS CHAR) AS po_reduce_only, CAST(po.close_position AS CHAR) AS close_position,
 p.position_id, p.credential_id AS position_credential, p.bot_uid AS position_bot,
 p.symbol_key AS position_symbol, p.direction, CAST(p.entry_epoch AS CHAR) AS entry_epoch,
 eo.status AS eo_status, po.status AS po_status, p.state AS position_state, p.qty
FROM engine_orders eo JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id
 JOIN position_state p ON p.position_id=po.position_id
WHERE eo.engine_order_id=? AND po.protection_order_id=?
"#;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Binding {
    fixed: Vec<String>,
    wire: AlgoWire,
}
impl Binding {
    fn value(&self, name: &str) -> &str {
        &self.fixed[FIXED
            .iter()
            .position(|field| *field == name)
            .expect("fixed binding field")]
    }
    fn validate(&self) -> anyhow::Result<()> {
        for (left, right) in [
            ("eo_credential", "po_credential"),
            ("eo_credential", "position_credential"),
            ("bot_uid", "position_bot"),
            ("symbol_key", "position_symbol"),
            ("eo_position", "po_position"),
            ("eo_position", "position_id"),
            ("engine_order_id", "po_engine"),
            ("eo_client", "po_client"),
            ("price", "trigger_price"),
            ("eo_reduce_only", "po_reduce_only"),
        ] {
            if self.value(left) != self.value(right) {
                bail!("protection dispatch binding conflict: {left}/{right}");
            }
        }
        let kind = self.value("kind");
        let expected_type = match kind {
            "sl" => "STOP_MARKET",
            "tp" => "TAKE_PROFIT_MARKET",
            _ => bail!("invalid protection kind"),
        };
        let expected_side = match self.value("action") {
            "close_long" => "SELL",
            "close_short" => "BUY",
            _ => bail!("invalid protection action"),
        };
        let quantity = if self.value("close_position") == "1" {
            None
        } else {
            Some(self.value("quantity").trim().to_owned())
        };
        if self.value("generation").parse::<i64>()? < 1
            || self.value("entry_epoch").parse::<i64>()? < 0
            || self.value("eo_reduce_only") != "1"
            || self.wire.order_type != expected_type
            || self.value("order_type").to_ascii_uppercase() != expected_type
            || self.value("order_role") != format!("protection_{kind}")
            || self.value("direction")
                != if expected_side == "SELL" {
                    "long"
                } else {
                    "short"
                }
            || self.value("side") != self.value("direction")
            || self.wire.side != expected_side
            || self.wire.trigger_price != self.value("price").trim()
            || self.wire.client_algo_id != self.value("eo_client")
            || self.wire.quantity != quantity
            || !self.wire.reduce_only
            || self.wire.close_position != (self.value("close_position") == "1")
        {
            bail!("protection dispatch fixed wire binding conflict");
        }
        let identity = crate::binance_futures::usdm_submission_identity(
            self.value("symbol_key"),
            self.value("exchange_name"),
            self.value("market_kind"),
        )
        .map_err(|e| anyhow!(e))?;
        if identity.symbol != self.wire.symbol {
            bail!("protection wire symbol mismatch");
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SafeDispatchDenial(&'static str);
impl std::fmt::Display for SafeDispatchDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for SafeDispatchDenial {}

/// Public diagnostics never expand arbitrary transport/HTTP error chains: a
/// reqwest cause may carry the signed URL. These categories carry no payload.
pub(crate) fn safe_error_detail(error: &anyhow::Error) -> String {
    if let Some(denial) = error.downcast_ref::<SafeDispatchDenial>() {
        return format!("PROTECTION_DISPATCH_NOT_STARTED: {denial}");
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some())
    {
        return "PROTECTION_DISPATCH_DATABASE_ERROR: local dispatch persistence failed".to_owned();
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<reqwest::Error>().is_some())
    {
        return "PROTECTION_DISPATCH_TRANSPORT_ERROR: request outcome unavailable".to_owned();
    }
    error.to_string()
}

pub(crate) enum FailureDisposition {
    NotStarted,
    LocalProtectionFailure,
    MayHaveSent,
}

pub(crate) struct ProtectionDispatchPermit {
    store: Store,
    attempt_id: String,
    owner_token: String,
    binding: Binding,
    revision: i64,
    authorized_post_count: i64,
    commit_uncertain: bool,
    finished: bool,
    #[cfg(test)]
    after_arm: Option<(
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    )>,
}

async fn business_row(
    tx: &mut Transaction<'_, Any>,
    kind: DbKind,
    engine: &str,
    protection: &str,
) -> anyhow::Result<sqlx::any::AnyRow> {
    let query = if kind == DbKind::MySql {
        format!("{BUSINESS_QUERY} FOR UPDATE")
    } else {
        BUSINESS_QUERY.to_owned()
    };
    sqlx::query(&query)
        .bind(engine)
        .bind(protection)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("protection dispatch business identity missing"))
}
fn fixed_from_row(row: &sqlx::any::AnyRow) -> anyhow::Result<Vec<String>> {
    FIXED
        .iter()
        .map(|field| {
            row.try_get::<String, _>(*field)
                .with_context(|| format!("missing protection identity {field}"))
        })
        .collect()
}

/// Only called inside the transaction which just inserted eo+po. A rolled back
/// insert leaves no durable authority, even if the returned value is retained.
pub(crate) async fn prepare_in_tx(
    store: &Store,
    tx: &mut Transaction<'_, Any>,
    engine: &str,
    protection: &str,
    wire: AlgoWire,
    now: i64,
) -> anyhow::Result<ProtectionDispatchPermit> {
    let row = business_row(tx, store.kind(), engine, protection).await?;
    let binding = Binding {
        fixed: fixed_from_row(&row)?,
        wire,
    };
    binding.validate()?;
    if binding.value("engine_order_id") != engine
        || binding.value("protection_order_id") != protection
    {
        bail!("protection dispatch exact insertion identity mismatch");
    }
    let attempt_id = format!("dispatch_{}", ulid::Ulid::new());
    let owner_token = ulid::Ulid::new().to_string();
    sqlx::query("INSERT INTO protection_dispatch_attempts (attempt_id,engine_order_id,protection_order_id,owner_token,binding_json,revision,authorized_post_count,phase,dispatch_closed,sender_finished,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,0,0,'prepared',0,0,?,?)")
        .bind(&attempt_id).bind(engine).bind(protection).bind(&owner_token).bind(serde_json::to_string(&binding)?).bind(now).bind(now).execute(&mut **tx).await?;
    Ok(ProtectionDispatchPermit {
        store: store.clone(),
        attempt_id,
        owner_token,
        binding,
        revision: 0,
        authorized_post_count: 0,
        commit_uncertain: false,
        finished: false,
        #[cfg(test)]
        after_arm: None,
    })
}

impl ProtectionDispatchPermit {
    pub(crate) fn disposition(&self, error: &anyhow::Error) -> FailureDisposition {
        if self.authorized_post_count > 0 || self.commit_uncertain {
            FailureDisposition::MayHaveSent
        } else if error.downcast_ref::<SafeDispatchDenial>().is_some() {
            FailureDisposition::NotStarted
        } else {
            FailureDisposition::LocalProtectionFailure
        }
    }
    /// The outer order CAS can reject before the submit future is polled.
    /// Inspect only; this path never authorizes or fabricates an arm.
    pub(crate) async fn unpolled_denial(&mut self) -> (FailureDisposition, anyhow::Error) {
        let result: anyhow::Result<()> = async {
            let mut tx = self.store.pool().begin().await?;
            let attempt = self.attempt_row(&mut tx).await?;
            let count = crate::store::try_get_i64_text(&attempt, "count_text")?;
            self.authorized_post_count = self.authorized_post_count.max(count);
            let current = business_row(
                &mut tx,
                self.store.kind(),
                self.binding.value("engine_order_id"),
                self.binding.value("protection_order_id"),
            )
            .await?;
            if fixed_from_row(&current)? != self.binding.fixed {
                bail!("unpolled protection persisted identity changed");
            }
            let state: String = current.try_get("position_state")?;
            let qty: String = current.try_get("qty")?;
            if state == "closed" && crate::reconcile::snapshot_decimal_is_zero(&qty) == Some(true) {
                return Err(SafeDispatchDenial(
                    "unpolled protection position is exactly closed and strictly zero",
                )
                .into());
            }
            bail!("unpolled protection order stopped while position still requires management")
        }
        .await;
        let error = result.expect_err("unpolled denial never grants authority");
        (self.disposition(&error), error)
    }
    pub(crate) fn finished(&self) -> bool {
        self.finished
    }
    async fn attempt_row(
        &self,
        tx: &mut Transaction<'_, Any>,
    ) -> anyhow::Result<sqlx::any::AnyRow> {
        // Obtain the SQLite writer lock before reads. No business facts change.
        if self.store.kind() != DbKind::MySql {
            sqlx::query(
                "UPDATE protection_dispatch_attempts SET revision=revision WHERE attempt_id=?",
            )
            .bind(&self.attempt_id)
            .execute(&mut **tx)
            .await?;
        }
        let query = if self.store.kind() == DbKind::MySql {
            "SELECT *, CAST(revision AS CHAR) AS rev_text, CAST(authorized_post_count AS CHAR) AS count_text FROM protection_dispatch_attempts WHERE attempt_id=? FOR UPDATE"
        } else {
            "SELECT *, CAST(revision AS CHAR) AS rev_text, CAST(authorized_post_count AS CHAR) AS count_text FROM protection_dispatch_attempts WHERE attempt_id=?"
        };
        let row = sqlx::query(query)
            .bind(&self.attempt_id)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("protection dispatch attempt missing; no authority"))?;
        if row.try_get::<String, _>("attempt_id")? != self.attempt_id
            || row.try_get::<String, _>("engine_order_id")? != self.binding.value("engine_order_id")
            || row.try_get::<String, _>("protection_order_id")?
                != self.binding.value("protection_order_id")
            || row.try_get::<String, _>("owner_token")? != self.owner_token
            || crate::store::try_get_text(&row, "binding_json")?.as_deref()
                != Some(serde_json::to_string(&self.binding)?.as_str())
        {
            bail!("protection dispatch immutable owner/binding mismatch");
        }
        Ok(row)
    }
    /// Called after all queuing/signing, for every retry. Commit is the grant
    /// linearization point; a later fence cannot revoke this in-flight grant.
    pub(crate) async fn authorize(
        &mut self,
        credential: &str,
        wire: &AlgoWire,
    ) -> anyhow::Result<()> {
        if self.finished
            || credential != self.binding.value("eo_credential")
            || wire != &self.binding.wire
        {
            bail!("protection dispatch caller binding mismatch");
        }
        let mut tx = self.store.pool().begin().await?;
        let row = self.attempt_row(&mut tx).await?;
        let revision = crate::store::try_get_i64_text(&row, "rev_text")?;
        let count = crate::store::try_get_i64_text(&row, "count_text")?;
        let previous_count = self.authorized_post_count;
        self.authorized_post_count = previous_count.max(count);
        let current = business_row(
            &mut tx,
            self.store.kind(),
            self.binding.value("engine_order_id"),
            self.binding.value("protection_order_id"),
        )
        .await?;
        if fixed_from_row(&current)? != self.binding.fixed {
            bail!("protection dispatch persisted identity changed");
        }
        let position_state: String = current.try_get("position_state")?;
        let qty: String = current.try_get("qty")?;
        if position_state == "closed"
            && crate::reconcile::snapshot_decimal_is_zero(&qty) == Some(true)
        {
            return Err(SafeDispatchDenial(
                "protection position is exactly closed and strictly zero",
            )
            .into());
        }
        if row.try_get::<i64, _>("dispatch_closed")? != 0 {
            bail!("protection dispatch fenced while position still requires management");
        }
        if revision != self.revision || count != previous_count {
            bail!("protection dispatch revision mismatch");
        }
        let eo: String = current.try_get("eo_status")?;
        let po: String = current.try_get("po_status")?;
        if eo != "pending_submit" || po != "requested" {
            bail!("protection order has left its permitted submission state while position requires management");
        }
        let changed = sqlx::query("UPDATE protection_dispatch_attempts SET phase='armed', authorized_post_count=authorized_post_count+1, revision=revision+1, updated_at_ms=? WHERE attempt_id=? AND owner_token=? AND revision=? AND dispatch_closed=0 AND sender_finished=0")
            .bind(crate::svc::now_ms()).bind(&self.attempt_id).bind(&self.owner_token).bind(revision).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            bail!("protection dispatch authorization CAS rejected");
        }
        self.commit_uncertain = true;
        tx.commit()
            .await
            .context("protection dispatch arm commit uncertain")?;
        self.revision = revision + 1;
        self.authorized_post_count = count + 1;
        self.commit_uncertain = false;
        #[cfg(test)]
        if let Some((arrived, resume)) = self.after_arm.as_ref() {
            arrived.notify_one();
            resume.notified().await;
        }
        Ok(())
    }
    pub(crate) async fn lock_for_ack(&self, tx: &mut Transaction<'_, Any>) -> anyhow::Result<()> {
        self.attempt_row(tx).await?;
        let row = business_row(
            tx,
            self.store.kind(),
            self.binding.value("engine_order_id"),
            self.binding.value("protection_order_id"),
        )
        .await?;
        if fixed_from_row(&row)? != self.binding.fixed {
            bail!("protection ACK persisted identity changed");
        }
        Ok(())
    }
    /// Invoked by the completed logical caller; never Drop, timeout or age.
    /// ACK callers use this inside the transaction persisting validated ACK.
    pub(crate) async fn finish_in_tx(
        &self,
        tx: &mut Transaction<'_, Any>,
        outcome: &str,
    ) -> anyhow::Result<()> {
        let row = self.attempt_row(tx).await?;
        let revision = crate::store::try_get_i64_text(&row, "rev_text")?;
        if row.try_get::<i64, _>("sender_finished")? != 0 {
            bail!("protection sender already closed");
        }
        let changed = sqlx::query("UPDATE protection_dispatch_attempts SET dispatch_closed=1,sender_finished=1,outcome=?,revision=revision+1,updated_at_ms=? WHERE attempt_id=? AND owner_token=? AND revision=? AND sender_finished=0")
            .bind(outcome).bind(crate::svc::now_ms()).bind(&self.attempt_id).bind(&self.owner_token).bind(revision).execute(&mut **tx).await?.rows_affected();
        if changed != 1 {
            bail!("protection dispatch closure CAS rejected");
        }
        Ok(())
    }
    pub(crate) fn acknowledge_committed_finish(&mut self) {
        self.finished = true;
    }
    pub(crate) async fn finish(&mut self, outcome: &str) -> anyhow::Result<()> {
        let mut tx = self.store.pool().begin().await?;
        self.finish_in_tx(&mut tx, outcome).await?;
        tx.commit()
            .await
            .context("protection dispatch closure commit uncertain")?;
        self.finished = true;
        Ok(())
    }
}

/// Exact internal fence. It says nothing about an already armed network call.
#[allow(dead_code)]
pub(crate) async fn fence(
    store: &Store,
    attempt_id: &str,
    expected_revision: i64,
) -> anyhow::Result<bool> {
    Ok(sqlx::query("UPDATE protection_dispatch_attempts SET dispatch_closed=1,revision=revision+1,updated_at_ms=? WHERE HEX(attempt_id)=HEX(?) AND revision=? AND dispatch_closed=0 AND sender_finished=0")
        .bind(crate::svc::now_ms()).bind(attempt_id).bind(expected_revision).execute(store.pool()).await?.rows_affected() == 1)
}

#[cfg(test)]
pub(crate) mod tests;
