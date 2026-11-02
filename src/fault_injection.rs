//! Process-level L4 fault checkpoints.
//!
//! This module is only compiled with the `l4-fault-injection` Cargo feature.
//! Even in that build, checkpoints require `L4_FAULT_INJECTION_ENABLED=1`
//! and absolute filesystem transport paths. Normal builds contain no
//! runtime-selectable fault branch. Remove this module once an external crash
//! controller can observe the same durable boundaries without cooperation.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use sqlx::Row;

use crate::store::Store;

const ENABLED: &str = "L4_FAULT_INJECTION_ENABLED";
const POINT: &str = "FAULT_INJECT";
const MARKER_DIR: &str = "L4_FAULT_MARKER_DIR";

pub(crate) fn enabled() -> bool {
    std::env::var(ENABLED).is_ok_and(|value| value.trim() == "1")
}

pub(crate) fn validate_file_transport() -> anyhow::Result<()> {
    if !enabled() {
        bail!("L4_FAULT_INJECTION_ENABLED=1 is required");
    }
    for name in ["L4_FAKE_EXCHANGE_DB", "L4_FILE_RPC_DIR"] {
        let raw = std::env::var(name).with_context(|| format!("{name} is required"))?;
        if !Path::new(raw.trim()).is_absolute() {
            bail!("{name} must be absolute");
        }
    }
    Ok(())
}

pub(crate) fn selected(point: &str) -> bool {
    enabled() && std::env::var(POINT).is_ok_and(|value| value.trim() == point)
}

pub(crate) fn require_transaction_truth(
    point: &str,
    verdict_is_pending: bool,
) -> anyhow::Result<()> {
    if selected(point) && !verdict_is_pending {
        bail!("selected in-transaction checkpoint requires one pending risk verdict");
    }
    Ok(())
}

fn marker_path(point: &str) -> anyhow::Result<PathBuf> {
    let dir = std::env::var(MARKER_DIR).context("L4_FAULT_MARKER_DIR is required")?;
    let path = Path::new(dir.trim());
    if !path.is_absolute() {
        bail!("L4_FAULT_MARKER_DIR must be absolute");
    }
    Ok(path.join(format!("{point}.ready")))
}

fn stop_at(point: &str) -> anyhow::Result<()> {
    validate_file_transport()?;
    let marker = marker_path(point)?;
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent).context("create L4 marker directory")?;
    }
    std::fs::write(
        &marker,
        format!("pid={}\npoint={point}\n", std::process::id()),
    )
    .with_context(|| format!("write L4 marker {}", marker.display()))?;
    // Hold the exact durable boundary; the external harness delivers SIGKILL.
    let result = unsafe { libc::raise(libc::SIGSTOP) };
    if result != 0 {
        bail!("raise(SIGSTOP) failed");
    }
    Ok(())
}

pub(crate) fn checkpoint(point: &str) -> anyhow::Result<()> {
    if selected(point) {
        stop_at(point)?;
    }
    Ok(())
}

pub(crate) async fn startup_checkpoint(store: &Store) -> anyhow::Result<()> {
    let point = std::env::var(POINT).unwrap_or_default();
    match point.trim() {
        "after_sl_confirmed" if selected("after_sl_confirmed") => {
            let count = sqlx::query(
                "SELECT COUNT(*) AS n FROM protection_orders WHERE kind='sl' AND verified_from_exchange=1",
            )
            .fetch_one(store.pool())
            .await?
            .get::<i64, _>("n");
            if count < 1 {
                bail!("after_sl_confirmed requires a durable confirmed SL");
            }
            stop_at("after_sl_confirmed")?;
        }
        "after_fill_before_sl" if selected("after_fill_before_sl") => {
            let count = sqlx::query(
                r#"
                SELECT COUNT(*) AS n
                FROM position_state ps
                JOIN position_protection_plans pp ON pp.position_id=ps.position_id
                WHERE ps.state='open'
                  AND NOT EXISTS (
                    SELECT 1 FROM protection_orders po
                    WHERE po.position_id=ps.position_id
                      AND po.kind='sl' AND po.verified_from_exchange=1
                  )
                "#,
            )
            .fetch_one(store.pool())
            .await?
            .get::<i64, _>("n");
            if count < 1 {
                bail!(
                    "after_fill_before_sl requires an open planned position without confirmed SL"
                );
            }
            stop_at("after_fill_before_sl")?;
        }
        _ => {}
    }
    Ok(())
}
