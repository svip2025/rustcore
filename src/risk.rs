use ai_trade_contracts::sha256_hex;
use risk_policy::{
    self, AccountSnapshot, Action, DataSnapshot, Decision, Input, Intent, KillSwitch,
    LegacySizingInput, Policy, Position, PromotionCertification, Reservation, RiskBudget,
    RiskVerdict, RuleOutcome, RuleResult, StageCertificationInput,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tonic::Status;

use crate::protection::OrderRole;
use crate::proto;
use crate::store::{try_get_nullable_string, try_get_text, DbKind};

#[derive(Clone, Debug)]
pub(crate) struct TypedProtectionInput {
    pub stop_loss_price: String,
    pub take_profit_price: String,
}

#[derive(Clone, Debug)]
pub(crate) struct WireRiskContext {
    pub typed_protection_capable: bool,
    pub data_snapshot_capable: bool,
    pub data_snapshot: Option<DataSnapshot>,
    pub intent_id: String,
    pub protection: Option<TypedProtectionInput>,
    pub valid_until_ms: Option<i64>,
    pub max_slippage_bps: Option<u32>,
    pub config_hash: String,
    pub prompt_hash: String,
    pub policy_version: String,
    pub code_commit: String,
}

pub(crate) fn map_v2_data_snapshot(
    snapshot: Option<&crate::proto_v2::DataSnapshotDecision>,
) -> Option<DataSnapshot> {
    let snapshot = snapshot?;
    let id = snapshot.snapshot_id.as_bytes();
    let valid_id = id.len() == 37
        && id.starts_with(b"snap_")
        && id[5..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte));
    if !valid_id {
        // A malformed content-addressed id cannot be linked to the durable
        // snapshot row, so it is deliberately indistinguishable from absence.
        return None;
    }

    let mut errors = Vec::new();
    if !snapshot.allow_close {
        errors.push("allow_close=false violates the close-always-open invariant".to_string());
    }
    let gate_allows_open = snapshot.quality_gate == "pass";
    if !matches!(snapshot.quality_gate.as_str(), "pass" | "degraded" | "fail") {
        errors.push(format!(
            "quality_gate={} is not pass|degraded|fail",
            snapshot.quality_gate
        ));
    } else if gate_allows_open != snapshot.allow_open {
        errors.push(format!(
            "quality_gate={} is inconsistent with allow_open={}",
            snapshot.quality_gate, snapshot.allow_open
        ));
    }

    Some(DataSnapshot {
        // Both the gate and the explicit boolean must permit opening. Any
        // consistency error also fails closed, including allow_close=false.
        allow_open: gate_allows_open && snapshot.allow_open && errors.is_empty(),
        consistency_error: (!errors.is_empty()).then(|| errors.join("; ")),
    })
}

fn parse_decimal(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn action(value: &str) -> Action {
    match value {
        "open_long" => Action::OpenLong,
        "open_short" => Action::OpenShort,
        "close_long" => Action::CloseLong,
        "close_short" => Action::CloseShort,
        _ => Action::Hold,
    }
}

fn decision(value: &Decision) -> &'static str {
    match value {
        Decision::Approve => "approve",
        Decision::Resize => "resize",
        Decision::Reject => "reject",
    }
}

pub(crate) fn verdict_audit_payload(verdict: &RiskVerdict) -> serde_json::Value {
    serde_json::json!({
        "riskVerdictId": verdict.risk_verdict_id.as_str(),
        "intentId": verdict.intent_id.as_str(),
        "createdAtMs": verdict.created_at_ms,
        "policyVersion": verdict.policy_version.as_str(),
        "policyHash": verdict.policy_hash.as_str(),
        "decision": decision(&verdict.decision),
        "rejectReasonCode": verdict.reject_reason_code.as_deref(),
        "sizingJson": verdict.sizing_json().as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .unwrap_or(serde_json::Value::Null),
        "rulesJson": serde_json::from_str::<serde_json::Value>(&verdict.rules_json())
            .unwrap_or_else(|_| serde_json::Value::Array(Vec::new())),
        "budgetAsOfMs": verdict.budget_as_of_ms,
        "evaluatorVersion": verdict.evaluator_version.as_str(),
    })
}

fn apply_runtime_limits(mut policy: Policy) -> Policy {
    policy.preconditions.risk_budget_max_age_ms = std::env::var("RISK_BUDGET_MAX_AGE_MS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30_000);
    policy
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PolicyOpenGuard {
    rule: String,
    reason_code: String,
    detail: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FillRecoveryGuardDisposition {
    RejectEntry,
    SkipClose,
    SkipHold,
    SkipReduceOnly,
    SkipProtection,
    SkipL3Flatten,
}

const CREDENTIAL_DEGRADE_GUARD_RULE: &str = "credential_degrade_order_guard";

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CredentialDegradeGuardDisposition {
    RejectEntry,
    SkipClose,
    SkipHold,
    SkipReduceOnly,
    SkipProtection,
    SkipL3Flatten,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CredentialDegradeGuard {
    rule: String,
    reason_code: String,
    detail: String,
    disposition: CredentialDegradeGuardDisposition,
}

const INVALID_PERSISTED_KILL_SWITCH_RULE: &str = "persisted_kill_switch_integrity";
const INVALID_PERSISTED_KILL_SWITCH_REASON_CODE: &str = "INVALID_PERSISTED_KILL_SWITCH";

/// A malformed persisted kill-switch value is an entry guard, not a global
/// execution failure.  Safety actions must keep their existing escape hatches
/// even when the authority row itself needs operator repair.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum InvalidPersistedKillSwitchGuardDisposition {
    RejectEntry,
    SkipClose,
    SkipHold,
    SkipReduceOnly,
    SkipProtection,
    SkipL3Flatten,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct InvalidPersistedKillSwitchGuard {
    rule: String,
    reason_code: String,
    detail: String,
    disposition: InvalidPersistedKillSwitchGuardDisposition,
}

fn invalid_persisted_kill_switch_guard_disposition(
    action: &str,
    reduce_only: bool,
    order_role: OrderRole,
) -> InvalidPersistedKillSwitchGuardDisposition {
    if order_role == OrderRole::Emergency {
        InvalidPersistedKillSwitchGuardDisposition::SkipL3Flatten
    } else if matches!(
        order_role,
        OrderRole::ProtectionSl | OrderRole::ProtectionTp
    ) {
        InvalidPersistedKillSwitchGuardDisposition::SkipProtection
    } else if reduce_only {
        InvalidPersistedKillSwitchGuardDisposition::SkipReduceOnly
    } else if action == "hold" {
        // V2 maps Hold to the Exit role, but it still has no exchange
        // exposure and must retain the more precise hold disposition.
        InvalidPersistedKillSwitchGuardDisposition::SkipHold
    } else if matches!(action, "close_long" | "close_short") || order_role == OrderRole::Exit {
        InvalidPersistedKillSwitchGuardDisposition::SkipClose
    } else if !matches!(action, "open_long" | "open_short") {
        InvalidPersistedKillSwitchGuardDisposition::SkipHold
    } else {
        InvalidPersistedKillSwitchGuardDisposition::RejectEntry
    }
}

pub(crate) fn credential_degrade_guard_disposition(
    action: &str,
    reduce_only: bool,
    order_role: OrderRole,
) -> CredentialDegradeGuardDisposition {
    if order_role == OrderRole::Emergency {
        CredentialDegradeGuardDisposition::SkipL3Flatten
    } else if matches!(
        order_role,
        OrderRole::ProtectionSl | OrderRole::ProtectionTp
    ) {
        CredentialDegradeGuardDisposition::SkipProtection
    } else if reduce_only {
        CredentialDegradeGuardDisposition::SkipReduceOnly
    } else if matches!(action, "close_long" | "close_short") || order_role == OrderRole::Exit {
        CredentialDegradeGuardDisposition::SkipClose
    } else if !matches!(action, "open_long" | "open_short") {
        CredentialDegradeGuardDisposition::SkipHold
    } else {
        CredentialDegradeGuardDisposition::RejectEntry
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FillRecoveryOpenGuard {
    rule: String,
    reason_code: String,
    detail: String,
    disposition: FillRecoveryGuardDisposition,
}

pub(crate) fn fill_recovery_guard_disposition(
    action: &str,
    reduce_only: bool,
    order_role: OrderRole,
) -> FillRecoveryGuardDisposition {
    if order_role == OrderRole::Emergency {
        FillRecoveryGuardDisposition::SkipL3Flatten
    } else if matches!(
        order_role,
        OrderRole::ProtectionSl | OrderRole::ProtectionTp
    ) {
        FillRecoveryGuardDisposition::SkipProtection
    } else if reduce_only {
        FillRecoveryGuardDisposition::SkipReduceOnly
    } else if matches!(action, "close_long" | "close_short") || order_role == OrderRole::Exit {
        FillRecoveryGuardDisposition::SkipClose
    } else if !matches!(action, "open_long" | "open_short") {
        FillRecoveryGuardDisposition::SkipHold
    } else {
        FillRecoveryGuardDisposition::RejectEntry
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplayReject {
    rule: String,
    reason_code: String,
    detail: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReplayRuleOutcome {
    Pass,
    Capped,
    Skipped,
    Reject,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplayRule {
    rule: String,
    outcome: ReplayRuleOutcome,
    reason_code: Option<String>,
    reason: Option<String>,
    detail: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RiskReplayInput {
    input: Input,
    #[serde(default)]
    entry_market_reference: Option<EntryMarketReference>,
    policy_open_guard: Option<PolicyOpenGuard>,
    #[serde(default)]
    invalid_persisted_kill_switch_guard: Option<InvalidPersistedKillSwitchGuard>,
    #[serde(default)]
    credential_degrade_guard: Option<CredentialDegradeGuard>,
    #[serde(default)]
    fill_recovery_open_guard: Option<FillRecoveryOpenGuard>,
    post_evaluation_rule: Option<ReplayRule>,
    post_evaluation_reject: Option<ReplayReject>,
}

async fn load_fill_recovery_open_guard(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
    action: &str,
    reduce_only: bool,
    order_role: OrderRole,
    run_mode: &str,
) -> Result<Option<FillRecoveryOpenGuard>, Status> {
    if !matches!(run_mode, "demo" | "live_small" | "live") {
        return Ok(None);
    }
    let row = sqlx::query(
        r#"
        SELECT ready, generation, phase
        FROM fill_stream_state
        WHERE credential_id = ?
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load fill stream state failed: {error}")))?
    .into_iter()
    .next();
    if row
        .as_ref()
        .is_some_and(|row| row.get::<i64, _>("ready") != 0)
    {
        return Ok(None);
    }
    let (generation, phase) = row
        .map(|row| {
            (
                row.get::<i64, _>("generation"),
                row.get::<String, _>("phase"),
            )
        })
        .unwrap_or_else(|| (0, "not_started".to_string()));
    Ok(Some(FillRecoveryOpenGuard {
        rule: crate::fill_replay::FILL_RECOVERY_RULE.to_string(),
        reason_code: crate::fill_replay::FILL_RECOVERY_REASON_CODE.to_string(),
        detail: format!(
            "credential={credential_id} symbol_key={symbol_key} generation={generation} phase={phase}"
        ),
        disposition: fill_recovery_guard_disposition(action, reduce_only, order_role),
    }))
}

async fn load_policy(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
) -> Result<(Policy, String, Option<PolicyOpenGuard>), Status> {
    let row = sqlx::query(
        r#"
        SELECT policy_hash, policy_json
        FROM risk_policies_snapshot
        WHERE activated_at_ms IS NOT NULL
        ORDER BY activated_at_ms DESC
        LIMIT 1
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load risk policy failed: {error}")))?
    .into_iter()
    .next();

    if let Some(row) = row {
        let json = row.get::<String, _>("policy_json");
        let policy = serde_json::from_str::<Policy>(&json)
            .map_err(|error| Status::internal(format!("invalid persisted risk policy: {error}")))?;
        let persisted_hash = row.get::<String, _>("policy_hash");
        let computed_hash = risk_policy::policy_hash(&policy);
        let factory = risk_policy::factory_policy();
        let open_guard = if persisted_hash != computed_hash {
            Some(PolicyOpenGuard {
                rule: "policy_hash_integrity".to_string(),
                reason_code: "POLICY_HASH_MISMATCH".to_string(),
                detail: format!(
                    "version={} persisted policy hash does not match canonical policy JSON",
                    policy.policy_version
                ),
            })
        } else if policy.policy_version == factory.policy_version
            && computed_hash != risk_policy::FACTORY_POLICY_HASH.trim()
        {
            Some(PolicyOpenGuard {
                rule: "factory_policy_freshness".to_string(),
                reason_code: "STALE_FACTORY_POLICY".to_string(),
                detail: format!(
                    "version={} persisted_hash={} factory_hash={}",
                    policy.policy_version,
                    computed_hash,
                    risk_policy::FACTORY_POLICY_HASH.trim()
                ),
            })
        } else {
            None
        };
        let policy = apply_runtime_limits(policy);
        Ok((
            policy.clone(),
            risk_policy::policy_hash(&policy),
            open_guard,
        ))
    } else {
        let policy = apply_runtime_limits(risk_policy::factory_policy());
        let hash = risk_policy::policy_hash(&policy);
        Ok((policy, hash, None))
    }
}

async fn load_positions(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    policy: &Policy,
) -> Result<Vec<Position>, Status> {
    let rows = sqlx::query(
        r#"
        SELECT bot_uid, symbol_key, direction, state, qty,
               COALESCE(pending_idem_key, '') AS pending_idem_key,
               COALESCE(entry_price, '0') AS entry_price
        FROM position_state
        WHERE credential_id = ?
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load risk positions failed: {error}")))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let symbol_key = row.get::<String, _>("symbol_key");
            let base = symbol_key.split('|').nth(2).unwrap_or_default();
            let correlation_group = policy
                .portfolio
                .correlation_groups
                .iter()
                .find(|(_, symbols)| symbols.iter().any(|symbol| symbol == base))
                .map(|(name, _)| name.clone());
            let qty = parse_decimal(&row.get::<String, _>("qty"))
                .unwrap_or(0.0)
                .abs();
            let price = parse_decimal(&row.get::<String, _>("entry_price"))
                .unwrap_or(0.0)
                .abs();
            Position {
                bot_uid: row.get("bot_uid"),
                symbol_key,
                direction: row.get("direction"),
                state: row.get("state"),
                notional_usdt: qty * price,
                correlation_group,
                pending_retry: row.get::<String, _>("state") == "pending"
                    && !row.get::<String, _>("pending_idem_key").is_empty(),
            }
        })
        .collect())
}

async fn load_promotion_certifications(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    bot_uid: &str,
) -> Result<Vec<PromotionCertification>, Status> {
    let rows = sqlx::query(
        r#"
        SELECT cert_id, stage, config_hash, prompt_hash, policy_version,
               code_commit, passed,
               CAST(invalidated_at_ms AS CHAR) AS invalidated_at_ms_text,
               CAST(certified_at_ms AS CHAR) AS certified_at_ms_text
        FROM promotion_certifications_projection
        WHERE bot_uid = ?
        "#,
    )
    .bind(bot_uid)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Status::internal(format!(
            "db load promotion certification projection failed: {error}"
        ))
    })?;

    rows.into_iter()
        .map(|row| {
            let invalidated_at_ms = try_get_nullable_string(&row, "invalidated_at_ms_text")?
                .map(|value| {
                    value.parse::<i64>().map_err(|_| {
                        sqlx::Error::Protocol(
                            "stored promotion invalidated_at_ms is invalid".to_string(),
                        )
                    })
                })
                .transpose()?;
            let certified_at_ms = row
                .get::<String, _>("certified_at_ms_text")
                .parse::<i64>()
                .map_err(|_| {
                    sqlx::Error::Protocol("stored promotion certified_at_ms is invalid".to_string())
                })?;
            Ok(PromotionCertification {
                cert_id: row.get("cert_id"),
                stage: row.get("stage"),
                config_hash: row.get("config_hash"),
                prompt_hash: row.get("prompt_hash"),
                policy_version: row.get("policy_version"),
                code_commit: row.get("code_commit"),
                passed: row.get::<i64, _>("passed") != 0,
                invalidated_at_ms,
                certified_at_ms,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(|error| {
            Status::internal(format!(
                "decode promotion certification projection failed: {error}"
            ))
        })
}

async fn load_risk_budget(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> Result<(Option<RiskBudget>, Option<AccountSnapshot>), Status> {
    let row = sqlx::query(
        r#"
        SELECT CAST(as_of_ms AS CHAR) AS as_of_ms_text,
               equity_usdt, available_usdt,
               today_realized_pnl_usdt, today_realized_pnl_pct,
               drawdown_pct, total_exposure_pct, open_positions_count,
               group_exposure_json, remaining_credential_notional_usdt
        FROM risk_budget
        WHERE credential_id = ?
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load risk budget failed: {error}")))?
    .into_iter()
    .next();

    let Some(row) = row else {
        return Ok((None, None));
    };
    // sqlx 0.7.4 Any/SQLite historically narrowed BIGINT reads; 0.8.6 fixes it.
    // Keep the existing decimal projection for stable cross-dialect decoding.
    let as_of_ms = row
        .get::<String, _>("as_of_ms_text")
        .parse::<i64>()
        .unwrap_or_default();
    let equity = parse_decimal(&row.get::<String, _>("equity_usdt"));
    let available = parse_decimal(&row.get::<String, _>("available_usdt"));
    let realized_usdt = parse_decimal(&row.get::<String, _>("today_realized_pnl_usdt"));
    let realized_pct = row.get::<f64, _>("today_realized_pnl_pct");
    let drawdown_pct = row.get::<f64, _>("drawdown_pct");
    let total_exposure_pct = row.get::<f64, _>("total_exposure_pct");
    let open_positions_count = row.get::<i64, _>("open_positions_count");
    let remaining_valid = match try_get_nullable_string(&row, "remaining_credential_notional_usdt")
    {
        Ok(Some(value)) => parse_decimal(&value).is_some(),
        Ok(None) => true,
        Err(error) => {
            tracing::error!(
                column = "remaining_credential_notional_usdt",
                %error,
                "risk budget optional column decode failed"
            );
            false
        }
    };
    let groups_valid = match try_get_text(&row, "group_exposure_json") {
        Ok(Some(json)) => serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json)
            .map(|groups| {
                groups.values().all(|value| {
                    value
                        .as_f64()
                        .map(|number| number.is_finite() && number >= 0.0)
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false),
        Ok(None) => true,
        Err(error) => {
            tracing::error!(
                column = "group_exposure_json",
                %error,
                "risk budget optional column decode failed"
            );
            false
        }
    };

    let critical_valid = as_of_ms > 0
        && equity.is_some()
        && available.is_some()
        && realized_usdt.is_some()
        && realized_pct.is_finite()
        && drawdown_pct.is_finite()
        && drawdown_pct >= 0.0
        && total_exposure_pct.is_finite()
        && total_exposure_pct >= 0.0
        && open_positions_count >= 0
        && groups_valid
        && remaining_valid;
    if !critical_valid {
        // A malformed or incomplete row is equivalent to no usable budget.
        // The existing freshness rule then produces STALE_RISK_BUDGET for an
        // opening intent while close intents retain their explicit exemption.
        return Ok((None, None));
    }

    let equity = equity.unwrap_or_default().max(0.0);
    let total_exposure_usdt = equity * total_exposure_pct;
    Ok((
        Some(RiskBudget { as_of_ms }),
        Some(AccountSnapshot {
            equity_usdt: equity,
            available_margin_usdt: available.unwrap_or_default().max(0.0),
            today_realized_pnl_pct: Some(realized_pct),
            drawdown_pct: Some(drawdown_pct),
            total_exposure_usdt,
            credential_notional_usdt: total_exposure_usdt,
        }),
    ))
}

#[derive(Debug)]
enum BudgetKillSwitchRead {
    Inactive,
    Active {
        level: crate::kill_switch::Level,
        reason: String,
    },
    Invalid(crate::kill_switch::InvalidPersistedLevel),
}

async fn load_budget_kill_switch(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> Result<BudgetKillSwitchRead, Status> {
    let row = sqlx::query(
        r#"
        SELECT kill_switch_level, COALESCE(kill_switch_scope, '') AS kill_switch_scope,
               COALESCE(kill_switch_reason, '') AS kill_switch_reason
        FROM risk_budget WHERE credential_id=? LIMIT 1
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load budget kill switch failed: {error}")))?
    .into_iter()
    .next();
    let Some(row) = row else {
        return Ok(BudgetKillSwitchRead::Inactive);
    };
    let level_text = row.get::<String, _>("kill_switch_level");
    let Some(level) = crate::kill_switch::parse_persisted_level(&level_text) else {
        return Ok(BudgetKillSwitchRead::Invalid(
            crate::kill_switch::InvalidPersistedLevel {
                scope_key: format!("risk_budget:cred:{credential_id}"),
                raw_level: level_text,
            },
        ));
    };
    if level == crate::kill_switch::Level::None {
        return Ok(BudgetKillSwitchRead::Inactive);
    }
    let scope = row.get::<String, _>("kill_switch_scope");
    if !scope.is_empty() && scope != "credential" {
        return Err(Status::failed_precondition(
            "risk_budget active kill switch must use credential scope; use SetKillSwitch for global/bot",
        ));
    }
    Ok(BudgetKillSwitchRead::Active {
        level,
        reason: row.get::<String, _>("kill_switch_reason"),
    })
}

async fn arm_or_preserve_invalid(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    request: crate::kill_switch::ArmRequest<'_>,
) -> Result<(), Status> {
    let scope_key = request.scope_key.to_string();
    let trigger_source = request.trigger_source.to_string();
    match crate::kill_switch::arm_in_tx(tx, db_kind, request).await {
        Ok(_) => Ok(()),
        Err(error)
            if error
                .downcast_ref::<crate::kill_switch::InvalidPersistedLevel>()
                .is_some() =>
        {
            // Preserve the undecodable authority byte-for-byte.  A later
            // effective-state read turns it into an entry-only replay guard;
            // an automatic or budget latch must neither downgrade it nor make
            // close/protection/flatten fail before policy evaluation.
            tracing::error!(
                %scope_key,
                %trigger_source,
                error = %error,
                "automatic kill-switch arm found an invalid persisted authority; preserving it"
            );
            Ok(())
        }
        Err(error) => Err(Status::internal(format!(
            "db latch {trigger_source} kill switch failed: {error}"
        ))),
    }
}

async fn load_account(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
) -> Result<Option<AccountSnapshot>, Status> {
    // Simulation credentials have no reconcile-maintained engine balance or
    // exchange position truth; the legacy fallback must read the same durable
    // simulation facts as the account projection instead of a stale funding
    // constant with zero positions. Unknown facts map to None: the existing
    // require_risk_budget precondition then rejects opening fail-closed, and
    // close/protection intents gain no account precondition here.
    if crate::venue::is_simulation_credential(credential_id) {
        let Some(facts) = crate::svc_v2::simulation_legacy_account_facts(tx, credential_id).await?
        else {
            return Ok(None);
        };
        return Ok(Some(AccountSnapshot {
            equity_usdt: facts.equity_usdt,
            available_margin_usdt: facts.available_margin_usdt.max(0.0),
            today_realized_pnl_pct: None,
            drawdown_pct: None,
            total_exposure_usdt: facts.total_exposure_usdt,
            credential_notional_usdt: facts.total_exposure_usdt,
        }));
    }
    let balance = sqlx::query(
        r#"
        SELECT wallet_balance, available_balance
        FROM engine_balances
        WHERE credential_id = ? AND asset = 'USDT'
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load risk balance failed: {error}")))?
    .into_iter()
    .next();

    let Some(balance) = balance else {
        return Ok(None);
    };
    let equity = parse_decimal(&balance.get::<String, _>("wallet_balance"))
        .unwrap_or(0.0)
        .max(0.0);
    let available = parse_decimal(&balance.get::<String, _>("available_balance"))
        .unwrap_or(0.0)
        .max(0.0);

    let rows = sqlx::query(
        r#"
        SELECT position_amt, mark_price
        FROM engine_positions
        WHERE credential_id = ?
        "#,
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load risk exposure failed: {error}")))?;
    let exposure = rows
        .iter()
        .map(|row| {
            parse_decimal(&row.get::<String, _>("position_amt"))
                .unwrap_or(0.0)
                .abs()
                * parse_decimal(&row.get::<String, _>("mark_price"))
                    .unwrap_or(0.0)
                    .abs()
        })
        .sum();

    Ok(Some(AccountSnapshot {
        equity_usdt: equity,
        available_margin_usdt: available,
        // The legacy account snapshot cannot supply these facts. A valid T1.5
        // risk_budget replaces them before evaluation; without one, the
        // required freshness precondition rejects an open fail-closed.
        today_realized_pnl_pct: None,
        drawdown_pct: None,
        total_exposure_usdt: exposure,
        credential_notional_usdt: exposure,
    }))
}

/// 确定性 sizing 允许使用的最大行情年龄。
///
/// AUD-P2-H7：bar 查询此前**没有任何时间谓词**，`ORDER BY ts_ms DESC LIMIT 1`
/// 会原样取回 N 小时前的收盘价，并直接进 sizing / SL 距离 / 交易所数量。
/// kline WS 断流后 bar 触发虽停，但 timer 触发与 admin 手动触发仍能以陈旧价
/// 开出市价单——engine safety contract「行情陈旧仍开仓」明列为不可协商的 High。
///
/// 取值对齐 orchestrator 侧既有的 30s quote freshness 门（那条只在 snapshot
/// 构建时刻、且走另一条 REST 通路，覆盖不到这里）。宁可拒绝也不用陈旧价定仓。
pub(crate) const MAX_ENTRY_REFERENCE_AGE_MS: i64 = 30_000;

/// Engine-owned quote provenance. This never comes from the v2 request.
/// Retaining it outside the pure policy input keeps old evaluator records
/// readable without changing the policy schema or its cost coefficients.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct EntryMarketReference {
    source: String,
    symbol_key: String,
    mark_price: Option<String>,
    as_of_ms: Option<i64>,
    evaluated_at_ms: i64,
    unavailable_reason: Option<String>,
}

impl EntryMarketReference {
    pub(crate) fn from_constraints(
        constraints: Option<&Result<crate::exchange_filters::ExchangeConstraints, String>>,
        symbol_key: &str,
        now_ms: i64,
    ) -> Self {
        let quote = constraints.and_then(|result| result.as_ref().ok());
        let mark_price = quote.map(|quote| quote.mark_price.clone());
        let as_of_ms = quote.and_then(|quote| quote.mark_price_as_of_ms);
        let unavailable_reason = if quote.is_none() {
            Some("venue_constraints_unavailable")
        } else if mark_price
            .as_deref()
            .and_then(parse_decimal)
            .filter(|p| *p > 0.0)
            .is_none()
        {
            Some("invalid_venue_mark_price")
        } else if as_of_ms.is_none() {
            Some("venue_quote_observation_time_missing")
        } else if as_of_ms.is_some_and(|at| at > now_ms) {
            Some("venue_quote_observation_in_future")
        } else if as_of_ms.is_some_and(|at| now_ms.saturating_sub(at) > MAX_ENTRY_REFERENCE_AGE_MS)
        {
            Some("venue_quote_observation_stale")
        } else {
            None
        }
        .map(str::to_string);
        Self {
            source: "binance_public_mark_price".to_string(),
            symbol_key: symbol_key.to_string(),
            mark_price,
            as_of_ms,
            evaluated_at_ms: now_ms,
            unavailable_reason,
        }
    }

    fn price(&self) -> Option<f64> {
        if self.unavailable_reason.is_some() {
            return None;
        }
        self.mark_price
            .as_deref()
            .and_then(parse_decimal)
            .filter(|price| *price > 0.0)
    }
}

async fn load_market_requested_leverage(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
) -> Result<u32, Status> {
    // Account projections are keyed by exchange symbol. Their optional local
    // position binding is not a prerequisite for a first entry.
    let identity = crate::binance_futures::usdm_identity(symbol_key)
        .ok_or_else(|| Status::invalid_argument("invalid USD-M identity for market reference"))?;
    let row = sqlx::query(
        "SELECT leverage FROM engine_positions WHERE credential_id=? AND exchange='binance' AND market_kind='futures' AND symbol=? LIMIT 1",
    ).bind(credential_id).bind(identity.symbol).fetch_optional(&mut **tx).await
        .map_err(|error| Status::internal(format!("db load market leverage failed: {error}")))?;
    // This is the existing sizing default, not a claim that venue leverage is
    // configured to 1. Venue leverage configuration is not changed here.
    Ok(row
        .and_then(|row| parse_decimal(&row.get::<String, _>("leverage")))
        .unwrap_or(1.0)
        .max(1.0) as u32)
}

/// 仅供测试：直接取 bar 侧的入场参考价，用于钉住 MAX_ENTRY_REFERENCE_AGE_MS
/// 这条时间谓词本身（AUD-P2-H7）。走与生产同一条 SQL。
#[cfg(test)]
pub(crate) async fn entry_reference_price_for_test(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    symbol_key: &str,
    now_ms: i64,
) -> Result<Option<f64>, Status> {
    let mut intent = proto::OrderIntent::default();
    intent.symbol_key = symbol_key.to_string();
    let (price, _) = load_entry_price_and_leverage(tx, credential_id, &intent, now_ms).await?;
    Ok(price)
}

async fn load_entry_price_and_leverage(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    intent: &proto::OrderIntent,
    now_ms: i64,
) -> Result<(Option<f64>, u32), Status> {
    if let Some(price) = parse_decimal(&intent.price).filter(|price| *price > 0.0) {
        return Ok((Some(price), 1));
    }
    let row = sqlx::query(
        r#"
        SELECT mark_price, leverage
        FROM engine_positions
        WHERE credential_id = ? AND symbol_key = ?
        LIMIT 1
        "#,
    )
    .bind(credential_id)
    .bind(&intent.symbol_key)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load entry reference failed: {error}")))?
    .into_iter()
    .next();
    if let Some(row) = row {
        return Ok({
            let price =
                parse_decimal(&row.get::<String, _>("mark_price")).filter(|price| *price > 0.0);
            let leverage = parse_decimal(&row.get::<String, _>("leverage"))
                .unwrap_or(1.0)
                .max(1.0) as u32;
            (price, leverage)
        });
    }
    // Market v2 intents intentionally do not overload `limit_price`. The
    // entry reference comes from the latest persisted engine market-data
    // event, which is an observed input rather than a synthesized price.
    let bar = sqlx::query(
        r#"
        SELECT payload_json FROM engine_order_events
        WHERE credential_id=? AND event_type='bar_closed' AND symbol_key=?
          AND ts_ms >= ?
        ORDER BY ts_ms DESC LIMIT 1
        "#,
    )
    .bind(credential_id)
    .bind(&intent.symbol_key)
    .bind(now_ms.saturating_sub(MAX_ENTRY_REFERENCE_AGE_MS))
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load market entry reference failed: {error}")))?
    .into_iter()
    .next();
    let price = bar.and_then(|row| {
        let payload = row.get::<Vec<u8>, _>("payload_json");
        serde_json::from_slice::<serde_json::Value>(&payload)
            .ok()
            .and_then(|value| {
                value.get("close").and_then(|close| {
                    close
                        .as_str()
                        .and_then(parse_decimal)
                        .or_else(|| close.as_f64().filter(|value| value.is_finite()))
                })
            })
            .filter(|value| *value > 0.0)
    });
    Ok((price, 1))
}

pub(crate) async fn evaluate_v1(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: DbKind,
    credential_id: &str,
    intent: &proto::OrderIntent,
    now_ms: i64,
    wire: Option<&WireRiskContext>,
    run_mode: &str,
    venue_is_testnet: bool,
    order_role: OrderRole,
    entry_market_reference: Option<&EntryMarketReference>,
) -> Result<(RiskVerdict, String, String), Status> {
    // Step 1: all persisted evaluator facts are loaded from this transaction
    // snapshot before invoking the pure function.
    let (policy, policy_hash, policy_open_guard) = load_policy(tx).await?;
    let fill_recovery_open_guard = load_fill_recovery_open_guard(
        tx,
        credential_id,
        &intent.symbol_key,
        &intent.action,
        intent.reduce_only,
        order_role,
        run_mode,
    )
    .await?;
    let positions = load_positions(tx, credential_id, &policy).await?;
    let certifications = load_promotion_certifications(tx, &intent.bot_uid).await?;
    let (risk_budget, budget_account) = load_risk_budget(tx, credential_id).await?;
    let account = if budget_account.is_some() {
        budget_account
    } else {
        load_account(tx, credential_id).await?
    };
    let mut invalid_persisted_levels = Vec::new();
    let budget_kill_switch = match load_budget_kill_switch(tx, credential_id).await? {
        BudgetKillSwitchRead::Inactive => None,
        BudgetKillSwitchRead::Active { level, reason } => Some((level, reason)),
        BudgetKillSwitchRead::Invalid(invalid) => {
            invalid_persisted_levels.push(invalid);
            None
        }
    };

    // T1.6 automatic triggers are latched in the same transaction as the next
    // verdict. They never auto-clear and can only become less restrictive via
    // the explicit reset path after a clean reconcile.
    if let Some(account) = account.as_ref() {
        let automatic = if account
            .drawdown_pct
            .is_some_and(|value| value >= policy.account.max_drawdown_pct)
        {
            Some((
                crate::kill_switch::Level::Flatten,
                "auto_drawdown",
                format!(
                    "drawdown reached policy maximum {}",
                    policy.account.max_drawdown_pct
                ),
            ))
        } else if account
            .today_realized_pnl_pct
            .is_some_and(|value| value <= -policy.account.max_daily_loss_pct)
        {
            Some((
                crate::kill_switch::Level::ManageOnly,
                "auto_daily_loss",
                format!(
                    "daily loss reached policy maximum {}",
                    policy.account.max_daily_loss_pct
                ),
            ))
        } else {
            None
        };
        if let Some((level, trigger_source, reason)) = automatic {
            arm_or_preserve_invalid(
                tx,
                db_kind,
                crate::kill_switch::ArmRequest {
                    scope_key: &format!("cred:{credential_id}"),
                    level,
                    trigger_source,
                    reason: &reason,
                    now_ms,
                },
            )
            .await?;
        }
    }
    // T1.5 reserved these columns. T1.6 consumes an active credential snapshot
    // by latching it into the authoritative state table; a later `none` budget
    // cannot auto-reset it.
    if let Some((level, reason)) = budget_kill_switch {
        arm_or_preserve_invalid(
            tx,
            db_kind,
            crate::kill_switch::ArmRequest {
                scope_key: &format!("cred:{credential_id}"),
                level,
                trigger_source: "risk_budget",
                reason: if reason.is_empty() {
                    "risk_budget kill switch"
                } else {
                    &reason
                },
                now_ms,
            },
        )
        .await?;
    }
    let effective_read = crate::kill_switch::effective_in_tx(tx, credential_id, &intent.bot_uid)
        .await
        .map_err(|error| Status::internal(format!("db load kill switch failed: {error}")))?;
    invalid_persisted_levels.extend(effective_read.invalid_levels);
    invalid_persisted_levels.sort_by(|left, right| {
        left.scope_key
            .cmp(&right.scope_key)
            .then_with(|| left.raw_level.cmp(&right.raw_level))
    });
    let invalid_persisted_kill_switch_guard =
        (!invalid_persisted_levels.is_empty()).then(|| InvalidPersistedKillSwitchGuard {
            rule: INVALID_PERSISTED_KILL_SWITCH_RULE.to_string(),
            reason_code: INVALID_PERSISTED_KILL_SWITCH_REASON_CODE.to_string(),
            detail: invalid_persisted_levels
                .iter()
                .map(|invalid| {
                    format!(
                        "scope_key={} raw_level={:?}",
                        invalid.scope_key, invalid.raw_level
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
            disposition: invalid_persisted_kill_switch_guard_disposition(
                &intent.action,
                intent.reduce_only,
                order_role,
            ),
        });
    let kill_switch = effective_read.effective.map(|state| KillSwitch {
        level: state.level.as_str().to_string(),
        scope_key: state.scope_key,
    });
    let (entry_ref_price, requested_leverage) = if let Some(reference) = entry_market_reference {
        (
            reference.price(),
            load_market_requested_leverage(tx, credential_id, &intent.symbol_key).await?,
        )
    } else {
        load_entry_price_and_leverage(tx, credential_id, intent, now_ms).await?
    };
    let reservation_row = sqlx::query(
        "SELECT reserved_usdt, cap_usdt FROM notional_reservations WHERE credential_id = ?",
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load reservation failed: {error}")))?
    .into_iter()
    .next();
    let reservation = reservation_row
        .map(|row| Reservation {
            reserved_usdt: parse_decimal(&row.get::<String, _>("reserved_usdt")).unwrap_or(0.0),
            cap_usdt: parse_decimal(&row.get::<String, _>("cap_usdt")).unwrap_or(0.0),
        })
        .unwrap_or(Reservation {
            reserved_usdt: 0.0,
            cap_usdt: policy.portfolio.max_credential_notional_usdt,
        });
    // **段③ 批 0（H-01，High · engine safety contract「对账失败后继续交易」）。**
    //
    // 开仓闸门此前只读 `degraded` 一列，而"对账差异未消解"和"熔断器跳闸"
    // 共用那一个槽位：交易所错误占住标志之后，对账差异一个字都写不进健康行，
    // 随后一次成功 ACK 把整行清零——差异未消解却恢复放行。两条事实现在各有
    // 自己的列（迁移 `0029`），闸门读它们的**或**。
    //
    // `COALESCE(reconcile_diff_open, 1)`：0029 之前的存量行本列是 `NULL`，
    // 语义是"从未被写过"。被掩盖的差异**不可从库里恢复**，所以"证不出没有
    // 差异"在开仓这个判据上等价于"有差异"（engine safety contract）——宁可多拦
    // 一次开仓，等下一轮对账用真值把它收敛成 0。启动期的
    // `revalidate_legacy_credential_reconcile_diff` 会把 `NULL` 显式收敛掉，
    // 这里的 `COALESCE` 是第二层：读侧不依赖那一步真的跑过。
    let degraded = sqlx::query(
        "SELECT degraded, COALESCE(reconcile_diff_open, 1) AS reconcile_diff_open \
         FROM engine_credential_health WHERE credential_id = ? LIMIT 1",
    )
    .bind(credential_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db load credential health failed: {error}")))?
    .into_iter()
    .next()
    .map(|row| row.get::<i64, _>("degraded") != 0 || row.get::<i64, _>("reconcile_diff_open") != 0)
    .unwrap_or(false);
    let credential_degrade_guard = degraded.then(|| CredentialDegradeGuard {
        rule: CREDENTIAL_DEGRADE_GUARD_RULE.to_string(),
        reason_code: "DEGRADED_CREDENTIAL".to_string(),
        detail: format!("credential={credential_id} mode=manage_only"),
        disposition: credential_degrade_guard_disposition(
            &intent.action,
            intent.reduce_only,
            order_role,
        ),
    });

    // Step 2: pure, zero-I/O evaluation. Frozen v1 has no typed protection, so
    // it passes those fields as absent. Its caller quantity is explicit
    // compatibility input for caps and reservation only; it never enters the
    // risk-budget sizing formula.
    let typed_protection_capable = wire
        .map(|context| context.typed_protection_capable)
        .unwrap_or(false);
    // Frozen v1 cannot carry a DataSnapshot decision. Keep the exemption
    // explicit until the v1 production path is retired; v2 always marks the
    // capability true, so a missing/invalid snapshot fails closed there.
    let data_snapshot_capable = wire
        .map(|context| context.data_snapshot_capable)
        .unwrap_or(false);
    let legacy_sizing = if !typed_protection_capable
        && matches!(intent.action.as_str(), "open_long" | "open_short")
    {
        parse_decimal(&intent.quantity)
            .filter(|quantity| *quantity > 0.0)
            .map(|quantity| LegacySizingInput {
                quantity,
                reason: "risk-budget sizing unavailable: v1 wire has no typed protection fields"
                    .to_string(),
            })
    } else {
        None
    };
    let reservation_cap = format_decimal(policy.portfolio.max_credential_notional_usdt);
    let replay_input = RiskReplayInput {
        entry_market_reference: entry_market_reference.cloned(),
        input: Input {
            policy,
            policy_hash,
            intent: Intent {
                intent_id: wire
                    .map(|context| context.intent_id.clone())
                    .unwrap_or_else(|| format!("v1:{}", intent.idempotency_key)),
                bot_uid: intent.bot_uid.clone(),
                symbol_key: intent.symbol_key.clone(),
                action: action(&intent.action),
                reduce_only: intent.reduce_only,
                entry_ref_price,
                stop_loss_price: wire
                    .and_then(|context| context.protection.as_ref())
                    .and_then(|protection| parse_decimal(&protection.stop_loss_price)),
                take_profit_price: wire
                    .and_then(|context| context.protection.as_ref())
                    .and_then(|protection| parse_decimal(&protection.take_profit_price)),
                conviction: None,
                valid_until_ms: wire.and_then(|context| context.valid_until_ms),
                requested_leverage,
                max_slippage_bps: wire.and_then(|context| context.max_slippage_bps),
                observed_slippage_bps: None,
            },
            now_ms,
            data_snapshot: wire.and_then(|context| context.data_snapshot.clone()),
            data_snapshot_capable,
            stage_certification: Some(StageCertificationInput {
                requested_stage: run_mode.to_string(),
                config_hash: wire
                    .map(|context| context.config_hash.clone())
                    .unwrap_or_default(),
                prompt_hash: wire
                    .map(|context| context.prompt_hash.clone())
                    .unwrap_or_default(),
                policy_version: wire
                    .map(|context| context.policy_version.clone())
                    .unwrap_or_default(),
                code_commit: wire
                    .map(|context| context.code_commit.clone())
                    .unwrap_or_default(),
                venue_is_testnet,
                certifications,
            }),
            risk_budget,
            kill_switch,
            account,
            positions,
            exchange_filters: None,
            reservation: Some(reservation),
            // The wrapper guard below owns the order-role-aware manage_only
            // decision. Do not also feed the boolean-only policy rule, or the
            // two guards could mask a one-sided mutation.
            credential_degraded: false,
            context: None,
            churn: None,
            typed_protection_capable,
            legacy_sizing,
        },
        policy_open_guard,
        invalid_persisted_kill_switch_guard,
        credential_degrade_guard,
        fill_recovery_open_guard,
        post_evaluation_rule: None,
        post_evaluation_reject: None,
    };
    let verdict = evaluate_replay(&replay_input);
    let replay_input_json = serde_json::to_string(&replay_input).map_err(|error| {
        Status::internal(format!("serialize risk replay input failed: {error}"))
    })?;
    Ok((verdict, reservation_cap, replay_input_json))
}

fn evaluate_replay(replay: &RiskReplayInput) -> RiskVerdict {
    // The pure policy's `None` means "no authority input was supplied".  An
    // invalid durable row is different: the wrapper below owns that fact and
    // applies it by order role.  Give every opening-shaped intent with no valid
    // sibling a temporary neutral value, then remove that synthetic rule before
    // appending the explicit integrity rule.  This includes reduce-only,
    // protection, and emergency roles: their guard result is skipped rather
    // than allowing the pure evaluator's missing-input rule to reject them.
    // Close and hold retain the pure policy's standard not-applicable rule.  The
    // serialized replay input keeps the real shape (`kill_switch=None` + guard),
    // so no invalid row is normalized into durable or replay authority.
    let substitute_invalid_for_missing = replay
        .invalid_persisted_kill_switch_guard
        .as_ref()
        .is_some()
        && replay.input.kill_switch.is_none()
        && replay.input.intent.action.is_open();
    let mut policy_input = replay.input.clone();
    if substitute_invalid_for_missing {
        policy_input.kill_switch = Some(KillSwitch {
            level: "none".to_string(),
            scope_key: "invalid-persisted-authority-placeholder".to_string(),
        });
    }
    let mut verdict = risk_policy::evaluate(&policy_input);
    if substitute_invalid_for_missing {
        verdict
            .rules
            .retain(|rule| rule.rule != "kill_switch_level");
    }
    if let Some(reference) = replay
        .entry_market_reference
        .as_ref()
        .filter(|_| replay.input.intent.action.is_open() && !replay.input.intent.reduce_only)
    {
        if let Some(reason) = reference.unavailable_reason.as_deref() {
            verdict = rejected_verdict(
                verdict,
                "entry_market_reference",
                "EXCHANGE_FILTERS_UNAVAILABLE",
                reason,
            );
        }
    }
    if let Some(guard) = replay.policy_open_guard.as_ref() {
        if replay.input.intent.action.is_open() {
            verdict = rejected_verdict(verdict, &guard.rule, &guard.reason_code, &guard.detail);
        } else {
            verdict.rules.push(RuleResult {
                rule: guard.rule.clone(),
                result: RuleOutcome::Skipped,
                reason_code: None,
                reason: Some("not_applicable_to_close_or_hold".to_string()),
                detail: Some(guard.detail.clone()),
            });
        }
    }
    if let Some(guard) = replay.credential_degrade_guard.as_ref() {
        if guard.disposition == CredentialDegradeGuardDisposition::RejectEntry {
            verdict = rejected_verdict(verdict, &guard.rule, &guard.reason_code, &guard.detail);
        } else {
            verdict.rules.push(RuleResult {
                rule: guard.rule.clone(),
                result: RuleOutcome::Skipped,
                reason_code: None,
                reason: Some(
                    match guard.disposition {
                        CredentialDegradeGuardDisposition::RejectEntry => {
                            unreachable!("handled above")
                        }
                        CredentialDegradeGuardDisposition::SkipClose => {
                            "close_must_remain_available"
                        }
                        CredentialDegradeGuardDisposition::SkipHold => {
                            "hold_has_no_exchange_exposure"
                        }
                        CredentialDegradeGuardDisposition::SkipReduceOnly => {
                            "reduce_only_must_remain_available"
                        }
                        CredentialDegradeGuardDisposition::SkipProtection => {
                            "protection_must_remain_available"
                        }
                        CredentialDegradeGuardDisposition::SkipL3Flatten => {
                            "l3_flatten_must_remain_available"
                        }
                    }
                    .to_string(),
                ),
                detail: Some(guard.detail.clone()),
            });
        }
    }
    if let Some(guard) = replay.fill_recovery_open_guard.as_ref() {
        if guard.disposition == FillRecoveryGuardDisposition::RejectEntry {
            verdict = rejected_verdict(verdict, &guard.rule, &guard.reason_code, &guard.detail);
        } else {
            verdict.rules.push(RuleResult {
                rule: guard.rule.clone(),
                result: RuleOutcome::Skipped,
                reason_code: None,
                reason: Some(
                    match guard.disposition {
                        FillRecoveryGuardDisposition::RejectEntry => unreachable!("handled above"),
                        FillRecoveryGuardDisposition::SkipClose => "close_must_remain_available",
                        FillRecoveryGuardDisposition::SkipHold => "hold_has_no_exchange_exposure",
                        FillRecoveryGuardDisposition::SkipReduceOnly => {
                            "reduce_only_must_remain_available"
                        }
                        FillRecoveryGuardDisposition::SkipProtection => {
                            "protection_must_remain_available"
                        }
                        FillRecoveryGuardDisposition::SkipL3Flatten => {
                            "l3_flatten_must_remain_available"
                        }
                    }
                    .to_string(),
                ),
                detail: Some(guard.detail.clone()),
            });
        }
    }
    if let Some(guard) = replay.invalid_persisted_kill_switch_guard.as_ref() {
        if guard.disposition == InvalidPersistedKillSwitchGuardDisposition::RejectEntry {
            verdict = rejected_verdict(verdict, &guard.rule, &guard.reason_code, &guard.detail);
        } else {
            verdict.rules.push(RuleResult {
                rule: guard.rule.clone(),
                result: RuleOutcome::Skipped,
                reason_code: None,
                reason: Some(
                    match guard.disposition {
                        InvalidPersistedKillSwitchGuardDisposition::RejectEntry => {
                            unreachable!("handled above")
                        }
                        InvalidPersistedKillSwitchGuardDisposition::SkipClose => {
                            "close_must_remain_available"
                        }
                        InvalidPersistedKillSwitchGuardDisposition::SkipHold => {
                            "hold_has_no_exchange_exposure"
                        }
                        InvalidPersistedKillSwitchGuardDisposition::SkipReduceOnly => {
                            "reduce_only_must_remain_available"
                        }
                        InvalidPersistedKillSwitchGuardDisposition::SkipProtection => {
                            "protection_must_remain_available"
                        }
                        InvalidPersistedKillSwitchGuardDisposition::SkipL3Flatten => {
                            "l3_flatten_must_remain_available"
                        }
                    }
                    .to_string(),
                ),
                detail: Some(guard.detail.clone()),
            });
        }
    }
    if let Some(rule) = replay.post_evaluation_rule.as_ref() {
        if matches!(rule.outcome, ReplayRuleOutcome::Reject) {
            verdict = rejected_verdict(
                verdict,
                &rule.rule,
                rule.reason_code
                    .as_deref()
                    .unwrap_or("EXCHANGE_FILTER_REJECT"),
                &rule.detail,
            );
        } else {
            let result = match rule.outcome {
                ReplayRuleOutcome::Pass => RuleOutcome::Pass,
                ReplayRuleOutcome::Capped => RuleOutcome::Capped,
                ReplayRuleOutcome::Skipped => RuleOutcome::Skipped,
                ReplayRuleOutcome::Reject => unreachable!("handled above"),
            };
            if result == RuleOutcome::Capped && verdict.decision == Decision::Approve {
                verdict.decision = Decision::Resize;
            }
            verdict.rules.push(RuleResult {
                rule: rule.rule.clone(),
                result,
                reason_code: rule.reason_code.clone(),
                reason: rule.reason.clone(),
                detail: Some(rule.detail.clone()),
            });
            let amendment_seed = format!(
                "{}|{}|{:?}|{}",
                verdict.risk_verdict_id, rule.rule, rule.outcome, rule.detail
            );
            verdict.risk_verdict_id = format!("rv_{}", &sha256_hex(&amendment_seed)[..26]);
        }
    }
    if let Some(reject) = replay.post_evaluation_reject.as_ref() {
        verdict = rejected_verdict(verdict, &reject.rule, &reject.reason_code, &reject.detail);
    }
    verdict
}

pub(crate) fn replay_exchange_filter_result(
    replay_input_json: &str,
    result: &crate::exchange_filters::PreparedOrder,
) -> Result<(RiskVerdict, String), Status> {
    let mut replay = serde_json::from_str::<RiskReplayInput>(replay_input_json)
        .map_err(|error| Status::internal(format!("decode risk replay input failed: {error}")))?;
    let outcome = match result.outcome {
        crate::exchange_filters::FilterAuditOutcome::Pass => ReplayRuleOutcome::Pass,
        crate::exchange_filters::FilterAuditOutcome::Capped => ReplayRuleOutcome::Capped,
        crate::exchange_filters::FilterAuditOutcome::Skipped => ReplayRuleOutcome::Skipped,
        crate::exchange_filters::FilterAuditOutcome::Reject => ReplayRuleOutcome::Reject,
    };
    replay.post_evaluation_rule = Some(ReplayRule {
        rule: "exchange_filters".to_string(),
        outcome,
        reason_code: result.reason_code.map(str::to_string),
        reason: result.reason.map(str::to_string),
        detail: result.detail.clone(),
    });
    let replay_input_json = serde_json::to_string(&replay).map_err(|error| {
        Status::internal(format!("serialize risk replay input failed: {error}"))
    })?;
    let verdict = replay_verdict(&replay_input_json)?;
    Ok((verdict, replay_input_json))
}

pub(crate) fn replay_verdict(replay_input_json: &str) -> Result<RiskVerdict, Status> {
    let replay = serde_json::from_str::<RiskReplayInput>(replay_input_json)
        .map_err(|error| Status::internal(format!("decode risk replay input failed: {error}")))?;
    Ok(evaluate_replay(&replay))
}

pub(crate) fn replay_rejection(
    replay_input_json: &str,
    rule: &str,
    reason_code: &str,
    detail: &str,
) -> Result<(RiskVerdict, String), Status> {
    let mut replay = serde_json::from_str::<RiskReplayInput>(replay_input_json)
        .map_err(|error| Status::internal(format!("decode risk replay input failed: {error}")))?;
    replay.post_evaluation_reject = Some(ReplayReject {
        rule: rule.to_string(),
        reason_code: reason_code.to_string(),
        detail: detail.to_string(),
    });
    let replay_input_json = serde_json::to_string(&replay).map_err(|error| {
        Status::internal(format!("serialize risk replay input failed: {error}"))
    })?;
    let verdict = replay_verdict(&replay_input_json)?;
    Ok((verdict, replay_input_json))
}

fn format_decimal(value: f64) -> String {
    let mut text = format!("{:.12}", value.max(0.0));
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

pub(crate) async fn insert_verdict(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    intent: &proto::OrderIntent,
    verdict: &RiskVerdict,
    replay_input_json: &str,
) -> Result<(), Status> {
    let approved_intent_key = if verdict.decision == Decision::Reject {
        None
    } else {
        Some(verdict.intent_id.as_str())
    };
    sqlx::query(
        r#"
        INSERT INTO risk_verdicts (
          risk_verdict_id, intent_id, approved_intent_key, credential_id,
          bot_uid, symbol_key, created_at_ms, policy_version, policy_hash,
          decision, reject_reason_code, sizing_json, rules_json,
          budget_as_of_ms, evaluator_version, replay_input_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&verdict.risk_verdict_id)
    .bind(&verdict.intent_id)
    .bind(approved_intent_key)
    .bind(credential_id)
    .bind(&intent.bot_uid)
    .bind(&intent.symbol_key)
    .bind(verdict.created_at_ms)
    .bind(&verdict.policy_version)
    .bind(&verdict.policy_hash)
    .bind(decision(&verdict.decision))
    .bind(verdict.reject_reason_code.as_deref())
    .bind(verdict.sizing_json())
    .bind(verdict.rules_json())
    .bind(verdict.budget_as_of_ms)
    .bind(&verdict.evaluator_version)
    .bind(replay_input_json)
    .execute(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db insert risk verdict failed: {error}")))?;
    Ok(())
}

pub(crate) fn rejected_verdict(
    mut verdict: RiskVerdict,
    rule: &str,
    code: &str,
    detail: &str,
) -> RiskVerdict {
    let rejection_seed = format!("{}|{}|{}|{}", verdict.risk_verdict_id, rule, code, detail);
    verdict.risk_verdict_id = format!("rv_{}", &sha256_hex(&rejection_seed)[..26]);
    verdict.decision = Decision::Reject;
    verdict.reject_reason_code = Some(code.to_string());
    verdict.rules.push(RuleResult {
        rule: rule.to_string(),
        result: RuleOutcome::Reject,
        reason_code: Some(code.to_string()),
        reason: None,
        detail: Some(detail.to_string()),
    });
    verdict
}

pub(crate) async fn reserve_notional(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    cap_usdt: &str,
    notional_usdt: &str,
    now_ms: i64,
) -> Result<bool, Status> {
    let exists = sqlx::query("SELECT 1 FROM notional_reservations WHERE credential_id = ? LIMIT 1")
        .bind(credential_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| Status::internal(format!("db check reservation failed: {error}")))?
        .into_iter()
        .next()
        .is_some();
    if !exists {
        let insert = sqlx::query(
            r#"
            INSERT INTO notional_reservations (
              credential_id, reserved_usdt, cap_usdt, updated_at_ms
            ) VALUES (?, '0', ?, ?)
            "#,
        )
        .bind(credential_id)
        .bind(cap_usdt)
        .bind(now_ms)
        .execute(&mut **tx)
        .await;
        if let Err(error) = insert {
            if !crate::position_state::is_unique_violation(&error) {
                return Err(Status::internal(format!(
                    "db initialize reservation failed: {error}"
                )));
            }
        }
    }

    let updated = sqlx::query(
        r#"
        UPDATE notional_reservations
        SET reserved_usdt = CAST(reserved_usdt AS DECIMAL(36, 18))
                            + CAST(? AS DECIMAL(36, 18)),
            updated_at_ms = ?
        WHERE credential_id = ?
          AND CAST(reserved_usdt AS DECIMAL(36, 18))
              + CAST(? AS DECIMAL(36, 18))
              <= CAST(cap_usdt AS DECIMAL(36, 18))
        "#,
    )
    .bind(notional_usdt)
    .bind(now_ms)
    .bind(credential_id)
    .bind(notional_usdt)
    .execute(&mut **tx)
    .await
    .map_err(|error| Status::internal(format!("db reserve notional failed: {error}")))?;
    Ok(updated.rows_affected() == 1)
}

pub(crate) async fn release_notional(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    credential_id: &str,
    notional_usdt: &str,
    now_ms: i64,
) -> Result<(), Status> {
    let updated = sqlx::query(
        r#"
        UPDATE notional_reservations
        SET reserved_usdt = CAST(reserved_usdt AS DECIMAL(36, 18))
                            - CAST(? AS DECIMAL(36, 18)),
            updated_at_ms = ?
        WHERE credential_id = ?
          AND CAST(reserved_usdt AS DECIMAL(36, 18))
              >= CAST(? AS DECIMAL(36, 18))
        "#,
    )
    .bind(notional_usdt)
    .bind(now_ms)
    .bind(credential_id)
    .bind(notional_usdt)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Status::internal(format!("db release notional reservation failed: {error}"))
    })?;
    if updated.rows_affected() != 1 {
        return Err(Status::failed_precondition(
            "notional reservation release did not match durable reserved amount",
        ));
    }
    Ok(())
}

/// Lock the durable reservation before a cleanup path locks its position and
/// engine order. This mirrors Entry creation's reservation -> position -> order
/// sequence and prevents a retry from deadlocking against claim retirement.
pub(crate) async fn lock_notional_reservation(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    db_kind: crate::store::DbKind,
    credential_id: &str,
) -> Result<(), Status> {
    let result = match db_kind {
        crate::store::DbKind::MySql => sqlx::query(
            "SELECT credential_id FROM notional_reservations \
             WHERE credential_id=? FOR UPDATE",
        )
        .bind(credential_id)
        .fetch_one(&mut **tx)
        .await
        .map(|_| ()),
        crate::store::DbKind::Sqlite => sqlx::query(
            "UPDATE notional_reservations SET updated_at_ms=updated_at_ms \
             WHERE credential_id=?",
        )
        .bind(credential_id)
        .execute(&mut **tx)
        .await
        .and_then(|result| {
            (result.rows_affected() == 1)
                .then_some(())
                .ok_or(sqlx::Error::RowNotFound)
        }),
        crate::store::DbKind::Unknown => {
            return Err(Status::internal(
                "notional reservation lock requires a known database dialect",
            ));
        }
    };
    match result {
        Ok(()) => Ok(()),
        Err(sqlx::Error::RowNotFound) => Err(Status::failed_precondition(
            "pending Entry has no durable notional reservation",
        )),
        Err(error) => Err(Status::internal(format!(
            "db lock notional reservation failed: {error}"
        ))),
    }
}

pub(crate) async fn insert_engine_order(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    engine_order_id: &str,
    credential_id: &str,
    intent: &proto::OrderIntent,
    authority_idem_key: &str,
    position_id: Option<&str>,
    order_role: &str,
    risk_verdict_id: Option<&str>,
    status: &str,
    now_ms: i64,
) -> Result<bool, Status> {
    let risk_verdict_id = risk_verdict_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Status::failed_precondition("engine order requires risk verdict"))?;
    let insert = sqlx::query(
        r#"
        INSERT INTO engine_orders (
          engine_order_id, credential_id, bot_uid, run_id, cycle_id,
          idempotency_key, client_order_id_short, exchange_order_id,
          exchange, market_kind, symbol_key, action, side, order_type,
          price, quantity, reduce_only, status, risk_verdict_id,
          position_id, order_role,
          created_at_ms, updated_at_ms
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        "#,
    )
    .bind(engine_order_id)
    .bind(credential_id)
    .bind(&intent.bot_uid)
    .bind(&intent.run_id)
    .bind(&intent.cycle_id)
    .bind(authority_idem_key)
    .bind(&intent.client_order_id_short)
    .bind(&intent.exchange)
    .bind(&intent.market_kind)
    .bind(&intent.symbol_key)
    .bind(&intent.action)
    .bind(&intent.side)
    .bind(&intent.order_type)
    .bind(if intent.price.is_empty() {
        None
    } else {
        Some(intent.price.as_str())
    })
    .bind(&intent.quantity)
    .bind(if intent.reduce_only { 1i64 } else { 0i64 })
    .bind(status)
    .bind(risk_verdict_id)
    .bind(position_id)
    .bind(order_role)
    .bind(now_ms)
    .bind(now_ms)
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(true),
        Err(error) if crate::position_state::is_unique_violation(&error) => Ok(false),
        Err(error) => Err(Status::internal(format!(
            "db insert engine order failed: {error}"
        ))),
    }
}

#[cfg(test)]
mod credential_degrade_guard_tests {
    use super::{credential_degrade_guard_disposition, CredentialDegradeGuardDisposition};
    use crate::protection::OrderRole;

    #[test]
    fn t310_manage_only_rejects_entry() {
        assert_eq!(
            credential_degrade_guard_disposition("open_long", false, OrderRole::Entry),
            CredentialDegradeGuardDisposition::RejectEntry
        );
    }

    #[test]
    fn t310_manage_only_skips_close_independently() {
        assert_eq!(
            credential_degrade_guard_disposition("close_long", false, OrderRole::Exit),
            CredentialDegradeGuardDisposition::SkipClose
        );
    }

    #[test]
    fn t310_manage_only_skips_hold_independently() {
        assert_eq!(
            credential_degrade_guard_disposition("hold", false, OrderRole::Entry),
            CredentialDegradeGuardDisposition::SkipHold
        );
    }

    #[test]
    fn t310_manage_only_skips_reduce_only_independently() {
        assert_eq!(
            credential_degrade_guard_disposition("open_long", true, OrderRole::Entry),
            CredentialDegradeGuardDisposition::SkipReduceOnly
        );
    }

    #[test]
    fn t310_manage_only_skips_protection_independently() {
        assert_eq!(
            credential_degrade_guard_disposition("open_long", false, OrderRole::ProtectionSl),
            CredentialDegradeGuardDisposition::SkipProtection
        );
    }

    #[test]
    fn t310_manage_only_skips_l3_flatten_independently() {
        assert_eq!(
            credential_degrade_guard_disposition("open_long", false, OrderRole::Emergency),
            CredentialDegradeGuardDisposition::SkipL3Flatten
        );
    }
}

#[cfg(test)]
mod invalid_persisted_kill_switch_guard_tests {
    use std::sync::Once;

    use risk_policy::{
        AccountSnapshot, Action, DataSnapshot, Decision, ExchangeFilters, Input, Intent,
        Reservation, RiskBudget, RuleOutcome, StageCertificationInput,
    };

    use super::{
        evaluate_replay, invalid_persisted_kill_switch_guard_disposition, load_budget_kill_switch,
        BudgetKillSwitchRead, InvalidPersistedKillSwitchGuard,
        InvalidPersistedKillSwitchGuardDisposition, RiskReplayInput,
        INVALID_PERSISTED_KILL_SWITCH_REASON_CODE, INVALID_PERSISTED_KILL_SWITCH_RULE,
    };
    use crate::protection::OrderRole;
    use crate::store::Store;

    static INSTALL_DRIVERS: Once = Once::new();
    const SYMBOL: &str = "binance|futures|BTC|USDT|USDT";

    fn base_input() -> Input {
        let policy = risk_policy::factory_policy();
        Input {
            policy_hash: risk_policy::policy_hash(&policy),
            policy,
            intent: Intent {
                intent_id: "int_invalid_persisted_kill_switch".to_string(),
                bot_uid: "bot_invalid_persisted_kill_switch".to_string(),
                symbol_key: SYMBOL.to_string(),
                action: Action::OpenLong,
                reduce_only: false,
                entry_ref_price: Some(100.0),
                stop_loss_price: Some(95.0),
                take_profit_price: Some(111.0),
                conviction: Some(0.90),
                valid_until_ms: Some(2_000_000),
                requested_leverage: 3,
                max_slippage_bps: Some(15),
                observed_slippage_bps: Some(5),
            },
            now_ms: 1_000_000,
            data_snapshot: Some(DataSnapshot {
                allow_open: true,
                consistency_error: None,
            }),
            data_snapshot_capable: true,
            stage_certification: Some(StageCertificationInput {
                requested_stage: "paper".to_string(),
                config_hash: "c".repeat(64),
                prompt_hash: format!("sha256:{}", "p".repeat(64)),
                policy_version: "risk_policy_v3".to_string(),
                code_commit: "a".repeat(40),
                venue_is_testnet: false,
                certifications: vec![],
            }),
            risk_budget: Some(RiskBudget { as_of_ms: 990_000 }),
            kill_switch: None,
            account: Some(AccountSnapshot {
                equity_usdt: 10_000.0,
                available_margin_usdt: 5_000.0,
                today_realized_pnl_pct: Some(0.0),
                drawdown_pct: Some(0.0),
                total_exposure_usdt: 0.0,
                credential_notional_usdt: 0.0,
            }),
            positions: vec![],
            exchange_filters: Some(ExchangeFilters {
                lot_step: 0.001,
                min_notional_usdt: 5.0,
            }),
            reservation: Some(Reservation {
                reserved_usdt: 0.0,
                cap_usdt: 5_000.0,
            }),
            credential_degraded: false,
            context: None,
            churn: None,
            typed_protection_capable: true,
            legacy_sizing: None,
        }
    }

    fn replay_for(action: Action, reduce_only: bool, order_role: OrderRole) -> RiskReplayInput {
        let mut input = base_input();
        input.intent.action = action;
        input.intent.reduce_only = reduce_only;
        let action_text = match action {
            Action::OpenLong => "open_long",
            Action::OpenShort => "open_short",
            Action::CloseLong => "close_long",
            Action::CloseShort => "close_short",
            Action::Hold => "hold",
        };
        RiskReplayInput {
            input,
            entry_market_reference: None,
            policy_open_guard: None,
            invalid_persisted_kill_switch_guard: Some(InvalidPersistedKillSwitchGuard {
                rule: INVALID_PERSISTED_KILL_SWITCH_RULE.to_string(),
                reason_code: INVALID_PERSISTED_KILL_SWITCH_REASON_CODE.to_string(),
                detail: "scope_key=global raw_level=\" FLATTEN \"".to_string(),
                disposition: invalid_persisted_kill_switch_guard_disposition(
                    action_text,
                    reduce_only,
                    order_role,
                ),
            }),
            credential_degrade_guard: None,
            fill_recovery_open_guard: None,
            post_evaluation_rule: None,
            post_evaluation_reject: None,
        }
    }

    #[test]
    fn dirty_authority_disposition_is_role_aware() {
        let cases = [
            (
                "open_long",
                false,
                OrderRole::Entry,
                InvalidPersistedKillSwitchGuardDisposition::RejectEntry,
            ),
            (
                "close_long",
                false,
                OrderRole::Exit,
                InvalidPersistedKillSwitchGuardDisposition::SkipClose,
            ),
            (
                "hold",
                false,
                OrderRole::Exit,
                InvalidPersistedKillSwitchGuardDisposition::SkipHold,
            ),
            (
                "open_long",
                true,
                OrderRole::Entry,
                InvalidPersistedKillSwitchGuardDisposition::SkipReduceOnly,
            ),
            (
                "open_long",
                false,
                OrderRole::ProtectionSl,
                InvalidPersistedKillSwitchGuardDisposition::SkipProtection,
            ),
            (
                "open_long",
                false,
                OrderRole::Emergency,
                InvalidPersistedKillSwitchGuardDisposition::SkipL3Flatten,
            ),
        ];
        for (action, reduce_only, order_role, expected) in cases {
            assert_eq!(
                invalid_persisted_kill_switch_guard_disposition(action, reduce_only, order_role),
                expected,
                "action={action} reduce_only={reduce_only} order_role={order_role:?}"
            );
        }
    }

    #[test]
    fn dirty_authority_rejects_entry_with_fixed_reason_code() {
        let verdict = evaluate_replay(&replay_for(Action::OpenLong, false, OrderRole::Entry));
        assert_eq!(verdict.decision, Decision::Reject);
        assert_eq!(
            verdict.reject_reason_code.as_deref(),
            Some(INVALID_PERSISTED_KILL_SWITCH_REASON_CODE)
        );
        let guard = verdict
            .rules
            .iter()
            .find(|rule| rule.rule == INVALID_PERSISTED_KILL_SWITCH_RULE)
            .expect("persisted authority guard result");
        assert_eq!(guard.result, RuleOutcome::Reject);
        assert_eq!(
            guard.reason_code.as_deref(),
            Some(INVALID_PERSISTED_KILL_SWITCH_REASON_CODE)
        );
        assert!(
            verdict
                .rules
                .iter()
                .all(|rule| rule.rule != "kill_switch_level"),
            "a synthetic neutral input must not leak into the replay audit"
        );
    }

    #[test]
    fn dirty_authority_remains_the_primary_wrapper_guard_after_a_policy_reject() {
        let mut replay = replay_for(Action::OpenLong, false, OrderRole::Entry);
        replay.input.risk_budget = Some(RiskBudget { as_of_ms: 0 });

        let verdict = evaluate_replay(&replay);
        let first_reject = verdict
            .rules
            .iter()
            .find(|rule| rule.result == RuleOutcome::Reject)
            .expect("at least one rejecting rule");
        assert_eq!(first_reject.rule, "risk_budget_freshness");
        assert_eq!(
            first_reject.reason_code.as_deref(),
            Some("STALE_RISK_BUDGET")
        );
        assert_eq!(
            verdict.reject_reason_code.as_deref(),
            Some(INVALID_PERSISTED_KILL_SWITCH_REASON_CODE),
            "the persisted-authority guard must retain the existing wrapper precedence"
        );

        let integrity_reject = verdict
            .rules
            .iter()
            .find(|rule| rule.rule == INVALID_PERSISTED_KILL_SWITCH_RULE)
            .expect("persisted authority guard result");
        assert_eq!(integrity_reject.result, RuleOutcome::Reject);
        assert_eq!(
            integrity_reject.reason_code.as_deref(),
            Some(INVALID_PERSISTED_KILL_SWITCH_REASON_CODE)
        );
    }

    #[test]
    fn dirty_authority_keeps_non_entry_roles_available() {
        let cases = [
            (
                Action::CloseLong,
                false,
                OrderRole::Exit,
                "close_must_remain_available",
            ),
            (
                Action::Hold,
                false,
                OrderRole::Exit,
                "hold_has_no_exchange_exposure",
            ),
            (
                Action::CloseLong,
                true,
                OrderRole::Exit,
                "reduce_only_must_remain_available",
            ),
            (
                Action::CloseLong,
                true,
                OrderRole::ProtectionTp,
                "protection_must_remain_available",
            ),
            (
                Action::CloseLong,
                true,
                OrderRole::Emergency,
                "l3_flatten_must_remain_available",
            ),
        ];
        for (action, reduce_only, order_role, reason) in cases {
            let verdict = evaluate_replay(&replay_for(action, reduce_only, order_role));
            assert_ne!(
                verdict.decision,
                Decision::Reject,
                "action={action:?} reduce_only={reduce_only} order_role={order_role:?}: {}",
                verdict.rules_json()
            );
            let guard = verdict
                .rules
                .iter()
                .find(|rule| rule.rule == INVALID_PERSISTED_KILL_SWITCH_RULE)
                .expect("persisted authority guard result");
            assert_eq!(guard.result, RuleOutcome::Skipped);
            assert_eq!(guard.reason.as_deref(), Some(reason));

            let kill_switch_rule = verdict
                .rules
                .iter()
                .find(|rule| rule.rule == "kill_switch_level")
                .expect("canonical missing-authority rule must remain in the audit");
            assert_eq!(kill_switch_rule.result, RuleOutcome::Skipped);
            assert_eq!(
                kill_switch_rule.reason.as_deref(),
                Some("not_applicable_to_close_or_hold")
            );
        }
    }

    #[test]
    fn dirty_authority_keeps_open_shaped_safety_roles_available() {
        let cases = [
            (
                Action::OpenLong,
                true,
                OrderRole::Entry,
                "reduce_only_must_remain_available",
            ),
            (
                Action::OpenLong,
                false,
                OrderRole::ProtectionSl,
                "protection_must_remain_available",
            ),
            (
                Action::OpenLong,
                false,
                OrderRole::ProtectionTp,
                "protection_must_remain_available",
            ),
            (
                Action::OpenLong,
                false,
                OrderRole::Emergency,
                "l3_flatten_must_remain_available",
            ),
        ];
        for (action, reduce_only, order_role, reason) in cases {
            let replay = replay_for(action, reduce_only, order_role);
            let verdict = evaluate_replay(&replay);
            assert_ne!(
                verdict.decision,
                Decision::Reject,
                "action={action:?} reduce_only={reduce_only} order_role={order_role:?}: {}",
                verdict.rules_json()
            );
            let guard = verdict
                .rules
                .iter()
                .find(|rule| rule.rule == INVALID_PERSISTED_KILL_SWITCH_RULE)
                .expect("persisted authority guard result");
            assert_eq!(guard.result, RuleOutcome::Skipped);
            assert_eq!(guard.reason.as_deref(), Some(reason));
            assert!(
                verdict
                    .rules
                    .iter()
                    .all(|rule| rule.rule != "kill_switch_level"),
                "the temporary neutral authority must not leak into the audit"
            );

            let replay_json = serde_json::to_value(&replay).expect("serialize replay input");
            assert!(
                replay_json["input"]["kill_switch"].is_null(),
                "the replay must retain the missing valid authority"
            );
        }
    }

    #[tokio::test]
    async fn budget_loader_preserves_noncanonical_level_as_invalid() {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path = std::env::temp_dir().join(format!(
            "rustcore-invalid-budget-kill-switch-{}.db",
            ulid::Ulid::new()
        ));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect sqlite test store and run migrations");
        sqlx::query(
            r#"
            INSERT INTO risk_budget (
              credential_id, as_of_ms, equity_usdt, available_usdt,
              today_realized_pnl_usdt, today_realized_pnl_pct,
              drawdown_pct, total_exposure_pct, open_positions_count,
              group_exposure_json, kill_switch_level, kill_switch_scope,
              kill_switch_reason, remaining_credential_notional_usdt
            ) VALUES (
              'cred_dirty_budget', 1, '10000', '10000', '0', 0,
              0, 0, 0, '{}', 'none ', 'credential', 'fixture', '5000'
            )
            "#,
        )
        .execute(store.pool())
        .await
        .expect("insert dirty budget authority");

        let mut tx = store.pool().begin().await.expect("begin loader test");
        match load_budget_kill_switch(&mut tx, "cred_dirty_budget")
            .await
            .expect("dirty level is data, not a loader failure")
        {
            BudgetKillSwitchRead::Invalid(invalid) => {
                assert_eq!(invalid.scope_key, "risk_budget:cred:cred_dirty_budget");
                assert_eq!(invalid.raw_level, "none ");
            }
            other => panic!("expected invalid persisted authority, got {other:?}"),
        }
        let raw_level: String =
            sqlx::query_scalar("SELECT kill_switch_level FROM risk_budget WHERE credential_id = ?")
                .bind("cred_dirty_budget")
                .fetch_one(&mut *tx)
                .await
                .expect("read authority after loader");
        assert_eq!(raw_level, "none ", "loader must not normalize corruption");
        tx.rollback().await.expect("rollback loader test");
        store.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod fill_recovery_guard_tests {
    use super::{fill_recovery_guard_disposition, FillRecoveryGuardDisposition};
    use crate::protection::OrderRole;

    #[test]
    fn t39b_incomplete_fill_recovery_rejects_entry() {
        assert_eq!(
            fill_recovery_guard_disposition("open_long", false, OrderRole::Entry),
            FillRecoveryGuardDisposition::RejectEntry
        );
    }

    #[test]
    fn t39b_incomplete_fill_recovery_skips_close_independently() {
        assert_eq!(
            fill_recovery_guard_disposition("close_long", false, OrderRole::Exit),
            FillRecoveryGuardDisposition::SkipClose
        );
    }

    #[test]
    fn t39b_incomplete_fill_recovery_skips_hold_independently() {
        assert_eq!(
            fill_recovery_guard_disposition("hold", false, OrderRole::Entry),
            FillRecoveryGuardDisposition::SkipHold
        );
    }

    #[test]
    fn t39b_incomplete_fill_recovery_skips_reduce_only_independently() {
        assert_eq!(
            fill_recovery_guard_disposition("open_long", true, OrderRole::Entry),
            FillRecoveryGuardDisposition::SkipReduceOnly
        );
    }

    #[test]
    fn t39b_incomplete_fill_recovery_skips_protection_independently() {
        assert_eq!(
            fill_recovery_guard_disposition("open_long", false, OrderRole::ProtectionSl),
            FillRecoveryGuardDisposition::SkipProtection
        );
    }

    #[test]
    fn t39b_incomplete_fill_recovery_skips_l3_flatten_independently() {
        assert_eq!(
            fill_recovery_guard_disposition("open_long", false, OrderRole::Emergency),
            FillRecoveryGuardDisposition::SkipL3Flatten
        );
    }
}

#[cfg(test)]
mod market_reference_tests {
    use super::*;

    fn quote(
        price: &str,
        at: Option<i64>,
    ) -> Result<crate::exchange_filters::ExchangeConstraints, String> {
        Ok(crate::exchange_filters::ExchangeConstraints {
            filters: crate::exchange_filters::btcusdt_test_filters(),
            mark_price: price.to_string(),
            mark_price_as_of_ms: at,
            stale_filters: false,
        })
    }

    #[test]
    fn market_reference_bounds_apply_to_actual_evaluation_time() {
        let key = "binance|futures|BTC|USDT|USDT";
        let observed = quote("79683.6192029", Some(10_000));
        let fresh = EntryMarketReference::from_constraints(Some(&observed), key, 40_000);
        assert_eq!(fresh.price(), Some(79683.6192029));
        assert_eq!(fresh.as_of_ms, Some(10_000));
        // A cache TTL of 60s must not turn a 45s-old quote into a fresh input.
        let stale = EntryMarketReference::from_constraints(Some(&observed), key, 55_000);
        assert_eq!(stale.price(), None);
        assert_eq!(
            stale.unavailable_reason.as_deref(),
            Some("venue_quote_observation_stale")
        );
        assert!(
            EntryMarketReference::from_constraints(Some(&observed), key, 40_001)
                .price()
                .is_none()
        );
        let future = EntryMarketReference::from_constraints(Some(&observed), key, 9_999);
        assert_eq!(future.price(), None);
        assert_eq!(
            future.unavailable_reason.as_deref(),
            Some("venue_quote_observation_in_future")
        );
    }

    #[test]
    fn market_reference_invalid_quote_and_close_sentinel_remain_unavailable() {
        let key = "binance|futures|BTC|USDT|USDT";
        for price in ["", "0", "-1", "NaN", "inf", "1e999"] {
            let value = quote(price, Some(10_000));
            let observed = EntryMarketReference::from_constraints(Some(&value), key, 10_000);
            assert_eq!(observed.price(), None, "invalid price {price}");
            assert_eq!(
                observed.unavailable_reason.as_deref(),
                Some("invalid_venue_mark_price")
            );
        }
        let sentinel = quote("1", None);
        let observed = EntryMarketReference::from_constraints(Some(&sentinel), key, 10_000);
        assert_eq!(observed.price(), None);
        assert_eq!(
            observed.unavailable_reason.as_deref(),
            Some("venue_quote_observation_time_missing")
        );
        let failed = Err("test transport unavailable".to_string());
        for source in [None, Some(&failed)] {
            let observed = EntryMarketReference::from_constraints(source, key, 10_000);
            assert_eq!(observed.price(), None);
            assert_eq!(
                observed.unavailable_reason.as_deref(),
                Some("venue_constraints_unavailable")
            );
        }
    }
}
