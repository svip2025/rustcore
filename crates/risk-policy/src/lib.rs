//! Deterministic risk policy evaluation. This crate performs no I/O and reads
//! no clock: every fact, including `now_ms`, is supplied in [`Input`].

use std::collections::BTreeMap;

use ai_trade_contracts::{canonical_json, sha256_hex};
use serde::{Deserialize, Serialize};

pub const EVALUATOR_VERSION: &str = "risk-policy@1.1.0";
pub const FACTORY_POLICY_JSON: &str = include_str!("../policy/risk_policy_v3.json");
pub const FACTORY_POLICY_HASH: &str = include_str!("../policy/risk_policy_v3.sha256");

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Policy {
    pub policy_version: String,
    pub schema_version: u32,
    pub preconditions: Preconditions,
    pub per_trade: PerTrade,
    pub portfolio: Portfolio,
    pub account: AccountPolicy,
    pub churn: ChurnPolicy,
    pub context_filters: ContextFilters,
    pub symbol_controls: SymbolControls,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Preconditions {
    pub require_data_quality_pass: bool,
    pub risk_budget_max_age_ms: i64,
    pub reject_expired_intent: bool,
    pub require_risk_budget: bool,
    pub require_kill_switch: bool,
    pub require_data_snapshot: bool,
    pub require_exchange_filters: bool,
    pub require_typed_protection: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerTrade {
    pub risk_per_trade_pct: f64,
    pub min_conviction_open: f64,
    pub min_conviction_open_short: f64,
    pub protection_required: bool,
    pub min_stop_distance_pct: f64,
    pub max_stop_distance_pct: f64,
    pub min_rr: f64,
    pub fee_per_side: f64,
    pub slippage_round_trip: f64,
    pub cost_aware_tp_multiple: f64,
    pub max_leverage: u32,
    pub max_slippage_bps: u32,
    pub min_notional_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Portfolio {
    pub max_position_notional_usdt: f64,
    pub max_credential_notional_usdt: f64,
    pub max_managed_capital_usdt: f64,
    pub max_total_exposure_pct: f64,
    pub max_pair_exposure_pct: f64,
    pub max_concurrent_positions: usize,
    pub max_same_direction_positions: usize,
    pub max_correlated_group_exposure_pct: f64,
    pub correlation_groups: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountPolicy {
    pub max_daily_loss_pct: f64,
    pub max_drawdown_pct: f64,
    pub min_equity_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChurnPolicy {
    pub min_hold_minutes: i64,
    pub min_conviction_close: f64,
    pub entry_cooldown_minutes_after_stop: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextFilters {
    pub enabled: bool,
    pub long_max_range_pos: f64,
    pub short_min_range_pos: f64,
    pub min_trend_score_long: i32,
    pub max_trend_score_short: i32,
    pub min_vol_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SymbolControls {
    pub disabled_for_open: Vec<String>,
    pub disabled_for_short: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input {
    pub policy: Policy,
    pub policy_hash: String,
    pub intent: Intent,
    pub now_ms: i64,
    pub data_snapshot: Option<DataSnapshot>,
    /// True only for a wire contract that can carry a DataSnapshot decision.
    /// Frozen v1 sets this false until that production path is retired; once
    /// v1 is removed, this compatibility discriminator can be removed too.
    pub data_snapshot_capable: bool,
    pub stage_certification: Option<StageCertificationInput>,
    pub risk_budget: Option<RiskBudget>,
    pub kill_switch: Option<KillSwitch>,
    pub account: Option<AccountSnapshot>,
    pub positions: Vec<Position>,
    pub exchange_filters: Option<ExchangeFilters>,
    pub reservation: Option<Reservation>,
    pub credential_degraded: bool,
    pub context: Option<MarketContext>,
    pub churn: Option<ChurnState>,
    /// True only for a wire contract that can carry typed protection fields.
    /// Frozen v1 sets this false so `require_typed_protection` is explicitly
    /// inapplicable instead of relying on `None` values as an implicit signal.
    pub typed_protection_capable: bool,
    /// Compatibility quantity for a wire format that cannot carry typed
    /// protection. It is never used when complete entry/SL/TP inputs exist.
    pub legacy_sizing: Option<LegacySizingInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LegacySizingInput {
    pub quantity: f64,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Intent {
    pub intent_id: String,
    pub bot_uid: String,
    pub symbol_key: String,
    pub action: Action,
    /// Effective OMS flag. An explicitly reduce-only request is a safety
    /// action even if a malformed caller pairs it with an opening action.
    pub reduce_only: bool,
    pub entry_ref_price: Option<f64>,
    pub stop_loss_price: Option<f64>,
    pub take_profit_price: Option<f64>,
    pub conviction: Option<f64>,
    pub valid_until_ms: Option<i64>,
    pub requested_leverage: u32,
    pub max_slippage_bps: Option<u32>,
    pub observed_slippage_bps: Option<u32>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
    Hold,
}

impl Action {
    pub fn is_open(self) -> bool {
        matches!(self, Self::OpenLong | Self::OpenShort)
    }

    pub fn is_close(self) -> bool {
        matches!(self, Self::CloseLong | Self::CloseShort)
    }

    fn direction(self) -> Option<&'static str> {
        match self {
            Self::OpenLong | Self::CloseLong => Some("long"),
            Self::OpenShort | Self::CloseShort => Some("short"),
            Self::Hold => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataSnapshot {
    pub allow_open: bool,
    /// A wire-level invariant violation. Opening fails closed with this audit
    /// detail; closing remains outside the data-quality gate.
    pub consistency_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StageCertificationInput {
    pub requested_stage: String,
    pub config_hash: String,
    pub prompt_hash: String,
    pub policy_version: String,
    pub code_commit: String,
    pub venue_is_testnet: bool,
    pub certifications: Vec<PromotionCertification>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PromotionCertification {
    pub cert_id: String,
    pub stage: String,
    pub config_hash: String,
    pub prompt_hash: String,
    pub policy_version: String,
    pub code_commit: String,
    pub passed: bool,
    pub invalidated_at_ms: Option<i64>,
    pub certified_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskBudget {
    pub as_of_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KillSwitch {
    pub level: String,
    pub scope_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountSnapshot {
    pub equity_usdt: f64,
    pub available_margin_usdt: f64,
    pub today_realized_pnl_pct: Option<f64>,
    pub drawdown_pct: Option<f64>,
    pub total_exposure_usdt: f64,
    pub credential_notional_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Position {
    pub bot_uid: String,
    pub symbol_key: String,
    pub direction: String,
    pub state: String,
    pub notional_usdt: f64,
    pub correlation_group: Option<String>,
    /// A durable pending claim whose semantic idempotency key is already the
    /// authoritative key for this entry epoch. Replays may proceed to the DB
    /// idempotency check; unrelated active positions remain rejected.
    pub pending_retry: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExchangeFilters {
    pub lot_step: f64,
    pub min_notional_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Reservation {
    pub reserved_usdt: f64,
    pub cap_usdt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MarketContext {
    pub range_pos: f64,
    pub trend_score_1h: i32,
    pub trend_score_4h: i32,
    pub vol_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChurnState {
    pub position_age_minutes: i64,
    pub daily_loss_triggered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    Approve,
    Resize,
    Reject,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleOutcome {
    Pass,
    Capped,
    Reject,
    Skipped,
}

#[derive(Clone, Debug, Serialize)]
pub struct RuleResult {
    pub rule: String,
    pub result: RuleOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CapRecord {
    pub cap: String,
    pub before: String,
    pub after: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "mode")]
pub enum Sizing {
    #[serde(rename = "risk_budget")]
    RiskBudget {
        equity_usdt: String,
        risk_per_trade_pct: f64,
        risk_budget_usdt: String,
        entry_ref_price: String,
        stop_loss_price: String,
        stop_distance_pct: f64,
        leverage: u32,
        computed_qty: String,
        caps: Vec<CapRecord>,
        capped_by: Vec<String>,
        final_qty: String,
        final_notional_usdt: String,
    },
    #[serde(rename = "legacy_client_quantity")]
    LegacyClientQuantity {
        risk_budget_sizing: RuleOutcome,
        reason: String,
        detail: String,
        requested_qty: String,
        leverage: u32,
        caps: Vec<CapRecord>,
        capped_by: Vec<String>,
        final_qty: String,
        final_notional_usdt: String,
    },
}

impl Sizing {
    pub fn final_qty(&self) -> &str {
        match self {
            Self::RiskBudget { final_qty, .. } | Self::LegacyClientQuantity { final_qty, .. } => {
                final_qty
            }
        }
    }

    pub fn final_notional_usdt(&self) -> &str {
        match self {
            Self::RiskBudget {
                final_notional_usdt,
                ..
            }
            | Self::LegacyClientQuantity {
                final_notional_usdt,
                ..
            } => final_notional_usdt,
        }
    }

    pub fn effective_stop_loss_price(&self) -> Option<&str> {
        match self {
            Self::RiskBudget {
                stop_loss_price, ..
            } => Some(stop_loss_price),
            Self::LegacyClientQuantity { .. } => None,
        }
    }

    pub fn capped_by(&self) -> &[String] {
        match self {
            Self::RiskBudget { capped_by, .. } | Self::LegacyClientQuantity { capped_by, .. } => {
                capped_by
            }
        }
    }

    pub fn caps(&self) -> &[CapRecord] {
        match self {
            Self::RiskBudget { caps, .. } | Self::LegacyClientQuantity { caps, .. } => caps,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RiskVerdict {
    pub schema_version: u32,
    pub risk_verdict_id: String,
    pub intent_id: String,
    pub created_at_ms: i64,
    pub policy_version: String,
    pub policy_hash: String,
    pub decision: Decision,
    pub reject_reason_code: Option<String>,
    pub sizing: Option<Sizing>,
    pub rules: Vec<RuleResult>,
    pub budget_as_of_ms: Option<i64>,
    pub evaluator_version: String,
}

impl RiskVerdict {
    pub fn rules_json(&self) -> String {
        serde_json::to_string(&self.rules).expect("rules serialize")
    }

    pub fn sizing_json(&self) -> Option<String> {
        self.sizing
            .as_ref()
            .map(|sizing| serde_json::to_string(sizing).expect("sizing serializes"))
    }

    pub fn replay_fingerprint(&self) -> String {
        let value = serde_json::to_value(self).expect("verdict serializes");
        sha256_hex(&canonical_json(&value))
    }
}

pub fn factory_policy() -> Policy {
    serde_json::from_str(FACTORY_POLICY_JSON).expect("factory policy JSON is valid")
}

pub fn policy_hash(policy: &Policy) -> String {
    let value = serde_json::to_value(policy).expect("policy serializes");
    format!("sha256:{}", sha256_hex(&canonical_json(&value)))
}

fn pass(rule: &str, detail: impl Into<Option<String>>) -> RuleResult {
    RuleResult {
        rule: rule.to_string(),
        result: RuleOutcome::Pass,
        reason_code: None,
        reason: None,
        detail: detail.into(),
    }
}

fn capped(rule: &str, detail: String) -> RuleResult {
    RuleResult {
        rule: rule.to_string(),
        result: RuleOutcome::Capped,
        reason_code: None,
        reason: None,
        detail: Some(detail),
    }
}

fn reject(rule: &str, code: &str, detail: impl Into<String>) -> RuleResult {
    RuleResult {
        rule: rule.to_string(),
        result: RuleOutcome::Reject,
        reason_code: Some(code.to_string()),
        reason: None,
        detail: Some(detail.into()),
    }
}

fn skipped(rule: &str, reason: &str) -> RuleResult {
    RuleResult {
        rule: rule.to_string(),
        result: RuleOutcome::Skipped,
        reason_code: None,
        reason: Some(reason.to_string()),
        detail: None,
    }
}

fn active(position: &Position) -> bool {
    matches!(
        position.state.as_str(),
        "pending" | "open" | "protected" | "closing" | "flattening"
    )
}

fn decimal(value: f64) -> String {
    if !value.is_finite() {
        return "0".to_string();
    }
    let mut text = format!("{:.12}", value.max(0.0));
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" || text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 || !step.is_finite() {
        value.max(0.0)
    } else {
        ((value.max(0.0) / step) + 1e-12).floor() * step
    }
}

fn cap_qty(qty: &mut f64, maximum: f64, name: &str, caps: &mut Vec<CapRecord>) -> bool {
    let maximum = maximum.max(0.0);
    if maximum + 1e-12 < *qty {
        let before = *qty;
        *qty = maximum;
        caps.push(CapRecord {
            cap: name.to_string(),
            before: decimal(before),
            after: decimal(*qty),
        });
        true
    } else {
        false
    }
}

fn not_open(rule: &str) -> RuleResult {
    skipped(rule, "not_applicable_to_close_or_hold")
}

fn stage_rank(stage: &str) -> Option<u8> {
    match stage {
        "paper" => Some(0),
        "demo" => Some(1),
        "live_small" => Some(2),
        "live" => Some(3),
        _ => None,
    }
}

fn stage_certified_rule(input: &Input) -> RuleResult {
    if !input.intent.action.is_open() || input.intent.reduce_only {
        return not_open("stage_certified");
    }

    let Some(stage) = input.stage_certification.as_ref() else {
        return reject(
            "stage_certified",
            "STAGE_NOT_CERTIFIED",
            "authorization_source=none requested_stage=unknown fallback_stage=paper input_absent",
        );
    };
    let requested = stage.requested_stage.trim().to_ascii_lowercase();
    if requested == "paper" {
        return skipped("stage_certified", "paper_does_not_require_certification");
    }
    let Some(requested_rank) = stage_rank(&requested) else {
        return reject(
            "stage_certified",
            "STAGE_NOT_CERTIFIED",
            format!(
                "authorization_source=none requested_stage={requested} fallback_stage=paper unsupported_stage"
            ),
        );
    };

    // Select from raw projection facts inside the pure evaluator. The caller
    // never supplies or negates an `authorized` boolean, so replay and polarity
    // tests cover the same selection logic used by production.
    let matching = stage.certifications.iter().find(|certification| {
        certification.passed
            && certification.invalidated_at_ms.is_none()
            && stage_rank(&certification.stage).is_some_and(|rank| rank >= requested_rank)
            && certification.config_hash == stage.config_hash
            && certification.prompt_hash == stage.prompt_hash
            && certification.policy_version == stage.policy_version
            && certification.code_commit == stage.code_commit
            && stage.policy_version == input.policy.policy_version
    });
    if let Some(certification) = matching {
        return pass(
            "stage_certified",
            Some(format!(
                "authorization_source=certification cert_id={} requested_stage={} certified_stage={}",
                certification.cert_id, requested, certification.stage
            )),
        );
    }

    if requested == "demo" && stage.venue_is_testnet {
        return pass(
            "stage_certified",
            Some(
                "authorization_source=testnet_venue requested_stage=demo certified_stage=none"
                    .to_string(),
            ),
        );
    }

    let fallback_stage = if stage.venue_is_testnet {
        "demo"
    } else {
        "paper"
    };
    reject(
        "stage_certified",
        "STAGE_NOT_CERTIFIED",
        format!(
            "authorization_source=none requested_stage={requested} fallback_stage={fallback_stage} no_matching_active_passed_certification"
        ),
    )
}

/// Evaluate every rule. Rules are deliberately appended without early return;
/// decision derivation occurs only after the complete audit vector exists.
pub fn evaluate(input: &Input) -> RiskVerdict {
    let open = input.intent.action.is_open();
    let close = input.intent.action.is_close();
    let policy = &input.policy;
    let mut rules = Vec::with_capacity(27);
    let mut caps = Vec::new();

    // Preconditions supplied by later roadmap tasks.
    rules.push(stage_certified_rule(input));

    if !open {
        let mut result = not_open("data_quality_gate");
        result.detail = input
            .data_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.consistency_error.clone());
        rules.push(result);
    } else if !input.data_snapshot_capable {
        rules.push(skipped("data_quality_gate", "wire_not_capable"));
    } else if let Some(snapshot) = &input.data_snapshot {
        if let Some(error) = snapshot.consistency_error.as_deref() {
            rules.push(reject(
                "data_quality_gate",
                "DATA_SNAPSHOT_INCONSISTENT",
                error,
            ));
        } else if policy.preconditions.require_data_quality_pass && !snapshot.allow_open {
            rules.push(reject(
                "data_quality_gate",
                "DATA_QUALITY_FAIL",
                "allow_open=false",
            ));
        } else {
            rules.push(pass(
                "data_quality_gate",
                Some(format!("allow_open={}", snapshot.allow_open)),
            ));
        }
    } else if policy.preconditions.require_data_snapshot {
        rules.push(reject(
            "data_quality_gate",
            "DATA_QUALITY_FAIL",
            "required input absent",
        ));
    } else {
        rules.push(skipped("data_quality_gate", "input_absent"));
    }

    if !open {
        rules.push(not_open("risk_budget_freshness"));
    } else if let Some(budget) = &input.risk_budget {
        let age = input.now_ms.saturating_sub(budget.as_of_ms);
        if age > policy.preconditions.risk_budget_max_age_ms {
            rules.push(reject(
                "risk_budget_freshness",
                "STALE_RISK_BUDGET",
                format!("age_ms={age}"),
            ));
        } else {
            rules.push(pass("risk_budget_freshness", Some(format!("age_ms={age}"))));
        }
    } else if policy.preconditions.require_risk_budget {
        rules.push(reject(
            "risk_budget_freshness",
            "STALE_RISK_BUDGET",
            "required input absent",
        ));
    } else {
        rules.push(skipped("risk_budget_freshness", "input_absent"));
    }

    if !open {
        rules.push(not_open("kill_switch_level"));
    } else if let Some(kill_switch) = &input.kill_switch {
        if kill_switch.level != "none" {
            rules.push(reject(
                "kill_switch_level",
                "KILL_SWITCH_ACTIVE",
                format!(
                    "level={} scope_key={}",
                    kill_switch.level, kill_switch.scope_key
                ),
            ));
        } else {
            rules.push(pass(
                "kill_switch_level",
                Some(format!("level=none scope_key={}", kill_switch.scope_key)),
            ));
        }
    } else if policy.preconditions.require_kill_switch {
        rules.push(reject(
            "kill_switch_level",
            "KILL_SWITCH_ACTIVE",
            "required input absent",
        ));
    } else {
        rules.push(skipped("kill_switch_level", "input_absent"));
    }

    if !open || !policy.preconditions.reject_expired_intent {
        rules.push(if open {
            pass("intent_not_expired", Some("check_disabled".to_string()))
        } else {
            not_open("intent_not_expired")
        });
    } else if let Some(valid_until_ms) = input.intent.valid_until_ms {
        if input.now_ms >= valid_until_ms {
            rules.push(reject(
                "intent_not_expired",
                "INTENT_EXPIRED",
                format!("now_ms={} valid_until_ms={valid_until_ms}", input.now_ms),
            ));
        } else {
            rules.push(pass(
                "intent_not_expired",
                Some(format!("valid_until_ms={valid_until_ms}")),
            ));
        }
    } else {
        rules.push(skipped("intent_not_expired", "input_absent"));
    }

    let prices = input
        .intent
        .entry_ref_price
        .zip(input.intent.stop_loss_price)
        .zip(input.intent.take_profit_price);
    let typed_protection_present = prices.is_some();
    if !open {
        rules.push(not_open("protection_required"));
    } else if !input.typed_protection_capable {
        rules.push(skipped("protection_required", "wire_not_capable"));
    } else if !typed_protection_present && policy.preconditions.require_typed_protection {
        rules.push(reject(
            "protection_required",
            "MISSING_PROTECTION",
            "SL/TP/entry input absent",
        ));
    } else if !typed_protection_present {
        rules.push(skipped("protection_required", "input_absent"));
    } else {
        let detail = if policy.per_trade.protection_required {
            "required_and_present"
        } else {
            "present_not_required"
        };
        rules.push(pass("protection_required", Some(detail.to_string())));
    }

    let mut effective_stop = input.intent.stop_loss_price;
    let mut stop_distance = None;
    if !open {
        rules.push(not_open("stop_distance_clamp"));
    } else if input.typed_protection_capable && typed_protection_present {
        let (entry, stop) = input
            .intent
            .entry_ref_price
            .zip(input.intent.stop_loss_price)
            .expect("complete typed protection has entry and stop");
        if entry > 0.0 {
            let raw = (entry - stop).abs() / entry;
            let clamped = raw.clamp(
                policy.per_trade.min_stop_distance_pct,
                policy.per_trade.max_stop_distance_pct,
            );
            stop_distance = Some(clamped);
            if (raw - clamped).abs() > 1e-12 {
                let adjusted = match input.intent.action {
                    Action::OpenLong => entry * (1.0 - clamped),
                    Action::OpenShort => entry * (1.0 + clamped),
                    _ => stop,
                };
                effective_stop = Some(adjusted);
                caps.push(CapRecord {
                    cap: "stop_distance_clamp".to_string(),
                    before: decimal(raw),
                    after: decimal(clamped),
                });
                rules.push(capped(
                    "stop_distance_clamp",
                    format!("{raw:.8} -> {clamped:.8}"),
                ));
            } else {
                rules.push(pass(
                    "stop_distance_clamp",
                    Some(format!("distance={raw:.8}")),
                ));
            }
        } else {
            rules.push(reject(
                "stop_distance_clamp",
                "SIZE_BELOW_EXCHANGE_MIN",
                "entry_ref_price<=0",
            ));
        }
    } else {
        rules.push(skipped("stop_distance_clamp", "input_absent"));
    }

    if !open {
        rules.push(not_open("min_rr"));
    } else if let Some(((entry, stop), tp)) = prices {
        let risk = (entry - stop).abs();
        let rr = if risk > 0.0 {
            (tp - entry).abs() / risk
        } else {
            0.0
        };
        if rr + 1e-12 < policy.per_trade.min_rr {
            rules.push(reject("min_rr", "RR_TOO_LOW", format!("rr={rr:.8}")));
        } else {
            rules.push(pass("min_rr", Some(format!("rr={rr:.8}"))));
        }
    } else {
        rules.push(skipped("min_rr", "input_absent"));
    }

    if !open {
        rules.push(not_open("cost_aware_tp"));
    } else if let Some(((entry, _), tp)) = prices {
        let tp_pct = if entry > 0.0 {
            (tp - entry).abs() / entry
        } else {
            0.0
        };
        // TP and round-trip costs share the entry-notional denominator.
        // Expressing both on margin multiplies each by leverage, which cancels.
        // Leverage remains an independent sizing constraint.
        let required = policy.per_trade.cost_aware_tp_multiple
            * (policy.per_trade.fee_per_side * 2.0 + policy.per_trade.slippage_round_trip);
        if tp_pct + 1e-12 < required {
            rules.push(reject(
                "cost_aware_tp",
                "TP_BELOW_COST",
                format!("tp_pct={tp_pct:.8} required={required:.8}"),
            ));
        } else {
            rules.push(pass("cost_aware_tp", Some(format!("tp_pct={tp_pct:.8}"))));
        }
    } else {
        rules.push(skipped("cost_aware_tp", "input_absent"));
    }

    if !open {
        rules.push(not_open("min_conviction"));
    } else if let Some(conviction) = input.intent.conviction {
        let minimum = if input.intent.action == Action::OpenShort {
            policy.per_trade.min_conviction_open_short
        } else {
            policy.per_trade.min_conviction_open
        };
        if conviction + 1e-12 < minimum {
            rules.push(reject(
                "min_conviction",
                "CONVICTION_TOO_LOW",
                format!("conviction={conviction:.8} minimum={minimum:.8}"),
            ));
        } else {
            rules.push(pass(
                "min_conviction",
                Some(format!("conviction={conviction:.8}")),
            ));
        }
    } else {
        rules.push(skipped("min_conviction", "input_absent"));
    }

    let requested_leverage = input.intent.requested_leverage.max(1);
    let effective_leverage = requested_leverage.min(policy.per_trade.max_leverage);
    if !open {
        rules.push(not_open("max_leverage"));
    } else if policy.per_trade.max_leverage == 0 {
        rules.push(reject(
            "max_leverage",
            "LEVERAGE_EXCEEDED",
            "policy permits no leverage",
        ));
    } else if requested_leverage > policy.per_trade.max_leverage {
        caps.push(CapRecord {
            cap: "max_leverage".to_string(),
            before: requested_leverage.to_string(),
            after: effective_leverage.to_string(),
        });
        rules.push(capped(
            "max_leverage",
            format!("{requested_leverage} -> {effective_leverage}"),
        ));
    } else {
        rules.push(pass(
            "max_leverage",
            Some(format!("leverage={requested_leverage}")),
        ));
    }

    if !open {
        rules.push(not_open("symbol_controls"));
    } else {
        let disabled = policy
            .symbol_controls
            .disabled_for_open
            .iter()
            .any(|s| s == &input.intent.symbol_key)
            || (input.intent.action == Action::OpenShort
                && policy
                    .symbol_controls
                    .disabled_for_short
                    .iter()
                    .any(|s| s == &input.intent.symbol_key));
        if disabled {
            rules.push(reject(
                "symbol_controls",
                "SYMBOL_DISABLED",
                "symbol disabled by policy",
            ));
        } else {
            rules.push(pass("symbol_controls", None));
        }
    }

    if !open {
        rules.push(not_open("context_filters"));
    } else if !policy.context_filters.enabled {
        rules.push(skipped("context_filters", "disabled_by_policy"));
    } else if let Some(context) = &input.context {
        let allowed = match input.intent.action {
            Action::OpenLong => {
                context.range_pos <= policy.context_filters.long_max_range_pos
                    && context.trend_score_1h >= policy.context_filters.min_trend_score_long
                    && context.trend_score_4h >= policy.context_filters.min_trend_score_long
                    && context.vol_ratio >= policy.context_filters.min_vol_ratio
            }
            Action::OpenShort => {
                context.range_pos >= policy.context_filters.short_min_range_pos
                    && context.trend_score_1h <= policy.context_filters.max_trend_score_short
                    && context.trend_score_4h <= policy.context_filters.max_trend_score_short
                    && context.vol_ratio >= policy.context_filters.min_vol_ratio
            }
            _ => true,
        };
        if allowed {
            rules.push(pass("context_filters", None));
        } else {
            // Contracts define no context-specific stable code. EXPOSURE_EXCEEDED
            // is intentionally not overloaded; the audit record rejects while
            // the report calls out this contract gap.
            rules.push(reject(
                "context_filters",
                "EXPOSURE_EXCEEDED",
                "market context rejected by enabled policy",
            ));
        }
    } else {
        rules.push(skipped("context_filters", "input_absent"));
    }

    if !close {
        rules.push(skipped("churn_guard", "not_applicable_to_open_or_hold"));
    } else if let Some(churn) = &input.churn {
        if churn.daily_loss_triggered {
            rules.push(skipped("churn_guard", "daily_loss_exemption"));
        } else {
            let conviction = input.intent.conviction.unwrap_or(0.0);
            if churn.position_age_minutes < policy.churn.min_hold_minutes
                || conviction < policy.churn.min_conviction_close
            {
                // No dedicated stable churn code exists in Contracts §5.2.
                rules.push(reject(
                    "churn_guard",
                    "CONVICTION_TOO_LOW",
                    format!(
                        "age_minutes={} conviction={conviction:.8}",
                        churn.position_age_minutes
                    ),
                ));
            } else {
                rules.push(pass("churn_guard", None));
            }
        }
    } else {
        rules.push(skipped("churn_guard", "input_absent"));
    }

    if !open {
        rules.push(not_open("single_active_entry"));
    } else {
        let direction = input.intent.action.direction().unwrap_or_default();
        let duplicate = input.positions.iter().any(|p| {
            active(p)
                && p.bot_uid == input.intent.bot_uid
                && p.symbol_key == input.intent.symbol_key
                && p.direction == direction
                && !p.pending_retry
        });
        if duplicate {
            rules.push(reject(
                "single_active_entry",
                "DUPLICATE_ACTIVE_ENTRY",
                "matching active position exists",
            ));
        } else {
            rules.push(pass("single_active_entry", None));
        }
    }

    let account = input.account.as_ref();
    let entry = input.intent.entry_ref_price.filter(|v| *v > 0.0);
    let equity = account.map(|a| a.equity_usdt.max(0.0));
    let legacy_sizing = if open && !input.typed_protection_capable {
        input
            .legacy_sizing
            .as_ref()
            .filter(|legacy| legacy.quantity.is_finite() && legacy.quantity > 0.0)
    } else {
        None
    };
    let mut qty = legacy_sizing.map(|legacy| legacy.quantity).unwrap_or(0.0);
    let mut raw_qty = 0.0;
    if open {
        if let (Some(entry), Some(distance), Some(equity)) = (entry, stop_distance, equity) {
            if distance > 0.0 {
                raw_qty = equity * policy.per_trade.risk_per_trade_pct / (distance * entry);
                qty = raw_qty;
            }
        }
    }

    let current_symbol = input
        .positions
        .iter()
        .filter(|p| active(p) && p.symbol_key == input.intent.symbol_key)
        .map(|p| p.notional_usdt.max(0.0))
        .sum::<f64>();
    let position_remaining =
        (policy.portfolio.max_position_notional_usdt - current_symbol).max(0.0);
    if !open {
        rules.push(not_open("max_position_notional"));
    } else if position_remaining <= 0.0 {
        rules.push(reject(
            "max_position_notional",
            "NOTIONAL_EXCEEDED",
            "no remaining position notional",
        ));
    } else if let Some(entry) = entry {
        let before = qty;
        if cap_qty(
            &mut qty,
            position_remaining / entry,
            "max_position_notional",
            &mut caps,
        ) {
            rules.push(capped(
                "max_position_notional",
                format!("{} -> {}", decimal(before), decimal(qty)),
            ));
        } else {
            rules.push(pass("max_position_notional", None));
        }
    } else {
        rules.push(skipped("max_position_notional", "input_absent"));
    }

    let credential_current = account
        .map(|a| a.credential_notional_usdt.max(0.0))
        .unwrap_or(0.0);
    let credential_remaining =
        (policy.portfolio.max_credential_notional_usdt - credential_current).max(0.0);
    if !open {
        rules.push(not_open("max_credential_notional"));
    } else if account.is_none() {
        rules.push(skipped("max_credential_notional", "input_absent"));
    } else if credential_remaining <= 0.0 {
        rules.push(reject(
            "max_credential_notional",
            "NOTIONAL_EXCEEDED",
            "no remaining credential notional",
        ));
    } else if let Some(entry) = entry {
        let before = qty;
        if cap_qty(
            &mut qty,
            credential_remaining / entry,
            "max_credential_notional",
            &mut caps,
        ) {
            rules.push(capped(
                "max_credential_notional",
                format!("{} -> {}", decimal(before), decimal(qty)),
            ));
        } else {
            rules.push(pass("max_credential_notional", None));
        }
    } else {
        rules.push(skipped("max_credential_notional", "input_absent"));
    }

    let total_remaining = account.map(|a| {
        (policy.portfolio.max_total_exposure_pct * a.equity_usdt - a.total_exposure_usdt).max(0.0)
    });
    if !open {
        rules.push(not_open("max_total_exposure"));
    } else if let (Some(remaining), Some(entry)) = (total_remaining, entry) {
        if remaining <= 0.0 {
            rules.push(reject(
                "max_total_exposure",
                "EXPOSURE_EXCEEDED",
                "no remaining total exposure",
            ));
        } else {
            let before = qty;
            if cap_qty(
                &mut qty,
                remaining / entry * effective_leverage.max(1) as f64,
                "max_total_exposure",
                &mut caps,
            ) {
                rules.push(capped(
                    "max_total_exposure",
                    format!("{} -> {}", decimal(before), decimal(qty)),
                ));
            } else {
                rules.push(pass("max_total_exposure", None));
            }
        }
    } else {
        rules.push(skipped("max_total_exposure", "input_absent"));
    }

    if !open {
        rules.push(not_open("max_concurrent_positions"));
    } else {
        let active_positions = input.positions.iter().filter(|p| active(p)).count();
        let same_direction = input
            .positions
            .iter()
            .filter(|p| active(p) && Some(p.direction.as_str()) == input.intent.action.direction())
            .count();
        if active_positions >= policy.portfolio.max_concurrent_positions
            || same_direction >= policy.portfolio.max_same_direction_positions
        {
            rules.push(reject(
                "max_concurrent_positions",
                "TOO_MANY_POSITIONS",
                format!("active={active_positions} same_direction={same_direction}"),
            ));
        } else {
            rules.push(pass(
                "max_concurrent_positions",
                Some(format!(
                    "active={active_positions} same_direction={same_direction}"
                )),
            ));
        }
    }

    let base = input
        .intent
        .symbol_key
        .split('|')
        .nth(2)
        .unwrap_or_default();
    let group = policy
        .portfolio
        .correlation_groups
        .iter()
        .find(|(_, symbols)| symbols.iter().any(|symbol| symbol == base))
        .map(|(name, _)| name.as_str());
    let group_current = group.map(|name| {
        input
            .positions
            .iter()
            .filter(|p| active(p) && p.correlation_group.as_deref() == Some(name))
            .map(|p| p.notional_usdt.max(0.0))
            .sum::<f64>()
    });
    if !open {
        rules.push(not_open("correlated_group_exposure"));
    } else if let (Some(group_name), Some(current), Some(account), Some(entry)) =
        (group, group_current, account, entry)
    {
        let remaining = (policy.portfolio.max_correlated_group_exposure_pct * account.equity_usdt
            - current)
            .max(0.0);
        if remaining <= 0.0 {
            rules.push(reject(
                "correlated_group_exposure",
                "EXPOSURE_EXCEEDED",
                format!("group={group_name} has no remaining exposure"),
            ));
        } else {
            let before = qty;
            if cap_qty(
                &mut qty,
                remaining / entry * effective_leverage.max(1) as f64,
                "correlated_group_exposure",
                &mut caps,
            ) {
                rules.push(capped(
                    "correlated_group_exposure",
                    format!("{} -> {}", decimal(before), decimal(qty)),
                ));
            } else {
                rules.push(pass(
                    "correlated_group_exposure",
                    Some(format!("group={group_name}")),
                ));
            }
        }
    } else if group.is_none() {
        rules.push(pass(
            "correlated_group_exposure",
            Some("symbol_not_grouped".to_string()),
        ));
    } else {
        rules.push(skipped("correlated_group_exposure", "input_absent"));
    }

    if !open {
        rules.push(not_open("daily_loss_limit"));
        rules.push(not_open("max_drawdown"));
        rules.push(not_open("min_equity"));
    } else if let Some(account) = account {
        if let Some(today_realized_pnl_pct) = account.today_realized_pnl_pct {
            if today_realized_pnl_pct <= -policy.account.max_daily_loss_pct {
                rules.push(reject(
                    "daily_loss_limit",
                    "DAILY_LOSS_REACHED",
                    format!("pnl_pct={today_realized_pnl_pct:.8}"),
                ));
            } else {
                rules.push(pass("daily_loss_limit", None));
            }
        } else if policy.preconditions.require_risk_budget {
            rules.push(reject(
                "daily_loss_limit",
                "DAILY_LOSS_REACHED",
                "required input absent",
            ));
        } else {
            rules.push(skipped("daily_loss_limit", "input_absent"));
        }
        if let Some(drawdown_pct) = account.drawdown_pct {
            if drawdown_pct >= policy.account.max_drawdown_pct {
                rules.push(reject(
                    "max_drawdown",
                    "DRAWDOWN_REACHED",
                    format!("drawdown_pct={drawdown_pct:.8}"),
                ));
            } else {
                rules.push(pass("max_drawdown", None));
            }
        } else if policy.preconditions.require_risk_budget {
            rules.push(reject(
                "max_drawdown",
                "DRAWDOWN_REACHED",
                "required input absent",
            ));
        } else {
            rules.push(skipped("max_drawdown", "input_absent"));
        }
        if account.equity_usdt < policy.account.min_equity_usdt {
            rules.push(reject(
                "min_equity",
                "EQUITY_BELOW_MIN",
                format!("equity={}", decimal(account.equity_usdt)),
            ));
        } else {
            rules.push(pass("min_equity", None));
        }
    } else if policy.preconditions.require_risk_budget {
        rules.push(reject(
            "daily_loss_limit",
            "DAILY_LOSS_REACHED",
            "required input absent",
        ));
        rules.push(reject(
            "max_drawdown",
            "DRAWDOWN_REACHED",
            "required input absent",
        ));
        rules.push(reject(
            "min_equity",
            "EQUITY_BELOW_MIN",
            "required input absent",
        ));
    } else {
        rules.push(skipped("daily_loss_limit", "input_absent"));
        rules.push(skipped("max_drawdown", "input_absent"));
        rules.push(skipped("min_equity", "input_absent"));
    }

    if !open {
        rules.push(not_open("notional_reservation"));
    } else if let (Some(reservation), Some(entry)) = (&input.reservation, entry) {
        let remaining = (reservation.cap_usdt - reservation.reserved_usdt).max(0.0);
        let requested_notional = qty * entry;
        if remaining <= 0.0 || requested_notional > remaining + 1e-12 {
            rules.push(reject(
                "notional_reservation",
                "RESERVATION_CONFLICT",
                format!(
                    "requested={} remaining={}",
                    decimal(requested_notional),
                    decimal(remaining)
                ),
            ));
        } else {
            rules.push(pass("notional_reservation", None));
        }
    } else {
        rules.push(reject(
            "notional_reservation",
            "RESERVATION_CONFLICT",
            "reservation input absent",
        ));
    }

    // Pair and available-margin caps belong to the sizing formula even though
    // §2.1 does not assign them standalone rule names.
    if open {
        if let (Some(account), Some(entry)) = (account, entry) {
            cap_qty(
                &mut qty,
                policy.portfolio.max_pair_exposure_pct * account.equity_usdt / entry
                    * effective_leverage.max(1) as f64,
                "max_pair_exposure",
                &mut caps,
            );
            cap_qty(
                &mut qty,
                account.available_margin_usdt.max(0.0) * effective_leverage.max(1) as f64 / entry,
                "available_margin",
                &mut caps,
            );
        }
    }

    if !open {
        rules.push(not_open("exchange_min_size"));
    } else if let Some(filters) = &input.exchange_filters {
        let before = qty;
        qty = floor_to_step(qty, filters.lot_step);
        if qty + 1e-12 < before {
            caps.push(CapRecord {
                cap: "floor_to_step".to_string(),
                before: decimal(before),
                after: decimal(qty),
            });
        }
        let minimum = policy
            .per_trade
            .min_notional_usdt
            .max(filters.min_notional_usdt);
        let notional = entry.unwrap_or(0.0) * qty;
        if notional + 1e-12 < minimum {
            rules.push(reject(
                "exchange_min_size",
                "SIZE_BELOW_EXCHANGE_MIN",
                format!(
                    "notional={} minimum={}",
                    decimal(notional),
                    decimal(minimum)
                ),
            ));
        } else {
            rules.push(pass(
                "exchange_min_size",
                Some(format!("notional={}", decimal(notional))),
            ));
        }
    } else if policy.preconditions.require_exchange_filters {
        rules.push(reject(
            "exchange_min_size",
            "SIZE_BELOW_EXCHANGE_MIN",
            "required input absent",
        ));
    } else {
        rules.push(skipped("exchange_min_size", "input_absent"));
    }

    if !open {
        rules.push(not_open("slippage_limit"));
    } else if let Some(observed) = input.intent.observed_slippage_bps {
        let requested = input
            .intent
            .max_slippage_bps
            .unwrap_or(policy.per_trade.max_slippage_bps);
        let allowed = requested.min(policy.per_trade.max_slippage_bps);
        if observed > allowed {
            rules.push(reject(
                "slippage_limit",
                "SLIPPAGE_EXCEEDED",
                format!("observed={observed} allowed={allowed}"),
            ));
        } else {
            rules.push(pass(
                "slippage_limit",
                Some(format!("observed={observed} allowed={allowed}")),
            ));
        }
    } else {
        rules.push(skipped("slippage_limit", "input_absent"));
    }

    if !open {
        rules.push(not_open("credential_health"));
    } else if input.credential_degraded {
        rules.push(reject(
            "credential_health",
            "DEGRADED_CREDENTIAL",
            "credential degraded",
        ));
    } else {
        rules.push(pass("credential_health", None));
    }

    let had_sizing_caps = !caps.is_empty();
    let sizing = if open {
        match (account, entry, stop_distance, effective_stop) {
            (Some(account), Some(entry), Some(distance), Some(stop))
                if raw_qty.is_finite() && raw_qty > 0.0 =>
            {
                Some(Sizing::RiskBudget {
                    equity_usdt: decimal(account.equity_usdt),
                    risk_per_trade_pct: policy.per_trade.risk_per_trade_pct,
                    risk_budget_usdt: decimal(
                        account.equity_usdt * policy.per_trade.risk_per_trade_pct,
                    ),
                    entry_ref_price: decimal(entry),
                    stop_loss_price: decimal(stop),
                    stop_distance_pct: distance,
                    leverage: effective_leverage,
                    computed_qty: decimal(raw_qty),
                    capped_by: caps.iter().map(|cap| cap.cap.clone()).collect(),
                    caps,
                    final_qty: decimal(qty),
                    final_notional_usdt: decimal(qty * entry),
                })
            }
            _ => legacy_sizing
                .zip(entry)
                .map(|(legacy, entry)| Sizing::LegacyClientQuantity {
                    risk_budget_sizing: RuleOutcome::Skipped,
                    reason: "input_absent".to_string(),
                    detail: legacy.reason.clone(),
                    requested_qty: decimal(legacy.quantity),
                    leverage: effective_leverage,
                    capped_by: caps.iter().map(|cap| cap.cap.clone()).collect(),
                    caps,
                    final_qty: decimal(qty),
                    final_notional_usdt: decimal(qty * entry),
                }),
        }
    } else {
        None
    };

    let reject_reason_code = rules
        .iter()
        .find(|rule| rule.result == RuleOutcome::Reject)
        .and_then(|rule| rule.reason_code.clone());
    let decision = if reject_reason_code.is_some() {
        Decision::Reject
    } else if had_sizing_caps || rules.iter().any(|rule| rule.result == RuleOutcome::Capped) {
        Decision::Resize
    } else {
        Decision::Approve
    };

    let verdict_seed = format!(
        "{}|{}|{}|{}",
        input.intent.intent_id,
        input.now_ms,
        input.policy.policy_version,
        rules.len()
    );
    RiskVerdict {
        schema_version: 1,
        risk_verdict_id: format!("rv_{}", &sha256_hex(&verdict_seed)[..26]),
        intent_id: input.intent.intent_id.clone(),
        created_at_ms: input.now_ms,
        policy_version: input.policy.policy_version.clone(),
        policy_hash: input.policy_hash.clone(),
        decision,
        reject_reason_code,
        sizing,
        rules,
        budget_as_of_ms: input.risk_budget.as_ref().map(|budget| budget.as_of_ms),
        evaluator_version: EVALUATOR_VERSION.to_string(),
    }
}
