use risk_policy::*;

const SYMBOL: &str = "binance|futures|BTC|USDT|USDT";

fn base_input() -> Input {
    let policy = factory_policy();
    Input {
        policy_hash: policy_hash(&policy),
        policy,
        intent: Intent {
            intent_id: "int_test".to_string(),
            bot_uid: "bot_test".to_string(),
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
        kill_switch: Some(KillSwitch {
            level: "none".to_string(),
            scope_key: "global".to_string(),
        }),
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

fn rule<'a>(verdict: &'a RiskVerdict, name: &str) -> &'a RuleResult {
    verdict.rules.iter().find(|rule| rule.rule == name).unwrap()
}

fn assert_code(input: Input, code: &str) {
    let verdict = evaluate(&input);
    assert_eq!(verdict.decision, Decision::Reject);
    assert!(
        verdict.rules.iter().any(|rule| {
            rule.result == RuleOutcome::Reject && rule.reason_code.as_deref() == Some(code)
        }),
        "missing reject code {code}: {}",
        verdict.rules_json()
    );
}

#[test]
fn factory_policy_hash_uses_contract_canonical_json() {
    let policy = factory_policy();
    assert!(policy.preconditions.require_data_snapshot);
    assert!(policy.preconditions.require_typed_protection);
    assert!(policy.preconditions.require_kill_switch);
    assert!(policy.per_trade.protection_required);
    let actual = policy_hash(&policy);
    assert_eq!(FACTORY_POLICY_HASH.trim(), actual);
}

#[test]
fn baseline_is_approved() {
    let verdict = evaluate(&base_input());
    assert_eq!(
        verdict.decision,
        Decision::Approve,
        "{}",
        verdict.rules_json()
    );
    assert_eq!(verdict.rules.len(), 27);
}

fn matching_certification() -> PromotionCertification {
    PromotionCertification {
        cert_id: "cert_live".to_string(),
        stage: "live".to_string(),
        config_hash: "c".repeat(64),
        prompt_hash: format!("sha256:{}", "p".repeat(64)),
        policy_version: "risk_policy_v3".to_string(),
        code_commit: "a".repeat(40),
        passed: true,
        invalidated_at_ms: None,
        certified_at_ms: 900_000,
    }
}

fn live_input_with_certification() -> Input {
    let mut input = base_input();
    let stage = input.stage_certification.as_mut().unwrap();
    stage.requested_stage = "live".to_string();
    stage.certifications = vec![matching_certification()];
    input
}

#[test]
fn stage_certified_live_requires_and_accepts_exact_active_passed_certification() {
    let mut input = live_input_with_certification();
    let verdict = evaluate(&input);
    let result = rule(&verdict, "stage_certified");
    assert_eq!(result.result, RuleOutcome::Pass);
    assert!(result
        .detail
        .as_deref()
        .unwrap()
        .contains("authorization_source=certification"));

    input
        .stage_certification
        .as_mut()
        .unwrap()
        .certifications
        .clear();
    assert_code(input, "STAGE_NOT_CERTIFIED");
}

#[test]
fn stage_certified_rejects_policy_version_mismatch() {
    let mut policy_mismatch = live_input_with_certification();
    policy_mismatch
        .stage_certification
        .as_mut()
        .unwrap()
        .policy_version = "risk_policy_v4".to_string();
    assert_code(policy_mismatch, "STAGE_NOT_CERTIFIED");
}

#[test]
fn stage_certified_rejects_stage_config_prompt_and_commit_mismatches() {
    let mut stage_mismatch = live_input_with_certification();
    stage_mismatch
        .stage_certification
        .as_mut()
        .unwrap()
        .certifications[0]
        .stage = "demo".to_string();

    let mut config_mismatch = live_input_with_certification();
    config_mismatch
        .stage_certification
        .as_mut()
        .unwrap()
        .config_hash = "d".repeat(64);

    let mut prompt_mismatch = live_input_with_certification();
    prompt_mismatch
        .stage_certification
        .as_mut()
        .unwrap()
        .prompt_hash = format!("sha256:{}", "q".repeat(64));

    let mut commit_mismatch = live_input_with_certification();
    commit_mismatch
        .stage_certification
        .as_mut()
        .unwrap()
        .code_commit = "b".repeat(40);

    for input in [
        stage_mismatch,
        config_mismatch,
        prompt_mismatch,
        commit_mismatch,
    ] {
        assert_code(input, "STAGE_NOT_CERTIFIED");
    }
}

#[test]
fn stage_certified_rejects_invalidated_certification() {
    let mut invalidated = live_input_with_certification();
    invalidated
        .stage_certification
        .as_mut()
        .unwrap()
        .certifications[0]
        .invalidated_at_ms = Some(999_000);
    assert_code(invalidated, "STAGE_NOT_CERTIFIED");
}

#[test]
fn stage_certified_rejects_failed_certification() {
    let mut failed = live_input_with_certification();
    failed.stage_certification.as_mut().unwrap().certifications[0].passed = false;
    assert_code(failed, "STAGE_NOT_CERTIFIED");
}

#[test]
fn stage_certified_demo_testnet_fallback_is_distinct_from_certification() {
    let mut input = base_input();
    let stage = input.stage_certification.as_mut().unwrap();
    stage.requested_stage = "demo".to_string();
    stage.venue_is_testnet = true;

    let venue_verdict = evaluate(&input);
    let venue_rule = rule(&venue_verdict, "stage_certified");
    assert_eq!(venue_rule.result, RuleOutcome::Pass);
    assert!(venue_rule
        .detail
        .as_deref()
        .unwrap()
        .contains("authorization_source=testnet_venue"));

    input
        .stage_certification
        .as_mut()
        .unwrap()
        .certifications
        .push(matching_certification());
    let certified_verdict = evaluate(&input);
    let certified_rule = rule(&certified_verdict, "stage_certified");
    assert_eq!(certified_rule.result, RuleOutcome::Pass);
    assert!(certified_rule
        .detail
        .as_deref()
        .unwrap()
        .contains("authorization_source=certification"));
    assert_ne!(venue_rule.detail, certified_rule.detail);
}

#[test]
fn stage_certified_mainnet_venue_without_certification_rejects_demo() {
    let mut input = base_input();
    let stage = input.stage_certification.as_mut().unwrap();
    stage.requested_stage = "demo".to_string();
    // Mainnet projects the fail-closed `venue_is_testnet=false` fact.
    stage.venue_is_testnet = false;
    stage.certifications.clear();

    let verdict = evaluate(&input);
    let result = rule(&verdict, "stage_certified");
    assert_eq!(result.result, RuleOutcome::Reject);
    assert_eq!(result.reason_code.as_deref(), Some("STAGE_NOT_CERTIFIED"));
    let detail = result.detail.as_deref().unwrap();
    assert!(detail.contains("authorization_source=none"));
    assert!(detail.contains("requested_stage=demo"));
    assert!(detail.contains("fallback_stage=paper"));
}

#[test]
fn stage_certified_missing_venue_without_certification_rejects_demo() {
    let mut input = base_input();
    let stage = input.stage_certification.as_mut().unwrap();
    stage.requested_stage = "demo".to_string();
    // An unconfigured venue is deliberately projected to the same fail-closed
    // fact as mainnet. The rustcore adapter separately guards this projection.
    stage.venue_is_testnet = false;
    stage.certifications.clear();

    let verdict = evaluate(&input);
    let result = rule(&verdict, "stage_certified");
    assert_eq!(result.result, RuleOutcome::Reject);
    assert_eq!(result.reason_code.as_deref(), Some("STAGE_NOT_CERTIFIED"));
    let detail = result.detail.as_deref().unwrap();
    assert!(detail.contains("authorization_source=none"));
    assert!(detail.contains("requested_stage=demo"));
    assert!(detail.contains("fallback_stage=paper"));
}

#[test]
fn stage_certified_testnet_venue_does_not_authorize_live_stages() {
    for requested_stage in ["live_small", "live"] {
        let mut input = base_input();
        let stage = input.stage_certification.as_mut().unwrap();
        stage.requested_stage = requested_stage.to_string();
        stage.venue_is_testnet = true;
        stage.certifications.clear();

        let uncertified_verdict = evaluate(&input);
        let uncertified = rule(&uncertified_verdict, "stage_certified");
        assert_eq!(uncertified.result, RuleOutcome::Reject, "{requested_stage}");
        assert_eq!(
            uncertified.reason_code.as_deref(),
            Some("STAGE_NOT_CERTIFIED"),
            "{requested_stage}"
        );
        let detail = uncertified.detail.as_deref().unwrap();
        assert!(
            detail.contains("authorization_source=none"),
            "{requested_stage}"
        );
        assert!(detail.contains("fallback_stage=demo"), "{requested_stage}");

        input
            .stage_certification
            .as_mut()
            .unwrap()
            .certifications
            .push(matching_certification());
        let certified_verdict = evaluate(&input);
        let certified = rule(&certified_verdict, "stage_certified");
        assert_eq!(certified.result, RuleOutcome::Pass, "{requested_stage}");
        assert!(
            certified
                .detail
                .as_deref()
                .unwrap()
                .contains("authorization_source=certification"),
            "{requested_stage}"
        );
    }
}

#[test]
fn stage_certified_skips_close_hold_reduce_only_protection_and_l3_flatten() {
    for (case, action, reduce_only) in [
        ("close", Action::CloseLong, false),
        ("protection", Action::CloseShort, true),
        ("l3_flatten", Action::CloseLong, true),
        ("hold", Action::Hold, false),
        ("explicit_reduce_only", Action::OpenLong, true),
    ] {
        let mut input = base_input();
        input.intent.action = action;
        input.intent.reduce_only = reduce_only;
        input.stage_certification.as_mut().unwrap().requested_stage = "live".to_string();
        let verdict = evaluate(&input);
        let result = rule(&verdict, "stage_certified");
        assert_eq!(result.result, RuleOutcome::Skipped, "{case}");
        assert_eq!(
            result.reason.as_deref(),
            Some("not_applicable_to_close_or_hold"),
            "{case}"
        );
    }
}

macro_rules! rejection_test {
    ($name:ident, $code:literal, $body:expr) => {
        #[test]
        fn $name() {
            let mut input = base_input();
            ($body)(&mut input);
            assert_code(input, $code);
        }
    };
}

rejection_test!(
    reject_code_data_quality_fail,
    "DATA_QUALITY_FAIL",
    |input: &mut Input| {
        input.data_snapshot.as_mut().unwrap().allow_open = false;
    }
);
rejection_test!(
    reject_code_data_snapshot_inconsistent,
    "DATA_SNAPSHOT_INCONSISTENT",
    |input: &mut Input| {
        input.data_snapshot.as_mut().unwrap().consistency_error =
            Some("snapshot identity mismatch".to_string());
    }
);
rejection_test!(
    reject_code_stale_risk_budget,
    "STALE_RISK_BUDGET",
    |input: &mut Input| {
        input.risk_budget.as_mut().unwrap().as_of_ms = 1;
    }
);
rejection_test!(
    reject_code_kill_switch_active,
    "KILL_SWITCH_ACTIVE",
    |input: &mut Input| {
        input.kill_switch.as_mut().unwrap().level = "pause_new".to_string();
    }
);
rejection_test!(
    reject_code_missing_protection,
    "MISSING_PROTECTION",
    |input: &mut Input| {
        input.policy.preconditions.require_typed_protection = true;
        input.intent.stop_loss_price = None;
    }
);
rejection_test!(reject_code_rr_too_low, "RR_TOO_LOW", |input: &mut Input| {
    input.intent.take_profit_price = Some(101.0);
});
rejection_test!(
    reject_code_tp_below_cost,
    "TP_BELOW_COST",
    |input: &mut Input| {
        input.policy.per_trade.min_rr = 0.0;
        input.intent.take_profit_price = Some(100.1);
    }
);

#[test]
fn real_typed_protection_below_min_rr_rejects_rr_too_low() {
    let mut input = base_input();
    input.intent.stop_loss_price = Some(95.0);
    input.intent.take_profit_price = Some(101.0);
    let verdict = evaluate(&input);
    let result = rule(&verdict, "min_rr");
    assert_eq!(verdict.decision, Decision::Reject);
    assert_eq!(result.result, RuleOutcome::Reject);
    assert_eq!(result.reason_code.as_deref(), Some("RR_TOO_LOW"));
}

#[test]
fn real_typed_protection_below_cost_line_rejects_tp_below_cost() {
    let mut input = base_input();
    input.policy.per_trade.min_rr = 0.0;
    input.intent.stop_loss_price = Some(95.0);
    input.intent.take_profit_price = Some(100.1);
    let verdict = evaluate(&input);
    let result = rule(&verdict, "cost_aware_tp");
    assert_eq!(verdict.decision, Decision::Reject);
    assert_eq!(result.result, RuleOutcome::Reject);
    assert_eq!(result.reason_code.as_deref(), Some("TP_BELOW_COST"));
}
rejection_test!(
    reject_code_conviction_too_low,
    "CONVICTION_TOO_LOW",
    |input: &mut Input| {
        input.intent.conviction = Some(0.1);
    }
);
rejection_test!(
    reject_code_leverage_exceeded,
    "LEVERAGE_EXCEEDED",
    |input: &mut Input| {
        input.policy.per_trade.max_leverage = 0;
    }
);
rejection_test!(
    reject_code_duplicate_active_entry,
    "DUPLICATE_ACTIVE_ENTRY",
    |input: &mut Input| {
        input.positions.push(Position {
            bot_uid: input.intent.bot_uid.clone(),
            symbol_key: input.intent.symbol_key.clone(),
            direction: "long".to_string(),
            state: "open".to_string(),
            notional_usdt: 100.0,
            correlation_group: Some("majors".to_string()),
            pending_retry: false,
        });
    }
);
rejection_test!(
    reject_code_notional_exceeded,
    "NOTIONAL_EXCEEDED",
    |input: &mut Input| {
        input.policy.portfolio.max_position_notional_usdt = 0.0;
    }
);
rejection_test!(
    reject_code_exposure_exceeded,
    "EXPOSURE_EXCEEDED",
    |input: &mut Input| {
        input.policy.portfolio.max_total_exposure_pct = 0.0;
    }
);
rejection_test!(
    reject_code_too_many_positions,
    "TOO_MANY_POSITIONS",
    |input: &mut Input| {
        input.policy.portfolio.max_concurrent_positions = 0;
    }
);
rejection_test!(
    reject_code_daily_loss_reached,
    "DAILY_LOSS_REACHED",
    |input: &mut Input| {
        input.account.as_mut().unwrap().today_realized_pnl_pct = Some(-0.04);
    }
);
rejection_test!(
    reject_code_drawdown_reached,
    "DRAWDOWN_REACHED",
    |input: &mut Input| {
        input.account.as_mut().unwrap().drawdown_pct = Some(0.20);
    }
);
rejection_test!(
    reject_code_equity_below_min,
    "EQUITY_BELOW_MIN",
    |input: &mut Input| {
        input.policy.account.min_equity_usdt = 20_000.0;
    }
);
rejection_test!(
    reject_code_intent_expired,
    "INTENT_EXPIRED",
    |input: &mut Input| {
        input.intent.valid_until_ms = Some(input.now_ms);
    }
);
rejection_test!(
    reject_code_reservation_conflict,
    "RESERVATION_CONFLICT",
    |input: &mut Input| {
        input.reservation.as_mut().unwrap().reserved_usdt = 5_000.0;
    }
);
rejection_test!(
    reject_code_symbol_disabled,
    "SYMBOL_DISABLED",
    |input: &mut Input| {
        input
            .policy
            .symbol_controls
            .disabled_for_open
            .push(SYMBOL.to_string());
    }
);
rejection_test!(
    reject_code_slippage_exceeded,
    "SLIPPAGE_EXCEEDED",
    |input: &mut Input| {
        input.intent.observed_slippage_bps = Some(16);
    }
);
rejection_test!(
    reject_code_degraded_credential,
    "DEGRADED_CREDENTIAL",
    |input: &mut Input| {
        input.credential_degraded = true;
    }
);
rejection_test!(
    reject_code_size_below_exchange_min,
    "SIZE_BELOW_EXCHANGE_MIN",
    |input: &mut Input| {
        input.exchange_filters.as_mut().unwrap().min_notional_usdt = 2_000.0;
    }
);

#[test]
fn promotion_reject_reason_coverage() {
    let mut cases: Vec<(&str, Input)> = Vec::new();
    let mut add = |code: &'static str, mutate: fn(&mut Input)| {
        let mut input = base_input();
        mutate(&mut input);
        cases.push((code, input));
    };
    add("STAGE_NOT_CERTIFIED", |input| {
        input.stage_certification.as_mut().unwrap().requested_stage = "live".to_string();
    });
    add("DATA_QUALITY_FAIL", |input| {
        input.data_snapshot.as_mut().unwrap().allow_open = false;
    });
    add("DATA_SNAPSHOT_INCONSISTENT", |input| {
        input.data_snapshot.as_mut().unwrap().consistency_error = Some("mismatch".to_string());
    });
    add("STALE_RISK_BUDGET", |input| {
        input.risk_budget.as_mut().unwrap().as_of_ms = 1;
    });
    add("KILL_SWITCH_ACTIVE", |input| {
        input.kill_switch.as_mut().unwrap().level = "pause_new".to_string();
    });
    add("MISSING_PROTECTION", |input| {
        input.intent.stop_loss_price = None;
    });
    add("RR_TOO_LOW", |input| {
        input.intent.take_profit_price = Some(101.0);
    });
    add("TP_BELOW_COST", |input| {
        input.policy.per_trade.min_rr = 0.0;
        input.intent.take_profit_price = Some(100.1);
    });
    add("CONVICTION_TOO_LOW", |input| {
        input.intent.conviction = Some(0.1);
    });
    add("LEVERAGE_EXCEEDED", |input| {
        input.policy.per_trade.max_leverage = 0;
    });
    add("DUPLICATE_ACTIVE_ENTRY", |input| {
        input.positions.push(Position {
            bot_uid: input.intent.bot_uid.clone(),
            symbol_key: input.intent.symbol_key.clone(),
            direction: "long".to_string(),
            state: "open".to_string(),
            notional_usdt: 100.0,
            correlation_group: Some("majors".to_string()),
            pending_retry: false,
        });
    });
    add("NOTIONAL_EXCEEDED", |input| {
        input.policy.portfolio.max_position_notional_usdt = 0.0;
    });
    add("EXPOSURE_EXCEEDED", |input| {
        input.policy.portfolio.max_total_exposure_pct = 0.0;
    });
    add("TOO_MANY_POSITIONS", |input| {
        input.policy.portfolio.max_concurrent_positions = 0;
    });
    add("DAILY_LOSS_REACHED", |input| {
        input.account.as_mut().unwrap().today_realized_pnl_pct = Some(-0.04);
    });
    add("DRAWDOWN_REACHED", |input| {
        input.account.as_mut().unwrap().drawdown_pct = Some(0.20);
    });
    add("EQUITY_BELOW_MIN", |input| {
        input.policy.account.min_equity_usdt = 20_000.0;
    });
    add("INTENT_EXPIRED", |input| {
        input.intent.valid_until_ms = Some(input.now_ms);
    });
    add("RESERVATION_CONFLICT", |input| {
        input.reservation.as_mut().unwrap().reserved_usdt = 5_000.0;
    });
    add("SYMBOL_DISABLED", |input| {
        input
            .policy
            .symbol_controls
            .disabled_for_open
            .push(SYMBOL.to_string());
    });
    add("SLIPPAGE_EXCEEDED", |input| {
        input.intent.observed_slippage_bps = Some(16);
    });
    add("DEGRADED_CREDENTIAL", |input| {
        input.credential_degraded = true;
    });
    add("SIZE_BELOW_EXCHANGE_MIN", |input| {
        input.exchange_filters.as_mut().unwrap().min_notional_usdt = 2_000.0;
    });

    let mut exercised = Vec::new();
    for (expected, input) in cases {
        let verdict = evaluate(&input);
        assert!(
            verdict.rules.iter().any(|rule| {
                rule.result == RuleOutcome::Reject && rule.reason_code.as_deref() == Some(expected)
            }),
            "missing reject code {expected}: {}",
            verdict.rules_json()
        );
        exercised.push(expected);
    }
    exercised.sort_unstable();
    exercised.dedup();
    println!(
        "PROMOTION_REJECT_COVERAGE_JSON={{\"codes\":{:?}}}",
        exercised
    );
}

fn assert_resize_cap(mut input: Input, cap: &str) {
    // Keep the exchange minimum below every deliberately small resized order.
    input.policy.per_trade.min_notional_usdt = 0.01;
    input.exchange_filters.as_mut().unwrap().min_notional_usdt = 0.01;
    let verdict = evaluate(&input);
    assert_eq!(
        verdict.decision,
        Decision::Resize,
        "{}",
        verdict.rules_json()
    );
    let sizing = verdict.sizing.expect("sizing");
    assert!(
        sizing.capped_by().iter().any(|item| item == cap),
        "missing {cap}: {sizing:?}"
    );
    assert!(sizing.caps().iter().any(|item| item.cap == cap));
}

#[test]
fn capped_stop_distance_clamp_resizes_and_audits_cap() {
    let mut input = base_input();
    input.intent.stop_loss_price = Some(99.9);
    input.intent.take_profit_price = Some(102.0);
    assert_resize_cap(input, "stop_distance_clamp");
}

#[test]
fn capped_max_leverage_resizes_and_audits_cap() {
    let mut input = base_input();
    input.intent.requested_leverage = 20;
    assert_resize_cap(input, "max_leverage");
}

#[test]
fn capped_max_position_notional_resizes_and_audits_cap() {
    let mut input = base_input();
    input.policy.portfolio.max_position_notional_usdt = 500.0;
    assert_resize_cap(input, "max_position_notional");
}

#[test]
fn capped_max_credential_notional_resizes_and_audits_cap() {
    let mut input = base_input();
    input.account.as_mut().unwrap().credential_notional_usdt = 4_900.0;
    assert_resize_cap(input, "max_credential_notional");
}

#[test]
fn capped_max_total_exposure_resizes_and_audits_cap() {
    let mut input = base_input();
    input.policy.portfolio.max_total_exposure_pct = 0.01;
    assert_resize_cap(input, "max_total_exposure");
}

#[test]
fn capped_correlated_group_exposure_resizes_and_audits_cap() {
    let mut input = base_input();
    input.policy.portfolio.max_correlated_group_exposure_pct = 0.01;
    assert_resize_cap(input, "correlated_group_exposure");
}

#[test]
fn sizing_max_pair_exposure_cap_is_recorded() {
    let mut input = base_input();
    input.policy.portfolio.max_position_notional_usdt = 100_000.0;
    input.policy.portfolio.max_pair_exposure_pct = 0.01;
    assert_resize_cap(input, "max_pair_exposure");
}

#[test]
fn sizing_available_margin_cap_is_recorded() {
    let mut input = base_input();
    input.policy.portfolio.max_position_notional_usdt = 100_000.0;
    input.account.as_mut().unwrap().available_margin_usdt = 10.0;
    assert_resize_cap(input, "available_margin");
}

#[test]
fn evaluates_all_rules_without_short_circuit() {
    let mut input = base_input();
    input.data_snapshot.as_mut().unwrap().allow_open = false;
    input.kill_switch.as_mut().unwrap().level = "manage_only".to_string();
    input.intent.valid_until_ms = Some(input.now_ms);
    input.credential_degraded = true;
    let verdict = evaluate(&input);
    let rejected = verdict
        .rules
        .iter()
        .filter(|rule| rule.result == RuleOutcome::Reject)
        .count();
    assert!(rejected >= 4, "{}", verdict.rules_json());
    assert_eq!(
        rule(&verdict, "data_quality_gate").result,
        RuleOutcome::Reject
    );
    assert_eq!(
        rule(&verdict, "kill_switch_level").result,
        RuleOutcome::Reject
    );
    assert_eq!(
        rule(&verdict, "intent_not_expired").result,
        RuleOutcome::Reject
    );
    assert_eq!(
        rule(&verdict, "credential_health").result,
        RuleOutcome::Reject
    );
}

#[test]
fn sizing_formula_raw_qty_and_floor_to_step() {
    let mut input = base_input();
    input.policy.portfolio.max_position_notional_usdt = 100_000.0;
    input.policy.portfolio.max_credential_notional_usdt = 100_000.0;
    input.policy.portfolio.max_total_exposure_pct = 1.0;
    input.policy.portfolio.max_pair_exposure_pct = 1.0;
    input.policy.portfolio.max_correlated_group_exposure_pct = 1.0;
    input.reservation.as_mut().unwrap().cap_usdt = 100_000.0;
    input.exchange_filters.as_mut().unwrap().lot_step = 0.3;
    let verdict = evaluate(&input);
    assert_eq!(verdict.decision, Decision::Resize);
    let sizing = verdict.sizing.unwrap();
    let Sizing::RiskBudget {
        risk_budget_usdt,
        computed_qty,
        final_qty,
        final_notional_usdt,
        capped_by,
        ..
    } = sizing
    else {
        panic!("typed protection must use risk-budget sizing");
    };
    assert_eq!(risk_budget_usdt, "50");
    assert_eq!(computed_qty, "10");
    assert_eq!(final_qty, "9.9");
    assert_eq!(final_notional_usdt, "990");
    assert!(capped_by.iter().any(|cap| cap == "floor_to_step"));
}

#[test]
fn require_typed_protection_missing_rejects() {
    let mut input = base_input();
    input.policy.preconditions.require_typed_protection = true;
    input.intent.stop_loss_price = None;
    input.intent.take_profit_price = None;
    assert_code(input, "MISSING_PROTECTION");
}

#[test]
fn absent_legacy_percentages_cannot_open_without_typed_protection() {
    let mut input = base_input();
    input.policy.preconditions.require_typed_protection = true;
    input.intent.stop_loss_price = None;
    input.intent.take_profit_price = None;

    let verdict = evaluate(&input);
    assert_eq!(verdict.decision, Decision::Reject);
    let protection = rule(&verdict, "protection_required");
    assert_eq!(protection.result, RuleOutcome::Reject);
    assert_eq!(
        protection.reason_code.as_deref(),
        Some("MISSING_PROTECTION")
    );
}

#[test]
fn optional_typed_protection_missing_skips_all_four_rules() {
    let mut input = base_input();
    input.policy.preconditions.require_typed_protection = false;
    input.intent.stop_loss_price = None;
    input.intent.take_profit_price = None;
    input.typed_protection_capable = false;
    input.legacy_sizing = Some(LegacySizingInput {
        quantity: 2.0,
        reason: "test wire has no typed protection fields".to_string(),
    });
    let verdict = evaluate(&input);
    assert_ne!(
        verdict.decision,
        Decision::Reject,
        "{}",
        verdict.rules_json()
    );
    for name in [
        "protection_required",
        "stop_distance_clamp",
        "min_rr",
        "cost_aware_tp",
    ] {
        let result = rule(&verdict, name);
        assert_eq!(result.result, RuleOutcome::Skipped, "rule={name}");
        assert_eq!(
            result.reason.as_deref(),
            Some(if name == "protection_required" {
                "wire_not_capable"
            } else {
                "input_absent"
            }),
            "rule={name}"
        );
    }
}

macro_rules! missing_switch_tests {
    ($reject_name:ident, $skip_name:ident, $field:ident, $input_field:ident, $rule:literal, $code:literal) => {
        #[test]
        fn $reject_name() {
            let mut input = base_input();
            input.policy.preconditions.$field = true;
            input.$input_field = None;
            assert_code(input, $code);
        }

        #[test]
        fn $skip_name() {
            let mut input = base_input();
            input.policy.preconditions.$field = false;
            input.$input_field = None;
            let verdict = evaluate(&input);
            assert_ne!(
                verdict.decision,
                Decision::Reject,
                "{}",
                verdict.rules_json()
            );
            let result = rule(&verdict, $rule);
            assert_eq!(result.result, RuleOutcome::Skipped);
            assert_eq!(result.reason.as_deref(), Some("input_absent"));
        }
    };
}

missing_switch_tests!(
    require_risk_budget_missing_rejects,
    optional_risk_budget_missing_is_skipped,
    require_risk_budget,
    risk_budget,
    "risk_budget_freshness",
    "STALE_RISK_BUDGET"
);

#[test]
fn required_data_snapshot_is_explicitly_inapplicable_to_frozen_v1_wire() {
    let mut input = base_input();
    input.data_snapshot_capable = false;
    input.data_snapshot = None;
    let verdict = evaluate(&input);
    assert_ne!(
        verdict.decision,
        Decision::Reject,
        "{}",
        verdict.rules_json()
    );
    let result = rule(&verdict, "data_quality_gate");
    assert_eq!(result.result, RuleOutcome::Skipped);
    assert_eq!(result.reason.as_deref(), Some("wire_not_capable"));
}
missing_switch_tests!(
    require_kill_switch_missing_rejects,
    optional_kill_switch_missing_is_skipped,
    require_kill_switch,
    kill_switch,
    "kill_switch_level",
    "KILL_SWITCH_ACTIVE"
);
missing_switch_tests!(
    require_data_snapshot_missing_rejects,
    optional_data_snapshot_missing_is_skipped,
    require_data_snapshot,
    data_snapshot,
    "data_quality_gate",
    "DATA_QUALITY_FAIL"
);
missing_switch_tests!(
    require_exchange_filters_missing_rejects,
    optional_exchange_filters_missing_is_skipped,
    require_exchange_filters,
    exchange_filters,
    "exchange_min_size",
    "SIZE_BELOW_EXCHANGE_MIN"
);

#[test]
fn close_is_approved_when_every_open_only_rule_would_reject() {
    let mut input = base_input();
    input.intent.action = Action::CloseLong;
    input.intent.stop_loss_price = None;
    input.intent.take_profit_price = None;
    input.intent.valid_until_ms = Some(0);
    input.intent.conviction = Some(0.0);
    input.data_snapshot.as_mut().unwrap().allow_open = false;
    input.risk_budget.as_mut().unwrap().as_of_ms = 0;
    input.kill_switch.as_mut().unwrap().level = "flatten".to_string();
    input.credential_degraded = true;
    input.account.as_mut().unwrap().today_realized_pnl_pct = Some(-1.0);
    input.account.as_mut().unwrap().drawdown_pct = Some(1.0);
    input.account.as_mut().unwrap().equity_usdt = 0.0;
    input.reservation.as_mut().unwrap().reserved_usdt = 99_999.0;
    input
        .policy
        .symbol_controls
        .disabled_for_open
        .push(SYMBOL.to_string());
    input.positions.push(Position {
        bot_uid: input.intent.bot_uid.clone(),
        symbol_key: input.intent.symbol_key.clone(),
        direction: "long".to_string(),
        state: "open".to_string(),
        notional_usdt: 1_000_000.0,
        correlation_group: Some("majors".to_string()),
        pending_retry: false,
    });
    input.churn = Some(ChurnState {
        position_age_minutes: 0,
        daily_loss_triggered: true,
    });

    let verdict = evaluate(&input);
    assert_eq!(
        verdict.decision,
        Decision::Approve,
        "{}",
        verdict.rules_json()
    );
    assert_eq!(
        rule(&verdict, "churn_guard").reason.as_deref(),
        Some("daily_loss_exemption")
    );
}

#[test]
fn identical_input_replays_to_identical_fingerprint() {
    let input = base_input();
    let first = evaluate(&input);
    let second = evaluate(&input);
    assert_eq!(first.replay_fingerprint(), second.replay_fingerprint());
    assert_eq!(first.rules_json(), second.rules_json());
    assert_eq!(first.sizing_json(), second.sizing_json());
}

// --- cost_aware_tp unit basis (risk-policy@1.1.0) --------------------------
// tp_pct and the round-trip cost both live on trade-notional basis; leverage
// scales numerator and cost by the same factor, so the threshold must not be
// multiplied by requested or capped leverage.

fn cost_case(mut input: Input, action: Action, lev: u32, tp: f64) -> Input {
    input.intent.action = action;
    input.intent.requested_leverage = lev;
    input.intent.take_profit_price = Some(tp);
    input.intent.stop_loss_price = Some(if action == Action::OpenShort {
        105.0
    } else {
        95.0
    });
    // Isolate the cost gate from the separate min_rr gate in threshold tests;
    // coefficient preservation is pinned by the boundary test below.
    input.policy.per_trade.min_rr = 0.0;
    input
}

#[test]
fn cost_aware_tp_threshold_is_notional_and_leverage_invariant_for_long_and_short() {
    // Factory notional threshold: 2.0 * (0.0004*2 + 0.0002) = 0.002 of notional.
    // A TP at 0.003 sits above it at every leverage; 0.001 sits below it at
    // every leverage. Restoring a leverage multiplier flips the lev>=3 pass
    // cases to reject; disabling the gate flips the reject cases.
    for lev in [1u32, 3, 10, 20] {
        for (action, above_tp, below_tp) in [
            (Action::OpenLong, 100.3, 100.1),
            (Action::OpenShort, 99.7, 99.9),
        ] {
            let pass_input = cost_case(base_input(), action, lev, above_tp);
            let verdict = evaluate(&pass_input);
            assert_eq!(
                rule(&verdict, "cost_aware_tp").result,
                RuleOutcome::Pass,
                "lev={lev} {action:?}: {}",
                verdict.rules_json()
            );
            assert!(
                verdict.reject_reason_code.is_none(),
                "lev={lev} {action:?}: {}",
                verdict.rules_json()
            );

            let reject_input = cost_case(base_input(), action, lev, below_tp);
            let verdict = evaluate(&reject_input);
            let result = rule(&verdict, "cost_aware_tp");
            assert_eq!(result.result, RuleOutcome::Reject, "lev={lev} {action:?}");
            assert_eq!(result.reason_code.as_deref(), Some("TP_BELOW_COST"));
        }
    }
}

#[test]
fn cost_aware_tp_pass_does_not_weaken_leverage_clamp() {
    // Requested 20 against policy max 10: the clamp must still apply and the
    // sizing must use 10 while the cost gate passes on the shared notional
    // basis. The two controls stay independent in both directions.
    let mut input = cost_case(base_input(), Action::OpenLong, 20, 100.3);
    let verdict = evaluate(&input);
    assert_eq!(rule(&verdict, "cost_aware_tp").result, RuleOutcome::Pass);
    let leverage = rule(&verdict, "max_leverage");
    assert_eq!(leverage.result, RuleOutcome::Capped);
    assert_eq!(leverage.detail.as_deref(), Some("20 -> 10"));
    let sizing = verdict.sizing.expect("sizing");
    let sized_leverage = match &sizing {
        Sizing::RiskBudget { leverage, .. } => *leverage,
        Sizing::LegacyClientQuantity { .. } => {
            panic!("typed protection must use risk-budget sizing")
        }
    };
    assert_eq!(sized_leverage, 10);
    assert!(sizing.capped_by().iter().any(|cap| cap == "max_leverage"));

    // The gate itself stays armed at the clamped leverage: a below-threshold
    // TP still rejects while the clamp outcome is unchanged.
    input.intent.take_profit_price = Some(100.1);
    let verdict = evaluate(&input);
    assert_eq!(rule(&verdict, "cost_aware_tp").result, RuleOutcome::Reject);
    assert_eq!(verdict.reject_reason_code.as_deref(), Some("TP_BELOW_COST"));
    assert_eq!(rule(&verdict, "max_leverage").result, RuleOutcome::Capped);
}

#[test]
fn cost_aware_tp_custom_coefficients_define_boundary_on_notional_basis() {
    // Custom coefficients: 4.0 * (0.001*2 + 0.0015) = 0.014 of notional,
    // independent of the requested leverage (3 here).
    let mut input = cost_case(base_input(), Action::OpenLong, 3, 101.4);
    input.policy.per_trade.fee_per_side = 0.001;
    input.policy.per_trade.slippage_round_trip = 0.0015;
    input.policy.per_trade.cost_aware_tp_multiple = 4.0;

    // Exactly at the boundary passes (inclusive comparison).
    let boundary = evaluate(&input);
    assert_eq!(
        rule(&boundary, "cost_aware_tp").result,
        RuleOutcome::Pass,
        "{}",
        boundary.rules_json()
    );

    // Just below the boundary rejects with the exact composed threshold, so
    // the fee x2, slippage, and multiple all remain in the formula.
    input.intent.take_profit_price = Some(100.0 * (1.014 - 0.00000001));
    let below = evaluate(&input);
    let result = rule(&below, "cost_aware_tp");
    assert_eq!(result.result, RuleOutcome::Reject);
    let detail = result.detail.as_deref().unwrap();
    assert!(detail.contains("required=0.01400000"), "{detail}");

    // The short side mirrors the same boundary on the shared notional basis.
    let mut short_input = cost_case(base_input(), Action::OpenShort, 3, 98.6);
    short_input.policy.per_trade.fee_per_side = 0.001;
    short_input.policy.per_trade.slippage_round_trip = 0.0015;
    short_input.policy.per_trade.cost_aware_tp_multiple = 4.0;
    let short_boundary = evaluate(&short_input);
    assert_eq!(
        rule(&short_boundary, "cost_aware_tp").result,
        RuleOutcome::Pass,
        "{}",
        short_boundary.rules_json()
    );
    short_input.intent.take_profit_price = Some(100.0 * (1.0 - 0.014 + 0.00000001));
    let short_below = evaluate(&short_input);
    assert_eq!(
        rule(&short_below, "cost_aware_tp").result,
        RuleOutcome::Reject
    );

    // Preserve the existing numerical tolerance: less than epsilon below
    // the notional boundary passes; more than epsilon below still rejects.
    let near = cost_case(
        base_input(),
        Action::OpenLong,
        20,
        100.0 * (1.002 - 0.5e-12),
    );
    assert_eq!(
        rule(&evaluate(&near), "cost_aware_tp").result,
        RuleOutcome::Pass
    );
    let outside = cost_case(
        base_input(),
        Action::OpenLong,
        20,
        100.0 * (1.002 - 2.0e-12),
    );
    assert_eq!(
        rule(&evaluate(&outside), "cost_aware_tp").result,
        RuleOutcome::Reject
    );

    // Factory coefficients are untouched: 2.0 * (0.0004*2 + 0.0002) = 0.002.
    let factory_boundary = cost_case(base_input(), Action::OpenLong, 3, 100.2);
    let verdict = evaluate(&factory_boundary);
    assert_eq!(
        rule(&verdict, "cost_aware_tp").result,
        RuleOutcome::Pass,
        "{}",
        verdict.rules_json()
    );
    let factory_below = cost_case(
        base_input(),
        Action::OpenLong,
        3,
        100.0 * (1.002 - 0.00000001),
    );
    let verdict = evaluate(&factory_below);
    let result = rule(&verdict, "cost_aware_tp");
    assert_eq!(result.result, RuleOutcome::Reject);
    assert!(result
        .detail
        .as_deref()
        .unwrap()
        .contains("required=0.00200000"));
}

#[test]
fn cost_aware_tp_observed_intent_requested_20_capped_10_now_passes() {
    // Actual observed input: entry 79670.3, stop 78865.875, TP 82052.375 (2.989916%), requested
    // leverage 20, factory policy. The leverage-multiplied threshold
    // (0.002 * 20 = 0.04) rejected this as TP_BELOW_COST; on the shared
    // notional basis (0.002) the cost gate passes with every cap still binding.
    assert_eq!(EVALUATOR_VERSION, "risk-policy@1.1.0");
    let mut input = base_input();
    input.intent.requested_leverage = 20;
    input.intent.entry_ref_price = Some(79670.3);
    input.intent.stop_loss_price = Some(78865.875);
    input.intent.take_profit_price = Some(82052.375);
    let verdict = evaluate(&input);

    assert!(
        verdict.reject_reason_code.is_none(),
        "{}",
        verdict.rules_json()
    );
    let cost = rule(&verdict, "cost_aware_tp");
    assert_eq!(cost.result, RuleOutcome::Pass);
    assert!(cost
        .detail
        .as_deref()
        .unwrap()
        .contains("tp_pct=0.02989916"));

    let leverage = rule(&verdict, "max_leverage");
    assert_eq!(leverage.result, RuleOutcome::Capped);
    assert_eq!(leverage.detail.as_deref(), Some("20 -> 10"));

    assert_eq!(verdict.decision, Decision::Resize);
    let sizing = verdict.sizing.expect("sizing");
    let Sizing::RiskBudget {
        leverage: sized_leverage,
        final_notional_usdt,
        capped_by,
        ..
    } = &sizing
    else {
        panic!("typed protection must use risk-budget sizing");
    };
    assert_eq!(*sized_leverage, 10);
    // No sizing or notional cap was weakened to let this pass: the risk-sized
    // quantity still uses the capped leverage and the 1000 USDT notional cap.
    assert!(capped_by.iter().any(|cap| cap == "max_leverage"));
    assert!(capped_by.iter().any(|cap| cap == "max_position_notional"));
    // This fixture retains a 0.001 exchange lot step. At the actual BTC
    // price, downward lot rounding leaves less than the 1000 notional cap.
    let notional: f64 = final_notional_usdt.parse().unwrap();
    assert!(notional <= 1000.0 && notional > 900.0, "{notional}");
}

// Round 2A keeps the factory policy unchanged; these exercise-only distances
// and economics are an explicitly named testnet policy, not strategy evidence.
fn round2_input(short: bool, stop_pct: f64, tp_pct: f64) -> Input {
    let mut input = base_input();
    input.policy = testnet_policy();
    input.policy_hash = policy_hash(&input.policy);
    input.intent.action = if short {
        Action::OpenShort
    } else {
        Action::OpenLong
    };
    input.intent.requested_leverage = 1;
    input.intent.entry_ref_price = Some(100.0);
    input.intent.stop_loss_price = Some(if short {
        100.0 * (1.0 + stop_pct)
    } else {
        100.0 * (1.0 - stop_pct)
    });
    input.intent.take_profit_price = Some(if short {
        100.0 * (1.0 - tp_pct)
    } else {
        100.0 * (1.0 + tp_pct)
    });
    let stage = input.stage_certification.as_mut().unwrap();
    stage.policy_version = TESTNET_POLICY_VERSION.into();
    stage.requested_stage = "demo".into();
    stage.venue_is_testnet = true;
    input
}

#[test]
fn round2_s2_s3_testnet_distances_are_not_clamped_and_caps_remain() {
    let factory_before = policy_hash(&factory_policy());
    assert_eq!(policy_hash(&testnet_policy()), TESTNET_POLICY_HASH.trim());
    for short in [false, true] {
        for (sl, tp) in [(0.0015, 0.03), (0.03, 0.0015)] {
            let input = round2_input(short, sl, tp);
            let verdict = evaluate(&input);
            assert_ne!(
                verdict.decision,
                Decision::Reject,
                "{}",
                verdict.rules_json()
            );
            assert_eq!(
                rule(&verdict, "stop_distance_clamp").result,
                RuleOutcome::Pass
            );
            let sizing = verdict.sizing.unwrap();
            assert!(!sizing
                .capped_by()
                .iter()
                .any(|cap| cap == "stop_distance_clamp"));
            let notional = sizing.final_notional_usdt().parse::<f64>().unwrap();
            assert!(
                notional <= 1000.0
                    && notional <= 0.15 * input.account.as_ref().unwrap().equity_usdt
            );
            assert_eq!(input.policy.per_trade.risk_per_trade_pct, 0.005);
            assert_eq!(input.policy.portfolio.max_position_notional_usdt, 1000.0);
            assert_eq!(input.policy.portfolio.max_pair_exposure_pct, 0.15);
        }
    }
    assert_eq!(policy_hash(&factory_policy()), factory_before);
    assert_eq!(factory_before, FACTORY_POLICY_HASH.trim());
    assert_eq!(factory_policy().per_trade.min_stop_distance_pct, 0.008);
    assert_eq!(factory_policy().per_trade.min_rr, 2.0);
    assert_eq!(factory_policy().per_trade.cost_aware_tp_multiple, 2.0);
}

#[test]
fn round2_testnet_policy_cannot_be_used_on_another_venue_or_stage_even_with_certification() {
    for (stage_name, testnet) in [
        ("paper", true),
        ("demo", false),
        ("live_small", true),
        ("live", false),
    ] {
        let mut input = round2_input(false, 0.03, 0.0015);
        let stage = input.stage_certification.as_mut().unwrap();
        stage.requested_stage = stage_name.into();
        stage.venue_is_testnet = testnet;
        let mut cert = matching_certification();
        cert.policy_version = TESTNET_POLICY_VERSION.into();
        stage.certifications.push(cert);
        assert_code(input, "TESTNET_POLICY_FORBIDDEN");
    }
    let mut input = round2_input(false, 0.03, 0.0015);
    input.stage_certification.as_mut().unwrap().policy_version = "risk_policy_v3".into();
    assert_code(input, "TESTNET_POLICY_FORBIDDEN");
}

#[test]
fn round2_testnet_out_of_range_stop_is_rejected_instead_of_silently_rewritten() {
    for distance in [0.0001, 0.08] {
        let input = round2_input(false, distance, 0.03);
        assert_code(input, "TESTNET_STOP_DISTANCE_OUT_OF_RANGE");
    }
}
