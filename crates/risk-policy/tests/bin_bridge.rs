use std::io::Write;
use std::process::{Command, Stdio};

use ai_trade_contracts::canonical_json;
use risk_policy::*;

const SYMBOL: &str = "binance|futures|BTC|USDT|USDT";

fn base_input() -> Input {
    let policy = factory_policy();
    Input {
        policy_hash: policy_hash(&policy),
        policy,
        intent: Intent {
            intent_id: "int_bridge".to_string(),
            bot_uid: "bot_bridge".to_string(),
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

fn canonical_input(input: &Input) -> Vec<u8> {
    let value = serde_json::to_value(input).expect("input serializes");
    format!("{}\n", canonical_json(&value)).into_bytes()
}

fn direct_verdict(input: &Input) -> Vec<u8> {
    let value = serde_json::to_value(evaluate(input)).expect("verdict serializes");
    format!("{}\n", canonical_json(&value)).into_bytes()
}

fn bin_verdict(input_bytes: &[u8]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_risk_policy_eval"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("risk-policy evaluator bin starts");
    child
        .stdin
        .take()
        .expect("stdin pipe")
        .write_all(input_bytes)
        .expect("write evaluator input");
    child.wait_with_output().expect("wait for evaluator")
}

#[test]
fn bin_and_library_are_byte_identical_for_the_same_input_batch() {
    let approved = base_input();
    let mut rejected = base_input();
    rejected.data_snapshot.as_mut().unwrap().allow_open = false;
    let mut resized = base_input();
    resized.intent.requested_leverage = 20;

    for input in [&approved, &rejected, &resized] {
        let output = bin_verdict(&canonical_input(input));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stderr, b"");
        assert_eq!(output.stdout, direct_verdict(input));
    }
}

#[test]
fn invalid_input_exits_nonzero_without_a_default_verdict() {
    let output = bin_verdict(br#"{"intent":"not an Input"}"#);

    assert!(!output.status.success());
    assert_eq!(output.stdout, b"");
    assert!(String::from_utf8_lossy(&output.stderr).contains("parse risk-policy Input JSON"));
}
