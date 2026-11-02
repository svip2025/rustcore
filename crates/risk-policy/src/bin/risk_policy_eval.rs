use std::io::{self, Read, Write};

use ai_trade_contracts::canonical_json;
use risk_policy::{evaluate, Input};

fn run() -> Result<(), String> {
    let mut input_bytes = Vec::new();
    io::stdin()
        .read_to_end(&mut input_bytes)
        .map_err(|error| format!("read risk-policy input from stdin: {error}"))?;
    let input: Input = serde_json::from_slice(&input_bytes)
        .map_err(|error| format!("parse risk-policy Input JSON: {error}"))?;
    let verdict = evaluate(&input);
    let value = serde_json::to_value(verdict)
        .map_err(|error| format!("serialize risk-policy verdict: {error}"))?;
    let output = canonical_json(&value);

    let mut stdout = io::stdout().lock();
    stdout
        .write_all(output.as_bytes())
        .and_then(|()| stdout.write_all(b"\n"))
        .map_err(|error| format!("write risk-policy verdict to stdout: {error}"))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("risk_policy_eval: {error}");
        std::process::exit(2);
    }
}
