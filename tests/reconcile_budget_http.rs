//! Real management HTTP entry, Python gRPC client, authenticated Rust and SQLite.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod pb {
    tonic::include_proto!("execution_engine.v2");
}

// C-core can turn user metadata "1n" into "1m" on a reused Python channel.
// Keep tonic's gRPC codec, but use hyper directly: tonic Channel interprets the
// header as a client deadline and can cancel before HEADERS reach the recorder.
// The fixture verifies actual HTTP/2 bytes; the independent watchdog stays 95s.
fn drive_exact_header(evidence: &Path, child: &mut Child) -> anyhow::Result<()> {
    use prost::Message;
    let ready = evidence.join("rpc-ready.json");
    let started = Instant::now();
    while !ready.exists() {
        anyhow::ensure!(
            child.try_wait()?.is_none(),
            "fixture exited before RPC ready"
        );
        anyhow::ensure!(
            started.elapsed() < Duration::from_secs(35),
            "RPC ready timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let input: serde_json::Value = serde_json::from_slice(&std::fs::read(ready)?)?;
    let response = tokio::runtime::Runtime::new()?.block_on(async {
        let mut connector = hyper::client::HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let transport = hyper::Client::builder()
            .http2_only(true)
            .build::<_, tonic::body::BoxBody>(connector);
        let mut client =
            tonic::client::Grpc::with_origin(transport, input["uri"].as_str().unwrap().parse()?);
        client.ready().await?;
        let bytes = hex::decode(input["request_hex"].as_str().unwrap())?;
        let mut request = tonic::Request::new(pb::ReconcileNowRequest::decode(bytes.as_slice())?);
        request.metadata_mut().insert(
            "authorization",
            input["authorization"].as_str().unwrap().parse()?,
        );
        request.metadata_mut().insert(
            "grpc-timeout",
            input["timeout_header"].as_str().unwrap().parse()?,
        );
        let rpc = client.unary::<_, pb::ReconcileNowResponse, _>(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/execution_engine.v2.EngineExecution/ReconcileNow",
            ),
            tonic::codec::ProstCodec::default(),
        );
        let response = match tokio::time::timeout(Duration::from_secs(95), rpc).await? {
            Ok(response) => {
                serde_json::json!({"status":"OK", "completed":response.into_inner().completed})
            }
            Err(error) => serde_json::json!({"status":match error.code() {
                tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
                tonic::Code::Cancelled => "CANCELLED",
                _ => "UNEXPECTED_ERROR",
            }, "error":error.message()}),
        };
        Ok::<_, anyhow::Error>(response)
    })?;
    std::fs::write(
        evidence.join("rpc-response.tmp"),
        serde_json::to_vec(&response)?,
    )?;
    std::fs::rename(
        evidence.join("rpc-response.tmp"),
        evidence.join("rpc-response.json"),
    )?;
    Ok(())
}

fn python(repo: &Path) -> PathBuf {
    let candidates = if let Some(value) = std::env::var_os("RUSTCORE_TEST_PYTHON") {
        vec![PathBuf::from(value)]
    } else {
        vec![
            repo.join(".venv/bin/python"),
            repo.join(".venv-gate/bin/python"),
            PathBuf::from("python3"),
            PathBuf::from("python"),
        ]
    };
    for candidate in &candidates {
        if isolated_python(candidate)
            .args([
                "-c",
                "import sys,configparser; from pathlib import Path; p=Path(sys.argv[1]); c=configparser.ConfigParser(); c.read(p/'pytest.ini'); sys.path[:0]=[str(p/x) for x in c['pytest']['pythonpath'].split()]; import grpc,uvicorn; import execution_orchestrator.http.server",
            ])
            .arg(repo)
            .output()
            .is_ok_and(|r| r.status.success())
        {
            return candidate.clone();
        }
    }
    panic!("K08 real-chain fixture needs repository Python dependencies. Set RUSTCORE_TEST_PYTHON to a suitable interpreter; checked {candidates:?}");
}

fn isolated_python(path: &Path) -> Command {
    let mut command = Command::new(path);
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("ORCH_SCOPED_ENV", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1");
    command
}

fn exercise(case: &str) {
    let repo_root = PathBuf::from(
        std::env::var_os("AI_TRADE_PROTOTYPE_ROOT")
            .expect("AI_TRADE_PROTOTYPE_ROOT must point at an ai-trade-prototype checkout"),
    );
    let repo = repo_root.as_path();
    let base = std::env::var_os("K08_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let evidence = base.join(format!("k08-{case}-{}", ulid::Ulid::new()));
    let mut command = isolated_python(&python(repo));
    command
        .arg(repo.join("tests/gate/reconcile-budget/run_fixture.py"))
        .arg("--repo")
        .arg(repo)
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_rustcore"))
        .arg("--case")
        .arg(case)
        .arg("--evidence")
        .arg(&evidence)
        .env("ORCH_SCOPED_ENV", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1");
    let exact_header = matches!(case, "zero_header" | "tiny_header" | "invalid_header");
    if exact_header {
        command.arg("--external-header-driver");
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start real K08 fixture");
    let driver = if exact_header {
        drive_exact_header(&evidence, &mut child)
    } else {
        Ok(())
    };
    // The fixture has its own bounded wait and owns all cleanup even if the driver fails.
    let result = child.wait_with_output().expect("wait for K08 fixture");
    eprintln!(
        "K08 {case}: {} evidence={}",
        String::from_utf8_lossy(&result.stdout),
        evidence.display()
    );
    assert!(
        result.status.success(),
        "fixture failed: {}; evidence={}",
        String::from_utf8_lossy(&result.stderr),
        evidence.display()
    );
    driver.expect("exact header driver");
}

#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r01_default_cold_five_positions() {
    exercise("default_five");
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r01_zero_one_and_hot_controls() {
    for case in ["zero", "one", "hot"] {
        exercise(case);
    }
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r02_server_cap_without_grpc_header() {
    exercise("server_cap");
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r02_zero_tiny_invalid_headers() {
    for case in ["zero_header", "tiny_header", "invalid_header"] {
        exercise(case);
    }
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r03_short_budget_stage_matrix() {
    for case in ["admission", "http", "ordinary", "protection", "db"] {
        exercise(case);
    }
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r01_r05_real_missing_protection_and_owned_confirmation() {
    for case in ["recovery", "recovery_cancel"] {
        exercise(case);
    }
}
#[test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
fn k08_r05_signed_unsigned_settlement_outlives_rpc() {
    for case in [
        "settlement_signed_429",
        "settlement_signed_418",
        "settlement_unsigned_429",
        "settlement_unsigned_418",
    ] {
        exercise(case);
    }
}
