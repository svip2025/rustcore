//! The actual FastAPI route, unmodified Python gRPC client and compiled Rust
//! executable share one isolated SQLite fixture and loopback exchange.
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

fn python_on_path(name: &OsStr) -> Option<PathBuf> {
    let cwd = std::env::current_dir().expect("test working directory");
    std::env::var_os("PATH").and_then(|value| {
        std::env::split_paths(&value)
            .map(|directory| {
                if directory.is_absolute() {
                    directory.join(name)
                } else {
                    cwd.join(directory).join(name)
                }
            })
            .find(|candidate| candidate.is_file())
    })
}

fn test_python(root: &Path) -> PathBuf {
    // Keep the venv executable path intact: canonicalizing its symlink to the
    // system Python would discard the selected environment's installed modules.
    if let Some(override_path) = std::env::var_os("RUSTCORE_TEST_PYTHON") {
        assert!(!override_path.is_empty(), "RUSTCORE_TEST_PYTHON is empty");
        let path = PathBuf::from(&override_path);
        return if path.is_absolute() {
            path
        } else if path.components().count() > 1 {
            std::env::current_dir()
                .expect("test working directory")
                .join(path)
        } else {
            python_on_path(&override_path)
                .expect("RUSTCORE_TEST_PYTHON executable must exist on the caller's PATH")
        };
    }
    for directory in [".venv-gate", ".venv"] {
        let candidate = root.join(directory).join("bin/python");
        if candidate.is_file() {
            return candidate;
        }
    }
    python_on_path(OsStr::new("python3"))
        .or_else(|| python_on_path(OsStr::new("python")))
        .expect("real-chain test requires Python on the caller's PATH")
}

async fn run(case: &str) {
    let root = PathBuf::from(
        std::env::var_os("AI_TRADE_PROTOTYPE_ROOT")
            .expect("AI_TRADE_PROTOTYPE_ROOT must point at an ai-trade-prototype checkout"),
    );
    let python = test_python(&root);
    println!("real-chain interpreter: {}", python.display());
    let base = std::env::var_os("GET_ORDER_BUDGET_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("get-order-budget-{case}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("fixture.db");
    std::fs::File::create(&db).unwrap();
    sqlx::any::install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool.close().await;
    let output = std::process::Command::new(python)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("ORCH_SCOPED_ENV", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg(root.join("tests/gate/get-order-budget/test_real_chain.py"))
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_rustcore"))
        .arg("--directory")
        .arg(&dir)
        .arg("--case")
        .arg(case)
        .current_dir(&root)
        .output()
        .unwrap();
    std::fs::write(dir.join("python.stdout.log"), &output.stdout).unwrap();
    std::fs::write(dir.join("python.stderr.log"), &output.stderr).unwrap();
    println!("real-chain evidence: {}", dir.display());
    assert!(
        output.status.success(),
        "real-chain {case}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn get_order_budget_real_chain_cold_five() {
    run("cold-five").await;
}

#[tokio::test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn get_order_budget_real_chain_zero_positions() {
    run("zero").await;
}

#[tokio::test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn get_order_budget_real_chain_one_position() {
    run("one").await;
}

#[tokio::test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn get_order_budget_real_chain_same_route_other_credential() {
    run("same-route").await;
}

#[tokio::test]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn get_order_budget_real_chain_closed_and_simulation_excluded() {
    run("excluded").await;
}
