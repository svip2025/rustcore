//! Real-binary rejection proofs. No internal startup helper replaces main.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::net::TcpListener;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn directory(label: &str) -> PathBuf {
    let root = std::env::var_os("STARTUP_ADMISSION_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = root.join(format!("startup-{label}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn command(dir: &Path, db: &Path, addr: &str) -> Command {
    let log = std::fs::File::create(dir.join("engine.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustcore"));
    command
        .env_clear()
        .env("RUSTCORE_DB_URL", format!("sqlite://{}", db.display()))
        .env("RUSTCORE_GRPC_ADDR", addr)
        .env("RUSTCORE_GRPC_AUTH_DISABLED", "1")
        .env("ENGINE_LIVE_ENABLED", "0")
        .env("RUST_LOG", "info")
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    command
}

fn store_fingerprint(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Some(Sha256::digest(bytes).to_vec()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("cannot observe owned test database: {error}"),
    }
}

async fn rejected(mut command: Command, dir: &Path, expected: &str) {
    let url = command
        .get_envs()
        .find(|(key, _)| *key == "RUSTCORE_DB_URL")
        .and_then(|(_, value)| value)
        .unwrap()
        .to_str()
        .unwrap();
    let db = PathBuf::from(url.strip_prefix("sqlite://").unwrap());
    let before = store_fingerprint(&db);
    let mut child = ChildGuard(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(8);
    let status: ExitStatus = loop {
        // Observe the contract while a wrongly admitted child is still alive;
        // a mutation must fail on real effects, not only a later timeout.
        assert_eq!(
            store_fingerprint(&db),
            before,
            "rejected startup mutated or created its store"
        );
        if let Some(status) = child.0.try_wait().unwrap() {
            assert_eq!(child.0.wait().unwrap(), status, "child must be reaped");
            break status;
        }
        let log = std::fs::read_to_string(dir.join("engine.log")).unwrap();
        assert!(
            !log.contains("rustcore starting"),
            "rejected startup entered serving: {log}"
        );
        assert!(
            Instant::now() < deadline,
            "rejected startup remained running; child_exit=None, store_unchanged=true, log={log}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(
        store_fingerprint(&db),
        before,
        "rejected startup changed its store before exit"
    );
    let log = std::fs::read_to_string(dir.join("engine.log")).unwrap();
    assert!(
        !status.success(),
        "invalid startup unexpectedly served: {log}"
    );
    assert!(log.contains(expected), "wrong startup rejection: {log}");
    assert!(
        !log.contains("rustcore starting"),
        "rejected startup reached serving"
    );
}

fn certificate(dir: &Path, label: &str) -> (PathBuf, PathBuf) {
    let cert = dir.join(format!("{label}.pem"));
    let key = dir.join(format!("{label}.key"));
    let output = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=startup-loopback",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .unwrap();
    assert!(output.status.success(), "generate isolated TLS fixture");
    (cert, key)
}

#[tokio::test]
async fn occupied_listener_rejects_before_creating_nested_store() {
    let dir = directory("occupied");
    let db = dir.join("not-created/nested/engine.db");
    let owner = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    rejected(
        command(&dir, &db, &owner.local_addr().unwrap().to_string()),
        &dir,
        "reserve gRPC listener failed",
    )
    .await;
    assert!(
        !dir.join("not-created").exists(),
        "rejected startup created store directories"
    );
}

#[tokio::test]
async fn invalid_auth_tls_and_address_reject_before_creating_store() {
    let root = directory("invalid-config");
    let (cert, key) = certificate(&root, "good");
    let (_, other_key) = certificate(&root, "other");
    let invalid = root.join("invalid.pem");
    std::fs::write(&invalid, "not a PEM certificate or key").unwrap();
    for case in [
        "auth",
        "address",
        "cert-only",
        "key-only",
        "ca-only",
        "empty-cert",
        "missing-cert",
        "invalid-cert",
        "invalid-key",
        "invalid-ca",
        "mismatched-key",
    ] {
        let dir = root.join(case);
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("absent/engine.db");
        let mut child = command(&dir, &db, "127.0.0.1:0");
        let expected = match case {
            "auth" => {
                child
                    .env("RUSTCORE_GRPC_ADDR", "0.0.0.0:0")
                    .env_remove("RUSTCORE_GRPC_AUTH_DISABLED");
                "gRPC auth required"
            }
            "address" => {
                child.env("RUSTCORE_GRPC_ADDR", "invalid address");
                "invalid RUSTCORE_GRPC_ADDR"
            }
            "cert-only" => {
                child.env("RUSTCORE_GRPC_TLS_CERT_PATH", &cert);
                "gRPC TLS requires both"
            }
            "key-only" => {
                child.env("RUSTCORE_GRPC_TLS_KEY_PATH", &key);
                "gRPC TLS requires both"
            }
            "ca-only" => {
                child.env("RUSTCORE_GRPC_TLS_CLIENT_CA_PATH", &cert);
                "gRPC TLS requires both"
            }
            "empty-cert" => {
                child.env("RUSTCORE_GRPC_TLS_CERT_PATH", "");
                "configured gRPC TLS path must not be empty"
            }
            other => {
                child
                    .env("RUSTCORE_GRPC_TLS_CERT_PATH", &cert)
                    .env("RUSTCORE_GRPC_TLS_KEY_PATH", &key);
                match other {
                    "missing-cert" => {
                        child.env("RUSTCORE_GRPC_TLS_CERT_PATH", root.join("missing.pem"));
                    }
                    "invalid-cert" => {
                        child.env("RUSTCORE_GRPC_TLS_CERT_PATH", &invalid);
                    }
                    "invalid-key" => {
                        child.env("RUSTCORE_GRPC_TLS_KEY_PATH", &invalid);
                    }
                    "invalid-ca" => {
                        child.env("RUSTCORE_GRPC_TLS_CLIENT_CA_PATH", &invalid);
                    }
                    "mismatched-key" => {
                        child.env("RUSTCORE_GRPC_TLS_KEY_PATH", &other_key);
                    }
                    _ => unreachable!(),
                }
                if other == "missing-cert" {
                    "read gRPC TLS certificate failed"
                } else {
                    if other == "invalid-ca" {
                        "validate gRPC TLS client CA failed"
                    } else {
                        "validate gRPC TLS identity failed"
                    }
                }
            }
        };
        rejected(child, &dir, expected).await;
        assert!(
            !dir.join("absent").exists(),
            "{case} created a database directory"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_seeded_recovery_performs_no_exchange_io_or_store_mutation() {
    let root = directory("seeded-rejection");
    sqlx::any::install_default_drivers();
    let accepts = Arc::new(AtomicUsize::new(0));
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = probe.local_addr().unwrap();
    let count = accepts.clone();
    let accept_task = tokio::spawn(async move {
        while let Ok((_stream, _)) = probe.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    let owner = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    for case in ["occupied", "auth", "tls"] {
        let dir = root.join(case);
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("engine.db");
        std::fs::File::create(&db).unwrap();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO fill_recovery_scopes (credential_id,symbol,registered_at_ms,updated_at_ms,scope_known,scope_proved_at_ms) VALUES ('startup','BTCUSDT',1,1,1,1)").execute(&pool).await.unwrap();
        pool.close().await;
        let before = std::fs::read(&db).unwrap();
        let mut child = command(&dir, &db, "127.0.0.1:0");
        child
            .env("BINANCE_API_KEY_STARTUP", "isolated-startup-key")
            .env("BINANCE_API_SECRET_STARTUP", "isolated-startup-secret")
            .env("BINANCE_FUTURES_BASE_URL", format!("http://{upstream}"))
            .env("BINANCE_FUTURES_WS_BASE_URL", format!("ws://{upstream}"));
        let expected = match case {
            "occupied" => {
                child.env(
                    "RUSTCORE_GRPC_ADDR",
                    owner.local_addr().unwrap().to_string(),
                );
                "reserve gRPC listener failed"
            }
            "auth" => {
                child
                    .env("RUSTCORE_GRPC_ADDR", "0.0.0.0:0")
                    .env_remove("RUSTCORE_GRPC_AUTH_DISABLED");
                "gRPC auth required"
            }
            "tls" => {
                child.env("RUSTCORE_GRPC_TLS_CERT_PATH", "incomplete");
                "gRPC TLS requires both"
            }
            _ => unreachable!(),
        };
        rejected(child, &dir, expected).await;
        assert_eq!(
            std::fs::read(&db).unwrap(),
            before,
            "{case} changed the seeded database"
        );
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        0,
        "rejected startup reached the exchange transport"
    );
    accept_task.abort();
    let _ = accept_task.await;
}

#[cfg(not(feature = "l4-fault-injection"))]
#[tokio::test]
async fn production_build_cannot_bypass_admission_with_l4_environment() {
    let dir = directory("production-l4-denied");
    let db = dir.join("absent/engine.db");
    let owner = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = command(&dir, &db, &owner.local_addr().unwrap().to_string());
    child
        .env("L4_FILE_RPC_DIR", dir.join("file-rpc"))
        .env("L4_FAKE_EXCHANGE_DB", dir.join("fake.db"))
        .env("L4_FAULT_INJECTION_ENABLED", "1");
    rejected(child, &dir, "reserve gRPC listener failed").await;
    assert!(!dir.join("absent").exists());
    assert!(!dir.join("file-rpc").exists());
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test]
async fn l4_file_transport_rejects_invalid_config_before_store() {
    let root = directory("invalid-l4");
    for case in ["disabled", "relative", "file-instead-of-directory"] {
        let dir = root.join(case);
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("absent/engine.db");
        let rpc = dir.join("rpc");
        let mut child = command(&dir, &db, "invalid socket address unused by file transport");
        child
            .env("L4_FILE_RPC_DIR", &rpc)
            .env("L4_FAKE_EXCHANGE_DB", dir.join("fake.db"))
            .env("L4_FAULT_INJECTION_ENABLED", "1");
        let expected = match case {
            "disabled" => {
                child.env_remove("L4_FAULT_INJECTION_ENABLED");
                "L4_FAULT_INJECTION_ENABLED=1 is required"
            }
            "relative" => {
                child.env("L4_FILE_RPC_DIR", "relative-rpc");
                "L4_FILE_RPC_DIR must be absolute"
            }
            "file-instead-of-directory" => {
                std::fs::write(&rpc, "owned ordinary file").unwrap();
                "prepare L4 file RPC directory failed"
            }
            _ => unreachable!(),
        };
        rejected(child, &dir, expected).await;
        assert!(
            !dir.join("absent").exists(),
            "invalid L4 config created the store"
        );
    }
}

#[cfg(feature = "l4-fault-injection")]
#[tokio::test]
async fn l4_file_transport_serves_with_invalid_grpc_configuration() {
    use prost::Message;
    mod pb {
        tonic::include_proto!("execution_engine.v2");
    }
    let dir = directory("valid-l4");
    let db = dir.join("nested/engine.db");
    let rpc = dir.join("rpc");
    let mut process = command(&dir, &db, "invalid socket address unused by file transport");
    process
        .env("L4_FILE_RPC_DIR", &rpc)
        .env("L4_FAKE_EXCHANGE_DB", dir.join("fake.db"))
        .env("L4_FAULT_INJECTION_ENABLED", "1")
        .env(
            "RUSTCORE_GRPC_TLS_CERT_PATH",
            "unused incomplete TLS config",
        );
    let mut child = ChildGuard(process.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let exit = child.0.try_wait().unwrap();
        assert_eq!(
            exit,
            None,
            "legal L4 startup exited: {}",
            std::fs::read_to_string(dir.join("engine.log")).unwrap()
        );
        if rpc.join("ready").exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "live L4 child did not publish readiness"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(db.is_file(), "legal L4 startup must initialize the store");
    let request = pb::GetPositionStatesRequest {
        credential_id: "isolated-l4".into(),
        ..Default::default()
    };
    std::fs::write(rpc.join("probe.tmp"), request.encode_to_vec()).unwrap();
    std::fs::rename(
        rpc.join("probe.tmp"),
        rpc.join("probe.v2_get_position_states.req"),
    )
    .unwrap();
    loop {
        let exit = child.0.try_wait().unwrap();
        assert_eq!(exit, None, "legal L4 child exited during RPC");
        assert!(
            !rpc.join("probe.err").exists(),
            "L4 RPC rejected: {}",
            std::fs::read_to_string(rpc.join("probe.err")).unwrap_or_default()
        );
        if let Ok(bytes) = std::fs::read(rpc.join("probe.ok")) {
            let response = pb::GetPositionStatesResponse::decode(bytes.as_slice()).unwrap();
            assert!(response.positions.is_empty());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "live L4 child did not complete RPC"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert!(!status.success());
    assert_eq!(child.0.try_wait().unwrap(), Some(status));
}
