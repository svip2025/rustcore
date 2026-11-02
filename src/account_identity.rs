//! Configuration identity admission, not exchange-account canonicalization.
//! Keys only live in private values here; diagnostics contain credential IDs.
use std::collections::BTreeSet;

use sqlx::Row;

use crate::binance_futures::{keys_from_env, BinanceKeys};
use crate::store::Store;
use crate::venue::{CredentialKind, RestTransport, TransportMode, Venue};

pub(crate) const CONFLICT_CODE: &str = "BINANCE_API_IDENTITY_CONFLICT";
pub(crate) const UNAVAILABLE_CODE: &str = "BINANCE_API_IDENTITY_UNAVAILABLE";

pub(crate) fn denial_code(reason: &str) -> &'static str {
    if reason.starts_with(CONFLICT_CODE) {
        CONFLICT_CODE
    } else {
        UNAVAILABLE_CODE
    }
}

// Deliberately no Debug/Display implementation: the endpoint or key can contain
// confidential configuration. Neither its values nor a digest leave this module.
#[derive(PartialEq, Eq)]
struct Domain {
    venue: Option<Venue>,
    mode: TransportMode,
    endpoint: String,
}

impl Domain {
    fn from_transport(transport: RestTransport) -> Result<Self, &'static str> {
        let mut endpoint = url::Url::parse(&transport.base_url).map_err(|_| UNAVAILABLE_CODE)?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err(UNAVAILABLE_CODE);
        }
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        let venue = transport.venue.or_else(|| match endpoint.host_str() {
            Some("demo-fapi.binance.com" | "testnet.binancefuture.com") => Some(Venue::Testnet),
            Some("fapi.binance.com") => Some(Venue::Mainnet),
            _ => None,
        });
        Ok(Self {
            venue,
            mode: transport.mode,
            endpoint: endpoint.as_str().trim_end_matches('/').to_owned(),
        })
    }
}

struct Identity {
    credential_id: String,
    keys: BinanceKeys,
    domain: Domain,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Conflict {
    pub(crate) credential_ids: Vec<String>,
    pub(crate) environment: &'static str,
    pub(crate) transport: &'static str,
    pub(crate) secret_mismatch: bool,
}

impl Conflict {
    pub(crate) fn reason(&self) -> String {
        format!(
            "{CONFLICT_CODE}: environment={} transport={} kind={} credentials={}",
            self.environment,
            self.transport,
            if self.secret_mismatch {
                "secret_mismatch"
            } else {
                "duplicate_api_identity"
            },
            self.credential_ids.join(",")
        )
    }

    fn report(&self) {
        tracing::warn!(
            error_code = CONFLICT_CODE,
            credential_ids = ?self.credential_ids,
            environment = self.environment,
            transport = self.transport,
            secret_mismatch = self.secret_mismatch,
            evaluation = "current_runtime_configuration",
            "duplicate Binance API configuration; new risk is refused for every member"
        );
    }
}

fn configured_credentials() -> BTreeSet<String> {
    std::env::vars_os()
        .filter_map(|(name, _)| {
            let name = name.to_str()?;
            let suffix = ["BINANCE_API_KEY_", "BINANCE_API_SECRET_", "BINANCE_VENUE_"]
                .iter()
                .find_map(|prefix| name.strip_prefix(prefix))?;
            let credential =
                String::from_utf8(hex::decode(suffix.strip_prefix('H')?).ok()?).ok()?;
            // Only the exact reversible locator consumed by the key resolver is
            // configuration. Legacy lossy names must not invent extra members.
            (crate::venue::credential_secret_locator(&credential) == suffix
                && crate::venue::credential_kind(&credential) == CredentialKind::ExchangeBacked)
                .then_some(credential)
        })
        .collect()
}

async fn identities(store: &Store, requested: Option<&str>) -> Result<Vec<Identity>, &'static str> {
    let mut credentials = configured_credentials();
    // Include durable scopes even when they currently use global fallback keys,
    // and before any user-stream/reconciler task has been started. This is not a
    // first-registration-wins task map. Simulation identities never resolve keys.
    // SQL UNION/DISTINCT can fold case or trailing spaces under MySQL's column
    // collation. Group by the exact byte encoding within each table and perform
    // final member deduplication in Rust; return the original credential bytes.
    let query = [
        "engine_orders",
        "position_state",
        "protection_orders",
        "engine_fills",
        "engine_credential_health",
        "risk_budget",
        "fill_recovery_scopes",
        "fill_replay_cursors",
        "fill_stream_state",
        "position_protection_plans",
        "market_stream_subscriptions",
        "risk_verdicts",
        "engine_checkpoints",
    ]
    .iter()
    .map(|table| {
        format!(
            "SELECT MIN(credential_id) AS credential_id FROM {table} GROUP BY HEX(credential_id)"
        )
    })
    .collect::<Vec<_>>()
    .join(" UNION ALL ");
    let rows = sqlx::query(&query)
        .fetch_all(store.pool())
        .await
        .map_err(|_| UNAVAILABLE_CODE)?;
    for row in rows {
        credentials.insert(
            row.try_get::<String, _>("credential_id")
                .map_err(|_| UNAVAILABLE_CODE)?,
        );
    }
    if let Some(requested) = requested {
        credentials.insert(requested.to_owned());
    }
    let mut identities = Vec::new();
    for credential_id in credentials {
        let Some(keys) = keys_from_env(&credential_id).map_err(|_| UNAVAILABLE_CODE)? else {
            continue;
        };
        let domain = Domain::from_transport(
            crate::venue::rest_transport_for_credential(&credential_id)
                .map_err(|_| UNAVAILABLE_CODE)?,
        )?;
        identities.push(Identity {
            credential_id,
            keys,
            domain,
        });
    }
    Ok(identities)
}

fn conflict_for(identities: &[Identity], credential_id: &str) -> Option<Conflict> {
    let identity = identities
        .iter()
        .find(|value| value.credential_id == credential_id)?;
    let members = identities
        .iter()
        .filter(|other| {
            other.domain == identity.domain && other.keys.api_key == identity.keys.api_key
        })
        .collect::<Vec<_>>();
    if members.len() < 2 {
        return None;
    }
    Some(Conflict {
        credential_ids: members
            .iter()
            .map(|member| member.credential_id.clone())
            .collect(),
        environment: match identity.domain.venue {
            Some(Venue::Testnet) => "testnet",
            Some(Venue::Mainnet) => "mainnet",
            None => "custom_transport",
        },
        transport: identity.domain.mode.as_str(),
        secret_mismatch: members
            .iter()
            .any(|other| other.keys.api_secret != identity.keys.api_secret),
    })
}

/// Recompute on every admission. Ready/reconcile writes and restarts cannot
/// clear a still-present conflict. Different keys prove no account separation.
pub(crate) async fn entry_denial(store: &Store, credential_id: &str) -> Option<String> {
    match identities(store, Some(credential_id)).await {
        Ok(identities) => conflict_for(&identities, credential_id).map(|conflict| {
            conflict.report();
            conflict.reason()
        }),
        Err(code) => {
            tracing::warn!(
                error_code = code,
                credential_id,
                "Binance API identity admission unavailable"
            );
            Some(code.to_owned())
        }
    }
}

/// Observers keep managing all credentials. This diagnostic is never a reason
/// to stop a second stream, recovery task, protection or close.
pub(crate) async fn diagnose(store: &Store, requested: Option<&str>) {
    match identities(store, requested).await {
        Ok(identities) => {
            for identity in &identities {
                if let Some(conflict) = conflict_for(&identities, &identity.credential_id) {
                    if conflict.credential_ids.first() == Some(&identity.credential_id) {
                        conflict.report();
                    }
                }
            }
        }
        Err(code) => tracing::warn!(
            error_code = code,
            "Binance API identity diagnostic unavailable; management continues"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};
    use tracing::instrument::WithSubscriber;

    struct Env(String, Option<OsString>);
    impl Env {
        fn set(name: impl Into<String>, value: &str) -> Self {
            let name = name.into();
            let previous = std::env::var_os(&name);
            std::env::set_var(&name, value);
            Self(name, previous)
        }
    }
    impl Drop for Env {
        fn drop(&mut self) {
            match self.1.take() {
                Some(value) => std::env::set_var(&self.0, value),
                None => std::env::remove_var(&self.0),
            }
        }
    }
    fn configure(credential: &str, key: &str, secret: &str, venue: &str) -> Vec<Env> {
        let (key_name, secret_name) = crate::binance_futures::key_env_names(credential);
        vec![
            Env::set(key_name, key),
            Env::set(secret_name, secret),
            Env::set(crate::venue::venue_env_name(credential), venue),
        ]
    }

    #[test]
    fn current_and_legacy_testnet_rest_hosts_infer_testnet_identity_domain() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        for endpoint in [
            "https://demo-fapi.binance.com",
            "https://testnet.binancefuture.com",
        ] {
            let _endpoint = Env::set("BINANCE_FUTURES_BASE_URL", endpoint);
            let transport =
                crate::venue::rest_transport_for_credential("testnet-endpoint-domain").unwrap();
            assert_eq!(transport.venue, None);
            let domain = Domain::from_transport(transport).unwrap();
            assert_eq!(domain.venue, Some(Venue::Testnet));
        }
    }

    async fn store() -> (Store, std::path::PathBuf) {
        sqlx::any::install_default_drivers();
        let path = std::env::temp_dir().join(format!("rustcore-a1-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        (store, path)
    }
    async fn cleanup(store: Store, path: std::path::PathBuf) {
        store.pool().close().await;
        std::fs::remove_file(path).unwrap();
    }
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    #[tokio::test]
    async fn a1_startup_and_demand_diagnose_both_members_without_secret_output() {
        use sha2::Digest;
        let _serial = crate::binance_futures::tests::env_serial_guard();
        const KEY: &str = "A1_PRIVATE_KEY_SENTINEL_70937";
        const SECRET: &str = "A1_PRIVATE_SECRET_SENTINEL_98246";
        let _a = configure("a1-start-a", KEY, SECRET, "testnet");
        let _b = configure("a1-start-b", KEY, SECRET, "testnet");
        let (store, path) = store().await;
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        async {
            let manager =
                crate::binance_user_stream::BinanceUserStreamManager::disabled_for_tests();
            let limiter = crate::rate_limiter::CredentialRateLimiter::shared(
                crate::rate_limiter::TokenBucketConfig::from_env(None),
            );
            manager
                .start_discovered(
                    store.clone(),
                    limiter.clone(),
                    &crate::reconcile::ReconcilerManager::new(),
                )
                .await
                .unwrap();
            manager
                .ensure_running(
                    "a1-start-b",
                    "BTCUSDT",
                    store.clone(),
                    limiter,
                    keys_from_env("a1-start-b").unwrap().unwrap(),
                    crate::fill_replay::RecoveryTrigger::Startup,
                )
                .await
                .unwrap();
            for credential in ["a1-start-a", "a1-start-b"] {
                let denial = entry_denial(&store, credential).await.unwrap();
                assert!(denial.starts_with(CONFLICT_CODE));
                assert!(denial.contains("duplicate_api_identity"));
                assert!(denial.contains("a1-start-a,a1-start-b"));
                assert!(!denial.contains(KEY) && !denial.contains(SECRET));
            }
        }
        .with_subscriber(subscriber)
        .await;
        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.matches(CONFLICT_CODE).count() >= 4,
            "startup, demand and both admissions must diagnose"
        );
        for secret in [KEY, SECRET] {
            assert!(!output.contains(secret), "a sensitive sentinel was logged");
            assert!(
                !output.contains(&hex::encode(sha2::Sha256::digest(secret.as_bytes()))),
                "a sensitive digest was logged"
            );
        }
        assert_eq!(
            sqlx::query("SELECT COUNT(*) AS n FROM engine_orders")
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<i64, _>("n"),
            0
        );
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a1_secret_mismatch_rotation_and_store_restart_recompute_current_config() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _a = configure("a1-rotate-a", "a1-shared", "a1-secret-a", "testnet");
        let _b = configure("a1-rotate-b", "a1-shared", "a1-secret-b", "testnet");
        let (store, path) = store().await;
        for credential in ["a1-rotate-a", "a1-rotate-b"] {
            assert!(entry_denial(&store, credential)
                .await
                .unwrap()
                .contains("secret_mismatch"));
        }
        store.pool().close().await;
        let store = Store::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        assert!(entry_denial(&store, "a1-rotate-a")
            .await
            .unwrap()
            .contains("secret_mismatch"));
        // Controlled in-process test replacement, not an external-process env
        // hot-reload claim. Each invocation resolves the current configuration.
        std::env::set_var(
            crate::binance_futures::key_env_names("a1-rotate-b").0,
            "a1-rotated-distinct",
        );
        assert!(entry_denial(&store, "a1-rotate-a").await.is_none());
        assert!(entry_denial(&store, "a1-rotate-b").await.is_none());
        std::env::set_var(
            crate::binance_futures::key_env_names("a1-rotate-b").0,
            "a1-shared",
        );
        assert!(entry_denial(&store, "a1-rotate-a").await.is_some());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a1_actual_environment_includes_legacy_mainnet_and_separates_testnet() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _a = configure(
            "a1-domain-a",
            "a1-domain-key",
            "a1-domain-secret",
            "mainnet",
        );
        let _b = configure("a1-domain-b", "a1-domain-key", "a1-domain-secret", "");
        let _url = Env::set(
            "BINANCE_FUTURES_BASE_URL",
            crate::venue::BINANCE_MAINNET_REST_BASE_URL,
        );
        let (store, path) = store().await;
        assert!(entry_denial(&store, "a1-domain-b")
            .await
            .unwrap()
            .contains("environment=mainnet"));
        std::env::set_var(crate::venue::venue_env_name("a1-domain-b"), "testnet");
        // Remove the global mainnet override, which correctly conflicts with an
        // explicitly configured testnet transport in the production resolver.
        std::env::remove_var("BINANCE_FUTURES_BASE_URL");
        assert!(entry_denial(&store, "a1-domain-a").await.is_none());
        assert!(entry_denial(&store, "a1-domain-b").await.is_none());
        cleanup(store, path).await;
    }

    #[test]
    fn a1_transport_mode_and_endpoint_are_identity_domain_boundaries() {
        fn identity(credential: &str, mode: TransportMode, endpoint: &str) -> Identity {
            Identity {
                credential_id: credential.into(),
                keys: BinanceKeys {
                    api_key: "shared".into(),
                    api_secret: "secret".into(),
                },
                domain: Domain {
                    venue: Some(Venue::Testnet),
                    mode,
                    endpoint: endpoint.into(),
                },
            }
        }
        let a = identity(
            "a",
            TransportMode::Direct,
            "https://testnet.binancefuture.com",
        );
        let b = identity(
            "b",
            TransportMode::FaultProxy,
            "https://testnet.binancefuture.com",
        );
        assert!(conflict_for(&[a, b], "a").is_none());
        let a = identity("a", TransportMode::FaultProxy, "https://localhost:9001");
        let b = identity("b", TransportMode::FaultProxy, "https://localhost:9002");
        assert!(conflict_for(&[a, b], "a").is_none());
        let a = identity("a", TransportMode::FaultProxy, "https://localhost:9001");
        let b = identity("b", TransportMode::FaultProxy, "https://localhost:9001");
        assert_eq!(conflict_for(&[a, b], "a").unwrap().transport, "fault_proxy");
    }

    #[tokio::test]
    async fn a1_durable_scopes_global_fallback_and_trim_locator_do_not_collapse_members() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let _key = Env::set("BINANCE_API_KEY", "a1-global-key");
        let _secret = Env::set("BINANCE_API_SECRET", "a1-global-secret");
        let (store, path) = store().await;
        sqlx::query("INSERT INTO position_state(position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES('a1-history','a1-history','bot','binance|futures|BTC|USDT|USDT','long',1,'closed','0',1)")
            .execute(store.pool()).await.unwrap();
        let denial = entry_denial(&store, "a1-later-rpc").await.unwrap();
        assert!(denial.contains("a1-history") && denial.contains("a1-later-rpc"));
        let values = identities(&store, Some(" a1-history ")).await.unwrap();
        let conflict = conflict_for(&values, " a1-history ").unwrap();
        assert!(conflict.credential_ids.contains(&" a1-history ".to_owned()));
        assert!(conflict.credential_ids.contains(&"a1-history".to_owned()));
        assert!(entry_denial(&store, "paper").await.is_none());
        assert!(
            entry_denial(&store, "paper:0123456789abcdef0123456789abcdef")
                .await
                .is_none()
        );
        assert!(entry_denial(&store, "paper:malformed").await.is_none());
        cleanup(store, path).await;
    }

    #[tokio::test]
    async fn a1_unavailable_scope_is_explicit_and_diagnostics_do_not_stop_management() {
        let _serial = crate::binance_futures::tests::env_serial_guard();
        let (store, path) = store().await;
        store.pool().close().await;
        assert_eq!(
            entry_denial(&store, "a1-unavailable").await.as_deref(),
            Some(UNAVAILABLE_CODE)
        );
        diagnose(&store, Some("a1-unavailable")).await;
        std::fs::remove_file(path).unwrap();
    }
}
