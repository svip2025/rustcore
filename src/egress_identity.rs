use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::Context;
use url::Url;

use crate::store::{DbKind, Store};

const DEFAULT_PROBE_INTERVAL_MS: u64 = 300_000;
const MIN_PROBE_INTERVAL_MS: u64 = 30_000;
const DEFAULT_OWNER_HEARTBEAT_MS: u64 = 10_000;
const MIN_OWNER_HEARTBEAT_MS: u64 = 1_000;
const MAX_OWNER_HEARTBEAT_MS: u64 = 60_000;
const PROBE_TIMEOUT_SECS: u64 = 5;
const FORBIDDEN_PROBE_HOST_SUFFIXES: [&str; 4] = [
    "binance.com",
    "binancefuture.com",
    "binance.us",
    "binance.vision",
];

#[derive(Clone, Debug)]
pub struct EgressIdentityConfig {
    pub route_id: Option<String>,
    route_id_invalid: bool,
    pub expected_source_ip: Option<IpAddr>,
    expected_source_ip_invalid: bool,
    pub source_probe_url: Option<Url>,
    source_probe_url_forbidden: bool,
    pub source_probe_interval_ms: u64,
    pub owner_heartbeat_ms: u64,
}

impl EgressIdentityConfig {
    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let values: HashMap<String, String> = pairs.into_iter().collect();

        let raw_route_id = nonempty(values.get("BINANCE_EGRESS_ROUTE_ID"));
        let (route_id, route_id_invalid) = match raw_route_id {
            Some(value) if crate::binance_futures::validate_egress_route_id(value).is_ok() => {
                (Some(value.to_string()), false)
            }
            Some(_) => (None, true),
            None => (None, false),
        };

        let raw_expected = nonempty(values.get("BINANCE_EGRESS_EXPECTED_SOURCE_IP"));
        let (expected_source_ip, expected_source_ip_invalid) = match raw_expected {
            Some(value) => match value.parse::<IpAddr>() {
                Ok(ip) => (Some(ip), false),
                Err(_) => (None, true),
            },
            None => (None, false),
        };

        let raw_probe_url = nonempty(values.get("BINANCE_EGRESS_SOURCE_PROBE_URL"));
        let (source_probe_url, source_probe_url_forbidden) = match raw_probe_url {
            Some(value) => match validate_probe_url(value) {
                Ok(url) => (Some(url), false),
                Err(_) => (None, true),
            },
            None => (None, false),
        };

        let source_probe_interval_ms = parse_u64(
            values.get("BINANCE_EGRESS_SOURCE_PROBE_INTERVAL_MS"),
            DEFAULT_PROBE_INTERVAL_MS,
        )
        .max(MIN_PROBE_INTERVAL_MS);
        let owner_heartbeat_ms = parse_u64(
            values.get("BINANCE_EGRESS_OWNER_HEARTBEAT_MS"),
            DEFAULT_OWNER_HEARTBEAT_MS,
        )
        .clamp(MIN_OWNER_HEARTBEAT_MS, MAX_OWNER_HEARTBEAT_MS);

        Self {
            route_id,
            route_id_invalid,
            expected_source_ip,
            expected_source_ip_invalid,
            source_probe_url,
            source_probe_url_forbidden,
            source_probe_interval_ms,
            owner_heartbeat_ms,
        }
    }

    pub fn from_env() -> Self {
        Self::from_pairs(std::env::vars())
    }

    fn lease_ttl_ms(&self) -> i64 {
        i64::try_from(self.owner_heartbeat_ms.saturating_mul(3)).unwrap_or(i64::MAX)
    }
}

fn nonempty(value: Option<&String>) -> Option<&str> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_u64(value: Option<&String>, default: u64) -> u64 {
    nonempty(value)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn probe_host_is_forbidden(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    FORBIDDEN_PROBE_HOST_SUFFIXES
        .iter()
        .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

fn validate_probe_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid URL: {error}"))?;
    if url.scheme() != "https" {
        return Err("probe URL must use https".to_string());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "probe URL must have a host".to_string())?;
    if probe_host_is_forbidden(host) {
        return Err("probe URL must not point at Binance".to_string());
    }
    Ok(url)
}

pub fn egress_owner_key(route_id: Option<&str>, hostname: &str) -> String {
    match route_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(route_id) if crate::binance_futures::validate_egress_route_id(route_id).is_ok() => {
            format!("binance-egress:route:{route_id}")
        }
        _ => format!("binance-egress:host:{hostname}"),
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "unknown-host".to_string())
}

#[derive(Clone, Debug)]
struct RuntimeIdentity {
    owner_key: String,
    owner_id: String,
}

impl RuntimeIdentity {
    fn new(config: &EgressIdentityConfig) -> Self {
        let hostname = hostname();
        Self {
            owner_key: egress_owner_key(config.route_id.as_deref(), &hostname),
            owner_id: format!("{}:{}:{}", hostname, std::process::id(), ulid::Ulid::new()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerLease {
    Held {
        lease_until_ms: i64,
    },
    HeldByOther {
        holder_owner_id: String,
        lease_until_ms: i64,
    },
}

pub async fn claim_or_renew(
    store: &Store,
    key: &str,
    owner_id: &str,
    now_ms: i64,
    ttl_ms: i64,
) -> anyhow::Result<OwnerLease> {
    let lease_until_ms = now_ms.saturating_add(ttl_ms);
    let query = match store.kind() {
        DbKind::MySql => {
            "INSERT INTO binance_egress_owners \
             (egress_owner_key, owner_id, lease_until_ms, heartbeat_at_ms) VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             owner_id = IF(owner_id = VALUES(owner_id) OR lease_until_ms < VALUES(heartbeat_at_ms), \
                           VALUES(owner_id), owner_id), \
             lease_until_ms = IF(owner_id = VALUES(owner_id) OR lease_until_ms < VALUES(heartbeat_at_ms), \
                                 VALUES(lease_until_ms), lease_until_ms), \
             heartbeat_at_ms = IF(owner_id = VALUES(owner_id) OR lease_until_ms < VALUES(heartbeat_at_ms), \
                                  VALUES(heartbeat_at_ms), heartbeat_at_ms)"
        }
        _ => {
            "INSERT INTO binance_egress_owners \
             (egress_owner_key, owner_id, lease_until_ms, heartbeat_at_ms) VALUES (?, ?, ?, ?) \
             ON CONFLICT(egress_owner_key) DO UPDATE SET \
             owner_id = excluded.owner_id, lease_until_ms = excluded.lease_until_ms, \
             heartbeat_at_ms = excluded.heartbeat_at_ms \
             WHERE binance_egress_owners.owner_id = excluded.owner_id \
                OR binance_egress_owners.lease_until_ms < excluded.heartbeat_at_ms"
        }
    };
    sqlx::query(query)
        .bind(key)
        .bind(owner_id)
        .bind(lease_until_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .context("claim or renew Binance egress owner lease")?;

    let row = sqlx::query(
        "SELECT owner_id, CAST(lease_until_ms AS CHAR) AS lease_until_ms_text \
         FROM binance_egress_owners WHERE egress_owner_key = ?",
    )
    .bind(key)
    .fetch_one(store.pool())
    .await
    .context("read Binance egress owner lease after CAS")?;
    let holder_owner_id = crate::store::try_get_text(&row, "owner_id")?
        .ok_or_else(|| anyhow::anyhow!("Binance egress owner_id unexpectedly NULL"))?;
    let stored_lease_until_ms = crate::store::try_get_i64_text(&row, "lease_until_ms_text")
        .context("decode Binance egress lease_until_ms")?;
    if holder_owner_id == owner_id {
        Ok(OwnerLease::Held {
            lease_until_ms: stored_lease_until_ms,
        })
    } else {
        Ok(OwnerLease::HeldByOther {
            holder_owner_id,
            lease_until_ms: stored_lease_until_ms,
        })
    }
}

pub fn parse_probe_body(body: &str) -> Option<IpAddr> {
    body.trim().parse().ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceCheck {
    Unconfigured,
    Match,
    Mismatch { expected: IpAddr, observed: IpAddr },
}

pub fn source_check(expected: Option<IpAddr>, observed: IpAddr) -> SourceCheck {
    match expected {
        None => SourceCheck::Unconfigured,
        Some(expected) if expected == observed => SourceCheck::Match,
        Some(expected) => SourceCheck::Mismatch { expected, observed },
    }
}

async fn persist_source_check(
    store: &Store,
    owner_key: &str,
    expected: Option<IpAddr>,
    observed: IpAddr,
    check: &SourceCheck,
    now_ms: i64,
) -> anyhow::Result<()> {
    let expected = expected.map(|ip| ip.to_string());
    let observed = observed.to_string();
    let mismatch_since_ms = matches!(check, SourceCheck::Mismatch { .. }).then_some(now_ms);
    let query = match store.kind() {
        DbKind::MySql => {
            "INSERT INTO binance_egress_identity \
             (egress_owner_key, expected_source_ip, observed_source_ip, observed_at_ms, \
              probe_failed_at_ms, mismatch_since_ms, updated_at_ms) \
             VALUES (?, ?, ?, ?, NULL, ?, ?) ON DUPLICATE KEY UPDATE \
             expected_source_ip = VALUES(expected_source_ip), \
             observed_source_ip = VALUES(observed_source_ip), \
             observed_at_ms = VALUES(observed_at_ms), probe_failed_at_ms = NULL, \
             mismatch_since_ms = IF(VALUES(mismatch_since_ms) IS NULL, NULL, \
                                    COALESCE(mismatch_since_ms, VALUES(mismatch_since_ms))), \
             updated_at_ms = VALUES(updated_at_ms)"
        }
        _ => {
            "INSERT INTO binance_egress_identity \
             (egress_owner_key, expected_source_ip, observed_source_ip, observed_at_ms, \
              probe_failed_at_ms, mismatch_since_ms, updated_at_ms) \
             VALUES (?, ?, ?, ?, NULL, ?, ?) ON CONFLICT(egress_owner_key) DO UPDATE SET \
             expected_source_ip = excluded.expected_source_ip, \
             observed_source_ip = excluded.observed_source_ip, \
             observed_at_ms = excluded.observed_at_ms, probe_failed_at_ms = NULL, \
             mismatch_since_ms = CASE WHEN excluded.mismatch_since_ms IS NULL THEN NULL \
                                      ELSE COALESCE(binance_egress_identity.mismatch_since_ms, \
                                                    excluded.mismatch_since_ms) END, \
             updated_at_ms = excluded.updated_at_ms"
        }
    };
    sqlx::query(query)
        .bind(owner_key)
        .bind(expected)
        .bind(observed)
        .bind(now_ms)
        .bind(mismatch_since_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .context("persist Binance egress source check")?;
    Ok(())
}

async fn persist_probe_failure(
    store: &Store,
    owner_key: &str,
    expected: Option<IpAddr>,
    now_ms: i64,
) -> anyhow::Result<()> {
    let expected = expected.map(|ip| ip.to_string());
    let query = match store.kind() {
        DbKind::MySql => {
            "INSERT INTO binance_egress_identity \
             (egress_owner_key, expected_source_ip, observed_source_ip, observed_at_ms, \
              probe_failed_at_ms, mismatch_since_ms, updated_at_ms) \
             VALUES (?, ?, NULL, NULL, ?, NULL, ?) ON DUPLICATE KEY UPDATE \
             expected_source_ip = VALUES(expected_source_ip), \
             probe_failed_at_ms = VALUES(probe_failed_at_ms), updated_at_ms = VALUES(updated_at_ms)"
        }
        _ => {
            "INSERT INTO binance_egress_identity \
             (egress_owner_key, expected_source_ip, observed_source_ip, observed_at_ms, \
              probe_failed_at_ms, mismatch_since_ms, updated_at_ms) \
             VALUES (?, ?, NULL, NULL, ?, NULL, ?) ON CONFLICT(egress_owner_key) DO UPDATE SET \
             expected_source_ip = excluded.expected_source_ip, \
             probe_failed_at_ms = excluded.probe_failed_at_ms, updated_at_ms = excluded.updated_at_ms"
        }
    };
    sqlx::query(query)
        .bind(owner_key)
        .bind(expected)
        .bind(now_ms)
        .bind(now_ms)
        .execute(store.pool())
        .await
        .context("persist Binance egress source probe failure")?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseTickReport {
    Lease(OwnerLease),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceTickReport {
    Disabled,
    NotDue,
    Checked(SourceCheck),
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityTickReport {
    pub lease: LeaseTickReport,
    pub source: SourceTickReport,
}

async fn probe_source(client: &reqwest::Client, url: &Url) -> anyhow::Result<IpAddr> {
    let body = client
        .get(url.clone())
        .send()
        .await
        .context("send source IP probe")?
        .error_for_status()
        .context("source IP probe returned a non-success status")?
        .text()
        .await
        .context("read source IP probe body")?;
    parse_probe_body(&body)
        .ok_or_else(|| anyhow::anyhow!("source IP probe body is not an IP literal"))
}

async fn identity_tick(
    store: &Store,
    config: &EgressIdentityConfig,
    runtime: &RuntimeIdentity,
    client: Option<&reqwest::Client>,
    now_ms: i64,
    probe_due: bool,
) -> IdentityTickReport {
    let lease = match claim_or_renew(
        store,
        &runtime.owner_key,
        &runtime.owner_id,
        now_ms,
        config.lease_ttl_ms(),
    )
    .await
    {
        Ok(lease) => LeaseTickReport::Lease(lease),
        Err(error) => LeaseTickReport::Error(format!("{error:#}")),
    };

    let source = match (&config.source_probe_url, probe_due) {
        (None, _) => SourceTickReport::Disabled,
        (Some(_), false) => SourceTickReport::NotDue,
        (Some(url), true) => match client {
            None => {
                let message = "source IP probe client could not be constructed".to_string();
                match persist_probe_failure(
                    store,
                    &runtime.owner_key,
                    config.expected_source_ip,
                    now_ms,
                )
                .await
                {
                    Ok(()) => SourceTickReport::Failed(message),
                    Err(error) => {
                        SourceTickReport::Failed(format!("{message}; persistence: {error:#}"))
                    }
                }
            }
            Some(client) => match probe_source(client, url).await {
                Ok(observed) => {
                    let check = source_check(config.expected_source_ip, observed);
                    match persist_source_check(
                        store,
                        &runtime.owner_key,
                        config.expected_source_ip,
                        observed,
                        &check,
                        now_ms,
                    )
                    .await
                    {
                        Ok(()) => SourceTickReport::Checked(check),
                        Err(error) => {
                            SourceTickReport::Failed(format!("persist source check: {error:#}"))
                        }
                    }
                }
                Err(probe_error) => {
                    match persist_probe_failure(
                        store,
                        &runtime.owner_key,
                        config.expected_source_ip,
                        now_ms,
                    )
                    .await
                    {
                        Ok(()) => SourceTickReport::Failed(format!("{probe_error:#}")),
                        Err(error) => SourceTickReport::Failed(format!(
                            "{probe_error:#}; persist probe failure: {error:#}"
                        )),
                    }
                }
            },
        },
    };

    IdentityTickReport { lease, source }
}

pub fn mainnet_configured<I>(pairs: I) -> bool
where
    I: IntoIterator<Item = (String, String)>,
{
    pairs.into_iter().any(|(name, value)| {
        (name.starts_with("BINANCE_VENUE_") && value.trim().eq_ignore_ascii_case("mainnet"))
            || (name == "ENGINE_LIVE_ENABLED"
                && matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                ))
    })
}

pub fn log_startup_identity(config: &EgressIdentityConfig, mainnet_configured: bool) {
    let hostname = hostname();
    let owner_key = egress_owner_key(config.route_id.as_deref(), &hostname);
    tracing::info!(
        route = if config.route_id.is_some() { "present" } else { "absent" },
        expected_source_ip = if config.expected_source_ip.is_some() { "present" } else { "absent" },
        probe = if config.source_probe_url.is_some() { "enabled" } else { "disabled" },
        %owner_key,
        "binance egress identity"
    );
    if config.route_id_invalid {
        tracing::error!(error_code = "BINANCE_EGRESS_ROUTE_ID_INVALID", %owner_key, "invalid Binance egress route identity; using host owner key and continuing startup");
    }
    if config.expected_source_ip_invalid {
        tracing::error!(error_code = "BINANCE_EGRESS_EXPECTED_IP_INVALID", "invalid expected Binance egress source IP; treating it as unconfigured and continuing startup");
    }
    if config.source_probe_url_forbidden {
        tracing::error!(
            error_code = "BINANCE_EGRESS_PROBE_URL_FORBIDDEN",
            "forbidden Binance egress source probe URL; disabling the probe and continuing startup"
        );
    }
    if mainnet_configured && config.route_id.is_none() {
        tracing::error!(error_code = "BINANCE_EGRESS_ROUTE_ID_MISSING", %owner_key, "mainnet is configured without a valid Binance egress route identity; continuing startup");
    }
}

pub fn spawn_egress_identity_task(
    store: Store,
    config: EgressIdentityConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let runtime = RuntimeIdentity::new(&config);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(PROBE_TIMEOUT_SECS))
            .build()
            .ok();
        let mut next_probe_at_ms = i64::MIN;
        let mut ticker = tokio::time::interval(Duration::from_millis(config.owner_heartbeat_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now_ms = crate::svc::now_ms();
            let probe_due = config.source_probe_url.is_some() && now_ms >= next_probe_at_ms;
            if probe_due {
                next_probe_at_ms = now_ms.saturating_add(
                    i64::try_from(config.source_probe_interval_ms).unwrap_or(i64::MAX),
                );
            }
            let report = identity_tick(
                &store,
                &config,
                &runtime,
                client.as_ref(),
                now_ms,
                probe_due,
            )
            .await;
            match report.lease {
                LeaseTickReport::Lease(OwnerLease::Held { .. }) => {}
                LeaseTickReport::Lease(OwnerLease::HeldByOther {
                    holder_owner_id,
                    lease_until_ms,
                }) => tracing::error!(
                    error_code = "BINANCE_EGRESS_OWNER_CONFLICT",
                    owner_key = %runtime.owner_key,
                    %holder_owner_id,
                    lease_until_ms,
                    "another rustcore owner holds this Binance egress lease"
                ),
                LeaseTickReport::Error(error) => tracing::warn!(
                    error_code = "BINANCE_EGRESS_OWNER_LEASE_ERROR",
                    owner_key = %runtime.owner_key,
                    %error,
                    "Binance egress owner lease tick failed"
                ),
            }
            match report.source {
                SourceTickReport::Checked(SourceCheck::Mismatch { expected, observed }) => {
                    tracing::error!(
                        error_code = "BINANCE_EGRESS_SOURCE_MISMATCH",
                        %expected,
                        %observed,
                        owner_key = %runtime.owner_key,
                        "observed Binance egress source IP does not match the deployment declaration"
                    );
                }
                SourceTickReport::Failed(error) => tracing::warn!(
                    error_code = "BINANCE_EGRESS_SOURCE_PROBE_FAILED",
                    owner_key = %runtime.owner_key,
                    %error,
                    "Binance egress source probe failed"
                ),
                SourceTickReport::Disabled
                | SourceTickReport::NotDue
                | SourceTickReport::Checked(SourceCheck::Unconfigured)
                | SourceTickReport::Checked(SourceCheck::Match) => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::path::PathBuf;
    use std::sync::Once;

    use sqlx::Row;

    use super::*;
    use crate::store::mysql_test_support::MySqlScratch;

    static INSTALL_DRIVERS: Once = Once::new();

    struct TestStore {
        path: PathBuf,
        store: Store,
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let base = self.path.to_string_lossy();
            let _ = std::fs::remove_file(format!("{base}-shm"));
            let _ = std::fs::remove_file(format!("{base}-wal"));
        }
    }

    async fn test_store(label: &str) -> TestStore {
        INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
        let path =
            std::env::temp_dir().join(format!("rustcore-egress-{label}-{}.db", ulid::Ulid::new()));
        let store = Store::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .expect("connect temporary SQLite store");
        TestStore { path, store }
    }

    async fn assert_owner_lease_cas(store_a: Store, store_b: Store, backend: &str) {
        let key = format!("lease-sequence-{}", ulid::Ulid::new());
        assert_eq!(
            claim_or_renew(&store_a, &key, "owner-a", 1_000, 3_000)
                .await
                .expect("owner A claims"),
            OwnerLease::Held {
                lease_until_ms: 4_000
            }
        );
        assert_eq!(
            claim_or_renew(&store_b, &key, "owner-b", 2_000, 3_000)
                .await
                .expect("owner B observes the live holder"),
            OwnerLease::HeldByOther {
                holder_owner_id: "owner-a".to_string(),
                lease_until_ms: 4_000,
            }
        );
        assert_eq!(
            claim_or_renew(&store_a, &key, "owner-a", 3_000, 3_000)
                .await
                .expect("owner A renews"),
            OwnerLease::Held {
                lease_until_ms: 6_000
            },
            "owner A must extend its lease on {backend}"
        );
        assert_eq!(
            claim_or_renew(&store_b, &key, "owner-b", 4_001, 3_000)
                .await
                .expect("owner B observes the renewed holder"),
            OwnerLease::HeldByOther {
                holder_owner_id: "owner-a".to_string(),
                lease_until_ms: 6_000,
            },
            "the original expiry must not defeat A's renewal on {backend}"
        );
        assert_eq!(
            claim_or_renew(&store_b, &key, "owner-b", 6_001, 3_000)
                .await
                .expect("owner B takes over after expiry"),
            OwnerLease::Held {
                lease_until_ms: 9_001
            }
        );

        let concurrent_key = format!("lease-concurrent-{}", ulid::Ulid::new());
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::with_capacity(8);
        for index in 0..8 {
            let store = if index % 2 == 0 {
                store_a.clone()
            } else {
                store_b.clone()
            };
            let barrier = barrier.clone();
            let key = concurrent_key.clone();
            let owner = format!("owner-{index}");
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let lease = claim_or_renew(&store, &key, &owner, 10_000, 3_000)
                    .await
                    .expect("concurrent owner claim");
                (owner, lease)
            }));
        }
        let mut results = Vec::with_capacity(8);
        for task in tasks {
            results.push(task.await.expect("join a concurrent owner claimant"));
        }
        let winners = results
            .iter()
            .filter(|(_, lease)| matches!(lease, OwnerLease::Held { .. }))
            .collect::<Vec<_>>();
        assert_eq!(
            winners.len(),
            1,
            "exactly one of eight owners must hold the {backend} lease: {results:?}"
        );
        let winner = winners[0].0.as_str();
        assert_eq!(
            winners[0].1,
            OwnerLease::Held {
                lease_until_ms: 13_000
            }
        );
        for (owner, lease) in &results {
            if owner == winner {
                continue;
            }
            assert_eq!(
                lease,
                &OwnerLease::HeldByOther {
                    holder_owner_id: winner.to_string(),
                    lease_until_ms: 13_000,
                },
                "every losing {backend} claimant must observe the same winner"
            );
        }
    }

    fn config(pairs: &[(&str, &str)]) -> EgressIdentityConfig {
        EgressIdentityConfig::from_pairs(
            pairs
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        )
    }

    #[test]
    fn owner_key_prefers_a_valid_route_and_falls_back_to_the_host() {
        assert_eq!(
            egress_owner_key(Some("nat-hkg-01"), "host-a"),
            "binance-egress:route:nat-hkg-01"
        );
        assert_eq!(
            egress_owner_key(None, "host-a"),
            "binance-egress:host:host-a"
        );
        assert_eq!(
            egress_owner_key(Some("bad route/id"), "host-a"),
            "binance-egress:host:host-a"
        );
    }

    #[test]
    fn identity_config_defaults_clamps_and_disables_invalid_declarations() {
        let defaults = config(&[]);
        assert_eq!(defaults.source_probe_interval_ms, 300_000);
        assert_eq!(defaults.owner_heartbeat_ms, 10_000);

        let clamped = config(&[
            ("BINANCE_EGRESS_SOURCE_PROBE_INTERVAL_MS", "1"),
            ("BINANCE_EGRESS_OWNER_HEARTBEAT_MS", "999999"),
            ("BINANCE_EGRESS_ROUTE_ID", "bad route"),
            ("BINANCE_EGRESS_EXPECTED_SOURCE_IP", "not-an-ip"),
            (
                "BINANCE_EGRESS_SOURCE_PROBE_URL",
                "https://fapi.binance.com",
            ),
        ]);
        assert_eq!(clamped.source_probe_interval_ms, 30_000);
        assert_eq!(clamped.owner_heartbeat_ms, 60_000);
        assert!(clamped.route_id.is_none() && clamped.route_id_invalid);
        assert!(clamped.expected_source_ip.is_none() && clamped.expected_source_ip_invalid);
        assert!(clamped.source_probe_url.is_none() && clamped.source_probe_url_forbidden);

        let minimum_heartbeat = config(&[("BINANCE_EGRESS_OWNER_HEARTBEAT_MS", "10")]);
        assert_eq!(minimum_heartbeat.owner_heartbeat_ms, 1_000);
        assert_eq!(minimum_heartbeat.lease_ttl_ms(), 3_000);
    }

    #[tokio::test]
    async fn a_live_lease_refuses_a_second_owner() {
        let test_store = test_store("live-lease").await;
        let store = &test_store.store;
        let key = format!("lease-{}", ulid::Ulid::new());
        assert_eq!(
            claim_or_renew(store, &key, "owner-a", 1_000, 3_000)
                .await
                .expect("first owner claims"),
            OwnerLease::Held {
                lease_until_ms: 4_000
            }
        );
        assert_eq!(
            claim_or_renew(store, &key, "owner-b", 2_000, 3_000)
                .await
                .expect("conflict is a report"),
            OwnerLease::HeldByOther {
                holder_owner_id: "owner-a".to_string(),
                lease_until_ms: 4_000,
            }
        );
    }

    #[tokio::test]
    async fn an_expired_lease_can_be_taken_over() {
        let test_store = test_store("expired-lease").await;
        let store = &test_store.store;
        let key = format!("lease-{}", ulid::Ulid::new());
        claim_or_renew(store, &key, "owner-a", 1_000, 3_000)
            .await
            .expect("first owner claims");
        assert_eq!(
            claim_or_renew(store, &key, "owner-b", 4_001, 3_000)
                .await
                .expect("expired lease is claimable"),
            OwnerLease::Held {
                lease_until_ms: 7_001
            }
        );
    }

    #[tokio::test]
    async fn renewal_extends_the_lease() {
        let test_store = test_store("renew-lease").await;
        let store = &test_store.store;
        let key = format!("lease-{}", ulid::Ulid::new());
        claim_or_renew(store, &key, "owner-a", 1_000, 3_000)
            .await
            .expect("first owner claims");
        assert_eq!(
            claim_or_renew(store, &key, "owner-a", 2_000, 3_000)
                .await
                .expect("same owner renews"),
            OwnerLease::Held {
                lease_until_ms: 5_000
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_owner_lease_cas_refuses_live_holder_and_allows_expired_takeover() {
        let test_store = test_store("owner-cas").await;
        assert_owner_lease_cas(test_store.store.clone(), test_store.store.clone(), "SQLite").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mysql_owner_lease_cas_refuses_live_holder_and_allows_expired_takeover() {
        const TEST: &str = "mysql_owner_lease_cas_refuses_live_holder_and_allows_expired_takeover";
        let Some(scratch) = MySqlScratch::create("m1owner", TEST).await else {
            return;
        };
        let store_a = scratch.connect_store().await;
        let store_b = scratch.connect_store().await;
        assert_owner_lease_cas(store_a.clone(), store_b.clone(), "MySQL").await;
        store_a.pool().close().await;
        store_b.pool().close().await;
        scratch.cleanup().await;
    }

    #[tokio::test]
    async fn a_conflict_is_a_report_not_an_error() {
        let test_store = test_store("conflict-report").await;
        let store = &test_store.store;
        let runtime_a = RuntimeIdentity {
            owner_key: format!("lease-{}", ulid::Ulid::new()),
            owner_id: "owner-a".to_string(),
        };
        let runtime_b = RuntimeIdentity {
            owner_key: runtime_a.owner_key.clone(),
            owner_id: "owner-b".to_string(),
        };
        let base_config = config(&[]);
        identity_tick(store, &base_config, &runtime_a, None, 1_000, false).await;
        let conflict = identity_tick(store, &base_config, &runtime_b, None, 2_000, false).await;
        assert!(matches!(
            conflict.lease,
            LeaseTickReport::Lease(OwnerLease::HeldByOther { .. })
        ));

        sqlx::query("DROP TABLE binance_egress_owners")
            .execute(store.pool())
            .await
            .expect("remove owner table to force a DB report");
        let db_failure = identity_tick(store, &base_config, &runtime_b, None, 3_000, false).await;
        assert!(matches!(db_failure.lease, LeaseTickReport::Error(_)));

        let source_config = config(&[("BINANCE_EGRESS_SOURCE_PROBE_URL", "https://127.0.0.1:9")]);
        let probe_failure =
            identity_tick(store, &source_config, &runtime_b, None, 4_000, true).await;
        assert!(matches!(probe_failure.source, SourceTickReport::Failed(_)));

        let source = include_str!("egress_identity.rs");
        let body = source
            .split("pub fn spawn_egress_identity_task")
            .nth(1)
            .expect("spawn function exists")
            .split("#[cfg(test)]")
            .next()
            .expect("spawn body is bounded");
        assert!(!body.contains("unwrap("));
        assert!(!body.contains("expect("));
        assert!(!body.contains('?'));
    }

    #[test]
    fn probe_body_parses_only_ip_literals() {
        assert_eq!(
            parse_probe_body("1.2.3.4\n"),
            Some(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)))
        );
        assert_eq!(
            parse_probe_body("2001:db8::1"),
            Some(IpAddr::V6("2001:db8::1".parse::<Ipv6Addr>().unwrap()))
        );
        assert_eq!(parse_probe_body("<html>no</html>"), None);
    }

    #[test]
    fn source_check_three_way() {
        let one = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let two = IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8));
        assert_eq!(source_check(None, one), SourceCheck::Unconfigured);
        assert_eq!(source_check(Some(one), one), SourceCheck::Match);
        assert_eq!(
            source_check(Some(one), two),
            SourceCheck::Mismatch {
                expected: one,
                observed: two,
            }
        );
    }

    #[test]
    fn probe_url_never_points_at_binance() {
        assert!(validate_probe_url("https://api.ipify.org").is_ok());
        assert!(validate_probe_url("https://fapi.binance.com/x").is_err());
        assert!(validate_probe_url("https://testnet.binancefuture.com").is_err());
        assert!(validate_probe_url("http://api.ipify.org").is_err());
        assert!(validate_probe_url("https://binance.com.evil.example").is_ok());
        assert!(validate_probe_url("https://api.binance.us").is_err());
        assert!(validate_probe_url("https://api.binance.vision").is_err());
    }

    #[tokio::test]
    async fn identity_row_tracks_mismatch_and_probe_failure_honestly() {
        let test_store = test_store("source-row").await;
        let store = &test_store.store;
        let key = format!("source-{}", ulid::Ulid::new());
        let expected = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let observed = IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8));
        let mismatch = source_check(Some(expected), observed);
        persist_source_check(store, &key, Some(expected), observed, &mismatch, 10_000)
            .await
            .expect("persist mismatch");
        let mismatch_row = sqlx::query(
            "SELECT CAST(mismatch_since_ms AS CHAR) AS mismatch_since_text \
             FROM binance_egress_identity WHERE egress_owner_key = ?",
        )
        .bind(&key)
        .fetch_one(store.pool())
        .await
        .expect("read mismatch start");
        assert_eq!(
            crate::store::try_get_i64_text(&mismatch_row, "mismatch_since_text").unwrap(),
            10_000
        );

        let matched = source_check(Some(expected), expected);
        persist_source_check(store, &key, Some(expected), expected, &matched, 11_000)
            .await
            .expect("persist match");
        persist_probe_failure(store, &key, Some(expected), 12_000)
            .await
            .expect("persist probe failure");

        let row = sqlx::query(
            "SELECT observed_source_ip, CAST(observed_at_ms AS CHAR) AS observed_at_text, \
                    CAST(probe_failed_at_ms AS CHAR) AS failed_at_text, \
                    CAST(mismatch_since_ms AS CHAR) AS mismatch_since_text \
             FROM binance_egress_identity WHERE egress_owner_key = ?",
        )
        .bind(&key)
        .fetch_one(store.pool())
        .await
        .expect("read identity row");
        assert_eq!(
            row.get::<String, _>("observed_source_ip"),
            expected.to_string()
        );
        assert_eq!(
            crate::store::try_get_i64_text(&row, "observed_at_text").unwrap(),
            11_000
        );
        assert_eq!(
            crate::store::try_get_i64_text(&row, "failed_at_text").unwrap(),
            12_000
        );
        assert!(crate::store::try_get_text(&row, "mismatch_since_text")
            .unwrap()
            .is_none());
    }

    #[test]
    fn route_id_missing_is_only_an_error_on_mainnet() {
        assert!(mainnet_configured(vec![(
            "BINANCE_VENUE_H01".to_string(),
            " MainNet ".to_string(),
        )]));
        assert!(mainnet_configured(vec![(
            "ENGINE_LIVE_ENABLED".to_string(),
            "yes".to_string(),
        )]));
        assert!(!mainnet_configured(vec![(
            "BINANCE_VENUE_H01".to_string(),
            "testnet".to_string(),
        )]));
    }

    #[test]
    fn startup_wires_the_egress_identity_task() {
        let main = include_str!("main.rs");
        assert!(main.contains("egress_identity::log_startup_identity("));
        assert!(main.contains("egress_identity::spawn_egress_identity_task("));
    }

    #[tokio::test]
    async fn migration_0032_creates_both_identity_tables() {
        let test_store = test_store("migration-0032").await;
        let store = &test_store.store;
        for (table, columns) in [
            (
                "binance_egress_owners",
                vec![
                    "egress_owner_key",
                    "owner_id",
                    "lease_until_ms",
                    "heartbeat_at_ms",
                ],
            ),
            (
                "binance_egress_identity",
                vec![
                    "egress_owner_key",
                    "expected_source_ip",
                    "observed_source_ip",
                    "observed_at_ms",
                    "probe_failed_at_ms",
                    "mismatch_since_ms",
                    "updated_at_ms",
                ],
            ),
        ] {
            let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
                .fetch_all(store.pool())
                .await
                .expect("read SQLite table shape");
            let actual: Vec<String> = rows
                .iter()
                .map(|row| row.get::<String, _>("name"))
                .collect();
            assert_eq!(actual, columns);
        }
    }
}
