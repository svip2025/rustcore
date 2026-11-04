//! Bounded read diagnostics and explicit testnet-only user-stream fault control.
//! No account ID or listenKey is returned by these responses.
use crate::proto_v2 as p;
use crate::store::Store;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use tokio::sync::mpsc;
use tonic::Status;

#[derive(Clone, Copy, Debug)]
pub(crate) enum UserStreamFault {
    Disconnect,
    ListenKeyExpired,
}
fn controls() -> &'static Mutex<HashMap<String, mpsc::Sender<UserStreamFault>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, mpsc::Sender<UserStreamFault>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}
pub(crate) fn register_stream(credential: &str) -> anyhow::Result<mpsc::Receiver<UserStreamFault>> {
    let (tx, rx) = mpsc::channel(1);
    let mut registry = controls()
        .lock()
        .map_err(|_| anyhow::anyhow!("user stream control unavailable"))?;
    registry.retain(|_, sender| !sender.is_closed());
    registry.insert(credential.into(), tx);
    Ok(rx)
}
fn guard_allows(
    enabled: bool,
    live_disabled: bool,
    demo: bool,
    venue: Option<crate::venue::Venue>,
) -> bool {
    enabled && live_disabled && demo && venue == Some(crate::venue::Venue::Testnet)
}
fn validate_fault_guard(credential: &str) -> Result<(), Status> {
    let venue = crate::venue::venue_for_credential(credential)
        .map_err(|_| Status::failed_precondition("fault venue unavailable"))?;
    if !guard_allows(
        std::env::var("ROUND2_TESTNET_FAULTS").ok().as_deref() == Some("1"),
        matches!(
            std::env::var("ENGINE_LIVE_ENABLED").ok().as_deref(),
            Some("false" | "0")
        ),
        std::env::var("ENGINE_STAGE_MAX").ok().as_deref() == Some("demo"),
        venue,
    ) {
        return Err(Status::failed_precondition("user stream faults require explicit testnet, demo ceiling, live disabled and ROUND2_TESTNET_FAULTS=1"));
    }
    Ok(())
}
pub(crate) fn inject(
    request: p::InjectUserStreamFaultRequest,
) -> Result<p::InjectUserStreamFaultResponse, Status> {
    if request.schema_version != 1 {
        return Err(Status::invalid_argument("schema_version must be 1"));
    }
    validate_fault_guard(&request.credential_id)?;
    let action = match request.action.as_str() {
        "disconnect" => UserStreamFault::Disconnect,
        "listen_key_expired" => UserStreamFault::ListenKeyExpired,
        _ => return Err(Status::invalid_argument("unknown user stream fault action")),
    };
    let registry = controls()
        .lock()
        .map_err(|_| Status::internal("user stream control unavailable"))?;
    let sender = registry
        .get(&request.credential_id)
        .ok_or_else(|| Status::failed_precondition("no active user data stream"))?;
    sender.try_send(action).map_err(|_| {
        Status::failed_precondition("user data stream is inactive or already has a pending fault")
    })?;
    Ok(p::InjectUserStreamFaultResponse {
        accepted: true,
        reason: "Fault queued to the active kernel user-stream reader".into(),
        schema_version: 1,
    })
}
pub(crate) async fn record_connection_attempt(
    store: &Store,
    credential: &str,
    scope: &str,
    reconnect: bool,
    now: i64,
) -> anyhow::Result<()> {
    if !matches!(scope, "USER_DATA_STREAM" | "TRADE_WS_API" | "MARKET_DATA") {
        anyhow::bail!("invalid websocket diagnostic scope");
    }
    sqlx::query("INSERT INTO websocket_connection_attempts (connection_id,credential_id,stream_kind,is_reconnect,observed_at_ms) VALUES (?,?,?,?,?)")
        .bind(ulid::Ulid::new().to_string()).bind(credential).bind(scope).bind(i16::from(reconnect)).bind(now).execute(store.pool()).await?;
    Ok(())
}
async fn scope_metric(
    store: &Store,
    credential: &str,
    scope: &str,
    disabled: bool,
    now: i64,
) -> Result<p::WebSocketReconnectMetric, Status> {
    let rows=sqlx::query("SELECT CAST(COUNT(*) AS CHAR) AS attempts,CAST(COALESCE(SUM(is_reconnect),0) AS CHAR) AS n FROM websocket_connection_attempts WHERE credential_id=? AND stream_kind=?")
        .bind(credential).bind(scope).fetch_one(store.pool()).await.map_err(|_|Status::internal("runtime diagnostics read failed"))?;
    let parse = |field| {
        rows.get::<String, _>(field)
            .parse::<u64>()
            .map_err(|_| Status::internal("invalid diagnostic count"))
    };
    let observed = parse("attempts")? > 0;
    let count = parse("n")?;
    let last=sqlx::query("SELECT CAST(COALESCE(SUM(is_reconnect),0) AS CHAR) AS n FROM websocket_connection_attempts WHERE credential_id=? AND stream_kind=? AND observed_at_ms>=? AND observed_at_ms<=?")
        .bind(credential).bind(scope).bind(now-60_000).bind(now).fetch_one(store.pool()).await.map_err(|_|Status::internal("runtime diagnostics read failed"))?
        .get::<String,_>("n").parse::<u64>().map_err(|_|Status::internal("invalid diagnostic count"))?;
    Ok(p::WebSocketReconnectMetric {
        scope: scope.into(),
        status: if observed {
            "OBSERVED"
        } else if disabled {
            "DISABLED"
        } else {
            "NOT_OBSERVED"
        }
        .into(),
        reconnect_count: observed.then_some(count),
        reconnects_last_minute: observed.then_some(last),
    })
}
pub(crate) async fn read(
    store: &Store,
    request: p::GetRuntimeDiagnosticsRequest,
) -> Result<p::GetRuntimeDiagnosticsResponse, Status> {
    if request.schema_version != 1 || request.credential_id.trim().is_empty() {
        return Err(Status::invalid_argument("schema 1 and credential required"));
    }
    let now = crate::svc::now_ms();
    let user = scope_metric(
        store,
        &request.credential_id,
        "USER_DATA_STREAM",
        false,
        now,
    )
    .await?;
    let trade = scope_metric(
        store,
        &request.credential_id,
        "TRADE_WS_API",
        !crate::binance_ws_api::ws_api_enabled(),
        now,
    )
    .await?;
    let market_enabled = sqlx::query(
        "SELECT credential_id FROM market_stream_subscriptions WHERE credential_id=? LIMIT 1",
    )
    .bind(&request.credential_id)
    .fetch_optional(store.pool())
    .await
    .map_err(|_| Status::internal("market diagnostics unavailable"))?
    .is_some();
    let market = scope_metric(
        store,
        &request.credential_id,
        "MARKET_DATA",
        !market_enabled,
        now,
    )
    .await?;
    let state = crate::fill_replay::load_fill_stream_state(store, &request.credential_id)
        .await
        .map_err(|_| Status::internal("user stream diagnostics unavailable"))?;
    Ok(p::GetRuntimeDiagnosticsResponse {
        observed_at_ms: now,
        user_stream_reconnect_count: user.reconnect_count.unwrap_or(0),
        user_stream_reconnects_last_minute: user.reconnects_last_minute.unwrap_or(0),
        user_stream_ready: state.as_ref().is_some_and(|s| s.ready),
        user_stream_phase: state.map(|s| s.phase).unwrap_or_else(|| "unknown".into()),
        websocket_scopes: vec![user, trade, market],
        schema_version: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round2_f4_guard_requires_every_explicit_safety_condition() {
        for enabled in [false, true] {
            for disabled in [false, true] {
                for demo in [false, true] {
                    for venue in [
                        None,
                        Some(crate::venue::Venue::Mainnet),
                        Some(crate::venue::Venue::Testnet),
                    ] {
                        assert_eq!(
                            guard_allows(enabled, disabled, demo, venue),
                            enabled
                                && disabled
                                && demo
                                && venue == Some(crate::venue::Venue::Testnet)
                        );
                    }
                }
            }
        }
    }
    #[tokio::test]
    async fn round2_reconnect_counters_survive_restart_without_cross_account_leakage() {
        sqlx::any::install_default_drivers();
        let path = std::env::temp_dir().join(format!("r2-diagnostics-{}.db", ulid::Ulid::new()));
        let url = format!("sqlite://{}", path.display());
        let store = Store::connect(&url).await.unwrap();
        let now = crate::svc::now_ms();
        record_connection_attempt(&store, "diag-a", "USER_DATA_STREAM", true, now - 120_000)
            .await
            .unwrap();
        record_connection_attempt(&store, "diag-a", "USER_DATA_STREAM", true, now)
            .await
            .unwrap();
        record_connection_attempt(&store, "diag-b", "USER_DATA_STREAM", true, now)
            .await
            .unwrap();
        record_connection_attempt(&store, "diag-a", "TRADE_WS_API", false, now)
            .await
            .unwrap();
        record_connection_attempt(&store, "diag-a", "TRADE_WS_API", true, now)
            .await
            .unwrap();
        record_connection_attempt(&store, "diag-a", "MARKET_DATA", false, now)
            .await
            .unwrap();
        store.pool().close().await;
        let restarted = Store::connect(&url).await.unwrap();
        let observed = read(
            &restarted,
            p::GetRuntimeDiagnosticsRequest {
                credential_id: "diag-a".into(),
                schema_version: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(observed.user_stream_reconnect_count, 2);
        assert_eq!(observed.user_stream_reconnects_last_minute, 1);
        assert_eq!(
            observed
                .websocket_scopes
                .iter()
                .map(|m| (
                    m.scope.as_str(),
                    m.status.as_str(),
                    m.reconnect_count,
                    m.reconnects_last_minute
                ))
                .collect::<Vec<_>>(),
            vec![
                ("USER_DATA_STREAM", "OBSERVED", Some(2), Some(1)),
                ("TRADE_WS_API", "OBSERVED", Some(1), Some(1)),
                ("MARKET_DATA", "OBSERVED", Some(0), Some(0))
            ]
        );
        let unknown = read(
            &restarted,
            p::GetRuntimeDiagnosticsRequest {
                credential_id: "diag-unobserved".into(),
                schema_version: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(unknown.websocket_scopes[0].status, "NOT_OBSERVED");
        assert_eq!(unknown.websocket_scopes[0].reconnect_count, None);
        assert_eq!(unknown.websocket_scopes[2].status, "DISABLED");
        assert_eq!(unknown.websocket_scopes[2].reconnect_count, None);
        assert!(!observed.user_stream_ready);
        assert_eq!(observed.user_stream_phase, "unknown");
        restarted.pool().close().await;
        let _ = std::fs::remove_file(path);
    }
}
