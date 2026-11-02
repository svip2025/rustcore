use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use serde_json::{json, Value};
use sqlx::Row;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;

use super::{EngineExecution, EngineExecutionService};
use crate::binance_futures::BinanceFuturesClient;
use crate::proto::{CancelOrderRequest, GetOpenOrdersRequest};
use crate::store::Store;

static INSTALL_DRIVERS: Once = Once::new();
const VECTOR: &str = include_str!("../tests/fixtures/open_algo_cleanup_v1.json");

struct EnvGuard(String, Option<OsString>);
impl EnvGuard {
    fn set(name: String, value: &str) -> Self {
        let old = std::env::var_os(&name);
        std::env::set_var(&name, value);
        Self(name, old)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.1.take() {
            Some(value) => std::env::set_var(&self.0, value),
            None => std::env::remove_var(&self.0),
        }
    }
}

fn text<'a>(row: &'a Value, key: &str) -> &'a str {
    row[key].as_str().unwrap_or("")
}

async fn seed_order(store: &Store, row: &Value, index: usize) {
    let ts = 1_788_720_000_000_i64 + index as i64;
    sqlx::query("INSERT OR IGNORE INTO position_state (position_id,credential_id,bot_uid,symbol_key,direction,entry_epoch,state,qty,updated_at_ms) VALUES (?,?,?,?,'long',1,?,?,?)")
        .bind(text(row, "position_id")).bind(text(row, "credential_id"))
        .bind(text(row, "bot_uid")).bind(text(row, "symbol_key"))
        .bind(if text(row, "position_id") == "position-old" { "closed" } else { "open" })
        .bind(if text(row, "position_id") == "position-old" { "0" } else { "1" })
        .bind(ts).execute(store.pool()).await.unwrap();
    let role = text(row, "order_role");
    sqlx::query("INSERT INTO engine_orders (engine_order_id,credential_id,bot_uid,run_id,cycle_id,idempotency_key,client_order_id_short,exchange_order_id,exchange,market_kind,symbol_key,action,side,order_type,quantity,reduce_only,status,position_id,order_role,created_at_ms,updated_at_ms) VALUES (?,?,?,'run-fixture',?,?,?,?, 'binance','futures',?,?,'long',?,'1',?,?,?,?,?,?)")
        .bind(text(row, "engine_order_id")).bind(text(row, "credential_id"))
        .bind(text(row, "bot_uid")).bind(text(row, "cycle_id"))
        .bind(format!("idem-{}", text(row, "engine_order_id")))
        .bind(text(row, "client_algo_id"))
        .bind(row["exchange_order_id"].as_str())
        .bind(text(row, "symbol_key"))
        .bind(if role == "entry" { "open_long" } else { "close_long" })
        .bind(match role { "protection_sl" => "stop_market", "protection_tp" => "take_profit_market", _ => "market" })
        .bind(i64::from(role != "entry")).bind(text(row, "status"))
        .bind(text(row, "position_id")).bind(role).bind(ts).bind(ts)
        .execute(store.pool()).await.unwrap();
    if role.starts_with("protection_") {
        sqlx::query("INSERT INTO protection_orders (protection_order_id,position_id,credential_id,kind,generation,engine_order_id,exchange_algo_id,client_algo_id,trigger_price,reduce_only,close_position,status,verified_from_exchange,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?,'100',1,0,?,?,?,?)")
            .bind(format!("po-{}", text(row, "engine_order_id")))
            .bind(text(row, "position_id")).bind(text(row, "credential_id"))
            .bind(role.trim_start_matches("protection_"))
            .bind(row["generation"].as_i64().unwrap())
            .bind(text(row, "engine_order_id")).bind(row["exchange_algo_id"].as_str())
            .bind(row["client_algo_id"].as_str()).bind(text(row, "status"))
            .bind(i64::from(matches!(text(row, "status"), "confirmed" | "triggering" | "triggered")))
            .bind(ts).bind(ts).execute(store.pool()).await.unwrap();
    }
}

async fn fixture() -> (Store, PathBuf, Value) {
    INSTALL_DRIVERS.call_once(sqlx::any::install_default_drivers);
    let path = std::env::temp_dir().join(format!("open-algo-list-{}.db", ulid::Ulid::new()));
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let vector: Value = serde_json::from_str(VECTOR).unwrap();
    for (index, row) in vector["orders"].as_array().unwrap().iter().enumerate() {
        seed_order(&store, row, index).await;
    }
    (store, path, vector)
}

async fn list(
    service: &EngineExecutionService,
    credential: &str,
    symbol: &str,
    cycle: &str,
) -> BTreeMap<String, String> {
    service
        .get_open_orders(Request::new(GetOpenOrdersRequest {
            credential_id: credential.into(),
            symbol_key: symbol.into(),
            cycle_id: cycle.into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .orders
        .into_iter()
        .map(|row| (row.engine_order_id, row.status))
        .collect()
}

#[tokio::test]
async fn get_open_orders_keeps_active_algo_truth_filters_and_limit() {
    let (store, path, vector) = fixture().await;
    let service = EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
    let credential = text(&vector, "credential_id");
    let observed = list(&service, credential, "", "").await;
    let expected = vector["expected_visible_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        observed
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    for row in vector["orders"].as_array().unwrap() {
        if let Some(status) = observed.get(text(row, "engine_order_id")) {
            assert_eq!(
                status,
                text(row, "status"),
                "wire status must remain authoritative"
            );
        }
    }
    assert_eq!(
        list(&service, credential, "binance|futures|ETH|USDT|USDT", "")
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["foreign-symbol"]
    );
    assert_eq!(
        list(&service, credential, "", "cycle-new")
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["samebot-newposition"]
    );
    let filtered = list(
        &service,
        credential,
        text(&vector, "symbol_key"),
        "cycle-old",
    )
    .await;
    assert!(
        !filtered.contains_key("samebot-newposition") && !filtered.contains_key("foreign-symbol")
    );
    assert!(filtered.contains_key("sl-old") && filtered.contains_key("tp-old"));
    assert!(list(&service, "unowned-credential", "", "")
        .await
        .is_empty());
    for index in 0..120 {
        let mut row = vector["orders"][6].clone();
        row["engine_order_id"] = json!(format!("limit-{index:03}"));
        row["credential_id"] = json!("limit-credential");
        row["position_id"] = json!("position-limit");
        seed_order(&store, &row, 1000 + index).await;
    }
    let limited = service
        .get_open_orders(Request::new(GetOpenOrdersRequest {
            credential_id: "limit-credential".into(),
            symbol_key: String::new(),
            cycle_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .orders;
    assert_eq!(limited.len(), 100);
    assert_eq!(limited.first().unwrap().engine_order_id, "limit-119");
    assert_eq!(limited.last().unwrap().engine_order_id, "limit-020");
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
}

fn exchange_env(credential: &str) -> Vec<EnvGuard> {
    let (key, secret) = crate::binance_futures::key_env_names(credential);
    vec![
        EnvGuard::set(crate::venue::venue_env_name(credential), "testnet"),
        EnvGuard::set(key, "local-test-key"),
        EnvGuard::set(secret, "local-test-secret"),
    ]
}

struct AlgoLookupLoopback {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<Value>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

fn lookup_updated_at_ms(vector: &Value, engine_order_id: &str) -> i64 {
    // Use the frozen vector's original row index and the same epoch as seed_order.
    let index = vector["orders"]
        .as_array()
        .unwrap()
        .iter()
        .position(|row| text(row, "engine_order_id") == engine_order_id)
        .unwrap();
    1_788_720_000_000_i64 + index as i64
}

impl AlgoLookupLoopback {
    async fn start(vector: &Value) -> Self {
        let expected: BTreeMap<String, Value> = vector["orders"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| {
                vector["expected_cancel_ids"]
                    .as_array()
                    .unwrap()
                    .contains(&row["engine_order_id"])
            })
            .map(|row| {
                let id = text(row, "exchange_algo_id");
                (
                    id.to_string(),
                    json!({"algoId": id.parse::<i64>().unwrap(),
                        "clientAlgoId": text(row, "client_algo_id"),
                        "algoStatus": "CANCELED",
                        "updateTime": lookup_updated_at_ms(vector, text(row, "engine_order_id"))}),
                )
            })
            .collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("owned loopback HTTP listener");
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0, "incomplete GetOrder HTTP request");
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() <= 16_384, "oversized GetOrder HTTP request");
                }
                let headers = std::str::from_utf8(&bytes).unwrap();
                let mut first = headers.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap();
                let target = first.next().unwrap();
                assert!(
                    target.starts_with('/'),
                    "loopback requires origin-form HTTP"
                );
                let url = url::Url::parse(&format!("http://{address}{target}")).unwrap();
                let query: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
                let query_keys: Vec<_> = query.keys().map(String::as_str).collect();
                let api_key_matches = headers.lines().skip(1).any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.eq_ignore_ascii_case("X-MBX-APIKEY")
                            && value.trim() == "local-test-key"
                    })
                });
                let identity = query.get("algoId");
                let response = identity.and_then(|id| expected.get(id));
                let valid = method == "GET"
                    && url.path() == "/fapi/v1/algoOrder"
                    && query_keys == ["algoId", "recvWindow", "signature", "timestamp"]
                    && url.query_pairs().count() == 4
                    && query
                        .get("timestamp")
                        .is_some_and(|s| s.parse::<u64>().is_ok())
                    && query
                        .get("recvWindow")
                        .is_some_and(|s| s.parse::<u64>().is_ok())
                    && query.get("signature").is_some_and(|s| !s.is_empty())
                    && api_key_matches
                    && response.is_some();
                observed.lock().unwrap().push(json!({
                    "method": method, "path": url.path(), "algo_id": identity,
                    "query_keys": query_keys, "api_key_matches": api_key_matches,
                    "valid": valid, "response_update_time": response.map(|body| &body["updateTime"])
                }));
                let (status, body) = if valid {
                    ("200 OK", response.unwrap().to_string())
                } else {
                    (
                        "400 Bad Request",
                        json!({"code": -1100, "msg": "unexpected loopback algo query"}).to_string(),
                    )
                };
                let wire = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(wire.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            address,
            requests,
            task: Some(task),
        }
    }

    async fn stop(&mut self) -> Vec<Value> {
        if let Some(task) = self.task.take() {
            task.abort();
            if let Err(error) = task.await {
                assert!(error.is_cancelled(), "loopback HTTP task failed: {error}");
            }
        }
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for AlgoLookupLoopback {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn exact_delete_query(request: &str) -> BTreeMap<String, String> {
    assert!(request.starts_with("DELETE "), "{request}");
    let target = request.split_whitespace().nth(1).unwrap();
    let url = url::Url::parse(target).unwrap();
    assert_eq!(url.path(), "/fapi/v1/algoOrder");
    let query: BTreeMap<_, _> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(query.contains_key("signature") && query.contains_key("timestamp"));
    assert!(!query.contains_key("symbol"), "no symbol-wide cancellation");
    query
}

async fn assert_canceled(store: &Store, id: &str) {
    let row = sqlx::query("SELECT eo.status AS order_status,po.status AS protection_status,po.algo_status FROM engine_orders eo JOIN protection_orders po ON po.engine_order_id=eo.engine_order_id WHERE eo.engine_order_id=?")
        .bind(id).fetch_one(store.pool()).await.unwrap();
    assert_eq!(row.get::<String, _>("order_status"), "canceled");
    assert_eq!(row.get::<String, _>("protection_status"), "canceled");
    assert_eq!(row.get::<String, _>("algo_status"), "CANCELED");
}

#[tokio::test]
async fn cancel_order_uses_exact_client_algo_identity_when_exchange_id_is_absent() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let (store, path, vector) = fixture().await;
    let credential = text(&vector, "credential_id");
    let _env = exchange_env(credential);
    let http = BinanceFuturesClient::new_with_mock_responses(
        credential,
        vec![(
            200,
            json!({"algoId": 94001, "clientAlgoId":"client-only-pending", "algoStatus":"CANCELED"})
                .to_string(),
        )],
    );
    let mut service =
        EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
    service.cancel_client_override = Some(http.clone());
    let id = text(&vector, "client_fallback_cancel_id");
    let response = service
        .cancel_order(Request::new(CancelOrderRequest {
            credential_id: credential.into(),
            engine_order_id: id.into(),
            reason: "client fallback".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.accepted, "{}", response.message);
    let requests = http.recorded_requests();
    assert_eq!(requests.len(), 1);
    let query = exact_delete_query(&requests[0]);
    assert_eq!(
        query.get("clientAlgoId").map(String::as_str),
        Some("client-only-pending")
    );
    assert!(!query.contains_key("algoId"));
    assert_canceled(&store, id).await;
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "cross-repo: needs an ai-trade-prototype checkout with its Python env; set AI_TRADE_PROTOTYPE_ROOT and run with --ignored"]
async fn python_client_lists_and_cancels_exact_algos_over_authenticated_grpc() {
    let _serial = crate::binance_futures::tests::env_serial_guard();
    let (store, path, vector) = fixture().await;
    let credential = text(&vector, "credential_id");
    let mut lookup = AlgoLookupLoopback::start(&vector).await;
    let rest_url = format!("http://{}", lookup.address);
    let (key, secret) = crate::binance_futures::key_env_names(credential);
    let _env = vec![
        EnvGuard::set(crate::venue::venue_env_name(credential), ""),
        EnvGuard::set(key, "local-test-key"),
        EnvGuard::set(secret, "local-test-secret"),
        EnvGuard::set("BINANCE_FUTURES_BASE_URL".into(), &rest_url),
        EnvGuard::set(
            "BINANCE_FUTURES_WS_BASE_URL".into(),
            &format!("ws://{}", lookup.address),
        ),
        EnvGuard::set("ENGINE_LIVE_ENABLED".into(), "1"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_PER_SEC".into(), "5"),
        EnvGuard::set("RUSTCORE_BINANCE_WEIGHT_BURST".into(), "100"),
        EnvGuard::set("NO_PROXY".into(), "127.0.0.1,localhost"),
        EnvGuard::set("no_proxy".into(), "127.0.0.1,localhost"),
    ];
    assert_eq!(
        crate::venue::rest_transport_for_credential(credential)
            .unwrap()
            .base_url,
        rest_url,
        "GetOrder must construct its HTTP client for the owned loopback listener"
    );
    let mut cancel_ids: Vec<_> = vector["expected_cancel_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    cancel_ids.sort();
    let transport = crate::venue::rest_transport_for_credential(credential).unwrap();
    let config = crate::rate_limiter::TokenBucketConfig::from_env(transport.venue);
    assert_eq!(config.refill_per_sec, 5.0);
    assert_eq!(config.capacity, 100.0);
    let scope =
        crate::binance_futures::egress_scope_for_transport(transport.venue, &transport.base_url)
            .unwrap();
    let limiter = crate::rate_limiter::CredentialRateLimiter::shared(config);
    let prior_tokens = limiter.stored_tokens(&scope).await;
    assert_eq!(
        prior_tokens, None,
        "the owned loopback scope must start fresh"
    );
    // Production lazily initializes a fresh bucket with config.capacity tokens.
    let initial_burst = config.capacity;
    let open_positions = crate::binance_futures::open_positions_on_egress(&store, &scope)
        .await
        .unwrap();
    let per_position = crate::binance_futures::protectable_exit_weight_per_position();
    let protectable_budget = crate::binance_futures::protectable_exit_budget_1m(
        &config,
        std::env::var("RUSTCORE_BINANCE_PROTECTABLE_EXIT_BUDGET_1M")
            .ok()
            .as_deref(),
    );
    let reserve = crate::binance_futures::exit_weight_reserve_from_env()
        .max((open_positions as f64 * per_position).min(protectable_budget));
    assert_eq!(open_positions, 4);
    assert_eq!(per_position, 13.0);
    assert_eq!(reserve, 52.0);
    // Each exact algo DELETE/GET costs one in the production endpoint table.
    // This wiring test needs all three pairs without waiting for token refill.
    let request_weight = cancel_ids.len() as f64 * 2.0;
    assert!(initial_burst >= reserve + request_weight);
    let fixture_budget = json!({
        "scope": scope, "capacity": config.capacity,
        "refill_per_sec": config.refill_per_sec, "prior_stored_tokens": prior_tokens,
        "initial_available_burst": initial_burst, "open_positions": open_positions,
        "per_position": per_position, "protectable_budget_1m": protectable_budget,
        "reserve": reserve, "three_gets_and_cancels_weight": request_weight
    });
    eprintln!("open algo fixture budget: {fixture_budget}");
    let rows = vector["orders"].as_array().unwrap();
    let responses = cancel_ids.iter().map(|id| {
        let row = rows.iter().find(|row| text(row, "engine_order_id") == id).unwrap();
        (200, json!({"algoId": text(row, "exchange_algo_id").parse::<i64>().unwrap(), "clientAlgoId":text(row, "client_algo_id"), "algoStatus":"CANCELED"}).to_string())
    }).collect();
    let http = BinanceFuturesClient::new_with_mock_responses(credential, responses);
    let mut exec = EngineExecutionService::new(store.clone()).without_background_exchange_tasks();
    exec.cancel_client_override = Some(http.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("real loopback socket required; never skip this wiring proof");
    let address = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let token = format!("local-open-algo-{}", ulid::Ulid::new());
    let events = super::EngineEventsService::new(store.clone(), Duration::from_millis(800));
    let v2 =
        crate::svc_v2::EngineExecutionV2Service::with_shared_execution(store.clone(), exec.clone());
    let router = crate::add_engine_services(
        tonic::transport::Server::builder(),
        exec,
        events,
        v2,
        crate::svc_v2::EngineEventsV2Skeleton,
        Some(crate::AuthInterceptor::new(vec![token.clone()])),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::path::PathBuf::from(
        std::env::var_os("AI_TRADE_PROTOTYPE_ROOT")
            .expect("AI_TRADE_PROTOTYPE_ROOT must point at an ai-trade-prototype checkout"),
    );
    let python = std::env::var_os("RUSTCORE_TEST_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let local = root.join(".venv-gate/bin/python");
            if local.is_file() {
                local
            } else {
                PathBuf::from("python3")
            }
        });
    let python_path =
        std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/local/bin:/usr/bin:/bin"));
    let script = manifest.join("tests/fixtures/open_algo_client_probe.py");
    let vector_path = manifest.join("tests/fixtures/open_algo_cleanup_v1.json");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(python)
            .arg(script)
            .arg(address.to_string())
            .arg(vector_path)
            .env_clear()
            .env("PATH", python_path)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("RUSTCORE_GRPC_AUTH_TOKEN", token)
            .output()
            .expect("run real Python client")
    })
    .await
    .unwrap();
    let _ = stop.send(());
    server.await.unwrap();
    let lookup_requests = lookup.stop().await;
    eprintln!("open algo fixture DB: {}", path.display());
    assert!(
        output.status.success(),
        "Python wire consumer failed:\n{}\n{}\nGetOrder HTTP observations: {lookup_requests:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut observation: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        lookup_requests
            .iter()
            .all(|request| request["valid"] == json!(true)),
        "unexpected GetOrder HTTP query: {lookup_requests:?}"
    );
    let mut observed_algo_ids: Vec<_> = lookup_requests
        .iter()
        .map(|request| text(request, "algo_id").to_string())
        .collect();
    let mut expected_algo_ids: Vec<_> = cancel_ids
        .iter()
        .map(|id| {
            text(
                rows.iter()
                    .find(|row| text(row, "engine_order_id") == id)
                    .unwrap(),
                "exchange_algo_id",
            )
            .to_string()
        })
        .collect();
    observed_algo_ids.sort();
    expected_algo_ids.sort();
    assert_eq!(
        observed_algo_ids, expected_algo_ids,
        "every canceled algorithm must be read exactly once through loopback HTTP"
    );
    let requests = http.recorded_requests();
    assert_eq!(requests.len(), cancel_ids.len());
    let mut persisted_lookup_times = BTreeMap::new();
    for (request, id) in requests.iter().zip(&cancel_ids) {
        let row = rows
            .iter()
            .find(|row| text(row, "engine_order_id") == id)
            .unwrap();
        let query = exact_delete_query(request);
        assert_eq!(
            query.get("algoId").map(String::as_str),
            Some(text(row, "exchange_algo_id"))
        );
        assert!(
            !query.contains_key("clientAlgoId"),
            "prefer exact exchange algoId"
        );
        assert_canceled(&store, id).await;
        let persisted: String = sqlx::query(
            "SELECT CAST(algo_updated_at_ms AS TEXT) AS observed_ms FROM protection_orders WHERE credential_id=? AND engine_order_id=?",
        )
        .bind(credential)
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("observed_ms");
        assert_eq!(
            persisted,
            lookup_updated_at_ms(&vector, id).to_string(),
            "GetOrder must persist the loopback exchange time for {id}, beyond the cancel ACK"
        );
        persisted_lookup_times.insert(id.clone(), persisted);
    }
    for id in ["samebot-newposition", "foreign-bot", "parent-triggered"] {
        let status: String =
            sqlx::query("SELECT status FROM engine_orders WHERE engine_order_id=?")
                .bind(id)
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get("status");
        assert_eq!(
            status,
            if id == "parent-triggered" {
                "triggered"
            } else {
                "confirmed"
            }
        );
    }
    observation["captured_http_requests"] = json!(requests);
    observation["captured_get_order_http_requests"] = json!(lookup_requests);
    observation["get_order_http_endpoint"] = json!(rest_url);
    observation["fixture_budget"] = fixture_budget;
    observation["persisted_get_order_updated_at_ms"] = json!(persisted_lookup_times);
    observation["http_boundary"] = json!(
        "CancelOrder uses in-memory responses; every expected GetOrder algo query reached the owned loopback HTTP server"
    );
    observation["persisted_cancel_count"] = json!(cancel_ids.len());
    if let Some(out) = std::env::var_os("RUSTCORE_OPEN_ALGO_WIRE_OUTPUT") {
        std::fs::write(out, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
    }
    println!(
        "real Python→authenticated gRPC→SQL→protobuf→CancelOrder; exact Algo DELETEs={}",
        requests.len()
    );
    store.pool().close().await;
    std::fs::remove_file(path).unwrap();
}
