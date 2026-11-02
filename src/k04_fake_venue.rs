//! K04 · loopback Binance USDⓈ-M fake used by the kill-switch and reconcile
//! regression proofs.
//!
//! Only the exchange HTTP boundary is replaced: the production client signs,
//! rate-limits and sends real HTTP to `127.0.0.1`, and this listener answers
//! from a small mutable book (open orders, open/historical Algo orders,
//! positions). Every request is recorded with its merged query/body
//! parameters so tests assert what production actually sent.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
pub(crate) struct RecordedRequest {
    /// `X-MBX-APIKEY` — which credential's key signed this request.
    pub api_key: String,
    pub method: String,
    pub path: String,
    pub params: BTreeMap<String, String>,
}

impl RecordedRequest {
    pub(crate) fn param(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(String::as_str)
    }

    pub(crate) fn is(&self, method: &str, path: &str) -> bool {
        self.method == method && self.path == path
    }
}

#[derive(Default)]
pub(crate) struct FakeVenueBook {
    /// Working ordinary orders (`symbol`, `orderId`, `clientOrderId`, `status`).
    pub open_orders: Vec<Value>,
    /// Ordinary orders that left the book (answerable by `GET /fapi/v1/order`).
    pub order_history: Vec<Value>,
    /// Working Algo orders (`algoId`, `clientAlgoId`, `algoStatus`, `symbol`).
    pub open_algos: Vec<Value>,
    /// Non-working Algo orders still answerable by `GET /fapi/v1/algoOrder`.
    pub algo_history: Vec<Value>,
    /// `positionRisk` rows.
    pub positions: Vec<Value>,
    /// Algo identities (`algoId:<n>` / `clientAlgoId:<s>`) whose cancel fails
    /// with a venue error while they stay working.
    pub failing_algo_cancels: HashSet<String>,
    /// Algo identities whose single-order query fails with a venue error.
    pub failing_algo_queries: HashSet<String>,
    /// Ordinary orderIds whose DELETE is rejected while they remain working.
    pub failing_order_cancels: HashSet<String>,
    /// WO-M1：`POST /fapi/v1/order` 的逐单脚本（先进先出；空则立即成交，
    /// 即历史默认行为）。
    pub order_behaviors: VecDeque<OrderBehavior>,
    /// WO-M7：回环 WS 只发送这份冻结 kline 帧。
    pub kline_frame: Option<String>,
    pub requests: Vec<RecordedRequest>,
}

/// WO-M1 · 普通单提交在替身交易所上的结局。每一种都只改替身自己的账本，
/// 引擎看到的只有真实 HTTP 应答（或超时）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OrderBehavior {
    /// 立即全部成交（reduce-only 按成交量减仓）。
    FillNow,
    /// 业务拒单：HTTP 400 + `{code,msg}`，账本不变、不留订单。
    Reject { code: i64, msg: String },
    /// 受理并挂单（`NEW`，未成交），之后由用例经 [`FakeVenue::fill_order`]
    /// 推进部分 / 全部成交。
    Rest,
    /// 交易所侧已全部成交并入账，但 HTTP 应答压住 `hold_ms` 毫秒才回——
    /// 生产客户端在请求超时后只能把结果记成未知。
    FillButHoldResponse { hold_ms: u64 },
}

/// 替身账本里的十进制数量（只用于用例自己的小数，避免浮点比较）。
fn fake_units(value: &str) -> i128 {
    let value = value.trim();
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or((false, value));
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    assert!(
        fraction.len() <= 8,
        "fake venue quantities carry <= 8 decimals"
    );
    let digits = format!("{integer}{fraction:0<8}");
    let magnitude = digits.parse::<i128>().expect("fake venue decimal");
    if negative {
        -magnitude
    } else {
        magnitude
    }
}

fn fake_text(units: i128) -> String {
    if units == 0 {
        return "0".to_string();
    }
    let negative = units < 0;
    let magnitude = format!("{:09}", units.unsigned_abs());
    let (integer, fraction) = magnitude.split_at(magnitude.len() - 8);
    let integer = integer.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    let mut rendered = if integer.is_empty() {
        "0".to_string()
    } else {
        integer.to_string()
    };
    if !fraction.is_empty() {
        rendered.push('.');
        rendered.push_str(fraction);
    }
    if negative {
        rendered.insert(0, '-');
    }
    rendered
}

impl FakeVenueBook {
    /// WO-M1：交易所推进一张挂着的普通单——成交 `quantity`（累计到
    /// `executedQty`，reduce-only 同步减仓）；累计满额即 `FILLED` 并移出
    /// 挂单簿。返回推进后的订单（含交易所单号与累计成交量）。
    pub(crate) fn fill_order(&mut self, client_order_id: &str, quantity: &str) -> Value {
        let index = self
            .open_orders
            .iter()
            .position(|order| {
                order.get("clientOrderId").and_then(Value::as_str) == Some(client_order_id)
            })
            .expect("fake venue: the order to fill must be resting");
        let mut order = self.open_orders[index].clone();
        let executed =
            fake_units(order["executedQty"].as_str().unwrap_or("0")) + fake_units(quantity);
        let original = fake_units(order["origQty"].as_str().unwrap_or("0"));
        assert!(executed <= original, "fake venue: cannot overfill an order");
        order["executedQty"] = json!(fake_text(executed));
        order["avgPrice"] = json!("100");
        order["updateTime"] = json!(crate::svc::now_ms());
        if order.get("reduceOnly").and_then(Value::as_bool) == Some(true) {
            let symbol = order["symbol"].as_str().unwrap_or_default().to_string();
            self.reduce_position(&symbol, quantity);
        }
        if executed == original {
            order["status"] = json!("FILLED");
            self.open_orders.remove(index);
            self.order_history.push(order.clone());
        } else {
            order["status"] = json!("PARTIALLY_FILLED");
            self.open_orders[index] = order.clone();
        }
        order
    }

    /// 按一笔 reduce-only 成交把该 symbol 的仓位绝对值减少 `quantity`
    /// （单向持仓：`positionAmt` 带符号；不会越过零）。
    fn reduce_position(&mut self, symbol: &str, quantity: &str) {
        for position in self.positions.iter_mut() {
            if position.get("symbol").and_then(Value::as_str) != Some(symbol) {
                continue;
            }
            let amount = fake_units(position["positionAmt"].as_str().unwrap_or("0"));
            let reduce = fake_units(quantity).min(amount.abs());
            let next = if amount < 0 {
                amount + reduce
            } else {
                amount - reduce
            };
            position["positionAmt"] = json!(fake_text(next));
        }
    }
}

pub(crate) struct FakeVenue {
    pub addr: std::net::SocketAddr,
    pub book: Arc<Mutex<FakeVenueBook>>,
    task: tokio::task::JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for FakeVenue {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn venue_error(code: i64, msg: &str) -> (u16, Value) {
    (400, json!({ "code": code, "msg": msg }))
}

fn algo_matches(algo: &Value, params: &BTreeMap<String, String>) -> bool {
    if let Some(id) = params.get("algoId") {
        return algo
            .get("algoId")
            .and_then(Value::as_i64)
            .map(|v| v.to_string())
            == Some(id.clone());
    }
    if let Some(id) = params.get("clientAlgoId") {
        return algo.get("clientAlgoId").and_then(Value::as_str) == Some(id.as_str());
    }
    false
}

fn identity_keys(params: &BTreeMap<String, String>) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(id) = params.get("algoId") {
        keys.push(format!("algoId:{id}"));
    }
    if let Some(id) = params.get("clientAlgoId") {
        keys.push(format!("clientAlgoId:{id}"));
    }
    keys
}

fn respond(
    book: &mut FakeVenueBook,
    method: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> (u16, Value, u64) {
    match respond_order_script(book, method, path, params) {
        Some(scripted) => scripted,
        None => {
            let (status, payload) = respond_book(book, method, path, params);
            (status, payload, 0)
        }
    }
}

/// WO-M1：普通单提交按脚本应答，并把订单记进账本（`GET /fapi/v1/order`
/// 与 `openOrders` 据此作答——交易所真值必须能被重查）。
fn respond_order_script(
    book: &mut FakeVenueBook,
    method: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> Option<(u16, Value, u64)> {
    if (method, path) != ("POST", "/fapi/v1/order") {
        return None;
    }
    let behavior = book
        .order_behaviors
        .pop_front()
        .unwrap_or(OrderBehavior::FillNow);
    let symbol = params.get("symbol").cloned().unwrap_or_default();
    let quantity = params.get("quantity").cloned().unwrap_or_default();
    let reduce_only = params.get("reduceOnly").map(String::as_str) == Some("true");
    let order_id = 990_000 + book.requests.len() as i64;
    let mut order = json!({
        "orderId": order_id,
        "clientOrderId": params.get("newClientOrderId").cloned().unwrap_or_default(),
        "status": "NEW",
        "symbol": symbol,
        "side": params.get("side").cloned().unwrap_or_default(),
        "type": params.get("type").cloned().unwrap_or_default(),
        "origQty": quantity,
        "executedQty": "0",
        "cumQuote": "0",
        "avgPrice": "0",
        "reduceOnly": reduce_only,
        "updateTime": crate::svc::now_ms(),
    });
    let fill_all = |book: &mut FakeVenueBook, order: &mut Value| {
        order["status"] = json!("FILLED");
        order["executedQty"] = json!(quantity.clone());
        order["avgPrice"] = json!("100");
        if reduce_only {
            book.reduce_position(&symbol, &quantity);
        }
        book.order_history.push(order.clone());
    };
    Some(match behavior {
        OrderBehavior::FillNow => {
            fill_all(book, &mut order);
            (200, order, 0)
        }
        OrderBehavior::FillButHoldResponse { hold_ms } => {
            fill_all(book, &mut order);
            (200, order, hold_ms)
        }
        OrderBehavior::Rest => {
            book.open_orders.push(order.clone());
            (200, order, 0)
        }
        OrderBehavior::Reject { code, msg } => {
            let (status, payload) = venue_error(code, &msg);
            (status, payload, 0)
        }
    })
}

fn respond_book(
    book: &mut FakeVenueBook,
    method: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> (u16, Value) {
    let symbol = params.get("symbol").cloned();
    let by_symbol = |rows: &Vec<Value>| -> Value {
        Value::Array(
            rows.iter()
                .filter(|row| {
                    symbol.as_deref().map_or(true, |symbol| {
                        row.get("symbol").and_then(Value::as_str) == Some(symbol)
                    })
                })
                .cloned()
                .collect(),
        )
    };
    match (method, path) {
        ("GET", "/fapi/v1/exchangeInfo") => (
            200,
            json!({"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":1000000}],"symbols":[]}),
        ),
        ("GET", "/fapi/v1/openOrders") => (200, by_symbol(&book.open_orders)),
        ("GET", "/fapi/v1/openAlgoOrders") => (200, by_symbol(&book.open_algos)),
        ("GET", "/fapi/v2/positionRisk") => (200, Value::Array(book.positions.clone())),
        ("GET", "/fapi/v2/balance") => (
            200,
            json!([{"asset":"USDT","balance":"10000","availableBalance":"10000"}]),
        ),
        ("GET", "/fapi/v1/algoOrder") => {
            if identity_keys(params)
                .iter()
                .any(|key| book.failing_algo_queries.contains(key))
            {
                return venue_error(-1000, "fake venue: algo query unavailable");
            }
            book.open_algos
                .iter()
                .chain(book.algo_history.iter())
                .find(|algo| algo_matches(algo, params))
                .map(|algo| (200, algo.clone()))
                .unwrap_or_else(|| venue_error(-2013, "Order does not exist."))
        }
        ("DELETE", "/fapi/v1/algoOrder") => {
            if identity_keys(params)
                .iter()
                .any(|key| book.failing_algo_cancels.contains(key))
            {
                return venue_error(-1000, "fake venue: algo cancel unavailable");
            }
            let Some(index) = book
                .open_algos
                .iter()
                .position(|algo| algo_matches(algo, params))
            else {
                return venue_error(-2011, "Unknown order sent.");
            };
            let mut algo = book.open_algos.remove(index);
            algo["algoStatus"] = json!("CANCELED");
            algo["updateTime"] = json!(crate::svc::now_ms());
            book.algo_history.push(algo.clone());
            (200, algo)
        }
        ("DELETE", "/fapi/v1/order") => {
            if params
                .get("orderId")
                .is_some_and(|id| book.failing_order_cancels.contains(id))
            {
                return venue_error(-1000, "fake venue: ordinary cancel unavailable");
            }
            let index = book.open_orders.iter().position(|order| {
                order.get("symbol").and_then(Value::as_str) == symbol.as_deref()
                    && match (params.get("orderId"), params.get("origClientOrderId")) {
                        (Some(id), _) => {
                            order
                                .get("orderId")
                                .and_then(Value::as_i64)
                                .map(|v| v.to_string())
                                == Some(id.clone())
                        }
                        (None, Some(id)) => {
                            order.get("clientOrderId").and_then(Value::as_str) == Some(id.as_str())
                        }
                        (None, None) => false,
                    }
            });
            match index {
                Some(index) => {
                    let mut order = book.open_orders.remove(index);
                    order["status"] = json!("CANCELED");
                    book.order_history.push(order.clone());
                    (200, order)
                }
                None => venue_error(-2011, "Unknown order sent."),
            }
        }
        ("GET", "/fapi/v1/order") => book
            .open_orders
            .iter()
            .chain(book.order_history.iter())
            .find(|order| {
                order.get("symbol").and_then(Value::as_str) == symbol.as_deref()
                    && match (params.get("orderId"), params.get("origClientOrderId")) {
                        (Some(id), _) => {
                            order
                                .get("orderId")
                                .and_then(Value::as_i64)
                                .map(|v| v.to_string())
                                == Some(id.clone())
                        }
                        (None, Some(id)) => {
                            order.get("clientOrderId").and_then(Value::as_str) == Some(id.as_str())
                        }
                        (None, None) => false,
                    }
            })
            .map(|order| (200, order.clone()))
            .unwrap_or_else(|| venue_error(-2013, "Order does not exist.")),
        // `POST /fapi/v1/order` 由 respond_order_script 按脚本应答。
        _ => venue_error(-9999, "fake venue: unexpected endpoint"),
    }
}

fn parse_params(raw: &str, into: &mut BTreeMap<String, String>) {
    for (name, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        into.insert(name.into_owned(), value.into_owned());
    }
}

async fn serve_one(mut socket: tokio::net::TcpStream, book: Arc<Mutex<FakeVenueBook>>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let content_length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0u8; 4096];
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&bytes[header_end..]).to_string();
    let first = head.lines().next().unwrap_or_default();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut params = BTreeMap::new();
    parse_params(query, &mut params);
    parse_params(&body, &mut params);
    let (status, payload, delay_ms) = {
        let mut book = book.lock().expect("lock fake venue book");
        let api_key = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.trim().eq_ignore_ascii_case("x-mbx-apikey"))
            .map(|(_, value)| value.trim().to_string())
            .unwrap_or_default();
        book.requests.push(RecordedRequest {
            api_key,
            method: method.clone(),
            path: path.to_string(),
            params: params.clone(),
        });
        respond(&mut book, &method, path, &params)
    };
    if delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    let payload = payload.to_string();
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let _ = socket
        .write_all(
            format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            )
            .as_bytes(),
        )
        .await;
}

impl FakeVenue {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the K04 loopback fake venue");
        let addr = listener.local_addr().expect("fake venue address");
        let book = Arc::new(Mutex::new(FakeVenueBook::default()));
        let served = book.clone();
        let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.expect("accept fake venue connection");
                        handlers.spawn(serve_one(socket, served.clone()));
                    }
                    Some(outcome) = handlers.join_next(), if !handlers.is_empty() => {
                        outcome.expect("fake venue handler must run");
                    }
                }
            }
            drop(listener);
            while let Some(outcome) = handlers.join_next().await {
                outcome.expect("drain fake venue handler");
            }
        });
        Self {
            addr,
            book,
            task,
            shutdown: Some(shutdown),
        }
    }

    pub(crate) fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub(crate) fn with_book<R>(&self, edit: impl FnOnce(&mut FakeVenueBook) -> R) -> R {
        edit(&mut self.book.lock().expect("lock fake venue book"))
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.with_book(|book| book.requests.clone())
    }

    /// Call after the production producer has stopped. Close intake and wait
    /// for every accepted HTTP handler before taking the final finite snapshot.
    pub(crate) async fn stop_and_drain(&mut self) -> Vec<RecordedRequest> {
        self.shutdown.take().expect("stop venue once").send(()).ok();
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut self.task)
            .await
            .expect("fake venue must drain within five seconds")
            .expect("fake venue task must finish");
        self.requests()
    }
}

/// WO-M1 · 同一本账、同一套应答，经**真实 TLS** 暴露在 127.0.0.1 上：
/// 生产客户端在显式 testnet venue 下只经 fault-proxy 传输（https + 固定
/// CA）出站，这样凭据的 venue 配置、签名、限流与 HTTP 全部是生产路径，
/// 只有交易所端是本地替身。
pub(crate) struct TlsFakeVenue {
    pub addr: std::net::SocketAddr,
    pub book: Arc<Mutex<FakeVenueBook>>,
    pub ca_path: std::path::PathBuf,
    tls_dir: std::path::PathBuf,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for TlsFakeVenue {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::fs::remove_dir_all(&self.tls_dir);
    }
}

fn tls_serve_one(
    mut socket: rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>,
    book: Arc<Mutex<FakeVenueBook>>,
) {
    use std::io::{Read, Write};
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let read = match socket.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    // WO-M7：TLS 故障代理的真实 WebSocket 握手与帧出口。
    if let Some(key) = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
        .map(|(_, value)| value.trim())
    {
        let path = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let frame = {
            let mut book = book.lock().expect("lock fake venue WS book");
            book.requests.push(RecordedRequest {
                api_key: String::new(),
                method: "GET".into(),
                path: path.into(),
                params: BTreeMap::new(),
            });
            book.kline_frame.clone().expect("configured kline fixture")
        };
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        write!(socket, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").expect("WS upgrade");
        socket.flush().expect("flush WS upgrade");
        let mut ws = tokio_tungstenite::tungstenite::WebSocket::from_raw_socket(
            socket,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        );
        ws.send(tokio_tungstenite::tungstenite::Message::Text(frame))
            .expect("send kline");
        let _ = ws.close(None);
        return;
    }
    let content_length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => bytes.extend_from_slice(&chunk[..read]),
        }
    }
    let body = String::from_utf8_lossy(&bytes[header_end..]).to_string();
    let first = head.lines().next().unwrap_or_default();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut params = BTreeMap::new();
    parse_params(query, &mut params);
    parse_params(&body, &mut params);
    let (status, payload, delay_ms) = {
        let mut book = book.lock().expect("lock fake venue book");
        let api_key = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.trim().eq_ignore_ascii_case("x-mbx-apikey"))
            .map(|(_, value)| value.trim().to_string())
            .unwrap_or_default();
        book.requests.push(RecordedRequest {
            api_key,
            method: method.clone(),
            path: path.to_string(),
            params: params.clone(),
        });
        respond(&mut book, &method, path, &params)
    };
    if delay_ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
    let payload = payload.to_string();
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let _ = socket.write_all(
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        )
        .as_bytes(),
    );
    let _ = socket.flush();
}

impl TlsFakeVenue {
    pub(crate) fn start() -> Self {
        let tls_dir =
            std::env::temp_dir().join(format!("wo-m1-fake-venue-tls-{}", ulid::Ulid::new()));
        std::fs::create_dir(&tls_dir).expect("create the fake venue TLS directory");
        let ca_path = tls_dir.join("loopback.pem");
        let key_path = tls_dir.join("loopback.key");
        let generated = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=wo-m1-fake-venue",
                "-addext",
                "subjectAltName=IP:127.0.0.1,DNS:fapi.binance.com,DNS:testnet.binancefuture.com",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-addext",
                "extendedKeyUsage=serverAuth",
                "-keyout",
            ])
            .arg(&key_path)
            .arg("-out")
            .arg(&ca_path)
            .output()
            .expect("run openssl for the isolated fake venue certificate");
        assert!(
            generated.status.success(),
            "generate the isolated fake venue TLS certificate"
        );
        let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(&ca_path).expect("open fake venue certificate"),
        ))
        .collect::<Result<Vec<_>, _>>()
        .expect("parse fake venue certificate");
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
            std::fs::File::open(&key_path).expect("open fake venue key"),
        ))
        .expect("parse fake venue key")
        .expect("fake venue key present");
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .expect("fake venue TLS config"),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the TLS fake venue");
        listener
            .set_nonblocking(true)
            .expect("non-blocking fake venue listener");
        let addr = listener.local_addr().expect("fake venue address");
        let book = Arc::new(Mutex::new(FakeVenueBook::default()));
        let served = book.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        std::thread::spawn(move || {
            while !stopping.load(std::sync::atomic::Ordering::SeqCst) {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let _ = socket.set_nonblocking(false);
                let _ = socket.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                let _ = socket.set_write_timeout(Some(std::time::Duration::from_secs(5)));
                let Ok(connection) = rustls::ServerConnection::new(config.clone()) else {
                    continue;
                };
                let socket = rustls::StreamOwned::new(connection, socket);
                let book = served.clone();
                std::thread::spawn(move || tls_serve_one(socket, book));
            }
        });
        Self {
            addr,
            book,
            ca_path,
            tls_dir,
            stop,
        }
    }

    pub(crate) fn rest_base_url(&self) -> String {
        format!("https://{}", self.addr)
    }

    pub(crate) fn ws_base_url(&self) -> String {
        format!("wss://{}", self.addr)
    }

    pub(crate) fn with_book<R>(&self, edit: impl FnOnce(&mut FakeVenueBook) -> R) -> R {
        edit(&mut self.book.lock().expect("lock fake venue book"))
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.with_book(|book| book.requests.clone())
    }
}

pub(crate) fn open_order(symbol: &str, order_id: i64, client_order_id: &str) -> Value {
    json!({
        "symbol": symbol, "orderId": order_id, "clientOrderId": client_order_id,
        "status": "NEW", "side": "SELL", "type": "LIMIT", "origQty": "1",
        "executedQty": "0", "price": "90", "reduceOnly": true, "updateTime": 1
    })
}

pub(crate) fn algo_order(symbol: &str, algo_id: i64, client_algo_id: &str, status: &str) -> Value {
    json!({
        "algoId": algo_id, "clientAlgoId": client_algo_id, "algoStatus": status,
        "symbol": symbol, "side": "SELL", "algoType": "CONDITIONAL",
        "orderType": "STOP_MARKET", "triggerPrice": "90", "quantity": "1",
        "updateTime": 1
    })
}

pub(crate) fn position(symbol: &str, position_amt: &str) -> Value {
    json!({
        "symbol": symbol, "positionAmt": position_amt, "entryPrice": "100",
        "markPrice": "100", "leverage": "1", "unRealizedProfit": "0",
        "liquidationPrice": "0", "positionSide": "BOTH"
    })
}

// WO-M7 D39：仅替换 DNS/TLS 网络边界；生产 venue、transport、签名与 HTTP
// 均原样执行。映射只活在持 env 串行锁的夹具生命周期内，无运行时环境开关。
#[derive(Clone)]
struct FixtureRoute {
    venue: crate::venue::Venue,
    addr: std::net::SocketAddr,
    certificate: reqwest::Certificate,
}

fn fixture_routes() -> &'static Mutex<std::collections::HashMap<String, FixtureRoute>> {
    static ROUTES: std::sync::OnceLock<Mutex<std::collections::HashMap<String, FixtureRoute>>> =
        std::sync::OnceLock::new();
    ROUTES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

pub(crate) struct FixtureRouteGuard(String);

impl Drop for FixtureRouteGuard {
    fn drop(&mut self) {
        fixture_routes().lock().unwrap().remove(&self.0);
    }
}

impl TlsFakeVenue {
    pub(crate) fn route_direct_credential(
        &self,
        credential: &str,
        venue: crate::venue::Venue,
    ) -> FixtureRouteGuard {
        let certificate =
            reqwest::Certificate::from_pem(&std::fs::read(&self.ca_path).unwrap()).unwrap();
        assert!(fixture_routes()
            .lock()
            .unwrap()
            .insert(
                credential.into(),
                FixtureRoute {
                    venue,
                    addr: self.addr,
                    certificate,
                }
            )
            .is_none());
        FixtureRouteGuard(credential.into())
    }
}

pub(crate) fn route_fixture_http(
    credential: &str,
    base_url: &str,
    builder: reqwest::ClientBuilder,
) -> reqwest::ClientBuilder {
    let Some(route) = fixture_routes().lock().unwrap().get(credential).cloned() else {
        return builder;
    };
    assert_eq!(
        base_url,
        route.venue.rest_base_url(),
        "production venue must resolve before fixture DNS"
    );
    let host = url::Url::parse(base_url)
        .unwrap()
        .host_str()
        .unwrap()
        .to_string();
    builder
        .no_proxy()
        .resolve(&host, route.addr)
        .add_root_certificate(route.certificate)
        .redirect(reqwest::redirect::Policy::none())
}
