use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use tokio_tungstenite::Connector;

pub(crate) const BINANCE_TESTNET_REST_BASE_URL: &str = "https://testnet.binancefuture.com";
pub(crate) const BINANCE_TESTNET_WS_BASE_URL: &str = "wss://stream.binancefuture.com";
pub(crate) const BINANCE_MAINNET_REST_BASE_URL: &str = "https://fapi.binance.com";
pub(crate) const BINANCE_MAINNET_WS_BASE_URL: &str = "wss://fstream.binance.com";

const REST_BASE_URL_ENV: &str = "BINANCE_FUTURES_BASE_URL";
const WS_BASE_URL_ENV: &str = "BINANCE_FUTURES_WS_BASE_URL";
pub(crate) const FAULT_PROXY_REST_BASE_URL_ENV: &str = "BINANCE_FAULT_PROXY_REST_BASE_URL";
pub(crate) const FAULT_PROXY_WS_BASE_URL_ENV: &str = "BINANCE_FAULT_PROXY_WS_BASE_URL";
pub(crate) const FAULT_PROXY_CA_PEM_PATH_ENV: &str = "BINANCE_FAULT_PROXY_CA_PEM_PATH";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyEndpointState {
    Unconfigured,
    Configured,
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyCaState {
    Readable,
    Missing,
    Unreadable,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeploymentStageCeiling {
    DemoOrBelow,
    AboveDemo,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportDecision {
    Direct,
    FaultProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportRejection {
    IncompleteEndpoints,
    VenueNotTestnet,
    CaMissing,
    CaUnreadable,
    CaInvalid,
    LiveEnabled,
    StageAboveDemo,
    StageInvalid,
}

impl fmt::Display for TransportRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IncompleteEndpoints => {
                "fault-proxy transport requires both REST and WebSocket endpoints"
            }
            Self::VenueNotTestnet => {
                "fault-proxy transport requires an explicit testnet credential venue"
            }
            Self::CaMissing => "fault-proxy transport requires a test CA PEM path",
            Self::CaUnreadable => "fault-proxy test CA PEM file is not readable",
            Self::CaInvalid => "fault-proxy test CA PEM must contain exactly one valid certificate",
            Self::LiveEnabled => {
                "fault-proxy transport is forbidden while ENGINE_LIVE_ENABLED is true"
            }
            Self::StageAboveDemo => {
                "fault-proxy transport requires ENGINE_STAGE_MAX no higher than demo"
            }
            Self::StageInvalid => "fault-proxy transport requires a valid ENGINE_STAGE_MAX value",
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct TransportDecisionInput {
    venue: Option<Venue>,
    endpoints: ProxyEndpointState,
    ca: ProxyCaState,
    live_enabled: bool,
    stage_ceiling: DeploymentStageCeiling,
}

fn decide_transport(
    input: TransportDecisionInput,
) -> Result<TransportDecision, TransportRejection> {
    match input.endpoints {
        ProxyEndpointState::Unconfigured => return Ok(TransportDecision::Direct),
        ProxyEndpointState::Incomplete => return Err(TransportRejection::IncompleteEndpoints),
        ProxyEndpointState::Configured => {}
    }
    match input.venue {
        Some(Venue::Testnet) => {}
        Some(Venue::Mainnet) | None => return Err(TransportRejection::VenueNotTestnet),
    }
    match input.ca {
        ProxyCaState::Readable => {}
        ProxyCaState::Missing => return Err(TransportRejection::CaMissing),
        ProxyCaState::Unreadable => return Err(TransportRejection::CaUnreadable),
        ProxyCaState::Invalid => return Err(TransportRejection::CaInvalid),
    }
    if input.live_enabled {
        return Err(TransportRejection::LiveEnabled);
    }
    match input.stage_ceiling {
        DeploymentStageCeiling::DemoOrBelow => Ok(TransportDecision::FaultProxy),
        DeploymentStageCeiling::AboveDemo => Err(TransportRejection::StageAboveDemo),
        DeploymentStageCeiling::Invalid => Err(TransportRejection::StageInvalid),
    }
}

#[derive(Debug, Clone)]
struct FaultProxyRoot {
    pem: Arc<[u8]>,
    certificate_der: rustls::pki_types::CertificateDer<'static>,
}

#[derive(Clone)]
struct FaultProxyConfig {
    rest_base_url: String,
    ws_base_url: String,
    root: FaultProxyRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportMode {
    Direct,
    FaultProxy,
}

impl TransportMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::FaultProxy => "fault_proxy",
        }
    }
}

fn tls_trust_profile(mode: TransportMode) -> TlsTrustProfile {
    TlsTrustProfile {
        built_in_roots_preserved: true,
        added_test_roots: usize::from(mode == TransportMode::FaultProxy),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TlsTrustProfile {
    pub(crate) built_in_roots_preserved: bool,
    pub(crate) added_test_roots: usize,
}

#[derive(Clone)]
pub(crate) struct RestTransport {
    pub(crate) base_url: String,
    pub(crate) venue: Option<Venue>,
    pub(crate) mode: TransportMode,
    root: Option<FaultProxyRoot>,
}

impl RestTransport {
    pub(crate) fn trust_profile(&self) -> TlsTrustProfile {
        tls_trust_profile(self.mode)
    }

    pub(crate) fn apply_test_root(
        &self,
        builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, String> {
        match self.root.as_ref() {
            Some(root) => {
                let certificate = reqwest::Certificate::from_pem(root.pem.as_ref())
                    .map_err(|error| format!("parse fault-proxy REST test CA failed: {error}"))?;
                Ok(builder.add_root_certificate(certificate))
            }
            None => Ok(builder),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WsTransport {
    pub(crate) base_url: String,
    pub(crate) venue: Option<Venue>,
    pub(crate) mode: TransportMode,
    root: Option<FaultProxyRoot>,
}

impl WsTransport {
    pub(crate) fn trust_profile(&self) -> TlsTrustProfile {
        tls_trust_profile(self.mode)
    }

    pub(crate) fn connector(&self) -> Result<Option<Connector>, String> {
        let Some(root) = self.root.as_ref() else {
            return Ok(None);
        };
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let built_in_count = roots.len();
        if built_in_count == 0 {
            return Err("fault-proxy WebSocket TLS config has no built-in roots".to_string());
        }
        roots
            .add(root.certificate_der.clone())
            .map_err(|error| format!("add fault-proxy WebSocket test CA failed: {error}"))?;
        if roots.len() != built_in_count + 1 {
            return Err(
                "fault-proxy WebSocket TLS config did not add exactly one root".to_string(),
            );
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Some(Connector::Rustls(Arc::new(config))))
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum Venue {
    Testnet,
    Mainnet,
}

impl Venue {
    pub(crate) fn rest_base_url(self) -> &'static str {
        match self {
            Self::Testnet => BINANCE_TESTNET_REST_BASE_URL,
            Self::Mainnet => BINANCE_MAINNET_REST_BASE_URL,
        }
    }

    pub(crate) fn ws_base_url(self) -> &'static str {
        match self {
            Self::Testnet => BINANCE_TESTNET_WS_BASE_URL,
            Self::Mainnet => BINANCE_MAINNET_WS_BASE_URL,
        }
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Testnet => formatter.write_str("testnet"),
            Self::Mainnet => formatter.write_str("mainnet"),
        }
    }
}

/// Exact legacy paper identity or a per-account 128-bit lowercase hex
/// namespace. Every other credential is exchange-backed unless other guards
/// reject it.
pub(crate) fn is_simulation_credential(credential_id: &str) -> bool {
    if credential_id == "paper" {
        return true;
    }
    let Some(account_uid) = credential_id.strip_prefix("paper:") else {
        return false;
    };
    account_uid.len() == 32
        && account_uid
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// 凭据身份的三态判别。**不要用 `Option<Venue>` 的 `None` 代替它**——
/// `None` 同时表示「模拟凭据」与「未配置 venue 的交易所凭据」，两者语义相反：
/// 前者永远不该碰交易所，后者只是还没配 venue。混用这两者正是 /// 修掉的那条链路的成因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialKind {
    /// `paper` 或 `paper:<32位小写hex>`——Product Demo / 模拟账户。
    Simulation,
    /// 身份未被证明：要么落在保留的 `paper` 命名空间却不合模拟凭据文法
    /// （拼错一位的 Product Demo 凭据），要么不满足交易所凭据的闭世界文法
    /// （含非 ASCII、零宽字符、控制字符或超长）。两者都**不得**被当成交易所
    /// 凭据——否则会静默拿到全局交易所密钥。一律 fail-closed 拒绝。
    UnprovenIdentity,
    /// 其余一律按交易所凭据处理，是否放行仍由 venue 与 live 开关决定。
    ExchangeBacked,
}

/// 交易所凭据 id 的**闭世界文法**。只接受 ASCII 字母数字与 `._:-`，长度 1..=64。
///
/// 这是「证明它是交易所凭据」而不是「排除已知的 paper 拼法」。黑名单式判别
/// 在本仓已被判定为反模式，且实测可被绕过：`pаper:<32hex>`（第二个字符是
/// 西里尔字母 `а` U+0430）与 `pa<U+200B>per:<32hex>`（零宽空格）都不以 ASCII
/// `paper` 开头，在黑名单实现下会被判成交易所凭据，进而回落到全局 mainnet 密钥。
fn is_wellformed_exchange_credential_id(credential_id: &str) -> bool {
    let value = credential_id.trim();
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// 判别凭据身份。两条纪律：
/// ①`paper` 是**保留命名空间**——以 `paper` 打头却不合模拟凭据文法的串落
///   `UnprovenIdentity`，不会掉进 `ExchangeBacked`；
/// ②`ExchangeBacked` 必须**被证明**（通过上面的闭世界文法），而不是「不是 paper」。
///   任何含非 ASCII、控制字符、零宽字符或超长的 id 一律落
///   `UnprovenIdentity` 而被拒，绝不放行到交易所密钥解析。
pub(crate) fn credential_kind(credential_id: &str) -> CredentialKind {
    if is_simulation_credential(credential_id) {
        return CredentialKind::Simulation;
    }
    if !is_wellformed_exchange_credential_id(credential_id) {
        return CredentialKind::UnprovenIdentity;
    }
    if credential_id
        .trim()
        .to_ascii_lowercase()
        .starts_with("paper")
    {
        return CredentialKind::UnprovenIdentity;
    }
    CredentialKind::ExchangeBacked
}

/// credential identity → secret locator 的**确定性单射**编码。
///
/// **为什么不能用「把非字母数字换成 `_` 再大写」那套**：
/// 那是**有损**映射，`demo-account` / `demo_account` / `demo.account` 会折叠成
/// 同一个 `DEMO_ACCOUNT`，三个不同凭据因此共用同一把交易所密钥。大小写归一
/// 同样有损（`a` 与 `A` 折叠）。有损即可碰撞，碰撞即"拿错密钥下单"。
///
/// 这里改用 **UTF-8 字节的十六进制编码**：hex 是单射的（不同字节串必得不同
/// 编码），且落在 env 变量名允许的字符集内。代价是名字不可读——用可读性换
/// "两个凭据永不共用密钥"，在交易路径上是划算的；`scripts/credential-locator.mjs`
/// 提供正反向换算，运维不必手算。
///
/// **裁决明确禁止的两条**：不得靠收紧既有 credential id 文法来回避碰撞
/// （那会拒掉已部署的凭据）；不得在找不到新 locator 时回落到旧的有损 locator
/// （回落等于把碰撞面原样保留）。
/// **调用前提**：只对通过 `credential_kind` 判为 `ExchangeBacked` 的 id 使用。
/// Rust `trim` 与 Python `str.strip` 剥除的字符集不完全相同（U+001C / U+0085
/// 等 Python 剥而 Rust 不剥）；文法内的 id 不含这些字符，两侧才逐字节恒等。
/// 编排器侧的对应实现见 `execution_orchestrator/credential_locator.py`。
pub(crate) fn credential_secret_locator(credential_id: &str) -> String {
    let mut locator = String::with_capacity(1 + credential_id.trim().len() * 2);
    locator.push('H');
    for byte in credential_id.trim().as_bytes() {
        locator.push_str(&format!("{byte:02X}"));
    }
    locator
}

pub(crate) fn venue_env_name(credential_id: &str) -> String {
    format!("BINANCE_VENUE_{}", credential_secret_locator(credential_id))
}

pub(crate) fn parse_venue(raw: Option<&str>) -> Result<Option<Venue>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Ok(None);
    }
    match value.to_ascii_lowercase().as_str() {
        "testnet" => Ok(Some(Venue::Testnet)),
        "mainnet" => Ok(Some(Venue::Mainnet)),
        _ => Err(format!(
            "invalid Binance venue {value:?}; expected testnet or mainnet"
        )),
    }
}

pub(crate) fn venue_for_credential(credential_id: &str) -> Result<Option<Venue>, String> {
    if is_simulation_credential(credential_id) {
        return Ok(None);
    }
    let env_name = venue_env_name(credential_id);
    let raw = match std::env::var(&env_name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            return Err(format!(
                "cannot read {env_name} for credential_id {credential_id:?}: {error}"
            ))
        }
    };
    parse_venue(raw.as_deref())
        .map_err(|error| format!("{env_name} for credential_id {credential_id:?}: {error}"))
}

#[derive(Clone, Copy)]
enum BaseUrlKind {
    Rest,
    WebSocket,
}

impl BaseUrlKind {
    fn env_name(self) -> &'static str {
        match self {
            Self::Rest => REST_BASE_URL_ENV,
            Self::WebSocket => WS_BASE_URL_ENV,
        }
    }

    fn default_url(self) -> &'static str {
        match self {
            Self::Rest => BINANCE_MAINNET_REST_BASE_URL,
            Self::WebSocket => BINANCE_MAINNET_WS_BASE_URL,
        }
    }

    fn venue_url(self, venue: Venue) -> &'static str {
        match self {
            Self::Rest => venue.rest_base_url(),
            Self::WebSocket => venue.ws_base_url(),
        }
    }
}

fn known_venue_from_url(raw: &str) -> Option<Venue> {
    let parsed = url::Url::parse(raw.trim()).ok()?;
    let host = parsed.host_str()?;
    if host.eq_ignore_ascii_case("testnet.binancefuture.com")
        || host.eq_ignore_ascii_case("stream.binancefuture.com")
    {
        Some(Venue::Testnet)
    } else if host.eq_ignore_ascii_case("fapi.binance.com")
        || host.eq_ignore_ascii_case("fstream.binance.com")
    {
        Some(Venue::Mainnet)
    } else {
        None
    }
}

fn optional_env(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}

fn proxy_endpoint_state(rest: Option<&str>, websocket: Option<&str>) -> ProxyEndpointState {
    match (rest, websocket) {
        (None, None) => ProxyEndpointState::Unconfigured,
        (Some(_), Some(_)) => ProxyEndpointState::Configured,
        (Some(_), None) | (None, Some(_)) => ProxyEndpointState::Incomplete,
    }
}

fn normalize_proxy_endpoint(
    raw: &str,
    expected_scheme: &str,
    name: &str,
) -> Result<String, String> {
    let mut parsed =
        url::Url::parse(raw.trim()).map_err(|error| format!("invalid {name}: {error}"))?;
    if parsed.scheme() != expected_scheme {
        return Err(format!("{name} must use {expected_scheme}"));
    }
    if parsed.host_str().is_none() {
        return Err(format!("{name} must include a host"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!("{name} must not contain user information"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(format!("{name} must not contain a query or fragment"));
    }
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

fn parse_live_enabled(raw: Option<&str>) -> bool {
    raw.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

fn parse_stage_ceiling(raw: Option<&str>) -> DeploymentStageCeiling {
    match raw.unwrap_or("paper").trim().to_ascii_lowercase().as_str() {
        "paper" | "demo" => DeploymentStageCeiling::DemoOrBelow,
        "live_small" | "live" => DeploymentStageCeiling::AboveDemo,
        _ => DeploymentStageCeiling::Invalid,
    }
}

fn load_fault_proxy_root(path: &str) -> Result<FaultProxyRoot, (ProxyCaState, String)> {
    let pem = std::fs::read(path).map_err(|error| {
        (
            ProxyCaState::Unreadable,
            format!("{}: {error}", TransportRejection::CaUnreadable),
        )
    })?;
    let certificates = rustls_pemfile::certs(&mut Cursor::new(pem.as_slice()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            (
                ProxyCaState::Invalid,
                format!("{}: {error}", TransportRejection::CaInvalid),
            )
        })?;
    if certificates.len() != 1 {
        return Err((
            ProxyCaState::Invalid,
            TransportRejection::CaInvalid.to_string(),
        ));
    }
    reqwest::Certificate::from_pem(&pem).map_err(|error| {
        (
            ProxyCaState::Invalid,
            format!("{}: {error}", TransportRejection::CaInvalid),
        )
    })?;
    let mut validation_store = rustls::RootCertStore::empty();
    validation_store
        .add(certificates[0].clone())
        .map_err(|error| {
            (
                ProxyCaState::Invalid,
                format!("{}: {error}", TransportRejection::CaInvalid),
            )
        })?;
    Ok(FaultProxyRoot {
        pem: Arc::from(pem),
        certificate_der: certificates[0].clone(),
    })
}

fn fault_proxy_config_for_venue(venue: Option<Venue>) -> Result<Option<FaultProxyConfig>, String> {
    let rest = optional_env(FAULT_PROXY_REST_BASE_URL_ENV)?;
    let websocket = optional_env(FAULT_PROXY_WS_BASE_URL_ENV)?;
    let endpoints = proxy_endpoint_state(rest.as_deref(), websocket.as_deref());

    if endpoints == ProxyEndpointState::Unconfigured {
        decide_transport(TransportDecisionInput {
            venue,
            endpoints,
            ca: ProxyCaState::Missing,
            live_enabled: false,
            stage_ceiling: DeploymentStageCeiling::DemoOrBelow,
        })
        .map_err(|error| error.to_string())?;
        return Ok(None);
    }
    if endpoints == ProxyEndpointState::Incomplete {
        return Err(TransportRejection::IncompleteEndpoints.to_string());
    }

    let ca_path = optional_env(FAULT_PROXY_CA_PEM_PATH_ENV)?;
    let (ca_state, root, ca_error) = match ca_path.as_deref() {
        None => (ProxyCaState::Missing, None, None),
        Some(path) => match load_fault_proxy_root(path) {
            Ok(root) => (ProxyCaState::Readable, Some(root), None),
            Err((state, error)) => (state, None, Some(error)),
        },
    };
    let live_enabled = parse_live_enabled(optional_env("ENGINE_LIVE_ENABLED")?.as_deref());
    let stage_ceiling = parse_stage_ceiling(optional_env("ENGINE_STAGE_MAX")?.as_deref());
    decide_transport(TransportDecisionInput {
        venue,
        endpoints,
        ca: ca_state,
        live_enabled,
        stage_ceiling,
    })
    .map_err(|error| match error {
        TransportRejection::CaUnreadable | TransportRejection::CaInvalid => {
            ca_error.unwrap_or_else(|| error.to_string())
        }
        _ => error.to_string(),
    })?;

    let rest = normalize_proxy_endpoint(
        rest.as_deref().expect("configured REST endpoint"),
        "https",
        FAULT_PROXY_REST_BASE_URL_ENV,
    )?;
    let ws_base_url = normalize_proxy_endpoint(
        websocket.as_deref().expect("configured WebSocket endpoint"),
        "wss",
        FAULT_PROXY_WS_BASE_URL_ENV,
    )?;
    Ok(Some(FaultProxyConfig {
        rest_base_url: rest,
        ws_base_url,
        root: root.expect("allowed fault-proxy decision has parsed CA"),
    }))
}

fn resolve_base_url(
    credential_id: &str,
    venue: Option<Venue>,
    explicitly_configured_base_url: Option<&str>,
    kind: BaseUrlKind,
) -> Result<String, String> {
    if let Some(venue) = venue {
        if let Some(base_url) = explicitly_configured_base_url {
            if known_venue_from_url(base_url) != Some(venue) {
                return Err(format!(
                    "credential_id {credential_id:?} selects venue {venue}, but {} conflicts with it",
                    kind.env_name()
                ));
            }
        }
        return Ok(kind.venue_url(venue).to_string());
    }

    Ok(explicitly_configured_base_url
        .unwrap_or_else(|| kind.default_url())
        .trim()
        .trim_end_matches('/')
        .to_string())
}

fn base_url_for_credential(credential_id: &str, kind: BaseUrlKind) -> Result<String, String> {
    let venue = venue_for_credential(credential_id)?;
    let configured = match std::env::var(kind.env_name()) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(format!("cannot read {}: {error}", kind.env_name())),
    };
    resolve_base_url(credential_id, venue, configured.as_deref(), kind)
}

pub(crate) fn rest_base_url_for_credential(credential_id: &str) -> Result<String, String> {
    base_url_for_credential(credential_id, BaseUrlKind::Rest)
}

pub(crate) fn ws_base_url_for_credential(credential_id: &str) -> Result<String, String> {
    base_url_for_credential(credential_id, BaseUrlKind::WebSocket)
}

pub(crate) fn rest_transport_for_credential(credential_id: &str) -> Result<RestTransport, String> {
    let venue = venue_for_credential(credential_id)?;
    match fault_proxy_config_for_venue(venue)? {
        Some(proxy) => Ok(RestTransport {
            base_url: proxy.rest_base_url,
            venue,
            mode: TransportMode::FaultProxy,
            root: Some(proxy.root),
        }),
        None => Ok(RestTransport {
            base_url: rest_base_url_for_credential(credential_id)?,
            venue,
            mode: TransportMode::Direct,
            root: None,
        }),
    }
}

pub(crate) fn ws_transport_for_credential(credential_id: &str) -> Result<WsTransport, String> {
    let venue = venue_for_credential(credential_id)?;
    // WO-M7：模拟账户的行情走已配置的 paper WS，不消费交易所故障代理。
    // venue_for_credential 对模拟身份短路，显式交易所配置须先拒绝，不能被
    // 这个分支吞掉；未知凭据仍走下方原有守卫。
    if is_simulation_credential(credential_id) {
        if optional_env(&venue_env_name(credential_id))?.is_some() {
            return Err(TransportRejection::VenueNotTestnet.to_string());
        }
        return Ok(WsTransport {
            base_url: ws_base_url_for_credential(credential_id)?,
            venue,
            mode: TransportMode::Direct,
            root: None,
        });
    }
    match fault_proxy_config_for_venue(venue)? {
        Some(proxy) => Ok(WsTransport {
            base_url: proxy.ws_base_url,
            venue,
            mode: TransportMode::FaultProxy,
            root: Some(proxy.root),
        }),
        None => Ok(WsTransport {
            base_url: ws_base_url_for_credential(credential_id)?,
            venue,
            mode: TransportMode::Direct,
            root: None,
        }),
    }
}

pub(crate) fn validate_fault_proxy_startup_config() -> Result<(), String> {
    let rest = optional_env(FAULT_PROXY_REST_BASE_URL_ENV)?;
    let websocket = optional_env(FAULT_PROXY_WS_BASE_URL_ENV)?;
    match proxy_endpoint_state(rest.as_deref(), websocket.as_deref()) {
        ProxyEndpointState::Unconfigured => return Ok(()),
        ProxyEndpointState::Incomplete => {
            return Err(TransportRejection::IncompleteEndpoints.to_string())
        }
        ProxyEndpointState::Configured => {}
    }

    let mut venues = std::env::vars_os()
        .filter_map(|(name, value)| {
            name.to_str()
                .filter(|name| name.starts_with("BINANCE_VENUE_"))
                .map(|name| (name.to_string(), value.to_string_lossy().into_owned()))
        })
        .collect::<Vec<_>>();
    venues.sort_by(|left, right| left.0.cmp(&right.0));
    if venues.is_empty() {
        return fault_proxy_config_for_venue(None).map(|_| ());
    }
    for (name, raw) in venues {
        let venue = parse_venue(Some(&raw)).map_err(|error| format!("{name}: {error}"))?;
        fault_proxy_config_for_venue(venue)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulation_credential_allowlist_is_exact_and_shape_bounded() {
        let uid = "0123456789abcdef0123456789abcdef";
        assert!(is_simulation_credential("paper"));
        assert!(is_simulation_credential(&format!("paper:{uid}")));

        for rejected in [
            " paper",
            "paper ",
            "PAPER",
            "paper:",
            "paper:abc",
            "paper:0123456789abcdef0123456789abcde",
            "paper:0123456789abcdef0123456789abcdef0",
            "paper:0123456789ABCDEF0123456789ABCDEF",
            "paper_0123456789abcdef0123456789abcdef",
            "paper-0123456789abcdef0123456789abcdef",
            "live-paper-0123456789abcdef0123456789abcdef",
            "credential-containing-paper",
        ] {
            assert!(!is_simulation_credential(rejected), "input={rejected:?}");
        }
    }

    #[test]
    fn product_demo_credential_cross_language_contract_is_exact() {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Fixture {
            account_uid: String,
            credential_id: String,
        }

        // Cross-language contract: TypeScript and Python load this same fixture path.
        let fixtures: Vec<Fixture> = serde_json::from_str(include_str!(
            "../tests/fixtures/product-demo-credential-identities.json"
        ))
        .expect("credential identity fixture must be valid JSON");

        for fixture in fixtures {
            let account_uid = fixture.account_uid;
            let credential_id = fixture.credential_id;
            assert!(
                is_simulation_credential(&credential_id),
                "account_uid={account_uid:?} credential_id={credential_id:?}"
            );
            assert!(!is_simulation_credential(
                &credential_id.to_ascii_uppercase()
            ));
            assert!(!is_simulation_credential(
                &credential_id[..credential_id.len() - 1]
            ));
            assert!(!is_simulation_credential(&format!("{credential_id}0")));
        }
    }

    #[test]
    fn parse_venue_accepts_supported_values_case_and_whitespace() {
        assert_eq!(parse_venue(Some("testnet")), Ok(Some(Venue::Testnet)));
        assert_eq!(parse_venue(Some("mainnet")), Ok(Some(Venue::Mainnet)));
        assert_eq!(parse_venue(Some("  TeStNeT\t")), Ok(Some(Venue::Testnet)));
        assert_eq!(parse_venue(Some(" MAINNET ")), Ok(Some(Venue::Mainnet)));
    }

    #[test]
    fn parse_venue_rejects_unknown_nonempty_value() {
        assert!(parse_venue(Some("production")).is_err());
    }

    #[test]
    fn parse_venue_treats_empty_and_missing_as_unconfigured() {
        assert_eq!(parse_venue(None), Ok(None));
        assert_eq!(parse_venue(Some("  ")), Ok(None));
    }

    #[test]
    fn testnet_rest_base_url_is_the_testnet_constant() {
        assert_eq!(
            Venue::Testnet.rest_base_url(),
            BINANCE_TESTNET_REST_BASE_URL
        );
    }

    #[test]
    fn testnet_ws_base_url_is_the_testnet_constant() {
        assert_eq!(Venue::Testnet.ws_base_url(), BINANCE_TESTNET_WS_BASE_URL);
    }

    #[test]
    fn explicit_venue_rejects_conflicting_explicit_base_url() {
        let error = resolve_base_url(
            "desk.test-1",
            Some(Venue::Testnet),
            Some(BINANCE_MAINNET_REST_BASE_URL),
            BaseUrlKind::Rest,
        )
        .expect_err("mainnet REST host must conflict with testnet venue");
        assert!(error.contains("desk.test-1"));
        assert!(error.contains(REST_BASE_URL_ENV));
    }

    #[test]
    fn explicit_venue_rejects_conflicting_explicit_ws_base_url() {
        let error = resolve_base_url(
            "desk.test-1",
            Some(Venue::Testnet),
            Some(BINANCE_MAINNET_WS_BASE_URL),
            BaseUrlKind::WebSocket,
        )
        .expect_err("mainnet WS host must conflict with testnet venue");
        assert!(error.contains("desk.test-1"));
        assert!(error.contains(WS_BASE_URL_ENV));
    }

    #[test]
    fn explicit_venue_rejects_custom_endpoint_as_conflict() {
        assert!(resolve_base_url(
            "desk.test-1",
            Some(Venue::Testnet),
            Some("https://proxy.example.test/binance"),
            BaseUrlKind::Rest,
        )
        .is_err());
    }

    #[test]
    fn matching_explicit_host_is_accepted_but_venue_owns_the_result() {
        assert_eq!(
            resolve_base_url(
                "desk.test-1",
                Some(Venue::Testnet),
                Some("https://TESTNET.binancefuture.com/custom/path"),
                BaseUrlKind::Rest,
            ),
            Ok(BINANCE_TESTNET_REST_BASE_URL.to_string())
        );
    }

    #[test]
    fn unconfigured_venue_preserves_legacy_explicit_base_url_behavior() {
        assert_eq!(
            resolve_base_url(
                "desk.test-1",
                None,
                Some(" https://legacy.example.test/path/ "),
                BaseUrlKind::Rest,
            ),
            Ok("https://legacy.example.test/path".to_string())
        );
    }

    #[test]
    fn unconfigured_venue_preserves_legacy_mainnet_defaults() {
        assert_eq!(
            resolve_base_url("desk.test-1", None, None, BaseUrlKind::Rest),
            Ok(BINANCE_MAINNET_REST_BASE_URL.to_string())
        );
        assert_eq!(
            resolve_base_url("desk.test-1", None, None, BaseUrlKind::WebSocket),
            Ok(BINANCE_MAINNET_WS_BASE_URL.to_string())
        );
    }

    #[test]
    fn b1_credential_secret_locator_is_shared_by_venue_and_key_names() {
        let credential_id = " desk-test.v1 ";
        let locator = credential_secret_locator(credential_id);
        assert_eq!(locator, "H6465736B2D746573742E7631");
        assert_eq!(
            venue_env_name(credential_id),
            "BINANCE_VENUE_H6465736B2D746573742E7631"
        );
        assert_eq!(
            crate::binance_futures::key_env_names(credential_id),
            (
                "BINANCE_API_KEY_H6465736B2D746573742E7631".to_string(),
                "BINANCE_API_SECRET_H6465736B2D746573742E7631".to_string()
            )
        );
    }

    /// **单射性**：旧的有损归一把这一组全折叠成 `DEMO_ACCOUNT`，
    /// 三个不同凭据因此共用同一把交易所密钥。
    /// 新 locator 必须让它们两两不同。
    #[test]
    fn b1_credential_secret_locator_separates_previously_colliding_ids() {
        let colliding = [
            "demo-account",
            "demo_account",
            "demo.account",
            "demo:account",
            "DEMO_ACCOUNT",
            "Demo-Account",
        ];
        let locators: Vec<String> = colliding
            .iter()
            .map(|id| credential_secret_locator(id))
            .collect();
        for (i, a) in locators.iter().enumerate() {
            for (j, b) in locators.iter().enumerate() {
                if i != j {
                    assert_ne!(
                        a, b,
                        "{:?} 与 {:?} 仍然共用 locator",
                        colliding[i], colliding[j]
                    );
                }
            }
        }
        // 旧实现的折叠值绝不能再出现在任何 locator 里
        for locator in &locators {
            assert!(
                !locator.contains("DEMO_ACCOUNT"),
                "{locator} 仍带有损归一痕迹"
            );
        }
    }

    /// 单射性的一般证明面：hex 编码可逆，逐字节还原得回原串（trim 后）。
    /// 有了可逆性就不需要"抽样断言两两不等"来碰运气。
    #[test]
    fn b1_credential_secret_locator_is_reversible_hence_injective() {
        for credential_id in [
            "demo-account.v1",
            "cred_42",
            "desk-test-v1",
            "a-b",
            "a_b",
            "A_B",
            "binance:sub-account-7",
        ] {
            let locator = credential_secret_locator(credential_id);
            let hex = locator
                .strip_prefix('H')
                .expect("locator must be H-prefixed");
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
                .collect();
            assert_eq!(
                String::from_utf8(bytes).expect("valid utf8"),
                credential_id.trim(),
                "locator 必须能还原回原始 credential id"
            );
        }
    }

    /// 确定性：同一个 id 反复求值必须完全一致（locator 会被写进部署环境，
    /// 不确定就等于每次部署都换一把密钥的名字）。
    #[test]
    fn b1_credential_secret_locator_is_deterministic() {
        for _ in 0..8 {
            assert_eq!(
                credential_secret_locator("demo-account.v1"),
                "H64656D6F2D6163636F756E742E7631"
            );
        }
    }

    fn allowed_proxy_input() -> TransportDecisionInput {
        TransportDecisionInput {
            venue: Some(Venue::Testnet),
            endpoints: ProxyEndpointState::Configured,
            ca: ProxyCaState::Readable,
            live_enabled: false,
            stage_ceiling: DeploymentStageCeiling::DemoOrBelow,
        }
    }

    #[test]
    fn fault_proxy_transport_decision_covers_the_complete_truth_table() {
        let venues = [Some(Venue::Testnet), Some(Venue::Mainnet), None];
        let endpoints = [
            ProxyEndpointState::Configured,
            ProxyEndpointState::Unconfigured,
        ];
        let ca_states = [
            ProxyCaState::Readable,
            ProxyCaState::Missing,
            ProxyCaState::Invalid,
        ];
        let live_states = [false, true];
        let stage_ceilings = [
            DeploymentStageCeiling::DemoOrBelow,
            DeploymentStageCeiling::AboveDemo,
        ];
        let mut cases = 0usize;

        for venue in venues {
            for endpoint_state in endpoints {
                for ca in ca_states {
                    for live_enabled in live_states {
                        for stage_ceiling in stage_ceilings {
                            cases += 1;
                            let input = TransportDecisionInput {
                                venue,
                                endpoints: endpoint_state,
                                ca,
                                live_enabled,
                                stage_ceiling,
                            };
                            let expected = if endpoint_state == ProxyEndpointState::Unconfigured {
                                Ok(TransportDecision::Direct)
                            } else if venue != Some(Venue::Testnet) {
                                Err(TransportRejection::VenueNotTestnet)
                            } else if ca == ProxyCaState::Missing {
                                Err(TransportRejection::CaMissing)
                            } else if ca == ProxyCaState::Invalid {
                                Err(TransportRejection::CaInvalid)
                            } else if live_enabled {
                                Err(TransportRejection::LiveEnabled)
                            } else if stage_ceiling == DeploymentStageCeiling::AboveDemo {
                                Err(TransportRejection::StageAboveDemo)
                            } else {
                                Ok(TransportDecision::FaultProxy)
                            };
                            assert_eq!(decide_transport(input), expected, "input={input:?}");
                        }
                    }
                }
            }
        }

        assert_eq!(cases, 72);
    }

    #[test]
    fn fault_proxy_rejects_mainnet_venue_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.venue = Some(Venue::Mainnet);
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::VenueNotTestnet)
        );
    }

    #[test]
    fn fault_proxy_allows_testnet_when_every_guard_is_valid() {
        assert_eq!(
            decide_transport(allowed_proxy_input()),
            Ok(TransportDecision::FaultProxy)
        );
    }

    #[test]
    fn fault_proxy_rejects_missing_venue_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.venue = None;
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::VenueNotTestnet)
        );
    }

    #[test]
    fn fault_proxy_rejects_missing_ca_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.ca = ProxyCaState::Missing;
        assert_eq!(decide_transport(input), Err(TransportRejection::CaMissing));
    }

    #[test]
    fn fault_proxy_rejects_unreadable_ca_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.ca = ProxyCaState::Unreadable;
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::CaUnreadable)
        );
    }

    #[test]
    fn fault_proxy_rejects_invalid_ca_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.ca = ProxyCaState::Invalid;
        assert_eq!(decide_transport(input), Err(TransportRejection::CaInvalid));
    }

    #[test]
    fn fault_proxy_ca_loader_rejects_unreadable_path_without_fallback() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("definitely-not-present-fault-proxy-ca.pem");
        let (state, _) = load_fault_proxy_root(path.to_str().expect("UTF-8 manifest path"))
            .expect_err("missing CA path must reject client construction");

        assert_eq!(state, ProxyCaState::Unreadable);
    }

    #[test]
    fn fault_proxy_ca_loader_rejects_readable_non_certificate_without_fallback() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let (state, _) = load_fault_proxy_root(path.to_str().expect("UTF-8 manifest path"))
            .expect_err("readable invalid CA file must reject client construction");

        assert_eq!(state, ProxyCaState::Invalid);
    }

    #[test]
    fn fault_proxy_rejects_live_enabled_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.live_enabled = true;
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::LiveEnabled)
        );
    }

    #[test]
    fn fault_proxy_rejects_stage_above_demo_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.stage_ceiling = DeploymentStageCeiling::AboveDemo;
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::StageAboveDemo)
        );
    }

    #[test]
    fn fault_proxy_rejects_invalid_stage_with_every_other_guard_valid() {
        let mut input = allowed_proxy_input();
        input.stage_ceiling = DeploymentStageCeiling::Invalid;
        assert_eq!(
            decide_transport(input),
            Err(TransportRejection::StageInvalid)
        );
    }

    #[test]
    fn fault_proxy_rejects_each_one_sided_endpoint_configuration() {
        let rest_only = proxy_endpoint_state(Some("https://127.0.0.1:8443"), None);
        assert_eq!(rest_only, ProxyEndpointState::Incomplete);
        let mut rest_only_input = allowed_proxy_input();
        rest_only_input.endpoints = rest_only;
        assert_eq!(
            decide_transport(rest_only_input),
            Err(TransportRejection::IncompleteEndpoints)
        );

        let websocket_only = proxy_endpoint_state(None, Some("wss://127.0.0.1:8444"));
        assert_eq!(websocket_only, ProxyEndpointState::Incomplete);
        let mut websocket_only_input = allowed_proxy_input();
        websocket_only_input.endpoints = websocket_only;
        assert_eq!(
            decide_transport(websocket_only_input),
            Err(TransportRejection::IncompleteEndpoints)
        );
    }

    #[test]
    fn fault_proxy_endpoints_require_authenticated_tls_schemes() {
        assert_eq!(
            normalize_proxy_endpoint(
                " https://127.0.0.1:8443/path/ ",
                "https",
                FAULT_PROXY_REST_BASE_URL_ENV,
            ),
            Ok("https://127.0.0.1:8443/path".to_string())
        );
        assert!(normalize_proxy_endpoint(
            "http://127.0.0.1:8443",
            "https",
            FAULT_PROXY_REST_BASE_URL_ENV,
        )
        .is_err());
        assert!(normalize_proxy_endpoint(
            "ws://127.0.0.1:8444",
            "wss",
            FAULT_PROXY_WS_BASE_URL_ENV,
        )
        .is_err());
    }

    #[test]
    fn tls_trust_plan_is_additive_only_for_fault_proxy_transport() {
        assert_eq!(
            tls_trust_profile(TransportMode::Direct),
            TlsTrustProfile {
                built_in_roots_preserved: true,
                added_test_roots: 0,
            }
        );
        assert_eq!(
            tls_trust_profile(TransportMode::FaultProxy),
            TlsTrustProfile {
                built_in_roots_preserved: true,
                added_test_roots: 1,
            }
        );
    }

    #[test]
    fn transport_call_sites_keep_startup_rest_and_both_websocket_guards_live() {
        let main_source = include_str!("main.rs");
        assert!(main_source.contains("validate_fault_proxy_startup_config()"));

        let rest_source = include_str!("binance_futures.rs");
        assert!(rest_source.contains("rest_transport_for_credential(credential_id)"));
        assert!(rest_source.contains("apply_test_root(http_builder)"));

        let venue_source = include_str!("venue.rs");
        let rest_root_add = ["builder.add_root_", "certificate(certificate)"].concat();
        assert!(venue_source.contains(&rest_root_add));
        let built_in_roots = ["webpki_roots::TLS_SERVER_", "ROOTS.iter().cloned()"].concat();
        assert!(venue_source.contains(&built_in_roots));
        let ws_root_add = [".add(root.certificate_", "der.clone())"].concat();
        assert!(venue_source.contains(&ws_root_add));
        let activation_call = ["match fault_proxy_config_for_", "venue(venue)?"].concat();
        assert_eq!(venue_source.matches(&activation_call).count(), 2);
        let incomplete_rejection = [
            "return Err(TransportRejection::Incomplete",
            "Endpoints.to_string())",
        ]
        .concat();
        assert_eq!(venue_source.matches(&incomplete_rejection).count(), 2);

        let user_stream_source = include_str!("binance_user_stream.rs");
        assert!(user_stream_source.contains("ws_transport_for_credential(credential_id)"));
        assert!(user_stream_source.contains("connect_async_tls_with_config"));

        let market_data_source = include_str!("market_data.rs");
        assert!(market_data_source.contains("ws_transport_for_credential(credential_id)"));
        assert!(market_data_source.contains("connect_async_tls_with_config"));
    }

    fn collect_rust_sources(path: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        let entries = std::fs::read_dir(path)
            .unwrap_or_else(|error| panic!("scan {} failed: {error}", path.display()));
        for entry in entries {
            let entry = entry.expect("read source tree entry");
            let entry_path = entry.path();
            if entry_path.is_dir() {
                collect_rust_sources(&entry_path, files);
            } else if entry_path.extension().and_then(|value| value.to_str()) == Some("rs") {
                files.push(entry_path);
            }
        }
    }

    #[test]
    fn rust_source_tree_contains_no_tls_verification_bypass() {
        let forbidden = [
            ["danger_", "accept_invalid_certs"].concat(),
            ["danger_", "accept_invalid_hostnames"].concat(),
            ["tls_built_in_root_certs", "(false)"].concat(),
            ["ServerCert", "Verifier"].concat(),
            ["verify_", "server_cert"].concat(),
            ["with_custom_", "certificate_verifier"].concat(),
        ];
        let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rust_sources(&source_root, &mut files);
        let files_scanned = files.len();
        assert!(
            files_scanned > 0,
            "TLS negative invariant scanned zero Rust files under {}",
            source_root.display()
        );

        for path in files {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {} failed: {error}", path.display()));
            for (line_index, line) in source.lines().enumerate() {
                for needle in &forbidden {
                    assert!(
                        !line.contains(needle),
                        "forbidden TLS verification bypass {needle:?} at {}:{}",
                        path.display(),
                        line_index + 1
                    );
                }
            }
        }
    }
}
