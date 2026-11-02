// AUTO-GENERATED FROM schemas/data-snapshot.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataSnapshot {
    pub schema_version: i64,
    pub snapshot_id: String,
    pub created_at_ms: u64,
    pub bot_uid: String,
    pub symbol_key: String,
    pub sources: Vec<DataSource>,
    pub market: MarketSnapshot,
    pub account: AccountSnapshot,
    pub quality: DataQuality,
}

pub type DecimalString = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataSource {
    pub capability: String,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub as_of_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub degraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_from: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Candle {
    pub open_time_ms: u64,
    pub o: DecimalString,
    pub h: DecimalString,
    pub l: DecimalString,
    pub c: DecimalString,
    pub v: DecimalString,
    pub closed: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSet {
    pub ema_short: f64,
    pub ema_long: f64,
    pub rsi_14: f64,
    pub macd: f64,
    pub macd_signal: f64,
    pub macd_hist: f64,
    pub bb_upper: f64,
    pub bb_middle: f64,
    pub bb_lower: f64,
    pub atr_14: f64,
    pub vol_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DerivativesSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub funding_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_funding_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_interest: Option<DecimalString>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSnapshot {
    pub last_price: DecimalString,
    pub timeframe_main: String,
    pub candles: BTreeMap<String, Vec<Candle>>,
    pub indicators: BTreeMap<String, IndicatorSet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derivatives: Option<DerivativesSnapshot>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionState {
    pub sl_order_id: String,
    pub sl_price: DecimalString,
    pub tp_order_id: String,
    pub tp_price: DecimalString,
    pub verified_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionSnapshot {
    pub symbol_key: String,
    pub side: PositionSnapshotSide,
    pub qty: DecimalString,
    pub entry_price: DecimalString,
    pub mark_price: DecimalString,
    pub leverage: u64,
    pub unrealized_pnl_usdt: DecimalString,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidation_price: Option<DecimalString>,
    pub opened_at_ms: u64,
    pub age_minutes: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection: Option<ProtectionState>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshot {
    pub equity_usdt: DecimalString,
    pub available_usdt: DecimalString,
    pub used_margin_usdt: DecimalString,
    pub today_realized_pnl_usdt: DecimalString,
    pub open_positions_count: u64,
    pub positions: Vec<PositionSnapshot>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualityCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataQuality {
    pub gate: DataQualityGate,
    pub checks: Vec<QualityCheck>,
    pub allow_open: bool,
    pub allow_close: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum PositionSnapshotSide {
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "short")]
    Short,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum DataQualityGate {
    #[serde(rename = "pass")]
    Pass,
    #[serde(rename = "degraded")]
    Degraded,
    #[serde(rename = "fail")]
    Fail,
}
