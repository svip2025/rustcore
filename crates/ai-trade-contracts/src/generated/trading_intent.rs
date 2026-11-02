// AUTO-GENERATED FROM schemas/trading-intent.schema.json.
// DO NOT EDIT BY HAND. Run `pnpm contracts:generate`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TradingIntent {
    pub schema_version: i64,
    pub intent_id: String,
    pub bot_uid: String,
    pub run_id: String,
    pub cycle_id: String,
    pub snapshot_id: String,
    pub evidence_ids: Vec<String>,
    pub decision_agent_id: String,
    pub model_ids: Vec<String>,
    pub prompt_hash: String,
    pub config_hash: String,
    pub policy_version: String,
    pub action: TradingIntentAction,
    pub symbol_key: String,
    pub entry: EntryIntent,
    pub protection: ProtectionIntent,
    pub conviction: f64,
    pub valid_until_ms: u64,
    pub reasoning: String,
    pub reasoning_digest: String,
}

pub type DecimalString = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EntryIntent {
    pub order_type: EntryIntentOrderType,
    #[serde(deserialize_with = "crate::deserialize_required_nullable")]
    pub limit_price: Option<DecimalString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_slippage_bps: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionIntent {
    pub stop_loss_price: DecimalString,
    pub take_profit_price: DecimalString,
    pub required: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum TradingIntentAction {
    #[serde(rename = "open_long")]
    OpenLong,
    #[serde(rename = "open_short")]
    OpenShort,
    #[serde(rename = "close_long")]
    CloseLong,
    #[serde(rename = "close_short")]
    CloseShort,
    #[serde(rename = "hold")]
    Hold,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum EntryIntentOrderType {
    #[serde(rename = "market")]
    Market,
    #[serde(rename = "limit")]
    Limit,
}
