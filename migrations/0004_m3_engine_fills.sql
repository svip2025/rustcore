-- M3 / PR-EE-18d: structured fills + tradeId dedupe (exchange_trade_id)

CREATE TABLE IF NOT EXISTS engine_fills (
  credential_id VARCHAR(64) NOT NULL,
  exchange_trade_id VARCHAR(64) NOT NULL,
  engine_order_id VARCHAR(64) NOT NULL,
  exchange_order_id VARCHAR(128),
  client_order_id_short VARCHAR(64),
  exchange VARCHAR(32) NOT NULL,
  market_kind VARCHAR(16) NOT NULL,
  symbol_key VARCHAR(128),
  symbol VARCHAR(32),
  side VARCHAR(16),
  fill_qty VARCHAR(64),
  fill_price VARCHAR(64),
  fee VARCHAR(64),
  fee_asset VARCHAR(32),
  realized_pnl VARCHAR(64),
  ts_exchange_ms BIGINT,
  ts_event_ms BIGINT,
  raw_json BLOB,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, exchange_trade_id)
);

CREATE INDEX engine_fills_credential_order_idx
  ON engine_fills (credential_id, engine_order_id);
