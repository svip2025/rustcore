-- P3/T3.9-a: symbol-scoped fill identity and durable REST replay cursors.
--
-- Binance USD-M userTrades is queried per symbol and its trade id is scoped
-- by that symbol.  The 0004 primary key omitted symbol, so this migration
-- builds the corrected table, copies every row, then keeps the pre-migration
-- table as an immutable recovery copy.  Existing rows with a usable symbol
-- are backfilled exactly.  Rows without one are explicitly marked unknown;
-- they must not be used for cross-symbol inference.

CREATE TABLE engine_fills_symbol_scoped (
  credential_id VARCHAR(64) NOT NULL,
  symbol_identity VARCHAR(64) NOT NULL DEFAULT '__legacy_unknown_symbol__',
  symbol_scope_known BIGINT NOT NULL DEFAULT 0,
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
  PRIMARY KEY (credential_id, symbol_identity, exchange_trade_id)
);

INSERT INTO engine_fills_symbol_scoped (
  credential_id, symbol_identity, symbol_scope_known, exchange_trade_id,
  engine_order_id, exchange_order_id, client_order_id_short, exchange,
  market_kind, symbol_key, symbol, side, fill_qty, fill_price, fee, fee_asset,
  realized_pnl, ts_exchange_ms, ts_event_ms, raw_json, created_at_ms
)
SELECT
  credential_id,
  CASE
    WHEN symbol IS NULL OR TRIM(symbol) = ''
      THEN '__legacy_unknown_symbol__'
    ELSE UPPER(TRIM(symbol))
  END,
  CASE WHEN symbol IS NULL OR TRIM(symbol) = '' THEN 0 ELSE 1 END,
  exchange_trade_id, engine_order_id, exchange_order_id,
  client_order_id_short, exchange, market_kind, symbol_key, symbol, side,
  fill_qty, fill_price, fee, fee_asset, realized_pnl, ts_exchange_ms,
  ts_event_ms, raw_json, created_at_ms
FROM engine_fills;

ALTER TABLE engine_fills RENAME TO engine_fills_legacy_0004;
ALTER TABLE engine_fills_symbol_scoped RENAME TO engine_fills;

CREATE INDEX engine_fills_symbol_credential_order_idx
  ON engine_fills (credential_id, engine_order_id);

CREATE TABLE fill_replay_cursors (
  credential_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  last_trade_id BIGINT NOT NULL,
  last_success_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, symbol)
);

CREATE TABLE fill_replay_cursor_resets (
  reset_id VARCHAR(32) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  old_last_trade_id BIGINT,
  new_last_trade_id BIGINT NOT NULL,
  reason VARCHAR(255) NOT NULL,
  actor VARCHAR(128) NOT NULL,
  reset_at_ms BIGINT NOT NULL,
  PRIMARY KEY (reset_id)
);

CREATE INDEX fill_replay_cursor_resets_scope_idx
  ON fill_replay_cursor_resets (credential_id, symbol, reset_at_ms);
