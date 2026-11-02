-- M2: Reconcile (supporting tables)

CREATE TABLE IF NOT EXISTS engine_positions (
  credential_id VARCHAR(64) NOT NULL,
  exchange VARCHAR(32) NOT NULL,
  market_kind VARCHAR(16) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  symbol_key VARCHAR(128),
  position_amt VARCHAR(64) NOT NULL,
  entry_price VARCHAR(64) NOT NULL,
  mark_price VARCHAR(64) NOT NULL,
  leverage VARCHAR(32) NOT NULL,
  unrealized_pnl VARCHAR(64) NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  raw_json BLOB,
  PRIMARY KEY (credential_id, symbol)
);

CREATE INDEX engine_positions_credential_updated_idx
  ON engine_positions (credential_id, updated_at_ms);

CREATE TABLE IF NOT EXISTS engine_balances (
  credential_id VARCHAR(64) NOT NULL,
  exchange VARCHAR(32) NOT NULL,
  market_kind VARCHAR(16) NOT NULL,
  asset VARCHAR(32) NOT NULL,
  wallet_balance VARCHAR(64) NOT NULL,
  available_balance VARCHAR(64) NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  raw_json BLOB,
  PRIMARY KEY (credential_id, asset)
);

CREATE INDEX engine_balances_credential_updated_idx
  ON engine_balances (credential_id, updated_at_ms);
