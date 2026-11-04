-- Funding history is exchange truth, never an extra balance credit. Reconciled
-- wallet balances already contain these cash flows.
CREATE TABLE exchange_funding_ledger (
  ledger_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  exchange_transaction_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  asset VARCHAR(16) NOT NULL,
  amount_usdt VARCHAR(64) NOT NULL,
  transaction_time_ms BIGINT NOT NULL,
  observed_at_ms BIGINT NOT NULL
);
CREATE INDEX exchange_funding_scope_time ON exchange_funding_ledger (credential_id, symbol, transaction_time_ms);
CREATE TABLE exchange_funding_observations (
  observation_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  start_time_ms BIGINT NOT NULL,
  end_time_ms BIGINT NOT NULL,
  source_as_of_ms BIGINT NOT NULL,
  entry_count BIGINT NOT NULL,
  total_funding_usdt VARCHAR(64) NOT NULL
);
CREATE TABLE websocket_connection_attempts (
  connection_id VARCHAR(32) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  stream_kind VARCHAR(32) NOT NULL,
  is_reconnect SMALLINT NOT NULL,
  observed_at_ms BIGINT NOT NULL
);
CREATE INDEX websocket_attempt_scope_time ON websocket_connection_attempts (credential_id, stream_kind, observed_at_ms);
