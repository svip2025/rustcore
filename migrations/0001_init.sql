CREATE TABLE IF NOT EXISTS engine_orders (
  engine_order_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  run_id VARCHAR(64),
  cycle_id VARCHAR(64),
  idempotency_key VARCHAR(80) NOT NULL,
  client_order_id_short VARCHAR(64),
  exchange_order_id VARCHAR(128),
  exchange VARCHAR(32) NOT NULL,
  market_kind VARCHAR(16) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  action VARCHAR(32) NOT NULL,
  side VARCHAR(16) NOT NULL,
  order_type VARCHAR(16) NOT NULL,
  price VARCHAR(64),
  quantity VARCHAR(64) NOT NULL,
  reduce_only INTEGER NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);

CREATE UNIQUE INDEX engine_orders_bot_idem_uniq
  ON engine_orders (bot_uid, idempotency_key);

CREATE INDEX engine_orders_credential_status_idx
  ON engine_orders (credential_id, status);

CREATE INDEX engine_orders_exchange_order_id_idx
  ON engine_orders (credential_id, exchange_order_id);

CREATE INDEX engine_orders_credential_client_order_id_short_idx
  ON engine_orders (credential_id, client_order_id_short);

CREATE TABLE IF NOT EXISTS engine_order_events (
  credential_id VARCHAR(64) NOT NULL,
  seq BIGINT NOT NULL,
  event_id VARCHAR(64) NOT NULL,
  ts_ms BIGINT NOT NULL,
  event_type VARCHAR(32) NOT NULL,
  bot_uid VARCHAR(64),
  run_id VARCHAR(64),
  cycle_id VARCHAR(64),
  idempotency_key VARCHAR(80),
  client_order_id_short VARCHAR(64),
  exchange_order_id VARCHAR(128),
  exchange VARCHAR(32),
  market_kind VARCHAR(16),
  symbol_key VARCHAR(128),
  payload_json BLOB,
  PRIMARY KEY (credential_id, seq),
  UNIQUE (credential_id, event_id)
);

CREATE INDEX engine_order_events_bot_ts_idx
  ON engine_order_events (bot_uid, ts_ms);

CREATE TABLE IF NOT EXISTS engine_checkpoints (
  credential_id VARCHAR(64) PRIMARY KEY,
  last_seq_emitted BIGINT NOT NULL DEFAULT 0,
  updated_at_ms BIGINT NOT NULL
);
