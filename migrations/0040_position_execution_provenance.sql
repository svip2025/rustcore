CREATE TABLE position_execution_provenance (
  position_id VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  entry_epoch BIGINT NOT NULL,
  entry_engine_order_id VARCHAR(64) NOT NULL,
  run_mode VARCHAR(16) NOT NULL,
  execution_venue VARCHAR(16) NOT NULL,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (position_id)
);
