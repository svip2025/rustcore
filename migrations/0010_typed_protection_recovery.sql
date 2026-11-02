-- P1/T1.7-b typed protection plan persisted before venue submission.
-- It is the restart-safe source for initial attachment and recovery FIX.

CREATE TABLE IF NOT EXISTS position_protection_plans (
  position_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  run_id VARCHAR(64),
  cycle_id VARCHAR(64),
  exchange VARCHAR(32) NOT NULL,
  market_kind VARCHAR(32) NOT NULL,
  symbol_key VARCHAR(160) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  stop_loss_price VARCHAR(64) NOT NULL,
  take_profit_price VARCHAR(64) NOT NULL,
  required INTEGER NOT NULL DEFAULT 1,
  confirm_timeout_ms BIGINT NOT NULL,
  generation BIGINT NOT NULL DEFAULT 1,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);

CREATE INDEX position_protection_plans_credential_idx
  ON position_protection_plans (credential_id, bot_uid);

CREATE TABLE IF NOT EXISTS recovery_order_lookups (
  lookup_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  client_order_id_short VARCHAR(128) NOT NULL,
  completed INTEGER NOT NULL,
  found INTEGER NOT NULL,
  confirmed_never_filled INTEGER NOT NULL,
  status VARCHAR(32),
  observed_at_ms BIGINT NOT NULL
);

CREATE INDEX recovery_order_lookups_client_idx
  ON recovery_order_lookups (credential_id, client_order_id_short, observed_at_ms);

CREATE TABLE IF NOT EXISTS recovery_pending_releases (
  release_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  position_id VARCHAR(64) NOT NULL,
  client_order_id_short VARCHAR(128) NOT NULL,
  entry_epoch BIGINT NOT NULL,
  released_at_ms BIGINT NOT NULL
);
