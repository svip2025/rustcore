-- P1-A implementation note:
-- the data model section 2.5 specifies a partial unique index,
-- but sqlx runs this same migration on SQLite and MySQL, and MySQL does not
-- support that WHERE-clause index syntax. active_key is therefore non-NULL
-- exactly while a row is active and NULL once it is closed. Both backends
-- allow multiple NULL values in a unique index, making this equivalent to the
-- planned active-row uniqueness rule while keeping the migration portable.

CREATE TABLE IF NOT EXISTS position_state (
  position_id VARCHAR(64) PRIMARY KEY,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  direction VARCHAR(8) NOT NULL,
  entry_epoch BIGINT NOT NULL,
  state VARCHAR(16) NOT NULL,
  active_key VARCHAR(255),
  pending_intent_id VARCHAR(64),
  pending_idem_key VARCHAR(128),
  qty VARCHAR(64) NOT NULL DEFAULT '0',
  entry_price VARCHAR(64),
  opened_at_ms BIGINT,
  closed_at_ms BIGINT,
  last_reconciled_ms BIGINT,
  updated_at_ms BIGINT NOT NULL
);

CREATE UNIQUE INDEX position_state_active_uniq
  ON position_state (active_key);

CREATE INDEX position_state_cred_idx
  ON position_state (credential_id, state);

CREATE INDEX position_state_epoch_idx
  ON position_state (bot_uid, symbol_key, direction, entry_epoch);
