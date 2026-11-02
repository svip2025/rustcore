-- P1/T1.6 portable, engine-authoritative kill-switch state and audit timeline.
--
-- SQLite and MySQL run this same migration set. Keep the DDL deliberately
-- conservative: no partial indexes, no conditional index syntax, and no
-- dialect-specific generated/default timestamp expressions.

CREATE TABLE IF NOT EXISTS kill_switch_state (
  scope_key VARCHAR(128) PRIMARY KEY,
  level VARCHAR(32) NOT NULL,
  reason TEXT,
  trigger_source VARCHAR(64),
  armed_at_ms BIGINT NOT NULL,
  flatten_state VARCHAR(32),
  last_reconcile_clean BIGINT,
  last_reconcile_at_ms BIGINT,
  updated_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS kill_switch_events (
  event_id VARCHAR(32) PRIMARY KEY,
  scope VARCHAR(32) NOT NULL,
  scope_key VARCHAR(128) NOT NULL,
  level VARCHAR(32) NOT NULL,
  trigger_source VARCHAR(64) NOT NULL,
  reason TEXT,
  armed_at_ms BIGINT NOT NULL,
  flatten_state VARCHAR(32),
  reconcile_clean BIGINT,
  reset_by VARCHAR(128),
  reset_at_ms BIGINT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);

-- Per-credential convergence prevents one clean credential from completing or
-- resetting a wider global/bot scope while another credential is still dirty.
CREATE TABLE IF NOT EXISTS kill_switch_reconcile_status (
  scope_key VARCHAR(128) NOT NULL,
  credential_id VARCHAR(128) NOT NULL,
  reconcile_clean BIGINT NOT NULL,
  flatten_complete BIGINT NOT NULL,
  reconciled_at_ms BIGINT NOT NULL,
  PRIMARY KEY (scope_key, credential_id)
);

CREATE INDEX kill_switch_events_scope_idx
  ON kill_switch_events(scope, scope_key, armed_at_ms);

-- A missing credential/bot row is not an active switch. The seeded global
-- row gives the evaluator a determinate `none` state from first startup.
INSERT INTO kill_switch_state (
  scope_key, level, reason, trigger_source, armed_at_ms, flatten_state,
  last_reconcile_clean, last_reconcile_at_ms, updated_at_ms
) VALUES ('global', 'none', 'factory default', 'migration_seed', 0, NULL, NULL, NULL, 0);
