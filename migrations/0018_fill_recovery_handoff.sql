-- P3/T3.9-b: durable fill-recovery scopes and readiness handoff.
--
-- Scopes are registered before a stream cycle starts.  Readiness is stored
-- per credential because one failed symbol leaves the account ledger
-- incomplete.  A process restart or reconnect first writes ready=0 and only
-- a completed REST backfill plus successful reconcile may write ready=1.

CREATE TABLE fill_recovery_scopes (
  credential_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  registered_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, symbol)
);

CREATE TABLE fill_stream_state (
  credential_id VARCHAR(64) NOT NULL,
  generation BIGINT NOT NULL DEFAULT 0,
  ready BIGINT NOT NULL DEFAULT 0,
  phase VARCHAR(32) NOT NULL,
  current_symbol VARCHAR(32),
  cursor_last_trade_id BIGINT,
  failure_reason VARCHAR(512),
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id)
);

CREATE INDEX fill_stream_state_ready_idx
  ON fill_stream_state (ready, updated_at_ms);
