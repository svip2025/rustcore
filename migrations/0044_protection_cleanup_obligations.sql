-- D30 (2026-10-02): durable protection-cleanup obligations for closed positions.
--
-- A position that reaches zero through an exit fill or exchange position truth
-- leaves its venue-side protection orders (Binance Algo conditionals, often
-- closePosition=true, plus ordinary reduce-only children) live with no owner.
-- Each close now records one obligation row covering every protection order of
-- that position across ALL generations; a boot-spawned driver cancels them
-- (Algo first, then ordinary, reusing the CancelOrder / L3 primitives) and only
-- marks the obligation complete after exchange truth confirms terminal state.
--
-- SQLite and MySQL run this same migration: only VARCHAR/BIGINT/TEXT columns
-- and ordinary indexes, no partial indexes or dialect-specific DDL.

CREATE TABLE protection_cleanup_obligations (
  position_id VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(160) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  state VARCHAR(24) NOT NULL,
  origin VARCHAR(24) NOT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
  complete_reason VARCHAR(64),
  attempt_outcomes_json TEXT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  completed_at_ms BIGINT,
  PRIMARY KEY (position_id)
);

CREATE INDEX protection_cleanup_pending_scope_idx
  ON protection_cleanup_obligations (credential_id, symbol_key, direction, state);
