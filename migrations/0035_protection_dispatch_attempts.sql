-- New managed submissions only. Zero DML: missing historical records mean
-- legacy_unknown, never prepared/never_sent. No age-based owner takeover.
CREATE TABLE protection_dispatch_attempts (
  attempt_id VARCHAR(64) NOT NULL PRIMARY KEY,
  engine_order_id VARCHAR(64) NOT NULL,
  protection_order_id VARCHAR(64) NOT NULL,
  owner_token VARCHAR(64) NOT NULL,
  binding_json TEXT NOT NULL,
  revision BIGINT NOT NULL,
  authorized_post_count BIGINT NOT NULL,
  phase VARCHAR(16) NOT NULL,
  dispatch_closed BIGINT NOT NULL,
  sender_finished BIGINT NOT NULL,
  outcome VARCHAR(32),
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  UNIQUE (engine_order_id),
  UNIQUE (protection_order_id)
);
