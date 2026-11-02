CREATE TABLE position_close_requests (
  credential_id VARCHAR(64) NOT NULL,
  request_ref VARCHAR(26) NOT NULL,
  actor_member_id BIGINT NOT NULL,
  position_id VARCHAR(64) NOT NULL,
  expected_revision BIGINT NOT NULL,
  expected_boot_id VARCHAR(32) NOT NULL,
  params_hash VARCHAR(64) NOT NULL,
  scope_key VARCHAR(64) NOT NULL,
  target_json TEXT NOT NULL,
  idempotency_key VARCHAR(80) NOT NULL,
  client_order_id_short VARCHAR(64) NOT NULL,
  phase VARCHAR(24) NOT NULL,
  reason_code VARCHAR(128) NOT NULL,
  engine_order_id VARCHAR(64),
  risk_verdict_id VARCHAR(64),
  outcome_json TEXT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  finalized_at_ms BIGINT,
  PRIMARY KEY (credential_id, request_ref)
);

CREATE TABLE position_close_scope_locks (
  scope_key VARCHAR(64) NOT NULL,
  pending_request_ref VARCHAR(26),
  PRIMARY KEY (scope_key)
);
