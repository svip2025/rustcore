-- M2: Risk Gate (supporting tables)

CREATE TABLE IF NOT EXISTS engine_credential_health (
  credential_id VARCHAR(64) PRIMARY KEY,
  degraded INTEGER NOT NULL DEFAULT 0,
  degraded_reason TEXT,
  consecutive_errors BIGINT NOT NULL DEFAULT 0,
  first_error_ms BIGINT NOT NULL DEFAULT 0,
  last_error_ms BIGINT NOT NULL DEFAULT 0,
  updated_at_ms BIGINT NOT NULL
);
