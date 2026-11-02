CREATE TABLE IF NOT EXISTS binance_egress_owners (
  egress_owner_key VARCHAR(255) PRIMARY KEY,
  owner_id VARCHAR(255) NOT NULL,
  lease_until_ms BIGINT NOT NULL,
  heartbeat_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS binance_egress_identity (
  egress_owner_key VARCHAR(255) PRIMARY KEY,
  expected_source_ip VARCHAR(64),
  observed_source_ip VARCHAR(64),
  observed_at_ms BIGINT,
  probe_failed_at_ms BIGINT,
  mismatch_since_ms BIGINT,
  updated_at_ms BIGINT NOT NULL
);
