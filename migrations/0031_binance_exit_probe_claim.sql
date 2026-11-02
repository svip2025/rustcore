CREATE TABLE IF NOT EXISTS binance_exit_probe_claims (
  egress_scope VARCHAR(255) PRIMARY KEY,
  window_until_ms BIGINT NOT NULL,
  claimed_at_ms BIGINT NOT NULL
);
