CREATE TABLE IF NOT EXISTS binance_rate_limit_cooldowns (
  egress_scope VARCHAR(255) PRIMARY KEY,
  cooldown_until_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);
