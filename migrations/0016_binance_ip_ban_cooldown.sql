CREATE TABLE IF NOT EXISTS binance_ip_ban_cooldowns (
  egress_scope VARCHAR(255) PRIMARY KEY,
  ban_until_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);
