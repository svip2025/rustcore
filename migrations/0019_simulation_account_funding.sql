-- Product Demo simulation funding: persistent idempotency authority.

CREATE TABLE IF NOT EXISTS simulation_account_funding_events (
  idempotency_identity VARCHAR(128) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  amount_usdt VARCHAR(64) NOT NULL,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (idempotency_identity)
);

CREATE INDEX simulation_account_funding_credential_created_idx
  ON simulation_account_funding_events (credential_id, created_at_ms);
