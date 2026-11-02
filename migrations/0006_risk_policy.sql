-- P1/T1.4 portable risk-policy storage.
--
-- The planned SQLite partial unique index on approved verdicts is not valid
-- MySQL syntax. `approved_intent_key` is therefore populated with intent_id
-- only for approve/resize verdicts and remains NULL for rejects. SQLite and
-- MySQL both permit multiple NULLs in a normal UNIQUE index, so this is the
-- portable equivalent: at most one approving verdict per intent, unlimited
-- append-only rejection records.
--
-- `engine_orders.risk_verdict_id` remains nullable for existing rows. The v1
-- insertion helper rejects an absent verdict before issuing SQL, but a portable
-- cross-dialect constraint that both preserves legacy rows and rejects omitted
-- values is not available here. DB-level NOT NULL remains the documented M6
-- follow-up after backfill.

CREATE TABLE IF NOT EXISTS risk_policies_snapshot (
  policy_version VARCHAR(64) PRIMARY KEY,
  policy_hash VARCHAR(80) NOT NULL,
  policy_json TEXT NOT NULL,
  schema_version BIGINT NOT NULL,
  received_at_ms BIGINT NOT NULL,
  activated_at_ms BIGINT
);

CREATE TABLE IF NOT EXISTS risk_verdicts (
  risk_verdict_id VARCHAR(80) PRIMARY KEY,
  intent_id VARCHAR(128) NOT NULL,
  approved_intent_key VARCHAR(128),
  credential_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  created_at_ms BIGINT NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  policy_hash VARCHAR(80) NOT NULL,
  decision VARCHAR(16) NOT NULL,
  reject_reason_code VARCHAR(64),
  sizing_json TEXT,
  rules_json TEXT NOT NULL,
  budget_as_of_ms BIGINT,
  evaluator_version VARCHAR(64) NOT NULL
);

CREATE INDEX risk_verdicts_intent_idx
  ON risk_verdicts (intent_id);

CREATE INDEX risk_verdicts_bot_time_idx
  ON risk_verdicts (bot_uid, created_at_ms);

CREATE UNIQUE INDEX risk_verdicts_intent_approved_uniq
  ON risk_verdicts (approved_intent_key);

CREATE TABLE IF NOT EXISTS notional_reservations (
  credential_id VARCHAR(64) PRIMARY KEY,
  reserved_usdt VARCHAR(64) NOT NULL DEFAULT '0',
  cap_usdt VARCHAR(64) NOT NULL,
  updated_at_ms BIGINT NOT NULL
);

ALTER TABLE engine_orders ADD COLUMN risk_verdict_id VARCHAR(80);

CREATE INDEX engine_orders_verdict_idx
  ON engine_orders (risk_verdict_id);
