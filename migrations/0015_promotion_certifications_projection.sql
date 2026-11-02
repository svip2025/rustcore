-- P3/T3.6-a engine-local projection of authoritative MySQL certifications.
--
-- Missing projection data is deliberately fail-closed for live_small/live
-- opening intents. paper is outside the rule, while demo retains its explicit
-- testnet-venue authorization so an initially empty projection cannot disable
-- the testnet evidence needed to produce the first certification.

CREATE TABLE IF NOT EXISTS promotion_certifications_projection (
  cert_id VARCHAR(64) NOT NULL,
  bot_uid VARCHAR(64) NOT NULL,
  stage VARCHAR(16) NOT NULL,
  config_hash VARCHAR(80) NOT NULL,
  prompt_hash VARCHAR(80) NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  code_commit VARCHAR(40) NOT NULL,
  passed BIGINT NOT NULL,
  invalidated_at_ms BIGINT,
  certified_at_ms BIGINT NOT NULL,
  PRIMARY KEY (cert_id),
  UNIQUE (bot_uid, stage, config_hash, prompt_hash, policy_version, code_commit)
);

CREATE INDEX promotion_certifications_projection_bot_stage_idx
  ON promotion_certifications_projection (bot_uid, stage);
