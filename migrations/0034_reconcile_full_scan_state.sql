-- A8 tiered reconciliation: the last successful unbiased account scan must
-- survive process restarts.  Zero DML is intentional: a missing row means the
-- credential has never proved a full scan and therefore is immediately due.
CREATE TABLE reconcile_full_scan_state (
  credential_id VARCHAR(64) NOT NULL,
  last_full_scan_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id)
);
