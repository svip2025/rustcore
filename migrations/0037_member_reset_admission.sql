-- P4b (ruling #35 D10): member-reset admission gate, bound to the CURRENT
-- engine boot cycle, plus the runtime provenance timeline.
--
-- member_reset_admission is a single row (id=1) seeded DISABLED. enabled=1 is
-- valid ONLY while admitted_boot_id equals the running process's boot id;
-- every startup unconditionally disables it and bumps admission_version
-- (FA2 §1.3), so an admission never survives an unproven restart cycle. The
-- persistent row and history are audit facts, not cross-boot authorization.
--
-- member_reset_admission_history keeps the append-only record (admitted /
-- revoked_boot / revoked_fence). Per the Lead closeout it carries exactly
-- event_id, boot_id, action, basis, at_ms, actor -- no admission_version
-- column; the version lives and is checked on the admission singleton only.
--
-- engine_runtime_provenance registers every new-protocol boot (old binaries
-- never write it). It is a timeline for admission attestation, not a
-- mechanical exclusivity proof.
--
-- Both backends run this same file. Single statements, no CHECK, no COLLATE.

CREATE TABLE IF NOT EXISTS member_reset_admission (
  id INT NOT NULL PRIMARY KEY,
  enabled SMALLINT NOT NULL DEFAULT 0,
  admitted_boot_id VARCHAR(32) NOT NULL DEFAULT '',
  admission_version BIGINT NOT NULL DEFAULT 0,
  basis VARCHAR(200) NOT NULL DEFAULT '',
  admitted_at_ms BIGINT NOT NULL DEFAULT 0,
  admitted_by VARCHAR(128) NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS member_reset_admission_history (
  event_id VARCHAR(32) NOT NULL PRIMARY KEY,
  boot_id VARCHAR(32) NOT NULL,
  action VARCHAR(16) NOT NULL,
  basis VARCHAR(200) NOT NULL,
  at_ms BIGINT NOT NULL,
  actor VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS engine_runtime_provenance (
  boot_id VARCHAR(32) NOT NULL PRIMARY KEY,
  protocol_version SMALLINT NOT NULL,
  started_at_ms BIGINT NOT NULL
);

INSERT INTO member_reset_admission
  (id, enabled, admitted_boot_id, admission_version, basis, admitted_at_ms, admitted_by)
VALUES (1, 0, '', 0, '', 0, '');
