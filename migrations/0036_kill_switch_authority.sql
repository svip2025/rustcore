-- P4b (ruling #35 D10/D1): kill-switch authority ownership columns.
--
-- authority_kind / authority_member_id record WHO holds the reset eligibility
-- for the current revision: 0 = no provenance information (never armed after
-- reset, or armed-but-unprovable legacy row), 1 = platform (manual sources),
-- 2 = auto (auto_* sources), 3 = member (member_api). They are the CURRENT
-- ownership and MAY be updated by a same-level platform/auto takeover (in the
-- same transaction on both the state row and the active event row).
--
-- The immutable ARM EXECUTION IDENTITY lives in separate columns added here:
-- trigger_source (existing), arm_request_ref, arm_actor_kind,
-- arm_actor_member_id are written once by the arm transaction and never
-- updated afterwards; reset_request_ref is written once by reset. This keeps
-- "who originally armed" and "who may reset now" as distinct, non-overlapping
-- facts (FA C2.3).
--
-- Single-column single-statement ALTERs only (0008/0023/0026 precedent); no
-- CHECK, no COLLATE, zero DML. Byte-exactness comes from integer encoding
-- (authority/actor kinds) plus closed-world canonical ULID validation with
-- BINARY comparison in the app layer for the two ref string columns.

ALTER TABLE kill_switch_state ADD COLUMN authority_kind SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE kill_switch_state ADD COLUMN authority_member_id INT;
ALTER TABLE kill_switch_events ADD COLUMN authority_kind SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE kill_switch_events ADD COLUMN authority_member_id INT;
ALTER TABLE kill_switch_events ADD COLUMN arm_request_ref VARCHAR(64);
ALTER TABLE kill_switch_events ADD COLUMN reset_request_ref VARCHAR(64);
ALTER TABLE kill_switch_events ADD COLUMN arm_actor_kind SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE kill_switch_events ADD COLUMN arm_actor_member_id INT;
