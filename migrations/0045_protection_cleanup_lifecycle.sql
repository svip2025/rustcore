-- M0 fix (2026-10-02, review claude-review-m0 P0-1 / P2-5): bind each
-- protection-cleanup obligation to ONE close lifecycle of its position.
--
-- reconcile's ReopenClosed reuses a closed position_id. An obligation keyed
-- by position_id alone could therefore still cancel the protection of the
-- reopened lifecycle. close_revision records the position_state revision the
-- arming close wrote; every driver CAS carries (position_id, close_revision),
-- the reopen transaction retires a non-complete obligation, and a later close
-- re-arms the row at its own revision. Rows armed before this migration keep
-- revision 0 (their lifecycle is still the only one they can describe).
--
-- The due-scan index serves the driver's 1 Hz `state = 'pending' AND
-- next_attempt_at_ms <= ?` scan; the obligation table is append-only.
--
-- Additive on purpose: 0044 stays byte-identical. Both statements are single
-- portable DDL (SQLite and MySQL run this same file), and each one is its
-- own registered crash re-entry step in store.rs.

ALTER TABLE protection_cleanup_obligations ADD COLUMN close_revision BIGINT NOT NULL DEFAULT 0;

CREATE INDEX protection_cleanup_due_scan_idx
  ON protection_cleanup_obligations (state, next_attempt_at_ms);
