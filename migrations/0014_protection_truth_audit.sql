-- Protection truth projection and timestamp clarification.
--
-- SQLite and MySQL execute this same migration.  This deliberately adds a
-- nullable BIGINT column and backfills it with a plain single-table UPDATE;
-- both statements are shared syntax, unlike a cross-version column rename.
-- The legacy verified_at_ms column remains in place for migration safety but
-- is no longer read or written by rustcore.
--
-- verified_from_exchange is the CURRENT protection-state projection, not an
-- append-only audit fact.  It must move from 1 back to 0 when exact exchange
-- truth reports CANCELED, REJECTED, or EXPIRED.  Making it monotonic would make
-- a canceled protection order look valid and could disguise a naked position
-- as protected.  Immutable history belongs in engine_order_events.

ALTER TABLE protection_orders
  ADD COLUMN last_verified_at_ms BIGINT;

UPDATE protection_orders
SET last_verified_at_ms = verified_at_ms
WHERE verified_at_ms IS NOT NULL;
