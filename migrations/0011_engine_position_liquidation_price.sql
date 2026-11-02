-- P2/T2.2-b1 account-truth RPC support.
-- This migration is intentionally limited to one nullable VARCHAR column:
-- SQLite and MySQL both accept ADD COLUMN with this definition, and existing
-- rows remain valid. No dialect-specific branch or backfill is required.

ALTER TABLE engine_positions
  ADD COLUMN liquidation_price VARCHAR(64);
