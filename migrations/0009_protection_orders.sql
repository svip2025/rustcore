-- P1/T1.7-a portable typed protection-order storage.
--
-- SQLite and MySQL run this same migration set. Keep decimal values as text
-- and avoid partial/conditional indexes or dialect-specific timestamp syntax.

ALTER TABLE engine_orders ADD COLUMN position_id VARCHAR(64);
ALTER TABLE engine_orders ADD COLUMN order_role VARCHAR(32);

CREATE INDEX engine_orders_position_idx
  ON engine_orders (position_id);

CREATE TABLE IF NOT EXISTS protection_orders (
  protection_order_id VARCHAR(64) PRIMARY KEY,
  position_id VARCHAR(64) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  kind VARCHAR(8) NOT NULL,
  generation BIGINT NOT NULL DEFAULT 1,
  engine_order_id VARCHAR(64),
  exchange_order_id VARCHAR(128),
  trigger_price VARCHAR(64) NOT NULL,
  reduce_only INTEGER NOT NULL DEFAULT 1,
  close_position INTEGER NOT NULL DEFAULT 1,
  status VARCHAR(32) NOT NULL,
  verified_from_exchange INTEGER NOT NULL DEFAULT 0,
  verified_at_ms BIGINT,
  created_at_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL
);

CREATE UNIQUE INDEX protection_orders_pos_kind_gen_uniq
  ON protection_orders (position_id, kind, generation);

CREATE INDEX protection_orders_pos_idx
  ON protection_orders (position_id, status);
