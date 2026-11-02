-- BINANCE-COND-ORDER-4120: portable Binance Algo identity for protection orders.
--
-- SQLite and MySQL execute this same migration.  It therefore uses only
-- nullable VARCHAR/BIGINT columns and ordinary unique indexes; no filtered
-- index, generated column, or dialect-specific DDL is used.  Both dialects
-- allow multiple rows containing NULL in these composite unique indexes, while
-- still rejecting duplicate non-NULL exchange identities per credential.
--
-- From this migration forward, protection_orders.exchange_order_id stores only
-- the ordinary orderId created after an Algo order triggers (actualOrderId).  It
-- must never store algoId.  Existing rows are intentionally not rewritten:
-- before this migration the column held the legacy conditional orderId from
-- /fapi/v1/order (or a paper marker), so a row with exchange_algo_id IS NULL is
-- historical/legacy data and must be interpreted under that former meaning.

ALTER TABLE protection_orders
  ADD COLUMN exchange_algo_id VARCHAR(128);

ALTER TABLE protection_orders
  ADD COLUMN client_algo_id VARCHAR(128);

ALTER TABLE protection_orders
  ADD COLUMN algo_status VARCHAR(32);

ALTER TABLE protection_orders
  ADD COLUMN algo_updated_at_ms BIGINT;

CREATE UNIQUE INDEX protection_orders_exchange_algo_uniq
  ON protection_orders (credential_id, exchange_algo_id);

CREATE UNIQUE INDEX protection_orders_client_algo_uniq
  ON protection_orders (credential_id, client_algo_id);
