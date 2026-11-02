-- P4b (ruling #35 D8a/D9): per-condition reconcile truth bits.
--
-- The persisted reconcile row historically carries only two aggregates
-- (reconcile_clean, flatten_complete); the five underlying bits of
-- ReconcileTruth are not derivable from them. record_reconcile_for_scope,
-- which holds the full truth, writes these columns in the same transaction.
--
-- NULL = legacy row or never projected: reported as KSTRI_UNKNOWN by the read
-- RPC, never coerced to false or a measured zero (engine safety contract). Zero DML,
-- no backfill; the next projection covers a credential naturally.

ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_positions_zero BIGINT;
ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_orders_empty BIGINT;
ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_algo_orders_empty BIGINT;
ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_local_zero_diff BIGINT;
ALTER TABLE kill_switch_reconcile_status ADD COLUMN truth_no_diff BIGINT;
