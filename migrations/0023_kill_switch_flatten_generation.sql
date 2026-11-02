-- flatten latch 的代际号。
--
-- `latch_flatten_pending` 之前没有任何 identity：一次在**旧一轮**开始、
-- 读完交易所真值之后才落地的 reconcile，可以把**新一轮** latch 直接写成
-- `flattened`（TOCTOU）。新敞口于是从重试集合里消失，而它根本没被平掉。
--
-- 代际号让"这条真值是哪一轮观察到的"成为可比的事实：latch 每次（重新）
-- 闩住就 +1，reconcile 必须在读交易所之前取号，落库时按号 CAS——
-- 旧号的真值只能被丢弃，绝不能收敛新敞口。
--
-- 纯 DDL：不改任何存量行。存量行取默认 0，与"从未被 latch 过"等价；
-- 第一次 latch 之后所有比较都在同一套号里进行。
ALTER TABLE kill_switch_state
  ADD COLUMN flatten_generation BIGINT NOT NULL DEFAULT 0;

ALTER TABLE kill_switch_reconcile_status
  ADD COLUMN flatten_generation BIGINT NOT NULL DEFAULT 0;
