-- kill-switch 事件行的代际号。
--
-- 0023 给 `kill_switch_state` 与 `kill_switch_reconcile_status` 加了
-- `flatten_generation`，但 `record_reconcile_for_scope` 回写**审计事件**那一步
-- 仍然是「按 `armed_at_ms DESC` 取最新一条未 reset 的事件，无条件写进去」：
-- 一次早于当前 latch 的对账真值即便被 state 的代际谓词挡在门外，照样会把
-- `reconcile_clean` / `flatten_state` 写到**新一轮**的事件行上——审计面因此
-- 可以显示"这一轮已经 clean/flattened"，而 state 里根本没有发生过这件事。
--
-- 纯 DDL：不改任何存量行。存量事件行取默认 0，与 `kill_switch_state` 的
-- 存量默认值同源（"从未被 latch 过"），第一次 arm/latch 之后所有比较都在
-- 同一套号里进行。
ALTER TABLE kill_switch_events
  ADD COLUMN flatten_generation BIGINT NOT NULL DEFAULT 0;
