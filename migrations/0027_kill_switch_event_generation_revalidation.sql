-- 0026 存量事件代际的可审计复核。
--
-- 0026 给 `kill_switch_events` 加了 `flatten_generation`，**存量行取默认 0**，
-- 并在注释里写「与 `kill_switch_state` 的存量默认值同源」。这句只对
-- "0023 之前从未被 latch 过的库"成立：0023 是在 **R14** 落地的，此后每一次
-- `latch_flatten_pending` 都把 `kill_switch_state.flatten_generation` 推高一格。
-- 一个在 R14 与 R15 之间跑过、且此刻正闩着 flatten 的部署，升级到 0026 之后
-- 就是 **state.generation = N > 0，而它那条 active 事件行 generation = 0**。
--
-- 后果在 `record_reconcile_for_scope`（`kill_switch.rs` 的事件绑代分支）：
-- 本代事件一条都选不出来，`has_live_latch` 又为真 ⇒ 每一次对账都回滚并返回
-- `DiscardedRaced`。这条 latch 因此**永远收敛不了**——`flatten_state` 停在
-- `flattening`，重试任务每一轮都重新撤单 + reduce-only flatten（重复处置），
-- 而 `kill_switch_reconcile_status` 一行都写不进去。
--
-- 这里**零 DML**：不改任何存量行，只加一列"是否已被复核"与一张逐行证据表。
--
-- * `generation_proved_at_ms IS NULL` = 0026 留下的、**未经复核**的事件行；
-- * 非 NULL = 已经过 `kill_switch::revalidate_legacy_event_generations` 逐行复核，
--   结论（adopted / isolated / anchored）与证据写在下面这张表里。
--
-- 复核由 `Store::connect` 在迁移之后、任何对账/重试任务启动之前执行，幂等：
-- 只处理"当前代际下没有 active 事件"的 armed scope，跑过一次之后是空集。
ALTER TABLE kill_switch_events
  ADD COLUMN generation_proved_at_ms BIGINT;

CREATE TABLE kill_switch_event_generation_revalidations (
  scope_key VARCHAR(128) NOT NULL,
  event_id VARCHAR(32) NOT NULL,
  previous_generation BIGINT NOT NULL,
  resolved_generation BIGINT NOT NULL,
  state_generation BIGINT NOT NULL,
  verdict VARCHAR(32) NOT NULL,
  evidence TEXT NOT NULL,
  revalidated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (scope_key, event_id)
);
