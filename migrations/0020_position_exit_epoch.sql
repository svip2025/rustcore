-- AUD-P2-H6：exit 幂等键按 position 常量派生，close 一次失败即永久锁仓。
--
-- exit 键此前是 `exit|{bot_uid}|{position_id}|{exit_reason}`，其中 exit_reason
-- 是硬编码常量 manual_close——对照 entry 键含 entry_epoch，全仓 grep exit_epoch
-- 零命中。后果链：
--   首次 close 置 state='closing' 并写 pending_idem_key；
--   提交失败（如 v2 空 quantity → -1102）后订单转 unknown，但 position_state
--   **仍停在 closing 且带着那把陈旧的键**；
--   重试命中 engine_orders 唯一键返回 duplicate、永不重新提交；
--   closing ∈ ACTIVE_STATES，同方向新开仓又被 DUPLICATE_ACTIVE_ENTRY 拒绝。
--   净效果：仓位既平不掉也开不了，唯一恢复手段是 L3 flatten。
--
-- 修复需要三件，本迁移提供其存储基础：
--   1) exit_epoch：每次**终态失败**后自增，使重试得到一把新键；
--   2) pre_exit_state：记录进入 closing 之前的状态，失败解锁时据实恢复
--      （open 与 protected 语义不同，不能一律回落到 open）。
-- 解锁路径本身在 svc.rs 的 live_error 分支，reconcile 对 closing 的保留因此
-- 变成「保留**在途**的 close」而不再是「钉死一个死状态」。

ALTER TABLE position_state ADD COLUMN exit_epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE position_state ADD COLUMN pre_exit_state VARCHAR(16);
