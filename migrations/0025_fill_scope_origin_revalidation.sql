-- 旧派生恢复域的可审计复核。
--
-- 0024 只复核了 `engine_fills`。可 `discover_fill_recovery_scopes` 的恢复域
-- 有**四个**来源，其中两个是 0017/0018 建的旧派生表：
--
--   * `fill_recovery_scopes`（0018）
--   * `fill_replay_cursors`（0017）
--
-- 这两张表**一列身份都没有**——只有 `(credential_id, symbol)`。发现器此前
-- 无条件 UNION 它们：一条没有任何 proved fill、也没有任何双源一致
-- engine_order 支撑的旧行，照样播种一个 Binance USDⓈ-M 恢复域，随后
-- fill recovery 拿着它去逐页回补别人的成交，并把回补结果记成本域真值。
-- 而 `scopes_for_credential`（真正驱动 REST 回补的那一条）读的就是
-- `fill_recovery_scopes` 本身，连 UNION 都不经过。
--
-- 这里**零 DML**：不改任何存量行，只给两张表各加两列"是否已被复核 / 结论"。
--
-- * `scope_proved_at_ms IS NULL` = 0017/0018 留下的、**未经复核**的旧行
--   ——发现器与回补域一律 fail-closed（未证明即无域）；
-- * 非 NULL = 已经过 `fill_replay::fill_scope_origin_proof` 逐行复核，
--   结论写在同一行的 `scope_known` 上，逐行证据写在下面这张审计表里。
--
-- `scope_known` 默认 1 是为了让"新写入的行天生已知"这件事不依赖写入侧记得
-- 显式赋值；真正决定一行能不能建域的是 `scope_known=1 AND
-- scope_proved_at_ms IS NOT NULL` 两者**同时**成立。
--
-- 复核由 `fill_replay::revalidate_legacy_fill_scope_origins` 在 `Store::connect`
-- 里执行（迁移之后、0024 的 fill 复核之后、任何恢复域发现之前），幂等：
-- 只处理 NULL 行。
ALTER TABLE fill_recovery_scopes
  ADD COLUMN scope_known BIGINT NOT NULL DEFAULT 1;

ALTER TABLE fill_recovery_scopes
  ADD COLUMN scope_proved_at_ms BIGINT;

ALTER TABLE fill_replay_cursors
  ADD COLUMN scope_known BIGINT NOT NULL DEFAULT 1;

ALTER TABLE fill_replay_cursors
  ADD COLUMN scope_proved_at_ms BIGINT;

CREATE TABLE fill_scope_origin_revalidations (
  origin_table VARCHAR(32) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  resolved_scope_known BIGINT NOT NULL,
  verdict VARCHAR(32) NOT NULL,
  evidence TEXT NOT NULL,
  revalidated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (origin_table, credential_id, symbol)
);
