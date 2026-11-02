-- fill 域身份的可审计复核。
--
-- 0017 的 backfill 把**所有非空** legacy `symbol` 标成 `symbol_scope_known=1`
-- （`0017_fill_replay_data_layer.sql:41-53`），而 `discover_fill_recovery_scopes`
-- 正是从 `symbol_scope_known=1` 里捞恢复域。于是一条 venue/market/settle
-- 从未被验证过的旧行，可以播种一个 Binance USDⓈ-M 恢复域，随后 fill
-- recovery 拿着它去逐页回补别人的成交。
--
-- 这里**零 DML**：不改任何存量行，只加一列"是否已被复核"。
--
-- * `symbol_scope_proved_at_ms IS NULL` = 0017 留下的、**未经复核**的标记
--   ——`discover_fill_recovery_scopes` 不再接受它（fail-closed：未证明即无域）；
-- * 非 NULL = 已经过生产谓词（`usdm_identity` + 双源一致）逐行复核，
--   结论写在同一行的 `symbol_scope_known` 上，逐行证据写在下面这张审计表里。
--
-- 复核由 `fill_replay::revalidate_legacy_fill_symbol_scopes` 在 `Store::connect`
-- 里执行（迁移之后、任何恢复域发现之前），幂等：只处理 NULL 行。
ALTER TABLE engine_fills
  ADD COLUMN symbol_scope_proved_at_ms BIGINT;

CREATE TABLE fill_symbol_scope_revalidations (
  credential_id VARCHAR(64) NOT NULL,
  symbol_identity VARCHAR(64) NOT NULL,
  exchange_trade_id VARCHAR(64) NOT NULL,
  previous_scope_known BIGINT NOT NULL,
  resolved_scope_known BIGINT NOT NULL,
  verdict VARCHAR(32) NOT NULL,
  evidence TEXT NOT NULL,
  revalidated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, symbol_identity, exchange_trade_id)
);
