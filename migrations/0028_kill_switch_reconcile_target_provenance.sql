-- frozen expected target 的来源。
--
-- R18 把「当前 generation 的**全部** `kill_switch_reconcile_status` 行」当成
-- 本 revision 在 arm 那一刻冻结下来的 expected target 集合。这张表有**两个**
-- 写者：`reset_reconcile_targets_in_tx`（arm 时冻结成员）与
-- `record_reconcile_for_scope`（投影一份真值）。第二个写者写下的行在下一次读
-- 的时候与第一个写者写下的行**逐字无法区分**——于是"谁是本 revision 的
-- expected 成员"这个判据的输入，包含了它自己的输出：一条同代的 outsider
-- clean/flat 行（R17 存量行，或任何一次绕过成员资格写进来的行）会把自己
-- 算进 expected，随后凭这一行让整个 scope 收敛（自授权）。
--
-- 这一列就是那份缺失的 provenance：
--
-- * `'arm_snapshot'` —— `reset_reconcile_targets_in_tx` 在 arm 事务里写的
--   冻结成员行。**只有它**构成 frozen expected target。
-- * `'reconcile_discovery'` —— 投影路径为一个"此刻的成员"（
--   `reconcile_targets_in_tx` 认得，例如对账刚发现的孤儿仓位）新建的行。
--   它靠**此刻的成员资格**参与收敛，不靠自己这一行。
-- * `'legacy_isolated'` —— 启动期复核判定"证不出归属"的 0028 之前的存量行。
-- * `NULL` —— 0028 之前写下的、**尚未复核**的存量行。
--
-- 这里**零 DML**：不改任何存量行。存量行取 `NULL`，语义是"来源不可证明"，
-- 而不可证明的行**不得**充当 expected 成员（engine safety contract：缺失不得被
-- 当成正常）。把它们收敛回可用状态的是启动期的
-- `kill_switch::revalidate_legacy_reconcile_targets`——它在 `Store::connect`
-- 里、迁移之后、任何对账/重试任务启动之前跑，逐行给出结论并留证据：
--
-- * `adopted` —— 这个 credential 此刻仍是 scope 的成员（
--   `reconcile_targets_in_tx` 认得它）⇒ 收进 `'arm_snapshot'`，
--   同时把 clean/complete 复位成 0：来源不可证明的 clean 票不得留任
--   （这一轮必须重新报一次真值才能收敛）。
-- * `isolated` —— 此刻不是成员、也证不出它是 arm 冻结进来的 ⇒
--   `'legacy_isolated'`，行留着可审计，但既不能写、也不参与聚合。
-- * `anchored` —— 复核重建快照时补上"此刻是成员、本代却一行都没有"的
--   那些 credential，让 expected 集合重新完整。
--
-- 幂等：复核只处理**当前代际下存在 `NULL` provenance 行**的 armed scope，
-- 跑过一次之后是空集，健康库上零写入。
ALTER TABLE kill_switch_reconcile_status
  ADD COLUMN expected_provenance VARCHAR(32);

CREATE TABLE kill_switch_reconcile_target_revalidations (
  scope_key VARCHAR(128) NOT NULL,
  credential_id VARCHAR(128) NOT NULL,
  generation BIGINT NOT NULL,
  verdict VARCHAR(32) NOT NULL,
  evidence TEXT NOT NULL,
  revalidated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (scope_key, credential_id, generation)
);
