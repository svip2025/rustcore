-- P1/T1.5 portable portfolio risk-budget storage.
--
-- SQLite and MySQL run this same migration set. Keep decimal amounts as
-- strings and avoid dialect-specific partial/conditional index syntax.
-- T1.6 owns kill-switch behavior; this migration only reserves and stores the
-- three kill-switch columns required by the published data model.

CREATE TABLE IF NOT EXISTS risk_budget (
  credential_id VARCHAR(64) PRIMARY KEY,
  as_of_ms BIGINT NOT NULL,
  equity_usdt VARCHAR(64) NOT NULL,
  available_usdt VARCHAR(64) NOT NULL,
  today_realized_pnl_usdt VARCHAR(64) NOT NULL,
  today_realized_pnl_pct DOUBLE NOT NULL,
  drawdown_pct DOUBLE NOT NULL,
  total_exposure_pct DOUBLE NOT NULL,
  open_positions_count BIGINT NOT NULL,
  group_exposure_json TEXT,
  kill_switch_level VARCHAR(32) NOT NULL DEFAULT 'none',
  kill_switch_scope VARCHAR(32),
  kill_switch_reason TEXT,
  remaining_credential_notional_usdt VARCHAR(64)
);
