-- --
-- 行情中断触发源此前对一个 credential 下的**任意** bar_closed 取 MAX(ts_ms)，
-- 于是 BTC/1m 健康就能永久掩盖 ETH/1m 中断；阈值又是固定 180s，
-- 纯 1h 周期的流会在 180 秒后误触发。
--
-- 判据必须按**真实的 market-stream identity**（credential + symbol + timeframe）
-- 逐条判定。事件表里 timeframe 只活在 payload JSON 里，SQL 够不到；
-- 因此单独立一张心跳表，由 kline 收盘路径 upsert。
--
-- 只登记**真的收到过 bar** 的流：从未出现过的流不在表里，也就不会被判"中断"
-- （"从未有过 bar ≠ 中断"这条误触发边界因此天然成立）。
CREATE TABLE IF NOT EXISTS market_stream_health (
  credential_id VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  timeframe VARCHAR(16) NOT NULL,
  last_bar_ms BIGINT NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, symbol_key, timeframe)
);

CREATE INDEX market_stream_health_last_bar_idx
  ON market_stream_health (credential_id, last_bar_ms);
