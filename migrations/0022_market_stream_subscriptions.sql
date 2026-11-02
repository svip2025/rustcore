-- --
-- R4 立了 `market_stream_health`（**观测面**：真的收到过 bar 的流），并据此
-- 逐流判断陈旧。三轮段级复评逐轮指出同一个缺口：**判据的输入集合里
-- 根本没有"应该存在但从未出现过的流"**——有敞口、配置要求 BTC/1m，
-- 但该订阅从第一根 bar 起就没起来，心跳表里没有这一行，
-- `streams=[]` 于是遍历空集返回 None。安全触发器对"从一开始就死"的流失明。
--
-- 缺的是**订阅真值集合**（required），它此前只活在一次 `EnsureMarketData`
-- RPC 的入参里，落地即丢。这张表把它记下来，判据因此可以算
-- `required − observed` 这个差集。
--
-- 语义边界（写清楚，避免下一轮被当成心跳表的重复）：
--   * `market_stream_health`  = **观测**：这条流最近一次真的收到 bar 是什么时候；
--   * `market_stream_subscriptions` = **要求**：我们被要求让这条流活着，从何时起。
-- 两张表都不是自报——前者由 kline 收盘路径写，后者由订阅入口写。
--
-- `requested_at_ms` 是**首次**被要求的时刻，重复 ensure 不刷新它：
-- 刷新会让一条从未起来的流每次重试都把计时清零，永远够不到阈值。
CREATE TABLE IF NOT EXISTS market_stream_subscriptions (
  credential_id VARCHAR(64) NOT NULL,
  symbol_key VARCHAR(128) NOT NULL,
  timeframe VARCHAR(16) NOT NULL,
  requested_at_ms BIGINT NOT NULL,
  PRIMARY KEY (credential_id, symbol_key, timeframe)
);

CREATE INDEX market_stream_subscriptions_requested_idx
  ON market_stream_subscriptions (credential_id, requested_at_ms);
