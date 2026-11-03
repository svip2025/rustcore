-- One durable release per admitted Entry allocation. The marker and the
-- subtraction from notional_reservations commit in the same transaction.
CREATE TABLE notional_reservation_releases (
  entry_engine_order_id VARCHAR(64) PRIMARY KEY,
  risk_verdict_id VARCHAR(80) NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  position_id VARCHAR(64) NOT NULL,
  notional_usdt VARCHAR(64) NOT NULL,
  truth_read_started_at_ms BIGINT NOT NULL,
  released_at_ms BIGINT NOT NULL,
  CONSTRAINT notional_reservation_release_verdict_uniq UNIQUE (risk_verdict_id)
);
