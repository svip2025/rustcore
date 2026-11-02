-- Preserve 0001 and its checksum. MySQL enforces VARCHAR length while SQLite
-- does not; the latter deliberately treats this executable comment as a no-op.
-- Store preflight/reentry accepts only the canonical 16/32 NOT NULL shapes.
/*! ALTER TABLE engine_orders MODIFY COLUMN order_type VARCHAR(32) NOT NULL */;
