-- L5-S3 historical risk replay support.
--
-- Existing rows remain NULL because their exact evaluator inputs cannot be
-- reconstructed safely. Every new rustcore verdict stores the complete replay
-- envelope, including policy-integrity and post-evaluation rejection guards.

ALTER TABLE risk_verdicts
  ADD COLUMN replay_input_json TEXT;
