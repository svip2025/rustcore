# risk-policy

Deterministic, I/O-free order preflight policy for rustcore. The public entry
point is `evaluate(&Input) -> RiskVerdict`; time and every external fact are
explicit inputs. The crate intentionally has no async runtime, database, file,
network, or system-clock dependency.

`policy/risk_policy_v3.json` is the factory P1 policy snapshot. Its identity is
`sha256:` plus the SHA-256 of the canonical JSON produced by
`ai_trade_contracts::canonical_json`. The adjacent `.sha256` file is checked by
the crate tests. Inputs delivered by later roadmap tasks remain optional. A
disabled requirement plus absent input is recorded as `skipped/input_absent`;
an enabled requirement plus absent input rejects the opening intent. The
factory policy requires DataSnapshot decisions on snapshot-capable wires.

The frozen v1 engine proto cannot carry several typed-intent facts. Callers must
not invent them: an absent fact stays absent and is visible in `rules_json`.
The factory policy now has `require_typed_protection=true`. The input carries an
explicit `typed_protection_capable` discriminator: v2 enforces missing typed
SL/TP as `MISSING_PROTECTION`, while frozen v1 records
`protection_required=skipped/wire_not_capable` and leaves the remaining price
rules `skipped/input_absent`. In the v1 compatibility path the evaluator caps
and reserves the client quantity and records
`sizing.mode=legacy_client_quantity`; it does not run risk-budget sizing without
a real stop.

The same explicit capability pattern applies to DataSnapshot decisions. v2
sets `data_snapshot_capable=true`; a missing or malformed `snapshot_id` rejects
opening. Frozen v1 sets it false and records
`data_quality_gate=skipped/wire_not_capable` until that production path is
retired. Snapshot consistency errors fail closed for opening while the
open-only rule never rejects closing.
