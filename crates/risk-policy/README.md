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

Round 2A's `risk_policy_testnet_v1` is an explicit exercise policy selected by
an intent's `policy_version`; it never activates or replaces the default
snapshot. The engine and replay evaluator admit its opening intents only for
`run_mode=demo` on an explicitly resolved testnet venue. A matching live
certification cannot authorize this exercise policy on another stage or venue.
The exercise policy changes only minimum SL distance (0.15%), minimum RR
(0.05), and cost-aware TP multiple (1). Fees/slippage assumptions, 0.5% risk
budget, 15% pair exposure, and 1000 USDT position cap stay the same. S2/S3
protection distances exercise execution and do not establish trading value.
Out-of-range SL distances reject instead of being silently clamped.

For this policy only, the engine retains the caller's exact typed SL string in
its durable risk replay input and protection plan after checking its numeric
identity and verifying that no stop clamp occurred. This avoids reserializing
an already explicit price through f64 and shifting it by one exchange tick.
It is a narrow 2A compatibility measure, not a full fixed-point sizing pipeline;
multi-symbol/tick-size and full fixed-point conversion remain outside 2A.
