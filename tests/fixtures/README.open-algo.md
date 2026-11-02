# Open Algo cleanup regression fixture

`open_algo_cleanup_v1.json` is the common identity vector for the Rust list/cancel
entry tests and the separately versioned kernel probe v4 tests. Keep both sides
on identical bytes; do not recreate the returned DTO independently.

The Rust test `svc::open_algo_list_tests` seeds an isolated SQLite store through
the real migrations. Its wire test starts the real authenticated vnext gRPC
services on an owned loopback socket and executes `open_algo_client_probe.py`
through the production `RustCoreGrpcClient`. The consumer reads Rust-produced
protobuf responses, including unchanged status strings, and calls the real
`CancelOrder` RPC for the three target Algo orders. Only the Binance HTTP
responses/capture are replaced. Keys are local dummy values; no exchange or VM
is contacted. The cancel path still performs credential, venue and order
identity resolution and persists both order records.

Python selection is `RUSTCORE_TEST_PYTHON`, an existing repository-local
`.venv-gate/bin/python`, then `python3` on `PATH`. Python needs the repository's
orchestrator dependencies. Missing dependencies, process errors and unavailable
loopback sockets fail the test rather than skipping it. Fixture paths are
resolved from `CARGO_MANIFEST_DIR`.

Run from the repository root:

```sh
CARGO_BUILD_JOBS=2 cargo test --offline --manifest-path services/rustcore/Cargo.toml --bin rustcore svc::open_algo_list_tests -- --test-threads=1
```

Set `RUSTCORE_OPEN_ALGO_WIRE_OUTPUT` to save the actual response protobuf bytes,
fixture hash, captured exact Algo DELETEs and persisted cancel count. The HTTP
timestamps/signatures change between runs, so the observation hash is not the
frozen vector hash.

The list retains credential/symbol/cycle filtering and its 100-row limit.
`confirmed` and `triggering` are visible; `triggered` is excluded because a
triggered parent's ordinary child requires separate lifecycle handling.
The probe owns position lineage selection and the final exchange-truth check;
this Rust wire consumer selects only vector-named targets from the actual
response. These local entry regressions do not establish remote convergence,
deployment certification, automatic ordinary-exit cleanup or child cancellation.
