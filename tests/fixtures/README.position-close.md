# B3-K1 exact position close vectors

`position_close_v1.json` is emitted by
`position_close_tests::b3_sqlite_one_sided_contract_vector`. The producer uses the
real authenticated loopback gRPC server, generated protobuf codec, production
FundSimulationAccount / PutRiskBudget / ExecuteIntents / GetPositionCloseTarget /
ClosePosition / GetPositionCloseReceipt methods and the migrated SQLite store.
It does not manually seed entry provenance or produce an expected response on
the consumer side. The only market input is a closed-bar fixture written through
the existing event sink; that same event supplies the actual paper fill price.

All uint64 protobuf values are decimal strings in these JSON examples. In
particular `execution_outcome.revision` is the post-close revision, whereas
`target_revision` is the pre-close revision. The frozen paper transaction
outcome and the current position observation are distinct. Protection cleanup
remains `unknown / PROTECTION_CLEANUP_NOT_PROVEN`.

Every nested target, position observation and protection observation carries
`schema_version: 1`, read from the actual decoded protobuf message. The producer
asserts these versions before serializing; it does not inject a default version.

The SQLite producer additionally captures `cases.reopenedCurrentReceipt` after a
clearly identified controlled same-ID row reopen. This proves that the real
receipt RPC separates the frozen zero outcome from current non-zero quantity;
it is not a new-entry or reconcile-entrypoint wiring proof. `preparedReceipt`
pins the production T1 helper after a real entry, and `pendingUnknownReceipt`
comes from a real child process opening the same database with a new process
boot and authenticated service. `caseSources` records those boundaries. The separate fault test below
provides actual OS-process SIGKILL/restart evidence.

Regenerate the source fixture from the repository root (the output path is a
caller-selected local artifact, never a production service destination):

```sh
B3_CLOSE_VECTOR_PATH="$PWD/services/rustcore/tests/fixtures/position_close_v1.json" \
  cargo test --manifest-path services/rustcore/Cargo.toml --bin rustcore \
  position_close_tests::b3_sqlite_one_sided_contract_vector -- --exact --nocapture
```

The independent MySQL producer is explicitly ignored by default. On an owned
scratch-capable MySQL instance, set `RUSTCORE_TEST_MYSQL_URL` privately, set
`RUSTCORE_REQUIRE_MYSQL_TESTS=1`, choose a separate `B3_CLOSE_VECTOR_PATH`, and run:

```sh
cargo test --manifest-path services/rustcore/Cargo.toml --bin rustcore \
  position_close_tests::b3_mysql_one_sided_contract_vector -- --exact --ignored --nocapture
```

It creates a unique `rustcore_b3close_*` database, runs production migrations and
the same authenticated RPC scenario, closes its service and removes only its
own database. An absent MySQL URL is not evidence of a MySQL pass. Do not print
that URL or copy it into committed artifacts.

`cargo test --features l4-fault-injection --manifest-path
services/rustcore/Cargo.toml --bin rustcore b3_ -- --nocapture` additionally runs a
parent test that starts three isolated child processes and kills them after T1
prepare, before the shared OMS transaction commits, and after that commit.
The child-only test is ignored in direct runs and is invoked by the parent.

Scope: canonical `paper` / `paper:<32 lowercase hex>` credentials with durable
production-entry paper provenance only. Exchange credentials and old positions
without provenance are unavailable. The kernel is an internal authenticated
RPC boundary; member ownership and the `close_holding` password challenge must
be proved independently in the upper HTTP/Python path. These offline fixtures
are neither deployment certification nor venue execution evidence.
