# Testing

## Run the suite

```bash
docker compose up -d
cargo test
```

The default Cargo command includes every workspace member. Compose provides a
disposable MySQL fixture on `127.0.0.1:13306`; tests create and migrate their
own scratch databases. Its passwordless root account is for this loopback-only
fixture. Use a disposable database, because tests create and drop databases.

The checked-in Cargo environment sets `RUSTCORE_TEST_MYSQL_URL` and
`RUSTCORE_REQUIRE_MYSQL_TESTS=1`. Explicit caller environment variables take
precedence. The requirement flag makes a missing or unreachable required MySQL
fixture fail. Without this flag, some optional MySQL tests can return early and
still print `ok`; that output alone is not evidence of MySQL coverage.

## Counts and ignored cases

The release suite reports **1,388 passing top-level tests, zero failures, and
30 ignored entries**. Four child-helper invocations also print successful test
summaries. Counting those nested results produces the earlier total of 1,392;
they are not four additional top-level tests. Cargo prints separate summaries
for each binary. To aggregate top-level results, exclude targeted child-helper
summaries with nonzero `filtered out` counts.

Ignored entries include subprocess helpers invoked by parent tests, additional
opt-in database scenarios, and 15 integration scenarios requiring an external
Python application checkout. Check the individual `#[ignore]` reason and its
fixture prerequisites before running it. Running every ignored helper directly
is not a supported substitute for its parent test.

The normal suite exercises real MySQL transactions, compare-and-swap updates,
connection return and closure, cancellation, and recovery, as well as SQLite
and scripted exchange scenarios. Release validation retained 65 named
MySQL/connection-pool checks with their passing output.

## Build settings and timing

Development and test profiles omit debug symbols, retaining Cargo's normal
assertions and overflow checks. Eight test threads are used by default.

```bash
# Reduce concurrency on a smaller machine.
RUST_TEST_THREADS=4 cargo test

# Include symbols when debugging a test.
CARGO_PROFILE_TEST_DEBUG=2 cargo test <test-name>
```

On an 8-vCPU, 16-GiB Linux host, release validation measured:

| Setup | Complete `cargo test` command |
| --- | ---: |
| Four test threads; compiler dependency cache reused | 790.289 seconds |
| Eight test threads; empty build directory; two compilation jobs | 625.616 seconds |
| Eight test threads; empty build directory; default compilation jobs | 554.339 seconds |

The final run took 555.596 seconds from bundle clone through Compose readiness
to test completion. Downloaded dependency sources were cached, the build-output
directory was empty, and the task's MySQL fixture was already healthy. A
separate first MySQL initialization took 29.173 seconds; adding that measurement
is a reference budget, not a measured fully cold end-to-end run. Tool
installation and uncached downloads are outside these timings. Hardware,
storage, and network speed affect a new checkout's elapsed time.

## Formatting, lints, and licenses

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo deny check licenses
```

The dependency policy is in [`deny.toml`](../deny.toml). The vendored SQLx
patch retains its upstream licenses and is explained in
[`PATCHES.md`](../vendor/sqlx-core-0.8.6/PATCHES.md).

Stop the disposable MySQL fixture after testing:

```bash
docker compose down
```
