# Local patches to sqlx-core 0.8.6 (K07)

This directory is a controlled snapshot of the crates.io `sqlx-core` 0.8.6
package with one local patch (K07: pool connection return / close lifecycle).
It is wired in by the single `[patch.crates-io]` entry in
the root `Cargo.toml`. Every other SQLx crate (`sqlx`, `sqlx-mysql`,
`sqlx-sqlite`, `sqlx-macros*`) still comes from crates.io at 0.8.6.

## Source

| Item | Value |
| --- | --- |
| Package | `sqlx-core` 0.8.6, crates.io |
| Archive `sqlx-core-0.8.6.crate` SHA-256 | `ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6` (recomputed from the archive itself; equals the `Cargo.lock` checksum) |
| Upstream VCS (from `.cargo_vcs_info.json`, informational only) | launchbadge/sqlx `bab1b022bd56a64f9a08b46b36b97c5cff19d77e`, path `sqlx-core` |
| Snapshot | all 100 files of the archive, unpacked unchanged; Cargo's local `.cargo-ok` marker is not part of the archive and is not included |
| License | `MIT OR Apache-2.0`; `LICENSE-MIT` and `LICENSE-APACHE` are the unchanged upstream texts. The archive contains no NOTICE file. |

`K07-UPSTREAM.sha256` lists the SHA-256 of every upstream file as unpacked.
`sha256sum -c K07-UPSTREAM.sha256` from this directory must report exactly the
three files below as changed and nothing else. `Cargo.toml` (normalized) and
`Cargo.toml.orig` are unchanged.

## Changed and added files

| File | Change |
| --- | --- |
| `src/pool/connection.rs` | behavior (below) |
| `src/pool/inner.rs` | behavior (below); registers `k07_tests` under `#[cfg(test)]` |
| `src/pool/mod.rs` | documentation of `Pool::close` only |
| `src/pool/k07_tests.rs` | new, `#[cfg(test)]` only; never compiled into the application |
| `PATCHES.md`, `K07-UPSTREAM.sha256` | new, provenance |

Changed code is marked with `K07` comments. The `#[cfg(test)]` statements in
`inner.rs` fire test hooks; they do not exist in non-test builds, and there is
no feature flag or runtime switch.

## Behavior

1. **Owned return deadline.** The future that returns a dropped connection
   (`PoolConnection::return_to_pool`) gets one absolute deadline,
   `RETURN_TO_POOL_TIMEOUT` = 5 s, fixed when the future is created. It covers
   `after_release`, the release ping, any graceful close and the
   `min_connections` maintenance tail started by that return. On expiry (or if
   the deadline has already passed at the first poll) the future is dropped
   together with the connection and size guard it owns; nothing is detached or
   moved to another task. `take_and_close` (close-on-drop) shares one 5 s
   budget between its graceful close and its maintenance tail.
2. **Unknown state is dropped, not closed.** A failed release ping or an
   `after_release` error drops the connection instead of awaiting
   `close_hard()` (the Any backend's `close_hard` delegates to a graceful close
   that can itself await the transport).
3. **Lifecycle gate.** A private `Mutex<()>` linearizes `mark_closed`, the
   closed check + size increment in `try_increment_size`, and the closed check
   + idle count/queue publication + permit hand-off in the central
   `try_release`. It is never held across an `.await`, a raw connection drop or
   a user callback. A closed pool refuses the connection and hands it back;
   the normal return path then closes it gracefully within its deadline, and
   the other central callers (`connect_with`, `try_min_connections`, the idle
   reaper) drop it after the gate is released.
4. **Close waits for real resources.** `close()` registers a size-event
   listener, drains idle connections, and completes only when `size` is zero;
   otherwise it waits for the next size decrement. Every size decrement
   notifies all waiting closers, so concurrent closers all complete. Each idle
   connection taken by `close()` gets a graceful close bounded by
   `CLOSE_IDLE_TIMEOUT` = 5 s and is then dropped. Its size guard owns no
   permit (the permit was already returned when it became idle), so closing an
   idle connection never adds a semaphore permit, including in child pools.
5. **Raw before size.** `OwnedClose` owns the driver close future and the size
   guard with the future declared first, so the raw connection is dropped
   before the size decrement is published on completion, timeout and
   cancellation. `num_idle` is incremented before an idle connection can be
   popped and decremented on every pop, including pops by `close()`.

The whole-pool close is not a 5 s SLO: with `max_connections` = 8, a single
closer that finds 8 unresponsive idle connections can take up to 8 x 5 s plus
scheduling, and a close always keeps waiting for connections that the
application holds, leaks or detaches, or whose tasks never get to run.

## Not changed (known boundaries)

- `sqlx-mysql`, the Any driver's `close_hard`, TLS, SQLite workers.
- The acquire path (`check_idle_conn` ping / `before_acquire`, `connect` and
  `after_connect`): still bounded only by `acquire_timeout`.
- The idle reaper's own graceful close of expired idle connections (it is
  cancelled when the pool closes).
- `PoolConnection::close`, `detach`, `leak`, `close_on_drop` semantics other
  than the shared budget above.
- Cancelling a connection's return does not tell whether any earlier business
  statement on it committed or rolled back.

## Tests

- `src/pool/k07_tests.rs`: deterministic interleaving tests with a scripted Any
  backend, run by the independent root
  `tests/gate/k07-pool-lifecycle/internal-harness` (see its manifest for why
  it compiles this `src/lib.rs` directly).
- `tests/k07_pool_lifecycle.rs` and the `k07_store_` test in
  `src/store.rs`: real MySQL through a loopback proxy that
  withholds release COM_PING replies.

## Removal condition

Remove this directory and the `[patch.crates-io]` entry when SQLx is upgraded
to a release that (a) bounds the owned return of a dropped connection and
drops unknown-state connections, and (b) completes `Pool::close` only after
every counted connection is gone, without permit inflation, for concurrent
closers. Before removal, the K07 real-MySQL tests must pass against the
upgraded registry crate. Any SQLx version change makes this 0.8.6 patch
unusable (Cargo reports the patch as unused); it must then be removed or
re-ported and re-reviewed, never silently kept.
