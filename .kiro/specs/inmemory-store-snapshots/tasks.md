# Implementation Plan

- [x] 1. Derive groundwork (mechanical, no logic)
  - DONE 2026-08-20: all four sub-tasks landed; `Cargo.lock` verified unchanged.
  - [x] 1.1 Add `Serialize`/`Deserialize` to the kernel transition-op enums
    - `ActivityOp`, `TimerOp`, `DispatchOp` in
      `crates/tokeira-kernel/src/transition.rs` (aligning them with `ProjectionOp`,
      which already derives). No other kernel change.
    - _Requirements: 5.4_
  - [x] 1.2 Add `PartialOrd`/`Ord` to ID newtypes and composite keys
    - `NamespaceId`, `RunId`, `RunKey`, `ShardId`, `TaskQueueName` in
      `tokeira-types`; `DeploymentKey`, `StoredTaskQueueConfigKey` in
      `crates/tokeira-storage/src/api.rs`.
    - _Requirements: 1.3, 5.4_
  - [x] 1.3 Add `Serialize`/`Deserialize` to the seven `api.rs` types and
    `ActivityDispatchEntry`
    - `WorkerTaskProvenance`, `RequestRecord`, `TransitionAuditRecord`,
      `ProjectionRecord`, `BacklogEntry`, `DispatchableActivityTask`,
      `DispatchableWorkflowTask` (`api.rs`); `ActivityDispatchEntry` (`memory.rs`).
      Bare `OffsetDateTime`/`Duration` fields, no serde attributes (house pattern).
    - _Requirements: 1.1_
  - [x] 1.4 Make `postcard` unconditional for `tokeira-storage`
    - Drop `optional = true` and remove `"dep:postcard"` from the `dsql` feature in
      `crates/tokeira-storage/Cargo.toml`. Verify `git diff --stat Cargo.lock` is
      empty.
    - _Requirements: 5.1, 5.2_

- [x] 2. Snapshot mechanism (`crates/tokeira-storage/src/memory.rs`)
  - DONE 2026-08-20: `SnapshotDoc` + `snapshot()`/`from_snapshot()` as designed; one
    addition over the design's error table — `SnapshotError::TrailingBytes` as a
    distinct variant (constructing a synthetic `postcard::Error` for trailing bytes
    would misattribute the failure).
  - [x] 2.1 Add `SnapshotDoc` and the two conversion functions
    - Mirror struct with sorted `Vec<(K, V)>` map fields, `Vec<BacklogEntry>` in
      queue order, no test-only fields. `StoreState → SnapshotDoc` starts with a full
      no-rest destructuring of `StoreState` so unclassified new fields fail to
      compile; `SnapshotDoc → StoreState` rebuilds maps and defaults the test-only
      fields.
    - _Requirements: 1.1, 1.4, 3.3_
  - [x] 2.2 Add `SNAPSHOT_FORMAT_VERSION`, `SnapshotError`, and the two API methods
    - `snapshot()` (single lock acquisition, version-prefixed postcard encoding, no
      state mutation) and `from_snapshot()` (version decoded and checked before
      payload decode; trailing bytes rejected; fresh `Arc<Mutex<_>>`). `thiserror`
      variants per the design's error table.
    - _Requirements: 1.1, 1.2, 1.5, 2.1, 2.2, 2.3, 3.1_
  - [x] 2.3 Document the surface
    - Module/API docs: boot-only rationale (lease/fencing), format instability
      (dev/embedded tier, refuse-not-migrate), past-due timers firing on restore as
      correct durable semantics, test-only state reset.
    - _Requirements: 2.4, 4.2 (documentation half), 3.1_

- [x] 3. Checkpoint: storage compiles and is clippy-clean
  - DONE 2026-08-20: `cargo check`/`clippy --all-targets` clean on storage, kernel,
    and types.
  - `cargo check -p tokeira-storage` and
    `cargo clippy -p tokeira-storage --all-targets`; also
    `cargo check -p tokeira-kernel -p tokeira-types`. Zero warnings.

- [x] 4. Property test: Property 1 — snapshot round-trip identity
  - In `memory.rs`'s test module; proptest ≥100 iterations over generated public-API
    operation sequences (reuse `preservation_property_tests.rs` fixture style).
    Assert `snapshot(from_snapshot(snap)) == snap` and repeat-snapshot byte identity.
  - Tag: `// Feature: inmemory-store-snapshots, Property 1: snapshot round-trip identity`
  - _Requirements: 1.1, 1.3, 1.5, 3.2_

- [x] 5. Property test: Property 2 — malformed and mismatched input refused
  - Proptest ≥100 iterations over re-stamped versions, truncations, appended
    trailing bytes, and arbitrary byte strings; assert the mapped `SnapshotError`
    variant and absence of panics.
  - Tag: `// Feature: inmemory-store-snapshots, Property 2: malformed input refused`
  - _Requirements: 2.1, 2.2, 2.3_

- [x] 6. Property test: Property 3 — test-only state never survives restore
  - Proptest ≥100 iterations: stores with injected conflicts/policy produce
    injection-free restored stores and injection-invariant snapshot bytes.
  - Tag: `// Feature: inmemory-store-snapshots, Property 3: test-only state reset`
  - _Requirements: 1.4, 3.3_

  - DONE 2026-08-20 (tasks 4–6): all three storage properties in `memory.rs`'s test
    module, 100 cases each, seeded via a generated public-API op sequence
    (`SnapshotSeedOp`); full storage suite 90/90 green under nextest.

- [x] 7. Unit tests (example-based, `memory.rs` test module)
  - Empty-store round trip; fixed version-mismatch and truncation fixtures
    (exact error variants and message contents); consistent-cut test with a
    concurrent writer task (decoded snapshot internally consistent, e.g.
    `history`/`history_principals` lengths agree) synchronized without sleeps.
  - _Requirements: 1.2, 2.2, 2.3_

- [x] 8. Property test: Property 4 — restore-then-recovery equals restart
  - New `crates/tokeira-runtime/tests/runtime_snapshot.rs`. Proptest ≥32 iterations
    over small generated workloads (starts, signals, timers incl. past-due,
    activities) driven through a real runtime; compare a fresh runtime over the
    restored store against a fresh runtime over the original store: pollable tasks,
    due-timer firing, repository reads. Plus deterministic example cases for
    past-due-timer-fires-immediately and the `runtime_lane.rs:407` restart pattern
    with a snapshot-restored store.
  - Tag: `// Feature: inmemory-store-snapshots, Property 4: restore-then-recovery equivalence`
  - _Requirements: 3.1, 3.4, 4.1, 4.2, 4.3_

  - DONE 2026-08-20 (task 8): `crates/tokeira-runtime/tests/runtime_snapshot.rs` —
    Property 4 at 32 cases over generated workloads (immediate/signaled/past-due
    delay/future delay) plus the two pinned deterministic cases. Equivalence is
    asserted through observable recovery (repository reads, the exact recovered
    task set, past-due firing, activity republish) rather than `SweepResult`
    tallies — the runtime's `acquire_shard` recovery entry point does not expose
    the tally, and observable equivalence is the requirement's substance.

- [ ] 9. Checkpoint: full finish bar green (§10.4)
  - fmt, `cargo lint --locked`, `cargo check --workspace --locked`,
    `cargo nextest run --workspace --locked`, doctests, rustdoc with
    `-D warnings` — run on the designated builder, one command per step.
  - Confirm `Cargo.lock` untouched (`git diff --stat` clean of it).

## Task Dependency Graph

- 1.1, 1.2, 1.3, 1.4 → 2.1 (doc needs the derives and unconditional postcard)
- 2.1 → 2.2 → 2.3 → 3
- 3 → 4, 5, 6, 7 (storage-local tests; parallel)
- 3 → 8 (runtime integration test; needs the API, independent of 4–7)
- 4, 5, 6, 7, 8 → 9

## Notes

- Ordering rationale: derives are pure groundwork with zero behaviour impact, so they
  land first and unblock the doc; the mechanism precedes every test; the runtime
  equivalence test only needs the public API and can proceed in parallel with the
  storage-local property tests.
- Property 4 runs at a reduced iteration count (≥32) because each case constructs two
  full runtimes; the deterministic example cases pin the two scenarios named by the
  requirements regardless of generator coverage.
- Equality caveat from the design: `DispatchableActivityTask`/`DispatchableWorkflowTask`
  `PartialEq` ignores `order` — round-trip assertions go through snapshot-byte
  identity, not `PartialEq`, wherever `order` matters.
- No dependency movement anywhere; if any step appears to require one, stop and raise
  it instead (§10.3).
