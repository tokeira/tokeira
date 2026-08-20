# Requirements Document

## Introduction

This feature adds snapshot persist/restore to the in-memory storage backend
(`InMemoryStore`, `crates/tokeira-storage/src/memory.rs`): a way to capture the store's
full durable-equivalent state as one opaque byte string, and to construct a new store
from such a byte string at boot. It is the mechanism half of embedded-tier persistence —
the consumer that decides *when* to snapshot (interval policy, snapshot-on-shutdown)
is the embedded-engine slice (T2) and is explicitly out of scope here.

This is a Tokeira-internal mechanism with no Temporal wire surface, so the behaviour
authority is not the targeted Temporal release; it is the store's own repository
contract (`crates/tokeira-storage/src/api.rs`) and the runtime recovery path
(`crates/tokeira-runtime/src/recovery.rs`). The governing correctness claim: a store
restored from a snapshot and taken through normal runtime recovery must be
indistinguishable from a process restart against durable storage holding the same
state.

This is a contained change to `tokeira-storage` (new API surface plus serde derives on
its private state), with one integration test in `tokeira-runtime` proving the recovery
claim. It is a state-format change (new persisted format), hence spec-required per the
root change classification.

## Glossary

- **Store:** an `InMemoryStore` — one `Arc<Mutex<StoreState>>` exercising the full
  repository contract in a single process (`memory.rs:62-65`).
- **Durable-equivalent state:** the subset of `StoreState` that models what the DSQL
  backend would persist — run state, history, indexes, dispatch sources, leases,
  registries. Enumerated exhaustively in the State Policy table below.
- **Test-only state:** `StoreState` fields that exist solely as test hooks:
  `conflict_injections` (injected OCC conflicts) and `conflict_policy` (injected
  current-execution conflict policy). Never part of a snapshot.
- **Snapshot:** a byte string encoding one consistent cut of the store's
  durable-equivalent state, produced while holding the store's mutex.
- **Consistent cut:** a serialization of the whole `StoreState` observed under a single
  acquisition of the store lock — no interleaved mutation, no torn subset.
- **Restore:** constructing a **new** store from snapshot bytes. Boot-only: there is no
  way to load a snapshot into a live store.
- **Format version:** a `u32` stamped into every snapshot, checked before any payload
  decoding on restore.
- **Recovery sweep:** `sweep_shard` (`recovery.rs:87`) — the one-time rebuild of
  volatile dispatch/timeout state from the durable store that runs on every shard
  takeover, including process restart.

## Target State

Supported after this feature:

- `InMemoryStore::snapshot()` returns the versioned snapshot bytes for the current
  state — one consistent cut, deterministic for a given state.
- `InMemoryStore::from_snapshot(bytes)` constructs a fresh store from a snapshot,
  refusing version mismatches and malformed input with typed errors.
- A restored store is a full peer of the original under the repository contract:
  every read observes the captured state; every write proceeds normally (fencing,
  OCC, dedupe intact); runtime recovery over it reconstructs the same derived state
  as it would over the original.
- Timers restore with their absolute fire times. A timer already past due at restore
  fires immediately once recovery injects it — this is correct durable-timer
  semantics (a real cluster restarted after downtime does the same), documented, not
  "fixed".

Explicitly out of scope:

- **Scheduling policy.** No interval snapshots, no snapshot-on-shutdown, no file I/O.
  This slice delivers bytes-out/construct-from-bytes-in only; policy belongs to the
  embedded-engine slice (T2).
- **Format stability.** The snapshot format is NOT a compatibility surface. It is the
  dev/embedded tier; the version stamp exists to *refuse* old snapshots, not to
  migrate them. No cross-version decode is ever attempted.
- **Hot restore.** No `&self` method ever replaces a live store's state — a live swap
  would violate the lease/fencing assumptions of every runtime component holding the
  store.
- **DSQL backend.** Untouched.

Sanctioned exceptions: none.

## Evidence From Current Code

- **State shape (authoritative):** `StoreState`,
  `crates/tokeira-storage/src/memory.rs:76-151` — 26 fields; the State Policy table
  below accounts for each.
- **Store handle:** `InMemoryStore { inner: Arc<Mutex<StoreState>> }`,
  `memory.rs:62-65`; existing public constructors are `Default` and
  `with_shard_count` (`memory.rs:239`).
- **Test hooks:** `inject_conflict` (`memory.rs:254`) and `set_conflict_policy`
  (`memory.rs:262`) — the only writers of test-only state.
- **Recovery path (authoritative for the equivalence claim):** `sweep_shard`,
  `crates/tokeira-runtime/src/recovery.rs:87` — idempotent, commits nothing,
  rebuilds brokers/timeout trackers from the repository; module docs
  (`recovery.rs:1-31`) state the restart contract this feature must preserve.
- **Restart test pattern:** `runtime_lane.rs:407`
  (`restart_preserves_delayed_start_callbacks_and_versioning_route`) — a second
  runtime constructed over the same store models restart; the equivalence test swaps
  in a snapshot-restored store.
- **Serialization precedent:** the DSQL backend persists kernel/storage types with
  postcard via `dsql/codec.rs:48-62`; `worker_compute.rs:26` already version-stamps
  postcard documents. `postcard` is already a dependency of this crate
  (`Cargo.toml:18`, currently optional behind the `dsql` feature).
- **No stored `Instant`:** `std::time::Instant` appears in `memory.rs` only as local
  metrics timing inside methods (e.g. `memory.rs:276,322`); `StoreState` holds none.
  Timers are absolute `OffsetDateTime` inside kernel state. This must remain true.
- **Determinism hazard:** `crates/tokeira-storage/AGENTS.md` — observable output
  drawn from `HashMap` iteration order is a live hazard in this crate; snapshot
  bytes are observable output.

## State Policy

Every `StoreState` field (`memory.rs:76-151`), exhaustively. Policy is one of:
**persisted** (captured in the snapshot, restored verbatim) or **reset** (never
serialized; restored store gets the default).

| Field | Policy | Rationale |
|---|---|---|
| `current_open` | persisted | Current-open-run index; DSQL persists the same distinction (`current_execution.is_open`). |
| `current_execution` | persisted | Current-run pointer retained after close; survives restart in DSQL. |
| `execution_index` | persisted | Durable run lookup by full execution identity. |
| `runs` | persisted | Materialized hot state (`WorkflowState`) — the store's core content. |
| `worker_deployments` | persisted | Worker Deployment registry records. |
| `workflow_rules` | persisted | Durable namespace Workflow Rules. |
| `task_queue_configs` | persisted | Public task-queue delivery policy. |
| `worker_task_provenance` | persisted | Token evidence records carry absolute expiry; expired records are filtered on read/sweep as normal. |
| `deployment_token_hwm` | persisted | Conflict-token high-water-mark is monotonic across the deployment name's lifetime (`memory.rs:104-114`); resetting it would let a restored store reissue an old token. |
| `history` | persisted | Authoritative event stream — the §3 authority. |
| `history_principals` | persisted | Attribution aligned index-for-index with `history`; dropping it would desync `read_attributed_history`. |
| `request_dedupe` | persisted | Request dedupe must survive restart or replayed client retries double-apply. |
| `transition_audit` | persisted | Test/admin audit log mirrors durable transitions. |
| `projection_log` | persisted | Projection records awaiting workers; a restart must not lose unprojected records. |
| `bundle_leases` | persisted | Lease rows survive restart in DSQL; the restored epoch is the fencing floor the recovering node re-acquires above. |
| `routing_generation` | persisted | Controller routing generation is monotonic; reset would un-fence stale routing decisions. |
| `budget_version` | persisted | CAS version for budget allocation; same monotonicity argument. |
| `activity_dispatch` | persisted | Durable dispatch source for activity work (`memory.rs:131-136`) — the sweep republishes from it. |
| `dispatch_backlog` | persisted | Ordered durable backlog; restart must not lose undelivered tasks. |
| `conflict_injections` | **reset** | Test-only OCC conflict hook. |
| `conflict_policy` | **reset** | Test-only injected conflict policy; restored store gets the default policy. |
| `activity_state_table` | persisted | Activity timeout/sweep materialization read by sweeps. |
| `timer_bucket` | persisted | Timer sweep materialization; fire times are absolute `OffsetDateTime`. |
| `run_shard_map` | persisted | Deterministic run→shard assignment must survive restart or runs change shards. |
| `shard_count` | persisted | Shard-count configuration participates in shard assignment; a restored store must shard identically. |

## Requirements

### Requirement 1: Snapshot capture

**User Story:** As an embedded-engine integrator, I want to capture the store's full
state as one byte string, so that I can persist it and later resume from it.

#### Acceptance Criteria

1. WHEN `snapshot` is called, THE store SHALL serialize all persisted fields of the
   State Policy table under a single acquisition of the store lock and return the
   encoded bytes.
2. WHEN `snapshot` is called concurrently with repository writes, THE returned bytes
   SHALL decode to a state that is exactly the store's state at one lock acquisition —
   never a partial or interleaved view.
3. WHEN `snapshot` is called twice on stores holding equal state, THE store SHALL
   return byte-identical output (serialization order is deterministic, independent of
   `HashMap` iteration order).
4. THE snapshot payload SHALL NOT contain any test-only state, and SHALL NOT contain
   any `std::time::Instant`-derived value.
5. WHEN `snapshot` is called, THE store SHALL NOT mutate any observable state.

### Requirement 2: Versioned format and refusal

**User Story:** As an embedded-engine integrator, I want snapshots version-stamped and
mismatches refused outright, so that a stale snapshot fails loudly at boot instead of
corrupting state.

#### Acceptance Criteria

1. THE snapshot encoding SHALL begin with a format-version stamp that is decodable
   without decoding the payload.
2. IF `from_snapshot` is given bytes whose format version differs from the crate's
   current version, THEN THE store SHALL return a version-mismatch error naming both
   the found and supported versions, and SHALL NOT construct a store.
3. IF `from_snapshot` is given bytes that fail to decode (truncated, corrupt, or
   trailing garbage after the payload), THEN THE store SHALL return a decode error and
   SHALL NOT construct a store or panic.
4. THE crate documentation for the snapshot API SHALL state that the format is
   unstable across Tokeira versions and exists for the dev/embedded tier only.

### Requirement 3: Boot-only restore

**User Story:** As an embedded-engine integrator, I want restore to be a constructor,
so that a snapshot can never be loaded into a store the runtime is already using.

#### Acceptance Criteria

1. THE restore API SHALL be a constructor producing a new `InMemoryStore`; no public
   API SHALL replace or mutate the state of an existing store from snapshot bytes.
2. WHEN `from_snapshot` succeeds, THE restored store SHALL return, for every persisted
   field, the same results through the repository read surface as the source store did
   at capture time.
3. WHEN `from_snapshot` succeeds, THE restored store SHALL have empty
   `conflict_injections` and the default `CurrentExecutionConflictPolicy`.
4. WHEN repository writes are performed against a restored store, THE store SHALL
   apply the same OCC, fencing, dedupe, and conflict-token rules as a store that
   reached the same state directly (in particular, `deployment_token_hwm` and lease
   epochs continue from their captured values).

### Requirement 4: Restart equivalence through recovery

**User Story:** As a runtime operator, I want a restored store plus normal recovery to
be indistinguishable from a process restart against durable storage, so that embedded
resume needs no special-case recovery path.

#### Acceptance Criteria

1. WHEN a fresh runtime performs its recovery sweep over a restored store, THE sweep
   SHALL reconstruct the same derived state (republished workflow and activity tasks,
   due-timer injections, timeout-tracker entries, and `SweepResult` tallies) as the
   same sweep over the source store.
2. WHEN a timer whose fire time is already past is present in a restored store, THE
   recovery sweep SHALL inject it as due immediately — absolute-time durable-timer
   semantics, identical to a delayed process restart.
3. THE restored store SHALL require no recovery step beyond what a restart against the
   same store already performs (no snapshot-specific fixup pass anywhere in the
   runtime).

### Requirement 5: Packaging and scope discipline

**User Story:** As the workspace steward, I want the mechanism delivered without
dependency movement or policy creep, so that the slice stays reviewable and the
lockfile untouched.

#### Acceptance Criteria

1. THE feature SHALL use `postcard` (already a dependency of `tokeira-storage`) for
   payload encoding, and SHALL NOT add, remove, or upgrade any workspace dependency;
   `Cargo.lock` SHALL be unchanged.
2. THE snapshot API SHALL be available in the crate's default feature set (making the
   existing optional `postcard` dependency unconditional for this crate is sanctioned;
   it changes no locked dependency versions).
3. THE feature SHALL NOT introduce snapshot scheduling, file I/O, or shutdown hooks in
   any crate.
4. THE change SHALL NOT modify any behaviour of the kernel, the DSQL backend, or any
   public Temporal-facing surface. Derive-only additions (`Serialize`/`Deserialize`
   on the three kernel transition-op enums, `PartialOrd`/`Ord` on ID newtypes in
   `tokeira-types`) are sanctioned per §1's standing serializable-types rule and add
   no I/O or logic.

## Iteration and Feedback Notes

- Design constraints fixed with Ian before this spec: single-lock consistent cut;
  boot-only constructor restore; version-stamp-and-refuse; test-only state reset;
  postcard from the workspace; mechanism-only (policy is T2); past-due timers firing
  on restore is correct and documented.
- Serde derive coverage of every type reachable from `StoreState` was verified
  against the source (2026-08-20): eleven types lack `Serialize`/`Deserialize` (seven
  in `api.rs`, the private `ActivityDispatchEntry`, and the kernel's `ActivityOp`/
  `TimerOp`/`DispatchOp`); nothing reachable holds an `Instant`, trait object, or
  function pointer. The design records the exact derive additions.
