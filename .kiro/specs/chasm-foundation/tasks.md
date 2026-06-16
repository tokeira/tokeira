# Implementation Plan: CHASM Foundation

## Overview

This plan converts the CHASM Foundation design into a series of incremental coding steps for a
code-generation LLM. It is organized into the four delivery layers the design defines, each
independently buildable and testable before the next:

1. **Layer 1 — Substrate**: the `tokeira-chasm` pure crate and `tokeira-chasm-derive` proc-macro.
2. **Layer 2 — Engine + storage**: the CHASM engine surface in `tokeira-runtime` and the node table in
   `tokeira-storage`.
3. **Layer 3 — Activity component + edge**: `tokeira-chasm-activity` (component #1) and the
   `tokeira-edge` `*ActivityExecution` bridge.
4. **Layer 4 — Visibility generalization + activity discovery**: one archetype-neutral, versioned-
   snapshot visibility plane shared by workflows and CHASM components (Requirement 10), the workflow
   producer migrated to snapshots, and edge `List`/`CountActivityExecutions` (Requirement 13).

Each task builds on prior tasks and ends by wiring new code into an integrated, testable surface — no
orphaned code. Property-based tests use `proptest`, run ≥100 iterations, and carry a
`// Feature: chasm-foundation, Property N` tag (`AGENTS §9`; Requirement 12.3). Test sub-tasks are
marked optional with `*`. All code follows `AGENTS`: edition 2024, `thiserror` in libs, no `unwrap`/
`expect` outside tests, no `unsafe`, no runtime reflection, module/public-item docs.

Where the design defers a detail to implementation, the task carries a **ground-truth** callout to
verify against `../temporal @ v1.31.0` before finalizing.

---

## Tasks

### Layer 1 — Substrate (`tokeira-chasm` + `tokeira-chasm-derive`)

- [x] 1. Scaffold the substrate crates and wire them into the workspace
  - [x] 1.1 Scaffold the `tokeira-chasm` pure crate
    - Add `crates/tokeira-chasm` as a workspace member; `Cargo.toml` depends only on `tokeira-types`
      and `tokeira-proto` (plus `serde`, `prost`, `thiserror`); no async/I/O/storage/metrics deps
    - Add the crate `//!` module doc placing it as a peer of `tokeira-kernel` (no kernel dependency)
    - Define the `ChasmError` taxonomy with `thiserror` (`IllegalTransition`, `StaleStamp`,
      `StaleReference`, `Validation`, plus engine-surfaced variants); no `unwrap`/`expect` in lib code
    - _Requirements: 1.1, 1.2, 1.5, 1.6, 6.8_
  - [x] 1.2 Scaffold the `tokeira-chasm-derive` proc-macro crate
    - Add `crates/tokeira-chasm-derive` as a workspace member; `Cargo.toml` sets `proc-macro = true`
      and depends only on `syn`, `quote`, `proc-macro2`
    - Stub the `#[derive(Component)]` entry point with module docs; emit no `unsafe`
    - _Requirements: 1.3, 1.6, 3.6_

- [x] 2. Implement the component model, lifecycle, and field types in `tokeira-chasm`
  - [x] 2.1 Implement `LifecycleState`, the `Component`/`RootComponent` traits, and the
    `Context`/`MutableContext` traits
    - `LifecycleState { Running, Completed, Failed }` with `is_closed()`; `Component` (assoc `Data`,
      `FQN`, `lifecycle_state`, `fields`); `RootComponent` (`terminate`, `context_metadata`)
    - Object-safe `Context` (read-only) and `MutableContext: Context` (adds `add_task` + field
      mutation), returning `Result<_, ChasmError>`
    - _Requirements: 2.1, 2.2, 2.9, 6.3_
  - [x] 2.2 Implement the field types and the field-registry contract
    - `Field<T>`, `Map<K, T>`, `ParentPtr<T>` (lazy value resolution against the tree); the
      `ParentPtr` ancestry walk skips map nodes
    - `FieldDescriptor`, `FieldKind { Data, Component, Map, Parent, Transient }`, `FieldRegistry<'a>`
      consumed by `Component::fields()`
    - _Requirements: 2.5, 2.6, 2.7, 2.8_

- [x] 3. Implement the `VersionedTransition` clock
  - [x] 3.1 Implement `VersionedTransition`, `Staleness`, `staleness_check`, and wire encode/decode
    - `{ namespace_failover_version: i64, transition_count: i64 }`; `staleness_check` yields exactly
      one of `Advanced`/`Same`/`Behind`, comparing failover first then `transition_count`
    - Lossless wire round-trip (ground-truth shape against `hsm.proto:114 @ v1.31.0`)
    - _Requirements: 5.4, 5.5, 5.6_

- [x] 4. Implement the path encoder
  - [x] 4.1 Implement `Path_Encoder` (tokeira-owned)
    - Encode node paths with `$` introducing a child field and `#` introducing a collection child;
      order separators below normal path-segment bytes so a prefix range is exactly a subtree
    - **Ground-truth**: verify the exact separator bytes and sort contract against
      `path_encoder.go:25-75 @ v1.31.0` (reproduce the contract, do not port code)
    - _Requirements: 4.2, 4.5_
  - [ ]* 4.2 Write property test for path-encoder ordering
    - **Property 8: Node range-scan correctness (encoder order)**
    - **Validates: Requirements 4.3**
    - `prop_path_encoder_order`: the sort order makes every subtree/collection a contiguous range;
      ≥100 iterations; `// Feature: chasm-foundation, Property 8` tag
    - _Requirements: 12.3_

- [x] 5. Implement the node, node tree, and atomic transition close
  - [x] 5.1 Implement `Node`, `ExecutionKey`, the node tree, and node (de)serialization
    - `ExecutionKey { namespace_id, business_id, run_id }`; `ChasmNode { metadata, data }`; tree keyed
      by encoded path; each `Field`/`Map` child is its own node; `metadata` always present
    - _Requirements: 4.1, 2.7_
  - [x] 5.2 Implement `close_transaction` (dirty tracking + VT stamping) in the pure crate
    - Track nodes mutated during a transition; on close, stamp every dirty node with a new VT and
      return the dirty-node set plus the transition result; field writes and task schedules are
      committed/rolled-back together (atomic unit returned to the engine)
    - _Requirements: 5.1, 5.2_
  - [ ]* 5.3 Write property test for node serialization round-trip
    - **Property 1: Node serialization round-trip**
    - **Validates: Requirements 9.2**
    - `prop_node_serialization_roundtrip`: `deserialize(serialize(n)) == n` incl. metadata outboxes +
      data; ≥100 iterations; Property 1 tag
    - _Requirements: 12.3_
  - [ ]* 5.4 Write property test for VT monotonicity
    - **Property 3: VT monotonicity**
    - **Validates: Requirements 5.4, 5.5**
    - `prop_vt_monotonicity`: across a committed transition sequence on one execution,
      `vt_{i+1}.staleness_check(vt_i) == Advanced`; ≥100 iterations; Property 3 tag
    - _Requirements: 12.3_

- [x] 6. Implement the registry and library
  - [x] 6.1 Implement `Registry`, `Library`, and `RegistryBuilder`
    - Index components by FQN, by `u32` type id (fingerprint of FQN), and by Rust `TypeId`; reserve
      archetype id `0` for legacy Workflow; build once via builder, immutable thereafter
    - _Requirements: 8.1, 8.2, 8.3_

- [x] 7. Implement `ComponentRef`
  - [x] 7.1 Implement the `ComponentRef` wire type with encode/decode and staleness
    - Carry `execution_key`, `archetype_id`, `execution_versioned_transition`, `component_path[]`,
      `component_initial_versioned_transition`; node identity = `(path, initial_vt)`; staleness via
      `execution_versioned_transition`; lossless wire round-trip (ground-truth `ref.go:16 @ v1.31.0`)
    - _Requirements: 8.4, 8.5, 8.6_
  - [ ]* 7.2 Write property test for `ComponentRef` round-trip + staleness
    - **Property 9: ComponentRef round-trip + staleness**
    - **Validates: Requirements 8.5, 8.6**
    - `prop_component_ref_roundtrip_staleness`: `decode(encode(r)) == r`; a behind-VT ref is reported
      stale; ≥100 iterations; Property 9 tag
    - _Requirements: 12.3_

- [x] 8. Implement the task model and outbox close semantics
  - [x] 8.1 Implement `Task`, `TaskValidator`, `TaskKind`, `TaskValidity`, and the node outbox
    - `TaskKind { Pure, SideEffect }`; `Task` with `KIND` + `fire_at()`; `TaskValidator::validate`;
      persist tasks in the owning node's `pure_tasks[]` / `side_effect_tasks[]` with `(VT, offset)`
      identity
    - _Requirements: 7.1, 7.2_
  - [x] 8.2 Implement outbox re-validation on dirty close
    - On every dirty close, re-validate every outbox task; drop `Drop` results without executing;
      retain `Valid` tasks with stable `(VT, offset)`; compute the single earliest surviving pure-task
      deadline tree-wide and the surviving side-effect tasks, returned from `close_transaction`
    - _Requirements: 7.3, 7.4, 7.5_
  - [ ]* 8.3 Write property test for task validate-then-drop
    - **Property 5: Task validate-then-drop**
    - **Validates: Requirements 7.4, 7.5**
    - `prop_task_validate_then_drop`: dropped tasks never execute; valid tasks keep `(VT, offset)`;
      ≥100 iterations; Property 5 tag
    - _Requirements: 12.3_

- [x] 9. Implement the `#[derive(Component)]` macro
  - [x] 9.1 Generate the static field registry and enforce the four compile-time rules
    - Classify each field as `Field`/`Map`/`ParentPtr`/transient from syntactic types; emit
      `Component::fields()`; enforce: (1) exactly one `#[chasm(data)]` proto field, (2) persistent
      fields are `Field`/`Map`/`ParentPtr` not bare pointers, (3) `Map<K,T>` value bound, (4)
      unmanaged non-transient field is a compile error; emit no `unsafe`, no runtime inspection
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_
  - [ ]* 9.2 Write `trybuild` compile-fail tests for the macro rules
    - Cases: missing data field, two data fields, unmanaged non-transient field, bad map value type;
      assert each fails with a clear `compile_error!`
    - _Requirements: 3.2, 3.3, 3.4, 3.5_

- [x] 10. Checkpoint — Layer 1 substrate
  - Ensure `cargo +nightly fmt --all --check`, `cargo lint`, and `cargo test` pass for
    `tokeira-chasm` and `tokeira-chasm-derive`. Ensure all tests pass, ask the user if questions arise.

### Layer 2 — Engine + storage (`tokeira-runtime` engine + `tokeira-storage` node table)

- [x] 11. Implement node storage on DSQL
  - [x] 11.1 Add the node table migration
    - Single base `CREATE TABLE chasm_node` migration keyed by `(namespace_id, business_id, run_id,
      encoded_path)` carrying VT stamp, initial VT, `metadata`, nullable `data`; DSQL-safe subset (one
      statement per file, no `CHECK`, no `BIGSERIAL`, secondary indexes `ASYNC`); no `ALTER` (build
      phase). Read `crates/tokeira-storage/AGENTS.md` first.
    - _Requirements: 9.1, 9.7, 9.8_
  - [x] 11.2 Implement the node store: write-only-dirty-nodes with OCC/CAS fencing
    - Persist exactly the dirty-node set; condition each write on the node's stored VT matching the VT
      read (compare-and-set); on conflict, reload + re-run the transition (no force-overwrite); store
      task outboxes inside `metadata`
    - _Requirements: 9.3, 9.4, 9.5, 9.6_
  - [x] 11.3 Implement encoded-path prefix range-scan loads
    - Load a subtree, a collection, or an ancestor chain as a single prefix range scan over
      `encoded_path` within one execution
    - _Requirements: 4.4_
  - [ ]* 11.4 Write property test for dirty-only writes
    - **Property 4: Dirty-only writes**
    - **Validates: Requirements 9.3**
    - `prop_dirty_only_writes`: persisted node set equals dirtied node set (no clean rewrite, no
      skipped dirty); ≥100 iterations; Property 4 tag
    - _Requirements: 12.3_
  - [ ]* 11.5 Write property test for node range-scan correctness
    - **Property 8: Node range-scan correctness**
    - **Validates: Requirements 4.4**
    - `prop_node_range_scan`: the prefix range returns exactly the subtree / collection children;
      ≥100 iterations; Property 8 tag
    - _Requirements: 12.3_

- [x] 12. Implement the engine surface in `tokeira-runtime`
  - [x] 12.1 Implement the `Engine` trait, request/outcome types, and `Context`/`MutableContext` impls
    - Async `Engine` trait with all seven operations; explicit engine handle (no ambient injection);
      runtime `Context`/`MutableContext` impls; `MutableContext` only inside a transition. Read
      `crates/tokeira-runtime/AGENTS.md` first.
    - _Requirements: 6.1, 6.3, 6.4, 6.7_
  - [x] 12.2 Implement `StartExecution`, `UpdateWithStartExecution`, `UpdateComponent`, `ReadComponent`
    - Drive transitions through tokeira's fenced `commit_transition`; `UpdateWithStart` starts-if-
      absent then updates in one transition honouring the business-id reuse/conflict policy; `Read` is
      a snapshot load with no dirty nodes/tasks
    - _Requirements: 6.1, 6.2, 5.3_
  - [x] 12.3 Implement the `TypedEngine<C>` wrappers
    - Generic monomorphized `start`/`update`/`read` over a concrete `Component` (no runtime dispatch)
    - _Requirements: 6.1, 6.7_
  - [x] 12.4 Implement execution close on root lifecycle + reject mutations on a closed execution
    - When the root `lifecycle_state()` is closed, close the Execution; reject any further mutating
      transition on a closed execution
    - _Requirements: 2.3, 2.4_
  - [x] 12.5 Implement `DeleteExecution` (range delete) and `NotifyExecution`
    - Range-delete the node subtree; notify hook to wake pollers / re-evaluate tasks
    - _Requirements: 6.1_
  - [ ]* 12.6 Write property test for lifecycle implies execution close
    - **Property 11: Lifecycle implies execution close**
    - **Validates: Requirements 2.3, 2.4**
    - `prop_lifecycle_close`: when the root is closed, the execution is closed and no further mutating
      transition is admitted (uses a minimal test root component); ≥100 iterations; Property 11 tag
    - _Requirements: 12.3_

- [x] 13. Implement long-poll, side-effect dispatch, and the physical timer
  - [x] 13.1 Implement `PollComponent` monotonic long-poll
    - Block on a notify keyed by execution VT; resolve when the component VT advances past the caller's
      token; return empty when the deadline minus `longPollBuffer` elapses without advancing
    - _Requirements: 6.5, 6.6_
  - [x] 13.2 Implement post-commit side-effect dispatch and single physical timer arming
    - Dispatch surviving side-effect tasks only post-commit; arm at most one physical timer per
      execution tree at the earliest valid pure-task `fire_at()` tree-wide; hold `physical_task_status`
      as engine-local, non-replicated state that never bumps the VT
    - _Requirements: 7.6, 7.7, 7.8_
  - [ ]* 13.3 Write property test for long-poll monotonicity
    - **Property 7: Long-poll monotonicity**
    - **Validates: Requirements 6.5, 6.6**
    - `prop_long_poll_monotonicity`: a poll never returns a state `Behind` the caller's token (returns
      `Advanced` or empty); ≥100 iterations; Property 7 tag
    - _Requirements: 12.3_
  - [ ]* 13.4 Write property test for the single earliest pure timer
    - **Property 6: Single earliest pure timer**
    - **Validates: Requirements 7.6**
    - `prop_single_earliest_pure_timer`: at most one armed timer, equal to the earliest valid pure task
      tree-wide; ≥100 iterations; Property 6 tag
    - _Requirements: 12.3_

- [x] 14. Implement the visibility hook
  - [x] 14.1 Implement the search-attribute provider hook → projection sink
    - On transition close, collect the contributing component's declared search attributes and emit
      them as derived projection writes to `tokeira-projection`, off the correctness path
    - _Requirements: 10.1, 10.2, 10.3_

- [x] 15. Engine integration tests
  - [x]* 15.1 Write engine integration tests over the in-memory store
    - Drive start/update/read/delete and the OCC reload-and-rerun path; use synchronization primitives,
      no sleeps (`AGENTS §1`)
    - _Requirements: 6.1, 9.5_

- [x] 16. Checkpoint — Layer 2 engine + storage
  - Ensure `cargo +nightly fmt --all --check`, `cargo lint`, and `cargo test` pass for the new
    `tokeira-runtime` and `tokeira-storage` surfaces. Ensure all tests pass, ask the user if questions
    arise.

### Layer 3 — Activity component (`tokeira-chasm-activity`) + edge wiring

- [x] 17. Scaffold and model the activity component
  - [x] 17.1 Scaffold the `tokeira-chasm-activity` crate
    - Add `crates/tokeira-chasm-activity` as a workspace member depending on `tokeira-chasm`,
      `tokeira-chasm-derive`, `tokeira-types`, `tokeira-proto`; no engine internals
    - _Requirements: 1.4_
  - [x] 17.2 Implement the `ActivityExecution` root component and lifecycle mapping
    - `#[derive(Component)]` with `#[chasm(fqn = "activity.activity")]`, one `#[chasm(data)]`
      `ActivityStateProto` (status + attempt `stamp`), input/timeouts/retry/result data fields; register
      under library `activity`; `ActivityStatus` enum (8 states); lifecycle mapping
      (`COMPLETED→Completed`; `FAILED|CANCELED|TERMINATED|TIMED_OUT→Failed`; else `Running`)
    - _Requirements: 11.1, 11.2, 11.3_

- [x] 18. Implement the activity transition table
  - [x] 18.1 Implement `apply()` with stamp fencing
    - Legal transitions per `statemachine.go @ v1.31.0` (Scheduled/Rescheduled/Started/Completed/
      Failed/CancelRequested/Canceled/Terminated/TimedOut); illegal `(from, event)` →
      `Err(IllegalTransition)` leaving state unchanged; stamp mismatch → superseded (no-op); on
      Scheduled/Started schedule the relevant timers + dispatch task; on terminal, let validate-then-
      drop reap stragglers
    - _Requirements: 11.4, 11.5, 11.6_
  - [ ]* 18.2 Write property test for transition legality
    - **Property 2: Transition legality**
    - **Validates: Requirements 11.4, 11.5**
    - `prop_transition_legality`: random event sequences never reach an illegal state; illegal events
      rejected and leave state unchanged; ≥100 iterations; Property 2 tag
    - _Requirements: 12.3_
  - [x]* 18.3 Write unit tests for lifecycle mapping and stamp supersession
    - Cover the lifecycle mapping for every `ActivityStatus` and stamp-mismatch supersession
    - _Requirements: 11.3, 11.6_

- [x] 19. Implement the activity tasks
  - [x] 19.1 Implement the `dispatch` side-effect task and its validator
    - `TaskKind::SideEffect`; post-commit enqueue to matching (`AddActivityTask`); validator drops if
      the attempt advanced or the activity already started/closed; stamp-fenced
    - _Requirements: 11.7_
  - [x] 19.2 Implement the pure timer tasks and their validators
    - `scheduleToStart`, `scheduleToClose`, `startToClose`, `heartbeat` as `TaskKind::Pure`; each
      `fire_at()` from normalized timeouts; each validator drops on stamp mismatch or terminal state;
      firing produces a `TimedOut` (or reschedule) transition
    - _Requirements: 11.7_

- [x] 20. Implement activity validation and configuration
  - [x] 20.1 Implement request validation
    - Require a user-defined task queue; require `activityId`/`activityType` non-empty and within
      `MaxIDLengthLimit`; retry-policy defaulting; timeout normalization (schedule-to-start /
      schedule-to-close / start-to-close capped to run timeout; heartbeat ≤ start-to-close)
    - **Ground-truth**: reproduce the rules against `validator.go @ v1.31.0` (do not invent)
    - _Requirements: 11.9_
  - [x] 20.2 Implement the activity config (config-as-constant)
    - `{ enable_standalone: bool = false (per-namespace), long_poll_timeout = 20s, long_poll_buffer =
      1s }`; `serde(deny_unknown_fields)`; no env vars (`AGENTS` Configuration)
    - _Requirements: 11.11, 11.12_
  - [ ]* 20.3 Write property test for config round-trip
    - **Property 10: Config round-trip**
    - **Validates: Requirements 11.12**
    - `prop_activity_config_roundtrip`: config round-trips without loss; unknown fields rejected; ≥100
      iterations; Property 10 tag
    - _Requirements: 12.3_
  - [x]* 20.4 Write unit tests for validation edge cases
    - Missing task queue, over-length id, timeout normalization incl. heartbeat ≤ start-to-close and
      cap-to-run-timeout
    - _Requirements: 11.9_

- [x] 21. Wire the edge bridge
  - [x] 21.1 Translate the public `*ActivityExecution` RPCs to engine calls
    - Map `Start→StartExecution`, `RequestCancel`/`Terminate→UpdateComponent`, `Describe→ReadComponent`,
      `Poll→PollComponent`, `Delete→DeleteExecution`; validate per task 20.1 before admitting; route
      `ListActivityExecutions`/`CountActivityExecutions` to the projection plane. Read
      `crates/tokeira-edge/AGENTS.md` first.
    - _Requirements: 11.8_
  - [x] 21.2 Implement the `activity.enableStandalone` per-namespace gate
    - When disabled for a namespace, do not admit the request and return the targeted-release status
    - **Ground-truth**: resolve the exact disabled-feature gRPC status against `frontend.go @ v1.31.0`
      (likely `FAILED_PRECONDITION`/`UNIMPLEMENTED`) before finalizing — verify against v1.31.0
    - _Requirements: 11.10_
  - [x] 21.3 Contribute the activity search attributes
    - Declare `ActivityType`, `ExecutionStatus`, `TaskQueue` through the visibility hook (task 14.1) so
      they flow to projection
    - _Requirements: 10.4_

- [ ] 22. End-to-end activity tests
  - [ ]* 22.1 Write the happy-path integration test
    - start → dispatch → started → completed → describe/poll over the in-memory store
    - _Requirements: 11.7, 11.8_
  - [ ]* 22.2 Write timeout, cancel/terminate, and long-poll-wake integration tests
    - schedule-to-start / start-to-close / heartbeat fire → `TimedOut`; cancel/terminate paths;
      long-poll wake-on-VT-advance using synchronization primitives (no sleeps)
    - _Requirements: 11.7, 6.5_
  - [ ]* 22.3 Write edge-level tests for the gate and translation
    - Assert the `enableStandalone` gate admits/denies correctly and the request/response translation
      matches the targeted-release contract
    - _Requirements: 11.10, 11.8_

### Layer 4 — Visibility generalization + activity discovery (`tokeira-projection`, `tokeira-storage`, kernel emission, `tokeira-runtime` adapter, `tokeira-edge`)

> Implements Requirement 10 (archetype-neutral versioned-snapshot plane) and Requirement 13 (activity
> discovery + scoping). Sequencing decision (confirmed): the workflow producer migrates to versioned
> snapshots **now**, in Stage 1 — no throwaway delta-fold. The authoritative design is design.md
> "Visibility Generalization: One Logical Index for All Archetypes". Each stage compiles + tests; the
> workflow list/count/UI stays green throughout.

- [x] 23. Stage 1 — Generalize the visibility store, record, and sink; migrate the workflow producer to snapshots
  - [x] 23.1 Generalize the projection record + store model to the versioned-snapshot / archetype shape
    - Replace the delta `ProjectionOp` contract with a versioned `VisibilitySnapshot` record carrying
      `(namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq)` plus the full
      post-transition image; introduce first-class non-null `archetype_id`, `status_keyword`,
      `lifecycle_state` (OPEN/CLOSED/DELETED), generic lifecycle timestamps, `execution_type`,
      `task_queue`, generic `transition_count`, `memo_blob`, `search_attr_generation`. Touches
      `tokeira-projection/src/{types.rs,store.rs,visibility_api.rs}` and
      `tokeira-storage/src/api.rs` (`ProjectionContext`/`ProjectionRecord`). Read
      `crates/tokeira-storage/AGENTS.md` first.
    - _Requirements: 10.1, 10.2, 10.5, 10.6_
  - [x] 23.2 Add the generalized visibility DSQL migrations
    - Single base `CREATE TABLE execution_visibility_current` keyed `(namespace_id, archetype_id,
      run_key)` with composite index `(namespace_id, archetype_id, status_keyword, start_time DESC,
      run_key)` created `ASYNC`; typed attr-index table carrying `generation`; striped rollup table
      `(namespace_id, archetype_id, dimension, value, stripe)`; `projection_checkpoint(partition,
      last_applied_version)`. DSQL-safe subset (one statement per file, no `CHECK`, no `BIGSERIAL`,
      indexes `ASYNC`); no `ALTER` (build phase).
    - _Requirements: 10.7, 10.8, 10.9_
  - [x] 23.3 Make the sink monotonic + idempotent (upsert-iff-newer) with the generation pattern
    - Apply a snapshot only when its `(authority_epoch, source_transition_seq)` is strictly newer than
      the stored version; write typed-attr rows at generation N then flip `search_attr_generation = N`
      and GC old generations; reserve system fields (`archetype`/`status`/`lifecycle_state`/
      `namespace`/`run_id`/`business_id`); striped rollups guarded by applied-version and rebuildable
      from current; partitioned checkpoints. Replaces the delta-fold in `visibility_sink.rs`.
    - _Requirements: 10.3, 10.7, 10.8, 10.10, 10.12_
  - [x] 23.4 Migrate the workflow producer to versioned snapshots
    - Change the kernel projection emission from `ProjectionOp::UpsertExecution`/`CloseExecution`
      deltas to a full versioned `VisibilitySnapshot` (archetype = workflow; `status_keyword` +
      `lifecycle_state` derived from workflow `ExecutionStatus`), stamped with the workflow transition
      seq as `source_transition_seq` and the run's authority as `authority_epoch`. Drop the sink's
      delta-fold path. Workflow list/count/UI behaviour is preserved.
    - **Ground-truth**: the workflow `ExecutionStatus` → `status_keyword`/`lifecycle_state` mapping must
      keep `v1.31.0` list/count semantics (OPEN vs CLOSED, terminal statuses).
    - _Requirements: 10.13, 10.2, 10.14_
  - [x]* 23.5 Write property test for monotonic idempotent snapshot apply
    - **Property 12: Monotonic snapshot apply**
    - **Validates: Requirements 10.3**
    - `prop_monotonic_snapshot_apply`: arbitrary interleaving/retry/out-of-order of snapshots converges
      to the newest-version image and never regresses status nor revives a closed execution; ≥100
      iterations; `// Feature: chasm-foundation, Property 12` tag
    - _Requirements: 12.3_
  - [x]* 23.6 Write property test for rollup idempotence + rebuildability
    - **Property 13: Idempotent rollups**
    - **Validates: Requirements 10.8**
    - `prop_rollup_idempotent_rebuildable`: replayed snapshots never double-count; striped rollups equal
      a fresh rebuild from `current`; ≥100 iterations; `// Feature: chasm-foundation, Property 13` tag
    - _Requirements: 12.3_
  - [x] 23.7 Migrate the projection read paths to `status_keyword` and port the DSQL visibility store onto the generalized schema
    - **Read-path status migration.** The store/record/sink were generalized in 23.1/23.3, but the
      in-memory and DSQL **query** layer still keyed status off the workflow-typed `ExecutionStatus`
      enum (the list filter via `FilterValue::Status(row.status)`, the `group_by` value, and the
      rollup dimension all read `format!("{:?}", row.status)`). Migrated them to the generic,
      archetype-scoped `status_keyword` column so a non-workflow archetype (activity) whose status
      has no `ExecutionStatus` variant can be listed/counted/rolled-up. The typed `ExecutionRow.status`
      is now workflow-facing output only (the `ExecutionSummary`), not an index query key.
    - **DSQL store port.** Ported the DSQL visibility store off the workflow-only `vis_execution`/
      `vis_rollup`/`sa_current`/`sa_*_idx` tables onto the generalized schema: rows →
      `execution_visibility_current` (apply-iff-newer version guard), rollups →
      `execution_visibility_rollup` (archetype-scoped, 16-way striped), custom-SA index →
      `execution_visibility_attr_index` (single-table delete+insert replace, typed value columns +
      `value_discriminator`). The attr index deliberately deviates from 23.3's generation pattern —
      recorded in `V052` + `reference/DECISION-visibility-dsql-schema.md` (tiny attr sets make the
      generation pattern's conflict-surface win marginal versus its query-side cost). Workflows and
      activities now share one DSQL index.
    - **Verified.** Workflow List/Count/group-by/rollup behaviour unchanged: 47 in-memory projection
      tests pass incl. Properties 12/13; the pure `compile_filter` SQL tests pass under
      `--features dsql`; default + edge + storage compile clean (0 warnings).
    - **Carved out** (now 23.8 / cleanup): the projection **checkpoint** table port (needs a
      `VisibilityStore` trait-shape change → 23.8) and retiring the now-unused legacy `vis_*`/`sa_*`
      migrations (blocked by the build-phase no-gap constraint — separate cleanup).
    - Commits: `1f71cce0` (in-memory read path + types), `e7946c45` (DSQL row + read path),
      `68ef7501` (DSQL rollup), `26af4204` (DSQL attr index).
    - **Ground-truth**: status is a low-cardinality keyword SA in `v1.31.0`, not an enum column;
      see `reference/DECISION-visibility-status-keyword.md` (`activity.go:40,594,932 @ v1.31.0`).
    - _Requirements: 10.5, 10.13_
  - [x] 23.8 Port the DSQL projection checkpoint onto `projection_checkpoint` (V055)
    - Reshaped the `VisibilityStore` checkpoint methods from `sink_id: &str` to
      `load_checkpoint(partition_id: u32)` / `save_checkpoint(&cursor)` (partition from
      `cursor.partition_id`), and ported the DSQL impl onto V055 `projection_checkpoint`
      (`partition_id` PK, `last_applied_version` BYTEA). The redundant `sink_id` is gone: the bootstrap
      already runs one worker per partition and smuggled the partition through the sink-id string while
      the cursor already carried the real `partition_id`. Removed `VisibilitySink`'s now-dead `sink_id`
      field / ctor param / unused `sink_id()` getter and the bootstrap factory's `String` argument.
    - **fanout guard.** V055 keys by `partition_id` alone, so the worker resumes a stored checkpoint
      only when `stored.fanout == expected.fanout`, else restarts the partition from the beginning —
      safe because apply is idempotent + monotonic (Properties 12/13). See
      `reference/DECISION-visibility-checkpoint-partition.md`.
    - **Verified.** projection lib+tests compile clean (default + `--features dsql`, 0 warnings);
      47 in-memory tests pass incl. `prop_checkpoint_round_trip` (now partition-keyed) and the worker
      checkpoint tests; `tokeirad` + `grpc_roundtrip` compile and 6/7 pass (the 1 failure,
      `..._update_completed_through_protocol_messages`, is pre-existing on clean HEAD — unrelated).
    - The legacy `projector_checkpoint` (V011) is now unused but its migration file stays: deleting a
      non-highest build-phase migration would gap the versions. Retiring it is the separate `vis_*`/
      `sa_*` migration cleanup.
    - **Classification: Standard** (root `AGENTS.md` § Change Classification). It conforms an internal
      Rust trait to an already-decided requirement (Req 10.9) + committed migration (V055). It is *not* a
      wire-contract change — "wire contract" is reserved for the vendored Temporal protos under
      `proto/upstream/` (§8); `VisibilityStore` is plumbing between the projection/runtime/storage crates.
      It is *not* an open architectural decision: the Architectural tier's "spec update **or** approval"
      is already satisfied by Req 10.9 + V055, and build-phase schema is malleable until a baseline is cut
      (`AGENTS.md` migration rules). Proceed without escalation; tests pass + follow existing patterns.
    - Design point (resolved in `reference/DECISION-visibility-checkpoint-partition.md`, not escalated):
      `partition_id` was already a first-class field on `ProjectionCursor` and the projection log already
      partitions by it, so no new derivation was invented — the port just keys the checkpoint on the
      `partition_id` the cursor already carries; `fanout` rides inside `last_applied_version` with the
      resume-time guard above.
    - _Requirements: 10.9_

- [x] 24. Stage 2 — CHASM `VisibilitySnapshot` contract + engine→projection adapter + bootstrap wiring
  - [x] 24.1 Define the typed `VisibilitySnapshot` contribution interface in `tokeira-chasm`
    - `tokeira_chasm::VisibilitySnapshot` + the `VisibilityContributor` hook carry the typed
      post-transition image (status_keyword, lifecycle, lifecycle timestamps, execution_type/task_queue,
      **typed** search attributes, memo); the version/namespace/run_key/archetype the component cannot
      know are stamped by the runtime adapter (24.2). Reserved system fields are structurally separate
      from `search_attributes` and the adapter rejects any reserved name appearing there (Req 10.10).
      The interface is now consumed end-to-end by the engine hook + adapter (24.2).
    - _Requirements: 10.2, 10.10_
  - [x] 24.2 Implement the engine→projection adapter and replace `NoopVisibilitySink` at bootstrap
    - Widened the engine's `VisibilitySink` hook from `record(key, Vec<(String,String)>)` to
      `record(key, archetype_id, version, snapshot)`; the typed layer (`chasm/typed.rs`) builds the
      `VisibilitySnapshot` from the live component on start **and** update (mirroring how it already
      builds string `search_attributes`) and threads it through `Start`/`UpdateRequest`. New
      `ProjectionVisibilitySink` (in `tokeira-runtime`) converts the snapshot to a `ProjectionRecord`
      and applies it through the **same** projection `ProjectionSink::apply` the workflow worker uses —
      so activities and workflows land in one logical index. Wired into `tokeirad` bootstrap in place of
      `NoopVisibilitySink`. The post-commit visibility write is best-effort (logged, not propagated) so a
      failed projection cannot fail the committed transition (Req 10.15).
    - **Settled by spec**: writes to the shared visibility store directly (not `projection_log`),
      design.md:562-564; the "one log, two producers" soundness (design.md:403-416) licenses the second
      store writer under apply-iff-newer fencing. Mapping/threading recorded in
      `reference/DECISION-visibility-engine-adapter.md`.
    - **Verified.** `tokeira-runtime` lib compiles clean (0 warnings); 290 lib tests pass + 4 new
      `visibility_adapter` mapping tests (run_key=run_id, lifecycle/version mapping, reserved-field
      rejection, non-UUID rejection); `tokeirad`/edge/activity compile; `--features dsql` clean;
      `grpc_roundtrip` 6/7 (the 1 failure is pre-existing on clean HEAD, unrelated).
    - _Requirements: 10.2, 10.11, 10.15_
  - [x] 24.3 Implement the activity component's `VisibilitySnapshot` contribution
    - `ActivityExecution::visibility_snapshot` contributes `ActivityType`/`TaskQueue` as the generic
      `execution_type`/`task_queue` system fields, `lifecycle_state` from the activity status, the
      scheduled time as `start_time`, and **no** user EAV rows (reserved fields stay system fields).
    - **status_keyword is the collapsed API status** — `Scheduled`/`Started`/`CancelRequested` →
      `Running`, terminals pass through (new `api_status_name`), so list/count filtering by
      `ExecutionStatus = Running` matches a scheduled/started activity. Ground-truth:
      `InternalStatusToAPIStatus` (`activity.go:594,932 @ v1.31.0`);
      `reference/DECISION-visibility-status-keyword.md`. (The legacy string `search_attributes()` hook,
      no longer read by the engine, still emits the fine-grained name; it is phasing out per 24.1.)
    - **close_time on close**: a component persists no close timestamp, so the engine stamps the
      transition wall-clock into the snapshot when the lifecycle closes (`post_commit`), CHASM-only.
    - Verified: 27 activity-crate tests pass incl. the collapse table (`Scheduled`→`Running`, …) and the
      system-fields snapshot test; runtime lib tests green with the close-time stamp.
    - _Requirements: 10.4_

- [ ] 25. Stage 3 — Edge `ListActivityExecutions`/`CountActivityExecutions` + scoping + capability flag
  - [x] 25.1 Implement archetype-scoped activity List/Count at the edge
    - **Scoping (no caller escape).** `CompiledFilter` carries a forced `archetype` the visibility
      endpoints set after compiling the user query; the workflow endpoints pin `ArchetypeId::WORKFLOW`
      (fixing the Stage-2 leak where workflow List/Count returned activity rows) and the activity
      endpoints pin the activity archetype. Both stores apply it (in-memory `matches_filter` gate; DSQL
      inlined `archetype_id` clause). Commit `0330d2b1`.
    - **Activity endpoints.** `VisibilityApi.list_activities`/`count_activities` (projection-side,
      commit `49644670`) take the archetype id explicitly because the projection plane is
      archetype-neutral. The gRPC `list_activity_executions`/`count_activity_executions` handlers
      (previously `UNIMPLEMENTED`) resolve the activity archetype id from the activity bridge
      (`ActivityBridge::archetype_id()`, the stable `archetype_id_for_fqn`) and route through the edge
      `WorkflowService` to the visibility plane; they stay `UNIMPLEMENTED` when standalone activities
      are disabled (no bridge). New `Action::List/CountActivityExecutions` for the interceptor.
    - _Requirements: 13.1_
  - [ ] 25.2 Implement `TemporalNamespaceDivision` as a virtual system SA compiling to `archetype_id`
    - Accept it in visibility query syntax and compile it to `archetype_id`; never store or resolve it
      as a generic string search attribute.
    - _Requirements: 13.2_
  - [x] 25.3 Translate projected rows to `ActivityExecutionListInfo`
    - `activity_execution_list_info_from_summary` builds the wire shape: `activity_id`/`activity_type`
      from the generic business id / execution type, `state_transition_count` from the generic
      `transition_count` (Req 10.14), `execution_duration` derived as `close - schedule` (populated only
      when closed, per the proto), timestamps, search attributes.
    - **status mapping (ground-truthed).** `activity_status_to_proto` maps the stored collapsed
      `status_keyword` → `ActivityExecutionStatus`: `Running` (covering SCHEDULED/STARTED/
      CANCEL_REQUESTED) → `RUNNING`, terminals 1:1. This is the enum's own RUNNING semantics
      (`enums/v1/activity.proto:ACTIVITY_EXECUTION_STATUS_RUNNING @ v1.31.0`), confirming the 24.3
      collapse — so the mapping is a clean 1:1 from the index value.
    - Verified: edge translate unit tests (`activity_status_keyword_maps_to_wire_enum`,
      `activity_summary_translates_to_list_info`); 224 edge lib tests pass; tokeirad builds clean.
    - _Requirements: 13.3, 10.14_
  - [ ] 25.4 Report the `standalone_activities` capability from `enableStandalone`
    - `namespace_to_proto` sets `standalone_activities` from the effective `activity.enableStandalone`
      (server-uniform), not hardcoded `false`.
    - **Ground-truth**: `namespace_handler.go:868 @ v1.31.0`.
    - _Requirements: 13.4_

- [ ] 26. Stage 4 — Hardening: repair scanner / outbox-in-commit
  - [ ] 26.1 Guarantee a committed transition cannot permanently lack a projection
    - Provide an outbox row written in the authoritative commit, or a repair scanner that finds
      committed transitions whose visibility version is unprojected and re-emits the snapshot
      (transition-derived, repairable — C2.5).
    - _Requirements: 10.11_
  - [ ]* 26.2 Write property test for repair convergence
    - **Property 14: Repair convergence**
    - **Validates: Requirements 10.11**
    - `prop_repair_convergence`: after arbitrary dropped projections, repair drives the index to the
      fold of the latest committed snapshots; ≥100 iterations; `// Feature: chasm-foundation, Property
      14` tag
    - _Requirements: 12.3_

- [ ] 27. Final checkpoint — full feature
  - Ensure `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`, and
    `cargo test --workspace` pass across all three new crates and the touched runtime/storage/edge/
    projection surfaces. Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP; core
  implementation tasks are never optional.
- Each task references specific requirement sub-clauses for traceability; property-test tasks also
  reference Requirement 12.3 (the `proptest` ≥100-iteration + tag posture).
- Property tests validate the design's universal correctness properties (Properties 1–14); unit tests
  validate specific examples and edge cases; integration tests exercise end-to-end flows.
- Verification is gated by tokeira's own unit + property tests, not the Temporal Go corpus (Requirement
  12.1, 12.2), because the out-of-process harness cannot deliver the in-process override (foundation §7).
- **Ground-truth** callouts mark details the design defers to implementation: the exact path-encoder
  separator bytes/sort (4.1), the `ComponentRef`/VT wire shapes (3.1, 7.1), the activity validation
  rules (20.1), the disabled-feature gRPC status (21.2), the workflow `ExecutionStatus` →
  `status_keyword`/`lifecycle_state` mapping (23.4), the `ActivityExecutionListInfo` wire shape (25.3),
  and the `standalone_activities` namespace-capability source (25.4) — each verified against
  `../temporal @ v1.31.0` per `AGENTS §8` before finalizing.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.2", "3.1", "4.1"] },
    { "id": 2, "tasks": ["2.1", "4.2", "5.1"] },
    { "id": 3, "tasks": ["5.2", "6.1", "7.1", "8.1", "9.1"] },
    { "id": 4, "tasks": ["5.3", "5.4", "7.2", "8.2", "9.2"] },
    { "id": 5, "tasks": ["8.3"] },
    { "id": 6, "tasks": ["11.1"] },
    { "id": 7, "tasks": ["11.2", "11.3"] },
    { "id": 8, "tasks": ["11.4", "11.5", "12.1"] },
    { "id": 9, "tasks": ["12.2", "12.3", "12.5"] },
    { "id": 10, "tasks": ["12.4", "13.1"] },
    { "id": 11, "tasks": ["12.6", "13.2", "13.3"] },
    { "id": 12, "tasks": ["13.4", "14.1"] },
    { "id": 13, "tasks": ["15.1"] },
    { "id": 14, "tasks": ["17.1"] },
    { "id": 15, "tasks": ["17.2"] },
    { "id": 16, "tasks": ["18.1", "19.1", "20.1", "20.2"] },
    { "id": 17, "tasks": ["18.2", "18.3", "19.2", "20.3", "20.4"] },
    { "id": 18, "tasks": ["21.1", "21.3"] },
    { "id": 19, "tasks": ["21.2"] },
    { "id": 20, "tasks": ["22.1", "22.2", "22.3"] },
    { "id": 21, "tasks": ["23.1", "23.2"] },
    { "id": 22, "tasks": ["23.3", "23.4"] },
    { "id": 23, "tasks": ["23.5", "23.6", "23.7", "23.8"] },
    { "id": 24, "tasks": ["24.1"] },
    { "id": 25, "tasks": ["24.2", "24.3"] },
    { "id": 26, "tasks": ["25.1", "25.2", "25.4"] },
    { "id": 27, "tasks": ["25.3"] },
    { "id": 28, "tasks": ["26.1"] },
    { "id": 29, "tasks": ["26.2"] }
  ]
}
```
