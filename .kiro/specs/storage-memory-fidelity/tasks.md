# Implementation Plan: Storage Memory Fidelity

## Overview

Close the fidelity gaps between `InMemoryStore` and the planned DSQL backend by adding activity dispatch tracking, an explicit-persist dispatch backlog (Tier C per 040-delivery-broker), OCC conflict injection, configurable current-execution conflict policies (`Reject` and `AllowAfterClose`), and independent normalized activity/timer structures. All changes are scoped to `tokeira-storage` (`api.rs` and `memory.rs`), plus a `proptest` dev-dependency in `Cargo.toml`.

## Tasks

- [x] 1. Add new types and trait extensions in `api.rs`
  - [x] 1.1 Define new data types in `api.rs`
    - Add `DispatchableActivityTask` struct with fields: `run_key`, `queue`, `activity_id`, `schedule_event_id`, `attempt`
    - Add `BacklogTaskKind` enum with `Workflow` and `Activity { activity_id: String }` variants
    - Add `BacklogEntry` struct with fields: `run_key`, `queue`, `kind`, `insertion_seq`
    - Add `CurrentExecutionConflictPolicy` enum with `Reject` and `AllowAfterClose` variants and `Default` impl returning `Reject`
    - _Requirements: 2.3, 3.5, 5.1_

  - [x] 1.2 Add `list_dispatchable_activity_tasks`, `persist_to_backlog`, and `drain_backlog` to `RunRepository` trait
    - Add `async fn list_dispatchable_activity_tasks(&self, queue: &QueueKey, limit: usize) -> Result<Vec<DispatchableActivityTask>>` to the trait
    - Add `async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()>` to the trait
    - Add `async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>>` to the trait
    - _Requirements: 2.1, 3.1, 3.2_

  - [x] 1.3 Add `Arc<T>` blanket impls for the three new trait methods
    - Extend the existing `impl<T> RunRepository for std::sync::Arc<T>` block with delegating implementations for `list_dispatchable_activity_tasks`, `persist_to_backlog`, and `drain_backlog`
    - _Requirements: 2.1, 3.1, 3.2_

- [x] 2. Extend `StoreState` and add helper methods on `InMemoryStore`
  - [x] 2.1 Add new fields to `StoreState` in `memory.rs`
    - Add `activity_dispatch: HashMap<(RunKey, String), DispatchableActivityTask>` (Req 1)
    - Add `dispatch_backlog: VecDeque<BacklogEntry>` and `backlog_next_seq: u64` (Req 3)
    - Add `conflict_injections: HashMap<RunKey, usize>` (Req 4)
    - Add `conflict_policy: CurrentExecutionConflictPolicy` (Req 5)
    - Add `activity_state_table: HashMap<(RunKey, String), ActivityState>` (Req 6)
    - Add `timer_bucket: HashMap<(RunKey, String), TimerState>` (Req 6)
    - Update the `Default` impl to initialize all new fields
    - _Requirements: 1.1, 3.1, 4.1, 5.1, 6.1, 6.3_

  - [x] 2.2 Implement `inject_conflict` and `set_conflict_policy` on `InMemoryStore`
    - `pub async fn inject_conflict(&self, run_key: RunKey, count: usize)` — sets `conflict_injections[run_key] = count`
    - `pub async fn set_conflict_policy(&self, policy: CurrentExecutionConflictPolicy)` — sets `conflict_policy`
    - _Requirements: 4.1, 4.4, 5.2, 5.6_

- [x] 3. Checkpoint
  - Ensure the project compiles with `cargo check -p tokeira-storage`. Ask the user if questions arise.

- [x] 4. Update `commit_transition` with new commit-flow steps
  - [x] 4.1 Add conflict injection check at the top of `commit_transition`
    - After acquiring the lock, before the existing OCC fence check, check `conflict_injections[run_key]`; if > 0, decrement and return `CommitResult::Conflict` with a reason string indicating the conflict was injected, without modifying any stored state
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 4.2 Replace hardcoded current-execution conflict logic with policy-based dispatch
    - Replace the existing `if transition.expected_seq == ZERO && state.status.is_open()` block with a `match` on `store.conflict_policy`
    - `Reject`: return `Conflict` when an open execution exists (existing behavior)
    - `AllowAfterClose`: return `Conflict` only when an open execution exists; allow creation when only closed executions exist
    - _Requirements: 5.3, 5.4, 5.5, 5.6_

  - [x] 4.3 Apply `ActivityOp`s to the independent activity state table
    - On `ActivityOp::Upsert(activity_state)`: insert/update `activity_state_table[(run_key, activity_state.activity_id.clone())]`
    - On `ActivityOp::Delete { activity_id }`: remove from `activity_state_table[(run_key, activity_id)]` and also remove from `activity_dispatch[(run_key, activity_id)]`
    - _Requirements: 1.3, 6.1, 6.2_

  - [x] 4.4 Apply `TimerOp`s to the independent timer bucket
    - On `TimerOp::Upsert(timer_state)`: insert/update `timer_bucket[(run_key, timer_state.timer_id.clone())]`
    - On `TimerOp::Delete { timer_id }`: remove from `timer_bucket[(run_key, timer_id)]`
    - _Requirements: 6.3, 6.4_

  - [x] 4.5 Apply `DispatchOp::EnqueueActivityTask` to activity dispatch tracking
    - On `DispatchOp::EnqueueActivityTask`: insert into `activity_dispatch[(run_key, activity_id)]` with all fields
    - Note: do NOT insert into dispatch_backlog — backlog is Tier C, managed by the broker via `persist_to_backlog`
    - _Requirements: 1.1, 1.2_

- [x] 5. Update `list_due_timers` to use `timer_bucket`
  - [x] 5.1 Rewrite `list_due_timers` to scan `store.timer_bucket` instead of iterating `WorkflowState.timers`
    - Iterate `timer_bucket.values()`, filter by `fire_at <= now`, collect up to `limit` `DueTimer` entries
    - _Requirements: 6.5_

- [x] 6. Implement new `RunRepository` trait methods on `InMemoryStore`
  - [x] 6.1 Implement `list_dispatchable_activity_tasks`
    - Filter `activity_dispatch.values()` by matching `queue`, return up to `limit` entries as `Vec<DispatchableActivityTask>`
    - _Requirements: 2.1, 2.2, 2.4_

  - [x] 6.2 Implement `persist_to_backlog`
    - Insert each entry into `dispatch_backlog`, assigning monotonically increasing `insertion_seq` from `backlog_next_seq`
    - _Requirements: 3.1, 3.5_

  - [x] 6.3 Implement `drain_backlog`
    - Scan `dispatch_backlog`, collect and remove entries matching the provided `QueueKey` up to `limit`, return in insertion-sequence order
    - _Requirements: 3.2, 3.3, 7.2, 7.3, 7.4_

- [x] 7. Checkpoint
  - Ensure `cargo check -p tokeira-storage` passes and all existing tests still pass with `cargo test -p tokeira-storage`. Ask the user if questions arise.

- [x] 8. Add `proptest` dev-dependency and test generators
  - [x] 8.1 Add `proptest` to `Cargo.toml` dev-dependencies
    - Add `[dev-dependencies]` section with `proptest = "1"` (or workspace version if available)
    - _Requirements: design testing strategy_

  - [x] 8.2 Create proptest strategy generators in the test module of `memory.rs`
    - `arb_run_key()` — random `RunKey`
    - `arb_namespace_id()` — random `NamespaceId`
    - `arb_queue_key()` — random `QueueKey` with random namespace, queue name, and task kind
    - `arb_activity_state()` — random `ActivityState` with random timeouts
    - `arb_timer_state()` — random `TimerState` with random fire times
    - `arb_transition(expected_seq)` — random `Transition` with configurable ops
    - `arb_enqueue_activity_task()` — random `DispatchOp::EnqueueActivityTask`
    - `arb_enqueue_workflow_task()` — random `DispatchOp::EnqueueWorkflowTask`
    - _Requirements: design testing strategy_

- [x] 9. Property-based tests for activity dispatch and backlog
  - [x] 9.1 Write property test for activity dispatch round-trip fidelity
    - **Property 1: Activity dispatch round-trip fidelity**
    - **Validates: Requirements 1.1, 1.2**

  - [x] 9.2 Write property test for activity dispatch cleanup on delete
    - **Property 2: Activity dispatch cleanup on delete**
    - **Validates: Requirements 1.3**

  - [x] 9.3 Write property test for failed commits leaving all structures unchanged
    - **Property 3: Failed commits leave all structures unchanged**
    - **Validates: Requirements 1.4, 6.6**

  - [x] 9.4 Write property test for activity task sweep returning matching tasks up to limit
    - **Property 4: Activity task sweep returns matching tasks up to limit**
    - **Validates: Requirements 2.2**

  - [x] 9.5 Write property test for backlog insertion via persist_to_backlog
    - **Property 5: Backlog insertion via persist_to_backlog**
    - **Validates: Requirements 3.1, 3.5**

  - [x] 9.6 Write property test for drain backlog returning matching entries in insertion order
    - **Property 6: Drain backlog returns matching entries in insertion order**
    - **Validates: Requirements 3.3, 7.4**

- [x] 10. Property-based tests for conflict injection and policies
  - [x] 10.1 Write property test for conflict injection lifecycle
    - **Property 7: Conflict injection lifecycle**
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

  - [x] 10.2 Write property test for Reject and AllowAfterClose policies blocking when open execution exists
    - **Property 8: Reject and AllowAfterClose policies block when open execution exists**
    - **Validates: Requirements 5.3, 5.5**

  - [x] 10.3 Write property test for AllowAfterClose permitting creation after close
    - **Property 9: AllowAfterClose permits creation after close**
    - **Validates: Requirements 5.4**

- [x] 11. Property-based tests for independent state structures
  - [x] 11.1 Write property test for independent activity and timer state upsert/delete
    - **Property 10: Independent activity and timer state upsert/delete**
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**

  - [x] 11.2 Write property test for independent structures mirroring WorkflowState maps
    - **Property 11: Independent structures mirror WorkflowState maps**
    - **Validates: Requirements 6.7, 6.8**

  - [x] 11.3 Write property test for backlog size invariant
    - **Property 12: Backlog size invariant**
    - **Validates: Requirements 7.1, 7.2**

- [x] 12. Unit tests for edge cases
  - [x] 12.1 Write unit tests for edge cases and specific examples
    - Default policy is `Reject` (Req 5.6)
    - Empty queue sweep returns empty vec (Req 2.4)
    - Empty drain returns empty vec without modifying backlog (Req 7.3)
    - `inject_conflict` called twice replaces the count (Req 4.4)
    - Backlog insertion ordering within a single `persist_to_backlog` call matches input order
    - `commit_transition` does NOT write to dispatch backlog (verify backlog empty after commit with enqueue ops)
    - _Requirements: 2.4, 4.4, 5.6, 7.3_

- [x] 13. Final checkpoint
  - Ensure all tests pass with `cargo test -p tokeira-storage`. Ask the user if questions arise.

## Notes

- All changes are scoped to `tokeira-storage` crate: `api.rs`, `memory.rs`, and `Cargo.toml`
- The design uses Rust throughout; no language selection needed
- Property tests use `proptest` crate with minimum 100 iterations per property
- Each property test references a specific correctness property from the design document
- Checkpoints at tasks 3, 7, and 13 ensure incremental validation
- **Deferred from this spec**: `Reuse` policy (needs distinct `CommitResult` variant or higher-level API), `TerminateThenStart` policy (needs kernel-driven termination transition per 010-history-as-authority)
