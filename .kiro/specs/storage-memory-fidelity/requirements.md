# Requirements Document

## Introduction

The `InMemoryStore` in `tokeira-storage::memory` serves as the primary storage backend for kernel tests, edge integration tests, local development, and Codex-driven feature work. While it already implements OCC fencing, request dedup, history append, projection log, lease management, and workflow task dispatch tracking, several gaps remain between what the in-memory store tracks and what a real DSQL backend would do.

This feature closes those gaps by adding activity task dispatch tracking and sweep, an explicit-persist dispatch backlog (matching the broker's Tier C model), OCC conflict injection for test harnesses, configurable current-execution conflict policies, and faithful application of activity/timer ops to independent tracking structures. The goal is to make the dev store a high-fidelity stand-in so that runtime and broker code can be developed and tested without a DSQL cluster.

## Glossary

- **InMemoryStore**: The `tokeira_storage::memory::InMemoryStore` struct that implements `RunRepository`, `ProjectionLog`, `LeaseRepository`, and `ConnectionDirector` using in-memory data structures protected by `tokio::sync::Mutex`.
- **RunRepository**: The storage trait in `api.rs` that defines the query and commit surface for workflow run state.
- **Transition**: The `tokeira_kernel::Transition` struct representing one fenced commit containing next state, history events, activity ops, timer ops, dispatch ops, projection ops, and request dedupe ops.
- **CommitResult**: The enum (`Applied`, `Conflict`, `Duplicate`) returned by `commit_transition` to classify the outcome of a fenced commit.
- **DispatchOp**: The enum in `transition.rs` describing side-effect dispatch instructions produced by the kernel, including `EnqueueWorkflowTask`, `EnqueueActivityTask`, `StartChildWorkflow`, and others.
- **ActivityOp**: The enum (`Upsert(ActivityState)`, `Delete { activity_id }`) describing mutations to the normalized activity state table.
- **TimerOp**: The enum (`Upsert(TimerState)`, `Delete { timer_id }`) describing mutations to the timer bucket table.
- **DispatchBacklog**: A durable fallback structure for unmatched workflow and activity tasks, modeled after the `dispatch_backlog` table in the DSQL schema. Per the delivery broker architecture (040-delivery-broker), backlog is Tier C — entries are only persisted when the broker explicitly decides to (after the live-ready grace window, under pressure, or on shard unload), not automatically on every enqueue.
- **OCC_Conflict_Injection**: A test-only mechanism on InMemoryStore that allows callers to force `CommitResult::Conflict` on the next N commits for a given run, enabling retry/conflict path testing.
- **CurrentExecutionConflictPolicy**: An enum describing how `commit_transition` handles `start_workflow` when a current execution already exists for the same `(namespace_id, workflow_id)`. This spec covers `Reject` and `AllowAfterClose` only. `Reuse` and `TerminateThenStart` are deferred (see Deferred Policies below).
- **QueueKey**: The `tokeira_types::QueueKey` struct identifying a task queue by namespace, queue name, task kind, and optional deployment/build metadata.
- **ActivityState**: The `tokeira_kernel::state::ActivityState` struct representing the durable state of a scheduled activity.
- **TimerState**: The `tokeira_kernel::state::TimerState` struct representing the durable state of a scheduled timer.
- **WorkflowState**: The `tokeira_kernel::state::WorkflowState` struct representing the summary durable state of a workflow run.
- **RunKey**: A UUID-typed key uniquely identifying a workflow run in storage.
- **TransitionSeq**: A monotonically increasing sequence number used for OCC fencing on each run.

## Requirements

### Requirement 1: Activity Task Dispatch Tracking

**User Story:** As a runtime developer, I want the InMemoryStore to track enqueued activity tasks from `DispatchOp::EnqueueActivityTask`, so that the broker can sweep for dispatchable activity work the same way it sweeps for workflow tasks.

#### Acceptance Criteria

1. WHEN `commit_transition` processes a `Transition` containing one or more `DispatchOp::EnqueueActivityTask` entries, THE InMemoryStore SHALL insert each activity task into an internal activity dispatch tracking structure keyed by `(run_key, activity_id)`.
2. THE InMemoryStore SHALL store the `queue`, `activity_id`, `schedule_event_id`, `attempt`, and all timeout fields from each `EnqueueActivityTask` dispatch op in the activity dispatch tracking structure.
3. WHEN `commit_transition` processes a `Transition` containing an `ActivityOp::Delete` for a given `activity_id`, THE InMemoryStore SHALL remove the corresponding entry from the activity dispatch tracking structure for that `(run_key, activity_id)`.
4. WHEN `commit_transition` returns `CommitResult::Conflict` or `CommitResult::Duplicate`, THE InMemoryStore SHALL leave the activity dispatch tracking structure unchanged for that transition.

### Requirement 2: Activity Task Sweep Query

**User Story:** As a runtime developer, I want a `list_dispatchable_activity_tasks` method on `RunRepository`, so that the broker can discover pending activity tasks for a given queue.

#### Acceptance Criteria

1. THE RunRepository trait SHALL expose a `list_dispatchable_activity_tasks` method that accepts a `QueueKey` reference and a `limit: usize` parameter and returns a `Result<Vec<DispatchableActivityTask>>`.
2. WHEN `list_dispatchable_activity_tasks` is called, THE InMemoryStore SHALL return activity tasks from the activity dispatch tracking structure whose `queue` matches the provided `QueueKey`, up to the specified `limit`.
3. THE `DispatchableActivityTask` struct SHALL contain `run_key: RunKey`, `queue: QueueKey`, `activity_id: String`, `schedule_event_id: i64`, and `attempt: u32`.
4. WHEN the activity dispatch tracking structure contains no matching tasks for the given `QueueKey`, THE InMemoryStore SHALL return an empty `Vec`.

### Requirement 3: Explicit-Persist Dispatch Backlog

**User Story:** As a runtime developer, I want the InMemoryStore to support an explicit-persist dispatch backlog that the broker can write to when it decides a task should be durably backed, so that the dev store faithfully models the broker's Tier C backlog semantics rather than automatically persisting every enqueue op.

#### Acceptance Criteria

1. THE RunRepository trait SHALL expose a `persist_to_backlog` method that accepts a `Vec<BacklogEntry>` and inserts them into the dispatch backlog structure, assigning each a monotonically increasing insertion sequence.
2. THE RunRepository trait SHALL expose a `drain_backlog` method that accepts a `QueueKey` reference and a `limit: usize` parameter and returns a `Result<Vec<BacklogEntry>>`, removing returned entries from the backlog.
3. WHEN `drain_backlog` is called, THE InMemoryStore SHALL return and remove up to `limit` backlog entries matching the provided `QueueKey`, ordered by insertion time.
4. `commit_transition` SHALL NOT automatically insert entries into the dispatch backlog. The broker is responsible for deciding when to persist tasks to backlog via `persist_to_backlog`.
5. THE `BacklogEntry` struct SHALL contain `run_key: RunKey`, `queue: QueueKey`, `kind: BacklogTaskKind`, and `insertion_seq: u64`.

### Requirement 4: OCC Conflict Injection for Tests

**User Story:** As a test author, I want to inject artificial OCC conflicts into the InMemoryStore, so that I can exercise retry and conflict-handling paths in the runtime without relying on real concurrency races.

#### Acceptance Criteria

1. THE InMemoryStore SHALL expose a `inject_conflict` method that accepts a `run_key: RunKey` and a `count: usize` parameter, causing the next `count` calls to `commit_transition` for that `run_key` to return `CommitResult::Conflict` with a reason string indicating the conflict was injected.
2. WHEN `inject_conflict` has been called for a `run_key` and the remaining injection count is greater than zero, THE InMemoryStore SHALL return `CommitResult::Conflict` from `commit_transition` for that `run_key` and decrement the injection count by one, without modifying any stored state.
3. WHEN the injection count for a `run_key` reaches zero, THE InMemoryStore SHALL resume normal `commit_transition` behavior for that `run_key`.
4. WHEN `inject_conflict` is called multiple times for the same `run_key`, THE InMemoryStore SHALL replace the previous injection count with the new value.

### Requirement 5: Current-Execution Conflict Policies

**User Story:** As a runtime developer, I want the InMemoryStore to support configurable current-execution conflict policies, so that `start_workflow` behavior can vary between reject and allow-after-close semantics.

#### Acceptance Criteria

1. THE InMemoryStore SHALL support a `CurrentExecutionConflictPolicy` enum with variants: `Reject` and `AllowAfterClose`.
2. THE InMemoryStore SHALL expose a `set_conflict_policy` method that accepts a `CurrentExecutionConflictPolicy` value and applies it to all subsequent `commit_transition` calls that create new workflow executions.
3. WHILE the policy is set to `Reject`, WHEN `commit_transition` creates a new execution and an open execution already exists for the same `(namespace_id, workflow_id)`, THE InMemoryStore SHALL return `CommitResult::Conflict` with a reason indicating a current execution already exists.
4. WHILE the policy is set to `AllowAfterClose`, WHEN `commit_transition` creates a new execution and a closed execution exists for the same `(namespace_id, workflow_id)` with no open execution, THE InMemoryStore SHALL proceed with normal execution creation.
5. WHILE the policy is set to `AllowAfterClose`, WHEN `commit_transition` creates a new execution and an open execution already exists for the same `(namespace_id, workflow_id)`, THE InMemoryStore SHALL return `CommitResult::Conflict`.
6. THE InMemoryStore SHALL default to the `Reject` policy, preserving the current behavior when no policy is explicitly set.

#### Deferred Policies

The following policies are intentionally deferred from this spec:

- **Reuse**: Returning `CommitResult::Applied` with an existing run's state when the caller's transition was not persisted would overload the `Applied` variant and lose critical semantics for callers. This needs either a distinct `CommitResult` variant or a higher-level `start_workflow` method that checks before calling `commit_transition`. Deferred until the runtime's start-workflow path is designed.
- **TerminateThenStart**: Having storage silently terminate an existing execution violates the project's authoritative-transition model (010-history-as-authority). No state visible to the rest of the system may exist unless it can be explained by a committed history transition. Termination needs a real kernel-driven transition path with history events, projection close ops, and a transition audit record. Deferred until the kernel has a proper terminate-and-replace command.

### Requirement 6: Faithful Activity and Timer State Tracking

**User Story:** As a runtime developer, I want the InMemoryStore to maintain independent activity state and timer bucket tracking structures that mirror the DSQL `activity_state` and `timer_bucket` tables, so that sweep queries and state inspection operate on normalized data rather than only on the embedded `WorkflowState` maps.

#### Acceptance Criteria

1. WHEN `commit_transition` processes an `ActivityOp::Upsert(activity_state)`, THE InMemoryStore SHALL insert or update the activity in an independent activity state tracking structure keyed by `(run_key, activity_id)`.
2. WHEN `commit_transition` processes an `ActivityOp::Delete { activity_id }`, THE InMemoryStore SHALL remove the entry from the independent activity state tracking structure for that `(run_key, activity_id)`.
3. WHEN `commit_transition` processes a `TimerOp::Upsert(timer_state)`, THE InMemoryStore SHALL insert or update the timer in an independent timer bucket tracking structure keyed by `(run_key, timer_id)`.
4. WHEN `commit_transition` processes a `TimerOp::Delete { timer_id }`, THE InMemoryStore SHALL remove the entry from the independent timer bucket tracking structure for that `(run_key, timer_id)`.
5. THE `list_due_timers` method SHALL query the independent timer bucket tracking structure instead of iterating over `WorkflowState.timers` maps.
6. WHEN `commit_transition` returns `CommitResult::Conflict` or `CommitResult::Duplicate`, THE InMemoryStore SHALL leave the independent activity state and timer bucket tracking structures unchanged for that transition.
7. FOR ALL committed transitions, the independent activity state tracking structure SHALL contain the same entries as the union of all `WorkflowState.activities` maps across stored runs (round-trip consistency).
8. FOR ALL committed transitions, the independent timer bucket tracking structure SHALL contain the same entries as the union of all `WorkflowState.timers` maps across stored runs (round-trip consistency).

### Requirement 7: Dispatch Backlog Consistency Invariants

**User Story:** As a test author, I want the dispatch backlog to maintain consistency invariants that mirror the DSQL storage contract, so that tests exercising the broker sweep logic are reliable.

#### Acceptance Criteria

1. FOR ALL calls to `persist_to_backlog` that insert N entries, THE InMemoryStore SHALL increase the dispatch backlog size by exactly N.
2. FOR ALL calls to `drain_backlog` that return N entries, THE InMemoryStore SHALL decrease the dispatch backlog size by exactly N.
3. WHEN `drain_backlog` is called with a `QueueKey` that has no matching backlog entries, THE InMemoryStore SHALL return an empty `Vec` without modifying the backlog.
4. THE InMemoryStore SHALL assign each backlog entry a monotonically increasing insertion sequence, and `drain_backlog` SHALL return entries in insertion-sequence order.
