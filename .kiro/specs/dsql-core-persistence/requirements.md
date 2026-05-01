# Requirements Document

## Introduction

This spec covers Feature 2 (Core Persistence — RunRepository on DSQL) from the umbrella `dsql-storage-implementation` spec. It implements the `RunRepository` trait methods that operate on the core tables: `workflow_hot`, `history_batch`, `current_execution`, and `request_dedupe`.

The scope is:

1. **DsqlRunRepository** — a new struct implementing `RunRepository` against Aurora DSQL, using the `DsqlConnectionDirector` and codec module from Feature 1 (`dsql-schema-connection`).
2. **Fenced commit transaction** — `commit_transition` as a single DSQL transaction with shard epoch fencing and transition-seq OCC.
3. **Read operations** — `load_run`, `resolve_execution`, `find_latest_run`, `read_history`, `lookup_request_dedupe`.
4. **Reset materialization** — `materialize_reset_successor` for workflow reset fork.
5. **OCC conflict classification** — surfacing retryable vs. validation conflicts to the runtime without internal retry.
6. **Request deduplication** — duplicate detection within the commit transaction via `request_dedupe` table.

The authoritative architecture documents are [050-dsql-storage](../../../docs/architecture/050-dsql-storage.md) and the Feature 1 design at [dsql-schema-connection/design.md](../dsql-schema-connection/design.md). The in-memory implementation in `tokeira-storage/src/memory.rs` is the behavioral reference.

### What This Spec Covers

The `RunRepository` trait methods assigned to Feature 2 in the umbrella spec audit table:

| Method | Description |
|---|---|
| `commit_transition` | Primary write path — one fenced DSQL transaction |
| `load_run` | Read `workflow_hot` and deserialize `WorkflowState` |
| `resolve_execution` | `current_execution` lookup by namespace + workflow_id |
| `find_latest_run` | `current_execution` lookup (open or closed) |
| `read_history` | Paginated reads from `history_batch` |
| `lookup_request_dedupe` | Idempotency check against `request_dedupe` |
| `read_transition_audit` | Debug/test audit log (may be test-only for DSQL) |
| `materialize_reset_successor` | Reset fork materialization |

### What This Spec Does NOT Cover

- Activity state queries — `list_dispatchable_activity_tasks`, `list_open_activities_for_shard`, etc. (Feature 3: `dsql-side-tables`)
- Timer bucket queries — `list_due_timers`, `list_due_timers_for_shard` (Feature 3: `dsql-side-tables`)
- Shard lease management — `try_acquire_bundle`, `renew_bundle` (Feature 4: `dsql-shard-leasing`)
- Dispatch backlog persistence — `persist_to_backlog`, `drain_backlog` (Feature 5: `dsql-dispatch-backlog`)
- Projection persistence — `ProjectionLog::read_from`, projector checkpoints (Feature 6: `dsql-projection-persistence`)
- Queue-filtered dispatchable queries — `list_dispatchable_workflow_tasks` (Feature 3: `dsql-side-tables`)
- Shard-filtered sweep queries (Feature 3: `dsql-side-tables`)

### Dependencies

- Feature 1 (`dsql-schema-connection`) provides: `DsqlStore`, `DsqlConnectionDirector`, `DsqlPermit`, `DsqlConnector`, codec module (`encode`/`decode` helpers), and the schema DDL for all tables used here.

### Key DSQL Constraints Shaping This Design

- **OCC with Repeatable Read** — conflicts detected at commit time, not during reads. The storage layer classifies outcomes; the runtime decides whether to retry.
- **3,000-row mutation limit** — the commit transaction write set must stay bounded.
- **5-minute max transaction time** — transactions must be narrow and fast.
- **No temp tables** — CTE-based query compilation where staging is needed.
- **Postcard serialization** — all BYTEA columns use the codec module from Feature 1.

## Glossary

- **DsqlRunRepository**: The struct implementing `RunRepository` against Aurora DSQL, using `DsqlConnectionDirector` for connection management and the codec module for serialization.
- **RunRepository**: The primary storage trait in `tokeira-storage/src/api.rs` defining methods for durable run persistence.
- **DsqlStore**: The production DSQL storage foundation from Feature 1, providing `DsqlConnectionDirector` and `MigrationRunner`.
- **DsqlConnectionDirector**: The connection director from Feature 1 implementing class-based connection budget control with reservoir pattern.
- **DsqlPermit**: A held connection permit carrying a real `PoolConnection<Postgres>`, scoped to a `DbClass`.
- **Transition**: The bounded, explicit description of what must be committed as a result of one kernel `apply` call. Contains `next_state`, `history_events`, `activity_ops`, `timer_ops`, `dispatch_ops`, `projection_ops`, and `request_dedupe_ops`.
- **TransitionSeq**: Monotonic fence/checkpoint number for committed state transitions, incremented exactly once per transition. Used as the OCC fence in `commit_transition`.
- **ShardEpoch**: Monotonically increasing epoch number for shard ownership fencing. A stale epoch causes commit rejection.
- **RunKey**: UUID-based durable storage key for a workflow run, used as primary key in `workflow_hot`.
- **CommitResult**: Enum with variants `Applied`, `Conflict`, and `Duplicate` — the return type of `commit_transition`.
- **LoadedRun**: Enum with variants `Existing(WorkflowState)` and `Absent` — the return type of `load_run`.
- **WorkflowState**: The full current state of a workflow run, serialized to BYTEA in `workflow_hot.state_data` using postcard.
- **OCC**: Optimistic Concurrency Control — DSQL's conflict detection model where transactions proceed optimistically and conflicts are detected at commit time.
- **Workflow_Hot**: Small current summary row per open run, containing the compact `WorkflowState` needed for the runtime to process the next command.
- **History_Batch**: Immutable append-only event batch table. Each row contains a contiguous range of history events from one transition.
- **Current_Execution**: Mapping table from `(namespace_id, workflow_id)` to the current run identity and open/closed status.
- **Request_Dedupe**: Idempotency record table for external command deduplication, keyed by `(namespace_id, workflow_id, request_id)`.
- **Shard_Lease**: Table tracking shard ownership with epoch fencing for single-writer guarantees. Read (not written) by this spec during commit fencing.
- **CurrentExecutionConflictPolicy**: Enum controlling behavior when a Start command collides with an existing execution — `Reject` or `AllowAfterClose`.
- **Codec**: The `tokeira-storage/src/dsql/codec.rs` module providing postcard-based `encode`/`decode` helpers for all BYTEA column types.
- **DbClass**: Enumerated workload class (Control, Commit, Read, Projection, Maintenance) used for connection budget prioritization.

## Requirements

### Requirement 1: DsqlRunRepository Struct

**User Story:** As a Tokeira developer, I want a `DsqlRunRepository` struct that implements `RunRepository` using the DSQL connection infrastructure from Feature 1, so that the runtime can use DSQL for durable persistence without changes to the runtime layer.

#### Acceptance Criteria

1. THE DsqlRunRepository SHALL implement the `RunRepository` trait from `tokeira-storage/src/api.rs`.
2. THE DsqlRunRepository SHALL accept a `DsqlConnectionDirector` reference for acquiring database connections with class-based permits.
3. THE DsqlRunRepository SHALL use `DbClass::Commit` when acquiring connections for `commit_transition` and `materialize_reset_successor`.
4. THE DsqlRunRepository SHALL use `DbClass::Read` when acquiring connections for `load_run`, `resolve_execution`, `find_latest_run`, `read_history`, `lookup_request_dedupe`, and `read_transition_audit`.
5. THE DsqlRunRepository SHALL use the codec module from Feature 1 for all postcard serialization and deserialization of BYTEA column values.

### Requirement 2: Fenced Commit Transaction

**User Story:** As a Tokeira developer, I want `commit_transition` to execute as a single fenced DSQL transaction, so that one workflow transition is atomically persisted with OCC conflict detection and shard epoch fencing.

#### Acceptance Criteria

1. WHEN `commit_transition` is called, THE DsqlRunRepository SHALL execute all writes for the transition within a single DSQL transaction.
2. THE transaction SHALL validate the caller's `TransitionSeq` against the durable `transition_seq` value in `workflow_hot` and return `CommitResult::Conflict` on mismatch.
3. THE transaction SHALL validate the caller's `ShardEpoch` against the durable shard lease epoch in `shard_lease` and return `CommitResult::Conflict` if the epoch is stale.
4. WHEN the epoch is `ShardEpoch::ZERO`, THE DsqlRunRepository SHALL skip the shard epoch fence check to support test and bootstrap scenarios.
5. WHEN the transaction succeeds, THE DsqlRunRepository SHALL return `CommitResult::Applied` with the new authoritative `WorkflowState`.
6. WHEN a DSQL OCC conflict is detected at commit time (serialization failure), THE DsqlRunRepository SHALL return `CommitResult::Conflict` with a reason describing the OCC conflict.
7. THE DsqlRunRepository SHALL NOT silently retry OCC conflicts internally; conflict classification is returned to the runtime for decision.

### Requirement 3: Commit Transaction Write Set

**User Story:** As a Tokeira developer, I want `commit_transition` to persist all transition components atomically, so that history, state, side effects, and derived data are never partially visible.

#### Acceptance Criteria

1. WHEN `commit_transition` succeeds, THE DsqlRunRepository SHALL have upserted the `workflow_hot` row with the new `WorkflowState` serialized via the codec module, updating `transition_seq`, `state_data`, `shard_id`, and `updated_at`.
2. WHEN `commit_transition` succeeds and the transition contains history events, THE DsqlRunRepository SHALL have inserted a `history_batch` row containing the serialized events with `first_event_id`, `last_event_id`, and `transition_seq`.
3. WHEN `commit_transition` succeeds and the transition contains `ActivityOp::Upsert` entries, THE DsqlRunRepository SHALL have upserted corresponding rows in `activity_state`.
4. WHEN `commit_transition` succeeds and the transition contains `ActivityOp::Delete` entries, THE DsqlRunRepository SHALL have deleted corresponding rows from `activity_state`.
5. WHEN `commit_transition` succeeds and the transition contains `TimerOp::Upsert` entries, THE DsqlRunRepository SHALL have upserted corresponding rows in `timer_bucket`.
6. WHEN `commit_transition` succeeds and the transition contains `TimerOp::Delete` entries, THE DsqlRunRepository SHALL have deleted corresponding rows from `timer_bucket`.
7. WHEN `commit_transition` succeeds and the transition contains `RequestDedupeOp` entries, THE DsqlRunRepository SHALL have inserted records into `request_dedupe`.
8. WHEN `commit_transition` succeeds and the transition contains `ProjectionOp` entries, THE DsqlRunRepository SHALL have appended records to `projection_log` within the same transaction.
9. THE transaction write set SHALL remain within DSQL's 3,000-row mutation limit for any single transition.
10. THE transaction SHALL complete within DSQL's 5-minute maximum transaction time.

### Requirement 4: Start Workflow with Conflict Policy

**User Story:** As a Tokeira developer, I want `commit_transition` for a Start command to respect the current-execution conflict policy, so that workflow-id reuse semantics are enforced at the storage level.

#### Acceptance Criteria

1. WHEN a Start transition is committed (expected_seq is zero and status is open) and no `current_execution` row exists for `(namespace_id, workflow_id)`, THE DsqlRunRepository SHALL insert the new mapping with `is_open = true`.
2. WHEN a Start transition is committed and a `current_execution` row exists with `is_open = true` under the Reject policy, THE DsqlRunRepository SHALL return `CommitResult::Conflict`.
3. WHEN a Start transition is committed and a `current_execution` row exists with `is_open = false` under the AllowAfterClose policy, THE DsqlRunRepository SHALL replace the mapping with the new run identity and `is_open = true`.
4. WHEN a Start transition is committed and a `current_execution` row exists with `is_open = true` under the AllowAfterClose policy, THE DsqlRunRepository SHALL return `CommitResult::Conflict`.
5. WHEN a transition closes a workflow (status transitions to a terminal state), THE DsqlRunRepository SHALL update the `current_execution` row to set `is_open = false`.

### Requirement 5: Duplicate Request Detection

**User Story:** As a Tokeira developer, I want `commit_transition` to detect duplicate requests within the same transaction, so that idempotent handling is enforced at the storage level.

#### Acceptance Criteria

1. WHEN `commit_transition` inserts a `request_dedupe` record and a record with the same `(namespace_id, workflow_id, request_id)` already exists, THE DsqlRunRepository SHALL return `CommitResult::Duplicate`.
2. THE duplicate check SHALL be performed within the same transaction as the rest of the commit to prevent race conditions.

### Requirement 6: Load Run State

**User Story:** As a Tokeira developer, I want `load_run` to read the current `WorkflowState` from DSQL, so that the runtime can process the next command for a run.

#### Acceptance Criteria

1. WHEN `load_run` is called with a known `RunKey`, THE DsqlRunRepository SHALL return `LoadedRun::Existing` with the `WorkflowState` deserialized from the `workflow_hot.state_data` column using the codec module.
2. WHEN `load_run` is called with an unknown `RunKey`, THE DsqlRunRepository SHALL return `LoadedRun::Absent`.
3. FOR ALL `WorkflowState` values, serializing to `state_data` via the codec and then deserializing SHALL produce a value equal to the original (round-trip property).

### Requirement 7: Resolve Execution

**User Story:** As a Tokeira developer, I want `resolve_execution` to look up run identity from `current_execution`, so that the runtime can route commands to the correct run.

#### Acceptance Criteria

1. WHEN `resolve_execution` is called with an `ExecutionRef` that has no `run_id`, THE DsqlRunRepository SHALL query `current_execution` for `(namespace_id, workflow_id)` where `is_open = true` and return the `RunKey`, or `None` if no open run exists.
2. WHEN `resolve_execution` is called with an `ExecutionRef` that has a specific `run_id`, THE DsqlRunRepository SHALL query `workflow_hot` for rows matching `(namespace_id, workflow_id)`, deserialize each `state_data`, and return the `RunKey` of the row whose `WorkflowState.run_id` matches, regardless of open/closed status, or `None` if no matching run exists. This is necessary because `current_execution` only holds the latest run and older runs are overwritten.

### Requirement 8: Find Latest Run

**User Story:** As a Tokeira developer, I want `find_latest_run` to return the most recent run for a workflow, so that conflict resolution paths can distinguish "no run has ever existed" from "the last run is closed."

#### Acceptance Criteria

1. WHEN `find_latest_run` is called, THE DsqlRunRepository SHALL query `current_execution` for `(namespace_id, workflow_id)` and return the `RunKey` of the most recent run, whether open or closed, or `None` if no run has ever existed.

### Requirement 9: Read History

**User Story:** As a Tokeira developer, I want `read_history` to return paginated history events from DSQL, so that the runtime and edge layer can serve history to workers and API callers.

#### Acceptance Criteria

1. WHEN `read_history` is called, THE DsqlRunRepository SHALL query `history_batch` rows for the given `RunKey` where the batch contains events with `event_id > after_event_id`, ordered by `first_event_id` ascending.
2. THE DsqlRunRepository SHALL deserialize the `events_data` column using the codec module and reconstruct individual `HistoryEvent` values from the batch storage format.
3. THE DsqlRunRepository SHALL return at most `limit` events, filtering out events with `event_id <= after_event_id` from partially overlapping batches.
4. WHEN no events exist after `after_event_id`, THE DsqlRunRepository SHALL return an empty vector.
5. FOR ALL `Vec<HistoryEvent>` values, serializing to `events_data` via the codec and then deserializing SHALL produce a value equal to the original (round-trip property).

### Requirement 10: Request Deduplication Lookup

**User Story:** As a Tokeira developer, I want `lookup_request_dedupe` to check for previously committed requests, so that the runtime can short-circuit duplicate external commands.

#### Acceptance Criteria

1. WHEN `lookup_request_dedupe` is called for a request that was previously committed, THE DsqlRunRepository SHALL return the `RequestRecord` with the original run identity and transition sequence.
2. WHEN `lookup_request_dedupe` is called with an `ExecutionRef` that has a specific `run_id`, THE DsqlRunRepository SHALL only return the record if the stored `run_id` matches.
3. WHEN `lookup_request_dedupe` is called for an unknown request, THE DsqlRunRepository SHALL return `None`.

### Requirement 11: Materialize Reset Successor

**User Story:** As a Tokeira developer, I want `materialize_reset_successor` to create a new run by copying a prefix of the base run's history, so that workflow reset can fork execution from a prior point.

#### Acceptance Criteria

1. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL read history events from the base run's `history_batch` rows through `fork_event_id`.
2. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL insert the copied history prefix into the successor run's `history_batch`.
3. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL derive the successor's `WorkflowState` by replaying the copied history prefix using the kernel's `replay_history_prefix`.
4. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL insert a `workflow_hot` row for the successor run with the derived state.
5. WHEN `materialize_reset_successor` is called, THE DsqlRunRepository SHALL insert a `current_execution` row for the successor run.
6. IF `fork_event_id` is beyond the base run's committed history, THEN THE DsqlRunRepository SHALL return an error.
7. THE DsqlRunRepository SHALL reconstruct activity state and timer state from the replayed successor state and persist them to `activity_state` and `timer_bucket` respectively.

### Requirement 12: OCC Conflict Classification

**User Story:** As a Tokeira developer, I want the DSQL storage layer to classify OCC conflicts into actionable categories, so that the runtime can decide whether to retry, reload, or reject.

#### Acceptance Criteria

1. WHEN a DSQL transaction fails with a serialization conflict (SQLSTATE 40001), THE DsqlRunRepository SHALL classify the outcome as a retryable conflict and return `CommitResult::Conflict`.
2. WHEN a transition-seq fence check fails (expected_seq does not match durable value), THE DsqlRunRepository SHALL classify the outcome as a validation conflict and return `CommitResult::Conflict` with a reason indicating the sequence mismatch.
3. WHEN a shard epoch fence check fails (caller epoch does not match durable epoch), THE DsqlRunRepository SHALL classify the outcome as a validation conflict and return `CommitResult::Conflict` with a reason indicating the epoch mismatch.
4. THE DsqlRunRepository SHALL NOT silently retry any conflict internally; all conflict classification is returned to the runtime for decision.

### Requirement 13: Shard Epoch Fencing

**User Story:** As a Tokeira developer, I want every `commit_transition` to validate the shard epoch within the transaction, so that a stale owner cannot commit after failover.

#### Acceptance Criteria

1. WHEN `commit_transition` is called with a non-zero `ShardEpoch`, THE DsqlRunRepository SHALL read the current epoch from `shard_lease` for the run's shard within the same transaction.
2. IF the caller's epoch does not match the durable shard epoch, THEN THE DsqlRunRepository SHALL abort the transaction and return `CommitResult::Conflict` with a reason indicating epoch mismatch.
3. IF no lease row exists for the run's shard, THEN THE DsqlRunRepository SHALL abort the transaction and return `CommitResult::Conflict` with a reason indicating no active lease.
4. THE epoch check SHALL be performed within the same transaction as the state mutation to prevent TOCTOU races.
5. THE DsqlRunRepository SHALL derive the shard assignment from `RunKey` using the same deterministic mapping as the runtime and in-memory store.

### Requirement 14: Transition Audit Log

**User Story:** As a Tokeira developer, I want `read_transition_audit` to return a debug view of persisted transitions, so that semantic tests can verify that history and derived ops are all persisted together.

#### Acceptance Criteria

1. THE DsqlRunRepository SHALL support `read_transition_audit` by reconstructing `TransitionAuditRecord` values from the persisted `history_batch` data for a given `RunKey`. Each `history_batch` row maps to one `TransitionAuditRecord` with `history_events` populated from the deserialized batch. The `activity_ops`, `timer_ops`, `dispatch_ops`, and `projection_ops` fields SHALL be empty vectors because the side tables hold current materialized state, not per-transition ops — historical ops are not recoverable from the DSQL schema. This is sufficient for the primary use case (verifying history persistence in tests).
2. WHEN no transitions have been committed for a `RunKey`, THE DsqlRunRepository SHALL return an empty vector.

### Requirement 15: Workflow Close Updates Current Execution

**User Story:** As a Tokeira developer, I want `commit_transition` to update `current_execution` when a workflow closes, so that `resolve_execution` correctly reports no open run after termination.

#### Acceptance Criteria

1. WHEN `commit_transition` succeeds and the transition's `next_state` has a terminal execution status, THE DsqlRunRepository SHALL update the `current_execution` row to set `is_open = false`.
2. WHEN `commit_transition` succeeds and the transition is a non-start intermediate transition (expected_seq is non-zero) with an open execution status, THE DsqlRunRepository SHALL NOT modify the `current_execution` row. Start transitions with an open `next_state` are governed by Requirement 4, which requires inserting or replacing `current_execution` with `is_open = true`.
