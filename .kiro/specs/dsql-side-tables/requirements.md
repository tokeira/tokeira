# Requirements Document

## Introduction

This spec covers Feature 3 (Side Tables — Activity, Timer, Nexus State) from the umbrella `dsql-storage-implementation` spec. It implements the `RunRepository` trait methods that query the three side tables (`activity_state`, `timer_bucket`, `workflow_hot`) for dispatch and sweep operations in `DsqlRunRepository`.

These 10 methods are currently `bail!("Feature 3: dsql-side-tables")` stubs. This spec adds the query implementations, a new `activity_dispatch` DSQL table for durable activity dispatch state, write-path changes to maintain that table from `DispatchOp::EnqueueActivityTask`, and a runtime guard against duplicate activity starts.

The key insight: `activity_state` is a materialized open-activity table for timeout/sweep reconstruction — it is not a dispatch queue. Activities that are started, paused, or otherwise not ready for dispatch remain in `activity_state`. Dispatch should come from a dedicated table derived from `DispatchOp::EnqueueActivityTask`, matching the in-memory store's `activity_dispatch` HashMap.

### Scope

| Method | Table(s) | Category |
|---|---|---|
| `list_dispatchable_workflow_tasks` | `workflow_hot` | Queue-filtered dispatch |
| `list_dispatchable_activity_tasks` | `activity_dispatch` | Queue-filtered dispatch |
| `list_due_timers` | `timer_bucket` | Global timer sweep |
| `list_dispatchable_workflow_tasks_for_shard` | `workflow_hot` | Shard-filtered dispatch |
| `list_dispatchable_activity_tasks_for_shard` | `activity_dispatch` | Shard-filtered dispatch |
| `list_due_timers_for_shard` | `timer_bucket` | Shard-filtered timer sweep |
| `list_runs_with_workflow_timeouts_for_shard` | `workflow_hot` | Shard-filtered timeout sweep |
| `list_started_workflow_tasks_for_shard` | `workflow_hot` | Shard-filtered WFT timeout sweep |
| `list_open_activities_for_shard` | `activity_state` | Shard-filtered activity timeout sweep |
| `list_pending_nexus_operations_for_shard` | `workflow_hot` | Shard-filtered Nexus timeout sweep |

### What This Spec Does NOT Cover

- Core persistence methods (`commit_transition`, `load_run`, etc.) — Feature 2 (`dsql-core-persistence`)
- Shard lease management (`try_acquire_bundle`, `renew_bundle`) — Feature 4 (`dsql-shard-leasing`)
- Dispatch backlog persistence (`persist_to_backlog`, `drain_backlog`) — already implemented in `dsql-spread-keys`
- Projection persistence (`ProjectionLog::read_from`) — Feature 6 (`dsql-projection-persistence`)
- Base schema DDL and connection management — Feature 1 (`dsql-schema-connection`). This spec adds the `activity_dispatch` table DDL, one new index migration (`V024`), and write-path changes to `commit_transition` for the dispatch table lifecycle.

### Dependencies

- Feature 1 (`dsql-schema-connection`) provides: schema DDL for `activity_state`, `timer_bucket`, `workflow_hot` with their secondary indexes; `DsqlConnectionDirector` and `DsqlPermit`; codec module for postcard serialization.
- Feature 2 (`dsql-core-persistence`) provides: `DsqlRunRepository` struct, `shard_for_run_key`, `shard_id_to_uuid`, and the `commit_transition` implementation that writes to all three side tables.
- `dsql-spread-keys` provides: `RunKey::derive`, `dsql_spread_uuid`, and the revised `shard_id_to_uuid` using BLAKE3.

### Key Design Constraints

- All queries use `DbClass::Read` connections — no transactions, no writes.
- `shard_id` is bound as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
- `workflow_hot.state_data` stores the full `WorkflowState` as postcard-serialized BYTEA. The dispatch and sweep methods deserialize it to extract specific fields (`pending_workflow_task`, `workflow_execution_timeout`, `workflow_run_timeout`, `pending_nexus_operations`, etc.).
- `activity_state.state_data` stores `ActivityState` as postcard-serialized BYTEA. Used by sweep methods only (`list_open_activities_for_shard`). Activity dispatch queries read from the dedicated `activity_dispatch` table instead.
- `activity_dispatch` stores dispatch-ready activity tasks with denormalized queue identity columns. Dispatch queries do not deserialize `ActivityState`; they decode only `input_data` as postcard-encoded `Payloads`.
- `timer_bucket.timer_data` stores `TimerState` as postcard-serialized BYTEA.
- The `limit` parameter bounds the result set size for all methods.
- The in-memory store in `memory.rs` is the behavioral reference for all methods.

### Schema Reference

**activity_state** — PK: `(run_key, schedule_event_id)`, indexes: `(shard_id)`, `(queue_namespace, queue_name)`, `(run_key, activity_id)`. Used by sweep queries only (`list_open_activities_for_shard`).

**activity_dispatch** — PK: `(key)` spread UUID, indexes: `(shard_id)`, `(queue_namespace, queue_name, task_kind, deployment, build_id)`, `(run_key)`. Added by this spec. Used by dispatch queries (`list_dispatchable_activity_tasks`, `list_dispatchable_activity_tasks_for_shard`) and run-scoped dispatch cleanup on workflow pause.

**timer_bucket** — PK: `(shard_id, fire_at, run_key, timer_id)`, indexes: `(shard_id, fire_at)`, `(run_key, timer_id)`

**workflow_hot** — PK: `(run_key)`, indexes: `(shard_id)`, `(namespace_id)` (V024, added by this spec)

## Glossary

- **DsqlRunRepository**: The struct implementing `RunRepository` against Aurora DSQL, using `DsqlConnectionDirector` for connection management and the codec module for serialization.
- **RunRepository**: The primary storage trait in `tokeira-storage/src/api.rs` defining methods for durable run persistence.
- **WorkflowState**: The full current state of a workflow run, serialized to BYTEA in `workflow_hot.state_data` using postcard. Contains `pending_workflow_task`, `activities`, `timers`, `pending_nexus_operations`, timeout configuration, and lifecycle status.
- **ActivityState**: The current state of an open activity, serialized to BYTEA in `activity_state.state_data` using postcard. Contains `activity_id`, `schedule_event_id`, `attempt`, timeouts, `started_at`, `task_queue`, `deployment`, `build_id`, and scheduling metadata. Used by sweep queries only — NOT the dispatch source.
- **ActivityDispatch**: A DSQL table storing one row per currently dispatchable activity task. Derived from `DispatchOp::EnqueueActivityTask`. Rows are refreshed on re-enqueue, removed when an activity starts, pauses, resolves, or the workflow pauses, and updated in place for still-dispatchable activity metadata changes. This is the DSQL equivalent of the in-memory store's `activity_dispatch` HashMap.
- **TimerState**: The state of a pending timer, serialized to BYTEA in `timer_bucket.timer_data` using postcard. Contains `timer_id`, `started_event_id`, and `fire_at`.
- **PendingWorkflowTask**: A workflow task that has been scheduled but not yet completed. Contains `logical_seq`, `scheduled_event_id`, `started_event_id`, `started_at`, and `attempt`. A task is dispatchable when `started_event_id` is `None`.
- **PendingNexusOperation**: A Nexus operation that has been scheduled but not yet reached a terminal state. Contains `operation_id`, `scheduled_event_id`, `schedule_to_close_timeout`, and `scheduled_at`.
- **QueueKey**: Composite key identifying a dispatch queue: `namespace_id`, `task_queue`, `task_kind`, `deployment`, `build_id`.
- **ShardId**: A `u32` shard identifier. Stored as UUID in DSQL via `DsqlRunRepository::shard_id_to_uuid`.
- **DbClass**: Enumerated workload class for connection budget prioritization. All methods in this spec use `DbClass::Read`.
- **Codec**: The `tokeira-storage/src/dsql/codec.rs` module providing postcard-based `encode`/`decode` helpers for all BYTEA column types.
- **StickyAffinity**: Binding a run to a specific worker for cache-local dispatch. Contains `worker_identity` and `expires_at`.
- **DispatchableWorkflowTask**: Return type for workflow dispatch queries. Contains `run_key`, `queue`, `logical_seq`, `sticky_preferred`, `sticky_expires_at`.
- **DispatchableActivityTask**: Return type for activity dispatch queries. Contains `run_key`, `queue`, `activity_id`, `input`, `schedule_event_id`, `attempt`.
- **DueTimer**: Return type for timer sweep queries. Contains `run_key` and `timer_id`.
- **WorkflowTimeoutSweepEntry**: Return type for workflow timeout sweep. Contains `run_key`, `workflow_execution_timeout`, `workflow_run_timeout`, `started_at`, `first_run_started_at`, `has_retry_policy`.
- **WftTimeoutSweepEntry**: Return type for WFT timeout sweep. Contains `run_key`, `logical_seq`, `started_event_id`, `started_at`, `workflow_task_timeout`.
- **ActivitySweepEntry**: Return type for activity timeout sweep. Contains `run_key`, `activity_id`, `schedule_event_id`, `attempt`, `original_scheduled_at`, `started_at`, and four timeout fields.
- **NexusSweepEntry**: Return type for Nexus timeout sweep. Contains `run_key`, `operation_id`, `scheduled_event_id`, `schedule_to_close_timeout`, `scheduled_at`.

## Requirements

### Requirement 1: Queue-Filtered Dispatchable Workflow Tasks

**User Story:** As a Tokeira developer, I want `list_dispatchable_workflow_tasks` to query DSQL for workflow runs with pending but not-yet-started workflow tasks matching a given queue, so that the broker can find workflow work for specific queues.

#### Acceptance Criteria

1. WHEN `list_dispatchable_workflow_tasks` is called with a `QueueKey`, THE DsqlRunRepository SHALL query `workflow_hot` rows, deserialize each `state_data` to `WorkflowState`, and return runs where `pending_workflow_task` is present and `pending_workflow_task.started_event_id` is `None`.
2. THE DsqlRunRepository SHALL filter results to only include runs whose `namespace_id` and `task_queue` match the given `QueueKey`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `DispatchableWorkflowTask.queue` with `task_kind` set to `Workflow` and `deployment` and `build_id` set to `None`, matching the in-memory store behavior.
6. THE DsqlRunRepository SHALL populate `sticky_preferred` and `sticky_expires_at` from the deserialized `WorkflowState.sticky` field, clearing expired sticky affinities where `expires_at` is at or before the current time.

### Requirement 2: Queue-Filtered Dispatchable Activity Tasks

**User Story:** As a Tokeira developer, I want `list_dispatchable_activity_tasks` to query DSQL for dispatchable activity entries matching a given queue, so that the broker can find activity work for specific queues.

#### Acceptance Criteria

1. WHEN `list_dispatchable_activity_tasks` is called with a `QueueKey`, THE DsqlRunRepository SHALL query the `activity_dispatch` table (not `activity_state`) using the queue identity index.
2. THE DsqlRunRepository SHALL filter results to only include activities whose full queue identity (including `task_kind`, `deployment`, `build_id`) matches the given `QueueKey`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `DispatchableActivityTask` fields (`run_key`, `queue`, `activity_id`, `input`, `schedule_event_id`, `attempt`) from the `activity_dispatch` row columns.

### Requirement 3: Global Due Timer Query

**User Story:** As a Tokeira developer, I want `list_due_timers` to query DSQL for timers whose fire_at deadline has passed, so that the runtime can fire expired timers.

#### Acceptance Criteria

1. WHEN `list_due_timers` is called with a `now` timestamp, THE DsqlRunRepository SHALL collect due timers across all shards by iterating `0..shard_count` and calling the shard-filtered path for each shard, collecting results until `limit` is reached. This avoids an unindexed `fire_at` scan on `timer_bucket` whose PK is `(shard_id, fire_at, ...)`.
2. THE DsqlRunRepository SHALL return at most `limit` results.
3. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
4. THE DsqlRunRepository SHALL populate `DueTimer` with `run_key` and `timer_id` from the queried rows.

### Requirement 4: Shard-Filtered Dispatchable Workflow Tasks

**User Story:** As a Tokeira developer, I want `list_dispatchable_workflow_tasks_for_shard` to query DSQL for workflow runs with pending workflow tasks within a specific shard, so that shard-based sweep recovery can republish pending workflow work after failover.

#### Acceptance Criteria

1. WHEN `list_dispatchable_workflow_tasks_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query `workflow_hot` rows where `shard_id` matches the UUID encoding of the given shard, deserialize each `state_data`, and return runs where `pending_workflow_task` is present and `pending_workflow_task.started_event_id` is `None`.
2. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL clear expired sticky affinities where `expires_at` is at or before the current time before populating the result.

### Requirement 5: Shard-Filtered Dispatchable Activity Tasks

**User Story:** As a Tokeira developer, I want `list_dispatchable_activity_tasks_for_shard` to query DSQL for dispatchable activities within a specific shard, so that shard-based sweep recovery can republish pending activity work after failover.

#### Acceptance Criteria

1. WHEN `list_dispatchable_activity_tasks_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query the `activity_dispatch` table (not `activity_state`) where `shard_id` matches the UUID encoding of the given shard.
2. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `DispatchableActivityTask` fields from the `activity_dispatch` row columns.

### Requirement 6: Shard-Filtered Due Timers

**User Story:** As a Tokeira developer, I want `list_due_timers_for_shard` to query DSQL for due timers within a specific shard, so that shard-based sweep recovery can fire expired timers after failover.

#### Acceptance Criteria

1. WHEN `list_due_timers_for_shard` is called with a `ShardId` and `now` timestamp, THE DsqlRunRepository SHALL query `timer_bucket` rows where `shard_id` matches the UUID encoding of the given shard and `fire_at <= now`.
2. THE DsqlRunRepository SHALL use the `(shard_id, fire_at)` primary key prefix for efficient range scans.
3. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
4. THE DsqlRunRepository SHALL return at most `limit` results.
5. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
6. THE DsqlRunRepository SHALL populate `DueTimer` with `run_key` and `timer_id` from the queried rows.

### Requirement 7: Shard-Filtered Workflow Timeout Sweep

**User Story:** As a Tokeira developer, I want `list_runs_with_workflow_timeouts_for_shard` to query DSQL for open runs with workflow timeout configuration within a specific shard, so that shard-based sweep recovery can reconstruct workflow timeout tracking after failover.

#### Acceptance Criteria

1. WHEN `list_runs_with_workflow_timeouts_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query `workflow_hot` rows where `shard_id` matches the UUID encoding of the given shard, deserialize each `state_data`, and return runs that are open and have at least one of `workflow_execution_timeout` or `workflow_run_timeout` configured.
2. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `WorkflowTimeoutSweepEntry` fields (`run_key`, `workflow_execution_timeout`, `workflow_run_timeout`, `started_at`, `first_run_started_at`, `has_retry_policy`) from the deserialized `WorkflowState`.

### Requirement 8: Shard-Filtered Started Workflow Task Sweep

**User Story:** As a Tokeira developer, I want `list_started_workflow_tasks_for_shard` to query DSQL for runs with started (in-progress) workflow tasks within a specific shard, so that shard-based sweep recovery can reconstruct WFT timeout tracking after failover.

#### Acceptance Criteria

1. WHEN `list_started_workflow_tasks_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query `workflow_hot` rows where `shard_id` matches the UUID encoding of the given shard, deserialize each `state_data`, and return runs where `pending_workflow_task` is present and both `started_event_id` and `started_at` are `Some`.
2. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `WftTimeoutSweepEntry` fields (`run_key`, `logical_seq`, `started_event_id`, `started_at`, `workflow_task_timeout`) from the deserialized `WorkflowState`.

### Requirement 9: Shard-Filtered Open Activity Sweep

**User Story:** As a Tokeira developer, I want `list_open_activities_for_shard` to query DSQL for open activities within a specific shard, so that shard-based sweep recovery can reconstruct activity timeout tracking after failover.

#### Acceptance Criteria

1. WHEN `list_open_activities_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query `activity_state` rows where `shard_id` matches the UUID encoding of the given shard and deserialize each `state_data` to `ActivityState`.
2. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
3. THE DsqlRunRepository SHALL return at most `limit` results.
4. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
5. THE DsqlRunRepository SHALL populate `ActivitySweepEntry` fields (`run_key`, `activity_id`, `schedule_event_id`, `attempt`, `original_scheduled_at`, `started_at`, `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, `heartbeat_timeout`) from the deserialized `ActivityState`.

### Requirement 10: Shard-Filtered Pending Nexus Operation Sweep

**User Story:** As a Tokeira developer, I want `list_pending_nexus_operations_for_shard` to query DSQL for pending Nexus operations with timeouts within a specific shard, so that shard-based sweep recovery can reconstruct Nexus timeout tracking after failover.

#### Acceptance Criteria

1. WHEN `list_pending_nexus_operations_for_shard` is called with a `ShardId`, THE DsqlRunRepository SHALL query `workflow_hot` rows where `shard_id` matches the UUID encoding of the given shard, deserialize each `state_data`, and extract pending Nexus operations from `WorkflowState.pending_nexus_operations`.
2. THE DsqlRunRepository SHALL only include Nexus operations where `schedule_to_close_timeout` is `Some` — operations without a timeout do not need timeout tracking reconstruction.
3. THE DsqlRunRepository SHALL only include Nexus operations from runs that are open.
4. THE DsqlRunRepository SHALL bind `shard_id` as UUID via `DsqlRunRepository::shard_id_to_uuid(shard_id)`.
5. THE DsqlRunRepository SHALL return at most `limit` results.
6. THE DsqlRunRepository SHALL use a `DbClass::Read` connection for the query.
7. THE DsqlRunRepository SHALL populate `NexusSweepEntry` fields (`run_key`, `operation_id`, `scheduled_event_id`, `schedule_to_close_timeout`, `scheduled_at`) from the deserialized `PendingNexusOperation`.

### Requirement 11: Consistent Shard UUID Encoding

**User Story:** As a Tokeira developer, I want all shard-filtered queries to use the same deterministic `shard_id_to_uuid` encoding as the write path, so that queries return the correct rows for a given shard.

#### Acceptance Criteria

1. ALL shard-filtered queries SHALL bind `shard_id` as UUID using `DsqlRunRepository::shard_id_to_uuid(shard_id)`, which uses `dsql_spread_uuid` with the `"shard"` domain prefix.
2. THE shard UUID encoding SHALL be identical to the encoding used by `commit_transition` when writing `shard_id` to `workflow_hot`, `activity_state`, and `timer_bucket`.

### Requirement 12: Read Connection Class

**User Story:** As a Tokeira developer, I want all side-table query methods to use `DbClass::Read` connections, so that read traffic does not compete with commit-path connections.

#### Acceptance Criteria

1. ALL 10 methods in this spec SHALL acquire connections using `DbClass::Read`.
2. 9 of the 10 methods SHALL execute single-statement queries outside any explicit transaction. The exception is `list_due_timers`, which is implemented as shard fanout (one `list_due_timers_for_shard` call per shard) because `timer_bucket` has no standalone `fire_at` index.

### Requirement 13: Postcard Deserialization

**User Story:** As a Tokeira developer, I want all BYTEA columns to be deserialized using the codec module from Feature 1, so that the serialization format is consistent across the read and write paths.

#### Acceptance Criteria

1. THE DsqlRunRepository SHALL deserialize `workflow_hot.state_data` using `codec::decode_workflow_state`.
2. THE DsqlRunRepository SHALL deserialize `activity_state.state_data` using `codec::decode_activity_state`.
3. IF deserialization fails, THEN THE DsqlRunRepository SHALL return an error with context identifying the table and row key.

### Requirement 14: Tracing Instrumentation

**User Story:** As a Tokeira developer, I want all side-table query methods to be instrumented with `tracing::instrument`, so that query latency and errors are observable.

#### Acceptance Criteria

1. ALL 10 methods in this spec SHALL be annotated with `#[instrument]` using the `dsql.` prefix naming convention (e.g., `dsql.list_due_timers`, `dsql.list_open_activities_for_shard`).
2. THE instrument spans SHALL include relevant parameters (`shard_id`, `limit`, `now` where applicable) as span fields.

### Requirement 15: Activity Dispatch Table

**User Story:** As a Tokeira developer, I want a dedicated `activity_dispatch` DSQL table that stores one row per currently dispatchable activity task, so that dispatch queries read from a purpose-built dispatch source rather than the open-activity state table.

#### Acceptance Criteria

1. THE `activity_dispatch` table SHALL store one row per dispatchable activity task, keyed by a spread UUID derived from `(run_key, activity_id)`.
2. THE table SHALL include columns for the full queue identity (`queue_namespace`, `queue_name`, `task_kind`, `deployment`, `build_id`), `shard_id`, `run_key`, `activity_id`, `schedule_event_id`, `attempt`, `input_data` (BYTEA, postcard-encoded `Payloads` using `codec::encode_payloads`/`codec::decode_payloads` helpers), and `created_at`.
3. THE table SHALL have secondary async indexes for shard-filtered queries (`shard_id`), queue-filtered queries (`queue_namespace, queue_name, task_kind, deployment, build_id`), and run-scoped bulk deletes on workflow pause (`run_key`).
4. THE table DDL SHALL be added as a new migration file in the `migrations/` directory. Tokeira targets schema version 1 — no migration from existing data is needed.

### Requirement 16: Activity Dispatch Write Path

**User Story:** As a Tokeira developer, I want `commit_transition` to maintain the `activity_dispatch` table from dispatch ops, activity ops, and workflow status changes, so that the dispatch table reflects the current set of dispatchable activities.

#### Acceptance Criteria

1. WHEN `commit_transition` processes a `DispatchOp::EnqueueActivityTask`, THE DsqlRunRepository SHALL insert a row into `activity_dispatch` with the full queue identity, input, and scheduling metadata using `INSERT ... ON CONFLICT (key) DO UPDATE` so that re-enqueue after reset/retry/unpause refreshes the existing row rather than failing on a primary-key conflict.
2. WHEN `commit_transition` processes an `ActivityOp::Delete`, THE DsqlRunRepository SHALL delete the corresponding row from `activity_dispatch` (by computing the spread key from `run_key` and `activity_id`) in addition to `activity_state`.
3. WHEN `commit_transition` processes an `ActivityOp::Upsert` where the upserted `ActivityState` has `pause_info.is_some()` or `started_at.is_some()`, THE DsqlRunRepository SHALL delete the corresponding row from `activity_dispatch` — paused and started activities are not dispatchable.
4. WHEN `commit_transition` processes an `ActivityOp::Upsert` where the upserted `ActivityState` has `pause_info.is_none()` and `started_at.is_none()` and a corresponding `activity_dispatch` row exists, THE DsqlRunRepository SHALL UPDATE (not INSERT) the row with the current queue identity, attempt, and input from the upserted state — this handles `UpdateActivityOptions` which can change the task queue. Only `DispatchOp::EnqueueActivityTask` creates new dispatch rows.
5. WHEN `commit_transition` transitions the workflow to `ExecutionStatus::Paused`, THE DsqlRunRepository SHALL delete all `activity_dispatch` rows for the run — a paused workflow suppresses all activity dispatch.
6. WHEN `commit_transition` transitions the workflow from `ExecutionStatus::Paused` to an open status (unpause), activity re-enqueue is handled by `DispatchOp::EnqueueActivityTask` emitted by the kernel's `apply_unpause_workflow` — no special dispatch-table logic needed.
7. ALL `activity_dispatch` writes SHALL occur within the same fenced transaction as the rest of the commit write set.

### Requirement 17: Activity Start Removes Dispatch Entry

**User Story:** As a Tokeira developer, I want a successful activity start to remove the activity from `activity_dispatch`, so that started activities are not re-dispatched on recovery.

#### Acceptance Criteria

1. WHEN the runtime's `start_activity_task` commits a transition containing `ActivityOp::Upsert` with `started_at = Some(...)`, THE `commit_transition` implementation SHALL delete the corresponding `activity_dispatch` row as specified by Requirement 16.3. No new transition write-set surface is needed — the delete is derived from the existing `ActivityOp::Upsert`.
2. THE in-memory store's `commit_transition` SHALL also remove the `(run_key, activity_id)` entry from `activity_dispatch` when processing `ActivityOp::Upsert` with `started_at = Some(...)`, making both stores consistent.

### Requirement 18: Runtime Guard Against Duplicate Activity Start

**User Story:** As a Tokeira developer, I want the runtime to reject activity start attempts for activities that are already started, so that duplicate broker delivery or stale recovery rows cannot produce a second `ActivityTaskStarted`.

#### Acceptance Criteria

1. WHEN `start_activity_task` loads the current `ActivityState` and `started_event_id` is already `Some(...)`, THE runtime SHALL return `None` (skip the activity) instead of emitting a new `ActivityTaskStarted` transition.
2. THIS guard is a correctness backstop — the primary protection is that started activities are removed from `activity_dispatch` (Requirement 17), but the runtime guard handles edge cases like duplicate broker delivery.

### Requirement 19: In-Memory Store Consistency

**User Story:** As a Tokeira developer, I want the in-memory store's activity dispatch behavior to match the DSQL implementation, so that property tests and semantic tests validate the correct dispatch lifecycle.

#### Acceptance Criteria

1. THE in-memory store SHALL remove the `(run_key, activity_id)` entry from `activity_dispatch` when processing a transition that contains an `ActivityOp::Upsert` with `started_at = Some(...)` or `pause_info = Some(...)`.
2. THE in-memory store SHALL update the `activity_dispatch` entry when processing an `ActivityOp::Upsert` where the activity is still dispatchable (`started_at.is_none()` and `pause_info.is_none()`) and a dispatch entry exists — this handles queue identity changes from `UpdateActivityOptions`.
3. THE in-memory store SHALL remove all `activity_dispatch` entries for a run when the workflow transitions to `ExecutionStatus::Paused`.
4. THE in-memory store SHALL insert into `activity_dispatch` when processing `DispatchOp::EnqueueActivityTask` (already implemented).
5. THE in-memory store SHALL remove from `activity_dispatch` when processing `ActivityOp::Delete` (already implemented).
6. AFTER these changes, `list_dispatchable_activity_tasks` and `list_dispatchable_activity_tasks_for_shard` SHALL NOT return started, paused, or workflow-paused activities from either the in-memory store or the DSQL implementation.
