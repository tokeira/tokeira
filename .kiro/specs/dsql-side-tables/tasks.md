# Implementation Plan: DSQL Side Tables — Read-Only Query Methods

## Overview

Replace 10 `bail!("Feature 3: dsql-side-tables")` stubs in `DsqlRunRepository` with concrete implementations. Query methods use `DbClass::Read` connections against `workflow_hot`, `activity_state`, `activity_dispatch`, and `timer_bucket`. The implementation follows four query patterns (A–D) identified in the design, with a shared `sticky_fields` helper for sticky affinity expiry clearing.

Code changes span DSQL repository queries, activity-dispatch write-path maintenance, in-memory-store parity, runtime duplicate-start guarding, codec helpers, and migrations V024-V028.

## Tasks

- [x] 0. Activity dispatch table and write path
  - [x] 0.1 Create `activity_dispatch` table DDL and index migrations
    - Add `V025__activity_dispatch.sql` with the table DDL (spread UUID PK, full queue identity columns, shard_id, input_data BYTEA)
    - Add `V026__idx_activity_dispatch_shard.sql`: `CREATE INDEX ASYNC idx_activity_dispatch_shard ON activity_dispatch (shard_id);`
    - Add `V027__idx_activity_dispatch_queue.sql`: `CREATE INDEX ASYNC idx_activity_dispatch_queue ON activity_dispatch (queue_namespace, queue_name, task_kind, deployment, build_id);`
    - Add `V028__idx_activity_dispatch_run_key.sql`: `CREATE INDEX ASYNC idx_activity_dispatch_run_key ON activity_dispatch (run_key);` — needed for bulk delete on workflow pause
    - _Requirements: 15.1, 15.2, 15.3, 15.4_

  - [x] 0.2 Add `activity_dispatch` write path to `commit_transition`
    - Add a `activity_dispatch_key(run_key, activity_id) -> Uuid` helper using `dsql_spread_uuid(&[b"activity-dispatch", run_key.0.as_bytes(), activity_id.as_bytes()])`
    - On `DispatchOp::EnqueueActivityTask`: `INSERT INTO activity_dispatch ... ON CONFLICT (key) DO UPDATE SET ...` with computed spread key, full queue identity, input (postcard-encoded `Payloads`), scheduling metadata. ON CONFLICT handles re-enqueue after reset/retry/unpause.
    - On `ActivityOp::Delete`: delete from `activity_dispatch` by PK (`WHERE key = $1`, computing the spread key from `run_key` and `activity_id`)
    - On `ActivityOp::Upsert` where `pause_info.is_some()` or `started_at.is_some()`: delete from `activity_dispatch` by PK
    - On `ActivityOp::Upsert` where `pause_info.is_none()` and `started_at.is_none()` and a corresponding `activity_dispatch` row exists: `UPDATE activity_dispatch SET queue_namespace = $2, queue_name = $3, task_kind = $4, deployment = $5, build_id = $6, attempt = $7, input_data = $8 WHERE key = $1` — UPDATE only, not INSERT. Only `DispatchOp::EnqueueActivityTask` creates rows. This prevents creating dispatch entries for activities scheduled while the workflow is paused (those have `started_at = None` and `pause_info = None` but were never enqueued).
    - On workflow status transition to `Paused`: delete all `activity_dispatch` rows for the run using `DELETE FROM activity_dispatch WHERE run_key = $1` (requires `(run_key)` index — add V028)
    - All writes within the same fenced transaction
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7_

  - [x] 0.3 Verify activity start removes dispatch entry via existing mechanism
    - The runtime's `start_activity_task` already emits `ActivityOp::Upsert` with `started_at = Some(...)`. Requirement 16.3 handles this: `commit_transition` derives the dispatch-row delete from `ActivityOp::Upsert` with `started_at.is_some()`. No new transition write-set surface is needed.
    - In the in-memory store's `commit_transition`, apply the same logic: when processing `ActivityOp::Upsert` with `started_at.is_some()`, remove `(run_key, activity_id)` from `activity_dispatch`
    - _Requirements: 17.1, 17.2_

  - [x] 0.4 Add runtime guard against duplicate activity start
    - In `start_activity_task`, after loading the current `ActivityState`, reject (return `None`) if `current.started_event_id.is_some()`
    - This is a backstop — the primary protection is dispatch entry removal (0.3)
    - _Requirements: 18.1, 18.2_

  - [x] 0.5 Add tests for activity dispatch lifecycle
    - Test: scheduled activity appears in dispatch queries
    - Test: started activity does NOT appear in dispatch queries (removed on start via `ActivityOp::Upsert` with `started_at = Some(...)`)
    - Test: individually paused activity does NOT appear in dispatch queries (removed on pause via `ActivityOp::Upsert` with `pause_info = Some(...)`)
    - Test: workflow-paused run has NO activities in dispatch queries (bulk delete on workflow pause)
    - Test: unpaused activity re-appears in dispatch queries (re-enqueued via `DispatchOp::EnqueueActivityTask`)
    - Test: `UpdateActivityOptions` changing task queue updates the dispatch entry's queue identity
    - Test: resolved activity does NOT appear in dispatch queries (removed on `ActivityOp::Delete`)
    - Test: runtime rejects start for already-started activity (`started_event_id.is_some()`)
    - Test: in-memory store and DSQL implementation agree on dispatch lifecycle
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5, 17.1, 17.2, 18.1, 19.1, 19.2, 19.3, 19.6_

- [x] 1. Add `workflow_hot(namespace_id)` index migration and extract helpers
  - [x] 1.1 Create `V024__idx_workflow_hot_namespace.sql`
    - Content: `CREATE INDEX ASYNC idx_workflow_hot_namespace ON workflow_hot (namespace_id);`
    - Required for `list_dispatchable_workflow_tasks` which queries `workflow_hot WHERE namespace_id = $1`
    - _Requirements: 1.1, 1.2_

  - [x] 1.2 Add the `sticky_fields` helper function
    - Add `fn sticky_fields(state: &WorkflowState, now: OffsetDateTime) -> (Option<WorkerIdentity>, Option<OffsetDateTime>)` as a free function near the bottom of `run_repository.rs` (alongside other helpers like `partition_for`)
    - If `sticky.expires_at > now`, return `(Some(worker_identity), Some(expires_at))`; otherwise return `(None, None)`
    - _Requirements: 1.6, 4.5_

  - [x] 1.3 Write property test for sticky affinity expiry clearing
    - **Property 2: Sticky Affinity Expiry Clearing**
    - Generate random `WorkflowState` values with varying `sticky` fields (None, expired, non-expired) and a random `now` timestamp
    - Assert: if `sticky.expires_at > now`, output has `Some` values matching the sticky; if `sticky.expires_at <= now` or sticky is `None`, output is `(None, None)`
    - **Validates: Requirements 1.6, 4.5**

- [x] 2. Implement Pattern D — Timer range scan methods (no deserialization)
  - [x] 2.1 Implement `list_due_timers`
    - Add `#[instrument(name = "dsql.list_due_timers", skip(self), fields(%limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Implement as shard fanout: iterate `0..self.shard_count`, call `self.list_due_timers_for_shard(ShardId(i), now, remaining_limit)` for each shard, collecting results until `limit` is reached
    - This avoids an unindexed `fire_at` scan — `timer_bucket` PK is `(shard_id, fire_at, ...)` with no standalone `fire_at` index
    - _Requirements: 3.1, 3.2, 3.4, 12.1, 14.1_

  - [x] 2.2 Implement `list_due_timers_for_shard`
    - Add `#[instrument(name = "dsql.list_due_timers_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Acquire `DbClass::Read` connection
    - Bind `shard_id` via `Self::shard_id_to_uuid(shard_id)`
    - SQL: `SELECT run_key, timer_id FROM timer_bucket WHERE shard_id = $1 AND fire_at <= $2 LIMIT $3`
    - Map rows to `DueTimer`
    - _Requirements: 6.1, 6.3, 6.4, 6.6, 11.1, 12.1, 14.1_

  - [x] 2.3 Write property test for timer deadline filter
    - **Property 4: Timer Deadline Filter**
    - Generate a set of `(run_key, timer_id, fire_at)` tuples with varying `fire_at` values and a random `now` timestamp
    - Assert: only timers where `fire_at <= now` are returned, and `DueTimer` fields match stored values
    - **Validates: Requirements 3.1, 3.4, 6.1, 6.6**

  - [x] 2.4 Write property test for result limit invariant
    - **Property 5: Result Limit Invariant**
    - Generate random inputs for each of the 10 methods with varying `limit` values (including 0)
    - Assert: `result.len() <= limit` for every call
    - Test the timer methods directly; test the filter/collect logic for workflow/activity methods
    - **Validates: Requirements 1.3, 2.3, 3.2, 4.3, 5.3, 6.4, 7.3, 8.3, 9.3, 10.5**

- [x] 3. Checkpoint — Ensure timer methods compile and tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement Pattern A — Shard-filtered `workflow_hot` scan methods
  - [x] 4.1 Implement `list_dispatchable_workflow_tasks_for_shard`
    - Add `#[instrument(name = "dsql.list_dispatchable_workflow_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Acquire `DbClass::Read` connection
    - SQL: `SELECT run_key, state_data FROM workflow_hot WHERE shard_id = $1`
    - Bind `Self::shard_id_to_uuid(shard_id)`
    - For each row: decode `state_data` via `codec::decode_workflow_state` with `.with_context(|| format!("workflow_hot row {run_key}"))`
    - Filter: `pending_workflow_task.is_some()` AND `started_event_id.is_none()`
    - Use `sticky_fields(&state, now)` for sticky affinity
    - Construct `DispatchableWorkflowTask` with `queue.task_kind = Workflow`, `deployment = None`, `build_id = None`
    - Stop collecting at `limit`
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 11.1, 12.1, 13.1, 13.3, 14.1_

  - [x] 4.2 Implement `list_runs_with_workflow_timeouts_for_shard`
    - Add `#[instrument(name = "dsql.list_runs_with_workflow_timeouts_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Same SQL as 4.1 (shard-filtered `workflow_hot`)
    - Filter: `status.is_open()` AND (`workflow_execution_timeout.is_some()` OR `workflow_run_timeout.is_some()`)
    - Map to `WorkflowTimeoutSweepEntry` with fields from deserialized `WorkflowState`
    - _Requirements: 7.1, 7.2, 7.3, 7.5, 11.1, 12.1, 13.1, 13.3, 14.1_

  - [x] 4.3 Implement `list_started_workflow_tasks_for_shard`
    - Add `#[instrument(name = "dsql.list_started_workflow_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Same SQL as 4.1
    - Filter: `pending_workflow_task` present with both `started_event_id.is_some()` AND `started_at.is_some()`
    - Map to `WftTimeoutSweepEntry`
    - _Requirements: 8.1, 8.2, 8.3, 8.5, 11.1, 12.1, 13.1, 13.3, 14.1_

  - [x] 4.4 Implement `list_pending_nexus_operations_for_shard`
    - Add `#[instrument(name = "dsql.list_pending_nexus_operations_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Same SQL as 4.1
    - Filter: `status.is_open()`, then iterate `pending_nexus_operations` and include only entries where `schedule_to_close_timeout.is_some()`
    - The `limit` applies to total `NexusSweepEntry` count, not workflow row count — break out of both loops when limit reached
    - Map to `NexusSweepEntry`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.7, 11.1, 12.1, 13.1, 13.3, 14.1_

  - [x] 4.5 Write property test for workflow dispatch eligibility filter
    - **Property 1: Workflow Dispatch Eligibility Filter**
    - Generate random `WorkflowState` values with varying `pending_workflow_task` (None, scheduled-not-started, started) and varying `namespace_id`/`task_queue`
    - Assert: only states with `pending_workflow_task.is_some()` AND `started_event_id.is_none()` are included
    - Assert: `queue.task_kind == Workflow`, `deployment == None`, `build_id == None`
    - **Validates: Requirements 1.1, 1.2, 1.5, 4.1**

  - [x] 4.6 Write property test for workflow timeout sweep filter and field mapping
    - **Property 6: Workflow Timeout Sweep Filter and Field Mapping**
    - Generate random `WorkflowState` values with varying `status`, `workflow_execution_timeout`, `workflow_run_timeout`
    - Assert: only open runs with at least one timeout configured are included
    - Assert: output fields match deserialized `WorkflowState` fields
    - **Validates: Requirements 7.1, 7.5**

  - [x] 4.7 Write property test for started WFT sweep filter and field mapping
    - **Property 7: Started WFT Sweep Filter and Field Mapping**
    - Generate random `WorkflowState` values with varying `pending_workflow_task` states
    - Assert: only runs with `started_event_id.is_some()` AND `started_at.is_some()` are included
    - Assert: output fields match deserialized state
    - **Validates: Requirements 8.1, 8.5**

  - [x] 4.8 Write property test for Nexus operation sweep filter and field mapping
    - **Property 9: Nexus Operation Sweep Filter and Field Mapping**
    - Generate random `WorkflowState` values with varying `status` and `pending_nexus_operations` (with and without `schedule_to_close_timeout`)
    - Assert: only open runs contribute entries, and only operations with `schedule_to_close_timeout.is_some()` are included
    - Assert: limit applies to total `NexusSweepEntry` count, not workflow row count
    - Assert: output fields match deserialized operation
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.7**

- [x] 5. Checkpoint — Ensure Pattern A methods compile and tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement Pattern B — Shard-filtered `activity_state` scan methods
  - [x] 6.1 Implement `list_dispatchable_activity_tasks_for_shard`
    - Add `#[instrument(name = "dsql.list_dispatchable_activity_tasks_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Acquire `DbClass::Read` connection
    - SQL: `SELECT run_key, activity_id, queue_namespace, queue_name, task_kind, deployment, build_id, schedule_event_id, attempt, input_data FROM activity_dispatch WHERE shard_id = $1 LIMIT $2`
    - Bind `Self::shard_id_to_uuid(shard_id)` and `i64::try_from(limit)?`
    - Construct `DispatchableActivityTask` directly from row columns — no BYTEA deserialization needed for queue/scheduling fields. Decode `input_data` via `codec::decode::<Payloads>` (postcard-encoded `Payloads` — the same type used in `DispatchableActivityTask.input`). Add `encode_payloads`/`decode_payloads` helpers to the codec module.
    - _Requirements: 5.1, 5.2, 5.3, 5.5, 11.1, 12.1, 14.1_

  - [x] 6.2 Implement `list_open_activities_for_shard`
    - Add `#[instrument(name = "dsql.list_open_activities_for_shard", skip(self), fields(shard_id = shard_id.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - SQL: `SELECT run_key, queue_namespace, state_data FROM activity_state WHERE shard_id = $1 LIMIT $2`
    - Deserialize each row via `codec::decode_activity_state` with context
    - Map to `ActivitySweepEntry` with all timeout and scheduling fields from deserialized `ActivityState`
    - _Requirements: 9.1, 9.2, 9.3, 9.5, 11.1, 12.1, 13.2, 13.3, 14.1_

  - [x] 6.3 Write property test for activity dispatch field fidelity
    - **Property 3: Activity Dispatch Field Fidelity**
    - Generate random `ActivityState` values with varying fields
    - Assert: output `DispatchableActivityTask` fields (`run_key`, `activity_id`, `input`, `schedule_event_id`, `attempt`) match the deserialized state
    - Assert: `queue.task_kind == Activity`
    - **Validates: Requirements 2.1, 2.2, 2.5, 5.1, 5.5**

  - [x] 6.4 Write property test for activity sweep field mapping
    - **Property 8: Activity Sweep Field Mapping**
    - Generate random `ActivityState` values
    - Assert: output `ActivitySweepEntry` fields (`run_key`, `activity_id`, `schedule_event_id`, `attempt`, `original_scheduled_at`, `started_at`, and all four timeout fields) match the deserialized state
    - **Validates: Requirements 9.1, 9.5**

- [x] 7. Implement Pattern C — Queue-filtered scan methods
  - [x] 7.1 Implement `list_dispatchable_workflow_tasks`
    - Add `#[instrument(name = "dsql.list_dispatchable_workflow_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Acquire `DbClass::Read` connection
    - SQL: `SELECT run_key, state_data FROM workflow_hot WHERE namespace_id = $1`
    - Bind `queue.namespace_id.0`
    - For each row: decode `state_data`, filter by `task_queue` match with `queue.task_queue`, `pending_workflow_task.is_some()`, `started_event_id.is_none()`
    - Use `sticky_fields` for sticky affinity
    - Construct `DispatchableWorkflowTask` with `queue.task_kind = Workflow`, `deployment = None`, `build_id = None`
    - Stop at `limit`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 12.1, 13.1, 13.3, 14.1_

  - [x] 7.2 Implement `list_dispatchable_activity_tasks`
    - Add `#[instrument(name = "dsql.list_dispatchable_activity_tasks", skip(self), fields(namespace_id = %queue.namespace_id.0, task_queue = %queue.task_queue.0, %limit))]`
    - Return empty `Vec` immediately if `limit == 0`
    - Acquire `DbClass::Read` connection
    - SQL: `SELECT run_key, activity_id, queue_namespace, queue_name, task_kind, deployment, build_id, schedule_event_id, attempt, input_data FROM activity_dispatch WHERE queue_namespace = $1 AND queue_name = $2 AND task_kind = $3 AND deployment IS NOT DISTINCT FROM $4 AND build_id IS NOT DISTINCT FROM $5 LIMIT $6`
    - Full QueueKey filter in SQL — no application-level filtering needed
    - Construct `DispatchableActivityTask` directly from row columns
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 12.1, 14.1_

- [x] 8. Checkpoint — Ensure all 10 methods compile and tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Add unit tests for edge cases
  - [x] 9.1 Unit test: zero limit returns empty Vec without DB query
    - Test that calling each method with `limit = 0` returns an empty `Vec`
    - Use the `RecordingAcquirer` mock that panics if `acquire` is called, proving no DB round-trip occurs
    - _Requirements: 1.3, 2.3, 3.2, 4.3, 5.3, 6.4, 7.3, 8.3, 9.3, 10.5_

  - [x] 9.2 Unit test: `sticky_fields` with expired affinity returns None
    - Construct a `WorkflowState` with `sticky.expires_at <= now`
    - Verify `sticky_fields` returns `(None, None)`
    - _Requirements: 1.6, 4.5_

  - [x] 9.3 Unit test: `sticky_fields` with non-expired affinity returns values
    - Construct a `WorkflowState` with `sticky.expires_at > now`
    - Verify `sticky_fields` returns `(Some(worker_identity), Some(expires_at))`
    - _Requirements: 1.6, 4.5_

  - [x] 9.4 Unit test: `list_due_timers` shard fanout respects limit across shards
    - Verify that the shard fanout stops collecting once `limit` is reached, even if more shards remain
    - This is a logic test on the fanout loop, not a SQL test
    - _Requirements: 3.2_

  - [x] 9.5 Unit test: Nexus limit spans multiple runs
    - Extract the Nexus filter/collect logic into a testable helper
    - Construct multiple `WorkflowState` values each with multiple `PendingNexusOperation` entries
    - Verify the limit applies to total `NexusSweepEntry` count, not workflow row count
    - _Requirements: 10.5_

  - [x] 9.6 Unit test: activity queue mismatch filtering
    - Extract the activity queue-match filter into a testable helper
    - Construct activities with matching `(namespace, queue_name)` but different `(deployment, build_id)`
    - Verify the filter excludes mismatched activities
    - _Requirements: 2.2_

- [x] 10. Final checkpoint — Ensure all tests pass
  - Run `cargo clippy --workspace --all-targets` and `cargo test -p tokeira-storage`. Ensure all tests pass, ask the user if questions arise.

## Notes

- Code changes span `tokeira-storage/src/dsql/run_repository.rs`, `tokeira-storage/src/memory.rs`, `tokeira-runtime/src/runtime.rs`, and `tokeira-storage/src/dsql/codec.rs`
- Migrations V024–V028: V024 (workflow_hot namespace index), V025 (activity_dispatch table), V026–V028 (activity_dispatch shard/queue/run_key indexes)
- Query methods use `DbClass::Read` — no transactions, no writes. Write-path changes (Task 0) are within the existing `commit_transition` fenced transaction.
- The `#[cfg(feature = "dsql")]` gate is already on the module; no additional gating needed
- Property tests use `proptest` with the tag format: `Feature: dsql-side-tables, Property {N}: {title}`
- Unit tests use a mock `DsqlConnectionAcquirer` (the `#[cfg(test)]` trait already exists in the file)
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
