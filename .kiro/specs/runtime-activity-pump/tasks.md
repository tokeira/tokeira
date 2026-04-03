# Implementation Plan: Activity Pump — Dispatch, Poll, Complete, Retry

## Overview

Add the activity task delivery pipeline to `tokeira-runtime`, completing the second major runtime feature after Lane OCC Retry. The implementation proceeds bottom-up: token type update first, then the activity broker, then retry policy pure functions, then the runtime facade methods (poll, complete, fail, sweep), then publisher wiring, and finally integration tests. Property tests are placed immediately after the code they validate — all property tests are required, not optional.

## Tasks

- [x] 0. Extend kernel/storage types with input and retry policy (prerequisite)
  - [x] 0.1 Add `input: Payloads` and `retry_policy: Option<RetryPolicy>` fields to `ActivityState` in `tokeira/crates/tokeira-kernel/src/state.rs`
    - _Requirements: 0.1, 0.2_

  - [x] 0.2 Add `retry_policy: Option<RetryPolicy>` field to `WorkflowCommand::ScheduleActivity` in `tokeira/crates/tokeira-kernel/src/command.rs`
    - _Requirements: 0.3_

  - [x] 0.3 Add `input: Payloads` field to `DispatchOp::EnqueueActivityTask` in `tokeira/crates/tokeira-kernel/src/transition.rs`
    - _Requirements: 0.6_

  - [x] 0.4 Update kernel's `apply_workflow_command` for `ScheduleActivity` to populate `ActivityState.input` and `ActivityState.retry_policy` from the command, and `DispatchOp::EnqueueActivityTask.input` from the command
    - _Requirements: 0.4, 0.6_

  - [x] 0.5 Add `input: Payloads` field to `DispatchableActivityTask` in `tokeira/crates/tokeira-storage/src/api.rs`
    - _Requirements: 0.5_

  - [x] 0.6 Fix all compilation errors from the type changes across the workspace
    - Update all code that constructs `ActivityState`, `DispatchOp::EnqueueActivityTask`, `DispatchableActivityTask`, and `WorkflowCommand::ScheduleActivity` to populate the new fields
    - _Requirements: 0.7_

  - [x] 0.7 Checkpoint — Ensure `cargo check` passes across the workspace
    - _Requirements: 0.7_

- [x] 1. Update `ActivityTaskToken` in `tokeira-types`
  - [x] 1.1 Modify `ActivityTaskToken` in `tokeira/crates/tokeira-types/src/tokens.rs`
    - Add `activity_id: String` field
    - Remove `started_event_id: i64` field
    - Keep `run_key`, `schedule_event_id`, `attempt`, `shard_epoch`
    - Ensure `Clone, Debug, PartialEq, Eq, Serialize, Deserialize` derives are present
    - _Requirements: 8.1, 8.2, 8.3_

  - [x] 1.2 Fix any compilation errors from the token change
    - Update any references to `ActivityTaskToken` across the workspace that use `started_event_id`
    - _Requirements: 8.1_

  - [x] 1.3 Write property test: ActivityTaskToken round-trip fidelity
    - **Property 4: ActivityTaskToken round-trip fidelity**
    - For any `(run_key, activity_id, schedule_event_id, attempt, shard_epoch)`, construct a token and verify all fields read back identically; verify clone equality
    - **Validates: Requirements 3.2, 8.1, 8.2**

- [x] 2. Implement `InMemoryActivityBroker`
  - [x] 2.1 Create `InMemoryActivityBroker` in `tokeira/crates/tokeira-runtime/src/broker.rs` (or a new `activity_broker.rs`)
    - `ActivityBrokerState` with `ready: HashMap<QueueKey, VecDeque<DispatchableActivityTask>>` and `enqueued: HashSet<(RunKey, String, u32)>`
    - `publish_activity_task`: returns `Result<()>`, dedup by `(run_key, activity_id, attempt)`, push to per-queue FIFO, notify waiters
    - `poll_activity_task`: long-poll with timeout, pop from queue FIFO, remove from dedup set
    - No sticky routing (unlike workflow broker)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 2.2 Write property test: Activity broker deduplication
    - **Property 1: Activity broker deduplication by (run_key, activity_id, attempt)**
    - Publish same `(run_key, activity_id, attempt)` multiple times; verify poll returns the task exactly once, second poll returns `None`
    - **Validates: Requirements 1.3, 1.5, 9.4**

  - [x] 2.3 Write property test: Activity broker queue isolation
    - **Property 2: Activity broker queue isolation**
    - Publish on queue A, poll on queue B; verify queue B returns `None`, queue A returns the task
    - **Validates: Requirements 1.2**

- [x] 3. Implement retry policy pure functions
  - [x] 3.1 Add `evaluate_activity_retry` and `compute_retry_backoff` in `tokeira/crates/tokeira-runtime/src/runtime.rs` (or a new `retry.rs`)
    - `RetryDecision` enum: `Retry { next_attempt: u32 }` or `Exhausted`
    - `evaluate_activity_retry(policy, current_attempt, failure_error_type) -> RetryDecision`
      - `Exhausted` when `maximum_attempts > 0 && current_attempt >= maximum_attempts`
      - `Exhausted` when `failure_error_type` matches any entry in `non_retryable_error_types`
      - `Retry { next_attempt: current_attempt + 1 }` otherwise
      - `maximum_attempts == 0` means unlimited retries for retryable errors
    - `compute_retry_backoff(policy, attempt) -> Duration`
      - Formula: `initial_interval * backoff_coefficient^(attempt - 1)`, capped at `maximum_interval`
      - Defensive: clamp `backoff_coefficient` to `max(1.0, value)`, zero `initial_interval` produces zero backoff
    - _Requirements: 5.5, 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x] 3.2 Write property test: Retry-or-resolve decision
    - **Property 7: Retry-or-resolve decision**
    - For any `(RetryPolicy, current_attempt, failure_error_type)`, verify `Exhausted` iff `(maximum_attempts > 0 && current_attempt >= maximum_attempts)` or error type matches `non_retryable_error_types`; `Retry` with `next_attempt = current_attempt + 1` otherwise; `maximum_attempts == 0` always retries for retryable types
    - **Validates: Requirements 5.3, 5.4, 5.6, 6.2, 6.3**

  - [x] 3.3 Write property test: Backoff computation
    - **Property 8: Backoff computation**
    - For any `(RetryPolicy, attempt)`, verify result equals `min(initial_interval * backoff_coefficient^(attempt-1), maximum_interval)` when `maximum_interval` is set, or `initial_interval * backoff_coefficient^(attempt-1)` when not
    - **Validates: Requirements 6.4, 6.5**

- [x] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement `StartedActivityTask` and activity-task-start transaction
  - [x] 5.1 Define `StartedActivityTask` struct in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Fields: `run_key`, `activity_id`, `task_queue`, `token: ActivityTaskToken`, `input: Payloads`, `attempt`, `schedule_to_close_timeout`, `start_to_close_timeout`, `heartbeat_timeout`
    - _Requirements: 2.2, 2.5_

  - [x] 5.2 Implement `start_activity_task` private method on `TokeiraRuntime`
    - Load `WorkflowState` via `repo.load_run(task.run_key)`
    - Validate activity exists in `state.activities` and attempt matches
    - Update `ActivityState` (increment `stamp` to record start)
    - Commit via `repo.commit_transition` with empty `history_events` but `ActivityOp::Upsert` for the updated activity
    - On success: construct `ActivityTaskToken` (with `shard_epoch: ShardEpoch::ZERO` until Feature 11) and return `Some(StartedActivityTask)`
    - On OCC conflict: retry with bounded retries (reload, revalidate, re-commit). If activity gone after reload, return `None`. If retries exhaust and activity still present, re-publish task to activity broker and return `None`.
    - If activity not found in `state.activities` after any load: return `None`
    - _Requirements: 3.1, 3.2, 3.6_

  - [x] 5.3 Write property test: Activity start produces no history events
    - **Property 3: Activity start produces no history events**
    - For any activity-task-start transaction, verify the committed transition has empty `history_events` and contains an `ActivityOp::Upsert`
    - **Validates: Requirements 3.1, 3.6**

- [x] 6. Implement `poll_activity_task` facade method
  - [x] 6.1 Add `poll_activity_task` to `TokeiraRuntime` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Accept `queue: QueueKey`, `worker_identity: WorkerIdentity`, `timeout_after: tokio::time::Duration`
    - Poll `activity_broker` for a task
    - If matched, call `start_activity_task`; if start succeeds return `Some(StartedActivityTask)`, if start fails return `None`
    - If no task within timeout, return `None`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 6.2 Write property test: Successful poll returns started task with valid token
    - **Property 13: Successful poll returns started task with valid token**
    - For any published activity task on a matching queue, when start transaction succeeds, verify returned `StartedActivityTask` has token fields matching the activity's current state
    - **Validates: Requirements 2.2, 2.4, 2.5**

- [x] 7. Implement token validation and `complete_activity_task`
  - [x] 7.1 Implement token validation helper on `TokeiraRuntime`
    - Load `WorkflowState` for `token.run_key`
    - Check `activity_id` exists in `state.activities`
    - Check `token.attempt == activity_state.attempt`
    - Check `token.shard_epoch` matches current epoch
    - Return error describing which check failed if any fails
    - _Requirements: 3.3, 3.4, 3.5, 8.4_

  - [x] 7.2 Implement `complete_activity_task` on `TokeiraRuntime`
    - Accept `token: ActivityTaskToken` and `result: Payloads`
    - Validate token via the helper
    - Submit `Command::ActivityResolved` with `ActivityResolution::Completed { result }` via the lane
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [x] 7.3 Write property test: Stale token rejection
    - **Property 5: Stale token rejection**
    - For any token where activity_id is missing, attempt mismatches, or shard_epoch mismatches, verify both `complete_activity_task` and `fail_activity_task` reject with error and no state mutation
    - **Validates: Requirements 3.3, 3.4, 3.5, 4.3, 5.2, 8.4**

  - [x] 7.4 Write property test: Completion submits correct ActivityResolved command
    - **Property 6: Completion submits correct ActivityResolved command**
    - For any valid token and result, verify `complete_activity_task` submits `Command::ActivityResolved` with `Completed` resolution, matching `activity_id`, and `worker_identity` present
    - **Validates: Requirements 4.2, 4.4**

- [x] 8. Implement `fail_activity_task` with retry logic
  - [x] 8.1 Implement `fail_activity_task` on `TokeiraRuntime`
    - Accept `token: ActivityTaskToken`, `failure_message: String`, `failure_error_type: Option<String>`
    - Validate token
    - Load activity's `RetryPolicy` from `ActivityState.retry_policy` (per-activity), falling back to `WorkflowState.retry_policy` (workflow-level) if not set
    - Call `evaluate_activity_retry` to decide retry vs exhausted
    - If retry: publish new task to `activity_broker` with `attempt + 1`, same `run_key`/`activity_id`/`schedule_event_id`/`queue`
    - If exhausted: submit `Command::ActivityResolved` with `ActivityResolution::Failed { message }` via the lane
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.6_

  - [x] 8.2 Write property test: Re-dispatch preserves identity with incremented attempt
    - **Property 9: Re-dispatch preserves identity with incremented attempt**
    - For any failure where retry is permitted, verify the re-dispatched task has same `run_key`, `activity_id`, `schedule_event_id`, `queue` but `attempt + 1`; verify no `ActivityResolved` command submitted
    - **Validates: Requirements 5.3, 6.6**

- [x] 9. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Wire `RuntimeDispatchPublisher` to activity broker
  - [x] 10.1 Add `activity_broker: InMemoryActivityBroker` field to `RuntimeDispatchPublisher` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Update constructor to accept the activity broker
    - _Requirements: 7.1, 7.3_

  - [x] 10.2 Replace `EnqueueActivityTask` stub with real activity broker call
    - In `RuntimeDispatchPublisher::publish`, match `DispatchOp::EnqueueActivityTask` and call `self.activity_broker.publish_activity_task(...)` with fields from the dispatch op
    - _Requirements: 7.1, 7.2, 7.3_

  - [x] 10.3 Add `activity_broker: InMemoryActivityBroker` field to `TokeiraRuntime`
    - Update `TokeiraRuntime::new` to create the activity broker and pass it to `RuntimeDispatchPublisher`
    - Expose `activity_broker()` accessor
    - _Requirements: 1.1, 7.1_

  - [x] 10.4 Write property test: Publisher wires EnqueueActivityTask to activity broker
    - **Property 10: Publisher wires EnqueueActivityTask to activity broker**
    - For any committed transition containing `DispatchOp::EnqueueActivityTask`, verify `RuntimeDispatchPublisher` publishes a `DispatchableActivityTask` with matching fields
    - **Validates: Requirements 7.1, 7.2**

  - [x] 10.5 Write property test: Publisher continues on activity broker failure
    - **Property 11: Publisher continues on activity broker failure**
    - When activity broker publish fails, verify publisher continues processing remaining ops and does not return error
    - **Validates: Requirements 7.4**

- [x] 11. Implement `republish_activity_queue` sweep method
  - [x] 11.1 Add `republish_activity_queue` to `TokeiraRuntime`
    - Accept `queue: QueueKey` and `limit: usize`
    - Call `repo.list_dispatchable_activity_tasks(&queue, limit)`
    - Publish each task to `activity_broker`
    - Return count of tasks republished
    - _Requirements: 9.1, 9.2, 9.3_

  - [x] 11.2 Write property test: Sweep republishes all dispatchable tasks and returns count
    - **Property 12: Sweep republishes all dispatchable tasks and returns count**
    - For any set of `DispatchableActivityTask` records in storage, verify `republish_activity_queue` publishes each to the broker and returns the correct count
    - **Validates: Requirements 9.1, 9.2, 9.3**

- [x] 12. Update re-exports in `lib.rs`
  - [x] 12.1 Update `tokeira/crates/tokeira-runtime/src/lib.rs`
    - If a new `activity_broker.rs` module was created, add `pub mod activity_broker;` and `pub use activity_broker::*;`
    - Ensure `InMemoryActivityBroker`, `StartedActivityTask`, `RetryDecision`, `evaluate_activity_retry`, `compute_retry_backoff` are publicly accessible
    - _Requirements: 1.1, 2.1_

- [x] 13. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Integration tests with InMemoryStore
  - [x] 14.1 Write integration test: schedule activity, poll, complete, verify ActivityResolved
    - Start a workflow, complete a workflow task with `ScheduleActivity` command, poll the activity task, complete it, verify `ActivityResolved(Completed)` produces correct history events
    - _Requirements: 2.2, 4.2, 7.1_

  - [x] 14.2 Write integration test: schedule activity, poll, fail with retryable error, verify re-dispatch
    - Start a workflow, schedule an activity with a retry policy, poll and fail with a retryable error, verify the activity is re-dispatched with `attempt + 1` and becomes pollable again
    - _Requirements: 5.3, 6.6_

  - [x] 14.3 Write integration test: schedule activity, poll, fail with non-retryable error, verify ActivityResolved(Failed)
    - Start a workflow, schedule an activity, poll and fail with a non-retryable error type, verify `ActivityResolved(Failed)` is submitted
    - _Requirements: 5.4, 6.3_

  - [x] 14.4 Write integration test: republish_activity_queue after restart
    - Start a workflow, schedule an activity, create a fresh `TokeiraRuntime` (simulating restart), call `republish_activity_queue`, verify the activity becomes pollable
    - _Requirements: 9.1, 9.4_

- [x] 15. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) — this is a correctness feature where the properties are the point.
- Property tests use `proptest` crate with mock `RunRepository` patterns from the existing `lane.rs` tests.
- Each property test references its design property number and the requirements it validates.
- The design uses Rust throughout; no language selection needed.
- Checkpoints are placed after pure functions, after facade methods, and at the end.
- The `start_activity_task` transaction bypasses the kernel — it commits directly to storage with empty history events and an `ActivityOp::Upsert`.
- Retry logic is a runtime concern; the kernel only sees terminal `ActivityResolved` commands.
