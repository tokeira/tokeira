# Requirements Document: Activity Pump — Dispatch, Poll, Complete, Retry

## Introduction

This document captures the requirements for the Activity Pump subsystem of `tokeira-runtime` (Feature 2 in the runtime-complete-implementation master spec). The Activity Pump is responsible for activity task dispatch from committed transitions, activity task polling by workers, activity completion and failure handling, activity retry logic, and the activity task start transaction that records starts in authoritative state.

The Activity Pump parallels the existing workflow task delivery path (InMemoryBroker + poll_workflow_task + complete_workflow_task) but differs in two key ways:

1. Activity task starts are NOT history events. Unlike WorkflowTaskStarted (which is a kernel command producing a history event), ActivityTaskStarted in Temporal is recorded retroactively when the activity resolves. The runtime tracks activity starts in its own mutable state without going through the kernel.
2. Activity retry is a runtime concern. When an activity fails and the retry policy permits retry, the runtime re-dispatches the activity with an incremented attempt count rather than submitting an ActivityResolved command to the kernel.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing), which is already implemented. The lane's DispatchPublisher trait and RuntimeDispatchPublisher are in place, with EnqueueActivityTask currently logged as a stub.

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Activity_Broker**: The in-memory delivery subsystem for activity tasks, parallel to InMemoryBroker for workflow tasks. Matches published activity tasks with waiting activity pollers. Not authoritative — the sweeper can reconstruct its state from durable storage.
- **Activity_Task_Token**: An opaque token encoding (run_key, activity_id, schedule_event_id, attempt, shard_epoch) that uniquely identifies a started activity task attempt. Used by workers to complete or fail the activity.
- **Activity_State**: The per-activity mutable state tracked in WorkflowState.activities, containing activity_id, schedule_event_id, task_queue, attempt, and timeout configuration.
- **Retry_Policy**: Configuration governing activity retry behavior: initial_interval, backoff_coefficient, maximum_interval, maximum_attempts, and non_retryable_error_types.
- **QueueKey**: Composite key (namespace_id, task_queue_name, task_kind, deployment, build_id) used to route tasks to compatible workers.
- **DispatchOp**: A value emitted by the Kernel in a committed Transition telling the runtime what task delivery action to perform.
- **Lane**: A single-thread serial command processor that routes commands to the kernel, commits transitions, and publishes dispatch ops.
- **ActivityResolution**: The terminal outcome of an activity: Completed, Failed, TimedOut, or Canceled.
- **Shard_Epoch**: Monotonically increasing fencing token for shard ownership, embedded in activity task tokens to detect stale completions.
- **DispatchableActivityTask**: The storage-level representation of an activity task ready for dispatch, carrying run_key, queue, activity_id, schedule_event_id, and attempt.

## Requirements

---

### Requirement 0: Extend ActivityState and DispatchableActivityTask with Input and Retry Policy

**User Story:** As a Tokeira developer, I want `ActivityState` to carry the activity's input payload and retry policy, and `DispatchableActivityTask` to carry the input payload, so that the runtime can deliver input to workers and evaluate per-activity retry policy without requiring additional storage lookups.

#### Acceptance Criteria

1. THE `ActivityState` struct in `tokeira-kernel` SHALL be extended with an `input: Payloads` field that stores the activity's input payload from the `ScheduleActivity` workflow command.
2. THE `ActivityState` struct SHALL be extended with a `retry_policy: Option<RetryPolicy>` field that stores the per-activity retry policy from the `ScheduleActivity` workflow command.
3. THE `ScheduleActivity` workflow command in `tokeira-kernel` SHALL be extended with a `retry_policy: Option<RetryPolicy>` field.
4. THE kernel's `apply_workflow_command` handler for `ScheduleActivity` SHALL populate `ActivityState.input` and `ActivityState.retry_policy` from the command fields.
5. THE `DispatchableActivityTask` struct in `tokeira-storage` SHALL be extended with an `input: Payloads` field.
6. THE `DispatchOp::EnqueueActivityTask` variant SHALL be extended with an `input: Payloads` field.
7. ALL existing code that constructs `ActivityState`, `DispatchableActivityTask`, or `DispatchOp::EnqueueActivityTask` SHALL be updated to populate the new fields.

---

### Requirement 1: Activity Task Broker

**User Story:** As a Tokeira developer, I want an in-memory activity task broker parallel to the workflow task broker, so that activity tasks published from committed transitions can be matched with waiting activity worker pollers.

#### Acceptance Criteria

1. THE Runtime SHALL maintain an Activity_Broker that accepts published activity tasks and matches them with waiting activity pollers.
2. THE Activity_Broker SHALL key tasks and pollers by QueueKey (namespace_id, task_queue_name, activity task kind, deployment, build_id).
3. THE Activity_Broker SHALL support deduplication by the composite key (run_key, activity_id, attempt) to prevent duplicate dispatch of the same activity task attempt.
4. THE Activity_Broker SHALL support a notify/wake mechanism so that blocked pollers are woken when a new activity task is published.
5. WHEN an activity task is published with a (run_key, activity_id, attempt) triple that already exists in the Activity_Broker, THE Activity_Broker SHALL discard the duplicate silently.


### Requirement 2: Poll Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `poll_activity_task` method on TokeiraRuntime, so that activity workers can long-poll for activity tasks and receive them when available.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `poll_activity_task` method that accepts a QueueKey, worker identity (WorkerIdentity), and timeout duration.
2. WHEN a compatible activity task is available in the Activity_Broker, THE Runtime SHALL return the task to the poller.
3. WHEN no compatible activity task is available within the timeout duration, THE Runtime SHALL return None.
4. WHEN an activity task is matched to a poller, THE Runtime SHALL perform an activity-task-start transaction that records the start in authoritative Activity_State before returning the task to the worker.
5. WHEN the activity-task-start transaction succeeds, THE Runtime SHALL return a started activity task containing the Activity_Task_Token and task metadata (run_key, activity_id, task_queue, attempt, input, timeout configuration) to the poller.
6. WHEN the activity-task-start transaction fails because the activity no longer exists in the run's Activity_State after reloading (e.g., the activity was canceled or the run closed), THE Runtime SHALL discard the matched task and return None to the poller.
7. WHEN the activity-task-start transaction fails due to an OCC conflict (the run state changed concurrently but the activity may still be pending), THE Runtime SHALL retry the start transaction with bounded retries (reload state, revalidate, re-commit). If retries exhaust and the activity is still present, THE Runtime SHALL re-publish the task to the Activity_Broker rather than silently dropping it.

### Requirement 3: Activity Task Start Transaction

**User Story:** As a Tokeira developer, I want activity task starts to be recorded in authoritative mutable state, so that stale completions and duplicate starts are rejected and the runtime can track which activities are currently running.

#### Acceptance Criteria

1. WHEN an activity task is matched to a poller, THE Runtime SHALL update the Activity_State for that activity to record the start (incrementing the started attempt or updating start metadata) without submitting a kernel command or producing a history event.
2. THE Activity_Task_Token SHALL encode run_key, activity_id, schedule_event_id, attempt, and shard_epoch as a structured value.
3. WHEN a completion or failure arrives with an Activity_Task_Token whose attempt does not match the current Activity_State attempt, THE Runtime SHALL reject the request.
4. WHEN a completion or failure arrives with an Activity_Task_Token whose activity_id is not present in the run's Activity_State, THE Runtime SHALL reject the request.
5. WHEN a completion or failure arrives with an Activity_Task_Token whose shard_epoch does not match the current shard epoch, THE Runtime SHALL reject the request. Note: until shard lease ownership is implemented (Feature 11: Sweeper and Recovery), the runtime uses `ShardEpoch::ZERO` for all tokens and this check is a no-op. The field is present for forward compatibility.
6. THE Runtime SHALL record the activity start by updating the Activity_State through a storage commit (updating the activity row in the same fenced transaction model used for run state), preserving the history-as-authority invariant that all durable state is explained by committed transitions.


### Requirement 4: Complete Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `complete_activity_task` method on TokeiraRuntime, so that workers can report successful activity completion and the result is delivered to the owning workflow run.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `complete_activity_task` method that accepts an Activity_Task_Token and a result payload (Payloads).
2. WHEN the Activity_Task_Token is valid (activity_id exists, attempt matches, shard_epoch matches), THE Runtime SHALL submit an `ActivityResolved` command with a Completed resolution carrying the result payload to the owning run via the Lane.
3. WHEN the Activity_Task_Token is stale (mismatched attempt, unknown activity_id, or mismatched shard_epoch), THE Runtime SHALL reject the completion and return an error without mutating any state.
4. THE `ActivityResolved` command SHALL carry the activity_id, the Completed resolution with the result payload, and the worker_identity from the token or request context.

### Requirement 5: Fail Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `fail_activity_task` method on TokeiraRuntime, so that workers can report activity failures and the runtime can decide whether to retry or resolve the activity as failed.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `fail_activity_task` method that accepts an Activity_Task_Token, a failure message (String), and an optional list of non-retryable error type names.
2. WHEN the Activity_Task_Token is stale (mismatched attempt, unknown activity_id, or mismatched shard_epoch), THE Runtime SHALL reject the failure and return an error without mutating any state.
3. WHEN the Activity_Task_Token is valid and the Retry_Policy permits retry (current attempt is less than maximum_attempts and the failure error type is not in non_retryable_error_types), THE Runtime SHALL re-dispatch the activity to the Activity_Broker with an incremented attempt count rather than submitting an ActivityResolved command.
4. WHEN the Activity_Task_Token is valid and the Retry_Policy is exhausted (maximum_attempts reached or the failure error type is in non_retryable_error_types), THE Runtime SHALL submit an `ActivityResolved` command with a Failed resolution to the owning run via the Lane.
5. THE Runtime SHALL evaluate retry policy logic (maximum_attempts check, non_retryable_error_types check, backoff interval computation) outside the kernel, as a runtime concern.
6. WHEN the Retry_Policy has maximum_attempts set to 0, THE Runtime SHALL treat the activity as having unlimited retries (retry on every retryable failure).


### Requirement 6: Activity Retry Policy Evaluation

**User Story:** As a Tokeira developer, I want the runtime to evaluate activity retry policy logic correctly, so that failed activities are retried according to their configured policy before being resolved as failed.

#### Acceptance Criteria

1. THE Runtime SHALL obtain the Retry_Policy for an activity from the activity's `ActivityState.retry_policy` field. This requires `ActivityState` to be extended with `retry_policy: Option<RetryPolicy>` as a prerequisite to this feature. If no per-activity retry policy is set, THE Runtime SHALL fall back to the workflow-level `WorkflowState.retry_policy`.
2. WHEN maximum_attempts is greater than 0 and the current attempt number is greater than or equal to maximum_attempts, THE Runtime SHALL consider the retry policy exhausted.
3. WHEN the failure error type matches any entry in non_retryable_error_types, THE Runtime SHALL consider the retry policy exhausted regardless of the attempt count.
4. WHEN the retry policy permits retry, THE Runtime SHALL compute the next retry backoff interval using initial_interval, backoff_coefficient, and maximum_interval from the Retry_Policy.
5. THE computed backoff interval SHALL equal `initial_interval * backoff_coefficient^(attempt - 1)`, capped at maximum_interval when maximum_interval is configured.
6. WHEN re-dispatching a retried activity, THE Runtime SHALL publish the activity task to the Activity_Broker with the same queue key, run_key, activity_id, schedule_event_id, and the incremented attempt count.

### Requirement 7: Activity Dispatch Op Handling

**User Story:** As a Tokeira developer, I want the RuntimeDispatchPublisher to handle DispatchOp::EnqueueActivityTask from committed transitions, so that scheduled activities are delivered to the Activity_Broker for worker polling.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::EnqueueActivityTask, THE RuntimeDispatchPublisher SHALL publish the activity task to the Activity_Broker.
2. THE published activity task SHALL carry the queue key (QueueKey), run_key, activity_id, schedule_event_id, attempt, and timeout configuration (schedule_to_close_timeout, schedule_to_start_timeout, start_to_close_timeout, heartbeat_timeout) from the dispatch op.
3. THE RuntimeDispatchPublisher SHALL replace the current stub log statement for EnqueueActivityTask with a call to the Activity_Broker's publish method.
4. WHEN the Activity_Broker's `publish_activity_task` method returns an error, THE RuntimeDispatchPublisher SHALL log the error at warn level and continue processing remaining dispatch ops, consistent with the non-authoritative nature of the broker.


### Requirement 8: Activity Task Token Structure

**User Story:** As a Tokeira developer, I want a well-defined ActivityTaskToken type, so that activity completions and failures can be validated against authoritative state and stale tokens are rejected deterministically.

#### Acceptance Criteria

1. THE Activity_Task_Token SHALL be a structured type containing run_key (RunKey), activity_id (String), schedule_event_id (i64), attempt (u32), and shard_epoch (ShardEpoch).
2. THE Activity_Task_Token SHALL implement Clone, Debug, and PartialEq.
3. THE Activity_Task_Token SHALL be constructable from the fields available at activity-task-start time (run_key from the dispatchable task, activity_id and schedule_event_id from the Activity_State, attempt from the current attempt, shard_epoch from the current shard ownership).
4. THE Runtime SHALL use the Activity_Task_Token fields to validate incoming completions and failures against the current Activity_State before processing them.

### Requirement 9: Activity Broker Sweep Support

**User Story:** As a Tokeira developer, I want the runtime to support republishing activity tasks from durable storage, so that the sweeper can reconstruct Activity_Broker state after restart or shard failover.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `republish_activity_queue` method that reads dispatchable activity tasks from storage for a given QueueKey and publishes them to the Activity_Broker.
2. WHEN `republish_activity_queue` is called, THE Runtime SHALL call `storage.list_dispatchable_activity_tasks(queue, limit)` and publish each returned task to the Activity_Broker.
3. THE `republish_activity_queue` method SHALL return the count of tasks republished.
4. THE Activity_Broker's deduplication by (run_key, activity_id, attempt) SHALL ensure that republished tasks do not create duplicates if the task is already present in the broker.
