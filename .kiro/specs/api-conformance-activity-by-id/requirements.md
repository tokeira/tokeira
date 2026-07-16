# Requirements Document

## Introduction

Implement workflow-scoped activity API conformance in the Tokeira edge/runtime path. The original scope
covered the ById response RPCs and token-based cancellation; this revision also accounts for
`UpdateActivityOptions`, `PauseActivity`, `UnpauseActivity`, and `ResetActivity`, which Temporal
v1.31.0 serves as public RPCs. Their API comments announce a future deprecation, but API v1.62.8
contains neither a formal deprecation marker nor the named replacement RPCs. They are therefore
part of the v1.31.0 behavioural target rather than excluded aliases.

This is child spec #1 of the `api-conformance-tracker` umbrella.

## Glossary

- **Edge**: The compatibility layer (`tokeira-edge`) that admits gRPC requests, translates them to domain types, and delegates to the runtime. Does not own workflow semantics.
- **Runtime**: The authoritative execution layer (`tokeira-runtime`) that owns activity lifecycle, heartbeat tracking, and transition submission.
- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that applies transitions and emits history events. No I/O, no async.
- **ActivityTaskToken**: A structured token containing `(run_key, activity_id, schedule_event_id, attempt, shard_epoch)` that identifies an in-flight activity task.
- **ById_Resolution**: The process of resolving `(namespace, workflow_id, run_id, activity_id)` to a `RunKey` via the edge's execution-home resolution, then asking the runtime to construct an `ActivityTaskToken` from authoritative activity state.
- **RunKey**: A UUID that uniquely identifies a workflow run in storage.
- **Task_Token**: An opaque byte sequence that SDKs pass back to the server to identify which task they are responding to.
- **Heartbeat**: A periodic signal from an activity worker indicating the activity is still alive. Returns whether cancellation has been requested.
- **Activity_Resolution**: A kernel command that resolves a pending activity as Completed, Failed, or Canceled.
- **UpdateActivityOptions**: A workflow-scoped API that hot-patches the options on one or more
  pending activities without adding history events.
- **Activity pause**: Durable pending-activity metadata that suppresses a future retry/start while
  allowing an already-running attempt to finish.
- **Activity reset**: A workflow-scoped mutation that resets attempt/timer state and optionally
  heartbeat progress, pause state, and original options.

## Target State

`Implemented`. All v1.31.0 workflow-scoped activity targets and option fields are handled with the
same lifecycle behavior, validation, and response semantics as the targeted server. Fields present
only in the newer vendored API remain governed by the two-pin compatibility policy and do not
silently broaden the v1.31.0 claim.

## Evidence From Current Code

- **Wire shape:** `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` and
  `service.proto`; Temporal v1.31.0 shipped API v1.62.8.
- **Target behavior:** `service/frontend/workflow_handler.go`,
  `service/history/api/{pauseactivity,unpauseactivity,resetactivity,updateactivityoptions}/api.go`,
  and `service/history/workflow/activity.go @ v1.31.0`.
- **Current Tokeira substrate:** command types in `crates/tokeira-kernel/src/command.rs`, transitions
  in `crates/tokeira-kernel/src/kernel.rs`, durable heartbeat details in
  `crates/tokeira-kernel/src/state.rs`, and public stubs/wiring in
  `crates/tokeira-edge/src/grpc/workflow_service.rs`.
- **Known mismatch to correct:** the existing reset command cannot represent `keep_paused`, jitter,
  restore-original options, or the next-instance heartbeat-reset marker; the existing unpause
  transition rejects an already-unpaused activity although v1.31.0 treats it as a no-op.
- **Ownership correction:** the kernel stores and deterministically mutates pause/reset flags, while
  `commit_activity_retry` consumes those flags during retry preparation and the raw runtime
  heartbeat transition persists heartbeat details and projects response flags.
- **Token validation:** `RecordActivityTaskHeartbeat`, Complete, Failed, and Canceled all call
  `IsActivityTaskNotFoundForToken`; for a non-empty scheduled event id it rejects an attempt mismatch
  (`service/history/api/activity_util.go:58-78 @ v1.31.0`). Reset does not create an exception to this
  shared validator.
- **Spec ownership:** this revision supersedes stale activity-management assumptions without
  reopening the completed `kernel-pause-activity-management` Feature-11 record.

## Activity-Control Field Policy

### `PauseActivityRequest`

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `namespace` | Resolve the namespace | Namespace status from the standard resolver | None before resolution |
| `execution` | Require workflow id; empty run id selects current run | `INVALID_ARGUMENT` for missing workflow id | Selects one authoritative run |
| `identity` | Store in manual pause info | None | Audit metadata only |
| `activity.id` | Pause the matching pending activity | Activity-not-found when absent | Mutates one pending activity |
| `activity.type` | Pause every pending activity of the type | Activity-not-found when no match | Mutates all matches |
| `reason` | Store in manual pause info | None | Audit metadata only |

### `ResetActivityRequest`

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `namespace` | Resolve the namespace | Namespace status from the standard resolver | None before resolution |
| `execution` | Require workflow id; empty run id selects current run | `INVALID_ARGUMENT` for missing workflow id | Selects one authoritative run |
| `identity` | Preserve as caller identity | None | Audit/attribution only |
| `activity.id` | Reset the matching pending activity | Activity-not-found when absent | Mutates one pending activity |
| `activity.type` | Reset every pending activity of the type | Activity-not-found when no match | Mutates all matches |
| `reset_heartbeat` | Clear heartbeat state for the new instance | None | Sets durable reset intent and clears on retry/start |
| `keep_paused` | Preserve pause only when true | None | Otherwise clears pause on a scheduled activity |
| `jitter` | Schedule a retry in the half-open interval `[now, now+jitter)` | Invalid duration conversion | Regenerates retry dispatch/timers |
| `restore_original_options` | Reload options from the first schedule event | `INVALID_ARGUMENT` if that event is absent/invalid | Restores task queue, timeouts, and retry policy |

## Terminal-State Error Policy

| Condition | Expected behavior |
|---|---|
| Activity already completed/failed/canceled before ById resolve | `ActivityNotFound` or terminal-specific `FAILED_PRECONDITION`, matching token-path semantics |
| Duplicate terminal token response | Preserve existing token-path idempotency/error behavior |
| ById heartbeat for scheduled-but-not-started activity | Success with `cancel_requested = false`; compare against Temporal behavior in conformance tests |
| Cancel details accepted | Details must be emitted in `ActivityTaskCanceled` history event |

## Requirements

### Requirement 1: Resolve Activity by ID

**User Story:** As an SDK developer, I want to address activities by `(namespace, workflow_id, run_id, activity_id)` instead of a task token, so that external systems can interact with activities without holding the original token.

#### Acceptance Criteria

1. WHEN a ById activity RPC is received with `(namespace, workflow_id, run_id, activity_id)`, THE Edge SHALL resolve the workflow execution to a `RunKey` using the same execution-home resolution path used by other ById RPCs.
2. IF the `(namespace, workflow_id, run_id)` tuple does not resolve to an existing execution, THEN THE Edge SHALL return a gRPC `NOT_FOUND` status with a descriptive message.
3. IF the `activity_id` does not correspond to a pending activity in the resolved run, THEN THE Edge SHALL return a gRPC `NOT_FOUND` status indicating the activity was not found.
4. WHEN the `run_id` field is empty, THE Edge SHALL resolve to the current (latest) run for the given `(namespace, workflow_id)`.
5. IF the `run_id` field is non-empty but does not parse as a valid `RunId`, THEN THE Edge SHALL return a gRPC `INVALID_ARGUMENT` status.
6. IF the `activity_id` corresponds to a scheduled but not-yet-started activity and the RPC attempts to complete, fail, or cancel that activity, THEN THE Edge SHALL return a gRPC `FAILED_PRECONDITION` status indicating the activity has not started.

### Requirement 2: Record Activity Task Heartbeat By ID

**User Story:** As an SDK developer, I want to record heartbeats for an activity using its workflow and activity identifiers, so that long-running activities can report liveness without holding the original task token.

#### Acceptance Criteria

1. WHEN a `RecordActivityTaskHeartbeatById` request is received with valid identifiers for a started activity, THE Edge SHALL delegate to the same runtime heartbeat path used by the token-based `RecordActivityTaskHeartbeat`.
2. WHEN the heartbeat succeeds, THE Edge SHALL return a response containing the `cancel_requested` flag reflecting whether cancellation has been requested for the activity.
3. IF the resolved activity has started but is not currently tracked by the runtime heartbeat store, THEN THE Edge SHALL return a successful response with `cancel_requested` set to false.
4. IF the resolved activity has not started (no `started_event_id`), THEN THE Edge SHALL return a gRPC `NOT_FOUND` status, matching v1.31.0: the by-id heartbeat builds a token with an empty scheduled/started event id and calls the same history RPC as the token path, where `IsActivityTaskNotFoundForToken` (`service/history/api/activity_util.go:58 @ v1.31.0`, invoked with a nil `isCompletedByID`) returns not-found whenever `StartedEventId` is empty. There is no by-id exemption, and the token-based heartbeat path already rejects an unstarted activity the same way.
5. WHEN a `RecordActivityTaskHeartbeatById` request includes a `details` payload, THE Edge SHALL pass the details to the runtime heartbeat path.
6. THE token-based and ById heartbeat handlers SHALL share a runtime heartbeat API that accepts the optional heartbeat `details` payload, so both paths preserve identical heartbeat metadata.

### Requirement 3: Respond Activity Task Completed By ID

**User Story:** As an SDK developer, I want to complete an activity using its workflow and activity identifiers, so that external completion systems can report results without holding the original task token.

#### Acceptance Criteria

1. WHEN a `RespondActivityTaskCompletedById` request is received with valid identifiers and a result payload, THE Edge SHALL delegate to the same runtime activity completion path used by the token-based `RespondActivityTaskCompleted`.
2. WHEN the activity completion is committed, THE Edge SHALL return a successful empty response.
3. WHEN the activity completion is committed, THE Edge SHALL notify the history lane so that the workflow task is scheduled promptly.

### Requirement 4: Respond Activity Task Failed By ID

**User Story:** As an SDK developer, I want to fail an activity using its workflow and activity identifiers, so that external systems can report failures without holding the original task token.

#### Acceptance Criteria

1. WHEN a `RespondActivityTaskFailedById` request is received with valid identifiers and a failure payload, THE Edge SHALL delegate to the same runtime activity failure path used by the token-based `RespondActivityTaskFailed`.
2. WHEN the activity failure triggers a retry (per the activity's retry policy), THE Runtime SHALL re-dispatch the activity at the next attempt.
3. WHEN the activity failure is terminal (no more retries), THE Runtime SHALL resolve the activity as failed and schedule a workflow task.

### Requirement 5: Respond Activity Task Canceled By ID

**User Story:** As an SDK developer, I want to confirm activity cancellation using its workflow and activity identifiers, so that external systems can acknowledge cancellation without holding the original task token.

#### Acceptance Criteria

1. WHEN a `RespondActivityTaskCanceledById` request is received with valid identifiers and an optional details payload, THE Edge SHALL resolve the activity as canceled via the kernel's `ActivityResolution::Canceled` path.
2. WHEN the cancellation is committed, THE Edge SHALL return a successful empty response.
3. WHEN the cancellation is committed, THE Edge SHALL notify the history lane so that the workflow task is scheduled promptly.

### Requirement 6: Respond Activity Task Canceled (Token-Based)

**User Story:** As an SDK developer, I want to confirm activity cancellation using a task token, so that the standard SDK cancellation acknowledgment flow works end-to-end.

#### Acceptance Criteria

1. WHEN a `RespondActivityTaskCanceled` request is received with a valid task token and an optional details payload, THE Edge SHALL resolve the activity as canceled via the kernel's `ActivityResolution::Canceled` path.
2. WHEN the cancellation is committed, THE Edge SHALL return a successful empty response.
3. WHEN the cancellation is committed, THE Edge SHALL notify the history lane so that the workflow task is scheduled promptly.
4. IF the task token is malformed or does not decode to a valid `ActivityTaskToken`, THEN THE Edge SHALL return a gRPC `INVALID_ARGUMENT` status.

### Requirement 7: Update Activity Options

**User Story:** As an operator, I want to update options on pending activities, so that I can adjust
their behavior without canceling and re-scheduling them.

#### Acceptance Criteria

1. WHEN an `UpdateActivityOptions` request is received with valid identifiers and at least one changed option, THE Edge SHALL delegate to the kernel's `UpdateActivityOptions` command.
2. THE activity-options path SHALL support field-mask updates for task queue, all four activity
   timeouts, and every retry-policy field served by v1.31.0.
3. IF the referenced activity does not exist in the resolved run, THEN THE Edge SHALL return a gRPC `NOT_FOUND` status.
4. WHEN the update is committed, THE Edge SHALL return a response containing the updated `ActivityOptions` reflecting the new values.
5. WHEN the request targets an activity type, THE runtime SHALL update every pending activity of
   that type.
6. IF the request targets a scheduled but not-yet-started activity, THEN THE Edge SHALL allow the update to proceed because activity options are attached to the pending activity state and do not require a started activity token.
7. WHEN `restore_original` is true, THE runtime SHALL restore options from the activity's first
   `ActivityTaskScheduled` event.
8. IF `restore_original` is combined with an update mask or replacement options, THEN THE Edge SHALL
   return `INVALID_ARGUMENT` before mutation.

### Requirement 8: ById Token Construction

**User Story:** As a system maintainer, I want the runtime to construct a valid `ActivityTaskToken` from authoritative activity state, so that ById handlers reuse the existing token validation and completion paths without duplicating shard-epoch logic in the edge.

#### Acceptance Criteria

1. WHEN a ById handler resolves an execution to a `RunKey`, THE Edge SHALL pass the `run_key` and `activity_id` to the runtime for token construction.
2. THE Runtime SHALL construct an `ActivityTaskToken` with `run_key`, `activity_id`, `schedule_event_id`, `attempt`, and the current shard epoch from runtime-owned shard state.
3. IF the kernel state for the activity cannot be read (run not loaded or activity missing), THEN THE Edge SHALL return a gRPC `NOT_FOUND` status before attempting activity mutation delegation.
4. IF runtime token resolution returns `RunNotFound`, THEN THE Edge SHALL map it to the same gRPC `NOT_FOUND` response used for unresolved workflow executions.

### Requirement 9: Identity Propagation

**User Story:** As an operator, I want the `identity` field from ById requests to be propagated through to the runtime, so that activity completion/failure events record which worker or system performed the action.

#### Acceptance Criteria

1. WHEN a ById activity RPC includes a non-empty `identity` field, THE Edge SHALL propagate the identity to the runtime as the `worker_identity` parameter.
2. WHEN a ById activity RPC has an empty `identity` field, THE Edge SHALL pass `None` as the `worker_identity` to the runtime.

### Requirement 10: Pause Workflow-Scoped Activities

**User Story:** As an operator, I want to pause a pending activity by id or type, so that a retry or
future start is held without aborting an attempt that is already running.

#### Acceptance Criteria

1. WHEN `PauseActivity` targets an existing unpaused scheduled activity, THE runtime SHALL persist
   manual pause info and fence its outstanding dispatch.
2. WHEN `PauseActivity` targets an already-paused activity, THE runtime SHALL return success without
   changing its pause metadata or stamp.
3. WHEN `PauseActivity` targets a running activity, THE runtime SHALL allow that running attempt to
   complete successfully.
4. WHEN a running paused activity fails with a retry remaining, THE runtime SHALL retain it paused.
5. WHEN a heartbeat is recorded for a paused running activity, THE response SHALL set
   `activity_paused` to true.
6. WHEN `PauseActivity` targets an activity type, THE runtime SHALL pause every pending activity of
   that type.
7. IF no pending activity matches the selected id or type, THEN THE Edge SHALL return the
   v1.31.0 activity-not-found status.
8. WHEN `PauseActivity` succeeds, THE runtime SHALL add no workflow history event and schedule no
   workflow task.
9. WHEN a running paused activity is parked after failure, THE runtime SHALL clear its started event,
   start version, started time, and request id.
10. WHEN a running paused activity is parked after failure, THE runtime SHALL suppress retry
    dispatch.
11. WHEN a running paused activity is parked after failure, THE runtime SHALL increment its attempt.

### Requirement 11: Reset Workflow-Scoped Activities

**User Story:** As an operator, I want to reset pending activities by id or type, so that attempt,
timer, heartbeat, pause, and original-option state can be restarted predictably.

#### Acceptance Criteria

1. WHEN `ResetActivity` targets a scheduled retry, THE runtime SHALL reset its attempt to one and
   regenerate dispatch immediately unless jitter or retained pause delays it.
2. WHEN `ResetActivity` targets a running activity, THE runtime SHALL reset the future-attempt state
   without dispatching a concurrent replacement attempt.
3. WHEN `reset_heartbeat` is true on a running activity, THE kernel SHALL persist next-instance
   heartbeat-reset intent without clearing heartbeat details from the current attempt.
4. WHEN `keep_paused` is true for a paused activity, THE runtime SHALL keep it paused after reset.
5. WHEN `keep_paused` is false for a paused scheduled activity, THE runtime SHALL clear the pause and
   make it eligible for dispatch.
6. WHEN `restore_original_options` is true, THE runtime SHALL restore task queue, timeouts, and retry
   policy from the first schedule event.
7. WHEN a positive jitter is supplied for a dispatchable reset, THE runtime SHALL choose a schedule
   time in the half-open interval `[now, now+jitter)`.
8. WHEN `ResetActivity` targets an activity type, THE runtime SHALL reset every pending activity of
   that type.
9. IF no pending activity matches the selected id or type, THEN THE Edge SHALL return the v1.31.0
   activity-not-found status.
10. WHEN `ResetActivity` succeeds, THE runtime SHALL add no workflow history event and schedule no
    workflow task.
11. WHEN a post-reset heartbeat token still matches the activity attempt and start version, THE
    runtime SHALL accept the heartbeat and return `activity_reset = true`.
12. IF a post-reset heartbeat or completion token's attempt no longer matches the reset activity,
    THEN THE runtime SHALL reject it as activity-task-not-found through the shared v1.31.0 validator.
13. WHEN retry preparation advances a reset activity to its next attempt, THE runtime SHALL clear
    heartbeat details when next-instance reset intent is set.
14. WHEN retry preparation advances a reset activity to its next attempt, THE runtime SHALL clear
    both the activity-reset and heartbeat-reset intent flags.

### Requirement 12: Unpause Workflow-Scoped Activities

**User Story:** As an operator, I want to resume paused activities, so that held work can continue
with optional attempt, heartbeat, and jitter reset behavior.

#### Acceptance Criteria

1. WHEN `UnpauseActivity` targets a paused scheduled activity, THE runtime SHALL clear pause state
   and regenerate dispatch.
2. WHEN `UnpauseActivity` targets an activity that is not paused, THE runtime SHALL return success
   without changing it.
3. WHEN `reset_attempts` is true, THE runtime SHALL reset the activity attempt to one.
4. WHEN `reset_heartbeat` is true, THE runtime SHALL clear heartbeat details and heartbeat timing.
5. WHEN positive jitter is supplied, THE runtime SHALL choose the resumed schedule time in the
   half-open interval `[now, now+jitter)`.
6. WHEN the target is an activity type or all activities, THE runtime SHALL apply the operation to
   every matching pending activity.
7. IF no pending activity matches the selected id or type, THEN THE Edge SHALL return the v1.31.0
   activity-not-found status.
8. WHEN `UnpauseActivity` succeeds, THE runtime SHALL add no workflow history event and schedule no
   workflow task.
9. WHEN a paused activity is actually unpaused, THE kernel SHALL increment its activity stamp even
   if the activity is currently running.
