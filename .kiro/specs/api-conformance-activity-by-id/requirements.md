# Requirements Document

## Introduction

Implement six currently-stubbed activity RPCs in the Tokeira edge layer, bringing them from `Stubbed` (returning `tonic::Status::unimplemented`) to `Implemented`. Five of the six RPCs are "ById" variants that resolve activities using `(namespace, workflow_id, run_id, activity_id)` instead of decoding an opaque task token. The sixth is the token-based `RespondActivityTaskCanceled` which is stubbed despite the Complete and Failed token paths already existing. A deprecated `UpdateActivityOptions` RPC is also included for backward compatibility.

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
- **UpdateActivityOptions**: A deprecated Temporal API that hot-patches timeout and routing options on a pending activity without canceling it.

## Target State

`ImplementedSubset`. ById activity completion/failure/cancel/heartbeat,
token-based cancel, and single-id `UpdateActivityOptions` are implemented. Bulk
`UpdateActivityOptions` targets (`type` and `match_all`) remain explicitly
unsupported and keep this spec out of full `Implemented` status.

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
4. IF the resolved activity has not started (no `started_event_id`), THEN THE Edge SHALL return a successful response with `cancel_requested` set to false without delegating to the runtime heartbeat path.
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

**User Story:** As an operator using a legacy SDK, I want to update timeout and routing options on a running activity, so that I can adjust activity behavior without canceling and re-scheduling it.

#### Acceptance Criteria

1. WHEN an `UpdateActivityOptions` request is received with valid identifiers and at least one changed option, THE Edge SHALL delegate to the kernel's `UpdateActivityOptions` command.
2. THE Edge SHALL support updating the following fields: `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, `heartbeat_timeout`, and `task_queue`.
3. IF the referenced activity does not exist in the resolved run, THEN THE Edge SHALL return a gRPC `NOT_FOUND` status.
4. WHEN the update is committed, THE Edge SHALL return a response containing the updated `ActivityOptions` reflecting the new values.
5. IF the request targets activities by `type` or `match_all` rather than a single `activity_id`, THEN THE Edge SHALL return a gRPC `UNIMPLEMENTED` status indicating that bulk activity option updates are not yet supported.
6. IF the request targets a scheduled but not-yet-started activity, THEN THE Edge SHALL allow the update to proceed because activity options are attached to the pending activity state and do not require a started activity token.

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
