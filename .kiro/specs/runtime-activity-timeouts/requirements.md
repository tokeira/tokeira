# Requirements Document: Activity Heartbeat and Timeouts

## Introduction

This document captures the requirements for Feature 3 of the Tokeira runtime: Activity Heartbeat and Timeouts. This feature adds the ability for long-running activities to report progress via heartbeats, and for the runtime to detect and enforce four activity timeout types: heartbeat, schedule-to-start, start-to-close, and schedule-to-close.

Activity heartbeat is a purely runtime-side operation — it does not go through the kernel and produces no history events. The runtime maintains a last-heartbeat timestamp in its own tracking state and uses it for heartbeat timeout detection.

Activity timeouts are detected by a background scanner that periodically inspects tracked activities and submits `ActivityResolved(TimedOut)` commands through the lane when timeouts are detected. This scanner is non-authoritative: the authoritative state transition happens when the kernel processes the `ActivityResolved` command. If the scanner fires a stale timeout (activity already resolved, run already closed), the kernel rejects it harmlessly.

This feature depends on Feature 2 (Activity Pump), which provides `poll_activity_task`, `complete_activity_task`, `fail_activity_task`, the `InMemoryActivityBroker`, activity-task-start transactions, and the `ActivityTaskToken`.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **Heartbeat**: A periodic progress report from a running activity worker to the Runtime, used for timeout detection and cancellation propagation. Heartbeats are purely runtime state — no kernel involvement, no history events.
- **Activity_Timeout_Scanner**: A background task in the Runtime that periodically checks tracked activities for timeout violations and submits `ActivityResolved(TimedOut)` commands through the lane.
- **Heartbeat_Timeout**: The maximum allowed interval between consecutive heartbeats for a started activity. Configured per activity via `ActivityState.heartbeat_timeout`.
- **Schedule_To_Start_Timeout**: The maximum allowed time between activity scheduling and activity start. Configured per activity via `ActivityState.schedule_to_start_timeout`.
- **Start_To_Close_Timeout**: The maximum allowed time between activity start and activity completion. Configured per activity via `ActivityState.start_to_close_timeout`.
- **Schedule_To_Close_Timeout**: The maximum allowed time between activity scheduling and activity completion, regardless of start state. Configured per activity via `ActivityState.schedule_to_close_timeout`. This is the outer timeout that bounds the entire activity lifecycle.
- **Activity_Tracking_State**: Runtime-local state that tracks per-activity timestamps (original_scheduled_at, last_dispatched_at, started_at, last_heartbeat_at) and cancellation status needed for heartbeat processing and timeout detection. This state is ephemeral — it is not persisted and is lost on runtime restart. Reconstruction after restart is deferred to Feature 11 (Sweeper and Recovery).
- **ActivityTaskToken**: Token encoding `run_key`, `activity_id`, `schedule_event_id`, `attempt`, and `shard_epoch`, used to validate heartbeat, completion, and failure requests.
- **Cancellation_Indicator**: A boolean flag returned from `record_activity_heartbeat` indicating that the activity has a pending cancellation request. The worker uses this to initiate graceful shutdown.

## Requirements

---

### Requirement 1: Record Activity Heartbeat Endpoint

**User Story:** As a Tokeira developer, I want a `record_activity_heartbeat` endpoint, so that long-running activities can report progress and detect cancellation.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `record_activity_heartbeat` method that accepts an `ActivityTaskToken` and heartbeat details (`Payloads`).
2. WHEN a valid heartbeat is received, THE Runtime SHALL update the last-heartbeat timestamp for the activity in the Activity_Tracking_State.
3. WHEN a heartbeat is received for an activity that has a pending cancellation, THE Runtime SHALL return a Cancellation_Indicator set to true.
4. WHEN a heartbeat is received for an activity that has no pending cancellation, THE Runtime SHALL return a Cancellation_Indicator set to false.
5. WHEN a heartbeat is received with a stale token (activity not found, attempt mismatch, or shard epoch mismatch), THE Runtime SHALL reject the heartbeat with an error and SHALL NOT update any state.
6. THE Runtime SHALL NOT submit any command to the kernel or produce any history event when processing a heartbeat.

---

### Requirement 2: Activity Tracking State Management

**User Story:** As a Tokeira developer, I want the runtime to track per-activity timestamps and cancellation status, so that heartbeat processing and timeout detection have the data they need.

#### Acceptance Criteria

1. WHEN an activity task is first published to the Activity_Broker via a `DispatchOp::EnqueueActivityTask` (attempt 1), THE Runtime SHALL record the `original_scheduled_at` and `last_dispatched_at` timestamps in the Activity_Tracking_State keyed by `(run_key, activity_id)`.
2. WHEN an activity-task-start transaction succeeds, THE Runtime SHALL record the started_at timestamp in the Activity_Tracking_State for that activity.
3. WHEN a valid heartbeat is received, THE Runtime SHALL update the last_heartbeat_at timestamp in the Activity_Tracking_State for that activity.
4. WHEN an activity is resolved (completed, failed with exhausted retries, timed out, or canceled), THE Runtime SHALL remove the activity from the Activity_Tracking_State.
5. WHEN an `ActivityTaskCancelRequested` history event is committed for an activity, THE Runtime SHALL mark the activity as cancel_requested in the Activity_Tracking_State.
6. THE Activity_Tracking_State SHALL be keyed by `(run_key, activity_id)` to support lookup from both heartbeat processing and timeout scanning.
7. WHEN an activity is re-dispatched for retry (attempt > 1), THE Runtime SHALL update `last_dispatched_at` to the current time and clear `started_at` and `last_heartbeat_at`, but SHALL NOT overwrite `original_scheduled_at`. This ensures `schedule_to_close_timeout` measures from the original scheduling, while `schedule_to_start_timeout` measures from the most recent dispatch.
8. THE Activity_Tracking_State is ephemeral — it is not persisted and is lost on runtime restart. Reconstruction of tracking state after restart is deferred to Feature 11 (Sweeper and Recovery).

---

### Requirement 3: Heartbeat Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect heartbeat timeouts, so that unresponsive activities are terminated.

#### Acceptance Criteria

1. WHEN an activity has a configured heartbeat_timeout and the activity has been started and the elapsed time since the last heartbeat exceeds the heartbeat_timeout, THE Activity_Timeout_Scanner SHALL submit an `ActivityResolved` command with a `TimedOut` resolution (timeout_type "HEARTBEAT") to the owning run via the lane.
2. WHEN an activity has a configured heartbeat_timeout and the activity has been started but no heartbeat has been received, THE Activity_Timeout_Scanner SHALL use the started_at timestamp as the baseline for heartbeat timeout detection.
3. THE Activity_Timeout_Scanner SHALL only check heartbeat timeouts for activities that have a started_at timestamp in the Activity_Tracking_State.

---

### Requirement 4: Schedule-to-Start Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect schedule-to-start timeouts, so that activities stuck in the dispatch queue are timed out.

#### Acceptance Criteria

1. WHEN an activity has a configured schedule_to_start_timeout and the elapsed time since `last_dispatched_at` exceeds the timeout and the activity has no started_at timestamp, THE Activity_Timeout_Scanner SHALL submit an `ActivityResolved` command with a `TimedOut` resolution (timeout_type "SCHEDULE_TO_START") to the owning run via the lane.
2. THE Activity_Timeout_Scanner SHALL NOT check schedule-to-start timeout for activities that already have a started_at timestamp.

---

### Requirement 5: Start-to-Close Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect start-to-close timeouts, so that activities that run too long are terminated.

#### Acceptance Criteria

1. WHEN an activity has a configured start_to_close_timeout and the activity has a started_at timestamp and the elapsed time since started_at exceeds the timeout, THE Activity_Timeout_Scanner SHALL submit an `ActivityResolved` command with a `TimedOut` resolution (timeout_type "START_TO_CLOSE") to the owning run via the lane.
2. THE Activity_Timeout_Scanner SHALL only check start-to-close timeout for activities that have a started_at timestamp in the Activity_Tracking_State.

---

### Requirement 6: Schedule-to-Close Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect schedule-to-close timeouts, so that the overall activity lifecycle is bounded.

#### Acceptance Criteria

1. WHEN an activity has a configured schedule_to_close_timeout and the elapsed time since `original_scheduled_at` exceeds the timeout, THE Activity_Timeout_Scanner SHALL submit an `ActivityResolved` command with a `TimedOut` resolution (timeout_type "SCHEDULE_TO_CLOSE") to the owning run via the lane, regardless of whether the activity has been started.
2. WHEN both schedule-to-close timeout and another timeout type (heartbeat, schedule-to-start, or start-to-close) fire for the same activity in the same scan cycle, THE Activity_Timeout_Scanner SHALL submit only the schedule-to-close timeout resolution.

---

### Requirement 7: Activity Timeout Scanner Background Task

**User Story:** As a Tokeira developer, I want a background scanner that periodically checks activities for timeout violations, so that timeouts are detected without polling from external callers.

#### Acceptance Criteria

1. THE Runtime SHALL run a background task (Activity_Timeout_Scanner) that periodically iterates over all entries in the Activity_Tracking_State and checks each activity against its configured timeouts.
2. THE Activity_Timeout_Scanner SHALL use a configurable scan interval with a sensible default (e.g. 1 second).
3. THE Activity_Timeout_Scanner SHALL read timeout configuration (heartbeat_timeout, schedule_to_start_timeout, start_to_close_timeout, schedule_to_close_timeout) from the kernel's `ActivityState` via storage, not from the Activity_Tracking_State.
4. WHEN the Activity_Timeout_Scanner detects a timeout violation, THE Activity_Timeout_Scanner SHALL submit the `ActivityResolved(TimedOut)` command to the owning run's lane using the same `submit` path used by other runtime commands.
5. THE Activity_Timeout_Scanner SHALL be non-authoritative: the authoritative state transition happens when the kernel processes the `ActivityResolved` command. IF the kernel rejects the command (UnknownActivity, run closed, or activity already resolved), THE Runtime SHALL treat the rejection as a harmless no-op.
6. THE Activity_Timeout_Scanner SHALL process timeout violations in bounded batches per scan cycle to avoid starving other lane work.

---

### Requirement 8: Timeout Scanner Lifecycle

**User Story:** As a Tokeira developer, I want the timeout scanner to start and stop with the runtime, so that timeout detection is active only while the runtime is serving.

#### Acceptance Criteria

1. WHEN the Runtime is created, THE Runtime SHALL start the Activity_Timeout_Scanner background task.
2. WHEN the Runtime is shut down, THE Runtime SHALL stop the Activity_Timeout_Scanner background task gracefully.
3. IF the Activity_Timeout_Scanner encounters a transient error while loading activity state from storage, THEN THE Activity_Timeout_Scanner SHALL log the error and continue to the next scan cycle rather than crashing.
