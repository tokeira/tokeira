# Requirements Document: Edge Poll Response Fidelity

## Introduction

This spec addresses the highest-priority SDK correctness gaps in the poll response and start response translation layer. The proto field audit (`../edge-complete-implementation/reference/proto-field-audit.md`) identified four categories of missing fields that directly affect SDK behavior: `previous_started_event_id` (critical for sticky replay), the `started` field on `StartWorkflowExecutionResponse`, `WorkflowTaskScheduled` event attributes, and poll response timestamps. Without these fields, the SDK replays from the beginning of history on every workflow task, cannot distinguish new vs existing workflows, and lacks the data needed for latency computation and timeout enforcement.

This is Feature 1 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features and is the highest priority among the field fidelity gaps.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **History_Serializer**: The module `tokeira-edge/src/translate/history_serializer.rs` that converts kernel `HistoryEvent` values into proto `temporal.api.history.v1.History` messages.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Poll_Response**: The proto `PollWorkflowTaskQueueResponse` returned to SDK workers when they poll for workflow tasks.
- **Start_Response**: The proto `StartWorkflowExecutionResponse` returned when a client starts a workflow.
- **WFT**: Workflow Task — the unit of work dispatched to an SDK worker for replay and command generation.
- **StartedWorkflowTask**: The runtime struct (`tokeira_runtime::StartedWorkflowTask`) that carries metadata about a workflow task that has been polled and started.
- **PendingWorkflowTask**: The kernel struct (`tokeira_kernel::state::PendingWorkflowTask`) that tracks the currently pending workflow task for a run.
- **TransitionBuilder**: The internal kernel helper that accumulates events, state mutations, and dispatch ops during a single command application.
- **WorkflowTaskScheduled**: The history event emitted when the kernel places a workflow task on the task queue.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

### Requirement 1: PollWorkflowTaskQueueResponse — previous_started_event_id

**User Story:** As an SDK user, I want `PollWorkflowTaskQueueResponse` to include `previous_started_event_id`, so that the SDK can determine the sticky replay boundary and avoid replaying the entire history on every workflow task.

#### Acceptance Criteria

1. WHEN the Kernel completes a workflow task, THE Kernel SHALL record the `started_event_id` of that completed task as the `previous_started_event_id` for the next workflow task.
2. WHEN the Kernel schedules a new workflow task and a previous workflow task has been completed, THE Kernel SHALL include the `previous_started_event_id` in the `WorkflowTaskScheduled` event data or in the `PendingWorkflowTask` state.
3. WHEN the workflow task is the first task for the run (no previous completion exists), THE Kernel SHALL set `previous_started_event_id` to 0.
4. WHEN the Runtime constructs a `StartedWorkflowTask`, THE Runtime SHALL include the `previous_started_event_id` value from the run's authoritative state.
5. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate proto field 4 (`previous_started_event_id`) from the `StartedWorkflowTask` metadata.
6. FOR ANY workflow that has completed at least one WFT, the next poll response's `previous_started_event_id` SHALL equal the `started_event_id` of the most recently completed WFT.
7. WHEN a workflow task fails or times out (without completing), THE Kernel SHALL NOT update `previous_started_event_id`. Only successful completions advance this field. This matches the Temporal server's `LastCompletedWorkflowTaskStartedEventId` semantics — the field tracks the last *completed* WFT, not the last *started* WFT.

### Requirement 2: StartWorkflowExecutionResponse — started field

**User Story:** As an SDK user, I want `StartWorkflowExecutionResponse` to include the `started` field, so that the SDK can distinguish "started new workflow" from "returned existing workflow" when using conflict policies.

#### Acceptance Criteria

1. WHEN a new workflow execution is created by a `StartWorkflowExecution` request, THE Edge_Layer SHALL set the `started` field to `true` on the `StartWorkflowExecutionResponse`.
2. WHEN a `StartWorkflowExecution` request returns an existing execution due to a conflict policy (e.g., `UseExisting`), THE Edge_Layer SHALL set the `started` field to `false`.
3. THE `StartWorkflowExecutionResponse` edge DTO SHALL carry a `started` boolean field so the gRPC translate layer can populate the proto field.

### Requirement 3: WorkflowTaskScheduled — task_queue, start_to_close_timeout, attempt

**User Story:** As an SDK user, I want `WorkflowTaskScheduledEventAttributes` in history to include `task_queue`, `start_to_close_timeout`, and `attempt`, so that the SDK has complete information during replay and the runtime can enforce workflow task timeouts.

#### Acceptance Criteria

1. WHEN the Kernel emits a `WorkflowTaskScheduled` event, THE Kernel SHALL include the task queue name, workflow task timeout, and attempt count in the `HistoryEventKind::WorkflowTaskScheduled` variant.
2. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `task_queue` proto field with a `TaskQueue` message containing the run's task queue name.
3. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `start_to_close_timeout` proto field from the run's `workflow_task_timeout`.
4. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `attempt` proto field from the pending workflow task's attempt count.
5. FOR ANY `WorkflowTaskScheduled` event in a serialized history, the proto attributes SHALL have non-default `task_queue`, `start_to_close_timeout`, and `attempt` fields.
6. WHEN a workflow task has been started and the elapsed time since `started_time` exceeds the `start_to_close_timeout`, THE Runtime SHALL submit a `Command::WorkflowTaskTimedOut` to the kernel for the run. The kernel already handles this command — the runtime is responsible for detecting the timeout condition and firing it.
7. THE Runtime SHALL run a background scanner (or integrate with an existing scanner) that periodically checks started workflow tasks for start-to-close timeout violations, using the `started_time` and `start_to_close_timeout` values recorded on the run's authoritative state.

### Requirement 4: PollWorkflowTaskQueueResponse — scheduled_time and started_time

**User Story:** As an SDK user, I want `PollWorkflowTaskQueueResponse` to include `scheduled_time` and `started_time`, so that the SDK can compute task latency and the runtime can enforce workflow task timeouts accurately.

#### Acceptance Criteria

1. WHEN the Runtime constructs a `StartedWorkflowTask`, THE Runtime SHALL include the `scheduled_time` (timestamp of the `WorkflowTaskScheduled` event) and `started_time` (timestamp of the `WorkflowTaskStarted` event) from the run's history.
2. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate `scheduled_time` (proto field 12) with the timestamp from the `WorkflowTaskScheduled` event.
3. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate `started_time` (proto field 13) with the timestamp from the `WorkflowTaskStarted` event.
4. THE `scheduled_time` and `started_time` SHALL be encoded as `google.protobuf.Timestamp` values using the standard well-known type conversion helpers.
5. WHEN the Runtime records a `started_time` for a workflow task, THE Runtime SHALL use that timestamp together with the run's `workflow_task_timeout` (start-to-close) to compute the deadline for WFT timeout enforcement. If the worker does not complete the task before `started_time + workflow_task_timeout`, the runtime SHALL submit a `Command::WorkflowTaskTimedOut`.
6. THE Runtime SHALL persist `started_time` in the run's authoritative state (via the kernel's `PendingWorkflowTask`) so that timeout enforcement survives runtime restarts and shard failover. On shard acquisition, the sweeper SHALL repopulate `WftTimeoutTrackingState` from durable storage by querying for runs with started WFTs, following the same recovery pattern as `WorkflowTimeoutTrackingState` and `ActivityTrackingState`.
