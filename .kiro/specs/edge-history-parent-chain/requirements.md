# Requirements Document: Edge History Parent Chain

## Introduction

This document captures the requirements for completing the history event field gaps identified in `docs/proto-field-audit.md` §3 that affect replay correctness and execution chain tracing. The kernel already carries parent metadata (`parent_run_key`, `parent_workflow_id`, `first_execution_run_id`) and execution chain fields (`continued_execution_run_id`) on `StartRequest` and `WorkflowState`, but these are not threaded into the `WorkflowExecutionStarted` event or serialized to proto. Similarly, `WorkflowExecutionContinuedAsNew` drops several fields the serializer has access to, and the `control` field on signal-external and cancel-external events is absent.

This is Feature 3 from the umbrella spec `edge-complete-implementation`. It depends on Feature 1 (poll response fidelity) for the `WorkflowTaskScheduled` attribute overlap, and on Feature 2 (failure objects) for the opaque `Payload` failure representation used by `continued_failure`.

The scope covers four requirement groups:
1. Parent and chain fields on `WorkflowExecutionStarted`
2. Missing fields on `WorkflowExecutionContinuedAsNew`
3. Verification that `ActivityTaskScheduled` timeout serialization is already complete
4. The `control` field on signal-external and cancel-external initiated events

`cron_schedule` on `WorkflowExecutionStarted` is deferred to Feature 6 (schedules).

## Glossary

- **Kernel**: The pure state-machine module (`tokeira-kernel`) that computes workflow state transitions with zero I/O.
- **History_Serializer**: The module `tokeira-edge/src/translate/history_serializer.rs` that converts kernel `HistoryEvent` values into proto `temporal.api.history.v1.History` messages.
- **StartRequest**: The `tokeira_kernel::command::StartRequest` struct that carries all parameters needed to bootstrap a new workflow run.
- **WorkflowState**: The `tokeira_kernel::state::WorkflowState` struct that holds the durable summary state for an open or closed workflow run.
- **ContinueAsNew_Event**: The `HistoryEventKind::WorkflowExecutionContinuedAsNew` variant emitted when a workflow closes by spawning a successor run.
- **Started_Event**: The `HistoryEventKind::WorkflowExecutionStarted` variant that is always the first event in a run's history.
- **Predecessor_Run**: The workflow run that closed via continue-as-new or retry, creating the current run as its successor.
- **Original_Execution_Run_Id**: The run ID of the very first run in the execution chain, before any retries or continue-as-new. For the first run, it equals the run's own run ID.
- **First_Execution_Run_Id**: The run ID of the first run in the current retry chain. Resets on continue-as-new.
- **Parent_Initiated_Event_Id**: The event ID of the `StartChildWorkflowExecutionInitiated` event in the parent workflow's history.
- **Control_Field**: A string field on signal-external and cancel-external initiated events used by SDKs to correlate initiated events with workflow commands during replay. Typically the command's sequence number.
- **ContinueAsNew_Initiator**: An enum indicating what triggered the continue-as-new: a workflow command, a retry after failure, or a cron schedule.
- **Opaque_Failure_Payload**: A `Payload` with encoding `temporal/failure+proto` that carries a complete proto `Failure` as opaque bytes through the kernel (established by Feature 2).
- **Runtime**: The `tokeira-runtime` crate that orchestrates I/O, reads predecessor state, and threads data into kernel commands.
- **Lane**: The runtime module (`tokeira-runtime/src/lane.rs`) that processes committed transitions and handles continue-as-new successor creation.
- **DispatchOp**: A side-effect instruction emitted by the kernel as part of a `Transition`, executed by the runtime's publisher.

## Requirements

---

### Requirement 1: WorkflowExecutionStarted — parent workflow metadata

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include parent workflow information (`parent_workflow_execution`, `parent_workflow_namespace`, `parent_workflow_namespace_id`, `parent_initiated_event_id`), so that the SDK can correctly identify child workflows and trace parent-child relationships during replay.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a Started_Event for a child workflow, THE History_Serializer SHALL populate `parent_workflow_execution` with the parent's workflow ID and run ID.
2. WHEN the History_Serializer serializes a Started_Event for a child workflow, THE History_Serializer SHALL populate `parent_workflow_namespace_id` from the parent's namespace ID. The `parent_workflow_namespace` (human-readable name) SHALL be left empty because the kernel/runtime path carries namespace IDs, not namespace names, and the history serializer does not have a namespace-name lookup at serialization time. This matches the proto comment: "server must use `parent_workflow_namespace_id` only."
3. WHEN the History_Serializer serializes a Started_Event for a child workflow, THE History_Serializer SHALL populate `parent_initiated_event_id` with the event ID of the `StartChildWorkflowExecutionInitiated` event in the parent's history.
4. WHEN the History_Serializer serializes a Started_Event for a non-child workflow (no parent), THE History_Serializer SHALL leave `parent_workflow_execution` as None, `parent_workflow_namespace_id` as empty, and `parent_initiated_event_id` as 0.
5. WHEN the Kernel emits a Started_Event, THE Kernel SHALL include parent workflow ID, parent run ID, parent namespace ID, and Parent_Initiated_Event_Id in the event data, sourced from the StartRequest.
6. WHEN the Runtime creates a StartRequest for a child workflow, THE Runtime SHALL populate `parent_namespace_id`, `parent_run_id`, and `parent_initiated_event_id` from the `DispatchOp::StartChildWorkflow` fields.

### Requirement 2: WorkflowExecutionStarted — original_execution_run_id

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include `original_execution_run_id`, so that the SDK can trace back to the very first run in the execution chain across retries and continue-as-new.

#### Acceptance Criteria

1. THE History_Serializer SHALL populate `original_execution_run_id` with the run ID of the very first run in the execution chain (before any retries or continue-as-new).
2. WHEN the workflow run is the first run in the chain (no predecessor), THE Kernel SHALL set `original_execution_run_id` equal to the run's own run ID.
3. WHEN the Runtime creates a StartRequest for a continue-as-new successor or retry, THE Runtime SHALL propagate `original_execution_run_id` from the Predecessor_Run's state.
4. WHEN the Kernel emits a Started_Event, THE Kernel SHALL include `original_execution_run_id` in the event data, sourced from the StartRequest.
5. THE StartRequest SHALL carry an `original_execution_run_id: Option<RunId>` field, and THE WorkflowState SHALL carry an `original_execution_run_id: Option<RunId>` field.

### Requirement 3: WorkflowExecutionStarted — continued_failure

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include `continued_failure` when the predecessor run failed before continuing-as-new, so that the SDK can access the failure from the previous run.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a Started_Event for a run whose predecessor failed before continuing-as-new, THE History_Serializer SHALL populate `continued_failure` with the failure from the Predecessor_Run.
2. WHEN the predecessor run did not fail (completed successfully or was not a continue-as-new), THE History_Serializer SHALL leave `continued_failure` as None.
3. WHEN the Runtime creates a StartRequest for a continue-as-new successor, THE Runtime SHALL read the Predecessor_Run's terminal failure and include it in the StartRequest as an Opaque_Failure_Payload.
4. WHEN the Kernel emits a Started_Event, THE Kernel SHALL include `continued_failure: Option<Payload>` in the event data, sourced from the StartRequest.

### Requirement 4: WorkflowExecutionStarted — last_completion_result

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include `last_completion_result` when a previous run in the chain completed successfully, so that the SDK can access the result from the last successful run.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a Started_Event for a run that has a last completion result from a predecessor, THE History_Serializer SHALL populate `last_completion_result` with the result payloads.
2. WHEN no predecessor run completed successfully, THE History_Serializer SHALL leave `last_completion_result` as None.
3. WHEN the Runtime creates a StartRequest for a continue-as-new or retry successor, THE Runtime SHALL read the Predecessor_Run's terminal result (if it completed successfully) and include it in the StartRequest.
4. WHEN the Kernel emits a Started_Event, THE Kernel SHALL include `last_completion_result: Option<Payloads>` in the event data, sourced from the StartRequest.

### Requirement 5: WorkflowExecutionContinuedAsNew — workflow_execution_timeout (non-goal)

**Note:** The upstream proto `WorkflowExecutionContinuedAsNewEventAttributes` does NOT have a `workflow_execution_timeout` field. The proto contains `workflow_run_timeout` and `workflow_task_timeout`, but execution timeout is intentionally omitted. The kernel carries `workflow_execution_timeout` internally for successor run creation, but the history serializer correctly ignores it with a wildcard `_` pattern. No serializer change is needed for this field.

#### Acceptance Criteria

1. THE History_Serializer SHALL continue to ignore `workflow_execution_timeout` on the ContinueAsNew_Event with a wildcard pattern, because the upstream proto does not have this field.
2. THE Kernel SHALL continue to carry `workflow_execution_timeout` in the ContinueAsNew_Event data for internal use by the runtime when creating the successor run.

### Requirement 6: WorkflowExecutionContinuedAsNew — retry_policy (non-goal)

**Note:** The upstream proto `WorkflowExecutionContinuedAsNewEventAttributes` does NOT have a `retry_policy` field, similar to `workflow_execution_timeout`. The kernel carries `retry_policy` in the ContinuedAsNew event data for internal use by the runtime when creating the successor run's `StartRequest`, but the history serializer correctly ignores it with a wildcard `_` pattern. No serializer change is needed.

#### Acceptance Criteria

1. THE History_Serializer SHALL continue to ignore `retry_policy` on the ContinueAsNew_Event with a wildcard pattern, because the upstream proto does not have this field.
2. THE Kernel SHALL continue to carry `retry_policy` in the ContinueAsNew_Event data for internal use by the runtime when creating the successor run.

### Requirement 7: WorkflowExecutionContinuedAsNew — initiator

**User Story:** As an SDK user, I want `WorkflowExecutionContinuedAsNewEventAttributes` to include `initiator`, so that the SDK can distinguish whether the continue-as-new was triggered by a workflow command, a retry, or a cron schedule.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a ContinueAsNew_Event, THE History_Serializer SHALL populate `initiator` with the ContinueAsNew_Initiator type.
2. WHEN the Kernel emits a ContinueAsNew_Event triggered by a `ContinueAsNew` workflow command, THE Kernel SHALL set the initiator to `Workflow`.
3. NOTE: Retry-initiated continue-as-new does NOT produce a `WorkflowExecutionContinuedAsNew` event in the current architecture. When a failed run is retried, the runtime emits `WorkflowExecutionFailed` (with the failure) and then creates a successor `StartRequest` directly — no CAN event is emitted. The `initiator: Retry` variant exists for future use when the retry path is changed to emit CAN events (matching Temporal's newer behavior where `WorkflowExecutionFailed` carries `new_execution_run_id`). For now, only `initiator: Workflow` is produced.
4. WHEN the Runtime creates a continue-as-new successor due to a cron schedule, THE Runtime SHALL set the initiator to `CronSchedule` (deferred to Feature 6).

### Requirement 8: WorkflowExecutionContinuedAsNew — failure and last_completion_result

**User Story:** As an SDK user, I want `WorkflowExecutionContinuedAsNewEventAttributes` to include `failure` and `last_completion_result`, so that the successor run can access the current run's terminal state.

#### Acceptance Criteria

1. WHEN a workflow-initiated ContinueAsNew is emitted, THE Kernel SHALL set `failure: None` and `last_completion_result: None` on the ContinueAsNew_Event, because the current run has not completed or failed — it is continuing.
2. WHEN the History_Serializer serializes a ContinueAsNew_Event with a failure, THE History_Serializer SHALL populate the `failure` field on the proto attributes.
3. WHEN the History_Serializer serializes a ContinueAsNew_Event with a last completion result, THE History_Serializer SHALL populate the `last_completion_result` field on the proto attributes.
4. NOTE: For retry-initiated successors, the failure and last_completion_result are carried on the successor's `WorkflowExecutionStarted` event (via `continued_failure` and `last_completion_result` on `StartRequest`), not on a CAN event. See Requirement 7 AC 3.

### Requirement 9: ActivityTaskScheduled — timeout serialization verification

**User Story:** As an SDK user, I want to confirm that `ActivityTaskScheduledEventAttributes` includes all four timeout fields faithfully, so that the SDK can enforce activity timeouts during replay.

#### Acceptance Criteria

1. FOR EVERY `ActivityTaskScheduled` event where the Kernel carries `schedule_to_close_timeout`, THE History_Serializer SHALL populate the corresponding proto field.
2. FOR EVERY `ActivityTaskScheduled` event where the Kernel carries `schedule_to_start_timeout`, THE History_Serializer SHALL populate the corresponding proto field.
3. FOR EVERY `ActivityTaskScheduled` event where the Kernel carries `start_to_close_timeout`, THE History_Serializer SHALL populate the corresponding proto field.
4. FOR EVERY `ActivityTaskScheduled` event where the Kernel carries `heartbeat_timeout`, THE History_Serializer SHALL populate the corresponding proto field.
5. THE History_Serializer SHALL NOT silently drop any timeout field that the Kernel provides.

### Requirement 10: SignalExternalWorkflowExecutionInitiated — control field

**User Story:** As an SDK user, I want `SignalExternalWorkflowExecutionInitiatedEventAttributes` to include the `control` field, so that the SDK can correlate initiated events with their workflow commands during replay.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `SignalExternalWorkflowExecutionInitiated` event, THE History_Serializer SHALL populate the `control` field from the kernel event data.
2. WHEN the Kernel emits a `SignalExternalWorkflowExecutionInitiated` event, THE Kernel SHALL include the Control_Field in the event data.
3. WHEN the SDK sends a `SignalExternalWorkflowExecution` workflow command, THE Kernel SHALL accept a `control` string parameter on the command and thread it into the initiated event.

### Requirement 11: RequestCancelExternalWorkflowExecutionInitiated — control field

**User Story:** As an SDK user, I want `RequestCancelExternalWorkflowExecutionInitiatedEventAttributes` to include the `control` field, so that the SDK can correlate initiated events with their workflow commands during replay.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `RequestCancelExternalWorkflowExecutionInitiated` event, THE History_Serializer SHALL populate the `control` field from the kernel event data.
2. WHEN the Kernel emits a `RequestCancelExternalWorkflowExecutionInitiated` event, THE Kernel SHALL include the Control_Field in the event data.
3. WHEN the SDK sends a `RequestCancelExternalWorkflowExecution` workflow command, THE Kernel SHALL accept a `control` string parameter on the command and thread it into the initiated event.
