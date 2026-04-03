# Requirements Document: Kernel Pause/Unpause and Activity Management (Feature 11)

## Introduction

This document captures the requirements for Feature 11 of the Tokeira kernel (`tokeira-kernel`): Pause/Unpause and Activity Management. This feature adds six new top-level commands to the kernel: two for workflow-level pause/unpause and four for operator-initiated activity management.

Workflow pause introduces a new non-terminal `ExecutionStatus::Paused` status. A paused workflow is still "open" but not executing — the kernel suppresses WFT scheduling for commands that would normally trigger one, and rejects Updates (which require an active WFT to process). WFT lifecycle commands (Started, Completed, Failed, TimedOut) are not explicitly rejected by the kernel; instead, the runtime uses a `wft_stamp` field on `WorkflowState` (bumped on pause) to detect and reject stale WFT deliveries at the delivery layer. Commands like Signal, Cancel, Terminate, Reset, and UpdateExecutionOptions proceed normally but without scheduling a WFT.

Activity management commands that would normally dispatch activity tasks (UnpauseActivity, ResetActivity) suppress the `DispatchOp::EnqueueActivityTask` when the workflow is paused. The state mutation still occurs (pause_info cleared, stamp bumped, attempt reset), but the dispatch is deferred until the workflow is unpaused — at which point `UnpauseWorkflow` re-dispatches all pending activities.

Activity management commands are pure state mutations that operate on individual activities within a workflow. They do not emit history events. They are operator-initiated and carry request deduplication.

This feature depends on Features 1–10 (all complete). The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are at [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md).

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **ExecutionStatus**: Lifecycle state visible to operators and projections. Currently: Running, Completed, Failed, Cancelled, Terminated, ContinuedAsNew, TimedOut. This feature adds Paused.
- **PauseInfo**: A struct stored on WorkflowState when a workflow is paused, carrying pause_time, identity, reason, and request_id.
- **ActivityPauseInfo**: A struct stored on ActivityState when an individual activity is paused, carrying pause_time, identity, and reason.
- **ActivityState**: Tracking record for an open activity in WorkflowState. This feature adds optional pause_info and a stamp field.
- **Stamp**: A monotonic counter on ActivityState used to invalidate stale activity tasks. Bumped on pause, unpause, option updates, and reset.
- **WFT Stamp**: A monotonic counter on WorkflowState (`wft_stamp`) used to invalidate stale workflow task deliveries. Bumped on PauseWorkflow. The runtime includes this in task tokens and validates on WorkflowTaskStarted.
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **ActivityOp**: An operation emitted by the Kernel for activity state persistence (Upsert or Delete).
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **TransitionBuilder**: Internal helper that assembles a Transition by taking ownership of WorkflowState, emitting events with contiguous IDs, and incrementing transition_seq exactly once on `finish()`.

## Requirements

---

## Group A: Workflow Pause/Unpause

### Requirement 11.1: ExecutionStatus::Paused — New Non-Terminal Status

**User Story:** As a Tokeira developer, I want a Paused execution status, so that operators can suspend workflow execution without closing the run.

#### Acceptance Criteria

1. THE ExecutionStatus enum SHALL include a `Paused` variant.
2. THE ExecutionStatus::Paused SHALL be a non-terminal status: `is_open()` SHALL return true for Paused.
3. WHEN a workflow is in Paused status, THE Kernel's `expect_open` helper SHALL treat Paused as open (not reject with RunClosed).
4. WHEN a workflow is in Paused status and a command would normally schedule a WFT (Signal, Cancel, ActivityResolved, TimerDue, ChildStartConfirmed, ChildResolved, ExternalSignalResolved, ExternalCancelResolved, NexusOperationResolved), THE Kernel SHALL NOT schedule a WFT. The command SHALL still emit its events and mutate state normally.
5. WHEN an Update command is received for a paused run, THE Kernel SHALL reject with WorkflowPaused. Updates require an active WFT to process and cannot be accepted while paused.

### Requirement 11.2: PauseInfo on WorkflowState

**User Story:** As a Tokeira developer, I want PauseInfo stored on WorkflowState when a workflow is paused, so that the pause metadata is available for queries and unpause logic.

#### Acceptance Criteria

1. THE WorkflowState SHALL include a `pause_info: Option<PauseInfo>` field.
2. THE PauseInfo struct SHALL contain `pause_time: OffsetDateTime`, `identity: String`, `reason: String`, and `request_id: String`.
3. WHEN a workflow is paused, THE WorkflowState's pause_info SHALL be Some containing the pause metadata.
4. WHEN a workflow is not paused, THE WorkflowState's pause_info SHALL be None.
5. THE WorkflowState SHALL include a `wft_stamp: u64` field, initialized to 0 on Start.
6. WHEN a PauseWorkflow command is received, THE Kernel SHALL increment `wft_stamp` on next_state. The runtime includes this stamp in WFT task tokens and validates it on WorkflowTaskStarted to detect stale deliveries.

### Requirement 11.3: PauseWorkflow Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to pause a running workflow execution, so that operators can suspend workflow execution while preserving state.

#### Acceptance Criteria

1. WHEN a PauseWorkflow command is received for a run with ExecutionStatus::Running, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL emit a WorkflowExecutionPaused history event carrying identity, reason, and request_id.
3. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL set ExecutionStatus to Paused on next_state.
4. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL set PauseInfo on next_state carrying pause_time, identity, reason, and request_id.
5. WHEN a PauseWorkflow command is received for a running run with pending activities, THE Kernel SHALL emit an ActivityOp::Upsert for each pending activity with an incremented stamp.
6. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL emit a ProjectionOp::UpsertExecution with Paused status.
7. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL NOT schedule a workflow task.
8. WHEN a PauseWorkflow command is received for a running run, THE Kernel SHALL increment `wft_stamp` on next_state to invalidate any in-flight WFT deliveries.
9. WHEN a PauseWorkflow command is received for a run already in Paused status with the same request_id, THE Kernel SHALL return Ok with no state change (idempotent noop).
10. WHEN a PauseWorkflow command is received for a run already in Paused status with a different request_id, THE Kernel SHALL reject with AlreadyPaused.
11. WHEN a PauseWorkflow command is received for a missing run, THE Kernel SHALL reject with MissingRun.
12. WHEN a PauseWorkflow command is received for a closed (terminal) run, THE Kernel SHALL reject with RunClosed.

### Requirement 11.4: UnpauseWorkflow Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to unpause a paused workflow execution, so that operators can resume workflow execution.

#### Acceptance Criteria

1. WHEN an UnpauseWorkflow command is received for a run with ExecutionStatus::Paused, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an UnpauseWorkflow command is received for a paused run, THE Kernel SHALL emit a WorkflowExecutionUnpaused history event carrying identity, reason, and request_id.
3. WHEN an UnpauseWorkflow command is received for a paused run, THE Kernel SHALL set ExecutionStatus back to Running on next_state.
4. WHEN an UnpauseWorkflow command is received for a paused run, THE Kernel SHALL clear PauseInfo (set to None) on next_state.
5. WHEN an UnpauseWorkflow command is received for a paused run with pending activities, THE Kernel SHALL emit an ActivityOp::Upsert for each pending activity with an incremented stamp.
6. WHEN an UnpauseWorkflow command is received for a paused run with pending activities, THE Kernel SHALL emit a DispatchOp::EnqueueActivityTask for each pending activity to regenerate activity tasks.
7. WHEN an UnpauseWorkflow command is received for a paused run and no WFT is currently pending, THE Kernel SHALL schedule a workflow task. WHEN a WFT is already pending (it was pending when the workflow was paused), THE Kernel SHALL NOT schedule a second one (at-most-one-WFT invariant).
8. WHEN an UnpauseWorkflow command is received for a paused run, THE Kernel SHALL emit a ProjectionOp::UpsertExecution with Running status.
9. WHEN an UnpauseWorkflow command is received for a run that is not in Paused status, THE Kernel SHALL reject with NotPaused.
10. WHEN an UnpauseWorkflow command is received for a missing run, THE Kernel SHALL reject with MissingRun.
11. WHEN an UnpauseWorkflow command is received for a closed (terminal) run, THE Kernel SHALL reject with RunClosed.

### Requirement 11.5: History Events for Pause/Unpause

**User Story:** As a Tokeira developer, I want WorkflowExecutionPaused and WorkflowExecutionUnpaused history events, so that pause/unpause operations are recorded in the authoritative event stream.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowExecutionPaused` variant carrying identity (String), reason (String), and request_id (String).
2. THE HistoryEventKind enum SHALL include a `WorkflowExecutionUnpaused` variant carrying identity (String), reason (String), and request_id (String).
3. WHEN a PauseWorkflow command produces a transition, THE Transition SHALL contain exactly one WorkflowExecutionPaused history event.
4. WHEN an UnpauseWorkflow command produces a transition and no WFT was pending, THE Transition SHALL contain one WorkflowExecutionUnpaused event and one WorkflowTaskScheduled event. WHEN a WFT was already pending, THE Transition SHALL contain only the WorkflowExecutionUnpaused event.

### Requirement 11.6: Paused Workflow Interaction with Existing Commands

**User Story:** As a Tokeira developer, I want paused workflows to correctly interact with existing commands, so that the pause semantics are consistent across the kernel.

#### Acceptance Criteria

1. WHEN a Signal command is received for a paused run, THE Kernel SHALL emit the WorkflowExecutionSignaled event and RequestDedupeOp, but SHALL NOT schedule a WFT.
2. WHEN a Cancel command is received for a paused run, THE Kernel SHALL emit the WorkflowExecutionCancelRequested event and RequestDedupeOp, but SHALL NOT schedule a WFT.
3. WHEN a Terminate command is received for a paused run, THE Kernel SHALL close the run with Terminated status (same behavior as for a running run). Paused is treated as open.
4. WHEN a Reset command is received for a paused run, THE Kernel SHALL process the reset (same behavior as for a running run). Paused is treated as open.
5. WHEN an Update command is received for a paused run, THE Kernel SHALL reject with WorkflowPaused. Updates cannot be accepted while the workflow is paused because there is no active WFT to process them.
6. WHEN an UpdateExecutionOptions command is received for a paused run, THE Kernel SHALL process it normally (same behavior as for a running run). Execution options are metadata, not WFT-dependent.
7. WHEN an ActivityResolved command is received for a paused run, THE Kernel SHALL emit the resolution event and remove the activity, but SHALL NOT schedule a WFT.
8. WHEN a TimerDue command is received for a paused run, THE Kernel SHALL emit the TimerFired event and remove the timer, but SHALL NOT schedule a WFT.
9. WHEN a WorkflowTaskStarted command is received for a paused run, THE Kernel SHALL NOT explicitly reject it. The runtime uses the `wft_stamp` field on WorkflowState (bumped on pause) to detect stale WFT deliveries at the delivery layer. If a WFT was pending before the pause, the kernel processes WorkflowTaskStarted normally.
10. WHEN a WorkflowTaskCompleted command is received for a paused run, THE Kernel SHALL NOT explicitly reject it. If a WFT was started before the pause, the kernel processes the completion normally. However, the kernel SHALL NOT schedule a new WFT if `force_new_workflow_task` is set (the paused status suppresses WFT scheduling).
11. WHEN a WorkflowTaskFailed command is received for a paused run, THE Kernel SHALL NOT explicitly reject it. The kernel processes the failure normally but SHALL NOT re-dispatch the WFT (the paused status suppresses WFT scheduling).
12. WHEN a WorkflowTaskTimedOut command is received for a paused run, THE Kernel SHALL NOT explicitly reject it. The kernel processes the timeout normally but SHALL NOT re-dispatch the WFT (the paused status suppresses WFT scheduling).
13. WHEN a WorkflowExecutionTimedOut command is received for a paused run, THE Kernel SHALL close the run with TimedOut status (same behavior as for a running run). Paused is treated as open.
14. WHEN a ChildStartConfirmed, ChildResolved, ExternalSignalResolved, ExternalCancelResolved, or NexusOperationResolved command is received for a paused run, THE Kernel SHALL process it normally but SHALL NOT schedule a WFT.

---

## Group B: Activity Management

### Requirement 11.7: ActivityState Extensions

**User Story:** As a Tokeira developer, I want ActivityState to carry pause_info and stamp fields, so that activity management commands can track pause state and invalidate stale tasks.

#### Acceptance Criteria

1. THE ActivityState struct SHALL include a `pause_info: Option<ActivityPauseInfo>` field.
2. THE ActivityState struct SHALL include a `stamp: u64` field.
3. THE ActivityPauseInfo struct SHALL contain `pause_time: OffsetDateTime`, `identity: String`, and `reason: String`.
4. WHEN a new activity is scheduled via ScheduleActivity workflow command, THE Kernel SHALL initialize stamp to 0 and pause_info to None.

### Requirement 11.8: UpdateActivityOptions Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to update activity options on pending activities, so that operators can modify timeouts, retry policy, task queue, or other options without restarting the workflow.

#### Acceptance Criteria

1. WHEN an UpdateActivityOptions command is received for an open run with a known activity_id, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an UpdateActivityOptions command is received, THE Kernel SHALL mutate the specified fields on the ActivityState (timeouts, task_queue).
3. WHEN an UpdateActivityOptions command is received, THE Kernel SHALL increment the activity's stamp.
4. WHEN an UpdateActivityOptions command is received, THE Kernel SHALL emit an ActivityOp::Upsert with the updated ActivityState.
5. WHEN an UpdateActivityOptions command is received, THE Kernel SHALL NOT emit history events.
6. WHEN an UpdateActivityOptions command is received, THE Kernel SHALL NOT schedule a workflow task.
7. WHEN an UpdateActivityOptions command is received for an unknown activity_id, THE Kernel SHALL reject with UnknownActivity.
8. WHEN an UpdateActivityOptions command is received for a missing run, THE Kernel SHALL reject with MissingRun.
9. WHEN an UpdateActivityOptions command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 11.9: PauseActivity Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to pause a specific pending activity, so that operators can suspend individual activity execution.

#### Acceptance Criteria

1. WHEN a PauseActivity command is received for an open run with a known activity_id, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a PauseActivity command is received, THE Kernel SHALL set ActivityPauseInfo on the activity carrying pause_time, identity, and reason.
3. WHEN a PauseActivity command is received, THE Kernel SHALL increment the activity's stamp.
4. WHEN a PauseActivity command is received, THE Kernel SHALL emit an ActivityOp::Upsert with the updated ActivityState.
5. WHEN a PauseActivity command is received, THE Kernel SHALL NOT emit history events.
6. WHEN a PauseActivity command is received, THE Kernel SHALL NOT schedule a workflow task.
7. WHEN a PauseActivity command is received for an unknown activity_id, THE Kernel SHALL reject with UnknownActivity.
8. WHEN a PauseActivity command is received for a missing run, THE Kernel SHALL reject with MissingRun.
9. WHEN a PauseActivity command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 11.10: UnpauseActivity Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to unpause a paused activity, so that operators can resume individual activity execution.

#### Acceptance Criteria

1. WHEN an UnpauseActivity command is received for an open run with a known activity_id that has pause_info set, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an UnpauseActivity command is received for a paused activity, THE Kernel SHALL clear ActivityPauseInfo (set to None) on the activity.
3. WHEN an UnpauseActivity command is received for a paused activity, THE Kernel SHALL increment the activity's stamp.
4. WHEN an UnpauseActivity command is received for a paused activity, THE Kernel SHALL emit an ActivityOp::Upsert with the updated ActivityState.
5. WHEN an UnpauseActivity command is received for a paused activity and the workflow status is Running, THE Kernel SHALL emit a DispatchOp::EnqueueActivityTask to regenerate the activity task. WHEN the workflow status is Paused, THE Kernel SHALL NOT emit the DispatchOp (the dispatch is deferred until UnpauseWorkflow re-dispatches all activities).
6. WHEN an UnpauseActivity command is received, THE Kernel SHALL NOT emit history events.
7. WHEN an UnpauseActivity command is received, THE Kernel SHALL NOT schedule a workflow task.
8. WHEN an UnpauseActivity command is received for an activity that is not paused, THE Kernel SHALL reject with ActivityNotPaused.
9. WHEN an UnpauseActivity command is received for an unknown activity_id, THE Kernel SHALL reject with UnknownActivity.
10. WHEN an UnpauseActivity command is received for a missing run, THE Kernel SHALL reject with MissingRun.
11. WHEN an UnpauseActivity command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 11.11: ResetActivity Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to reset an activity's attempt count and optionally its heartbeat and options, so that operators can retry a stuck activity from scratch.

#### Acceptance Criteria

1. WHEN a ResetActivity command is received for an open run with a known activity_id, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a ResetActivity command is received, THE Kernel SHALL reset the activity's attempt to 1.
3. WHEN a ResetActivity command is received with reset_heartbeat set to true, THE Kernel SHALL clear the activity's heartbeat details if the field exists on ActivityState. NOTE: ActivityState does not currently carry heartbeat_details; the `reset_heartbeat` flag is accepted on the request struct for API compatibility but is a no-op until ActivityState gains that field in a future feature.
4. WHEN a ResetActivity command is received, THE Kernel SHALL increment the activity's stamp.
5. WHEN a ResetActivity command is received, THE Kernel SHALL emit an ActivityOp::Upsert with the updated ActivityState.
6. WHEN a ResetActivity command is received and the workflow status is Running, THE Kernel SHALL emit a DispatchOp::EnqueueActivityTask to regenerate the activity task. WHEN the workflow status is Paused, THE Kernel SHALL NOT emit the DispatchOp (the dispatch is deferred until UnpauseWorkflow re-dispatches all activities).
7. WHEN a ResetActivity command is received, THE Kernel SHALL NOT emit history events.
8. WHEN a ResetActivity command is received, THE Kernel SHALL NOT schedule a workflow task.
9. WHEN a ResetActivity command is received for an unknown activity_id, THE Kernel SHALL reject with UnknownActivity.
10. WHEN a ResetActivity command is received for a missing run, THE Kernel SHALL reject with MissingRun.
11. WHEN a ResetActivity command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## Cross-Cutting Requirements for Feature 11

### Requirement 11.12: Reject Taxonomy — Feature 11 Additions

**User Story:** As a Tokeira developer, I want the Kernel's Reject enum to cover all rejection reasons for pause and activity management, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Kernel SHALL define a `WorkflowPaused` Reject variant for Update commands received while the workflow is paused.
2. THE Kernel SHALL define an `AlreadyPaused` Reject variant for PauseWorkflow commands received when the workflow is already paused with a different request_id.
3. THE Kernel SHALL define a `NotPaused` Reject variant for UnpauseWorkflow commands received when the workflow is not paused.
4. THE Kernel SHALL define an `ActivityNotPaused` Reject variant (carrying activity_id) for UnpauseActivity commands received when the activity is not paused.

### Requirement 11.13: Command Taxonomy — Feature 11 Additions

**User Story:** As a Tokeira developer, I want the Command enum to include all six new commands, so that the kernel can route them through the apply method.

#### Acceptance Criteria

1. THE Command enum SHALL include a `PauseWorkflow` variant carrying identity, reason, request context, and now timestamp.
2. THE Command enum SHALL include an `UnpauseWorkflow` variant carrying identity, reason, request context, and now timestamp.
3. THE Command enum SHALL include an `UpdateActivityOptions` variant carrying activity_id, optional field updates (timeouts, task_queue), request context, and now timestamp.
4. THE Command enum SHALL include a `PauseActivity` variant carrying activity_id, identity, reason, request context, and now timestamp.
5. THE Command enum SHALL include an `UnpauseActivity` variant carrying activity_id, request context, and now timestamp.
6. THE Command enum SHALL include a `ResetActivity` variant carrying activity_id, reset_heartbeat flag, request context, and now timestamp.
7. ALL six new commands SHALL carry RequestContext for deduplication and SHALL be classified as operator-initiated external commands.

### Requirement 11.14: Structural Invariants for Feature 11 Transitions

**User Story:** As a Tokeira developer, I want all transitions produced by Feature 11 commands to satisfy the existing structural invariants, so that correctness is verifiable by property tests.

#### Acceptance Criteria

1. FOR ALL Transitions produced by PauseWorkflow and UnpauseWorkflow, event IDs SHALL be contiguous within the transition.
2. FOR ALL Transitions produced by PauseWorkflow and UnpauseWorkflow, transition_seq SHALL increment exactly once.
3. FOR ALL Transitions produced by activity management commands (UpdateActivityOptions, PauseActivity, UnpauseActivity, ResetActivity), THE Transition SHALL contain zero history events.
4. FOR ALL Transitions produced by activity management commands, transition_seq SHALL increment exactly once.
5. FOR ALL Transitions produced by activity management commands, every ActivityOp::Upsert SHALL have a corresponding entry in next_state.activities.
6. FOR ALL Transitions where the workflow is paused, next_state.status SHALL be Paused and next_state.pause_info SHALL be Some.
7. FOR ALL Transitions where the workflow is unpaused, next_state.status SHALL be Running and next_state.pause_info SHALL be None.
8. FOR ALL Transitions produced by PauseWorkflow, next_state SHALL NOT contain a newly scheduled PendingWorkflowTask (existing pending WFT may remain).

### Requirement 11.15: Idempotent Noop Transition for PauseWorkflow

**User Story:** As a Tokeira developer, I want PauseWorkflow to be idempotent when the same request_id is used, so that retries do not produce duplicate state changes.

#### Acceptance Criteria

1. WHEN a PauseWorkflow command is received for an already-paused run with the same request_id as the existing PauseInfo, THE Kernel SHALL return an Ok result.
2. WHEN a PauseWorkflow idempotent noop occurs, THE Transition SHALL contain no history events, no activity ops, no dispatch ops, and no projection ops.
3. WHEN a PauseWorkflow idempotent noop occurs, THE Transition's next_state SHALL be identical to the input WorkflowState except for transition_seq which SHALL increment by one.

### Requirement 11.16: Architecture Documentation for Feature 11 Commands

**User Story:** As a Tokeira developer, I want the kernel architecture doc and crate reference doc to document all Feature 11 commands with their Temporal-derived semantics, so that the authoritative specification stays current and traceable.

#### Acceptance Criteria

1. THE architecture doc (`docs/architecture/020-kernel.md`) SHALL include behavioral specifications for `PauseWorkflow` and `UnpauseWorkflow` in the command taxonomy section, documenting the Temporal behaviour they implement: Paused as a non-terminal status, WFT scheduling suppression, stamp invalidation of pending activities and WFTs, Update rejection, and idempotent pause with matching request_id.
2. THE architecture doc SHALL include behavioral specifications for `UpdateActivityOptions`, `PauseActivity`, `UnpauseActivity`, and `ResetActivity` in the command taxonomy section, documenting the Temporal behaviour they implement: pure state mutations with no history events, stamp-based task invalidation, and activity-level pause/unpause lifecycle.
3. THE architecture doc SHALL update the command taxonomy table to include all six new commands with their origin (Operator/External), open-run requirement, and request dedup classification.
4. THE architecture doc SHALL update the `WorkflowState` section to include `pause_info: Option<PauseInfo>` and the extended `ActivityState` fields (`pause_info`, `stamp`).
5. THE architecture doc SHALL document the interaction between Paused status and existing commands, specifically: WFT lifecycle commands are not rejected by the kernel (stamp invalidation is a delivery-layer concern), Updates are rejected with `WorkflowPaused`, and all other commands proceed normally but suppress WFT scheduling.
6. THE crate reference doc (`docs/crates/kernel.md`) SHALL update the implementation status table, command taxonomy tables, reject taxonomy, and Temporal feature coverage to reflect Feature 11.
7. ALL documentation SHALL refer to the upstream behaviour as "Temporal" (not "temporal-dsql" or any internal project name).
