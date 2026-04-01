# Requirements Document: Child Workflows (Feature 5)

## Introduction

This document captures the requirements for Feature 5 of the Tokeira kernel implementation: Child Workflow support. Feature 5 depends on Feature 1 (kernel-foundation-wft-lifecycle), Feature 3 (kernel-cancel-terminate), and Feature 4 (kernel-continue-as-new-timeout), all of which are complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 5.1–5.4).

Feature 5 adds child workflow support to the kernel. This is the most complex entity lifecycle after activities because it involves:

- **Initiation** via a `StartChildWorkflow` workflow command within `WorkflowTaskCompleted`
- **Start confirmation** via a `ChildStartConfirmed` top-level command from the runtime (success or failure variant)
- **Terminal resolution** via a `ChildResolved` top-level command from the runtime
- **Parent Close Policy enforcement** when the parent workflow closes

From the parent's perspective, the entire chain of Continue-As-New runs for a child is treated as a single execution. The kernel only sees the final resolution.

Key design points:
- `ChildStartConfirmed` and `ChildResolved` are top-level kernel commands issued by the runtime, not by worker code. They are internal runtime machinery and do NOT carry `RequestContext` or emit `RequestDedupeOp`.
- The kernel does NOT create the child run. It emits `DispatchOp::StartChildWorkflow` and the runtime handles creation.
- `ChildStartConfirmed` carries the `initiated_event_id` for fencing: the kernel validates it matches the child entry. A stale confirmation for a child that was already resolved or removed is rejected.
- Parent Close Policy is applied on ALL close paths: Terminate, TimedOut, Completed, Failed, Cancelled, and ContinuedAsNew.
- Feature 3's Terminate and Feature 4's WorkflowExecutionTimedOut must be extended to apply Parent Close Policy to open children (they currently only clean up activities and timers).
- CompleteWorkflow, FailWorkflow, CancelWorkflow, and ContinueAsNew must also apply Parent Close Policy to open children on close.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **PendingWorkflowTask**: The authoritative record that a WFT exists for the run.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **ChildWorkflowState**: Tracking record for an open child workflow in WorkflowState, containing child_workflow_id, child_run_id (Option), initiated_event_id, started_event_id (Option), and parent_close_policy.
- **ParentClosePolicy**: Policy applied to open child workflows when the parent closes: Terminate, RequestCancel, or Abandon.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.

## Requirements

---

## New Types and Command Variants

### Requirement 1.1: ChildWorkflowState Type

**User Story:** As a Tokeira developer, I want a ChildWorkflowState type to track open child workflows in WorkflowState, so that the kernel can manage child lifecycle.

#### Acceptance Criteria

1. THE `ChildWorkflowState` struct SHALL include a `child_workflow_id` field of type `WorkflowId`.
2. THE `ChildWorkflowState` struct SHALL include a `child_run_id` field of type `Option<RunId>`, initialized to `None` until the child start is confirmed.
3. THE `ChildWorkflowState` struct SHALL include an `initiated_event_id` field of type `i64` recording the event ID of the StartChildWorkflowExecutionInitiated event.
4. THE `ChildWorkflowState` struct SHALL include a `started_event_id` field of type `Option<i64>`, initialized to `None` until the child start is confirmed.
5. THE `ChildWorkflowState` struct SHALL include a `parent_close_policy` field of type `ParentClosePolicy`.
6. THE `ChildWorkflowState` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.2: ParentClosePolicy Enum

**User Story:** As a Tokeira developer, I want a ParentClosePolicy enum, so that the kernel can determine what action to take on open children when the parent closes.

#### Acceptance Criteria

1. THE `ParentClosePolicy` enum SHALL include a `Terminate` variant.
2. THE `ParentClosePolicy` enum SHALL include a `RequestCancel` variant.
3. THE `ParentClosePolicy` enum SHALL include an `Abandon` variant.
4. THE `ParentClosePolicy` enum SHALL derive `Clone, Copy, Debug, PartialEq, Eq`.

### Requirement 1.3: WorkflowState Children Field

**User Story:** As a Tokeira developer, I want WorkflowState to include a children map, so that open child workflows are tracked as part of the run's durable state.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL include a `children` field of type `BTreeMap<WorkflowId, ChildWorkflowState>`.
2. WHEN a new WorkflowState is initialized (via Start command), THE `children` map SHALL be empty.

### Requirement 1.4: StartChildWorkflow Workflow Command Variant

**User Story:** As a Tokeira developer, I want a StartChildWorkflow variant in the WorkflowCommand enum, so that workflow code can initiate child workflow executions.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `StartChildWorkflow` variant with fields: `child_workflow_id` (WorkflowId), `namespace_id` (NamespaceId), `workflow_type` (WorkflowType), `task_queue` (TaskQueueName), `input` (Payloads), and `parent_close_policy` (ParentClosePolicy).

### Requirement 1.5: ChildStartConfirmed Command Variant

**User Story:** As a Tokeira developer, I want a ChildStartConfirmed variant in the Command enum, so that the runtime can confirm child workflow start success or failure.

#### Acceptance Criteria

1. THE Command enum SHALL include a `ChildStartConfirmed(ChildStartConfirmedRequest)` variant.
2. THE `ChildStartConfirmedRequest` struct SHALL include a `child_workflow_id` field of type `WorkflowId`.
3. THE `ChildStartConfirmedRequest` struct SHALL include an `initiated_event_id` field of type `i64` for fencing against stale confirmations.
4. THE `ChildStartConfirmedRequest` struct SHALL include a `result` field of type `ChildStartResult` distinguishing success from failure.
5. THE `ChildStartConfirmedRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
6. THE `ChildStartConfirmedRequest` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.6: ChildStartResult Enum

**User Story:** As a Tokeira developer, I want a ChildStartResult enum to distinguish between successful and failed child starts, so that the kernel can emit the correct events.

#### Acceptance Criteria

1. THE `ChildStartResult` enum SHALL include a `Started` variant with fields: `child_run_id` (RunId) and `workflow_type` (WorkflowType).
2. THE `ChildStartResult` enum SHALL include a `Failed` variant with field: `cause` (String).
3. THE `ChildStartResult` enum SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.7: ChildResolved Command Variant

**User Story:** As a Tokeira developer, I want a ChildResolved variant in the Command enum, so that the runtime can report child workflow terminal resolution.

#### Acceptance Criteria

1. THE Command enum SHALL include a `ChildResolved(ChildResolvedRequest)` variant.
2. THE `ChildResolvedRequest` struct SHALL include a `child_workflow_id` field of type `WorkflowId`.
3. THE `ChildResolvedRequest` struct SHALL include a `resolution` field of type `ChildResolution`.
4. THE `ChildResolvedRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
5. THE `ChildResolvedRequest` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.8: ChildResolution Enum

**User Story:** As a Tokeira developer, I want a ChildResolution enum to represent the terminal status of a child workflow, so that the kernel can emit the correct terminal event.

#### Acceptance Criteria

1. THE `ChildResolution` enum SHALL include a `Completed` variant with field: `result` (Payloads).
2. THE `ChildResolution` enum SHALL include a `Failed` variant with field: `failure` (String).
3. THE `ChildResolution` enum SHALL include a `Canceled` variant.
4. THE `ChildResolution` enum SHALL include a `Terminated` variant.
5. THE `ChildResolution` enum SHALL include a `TimedOut` variant.
6. THE `ChildResolution` enum SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.9: New HistoryEventKind Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for child workflow lifecycle events, so that these events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `StartChildWorkflowExecutionInitiated` variant with fields: `child_workflow_id` (WorkflowId), `workflow_type` (WorkflowType), `task_queue` (TaskQueueName), `input` (Payloads), `namespace_id` (NamespaceId), and `parent_close_policy` (ParentClosePolicy).
2. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionStarted` variant with fields: `child_workflow_id` (WorkflowId), `child_run_id` (RunId), and `workflow_type` (WorkflowType).
3. THE HistoryEventKind enum SHALL include a `StartChildWorkflowExecutionFailed` variant with fields: `child_workflow_id` (WorkflowId) and `cause` (String).
4. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionCompleted` variant with fields: `child_workflow_id` (WorkflowId) and `result` (Payloads).
5. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionFailed` variant with fields: `child_workflow_id` (WorkflowId) and `failure` (String).
6. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionCanceled` variant with field: `child_workflow_id` (WorkflowId).
7. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionTerminated` variant with field: `child_workflow_id` (WorkflowId).
8. THE HistoryEventKind enum SHALL include a `ChildWorkflowExecutionTimedOut` variant with field: `child_workflow_id` (WorkflowId).

### Requirement 1.10: New DispatchOp Variants

**User Story:** As a Tokeira developer, I want new DispatchOp variants for child workflow operations, so that the runtime knows what delivery actions to take.

#### Acceptance Criteria

1. THE DispatchOp enum SHALL include a `StartChildWorkflow` variant with fields: `child_workflow_id` (WorkflowId), `namespace_id` (NamespaceId), `workflow_type` (WorkflowType), `task_queue` (TaskQueueName), and `input` (Payloads).
2. THE DispatchOp enum SHALL include a `TerminateChild` variant with fields: `child_workflow_id` (WorkflowId), `child_run_id` (RunId), and `reason` (String).
3. THE DispatchOp enum SHALL include a `CancelChild` variant with fields: `child_workflow_id` (WorkflowId), `child_run_id` (RunId), and `reason` (String).

### Requirement 1.11: New Reject Variants

**User Story:** As a Tokeira developer, I want new Reject variants for child workflow errors, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Reject enum SHALL include a `DuplicateChildWorkflowId(WorkflowId)` variant for when StartChildWorkflow references a child_workflow_id already in the open children map.
2. THE Reject enum SHALL include an `UnknownChild(WorkflowId)` variant for when ChildResolved or ChildStartConfirmed references a child not in the open children map.
3. THE Reject enum SHALL include a `StaleChildConfirmation` variant with fields `child_workflow_id` (WorkflowId) and `expected_initiated_event_id` (i64) for when ChildStartConfirmed carries an initiated_event_id that does not match the child entry.

---

## StartChildWorkflow Workflow Command Behavior

### Requirement 2.1: StartChildWorkflow Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to initiate child workflow executions from workflow commands, so that workflows can compose other workflows.

#### Acceptance Criteria

1. WHEN a StartChildWorkflow workflow command is received within WorkflowTaskCompleted with a unique child_workflow_id, THE Kernel SHALL emit a StartChildWorkflowExecutionInitiated event carrying the child_workflow_id, workflow_type, task_queue, input, namespace_id, and parent_close_policy.
2. WHEN a StartChildWorkflow workflow command is received, THE Kernel SHALL add a ChildWorkflowState entry to the children map with child_run_id None, started_event_id None, the initiated_event_id set to the emitted event's ID, and the specified parent_close_policy.
3. WHEN a StartChildWorkflow workflow command is received, THE Kernel SHALL push a DispatchOp::StartChildWorkflow with the child_workflow_id, namespace_id, workflow_type, task_queue, and input.
4. WHEN a StartChildWorkflow workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

### Requirement 2.2: StartChildWorkflow Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject duplicate child workflow IDs, so that the same child cannot be initiated twice.

#### Acceptance Criteria

1. WHEN a StartChildWorkflow workflow command is received with a child_workflow_id that is already in the open children map, THE Kernel SHALL reject with DuplicateChildWorkflowId.

---

## ChildStartConfirmed Command Behavior

### Requirement 3.1: ChildStartConfirmed Success Path

**User Story:** As a Tokeira developer, I want the Kernel to record child workflow start confirmations, so that the parent can track child lifecycle.

#### Acceptance Criteria

1. WHEN a ChildStartConfirmed command is received with a Started result for a known child whose initiated_event_id matches, THE Kernel SHALL emit a ChildWorkflowExecutionStarted event carrying the child_workflow_id, child_run_id, and workflow_type.
2. WHEN a ChildStartConfirmed command is received with a Started result, THE Kernel SHALL update the child entry in the children map to record the started_event_id (set to the emitted event's ID) and child_run_id.
3. WHEN a ChildStartConfirmed command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a ChildStartConfirmed command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. WHEN a ChildStartConfirmed command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is internal runtime machinery).

### Requirement 3.2: ChildStartConfirmed Failure Path

**User Story:** As a Tokeira developer, I want the Kernel to handle child start failures, so that the parent workflow can observe that the child could not be started.

#### Acceptance Criteria

1. WHEN a ChildStartConfirmed command is received with a Failed result for a known child whose initiated_event_id matches, THE Kernel SHALL emit a StartChildWorkflowExecutionFailed event carrying the child_workflow_id and cause.
2. WHEN a ChildStartConfirmed command is received with a Failed result, THE Kernel SHALL remove the child from the open children map.
3. WHEN a ChildStartConfirmed command is received with a Failed result and no WFT is pending, THE Kernel SHALL schedule a workflow task.

### Requirement 3.3: ChildStartConfirmed Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid ChildStartConfirmed commands, so that stale or unknown confirmations are caught.

#### Acceptance Criteria

1. WHEN a ChildStartConfirmed command is received for a child_workflow_id not in the open children map, THE Kernel SHALL reject with UnknownChild.
2. WHEN a ChildStartConfirmed command is received with an initiated_event_id that does not match the child entry's initiated_event_id, THE Kernel SHALL reject with StaleChildConfirmation carrying the child_workflow_id and the expected initiated_event_id.

---

## ChildResolved Command Behavior

### Requirement 4.1: ChildResolved Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to process child workflow resolutions, so that the parent workflow can observe child completion.

#### Acceptance Criteria

1. WHEN a ChildResolved command is received for a known open child with a Completed resolution, THE Kernel SHALL emit a ChildWorkflowExecutionCompleted event carrying the child_workflow_id and result.
2. WHEN a ChildResolved command is received for a known open child with a Failed resolution, THE Kernel SHALL emit a ChildWorkflowExecutionFailed event carrying the child_workflow_id and failure.
3. WHEN a ChildResolved command is received for a known open child with a Canceled resolution, THE Kernel SHALL emit a ChildWorkflowExecutionCanceled event carrying the child_workflow_id.
4. WHEN a ChildResolved command is received for a known open child with a Terminated resolution, THE Kernel SHALL emit a ChildWorkflowExecutionTerminated event carrying the child_workflow_id.
5. WHEN a ChildResolved command is received for a known open child with a TimedOut resolution, THE Kernel SHALL emit a ChildWorkflowExecutionTimedOut event carrying the child_workflow_id.
6. WHEN a ChildResolved command is received, THE Kernel SHALL remove the child from the open children map.
7. WHEN a ChildResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
8. WHEN a ChildResolved command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
9. WHEN a ChildResolved command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is internal runtime machinery).

### Requirement 4.2: ChildResolved Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject ChildResolved for unknown children, so that stale or invalid resolutions are caught.

#### Acceptance Criteria

1. WHEN a ChildResolved command is received for a child_workflow_id not in the open children map, THE Kernel SHALL reject with UnknownChild.

---

## Parent Close Policy

### Requirement 5.1: Parent Close Policy on Terminate

**User Story:** As a Tokeira developer, I want Terminate to apply Parent Close Policy to open children, so that child workflows are handled according to the configured policy when the parent is hard-stopped.

**Scope note:** Feature 3 implemented Terminate's cleanup for activities and timers only. This requirement extends Terminate to also apply Parent Close Policy to open children.

#### Acceptance Criteria

1. WHEN a Terminate command is received and open children exist with ParentClosePolicy::Terminate, THE Kernel SHALL emit a DispatchOp::TerminateChild for each such child carrying the child_workflow_id, child_run_id, and a reason indicating parent termination.
2. WHEN a Terminate command is received and open children exist with ParentClosePolicy::RequestCancel, THE Kernel SHALL emit a DispatchOp::CancelChild for each such child carrying the child_workflow_id, child_run_id, and a reason indicating parent termination.
3. WHEN a Terminate command is received and open children exist with ParentClosePolicy::Abandon, THE Kernel SHALL take no action for those children.
4. WHEN a Terminate command is received, THE Kernel SHALL remove all children from the open children map in next_state (next_state.children SHALL be empty).

### Requirement 5.2: Parent Close Policy on WorkflowExecutionTimedOut

**User Story:** As a Tokeira developer, I want WorkflowExecutionTimedOut to apply Parent Close Policy to open children, so that child workflows are handled when the parent times out.

**Scope note:** Feature 4 implemented WorkflowExecutionTimedOut's cleanup for activities and timers only. This requirement extends it to also apply Parent Close Policy to open children.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received and open children exist, THE Kernel SHALL apply Parent Close Policy using the same logic as Terminate (Requirement 5.1).
2. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL remove all children from the open children map in next_state.

### Requirement 5.3: Parent Close Policy on CompleteWorkflow

**User Story:** As a Tokeira developer, I want CompleteWorkflow to apply Parent Close Policy to open children, so that child workflows are handled when the parent completes successfully.

#### Acceptance Criteria

1. WHEN a CompleteWorkflow workflow command is received and open children exist, THE Kernel SHALL apply Parent Close Policy using the same logic as Terminate (Requirement 5.1).
2. WHEN a CompleteWorkflow workflow command is received, THE Kernel SHALL remove all children from the open children map in next_state.

### Requirement 5.4: Parent Close Policy on FailWorkflow

**User Story:** As a Tokeira developer, I want FailWorkflow to apply Parent Close Policy to open children, so that child workflows are handled when the parent fails.

#### Acceptance Criteria

1. WHEN a FailWorkflow workflow command is received and open children exist, THE Kernel SHALL apply Parent Close Policy using the same logic as Terminate (Requirement 5.1).
2. WHEN a FailWorkflow workflow command is received, THE Kernel SHALL remove all children from the open children map in next_state.

### Requirement 5.5: Parent Close Policy on CancelWorkflow

**User Story:** As a Tokeira developer, I want CancelWorkflow to apply Parent Close Policy to open children, so that child workflows are handled when the parent is canceled.

#### Acceptance Criteria

1. WHEN a CancelWorkflow workflow command is received and open children exist, THE Kernel SHALL apply Parent Close Policy using the same logic as Terminate (Requirement 5.1).
2. WHEN a CancelWorkflow workflow command is received, THE Kernel SHALL remove all children from the open children map in next_state.

### Requirement 5.6: Parent Close Policy on ContinueAsNew

**User Story:** As a Tokeira developer, I want ContinueAsNew to apply Parent Close Policy to open children, so that child workflows are handled when the parent continues as a new run.

#### Acceptance Criteria

1. WHEN a ContinueAsNew workflow command is received and open children exist, THE Kernel SHALL apply Parent Close Policy using the same logic as Terminate (Requirement 5.1).
2. WHEN a ContinueAsNew workflow command is received, THE Kernel SHALL remove all children from the open children map in next_state.

### Requirement 5.7: Parent Close Policy for Unstarted Children

**User Story:** As a Tokeira developer, I want Parent Close Policy to handle children that have been initiated but not yet started (child_run_id is None), so that all open children are addressed on parent close.

#### Acceptance Criteria

1. WHEN Parent Close Policy is applied to a child with ParentClosePolicy::Terminate and child_run_id is None, THE Kernel SHALL NOT emit a DispatchOp::TerminateChild for that child (there is no run to terminate).
2. WHEN Parent Close Policy is applied to a child with ParentClosePolicy::RequestCancel and child_run_id is None, THE Kernel SHALL NOT emit a DispatchOp::CancelChild for that child (there is no run to cancel).
3. WHEN Parent Close Policy is applied, THE Kernel SHALL remove all children from the open children map regardless of whether child_run_id is Some or None.

---

## BasicKernel Integration

### Requirement 6.1: BasicKernel Apply Routing for Child Commands

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route ChildStartConfirmed and ChildResolved commands to dedicated handler methods, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a ChildStartConfirmed command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_child_start_confirmed` method.
2. WHEN a ChildResolved command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_child_resolved` method.
3. THE `apply_child_start_confirmed` method SHALL follow the same pattern as existing apply methods: call `expect_open`, construct a TransitionBuilder, validate the child entry, emit event, update state, conditionally schedule WFT, and call `finish`.
4. THE `apply_child_resolved` method SHALL follow the same pattern: call `expect_open`, construct a TransitionBuilder, validate the child entry, emit event, remove child, conditionally schedule WFT, and call `finish`.

### Requirement 6.2: Workflow Command Dispatch for StartChildWorkflow

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle StartChildWorkflow, so that child workflow initiation is processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::StartChildWorkflow` that validates the child_workflow_id is unique, emits StartChildWorkflowExecutionInitiated, creates ChildWorkflowState, and pushes DispatchOp::StartChildWorkflow.

### Requirement 6.3: Parent Close Policy Helper

**User Story:** As a Tokeira developer, I want a shared helper function for applying Parent Close Policy, so that all close paths use consistent logic.

#### Acceptance Criteria

1. THE Kernel SHALL implement a shared helper (e.g., `apply_parent_close_policy` on TransitionBuilder) that iterates over open children and emits the appropriate DispatchOp based on each child's ParentClosePolicy and child_run_id.
2. THE helper SHALL be called from Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, and ContinueAsNew close paths.
3. THE helper SHALL clear the children map in next_state after processing all children.

### Requirement 6.4: Start Command Extension for Children Field

**User Story:** As a Tokeira developer, I want the Start command handler to initialize the children map, so that the new field is properly set on WorkflowState creation.

#### Acceptance Criteria

1. WHEN a Start command initializes a new WorkflowState, THE children field SHALL be set to an empty BTreeMap.

---

## Structural Invariants

### Requirement 7.1: Event ID Contiguity for Child Workflow Transitions

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for all child workflow transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL ChildStartConfirmed transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL ChildResolved transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL transitions containing StartChildWorkflow workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.

### Requirement 7.2: Transition Sequence Increment for Child Workflow Transitions

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for child workflow transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL ChildStartConfirmed transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.
2. FOR ALL ChildResolved transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 7.3: At-Most-One-WFT Invariant for Child Commands

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after ChildStartConfirmed and ChildResolved, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL ChildStartConfirmed transitions, next_state SHALL contain at most one PendingWorkflowTask.
2. FOR ALL ChildResolved transitions, next_state SHALL contain at most one PendingWorkflowTask.

### Requirement 7.4: Children Map Consistency

**User Story:** As a Tokeira developer, I want the children map to be consistent after every transition, so that child lifecycle tracking is accurate.

#### Acceptance Criteria

1. FOR ALL StartChildWorkflow workflow commands that succeed, THE next_state.children map SHALL contain an entry for the child_workflow_id with the correct initiated_event_id and parent_close_policy.
2. FOR ALL ChildStartConfirmed(Started) transitions, THE next_state.children map SHALL contain the child entry with started_event_id set to Some and child_run_id set to Some.
3. FOR ALL ChildStartConfirmed(Failed) transitions, THE next_state.children map SHALL NOT contain the child entry.
4. FOR ALL ChildResolved transitions, THE next_state.children map SHALL NOT contain the resolved child entry.

### Requirement 7.5: Terminal State Invariants for Close with Children

**User Story:** As a Tokeira developer, I want all close paths to clear the children map, so that no orphaned child entries remain in terminal state.

#### Acceptance Criteria

1. FOR ALL Terminate transitions, next_state.children SHALL be empty.
2. FOR ALL WorkflowExecutionTimedOut transitions, next_state.children SHALL be empty.
3. FOR ALL WorkflowTaskCompleted transitions containing a CompleteWorkflow command, next_state.children SHALL be empty.
4. FOR ALL WorkflowTaskCompleted transitions containing a FailWorkflow command, next_state.children SHALL be empty.
5. FOR ALL WorkflowTaskCompleted transitions containing a CancelWorkflow command, next_state.children SHALL be empty.
6. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, next_state.children SHALL be empty.

### Requirement 7.6: Parent Close Policy Dispatch Op Consistency

**User Story:** As a Tokeira developer, I want the number of Parent Close Policy dispatch ops to match the number of started children with non-Abandon policies, so that cleanup is complete and not over-counted.

#### Acceptance Criteria

1. FOR ALL close transitions, THE number of DispatchOp::TerminateChild ops SHALL equal the number of open children with ParentClosePolicy::Terminate and child_run_id Some in the input state.
2. FOR ALL close transitions, THE number of DispatchOp::CancelChild ops SHALL equal the number of open children with ParentClosePolicy::RequestCancel and child_run_id Some in the input state.
3. FOR ALL close transitions, no DispatchOp::TerminateChild or DispatchOp::CancelChild SHALL be emitted for children with ParentClosePolicy::Abandon.
4. FOR ALL close transitions, no DispatchOp::TerminateChild or DispatchOp::CancelChild SHALL be emitted for children with child_run_id None.

### Requirement 7.7: No RequestDedupeOp for Internal Commands

**User Story:** As a Tokeira developer, I want ChildStartConfirmed and ChildResolved to never emit RequestDedupeOp, so that the internal/external command distinction is maintained.

#### Acceptance Criteria

1. FOR ALL ChildStartConfirmed transitions, request_dedupe_ops SHALL be empty.
2. FOR ALL ChildResolved transitions, request_dedupe_ops SHALL be empty.

---

## Property Tests

### Requirement 8.1: StartChildWorkflow Creates Child Entry Property

**User Story:** As a Tokeira developer, I want a property test verifying that StartChildWorkflow always creates a child entry with correct initial state, so that child initiation is guaranteed correct.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState and FOR ALL valid StartChildWorkflow commands with unique child_workflow_id, WHEN the command is applied within a WorkflowTaskCompleted, THE next_state.children SHALL contain an entry for the child_workflow_id with child_run_id None, started_event_id None, and the specified parent_close_policy.

### Requirement 8.2: StartChildWorkflow Emits DispatchOp Property

**User Story:** As a Tokeira developer, I want a property test verifying that StartChildWorkflow always emits a DispatchOp::StartChildWorkflow, so that the runtime is always notified.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a StartChildWorkflow command with a unique child_workflow_id, THE dispatch_ops SHALL contain a DispatchOp::StartChildWorkflow with the matching child_workflow_id.

### Requirement 8.3: ChildStartConfirmed WFT Coalescing Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildStartConfirmed follows the WFT coalescing pattern, so that the at-most-one-WFT invariant is maintained.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a known child and no pending WFT, WHEN ChildStartConfirmed(Started) is applied, THE next_state SHALL have a pending WFT and dispatch_ops SHALL contain one EnqueueWorkflowTask.
2. FOR ALL valid open WorkflowState with a known child and a pending WFT, WHEN ChildStartConfirmed(Started) is applied, THE dispatch_ops SHALL NOT contain an EnqueueWorkflowTask.

### Requirement 8.4: ChildStartConfirmed Failure Removes Child Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildStartConfirmed(Failed) removes the child from state, so that failed children do not linger.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a known child, WHEN ChildStartConfirmed(Failed) is applied, THE next_state.children SHALL NOT contain the child_workflow_id.

### Requirement 8.5: ChildResolved Removes Child Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildResolved always removes the child from state, so that resolved children do not linger.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a known child and FOR ALL ChildResolution variants, WHEN ChildResolved is applied, THE next_state.children SHALL NOT contain the child_workflow_id.

### Requirement 8.6: ChildResolved WFT Coalescing Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildResolved follows the WFT coalescing pattern, so that the at-most-one-WFT invariant is maintained.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a known child and no pending WFT, WHEN ChildResolved is applied, THE next_state SHALL have a pending WFT.
2. FOR ALL valid open WorkflowState with a known child and a pending WFT, WHEN ChildResolved is applied, THE dispatch_ops SHALL NOT contain an EnqueueWorkflowTask.

### Requirement 8.7: Parent Close Policy on Terminate Cleans Up All Children Property

**User Story:** As a Tokeira developer, I want a property test verifying that Terminate cleans up all open children via Parent Close Policy, so that no orphaned child entries remain.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with N open children, WHEN Terminate is applied, THE next_state.children SHALL be empty.
2. FOR ALL valid open WorkflowState with open children, WHEN Terminate is applied, THE number of DispatchOp::TerminateChild ops SHALL equal the number of children with ParentClosePolicy::Terminate and child_run_id Some, and THE number of DispatchOp::CancelChild ops SHALL equal the number of children with ParentClosePolicy::RequestCancel and child_run_id Some.

### Requirement 8.8: Parent Close Policy on All Close Paths Property

**User Story:** As a Tokeira developer, I want a property test verifying that all close paths (Terminate, TimedOut, Complete, Fail, Cancel, ContinueAsNew) clear the children map, so that the terminal state invariant is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid close transitions (Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew), THE next_state.children SHALL be empty.

### Requirement 8.9: ChildStartConfirmed Fencing Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildStartConfirmed rejects stale confirmations, so that fencing is enforced.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a known child, WHEN ChildStartConfirmed is applied with an initiated_event_id that does not match the child entry's initiated_event_id, THE Kernel SHALL reject with StaleChildConfirmation.

### Requirement 8.10: No RequestDedupeOp for Child Commands Property

**User Story:** As a Tokeira developer, I want a property test verifying that ChildStartConfirmed and ChildResolved never emit RequestDedupeOp, so that the internal/external command distinction is enforced.

#### Acceptance Criteria

1. FOR ALL valid ChildStartConfirmed transitions, request_dedupe_ops SHALL be empty.
2. FOR ALL valid ChildResolved transitions, request_dedupe_ops SHALL be empty.

### Requirement 8.11: Duplicate Child Workflow ID Rejection Property

**User Story:** As a Tokeira developer, I want a property test verifying that StartChildWorkflow rejects duplicate child_workflow_ids, so that the uniqueness constraint is enforced.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a child_workflow_id already in the children map, WHEN a StartChildWorkflow command with the same child_workflow_id is applied, THE Kernel SHALL reject with DuplicateChildWorkflowId.

---

## Workspace Compile Checkpoint

### Requirement 9.1: Workspace Compilation After Type Changes

**User Story:** As a Tokeira developer, I want the workspace to compile after all type changes are made, so that downstream breakage from new enum variants and struct fields is caught early.

#### Acceptance Criteria

1. AFTER adding ChildWorkflowState, ParentClosePolicy, new Command variants, new WorkflowCommand variant, new HistoryEventKind variants, new DispatchOp variants, new Reject variants, and the children field to WorkflowState, THE workspace SHALL compile with `cargo check --workspace`.
2. AFTER extending Terminate and WorkflowExecutionTimedOut to apply Parent Close Policy, THE workspace SHALL compile with `cargo check --workspace`.
