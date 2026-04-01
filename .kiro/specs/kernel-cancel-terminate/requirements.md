# Requirements Document: Cancel and Terminate (Feature 3)

## Introduction

This document captures the requirements for Feature 3 of the Tokeira kernel implementation: Cancel and Terminate command handling. Feature 3 depends on Feature 1 (kernel-foundation-wft-lifecycle), which is complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 3.1–3.7).

Feature 3 adds two top-level kernel commands (Cancel and Terminate) and three workflow commands (CancelWorkflow, RequestCancelActivity, CancelTimer). These commands implement the two cancellation paradigms in Temporal's model:

- **Cancel** is cooperative (two-phase): the kernel records the request and schedules a WFT so the workflow code can decide how to handle it. The run stays open.
- **Terminate** is unconditional: the kernel closes the run immediately with `ExecutionStatus::Terminated`, cleans up all open entities, and does NOT schedule a WFT.

Both Cancel and Terminate are external API commands that carry `RequestContext` and emit `RequestDedupeOp`. Cancel follows the same WFT coalescing pattern as Signal (schedule WFT if none pending). Terminate follows the same close mechanics as CompleteWorkflow/FailWorkflow but additionally cleans up open activities and timers.

The three workflow commands are issued by workflow code within `WorkflowTaskCompleted`:
- **CancelWorkflow** is a terminal command that closes the run with `ExecutionStatus::Cancelled` after the workflow has performed cleanup.
- **RequestCancelActivity** requests cancellation of an in-progress activity but does NOT remove it — the activity stays pending until resolved.
- **CancelTimer** immediately removes the timer from state (unlike RequestCancelActivity).

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call. Contains next_state, history events, dispatch ops, projection ops, activity/timer ops, and request dedupe ops.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **PendingWorkflowTask**: The authoritative record that a WFT exists for the run, tracking logical_seq, scheduled/started event IDs, and attempt count.
- **StickyAffinity**: Worker preference recorded on run state when a worker provides a sticky_ttl during WorkflowTaskStarted.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **RequestContext**: Metadata carried by external API commands containing a request_id for deduplication and optional caller_identity.
- **ActivityState**: Tracking record for an open activity in WorkflowState.
- **TimerState**: Tracking record for an open timer in WorkflowState.
- **CancelRequest**: The request struct for the Cancel command, carrying the cancellation reason, optional external initiator workflow execution, and RequestContext.
- **TerminateRequest**: The request struct for the Terminate command, carrying the reason, optional details, caller identity, and RequestContext.
- **ActivityOp**: An operation emitted by the Kernel for activity lifecycle management (Upsert or Delete).
- **TimerOp**: An operation emitted by the Kernel for timer lifecycle management (Upsert or Delete).

## Requirements

---

## New Types and Command Variants

### Requirement 1.1: Cancel Command Variant

**User Story:** As a Tokeira developer, I want a Cancel variant in the Command enum, so that external callers and parent workflows can request graceful cancellation.

#### Acceptance Criteria

1. THE Command enum SHALL include a `Cancel(CancelRequest)` variant.
2. THE CancelRequest struct SHALL include a `reason` field of type `String` carrying the cancellation reason.
3. THE CancelRequest struct SHALL include an `external_initiator` field of type `Option<ExternalWorkflowExecution>` identifying the external workflow that initiated the cancellation (for parent-driven cancellation).
4. THE CancelRequest struct SHALL include a `request` field of type `RequestContext` for deduplication.
5. THE CancelRequest struct SHALL include a `now` field of type `OffsetDateTime` for the event timestamp.
6. THE CancelRequest struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.1a: ExternalWorkflowExecution Type

**User Story:** As a Tokeira developer, I want an ExternalWorkflowExecution type to identify the workflow that initiated a cancellation, so that parent-driven cancellation is traceable in history.

#### Acceptance Criteria

1. THE `ExternalWorkflowExecution` struct SHALL include a `namespace_id` field of type `NamespaceId`.
2. THE `ExternalWorkflowExecution` struct SHALL include a `workflow_id` field of type `WorkflowId`.
3. THE `ExternalWorkflowExecution` struct SHALL include a `run_id` field of type `RunId`.
4. THE `ExternalWorkflowExecution` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.2: Terminate Command Variant

**User Story:** As a Tokeira developer, I want a Terminate variant in the Command enum, so that external callers and operators can hard-stop a workflow run.

#### Acceptance Criteria

1. THE Command enum SHALL include a `Terminate(TerminateRequest)` variant.
2. THE TerminateRequest struct SHALL include a `reason` field of type `String` carrying the termination reason.
3. THE TerminateRequest struct SHALL include a `details` field of type `Option<Payloads>` carrying optional structured termination details.
4. THE TerminateRequest struct SHALL include an `identity` field of type `String` identifying the caller who initiated the termination.
5. THE TerminateRequest struct SHALL include a `request` field of type `RequestContext` for deduplication.
6. THE TerminateRequest struct SHALL include a `now` field of type `OffsetDateTime` for the event timestamp.
7. THE TerminateRequest struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.3: CancelWorkflow Workflow Command Variant

**User Story:** As a Tokeira developer, I want a CancelWorkflow variant in the WorkflowCommand enum, so that workflow code can confirm cancellation after cleanup.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `CancelWorkflow` variant.
2. THE CancelWorkflow variant SHALL be a unit variant (no fields); the cancellation reason was already recorded in the WorkflowExecutionCancelRequested event.

### Requirement 1.4: RequestCancelActivity Workflow Command Variant

**User Story:** As a Tokeira developer, I want a RequestCancelActivity variant in the WorkflowCommand enum, so that workflow code can request cancellation of in-progress activities.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `RequestCancelActivity` variant with an `activity_id` field of type `String`.

### Requirement 1.5: CancelTimer Workflow Command Variant

**User Story:** As a Tokeira developer, I want a CancelTimer variant in the WorkflowCommand enum, so that workflow code can cancel pending timers.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `CancelTimer` variant with a `timer_id` field of type `String`.

### Requirement 1.6: New History Event Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for cancel and terminate events, so that these lifecycle events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowExecutionCancelRequested` variant with fields: `reason` (String), `external_workflow_execution` (Option<ExternalWorkflowExecution>), and `request_id` (String).
2. THE HistoryEventKind enum SHALL include a `WorkflowExecutionTerminated` variant with fields: `reason` (String), `details` (Option<Payloads>), and `identity` (String).
3. THE HistoryEventKind enum SHALL include a `WorkflowExecutionCanceled` variant (unit variant, no fields).
4. THE HistoryEventKind enum SHALL include an `ActivityTaskCancelRequested` variant with field: `activity_id` (String).
5. THE HistoryEventKind enum SHALL include a `TimerCanceled` variant with field: `timer_id` (String).

---

## Cancel Command Behavior

### Requirement 2.1: Cancel Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to record cancellation requests and schedule a WFT, so that workflows can be gracefully stopped with an opportunity for cleanup.

#### Acceptance Criteria

1. WHEN a Cancel command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Cancel command is received, THE Kernel SHALL emit a WorkflowExecutionCancelRequested event carrying the reason, the external_workflow_execution (if present), and the request_id.
3. WHEN a Cancel command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a Cancel command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. WHEN a Cancel command is received, THE Kernel SHALL NOT close the run; cancellation is a cooperative two-phase operation.
6. WHEN a Cancel command is received, THE Kernel SHALL NOT emit any ProjectionOp (the run remains open and unchanged from a visibility perspective).
7. WHEN a Cancel command is received, THE Kernel SHALL NOT emit any ActivityOp or TimerOp (open entities are not affected by a cancel request).
8. WHEN a Cancel command is received, THE next_state.status SHALL remain ExecutionStatus::Running.

### Requirement 2.2: Cancel Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid Cancel commands, so that cancellation of non-existent or already-closed runs is caught.

#### Acceptance Criteria

1. WHEN a Cancel command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
2. WHEN a Cancel command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## Terminate Command Behavior

### Requirement 3.1: Terminate Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to immediately terminate a workflow run, so that hard-stop semantics are available without consulting the worker.

#### Acceptance Criteria

1. WHEN a Terminate command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Terminate command is received, THE Kernel SHALL emit a WorkflowExecutionTerminated event carrying the reason, optional details, and caller identity.
3. WHEN a Terminate command is received, THE Kernel SHALL close the run with ExecutionStatus::Terminated by calling the TransitionBuilder's `close` method (set terminal status, clear pending WFT, clear StickyAffinity, emit ProjectionOp::CloseExecution).
4. WHEN a Terminate command is received, THE Kernel SHALL NOT schedule a workflow task; the worker is not consulted.
5. WHEN a Terminate command is received, THE Kernel SHALL NOT emit any DispatchOp (no WFT, no activity tasks, no timer tasks).

### Requirement 3.2: Terminate Entity Cleanup

**User Story:** As a Tokeira developer, I want Terminate to clean up all open entities, so that no orphaned activities or timers remain after a hard stop.

**Scope note:** The architecture doc (020-kernel.md) specifies that Terminate should also apply Parent Close Policy to open child workflows. Child workflow tracking is not yet implemented (it is Feature 5). Feature 3 implements Terminate's cleanup for activities and timers only. When Feature 5 adds child workflow support, Terminate's cleanup logic must be extended to apply Parent Close Policy to open children.

#### Acceptance Criteria

1. WHEN a Terminate command is received and open activities exist, THE Kernel SHALL emit an ActivityOp::Delete for each open activity.
2. WHEN a Terminate command is received and open timers exist, THE Kernel SHALL emit a TimerOp::Delete for each open timer.
3. WHEN a Terminate command is received, THE Kernel SHALL clear the activities map in next_state (next_state.activities SHALL be empty).
4. WHEN a Terminate command is received, THE Kernel SHALL clear the timers map in next_state (next_state.timers SHALL be empty).
5. WHEN a Terminate command is received with no open activities or timers, THE Kernel SHALL emit no ActivityOp or TimerOp (cleanup is a no-op when there are no open entities).

### Requirement 3.3: Terminate Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid Terminate commands, so that termination of non-existent or already-closed runs is caught.

#### Acceptance Criteria

1. WHEN a Terminate command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
2. WHEN a Terminate command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## CancelWorkflow Workflow Command Behavior

### Requirement 4.1: CancelWorkflow Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle the CancelWorkflow workflow command, so that workflow code can confirm cancellation after cleanup.

#### Acceptance Criteria

1. WHEN a CancelWorkflow workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionCanceled event.
2. WHEN a CancelWorkflow workflow command is received, THE Kernel SHALL close the run with ExecutionStatus::Cancelled by calling the TransitionBuilder's `close` method.
3. WHEN a CancelWorkflow workflow command is received, THE apply_workflow_command function SHALL return `true` (indicating the run is closed), so that subsequent workflow commands in the same WFT completion are rejected with CommandsAfterClose.

---

## RequestCancelActivity Workflow Command Behavior

### Requirement 5.1: RequestCancelActivity Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle activity cancellation requests from workflow code, so that workflows can request cancellation of in-progress activities.

#### Acceptance Criteria

1. WHEN a RequestCancelActivity workflow command is received for an open activity, THE Kernel SHALL emit an ActivityTaskCancelRequested event carrying the activity_id.
2. WHEN a RequestCancelActivity workflow command is received, THE Kernel SHALL keep the activity in the pending activities map (next_state.activities SHALL still contain the activity).
3. WHEN a RequestCancelActivity workflow command is received, THE Kernel SHALL NOT emit any ActivityOp (the activity is not deleted or modified; it remains pending until resolved).
4. WHEN a RequestCancelActivity workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

### Requirement 5.2: RequestCancelActivity Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject RequestCancelActivity for unknown activities, so that invalid cancel requests are caught.

#### Acceptance Criteria

1. WHEN a RequestCancelActivity workflow command is received for an activity_id that is not in the open activities map, THE Kernel SHALL reject with UnknownActivity.

### Requirement 5.3: Activity Cancellation Lifecycle

**User Story:** As a Tokeira developer, I want the full activity cancellation lifecycle to work end-to-end, so that RequestCancelActivity followed by ActivityResolved(Canceled) produces the correct events and state changes.

#### Acceptance Criteria

1. WHEN a RequestCancelActivity workflow command is received followed later by an ActivityResolved command with a Canceled resolution for the same activity, THE Kernel SHALL have emitted both an ActivityTaskCancelRequested event (from RequestCancelActivity) and an ActivityTaskCanceled event (from ActivityResolved).
2. WHEN an ActivityResolved command with a Canceled resolution is received, THE Kernel SHALL remove the activity from the activities map and push an ActivityOp::Delete (this behavior is already implemented in Feature 1).

---

## CancelTimer Workflow Command Behavior

### Requirement 6.1: CancelTimer Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle timer cancellation from workflow code, so that workflows can cancel pending timers.

#### Acceptance Criteria

1. WHEN a CancelTimer workflow command is received for an open timer, THE Kernel SHALL emit a TimerCanceled event carrying the timer_id.
2. WHEN a CancelTimer workflow command is received, THE Kernel SHALL remove the timer from the timers map in next_state.
3. WHEN a CancelTimer workflow command is received, THE Kernel SHALL push a TimerOp::Delete for the canceled timer.
4. WHEN a CancelTimer workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

### Requirement 6.2: CancelTimer Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject CancelTimer for unknown timers, so that invalid cancel requests are caught.

#### Acceptance Criteria

1. WHEN a CancelTimer workflow command is received for a timer_id that is not in the open timers map, THE Kernel SHALL reject with UnknownTimer.

---

## Reject Taxonomy Extensions

### Requirement 7.1: Reject Variants for Feature 3

**User Story:** As a Tokeira developer, I want the Kernel's Reject enum to cover rejection reasons specific to Feature 3, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Kernel SHALL reuse the existing `UnknownActivity` Reject variant when RequestCancelActivity references an activity_id not in the open activities map.
2. THE Kernel SHALL reuse the existing `UnknownTimer` Reject variant when CancelTimer references a timer_id not in the open timers map.
3. THE Kernel SHALL reuse the existing `MissingRun` and `RunClosed` Reject variants for Cancel and Terminate commands targeting absent or closed runs.

---

## BasicKernel Integration

### Requirement 8.1: BasicKernel Apply Routing for Cancel and Terminate

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route Cancel and Terminate commands to dedicated handler methods, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a Cancel command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_cancel` method.
2. WHEN a Terminate command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_terminate` method.
3. THE `apply_cancel` method SHALL follow the same pattern as `apply_signal`: call `expect_open`, construct a TransitionBuilder, emit dedupe op, emit event, conditionally schedule WFT, and call `finish`.
4. THE `apply_terminate` method SHALL follow the same pattern as existing apply methods: call `expect_open`, construct a TransitionBuilder, emit dedupe op, emit event, call `close`, clean up entities, and call `finish`.

### Requirement 8.2: Workflow Command Dispatch for Feature 3

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle CancelWorkflow, RequestCancelActivity, and CancelTimer, so that these workflow commands are processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::CancelWorkflow` that emits WorkflowExecutionCanceled and calls `close(ExecutionStatus::Cancelled)`.
2. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::RequestCancelActivity` that validates the activity exists and emits ActivityTaskCancelRequested.
3. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::CancelTimer` that validates the timer exists, emits TimerCanceled, removes the timer, and pushes TimerOp::Delete.

---

## Structural Invariants

### Requirement 9.1: Event ID Contiguity for Cancel and Terminate

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for Cancel and Terminate transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL Cancel transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL Terminate transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL Cancel and Terminate transitions, next_state.last_event_id SHALL equal the last emitted event's event_id.

### Requirement 9.2: Transition Sequence Increment for Cancel and Terminate

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for Cancel and Terminate, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL Cancel transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.
2. FOR ALL Terminate transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 9.3: At-Most-One-WFT Invariant for Cancel

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after Cancel, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL Cancel transitions, next_state SHALL contain at most one PendingWorkflowTask.
2. WHEN Cancel schedules a WFT, THE dispatch_ops SHALL contain exactly one EnqueueWorkflowTask.
3. WHEN Cancel coalesces (WFT already pending), THE dispatch_ops SHALL be empty.

### Requirement 9.4: Terminal State Invariants for Terminate

**User Story:** As a Tokeira developer, I want Terminate to satisfy all terminal state invariants, so that the closed run is well-formed.

#### Acceptance Criteria

1. FOR ALL Terminate transitions, next_state.pending_workflow_task SHALL be None.
2. FOR ALL Terminate transitions, next_state.sticky SHALL be None.
3. FOR ALL Terminate transitions, next_state.closed_at SHALL be Some.
4. FOR ALL Terminate transitions, next_state.status SHALL be ExecutionStatus::Terminated.
5. FOR ALL Terminate transitions, next_state.activities SHALL be empty.
6. FOR ALL Terminate transitions, next_state.timers SHALL be empty.
7. FOR ALL Terminate transitions, dispatch_ops SHALL be empty (no WFT is scheduled).

### Requirement 9.5: Entity Cleanup Consistency for Terminate

**User Story:** As a Tokeira developer, I want the number of ActivityOp::Delete and TimerOp::Delete ops emitted by Terminate to match the number of open entities in the input state, so that cleanup is complete and not over-counted.

#### Acceptance Criteria

1. FOR ALL Terminate transitions, THE number of ActivityOp::Delete ops SHALL equal the number of entries in the input state's activities map.
2. FOR ALL Terminate transitions, THE number of TimerOp::Delete ops SHALL equal the number of entries in the input state's timers map.
3. FOR ALL Terminate transitions, every ActivityOp::Delete SHALL reference an activity_id that existed in the input state's activities map.
4. FOR ALL Terminate transitions, every TimerOp::Delete SHALL reference a timer_id that existed in the input state's timers map.

### Requirement 9.6: CancelWorkflow Terminal State Invariants

**User Story:** As a Tokeira developer, I want CancelWorkflow to satisfy terminal state invariants, so that the canceled run is well-formed.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a CancelWorkflow command, next_state.status SHALL be ExecutionStatus::Cancelled.
2. FOR ALL WorkflowTaskCompleted transitions containing a CancelWorkflow command, next_state.pending_workflow_task SHALL be None.
3. FOR ALL WorkflowTaskCompleted transitions containing a CancelWorkflow command, next_state.closed_at SHALL be Some.

### Requirement 9.7: RequestCancelActivity State Preservation

**User Story:** As a Tokeira developer, I want RequestCancelActivity to preserve the activity in state, so that the activity lifecycle is not prematurely terminated.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a RequestCancelActivity command for a valid activity, THE activity SHALL remain in next_state.activities.
2. FOR ALL WorkflowTaskCompleted transitions containing a RequestCancelActivity command, THE activity_ops SHALL NOT contain an ActivityOp::Delete for that activity.

### Requirement 9.8: CancelTimer State Removal

**User Story:** As a Tokeira developer, I want CancelTimer to remove the timer from state, so that the timer is immediately cleaned up.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a CancelTimer command for a valid timer, THE timer SHALL NOT be in next_state.timers.
2. FOR ALL WorkflowTaskCompleted transitions containing a CancelTimer command, THE timer_ops SHALL contain a TimerOp::Delete for that timer.

---

## Property Tests

### Requirement 10.1: Cancel Does Not Close the Run Property

**User Story:** As a Tokeira developer, I want a property test verifying that Cancel never closes the run, so that the cooperative cancellation contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState (with or without pending WFT, with or without open activities/timers) and FOR ALL valid CancelRequest values, WHEN Cancel is applied, THE next_state.status SHALL be ExecutionStatus::Running and next_state.closed_at SHALL be None.

### Requirement 10.2: Cancel WFT Coalescing Property

**User Story:** As a Tokeira developer, I want a property test verifying that Cancel follows the WFT coalescing pattern, so that the at-most-one-WFT invariant is maintained.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with no pending WFT and FOR ALL valid CancelRequest values, WHEN Cancel is applied, THE next_state SHALL have a pending WFT and dispatch_ops SHALL contain one EnqueueWorkflowTask.
2. FOR ALL valid open WorkflowState with a pending WFT and FOR ALL valid CancelRequest values, WHEN Cancel is applied, THE dispatch_ops SHALL be empty (no additional WFT scheduled).

### Requirement 10.3: Cancel Emits Request Dedupe Property

**User Story:** As a Tokeira developer, I want a property test verifying that Cancel always emits a RequestDedupeOp, so that idempotent handling is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid Cancel transitions, THE request_dedupe_ops SHALL contain exactly one RequestDedupeOp with the request_id from the CancelRequest.

### Requirement 10.4: Terminate Closes the Run Property

**User Story:** As a Tokeira developer, I want a property test verifying that Terminate always closes the run with Terminated status, so that the hard-stop contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState and FOR ALL valid TerminateRequest values, WHEN Terminate is applied, THE next_state.status SHALL be ExecutionStatus::Terminated and next_state.closed_at SHALL be Some.

### Requirement 10.5: Terminate Cleans Up All Open Entities Property

**User Story:** As a Tokeira developer, I want a property test verifying that Terminate cleans up all open activities and timers, so that no orphaned entities remain.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with N open activities and M open timers, WHEN Terminate is applied, THE activity_ops SHALL contain exactly N ActivityOp::Delete ops and THE timer_ops SHALL contain exactly M TimerOp::Delete ops, and next_state.activities and next_state.timers SHALL both be empty.

### Requirement 10.6: Terminate Emits No Dispatch Ops Property

**User Story:** As a Tokeira developer, I want a property test verifying that Terminate never schedules a WFT, so that the worker is never consulted after a hard stop.

#### Acceptance Criteria

1. FOR ALL valid Terminate transitions, THE dispatch_ops SHALL be empty.

### Requirement 10.7: Terminate Emits Request Dedupe Property

**User Story:** As a Tokeira developer, I want a property test verifying that Terminate always emits a RequestDedupeOp, so that idempotent handling is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid Terminate transitions, THE request_dedupe_ops SHALL contain exactly one RequestDedupeOp with the request_id from the TerminateRequest.

### Requirement 10.8: CancelWorkflow Is Terminal Property

**User Story:** As a Tokeira developer, I want a property test verifying that CancelWorkflow closes the run with Canceled status, so that the terminal command contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a CancelWorkflow command as the last (or only) workflow command, THE next_state.status SHALL be ExecutionStatus::Cancelled and next_state.closed_at SHALL be Some.

### Requirement 10.9: RequestCancelActivity Preserves Activity Property

**User Story:** As a Tokeira developer, I want a property test verifying that RequestCancelActivity does not remove the activity from state, so that the activity lifecycle is not prematurely terminated.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a RequestCancelActivity command for a valid activity, THE activity SHALL remain in next_state.activities with the same ActivityState as before.

### Requirement 10.10: CancelTimer Removes Timer Property

**User Story:** As a Tokeira developer, I want a property test verifying that CancelTimer removes the timer from state and emits TimerOp::Delete, so that timer cleanup is immediate.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a CancelTimer command for a valid timer, THE timer SHALL NOT be in next_state.timers and THE timer_ops SHALL contain a TimerOp::Delete for that timer_id.

### Requirement 10.11: Structural Invariants Hold for New Commands Property

**User Story:** As a Tokeira developer, I want the existing structural invariant properties (event ID contiguity, transition_seq increment, at-most-one-WFT, last_event_id consistency) to cover Cancel and Terminate, so that the universal invariants are not violated by the new commands.

#### Acceptance Criteria

1. FOR ALL valid Cancel and Terminate transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL valid Cancel and Terminate transitions, next_state.transition_seq SHALL equal expected_seq + 1.
3. FOR ALL valid Cancel transitions, next_state SHALL contain at most one PendingWorkflowTask.
4. FOR ALL valid Terminate transitions, next_state.pending_workflow_task SHALL be None.
5. FOR ALL valid Cancel and Terminate transitions, next_state.last_event_id SHALL equal the last emitted event's event_id.

---

## Golden Transition Tests

### Requirement 11.1: Cancel with No Pending WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Cancel on an open run with no pending WFT, so that the exact transition output is pinned.

#### Acceptance Criteria

1. WHEN a Cancel command is applied to an open run with no pending WFT, THE test SHALL assert the exact Transition including: one WorkflowExecutionCancelRequested event with correct reason, external_workflow_execution, and request_id; one WorkflowTaskScheduled event; next_state with status Running, a pending WFT, and unchanged activities/timers; one RequestDedupeOp; one DispatchOp::EnqueueWorkflowTask; empty activity_ops, timer_ops, and projection_ops.

### Requirement 11.2: Cancel with Pending WFT (Coalescing) Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Cancel on an open run with a pending WFT, so that the coalescing behavior is pinned.

#### Acceptance Criteria

1. WHEN a Cancel command is applied to an open run with a pending WFT, THE test SHALL assert the exact Transition including: one WorkflowExecutionCancelRequested event; NO WorkflowTaskScheduled event; next_state with the same pending WFT as input; one RequestDedupeOp; empty dispatch_ops, activity_ops, timer_ops, and projection_ops.

### Requirement 11.3: Cancel with External Initiator Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Cancel with an external initiator (parent-driven cancellation), so that the external_workflow_execution field is correctly recorded.

#### Acceptance Criteria

1. WHEN a Cancel command with a non-None external_initiator is applied, THE test SHALL assert that the WorkflowExecutionCancelRequested event carries the external_workflow_execution with the correct namespace_id, workflow_id, and run_id.

### Requirement 11.4: Cancel Rejection Path Golden Tests

**User Story:** As a Tokeira developer, I want golden tests for all Cancel rejection paths, so that error conditions are pinned.

#### Acceptance Criteria

1. WHEN a Cancel command is applied to LoadedRun::Absent, THE test SHALL assert Reject::MissingRun.
2. WHEN a Cancel command is applied to a closed run, THE test SHALL assert Reject::RunClosed.

### Requirement 11.5: Terminate with No Open Entities Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Terminate on an open run with no open activities or timers, so that the minimal terminate transition is pinned.

#### Acceptance Criteria

1. WHEN a Terminate command is applied to an open run with no open activities or timers, THE test SHALL assert the exact Transition including: one WorkflowExecutionTerminated event with correct reason, details, and identity; next_state with status Terminated, closed_at set, no pending WFT, no sticky, empty activities and timers; one RequestDedupeOp; one ProjectionOp::CloseExecution with Terminated status; empty dispatch_ops, activity_ops, and timer_ops.

### Requirement 11.6: Terminate with Open Activities and Timers Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Terminate on an open run with open activities and timers, so that entity cleanup is pinned.

#### Acceptance Criteria

1. WHEN a Terminate command is applied to an open run with two open activities and one open timer, THE test SHALL assert the exact Transition including: one WorkflowExecutionTerminated event; next_state with empty activities and timers; two ActivityOp::Delete ops (one per activity); one TimerOp::Delete op; one RequestDedupeOp; one ProjectionOp::CloseExecution; empty dispatch_ops.

### Requirement 11.7: Terminate with Pending WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Terminate on an open run with a pending WFT, so that WFT clearing on terminate is pinned.

#### Acceptance Criteria

1. WHEN a Terminate command is applied to an open run with a pending WFT, THE test SHALL assert that next_state.pending_workflow_task is None and dispatch_ops is empty (the pending WFT is cleared, not re-dispatched).

### Requirement 11.8: Terminate Rejection Path Golden Tests

**User Story:** As a Tokeira developer, I want golden tests for all Terminate rejection paths, so that error conditions are pinned.

#### Acceptance Criteria

1. WHEN a Terminate command is applied to LoadedRun::Absent, THE test SHALL assert Reject::MissingRun.
2. WHEN a Terminate command is applied to a closed run, THE test SHALL assert Reject::RunClosed.

### Requirement 11.9: CancelWorkflow Workflow Command Golden Test

**User Story:** As a Tokeira developer, I want a golden test for CancelWorkflow within a WorkflowTaskCompleted, so that the terminal workflow command behavior is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing a CancelWorkflow workflow command is applied, THE test SHALL assert the exact Transition including: one WorkflowTaskCompleted event, one WorkflowExecutionCanceled event; next_state with status Canceled, closed_at set, no pending WFT, no sticky; one ProjectionOp::CloseExecution with Canceled status; empty request_dedupe_ops, activity_ops, timer_ops.

### Requirement 11.10: CancelWorkflow Followed by Another Command Golden Test

**User Story:** As a Tokeira developer, I want a golden test verifying that commands after CancelWorkflow are rejected, so that the terminal command contract is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing CancelWorkflow followed by a RequestNewWorkflowTask is applied, THE test SHALL assert Reject::CommandsAfterClose with the correct index.

### Requirement 11.11: RequestCancelActivity Golden Test

**User Story:** As a Tokeira developer, I want a golden test for RequestCancelActivity within a WorkflowTaskCompleted, so that the activity cancel request behavior is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing a RequestCancelActivity workflow command for an open activity is applied, THE test SHALL assert the exact Transition including: one WorkflowTaskCompleted event, one ActivityTaskCancelRequested event with the correct activity_id; next_state with the activity still in the activities map; empty activity_ops (no delete).

### Requirement 11.12: RequestCancelActivity for Unknown Activity Golden Test

**User Story:** As a Tokeira developer, I want a golden test for RequestCancelActivity targeting an unknown activity, so that the rejection is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing a RequestCancelActivity for a non-existent activity_id is applied, THE test SHALL assert Reject::UnknownActivity.

### Requirement 11.13: CancelTimer Golden Test

**User Story:** As a Tokeira developer, I want a golden test for CancelTimer within a WorkflowTaskCompleted, so that the timer cancellation behavior is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing a CancelTimer workflow command for an open timer is applied, THE test SHALL assert the exact Transition including: one WorkflowTaskCompleted event, one TimerCanceled event with the correct timer_id; next_state with the timer removed from the timers map; one TimerOp::Delete for the timer.

### Requirement 11.14: CancelTimer for Unknown Timer Golden Test

**User Story:** As a Tokeira developer, I want a golden test for CancelTimer targeting an unknown timer, so that the rejection is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command containing a CancelTimer for a non-existent timer_id is applied, THE test SHALL assert Reject::UnknownTimer.

### Requirement 11.15: RequestCancelActivity Then ActivityResolved(Canceled) Golden Test

**User Story:** As a Tokeira developer, I want a golden test for the full activity cancellation lifecycle, so that the two-step cancel flow is pinned.

#### Acceptance Criteria

1. THE test SHALL apply a WorkflowTaskCompleted containing RequestCancelActivity for an open activity, then apply an ActivityResolved command with Canceled resolution for the same activity. THE test SHALL assert that the first transition contains an ActivityTaskCancelRequested event with the activity still in state, and the second transition contains an ActivityTaskCanceled event with the activity removed from state and an ActivityOp::Delete emitted.

### Requirement 11.16: Cancel Then CancelWorkflow End-to-End Golden Test

**User Story:** As a Tokeira developer, I want a golden test for the full cooperative cancellation lifecycle (Cancel → WFT → CancelWorkflow), so that the two-phase cancel flow is pinned.

#### Acceptance Criteria

1. THE test SHALL apply a Cancel command (producing WorkflowExecutionCancelRequested and scheduling a WFT), then apply WorkflowTaskStarted, then apply WorkflowTaskCompleted containing CancelWorkflow. THE test SHALL assert that the final transition closes the run with ExecutionStatus::Cancelled.
