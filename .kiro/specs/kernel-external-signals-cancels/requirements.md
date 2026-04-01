# Requirements Document: External Signals and Cancel Requests (Feature 6)

## Introduction

This document captures the requirements for Feature 6 of the Tokeira kernel implementation: External Signals and Cancel Requests. Feature 6 depends on Feature 1 (Foundation + WFT lifecycle) and Feature 3 (Cancel and Terminate), both of which are complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 6.1–6.4).

Feature 6 adds the ability for workflow code to signal or cancel other workflow executions. This follows the same initiation/resolution pattern as child workflows (Feature 5) but is structurally simpler:

- **No start confirmation step.** Signals and cancel requests are fire-and-forget from the kernel's perspective — the runtime confirms delivery via resolution commands.
- **No parent close policy.** When the parent closes, the kernel discards all pending external signal and cancel entries by clearing the maps. No dispatch ops are emitted for discarded entries. Any late runtime resolution arriving after the close will be rejected with `UnknownExternalSignal` or `UnknownExternalCancel` because the run is closed (`RunClosed` rejection fires before the pending map lookup).
- **No state update on resolution.** Resolution simply removes the entry from the pending set.

The feature introduces two workflow commands (within `WorkflowTaskCompleted`) and two top-level commands (from the runtime):

1. `SignalExternalWorkflowExecution` — workflow command that initiates a signal to another workflow.
2. `RequestCancelExternalWorkflowExecution` — workflow command that initiates a cancel request to another workflow.
3. `ExternalSignalResolved` — top-level command issued by the runtime when signal delivery succeeds or fails.
4. `ExternalCancelResolved` — top-level command issued by the runtime when cancel request delivery succeeds or fails.

Both resolution commands are internal runtime machinery — no `RequestDedupeOp`. Both follow the same WFT coalescing pattern as `Signal`, `ActivityResolved`, `ChildStartConfirmed`, and `ChildResolved`.

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
- **PendingExternalSignal**: Tracking record for an initiated but not yet resolved external signal in WorkflowState. Keyed by initiated_event_id. Tracks target_workflow_id, target_run_id (Option), and signal_name.
- **PendingExternalCancel**: Tracking record for an initiated but not yet resolved external cancel request in WorkflowState. Keyed by initiated_event_id. Tracks target_workflow_id and target_run_id (Option).
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **ExternalWorkflowExecution**: Existing type (from Feature 3) identifying a workflow execution by namespace_id, workflow_id, and run_id.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.

## Requirements

---

## New Types and State Fields

### Requirement 1.1: PendingExternalSignal Type

**User Story:** As a Tokeira developer, I want a PendingExternalSignal type to track initiated external signals in WorkflowState, so that the kernel can manage external signal lifecycle.

#### Acceptance Criteria

1. THE `PendingExternalSignal` struct SHALL include an `initiated_event_id` field of type `i64` recording the event ID of the SignalExternalWorkflowExecutionInitiated event.
2. THE `PendingExternalSignal` struct SHALL include a `target_workflow_id` field of type `WorkflowId`.
3. THE `PendingExternalSignal` struct SHALL include a `target_run_id` field of type `Option<RunId>`.
4. THE `PendingExternalSignal` struct SHALL include a `signal_name` field of type `String`.
5. THE `PendingExternalSignal` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.2: PendingExternalCancel Type

**User Story:** As a Tokeira developer, I want a PendingExternalCancel type to track initiated external cancel requests in WorkflowState, so that the kernel can manage external cancel lifecycle.

#### Acceptance Criteria

1. THE `PendingExternalCancel` struct SHALL include an `initiated_event_id` field of type `i64` recording the event ID of the RequestCancelExternalWorkflowExecutionInitiated event.
2. THE `PendingExternalCancel` struct SHALL include a `target_workflow_id` field of type `WorkflowId`.
3. THE `PendingExternalCancel` struct SHALL include a `target_run_id` field of type `Option<RunId>`.
4. THE `PendingExternalCancel` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.3: WorkflowState Pending External Maps

**User Story:** As a Tokeira developer, I want WorkflowState to include pending external signal and cancel maps, so that initiated external operations are tracked as part of the run's durable state.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL include a `pending_external_signals` field of type `BTreeMap<i64, PendingExternalSignal>` keyed by initiated_event_id.
2. THE `WorkflowState` struct SHALL include a `pending_external_cancels` field of type `BTreeMap<i64, PendingExternalCancel>` keyed by initiated_event_id.
3. WHEN a new WorkflowState is initialized (via Start command), THE `pending_external_signals` map SHALL be empty.
4. WHEN a new WorkflowState is initialized (via Start command), THE `pending_external_cancels` map SHALL be empty.

### Requirement 1.4: New WorkflowCommand Variants

**User Story:** As a Tokeira developer, I want new WorkflowCommand variants for external signal and cancel initiation, so that workflow code can express these operations.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `SignalExternalWorkflowExecution` variant with fields: `target_workflow_id` (WorkflowId), `target_run_id` (Option\<RunId\>), `signal_name` (String), and `input` (Payloads).
2. THE WorkflowCommand enum SHALL include a `RequestCancelExternalWorkflowExecution` variant with fields: `target_workflow_id` (WorkflowId) and `target_run_id` (Option\<RunId\>).

### Requirement 1.5: New Command Variants

**User Story:** As a Tokeira developer, I want new Command variants for external signal and cancel resolution, so that the runtime can report delivery outcomes.

#### Acceptance Criteria

1. THE Command enum SHALL include an `ExternalSignalResolved(ExternalSignalResolvedRequest)` variant.
2. THE `ExternalSignalResolvedRequest` struct SHALL include an `initiated_event_id` field of type `i64` for fencing against stale resolutions.
3. THE `ExternalSignalResolvedRequest` struct SHALL include a `result` field of type `ExternalSignalResult` distinguishing success from failure.
4. THE `ExternalSignalResolvedRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
5. THE `ExternalSignalResolvedRequest` struct SHALL derive `Clone, Debug, PartialEq`.
6. THE Command enum SHALL include an `ExternalCancelResolved(ExternalCancelResolvedRequest)` variant.
7. THE `ExternalCancelResolvedRequest` struct SHALL include an `initiated_event_id` field of type `i64` for fencing against stale resolutions.
8. THE `ExternalCancelResolvedRequest` struct SHALL include a `result` field of type `ExternalCancelResult` distinguishing success from failure.
9. THE `ExternalCancelResolvedRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
10. THE `ExternalCancelResolvedRequest` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.6: ExternalSignalResult and ExternalCancelResult Enums

**User Story:** As a Tokeira developer, I want result enums for external signal and cancel resolution, so that the kernel can distinguish success from failure.

#### Acceptance Criteria

1. THE `ExternalSignalResult` enum SHALL include a `Signaled` variant (success).
2. THE `ExternalSignalResult` enum SHALL include a `Failed` variant with field: `cause` (String).
3. THE `ExternalSignalResult` enum SHALL derive `Clone, Debug, PartialEq`.
4. THE `ExternalCancelResult` enum SHALL include a `CancelRequested` variant (success).
5. THE `ExternalCancelResult` enum SHALL include a `Failed` variant with field: `cause` (String).
6. THE `ExternalCancelResult` enum SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.7: New HistoryEventKind Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for external signal and cancel lifecycle events, so that these events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `SignalExternalWorkflowExecutionInitiated` variant with fields: `target_workflow_id` (WorkflowId), `target_run_id` (Option\<RunId\>), `signal_name` (String), and `input` (Payloads).
2. THE HistoryEventKind enum SHALL include an `ExternalWorkflowExecutionSignaled` variant with fields: `initiated_event_id` (i64) and `target_workflow_id` (WorkflowId).
3. THE HistoryEventKind enum SHALL include a `SignalExternalWorkflowExecutionFailed` variant with fields: `initiated_event_id` (i64), `target_workflow_id` (WorkflowId), and `cause` (String).
4. THE HistoryEventKind enum SHALL include a `RequestCancelExternalWorkflowExecutionInitiated` variant with fields: `target_workflow_id` (WorkflowId) and `target_run_id` (Option\<RunId\>).
5. THE HistoryEventKind enum SHALL include an `ExternalWorkflowExecutionCancelRequested` variant with fields: `initiated_event_id` (i64) and `target_workflow_id` (WorkflowId).
6. THE HistoryEventKind enum SHALL include a `RequestCancelExternalWorkflowExecutionFailed` variant with fields: `initiated_event_id` (i64), `target_workflow_id` (WorkflowId), and `cause` (String).

### Requirement 1.8: New DispatchOp Variants

**User Story:** As a Tokeira developer, I want new DispatchOp variants for external signal and cancel delivery, so that the runtime knows what delivery actions to take.

#### Acceptance Criteria

1. THE DispatchOp enum SHALL include a `SignalExternalWorkflow` variant with fields: `target_workflow_id` (WorkflowId), `target_run_id` (Option\<RunId\>), `signal_name` (String), and `input` (Payloads).
2. THE DispatchOp enum SHALL include a `RequestCancelExternalWorkflow` variant with fields: `target_workflow_id` (WorkflowId) and `target_run_id` (Option\<RunId\>).

### Requirement 1.9: New Reject Variants

**User Story:** As a Tokeira developer, I want new Reject variants for external signal and cancel resolution errors, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Reject enum SHALL include an `UnknownExternalSignal(i64)` variant for when ExternalSignalResolved references an initiated_event_id not in the pending external signals map.
2. THE Reject enum SHALL include an `UnknownExternalCancel(i64)` variant for when ExternalCancelResolved references an initiated_event_id not in the pending external cancels map.

---

## SignalExternalWorkflowExecution Workflow Command Behavior

### Requirement 2.1: SignalExternalWorkflowExecution Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to initiate external workflow signals from workflow commands, so that workflows can signal other workflow executions.

#### Acceptance Criteria

1. WHEN a SignalExternalWorkflowExecution workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a SignalExternalWorkflowExecutionInitiated event carrying the target_workflow_id, target_run_id, signal_name, and input.
2. WHEN a SignalExternalWorkflowExecution workflow command is received, THE Kernel SHALL add a PendingExternalSignal entry to the pending_external_signals map keyed by the initiated_event_id, recording the initiated_event_id, target_workflow_id, target_run_id, and signal_name.
3. WHEN a SignalExternalWorkflowExecution workflow command is received, THE Kernel SHALL push a DispatchOp::SignalExternalWorkflow with the target_workflow_id, target_run_id, signal_name, and input.
4. WHEN a SignalExternalWorkflowExecution workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

---

## ExternalSignalResolved Command Behavior

### Requirement 3.1: ExternalSignalResolved Success Path

**User Story:** As a Tokeira developer, I want the Kernel to record successful external signal delivery, so that workflow code can observe that the signal was delivered.

#### Acceptance Criteria

1. WHEN an ExternalSignalResolved command is received with a Signaled result for a known pending external signal whose initiated_event_id matches, THE Kernel SHALL emit an ExternalWorkflowExecutionSignaled event carrying the initiated_event_id and target_workflow_id.
2. WHEN an ExternalSignalResolved command is received with a Signaled result, THE Kernel SHALL remove the entry from the pending_external_signals map.
3. WHEN an ExternalSignalResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN an ExternalSignalResolved command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. WHEN an ExternalSignalResolved command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is internal runtime machinery).

### Requirement 3.2: ExternalSignalResolved Failure Path

**User Story:** As a Tokeira developer, I want the Kernel to record failed external signal delivery, so that workflow code can observe that the signal could not be delivered.

#### Acceptance Criteria

1. WHEN an ExternalSignalResolved command is received with a Failed result for a known pending external signal whose initiated_event_id matches, THE Kernel SHALL emit a SignalExternalWorkflowExecutionFailed event carrying the initiated_event_id, target_workflow_id, and cause.
2. WHEN an ExternalSignalResolved command is received with a Failed result, THE Kernel SHALL remove the entry from the pending_external_signals map.
3. WHEN an ExternalSignalResolved command is received with a Failed result and no WFT is pending, THE Kernel SHALL schedule a workflow task.

### Requirement 3.3: ExternalSignalResolved Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject ExternalSignalResolved for unknown pending signals, so that stale or invalid resolutions are caught.

#### Acceptance Criteria

1. WHEN an ExternalSignalResolved command is received with an initiated_event_id not in the pending_external_signals map, THE Kernel SHALL reject with UnknownExternalSignal carrying the initiated_event_id.

---

## RequestCancelExternalWorkflowExecution Workflow Command Behavior

### Requirement 4.1: RequestCancelExternalWorkflowExecution Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to initiate external workflow cancel requests from workflow commands, so that workflows can request cancellation of other workflow executions.

#### Acceptance Criteria

1. WHEN a RequestCancelExternalWorkflowExecution workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a RequestCancelExternalWorkflowExecutionInitiated event carrying the target_workflow_id and target_run_id.
2. WHEN a RequestCancelExternalWorkflowExecution workflow command is received, THE Kernel SHALL add a PendingExternalCancel entry to the pending_external_cancels map keyed by the initiated_event_id, recording the initiated_event_id, target_workflow_id, and target_run_id.
3. WHEN a RequestCancelExternalWorkflowExecution workflow command is received, THE Kernel SHALL push a DispatchOp::RequestCancelExternalWorkflow with the target_workflow_id and target_run_id.
4. WHEN a RequestCancelExternalWorkflowExecution workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

---

## ExternalCancelResolved Command Behavior

### Requirement 5.1: ExternalCancelResolved Success Path

**User Story:** As a Tokeira developer, I want the Kernel to record successful external cancel request delivery, so that workflow code can observe that the cancel request was delivered.

#### Acceptance Criteria

1. WHEN an ExternalCancelResolved command is received with a CancelRequested result for a known pending external cancel whose initiated_event_id matches, THE Kernel SHALL emit an ExternalWorkflowExecutionCancelRequested event carrying the initiated_event_id and target_workflow_id.
2. WHEN an ExternalCancelResolved command is received with a CancelRequested result, THE Kernel SHALL remove the entry from the pending_external_cancels map.
3. WHEN an ExternalCancelResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN an ExternalCancelResolved command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. WHEN an ExternalCancelResolved command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is internal runtime machinery).

### Requirement 5.2: ExternalCancelResolved Failure Path

**User Story:** As a Tokeira developer, I want the Kernel to record failed external cancel request delivery, so that workflow code can observe that the cancel request could not be delivered.

#### Acceptance Criteria

1. WHEN an ExternalCancelResolved command is received with a Failed result for a known pending external cancel whose initiated_event_id matches, THE Kernel SHALL emit a RequestCancelExternalWorkflowExecutionFailed event carrying the initiated_event_id, target_workflow_id, and cause.
2. WHEN an ExternalCancelResolved command is received with a Failed result, THE Kernel SHALL remove the entry from the pending_external_cancels map.
3. WHEN an ExternalCancelResolved command is received with a Failed result and no WFT is pending, THE Kernel SHALL schedule a workflow task.

### Requirement 5.3: ExternalCancelResolved Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject ExternalCancelResolved for unknown pending cancels, so that stale or invalid resolutions are caught.

#### Acceptance Criteria

1. WHEN an ExternalCancelResolved command is received with an initiated_event_id not in the pending_external_cancels map, THE Kernel SHALL reject with UnknownExternalCancel carrying the initiated_event_id.

---

## BasicKernel Integration

### Requirement 6.1: BasicKernel Apply Routing for External Commands

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route ExternalSignalResolved and ExternalCancelResolved commands to dedicated handler methods, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN an ExternalSignalResolved command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_external_signal_resolved` method.
2. WHEN an ExternalCancelResolved command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_external_cancel_resolved` method.
3. THE `apply_external_signal_resolved` method SHALL follow the same pattern as existing apply methods: call `expect_open`, construct a TransitionBuilder, look up the pending entry by initiated_event_id, emit the appropriate event, remove from pending set, conditionally schedule WFT, and call `finish`.
4. THE `apply_external_cancel_resolved` method SHALL follow the same pattern: call `expect_open`, construct a TransitionBuilder, look up the pending entry by initiated_event_id, emit the appropriate event, remove from pending set, conditionally schedule WFT, and call `finish`.

### Requirement 6.2: Workflow Command Dispatch for External Operations

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle SignalExternalWorkflowExecution and RequestCancelExternalWorkflowExecution, so that external operations are processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::SignalExternalWorkflowExecution` that emits SignalExternalWorkflowExecutionInitiated, creates PendingExternalSignal, and pushes DispatchOp::SignalExternalWorkflow.
2. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::RequestCancelExternalWorkflowExecution` that emits RequestCancelExternalWorkflowExecutionInitiated, creates PendingExternalCancel, and pushes DispatchOp::RequestCancelExternalWorkflow.

---

## Close Path Cleanup

### Requirement 7.1: Pending External Maps Cleared on Close

**User Story:** As a Tokeira developer, I want all close paths to clear the pending external signal and cancel maps, so that no orphaned pending entries remain in terminal state.

#### Acceptance Criteria

1. WHEN the Kernel closes a run via Terminate, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
2. WHEN the Kernel closes a run via WorkflowExecutionTimedOut, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
3. WHEN the Kernel closes a run via CompleteWorkflow, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
4. WHEN the Kernel closes a run via FailWorkflow, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
5. WHEN the Kernel closes a run via CancelWorkflow, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
6. WHEN the Kernel closes a run via ContinueAsNew, THE Kernel SHALL clear the pending_external_signals and pending_external_cancels maps in next_state.
7. WHEN the Kernel clears pending external maps on close, THE Kernel SHALL NOT emit any DispatchOps for the cleared entries (unlike children, there is no parent close policy for external signals and cancels).

---

## Structural Invariants

### Requirement 8.1: Event ID Contiguity for External Signal and Cancel Transitions

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for all external signal and cancel transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL ExternalSignalResolved transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL ExternalCancelResolved transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL transitions containing SignalExternalWorkflowExecution workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.
4. FOR ALL transitions containing RequestCancelExternalWorkflowExecution workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.

### Requirement 8.2: Transition Sequence Increment for External Signal and Cancel Transitions

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for external signal and cancel transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL ExternalSignalResolved transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.
2. FOR ALL ExternalCancelResolved transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 8.3: At-Most-One-WFT Invariant for External Commands

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after ExternalSignalResolved and ExternalCancelResolved, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL ExternalSignalResolved transitions, next_state SHALL contain at most one PendingWorkflowTask.
2. FOR ALL ExternalCancelResolved transitions, next_state SHALL contain at most one PendingWorkflowTask.

### Requirement 8.4: Pending External Maps Consistency

**User Story:** As a Tokeira developer, I want the pending external maps to be consistent after every transition, so that external operation lifecycle tracking is accurate.

#### Acceptance Criteria

1. FOR ALL SignalExternalWorkflowExecution workflow commands that succeed, THE next_state.pending_external_signals map SHALL contain an entry keyed by the initiated_event_id with the correct target_workflow_id, target_run_id, and signal_name.
2. FOR ALL ExternalSignalResolved transitions (success or failure), THE next_state.pending_external_signals map SHALL NOT contain the resolved entry.
3. FOR ALL RequestCancelExternalWorkflowExecution workflow commands that succeed, THE next_state.pending_external_cancels map SHALL contain an entry keyed by the initiated_event_id with the correct target_workflow_id and target_run_id.
4. FOR ALL ExternalCancelResolved transitions (success or failure), THE next_state.pending_external_cancels map SHALL NOT contain the resolved entry.

### Requirement 8.5: Terminal State Invariants for Close with Pending External Operations

**User Story:** As a Tokeira developer, I want all close paths to leave empty pending external maps, so that terminal state is clean.

#### Acceptance Criteria

1. FOR ALL transitions where the run is closed, next_state.pending_external_signals SHALL be empty.
2. FOR ALL transitions where the run is closed, next_state.pending_external_cancels SHALL be empty.

### Requirement 8.6: No Request Deduplication for Resolution Commands

**User Story:** As a Tokeira developer, I want resolution commands to carry no request dedup, so that the internal runtime machinery boundary is respected.

#### Acceptance Criteria

1. FOR ALL ExternalSignalResolved transitions, THE Transition SHALL contain zero RequestDedupeOps.
2. FOR ALL ExternalCancelResolved transitions, THE Transition SHALL contain zero RequestDedupeOps.

---

## Downstream Breakage and Compilation

### Requirement 9.1: Workspace Compilation After Type Changes

**User Story:** As a Tokeira developer, I want the workspace to compile after all type changes are made, so that downstream breakage from new enum variants and struct fields is resolved before behavioral implementation begins.

#### Acceptance Criteria

1. WHEN the new WorkflowCommand variants, Command variants, HistoryEventKind variants, DispatchOp variants, Reject variants, and WorkflowState fields are added, THE workspace SHALL compile without errors.
2. THE Start command handler SHALL initialize pending_external_signals and pending_external_cancels to empty BTreeMaps.
3. THE close helper on TransitionBuilder SHALL clear pending_external_signals and pending_external_cancels maps (no dispatch ops emitted for these entries).
