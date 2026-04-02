# Requirements Document: Nexus Operations (Feature 9)

## Introduction

This document captures the requirements for Feature 9 of the Tokeira kernel implementation: Nexus Operations. Feature 9 depends on Feature 1 (Foundation + WFT lifecycle) and Feature 3 (Cancel and Terminate), both of which are complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 9.1–9.3).

Feature 9 adds Nexus operation support to the kernel. Nexus is Temporal's mechanism for cross-namespace workflow invocation through typed service contracts. This follows the same initiation/resolution pattern as external signals (Feature 6) and child workflows (Feature 5), but with a key structural difference:

- **The Started resolution is non-terminal.** Unlike entity resolution patterns (activities, children, external signals/cancels), the `Started` variant does NOT remove the operation from the pending set and does NOT schedule a WFT. The operation continues running asynchronously. Only terminal resolutions (`Completed`, `Failed`, `Canceled`, `TimedOut`) remove the operation from pending and schedule a WFT. (Note: non-terminal runtime callbacks exist elsewhere in the kernel — WFT failure/timeout also leave the entity live — but Started is the first non-terminal *entity resolution* pattern where the pending entry is retained.)
- **No parent close policy.** When the parent closes, the kernel discards all pending Nexus operation entries by clearing the map. No dispatch ops are emitted for discarded entries. Any late runtime resolution arriving after the close will be rejected with `RunClosed` before the pending map lookup.
- **Fencing via scheduled_event_id.** The `NexusOperationResolved` command carries a `scheduled_event_id` for fencing against stale resolutions, following the same pattern as `ChildStartConfirmed` and `ExternalSignalResolved`.
- **No RequestDedupeOp.** `NexusOperationResolved` is internal runtime machinery.

The feature introduces two workflow commands (within `WorkflowTaskCompleted`) and one top-level command (from the runtime):

1. `ScheduleNexusOperation` — workflow command that initiates a Nexus operation. Emits NexusOperationScheduled event. Adds PendingNexusOperation to the pending set. Pushes DispatchOp::ScheduleNexusOperation. Rejects with DuplicateNexusOperationId if the operation_id is already pending.
2. `CancelNexusOperation` — workflow command that requests cancellation of a pending Nexus operation. Emits NexusOperationCancelRequested event. Pushes DispatchOp::CancelNexusOperation. The operation remains pending until resolved.
3. `NexusOperationResolved` — top-level command issued by the runtime when the Nexus operation reaches a result. Has five resolution variants: Started (non-terminal), Completed, Failed, Canceled, TimedOut.

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
- **PendingNexusOperation**: Tracking record for a scheduled but not yet terminally resolved Nexus operation in WorkflowState. Keyed by operation_id (String). Tracks operation_id, scheduled_event_id, endpoint, service, and operation.
- **NexusResolution**: Enum distinguishing the five resolution outcomes: Started (non-terminal), Completed, Failed, Canceled, TimedOut.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.

## Requirements

---

## New Types and State Fields

### Requirement 1.1: PendingNexusOperation Type

**User Story:** As a Tokeira developer, I want a PendingNexusOperation type to track scheduled Nexus operations in WorkflowState, so that the kernel can manage Nexus operation lifecycle.

#### Acceptance Criteria

1. THE `PendingNexusOperation` struct SHALL include an `operation_id` field of type `String` identifying the Nexus operation.
2. THE `PendingNexusOperation` struct SHALL include a `scheduled_event_id` field of type `i64` recording the event ID of the NexusOperationScheduled event.
3. THE `PendingNexusOperation` struct SHALL include an `endpoint` field of type `String` identifying the Nexus endpoint.
4. THE `PendingNexusOperation` struct SHALL include a `service` field of type `String` identifying the Nexus service.
5. THE `PendingNexusOperation` struct SHALL include an `operation` field of type `String` identifying the Nexus operation name.
6. THE `PendingNexusOperation` struct SHALL include a `started` field of type `bool`, initialized to `false`, set to `true` when a Started resolution is processed.
7. THE `PendingNexusOperation` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.2: NexusResolution Enum

**User Story:** As a Tokeira developer, I want a NexusResolution enum to distinguish the five resolution outcomes, so that the kernel can handle each variant correctly.

#### Acceptance Criteria

1. THE `NexusResolution` enum SHALL include a `Started` variant representing an async operation accepted by the handler (non-terminal — the operation remains pending).
2. THE `NexusResolution` enum SHALL include a `Completed` variant with field: `result` (Payloads).
3. THE `NexusResolution` enum SHALL include a `Failed` variant with field: `failure` (String).
4. THE `NexusResolution` enum SHALL include a `Canceled` variant.
5. THE `NexusResolution` enum SHALL include a `TimedOut` variant.
6. THE `NexusResolution` enum SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.3: WorkflowState Pending Nexus Operations Map

**User Story:** As a Tokeira developer, I want WorkflowState to include a pending Nexus operations map, so that scheduled Nexus operations are tracked as part of the run's durable state.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL include a `pending_nexus_operations` field of type `BTreeMap<String, PendingNexusOperation>` keyed by operation_id.
2. WHEN a new WorkflowState is initialized (via Start command), THE `pending_nexus_operations` map SHALL be empty.

### Requirement 1.4: New WorkflowCommand Variants

**User Story:** As a Tokeira developer, I want new WorkflowCommand variants for Nexus operation scheduling and cancellation, so that workflow code can express these operations.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `ScheduleNexusOperation` variant with fields: `operation_id` (String), `endpoint` (String), `service` (String), `operation` (String), `input` (Payloads), and `schedule_to_close_timeout` (Option\<Duration\>).
2. THE WorkflowCommand enum SHALL include a `CancelNexusOperation` variant with field: `scheduled_event_id` (i64) referencing the operation to cancel.

### Requirement 1.5: New Command Variant

**User Story:** As a Tokeira developer, I want a new Command variant for Nexus operation resolution, so that the runtime can report Nexus operation outcomes.

#### Acceptance Criteria

1. THE Command enum SHALL include a `NexusOperationResolved(NexusOperationResolvedRequest)` variant.
2. THE `NexusOperationResolvedRequest` struct SHALL include an `operation_id` field of type `String` identifying the Nexus operation.
3. THE `NexusOperationResolvedRequest` struct SHALL include a `scheduled_event_id` field of type `i64` for fencing against stale resolutions.
4. THE `NexusOperationResolvedRequest` struct SHALL include a `resolution` field of type `NexusResolution`.
5. THE `NexusOperationResolvedRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
6. THE `NexusOperationResolvedRequest` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.6: New HistoryEventKind Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for Nexus operation lifecycle events, so that these events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `NexusOperationScheduled` variant with fields: `operation_id` (String), `endpoint` (String), `service` (String), `operation` (String), `input` (Payloads), and `schedule_to_close_timeout` (Option\<Duration\>).
2. THE HistoryEventKind enum SHALL include a `NexusOperationStarted` variant with fields: `operation_id` (String) and `scheduled_event_id` (i64).
3. THE HistoryEventKind enum SHALL include a `NexusOperationCompleted` variant with fields: `operation_id` (String), `scheduled_event_id` (i64), and `result` (Payloads).
4. THE HistoryEventKind enum SHALL include a `NexusOperationFailed` variant with fields: `operation_id` (String), `scheduled_event_id` (i64), and `failure` (String).
5. THE HistoryEventKind enum SHALL include a `NexusOperationCanceled` variant with fields: `operation_id` (String) and `scheduled_event_id` (i64).
6. THE HistoryEventKind enum SHALL include a `NexusOperationTimedOut` variant with fields: `operation_id` (String) and `scheduled_event_id` (i64).
7. THE HistoryEventKind enum SHALL include a `NexusOperationCancelRequested` variant with field: `scheduled_event_id` (i64).

### Requirement 1.7: New DispatchOp Variants

**User Story:** As a Tokeira developer, I want new DispatchOp variants for Nexus operation scheduling and cancellation, so that the runtime knows what delivery actions to take.

#### Acceptance Criteria

1. THE DispatchOp enum SHALL include a `ScheduleNexusOperation` variant with fields: `operation_id` (String), `endpoint` (String), `service` (String), `operation` (String), `input` (Payloads), and `schedule_to_close_timeout` (Option\<Duration\>).
2. THE DispatchOp enum SHALL include a `CancelNexusOperation` variant with field: `scheduled_event_id` (i64).

### Requirement 1.8: New Reject Variants

**User Story:** As a Tokeira developer, I want new Reject variants for Nexus operation errors, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Reject enum SHALL include a `DuplicateNexusOperationId(String)` variant for when ScheduleNexusOperation references an operation_id that is already in the pending Nexus operations map.
2. THE Reject enum SHALL include an `UnknownNexusOperation(String)` variant for when NexusOperationResolved references an operation_id not in the pending Nexus operations map.
3. THE Reject enum SHALL include a `StaleNexusResolution` variant with fields `operation_id` (String) and `expected_scheduled_event_id` (i64) for when NexusOperationResolved carries a scheduled_event_id that does not match the pending operation's scheduled_event_id.
4. THE Reject enum SHALL include a `NexusOperationAlreadyStarted(String)` variant for when a Started resolution is received for an operation whose `started` flag is already `true`.

---

## ScheduleNexusOperation Workflow Command Behavior

### Requirement 2.1: ScheduleNexusOperation Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to schedule Nexus operations from workflow commands, so that workflows can invoke cross-namespace services through typed contracts.

#### Acceptance Criteria

1. WHEN a ScheduleNexusOperation workflow command is received within WorkflowTaskCompleted with a unique operation_id, THE Kernel SHALL emit a NexusOperationScheduled event carrying the operation_id, endpoint, service, operation, input, and schedule_to_close_timeout.
2. WHEN a ScheduleNexusOperation workflow command is received, THE Kernel SHALL add a PendingNexusOperation entry to the pending_nexus_operations map keyed by operation_id, recording the operation_id, scheduled_event_id, endpoint, service, and operation.
3. WHEN a ScheduleNexusOperation workflow command is received, THE Kernel SHALL push a DispatchOp::ScheduleNexusOperation with the operation_id, endpoint, service, operation, input, and schedule_to_close_timeout.
4. WHEN a ScheduleNexusOperation workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

### Requirement 2.2: ScheduleNexusOperation Duplicate Rejection

**User Story:** As a Tokeira developer, I want the Kernel to reject duplicate Nexus operation IDs, so that each operation_id is unique within a run's pending set.

#### Acceptance Criteria

1. WHEN a ScheduleNexusOperation workflow command is received with an operation_id that is already in the pending_nexus_operations map, THE Kernel SHALL reject with DuplicateNexusOperationId carrying the operation_id.

---

## CancelNexusOperation Workflow Command Behavior

### Requirement 3.1: CancelNexusOperation Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle Nexus operation cancellation requests from workflow commands, so that workflows can cancel pending Nexus operations.

#### Acceptance Criteria

1. WHEN a CancelNexusOperation workflow command is received within WorkflowTaskCompleted for a `scheduled_event_id` that matches a pending Nexus operation, THE Kernel SHALL emit a NexusOperationCancelRequested event carrying the scheduled_event_id.
2. WHEN a CancelNexusOperation workflow command is received for a valid pending operation, THE Kernel SHALL push a DispatchOp::CancelNexusOperation with the scheduled_event_id.
3. WHEN a CancelNexusOperation workflow command is received, THE Kernel SHALL keep the Nexus operation in the pending_nexus_operations map until it is resolved.
4. WHEN a CancelNexusOperation workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).

### Requirement 3.2: CancelNexusOperation Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject CancelNexusOperation for unknown operations, so that cancel requests for non-existent or already-resolved operations are caught.

#### Acceptance Criteria

1. WHEN a CancelNexusOperation workflow command is received with a `scheduled_event_id` that does not match any pending Nexus operation's `scheduled_event_id`, THE Kernel SHALL reject with UnknownNexusOperation.

---

## NexusOperationResolved Command Behavior

### Requirement 4.1: NexusOperationResolved — Started (Non-Terminal)

**User Story:** As a Tokeira developer, I want the Kernel to record when a Nexus operation is accepted by the handler as an async operation, so that the operation's started status is tracked without removing it from pending.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a Started variant for a known pending Nexus operation whose scheduled_event_id matches and whose `started` flag is `false`, THE Kernel SHALL emit a NexusOperationStarted event carrying the operation_id and scheduled_event_id.
2. WHEN a NexusOperationResolved command is received with a Started variant, THE Kernel SHALL set the `started` flag on the pending operation to `true`.
3. WHEN a NexusOperationResolved command is received with a Started variant, THE Kernel SHALL keep the operation in the pending_nexus_operations map (the operation is still running asynchronously).
4. WHEN a NexusOperationResolved command is received with a Started variant, THE Kernel SHALL NOT schedule a workflow task (the operation has not reached a terminal state).
5. WHEN a NexusOperationResolved command is received with a Started variant, THE Kernel SHALL NOT emit a RequestDedupeOp (this is internal runtime machinery).
6. WHEN a NexusOperationResolved command is received with a Started variant for a pending operation whose `started` flag is already `true`, THE Kernel SHALL reject with NexusOperationAlreadyStarted carrying the operation_id.

### Requirement 4.2: NexusOperationResolved — Completed (Terminal)

**User Story:** As a Tokeira developer, I want the Kernel to record successful Nexus operation completion, so that workflow code can observe the result.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a Completed variant for a known pending Nexus operation whose scheduled_event_id matches, THE Kernel SHALL emit a NexusOperationCompleted event carrying the operation_id, scheduled_event_id, and result.
2. WHEN a NexusOperationResolved command is received with a Completed variant, THE Kernel SHALL remove the operation from the pending_nexus_operations map.
3. WHEN a NexusOperationResolved command is received with a Completed variant and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a NexusOperationResolved command is received with a Completed variant and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.

### Requirement 4.3: NexusOperationResolved — Failed (Terminal)

**User Story:** As a Tokeira developer, I want the Kernel to record Nexus operation failure, so that workflow code can observe the failure.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a Failed variant for a known pending Nexus operation whose scheduled_event_id matches, THE Kernel SHALL emit a NexusOperationFailed event carrying the operation_id, scheduled_event_id, and failure.
2. WHEN a NexusOperationResolved command is received with a Failed variant, THE Kernel SHALL remove the operation from the pending_nexus_operations map.
3. WHEN a NexusOperationResolved command is received with a Failed variant and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a NexusOperationResolved command is received with a Failed variant and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.

### Requirement 4.4: NexusOperationResolved — Canceled (Terminal)

**User Story:** As a Tokeira developer, I want the Kernel to record Nexus operation cancellation, so that workflow code can observe the cancellation.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a Canceled variant for a known pending Nexus operation whose scheduled_event_id matches, THE Kernel SHALL emit a NexusOperationCanceled event carrying the operation_id and scheduled_event_id.
2. WHEN a NexusOperationResolved command is received with a Canceled variant, THE Kernel SHALL remove the operation from the pending_nexus_operations map.
3. WHEN a NexusOperationResolved command is received with a Canceled variant and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a NexusOperationResolved command is received with a Canceled variant and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.

### Requirement 4.5: NexusOperationResolved — TimedOut (Terminal)

**User Story:** As a Tokeira developer, I want the Kernel to record Nexus operation timeout, so that workflow code can observe the timeout.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a TimedOut variant for a known pending Nexus operation whose scheduled_event_id matches, THE Kernel SHALL emit a NexusOperationTimedOut event carrying the operation_id and scheduled_event_id.
2. WHEN a NexusOperationResolved command is received with a TimedOut variant, THE Kernel SHALL remove the operation from the pending_nexus_operations map.
3. WHEN a NexusOperationResolved command is received with a TimedOut variant and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a NexusOperationResolved command is received with a TimedOut variant and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.

### Requirement 4.6: NexusOperationResolved — Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject NexusOperationResolved for unknown or stale operations, so that invalid resolutions are caught.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with an operation_id not in the pending_nexus_operations map, THE Kernel SHALL reject with UnknownNexusOperation carrying the operation_id.
2. WHEN a NexusOperationResolved command is received with a scheduled_event_id that does not match the pending operation's scheduled_event_id, THE Kernel SHALL reject with StaleNexusResolution carrying the operation_id and the expected scheduled_event_id.

---

## BasicKernel Integration

### Requirement 5.1: BasicKernel Apply Routing for NexusOperationResolved

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route NexusOperationResolved commands to a dedicated handler method, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_nexus_operation_resolved` method.
2. THE `apply_nexus_operation_resolved` method SHALL follow the same pattern as existing apply methods: call `expect_open`, construct a TransitionBuilder, look up the pending entry by operation_id, validate scheduled_event_id fencing, emit the appropriate event, conditionally remove from pending set, conditionally schedule WFT, and call `finish`.

### Requirement 5.2: Workflow Command Dispatch for Nexus Operations

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle ScheduleNexusOperation and CancelNexusOperation, so that Nexus operations are processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::ScheduleNexusOperation` that emits NexusOperationScheduled, creates PendingNexusOperation, and pushes DispatchOp::ScheduleNexusOperation.
2. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::CancelNexusOperation` that emits NexusOperationCancelRequested and pushes DispatchOp::CancelNexusOperation.

---

## Close Path Cleanup

### Requirement 6.1: Pending Nexus Operations Map Cleared on Close

**User Story:** As a Tokeira developer, I want all close paths to clear the pending Nexus operations map, so that no orphaned pending entries remain in terminal state.

#### Acceptance Criteria

1. WHEN the Kernel closes a run via Terminate, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
2. WHEN the Kernel closes a run via WorkflowExecutionTimedOut, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
3. WHEN the Kernel closes a run via CompleteWorkflow, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
4. WHEN the Kernel closes a run via FailWorkflow, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
5. WHEN the Kernel closes a run via CancelWorkflow, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
6. WHEN the Kernel closes a run via ContinueAsNew, THE Kernel SHALL clear the pending_nexus_operations map in next_state.
7. WHEN the Kernel clears the pending Nexus operations map on close, THE Kernel SHALL NOT emit any DispatchOps for the cleared entries (unlike children, there is no parent close policy for Nexus operations).

---

## Structural Invariants

### Requirement 7.1: Event ID Contiguity for Nexus Transitions

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for all Nexus operation transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL NexusOperationResolved transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL transitions containing ScheduleNexusOperation workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL transitions containing CancelNexusOperation workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.

### Requirement 7.2: Transition Sequence Increment for Nexus Transitions

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for Nexus operation transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL NexusOperationResolved transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 7.3: At-Most-One-WFT Invariant for Nexus Commands

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after NexusOperationResolved, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL NexusOperationResolved transitions, next_state SHALL contain at most one PendingWorkflowTask.

### Requirement 7.4: Pending Nexus Operations Map Consistency

**User Story:** As a Tokeira developer, I want the pending Nexus operations map to be consistent after every transition, so that Nexus operation lifecycle tracking is accurate.

#### Acceptance Criteria

1. FOR ALL ScheduleNexusOperation workflow commands that succeed, THE next_state.pending_nexus_operations map SHALL contain an entry keyed by operation_id with the correct scheduled_event_id, endpoint, service, and operation.
2. FOR ALL NexusOperationResolved transitions with a Started variant, THE next_state.pending_nexus_operations map SHALL still contain the operation entry (Started is non-terminal).
3. FOR ALL NexusOperationResolved transitions with a terminal variant (Completed, Failed, Canceled, TimedOut), THE next_state.pending_nexus_operations map SHALL NOT contain the resolved operation entry.

### Requirement 7.5: Terminal State Invariants for Close with Pending Nexus Operations

**User Story:** As a Tokeira developer, I want all close paths to leave an empty pending Nexus operations map, so that terminal state is clean.

#### Acceptance Criteria

1. FOR ALL transitions where the run is closed, next_state.pending_nexus_operations SHALL be empty.

### Requirement 7.6: No Request Deduplication for NexusOperationResolved

**User Story:** As a Tokeira developer, I want NexusOperationResolved to carry no request dedup, so that the internal runtime machinery boundary is respected.

#### Acceptance Criteria

1. FOR ALL NexusOperationResolved transitions, THE Transition SHALL contain zero RequestDedupeOps.

### Requirement 7.7: Started Resolution Does Not Schedule WFT

**User Story:** As a Tokeira developer, I want the Started resolution to not schedule a WFT, so that the non-terminal nature of Started is enforced and the workflow is not woken up prematurely.

#### Acceptance Criteria

1. FOR ALL NexusOperationResolved transitions with a Started variant where no WFT was previously pending, THE next_state SHALL NOT contain a PendingWorkflowTask.
2. FOR ALL NexusOperationResolved transitions with a Started variant, THE Transition SHALL NOT contain a DispatchOp::EnqueueWorkflowTask.

---

## Downstream Breakage and Compilation

### Requirement 8.1: Workspace Compilation After Type Changes

**User Story:** As a Tokeira developer, I want the workspace to compile after all type changes are made, so that downstream breakage from new enum variants and struct fields is resolved before behavioral implementation begins.

#### Acceptance Criteria

1. WHEN the new WorkflowCommand variants, Command variant, HistoryEventKind variants, DispatchOp variants, Reject variants, NexusResolution enum, NexusOperationResolvedRequest struct, and PendingNexusOperation struct are added, THE workspace SHALL compile without errors.
2. THE Start command handler SHALL initialize pending_nexus_operations to an empty BTreeMap.
3. THE close helper on TransitionBuilder SHALL clear the pending_nexus_operations map (no dispatch ops emitted for these entries).
