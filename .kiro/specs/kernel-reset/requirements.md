# Requirements Document: Reset (Feature 10)

## Introduction

This document captures the requirements for Feature 10 of the Tokeira kernel implementation: the Reset top-level command. Feature 10 depends on Feature 1 (foundation + WFT lifecycle), Feature 2 (WFT failure/timeout), Feature 3 (cancel and terminate), Feature 4 (ContinueAsNew + workflow timeout), Feature 5 (child workflows — for parent close policy), Feature 6 (external signals/cancels — for pending map cleanup), Feature 7 (updates — for pending map cleanup), and Feature 9 (Nexus operations — for pending map cleanup), all of which are complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirement 10.1).

Feature 10 adds:

- **Reset** — a top-level kernel command issued by an operator. Reset terminates the current workflow execution, discards history after a chosen event ID, and starts a new execution that replays from that point. The kernel emits a `WorkflowTaskFailed` event with a `RESET_WORKFLOW` cause (using the existing `WorkflowTaskFailedCause` enum from Feature 2), closes the current run with `ExecutionStatus::Terminated`, cleans up all open entities (same pattern as Terminate), and emits reset metadata for the runtime to create the successor run.

Key design points from the architecture doc:
- Reset is architecturally similar to ContinueAsNew: close the current run, let the runtime create the successor. The difference is that the successor replays from a historical point rather than starting fresh.
- The kernel does NOT copy history or construct the reset run's initial state — the runtime handles that. The runtime reads the fork event ID, loads history up to that point, and issues a Start command for the new run.
- The `WorkflowTaskFailed` event with `RESET_WORKFLOW` cause is how Temporal records a reset in history.
- Reset does NOT require a pending started WFT. It can be applied to any open run. The `WorkflowTaskFailed` event is emitted as a synthetic event that records the reset, not as a response to an actual WFT failure.
- Reset carries `RequestContext` for dedup (it is an operator-initiated external command).

This feature modifies:
- `WorkflowTaskFailedCause` enum — gains a `ResetWorkflow` variant.
- `WorkflowTaskFailed` HistoryEventKind variant — gains optional reset metadata fields (`base_run_id`, `new_run_id`, `fork_event_version`). These are `Option` because they are only populated for `RESET_WORKFLOW` cause, not for regular WFT failures.
- `Command` enum — gains a `Reset(ResetRequest)` variant.
- `Reject` enum — gains a `ResetConstraintViolation { reason: String }` variant.
- Existing `WorkflowTaskFailed` event construction sites must be updated to provide `None` for the new optional fields.

Downstream breakage: `Command` gains `Reset`. `Reject` gains `ResetConstraintViolation`. `WorkflowTaskFailed` event gains optional fields. `WorkflowTaskFailedCause` gains `ResetWorkflow`. The task plan must include a workspace compile checkpoint.

Tests should extend existing `property_tests.rs` and `golden_tests.rs`. All test tasks are required. Golden tests should be individual `#[test]` functions. Property tests should use the `proptest! { }` block style.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **Reset**: A top-level kernel command issued by an operator that terminates the current run and emits metadata for the runtime to create a successor run that replays from a chosen historical event.
- **ResetRequest**: The request struct for the Reset command, carrying fork_event_id, new_run_id, reason, RequestContext, and now.
- **Fork_Event_ID**: The event ID in the current run's history from which the reset successor will replay. History up to and including this event is preserved; history after it is discarded by the runtime.
- **WorkflowTaskFailedCause**: A domain enum describing why a workflow task failed. Feature 2 defined variants for non-determinism, bad attributes, etc. Feature 10 adds `ResetWorkflow`.
- **ResetConstraintViolation**: A Reject variant for invalid reset parameters (e.g., fork_event_id out of range).
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **ActivityOp**: An operation emitted by the Kernel for activity lifecycle management (Upsert or Delete).
- **TimerOp**: An operation emitted by the Kernel for timer lifecycle management (Upsert or Delete).

## Requirements

---

## New Types and Enum Extensions

### Requirement 1.1: ResetRequest Struct

**User Story:** As a Tokeira developer, I want a ResetRequest struct to carry all reset-specific fields, so that the Reset command has its own dedicated request type separate from WorkflowTaskFailedRequest.

#### Acceptance Criteria

1. THE ResetRequest struct SHALL include a `fork_event_id` field of type `i64` (the event ID to reset to).
2. THE ResetRequest struct SHALL include a `new_run_id` field of type `RunId` (the run ID for the reset successor).
3. THE ResetRequest struct SHALL include a `reason` field of type `String` (operator-provided reason for the reset).
4. THE ResetRequest struct SHALL include a `request` field of type `RequestContext` (for dedup).
5. THE ResetRequest struct SHALL include a `now` field of type `OffsetDateTime`.
6. THE ResetRequest struct SHALL derive Clone, Debug, PartialEq.
7. THE ResetRequest struct SHALL be defined in the kernel command module.

### Requirement 1.2: Reset Command Variant

**User Story:** As a Tokeira developer, I want a Reset variant in the Command enum, so that the kernel can receive reset commands from operators.

#### Acceptance Criteria

1. THE Command enum SHALL include a `Reset(ResetRequest)` variant.

### Requirement 1.3: WorkflowTaskFailedCause ResetWorkflow Variant

**User Story:** As a Tokeira developer, I want a ResetWorkflow variant in the WorkflowTaskFailedCause enum, so that the WorkflowTaskFailed event can distinguish resets from regular WFT failures.

#### Acceptance Criteria

1. THE WorkflowTaskFailedCause enum SHALL include a `ResetWorkflow` variant.

### Requirement 1.4: WorkflowTaskFailed Event Reset Metadata Fields

**User Story:** As a Tokeira developer, I want the WorkflowTaskFailed HistoryEventKind variant to carry optional reset metadata, so that the runtime can read reset-specific information from the event.

#### Acceptance Criteria

1. THE `WorkflowTaskFailed` HistoryEventKind variant SHALL include a `base_run_id` field of type `Option<RunId>`.
2. THE `WorkflowTaskFailed` HistoryEventKind variant SHALL include a `new_run_id` field of type `Option<RunId>`.
3. THE `WorkflowTaskFailed` HistoryEventKind variant SHALL include a `fork_event_version` field of type `Option<i64>`.
4. THE `WorkflowTaskFailed` HistoryEventKind variant SHALL include a `fork_event_id` field of type `Option<i64>`.
5. WHEN the `WorkflowTaskFailed` event is emitted for a regular WFT failure (non-reset), THE `base_run_id`, `new_run_id`, `fork_event_version`, and `fork_event_id` fields SHALL be `None`.
6. WHEN the `WorkflowTaskFailed` event is emitted for a reset, THE `base_run_id` SHALL be `Some` containing the current run's run_id, THE `new_run_id` SHALL be `Some` containing the reset successor's run_id, THE `fork_event_id` SHALL be `Some` containing the reset point, and THE `fork_event_version` SHALL be `None` (reserved for future versioning support).

### Requirement 1.5: ResetConstraintViolation Reject Variant

**User Story:** As a Tokeira developer, I want a ResetConstraintViolation variant in the Reject enum, so that invalid reset parameters produce a precise, programmatic rejection.

#### Acceptance Criteria

1. THE Reject enum SHALL include a `ResetConstraintViolation { reason: String }` variant.

---

## Reset Command Behavior

### Requirement 2.1: Reset Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle Reset, so that operators can discard history after a chosen event and restart from that point.

#### Acceptance Criteria

1. WHEN a Reset command is received for an open run with a valid fork_event_id, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Reset command is received, THE Kernel SHALL emit a WorkflowTaskFailed event with cause `ResetWorkflow`, carrying the reset metadata fields (base_run_id set to the current run's run_id, new_run_id set to the request's new_run_id, fork_event_version set to None).
3. WHEN a Reset command is received, THE Kernel SHALL close the current run with ExecutionStatus::Terminated by calling the TransitionBuilder's `close` method (set terminal status, clear pending WFT, clear StickyAffinity, clear pending entities, emit ProjectionOp::CloseExecution).
4. THE Kernel SHALL NOT create the successor run; the runtime reads the WorkflowTaskFailed event's reset metadata and issues a Start command for the successor.
5. THE Kernel SHALL NOT copy history or construct the reset run's initial state; the runtime handles that.

### Requirement 2.2: Reset WorkflowTaskFailed Event Details

**User Story:** As a Tokeira developer, I want the WorkflowTaskFailed event emitted by Reset to carry correct scheduled/started event IDs, so that the event is well-formed regardless of WFT state.

#### Acceptance Criteria

1. WHEN a Reset command is received and a pending WFT exists with a started_event_id, THE WorkflowTaskFailed event's scheduled_event_id SHALL reference the pending WFT's scheduled_event_id, and the started_event_id SHALL reference the pending WFT's started_event_id.
2. WHEN a Reset command is received and a pending WFT exists without a started_event_id (scheduled but not started), THE WorkflowTaskFailed event's scheduled_event_id SHALL reference the pending WFT's scheduled_event_id, and the started_event_id SHALL be 0 (sentinel value).
3. WHEN a Reset command is received and no pending WFT exists, THE WorkflowTaskFailed event's scheduled_event_id SHALL be 0 (sentinel value), and the started_event_id SHALL be 0 (sentinel value).
4. WHEN a Reset command is received, THE WorkflowTaskFailed event's logical_seq SHALL be the current next_workflow_task_seq (the next available sequence, since this is a synthetic event not tied to an actual WFT).
5. WHEN a Reset command is received, THE WorkflowTaskFailed event's identity SHALL be a WorkerIdentity representing the reset operator (constructed from the request's reason or a fixed sentinel like "reset").
6. WHEN a Reset command is received, THE WorkflowTaskFailed event's failure_details SHALL be None.

### Requirement 2.3: Reset Entity Cleanup

**User Story:** As a Tokeira developer, I want Reset to clean up all open entities, so that no orphaned activities, timers, children, or pending operations remain after a reset.

#### Acceptance Criteria

1. WHEN a Reset command is received and open activities exist, THE Kernel SHALL emit an ActivityOp::Delete for each open activity and clear the activities map in next_state.
2. WHEN a Reset command is received and open timers exist, THE Kernel SHALL emit a TimerOp::Delete for each open timer and clear the timers map in next_state.
3. WHEN a Reset command is received and open child workflows exist, THE Kernel SHALL apply Parent Close Policy for each open child (same as Terminate).
4. WHEN a Reset command is received, THE TransitionBuilder's `close` method SHALL clear pending_external_signals, pending_external_cancels, pending_updates, and pending_nexus_operations maps.

### Requirement 2.4: Reset Fork Event ID Validation

**User Story:** As a Tokeira developer, I want the Kernel to validate the fork_event_id, so that invalid reset points are rejected before any state mutation occurs.

#### Acceptance Criteria

1. WHEN a Reset command is received with a fork_event_id less than or equal to 0, THE Kernel SHALL reject with ResetConstraintViolation carrying a reason indicating the fork_event_id must be positive.
2. WHEN a Reset command is received with a fork_event_id greater than the run's last_event_id, THE Kernel SHALL reject with ResetConstraintViolation carrying a reason indicating the fork_event_id exceeds the last event.
3. WHEN a Reset command is received with a fork_event_id equal to 1 (the first event), THE Kernel SHALL accept the reset (fork_event_id == 1 is valid; it means replay from the very beginning).
4. WHEN a Reset command is received with a fork_event_id equal to last_event_id, THE Kernel SHALL accept the reset (fork_event_id == last_event_id is valid; it means replay from the latest point).

---

## Reset Rejection Paths

### Requirement 3.1: Reset Rejection for Missing or Closed Runs

**User Story:** As a Tokeira developer, I want the Kernel to reject Reset commands against non-existent or closed runs, so that resets are only applied to valid open runs.

#### Acceptance Criteria

1. WHEN a Reset command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
2. WHEN a Reset command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## BasicKernel Integration

### Requirement 4.1: BasicKernel Apply Routing for Reset

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route Reset commands to a dedicated handler method, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a Reset command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_reset` method.
2. THE `apply_reset` method SHALL follow the established pattern: validate with `expect_open`, validate fork_event_id, construct a TransitionBuilder, emit RequestDedupeOp, emit WorkflowTaskFailed event with reset metadata, call `close`, clean up entities, apply parent close policy, and call `finish`.

---

## Downstream Breakage and Compile Checkpoint

### Requirement 5.1: Exhaustive Match Updates

**User Story:** As a Tokeira developer, I want all known exhaustive matches on modified enums to be updated, so that the workspace compiles after the type changes.

#### Acceptance Criteria

1. WHEN `Command` gains the `Reset` variant, THE exhaustive match in `BasicKernel::apply` in kernel.rs SHALL include a match arm for `Reset`.
2. WHEN `WorkflowTaskFailedCause` gains the `ResetWorkflow` variant, known exhaustive matches across the workspace SHALL be updated to handle the new variant.
3. WHEN `WorkflowTaskFailed` HistoryEventKind variant gains `base_run_id`, `new_run_id`, and `fork_event_version` fields, ALL existing construction sites for this variant SHALL be updated to provide `None` for the new optional fields.
4. WHEN `Reject` gains the `ResetConstraintViolation` variant, known exhaustive matches across the workspace SHALL be updated.
5. AFTER all known call-site updates are applied, `cargo check --workspace` SHALL pass. Any additional compile failures discovered by the workspace check SHALL also be fixed before the feature is considered complete.

---

## Structural Invariants

### Requirement 6.1: Event ID Contiguity for Reset

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for Reset transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL Reset transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL Reset transitions, next_state.last_event_id SHALL equal the last emitted event's event_id.

### Requirement 6.2: Transition Sequence Increment for Reset

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for Reset transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL Reset transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 6.3: Reset Terminal State Invariants

**User Story:** As a Tokeira developer, I want Reset to satisfy all terminal state invariants, so that the closed run is well-formed.

#### Acceptance Criteria

1. FOR ALL Reset transitions, next_state.status SHALL be ExecutionStatus::Terminated.
2. FOR ALL Reset transitions, next_state.pending_workflow_task SHALL be None.
3. FOR ALL Reset transitions, next_state.sticky SHALL be None.
4. FOR ALL Reset transitions, next_state.closed_at SHALL be Some.
5. FOR ALL Reset transitions, next_state.activities SHALL be empty.
6. FOR ALL Reset transitions, next_state.timers SHALL be empty.
7. FOR ALL Reset transitions, next_state.pending_external_signals SHALL be empty.
8. FOR ALL Reset transitions, next_state.pending_external_cancels SHALL be empty.
9. FOR ALL Reset transitions, next_state.pending_updates SHALL be empty.
10. FOR ALL Reset transitions, next_state.pending_nexus_operations SHALL be empty.
11. FOR ALL Reset transitions, next_state.children SHALL be empty.

### Requirement 6.4: Reset Entity Cleanup Consistency

**User Story:** As a Tokeira developer, I want the number of cleanup ops emitted by Reset to match the number of open entities, so that cleanup is complete and not over-counted.

#### Acceptance Criteria

1. FOR ALL Reset transitions, THE number of ActivityOp::Delete ops SHALL equal the number of entries in the input state's activities map.
2. FOR ALL Reset transitions, THE number of TimerOp::Delete ops SHALL equal the number of entries in the input state's timers map.
3. FOR ALL Reset transitions, every ActivityOp::Delete SHALL reference an activity_id that existed in the input state's activities map.
4. FOR ALL Reset transitions, every TimerOp::Delete SHALL reference a timer_id that existed in the input state's timers map.

### Requirement 6.5: Reset Request Dedup

**User Story:** As a Tokeira developer, I want Reset to emit exactly one RequestDedupeOp, so that the external-command-dedup contract is maintained.

#### Acceptance Criteria

1. FOR ALL Reset transitions, request_dedupe_ops SHALL contain exactly one RequestDedupeOp with the request_id from the ResetRequest.

### Requirement 6.6: Reset Emits No Dispatch Ops for WFT

**User Story:** As a Tokeira developer, I want Reset to not schedule a workflow task for the current run, so that the terminated run does not receive further work.

#### Acceptance Criteria

1. FOR ALL Reset transitions, THE dispatch_ops SHALL NOT contain any EnqueueWorkflowTask ops.

### Requirement 6.7: Reset WorkflowTaskFailed Event Metadata Consistency

**User Story:** As a Tokeira developer, I want the WorkflowTaskFailed event emitted by Reset to always carry reset metadata, so that the runtime can distinguish reset events from regular WFT failures.

#### Acceptance Criteria

1. FOR ALL Reset transitions, THE WorkflowTaskFailed event's failure_cause SHALL be WorkflowTaskFailedCause::ResetWorkflow.
2. FOR ALL Reset transitions, THE WorkflowTaskFailed event's base_run_id SHALL be Some containing the input state's run_id.
3. FOR ALL Reset transitions, THE WorkflowTaskFailed event's new_run_id SHALL be Some containing the ResetRequest's new_run_id.

### Requirement 6.8: Regular WorkflowTaskFailed Events Carry No Reset Metadata

**User Story:** As a Tokeira developer, I want regular (non-reset) WorkflowTaskFailed events to carry None for all reset metadata fields, so that the new optional fields do not affect existing behavior.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskFailed transitions produced by the existing WorkflowTaskFailed command (Feature 2), THE base_run_id, new_run_id, fork_event_version, and fork_event_id fields SHALL all be None.

---

## Property Tests

### Requirement 7.1: Reset Closes the Run Property

**User Story:** As a Tokeira developer, I want a property test verifying that Reset always closes the run with Terminated status, so that the terminal command contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with last_event_id >= 1 and FOR ALL valid ResetRequest values with fork_event_id in [1, last_event_id], WHEN Reset is applied, THE next_state.status SHALL be ExecutionStatus::Terminated and next_state.closed_at SHALL be Some.

### Requirement 7.2: Reset Cleans Up All Open Entities Property

**User Story:** As a Tokeira developer, I want a property test verifying that Reset cleans up all open activities and timers, so that no orphaned entities remain.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with N open activities and M open timers, WHEN Reset is applied with a valid fork_event_id, THE activity_ops SHALL contain exactly N ActivityOp::Delete ops and THE timer_ops SHALL contain exactly M TimerOp::Delete ops, and next_state.activities and next_state.timers SHALL both be empty.

### Requirement 7.3: Reset Emits Exactly One Request Dedupe Op Property

**User Story:** As a Tokeira developer, I want a property test verifying that Reset always emits exactly one RequestDedupeOp, so that the external-command-dedup contract is maintained.

#### Acceptance Criteria

1. FOR ALL valid Reset transitions, THE request_dedupe_ops SHALL contain exactly one entry, and its request_id SHALL match the ResetRequest's request.request_id.

### Requirement 7.4: Reset Fork Event ID Validation Property

**User Story:** As a Tokeira developer, I want a property test verifying that Reset rejects invalid fork_event_id values, so that the validation boundary is correct.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState and FOR ALL fork_event_id values <= 0 or > last_event_id, WHEN Reset is applied, THE Kernel SHALL return Err(Reject::ResetConstraintViolation).

### Requirement 7.5: Reset WorkflowTaskFailed Event Always Carries Reset Metadata Property

**User Story:** As a Tokeira developer, I want a property test verifying that the WorkflowTaskFailed event emitted by Reset always carries reset metadata, so that the runtime can reliably identify reset events.

#### Acceptance Criteria

1. FOR ALL valid Reset transitions, THE WorkflowTaskFailed event's base_run_id SHALL be Some, THE new_run_id SHALL be Some, and THE failure_cause SHALL be WorkflowTaskFailedCause::ResetWorkflow.

### Requirement 7.6: Reset Emits No WFT Dispatch Ops Property

**User Story:** As a Tokeira developer, I want a property test verifying that Reset never schedules a WFT for the current run, so that the terminated run does not receive further work.

#### Acceptance Criteria

1. FOR ALL valid Reset transitions, THE dispatch_ops SHALL NOT contain any EnqueueWorkflowTask ops.
