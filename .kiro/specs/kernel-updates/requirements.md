# Requirements Document: Updates (Feature 7)

## Introduction

This document captures the requirements for Feature 7 of the Tokeira kernel implementation: Updates. Feature 7 depends on Feature 1 (Foundation + WFT lifecycle), which is complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 7.1–7.4).

Feature 7 adds Temporal's Update feature to the kernel. Updates are the most complex message type because they span two transitions: acceptance (when the update arrives) and completion (when the worker finishes processing). Unlike a signal, the caller waits for the workflow to process the update and return a result or error. Unlike a query, the update mutates workflow state and is recorded in history.

This feature is not purely additive — it modifies durable `WorkflowState` (new `pending_updates` field) and the shared `close()` method on `TransitionBuilder` (clearing `pending_updates` on terminal close). The change surface includes:

- **WorkflowState**: gains `pending_updates: BTreeMap<String, PendingUpdate>` field.
- **Command**: gains `Update` variant.
- **WorkflowCommand**: gains 3 new variants (`UpdateCompleted`, `UpdateRejected`, `ProtocolMessage`).
- **HistoryEventKind**: gains 3 new variants (`WorkflowExecutionUpdateAccepted`, `WorkflowExecutionUpdateCompleted`, `WorkflowExecutionUpdateRejected`).
- **Reject**: gains `UnknownUpdate(String)` variant.
- **TransitionBuilder::close()**: must clear `pending_updates` (same discard-on-close pattern as `pending_external_signals`/`pending_external_cancels`).
- **Start command handler**: must initialize `pending_updates` to empty.

The feature introduces one top-level command and three workflow commands:

1. `Update` — top-level command issued by an external caller. Emits `WorkflowExecutionUpdateAccepted`, adds `PendingUpdate` to the pending set, schedules WFT if none pending (same coalescing as Signal). Carries `RequestContext` for dedup.
2. `UpdateCompleted` — workflow command within `WorkflowTaskCompleted`. Emits `WorkflowExecutionUpdateCompleted`, removes the update from the pending set. No `RequestDedupeOp`.
3. `UpdateRejected` — workflow command within `WorkflowTaskCompleted`. Emits `WorkflowExecutionUpdateRejected`, removes the update from the pending set. No `RequestDedupeOp`.
4. `ProtocolMessage` — workflow command within `WorkflowTaskCompleted`. Internal ordering primitive for update event sequencing. References a message ID that maps to an update acceptance or rejection. No standalone event emitted.

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
- **PendingUpdate**: Tracking record for an accepted but not yet completed update in WorkflowState. Keyed by update_id (String). Tracks update_id, accepted_event_id (i64), and name (String).
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **RequestContext**: Deduplication context carried by external API commands, containing a request_id.
- **ProtocolMessage**: An internal sequencing directive within a WFT completion that controls where update events land in the event sequence. Not a standalone history event.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.

## Requirements

---

## New Types and State Fields

### Requirement 1.1: PendingUpdate Type

**User Story:** As a Tokeira developer, I want a PendingUpdate type to track accepted but not yet completed updates in WorkflowState, so that the kernel can manage update lifecycle.

#### Acceptance Criteria

1. THE `PendingUpdate` struct SHALL include an `update_id` field of type `String` uniquely identifying the update.
2. THE `PendingUpdate` struct SHALL include an `accepted_event_id` field of type `i64` recording the event ID of the WorkflowExecutionUpdateAccepted event.
3. THE `PendingUpdate` struct SHALL include a `name` field of type `String` recording the update handler name.
4. THE `PendingUpdate` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.2: WorkflowState Pending Updates Map

**User Story:** As a Tokeira developer, I want WorkflowState to include a pending updates map, so that accepted updates are tracked as part of the run's durable state.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL include a `pending_updates` field of type `BTreeMap<String, PendingUpdate>` keyed by update_id.
2. WHEN a new WorkflowState is initialized (via Start command), THE `pending_updates` map SHALL be empty.


### Requirement 1.3: UpdateRequest Type

**User Story:** As a Tokeira developer, I want an UpdateRequest type to carry the data for the Update top-level command, so that the kernel has all necessary fields for update acceptance.

#### Acceptance Criteria

1. THE `UpdateRequest` struct SHALL include an `update_id` field of type `String` uniquely identifying the update.
2. THE `UpdateRequest` struct SHALL include an `update_name` field of type `String` identifying the update handler name.
3. THE `UpdateRequest` struct SHALL include an `input` field of type `Payloads` carrying the arguments to the update handler.
4. THE `UpdateRequest` struct SHALL include a `request` field of type `RequestContext` for deduplication.
5. THE `UpdateRequest` struct SHALL include a `now` field of type `OffsetDateTime`.
6. THE `UpdateRequest` struct SHALL derive `Clone, Debug, PartialEq`.

### Requirement 1.4: New Command Variant

**User Story:** As a Tokeira developer, I want a new Command variant for the Update top-level command, so that external callers can send updates to running workflows.

#### Acceptance Criteria

1. THE Command enum SHALL include an `Update(UpdateRequest)` variant.

### Requirement 1.5: New WorkflowCommand Variants

**User Story:** As a Tokeira developer, I want new WorkflowCommand variants for update completion, rejection, and protocol message sequencing, so that workflow code can express update outcomes.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include an `UpdateCompleted` variant with fields: `update_id` (String) and `result` (Payloads).
2. THE WorkflowCommand enum SHALL include an `UpdateRejected` variant with fields: `update_id` (String) and `failure` (String).
3. THE WorkflowCommand enum SHALL include a `ProtocolMessage` variant with fields: `message_id` (String) and `body` (UpdateProtocolBody).

### Requirement 1.6: New HistoryEventKind Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for update lifecycle events, so that these events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowExecutionUpdateAccepted` variant with fields: `update_id` (String), `update_name` (String), and `input` (Payloads).
2. THE HistoryEventKind enum SHALL include a `WorkflowExecutionUpdateCompleted` variant with fields: `update_id` (String) and `result` (Payloads).
3. THE HistoryEventKind enum SHALL include a `WorkflowExecutionUpdateRejected` variant with fields: `update_id` (String) and `failure` (String).

### Requirement 1.7: New Reject Variant

**User Story:** As a Tokeira developer, I want a new Reject variant for update completion/rejection of unknown updates, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Reject enum SHALL include an `UnknownUpdate(String)` variant for when UpdateCompleted or UpdateRejected references an update_id not in the pending_updates map.

---

## Update Command Behavior (Top-Level)

### Requirement 2.1: Update Command Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to accept updates from external callers, so that synchronous tracked write requests can be delivered to running workflows.

#### Acceptance Criteria

1. WHEN an Update command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an Update command is received, THE Kernel SHALL emit a WorkflowExecutionUpdateAccepted event carrying the update_id, update_name, and input.
3. WHEN an Update command is received, THE Kernel SHALL add a PendingUpdate entry to the pending_updates map keyed by update_id, recording the update_id, accepted_event_id (from the emitted event), and name.
4. WHEN an Update command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
5. WHEN an Update command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.

### Requirement 2.2: Update Command Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject Update commands for missing, closed, or duplicate-update runs, so that invalid updates are caught at the kernel boundary.

#### Acceptance Criteria

1. WHEN an Update command is received for a missing run, THE Kernel SHALL reject with MissingRun.
2. WHEN an Update command is received for a closed run, THE Kernel SHALL reject with RunClosed.
3. WHEN an Update command is received with an update_id that is already in the pending_updates map, THE Kernel SHALL reject with DuplicateUpdateId carrying the update_id.

### Requirement 2.3: DuplicateUpdateId Reject Variant

**User Story:** As a Tokeira developer, I want a DuplicateUpdateId reject variant, so that duplicate update acceptance is caught at the kernel boundary.

#### Acceptance Criteria

1. THE Reject enum SHALL include a `DuplicateUpdateId(String)` variant for when an Update command references an update_id already in the pending_updates map.

---

## UpdateCompleted Workflow Command Behavior

### Requirement 3.1: UpdateCompleted Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to record update completions from workflow code, so that update callers can receive results.

#### Acceptance Criteria

1. WHEN an UpdateCompleted workflow command is received for a known pending update within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionUpdateCompleted event carrying the update_id and result.
2. WHEN an UpdateCompleted workflow command is received, THE Kernel SHALL remove the update from the pending_updates map.
3. WHEN an UpdateCompleted workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).
4. WHEN an UpdateCompleted workflow command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is a workflow command, not an external API command).

### Requirement 3.2: UpdateCompleted Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject UpdateCompleted for unknown updates, so that stale or invalid completions are caught.

#### Acceptance Criteria

1. WHEN an UpdateCompleted workflow command is received with an update_id not in the pending_updates map, THE Kernel SHALL reject with UnknownUpdate carrying the update_id.

---

## UpdateRejected Workflow Command Behavior

### Requirement 4.1: UpdateRejected Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to record update rejections from workflow code, so that update callers can receive rejection reasons.

#### Acceptance Criteria

1. WHEN an UpdateRejected workflow command is received for a known pending update within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionUpdateRejected event carrying the update_id and failure.
2. WHEN an UpdateRejected workflow command is received, THE Kernel SHALL remove the update from the pending_updates map.
3. WHEN an UpdateRejected workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).
4. WHEN an UpdateRejected workflow command is received, THE Kernel SHALL NOT emit a RequestDedupeOp (this is a workflow command, not an external API command).

### Requirement 4.2: UpdateRejected Rejection Path

**User Story:** As a Tokeira developer, I want the Kernel to reject UpdateRejected for unknown updates, so that stale or invalid rejections are caught.

#### Acceptance Criteria

1. WHEN an UpdateRejected workflow command is received with an update_id not in the pending_updates map, THE Kernel SHALL reject with UnknownUpdate carrying the update_id.

---

## ProtocolMessage Workflow Command Behavior

### Requirement 5.1: ProtocolMessage Carries Update Outcome

**User Story:** As a Tokeira developer, I want ProtocolMessage to carry the update outcome inline and emit the corresponding event at the correct position in the command sequence, so that update events are interleaved correctly with other workflow commands in history.

#### Acceptance Criteria

1. THE `ProtocolMessage` WorkflowCommand variant SHALL include a `message_id` field of type `String` referencing the update protocol session.
2. THE `ProtocolMessage` WorkflowCommand variant SHALL include an `body` field of type `UpdateProtocolBody` carrying the actual update outcome.
3. THE `UpdateProtocolBody` enum SHALL include an `Accepted { update_id: String, update_name: String, input: Payloads }` variant for update acceptance.
4. THE `UpdateProtocolBody` enum SHALL include a `Completed { update_id: String, result: Payloads }` variant for update completion.
5. THE `UpdateProtocolBody` enum SHALL include a `Rejected { update_id: String, failure: String }` variant for update rejection.
6. WHEN a ProtocolMessage with an `Accepted` body is processed, THE Kernel SHALL emit a `WorkflowExecutionUpdateAccepted` event at the current position in the event sequence and add a `PendingUpdate` entry to the pending_updates map.
7. WHEN a ProtocolMessage with a `Completed` body is processed, THE Kernel SHALL look up the update_id in pending_updates, reject with `UnknownUpdate` if not found, emit a `WorkflowExecutionUpdateCompleted` event, and remove the entry from pending_updates.
8. WHEN a ProtocolMessage with a `Rejected` body is processed, THE Kernel SHALL look up the update_id in pending_updates, reject with `UnknownUpdate` if not found, emit a `WorkflowExecutionUpdateRejected` event, and remove the entry from pending_updates.
9. WHEN a ProtocolMessage workflow command is received, THE apply_workflow_command function SHALL return `false` (the run is not closed).
10. THE ProtocolMessage SHALL NOT emit a standalone "protocol message" event; the event it emits is determined by the body variant.

### Requirement 5.2: UpdateProtocolBody Enum

**User Story:** As a Tokeira developer, I want an UpdateProtocolBody enum to represent the possible outcomes carried by a ProtocolMessage, so that the kernel can determine what event to emit.

#### Acceptance Criteria

1. THE `UpdateProtocolBody` enum SHALL derive `Clone, Debug, PartialEq`.
2. THE `UpdateProtocolBody` enum SHALL be defined in the kernel command module.

---

## BasicKernel Integration

### Requirement 6.1: BasicKernel Apply Routing for Update Command

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route the Update command to a dedicated handler method, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN an Update command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_update` method.
2. THE `apply_update` method SHALL follow the same pattern as existing apply methods: call `expect_open`, construct a TransitionBuilder, emit RequestDedupeOp, emit WorkflowExecutionUpdateAccepted event, add PendingUpdate to state, conditionally schedule WFT, and call `finish`.

### Requirement 6.2: Workflow Command Dispatch for Update Operations

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle UpdateCompleted, UpdateRejected, and ProtocolMessage, so that update operations are processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::UpdateCompleted` that looks up the update_id in pending_updates, rejects with UnknownUpdate if not found, emits WorkflowExecutionUpdateCompleted, and removes the entry from pending_updates.
2. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::UpdateRejected` that looks up the update_id in pending_updates, rejects with UnknownUpdate if not found, emits WorkflowExecutionUpdateRejected, and removes the entry from pending_updates.
3. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::ProtocolMessage` that acts as a sequencing no-op (no standalone event, returns `false`).

---

## Close Path Cleanup

### Requirement 7.1: Pending Updates Map Cleared on Close

**User Story:** As a Tokeira developer, I want all close paths to clear the pending updates map, so that no orphaned pending updates remain in terminal state.

#### Acceptance Criteria

1. WHEN the Kernel closes a run via Terminate, THE Kernel SHALL clear the pending_updates map in next_state.
2. WHEN the Kernel closes a run via WorkflowExecutionTimedOut, THE Kernel SHALL clear the pending_updates map in next_state.
3. WHEN the Kernel closes a run via CompleteWorkflow, THE Kernel SHALL clear the pending_updates map in next_state.
4. WHEN the Kernel closes a run via FailWorkflow, THE Kernel SHALL clear the pending_updates map in next_state.
5. WHEN the Kernel closes a run via CancelWorkflow, THE Kernel SHALL clear the pending_updates map in next_state.
6. WHEN the Kernel closes a run via ContinueAsNew, THE Kernel SHALL clear the pending_updates map in next_state.
7. WHEN the Kernel clears pending_updates on close, THE Kernel SHALL NOT emit any DispatchOps for the cleared entries (discard on close, no dispatch ops — same pattern as pending_external_signals and pending_external_cancels).

---

## Structural Invariants

### Requirement 8.1: Event ID Contiguity for Update Transitions

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for all update transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL Update command transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL transitions containing UpdateCompleted workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL transitions containing UpdateRejected workflow commands, event IDs SHALL be contiguous starting from last_event_id + 1.

### Requirement 8.2: Transition Sequence Increment for Update Transitions

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for update transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL Update command transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 8.3: At-Most-One-WFT Invariant for Update Command

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after Update commands, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL Update command transitions, next_state SHALL contain at most one PendingWorkflowTask.
2. WHEN an Update command is received and a WFT is already pending, THE Transition SHALL NOT contain a DispatchOp::EnqueueWorkflowTask.

### Requirement 8.4: Pending Updates Map Consistency

**User Story:** As a Tokeira developer, I want the pending updates map to be consistent after every transition, so that update lifecycle tracking is accurate.

#### Acceptance Criteria

1. FOR ALL Update command transitions that succeed, THE next_state.pending_updates map SHALL contain an entry keyed by update_id with the correct update_id, accepted_event_id, and name.
2. FOR ALL UpdateCompleted workflow commands that succeed, THE next_state.pending_updates map SHALL NOT contain the completed entry.
3. FOR ALL UpdateRejected workflow commands that succeed, THE next_state.pending_updates map SHALL NOT contain the rejected entry.

### Requirement 8.5: Terminal State Invariants for Close with Pending Updates

**User Story:** As a Tokeira developer, I want all close paths to leave an empty pending updates map, so that terminal state is clean.

#### Acceptance Criteria

1. FOR ALL transitions where the run is closed, next_state.pending_updates SHALL be empty.

### Requirement 8.6: Request Deduplication Boundary for Update Operations

**User Story:** As a Tokeira developer, I want the Update top-level command to carry request dedup and the workflow commands to not carry request dedup, so that the external/internal boundary is respected.

#### Acceptance Criteria

1. FOR ALL Update command transitions, THE Transition SHALL contain exactly one RequestDedupeOp.
2. FOR ALL transitions containing only UpdateCompleted and/or UpdateRejected workflow commands (no top-level Update), THE Transition SHALL contain zero RequestDedupeOps.

---

## Downstream Breakage and Compilation

### Requirement 9.1: Workspace Compilation After Type Changes

**User Story:** As a Tokeira developer, I want the workspace to compile after all type changes are made, so that downstream breakage from new enum variants and struct fields is resolved before behavioral implementation begins.

#### Acceptance Criteria

1. WHEN the new Command variant (Update), WorkflowCommand variants (UpdateCompleted, UpdateRejected, ProtocolMessage), HistoryEventKind variants (WorkflowExecutionUpdateAccepted, WorkflowExecutionUpdateCompleted, WorkflowExecutionUpdateRejected), Reject variant (UnknownUpdate), and WorkflowState field (pending_updates) are added, THE workspace SHALL compile without errors.
2. THE Start command handler SHALL initialize pending_updates to an empty BTreeMap.
3. THE close helper on TransitionBuilder SHALL clear the pending_updates map (no dispatch ops emitted for these entries).
