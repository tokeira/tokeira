# Requirements Document: Kernel Markers and Execution Options (Feature 8)

## Introduction

This document captures the requirements for Feature 8 of the Tokeira kernel implementation: Markers and Execution Options. This feature adds two relatively simple capabilities to the kernel:

1. **RecordMarker** — a workflow command (within WorkflowTaskCompleted) that records opaque SDK-interpreted data in history. The kernel treats markers as pass-through: it assigns an event ID and records the event without interpreting the contents. SDKs use markers for side effects, local activity results, mutable side effects, and version markers during replay.

2. **UpdateExecutionOptions** — a top-level command that allows updating workflow execution options on a running workflow, such as versioning overrides and completion callbacks. This is a server-side mutation that does not come from workflow code and does not schedule a WFT.

Both commands are straightforward: RecordMarker is a pure event emission with no state change, and UpdateExecutionOptions is a state mutation + event emission + request dedup.

This feature depends on Feature 1 (Foundation + WFT Lifecycle), which is complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Feature 8, Requirements 8.1–8.2).

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **WorkflowCommand**: A command produced by workflow code when a workflow task completes. RecordMarker is a workflow command.
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call. Contains next_state, history events, dispatch ops, projection ops, activity/timer ops, and request dedupe ops.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **TransitionBuilder**: Internal helper that assembles a Transition by taking ownership of WorkflowState, emitting events with contiguous IDs, and incrementing transition_seq exactly once on `finish()`.
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **RequestContext**: Metadata carried by external API commands, including a request_id for deduplication.
- **Marker**: An opaque history entry used by SDKs to record side effects, local activity results, mutable side effects, and version markers. The kernel does not interpret marker contents.
- **VersioningOverride**: A placeholder type representing worker versioning configuration that can be set on a running workflow. The exact shape depends on future worker versioning design.
- **CompletionCallback**: A placeholder type representing a callback to be invoked when the workflow completes. The exact shape depends on future callback design.
- **Payloads**: A collection of serialized data payloads.
- **Payload**: A single serialized data payload.
- **HistoryEventKind**: The enum of all possible history event types emitted by the kernel.

## Requirements

---

### Requirement 8.1: RecordMarker Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to record opaque markers in history, so that SDKs can persist side effects, local activity results, and version markers for stable replay.

#### Acceptance Criteria

1. WHEN a RecordMarker workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a MarkerRecorded event carrying the marker_name (String), details (BTreeMap<String, Payloads>), failure (Option<Payload>), and header (Option<BTreeMap<String, Payload>>).
2. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT modify WorkflowState beyond updating last_event_id for the emitted event.
3. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT emit any dispatch ops.
4. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT emit any projection ops.
5. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT close the run (the command returns false from the closes-run check).
6. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT emit any RequestDedupeOp (RecordMarker is a workflow command within WorkflowTaskCompleted, not an external API command).

---

### Requirement 8.2: MarkerRecorded History Event

**User Story:** As a Tokeira developer, I want the MarkerRecorded event to carry all marker data faithfully, so that SDKs can reconstruct marker state during replay.

#### Acceptance Criteria

1. THE MarkerRecorded HistoryEventKind variant SHALL carry marker_name (String), details (BTreeMap<String, Payloads>), failure (Option<Payload>), and header (Option<BTreeMap<String, Payload>>).
2. THE Kernel SHALL assign a contiguous event ID to the MarkerRecorded event following the standard TransitionBuilder emit semantics.
3. FOR ALL MarkerRecorded events, THE Kernel SHALL preserve the marker data exactly as provided in the RecordMarker workflow command without interpretation or transformation.

---

### Requirement 8.3: RecordMarker WorkflowCommand Variant

**User Story:** As a Tokeira developer, I want the WorkflowCommand enum to include a RecordMarker variant, so that workflow code can express marker recording intent through the state machine.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a RecordMarker variant carrying marker_name (String), details (BTreeMap<String, Payloads>), failure (Option<Payload>), and header (Option<BTreeMap<String, Payload>>).
2. WHEN the RecordMarker variant is processed during WorkflowTaskCompleted command application, THE Kernel SHALL apply the same sequential processing rules as all other workflow commands (including CommandsAfterClose rejection if a preceding command closed the run).

---

### Requirement 8.4: UpdateExecutionOptions Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to handle execution option updates, so that operators can modify versioning overrides and completion callbacks on running workflows without a WFT round-trip.

#### Acceptance Criteria

1. WHEN an UpdateExecutionOptions command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID carried in the RequestContext.
2. WHEN an UpdateExecutionOptions command is received, THE Kernel SHALL emit a WorkflowExecutionOptionsUpdated event carrying the versioning_override (Option<VersioningOverride>), completion_callbacks (Vec<CompletionCallback>), and attached_request_id (Option<String>).
3. WHEN an UpdateExecutionOptions command is received with `versioning_override: FieldChange::Set(v)`, THE Kernel SHALL set the versioning_override field on WorkflowState to `Some(v)`.
4. WHEN an UpdateExecutionOptions command is received with `versioning_override: FieldChange::Clear`, THE Kernel SHALL set the versioning_override field on WorkflowState to `None`.
5. WHEN an UpdateExecutionOptions command is received with `completion_callbacks: FieldChange::Set(v)`, THE Kernel SHALL replace the completion_callbacks field on WorkflowState with `v`.
6. WHEN an UpdateExecutionOptions command is received with `completion_callbacks: FieldChange::Clear`, THE Kernel SHALL set the completion_callbacks field on WorkflowState to an empty Vec.
7. WHEN an UpdateExecutionOptions command is received with `versioning_override: FieldChange::Unchanged` or `completion_callbacks: FieldChange::Unchanged`, THE Kernel SHALL leave the corresponding field unchanged.
6. WHEN an UpdateExecutionOptions command is received, THE Kernel SHALL NOT schedule a workflow task (this is a server-side mutation, not a workflow-code-driven operation).
7. WHEN an UpdateExecutionOptions command is received, THE Kernel SHALL NOT close the run.
8. WHEN an UpdateExecutionOptions command is received for a missing run, THE Kernel SHALL reject with MissingRun.
9. WHEN an UpdateExecutionOptions command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

### Requirement 8.5: WorkflowExecutionOptionsUpdated History Event

**User Story:** As a Tokeira developer, I want the WorkflowExecutionOptionsUpdated event to capture the execution option changes, so that the history faithfully records what was modified.

#### Acceptance Criteria

1. THE WorkflowExecutionOptionsUpdated HistoryEventKind variant SHALL carry `versioning_override` of type `FieldChange<VersioningOverride>`, `completion_callbacks` of type `FieldChange<Vec<CompletionCallback>>`, and `attached_request_id` of type `Option<String>`.
2. THE Kernel SHALL assign a contiguous event ID to the WorkflowExecutionOptionsUpdated event following the standard TransitionBuilder emit semantics.

---

### Requirement 8.6: UpdateExecutionOptions Request Structure

**User Story:** As a Tokeira developer, I want the UpdateExecutionOptionsRequest to carry explicit change semantics for each field, so that the command can unambiguously express set, clear, or leave-unchanged for versioning overrides and callbacks.

#### Acceptance Criteria

1. THE UpdateExecutionOptionsRequest struct SHALL carry `versioning_override` of type `FieldChange<VersioningOverride>` where `FieldChange::Unchanged` means no change, `FieldChange::Set(v)` means set to `v`, and `FieldChange::Clear` means remove.
2. THE UpdateExecutionOptionsRequest struct SHALL carry `completion_callbacks` of type `FieldChange<Vec<CompletionCallback>>` where `FieldChange::Unchanged` means no change, `FieldChange::Set(v)` means replace with `v`, and `FieldChange::Clear` means remove all.
3. THE UpdateExecutionOptionsRequest struct SHALL carry `attached_request_id` of type `Option<String>` for the event payload.
4. THE UpdateExecutionOptionsRequest struct SHALL carry `request` of type `RequestContext` and `now` of type `OffsetDateTime`.

### Requirement 8.6a: FieldChange Enum

**User Story:** As a Tokeira developer, I want a FieldChange enum to express explicit set/clear/unchanged semantics, so that request and event payloads are unambiguous.

#### Acceptance Criteria

1. THE `FieldChange<T>` enum SHALL include `Unchanged`, `Set(T)`, and `Clear` variants.
2. THE `FieldChange<T>` enum SHALL derive `Clone, Debug, PartialEq`.
3. THE `FieldChange<T>` enum SHALL be defined in the kernel command module (or state module if shared).

---

### Requirement 8.7: WorkflowState Fields for Execution Options

**User Story:** As a Tokeira developer, I want WorkflowState to include versioning_override and completion_callbacks fields, so that execution options are tracked as part of the run's durable state.

#### Acceptance Criteria

1. THE WorkflowState struct SHALL include a versioning_override field of type Option<VersioningOverride>.
2. THE WorkflowState struct SHALL include a completion_callbacks field of type Vec<CompletionCallback>.
3. WHEN a Start command initializes WorkflowState, THE Kernel SHALL set versioning_override to None and completion_callbacks to an empty Vec.
4. WHEN the Kernel closes a run, THE Kernel SHALL NOT clear versioning_override or completion_callbacks (these are metadata fields, not pending operations).

---

### Requirement 8.8: Command Enum Extension for UpdateExecutionOptions

**User Story:** As a Tokeira developer, I want the Command enum to include an UpdateExecutionOptions variant, so that the kernel can route execution option updates through the standard apply path.

#### Acceptance Criteria

1. THE Command enum SHALL include an UpdateExecutionOptions variant carrying an UpdateExecutionOptionsRequest.
2. WHEN the Kernel's apply method receives a Command::UpdateExecutionOptions, THE Kernel SHALL route it to the UpdateExecutionOptions handler following the same pattern as other top-level commands.

---

### Requirement 8.9: Placeholder Types for Versioning and Callbacks

**User Story:** As a Tokeira developer, I want VersioningOverride and CompletionCallback to exist as placeholder types, so that the kernel can compile and the types can be refined when worker versioning and callback designs are finalized.

#### Acceptance Criteria

1. THE VersioningOverride type SHALL be defined as a placeholder struct that derives Clone, Debug, and PartialEq.
2. THE CompletionCallback type SHALL be defined as a placeholder struct that derives Clone, Debug, and PartialEq.
3. THE placeholder types SHALL be located in the kernel crate's type definitions (or re-exported from tokeira-types if appropriate).

---

### Requirement 8.10: Workspace Compilation After Type Changes

**User Story:** As a Tokeira developer, I want the workspace to compile after all type changes from Feature 8, so that downstream breakage from new enum variants and struct fields is resolved.

#### Acceptance Criteria

1. WHEN WorkflowCommand gains the RecordMarker variant, THE workspace SHALL compile without errors.
2. WHEN Command gains the UpdateExecutionOptions variant, THE workspace SHALL compile without errors.
3. WHEN HistoryEventKind gains MarkerRecorded and WorkflowExecutionOptionsUpdated variants, THE workspace SHALL compile without errors.
4. WHEN WorkflowState gains versioning_override and completion_callbacks fields, THE workspace SHALL compile without errors.
5. WHEN the Start command handler is updated to initialize versioning_override and completion_callbacks, THE workspace SHALL compile without errors.
