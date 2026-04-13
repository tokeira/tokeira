# Requirements Document: Update Two-Phase Lifecycle

## Introduction

This document captures the requirements for Feature 14 (Update Two-Phase Lifecycle) of the Tokeira runtime. Updates are synchronous, tracked write requests that span two kernel transitions: acceptance (when the update arrives via `Command::Update`) and completion (when the worker processes the update and returns a result or rejection via `WorkflowTaskCompleted` carrying `UpdateCompleted`, `UpdateRejected`, or `ProtocolMessage` workflow commands).

Unlike signals, updates require the caller to wait for a response. Unlike queries, updates mutate workflow state and are recorded in history. The runtime must bridge the gap between the initial `Command::Update` commit (which records acceptance in history and schedules a workflow task) and the eventual worker response (which completes or rejects the update in a subsequent transition).

The update lifecycle introduces a new runtime-level coordination mechanism: the runtime must track pending update callers, correlate worker responses back to waiting callers, and enforce timeouts so that unresponsive workers do not block callers indefinitely. This coordination is entirely a runtime concern — the kernel already handles the state machine transitions for updates (acceptance, completion, rejection, duplicate detection, and pending update tracking in `WorkflowState.pending_updates`).

Key architectural constraints:
- Updates go through the kernel — they produce history events, transitions, and dispatch ops.
- The kernel handles update state transitions: `Command::Update` emits `WorkflowExecutionUpdateAccepted` and adds to `pending_updates`; `UpdateCompleted`/`UpdateRejected`/`ProtocolMessage` workflow commands emit completion/rejection events and remove from `pending_updates`.
- The runtime routes `Command::Update` to the owning lane via `submit()`, the same path as signals and other commands.
- The runtime must maintain an in-memory registry of waiting update callers, keyed by `(run_key, update_id)`.
- Update caller notification happens when the lane processes a `WorkflowTaskCompleted` that contains update-resolving workflow commands.
- The kernel rejects updates to paused workflows (`Reject::WorkflowPaused`) and duplicate update IDs still in `pending_updates` (`Reject::DuplicateUpdateId`).
- The storage layer returns `CommitResult::Duplicate` (bare, no state) when the same `request_id` was already committed. This is distinct from `Reject::DuplicateUpdateId`: the kernel rejection fires for update_ids still pending; the storage dedupe fires for request_ids already committed regardless of whether the update is still pending, already completed, or already rejected. Because `CommitResult::Duplicate` carries no metadata, the runtime cannot determine the update's current lifecycle phase.
- Update timeouts are a runtime concern; the kernel's `pending_updates` state is not modified by a timeout.

Depends on: Feature 1 (Lane OCC Retry and Mailbox Coalescing).

The authoritative specifications are [020-kernel](../../../docs/architecture/020-kernel.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`TokeiraRuntime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **Kernel**: The pure deterministic state machine (`BasicKernel`) that computes transitions from loaded run state and commands. The kernel handles `Command::Update` and the `UpdateCompleted`/`UpdateRejected`/`ProtocolMessage` workflow commands.
- **Update**: A synchronous, tracked write request to a workflow execution. Identified by an `update_id` and `update_name`, carrying serialized input arguments. Updates span two transitions: acceptance and completion.
- **Update_Caller**: The entity waiting for an update to be accepted and/or completed. The Runtime holds a response channel for each waiting caller.
- **Update_Registry**: An in-memory map maintained by the Runtime, keyed by `(RunKey, update_id)`, holding response channels for callers waiting on update acceptance or completion.
- **Update_Acceptance**: The point at which the kernel commits a `Command::Update` transition, emitting a `WorkflowExecutionUpdateAccepted` history event and adding the update to `pending_updates`.
- **Update_Completion**: The point at which the worker completes the update via an `UpdateCompleted` or `ProtocolMessage(Completed)` workflow command within `WorkflowTaskCompleted`, emitting a `WorkflowExecutionUpdateCompleted` event and removing the update from `pending_updates`.
- **Update_Rejection**: The point at which the worker rejects the update via an `UpdateRejected` or `ProtocolMessage(Rejected)` workflow command within `WorkflowTaskCompleted`, emitting a `WorkflowExecutionUpdateRejected` event and removing the update from `pending_updates`.
- **PendingUpdate**: The kernel state entry (`PendingUpdate { update_id, accepted_event_id, name }`) tracking an accepted-but-not-yet-completed update in `WorkflowState.pending_updates`.
- **Update_Timeout**: The configurable maximum duration the Runtime waits for an update to reach acceptance or completion before returning a timeout error to the caller.
- **ExecutionRef**: A composite reference (`namespace_id`, `workflow_id`, optional `run_id`) used to identify a target workflow execution.
- **RunKey**: The durable storage key for a specific run, resolved from an ExecutionRef via the repository.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from a committed transition.
- **CommitResult**: The outcome of a storage commit — Applied (with new state), Conflict (OCC failure), or Duplicate (request already processed).
- **WorkflowCommand**: A command produced by workflow code within `WorkflowTaskCompleted`. Includes `UpdateCompleted`, `UpdateRejected`, and `ProtocolMessage` variants relevant to updates.
- **UpdateProtocolBody**: The body of a `ProtocolMessage` workflow command, carrying `Accepted`, `Completed`, or `Rejected` variants for update lifecycle events.

## Requirements

---

### Requirement 1: Update Command Submission

**User Story:** As a Tokeira developer, I want the runtime to expose an update method that routes `Command::Update` through the kernel via the lane, so that updates are recorded in history and delivered to workers.

#### Acceptance Criteria

1. THE Runtime SHALL expose an `update_workflow` method that accepts an ExecutionRef, an update_id (string), an update_name (string), a serialized input payload, a RequestContext, and an Update_Timeout duration.
2. WHEN `update_workflow` is called, THE Runtime SHALL resolve the ExecutionRef to a RunKey via the repository.
3. IF the ExecutionRef cannot be resolved to a RunKey, THEN THE Runtime SHALL return an error indicating the execution was not found.
4. WHEN the RunKey is resolved, THE Runtime SHALL construct a `Command::Update(UpdateRequest)` with the provided update_id, update_name, input, request context, and current wall-clock time, and submit it to the owning lane via the existing `submit()` path.
5. WHEN the lane commits the `Command::Update` transition successfully, THE Runtime SHALL publish any resulting dispatch ops (workflow task scheduling) after the commit succeeds, using the same `DispatchPublisher` mechanism as other commands.
6. IF the kernel rejects the `Command::Update` (e.g., `Reject::WorkflowPaused`, `Reject::DuplicateUpdateId`, `Reject::RunClosed`), THEN THE Runtime SHALL return the rejection as an error to the caller.

---

### Requirement 2: Update Caller Registry

**User Story:** As a Tokeira developer, I want the runtime to maintain an in-memory registry of waiting update callers, so that worker responses can be correlated back to the correct caller.

#### Acceptance Criteria

1. THE Runtime SHALL maintain an Update_Registry that maps `(RunKey, update_id)` to a response channel for each waiting update caller.
2. WHEN `update_workflow` is called with `wait_policy = Completed`, THE Runtime SHALL register the caller in the Update_Registry with a response channel BEFORE submitting the `Command::Update` to the lane. This pre-registration ensures that a fast worker completing the update between dispatch-op publication and `submit()` return does not race ahead of the registry entry. If the subsequent `submit()` fails or the kernel rejects the command, THE Runtime SHALL remove the pre-registered entry from the Update_Registry before returning the error.
3. WHEN an update is completed, rejected, or times out, THE Runtime SHALL remove the corresponding entry from the Update_Registry.
4. THE Update_Registry SHALL be safe for concurrent access from multiple lanes and multiple `update_workflow` callers.
5. THE Update_Registry SHALL NOT be persisted to durable storage. The registry is purely in-memory and ephemeral.

---

### Requirement 3: Update Acceptance Notification

**User Story:** As a Tokeira developer, I want the runtime to notify waiting callers when an update is accepted, so that callers can distinguish between "accepted and waiting for completion" and "not yet processed."

#### Acceptance Criteria

1. WHEN the `Command::Update` transition commits successfully (CommitResult::Applied), THE Runtime SHALL notify the waiting caller that the update has been accepted.
2. THE acceptance notification SHALL carry the accepted_event_id from the committed transition's history events.
3. WHEN the `Command::Update` transition returns CommitResult::Duplicate, THE Runtime SHALL treat the request as a deduplicated replay. Because `CommitResult::Duplicate` carries no state, the runtime cannot extract an `accepted_event_id` or determine whether the original update is still pending, already completed, or already rejected. THE Runtime SHALL return `UpdateOutcome::Accepted { accepted_event_id: 0 }` as a sentinel indicating "accepted in a prior commit, event ID unknown." THE Runtime SHALL NOT register the caller in the Update_Registry for completion waiting, because there is no guarantee a future resolution event will arrive (the update may already be terminal). Callers that need the completion result for a deduplicated update must poll the workflow history or re-resolve via `describe_workflow_execution`.
4. THE Runtime SHALL support callers that wait only for acceptance (returning immediately after acceptance without waiting for completion). When `wait_policy = Accepted`, no Update_Registry entry is created.
5. THE Runtime SHALL support callers that wait for both acceptance and completion (continuing to wait after acceptance until the worker completes or rejects the update, or the timeout expires). The two-phase lifecycle (acceptance then completion) applies only to `CommitResult::Applied` with `wait_policy = Completed`. The `CommitResult::Duplicate` path does not participate in phase 2 — the caller receives `UpdateOutcome::Accepted` immediately and must use other means (e.g., history polling) to obtain the terminal result.

---

### Requirement 4: Update Completion and Rejection Notification

**User Story:** As a Tokeira developer, I want the runtime to notify waiting callers when a worker completes or rejects an update, so that callers receive the update result.

#### Acceptance Criteria

1. WHEN the lane processes a `WorkflowTaskCompleted` transition that contains an `UpdateCompleted { update_id, result }` workflow command, THE Runtime SHALL look up the update_id in the Update_Registry and notify the waiting caller with the completion result payload.
2. WHEN the lane processes a `WorkflowTaskCompleted` transition that contains an `UpdateRejected { update_id, failure }` workflow command, THE Runtime SHALL look up the update_id in the Update_Registry and notify the waiting caller with the rejection failure reason.
3. WHEN the lane processes a `WorkflowTaskCompleted` transition that contains a `ProtocolMessage` with `UpdateProtocolBody::Completed { update_id, result }`, THE Runtime SHALL notify the waiting caller with the completion result, using the same mechanism as the standalone `UpdateCompleted` command.
4. WHEN the lane processes a `WorkflowTaskCompleted` transition that contains a `ProtocolMessage` with `UpdateProtocolBody::Rejected { update_id, failure }`, THE Runtime SHALL notify the waiting caller with the rejection failure, using the same mechanism as the standalone `UpdateRejected` command.
5. WHEN an update completion or rejection is committed but no caller is waiting in the Update_Registry (e.g., the caller already timed out), THE Runtime SHALL discard the notification silently without error.
6. THE Runtime SHALL extract update resolution information from the committed transition's history events, not from the raw workflow commands, to ensure only committed resolutions are reported to callers.

---

### Requirement 5: Update Timeout Handling

**User Story:** As a Tokeira developer, I want update dispatch to enforce a configurable timeout, so that unresponsive workers do not block update callers indefinitely.

#### Acceptance Criteria

1. THE Runtime SHALL enforce the Update_Timeout duration on each `update_workflow` call, starting from when the method is invoked.
2. WHEN the Update_Timeout expires before the update is accepted, THE Runtime SHALL return a timeout error to the caller and remove the entry from the Update_Registry.
3. WHEN the Update_Timeout expires after acceptance but before completion, THE Runtime SHALL return a timeout error to the caller indicating that the update was accepted but not completed within the timeout, and remove the entry from the Update_Registry.
4. THE Runtime SHALL NOT modify run state, pending_updates, or create transitions as a result of an update timeout at the runtime level. The update remains pending in the kernel's `WorkflowState.pending_updates` and may still be completed by the worker in a future transition.
5. THE Update_Timeout SHALL be configurable per `update_workflow` call. The caller provides the timeout as a parameter.
6. WHEN an update times out and the worker later completes or rejects the update, THE Runtime SHALL process the worker's response normally through the kernel (the transition commits and history is updated) but SHALL NOT attempt to notify the timed-out caller.

---

### Requirement 6: Concurrent Updates to the Same Run

**User Story:** As a Tokeira developer, I want multiple concurrent updates to the same run to be independent, so that one slow update does not block or interfere with another.

#### Acceptance Criteria

1. THE Runtime SHALL support multiple concurrent `update_workflow` calls targeting the same RunKey with different update_ids.
2. EACH concurrent update SHALL have its own independent entry in the Update_Registry, its own response channel, and its own Update_Timeout.
3. THE Runtime SHALL NOT serialize concurrent updates to the same run at the runtime level. Each `Command::Update` is submitted independently to the lane. The lane serializes them per-run as part of normal lane processing.
4. WHEN multiple updates are pending for the same run, completing or rejecting one update SHALL NOT affect any other pending update for that run.

---

### Requirement 7: Update Lifecycle Event Extraction

**User Story:** As a Tokeira developer, I want the runtime to detect update resolution events from committed transitions, so that waiting callers are notified promptly.

#### Acceptance Criteria

1. WHEN a `WorkflowTaskCompleted` transition is committed, THE Runtime SHALL scan the committed history events for `WorkflowExecutionUpdateCompleted` and `WorkflowExecutionUpdateRejected` events.
2. FOR EACH `WorkflowExecutionUpdateCompleted` event found, THE Runtime SHALL extract the update_id and result, and notify the corresponding caller in the Update_Registry.
3. FOR EACH `WorkflowExecutionUpdateRejected` event found, THE Runtime SHALL extract the update_id and failure reason, and notify the corresponding caller in the Update_Registry.
4. THE Runtime SHALL process all update resolution events from a single transition before moving to the next mailbox item, ensuring callers are notified in the same activation cycle as the commit.
5. WHEN a `WorkflowTaskCompleted` transition contains multiple update resolutions (e.g., two updates completed in the same WFT), THE Runtime SHALL notify each corresponding caller independently.

---

### Requirement 8: Update Dispatch for Closed and Paused Executions

**User Story:** As a Tokeira developer, I want updates to closed or paused executions to be rejected cleanly, so that callers receive a clear error rather than waiting indefinitely.

#### Acceptance Criteria

1. WHEN `update_workflow` is called and the kernel rejects the `Command::Update` with `Reject::RunClosed`, THE Runtime SHALL return an error indicating the execution is closed.
2. WHEN `update_workflow` is called and the kernel rejects the `Command::Update` with `Reject::WorkflowPaused`, THE Runtime SHALL return an error indicating the workflow is paused.
3. WHEN `update_workflow` is called and the kernel rejects the `Command::Update` with `Reject::DuplicateUpdateId`, THE Runtime SHALL return an error indicating the update ID is already pending.
4. THE Runtime SHALL NOT register the caller in the Update_Registry when `wait_policy = Accepted` (no completion waiting needed) or when `CommitResult::Duplicate` is returned (the update may already be terminal). For `wait_policy = Completed`, the caller is pre-registered before `submit()` and removed on failure — see Requirement 2.2.

---

### Requirement 9: Update Registry Cleanup on Run Close

**User Story:** As a Tokeira developer, I want the update registry to be cleaned up when a run closes, so that callers waiting on updates for a closed run are notified promptly rather than waiting for timeout.

#### Acceptance Criteria

1. WHEN a transition closes a run (the committed `WorkflowState` has `closed_at` set), THE Runtime SHALL scan the Update_Registry for all entries matching the closed run's RunKey.
2. FOR EACH matching entry in the Update_Registry, THE Runtime SHALL notify the waiting caller that the run has closed without completing the update, and remove the entry.
3. THE Runtime SHALL perform registry cleanup for run closure in the same activation cycle as the close commit, before processing the next mailbox item.
