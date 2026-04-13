# Requirements Document: Runtime Reset Replay Support

## Introduction

`ResetWorkflowExecution` in the edge cannot be completed correctly on the current runtime/storage surface. The kernel already records reset intent by closing the current run with a synthetic `WorkflowTaskFailed { cause: ResetWorkflow, base_run_id, new_run_id, fork_event_id }`, but the runtime has no successor-orchestration path equivalent to Continue-As-New and no authoritative way to materialize the successor from the chosen historical fork point.

This feature adds the minimal runtime and storage contract needed to make reset real:

- a kernel history-replay function that reconstructs `WorkflowState` from a history prefix
- the runtime can submit `Command::Reset`
- the lane post-commit path can detect a committed reset close
- storage can materialize a successor run using the kernel replay function to derive correct state
- the runtime owns the successor `RunKey` choice before reset is submitted
- the runtime can return the successor `run_id` only after that successor exists durably

The kernel replay function is the critical prerequisite. The current storage model persists only the final `WorkflowState` per run — there are no per-transition or per-event state snapshots. Reconstructing state at an arbitrary `fork_event_id` (especially mid-transition, e.g. at a `WORKFLOW_TASK_COMPLETED` boundary) requires replaying the history prefix through the kernel. The kernel is pure and deterministic, making this replay authoritative.

## Requirements

### Requirement 1: Runtime Reset Entry Point

**User Story:** As an edge developer, I want a first-class runtime reset method, so that `ResetWorkflowExecution` does not need to bypass the runtime and submit kernel commands directly.

#### Acceptance Criteria

1. THE runtime SHALL expose a `reset_workflow(execution: ExecutionRef, request: ResetRequest) -> Result<ResetWorkflowResult>` entry point.
2. WHEN the target execution cannot be resolved, THE runtime SHALL return an error indicating the execution was not found.
3. THE runtime SHALL submit `Command::Reset(request)` for the resolved run through the normal lane path.
4. THE runtime SHALL choose the successor `RunKey` before submitting reset and carry it through the reset/materialization flow.
5. THE runtime SHALL wait for successor materialization to complete before returning success to the caller.

### Requirement 2: Reset Successor Detection

**User Story:** As a runtime developer, I want the lane post-commit path to detect reset closes, so that successor creation happens only after the reset transition commits durably.

#### Acceptance Criteria

1. WHEN a committed transition closes a run because of reset, THE lane SHALL detect that condition in its post-commit path.
2. THE detection rule SHALL be based on committed history, not inferred only from terminal status.
3. THE lane SHALL extract `base_run_id`, `new_run_id`, and `fork_event_id` from the committed `WorkflowTaskFailed { cause: ResetWorkflow, ... }` history event.
4. WHEN the run is closed with `ExecutionStatus::Terminated` for a non-reset reason, THE lane SHALL NOT trigger reset successor creation.
5. THE lane SHALL use runtime-supplied successor identity (`RunKey`, `RunId`) rather than inventing successor storage identity after commit.
6. THE successor `RunKey` SHALL be deterministically derived from the `new_run_id` persisted in the reset history event (e.g. `RunKey(new_run_id.0)`), so that if the process crashes between reset commit and successor materialization, recovery can re-derive the same `RunKey` from the committed history without a durable side table.

### Requirement 3: Kernel History Replay for State Reconstruction

**User Story:** As a storage developer, I want a kernel function that reconstructs `WorkflowState` from a history prefix, so that reset successor materialization can derive correct state at any fork point without requiring per-event state snapshots in storage.

#### Acceptance Criteria

1. THE Kernel SHALL expose a `replay_history_prefix(ctx: ReplayContext, events: &[HistoryEvent]) -> Result<WorkflowState>` function that processes a sequence of history events and returns the `WorkflowState` as of the last event.
2. THE `ReplayContext` struct SHALL carry the non-historical envelope fields that are not present in history events: `run_key`, `namespace_id`, `workflow_id`, `run_id`, `deployment`, `build_id`, `parent_run_key`, `parent_workflow_id`, `first_run_started_at`. These are supplied by the caller (storage materialization) from the predecessor run's metadata and the chosen successor identity.
3. THE replay function SHALL process events in order, applying each event's effect to the state exactly as the kernel would during normal forward execution.
4. THE replay function SHALL handle all `HistoryEventKind` variants that appear in committed history (Started, Signaled, TaskScheduled, TaskStarted, TaskCompleted, TaskFailed, TaskTimedOut, ActivityScheduled, ActivityStarted, ActivityCompleted, ActivityFailed, ActivityTimedOut, ActivityCanceled, ActivityCancelRequested, TimerStarted, TimerFired, TimerCanceled, MarkerRecorded, child workflow events, external signal events, nexus events, update events, etc.).
5. THE replay function SHALL reconstruct activity state, timer state, child workflow state, and pending workflow task state from the history events.
6. THE replay function SHALL be pure and deterministic — given the same context and event sequence, it SHALL always produce the same `WorkflowState`.
7. FOR ALL valid history prefixes ending at a `WorkflowTaskCompleted` event, THE replay function SHALL produce a `WorkflowState` where `pending_workflow_task` is `None` (the completed WFT is no longer pending).
8. THE replay function SHALL reject an empty event sequence or a sequence whose first event is not `WorkflowExecutionStarted`.
9. THE replay function SHALL set `transition_seq` to `TransitionSeq::ZERO` on the output state. The successor run starts with a fresh OCC fence — transition boundaries are not encoded in history and cannot be reconstructed from events alone.
10. THE replay function SHALL set `last_event_id` to the `event_id` of the last event in the prefix.
11. THE replay function SHALL set the following truly non-historical state fields to their reset defaults, since these fields are mutated by kernel operations that do not emit history events and cannot be reconstructed from history:
    - `sticky` → `None` (sticky affinity is set from workflow-task start state, not a history event)
    - `wft_stamp` → `0` (monotonic stamp for invalidating in-flight WFT deliveries)
    - `ActivityState.pause_info` → `None` for all activities (activity pause/unpause/reset are operational actions without history events)
    - `ActivityState.stamp` → `0` for all activities (monotonic stamp for invalidating in-flight activity deliveries)
12. THE replay function SHALL reconstruct `pause_info` from `WorkflowExecutionPaused` / `WorkflowExecutionUnpaused` history events, and `versioning_override` and `completion_callbacks` from `WorkflowExecutionOptionsUpdated` history events — these fields ARE represented in history and must not be defaulted.

### Requirement 4: Authoritative History Prefix Materialization

**User Story:** As a runtime developer, I want storage to materialize a successor run from a history prefix, so that reset replay is based on authoritative committed history rather than summary state guesses.

#### Acceptance Criteria

1. THE storage layer SHALL expose a method that creates a successor run from a base run's committed history prefix up to and including `fork_event_id`.
2. THE materialization method SHALL preserve event order and event IDs within the copied prefix.
3. THE materialization method SHALL create the successor under the provided `new_run_id` / `RunKey`.
4. THE materialization method SHALL reject when `fork_event_id` is outside the committed history of the base run.
5. THE materialization method SHALL be atomic with respect to the successor run's initial durable presence.
6. THE materialization method SHALL derive the successor's `WorkflowState` by calling the kernel's `replay_history_prefix` function on the copied history prefix, rather than copying or guessing from the predecessor's final state.
7. THE materialization method SHALL populate the current-execution mapping for the successor `(namespace_id, workflow_id)` so that normal execution resolution can find the new run.
8. THE materialization method SHALL make the copied history readable through normal `read_history(successor_run_key, ...)` calls immediately after success.
9. THE materialization method SHALL NOT fabricate request-dedupe, backlog, or broker-delivery state beyond what is required for the successor run to exist durably and be queryable.

### Requirement 5: Successor Visibility to Runtime

**User Story:** As an edge developer, I want `ResetWorkflowExecution` to return a `run_id` only after the successor exists durably, so that the response never points at a run that was not created.

#### Acceptance Criteria

1. WHEN reset successor materialization succeeds, THE runtime SHALL return success only after the successor run is durably queryable.
2. WHEN reset successor materialization fails after the predecessor reset has committed, THE runtime SHALL surface an error and log the failure at error level.
3. THE predecessor's committed reset SHALL remain authoritative even if successor creation fails.
4. "durably queryable" SHALL mean:
   - `load_run(successor_run_key)` returns `LoadedRun::Existing`
   - `read_history(successor_run_key, ...)` returns the copied history prefix
   - execution resolution for `(namespace_id, workflow_id)` can find the successor run
5. THE reset completion model SHALL be synchronous from the caller's perspective, not detached fire-and-forget orchestration.

### Requirement 6: Reset Event Validation in Edge

**User Story:** As a UI user, I want invalid reset targets rejected before mutation, so that I cannot reset to a non-workflow-task event accidentally.

#### Acceptance Criteria

1. THE edge SHALL load committed history for the target run before issuing reset.
2. WHEN `workflow_task_finish_event_id` does not refer to a `WORKFLOW_TASK_COMPLETED`, `WORKFLOW_TASK_FAILED`, `WORKFLOW_TASK_TIMED_OUT`, or `WORKFLOW_TASK_STARTED` event, THE edge SHALL return `INVALID_ARGUMENT`.
3. WHEN the target execution does not exist, THE edge SHALL return `NOT_FOUND`.

### Requirement 7: Minimal Scope

**User Story:** As a maintainer, I want the reset replay support feature to stay narrowly scoped, so that it unblocks reset without expanding into unrelated replay machinery.

#### Acceptance Criteria

1. THE feature SHALL NOT redesign general workflow replay beyond the `replay_history_prefix` function needed for reset.
2. THE feature SHALL NOT change non-reset successor paths such as Continue-As-New.
3. THE feature SHALL introduce only the kernel replay function, runtime orchestration, and storage primitives required to make reset successor creation authoritative.
