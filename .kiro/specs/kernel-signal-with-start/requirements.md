# Requirements Document

## Introduction

The Temporal API provides `SignalWithStartWorkflowExecution` and workflow ID conflict resolution policies that determine what happens when a workflow start request targets an ID that already has a running or closed execution. Tokeira currently has none of this — `apply_start` rejects with `RunAlreadyExists` if any run exists, and there is no atomic signal-with-start primitive.

This spec adds three capabilities:
1. **Atomic signal-with-start** — a kernel primitive that creates a workflow and delivers a signal in a single transition, producing the correct `Started → Signaled → WFTScheduled` event order
2. **WorkflowIdConflictPolicy** — determines behavior when starting a workflow whose ID has a *running* execution (fail, use existing, or terminate existing)
3. **WorkflowIdReusePolicy** — determines behavior when starting a workflow whose ID has a *closed* execution (allow duplicate, allow only if failed, or reject)

These policies apply to both `StartWorkflowExecution` and `SignalWithStartWorkflowExecution`.

## Glossary

- **Kernel**: The `BasicKernel` in `tokeira-kernel` — pure, deterministic, no I/O.
- **Transition**: One atomic commit: next state, history events, dispatch ops, projection ops.
- **TransitionBuilder**: Internal builder that accumulates events and state mutations.
- **LoadedRun**: `Absent` (no run) or `Existing(WorkflowState)`.
- **WFT**: Workflow Task.
- **ConflictPolicy**: `WorkflowIdConflictPolicy` — governs behavior when the target workflow ID has a *running* execution.
- **ReusePolicy**: `WorkflowIdReusePolicy` — governs behavior when the target workflow ID has a *closed* execution.

## Requirements

### Requirement 1: Atomic SignalWithStart kernel primitive

**User Story:** As a runtime developer, I want a single kernel operation that atomically creates a workflow and delivers a signal, so that the signal appears in history before the first workflow task.

#### Acceptance Criteria

1. THE Kernel SHALL accept a `SignalWithStartRequest` containing all `StartRequest` fields plus `signal_name` and `signal_input`. Signal header support is deferred — `HistoryEventKind::WorkflowExecutionSignaled` does not currently carry a header field.
2. WHEN applied with `LoadedRun::Absent`, THE Kernel SHALL produce a `Transition` with exactly three history events: `WorkflowExecutionStarted` (event_id=1), `WorkflowExecutionSignaled` (event_id=2), `WorkflowTaskScheduled` (event_id=3).
3. WHEN applied with `LoadedRun::Absent`, THE Transition SHALL have `next_state.status == Running`, `pending_workflow_task` populated, one `DispatchOp::EnqueueWorkflowTask`, one `RequestDedupeOp`, and one `ProjectionOp::UpsertExecution`.
4. WHEN applied with `LoadedRun::Existing` (any state), THE Kernel SHALL return `Err(Reject::RunAlreadyExists)`. The kernel method only handles the absent case — the runtime owns all existing-run branching (signal delivery, conflict resolution, terminate-and-restart) before calling the kernel.

### Requirement 2: WorkflowIdConflictPolicy for running workflows

**User Story:** As an SDK user, I want to control what happens when I start a workflow whose ID already has a running execution, so that I can choose between failing, reusing the existing run, or terminating it.

#### Acceptance Criteria

1. THE system SHALL support three conflict policies for running workflows:
   - `Fail` — return `WorkflowExecutionAlreadyStarted` error
   - `UseExisting` — return the existing run_id without modification
   - `TerminateExisting` — terminate the running workflow and start a new one
2. WHEN `ConflictPolicy::Fail` and a running workflow exists, THE runtime SHALL return an error without modifying the existing workflow.
3. WHEN `ConflictPolicy::UseExisting` and a running workflow exists, THE runtime SHALL return the existing `run_id` without starting a new workflow. For `StartWorkflowExecution`, the runtime's `start_workflow` method SHALL return a `StartWorkflowResult` that can indicate either `Created { run_id }` or `AlreadyRunning { run_id }`, so the edge layer can distinguish the two cases.
4. WHEN `ConflictPolicy::TerminateExisting` and a running workflow exists, THE runtime SHALL terminate the existing workflow (emitting `WorkflowExecutionTerminated`) and then start a new workflow. These are two sequential commits, not one atomic operation. The runtime SHALL NOT return success until both the termination and the new start have committed successfully.
5. FOR `SignalWithStartWorkflowExecution` with `ConflictPolicy::UseExisting`, THE runtime SHALL deliver the signal to the existing running workflow and return its `run_id`.
6. FOR `SignalWithStartWorkflowExecution` with `ConflictPolicy::TerminateExisting`, THE runtime SHALL terminate the existing workflow, start a new one with the signal delivered atomically, and return the new `run_id`.
7. THE default conflict policy (when unspecified) SHALL be `UseExisting` for `SignalWithStart` and `Fail` for `StartWorkflowExecution`.

### Requirement 3: WorkflowIdReusePolicy for closed workflows

**User Story:** As an SDK user, I want to control what happens when I start a workflow whose ID had a previous closed execution, so that I can prevent accidental re-runs or allow them selectively.

#### Acceptance Criteria

1. THE system SHALL support three reuse policies for closed workflows:
   - `AllowDuplicate` — always allow starting a new run
   - `AllowDuplicateFailedOnly` — allow only if the previous run failed, cancelled, or terminated (not completed)
   - `RejectDuplicate` — never allow reuse
2. WHEN `ReusePolicy::AllowDuplicate` and a closed workflow exists, THE runtime SHALL start a new run.
3. WHEN `ReusePolicy::AllowDuplicateFailedOnly` and the previous run completed successfully, THE runtime SHALL return a `WorkflowExecutionAlreadyStarted` error.
4. WHEN `ReusePolicy::AllowDuplicateFailedOnly` and the previous run failed/cancelled/terminated/timed out, THE runtime SHALL start a new run.
5. WHEN `ReusePolicy::RejectDuplicate` and any closed workflow exists, THE runtime SHALL return a `WorkflowExecutionAlreadyStarted` error.
6. THE default reuse policy (when unspecified) SHALL be `AllowDuplicate`.

### Requirement 4: Policy extraction from proto requests

**User Story:** As an edge developer, I want the conflict and reuse policies extracted from proto requests and threaded through the system, so that the runtime can apply them.

#### Acceptance Criteria

1. THE edge layer SHALL extract `workflow_id_reuse_policy` and `workflow_id_conflict_policy` from `StartWorkflowExecutionRequest` and `SignalWithStartWorkflowExecutionRequest` proto messages.
2. THE edge layer SHALL migrate the deprecated `WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING` to `ConflictPolicy::TerminateExisting` + `ReusePolicy::AllowDuplicate` (matching Temporal's migration logic).
3. THE `StartRequest` and `SignalWithStartRequest` structs SHALL carry both policies as fields.
4. THE runtime SHALL pass the policies to the conflict resolution logic before calling the kernel.

### Requirement 5: History event order correctness

**User Story:** As an SDK developer, I want the signal-with-start history to match the Temporal protocol exactly.

#### Acceptance Criteria

1. FOR ALL valid `SignalWithStartRequest` values applied to `LoadedRun::Absent`, `WorkflowExecutionStarted.event_id < WorkflowExecutionSignaled.event_id < WorkflowTaskScheduled.event_id`.
2. THE `WorkflowExecutionSignaled` event SHALL contain the exact `signal_name` and `signal_input` from the request. Signal header fidelity is deferred until `HistoryEventKind::WorkflowExecutionSignaled` gains a header field.
3. THE `WorkflowExecutionStarted` event SHALL contain the exact `workflow_type`, `task_queue`, and `input` from the request.
