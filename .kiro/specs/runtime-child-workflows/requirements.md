# Requirements Document: Child Workflow Orchestration

## Introduction

This document captures the requirements for Feature 6 of the Tokeira runtime: Child Workflow Orchestration. This feature wires the runtime's `DispatchPublisher` to handle child workflow dispatch ops (`StartChildWorkflow`, `TerminateChild`, `CancelChild`) and delivers child resolution results back to the parent workflow.

Child workflow orchestration is a two-phase operation. When a parent workflow issues a `StartChildWorkflow` command via a workflow task completion, the kernel emits a `DispatchOp::StartChildWorkflow`. The runtime must then:

1. Issue a `Command::Start` to create the child workflow run.
2. Deliver a `Command::ChildStartConfirmed` back to the parent run with either a success variant (carrying the child's `run_id` and `workflow_type`) or a failure variant (e.g., workflow ID already exists).

When the child workflow reaches a terminal state, the runtime must deliver a `Command::ChildResolved` to the parent run with the appropriate resolution variant (Completed, Failed, Canceled, Terminated, TimedOut).

The kernel already handles all child-related commands authoritatively:
- `Command::ChildStartConfirmed` updates `ChildWorkflowState` with the child's `run_id` and `started_event_id`, or removes the child entry on failure.
- `Command::ChildResolved` emits the appropriate history event and removes the child from the parent's `children` map.
- `apply_parent_close_policy()` is called on every close path (complete, fail, cancel, terminate, timeout, reset, continue-as-new) and emits `DispatchOp::TerminateChild` or `DispatchOp::CancelChild` for started children based on their `ParentClosePolicy`.

The `RuntimeDispatchPublisher` currently logs child/signal/cancel/nexus dispatch ops as stubs. This feature replaces the child workflow stubs with working implementations.

A key design consideration is child resolution delivery. When a child run closes, the runtime needs to know which parent run to notify. The child's `WorkflowState` does not currently carry parent identity. The implementation must either extend `StartRequest`/`WorkflowState` with parent run metadata, or maintain a runtime-local parent-child mapping. This is an implementation design decision captured in the design document.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing) and Feature 2 (Activity Pump), both of which are already implemented.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **DispatchPublisher**: The trait responsible for publishing dispatch ops from committed transitions. The `RuntimeDispatchPublisher` implements this trait and routes ops to the appropriate subsystem (broker, activity broker, or orchestration handler).
- **DispatchOp_StartChildWorkflow**: A dispatch op emitted by the kernel when a workflow task completion includes a `WorkflowCommand::StartChildWorkflow`. Carries `child_workflow_id`, `namespace_id`, `workflow_type`, `task_queue`, and `input`.
- **DispatchOp_TerminateChild**: A dispatch op emitted by the kernel's `apply_parent_close_policy()` when a parent run closes and a started child has `ParentClosePolicy::Terminate`. Carries `child_workflow_id`, `child_run_id`, and `reason`.
- **DispatchOp_CancelChild**: A dispatch op emitted by the kernel's `apply_parent_close_policy()` when a parent run closes and a started child has `ParentClosePolicy::RequestCancel`. Carries `child_workflow_id`, `child_run_id`, and `reason`.
- **ChildStartConfirmed**: A kernel command (`Command::ChildStartConfirmed`) delivered to the parent run after the runtime attempts to start the child. Contains `ChildStartResult::Started` (with `child_run_id` and `workflow_type`) or `ChildStartResult::Failed` (with `cause`).
- **ChildResolved**: A kernel command (`Command::ChildResolved`) delivered to the parent run when the child reaches a terminal state. Contains a `ChildResolution` variant: Completed, Failed, Canceled, Terminated, or TimedOut.
- **ChildWorkflowState**: Kernel state tracking an open child workflow in the parent's `WorkflowState.children` map. Keyed by `WorkflowId`. Carries `initiated_event_id`, `child_run_id` (once started), `started_event_id`, and `parent_close_policy`.
- **ParentClosePolicy**: Enum controlling what happens to a child when the parent closes: `Terminate` (forcibly terminate), `RequestCancel` (cooperative cancel), or `Abandon` (leave running).
- **Parent_Run_Key**: The `RunKey` of the parent workflow run. Required by the runtime to deliver `ChildStartConfirmed` and `ChildResolved` commands back to the parent. Must be discoverable from the child's context.

## Requirements

---

### Requirement 1: Start Child Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::StartChildWorkflow`, so that child workflow executions are created when the parent requests them.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::StartChildWorkflow`, THE Runtime SHALL issue a `Command::Start` for the child workflow with the `namespace_id`, `child_workflow_id` (as the child's `workflow_id`), `workflow_type`, `task_queue`, and `input` from the dispatch op.
2. THE Runtime SHALL assign a fresh `RunKey` and `RunId` for the child workflow run.
3. THE Runtime SHALL record the parent-child relationship so that the parent's `RunKey` is discoverable from the child's context for later resolution delivery.
4. WHEN the child `Command::Start` succeeds (returns `CommitResult::Applied`), THE Runtime SHALL submit a `Command::ChildStartConfirmed` to the parent run with `ChildStartResult::Started { child_run_id, workflow_type }`.
5. WHEN the child `Command::Start` fails (returns an error or `CommitResult::Conflict` after retry exhaustion), THE Runtime SHALL submit a `Command::ChildStartConfirmed` to the parent run with `ChildStartResult::Failed { cause }` describing the failure reason.
6. THE Runtime SHALL set the `initiated_event_id` field in the `ChildStartConfirmedRequest` to match the `initiated_event_id` recorded in the parent's `ChildWorkflowState` for the child.

---

### Requirement 2: Terminate Child Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::TerminateChild`, so that parent close policy can terminate child workflows.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::TerminateChild`, THE Runtime SHALL resolve the child run using the `child_run_id` from the dispatch op and submit a `Command::Terminate` to the child run with the `reason` from the dispatch op.
2. IF the child run is already closed, THEN THE Runtime SHALL treat the kernel rejection as a harmless no-op and log at debug level.
3. IF the child run is not found (absent from storage), THEN THE Runtime SHALL treat the absence as a harmless no-op and log at debug level.

---

### Requirement 3: Cancel Child Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle `DispatchOp::CancelChild`, so that parent close policy can request cancellation of child workflows.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::CancelChild`, THE Runtime SHALL resolve the child run using the `child_run_id` from the dispatch op and submit a `Command::Cancel` to the child run with the `reason` from the dispatch op.
2. IF the child run is already closed, THEN THE Runtime SHALL treat the kernel rejection as a harmless no-op and log at debug level.
3. IF the child run is not found (absent from storage), THEN THE Runtime SHALL treat the absence as a harmless no-op and log at debug level.

---

### Requirement 4: Child Resolution Delivery

**User Story:** As a Tokeira developer, I want the runtime to deliver child workflow resolutions back to the parent, so that the parent can observe child completion, failure, cancellation, termination, or timeout.

#### Acceptance Criteria

1. WHEN a child workflow run reaches a terminal state (the committed transition's `next_state.closed_at` is `Some`), THE Runtime SHALL submit a `Command::ChildResolved` to the parent run with the appropriate `ChildResolution` variant based on the child's terminal `ExecutionStatus` and close details:
   - `ExecutionStatus::Completed` maps to `ChildResolution::Completed` with the result payload from `WorkflowState.close_result`.
   - `ExecutionStatus::Failed` maps to `ChildResolution::Failed` with the failure message from `WorkflowState.close_failure`.
   - `ExecutionStatus::Cancelled` maps to `ChildResolution::Canceled`.
   - `ExecutionStatus::Terminated` maps to `ChildResolution::Terminated`.
   - `ExecutionStatus::TimedOut` maps to `ChildResolution::TimedOut`.
2. THE Runtime SHALL identify the parent run from the child's execution metadata (parent `RunKey` recorded at child start time).
3. IF the parent run is already closed when the child resolves, THEN THE Runtime SHALL treat the kernel rejection (`Reject::RunClosed`) as a harmless no-op and log at debug level.
4. IF the parent run is not found when the child resolves, THEN THE Runtime SHALL treat the absence as a harmless no-op and log at debug level.
5. THE Runtime SHALL set the `child_workflow_id` field in the `ChildResolvedRequest` to the child's `workflow_id`.

---

### Requirement 5: Parent Close Policy Enforcement

**User Story:** As a Tokeira developer, I want the runtime to enforce parent close policy dispatch ops asynchronously, so that child workflows are handled according to the configured policy when the parent closes without blocking the parent's close commit.

#### Acceptance Criteria

1. WHEN a committed transition closes a parent run and the kernel emits `DispatchOp::TerminateChild` ops, THE Runtime SHALL execute each termination dispatch.
2. WHEN a committed transition closes a parent run and the kernel emits `DispatchOp::CancelChild` ops, THE Runtime SHALL execute each cancel dispatch.
3. THE Runtime SHALL process parent close policy dispatch ops asynchronously; failure to terminate or cancel a child SHALL NOT block the parent's close commit or cause the parent's commit to fail.
4. IF a parent close policy dispatch fails (child not found, child already closed, transient error), THEN THE Runtime SHALL log the failure at warn level and continue processing remaining dispatch ops.

---

### Requirement 6: Parent Identity Propagation

**User Story:** As a Tokeira developer, I want the runtime to propagate parent identity when starting child workflows, so that child resolution delivery can find the parent run.

#### Acceptance Criteria

1. WHEN the Runtime starts a child workflow via `Command::Start`, THE Runtime SHALL record the parent's `RunKey` and `WorkflowId` in a location discoverable from the child's context.
2. THE parent identity mechanism SHALL survive runtime restarts. Parent identity recorded only in volatile runtime memory is insufficient; the parent-child relationship must be recoverable from durable state.
3. THE parent identity mechanism SHALL support the case where the parent and child are on different runtime nodes (different shards).

---

### Requirement 7: Child Dispatch Error Resilience

**User Story:** As a Tokeira developer, I want child workflow dispatch operations to be resilient to transient errors, so that temporary failures do not leave the parent in an inconsistent state.

#### Acceptance Criteria

1. IF the child `Command::Start` encounters a transient error (storage unavailable, lane channel closed), THEN THE Runtime SHALL still deliver a `Command::ChildStartConfirmed` with a failure variant to the parent run so the parent is not left waiting indefinitely.
2. IF the `Command::ChildStartConfirmed` delivery to the parent fails (parent lane closed, OCC exhaustion), THEN THE Runtime SHALL log the failure at warn level. The parent's `ChildWorkflowState` will remain in the initiated-but-unconfirmed state until the sweeper (Feature 11) or a future reconciliation mechanism resolves it.
3. IF the `Command::ChildResolved` delivery to the parent fails (parent lane closed, OCC exhaustion), THEN THE Runtime SHALL log the failure at warn level. The parent's `ChildWorkflowState` will remain in the started-but-unresolved state until the sweeper (Feature 11) or a future reconciliation mechanism resolves it.
4. IF a `DispatchOp::TerminateChild` or `DispatchOp::CancelChild` encounters a transient error, THEN THE Runtime SHALL log the failure at warn level and continue processing remaining dispatch ops in the batch.

---

### Requirement 8: Child Start Inherits Default Configuration

**User Story:** As a Tokeira developer, I want child workflow starts to use sensible defaults for fields not specified in the dispatch op, so that child workflows are created with valid configuration.

#### Acceptance Criteria

1. THE Runtime SHALL set the child's `workflow_task_timeout` to a sensible default (e.g. 10 seconds) when starting a child workflow, since the `DispatchOp::StartChildWorkflow` does not carry a workflow task timeout.
2. THE Runtime SHALL set the child's `memo`, `search_attributes`, `retry_policy`, `workflow_execution_timeout`, and `workflow_run_timeout` to empty or None defaults when starting a child workflow, since the dispatch op does not carry these fields.
3. THE Runtime SHALL set the child's `attempt` to 1 and `continued_execution_run_id` and `first_execution_run_id` to None, since the child is a fresh execution.

---

### Requirement 9: Close Details on WorkflowState (Prerequisite)

**User Story:** As a Tokeira developer, I want `WorkflowState` to retain terminal result and failure details, so that the runtime can extract them for child resolution delivery without reading history events.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL be extended with `close_result: Option<Payloads>` to store the result payload when a workflow completes successfully.
2. THE `WorkflowState` struct SHALL be extended with `close_failure: Option<String>` to store the failure message when a workflow fails.
3. THE kernel's `close()` path for `CompleteWorkflow` SHALL populate `close_result` with the result payload.
4. THE kernel's `close()` path for `FailWorkflow` SHALL populate `close_failure` with the failure message.
5. FOR all other close paths (Cancel, Terminate, TimedOut, ContinuedAsNew), `close_result` and `close_failure` SHALL remain `None`.

---

### Requirement 10: Publisher Repository Access for Child Run Resolution

**User Story:** As a Tokeira developer, I want the `RuntimeDispatchPublisher` to have access to the run repository, so that it can resolve `child_run_id` (RunId) to `RunKey` for lane routing when dispatching `TerminateChild` and `CancelChild` commands.

#### Acceptance Criteria

1. THE `RuntimeDispatchPublisher` SHALL hold a shared reference to the `RunRepository` so it can call `resolve_execution` to map `RunId` → `RunKey`.
2. WHEN dispatching `TerminateChild` or `CancelChild`, THE publisher SHALL resolve the child's `RunKey` via `repo.resolve_execution` using the `child_workflow_id` and `child_run_id` from the dispatch op.
3. IF `resolve_execution` returns `None` (child not found), THE publisher SHALL treat the dispatch as a harmless no-op and log at debug level.
