# Requirements Document

## Introduction

Complete the pause/unpause workflow feature across the Tokeira stack. The kernel already has `apply_pause_workflow` and `apply_unpause_workflow` transition logic, but several gaps remain: signals written while paused must be recorded without scheduling a WFT, pause idempotency must match Temporal's request-id-gated behavior, the edge gRPC handlers are placeholder handlers returning UNIMPLEMENTED, the runtime adapter lacks pause/unpause methods, DescribeWorkflowExecution does not surface pause metadata, paused queries are not rejected correctly, and visibility does not expose paused workflow status queries. This document closes those gaps to match Temporal v1.31.0 semantics.

## Glossary

- **Kernel**: The `tokeira-kernel` crate — a pure deterministic state machine that applies commands and produces transitions. No I/O, no async.
- **Edge**: The `tokeira-edge` crate — the gRPC compatibility shell that translates Temporal proto requests into kernel commands.
- **Runtime**: The `tokeira-runtime` crate — owns lanes, broker, sweepers, and dispatches commands to the kernel.
- **WorkflowState**: The authoritative per-run state struct in the kernel, containing `pause_info`, `wft_stamp`, and all pending entities.
- **PauseInfo**: Metadata struct recording `pause_time`, `identity`, `reason`, and `request_id` when a workflow is paused.
- **WFT**: Workflow Task — the unit of work dispatched to a worker SDK for replay and command generation.
- **Signal**: An asynchronous message delivered to a workflow execution, triggering a WFT for the workflow code to observe it.
- **Visibility**: The projection-plane read model that supports list/count queries with search attributes.
- **System_Search_Attribute**: A server-managed visibility field, such as `ExecutionStatus`, that is automatically maintained on state transitions.

## Requirements

### Requirement 1: Signals While Paused

**User Story:** As a workflow operator, I want signals sent to a paused workflow to be recorded durably without waking the workflow, so that no signals are lost during a maintenance pause and workflow code observes them after unpause.

#### Acceptance Criteria

1. WHILE the Kernel WorkflowState has `status == ExecutionStatus::Paused`, WHEN a `Signal` command is applied, THE Kernel SHALL emit the normal `WorkflowExecutionSignaled` history event and SHALL NOT schedule a WFT.
2. WHILE paused, signal history events SHALL preserve every field currently modeled on `SignalRequest` and emitted by the normal running signal path, including `signal_name`, `input`, `identity`, and `request_id`.
3. THE Kernel SHALL record a `RequestDedupeOp` for each signal at signal time, so that duplicate signal deliveries are rejected even while paused.
4. WHILE the Kernel WorkflowState has `status == ExecutionStatus::Paused`, WHEN a `SignalWithStart` command targets an existing run, THE start branch SHALL NOT be taken and the existing run SHALL be signalled using the same paused signal behavior as a standalone `Signal` command.
5. WHEN an `UnpauseWorkflow` command is applied, THE Kernel SHALL schedule a WFT so workflow code can process signal events and other server-side events accumulated while paused.

### Requirement 2: Pause and Unpause Idempotency

**User Story:** As an operator using automation tooling, I want pause retries with the same request ID to be idempotent while conflicting pause/unpause calls report precondition failures, so that retries are safe without hiding operator mistakes.

#### Acceptance Criteria

1. WHEN a `PauseWorkflow` command is applied to a workflow that is already paused and the request ID matches the stored pause request ID, THE Kernel SHALL return a no-op transition (no new events, no state change) with success status.
2. WHEN a `PauseWorkflow` command is applied to a workflow that is already paused and the request ID differs from the stored pause request ID, THE Kernel SHALL return `Reject::AlreadyPaused`.
3. WHEN an `UnpauseWorkflow` command is applied to a workflow that is not paused, THE Kernel SHALL return `Reject::NotPaused`.
4. THE Edge SHALL map `Reject::AlreadyPaused` and `Reject::NotPaused` to gRPC `FAILED_PRECONDITION`.
5. THE internal `PauseInfo` SHALL retain the pause request ID for idempotency checks even though the proto pause info response does not expose request ID.

### Requirement 3: Edge gRPC Handlers

**User Story:** As an SDK client, I want to call `PauseWorkflowExecution` and `UnpauseWorkflowExecution` RPCs and receive proper responses, so that I can control workflow pause state through the standard Temporal API.

#### Acceptance Criteria

1. WHEN the Edge receives a `PauseWorkflowExecutionRequest`, THE Edge SHALL translate the request into a `Command::PauseWorkflow` with the caller identity, reason, and request context, route it to the Runtime, and return a `PauseWorkflowExecutionResponse` on success.
2. WHEN the Edge receives an `UnpauseWorkflowExecutionRequest`, THE Edge SHALL translate the request into a `Command::UnpauseWorkflow` with the caller identity, reason, and request context, route it to the Runtime, and return an `UnpauseWorkflowExecutionResponse` on success.
3. WHEN the Edge translates a `PauseWorkflowExecutionRequest` that omits the namespace or workflow execution fields, THE Edge SHALL return an `INVALID_ARGUMENT` gRPC status.
4. THE Edge SHALL replace the current placeholder unary handlers for `pause_workflow_execution` and `unpause_workflow_execution` with real handler implementations.
5. THE Edge SHALL report `workflow_pause: true` in namespace capabilities where Temporal exposes workflow-pause support.

### Requirement 4: Runtime Adapter Methods

**User Story:** As the Edge layer, I want to call `pause_workflow` and `unpause_workflow` methods on the Runtime adapter, so that pause commands are routed through the standard lane-based execution path.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `pause_workflow` method that accepts namespace, workflow ID, run ID, identity, reason, and request context, loads the run on the owning lane, applies `Command::PauseWorkflow` via the kernel, and persists the resulting transition.
2. THE Runtime SHALL expose an `unpause_workflow` method that accepts namespace, workflow ID, run ID, identity, reason, and request context, loads the run on the owning lane, applies `Command::UnpauseWorkflow` via the kernel, and persists the resulting transition.
3. WHEN the Runtime processes a successful `UnpauseWorkflow` transition that contains a WFT dispatch op, THE Runtime SHALL enqueue that WFT dispatch through the standard broker path.

### Requirement 5: DescribeWorkflowExecution Surfaces Pause Info

**User Story:** As an operator, I want `DescribeWorkflowExecution` to show whether a workflow is paused and the associated metadata, so that I can inspect pause state through the standard API.

#### Acceptance Criteria

1. WHEN the Edge builds a `DescribeWorkflowExecutionResponse` for a paused workflow, THE Edge SHALL populate the `workflow_execution_info.status` field with `WORKFLOW_EXECUTION_STATUS_PAUSED` (proto enum value 8).
2. WHEN the Edge builds a `DescribeWorkflowExecutionResponse` for a paused workflow, THE Edge SHALL populate `workflow_extended_info.pause_info` with `identity`, `paused_time`, and `reason` from the kernel `PauseInfo`.
3. WHEN the Edge builds a `DescribeWorkflowExecutionResponse` for a running workflow, THE Edge SHALL omit `workflow_extended_info.pause_info`.

### Requirement 6: Visibility Status Query

**User Story:** As an operator, I want to filter workflow executions by pause state in list queries, so that I can find all paused workflows across a namespace.

#### Acceptance Criteria

1. WHEN the Kernel emits a `ProjectionOp::UpsertExecution` with `status == ExecutionStatus::Paused`, THE Projection layer SHALL make the standard status field queryable as `ExecutionStatus = "Paused"`.
2. WHEN the Kernel emits a `ProjectionOp::UpsertExecution` with `status == ExecutionStatus::Running` during unpause, THE Projection layer SHALL make the standard status field queryable as `ExecutionStatus = "Running"`.
3. THE Visibility filter parser SHALL accept `ExecutionStatus = "Paused"` and map it to `ExecutionStatus::Paused`.
4. THE Visibility layer SHALL support filtering by `ExecutionStatus = "Paused"` in `ListWorkflowExecutions` and `CountWorkflowExecutions` queries.

### Requirement 7: Paused Workflow Behavior Invariants

**User Story:** As a workflow developer, I want in-flight activities, timers, and child workflows to continue executing while the parent is paused, so that pause only affects new WFT scheduling without disrupting already-dispatched work.

#### Acceptance Criteria

1. WHILE a workflow is in `ExecutionStatus::Paused`, no kernel command other than a successful `UnpauseWorkflow` SHALL schedule a workflow task. `UnpauseWorkflow` SHALL schedule a WFT only after it has transitioned status to `ExecutionStatus::Running`, so the centralized WFT-scheduling gate naturally permits it.
2. WHILE paused, commands that would normally wake workflow code SHALL record their state changes and history events but SHALL NOT enqueue a WFT. This includes signals, cancel requests, activity resolutions, timer firings, child workflow resolutions, external signal/cancel resolutions, Nexus terminal resolutions, query-task scheduling, and any future WFT wakeup path.
3. THE Kernel SHALL enforce paused WFT suppression at the `TransitionBuilder::schedule_workflow_task()` chokepoint, or equivalent single WFT-scheduling helper, so callers cannot accidentally bypass the invariant.
4. THE only intentional exemption SHALL be successful `apply_unpause_workflow`, which changes status back to `ExecutionStatus::Running` before scheduling the WFT that lets workflow code observe accumulated changes.
5. WHILE the Kernel WorkflowState has `status == ExecutionStatus::Paused`, THE Runtime SHALL continue to dispatch already-scheduled activity tasks, honor timer expirations, and process child workflow lifecycle events through the standard paths.
6. WHILE paused, WHEN a `QueryWorkflow` request is received, THE Runtime SHALL return a query rejection carrying `ExecutionStatus::Paused`, and THE Edge SHALL translate it to a `QueryRejected` response with status `WORKFLOW_EXECUTION_STATUS_PAUSED` (proto enum value 8).
