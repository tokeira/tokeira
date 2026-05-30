# Implementation Plan: Kernel Pause Workflow

## Overview

Close the gaps in workflow pause/unpause behavior across the Tokeira stack,
matching verified Temporal v1.31.0 semantics. Pause is a workflow-task
scheduling gate: server-side events continue to be recorded, signals are written
to history normally, queries are rejected, and unpause creates the WFT that lets
workflow code observe accumulated events.

## Tasks

- [x] 1. Kernel: pause/unpause semantics and WFT suppression
  - [x] 1.1 Preserve normal signal history while paused
    - In `apply_signal`, emit `WorkflowExecutionSignaled` for paused workflows using the same event fields as the normal signal path.
    - Preserve the existing `SignalRequest` fields emitted by the normal running signal path: `signal_name`, `input`, `identity`, and `request_id`.
    - Do not add new signal fields here. The paused path uses the same event construction as the running signal path.
    - Record the usual `RequestDedupeOp`.
    - Do not schedule a WFT while `state.status == ExecutionStatus::Paused`.
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 1.2 Apply paused signal behavior to existing-run SignalWithStart
    - When `SignalWithStart` targets an existing paused run, do not take the start branch.
    - Record the signal event in history and suppress WFT scheduling using the standalone signal path.
    - _Requirements: 1.4_

  - [x] 1.3 Keep request-id-gated pause idempotency
    - Keep `Reject::AlreadyPaused`.
    - If already paused and request ID matches `PauseInfo.request_id`, return a no-op success transition.
    - If already paused and request ID differs, return `Reject::AlreadyPaused`.
    - _Requirements: 2.1, 2.2, 2.5_

  - [x] 1.4 Keep unpause precondition behavior
    - Keep `Reject::NotPaused`.
    - If workflow is not paused, return `Reject::NotPaused`.
    - _Requirements: 2.3_

  - [x] 1.5 Ensure unpause schedules a workflow task
    - On successful unpause, emit `WorkflowExecutionUnpaused`, set status to `Running`, clear `pause_info`, emit a running-status projection update, and schedule one WFT when none is pending.
    - _Requirements: 1.5, 4.3, 6.3, 7.4_

  - [x] 1.6 Centralize WFT suppression while paused
    - Make `TransitionBuilder::schedule_workflow_task()` (or the single WFT-scheduling helper) a no-op when `state.status == ExecutionStatus::Paused`.
    - Verify every command path that enqueues a WFT uses this helper or carries an equivalent paused-state guard.
    - Audit direct `DispatchOp::EnqueueWorkflowTask` pushes and either route them through the helper or keep an explicit paused-state guard.
    - Document that `apply_unpause_workflow` is the only intentional exemption because it sets status to `Running` before scheduling.
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [x] 1.7 Remove signal queueing implementation references
    - Do not add pause-specific signal queueing state.
    - Ensure tests assert signals are history events immediately, not queued state.
    - _Requirements: 1.1, 1.2_

  - [x] 1.8 Property test: Signal While Paused Records History Without WFT
    - Generate paused `WorkflowState` values and valid `SignalRequest` values.
    - Assert one `WorkflowExecutionSignaled` history event, metadata fidelity, one dedupe op, and zero `EnqueueWorkflowTask` dispatch ops.
    - **Validates: Requirements 1.1, 1.2, 1.3**

  - [x] 1.9 Property test: SignalWithStart Existing Paused Run
    - Generate existing paused runs and valid signal-with-start requests.
    - Assert the existing run receives a signal history event and no WFT dispatch.
    - **Validates: Requirement 1.4**

  - [x] 1.10 Property test: Pause Idempotency Is Request-ID-Gated
    - Generate paused states with stored pause request IDs.
    - Assert same request ID is no-op success and different request ID returns `Reject::AlreadyPaused`.
    - **Validates: Requirements 2.1, 2.2**

  - [x] 1.11 Property test: Unpause Requires Paused State
    - Generate non-paused open states and arbitrary `UnpauseWorkflowRequest` values.
    - Assert `Reject::NotPaused`.
    - **Validates: Requirement 2.3**

  - [x] 1.12 Property test: WFT suppression across all wakeup paths while paused
    - Generate paused states and any command variant that would normally schedule a WFT, excluding `UnpauseWorkflow`.
    - Cover the full command enum, including signal, cancel, activity resolution, timer due, child start/resolution, external signal/cancel resolution, Nexus terminal resolution, query-task scheduling, workflow-task failure retry, and workflow-command follow-up scheduling.
    - The property may be implemented as grouped generators, such as resolution commands, external signal/cancel commands, Nexus terminal commands, lifecycle commands, and workflow-task follow-up commands. Each test group must list the covered command variants explicitly so reviewers can confirm completeness.
    - For valid commands, assert state/history effects are recorded as appropriate and no `DispatchOp::EnqueueWorkflowTask` is emitted.
    - **Validates: Requirements 7.1, 7.2, 7.3**

- [x] 2. Checkpoint - Kernel changes complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Edge: gRPC handlers, query rejection, and describe enrichment
  - [x] 3.1 Add `pause_workflow` and `unpause_workflow` to the runtime adapter
    - In `crates/tokeira-edge/src/grpc/runtime_adapter.rs`, add `pause_workflow` and `unpause_workflow` methods following the same pattern as `signal_workflow` / `terminate_workflow`.
    - Map `Reject::AlreadyPaused` and `Reject::NotPaused` to `FAILED_PRECONDITION`.
    - _Requirements: 2.4, 4.1, 4.2_

  - [x] 3.2 Add edge DTOs and `to_internal` translation functions
    - Add pause/unpause edge request/response DTOs if missing.
    - In `crates/tokeira-edge/src/translate/to_internal.rs`, add `pause_request` and `unpause_request` functions that translate edge request types into kernel `PauseWorkflowRequest` / `UnpauseWorkflowRequest`.
    - Preserve namespace, workflow ID, run ID when present, identity, reason, and request ID.
    - _Requirements: 3.1, 3.2_

  - [x] 3.3 Implement `pause_workflow_execution` inner service method
    - In `crates/tokeira-edge/src/workflow_service.rs`, add `pause_workflow_execution` following the `signal_workflow_execution` pattern.
    - Validate namespace/workflow ID, route locally, translate, call runtime adapter.
    - Return `INVALID_ARGUMENT` for missing namespace or workflow ID.
    - _Requirements: 3.1, 3.3_

  - [x] 3.4 Implement `unpause_workflow_execution` inner service method
    - In `crates/tokeira-edge/src/workflow_service.rs`, add `unpause_workflow_execution` following the same pattern.
    - Return `INVALID_ARGUMENT` for missing namespace or workflow ID.
    - _Requirements: 3.2, 3.3_

  - [x] 3.5 Replace placeholder unary stubs with real gRPC handlers
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, replace the current placeholder implementations for `pause_workflow_execution` and `unpause_workflow_execution`.
    - Add proto translation functions in `crates/tokeira-edge/src/grpc/translate.rs` for pause/unpause request/response conversion.
    - _Requirements: 3.4_

  - [x] 3.6 Report workflow pause namespace capability
    - Set `namespace_info.capabilities.workflow_pause = true` in namespace description translation once pause/unpause is implemented.
    - Do not add a `workflow_pause` field to `GetSystemInfoResponse.Capabilities`; the v1.31 proto does not expose one there.
    - _Requirements: 3.5_

  - [x] 3.7 Enrich `DescribeWorkflowExecution` with pause info
    - Map `ExecutionStatus::Paused` to proto enum `WORKFLOW_EXECUTION_STATUS_PAUSED` (value 8).
    - Add `pause_info` to `WorkflowExecutionDescription` or equivalent edge DTO if needed.
    - Populate `workflow_execution_info.pause_info` with `identity`, `paused_time`, and `reason`.
    - Omit `workflow_execution_info.pause_info` for non-paused workflows.
    - Do not expose request ID in proto pause info.
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 3.8 Translate runtime query rejections to proto
    - In `from_internal::query_response` and `grpc::translate::query_response_to_proto`, map `QueryResult::Rejected { status }` to `QueryWorkflowResponse.query_rejected`.
    - Ensure `ExecutionStatus::Paused` maps to `WORKFLOW_EXECUTION_STATUS_PAUSED` (value 8).
    - The edge must not load workflow state to infer pause status; it only translates the runtime result.
    - _Requirements: 7.6_

  - [x] 3.9 Unit tests for edge pause/unpause handlers and pause surfaces
    - Test proto → edge request translation round trip.
    - Test `INVALID_ARGUMENT` on missing namespace/workflow ID.
    - Test `FAILED_PRECONDITION` mappings for already-paused / not-paused errors.
    - Test namespace capabilities include `workflow_pause: true`.
    - Test DescribeWorkflowExecution status value 8 and nested `pause_info`.
    - Test QueryWorkflow returns query rejection with status Paused.
    - _Requirements: 2.4, 3.1, 3.2, 3.3, 3.5, 5.1, 5.2, 5.3, 7.6_

- [x] 4. Runtime: Adapter methods
  - [x] 4.1 Add `pause_workflow` method to `TokeiraRuntime`
    - Add `pause_workflow` that resolves the execution, submits `Command::PauseWorkflow`, and returns the commit result.
    - Follow the same pattern as `signal_workflow` / `terminate_workflow`.
    - _Requirements: 4.1_

  - [x] 4.2 Add `unpause_workflow` method to `TokeiraRuntime`
    - Add `unpause_workflow` that resolves the execution, submits `Command::UnpauseWorkflow`, and returns the commit result.
    - The existing post-commit path routes unpause WFT dispatch ops through the broker.
    - _Requirements: 4.2, 4.3_

  - [x] 4.3 Add runtime paused-query rejection
    - Extend the runtime query result type with `QueryResult::Rejected { status: ExecutionStatus }`.
    - In `TokeiraRuntime::query_workflow`, after loading the run and before publishing a query task or scheduling a query WFT, return `QueryResult::Rejected { status: ExecutionStatus::Paused }` when the run is paused.
    - Ensure paused queries do not publish to the broker and do not submit `ScheduleQueryTask`.
    - _Requirements: 7.6_

  - [x] 4.4 Unit tests for runtime pause/unpause and query rejection methods
    - Verify `pause_workflow` routes through standard submit path.
    - Verify `unpause_workflow` routes through standard submit path.
    - Verify dispatch ops from unpause transition flow through broker.
    - Verify `query_workflow` returns `QueryResult::Rejected { status: Paused }` for paused workflows without broker publication.
    - _Requirements: 4.1, 4.2, 4.3, 7.6_

- [x] 5. Projection: visibility status filtering
  - [x] 5.1 Add `"Paused"` to the visibility filter status parser
    - In `crates/tokeira-projection/src/filter.rs`, add `"Paused" => Ok(ExecutionStatus::Paused)` to `parse_status`.
    - Ensure status labels in query compiler and rollup map `ExecutionStatus::Paused` to `"Paused"`.
    - _Requirements: 6.1, 6.4_

  - [x] 5.2 Verify pause/unpause projection status updates
    - On pause projection, verify `ProjectionOp::UpsertExecution { status: ExecutionStatus::Paused, .. }` makes the run match `ExecutionStatus = "Paused"`.
    - On unpause projection, verify `ProjectionOp::UpsertExecution { status: ExecutionStatus::Running, .. }` makes the run stop matching `ExecutionStatus = "Paused"` and match `ExecutionStatus = "Running"`.
    - _Requirements: 6.1, 6.2, 6.4_

  - [x] 5.3 Unit tests for visibility filters
    - Test `ExecutionStatus = "Paused"` parsing and evaluation.
    - Test a paused workflow is returned by `ListWorkflowExecutions` / `CountWorkflowExecutions` for `ExecutionStatus = "Paused"`.
    - Test the same workflow no longer matches `ExecutionStatus = "Paused"` and matches `ExecutionStatus = "Running"` after unpause.
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 6. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property and unit tests in this plan are required because they validate externally visible Temporal compatibility semantics.
- Each task references specific requirements for traceability.
- Checkpoints ensure incremental validation.
- The kernel stays pure — all new kernel logic is deterministic state manipulation with no I/O.
- Rust edition 2024, `thiserror` for errors, no `.unwrap()` outside tests.
- `proptest` is the PBT framework used in this workspace.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "1.4"] },
    { "id": 1, "tasks": ["1.5", "1.6", "1.7"] },
    { "id": 2, "tasks": ["1.8", "1.9", "1.10", "1.11", "1.12"] },
    { "id": 3, "tasks": ["2", "3.1", "3.2", "4.1", "4.2", "4.3", "5.1", "5.2"] },
    { "id": 4, "tasks": ["3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "4.4", "5.3"] },
    { "id": 5, "tasks": ["3.9", "6"] }
  ]
}
```
