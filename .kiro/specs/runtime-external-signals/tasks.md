# Implementation Plan: External Signal and Cancel Delivery

## Overview

Wire the runtime's `DispatchPublisher` to handle `DispatchOp::SignalExternalWorkflow` and `DispatchOp::RequestCancelExternalWorkflow`, replacing the current stub logging with working implementations. This involves extending the dispatch op variants with originator identity fields, updating the kernel's `apply_workflow_command` to populate them, and implementing the two new handler methods and match arms in `RuntimeDispatchPublisher`.

## Tasks

- [x] 1. Extend kernel WorkflowCommand and DispatchOp variants
  - [x] 1.0 Add `target_namespace_id: NamespaceId` to `WorkflowCommand::SignalExternalWorkflowExecution` and `WorkflowCommand::RequestCancelExternalWorkflowExecution` in `crates/tokeira-kernel/src/command.rs`
    - _Requirements: 3.1, 3.2_

  - [x] 1.1 Extend `DispatchOp::SignalExternalWorkflow` in `crates/tokeira-kernel/src/transition.rs`
    - Add `originator_run_key: RunKey`, `namespace_id: NamespaceId`, `initiated_event_id: i64` fields
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 1.2 Extend `DispatchOp::RequestCancelExternalWorkflow` in `crates/tokeira-kernel/src/transition.rs`
    - Add `originator_run_key: RunKey`, `originator_namespace_id: NamespaceId`, `originator_workflow_id: WorkflowId`, `originator_run_id: RunId`, `namespace_id: NamespaceId`, `initiated_event_id: i64`, `reason: String` fields
    - _Requirements: 4.4, 4.5, 4.6_

  - [x] 1.3 Fix all compile errors from the extended variants
    - Update all existing match arms and construction sites across the codebase that reference `WorkflowCommand::SignalExternalWorkflowExecution`, `WorkflowCommand::RequestCancelExternalWorkflowExecution`, `DispatchOp::SignalExternalWorkflow`, and `DispatchOp::RequestCancelExternalWorkflow` to include the new fields
    - For `WorkflowCommand` construction in tests, set `target_namespace_id` to the originator's namespace (same-namespace default)
    - _Requirements: 3.1, 3.2, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 2. Update kernel `apply_workflow_command` to populate new dispatch op fields
  - [x] 2.1 Update `SignalExternalWorkflowExecution` arm in `crates/tokeira-kernel/src/kernel.rs`
    - Populate `originator_run_key` from `builder.state.run_key`
    - Populate `namespace_id` from the workflow command's `target_namespace_id`
    - Populate `initiated_event_id` from the emitted event ID
    - _Requirements: 3.3, 4.7_

  - [x] 2.2 Update `RequestCancelExternalWorkflowExecution` arm in `crates/tokeira-kernel/src/kernel.rs`
    - Populate `originator_run_key` from `builder.state.run_key`
    - Populate `originator_namespace_id` from `builder.state.namespace_id`
    - Populate `originator_workflow_id` from `builder.state.workflow_id.clone()`
    - Populate `originator_run_id` from `builder.state.run_id`
    - Populate `namespace_id` from the workflow command's `target_namespace_id`
    - Populate `initiated_event_id` from the emitted event ID
    - Populate `reason` with a descriptive string including the originator workflow ID
    - _Requirements: 3.3, 4.8_

  - [x] 2.3 Write property test for kernel signal dispatch op field population
    - **Property 5: Kernel populates signal dispatch op fields from workflow state**
    - Generate random `WorkflowState` values (varying `run_key`, `namespace_id`, `last_event_id`) and random `SignalExternalWorkflowExecution` commands
    - Apply via `BasicKernel` and verify emitted `DispatchOp::SignalExternalWorkflow` carries correct `originator_run_key`, `namespace_id`, and `initiated_event_id`
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.7**

  - [x] 2.4 Write property test for kernel cancel dispatch op field population
    - **Property 6: Kernel populates cancel dispatch op fields from workflow state**
    - Generate random `WorkflowState` values (varying `run_key`, `namespace_id`, `workflow_id`, `run_id`) and random `RequestCancelExternalWorkflowExecution` commands
    - Apply via `BasicKernel` and verify emitted `DispatchOp::RequestCancelExternalWorkflow` carries correct `originator_run_key`, `originator_namespace_id`, `originator_workflow_id`, `originator_run_id`, `namespace_id`, and `initiated_event_id`
    - **Validates: Requirements 4.4, 4.5, 4.6, 4.8**

- [x] 3. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement `handle_signal_external_workflow` on `RuntimeDispatchPublisher`
  - [x] 4.1 Add `handle_signal_external_workflow` method in `crates/tokeira-runtime/src/runtime.rs`
    - Resolve target execution via `resolve_execution` using `namespace_id`, `target_workflow_id`, `target_run_id`
    - Submit `Command::Signal` to target run with `signal_name`, `input`, and a `RequestContext` with unique `request_id` and `caller_identity` of `"runtime-external-signal-orchestrator"`
    - On success (`Applied` or `Duplicate`), build `ExternalSignalResult::Signaled`
    - On any failure (not found, conflict, transient error), build `ExternalSignalResult::Failed { cause }`
    - Always deliver `Command::ExternalSignalResolved` to originator run with correct `initiated_event_id`
    - Log at warn level if resolution delivery to originator fails
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 5.1, 6.1, 6.2, 6.4, 7.1, 7.2, 7.3_

  - [x] 4.2 Write property test for signal command construction
    - **Property 1: Signal command construction**
    - Generate random `signal_name`, `input`, `originator_run_key`, `namespace_id`, `initiated_event_id`
    - Mock repo returns a valid `RunKey` for the target; mock lane captures the `Command::Signal`
    - Verify `signal_name`, `input`, `request.request_id` (non-empty), and `request.caller_identity` match expectations
    - **Validates: Requirements 1.2, 7.1, 7.2, 7.3**

  - [x] 4.3 Write property test for signal resolution always delivered
    - **Property 3: Signal resolution always delivered with correct result**
    - Generate random dispatch ops and a random outcome selector (success, not-found, closed, transient error)
    - Configure mock repo and lanes per the outcome
    - Verify resolution is always delivered to originator with correct `initiated_event_id` and appropriate `ExternalSignalResult` variant
    - **Validates: Requirements 1.3, 1.4, 1.5, 1.6, 5.1, 6.1, 6.2**

- [x] 5. Implement `handle_cancel_external_workflow` on `RuntimeDispatchPublisher`
  - [x] 5.1 Add `handle_cancel_external_workflow` method in `crates/tokeira-runtime/src/runtime.rs`
    - Resolve target execution via `resolve_execution` using `namespace_id`, `target_workflow_id`, `target_run_id`
    - Submit `Command::Cancel` to target run with `reason`, `external_initiator` populated from `originator_namespace_id`, `originator_workflow_id`, `originator_run_id`, and a `RequestContext` with unique `request_id` and `caller_identity` of `"runtime-external-cancel-orchestrator"`
    - On success (`Applied` or `Duplicate`), build `ExternalCancelResult::CancelRequested`
    - On any failure (not found, conflict, transient error), build `ExternalCancelResult::Failed { cause }`
    - Always deliver `Command::ExternalCancelResolved` to originator run with correct `initiated_event_id`
    - Log at warn level if resolution delivery to originator fails
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 5.2, 6.1, 6.2, 6.4, 8.1, 8.2, 8.3_

  - [x] 5.2 Write property test for cancel command construction
    - **Property 2: Cancel command construction**
    - Generate random `originator_namespace_id`, `originator_workflow_id`, `originator_run_id`, `reason`, and target identity
    - Mock repo returns a valid `RunKey`; mock lane captures the `Command::Cancel`
    - Verify `reason`, `request.request_id` (non-empty), `request.caller_identity`, and `external_initiator` field mapping
    - **Validates: Requirements 2.2, 8.1, 8.2, 8.3**

  - [x] 5.3 Write property test for cancel resolution always delivered
    - **Property 4: Cancel resolution always delivered with correct result**
    - Generate random dispatch ops and a random outcome selector (success, not-found, closed, transient error)
    - Configure mock repo and lanes per the outcome
    - Verify resolution is always delivered to originator with correct `initiated_event_id` and appropriate `ExternalCancelResult` variant
    - **Validates: Requirements 2.3, 2.4, 2.5, 2.6, 5.2, 6.1, 6.2**

- [x] 6. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Wire match arms in `RuntimeDispatchPublisher::publish` and target resolution
  - [x] 7.1 Add `DispatchOp::SignalExternalWorkflow` match arm in `publish()` in `crates/tokeira-runtime/src/runtime.rs`
    - Clone fields, `tokio::spawn` a task that calls `handle_signal_external_workflow`
    - Place before the `other =>` catch-all arm
    - _Requirements: 1.1, 6.3_

  - [x] 7.2 Add `DispatchOp::RequestCancelExternalWorkflow` match arm in `publish()` in `crates/tokeira-runtime/src/runtime.rs`
    - Clone fields, `tokio::spawn` a task that calls `handle_cancel_external_workflow`
    - Place before the `other =>` catch-all arm
    - _Requirements: 2.1, 6.3_

  - [x] 7.3 Write property test for target resolution namespace usage
    - **Property 7: Target resolution uses dispatch op namespace**
    - Generate random `namespace_id`, `target_workflow_id`, `target_run_id` values
    - Mock repo captures the `ExecutionRef` passed to `resolve_execution`
    - Verify the `ExecutionRef` fields match the dispatch op values
    - **Validates: Requirements 1.1, 2.1, 3.1, 3.2**

- [x] 8. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property test tasks are required (not optional) per project convention
- Each property test references a specific correctness property from the design document
- The implementation follows the same async `tokio::spawn` pattern used for child workflow orchestration (Feature 6)
- Cross-namespace support defaults to the originator's namespace until `WorkflowCommand` gains an optional `target_namespace_id` field
