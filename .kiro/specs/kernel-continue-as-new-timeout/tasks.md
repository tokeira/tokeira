# Implementation Plan: ContinueAsNew and Workflow-Level Timeout (Feature 4)

## Overview

All changes are additive to the existing kernel and types crates. Types first (tokeira-types then kernel types), kernel logic second, downstream exhaustive match fixes, workspace compile checkpoint, then tests. `WorkflowExecutionFailed` gains `retry_state` and `attempt` fields — this is a breaking change requiring all construction sites to be updated.

## Tasks

- [x] 1. Add new types and enum variants (tokeira-types)
  - [x] 1.1 Add ContinuedAsNew and TimedOut variants to ExecutionStatus in tokeira-types/src/execution.rs
    - Add `ContinuedAsNew` and `TimedOut` to the `ExecutionStatus` enum
    - Update `is_open` to return `false` for both new variants
    - _Requirements: 1.1_

- [x] 2. Add new types and enum variants (kernel)
  - [x] 2.1 Add WorkflowTimeoutType and RetryState enums to command.rs
    - Add `WorkflowTimeoutType` enum with `ExecutionTimeout`, `RunTimeout` variants, deriving `Clone, Debug, PartialEq`
    - Add `RetryState` enum with `InProgress`, `NonRetryableFailure`, `Timeout`, `MaximumAttemptsReached`, `RetryPolicyNotSet`, `InternalServerError`, `CancelRequested` variants, deriving `Clone, Debug, PartialEq`
    - _Requirements: 1.2, 1.3_

  - [x] 2.2 Add WorkflowExecutionTimedOutRequest struct and Command variant to command.rs
    - Add `WorkflowExecutionTimedOutRequest` struct with `timeout_type: WorkflowTimeoutType`, `retry_state: RetryState`, `now: OffsetDateTime`, deriving `Clone, Debug, PartialEq`
    - Add `WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest)` to the `Command` enum
    - No `RequestContext` field
    - _Requirements: 1.5_

  - [x] 2.3 Add ContinueAsNew variant to WorkflowCommand enum in command.rs
    - Add `ContinueAsNew { new_run_id, workflow_type, task_queue, input, memo, search_attributes, workflow_execution_timeout, workflow_run_timeout, workflow_task_timeout }` variant
    - _Requirements: 1.4_

  - [x] 2.4 Add new HistoryEventKind variants and update WorkflowExecutionFailed in event.rs
    - Add `WorkflowExecutionContinuedAsNew { new_run_id, workflow_type, task_queue, input, memo, search_attributes, workflow_execution_timeout, workflow_run_timeout, workflow_task_timeout }`
    - Add `WorkflowExecutionTimedOut { timeout_type: WorkflowTimeoutType, retry_state: RetryState }` — import `WorkflowTimeoutType` and `RetryState` from `crate::command`
    - Add `retry_state: RetryState` and `attempt: u32` fields to existing `WorkflowExecutionFailed` variant
    - _Requirements: 1.6, 1.7_

- [x] 3. Implement kernel logic
  - [x] 3.1 Add ContinueAsNew match arm to apply_workflow_command in kernel.rs
    - Emit `WorkflowExecutionContinuedAsNew` event carrying all fields from the command
    - Call `builder.close(ExecutionStatus::ContinuedAsNew)`
    - Return `Ok(true)` — run is closed, subsequent commands rejected with `CommandsAfterClose`
    - _Requirements: 2.1, 5.2, 7.3, 7.7_

  - [x] 3.2 Update FailWorkflow match arm in apply_workflow_command to emit retry metadata
    - Compute `retry_state`: if `builder.state.retry_policy.is_some()` → `RetryState::InProgress`, else → `RetryState::RetryPolicyNotSet`
    - Read `attempt` from `builder.state.attempt`
    - Pass `retry_state` and `attempt` to the `WorkflowExecutionFailed` event emission
    - _Requirements: 4.1, 7.8_

  - [x] 3.3 Add apply_workflow_execution_timed_out method and Command match arm in kernel.rs
    - Add `WorkflowExecutionTimedOut(req) => self.apply_workflow_execution_timed_out(loaded, req)` match arm in `BasicKernel::apply`
    - Implement `apply_workflow_execution_timed_out`: `expect_open` → `TransitionBuilder` → emit `WorkflowExecutionTimedOut` event (timeout_type, retry_state from request) → `close(ExecutionStatus::TimedOut)` → `std::mem::take` activities → `ActivityOp::Delete` each → `std::mem::take` timers → `TimerOp::Delete` each → `finish()`
    - No `RequestDedupeOp`, no `DispatchOp`
    - _Requirements: 3.1, 3.2, 3.3, 5.1, 7.4, 7.5, 7.6_

- [x] 4. Update downstream call sites
  - [x] 4.1 Update tokeira-edge translate.rs exhaustive matches
    - Add `ContinueAsNew` to `workflow_command_to_proto` non-proto arm (alongside CancelWorkflow, etc.)
    - Add `ContinuedAsNew` and `TimedOut` arms to `execution_status_to_proto` (map to `ContinuedAsNew` and `TimedOut` proto variants)
    - _Requirements: 6.1.3, 6.1.4_

  - [x] 4.2 Update tokeira-edge grpc_properties.rs exhaustive matches and generators
    - Add `ContinuedAsNew` and `TimedOut` to `execution_status_to_proto` match
    - Add `ContinuedAsNew` and `TimedOut` to `arb_execution_status()` prop_oneof
    - Add `ContinueAsNew` to `arb_workflow_command()` prop_oneof generator
    - Add `WorkflowCommand::ContinueAsNew { .. }` to the non-roundtrippable arm in `property_workflow_command_roundtrip`
    - _Requirements: 6.1.3, 6.1.4_

  - [x] 4.3 Update all WorkflowExecutionFailed construction sites across the workspace
    - Update golden_tests.rs: any `WorkflowExecutionFailed { .. }` match or construction to include `retry_state` and `attempt`
    - Update property_tests.rs: any `WorkflowExecutionFailed { .. }` match to include the new fields
    - _Requirements: 6.1.5_

- [x] 5. Workspace compile checkpoint
  - Run `cargo check --workspace`. Ensure no errors. Fix any additional compile failures discovered beyond the known call sites in task 4. Ask the user if questions arise.
  - _Requirements: 6.1.6_

- [x] 6. Add property-based tests
  - [x] 6.1 Add new generators to property_tests.rs
    - Add `arb_workflow_timeout_type()` — generates `WorkflowTimeoutType::ExecutionTimeout` or `RunTimeout`
    - Add `arb_retry_state()` — generates one of the seven `RetryState` variants
    - Add `arb_continue_as_new_command()` — generates random `ContinueAsNew` workflow command with all fields
    - Add `arb_workflow_execution_timed_out_request(now)` — generates random `WorkflowExecutionTimedOutRequest`
    - Import new types: `WorkflowTimeoutType`, `RetryState`, `WorkflowExecutionTimedOutRequest`
    - _Requirements: 8.1–8.10_

  - [x] 6.2 Extend arb_valid_pair() with 2 new arms
    - Arm 1: `WorkflowExecutionTimedOut` — open state with 0–3 activities, 0–3 timers, optional pending WFT and sticky
    - Arm 2: Add `arb_continue_as_new_command()` to the existing WFT completed `prop_oneof!`
    - This automatically extends existing properties 4, 5, 7, 8, 9, 10 to cover new commands (Design Property 9)
    - _Requirements: 7.1, 7.2_

  - [x] 6.3 Write property_25_continue_as_new_closes_with_terminal_invariants
    - `proptest! { }` block: for any valid open state with started WFT and ContinueAsNew command, status is ContinuedAsNew, closed_at is Some, pending_workflow_task is None, sticky is None, dispatch_ops empty
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 1: ContinueAsNew closes with full terminal state invariants`
    - **Design Property 1**
    - _Requirements: 2.1.2, 2.1.5, 7.3, 8.1_

  - [x] 6.4 Write property_26_continue_as_new_field_pass_through
    - `proptest! { }` block: for any valid ContinueAsNew command, the emitted WorkflowExecutionContinuedAsNew event carries identical values for all 9 fields
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 2: ContinueAsNew field pass-through`
    - **Design Property 2**
    - _Requirements: 2.1.1, 7.7, 8.2_

  - [x] 6.5 Write property_27_continue_as_new_is_terminal
    - `proptest! { }` block: ContinueAsNew followed by any additional workflow command → CommandsAfterClose rejection
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 3: ContinueAsNew is terminal (CommandsAfterClose)`
    - **Design Property 3**
    - _Requirements: 2.1.3, 8.9_

  - [x] 6.6 Write property_28_timeout_closes_with_terminal_invariants
    - `proptest! { }` block: for any valid open state and WorkflowExecutionTimedOutRequest, status is TimedOut, closed_at is Some, pending_workflow_task is None, sticky is None, activities empty, timers empty, dispatch_ops empty
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 4: WorkflowExecutionTimedOut closes with full terminal state invariants`
    - **Design Property 4**
    - _Requirements: 3.1.2, 3.1.3, 3.1.4, 7.4, 8.4_

  - [x] 6.7 Write property_29_timeout_entity_cleanup
    - `proptest! { }` block: activity_ops count equals input activities count, timer_ops count equals input timers count, all delete ops reference IDs from input state, next_state maps empty
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 5: WorkflowExecutionTimedOut entity cleanup count and consistency`
    - **Design Property 5**
    - _Requirements: 3.2, 7.5, 8.5_

  - [x] 6.8 Write property_30_timeout_event_field_pass_through
    - `proptest! { }` block: emitted WorkflowExecutionTimedOut event's timeout_type and retry_state equal the request's values
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 6: WorkflowExecutionTimedOut event field pass-through`
    - **Design Property 6**
    - _Requirements: 3.1.1, 4.2, 8.10_

  - [x] 6.9 Write property_31_timeout_no_request_dedupe
    - `proptest! { }` block: for any valid WorkflowExecutionTimedOut transition, request_dedupe_ops is empty
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 7: WorkflowExecutionTimedOut emits no request dedupe`
    - **Design Property 7**
    - _Requirements: 3.1.5, 7.6, 8.7_

  - [x] 6.10 Write property_32_fail_workflow_retry_metadata
    - `proptest! { }` block: if retry_policy present → retry_state is InProgress; if absent → RetryPolicyNotSet; attempt always equals state's attempt count
    - Tag: `// Feature: kernel-continue-as-new-timeout, Property 8: FailWorkflow retry metadata consistency`
    - **Design Property 8**
    - _Requirements: 4.1, 7.8, 8.8_

- [x] 7. Add golden tests
  - [x] 7.1 Write ContinueAsNew happy path golden tests in golden_tests.rs
    - `continue_as_new_closes_run` — ContinueAsNew within WFT completed, verify status ContinuedAsNew, event fields, no dispatch
    - `continue_as_new_then_another_command` — ContinueAsNew followed by RequestNewWorkflowTask → CommandsAfterClose
    - _Requirements: 2.1, 7.3, 7.7_

  - [x] 7.2 Write WorkflowExecutionTimedOut happy path golden tests in golden_tests.rs
    - `workflow_execution_timed_out_no_entities` — Timeout on open run, no activities/timers
    - `workflow_execution_timed_out_with_entities` — Timeout with 2 activities + 1 timer, verify cleanup ops
    - `workflow_execution_timed_out_with_pending_wft` — Timeout clears pending WFT and sticky
    - _Requirements: 3.1, 3.2, 7.4, 7.5_

  - [x] 7.3 Write WorkflowExecutionTimedOut rejection golden tests in golden_tests.rs
    - `reject_timeout_absent_run` — MissingRun
    - `reject_timeout_closed_run` — RunClosed
    - _Requirements: 3.3_

  - [x] 7.4 Write FailWorkflow retry metadata golden tests in golden_tests.rs
    - `fail_workflow_with_retry_policy` — retry_state=InProgress, attempt from state
    - `fail_workflow_without_retry_policy` — retry_state=RetryPolicyNotSet, attempt from state
    - _Requirements: 4.1, 7.8_

- [x] 8. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All test tasks are required (not optional) per project convention
- Property tests use `proptest! { }` block style consistent with Features 1 and 3
- Golden tests are individual `#[test]` functions
- All tests extend existing files (property_tests.rs and golden_tests.rs), no new test files
- Property tests are numbered 25–32, continuing from Feature 3's numbering
- Each property test is tagged with its design property reference
- `WorkflowExecutionFailed` gaining `retry_state` and `attempt` is a breaking change — task 4.3 covers all construction sites
- Entity cleanup in `apply_workflow_execution_timed_out` uses `std::mem::take` pattern (same as Terminate in Feature 3)
