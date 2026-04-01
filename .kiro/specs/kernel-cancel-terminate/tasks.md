# Implementation Plan: Cancel and Terminate (Feature 3)

## Overview

All changes are additive to the existing kernel crate. New types, enum variants, and methods are added; no existing code paths change within the kernel. However, `WorkflowCommand` is matched exhaustively in downstream crates (notably `tokeira-edge` `translate.rs` and `grpc_properties.rs`), so those call sites will need wildcard or explicit arms for the new variants. Types first, kernel logic second, compile checkpoint, downstream fixes, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add ExternalWorkflowExecution, CancelRequest, and TerminateRequest structs to command.rs
    - Add `ExternalWorkflowExecution` struct with `namespace_id`, `workflow_id`, `run_id` fields, deriving `Clone, Debug, PartialEq`
    - Add `CancelRequest` struct with `reason`, `external_initiator`, `request`, `now` fields
    - Add `TerminateRequest` struct with `reason`, `details`, `identity`, `request`, `now` fields
    - _Requirements: 1.1, 1.1a, 1.2_

  - [x] 1.2 Add Cancel and Terminate variants to Command enum in command.rs
    - Add `Cancel(CancelRequest)` and `Terminate(TerminateRequest)` to the `Command` enum
    - _Requirements: 1.1.1, 1.2.1_

  - [x] 1.3 Add CancelWorkflow, RequestCancelActivity, and CancelTimer variants to WorkflowCommand enum in command.rs
    - Add `CancelWorkflow` unit variant
    - Add `RequestCancelActivity { activity_id: String }` variant
    - Add `CancelTimer { timer_id: String }` variant
    - _Requirements: 1.3, 1.4, 1.5_

  - [x] 1.4 Add new HistoryEventKind variants to event.rs
    - Add `WorkflowExecutionCancelRequested { reason, external_workflow_execution, request_id }` — requires importing `ExternalWorkflowExecution` from `crate::command`
    - Add `WorkflowExecutionTerminated { reason, details, identity }`
    - Add `WorkflowExecutionCanceled` unit variant
    - Add `ActivityTaskCancelRequested { activity_id }`
    - Add `TimerCanceled { timer_id }`
    - _Requirements: 1.6_

- [x] 2. Implement kernel logic
  - [x] 2.1 Add apply_cancel method to BasicKernel in kernel.rs
    - Follow the same pattern as `apply_signal`: `expect_open` → `TransitionBuilder` → push `RequestDedupeOp` → emit `WorkflowExecutionCancelRequested` → if no pending WFT, `schedule_workflow_task()` → `finish()`
    - Import `CancelRequest` in the use block
    - _Requirements: 2.1, 2.2, 8.1.1, 8.1.3_

  - [x] 2.2 Add apply_terminate method to BasicKernel in kernel.rs
    - `expect_open` → `TransitionBuilder` → push `RequestDedupeOp` → emit `WorkflowExecutionTerminated` → `close(ExecutionStatus::Terminated)` → for each activity push `ActivityOp::Delete` then `state.activities.clear()` → for each timer push `TimerOp::Delete` then `state.timers.clear()` → `finish()`
    - Import `TerminateRequest` in the use block
    - _Requirements: 3.1, 3.2, 3.3, 8.1.2, 8.1.4_

  - [x] 2.3 Add Cancel and Terminate match arms to BasicKernel::apply
    - `Command::Cancel(req) => self.apply_cancel(loaded, req)`
    - `Command::Terminate(req) => self.apply_terminate(loaded, req)`
    - _Requirements: 8.1.1, 8.1.2_

  - [x] 2.4 Add CancelWorkflow, RequestCancelActivity, and CancelTimer match arms to apply_workflow_command
    - `CancelWorkflow`: emit `WorkflowExecutionCanceled`, call `builder.close(ExecutionStatus::Cancelled)`, return `Ok(true)`
    - `RequestCancelActivity { activity_id }`: validate activity exists or reject `UnknownActivity`, emit `ActivityTaskCancelRequested`, return `Ok(false)`
    - `CancelTimer { timer_id }`: validate timer exists or reject `UnknownTimer`, emit `TimerCanceled`, remove timer from state, push `TimerOp::Delete`, return `Ok(false)`
    - _Requirements: 4.1, 5.1, 5.2, 6.1, 6.2, 7.1, 8.2_

- [x] 3. Checkpoint — compile check
  - Ensure the kernel crate compiles with `cargo check -p tokeira-kernel`. Ask the user if questions arise.

- [x] 3.5. Update downstream call sites and workspace compile check
  - [x] 3.5.1 Update `tokeira-edge` `translate.rs` exhaustive matches on `WorkflowCommand` to handle `CancelWorkflow`, `RequestCancelActivity`, and `CancelTimer` (add match arms or wildcard)
  - [x] 3.5.2 Update `tokeira-edge` `grpc_properties.rs` test generators to include the new `WorkflowCommand` variants
  - [x] 3.5.3 Ensure `cargo check --workspace` passes with no errors

- [x] 4. Add property-based tests
  - [x] 4.1 Add new generators to property_tests.rs
    - Add `arb_external_workflow_execution()` generator
    - Add `arb_cancel_request(now)` generator — random reason, optional ExternalWorkflowExecution, RequestContext
    - Add `arb_terminate_request(now)` generator — random reason, optional details, identity, RequestContext
    - Import new types: `CancelRequest`, `TerminateRequest`, `ExternalWorkflowExecution`
    - _Requirements: 10.1–10.11_

  - [x] 4.2 Extend arb_valid_pair() with 6 new arms
    - Arm 1: Cancel with no pending WFT (open state, optional activities/timers)
    - Arm 2: Cancel with pending WFT (open state with started WFT)
    - Arm 3: Terminate with entities (open state with 0–3 activities and 0–3 timers)
    - Arm 4: WorkflowTaskCompleted with CancelWorkflow — add to existing WFT completed `prop_oneof!`
    - Arm 5: WorkflowTaskCompleted with RequestCancelActivity — state with started WFT and open activity
    - Arm 6: WorkflowTaskCompleted with CancelTimer — state with started WFT and open timer
    - This automatically extends existing properties 4, 5, 7, 8, 9, 10 to cover new commands (Design Property 9)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 10.11_

  - [x] 4.3 Write property_17_cancel_event_field_pass_through
    - `proptest! { }` block: for any valid open state and CancelRequest, the emitted WorkflowExecutionCancelRequested event carries the request's reason, external_initiator, and request_id
    - Tag: `// Feature: kernel-cancel-terminate, Property 1: Cancel event field pass-through`
    - **Design Property 1**
    - _Requirements: 2.1.2_

  - [x] 4.4 Write property_18_cancel_does_not_close
    - `proptest! { }` block: for any valid open state and CancelRequest, next_state.status is Running, closed_at is None, projection_ops/activity_ops/timer_ops are empty
    - Tag: `// Feature: kernel-cancel-terminate, Property 2: Cancel does not close and has minimal side effects`
    - **Design Property 2**
    - _Requirements: 2.1.5, 2.1.6, 2.1.7, 2.1.8, 10.1.1_

  - [x] 4.5 Write property_19_cancel_wft_coalescing
    - `proptest! { }` block: two sub-cases — no pending WFT → schedules WFT + one EnqueueWorkflowTask; pending WFT → dispatch_ops empty, existing WFT preserved
    - Tag: `// Feature: kernel-cancel-terminate, Property 3: Cancel WFT coalescing`
    - **Design Property 3**
    - _Requirements: 2.1.3, 2.1.4, 9.3.1, 9.3.2, 9.3.3, 10.2.1, 10.2.2_

  - [x] 4.6 Write property_20_terminate_event_field_pass_through
    - `proptest! { }` block: for any valid open state and TerminateRequest, the emitted WorkflowExecutionTerminated event carries the request's reason, details, and identity
    - Tag: `// Feature: kernel-cancel-terminate, Property 4: Terminate event field pass-through`
    - **Design Property 4**
    - _Requirements: 3.1.2_

  - [x] 4.7 Write property_21_terminate_closes_with_terminal_invariants
    - `proptest! { }` block: status is Terminated, closed_at is Some, pending_workflow_task is None, sticky is None, activities empty, timers empty, dispatch_ops empty
    - Tag: `// Feature: kernel-cancel-terminate, Property 5: Terminate closes with full terminal state invariants`
    - **Design Property 5**
    - _Requirements: 3.1.3, 3.1.4, 3.1.5, 9.4.1–9.4.7, 10.4.1, 10.6.1_

  - [x] 4.8 Write property_22_terminate_entity_cleanup
    - `proptest! { }` block: activity_ops count equals input activities count, timer_ops count equals input timers count, all delete ops reference IDs from input state
    - Tag: `// Feature: kernel-cancel-terminate, Property 6: Terminate entity cleanup count and consistency`
    - **Design Property 6**
    - _Requirements: 3.2.1–3.2.4, 9.5.1–9.5.4, 10.5.1_

  - [x] 4.9 Write property_23_request_cancel_activity_preserves_activity
    - `proptest! { }` block: for any valid WFT completed with RequestCancelActivity for a valid activity, the activity remains in next_state.activities with same ActivityState, no ActivityOp::Delete emitted
    - Tag: `// Feature: kernel-cancel-terminate, Property 7: RequestCancelActivity preserves activity in state`
    - **Design Property 7**
    - _Requirements: 5.1.1–5.1.4, 9.7.1, 9.7.2, 10.9.1_

  - [x] 4.10 Write property_24_cancel_timer_removes_timer
    - `proptest! { }` block: for any valid WFT completed with CancelTimer for a valid timer, the timer is NOT in next_state.timers, timer_ops contains TimerOp::Delete for that timer_id
    - Tag: `// Feature: kernel-cancel-terminate, Property 8: CancelTimer removes timer and emits delete op`
    - **Design Property 8**
    - _Requirements: 6.1.1–6.1.4, 9.8.1, 9.8.2, 10.10.1_

- [x] 5. Add golden tests
  - [x] 5.1 Write cancel happy path golden tests in golden_tests.rs
    - `cancel_with_no_pending_wft` — Cancel on open run, no WFT → schedules WFT, assert exact transition
    - `cancel_with_pending_wft` — Cancel on open run, WFT pending → coalesces, no dispatch
    - `cancel_with_external_initiator` — Cancel with ExternalWorkflowExecution set, assert event fields
    - Import new types: `CancelRequest`, `ExternalWorkflowExecution`
    - _Requirements: 11.1, 11.2, 11.3_

  - [x] 5.2 Write cancel rejection golden tests in golden_tests.rs
    - `reject_cancel_absent_run` — MissingRun
    - `reject_cancel_closed_run` — RunClosed
    - _Requirements: 11.4_

  - [x] 5.3 Write terminate happy path golden tests in golden_tests.rs
    - `terminate_no_open_entities` — Terminate on open run, no entities, assert exact transition
    - `terminate_with_activities_and_timers` — Terminate with 2 activities + 1 timer, assert cleanup ops
    - `terminate_with_pending_wft` — Terminate clears pending WFT, assert no dispatch
    - Import new type: `TerminateRequest`
    - _Requirements: 11.5, 11.6, 11.7_

  - [x] 5.4 Write terminate rejection golden tests in golden_tests.rs
    - `reject_terminate_absent_run` — MissingRun
    - `reject_terminate_closed_run` — RunClosed
    - _Requirements: 11.8_

  - [x] 5.5 Write workflow command golden tests in golden_tests.rs
    - `cancel_workflow_command` — CancelWorkflow closes with Canceled
    - `cancel_workflow_then_another_command` — CommandsAfterClose rejection at correct index
    - `request_cancel_activity` — Activity preserved in state, no ActivityOp::Delete
    - `request_cancel_activity_unknown` — UnknownActivity rejection
    - `cancel_timer` — Timer removed, TimerOp::Delete emitted
    - `cancel_timer_unknown` — UnknownTimer rejection
    - `request_cancel_activity_then_resolved_canceled` — Full lifecycle: RequestCancelActivity → ActivityResolved(Canceled)
    - _Requirements: 11.9, 11.10, 11.11, 11.12, 11.13, 11.14, 11.15_

  - [x] 5.6 Write end-to-end golden test in golden_tests.rs
    - `cancel_then_cancel_workflow_e2e` — Cancel → WFTStarted → WFTCompleted(CancelWorkflow) → assert final status Canceled
    - _Requirements: 11.16_

- [x] 6. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All test tasks are required (not optional) per project convention
- Property tests use `proptest! { }` block style consistent with Feature 1
- Golden tests are individual `#[test]` functions
- All tests extend existing files (property_tests.rs and golden_tests.rs), no new test files
- Property tests are numbered 17–24, continuing from Feature 1's numbering
- Each property test is tagged with its design property reference
