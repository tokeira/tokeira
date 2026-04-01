# Implementation Plan: Child Workflows (Feature 5)

## Overview

This feature is both additive (new types, commands, methods) and invasive (modifies 6 existing close paths and adds a new field to durable WorkflowState). New types, enum variants, 3 new kernel methods, a shared Parent Close Policy helper, and modifications to all existing close paths (Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew). Downstream crates (`tokeira-edge` translate.rs, grpc_properties.rs) will need wildcard or explicit arms for new `WorkflowCommand`, `Command`, `Reject`, `DispatchOp`, and `HistoryEventKind` variants. `WorkflowState` gains a `children` field — all construction sites must be updated. Feature 5 depends on Features 1, 3, and 4. Types first, kernel logic second, close path extensions, downstream fixes, workspace compile checkpoint, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add ChildWorkflowState, ParentClosePolicy, and children field to state.rs
    - Add `ParentClosePolicy` enum with `Terminate`, `RequestCancel`, `Abandon` variants, deriving `Clone, Copy, Debug, PartialEq, Eq`
    - Add `ChildWorkflowState` struct with `child_workflow_id` (WorkflowId), `child_run_id` (Option\<RunId\>), `initiated_event_id` (i64), `started_event_id` (Option\<i64\>), `parent_close_policy` (ParentClosePolicy), deriving `Clone, Debug, PartialEq`
    - Add `children: BTreeMap<WorkflowId, ChildWorkflowState>` field to `WorkflowState`
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 1.2 Add ChildStartConfirmedRequest, ChildStartResult, ChildResolvedRequest, ChildResolution structs and enums to command.rs
    - Add `ChildStartConfirmedRequest` struct with `child_workflow_id`, `initiated_event_id`, `result` (ChildStartResult), `now`
    - Add `ChildStartResult` enum with `Started { child_run_id, workflow_type }` and `Failed { cause }`
    - Add `ChildResolvedRequest` struct with `child_workflow_id`, `resolution` (ChildResolution), `now`
    - Add `ChildResolution` enum with `Completed { result }`, `Failed { failure }`, `Canceled`, `Terminated`, `TimedOut`
    - All derive `Clone, Debug, PartialEq`
    - _Requirements: 1.5, 1.6, 1.7, 1.8_

  - [x] 1.3 Add StartChildWorkflow variant to WorkflowCommand enum in command.rs
    - Add `StartChildWorkflow { child_workflow_id, namespace_id, workflow_type, task_queue, input, parent_close_policy }` variant
    - Import `ParentClosePolicy` from `crate::state`
    - _Requirements: 1.4_

  - [x] 1.4 Add ChildStartConfirmed and ChildResolved variants to Command enum in command.rs
    - Add `ChildStartConfirmed(ChildStartConfirmedRequest)` and `ChildResolved(ChildResolvedRequest)` to the `Command` enum
    - _Requirements: 1.5.1, 1.7.1_

  - [x] 1.5 Add 8 new HistoryEventKind variants to event.rs
    - Add `StartChildWorkflowExecutionInitiated { child_workflow_id, workflow_type, task_queue, input, namespace_id, parent_close_policy }` — import `ParentClosePolicy` from `crate::state`
    - Add `ChildWorkflowExecutionStarted { child_workflow_id, child_run_id, workflow_type }`
    - Add `StartChildWorkflowExecutionFailed { child_workflow_id, cause }`
    - Add `ChildWorkflowExecutionCompleted { child_workflow_id, result }`
    - Add `ChildWorkflowExecutionFailed { child_workflow_id, failure }`
    - Add `ChildWorkflowExecutionCanceled { child_workflow_id }`
    - Add `ChildWorkflowExecutionTerminated { child_workflow_id }`
    - Add `ChildWorkflowExecutionTimedOut { child_workflow_id }`
    - _Requirements: 1.9_

  - [x] 1.6 Add 3 new DispatchOp variants to transition.rs
    - Add `StartChildWorkflow { child_workflow_id, namespace_id, workflow_type, task_queue, input }` — import `WorkflowId`, `NamespaceId`, `WorkflowType`, `Payloads` from `tokeira_types`
    - Add `TerminateChild { child_workflow_id, child_run_id, reason }` — import `RunId` from `tokeira_types`
    - Add `CancelChild { child_workflow_id, child_run_id, reason }`
    - _Requirements: 1.10_

  - [x] 1.7 Add 3 new Reject variants to kernel.rs
    - Add `DuplicateChildWorkflowId(WorkflowId)` with `#[error("duplicate child workflow id: {0}")]`
    - Add `UnknownChild(WorkflowId)` with `#[error("unknown child: {0}")]`
    - Add `StaleChildConfirmation { child_workflow_id: WorkflowId, expected_initiated_event_id: i64 }` with appropriate `#[error(...)]`
    - Import `WorkflowId` if not already in scope
    - _Requirements: 1.11_

- [x] 2. Initialize children field and implement new kernel methods
  - [x] 2.1 Initialize children to empty BTreeMap in apply_start
    - Add `children: BTreeMap::new()` to the `WorkflowState` initialization in `apply_start`
    - _Requirements: 1.3.2, 6.4_

  - [x] 2.2 Add StartChildWorkflow match arm to apply_workflow_command
    - Validate `child_workflow_id` not already in `builder.state.children`, reject with `DuplicateChildWorkflowId` if duplicate
    - Emit `StartChildWorkflowExecutionInitiated` event, capture `initiated_event_id`
    - Insert `ChildWorkflowState { child_workflow_id, child_run_id: None, initiated_event_id, started_event_id: None, parent_close_policy }` into `builder.state.children`
    - Push `DispatchOp::StartChildWorkflow { child_workflow_id, namespace_id, workflow_type, task_queue, input }`
    - Return `Ok(false)` (run is not closed)
    - _Requirements: 2.1, 2.2, 6.2_

  - [x] 2.3 Implement apply_child_start_confirmed method on BasicKernel
    - `expect_open` → `TransitionBuilder` → look up child by `child_workflow_id` or reject `UnknownChild` → validate `initiated_event_id` matches or reject `StaleChildConfirmation`
    - Match on `result`: `Started` → emit `ChildWorkflowExecutionStarted`, update child entry with `child_run_id` and `started_event_id`; `Failed` → emit `StartChildWorkflowExecutionFailed`, remove child from map
    - If no pending WFT, call `schedule_workflow_task()`
    - Call `finish()` — no `RequestDedupeOp` pushed
    - _Requirements: 3.1, 3.2, 3.3, 6.1.1, 6.1.3_

  - [x] 2.4 Implement apply_child_resolved method on BasicKernel
    - `expect_open` → `TransitionBuilder` → look up child by `child_workflow_id` or reject `UnknownChild`
    - Match on `resolution`: emit the corresponding terminal event variant (`ChildWorkflowExecutionCompleted`, `ChildWorkflowExecutionFailed`, `ChildWorkflowExecutionCanceled`, `ChildWorkflowExecutionTerminated`, `ChildWorkflowExecutionTimedOut`)
    - Remove child from `builder.state.children`
    - If no pending WFT, call `schedule_workflow_task()`
    - Call `finish()` — no `RequestDedupeOp` pushed
    - _Requirements: 4.1, 4.2, 6.1.2, 6.1.4_

  - [x] 2.5 Add ChildStartConfirmed and ChildResolved match arms to BasicKernel::apply
    - `Command::ChildStartConfirmed(req) => self.apply_child_start_confirmed(loaded, req)`
    - `Command::ChildResolved(req) => self.apply_child_resolved(loaded, req)`
    - _Requirements: 6.1.1, 6.1.2_

- [x] 3. Implement Parent Close Policy helper and extend close paths
  - [x] 3.1 Add apply_parent_close_policy method to TransitionBuilder
    - `std::mem::take` the children map
    - Iterate: for each child with `child_run_id: Some`, match on `parent_close_policy` — `Terminate` → push `DispatchOp::TerminateChild`, `RequestCancel` → push `DispatchOp::CancelChild`, `Abandon` → skip
    - Children with `child_run_id: None` → no dispatch op, just removed
    - Children map is already empty from `std::mem::take`
    - _Requirements: 5.7, 6.3_

  - [x] 3.2 Extend apply_terminate to call apply_parent_close_policy
    - After existing `std::mem::take` for activities/timers, add `builder.apply_parent_close_policy()`
    - _Requirements: 5.1_

  - [x] 3.3 Extend apply_workflow_execution_timed_out to call apply_parent_close_policy
    - After existing `std::mem::take` for activities/timers, add `builder.apply_parent_close_policy()`
    - _Requirements: 5.2_

  - [x] 3.4 Extend CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew in apply_workflow_command to call apply_parent_close_policy
    - In each close arm, after `builder.close(...)`, add `builder.apply_parent_close_policy()` before returning `Ok(true)`
    - _Requirements: 5.3, 5.4, 5.5, 5.6_

- [x] 4. Fix downstream breakage and workspace compile checkpoint
  - [x] 4.1 Update tokeira-edge translate.rs exhaustive matches
    - Add match arms or wildcards for new `WorkflowCommand::StartChildWorkflow`, new `Command` variants, new `HistoryEventKind` variants, new `DispatchOp` variants, and new `Reject` variants
    - _Requirements: 9.1_

  - [x] 4.2 Update tokeira-edge grpc_properties.rs test generators
    - Add new `WorkflowCommand`, `HistoryEventKind`, `DispatchOp`, and `Reject` variants to any exhaustive generators or matches
    - _Requirements: 9.1_

  - [x] 4.3 Update any other downstream crates that match on WorkflowState, Command, Reject, DispatchOp, or HistoryEventKind
    - Search workspace for exhaustive matches on these types and add arms for new variants
    - Ensure `WorkflowState` construction sites include `children: BTreeMap::new()` (or equivalent)
    - _Requirements: 9.1_

  - [x] 4.4 Workspace compile checkpoint
    - Run `cargo check --workspace` and fix any remaining compile errors
    - Ensure all tests pass, ask the user if questions arise.
    - _Requirements: 9.1_

- [x] 5. Add property-based tests
  - [x] 5.1 Add new generators to property_tests.rs
    - Add `arb_parent_close_policy()` — generates random `ParentClosePolicy` variant
    - Add `arb_child_workflow_state(initiated_event_id)` — generates `ChildWorkflowState` with random policy, optional `child_run_id`/`started_event_id`
    - Add `arb_child_start_result()` — generates random `ChildStartResult` (Started or Failed)
    - Add `arb_child_resolution()` — generates random `ChildResolution` variant
    - Add `arb_start_child_workflow_command()` — generates random `StartChildWorkflow` workflow command
    - Add `arb_children(n, initiated_event_id_base)` — generates 0–n random children in a `BTreeMap`
    - Add `with_child(state, child_workflow_id, initiated_event_id, policy, started)` — helper to add a child to state
    - Import new types: `ChildStartConfirmedRequest`, `ChildStartResult`, `ChildResolvedRequest`, `ChildResolution`, `ChildWorkflowState`, `ParentClosePolicy`
    - _Requirements: 8.1–8.11_

  - [x] 5.2 Extend arb_valid_pair() with 8 new arms
    - Arm 1: `ChildStartConfirmed(Started)`, no pending WFT — open state with initiated child, matching `initiated_event_id`
    - Arm 2: `ChildStartConfirmed(Started)`, with pending WFT — same but state has pending WFT
    - Arm 3: `ChildStartConfirmed(Failed)` — open state with initiated child
    - Arm 4: `ChildResolved` (all variants) — open state with started child, random `ChildResolution`
    - Arm 5: `WorkflowTaskCompleted` with `StartChildWorkflow` — add to existing WFT completed `prop_oneof!`
    - Arm 6: Terminate with children — extend existing Terminate arm to include 0–3 random children with random policies and random `child_run_id`
    - Arm 7: `WorkflowExecutionTimedOut` with children — extend existing TimedOut arm to include random children
    - Arm 8: Close workflow commands with children — extend CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew arms to include random children
    - This automatically extends existing properties 4, 5, 7, 8, 9, 10 to cover all new commands (Design Property 12)
    - _Requirements: 7.1, 7.2, 7.3_

  - [x] 5.3 Write property_33_start_child_workflow_happy_path
    - `proptest! { }` block: for any valid open state with started WFT and any StartChildWorkflow command with unique child_workflow_id, next_state.children contains entry with child_run_id None, started_event_id None, correct initiated_event_id and parent_close_policy; history contains StartChildWorkflowExecutionInitiated; dispatch_ops contains DispatchOp::StartChildWorkflow
    - Tag: `// Feature: kernel-child-workflows, Property 1: StartChildWorkflow creates child entry, emits event, and emits dispatch op`
    - **Design Property 1**
    - _Requirements: 2.1, 7.4.1, 8.1, 8.2_

  - [x] 5.4 Write property_34_start_child_workflow_rejects_duplicate
    - `proptest! { }` block: for any valid open state with a child already in children map, StartChildWorkflow with same child_workflow_id rejects with DuplicateChildWorkflowId
    - Tag: `// Feature: kernel-child-workflows, Property 2: StartChildWorkflow rejects duplicate child_workflow_id`
    - **Design Property 2**
    - _Requirements: 2.2, 8.11_

  - [x] 5.5 Write property_35_child_start_confirmed_started
    - `proptest! { }` block: for any valid open state with known child and matching initiated_event_id, ChildStartConfirmed(Started) emits ChildWorkflowExecutionStarted and updates child entry with child_run_id and started_event_id
    - Tag: `// Feature: kernel-child-workflows, Property 3: ChildStartConfirmed(Started) emits started event and updates child entry`
    - **Design Property 3**
    - _Requirements: 3.1, 7.4.2, 8.3_

  - [x] 5.6 Write property_36_child_start_confirmed_failed
    - `proptest! { }` block: for any valid open state with known child and matching initiated_event_id, ChildStartConfirmed(Failed) emits StartChildWorkflowExecutionFailed and removes child from map
    - Tag: `// Feature: kernel-child-workflows, Property 4: ChildStartConfirmed(Failed) emits failed event and removes child`
    - **Design Property 4**
    - _Requirements: 3.2, 7.4.3, 8.4_

  - [x] 5.7 Write property_37_child_start_confirmed_wft_coalescing
    - `proptest! { }` block: two sub-cases — no pending WFT → schedules WFT + one EnqueueWorkflowTask; pending WFT → dispatch_ops has no EnqueueWorkflowTask, existing WFT preserved
    - Tag: `// Feature: kernel-child-workflows, Property 5: ChildStartConfirmed WFT coalescing`
    - **Design Property 5**
    - _Requirements: 3.1.3, 3.1.4, 3.2.3, 7.3.1, 8.3_

  - [x] 5.8 Write property_38_child_start_confirmed_fencing
    - `proptest! { }` block: for any valid open state with known child, ChildStartConfirmed with mismatched initiated_event_id rejects with StaleChildConfirmation carrying correct child_workflow_id and expected_initiated_event_id
    - Tag: `// Feature: kernel-child-workflows, Property 6: ChildStartConfirmed fencing rejects stale initiated_event_id`
    - **Design Property 6**
    - _Requirements: 3.3.2, 8.9_

  - [x] 5.9 Write property_39_child_resolved_event_matches_variant
    - `proptest! { }` block: for any valid open state with known child and any ChildResolution variant, emitted event matches: Completed→ChildWorkflowExecutionCompleted, Failed→ChildWorkflowExecutionFailed, Canceled→ChildWorkflowExecutionCanceled, Terminated→ChildWorkflowExecutionTerminated, TimedOut→ChildWorkflowExecutionTimedOut
    - Tag: `// Feature: kernel-child-workflows, Property 7: ChildResolved event matches resolution variant`
    - **Design Property 7**
    - _Requirements: 4.1.1–4.1.5_

  - [x] 5.10 Write property_40_child_resolved_removes_child
    - `proptest! { }` block: for any valid open state with known child and any ChildResolution variant, next_state.children does not contain the child_workflow_id
    - Tag: `// Feature: kernel-child-workflows, Property 8: ChildResolved removes child`
    - **Design Property 8**
    - _Requirements: 4.1.6, 7.4.4, 8.5_

  - [x] 5.11 Write property_41_child_resolved_wft_coalescing
    - `proptest! { }` block: two sub-cases — no pending WFT → schedules WFT; pending WFT → no EnqueueWorkflowTask
    - Tag: `// Feature: kernel-child-workflows, Property 9: ChildResolved WFT coalescing`
    - **Design Property 9**
    - _Requirements: 4.1.7, 4.1.8, 7.3.2, 8.6_

  - [x] 5.12 Write property_42_parent_close_policy_all_paths
    - `proptest! { }` block: for any valid close transition (Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew) with N open children: next_state.children is empty; TerminateChild count equals children with Terminate policy and child_run_id Some; CancelChild count equals children with RequestCancel policy and child_run_id Some; no ops for Abandon or child_run_id None
    - Tag: `// Feature: kernel-child-workflows, Property 10: Parent Close Policy on all close paths`
    - **Design Property 10**
    - _Requirements: 5.1–5.7, 7.5, 7.6, 8.7, 8.8_

  - [x] 5.13 Write property_43_start_initializes_children_empty
    - `proptest! { }` block: for any valid Start transition, next_state.children is empty
    - Tag: `// Feature: kernel-child-workflows, Property 13: Start initializes children to empty`
    - **Design Property 13**
    - _Requirements: 1.3, 6.4_

- [x] 6. Add golden tests
  - [x] 6.1 Write StartChildWorkflow golden tests in golden_tests.rs
    - `start_child_workflow_happy_path` — Unique child_workflow_id within WFT completed → initiated event + child entry + dispatch op
    - `start_child_workflow_duplicate_rejected` — Duplicate child_workflow_id → `DuplicateChildWorkflowId`
    - `start_child_workflow_does_not_close` — Run remains open after StartChildWorkflow
    - Import new types: `ChildWorkflowState`, `ParentClosePolicy`, `ChildStartConfirmedRequest`, `ChildStartResult`, `ChildResolvedRequest`, `ChildResolution`
    - _Requirements: 2.1, 2.2_

  - [x] 6.2 Write ChildStartConfirmed golden tests in golden_tests.rs
    - `child_start_confirmed_started_no_wft` — Started result, no pending WFT → started event + WFT scheduled
    - `child_start_confirmed_started_with_wft` — Started result, WFT pending → started event, no second WFT
    - `child_start_confirmed_failed` — Failed result → failed event + child removed + WFT scheduled
    - `child_start_confirmed_unknown_child` — Unknown child_workflow_id → `UnknownChild`
    - `child_start_confirmed_stale_fencing` — Mismatched initiated_event_id → `StaleChildConfirmation`
    - _Requirements: 3.1, 3.2, 3.3_

  - [x] 6.3 Write ChildResolved golden tests in golden_tests.rs
    - `child_resolved_completed` — Completed resolution → completed event + child removed + WFT
    - `child_resolved_failed` — Failed resolution → failed event + child removed
    - `child_resolved_all_terminal_variants` — Canceled/Terminated/TimedOut each emit correct event
    - `child_resolved_unknown_child` — Unknown child_workflow_id → `UnknownChild`
    - _Requirements: 4.1, 4.2_

  - [x] 6.4 Write Parent Close Policy golden tests in golden_tests.rs
    - `terminate_with_children_policy_terminate` — Terminate with Terminate-policy started children → TerminateChild ops
    - `terminate_with_children_policy_cancel` — Terminate with RequestCancel-policy started children → CancelChild ops
    - `terminate_with_children_policy_abandon` — Terminate with Abandon-policy children → no child dispatch ops
    - `terminate_with_unstarted_children` — Children with child_run_id None → no dispatch ops, children still cleared
    - _Requirements: 5.1, 5.7_

  - [x] 6.5 Write close path coverage golden tests in golden_tests.rs
    - `complete_workflow_with_children` — CompleteWorkflow clears children + emits policy ops
    - `continue_as_new_with_children` — ContinueAsNew clears children + emits policy ops
    - `workflow_execution_timed_out_with_children` — TimedOut clears children + emits policy ops
    - _Requirements: 5.2, 5.3, 5.6_

  - [x] 6.6 Write end-to-end golden test in golden_tests.rs
    - `child_workflow_full_lifecycle_e2e` — StartChildWorkflow → ChildStartConfirmed(Started) → ChildResolved(Completed) → child removed, WFT scheduled
    - _Requirements: 2.1, 3.1, 4.1_

- [x] 7. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel` and `cargo check --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All test tasks are required (not optional) per project convention
- Property tests use `proptest! { }` block style consistent with Features 1–4
- Golden tests are individual `#[test]` functions
- All tests extend existing files (property_tests.rs and golden_tests.rs), no new test files
- Property tests are numbered 33–43, continuing from Feature 4's numbering
- Each property test is tagged with its design property reference
- Design Properties 11 and 12 are covered by extending `arb_valid_pair()` and the existing structural invariant property tests (properties 4, 5, 7, 8, 9, 10)
- `make_open_state()` in both test files will need the `children: BTreeMap::new()` field added
