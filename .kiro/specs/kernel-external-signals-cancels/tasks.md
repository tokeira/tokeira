# Implementation Plan: External Signals and Cancel Requests (Feature 6)

## Overview

This feature adds new types, 2 new workflow commands, 2 new top-level commands, and 2 new kernel methods. It also modifies durable `WorkflowState` (2 new fields) and the shared `close()` method on `TransitionBuilder` (clearing the new pending maps on every terminal path). Downstream crates gain new variants for WorkflowCommand, Command, Reject, DispatchOp, HistoryEventKind. Types first, kernel logic second, downstream fixes, workspace compile checkpoint, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add PendingExternalSignal, PendingExternalCancel, and pending maps to state.rs
    - Add `PendingExternalSignal` struct with `initiated_event_id` (i64), `target_workflow_id` (WorkflowId), `target_run_id` (Option\<RunId\>), `signal_name` (String), deriving `Clone, Debug, PartialEq`
    - Add `PendingExternalCancel` struct with `initiated_event_id` (i64), `target_workflow_id` (WorkflowId), `target_run_id` (Option\<RunId\>), deriving `Clone, Debug, PartialEq`
    - Add `pending_external_signals: BTreeMap<i64, PendingExternalSignal>` and `pending_external_cancels: BTreeMap<i64, PendingExternalCancel>` fields to `WorkflowState`
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 1.2 Add ExternalSignalResolvedRequest, ExternalCancelResolvedRequest, result enums, and Command variants to command.rs
    - Add `ExternalSignalResult` enum with `Signaled` and `Failed { cause: String }`, deriving `Clone, Debug, PartialEq`
    - Add `ExternalCancelResult` enum with `CancelRequested` and `Failed { cause: String }`, deriving `Clone, Debug, PartialEq`
    - Add `ExternalSignalResolvedRequest` struct with `initiated_event_id` (i64), `result` (ExternalSignalResult), `now` (OffsetDateTime), deriving `Clone, Debug, PartialEq`
    - Add `ExternalCancelResolvedRequest` struct with `initiated_event_id` (i64), `result` (ExternalCancelResult), `now` (OffsetDateTime), deriving `Clone, Debug, PartialEq`
    - Add `ExternalSignalResolved(ExternalSignalResolvedRequest)` and `ExternalCancelResolved(ExternalCancelResolvedRequest)` to `Command` enum
    - _Requirements: 1.5, 1.6_

  - [x] 1.3 Add SignalExternalWorkflowExecution and RequestCancelExternalWorkflowExecution variants to WorkflowCommand enum in command.rs
    - Add `SignalExternalWorkflowExecution { target_workflow_id: WorkflowId, target_run_id: Option<RunId>, signal_name: String, input: Payloads }`
    - Add `RequestCancelExternalWorkflowExecution { target_workflow_id: WorkflowId, target_run_id: Option<RunId> }`
    - _Requirements: 1.4_

  - [x] 1.4 Add 6 new HistoryEventKind variants to event.rs
    - Add `SignalExternalWorkflowExecutionInitiated { target_workflow_id, target_run_id, signal_name, input }`
    - Add `ExternalWorkflowExecutionSignaled { initiated_event_id, target_workflow_id }`
    - Add `SignalExternalWorkflowExecutionFailed { initiated_event_id, target_workflow_id, cause }`
    - Add `RequestCancelExternalWorkflowExecutionInitiated { target_workflow_id, target_run_id }`
    - Add `ExternalWorkflowExecutionCancelRequested { initiated_event_id, target_workflow_id }`
    - Add `RequestCancelExternalWorkflowExecutionFailed { initiated_event_id, target_workflow_id, cause }`
    - _Requirements: 1.7_

  - [x] 1.5 Add 2 new DispatchOp variants to transition.rs
    - Add `SignalExternalWorkflow { target_workflow_id, target_run_id, signal_name, input }`
    - Add `RequestCancelExternalWorkflow { target_workflow_id, target_run_id }`
    - Import `WorkflowId`, `RunId`, `Payloads` from `tokeira_types` if not already in scope
    - _Requirements: 1.8_

  - [x] 1.6 Add 2 new Reject variants to kernel.rs
    - Add `UnknownExternalSignal(i64)` with `#[error("unknown external signal: initiated_event_id={0}")]`
    - Add `UnknownExternalCancel(i64)` with `#[error("unknown external cancel: initiated_event_id={0}")]`
    - _Requirements: 1.9_

- [x] 2. Implement kernel logic for external signals and cancels
  - [x] 2.1 Initialize pending external maps to empty in apply_start
    - Add `pending_external_signals: BTreeMap::new()` and `pending_external_cancels: BTreeMap::new()` to the `WorkflowState` initialization in `apply_start`
    - _Requirements: 1.3.3, 1.3.4, 9.1.2_

  - [x] 2.2 Add SignalExternalWorkflowExecution and RequestCancelExternalWorkflowExecution match arms to apply_workflow_command
    - `SignalExternalWorkflowExecution`: emit `SignalExternalWorkflowExecutionInitiated`, capture `initiated_event_id`, insert `PendingExternalSignal` into `pending_external_signals`, push `DispatchOp::SignalExternalWorkflow`, return `Ok(false)`
    - `RequestCancelExternalWorkflowExecution`: emit `RequestCancelExternalWorkflowExecutionInitiated`, capture `initiated_event_id`, insert `PendingExternalCancel` into `pending_external_cancels`, push `DispatchOp::RequestCancelExternalWorkflow`, return `Ok(false)`
    - _Requirements: 2.1, 4.1, 6.2_

  - [x] 2.3 Extend TransitionBuilder::close() to clear pending external maps
    - Add `self.state.pending_external_signals.clear()` and `self.state.pending_external_cancels.clear()` inside `close()`, before the `ProjectionOp::CloseExecution` push. No dispatch ops emitted for cleared entries.
    - _Requirements: 7.1, 9.1.3_

  - [x] 2.4 Implement apply_external_signal_resolved method on BasicKernel
    - `expect_open` → `TransitionBuilder` → look up by `initiated_event_id` in `pending_external_signals` or reject `UnknownExternalSignal`
    - Match on `result`: `Signaled` → emit `ExternalWorkflowExecutionSignaled`; `Failed` → emit `SignalExternalWorkflowExecutionFailed`
    - Remove entry from `pending_external_signals`
    - If no pending WFT, call `schedule_workflow_task()`
    - Call `finish()` — no `RequestDedupeOp`
    - _Requirements: 3.1, 3.2, 3.3, 6.1.1, 6.1.3_

  - [x] 2.5 Implement apply_external_cancel_resolved method on BasicKernel
    - `expect_open` → `TransitionBuilder` → look up by `initiated_event_id` in `pending_external_cancels` or reject `UnknownExternalCancel`
    - Match on `result`: `CancelRequested` → emit `ExternalWorkflowExecutionCancelRequested`; `Failed` → emit `RequestCancelExternalWorkflowExecutionFailed`
    - Remove entry from `pending_external_cancels`
    - If no pending WFT, call `schedule_workflow_task()`
    - Call `finish()` — no `RequestDedupeOp`
    - _Requirements: 5.1, 5.2, 5.3, 6.1.2, 6.1.4_

  - [x] 2.6 Add ExternalSignalResolved and ExternalCancelResolved match arms to BasicKernel::apply
    - `Command::ExternalSignalResolved(req) => self.apply_external_signal_resolved(loaded, req)`
    - `Command::ExternalCancelResolved(req) => self.apply_external_cancel_resolved(loaded, req)`
    - Import `ExternalSignalResolvedRequest`, `ExternalCancelResolvedRequest`, `ExternalSignalResult`, `ExternalCancelResult` in kernel.rs use block
    - _Requirements: 6.1.1, 6.1.2_

- [x] 3. Fix downstream breakage and workspace compile checkpoint
  - [x] 3.1 Update tokeira-edge translate.rs exhaustive matches
    - Add match arms or wildcards for new `WorkflowCommand`, `Command`, `HistoryEventKind`, `DispatchOp`, and `Reject` variants
    - _Requirements: 9.1_

  - [x] 3.2 Update tokeira-edge grpc_properties.rs test generators
    - Add new `WorkflowCommand`, `HistoryEventKind`, `DispatchOp`, and `Reject` variants to any exhaustive generators or matches
    - _Requirements: 9.1_

  - [x] 3.3 Update any other downstream crates that match on WorkflowState, Command, Reject, DispatchOp, or HistoryEventKind
    - Search workspace for exhaustive matches on these types and add arms for new variants
    - Ensure all `WorkflowState` construction sites include `pending_external_signals: BTreeMap::new()` and `pending_external_cancels: BTreeMap::new()`
    - _Requirements: 9.1_

  - [x] 3.4 Workspace compile checkpoint
    - Run `cargo check --workspace` and fix any remaining compile errors
    - Ensure all tests pass, ask the user if questions arise.
    - _Requirements: 9.1_

- [x] 4. Add property-based tests
  - [x] 4.1 Add new generators to property_tests.rs
    - Add `arb_external_signal_result()` — generates random `ExternalSignalResult` variant
    - Add `arb_external_cancel_result()` — generates random `ExternalCancelResult` variant
    - Add `arb_pending_external_signal(initiated_event_id)` — generates `PendingExternalSignal` with random target fields
    - Add `arb_pending_external_cancel(initiated_event_id)` — generates `PendingExternalCancel` with random target fields
    - Add `arb_signal_external_workflow_command()` — generates random `SignalExternalWorkflowExecution` workflow command
    - Add `arb_request_cancel_external_workflow_command()` — generates random `RequestCancelExternalWorkflowExecution` workflow command
    - Add `with_pending_external_signal(state, initiated_event_id)` — helper to add a pending external signal to state
    - Add `with_pending_external_cancel(state, initiated_event_id)` — helper to add a pending external cancel to state
    - Add `with_random_pending_externals(state, n_signals, n_cancels, event_id_base)` — helper to add random pending externals
    - Import new types: `ExternalSignalResolvedRequest`, `ExternalSignalResult`, `ExternalCancelResolvedRequest`, `ExternalCancelResult`, `PendingExternalSignal`, `PendingExternalCancel`
    - _Requirements: 8.1–8.6_

  - [x] 4.2 Extend arb_valid_pair() with 11 new arms
    - Arm 1: `ExternalSignalResolved(Signaled)`, no pending WFT — open state with `PendingExternalSignal`, matching `initiated_event_id`
    - Arm 2: `ExternalSignalResolved(Signaled)`, with pending WFT
    - Arm 3: `ExternalSignalResolved(Failed)` — open state with `PendingExternalSignal`
    - Arm 4: `ExternalCancelResolved(CancelRequested)`, no pending WFT — open state with `PendingExternalCancel`
    - Arm 5: `ExternalCancelResolved(CancelRequested)`, with pending WFT
    - Arm 6: `ExternalCancelResolved(Failed)` — open state with `PendingExternalCancel`
    - Arm 7: `WorkflowTaskCompleted` with `SignalExternalWorkflowExecution` — add to existing WFT completed `prop_oneof!`
    - Arm 8: `WorkflowTaskCompleted` with `RequestCancelExternalWorkflowExecution` — add to existing WFT completed `prop_oneof!`
    - Arm 9: Terminate with pending externals — extend existing Terminate arm to include 0–2 random pending external signals and 0–2 random pending external cancels
    - Arm 10: `WorkflowExecutionTimedOut` with pending externals — extend similarly
    - Arm 11: Close workflow commands with pending externals — extend CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew arms to include random pending externals
    - This automatically extends existing structural invariant properties to cover all new commands (Design Property 10)
    - _Requirements: 8.1–8.6_

  - [x] 4.3 Write property_44_signal_external_workflow_happy_path
    - `proptest! { }` block: for any valid open state with started WFT and any SignalExternalWorkflowExecution command, next_state.pending_external_signals contains entry keyed by initiated_event_id with correct fields; history contains SignalExternalWorkflowExecutionInitiated; dispatch_ops contains DispatchOp::SignalExternalWorkflow; run stays open
    - Tag: `// Feature: kernel-external-signals-cancels, Property 1`
    - **Design Property 1**
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 8.4.1_

  - [x] 4.4 Write property_45_request_cancel_external_workflow_happy_path
    - `proptest! { }` block: for any valid open state with started WFT and any RequestCancelExternalWorkflowExecution command, next_state.pending_external_cancels contains entry keyed by initiated_event_id with correct fields; history contains RequestCancelExternalWorkflowExecutionInitiated; dispatch_ops contains DispatchOp::RequestCancelExternalWorkflow; run stays open
    - Tag: `// Feature: kernel-external-signals-cancels, Property 2`
    - **Design Property 2**
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.1.4, 8.4.3_

  - [x] 4.5 Write property_46_external_signal_resolved_event_and_removal
    - `proptest! { }` block: for any valid open state with known pending external signal and any ExternalSignalResult variant, emitted event matches variant (Signaled→ExternalWorkflowExecutionSignaled, Failed→SignalExternalWorkflowExecutionFailed); entry removed from pending_external_signals
    - Tag: `// Feature: kernel-external-signals-cancels, Property 3`
    - **Design Property 3**
    - _Requirements: 3.1.1, 3.1.2, 3.2.1, 3.2.2, 8.4.2_

  - [x] 4.6 Write property_47_external_cancel_resolved_event_and_removal
    - `proptest! { }` block: for any valid open state with known pending external cancel and any ExternalCancelResult variant, emitted event matches variant (CancelRequested→ExternalWorkflowExecutionCancelRequested, Failed→RequestCancelExternalWorkflowExecutionFailed); entry removed from pending_external_cancels
    - Tag: `// Feature: kernel-external-signals-cancels, Property 4`
    - **Design Property 4**
    - _Requirements: 5.1.1, 5.1.2, 5.2.1, 5.2.2, 8.4.4_

  - [x] 4.7 Write property_48_resolution_wft_coalescing
    - `proptest! { }` block: two sub-cases — no pending WFT → schedules WFT + one EnqueueWorkflowTask; pending WFT → no EnqueueWorkflowTask. Covers both ExternalSignalResolved and ExternalCancelResolved.
    - Tag: `// Feature: kernel-external-signals-cancels, Property 5`
    - **Design Property 5**
    - _Requirements: 3.1.3, 3.1.4, 3.2.3, 5.1.3, 5.1.4, 5.2.3, 8.3.1, 8.3.2_

  - [x] 4.8 Write property_49_resolution_rejects_unknown
    - `proptest! { }` block: for any valid open state, ExternalSignalResolved with unknown initiated_event_id → UnknownExternalSignal; ExternalCancelResolved with unknown initiated_event_id → UnknownExternalCancel
    - Tag: `// Feature: kernel-external-signals-cancels, Property 6`
    - **Design Property 6**
    - _Requirements: 3.3.1, 5.3.1, 1.9.1, 1.9.2_

  - [x] 4.9 Write property_50_no_dedup_for_resolution
    - `proptest! { }` block: for any valid ExternalSignalResolved or ExternalCancelResolved transition, request_dedupe_ops is empty
    - Tag: `// Feature: kernel-external-signals-cancels, Property 7`
    - **Design Property 7**
    - _Requirements: 3.1.5, 5.1.5, 8.6.1, 8.6.2_

  - [x] 4.10 Write property_51_close_clears_pending_externals
    - `proptest! { }` block: for any valid close transition (Terminate, TimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew) with N pending external signals and M pending external cancels, next_state maps are empty and dispatch_ops contains no SignalExternalWorkflow or RequestCancelExternalWorkflow
    - Tag: `// Feature: kernel-external-signals-cancels, Property 8`
    - **Design Property 8**
    - _Requirements: 7.1.1–7.1.7, 8.5.1, 8.5.2_

  - [x] 4.11 Write property_52_start_initializes_pending_externals_empty
    - `proptest! { }` block: for any valid Start transition, next_state.pending_external_signals and next_state.pending_external_cancels are empty
    - Tag: `// Feature: kernel-external-signals-cancels, Property 9`
    - **Design Property 9**
    - _Requirements: 1.3.3, 1.3.4, 9.1.2_

- [x] 5. Add golden tests
  - [x] 5.1 Write SignalExternalWorkflowExecution golden tests in golden_tests.rs
    - `signal_external_workflow_happy_path` — Emit initiated event + pending entry + dispatch op, run stays open
    - `signal_external_workflow_does_not_close` — Run remains open after SignalExternalWorkflowExecution
    - Import new types: `PendingExternalSignal`, `PendingExternalCancel`, `ExternalSignalResolvedRequest`, `ExternalSignalResult`, `ExternalCancelResolvedRequest`, `ExternalCancelResult`
    - _Requirements: 2.1_

  - [x] 5.2 Write RequestCancelExternalWorkflowExecution golden tests in golden_tests.rs
    - `request_cancel_external_workflow_happy_path` — Emit initiated event + pending entry + dispatch op, run stays open
    - `request_cancel_external_workflow_does_not_close` — Run remains open
    - _Requirements: 4.1_

  - [x] 5.3 Write ExternalSignalResolved golden tests in golden_tests.rs
    - `external_signal_resolved_signaled_no_wft` — Signaled result, no pending WFT → signaled event + entry removed + WFT scheduled
    - `external_signal_resolved_signaled_with_wft` — Signaled result, WFT pending → signaled event, no second WFT
    - `external_signal_resolved_failed` — Failed result → failed event + entry removed + WFT scheduled
    - `external_signal_resolved_unknown` — Unknown initiated_event_id → `UnknownExternalSignal`
    - _Requirements: 3.1, 3.2, 3.3_

  - [x] 5.4 Write ExternalCancelResolved golden tests in golden_tests.rs
    - `external_cancel_resolved_success_no_wft` — CancelRequested result, no pending WFT → cancel-requested event + entry removed + WFT scheduled
    - `external_cancel_resolved_success_with_wft` — CancelRequested result, WFT pending → event, no second WFT
    - `external_cancel_resolved_failed` — Failed result → failed event + entry removed + WFT scheduled
    - `external_cancel_resolved_unknown` — Unknown initiated_event_id → `UnknownExternalCancel`
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 5.5 Write close path coverage golden tests in golden_tests.rs
    - `terminate_clears_pending_externals` — Terminate with pending signals and cancels → maps cleared, no external dispatch ops
    - `complete_workflow_clears_pending_externals` — CompleteWorkflow with pending externals → maps cleared
    - `continue_as_new_clears_pending_externals` — ContinueAsNew with pending externals → maps cleared
    - _Requirements: 7.1_

  - [x] 5.6 Write end-to-end golden test in golden_tests.rs
    - `external_signal_full_lifecycle_e2e` — SignalExternalWorkflowExecution → ExternalSignalResolved(Signaled) → entry removed, WFT scheduled
    - _Requirements: 2.1, 3.1_

- [x] 6. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel` and `cargo check --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All test tasks are required (not optional) per project convention
- Property tests use `proptest! { }` block style consistent with Features 1–5
- Golden tests are individual `#[test]` functions
- All tests extend existing files (property_tests.rs and golden_tests.rs), no new test files
- Property tests are numbered 44–52, continuing from Feature 5's numbering
- Design Property 10 is covered by extending `arb_valid_pair()` and the existing structural invariant property tests
- `make_open_state()` in both test files will need `pending_external_signals: BTreeMap::new()` and `pending_external_cancels: BTreeMap::new()` fields added
