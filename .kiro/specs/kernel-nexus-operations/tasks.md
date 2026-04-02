# Implementation Plan: Kernel Nexus Operations (Feature 9)

## Overview

Add Nexus operation support to `tokeira-kernel`. Types first (state, command, event, transition, kernel), then kernel logic (apply_nexus_operation_resolved + 2 workflow command arms + Start init + close extension), then downstream fixes, workspace compile checkpoint, then tests. The key structural difference: Started resolution is non-terminal (does NOT remove from pending, does NOT schedule WFT).

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add `PendingNexusOperation` struct to `state.rs` and `pending_nexus_operations` field to `WorkflowState`
    - `PendingNexusOperation { operation_id: String, scheduled_event_id: i64, endpoint: String, service: String, operation: String, started: bool }` with `Clone, Debug, PartialEq`
    - Add `pending_nexus_operations: BTreeMap<String, PendingNexusOperation>` field to `WorkflowState` after `pending_updates`
    - _Requirements: 1.1.1–1.1.6, 1.3.1_

  - [x] 1.2 Add `NexusResolution`, `NexusOperationResolvedRequest`, and new `WorkflowCommand` variants to `command.rs`
    - `NexusResolution` enum: `Started`, `Completed { result: Payloads }`, `Failed { failure: String }`, `Canceled`, `TimedOut` with `Clone, Debug, PartialEq`
    - `NexusOperationResolvedRequest { operation_id: String, scheduled_event_id: i64, resolution: NexusResolution, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - Add `NexusOperationResolved(NexusOperationResolvedRequest)` variant to `Command` enum
    - Add `ScheduleNexusOperation { operation_id: String, endpoint: String, service: String, operation: String, input: Payloads, schedule_to_close_timeout: Option<Duration> }` variant to `WorkflowCommand`
    - Add `CancelNexusOperation { scheduled_event_id: i64 }` variant to `WorkflowCommand`
    - _Requirements: 1.2.1–1.2.6, 1.4.1, 1.4.2, 1.5.1–1.5.6_

  - [x] 1.3 Add 7 `HistoryEventKind` variants to `event.rs`
    - `NexusOperationScheduled { operation_id, endpoint, service, operation, input, schedule_to_close_timeout }`
    - `NexusOperationStarted { operation_id, scheduled_event_id }`
    - `NexusOperationCompleted { operation_id, scheduled_event_id, result }`
    - `NexusOperationFailed { operation_id, scheduled_event_id, failure }`
    - `NexusOperationCanceled { operation_id, scheduled_event_id }`
    - `NexusOperationTimedOut { operation_id, scheduled_event_id }`
    - `NexusOperationCancelRequested { scheduled_event_id }`
    - _Requirements: 1.6.1–1.6.7_

  - [x] 1.4 Add 2 `DispatchOp` variants to `transition.rs`
    - `ScheduleNexusOperation { operation_id: String, endpoint: String, service: String, operation: String, input: Payloads, schedule_to_close_timeout: Option<Duration> }`
    - `CancelNexusOperation { scheduled_event_id: i64 }`
    - _Requirements: 1.7.1, 1.7.2_

  - [x] 1.5 Add 3 `Reject` variants to `kernel.rs`
    - `DuplicateNexusOperationId(String)` with `#[error("duplicate nexus operation id: {0}")]`
    - `UnknownNexusOperation(String)` with `#[error("unknown nexus operation: {0}")]`
    - `StaleNexusResolution { operation_id: String, expected_scheduled_event_id: i64 }` with `#[error("stale nexus resolution for {operation_id}: expected scheduled_event_id {expected_scheduled_event_id}")]`
    - `NexusOperationAlreadyStarted(String)` with `#[error("nexus operation already started: {0}")]`
    - _Requirements: 1.8.1, 1.8.2, 1.8.3, 1.8.4_

- [x] 2. Implement kernel logic
  - [x] 2.1 Add `apply_nexus_operation_resolved` method and `Command::NexusOperationResolved` match arm in `kernel.rs`
    - New match arm in `BasicKernel::apply`: `Command::NexusOperationResolved(req) => self.apply_nexus_operation_resolved(loaded, req)`
    - `apply_nexus_operation_resolved` follows ExternalSignalResolved pattern: `expect_open` → lookup pending by `operation_id` (reject `UnknownNexusOperation`) → validate `scheduled_event_id` fencing (reject `StaleNexusResolution`) → emit appropriate event per resolution variant → if terminal: remove from pending + schedule WFT if none pending → `finish`
    - Started is non-terminal: reject with `NexusOperationAlreadyStarted` if `started` flag is already `true`, otherwise set `started = true`, do NOT remove from pending, do NOT schedule WFT
    - No `RequestDedupeOp` (internal runtime machinery)
    - _Requirements: 4.1.1–4.6.2, 5.1.1, 5.1.2, 7.6.1, 7.7.1, 7.7.2_

  - [x] 2.2 Add `ScheduleNexusOperation` and `CancelNexusOperation` match arms in `apply_workflow_command`
    - `ScheduleNexusOperation`: check duplicate `operation_id` (reject `DuplicateNexusOperationId`) → emit `NexusOperationScheduled` → insert `PendingNexusOperation` with `started: false` → push `DispatchOp::ScheduleNexusOperation` → return `Ok(false)`
    - `CancelNexusOperation`: validate `scheduled_event_id` references a pending Nexus operation (reject `UnknownNexusOperation` if not found) → emit `NexusOperationCancelRequested` → push `DispatchOp::CancelNexusOperation` → return `Ok(false)`
    - _Requirements: 2.1.1–2.1.4, 2.2.1, 3.1.1–3.1.4, 5.2.1, 5.2.2_

  - [x] 2.3 Extend `apply_start` initializer and `TransitionBuilder::close()`
    - Add `pending_nexus_operations: BTreeMap::new()` to `WorkflowState` initializer in `apply_start`
    - Add `self.state.pending_nexus_operations.clear();` to `close()` after `pending_updates.clear()` — no dispatch ops emitted for cleared entries
    - _Requirements: 1.3.2, 6.1.1–6.1.7, 8.1.2, 8.1.3_

- [x] 3. Fix downstream breakage
  - [x] 3.1 Fix all exhaustive match arms on `Command`, `WorkflowCommand`, `HistoryEventKind`, `DispatchOp`, `Reject` and all `WorkflowState` construction sites across the workspace
    - Search for exhaustive matches and struct literals that need the new variants/field
    - Add wildcard or explicit arms as appropriate for test helpers, serialization, display, etc.
    - Add `pending_nexus_operations` to `make_open_state` and any other test helper constructors
    - _Requirements: 8.1.1_

- [x] 4. Checkpoint — workspace compilation
  - Run `cargo check --workspace` and ensure zero errors. Ask the user if questions arise.

- [x] 5. Add golden tests to `golden_tests.rs`
  - [x] 5.1 Add `schedule_nexus_operation_happy_path` test
    - WFT completion with `ScheduleNexusOperation`. Assert: `NexusOperationScheduled` event with correct fields, pending entry created with correct `scheduled_event_id`/`endpoint`/`service`/`operation`, `DispatchOp::ScheduleNexusOperation` present, run still open.
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4_

  - [x] 5.2 Add `schedule_nexus_operation_duplicate_rejected` test
    - WFT completion with `ScheduleNexusOperation` where `operation_id` already in pending map. Assert: `Reject::DuplicateNexusOperationId`.
    - _Requirements: 2.2.1_

  - [x] 5.3 Add `cancel_nexus_operation_happy_path` test
    - WFT completion with `CancelNexusOperation`. Assert: `NexusOperationCancelRequested` event, `DispatchOp::CancelNexusOperation`, operation still in pending map, run still open.
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4_

  - [x] 5.4 Add `nexus_operation_resolved_started` test
    - `NexusOperationResolved` with Started variant. Assert: `NexusOperationStarted` event, operation still pending, no WFT scheduled, no dedup ops.
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.1.4_

  - [x] 5.5 Add `nexus_operation_resolved_completed` test
    - `NexusOperationResolved` with Completed variant, no WFT pending. Assert: `NexusOperationCompleted` event, operation removed from pending, WFT scheduled.
    - _Requirements: 4.2.1, 4.2.2, 4.2.3_

  - [x] 5.6 Add `nexus_operation_resolved_completed_with_pending_wft` test
    - Same as 5.5 but with WFT already pending. Assert: no second WFT.
    - _Requirements: 4.2.4_

  - [x] 5.7 Add `nexus_operation_resolved_failed` test
    - `NexusOperationResolved` with Failed variant. Assert: `NexusOperationFailed` event, operation removed, WFT scheduled.
    - _Requirements: 4.3.1, 4.3.2, 4.3.3_

  - [x] 5.8 Add `nexus_operation_resolved_canceled` test
    - `NexusOperationResolved` with Canceled variant. Assert: `NexusOperationCanceled` event, operation removed, WFT scheduled.
    - _Requirements: 4.4.1, 4.4.2, 4.4.3_

  - [x] 5.9 Add `nexus_operation_resolved_timed_out` test
    - `NexusOperationResolved` with TimedOut variant. Assert: `NexusOperationTimedOut` event, operation removed, WFT scheduled.
    - _Requirements: 4.5.1, 4.5.2, 4.5.3_

  - [x] 5.10 Add `nexus_operation_resolved_unknown_operation` test
    - `NexusOperationResolved` with unknown `operation_id`. Assert: `Reject::UnknownNexusOperation`.
    - _Requirements: 4.6.1_

  - [x] 5.11 Add `nexus_operation_resolved_stale` test
    - `NexusOperationResolved` with wrong `scheduled_event_id`. Assert: `Reject::StaleNexusResolution`.
    - _Requirements: 4.6.2_

  - [x] 5.11a Add `nexus_operation_resolved_started_duplicate` test
    - `NexusOperationResolved` with Started variant for an operation whose `started` flag is already `true`. Assert: `Reject::NexusOperationAlreadyStarted`.
    - _Requirements: 4.1.6_

  - [x] 5.11b Add `cancel_nexus_operation_unknown` test
    - `CancelNexusOperation` with a `scheduled_event_id` that does not match any pending operation. Assert: `Reject::UnknownNexusOperation`.
    - _Requirements: 3.2.1_

  - [x] 5.12 Add `nexus_operation_resolved_absent_run` test
    - `NexusOperationResolved` against `LoadedRun::Absent`. Assert: `Reject::MissingRun`.
    - _Requirements: 5.1.2_

  - [x] 5.13 Add `nexus_operation_resolved_closed_run` test
    - `NexusOperationResolved` against closed run. Assert: `Reject::RunClosed`.
    - _Requirements: 5.1.2_

  - [x] 5.14 Add `terminate_clears_pending_nexus_operations` test
    - Terminate with pending Nexus operations. Assert: `pending_nexus_operations` empty, no nexus dispatch ops from close.
    - _Requirements: 6.1.1, 6.1.7_

  - [x] 5.15 Add `close_via_complete_clears_pending_nexus_operations` test
    - CompleteWorkflow with pending Nexus operations. Assert: `pending_nexus_operations` empty, no nexus dispatch ops from close.
    - _Requirements: 6.1.3, 6.1.7_

- [x] 6. Add property tests to `property_tests.rs`
  - [x] 6.1 Extend `arb_valid_pair` strategy with Nexus arms
    - Add `WorkflowTaskCompleted` containing `ScheduleNexusOperation` commands
    - Add `WorkflowTaskCompleted` containing `CancelNexusOperation` commands (with pending nexus operation in state)
    - Add `Command::NexusOperationResolved` with Started variant against state with pending nexus operation
    - Add `Command::NexusOperationResolved` with terminal variants against state with pending nexus operation
    - Add helper strategies: `arb_nexus_resolution()`, `arb_schedule_nexus_operation_command()`, `arb_cancel_nexus_operation_command(scheduled_event_id)`, `with_pending_nexus_operation(state, operation_id)`
    - _Requirements: 7.1.1–7.3.1 (structural invariants covered by existing property tests via arb_valid_pair)_

  - [x] 6.2 Extend existing `property_1_start_field_pass_through` to assert `pending_nexus_operations` is empty
    - **Property 1: Start initializes pending_nexus_operations to empty**
    - **Validates: Requirements 1.3.2, 8.1.2**

  - [x] 6.3 Add Property 2 test: ScheduleNexusOperation event and state pass-through
    - `proptest!` block: generate random `ScheduleNexusOperation`, apply in WFT completion, assert `NexusOperationScheduled` event fields match, pending entry correct with `scheduled_event_id`/`endpoint`/`service`/`operation`, `DispatchOp::ScheduleNexusOperation` present, run still open
    - **Property 2: ScheduleNexusOperation event and state pass-through**
    - **Validates: Requirements 2.1.1, 2.1.2, 2.1.3, 2.1.4, 7.4.1**

  - [x] 6.4 Add Property 3 test: ScheduleNexusOperation duplicate rejection
    - `proptest!` block: generate state with pending nexus operation, schedule duplicate `operation_id`, assert `Reject::DuplicateNexusOperationId`
    - **Property 3: ScheduleNexusOperation duplicate rejection**
    - **Validates: Requirements 2.2.1**

  - [x] 6.5 Add Property 4 test: CancelNexusOperation event and dispatch
    - `proptest!` block: generate random `CancelNexusOperation` in WFT completion with pending nexus operation, assert `NexusOperationCancelRequested` event + `DispatchOp::CancelNexusOperation` + operation still pending + run still open
    - **Property 4: CancelNexusOperation event and dispatch**
    - **Validates: Requirements 3.1.1, 3.1.2, 3.1.3, 3.1.4**

  - [x] 6.6 Add Property 5 test: Started resolution is non-terminal
    - `proptest!` block: generate pending nexus operation, resolve with Started, assert `NexusOperationStarted` event + stays pending + no WFT scheduled + no `DispatchOp::EnqueueWorkflowTask` + no dedup ops
    - **Property 5: Started resolution is non-terminal**
    - **Validates: Requirements 4.1.1, 4.1.2, 4.1.3, 4.1.4, 7.6.1, 7.7.1, 7.7.2**

  - [x] 6.7 Add Property 6 test: Terminal resolution removes from pending and schedules WFT
    - `proptest!` block: generate pending nexus operation and random terminal resolution variant (Completed/Failed/Canceled/TimedOut), assert correct event + removed from pending + conditional WFT scheduling + no dedup ops
    - **Property 6: Terminal resolution removes from pending and schedules WFT**
    - **Validates: Requirements 4.2.1–4.5.4, 7.4.3, 7.6.1**

  - [x] 6.8 Add Property 7 test: NexusOperationResolved rejection paths
    - `proptest!` block: generate NexusOperationResolved against state without matching operation (unknown `operation_id` → `UnknownNexusOperation`, mismatched `scheduled_event_id` → `StaleNexusResolution`)
    - **Property 7: NexusOperationResolved rejection paths**
    - **Validates: Requirements 4.6.1, 4.6.2**

  - [x] 6.9 Add Property 8 test: Close clears pending Nexus operations without dispatch ops
    - `proptest!` block: generate state with pending nexus operations, close via various paths (Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew), assert `pending_nexus_operations` empty and no `DispatchOp::ScheduleNexusOperation`/`CancelNexusOperation` from close
    - **Property 8: Close clears pending Nexus operations without dispatch ops**
    - **Validates: Requirements 6.1.1–6.1.7, 7.5.1**

- [x] 7. Final checkpoint — all tests pass
  - Run `cargo test --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required (none marked optional) per user direction
- Property tests use `proptest! { }` block style, golden tests are individual `#[test]` functions
- Tests extend existing `golden_tests.rs` and `property_tests.rs` — no new test files
- Property 9 (structural invariants) is covered by extending `arb_valid_pair` in task 6.1 — existing structural property tests automatically cover Nexus transitions
- Started resolution is the key structural difference: non-terminal, does NOT remove from pending, does NOT schedule WFT
