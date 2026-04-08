# Implementation Plan: Nexus Operation Dispatch

## Overview

Wire the runtime's `DispatchPublisher` to handle `DispatchOp::ScheduleNexusOperation` and `DispatchOp::CancelNexusOperation` with outbound HTTP via a `NexusHttpClient` trait, endpoint resolution via `NexusEndpointRegistry`, and runtime-local timeout tracking via `NexusTimeoutScanner`. Extend the kernel's dispatch ops with originator identity fields. Follow existing patterns from child workflow, external signal, and workflow timeout features.

## Tasks

- [x] 1. Extend kernel dispatch ops with originator identity fields
  - [x] 1.1 Add `originator_run_key: RunKey`, `scheduled_event_id: i64`, and `scheduled_at: OffsetDateTime` fields to `DispatchOp::ScheduleNexusOperation` in `tokeira/crates/tokeira-kernel/src/transition.rs`
    - `scheduled_at` carries the authoritative event timestamp for timeout tracking
    - _Requirements: 2.1, 2.3, 2.5_

  - [x] 1.2 Add `originator_run_key: RunKey`, `operation_id: String`, `endpoint: String`, `service: String` fields to `DispatchOp::CancelNexusOperation` in `tokeira/crates/tokeira-kernel/src/transition.rs`
    - Update the enum variant to include the four new fields alongside existing `scheduled_event_id`
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 1.3 Update `apply_workflow_command` ScheduleNexusOperation arm in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Populate `originator_run_key` from `builder.state.run_key`
    - Populate `scheduled_event_id` from the emitted `NexusOperationScheduled` event ID
    - Populate `scheduled_at` from the emitted event's `happened_at` timestamp (which is `builder.now`)
    - _Requirements: 2.2, 2.4, 2.5_

  - [x] 1.4 Update `apply_workflow_command` CancelNexusOperation arm in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Populate `originator_run_key` from `builder.state.run_key`
    - Populate `operation_id`, `endpoint`, `service` from the matching `PendingNexusOperation` entry (already looked up as `known`)
    - _Requirements: 4.4_

  - [x] 1.5 Fix any compile errors in existing tests caused by the new dispatch op fields
    - Update test assertions and mock dispatch op constructions in `tokeira-kernel` tests
    - _Requirements: 2.1, 2.3, 4.1, 4.2, 4.3_

  - [x] 1.6 Write property test: kernel populates schedule dispatch op fields (Property 3)
    - **Property 3: Kernel populates schedule dispatch op fields from workflow state**
    - Generate random `WorkflowState` with random `run_key` and `last_event_id`, plus random `ScheduleNexusOperation` command parameters
    - Apply via `BasicKernel` and verify `originator_run_key == state.run_key` and `scheduled_event_id == state.last_event_id + 1`
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4**

  - [x] 1.7 Write property test: kernel populates cancel dispatch op fields (Property 4)
    - **Property 4: Kernel populates cancel dispatch op fields from workflow state and pending operation**
    - Generate random `WorkflowState` with a random `PendingNexusOperation` entry
    - Apply `CancelNexusOperation` and verify `originator_run_key`, `operation_id`, `endpoint`, `service` match
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

- [x] 2. Checkpoint — Ensure all kernel tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Implement NexusHttpClient trait, NexusEndpointRegistry, and NexusStartResult in `tokeira-runtime`
  - [x] 3.1 Define `NexusStartResult` enum, `NexusHttpClient` trait, `NexusEndpointConfig` struct, and `NexusEndpointRegistry` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - `NexusStartResult`: `SyncCompleted { result: Payloads }`, `SyncFailed { message: String }`, `AsyncAccepted`
    - `NexusHttpClient` trait with `start_operation` and `cancel_operation` async methods (`cancel_operation` takes address, operation_id, service — no operation name needed)
    - `NexusEndpointConfig` with `address: String`
    - `NexusEndpointRegistry` with `Arc<HashMap<String, NexusEndpointConfig>>` and `resolve` method
    - _Requirements: 6.1, 6.2, 6.3, 5.1, 5.2_

  - [x] 3.2 Write property test: endpoint registry lookup correctness (Property 11)
    - **Property 11: Endpoint registry lookup correctness**
    - Generate random endpoint name/address pairs, insert into registry
    - Verify registered names return correct address, unregistered names return `None`
    - **Validates: Requirements 5.1, 5.2**

- [x] 4. Implement NexusTimeoutEntry, NexusTimeoutTrackingState, and evaluate_nexus_timeout
  - [x] 4.1 Define `NexusTimeoutEntry`, `NexusTimeoutTrackingState`, `NexusTimeoutScannerConfig`, and `evaluate_nexus_timeout` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - `NexusTimeoutEntry` with `run_key`, `operation_id`, `scheduled_event_id`, `schedule_to_close_timeout`, `scheduled_at`
    - `NexusTimeoutTrackingState` with `insert`, `remove`, `remove_all_for_run`, `snapshot` methods
    - `NexusTimeoutScannerConfig` with `scan_interval` and `max_timeouts_per_scan` (defaults: 1s, 100)
    - `evaluate_nexus_timeout` pure function
    - _Requirements: 7.1, 7.2, 7.4, 7.5_

  - [x] 4.2 Write property test: nexus timeout evaluation correctness (Property 6)
    - **Property 6: Nexus timeout evaluation correctness**
    - Generate random `NexusTimeoutEntry` with random `schedule_to_close_timeout` and `scheduled_at`, plus random `now`
    - Verify `evaluate_nexus_timeout` returns correct boolean based on elapsed vs timeout, including zero-duration edge case
    - **Validates: Requirements 7.1**

- [x] 5. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Wire RuntimeDispatchPublisher for Nexus dispatch
  - [x] 6.1 Add `nexus_client: Arc<dyn NexusHttpClient>`, `nexus_registry: NexusEndpointRegistry`, and `nexus_timeout_tracking: NexusTimeoutTrackingState` fields to `RuntimeDispatchPublisher`
    - Update struct definition, `Clone` impl, and `new()` constructor
    - Update all call sites that construct `RuntimeDispatchPublisher` (in `TokeiraRuntime::new` and tests)
    - _Requirements: 6.4, 5.2_

  - [x] 6.2 Implement `handle_schedule_nexus_operation` method on `RuntimeDispatchPublisher`
    - Resolve endpoint via `nexus_registry.resolve`
    - Call `nexus_client.start_operation` with correct parameters
    - Map result to `NexusResolution` variant (SyncCompleted→Completed, SyncFailed→Failed, AsyncAccepted→Started, Err→Failed)
    - Unknown endpoint → `NexusResolution::Failed` with "endpoint not found" message
    - Submit `Command::NexusOperationResolved` to originator via `pick_lane().submit()`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 5.3, 9.1_

  - [x] 6.3 Implement `handle_cancel_nexus_operation` method on `RuntimeDispatchPublisher`
    - Resolve endpoint via `nexus_registry.resolve`; unknown endpoint → log warn, return
    - Call `nexus_client.cancel_operation`
    - On success: submit `NexusOperationResolved(Canceled)` to originator
    - On failure: log debug, no-op
    - _Requirements: 3.1, 3.2, 3.3, 5.4, 9.3_

  - [x] 6.4 Add `ScheduleNexusOperation` and `CancelNexusOperation` match arms to `RuntimeDispatchPublisher::publish`
    - Replace the `other =>` catch-all for these two variants
    - Insert timeout tracking entry if `schedule_to_close_timeout` is `Some`
    - `tokio::spawn` each handler call (fire-and-forget pattern)
    - _Requirements: 1.6, 3.4, 7.2, 8.1_

  - [x] 6.5 Write property test: HTTP client start_operation receives correct parameters (Property 1)
    - **Property 1: HTTP client start_operation receives correct parameters**
    - Generate random dispatch op fields, configure mock registry with random address
    - Mock HTTP client captures call arguments
    - Verify all fields (address, operation_id, service, operation, input, schedule_to_close_timeout) match
    - **Validates: Requirements 1.1, 1.2**

  - [x] 6.6 Write property test: schedule resolution always delivered with correct variant (Property 2)
    - **Property 2: Schedule resolution always delivered with correct variant**
    - Generate random dispatch ops and random outcome (sync-complete, sync-fail, async-accept, transient-error, unknown-endpoint)
    - Verify `Command::NexusOperationResolved` is always submitted with correct `operation_id`, `scheduled_event_id`, and `resolution` variant
    - **Validates: Requirements 1.3, 1.4, 1.5, 5.3, 9.1**

  - [x] 6.7 Write property test: cancel success delivers Canceled resolution (Property 5)
    - **Property 5: Cancel success delivers Canceled resolution**
    - Generate random cancel dispatch ops with endpoints present in registry, mock HTTP client returns success
    - Verify `NexusResolution::Canceled` is submitted to originator with correct `operation_id` and `scheduled_event_id`
    - **Validates: Requirements 3.1, 3.3**

  - [x] 6.8 Write property test: tracking entry inserted only when schedule_to_close_timeout is present (Property 7)
    - **Property 7: Tracking entry inserted only when schedule_to_close_timeout is present**
    - Generate random `ScheduleNexusOperation` dispatch ops with random `schedule_to_close_timeout` (Some or None)
    - Publish the op and verify tracking state contains entry iff timeout was Some
    - **Validates: Requirements 7.2, 8.1**

- [x] 7. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement lane integration for Nexus timeout tracking cleanup
  - [x] 8.1 Add `NexusTimeoutTrackingState` parameter to `spawn_lane` and `run_activation` in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Pass through to `run_activation`
    - _Requirements: 8.2, 8.3_

  - [x] 8.2 Add run-close cleanup: call `nexus_timeout_tracking.remove_all_for_run(message.run_key)` when `new_state.closed_at.is_some()`
    - Place alongside existing `workflow_timeout_tracking.remove(message.run_key)` call
    - _Requirements: 8.3_

  - [x] 8.3 Add resolution-commit cleanup: when committed command is `Command::NexusOperationResolved` with a terminal resolution (Completed, Failed, Canceled, TimedOut), call `nexus_timeout_tracking.remove(run_key, &operation_id)`. The `Started` resolution is non-terminal and SHALL NOT remove the tracking entry.
    - Inspect the committed command after successful `CommitResult::Applied`
    - _Requirements: 8.2_

  - [x] 8.4 Update all `spawn_lane` call sites to pass `NexusTimeoutTrackingState`
    - Update `TokeiraRuntime::new` and any test call sites
    - _Requirements: 8.2, 8.3_

  - [x] 8.5 Write property test: tracking entry removed on any terminal resolution (Property 8)
    - **Property 8: Tracking entry removed on any terminal resolution**
    - Generate random resolution variants (Completed, Failed, Canceled, TimedOut)
    - Insert tracking entry, commit `NexusOperationResolved`, verify entry removed
    - **Validates: Requirements 8.2**

  - [x] 8.6 Write property test: tracking entries removed on run close (Property 9)
    - **Property 9: Tracking entries removed on run close**
    - Generate random runs with multiple tracking entries
    - Close the run, verify all entries for that run are removed while entries for other runs remain
    - **Validates: Requirements 8.3**

- [x] 9. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement NexusTimeoutScanner background task
  - [x] 10.1 Implement `scan_nexus_timeouts_once` function in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Snapshot tracking state, iterate entries up to `max_timeouts_per_scan`
    - Call `evaluate_nexus_timeout` for each entry
    - Submit `Command::NexusOperationResolved(TimedOut)` via lane for timed-out entries
    - On success or kernel rejection: remove tracking entry
    - On transient error: log warn, leave entry for next cycle
    - _Requirements: 7.1, 7.3, 7.5, 7.6, 10.1, 10.2, 10.3_

  - [x] 10.2 Implement `run_nexus_timeout_scanner` async loop in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Loop with `tokio::select!` on cancellation token and `tokio::time::sleep(config.scan_interval)`
    - Call `scan_nexus_timeouts_once` each cycle
    - _Requirements: 7.3, 7.4_

  - [x] 10.3 Wire `NexusTimeoutScanner` into `TokeiraRuntime`
    - Add `nexus_timeout_tracking`, `nexus_timeout_scanner_handle`, `nexus_timeout_scanner_cancel` fields to `TokeiraRuntime`
    - Spawn scanner in `TokeiraRuntime::new`, accept `NexusTimeoutScannerConfig` parameter
    - Add `shutdown_nexus_timeout_scanner` method
    - Accept `Arc<dyn NexusHttpClient>` and `NexusEndpointRegistry` at construction time, pass to `RuntimeDispatchPublisher`
    - _Requirements: 7.3, 7.4, 7.5, 6.4_

  - [x] 10.4 Write property test: scanner batch bound (Property 10)
    - **Property 10: Scanner batch bound**
    - Generate random number of timed-out entries exceeding `max_timeouts_per_scan`
    - Run one scan cycle, verify at most `max_timeouts_per_scan` commands submitted
    - **Validates: Requirements 7.5**

- [x] 11. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All 11 correctness properties from the design are implemented as required (non-optional) property test tasks
- Each property test task references its property number and the requirements it validates
- The implementation language is Rust, matching the existing codebase and design document
- Checkpoints are placed after each major phase (kernel changes, runtime types, publisher wiring, lane integration, scanner)
- Property tests use the `proptest` crate consistent with existing test infrastructure
