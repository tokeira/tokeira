# Implementation Plan: WFT Failure and Timeout Recovery (Feature 2)

## Overview

Add `WorkflowTaskFailed` and `WorkflowTaskTimedOut` commands to the kernel. All changes are additive — new request structs, new Command/HistoryEventKind variants, new `apply_*` methods, and test extensions. No existing code paths change.

Ordered: types first, kernel logic second, compile checkpoint, tests last.

## Tasks

- [x] 1. Add new types, request structs, and Command variants
  - [x] 1.0 Add `WorkflowTaskFailedCause` and `WorkflowTaskTimeoutType` domain enums to `crates/tokeira-kernel/src/command.rs`
    - Add `WorkflowTaskFailedCause` enum with variants: `NonDeterminismError`, `BadScheduleActivityAttributes`, `BadStartTimerAttributes`, `UnhandledCommand`, `BadRequestCancelActivityAttributes`, `WorkflowWorkerUnhandledFailure`, `BadSignalWorkflowExecutionAttributes`
    - Add `WorkflowTaskTimeoutType` enum with variant: `StartToClose`
    - Both enums derive `Clone, Debug, PartialEq`
    - _Requirements: 1.1a, 1.2a_

  - [x] 1.1 Add WorkflowTaskFailedRequest and WorkflowTaskTimedOutRequest structs to `crates/tokeira-kernel/src/command.rs`
    - Add `WorkflowTaskFailedRequest` with fields: `logical_seq: LogicalTaskSeq`, `started_event_id: i64`, `failure_cause: WorkflowTaskFailedCause`, `failure_details: Option<Payload>`, `worker_identity: WorkerIdentity`, `now: OffsetDateTime`
    - Add `WorkflowTaskTimedOutRequest` with fields: `logical_seq: LogicalTaskSeq`, `started_event_id: i64`, `timeout_type: WorkflowTaskTimeoutType`, `now: OffsetDateTime`
    - Both structs derive `Clone, Debug, PartialEq`
    - _Requirements: 1.1.1–1.1.7, 1.2.1–1.2.5_

  - [x] 1.2 Add WorkflowTaskFailed and WorkflowTaskTimedOut variants to the Command enum in `crates/tokeira-kernel/src/command.rs`
    - Add `WorkflowTaskFailed(WorkflowTaskFailedRequest)` variant
    - Add `WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest)` variant
    - _Requirements: 1.1.1, 1.2.1_

  - [x] 1.3 Add WorkflowTaskFailed and WorkflowTaskTimedOut variants to HistoryEventKind in `crates/tokeira-kernel/src/event.rs`
    - Add `WorkflowTaskFailed { logical_seq, scheduled_event_id, started_event_id, failure_cause: WorkflowTaskFailedCause, failure_details, identity }` variant (no reset metadata — deferred to Feature 10)
    - Add `WorkflowTaskTimedOut { logical_seq, scheduled_event_id, started_event_id, timeout_type: WorkflowTaskTimeoutType }` variant
    - _Requirements: 1.3.1–1.3.7, 1.4.1–1.4.5_

  - [x] 1.4 Export new types from the crate's public API
    - Add `WorkflowTaskFailedRequest`, `WorkflowTaskTimedOutRequest`, `WorkflowTaskFailedCause`, `WorkflowTaskTimeoutType` to the `pub use` in `crates/tokeira-kernel/src/lib.rs`
    - _Requirements: 1.1.1, 1.1a, 1.2.1, 1.2a_

- [x] 2. Implement kernel apply methods
  - [x] 2.1 Add `apply_workflow_task_failed` method to BasicKernel in `crates/tokeira-kernel/src/kernel.rs`
    - Call `expect_open` to validate run exists and is open
    - Validate pending WFT exists (`NoPendingWorkflowTask` rejection)
    - Validate pending WFT has `started_event_id` (`WorkflowTaskNotStarted` rejection)
    - Validate request `logical_seq` matches pending WFT (`WorkflowTaskSeqMismatch` rejection)
    - Validate request `started_event_id` matches pending WFT (`WorkflowTaskTokenMismatch` rejection)
    - Emit `HistoryEventKind::WorkflowTaskFailed` with fields from pending WFT and request
    - Clear `started_event_id` to `None` on pending WFT (preserve `logical_seq` and `scheduled_event_id`)
    - Push `DispatchOp::EnqueueWorkflowTask` with `sticky_preferred` from current `state.sticky`
    - Do NOT clear sticky affinity, do NOT close the run
    - _Requirements: 2.1.1–2.1.8, 2.2.5–2.2.6, 4.1.1–4.1.4, 4.2.1–4.2.2, 4.3.1, 4.3.3, 4.3.4, 6.1.1, 6.1.3_

  - [x] 2.2 Add `apply_workflow_task_timed_out` method to BasicKernel in `crates/tokeira-kernel/src/kernel.rs`
    - Call `expect_open` to validate run exists and is open
    - Validate pending WFT exists (`NoPendingWorkflowTask` rejection)
    - Validate pending WFT has `started_event_id` (`WorkflowTaskNotStarted` rejection)
    - Validate request `logical_seq` matches pending WFT (`WorkflowTaskSeqMismatch` rejection)
    - Validate request `started_event_id` matches pending WFT (`WorkflowTaskTokenMismatch` rejection)
    - Emit `HistoryEventKind::WorkflowTaskTimedOut` with fields from pending WFT and request
    - Clear `started_event_id` to `None` on pending WFT (preserve `logical_seq` and `scheduled_event_id`)
    - Clear `state.sticky` to `None` (worker presumed dead)
    - Push `DispatchOp::EnqueueWorkflowTask` with `sticky_preferred: None`
    - Do NOT close the run
    - _Requirements: 3.1.1–3.1.8, 3.2.5–3.2.6, 4.1.1–4.1.4, 4.2.3–4.2.4, 4.3.2–4.3.4, 6.1.2, 6.1.4_

  - [x] 2.3 Add match arms for new commands in `BasicKernel::apply` in `crates/tokeira-kernel/src/kernel.rs`
    - Add `Command::WorkflowTaskFailed(req) => self.apply_workflow_task_failed(loaded, req)`
    - Add `Command::WorkflowTaskTimedOut(req) => self.apply_workflow_task_timed_out(loaded, req)`
    - _Requirements: 6.1.1, 6.1.2_

- [x] 3. Checkpoint — Compile check
  - Ensure `cargo check -p tokeira-kernel` passes with no errors. Ask the user if questions arise.

- [x] 4. Add golden tests for happy paths in `crates/tokeira-kernel/tests/golden_tests.rs`
  - [x] 4.1 Add `wft_failed_with_started_wft` golden test
    - Use `make_open_state_with_started_wft()`, set sticky affinity on state
    - Apply `Command::WorkflowTaskFailed` with failure_cause, failure_details, and worker_identity
    - Assert: one WorkflowTaskFailed history event with correct fields, pending WFT preserved with started_event_id None, sticky unchanged, one EnqueueWorkflowTask dispatch op with sticky_preferred, empty request_dedupe_ops/activity_ops/timer_ops/projection_ops
    - _Requirements: 8.1.1_

  - [x] 4.2 Add `wft_timed_out_with_started_wft` golden test
    - Use `make_open_state_with_started_wft()`, set sticky affinity on state
    - Apply `Command::WorkflowTaskTimedOut` with timeout_type
    - Assert: one WorkflowTaskTimedOut history event with correct fields, pending WFT preserved with started_event_id None, sticky cleared to None, one EnqueueWorkflowTask dispatch op with sticky_preferred None, empty request_dedupe_ops/activity_ops/timer_ops/projection_ops
    - _Requirements: 8.2.1_

  - [x] 4.3 Add `wft_failed_no_sticky` golden test
    - Use `make_open_state_with_started_wft()` (no sticky affinity set)
    - Apply `Command::WorkflowTaskFailed`
    - Assert: sticky remains None, EnqueueWorkflowTask has sticky_preferred None
    - _Requirements: 8.3.1_

  - [x] 4.4 Add `wft_timed_out_no_sticky` golden test
    - Use `make_open_state_with_started_wft()` (no sticky affinity set)
    - Apply `Command::WorkflowTaskTimedOut`
    - Assert: sticky is None, EnqueueWorkflowTask has sticky_preferred None
    - _Requirements: 8.4.1_

- [x] 5. Add golden tests for rejection paths in `crates/tokeira-kernel/tests/golden_tests.rs`
  - [x] 5.1 Add `reject_wft_failed_absent_run` golden test
    - Apply `Command::WorkflowTaskFailed` to `LoadedRun::Absent`
    - Assert: `Reject::MissingRun`
    - _Requirements: 8.5.1, 2.2.3_

  - [x] 5.2 Add `reject_wft_failed_closed_run` golden test
    - Apply `Command::WorkflowTaskFailed` to `make_closed_state()`
    - Assert: `Reject::RunClosed(ExecutionStatus::Completed)`
    - _Requirements: 8.5.2, 2.2.4_

  - [x] 5.3 Add `reject_wft_failed_no_pending` golden test
    - Apply `Command::WorkflowTaskFailed` to `make_open_state()` (no pending WFT)
    - Assert: `Reject::NoPendingWorkflowTask`
    - _Requirements: 8.5.3, 2.2.1_

  - [x] 5.4 Add `reject_wft_failed_not_started` golden test
    - Apply `Command::WorkflowTaskFailed` to `make_open_state_with_pending_wft()` (pending but not started)
    - Assert: `Reject::WorkflowTaskNotStarted { logical_seq: 3 }`
    - _Requirements: 8.5.4, 2.2.2_

  - [x] 5.5 Add `reject_wft_timed_out_absent_run` golden test
    - Apply `Command::WorkflowTaskTimedOut` to `LoadedRun::Absent`
    - Assert: `Reject::MissingRun`
    - _Requirements: 8.6.1, 3.2.3_

  - [x] 5.6 Add `reject_wft_timed_out_closed_run` golden test
    - Apply `Command::WorkflowTaskTimedOut` to `make_closed_state()`
    - Assert: `Reject::RunClosed(ExecutionStatus::Completed)`
    - _Requirements: 8.6.2, 3.2.4_

  - [x] 5.7 Add `reject_wft_timed_out_no_pending` golden test
    - Apply `Command::WorkflowTaskTimedOut` to `make_open_state()` (no pending WFT)
    - Assert: `Reject::NoPendingWorkflowTask`
    - _Requirements: 8.6.3, 3.2.1_

  - [x] 5.8 Add `reject_wft_timed_out_not_started` golden test
    - Apply `Command::WorkflowTaskTimedOut` to `make_open_state_with_pending_wft()` (pending but not started)
    - Assert: `Reject::WorkflowTaskNotStarted { logical_seq: 3 }`
    - _Requirements: 8.6.4, 3.2.2_

  - [x] 5.9 Add `reject_wft_failed_seq_mismatch` golden test
    - Apply `Command::WorkflowTaskFailed` with mismatched `logical_seq` to `make_open_state_with_started_wft()`
    - Assert: `Reject::WorkflowTaskSeqMismatch`
    - _Requirements: 8.5.5, 2.2.5_

  - [x] 5.10 Add `reject_wft_failed_started_event_mismatch` golden test
    - Apply `Command::WorkflowTaskFailed` with mismatched `started_event_id` to `make_open_state_with_started_wft()`
    - Assert: `Reject::WorkflowTaskTokenMismatch`
    - _Requirements: 8.5.6, 2.2.6_

  - [x] 5.11 Add `reject_wft_timed_out_seq_mismatch` golden test
    - Apply `Command::WorkflowTaskTimedOut` with mismatched `logical_seq` to `make_open_state_with_started_wft()`
    - Assert: `Reject::WorkflowTaskSeqMismatch`
    - _Requirements: 8.6.5, 3.2.5_

  - [x] 5.12 Add `reject_wft_timed_out_started_event_mismatch` golden test
    - Apply `Command::WorkflowTaskTimedOut` with mismatched `started_event_id` to `make_open_state_with_started_wft()`
    - Assert: `Reject::WorkflowTaskTokenMismatch`
    - _Requirements: 8.6.6, 3.2.6_

- [x] 6. Add property tests in `crates/tokeira-kernel/tests/property_tests.rs`
  - [x] 6.1 Add `arb_wft_failed_request` and `arb_wft_timed_out_request` generators
    - `arb_wft_failed_request(logical_seq, started_event_id)`: random `WorkflowTaskFailedCause` variant, optional failure_details (arb_payload), worker_identity; `logical_seq` and `started_event_id` are passed in to match the state
    - `arb_wft_timed_out_request(logical_seq, started_event_id)`: `WorkflowTaskTimeoutType::StartToClose`; `logical_seq` and `started_event_id` are passed in to match the state
    - _Requirements: 7.1–7.6_

  - [x] 6.2 Extend `arb_valid_pair()` with two new arms for WorkflowTaskFailed and WorkflowTaskTimedOut
    - WorkflowTaskFailed arm: generate state with started pending WFT and optional sticky affinity, pair with random WorkflowTaskFailedRequest
    - WorkflowTaskTimedOut arm: generate state with started pending WFT and optional sticky affinity, pair with random WorkflowTaskTimedOutRequest
    - This automatically extends existing properties 4, 5, 7, 8, 9, 10 to cover the new commands
    - _Requirements: 7.6.1–7.6.4, 5.1.1–5.1.3, 5.2.1–5.2.2, 5.3.1–5.3.3_

  - [x] 6.3 Add `property_11_wft_failed_event_field_pass_through` property test
    - **Property 1: WFT Failed event field pass-through**
    - For all valid WorkflowTaskFailedRequest, apply to state with started pending WFT, assert emitted event carries correct logical_seq, scheduled_event_id, started_event_id from state and failure_cause, failure_details, worker_identity from request
    - **Validates: Requirements 2.1.1**

  - [x] 6.4 Add `property_12_wft_timed_out_event_field_pass_through` property test
    - **Property 2: WFT TimedOut event field pass-through**
    - For all valid WorkflowTaskTimedOutRequest, apply to state with started pending WFT, assert emitted event carries correct logical_seq, scheduled_event_id, started_event_id from state and timeout_type from request
    - **Validates: Requirements 3.1.1**

  - [x] 6.5 Add `property_13_failure_timeout_preserve_pending_wft_identity` property test
    - **Property 3: Both commands preserve pending WFT identity**
    - For all valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, assert next_state.pending_workflow_task has same logical_seq and scheduled_event_id as input, started_event_id is None
    - **Validates: Requirements 2.1.2, 2.1.3, 3.1.2, 3.1.3, 4.1.1–4.1.4, 7.1.1**

  - [x] 6.6 Add `property_14_wft_failed_preserves_sticky` property test
    - **Property 4: WFT Failed preserves sticky affinity**
    - For all valid WorkflowTaskFailed transitions with optional sticky affinity, assert next_state.sticky equals input state's sticky, and dispatch op carries matching sticky_preferred
    - **Validates: Requirements 2.1.5, 4.2.1, 4.2.2, 7.3.1**

  - [x] 6.7 Add `property_15_wft_timed_out_clears_sticky` property test
    - **Property 5: WFT TimedOut clears sticky affinity**
    - For all valid WorkflowTaskTimedOut transitions with optional sticky affinity, assert next_state.sticky is None, and dispatch op carries sticky_preferred None
    - **Validates: Requirements 3.1.4, 3.1.5, 4.2.3, 4.2.4, 7.2.1**

  - [x] 6.8 Add `property_16_failure_timeout_minimal_side_effects` property test
    - **Property 6: Both commands produce minimal side effects**
    - For all valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, assert: exactly one history event of matching kind, exactly one EnqueueWorkflowTask dispatch op with correct logical_seq and QueueKey, empty request_dedupe_ops/activity_ops/timer_ops/projection_ops, status remains Running
    - **Validates: Requirements 2.1.4, 2.1.6–2.1.8, 3.1.5–3.1.8, 4.3.1–4.3.4, 5.3.3, 5.4.1–5.4.8, 7.4.1–7.4.2, 7.5.1–7.5.2**

- [x] 7. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel` and ensure all tests pass. Ask the user if questions arise.

## Notes

- All changes are additive — no existing call sites or tests break
- All test tasks are required (not optional) per Feature 1 review feedback
- Property tests extend existing `property_tests.rs`; golden tests extend existing `golden_tests.rs`
- No new test files are created
- Extending `arb_valid_pair()` automatically validates structural invariants (Properties 4–10) for the new commands
- Each property test references a specific design property and the requirements it validates
- Both request structs carry `logical_seq` and `started_event_id` for fencing against stale reports, matching the pattern used by `WorkflowTaskCompleted`
- Reset metadata (`base_run_id`, `new_run_id`, `fork_event_version`) is deliberately excluded — it will be added when Feature 10 (Reset) is specified
- `WorkflowTaskFailedCause` and `WorkflowTaskTimeoutType` are domain enums, not strings, to avoid stringly-typed contracts across crates
