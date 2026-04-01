# Implementation Plan: Kernel Foundation + WFT Lifecycle

## Overview

Field additions and new enum variants to bring `tokeira-kernel` in line with the architecture spec, plus a comprehensive test suite. Some new fields are non-optional (`workflow_task_timeout: Duration`, `attempt: u32`), which means downstream call sites that construct kernel types will need updates. The implementation proceeds types-first, then kernel logic, then downstream fixes, then tests.

## Tasks

- [x] 1. Add `RetryPolicy` type to `tokeira-types`
  - Create `tokeira/crates/tokeira-types/src/retry.rs` with the `RetryPolicy` struct (fields: `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `non_retryable_error_types`)
  - Add `pub mod retry;` and `pub use retry::*;` to `tokeira/crates/tokeira-types/src/lib.rs`
  - _Requirements: 2.1.1, 2.2.1, 3.1.3_

- [x] 2. Add timeout, retry, and chain fields to `StartRequest` and `WorkflowState`
  - [x] 2.1 Add fields to `StartRequest` in `tokeira/crates/tokeira-kernel/src/command.rs`
    - Add `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `workflow_task_timeout: Duration`, `retry_policy: Option<RetryPolicy>`, `attempt: u32`, `continued_execution_run_id: Option<RunId>`, `first_execution_run_id: Option<RunId>`
    - _Requirements: 1.2.1, 1.2.2, 1.2.3, 2.2.1, 2.2.2, 3.2.1, 3.2.2_

  - [x] 2.2 Add fields to `WorkflowState` in `tokeira/crates/tokeira-kernel/src/state.rs`
    - Add `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `workflow_task_timeout: Duration`, `retry_policy: Option<RetryPolicy>`, `attempt: u32`
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 2.1.1, 2.1.2_

- [x] 3. Add timeout fields to activity-related structs and enums
  - [x] 3.1 Add timeout fields to `WorkflowCommand::ScheduleActivity` in `tokeira/crates/tokeira-kernel/src/command.rs`
    - Add `schedule_to_close_timeout: Option<Duration>`, `schedule_to_start_timeout: Option<Duration>`, `start_to_close_timeout: Option<Duration>`, `heartbeat_timeout: Option<Duration>`
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.1.4_

  - [x] 3.2 Add timeout fields to `ActivityState` in `tokeira/crates/tokeira-kernel/src/state.rs`
    - Add `schedule_to_close_timeout: Option<Duration>`, `schedule_to_start_timeout: Option<Duration>`, `start_to_close_timeout: Option<Duration>`, `heartbeat_timeout: Option<Duration>`
    - _Requirements: 5.3.1_

  - [x] 3.3 Add timeout fields to `DispatchOp::EnqueueActivityTask` in `tokeira/crates/tokeira-kernel/src/transition.rs`
    - Add `schedule_to_close_timeout: Option<Duration>`, `schedule_to_start_timeout: Option<Duration>`, `start_to_close_timeout: Option<Duration>`, `heartbeat_timeout: Option<Duration>`
    - _Requirements: 5.4.1_

- [x] 4. Add new event variants and fields to `HistoryEventKind` and `ActivityResolution`
  - [x] 4.1 Add fields to `WorkflowExecutionStarted` variant in `tokeira/crates/tokeira-kernel/src/event.rs`
    - Add `continued_execution_run_id: Option<RunId>`, `first_execution_run_id: Option<RunId>`, `retry_policy: Option<RetryPolicy>`, `attempt: u32`, `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `workflow_task_timeout: Duration`
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.1.5, 3.1.6, 3.1.7_

  - [x] 4.2 Add timeout fields to `ActivityTaskScheduled` variant in `tokeira/crates/tokeira-kernel/src/event.rs`
    - Add `schedule_to_close_timeout: Option<Duration>`, `schedule_to_start_timeout: Option<Duration>`, `start_to_close_timeout: Option<Duration>`, `heartbeat_timeout: Option<Duration>`
    - _Requirements: 5.2.1_

  - [x] 4.3 Add `ActivityTaskTimedOut` and `ActivityTaskCanceled` variants to `HistoryEventKind` in `tokeira/crates/tokeira-kernel/src/event.rs`
    - `ActivityTaskTimedOut { activity_id: String, timeout_type: String }`
    - `ActivityTaskCanceled { activity_id: String, details: Option<Payloads> }`
    - _Requirements: 4.2.1, 4.2.2_

  - [x] 4.4 Add `TimedOut` and `Canceled` variants to `ActivityResolution` in `tokeira/crates/tokeira-kernel/src/event.rs`
    - `TimedOut { timeout_type: String }`
    - `Canceled { details: Option<Payloads> }`
    - _Requirements: 4.1.1, 4.1.2_

- [x] 5. Checkpoint — Ensure all type changes compile
  - Ensure `cargo check -p tokeira-kernel -p tokeira-types` passes. Ask the user if questions arise.

- [x] 6. Update kernel logic to use new fields
  - [x] 6.1 Update `apply_start` in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Copy new timeout, retry, attempt, and chain metadata fields from `StartRequest` into `WorkflowState` initializer
    - Copy new fields into `WorkflowExecutionStarted` event emission
    - _Requirements: 1.1.4, 1.2.4, 2.1.3, 2.2.3, 3.1.8, 3.2.3_

  - [x] 6.2 Update `apply_activity_resolved` in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Add match arms for `ActivityResolution::TimedOut` and `ActivityResolution::Canceled`
    - Emit `ActivityTaskTimedOut` and `ActivityTaskCanceled` events respectively
    - Existing post-match logic (remove activity, push Delete op, schedule WFT) applies to all variants
    - _Requirements: 4.1.3, 4.1.4, 4.1.5, 4.1.6_

  - [x] 6.3 Update `apply_workflow_command` for `ScheduleActivity` in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Destructure new timeout fields from `ScheduleActivity` variant
    - Pass timeout fields through to `ActivityTaskScheduled` event, `ActivityState`, and `DispatchOp::EnqueueActivityTask`
    - _Requirements: 5.1.5, 5.1.6, 5.2.2, 5.3.2, 5.4.2_

- [x] 7. Checkpoint — Ensure kernel logic compiles and existing behavior is preserved
  - Ensure `cargo check -p tokeira-kernel` passes. Ask the user if questions arise.

- [x] 7.5. Update downstream call sites for new non-optional fields
  - [x] 7.5.1 Update `tokeira-edge` call sites that construct `StartRequest` (e.g., `to_internal.rs`)
    - Add `workflow_task_timeout`, `attempt`, and other new fields with appropriate values
  - [x] 7.5.2 Update test helpers in `tokeira-edge` (e.g., `grpc_properties.rs`) that construct kernel types
    - Add new fields to struct literals
  - [x] 7.5.3 Ensure `cargo check --workspace` passes after all call-site updates

- [x] 8. Add `proptest` dev-dependency and create property test infrastructure
  - [x] 8.1 Add `proptest = "1.4"` to `[dev-dependencies]` in `tokeira/crates/tokeira-kernel/Cargo.toml`
    - _Requirements: 6.1.2_

  - [x] 8.2 Create `tokeira/crates/tokeira-kernel/tests/property_tests.rs` with `Arbitrary` strategy helpers
    - Implement `arb_valid_pair() -> impl Strategy<Value = (LoadedRun, Command)>` with sub-strategies for each command variant
    - Implement `arb_retry_policy()`, `arb_workflow_state()`, `arb_activity_resolution()` strategies
    - Each sub-strategy generates a `WorkflowState` in the right shape for its command variant, then generates a matching command
    - _Requirements: 6.1.2_

- [x] 9. Implement property tests (Properties 1–3: targeted pass-through properties)
  - [x] 9.1 Write property test for Property 1: Start field pass-through
    - **Property 1: Start field pass-through**
    - Generate arbitrary `StartRequest` applied to `LoadedRun::Absent`, assert `next_state` timeout/retry/attempt fields match `StartRequest`, and `WorkflowExecutionStarted` event carries matching chain metadata, retry, timeout, and attempt fields
    - **Validates: Requirements 1.1.4, 1.2.4, 2.1.3, 2.2.3, 3.1.8, 3.2.3**

  - [x] 9.2 Write property test for Property 2: Activity resolution event matches variant
    - **Property 2: Activity resolution event matches resolution variant**
    - Generate arbitrary open run with activity + arbitrary `ActivityResolution` variant, assert transition contains exactly one activity-terminal event matching the resolution variant and fields
    - **Validates: Requirements 4.1.3, 4.1.4**

  - [x] 9.3 Write property test for Property 3: ScheduleActivity timeout pass-through
    - **Property 3: ScheduleActivity timeout pass-through**
    - Generate arbitrary `ScheduleActivity` with timeout values in a `WorkflowTaskCompleted`, assert `ActivityTaskScheduled` event, `ActivityState`, and `DispatchOp::EnqueueActivityTask` all carry identical timeout fields
    - **Validates: Requirements 5.1.5, 5.1.6, 5.2.2, 5.3.2, 5.4.2**

- [x] 10. Implement property tests (Properties 4–10: universal invariants)
  - [x] 10.1 Write property test for Property 4: Event ID contiguity
    - **Property 4: Event ID contiguity**
    - For any valid `(LoadedRun, Command)` pair, assert `history_events` have contiguous event IDs starting from input `last_event_id + 1`
    - **Validates: Requirements 6.1.1**

  - [x] 10.2 Write property test for Property 5: Transition sequence increment
    - **Property 5: Transition sequence increment**
    - For any valid pair, assert `expected_seq` equals input `transition_seq` and `next_state.transition_seq` equals `expected_seq + 1`
    - **Validates: Requirements 6.2.1, 6.2.2, 8.1.4, 8.1.5**

  - [x] 10.3 Write property test for Property 6: Pending WFT identity preservation
    - **Property 6: Pending WFT identity preservation**
    - For any valid pair where the input state already has a pending WFT with logical_seq S and the command is a WFT-triggering command (Signal, ActivityResolved, TimerDue), assert that next_state.pending_workflow_task has the same logical_seq S AND no EnqueueWorkflowTask dispatch op is emitted. Additionally, for all transitions, assert next_state has at most one PendingWorkflowTask.
    - **Validates: Requirements 6.3.1, 8.3.1, 8.3.2**

  - [x] 10.4 Write property test for Property 7: Closed workflow no-schedule
    - **Property 7: Closed workflow no-schedule**
    - For any transition where `next_state.status != Running`, assert no `EnqueueWorkflowTask`/`EnqueueActivityTask` in dispatch_ops, `pending_workflow_task` is `None`, and `closed_at` is `Some`
    - **Validates: Requirements 6.4.1, 6.4.2, 6.4.3**

  - [x] 10.5 Write property test for Property 8: Last event ID consistency
    - **Property 8: Last event ID consistency**
    - For any valid pair, assert `next_state.last_event_id` equals last event's `event_id` (or input's `last_event_id` if no events)
    - **Validates: Requirements 6.5.1, 6.5.2**

  - [x] 10.6 Write property test for Property 9: ActivityOp and TimerOp consistency
    - **Property 9: ActivityOp and TimerOp consistency**
    - For any transition, assert every `Upsert` has a matching entry in `next_state` and every `Delete` has no matching entry
    - **Validates: Requirements 6.6.1, 6.6.2, 6.7.1, 6.7.2**

  - [x] 10.7 Write property test for Property 10: Request dedup correctness
    - **Property 10: Request dedup correctness**
    - For Start/Signal commands, assert exactly one `RequestDedupeOp`; for all other commands, assert empty `request_dedupe_ops`
    - **Validates: Requirements 8.2.1, 8.2.2**

- [x] 11. Create golden transition test helpers and success-path tests
  - [x] 11.1 Create `tokeira/crates/tokeira-kernel/tests/golden_tests.rs` with test helper module
    - Implement `helpers::make_start_request()`, `helpers::make_open_state()`, `helpers::make_open_state_with_pending_wft()`, `helpers::make_open_state_with_started_wft()`, `helpers::make_open_state_with_activity(id)`, `helpers::make_open_state_with_timer(id)`, `helpers::make_closed_state()`
    - All helpers populate the new timeout/retry/chain fields with sensible defaults
    - _Requirements: 7.1 through 7.10_

  - [x] 11.2 Write golden tests for success paths (11 tests)
    - Start from Absent (assert new timeout/retry fields) — _Requirement 7.1_
    - Signal with no pending WFT — _Requirement 7.2_
    - Signal with pending WFT — _Requirement 7.3_
    - WorkflowTaskStarted with sticky — _Requirement 7.4_
    - WorkflowTaskCompleted with ScheduleActivity + StartTimer (assert timeout fields) — _Requirement 7.5_
    - WorkflowTaskCompleted with CompleteWorkflow — _Requirement 7.6_
    - WorkflowTaskCompleted with FailWorkflow — _Requirement 7.7_
    - ActivityResolved Completed — _Requirement 7.8_
    - ActivityResolved TimedOut (new variant) — _Requirement 7.8_
    - ActivityResolved Canceled (new variant) — _Requirement 7.8_
    - TimerDue — _Requirement 7.9_

  - [x] 11.3 Write golden tests for rejection paths (15 tests)
    - Start on Existing → `RunAlreadyExists` — _Requirement 7.10.1_
    - Signal on Absent → `MissingRun` — _Requirement 7.10.2_
    - Signal on closed → `RunClosed` — _Requirement 7.10.3_
    - WFT Started no pending → `NoPendingWorkflowTask` — _Requirement 7.10.4_
    - WFT Started seq mismatch → `WorkflowTaskSeqMismatch` — _Requirement 7.10.5_
    - WFT Started already started → `WorkflowTaskAlreadyStarted` — _Requirement 7.10.6_
    - WFT Completed no pending → `NoPendingWorkflowTask` — _Requirement 7.10.7_
    - WFT Completed not started → `WorkflowTaskNotStarted` — _Requirement 7.10.8_
    - WFT Completed seq mismatch → `WorkflowTaskSeqMismatch` — _Requirement 7.10.9_
    - WFT Completed token mismatch → `WorkflowTaskTokenMismatch` — _Requirement 7.10.10_
    - ScheduleActivity duplicate → `DuplicateActivityId` — _Requirement 7.10.11_
    - StartTimer duplicate → `DuplicateTimerId` — _Requirement 7.10.12_
    - ActivityResolved unknown → `UnknownActivity` — _Requirement 7.10.13_
    - TimerDue unknown → `UnknownTimer` — _Requirement 7.10.14_
    - Commands after close → `CommandsAfterClose` — _Requirement 7.10.15_

- [x] 12. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel` and ensure all tests pass. Ask the user if questions arise.

## Notes

- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design document
- Golden tests pin exact transition output for regression detection
- Some type changes introduce non-optional fields that require downstream call-site updates (Task 7.5)
- The `RetryPolicy` type lives in `tokeira-types` since it's shared across kernel, runtime, and edge crates
