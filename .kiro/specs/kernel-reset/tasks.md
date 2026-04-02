# Implementation Plan: Kernel Reset (Feature 10)

## Overview

Add Reset command support to `tokeira-kernel` — the final kernel feature (10 of 10). Types first (`ResetRequest`, `Command::Reset`, `WorkflowTaskFailedCause::ResetWorkflow`, `Reject::ResetConstraintViolation`, `WorkflowTaskFailed` event gains 3 optional fields), then kernel logic (`apply_reset` following Terminate pattern), then downstream fixes (all existing `WorkflowTaskFailed` construction sites provide `None` for new fields), workspace compile checkpoint, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add `ResetRequest` struct and `Command::Reset` variant to `command.rs`
    - `ResetRequest { fork_event_id: i64, new_run_id: RunId, reason: String, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - Add `Reset(ResetRequest)` variant to `Command` enum
    - _Requirements: 1.1.1–1.1.7, 1.2.1_

  - [x] 1.2 Add `ResetWorkflow` variant to `WorkflowTaskFailedCause` in `command.rs`
    - Add `ResetWorkflow` variant to the `WorkflowTaskFailedCause` enum
    - _Requirements: 1.3.1_

  - [x] 1.3 Add 4 optional reset metadata fields to `WorkflowTaskFailed` in `event.rs`
    - Add `base_run_id: Option<RunId>`, `new_run_id: Option<RunId>`, `fork_event_version: Option<i64>`, `fork_event_id: Option<i64>` to `HistoryEventKind::WorkflowTaskFailed`
    - _Requirements: 1.4.1–1.4.4_

  - [x] 1.4 Add `ResetConstraintViolation` variant to `Reject` in `kernel.rs`
    - `ResetConstraintViolation { reason: String }` with `#[error("reset constraint violation: {reason}")]`
    - _Requirements: 1.5.1_

- [x] 2. Implement kernel logic
  - [x] 2.1 Add `apply_reset` method and `Command::Reset` match arm in `kernel.rs`
    - New match arm in `BasicKernel::apply`: `Command::Reset(req) => self.apply_reset(loaded, req)`
    - `apply_reset` follows Terminate pattern: `expect_open` → validate `fork_event_id` in `[1, last_event_id]` (reject `ResetConstraintViolation`) → determine `scheduled_event_id`/`started_event_id` from pending WFT (or 0/0 sentinels) → `logical_seq = state.next_workflow_task_seq` → emit `RequestDedupeOp` → emit `WorkflowTaskFailed` with `ResetWorkflow` cause and reset metadata (`base_run_id: Some(run_id)`, `new_run_id: Some(req.new_run_id)`, `fork_event_id: Some(req.fork_event_id)`, `fork_event_version: None`) → `close(Terminated)` → `std::mem::take` activities/timers + emit Delete ops → `apply_parent_close_policy` → `finish`
    - _Requirements: 2.1.1–2.1.5, 2.2.1–2.2.6, 2.3.1–2.3.4, 2.4.1–2.4.4, 3.1.1–3.1.2, 4.1.1–4.1.2_

- [x] 3. Fix downstream breakage
  - [x] 3.1 Update all existing `WorkflowTaskFailed` event construction sites to provide `None` for the 4 new fields
    - `apply_workflow_task_failed` in `kernel.rs`: add `base_run_id: None, new_run_id: None, fork_event_version: None, fork_event_id: None`
    - Any pattern matches on `WorkflowTaskFailed` in test files must include the new fields
    - _Requirements: 1.4.5, 5.1.3, 6.8.1_

  - [x] 3.2 Update exhaustive matches on `WorkflowTaskFailedCause`, `Command`, and `Reject` across the workspace
    - `WorkflowTaskFailedCause` matches: add `ResetWorkflow` arm
    - `Command` matches: already handled by 2.1 match arm addition
    - `Reject` matches: add `ResetConstraintViolation` arm
    - _Requirements: 5.1.1, 5.1.2, 5.1.4_

- [x] 4. Checkpoint — workspace compilation
  - Run `cargo check --workspace` and ensure zero errors. Fix any additional compile failures discovered. Ask the user if questions arise.
  - _Requirements: 5.1.5_

- [x] 5. Add golden tests to `golden_tests.rs`
  - [x] 5.1 Add `reset_happy_path_no_pending_wft` test
    - Reset against open run with no pending WFT. Assert: `WorkflowTaskFailed` event with `ResetWorkflow` cause, `scheduled_event_id=0`, `started_event_id=0`, `base_run_id=Some(run_id)`, `new_run_id=Some(req.new_run_id)`, `fork_event_version=None`, status=Terminated, closed_at=Some, one RequestDedupeOp, no EnqueueWorkflowTask dispatch ops.
    - _Requirements: 2.1.1–2.1.3, 2.2.3, 2.2.4, 2.2.5, 2.2.6, 6.6.1, 6.7.1–6.7.3_

  - [x] 5.2 Add `reset_happy_path_with_started_wft` test
    - Reset against open run with pending started WFT. Assert: `WorkflowTaskFailed` event references pending WFT's scheduled/started event IDs.
    - _Requirements: 2.2.1_

  - [x] 5.3 Add `reset_happy_path_with_scheduled_wft` test
    - Reset against open run with pending scheduled-but-not-started WFT. Assert: `WorkflowTaskFailed` event uses pending WFT's scheduled_event_id and `started_event_id=0`.
    - _Requirements: 2.2.2_

  - [x] 5.4 Add `reset_cleans_up_activities_and_timers` test
    - Reset against run with open activities and timers. Assert: `ActivityOp::Delete` and `TimerOp::Delete` for each, maps empty in next_state.
    - _Requirements: 2.3.1, 2.3.2, 6.4.1–6.4.4_

  - [x] 5.5 Add `reset_applies_parent_close_policy` test
    - Reset against run with open children. Assert: appropriate `DispatchOp::TerminateChild`/`CancelChild` ops, children map empty.
    - _Requirements: 2.3.3_

  - [x] 5.6 Add `reset_rejects_fork_event_id_zero` test
    - Assert: `Reject::ResetConstraintViolation`.
    - _Requirements: 2.4.1_

  - [x] 5.7 Add `reset_rejects_fork_event_id_negative` test
    - Assert: `Reject::ResetConstraintViolation`.
    - _Requirements: 2.4.1_

  - [x] 5.8 Add `reset_rejects_fork_event_id_exceeds_last` test
    - Assert: `Reject::ResetConstraintViolation`.
    - _Requirements: 2.4.2_

  - [x] 5.9 Add `reset_accepts_fork_event_id_one` test
    - Assert: Ok transition (boundary).
    - _Requirements: 2.4.3_

  - [x] 5.10 Add `reset_accepts_fork_event_id_equals_last` test
    - Assert: Ok transition (boundary).
    - _Requirements: 2.4.4_

  - [x] 5.11 Add `reset_rejects_absent_run` test
    - Assert: `Reject::MissingRun`.
    - _Requirements: 3.1.1_

  - [x] 5.12 Add `reset_rejects_closed_run` test
    - Assert: `Reject::RunClosed`.
    - _Requirements: 3.1.2_

- [x] 6. Add property tests to `property_tests.rs`
  - [x] 6.1 Extend `arb_valid_pair` with Reset arm and `arb_wft_failed_cause` with ResetWorkflow
    - Add `Command::Reset` with valid state/request pair to `arb_valid_pair` — ensures existing structural property tests (event ID contiguity, transition_seq increment) automatically cover Reset
    - Add `ResetWorkflow` variant to `arb_wft_failed_cause`
    - Add helper strategies: `arb_reset_request(state, now)` (fork_event_id in [1, last_event_id]), `arb_open_state_for_reset(now)` (open state with last_event_id >= 1 and varying entities)
    - _Requirements: 6.1.1–6.1.2, 6.2.1_

  - [x] 6.2 Add Property 1 test: Reset closes the run with terminal state invariants
    - `proptest!` block: generate random open state with entities, apply Reset with valid fork_event_id, assert status=Terminated, closed_at=Some, pending_workflow_task=None, sticky=None, all entity maps empty
    - **Property 1: Reset closes the run with terminal state invariants**
    - **Validates: Requirements 2.1.3, 2.3.4, 6.3.1–6.3.11, 7.1.1**

  - [x] 6.3 Add Property 2 test: Reset entity cleanup ops match input state
    - `proptest!` block: generate random open state with N activities and M timers, apply Reset, assert activity_ops has N Deletes, timer_ops has M Deletes, all IDs match input state
    - **Property 2: Reset entity cleanup ops match input state**
    - **Validates: Requirements 2.3.1, 2.3.2, 6.4.1–6.4.4, 7.2.1**

  - [x] 6.4 Add Property 3 test: Reset emits exactly one RequestDedupeOp
    - `proptest!` block: generate random valid Reset, assert exactly one RequestDedupeOp with correct request_id
    - **Property 3: Reset emits exactly one RequestDedupeOp**
    - **Validates: Requirements 2.1.1, 6.5.1, 7.3.1**

  - [x] 6.5 Add Property 4 test: Reset fork_event_id validation rejects invalid values
    - `proptest!` block: generate random open state and invalid fork_event_id (<=0 or >last_event_id), assert Err(Reject::ResetConstraintViolation)
    - **Property 4: Reset fork_event_id validation rejects invalid values**
    - **Validates: Requirements 2.4.1, 2.4.2, 7.4.1**

  - [x] 6.6 Add Property 5 test: Reset WorkflowTaskFailed event carries correct metadata
    - `proptest!` block: generate random valid Reset, assert WorkflowTaskFailed event has failure_cause=ResetWorkflow, base_run_id=Some(input run_id), new_run_id=Some(req.new_run_id), fork_event_version=None, failure_details=None, logical_seq=input next_workflow_task_seq, identity=WorkerIdentity("reset")
    - **Property 5: Reset WorkflowTaskFailed event carries correct metadata**
    - **Validates: Requirements 1.4.5, 2.1.2, 2.2.4–2.2.6, 6.7.1–6.7.3, 7.5.1**

  - [x] 6.7 Add Property 6 test: Reset emits no WFT dispatch ops
    - `proptest!` block: generate random valid Reset, assert dispatch_ops contains no EnqueueWorkflowTask
    - **Property 6: Reset emits no WFT dispatch ops**
    - **Validates: Requirements 6.6.1, 7.6.1**

  - [x] 6.8 Add Property 7 test: Regular WorkflowTaskFailed events carry no reset metadata
    - `proptest!` block: generate random WFT failure (non-reset), assert base_run_id=None, new_run_id=None, fork_event_version=None on emitted WorkflowTaskFailed event
    - **Property 7: Regular WorkflowTaskFailed events carry no reset metadata**
    - **Validates: Requirements 1.4.4, 6.8.1**

- [x] 7. Final checkpoint — all tests pass
  - Run `cargo test --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required (none marked optional) per user direction
- Property tests use `proptest! { }` block style, golden tests are individual `#[test]` functions
- Tests extend existing `golden_tests.rs` and `property_tests.rs` — no new test files
- Structural invariants (event ID contiguity, transition_seq increment) are covered by extending `arb_valid_pair` in task 6.1 — existing structural property tests automatically cover Reset
- The `WorkflowTaskFailed` event modification is a breaking change — task 3.1 must update all construction sites before the workspace will compile
