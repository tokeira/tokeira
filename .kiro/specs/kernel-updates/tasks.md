# Implementation Plan: Kernel Updates (Feature 7)

## Overview

Add Temporal's Update feature to `tokeira-kernel`. This modifies durable `WorkflowState` (new `pending_updates` field) and the shared `close()` method, plus adds new types and kernel logic. Downstream crates need exhaustive match updates and `WorkflowState` construction site fixes. Types first, then kernel logic, then downstream fixes, compile checkpoint, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add `PendingUpdate` struct to `state.rs` and `pending_updates: BTreeMap<String, PendingUpdate>` field to `WorkflowState`
    - `PendingUpdate { update_id: String, accepted_event_id: i64, name: String }` with `Clone, Debug, PartialEq`
    - Field goes after `pending_external_cancels`
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.2.1_

  - [x] 1.2 Add `UpdateRequest` struct and `Update` variant to `command.rs`
    - `UpdateRequest { update_id: String, update_name: String, input: Payloads, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - Add `Update(UpdateRequest)` variant to `Command` enum
    - Add `UpdateCompleted { update_id: String, result: Payloads }`, `UpdateRejected { update_id: String, failure: String }`, `ProtocolMessage { message_id: String, body: UpdateProtocolBody }` variants to `WorkflowCommand` enum
    - Add `UpdateProtocolBody` enum with `Accepted { update_id, update_name, input }`, `Completed { update_id, result }`, `Rejected { update_id, failure }` variants, deriving `Clone, Debug, PartialEq`
    - _Requirements: 1.3.1–1.3.6, 1.4.1, 1.5.1, 1.5.2, 1.5.3_

  - [x] 1.3 Add `HistoryEventKind` variants to `event.rs` and `Reject` variant to `kernel.rs`
    - `WorkflowExecutionUpdateAccepted { update_id: String, update_name: String, input: Payloads }`
    - `WorkflowExecutionUpdateCompleted { update_id: String, result: Payloads }`
    - `WorkflowExecutionUpdateRejected { update_id: String, failure: String }`
    - Add `UnknownUpdate(String)` to `Reject` enum with `#[error("unknown update: {0}")]`
    - Add `DuplicateUpdateId(String)` to `Reject` enum with `#[error("duplicate update id: {0}")]`
    - _Requirements: 1.6.1, 1.6.2, 1.6.3, 1.7.1, 2.3.1_

- [x] 2. Implement kernel logic
  - [x] 2.1 Add `apply_update` method and `Command::Update` match arm in `kernel.rs`
    - New match arm in `BasicKernel::apply`: `Command::Update(req) => self.apply_update(loaded, req)`
    - `apply_update` follows Signal pattern: `expect_open` → check `pending_updates` for duplicate `update_id` (reject `DuplicateUpdateId` if present) → emit `RequestDedupeOp` → emit `WorkflowExecutionUpdateAccepted` → insert `PendingUpdate` → coalesce WFT → `finish`
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 2.1.5, 2.2.1, 2.2.2, 2.2.3, 2.3.1, 6.1.1, 6.1.2_

  - [x] 2.2 Add `UpdateCompleted`, `UpdateRejected`, `ProtocolMessage` match arms in `apply_workflow_command`
    - `UpdateCompleted`: lookup in `pending_updates`, reject `UnknownUpdate` if missing, emit `WorkflowExecutionUpdateCompleted`, remove from map, return `Ok(false)`
    - `UpdateRejected`: same pattern, emit `WorkflowExecutionUpdateRejected`
    - `ProtocolMessage`: match on `body` variant — `Accepted` → check duplicate, emit `WorkflowExecutionUpdateAccepted`, insert `PendingUpdate`; `Completed` → lookup in `pending_updates`, reject `UnknownUpdate` if missing, emit `WorkflowExecutionUpdateCompleted`, remove; `Rejected` → same pattern, emit `WorkflowExecutionUpdateRejected`. Return `Ok(false)` in all cases.
    - _Requirements: 3.1.1–3.1.4, 3.2.1, 4.1.1–4.1.4, 4.2.1, 5.1.1–5.1.10, 5.2.1, 6.2.1, 6.2.2, 6.2.3_

  - [x] 2.3 Extend `TransitionBuilder::close()` and `apply_start` initializer
    - Add `self.state.pending_updates.clear();` to `close()` after `pending_external_cancels.clear()`
    - Add `pending_updates: BTreeMap::new()` to `WorkflowState` initializer in `apply_start`
    - _Requirements: 1.2.2, 7.1.1–7.1.7, 9.1.2, 9.1.3_

- [x] 3. Fix downstream breakage
  - [x] 3.1 Fix all exhaustive match arms on `Command`, `WorkflowCommand`, `HistoryEventKind`, `Reject`, and all `WorkflowState` construction sites across the workspace
    - Search for exhaustive matches and struct literals that need the new variants/field
    - Add wildcard or explicit arms as appropriate for test helpers, serialization, display, etc.
    - _Requirements: 9.1.1_

- [x] 4. Checkpoint — workspace compilation
  - Run `cargo check --workspace` and ensure zero errors. Ask the user if questions arise.

- [x] 5. Add golden tests to `golden_tests.rs`
  - [x] 5.1 Add `update_with_no_pending_wft` test
    - Update against open state, no WFT pending. Assert: UpdateAccepted event, WFT scheduled, dedup op, PendingUpdate in state.
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4_

  - [x] 5.2 Add `update_with_pending_wft` test
    - Update against open state with existing WFT. Assert: UpdateAccepted event, no new WFT, dedup op, PendingUpdate in state.
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.5_

  - [x] 5.3 Add `update_rejected_missing_run`, `update_rejected_closed_run`, and `update_duplicate_update_id` tests
    - Assert `Reject::MissingRun`, `Reject::RunClosed`, and `Reject::DuplicateUpdateId` respectively.
    - _Requirements: 2.2.1, 2.2.2, 2.2.3_

  - [x] 5.4 Add `update_completed_happy_path` test
    - WFT completion with `UpdateCompleted` for known pending update. Assert: UpdateCompleted event, update removed from pending, run still open.
    - _Requirements: 3.1.1, 3.1.2, 3.1.3_

  - [x] 5.5 Add `update_rejected_happy_path` test
    - WFT completion with `UpdateRejected` for known pending update. Assert: UpdateRejected event, update removed from pending, run still open.
    - _Requirements: 4.1.1, 4.1.2, 4.1.3_

  - [x] 5.6 Add `update_completed_unknown_update` and `update_rejected_unknown_update` tests
    - Assert `Reject::UnknownUpdate` for both.
    - _Requirements: 3.2.1, 4.2.1_

  - [x] 5.7 Add `protocol_message_accepted_body` test
    - WFT completion with `ProtocolMessage { body: Accepted { .. } }`. Assert: UpdateAccepted event emitted, PendingUpdate added, run still open.
    - [x] 5.7a Add `protocol_message_completed_body` test
    - WFT completion with `ProtocolMessage { body: Completed { .. } }` for a known pending update. Assert: UpdateCompleted event emitted, update removed from pending.
    - [x] 5.7b Add `protocol_message_rejected_body` test
    - WFT completion with `ProtocolMessage { body: Rejected { .. } }` for a known pending update. Assert: UpdateRejected event emitted, update removed from pending.
    - _Requirements: 5.1.6, 5.1.7, 5.1.8, 5.1.9, 5.1.10_

  - [x] 5.8 Add `terminate_clears_pending_updates` and `complete_workflow_clears_pending_updates` tests
    - Assert `next_state.pending_updates` is empty after close.
    - _Requirements: 7.1.1, 7.1.3_

- [x] 6. Add property tests to `property_tests.rs`
  - [x] 6.1 Extend `arb_valid_pair` strategy with Update arms
    - Add `Command::Update` generation (with and without pending WFT)
    - Add `UpdateCompleted`, `UpdateRejected`, `ProtocolMessage` to WFT completed command generation
    - Add helper strategies: `arb_update_request(now)`, `arb_update_completed_command()`, `arb_update_rejected_command()`, `with_pending_update(state, update_id)`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.2.1, 8.6.1, 8.6.2_

  - [x] 6.2 Add Property 1 assertion to existing start test
    - Extend `property_1_start_field_pass_through` (or equivalent) to assert `next_state.pending_updates.is_empty()`
    - **Property 1: Start initializes pending_updates to empty**
    - **Validates: Requirements 1.2.2, 9.1.2**

  - [x] 6.3 Add Property 2 test: Update acceptance
    - `proptest!` block: generate random `UpdateRequest`, apply against open state, assert dedup op, UpdateAccepted event fields, PendingUpdate entry with correct accepted_event_id and name
    - **Property 2: Update acceptance produces correct event, dedup op, and pending entry**
    - **Validates: Requirements 2.1.1, 2.1.2, 2.1.3, 8.4.1, 8.6.1**

  - [x] 6.4 Add Property 3 test: Update WFT coalescing
    - `proptest!` block: generate random `UpdateRequest` against states with/without pending WFT, assert WFT scheduled iff none was pending
    - **Property 3: Update WFT coalescing**
    - **Validates: Requirements 2.1.4, 2.1.5, 8.3.1, 8.3.2**

  - [x] 6.5 Add Property 4 test: UpdateCompleted removes pending and emits event
    - `proptest!` block: generate random `UpdateCompleted` against state with pending update, assert event fields, removal from map, run still open
    - **Property 4: UpdateCompleted removes pending update and emits correct event**
    - **Validates: Requirements 3.1.1, 3.1.2, 3.1.3, 8.4.2**

  - [x] 6.6 Add Property 5 test: UpdateRejected removes pending and emits event
    - `proptest!` block: generate random `UpdateRejected` against state with pending update, assert event fields, removal from map, run still open
    - **Property 5: UpdateRejected removes pending update and emits correct event**
    - **Validates: Requirements 4.1.1, 4.1.2, 4.1.3, 8.4.3**

  - [x] 6.7 Add Property 6 test: ProtocolMessage emits correct event per body variant
    - `proptest!` block: generate `ProtocolMessage` with random `UpdateProtocolBody` variant in WFT completion, assert correct event emitted and correct state change (Accepted → add pending, Completed/Rejected → remove pending)
    - **Property 6: ProtocolMessage emits the correct event based on body variant**
    - **Validates: Requirements 5.1.6, 5.1.7, 5.1.8, 5.1.9, 5.1.10**

- [x] 7. Extend existing close property tests for Property 7
  - Extend existing close property tests to include states with `pending_updates` populated. Assert `next_state.pending_updates.is_empty()` for all close paths.
  - **Property 7: Close clears pending_updates with no dispatch ops for cleared entries**
  - **Validates: Requirements 7.1.1–7.1.7, 8.5.1, 9.1.3**

- [x] 8. Final checkpoint — all tests pass
  - Run `cargo test --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required (none marked optional) per user direction
- Property tests use `proptest! { }` block style, golden tests are individual `#[test]` functions
- Tests extend existing `golden_tests.rs` and `property_tests.rs` — no new test files
- Property 8 (structural invariants) is covered by extending `arb_valid_pair` in task 6.1 — existing structural property tests automatically cover update transitions
