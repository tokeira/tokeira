# Implementation Plan: Kernel Markers and Execution Options (Feature 8)

## Overview

Add RecordMarker (workflow command) and UpdateExecutionOptions (top-level command) to `tokeira-kernel`. Types first, then kernel logic, then downstream fixes, workspace compile checkpoint, then tests. `VersioningOverride` and `CompletionCallback` are placeholder empty structs. Tests extend existing `golden_tests.rs` and `property_tests.rs`.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add placeholder types and `WorkflowState` fields in `state.rs`
    - Add `VersioningOverride` and `CompletionCallback` as empty structs deriving `Clone, Debug, PartialEq`
    - Add `versioning_override: Option<VersioningOverride>` and `completion_callbacks: Vec<CompletionCallback>` fields to `WorkflowState`
    - _Requirements: 8.7.1, 8.7.2, 8.9.1, 8.9.2, 8.9.3_

  - [x] 1.2 Add `UpdateExecutionOptionsRequest` struct and `UpdateExecutionOptions` variant to `command.rs`
    - `UpdateExecutionOptionsRequest { versioning_override: FieldChange<VersioningOverride>, completion_callbacks: FieldChange<Vec<CompletionCallback>>, attached_request_id: Option<String>, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - Add `FieldChange<T>` enum with `Unchanged`, `Set(T)`, `Clear` variants, deriving `Clone, Debug, PartialEq`
    - Add `UpdateExecutionOptions(UpdateExecutionOptionsRequest)` variant to `Command` enum
    - Add `RecordMarker { marker_name: String, details: BTreeMap<String, Payloads>, failure: Option<Payload>, header: Option<BTreeMap<String, Payload>> }` variant to `WorkflowCommand` enum
    - _Requirements: 8.3.1, 8.6.1, 8.6.2, 8.8.1_

  - [x] 1.3 Add `HistoryEventKind` variants to `event.rs`
    - `MarkerRecorded { marker_name: String, details: BTreeMap<String, Payloads>, failure: Option<Payload>, header: Option<BTreeMap<String, Payload>> }`
    - `WorkflowExecutionOptionsUpdated { versioning_override: FieldChange<VersioningOverride>, completion_callbacks: FieldChange<Vec<CompletionCallback>>, attached_request_id: Option<String> }`
    - _Requirements: 8.2.1, 8.5.1_

- [x] 2. Implement kernel logic
  - [x] 2.1 Add `RecordMarker` match arm in `apply_workflow_command`
    - Emit `MarkerRecorded` event with all fields passed through, return `Ok(false)`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.2.2, 8.2.3, 8.3.2_

  - [x] 2.2 Add `apply_update_execution_options` method and `Command::UpdateExecutionOptions` match arm
    - New match arm in `BasicKernel::apply`: `Command::UpdateExecutionOptions(req) => self.apply_update_execution_options(loaded, req)`
    - `apply_update_execution_options` follows Signal pattern: `expect_open` → emit `RequestDedupeOp` → emit `WorkflowExecutionOptionsUpdated` → match on `versioning_override` (`Set` → set, `Clear` → None, `Unchanged` → skip) → match on `completion_callbacks` (`Set` → replace, `Clear` → empty, `Unchanged` → skip) → NO WFT scheduled → `finish`
    - _Requirements: 8.4.1–8.4.9, 8.5.2, 8.8.2_

  - [x] 2.3 Update `apply_start` initializer
    - Add `versioning_override: None` and `completion_callbacks: Vec::new()` to `WorkflowState` initializer
    - _Requirements: 8.7.3_

- [x] 3. Fix downstream breakage
  - [x] 3.1 Fix all exhaustive match arms on `Command`, `WorkflowCommand`, `HistoryEventKind` and all `WorkflowState` construction sites across the workspace
    - Search for exhaustive matches and struct literals that need the new variants/fields
    - Add wildcard or explicit arms as appropriate for test helpers, serialization, display, etc.
    - _Requirements: 8.10.1, 8.10.2, 8.10.3, 8.10.4, 8.10.5_

- [x] 4. Checkpoint — workspace compilation
  - Run `cargo check --workspace` and ensure zero errors. Ask the user if questions arise.

- [x] 5. Add golden tests to `golden_tests.rs`
  - [x] 5.1 Add `record_marker_happy_path` test
    - WFT completion with a single `RecordMarker`. Assert: `MarkerRecorded` event with correct fields, run still open, no extra dispatch/projection ops.
    - _Requirements: 8.1.1, 8.2.1, 8.2.2, 8.2.3_

  - [x] 5.2 Add `record_marker_after_close_rejected` test
    - WFT completion with `CompleteWorkflow` followed by `RecordMarker`. Assert: `Reject::CommandsAfterClose`.
    - _Requirements: 8.3.2_

  - [x] 5.3 Add `update_execution_options_happy_path` test
    - `UpdateExecutionOptions` against open state. Assert: `WorkflowExecutionOptionsUpdated` event, dedup op, state fields updated, no WFT scheduled.
    - _Requirements: 8.4.1, 8.4.2, 8.4.3, 8.4.5, 8.4.6, 8.4.7_

  - [x] 5.4 Add `update_execution_options_clear_versioning` test
    - `UpdateExecutionOptions` with `versioning_override: FieldChange::Clear` against state with existing versioning override. Assert: `versioning_override` is `None`.
    - _Requirements: 8.4.4_

  - [x] 5.5 Add `update_execution_options_missing_run` test
    - `UpdateExecutionOptions` against `LoadedRun::Absent`. Assert: `Reject::MissingRun`.
    - _Requirements: 8.4.8_

  - [x] 5.6 Add `update_execution_options_closed_run` test
    - `UpdateExecutionOptions` against closed state. Assert: `Reject::RunClosed`.
    - _Requirements: 8.4.9_

  - [x] 5.7 Add `close_preserves_execution_options` test
    - Terminate with `versioning_override` and `completion_callbacks` set. Assert: fields preserved in `next_state`.
    - _Requirements: 8.7.4_

- [x] 6. Add property tests to `property_tests.rs`
  - [x] 6.1 Extend `arb_valid_pair` strategy with RecordMarker and UpdateExecutionOptions arms
    - Add `Command::UpdateExecutionOptions` generation against open state
    - Add `RecordMarker` to WFT completed command generation
    - Add helper strategies: `arb_record_marker_command()`, `arb_update_execution_options_request(now)`
    - _Requirements: 8.1.1, 8.4.1, 8.8.1, 8.8.2_

  - [x] 6.2 Extend existing start property test for Property 6
    - Extend `property_1_start_field_pass_through` (or equivalent) to assert `next_state.versioning_override.is_none()` and `next_state.completion_callbacks.is_empty()`
    - **Property 6: Start initializes execution option fields**
    - **Validates: Requirements 8.7.3**

  - [x] 6.3 Add Property 1 test: RecordMarker event field pass-through
    - `proptest!` block: generate random `RecordMarker`, apply in WFT completion, assert `MarkerRecorded` event fields match command fields exactly
    - **Property 1: RecordMarker event field pass-through**
    - **Validates: Requirements 8.1.1, 8.2.3**

  - [x] 6.4 Add Property 2 test: RecordMarker is a pure event emission
    - `proptest!` block: generate random `RecordMarker`, compare state before/after (only `last_event_id` and `transition_seq` change), assert no extra dispatch/projection ops, returns `false` (run not closed)
    - **Property 2: RecordMarker is a pure event emission**
    - **Validates: Requirements 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6**

  - [x] 6.5 Add Property 3 test: UpdateExecutionOptions produces correct event and dedup op
    - `proptest!` block: generate random `UpdateExecutionOptionsRequest`, apply, assert event fields and dedup op match request
    - **Property 3: UpdateExecutionOptions produces correct event and dedup op**
    - **Validates: Requirements 8.4.1, 8.4.2**

  - [x] 6.6 Add Property 4 test: UpdateExecutionOptions state mutation
    - `proptest!` block: generate random `UpdateExecutionOptionsRequest`, apply, assert state fields updated correctly (set/unset versioning_override, replace completion_callbacks)
    - **Property 4: UpdateExecutionOptions state mutation**
    - **Validates: Requirements 8.4.3, 8.4.4, 8.4.5**

  - [x] 6.7 Add Property 5 test: UpdateExecutionOptions does not schedule WFT and does not close
    - `proptest!` block: generate random `UpdateExecutionOptionsRequest`, apply, assert no `DispatchOp::EnqueueWorkflowTask`, `pending_workflow_task` unchanged, run still open
    - **Property 5: UpdateExecutionOptions does not schedule WFT and does not close**
    - **Validates: Requirements 8.4.6, 8.4.7**

  - [x] 6.8 Add Property 7 test: Close preserves execution option metadata
    - `proptest!` block: set execution option fields on state, close via various paths (Terminate, WorkflowExecutionTimedOut, CompleteWorkflow, FailWorkflow, CancelWorkflow, ContinueAsNew), assert fields preserved in `next_state`
    - **Property 7: Close preserves execution option metadata**
    - **Validates: Requirements 8.7.4**

- [x] 7. Final checkpoint — all tests pass
  - Run `cargo test --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required (none marked optional) per user direction
- Property tests use `proptest! { }` block style, golden tests are individual `#[test]` functions
- Tests extend existing `golden_tests.rs` and `property_tests.rs` — no new test files
- Property 8 (structural invariants) is covered by extending `arb_valid_pair` in task 6.1 — existing structural property tests automatically cover the new command types
- `VersioningOverride` and `CompletionCallback` are placeholder empty structs
- `close()` does NOT need modification — these are metadata fields, not pending operations
