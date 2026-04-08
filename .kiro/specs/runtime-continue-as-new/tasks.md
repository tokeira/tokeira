# Implementation Plan: Continue-As-New

## Overview

Implement the runtime continue-as-new feature in three layers: (1) extend kernel data models with `first_run_started_at` and `first_execution_run_id` on `WorkflowState`, (2) update the runtime's timeout tracking and evaluation to be chain-aware, and (3) wire the lane post-commit path to detect `ContinuedAsNew` status, construct the successor `StartRequest`, and submit it. All property tests are required.

## Tasks

- [x] 1. Extend kernel data models with chain-aware fields
  - [x] 1.1 Add `first_run_started_at: Option<OffsetDateTime>` to `StartRequest` in `tokeira/crates/tokeira-kernel/src/command.rs`
    - Add the field after `parent_workflow_id`
    - Add doc comment: wall-clock `started_at` of the very first run in the execution chain
    - _Requirements: 4.1_

  - [x] 1.2 Add `first_run_started_at: Option<OffsetDateTime>` and `first_execution_run_id: Option<RunId>` to `WorkflowState` in `tokeira/crates/tokeira-kernel/src/state.rs`
    - Add both fields near the existing chain identity fields (`continued_execution_run_id` is on `StartRequest` only; `first_execution_run_id` needs to be on state for successor construction)
    - _Requirements: 4.2, 3.1, 3.2_

  - [x] 1.3 Update `apply_start` in `tokeira/crates/tokeira-kernel/src/kernel.rs` to populate `first_run_started_at` and `first_execution_run_id` from `StartRequest`
    - Set `initial.first_run_started_at = req.first_run_started_at`
    - Set `initial.first_execution_run_id = req.first_execution_run_id`
    - _Requirements: 4.2, 3.1_

  - [x] 1.4 Fix all compilation errors from the new fields
    - Add `first_run_started_at: None` to all existing `StartRequest` construction sites across the codebase (edge, runtime, tests)
    - Add `first_run_started_at: None` and `first_execution_run_id: None` to all existing `WorkflowState` construction sites (test helpers like `sample_state`)
    - _Requirements: 4.1, 4.2_

  - [x] 1.5 Write property test for `apply_start` populating `first_run_started_at`
    - **Property 4: apply_start populates first_run_started_at**
    - For any `StartRequest` with arbitrary `first_run_started_at` (including `None`), verify the resulting `WorkflowState.first_run_started_at` equals the request's value
    - Also verify `first_execution_run_id` is propagated
    - Place in `tokeira/crates/tokeira-kernel/tests/property_tests.rs`
    - **Validates: Requirements 4.2**

- [x] 2. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Implement chain-aware execution timeout
  - [x] 3.1 Add `first_run_started_at: Option<OffsetDateTime>` to `WorkflowTimeoutEntry` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Add the field after `started_at`
    - _Requirements: 4.5_

  - [x] 3.2 Update `evaluate_workflow_timeout` in `tokeira/crates/tokeira-runtime/src/runtime.rs` to use `first_run_started_at` for execution timeout
    - Execution timeout: use `entry.first_run_started_at.unwrap_or(entry.started_at)` as the origin
    - Run timeout: continue using `entry.started_at` (unchanged)
    - _Requirements: 4.5, 4.6_

  - [x] 3.3 Update `start_workflow` in `tokeira/crates/tokeira-runtime/src/runtime.rs` to pass `first_run_started_at` when inserting `WorkflowTimeoutEntry`
    - Set `first_run_started_at: request.first_run_started_at` in the entry
    - _Requirements: 4.5_

  - [x] 3.4 Fix all compilation errors from the new `WorkflowTimeoutEntry` field
    - Add `first_run_started_at: None` to all existing `WorkflowTimeoutEntry` construction sites in tests
    - _Requirements: 4.5, 4.6_

  - [x] 3.5 Write property test for chain-aware execution timeout evaluation
    - **Property 2: Chain-aware execution timeout evaluation**
    - For any `WorkflowTimeoutEntry` with arbitrary `started_at`, `first_run_started_at`, `workflow_execution_timeout`, `workflow_run_timeout`, and any `now` timestamp: verify execution timeout uses `first_run_started_at.unwrap_or(started_at)`, run timeout uses `started_at`, execution timeout takes precedence, and `None` fallback is backward-compatible
    - Place in `tokeira/crates/tokeira-runtime/src/runtime.rs` tests module or a dedicated test file
    - **Validates: Requirements 4.5, 4.6**

- [x] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Extend `handle_message` return type to include history events
  - [x] 5.1 Change `handle_message` return type in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Change from `Result<(CommitResult, SmallVec<[DispatchOp; 4]>)>` to `Result<(CommitResult, SmallVec<[DispatchOp; 4]>, SmallVec<[HistoryEvent; 8]>)>`
    - Capture `transition.history_events.clone()` before commit and return alongside dispatch ops
    - On `Duplicate`, return empty history events
    - _Requirements: 1.1_

  - [x] 5.2 Update `run_activation` to destructure the new return tuple
    - Change the `handle_message` call site to destructure `(commit_result, dispatch_ops, history_events)`
    - Pass `history_events` through to the post-commit path (needed for continue-as-new event extraction)
    - _Requirements: 1.1_

  - [x] 5.3 Fix all compilation errors from the return type change
    - Update existing tests that call `handle_message` directly to destructure the third element
    - _Requirements: 1.1_

- [x] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement lane post-commit continue-as-new detection and successor start
  - [x] 7.1 Add continue-as-new detection branch in `run_activation` post-commit path in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - After the existing child resolution delivery block (inside `if new_state.closed_at.is_some()`), add a new branch checking `new_state.status == ExecutionStatus::ContinuedAsNew`
    - Scan `history_events` for `HistoryEventKind::WorkflowExecutionContinuedAsNew` variant
    - If status is `ContinuedAsNew` but no matching event found, log at error level and skip
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 7.2 Construct successor `StartRequest` from the extracted event and predecessor state
    - `run_key`: fresh `RunKey::new()`
    - `run_id`: `new_run_id` from event
    - `workflow_id`, `namespace_id`: from predecessor `new_state`
    - `workflow_type`, `task_queue`, `input`, `memo`, `search_attributes`, timeout config: from event
    - `retry_policy`: from predecessor `new_state`
    - `attempt`: 1
    - `continued_execution_run_id`: `Some(new_state.run_id)`
    - `first_execution_run_id`: `Some(new_state.first_execution_run_id.unwrap_or(new_state.run_id))`
    - `first_run_started_at`: `Some(new_state.first_run_started_at.unwrap_or(new_state.started_at))`
    - `parent_run_key`: `None`
    - `parent_workflow_id`: `None`
    - _Requirements: 2.1–2.9, 3.1–3.3, 4.3, 4.4_

  - [x] 7.3 Submit successor `Command::Start` via `publisher.submit_to_run` and handle outcomes
    - On `Ok(CommitResult::Applied { new_state })`: insert `WorkflowTimeoutEntry` using committed `new_state.started_at` and `new_state.first_run_started_at` (not `start_request.now`)
    - On `Ok(CommitResult::Duplicate)`: log at error level (unexpected — request-dedupe collision with fresh RunKey); sweeper reconciles
    - On `Ok(CommitResult::Conflict { .. })`: log at error level (should not be reached — lane retries OCC internally)
    - On `Err`: log at error level with predecessor and successor context; predecessor unaffected
    - _Requirements: 5.1, 5.2, 5.3, 6.1–6.4, 7.1, 7.2_

  - [x] 7.4 Write property test for successor StartRequest construction
    - **Property 1: Successor StartRequest construction**
    - Generate random predecessor `WorkflowState` and random `WorkflowExecutionContinuedAsNew` event fields
    - Mock publisher captures `Command::Start` submitted via `submit_to_run`
    - Verify all field mappings match the design specification
    - Place in `tokeira/crates/tokeira-runtime/src/lane.rs` tests module
    - **Validates: Requirements 2.1–2.9, 3.1, 3.2, 4.3, 4.4**

  - [x] 7.5 Write property test for detection triggers only for ContinuedAsNew
    - **Property 3: Detection triggers only for ContinuedAsNew**
    - Generate random terminal `ExecutionStatus` values
    - For `ContinuedAsNew`: verify `Command::Start` is submitted
    - For all other terminal statuses: verify no `Command::Start` is submitted
    - Place in `tokeira/crates/tokeira-runtime/src/lane.rs` tests module
    - **Validates: Requirements 1.1, 1.2**

  - [x] 7.6 Write property test for predecessor unaffected by successor outcome
    - **Property 5: Predecessor unaffected by successor outcome**
    - Generate random predecessor states and random successor start outcomes (success, conflict, duplicate, error)
    - Verify predecessor's `CommitResult::Applied` is returned regardless of successor outcome
    - Verify no commands are submitted to the predecessor's `RunKey` after close
    - Place in `tokeira/crates/tokeira-runtime/src/lane.rs` tests module
    - **Validates: Requirements 5.3, 6.2**

  - [x] 7.7 Write property test for successor timeout tracking entry
    - **Property 6: Successor timeout tracking entry**
    - Generate random successor `StartRequest` values with random timeout configurations
    - Verify: when either timeout is configured, a tracking entry is inserted with matching fields
    - Verify: when neither timeout is configured, no entry is inserted
    - Place in `tokeira/crates/tokeira-runtime/src/lane.rs` tests module
    - **Validates: Requirements 7.1, 7.2**

- [x] 8. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Final integration wiring and cleanup
  - [x] 9.1 Verify predecessor timeout entry removal on ContinuedAsNew close
    - The existing `closed_at` check in the lane post-commit path already calls `workflow_timeout_tracking.remove(message.run_key)` — verify this covers the `ContinuedAsNew` case with a unit test
    - _Requirements: 7.3_

  - [x] 9.2 Verify all `StartRequest` construction sites pass `first_run_started_at` correctly
    - `tokeira-edge` `to_internal.rs`: set `first_run_started_at: None` for fresh starts
    - `tokeira-runtime` child workflow start: set `first_run_started_at: None`
    - Ensure no construction site is missed
    - _Requirements: 4.1_

- [x] 10. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) per project convention
- Each correctness property from the design is implemented as a single property-based test
- The design uses Rust throughout; no language selection was needed
- `first_execution_run_id` already exists on `StartRequest` but must be added to `WorkflowState` for successor construction
- `first_run_started_at` is entirely new and must be added to `StartRequest`, `WorkflowState`, and `WorkflowTimeoutEntry`
- Checkpoints ensure incremental validation after each logical layer
- Property tests validate universal correctness properties; unit tests validate specific examples and edge cases
