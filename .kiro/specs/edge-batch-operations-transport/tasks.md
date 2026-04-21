# Implementation Plan: Edge Batch Operations Transport

## Overview

Implement the Batch Operations Transport layer in three phases: (1) domain types, in-memory store, proto translation, and the `start_batch_operation` handler; (2) the `BatchExecutionEngine` background task with visibility iteration, operation dispatch, progress tracking, and rate limiting; (3) lifecycle handlers (`stop`, `describe`, `list`). All new runtime types go in `crates/tokeira-runtime/src/batch.rs`, proto translation in `crates/tokeira-edge/src/translate/batch.rs`, and handler wiring in the existing `workflow_service.rs` files.

## Tasks

- [ ] 1. Define domain types and BatchOperationStore in tokeira-runtime
  - [ ] 1.1 Create `crates/tokeira-runtime/src/batch.rs` with domain types
    - Define `JobId`, `BatchOperationType`, `BatchOperationState`, `BatchOperationParams`, `BatchProgressCounters`, `WorkflowExecutionRef`, `BatchOperationEntry`, `BatchOperationSnapshot`, `BatchOperationInfo`, and `BatchError`
    - `BatchProgressCounters` uses `AtomicU64` for lock-free updates
    - `BatchOperationEntry` holds a `CancellationToken` from `tokio_util`
    - _Requirements: 1.1, 1.2, 1.5_

  - [ ] 1.2 Implement `BatchOperationStore` with DashMap backing
    - Implement `create()` — insert new entry, return `ALREADY_EXISTS` if key exists
    - Implement `describe()` — return `BatchOperationSnapshot` with resolved atomic counters, return `NOT_FOUND` if missing
    - Implement `stop()` — cancel the token, store reason/identity, return Ok for terminal states (idempotent), return `NOT_FOUND` if missing
    - Implement `set_state()` — update state and close_time (called by engine)
    - Implement `list()` — paginated listing filtered by namespace
    - Implement `get_cancellation_token()` — return clone of token for engine
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ] 1.3 Add `pub mod batch;` to `crates/tokeira-runtime/src/lib.rs`
    - Add module declaration and re-export public types
    - _Requirements: 1.1_

  - [ ]* 1.4 Write property test: batch store CRUD correctness (Property 1)
    - **Property 1: Batch store CRUD correctness**
    - Generate random create/describe sequences; verify create-then-describe round-trip, ALREADY_EXISTS on duplicate, NOT_FOUND on missing
    - **Validates: Requirements 1.1, 1.3, 1.4**

  - [ ]* 1.5 Write property test: pagination completeness (Property 5)
    - **Property 5: Pagination completeness**
    - Generate random batch operation sets and page sizes; verify iterating all pages returns every entry exactly once with correct info fields
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5**

  - [ ]* 1.6 Write property test: idempotent stop on terminal state (Property 6)
    - **Property 6: Idempotent stop on terminal state**
    - Generate batch operations in Completed or Failed state, call stop, verify success returned
    - **Validates: Requirements 7.3**

- [ ] 2. Implement proto translation for batch operations
  - [ ] 2.1 Create `crates/tokeira-edge/src/translate/batch.rs` with translation functions
    - Implement `start_batch_request_to_edge()` — parse `StartBatchOperationRequest` into creation params, validate non-empty job_id, presence of query/executions, presence of operation variant
    - Implement `describe_batch_response_to_proto()` — build `DescribeBatchOperationResponse` from `BatchOperationSnapshot`
    - Implement `list_batch_response_to_proto()` — build `ListBatchOperationsResponse` from `Vec<BatchOperationInfo>`
    - Implement `batch_operation_type_to_proto()` / `batch_operation_type_from_proto()` — enum mapping
    - Implement `batch_operation_state_to_proto()` / `batch_operation_state_from_proto()` — enum mapping
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [ ] 2.2 Add `pub mod batch;` to `crates/tokeira-edge/src/translate/mod.rs`
    - _Requirements: 3.1_

  - [ ] 2.3 Update `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` for batch types
    - Add: `BatchOperationSignal.header` — dropped at translation, not stored or delivered (kernel SignalRequest has no header field)
    - Add: `BatchOperationUpdateWorkflowExecutionOptions` — entire operation variant scoped out for MVP
    - Add: `BatchOperationReset.reset_reapply_type`, `current_run_only`, `reset_reapply_exclude_types` — not supported
    - _Requirements: 3.4_

  - [ ]* 2.4 Write property test: proto translation round-trip (Property 3)
    - **Property 3: Proto translation round-trip for batch types**
    - Generate random `BatchOperationType` and `BatchOperationState` values, verify round-trip through proto conversion; generate random `BatchOperationSnapshot`, verify describe response preserves all fields
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.5, 3.6**

  - [ ]* 2.5 Write property test: proto validation rejects invalid inputs (Property 4)
    - **Property 4: Proto validation rejects invalid inputs**
    - Generate `StartBatchOperationRequest` protos with empty job_id, missing operation variant, missing query+executions; verify translation returns errors
    - **Validates: Requirements 3.4**

- [ ] 3. Implement start_batch_operation handler
  - [ ] 3.1 Add `batch_store: Arc<BatchOperationStore>` to `WorkflowService` struct
    - Follow the same pattern as `schedule_store` — add field, wire through constructors, add accessor method
    - Modify `crates/tokeira-edge/src/workflow_service.rs`
    - _Requirements: 1.1_

  - [ ] 3.2 Implement `start_batch_operation` method on `WorkflowService`
    - Translate proto request via `start_batch_request_to_edge()`
    - Validate: empty job_id → `INVALID_ARGUMENT`, missing query+executions → `INVALID_ARGUMENT`, missing operation variant → `INVALID_ARGUMENT`
    - Create `BatchOperationEntry` with state `Running`, `start_time` = now, store reason/identity/max_operations_per_second
    - Insert into `batch_store` (handle `ALREADY_EXISTS`)
    - Capture a `BatchDispatchContext` from the validated `EdgeContext` produced by the start handler
    - Spawn `run_batch_operation` as a tokio task with the captured dispatch context
    - Return success response
    - Modify `crates/tokeira-edge/src/workflow_service.rs`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10_

  - [ ] 3.3 Wire `start_batch_operation` in gRPC handler
    - Replace the `Status::unimplemented` stub in `crates/tokeira-edge/src/grpc/workflow_service.rs` with a call to `self.service.start_batch_operation()`
    - _Requirements: 2.1_

  - [ ]* 3.4 Write unit tests for start handler validation
    - Test empty job_id → `INVALID_ARGUMENT`
    - Test missing query and executions → `INVALID_ARGUMENT`
    - Test missing operation variant → `INVALID_ARGUMENT`
    - Test duplicate job_id → `ALREADY_EXISTS`
    - Test valid request creates Running entry with correct fields (reason, identity, rate, signal/termination/reset params)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.7, 2.8, 2.9, 2.10_

- [ ] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement BatchExecutionEngine
  - [ ] 5.1 Implement `run_batch_operation` async function in `crates/tokeira-edge/src/batch_engine.rs`
    - Create new file `crates/tokeira-edge/src/batch_engine.rs`
    - Read operation params from store entry
    - Discover workflows: if `visibility_query` is set, call `WorkflowService::list_workflow_executions` with pagination; if `executions` is set, iterate the explicit list
    - Update `total_operation_count` after discovery
    - For each workflow: check `cancellation_token.is_cancelled()`, apply operation via `apply_operation` (calls WorkflowService methods), increment complete or failure counter, sleep for rate limiting
    - On completion: set state to `Completed`, record `close_time`
    - On unrecoverable visibility error: set state to `Failed`, record `close_time`
    - On cancellation: stop processing, set state to `Completed`, record `close_time`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 5.1, 5.2, 6.1, 6.2, 6.3_

  - [ ] 5.2 Implement `apply_operation` dispatch function
    - Add internal `WorkflowService` batch-dispatch methods: `terminate_workflow_batch_internal`, `cancel_workflow_batch_internal`, `signal_workflow_batch_internal`, `delete_workflow_batch_internal`, and `reset_workflow_batch_internal`
    - Each internal method accepts `&BatchDispatchContext` and `&WorkflowExecutionRef`, does not call `interceptors.begin`, and resolves the exact run from `workflow_ref.run_id` when present
    - Match on `BatchOperationParams` and call the corresponding internal batch-dispatch method
    - For Reset: resolve concrete `fork_event_id` per-workflow from the stored `BatchResetTarget` by reading the exact workflow execution's history
    - For Signal: pass signal_name and input (header is dropped at translation — not stored)
    - Use captured `BatchDispatchContext.edge_context` for request identity, principal, and audit context without re-authenticating from headers
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7_

  - [ ] 5.3 Implement rate limiting with `compute_sleep_duration`
    - When `max_operations_per_second > 0`, sleep `1.0 / rate` between operations
    - When zero or unset, apply default rate limit of 50 ops/sec
    - _Requirements: 5.3, 5.4_

  - [ ]* 5.4 Write property test: progress counter accuracy (Property 2)
    - **Property 2: Progress counter accuracy**
    - Generate random success/failure outcome sequences, run engine with mock runtime, verify final counters: total = N, complete = S, failure = F where S + F = N
    - **Validates: Requirements 1.5, 4.8, 4.9, 4.10**

  - [ ]* 5.5 Write unit tests for execution engine
    - Test engine completes with `Completed` state and `close_time` after processing all workflows
    - Test engine sets `Failed` state on unrecoverable visibility query error
    - Test default rate limit (50 ops/sec) applied when `max_operations_per_second` is zero
    - Test cancellation stops processing, state becomes `Completed`, already-applied ops not rolled back
    - _Requirements: 5.1, 5.2, 5.4, 6.1, 6.2, 6.3_

  - [ ]* 5.6 Write integration tests for engine operation dispatch
    - Test terminate batch calls `terminate_workflow` per workflow with correct params
    - Test cancel batch calls `cancel_workflow` per workflow
    - Test signal batch calls `signal_workflow` with signal name and input
    - Test delete batch calls the delete path per workflow
    - Test reset batch calls `reset_workflow` with options
    - Test visibility pagination: mock multi-page results, verify all pages consumed
    - Test explicit execution list: all executions processed
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7_

- [ ] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement lifecycle handlers (stop, describe, list)
  - [ ] 7.1 Implement `stop_batch_operation` method on `WorkflowService`
    - Look up batch operation in store; return `NOT_FOUND` if missing
    - If `Running`, set cancellation flag via `batch_store.stop()`
    - If terminal state, return success (idempotent)
    - Store reason and identity from the stop request
    - Modify `crates/tokeira-edge/src/workflow_service.rs`
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ] 7.2 Implement `describe_batch_operation` method on `WorkflowService`
    - Look up batch operation via `batch_store.describe()`; return `NOT_FOUND` if missing
    - Translate `BatchOperationSnapshot` to proto via `describe_batch_response_to_proto()`
    - Return operation type, job_id, state, start_time, close_time, progress counts, identity, reason
    - Modify `crates/tokeira-edge/src/workflow_service.rs`
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [ ] 7.3 Implement `list_batch_operations` method on `WorkflowService`
    - Call `batch_store.list()` with namespace, page_size, page_token
    - Translate results to proto via `list_batch_response_to_proto()`
    - Return paginated `BatchOperationInfo` items with `next_page_token`
    - Modify `crates/tokeira-edge/src/workflow_service.rs`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [ ] 7.4 Wire lifecycle handlers in gRPC layer
    - Replace `Status::unimplemented` stubs for `stop_batch_operation`, `describe_batch_operation`, `list_batch_operations` in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - _Requirements: 7.1, 8.1, 9.1_

  - [ ]* 7.5 Write unit tests for lifecycle handlers
    - Test stop on Running op sets cancellation flag and stores reason/identity
    - Test stop on non-existent returns `NOT_FOUND`
    - Test stop on terminal state returns success (idempotent)
    - Test describe returns all fields for Running and terminal states
    - Test describe on non-existent returns `NOT_FOUND`
    - Test list returns empty list for empty namespace
    - Test describe mid-execution shows partial progress counts
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 8.1, 8.2, 8.3, 8.4, 9.6_

- [ ] 8. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All property-based tests use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
- The `BatchOperationStore` follows the same `DashMap` + `Arc` pattern as `ScheduleStore` and `VersioningRuleStore`
- The `WorkflowService` integration follows the same pattern as `schedule_store` — field, constructors, accessor
- Each property test is tagged: `// Feature: edge-batch-operations-transport, Property N: <title>`
