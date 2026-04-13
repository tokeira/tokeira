# Implementation Plan: Projection Visibility

## Overview

Convert the feature design into incremental implementation steps that build the visibility pipeline from core types through storage, sink, query, and service layers. Each task builds on the previous, ending with full wiring into the existing `VisibilityApi` trait. All code lives in `tokeira-projection`. The existing `InMemoryVisibilitySink` is replaced by `InMemoryVisibilityStore` + `VisibilitySink`.

## Tasks

- [x] 0. Extend ProjectionRecord with ProjectionContext (upstream change)
  - [x] 0.1 Define `ProjectionContext` struct in `tokeira-storage/src/api.rs`
    - Add `ProjectionContext` with fields: `namespace_id: NamespaceId`, `workflow_id: WorkflowId`, `run_id: RunId`, `workflow_type: WorkflowType`, `task_queue: TaskQueueName`, `execution_status: ExecutionStatus`, `start_time: OffsetDateTime`, `execution_time: Option<OffsetDateTime>`, `close_time: Option<OffsetDateTime>`, `history_length: i64`, `state_transition_count: i64`
    - Following the pattern from the prototyping crate's `ProjectionContext`
    - _Requirements: 1.5_
  - [x] 0.2 Add `context: ProjectionContext` field to `ProjectionRecord` in `tokeira-storage/src/api.rs`
    - _Requirements: 1.5_
  - [x] 0.3 Update `InMemoryStore::commit_transition` to populate `ProjectionContext` from `WorkflowState`
    - Populate all context fields from `state` (the committed `WorkflowState`) when building the `ProjectionRecord`
    - _Requirements: 1.5_
  - [x] 0.4 Update all `ProjectionRecord` construction sites in tests to include `context`
    - Update tests in `memory.rs`, `lane.rs`, `runtime.rs` that construct `ProjectionRecord` or `ProjectionBatch`
    - _Requirements: 1.5_

- [x] 1. Core types and data models
  - [x] 1.1 Define core visibility types in `tokeira-projection/src/types.rs`
    - Add `AttrId(u64)`, `SearchAttrType` enum (Keyword, KeywordList, Int, Bool, Double, Datetime, Text), `AttrDescriptor { attr_id, attr_type }`
    - Add `ExecutionRow` struct with all system fields (run_key, namespace_id, workflow_id, run_id, workflow_type, task_queue, status, start_time, execution_time, close_time, history_length, state_transition_count, memo, search_attr_version)
    - Add `FilterExpr` enum (And, Or, Compare, In, Between, StartsWith), `FieldRef` (System/Custom), `SystemField`, `CompareOp`, `FilterValue`
    - Add `CompiledFilter { expr: Option<FilterExpr> }`
    - Add `SortOrder` enum, `PageToken`, `PageBounds`, `MAX_PAGE_SIZE` — note: the store supports multiple sort orders internally, but the current `ListWorkflowExecutionsRequest` DTO has no sort field, so only the default sort is selectable via the API until the edge DTO is extended
    - Add `RollupDimension`, `RollupDelta`, `RollupCounter`, `GroupByField`
    - Add `ListResult`, `CountResult` structs for store return types
    - Add `SearchAttrValue` re-export or conversion helpers from `tokeira_types::SearchAttrValue` to `SearchAttrType`
    - Update `Cargo.toml` to add `serde`, `serde_json`, `base64`, `time`, `ordered-float`, `tokio-util` dependencies
    - _Requirements: 1.1, 1.2, 1.5, 2.1, 2.6, 3.1, 4.1, 4.4, 5.2, 6.1_

  - [x] 1.2 Add `PageToken` serialization (base64 JSON round-trip)
    - Implement `PageTokenWire` serde struct with compact field names (ct, st, rk, so)
    - Implement `PageToken::encode() -> String` and `PageToken::decode(s) -> Result<PageToken>`
    - _Requirements: 4.2, 4.3, 4.6_

  - [x] 1.3 Update `lib.rs` to declare new modules
    - Add `pub mod types;` and re-export key types
    - _Requirements: 1.1_

- [x] 2. VisibilityStore trait
  - [x] 2.1 Define `VisibilityStore` trait in `tokeira-projection/src/store.rs`
    - Write path: `upsert_execution`, `upsert_search_attr_index`, `remove_search_attr_index`, `accumulate_rollup`
    - Read path: `list_executions`, `count_executions`, `count_from_rollup`
    - Checkpoint: `load_checkpoint`, `save_checkpoint`
    - Registry: `resolve_attr`, `register_attr`
    - Backward compat: `get_row`
    - All methods return `Result<T>` using `anyhow`
    - Update `lib.rs` to declare `pub mod store;`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 9.1, 9.2_

- [x] 3. InMemoryVisibilityStore — rows, registry, checkpoint
  - [x] 3.1 Implement `InMemoryVisibilityStore` in `tokeira-projection/src/memory.rs`
    - Internal `VisibilityState` struct with `rows: HashMap<RunKey, ExecutionRow>`, `sa_current`, typed index BTreeMaps, rollups, registry, checkpoints
    - Wrap in `Arc<Mutex<VisibilityState>>` following existing `InMemoryStore` pattern
    - Implement `upsert_execution`, `get_row`, `save_checkpoint`, `load_checkpoint`
    - Implement `register_attr`, `resolve_attr` for the registry
    - Update `lib.rs` to declare `pub mod memory;`
    - _Requirements: 8.1, 8.6, 9.1, 9.2, 9.3_

  - [x]* 3.2 Write property test: Checkpoint Round-Trip (Property 15)
    - **Property 15: Checkpoint Round-Trip**
    - For any valid `ProjectionCursor`, saving via `save_checkpoint` and loading via `load_checkpoint` returns an identical cursor
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 9.1, 9.2**

- [x] 4. VisibilitySink — apply logic
  - [x] 4.1 Implement `VisibilitySink` in `tokeira-projection/src/visibility_sink.rs`
    - `VisibilitySink<S: VisibilityStore>` with `store` and `sink_id` fields
    - Implement `ProjectionSink` trait
    - `apply` method: load/create `ExecutionRow`, apply each `ProjectionOp` in order, populate system fields from `ProjectionRecord` context
    - `UpsertExecution`: merge status, memo, search attributes; resolve attrs via registry; remove old index entries; insert new index entries
    - `CloseExecution`: set terminal status and `closed_at`
    - Compute rollup deltas for status/workflow_type/task_queue dimension changes
    - Write updated row, index entries, and rollup deltas to store
    - Update `lib.rs` to declare `pub mod visibility_sink;`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 6.1, 6.2_

  - [x]* 4.2 Write property test: Apply Correctness (Property 1)
    - **Property 1: Apply Correctness**
    - For any valid `ProjectionRecord`, applying it produces an `ExecutionRow` whose fields correctly reflect sequential application of all ops
    - Use `proptest` with minimum 100 iterations, arbitrary generators for `ProjectionRecord`
    - **Validates: Requirements 1.1, 1.2, 1.4, 1.5**

  - [x]* 4.3 Write property test: Idempotent Apply (Property 2)
    - **Property 2: Idempotent Apply**
    - For any valid `ProjectionRecord`, `apply(apply(state, record), record) == apply(state, record)`
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 1.3**

- [x] 5. Checkpoint - Core sink and store wiring
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Search attribute indexing in InMemoryVisibilityStore
  - [x] 6.1 Implement typed index operations in `memory.rs`
    - `upsert_search_attr_index`: insert into the correct typed BTreeMap based on `SearchAttrType`
    - `remove_search_attr_index`: remove from the correct typed BTreeMap
    - Handle all seven types: keyword, keyword_list, int, bool, double, datetime, text_token
    - Text tokenization: split into lowercase alphanumeric tokens, index each separately
    - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.7_

  - [x]* 6.2 Write property test: Search Attribute Indexing (Property 3)
    - **Property 3: Search Attribute Indexing**
    - For any `UpsertExecution` with registered search attributes, typed index entries contain the run key under the correct `(namespace, attr_id, value)` key
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 2.1, 2.2, 2.6**

  - [x]* 6.3 Write property test: Index Update Cleanup (Property 4)
    - **Property 4: Index Update Cleanup**
    - When a new value replaces an existing indexed value, old entries are removed and only new entries are present
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 2.3**

  - [x]* 6.4 Write property test: Text Tokenization (Property 5)
    - **Property 5: Text Tokenization**
    - For any non-empty text string, the text token index contains one entry per unique lowercase alphanumeric token
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 2.7**

- [x] 7. FilterExpr parser and query compilation
  - [x] 7.1 Implement filter parser in `tokeira-projection/src/filter.rs`
    - Parse Temporal-compatible list-filter strings into `FilterExpr` AST
    - Support AND, OR, comparison (=, !=, <, <=, >, >=), IN, BETWEEN, StartsWith operators
    - Support system fields: WorkflowId, RunId, WorkflowType, TaskQueue, ExecutionStatus, StartTime, CloseTime, HistoryLength, StateTransitionCount
    - Support custom search attribute names (resolved via registry)
    - Implement `compile_filter(input, namespace_id, store) -> Result<CompiledFilter>`
    - Type-check each predicate (value type matches field type)
    - Return descriptive errors for unknown attributes, type mismatches, parse failures
    - Update `lib.rs` to declare `pub mod filter;`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x]* 7.2 Write property test: Filter Expression Round-Trip (Property 6)
    - **Property 6: Filter Expression Round-Trip**
    - For any valid `FilterExpr` AST, printing to string and parsing back produces an equivalent AST
    - Use `proptest` with minimum 100 iterations, arbitrary generator for `FilterExpr`
    - **Validates: Requirements 3.1**

- [x] 8. Query execution in InMemoryVisibilityStore
  - [x] 8.1 Implement `list_executions` in `memory.rs`
    - Filter rows by namespace, then apply `CompiledFilter` predicates
    - For system field predicates: direct field comparison on `ExecutionRow`
    - For custom attribute predicates: lookup in typed index BTreeMaps
    - Apply sort order (default: close_time DESC NULLS FIRST, start_time DESC, run_key DESC)
    - Apply keyset pagination using `PageBounds`
    - Return `ListResult` with rows and optional next page token
    - _Requirements: 3.6, 4.1, 4.4, 4.5, 8.2_

  - [x] 8.2 Implement `count_executions` in `memory.rs`
    - Filter rows by namespace and `CompiledFilter`
    - Support `group_by` on system fields and custom search attributes
    - Return `CountResult` with total count and optional group counts
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 8.4_

  - [x]* 8.3 Write property test: Pagination Completeness (Property 7)
    - **Property 7: Pagination Completeness**
    - For any set of rows and page size 1..MAX_PAGE_SIZE, iterating all pages yields exactly the same rows as unbounded query
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 4.1, 4.2, 4.3**

  - [x]* 8.4 Write property test: Sort Order Correctness (Property 8)
    - **Property 8: Sort Order Correctness**
    - Default sort: open executions before closed, closed ordered by close_time DESC, ties broken by start_time DESC then run_key DESC
    - Non-default sorts: rows ordered by specified field and direction
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 4.4, 4.5**

  - [x]* 8.5 Write property test: Count-List Consistency (Property 9)
    - **Property 9: Count-List Consistency**
    - `count_executions` equals the number of rows from `list_executions` with the same filter
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 5.1**

  - [x]* 8.6 Write property test: Group-By Count Correctness (Property 10)
    - **Property 10: Group-By Count Correctness**
    - Sum of per-group counts equals total count; each per-group count matches filtered row count
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 5.2, 5.3**

- [x] 9. Checkpoint - Query pipeline
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Rollup planner and acceleration
  - [x] 10.1 Implement `RollupPlanner` in `tokeira-projection/src/rollup.rs`
    - Compute signed deltas during apply: +1 for new row dimensions, -1/+1 for status transitions
    - Bucket rollup entries by configurable time window
    - Update `lib.rs` to declare `pub mod rollup;`
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 10.2 Implement `count_from_rollup` in `memory.rs`
    - Serve count from rollup aggregates for ExecutionStatus, WorkflowType, TaskQueue dimensions
    - _Requirements: 6.4, 8.5_

  - [x]* 10.3 Write property test: Rollup Delta Conservation (Property 11)
    - **Property 11: Rollup Delta Conservation**
    - Sum of all rollup deltas for each `(namespace, dimension, value)` equals net change in row count
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 6.1, 6.2**

  - [x]* 10.4 Write property test: Rollup Time Bucketing (Property 12)
    - **Property 12: Rollup Time Bucketing**
    - Bucket assignment is deterministic; two timestamps in the same window map to the same bucket
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 6.3**

  - [x]* 10.5 Write property test: Rollup-Accelerated Count Consistency (Property 13)
    - **Property 13: Rollup-Accelerated Count Consistency**
    - Count via rollup equals count via direct scan for rollup-accelerated dimensions with no filter
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 6.4**

- [x] 11. VisibilityQueryService — VisibilityApi implementation
  - [x] 11.1 Implement `VisibilityQueryService` in `tokeira-projection/src/query_service.rs`
    - `VisibilityQueryService<S: VisibilityStore>` generic over store
    - Implement `VisibilityApi` trait from `tokeira-edge`
    - `list_workflows`: parse namespace → NamespaceId, compile filter, decode page token, call `store.list_executions`, map `ExecutionRow` → `WorkflowExecutionSummary`, encode next page token
    - `count_workflows`: parse namespace and filter, use `count_from_rollup` when applicable, otherwise `count_executions`
    - Add `tokeira-edge` as dependency in `Cargo.toml`
    - Update `lib.rs` to declare `pub mod query_service;`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [x]* 11.2 Write property test: ExecutionRow to Summary Mapping (Property 14)
    - **Property 14: ExecutionRow to Summary Mapping**
    - For any `ExecutionRow`, the produced `WorkflowExecutionSummary` has matching namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 7.1, 7.5**

- [x] 12. ProjectionWorker extensions
  - [x] 12.1 Extend `ProjectionWorker` with continuous loop in `worker.rs`
    - Add `pub async fn run(&self, cancel: CancellationToken) -> Result<()>`
    - Load checkpoint from `VisibilityStore::load_checkpoint` (or start from beginning if none exists)
    - Loop: read batch → apply each record via sink → save checkpoint via `VisibilityStore::save_checkpoint` → repeat
    - Checkpointing is a worker responsibility, not a sink responsibility — the `ProjectionSink` trait only has `apply(record)` and does not see batch/cursor boundaries
    - On empty batch: exponential backoff (100ms base, 5s cap)
    - On sink error: log at warn, backoff, retry without advancing checkpoint
    - On cancellation: finish current batch, save checkpoint, return Ok
    - Add `tokio-util` dependency for `CancellationToken`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 10.1, 10.2, 10.3, 10.4_

  - [x]* 12.2 Write property test: Checkpoint-After-Apply Invariant (Property 16)
    - **Property 16: Checkpoint-After-Apply Invariant**
    - If sink apply fails, checkpoint is not advanced past pre-apply position
    - Use `proptest` with minimum 100 iterations
    - **Validates: Requirements 9.4**

- [x] 13. Remove old InMemoryVisibilitySink and wire new store
  - [x] 13.1 Replace `InMemoryVisibilitySink` with `InMemoryVisibilityStore`
    - Remove old `visibility.rs` (InMemoryVisibilitySink, VisibilityRow)
    - Update `lib.rs` to remove old `pub mod visibility;` and re-exports
    - Ensure `get_row` backward compatibility on `InMemoryVisibilityStore`
    - Update any existing tests or imports that reference `InMemoryVisibilitySink`
    - _Requirements: 8.6_

- [x] 14. Checkpoint - Full pipeline
  - Ensure all tests pass, ask the user if questions arise.

- [x] 15. Unit tests and integration tests
  - [x]* 15.1 Write unit tests for error cases
    - Unknown search attribute name returns descriptive error (Req 2.4)
    - Search attribute type mismatch returns descriptive error (Req 2.5)
    - Unknown attribute in filter returns descriptive error (Req 3.4)
    - Type mismatch in filter returns descriptive error (Req 3.5)
    - Empty filter returns all executions (Req 3.6)
    - Invalid page token returns descriptive error (Req 4.6)
    - Count with no filter returns total (Req 5.4)
    - `get_row(run_key)` backward compatibility (Req 8.6)
    - No persisted cursor starts from beginning (Req 9.3)
    - _Requirements: 2.4, 2.5, 3.4, 3.5, 3.6, 4.6, 5.4, 8.6, 9.3_

  - [x]* 15.2 Write unit tests for worker lifecycle
    - Worker backoff on empty batch (Req 10.2)
    - Worker graceful shutdown on cancellation (Req 10.3)
    - Worker retry on sink error (Req 10.4)
    - _Requirements: 10.2, 10.3, 10.4_

  - [x]* 15.3 Write integration tests
    - End-to-end: projection record → sink apply → list_workflows returns execution (Req 7.3)
    - Full pagination walk-through with filter expressions (Req 7.4)
    - Worker lifecycle: start, process batches, cancel, verify checkpoint (Req 10.1)
    - _Requirements: 7.3, 7.4, 10.1_

- [x] 16. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All 16 property tests use `proptest` with `ProptestConfig::with_cases(100)` minimum
- Property test tag format: `// Feature: projection-visibility, Property N: <title>`
- All code lives in `tokeira-projection`; no changes to `tokeira-edge` or `tokeira-kernel`
- `rustfmt` max_width = 90 applies to all generated code
- The `VisibilityApi` trait is imported from `tokeira-edge` for the `VisibilityQueryService`
