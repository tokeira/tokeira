# Implementation Plan: DSQL Projection Persistence

## Overview

Implement the projection read path (`DsqlProjectionLog`), projector checkpoint management through `DsqlVisibilityStore`, DSQL visibility materialization, and `ExecutionStatus` stable numeric mapping. The implementation adds `dsql/projection_log.rs` in `tokeira-storage`, `dsql_store.rs` in `tokeira-projection`, updates `V012__vis_execution.sql` in place for `run_id UUID`, and adds the `to_db_smallint` / `TryFrom<i16>` methods on `ExecutionStatus` in `tokeira-types`.

## Tasks

- [x] 1. Add `ExecutionStatus` stable numeric mapping to `tokeira-types`
  - [x] 1.1 Add `ExecutionStatusDecodeError` and `to_db_smallint` / `TryFrom<i16>` to `ExecutionStatus`
    - Add `use thiserror::Error;` to `tokeira-types/src/execution.rs`
    - Add `#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)] #[error("unknown execution status database value {value}")] pub struct ExecutionStatusDecodeError { pub value: i16 }` 
    - Add `impl ExecutionStatus { pub fn to_db_smallint(self) -> i16 { match self { Self::Running => 0, Self::Paused => 1, Self::Completed => 2, Self::Failed => 3, Self::Cancelled => 4, Self::Terminated => 5, Self::ContinuedAsNew => 6, Self::TimedOut => 7 } } }`
    - Add `impl TryFrom<i16> for ExecutionStatus` with the reverse mapping, returning `ExecutionStatusDecodeError` for unknown values
    - Follow the exact pattern from `TaskKind::to_db_smallint` / `TryFrom<i16>` in `tokeira-types/src/task_queue.rs`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [x] 1.2 Write stability test for `ExecutionStatus` numeric mapping
    - Add `#[test] fn execution_status_database_mapping_is_stable()` in `tokeira-types/src/execution.rs`
    - Assert exact values: `Running=0, Paused=1, Completed=2, Failed=3, Cancelled=4, Terminated=5, ContinuedAsNew=6, TimedOut=7`
    - Assert round-trip for each variant: `TryFrom::<i16>::try_from(variant.to_db_smallint()) == Ok(variant)`
    - Assert unknown values return error: `TryFrom::<i16>::try_from(8).is_err()`, `TryFrom::<i16>::try_from(-1).is_err()`
    - _Requirements: 6.3, 6.4, 6.6_

  - [x] 1.3 Write property test for `ExecutionStatus` round-trip (Property 3)
    - **Feature: dsql-projection-persistence, Property 3: ExecutionStatus Numeric Round-Trip**
    - Use `proptest` with `prop_oneof!` to generate random `ExecutionStatus` variants
    - Verify `TryFrom::<i16>::try_from(status.to_db_smallint()) == Ok(status)` for all generated variants
    - Minimum 100 iterations
    - _Requirements: 6.5_

- [x] 2. Checkpoint — Ensure `tokeira-types` tests pass
  - Run `cargo test -p tokeira-types` and verify all tests pass including the new stability and property tests.

- [x] 3. Update `vis_execution.run_id` type in DDL
  - [x] 3.1 Update `V012__vis_execution.sql` in-place to use `UUID` for `run_id`
    - Change `run_id TEXT NOT NULL` to `run_id UUID NOT NULL` in the existing DDL file
    - Tokeira targets schema version 1 — in-place DDL update, no separate migration needed
    - _Requirements: 4.2 (run_id field in vis_execution)_

- [x] 4. Create `DsqlProjectionLog` in `dsql/projection_log.rs`
  - [x] 4.1 Move `DsqlConnectionAcquirer` trait to shared location
    - Move the `DsqlConnectionAcquirer` trait from `dsql/run_repository.rs` (where it's currently private) to `dsql/connection.rs` as `pub(crate)`
    - Update `run_repository.rs` to import from `connection.rs`
    - This allows storage-crate modules (`run_repository.rs` and `projection_log.rs`) to use the same test seam. `DsqlVisibilityStore` lives in `tokeira-projection` and uses a concrete `Arc<DsqlConnectionDirector>`; its SQL behavior is tested through pure helpers and gated DSQL integration tests rather than the storage-private acquirer trait.
    - _Requirements: 1.1_

  - [x] 4.2 Create the `DsqlProjectionLog` struct and constructors
    - Create new file `tokeira/crates/tokeira-storage/src/dsql/projection_log.rs`
    - Define `pub struct DsqlProjectionLog { director: Arc<dyn DsqlConnectionAcquirer> }`
    - Implement `pub fn new(director: Arc<DsqlConnectionDirector>) -> Self`
    - Implement `#[cfg(test)] fn new_with_acquirer(director: Arc<dyn DsqlConnectionAcquirer>) -> Self`
    - _Requirements: 1.1, 1.7_

  - [x] 4.3 Implement `ProjectionLog::read_from`
    - Validate cursor invariant: both `last_run_key` and `last_transition_seq` must be `Some` or both `None` — return error if mixed
    - If both `None` (beginning): execute beginning-of-partition query
    - If both `Some`: execute cursor-based query with `(run_key, transition_seq) > ($3, $4)`
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

- [x] 5. Wire `DsqlProjectionLog` into `DsqlStore`
  - [x] 5.1 Add `pub mod projection_log;` to `dsql/mod.rs` and `pub use projection_log::*;`
    - Add the module declaration and re-export
    - _Requirements: 1.1_

  - [x] 5.2 Add `projection_log` field to `DsqlStore` and construct in `from_connector`
    - Add `projection_log: projection_log::DsqlProjectionLog` field to `DsqlStore`
    - Construct `DsqlProjectionLog::new(Arc::clone(&director))` in `from_connector`
    - Add `pub fn projection_log(&self) -> &projection_log::DsqlProjectionLog { &self.projection_log }` accessor
    - _Requirements: 1.1_

- [x] 6. Checkpoint — Ensure compilation passes
  - Run `cargo check -p tokeira-storage` and verify the new module compiles without errors.

- [x] 7. Create `DsqlVisibilityStore` in `tokeira-projection`
  - [x] 7.1 Create the `DsqlVisibilityStore` struct
    - Create new file `tokeira/crates/tokeira-projection/src/dsql_store.rs`
    - Define `pub struct DsqlVisibilityStore { director: Arc<DsqlConnectionDirector> }`
    - The struct lives in `tokeira-projection` (not `tokeira-storage`) because it implements `VisibilityStore` + `ProjectionSink` from `tokeira-projection` — placing it in `tokeira-storage` would create a dependency cycle
    - Implement `pub fn new(director: Arc<DsqlConnectionDirector>) -> Self`
    - Gate behind `#[cfg(feature = "dsql")]` — add `dsql` feature to `tokeira-projection/Cargo.toml` that forwards to `tokeira-storage/dsql`
    - _Requirements: 7.1, 7.2_

  - [x] 7.2 Implement `VisibilityStore` — checkpoint methods
    - `load_checkpoint(sink_id)`: `SELECT last_applied_cursor FROM projector_checkpoint WHERE sink_id = $1` — queries by `sink_id` only, matching the trait signature. The caller (worker) must ensure `sink_id` is unique per `(partition_id, fanout)` substream (e.g., `"visibility-p0-f1"`). This matches the in-memory store's `HashMap<String, ProjectionCursor>` keyed by `sink_id`.
    - `save_checkpoint(sink_id, cursor)`: `INSERT INTO projector_checkpoint (sink_id, partition_id, fanout, last_applied_cursor, updated_at) VALUES ($1, $2, $3, $4, now()) ON CONFLICT (sink_id, partition_id, fanout) DO UPDATE SET last_applied_cursor = EXCLUDED.last_applied_cursor, updated_at = now()` — derives `partition_id` and `fanout` from the cursor. The PK includes partition/fanout for future multi-partition-per-sink support, but the current worker uses one sink_id per substream.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x] 7.3 Implement `VisibilityStore` — write methods
    - `upsert_execution`: INSERT INTO vis_execution ... ON CONFLICT (run_key) DO UPDATE — fully implemented
    - `delete_execution`: DELETE FROM vis_execution WHERE run_key = $1 — fully implemented
    - `upsert_search_attr_index` → `bail!("projection-visibility spec")` — no search-attr tables exist yet in the DSQL schema
    - `remove_search_attr_index` → `bail!("projection-visibility spec")` — same
    - `accumulate_rollup` → `bail!("projection-visibility spec")` — no rollup tables exist yet
    - The `ProjectionSink::apply` implementation (task 7.5) must NOT route search-attribute patches through these stubs — it should skip search-attr ops until the tables exist
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 7.7, 7.8_

  - [x] 7.4 Implement `VisibilityStore` — query method stubs
    - `list_executions` → `bail!("projection-visibility spec")`
    - `count_executions` → `bail!("projection-visibility spec")`
    - `count_from_rollup` → `bail!("projection-visibility spec")`
    - `resolve_attr` → `bail!("projection-visibility spec")`
    - `register_attr` → `bail!("projection-visibility spec")`
    - `get_row` → `None`
    - These stubs allow compilation and the `VisibilitySink` wrapper to be constructed, while deferring the full query implementation
    - _Requirements: 7.6_

  - [x] 7.5 Implement `ProjectionSink::apply`
    - Process a single `ProjectionRecord` — iterate over `record.ops` in order
    - For `UpsertExecution`: call `self.upsert_execution` with an `ExecutionRow` built from the record's `ProjectionContext` + op fields
    - For `CloseExecution`: UPDATE vis_execution with terminal status and close_time; if 0 rows affected, INSERT catch-up row
    - Memo merge: read existing memo, merge patch, write back (or skip if empty patch)
    - Do not call the stubbed search-attribute, rollup, or query methods in this spec
    - _Requirements: 4.1, 5.1, 5.2, 5.3, 5.4, 5.5, 7.4, 7.8, 8.1, 8.2_

- [x] 8. Register modules and wire into DsqlStore
  - [x] 8.1 Add `pub mod projection_log;` to `dsql/mod.rs` and `pub use projection_log::*;`
    - _Requirements: 1.1_

  - [x] 8.2 Add `projection_log` field to `DsqlStore` and construct in `from_connector`
    - Add `projection_log: projection_log::DsqlProjectionLog` field
    - Construct `DsqlProjectionLog::new(Arc::clone(&director))` in `from_connector`
    - Add `pub fn projection_log(&self) -> &projection_log::DsqlProjectionLog` accessor
    - _Requirements: 1.1_

  - [x] 8.3 Register `dsql_store` module in `tokeira-projection`
    - Add `#[cfg(feature = "dsql")] pub mod dsql_store;` to `tokeira-projection/src/lib.rs`
    - Add `tokeira-storage = { path = "../tokeira-storage", features = ["dsql"] }` to `tokeira-projection/Cargo.toml` under the `dsql` feature
    - Add `dsql` feature to `tokeira-projection/Cargo.toml`: `dsql = ["tokeira-storage/dsql"]`
    - _Requirements: 7.1, 7.2_

- [x] 9. Checkpoint — Ensure compilation passes
  - Run `cargo check -p tokeira-storage` and `cargo check -p tokeira-projection --features dsql` and verify the new modules compile without errors.

- [x] 10. Property-based tests
  - [x] 10.1 Write property test for cursor-based pagination correctness (Property 1)
    - **Feature: dsql-projection-persistence, Property 1: Cursor-Based Pagination Correctness**
    - Extract a pure `interpret_read_from` helper that takes a sorted slice of `(RunKey, TransitionSeq)` pairs, a cursor `(Option<RunKey>, Option<TransitionSeq>)`, and a limit, and returns the expected result indices and next cursor
    - Use `proptest` to generate: random `Vec<(RunKey, TransitionSeq)>` (sorted), random cursor position (None or a valid position from the vec), random limit (1..=50)
    - Verify: returned records are strictly after cursor, in ascending order, limited to `limit`, and `next_cursor` points to the last returned record (or original cursor if empty)
    - Minimum 100 iterations
    - Test location: `tokeira-storage/src/dsql/projection_log.rs`
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

  - [x] 10.2 Write property test for projection codec round-trip (Property 2)
    - **Feature: dsql-projection-persistence, Property 2: Projection Codec Round-Trip**
    - Add `Arbitrary` implementations (or `proptest` strategies) for `ProjectionContext`, `ProjectionOp`, and `ProjectionCursor` (with and without `last_run_key`/`last_transition_seq`)
    - Verify `decode_projection_context(encode_projection_context(&ctx)?) == Ok(ctx)` for all generated `ProjectionContext` values
    - Verify `decode_projection_ops(encode_projection_ops(&ops)?) == Ok(ops)` for all generated `Vec<ProjectionOp>` values
    - Verify `decode_projection_cursor(encode_projection_cursor(&cursor)?) == Ok(cursor)` for all generated `ProjectionCursor` values
    - Minimum 100 iterations per type
    - Test location: `tokeira-storage/src/dsql/codec.rs` (extend existing proptest block)
    - _Requirements: 1.8, 1.9, 2.5_

  - [x] 10.3 Write property test for Memo codec round-trip (Property 4)
    - **Feature: dsql-projection-persistence, Property 4: Memo Codec Round-Trip**
    - Generate random `Memo` values (BTreeMap<String, Payload> with random keys and payload data)
    - Verify `decode::<Memo>(encode(&memo)?) == Ok(memo)` for all generated values
    - Minimum 100 iterations
    - Test location: `tokeira-storage/src/dsql/codec.rs`
    - _Requirements: 8.3_

  - [x] 10.4 Write property test for visibility sink operation ordering (Property 5)
    - **Feature: dsql-projection-persistence, Property 5: Visibility Sink Operation Ordering**
    - Extract a pure `resolve_final_vis_state` helper that takes a `Vec<ProjectionOp>` and a `ProjectionContext`, and returns the final `(ExecutionStatus, Option<OffsetDateTime>)` (status, close_time)
    - Use `proptest` to generate random `ProjectionRecord` values containing 1–4 ops (mix of `UpsertExecution` and `CloseExecution`)
    - Verify: the final status and close_time match the last operation in the sequence
    - Minimum 100 iterations
    - Test location: `tokeira-projection/src/dsql_store.rs`
    - _Requirements: 7.4, 5.1, 5.2, 5.3_

- [x] 11. Unit tests for `DsqlProjectionLog`
  - [x] 11.1 Write unit test for `DbClass::Projection` routing on `read_from`
    - Use `RecordingAcquirer` mock to verify `read_from` acquires `DbClass::Projection`
    - _Requirements: 1.7_

  - [x] 11.2 Write unit test for beginning-of-partition cursor behavior
    - Verify that when `cursor.last_run_key.is_none()`, the query does not include the row-value comparison predicate
    - Can be tested via the pure `interpret_read_from` helper with a beginning cursor
    - _Requirements: 1.2_

  - [x] 11.3 Write unit test for empty partition returns original cursor
    - Verify `read_from` on an empty result returns `next_cursor == input cursor`
    - Can be tested via the pure helper
    - _Requirements: 1.5_

- [x] 12. Unit tests for `DsqlVisibilityStore`
  - [x] 12.1 Write unit test for `ProjectionSink::apply` decision logic
    - Test pure helpers for operation ordering, memo merge, and field mapping in `tokeira-projection/src/dsql_store.rs`
    - Verify `DbClass::Projection` routing for `DsqlVisibilityStore` in the gated DSQL integration tests because it holds a concrete `Arc<DsqlConnectionDirector>` rather than the storage-private mock acquirer trait
    - _Requirements: 4.6_

  - [x] 12.2 Write unit test for `ExecutionStatus` encoding in visibility writes
    - Verify that the visibility sink uses `to_db_smallint()` when binding `execution_status`
    - Can be tested by processing a record and checking the bound value via a mock or by verifying the pure helper
    - _Requirements: 4.3_

  - [x] 12.3 Write unit test for CloseExecution catch-up insert
    - Verify that processing a `CloseExecution` without a prior `UpsertExecution` produces a complete row
    - Test via the pure `resolve_final_vis_state` helper: a single `CloseExecution` op should produce the terminal status and close_time
    - _Requirements: 5.5_

  - [x] 12.4 Write unit test for memo merge behavior
    - Verify that processing `UpsertExecution` with memo `{a: 1}` then `UpsertExecution` with memo_patch `{b: 2}` produces merged memo `{a: 1, b: 2}`
    - Verify that processing `UpsertExecution` with memo then `UpsertExecution` with empty memo_patch preserves the original memo
    - Test via a pure memo merge helper
    - _Requirements: 8.1, 8.2_

- [x] 13. Unit tests for `ExecutionStatus` (additional edge cases)
  - [x] 13.1 Write unit test for unknown `i16` values
    - Verify `TryFrom::<i16>::try_from(8)` returns `Err(ExecutionStatusDecodeError { value: 8 })`
    - Verify `TryFrom::<i16>::try_from(-1)` returns `Err(ExecutionStatusDecodeError { value: -1 })`
    - Verify `TryFrom::<i16>::try_from(100)` returns `Err(ExecutionStatusDecodeError { value: 100 })`
    - _Requirements: 6.4_

- [x] 14. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-types -p tokeira-storage` and `cargo test -p tokeira-projection --features dsql` and verify all tests pass.

- [x] 15. Tracing instrumentation verification
  - [x] 15.1 Verify all public methods have `#[instrument]` annotations
    - `DsqlProjectionLog::read_from` — `#[instrument(name = "dsql.read_from", ...)]`
    - `DsqlVisibilityStore::load_checkpoint` — `#[instrument(name = "dsql.visibility_store.load_checkpoint", ...)]`
    - `DsqlVisibilityStore::save_checkpoint` — `#[instrument(name = "dsql.visibility_store.save_checkpoint", ...)]`
    - `DsqlVisibilityStore::apply` — `#[instrument(name = "dsql.visibility_store.apply", ...)]`
    - Verify span fields include relevant parameters (partition_id, fanout, sink_id, run_key, record_count) and exclude large serialized payloads
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [x] 16. Integration tests (gated behind `dsql-integration` feature)
  - [x] 16.1 Integration test: `read_from` pagination cycle
    - Insert multiple `projection_log` rows via `commit_transition`
    - Read with beginning cursor, verify first batch
    - Read with returned `next_cursor`, verify subsequent batch
    - Continue until all records consumed
    - Verify total records match inserted count and ordering is correct
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

  - [x] 16.2 Integration test: checkpoint persist and resume
    - Write a checkpoint for a substream-unique sink id such as `"visibility-p0-f1"` and a cursor carrying the matching `partition_id`/`fanout`
    - Read it back by sink id, verify the cursor matches
    - Write a different cursor for the same sink id, partition, and fanout
    - Read it back, verify the updated cursor
    - Read a non-existent checkpoint, verify `None`
    - _Requirements: 2.1, 2.2, 3.1, 3.2, 3.3_

  - [x] 16.3 Integration test: visibility sink end-to-end
    - Start a workflow via `commit_transition` (produces projection_log rows)
    - Read projection records via `read_from`
    - Apply records via `DsqlVisibilityStore::apply`
    - Query `vis_execution` directly to verify the materialized row has correct fields
    - Complete the workflow, read the `CloseExecution` record, apply it
    - Verify `vis_execution` row has terminal status and close_time
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 5.4_

  - [x] 16.4 Integration test: CloseExecution catch-up insert
    - Process a `CloseExecution` record for a `run_key` that has no existing `vis_execution` row
    - Verify a complete row is inserted with the context metadata and close fields
    - _Requirements: 5.5_

  - [x] 16.5 Integration test: memo merge end-to-end
    - Process an `UpsertExecution` with memo `{key_a: payload_a}`
    - Process another `UpsertExecution` with memo_patch `{key_b: payload_b}`
    - Query `vis_execution.memo`, deserialize, verify both keys are present
    - Process an `UpsertExecution` with empty memo_patch, verify memo is unchanged
    - _Requirements: 8.1, 8.2_

- [x] 17. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-types -p tokeira-storage` and `cargo test -p tokeira-projection --features dsql` and verify all tests pass including property tests, unit tests, and (if DSQL available) integration tests.

## Notes

- All tests are required — none are marked optional per project convention.
- `DsqlProjectionLog` lives in `tokeira-storage/src/dsql/projection_log.rs` (read path + ProjectionLog trait)
- `DsqlVisibilityStore` lives in `tokeira-projection/src/dsql_store.rs` (implements `VisibilityStore` + `ProjectionSink` — checkpoint + visibility writes)
- The `DsqlConnectionAcquirer` trait is moved from `run_repository.rs` to `connection.rs` as `pub(crate)` so storage-crate modules can share it. Cross-crate DSQL visibility code uses `Arc<DsqlConnectionDirector>` directly.
- `vis_execution.run_id` is updated from TEXT to UUID in-place in V012 (schema version 1)
- No new migration files — V012 is updated in-place
- Cursor invariant: both `last_run_key` and `last_transition_seq` must be `Some` or both `None`
