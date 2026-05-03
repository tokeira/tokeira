# Implementation Plan: DSQL Projection Persistence

## Overview

Implement the projection read path (`DsqlProjectionLog`), projector checkpoint management, visibility sink (`DsqlVisibilitySink`), and `ExecutionStatus` stable numeric mapping. The implementation adds two new files to `dsql/`, a migration for `vis_execution.run_id`, and the `to_db_smallint` / `TryFrom<i16>` methods on `ExecutionStatus` in `tokeira-types`.

## Tasks

- [ ] 1. Add `ExecutionStatus` stable numeric mapping to `tokeira-types`
  - [ ] 1.1 Add `ExecutionStatusDecodeError` and `to_db_smallint` / `TryFrom<i16>` to `ExecutionStatus`
    - Add `use thiserror::Error;` to `tokeira-types/src/execution.rs`
    - Add `#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)] #[error("unknown execution status database value {value}")] pub struct ExecutionStatusDecodeError { pub value: i16 }` 
    - Add `impl ExecutionStatus { pub fn to_db_smallint(self) -> i16 { match self { Self::Running => 0, Self::Paused => 1, Self::Completed => 2, Self::Failed => 3, Self::Cancelled => 4, Self::Terminated => 5, Self::ContinuedAsNew => 6, Self::TimedOut => 7 } } }`
    - Add `impl TryFrom<i16> for ExecutionStatus` with the reverse mapping, returning `ExecutionStatusDecodeError` for unknown values
    - Follow the exact pattern from `TaskKind::to_db_smallint` / `TryFrom<i16>` in `tokeira-types/src/task_queue.rs`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ] 1.2 Write stability test for `ExecutionStatus` numeric mapping
    - Add `#[test] fn execution_status_database_mapping_is_stable()` in `tokeira-types/src/execution.rs`
    - Assert exact values: `Running=0, Paused=1, Completed=2, Failed=3, Cancelled=4, Terminated=5, ContinuedAsNew=6, TimedOut=7`
    - Assert round-trip for each variant: `TryFrom::<i16>::try_from(variant.to_db_smallint()) == Ok(variant)`
    - Assert unknown values return error: `TryFrom::<i16>::try_from(8).is_err()`, `TryFrom::<i16>::try_from(-1).is_err()`
    - _Requirements: 6.3, 6.4, 6.6_

  - [ ] 1.3 Write property test for `ExecutionStatus` round-trip (Property 3)
    - **Feature: dsql-projection-persistence, Property 3: ExecutionStatus Numeric Round-Trip**
    - Use `proptest` with `prop_oneof!` to generate random `ExecutionStatus` variants
    - Verify `TryFrom::<i16>::try_from(status.to_db_smallint()) == Ok(status)` for all generated variants
    - Minimum 100 iterations
    - _Requirements: 6.5_

- [ ] 2. Checkpoint — Ensure `tokeira-types` tests pass
  - Run `cargo test -p tokeira-types` and verify all tests pass including the new stability and property tests.

- [ ] 3. Add `vis_execution.run_id` UUID migration
  - [ ] 3.1 Create migration `V013__vis_execution_run_id_uuid.sql`
    - Create `tokeira/crates/tokeira-storage/migrations/V013__vis_execution_run_id_uuid.sql`
    - Content: `ALTER TABLE vis_execution ALTER COLUMN run_id TYPE UUID USING run_id::uuid;`
    - This is safe because no rows exist yet in `vis_execution`
    - _Requirements: 4.2 (run_id field in vis_execution)_

- [ ] 4. Create `DsqlProjectionLog` in `dsql/projection_log.rs`
  - [ ] 4.1 Create the `DsqlProjectionLog` struct and constructors
    - Create new file `tokeira/crates/tokeira-storage/src/dsql/projection_log.rs`
    - Define `pub struct DsqlProjectionLog { director: Arc<dyn DsqlConnectionAcquirer> }`
    - Implement `pub fn new(director: Arc<DsqlConnectionDirector>) -> Self` that casts to `Arc<dyn DsqlConnectionAcquirer>`
    - Implement `#[cfg(test)] fn new_with_acquirer(director: Arc<dyn DsqlConnectionAcquirer>) -> Self` for testing
    - Add `use` imports for `DsqlConnectionAcquirer`, `DsqlConnectionDirector`, `DsqlPermit`, `DbClass`, `ProjectionBatch`, `ProjectionRecord`, `ProjectionContext`, `ProjectionLog`, `ProjectionCursor`, codec helpers, `anyhow::Result`, `async_trait`, `tracing::instrument`, `tokeira_kernel::ProjectionOp`, `tokeira_types::{RunKey, TransitionSeq}`
    - _Requirements: 1.1, 1.7_

  - [ ] 4.2 Implement `ProjectionLog::read_from`
    - Add `#[instrument(name = "dsql.read_from", skip(self), fields(partition_id = cursor.partition_id, fanout = cursor.fanout, limit))]`
    - Acquire `DbClass::Projection` permit via `self.director.acquire(DbClass::Projection).await?`
    - If `cursor.last_run_key.is_none()` (beginning of partition): execute the beginning-of-partition query with `partition_id`, `fanout`, `limit`
    - If cursor has position: execute the cursor-based query with `partition_id`, `fanout`, `last_run_key`, `last_transition_seq`, `limit` using `(run_key, transition_seq) > ($3, $4)` row-value comparison
    - For each returned row: decode `context_data` via `codec::decode_projection_context`, decode `ops_data` via `codec::decode_projection_ops`
    - Build `ProjectionRecord` for each row with `partition_id`, `fanout` from cursor, `run_key`, `transition_seq`, decoded `context`, decoded `ops`
    - Set `next_cursor`: if records non-empty, use last record's `(partition_id, fanout, run_key, transition_seq)`; if empty, return original cursor unchanged
    - Return `ProjectionBatch { records, next_cursor }`
    - Bind `transition_seq` as `i64` using checked conversion from `TransitionSeq(u64)` — use the existing `i64_from_u64` helper pattern
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [ ] 4.3 Implement `read_checkpoint`
    - Add `#[instrument(name = "dsql.read_checkpoint", skip(self), fields(sink_id = %sink_id, partition_id, fanout))]`
    - Acquire `DbClass::Projection` permit
    - Execute `SELECT last_applied_cursor FROM projector_checkpoint WHERE sink_id = $1 AND partition_id = $2 AND fanout = $3`
    - If row exists: decode `last_applied_cursor` via `codec::decode_projection_cursor`, return `Some(cursor)`
    - If no row: return `None`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ] 4.4 Implement `write_checkpoint`
    - Add `#[instrument(name = "dsql.write_checkpoint", skip(self, cursor), fields(sink_id = %sink_id, partition_id = cursor.partition_id, fanout = cursor.fanout))]`
    - Acquire `DbClass::Projection` permit
    - Serialize cursor via `codec::encode_projection_cursor`
    - Execute `INSERT INTO projector_checkpoint (sink_id, partition_id, fanout, last_applied_cursor, updated_at) VALUES ($1, $2, $3, $4, now()) ON CONFLICT (sink_id, partition_id, fanout) DO UPDATE SET last_applied_cursor = EXCLUDED.last_applied_cursor, updated_at = now()`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 5. Register `projection_log` module in `dsql/mod.rs` and wire into `DsqlStore`
  - [ ] 5.1 Add `pub mod projection_log;` to `dsql/mod.rs` and `pub use projection_log::*;`
    - Add the module declaration and re-export
    - _Requirements: 1.1_

  - [ ] 5.2 Add `projection_log` field to `DsqlStore` and construct in `from_connector`
    - Add `projection_log: projection_log::DsqlProjectionLog` field to `DsqlStore`
    - Construct `DsqlProjectionLog::new(Arc::clone(&director))` in `from_connector`
    - Add `pub fn projection_log(&self) -> &projection_log::DsqlProjectionLog { &self.projection_log }` accessor
    - _Requirements: 1.1_

- [ ] 6. Checkpoint — Ensure compilation passes
  - Run `cargo check -p tokeira-storage` and verify the new module compiles without errors.

- [ ] 7. Create `DsqlVisibilitySink` in `dsql/visibility_sink.rs`
  - [ ] 7.1 Create the `DsqlVisibilitySink` struct and constructors
    - Create new file `tokeira/crates/tokeira-storage/src/dsql/visibility_sink.rs`
    - Define `pub struct DsqlVisibilitySink { director: Arc<dyn DsqlConnectionAcquirer> }`
    - Implement `pub fn new(director: Arc<DsqlConnectionDirector>) -> Self`
    - Implement `#[cfg(test)] fn new_with_acquirer(director: Arc<dyn DsqlConnectionAcquirer>) -> Self`
    - _Requirements: 7.1, 7.2_

  - [ ] 7.2 Implement `apply_batch` — UpsertExecution path
    - Add `#[instrument(name = "dsql.visibility_sink.apply_batch", skip(self, records), fields(record_count = records.len()))]`
    - Acquire `DbClass::Projection` permit
    - Iterate over each `ProjectionRecord` in the batch
    - For each record, iterate over `record.ops` in order
    - For `ProjectionOp::UpsertExecution { status, memo_patch, .. }`:
      - If `memo_patch` is empty: execute the upsert SQL with `memo = NULL` (CASE preserves existing)
      - If `memo_patch` is non-empty: read existing memo from `vis_execution` (if any), deserialize, merge patch keys, serialize merged memo, execute upsert with merged memo
      - Bind: `run_key`, `namespace_id`, `workflow_id`, `run_id.0` (UUID), `workflow_type`, `task_queue`, `status.to_db_smallint()`, `start_time`, `execution_time`, `history_length`, `state_transition_count`, memo BYTEA (or NULL)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 7.3, 8.1, 8.2_

  - [ ] 7.3 Implement `apply_batch` — CloseExecution path
    - For `ProjectionOp::CloseExecution { status, closed_at }`:
      - Execute `UPDATE vis_execution SET execution_status = $1, close_time = $2, history_length = $3, state_transition_count = $4 WHERE run_key = $5`
      - Bind `status.to_db_smallint()`, `closed_at`, `record.context.history_length`, `record.context.state_transition_count`, `record.run_key`
      - If UPDATE affects 0 rows (catch-up case): execute full INSERT using `record.context` metadata combined with close operation fields
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 7.3, 7.4_

- [ ] 8. Register `visibility_sink` module in `dsql/mod.rs` and wire into `DsqlStore`
  - [ ] 8.1 Add `pub mod visibility_sink;` to `dsql/mod.rs` and `pub use visibility_sink::*;`
    - Add the module declaration and re-export
    - _Requirements: 7.1_

  - [ ] 8.2 Add `visibility_sink` field to `DsqlStore` and construct in `from_connector`
    - Add `visibility_sink: visibility_sink::DsqlVisibilitySink` field to `DsqlStore`
    - Construct `DsqlVisibilitySink::new(Arc::clone(&director))` in `from_connector`
    - Add `pub fn visibility_sink(&self) -> &visibility_sink::DsqlVisibilitySink { &self.visibility_sink }` accessor
    - _Requirements: 7.1, 7.2_

- [ ] 9. Checkpoint — Ensure compilation passes
  - Run `cargo check -p tokeira-storage` and verify both new modules compile without errors.

- [ ] 10. Property-based tests
  - [ ] 10.1 Write property test for cursor-based pagination correctness (Property 1)
    - **Feature: dsql-projection-persistence, Property 1: Cursor-Based Pagination Correctness**
    - Extract a pure `interpret_read_from` helper that takes a sorted slice of `(RunKey, TransitionSeq)` pairs, a cursor `(Option<RunKey>, Option<TransitionSeq>)`, and a limit, and returns the expected result indices and next cursor
    - Use `proptest` to generate: random `Vec<(RunKey, TransitionSeq)>` (sorted), random cursor position (None or a valid position from the vec), random limit (1..=50)
    - Verify: returned records are strictly after cursor, in ascending order, limited to `limit`, and `next_cursor` points to the last returned record (or original cursor if empty)
    - Minimum 100 iterations
    - Test location: `tokeira-storage/src/dsql/projection_log.rs`
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

  - [ ] 10.2 Write property test for projection codec round-trip (Property 2)
    - **Feature: dsql-projection-persistence, Property 2: Projection Codec Round-Trip**
    - Add `Arbitrary` implementations (or `proptest` strategies) for `ProjectionContext`, `ProjectionOp`, and `ProjectionCursor` (with and without `last_run_key`/`last_transition_seq`)
    - Verify `decode_projection_context(encode_projection_context(&ctx)?) == Ok(ctx)` for all generated `ProjectionContext` values
    - Verify `decode_projection_ops(encode_projection_ops(&ops)?) == Ok(ops)` for all generated `Vec<ProjectionOp>` values
    - Verify `decode_projection_cursor(encode_projection_cursor(&cursor)?) == Ok(cursor)` for all generated `ProjectionCursor` values
    - Minimum 100 iterations per type
    - Test location: `tokeira-storage/src/dsql/codec.rs` (extend existing proptest block)
    - _Requirements: 1.8, 1.9, 2.5_

  - [ ] 10.3 Write property test for Memo codec round-trip (Property 4)
    - **Feature: dsql-projection-persistence, Property 4: Memo Codec Round-Trip**
    - Generate random `Memo` values (BTreeMap<String, Payload> with random keys and payload data)
    - Verify `decode::<Memo>(encode(&memo)?) == Ok(memo)` for all generated values
    - Minimum 100 iterations
    - Test location: `tokeira-storage/src/dsql/codec.rs`
    - _Requirements: 8.3_

  - [ ] 10.4 Write property test for visibility sink operation ordering (Property 5)
    - **Feature: dsql-projection-persistence, Property 5: Visibility Sink Operation Ordering**
    - Extract a pure `resolve_final_vis_state` helper that takes a `Vec<ProjectionOp>` and a `ProjectionContext`, and returns the final `(ExecutionStatus, Option<OffsetDateTime>)` (status, close_time)
    - Use `proptest` to generate random `ProjectionRecord` values containing 1–4 ops (mix of `UpsertExecution` and `CloseExecution`)
    - Verify: the final status and close_time match the last operation in the sequence
    - Minimum 100 iterations
    - Test location: `tokeira-storage/src/dsql/visibility_sink.rs`
    - _Requirements: 7.4, 5.1, 5.2, 5.3_

- [ ] 11. Unit tests for `DsqlProjectionLog`
  - [ ] 11.1 Write unit test for `DbClass::Projection` routing on `read_from`
    - Use `RecordingAcquirer` mock to verify `read_from` acquires `DbClass::Projection`
    - _Requirements: 1.7_

  - [ ] 11.2 Write unit test for `DbClass::Projection` routing on `read_checkpoint` and `write_checkpoint`
    - Use `RecordingAcquirer` mock to verify both methods acquire `DbClass::Projection`
    - _Requirements: 2.4, 3.5_

  - [ ] 11.3 Write unit test for beginning-of-partition cursor behavior
    - Verify that when `cursor.last_run_key.is_none()`, the query does not include the row-value comparison predicate
    - Can be tested via the pure `interpret_read_from` helper with a beginning cursor
    - _Requirements: 1.2_

  - [ ] 11.4 Write unit test for empty partition returns original cursor
    - Verify `read_from` on an empty result returns `next_cursor == input cursor`
    - Can be tested via the pure helper
    - _Requirements: 1.5_

- [ ] 12. Unit tests for `DsqlVisibilitySink`
  - [ ] 12.1 Write unit test for `DbClass::Projection` routing on `apply_batch`
    - Use `RecordingAcquirer` mock to verify `apply_batch` acquires `DbClass::Projection`
    - _Requirements: 4.6_

  - [ ] 12.2 Write unit test for `ExecutionStatus` encoding in visibility writes
    - Verify that the visibility sink uses `to_db_smallint()` when binding `execution_status`
    - Can be tested by processing a record and checking the bound value via a mock or by verifying the pure helper
    - _Requirements: 4.3_

  - [ ] 12.3 Write unit test for CloseExecution catch-up insert
    - Verify that processing a `CloseExecution` without a prior `UpsertExecution` produces a complete row
    - Test via the pure `resolve_final_vis_state` helper: a single `CloseExecution` op should produce the terminal status and close_time
    - _Requirements: 5.5_

  - [ ] 12.4 Write unit test for memo merge behavior
    - Verify that processing `UpsertExecution` with memo `{a: 1}` then `UpsertExecution` with memo_patch `{b: 2}` produces merged memo `{a: 1, b: 2}`
    - Verify that processing `UpsertExecution` with memo then `UpsertExecution` with empty memo_patch preserves the original memo
    - Test via a pure memo merge helper
    - _Requirements: 8.1, 8.2_

- [ ] 13. Unit tests for `ExecutionStatus` (additional edge cases)
  - [ ] 13.1 Write unit test for unknown `i16` values
    - Verify `TryFrom::<i16>::try_from(8)` returns `Err(ExecutionStatusDecodeError { value: 8 })`
    - Verify `TryFrom::<i16>::try_from(-1)` returns `Err(ExecutionStatusDecodeError { value: -1 })`
    - Verify `TryFrom::<i16>::try_from(100)` returns `Err(ExecutionStatusDecodeError { value: 100 })`
    - _Requirements: 6.4_

- [ ] 14. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-types -p tokeira-storage` and verify all tests pass.

- [ ] 15. Tracing instrumentation verification
  - [ ] 15.1 Verify all public methods have `#[instrument]` annotations
    - `DsqlProjectionLog::read_from` — `#[instrument(name = "dsql.read_from", ...)]`
    - `DsqlProjectionLog::read_checkpoint` — `#[instrument(name = "dsql.read_checkpoint", ...)]`
    - `DsqlProjectionLog::write_checkpoint` — `#[instrument(name = "dsql.write_checkpoint", ...)]`
    - `DsqlVisibilitySink::apply_batch` — `#[instrument(name = "dsql.visibility_sink.apply_batch", ...)]`
    - Verify span fields include relevant parameters (partition_id, fanout, sink_id, run_key, record_count) and exclude large serialized payloads
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [ ] 16. Integration tests (gated behind `dsql-integration` feature)
  - [ ] 16.1 Integration test: `read_from` pagination cycle
    - Insert multiple `projection_log` rows via `commit_transition`
    - Read with beginning cursor, verify first batch
    - Read with returned `next_cursor`, verify subsequent batch
    - Continue until all records consumed
    - Verify total records match inserted count and ordering is correct
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

  - [ ] 16.2 Integration test: checkpoint persist and resume
    - Write a checkpoint for `("visibility", partition_id, fanout)`
    - Read it back, verify the cursor matches
    - Write a different cursor for the same key
    - Read it back, verify the updated cursor
    - Read a non-existent checkpoint, verify `None`
    - _Requirements: 2.1, 2.2, 3.1, 3.2, 3.3_

  - [ ] 16.3 Integration test: visibility sink end-to-end
    - Start a workflow via `commit_transition` (produces projection_log rows)
    - Read projection records via `read_from`
    - Apply records via `DsqlVisibilitySink::apply_batch`
    - Query `vis_execution` directly to verify the materialized row has correct fields
    - Complete the workflow, read the `CloseExecution` record, apply it
    - Verify `vis_execution` row has terminal status and close_time
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 5.4_

  - [ ] 16.4 Integration test: CloseExecution catch-up insert
    - Process a `CloseExecution` record for a `run_key` that has no existing `vis_execution` row
    - Verify a complete row is inserted with the context metadata and close fields
    - _Requirements: 5.5_

  - [ ] 16.5 Integration test: memo merge end-to-end
    - Process an `UpsertExecution` with memo `{key_a: payload_a}`
    - Process another `UpsertExecution` with memo_patch `{key_b: payload_b}`
    - Query `vis_execution.memo`, deserialize, verify both keys are present
    - Process an `UpsertExecution` with empty memo_patch, verify memo is unchanged
    - _Requirements: 8.1, 8.2_

- [ ] 17. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-types -p tokeira-storage` and verify all tests pass including property tests, unit tests, and (if DSQL available) integration tests.

## Notes

- All tests are required — none are marked optional per project convention.
- Property tests target pure helper functions extracted from the SQL-dependent code, keeping them fast and deterministic.
- The `RecordingAcquirer` mock (already exists in `run_repository.rs` tests) is reused for `DbClass::Projection` routing verification.
- Each task references specific requirements for traceability.
- Checkpoints ensure incremental validation.
- No schema changes to `projection_log` or `projector_checkpoint` — only `vis_execution.run_id` gets a type change via V013.
- The `DsqlConnectionAcquirer` trait (already defined in `run_repository.rs`) is reused by both new modules. It may need to be moved to a shared location (e.g., `connection.rs`) or re-exported — handle this during task 4.1 if needed.
