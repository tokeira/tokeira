# Requirements Document

## Introduction

This spec covers Feature 6 (Projection Persistence) from the umbrella `dsql-storage-implementation` spec. It implements the read path for the projection log, projector checkpoint management, and the visibility sink that materializes `vis_execution` rows from projection operations.

The scope is:

1. **`ProjectionLog::read_from`** — partitioned projection log reads from the `projection_log` DSQL table, returning batches of `ProjectionRecord` with cursor advancement.
2. **Projector checkpoint management** — read and write of per-sink, per-substream cursors in the `projector_checkpoint` table, enabling projection workers to resume from their last committed position after restart.
3. **Visibility sink** — a DSQL-backed `VisibilityStore`/`ProjectionSink` implementation that processes `ProjectionOp::UpsertExecution` and `ProjectionOp::CloseExecution` operations to maintain `vis_execution` rows, the materialized read model for Temporal-compatible list/filter/count queries.
4. **`ExecutionStatus` stable numeric mapping** — a durable `SMALLINT` encoding for `ExecutionStatus` variants stored in `vis_execution.execution_status`, following the same pattern as `TaskKind::to_db_smallint`.

The authoritative architecture documents are [070-projection-plane](../../../docs/architecture/070-projection-plane.md) and [080-sql-visibility](../../../docs/architecture/080-sql-visibility.md). The in-memory implementation of `ProjectionLog` in `tokeira-storage/src/memory.rs` is the behavioral reference for `read_from`.

### What This Spec Covers

| Component | Table(s) | Description |
|---|---|---|
| `ProjectionLog::read_from` | `projection_log` | Partitioned log reads with cursor-based pagination |
| Checkpoint read | `projector_checkpoint` | Load last-applied cursor by substream-unique `sink_id` |
| Checkpoint write | `projector_checkpoint` | Upsert cursor after a batch is successfully applied |
| Visibility sink | `vis_execution` | Materialize/update execution rows from `ProjectionOp` |
| `ExecutionStatus` mapping | `vis_execution` | Stable `SMALLINT` encoding for execution status |

### What This Spec Does NOT Cover

- **Visibility query API** — list/filter/count queries against `vis_execution` are a separate `projection-visibility` spec.
- **Projection worker/consumer loop** — the runtime orchestration that calls `read_from`, applies ops to sinks, and writes checkpoints is a runtime concern.
- **New table DDL** — all tables (`projection_log`, `projector_checkpoint`, `vis_execution`) already exist from Feature 1 (`dsql-schema-connection`). This spec updates the schema-version-1 `V012__vis_execution.sql` definition in place so `vis_execution.run_id` is `UUID` rather than `TEXT`.
- **`projection_log` writes** — already implemented by `commit_transition` in Feature 2 (`dsql-core-persistence`).
- **Search attribute indexing** — custom search attribute columns and indexes are deferred.

### Dependencies

- Feature 1 (`dsql-schema-connection`) — schema DDL for `projection_log`, `projector_checkpoint`, `vis_execution`; `DsqlConnectionDirector`; codec module.
- Feature 2 (`dsql-core-persistence`) — `projection_log` writes in `commit_transition`; `DsqlRunRepository` struct.
- `dsql-spread-keys` — `RunKey::derive`, `dsql_spread_uuid`.

### Key DSQL Constraints Shaping This Design

- **OCC with Repeatable Read** — checkpoint upserts and visibility writes are subject to OCC conflict detection at commit time.
- **No temp tables** — cursor-based pagination uses SQL `WHERE` clauses with composite key ordering, not temp tables.
- **Postcard serialization** — `context_data`, `ops_data`, and `last_applied_cursor` are postcard-encoded BYTEA columns using the codec module.
- **`DbClass::Projection`** — all operations in this spec use `DbClass::Projection` connections, which are lower priority than `Commit` and `Read` traffic.

## Glossary

- **ProjectionLog**: The read-only trait in `tokeira-storage/src/api.rs` for projection workers to consume partitioned projection log substreams.
- **ProjectionBatch**: A page of `ProjectionRecord` values returned by `ProjectionLog::read_from`, together with a `next_cursor` for the subsequent call.
- **ProjectionRecord**: One row in the projection log, grouping all `ProjectionOp` values from a single transition. Contains `partition_id`, `fanout`, `run_key`, `transition_seq`, `context: ProjectionContext`, and `ops: Vec<ProjectionOp>`.
- **ProjectionContext**: Execution metadata snapshot stored alongside projection ops — namespace, workflow identity, status, timestamps, history length, and state transition count.
- **ProjectionOp**: Semantic projection operation emitted by the kernel. Two variants: `UpsertExecution` (update status, memo, search attributes) and `CloseExecution` (mark terminal with close timestamp).
- **ProjectionCursor**: Stable cursor for projector progress, shaped around `(partition_id, fanout, last_run_key, last_transition_seq)`. Serialized via postcard for the checkpoint table.
- **Projector_Checkpoint**: Per-sink, per-substream cursor tracking table keyed by `(sink_id, partition_id, fanout)`. Stores the `last_applied_cursor` as postcard-encoded BYTEA.
- **Vis_Execution**: Materialized visibility row store for Temporal-compatible list/filter/count queries. One row per `run_key`, upserted by the visibility sink.
- **Visibility_Sink**: The `ProjectionSink::apply` implementation on `DsqlVisibilityStore` that processes one `ProjectionRecord` at a time and writes to `vis_execution`.
- **ExecutionStatus**: Enum in `tokeira-types` representing workflow lifecycle states: `Running`, `Paused`, `Completed`, `Failed`, `Cancelled`, `Terminated`, `ContinuedAsNew`, `TimedOut`.
- **DsqlConnectionDirector**: The connection director from Feature 1 implementing class-based connection budget control with reservoir pattern.
- **DsqlPermit**: A held connection permit carrying a real `PoolConnection<Postgres>`, scoped to a `DbClass`.
- **DbClass**: Enumerated workload class. All operations in this spec use `DbClass::Projection`.
- **Codec**: The `tokeira-storage/src/dsql/codec.rs` module providing postcard-based `encode`/`decode` helpers for all BYTEA column types.
- **Sink_Id**: A `TEXT` identifier for a projection sink (e.g., `"visibility"`). Used as part of the checkpoint table's primary key.

## Requirements

### Requirement 1: ProjectionLog Read Path

**User Story:** As a Tokeira developer, I want `ProjectionLog::read_from` implemented against DSQL, so that projection workers can consume the partitioned projection log to maintain read models.

#### Acceptance Criteria

1. THE DsqlProjectionLog SHALL implement the `ProjectionLog` trait from `tokeira-storage/src/api.rs`.
2. WHEN `read_from` is called with a cursor at the beginning of a partition (no `last_run_key` or `last_transition_seq`), THE DsqlProjectionLog SHALL return the first `limit` records from the `projection_log` table for the given `(partition_id, fanout)`, ordered by `(run_key, transition_seq)` ascending.
3. WHEN `read_from` is called with a cursor that has `last_run_key` and `last_transition_seq`, THE DsqlProjectionLog SHALL return records strictly after that position in `(run_key, transition_seq)` order, up to `limit`.
4. WHEN `read_from` returns records, THE DsqlProjectionLog SHALL set `next_cursor` to the `(partition_id, fanout, run_key, transition_seq)` of the last returned record.
5. WHEN `read_from` returns no records (the partition is caught up), THE DsqlProjectionLog SHALL return the original cursor unchanged as `next_cursor`.
6. THE DsqlProjectionLog SHALL deserialize `context_data` using `codec::decode_projection_context` and `ops_data` using `codec::decode_projection_ops` for each returned row.
7. THE DsqlProjectionLog SHALL use `DbClass::Projection` when acquiring connections.
8. FOR ALL `ProjectionContext` values, serializing via the codec and then deserializing SHALL produce a value equal to the original (round-trip property).
9. FOR ALL `Vec<ProjectionOp>` values, serializing via the codec and then deserializing SHALL produce a value equal to the original (round-trip property).

### Requirement 2: Projector Checkpoint Read

**User Story:** As a Tokeira developer, I want to read the last-applied cursor for a projection sink's substream, so that projection workers can resume from their committed position after restart.

#### Acceptance Criteria

1. WHEN a checkpoint exists for the given `sink_id`, THE DsqlVisibilityStore SHALL return the deserialized `ProjectionCursor` from the `last_applied_cursor` column of `projector_checkpoint`.
2. WHEN no checkpoint exists for the given `sink_id`, THE DsqlVisibilityStore SHALL return `None`.
3. THE DsqlVisibilityStore SHALL deserialize `last_applied_cursor` using `codec::decode_projection_cursor`.
4. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.
5. FOR ALL `ProjectionCursor` values, serializing via the codec and then deserializing SHALL produce a value equal to the original (round-trip property).
6. THE caller (ProjectionWorker) SHALL ensure `sink_id` is unique per `(partition_id, fanout)` substream (e.g., `"visibility-p0-f1"`). The checkpoint table PK includes `(sink_id, partition_id, fanout)` for future multi-partition-per-sink support, but the current `load_checkpoint` trait signature takes only `sink_id`.

### Requirement 3: Projector Checkpoint Write

**User Story:** As a Tokeira developer, I want to persist the last-applied cursor for a projection sink's substream, so that projection progress survives restarts and failovers.

#### Acceptance Criteria

1. WHEN a checkpoint is written, THE DsqlVisibilityStore SHALL upsert a row in `projector_checkpoint` with the postcard-serialized `ProjectionCursor` as `last_applied_cursor`, deriving `partition_id` and `fanout` from the cursor.
2. WHEN a checkpoint row already exists for `(sink_id, partition_id, fanout)`, THE DsqlVisibilityStore SHALL update the `last_applied_cursor` and `updated_at` columns.
3. WHEN a checkpoint row does not exist, THE DsqlVisibilityStore SHALL insert a new row.
4. THE DsqlVisibilityStore SHALL serialize the cursor using `codec::encode_projection_cursor`.
5. THE DsqlVisibilityStore SHALL use `DbClass::Projection` when acquiring connections.

### Requirement 4: Visibility Sink — Upsert Execution

**User Story:** As a Tokeira developer, I want the visibility sink to materialize `vis_execution` rows from `ProjectionOp::UpsertExecution` operations, so that the visibility query API has up-to-date execution metadata.

#### Acceptance Criteria

1. WHEN the Visibility_Sink processes a `ProjectionRecord` containing `ProjectionOp::UpsertExecution`, THE Visibility_Sink SHALL upsert a row in `vis_execution` with the execution metadata from the record's `ProjectionContext`.
2. THE upserted `vis_execution` row SHALL contain: `run_key`, `namespace_id`, `workflow_id`, `run_id`, `workflow_type`, `task_queue`, `execution_status`, `start_time`, `execution_time`, `history_length`, and `state_transition_count` from the `ProjectionContext`.
3. THE Visibility_Sink SHALL store `execution_status` as a `SMALLINT` using the stable `ExecutionStatus` numeric mapping.
4. WHEN a `vis_execution` row already exists for the `run_key`, THE Visibility_Sink SHALL update the mutable fields: `execution_status`, `execution_time`, `history_length`, `state_transition_count`, and `memo`.
5. THE Visibility_Sink SHALL serialize `memo` as postcard-encoded BYTEA when the `UpsertExecution` operation includes a non-empty memo patch.
6. THE Visibility_Sink SHALL use `DbClass::Projection` when acquiring connections.

### Requirement 5: Visibility Sink — Close Execution

**User Story:** As a Tokeira developer, I want the visibility sink to update `vis_execution` rows from `ProjectionOp::CloseExecution` operations, so that closed workflows are correctly reflected in visibility queries.

#### Acceptance Criteria

1. WHEN the Visibility_Sink processes a `ProjectionRecord` containing `ProjectionOp::CloseExecution`, THE Visibility_Sink SHALL update the `vis_execution` row for the record's `run_key`.
2. THE update SHALL set `execution_status` to the terminal status from the `CloseExecution` operation, using the stable numeric mapping.
3. THE update SHALL set `close_time` to the `closed_at` timestamp from the `CloseExecution` operation.
4. THE update SHALL also set `history_length` and `state_transition_count` from the `ProjectionContext`.
5. IF no `vis_execution` row exists for the `run_key` when processing a `CloseExecution`, THEN THE Visibility_Sink SHALL insert a complete row using the `ProjectionContext` metadata combined with the close operation fields. This handles the case where the upsert was missed or the sink is catching up.

### Requirement 6: ExecutionStatus Stable Numeric Mapping

**User Story:** As a Tokeira developer, I want a stable numeric mapping for `ExecutionStatus` variants to `SMALLINT`, so that `vis_execution.execution_status` values are durable and consistent across code changes.

#### Acceptance Criteria

1. THE ExecutionStatus type SHALL provide a `to_db_smallint` method returning a stable `i16` value for each variant.
2. THE ExecutionStatus type SHALL provide a `TryFrom<i16>` implementation that decodes the `SMALLINT` back to the enum variant.
3. THE numeric mapping SHALL be: `Running = 0`, `Paused = 1`, `Completed = 2`, `Failed = 3`, `Cancelled = 4`, `Terminated = 5`, `ContinuedAsNew = 6`, `TimedOut = 7`.
4. WHEN an unknown `i16` value is encountered during decoding, THE implementation SHALL return an explicit error type (`ExecutionStatusDecodeError`).
5. FOR ALL `ExecutionStatus` variants, encoding to `i16` and then decoding SHALL produce the original variant (round-trip property).
6. THE numeric mapping SHALL be verified by a stability test that asserts the exact numeric value for each variant, preventing accidental reordering.

### Requirement 7: Visibility Store as DSQL Implementation

**User Story:** As a Tokeira developer, I want a DSQL-backed `VisibilityStore` implementation that also implements `ProjectionSink`, so that the `ProjectionWorker` can use it directly for both sink operations and checkpoint management.

#### Acceptance Criteria

1. THE DsqlVisibilityStore SHALL implement both `VisibilityStore` (from `tokeira-projection/src/store.rs`) and `ProjectionSink` (from `tokeira-projection/src/sink.rs`).
2. THE DsqlVisibilityStore SHALL live in `tokeira-projection/src/dsql_store.rs` (not `tokeira-storage`) to avoid a dependency cycle.
3. THE DsqlVisibilityStore SHALL accept a `DsqlConnectionDirector` reference for acquiring database connections.
4. THE `ProjectionSink::apply` implementation SHALL process a single `ProjectionRecord`, iterating over its `ops` in order so that an `UpsertExecution` followed by a `CloseExecution` in the same record produces the correct final state.
5. THE DsqlVisibilityStore SHALL be instrumented with `tracing::instrument` on all public methods.
6. THE `VisibilityStore` query methods (`list_executions`, `count_executions`, `count_from_rollup`, `resolve_attr`, `register_attr`, `get_row`) SHALL return `bail!("projection-visibility spec")` stubs for this spec. The full query implementation is deferred to the `projection-visibility` spec.
7. THE `VisibilityStore` write methods `upsert_execution` and `delete_execution` SHALL be fully implemented against DSQL. The search-attribute methods (`upsert_search_attr_index`, `remove_search_attr_index`) and `accumulate_rollup` SHALL return `bail!("projection-visibility spec")` stubs because the DSQL schema does not yet include search-attribute or rollup tables.
8. THE `ProjectionSink::apply` implementation SHALL NOT call stubbed search-attribute, rollup, or query methods in this spec; it SHALL write `vis_execution` directly and skip search-attribute/rollup side effects until the `projection-visibility` spec adds those tables.

### Requirement 8: Memo Persistence in Visibility

**User Story:** As a Tokeira developer, I want memo data persisted in `vis_execution`, so that visibility queries can return memo content without loading the full workflow state.

#### Acceptance Criteria

1. WHEN the Visibility_Sink processes an `UpsertExecution` with a non-empty `memo_patch`, THE Visibility_Sink SHALL merge the patch into the stored memo and persist the result as postcard-encoded BYTEA in `vis_execution.memo`.
2. WHEN the Visibility_Sink processes an `UpsertExecution` with an empty `memo_patch`, THE Visibility_Sink SHALL leave the existing `memo` column unchanged.
3. FOR ALL `Memo` values, serializing via postcard and then deserializing SHALL produce a value equal to the original (round-trip property).

### Requirement 9: Tracing Instrumentation

**User Story:** As a Tokeira developer, I want all DSQL projection persistence methods instrumented with tracing, so that operational issues can be diagnosed from structured logs.

#### Acceptance Criteria

1. THE DsqlProjectionLog SHALL annotate all `ProjectionLog` trait methods with `tracing::instrument`.
2. THE DsqlVisibilityStore SHALL annotate checkpoint read and write methods (`load_checkpoint`, `save_checkpoint`) with `tracing::instrument`.
3. THE Visibility_Sink SHALL annotate all public methods, including `ProjectionSink::apply`, with `tracing::instrument`.
4. THE instrumentation SHALL include relevant parameters (partition_id, fanout, sink_id, run_key) as span fields where appropriate, excluding large serialized payloads.
