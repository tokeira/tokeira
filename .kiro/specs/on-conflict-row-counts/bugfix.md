# Bugfix Requirements Document

## Introduction

On Aurora DSQL, an `INSERT … ON CONFLICT` that leaves a row unwritten reports the row as written. This happens with `DO NOTHING` and with a `DO UPDATE … WHERE` whose condition is false. The stored data is right, and so is the statement's `RETURNING` output, which gives back only the rows it wrote. PostgreSQL counts only the rows inserted or updated ([`INSERT`, Outputs](https://www.postgresql.org/docs/current/sql-insert.html)), so tests that run on PostgreSQL can't see the difference.

The evidence was observed on Aurora DSQL with test data:
- A lone `DO NOTHING` conflict reports `INSERT 0 1`; one new row and one conflict report `INSERT 0 2`. With `RETURNING`, the conflict gives back no row.
- The shard lease's own insert, run in a transaction against another node's live lease, reports one row. The lease still names the other node, and the transaction wrote nothing.
- The CHASM current-run pointer's upsert, with a `WHERE` that doesn't match, reports `INSERT 0 1` and leaves the row unchanged. The result is the same when the expected version is bound as NULL. With `RETURNING`, the same statement gives back no row. With a `WHERE` that matches, it reports `INSERT 0 1` and updates the row.
- Statements that write nothing report it correctly: `UPDATE 0`, `DELETE 0`, and `INSERT 0 0` for an `INSERT … SELECT … WHERE false`. So the miscount is in DSQL's command tag after an `ON CONFLICT` clause that skips the row, not in how the client displays it.

Nine storage operations read that count through sqlx's `rows_affected()`, and on DSQL each takes the skipped row as written:
- a node is told it holds a shard lease another node holds;
- a controller takes a namespace slot another controller holds;
- a duplicate provider action and a lost configuration write are reported as committed;
- a provenance digest conflict is reported as a new row;
- a CHASM execution commits while the current-run pointer names another run;
- the visibility projection replaces or clears the search-attribute index of a newer version;
- a backfill and a dispatch metric over-count.

This spec covers those nine operations and a check that keeps the count out of any future one. Other `ON CONFLICT` statements either always write their row or never read the count; Unchanged Behavior lists them.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a node asks for a shard lease whose row exists, held by itself or another node, live or expired, THEN on DSQL the system skips the takeover (`try_acquire_bundle` and `interpret_acquire`, `crates/tokeira-storage/src/dsql/run_repository/leases.rs`):
- When the stored epoch is 1, it reports the lease acquired at epoch 1 without writing it.
- When the epoch is higher, it fails with "new shard lease returned unexpected epoch".
- So two nodes can hold the lease at the same epoch, which fencing can't tell apart; an owner's own acquire doesn't extend its expiry; and no node can take over an expired lease past epoch 1.

1.2 WHEN a worker-compute controller is admitted while another controller holds the namespace's first slot THEN on DSQL the system assigns that slot to it anyway and makes it active, so every admitted controller takes slot 0 and the namespace's slot limit never refuses one (`admit_controller`, `crates/tokeira-storage/src/dsql/worker_compute_repository.rs`)

1.3 WHEN a worker-compute decision commits with a provider action whose id is already stored THEN on DSQL the system commits the controller's next state instead of answering `Conflict`, while the stored action keeps its earlier content (`insert_action` and `commit_decision`, `worker_compute_repository.rs`)

1.4 WHEN worker-task provenance is stored for a digest that already has a row THEN on DSQL the system answers `Inserted`, whether the stored row is equal, which should answer `AlreadyPresent`, or different, which should fail with `DigestConflict` (`put`, `crates/tokeira-storage/src/dsql/worker_task_provenance.rs`)

1.5 WHEN a task queue's configuration is created, with no expected revision, while another writer has already created it THEN on DSQL the system answers `Applied` with the caller's revision, though the stored configuration is the other writer's (`execute_cas` and its caller, `crates/tokeira-storage/src/dsql/task_queue_config.rs`)

1.6 WHEN the CHASM current-execution backfill copies a page in which a key conflicts THEN on DSQL the system counts the conflicting row as copied (`backfill_current_executions`, `crates/tokeira-storage/src/dsql/chasm_node.rs`). The backfill runs only at bootstrap, after other writers have stopped, and its statement already skips keys that exist, so no key conflicts in that use; the count is wrong only if one does.

1.7 WHEN a CHASM execution is persisted while its current-run pointer no longer holds what the persist expected (another run, another version, or any pointer where the persist expected none) THEN on DSQL the pointer's guarded upsert writes nothing but reports a row (`persist_new_execution`, `chasm_node.rs`). The system commits the execution's nodes while the pointer names the other run: a torn current-run state, which the pointer's guard exists to prevent.

1.8 WHEN the visibility projection applies a record, or a deletion, that is no newer than the stored row by the time its upsert runs, having raced a newer one past the optimistic version check before it, THEN on DSQL the upsert's version guard writes nothing but reports a row (`upsert_execution_row` and its callers, `crates/tokeira-projection/src/dsql_store.rs`). The system replaces the run's search-attribute index rows with the stale record's, or clears them for the stale deletion. That erases the index of the newer visible version, and filtered List and Count queries return wrong runs.

1.9 WHEN durable dispatch persists backlog entries whose keys already exist THEN on DSQL the backlog's rows-written metric counts them as written (`do_persist_to_backlog`, `crates/tokeira-storage/src/dsql/run_repository/dispatch.rs`)

### Expected Behavior (Correct)

2.1 WHEN a node asks for a shard lease THEN the system SHALL decide from the rows its statements wrote, on DSQL as on PostgreSQL:
- with no lease row, it SHALL write one at epoch 1 and report the lease acquired at epoch 1;
- with a live row the caller holds, it SHALL extend the lease and report it acquired at the same epoch;
- with an expired row, or one held by no one, it SHALL take the lease over at the next epoch and report it acquired at that epoch;
- with a live row another node holds, it SHALL write nothing and report the lease rejected, with the holder and its epoch.

2.2 WHEN a worker-compute controller is admitted THEN the system SHALL assign it the lowest slot its insert wrote, and SHALL make it capacity-limited, holding no slot, when every slot under the namespace limit is held.

2.3 WHEN a worker-compute decision commits with a provider action whose id is already stored THEN the system SHALL answer `Conflict` and SHALL write nothing.

2.4 WHEN worker-task provenance is stored THEN the system SHALL answer `Inserted` only when it wrote the row. For a digest that already has a row, it SHALL answer `AlreadyPresent` when the stored row is equal, and fail with `DigestConflict` otherwise.

2.5 WHEN a task queue's configuration is created with no expected revision THEN the system SHALL answer `Applied` only when it wrote the row, and `Conflict` when a row already exists.

2.6 WHEN the CHASM current-execution backfill copies a page THEN the system SHALL return the number of rows it wrote.

2.7 WHEN a CHASM execution is persisted THEN the system SHALL commit its nodes only when the pointer's upsert wrote the pointer. Otherwise it SHALL roll back and answer the pointer conflict, writing nothing.

2.8 WHEN the visibility projection applies a record or a deletion THEN the system SHALL replace or clear the run's search-attribute index rows only when the upsert wrote the row.

2.9 WHEN durable dispatch persists backlog entries THEN the backlog's rows-written metric SHALL count the entries the insert wrote.

2.10 The rows an `INSERT … ON CONFLICT` wrote, when its conflict clause can leave the row unwritten (`DO NOTHING`, or `DO UPDATE … WHERE`), SHALL be counted from the rows its `RETURNING` clause gives back, never from the statement's command tag. A function that runs such a statement SHALL NOT read `rows_affected()`, and a test in the default suite SHALL fail when one does.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN no conflicting row exists, or a guarded upsert's condition holds, THEN each operation SHALL CONTINUE TO write and answer as today.

3.2 On PostgreSQL, each operation SHALL CONTINUE TO answer as today, conflicts included, since PostgreSQL's count and the returned rows agree.

3.3 The in-memory stores SHALL CONTINUE TO answer each operation as today; they run no SQL.

3.4 Each operation SHALL CONTINUE TO run in the same transaction, under the same fences, with the same isolation and retry handling, and SHALL write the same rows.

3.5 These statements SHALL CONTINUE as today:
- Upserts whose `DO UPDATE` has no `WHERE` always write their row, and DSQL counts it. This covers the run commit's writes (`crates/tokeira-storage/src/dsql/run_repository/commit.rs`), the CHASM node writes, the worker-compute controller and worker-deployment records, and the projection's rollup and checkpoint writes.
- These use a skipping conflict clause but never decide from its count:
  - the control lease's claim insert, which locks and reads the claim back (`crates/tokeira-storage/src/dsql/control_lease.rs`);
  - the migration runner's inserts into `schema_version` and `schema_compatibility`, each read back by a `SELECT` (`crates/tokeira-storage/src/dsql/migration.rs`); its idempotency classifier matches migration text and runs nothing;
  - the CHASM backfill marker's insert (`set_backfill_marker`, `chasm_node.rs`);
  - the worker-compute queue sample's guarded upsert (`put_queue_sample`, `worker_compute_repository.rs`);
  - the projection's attribute registration, which already counts its `RETURNING` row, and its search-attribute index insert, whose result is ignored (`dsql_store.rs`).

### Out of Scope

- How DSQL reports the count. Tokeira counts the returned rows, which is right on both engines.
