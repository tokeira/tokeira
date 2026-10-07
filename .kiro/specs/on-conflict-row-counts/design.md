# On-Conflict Row Counts — Bugfix Design

## Overview

The nine operations stop reading the command tag after an `ON CONFLICT` clause that can skip the row, and decide from the rows the statement returns:
- Each such statement gains a `RETURNING` clause and is fetched rather than executed. A single-row statement wrote its row exactly when a row comes back; a multi-row statement wrote as many rows as come back.
- Where an operation follows the statement with an `UPDATE` and decides from that too, the `UPDATE` returns its row as well, so the operation decides every outcome the same way.
- A source check in the default test suite fails when a function that runs such a statement reads `rows_affected()`.

## Glossary

- **Command tag:** the status a PostgreSQL-protocol server returns for a statement, such as `INSERT 0 1`. sqlx exposes its count as `PgQueryResult::rows_affected()`.
- **Skipping statement:** an `INSERT … ON CONFLICT` whose conflict clause can leave the row unwritten. That is `DO NOTHING`, or a `DO UPDATE` with a `WHERE` condition.
- **Returned rows:** the rows a statement's `RETURNING` clause gives back. On PostgreSQL and on DSQL, these are the rows the statement wrote.

## How this maps onto Tokeira's architecture

DSQL is the store Tokeira is built around, and its behaviour is the contract here. Three facts shape the fix.

1. **Only DSQL misreports.** On PostgreSQL the command tag and the returned rows agree, so a statement that counts its returned rows answers the same on both engines. The fix changes no behaviour on PostgreSQL.
2. **Tests on PostgreSQL can't see the defect.** The live DSQL suites that would have caught the lease and pointer cases connect only when an operator points them at a database (`docs/testing/dsql-live-suites.md`). So the fix's proof runs on a DSQL cluster created for the run, and the default suite gains a source check that needs no database.
3. **The in-memory stores run no SQL.** They keep their own maps and aren't affected.

## Bug Details

### Bug Condition

An operation reads `rows_affected()` after a skipping statement that skipped its row, on DSQL:
- a shard lease asked for while its row exists (1.1);
- a controller admitted while another holds a slot it tries (1.2);
- a decision committed with a stored action id (1.3);
- provenance stored for a digest with a row (1.4);
- a configuration created while another writer's exists (1.5);
- a backfill page with a conflicting key (1.6);
- a CHASM execution persisted against a pointer that no longer holds what it expected (1.7);
- a visibility record or deletion no newer than the stored row (1.8);
- backlog entries whose keys exist (1.9).

### Examples

- **Lease held live.** Node A holds shard 7 at epoch 1, and node B asks for it. On PostgreSQL B is rejected. On DSQL, B's insert reports a row, so B skips the takeover, reads epoch 1, and is told it acquired the shard at epoch 1, while the row still names A. Both nodes now write under epoch 1.
- **Lease expired past epoch 1.** Node A's lease on shard 7 expired at epoch 3, and node B asks for it. On PostgreSQL B takes it over at epoch 4. On DSQL, B's acquire fails with "new shard lease returned unexpected epoch 3", and so does every later attempt, A's included.
- **Concurrent CHASM creates.** Two creates of the same business id race. Both expected no pointer, and the first writes it. On PostgreSQL the second's upsert writes nothing, so it rolls back. On DSQL the second commits its nodes, and the pointer still names the first run.
- **Out-of-order visibility record.** A visibility record at transition 7 passes the projection's version check, then loses the race to a record at transition 9. On PostgreSQL its upsert writes nothing, and transition 9's index stands. On DSQL the projection takes it as applied, and replaces transition 9's search-attribute index with transition 7's.
- **Concurrent configuration creates.** Two writers create the same task queue's configuration. On PostgreSQL the second gets `Conflict` and rereads. On DSQL both get `Applied`, and the second writer's change is lost without anyone knowing.

## Expected Behavior

### Preservation Requirements

- Without a conflicting row, or with a guard that holds, every operation writes and answers as today (3.1).
- On PostgreSQL and in the in-memory stores, every operation answers as today (3.2, 3.3).
- Transactions, fences, retries and the rows written are unchanged (3.4).
- Statements that always write, or never decide from the count, are unchanged (3.5).

## Root Cause

Each operation reads `rows_affected()` from a skipping statement:

- `try_acquire_bundle` runs the takeover `UPDATE` only when the insert's count is 0, and `interpret_acquire` maps the insert and update counts to an outcome (`crates/tokeira-storage/src/dsql/run_repository/leases.rs`).
- `admit_controller` takes the first slot whose insert counts 1 (`crates/tokeira-storage/src/dsql/worker_compute_repository.rs`).
- `insert_action` returns the insert's count, and `commit_decision` answers `Conflict` unless it is 1 (`worker_compute_repository.rs`).
- `put` answers `Inserted` when the insert counts 1 (`crates/tokeira-storage/src/dsql/worker_task_provenance.rs`).
- `execute_cas` returns the insert's count for a create, which its caller maps to `Applied` or `Conflict` (`crates/tokeira-storage/src/dsql/task_queue_config.rs`).
- `backfill_current_executions` returns the insert's count (`crates/tokeira-storage/src/dsql/chasm_node.rs`).
- `persist_new_execution` rolls back on a pointer conflict only when the pointer upsert's count is 0 (`chasm_node.rs`).
- `upsert_execution_row` reports the row applied when its count is above 0, and its callers replace or clear the index from that (`crates/tokeira-projection/src/dsql_store.rs`).
- `do_persist_to_backlog` adds each insert's count to its rows-written metric (`crates/tokeira-storage/src/dsql/run_repository/dispatch.rs`).

## Correctness Properties

Property 1: A shard lease is decided from what was written

_For any_ shard whose lease row is absent, or held by the asking node or another, live or expired, at any epoch: a node's request for it SHALL answer as 2.1 requires. The stored lease after it SHALL name the node and epoch the answer reports, or be unchanged when the answer is a rejection. This SHALL hold on DSQL and on PostgreSQL.

**Validates: Requirements 2.1, 2.10**

Property 2: A controller takes only a slot it wrote

_For any_ namespace slot limit and set of slots held by other controllers, an admitted controller SHALL hold the lowest slot no other controller holds, or be capacity-limited when there is none. No two controllers SHALL hold the same slot.

**Validates: Requirements 2.2, 2.10**

Property 3: A stored action id is a conflict

_For any_ decision committed with a provider action, the commit SHALL answer `Conflict` and write nothing exactly when the action's id is already stored.

**Validates: Requirements 2.3, 2.10**

Property 4: Provenance answers what is stored

_For any_ provenance record and any stored row for its digest (absent, equal or different), storing the record SHALL answer `Inserted`, `AlreadyPresent` or `DigestConflict` respectively, and SHALL leave an existing row as it was.

**Validates: Requirements 2.4, 2.10**

Property 5: Only the writer of a new configuration is told it applied

_For any_ configuration created with no expected revision, the create SHALL answer `Applied` exactly when no configuration for that task queue was stored, and otherwise `Conflict`, leaving the stored one unchanged.

**Validates: Requirements 2.5, 2.10**

Property 6: A backfill returns what it copied

_For any_ set of legacy current-run rows and already-scoped keys, each backfill call SHALL return the number of keys it wrote, and the calls' sum SHALL equal the number of keys that weren't scoped before.

**Validates: Requirements 2.6, 2.10**

Property 7: A CHASM execution commits only with its pointer

_For any_ current-run pointer and any persist of a new execution, the persist SHALL commit its nodes and write its pointer exactly when the pointer held what the persist expected. The persist expects either no pointer, or one naming a given run and version. Otherwise it SHALL answer the pointer conflict, and leave the nodes and the pointer as they were.

**Validates: Requirements 2.7, 2.10**

Property 8: The visibility index follows the newest version

_For any_ stored visibility row and any record or deletion applied to it, the run's search-attribute index SHALL be replaced or cleared exactly when the incoming version is newer than the stored one when the upsert runs. Otherwise the index SHALL be left as the stored version wrote it.

**Validates: Requirements 2.8, 2.10**

Property 9: The backlog metric counts what was written

_For any_ batch of backlog entries, some of whose keys are already stored, persisting the batch SHALL record as written the number of entries whose keys were new.

**Validates: Requirements 2.9, 2.10**

Property 10: No function decides from a skipping statement's command tag

_For any_ Rust source the check is given, it SHALL report every function that runs a skipping statement and reads `rows_affected()`, and only those. A function runs a skipping statement when it writes one inline or names a constant holding one.

**Validates: Requirement 2.10**

## Fix Implementation

### The pattern

- A skipping statement gains `RETURNING` with one column and is fetched: `fetch_optional` for one row, whose presence means the row was written, and `fetch_all` for many, whose length is the number written.
- The function running it reads no command tag.
- Where a function also runs an `UPDATE` and decides from it, that statement returns its row and is fetched the same way. So the function decides every outcome from returned rows, and the source check holds for it without exceptions.

### The nine operations

- **Shard lease** (`run_repository/leases.rs`):
  - The insert returns `epoch`. When it returns none, the takeover `UPDATE` returns `epoch` under its unchanged guard.
  - `interpret_acquire` takes the inserted epoch, the updated epoch and the rejected row, and maps them to the same outcomes as today: an inserted lease must be at epoch 1, an update reports its epoch, and neither means a rejection read from the row.
  - The `SELECT` that read the epoch back after a write goes, since the statements return it.
- **Controller slot** (`worker_compute_repository.rs`, `admit_controller`): the slot insert returns `slot`, and the loop takes the first slot that comes back.
- **Provider action** (`worker_compute_repository.rs`, `insert_action`): the insert returns `action_id`. `insert_action` returns whether it wrote the action, and `commit_decision` answers `Conflict` when it didn't.
- **Provenance** (`worker_task_provenance.rs`, `put`): the insert returns `token_digest`. When none comes back, `put` reads the stored row and answers `AlreadyPresent` or `DigestConflict`, as today.
- **Task queue configuration** (`task_queue_config.rs`, `execute_cas`): the create's insert and the compare-and-set `UPDATE` each return `revision`. `execute_cas` returns whether a row came back, and its caller answers `Applied` or `Conflict` from that.
- **CHASM backfill** (`chasm_node.rs`, `backfill_current_executions`): the insert returns one column per row, and the call returns the number of rows that come back.
- **CHASM pointer** (`chasm_node.rs`, `persist_new_execution`): the pointer's guarded upsert returns `run_id`. When none comes back, the persist rolls back and answers the pointer conflict, as it does today for a count of 0.
- **Visibility row** (`crates/tokeira-projection/src/dsql_store.rs`, `upsert_execution_row`): the guarded upsert returns `run_key`, and the function reports the row applied exactly when one comes back. Its callers are unchanged.
- **Backlog** (`run_repository/dispatch.rs`, `do_persist_to_backlog`): each insert returns `key`, and the metric adds one for each entry whose row comes back.

### The source check (`crates/tokeira-storage/tests/skipping_insert_counts.rs`)

- A test in the default suite reads every `.rs` file under the workspace's `crates/*/src` and `apps/*/src`, unit tests included. Integration tests under `tests/` are left out, since the live probe there reads the count on purpose, to show DSQL's.
- It strips comments, keeps string literals, raw ones included, and splits each file into function bodies by matching braces outside literals.
- A literal is a skipping statement when it holds `ON CONFLICT` and either `DO NOTHING`, or `DO UPDATE` followed by `WHERE`.
- A function runs one when its body holds such a literal, or names a constant whose literal is one. The test fails for each such function whose body calls `rows_affected`, naming the file and function.
- The test checks itself, as Property 10, on generated sources. The sources mix functions with and without a skipping statement, written inline or through a constant, and with and without a `rows_affected` call, among comments and literals that mention either.

### Live suites

`docs/testing/dsql-live-suites.md` lists the new suite and tests, and the commands that run them serially.

## Testing Strategy

### Exploratory Bug Condition Checking

- **Before the fix**, on a DSQL cluster created for the run:
  - A probe through sqlx shows each kind of skipping statement that skipped its row reporting a count of 1, while its `RETURNING` gives back no row (`crates/tokeira-storage/tests/dsql_skipping_insert_counts.rs`).
  - The live tests of 1.1 and 1.7 fail as those criteria predict: `dsql_shard_leasing`'s expired-takeover tests, which expect the takeover at epoch 2, and the CHASM concurrent-starts test.
  - Properties 2 to 9 fail for the conflicting cases.
- **After the fix**, the same run passes.
- **Negative controls**, each patched in, run, then reversed so the tree is byte-identical:
  - Each operation reading the count again fails its live test on DSQL.
  - A `rows_affected` call put back in each fixed function fails the source check, which names that function.

### Property-Based Tests

- Properties 1 to 9 run on the DSQL cluster, through each store's API, over generated starting rows. They use few cases each and run serially, as the live suites do, gated on `TOKEIRA_DSQL_TEST_DATABASE_URL`:
  - Property 1: `crates/tokeira-storage/tests/dsql_shard_leasing.rs`.
  - Properties 2 to 5 and 9: `crates/tokeira-storage/tests/dsql_skipping_insert_counts.rs`.
  - Properties 6 and 7: the CHASM store's live tests, `crates/tokeira-storage/src/dsql/chasm_node.rs`.
  - Property 8: `crates/tokeira-projection/tests/dsql_projection_persistence.rs`, with a stale record behind a newer one and a stale deletion behind a newer record.
- Property 10 runs in the default suite, on generated sources.

### Unit Tests

- `interpret_acquire` for each combination of inserted epoch, updated epoch and rejected row, the invalid ones included.
- The source check over the workspace, in the default suite.

### Preservation Checking

- The existing storage and projection tests stay green.
- The live suites stay green on DSQL, `dsql_shard_leasing` and the CHASM live tests included.
- Each DSQL run records the date, revision, suite and outcome, and when the cluster was created and deleted, without its identifier or endpoint.
