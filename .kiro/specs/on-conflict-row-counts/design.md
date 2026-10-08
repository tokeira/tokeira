# On-Conflict Row Counts — Design

## Overview

The operations keep deciding from the row count of their conditional inserts. That count is right on DSQL through sqlx, and what this design adds is the live coverage that keeps it so:
- a probe of the count itself;
- one property per operation, each run through the operation's store on a DSQL cluster with conflicting rows among its cases.

The projection crate's live feature now builds its tests, and the live-suites guide lists them all.

## Where the contract lives

| Operation | Code | Statement |
|---|---|---|
| Shard lease | `try_acquire_bundle`, `crates/tokeira-storage/src/dsql/run_repository/leases.rs` | `DO NOTHING` insert, then the takeover `UPDATE` when it counts 0 |
| Controller slot | `admit_controller`, `crates/tokeira-storage/src/dsql/worker_compute_repository.rs` | `DO NOTHING` insert per slot |
| Provider action | `insert_action` and `commit_decision`, same file | `DO NOTHING` insert |
| Provenance | `put`, `crates/tokeira-storage/src/dsql/worker_task_provenance.rs` | `DO NOTHING` insert |
| Configuration create | `execute_cas`, `crates/tokeira-storage/src/dsql/task_queue_config.rs` | `DO NOTHING` insert |
| CHASM backfill | `backfill_current_executions`, `crates/tokeira-storage/src/dsql/chasm_node.rs` | `INSERT … SELECT … DO NOTHING` |
| CHASM pointer | `persist_new_execution`, same file | guarded `DO UPDATE … WHERE`, after its transaction compares the current-run pointer with the one expected |
| Visibility row | `upsert_execution_row`, `crates/tokeira-projection/src/dsql_store.rs` | guarded `DO UPDATE … WHERE` |
| Backlog | `do_persist_to_backlog`, `crates/tokeira-storage/src/dsql/run_repository/dispatch.rs` | `DO NOTHING` insert per entry |

Each reads the count with sqlx's `execute` and `rows_affected()`. The other conditional inserts in the workspace never read it:
- the control lease's claim;
- the migration runner's version and compatibility inserts;
- the CHASM backfill marker;
- the worker-compute queue sample;
- the projection's attribute registration, which reads its `RETURNING` row instead, and its search-attribute index insert.

## Correctness Properties

Property 1: A shard lease is decided from what was written

_For any_ shard whose lease row is absent, or held by the asking node or another, live or expired, at any epoch: a node's request for it SHALL answer as Requirement 2 states. The stored lease after it SHALL name the node and epoch the answer reports, or be unchanged after a rejection.

**Validates: Requirements 1.1, 2.1-2.4**

Property 2: A controller takes only a slot it wrote

_For any_ namespace slot limit and set of slots held by other controllers, an admitted controller SHALL hold the lowest slot no other controller holds, or be capacity-limited when there is none.

**Validates: Requirements 1.1, 3.1**

Property 3: A stored action id is a conflict

_For any_ decision committed with a provider action, the commit SHALL answer `Conflict` and write nothing exactly when the action's id is already stored.

**Validates: Requirements 1.1, 3.2**

Property 4: Provenance answers what is stored

_For any_ provenance record and any stored row for its digest (absent, equal or different), storing the record SHALL answer `Inserted`, `AlreadyPresent` or `DigestConflict` respectively, and SHALL leave an existing row as it was.

**Validates: Requirements 1.1, 4.1**

Property 5: Only the writer of a new configuration is told it applied

_For any_ configuration created with no expected revision, the create SHALL answer `Applied` exactly when no configuration for that task queue was stored, and otherwise `Conflict`, leaving the stored one unchanged.

**Validates: Requirements 1.1, 5.1**

Property 6: A backfill returns what it copied

_For any_ set of legacy current-run pointers and already-scoped keys, and any batch size, each backfill call SHALL return the number of pointers it wrote. The calls SHALL together copy every pointer that wasn't scoped before.

**Validates: Requirements 1.1, 6.3**

Property 7: A CHASM execution commits only with its pointer

_For any_ current-run pointer, present or absent, and any expectation the persist carries, the persist SHALL commit exactly when Requirement 6.1 says, and otherwise answer the conflict of 6.2. The persist expects none, the pointer, its run at another version, or another run.

**Validates: Requirements 1.1, 6.1, 6.2**

Property 8: The visibility row follows the newest version

_For any_ stored and incoming visibility versions, the upsert SHALL report the row applied, and replace it, exactly when the incoming version is newer.

**Validates: Requirements 1.1, 7.1**

Property 9: The backlog metric counts what was written

_For any_ batch of backlog entries, some of whose keys are already stored, persisting the batch SHALL record as written the number of entries whose keys were new.

**Validates: Requirements 1.1, 8.1**

## Testing Strategy

- **The probe** (`the_row_count_is_the_rows_written`, `crates/tokeira-storage/tests/dsql_on_conflict_counts.rs`) checks Requirement 1:
  - the shard lease's insert counts 1, and 0 against the lease it then leaves as it was;
  - the CHASM pointer's guarded upsert counts 0 when its guard fails, leaving the pointer, and 1 when it holds.
- **The properties** run through each operation's store. Every run checks every kind of stored row a property names, so no conflict path rests on a random draw.
  - Properties 2 to 5, 7, 8 and 9 check every input in a small, finite set: for example every slot limit up to 3 with every set of held slots below it, or every pair of versions over two epochs and two transitions.
  - Property 1 checks each kind of lease row at a generated epoch. Property 6 generates its pointer counts and batch sizes, with none, one or two pointers already scoped in turn.
  - Generated cases are unshrunk and unpersisted, because each costs DSQL round trips.
- **Where they live.**
  - Property 1 is in `crates/tokeira-storage/tests/dsql_shard_leasing.rs`.
  - Properties 2 to 5 and 9 are in `dsql_on_conflict_counts.rs`.
  - Properties 6 and 7 are among the CHASM store's live tests in `crates/tokeira-storage/src/dsql/chasm_node.rs`.
  - Property 8 is in the projection store's tests in `crates/tokeira-projection/src/dsql_store.rs`. It checks `upsert_execution_row` directly, since its answer gates every replacement and clear of a run's index rows. The race that brings a stale record past the projection's version check can't be staged deterministically through the store's API.
- **The existing live tests stay as they are**, among them the lease takeover tests and the CHASM concurrent-starts test.
- **Running.** All of these run serially on a DSQL cluster created for the run and deleted after it, with the commands in `docs/testing/dsql-live-suites.md`.
