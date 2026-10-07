# Requirements Document

## Introduction

Nine storage operations write with an `INSERT … ON CONFLICT` whose conflict clause can leave the row unwritten: `DO NOTHING`, or a `DO UPDATE` with a `WHERE` condition. Each decides what happened from the statement's row count, which sqlx reads from the command tag. On Aurora DSQL that count is the number of rows the statement wrote, as on PostgreSQL ([`INSERT`, Outputs](https://www.postgresql.org/docs/current/sql-insert.html)). A conflict that writes nothing counts 0, and each row written counts 1.

This spec states that contract and the operations' answers that rest on it. A live suite guards both on a DSQL cluster, since tests on PostgreSQL can't speak for DSQL.

## Glossary

- **Row count:** the count sqlx's `PgQueryResult::rows_affected()` reads from a statement's command tag.
- **Conditional insert:** an `INSERT … ON CONFLICT` whose conflict clause can leave the row unwritten: `DO NOTHING`, or a `DO UPDATE` with a `WHERE` condition.
- **Live suite:** a test that connects only when `TOKEIRA_DSQL_TEST_DATABASE_URL` names a disposable DSQL database (`docs/testing/dsql-live-suites.md`).
- **Current-run pointer:** the run a CHASM business id resolves to under an archetype, as `current_run` answers it. That is the scoped pointer, or, until the pointer backfill completes, a legacy pointer whose root node has that archetype.

## Requirements

### Requirement 1: Row counts

**User Story:** As a storage maintainer, I want the row count of a conditional insert on DSQL to be the rows it wrote, so that the operations built on it can decide from it.

#### Acceptance Criteria

1. WHEN a conditional insert on DSQL leaves its row unwritten THEN its row count SHALL be 0, and SHALL be 1 for each row it writes, for `DO NOTHING` and for a `DO UPDATE … WHERE` alike.
2. THE live suite SHALL check the row count of the shard lease's insert, and of the CHASM current-run pointer's guarded upsert, for a row written and for a row left unwritten.

### Requirement 2: Shard leases

**User Story:** As an operator, I want a shard lease to have one owner at a time, so that fencing can tell the owner's writes from a former owner's.

#### Acceptance Criteria

1. WHEN a node asks for a shard lease with no lease row THEN the system SHALL write one at epoch 1 and report the lease acquired at epoch 1.
2. WHEN a node asks for a shard lease whose row it holds and that is live THEN the system SHALL extend the lease and report it acquired at the same epoch.
3. WHEN a node asks for a shard lease whose row has expired, or is held by no one, THEN the system SHALL take the lease over at the next epoch and report it acquired at that epoch.
4. WHEN a node asks for a shard lease whose live row another node holds THEN the system SHALL write nothing and report the lease rejected, with the holder and its epoch.

### Requirement 3: Worker-compute slots and actions

**User Story:** As an operator, I want a namespace's worker-compute slot limit and its actions' identities to hold, so that capacity and provider actions stay as decided.

#### Acceptance Criteria

1. WHEN a worker-compute controller is admitted THEN the system SHALL assign it the lowest namespace slot no other controller holds, and SHALL make it capacity-limited, holding no slot, when every slot under the namespace limit is held.
2. WHEN a worker-compute decision commits with a provider action whose id is already stored THEN the system SHALL answer `Conflict` and SHALL write nothing.

### Requirement 4: Worker-task provenance

**User Story:** As a security reviewer, I want a provenance digest that is already stored to be reported as such, so that a conflicting origin is never taken for a new record.

#### Acceptance Criteria

1. WHEN worker-task provenance is stored THEN the system SHALL answer `Inserted` only when it wrote the row. For a digest that already has a row, it SHALL answer `AlreadyPresent` when the stored row is equal, and fail with `DigestConflict` otherwise, leaving the stored row as it was.

### Requirement 5: Task-queue configuration

**User Story:** As an operator, I want only the writer of a new task-queue configuration to be told it applied, so that a concurrent create is never lost silently.

#### Acceptance Criteria

1. WHEN a task queue's configuration is created with no expected revision THEN the system SHALL answer `Applied` only when it wrote the row, and `Conflict` when a configuration already exists, leaving it unchanged.

### Requirement 6: CHASM

**User Story:** As a storage maintainer, I want a CHASM execution's nodes and its current-run pointer never to tear, and the pointer backfill to report what it copied.

#### Acceptance Criteria

1. WHEN a CHASM execution is persisted with an expected current-run pointer THEN the system SHALL commit its nodes and its pointer when the current-run pointer is the one expected, or when there is none and none is expected.
2. WHEN the current-run pointer is not the one expected, including when there is one and none is expected or there is none and one is expected, THEN the system SHALL answer the pointer conflict, leaving the nodes and the pointer as they were.
3. WHEN the CHASM current-execution backfill copies a page THEN each call SHALL return the number of pointers it wrote.

### Requirement 7: Visibility rows

**User Story:** As a user of filtered List and Count queries, I want a run's visibility row and search-attribute index to follow its newest version.

#### Acceptance Criteria

1. WHEN the visibility projection upserts a run's row THEN the system SHALL replace the row, and report it applied, exactly when the incoming version is newer than the stored one. The run's search-attribute index rows are replaced or cleared only on that report.

### Requirement 8: Dispatch backlog

**User Story:** As an operator reading storage metrics, I want the backlog's rows-written metric to count what was written.

#### Acceptance Criteria

1. WHEN durable dispatch persists backlog entries THEN the backlog's rows-written metric SHALL count the entries whose rows were written, and not those whose keys already existed.

### Requirement 9: Live coverage

**User Story:** As a maintainer, I want each of these contracts checked on DSQL itself.

#### Acceptance Criteria

1. THE live suite SHALL check each of Requirements 2 to 8 on a DSQL cluster, with conflicting rows among the cases of every run.
2. THE projection crate's `dsql-integration` feature SHALL build its live tests, which need the storage crate's test constructor.
3. `docs/testing/dsql-live-suites.md` SHALL list each live test and the command that runs it.

## Out of Scope

- Applying the migrations to a new DSQL cluster while asynchronous index builds are still running, which can fail a DDL commit with a schema conflict (`OC001`); [`migration-runner-occ-retry`](../migration-runner-occ-retry/bugfix.md) handles it.
