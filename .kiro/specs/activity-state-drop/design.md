# Activity State Drop — Bugfix Design

## Overview

A migration drops the `activity_state` table, which nothing writes or reads, and the code stops touching it:
- **The migration.** `DROP TABLE IF EXISTS activity_state`, one statement at the next free number. On DSQL it drops the table's three secondary indexes with it.
- **The runner.** The migration runner proves `DROP TABLE IF EXISTS` idempotent, so the embedded engine's automatic migration accepts the drop and replays it safely.
- **The purge.** Run deletion stops deleting `activity_state` rows.
- **The codec.** The row codec goes, and so does the requirement that fixed its layout.
- **The contract.** The schema contract's target and maximum readable versions move to the drop, and its minimum supported version stays 1.

## Glossary

- **Plain runner path:** `MigrationRunner::apply_connection`, which `tkr schema setup` uses. It applies each unapplied migration in its own transaction and records its ledger row.
- **Automatic path:** `MigrationRunner::apply_decision`, which the embedded engine uses under the automatic policy. It applies only migrations it can prove idempotent (`migration_is_idempotent`), so that it can replay one interrupted between its statement and its ledger row.
- **Purge:** the paged deletion of the tables a deleted run owns, history last (`PURGE_TABLES`, `crates/tokeira-storage/src/dsql/run_repository/bulk_write.rs`).

## How DSQL drops the table

An ephemeral cluster checked the behaviour this design relies on:
- **The plain runner path applies drops.** It applied the existing migrations and four further ones: three `DROP INDEX IF EXISTS` statements and then `DROP TABLE IF EXISTS activity_state`. It recorded their ledger rows, and left neither the table nor its indexes.
- **A drop replays as nothing.** Each drop, run again, succeeded and changed nothing.
- **A table's drop takes its indexes.** `DROP TABLE` on a table with an asynchronously built secondary index removed the index with the table.

So one migration is enough. Dropping the three indexes first would add three migrations that change nothing the table's drop doesn't.

## Bug Details

### Bug Condition

The bug condition holds wherever the code or the schema still keeps `activity_state`:
- a schema migrated to this release's target;
- a run's deletion;
- the storage crate's codec.

### Examples

- **A new cluster.** Migrating it today creates `activity_state` and builds three secondary indexes on it. After the fix, the migrations end with the table dropped.
- **An upgraded cluster.** A cluster an earlier release migrated holds `activity_state` rows. After the fix, `tkr schema setup` or the embedded engine drops the table, rows and all.
- **A run deletion.** Today the purge spends a step on the run's `activity_state` rows. After the fix, it purges the other tables only.

## Expected Behavior

### Preservation Requirements

- Earlier schemas, rows included, migrate forward under both paths (3.1).
- The migrations that created the table stay as they are (3.2).
- Run deletion purges every other table a run owns, history last (3.3).
- The in-memory store's snapshots keep their activity state table's slot (3.4).

## Root Cause

- **The table stayed for older nodes.** activity-state-writes stopped writing the table but kept it, and kept the purge's step for it, while a node of an earlier release might still write it. The stopped upgrade removes that reason.
- **The runner can't replay a drop.** `migration_is_idempotent` recognizes only `CREATE ... IF NOT EXISTS` forms, `ADD COLUMN IF NOT EXISTS` and `INSERT ... ON CONFLICT ... DO NOTHING`. So the automatic path refuses any drop, even one written to replay safely.

## Correctness Properties

Property 1: Migrating to the target leaves no activity state table

_For any_ schema, whether empty, or at the previous target with rows in `activity_state`, migrating it to the target under the plain runner path or the automatic path SHALL leave no `activity_state` table and none of its three indexes.

**Validates: Requirements 2.1, 2.2, 3.1, 3.2**

Property 2: The drop replays as nothing

_For any_ schema whose drop committed but whose ledger row didn't, applying the drop again SHALL succeed, record the ledger row, and change nothing else.

**Validates: Requirements 2.2**

Property 3: A run's deletion purges what it owns, and never `activity_state`

_For any_ run, deleting it SHALL purge every table it owns, history last, and SHALL issue no statement against `activity_state`.

**Validates: Requirements 2.3, 3.3**

## Fix Implementation

### Storage

- **The migration.** `V083__drop_activity_state.sql` holds `DROP TABLE IF EXISTS activity_state;`. V083 is the next free number now; a migration that lands first moves the drop up. The DDL validator admits the statement.
- **Idempotency.** `migration_is_idempotent` accepts a migration whose whole statement is `DROP TABLE IF EXISTS` and one table name, so a file with another statement beside the drop isn't proven idempotent. A drop that finds no table does nothing, so replaying it after a crash is safe. `every_embedded_migration_is_proven_idempotent` then covers the drop.
- **The purge.** `PURGE_TABLES` loses its `activity_state` entry, and the purge's table inventory test follows.
- **The codec.** `encode_activity_state` and `decode_activity_state` go, with the tests that use them in `dsql/codec.rs`, in storage's `codec.rs` and in `dsql/run_repository/mod.rs`. The comments that cite the row's layout follow, in `commit.rs` and in the kernel's `state.rs`.
- **Bookkeeping**, as for every migration:
  - storage's `schema-contract.toml` and build-info's packaged copy: target and maximum readable version 83, the new digest, immutable through 83;
  - `schema-baseline.lock`: version 83 and its checksum;
  - build-info's contract test and the runner's migration-count test;
  - the migration paragraph in storage's `AGENTS.md`.

### Release

The release that carries the drop says "Upgrade with every node stopped." once. The change adds a `changed` fragment with that line unless an unreleased fragment already says it.

### Specs and documentation this changes

Each says what holds once the table is gone:
- [activity-state-writes](../activity-state-writes/bugfix.md): criterion 3.2 and its design's passages on the table, its indexes, the codec and the purge.
- [activity-heartbeat-time](../activity-heartbeat-time/requirements.md): criterion 3.6 and its design.
- [bounded-bulk-writes](../bounded-bulk-writes/design.md) and [temporal-ui-support](../temporal-ui-support/design.md): the purge's table list.
- [dsql-core-persistence](../dsql-core-persistence/requirements.md) and [dsql-side-tables](../dsql-side-tables/requirements.md): the table as part of the schema.
- [dsql-storage-implementation](../dsql-storage-implementation/requirements.md), [dsql-schema-connection](../dsql-schema-connection/requirements.md) and [dsql-spread-keys](../dsql-spread-keys/requirements.md): a note that V083 drops the table.
- [storage-memory-fidelity](../storage-memory-fidelity/design.md), [delivery-broker-simulator](../delivery-broker-simulator/requirements.md), the broker simulator's README and model: pending activities live in run state.
- [recovery-index](../recovery-index/requirements.md), [runtime-sweeper-recovery](../runtime-sweeper-recovery/requirements.md), [runtime-complete-implementation](../runtime-complete-implementation/requirements.md) and [runtime-durable-backlog](../runtime-durable-backlog/design.md), where they name the table as something not to read.
- [DSQL storage](../../../docs/architecture/050-dsql-storage.md) and [history as authority](../../../docs/architecture/010-history-as-authority.md), and the diagrams that list the table: `nexus-plane.svg`, `three-planes.svg` and `hot-path.svg`.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there:
- **offline:** the runner proves `DROP TABLE IF EXISTS activity_state;` idempotent;
- **offline:** the purge's tables leave out `activity_state`;
- **live:** a schema migrated to the target holds no `activity_state` table.

### Property-Based Tests

- Property 1 runs live, from an empty schema and from one at the previous target with rows in `activity_state`, under both paths.
- Property 2 runs live, by forgetting the drop's ledger row and applying it again. It also runs offline, over the runner's idempotency check.
- Property 3 runs offline over the purge's tables, and live through a run deletion after the drop.

### Unit Tests

- The runner proves `DROP TABLE IF EXISTS` idempotent. It still refuses a `DROP TABLE` without `IF EXISTS`, and a file with another statement beside the drop.
- The schema contract's target and maximum readable versions equal the newest embedded migration's version, so a contract left behind the migrations fails offline. A release that embeds a migration it only reads, past its target (the `TARGET < C <= MAX` row of managed-embedded-dsql's decision table), relaxes this to target ≤ maximum readable ≤ newest.
- The baseline lock names the drop.

### Live Test

On an ephemeral cluster:
1. Migrate a schema to the previous target. Write more than 3,000 `activity_state` rows, in batches under DSQL's limit on the rows a transaction modifies, so the drop is shown free of that limit on a table an earlier release filled.
2. Migrate it to the target with `tkr schema setup`'s path, and check that a validate-only assessment then finds it compatible.
3. Forget the drop's ledger row and apply it again under the automatic path.
4. Delete a run.

Then migrate a new empty schema under the automatic path.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test that covers it:
- the drop without `IF EXISTS`, whose replay fails;
- the idempotency check without `DROP TABLE IF EXISTS`, which the automatic path then refuses;
- the purge still deleting `activity_state` rows, which fails once the table is gone;
- the schema contract left at the previous target, which the offline check of the contract catches, and which the validate-only assessment after setup finds a future schema.

### Preservation Checking

The migration runner's tests, the purge's tests and the schema-bootstrap tests stay green, as do the live runs of an earlier schema migrating forward.
