# Bugfix Requirements Document

## Introduction

Tokeira applies its schema migrations through two kinds of path in `crates/tokeira-storage/src/dsql/migration.rs`.

- **The guarded path** (`MigrationRunner::apply_decision`) runs during engine startup under the schema-migration claim. Each migration goes through `execute_migration_step`, which retries a statement or commit that fails with SQLSTATE `40001` up to `DEFAULT_MIGRATION_OCC_RETRIES` times (5). When the migration starts an `ASYNC` index build, it then waits for the build to finish (`wait_for_async_index`).
- **The plain runner paths** do neither: `MigrationRunner::apply`, which takes a pool, and `MigrationRunner::apply_connection`, which takes a single connection and is what `tkr schema setup` uses (`apps/tkr/src/commands/schema.rs`). Each migration's statement runs once and commits once. Any error stops the run with "failed to apply migration" or "failed to commit migration".

Aurora DSQL returns SQLSTATE `40001` with code OC001 when a session's cached schema catalog is behind a catalog change another transaction made, and the rebase onto the new catalog fails. Retrying from the same session refreshes the cache, and the retry succeeds unless the catalog has changed again ([Troubleshooting concurrency control responses](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/troubleshooting.html#troubleshooting-occ)). An `ASYNC` index build that is still running is one source of such changes.

On a newly created cluster, a full migration run through the plain path once failed to commit V047 with OC001 while index builds from earlier work were still running. The next run completed. The guarded path would have retried.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN `apply` or `apply_connection` runs a migration whose statement or commit fails with SQLSTATE `40001` (OC000 or OC001) THEN the run stops with that migration unapplied, though a retry would succeed.

1.2 WHEN `apply` or `apply_connection` applies a migration that starts an `ASYNC` index build THEN it moves to the next migration without waiting for the build to finish, so a later migration can meet the catalog change the build makes.

### Expected Behavior (Correct)

2.1 WHEN `apply` or `apply_connection` runs a migration whose statement or commit fails with SQLSTATE `40001` THEN the system SHALL retry the migration in a new transaction, up to the guarded path's limit, and SHALL stop with today's error only once the limit is spent. A `40001` failure aborts the transaction, so the retry repeats a migration that applied nothing; the retry SHALL NOT depend on the migration being idempotent.

2.2 WHEN `apply` or `apply_connection` applies a migration that starts an `ASYNC` index build THEN the system SHALL wait for the build to finish before it records the migration and applies the next one, as the guarded path does.

2.3 The system SHALL record a migration in `schema_version` once, after its transaction commits, whether or not the migration was retried.

### Unchanged Behavior (Regression Prevention)

3.1 The guarded path SHALL CONTINUE TO behave as today: its claim and fence, its idempotency requirement for the migrations it runs, its retry limit and its ledger records.

3.2 Migration bytes, checksums, version order, the `schema_version` ledger and the checksum check for applied migrations SHALL CONTINUE as today.

3.3 A failure with any SQLSTATE other than `40001` SHALL CONTINUE TO stop the run at once, with today's error.

3.4 A run against an up-to-date schema SHALL CONTINUE TO apply nothing and change nothing.

### Out of Scope

- Changing the retry limit or adding backoff to the guarded path.
- Recovering from a commit whose outcome is uncertain, such as a server-unavailable error during `COMMIT`. Those aren't `40001`, and 3.3 keeps today's behaviour for them.
- The content of any migration.
