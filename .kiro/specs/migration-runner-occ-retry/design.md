# Migration Runner OCC Retry — Bugfix Design

## Overview

Give the plain migration paths the guarded path's per-migration retry and index-build wait. One shared
helper runs a migration's transaction, retrying `40001` failures, and then waits for any `ASYNC` index build
the migration started. `apply`, `apply_connection` and the guarded path's step all use it. The guarded path
keeps its own idempotency check in front of it.

## Root Cause

`apply` (pool) and `apply_connection` (one connection) in `crates/tokeira-storage/src/dsql/migration.rs`
each inline a single `begin`, `execute` and `commit` per migration. They return the first error, and never
call `wait_for_async_index`. `execute_migration_step` has the retry loop and the wait, but it also rejects
migrations that aren't idempotent (`migration_is_idempotent`), which the guarded path needs for its
crash-recovery semantics. So the plain paths can't simply call it.

## Fix Implementation

- **Shared step.** Extract the loop from `execute_migration_step` into one function that takes a connection
  and a migration and returns the job id of any `ASYNC` index build:
  - begin, execute, and commit;
  - on a `40001` (`is_occ_error`) from the statement or the commit, start again in a new transaction on the
    same connection, up to `DEFAULT_MIGRATION_OCC_RETRIES`;
  - on any other error, return it.

  A second shared function waits for the build when `parse_async_index_spec` recognises the statement.
  `execute_migration_step` becomes the idempotency check, then the shared step, then the wait, with the same
  behaviour as today.
- **`apply_connection`.** For each unapplied migration, run the shared step and the wait on the given
  connection, then record the migration as today. Retrying on the same connection is what refreshes its
  catalog cache.
- **`apply` (pool).** Acquire one connection per migration, then do the same as `apply_connection` on it, so
  every retry of a migration uses one session.
- **Errors.** When the retries run out, or a non-`40001` error occurs, return today's context: "failed to
  apply migration V…" for a statement and "failed to commit migration V…" for a commit. Record the failure
  metric as today.
- **No idempotency gate on the plain paths.** A `40001` failure aborts the transaction, so the retried
  migration applied nothing, whatever its form (criterion 2.1). The gate stays where it serves its purpose,
  on the guarded path.

## Correctness Properties

Property 1: A conflict is retried and the migration is recorded once

_For any_ sequence of `40001` failures on a migration's statement or commit, shorter than the retry limit,
followed by success, the plain paths SHALL commit the migration once, record it once in `schema_version`, and
go on to the next migration. With the limit's worth of failures, they SHALL stop with today's error, having
recorded nothing for that migration.

**Validates: Requirements 2.1, 2.3**

Property 2: Other failures stop the run

_For any_ failure whose SQLSTATE isn't `40001`, the plain paths SHALL stop at once, with today's error, and
retry nothing.

**Validates: Requirement 3.3**

## Testing Strategy

- **Unit tests** of the shared step over an executor double that fails the statement or the commit with
  `40001` a set number of times, then succeeds. Cover Properties 1 and 2, and check that the guarded path
  still refuses a non-idempotent migration. If the step needs a small internal trait or closure to accept
  the double, keep it private to the module.
- **Live tests** on an ephemeral Aurora DSQL cluster, never a PostgreSQL stand-in. On a newly created
  cluster:
  - `apply_connection` and `apply` each migrate the full embedded corpus to the target in one run;
  - a second run applies nothing.

  OC001 can't be provoked on demand, so the unit tests carry the retry, and the live tests prove the
  rewired paths end to end, including the waits for index builds.
- **Exploration first.** Before the fix, the unit test for Property 1 fails against today's
  `apply_connection`.
- **Preservation.** The existing migration tests stay green, the guarded path's tests included.
