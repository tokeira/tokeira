# AGENTS — tokeira-storage

Crate-local rules. The root `AGENTS.md` still applies; this refines it for storage.
On conflict, the stricter rule wins.

## The one boundary: migrations are the authoritative schema, and they are unforgiving

DSQL migrations live in `crates/tokeira-storage/migrations/` as `VNNN__snake_case.sql`,
one statement per file. `build.rs` embeds them at compile time; the runner
(`src/dsql/migration.rs`) is forward-only, checksum-verified, and rejects version gaps
and duplicates. There is no hand-maintained schema dump — the migrations directory IS
the schema.

This file is the **canonical** home of the root heading *Adding or Changing a DSQL
Migration* (the root keeps the name and points here). The rules:

- **Build phase (now): no `ALTER TABLE`.** Fold a new column/constraint into the
  table's base `CREATE TABLE` migration and let its checksum change. Do not add a
  follow-up `ALTER`. (This flips to strictly forward-only once a baseline is cut;
  removing the build-phase rule from this file is itself the signal the baseline
  exists.)
- **Contiguous versions.** No gaps, no duplicate `VNNN`. Deleting the highest migration
  is acceptable during the build phase; editing an applied one after baseline is not —
  each post-baseline change is a new `VNNN` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
  included).
- **DSQL DDL subset always.** One statement per file; secondary indexes created `ASYNC`;
  no `CHECK` constraints (validate in the application); no `BIGSERIAL` (generate IDs
  in-app). `src/dsql/validation.rs` (`DdlValidator`) enforces the safe subset — if it
  rejects your DDL, the DDL is wrong, not the validator.

## Connection management is a DSQL survival invariant

`max_idle_conns` MUST equal `max_conns` (root file). Idle decay under DSQL's
cluster-wide 100 conn/sec rate limit causes rate-limit storms. The reservoir, token
bucket, and slot-block manager (`src/dsql/`) exist to respect 100/sec and 10k-concurrent;
do not bypass them with ad-hoc `driver.Open()`-style connection creation.

## Determinism in tests

`bug_condition_exploration_tests.rs` and `preservation_property_tests.rs` are
proptest-based. Storage logic that draws on iteration order of a `HashMap` while
producing observable output (ordering, RNG-paired effects) is a determinism hazard —
prefer ordered structures or sort before emitting.

## Where things belong instead

- Workflow *semantics* → `tokeira-kernel` (pure). Storage persists bytes; it never
  decides transition correctness.
- Runtime orchestration of commits (fencing, lane ownership) → `tokeira-runtime`.
  Storage exposes the repository; the runtime decides when and under what fence to call it.
