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

- **Baseline lock discipline.** The tracked baseline through V067 prevents
  uncoordinated edits and makes any migration/lock mismatch a build failure. Before
  Tokeira declares its first durable release baseline, an explicitly approved schema
  re-cut may replace a locked migration only when migration bytes, baseline metadata,
  schema contract and digests, build information, tests, and this policy are updated as
  one reviewed unit. After that first durable release, migrations are strictly
  forward-only: never edit, rename, reorder, or delete a baseline-locked migration;
  every schema change is a new migration above the current head, including any
  supported, idempotent `ALTER TABLE` operation.
- **Contiguous versions.** No gaps or duplicate `VNNN`; the next schema change after
  the baseline starts at V068.
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
