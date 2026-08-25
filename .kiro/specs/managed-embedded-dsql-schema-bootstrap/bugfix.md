# Managed Embedded DSQL Schema Bootstrap Bugfix

## Status and Scope

This document defines a focused correction to the managed embedded DSQL schema
application protocol. It refines the existing
[managed embedded DSQL requirements](../managed-embedded-dsql/requirements.md#requirement-5-mode-specific-migration-policy),
especially Requirements 5.5 and 5.9–5.13, without redefining the feature.

The selected correction is orchestration-only: it does not change migration SQL, the
release schema contract, the migration-set digest, the connection coordinator, or the
kernel. Odori dependency movement and live-cluster recovery follow only after this
upstream correction is merged.

## Verified Current Behavior

The following statements are verified against Tokeira source at
`4e8cd6e81a6f18f41d350a8adb8b4cd625c665bc`. They describe the defect, not proposed
behavior:

1. [`MigrationRunner::apply_decision`](../../../crates/tokeira-storage/src/dsql/migration.rs)
   executes an unapplied migration, records its `schema_version` row, and immediately
   calls `persist_compatibility` for that version.
2. `persist_compatibility` inserts into `schema_compatibility`, whose exact table
   definition is migration
   [`V066__schema_compatibility.sql`](../../../crates/tokeira-storage/migrations/V066__schema_compatibility.sql).
3. `bootstrap_migration_coordination` and the post-claim prelude use only the exact V001
   `schema_version` and V067 `tokeira_control_lease` migration bytes. Neither installs
   V066 before the migration loop.
4. An empty schema is assessed as `Initialize` under automatic policy. V001 can therefore
   be executed and recorded before `schema_compatibility` exists, after which the first
   compatibility write fails with an undefined-table database error.
5. A retry observes the valid ledger prefix left by the preceding attempt and can repeat
   the same failure after advancing another migration. Repeated startup attempts are not
   a recovery mechanism.
6. [`startup_phase`](../../../crates/tokeira-engine/src/lib.rs) maps an inner phase error
   to `EmbeddedEngineStartError::Phase` without retaining or emitting the cause. Its
   nearby documentation currently says details remain in host telemetry, but this
   mapping emits no such diagnostic.
7. Automatic policy intentionally permits one compatible-but-writable case: a fully
   checksum-validated legacy schema may receive missing compatibility metadata.
   `Compatible { legacy_backfill: false }`, `MigrationRequired`, and validate-only
   decisions are the read-only cases preserved by this bugfix.

The existing
[migration design](../managed-embedded-dsql/design.md#5-compatibility-assessment-and-migrations-tokeira-storage)
already requires ledger recording followed by compatibility persistence for every
migration. The missing prerequisite is availability of the compatibility table before
the first such persistence operation.

## Reproduction

The smallest counterexample starts from a new managed cluster with no schema tables:

1. Automatic assessment returns `Initialize { target: 67 }`.
2. Pre-claim coordination installs the exact V001 and V067 tables.
3. The engine acquires the `schema-migration` claim.
4. The post-claim prelude idempotently installs V001 and V067 again.
5. The runner executes V001 and records the V001 ledger row.
6. The runner calls `persist_compatibility(V001)` before V066 has installed
   `schema_compatibility`.
7. Schema startup fails, leaving a recoverable but partial state instead of converging
   through V067.

No AWS resource action is required to reproduce the ordering defect. A pure operation
model can reproduce it from the migration plan and table-availability state.

## Formal Bug Condition

Let

```text
X = (policy, decision, applied_prefix, target, fence, compatibility_table, operations)
```

where `applied_prefix` is a checksum-valid contiguous ledger prefix and `operations` is
the ordered application plan after assessment.

`C(X)` is true exactly when:

```text
policy = Automatic
AND decision in {Initialize, Migrate}
AND fence = Owned("schema-migration")
AND compatibility_table = Absent
AND operations contains Execute(Vn) < Record(Vn) < PersistCompatibility(Vn)
AND operations contains no BootstrapCompatibility(exact V066 bytes)
    between AcquireFence and PersistCompatibility(Vn)
```

for an unapplied `Vn <= target`.

The minimum counterexample is `applied_prefix = []` and `Vn = V001`. The violated
invariant is that every compatibility persistence operation must be preceded by
post-claim availability of the exact compatibility-table schema.

The exploration property is the universal claim `for all valid X, not C(X)`. It must
fail against the current operation ordering and shrink to the empty-prefix/V001 case
before implementation begins. If it passes on the unfixed model, the model or root-cause
analysis is wrong and implementation must stop.

## Expected Behavior

### Requirement 1: Fenced Application-Metadata Bootstrap

1.1 WHEN an automatic `Initialize` or `Migrate` decision owns the
`schema-migration` claim, THE migration runner SHALL install
`schema_compatibility` before its first compatibility persistence operation.

1.2 WHEN the runner installs the application-metadata bootstrap, THE migration runner
SHALL execute the byte-for-byte V066 statement selected by the embedded schema contract
as one standalone statement.

1.3 WHEN the runner is about to install the application-metadata bootstrap, THE
migration runner SHALL revalidate the active `schema-migration` fence immediately before
the statement.

1.4 WHEN the application-metadata bootstrap statement completes, THE migration runner
SHALL revalidate the active `schema-migration` fence before any later migration, ledger,
or compatibility write.

1.5 WHEN coordination must be bootstrapped before claim acquisition, THE migration
runner SHALL limit the pre-claim statements to the exact V001 and V067 migration bytes.

1.6 WHEN the ordered migration loop reaches V066 after the table was bootstrapped, THE
migration runner SHALL still execute or recognize V066 and record it in strict ledger
order.

1.7 WHEN the migration fence is lost, THE migration runner SHALL issue no later
bootstrap, migration, ledger, or compatibility write.

### Requirement 2: Recovery and Convergence

2.1 WHEN automatic application begins from any checksum-valid contiguous prefix below
`TARGET`, THE migration runner SHALL converge the schema and ledger through `TARGET`.

2.2 WHEN automatic application restarts after any injected operation boundary,
including after ledger recording and before compatibility persistence, THE migration
runner SHALL accept checksum-valid compatibility metadata behind the authoritative
ledger and replay idempotently without checksum drift.

2.3 WHEN a migration ledger row is recorded, THE migration runner SHALL have completed
that migration's physical DDL or DML statement.

2.4 WHEN compatibility metadata records version `Vn`, THE migration runner SHALL have a
contiguous ledger through `Vn` whose cumulative digest matches that metadata.

2.5 WHEN automatic application completes successfully, THE migration runner SHALL leave
`TARGET` as both the ledger head and the latest compatibility version.

### Requirement 3: Read-Only and Legacy Decisions

3.1 WHEN schema assessment uses validate-only policy, THE schema path SHALL leave schema
state unchanged.

3.2 WHEN application receives `MigrationRequired`, THE migration runner SHALL issue no
bootstrap write.

3.3 WHEN application receives `Compatible { legacy_backfill: false }`, THE migration
runner SHALL issue no bootstrap write.

3.4 WHEN automatic policy receives `Compatible { legacy_backfill: true }` for missing or
checksum-valid lagging metadata, THE migration runner SHALL use the checksum-validated
compatibility backfill behavior.

### Requirement 4: Startup and Live-Test Safety

4.1 WHEN schema application fails, THE embedded engine SHALL return no usable engine
handle or serving listener.

4.2 WHEN the live managed-DSQL test has reached a ready cluster, THE test harness SHALL
attempt descriptor-bound administrative teardown after capturing the engine exercise
result, including when that result is an error.

4.3 WHEN the live managed-DSQL test may create and delete a cluster, THE test harness
SHALL preserve the exact `CREATE_AND_DELETE` acknowledgement gate and plan-bound identity
confirmation.

4.4 WHEN `startup_phase` discards an inner error, THE engine documentation SHALL not
claim that the discarded cause was emitted to host telemetry.

4.5 IF a diagnostic reason is exposed by this correction or a linked follow-up, THEN THE
engine SHALL use a bounded allowlisted classification rather than raw nested error text.

### Requirement 5: Pre-Release Schema Policy

5.1 WHILE Tokeira has not declared its first durable release baseline, THE tracked
storage policy SHALL describe the baseline lock as protection from uncoordinated edits
rather than a prohibition on an explicitly reviewed, internally consistent schema
re-cut.

5.2 WHEN the selected correction changes only bootstrap ordering, THE implementation
SHALL leave V066, V067, `schema-baseline.lock`, `schema-contract.toml`, build metadata,
and migration digests unchanged.

5.3 IF a future pre-release change deliberately re-cuts an existing migration, THEN THE
storage change SHALL update migration bytes, baseline metadata, schema contract and
digests, build information, tests, and written policy as one reviewed unit.

## Preservation Set

The correction must preserve these behaviors:

- V001 and V067 remain the only pre-claim coordination bootstrap exceptions.
- V066 is a post-claim application-metadata bootstrap and remains an ordinary numbered
  migration whose ledger identity is neither invented nor skipped.
- Every bootstrap statement remains byte-for-byte tied to its embedded migration source.
- Migrations remain contiguous, checksum-verified, idempotent for replay, and limited to
  one statement per migration transaction.
- Compatibility digests remain cumulative over the recognized migration prefix and
  future, unsupported, or checksum-mismatched schemas remain rejected.
- Automatic legacy-metadata backfill remains distinct from the read-only compatible
  decision.
- The existing bounded connection director and control-lease path remain the only DSQL
  connection and schema-ownership mechanisms.
- Crash-safe cluster descriptor recovery, deletion protection, and the rule that engine
  drop never deletes a cluster remain unchanged.
- No kernel behavior or dependency changes are introduced.

## Required Verification

1. Add the failing exploration property for `not C(X)` first, using at least 100 cases,
   and record the shrunk empty-prefix/V001 counterexample.
2. After the correction, reuse the same property as a regression and add a correction
   property covering every valid prefix below `TARGET`.
3. Add a crash-boundary property proving physical statement, ledger, and compatibility
   ordering plus replay convergence.
4. Add preservation properties with at least 100 cases for read-only decisions, exact
   bootstrap bytes, strict ordering, cumulative digests, and fence loss.
5. Add opt-in DSQL integration coverage for empty initialization and partial-prefix
   recovery.
6. Strengthen the ignored live-AWS engine test to cover startup, `service_override`,
   restart against the same cluster, and teardown-on-error control flow. Running it
   remains billable, destructive, and separately operator-authorized.

## Out of Scope

- Re-cutting V066/V067 or changing the release schema contract for this ordering-only
  correction.
- Manually modifying a live cluster's ledger or compatibility metadata.
- Inspecting, replacing, logging, or deleting a managed descriptor or its lock sidecar.
- Running any live-AWS operation without explicit operator authorization.
- Broadening public errors with raw SQLx, AWS SDK, connection, descriptor, token, or
  credential details. A wider bounded-diagnostics API may be a separate linked change.
- Updating Odori before the fixed Tokeira revision is merged.
