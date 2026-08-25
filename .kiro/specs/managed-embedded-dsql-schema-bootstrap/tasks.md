# Implementation Plan

Requirements: [`bugfix.md`](bugfix.md). Design: [`design.md`](design.md).

## Tasks

- [x] 1. Bug-condition exploration property: prove the current ordering fails
  - [x] 1.1 Extract the current post-claim V001/V067 prelude into the private,
    production-consumed `post_claim_bootstrap_statements` seam in
    `crates/tokeira-storage/src/dsql/migration.rs` without adding V066 or otherwise
    changing behavior.
    - Keep `bootstrap_statements_for_decision` unchanged as the pre-claim plan.
    - _Requirements: 1.1, 1.5_
  - [x] 1.2 Add the pure application-ordering model in
    `crates/tokeira-storage/src/dsql/migration/schema_bootstrap_property_tests.rs`.
    - Derive bootstrap operations from the production seam and migration operations from
      the embedded recognized migration set.
    - Generate checksum-valid contiguous prefixes below `TARGET`, corresponding
      automatic decisions, and an owned migration fence.
    - Use ordered structures so the counterexample shrinks deterministically.
    - _Requirements: 1.1, 2.1_
  - [x] 1.3 Implement Property 1 as the exploration PBT with at least 100 cases.
    - Assert that exact V066 occurs after claim acquisition, between immediate fence
      checks, and before the first compatibility persistence operation.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 1: bug condition is eliminated`
    - Run the focused property against the unfixed plan and confirm that it fails.
    - Record the shrunk counterexample; the expected minimum is an empty applied prefix
      followed by V001 recording and compatibility persistence without V066.
    - If the property passes, stop and re-investigate the production anchor or root cause;
      do not proceed to Task 2.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. Apply the minimal fenced V066 bootstrap correction
  - [x] 2.1 Append `schema_compatibility_bootstrap_sql()` to the private post-claim plan
    after exact V001 and V067.
    - Interpret it on the existing admitted control connection as one standalone
      statement.
    - Do not add V066 to the pre-claim coordination bootstrap.
    - _Requirements: 1.1, 1.2, 1.5, 1.6_
  - [x] 2.2 Preserve the fence boundary around application-metadata bootstrap.
    - Retain the fence check before every post-claim bootstrap statement.
    - Add the final fence revalidation after V066 and before migration iteration.
    - Leave the existing pre-migration, pre-ledger, and pre-compatibility checks intact.
    - _Requirements: 1.3, 1.4, 1.7_
  - [x] 2.3 Rerun the unchanged Property 1 exploration test and confirm it now passes at
    the same case count.
    - Do not weaken its generator, assertion, or production-plan anchor after observing
      the expected failure.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 3. Add correction properties for convergence, crashes, and fencing
  - [x] 3.1 Implement Property 2: every valid prefix converges, with at least 100 cases.
    - Model physical completion, contiguous ledger advancement, compatibility-table
      availability, compatibility version, and cumulative digest.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 2: every valid prefix converges`
    - _Requirements: 1.6, 2.1, 2.3, 2.4, 2.5_
  - [x] 3.2 Implement Property 3: every crash boundary recovers, with at least 100 cases.
    - Inject interruption before or after each bootstrap, execution, ledger, and
      compatibility operation, then replay from the resulting state.
    - Assert equivalence with uninterrupted target schema, ledger, compatibility
      version, and digest.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 3: every crash boundary recovers`
    - _Requirements: 2.2, 2.3, 2.4, 2.5_
  - [x] 3.3 Implement Property 4: fence loss stops mutation, with at least 100 cases.
    - Inject fence loss at every generated fence-check boundary and assert that no later
      bootstrap, migration, ledger, or compatibility state changes.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 4: fence loss stops mutation`
    - _Requirements: 1.3, 1.4, 1.7_

- [x] 4. Add preservation properties and exact bootstrap cases
  - [x] 4.1 Implement Property 5: decisions outside the bug condition are preserved,
    with at least 100 cases.
    - Generate valid observations and both migration policies.
    - Assert mutation-free validate-only, `MigrationRequired`, and
      `Compatible { legacy_backfill: false }` behavior.
    - Assert that automatic `Compatible { legacy_backfill: true }` for missing or
      checksum-valid lagging metadata uses the validated metadata-backfill sequence.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 5: decisions outside the bug condition are preserved`
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 4.2 Implement Property 6: bootstrap sources and schema contract are preserved,
    with at least 100 cases.
    - Sample every pre-claim and post-claim plan position and compare its bytes with the
      selected embedded migration source.
    - Assert one validated statement and unchanged embedded migration identities,
      target/readable envelope, consistency marker, and migration-set digest.
    - Tag: `// Feature: managed-embedded-dsql-schema-bootstrap, Property 6: bootstrap sources and schema contract are preserved`
    - _Requirements: 1.2, 1.5, 5.2_
  - [x] 4.3 Extend fixed migration unit tests.
    - Assert the exact pre-claim set `[V001, V067]` and exact post-claim order
      `[V001, V067, V066]`.
    - Keep byte-for-byte `include_str!` checks and the existing decision-table cases.
    - Verify that V066 remains an ordinary recognized ledger migration rather than a
      synthetic ledger entry.
    - _Requirements: 1.2, 1.5, 1.6, 3.1, 3.2, 3.3, 3.4_

- [x] 5. Checkpoint: focused storage implementation and properties are green
  - Run `cargo +nightly fmt --all`.
  - Run the focused migration/property suite with
    `cargo test -p tokeira-storage --features dsql --lib --locked`.
  - Run `cargo clippy -p tokeira-storage --features dsql --all-targets --locked -- -D warnings`.
  - Confirm all six property tags are present, each case count is at least 100, and the
    Property 1 failure evidence was recorded before the correction.
  - _Requirements: 1.1–1.7, 2.1–2.5, 3.1–3.4, 5.2_

- [x] 6. Align checked-in contracts and bounded diagnostic documentation
  - [x] 6.1 Update the migration section of
    `.kiro/specs/managed-embedded-dsql/design.md` to distinguish exact V001/V067
    pre-claim coordination bootstrap from exact V066 post-claim application-metadata
    bootstrap.
    - _Requirements: 1.1, 1.5, 1.6_
  - [x] 6.2 Update `crates/tokeira-storage/AGENTS.md` so the baseline lock is described
    as a guard against uncoordinated edits while an explicitly approved pre-release
    re-cut must update every schema artifact together.
    - Preserve the strict forward-only rule after the first durable release.
    - _Requirements: 5.1, 5.3_
  - [x] 6.3 Correct the `startup_phase`/`EmbeddedEngineStartError::Phase` documentation
    that claims discarded inner errors remain in host telemetry.
    - Keep the current phase-only public error; do not add raw nested causes.
    - _Requirements: 4.4, 4.5_
  - [x] 6.4 Verify by diff that this correction changes no migration SQL,
    `schema-baseline.lock`, `schema-contract.toml`, generated build metadata, migration
    digest, dependency manifest, or `Cargo.lock`.
    - _Requirements: 5.2_

- [x] 7. Make the live managed-DSQL harness teardown-safe
  - [x] 7.1 Factor the post-ready engine work in
    `crates/tokeira-engine/tests/live_managed_dsql.rs` into an
    `exercise_ready_cluster` helper.
    - Capture startup and exercise errors instead of returning early from the outer test.
    - Attempt explicit shutdown for every engine handle successfully created.
    - Retain `service_override`, competing-owner rejection, restart, and persisted-run
      assertions.
    - _Requirements: 4.1, 4.2_
  - [x] 7.2 Factor descriptor-bound administrative destruction into
    `destroy_ready_cluster` and invoke it after every post-ready exercise result.
    - Preserve `plan_destroy`, exact plan confirmation, `apply_destroy`, and destroyed
      tombstone verification.
    - Preserve the exact `CREATE_AND_DELETE` acknowledgement gate.
    - _Requirements: 4.2, 4.3_
  - [x] 7.3 Add exhaustive helper tests for success/failure combinations.
    - Prove teardown is attempted once after readiness for exercise success and failure.
    - Give teardown failure resource-safety precedence in the bounded result class.
    - Add canary errors containing credential, database-token, creation-token,
      connection-string, endpoint, and descriptor-like values; assert none appear in the
      combined output.
    - _Requirements: 4.2, 4.4, 4.5_

- [x] 8. Add the opt-in real DSQL schema boundary
  - [x] 8.1 Add `crates/tokeira-storage/tests/dsql_schema_bootstrap.rs` behind
    `dsql-integration`.
    - Require an explicitly supplied disposable test database and refuse to reset or
      mutate an unrecognized/non-test schema.
    - Do not read or use any managed descriptor.
    - _Requirements: 2.1, 4.3_
  - [x] 8.2 Exercise recovery from the minimal real partial prefix.
    - Build the V001 fixture only from the embedded migration bytes and identity; do not
      hard-code a checksum or modify compatibility rows.
    - Acquire the real `schema-migration` control lease and call `apply_decision` through
      the existing administrative connection path.
    - _Requirements: 1.1, 1.2, 1.3, 1.6, 2.1, 2.2_
  - [x] 8.3 Verify the real final boundary.
    - Assert every ledger identity through `TARGET`, ledger head `TARGET`, latest
      compatibility version `TARGET`, and the release contract's cumulative digest.
    - Keep the test opt-in and unexecuted by default; compilation remains part of the
      normal feature validation.
    - _Requirements: 2.3, 2.4, 2.5_

- [x] 9. Checkpoint: storage integration and engine harness compile cleanly
  - Run `cargo +nightly fmt --all`.
  - Run focused storage tests with `--features dsql` and `--locked`.
  - Compile the opt-in storage and engine boundaries with `--features dsql-integration`
    without executing the ignored billable test.
  - Run clippy for `tokeira-storage` and `tokeira-engine`, all targets, relevant features,
    `--locked`, and `-D warnings`.
  - Run the non-billable engine unit/property tests for cleanup-result classification and
    secret canaries.
  - _Requirements: 1.1–1.7, 2.1–2.5, 3.1–3.4, 4.1–4.5, 5.1–5.3_

- [x] 10. Checkpoint: complete Tokeira finish-green bar
  - Run `cargo +nightly fmt --all`.
  - Run `cargo lint --locked`.
  - Run `cargo check --workspace --locked`.
  - Run `cargo nextest run --workspace --locked`.
  - Run `cargo test --workspace --doc --locked`.
  - Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Confirm the build did not dirty the tree and `Cargo.lock` is unchanged.
  - Do not run the billable live-AWS test without a separate explicit operator
    authorization that supplies the private descriptor path and exact acknowledgement.
  - _Requirements: all_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"] },
    { "wave": 2, "tasks": ["2"] },
    { "wave": 3, "tasks": ["3", "4"] },
    { "wave": 4, "tasks": ["5"] },
    { "wave": 5, "tasks": ["6", "7", "8"] },
    { "wave": 6, "tasks": ["9"] },
    { "wave": 7, "tasks": ["10"] }
  ]
}
```

Task 1 must produce the expected failure before Task 2 changes the ordering. Tasks 3 and
4 depend on the corrected production plan. Tasks 6–8 may proceed only after the focused
storage checkpoint; Task 9 joins those paths before the full repository bar.

## Notes

- Expected-red evidence was captured before Task 2 with
  `cargo test -p tokeira-storage --features dsql --lib --locked property_bug_condition_is_eliminated -- --nocapture`.
  Proptest shrank the failure to `applied_prefix = 0`: the production-derived plan
  recorded V001 and attempted `PersistCompatibility(V001)` without any V066 bootstrap.
- The expected-red exploration interval is temporary and must not be pushed or proposed
  for review. Preserve its failing output as implementation evidence, then make the same
  test green through Task 2.
- The correction is intentionally smaller than a migration interpreter refactor. The
  private statement slice is the only new production test seam.
- The opt-in DSQL fixture is for a separately supplied disposable test database, never
  the operator's descriptor-bound cluster. It must fail closed rather than reset an
  unexpected schema.
- The tracked schema artifacts remain unchanged because the selected correction changes
  ordering only. The policy wording permits a future approved pre-release re-cut but does
  not perform one here.
- No dependency or kernel change is expected. Stop and raise if implementation implies
  either.
- Live AWS remains billable and destructive. Test code must be ready before authorization,
  but no live control-plane call is implied by approving this plan.
