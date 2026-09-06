# DSQL Connector 0.2 and SQLx 0.9 — Tasks

- [x] 0. Approval: an Architectural-class change (dependency movement across three crates
  and the lock), shipping in the 0.3.0 train the gRPC stack move already requires
  - _Requirements: 5.2_
  - **DONE 2026-09-06:** Operator implementation handoff authorizes this architectural
    dependency move; no version bump.

- [x] 1. Manifests and lock
  - **DONE 2026-09-06:** Manifests and lock updated together; exact movement is recorded below.

  - [x] 1.1 `crates/tokeira-storage/Cargo.toml`: connector `=0.2.2` with no features; `sqlx`
    `0.9` with the present feature list
    - _Requirements: 1.1, 1.2_
    - **DONE 2026-09-06:** Pinned connector =0.2.2 without pool/occ; SQLx 0.9 retains the
      existing features.

  - [x] 1.2 `crates/tokeira-projection/Cargo.toml` and `apps/tkr/Cargo.toml`: `sqlx` `0.9`
    - _Requirements: 1.2_
    - **DONE 2026-09-06:** Projection and tkr use SQLx 0.9 with their existing feature lists.

  - [x] 1.3 Root `Cargo.toml`: add `tracing-log` to the `tracing-subscriber` features;
    `crates/tokeira-observability/Cargo.toml`: `log` as a dev-dependency
    - _Requirements: 4.5_
    - **DONE 2026-09-06:** Declared tracing-log explicitly and added the observability log
      dev-dependency.

  - [x] 1.4 Regenerate the lock for those lines only; review the diff and record every line
    that moved beyond the SQLx and connector crates with its reason
    - _Requirements: 1.3, 1.4, 1.7_
    - **DONE 2026-09-06:** Offline workspace resolution exactly matches the handoff; see the
      package-by-package record below.

- [x] 2. Code on SQLx 0.9 and connector 0.2.2
  - **DONE 2026-09-06:** All thirteen call sites compile on SQLx 0.9; connection categories
    retain their observable labels.

  - [x] 2.1 Connection Factory: four explicit `from_dsql_error` arms with the compiler-required
    fallback for `#[non_exhaustive]`, remove `OccRetry` and the
    `occ_retry` label, keep the four-case unit test
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
    - **DONE 2026-09-06:** Removed OccRetry; retained four explicit arms and the
      operator-approved, compiler-required fallback, with the exact-pin review contract
      documented.

  - [x] 2.2 Migration runner: attest the four `migration.sql` sites; pass the two slice
    elements by value
    - _Requirements: 3.1, 3.2, 3.3, 3.6_
    - **DONE 2026-09-06:** Four audited migration strings and two copied static-slice elements;
      migration bytes are unchanged.

  - [x] 2.3 Worker-compute repository: attest the three `ACTION_COLUMNS` sites
    - _Requirements: 3.1, 3.2, 3.4_
    - **DONE 2026-09-06:** Three owned ACTION_COLUMNS queries carry SQL safety comments and
      attestations.

  - [x] 2.4 Projection store: attest the four compiler-output sites
    - _Requirements: 3.1, 3.2, 3.5_
    - **DONE 2026-09-06:** Four owned compiler-output queries carry SQL safety comments and
      attestations.

  - [x] 2.5 Confirm the `#[cfg(test)]` `DatabaseError` impl and `bind_sql_values` compile
    unchanged; confirm no `raw_sql` or new dynamic site was introduced
    - _Requirements: 3.7_
    - **DONE 2026-09-06:** The unchanged DatabaseError test implementation and bind_sql_values
      compile in the DSQL-enabled focused run; source scan confirms eleven attestations and no
      raw_sql.

- [x] 3. Checkpoint: `cargo check --workspace --locked` clean; storage, projection, and `tkr`
  suites green; `tkr schema` commands compile
  - **DONE 2026-09-06:** Workspace check passes; the complete nextest run passes all 3,341
    tests, including storage, projection, tkr, and the engine architecture checks.

- [x] 4. Policy
  - **DONE 2026-09-06:** Lock policy, advisory cleanup, and the tonic spec correction are
    complete; architecture tests and cargo deny pass.

  - [x] 4.1 Lock Invariant: remove the Connector Exception, add the unconditional absence
    list and the single-version assertions (Property 1)
    - _Requirements: 1.3, 1.4, 1.5, 1.6_
    - **DONE 2026-09-06:** The architecture test passes with SQLx 0.9 single-version checks,
      connector =0.2.2, and unconditional legacy-client absence.

  - [x] 4.2 `deny.toml`: remove the four advisories and their comments; run
    `cargo deny check advisories bans licenses sources`
    - _Requirements: 2.1, 2.2, 2.3_
    - **DONE 2026-09-06:** Removed only the four resolved advisories and their comments; cargo
      deny --locked check advisories bans licenses sources passes.

  - [x] 4.3 Amend `tonic-0-14-grpc-stack` requirements 1.3, its Connector Exception glossary
    entry, and its design Property 1 to name http 0.2 and http-body 0.4 as SDK Type
    Dependencies and record that the exception ended
    - _Requirements: 1.5_
    - **DONE 2026-09-06:** Corrected the tonic glossary, criterion 1.3, and Property 1; SDK
      type dependencies remain permitted after the exception ends.

- [x] 5. Properties and static checks
  - **DONE 2026-09-06:** Properties 2 and 3 and the runtime log bridge pass in the focused
    suite; all five engine architecture tests pass in the workspace run.

  - [x] 5.1 Property 2 property test: compiled projection SQL is value-free
    - _Requirements: 3.5_
    - **DONE 2026-09-06:** Random request strings and custom names stay out of compiled
      filters; newly allocated placeholders and bind values retain exact order. Focused nextest
      passes.

  - [x] 5.2 Property 3 property test: failure categories are total and message-independent
    - _Requirements: 4.2, 4.3, 4.4_
    - **DONE 2026-09-06:** Proptest covers all four enabled error variants over arbitrary
      messages; focused nextest passes.

  - [x] 5.3 Property 4 test: the Log Bridge is declared in the manifest and resolved in the
    lock; plus the runtime bridge test in `tokeira-observability`
    - _Requirements: 4.5, 4.6_
    - **DONE 2026-09-06:** Manifest and lock assertions pass in embedded_architecture; the
      capturing-layer test verifies the connector log target and message reach tracing.

  - [x] 5.4 Attestation scan: exactly eleven `AssertSqlSafe` sites across the three crates,
    each preceded by a `// SQL safety:` comment
    - _Requirements: 3.2, 3.8_
    - **DONE 2026-09-06:** The architecture scan passes: exactly eleven attestations, each with
      its nearby SQL safety comment.

  - [x] 5.5 Extend `credentialed_sql_and_aws_tests_are_non_default_and_sleep_free` to the
    new live test file
    - _Requirements: 6.3_
    - **DONE 2026-09-06:** The engine guard passes with the IAM test included in its exact
      first-line feature gate and no-sleep checks.

- [ ] 6. Live Evidence
  - [x] 6.1 Add `crates/tokeira-storage/tests/dsql_connector_iam.rs` (feature-gated,
    endpoint-gated, sleep-free)
    - _Requirements: 6.2_
    - **DONE 2026-09-06:** Added the feature/endpoint/region-gated SELECT 1 test; cargo check
      -p tokeira-storage --features dsql-integration --test dsql_connector_iam --locked passes
      without a live run.

  - [x] 6.2 Write `docs/testing/dsql-live-suites.md`; link it from the testing index if one
    exists
    - _Requirements: 6.5_
    - **DONE 2026-09-06:** Documented URL fallback, endpoint/region gates, schema-bootstrap
      acknowledgement, and the managed lifecycle link; offline lychee passes.

  - [ ] 6.3 Run the endpoint-gated test and the ten URL-gated tests against a live cluster
    on the migrated stack; record the runs here with dates
    - not run in the landing slice; operator host
    - _Requirements: 4.7, 6.4_
  - [ ] 6.4 Run the managed lifecycle test once on the migrated stack; record the run here
    - not run in the landing slice; operator host
    - _Requirements: 4.7, 6.4_

- [x] 7. Documentation and release notes
  - **DONE 2026-09-06:** Documentation confirmation and both release fragments are complete.

  - [x] 7.1 Confirm `docs/architecture/060-connection-management.md`, `docs/crates/storage.md`,
    and `docs/crates/projection.md` stay accurate; record the confirmation
    - _Requirements: 6.6_
    - **DONE 2026-09-06:** All three named documents remain accurate without edits; the
      specific contracts checked are recorded below.

  - [x] 7.2 Add the `changed` and `security` fragments under `.changes/unreleased/`
    - _Requirements: 5.3_
    - **DONE 2026-09-06:** Added changed and security fragments with matching UUID Slice values
      and bodies within 8–180 characters.

- [x] 8. Checkpoint: the root §10.4 Bar and `cargo deny check` green at the head; public
  storage items keep their names and shapes over SQLx 0.9 types
  - _Requirements: 1.8, 5.1, 5.4, 6.1_
  - **DONE 2026-09-06:** All six bar commands and cargo deny pass after the operator-approved
    three-line edge cleanup; lint has zero warnings and all 3,341 workspace tests pass.
    Public storage items retain their names and shapes over SQLx 0.9 types, apart from
    the specified removal of the unreachable OccRetry error variant.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0"] },
    { "id": 1, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 2, "tasks": ["1.4"] },
    { "id": 3, "tasks": ["2.1", "2.2", "2.3", "2.4"] },
    { "id": 4, "tasks": ["2.5", "3"] },
    { "id": 5, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 6, "tasks": ["5.1", "5.2", "5.3", "5.4", "5.5", "6.1", "6.2", "7.1", "7.2"] },
    { "id": 7, "tasks": ["6.3", "6.4"] },
    { "id": 8, "tasks": ["8"] }
  ]
}
```

## Notes

- Task 0 is the Architectural-class approval the root change classification requires for
  dependency movement. The breaking-release decision was taken with the gRPC stack move;
  this feature rides the same 0.3.0 train.
- Task 1.4 is the only step that touches the lock, and it moves exactly the lines the
  resolver needs. `cargo deny check` is not part of the Bar; task 4.2 runs it explicitly,
  and CI's supply-chain gate runs it again.
- Task 4.3 edits another feature's spec. It is executed only on approval of this spec,
  which is the explicit instruction the root spec-editing rule requires.
- Tasks 6.3 and 6.4 run on the operator's host with credentials, never in CI, and are part
  of this feature's acceptance: without them the driver's wire behaviour on 0.9 and the
  connector's IAM path on 0.2.2 are unverified.
- The eleven Attested Sites are enumerated in the requirements' evidence; the scan in task
  5.4 holds that count so that a twelfth site is a deliberate change to the test, not an
  accident.

## Landing evidence — 2026-09-06

- The operator authorized implementation and the dependency movement in the 0.3.0 train.
  No release version changed. The operator also approved retaining the compiler-required
  fallback after a four-arm match failed with E0004: connector 0.2.2 declares `DsqlError`
  as `#[non_exhaustive]` in `src/error.rs`. The exact pin and review requirement prevent
  an added variant from arriving silently; Property 3 remains unchanged.
- The operator approved the three-line edge cleanup needed for zero-warning lint:
  `grpc/translate.rs` copies `event_time` and `metering_metadata` directly, and
  `worker_inventory.rs` copies `start_time` directly. These Copy values retain the
  same behavior; no other edge code changed.
- SQL safety comments also cover the existing directory-backed `MigrationRunner::new`
  path, as verified in `discover` and `discover_directory_migrations`. Production still
  uses the embedded corpus. No migration bytes, SQL text, or repository logic changed.
- `cargo update --workspace --offline` reproduced the handoff's expected lock movement
  exactly. Updated: connector 0.1.2 → 0.2.2; `sqlx`, `sqlx-core`, `sqlx-macros`,
  `sqlx-macros-core`, `sqlx-mysql`, `sqlx-postgres`, and `sqlx-sqlite` 0.8.6 → 0.9.0.
  SQLx 0.9 requires the accompanying `etcetera` 0.8.0 → 0.11.0, `flume` 0.11.1 → 0.12.0,
  `hashlink` 0.10.0 → 0.11.1, `hkdf` 0.12.4 → 0.13.0, and `whoami` 1.6.1 → 2.1.3 moves.
- The connector's disabled SDK defaults and SQLx's root-store move remove `h2` 0.3.27,
  `hyper` 0.14.32, `hyper-rustls` 0.24.2, `tokio-rustls` 0.24.1, `rustls` 0.21.12,
  `rustls-webpki` 0.101.7, and `webpki-roots` 0.26.11. Dependencies no longer required
  by SQLx 0.9 and the remaining client graph also leave: `md-5` 0.10.6,
  `num-bigint-dig` 0.8.6, `num-iter` 0.1.45, `pkcs1` 0.7.5, `plain` 0.2.3,
  `redox_syscall` 0.7.4, `rsa` 0.9.10, `sct` 0.7.1, `socket2` 0.5.10, and `wasite` 0.1.0.
- `http` 0.2.12 and `http-body` 0.4.6 remain SDK type dependencies. AWS SDK package
  versions are unchanged; dependency-array changes reflect the new graph and removed
  duplicate-version qualifiers. The connector adds its already-resolved
  `aws-credential-types` and `log` dependencies; observability adds its `log`
  dev-dependency. No unrelated package version moved. The optional SDK spike was not run,
  so its separate lock remains unchanged.
- Confirmed without edits: `docs/architecture/060-connection-management.md` still describes
  the reservoir as the sole runtime owner and `connect_with` as the IAM connection path;
  `docs/crates/storage.md` and `docs/crates/projection.md` still describe the correct
  persistence, connection, and visibility boundaries. No testing index exists under
  `docs/testing/`; `dsql-live-suites.md` links the existing managed lifecycle runbook.

## Validation — 2026-09-06

| Command | Result |
|---|---|
| `cargo +nightly fmt --all` | Passed |
| `cargo lint --locked` | Passed; zero warnings after the operator-approved three-line edge cleanup |
| `cargo check --workspace --locked` | Passed |
| `cargo nextest run --workspace --locked` | 3,341 passed; two existing ignored tests skipped |
| `cargo test --workspace --doc --locked` | Passed; 1 doctest passed, 20 ignored |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Passed |
| `cargo deny --locked check advisories bans licenses sources` | All four checks passed; existing duplicate-version and license-metadata warnings |
| `cargo nextest run -p tokeira-storage -p tokeira-projection -p tokeira-observability --features dsql --locked` | 418 passed |
| `cargo check -p tokeira-storage --features dsql-integration --test dsql_connector_iam --locked` | Passed; compile only |
| `lychee --offline --no-progress --hidden` on all changed Markdown files | 19 links passed |

The native linker reports macOS minimum-version warnings in dependency objects during
builds; no toolchain or shared-cache configuration was changed. The four removed RustSec
advisories are not encountered by cargo-deny. Tasks 6.3 and 6.4 remain operator-host work,
and this landing slice does not claim live wire or IAM verification.
