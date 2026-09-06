# DSQL Connector 0.2 and SQLx 0.9 — Tasks

- [ ] 0. Approval: an Architectural-class change (dependency movement across three crates
  and the lock), shipping in the 0.3.0 train the gRPC stack move already requires
  - _Requirements: 5.2_

- [ ] 1. Manifests and lock
  - [ ] 1.1 `crates/tokeira-storage/Cargo.toml`: connector `=0.2.2` with no features; `sqlx`
    `0.9` with the present feature list
    - _Requirements: 1.1, 1.2_
  - [ ] 1.2 `crates/tokeira-projection/Cargo.toml` and `apps/tkr/Cargo.toml`: `sqlx` `0.9`
    - _Requirements: 1.2_
  - [ ] 1.3 Root `Cargo.toml`: add `tracing-log` to the `tracing-subscriber` features;
    `crates/tokeira-observability/Cargo.toml`: `log` as a dev-dependency
    - _Requirements: 4.5_
  - [ ] 1.4 Regenerate the lock for those lines only; review the diff and record every line
    that moved beyond the SQLx and connector crates with its reason
    - _Requirements: 1.3, 1.4, 1.7_

- [ ] 2. Code on SQLx 0.9 and connector 0.2.2
  - [ ] 2.1 Connection Factory: exhaustive `from_dsql_error`, remove `OccRetry` and the
    `occ_retry` label, keep the four-case unit test
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
  - [ ] 2.2 Migration runner: attest the four `migration.sql` sites; pass the two slice
    elements by value
    - _Requirements: 3.1, 3.2, 3.3, 3.6_
  - [ ] 2.3 Worker-compute repository: attest the three `ACTION_COLUMNS` sites
    - _Requirements: 3.1, 3.2, 3.4_
  - [ ] 2.4 Projection store: attest the four compiler-output sites
    - _Requirements: 3.1, 3.2, 3.5_
  - [ ] 2.5 Confirm the `#[cfg(test)]` `DatabaseError` impl and `bind_sql_values` compile
    unchanged; confirm no `raw_sql` or new dynamic site was introduced
    - _Requirements: 3.7_

- [ ] 3. Checkpoint: `cargo check --workspace --locked` clean; storage, projection, and `tkr`
  suites green; `tkr schema` commands compile

- [ ] 4. Policy
  - [ ] 4.1 Lock Invariant: remove the Connector Exception, add the unconditional absence
    list and the single-version assertions (Property 1)
    - _Requirements: 1.3, 1.4, 1.5, 1.6_
  - [ ] 4.2 `deny.toml`: remove the four advisories and their comments; run
    `cargo deny check advisories bans licenses sources`
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 4.3 Amend `tonic-0-14-grpc-stack` requirements 1.3, its Connector Exception glossary
    entry, and its design Property 1 to name http 0.2 and http-body 0.4 as SDK Type
    Dependencies and record that the exception ended
    - _Requirements: 1.5_

- [ ] 5. Properties and static checks
  - [ ] 5.1 Property 2 property test: compiled projection SQL is value-free
    - _Requirements: 3.5_
  - [ ] 5.2 Property 3 property test: failure categories are total and message-independent
    - _Requirements: 4.2, 4.3, 4.4_
  - [ ] 5.3 Property 4 test: the Log Bridge is declared in the manifest and resolved in the
    lock; plus the runtime bridge test in `tokeira-observability`
    - _Requirements: 4.5, 4.6_
  - [ ] 5.4 Attestation scan: exactly eleven `AssertSqlSafe` sites across the three crates,
    each preceded by a `// SQL safety:` comment
    - _Requirements: 3.2, 3.8_
  - [ ] 5.5 Extend `credentialed_sql_and_aws_tests_are_non_default_and_sleep_free` to the
    new live test file
    - _Requirements: 6.3_

- [ ] 6. Live Evidence
  - [ ] 6.1 Add `crates/tokeira-storage/tests/dsql_connector_iam.rs` (feature-gated,
    endpoint-gated, sleep-free)
    - _Requirements: 6.2_
  - [ ] 6.2 Write `docs/testing/dsql-live-suites.md`; link it from the testing index if one
    exists
    - _Requirements: 6.5_
  - [ ] 6.3 Run the endpoint-gated test and the ten URL-gated tests against a live cluster
    on the migrated stack; record the runs here with dates
    - _Requirements: 4.7, 6.4_
  - [ ] 6.4 Run the managed lifecycle test once on the migrated stack; record the run here
    - _Requirements: 4.7, 6.4_

- [ ] 7. Documentation and release notes
  - [ ] 7.1 Confirm `docs/architecture/060-connection-management.md`, `docs/crates/storage.md`,
    and `docs/crates/projection.md` stay accurate; record the confirmation
    - _Requirements: 6.6_
  - [ ] 7.2 Add the `changed` and `security` fragments under `.changes/unreleased/`
    - _Requirements: 5.3_

- [ ] 8. Checkpoint: the root §10.4 Bar and `cargo deny check` green at the head; public
  storage items keep their names and shapes over SQLx 0.9 types
  - _Requirements: 1.8, 5.1, 5.4, 6.1_

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
