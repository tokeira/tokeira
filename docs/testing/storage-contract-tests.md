# Storage contract tests

The [specification](../../.kiro/specs/storage-contract-tests/requirements.md)
requires one semantic oracle for memory and DSQL. Shared cases live in
`crates/tokeira-storage/src/contract_tests/`; the adapters live under
`memory::contract_tests` and `dsql::run_repository::contract_tests`. Expectations
come from authored inputs, not the production selector being tested.

Verification completed on 1 November 2026; the receipt below records the executed
selection and immutable archives. A listed test or a successful default run is
not evidence of live DSQL execution.

## Generated contracts

Names below are exact leaf names. The memory and live storage module prefixes
above make them fully qualified; the acquisition entries are under the runtime
crate's `runtime::dsql_tests` module. Confirm selections with nextest's archive
listing before execution.

| Property or boundary | Memory entry | DSQL entry | Required execution |
|---|---|---|---|
| 1: History pages, suffixes, exact events and principals | `storage_contract_memory_history` | `storage_contract_live_history` | 100 generated cases each, plus zero/absent, exact page multiples, >1,000 events and split-history examples |
| 2: Current-format recovery, zero limit and removed cursor row | `storage_contract_memory_recovery` | `storage_contract_live_recovery` | 100 cases each |
| 3: Image ordering, scopes and a late insert below the cursor | `storage_contract_memory_projection_pages` | `storage_contract_live_projection_pages` | 100 cases each |
| 4: Exact image deletion and unobserved newer images | `storage_contract_memory_projection_deletion` | `storage_contract_live_projection_deletion` | 100 cases each |
| 5: Accepted/rejected writes, dedupe and current-pointer outcomes | `storage_contract_memory_commit_outcomes` | `storage_contract_live_commit_outcomes` | 100 cases each |
| 6: Competing same-sequence writes and competing starts | `storage_contract_memory_competing_commits` | `storage_contract_live_competing_commits` | 100 cases each |
| 7: Original lease fence after takeover | `storage_contract_memory_lease_fence` | `storage_contract_live_lease_fence` | 100 cases each |
| 8: Legacy-before-flagged recovery and a permitted phase revisit | No legacy physical representation | `storage_contract_live_legacy_recovery` | 100 DSQL cases |
| 9: Truncation, reordering, omitted continuation and partial-write controls | `contract_tests::storage_contract_oracle_rejects_corruption` | Same pure comparator; no database claim | 100 generated cases, all four mutations per case |
| 10: Transaction startup cancellation and preserved health | `dsql::connection::transaction_tests::storage_contract_transaction_start_health` (pure; requires the `dsql` feature) | `dsql::connection::transaction_tests::live::dsql_transaction_start_cancellation_and_preservation` | 100 generated cases covering all four outcomes each; real cancelled read-only/write startup, one-slot retirement, commit and rollback preservation |
| Direct SQL limit binding and mismatched explicit home | No database limit restriction | `storage_contract_live_limit_binding` | Fixed DSQL examples |
| Recovery across more than one page before activation | Existing runtime unit coverage | `storage_contract_live_acquisition_spans_recovery_pages` | Real repository, preparation, lease and acquisition |
| Superseded acquisition cannot install state | Existing generation-scoped tracker tests | `workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs` | Existing real acquisition invariant |

All new live entries require `dsql-integration`, are ignored by default and fail
when explicitly run without `TOKEIRA_DSQL_TEST_DATABASE_URL`. They do not fall
back to `DATABASE_URL`. The pure missing-configuration check is
`contract_tests::explicitly_selected_live_test_requires_configuration`.

Both backends use fixed seed `0x53544f5241474500 + property_number` and the same
proptest strategy. Failure output includes the seed and shrunk logical input.
Every attempt, including shrinking, receives fresh physical identities that
preserve the trace's ordering/home/partition relationships. No physical identity
is needed to replay the logical counterexample.

Set `TOKEIRA_STORAGE_CONTRACT_OUTPUT` to an operator-owned local directory to
retain TOML receipts. Each successful generated entry records its test, backend,
seed and successful case count. The captured tracing event contains the same
receipt. Counts are also checked inside the runner; a receipt never substitutes
for the final nextest outcome, because later fixed examples can still fail.

## Deliberate backend differences

| Observation | Policy and authority |
|---|---|
| Transaction, row and value sizes | Memory has no DSQL physical limits. Run the owning live suites below; [bounded bulk writes](../../.kiro/specs/bounded-bulk-writes/bugfix.md) and [large value parts](../../.kiro/specs/large-value-parts/bugfix.md) own the boundaries. |
| History caller limit | DSQL binds a fixed internal batch size, not the caller's event limit. The directly bound out-of-range checks apply to projection/recovery only. |
| Diagnostic transition audit | Memory retains operation vectors; DSQL reconstructs history-batch records. Rejection compares complete before/after audits within each backend, not invented identical persistence formats. |
| Recovery phases | Current-format candidates share one oracle. DSQL's NULL flag is a fixture-only legacy representation; [recovery index](../../.kiro/specs/recovery-index/requirements.md) defines the phase cursor. |
| Projection deletion and legacy seeding | Memory retains the last unseeded image; DSQL can seed from the newer image or applied visibility value. See [projection passes](../../.kiro/specs/projection-passes/bugfix.md). |
| Projection timestamp | DSQL checks seeded `created_at`; memory returns `None`. It is diagnostic, not ordering authority. |
| Physical row observations | Adapters compare complete row values within a backend. Semantic state/history/images are checked against shared expectations. Row counts alone cannot prove rejection. |

## Existing cases reused

These are existing owning-suite entries, not additional generated properties.
Their execution and case counts must be recorded separately from the new suite.
Names are exact leaves; expand module prefixes from the archived listing.

| Shared/physical case | Memory entry | DSQL entry |
|---|---|---|
| Event batches | `memory_read_from_batches_of_any_size` | `dsql_history_event_read_from_batches_of_any_size` |
| Reset-copy event lookup | `memory_read_from_a_reset_successors_copied_batches` | `dsql_history_event_read_from_a_reset_successors_copied_batches` |
| Split history | `memory_batches_over_the_part_size_read_back` | `dsql_batches_over_the_part_size_read_back` |
| Split state | `memory_a_state_over_the_part_size_loads_back` | `dsql_a_state_over_the_part_size_loads_back` |
| Split reset | `memory_a_reset_copies_values_over_the_part_size` | `dsql_a_reset_copies_values_over_the_part_size` |
| Split deletion | `memory_deletion_of_a_run_with_split_values` | `dsql_deletion_of_a_run_with_split_values` |
| Small backlog row budget | `bulk_spill_of_many_small_entries` | `dsql_bulk_spill_of_many_small_entries` |
| Large backlog byte budget | `bulk_spill_of_large_entries` | `dsql_bulk_spill_of_large_entries` |
| Large deletion | `bulk_deletion_of_a_large_run` | `dsql_bulk_deletion_of_a_large_run` |
| Reset above part/transaction sizes | `bulk_reset_over_one_mib`, `bulk_reset_over_ten_mib` | `dsql_bulk_reset_over_one_mib`, `dsql_bulk_reset_over_ten_mib` |
| Reset with many timers | `bulk_reset_with_many_timers` | `dsql_bulk_reset_with_many_timers` |
| Reset ordering/current pointer | `bulk_plain_reset_of_a_closed_workflow`, `bulk_reset_with_a_start_between` | `dsql_bulk_plain_reset_of_a_closed_workflow`, `dsql_bulk_reset_with_a_start_between` |
| Legacy image retention/fallback | `projection_accumulator_survives_restart_and_legacy_projection` | `dsql_projection_accumulator_seeds_from_the_newer_of_image_and_applied_value` |
| Shared lease rejection/renewal | `property_4_a_lost_fence_changes_nothing`, `the_owner_keeps_writing_across_lease_renewals` | `dsql_lease_fence_a_lost_fence_changes_nothing`, `dsql_lease_fence_the_owner_keeps_writing_across_renewals` |

DSQL-only supplemental selections are:

- `dsql_bulk_interrupted_deletion` and
  `dsql_bulk_abandoned_materialization_fences_its_stragglers`.
- `dsql_a_purge_held_after_each_transaction_leaves_whole_batches`,
  `dsql_a_history_read_held_before_its_parts_reads_whole_batches`,
  `dsql_a_load_held_before_its_parts_reads_one_state`, and
  `dsql_a_row_of_two_parts_commits`.
- `dsql_lease_fence_stale_commit_for_bundle_is_refused` and
  `dsql_lease_fence_stale_commit_is_refused`, which hold real transactions at
  controlled pause points beyond the shared takeover-before-attempt case.
- `atomic_successors_live` and
  `outbound_live_successor_reservation_and_interrupted_publication` for atomic
  successors and interrupted large-successor publication.
- All six `dsql_transaction_limits` integration entries:
  `dsql_three_thousand_rows_commit_and_one_more_is_refused`,
  `dsql_deleted_rows_count_toward_the_rows`,
  `dsql_deleted_bytes_do_not_count_toward_the_bytes`,
  `dsql_rows_read_for_update_do_not_count_toward_the_rows`,
  `dsql_a_value_over_one_mib_is_refused`, and
  `dsql_nine_mib_of_values_commit_and_ten_are_refused`.

The transaction probe recognizes the existing limit refusal and verifies no
partial writes. An unrelated SQL error is not success. The shared oracle does
not claim CHASM, worker-deployment or visibility-query parity, or full functional
conformance.

The transaction-start repair also needs preservation checks for the affected
CHASM, worker and outbound entry points. Run these supplemental live entries
after the preparation-sensitive group and before the final repair/isolation
reruns; each still requires the configured test URL:

- `dsql_nodes_over_the_part_size_read_back`
- `dsql_a_load_held_before_its_parts_reads_one_version`
- `dsql_requests_over_the_part_size_read_back`
- `dsql_worker_deployment_repository_cas_and_pagination_match_contract`
- `outbound_live_future_child_result_retains_deleted_parent`
- `outbound_live_interrupted_large_legacy_cutover`
- `outbound_live_original_fences_and_retained_destination_proof`
- `outbound_live_reset_conflict_preserves_all_participants`

## Build once, execute the archive

Use the [live-suite runbook](dsql-live-suites.md) for connection setup and cluster
lifecycle. Keep credentials and resource identities out of source, commands
captured for publication and committed evidence. No live CI/nightly job is added.

On the verification build host, record the source revision and content snapshot
digest. For an uncommitted build, retain its filtered source snapshot and report
the dirty state; never label it a clean-commit build. Build an immutable archive:

```bash
cargo nextest archive -p tokeira-storage -p tokeira-runtime --features dsql-integration --locked --archive-file storage-contract.tar.zst
sha256sum storage-contract.tar.zst
cargo nextest list --archive-file storage-contract.tar.zst --workspace-remap "$PWD" --message-format json > storage-contract-tests.json
```

Copy the archive unchanged to the live runner, along with the matching source
tree for fixture paths and nextest configuration. Materialize that source at the
same absolute path used for the build: migration fixtures embed
`CARGO_MANIFEST_DIR` at compile time, and `--workspace-remap` does not rewrite
those literals. Verify the archive digest. Set the test URL and receipt directory
in the operator environment, then preflight:

```bash
: "${TOKEIRA_DSQL_TEST_DATABASE_URL:?configure the disposable test database}"
: "${TOKEIRA_STORAGE_CONTRACT_OUTPUT:?select a local receipt directory}"
```

For a brand-new database, complete schema setup and asynchronous index builds
before the generated properties. The existing
[plain migration-runner paths](dsql-live-suites.md#plain-migration-runner-paths)
describe the empty-database gate and acknowledgement; select that integration
binary from the matching archive when using it for setup. Do not run its
empty-database setup against an already initialized schema. The new contract
fixtures perform placement preparation themselves.

Keep this setup separate from the property timeout: initial migration plus the
100-case projection property can exceed twenty minutes even when the warm
property fits. A timed-out attempt is failed evidence and may leave owned rows;
identify and clean only those fixture keys before a new labelled execution.

Run commands separately in this order; filters do not order tests. Use
`--success-output final` to retain case receipts in successful test output.
After schema setup, exercise real transaction cancellation and one-slot reuse
before starting the generated repository cases:

```bash
cargo nextest run --archive-file storage-contract.tar.zst --workspace-remap "$PWD" --profile dsql-live --run-ignored all --success-output final -E 'test(dsql_transaction_start_cancellation_and_preservation)'
cargo nextest run --archive-file storage-contract.tar.zst --workspace-remap "$PWD" --profile dsql-live --run-ignored all --success-output final -E 'test(storage_contract_live_projection_)'
cargo nextest run --archive-file storage-contract.tar.zst --workspace-remap "$PWD" --profile dsql-live --run-ignored all --success-output final -E 'test(storage_contract_live_) & !test(storage_contract_live_projection_) & !test(storage_contract_live_acquisition_)'
```

Run the new acquisition next, while the placement marker still records 4096
homes. The existing placement fixtures subsequently reset their marker to eight
homes, so running the new acquisition after them would fail the routing check.

```bash
cargo nextest run --archive-file storage-contract.tar.zst --workspace-remap "$PWD" --profile dsql-live --run-ignored all --success-output final -E 'test(storage_contract_live_acquisition_spans_recovery_pages)'
```

Then execute the existing preparation-sensitive group in the live-suite
runbook's order, including the superseded-acquisition, runtime activity-input and
edge remote-start baselines. Run the edge baseline on the separate freshly
migrated disposable database required by that runbook, preserving all 16
scenarios and exact read assertions. Build a separate immutable archive
including edge if selecting that sibling check; the two-package archive above
does not contain it. After the shared-database preparation-sensitive checks
finish, execute the owning-suite selections from the matrix. Preserve the
runbook's complete-repair and projection-isolation reruns after the bulk suites.
The isolated edge baseline may run last; all live test processes remain serial.

New recovery/lease fixtures reserve homes 4094 and 4095 of 4096. Commit-generated
projection cases use real partition 3 of 16 and decoys in partition 4/fanout 2;
run them before other suites populate those partitions. Foreign rows are a
fixture error, never silently filtered or deleted. Every trial removes only its
own rows. Runtime cleanup stops acquisition/renewal before releasing owned
leases and purging owned runs. Do not run global preparation while leases from
other suites remain active.

The existing `dsql-live` profile is serial, has no retries and allows twenty
minutes per test. If a property exceeds it, split the property into named seeded
batches and verify at least 100 successful cases per backend/property overall;
do not lower coverage or change the profile implicitly.

## Delivery evidence

Retain the following together before marking coverage complete:

- Source SHA, dirty/clean state and source snapshot digest; archive SHA-256 and
  feature flags.
- Archived test listing, exact selected names, actual executed counts and
  passed/failed/skipped outcomes. A URL-unset return from an older test is
  unexecuted live coverage even if nextest reports a pass.
- Generated receipts: properties 1–7 each have at least 100 memory and 100 DSQL
  cases; Property 8 has 100 DSQL cases; Properties 9 and 10 have 100 pure cases each. Retain
  counterexample seed/input on any failure.
- Results for fixed history boundaries, physical refusals, existing owning
  cases and both runtime acquisition cases.
- Fixture cleanup and ephemeral-cluster teardown outcomes. Resource identifiers
  and connection data belong only in private operator records.

Run the full root AGENTS.md §10.4 bar as well. Default tests and live tests are
complementary; neither substitutes for the other.

## Transaction-start audit

SQLx 0.9.0's PostgreSQL transaction manager increments its tracked depth after
BEGIN completes. Cancellation before that point leaves its rollback guard at
depth zero, even when the server has received BEGIN. `DsqlPermit::begin` and
`begin_read_only` mark the connection bad before polling SQLx and restore its
previous flag only after successful startup. The existing return processors
retire uncertain connections without adding a ping or rollback to healthy return.

Repository writes, history/state/CHASM snapshots, projection images, outbound
reads, and worker-compute/deployment writes use those permit methods. Helpers
inside an established transaction still accept `PgConnection` because SQLx then
owns rollback-on-drop. The remaining raw starts are deliberately separate:

- `control_lease.rs` has a dedicated `PgPool` implementation and a caller-owned
  connection API. Engine schema/ownership acquisition uses the latter before
  runtime admission; failure or deadline cancellation shuts down its director
  (`apply_embedded_schema`, `acquire_embedded_ownership`, `take_node_claim` and
  their callers in `tokeira-engine`). Busy results arise after BEGIN returned a
  handle and use explicit rollback. Renewal/release issue individual statements rather
  than BEGIN; the single-node renewal path has its own cancellation guard.
- `migration.rs` operates on caller-owned migration connections. Pool-backed
  migration fixtures use dedicated SQLx pools. Embedded schema startup runs
  before runtime admission and shuts its director down on failure/cancellation.
  No migration API or schema changes are included here.
- Raw starts in live fixture modules and integration binaries use dedicated
  SQLx test pools rather than reservoir permits.

These raw-connection APIs do not acquire the new permit guarantee themselves.
Any future caller reusing their connection after an interrupted start must retire
it or pass ownership through a cancellation guard. The startup lifecycle is the
reason the existing engine paths are outside this storage-only repair.

## Verification receipt — 1 November 2026

The final Rust source snapshot passes nightly formatting, zero-warning workspace
lint/check, 4,132 nextest cases, doctests and warnings-denied documentation. The
default run retains two existing ignored SDK integration tests:
`sdk_echo_roundtrip_against_post_sync_tokeirad` and
`sdk_scoped_worker_roundtrip_uses_production_authorization_bootstrap`. Doctests
report one pass and 21 existing ignores. Storage/runtime/edge feature lint and
both focused baseline checks also pass. All 11 new live entries reject missing
configuration.

The accepted live selection comprises 66 executions across 64 test identities,
including schema bootstrap and both before/after repair and projection-isolation
runs. All 17 generated receipts record 100 successful cases: seven memory,
eight DSQL and two pure properties. Seeds are the formula documented above.
The 101-run acquisition verifies exact restored generations; the cancellation
regression verifies read-only/write startup retirement and successful commit/
rollback preservation. The isolated remote-start baseline executes all 16
inline/split, sticky, cache and conflict scenarios with exact phase assertions.

The source snapshots were built from an uncommitted tree based on
`757ea24589ca8208d1a58f0801e03dd3a127816b`, without Git metadata. Archive provenance
is retained separately from the eventual commit identity. Production code,
migrations and test configuration are identical across revisions 5–8; revisions
6–8 strengthen test assertions, error reporting and baseline measurement.

| Revision | Accepted executions | Archive SHA-256 |
|---|---|---|
| 5 | 63 shared, physical, preparation and repository executions | `c04682c07e7e5fd50b5798eb3f51a400bf64f5411debe9b3a46800147ed3b848` |
| 6 | Multi-page acquisition with exact generation assertions | `30c1d29ccf0cebd21dcdd2a99e919779e1c5c1a269f7dc0e3ed550487fcc8da7` |
| 7 | Cancellation with original-error-preserving cleanup | `0f53b71926664612f3bf167be68fe797660d61d51849a8317c7160af98a0ccc3` |
| 8 | Exact-phase remote-start baseline | `f9b495967ffd85f4689c2bac08816c0222c1dc8e4003136dafb99ee533754bae` |

Final Rust sources match revision 8; subsequent edits contain delivery
documentation only. Revision 8's source snapshot SHA-256 is
`828d8d9ac92fab856bf6f0d28653f9424acef8df8fc1639d27db15e5a4c8943a`.
The storage/runtime/edge archive lists 2,036 feature-enabled tests across 38 binaries;
unselected tests are not counted as live passes.

Earlier failures remain separate evidence: the original cancelled-BEGIN defect,
an archive extraction that ran out of disk before executing a test, and the two
baseline fixture/measurement failures described above. The corrected runs do
not erase these attempts. Cleanup of the failed shared baseline preserved other
fixtures; the isolated baseline left no workflow, dispatch or baseline-lease
rows. Both ephemeral databases and
the disposable verification devbox were deleted after evidence collection.
Resource identifiers, endpoints and credentials remain in private operator
records. No schema, dependency, lockfile, configuration or CI changes accompany
this delivery.
