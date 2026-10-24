# DSQL live suites

These suites require an operator-selected Aurora DSQL database and connect only when
their environment gates are set. Most require `dsql-integration`; the CHASM, task-queue
policy and worker-deployment unit tests listed below require `--features dsql` and also
compile in the default suite. Live execution is outside the default validation and CI.
Run from the repository root with `--locked`; keep endpoints, URLs, credentials, and
resource identities in the operator environment rather than in committed files or shared
output.

Run every live suite with the repository's `dsql-live` nextest profile
(`.config/nextest.toml`): it runs one test at a time, never retries, and allows each
test twenty minutes. On a new cluster the first test that migrates applies the whole
embedded corpus and waits for every asynchronous index build, which takes about ten
minutes, longer than the default profile's three-minute ceiling.

## Shared-cluster order and isolation

Run the preparation-sensitive suites in these separate commands, in this order,
before any suite that acquires a shard lease. A nextest filter does not specify
execution order. Placement preparation rejects every live lease, including the
lease left by `dsql_shard_leasing::stale_epoch_is_fenced_after_expired_takeover`.
The activity-input runtime fixture also prepares placement and belongs before
lease-taking suites. Its repository and runtime use the same eight homes as
the placement and acquisition fixtures.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_generated_complete_repair)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(placement_live_)'
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --test dsql_activity_inputs --test-threads 1
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs)'
```

Then run the remaining suites below. The generated complete-repair contract uses
1024 logical homes and selects identities that derive to reserved home 1023; other
shared-cluster fixtures use homes below 256. Both complete repair walks assert that
every inspected key belongs to the current generated case, then remove its hot
and dispatch rows. The contract still runs 100 cases under the unchanged timeout.
Run it again after the bulk-write and other dispatch suites to exercise isolation
against their leftovers. The projection-log pagination fixture uses a fresh
positive partition and fresh run keys, then removes only those keys. Run its
suite again after storage suites to check isolation without clearing their logs.

Capture these counts before the first suite and after each suite to identify which
fixtures leave work on shard zero. Keep the suite name and counts with local run
results; the queries expose no workflow identifiers.

```sql
SELECT COUNT(*) AS shard_zero_recovery_candidates
FROM workflow_hot
WHERE shard_id = '746f6b65-6972-612d-7368-617200000000'
  AND (recovery_needed = true OR recovery_needed IS NULL);
SELECT COUNT(*) AS shard_zero_dispatch_rows
FROM workflow_dispatch
WHERE shard_id = '746f6b65-6972-612d-7368-617200000000';
```

The read-only `dsql_shared_cluster_counts` integration probe runs the same
queries through SQLx. Set `TOKEIRA_DSQL_COUNTS_OUTPUT` to an operator-selected
local file, then run that test with the URL gate and `dsql-integration` enabled.
It writes only counts, or a `not bootstrapped` schema status on a fresh cluster.
It neither creates tables nor takes a lease.

## URL-gated storage and projection

Set `TOKEIRA_DSQL_TEST_DATABASE_URL` to a connection URL for a disposable test database.
The four integration suites fall back to `DATABASE_URL` when that variable is unset
and return without connecting when neither is set. The five CHASM tests, the lease-fence
check, the task-queue policy and worker-deployment tests, the projection store's and
passes' tests and the projection-log test use only `TOKEIRA_DSQL_TEST_DATABASE_URL` and
return without connecting when it is unset. The
seven projection-accumulator tests are ignored by default, use only that variable, and
fail when it is unset.
These suites connect through SQLx directly rather than the IAM connector. The database must permit schema setup and test-data writes; do not
point them at a database holding application data.

| Suite | Tests | Coverage |
|---|---:|---|
| Storage `dsql_shard_leasing` | 5 | Lease ownership and fencing, and every lease state an acquire can meet |
| Storage `dsql_on_conflict_counts` | 6 | The row count DSQL returns for an `ON CONFLICT` statement that leaves its row unwritten, and the answers of the slot, action, provenance, configuration and backlog operations built on one |
| Storage `dsql_embedded_ownership` | 1 | Embedded ownership lifecycle |
| Storage `dsql_lease_fence` | 1 | The conflict rules the lease-fence model assumes, through SQLx: ten interleavings of fenced run commits, renewals and takeovers on the suite's own probe tables, including the weaker fences the model rejects; no migrations; `dsql-integration`, URL gate only |
| Projection `dsql_projection_persistence` | 6 | Visibility persistence and queries, a partition's projection-log pages, and the attribute-index rows the projection-accumulator seed reads |
| Storage `dsql_archetype_scoped_business_ids` | 1 | CHASM Property 8: scoped pointers and backfill; `--features dsql`, URL gate only |
| Storage `dsql_chasm_node_store_round_trips_and_fences` | 1 | CHASM nodes, atomic pointers and fencing; `--features dsql`, URL gate only |
| Storage `dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes` | 1 | Concurrent CHASM creates and superseding pointers reject stale admission without orphaned nodes; `--features dsql`, URL gate only |
| Storage `dsql_a_chasm_execution_commits_only_with_its_pointer` | 1 | A new execution's nodes commit only when the current-run pointer held what the start expected; `--features dsql`, URL gate only |
| Storage `dsql_a_backfill_returns_what_it_copied` | 1 | Each CHASM backfill call returns the pointers it copied; `--features dsql`, URL gate only |
| Projection `dsql_store::tests` | 5 | A visibility row is replaced only by a newer version; an apply's counters commit with its row; a failed read of the stored row fails the apply; the apply reads its row through the primary key; and concurrent applies in one rollup stripe never conflict; `--features dsql-integration`, URL gate only |
| Projection `dsql_store::passes_tests` | 5 | Projection passes: images below, above and behind a pass's position all applied; two projectors of one partition outliving a failure, with Count exact; images deleted only once their run's used-versions list is applied, a failed apply keeping its images and an older partition never read; failed applies on a counter or a read keeping their images, with Count exact; and one partition's applies a second, serially; set `TOKEIRA_PROJECTION_THROUGHPUT_OUTPUT` to keep the figures; `--features dsql-integration`, URL gate only |
| Projection `dsql_store::bounded_apply_tests` | 9 | The bounded visibility apply: a first apply and an update past 3,000 index rows; interleaved large applies of one run matching one image at every step; a staged apply interrupted before its flip keeping the previous image; orphans matching nothing, and cleanup leaving nothing below the run's version; the copy of the earlier attribute index resuming from its stored position, two copies at once, and a finished copy writing nothing; and the cell statements' plans; `--features dsql-integration`, URL gate only |
| Engine `live_embedded_attr_copy` | 1 | The embedded engine, started on the cluster by its identity, copies the earlier attribute index before anything applies an image. It restarts the cluster's copy, so run it after the other suites; `--features dsql-integration`, gated on the test URL and `TOKEIRA_DSQL_TEST_CLUSTER_ID`, `TOKEIRA_DSQL_TEST_CLUSTER_ARN`, `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION` |
| Storage `dsql::projection_log::tests::live` | 1 | A partition of 2,000 images paged in key order and deleted by name, a key at most once, with the page reads and the delete through the primary key; set `TOKEIRA_PROJECTION_LOG_PLAN_OUTPUT` to save the plans; `dsql-integration`, URL gate only |
| Storage `dsql_task_queue_policy_survives_repository_recreation` | 1 | A task queue's stored policy survives recreating the repository; `--features dsql`, URL gate only |
| Storage `dsql_worker_deployment_repository_cas_and_pagination_match_contract` | 1 | Worker-deployment compare-and-set and pagination follow the repository contract; `--features dsql`, URL gate only |
| Storage `dsql_projection_accumulator_*` | 8 | Projection-accumulator contracts: commit images, legacy seeds, the seed from the newer of image and applied value, failure isolation, growth accounting, reset boundaries, mixed writers and pruning, and snapshot races; `dsql-integration`, ignored by default |
| Storage `workflow_dispatch_live_atomic_reference_traces` | 1 | 100 generated atomic state/dispatch cases, reset materialization, duplicate/CAS rejection, rollback, and deletion; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_ordered_pages` | 1 | Normal/Exact paging, home scans including sticky rows, reset boundaries, speculative legacy delivery, and forced digest collisions; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_reset_uses_execution_home` | 1 | Eight-shard reset placement of hot state, timers and dispatch before any follow-up commit, with execution home distinct from the successor run-hash shard; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_ordered_traversal` | 1 | Property 3: 100 generated ordered traversals, ties, multiple execution homes, read-only pages and mutations behind a cursor; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_sticky_recovery` | 1 | Property 7: 100 generated durable deadline, affinity-reset, paused, speculative, closed and legacy cases, with normal rediscovery after timeout; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_complete_repair` | 1 | Property 8: 100 generated two-walk repairs, interrupted head restarts, stale/orphan/sticky rows, NULL/false recovery flags and unchanged authoritative bytes; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_repair_decode_and_encoding_failures_preserve_authority` | 1 | Wrong-home, corrupt hot bytes and invalid sequence encoding abort repair without partial changes; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_fenced_repair_*` | 2 | Property 9: 100 generated repair interleavings plus ten fixed insertion/deletion controls; takeover and release in both commit orders, renewal, successor writes and terminal fence loss; `dsql-integration`, URL gate only |
| Runtime `workflow_dispatch_live_fenced_repair_final_gate` | 1 | Property 9: 100 generated empty-home final-gate interleavings with real lease changes and matching local cancellation, expiry or replacement; `dsql-integration`, URL gate only |
| Runtime `workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs` | 1 | Real lease acquisition, interrupted recovery and rejected old-generation installs in all recovery trackers; runtime `dsql-integration`, URL gate only; run after storage schema bootstrap |
| Storage `workflow_dispatch_live_query_plans` | 1 | Before/after queue Live/Exact and actual home page SQL, including full traversal of deep equal-time pages, with 16,384 seeded rows; set `TOKEIRA_WORKFLOW_DISPATCH_PLAN_OUTPUT` to save plans, selectivity, returned-row/page-statement counts and traversal durations; `dsql-integration`, URL gate only |
| Storage `placement_live_` | 4 | Stopped-cluster hot/timer relocation, concurrent starters and late-page exclusion, raw legacy-encoded lease refusal, corruption diagnostics, reset preservation, duplicates/orphans, exact joined continuation plans and screening latency; `dsql-integration`, URL gate only |
| Storage `dsql_bulk_` (`bulk_write_tests`) | 10 | The paged bulk writes: spills of more than 3,000 rows or 10 MiB, the deletion of a run owning more than 3,000 rows, resets whose copied history is over 1 MiB or 10 MiB or whose successor holds 3,500 timers, a reset of a closed workflow, a start between a reset's commit and its materialization, a purge resumed by two purges at once, and an abandoned materialization refusing its open transaction; `dsql-integration`, URL gate only |
| Storage `dsql_lease_fence_` (`lease_fence_tests`) | 11 | The bundle lease fence through Tokeira's repository: stale transition commits and a deletion held after their fence reads while another node takes the home over, each aborted and refused on its retry; a materialization stopped between its transactions and a recovery switch, both refused after a takeover; 100 generated lease histories in which only a change of ownership writes the epoch; a lost fence under each loss and write, changing nothing; the owner's renewals and refreshes beside its writes in flight; a write that commits first aborting a takeover; and 100 generated interleavings of fenced writes with renewals, refreshes, takeovers and releases; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_legacy_disposal` | 1 | Property 10: 100 interrupted disposal and concurrent-commit cases, preserving activity bytes and authoritative state; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_disposal_rolls_back_cancelled_page_and_keeps_bounds` | 1 | Cancellation before page commit leaves legacy rows intact; resumed pages inspect at most 64 keys; `dsql-integration`, URL gate only |
| Storage `dsql_transaction_limits` | 6 | DSQL's transaction limits, apart from the bulk writes' budgets: 3,000 rows, deleted rows included and rows read `FOR UPDATE` not; 9 MiB of values but not 10, deleted rows not counted; 1 MiB a value; `dsql-integration`, URL gate only |
| Storage `history_event_tests` | 4 | The one-lookup read of a history event: each event of batches of one and of several events and of a reset successor's copied batches; the lookup's plan on a run of 5,000 batches, for an event near the end and one near the start: a backward scan of the primary key with both key columns in its index condition, no sort, and the limit pushed down to storage, which returns one row; and dispatch rows and backlog entries an earlier release wrote with an activity's input; set `TOKEIRA_HISTORY_EVENT_PLAN_OUTPUT` to save the plans; `dsql-integration`, URL gate only |
| Storage `large_value_tests` | 14 | Values over the part size, stored in parts: a 2 MiB payload and a batch of nearly 4 MiB, read back by each read of history; a state that grows over the part size and shrinks below it, up to 7.5 MiB; a reset of a base whose history and state are over the part size; a deletion whose purge leaves no part; 8 generated sequences of state sizes, each write leaving exactly its state's parts; a purge held after each of its transactions, a history read held before it reads its parts while a purge runs, and a load held before it reads its parts while a commit replaces the state, each reading whole values; a row of two parts of the part size, which DSQL commits; CHASM nodes whose data grows to 6 MiB and shrinks, read back by each read of their execution; 8 generated sequences of a node's data sizes, each write leaving exactly its parts; an execution's deletion leaving no part; a load of a node held before it reads its parts while a persist replaces the data, reading one version whole; and worker invocation requests on each side of the part size, up to 2 MiB, read back by each read of their action and stored whole only within the part size; `dsql-integration`, URL gate only |
| Runtime `dsql_activity_inputs` | 3 | Activity inputs read from history: six pending activities with inputs of about 200 KB each, three activities scheduled in one batch, and a reset successor's activities delivered from its copied batches; runtime `dsql-integration`, URL gate only; run after storage schema bootstrap |

Run the suites and their test cases serially against the selected database. The shard
fixtures reuse a deterministic shard ID across nextest's separate test processes:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_shard_leasing --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_on_conflict_counts --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_embedded_ownership --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_lease_fence --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test dsql_projection_persistence --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql_store::tests::dsql_/)'
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql_store::passes_tests::/)'
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql_store::bounded_apply_tests::/)'
cargo nextest run -p tokeira-engine --features dsql-integration --locked --profile dsql-live --test live_embedded_attr_copy --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql::projection_log::tests::live::/)'
cargo nextest run -p tokeira-storage --features dsql --locked --profile dsql-live --test-threads 1 -E 'test(=dsql::chasm_node::tests::dsql_archetype_scoped_business_ids) | test(=dsql::chasm_node::tests::dsql_chasm_node_store_round_trips_and_fences) | test(=dsql::chasm_node::tests::dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes) | test(=dsql::chasm_node::tests::dsql_a_chasm_execution_commits_only_with_its_pointer) | test(=dsql::chasm_node::tests::dsql_a_backfill_returns_what_it_copied)'
cargo nextest run -p tokeira-storage --features dsql --locked --profile dsql-live --test-threads 1 -E 'test(=dsql::task_queue_config::tests::dsql_task_queue_policy_survives_repository_recreation) | test(=dsql::worker_deployment_repository::tests::dsql_worker_deployment_repository_cas_and_pagination_match_contract)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 --run-ignored only -E 'test(/^dsql::run_repository::projection_accumulator_tests::/)'
```

The accumulator tests run for about 45 minutes in all. The longest two take about 14
minutes each, inside the profile's twenty-minute limit.

A green result with the URL gates unset is not live evidence. Record the revision, suite,
and outcome after a credentialed run. Leave out the connection URL, the run's date and
times, and the Region.

The workflow-dispatch tests use only `TOKEIRA_DSQL_TEST_DATABASE_URL`, apply the
embedded migrations, and await ASYNC index readiness. Run them serially on an
ephemeral cluster. The generated transaction suite also runs for several minutes.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_) & !test(workflow_dispatch_live_generated_complete_repair) & !test(workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs)'
```

The disposal continuation probe `workflow_dispatch_live_disposal_bounds_activity_prefix_and_passes_corruption`
checks the SQL used by the running adapter. Set `TOKEIRA_WORKFLOW_DISPOSAL_PLAN_OUTPUT`
to retain its plan locally. It verifies a 64-key activity prefix, corrupt-row deferral,
and an indexed continuation bound without a filtered workflow-only table scan.

The placement suite uses the same gate and migrated database. Its screening
fixture inserts 12,288 hot rows and timers, explains the exact first/deep joined
page SQL, and measures the unchanged placement pass. Set
`TOKEIRA_PLACEMENT_REPORT` to a local output path to retain the plans and the
explicitly labelled extrapolation. Test-only marker resets require a disposable
database with every other writer stopped.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(placement_live_)'
```

The runtime acquisition check reuses the migrated database from the storage
preparation suites above. It must precede lease-taking suites. It adds no dependency;
its opt-in feature enables
the existing DSQL storage dependency. Run it serially in the same cluster lifecycle:

```bash
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --lib -E 'test(workflow_dispatch_live_acquisition)'
```

The bulk-write tests and the limits probe also use only `TOKEIRA_DSQL_TEST_DATABASE_URL`.
The bulk-write tests apply the embedded migrations; the probe creates and writes only its own
table, `dsql_limits_probe`. Run them serially on an ephemeral cluster:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(dsql_bulk_)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_transaction_limits --test-threads 1
```

The lease-fence tests also use only `TOKEIRA_DSQL_TEST_DATABASE_URL` and apply the embedded
migrations. They hold writes and acquisitions at test-only pause points, so each takeover
lands at a chosen moment. Each test takes the leases of the homes its runs live on and
clears them when it finishes. Run them serially:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql::run_repository::lease_fence_tests::/)'
```

The workflow repair fence suites run after preparation-sensitive suites, alongside
the lease-taking suites. Storage owns home 241 of 256, cleans its recovery candidates
and dispatch keys after each case, and clears its lease. Runtime owns empty home 242
of 256 and releases its leases after each case. Neither scans shard 0 or another
suite's home. Storage bootstraps schema before the runtime suite:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --lib --test-threads 1 -E 'test(workflow_dispatch_live_fenced_repair_)'
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --lib --test-threads 1 -E 'test(workflow_dispatch_live_fenced_repair_final_gate)'
```

The fixed storage controls keep every other writer off the repair's dispatch key.
Only the lease changes while repair is paused. Generated cases additionally permit
successor writes, and use execution homes different from run-hash mappings. A
negative control that removes the repair fence must preserve its write pause point;
otherwise it tests a broken barrier instead of the ownership invariant. The runtime
control invalidates the original acquisition immediately before its local comparison;
it does not add a separate database read before activation. URL-unset returns count
as skipped live coverage, even when the test harness reports success.

The history-event tests also use only `TOKEIRA_DSQL_TEST_DATABASE_URL` and apply the
embedded migrations. The runtime's activity-input tests reuse the migrated database from
a preceding storage test, as the acquisition check does. Run the activity-input
suite in the preparation-sensitive order above, before lease-taking suites. Both
commands run serially in the same cluster lifecycle:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql::run_repository::history_event_tests::/)'
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --test dsql_activity_inputs --test-threads 1
```

The large-value tests also use only `TOKEIRA_DSQL_TEST_DATABASE_URL` and apply the
embedded migrations. They hold purges, history reads and loads at test-only pause
points. Run them serially:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(/^dsql::(run_repository::)?large_value_tests::/)'
```

## Endpoint-gated IAM connector

Set both `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION` for an existing
cluster. `dsql_connector_iam` returns without connecting when either is unset. The standard
AWS credential chain must supply an identity authorized for the connector's `admin`
connection, as described in the [managed lifecycle prerequisites](managed-embedded-dsql-live-aws.md#prerequisites).

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_connector_iam
```

This test builds the production `ConnectionFactory`, creates an IAM-authenticated TLS
connection, and checks `SELECT 1`. It does not provision a cluster or change schema, and
it does not use either URL gate above. It proves the connector path that the URL-gated
suites bypass.

## Schema-bootstrap recovery

The ignored `dsql_schema_bootstrap` test requires both
`TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_DATABASE_URL` and
`TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_ACK=MUTATE_DISPOSABLE_EMPTY_DATABASE`. It has no
`DATABASE_URL` fallback. It refuses a database whose current schema already contains
relations, then seeds the interrupted bootstrap state and migrates through the embedded
target. It never resets or cleans the database; use a new disposable empty database for
each run, including after an interruption.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live \
  --test dsql_schema_bootstrap --run-ignored only \
  -E 'test(=token_zero_empty_ledger_state_converges_through_the_embedded_target)'
```

The same test binary holds the ignored V068 upgrade test, which needs a separate
database already at V068 and its own acknowledgement. The filter above leaves it out;
the [V068 upgrade regression](managed-embedded-dsql-live-aws.md#v068-upgrade-regression)
section of the managed runbook describes how to run it.

## Plain migration runner paths

The ignored `dsql_migration_runner` test requires
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_DATABASE_URL` and
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK=MIGRATE_DISPOSABLE_EMPTY_DATABASE`, and refuses a
database whose current schema already contains relations. It migrates the full embedded
corpus through `MigrationRunner::apply_connection`, which `tkr schema setup` uses, then
checks that `MigrationRunner::apply` finds nothing to apply. Set
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_FIRST=pool` to run the two paths the other way round.
Use a new empty database for each run. A full migration with its index builds takes
several minutes.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live \
  --test dsql_migration_runner --run-ignored only
```

## The activity state drop

The ignored `dsql_activity_state_drop` test requires
`TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_DATABASE_URL` and
`TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_ACK=MUTATE_DISPOSABLE_EMPTY_DATABASE`, and refuses a
database whose current schema already contains relations. It runs these steps:
1. Migrates the database to the target of the release before the drop, and writes 4,000
   `activity_state` rows in transactions of 1,000.
2. Migrates it with `MigrationRunner::apply_connection`, which `tkr schema setup` uses, and
   checks that no `activity_state` table or index remains and that a validate-only
   assessment finds the schema compatible.
3. Forgets the drop's ledger row and applies the drop again under the automatic policy,
   as the embedded engine does after a crash.
4. Rebuilds an earlier release's table, with rows and indexes, for the automatic policy to
   drop.

Use a new empty database for each run. The first migration and the rebuilt indexes take
several minutes.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live \
  --test dsql_activity_state_drop --run-ignored only
```

## Distributed startup schema check

The three ignored `distributed_schema_live_tests` in `tokeira-engine` start a
listener-backed distributed node, as `tokeirad` with `infrastructure.storage = "dsql"`
starts, against an existing cluster and the two DynamoDB tables that coordinate its
connections. Set `TOKEIRA_DSQL_DISTRIBUTED_ENDPOINT`, `TOKEIRA_DSQL_DISTRIBUTED_REGION`,
`TOKEIRA_DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` and `TOKEIRA_DSQL_DISTRIBUTED_CONN_LEASE_TABLE`;
each test fails when one is unset. Run these tests serially (`--test-threads 1`). Use a
separate fixture from placement suites that deliberately use a different home count.
Each table needs only a string partition key named `pk`. The standard AWS credential chain must supply an identity authorized for the
connector's `admin` connection and for both tables.

```bash
cargo nextest run -p tokeira-engine --features dsql-integration --lib --locked --profile dsql-live \
  --test-threads 1 --run-ignored only -E 'test(/distributed_schema_live_tests::/)'
```

Each test first applies the embedded migrations and forgets any ledger version past what
the release reads, so the tests run in any order and again on the same cluster. The first
run on a new cluster applies the whole corpus.
- One test forgets the target's ledger row, and one records a version past the maximum
  readable one. Each start must be refused, and must leave as it found them the ledger,
  the compatibility record, the shard leases, the connection-slot blocks and both
  listener addresses. Each test then restores the ledger.
- The third starts a node on the target schema and calls `GetSystemInfo`.

## Single-node claim

The ignored `single_node_claim_live_tests` test in `tokeira-engine` starts listener-backed
distributed nodes without a controller on one cluster, with the environment and the two
tables of the distributed schema check's tests. Run it after those tests, so that a new
cluster's first migration doesn't count against its time limit.

```bash
cargo nextest run -p tokeira-engine --features dsql-integration --lib --locked --profile dsql-live \
  --test-threads 1 --run-ignored only -E 'test(/single_node_claim_live_tests::/)'
```

The test removes the `tokeirad-owner` claim, then:
- starts node A, then node B beside it. B tries for 80 s and refuses, naming A's claim. It
  leaves both its listener addresses free, the ledger, the shard leases and A's claim as
  they were, and none of its connection-slot blocks owned;
- stops A cleanly, which releases the claim, and starts B at once;
- starts A again and crashes it without a release. B serves only once A's hold has
  expired and 5 minutes more have passed on the database's clock;
- gives the claim to another owner with SQL. B's next renewal is fenced, B's server
  drains, and B returns an error.

The crash step waits out a full quiescence, so the test takes about seven minutes. It
removes the claim when it ends.

## Managed lifecycle

The [managed embedded DSQL live-AWS runbook](managed-embedded-dsql-live-aws.md) covers the
separate ignored test that creates and destroys a billable cluster and starts the full
embedded engine. Follow its acknowledgement, credential, descriptor, and recovery rules.
The connector migration needs this lifecycle run as well as the endpoint test and every
URL-gated test. They run on a credentialed operator host; the managed runbook adds a
longer override for its lifecycle test to the `dsql-live` profile, because cluster
lifecycle operations take longer still.
