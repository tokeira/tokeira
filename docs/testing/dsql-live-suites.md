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

## URL-gated storage and projection

Set `TOKEIRA_DSQL_TEST_DATABASE_URL` to a connection URL for a disposable test database.
The four integration suites fall back to `DATABASE_URL` when that variable is unset
and return without connecting when neither is set. The five CHASM tests, the task-queue
policy and worker-deployment tests, and the projection's visibility-row test use only
`TOKEIRA_DSQL_TEST_DATABASE_URL` and return without connecting when it is unset. The
seven projection-accumulator tests are ignored by default, use only that variable, and
fail when it is unset.
These suites connect through SQLx directly rather than the IAM connector. The database must permit schema setup and test-data writes; do not
point them at a database holding application data.

| Suite | Tests | Coverage |
|---|---:|---|
| Storage `dsql_shard_leasing` | 5 | Lease ownership and fencing, and every lease state an acquire can meet |
| Storage `dsql_on_conflict_counts` | 6 | The row count DSQL returns for an `ON CONFLICT` statement that leaves its row unwritten, and the answers of the slot, action, provenance, configuration and backlog operations built on one |
| Storage `dsql_embedded_ownership` | 1 | Embedded ownership lifecycle |
| Projection `dsql_projection_persistence` | 6 | Visibility persistence and queries |
| Storage `dsql_archetype_scoped_business_ids` | 1 | CHASM Property 8: scoped pointers and backfill; `--features dsql`, URL gate only |
| Storage `dsql_chasm_node_store_round_trips_and_fences` | 1 | CHASM nodes, atomic pointers and fencing; `--features dsql`, URL gate only |
| Storage `dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes` | 1 | Concurrent CHASM creates and superseding pointers reject stale admission without orphaned nodes; `--features dsql`, URL gate only |
| Storage `dsql_a_chasm_execution_commits_only_with_its_pointer` | 1 | A new execution's nodes commit only when the current-run pointer held what the start expected; `--features dsql`, URL gate only |
| Storage `dsql_a_backfill_returns_what_it_copied` | 1 | Each CHASM backfill call returns the pointers it copied; `--features dsql`, URL gate only |
| Projection `dsql_a_visibility_row_applies_only_a_newer_version` | 1 | A visibility row is replaced only by a newer version; `--features dsql-integration`, URL gate only |
| Storage `dsql_task_queue_policy_survives_repository_recreation` | 1 | A task queue's stored policy survives recreating the repository; `--features dsql`, URL gate only |
| Storage `dsql_worker_deployment_repository_cas_and_pagination_match_contract` | 1 | Worker-deployment compare-and-set and pagination follow the repository contract; `--features dsql`, URL gate only |
| Storage `dsql_projection_accumulator_*` | 7 | Projection-accumulator contracts: commit images, legacy seeds, failure isolation, growth accounting, reset boundaries, mixed writers and pruning, and snapshot races; `dsql-integration`, ignored by default |
| Storage `workflow_dispatch_live_atomic_reference_traces` | 1 | 100 generated atomic state/dispatch cases, reset materialization, duplicate/CAS rejection, rollback, and deletion; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_ordered_pages` | 1 | Normal/Exact paging, home scans including sticky rows, reset boundaries, speculative legacy delivery, and forced digest collisions; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_reset_uses_execution_home` | 1 | Eight-shard reset placement of hot state, timers and dispatch before any follow-up commit, with execution home distinct from the successor run-hash shard; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_ordered_traversal` | 1 | Property 3: 100 generated ordered traversals, ties, multiple execution homes, read-only pages and mutations behind a cursor; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_sticky_recovery` | 1 | Property 7: 100 generated durable deadline, affinity-reset, paused, speculative, closed and legacy cases, with normal rediscovery after timeout; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_complete_repair` | 1 | Property 8: 100 generated two-walk repairs, interrupted head restarts, stale/orphan/sticky rows, NULL/false recovery flags and unchanged authoritative bytes; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_repair_decode_and_encoding_failures_preserve_authority` | 1 | Wrong-home, corrupt hot bytes and invalid sequence encoding abort repair without partial changes; `dsql-integration`, URL gate only |
| Runtime `workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs` | 1 | Real lease acquisition, interrupted recovery and rejected old-generation installs in all recovery trackers; runtime `dsql-integration`, URL gate only; run after storage schema bootstrap |
| Storage `workflow_dispatch_live_query_plans` | 1 | Before/after queue Live/Exact and actual home page SQL, including full traversal of deep equal-time pages, with 16,384 seeded rows; set `TOKEIRA_WORKFLOW_DISPATCH_PLAN_OUTPUT` to save plans and selectivity; `dsql-integration`, URL gate only |

Run the suites and their test cases serially against the selected database. The shard
fixtures reuse a deterministic shard ID across nextest's separate test processes:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_shard_leasing --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_on_conflict_counts --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test dsql_embedded_ownership --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test dsql_projection_persistence --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(=dsql_store::tests::dsql_a_visibility_row_applies_only_a_newer_version)'
cargo nextest run -p tokeira-storage --features dsql --locked --profile dsql-live --test-threads 1 -E 'test(=dsql::chasm_node::tests::dsql_archetype_scoped_business_ids) | test(=dsql::chasm_node::tests::dsql_chasm_node_store_round_trips_and_fences) | test(=dsql::chasm_node::tests::dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes) | test(=dsql::chasm_node::tests::dsql_a_chasm_execution_commits_only_with_its_pointer) | test(=dsql::chasm_node::tests::dsql_a_backfill_returns_what_it_copied)'
cargo nextest run -p tokeira-storage --features dsql --locked --profile dsql-live --test-threads 1 -E 'test(=dsql::task_queue_config::tests::dsql_task_queue_policy_survives_repository_recreation) | test(=dsql::worker_deployment_repository::tests::dsql_worker_deployment_repository_cas_and_pagination_match_contract)'
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 --run-ignored only -E 'test(/^dsql::run_repository::projection_accumulator_tests::/)'
```

The accumulator tests run for about 45 minutes in all. The longest two take about 14
minutes each, inside the profile's twenty-minute limit.

A green result with the URL gates unset is not live evidence. Record the date, revision,
suite, and outcome after a credentialed run, without recording the connection URL.

The workflow-dispatch tests use only `TOKEIRA_DSQL_TEST_DATABASE_URL`, apply the
embedded migrations, and await ASYNC index readiness. Run them serially on an
ephemeral cluster. The generated transaction suite also runs for several minutes.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --profile dsql-live --test-threads 1 -E 'test(workflow_dispatch_live_)'
```

The runtime acquisition check reuses the migrated database from a preceding
storage workflow-dispatch test. It adds no dependency; its opt-in feature enables
the existing DSQL storage dependency. Run it serially in the same cluster lifecycle:

```bash
cargo nextest run -p tokeira-runtime --features dsql-integration --locked --profile dsql-live --lib -E 'test(workflow_dispatch_live_acquisition)'
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

## Managed lifecycle

The [managed embedded DSQL live-AWS runbook](managed-embedded-dsql-live-aws.md) covers the
separate ignored test that creates and destroys a billable cluster and starts the full
embedded engine. Follow its acknowledgement, credential, descriptor, and recovery rules.
The connector migration needs this lifecycle run as well as the endpoint test and every
URL-gated test. They run on a credentialed operator host; the managed runbook adds a
longer override for its lifecycle test to the `dsql-live` profile, because cluster
lifecycle operations take longer still.
