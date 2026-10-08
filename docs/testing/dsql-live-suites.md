# DSQL live suites

These suites require an operator-selected Aurora DSQL database and connect only when
their environment gates are set. Most require `dsql-integration`; the CHASM unit tests
listed below require `--features dsql` and also compile in the default suite. Live
execution is outside the default validation and CI. Run from the repository
root with `--locked`; keep endpoints, URLs, credentials, and resource identities in the
operator environment rather than in committed files or shared output.

## URL-gated storage and projection

Set `TOKEIRA_DSQL_TEST_DATABASE_URL` to a connection URL for a disposable test database.
The four integration suites fall back to `DATABASE_URL` when that variable is unset
and return without connecting when neither is set. The five CHASM tests and the
projection's visibility-row test use only `TOKEIRA_DSQL_TEST_DATABASE_URL` and return
without connecting when it is unset.
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
| Storage `workflow_dispatch_live_atomic_reference_traces` | 1 | 100 generated atomic state/dispatch cases, reset materialization, duplicate/CAS rejection, rollback, and deletion; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_ordered_pages` | 1 | Normal/Exact paging, home scans including sticky rows, reset boundaries, speculative legacy delivery, and forced digest collisions; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_reset_uses_execution_home` | 1 | Eight-shard reset placement of hot state, timers and dispatch before any follow-up commit, with execution home distinct from the successor run-hash shard; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_ordered_traversal` | 1 | Property 3: 100 generated ordered traversals, ties, multiple execution homes, read-only pages and mutations behind a cursor; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_generated_sticky_recovery` | 1 | Property 7: 100 generated durable deadline, affinity-reset, paused, speculative, closed and legacy cases, with normal rediscovery after timeout; `dsql-integration`, URL gate only |
| Storage `workflow_dispatch_live_query_plans` | 1 | Actual queue Live/Exact and home page SQL, first and continuation pages, with 8,192 seeded rows; set `TOKEIRA_WORKFLOW_DISPATCH_PLAN_OUTPUT` to save plans and selectivity; `dsql-integration`, URL gate only |

Run the suites and their test cases serially against the selected database. The shard
fixtures reuse a deterministic shard ID across nextest's separate test processes:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_shard_leasing --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_on_conflict_counts --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_embedded_ownership --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --test dsql_projection_persistence --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --test-threads 1 -E 'test(=dsql_store::tests::dsql_a_visibility_row_applies_only_a_newer_version)'
cargo nextest run -p tokeira-storage --features dsql --locked --test-threads 1 -E 'test(=dsql::chasm_node::tests::dsql_archetype_scoped_business_ids) | test(=dsql::chasm_node::tests::dsql_chasm_node_store_round_trips_and_fences) | test(=dsql::chasm_node::tests::dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes) | test(=dsql::chasm_node::tests::dsql_a_chasm_execution_commits_only_with_its_pointer) | test(=dsql::chasm_node::tests::dsql_a_backfill_returns_what_it_copied)'
```

A green result with the URL gates unset is not live evidence. Record the date, revision,
suite, and outcome after a credentialed run, without recording the connection URL.

The workflow-dispatch tests use only `TOKEIRA_DSQL_TEST_DATABASE_URL`, apply the
embedded migrations, and await ASYNC index readiness. Run them serially on an
ephemeral cluster. The generated transaction suite can exceed nextest's default
three-minute timeout; use a temporary profile with a longer timeout, as in the
[managed runbook](managed-embedded-dsql-live-aws.md), without changing the repository profile.

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test-threads 1 -E 'test(workflow_dispatch_live_)'
```

## Endpoint-gated IAM connector

Set both `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION` for an existing
cluster. `dsql_connector_iam` returns without connecting when either is unset. The standard
AWS credential chain must supply an identity authorized for the connector's `admin`
connection, as described in the [managed lifecycle prerequisites](managed-embedded-dsql-live-aws.md#prerequisites).

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_connector_iam
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
cargo nextest run -p tokeira-storage --features dsql-integration --locked \
  --test dsql_schema_bootstrap --run-ignored only
```

## Plain migration runner paths

The ignored `dsql_migration_runner` test requires
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_DATABASE_URL` and
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK=MIGRATE_DISPOSABLE_EMPTY_DATABASE`, and refuses a
database whose current schema already contains relations. It migrates the full embedded
corpus through `MigrationRunner::apply_connection`, which `tkr schema setup` uses, then
checks that `MigrationRunner::apply` finds nothing to apply. Set
`TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_FIRST=pool` to run the two paths the other way round.
Use a new empty database for each run. A full migration with its index builds takes
several minutes, longer than nextest's default three-minute ceiling, so run it with
`cargo test`:

```bash
cargo test -p tokeira-storage --features dsql-integration --locked \
  --test dsql_migration_runner -- --ignored
```

## Managed lifecycle

The [managed embedded DSQL live-AWS runbook](managed-embedded-dsql-live-aws.md) covers the
separate ignored test that creates and destroys a billable cluster and starts the full
embedded engine. Follow its acknowledgement, credential, descriptor, and recovery rules.
The connector migration needs this lifecycle run as well as the endpoint test and every
URL-gated test. They run on a credentialed operator host; the managed runbook supplies a
temporary nextest profile because the default three-minute timeout is too short for
cluster lifecycle operations.
