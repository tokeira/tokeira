# DSQL live suites

These suites require an operator-selected Aurora DSQL database and run only with
`dsql-integration`. They are outside the default test suite and CI. Run from the repository
root with `--locked`; keep endpoints, URLs, credentials, and resource identities in the
operator environment rather than in committed files or shared output.

## URL-gated storage and projection

Set `TOKEIRA_DSQL_TEST_DATABASE_URL` to a connection URL for a disposable test database.
Each suite falls back to `DATABASE_URL` when that variable is unset and returns without
connecting when neither is set. These suites connect through SQLx directly rather than
the IAM connector. The database must permit schema setup and test-data writes; do not
point them at a database holding application data.

| Suite | Tests | Coverage |
|---|---:|---|
| Storage `dsql_shard_leasing` | 4 | Lease ownership and fencing |
| Storage `dsql_embedded_ownership` | 1 | Embedded ownership lifecycle |
| Projection `dsql_projection_persistence` | 5 | Visibility persistence and queries |

Run the suites and their test cases serially against the selected database. The shard
fixtures reuse a deterministic shard ID across nextest's separate test processes:

```bash
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_shard_leasing --test-threads 1
cargo nextest run -p tokeira-storage --features dsql-integration --locked --test dsql_embedded_ownership --test-threads 1
cargo nextest run -p tokeira-projection --features dsql-integration --locked --test dsql_projection_persistence --test-threads 1
```

A green result with the URL gates unset is not live evidence. Record the date, revision,
suite, and outcome after a credentialed run, without recording the connection URL.

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

## Managed lifecycle

The [managed embedded DSQL live-AWS runbook](managed-embedded-dsql-live-aws.md) covers the
separate ignored test that creates and destroys a billable cluster and starts the full
embedded engine. Follow its acknowledgement, credential, descriptor, and recovery rules.
The connector migration needs this lifecycle run as well as the endpoint test and all ten
URL-gated tests. They run on a credentialed operator host; the managed runbook supplies a
temporary nextest profile because the default three-minute timeout is too short for
cluster lifecycle operations.
