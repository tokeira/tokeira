# Managed embedded DSQL live-AWS test

This runbook exercises a real, billable single-Region Aurora DSQL cluster. The test is
ignored by default and is never part of CI. It creates a cluster, injects a failure after
AWS accepts `CreateCluster`, recovers with the durable client token, starts the complete
embedded engine, serves an in-process Temporal request, shuts down cleanly, and then
explicitly disables deletion protection and destroys the cluster.

Do not run it against an account or Region where creating and deleting a cluster is not
authorized. Aurora DSQL meters compute, reads, writes, and storage; see AWS's current
[billing description](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/billing-metering.html).

## Preserved upstream schema-bootstrap reproduction — 26 August 2026

This is an upstream Tokeira recovery defect, not an Odori integration defect. The live
cluster and its private managed descriptor are the reproduction fixture. Preserve both:
do not run this document's destructive lifecycle test against that cluster, reset its
schema, edit its control rows, replace its descriptor, or create a substitute cluster.

The following behavior is verified against Tokeira merge revision
`b9fc3f789a34f3c524b97223417044b1eecca71a`:

- embedded startup still fails during the schema-compatibility phase;
- AWS reports the cluster as `ACTIVE`, and the descriptor's canonical cluster ID and ARN
  identify that same cluster;
- `schema_version` exists and contains zero rows;
- `schema_compatibility` does not exist;
- `tokeira_control_lease` contains an expired `schema-migration` claim whose fence token
  is `0`; and
- repeated startup attempts do not advance that fence token.

The source-level cause is now identified. Tokeira first commits the idempotent claim-row
insert, which produces the observed ownerless, expired fence token `0`. It then opened a
transaction and issued a separate `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
before locking or updating that row. Aurora DSQL fixes transactions at repeatable-read
isolation and documents the explicit driver form as
[`BEGIN ISOLATION LEVEL REPEATABLE READ`](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/accessing.html);
its [supported transaction-control syntax](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-sql-features.html)
does not include the separate `SET TRANSACTION` statement. The rejected statement was
therefore reached after the seed insert but before the row lock and fence update, exactly
matching the persistent token-zero state. The embedded engine still reduces that inner
database failure to its bounded schema-phase error, so the original process output alone
does not reveal this cause.

The correction opens both connection-scoped and pool-scoped lease transactions with the
supported `BEGIN` form. The opt-in real-DSQL regression in
`crates/tokeira-storage/tests/dsql_schema_bootstrap.rs` now seeds and verifies this exact
empty-ledger, absent-compatibility, expired-token-zero state before exercising automatic
recovery. It remains fail-closed, ignored by default, and has only been compile-verified;
the preserved live fixture is the required end-to-end proof after the corrected revision
is built and its execution is separately authorized.

The required regression is automatic, non-destructive recovery from this exact partial
bootstrap state. Using the same descriptor and cluster, managed embedded startup must
acquire or recover the schema-migration claim, converge the migration ledger and
compatibility metadata to the release target, and start the engine without manual table
or row repair. Keep the fixture live until that regression has passed or the operator
separately authorizes its explicit descriptor-bound destruction.

## Verified AWS contract

The harness follows the current official Aurora DSQL APIs:

- [`CreateCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_CreateCluster.html)
  accepts a 1–128-character printable-ASCII idempotency token, can enable deletion
  protection, and returns the cluster identifier, ARN, endpoint, and status. Tokeira
  persists its explicit token before calling this API and never relies on the SDK's
  generated default.
- [`GetCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_GetCluster.html)
  retrieves a cluster by its identifier and returns the current ARN, endpoint, deletion
  protection, and status. The test recovers by identifier; tags and endpoints are never
  identity.
- [`UpdateCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_UpdateCluster.html)
  updates deletion protection by identifier and accepts an idempotency token.
- [`DeleteCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_DeleteCluster.html)
  deletes by identifier and accepts an idempotency token. AWS requires deletion
  protection to be disabled first.

AWS currently documents a default quota of 20 single-Region clusters per account and
Region, 10,000 connections per cluster, 100 new connections per second, and a 1,000
connection burst. The embedded defaults are intentionally far below those ceilings;
consult the current [Aurora DSQL quotas and database limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)
before running the test.

## Prerequisites

Use an AWS identity available through the standard SDK credential chain in the target
Region. It needs these control-plane actions for the cluster lifecycle:

```text
dsql:CreateCluster
dsql:GetCluster
dsql:UpdateCluster
dsql:DeleteCluster
dsql:TagResource
```

The harness supplies one metadata tag at creation, which is why `dsql:TagResource` is
needed. It never queries that tag. Scope the resource policy to the intended account and
Region according to your operator policy. Creation necessarily precedes knowledge of the
new cluster ID.

The same identity also needs `dsql:DbConnectAdmin` for the IAM-signed `admin` connection
used by schema setup and the embedded storage path. AWS distinguishes this from
`dsql:DbConnect`, which is for a custom database role; see
[Aurora DSQL authentication and authorization](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/authentication-authorization.html).
No static database password is used.

Choose a private, absolute descriptor path whose parent already exists. The descriptor
contains the Region, canonical cluster ID and ARN, endpoint, and creation client token.
Treat the file as sensitive operational state: do not commit it, paste it into logs, or
share it. On Unix, the store creates the descriptor with mode `0600`; protect its parent
directory as well.

## Run

From the repository root, set the Region and a fresh descriptor path, then run only the
ignored live test:

```bash
TOKEIRA_LIVE_MANAGED_DSQL_ACK=CREATE_AND_DELETE \
TOKEIRA_LIVE_DSQL_REGION=eu-west-2 \
TOKEIRA_LIVE_DSQL_DESCRIPTOR_PATH=/absolute/private/path/managed-dsql-live.json \
cargo test -p tokeira-engine --test live_managed_dsql --locked \
  --features dsql-integration \
  managed_embedded_dsql_live_lifecycle -- --ignored --exact
```

The acknowledgement value is intentionally exact. The test allows up to 30 minutes for
each lifecycle boundary because cluster creation, activation, and deletion are remote
control-plane operations. A successful run leaves a destroyed tombstone at the descriptor
path; ordinary startup will not recreate from that tombstone. Choose a new descriptor path
for a subsequent new-cluster run.

## Recovery and cleanup

The preserved reproduction fixture above is an explicit exception to the destructive
cleanup procedure in this section. Do not disable its deletion protection or delete it
unless the operator separately retires the fixture after the recovery regression.

If the command stops before destruction completes, first rerun the exact same command with
the same Region and descriptor path. Never substitute an endpoint or search by tag. A
pending descriptor causes Tokeira to replay `CreateCluster` with the same durable client
token; a ready descriptor recovers the same canonical cluster ID and ARN. Deletion
protection remains enabled through ordinary engine startup and shutdown.

If rerunning cannot complete, use the Region and cluster ID in the protected descriptor to
inspect the resource with `GetCluster`. Then perform the same explicit administrative
sequence through an authorized AWS console or SDK client:

1. Confirm that the returned ARN matches the descriptor ARN exactly.
2. Disable deletion protection with `UpdateCluster` using a fresh explicit client token.
3. Delete that cluster ID with `DeleteCluster` using another fresh explicit client token.
4. Poll `GetCluster` until the cluster is `DELETED` or not found.

Do not delete the local descriptor merely to make startup create another cluster. If AWS
already deleted the cluster but the process stopped before writing the destroyed tombstone,
retain the descriptor as recovery evidence and use a new protected path for later tests.

The live test deliberately performs no `tkr` or `tkp` operation. Cluster lifecycle
authority remains an explicit library-level test boundary, separate from the embedded
engine's normal drop and shutdown paths.
