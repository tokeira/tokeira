# Managed embedded DSQL live-AWS test

This runbook exercises a real, billable single-Region Aurora DSQL cluster. The test is
ignored by default and is never part of CI. It creates a cluster, injects a failure after
AWS accepts `CreateCluster`, recovers with the durable client token, starts the complete
embedded engine, serves an in-process Temporal request, shuts down cleanly, and then
explicitly disables deletion protection and destroys the cluster.

Do not run it against an account or Region where creating and deleting a cluster is not
authorized. Aurora DSQL meters compute, reads, writes, and storage; see AWS's current
[billing description](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/billing-metering.html).

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
