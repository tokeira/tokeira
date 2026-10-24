# Bugfix Requirements Document

## Introduction

A listener-backed distributed node without a placement controller, `tokeirad` with `infrastructure.storage = "dsql"`
and no `infrastructure.placement.controller_endpoint`, assigns itself every execution home when it starts
(`self_assign_dsql_shards` in `crates/tokeira-engine/src/lib.rs`):
- It first relinquishes every shard lease it finds, whoever holds it. Its comment gives the reason: there is no other
  node, so any existing lease is stale.
- It then acquires every lease, sweeps each home, and serves. It never renews a lease, and it writes unfenced: its
  commits pass no epoch and read no `shard_lease` row ([bundle-lease-fence](../bundle-lease-fence/bugfix.md) criterion
  3.1; [dsql-performance-fixes](../dsql-performance-fixes/requirements.md) Requirements 1 and 2).

Nothing checks that there is no other node. A second such node on the same cluster relinquishes the first node's
leases and acquires them, and then both nodes serve and write the same runs unfenced. Each also sweeps every home and
republishes the work it finds, so both can offer the same workflow task. The schema check doesn't stop it
([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)): both nodes run on a schema they
can run on.

Upgrades depend on there being one node. A release upgrades with every node of the old release stopped before the new
one starts ("Upgrade with every node stopped.", `CHANGELOG.md`), and the schema check keeps a node of a release that
has it off a schema it can't read when the node starts. A node that is already running is stopped by nothing, and no
lease records the release of the node that holds it.

The managed embedded engine already guards its cluster this way. Before it assigns itself every home, it takes the
`embedded-owner` claim in `tokeira_control_lease` (V067): a row held on the database's clock for 60 s and renewed every
20 s, which a second engine finds busy ([managed-embedded-dsql](../managed-embedded-dsql/design.md) §6 and §7;
`acquire_embedded_ownership` in `crates/tokeira-engine/src/lib.rs`; `crates/tokeira-storage/src/dsql/control_lease.rs`).
A node without a controller takes no claim.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a listener-backed distributed node without a controller starts on a DSQL cluster where another such node is
serving THEN it relinquishes the other node's shard leases, acquires them, sweeps every home and serves, whatever
either node's release. Both nodes then write the same runs unfenced.

1.2 WHEN such a node stops, cleanly or not, THEN it leaves no record that it stopped, so a node that starts later can't
tell a stopped node from a running one.

### Expected Behavior (Correct)

2.1 WHEN a listener-backed distributed node without a controller starts on DSQL THEN, after its schema check passes and
before it builds the runtime, assigns itself any home or binds a listener, it SHALL acquire the cluster's single-node
claim: the `tokeirad-owner` row of `tokeira_control_lease`, held on the database's clock for 60 s.

2.2 WHEN the claim is held by another owner whose hold hasn't expired THEN the node SHALL try again every 5 s for up to
80 s, one hold and one renewal interval. WHEN the claim is still held after that THEN the node SHALL refuse to start,
with an error that names the holder and the hold's expiry and says to stop that node first.

2.3 WHEN the node refuses to start THEN it SHALL leave nothing started, as distributed-schema-startup-check criterion
2.5 lists: no listener bound, no runtime or background work, no shard lease taken, no change to any table, its DSQL
connections closed and its DynamoDB connection-slot blocks released.

2.4 WHEN the node acquires a claim whose previous owner never released it THEN it SHALL renew the claim and wait
5 minutes, DSQL's longest transaction, before it assigns itself any home. WHEN the claim had no owner, or its owner
released it, THEN the node SHALL go on at once.

2.5 WHILE the node holds the claim, it SHALL renew it every 20 s.

2.6 WHEN a renewal finds that another owner holds the claim, or no renewal is confirmed within 40 s of the last
confirmed one, THEN the node SHALL start no more DSQL work, other than the claim's own, and SHALL drain and exit with
an error.

2.7 WHEN the node shuts down cleanly THEN, once it has stopped serving and cancelled its background work, it SHALL
start no more DSQL work, wait up to 30 s for the work already started, and then release the claim. IF that work hasn't
finished by then THEN the node SHALL leave the claim to expire.

2.8 IF the node fails to start after it acquired the claim THEN it SHALL release the claim as 2.7 says, unless it is
still in 2.4's wait, in which case it SHALL leave the claim to expire.

### Unchanged Behavior (Regression Prevention)

3.1 A node with `infrastructure.placement.controller_endpoint` configured SHALL CONTINUE TO start without the claim,
and to take its leases as the controller directs.

3.2 The embedded engine SHALL CONTINUE TO take the `embedded-owner` claim as managed-embedded-dsql specifies.

3.3 A listener-backed node with in-memory storage SHALL CONTINUE TO start without a claim.

3.4 A node that holds the claim SHALL CONTINUE TO check the schema first, assign itself every home, sweep each one and
write unfenced, as distributed-schema-startup-check, dsql-performance-fixes Requirements 1 and 2 and bundle-lease-fence
criterion 3.1 specify.

3.5 This change SHALL add no migration and change no version in the schema contract: V067 created
`tokeira_control_lease`.

### Out of Scope

- **Nodes of releases before this change.** They take no claim, so a node of this release can't see one. The release
  note's "Upgrade with every node stopped." stays the only guard against them.
- **Nodes with a controller.** Their peers renew leases every second, so a refusal based on live leases would refuse
  every node that joins. Multi-node placement keeps its own leases and failover.
- **An embedded engine and a `tokeirad` on one cluster.** They take different claims, so neither sees the other. The
  embedded engine's claim records its cluster's ARN, which `tokeirad` doesn't know.
- **The embedded engine's expired takeover.** After it takes over an expired claim, the engine waits out the same
  5 minutes without renewing, so its claim expires during the wait and its first renewal fails. That is a fix of its
  own.
