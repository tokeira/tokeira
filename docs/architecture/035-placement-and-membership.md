# 035 Placement and Membership

**Status:** revised draft for architecture review  
**Related docs:** `000-overview.md`, `030-runtime-lanes.md`, `045-autoscaling-on-ecs-ec2.md`, `050-dsql-storage.md`, `055-admission-control.md`, `090-failover-and-recovery.md`

## Intent

This note defines how **Tokeira** should do:

- runtime node membership,
- shard placement,
- authoritative ownership fencing,
- edge routing,
- scale-out under high workflow volume,
- stale-route recovery via `NotShardOwner`, and
- failover without putting an external coordination store on the request path.

The revision in this note is deliberate:

> **DynamoDB is not a placement control plane for Tokeira.**  
> **Placement is controller-driven, routing is cache-based, and ownership is fenced in DSQL.**

DynamoDB remains acceptable for **DSQL connection-budget management**, but it should not sit in the workflow request path, and it should not be the authoritative source of shard placement.

## Why revise the earlier direction

Temporal's current server design uses Ringpop-backed membership and fixed History Shards; each History Shard owns workflow mutable state and internal task queues, which makes shard ownership, membership, and internal queue processing tightly coupled. Temporal also states that the total number of History Shards is fixed at cluster creation. [^temporal-server] [^temporal-history]

That is coherent for Temporal, but it is not the design Tokeira wants.

The earlier version of this note used DynamoDB for node heartbeats and advisory shard-owner state. That is still workable as a **cold-path** controller mechanism, but it is not the best fit for the architecture we now want:

- we do not want an extra database in the placement hot path,
- we do not want per-heartbeat or per-rebalance request volume to dominate cost,
- and we do not want authoritative liveness semantics to rely on DynamoDB TTL, which AWS documents as asynchronous and intended for background expiry rather than prompt coordination. [^ddb-ttl]

DynamoDB on-demand is also explicitly pay-per-request, which is fine for narrow control loops but exactly the wrong cost shape if placement becomes chatty. [^ddb-on-demand]

## Core principles

### 1. No external coordination store on the request path

The hot path for a workflow mutation or worker poll should be:

1. Edge hashes to a virtual routing shard.
2. Edge consults its **local cached routing map**.
3. Request goes to the selected runtime node.
4. Runtime routes to the owning lane and run actor.
5. Run actor commits through DSQL.

There should be **no DynamoDB read**, **no controller RPC**, and **no membership lookup** on the hot path.

### 2. Membership is advisory; DSQL fencing is authoritative

Aurora DSQL uses optimistic concurrency control and resolves conflicting writes at commit time, with conflicting transactions surfacing serialization errors that must be retried. [^dsql-occ] [^dsql-working-with]

That makes DSQL the right place to hold the authoritative ownership fence for placement. Membership and placement can be stale for a short time as long as stale owners cannot commit.

### 3. Separate routing granularity from lease granularity

Tokeira should use:

- a **large virtual routing shard space** for even distribution and cheap rebalance, and
- a **smaller lease-bundle space** for authoritative ownership.

That avoids the trap of renewing or moving tens of thousands of authoritative leases individually.

### 4. The workflow run remains the unit of correctness

As elsewhere in Tokeira, **run ordering** is the correctness boundary. Shards and bundles are placement devices, not execution subsystems.

## Revised model

## Placement roles

### Edge

The edge:

- hashes `(namespace_id, workflow_id)` to a **virtual routing shard**,
- maps that routing shard to a **lease bundle**,
- uses a cached `bundle -> owner` map,
- forwards the request to the selected runtime,
- refreshes its cache on `NotShardOwner` or bundle-generation change.

### Runtime node

A runtime node:

- participates in placement through a long-lived control stream,
- owns zero or more lease bundles,
- hosts lanes and run actors,
- renews bundle leases in DSQL,
- runs sweepers after bundle acquisition or takeover.

### Placement controller

A placement controller:

- receives runtime heartbeats and demand over internal gRPC,
- computes desired bundle placement,
- publishes routing snapshots and incremental changes,
- never becomes the correctness authority for ownership.

This controller should run as a small internal ECS service. ECS Service Connect is a good fit for the controller/runtime and edge/controller traffic because it gives ECS services stable service endpoints and connectivity without introducing a separate gossip substrate. [^ecs-sc] [^ecs-sc-concepts]

### DSQL lease authority

DSQL is the only authoritative ownership plane. A runtime owns a bundle only if the DSQL lease row says it does.

## Membership without DynamoDB

Instead of storing membership in DynamoDB, use **controller-managed live membership**.

### Proposed mechanism

- Each runtime opens a long-lived gRPC stream to the controller over Service Connect.
- The stream carries:
  - node identity,
  - version/build info,
  - shard pressure,
  - lane pressure,
  - connection-budget headroom,
  - drain state.
- The controller keeps live node state in memory.
- If the stream drops, the node is considered unavailable for new placement after a grace interval.

This makes membership cheap and immediate without turning it into a database workload.

### Why this is acceptable

Membership is **not authoritative**. If the controller loses track of a node prematurely, correctness is still protected because only the DSQL lease holder can commit.

## Controller availability model

Run the controller as a small **REPLICA** ECS service. ECS services are built to maintain the requested task count, and Service Connect can give the controller a stable internal endpoint. [^ecs-services] [^ecs-sc]

Use a small DSQL control lease for leader election:

```sql
CREATE TABLE control.controller_lease (
  lease_name      text PRIMARY KEY,
  owner_node_id   uuid NOT NULL,
  epoch           bigint NOT NULL,
  lease_until     timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL
);
```

Only the leader computes or publishes new placement. Followers are hot standbys.

If the controller fleet is temporarily unavailable:

- existing runtime owners continue renewing bundle leases directly in DSQL,
- edges continue using cached bundle maps,
- correctness is unaffected,
- rebalance and new-placement decisions pause until a leader returns.

That is an acceptable failure mode.

## Routing shards and lease bundles

### Routing shards

Use a large fixed routing-shard space, for example:

- `16K`,
- `64K`, or
- `128K` virtual routing shards.

These are used only for hashing and rebalance granularity.

### Lease bundles

Group routing shards into a much smaller number of authoritative lease bundles.

Example shape:

- `65,536` routing shards,
- `1,024` lease bundles,
- `64` routing shards per bundle.

This is an architectural example, not a required constant.

### Why bundles matter

This is the main correction to the earlier schema shape.

If authoritative ownership were per routing shard, the system would create unnecessary write volume for lease renewals and rebalances. Bundling keeps the routing space fine-grained while keeping the authoritative lease set small.

## DSQL authoritative lease table

```sql
CREATE TABLE control.bundle_lease (
  bundle_id        integer PRIMARY KEY,
  owner_node_id    uuid NOT NULL,
  epoch            bigint NOT NULL,
  lease_until      timestamptz NOT NULL,
  updated_at       timestamptz NOT NULL
);
```

### Semantics

- `bundle_id` is the authoritative ownership unit.
- `epoch` is the fencing generation.
- `lease_until` is the expiry time.
- `owner_node_id` is the runtime currently entitled to commit for that bundle.

### Rules

- owner changes bump `epoch`,
- ordinary renewals keep the same `epoch`,
- every mutating workflow commit validates the expected `bundle_id` and `epoch`,
- stale owners fail closed.

## Commit fencing

A run commit should validate the bundle fence as part of the same DSQL transaction that updates workflow state.

Conceptually:

```sql
-- pseudocode shape
SELECT epoch, owner_node_id, lease_until
FROM control.bundle_lease
WHERE bundle_id = $bundle_id;

-- verify owner == self, epoch == expected, lease_until > now()
-- then commit workflow transition
```

The important point is that the authoritative fence is in the same storage system that commits workflow transitions.

## Routing state distribution

Edges and runtimes should not query DSQL for placement on each request.

Instead, the controller publishes a compact routing snapshot:

- `routing_generation`,
- `bundle_id -> owner_runtime_addr`,
- optional health metadata.

### Distribution options

1. **Controller watch stream** to edges and runtimes.  
   Best default.

2. **Periodic pull** from controller.  
   Simpler fallback.

3. **Cache refresh on miss** when a runtime responds with `NotShardOwner`.

The edge only needs a cached bundle map and a generation number.

## Hot path after this revision

The hot path is now:

- **no DynamoDB**,
- **no controller round trip**,
- **no placement DB lookup**,
- **one cached route decision at edge**,
- **one authoritative DSQL fenced commit at runtime**.

That is the shape we want.

## Request-volume and cost implications

This design deliberately moves request volume away from DynamoDB-style control loops.

### What disappears

- no per-edge request reads against a placement table,
- no per-runtime heartbeat writes to DynamoDB,
- no per-rebalance fanout writes to a membership store,
- no TTL-based liveness cleanup loop.

### What remains

- steady but bounded controller/runtime stream traffic,
- DSQL bundle-lease renewals,
- occasional routing-snapshot publication,
- `NotShardOwner` retries during rebalance or failure.

The result is that placement cost is dominated by **internal service traffic** and **DSQL lease maintenance**, not by a chatty external key-value store.

## Scale-out and rebalance

### Normal scale-out

1. Add runtime hosts.
2. New runtimes join the controller stream.
3. Controller computes a revised bundle placement.
4. New runtimes acquire selected bundle leases in DSQL.
5. Controller increments `routing_generation` and publishes the new map.
6. Edges gradually refresh caches or learn via `NotShardOwner`.

### Failure

1. A runtime disappears.
2. Controller marks it unavailable for new placement.
3. Its bundle leases expire or are taken over.
4. New owners acquire bundles with higher epochs.
5. Sweepers rebuild dispatchable state for those bundles.
6. Edges reroute after cache refresh or `NotShardOwner`.

### Why stale routing is safe

Because correctness is fenced in DSQL, stale edge routing is a latency problem, not a correctness problem.

## ECS fit

This placement model fits ECS on EC2 well.

- Runtime can be a **DAEMON** service, which ECS documents as exactly one task per eligible active container instance. [^ecs-daemon]
- Controller can be a small **REPLICA** service. [^ecs-service-options]
- Service Connect can provide stable in-cluster endpoints for controller, runtime, edge, and projection services. [^ecs-sc] [^ecs-sc-concepts]

That means we can stay within the AWS primitives we already want to use, without adding DynamoDB to the placement loop.

## Where DynamoDB still belongs

DynamoDB still makes sense for **DSQL connection-budget control**, because that loop benefits from being outside the failure domain of DSQL itself. DynamoDB on-demand is explicitly a pay-per-request service, which is acceptable for a narrow and low-cardinality control plane. [^ddb-on-demand]

It does **not** need to be the placement store.

## Recommended design summary

For Tokeira placement and membership:

- **No DynamoDB on the placement path.**
- **Membership via controller-managed live streams.**
- **Authoritative ownership via DSQL bundle leases.**
- **Large routing-shard space, smaller lease-bundle space.**
- **Edge routing via cached bundle map.**
- **Controller for advisory placement, not correctness.**
- **`NotShardOwner` for stale-route recovery.**

## Review questions

1. What routing-shard count and bundle count give the right rebalance granularity for the first production target?
2. Should routing updates be purely push, purely pull, or hybrid push-plus-miss-refresh?
3. Do we want the controller leader lease in DSQL, or do we prefer a single externally managed leader process in the earliest phase?
4. What grace interval should controller membership use before declaring a runtime unavailable for new placement?
5. Do we want to keep a durable routing snapshot in DSQL for controller cold start, or reconstruct fully from the current bundle leases?

---

[^temporal-server]: Temporal Server docs describe Ringpop-backed service membership, fixed History Shards, and History Shards as the place where workflow state and internal task queues live. See: https://docs.temporal.io/temporal-service/temporal-server
[^temporal-history]: Temporal's History Service architecture doc describes fixed history shard count and Ringpop-coordinated shard ownership. See: https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md
[^ddb-on-demand]: DynamoDB on-demand capacity is explicitly pay-per-request. See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html
[^ddb-ttl]: DynamoDB TTL deletes expired items asynchronously and is appropriate for cleanup, not prompt authoritative liveness. See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
[^dsql-occ]: Aurora DSQL uses optimistic concurrency control and surfaces conflicting writes as serialization errors. See: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html
[^dsql-working-with]: Aurora DSQL's PostgreSQL guidance also describes its OCC model and lock-free behavior. See: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with.html
[^ecs-sc]: ECS Service Connect provides service endpoints and connectivity for ECS services. See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-connect.html
[^ecs-sc-concepts]: ECS Service Connect concepts doc. See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-connect-concepts.html
[^ecs-services]: ECS services maintain the requested task count for long-running services. See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html
[^ecs-daemon]: ECS `DAEMON` scheduling runs exactly one task on each eligible active container instance. See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_service-options.html
[^ecs-service-options]: ECS service scheduling strategy options. See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_service-options.html
