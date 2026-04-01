# 035 Placement and Membership

**Status:** revised draft for architecture review  
**Related docs:** [000-overview](000-overview.md), [030-runtime-lanes](030-runtime-lanes.md), [037-dynamic-placement](037-dynamic-placement.md), [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [050-dsql-storage](050-dsql-storage.md), [055-admission-control](055-admission-control.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

This note defines how **Tokeira** should do:

- runtime node membership,
- workflow-cell ownership,
- authoritative ownership fencing,
- edge routing,
- stale-route recovery via `NotShardOwner`, and
- failover without putting an external coordination store on the request path.

The design direction in this revision is:

> **Placement is queue-aware.**  
> **Authority is execution-scoped.**  
> **Ownership is fenced in DSQL.**

The earlier versions of this note treated placement mainly as a routing-shard problem and briefly used DynamoDB as an advisory placement store. This revision removes DynamoDB from placement entirely and makes the queue / execution split explicit.

## Why revise the earlier direction

Temporal’s current server design uses Ringpop-backed membership and fixed History Shards; each History Shard owns workflow mutable state and internal task queues, which makes shard ownership, membership, and internal queue processing tightly coupled. Temporal also states that the total number of History Shards is fixed at cluster creation.[^temporal-server][^temporal-history]

That is coherent for Temporal, but it is not the design Tokeira wants.

Tokeira does **not** want:

- an external coordination datastore on the request path,
- a shard abstraction that also owns durable queue processors,
- namespace-wide homing that is too coarse for uneven queue traffic,
- or raw membership convergence to be the correctness mechanism.

Instead, Tokeira wants:

- controller-driven but advisory placement,
- cached routing at the edge,
- DSQL fence rows as the only authoritative ownership record,
- and enough placement granularity to isolate hot queues without moving individual workflow runs continuously.

## Core principles

### 1. No external coordination store on the request path

The hot path for a workflow mutation or worker poll should be:

1. Edge resolves the operation to either a queue-home or an execution-home.
2. Edge consults its **local cached placement map**.
3. Request goes to the selected runtime node.
4. Runtime routes to the owning lane and run actor or broker.
5. Run actor commits through DSQL.

There should be **no DynamoDB read**, **no controller RPC**, and **no membership lookup** on the hot path.

### 2. Membership is advisory; DSQL fencing is authoritative

Aurora DSQL uses optimistic concurrency control and resolves conflicting writes at commit time, with conflicting transactions surfacing serialization errors that must be retried.[^dsql-occ][^dsql-working-with]

That makes DSQL the right place to hold the authoritative ownership fence for placement. Membership and placement can be stale for a short time as long as stale owners cannot commit.

### 3. Queue is the right placement hint; execution is the right authority object

The queue tells Tokeira where:

- workers naturally poll,
- backlog and live-ready state should live,
- and new executions on that queue should usually begin.

The workflow execution tells Tokeira where:

- signals,
- updates,
- cancels,
- exact-run operations,
- and current-run semantics

must be handled.

This is why Tokeira uses **queue-home** and **execution-home** as different concepts.

### 4. Routing granularity should be finer than lease granularity

Tokeira should use:

- a **large queue-partition space** for even spread and cheap rebalance, and
- a **smaller lease-bundle space** for authoritative ownership.

That avoids renewing or moving tens of thousands of tiny authoritative leases individually.

### 5. The workflow run remains the unit of correctness

As elsewhere in Tokeira, **run ordering** is the correctness boundary. Cells, bundles, and queue partitions are placement devices, not execution subsystems.

## Placement roles

### Edge

The edge:

- routes polls by **queue partition home**,
- routes start requests by **queue-aware start placement**,
- routes signals/updates/cancels by **execution-home**,
- caches `bundle -> owner` and `queue-partition -> preferred cell` maps,
- refreshes its cache on `NotShardOwner`, bundle-generation change, or controller update.

### Runtime node

A runtime node:

- participates in placement through a long-lived control stream,
- owns zero or more authoritative lease bundles,
- hosts lanes and run actors,
- renews bundle leases in DSQL,
- runs sweepers after bundle acquisition or takeover,
- may also host queue-home broker partitions.

### Placement controller

A placement controller:

- receives runtime heartbeats and demand over internal gRPC,
- computes desired bundle and queue-partition placement,
- publishes routing snapshots and incremental changes,
- never becomes the correctness authority for ownership.

This controller should run as a small internal ECS service. ECS Service Connect is a good fit for the controller/runtime and edge/controller traffic because it gives ECS services stable service endpoints and connectivity without introducing a separate gossip substrate.[^ecs-sc][^ecs-sc-concepts]

### DSQL lease authority

DSQL is the only authoritative ownership plane. A runtime owns a bundle only if the DSQL lease row says it does.

## Queue-home and execution-home

### Queue-home

`queue_home` is the placement preference for a queue family partition.

Conceptual key:

- `namespace_id`
- `task_queue`
- `task_kind` (`workflow` or `activity`)
- `queue_partition`

`queue_home` is used for:

- worker poll routing,
- live-ready and backlog ownership,
- default start placement for new workflow executions.

### Execution-home

`execution_home` is the authoritative home of a workflow execution or run chain.

Conceptual keys:

- `(namespace_id, workflow_id)` for current execution lookup,
- `run_id` for exact-run operations.

`execution_home` is used for:

- signals,
- updates,
- cancels / terminate,
- current execution pointer updates,
- exact-run operations,
- authoritative workflow-task and activity-state mutation.

### Why both are needed

A queue is not enough to identify the owner of a running workflow because many workflow operations do not naturally arrive with a queue. A workflow execution is not enough to optimize delivery locality because workers poll queues, not individual workflow IDs.

Tokeira therefore uses:

> **queue-home to choose where work should naturally start and where pollers should go,**  
> **execution-home to decide where workflow state is authoritatively mutated.**

## Membership without DynamoDB

Instead of storing membership in DynamoDB, use **controller-managed live membership**.

### Proposed mechanism

- Each runtime opens a long-lived gRPC stream to the controller over Service Connect.
- The stream carries:
  - node identity,
  - version/build info,
  - queue pressure,
  - lane pressure,
  - connection-budget headroom,
  - drain state.
- The controller keeps live node state in memory.
- If the stream drops, the node is considered unavailable for new placement after a grace interval.

This makes membership cheap and immediate without turning it into a database workload.

### Why this is acceptable

Membership is **not authoritative**. If the controller loses track of a node prematurely, correctness is still protected because only the DSQL lease holder can commit.

## Controller availability model

Run the controller as a small **REPLICA** ECS service. ECS services are built to maintain the requested task count, and Service Connect can give the controller a stable internal endpoint.[^ecs-services][^ecs-sc]

Use a small DSQL control lease for leader election. Only the leader computes or publishes new placement. Followers are hot standbys.

If the controller fleet is temporarily unavailable:

- existing runtime owners continue renewing bundle leases directly in DSQL,
- edges continue using cached routing maps,
- correctness is unaffected,
- rebalance and new-placement decisions pause until a leader returns.

That is an acceptable failure mode.

## Queue partitions and lease bundles

### Queue partitions

Use a large fixed queue-partition space. The exact count is an implementation choice, but it should be large enough that one hot queue can be split and rebalanced without moving an entire namespace.

A useful mental model is:

- queue family = `(namespace, task_queue, task_kind)`
- queue partition = `hash(placement_key) mod N`

`placement_key` should prefer:

1. an explicit application affinity key if provided,
2. otherwise the `workflow_id`,
3. otherwise a deterministic fallback.

### Lease bundles

Group authoritative work into a smaller set of DSQL lease bundles.

A bundle is the authoritative ownership unit used for fenced commit rights. Bundles may contain many queue partitions and many workflow executions.

### Why bundles matter

If authoritative ownership were per queue partition or per routing shard, the system would create unnecessary write volume for lease renewals and rebalances. Bundling keeps routing fine-grained while keeping the authoritative lease set small.

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
SELECT epoch, owner_node_id, lease_until
FROM control.bundle_lease
WHERE bundle_id = $bundle_id;

-- verify owner == self, epoch == expected, lease_until > now()
-- then commit workflow transition
```

The important point is that the authoritative fence is in the same storage system that commits workflow transitions.

## Routing state distribution

Edges and runtimes should not query DSQL for placement on each request.

Instead, the controller publishes a compact routing snapshot containing at least:

- `queue_partition -> preferred_cell_id`
- `bundle_id -> owner_node_id`
- `generation`

Edges and runtimes cache this data locally and update incrementally.

## Stale-route recovery

If an edge routes to the wrong runtime:

1. the runtime rejects the request with `NotShardOwner` / stale placement,
2. the edge refreshes the relevant placement map,
3. the edge retries.

This is acceptable because correctness is fenced at DSQL commit time, not at route-computation time.

## How this note relates to dynamic placement

This note is intentionally about **structure**:

- who owns what,
- what is authoritative,
- how edges and runtimes find owners,
- and what abstractions exist.

The policy for *when* to move queue partitions or rebias starts belongs in [037-dynamic-placement](037-dynamic-placement.md).

## Review questions

1. Is the queue-home / execution-home split clear enough for both poll routing and exact-run APIs?
2. Is the bundle size likely to be small enough for smooth rebalance, but large enough to keep lease churn cheap?
3. Should queue partitions be globally sized, or should hot queues be allowed to split to a larger partition count than cold queues?
4. Is `NotShardOwner` + cache refresh an acceptable user-visible retry path for all public APIs, or do some need special handling?

## References

[^temporal-server]: Temporal Server, official docs: https://docs.temporal.io/temporal-service/temporal-server
[^temporal-history]: Temporal History Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md
[^dsql-occ]: Aurora DSQL concurrency control: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html
[^dsql-working-with]: Aurora DSQL migration / working guide: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html
[^ecs-sc]: Amazon ECS Service Connect: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-connect.html
[^ecs-sc-concepts]: Amazon ECS Service Connect concepts: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-connect-concepts.html
[^ecs-services]: Amazon ECS services: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html
