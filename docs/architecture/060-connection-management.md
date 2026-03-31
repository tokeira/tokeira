# 060 Connection Management

**Status:** draft for architecture review  
**Related docs:** [050-dsql-storage](050-dsql-storage.md), [030-runtime-lanes](030-runtime-lanes.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Purpose

Connection management is not an implementation footnote for Tokeira. It is a primary part of making Aurora DSQL behave well under load and failure.

The central design decision is:

> **Lease connection budget globally, lease actual sockets locally.**

In other words:

- use DynamoDB to allocate **node-level budgets**,
- do **not** lease individual connections in DynamoDB,
- keep real sessions inside node-local pools controlled by those budgets.

## Why this differs from a naive pool

Aurora DSQL imposes cluster-wide connection constraints that matter directly to architecture:

- 10,000 active connections per cluster,
- 100 new connections per second,
- burst capacity of 1,000 new connections,
- 60-minute maximum connection duration.[^dsql-quotas]

These are the scarce resources. The problem is therefore not “who owns connection 847?” The problem is:

- how many sessions may this node hold,
- how fast may this node open new sessions,
- which workload classes get priority when budget is tight?

## Why not coordinate via DSQL itself

The budget allocator should remain outside DSQL’s blast radius. If the database is already rejecting new connections or suffering a reconnection storm, it is the wrong place to coordinate recovery.

DynamoDB is the right coordination substrate here because:

- it is separate from the DSQL failure domain,
- the state is small and coarse-grained,
- conditional writes and TTL fit the allocator problem well.[^dynamodb-conditions]

## Design overview

Tokeira should split connection management into three layers.

### 1. Node-local `ConnectionDirector`

Lives in `tokeira-storage`.

Responsibilities:

- maintain idle/warm session reservoirs,
- enforce class-based permits,
- borrow/return local sessions,
- rate-limit *new* opens with a token bucket,
- recycle sessions before DSQL’s hard lifetime cutoff.

### 2. Cluster-wide `BudgetAllocator`

Lives logically in DDB-backed control code.

Responsibilities:

- observe node demand,
- assign per-node active/open-rate budgets,
- reserve floor capacity for critical control traffic,
- expire dead nodes via heartbeat TTL.

### 3. Runtime `WorkAdmission`

Lives in runtime.

Responsibilities:

- report demand by class,
- stop low-priority work when storage budget is tight,
- protect critical classes such as shard lease renewals and commits.

## Workload classes

Tokeira should use explicit DB workload classes:

- `Control`
- `Commit`
- `StartTask`
- `Projection`
- `VisibilityRead`
- `Maintenance`

When the system is under stress, these classes should be degraded in roughly that order:

1. protect `Control`,
2. then protect `Commit`,
3. then `StartTask`,
4. throttle `VisibilityRead`,
5. heavily throttle or stop `Projection`,
6. stop `Maintenance` first.

This matches the durable-execution priority stack.

## ConnectionDirector behavior

The node-local acquire path should be:

1. acquire a class permit,
2. try to borrow an idle connection,
3. if none are idle, check:
   - active sessions < granted active budget,
   - open-rate bucket has tokens,
4. if both pass, open a new session,
5. otherwise wait or fail depending on class policy.

This ensures that the pool is governed by the budget, not the other way around.

## Why open-rate control matters more than pool size

In Aurora DSQL, reconnection storms are often more dangerous than steady-state usage because **new connection creation** is rate-limited cluster-wide.[^dsql-quotas]

Therefore, each node should maintain a local token bucket:

- refill rate = granted open-rate tokens per second,
- max capacity = granted burst,
- spend one token per new connection.

This shapes reconnect behavior across the cluster and reduces the chance that a rolling restart or network flap causes `CONFIGURED_LIMIT_EXCEEDED(53400)` for everyone at once.[^dsql-quotas]

## Global allocator model

The allocator should track **envelopes**, not sockets.

Example per-node grant:

- `granted_active`
- `granted_open_rate_per_sec`
- `granted_burst`
- `control_reserve`
- `generation`

Example per-node reported demand:

- `owned_shards`
- `pending_commits`
- `pending_starts`
- `pending_visibility_reads`
- `pending_projection_batches`

Allocation policy can then weight shard ownership and work pressure, rather than statically slicing the cluster equally.

## DynamoDB table shape

A simple model is enough.

### `tokeira_conn_allocator`

Singleton row per cluster with:

- allocator owner,
- lease expiry,
- cluster-wide limits,
- safety margins,
- generation.

### `tokeira_conn_nodes`

One row per node with:

- heartbeat timestamp,
- TTL,
- desired demand,
- observed usage,
- granted budgets,
- generation.

The allocator uses conditional writes to hold the allocator lease and publish new grants. DynamoDB conditional expressions are a natural fit for this compare-and-swap style control loop.[^dynamodb-conditions]

## Warm pools, not giant pools

Because new opens are rate-limited, Tokeira should prefer a **small warm pool** instead of oscillating between zero and a burst of fresh sessions.

Good rule of thumb:

- keep a warm baseline around recent p95 in-use count plus modest headroom,
- never let low-priority work inflate the baseline,
- let class semaphores, not giant idle reservoirs, absorb burst structure.

## Session lifetime and recycling

Aurora DSQL documents a maximum connection duration of 60 minutes.[^dsql-quotas] Aurora DSQL also uses authentication tokens for connection establishment; once the connection is established it remains valid even if the original token expires.[^auth-token]

Tokeira should therefore:

- cap session lifetime below 60 minutes, e.g. 50–55 minutes,
- add jitter so the whole pool does not recycle at once,
- recycle lazily on return-to-idle where possible,
- avoid mass reaping.

This is a very practical source of stability.

## IAM roles and roles-in-database

Aurora DSQL uses IAM authentication for cluster access and database roles for SQL authorization.[^auth-overview][^db-roles] That means Tokeira can sensibly separate roles for:

- admin/migrations,
- runtime commits,
- projection writers,
- read-only visibility APIs,

even if they all use the same managed endpoint.

## Endpoint model

Aurora DSQL single-region clusters expose a single managed endpoint and automatically redirect traffic on unavailability within the region.[^dsql-endpoint] Tokeira therefore does not need classic reader/writer endpoint choreography here. Pool separation should be about **workload class and DB role**, not endpoint topology.

## Recommended pool split

Keep it simple:

- **control pool**: tiny, protected, for shard lease renewal / ownership / sweep control,
- **runtime pool**: main pool for commits and task starts,
- optional **read/projection pool** if role separation or noisy-neighbor isolation needs it.

Long polls should never consume DSQL sessions.

Parked workflows should never consume DSQL sessions.

## Degraded mode

On connection exhaustion or open-rate exhaustion:

1. stop projection opens,
2. stop maintenance opens,
3. aggressively limit visibility reads,
4. protect control and commit traffic,
5. back off with jitter,
6. continue using healthy existing sessions if available.

This is the operational side of “durable execution first, observability second.”

## What this buys us

This design gives Tokeira:

- lower DDB churn than per-connection leasing,
- fewer reconnect storms,
- clear protection for control and commit traffic,
- no DSQL cost for long polls or parked workflows,
- a direct match to Aurora DSQL’s real constraints.

## Review questions

1. Should the allocator weight `owned_shards` more heavily than `pending_commits`, or vice versa?
2. Do we want a dedicated read-only visibility pool from day one, or start with class-based sharing only?
3. Should grants be strictly push-based from the allocator, or may nodes temporarily self-throttle below grant without writing a new desired value?

## References

[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html  
[^auth-overview]: Aurora DSQL authentication and authorization: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/authentication-authorization.html  
[^db-roles]: Using database roles and IAM authentication with Aurora DSQL: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/using-database-and-iam-roles.html  
[^auth-token]: Aurora DSQL authentication tokens: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/SECTION_authentication-token.html  
[^dsql-endpoint]: Aurora DSQL endpoint behavior (User Guide PDF): https://docs.aws.amazon.com/aurora-dsql/latest/userguide/aurora-dsql-ug.pdf  
[^dynamodb-conditions]: DynamoDB condition expressions: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
