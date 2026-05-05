# 060 Connection Management

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
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

## Connection demand analysis

This section traces every DSQL access path, what triggers it, and how many connections it actually needs. The reference deployment is 10 runtime nodes at 400 WPS aggregate (40 WPS per node) with 64 bundles.

### Control class — shard lease operations

| Operation | Trigger | Frequency per node |
|---|---|---|
| `try_acquire_bundle` | Shard acquisition at startup or rebalance | Rare — once per bundle on ownership change |
| `renew_bundle` | Periodic lease renewal | Every ~10s per owned bundle (30s lease / 3 renewals) |
| `list_bundle_leases` | Controller reads all leases for snapshot | Every snapshot cycle (~5s), controller only |
| `advance_generation` | Controller advances routing generation | Per snapshot publish, controller only |
| `allocate_budget` | Controller distributes connection budget | On membership change, controller only |

With 64 bundles across 10 nodes, each node owns ~6 bundles. That is ~6 renewals every 10 seconds = 0.6 ops/sec per node. These are short single-row reads/updates. One connection handles this comfortably.

**Peak concurrent connections: 1.** Budget: 2–3.

### Commit class — transition commits

| Operation | Trigger | Frequency per node |
|---|---|---|
| `commit_transition` | Every workflow state change (WFT completion, activity completion, signal, timer, cancel) | Proportional to WPS |
| `persist_to_backlog` | Unmatched task dispatch when no worker is polling | Proportional to unmatched tasks |
| `drain_backlog` | Backlog sweep drains queued tasks | Periodic per queue with pending backlog |

`commit_transition` is the hot path. It is a multi-statement transaction within a single connection: epoch fence check (`SELECT epoch FROM shard_lease`), OCC check (`SELECT transition_seq FROM workflow_hot FOR UPDATE`), dedupe check, optional current_execution conflict check, then the write set (workflow_hot upsert, history_batch insert, activity/timer/dispatch side-table mutations, projection_log insert, request_dedupe insert). One transaction, one connection, held for the full duration.

At 40 WPS per node with ~10ms average DSQL round-trip, that is 0.4 connections in-use concurrently on average. At p99 latency (~50ms), peak concurrent usage is ~2 connections. Burst structure matters: if 10 workflow tasks complete simultaneously on one lane, 10 concurrent commits happen briefly.

**Peak concurrent connections: ~5 (burst).** Budget: 15.

### Read class — load, resolve, history, sweep queries

| Operation | Trigger | Frequency per node |
|---|---|---|
| `resolve_execution` | Every incoming Start/Signal/Query from edge | Per API request |
| `find_latest_run` | Workflow-id conflict check on Start | Per Start |
| `load_run` | Lane loads run state for processing | Per WFT/activity start |
| `read_history` | Worker needs history replay (cache miss) | Per WFT with cache miss |
| `lookup_request_dedupe` | Dedupe check on read path | Per idempotent request |
| `list_dispatchable_workflow_tasks_for_shard` | Dispatch sweep per shard | ~1/sec per owned shard |
| `list_dispatchable_activity_tasks_for_shard` | Dispatch sweep per shard | ~1/sec per owned shard |
| `list_due_timers_for_shard` | Timer scanner | Every 200ms per owned shard |
| `list_runs_with_workflow_timeouts_for_shard` | Sweep reconstruction after shard acquisition | Once per shard on acquisition |
| `list_started_workflow_tasks_for_shard` | WFT timeout reconstruction | Once per shard on acquisition |
| `list_open_activities_for_shard` | Activity timeout reconstruction | Once per shard on acquisition |
| `list_pending_nexus_operations_for_shard` | Nexus timeout reconstruction | Once per shard on acquisition |

The timer scanner is the most frequent background reader: 200ms interval × 6 owned shards = 30 reads/sec per node. Each is a simple indexed query at ~5ms, so 0.15 connections in-use concurrently.

Dispatch sweeps add ~12 reads/sec per node (workflow + activity, one per shard per second). Load/resolve/history reads scale with WPS — at 40 WPS per node, roughly 80 reads/sec (load + resolve per workflow task). At 5ms each, that is 0.4 connections concurrent.

Sweep reconstruction queries fire once per shard on acquisition (startup or rebalance), not continuously. They are a brief burst of 4–5 queries per shard, then done.

**Peak concurrent connections: ~3.** Budget: 10.

### Projection class — projection log reads

| Operation | Trigger | Frequency per node |
|---|---|---|
| `read_from` (ProjectionLog) | Projection worker polling for new records | Continuous, per partition |

Projection workers poll the projection log for new entries to apply to visibility stores. With 1 partition per node (typical), that is one continuous reader at ~100ms poll interval with ~5ms query time.

**Peak concurrent connections: 1.** Budget: 3.

### Maintenance class — background housekeeping

Not yet implemented. Reserved for archival, cleanup, and other background operations.

**Peak concurrent connections: 0.** Budget: 2 (headroom for future use).

### Summary: 10-node reference deployment at 400 WPS

| Class | Peak concurrent (per node) | Recommended budget (per node) | What holds the connection |
|---|---|---|---|
| Control | 1 | 2–3 | Lease renewal: single-row `UPDATE shard_lease` |
| Commit | 5 (burst) | 15 | Transition commit: multi-statement tx (~10–50ms) |
| Read | 3 | 10 | Timer scans, dispatch sweeps, load/resolve queries |
| Projection | 1 | 3 | Projection log poll: `SELECT FROM projection_log` |
| Maintenance | 0 | 2 | Reserved |
| **Total** | **~10** | **~32** | |

**Cluster-wide totals at 10 nodes:**

- Connections: 320 out of 10,000 DSQL limit (3.2%)
- Startup fill rate: 320 connections at 100/sec = 3.2 seconds
- Per-node fill rate with controller fair-share: 32 connections at 10/sec = 3.2 seconds
- Connection recycling: ~1 connection/min per node (50-min lifetime), 10/min cluster-wide — negligible against 100/sec rate limit

### Where pressure actually comes from

1. **Commit transaction duration, not connection count.** A commit holds a connection for the entire multi-statement transaction. If DSQL latency spikes (OCC retries, cross-region), commit duration grows and connections pile up. Class-based semaphores protect Control from Commit backpressure.

2. **Startup fill rate.** 10 nodes × 32 connections = 320 needed. At 100/sec cluster-wide with controller fair-share (10/sec per node), each node fills in 3.2 seconds. Acceptable.

3. **Rolling restart.** One node restarts, needs 32 new connections at 10/sec (its fair share) = 3.2 seconds. The old node's connections are still alive until they expire (~50 minutes). No pressure on the 10k limit.

4. **Timer scanner frequency.** 200ms interval × 6 shards = 30 reads/sec. This is the most frequent DSQL access. If shard count per node increases (e.g., 64 shards on one node during drain), this grows linearly. At 64 shards per node it would be 320 reads/sec — still manageable but worth monitoring.

### Default allocation ratios

The `default_allocations` function in `connection.rs` uses percentage-based ratios that sum to `target_ready`:

| Class | Ratio | At target_ready=50 | At target_ready=32 |
|---|---|---|---|
| Control | 10% | 5 | 3 |
| Commit | 50% | 25 | 16 |
| Read | 20% | 10 | 6 |
| Projection | 10% | 5 | 3 |
| Maintenance | remainder | 5 | 4 |

The default `target_ready: 50` is generous for 400 WPS / 10 nodes. A `target_ready: 32` would be sufficient with headroom. The 50 default works without tuning up to ~1000 WPS per node before commit concurrency becomes the bottleneck.

### When DynamoDB-backed coordination becomes necessary

At the 10-node / 400 WPS reference deployment, the controller fair-share approach is sufficient. DynamoDB coordination (token bucket rate limiter, slot block manager) becomes valuable at two thresholds:

- **Work-conserving rate sharing** — when heterogeneous nodes (some doing heavy commit work, others mostly idle) need idle nodes' rate budget available to busy ones. Fair-share wastes the idle budget.
- **Controller-independent operation** — when connection management must survive controller unavailability indefinitely, not just for the `valid_until` directive window.

Neither is a day-one requirement. See the `connection-budget-allocator` deferred spec for the DynamoDB-backed approach.

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


## Implementation status

The node-local `ConnectionDirector` is implemented in `tokeira-storage/src/dsql/` using the official `aurora-dsql-sqlx-connector` (v0.1.2) as the underlying driver. The implementation includes:

- **Reservoir**: async-channel-based buffer with three background tasks (refiller, expiry scanner, return processor). Connections are pre-created and validated before checkout.
- **Token-bucket rate limiter**: lock-free atomic implementation with `reconfigure()` hook for future distributed coordination. Defaults to full cluster budget (100/sec, 1,000 burst) for single-node deployments.
- **Class budgets**: per-`DbClass` semaphores (Control, Commit, Read, Projection, Maintenance) with runtime reconfiguration.

The cluster-wide `BudgetAllocator` (DynamoDB-backed) is deferred to the `connection-budget-allocator` spec. The node-local rate limiter exposes a `reconfigure(rate, capacity)` method that the future allocator will call to adjust per-node shares.

### Implementation reference

See `.kiro/specs/dsql-schema-connection/` for the full spec and `tokeira-storage/src/dsql/` for the code.
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
