# 037 Dynamic Placement (pure multi-DSQL clusters)

**Status:** draft for architecture review  
**Decision direction:** preferred if Tokeira uses multiple workflow DSQL clusters  
**Related docs:** [000-overview](000-overview.md), [035-placement-and-membership](035-placement-and-membership.md), [040-delivery-broker](040-delivery-broker.md), [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [050-dsql-storage](050-dsql-storage.md), [055-admission-control](055-admission-control.md), [065-runtime-auto-tune](065-runtime-auto-tune.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

This note defines how **Tokeira** should do **dynamic placement** when it has **multiple workflow cells**, each backed by its own **Aurora DSQL cluster**.

The design in this note is explicitly:

- **pure multi-DSQL cluster**,
- **queue-aware for starts and polls**,
- **execution-scoped for authoritative mutation**,
- **dynamic for new work and queue partitions**,
- **conservative for already-running executions**,
- **with no role at all for S3 Express**.

The central policy statement is:

> **With one DSQL cluster, storage metrics drive tuning.**  
> **With multiple workflow DSQL clusters, storage metrics should also drive placement.**

## Why dynamic placement exists

Aurora DSQL exposes cluster-level ceilings and health signals that matter directly to Tokeira, including cluster count quotas, storage-per-cluster limits, connection limits, connection-rate limits, and observability metrics such as `TotalTransactions`, `OccConflicts`, `CommitLatency`, `ClusterStorageSize`, and DPU usage.[^dsql-quotas][^dsql-cw]

Once Tokeira has more than one workflow DSQL cluster, it can use those signals to do more than local admission or pacing. It can decide:

- where **new workflows** should start,
- where **queue partitions** should home,
- when a queue partition is hot enough to split,
- when a cell is hot enough that new starts should be biased away,
- and when a run chain or dormant execution should be migrated.

This is the key difference between **local auto-tune** and **dynamic placement**:

- local auto-tune changes how a cell behaves,
- dynamic placement changes where new and future work lands.

## Non-goals

This note is **not** about:

- moving the correctness authority out of DSQL,
- replacing DSQL with S3 or any object store,
- making every active run mobile at any instant,
- or continuously shuffling placements in response to tiny metric changes.

Tokeira still has one authoritative store per workflow cell. Placement is dynamic; correctness is not.

## System model

### Workflow cells

A **workflow cell** is the combination of:

- one authoritative workflow DSQL cluster,
- one runtime fleet able to own bundles in that cluster,
- one queue-home / broker surface for queue partitions assigned there.

This note assumes multiple workflow cells are available at the same time.

### Visibility cell

This note is about **workflow placement**. A dedicated visibility cluster can still exist, but it is orthogonal. Visibility load does not decide where workflow authority lives.

### Queue-home and execution-home

From [035-placement-and-membership](035-placement-and-membership.md):

- `queue_home` tells Tokeira where new work should naturally begin and where workers should poll,
- `execution_home` tells Tokeira where workflow state is authoritatively mutated.

Dynamic placement operates on both, but differently.

## Placement objects

### 1. Queue family

A queue family is identified by:

- `namespace_id`
- `task_queue`
- `task_kind`

This is still too coarse for high volume.

### 2. Queue partition

A queue partition is identified by:

- `queue_family`
- `queue_partition`

`queue_partition` is derived from a stable placement key, typically:

1. explicit affinity key,
2. otherwise `workflow_id`,
3. otherwise deterministic fallback.

This is the main unit of **dynamic placement**.

### 3. Execution home

An execution home is identified by:

- `(namespace_id, workflow_id)` for the current run chain,
- `run_id` for exact-run routing.

This is the authoritative object that owns signals, updates, cancels, and semantic transitions.

## Three control loops

Dynamic placement should run on **three different time scales**.

### Loop A: fast local tune (seconds)

Runs every few seconds inside each cell.

Purpose:

- tune broker budgets,
- tune edge poll admission,
- tune delivery fairness,
- tune projection pacing,
- protect commit reserves.

This loop responds to pressure but does **not** move placement.

Typical signals:

- `CommitLatency`
- `OccConflicts`
- `TotalTransactions`
- `QueryTimeouts`
- open long polls
- schedule-to-start
- sync match
- backlog age / count
- connection-budget headroom.[^dsql-cw][^worker-health]

### Loop B: medium placement loop (15–60 seconds)

Runs in the controller.

Purpose:

- rebias **new start placement**,
- move **queue partitions**,
- increase or decrease **hot queue partition counts**,
- rebalance queue-home distribution across workflow cells.

This is the main loop that should react to sustained storage or delivery pressure.

### Loop C: slow safe migration loop (minutes or maintenance windows)

Runs in the controller or maintenance service.

Purpose:

- migrate **dormant executions**,
- migrate **run chains** at `ContinueAsNew`,
- drain or evacuate cells,
- perform deliberate rebalance after large topology changes.

This loop should be conservative and bounded.

## The signals that should drive placement

### Storage pressure

Storage pressure is the first-class reason to rebias starts or move queue partitions in a multi-DSQL deployment.

A practical score is:

```text
storage_pressure(cell) =
    w1 * norm(CommitLatency)
  + w2 * norm(OccConflicts / max(TotalTransactions, 1))
  + w3 * norm(QueryTimeouts)
  + w4 * norm(WriteDPU + ReadDPU + ComputeDPU)
  + w5 * norm(BytesWritten)
  + w6 * norm(ClusterStorageSize growth)
```

These metrics are directly documented by Aurora DSQL.[^dsql-cw]

### Delivery pressure

Delivery pressure tells us whether a queue partition is healthy where it currently lives.

A practical score is based on:

- `workflow_task_schedule_to_start_latency`
- `activity_schedule_to_start_latency`
- sync match rate
- poll success rate
- backlog count / backlog age
- worker slot availability.[^worker-health][^worker-performance]

Temporal’s worker guidance is useful here because it explicitly frames queue health in terms of schedule-to-start, sync match, poll success, and backlog metrics, and gives rough targets for healthy systems.[^worker-health]

### Locality pressure

Locality pressure is Tokeira-specific and must come from internal metrics.

Key signals:

- cross-cell dispatch ratio,
- fraction of workflow-task starts that occur in the same cell as the authoritative commit,
- sticky hit rate,
- sticky fallback rate,
- cross-cell activity dispatch ratio.

This score prevents the controller from improving storage spread while accidentally destroying delivery locality.

### Runtime pressure

Runtime pressure describes per-cell compute and execution load.

Signals:

- runnable transitions queued per lane,
- lane activation / eviction churn,
- sweeper backlog,
- connection-budget headroom,
- edge poll pressure.

Runtime pressure helps distinguish “storage is hot” from “the runtime plane is simply underprovisioned.”

## What dynamic placement should move

### Move first

These are the safest and highest-value placement changes:

- **new-start placement weights**,
- **queue partition homes**,
- **queue partition count** for hot queues,
- **poll routing preference** for queue partitions.

These changes improve spread without changing the authoritative owner of already-running executions.

### Move carefully

These are safe only with explicit protocol support:

- **run-chain home at `ContinueAsNew`**,
- **dormant execution-home** for runs with no outstanding workflow task, no active task start, and no near-term timer fire.

### Avoid moving routinely

These moves should be rare:

- hot active runs,
- runs with a started workflow task,
- runs with in-flight activity start semantics,
- large namespace-wide moves in one step.

This is where churn, sticky erosion, and cross-cell amplification are most likely.

## Placement scoring

The controller should score placements, not hardcode them.

A simple starting function is:

```text
placement_score(cell, queue_partition) =
    - storage_pressure(cell)
    - runtime_pressure(cell)
    - delivery_pressure(queue_partition on cell)
    - locality_penalty(queue_partition, cell)
    + capacity_headroom(cell)
```

Then:

- **new starts** go to the best eligible cell,
- **queue partitions** move only if another cell is materially better,
- **existing executions** stay put unless a safe-boundary migration exists.

The same logic can be extended with hard exclusion constraints such as:

- maintenance / drain state,
- version skew,
- missing worker deployments,
- protected reserves.

## Hysteresis and anti-flapping rules

Dynamic placement must not flap.

Required guardrails:

- minimum dwell time for a moved partition,
- minimum improvement required before moving,
- cooldown after a move,
- maximum moves per control interval,
- rollback if locality gets materially worse,
- no queue split/merge loops within a short window.

These rules are as important as the scoring function itself.

## Start placement

On `StartWorkflowExecution`:

1. edge identifies the workflow queue family,
2. edge derives the queue partition from the placement key,
3. controller-published placement chooses a preferred workflow cell,
4. start is routed there,
5. the start commit establishes `execution_home` in that cell.

This makes queue-aware placement the **default start bias**, not the permanent semantic identity.

## Poll placement

Worker poll traffic should go to the **queue-home** for the worker’s queue partition.

This matters because the poll plane benefits the most from locality. If polls route to the right queue-home, the broker can exploit:

- sticky matches,
- live-ready offers,
- backlog fairness,
- and lower handoff latency.

## Execution routing after start

Once an execution home is chosen, the following should normally route by `execution_home`:

- signal,
- update,
- cancel / terminate,
- query that requires authoritative run state,
- current-run resolution,
- exact-run APIs.

That is why dynamic placement helps most for starts and polls first, and only later for selective safe execution migration.

## Safe migration boundaries

### Continue-As-New

This is the best migration boundary.

The old run closes, a new run is created, and the current-execution pointer advances. That is already a strong semantic boundary, so changing home cell there is natural.

### Dormant-run migration

A dormant run may be migrated if all of the following hold:

- no pending started workflow task,
- no active start-task reservation,
- no in-flight activity start,
- no due-immediately timer,
- queue-home for its next expected work has already moved or is materially better elsewhere.

### Maintenance / evacuation

If a cell is draining or degraded, Tokeira may migrate a larger set of dormant runs or run chains, but should still prefer safe boundaries over hot moves.

## Failure handling

Dynamic placement must always defer to authoritative fences.

If the controller moves a queue partition or start weight, but a stale edge still routes to the old cell:

1. request lands on the old runtime,
2. runtime rejects with stale placement / `NotShardOwner`,
3. edge refreshes placement,
4. retry lands on the correct cell.

Correctness comes from DSQL fence rows, not from controller freshness.

## What dynamic placement can and cannot solve

### It can help with

- spreading authoritative transaction load across multiple workflow DSQL clusters,
- keeping hot queues from collapsing into one cell,
- protecting healthy cells from noisy neighbors,
- improving sync match and schedule-to-start by restoring locality,
- reducing blast radius of hot workloads.

### It cannot solve

- one hot workflow that is intrinsically single-writer,
- one bad queue key that puts all work in one partition,
- one worker fleet that is simply underprovisioned,
- or a single-cluster deployment where all writes still hit the same DSQL cluster.

## Observability

The controller should export at least:

- placement score per `(cell, queue_partition)`,
- current queue-home distribution,
- current start-placement weights,
- cross-cell dispatch ratio,
- queue partition move count,
- queue split / merge count,
- execution migration count,
- rollback count,
- dwell-time violations,
- hottest queue partitions,
- effective authoritative spread.

A useful spread metric is:

```text
effective_spread = 1 / Σ(p_i²)
```

where `p_i` is the fraction of authoritative workflow write load on workflow cell `i`.

This is a practical way to measure whether placement is actually using the multi-cluster topology.

## Performance expectations

If Tokeira has multiple workflow DSQL clusters and locality remains good, dynamic placement should be able to turn cell count into real authoritative throughput headroom.

If Tokeira has only one workflow DSQL cluster, dynamic placement should be treated as a tuning problem, not a scaling solution.

This note does not set hard numerical targets, but it does imply one testable claim:

> **If queue-aware placement materially improves effective spread while keeping cross-cell dispatch and sticky fallback low, aggregate authoritative throughput should rise.**

## Review questions

1. Should queue partition count be global, or should hot queues be allowed to scale to larger partition counts than cold queues?
2. Is `ContinueAsNew` sufficient as the main execution-home migration boundary, or do we want a broader dormant-run migration protocol?
3. Which internal metrics are sufficient to estimate locality pressure without adding too much control-plane complexity?
4. Should placement scoring be fully centralized in one controller, or should cells propose local weights to the leader?
5. Do we want strict hard limits on cross-cell dispatch ratio before a move is considered unhealthy?

## References

[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html
[^dsql-cw]: Aurora DSQL monitoring metrics: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cloudwatch-monitoring.html
[^worker-health]: Temporal worker health guidance: https://docs.temporal.io/cloud/worker-health
[^worker-performance]: Temporal worker performance guidance: https://docs.temporal.io/develop/worker-performance
