# 030 Runtime Lanes

**Status:** draft for architecture review  
**Related docs:** [020-kernel](020-kernel.md), [040-delivery-broker](040-delivery-broker.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Purpose

`tokeira-runtime` is the execution shell around the pure kernel. It owns:

- shard acquisition and renewal,
- lane-local executors,
- run actor lifecycle,
- routing to the owning lane,
- timer scanning,
- sweeper-triggered rebuild work,
- publication of derived effects.

The runtime should be the **single place** where a run becomes active in memory.

## Design claim

Tokeira should run **many isolated workflow actors on a small number of single-thread execution lanes**, rather than dedicating a thread to every workflow or treating a Task Queue as a thread-affinity boundary.

This works because Temporal’s semantic boundary is the workflow execution, not the task queue, and because Tokio explicitly supports single-thread execution and `!Send` tasks via the current-thread scheduler, `LocalSet`, and `spawn_local`.[^task-queue][^tokio-current-thread][^tokio-localset][^tokio-spawn-local]

## Why lanes exist

Lanes give us three valuable properties at once:

### 1. Per-run serialization by construction

A run actor only executes on one lane at a time.

### 2. Low coordination overhead

Lane-local structures can be `Rc` / `RefCell` / `hashbrown::HashMap` style data structures without cross-thread locking in the hot path.

### 3. Cheap multiplexing

Thousands of mostly parked workflow actors can share a small number of threads.

This is closer in spirit to virtual actors than to a classic service-per-concern architecture. Orleans is a useful mental model here: actors are loaded on demand and runtime placement is an implementation concern, not an application contract.[^orleans]

## Topology

```text
shard
  -> lane
     -> run actor
```

More concretely:

- a **shard** is the ownership/fencing unit,
- each shard is assigned to a runtime node,
- each node hosts several **lanes**,
- each run is routed to one lane based on shard and run identity,
- a lane hosts many run actors and a few lane-local services.

## Runtime objects

### Shard owner

Holds the current shard lease epoch and is responsible for:

- admitting commands for runs in that shard,
- starting timer and sweeper tasks,
- publishing recovery work on failover.

### Lane executor

A single-thread executor with:

- run actor map,
- lane mailbox,
- local waiter caches / live-ready structures,
- eviction policy.

### Run actor

A demand-loaded object that:

1. loads current state,
2. drains mailbox work for that run,
3. invokes kernel,
4. commits transition,
5. publishes derived effects,
6. parks or evicts.

## Profile elasticity, not profile specialization

Temporal workflows can wait on signals, timers, child workflows, and activities for long periods, but Tokeira should **not** be framed as optimizing for one workload profile over another. The goal is strong performance across **all** of the major profiles we expect to see:

- **hot** runs that execute many consecutive transitions,
- **bursty** runs that receive clusters of signals or updates,
- **dormant** runs that wait a long time for external input,
- **high-cardinality** workloads with very many open executions,
- **mixed** tenants where all of the above coexist.

The runtime design should therefore be read as a claim about **profile elasticity**:

> **inactive runs should become cheap without making active runs slow.**

That is the reason to talk about parked runs. Parking is not a preferred workload shape; it is the mechanism that prevents long-lived dormant executions from imposing avoidable cost on hot or latency-sensitive executions.

A parked run:

- has no in-memory actor unless recently touched,
- has no dedicated DB connection,
- keeps only durable state in DSQL,
- is reloaded when a real command or due timer arrives.

That helps the dormant/high-cardinality profiles, but the same lane-based runtime is also intended to help the hot/bursty profiles:

- **hot runs** benefit from lane-local execution, minimal coordination, and keeping short follow-on work resident briefly,
- **bursty runs** benefit from mailbox draining and bounded coalescing,
- **dormant runs** benefit from aggressive eviction and near-zero steady-state runtime cost,
- **mixed workloads** benefit from fairness and from keeping queue/delivery concerns out of the correctness path.

This is a strong fit for the virtual-actor style and for durable workflow engines such as Netherite, which emphasizes partitioning, recovery logs, snapshots, and movement of stateful workflow execution across compute hosts.[^netherite]

## Actor lifecycle

The intended lifecycle is:

```text
mailbox event arrives
  -> ensure shard ownership
  -> route to lane
  -> load actor if absent
  -> drain one or more mailbox items
  -> commit transitions
  -> publish derived effects
  -> park or evict
```

The important optimization point is **mailbox coalescing**. If several signals arrive for the same parked run in a short window, the actor should drain multiple mailbox items before parking, subject to fairness and transaction-size bounds.

## Routing

Baseline routing should use:

```text
lane = hash(shard_id, run_key) mod lane_count
```

Optional future optimization:

- support **affinity keys** for classes of related runs that benefit from locality,
- keep that off by default because it trades parallelism for locality.

The default should maximize even heat across lanes.

## Why not route by task queue

Temporal’s docs make clear that a single Task Queue can deliver work for many runs, and partitioned task queues do not provide strict global FIFO.[^matching][^task-queue] Routing by task queue would therefore conflate a delivery concern with a correctness concern.

Routing by run preserves the actual semantic boundary.

## Lane-local runtime style

Tokio’s docs explicitly support:

- a current-thread scheduler that executes all tasks on the current thread,
- `LocalSet` for `!Send` tasks,
- `spawn_local` for futures that must remain on the same thread.[^tokio-current-thread][^tokio-localset][^tokio-spawn-local]

This is exactly the runtime style Tokeira should exploit. A lane-local executor should prefer:

- `spawn_local`,
- lane mailbox channels,
- minimal shared state with other lanes,
- explicit handoff at shard/lane boundaries.

## Lane responsibilities

A lane should own:

- run mailbox draining,
- actor residency / eviction,
- live-ready task offers for runs it currently hosts,
- local sticky preference hints,
- publication of transitions to delivery/projection subsystems.

A lane should **not** own:

- global backlog fairness policy,
- DDB connection budget control,
- cluster-wide namespace cache,
- projection sink application.

## Actor cache and eviction

Actor eviction policy should prefer:

1. keep actors with a started workflow task or immediately follow-on work,
2. keep recently signaled actors briefly for burst coalescing,
3. evict idle parked actors aggressively,
4. evict sticky-affine actors only when cache pressure requires it.

Sticky execution exists because reusing worker-local cached workflow state avoids replay overhead.[^sticky-execution] Tokeira should preserve that benefit, but actor residency in the runtime is a separate cache from worker sticky state.

## Shard concurrency model

Each shard should have:

- one current owner,
- one monotonically increasing epoch,
- one or more lanes serving runs for that shard on the owner node.

The key safety rule is:

> **a stale owner may not commit transitions even if it still has run actors in memory.**

That is enforced by fencing in storage, not by trusting runtime memory.

## Background tasks in runtime

Runtime background tasks should stay narrow:

- **timer scanner**: finds due timer buckets and injects `TimerDue`,
- **sweeper**: reconstructs dispatchable work after failover/restart,
- **lease renewer**: keeps shard fences alive,
- **metrics publisher**: emits lane and backlog pressure signals.

Each of these tasks should use classed DB permits from connection management, not raw pool access.

## Rust-specific advantages

Rust helps here in three specific ways:

### 1. Ownership and confinement

Lane-local actor state can be kept clearly confined.

### 2. Cheap async task multiplexing

Tokio tasks are lightweight and cooperatively scheduled.[^tokio-task]

### 3. Compile-time distinction between `Send` and `!Send`

This lets us intentionally keep lane-local structures out of cross-thread sharing.

## Research note

Orleans and Netherite are useful mental anchors, but Tokeira is not copying them directly.

- Orleans contributes the “virtual actor loaded on demand” intuition.[^orleans]
- Netherite contributes ideas around partitioning, snapshots, and efficient execution of durable workflows.[^netherite]

The key difference is that Tokeira must preserve Temporal-compatible APIs and history semantics while targeting DSQL specifically.

## Review questions

1. Do we want lanes strictly per node, or should shard ownership itself define lane count dynamically?
2. Should affinity-key routing exist in the first milestone, or remain a future optimization?
3. How aggressively should actor eviction favor burst coalescing over minimal memory use?

## References

[^matching]: Temporal Matching Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md  
[^task-queue]: Temporal Task Queues docs: https://docs.temporal.io/task-queue  
[^sticky-execution]: Temporal Sticky Execution docs: https://docs.temporal.io/sticky-execution  
[^tokio-current-thread]: Tokio runtime docs, current-thread scheduler: https://docs.rs/tokio/latest/tokio/runtime/index.html  
[^tokio-localset]: Tokio `LocalSet` docs: https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html  
[^tokio-spawn-local]: Tokio `spawn_local` docs: https://docs.rs/tokio/latest/tokio/task/fn.spawn_local.html  
[^tokio-task]: Tokio task docs: https://docs.rs/tokio/latest/tokio/task/  
[^orleans]: Bernstein et al., *Distributed Virtual Actors for Programmability and Scalability* (Orleans): https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf  
[^netherite]: Burckhardt et al., *Netherite: Efficient Execution of Serverless Workflows*: https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf
