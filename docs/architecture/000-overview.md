# 000 Overview

**Status:** draft for architecture review  
**Related docs:** [010-history-as-authority](010-history-as-authority.md), [015-configuration](015-configuration.md), [020-kernel](020-kernel.md), [025-system-services](025-system-services.md), [030-runtime-lanes](030-runtime-lanes.md), [035-placement-and-membership](035-placement-and-membership.md), [040-delivery-broker](040-delivery-broker.md), [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [050-dsql-storage](050-dsql-storage.md), [055-admission-control](055-admission-control.md), [060-connection-management](060-connection-management.md), [065-runtime-auto-tune](065-runtime-auto-tune.md), [070-projection-plane](070-projection-plane.md), [075-archival-to-s3](075-archival-to-s3.md), [080-sql-visibility](080-sql-visibility.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

**Tokeira** is a Temporal-compatible durable execution engine implemented in Rust and specialized for **Aurora DSQL** as its only persistence backend. The design goal is to preserve the public Temporal contract that SDKs, operators, and tooling care about, while changing the internal architecture enough to make ordering, delivery, and persistence materially simpler and more efficient.

This repo is **not** aiming for a service-by-service port of Temporal’s current Frontend / History / Matching / Worker layout. Temporal documents that server layout today, but it also documents a more important truth for this redesign: a single Task Queue can contain work for many Workflow Executions, task ordering on partitioned queues is not the same thing as workflow event ordering, and the Worker Service exists to run internal background Workflows rather than user code.[^temporal-server][^matching][^task-queue][^event-history] Workflow durability comes from event history, while queue delivery is an implementation detail with weaker ordering guarantees.

The central idea of Tokeira is therefore:

> **Preserve workflow semantics and public APIs, but collapse correctness around one authoritative per-run transition log.**

In practical terms, that means:

- **History is the durable authority.**
- **Hot workflow state is a persisted summary of that authority.**
- **Task delivery is derived from committed state, not co-equal with it.**
- **Visibility is a projection plane, not part of the correctness core.**
- **Archival is a separate durability/retention service, not part of the hot path.**
- **Aurora DSQL constraints shape the storage design directly.**

## Why this architecture exists

Temporal’s current docs describe four independently scalable services. History maintains mutable state, queues, and timers; Matching hosts Task Queues; Frontend handles routing and authorization; Worker Service runs internal background Workflows.[^temporal-server] That separation is operationally useful, but it also creates multiple ordering domains:

1. per-workflow event history,
2. internal History queue ordering,
3. Matching queue delivery ordering,
4. visibility update ordering.

For users, only the first one is truly semantic. Tokeira deliberately moves the other three into *derived* domains.

Aurora DSQL makes this redesign even more attractive. AWS documents a fixed PostgreSQL `Repeatable Read` isolation level, optimistic concurrency control, commit-time conflict detection, a 3,000-row mutation limit per transaction, a 5-minute maximum transaction time, one database per cluster, limits on schemas/tables, advice to avoid monotonically increasing primary keys on hot tables, and guidance to use CTEs/subqueries instead of temporary tables.[^dsql-migration][^dsql-quotas][^dsql-pk] Those constraints strongly favor:

- narrow, retryable transactions,
- single-writer ownership where possible,
- append-friendly side tables,
- no global queue-head rows,
- no correctness dependency on heavyweight shared coordination.

## Proposed system shape

Tokeira is organized into **three core planes**, **five primary crates**, and a small set of **operational services**.

### Plane 1: Compatibility edge

This plane preserves the Temporal-facing contract:

- `tokeira-edge`
- `tokeira-proto`
- `tokeira-types`

Responsibilities:

- expose `WorkflowService`, `OperatorService`, and health endpoints,
- perform authn/authz, namespace lookup, and request ID handling,
- gate long polls before they reach deeper runtime resources,
- translate public Temporal wire types into internal commands and DTOs.

The edge exists to preserve interfaces, not to own workflow semantics.

### Plane 2: Authoritative runtime and storage

This plane owns correctness:

- `tokeira-kernel`
- `tokeira-runtime`
- `tokeira-storage`

Responsibilities:

- shard ownership and fencing,
- lane-local execution of workflow actors,
- durable state transitions,
- durable timers, activity state, and task-start validation,
- derived dispatch intents.

This is where the system decides what *actually happened*.

### Plane 3: Projection plane

This plane owns read models:

- `tokeira-projection`

Responsibilities:

- canonical SQL visibility,
- rollups and operational summaries,
- optional custom sinks,
- independent sink checkpoints and replay.

This plane is intentionally open-ended and does **not** assume Elasticsearch.

### Operational services

Tokeira should also have a **small, explicit set of operational services** that are outside the correctness hot path:

- a **controller / autoscaler** service,
- a **system service** for long-running internal workflows and operator jobs,
- an **archival service** for exporting closed execution data to S3,
- optional admin / repair tooling.

These services exist because not every background concern belongs inside the runtime, and not every background concern should be implemented as a durable workflow. The rule is:

- **hot-path mechanics stay in runtime/control services**,
- **long-running, auditable platform jobs may use Tokeira’s own workflow engine**.

## Design principles

### 1. History as authority

Every state-changing request is turned into a per-run transition. That transition appends history, updates the run summary, and emits derived effects atomically. The system never relies on an external queue write as the canonical record that work exists.

### 2. Per-run total order, not global total order

Lamport’s classic observation is that distributed systems naturally give us a partial order (“happened before”), and stronger total ordering should only be imposed where the application actually needs it.[^lamport] Temporal’s own Task Queue docs make clear that queue ordering is not a global semantic guarantee.[^task-queue] Tokeira therefore enforces a **total order per workflow run**, plus explicit causal edges across runs and side effects.

### 3. Inactive runs should become cheap without making active runs slow

Temporal workflows can wait for external signals, timers, activities, and child completions for long periods, but other workloads can stay hot or burst sharply. Tokeira therefore does **not** optimize for one profile. Instead, the runtime should perform well across hot, bursty, dormant, high-cardinality, and mixed workloads by making inactive runs cheap **without** adding avoidable overhead to active runs. Actors are loaded on demand, commit a transition, publish derived effects, and then either continue locally or evict quickly when no near-term work remains.

### 4. Delivery is ephemeral-first

Worker polling and sync matching should live primarily in memory. Durable backlog is a fallback and recovery aid, not the default path. If a task can be scheduled and started without persisting a queue row, that should be the fast path.

### 5. Visibility is a projection, not a side database bolted onto correctness

Temporal’s visibility model already distinguishes a visibility store from the core persistence store, supports SQL backends, List Filters, custom Search Attributes, and dual visibility for migration.[^visibility][^dual-visibility][^list-filter][^search-attributes] Tokeira generalizes this into a typed projection log with replayable sinks.

### 6. Archival is separate from hot retention

Temporal’s self-hosted archival backs up closed Workflow Execution histories and visibility records to blob storage.[^archival] Tokeira should support the same category of outcome, but treat it as an explicit asynchronous service with its own pacing, retries, and S3 object model. DSQL hot retention can remain generous if cost and operational posture allow it; archival exists for long-tail durability, compliance, migration, and storage tiering, not because every closed execution must be evicted immediately.

### 7. System workflows are optional and deliberate

Temporal’s Worker Service runs internal background workflows.[^temporal-server] Tokeira should keep the equivalent function, but not as a fourth core correctness service. Internal workflows are a good fit for long-running, auditable operator jobs; they are a poor fit for hot-path routing, delivery, or lease control.

## Crate map

```text
crates/
  tokeira-types/
  tokeira-proto/
  tokeira-edge/
  tokeira-kernel/
  tokeira-runtime/
  tokeira-storage/
  tokeira-projection/
```

### `tokeira-types`

Strong internal identities and shared DTOs:

- `RunKey`, `RunId`, `NamespaceId`, `ShardEpoch`,
- task queue identities,
- task tokens,
- payload wrappers,
- visibility summaries.

### `tokeira-proto`

Public Temporal-compatible wire types plus Tokeira internal control-plane protos.

### `tokeira-edge`

Thin compatibility shell. No workflow correctness decisions here.

### `tokeira-kernel`

Pure deterministic state machine:

```rust
Command -> Transition
```

No I/O, no storage access, no delivery concerns.

### `tokeira-runtime`

Shards, lanes, run actors, delivery broker, sweepers, timer scanners.

### `tokeira-storage`

Aurora DSQL persistence, OCC retry, fenced transactions, and DDB-backed connection-budget allocation.

### `tokeira-projection`

Projection-log readers, SQL visibility applier, rollups, and custom sinks.

## The top-level execution model

A typical workflow mutation should look like this:

```text
public request
  -> edge translation
  -> shard routing
  -> lane-local run actor
  -> kernel.apply(loaded_state, command)
  -> one fenced DSQL commit
  -> publish derived dispatch/projection effects
  -> park actor
```

A typical long poll should look like this:

```text
long poll
  -> edge gate
  -> delivery broker
  -> sticky waiter? live-ready task? backlog?
  -> start-task transaction
  -> worker receives task token + history delta
```

A typical archival flow should look like this:

```text
workflow close
  -> close transition commits in DSQL
  -> archival candidate published asynchronously
  -> archival service reads closed execution + history
  -> writes immutable archive objects to S3
  -> marks archive complete
  -> later retention service may prune hot DSQL state
```

## What “Temporal-compatible” means here

For Tokeira, compatibility means:

- preserving the public `WorkflowService` and `OperatorService` contract,
- preserving workflow history semantics and replay model,
- preserving task-start / completion semantics,
- preserving visibility behavior that users and UI depend on,
- preserving the operational meaning of sticky execution, polling, retries, signals, timers, Continue-As-New, and archival/export outcomes users expect from a self-hosted durable execution platform.

It does **not** mean preserving internal service boundaries or today’s exact queue-processing topology.

## Reading guide

- Read [010-history-as-authority](010-history-as-authority.md) first if you want the core invariant.
- Read [020-kernel](020-kernel.md) for the deterministic state transition contract.
- Read [025-system-services](025-system-services.md) for the replacement of a Temporal-style Worker Service.
- Read [030-runtime-lanes](030-runtime-lanes.md) and [040-delivery-broker](040-delivery-broker.md) for execution and polling.
- Read [050-dsql-storage](050-dsql-storage.md) and [060-connection-management](060-connection-management.md) for persistence and DSQL-specific control.
- Read [070-projection-plane](070-projection-plane.md), [075-archival-to-s3](075-archival-to-s3.md), and [080-sql-visibility](080-sql-visibility.md) for read models, archival, and visibility.
- Read [090-failover-and-recovery](090-failover-and-recovery.md) for fencing, sweepers, and rebuild paths.

## Review questions

1. Is the compatibility boundary stated tightly enough, or should it explicitly enumerate more Temporal APIs?
2. Are we comfortable stating “history as authority” even though `workflow_hot` remains a persisted summary?
3. Do we want to treat delivery backlog as purely derived from run state, or do we want a stronger durable outbox abstraction inside storage?
4. Should archival be triggered immediately on close, or by a later retention window and policy engine?
5. Which internal jobs truly justify running as durable system workflows, and which should remain plain control services?

## References

[^temporal-server]: Temporal Server, official docs: https://docs.temporal.io/temporal-service/temporal-server  
[^matching]: Temporal Matching Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md  
[^task-queue]: Temporal Task Queues docs: https://docs.temporal.io/task-queue  
[^event-history]: Temporal Event History docs: https://docs.temporal.io/workflow-execution/event  
[^visibility]: Temporal Visibility docs: https://docs.temporal.io/visibility  
[^dual-visibility]: Temporal Dual Visibility docs: https://docs.temporal.io/dual-visibility  
[^list-filter]: Temporal List Filter docs: https://docs.temporal.io/list-filter  
[^search-attributes]: Temporal Search Attributes docs: https://docs.temporal.io/search-attribute  
[^archival]: Temporal self-hosted Archival docs: https://docs.temporal.io/self-hosted-guide/archival  
[^dsql-migration]: Aurora DSQL migration guide: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html  
[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html  
[^dsql-pk]: Aurora DSQL primary key guidance: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html  
[^lamport]: Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System* (1978): https://lamport.azurewebsites.net/pubs/time-clocks.pdf
