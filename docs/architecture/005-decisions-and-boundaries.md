# 005 Decisions and Boundaries

**Status:** accepted  
**Related docs:** [000-overview](000-overview.md), [010-history-as-authority](010-history-as-authority.md), [015-configuration](015-configuration.md), [020-kernel](020-kernel.md), [030-runtime-lanes](030-runtime-lanes.md), [040-delivery-broker](040-delivery-broker.md), [050-dsql-storage](050-dsql-storage.md), [060-connection-management](060-connection-management.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

This document is the **ground-truth record** of resolved architectural decisions, the Temporal compatibility boundary, and performance targets for Tokeira. It exists so that spec authors, reviewers, and future contributors can find settled answers in one place rather than hunting through review-question sections across many docs.

---

## Temporal Compatibility Boundary

### Preserved

Tokeira preserves the following Temporal semantics and wire-level behaviors:

- Temporal SDK wire protocol (gRPC `WorkflowService`)
- Workflow determinism semantics
- History event ordering
- Task token semantics
- Signal / query / update delivery
- Activity heartbeat
- Retry policies
- Continue-as-new chains
- Child workflows
- Timers
- Cancellation
- Termination
- Search attributes
- Memo
- Visibility queries

### Changed

Tokeira changes the following internal implementation details:

- **Internal server topology** — no separate Frontend / History / Matching / Worker services; collapsed into edge, runtime, and projection planes
- **Storage backend** — Aurora DSQL instead of Cassandra / MySQL / Postgres
- **Task queue implementation** — in-memory broker with durable backlog instead of Matching service
- **No partitioned task queues** — single logical queue per name
- **Connection management** — reservoir pattern for DSQL with distributed rate limiting

### Not supported (documented)

The following are explicitly out of scope or deferred:

- Multi-cluster replication
- Archival (deferred)

### In progress

The following are actively being implemented:

- Schedules (execution engine and 7 gRPC handlers)
- Batch operations (4 gRPC handlers)
- Nexus (task transport — 3 gRPC handlers; full routing parity with Temporal is the goal)
- Advanced visibility (SQL-native on DSQL — no Elasticsearch dependency)

---

## Performance Targets

Theoretical best on a capable cloud platform (ECS on EC2, Aurora DSQL, single region). These are design targets — the architecture should not have structural bottlenecks that prevent reaching them.

| Metric | Target | Notes |
|---|---|---|
| Workflow starts | 10,000+ WPS sustained | Per DSQL cluster; limited by commit throughput |
| Workflow task dispatch p99 | < 5 ms | Sync match (poller waiting when task arrives) |
| Workflow task dispatch p99 | < 50 ms | Async match (task waits for poller) |
| Activity dispatch p99 | < 5 ms | Sync match path |
| History event append | Single DSQL transaction per transition | Narrow, retryable |
| End-to-end workflow latency | < 100 ms | Start → first WFT complete (single-step workflow, co-located worker) |
| Connection budget | 10,000 connections per DSQL cluster | 100/sec sustained new-connection rate |
| Concurrent open workflows | 10M+ per DSQL cluster | Limited by storage, not compute |
| Node density | 50,000+ active runs per node | 8 vCPU ECS task |

**Validated so far:** Production validation on ECS + DSQL pending.

---

## Resolved Review Questions (Decision Log)

### From 040 (Delivery Broker)

**Q: "Separate broker instances for workflow and activity tasks, or one broker with distinct policies?"**

- **Decision:** Separate. `InMemoryBroker` for workflow tasks, `InMemoryActivityBroker` for activity tasks.
- **Rationale:** Workflow tasks have sticky routing, logical sequence tracking, and at-most-one-pending semantics. Activity tasks have retry/heartbeat semantics and multi-attempt dispatch. Separate types make the invariants enforceable at compile time.
- **Evidence:** `tokeira-runtime/src/broker.rs` implements both as distinct types.

**Q: "How long should the live-ready grace window be?"**

- **Decision:** No explicit grace window for MVP. Tasks are published to the broker immediately on commit. If no poller is waiting, the task stays in the broker's in-memory ready set indefinitely (until the run is evicted or the node fails, at which point recovery republishes from durable state).
- **Rationale:** With in-memory broker and shard-based recovery, there's no need for a time-based grace window. The sweeper handles republishing after failover.
- **Evidence:** `publisher.rs` publishes immediately; `recovery.rs` handles republish on shard acquisition.

**Q: "Should sticky preference be modeled in workflow_hot or in an auxiliary runtime cache?"**

- **Decision:** Modeled on `WorkflowState` (kernel state) as `sticky: Option<StickyAffinity>`. The kernel tracks sticky affinity and the broker uses it for routing.
- **Rationale:** Sticky affinity is part of the workflow's authoritative state — it affects which worker gets the next WFT. Keeping it on state means it survives recovery.
- **Evidence:** `tokeira-kernel/src/state.rs` has `sticky: Option<StickyAffinity>`.

### From 015 (Configuration)

**Q: "Do we want to expose any namespace-level fairness or priority policy on day one?"**

- **Decision:** No. For MVP, all namespaces are equal. Fairness is per-run (round-robin across runs with pending work on the same queue).
- **Rationale:** Namespace-level service classes require admission control infrastructure (055) which is deferred. Per-run fairness is sufficient for single-tenant and small multi-tenant deployments.
- **Evidence:** `fairness.rs` implements per-run round-robin, no namespace weighting.

### From 030 (Runtime Lanes)

**Q: "Do we want lanes strictly per node, or should shard ownership define lane count dynamically?"**

- **Decision:** Fixed lane count per node (configurable, default 4). Shards are distributed across lanes by `shard_id % lane_count`.
- **Rationale:** Dynamic lane count remains deferred, but routing by shard rather than run hash reduces shard movement blast radius from every lane to the one lane that owns the shard's command stream. Multiple shards may still share a lane.
- **Evidence:** `TokeiraRuntime::new()` takes `lane_count`; runtime and publisher paths derive `ShardId` with `shard_for(run_key, shard_count)` before lane selection; scanner paths route with shard context already present on the scan loop or timeout tracking entry.

### From 050 (DSQL Storage)

**Q: "Should the first milestone physically separate core and proj schemas?"**

- **Decision:** Single schema for MVP. All tables in one DSQL database.
- **Rationale:** DSQL has limited schema/table budgets. Separation adds operational complexity without benefit at current scale.
- **Evidence:** Single `schema.sql` in the DSQL tooling.

### From 090 (Failover and Recovery)

**Q: "Should the sweeper rebuild live-ready first and only later write backlog?"**

- **Decision:** Sweeper republishes to broker (live-ready) directly from durable state. No separate backlog write on recovery.
- **Rationale:** The broker IS the live-ready tier. Recovery means "read durable state, republish pending work to broker." Backlog is a future optimization for overflow.
- **Evidence:** `recovery.rs` reads dispatchable tasks from storage and publishes to broker.

---

## Unresolved Questions (Deferred)

| Question | Blocks |
|---|---|
| Namespace service classes and admission control | 055 |
| Dynamic placement across multiple DSQL clusters | 037 |
| Auto-tune control loops | 065 |
| Durable backlog overflow tier | 040 advanced |
| Archival service design | 075 |
