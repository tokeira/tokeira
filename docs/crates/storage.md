# tokeira-storage

**Purpose:** Aurora DSQL persistence with OCC retry and fenced transactions.

See [050-dsql-storage](../architecture/050-dsql-storage.md) for the full storage design and [060-connection-management](../architecture/060-connection-management.md) for connection budgeting.

## What it owns

- **Durable state commits** — fenced DSQL transactions for workflow transitions
- **History append** — immutable event batch storage
- **Activity/timer state** — normalized side tables for open entities
- **Request dedup** — idempotency table for external request IDs
- **Connection management** — node-local `ConnectionDirector` with class-based permits
- **Shard lease persistence** — fenced shard ownership rows
- **Current execution mapping** — `(namespace_id, workflow_id)` → run identity
- **OCC retry classification** — success / duplicate / retryable conflict / fatal

## What it does NOT own

- **State transition logic** — that's the kernel
- **Delivery** — that's the runtime broker
- **Projection** — projection sinks write their own rows
- **Connection budget allocation** — cluster-wide budgets are DynamoDB-backed

## Module Map

```
tokeira-storage/src/
  api.rs     — storage trait and contract definitions
  memory.rs  — in-memory development store for tests and examples
```

The in-memory store is useful for tests and Codex-driven feature work. A production DSQL implementation is planned but not yet written.

## DSQL-Specific Constraints

Aurora DSQL is not "PostgreSQL with infinite scale." The storage layer treats these constraints as first-class architecture inputs:

| Constraint | Impact on storage design |
|---|---|
| Fixed `Repeatable Read` isolation | OCC with commit-time conflict detection |
| 3,000-row mutation limit per transaction | Narrow, bounded write sets per transition |
| 5-minute max transaction time | Short-lived transactions only |
| 60-minute max connection duration | Session recycling with jitter |
| One database per cluster | Shared schemas, not table explosion |
| No temporary tables | CTEs and subqueries instead |
| No PL/pgSQL triggers | Application-managed state transitions |
| Monotonic PK anti-pattern | Random/distributed primary keys on hot tables |
| Async index creation | Schema migrations separate from hot path |

## Fenced Commit Model

Every state mutation is committed with explicit expectations:

```
expected_seq (TransitionSeq) → commit → conflict detection
```

1. Runtime loads run state with current `transition_seq`
2. Kernel produces `Transition` with `expected_seq` matching the loaded seq
3. Storage attempts commit: if durable seq has moved past `expected_seq`, the transaction aborts
4. Runtime decides: retry (reload + recompute), reject to caller, or fail shard

This fits DSQL's optimistic concurrency model directly.

## Schema Overview

```
core/
  shard_lease          — fenced shard ownership
  current_execution    — (namespace_id, workflow_id) → run identity
  workflow_hot         — small current summary row per open run
  history_batch        — immutable append-only event batches
  request_dedupe       — idempotency records
  activity_state       — normalized open activity state
  timer_bucket         — bucketed wakeup records for timer scanning

delivery/
  dispatch_backlog     — durable fallback for unmatched tasks

proj/
  projection_log       — typed durable mutations for sinks
  projector_checkpoint — per-sink, per-substream progress
  vis_execution        — canonical visibility rows
  vis_attr_*           — typed search attribute indexes
```

## Primary Key Design

Hot write tables use random/distributed keys per DSQL guidance:

- `workflow_hot.run_key = UUID`
- `history_batch(run_key, first_event_id)` — clusters by run
- `activity_state(run_key, schedule_event_id)`

Append-like system tables (`dispatch_backlog`, `projection_log`) use a fanout dimension before the time-like key to avoid hot-edge writes.

## Connection Budget Management

Connection management is split into three layers:

1. **Node-local `ConnectionDirector`** — idle/warm session reservoirs, class-based permits, open-rate token bucket, session recycling
2. **Cluster-wide `BudgetAllocator`** (DynamoDB-backed) — per-node active/open-rate budgets, heartbeat TTL
3. **Runtime `WorkAdmission`** — demand reporting, priority-based throttling

Workload classes in priority order: `Control` > `Commit` > `StartTask` > `VisibilityRead` > `Projection` > `Maintenance`.

## Storage API Shape

The storage API exposes the real contract, not a fake ORM:

- `load_current_execution`
- `load_hot`
- `commit_transition`
- `start_workflow_task`
- `start_activity_task`
- `renew_shard_lease`
- `find_dispatchable_*`
- `read_projection_substream`
- `advance_projector_checkpoint`

## OCC Retry Classification

Storage classifies commit outcomes into:

| Outcome | Runtime action |
|---|---|
| Success | Proceed to publish effects |
| Duplicate | Short-circuit (already committed) |
| Retryable conflict | Reload state, recompute via kernel, retry |
| Fatal validation conflict | Reject to caller or fail shard |

## Temporal Feature Coverage

| Feature | Storage participation |
|---|---|
| Workflow state | Persists `workflow_hot` summary row |
| History | Appends immutable `history_batch` records |
| Activities | Maintains `activity_state` side table |
| Timers | Maintains `timer_bucket` for scanner |
| Request dedup | Persists request IDs for idempotency |
| Shard fencing | Maintains `shard_lease` with epoch |
| Delivery backlog | Persists `dispatch_backlog` as fallback |
| Visibility | Hosts projection tables (written by projection sinks) |
| Connection management | Manages DSQL session lifecycle |
| OCC | Fenced commits with conflict detection |
