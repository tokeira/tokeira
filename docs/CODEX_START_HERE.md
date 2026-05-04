# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

Last updated: 2026-05-03

## Codebase snapshot

| Crate | Source lines | Tests | Status |
|-------|-------------|-------|--------|
| `tokeira-types` | 1,242 | 17 | Stable. Spread keys, ExecutionStatus/TaskKind stable numeric mappings. |
| `tokeira-kernel` | 4,902 | 230 | Stable. All kernel features implemented. |
| `tokeira-storage` | 9,432 | 110 | Active. In-memory store + DSQL backend (Features 1–4 complete). |
| `tokeira-runtime` | 20,373 | 209 | Complete. All runtime features + schedule/batch/nexus/versioning stores. |
| `tokeira-edge` | 17,743 | 146 | Active. All gRPC handlers implemented including eager dispatch. |
| `tokeira-proto` | 434 | — | Stable. Protobuf codegen. |
| `tokeira-projection` | 4,602 | 34 | Active. Visibility sink, rollups, filter compilation, query service, DSQL store (partial). |

Platform and tooling crates:

| Crate | Status |
|-------|--------|
| `tokeira-config` | Server config + generic TOML loader. |
| `tokeira-state` | CAS store + S3 state store for deployment state. |
| `tokeira-iac` | IaC engine: Module trait, diff/plan/apply/destroy. |
| `tokeira-deploy-engine` | Service lifecycle engine. |
| `tokeira-orchestrator` | Deployment orchestration facade. |
| `tokeira-compose` | Docker Compose provider (bollard). |
| `tokeira-aws` | AWS resource implementations. |

Apps: `tokeirad` (server), `tkr` (CLI), `tokeira-admin`, `tokeira-bench`, `tokeira-replay`.

Total: ~75k lines of Rust, ~785 tests, all passing.

## What has been built

### Kernel — complete

Pure deterministic state machine. All command variants handled.
Start, signal, WFT lifecycle, activities, timers, children,
external signals/cancels, nexus, updates, continue-as-new,
reset, pause/unpause, markers, execution options. 230 tests
(golden + property).

### Storage — in-memory complete, DSQL in progress

**In-memory store:** Full storage contract: OCC fencing, request
dedup, history append with pagination, activity/timer/nexus side
tables, dispatch backlog, projection log, lease management, shard
mapping, epoch validation.

**DSQL backend (Features 1–4 complete, Features 5–6 in progress):**

| Feature | Spec | Status |
|---------|------|--------|
| 1. Schema + Connection | `dsql-schema-connection` | ✅ Complete |
| 2. Core Persistence | `dsql-core-persistence` | ✅ Complete |
| 3. Side Tables | `dsql-side-tables` | ✅ Complete |
| 4. Shard Leasing | `dsql-shard-leasing` | ✅ Complete |
| 5. Spread Keys | `dsql-spread-keys` | ✅ Complete |
| 6. Projection Persistence | `dsql-projection-persistence` | ✅ Complete |
| 7. Projection Visibility | `projection-visibility` | 📋 Spec complete, implementation not started |

DSQL modules implemented in `tokeira-storage/src/dsql/`:
- `DsqlStore` — production storage foundation with connection director
- `DsqlRunRepository` — full `RunRepository` trait against DSQL (fenced commits, OCC, shard epochs)
- `DsqlProjectionLog` — partitioned projection log reads with cursor-based pagination
- `DsqlConnectionDirector` — class-based connection budget control with reservoir pattern
- `TokenBucketRateLimiter` — distributed rate limiting for DSQL connection rate
- `Reservoir` — warm connection pool with proactive expiry
- `MigrationRunner` — forward-only schema migration runner
- `DsqlConnector` — IAM-authenticated SQLx pool
- Codec module — postcard-based encode/decode for all BYTEA columns
- Shared `convert` module — checked numeric conversions for Rust ↔ DSQL type boundary
- DDL validator for DSQL constraints

28 migration files (V001–V028) covering all tables and indexes.

**DSQL visibility store** (`tokeira-projection/src/dsql_store.rs`):
- ✅ Checkpoint read/write
- ✅ Execution upsert/delete
- ✅ `ProjectionSink::apply` (UpsertExecution, CloseExecution, memo merge)
- ❌ Query methods (list, count, rollup) — stubbed, `projection-visibility` spec ready
- ❌ Search attribute registry and indexing — stubbed
- ❌ Rollup accumulation — stubbed

### Runtime — complete

Lane executors, workflow + activity brokers, dispatch publisher,
timer/WFT/activity/nexus/workflow timeout scanners, child
orchestration, external signal/cancel delivery, nexus dispatch
(HTTP + worker-targeted), continue-as-new, OCC retry, recovery
sweeper, worker registry, versioning rule store, schedule store
+ execution engine, batch operation store, shard-aware lane
routing. 209 tests.

### Edge — nearly complete

All WorkflowService gRPC handlers implemented including eager
dispatch. 146 tests. See AGENTS.md for the full handler list.

### Projection — working, DSQL visibility spec ready

Visibility sink, rollups, filter compilation, query service,
projection worker — all working against InMemoryVisibilityStore.
34 tests. `DsqlVisibilityStore` has write path complete, query
path stubbed pending the `projection-visibility` spec.

### Platform and deployment

- Local platform (bare-process) and Docker Compose platform with
  observability stack (Mimir, Loki, Grafana, Alloy)
- IaC engine with Module trait, dependency resolution, diff/plan/apply/destroy
- Deploy engine with Service lifecycle and runtime state
- `tkr` CLI with infra/deploy/dev/build commands
- Docker Compose provider via bollard

### Architecture documentation

22 architecture documents in `docs/architecture/`:

| Doc | Status | Topic |
|-----|--------|-------|
| 000-overview | accepted | System overview |
| 005-decisions-and-boundaries | accepted | Resolved decisions |
| 010-history-as-authority | accepted | Correctness model |
| 015-configuration | accepted | Config philosophy |
| 020-kernel | accepted | Kernel design |
| 025-system-services | accepted | System service model |
| 030-runtime-lanes | accepted | Lane execution model |
| 035-placement-and-membership | revised draft | Queue-home/execution-home, DSQL fencing |
| 037-dynamic-placement | draft | Dynamic placement policy |
| 040-delivery-broker | accepted | Broker design |
| 045-autoscaling-on-ecs-ec2 | revised draft | Custom autoscaler, private-only networking, runtime retirement |
| 050-dsql-storage | accepted | DSQL storage design |
| 055-admission-control | draft | Admission control |
| 060-connection-management | accepted | DSQL connection reservoir + rate limiting |
| 065-runtime-auto-tune | draft | Auto-tuning |
| 070-projection-plane | draft | Projection architecture |
| 080-sql-visibility | draft | SQL visibility model |
| 075-archival-to-s3 | future | S3 archival |
| 090-failover-and-recovery | draft | Failover design |
| 110-firecracker-* | future | Firecracker exploration |

Key recent changes to architecture docs:
- **045-autoscaling-on-ecs-ec2**: Added autoscaling invariants, safe runtime
  scale-in protocol (Loop C: runtime retirement), instance scale-in protection,
  metric freshness and degraded autoscaling, connection-aware scaling envelope,
  autoscaler/controller responsibility split, AWS actuator reconciliation,
  runtime scale-out decision classification. Review questions resolved.
- **V017 migration**: Updated `idx_vis_execution_ns_close` from `NULLS FIRST`
  to `NULLS LAST` to match Rust `Option::cmp` sort semantics.

### SDK examples working

hello_world, message_passing, continue_as_new, child_workflows,
timers, schedules — all running against tokeirad.

## Completion assessment

| Plane | Estimate | Notes |
|-------|----------|-------|
| Compatibility Edge | ~90% | All handlers implemented including eager dispatch. |
| Runtime & Storage | ~70% | Runtime complete. DSQL Features 1–6 complete, visibility queries pending. |
| Projection | ~70% | Working against in-memory. DSQL write path complete, query path spec ready. |
| Platform / Ops | ~25% | Local + Compose platforms working. ECS deployment, placement, autoscaling pending. |
| **Overall** | **~55%** | Core correctness works end-to-end. DSQL visibility + ops are the remaining bulk. |

## What to work on next (priority order)

### P0: DSQL Visibility Queries (`projection-visibility` spec)

The spec is complete (requirements, design, tasks). This is the
next implementation target. It replaces the stubbed query methods
in `DsqlVisibilityStore` with real DSQL implementations.

Scope:
- 14 new migration files (V029–V042): `sa_registry`, `sa_current`,
  7 typed index tables, `vis_rollup`, 3 `vis_execution` indexes
- Search attribute registry (`resolve_attr`, `register_attr`)
- Search attribute indexing (all 7 types including KeywordList)
- Rollup accumulation
- Filter-to-SQL compiler (pure function, parameterized queries)
- `list_executions` with keyset pagination and 5 sort orders
- `count_executions` with GROUP BY
- `count_from_rollup` from pre-aggregated rollup table
- Fix `InMemoryVisibilityStore` KeywordList/Text filter semantics
  to match Temporal's element-membership / token-matching contract
- Update `DsqlVisibilityStore::apply` to write search attrs + rollups

**Why P0:** ListWorkflowExecutions and CountWorkflowExecutions
need durable visibility for production. The spec is ready to implement.

### P1: Observability Foundation

Establish the metrics, tracing, and logging foundation.

- Metrics registry (Prometheus-compatible)
- OpenTelemetry tracing integration
- Structured logging with correlation IDs
- Per-crate instrumentation conventions
- Export pipeline (Prometheus scrape, OTLP export)

**Why P1:** DSQL features are already instrumented with `tracing::instrument`.
The foundation work is about the export pipeline and baseline metrics
for existing subsystems.

### P2: Shard Placement and Membership (`shard-placement-membership` spec)

Spec complete (requirements, design, tasks). Currently single-node
with in-memory shard ownership. Production needs distributed shard
assignment.

- Controller-managed live membership (gRPC streams, not DynamoDB)
- Queue-home and execution-home placement
- Bundle lease management via DSQL
- Shard rebalancing on node join/leave
- Edge routing cache with `NotShardOwner` recovery
- Controller-coordinated DSQL connection budget allocation
  (controller computes per-node shares from membership count,
  sends `ConnectionBudgetDirective` via membership stream;
  `TokenBucketRateLimiter::reconfigure()` hook already implemented;
  reservoir `target_ready` capped by `max_reservoir_size` to bound
  cluster-wide open connections)

Architecture: see 035-placement-and-membership.md and 037-dynamic-placement.md.

### P2: ECS Deployment (`ecs-deployment` spec)

Spec complete (requirements, design, tasks). The autoscaling
architecture doc (045) is revised and ready.

- Custom `tokeira-autoscaler` service reading Mimir
- REPLICA services for edge/projection/control
- DAEMON runtime with safe scale-in protocol (Loop C)
- Private-only networking with VPC endpoints
- Instance scale-in protection for runtime ASG
- Observability stack: Alloy sidecars + Mimir + Loki + Grafana ECS services

### P3: Admission Control

Architecture doc 055 exists but no spec or implementation.

- Edge `LongPollGate` for poll admission (referenced in 045)
- Per-namespace, per-task-queue, per-worker-identity limits
- Overload shedding with retryable semantics
- Broker budget tuning

### P3: Archival to S3

Deferred but needed for long-term history retention.

### P3: Remaining Proto Field Gaps

Features 1-4 from the edge-complete-implementation umbrella
are spec'd but some have remaining implementation work.

## Known deferred items

Items explicitly deferred during implementation that are not yet
tracked as specs or priority items:

### Infrastructure / operational

- **Runtime auto-tune** — Architecture doc 065 exists (draft). Local
  tuning of broker budgets, delivery fairness, projection pacing,
  commit reserves. No spec.
- **16 architecture docs have unresolved review questions** — Only
  045-autoscaling has resolved its review questions. The others
  represent open design decisions.

### Kernel / runtime

- **Snapshot + suffix recovery** — Architecture doc 090 describes
  persisted snapshot refs in `workflow_hot` for replay from snapshot
  instead of from origin. Currently recovery always replays from the
  full history prefix. (`TODO(storage)` at api.rs:595)
- **Broker sticky/live/backlog tier split** — The broker currently
  uses a single structure; the design calls for explicit tiers for
  better delivery performance. (`TODO(perf)` at broker.rs:61)
- **Sweep methods for activity tasks and archival eligibility** —
  The storage API has placeholder comments for sweep methods that
  aren't fully defined. (`TODO(storage)` at api.rs:258)

### Edge

- **26 UNIMPLEMENTED gRPC handlers** — Documented in AGENTS.md.
  Includes: legacy versioning (2), deployment management (5),
  legacy listing (4), activity-by-ID (5), namespace mutation (2),
  ExecuteMultiOperation, GetSearchAttributes, ListTaskQueuePartitions,
  activity/workflow options (5). Most are intentionally unimplemented
  (legacy or not-yet-needed), but some may be called by SDKs.
- **History serializer completeness** — Some event attributes use
  `Default::default()` placeholders because the kernel doesn't yet
  carry the full set of proto fields.
- **Worker-version capabilities in DescribeTaskQueue** — Tokeira
  doesn't yet publish worker-version capabilities or queue-level
  stats in DescribeTaskQueue responses.

### Projection

- **Batched apply for SQL sink** — `TODO(projection)` at sink.rs:14.
  The `ProjectionSink` trait only has single-record `apply()`.
- **Per-sink failure policies** — `TODO(projection)` at worker.rs:66.
  The projection worker has basic retry but no configurable per-sink
  failure policies.

## Invariants to preserve

- Never let transport or storage details leak into the kernel.
- Never make the projection path authoritative.
- Never let pollers or waiters become durable correctness objects.
- Never assume a lane owns a run forever.
- Never make inactivity expensive.
- No state visible to the system unless explained by a committed
  transition (010-history-as-authority).
- DSQL lease fencing is the only authoritative ownership mechanism.
- Missing metrics must never trigger scale-in.
- DSQL connection headroom is a hard scaling envelope.

## Spec structure

Each feature has a spec directory under `.kiro/specs/`:

```
.kiro/specs/{feature-name}/
  .config.kiro      — spec metadata
  requirements.md   — user stories + acceptance criteria
  design.md         — architecture, data models, properties
  tasks.md          — implementation plan with checkpoints
```

Architecture decisions are recorded in:
- `docs/architecture/005-decisions-and-boundaries.md`

## DSQL constraints reference

- OCC with Repeatable Read — conflicts surface as SQLSTATE 40001
- No temp tables — use CTEs and subqueries
- One DDL per transaction — each migration file has exactly one DDL statement
- `CREATE INDEX ASYNC` for non-blocking index creation
- No BIGSERIAL — application-generated IDs (Snowflake, UUID-derived)
- Connection rate limit: 100/sec cluster-wide
- Connection limit: 10,000 per cluster (default)
- `DbClass` connection budgets: Control 10%, Commit 50%, Read 20%, Projection 10%, Maintenance remainder

## TODO style

The codebase uses rich TODO comments:

- `TODO(correctness): ...`
- `TODO(perf): ...`
- `TODO(storage): ...`
- `TODO(edge): ...`
- `TODO(ops): ...`
- `TODO(runtime): ...`
- `TODO(projection-visibility): ...`

Extend that convention instead of adding generic TODOs.
