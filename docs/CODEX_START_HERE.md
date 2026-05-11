# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

Last updated: 2026-05-07

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
| `tokeira-aws` | AWS resource implementations and remote workstation lifecycle. |

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

**DSQL backend (complete):**

| Feature | Spec | Status |
|---------|------|--------|
| 1. Schema + Connection | `dsql-schema-connection` | ✅ Complete |
| 2. Core Persistence | `dsql-core-persistence` | ✅ Complete |
| 3. Side Tables | `dsql-side-tables` | ✅ Complete |
| 4. Shard Leasing | `dsql-shard-leasing` | ✅ Complete |
| 5. Spread Keys | `dsql-spread-keys` | ✅ Complete |
| 6. Projection Persistence | `dsql-projection-persistence` | ✅ Complete |
| 7. Projection Visibility | `projection-visibility` | ✅ Complete |

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
- ✅ Query methods (list, count, rollup) — delivered by `projection-visibility`
- ✅ Search attribute registry and indexing (all seven typed indexes including `KeywordList` element-membership and `Text` token matching)
- ✅ Rollup accumulation

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

### Projection — complete

Visibility sink, rollups, filter compilation, query service,
projection worker, and DSQL visibility store (full read and write
paths, typed search attributes, rollup accumulation). 34 tests.

### Platform and deployment

- Local platform (bare-process) and Docker Compose platform with
  observability stack (Mimir, Loki, Grafana, Alloy)
- IaC engine with Module trait, dependency resolution, diff/plan/apply/destroy
- Deploy engine with Service lifecycle and runtime state
- `tkr` CLI with infra/deploy/dev/build commands
- `tkr workstation` for AWS-backed remote Rust build workstations
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

## Backlog (priority order)

This is the living backlog. Items higher in the list are the next to pick up.
Items marked **spec complete** already have `requirements.md` (and often `design.md` + `tasks.md`) authored; the implementation is what's outstanding. Items marked **to spec** still need the requirements authored.

### P0 — `temporal-api-v1.62-sync`

**Status:** spec in progress (requirements drafted).

Resync the vendored Temporal API proto tree from `v1.43.0` to `v1.62.11` via `tools/proto-sync`. Classify every new RPC, field, message, and enum into one of `Ignore`, `No-op stub`, `Capability advertise`, `Wire through`, or `Full implementation (deferred)`. Delivers `CountSchedules`, `UpdateTaskQueueConfig`, the Nexus v2 wire surface, the `discard_speculative_workflow_task_with_events` client capability, and renames the `*ById` activity RPCs to their v1.62 unsuffixed names.

Unblocks: every subsequent backlog item that touches a v1.62-era proto surface. Compose-DSQL becomes operationally useful once a v0.4 SDK worker can complete a workflow against `tokeirad`.

### P1 — `temporal-compatibility`

**Status:** spec complete (requirements, design, tasks).

Temporal-server compatibility scope — the behaviours tokeirad must match beyond wire-compat, and the **version metadata tokeirad surfaces** to different consumers. Sits immediately after `temporal-api-v1.62-sync` because the sync establishes the Temporal API version tokeirad speaks (v1.62.11) and the SDK generation it targets (v0.4), and this spec is the one that exposes those facts in a consumable form to operators, SDKs, and downstream tooling (`GetSystemInfo.server_version`, `tkr` CLI reporting, README / CONTRIBUTING statements, operator-facing metrics labels). Details otherwise tracked in the spec itself.

Blocked on: `temporal-api-v1.62-sync` (the API version is the thing this spec surfaces).

### P2 — `edge-history-serializer-completeness`

**Status:** to spec.

Audit every `HistoryEvent.attributes` variant in `tokeira-edge::grpc::translate` where the translator currently falls back to `Default::default()` placeholders because the kernel event does not yet carry the full proto field set. For each placeholder, classify as: (a) kernel-available-but-unplumbed → wire through, (b) kernel-unavailable → surface kernel-side requirement and thread the field through kernel + runtime + edge, or (c) not-meaningful-in-tokeira's-model → document the rationale and mark the field as intentionally defaulted. Drive every placeholder to a decided resolution so SDKs that branch on history-event attributes see complete data.

Sits immediately after `temporal-compatibility` because it is the "tokeirad speaks completely on the wire" follow-on: compatibility surfaces server version, this surfaces history content.

Blocked on: `temporal-api-v1.62-sync` (some v1.62-added event attributes need decoding before the audit is complete).

### P3 — `observability-production`

**Status:** to spec.

Production-facing observability: export pipeline (Prometheus scrape, OTLP), OCC-conflict counters and retry histograms, migration events, connection-leak detection, DSQL-specific metrics (reservoir depth, rate-limiter tokens remaining, class-budget saturation), trace attributes surfacing the full gRPC → edge → runtime → kernel → storage path. Builds on top of the completed `observability-foundation` spec (metrics and tracing primitives) and fills in the application-level instrumentation that makes a production deployment legible.

The name matches `observability-foundation`'s full-word style, and distinguishes from the more narrowly-scoped `worker-heartbeat-observability` spec that appears lower in the backlog.

### P4 — `compose-dsql`

**Status:** spec complete (requirements).

Adds Aurora DSQL persistence to the compose platform (alongside the existing in-memory option). Covers: `DsqlModule` (managed + preexisting), `ComposeConfig.dsql` fields, endpoint writeback into `tokeirad.toml`, AWS credentials forwarded via the standard provider chain, `tkr schema setup|status|validate` wiring with build.rs-embedded migrations, two-phase infra apply lifecycle, tokeirad storage-backend selection via `infrastructure.storage`, visibility / projection wiring over `DsqlVisibilityStore`, rename of compose's `LocalStateModule` from `"remote-state"` to `"local-state"`.

Blocked on: `temporal-api-v1.62-sync` for the DSQL server to actually accept SDK traffic.

### P5 — `projection-batched-apply-and-failure-policies`

**Status:** to spec.

Two paired evolutions of the projection plane's interfaces. First: add `ProjectionSink::apply_batch(records: &[ProjectionRecord])` with a default implementation that calls `apply` N times, and provide a DSQL override using multi-row `VALUES (...)` inserts — the current `TODO(projection)` at `tokeira-projection/src/sink.rs:14`. Second: introduce `ProjectionWorker::FailurePolicy { retry_backoff, max_retries, dead_letter }` as per-sink configuration instead of the current single retry-only policy — the `TODO(projection)` at `tokeira-projection/src/worker.rs:66`.

Sits here because `compose-dsql` (P4) is the spec that first exercises the DSQL projection path at volume; the batched-apply optimisation pays off immediately after that spec lands.

Self-contained — no external blockers beyond `compose-dsql` providing a workload that makes the batching measurable.

### P6 — `worker-heartbeat-observability`

**Status:** to spec.

Absorb `RecordWorkerHeartbeat` from a no-op into durable observability: persist `WorkerHeartbeat` records with TTL, surface `ListWorkers` over the persisted data, expose derived metrics (polls/sec, slot occupancy, worker-fleet health), expire stale heartbeats on a sweep interval. Requires a storage migration (new `worker_heartbeat` table with `ttl_epoch` column for DSQL, in-memory mirror for local).

Blocked on: `temporal-api-v1.62-sync` (delivers the real `WorkerHeartbeat` proto surface that this spec consumes).

### P7 — `worker-config-management`

**Status:** to spec.

Server-backed worker configuration: `FetchWorkerConfig` and `UpdateWorkerConfig`. Lets operators push configuration changes (poller counts, slot sizes, rate limits) to workers without redeploying. Requires a new worker-config store (DSQL table + in-memory backend), versioning for optimistic concurrency on updates, and client plumbing that matches what the v0.4 SDK expects on startup and reconfig.

Blocked on: `temporal-api-v1.62-sync` (delivers the proto surface).

### P8 — `ecs-deployment`

**Status:** spec complete (requirements, design, tasks). Architecture doc 045 revised and review questions resolved.

Production ECS on EC2 deployment. Covers: custom `tokeira-autoscaler` reading Mimir, REPLICA services for edge / projection / control, DAEMON runtime with safe scale-in protocol (Loop C), private-only networking with VPC endpoints, instance scale-in protection for the runtime ASG, observability stack (Alloy sidecars + Mimir + Loki + Grafana) as ECS services, the controller-coordinated DSQL connection budget allocation via `ConnectionBudgetDirective`.

Blocked on: `observability-production` (needs production-grade metrics to drive the custom autoscaler).

### P9 — `runtime-broker-tiered-delivery`

**Status:** to spec.

Split the broker into explicit sticky / live / backlog tiers per the `TODO(perf)` at `tokeira-runtime/src/broker.rs:61`. The current broker uses a single structure; the design calls for explicit tiers so sticky (workflow-task-cached) pollers are served from one ring, live pollers from another, and backlog items drain through the slowest path. Delivers tier-selection logic per delivery, metrics for tier occupancy and transition rates, and preserves the existing `WorkflowTaskBroker` / `ActivityTaskBroker` external interfaces.

Sits after `ecs-deployment` because production deployments under real load are where the tier split pays off.

Self-contained — local to `tokeira-runtime/`, no storage or kernel changes.

### P10 — `kernel-pause-workflow`

**Status:** to spec.

First-class workflow-execution pause: `PauseWorkflowExecution` and `UnpauseWorkflowExecution`. Distinct from the v1.43-era activity-level pause-by-id surface. Requires a new kernel transition variant for paused state, timer scanner changes to skip paused workflows, dispatch-path routing to withhold tasks for paused executions, projection changes to surface paused state in visibility. Parallel to the existing `kernel-pause-activity-management` spec.

Blocked on: `temporal-api-v1.62-sync` (delivers the proto surface).

### P11 — `activity-executions-first-class`

**Status:** to spec.

Activities as first-class queryable objects, per the v1.52+ Temporal shift. Delivers: `StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`, `CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`, `DeleteActivityExecution`. Requires storage for standalone activities, projection/visibility extensions for activity records, runtime routing for activities outside a workflow context, kernel changes to handle activity executions as their own streams.

Blocked on: `temporal-api-v1.62-sync` (delivers the proto surface).

### P12 — `kernel-snapshot-suffix-recovery`

**Status:** to spec. Architecture doc 090-failover-and-recovery covers the design at high level.

Persist snapshot refs in the `workflow_hot` table so recovery can replay from snapshot + suffix instead of full history prefix. Delivers: new `LoadedRun::SnapshotRestore { snapshot_ref, after_event_id }` kernel variant, storage API additions to read/write snapshots (addresses `TODO(storage)` at `tokeira-storage/src/api.rs:595`), runtime changes so recovery picks snapshot+suffix when the snapshot is fresh enough, and a migration for the new `workflow_hot` columns.

Sits here because it is a performance / cold-start optimisation — nothing is broken without it, and the right time to prioritise is when production workloads are big enough that the recovery cost is measurable.

Self-contained. No external blockers beyond the pre-v0.4 SDK workflows being able to complete end-to-end against tokeirad.

### P13 — `worker-deployments`

**Status:** to spec.

Temporal's newer versioning primitive. Replaces Build ID versioning with named deployments, per-version ramping, task-queue routing by (namespace, task_queue) → version, and inherited-version workflows that keep running against their original version as new ones deploy. Delivers 11 RPCs (`DescribeWorker`, `ListWorkers`, `DescribeWorkerDeployment`, etc.), the full `temporal.api.deployment.v1` message package, new fields on `PollWorkflowTaskQueueResponse` / `RespondWorkflowTaskCompletedRequest` / `StartWorkflowExecutionRequest`, a new runtime broker for deployment/version state, a migration for deployment-metadata storage, and dispatch-path changes to pick a version per task-queue poll.

Also addresses the `DescribeTaskQueue` worker-version capabilities gap that was deferred previously — the version metadata required there flows from the worker-deployment router.

Blocked on: `worker-heartbeat-observability` (heartbeats carry the version a worker is serving, which feeds into the deployment router).

### P14 — `workflow-rules`

**Status:** to spec.

Server-side declarative policies that trigger actions on matching workflow executions. Delivers 5 RPCs (`CreateWorkflowRule`, `DescribeWorkflowRule`, `DeleteWorkflowRule`, `ListWorkflowRules`, `TriggerWorkflowRule`), the new `temporal.api.rules.v1` message package, a rule-evaluator background task in the runtime, projection integration to query matching workflows, kernel transitions for rule-triggered actions (terminate, reset, signal).

Self-contained feature. No blockers beyond `temporal-api-v1.62-sync`.

### P15 — `storage-archival-sweeps`

**Status:** to spec. Architecture doc 075 (future) covers the high-level design.

Two conjoined workstreams that have always been delivered together: (a) sweep-eligibility operations for activity tasks and archival eligibility (addresses `TODO(storage)` at `tokeira-storage/src/api.rs:258`) — defines what becomes eligible for archival vs. deletion; (b) archival to S3 — defines where eligible data goes for long-term history retention. Archive without sweep leaves orphan storage; sweep without archive loses data.

Sits last in the implementation chain because it only becomes relevant once DSQL storage is in production use at a scale where raw retention cost matters.

Blocked on: `compose-dsql` and `ecs-deployment` (archival only matters against a deployed DSQL backend running enough workload).

### P16 — `pipeline-foundation`

**Status:** spec complete (requirements, design, tasks).

Foundational CI/CD pipeline work. Details tracked in the spec itself.

## Known deferred items (not yet on the backlog)

Items explicitly deferred during implementation that are not yet
tracked as specs or backlog items:

### Infrastructure / operational

- **Runtime auto-tune** — Architecture doc 065 exists (draft). Local
  tuning of broker budgets, delivery fairness, projection pacing,
  commit reserves. No spec.
- **Admission control** — Architecture doc 055 exists but no spec or
  implementation. Edge `LongPollGate` for poll admission, per-namespace
  / per-task-queue / per-worker-identity limits, overload shedding with
  retryable semantics, broker budget tuning.
- **Shard placement and membership** — `shard-placement-membership`
  spec is complete (requirements, design, tasks) but implementation
  has not started. The work covers controller-managed live membership
  via gRPC streams, queue-home and execution-home placement, bundle
  lease management via DSQL, shard rebalancing, edge routing cache
  with `NotShardOwner` recovery, and controller-coordinated DSQL
  connection budget allocation. Currently single-node with in-memory
  shard ownership; production needs distributed shard assignment.
- **16 architecture docs have unresolved review questions** — Only
  045-autoscaling has resolved its review questions. The others
  represent open design decisions that will be consumed as each spec
  is authored.

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
