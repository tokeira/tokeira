# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

Last updated: 2026-05-20

## Codebase snapshot

| Crate | Source lines | Tests | Status |
|-------|-------------|-------|--------|
| `tokeira-types` | ~2,400 | 45+ | Stable. Placement types, routing snapshot, deterministic mapping functions (blake3). |
| `tokeira-kernel` | 4,902 | 230 | Stable. All kernel features implemented. |
| `tokeira-storage` | ~10,500 | 130+ | Active. In-memory store + DSQL backend complete. LeaseRepository extensions (relinquish, list, node_endpoint), ControlRepository (generation CAS, budget allocation). |
| `tokeira-runtime` | ~21,000 | 220+ | Complete. All runtime features + membership stream client + two-phase drain protocol. |
| `tokeira-edge` | ~18,500 | 160+ | Active. All gRPC handlers + routing cache (ArcSwap) + NotShardOwner recovery. |
| `tokeira-proto` | ~500 | — | Stable. Protobuf codegen including controller service. |
| `tokeira-projection` | 4,602 | 34 | Active. Visibility sink, rollups, filter compilation, query service, DSQL store. |
| `tokeira-controller` | ~1,800 | 30+ | Complete. Active-active placement controller library (membership, placement, generation, drain, service, config). |
| `tokeira-autoscaler` | ~2,200 | 40+ | Complete. Autoscaler library (loops A/B/C, envelope, freshness, reconciler, signals, leader, mimir, actuator). |

Platform and tooling crates:

| Crate | Status |
|-------|--------|
| `tokeira-config` | Server config + generic TOML loader. |
| `tokeira-state` | CAS store + S3 state store for deployment state. |
| `tokeira-iac` | IaC engine: Module trait, diff/plan/apply/destroy. |
| `tokeira-deploy-engine` | Service lifecycle engine with image drift detection. |
| `tokeira-orchestrator` | Deployment orchestration facade. |
| `tokeira-compose` | Docker Compose provider (bollard). |
| `tokeira-aws` | AWS resource implementations (VPC, ECS, ASG, ALB, IAM, DSQL, OpenSearch, etc.). |
| `platforms/ecs` | ECS on EC2 platform: config, modules (networking, DSQL, cluster, observability, services), Ops trait. |
| `platforms/compose` | Docker Compose platform with DSQL module and observability stack. |
| `platforms/local` | Bare-process local platform. |

Apps: `tokeirad` (server), `tkr` (CLI), `tokeira-admin`, `tokeira-autoscaler`, `tokeira-controller`, `tokeira-bench`, `tokeira-replay`.

Total: ~85k lines of Rust, ~900+ tests.

## What has been built

### Kernel — complete

Pure deterministic state machine. All command variants handled.
Start, signal, WFT lifecycle, activities, timers, children,
external signals/cancels, nexus, updates, continue-as-new,
reset, pause/unpause, markers, execution options. 230 tests
(golden + property).

### Storage — complete

**In-memory store:** Full storage contract: OCC fencing, request
dedup, history append with pagination, activity/timer/nexus side
tables, dispatch backlog, projection log, lease management, shard
mapping, epoch validation, bundle relinquish, ControlRepository.

**DSQL backend:** All 7 features complete. 28+ migration files.
LeaseRepository extensions (list_bundle_leases, relinquish_bundle,
node_endpoint write path). ControlRepository (generation CAS,
budget allocation CAS). Connection reservoir + rate limiting.

### Runtime — complete

Lane executors, workflow + activity brokers, dispatch publisher,
all scanners, child orchestration, external signal/cancel delivery,
nexus dispatch, continue-as-new, OCC retry, recovery sweeper,
worker registry, versioning rule store, schedule store + execution
engine, batch operation store, shard-aware lane routing, membership
stream client, two-phase drain protocol, heartbeat data collection.

### Edge — complete

All WorkflowService gRPC handlers implemented including eager
dispatch. Routing cache with ArcSwap, NotShardOwner recovery with
redirect hints, execution-home and queue-home resolution.

### Projection — complete

Visibility sink, rollups, filter compilation, query service,
projection worker, and DSQL visibility store (full read and write
paths, typed search attributes, rollup accumulation).

### Placement controller — complete

Active-active placement controller (`crates/tokeira-controller/`):
membership tracking, routing snapshot computation, CAS-based
generation counter, desired placement directives, two-phase drain
coordination, connection budget allocation, gRPC service.

Controller binary (`apps/tokeira-controller/`): config loading,
DSQL connection, gRPC server, placement loop, budget loop,
graceful shutdown.

### Autoscaler — complete

Autoscaler library (`crates/tokeira-autoscaler/`): Loop A (REPLICA
scaling with hysteresis), Loop B (runtime scale-out with pressure
classification), Loop C (runtime retirement with drain phases),
connection-aware scaling envelope, metric freshness tracker,
desired-state reconciler, Mimir client, Actuator trait, DSQL
leader election.

Autoscaler binary (`apps/tokeira-autoscaler/`): config loading,
DSQL leader lease, control loop orchestration, action application.

### ECS deployment platform — substantially complete

`platforms/ecs/`: config model, 5 IaC modules (networking, DSQL,
cluster, observability, services), deploy-engine integration,
Ops trait (scale, logs, port-forward). CLI integration complete
(exec, admin, port-forward, scale, logs).

### Platform and deployment

- Local, Compose, and ECS platforms
- IaC engine with Module trait, dependency resolution, diff/plan/apply/destroy
- Deploy engine with Service lifecycle, image drift detection, runtime state
- `tkr` CLI: infra, deploy, dev, build, scale, logs, port-forward, exec, admin, schema, workstation, image
- Docker Compose provider via bollard with DSQL module

### Architecture documentation

22 architecture documents in `docs/architecture/`. Key accepted docs:
000-overview, 005-decisions, 010-history-as-authority, 015-configuration,
020-kernel, 025-system-services, 030-runtime-lanes, 040-delivery-broker,
050-dsql-storage, 060-connection-management.

### SDK examples working

hello_world, message_passing, continue_as_new, child_workflows,
timers, schedules — all running against tokeirad.

## Completion assessment

| Plane | Estimate | Notes |
|-------|----------|-------|
| Compatibility Edge | ~95% | All handlers + routing cache + NotShardOwner recovery. |
| Runtime & Storage | ~90% | Runtime complete. DSQL complete. Placement membership complete. |
| Projection | ~75% | Working against in-memory and DSQL. Batched apply pending. |
| Platform / Ops | ~60% | Local + Compose + ECS platforms. ECS has remaining test tasks. |
| **Overall** | **~75%** | Core correctness works end-to-end. Remaining work is feature specs + production hardening. |

## Backlog (priority order)

Outstanding work only. Items higher in the list are the next to pick up.

### P1 — `temporal-compatibility`

**Status:** spec complete (requirements, design, tasks). Ready for implementation.

Temporal-server compatibility scope — the behaviours tokeirad must match beyond wire-compat, and the version metadata tokeirad surfaces to different consumers (`GetSystemInfo.server_version`, `tkr` CLI reporting, operator-facing metrics labels).

### P2 — `observability-production`

**Status:** to spec.

Production-facing observability: export pipeline (Prometheus scrape, OTLP), OCC-conflict counters and retry histograms, migration events, connection-leak detection, DSQL-specific metrics (reservoir depth, rate-limiter tokens remaining, class-budget saturation), trace attributes surfacing the full gRPC → edge → runtime → kernel → storage path.

### P3 — `projection-batched-apply-and-failure-policies`

**Status:** to spec.

Batched projection sink (`apply_batch` with multi-row DSQL inserts) and per-sink failure policies (retry_backoff, max_retries, dead_letter).

### P4 — `worker-heartbeat-observability`

**Status:** to spec.

Durable worker heartbeat persistence with TTL, `ListWorkers` over persisted data, derived metrics (polls/sec, slot occupancy, worker-fleet health).

### P5 — `worker-config-management`

**Status:** to spec.

Server-backed worker configuration: `FetchWorkerConfig` and `UpdateWorkerConfig`. Lets operators push configuration changes to workers without redeploying.

### P6 — `ecs-deployment` (remaining test tasks)

**Status:** implementation substantially complete. Outstanding: property tests (1.6, 1.7, 1.9), checkpoint verifications, and unit test task 9.14.

### P7 — `runtime-broker-tiered-delivery`

**Status:** to spec.

Split the broker into explicit sticky / live / backlog tiers. Local to `tokeira-runtime/`.

### P8 — `kernel-pause-workflow`

**Status:** to spec.

First-class workflow-execution pause: `PauseWorkflowExecution` and `UnpauseWorkflowExecution`.

### P9 — `activity-executions-first-class`

**Status:** to spec.

Activities as first-class queryable objects (8 RPCs).

### P10 — `kernel-snapshot-suffix-recovery`

**Status:** to spec.

Persist snapshot refs for recovery from snapshot + suffix instead of full history prefix.

### P11 — `worker-deployments`

**Status:** to spec. Blocked on `worker-heartbeat-observability`.

Named deployments, per-version ramping, task-queue routing by version.

### P12 — `workflow-rules`

**Status:** to spec.

Server-side declarative policies (5 RPCs).

### P13 — `storage-archival-sweeps`

**Status:** to spec. Blocked on `ecs-deployment`.

Sweep-eligibility + archival to S3.

### P14 — `pipeline-foundation`

**Status:** spec complete (requirements, design, tasks).

Foundational CI/CD pipeline work.

## Known deferred items (not yet on the backlog)

- **Runtime auto-tune** — Architecture doc 065 (draft). No spec.
- **Admission control** — Architecture doc 055 exists. No spec.
- **Dynamic placement** — Architecture doc 037 (draft). Deferred from shard-placement-membership MVP.
- **16 architecture docs have unresolved review questions** — Only 045-autoscaling has resolved its review questions.

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
