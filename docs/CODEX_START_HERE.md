# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

Last updated: 2026-06-02

## Temporal compatibility target

Behaviour is matched against **Temporal server v1.31.0**
(`TEMPORAL_SERVER_COMPAT`), built against the vendored **API v1.62.11**
(`TEMPORAL_PROTO_VERSION`) — both pinned in
`crates/tokeira-build-info/src/pinned.rs`. For any API-behaviour question
(field semantics, error/status mapping, defaulting, lifecycle ordering),
the contract is whatever the targeted release does, verified against
`proto/upstream/` for wire shape and the Temporal server source at tag
`v1.31.0` for behaviour. See AGENTS.md §8. The two pins are independent and
tracked ahead on purpose; do not bump the server-compat claim just because
the vendored proto moved.

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

### Worker heartbeat observability — complete

In-memory heartbeat store (`InMemoryHeartbeatStore` in
`tokeira-runtime/src/heartbeat.rs`): DashMap-backed, last-write-wins,
monotonic `last_seen`, TTL eviction, capacity eviction, maintenance
loop with staleness sampling. Shared `HeartbeatStore` trait in
`tokeira-types`. Edge decoder from upstream proto. Handler migration
for `RecordWorkerHeartbeat` and `ShutdownWorker`. Metrics: accepted/
rejected counters, active state gauge, age histogram, entry counts.

### Commit fencing correctness — complete

Closed the TOCTOU race in `commit_transition_for_bundle` (epoch check
and write now atomic in same transaction/lock). Routed activity start
and retry through fenced commit. Fixed hard-coded shard count in
continue-as-new successor timeout routing. Split `run_repository.rs`
and `runtime.rs` into focused sub-modules along correctness boundaries.

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

### Scenario samples

`scenarios/` holds end-to-end samples that exercise Tokeira's distinctive
server-side machinery (not basic SDK usage, which the client SDK examples
cover). Each is a standalone Cargo project, excluded from the workspace,
building against the published Temporal Rust SDK like a downstream consumer.
First scenario: `scenarios/worker-versioning/` (versioned workers + driver
that sets current/ramping/promote and asserts the observed routing). The
worker/starter client code is complete; its routing assertions go green once
`worker-deployments` dispatch routing lands.

## In progress

### API conformance (umbrella) — active

`.kiro/specs/api-conformance-tracker/` tracks bringing the 121-RPC Temporal
v1.31.0 surface from Partial/Stubbed/Deferred to `Implemented`, split across
16 child specs. Landed so far: `DescribeWorkflowExecution` conformance (root
execution + cancel + pending state), `api-conformance-activity-by-id`
(Completed). Specs revised to the `Implemented` target against v1.62.11:
`api-conformance-start-fields`, `api-conformance-wft-completion`,
`api-conformance-workflow-describe`. The remaining child specs are at Spec
Draft. The tracker is the source of truth for per-RPC state.

### `worker-deployments` (P10) — active, ~1/3 implemented

Worker Deployment v2 surface (13 RPCs) plus ownership of worker-versioning
**routing application**. Landed: durable `WorkerDeploymentRepository`
(in-memory + DSQL, migration `V047`), per-run kernel versioning state
(`WorkflowVersioningInfo` + pure transitions), and the runtime
`DeploymentRegistry` scaffold with deployment/version CRUD and routing-config
selection (tasks 1, 2, 4.1–4.4). Remaining: registry CAS/manager/poller-
presence/compute-config/drainage guards (4.5–4.9), **dispatch routing
integration** (task 6 — target-version resolution, transition start at
task-start, activity-task transition rejection, apply-at-WFT-completion +
eager routing), the 13 edge handlers + adapter, the describe versioning
projection, and the compatibility-matrix flip. Three sibling api-conformance
specs persist/thread the versioning fields and defer their *application* to
this spec.

## Completion assessment

| Plane | Estimate | Notes |
|-------|----------|-------|
| Compatibility Edge | ~95% | All handlers + routing cache + NotShardOwner recovery. |
| Runtime & Storage | ~90% | Runtime complete. DSQL complete. Placement membership complete. |
| Projection | ~75% | Working against in-memory and DSQL. Batched apply pending. |
| Platform / Ops | ~60% | Local + Compose + ECS platforms. ECS has remaining test tasks. |
| **Overall** | **~78%** | Core correctness works end-to-end. Active fronts: api-conformance umbrella (per-RPC v1.31.0 fidelity) and `worker-deployments` (versioning routing). Remaining work is feature specs + production hardening. |

## Backlog (priority order)

Outstanding work only. Items higher in the list are the next to pick up.
The `api-conformance-*` umbrella and `worker-deployments` are active — see
the "In progress" section above.

### P1 — `temporal-compatibility`

**Status:** spec complete; initial implementation landed (compatibility
target pinned to v1.31.0 / v1.62.11, matrix + CLI scaffolding). Ongoing:
classify remaining RPC/field surfaces in the feature matrix as the
api-conformance child specs land.

Temporal-server compatibility scope — the behaviours tokeirad must match beyond wire-compat, and the version metadata tokeirad surfaces to different consumers (`GetSystemInfo.server_version`, `tkr` CLI reporting, operator-facing metrics labels).

### P2 — `observability-production`

**Status:** Phase 1 implementation landed. Remaining phases: full export
pipeline + remaining DSQL/trace metrics.

Production-facing observability: export pipeline (Prometheus scrape, OTLP), OCC-conflict counters and retry histograms, migration events, connection-leak detection, DSQL-specific metrics (reservoir depth, rate-limiter tokens remaining, class-budget saturation), trace attributes surfacing the full gRPC → edge → runtime → kernel → storage path.

### P3 — `projection-batched-apply-and-failure-policies`

**Status:** to spec.

Batched projection sink (`apply_batch` with multi-row DSQL inserts) and per-sink failure policies (retry_backoff, max_retries, dead_letter).

### P4 — `worker-config-management`

**Status:** to spec.

Server-backed worker configuration: `FetchWorkerConfig` and `UpdateWorkerConfig`. Lets operators push configuration changes to workers without redeploying.

### P5 — `ecs-deployment` (remaining test tasks)

**Status:** implementation substantially complete. Outstanding: property tests (1.6, 1.7, 1.9), checkpoint verifications, and unit test task 9.14.

### P6 — `runtime-broker-tiered-delivery`

**Status:** to spec.

Split the broker into explicit sticky / live / backlog tiers. Local to `tokeira-runtime/`.

### P7 — `kernel-pause-workflow`

**Status:** implemented (PauseWorkflowExecution / UnpauseWorkflowExecution,
v1.31.0 semantics). Remaining: any follow-on edge/projection surfacing.

First-class workflow-execution pause: `PauseWorkflowExecution` and `UnpauseWorkflowExecution`.

### P8 — `activity-executions-first-class`

**Status:** to spec.

Activities as first-class queryable objects (8 RPCs).

### P9 — `kernel-snapshot-suffix-recovery`

**Status:** to spec.

Persist snapshot refs for recovery from snapshot + suffix instead of full history prefix.

### P10 — `worker-deployments`

**Status:** active, ~1/3 implemented (storage + kernel state + registry CRUD
landed; dispatch routing, edge handlers, describe projection pending). See
the "In progress" section above.

Named deployments, per-version ramping, task-queue routing by version.

### P11 — `workflow-rules`

**Status:** to spec.

Server-side declarative policies (5 RPCs).

### P12 — `storage-archival-sweeps`

**Status:** to spec. Blocked on `ecs-deployment`.

Sweep-eligibility + archival to S3.

### P13 — `pipeline-foundation`

**Status:** spec complete (requirements, design, tasks).

Foundational CI/CD pipeline work.

## Known deferred items (not yet on the backlog)

- **Runtime auto-tune** — Architecture doc 065 (draft). No spec.
- **Admission control** — Architecture doc 055 exists. No spec.
- **Dynamic placement** — Architecture doc 037 (draft). Deferred from shard-placement-membership MVP.
- **16 architecture docs have unresolved review questions** — Only 045-autoscaling has resolved its review questions.

## Prospective features (under evaluation, not yet specced)

Three Temporal features evaluated against Tokeira's architecture. All target
Temporal v1.31.0+ and are at early release stages; none is yet a committed spec.
Verified against the Temporal docs (June 2026). Scoping captured here so the
in/out-of-scope reasoning is not lost.

### Standalone Activities — strong fit, highest value

**What it is (Public Preview; CLI v1.7.0 / Server v1.31.0):** a top-level
Activity Execution started directly by a client (`temporal activity
start|execute|result|list|count|describe|cancel|terminate`) with **no
Workflow**. A new *kind* of top-level execution with its **own ID space**,
separate from Workflows — addressable, retryable, heartbeatable, cancelable,
with conflict/reuse-policy dedup, priority/fairness, and visibility
(`ListActivities` / `CountActivities` / `DescribeActivity`). The same Activity
function runs standalone or inside a Workflow with no code changes.

**Fit:** squarely in scope and aligned with the architecture. "History is
authority" generalizes cleanly — a standalone activity is a top-level run whose
authoritative per-run transition log records schedule → dispatch → start →
heartbeat/checkpoint → result. DSQL persistence, lane execution, and the
dispatch broker all apply.

**Home:** the existing **`activity-executions-first-class` (P8)** placeholder is
exactly this. Its placeholder text already says "Activity Executions as
first-class durable objects ... eight Activity Executions RPCs ... kernel
representation of pending activities as durable, addressable objects." Promote
it to a full spec, framed as Standalone Activities (Temporal's job-queue
primitive).

**Hard part:** a *peer top-level execution kind* alongside workflow runs — its
own start/dedup over an activity ID space, its own visibility records, and a
`start_activity` / `execute_activity` / `get_activity_result` client surface.
Kernel + storage + edge + projection feature, not an edge shim. Distinct from
`api-conformance-activity-by-id` (Completed), which handled *workflow-scheduled*
activities resolved by `(namespace, workflow_id, run_id, activity_id)`. PP
limitations bound v1 scope: no pause/reset/update, no `TerminateExisting` /
`TerminateIfRunning`.

### Workflow Streams — no server work; conformance validation only

**What it is (Public Preview):** a **Python SDK `contrib` library**
(`temporalio.contrib.workflow_streams`), not a server feature. A durable,
offset-addressed event channel hosted inside a Workflow, built **entirely** on
existing primitives — batched **Signals** (publish), long-poll **Updates**
(subscribe), and a **Query** (head offset). Wire handlers are ordinary calls:
`__temporal_workflow_stream_publish` (Signal), `__temporal_workflow_stream_poll`
(Update), `__temporal_workflow_stream_offset` (Query). Cross-language client
support is roadmap; Python only today.

**Fit:** **nothing to implement server-side.** If Tokeira conforms on Signal /
Update (long-poll, `AcceptedUpdateCompletedWorkflow` surfacing,
`WorkflowUpdateFailedError` on CAN-handoff validator rejection) / Query and
Continue-As-New, the upstream Python library runs against tokeirad unmodified.
It exercises hard: Update long-poll semantics, ~1 MB poll-response caps,
per-Signal payload limits, and CAN offset carry-over.

**Recommendation:** **do not spec a feature.** Treat it as a conformance target.
The real surface is `api-conformance-update-lifecycle` (harden if needed). Add a
`scenarios/workflow-streams/` sample once Update long-poll conformance is solid,
proving the upstream Python contrib lib runs unmodified against tokeirad — high
signal, low cost, matches what `scenarios/` exists for.

### Serverless Workers — forward bet, large multi-spec epic, NOT a conformance obligation

**What it is (Pre-release; select Temporal Cloud customers; experimental APIs):**
Workers invoked on demand (AWS Lambda today) instead of running long-lived.
Temporal *invokes* the worker when tasks arrive; it polls, processes, exits.

**The mechanism — Worker Controller Instance (WCI):** a **system Workflow** that
scales serverless workers per Task Queue conditions. One WCI per
`(deployment_name, build_id)` Worker Deployment Version that has a **compute
provider** configured; runs in namespace division
`TemporalWorkerControllerInstance`; workflow ID
`temporal-sys-worker-controller-instance:<deployment>:<build-id>`. Triggers:
**sync-match failure** (Matching pushes a signal to the WCI when no worker is
available — primary, low-latency path) and **Task Queue backlog** (metadata
monitor). On trigger, the WCI runs an activity that calls the compute provider's
invoke API (e.g. AWS Lambda `InvokeFunction` via an assumed IAM role).

**Connection to `worker-deployments`:** this is the consumer of the
`ComputeConfig` / `ComputeConfigScalingGroup` already being persisted there.
worker-deployments deliberately persists + validates compute config
(`ValidateWorkerDeploymentVersionComputeConfig` →
`WorkerControllerInstanceClient.ValidateWorkerControllerInstanceSpec`, from
`go.temporal.io/auto-scaled-workers/wci/client`) **without** building the
controller that consumes it. Serverless Workers is that controller plus the
invocation path.

**Scope — a 3-layer dependency stack, bottom-up:**

```
worker-deployments (versions + ComputeConfig persistence)   ← in progress
  └── worker-controller-instance (controller + Matching sync-match-failure push + backlog monitor)
        └── serverless-workers-lambda (compute-provider invocation + worker packaging)
```

**Key scoping decisions / cautions:**

- **NOT part of the v1.31.0 compatibility claim.** The WCI lives in a *separate
  experimental module* (`go.temporal.io/auto-scaled-workers`), not core
  `temporalio/temporal` at tag v1.31.0, and is Pre-release with backwards-
  incompatible APIs expected. Building it is a forward *capability* bet, not a
  conformance obligation under AGENTS.md §8. It competes for roadmap space on
  product value, not on "match v1.31.0."
- **Model the WCI as a Tokeira control-plane controller, NOT a ported system
  Workflow.** Temporal implements it as a system workflow; porting that
  reintroduces the exact tension `worker-deployments` avoided (control-plane
  correctness weight on synthetic per-run history vs. "history is authority" for
  user runs + the pure kernel). Tokeira already has `tokeira-controller`
  (active-active placement) and `tokeira-autoscaler` (sync-match / backlog
  pressure loops); a WCI plausibly becomes an **autoscaler mode** — "scale
  serverless workers per deployment version by invoking a compute provider" —
  reusing that machinery.
- **New runtime wiring:** Matching/broker must emit a per-version scaling trigger
  on sync-match failure (the broker already knows poller presence via
  `WorkerRegistry`). The compute-provider invocation is a *runtime* AWS call
  (`tokeira-aws` territory, but invocation not IaC), not the small part.
- **Hard dependency:** gate on `worker-deployments` completing first.

**Recommendation:** defer. When picked up, spec bottom-up as two new features
(`worker-controller-instance`, then `serverless-workers-lambda`) with the
"controller not system workflow" stance decided up front.

### Firecracker worker compute (isolated ephemeral workers) — Tokeira-native compute provider

**Motivation (Temporal Compute Team direction):** "Isolation for untrusted /
dynamic code — we're exploring primitives like microVM-based sandboxes and
ephemeral workers to run workload-scoped compute safely"
([Temporal Compute Team](https://temporalio.notion.site/Join-Temporal-s-Compute-Team-598b6bca8bf84d3babf3e8e2a22c283c)).
Tokeira should provide **Firecracker-based worker compute** as a first-class
capability: per-invocation microVM sandboxes that run workload-scoped (and
potentially untrusted / dynamically supplied) Activity and Workflow code with
hardware-level isolation, scaled to zero between tasks.

**Relationship to Serverless Workers / WCI:** this is the **self-hosted compute
provider** counterpart to AWS Lambda in the Serverless Workers stack. Where
Temporal's reference design invokes Lambda, Tokeira can own the whole loop:
the WCI controller (above) reacts to sync-match-failure / backlog and invokes a
**Firecracker compute provider** that boots an ephemeral microVM worker, which
polls the version's task queue, processes, and exits. It slots into the same
`ComputeConfig` / compute-provider abstraction — Firecracker becomes a provider
type alongside Lambda, so most of the stack is shared.

**Why this matters for Tokeira specifically:**

- **Untrusted / dynamic code is the differentiator.** Lambda gives ephemerality
  and scale-to-zero; a microVM gives *isolation strong enough to run code you
  don't trust*. That is the capability the Compute Team direction calls out and
  the reason to own the compute layer rather than only integrate a cloud
  provider. AI-agent / dynamic-tool workloads (run arbitrary generated code as
  an Activity) are the obvious driver.
- **Tokeira already owns its deployment substrate.** Unlike the Lambda path
  (Tokeira-as-client of a cloud API), Firecracker compute is Tokeira-as-host:
  microVM lifecycle, snapshotting for fast cold-start, jailer/cgroup
  confinement, and a per-VM Temporal client. This is new infrastructure
  (`tokeira-aws` does not cover bare microVM orchestration), likely a new
  platform/runtime crate.

**Scope and cautions:**

- **Dependency:** sits on top of the same stack as Serverless Workers —
  `worker-deployments` (ComputeConfig) → `worker-controller-instance` (WCI
  trigger + provider abstraction) → a `compute-provider-firecracker`
  implementation peer to `serverless-workers-lambda`. Build the provider
  abstraction once; Lambda and Firecracker are two implementations of it.
- **Not a Temporal-conformance feature.** Like Serverless Workers, this is a
  forward capability bet outside the v1.31.0 compatibility claim. Its value is
  product differentiation (safe untrusted compute), not API parity.
- **Security is the whole point — design it in, not on.** microVM boundary,
  no host filesystem/network egress by default, per-workload credential scoping,
  snapshot provenance, and resource caps are first-class requirements, not
  hardening passes. Firecracker + jailer is the baseline; the threat model is
  "the workload code is hostile."
- **Heavy infra investment.** microVM image build/snapshot pipeline, a host
  agent that boots/pools/reaps VMs, fast-restore for cold-start latency, and
  observability across ephemeral instances. This is a large, multi-quarter
  effort — capture the intent now; do not start until the WCI/provider
  abstraction exists.

**Recommendation:** record as a strategic direction. When the Serverless Workers
stack is specced, define the compute-provider abstraction so Firecracker is a
clean second implementation rather than a parallel stack. Spec
`compute-provider-firecracker` only after `worker-controller-instance` lands.

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
