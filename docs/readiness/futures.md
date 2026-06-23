# Futures — outstanding and prospective work

> A holding doc for forward-looking work: the outstanding feature backlog, deferred items, and
> prospective features under evaluation. Migrated from the retired `docs/CODEX_START_HERE.md`.
> **Statuses are as of the source snapshot (2026-06-02) and need triage** — treat priorities and
> "status" lines as inputs to triage, not current truth. Conformance progress is tracked separately in
> [`conformance.md`](./conformance.md); release/delivery status in [`delivery.md`](./delivery.md).

## Backlog (priority order)

Outstanding feature work, each item mapping to a spec under `.kiro/specs/`. Higher items were the next
to pick up at snapshot time. The `api-conformance-*` umbrella and `worker-deployments` were active.

### P1 — `temporal-compatibility`

Spec complete; initial implementation landed (compatibility target pinned to v1.31.0 / v1.62.11, matrix +
CLI scaffolding). Ongoing: classify remaining RPC/field surfaces in the feature matrix as the
api-conformance child specs land. Scope: the behaviours tokeirad must match beyond wire-compat, and the
version metadata it surfaces (`GetSystemInfo.server_version`, `tkr` CLI, operator metrics labels).

### P2 — `observability-production`

Phase 1 landed. Remaining: full export pipeline (Prometheus scrape, OTLP), OCC-conflict counters and
retry histograms, migration events, connection-leak detection, DSQL-specific metrics (reservoir depth,
rate-limiter tokens, class-budget saturation), and trace attributes across gRPC → edge → runtime →
kernel → storage.

### P3 — `projection-batched-apply-and-failure-policies`

To spec. Batched projection sink (`apply_batch` with multi-row DSQL inserts) and per-sink failure
policies (retry_backoff, max_retries, dead_letter).

### P4 — `worker-config-management`

To spec. Server-backed worker configuration: `FetchWorkerConfig` and `UpdateWorkerConfig`. Push config
to workers without redeploying.

### P5 — `ecs-deployment` (remaining test tasks)

Implementation substantially complete. Outstanding: property tests (1.6, 1.7, 1.9), checkpoint
verifications, and unit test task 9.14.

### P6 — `runtime-broker-tiered-delivery`

Specced — ready for implementation (`.kiro/specs/runtime-broker-tiered-delivery/`). Split the broker into
explicit sticky / live / backlog tiers. Local to `tokeira-runtime/` (+ `tokeira-edge` for poll/response
wiring). Priority first slice: deliver queries to quiescent workflows on the `PollWorkflowTaskQueue`
path — today a query to an idle workflow is stranded in the broker's separate `query_ready` channel
(drained only by `poll_query_task`, which the Temporal SDK never calls), so it times out. This blocks the
`agentic-orchestration` OpenAI sandbox sample. Ground-truthed to `queryworkflow/api.go` +
`matching_engine.go` @ v1.31.0. The related update-redelivery defect is already fixed (commit `2565975`).

### P7 — `kernel-pause-workflow`

Implemented (PauseWorkflowExecution / UnpauseWorkflowExecution, v1.31.0 semantics). Remaining: any
follow-on edge/projection surfacing. (Note: Temporal labels these experimental — see
[`../conformance/v1.31.0/excluded.md`](../conformance/v1.31.0/excluded.md).)

### P8 — `activity-executions-first-class`

To spec. Activities as first-class queryable objects (8 RPCs). This is the home for Standalone Activities
(see [Prospective features](#prospective-features-under-evaluation) below).

### P9 — `kernel-snapshot-suffix-recovery`

To spec. Persist snapshot refs for recovery from snapshot + suffix instead of full history prefix.

### P10 — `worker-deployments`

Active, ~1/3 implemented (storage + kernel state + registry CRUD landed; dispatch routing, edge handlers,
describe projection pending). Named deployments, per-version ramping, task-queue routing by version.

### P11 — `workflow-rules`

To spec. Server-side declarative policies (5 RPCs).

### P12 — `storage-archival-sweeps`

To spec. Blocked on `ecs-deployment`. Sweep-eligibility + archival to S3.

### P13 — `pipeline-foundation`

Spec complete (requirements, design, tasks). Foundational CI/CD pipeline work.

## Known deferred items (not yet on the backlog)

- **Runtime auto-tune** — Architecture doc 065 (draft). No spec.
- **Admission control** — Architecture doc 055 exists. No spec.
- **Dynamic placement** — Architecture doc 037 (draft). Deferred from shard-placement-membership MVP.
- **16 architecture docs have unresolved review questions** — only 045-autoscaling has resolved its
  review questions.

## Prospective features (under evaluation)

Temporal features evaluated against Tokeira's architecture. All target Temporal v1.31.0+ and were at
early release stages at snapshot time; none is yet a committed spec. Scoping captured so the
in/out-of-scope reasoning is not lost.

### Standalone Activities — strong fit, highest value

**What it is (Public Preview; Server v1.31.0):** a top-level Activity Execution started directly by a
client with **no Workflow** — a new *kind* of top-level execution with its **own ID space**, separate
from Workflows: addressable, retryable, heartbeatable, cancelable, with conflict/reuse-policy dedup,
priority/fairness, and visibility (`ListActivities` / `CountActivities` / `DescribeActivity`). The same
Activity function runs standalone or inside a Workflow with no code changes. (The API surface is now
reflected in the conformance definition as Public Preview — see
[`../conformance/v1.31.0/supported.md`](../conformance/v1.31.0/supported.md).)

**Fit:** squarely in scope and aligned with the architecture. "History is authority" generalizes cleanly
— a standalone activity is a top-level run whose authoritative per-run transition log records schedule →
dispatch → start → heartbeat/checkpoint → result. DSQL persistence, lane execution, and the dispatch
broker all apply.

**Home:** the existing **`activity-executions-first-class` (P8)** placeholder is exactly this. Promote it
to a full spec, framed as Standalone Activities (Temporal's job-queue primitive).

**Hard part:** a *peer top-level execution kind* alongside workflow runs — its own start/dedup over an
activity ID space, its own visibility records, and a `start_activity` / `execute_activity` /
`get_activity_result` client surface. Kernel + storage + edge + projection feature, not an edge shim.
Distinct from `api-conformance-activity-by-id` (Completed), which handled *workflow-scheduled* activities
resolved by `(namespace, workflow_id, run_id, activity_id)`. Public-preview limitations bound v1 scope:
no pause/reset/update, no `TerminateExisting` / `TerminateIfRunning`.

### Workflow Streams — no server work; conformance validation only

**What it is (Public Preview):** a **Python SDK `contrib` library**
(`temporalio.contrib.workflow_streams`), not a server feature. A durable, offset-addressed event channel
hosted inside a Workflow, built **entirely** on existing primitives — batched **Signals** (publish),
long-poll **Updates** (subscribe), and a **Query** (head offset). Wire handlers are ordinary calls:
`__temporal_workflow_stream_publish` (Signal), `__temporal_workflow_stream_poll` (Update),
`__temporal_workflow_stream_offset` (Query). Cross-language client support is roadmap; Python only today.

**Fit:** **nothing to implement server-side.** If Tokeira conforms on Signal / Update (long-poll,
`AcceptedUpdateCompletedWorkflow` surfacing, `WorkflowUpdateFailedError` on CAN-handoff validator
rejection) / Query and Continue-As-New, the upstream Python library runs against tokeirad unmodified. It
exercises hard: Update long-poll semantics, ~1 MB poll-response caps, per-Signal payload limits, and CAN
offset carry-over.

**Recommendation:** **do not spec a feature.** Treat it as a conformance target; harden
`api-conformance-update-lifecycle` if needed. Add a `scenarios/workflow-streams/` sample once Update
long-poll conformance is solid, proving the upstream Python contrib lib runs unmodified — high signal,
low cost.

### Serverless Workers — forward bet, large multi-spec epic, NOT a conformance obligation

**What it is (Pre-release; experimental APIs):** Workers invoked on demand (AWS Lambda today) instead of
running long-lived. Temporal *invokes* the worker when tasks arrive; it polls, processes, exits.

**The mechanism — Worker Controller Instance (WCI):** a **system Workflow** that scales serverless workers
per Task Queue conditions. One WCI per `(deployment_name, build_id)` Worker Deployment Version that has a
**compute provider** configured. Triggers: **sync-match failure** (Matching pushes a signal to the WCI
when no worker is available — primary, low-latency path) and **Task Queue backlog** (metadata monitor).
On trigger, the WCI runs an activity that calls the compute provider's invoke API (e.g. AWS Lambda
`InvokeFunction` via an assumed IAM role).

**Connection to `worker-deployments`:** this is the consumer of the `ComputeConfig` /
`ComputeConfigScalingGroup` already being persisted there. worker-deployments deliberately persists +
validates compute config (`ValidateWorkerDeploymentVersionComputeConfig`) **without** building the
controller that consumes it. Serverless Workers is that controller plus the invocation path.

**Scope — a 3-layer dependency stack, bottom-up:**

```
worker-deployments (versions + ComputeConfig persistence)   ← in progress
  └── worker-controller-instance (controller + Matching sync-match-failure push + backlog monitor)
        └── serverless-workers-lambda (compute-provider invocation + worker packaging)
```

**Key scoping decisions / cautions:**

- **NOT part of the v1.31.0 compatibility surface.** The WCI lives in a *separate experimental module*
  (`go.temporal.io/auto-scaled-workers`), not core `temporalio/temporal` at tag v1.31.0, and is
  Pre-release with backwards-incompatible APIs expected. A forward *capability* bet, not a conformance
  obligation under AGENTS.md §8.
- **Model the WCI as a Tokeira control-plane controller, NOT a ported system Workflow.** Porting the
  system-workflow design reintroduces the exact tension `worker-deployments` avoided (control-plane
  correctness weight on synthetic per-run history vs. "history is authority"). Tokeira already has
  `tokeira-controller` and `tokeira-autoscaler`; a WCI plausibly becomes an **autoscaler mode**.
- **New runtime wiring:** Matching/broker must emit a per-version scaling trigger on sync-match failure
  (the broker already knows poller presence via `WorkerRegistry`). The compute-provider invocation is a
  *runtime* AWS call (`tokeira-aws` territory, but invocation not IaC).
- **Hard dependency:** gate on `worker-deployments` completing first.

**Recommendation:** defer. When picked up, spec bottom-up as two new features
(`worker-controller-instance`, then `serverless-workers-lambda`) with the "controller not system
workflow" stance decided up front.

### Firecracker worker compute (isolated ephemeral workers) — Tokeira-native compute provider

**Motivation (Temporal Compute Team direction):** isolation for untrusted / dynamic code via
microVM-based sandboxes and ephemeral workers to run workload-scoped compute safely. Tokeira should
provide **Firecracker-based worker compute** as a first-class capability: per-invocation microVM
sandboxes that run workload-scoped (and potentially untrusted) Activity and Workflow code with
hardware-level isolation, scaled to zero between tasks.

**Relationship to Serverless Workers / WCI:** this is the **self-hosted compute provider** counterpart to
AWS Lambda in the Serverless Workers stack. The WCI controller reacts to sync-match-failure / backlog and
invokes a **Firecracker compute provider** that boots an ephemeral microVM worker, which polls the
version's task queue, processes, and exits. It slots into the same `ComputeConfig` / compute-provider
abstraction — Firecracker becomes a provider type alongside Lambda.

**Why this matters for Tokeira specifically:**

- **Untrusted / dynamic code is the differentiator.** Lambda gives ephemerality and scale-to-zero; a
  microVM gives *isolation strong enough to run code you don't trust*. AI-agent / dynamic-tool workloads
  (run arbitrary generated code as an Activity) are the obvious driver — directly relevant to Odori.
- **Tokeira already owns its deployment substrate.** Firecracker compute is Tokeira-as-host: microVM
  lifecycle, snapshotting for fast cold-start, jailer/cgroup confinement, and a per-VM Temporal client.
  New infrastructure (`tokeira-aws` does not cover bare microVM orchestration), likely a new
  platform/runtime crate.

**Scope and cautions:**

- **Dependency:** sits on the same stack as Serverless Workers — `worker-deployments` (ComputeConfig) →
  `worker-controller-instance` (WCI trigger + provider abstraction) → a `compute-provider-firecracker`
  implementation peer to `serverless-workers-lambda`. Build the provider abstraction once; Lambda and
  Firecracker are two implementations of it.
- **Not a Temporal-conformance feature.** A forward capability bet outside the v1.31.0 compatibility
  surface. Its value is product differentiation (safe untrusted compute), not API parity.
- **Security is the whole point — design it in, not on.** microVM boundary, no host filesystem/network
  egress by default, per-workload credential scoping, snapshot provenance, and resource caps are
  first-class requirements. Firecracker + jailer is the baseline; the threat model is "the workload code
  is hostile."
- **Heavy infra investment.** microVM image build/snapshot pipeline, a host agent that boots/pools/reaps
  VMs, fast-restore for cold-start latency, and observability across ephemeral instances. A large,
  multi-quarter effort.

**Recommendation:** record as a strategic direction. When the Serverless Workers stack is specced, define
the compute-provider abstraction so Firecracker is a clean second implementation. Spec
`compute-provider-firecracker` only after `worker-controller-instance` lands.
