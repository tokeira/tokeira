# In-Scope Surface — Temporal v1.31.0 (definition)

> Part of [the v1.31.0 compliance definition](./README.md). This page defines the **public API surface
> that constitutes the v1.31.0 conformance claim** — the *denominator*: what tokeira commits to making
> behave like Temporal v1.31.0. It is **definitional, not a status report.** "In scope" means "part of
> the claimed surface (the target)", not "achieved". For measured per-area progress — the *numerator* —
> see [`../../readiness/conformance.md`](../../readiness/conformance.md).
>
> The companion [`excluded.md`](./excluded.md) defines what is deliberately **out** of the claim, with
> reasons. The kernel-level command/event mapping that backs the workflow state machine is in
> [`command-surface.md`](./command-surface.md).

## What "fully conforming at the API level" means

Tokeira conforms at the API level when a Temporal **SDK, operator, or tool** can drive tokeira over the
public gRPC surface and observe the **same behaviour** Temporal server v1.31.0 would produce for the same
input lineage:

- the **same RPCs** are admitted (the surface below),
- with the **same request-field semantics, defaulting, and validation**,
- producing the **same `HistoryEvent` sequence** and the **same response shapes**,
- and the **same error codes / status mapping** on the failure paths.

The contract is whatever **v1.31.0** does, verified against ground truth (`proto/upstream/` for wire
shape; the v1.31.0 server source for behaviour) — never memory, SDK docs, or a newer release. A green
check in tokeira's own code is not conformance; a measured match against v1.31.0 ground truth is.

## The surface, by service

The claim covers the two public services Temporal SDKs and operators use. At API `v1.62.8` (the proto
Temporal server v1.31.0 ships) these total **121 RPCs**:

| Service | RPCs | Role |
|---------|-----:|------|
| `WorkflowService` | 109 | The SDK/client surface: workflow lifecycle, tasks, activities, signals, queries, updates, schedules, visibility, batch, Nexus task transport, worker versioning. |
| `OperatorService` | 12 | Operator surface: search-attribute registration, namespace deletion, Nexus endpoint admin, remote-cluster registry. |

> Tokeira's vendored proto is `v1.62.11`, intentionally ahead of `v1.62.8`. The 8 extra
> Nexus **operation-execution** RPCs that `v1.62.11` adds are **not** part of the v1.31.0 claim — they
> are tracked ahead and listed in [`excluded.md`](./excluded.md#rpcs-only-in-the-vendored-proto-tracked-ahead).

## The surface, by feature area

Each area is in scope of the v1.31.0 claim. The "Kernel weight" column flags whether the area drives the
authoritative transition log (see [`command-surface.md`](./command-surface.md)) or is an edge/runtime
concern. Measured achievement per area lives in [`../../readiness/conformance.md`](../../readiness/conformance.md).

| # | Feature area | Representative RPCs | Kernel weight |
|---|--------------|---------------------|:-------------:|
| 1 | **Workflow lifecycle** | Start, SignalWithStart, RequestCancel, Terminate, Reset, Delete | yes |
| 2 | **Workflow tasks** | PollWorkflowTaskQueue, RespondWorkflowTaskCompleted/Failed, ResetStickyTaskQueue | yes |
| 3 | **Activities (worker-dispatched)** | PollActivityTaskQueue, RecordActivityTaskHeartbeat(/ById), RespondActivityTask{Completed,Failed,Canceled}(/ById) | yes |
| 4 | **Standalone Activities** (first-class) | StartActivityExecution, Describe/Poll/List/CountActivityExecution, RequestCancel/Terminate/DeleteActivityExecution | yes — see [below](#standalone-activities) |
| 5 | **Timers, signals** | (timers are workflow commands); SignalWorkflowExecution | yes |
| 6 | **Queries** | QueryWorkflow, RespondQueryTaskCompleted | runtime |
| 7 | **Updates** | UpdateWorkflowExecution, PollWorkflowExecutionUpdate | yes |
| 8 | **Child & external workflows** | (StartChild / SignalExternal / RequestCancelExternal are workflow commands) | yes |
| 9 | **Nexus** | PollNexusTaskQueue, RespondNexusTask{Completed,Failed}; OperatorService endpoint CRUD | yes — see [below](#nexus) |
| 10 | **Schedules** | Create/Describe/Update/Patch/Delete/List/CountSchedules, ListScheduleMatchingTimes | runtime |
| 11 | **Visibility** | List/Count/ListOpen/ListClosed/ListArchived/ScanWorkflowExecutions, GetSearchAttributes | projection |
| 12 | **Search attributes (operator)** | OperatorService Add/List/RemoveSearchAttributes | projection |
| 13 | **Namespaces** | Register/Describe/List/Update/DeprecateNamespace; OperatorService DeleteNamespace | edge |
| 14 | **Batch operations** | Start/Stop/Describe/ListBatchOperations | runtime |
| 15 | **Task queues** | DescribeTaskQueue, UpdateTaskQueueConfig, ListTaskQueuePartitions | runtime |
| 16 | **Worker versioning (v2 rules)** | Update/GetWorkerVersioningRules, GetWorkerTaskReachability | runtime |
| 17 | **Worker deployments / inventory** | WorkerDeployment* family, RecordWorkerHeartbeat, ShutdownWorker, List/DescribeWorker | runtime |
| 18 | **Workflow rules** | Create/Describe/Delete/List/TriggerWorkflowRule | runtime |
| 19 | **Workflow options** | UpdateWorkflowExecutionOptions | yes |
| 20 | **Multi-operation** | ExecuteMultiOperation | runtime (composes kernel primitives) |
| 21 | **Cluster / system metadata** | GetClusterInfo, GetSystemInfo | edge |

## Standalone Activities

Temporal v1.31.0's API surface includes **first-class (standalone) activity executions** — activities
that exist as top-level entities rather than only as children of a workflow. These are the
`StartActivityExecution` / `Describe` / `Poll` / `List` / `Count` / `RequestCancel` / `Terminate` /
`DeleteActivityExecution` RPCs.

They are **in scope** of the v1.31.0 claim because they are part of the v1.62.8 surface that server
v1.31.0 ships. They are not yet implemented in tokeira (the edge defers them via the
`activity-executions-first-class` spec), and they are the C1 cluster in the functional corpus. Their
measured state lives in [`../../readiness/conformance.md`](../../readiness/conformance.md) — here we only
record that **full conformance requires them**.

The deprecated by-ID activity-control aliases that overlap this area (`UpdateActivityOptions`,
`PauseActivity`, `UnpauseActivity`, `ResetActivity` as standalone deprecated RPCs) are covered under the
worker-dispatched activity area; the non-deprecated control verbs are part of the standalone surface.

## Nexus

Nexus is **in scope**, across three layers:

1. **Endpoint administration** — `OperatorService` `Create/Get/Update/Delete/ListNexusEndpoint`. A live,
   store-backed endpoint registry (worker-targeted and external-URL targets). This is the C4a surface.
2. **Task transport** — `PollNexusTaskQueue`, `RespondNexusTaskCompleted`, `RespondNexusTaskFailed`: an
   external worker polls a scheduled Nexus operation and routes its result back to the caller workflow,
   across namespaces. This is the C4b transport surface (round-trip proven end-to-end).
3. **Async completion delivery** — completion-callback delivery for `WorkflowRunOperation`-style async
   Nexus operations (the `nexus-async-completion` spec). Required for durable async Nexus operations.

**In scope but not yet behaviourally measured:** the full Nexus operation lifecycle against the corpus
(`TestNexusApiTestSuite*`, `TestNexusWorkflowTestSuite`). **Out of scope:** cross-*cluster* Nexus routing
(deferred to multi-cluster work) and the `v1.62.11`-only Nexus operation-execution RPCs — see
[`excluded.md`](./excluded.md).

## Field-level fidelity is part of the claim

RPC presence is necessary but not sufficient. Full API conformance means each request/response **field**
behaves as v1.31.0 specifies. Known field-level gaps (start reuse/conflict policy, completion callbacks,
links, user metadata, sticky/versioning metadata on WFT completion, describe pending-* population,
schedule timezone round-trip, update `update_ref`/`stage`, activity event linkage, batch options) are
catalogued in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` and are part of the denominator: a field that
is dropped or defaulted differently from v1.31.0 is a conformance gap even when the RPC "works".

## Where the rest of the definition lives

- [`command-surface.md`](./command-surface.md) — the kernel-level command set and history-event surface
  that realises the workflow state machine (the engine-core half of the definition).
- [`excluded.md`](./excluded.md) — what is deliberately out of the claim, with reasons.
- [`../../readiness/conformance.md`](../../readiness/conformance.md) — measured progress toward this
  surface (the numerator). This page never asserts achievement; that doc does.
