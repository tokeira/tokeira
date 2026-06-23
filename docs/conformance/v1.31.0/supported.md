# The Temporal v1.31.0 Surface (definition)

> Part of [the v1.31.0 conformance definition](./README.md). This page defines the **public API surface
> that conformance to Temporal v1.31.0 targets**, described entirely in Temporal's own terms: the RPCs
> and each feature's maturity as stated by Temporal (the v1.31.0 release notes and the `v1.62.8` proto).
> It is **definitional, not a status report** — it says what v1.31.0 *is*, not how much has been built.
> Measured progress is tracked separately in [`../../readiness/conformance.md`](../../readiness/conformance.md).
>
> Features Temporal labels **experimental / pre-release**, internal surfaces, and RPCs absent from
> v1.31.0 are in [`excluded.md`](./excluded.md). Surfaces still under decision are in
> [`decisions.md`](./decisions.md).

## What "conforming at the API level" means

API-level conformance to v1.31.0 means a Temporal SDK, operator, or tool drives the public gRPC surface
and observes the **same behaviour** Temporal server v1.31.0 produces for the same input lineage:

- the **same RPCs** are admitted (the surface below),
- with the **same request-field semantics, defaulting, and validation**,
- producing the **same `HistoryEvent` sequence** and the **same response shapes**,
- and the **same error codes / status mapping** on the failure paths.

RPC presence is necessary but not sufficient; **field-level** fidelity is part of the bar (see
[below](#field-level-fidelity-is-part-of-the-surface)). The authority for every behaviour question is
what v1.31.0 does, verified against ground truth — the `v1.62.8` proto for wire shape and the v1.31.0
server source for behaviour.

## The two public services

Temporal SDKs and operators use two public services. At API `v1.62.8` (the proto Temporal server
v1.31.0 ships) these total **121 RPCs**:

| Service | RPCs | Role |
|---------|-----:|------|
| `WorkflowService` | 109 | SDK/client surface: workflow lifecycle, tasks, activities, signals, queries, updates, schedules, visibility, batch, Nexus task transport, worker versioning, worker deployments. |
| `OperatorService` | 12 | Operator surface: search-attribute registration, namespace deletion, Nexus endpoint admin, remote-cluster registry. |

## Feature areas and their v1.31.0 maturity

Each area below is part of the v1.31.0 surface. The maturity column quotes Temporal's own designation for
v1.31.0 (release notes / proto). Areas Temporal labels experimental or pre-release are **not** here — see
[`excluded.md`](./excluded.md); areas under decision are in [`decisions.md`](./decisions.md).

| Feature area | Representative RPCs | v1.31.0 maturity (Temporal) |
|--------------|---------------------|-----------------------------|
| Workflow lifecycle | Start, SignalWithStart, RequestCancel, Terminate, Reset, Delete, ExecuteMultiOperation | GA |
| Workflow tasks | PollWorkflowTaskQueue, RespondWorkflowTaskCompleted/Failed, ResetStickyTaskQueue | GA |
| Activities (worker-dispatched) | PollActivityTaskQueue, RecordActivityTaskHeartbeat(/ById), RespondActivityTask{Completed,Failed,Canceled}(/ById) | GA |
| Signals | SignalWorkflowExecution | GA |
| Queries | QueryWorkflow, RespondQueryTaskCompleted | GA |
| Updates | UpdateWorkflowExecution, PollWorkflowExecutionUpdate | GA |
| Child & external workflows | (workflow commands: StartChild, SignalExternal, RequestCancelExternal) | GA |
| Workflow options | UpdateWorkflowExecutionOptions | GA |
| Schedules | Create/Describe/Update/Patch/Delete/List/CountSchedules, ListScheduleMatchingTimes | GA |
| Visibility | List/Count/ListOpen/ListClosed/ListArchivedWorkflowExecutions, GetSearchAttributes | GA |
| Search attributes (operator) | OperatorService Add/List/RemoveSearchAttributes | GA |
| Namespaces | Register/Describe/List/Update/DeprecateNamespace; OperatorService DeleteNamespace | GA |
| Batch operations | Start/Stop/Describe/ListBatchOperations | GA |
| Task queues | DescribeTaskQueue, ListTaskQueuePartitions, UpdateTaskQueueConfig | GA |
| Cluster / system metadata | GetClusterInfo, GetSystemInfo | GA |
| Worker inventory | RecordWorkerHeartbeat, ShutdownWorker, ListWorkers, DescribeWorker, Fetch/UpdateWorkerConfig | GA |
| Workflow rules | Create/Describe/Delete/List/TriggerWorkflowRule | GA |
| **Nexus** | PollNexusTaskQueue, RespondNexusTask{Completed,Failed}; OperatorService endpoint CRUD | **GA** — see [below](#nexus) |
| **Worker Deployments** | Describe/Delete/ListWorkerDeployment, SetWorkerDeploymentManager, Describe/Delete/Set{Current,Ramping}/UpdateMetadata Version | **GA** — see [below](#worker-deployments) |
| **Standalone Activities** | StartActivityExecution, Describe/Poll/List/Count/RequestCancel/Terminate/DeleteActivityExecution | **Public Preview** — see [below](#standalone-activities) |

## Nexus

The v1.31.0 release notes state Nexus is **GA**: the feature flag was removed and Nexus is "always
enabled… out of the box," with token-based callback routing by default. v1.31.0 also adds
schedule-to-start and start-to-close timeouts for Nexus operations and reworks the Nexus error model so
handler and operation errors carry their own messages.

The v1.31.0 Nexus surface spans:

1. **Endpoint administration** — `OperatorService` `Create/Get/Update/Delete/ListNexusEndpoint`.
2. **Task transport** — `PollNexusTaskQueue`, `RespondNexusTaskCompleted`, `RespondNexusTaskFailed`: an
   external worker polls a scheduled Nexus operation and routes its result back to the caller workflow.
3. **Operation lifecycle within workflows** — scheduling and resolving Nexus operations as workflow
   commands, including async completion-callback delivery.

## Worker Deployments

The v1.31.0 release notes state Worker Deployment APIs are **GA** ("now fully GA… users can rely on the
signature and behavior consistency"). The GA set:

`DescribeWorkerDeployment`, `DeleteWorkerDeployment`, `ListWorkerDeployments`,
`SetWorkerDeploymentManager`, `DescribeWorkerDeploymentVersion`, `DeleteWorkerDeploymentVersion`,
`SetWorkerDeploymentCurrentVersion`, `SetWorkerDeploymentRampingVersion`,
`UpdateWorkerDeploymentVersionMetadata`.

The four newer worker-deployment APIs that v1.31.0 labels experimental/pre-release
(`CreateWorkerDeployment`, `CreateWorkerDeploymentVersion`, `UpdateWorkerDeploymentVersionComputeConfig`,
`ValidateWorkerDeploymentVersionComputeConfig`) are in [`excluded.md`](./excluded.md). The older
build-ID / versioning-rules surface that v1.31.0 deprecates is under decision in
[`decisions.md`](./decisions.md).

## Standalone Activities

The v1.31.0 release notes state Standalone Activities are **in public preview** — activities that run
independently of workflows, gated by the `activity.enableStandalone` dynamic-config flag (off by default
in Temporal). v1.31.0 adds the `DeleteStandaloneActivity` capability, durability improvements
(request IDs preserved across restarts, standby task discard handler, removal of the 1-day retention
limit), and extends `PollActivityTaskQueueResponse` with fields needed by workers running without a
parent workflow (`currentAttemptScheduledTime`, `namespace`).

The RPC surface is `StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`,
`ListActivityExecutions`, `CountActivityExecutions`, `RequestCancelActivityExecution`,
`TerminateActivityExecution`, `DeleteActivityExecution`.

## Field-level fidelity is part of the surface

RPC presence is necessary but not sufficient. API-level conformance means each request/response **field**
behaves as v1.31.0 specifies — start reuse/conflict policy, completion callbacks, links, user metadata,
sticky/versioning metadata on workflow-task completion, `DescribeWorkflowExecution` pending-* population,
schedule timezone round-trip, update `update_ref`/`stage`, activity event linkage, batch options, and so
on. A field that is dropped or defaulted differently from v1.31.0 is a conformance gap even when the RPC
otherwise responds.

## Related pages

- [`excluded.md`](./excluded.md) — experimental/pre-release features, internal surfaces, and RPCs absent
  from v1.31.0, each with Temporal's own designation as the reason.
- [`decisions.md`](./decisions.md) — surfaces still under decision.
- [`../../readiness/conformance.md`](../../readiness/conformance.md) — measured progress toward this
  surface.
