# Outside the v1.31.0 Conformance Surface — exclusions and reasons

> Part of [the v1.31.0 conformance definition](./README.md). This page lists what is **outside** the
> conformance surface defined in [`supported.md`](./supported.md), with the reason for each. The reasons
> are factual: a feature Temporal labels **experimental / pre-release**, a surface that is **internal**
> (not part of the public SDK/operator API), or an RPC **absent from v1.31.0**. It is **definitional, not
> a status report**.
>
> Surfaces that are present in v1.31.0 but still **under decision** (authentication) are not here —
> they are in [`decisions.md`](./decisions.md).

## 1. Experimental / pre-release features (Temporal-designated)

v1.31.0 ships these but labels them experimental or pre-release — their signatures and behaviour may
change. They are outside the conformance surface for that reason.

| Surface | Temporal v1.31.0 designation |
|---------|------------------------------|
| `PauseWorkflowExecution`, `UnpauseWorkflowExecution` | "experimental API… behavior may change" (proto) |
| Worker Deployment `CreateWorkerDeployment`, `CreateWorkerDeploymentVersion`, `UpdateWorkerDeploymentVersionComputeConfig`, `ValidateWorkerDeploymentVersionComputeConfig` | "Pre-Release… considered experimental and may see breaking changes" (release notes) |
| Serverless Workers / Worker Controller Instance (WCI) | "pre release"; a server component (`workercontroller.enabled`), disabled by default |
| Custom history/visibility archiver factories | "experimental" server options |

## 2. Internal surfaces (not part of the public API)

These are Temporal-internal RPC boundaries and operational tooling, not part of the public
`WorkflowService` / `OperatorService` surface an SDK or operator targets:

- **`AdminService`** in its entirety — cluster/shard/membership administration, DLQ management, history
  manipulation, internal task add/list.
- **`HistoryService` / `MatchingService` driven directly** — internal boundaries between Temporal server
  roles.
- **Dead-letter-queue (DLQ) management** — `GetDLQMessages`, `PurgeDLQMessages`, `MergeDLQMessages` and
  the replication DLQ.
- **Multi-cluster replication, failover, and remote task routing** — the replication *behaviour* behind
  the remote-cluster registry. (The `OperatorService` remote-cluster registry RPCs themselves —
  `AddOrUpdateRemoteCluster`, `RemoveRemoteCluster`, `ListClusters` — exist as metadata CRUD but do not
  imply replication.)
- **CHASM framework internals** — enabled in v1.31.0 but applications atop it are off by default;
  internal to the engine, not a public API.
- **Persistence / test-only inspection** used by Temporal's own test base.

Read-only and delivery-layer RPCs (e.g. `QueryWorkflow`, `DescribeWorkflowExecution`,
`RecordActivityTaskStarted`) are **not** excluded by being read-only — they are part of the public
surface and belong to [`supported.md`](./supported.md). Exclusion here is about features outside the
public conformance surface, not about whether an RPC mutates state.

## 3. RPCs absent from v1.31.0

The vendored proto (`v1.62.11`) is newer than the proto Temporal server v1.31.0 ships (`v1.62.8`). RPCs
present only in the newer proto are not part of v1.31.0. Today these are the 8 Nexus
**operation-execution** RPCs:

`StartNexusOperationExecution`, `DescribeNexusOperationExecution`, `PollNexusOperationExecution`,
`ListNexusOperationExecutions`, `CountNexusOperationExecutions`, `RequestCancelNexusOperationExecution`,
`TerminateNexusOperationExecution`, `DeleteNexusOperationExecution`.

They are absent from `v1.62.8`, so they are outside the v1.31.0 surface regardless of maturity. (See the
README's two-pins note: the proto version and the targeted server version move independently.)

## 4. Deprecated surfaces

v1.31.0 ships these but marks them **deprecated**; the GA replacements are in [`supported.md`](./supported.md).

| Surface | Replacement in v1.31.0 |
|---------|------------------------|
| Deployment v0 — `DescribeDeployment`, `ListDeployments`, `GetDeploymentReachability`, `GetCurrentDeployment`, `SetCurrentDeployment` | GA Worker Deployments |
| Activity-control aliases — `PauseActivity`, `UnpauseActivity`, `ResetActivity` | the standalone-activity control verbs |
| Worker Versioning V1/V2 **enabled-path semantics** — version sets, versioning rules, rule-computed reachability behind `UpdateWorkerBuildIdCompatibility`, `GetWorkerBuildIdCompatibility`, `UpdateWorkerVersioningRules`, `GetWorkerVersioningRules`, `GetWorkerTaskReachability` | GA Worker Deployments |

The V1/V2 enabled path is reachable only through non-default dynamic config
(`frontend.workerVersioningDataAPIs` / `frontend.workerVersioningRuleAPIs`, both default `false`);
the five RPCs themselves stay **in-surface as their stock-default rejections** — the exact
`PERMISSION_DENIED` errors a default-configuration v1.31.0 server produces. Decision record with the
full factual case: [`worker-versioning.md`](./worker-versioning.md).

## Related pages

- [`supported.md`](./supported.md) — the v1.31.0 conformance surface.
- [`decisions.md`](./decisions.md) — surfaces present in v1.31.0 that are still under decision.
