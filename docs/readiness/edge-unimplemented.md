# Edge UNIMPLEMENTED — current indicator

> A flat, current list of public-edge RPCs that answer `UNIMPLEMENTED`, split into **should be
> implemented** (in the v1.31.0 surface — see [`../conformance/v1.31.0/supported.md`](../conformance/v1.31.0/supported.md))
> and **intentional** (excluded or under decision). Generated from ground truth, not from a tracker.
>
> **Source of truth:** `Status::unimplemented(...)` and the `deferred_unary!` macro in
> `crates/tokeira-edge/src/grpc/{workflow_service,operator_service}.rs`.
> **Regenerate:** grep those two files for `unimplemented` and `deferred_unary!`.
> **As of:** commit `69e27645` · 2026-06-23.

## Should be implemented (in the v1.31.0 surface)

These RPCs are GA or Public-Preview in v1.31.0 yet answer `UNIMPLEMENTED` today. **Spec implemented** is
read from the owning spec's `tasks.md` (checkbox state at this commit): _No spec_ = no `tasks.md` /
placeholder only; _No (n/N)_ = `tasks.md` exists, n of N tasks checked.

| RPC | Service | How it stubs | Owning spec | Spec implemented |
|-----|---------|--------------|-------------|------------------|
| `ExecuteMultiOperation` | Workflow | `unimplemented` | `api-conformance-multi-operation` | No (0/15) |
| `UpdateWorkflowExecutionOptions` | Workflow | `unimplemented` | `api-conformance-workflow-options` | No (0/16) |
| `ListTaskQueuePartitions` | Workflow | `unimplemented` | `api-conformance-task-queue` | No (0/11) |
| `DeprecateNamespace` | Workflow | `unimplemented` | `api-conformance-namespace-full` | No (0/15) |
| `DeleteNamespace` | Operator | `unimplemented` | `api-conformance-namespace-full` | No (0/15) |
| `RemoveSearchAttributes` | Operator | `unimplemented` | — (no spec) | No spec |
| `AddOrUpdateRemoteCluster` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) | No (0/14) |
| `RemoveRemoteCluster` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) | No (0/14) |
| `ListClusters` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) | No (0/14) |
| `DescribeWorker` | Workflow | `deferred_unary!` | `worker-config-management` | No spec (placeholder) |
| `ListWorkers` | Workflow | `deferred_unary!` | `worker-config-management` | No spec (placeholder) |
| `FetchWorkerConfig` | Workflow | `deferred_unary!` | `worker-config-management` | No spec (placeholder) |
| `UpdateWorkerConfig` | Workflow | `deferred_unary!` | `worker-config-management` | No spec (placeholder) |
| `CreateWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` | No spec (placeholder) |
| `DescribeWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` | No spec (placeholder) |
| `DeleteWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` | No spec (placeholder) |
| `ListWorkflowRules` | Workflow | `deferred_unary!` | `workflow-rules` | No spec (placeholder) |
| `TriggerWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` | No spec (placeholder) |

### Standalone Activities — implemented but gated off by default

The 8 standalone-activity RPCs are bridged through the CHASM `ActivityBridge`, but the bridge is **off
by default** (`enable_standalone_activities = false`); when absent they answer `UNIMPLEMENTED`. So at
default config they read as unimplemented, even though the substrate exists. Public Preview in v1.31.0.
**Spec implemented: Partial** — `activity-executions-first-class` `tasks.md` shows 15/19 tasks checked.

`StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`,
`CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`,
`DeleteActivityExecution`.

## Intentional — not gaps against the v1.31.0 surface

These also answer `UNIMPLEMENTED`, by design. Listed so they are not mistaken for gaps.

| RPC(s) | Reason | Reference |
|--------|--------|-----------|
| `StartNexusOperationExecution` + the 7 other `*NexusOperationExecution` RPCs | Absent from v1.31.0 (vendored `v1.62.11`-only) | [`excluded.md`](../conformance/v1.31.0/excluded.md) |
| `DescribeDeployment`, `ListDeployments`, `GetDeploymentReachability`, `GetCurrentDeployment`, `SetCurrentDeployment` | Deprecated deployment v0 — replaced by GA Worker Deployments | [`excluded.md`](../conformance/v1.31.0/excluded.md) |
| `UpdateWorkerBuildIdCompatibility`, `GetWorkerBuildIdCompatibility` | Legacy worker-versioning V1 (version sets) | [`decisions.md`](../conformance/v1.31.0/decisions.md) — TBD |
| `PauseActivity`, `UnpauseActivity`, `ResetActivity` | Deprecated activity-control aliases | `activity-executions-first-class` |

## Notes

- This is the **whole-RPC** view. Field-level gaps (RPCs that respond but drop fields) are in
  `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.
- RPCs **not** listed here are either implemented or `Partial` (respond with gaps). For per-RPC
  status detail see [`conformance.md`](./conformance.md) and the api-conformance tracker.
- Worker Deployment RPCs are **not** here: they no longer answer `UNIMPLEMENTED` (the registry gates
  them with `FailedPrecondition` when unconfigured); they are tracked under `worker-deployments`.
- The activity by-ID RPCs (`RecordActivityTaskHeartbeatById`, etc.) are **not** here — they were
  implemented via `api-conformance-activity-by-id`.
