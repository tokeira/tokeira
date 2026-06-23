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

These RPCs are GA or Public-Preview in v1.31.0 yet answer `UNIMPLEMENTED` today.

| RPC | Service | How it stubs | Owning spec |
|-----|---------|--------------|-------------|
| `ExecuteMultiOperation` | Workflow | `unimplemented` | `api-conformance-multi-operation` |
| `UpdateWorkflowExecutionOptions` | Workflow | `unimplemented` | `api-conformance-workflow-options` |
| `ListTaskQueuePartitions` | Workflow | `unimplemented` | `api-conformance-task-queue` |
| `DeprecateNamespace` | Workflow | `unimplemented` | `api-conformance-namespace-full` |
| `DeleteNamespace` | Operator | `unimplemented` | `api-conformance-namespace-full` |
| `RemoveSearchAttributes` | Operator | `unimplemented` | (search attributes) |
| `AddOrUpdateRemoteCluster` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) |
| `RemoveRemoteCluster` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) |
| `ListClusters` | Operator | `unimplemented` | `api-conformance-remote-cluster` (registry only) |
| `DescribeWorker` | Workflow | `deferred_unary!` | `worker-config` |
| `ListWorkers` | Workflow | `deferred_unary!` | `worker-config` |
| `FetchWorkerConfig` | Workflow | `deferred_unary!` | `worker-config-management` |
| `UpdateWorkerConfig` | Workflow | `deferred_unary!` | `worker-config-management` |
| `CreateWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` |
| `DescribeWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` |
| `DeleteWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` |
| `ListWorkflowRules` | Workflow | `deferred_unary!` | `workflow-rules` |
| `TriggerWorkflowRule` | Workflow | `deferred_unary!` | `workflow-rules` |

### Standalone Activities — implemented but gated off by default

The 8 standalone-activity RPCs are bridged through the CHASM `ActivityBridge`, but the bridge is **off
by default** (`enable_standalone_activities = false`); when absent they answer `UNIMPLEMENTED`. So at
default config they read as unimplemented, even though the substrate exists. Public Preview in v1.31.0.

`StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`,
`CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`,
`DeleteActivityExecution` — spec `activity-executions-first-class`.

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
