# Edge UNIMPLEMENTED — work to be done

> A flat, current list of public-edge RPCs **in the v1.31.0 surface**
> ([`supported.md`](../conformance/v1.31.0/supported.md)) that answer `UNIMPLEMENTED` today — i.e. the
> remaining edge work. Generated from ground truth, not from a tracker. Intentionally-unimplemented RPCs
> (experimental, deprecated, internal, or absent from v1.31.0) are **not** work and are **not** here —
> see [`excluded.md`](../conformance/v1.31.0/excluded.md) / [`decisions.md`](../conformance/v1.31.0/decisions.md).
>
> **Source of truth:** `Status::unimplemented(...)` and the `deferred_unary!` macro in
> `crates/tokeira-edge/src/grpc/{workflow_service,operator_service}.rs`.
> **Regenerate:** grep those two files for `unimplemented` and `deferred_unary!`, then drop any RPC that
> `excluded.md`/`decisions.md` classifies as out-of-surface.
> **As of:** regenerated on top of commit `e2650eaa` · 2026-06-25.

## Work to be done

GA or Public-Preview RPCs in the v1.31.0 surface that answer `UNIMPLEMENTED`. **Spec implemented** is read
from the owning spec's `tasks.md` (checkbox state at this commit): _No spec_ = no `tasks.md` / placeholder
only; _No (n/N)_ = `tasks.md` exists, n of N tasks checked.

| RPC | Service | How it stubs | Owning spec | Spec implemented |
|-----|---------|--------------|-------------|------------------|
| `DeprecateNamespace` | Workflow | `unimplemented` | `api-conformance-namespace-full` | No (0/15) |
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
| `StartActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `DescribeActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `PollActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `ListActivityExecutions` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `CountActivityExecutions` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `RequestCancelActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `TerminateActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |
| `DeleteActivityExecution` | Workflow | `unimplemented` (off by default) | `activity-executions-first-class` | Partial (15/19) |

The standalone-activity RPCs (Public Preview in v1.31.0) are bridged through the CHASM `ActivityBridge`
but the bridge is **off by default** (`enable_standalone_activities = false`), so they answer
`UNIMPLEMENTED` at default config even though the substrate is partly built (15/19 spec tasks).

## Cross-reference with supported.md (minimality)

This list is **minimal and complete** when:

- **Complete** — it captures *every* in-surface public-edge RPC that answers `UNIMPLEMENTED`. The raw set
  is the exhaustive grep of `Status::unimplemented` + `deferred_unary!` in the two grpc files; the
  out-of-surface ones (per `excluded.md`/`decisions.md`) are then removed. RPCs that respond with dropped
  fields are *not* whole-RPC unimplemented — they are `Partial` and tracked in `UNSUPPORTED_FIELDS.md`.
- **Minimal** — every row maps to a feature area in
  [`supported.md`](../conformance/v1.31.0/supported.md). Nothing out-of-surface is here.

| Entry | supported.md feature area |
|-------|---------------------------|
| `DeprecateNamespace` | Namespaces |
| `RemoveSearchAttributes` | Search attributes (operator) |
| `AddOrUpdateRemoteCluster`, `RemoveRemoteCluster`, `ListClusters` | Remote-cluster registry |
| `DescribeWorker`, `ListWorkers`, `FetchWorkerConfig`, `UpdateWorkerConfig` | Worker inventory |
| Workflow-rule RPCs (×5) | Workflow rules |
| Standalone-activity RPCs (×8) | Standalone Activities (Public Preview) |

To re-verify: confirm each row still maps to a `supported.md` feature area, and that no `supported.md`
area has an RPC answering `UNIMPLEMENTED` that is absent from this list.

## Notes

- This is the **whole-RPC** view. Field-level gaps (RPCs that respond but drop fields) are in
  `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.
- Worker Deployment RPCs are **not** here: they no longer answer `UNIMPLEMENTED` (the registry gates them
  with `FailedPrecondition` when unconfigured); they are tracked under `worker-deployments`.
- The activity by-ID RPCs (`RecordActivityTaskHeartbeatById`, etc.) are **not** here — they were
  implemented via `api-conformance-activity-by-id`.
- `ExecuteMultiOperation` is **not** here — implemented (Update-with-Start) via
  `api-conformance-multi-operation`.
