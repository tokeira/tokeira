# Tokeira Temporal API Implementation Audit

**Audit target:** Tokeira snapshot uploaded for this review.

**Authoritative Temporal reference:** Temporal Server `v1.31.0`, which pins `go.temporal.io/api v1.62.8`. The authoritative service surfaces are `temporal/api/workflowservice/v1/service.proto` and `temporal/api/operatorservice/v1/service.proto` at API tag `v1.62.8`.

**Important version note:** the uploaded Tokeira snapshot vendors `proto/UPSTREAM_VERSION = v1.62.11`. This audit intentionally uses the narrower `v1.62.8` surface because the user requested Temporal Server `v1.31.0`. The vendored `v1.62.11` adds Nexus operation execution RPCs that are not part of this `v1.31.0` audit; those are listed separately near the end.

## Status meanings

| Status | Meaning |
|---|---|
| Implemented | Handler and backing behaviour appear present with no major gaps identified during static review. This audit uses this sparingly. |
| Partial | Handler and some backing behaviour exist, but known field, response, lifecycle, or conformance gaps remain. |
| Stubbed | The gRPC method returns `tonic::Status::unimplemented` directly. |
| Deferred | The method is wired through `deferred_unary!` and returns `UNIMPLEMENTED` with a named future spec. |
| Absent | No handler was found in the audited gRPC adapter. |

## Summary

- `WorkflowService` RPCs in Temporal API `v1.62.8`: **109**
- `OperatorService` RPCs in Temporal API `v1.62.8`: **12**
- Total audited RPCs: **121**
- Partial: **53**
- Stubbed: **36**
- Deferred: **32**

**Overall finding:** Tokeira has meaningful coverage of the core SDK execution path, especially workflow start, workflow-task polling/completion, history reads, activity polling/completion, signals, queries, updates, schedules, batch/Nexus task transport, worker heartbeats, and worker-versioning v2 rules. However, the majority of surfaces should remain conservatively classified as `Partial`, `Stubbed`, or `Deferred` until field-level conformance and SDK conformance tests are in place.

## Critical compatibility observations

1. **The vendored proto version is ahead of Temporal Server `v1.31.0`.** `proto/UPSTREAM_VERSION` in the snapshot says `v1.62.11`, while Temporal Server `v1.31.0` pins API `v1.62.8`. Treat this as proto drift unless the temporal-compatibility spec intentionally tracks a newer API than the claimed server baseline.
2. **The current feature matrix overclaims some surfaces.** `workflow-execution` is marked `Implemented` but contains `ExecuteMultiOperation`, `PauseWorkflowExecution`, `UnpauseWorkflowExecution`, and `UpdateWorkflowExecutionOptions`, all of which are stubbed/deferred. `schedules` is marked `Stubbed` even though schedule RPC handlers exist and are best described as `Partial`.
3. **Field-level gaps are material.** `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` documents unsupported fields in `StartWorkflowExecutionRequest`, `RespondWorkflowTaskCompletedRequest`, schedule transport, `DescribeWorkflowExecutionResponse`, `PollActivityTaskQueueResponse`, signals, batch operations, history event activity attributes, workflow options updated events, and update responses.
4. **Experimental APIs are mostly not implemented.** Worker deployments and workflow pause/unpause are exposed in the Temporal v1.31.0 API surface as experimental; Tokeira currently returns `UNIMPLEMENTED` for those surfaces.
5. **OperatorService is mostly stubbed.** Only `AddSearchAttributes` and `ListSearchAttributes` have nontrivial handlers; remote cluster and Nexus endpoint administration are not implemented.

## Recommended compatibility matrix corrections

| Feature | Current matrix state observed | Recommended state for Temporal v1.31.0 claim | Reason |
|---|---:|---:|---|
| `workflow-execution` | Implemented | Partial / Experimental | Includes multiple stubbed or deferred RPCs and unsupported start/update fields. |
| `workflow-history` | Implemented | Partial | Handler exists, but archival and event attribute fidelity need conformance proof. |
| `workflow-polling` | Implemented | Partial | Core path exists, but sticky/deployment/versioning metadata gaps remain. |
| `activity-tasks` | Experimental | Partial / Experimental | Token paths exist; by-id/cancel/pause/reset/update-options surfaces are stubbed. |
| `schedules` | Stubbed | Partial / Experimental | Schedule handlers are implemented but field round-trip gaps are documented. |
| `batch-operations` | Stubbed | Partial / Experimental | Batch handlers exist; unsupported batch fields/options are documented. |
| `nexus` | Experimental | Partial / Experimental | Nexus task transport exists; endpoint admin is stubbed and full operation lifecycle is absent for v1.31.0. |
| `deployments` | Experimental | Stubbed / Deferred | Experimental worker deployment APIs return `UNIMPLEMENTED`. |
| `worker-heartbeats` | Experimental | Partial / Experimental | Heartbeat/shutdown exist; list/describe/fetch/update worker config are deferred. |
| `search-attributes` | Experimental | Partial / Experimental | Operator add/list exist; workflow GetSearchAttributes and remove are stubbed. |
| `cluster-info` | Experimental | Partial | Workflow metadata exists; remote cluster OperatorService RPCs are stubbed. |

## Namespace management

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.RegisterNamespace` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:425` | Handler exists; namespace registry is present, but full Temporal namespace configuration/global namespace/deletion semantics are not proven. |
| `WorkflowService.DescribeNamespace` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:435` | Handler exists; namespace registry is present, but full Temporal namespace configuration/global namespace/deletion semantics are not proven. |
| `WorkflowService.ListNamespaces` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:451` | Handler exists; namespace registry is present, but full Temporal namespace configuration/global namespace/deletion semantics are not proven. |
| `WorkflowService.UpdateNamespace` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:461` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.DeprecateNamespace` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:467` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.DeleteNamespace` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:97` | Returns gRPC UNIMPLEMENTED. |

## Workflow execution lifecycle

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.StartWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:134` | Core start path exists; unsupported fields include reuse/conflict policy, client-supplied cron, start delay, callbacks, user metadata, links, and versioning override. |
| `WorkflowService.ExecuteMultiOperation` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:473` | Returns gRPC UNIMPLEMENTED; atomic multi-operation start is not implemented. |
| `WorkflowService.SignalWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:150` | Signal path exists; signal headers and links are not threaded. |
| `WorkflowService.SignalWithStartWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:546` | Combined signal/start path exists; inherits StartWorkflowExecution and Signal field gaps. |
| `WorkflowService.RequestCancelWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:350` | Cancel request handler exists; full Temporal cancellation corner cases and child/external propagation are not proven by this audit. |
| `WorkflowService.TerminateWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:336` | Terminate handler exists; full termination details/search attributes/child propagation are not proven by this audit. |
| `WorkflowService.ResetWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:561` | Reset handler exists; reset reapply semantics are limited and several batch reset options are explicitly unsupported. |
| `WorkflowService.DeleteWorkflowExecution` | Stable/normal | **Implemented** | Authoritative coordinator | `temporal-ui-support` Requirement 9; `TestWorkflowDeleteExecutionSuite @ v1.31.0` (3/3, two consecutive runs) | Direct deletion terminates an open target, atomically tombstones and purges the exact run, and prevents stale visibility resurrection. Automated retention and archival lifecycle policy remain separate from this RPC contract. |
| `WorkflowService.UpdateWorkflowExecutionOptions` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1577` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.PauseWorkflowExecution` | Experimental | **Deferred** | Deferred (`kernel-pause-workflow`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1458` | Deferred via kernel-pause-workflow spec; returns UNIMPLEMENTED. |
| `WorkflowService.UnpauseWorkflowExecution` | Experimental | **Deferred** | Deferred (`kernel-pause-workflow`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1464` | Deferred via kernel-pause-workflow spec; returns UNIMPLEMENTED. |

## Workflow history and task lifecycle

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.GetWorkflowExecutionHistory` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:397` | History read handler exists with pagination/long-poll support; full archival and all event attribute fidelity are not proven. |
| `WorkflowService.GetWorkflowExecutionHistoryReverse` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:479` | History read handler exists with pagination/long-poll support; full archival and all event attribute fidelity are not proven. |
| `WorkflowService.PollWorkflowTaskQueue` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:166` | Workflow task polling exists; sticky task queue behaviour is partial. |
| `WorkflowService.RespondWorkflowTaskCompleted` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:192` | Completion path exists; sticky attributes, SDK metadata, metering metadata, deployment, and versioning behaviour fields are unsupported or partial. |
| `WorkflowService.RespondWorkflowTaskFailed` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:494` | Failure handler exists; exact upstream consecutive-failure compaction semantics are not proven. |
| `WorkflowService.ResetStickyTaskQueue` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:651` | Returns success/no-op style response; sticky task queues are only partially implemented. |

## Activity task lifecycle

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.PollActivityTaskQueue` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:267` | Activity polling exists; heartbeat details and activity timing fields are not populated. |
| `WorkflowService.RecordActivityTaskHeartbeat` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:320` | Token heartbeat handler exists; heartbeat details persistence/checkpoint semantics are limited. |
| `WorkflowService.RecordActivityTaskHeartbeatById` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:510` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.RespondActivityTaskCompleted` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:288` | Token-based activity response path exists; activity event linkage fields and some retry/timing details are limited. |
| `WorkflowService.RespondActivityTaskCompletedById` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:518` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.RespondActivityTaskFailed` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:306` | Token-based activity response path exists; activity event linkage fields and some retry/timing details are limited. |
| `WorkflowService.RespondActivityTaskFailedById` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:526` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.RespondActivityTaskCanceled` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:532` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.RespondActivityTaskCanceledById` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:538` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.UpdateActivityOptions` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1571` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.PauseActivity` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1583` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.UnpauseActivity` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1589` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.ResetActivity` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1595` | Returns gRPC UNIMPLEMENTED. |

## Queries and updates

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.QueryWorkflow` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:364` | Query path exists with pending-query transport; conformance for signal/update ordering should remain required. |
| `WorkflowService.RespondQueryTaskCompleted` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:618` | Legacy query completion transport exists; responds success if no waiter per tests. |
| `WorkflowService.UpdateWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:375` | Workflow update handler exists; update_ref and lifecycle stage fields are not populated. |
| `WorkflowService.PollWorkflowExecutionUpdate` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1070` | Update polling exists; full stage/ref semantics are limited by update response gaps. |

## Visibility and workflow introspection

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.DescribeWorkflowExecution` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:215` | Describe handler exists; execution_config, pending activities/children/WFT, callbacks, and pending Nexus operations are not populated. |
| `WorkflowService.ListWorkflowExecutions` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:239` | Visibility list handler exists; query language/support depends on projection implementation. |
| `WorkflowService.CountWorkflowExecutions` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:253` | Visibility count handler exists; query language/support depends on projection implementation. |
| `WorkflowService.ListOpenWorkflowExecutions` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:588` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.ListClosedWorkflowExecutions` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:594` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.ListArchivedWorkflowExecutions` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:600` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.ScanWorkflowExecutions` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:606` | Returns gRPC UNIMPLEMENTED. |

## Task queues, cluster metadata, and system metadata

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.DescribeTaskQueue` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:740` | DescribeTaskQueue handler exists; worker reachability/backlog/build-ID detail completeness is not proven. |
| `WorkflowService.UpdateTaskQueueConfig` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1409` | Task queue config update handler exists for rate/fairness settings; full Temporal config semantics are not proven. |
| `WorkflowService.ListTaskQueuePartitions` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:768` | Returns gRPC UNIMPLEMENTED. |
| `WorkflowService.GetClusterInfo` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:752` | Cluster info handler exists; response is Tokeira-local/limited rather than full Temporal cluster topology. |
| `WorkflowService.GetSystemInfo` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:760` | System info handler exists; must remain standard Temporal wire response; capability flags require careful validation. |

## Schedules

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.CreateSchedule` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:774` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.DescribeSchedule` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:792` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.UpdateSchedule` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:810` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.PatchSchedule` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:830` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.ListScheduleMatchingTimes` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:842` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.DeleteSchedule` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:869` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.ListSchedules` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:883` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |
| `WorkflowService.CountSchedules` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1259` | Schedule handler exists; timezone_data, original calendar/cron round-trip, headers, user metadata, and versioning override have known gaps. |

## Search attributes

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.GetSearchAttributes` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:612` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.AddSearchAttributes` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/operator_service.rs:35` | Adds/upserts custom search attributes; validation exists but full Temporal search attribute registration workflow is not proven. |
| `OperatorService.RemoveSearchAttributes` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:57` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.ListSearchAttributes` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/operator_service.rs:64` | Lists custom attributes; system attributes and storage schema are default/empty. |

## Worker versioning and worker deployments

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.UpdateWorkerBuildIdCompatibility` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:908` | Legacy worker build-id compatibility API returns UNIMPLEMENTED; v2 rules are preferred. |
| `WorkflowService.GetWorkerBuildIdCompatibility` | Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:916` | Legacy worker build-id compatibility API returns UNIMPLEMENTED; v2 rules are preferred. |
| `WorkflowService.UpdateWorkerVersioningRules` | Unstable | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:924` | Worker versioning v2/build-id reachability logic exists; upstream labels the surface unstable and SDK conformance is not proven. |
| `WorkflowService.GetWorkerVersioningRules` | Unstable | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:969` | Worker versioning v2/build-id reachability logic exists; upstream labels the surface unstable and SDK conformance is not proven. |
| `WorkflowService.GetWorkerTaskReachability` | Deprecated | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:987` | Worker versioning v2/build-id reachability logic exists; upstream labels the surface unstable and SDK conformance is not proven. |
| `WorkflowService.DescribeDeployment` | Experimental, Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1030` | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorkerDeploymentVersion` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1284` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListDeployments` | Experimental, Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1038` | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.GetDeploymentReachability` | Experimental, Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1046` | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.GetCurrentDeployment` | Experimental, Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1054` | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.SetCurrentDeployment` | Experimental, Deprecated | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1062` | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentCurrentVersion` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1290` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorkerDeployment` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1296` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteWorkerDeployment` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1302` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteWorkerDeploymentVersion` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1308` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentRampingVersion` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1314` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListWorkerDeployments` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1320` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.CreateWorkerDeployment` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1326` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.CreateWorkerDeploymentVersion` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1332` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.UpdateWorkerDeploymentVersionComputeConfig` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1338` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ValidateWorkerDeploymentVersionComputeConfig` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1344` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.UpdateWorkerDeploymentVersionMetadata` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1350` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentManager` | Experimental | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1356` | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |

## Batch operations

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.StartBatchOperation` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1163` | Batch operation handler exists, but unsupported batch fields/options are documented and full Temporal batch semantics are not proven. |
| `WorkflowService.StopBatchOperation` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1175` | Batch operation handler exists, but unsupported batch fields/options are documented and full Temporal batch semantics are not proven. |
| `WorkflowService.DescribeBatchOperation` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1187` | Batch operation handler exists, but unsupported batch fields/options are documented and full Temporal batch semantics are not proven. |
| `WorkflowService.ListBatchOperations` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1202` | Batch operation handler exists, but unsupported batch fields/options are documented and full Temporal batch semantics are not proven. |

## Nexus and remote cluster administration

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.PollNexusTaskQueue` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1216` | Nexus task transport exists; endpoint administration and full Nexus operation lifecycle are not implemented for v1.31.0. |
| `WorkflowService.RespondNexusTaskCompleted` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1231` | Nexus task transport exists; endpoint administration and full Nexus operation lifecycle are not implemented for v1.31.0. |
| `WorkflowService.RespondNexusTaskFailed` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:1245` | Nexus task transport exists; endpoint administration and full Nexus operation lifecycle are not implemented for v1.31.0. |
| `OperatorService.GetNexusEndpoint` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:125` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.CreateNexusEndpoint` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:132` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.UpdateNexusEndpoint` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:139` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.DeleteNexusEndpoint` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:146` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.ListNexusEndpoints` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:153` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.AddOrUpdateRemoteCluster` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:104` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.RemoveRemoteCluster` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:111` | Returns gRPC UNIMPLEMENTED. |
| `OperatorService.ListClusters` | Stable/normal | **Stubbed** | Stubbed (`UNIMPLEMENTED`) | `crates/tokeira-edge/src/grpc/operator_service.rs:118` | Returns gRPC UNIMPLEMENTED. |

## Worker inventory and config

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.RecordWorkerHeartbeat` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:660` | Worker heartbeat/shutdown inventory path exists; full worker inventory/config/deployment semantics are not implemented. |
| `WorkflowService.ShutdownWorker` | Stable/normal | **Partial** | Handler present | `crates/tokeira-edge/src/grpc/workflow_service.rs:698` | Worker heartbeat/shutdown inventory path exists; full worker inventory/config/deployment semantics are not implemented. |
| `WorkflowService.ListWorkers` | Stable/normal | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1368` | Deferred via worker-deployments or worker-config-management spec; returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorker` | Stable/normal | **Deferred** | Deferred (`worker-deployments`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1362` | Deferred via worker-deployments or worker-config-management spec; returns UNIMPLEMENTED. |
| `WorkflowService.FetchWorkerConfig` | Stable/normal | **Deferred** | Deferred (`worker-config-management`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1443` | Deferred via worker-deployments or worker-config-management spec; returns UNIMPLEMENTED. |
| `WorkflowService.UpdateWorkerConfig` | Stable/normal | **Deferred** | Deferred (`worker-config-management`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1449` | Deferred via worker-deployments or worker-config-management spec; returns UNIMPLEMENTED. |

## Workflow rules

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.CreateWorkflowRule` | Stable/normal | **Deferred** | Deferred (`workflow-rules`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1377` | Deferred via workflow-rules spec; returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorkflowRule` | Stable/normal | **Deferred** | Deferred (`workflow-rules`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1383` | Deferred via workflow-rules spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteWorkflowRule` | Stable/normal | **Deferred** | Deferred (`workflow-rules`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1389` | Deferred via workflow-rules spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListWorkflowRules` | Stable/normal | **Deferred** | Deferred (`workflow-rules`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1395` | Deferred via workflow-rules spec; returns UNIMPLEMENTED. |
| `WorkflowService.TriggerWorkflowRule` | Stable/normal | **Deferred** | Deferred (`workflow-rules`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1401` | Deferred via workflow-rules spec; returns UNIMPLEMENTED. |

## First-class activity executions

| RPC | API flag | Implementation | Code status | Evidence | Notes |
|---|---|---|---|---|---|
| `WorkflowService.StartActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1473` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.DescribeActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1479` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.PollActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1485` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListActivityExecutions` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1491` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.CountActivityExecutions` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1497` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.RequestCancelActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1503` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.TerminateActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1509` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteActivityExecution` | Stable/normal | **Deferred** | Deferred (`activity-executions-first-class`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1515` | Deferred via activity-executions-first-class spec; returns UNIMPLEMENTED. |

## Experimental APIs in Temporal Server v1.31.0 API surface

| RPC | Deprecated? | Tokeira status | Notes |
|---|---|---|---|
| `WorkflowService.DescribeDeployment` | Yes | **Stubbed** | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorkerDeploymentVersion` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListDeployments` | Yes | **Stubbed** | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.GetDeploymentReachability` | Yes | **Stubbed** | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.GetCurrentDeployment` | Yes | **Stubbed** | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.SetCurrentDeployment` | Yes | **Stubbed** | Deprecated experimental deployment API returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentCurrentVersion` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DescribeWorkerDeployment` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteWorkerDeployment` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.DeleteWorkerDeploymentVersion` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentRampingVersion` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ListWorkerDeployments` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.CreateWorkerDeployment` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.CreateWorkerDeploymentVersion` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.UpdateWorkerDeploymentVersionComputeConfig` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.ValidateWorkerDeploymentVersionComputeConfig` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.UpdateWorkerDeploymentVersionMetadata` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.SetWorkerDeploymentManager` | No | **Deferred** | Deferred via worker-deployments spec; returns UNIMPLEMENTED. |
| `WorkflowService.PauseWorkflowExecution` | No | **Deferred** | Deferred via kernel-pause-workflow spec; returns UNIMPLEMENTED. |
| `WorkflowService.UnpauseWorkflowExecution` | No | **Deferred** | Deferred via kernel-pause-workflow spec; returns UNIMPLEMENTED. |

## Unstable but not explicitly experimental APIs

| RPC | Deprecated? | Tokeira status | Notes |
|---|---|---|---|
| `WorkflowService.UpdateWorkerVersioningRules` | No | **Partial** | Worker versioning v2/build-id reachability logic exists; upstream labels the surface unstable and SDK conformance is not proven. |
| `WorkflowService.GetWorkerVersioningRules` | No | **Partial** | Worker versioning v2/build-id reachability logic exists; upstream labels the surface unstable and SDK conformance is not proven. |

## Extra APIs in Tokeira vendored `v1.62.11` that are outside Temporal Server v1.31.0

These RPCs appear in the uploaded Tokeira snapshot because it vendors API `v1.62.11`, but they are not part of the Temporal Server `v1.31.0` / API `v1.62.8` audit surface. They should not be used to justify a `v1.31.0` compatibility claim, and they should be separated as “tracked ahead” metadata if retained.

| RPC | Tokeira code status | Evidence |
|---|---|---|
| `WorkflowService.CountNexusOperationExecutions` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1547` |
| `WorkflowService.DeleteNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1565` |
| `WorkflowService.DescribeNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1529` |
| `WorkflowService.ListNexusOperationExecutions` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1541` |
| `WorkflowService.PollNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1535` |
| `WorkflowService.RequestCancelNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1553` |
| `WorkflowService.StartNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1523` |
| `WorkflowService.TerminateNexusOperationExecution` | Deferred (`edge-nexus-task-transport`) | `crates/tokeira-edge/src/grpc/workflow_service.rs:1559` |

## Field-level and behaviour gaps requiring follow-up

The following gaps are explicitly documented in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` or inferred from the handler shape. These should become request-field/response-field compatibility matrix entries rather than being hidden behind broad RPC-level states.

- `StartWorkflowExecutionRequest`: reuse/conflict policy, client-supplied cron starts, continued failure, last completion result, start delay, completion callbacks, user metadata, links, and versioning override are not fully supported.
- `RespondWorkflowTaskCompletedRequest`: sticky attributes, SDK metadata, metering metadata, deployment, and versioning behaviour are not supported; `return_new_workflow_task` is only partially supported.
- `DescribeWorkflowExecutionResponse`: execution config, pending activities, pending children, pending workflow task, callbacks, and pending Nexus operations are not populated.
- `PollActivityTaskQueueResponse`: heartbeat details, scheduled time, current attempt scheduled time, and started time are not populated.
- `SignalWorkflowExecutionRequest`: header and links are not threaded.
- Schedule transport: embedded timezone data, original calendar/cron round-trip, scheduled start headers, user metadata, and versioning override are limited or unsupported.
- Batch operations: update workflow execution options, reset reapply type, current-run-only, reset reapply exclude types, and signal headers are unsupported or dropped.
- Activity history events: scheduled/started event linkage fields are not fully populated because the kernel tracks activities by `activity_id` rather than event ID.
- `UpdateWorkflowExecutionResponse`: `update_ref` and `stage` are not populated.

## Recommended next implementation priorities

1. **Fix version-baseline drift**: either vendor exactly API `v1.62.8` for the `v1.31.0` claim or split `tracked_upstream_api = v1.62.11` from `server_compat_baseline = v1.31.0`.
2. **Downgrade broad feature claims**: change all feature-matrix states that include stubbed/deferred RPCs from `Implemented` to `Partial`, `Experimental`, `Stubbed`, or split them into smaller feature IDs.
3. **Add field-level compatibility entries**: especially for start workflow, workflow-task completion, describe workflow, schedule transport, update responses, and activity event attributes.
4. **Prioritise SDK-critical conformance**: Go/Java/Python/TypeScript smoke tests for start → poll WFT → complete → activity → signal → query → update → history.
5. **Decide on schedules**: schedule RPCs are already present; either mark as partial/experimental and test them, or intentionally gate them behind compatibility dispatch.
6. **Keep worker deployments and workflow pause/unpause experimental/deferred** until the kernel/runtime semantics exist.
7. **Avoid patching upstream Temporal protos** for Tokeira metadata. Expose rich Tokeira compatibility metadata through a Tokeira-specific admin endpoint or `tkr compat show`, while keeping `GetSystemInfo` standard.
