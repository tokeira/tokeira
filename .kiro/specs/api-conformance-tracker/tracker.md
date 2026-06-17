# API Conformance Tracker

> **Reference only — not the progress tracker.** This is a static RPC-coverage index (which RPC is
> owned by which spec). The single canonical progress doc for functional conformance is
> `temporal-functional-conformance/reference/FINDINGS.md` (its **Status ledger**). Do not track
> done/completed here. (Decided 2026-06-17.)

**Target:** Temporal Server v1.31.0 (API v1.62.8) — 121 RPCs

**Source audit:** `reference/temporal_api_audit.md`

**Last updated:** 2026-05-29

---

## Progress

| Metric | Value |
|--------|-------|
| Total RPCs | 121 |
| Implemented | 0 |
| Partial | 53 |
| Stubbed | 36 |
| Deferred | 32 |
| **Coverage** | **0%** |

---

## Child Specs (new)

| # | Spec | Status | RPCs | Target state |
|---|------|--------|------|--------------|
| 1 | `api-conformance-activity-by-id` | Completed | 6 | Implemented |
| 2 | `api-conformance-workflow-describe` | Spec Draft | 1 | Implemented |
| 3 | `api-conformance-start-fields` | Spec Draft | 2 | Implemented |
| 4 | `api-conformance-wft-completion` | Spec Draft | 1 | Implemented |
| 5 | `api-conformance-activity-events` | Spec Draft | 2 | Implemented |
| 6 | `api-conformance-update-lifecycle` | Spec Draft | 2 | Implemented |
| 7 | `api-conformance-signal-headers` | Spec Draft | 1 | Implemented |
| 8 | `api-conformance-schedule-fields` | Spec Draft | 8 | Implemented |
| 9 | `api-conformance-namespace-full` | Spec Draft | 3 | Implemented |
| 10 | `api-conformance-visibility-legacy` | Spec Draft | 5 | Implemented |
| 11 | `api-conformance-nexus-admin` | Spec Draft | 5 | Implemented |
| 12 | `api-conformance-remote-cluster` | Spec Draft | 3 | Implemented |
| 13 | `api-conformance-multi-operation` | Spec Draft | 1 | Implemented |
| 14 | `api-conformance-task-queue` | Spec Draft | 2 | Implemented |
| 15 | `api-conformance-batch-fields` | Spec Draft | 4 | Implemented |
| 16 | `api-conformance-workflow-options` | Spec Draft | 1 | Implemented |

---

## Dependency Matrix

| Area | Specs involved | Dependency rule |
|---|---|---|
| Request metadata (`headers`, `links`, `user_metadata`) | signal, start, schedule, batch, WFT completion | Use one shared field policy/helper; do not let specs diverge. |
| Versioning and worker deployment fields | start, WFT completion, workflow options, task queue | Implement durable routing metadata and keep behavior consistent across all admission paths. |
| Pending activity state | activity-events, activity-by-id, workflow-describe | Define one pending activity snapshot model and reuse it. |
| Update lifecycle | update-lifecycle, workflow-describe, WFT completion | Persist enough update lifecycle state for restart-safe polling before describe consumes it. |
| Schedule-start metadata | schedule-fields, start-fields, signal-headers | Schedule firing must reuse direct-start metadata validation and translation. |
| Namespace deprecation | namespace-full, start, schedules, batch, multi-operation | Add one admission guard used by every start-like path. |
| Broad mutations | batch-fields, multi-operation, namespace delete | Require validation-first and explicit runtime/storage transaction boundaries. |
| Nexus | nexus-admin, workflow describe, Nexus task transport | Endpoint CRUD is admin registry work only; it does not complete Nexus task execution. |
| Remote cluster | remote-cluster, namespace failover, replication | Registry APIs implement metadata CRUD but do not imply multi-cluster replication, failover, or remote routing. |

## Existing Backlog Specs (tracked here for coverage)

| Spec | Status | RPCs | Target state |
|------|--------|------|--------------|
| `kernel-pause-workflow` (P7) | Not Started | 2 | Implemented |
| `activity-executions-first-class` (P8) | Not Started | 8 | Implemented |
| `worker-deployments` (P10) | Not Started | 14 | Implemented |
| `workflow-rules` (P11) | Not Started | 5 | Implemented |
| `worker-config-management` (P4) | Not Started | 4 | Implemented |

---

## RPC-Level Detail

### Spec 1: `api-conformance-activity-by-id`

| RPC | Current | Target |
|-----|---------|--------|
| RecordActivityTaskHeartbeatById | Stubbed | Implemented |
| RespondActivityTaskCompletedById | Stubbed | Implemented |
| RespondActivityTaskFailedById | Stubbed | Implemented |
| RespondActivityTaskCanceledById | Stubbed | Implemented |
| RespondActivityTaskCanceled | Stubbed | Implemented |
| UpdateActivityOptions | Stubbed | Implemented |

### Spec 2: `api-conformance-workflow-describe`

| RPC | Current | Target |
|-----|---------|--------|
| DescribeWorkflowExecution | Partial | Implemented |

Gaps: execution_config, pending activities, pending children, pending WFT, callbacks, pending Nexus operations.

### Spec 3: `api-conformance-start-fields`

| RPC | Current | Target |
|-----|---------|--------|
| StartWorkflowExecution | Partial | Implemented |
| SignalWithStartWorkflowExecution | Partial | Implemented |

Gaps: reuse/conflict policy, start delay, completion callbacks, user metadata, links, versioning override, client-supplied cron.

### Spec 4: `api-conformance-wft-completion`

| RPC | Current | Target |
|-----|---------|--------|
| RespondWorkflowTaskCompleted | Partial | Implemented |

Gaps: sticky attributes, SDK metadata, metering metadata, deployment, versioning behaviour, return_new_workflow_task.

### Spec 5: `api-conformance-activity-events`

| RPC | Current | Target |
|-----|---------|--------|
| PollActivityTaskQueue | Partial | Implemented |
| RecordActivityTaskHeartbeat | Partial | Implemented |

Gaps: heartbeat details, scheduled time, current attempt scheduled time, started time, event linkage fields.

### Spec 6: `api-conformance-update-lifecycle`

| RPC | Current | Target |
|-----|---------|--------|
| UpdateWorkflowExecution | Partial | Implemented |
| PollWorkflowExecutionUpdate | Partial | Implemented |

Gaps: update_ref, stage fields in response.

### Spec 7: `api-conformance-signal-headers`

| RPC | Current | Target |
|-----|---------|--------|
| SignalWorkflowExecution | Partial | Implemented |

Gaps: header and links not threaded.

### Spec 8: `api-conformance-schedule-fields`

| RPC | Current | Target |
|-----|---------|--------|
| CreateSchedule | Partial | Implemented |
| DescribeSchedule | Partial | Implemented |
| UpdateSchedule | Partial | Implemented |
| PatchSchedule | Partial | Implemented |
| ListScheduleMatchingTimes | Partial | Implemented |
| DeleteSchedule | Partial | Implemented |
| ListSchedules | Partial | Implemented |
| CountSchedules | Partial | Implemented |

Gaps: timezone_data, original calendar/cron round-trip, headers, user metadata, versioning override.

### Spec 9: `api-conformance-namespace-full`

| RPC | Current | Target |
|-----|---------|--------|
| UpdateNamespace | Stubbed | Implemented |
| DeprecateNamespace | Stubbed | Implemented |
| DeleteNamespace (OperatorService) | Stubbed | Implemented |

### Spec 10: `api-conformance-visibility-legacy`

| RPC | Current | Target |
|-----|---------|--------|
| ListOpenWorkflowExecutions | Stubbed | Implemented |
| ListClosedWorkflowExecutions | Stubbed | Implemented |
| ListArchivedWorkflowExecutions | Stubbed | Implemented |
| ScanWorkflowExecutions | Stubbed | Implemented |
| GetSearchAttributes | Stubbed | Implemented |

### Spec 11: `api-conformance-nexus-admin`

| RPC | Current | Target |
|-----|---------|--------|
| GetNexusEndpoint | Stubbed | Implemented |
| CreateNexusEndpoint | Stubbed | Implemented |
| UpdateNexusEndpoint | Stubbed | Implemented |
| DeleteNexusEndpoint | Stubbed | Implemented |
| ListNexusEndpoints | Stubbed | Implemented |

### Spec 12: `api-conformance-remote-cluster`

| RPC | Current | Target |
|-----|---------|--------|
| AddOrUpdateRemoteCluster | Stubbed | Implemented |
| RemoveRemoteCluster | Stubbed | Implemented |
| ListClusters | Stubbed | Implemented |

### Spec 13: `api-conformance-multi-operation`

| RPC | Current | Target |
|-----|---------|--------|
| ExecuteMultiOperation | Stubbed | Implemented |

### Spec 14: `api-conformance-task-queue`

| RPC | Current | Target |
|-----|---------|--------|
| ListTaskQueuePartitions | Stubbed | Implemented |
| DescribeTaskQueue | Partial | Implemented |

Gaps: worker reachability, backlog detail, build-ID completeness.

### Spec 15: `api-conformance-batch-fields`

| RPC | Current | Target |
|-----|---------|--------|
| StartBatchOperation | Partial | Implemented |
| StopBatchOperation | Partial | Implemented |
| DescribeBatchOperation | Partial | Implemented |
| ListBatchOperations | Partial | Implemented |

Gaps: update workflow execution options, reset reapply type, current-run-only, reset reapply exclude types, signal headers.

### Spec 16: `api-conformance-workflow-options`

| RPC | Current | Target |
|-----|---------|--------|
| UpdateWorkflowExecutionOptions | Stubbed | Implemented |

### Existing: `kernel-pause-workflow` (P7)

| RPC | Current | Target |
|-----|---------|--------|
| PauseWorkflowExecution | Deferred | Implemented |
| UnpauseWorkflowExecution | Deferred | Implemented |

### Existing: `activity-executions-first-class` (P8)

| RPC | Current | Target |
|-----|---------|--------|
| StartActivityExecution | Deferred | Implemented |
| DescribeActivityExecution | Deferred | Implemented |
| PollActivityExecution | Deferred | Implemented |
| ListActivityExecutions | Deferred | Implemented |
| CountActivityExecutions | Deferred | Implemented |
| RequestCancelActivityExecution | Deferred | Implemented |
| TerminateActivityExecution | Deferred | Implemented |
| DeleteActivityExecution | Deferred | Implemented |

### Existing: `worker-deployments` (P10)

| RPC | Current | Target |
|-----|---------|--------|
| DescribeWorkerDeploymentVersion | Deferred | Implemented |
| SetWorkerDeploymentCurrentVersion | Deferred | Implemented |
| DescribeWorkerDeployment | Deferred | Implemented |
| DeleteWorkerDeployment | Deferred | Implemented |
| DeleteWorkerDeploymentVersion | Deferred | Implemented |
| SetWorkerDeploymentRampingVersion | Deferred | Implemented |
| ListWorkerDeployments | Deferred | Implemented |
| CreateWorkerDeployment | Deferred | Implemented |
| CreateWorkerDeploymentVersion | Deferred | Implemented |
| UpdateWorkerDeploymentVersionComputeConfig | Deferred | Implemented |
| ValidateWorkerDeploymentVersionComputeConfig | Deferred | Implemented |
| UpdateWorkerDeploymentVersionMetadata | Deferred | Implemented |
| SetWorkerDeploymentManager | Deferred | Implemented |
| DescribeDeployment (deprecated) | Stubbed | Implemented |

### Existing: `workflow-rules` (P11)

| RPC | Current | Target |
|-----|---------|--------|
| CreateWorkflowRule | Deferred | Implemented |
| DescribeWorkflowRule | Deferred | Implemented |
| DeleteWorkflowRule | Deferred | Implemented |
| ListWorkflowRules | Deferred | Implemented |
| TriggerWorkflowRule | Deferred | Implemented |

### Existing: `worker-config-management` (P4)

| RPC | Current | Target |
|-----|---------|--------|
| FetchWorkerConfig | Deferred | Implemented |
| UpdateWorkerConfig | Deferred | Implemented |
| ListWorkers | Deferred | Implemented |
| DescribeWorker | Deferred | Implemented |

---

## RPCs not requiring new specs

These RPCs are already Partial and will reach Implemented through the field-level specs above or through existing handler improvements:

| RPC | Current | Covered by |
|-----|---------|------------|
| RegisterNamespace | Partial | spec 9 (namespace-full) |
| DescribeNamespace | Partial | spec 9 |
| ListNamespaces | Partial | spec 9 |
| RequestCancelWorkflowExecution | Partial | spec 3 (start-fields covers cancel propagation) |
| TerminateWorkflowExecution | Partial | spec 3 |
| ResetWorkflowExecution | Partial | existing handler (conformance testing) |
| DeleteWorkflowExecution | Partial | existing handler (conformance testing) |
| GetWorkflowExecutionHistory | Partial | spec 5 (activity-events improves event fidelity) |
| GetWorkflowExecutionHistoryReverse | Partial | spec 5 |
| PollWorkflowTaskQueue | Partial | spec 4 (wft-completion covers sticky) |
| RespondWorkflowTaskFailed | Partial | existing handler (conformance testing) |
| ResetStickyTaskQueue | Partial | spec 4 |
| RespondActivityTaskCompleted | Partial | spec 5 |
| RespondActivityTaskFailed | Partial | spec 5 |
| QueryWorkflow | Partial | existing handler (conformance testing) |
| RespondQueryTaskCompleted | Partial | existing handler (conformance testing) |
| ListWorkflowExecutions | Partial | existing handler (projection) |
| CountWorkflowExecutions | Partial | existing handler (projection) |
| GetClusterInfo | Partial | existing handler |
| GetSystemInfo | Partial | temporal-compatibility spec |
| UpdateWorkerVersioningRules | Partial | existing handler (conformance testing) |
| GetWorkerVersioningRules | Partial | existing handler (conformance testing) |
| GetWorkerTaskReachability | Partial | existing handler (conformance testing) |
| RecordWorkerHeartbeat | Partial | existing handler |
| ShutdownWorker | Partial | existing handler |
| DescribeTaskQueue | Partial | spec 14 |
| UpdateTaskQueueConfig | Partial | existing handler |
| PollNexusTaskQueue | Partial | spec 11 (nexus-admin completes the surface) |
| RespondNexusTaskCompleted | Partial | spec 11 |
| RespondNexusTaskFailed | Partial | spec 11 |
| AddSearchAttributes | Partial | spec 10 (visibility-legacy) |
| ListSearchAttributes | Partial | spec 10 |

---

## Deprecated RPCs (implement for backward compatibility)

| RPC | Current | Covered by |
|-----|---------|------------|
| DeprecateNamespace | Stubbed | spec 9 |
| ScanWorkflowExecutions | Stubbed | spec 10 |
| UpdateWorkerBuildIdCompatibility | Stubbed | intentional — v2 preferred |
| GetWorkerBuildIdCompatibility | Stubbed | intentional — v2 preferred |
| ListDeployments (deprecated) | Stubbed | worker-deployments |
| GetDeploymentReachability (deprecated) | Stubbed | worker-deployments |
| GetCurrentDeployment (deprecated) | Stubbed | worker-deployments |
| SetCurrentDeployment (deprecated) | Stubbed | worker-deployments |
| UpdateActivityOptions (deprecated) | Stubbed | spec 1 |
| PauseActivity (deprecated) | Stubbed | activity-executions-first-class |
| UnpauseActivity (deprecated) | Stubbed | activity-executions-first-class |
| ResetActivity (deprecated) | Stubbed | activity-executions-first-class |

---

## Priority order for implementation

1. **Specs 1–5** — Core SDK execution path (activity by-id, describe, start fields, WFT completion, activity events)
2. **Specs 6–7** — Update lifecycle and signal headers (SDK-visible gaps)
3. **Spec 8** — Schedule field fidelity
4. **Specs 14–16** — Task queue, batch fields, workflow options (small, independent)
5. **Specs 9–12** — Namespace, visibility legacy, nexus admin, remote cluster (operator-facing)
6. **Spec 13** — Multi-operation (complex, low SDK adoption currently)
7. **Existing specs** — P4, P7, P8, P10, P11 (larger features with kernel/runtime work)
---

## Conformance Infrastructure (from `temporal-compatibility` spec)

Once the API surface is conformant, the following infrastructure validates and advertises that conformance. These tasks are blocked until the RPC-level work above is substantially complete.

| Task area | Source spec | Status | Blocked on |
|-----------|-------------|--------|------------|
| Kernel `cfg_feature!` adoption (all features) | `temporal-compatibility` task 4 | Partially started (Implemented features only) | Full matrix correction |
| Edge `dispatch_rpc` adoption (all handlers) | `temporal-compatibility` task 5 | Partially started (Implemented features only) | Full matrix correction |
| Buffa/connect-rust codegen | `temporal-compatibility` task 7.3 | Blocked | Buffa/connect-rust toolchain setup |
| Compatibility service wiring (all processes) | `temporal-compatibility` task 7.7 | Blocked | Task 7.4 + connect-rust server |
| Dagger compatibility module | `temporal-compatibility` tasks 10, 11 | Blocked | `pipeline-foundation` spec |
| Generated-code freshness checks | `temporal-compatibility` task 10.4 | Blocked | Tasks 7.3 + 10.1 |
| `tkr compat show --remote` integration test | `temporal-compatibility` task 8.6 | Blocked | Task 7.7 |

**Sequencing:** Complete conformance specs (priority 1–7 above) → correct the feature matrix → adopt `cfg_feature!` and `dispatch_rpc` broadly → implement Buffa/connect-rust service → wire Dagger CI → release engineering.
