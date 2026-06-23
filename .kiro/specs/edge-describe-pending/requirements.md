# Requirements Document: Edge Describe & Operational Response Completeness

## Introduction

This spec addresses the operational response completeness gaps identified in `../edge-complete-implementation/reference/proto-field-audit.md` §2 — the `DescribeWorkflowExecution`, `DescribeNamespace`, `GetClusterInfo`, and `DescribeTaskQueue` responses that return hardcoded empty/zero values instead of real data. The Temporal UI and operational tooling depend on these fields for workflow inspection, namespace configuration display, and cluster health monitoring.

This is Feature 4 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features and is the lowest priority among the field fidelity gaps. The work divides into two categories:

1. **Pending entity extraction** (Requirements 1–3): The main work. `DescribeWorkflowExecutionResponse` is missing `pending_activities`, `pending_children`, and `pending_workflow_task`. The kernel's `WorkflowState` already carries `activities: BTreeMap<String, ActivityState>`, `children: BTreeMap<WorkflowId, ChildWorkflowState>`, and `pending_workflow_task: Option<PendingWorkflowTask>`. The data exists — it needs to be threaded through the `ExecutionResolver` → edge DTO → proto translation pipeline.

2. **Cosmetic defaults** (Requirements 4–6): `DescribeNamespaceResponse`, `GetClusterInfoResponse`, and `DescribeTaskQueueResponse` hardcode empty/zero values where sensible defaults or configuration-derived values should appear. Tokeira is single-cluster with no archival support — the fix is to set explicit disabled/default values rather than ambiguous zeros.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **ExecutionResolver**: The trait in `tokeira-edge/src/workflow_service.rs` that provides `describe_execution` — the runtime implements this to return `WorkflowExecutionDescription` for a given workflow.
- **WorkflowExecutionDescription**: The edge DTO in `tokeira-edge/src/translate/mod.rs` that carries describe response data from the runtime to the proto translation layer.
- **WorkflowState**: The `tokeira_kernel::state::WorkflowState` struct that holds the durable summary state for an open or closed workflow run, including `activities`, `children`, and `pending_workflow_task`.
- **ActivityState**: The `tokeira_kernel::state::ActivityState` struct tracking an open activity's ID, type, attempt, timestamps, and heartbeat timeout.
- **ChildWorkflowState**: The `tokeira_kernel::state::ChildWorkflowState` struct tracking an open child workflow's ID, run ID, initiated event ID, and parent close policy.
- **PendingWorkflowTask**: The `tokeira_kernel::state::PendingWorkflowTask` struct tracking the currently pending workflow task's scheduled/started event IDs, timestamps, and attempt count.
- **PendingActivityInfo**: The proto `temporal.api.workflow.v1.PendingActivityInfo` message within `DescribeWorkflowExecutionResponse`.
- **PendingChildExecutionInfo**: The proto `temporal.api.workflow.v1.PendingChildExecutionInfo` message within `DescribeWorkflowExecutionResponse`.
- **PendingWorkflowTaskInfo**: The proto `temporal.api.workflow.v1.PendingWorkflowTaskInfo` message within `DescribeWorkflowExecutionResponse`.
- **ClusterInfo**: The `tokeira_edge::operator_service::ClusterInfo` struct that carries cluster metadata for `GetClusterInfoResponse`.
- **NamespaceDescription**: The edge DTO in `tokeira-edge/src/translate/mod.rs` that carries namespace metadata for `DescribeNamespaceResponse`.
- **PollerInfo**: The edge DTO in `tokeira-edge/src/translate/mod.rs` that carries poller metadata for `DescribeTaskQueueResponse`.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

---

### Requirement 1: DescribeWorkflowExecution — pending_activities

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_activities`, so that I can see which activities are currently in progress for a workflow.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow with open activities, THE Edge_Layer SHALL populate `pending_activities` with a `PendingActivityInfo` entry for each open activity in the workflow's state.
2. EACH `PendingActivityInfo` SHALL include `activity_id` from `ActivityState.activity_id`, `activity_type` from `ActivityState.activity_type`, `attempt` from `ActivityState.attempt`, and `scheduled_time` from `ActivityState.scheduled_at`.
3. EACH `PendingActivityInfo` SHALL include `state` set to `PENDING_ACTIVITY_STATE_STARTED` when `ActivityState.started_at` is `Some`, and `PENDING_ACTIVITY_STATE_SCHEDULED` when `ActivityState.started_at` is `None`.
4. EACH `PendingActivityInfo` SHALL include `last_started_time` from `ActivityState.started_at` when the activity has been started.
5. NOTE: The upstream proto `PendingActivityInfo` does not have dedicated timeout fields (`heartbeat_timeout`, `schedule_to_close_timeout`, `start_to_close_timeout`). These timeouts are available in `ActivityState` but cannot be serialized to the proto. The proto does have `expiration_time` which could be computed from `scheduled_at + schedule_to_close_timeout`, but this is deferred for now.
6. NOTE: The upstream proto `PendingActivityInfo` has a `PENDING_ACTIVITY_STATE_CANCEL_REQUESTED` state, but Tokeira tracks cancel-requested status in the runtime-local activity-timeout tracker rather than in durable `WorkflowState`. Since this feature extracts data from `WorkflowState` only (kernel unchanged), `CANCEL_REQUESTED` cannot be surfaced. Activities with a pending cancellation will appear as `SCHEDULED` or `STARTED` based on their durable state. This is a known limitation.
6. WHEN the workflow has no open activities, THE Edge_Layer SHALL return an empty `pending_activities` list.
7. WHEN the ExecutionResolver provides workflow execution description data to the Edge_Layer, THE ExecutionResolver SHALL include the list of open activities with their current state.
8. THE `WorkflowExecutionDescription` edge DTO SHALL carry a `pending_activities: Vec<PendingActivityDescription>` field, where `PendingActivityDescription` is a new edge DTO struct mirroring the relevant `ActivityState` fields.

### Requirement 2: DescribeWorkflowExecution — pending_children

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_children`, so that I can see which child workflows are currently in progress.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow with open child workflows, THE Edge_Layer SHALL populate `pending_children` with a `PendingChildExecutionInfo` entry for each open child workflow in the workflow's state.
2. EACH `PendingChildExecutionInfo` SHALL include `workflow_id` from `ChildWorkflowState.child_workflow_id`, `run_id` from `ChildWorkflowState.child_run_id` (if started), `initiated_id` from `ChildWorkflowState.initiated_event_id`, and `parent_close_policy` from `ChildWorkflowState.parent_close_policy`.
3. NOTE: `PendingChildExecutionInfo.workflow_type_name` will be set to empty string because `ChildWorkflowState` does not currently carry the child's workflow type. Adding `workflow_type: WorkflowType` to `ChildWorkflowState` is the correct long-term fix but requires a kernel change that is out of scope for this operational spec. The field is informational — the UI can still display the child workflow ID.
3. WHEN the workflow has no open child workflows, THE Edge_Layer SHALL return an empty `pending_children` list.
4. WHEN the ExecutionResolver provides workflow execution description data to the Edge_Layer, THE ExecutionResolver SHALL include the list of open child workflows with their current state.
5. THE `WorkflowExecutionDescription` edge DTO SHALL carry a `pending_children: Vec<PendingChildDescription>` field, where `PendingChildDescription` is a new edge DTO struct mirroring the relevant `ChildWorkflowState` fields.

### Requirement 3: DescribeWorkflowExecution — pending_workflow_task

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_workflow_task`, so that I can see whether a workflow task is currently pending or in progress.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow with a pending workflow task, THE Edge_Layer SHALL populate `pending_workflow_task` with a `PendingWorkflowTaskInfo` entry.
2. THE `PendingWorkflowTaskInfo` SHALL include `state` set to `PENDING_WORKFLOW_TASK_STATE_STARTED` when `PendingWorkflowTask.started_event_id` is `Some`, and `PENDING_WORKFLOW_TASK_STATE_SCHEDULED` when `PendingWorkflowTask.started_event_id` is `None`.
3. THE `PendingWorkflowTaskInfo` SHALL include `scheduled_time` from `PendingWorkflowTask.scheduled_at`, `started_time` from `PendingWorkflowTask.started_at` (when started), and `attempt` from `PendingWorkflowTask.attempt`.
4. WHEN the workflow has no pending workflow task, THE Edge_Layer SHALL leave `pending_workflow_task` as None.
5. WHEN the ExecutionResolver provides workflow execution description data to the Edge_Layer, THE ExecutionResolver SHALL include the pending workflow task state if one exists.
6. THE `WorkflowExecutionDescription` edge DTO SHALL carry a `pending_workflow_task: Option<PendingWorkflowTaskDescription>` field, where `PendingWorkflowTaskDescription` is a new edge DTO struct mirroring the relevant `PendingWorkflowTask` fields.

### Requirement 4: DescribeNamespaceResponse — configuration fields

**User Story:** As a Tokeira operator, I want `DescribeNamespaceResponse` to include archival state, replication, and cluster configuration fields with sensible values, so that operational tooling can inspect namespace configuration without encountering ambiguous zeros.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeNamespaceResponse`, THE Edge_Layer SHALL set `history_archival_state` to `ARCHIVAL_STATE_DISABLED` (proto enum value 1) and `visibility_archival_state` to `ARCHIVAL_STATE_DISABLED` (proto enum value 1), because Tokeira does not support archival.
2. WHEN the Edge_Layer builds a `DescribeNamespaceResponse`, THE Edge_Layer SHALL populate `clusters` with at least one `ClusterReplicationConfig` entry containing the local cluster name (from `ClusterInfo.cluster_name`), rather than returning an empty list.
3. WHEN the Edge_Layer builds a `DescribeNamespaceResponse`, THE Edge_Layer SHALL set `failover_version` to a non-zero default (e.g., 1) rather than hardcoding 0, because Tokeira is single-cluster and does not support failover.
4. THE `NamespaceDescription` edge DTO SHALL carry fields for `description: String`, `owner_email: String`, and `custom_search_attribute_aliases: BTreeMap<String, String>` so that the proto translation layer can populate them from namespace configuration rather than hardcoding empty values.
5. IF the namespace configuration does not provide `description` or `owner_email`, THE Edge_Layer SHALL use empty strings as defaults.

### Requirement 5: GetClusterInfoResponse — version and client fields

**User Story:** As a Tokeira operator, I want `GetClusterInfoResponse` to include `supported_clients`, `version_info`, and `history_shard_count`, so that SDK clients can verify compatibility and operational tooling can inspect cluster configuration.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `supported_clients` with a map of SDK language to minimum supported version. The initial set SHALL include at least `{"temporal-go": "1.26.0", "temporal-java": "1.22.0", "temporal-python": "1.6.0", "temporal-typescript": "1.10.0"}` or the versions that Tokeira's proto API version supports.
2. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `version_info` with a `VersionInfo` message containing the Tokeira server version (from `ClusterInfo.version`).
3. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `history_shard_count` with the actual shard count from the runtime configuration rather than hardcoding 0. IF the shard count is not available, THE Edge_Layer SHALL use 1 as the default (single-shard).
4. THE `ClusterInfo` struct SHALL carry a `shard_count: i32` field so that the proto translation layer can populate `history_shard_count`.
5. THE `ClusterInfo` struct SHALL carry a `supported_clients: BTreeMap<String, String>` field so that the proto translation layer can populate the `supported_clients` map.

### Requirement 6: DescribeTaskQueueResponse — worker versioning documentation

**User Story:** As a Tokeira operator, I want `DescribeTaskQueueResponse` to clearly indicate that worker versioning capabilities are not yet supported, so that operational tooling does not misinterpret absent fields as errors.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeTaskQueueResponse`, THE Edge_Layer SHALL continue to set `worker_version_capabilities` to `None` on each `PollerInfo` entry, because Tokeira does not yet support worker versioning capabilities on pollers.
2. WHEN the Edge_Layer builds a `DescribeTaskQueueResponse`, THE Edge_Layer SHALL continue to set `versions_info` to the default empty value, because Tokeira does not yet support task queue versioning info.
3. THE codebase SHALL include a documentation comment on the `describe_task_queue_response_to_proto` function noting that `worker_version_capabilities` and `versions_info` are intentionally unsupported, replacing the current silent `None`/`Default::default()` with an explicit comment explaining the omission.
