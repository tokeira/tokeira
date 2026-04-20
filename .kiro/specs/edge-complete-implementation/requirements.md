# Requirements Document: Edge Complete Implementation

## Introduction

This document captures the full requirements for completing the `tokeira-edge` translation layer — both proto field fidelity and unimplemented gRPC handler categories. An audit (`docs/proto-field-audit.md`) identified ~100+ proto fields silently dropped via `..Default::default()`, 40+ `unwrap_or_default()` calls, incomplete history serialization, and 44 unimplemented gRPC handlers across major feature categories.

The initial `edge-proto-audit` spec addressed the first wave: command translation, activity data threading, history serializer basics, and long-poll for `GetWorkflowExecutionHistory`. This umbrella spec covers the **remaining gaps** — everything the audit identified that was not resolved by `edge-proto-audit`, plus the unimplemented handler categories that represent major feature areas.

The guiding principle: **every attribute of every upstream Temporal API proto message must be faithfully translated or explicitly documented as unsupported. Every gRPC handler must be implemented or return a clear unsupported status with documentation.**

The implementation is organized into 9 features with explicit dependency ordering. Features 1–4 address proto field fidelity gaps. Features 5–9 address unimplemented gRPC handler categories and optimizations that represent major Temporal feature areas.

**Dependency graph:**

- Feature 1 (Poll Response Fidelity) — no dependencies
- Feature 2 (Failure Object Completeness) — no dependencies
- Feature 3 (History Event Field Completeness) — depends on Feature 1 (WFT attributes overlap)
- Feature 4 (Describe and Operational Responses) — no dependencies, lowest priority among field fidelity
- Feature 5 (Worker Versioning Transport) — depends on Feature 4 (PollerInfo versioning fields)
- Feature 6 (Schedule Transport) — no dependencies
- Feature 7 (Batch Operations Transport) — no dependencies
- Feature 8 (Nexus Task Transport) — no dependencies
- Feature 9 (Eager Dispatch) — depends on Features 1, 2 (poll response and failure fidelity)

The actual design and tasks for each feature will live in child specs:
- `edge-poll-response-fidelity` (Feature 1)
- `edge-failure-objects` (Feature 2)
- `edge-history-parent-chain` (Feature 3)
- `edge-describe-pending` (Feature 4)
- `edge-worker-versioning-transport` (Feature 5)
- `edge-schedule-transport` (Feature 6)
- `edge-batch-operations-transport` (Feature 7)
- `edge-nexus-task-transport` (Feature 8)
- `edge-eager-dispatch` (Feature 9)

## Audit Gap Traceability

The table below maps every gap identified in `docs/proto-field-audit.md` to the feature that addresses it. Items already resolved by the `edge-proto-audit` spec are marked as such.

### §1 — Unimplemented gRPC Handlers (44)

| Category | Handler | Feature | Notes |
|---|---|---|---|
| Namespace | `update_namespace` | Deferred | Low priority; namespace mutation not needed for SDK correctness |
| Namespace | `deprecate_namespace` | Deferred | Low priority |
| Namespace | `execute_multi_operation` | Deferred | Advanced feature; not needed for SDK correctness |
| Activity by-ID | `record_activity_task_heartbeat_by_id` | Deferred | Convenience alias; standard heartbeat works |
| Activity by-ID | `respond_activity_task_completed_by_id` | Deferred | Convenience alias; standard completion works |
| Activity by-ID | `respond_activity_task_failed_by_id` | Deferred | Convenience alias; standard failure works |
| Activity by-ID | `respond_activity_task_canceled` | Deferred | Convenience alias; standard cancel works |
| Activity by-ID | `respond_activity_task_canceled_by_id` | Deferred | Convenience alias |
| Legacy listing | `list_open_workflow_executions` | Deferred | Superseded by `list_workflow_executions` with query |
| Legacy listing | `list_closed_workflow_executions` | Deferred | Superseded by `list_workflow_executions` with query |
| Legacy listing | `list_archived_workflow_executions` | Deferred | Requires archival support |
| Legacy listing | `scan_workflow_executions` | Deferred | Superseded by `list_workflow_executions` |
| Search/TaskQueue | `get_search_attributes` | Deferred | Low priority |
| Search/TaskQueue | `list_task_queue_partitions` | Deferred | Low priority |
| Scheduling | `create_schedule` | Feature 6 | Req 6.1 |
| Scheduling | `describe_schedule` | Feature 6 | Req 6.1 |
| Scheduling | `update_schedule` | Feature 6 | Req 6.1 |
| Scheduling | `patch_schedule` | Feature 6 | Req 6.2 |
| Scheduling | `list_schedule_matching_times` | Feature 6 | Req 6.2 |
| Scheduling | `delete_schedule` | Feature 6 | Req 6.1 |
| Scheduling | `list_schedules` | Feature 6 | Req 6.2 |
| Worker versioning | `update_worker_build_id_compatibility` | Feature 5 | Req 5.2 |
| Worker versioning | `get_worker_build_id_compatibility` | Feature 5 | Req 5.2 |
| Worker versioning | `update_worker_versioning_rules` | Feature 5 | Req 5.1 |
| Worker versioning | `get_worker_versioning_rules` | Feature 5 | Req 5.1 |
| Worker versioning | `get_worker_task_reachability` | Feature 5 | Req 5.3 |
| Worker versioning | `shutdown_worker` | Feature 5 | Req 5.4 |
| Deployment | `describe_deployment` | Feature 5 | Req 5.5 |
| Deployment | `list_deployments` | Feature 5 | Req 5.5 |
| Deployment | `get_deployment_reachability` | Feature 5 | Req 5.5 |
| Deployment | `get_current_deployment` | Feature 5 | Req 5.5 |
| Deployment | `set_current_deployment` | Feature 5 | Req 5.5 |
| Batch | `start_batch_operation` | Feature 7 | Req 7.1 |
| Batch | `stop_batch_operation` | Feature 7 | Req 7.2 |
| Batch | `describe_batch_operation` | Feature 7 | Req 7.2 |
| Batch | `list_batch_operations` | Feature 7 | Req 7.2 |
| Nexus | `poll_nexus_task_queue` | Feature 8 | Req 8.1 |
| Nexus | `respond_nexus_task_completed` | Feature 8 | Req 8.2 |
| Nexus | `respond_nexus_task_failed` | Feature 8 | Req 8.3 |
| Activity/WF options | `update_activity_options_by_id` | Deferred | Requires activity options model |
| Activity/WF options | `update_workflow_execution_options` | Deferred | Partially supported via kernel |
| Activity/WF options | `pause_activity_by_id` | Deferred | Requires activity pause model |
| Activity/WF options | `unpause_activity_by_id` | Deferred | Requires activity pause model |
| Activity/WF options | `reset_activity_by_id` | Deferred | Requires activity reset model |

### §2 — Response Fields Silently Dropped via `..Default::default()`

| Response | Proto Field | Feature | Notes |
|---|---|---|---|
| PollWorkflowTaskQueueResponse | `previous_started_event_id` | Feature 1 | Req 1.1 — critical for SDK replay |
| PollWorkflowTaskQueueResponse | `backlog_count_hint` | Feature 4 | Low priority; informational |
| PollWorkflowTaskQueueResponse | `next_page_token` | Deferred | Paginated history delivery; not needed for correctness |
| PollWorkflowTaskQueueResponse | `query` | Deferred | Legacy query field; superseded by query dispatch |
| PollWorkflowTaskQueueResponse | `scheduled_time` | Feature 1 | Req 1.4 |
| PollWorkflowTaskQueueResponse | `started_time` | Feature 1 | Req 1.4 |
| RespondWorkflowTaskCompletedResponse | `activity_tasks` | Feature 9 | Req 9.2 — eager activity return |
| RespondWorkflowTaskCompletedResponse | `reset_history_event_id` | Deferred | Reset support |
| DescribeWorkflowExecutionResponse | `pending_activities` | Feature 4 | Req 4.1 |
| DescribeWorkflowExecutionResponse | `pending_children` | Feature 4 | Req 4.2 |
| DescribeWorkflowExecutionResponse | `pending_workflow_task` | Feature 4 | Req 4.3 |
| StartWorkflowExecutionResponse | `started` | Feature 1 | Req 1.2 |
| StartWorkflowExecutionResponse | `eager_workflow_task` | Feature 9 | Req 9.1 — eager workflow task |
| DescribeNamespaceResponse | `description` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `owner_email` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `data` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `history_archival_state/uri` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `visibility_archival_state/uri` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `custom_search_attribute_aliases` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `clusters` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `failover_version` | Feature 4 | Req 4.4 |
| DescribeNamespaceResponse | `failover_history` | Feature 4 | Req 4.4 |
| GetClusterInfoResponse | `supported_clients` | Feature 4 | Req 4.5 |
| GetClusterInfoResponse | `version_info` | Feature 4 | Req 4.5 |
| GetClusterInfoResponse | `history_shard_count` | Feature 4 | Req 4.5 |
| DescribeTaskQueueResponse | `versions_info` | Feature 5 | Req 5.1 |
| DescribeTaskQueueResponse | `worker_version_capabilities` | Feature 4 | Req 4.6 |

### §3 — History Serializer Missing Event Attribute Fields

| Event | Missing Field(s) | Feature | Notes |
|---|---|---|---|
| All failure-bearing events | `failure_info` variants | Feature 2 | Req 2.1 — affects 6 event types |
| All failure-bearing events | `cause` (chained failure) | Feature 2 | Req 2.2 |
| All failure-bearing events | `stack_trace` | Feature 2 | Req 2.1 |
| All failure-bearing events | `source` | Feature 2 | Req 2.1 |
| All failure-bearing events | `encoded_attributes` | Feature 2 | Req 2.3 |
| WorkflowExecutionStarted | `parent_workflow_execution` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `parent_workflow_type` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `parent_initiated_event_id` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `continued_failure` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `last_completion_result` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `original_execution_run_id` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `first_execution_run_id` | Feature 3 | Req 3.1 |
| WorkflowExecutionStarted | `cron_schedule` | Feature 6 | Requires schedule support |
| WorkflowExecutionContinuedAsNew | `workflow_execution_timeout` | Feature 3 | Req 3.2 |
| WorkflowExecutionContinuedAsNew | `retry_policy` | Feature 3 | Req 3.2 |
| WorkflowExecutionContinuedAsNew | `initiator` | Feature 3 | Req 3.2 |
| WorkflowExecutionContinuedAsNew | `failure` | Feature 3 | Req 3.2 |
| WorkflowExecutionContinuedAsNew | `last_completion_result` | Feature 3 | Req 3.2 |
| WorkflowTaskScheduled | `task_queue` | Feature 1 | Req 1.3 |
| WorkflowTaskScheduled | `start_to_close_timeout` | Feature 1 | Req 1.3 |
| WorkflowTaskScheduled | `attempt` | Feature 1 | Req 1.3 |
| ActivityTaskScheduled | `schedule_to_close_timeout` | Feature 3 | Req 3.3 — verify serialization |
| ActivityTaskScheduled | `schedule_to_start_timeout` | Feature 3 | Req 3.3 — verify serialization |
| ActivityTaskScheduled | `start_to_close_timeout` | Feature 3 | Req 3.3 — verify serialization |
| ActivityTaskScheduled | `heartbeat_timeout` | Feature 3 | Req 3.3 — verify serialization |
| SignalExternalWorkflowExecutionInitiated | `control` | Feature 3 | Req 3.4 |
| RequestCancelExternalWorkflowExecutionInitiated | `control` | Feature 3 | Req 3.4 |

### §4 — `unwrap_or_default()` Silent Data Loss

| Location | Field | Feature | Notes |
|---|---|---|---|
| `start_request_to_edge` | `workflow_type` | Resolved | Fixed in `edge-proto-audit` |
| `start_request_to_edge` | `input` | Resolved | Fixed in `edge-proto-audit` |
| `start_request_to_edge` | `memo` | Resolved | Fixed in `edge-proto-audit` |
| `signal_request_to_edge` | `input` | Resolved | Fixed in `edge-proto-audit` |
| `respond_completed_request_to_edge` | query result `answer` | Resolved | Fixed in `edge-proto-audit` |
| `namespace_to_proto` | `namespace_id` | Feature 4 | Req 4.4 |
| `signal_with_start_request_to_edge` | `workflow_type` | Resolved | Fixed in `edge-proto-audit` |

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **History_Serializer**: The module `tokeira-edge/src/translate/history_serializer.rs` that converts kernel `HistoryEvent` values into proto `temporal.api.history.v1.History` messages.
- **Poll_Response**: The proto `PollWorkflowTaskQueueResponse` or `PollActivityTaskQueueResponse` returned to SDK workers when they poll for tasks.
- **Failure_Object**: The proto `temporal.api.failure.v1.Failure` message, which carries structured failure information including `failure_info` variants (ApplicationFailureInfo, TimeoutFailureInfo, CanceledFailureInfo, etc.), `cause` chains, `stack_trace`, and `encoded_attributes`.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.
- **Proto_Field_Audit**: The comprehensive audit document at `docs/proto-field-audit.md` identifying all dropped, hardcoded, and missing proto fields.
- **Describe_Response**: The proto `DescribeWorkflowExecutionResponse` returned by the `DescribeWorkflowExecution` gRPC endpoint.
- **Pending_Info**: The proto `PendingActivityInfo`, `PendingChildExecutionInfo`, and `PendingWorkflowTaskInfo` sub-messages within `DescribeWorkflowExecutionResponse`.
- **Worker_Versioning**: The Temporal feature allowing workers to register with version/deployment metadata, enabling task routing to compatible workers. Exposed via 6 gRPC handlers (`update_worker_build_id_compatibility`, `get_worker_build_id_compatibility`, `update_worker_versioning_rules`, `get_worker_versioning_rules`, `get_worker_task_reachability`, `shutdown_worker`).
- **Schedule**: The Temporal feature for cron-like recurring workflow execution. Exposed via 7 gRPC handlers (`create_schedule`, `describe_schedule`, `update_schedule`, `patch_schedule`, `list_schedule_matching_times`, `delete_schedule`, `list_schedules`).
- **Batch_Operation**: The Temporal feature for bulk operations on workflow executions (terminate, cancel, signal, reset, delete). Exposed via 4 gRPC handlers (`start_batch_operation`, `stop_batch_operation`, `describe_batch_operation`, `list_batch_operations`).
- **Nexus_Task**: The Temporal feature for cross-namespace service invocation through typed contracts. The edge layer must handle 3 gRPC handlers for Nexus worker polling and completion (`poll_nexus_task_queue`, `respond_nexus_task_completed`, `respond_nexus_task_failed`).
- **Deployment**: The Temporal feature for managing worker deployments. Exposed via 5 gRPC handlers (`describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment`).
- **Eager_Dispatch**: An optimization where the server returns a workflow task or activity task inline with the response that triggered it (e.g., `StartWorkflowExecutionResponse.eager_workflow_task` or `RespondWorkflowTaskCompletedResponse.activity_tasks`), avoiding a separate poll round-trip.

## Requirements

---

## Feature 1: Poll Response Fidelity

### Requirement 1.1: PollWorkflowTaskQueueResponse — previous_started_event_id

**User Story:** As an SDK user, I want `PollWorkflowTaskQueueResponse` to include `previous_started_event_id`, so that the SDK can determine the sticky replay boundary and avoid replaying the entire history on every workflow task.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate `previous_started_event_id` with the event ID of the `WorkflowTaskStarted` event from the previous workflow task completion.
2. WHEN the workflow task is the first task for the run (no previous completion exists), THE Edge_Layer SHALL set `previous_started_event_id` to 0.
3. WHEN the runtime provides a `StartedWorkflowTask` to the Edge_Layer, THE runtime SHALL include the `previous_started_event_id` value derived from the run's authoritative state.

### Requirement 1.2: StartWorkflowExecutionResponse — started field

**User Story:** As an SDK user, I want `StartWorkflowExecutionResponse` to include the `started` field, so that the SDK can distinguish "started new workflow" from "returned existing workflow" when using conflict policies.

#### Acceptance Criteria

1. WHEN a new workflow execution is created by a `StartWorkflowExecution` request, THE Edge_Layer SHALL set the `started` field to `true` on the response.
2. WHEN a `StartWorkflowExecution` request returns an existing execution due to a conflict policy (e.g., `UseExisting`), THE Edge_Layer SHALL set the `started` field to `false`.

### Requirement 1.3: WorkflowTaskScheduled — task_queue, start_to_close_timeout, attempt

**User Story:** As an SDK user, I want `WorkflowTaskScheduledEventAttributes` in history to include `task_queue`, `start_to_close_timeout`, and `attempt`, so that the SDK has complete information during replay.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `task_queue` field from the run's task queue.
2. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `start_to_close_timeout` field from the run's `workflow_task_timeout`.
3. WHEN the History_Serializer serializes a `WorkflowTaskScheduled` event, THE History_Serializer SHALL populate the `attempt` field from the pending workflow task's attempt count.
4. WHEN the kernel emits a `WorkflowTaskScheduled` event, THE kernel SHALL include the task queue name, workflow task timeout, and attempt count in the event data.

### Requirement 1.4: PollWorkflowTaskQueueResponse — scheduled_time and started_time

**User Story:** As an SDK user, I want `PollWorkflowTaskQueueResponse` to include `scheduled_time` and `started_time`, so that the SDK can compute task latency and the runtime can enforce workflow task timeouts accurately.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate `scheduled_time` with the timestamp of the `WorkflowTaskScheduled` event.
2. WHEN the Edge_Layer builds a `PollWorkflowTaskQueueResponse`, THE Edge_Layer SHALL populate `started_time` with the timestamp of the `WorkflowTaskStarted` event.
3. WHEN the runtime provides a `StartedWorkflowTask` to the Edge_Layer, THE runtime SHALL include the scheduled and started timestamps.

---

## Feature 2: Failure Object Completeness

### Requirement 2.1: Structured failure_info on all failure-bearing events

**User Story:** As an SDK user, I want failure objects in history events to include structured `failure_info` (ApplicationFailureInfo, TimeoutFailureInfo, CanceledFailureInfo, etc.), so that the SDK can distinguish failure types and present meaningful error information to workflow code.

#### Acceptance Criteria

1. WHEN the History_Serializer constructs a `Failure` proto for a `WorkflowExecutionFailed` event, THE History_Serializer SHALL populate `failure_info` with `ApplicationFailureInfo` containing the error type and non-retryable flag.
2. WHEN the History_Serializer constructs a `Failure` proto for an `ActivityTaskFailed` event, THE History_Serializer SHALL populate `failure_info` with `ApplicationFailureInfo` containing the error type.
3. WHEN the History_Serializer constructs a `Failure` proto for an `ActivityTaskTimedOut` event, THE History_Serializer SHALL populate `failure_info` with `TimeoutFailureInfo` containing the timeout type and last heartbeat details.
4. WHEN the History_Serializer constructs a `Failure` proto for a `WorkflowTaskFailed` event, THE History_Serializer SHALL populate `failure_info` with `ApplicationFailureInfo` derived from the failure cause and details.
5. WHEN the History_Serializer constructs a `Failure` proto for a `ChildWorkflowExecutionFailed` event, THE History_Serializer SHALL populate `failure_info` with `ChildWorkflowExecutionFailureInfo` containing the child's namespace, workflow execution, workflow type, and retry state.
6. WHEN the History_Serializer constructs a `Failure` proto for a `MarkerRecorded` event with a failure, THE History_Serializer SHALL populate `failure_info` with `ApplicationFailureInfo`.

### Requirement 2.2: Failure cause chains

**User Story:** As an SDK user, I want failure objects to include the `cause` field for chained failures, so that the SDK can present the full failure chain to workflow code.

#### Acceptance Criteria

1. WHEN the kernel records a failure that has a causal chain (e.g., an activity failure that caused a workflow failure), THE kernel SHALL preserve the cause information in the event data.
2. WHEN the History_Serializer constructs a `Failure` proto with cause information available, THE History_Serializer SHALL populate the `cause` field with a nested `Failure` proto representing the original cause.

### Requirement 2.3: Failure encoded_attributes

**User Story:** As an SDK user, I want failure objects to include `encoded_attributes`, so that the SDK can decode failure details using the configured data converter.

#### Acceptance Criteria

1. WHEN the kernel records a failure with encoded detail payloads, THE kernel SHALL preserve the encoded attributes in the event data.
2. WHEN the History_Serializer constructs a `Failure` proto with encoded attributes available, THE History_Serializer SHALL populate the `encoded_attributes` field.

### Requirement 2.4: Kernel failure model enrichment

**User Story:** As a Tokeira developer, I want the kernel's failure representation to carry structured failure metadata (error type, non-retryable flag, timeout type, cause chain), so that the edge layer can construct complete `Failure` proto objects.

#### Acceptance Criteria

1. WHEN the kernel processes a `FailWorkflow` command, THE kernel SHALL record the error type and non-retryable flag in the `WorkflowExecutionFailed` event.
2. WHEN the kernel processes an `ActivityResolved` command with a `Failed` resolution, THE kernel SHALL record the error type in the `ActivityTaskFailed` event.
3. WHEN the kernel processes an `ActivityResolved` command with a `TimedOut` resolution, THE kernel SHALL record the last heartbeat details in the `ActivityTaskTimedOut` event.
4. WHEN the kernel processes an `ActivityResolved` command with a `Canceled` resolution, THE kernel SHALL record the cancellation details in the `ActivityTaskCanceled` event.

---

## Feature 3: History Event Field Completeness

### Requirement 3.1: WorkflowExecutionStarted — parent and chain fields

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include parent workflow information and execution chain fields, so that the SDK can correctly identify child workflows and trace execution chains.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a child workflow, THE History_Serializer SHALL populate `parent_workflow_execution` with the parent's workflow ID and run ID.
2. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a child workflow, THE History_Serializer SHALL populate `parent_workflow_namespace` and `parent_workflow_namespace_id` from the parent's namespace.
3. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a child workflow, THE History_Serializer SHALL populate `parent_initiated_event_id` with the event ID of the `StartChildWorkflowExecutionInitiated` event in the parent's history.
4. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a continued-as-new run, THE History_Serializer SHALL populate `continued_failure` with the failure from the predecessor run if the predecessor failed.
5. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a continued-as-new or retried run, THE History_Serializer SHALL populate `last_completion_result` with the result from the last successful run in the chain.
6. THE History_Serializer SHALL populate `original_execution_run_id` with the run ID of the original run that started the execution chain (before any retries or continue-as-new).
7. THE History_Serializer SHALL populate `first_execution_run_id` with the run ID of the first run in the current retry chain.
8. WHEN the kernel emits a `WorkflowExecutionStarted` event, THE kernel SHALL include parent workflow metadata, continued failure, last completion result, and execution chain run IDs in the event data.

### Requirement 3.2: WorkflowExecutionContinuedAsNew — missing fields

**User Story:** As an SDK user, I want `WorkflowExecutionContinuedAsNewEventAttributes` to include all timeout and retry configuration, so that the successor run inherits the correct execution parameters.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `WorkflowExecutionContinuedAsNew` event, THE History_Serializer SHALL populate `workflow_execution_timeout` from the kernel event data.
2. WHEN the History_Serializer serializes a `WorkflowExecutionContinuedAsNew` event, THE History_Serializer SHALL populate `retry_policy` from the kernel event data.
3. WHEN the History_Serializer serializes a `WorkflowExecutionContinuedAsNew` event, THE History_Serializer SHALL populate `initiator` with the continue-as-new initiator type (workflow command, retry, or cron).
4. WHEN the predecessor run failed before continuing as new, THE History_Serializer SHALL populate the `failure` field on the continued-as-new event attributes.
5. WHEN the predecessor run had a last completion result, THE History_Serializer SHALL populate `last_completion_result` on the continued-as-new event attributes.

### Requirement 3.3: ActivityTaskScheduled — timeout serialization verification

**User Story:** As an SDK user, I want `ActivityTaskScheduledEventAttributes` to include all four timeout fields faithfully, so that the SDK can enforce activity timeouts during replay.

#### Acceptance Criteria

1. FOR EVERY `ActivityTaskScheduled` event where the kernel carries `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, or `heartbeat_timeout`, THE History_Serializer SHALL populate the corresponding proto field.
2. THE History_Serializer SHALL NOT silently drop any timeout field that the kernel provides.

### Requirement 3.4: SignalExternalWorkflowExecutionInitiated and RequestCancelExternal — control field

**User Story:** As an SDK user, I want `SignalExternalWorkflowExecutionInitiatedEventAttributes` and `RequestCancelExternalWorkflowExecutionInitiatedEventAttributes` to include the `control` field, so that the SDK can correlate initiated events with their workflow commands during replay.

#### Acceptance Criteria

1. WHEN the History_Serializer serializes a `SignalExternalWorkflowExecutionInitiated` event, THE History_Serializer SHALL populate the `control` field from the kernel event data.
2. WHEN the History_Serializer serializes a `RequestCancelExternalWorkflowExecutionInitiated` event, THE History_Serializer SHALL populate the `control` field from the kernel event data.
3. WHEN the kernel emits signal-external or cancel-external initiated events, THE kernel SHALL include the `control` field in the event data.

---

## Feature 4: Describe and Operational Responses

### Requirement 4.1: DescribeWorkflowExecution — pending_activities

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_activities`, so that I can see which activities are currently in progress for a workflow.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow, THE Edge_Layer SHALL populate `pending_activities` with a `PendingActivityInfo` entry for each open activity in the workflow's state.
2. EACH `PendingActivityInfo` SHALL include `activity_id`, `activity_type`, `state` (scheduled, started, cancel-requested), `last_heartbeat_time`, `attempt`, `scheduled_time`, `last_started_time`, and `heartbeat_details`.
3. WHEN the runtime provides workflow execution description data to the Edge_Layer, THE runtime SHALL include the list of open activities with their current state.

### Requirement 4.2: DescribeWorkflowExecution — pending_children

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_children`, so that I can see which child workflows are currently in progress.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow, THE Edge_Layer SHALL populate `pending_children` with a `PendingChildExecutionInfo` entry for each open child workflow in the workflow's state.
2. EACH `PendingChildExecutionInfo` SHALL include `workflow_id`, `run_id`, `workflow_type_name`, `initiated_id`, and `parent_close_policy`.
3. WHEN the runtime provides workflow execution description data to the Edge_Layer, THE runtime SHALL include the list of open child workflows with their current state.

### Requirement 4.3: DescribeWorkflowExecution — pending_workflow_task

**User Story:** As an SDK user or UI operator, I want `DescribeWorkflowExecutionResponse` to include `pending_workflow_task`, so that I can see whether a workflow task is currently pending or in progress.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeWorkflowExecutionResponse` for an open workflow with a pending workflow task, THE Edge_Layer SHALL populate `pending_workflow_task` with a `PendingWorkflowTaskInfo` entry.
2. THE `PendingWorkflowTaskInfo` SHALL include `state` (scheduled or started), `scheduled_time`, `started_time` (if started), and `attempt`.
3. WHEN the runtime provides workflow execution description data to the Edge_Layer, THE runtime SHALL include the pending workflow task state if one exists.

### Requirement 4.4: Namespace configuration fields

**User Story:** As a Tokeira operator, I want `DescribeNamespaceResponse` to include archival and replication configuration fields, so that operational tooling can inspect namespace configuration.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeNamespaceResponse`, THE Edge_Layer SHALL populate `history_archival_state` and `visibility_archival_state` from the namespace's configuration rather than hardcoding them to defaults.
2. IF Tokeira does not support archival, THEN THE Edge_Layer SHALL set archival state to `ARCHIVAL_STATE_DISABLED` and document archival as unsupported.
3. WHEN the Edge_Layer builds a `DescribeNamespaceResponse`, THE Edge_Layer SHALL populate `clusters` and `failover_version` from the namespace's configuration rather than hardcoding them to empty/zero.

### Requirement 4.5: Cluster info fields

**User Story:** As a Tokeira operator, I want `GetClusterInfoResponse` to include `supported_clients` and `version_info`, so that SDK clients can verify compatibility.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `supported_clients` with the SDK client versions that Tokeira supports.
2. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `version_info` with the Tokeira server version and supported feature set.
3. WHEN the Edge_Layer builds a `GetClusterInfoResponse`, THE Edge_Layer SHALL populate `history_shard_count` from the actual shard configuration rather than hardcoding 0.

### Requirement 4.6: Worker versioning capabilities on PollerInfo

**User Story:** As a Tokeira operator, I want `DescribeTaskQueueResponse` to include `worker_version_capabilities` on `PollerInfo`, so that operational tooling can inspect worker version information.

#### Acceptance Criteria

1. WHEN the Edge_Layer builds a `DescribeTaskQueueResponse`, THE Edge_Layer SHALL populate `worker_version_capabilities` on each `PollerInfo` entry from the worker's registered capabilities.
2. IF worker versioning is not yet implemented, THEN THE Edge_Layer SHALL document `worker_version_capabilities` as unsupported rather than silently returning None.

---

## Feature 5: Worker Versioning Transport

### Requirement 5.1: Worker versioning rule management handlers

**User Story:** As a Tokeira operator, I want the edge layer to support worker versioning rule management (`update_worker_versioning_rules`, `get_worker_versioning_rules`), so that I can configure version-based task routing for task queues.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives an `UpdateWorkerVersioningRulesRequest`, THE Edge_Layer SHALL translate the request to the runtime's versioning rule storage and return an `UpdateWorkerVersioningRulesResponse` with the updated rules.
2. WHEN the Edge_Layer receives a `GetWorkerVersioningRulesRequest`, THE Edge_Layer SHALL query the runtime's versioning rule storage and return a `GetWorkerVersioningRulesResponse` with the current rules for the task queue.
3. FOR EVERY field in the upstream `UpdateWorkerVersioningRulesRequest` and `GetWorkerVersioningRulesResponse` proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 5.2: Worker build ID compatibility handlers (legacy)

**User Story:** As a Tokeira operator, I want the edge layer to support legacy build ID compatibility management (`update_worker_build_id_compatibility`, `get_worker_build_id_compatibility`), so that older SDK versions can manage worker versioning.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives an `UpdateWorkerBuildIdCompatibilityRequest`, THE Edge_Layer SHALL translate the request to the runtime's versioning storage and return an `UpdateWorkerBuildIdCompatibilityResponse`.
2. WHEN the Edge_Layer receives a `GetWorkerBuildIdCompatibilityRequest`, THE Edge_Layer SHALL query the runtime's versioning storage and return a `GetWorkerBuildIdCompatibilityResponse`.
3. IF Tokeira does not support the legacy build ID compatibility API, THEN THE Edge_Layer SHALL return `Status::unimplemented` with a descriptive message and document the handlers as unsupported.

### Requirement 5.3: Worker task reachability handler

**User Story:** As a Tokeira operator, I want the edge layer to support `get_worker_task_reachability`, so that I can determine which task queues a given build ID can reach.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `GetWorkerTaskReachabilityRequest`, THE Edge_Layer SHALL query the runtime's versioning and task queue state and return a `GetWorkerTaskReachabilityResponse` with reachability information.
2. FOR EVERY field in the upstream `GetWorkerTaskReachabilityResponse` proto message, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 5.4: Shutdown worker handler

**User Story:** As a Tokeira operator, I want the edge layer to support `shutdown_worker`, so that I can gracefully drain a worker from a task queue.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `ShutdownWorkerRequest`, THE Edge_Layer SHALL signal the runtime to drain the specified worker and return a `ShutdownWorkerResponse`.
2. IF Tokeira does not support worker shutdown, THEN THE Edge_Layer SHALL return `Status::unimplemented` with a descriptive message and document the handler as unsupported.

### Requirement 5.5: Deployment management handlers

**User Story:** As a Tokeira operator, I want the edge layer to support deployment management (`describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment`), so that I can manage worker deployments through the API.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives any deployment management request (`DescribeDeploymentRequest`, `ListDeploymentsRequest`, `GetDeploymentReachabilityRequest`, `GetCurrentDeploymentRequest`, `SetCurrentDeploymentRequest`), THE Edge_Layer SHALL translate the request to the runtime's deployment management subsystem and return the corresponding response.
2. FOR EVERY field in the upstream deployment proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.
3. IF Tokeira does not yet support a deployment management handler, THEN THE Edge_Layer SHALL return `Status::unimplemented` with a descriptive message referencing the unsupported feature.

---

## Feature 6: Schedule Transport

### Requirement 6.1: Schedule CRUD handlers

**User Story:** As a Tokeira user, I want the edge layer to support schedule creation, description, update, and deletion (`create_schedule`, `describe_schedule`, `update_schedule`, `delete_schedule`), so that I can manage recurring workflow executions through the API.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `CreateScheduleRequest`, THE Edge_Layer SHALL translate the request to the runtime's schedule subsystem and return a `CreateScheduleResponse` with the created schedule's conflict token.
2. WHEN the Edge_Layer receives a `DescribeScheduleRequest`, THE Edge_Layer SHALL query the runtime's schedule storage and return a `DescribeScheduleResponse` with the schedule's configuration, state, and recent actions.
3. WHEN the Edge_Layer receives an `UpdateScheduleRequest`, THE Edge_Layer SHALL translate the update to the runtime's schedule subsystem and return an `UpdateScheduleResponse`.
4. WHEN the Edge_Layer receives a `DeleteScheduleRequest`, THE Edge_Layer SHALL delete the schedule via the runtime and return a `DeleteScheduleResponse`.
5. FOR EVERY field in the upstream schedule CRUD proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 6.2: Schedule operational handlers

**User Story:** As a Tokeira user, I want the edge layer to support schedule patching, listing, and matching time queries (`patch_schedule`, `list_schedules`, `list_schedule_matching_times`), so that I can operate and inspect schedules through the API.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `PatchScheduleRequest`, THE Edge_Layer SHALL apply the patch (trigger immediately, pause, unpause) to the schedule via the runtime and return a `PatchScheduleResponse`.
2. WHEN the Edge_Layer receives a `ListSchedulesRequest`, THE Edge_Layer SHALL query the runtime's schedule storage and return a `ListSchedulesResponse` with schedule entries and pagination.
3. WHEN the Edge_Layer receives a `ListScheduleMatchingTimesRequest`, THE Edge_Layer SHALL compute the matching times for the schedule's spec and return a `ListScheduleMatchingTimesResponse`.
4. FOR EVERY field in the upstream schedule operational proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

---

## Feature 7: Batch Operations Transport

### Requirement 7.1: Start batch operation handler

**User Story:** As a Tokeira operator, I want the edge layer to support `start_batch_operation`, so that I can perform bulk operations (terminate, cancel, signal, reset, delete) on workflow executions matching a visibility query.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `StartBatchOperationRequest`, THE Edge_Layer SHALL translate the request to the runtime's batch operation subsystem and return a `StartBatchOperationResponse` with the operation ID.
2. THE Edge_Layer SHALL support all batch operation types defined in the upstream proto: `BatchOperationTermination`, `BatchOperationCancellation`, `BatchOperationSignal`, `BatchOperationDeletion`, and `BatchOperationReset`.
3. FOR EVERY field in the upstream `StartBatchOperationRequest` proto message, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 7.2: Batch operation lifecycle handlers

**User Story:** As a Tokeira operator, I want the edge layer to support batch operation lifecycle management (`stop_batch_operation`, `describe_batch_operation`, `list_batch_operations`), so that I can monitor and control in-progress batch operations.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `StopBatchOperationRequest`, THE Edge_Layer SHALL signal the runtime to stop the batch operation and return a `StopBatchOperationResponse`.
2. WHEN the Edge_Layer receives a `DescribeBatchOperationRequest`, THE Edge_Layer SHALL query the runtime's batch operation state and return a `DescribeBatchOperationResponse` with operation status, progress counts, and close time.
3. WHEN the Edge_Layer receives a `ListBatchOperationsRequest`, THE Edge_Layer SHALL query the runtime's batch operation storage and return a `ListBatchOperationsResponse` with operation entries and pagination.
4. FOR EVERY field in the upstream batch operation lifecycle proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

---

## Feature 8: Nexus Task Transport

### Requirement 8.1: Poll Nexus task queue handler

**User Story:** As a Nexus worker, I want the edge layer to support `poll_nexus_task_queue`, so that Nexus workers can receive Nexus operation tasks dispatched by the runtime.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `PollNexusTaskQueueRequest`, THE Edge_Layer SHALL translate the request to the runtime's Nexus task broker and return a `PollNexusTaskQueueResponse` with the next available Nexus task.
2. WHEN no Nexus task is available within the poll timeout, THE Edge_Layer SHALL return an empty response.
3. THE `PollNexusTaskQueueResponse` SHALL include the task token, Nexus request (start or cancel), and all fields defined in the upstream proto.
4. FOR EVERY field in the upstream `PollNexusTaskQueueRequest` and `PollNexusTaskQueueResponse` proto messages, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 8.2: Respond Nexus task completed handler

**User Story:** As a Nexus worker, I want the edge layer to support `respond_nexus_task_completed`, so that Nexus workers can report successful operation completion back to the runtime.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `RespondNexusTaskCompletedRequest`, THE Edge_Layer SHALL translate the response to the runtime's Nexus operation resolution path and return a `RespondNexusTaskCompletedResponse`.
2. THE Edge_Layer SHALL support all Nexus response types: synchronous completion, asynchronous operation started, and operation error.
3. FOR EVERY field in the upstream `RespondNexusTaskCompletedRequest` proto message, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

### Requirement 8.3: Respond Nexus task failed handler

**User Story:** As a Nexus worker, I want the edge layer to support `respond_nexus_task_failed`, so that Nexus workers can report operation failures back to the runtime.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `RespondNexusTaskFailedRequest`, THE Edge_Layer SHALL translate the failure to the runtime's Nexus operation resolution path and return a `RespondNexusTaskFailedResponse`.
2. THE failure SHALL carry the structured Nexus `HandlerError` with error type, failure message, and retry behavior.
3. FOR EVERY field in the upstream `RespondNexusTaskFailedRequest` proto message, THE Edge_Layer SHALL faithfully translate the field or document it as unsupported.

---

## Feature 9: Eager Dispatch

### Requirement 9.1: Eager workflow task on StartWorkflowExecution

**User Story:** As an SDK user, I want `StartWorkflowExecutionResponse` to include an `eager_workflow_task` when the starting worker requests it, so that the first workflow task can be executed immediately without a separate poll round-trip.

#### Acceptance Criteria

1. WHEN a `StartWorkflowExecutionRequest` includes `request_eager_execution=true` and the starting worker is a compatible poller for the workflow's task queue, THE Edge_Layer SHALL include the first workflow task inline in the `StartWorkflowExecutionResponse.eager_workflow_task` field.
2. WHEN eager execution is requested but no compatible poller is available (e.g., the caller is a pure client, not a worker), THE Edge_Layer SHALL omit `eager_workflow_task` and let the task flow through normal poll dispatch.
3. THE `eager_workflow_task` SHALL be a fully populated `PollWorkflowTaskQueueResponse` containing the workflow's initial history, task token, and all fields required for the SDK to begin executing the workflow task.
4. WHEN an eager workflow task is returned, THE Edge_Layer SHALL NOT also publish the workflow task to the broker for normal poll dispatch (the task is consumed by the eager return).

### Requirement 9.2: Eager activity tasks on RespondWorkflowTaskCompleted

**User Story:** As an SDK user, I want `RespondWorkflowTaskCompletedResponse` to include `activity_tasks` for activities that can be eagerly dispatched to the completing worker, so that activity execution can begin immediately without a separate poll round-trip.

#### Acceptance Criteria

1. WHEN a `RespondWorkflowTaskCompletedRequest` includes `return_new_workflow_task=true` and the completing worker is also polling for activities on compatible task queues, THE Edge_Layer SHALL include eligible activity tasks in the `RespondWorkflowTaskCompletedResponse.activity_tasks` field.
2. EACH eagerly returned activity task SHALL be a fully populated `PollActivityTaskQueueResponse` containing the activity's input, task token, timeouts, and all fields required for the SDK to begin executing the activity.
3. WHEN activity tasks are eagerly returned, THE Edge_Layer SHALL NOT also publish those tasks to the activity broker for normal poll dispatch.
4. THE Edge_Layer SHALL limit the number of eagerly returned activity tasks to a configurable maximum per response.
5. WHEN no eligible activity tasks exist or the worker is not polling for activities, THE Edge_Layer SHALL return an empty `activity_tasks` list.

### Requirement 9.3: Eager dispatch coordination with broker

**User Story:** As a Tokeira developer, I want eager dispatch to coordinate with the broker, so that tasks are not double-dispatched and broker state remains consistent.

#### Acceptance Criteria

1. WHEN a task is eagerly dispatched, THE Edge_Layer SHALL atomically claim the task from the broker before including it in the response, preventing concurrent poll from also returning the same task.
2. IF the eager dispatch response fails to reach the client (e.g., connection drop), THE runtime SHALL detect the unacknowledged task and re-enqueue it for normal poll dispatch.
3. THE eager dispatch path SHALL use the same task token encoding and validation as normal poll dispatch, so that completions and failures are handled identically regardless of dispatch path.
