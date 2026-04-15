# Proto Field Propagation Audit

Comprehensive audit of the `tokeira-edge` and `tokeira-proto` crates for
unimplemented handlers, silently dropped fields, and incomplete proto
translation.

## 1. Unimplemented gRPC Handlers (44)

All return `Status::unimplemented()`.

| Category | Handlers |
|---|---|
| Namespace (3) | `update_namespace`, `deprecate_namespace`, `execute_multi_operation` |
| Activity by-ID (5) | `record_activity_task_heartbeat_by_id`, `respond_activity_task_completed_by_id`, `respond_activity_task_failed_by_id`, `respond_activity_task_canceled`, `respond_activity_task_canceled_by_id` |
| Legacy listing (4) | `list_open_workflow_executions`, `list_closed_workflow_executions`, `list_archived_workflow_executions`, `scan_workflow_executions` |
| Search/TaskQueue (2) | `get_search_attributes`, `list_task_queue_partitions` |
| Scheduling (7) | `create_schedule`, `describe_schedule`, `update_schedule`, `patch_schedule`, `list_schedule_matching_times`, `delete_schedule`, `list_schedules` |
| Worker versioning (6) | `update_worker_build_id_compatibility`, `get_worker_build_id_compatibility`, `update_worker_versioning_rules`, `get_worker_versioning_rules`, `get_worker_task_reachability`, `shutdown_worker` |
| Deployment (5) | `describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment` |
| Batch (4) | `start_batch_operation`, `stop_batch_operation`, `describe_batch_operation`, `list_batch_operations` |
| Nexus (3) | `poll_nexus_task_queue`, `respond_nexus_task_completed`, `respond_nexus_task_failed` |
| Activity/WF options (5) | `update_activity_options_by_id`, `update_workflow_execution_options`, `pause_activity_by_id`, `unpause_activity_by_id`, `reset_activity_by_id` |

## 2. Response Fields Silently Dropped via `..Default::default()`

### `poll_response_to_proto` (PollWorkflowTaskQueueResponse)

| Proto field | Status |
|---|---|
| `previous_started_event_id` (field 4) | **Not set** — defaults to 0. SDK uses this for sticky replay boundary. |
| `backlog_count_hint` (field 7) | Not set |
| `next_page_token` (field 9) | Not set |
| `query` (field 10) | Not set (legacy query field) |
| `scheduled_time` (field 12) | Not set |
| `started_time` (field 13) | Not set |

### `completed_response_to_proto` (RespondWorkflowTaskCompletedResponse)

| Proto field | Status |
|---|---|
| `activity_tasks` (field 2) | Not set (eager activity return) |
| `reset_history_event_id` (field 3) | Not set |

### `describe_response_to_proto` (DescribeWorkflowExecutionResponse)

| Proto field | Status |
|---|---|
| `pending_activities` | Not set |
| `pending_children` | Not set |
| `pending_workflow_task` | Not set |

### `start_response_to_proto` (StartWorkflowExecutionResponse)

| Proto field | Status |
|---|---|
| `started` (field 3) | Not set — should be `true` when a new workflow was started |
| `eager_workflow_task` (field 2) | Not set |

### `namespace_to_proto` (DescribeNamespaceResponse)

| Proto field | Status |
|---|---|
| `description` | Hardcoded empty |
| `owner_email` | Hardcoded empty |
| `data` | Hardcoded empty |
| `history_archival_state/uri` | Hardcoded 0/empty |
| `visibility_archival_state/uri` | Hardcoded 0/empty |
| `custom_search_attribute_aliases` | Hardcoded empty |
| `clusters` | Hardcoded empty |
| `failover_version` | Hardcoded 0 |
| `failover_history` | Hardcoded empty |

### `cluster_info_to_proto` (GetClusterInfoResponse)

| Proto field | Status |
|---|---|
| `supported_clients` | Hardcoded empty |
| `version_info` | Hardcoded None |
| `history_shard_count` | Hardcoded 0 |

### `describe_task_queue_response_to_proto`

| Proto field | Status |
|---|---|
| `versions_info` | Hardcoded default |
| `worker_version_capabilities` on PollerInfo | Hardcoded None |

## 3. History Serializer — Missing Event Attribute Fields

### Failure objects (all event types with failures)

Every `proto_failure::Failure` construction uses `..Default::default()`,
dropping:
- `cause` (chained failure)
- `stack_trace`
- `source`
- `failure_info` (ApplicationFailureInfo, TimeoutFailureInfo, etc.)
- `encoded_attributes`

Affected events: `WorkflowExecutionFailed`, `WorkflowTaskFailed`,
`ActivityTaskFailed`, `ActivityTaskTimedOut`, `ChildWorkflowExecutionFailed`,
`MarkerRecorded`.

### WorkflowExecutionStarted

Missing:
- `parent_workflow_execution`
- `parent_workflow_type`
- `parent_initiated_event_id`
- `continued_failure`
- `last_completion_result`
- `original_execution_run_id`
- `first_execution_run_id`
- `cron_schedule`

### WorkflowExecutionContinuedAsNew

Missing:
- `workflow_execution_timeout` (present in kernel event but not serialized)
- `retry_policy`
- `initiator`
- `failure`
- `last_completion_result`

### WorkflowTaskScheduled

Missing:
- `task_queue` (not set — SDK may need this)
- `start_to_close_timeout`
- `attempt`

### ActivityTaskScheduled

Missing:
- `schedule_to_close_timeout` (present in kernel but check serialization)
- `schedule_to_start_timeout`
- `start_to_close_timeout`
- `heartbeat_timeout`

### SignalExternalWorkflowExecutionInitiated / RequestCancelExternal

Missing:
- `control` field

## 4. `unwrap_or_default()` — Potential Silent Data Loss

High-risk instances where missing proto fields become empty values
instead of errors:

| Location | Field | Risk |
|---|---|---|
| `start_request_to_edge` | `workflow_type` | Empty string if missing |
| `start_request_to_edge` | `input` | Empty payloads if missing |
| `start_request_to_edge` | `memo` | Empty memo if missing |
| `signal_request_to_edge` | `input` | Empty payloads if missing |
| `respond_completed_request_to_edge` | query result `answer` | Empty payloads if missing |
| `namespace_to_proto` | `namespace_id` | Empty string if missing |
| `signal_with_start_request_to_edge` | `workflow_type` | Empty string if missing |

## 5. Priority Recommendations

### Critical (affects SDK correctness)

1. **`previous_started_event_id`** on `PollWorkflowTaskQueueResponse` — the SDK uses this for sticky replay. Without it, every WFT replays from the beginning. This causes unnecessary work and potential issues with incremental history.

2. **Failure object completeness** — `failure_info` (ApplicationFailureInfo, TimeoutFailureInfo, etc.) is never populated. The SDK uses this to distinguish failure types. Without it, all failures appear as generic.

3. **`started` field** on `StartWorkflowExecutionResponse` — the SDK may use this to distinguish "started new" vs "used existing" for conflict policies.

### Important (affects feature completeness)

4. **`WorkflowTaskScheduled` attributes** — missing `task_queue`, `start_to_close_timeout`, `attempt`. The SDK may use these during replay.

5. **`WorkflowExecutionContinuedAsNew` attributes** — missing `workflow_execution_timeout`, `retry_policy`. The successor run may not inherit these correctly.

6. **`DescribeWorkflowExecution` response** — missing `pending_activities`, `pending_children`. The Temporal UI uses these.

### Low Priority (cosmetic/operational)

7. Namespace configuration fields (archival, replication)
8. Cluster info fields (supported_clients, version_info)
9. Worker versioning capabilities on PollerInfo
