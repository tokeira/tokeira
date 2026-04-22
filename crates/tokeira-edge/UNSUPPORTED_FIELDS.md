# Unsupported Proto Fields

This document lists upstream Temporal API proto fields that Tokeira intentionally
does not support or cannot yet populate, along with rationale.

## StartWorkflowExecutionRequest

| Field | Status | Rationale |
|---|---|---|
| `workflow_id_reuse_policy` | Not supported | Tokeira uses simple ID-based dedup |
| `workflow_id_conflict_policy` | Not supported | Tokeira uses simple ID-based dedup |
| `cron_schedule` | Server-managed | Populated for schedule-triggered runs; client-supplied cron starts are still not accepted |
| `request_eager_execution` | Not supported | Eager dispatch not implemented |
| `continued_failure` | Not supported | Server-internal field for schedules |
| `last_completion_result` | Not supported | Server-internal field for schedules |
| `workflow_start_delay` | Not supported | Start delay not implemented |
| `completion_callbacks` | Not supported | Completion callbacks not implemented at start |
| `user_metadata` | Not supported | SDK user metadata not threaded |
| `links` | Not supported | Link tracking not implemented |
| `versioning_override` | Not supported | Versioning override not implemented |

## Schedule Transport

| Field | Status | Rationale |
|---|---|---|
| `ScheduleSpec.timezone_data` | Not round-tripped | Runtime uses `timezone_name`; embedded TZif definitions are dropped on describe/list |
| Original `ScheduleSpec.calendar` / `cron_string` | Not round-tripped | Inputs are compiled to `structured_calendar` before storage |
| `NewWorkflowExecutionInfo.header` | Not supported | Schedule action headers are not modeled internally |
| `NewWorkflowExecutionInfo.user_metadata` | Not supported | SDK user metadata is not threaded to scheduled starts |
| `NewWorkflowExecutionInfo.versioning_override` | Not supported | Scheduled starts use assignment rule evaluation; pinned overrides are rejected |

## RespondWorkflowTaskCompletedRequest

| Field | Status | Rationale |
|---|---|---|
| `sticky_attributes` | Not supported | Sticky task queues partially implemented |
| `return_new_workflow_task` | Not supported | Inline WFT return not implemented |
| `binary_checksum` | Deprecated | Superseded by worker versioning |
| `worker_version_stamp` | Deprecated | Superseded by deployment-based versioning |
| `sdk_metadata` | Not supported | SDK metadata not threaded |
| `metering_metadata` | Not supported | Metering not implemented |
| `deployment` | Not supported | Deployment-based versioning not implemented |
| `versioning_behavior` | Not supported | Versioning behavior not implemented |

## DescribeWorkflowExecutionResponse

| Field | Status | Rationale |
|---|---|---|
| `execution_config` | Not populated | Requires new storage query for execution config |
| `pending_activities` | Not populated | Requires new storage query for pending activity state |
| `pending_children` | Not populated | Requires new storage query for pending child state |
| `pending_workflow_task` | Not populated | Requires new storage query for pending WFT state |
| `callbacks` | Not populated | Completion callbacks not implemented |
| `pending_nexus_operations` | Not populated | Requires new storage query for pending Nexus state |

## PollActivityTaskQueueResponse

| Field | Status | Rationale |
|---|---|---|
| `heartbeat_details` | Not populated | Heartbeat state not threaded to poll response |
| `scheduled_time` | Not populated | Requires activity scheduled event timestamp |
| `current_attempt_scheduled_time` | Not populated | Requires retry state tracking |
| `started_time` | Not populated | Could be set to poll time |

## SignalWorkflowExecutionRequest

| Field | Status | Rationale |
|---|---|---|
| `header` | Not extracted | Signal headers not threaded |
| `links` | Not extracted | Link tracking not implemented |

## Batch Operations

| Field | Status | Rationale |
|---|---|---|
| `BatchOperationSignal.header` | Dropped at translation | Kernel `SignalRequest` has no header field |
| `BatchOperationUpdateWorkflowExecutionOptions` | Not supported | Workflow execution options update is outside the batch MVP |
| `BatchOperationReset.reset_reapply_type` | Not supported | Reset reapply semantics are not modeled by kernel reset |
| `BatchOperationReset.options.current_run_only` | Not supported | Batch reset resolves only within the exact execution reference being processed |
| `BatchOperationReset.options.reset_reapply_exclude_types` | Not supported | Reset reapply exclusion semantics are not modeled by kernel reset |

## History Event Attributes — Activity Events

Activity completion/failure/timeout/cancel events have `scheduled_event_id` and
`started_event_id` fields in the proto that link back to the scheduling and start
events. The kernel tracks activities by `activity_id` rather than event ID, so
these linkage fields cannot be populated until the kernel maintains an
`activity_id → event_id` mapping.

## History Event Attributes — WorkflowExecutionOptionsUpdated

The proto has a `versioning_override` field. The kernel's `VersioningOverride` is
a placeholder type, so this field cannot be meaningfully populated yet.

## UpdateWorkflowExecutionResponse

| Field | Status | Rationale |
|---|---|---|
| `update_ref` | Not populated | Update reference tracking not implemented |
| `stage` | Not populated | Update lifecycle stage not threaded |
