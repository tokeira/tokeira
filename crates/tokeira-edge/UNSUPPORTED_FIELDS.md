# Unsupported Proto Fields

This document lists upstream Temporal API proto fields that Tokeira intentionally
does not support or cannot yet populate, along with rationale.

## StartWorkflowExecutionRequest

| Field | Status | Rationale |
|---|---|---|
| `cron_schedule` | Server-managed | Populated for schedule-triggered runs; client-supplied cron starts are still not accepted |
| `request_eager_execution` | Supported | Used for eager workflow-task dispatch on start |
| `continued_failure` | Not supported | Server-internal field for schedules |
| `last_completion_result` | Not supported | Server-internal field for schedules |
| `workflow_start_delay` | Supported | Delays first WFT dispatch with a durable internal timer |
| `completion_callbacks` | Partially supported | Registration and describe rendering are implemented; terminal dispatch is pending |
| `user_metadata` | Supported | Threaded into start history |
| `links` | Supported | Threaded into start history |
| `versioning_override` | Supported | Translated into kernel versioning override and routed through WFT dispatch |
| `on_conflict_options` | Supported | Applies request id, callbacks, and links to the running workflow under `USE_EXISTING` |
| `priority` | Supported | Persisted and rendered through describe/history |
| `eager_worker_deployment_options` | Supported | Worker-deployment routing option used only when eager execution is requested |
| `time_skipping_config` | Rejected when behavioral | `enabled`, `disable_propagation`, present `fast_forward_config`, or nonzero `max_session_skip_count` return `INVALID_ARGUMENT`; an empty message remains accepted. `v132-gated-surfaces` owns the v1.32.0 stock rejection |

## Schedule Transport

| Field | Status | Rationale |
|---|---|---|
| `ScheduleSpec.timezone_data` | Not round-tripped | Runtime uses `timezone_name`; embedded TZif definitions are dropped on describe/list |
| Original `ScheduleSpec.calendar` / `cron_string` | Not round-tripped | Inputs are compiled to `structured_calendar` before storage |
| `NewWorkflowExecutionInfo.header` | Not supported | Schedule action headers are not modeled internally |
| `NewWorkflowExecutionInfo.user_metadata` | Not supported | SDK user metadata is not threaded to scheduled starts |
| `NewWorkflowExecutionInfo.versioning_override` | Not supported | Scheduled starts are unversioned (deprecated v0.2 assignment rules are gated off per `.kiro/specs/worker-deployments/reference/v1-v2-conformance-decision.md`); pinned overrides are rejected |

## RespondWorkflowTaskCompletedRequest

| Field | Status | Rationale |
|---|---|---|
| `sticky_attributes` | Supported | Validated and translated into sticky affinity TTL on WFT completion |
| `return_new_workflow_task` | Supported | Returns only a real durably scheduled and started WFT, or query-only WFT when the run is quiescent |
| `binary_checksum` | Deprecated | Accepted for back-compat only; superseded by worker versioning |
| `worker_version_stamp` | Deprecated | Accepted for back-compat and preserved as worker version metadata |
| `sdk_metadata` | Supported | Preserved on `WorkflowTaskCompleted` history |
| `metering_metadata` | Supported | Preserved on `WorkflowTaskCompleted` history |
| `deployment` | Deprecated | Accepted as fallback deployment metadata when current `deployment_options` is absent |
| `deployment_options` | Supported | Current worker deployment/versioning metadata |
| `resource_id` | Supported | Preserved as routing envelope metadata |
| `worker_instance_key` | Supported | Preserved as worker lifecycle envelope metadata |
| `worker_control_task_queue` | Supported | Preserved as worker lifecycle envelope metadata |
| `capabilities` | Supported | `discard_speculative_workflow_task_with_events` is preserved for speculative-WFT handling |
| `versioning_behavior` | Supported | Validated and preserved on WFT completion |

## DescribeWorkflowExecutionResponse

| Field | Status | Rationale |
|---|---|---|
| `execution_config.user_metadata` | Not populated | Start user metadata is not retained yet |
| `workflow_execution_info.versioning_info`, worker deployment/build-id fields | Not populated | Worker deployment/versioning state is not retained yet |
| `workflow_execution_info.priority` | Not populated | Workflow priority state is not modeled yet |
| `workflow_execution_info.external_payload_size_bytes`, `external_payload_count` | Not populated | External payload accounting is not modeled yet |
| `pending_activities.heartbeat_details`, heartbeat/retry timing fields | Not populated | Activity heartbeat and attempt timing state is not retained yet |
| `pending_activities` worker deployment/build-id fields | Not populated | Deprecated worker versioning fields remain default until worker deployment state exists |
| `callbacks` | Empty | Kernel callbacks are placeholders without representable callback URL, trigger, state, or timing data |
| `pending_nexus_operations` attempt/cancellation/block-reason fields | Not populated | Nexus delivery attempt and cancellation tracking is not retained yet |
| `workflow_extended_info.last_reset_time`, `reset_run_id`, `request_id_infos` | Not populated | Reset and request-id linkage state is not retained yet |

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

## History Event Attributes — WorkflowExecutionOptionsUpdated

| Field | Status | Rationale |
|---|---|---|
| `versioning_override` | Supported | Serialized from the kernel `VersioningOverride` (Set → value, Clear → `unset_versioning_override`) by `api-conformance-workflow-options`. |
| `attached_completion_callbacks`, `attached_request_id` | Supported | Projected into the event attributes (empty string when the update attached no request id), mirroring `CreateWorkflowExecutionOptionsUpdatedEvent` (`event_factory.go:413-414 @ v1.31.0`); attached links land on event-level `HistoryEvent.links` (`event_factory.go:419`). |
| `priority`, `time_skipping_config` | Not modeled | tokeira does not model these as mutable execution options; `UpdateWorkflowExecutionOptions` rejects a mask targeting them with `INVALID_ARGUMENT`. |

## UpdateWorkflowExecutionResponse

| Field | Status | Rationale |
|---|---|---|
| `update_ref` | Not populated | Update reference tracking not implemented |
| `stage` | Not populated | Update lifecycle stage not threaded |

## Temporal API v1.63.5 campaign request fields

The v1.32 compatibility campaign leaves these fields unmodeled until their owner
implements the behavior. Field numbers and shapes come from `proto/upstream/` at
`v1.63.5`. Existing request validation and deprecated request fields retain their
pre-resync treatment.

| Field | Current treatment | Owning spec |
|---|---|---|
| `DescribeNamespaceRequest.weak_consistency` | Dropped; no alternate read path | `v132-lifecycle-fidelity` |
| `ListWorkersRequest.include_system_workers` | Dropped; existing listing behavior retained | `v132-batch-operations-and-workers` |
| `DescribeActivityExecutionRequest.include_heartbeat_details` | Dropped; existing describe rendering retained | `v132-standalone-activities` |
| `DescribeActivityExecutionRequest.include_last_failure` | Dropped; existing describe rendering retained | `v132-standalone-activities` |
| `RespondWorkflowTaskCompletedRequest.page_number` | Dropped; no completion pagination | `v132-gated-surfaces` |
| `RespondWorkflowTaskCompletedRequest.intermediate_page` | Dropped; no completion pagination | `v132-gated-surfaces` |
| `StartBatchOperationRequest.target_executions` | Dropped; the deprecated `executions` field remains authoritative | `v132-batch-operations-and-workers` |
| `StartBatchOperationRequest.cancel_activities_operation`, `.terminate_activities_operation`, `.delete_activities_operation` | Same missing-operation error as the old decoder's unknown oneof tags; payloads are not translated | `v132-standalone-activities` |
| `PauseActivityRequest.request_id` | Dropped on the served deprecated RPC | `v132-standalone-activities` |
| `update.v1.Request.request_id` | Dropped; no attached request-id tracking | `v132-lifecycle-fidelity` |
| `update.v1.Request.completion_callbacks` | Dropped; no update callback attachment | `v132-lifecycle-fidelity` |
| `update.v1.Request.links` | Dropped; no update backlink attachment | `v132-lifecycle-fidelity` |
| `StartChildWorkflowExecutionCommandAttributes.versioning_override` | Dropped; child-start translation unchanged | `v132-worker-deployments` |
| `Command.event_group_markers` | Dropped; no event-group model | `v132-gated-surfaces` |
| `activity.v1.ActivityOptions.start_delay` | Dropped; no standalone delayed start | `v132-standalone-activities` |
| `VersioningOverride.one_time` | New oneof tag ignored; deprecated-field fallback retained | `v132-worker-deployments` |
| `common.v1.Link.workflow` | Rejected with the existing missing-variant error; older link variants retain their translation | `v132-lifecycle-fidelity` |
| `CountWorkersRequest` (all fields) | Entire RPC is deferred and returns `UNIMPLEMENTED` | `v132-batch-operations-and-workers` |
| `PauseActivityExecutionRequest`, `UnpauseActivityExecutionRequest`, `ResetActivityExecutionRequest`, `UpdateActivityExecutionOptionsRequest` (all fields) | Entire RPCs are deferred and return `UNIMPLEMENTED` | `v132-standalone-activities` |
| `PollWorkflowExecutionTimeSkippingRequest` (all fields) | Entire RPC is deferred and returns `UNIMPLEMENTED` | `v132-gated-surfaces` |

`WorkerHeartbeat.environment` is retained in the lossless heartbeat image and is not
dropped. `sdk.v1.ExternalStorageReference` has no request carrier in this vendored
surface; its future integration belongs to `v132-gated-surfaces`.
