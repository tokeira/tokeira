# Requirements Document

## Introduction

This spec completes `DescribeWorkflowExecution` conformance for the `api-conformance-tracker` umbrella. The current handler is Partial: it returns basic `workflow_execution_info` fields but does not populate `execution_config`, the full `workflow_execution_info` field set, `pending_activities`, `pending_children`, `pending_workflow_task`, `pending_nexus_operations`, or `workflow_extended_info`. This spec moves the RPC to `Implemented`: every response field is populated from authoritative Tokeira state, or explicitly accounted for with a named owning feature when the underlying capability does not yet exist.

## Glossary

- **Description snapshot:** A read-only runtime/projection view of one workflow run used to build `DescribeWorkflowExecutionResponse`.
- **Pending entity:** A workflow task, activity, child workflow, callback, or Nexus operation that is open in kernel state.
- **Execution config:** Task queue, timeouts, and user metadata captured at start, surfaced through `WorkflowExecutionConfig`.
- **Extended info:** The `WorkflowExecutionExtendedInfo` message (response field 8) carrying expiration, cancel-requested, reset, request-id, and pause metadata.
- **Owning feature:** Another named spec that owns the durable state a field depends on. A field whose backing capability is owned elsewhere is populated as soon as that state exists, and is truthfully empty until then - never fabricated.

## Target State

`Implemented`. Every field of `DescribeWorkflowExecutionResponse` is accounted for. Fields backed by durable Tokeira state are populated. Fields whose backing capability is owned by another named feature (worker versioning/deployment, completion callbacks, Nexus task execution, priority) are populated from that state once it exists and are truthfully empty until the owning feature lands; this spec does not fabricate them and does not return `UNIMPLEMENTED` for the RPC.

## Evidence From Current Code

- Proto messages inspected: `DescribeWorkflowExecutionRequest`, `DescribeWorkflowExecutionResponse` in `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` (8 response fields); nested `WorkflowExecutionConfig`, `WorkflowExecutionInfo`, `PendingActivityInfo`, `PendingChildExecutionInfo`, `PendingWorkflowTaskInfo`, `CallbackInfo`, `PendingNexusOperationInfo`, `WorkflowExecutionExtendedInfo`, `WorkflowExecutionPauseInfo`, `RequestIdInfo` in `proto/upstream/temporal/api/workflow/v1/message.proto`.
- Current handler: `WorkflowServiceGrpc::describe_workflow_execution` in `crates/tokeira-edge/src/grpc/workflow_service.rs`; inner `WorkflowService::describe_workflow_execution` in `crates/tokeira-edge/src/workflow_service.rs`.
- Current translation: `describe_response_to_proto`, `workflow_execution_info_from_description`, `pending_activity_to_proto`, `pending_child_to_proto`, `pending_wft_to_proto` in `crates/tokeira-edge/src/grpc/translate.rs`. `workflow_extended_info.pause_info` is already populated (delivered by `kernel-pause-workflow`).
- Existing DTOs: `WorkflowExecutionDescription`, `PendingActivityDescription`, `PendingChildDescription`, `PendingWorkflowTaskDescription`, `PauseInfoDescription` in `crates/tokeira-edge/src/translate/mod.rs`.
- Construction sites of `WorkflowExecutionDescription`: `apps/tokeirad/src/lib.rs` (`describe_execution`) and `crates/tokeira-edge/tests/grpc_new_endpoints.rs`.
- Runtime/storage sources: `RunRepository::load_run`, kernel `WorkflowState` (`activities`, `children`, `pending_nexus_operations`, `pending_workflow_task`, `pause_info`, parent fields, timeout fields, `first_execution_run_id`, `started_at`, `closed_at`).
- Kernel cancel state: the kernel emits `HistoryEventKind::WorkflowExecutionCancelRequested` but `WorkflowState` does not currently retain a `cancel_requested` flag; this spec adds a serializable flag (no I/O) to back `extended_info.cancel_requested`.
- Kernel root state: `WorkflowState` has direct parent fields but does not currently retain the root workflow execution for multi-generation child chains; this spec adds serializable root fields and authors them into `WorkflowExecutionStarted` when present. Replay restores them from the event when present and defaults to self when absent, so histories predating the root fields replay correctly.
- Unsupported-field entry: `DescribeWorkflowExecutionResponse` in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.

## Response Field Policy

`DescribeWorkflowExecutionResponse` has eight fields. Each is accounted for below.

### Field 1 — `execution_config` (`WorkflowExecutionConfig`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `task_queue` | Not populated | Populate | Run state `task_queue` |
| `workflow_execution_timeout` | Not populated | Populate when set | Run state `workflow_execution_timeout` |
| `workflow_run_timeout` | Not populated | Populate when set | Run state `workflow_run_timeout` |
| `default_workflow_task_timeout` | Not populated | Populate | Run state `workflow_task_timeout` |
| `user_metadata` | Not populated | Empty until start captures it | Owned by `api-conformance-start-fields`; populate once start records user metadata |

### Field 2 — `workflow_execution_info` (`WorkflowExecutionInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `execution`, `type`, `task_queue`, `status`, `start_time`, `close_time`, `history_length`, `state_transition_count`, `memo`, `search_attributes` | Populated | Preserve | Run/projection state |
| `execution_time` | Default | Populate from first run start / execution time | Run state `first_run_started_at` / `started_at` |
| `execution_duration` | Default | Populate when closed (`close_time - execution_time`) | Run state |
| `parent_namespace_id`, `parent_execution` | Default | Populate when the run has a parent | Run state parent fields |
| `root_execution` | Default | Populate from the started event's root execution when present; otherwise the run's own execution (self) | New run state root fields and start-event envelope |
| `first_run_id` | Default | Populate | Run state `first_execution_run_id` |
| `history_size_bytes` | Default | Populate when tracked; else default | Run state if available |
| `auto_reset_points` | Default | Empty until reset points are modeled | Owned by reset-point tracking; truthfully empty |
| `versioning_info`, `worker_deployment_name`, `assigned_build_id`, `inherited_build_id`, `most_recent_worker_version_stamp` | Default | Empty until versioning state exists | Owned by `worker-deployments`; deprecated build-id fields stay default |
| `priority` | Default | Empty until priority is modeled | Owned by priority feature |
| `external_payload_size_bytes`, `external_payload_count` | Default | Empty until external payloads are modeled | Owned by external-payload feature |

### Field 3 — `pending_activities` (`PendingActivityInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `activity_id`, `activity_type`, `state`, `attempt`, `maximum_attempts`, `scheduled_time`, `last_started_time` | Populated | Preserve | Kernel `ActivityState` |
| `heartbeat_details`, `last_heartbeat_time` | Default | Populate when heartbeat tracking exists | Owned by `api-conformance-activity-events` |
| `last_failure` | Default | Populate from `ActivityState.last_failure` when present | Kernel `ActivityState` |
| `expiration_time`, `current_retry_interval`, `last_attempt_complete_time`, `next_attempt_schedule_time` | Default | Populate from retry/timeout state when derivable; else default | Kernel `ActivityState` + `api-conformance-activity-events` |
| `paused`, `pause_info` | Default | Populate from `ActivityState.pause_info` | Kernel `ActivityState` (activity pause exists) |
| `last_worker_identity` | Default | Populate when tracked; else default | Runtime activity tracking |
| `activity_options` | Default | Populate from current activity options when modeled | Owned by activity-options work |
| Deprecated build-id / version-stamp / deployment fields | Default | Leave default | Deprecated; owned by `worker-deployments` |

### Field 4 — `pending_children` (`PendingChildExecutionInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `workflow_id`, `run_id`, `workflow_type_name`, `initiated_id`, `parent_close_policy` | Populated when child state passed through | Populate from kernel child state; `run_id` omitted until child started | Kernel `ChildWorkflowState` |

### Field 5 — `pending_workflow_task` (`PendingWorkflowTaskInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `state`, `scheduled_time`, `started_time`, `attempt` | Populated | Preserve | Kernel `PendingWorkflowTask` |
| `original_scheduled_time` | Default | Populate when heartbeat-WFT original schedule is tracked; else default | Kernel `PendingWorkflowTask` |

### Field 6 — `callbacks` (`CallbackInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| entire list | Placeholder only | Empty list; `CompletionCallback` currently carries no representable callback URL, trigger, or state, so placeholder kernel callbacks are not surfaced as fabricated `CallbackInfo` entries | Owned by `api-conformance-start-fields`, which gives `CompletionCallback` representable fields and renders `CallbackInfo` from them; this spec emits an empty list until that work lands |

### Field 7 — `pending_nexus_operations` (`PendingNexusOperationInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `endpoint`, `service`, `operation`, `scheduled_time`, `scheduled_event_id`, `schedule_to_close_timeout`, `state` | Not populated | Populate from kernel pending Nexus state | Kernel `PendingNexusOperation` |
| `operation_token`, `operation_id` (deprecated) | Default | Populate token when async-started; mirror into deprecated `operation_id` | Kernel `PendingNexusOperation` |
| `attempt`, `last_attempt_complete_time`, `last_attempt_failure`, `next_attempt_schedule_time`, `cancellation_info`, `blocked_reason`, `schedule_to_start_timeout`, `start_to_close_timeout` | Default | Populate when delivery/attempt tracking exists; else default | Owned by Nexus task transport; truthfully empty until tracked |

### Field 8 — `workflow_extended_info` (`WorkflowExecutionExtendedInfo`)

| Sub-field | Current | Target policy | Source |
|---|---|---|---|
| `pause_info` | Populated | Preserve | Kernel `WorkflowState.pause_info` (done by `kernel-pause-workflow`) |
| `execution_expiration_time` | Default | Populate when execution timeout set (`first_run_started_at + workflow_execution_timeout`) | Run state |
| `run_expiration_time` | Default | Populate when run timeout set (`started_at + workflow_run_timeout`) | Run state |
| `cancel_requested` | Default | Populate from new kernel `cancel_requested` flag | Kernel `WorkflowState` (new serializable field) |
| `original_start_time` | Default | Always populate from first run start, falling back to run start | Run state `first_run_started_at` / `started_at` |
| `last_reset_time` | Default | Empty until reset time is retained | Owned by reset tracking; truthfully empty |
| `reset_run_id` | Default | Empty until reset successor linkage is retained | Owned by reset tracking; truthfully empty |
| `request_id_infos` | Default | Empty until request-id history linkage is retained | Owned by request-id tracking; truthfully empty |

## Requirements

### Requirement 1: Populate Execution Config

**User Story:** As an operator, I want `DescribeWorkflowExecution` to return the execution's configuration, so that SDK tooling and dashboards can show task queue and timeout settings.

#### Acceptance Criteria

1. WHEN a workflow execution exists, THE Edge SHALL populate `execution_config.task_queue` from the run's task queue.
2. WHEN a workflow execution has a workflow execution timeout, THE Edge SHALL populate `execution_config.workflow_execution_timeout`; IF unset, THE field SHALL be left default.
3. WHEN a workflow execution has a workflow run timeout, THE Edge SHALL populate `execution_config.workflow_run_timeout`; IF unset, THE field SHALL be left default.
4. WHEN a workflow execution exists, THE Edge SHALL populate `execution_config.default_workflow_task_timeout` from the run's workflow task timeout.
5. WHERE start does not yet capture user metadata, THE Edge SHALL leave `execution_config.user_metadata` default, and tasks SHALL note `api-conformance-start-fields` as the owning feature.

### Requirement 2: Complete Workflow Execution Info

**User Story:** As an SDK client, I want `workflow_execution_info` to carry the stable execution fields, so that history correlation and parent/root linkage work.

#### Acceptance Criteria

1. WHEN a workflow execution exists, THE Edge SHALL preserve the currently populated `workflow_execution_info` fields (execution, type, task queue, status, start/close time, history length, state transition count, memo, search attributes).
2. WHEN a workflow execution exists, THE Edge SHALL populate `execution_time` from the run's first-run start time when available, else from the run start time.
3. WHEN a workflow execution is closed, THE Edge SHALL populate `execution_duration` as `close_time - execution_time`.
4. WHEN a workflow execution has a parent, THE Edge SHALL populate `parent_namespace_id` and `parent_execution`; IF there is no parent, THE fields SHALL be left default.
5. WHEN a workflow execution exists, THE Edge SHALL populate `root_execution` from the run's stored root workflow/run fields using Temporal v1.31.0 semantics: `WorkflowExecutionStarted.root_execution` when present, else the run's own execution (self). A run with no parent is its own root, and an older started event without root fields replays to self even when parent fields are present.
6. WHEN a workflow execution exists, THE Edge SHALL populate `first_run_id` from the first execution run id when available.
7. WHERE versioning, worker-deployment, priority, auto-reset-point, or external-payload state is owned by another feature and does not yet exist, THE Edge SHALL leave the corresponding `workflow_execution_info` fields default, and tasks SHALL name the owning feature.

### Requirement 3: Populate Pending Activities

**User Story:** As an operator, I want `pending_activities` to reflect open activities, so that I can diagnose stuck or retrying activities.

#### Acceptance Criteria

1. WHEN open activities exist, THE Edge SHALL emit exactly one `PendingActivityInfo` per open activity.
2. WHEN an activity is open, THE Edge SHALL populate `activity_id`, `activity_type`, `state`, `attempt`, `maximum_attempts`, `scheduled_time`, and `last_started_time` when started.
3. WHEN an activity has a recorded last failure, THE Edge SHALL populate `last_failure`.
4. WHEN an activity is individually paused, THE Edge SHALL set `paused` true and populate `pause_info` from the activity's pause record.
5. WHERE heartbeat details, heartbeat time, retry interval, or attempt timing are owned by `api-conformance-activity-events` and not yet tracked, THE Edge SHALL leave those fields default.
6. THE Edge SHALL leave deprecated build-id, version-stamp, and deployment fields default.

### Requirement 4: Populate Pending Children

**User Story:** As an operator, I want `pending_children` to reflect open child workflows, so that I can trace parent/child relationships.

#### Acceptance Criteria

1. WHEN open child workflows exist, THE Edge SHALL emit one `PendingChildExecutionInfo` per open child with `workflow_id`, `workflow_type_name`, `initiated_id`, and `parent_close_policy`.
2. WHEN a child workflow has started, THE Edge SHALL populate `run_id`; IF the child has not started, THE `run_id` SHALL be left default rather than emitting a placeholder.

### Requirement 5: Populate Pending Workflow Task

**User Story:** As an operator, I want `pending_workflow_task` to reflect the scheduled or started task, so that I can see whether a worker is processing the workflow.

#### Acceptance Criteria

1. WHEN a workflow task is scheduled or started, THE Edge SHALL populate `pending_workflow_task` with `state`, `scheduled_time`, `attempt`, and `started_time` when started.
2. WHERE the original scheduled time for a heartbeat workflow task is not tracked, THE Edge SHALL leave `original_scheduled_time` default rather than inventing a value.

### Requirement 6: Populate Pending Nexus Operations

**User Story:** As an operator, I want `pending_nexus_operations` to reflect open Nexus operations, so that I can diagnose outbound Nexus calls.

#### Acceptance Criteria

1. WHEN pending Nexus operations exist, THE Edge SHALL emit one `PendingNexusOperationInfo` per operation with `endpoint`, `service`, `operation`, `scheduled_time`, `scheduled_event_id`, `schedule_to_close_timeout`, and `state`.
2. WHEN a Nexus operation has transitioned to async-started, THE Edge SHALL populate `operation_token` and mirror it into the deprecated `operation_id`.
3. WHERE Nexus delivery attempt, cancellation, or block-reason tracking is owned by Nexus task transport and not yet retained, THE Edge SHALL leave those fields default.

### Requirement 7: Populate Workflow Extended Info

**User Story:** As an SDK client, I want `workflow_extended_info` populated, so that expiration, cancel-requested, and pause metadata are visible.

#### Acceptance Criteria

1. WHEN a workflow execution is paused, THE Edge SHALL populate `workflow_extended_info.pause_info` with identity, paused time, and reason (preserving existing behavior).
2. WHEN a workflow execution has an execution timeout, THE Edge SHALL populate `execution_expiration_time` as first-run start plus the execution timeout.
3. WHEN a workflow execution has a run timeout, THE Edge SHALL populate `run_expiration_time` as run start plus the run timeout.
4. WHEN a cancellation has been requested for the run, THE Edge SHALL set `workflow_extended_info.cancel_requested` true.
5. WHEN a workflow execution exists, THE Edge SHALL populate `original_start_time` from the first run start time, falling back to the run start time; this field is always populated for a real run.
6. WHERE reset time, reset run id, or request-id history linkage is not retained, THE Edge SHALL leave `last_reset_time`, `reset_run_id`, and `request_id_infos` default, and tasks SHALL name the owning feature.
7. WHEN a workflow execution exists, THE Edge SHALL emit `workflow_extended_info`; a plain running workflow with no timeout, pause, cancel, or reset still carries `original_start_time` and `cancel_requested = false`.

### Requirement 8: Snapshot Consistency

**User Story:** As an SDK client, I want describe data to come from one consistent run snapshot, so that pending-state fields do not contradict each other.

#### Acceptance Criteria

1. WHEN the Edge builds a describe response, THE pending entities and extended info SHALL be derived from the same loaded run state or projection version.
2. WHEN a pending entity is absent from the authoritative state, THE corresponding response list SHALL omit it rather than emitting default placeholder entries.
3. WHEN an event id or timestamp is unknown because state lacks the field, THE response SHALL leave that proto field default and SHALL NOT invent `0` as an authored event id.
4. THE serializer SHALL include regression coverage that a scheduled activity and a pending workflow task appear together after a workflow schedules an activity.

### Requirement 9: Request Validation and Errors

**User Story:** As an SDK client, I want describe to validate input and report missing executions clearly, so that errors are distinguishable.

#### Acceptance Criteria

1. IF `execution.run_id` is non-empty and malformed, THE Edge SHALL return gRPC `INVALID_ARGUMENT`.
2. WHEN `execution.run_id` is non-empty and valid, THE Edge SHALL describe that exact run.
3. WHEN `execution.run_id` is empty, THE Edge SHALL describe the current run.
4. IF the workflow execution cannot be resolved, THE Edge SHALL return gRPC `NOT_FOUND`.
5. THE Edge SHALL NOT use `EdgeError::Internal` for expected describe validation or lookup failures.

### Requirement 10: Metrics

**User Story:** As an operator, I want describe failures to be observable with the right gRPC labels, so that dashboards distinguish invalid input from missing executions.

#### Acceptance Criteria

1. WHEN `DescribeWorkflowExecution` fails for malformed input, THE edge gRPC metrics SHALL record `invalid_argument`.
2. WHEN `DescribeWorkflowExecution` fails because the execution is missing, THE edge gRPC metrics SHALL record `not_found`.
3. WHEN describe succeeds, THE edge gRPC metrics SHALL record success using the existing method and namespace labels.
