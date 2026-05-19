# Bugfix Requirements Document

## Introduction

The edge history serializer (`crates/tokeira-edge/src/translate/history_serializer.rs`) translates kernel `HistoryEventKind` variants into Temporal proto `HistoryEvent` messages for SDK consumption. Multiple event attribute structs use `..Default::default()` to fill proto fields that the kernel either already carries but doesn't wire through, or doesn't yet carry at all. This produces proto events with zero/empty values where SDKs expect populated data, causing:

- SDK state machine errors during replay (SDKs branch on specific attribute fields)
- Missing metadata in workflow history views (Temporal UI, `tctl`, CLI tools)
- Incorrect decision-making when SDKs use event attributes for routing or logic

The bug condition is conditional, not "all default values are wrong": for any `HistoryEventKind` variant where the serializer has authoritative non-empty event data for a proto field and still emits the field's default/zero value, the serialization is incomplete. Optional Temporal fields SHALL remain default/empty when the kernel/runtime did not author that value for the specific event path. The implementation MUST NOT invent placeholder IDs, identities, namespaces, retry states, worker versions, request IDs, or operation tokens only to avoid default values.

Each field in this bugfix falls into one of four implementation classes:

- **Serializer-only**: the current `HistoryEventKind` already carries the value and `history_serializer.rs` can wire it directly.
- **Kernel event enrichment**: the value must be added to the command/event model before serialization can be fixed.
- **Runtime/history-context enrichment**: the value is derived outside the single event, so the serializer API or history assembly layer must provide context explicitly.
- **Deferred proto-sync**: the field depends on v1.62-specific proto surface and must remain documented but unimplemented until `temporal-api-v1.62-sync`.

Blocked on: `temporal-api-v1.62-sync` for v1.62-specific proto fields. The audit and classification can proceed against the current proto surface, with v1.62-specific attributes marked as deferred.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a `WorkflowExecutionStarted` event is serialized THEN the system produces a proto event with `identity` set to `request_id` instead of the originating client identity, and `header`, `workflow_execution_expiration_time`, `first_workflow_task_backoff`, `initiator`, `source_version_stamp`, and `completion_callbacks` fields defaulted to zero/empty values

1.2 WHEN a `WorkflowExecutionCompleted` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` and `new_execution_run_id` defaulted to zero/empty

1.3 WHEN a `WorkflowExecutionFailed` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` and `new_execution_run_id` defaulted to zero/empty

1.4 WHEN a `WorkflowExecutionTimedOut` event is serialized THEN the system produces a proto event with `new_execution_run_id` defaulted to empty

1.5 WHEN a `WorkflowExecutionCancelRequested` event is serialized THEN the system produces a proto event with `external_initiated_event_id` and `identity` defaulted to zero/empty

1.6 WHEN a `WorkflowExecutionCanceled` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` and `details` defaulted to zero/empty

1.7 WHEN a `WorkflowExecutionContinuedAsNew` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id`, `workflow_execution_timeout`, `retry_policy`, `header`, and `inherit_build_id` fields defaulted to zero/empty

1.8 WHEN a `WorkflowExecutionSignaled` event is serialized THEN the system produces a proto event with `header`, `external_workflow_execution`, and `external_initiated_event_id` fields defaulted to zero/empty

1.9 WHEN a `WorkflowTaskStarted` event is serialized THEN the system produces a proto event with `request_id`, `suggest_continue_as_new`, and `history_size_bytes` fields defaulted to zero/empty

1.10 WHEN a `WorkflowTaskCompleted` event is serialized THEN the system produces a proto event with `binary_checksum`, `sdk_metadata`, `metering_metadata`, and `worker_version` fields defaulted to zero/empty

1.11 WHEN a `WorkflowTaskFailed` event is serialized THEN the system produces a proto event with `binary_checksum` and `worker_version` fields defaulted to zero/empty

1.12 WHEN an `ActivityTaskScheduled` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` defaulted to zero. `namespace` is also empty today, but that is not a bug for same-namespace activities because the namespace is implicit from the workflow execution context.

1.13 WHEN an `ActivityTaskStarted` event is serialized THEN the system produces a proto event with `request_id`, `last_failure`, and `worker_version` fields defaulted to zero/empty

1.14 WHEN an `ActivityTaskCompleted` event is serialized THEN the system produces a proto event with `identity` and `worker_version` fields defaulted to zero/empty

1.15 WHEN an `ActivityTaskFailed` event is serialized THEN the system produces a proto event with `identity`, `retry_state`, and `worker_version` fields defaulted to zero/empty

1.16 WHEN an `ActivityTaskTimedOut` event is serialized THEN the system produces a proto event with `retry_state` defaulted to zero

1.17 WHEN an `ActivityTaskCanceled` event is serialized THEN the system produces a proto event with `identity` and `workflow_task_completed_event_id` fields defaulted to zero/empty

1.18 WHEN an `ActivityTaskCancelRequested` event is serialized THEN the system produces a proto event with `scheduled_event_id` and `workflow_task_completed_event_id` defaulted to zero (the kernel carries `activity_id` but the proto expects `scheduled_event_id`)

1.19 WHEN a `TimerStarted` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` defaulted to zero

1.20 WHEN a `TimerCanceled` event is serialized THEN the system produces a proto event with `started_event_id` and `workflow_task_completed_event_id` defaulted to zero

1.21 WHEN a `MarkerRecorded` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` defaulted to zero

1.22 WHEN a `StartChildWorkflowExecutionInitiated` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id`, `namespace` (human-readable), `header`, `memo`, `search_attributes`, `workflow_execution_timeout`, `workflow_run_timeout`, `workflow_task_timeout`, `retry_policy`, and `cron_schedule` fields defaulted to zero/empty

1.23 WHEN a `ChildWorkflowExecutionStarted` event is serialized THEN the system produces a proto event with `header` defaulted to empty. This is intentionally defaulted in this spec because the parent-side child-start confirmation path has no authoritative child-started header source.

1.24 WHEN a `StartChildWorkflowExecutionFailed` event is serialized THEN the system produces a proto event with `initiated_event_id`, `namespace`, and `workflow_type` fields defaulted to zero/empty

1.25 WHEN a `ChildWorkflowExecutionCompleted` event is serialized THEN the system produces a proto event with `namespace` and child `run_id` defaulted to empty

1.26 WHEN a `ChildWorkflowExecutionFailed` event is serialized THEN the system produces a proto event with `namespace`, `retry_state`, and child `run_id` defaulted to empty/zero

1.27 WHEN a `ChildWorkflowExecutionCanceled` event is serialized THEN the system produces a proto event with `namespace`, `details`, `workflow_type`, and child `run_id` defaulted to empty

1.28 WHEN a `ChildWorkflowExecutionTerminated` event is serialized THEN the system produces a proto event with `namespace` and `workflow_type` defaulted to empty

1.29 WHEN a `ChildWorkflowExecutionTimedOut` event is serialized THEN the system produces a proto event with `namespace`, `workflow_type`, and `retry_state` defaulted to empty/zero

1.30 WHEN a `SignalExternalWorkflowExecutionInitiated` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id`, `namespace`, and `header` fields defaulted to zero/empty

1.31 WHEN an `ExternalWorkflowExecutionSignaled` event is serialized THEN the system produces a proto event with `namespace` and target `run_id` defaulted to empty

1.32 WHEN a `SignalExternalWorkflowExecutionFailed` event is serialized THEN the system produces a proto event with `namespace` and target `run_id` defaulted to empty

1.33 WHEN a `RequestCancelExternalWorkflowExecutionInitiated` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id` and `namespace` defaulted to zero/empty

1.34 WHEN an `ExternalWorkflowExecutionCancelRequested` event is serialized THEN the system produces a proto event with `namespace` and target `run_id` defaulted to empty

1.35 WHEN a `RequestCancelExternalWorkflowExecutionFailed` event is serialized THEN the system produces a proto event with `namespace` and target `run_id` defaulted to empty

1.36 WHEN a `NexusOperationScheduled` event is serialized THEN the system produces a proto event with `workflow_task_completed_event_id`, `nexus_header`, and `endpoint_id` fields defaulted to zero/empty

1.37 WHEN a `NexusOperationStarted` event is serialized THEN the current checked-in proto surface still uses `operation_id`; the v1.62 `operation_token` rename is tracked as deferred until `tokeira_proto` is regenerated

1.38 WHEN a `NexusOperationCompleted`, `NexusOperationFailed`, `NexusOperationCanceled`, or `NexusOperationTimedOut` event is serialized THEN the system produces a proto event with `operation_id` defaulted to empty (the kernel carries it but the serializer ignores it via `operation_id: _`)

1.39 WHEN a `WorkflowExecutionUpdateAccepted` event is serialized THEN the system produces a proto event with `accepted_request_sequencing_event_id` defaulted to zero

1.40 WHEN a `WorkflowExecutionUpdateCompleted` event is serialized THEN the system produces a proto event with `accepted_event_id` defaulted to zero (the kernel doesn't carry this on the Completed variant)

1.41 WHEN a `WorkflowExecutionUpdateRejected` event is serialized THEN the system produces a proto event with `rejected_request_message_id`, `rejected_request`, and `rejected_request_sequencing_event_id` fields defaulted to zero/empty

1.42 WHEN a `WorkflowExecutionOptionsUpdated` event is serialized THEN the system produces a proto event with all fields defaulted to empty because the kernel's `VersioningOverride` and `CompletionCallback` types have no proto mapping yet

1.43 WHEN a Tokeira-specific `WorkflowExecutionPaused` or `WorkflowExecutionUnpaused` event is serialized THEN the system currently maps it to `WorkflowExecutionCanceledEventAttributes` as a placeholder, which can mislead SDK-visible history consumers

### Expected Behavior (Correct)

2.1 WHEN a `WorkflowExecutionStarted` event is serialized THEN the system SHALL populate `identity` from the originating client identity and `header` from the start header only after adding those values to the kernel event; the serializer MUST NOT use `request_id` as `identity`. The system SHALL wire through `first_execution_run_id` and `original_execution_run_id` from kernel data already available. `initiator` requires either a kernel event field or a documented mapping from existing start context; until then it is kernel enrichment, not serializer-only.

2.2 WHEN a `WorkflowExecutionCompleted` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` from the WFT that produced the completion command (requires kernel addition of the producing WFT event ID)

2.3 WHEN a `WorkflowExecutionFailed` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` from the WFT that produced the failure command (requires kernel addition)

2.4 WHEN a `WorkflowExecutionTimedOut` event is serialized THEN the system SHALL populate `new_execution_run_id` only when the timeout transition actually creates a retry successor run. This requires adding `new_execution_run_id: Option<RunId>` to `HistoryEventKind::WorkflowExecutionTimedOut` and populating it from the runtime/kernel timeout retry path. If no retry successor exists, the proto field SHALL remain empty.

2.5 WHEN a `WorkflowExecutionCancelRequested` event is serialized THEN the system SHALL populate `external_initiated_event_id` and `identity` from the cancel request metadata (requires kernel addition)

2.6 WHEN a `WorkflowExecutionCanceled` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` and `details` from the cancel command (requires kernel additions)

2.7 WHEN a `WorkflowExecutionContinuedAsNew` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id`, `workflow_execution_timeout`, and `retry_policy` from kernel data already available but not wired through. `header` and `inherit_build_id` SHALL remain default unless the continued-as-new command/event model is enriched with authoritative values for those fields; the serializer MUST NOT synthesize them.

2.8 WHEN a `WorkflowExecutionSignaled` event is serialized THEN the system SHALL populate `header` when the signal carries headers (requires kernel addition for signal headers)

2.9 WHEN a `WorkflowTaskStarted` event is serialized THEN the system SHALL populate `request_id` from the task start command, `history_size_bytes` from the run's history size, and `suggest_continue_as_new` from the runtime's history-size policy. The runtime SHALL generate the request ID when it submits `StartWorkflowTaskRequest`; the kernel stamps the request fields onto the event.

2.10 WHEN a `WorkflowTaskCompleted` event is serialized THEN the system SHALL populate `sdk_metadata` and `worker_version` from the completion response after the gRPC request translation, internal DTO, `WorkflowTaskCompletedRequest`, and kernel event all preserve those values.

2.11 WHEN a `WorkflowTaskFailed` event is serialized THEN the system SHALL leave `worker_version` default in this spec. The current `respond_workflow_task_failed` handler does not submit a kernel command with worker version metadata; WFT failure is processed server-side by runtime paths such as SDK failure handling or timeout scanning. Populate this field when the `worker-deployments` spec adds worker version reporting to WFT failure responses.

2.12 WHEN an `ActivityTaskScheduled` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` from the WFT that produced the schedule command. `ActivityTaskScheduled.namespace` SHALL remain empty for same-namespace activities because the namespace is implicit from the workflow execution context; it SHALL be populated only when a future cross-namespace activity feature adds an authoritative target namespace name to the command/event model.

2.13 WHEN an `ActivityTaskStarted` event is serialized THEN the system SHALL populate `request_id` from the activity start metadata and `last_failure` from the previous attempt's failure when retrying (requires kernel addition)

2.14 WHEN an `ActivityTaskCompleted` event is serialized THEN the system SHALL populate `identity` from the worker that completed the activity (requires kernel addition)

2.15 WHEN an `ActivityTaskFailed` event is serialized THEN the system SHALL populate `identity` from the worker and `retry_state` from the retry resolution (requires kernel addition)

2.16 WHEN an `ActivityTaskTimedOut` event is serialized THEN the system SHALL populate `retry_state` from the retry resolution (requires kernel addition)

2.17 WHEN an `ActivityTaskCanceled` event is serialized THEN the system SHALL populate `identity` from the worker and `workflow_task_completed_event_id` from the cancel command's WFT only after those values are carried on the event or provided by explicit runtime/history context. The current event carries `details`, `scheduled_event_id`, and `started_event_id` only, so these fields are enrichment work, not serializer-only.

2.18 WHEN an `ActivityTaskCancelRequested` event is serialized THEN the system SHALL populate `scheduled_event_id` and `workflow_task_completed_event_id` only after resolving `activity_id` and the producing WFT through kernel event enrichment or explicit runtime/history context. The serializer MUST NOT perform implicit storage lookup.

2.19 WHEN a `TimerStarted` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` from the WFT that produced the timer command only after the timer-start event is enriched with that event ID or the history assembly layer provides it explicitly.

2.20 WHEN a `TimerCanceled` event is serialized THEN the system SHALL populate `started_event_id` by resolving the timer_id to its started event ID and `workflow_task_completed_event_id` from the cancel command's WFT only after those values are carried on the event or provided by explicit runtime/history context.

2.21 WHEN a `MarkerRecorded` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id` from the WFT that produced the marker command only after the marker event is enriched with that event ID or the history assembly layer provides it explicitly.

2.22 WHEN a `StartChildWorkflowExecutionInitiated` event is serialized THEN the system SHALL populate `workflow_task_completed_event_id`, human-readable `namespace`, header, memo, search attributes, workflow execution/run/task timeout fields, retry policy, and cron schedule only after adding those values to the child-start command/event model. The current event already carries only child workflow ID, workflow type, task queue, input, namespace ID, and parent close policy; all other fields are kernel event enrichment, not serializer-only.

2.23 WHEN a `ChildWorkflowExecutionStarted` event is serialized THEN the system SHALL leave `header` default in this spec. The parent-side child-start confirmation path currently receives only child run identity and does not receive the child's authored start header; populating this field requires a future `runtime-child-workflows` enhancement that echoes authored child-start metadata back to the parent.

2.24 WHEN child workflow terminal events are serialized THEN the system SHALL populate `namespace`, `workflow_type`, and child `run_id` only after those values are made available to serialization. The preferred fix is kernel event enrichment: include namespace, workflow type, and child run ID on `ChildWorkflowExecutionCompleted`, `ChildWorkflowExecutionFailed`, `ChildWorkflowExecutionCanceled`, `ChildWorkflowExecutionTerminated`, and `ChildWorkflowExecutionTimedOut` when known. An alternative implementation may pass explicit history context into the serializer, but the serializer MUST NOT attempt an implicit storage lookup or synthesize missing child metadata.

2.25 WHEN external signal/cancel events are serialized THEN the system SHALL populate `namespace` and target `run_id` only after those values are preserved on the initiated and result events, or after an explicit history-context enrichment layer is added. The current success/failure events only carry `initiated_event_id` and `target_workflow_id`, so the serializer cannot recover `target_run_id` or namespace by itself.

Namespace source contract: WHEN any event requires a human-readable Temporal `namespace` string THEN the system SHALL use an explicitly threaded namespace name from edge/runtime request context. If only `NamespaceId` is available, the serializer SHALL continue populating `namespace_id` fields where the proto supports them and SHALL leave human-readable `namespace` empty rather than serializing a UUID as a namespace name. The kernel and serializer MUST NOT perform implicit namespace lookup.

2.26 WHEN Nexus operation events are serialized THEN the system SHALL wire through `operation_id` on terminal events (the kernel already carries it but the serializer discards it via `_` bindings). The `NexusOperationStarted.operation_token` rename is deferred until `tokeira_proto` is regenerated with that field; until then the serializer continues populating the existing `operation_id` field, which is already correct.

2.27 WHEN update events are serialized THEN the system SHALL populate sequencing event IDs and request metadata from the kernel's update tracking data

2.28 WHEN a `WorkflowExecutionOptionsUpdated` event is serialized THEN the system SHALL populate `versioning_override` once the kernel's `VersioningOverride` type has a proto mapping

2.29 WHEN Tokeira-specific pause/resume events are serialized into SDK-visible Temporal history THEN the system SHALL NOT encode them as `WorkflowExecutionCanceledEventAttributes`. The implementation SHALL either (a) filter these internal events from SDK-visible history, (b) encode them as `MarkerRecorded` with a stable Tokeira marker name and payload, or (c) define a dedicated compatibility encoding in a separate spec. The chosen behavior MUST be regression-tested so pause/resume cannot be mistaken for cancellation.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN any `HistoryEventKind` variant is serialized with fields that are already correctly wired (e.g. `workflow_type`, `task_queue`, `input`, `result`, `failure`, `scheduled_event_id`, `started_event_id` on events that already populate them) THEN the system SHALL CONTINUE TO produce identical proto field values

3.2 WHEN a `WorkflowExecutionStarted` event is serialized THEN the system SHALL CONTINUE TO correctly populate `workflow_type`, `task_queue`, `input`, `memo`, `search_attributes`, `retry_policy`, `attempt`, `workflow_execution_timeout`, `workflow_run_timeout`, `workflow_task_timeout`, `parent_workflow_execution`, `parent_initiated_event_id`, `continued_execution_run_id`, `continued_failure`, `last_completion_result`, and `cron_schedule`

3.3 WHEN workflow task events (`Scheduled`, `Started`, `Completed`, `Failed`, `TimedOut`) are serialized THEN the system SHALL CONTINUE TO correctly populate `task_queue`, `start_to_close_timeout`, `attempt`, `scheduled_event_id`, `started_event_id`, `identity`, `cause`, and `failure` fields

3.4 WHEN activity task events are serialized THEN the system SHALL CONTINUE TO correctly populate `activity_id`, `activity_type`, `task_queue`, `input`, `header`, `retry_policy`, timeout fields, `scheduled_event_id`, `started_event_id`, `result`, `failure`, and `details`

3.5 WHEN timer events are serialized THEN the system SHALL CONTINUE TO correctly populate `timer_id`, `start_to_fire_timeout`, and `started_event_id` (on `TimerFired`)

3.6 WHEN child workflow events are serialized THEN the system SHALL CONTINUE TO correctly populate fields already carried by each event: initiated child events preserve `workflow_id`, `workflow_type`, `task_queue`, `input`, `namespace_id`, and `parent_close_policy`; child-started events preserve `workflow_id`, child `run_id`, `workflow_type`, and `initiated_event_id`; terminal child events preserve `workflow_id`, `initiated_event_id`, `started_event_id`, `result`, and `failure` where those values are present.

3.7 WHEN external signal/cancel events are serialized THEN the system SHALL CONTINUE TO correctly populate fields already carried by each event: initiated external events preserve target workflow ID, target run ID when present, signal name, input, and control; result/failure events preserve `initiated_event_id`, target workflow ID, and failure cause where present.

3.8 WHEN Nexus operation events are serialized THEN the system SHALL CONTINUE TO correctly populate `endpoint`, `service`, `operation`, `input`, `schedule_to_close_timeout`, `scheduled_event_id`, `result`, and `failure`

3.9 WHEN update events are serialized THEN the system SHALL CONTINUE TO correctly populate `protocol_instance_id`, `accepted_request`, `meta`, `outcome`, and `failure`

3.10 WHEN the `serialize_history` function is called THEN the system SHALL CONTINUE TO produce valid protobuf-encoded bytes that decode to a `temporal.api.history.v1.History` message without error

3.11 WHEN deprecated proto fields are written for v0.4 SDK wire-compat (e.g. `ContinuedAsNew.failure`, `SignalExternal.control`, `NexusStarted.operation_id`) THEN the system SHALL CONTINUE TO populate those deprecated fields for backward compatibility

### Implementation Classification

4.1 Serializer-only fixes include fields already present on `HistoryEventKind`, such as `NexusOperationCompleted.operation_id`, `NexusOperationFailed.operation_id`, `NexusOperationCanceled.operation_id`, and `NexusOperationTimedOut.operation_id`, plus any currently ignored fields explicitly carried by the event.

4.2 Kernel event enrichment fixes include start identity/header, producing `workflow_task_completed_event_id` fields, timeout retry `new_execution_run_id`, activity completion/failure/cancel worker identity and retry state, timer/marker producing event IDs, child-start optional attributes, child terminal metadata, external signal/cancel target namespace/run ID, and update sequencing/request metadata not currently carried on update events. Child-started header and continued-as-new header/inherit-build-id remain intentionally defaulted until their command/runtime contracts are enriched.

4.3 Runtime/history-context enrichment fixes include values derived from history size, task-start request IDs, or relationships to earlier events if the team chooses not to duplicate them into each kernel event. This context must be passed explicitly to the serializer or applied before serialization; `history_serializer.rs` remains a pure event-to-proto projector.

4.4 Deferred proto-sync fixes include fields not exposed by the currently generated `tokeira_proto` surface or fields owned by a separate feature spec. `NexusOperationStarted.operation_token` remains deferred until the generated proto exposes the v1.62 field; the current serializer should keep writing `operation_id`.
