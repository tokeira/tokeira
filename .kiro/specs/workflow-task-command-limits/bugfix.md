# Bugfix Requirements Document

## Introduction

Temporal v1.31.0 checks what a workflow task's completion carries as it applies the completion. Each command's payload is checked against the blob size limit (2 MiB, `limit.blobSize.error`), a memo against the memo size limit (2 MiB, `limit.memoSize.error`), and search attributes against the key count (100), value (2 KiB) and total (40 KiB) limits (`common/dynamicconfig/constants.go:316-335, 807-821 @ v1.31.0`). Each update protocol message's body is checked against the blob size limit, and pending Nexus operations against 30 (`components/nexusoperations/config.go:36-43 @ v1.31.0`). A payload over its limit terminates the workflow. A count over its limit fails the workflow task. A query result over the blob size limit fails that query.

Tokeira checks none of these when a completion is applied. An oversized command is written into history and run state, where it is either stored or fails at DSQL's 1 MiB column limit with a storage error instead of v1.31.0's answer. A run can also hold more search attributes and pending Nexus operations than v1.31.0 allows.

This spec covers the limits on a workflow task's completion and on query results. The limits checked when a request arrives are in [payload-admission-limits](../payload-admission-limits/bugfix.md). The limits on a run's growth (history, state, buffered events) and on its signals and updates are separate changes.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a workflow task completion carries a ScheduleActivityTask input, CompleteWorkflowExecution result, FailWorkflowExecution failure, RecordMarker details, ContinueAsNewWorkflowExecution input, StartChildWorkflowExecution input, SignalExternalWorkflowExecution input or ScheduleNexusOperation input over the blob size limit THEN the system records it

1.2 WHEN ContinueAsNewWorkflowExecution or StartChildWorkflowExecution carries a memo over the memo size limit, or search attributes over the value or total limit THEN the system records them

1.3 WHEN UpsertWorkflowSearchAttributes or ModifyWorkflowProperties carries upserted fields over the blob size limit, or leaves the run's search attributes or memo over their limits THEN the system records them

1.4 WHEN UpsertWorkflowSearchAttributes, ContinueAsNewWorkflowExecution or StartChildWorkflowExecution carries more than 100 search attributes THEN the system records them

1.5 WHEN an update protocol message in a completion has a body over the blob size limit THEN the system processes it

1.6 WHEN ScheduleNexusOperation is sent while the run has 30 pending Nexus operations THEN the system schedules it

1.7 WHEN a query result in a workflow task completion, or in RespondQueryTaskCompleted, is over the blob size limit THEN the system delivers it

### Expected Behavior (Correct)

2.1 WHEN a command's payload is over the blob size limit THEN the system SHALL terminate the workflow and SHALL answer the completion with `InvalidArgument` `"{cause}: {message}"`, with this cause and message (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:472-478, 705-711, 760-766, 924-930, 983-989, 1106-1112, 1188-1194`; `components/nexusoperations/workflow/commands.go:144-150 @ v1.31.0`):

| Command | Measured | Cause | Message |
|---|---|---|---|
| ScheduleActivityTask | the input | `BadScheduleActivityAttributes` | `ScheduleActivityTaskCommandAttributes.Input exceeds size limit.` |
| CompleteWorkflowExecution | the result | `BadScheduleActivityAttributes` (v1.31.0's own choice) | `CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.` |
| FailWorkflowExecution | the failure | `BadFailWorkflowExecutionAttributes` | `FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.` |
| RecordMarker | the details map | `BadRecordMarkerAttributes` | `RecordMarkerCommandAttributes.Details exceeds size limit.` |
| ContinueAsNewWorkflowExecution | the input | `BadContinueAsNewAttributes` | `ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.` |
| StartChildWorkflowExecution | the input | `BadStartChildExecutionAttributes` | `StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.` |
| SignalExternalWorkflowExecution | the input | `BadSignalWorkflowExecutionAttributes` | `SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.` |
| ScheduleNexusOperation, except to the `__temporal_system` endpoint | the input | `BadScheduleNexusOperationAttributes` | `ScheduleNexusOperationCommandAttributes.Input exceeds size limit` |

2.2 WHEN ContinueAsNewWorkflowExecution's or StartChildWorkflowExecution's memo is over the memo size limit, or its search attributes are over the value or total limit, THEN the system SHALL terminate the workflow with the command's cause from 2.1. The memo messages are `ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.` and `StartChildWorkflowExecutionCommandAttributes.Memo exceeds size limit.`. The search attribute messages are `search attribute {name} value size {size} exceeds size limit {limit}` and `total size of search attributes {size} exceeds size limit {limit}`. The input is checked first, then the memo, then the search attributes (`workflow_task_completed_handler.go:983-1006, 1106-1129`; `common/searchattribute/validator.go:148-177 @ v1.31.0`).

2.3 WHEN UpsertWorkflowSearchAttributes' upserted fields are over the blob size limit THEN the system SHALL terminate the workflow with `BadSearchAttributes` and `UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.`. WHEN they leave the run's search attributes over the value or total limit THEN it SHALL terminate the workflow with `BadSearchAttributes` and the search attribute messages from 2.2 (`workflow_task_completed_handler.go:1241-1264 @ v1.31.0`).

2.4 WHEN ModifyWorkflowProperties' upserted memo fields are over the blob size limit THEN the system SHALL terminate the workflow with `BadModifyWorkflowPropertiesAttributes` and `ModifyWorkflowPropertiesCommandAttributes exceeds size limit.`. WHEN they leave the run's memo over the memo size limit THEN it SHALL terminate the workflow with `BadModifyWorkflowPropertiesAttributes` and `ModifyWorkflowPropertiesCommandAttributes. Memo exceeds size limit.` (`workflow_task_completed_handler.go:1292-1311 @ v1.31.0`).

2.5 WHEN UpsertWorkflowSearchAttributes, ContinueAsNewWorkflowExecution or StartChildWorkflowExecution carries more than 100 search attributes THEN the system SHALL fail the workflow task with `BadSearchAttributes`. The upsert's message is `number of search attributes {count} exceeds limit {limit}`. ContinueAsNewWorkflowExecution's is `invalid SearchAttributes on ContinueAsNewWorkflowExecutionCommand: {that message}. WorkflowType={type} TaskQueue={task queue}`, where v1.31.0 prints the task queue as protobuf text, whose spacing Go's protobuf library varies on purpose; and StartChildWorkflowExecution's is `invalid SearchAttributes on StartChildWorkflowCommand: {that message}. WorkflowId={id} WorkflowType={type} Namespace={namespace}` (`common/searchattribute/validator.go:60-75`; `service/history/api/command_attr_validator.go:360, 446-448, 524-526 @ v1.31.0`). On an upsert the count comes before the registered-key check, as it does in v1.31.0's `Validate`.

2.6 WHEN an update protocol message's body is over the blob size limit THEN the system SHALL terminate the workflow with `BadUpdateWorkflowExecutionMessage` and `Message type {body type} exceeds size limit.`, where the body type is the full name of the body's message, such as `temporal.api.update.v1.Acceptance`. The body is checked before the message is processed, both for a message a PROTOCOL_MESSAGE command references and for one processed after the commands (`workflow_task_completed_handler.go:187-198, 350-365`; `common/protocol/naming.go:44-63 @ v1.31.0`).

2.7 WHEN ScheduleNexusOperation is sent while the run has 30 or more pending Nexus operations THEN the system SHALL fail the workflow task with `PendingNexusOperationsLimitExceeded` and `workflow has reached the pending nexus operation limit of 30 for this namespace`. Operations scheduled earlier in the same completion count (`components/nexusoperations/workflow/commands.go:173-181 @ v1.31.0`).

2.8 WHEN the system terminates the workflow under 2.1-2.4 or 2.6 THEN it SHALL discard the completion, and in one transition SHALL record `WorkflowTaskFailed` with the command's cause, a server failure whose message is `"{cause}: {message}"` and which is not marked non-retryable, and the worker's identity; then the buffered events; then `WorkflowExecutionTerminated` with that message as its reason, no details and the identity `history-service`. It SHALL schedule no new workflow task (`service/history/api/respondworkflowtaskcompleted/api.go:470-512, 1049-1059 @ v1.31.0`).

2.9 The system SHALL process commands in request order and stop at the first limit or validation failure. Within a command, the checks SHALL run in v1.31.0's order: each command's attribute validation first; then its payload, memo and search attribute size checks in the order of 2.1-2.4; then its pending-count check. SignalExternalWorkflowExecution checks its pending count before its input size. ScheduleNexusOperation checks its input size before its pending count (`workflow_task_completed_handler.go:159-211, 426-1316`; `commands.go:144-181 @ v1.31.0`).

2.10 WHEN the workflow task's attempt is greater than 1 and the cause is not `UnhandledCommand` THEN the system SHALL persist nothing for a limit failure, terminating or not, and SHALL answer the completion with `InvalidArgument` and the same message (`api.go:478-481 @ v1.31.0`).

2.11 WHEN a query result in a workflow task completion is over the blob size limit THEN the system SHALL fail that query, and its caller SHALL receive `InvalidArgument` `Blob data size exceeds limit.` (`api.go:956-991 @ v1.31.0`). WHEN RespondQueryTaskCompleted's result is over the blob size limit THEN the system SHALL deliver a failed result whose error message is `Blob data size exceeds limit.`, and the call SHALL succeed (`service/frontend/workflow_handler.go:2914-2934 @ v1.31.0`).

2.12 Sizes SHALL be measured as v1.31.0 measures them: the protobuf-encoded size of the command's field (`Payloads`, `Failure`, `Memo`, `SearchAttributes`, and a single `Payload` for a Nexus input); RecordMarker details as the sum of each key's length and its `Payloads` size; upserted search attribute and memo fields as the sum of each key's length and its payload's data length; a search attribute value as its payload's data length; and a protocol message body as the protobuf-encoded size of its `Any` (`common/util.go:653-661`; `workflow_task_completed_handler.go:1318-1326 @ v1.31.0`). The run's search attributes and memo after an upsert are its current values with the upserted fields applied as the run applies them, where a field whose payload is JSON `null` or `[]` removes its key. A size SHALL exceed a limit only when it is greater than the limit.

2.13 The limits SHALL be v1.31.0's default values, given in the introduction, and SHALL be fixed: Tokeira offers no setting to change them. Above the blob warn limit (512 KiB) or the memo warn limit (2 KiB) the system SHALL only log.

2.14 The four causes these criteria use that Tokeira lacks, `BadFailWorkflowExecutionAttributes` (8), `BadModifyWorkflowPropertiesAttributes` (25), `BadScheduleNexusOperationAttributes` (32) and `PendingNexusOperationsLimitExceeded` (33), SHALL be appended to the persisted cause enum, so that existing values keep their encoding, and SHALL carry v1.31.0's names and proto values (`temporal/api/enums/v1/failed_cause.proto` in api-go v1.62.8).

### Unchanged Behavior (Regression Prevention)

3.1 WHEN every command, message and query result of a completion is within its limits THEN the system SHALL CONTINUE TO handle the completion as today

3.2 The existing command validations and the four pending-entity limits SHALL CONTINUE TO apply with their causes and messages, and in their order relative to each other

3.3 CancelWorkflowExecution's details, command headers and command user metadata SHALL CONTINUE TO be unchecked, as in v1.31.0 (`workflow_task_completed_handler.go:344-346, 840-868 @ v1.31.0`)

3.4 RespondWorkflowTaskFailed's failure SHALL CONTINUE TO be checked as [payload-admission-limits](../payload-admission-limits/bugfix.md) criterion 2.6 requires

### Out of Scope

- Registered-key, predefined-key and value-type validation of the search attributes on ContinueAsNewWorkflowExecution and StartChildWorkflowExecution commands. Tokeira checks registered keys only on upserts, which is a separate gap.
- Nexus command validation beyond the input size and the pending count: endpoint lookup, the service and operation name lengths (1,000), the header size (8 KiB) and disallowed headers. Tokeira doesn't carry a Nexus command's header today, which is a separate gap.
- The limits on a run's history, state, history batches and buffered events, and on its signals and updates.
- The `__temporal_system` endpoint's input, which v1.31.0 doesn't check.
