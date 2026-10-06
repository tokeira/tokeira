# Workflow Task Command Limits — Bugfix Design

## Overview

The edge measures what each command of a workflow task completion carries, on the proto completion, as v1.31.0 measures it, and passes the sizes to the kernel beside the commands. The kernel checks them as it applies each command, at the point in that command's validation where v1.31.0 checks them, together with the merged search attributes and memo, which only run state can supply, and the pending Nexus operation count. A payload over its limit ends the completion with a terminating rejection. The runtime then records the workflow task's failure, the buffered events and the workflow's termination in one transition. A count over its limit fails the workflow task through the existing invalid-command path. Query results are checked at the edge.

## Glossary

- **Blob size limit:** `limit.blobSize.error`, 2 MiB. Its warn limit, `limit.blobSize.warn`, is 512 KiB.
- **Memo size limit:** `limit.memoSize.error`, 2 MiB. Its warn limit is 2 KiB.
- **Search attribute limits:** at most 100 keys, a value's data of at most 2 KiB, and an encoded map of at most 40 KiB (`frontend.searchAttributes*`).
- **Pending Nexus operation limit:** `component.nexusoperations.limit.operation.concurrency`, 30.
- **Terminating failure:** a limit failure v1.31.0 reports with `terminateWorkflow` (`workflow_task_completed_handler.go:1477-1491 @ v1.31.0`): the workflow task fails and the workflow is terminated. A **failing** one uses `failWorkflowTask` (`:1461-1475`): the workflow task fails and a new one is scheduled.
- **Command sizes:** what the edge measures for one command (criterion 2.12), or nothing for a command with no limited fields.
- **Merged map:** the run's search attributes or memo with an upsert's fields applied, as the kernel would store them.

## Bug Details

### Bug Condition

A completion with a command whose payload, memo or search attributes are over their limits (1.1-1.3), whose search attributes number more than 100 (1.4), or with an update protocol message over the blob size limit (1.5); a ScheduleNexusOperation while 30 operations are pending (1.6); or a query result over the blob size limit (1.7).

### Examples

- A worker completes a workflow with a 3 MiB result. Tokeira records it, and on DSQL the commit fails. v1.31.0 records WorkflowTaskFailed with `BadScheduleActivityAttributes`, terminates the workflow with the reason `BadScheduleActivityAttributes: CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.`, and answers the worker `InvalidArgument` with the same text.
- An upsert sets 101 search attributes. Tokeira records them. v1.31.0 fails the workflow task with `BadSearchAttributes: number of search attributes 101 exceeds limit 100` and schedules a new one.
- A workflow with 30 pending Nexus operations schedules another. Tokeira schedules it. v1.31.0 fails the workflow task with `PendingNexusOperationsLimitExceeded`.

## Expected Behavior

### Preservation Requirements

- A completion within every limit behaves as today (3.1).
- The existing validations and pending-entity limits keep their causes, messages and relative order (3.2). The new checks are inserted between them, at v1.31.0's positions.
- CancelWorkflowExecution's details, command headers and user metadata stay unchecked (3.3).
- RespondWorkflowTaskFailed is unchanged (3.4).

## Root Cause

Nothing on the completion path measures a command. The kernel's command input carries domain payloads, and `WorkflowTaskCompletionLimits` holds only the four pending-entity counts (`crates/tokeira-kernel/src/command.rs`). The kernel has a failing rejection, `Reject::InvalidCommandAttributes`, but no terminating one. The conformance key registry says a workflow task's commands aren't checked (`crates/tokeira-conformance/src/lib.rs`).

## Correctness Properties

Property 1: Command limits match v1.31.0's

_For any_ completion of generated commands, each within or over its limits, the kernel's outcome SHALL be v1.31.0's: the first failing check in request order, and within a command in v1.31.0's order (criterion 2.9), decides whether the completion is applied, fails the workflow task, or terminates the workflow, with v1.31.0's cause and message.

**Validates: Requirements 2.1-2.7, 2.9**

Property 2: A terminating failure is recorded as v1.31.0 records it

_For any_ run with buffered events whose completion has a terminating failure, the committed events SHALL be WorkflowTaskFailed with the cause, the server failure and the worker's identity; then the buffered events; then WorkflowExecutionTerminated with the reason and the `history-service` identity. No event of the completion SHALL be committed, and no workflow task scheduled. On an attempt greater than 1, nothing SHALL be committed.

**Validates: Requirements 2.8, 2.10**

Property 3: Sizes match v1.31.0's measurements

_For any_ generated payloads, memos and search attributes, the sizes the kernel computes for the run's stored values SHALL equal the protobuf-encoded sizes of the payloads Tokeira encodes for them, and the edge's measurements SHALL equal v1.31.0's (criterion 2.12).

**Validates: Requirement 2.12**

## Fix Implementation

### Limits (`crates/tokeira-kernel/src/command.rs`)

- v1.31.0's blob, memo, search attribute and pending Nexus values become constants in a new `tokeira_kernel::limits` module. The edge's `grpc/payload_limits.rs` uses them, so each value has one definition (criterion 2.13).
- `WorkflowTaskCompletionLimits` gains the blob, memo and search attribute limits and `pending_nexus_operations`. The runtime resolves them as it resolves the pending-entity limits (`runtime/workflow_task.rs`). The blob and memo limits are the larger of each warn and error value, since `CheckEventBlobSizeLimit` errors only for a size above both (`common/util.go:578-608 @ v1.31.0`); with v1.31.0's values that is the error limit. The pending Nexus limit has no disabled value: v1.31.0 compares with `>=`, so 0 refuses every operation. The Temporal functional harness's build reads its overrides for the same keys; `conformance-config-override` owns that wiring, and production builds use the constants.

### Measurement (`crates/tokeira-edge/src/grpc/translate.rs`)

- `CommandPayloadSizes`, a kernel type, records for one command:
  - the size of its limited payload field from criterion 2.1's table, absent for a Nexus operation to `__temporal_system`;
  - its memo's size, for ContinueAsNewWorkflowExecution and StartChildWorkflowExecution;
  - its search attributes' key count, each value's data length, and the map's encoded size;
  - for an upsert or ModifyWorkflowProperties, the upserted fields' size (key lengths plus data lengths) and each set field's data length and encoded payload size, for the kernel's merged map;
  - for a protocol message, its body's encoded size and full type name.
- `respond_completed_request_to_edge` measures each proto command before converting it, and keeps each command's sizes with it through the splice of leftover protocol messages. `RespondWorkflowTaskCompletedRequest` gains `command_sizes`, one entry per command, which the runtime passes to the kernel's `WorkflowTaskCompletedRequest`. An empty list means unmeasured, for internal callers, and checks nothing.
- Above a warn limit the edge logs, as `payload_limits` does for requests (criterion 2.13).
- The registered-key check on upserts (`crates/tokeira-edge/src/workflow_service.rs`) first checks the key count, so an upsert with more than 100 keys gets the count message even if it also names an unregistered key (criterion 2.5).

### Checks (`crates/tokeira-kernel/src/kernel.rs`, new `payload_size.rs`)

- `apply_workflow_command` takes each command's sizes and checks them where criterion 2.9 places them, for example in ScheduleActivity after `normalize_activity_command` and before `reject_if_pending_limit_reached`.
- A payload, memo or search attribute size over its limit returns a new terminating rejection, `Reject::CommandExceedsLimit { cause, message }`.
- A key count over 100 fails the workflow task with `BadSearchAttributes` (criterion 2.5). On ContinueAsNewWorkflowExecution and StartChildWorkflowExecution the kernel tests the measured count with `Reject::InvalidCommandAttributes`. On an upsert the edge's registered-key check tests it first, and marks the command as it marks one naming an unregistered key, so the command keeps its place in the order.
- For an upsert, the kernel builds the merged map's sizes from the stored values and the upserted fields' measured sizes. A stored search attribute value is measured as the payload Tokeira encodes for it (`search_attr_value_to_payload`), without the `type` metadata that Tokeira adds and an SDK needn't send, so the merged check can't terminate a run over metadata Tokeira added. A stored memo field is measured as its payload's encoding. `payload_size.rs` holds this arithmetic, which mirrors protobuf's encoding of the domain types.
- ScheduleNexusOperation counts `pending_nexus_operations`, which already includes operations scheduled earlier in the completion, against `limits.pending_nexus_operations`, and fails with `PendingNexusOperationsLimitExceeded` (criterion 2.7). Like the pending-entity limits, it applies to unmeasured completions too.
- A protocol message's body size is checked before the message is applied, for referenced and leftover messages alike (criterion 2.6).
- `WorkflowTaskFailedCause` gains the four causes it lacks, appended after the last variant because the enum is encoded by position in history events (criterion 2.14). The edge's proto mapping and history serializer carry their v1.31.0 values. As with the four pending-entity causes, a node of an earlier release can't decode a `WorkflowTaskFailed` event that carries one.

### Recording a termination (kernel and `crates/tokeira-runtime/src/runtime/workflow_task.rs`)

- `WorkflowTaskFailedRequest` gains `terminate_reason: Option<String>`. With a reason, `apply_workflow_task_failed` records the failure as it does today and flushes the buffered events. It then clears the failed task, so the terminate tail writes no force-close event, and terminates the run through `terminate_run` with that reason, no details and the `history-service` identity, scheduling no workflow task (criterion 2.8). The lane drains the run's update waiters, as it does whenever a commit closes a run.
- The runtime's invalid-command seam handles `CommandExceedsLimit` as it handles `InvalidCommandAttributes`: the same `"{cause}: {message}"` message, the same abort of updates sent on the task, the same drop on an attempt greater than 1 (criterion 2.10), and the same `InvalidArgument` answer. It submits `WorkflowTaskFailed` with the message as `terminate_reason`.

### Query results

- **In a completion** (`grpc/translate.rs`, `workflow_service.rs`): an answer over the blob size limit becomes `QueryResultDto::ResultTooLarge`, and the waiting query fails with `InvalidArgument` `Blob data size exceeds limit.` (criterion 2.11). The runtime's `QueryResult` gains `ResultTooLarge`; a worker-failed result keeps today's QueryFailed answer.
- **RespondQueryTaskCompleted** (`grpc/workflow_service.rs`): a result over the blob size limit is replaced by a failed result with the error message `Blob data size exceeds limit.` and no failure, and the call succeeds.

### Functional harness wiring

- The harness's key registry (`crates/tokeira-conformance/src/lib.rs`) adds `component.nexusoperations.limit.operation.concurrency` as `Wired`, and the compatibility ledger classifies it as a conformance-only override, from which `docs/conformance/v1.31.0/temporal-configuration.md` is regenerated. The seven size keys are already wired; the runtime now reads them too. This serves only the Temporal functional harness, as `conformance-config-override`'s key table records. It is not a Tokeira setting.

### Other specs

- `payload-admission-limits` put a workflow task's commands out of scope; it now points here.
- `conformance-config-override`'s key table says a workflow task's commands aren't checked, and lists no Nexus key. It now names both consult sites and the Nexus key.

### Out of scope

- Search attribute validation on ContinueAsNew and StartChild beyond the key count, and Nexus command validation beyond the input size and pending count (bugfix Out of Scope).
- A run's growth limits, and its signal and update limits, which separate changes cover.

## Testing Strategy

### Exploratory Bug Condition Checking

- Negative controls: with each check removed or moved, the test that covers it fails. Twelve were run: an unchecked activity input, the memo before the input, no termination, a strict Nexus count, no merged-map total, a wrong map-entry size, marker details without keys, a measured system endpoint, the key count after the registered-key check, an unchecked answer in a completion and in RespondQueryTaskCompleted, and a seam that doesn't terminate.

### Property-Based Tests

- Property 1 in the kernel, over generated completions of commands each just within or just over each limit, against a model of v1.31.0's check order.
- Property 2 through the engine's in-process gRPC endpoint on the in-memory store (`crates/tokeira-engine/tests/workflow_task_command_limits.rs`): every terminating command, with and without a signal buffered while the workflow task is started. A close command with a buffered signal fails with `UnhandledCommand` before its size is checked, as in v1.31.0. A later attempt is a unit test there.
- Property 3 in the kernel (`crates/tokeira-kernel/tests/command_limits.rs`), comparing `payload_size.rs` with `prost` encoded lengths of the payloads `tokeira-proto` encodes, merged maps included, and at the edge, comparing measurements with `prost` encoded lengths.

### Unit Tests

- Each command at each of its limits and one byte over, with v1.31.0's cause and message.
- The count message on an upsert that also names an unregistered key.
- The `__temporal_system` endpoint's input stays unchecked.
- A query result over the limit, in a completion and in RespondQueryTaskCompleted: a unit test of the translation, and an engine test of both paths. A query that reaches the runtime after the workflow task is polled is answered as a query task, which the engine test accepts.

### Preservation Checking

- The existing kernel, runtime, edge and conformance tests stay green.
