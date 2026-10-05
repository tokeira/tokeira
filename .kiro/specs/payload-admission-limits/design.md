# Payload Admission Limits — Bugfix Design

## Overview

The edge's gRPC handlers check each payload a request carries against v1.31.0's limits before the request goes further: before translation, and before a standalone activity's token is routed to its own path. A client call over a limit fails with `InvalidArgument`. A worker response over a limit is recorded as v1.31.0 records it, as a non-retryable server failure.

## Glossary

- **Blob size limit:** `limit.blobSize.error`, 2 MiB, for one payload field. Its warn limit, `limit.blobSize.warn`, is 512 KiB.
- **Memo size limit:** `limit.memoSize.error`, 2 MiB. Its warn limit is 2 KiB.
- **Search attribute limits:** at most 100 keys, a value's data of at most 2 KiB, and an encoded map of at most 40 KiB (`frontend.searchAttributes*`).
- **Encoded size:** a message's protobuf-encoded length, `prost::Message::encoded_len`, which is what Go's `Size()` returns. An absent field has size 0.
- **Server failure:** a `Failure` with `ServerFailureInfo { non_retryable: true }` and a message (`common/failure` and `common/util.go:95-113 @ v1.31.0`).

## Bug Details

### Bug Condition

A request with a payload field or reason over the blob size limit (1.1, 1.2, 1.4–1.9), a memo over the memo size limit (1.2), or search attributes over their limits (1.3, 1.9). Below 4 MiB, nothing stops any of them before the runtime.

### Examples

- A 3 MiB signal input is accepted. v1.31.0 answers `InvalidArgument` "Blob data size exceeds limit.".
- An activity completes with a 3 MiB result, which Tokeira records as the result. v1.31.0 fails the activity with "Complete result exceeds size limit." and tells the worker the completion succeeded.
- A start with 101 search attributes is accepted. v1.31.0 answers "number of search attributes 101 exceeds limit 100".

## Expected Behavior

### Preservation Requirements

- A request within every limit behaves as today (3.1).
- The transport caps stay: gRPC 4 MiB, the HTTP API's 4 MiB, and Nexus's 2 MiB (3.2).
- The registered-key check applies where it does today: a standalone activity's start and a workflow task's upserts (3.3).

## Root Cause

The edge never had size limits. A comment in `crates/tokeira-edge/src/grpc/workflow_service.rs` defers `BlobSizeLimitError` to "where that limit is available", and `tokeira-conformance` lists `limit.blobSize.error` as not enforced.

## Correctness Properties

Property 1: The limits match v1.31.0's

_For any_ size and limit, a size SHALL exceed the limit exactly when it is greater than the limit, and the blob, memo and search attribute checks SHALL measure the same sizes as v1.31.0: the encoded size of the field's message, and a search attribute value's data length.

**Validates: Requirements 2.3, 2.10**

Property 2: Truncation matches v1.31.0's

_For any_ failure and size budget, the truncated failure SHALL keep the failure info's kind and its non-retryable flag (and an application failure's type), and SHALL fill source, message and stack trace in that order, each cut at a UTF-8 boundary, within the budget as `TruncateWithDepth` spends it, following causes to a depth of 20 (`common/failure/failure.go:48-95 @ v1.31.0`).

**Validates: Requirement 2.6**

Property 3: Oversized worker responses become failures

_For any_ activity response with a result, cancellation details or heartbeat details over the blob size limit, the activity SHALL be failed with the server failure for that field and the call SHALL succeed, with `cancel_requested` true for a heartbeat. _For any_ failure over the limit, the recorded failure SHALL be the server failure whose cause is the original truncated to the warn limit.

**Validates: Requirements 2.5, 2.6, 2.7, 2.8**

## Fix Implementation

### Limits (`crates/tokeira-edge/src/grpc/payload_limits.rs`, new)

- Constants for v1.31.0's values (criterion 2.12), each read through an accessor as the callback limits are in `grpc/translate.rs`. The Temporal functional harness's build can override the accessors; `conformance-config-override` owns that wiring, and production builds compile the constants alone.
- `check_blob_size(size, operation)` and `check_memo_size(size, operation)`: log a warning above the warn limit, and return the v1.31.0 error above the error limit.
- `check_search_attribute_count(&SearchAttributes)` and `check_search_attribute_sizes(&SearchAttributes)`: the key count, then each value's data length and the map's encoded size, with v1.31.0's messages. They are separate so that a standalone activity's registered-key check can sit between them.
- `server_failure(message)` builds the server failure, and `truncate_failure(&Failure, max_size)` ports `TruncateWithDepth` with a depth of 20.

### Client calls (`crates/tokeira-edge/src/grpc/workflow_service.rs`)

- **StartWorkflowExecution, and the start in ExecuteMultiOperation:** the search attribute count and sizes, then the input, then the memo. A workflow start has no registered-key check today (out of scope).
- **SignalWithStartWorkflowExecution:** the same, then the signal input (`service/history/api/signalwithstartworkflow/api.go:68`; `signal_with_start_workflow.go:85 @ v1.31.0`).
- **SignalWorkflowExecution:** the signal input.
- **QueryWorkflow:** the query arguments.
- **StartActivityExecution:** after the existing request id and identity checks, the input ("input exceeds length limit"), then the search attribute count, the existing registered-key check, and the search attribute sizes.
- **RequestCancelActivityExecution and TerminateActivityExecution:** the reason's length in bytes, beside the existing request id and identity checks that the `validate_sa_request_metadata` comment says it belongs with.

### Worker responses (same file)

- **Failing the activity.** RespondActivityTaskFailed's body, by task token and by id, becomes a helper that the conversions call with a request built from the original's token or ids, its identity, and the server failure. It keeps both paths: workflow activities through translation and `WorkflowService`, standalone activities through the CHASM bridge. The converted call is admitted as RespondActivityTaskFailed. Worker scopes and authorization treat the four activity responses alike (`crates/tokeira-auth/src/worker_scope.rs`), so the outcome doesn't change.
- **RespondActivityTaskCompleted and RespondActivityTaskCanceled**, by task token and by id: over the limit, fail the activity with "Complete result exceeds size limit." or "Cancel details exceed size limit.", and return the call's usual empty response.
- **RecordActivityTaskHeartbeat**, by task token and by id: over the limit, fail the activity with "Heartbeat details exceed size limit." and return `cancel_requested: true`.
- **RespondActivityTaskFailed**, by task token and by id: over the limit, drop the last heartbeat details and add their server failure to the response. Over the limit, replace the failure with "Failure exceeds size limit.", its cause truncated to the warn limit, and add that failure to the response too. Heartbeat details are checked first, as in v1.31.0.
- **RespondWorkflowTaskFailed:** over the limit, replace the failure as above. The response is unchanged.

### Functional harness wiring

- The harness's key registry (`crates/tokeira-conformance/src/lib.rs`) marks the seven keys `Wired`, as `conformance-config-override`'s key table records. This serves only the Temporal functional harness, whose tests shrink the limits to exercise them. It is not a Tokeira setting.

### Other specs

- `conformance-config-override` listed `limit.blobSize.error` among the limits with no consult site. Its key table now records the seven keys as overridable by the harness at request admission.
- `activity-heartbeat-time` says Tokeira enforces no blob size limit on heartbeat details; it points here.

### Out of scope

- A workflow task's commands, and a run's signals, updates, buffered events, history, state and history batches, which a separate change covers.
- Schedules. v1.31.0 checks a schedule's start action when it creates a CHASM schedule (`service/frontend/workflow_handler.go:3446-3466 @ v1.31.0`), and otherwise when the start happens. Tokeira's schedules start runs inside the runtime.

## Testing Strategy

### Exploratory Bug Condition Checking

- A test sends each call with a payload just over its limit and shows it accepted before the fix.

### Property-Based Tests

- Property 1 over generated sizes, payloads and search attribute maps, comparing with `prost` encoded lengths.
- Property 2 over generated failures, with causes and multi-byte text, against a reference model of `TruncateWithDepth`.
- Property 3 through the edge on the in-memory store, for workflow activities, with payloads generated just under and just over the limit.

### Unit Tests

- Each client call at the limit and one byte over, with v1.31.0's message.
- A standalone activity's oversized completion and heartbeat fail the activity, and its start, cancellation and termination refuse an oversized input or reason with v1.31.0's messages.
- The search attribute checks' order, and on a standalone activity's start the registered-key check between the count and the sizes.

### Preservation Checking

- The existing edge, conformance and standalone activity tests stay green.
