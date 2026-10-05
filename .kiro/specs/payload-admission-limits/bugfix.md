# Bugfix Requirements Document

## Introduction

Temporal v1.31.0 bounds what a client or worker can send. Each payload field may hold at most 2 MiB (`limit.blobSize.error`; above `limit.blobSize.warn`, 512 KiB, it only logs), a memo at most 2 MiB (`limit.memoSize.error`), and search attributes at most 100 keys of 2 KiB each and 40 KiB in all (`common/dynamicconfig/constants.go:316-334, 807-820 @ v1.31.0`). Over a limit, a client call fails with `InvalidArgument`, and a worker's response becomes a non-retryable server failure.

Tokeira checks none of these limits when a request arrives. Only the gRPC layer's 4 MiB message cap bounds a payload. So payloads that v1.31.0 refuses, up to 4 MiB, reach the runtime and storage. There they are either stored, which v1.31.0 never does, or fail at DSQL's 1 MiB column limit with a storage error instead of v1.31.0's answer.

This spec covers the limits v1.31.0 applies when a request arrives. The limits on a workflow task's commands, and on a run's signals, updates, history and state, are a separate change.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN SignalWorkflowExecution or SignalWithStartWorkflowExecution carries a signal input over 2 MiB THEN the system accepts it

1.2 WHEN StartWorkflowExecution, SignalWithStartWorkflowExecution or the start in ExecuteMultiOperation carries a workflow input or a memo over 2 MiB THEN the system accepts it

1.3 WHEN a start carries more than 100 search attributes, a search attribute value over 2 KiB, or search attributes over 40 KiB in all THEN the system accepts them

1.4 WHEN QueryWorkflow carries query arguments over 2 MiB THEN the system accepts them

1.5 WHEN RespondActivityTaskCompleted carries a result over 2 MiB, or RespondActivityTaskCanceled carries details over 2 MiB, by task token or by id THEN the system records them

1.6 WHEN RespondActivityTaskFailed, by task token or by id, or RespondWorkflowTaskFailed carries a failure over 2 MiB THEN the system records it

1.7 WHEN RecordActivityTaskHeartbeat carries details over 2 MiB, by task token or by id THEN the system records them

1.8 WHEN RespondActivityTaskFailed carries last heartbeat details over 2 MiB, by task token or by id THEN the system records them

1.9 WHEN StartActivityExecution carries an input over 2 MiB or search attributes over their limits, or RequestCancelActivityExecution or TerminateActivityExecution carries a reason over 2 MiB THEN the system accepts them

### Expected Behavior (Correct)

2.1 WHEN a signal input is over the blob size limit THEN the system SHALL reject the call with `InvalidArgument` "Blob data size exceeds limit." (`service/frontend/workflow_handler.go:2237`; `service/history/api/signal_workflow_util.go:37 @ v1.31.0`)

2.2 WHEN a start's workflow input is over the blob size limit THEN the system SHALL reject the call with `InvalidArgument` "Blob data size exceeds limit.". WHEN its memo is over the memo size limit THEN it SHALL reject the call with `InvalidArgument` "Memo size exceeds limit." The input is checked first (`service/history/api/create_workflow_util.go:230-256 @ v1.31.0`).

2.3 WHEN a start's search attributes exceed the key, value or total limit THEN the system SHALL reject the call with `InvalidArgument` "number of search attributes N exceeds limit L", "search attribute NAME value size N exceeds size limit L" or "total size of search attributes N exceeds size limit L", checked in that order. A workflow start SHALL check them before its input and memo (`common/searchattribute/validator.go:60-75, 145-175`; `service/frontend/workflow_handler.go:6148-6153 @ v1.31.0`). A standalone activity's start SHALL check the key count before its registered-key check and the sizes after it, as v1.31.0's `Validate` and `ValidateSize` do (`chasm/lib/activity/validator.go:249-271 @ v1.31.0`).

2.4 WHEN QueryWorkflow's query arguments are over the blob size limit THEN the system SHALL reject the call with `InvalidArgument` "Blob data size exceeds limit." (`service/frontend/workflow_handler.go:3156 @ v1.31.0`)

2.5 WHEN an activity's completion result or cancellation details are over the blob size limit THEN the system SHALL fail the activity instead, with a non-retryable server failure "Complete result exceeds size limit." or "Cancel details exceed size limit.", and SHALL answer the call as succeeded (`service/frontend/workflow_handler.go:1603-1640, 1705-1735, 2012-2040, 2112-2145 @ v1.31.0`)

2.6 WHEN a failure in RespondActivityTaskFailed or RespondWorkflowTaskFailed is over the blob size limit THEN the system SHALL record in its place a non-retryable server failure "Failure exceeds size limit." whose cause is the original failure truncated to the warn limit. For an activity, the response SHALL also list that server failure in its `failures` (`service/frontend/workflow_handler.go:1231-1245, 1824-1838, 1948-1962 @ v1.31.0`; `common/failure/failure.go:48-95 @ v1.31.0`)

2.7 WHEN heartbeat details are over the blob size limit THEN the system SHALL fail the activity instead, with a non-retryable server failure "Heartbeat details exceed size limit.", and SHALL answer with `cancel_requested` true (`service/frontend/workflow_handler.go:1406-1435, 1510-1540 @ v1.31.0`)

2.8 WHEN RespondActivityTaskFailed's last heartbeat details are over the blob size limit THEN the system SHALL drop them, record the failure without them, and list a non-retryable server failure "Heartbeat details exceed size limit." in the response's `failures` (`service/frontend/workflow_handler.go:1804-1821, 1927-1944 @ v1.31.0`)

2.9 WHEN StartActivityExecution's input is over the blob size limit THEN the system SHALL reject the call with `InvalidArgument` "input exceeds length limit", before its search attributes. WHEN a RequestCancelActivityExecution or TerminateActivityExecution reason is longer than the blob size limit THEN the system SHALL reject the call with `InvalidArgument` "reason exceeds length limit" (`chasm/lib/activity/validator.go:344-353, 402-412, 475-485 @ v1.31.0`).

2.10 Sizes SHALL be measured as v1.31.0 measures them: the protobuf-encoded size of the field's message (`Payloads`, `Memo`, `Failure`, `SearchAttributes`), except that a search attribute value is the length of its payload's data. A size SHALL exceed a limit only when it is greater than the limit.

2.11 The checks SHALL apply alike to workflow activities and standalone activities, and above the warn limit the system SHALL only log.

2.12 Production builds SHALL use v1.31.0's defaults. Conformance builds SHALL read `limit.blobSize.error`, `limit.blobSize.warn`, `limit.memoSize.error`, `limit.memoSize.warn`, `frontend.searchAttributesNumberOfKeysLimit`, `frontend.searchAttributesSizeOfValueLimit` and `frontend.searchAttributesTotalSizeLimit` from overrides, as they do the callback limits.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN every field of a request is within its limit THEN the system SHALL CONTINUE TO handle it as today

3.2 The gRPC 4 MiB receive limit, the HTTP API's 4 MiB body limit and the Nexus 2 MiB limits SHALL CONTINUE TO apply as today

3.3 The registered-key check SHALL CONTINUE TO apply where it applies today: to a standalone activity's start and to a workflow task's search attribute upserts

### Out of Scope

- Limits on a workflow task's commands, which v1.31.0 enforces by failing the task and terminating the run.
- The limits on a run's signals (10,000), updates, buffered events, history, state and history batches.
- Payloads between 1 and 2 MiB, which v1.31.0 accepts and which DSQL still can't store in one column.
- Registered-key checks on a workflow start. v1.31.0 rejects a key that isn't registered (`common/searchattribute/validator.go:101 @ v1.31.0`); Tokeira's workflow starts don't check, which is a separate gap.
