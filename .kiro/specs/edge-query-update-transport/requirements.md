# Requirements Document

## Introduction

The `message_passing` SDK example fails because query and update dispatch do not work end-to-end through the gRPC transport. The runtime has internal query dispatch (`QueryTask`, `QueryResult`) and update lifecycle (`UpdateRegistry`, `UpdateOutcome`) mechanisms, but the edge/gRPC layer does not wire them to the SDK's expected protocol.

Queries use the modern `queries` map on `PollWorkflowTaskQueueResponse` (field 14) with results returned via `query_results` on `RespondWorkflowTaskCompletedRequest` (field 8). Updates use `protocol.v1.Message` entries in the `messages` field on both poll response (field 15) and completion request (field 11). The legacy single-query mechanism (`query` field 10, responded via `RespondQueryTaskCompleted`) provides backward compatibility.

This feature wires the existing runtime query and update dispatch through the edge layer so that the SDK receives queries and updates piggybacked on workflow task poll responses, and the edge layer routes results from workflow task completions back to waiting callers.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate that translates between gRPC proto messages and internal runtime types.
- **WorkflowServiceGrpc**: The tonic gRPC service implementation that implements the `temporal.api.workflowservice.v1.WorkflowService` proto interface.
- **WFT**: Workflow Task — a unit of work dispatched to a worker that carries history, queries, and update messages.
- **Poll_Response**: The `PollWorkflowTaskQueueResponse` proto message returned to workers when they poll for workflow tasks.
- **Completion_Request**: The `RespondWorkflowTaskCompletedRequest` proto message sent by workers when they complete a workflow task.
- **Queries_Map**: The `map<string, WorkflowQuery> queries` field (field 14) on `PollWorkflowTaskQueueResponse` that carries pending queries keyed by query ID.
- **Query_Results_Map**: The `map<string, WorkflowQueryResult> query_results` field (field 8) on `RespondWorkflowTaskCompletedRequest` that carries query results keyed by query ID.
- **Messages_Field**: The `repeated protocol.v1.Message messages` field on both `PollWorkflowTaskQueueResponse` (field 15) and `RespondWorkflowTaskCompletedRequest` (field 11) that carries update protocol messages.
- **QueryTask**: The runtime's `QueryTask` struct containing a query type, arguments, and a oneshot response channel.
- **QueryResult**: The runtime's `QueryResult` enum with `Completed` and `Failed` variants.
- **UpdateRegistry**: The runtime's in-memory registry of waiting update callers, keyed by `(RunKey, update_id)`.
- **UpdateOutcome**: The runtime's `UpdateOutcome` enum with `Accepted`, `Completed`, and `Rejected` variants.
- **Protocol_Message**: A `temporal.api.protocol.v1.Message` proto that wraps update protocol bodies (`update.v1.Request`, `update.v1.Acceptance`, `update.v1.Rejection`, `update.v1.Response`) in a typed `google.protobuf.Any` envelope.
- **Legacy_Query**: The single-query mechanism using the `query` field (field 10) on `PollWorkflowTaskQueueResponse`, responded via `RespondQueryTaskCompleted`.
- **StartedWorkflowTask**: The runtime struct returned by `poll_workflow_task` containing `run_key`, `workflow_id`, `task_queue`, and `token`.
- **Broker**: The `InMemoryBroker` that manages query task queues and workflow task queues, providing `publish_query_task` and `poll_query_task` methods.

## Requirements

### Requirement 1: Query Piggybacking on WFT Poll Response

**User Story:** As an SDK worker, I want pending queries to be included in the `queries` map of the workflow task poll response, so that I can evaluate query handlers after replaying history.

#### Acceptance Criteria

1. WHEN the runtime has pending `QueryTask` entries for a workflow's task queue and a worker polls for a workflow task, THE Edge_Layer SHALL collect pending query tasks and include them in the `queries` map of the Poll_Response.
2. WHEN no pending queries exist for the polled task queue, THE Edge_Layer SHALL return the Poll_Response with an empty `queries` map.
3. FOR EACH query included in the `queries` map, THE Edge_Layer SHALL use the query's unique identifier as the map key and populate the `WorkflowQuery` proto with the `query_type` and `query_args` from the `QueryTask`.
4. THE Edge_Layer SHALL retain the `QueryTask` oneshot response channels so that query results from the Completion_Request can be routed back to the waiting callers.
5. WHEN a WFT poll response carries queries, THE Edge_Layer SHALL set `started_event_id` to zero to indicate the task does not advance history (query-only WFT behavior).

### Requirement 2: Query Result Routing from WFT Completion

**User Story:** As an SDK worker, I want to return query results in the `query_results` map of the workflow task completion, so that query callers receive their answers.

#### Acceptance Criteria

1. WHEN a Completion_Request contains a `query_results` map, THE Edge_Layer SHALL extract each entry and route the result back to the corresponding waiting query caller.
2. FOR EACH entry in the `query_results` map, THE Edge_Layer SHALL match the map key (query ID) to the retained `QueryTask` response channel and send the appropriate `QueryResult` (`Completed` with the answer payloads, or `Failed` with the error message).
3. WHEN a `query_results` entry references a query ID that has no retained response channel (caller timed out), THE Edge_Layer SHALL silently discard the result without returning an error.
4. WHEN a Completion_Request contains `query_results` but no commands, THE Edge_Layer SHALL treat the completion as a query-only response and skip command processing.
5. THE Edge_Layer SHALL translate `WorkflowQueryResult` proto `result_type` of `QUERY_RESULT_TYPE_ANSWERED` to `QueryResult::Completed` and `QUERY_RESULT_TYPE_FAILED` to `QueryResult::Failed`.

### Requirement 3: Update Request Messages on WFT Poll Response

**User Story:** As an SDK worker, I want pending update requests to be included in the `messages` field of the workflow task poll response, so that I can validate and process updates.

#### Acceptance Criteria

1. WHEN the runtime has pending update requests for a workflow and a worker polls for a workflow task, THE Edge_Layer SHALL include update request `protocol.v1.Message` entries in the `messages` field of the Poll_Response.
2. FOR EACH pending update, THE Edge_Layer SHALL construct a `protocol.v1.Message` with the `protocol_instance_id` set to the update ID, and the `body` containing an `update.v1.Request` proto with the update's `Meta` (update_id, identity) and `Input` (name, args).
3. WHEN no pending updates exist for the workflow, THE Edge_Layer SHALL return the Poll_Response with an empty `messages` field.
4. THE Edge_Layer SHALL retain the association between update IDs and the `UpdateRegistry` entries so that update response messages from the Completion_Request can be routed back to waiting callers.

### Requirement 4: Update Response Message Routing from WFT Completion

**User Story:** As an SDK worker, I want to return update acceptance, rejection, and completion messages in the `messages` field of the workflow task completion, so that update callers receive their outcomes.

#### Acceptance Criteria

1. WHEN a Completion_Request contains `messages` with `update.v1.Acceptance` bodies, THE Edge_Layer SHALL silently acknowledge the acceptance without routing it to the `UpdateRegistry`. Acceptance is produced by the runtime directly from the kernel commit path (`UpdateOutcome::Accepted`), not from worker messages. The `UpdateRegistry` only stores completion waiters with `Completed`/`Rejected`/`RunClosed` resolutions.
2. WHEN a Completion_Request contains `messages` with `update.v1.Rejection` bodies, THE Edge_Layer SHALL notify the corresponding `UpdateRegistry` entry with a `Rejected` resolution containing the failure message.
3. WHEN a Completion_Request contains `messages` with `update.v1.Response` bodies, THE Edge_Layer SHALL notify the corresponding `UpdateRegistry` entry with a `Completed` resolution containing the result payloads, or a `Rejected` resolution if the outcome is a failure.
4. WHEN a `messages` entry references an update ID that has no waiting caller in the `UpdateRegistry` (caller timed out), THE Edge_Layer SHALL silently discard the message without returning an error.
5. THE Edge_Layer SHALL extract the `protocol_instance_id` from each `protocol.v1.Message` to identify the target update.

### Requirement 5: Edge DTO Extensions for Queries and Messages

**User Story:** As a developer, I want the edge DTOs to carry query and message data, so that the translation layer can populate and extract these fields.

#### Acceptance Criteria

1. THE `PollWorkflowTaskQueueResponse` edge DTO SHALL include a `queries` field of type `HashMap<String, (String, Payloads)>` mapping query ID to (query_type, query_args).
2. THE `PollWorkflowTaskQueueResponse` edge DTO SHALL include a `messages` field carrying serialized update protocol messages.
3. THE `RespondWorkflowTaskCompletedRequest` edge DTO SHALL include a `query_results` field of type `HashMap<String, QueryResult>` mapping query ID to the query result.
4. THE `RespondWorkflowTaskCompletedRequest` edge DTO SHALL include a `messages` field carrying serialized update protocol response messages.

### Requirement 6: Legacy Query Support via RespondQueryTaskCompleted

**User Story:** As an SDK worker using the legacy query protocol, I want to respond to queries delivered via the `query` field using `RespondQueryTaskCompleted`, so that backward compatibility is maintained.

#### Acceptance Criteria

1. WHEN a single legacy query is pending for a workflow, THE Edge_Layer SHALL populate the `query` field (field 10) of the Poll_Response with the `WorkflowQuery` proto.
2. WHEN a `RespondQueryTaskCompleted` request is received, THE Edge_Layer SHALL extract the query result from the request and route it back to the waiting query caller via the retained response channel.
3. WHEN a `RespondQueryTaskCompleted` request references a query that has no waiting caller (caller timed out), THE Edge_Layer SHALL return a successful response without error.
4. THE Edge_Layer SHALL support both legacy (`query` field) and modern (`queries` map) query mechanisms, preferring the modern mechanism when the SDK uses it.

### Requirement 7: Proto Translation for Query Fields

**User Story:** As a developer, I want the gRPC translation layer to correctly serialize and deserialize query-related proto fields, so that queries round-trip through the transport.

#### Acceptance Criteria

1. THE `poll_response_to_proto` function SHALL populate the `queries` map field of the proto `PollWorkflowTaskQueueResponse` from the edge DTO's queries.
2. THE `respond_completed_request_to_edge` function SHALL extract the `query_results` map from the proto `RespondWorkflowTaskCompletedRequest` into the edge DTO.
3. FOR ALL valid query types and payloads, serializing a query into the `queries` map and deserializing the corresponding `query_results` entry SHALL preserve the query ID, result type, and answer payloads (round-trip property).
4. WHEN the `query_results` map is empty or absent in the proto, THE `respond_completed_request_to_edge` function SHALL produce an empty `query_results` in the edge DTO.

### Requirement 8: Proto Translation for Update Message Fields

**User Story:** As a developer, I want the gRPC translation layer to correctly serialize and deserialize update protocol messages, so that updates round-trip through the transport.

#### Acceptance Criteria

1. THE `poll_response_to_proto` function SHALL populate the `messages` repeated field of the proto `PollWorkflowTaskQueueResponse` from the edge DTO's messages.
2. THE `respond_completed_request_to_edge` function SHALL extract the `messages` repeated field from the proto `RespondWorkflowTaskCompletedRequest` into the edge DTO.
3. FOR ALL valid update request messages, serializing an `update.v1.Request` into a `protocol.v1.Message` and deserializing the corresponding `update.v1.Response` SHALL preserve the update ID and protocol instance ID (round-trip property).
4. WHEN the `messages` field is empty or absent in the proto, THE translation functions SHALL produce empty message collections in the edge DTOs.

### Requirement 9: UNSUPPORTED_FIELDS Documentation Update

**User Story:** As a developer, I want the UNSUPPORTED_FIELDS.md to reflect the newly supported fields, so that the documentation stays accurate.

#### Acceptance Criteria

1. WHEN the `queries` map on `PollWorkflowTaskQueueResponse` is implemented, THE UNSUPPORTED_FIELDS.md SHALL remove the `queries` entry from the unsupported list or mark it as supported.
2. WHEN the `messages` field on `PollWorkflowTaskQueueResponse` is implemented, THE UNSUPPORTED_FIELDS.md SHALL remove the `messages` entry from the unsupported list or mark it as supported.
3. WHEN the `query_results` field on `RespondWorkflowTaskCompletedRequest` is implemented, THE UNSUPPORTED_FIELDS.md SHALL remove the `query_results` entry from the unsupported list or mark it as supported.
4. WHEN the `messages` field on `RespondWorkflowTaskCompletedRequest` is implemented, THE UNSUPPORTED_FIELDS.md SHALL remove the `messages` entry from the unsupported list or mark it as supported.

### Requirement 10: End-to-End Query Dispatch

**User Story:** As a user running the `message_passing` SDK example, I want `QueryWorkflow` calls to complete successfully, so that I can read workflow state without modifying it.

#### Acceptance Criteria

1. WHEN a client calls `QueryWorkflow`, THE Edge_Layer SHALL create a `QueryTask` via the runtime, piggyback it on the next WFT poll response in the `queries` map, and route the worker's `query_results` response back to the waiting client.
2. WHEN the query handler returns a result, THE client SHALL receive the result payloads within the configured timeout.
3. WHEN the query handler fails, THE client SHALL receive a query failure response.
4. THE query dispatch SHALL produce no workflow state transitions (no new history events, no transition_seq increment).

### Requirement 11: End-to-End Update Dispatch

**User Story:** As a user running the `message_passing` SDK example, I want `UpdateWorkflowExecution` calls to complete successfully, so that I can modify workflow state and receive the result.

#### Acceptance Criteria

1. WHEN a client calls `UpdateWorkflowExecution`, THE Edge_Layer SHALL create an update request `protocol.v1.Message`, piggyback it on the next WFT poll response in the `messages` field, and route the worker's update response messages back to the waiting client.
2. WHEN the update handler accepts and completes the update, THE client SHALL receive the `UpdateOutcome::Completed` with the result payloads.
3. WHEN the update validator rejects the update, THE client SHALL receive the `UpdateOutcome::Rejected` with the failure message.
4. WHEN the update handler accepts but the client's wait policy is `Accepted`, THE client SHALL receive `UpdateOutcome::Accepted` without waiting for completion.

### Requirement 12: Query Dispatch Must Integrate with WFT Scheduling

**User Story:** As an SDK worker evaluating a query, I want the query delivered on a real workflow task with full history, so that I can replay to the current state before evaluating the query handler.

#### Acceptance Criteria

1. WHEN a query arrives and no worker has the workflow cached on a sticky queue, THE runtime SHALL schedule a real WFT (via the kernel's `schedule_workflow_task`) and the edge layer SHALL piggyback the query on that WFT's poll response.
2. WHEN a query arrives and a worker has the workflow cached on a sticky queue, THE edge layer MAY deliver a query-only task with `started_event_id = 0` to that sticky worker.
3. THE query-only path (`started_event_id = 0`) SHALL only be used for sticky-queue delivery where the worker already has the workflow in memory. For non-sticky delivery, the query MUST be piggybacked on a real WFT.
4. WHEN a real WFT carries piggybacked queries, THE `queries` map SHALL be populated alongside the normal history and `started_event_id`. The worker replays history, evaluates queries, and returns both WFT completion commands and `query_results` together.
5. AS A WORKAROUND until sticky-queue detection is implemented, THE edge layer SHALL set `started_event_id` to the last event ID in the history for query-only tasks, forcing the SDK to replay. This is documented as a temporary measure with a `TODO(correctness)` comment.

### Requirement 13: PollWorkflowExecutionUpdate Long-Poll

**User Story:** As an SDK client, I want to long-poll for update results via `PollWorkflowExecutionUpdate`, so that `execute_update` can wait for the update to complete.

#### Acceptance Criteria

1. WHEN a `PollWorkflowExecutionUpdate` request is received, THE WorkflowServiceGrpc SHALL look up the update in the `UpdateRegistry` and wait for the resolution.
2. WHEN the update completes (accepted, completed, or rejected), THE response SHALL contain the update outcome.
3. WHEN the update is not found in the registry, THE response SHALL return a NOT_FOUND error.
4. WHEN the long-poll times out, THE response SHALL return an empty response indicating no result yet.
5. THE `PollWorkflowExecutionUpdate` RPC SHALL replace the current `Status::unimplemented` stub.
