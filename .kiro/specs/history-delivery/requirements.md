# Requirements Document

## Introduction

Temporal SDKs replay workflow code by consuming history events from the server. Today, Tokeira's `PollWorkflowTaskQueue` response returns an empty `history_blob`, which means no SDK can replay workflow logic. This feature closes that gap by:

1. Adding `temporal.api.history.v1` proto message types so history events can be serialized to the Temporal wire format.
2. Populating the `history_blob` field in the poll response with real serialized history.
3. Exposing a `GetWorkflowExecutionHistory` RPC for SDK cache-miss replay.
4. Wiring the projection-backed `VisibilityQueryService` into `tokeirad` so list/count queries return real data.
5. Reflecting actual workflow completion status in the `RespondWorkflowTaskCompleted` response.

## Glossary

- **History_Serializer**: The module in `tokeira-edge` that converts kernel `HistoryEvent` values into proto-encoded bytes using the `temporal.api.history.v1.HistoryEvent` message type.
- **History_Proto**: The set of protobuf message definitions under `temporal.api.history.v1` and `temporal.api.enums.v1` that describe history events on the wire.
- **Poll_Translator**: The `poll_response_to_proto` function in `tokeira-edge/src/grpc/translate.rs` that builds the gRPC `PollWorkflowTaskQueueResponse`.
- **History_Loader**: The `from_internal::poll_response` function in `tokeira-edge/src/translate/from_internal.rs` that constructs the edge-layer `PollWorkflowTaskQueueResponse` from a `StartedWorkflowTask`.
- **Run_Repository**: The `RunRepository` trait in `tokeira-storage/src/api.rs` that provides `read_history` for loading persisted events.
- **Kernel_Event**: The `HistoryEvent` struct and `HistoryEventKind` enum in `tokeira-kernel/src/event.rs`.
- **Workflow_Service**: The `WorkflowService` struct in `tokeira-edge/src/workflow_service.rs` that orchestrates gRPC endpoint logic.
- **Visibility_Query_Service**: The `VisibilityQueryService` in `tokeira-projection/src/query_service.rs` that implements `VisibilityApi` backed by the projection store.
- **Mutation_Outcome**: The `WorkflowMutationOutcome` struct in `tokeira-edge/src/workflow_service.rs` returned after a workflow state transition.
- **Workflow_State**: The `WorkflowState` struct in `tokeira-kernel/src/state.rs` containing the authoritative run state including `status`.

## Requirements

### Requirement 1: History Proto Definitions

**User Story:** As an SDK developer, I want Tokeira to emit history events in the standard `temporal.api.history.v1.HistoryEvent` proto format, so that SDK replay logic can consume them without custom deserialization.

#### Acceptance Criteria

1. THE History_Proto SHALL define a `temporal.api.history.v1.HistoryEvent` message containing an `event_id` (int64), `event_time` (int64 unix nanos), `event_type` (enum), and a `oneof attributes` field for each event kind.
2. THE History_Proto SHALL define a `temporal.api.enums.v1.EventType` enum with one variant per Kernel_Event `HistoryEventKind` discriminant (e.g. `EVENT_TYPE_WORKFLOW_EXECUTION_STARTED`, `EVENT_TYPE_ACTIVITY_TASK_SCHEDULED`).
3. THE History_Proto SHALL define one attributes message per event kind (e.g. `WorkflowExecutionStartedEventAttributes`, `ActivityTaskScheduledEventAttributes`) with fields matching the Kernel_Event variant fields.
4. THE History_Proto SHALL define a `temporal.api.history.v1.History` message containing a `repeated HistoryEvent events` field.
5. WHEN the proto definitions are compiled, THE generated Rust code SHALL be accessible from the `tokeira_proto` crate under a `history` module path.

### Requirement 2: History Event Serialization

**User Story:** As an SDK developer, I want the server to serialize kernel history events into proto-encoded bytes, so that the poll response carries a valid history blob.

#### Acceptance Criteria

1. THE History_Serializer SHALL convert each Kernel_Event into the corresponding `temporal.api.history.v1.HistoryEvent` proto message, preserving `event_id`, `happened_at` (as unix nanos), and all variant-specific fields.
2. THE History_Serializer SHALL map each `HistoryEventKind` discriminant to the matching `EventType` enum value.
3. WHEN a Kernel_Event contains `Payloads`, `Memo`, or `SearchAttributes` fields, THE History_Serializer SHALL convert them to the corresponding `temporal.api.common.v1` proto messages.
4. WHEN a Kernel_Event contains `time::Duration` fields, THE History_Serializer SHALL encode them as millisecond integer values in the proto attributes message.
5. WHEN a Kernel_Event contains `time::OffsetDateTime` fields, THE History_Serializer SHALL encode them as unix nanosecond integer values.
6. FOR ALL valid Kernel_Event values, serializing to proto bytes and then deserializing back SHALL produce an equivalent proto message (round-trip property).

### Requirement 3: History in Poll Response

**User Story:** As an SDK developer, I want `PollWorkflowTaskQueue` to return the full workflow history, so that the SDK can replay workflow code on each workflow task.

#### Acceptance Criteria

1. WHEN a workflow task is started, THE History_Loader SHALL read the full event history for the run from the Run_Repository using `read_history`.
2. THE History_Loader SHALL populate the `WorkflowTaskPayloadDto.history` field with the loaded Kernel_Event list.
3. THE Poll_Translator SHALL serialize the history events from the payload DTO into proto-encoded bytes using the History_Serializer.
4. THE Poll_Translator SHALL set the `history_blob` field of `PollWorkflowTaskQueueResponse` to the serialized bytes.
5. WHEN the run has zero history events beyond the initial state, THE Poll_Translator SHALL set `history_blob` to a valid empty `History` proto message encoding.
6. IF the Run_Repository returns an error during history loading, THEN THE History_Loader SHALL propagate the error to the caller.

### Requirement 4: GetWorkflowExecutionHistory RPC

**User Story:** As an SDK developer, I want a `GetWorkflowExecutionHistory` endpoint, so that the SDK can fetch history for replay after a sticky cache miss.

#### Acceptance Criteria

1. THE History_Proto SHALL define `GetWorkflowExecutionHistoryRequest` and `GetWorkflowExecutionHistoryResponse` messages in the `workflowservice.v1` package.
2. THE `GetWorkflowExecutionHistoryRequest` message SHALL contain `namespace` (string), `execution` (`WorkflowExecution`), and `maximum_page_size` (int32) fields.
3. THE `GetWorkflowExecutionHistoryResponse` message SHALL contain a `history` field of type `temporal.api.history.v1.History`.
4. WHEN a valid `GetWorkflowExecutionHistoryRequest` is received, THE Workflow_Service SHALL resolve the workflow execution to a run key, read the full history from the Run_Repository, serialize the events to proto, and return them in the response.
5. IF the workflow execution does not exist, THEN THE Workflow_Service SHALL return a NOT_FOUND gRPC status.
6. THE WorkflowService proto definition SHALL include the `GetWorkflowExecutionHistory` RPC method.

### Requirement 5: Wire VisibilityQueryService into tokeirad

**User Story:** As an operator, I want list and count workflow queries to return real data, so that I can observe running and completed workflows.

#### Acceptance Criteria

1. WHEN `tokeirad` starts, THE server bootstrap SHALL construct a `Visibility_Query_Service` backed by the projection store instead of `EmptyVisibilityApi`.
2. THE server bootstrap SHALL pass the `Visibility_Query_Service` instance to the `Workflow_Service` constructor as the `VisibilityApi` implementation.
3. WHEN a `ListWorkflowExecutions` request is received, THE Workflow_Service SHALL delegate to the `Visibility_Query_Service` and return real execution data.
4. WHEN a `CountWorkflowExecutions` request is received, THE Workflow_Service SHALL delegate to the `Visibility_Query_Service` and return real counts.

### Requirement 6: RespondWorkflowTaskCompleted Completion Status

**User Story:** As an SDK developer, I want the `RespondWorkflowTaskCompleted` response to reflect whether the workflow has completed, so that the SDK can clean up local state.

#### Acceptance Criteria

1. THE Mutation_Outcome SHALL carry the post-transition execution status from the Workflow_State.
2. WHEN the post-transition status is a terminal state (Completed, Failed, Canceled, Terminated, TimedOut, ContinuedAsNew), THE Poll_Translator SHALL set `workflow_completed` to `true` in the `RespondWorkflowTaskCompletedResponse`.
3. WHEN the post-transition status is Running, THE Poll_Translator SHALL set `workflow_completed` to `false`.
4. WHEN the post-transition status is ContinuedAsNew, THE Poll_Translator SHALL set `new_run_id` to the new run's identifier.
5. IF the transition was a duplicate (idempotent replay), THEN THE Poll_Translator SHALL set `workflow_completed` to `false`.
