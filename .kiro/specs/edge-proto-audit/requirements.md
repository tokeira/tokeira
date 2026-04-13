# Requirements Document

## Introduction

Tokeira aims to be API-compatible with Temporal. After migrating `tokeira-proto` to upstream Temporal API protos (v1.43.0), the SDK examples fail because the edge/proto translation layer drops, ignores, or incorrectly maps proto attributes. This spec establishes the principle that every attribute of every upstream proto message must be faithfully translated through the system, audits the current implementation against that principle, and validates correctness using the SDK examples in `sdk-core/crates/sdk/examples/`.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **History_Serializer**: The module `tokeira-edge/src/translate/history_serializer.rs` that converts kernel `HistoryEvent` values into proto `temporal.api.history.v1.History` messages.
- **Command_Translator**: The functions in `tokeira-edge/src/grpc/translate.rs` that convert proto `Command` messages into kernel `WorkflowCommand` variants and vice versa.
- **Long_Poll**: Server behavior where `GetWorkflowExecutionHistory` with `wait_new_event=true` blocks until a matching event appears rather than returning immediately.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

### Requirement 1: Full proto attribute fidelity

**User Story:** As an SDK user, I want tokeira to faithfully translate every attribute of every upstream Temporal API proto message through the full pipeline (proto → edge DTO → kernel/runtime → edge DTO → proto), so that tokeira is API-compatible with Temporal and any conforming SDK works without modification.

#### Acceptance Criteria

1. FOR EVERY field defined in an upstream proto request message that tokeira accepts (e.g. `StartWorkflowExecutionRequest`, `RespondWorkflowTaskCompletedRequest`, `PollWorkflowTaskQueueRequest`), THE Edge_Layer SHALL extract the field value and propagate it to the appropriate kernel or runtime structure. Fields that tokeira does not yet implement SHALL be explicitly documented as unsupported rather than silently dropped.
2. FOR EVERY field defined in an upstream proto response message that tokeira returns (e.g. `PollWorkflowTaskQueueResponse`, `PollActivityTaskQueueResponse`, `GetWorkflowExecutionHistoryResponse`), THE Edge_Layer SHALL populate the field from the corresponding kernel or runtime data. Fields SHALL NOT be left at their proto default when the system has the data to populate them.
3. FOR EVERY command variant in `temporal.api.command.v1.Command`, THE Command_Translator SHALL extract all attributes from the proto command and map them to the corresponding `WorkflowCommand` variant. Command types that tokeira does not yet support SHALL return an explicit unsupported error rather than silently failing.
4. FOR EVERY history event variant in `temporal.api.history.v1.HistoryEvent`, THE History_Serializer SHALL populate all attribute fields from the kernel `HistoryEventKind`. Kernel fields that are destructured but ignored (using `_` patterns) SHALL be mapped to their corresponding proto fields.
5. FOR EVERY `google.protobuf.Timestamp` or `google.protobuf.Duration` field in an upstream proto message, THE Edge_Layer SHALL use the well-known type conversion helpers (`to_proto_timestamp`, `to_proto_duration`) rather than raw integer representations.

### Requirement 2: Data threading through kernel and runtime

**User Story:** As an SDK user, I want data that originates in proto commands (like activity_type, workflow_id, retry_policy, timeouts) to be available in the corresponding proto responses, so that the SDK can function correctly.

#### Acceptance Criteria

1. WHEN the SDK sends a command containing data that must appear in a subsequent poll response or history event (e.g. `activity_type` in `ScheduleActivityTaskCommandAttributes`), THE system SHALL thread that data through the kernel → runtime → edge pipeline without loss.
2. WHEN the kernel's `WorkflowCommand` enum or the runtime's task structs lack a field needed to satisfy criterion 1, THE implementation SHALL add the field to the appropriate struct(s) and propagate it.
3. WHEN the runtime creates a `StartedActivityTask` or `StartedWorkflowTask`, THE runtime SHALL include all metadata needed by the SDK (activity_type, workflow_id, workflow_type, task_queue, attempt, timeouts).

### Requirement 3: Implement GetWorkflowExecutionHistory long-poll

**User Story:** As an SDK user, I want `GetWorkflowExecutionHistory` with `wait_new_event=true` to block until a matching event appears, so that `client.get_result()` does not busy-loop.

#### Acceptance Criteria

1. WHEN the Edge_Layer receives a `GetWorkflowExecutionHistory` request with `wait_new_event=true` and no matching events exist, THE Edge_Layer SHALL block the response until a matching event is committed or a timeout (60 seconds) elapses.
2. WHEN a matching event is committed while the request is waiting, THE Edge_Layer SHALL return the history including the new event.
3. WHEN the timeout elapses without a matching event, THE Edge_Layer SHALL return the current history (which may be empty of matching events).
4. WHEN `wait_new_event=false`, THE Edge_Layer SHALL return immediately with the current history.

### Requirement 4: SDK example end-to-end validation

**User Story:** As a developer, I want each SDK example to run successfully against tokeirad, so that I have concrete evidence the translation layer is correct.

#### Acceptance Criteria

1. WHEN the `hello_world` example worker and starter are run against tokeirad, THE workflow SHALL complete and return a greeting string.
2. WHEN the `activity_heartbeating` example is run against tokeirad, THE activity heartbeats SHALL be recorded and the workflow SHALL complete.
3. WHEN the `timer_examples` example is run against tokeirad, THE timers SHALL fire and the workflow SHALL complete.
4. WHEN the `message_passing` example is run against tokeirad, THE signals, queries, and updates SHALL be delivered and the workflow SHALL complete.
5. WHEN the `child_workflows` example is run against tokeirad, THE child workflows SHALL be started, collected, and the parent SHALL complete.
6. WHEN the `continue_as_new` example is run against tokeirad, THE workflow SHALL continue as new and eventually complete.
7. WHEN the `cancellation` example is run against tokeirad, THE workflow and activity cancellation with cleanup SHALL complete.
