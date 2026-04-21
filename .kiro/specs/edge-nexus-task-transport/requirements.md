# Requirements Document: Edge Nexus Task Transport

## Introduction

This spec implements the Nexus Task Transport layer — the 3 gRPC handlers for Temporal's Nexus worker polling and completion in the `tokeira-edge` crate, plus the backing Nexus task broker in `tokeira-runtime`. Nexus task transport is the worker-facing side of Nexus operations: workers poll for Nexus tasks (start or cancel operations), execute them, and report results back via completion or failure handlers.

This is Feature 8 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features in the umbrella spec. The work covers 3 gRPC handlers and a supporting broker:

1. **Nexus Task Broker + Poll Handler** (Phase 1): `NexusTaskBroker` in `tokeira-runtime` for queuing Nexus tasks by (namespace, task_queue), task token generation for completion correlation, and the `poll_nexus_task_queue` long-poll handler in `tokeira-edge`.
2. **Completion and Failure Handlers** (Phase 2): `respond_nexus_task_completed` and `respond_nexus_task_failed` handlers that decode task tokens, translate proto responses into kernel commands, and deliver `NexusOperationResolved` back to the originating run.
3. **Integration with Kernel Nexus Operation Lifecycle** (Phase 3): Wire the Nexus task broker into the runtime's dispatch publisher so that `DispatchOp::ScheduleNexusOperation` and `DispatchOp::CancelNexusOperation` targeting worker endpoints (as opposed to external HTTP endpoints) are routed through the broker instead of the `NexusHttpClient`.

The existing runtime-nexus-dispatch spec handles outbound Nexus operations via HTTP to external endpoints. This spec adds the alternative delivery path: when a Nexus endpoint targets a worker (via `EndpointTarget::Worker`), the operation is delivered through the Nexus task broker to a polling worker, following the same poll/complete pattern as workflow tasks and activity tasks.

The Nexus task broker follows the same `Notify`-based long-poll pattern as `InMemoryBroker` (workflow tasks) and `InMemoryActivityBroker` (activity tasks). Tasks are keyed by (namespace, task_queue) matching the `EndpointTarget::Worker` configuration.

Currently all 3 handler stubs exist in `tokeira-edge/src/grpc/workflow_service.rs` returning `Status::unimplemented`.

> **Proto version note:** The upstream proto (Tokeira v1.43.0) defines `RespondNexusTaskFailedRequest` with a `HandlerError error` field. The newer SDK-core proto adds a `Failure failure` field (field 5) and deprecates `error`. This spec targets the Tokeira proto version. The `failure` field support is deferred until the proto is synced.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **NexusTaskBroker**: The in-memory broker (in `tokeira-runtime`) that queues Nexus tasks for worker polling, keyed by (namespace, task_queue). Follows the same `Notify`-based long-poll pattern as `InMemoryBroker` and `InMemoryActivityBroker`.
- **NexusTask**: A unit of work delivered to a Nexus worker via polling. Contains a `Request` (start or cancel operation) and a task token for completion correlation.
- **TaskToken**: An opaque byte sequence encoding the information needed to correlate a completion or failure response back to the originating Nexus operation (run_key, operation_id, scheduled_event_id).
- **NexusRequest**: The proto `temporal.api.nexus.v1.Request` message containing either a `StartOperationRequest` or a `CancelOperationRequest`, plus headers and scheduled_time.
- **NexusResponse**: The proto `temporal.api.nexus.v1.Response` message containing either a `StartOperationResponse` or a `CancelOperationResponse`.
- **StartOperationResponse**: The proto response variant for start operations, with three outcomes: `Sync` (synchronous success with payload), `Async` (asynchronous acceptance with operation_id), or `operation_error` (unsuccessful completion).
- **HandlerError**: The proto `temporal.api.nexus.v1.HandlerError` message representing a handler-level error with an error_type and optional failure details.
- **NexusResolution**: The kernel enum representing how a Nexus operation was resolved: Started, Completed, Failed, Canceled, or TimedOut.
- **EndpointTarget_Worker**: The `EndpointTarget::Worker` variant in the Nexus endpoint configuration, specifying a target namespace and task queue for worker-based dispatch.
- **RuntimeDispatchPublisher**: The `DispatchPublisher` implementation in `tokeira-runtime` that forwards dispatch ops to the appropriate subsystem after a transition is committed.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

---

## Phase 1: Nexus Task Broker and Poll Handler

### Requirement 1: NexusTaskBroker — In-Memory Nexus Task Queue

**User Story:** As a Tokeira developer, I want an in-memory broker that queues Nexus tasks by (namespace, task_queue), so that Nexus workers can poll for and receive Nexus operation tasks.

#### Acceptance Criteria

1. THE NexusTaskBroker SHALL store queued NexusTask entries keyed by (namespace, task_queue) pair.
2. THE NexusTaskBroker SHALL be safe for concurrent access from multiple gRPC handler threads and runtime dispatch tasks.
3. WHEN a NexusTask is published to a (namespace, task_queue) pair, THE NexusTaskBroker SHALL wake any waiting pollers on that queue via a `Notify`-based mechanism (same pattern as `InMemoryBroker`).
4. WHEN a poller calls `poll_nexus_task` and a task is available in the queue, THE NexusTaskBroker SHALL return the task immediately.
5. WHEN a poller calls `poll_nexus_task` and no task is available, THE NexusTaskBroker SHALL block until a task is published or the long-poll timeout expires.
6. WHEN the long-poll timeout expires without a task becoming available, THE NexusTaskBroker SHALL return `None`.

### Requirement 2: Nexus Task Token Generation and Encoding

**User Story:** As a Tokeira developer, I want Nexus tasks to carry opaque task tokens that encode the originator's identity, so that completion and failure handlers can correlate responses back to the originating Nexus operation.

#### Acceptance Criteria

1. WHEN a NexusTask is created for delivery to a worker, THE NexusTaskBroker SHALL generate a task token encoding the originator's run_key, operation_id, and scheduled_event_id.
2. THE task token encoding SHALL be deterministic: the same (run_key, operation_id, scheduled_event_id) tuple SHALL produce the same token bytes.
3. THE task token SHALL be decodable back to the original (run_key, operation_id, scheduled_event_id) tuple by the completion and failure handlers.
4. WHEN a task token cannot be decoded (malformed or truncated bytes), THE decoding function SHALL return a descriptive error.
5. FOR ALL valid (run_key, operation_id, scheduled_event_id) tuples, encoding then decoding the task token SHALL produce the original tuple (round-trip property).

### Requirement 3: poll_nexus_task_queue Handler

**User Story:** As a Temporal SDK user, I want to poll for Nexus tasks via the `poll_nexus_task_queue` gRPC endpoint, so that my Nexus worker can receive and execute Nexus operations.

#### Acceptance Criteria

1. WHEN the `poll_nexus_task_queue` endpoint is called with a valid namespace, identity, and task_queue, THE handler SHALL long-poll the NexusTaskBroker for a task on the specified (namespace, task_queue) pair.
2. WHEN a NexusTask is available, THE handler SHALL return a `PollNexusTaskQueueResponse` containing the task_token and the embedded `Request` (start or cancel operation).
3. WHEN the long-poll times out without a task becoming available, THE handler SHALL return an empty `PollNexusTaskQueueResponse` (no task_token, no request).
4. WHEN the namespace is empty, THE handler SHALL return `INVALID_ARGUMENT`.
5. WHEN the task_queue is missing or has an empty name, THE handler SHALL return `INVALID_ARGUMENT`.

### Requirement 4: Proto Translation for Nexus Task Types

**User Story:** As a Tokeira developer, I want proto translation functions for Nexus task request and response types, so that the gRPC handlers can convert between proto messages and internal domain types.

#### Acceptance Criteria

1. THE Edge_Layer SHALL provide a translation function to construct a proto `temporal.api.nexus.v1.Request` from the internal Nexus task representation, including the `header`, `scheduled_time`, and the `variant` (start_operation or cancel_operation).
2. THE Edge_Layer SHALL provide a translation function to construct a `StartOperationRequest` from the internal representation, preserving `service`, `operation`, `request_id`, `callback`, `payload`, `callback_header`, and `links` fields.
3. THE Edge_Layer SHALL provide a translation function to construct a `CancelOperationRequest` from the internal representation, preserving `service`, `operation`, and `operation_id` fields.
4. WHEN a proto field contains an invalid value (e.g., empty task_queue name), THE translation function SHALL return a descriptive error rather than silently defaulting.

---

## Phase 2: Completion and Failure Handlers

### Requirement 5: respond_nexus_task_completed Handler

**User Story:** As a Temporal SDK user, I want to report successful Nexus task completion via the `respond_nexus_task_completed` gRPC endpoint, so that the originating workflow receives the operation result.

#### Acceptance Criteria

1. WHEN the `respond_nexus_task_completed` endpoint is called with a valid namespace, identity, task_token, and response, THE handler SHALL decode the task_token to extract the originator's run_key, operation_id, and scheduled_event_id.
2. WHEN the response contains a `StartOperationResponse::Sync` variant, THE handler SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Completed` containing the result payload to the originating run.
3. WHEN the response contains a `StartOperationResponse::Async` variant, THE handler SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Started` to the originating run.
4. WHEN the response contains a `StartOperationResponse::operation_error` variant, THE handler SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Failed` containing the failure information to the originating run.
5. WHEN the response contains a `CancelOperationResponse` variant, THE handler SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Canceled` to the originating run.
6. WHEN the task_token is empty or cannot be decoded, THE handler SHALL return `INVALID_ARGUMENT`.
7. WHEN the response is missing (no variant set), THE handler SHALL return `INVALID_ARGUMENT`.
8. IF the kernel rejects the `NexusOperationResolved` command (e.g., operation already resolved or run closed), THEN THE handler SHALL return a successful response (idempotent completion — the operation was already resolved).

### Requirement 6: respond_nexus_task_failed Handler

**User Story:** As a Temporal SDK user, I want to report Nexus task handler errors via the `respond_nexus_task_failed` gRPC endpoint, so that the originating workflow receives the failure information.

#### Acceptance Criteria

1. WHEN the `respond_nexus_task_failed` endpoint is called with a valid namespace, identity, task_token, and error, THE handler SHALL decode the task_token to extract the originator's run_key, operation_id, and scheduled_event_id.
2. WHEN the request contains a `HandlerError`, THE handler SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Failed` containing the error information to the originating run.
3. WHEN the task_token is empty or cannot be decoded, THE handler SHALL return `INVALID_ARGUMENT`.
4. WHEN the error is missing, THE handler SHALL return `INVALID_ARGUMENT`.
5. IF the kernel rejects the `NexusOperationResolved` command (e.g., operation already resolved or run closed), THEN THE handler SHALL return a successful response (idempotent failure — the operation was already resolved).

### Requirement 7: Proto Translation for Nexus Completion Types

**User Story:** As a Tokeira developer, I want proto translation functions for Nexus completion and failure types, so that the response handlers can convert between proto messages and kernel commands.

#### Acceptance Criteria

1. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::Sync` to `NexusResolution::Completed`, extracting the result payload.
2. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::Async` to `NexusResolution::Started`.
3. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::operation_error` to `NexusResolution::Failed`, extracting the failure information from the `UnsuccessfulOperationError`.
4. THE Edge_Layer SHALL provide a translation function from proto `CancelOperationResponse` to `NexusResolution::Canceled`.
5. THE Edge_Layer SHALL provide a translation function from proto `HandlerError` to `NexusResolution::Failed`, extracting the error_type and failure details.
6. WHEN a proto response variant is unrecognized or contains invalid data, THE translation function SHALL return a descriptive error rather than silently defaulting.

---

## Phase 3: Integration with Kernel Nexus Operation Lifecycle

### Requirement 8: Worker-Targeted Nexus Dispatch Routing

**User Story:** As a Tokeira developer, I want the runtime to route Nexus operations targeting worker endpoints through the NexusTaskBroker instead of the NexusHttpClient, so that Nexus workers can execute operations via the poll/complete pattern.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::ScheduleNexusOperation` and the resolved Nexus endpoint has an `EndpointTarget::Worker` target, THE RuntimeDispatchPublisher SHALL publish a NexusTask to the NexusTaskBroker on the target's (namespace, task_queue) pair instead of dispatching via the NexusHttpClient.
2. WHEN a committed transition contains a `DispatchOp::ScheduleNexusOperation` and the resolved Nexus endpoint has an `EndpointTarget::External` target, THE RuntimeDispatchPublisher SHALL continue dispatching via the NexusHttpClient (existing behavior from runtime-nexus-dispatch).
3. THE NexusTask published to the broker SHALL contain a `StartOperationRequest` populated from the dispatch op's service, operation, input payload, and operation_id (as request_id).
4. THE NexusTask published to the broker SHALL carry the originator's run_key, operation_id, and scheduled_event_id for task token generation.

### Requirement 9: Worker-Targeted Nexus Cancel Routing

**User Story:** As a Tokeira developer, I want the runtime to route Nexus cancel operations targeting worker endpoints through the NexusTaskBroker, so that Nexus workers receive cancel requests via the poll/complete pattern.

#### Acceptance Criteria

1. WHEN a committed transition contains a `DispatchOp::CancelNexusOperation` and the resolved Nexus endpoint has an `EndpointTarget::Worker` target, THE RuntimeDispatchPublisher SHALL publish a NexusTask to the NexusTaskBroker on the target's (namespace, task_queue) pair containing a `CancelOperationRequest`.
2. THE `CancelOperationRequest` SHALL carry the service, operation, and operation_id from the dispatch op.
3. WHEN a committed transition contains a `DispatchOp::CancelNexusOperation` and the resolved Nexus endpoint has an `EndpointTarget::External` target, THE RuntimeDispatchPublisher SHALL continue dispatching via the NexusHttpClient (existing behavior).

### Requirement 10: Nexus Task Timeout Tracking Integration

**User Story:** As a Tokeira developer, I want Nexus tasks delivered through the broker to participate in the existing timeout tracking, so that operations dispatched to workers are subject to the same schedule-to-close timeout enforcement as HTTP-dispatched operations.

#### Acceptance Criteria

1. WHEN a NexusTask with a non-None `schedule_to_close_timeout` is published to the NexusTaskBroker, THE Runtime SHALL insert a tracking entry into the existing `NexusTimeoutTrackingState` (same as HTTP-dispatched operations).
2. WHEN a `Command::NexusOperationResolved` with a terminal resolution (Completed, Failed, Canceled) is committed for a broker-dispatched Nexus operation, THE Runtime SHALL remove the corresponding tracking entry from `NexusTimeoutTrackingState`.
3. THE existing `NexusTimeoutScanner` SHALL handle timeout enforcement for broker-dispatched operations without modification — the scanner operates on `NexusTimeoutTrackingState` entries regardless of dispatch path.

### Requirement 11: Nexus Endpoint Registry — Worker Target Support

**User Story:** As a Tokeira developer, I want the Nexus endpoint registry to support worker targets alongside external targets, so that the runtime can route operations to the correct dispatch path.

#### Acceptance Criteria

1. THE `NexusEndpointConfig` SHALL support both `External` (address-based) and `Worker` (namespace + task_queue) target variants.
2. WHEN the RuntimeDispatchPublisher resolves a Nexus endpoint, THE publisher SHALL inspect the target variant to determine whether to dispatch via NexusHttpClient (External) or NexusTaskBroker (Worker).
3. WHEN a `DispatchOp::ScheduleNexusOperation` references an endpoint name that is not present in the NexusEndpointRegistry, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Failed` containing a descriptive "endpoint not found" message (existing behavior preserved).
