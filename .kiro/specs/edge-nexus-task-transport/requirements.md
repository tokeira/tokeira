# Requirements Document: Edge Nexus Task Transport

## Introduction

This spec owns the worker-facing Nexus Task Transport layer: the three public WorkflowService gRPC
handlers for polling, completion, and failure in `tokeira-edge`, plus the backing Nexus task broker in
`tokeira-runtime`. Workers poll for start/cancel work and return the outcome using an opaque task token.
Observable behaviour targets Temporal server v1.31.0; public message shape comes from the vendored
Temporal API v1.62.11.

This is Feature 8 from the umbrella spec `edge-complete-implementation`. The work covers three gRPC
handlers and a supporting broker:

1. **Nexus Task Broker + Poll Handler** (Phase 1): `NexusTaskBroker` in `tokeira-runtime` queues
   Nexus tasks by `(namespace_id, task_queue)`, authors UUID task identifiers, retains server-side
   completion correlation, and backs the `poll_nexus_task_queue` long poll.
2. **Completion and Failure Handlers** (Phase 2): `respond_nexus_task_completed` and
   `respond_nexus_task_failed` decode the v1.31.0 protobuf task token, validate it, atomically consume
   its broker correlation, and route the worker outcome to the waiting caller or originating run.
3. **Integration with Kernel Nexus Operation Lifecycle** (Phase 3): Wire the Nexus task broker into the runtime's dispatch publisher so that `DispatchOp::ScheduleNexusOperation` and `DispatchOp::CancelNexusOperation` targeting worker endpoints (as opposed to external HTTP endpoints) are routed through the broker instead of the `NexusHttpClient`.

The existing `runtime-nexus-dispatch` spec handles workflow-originated operations. This spec adds the
worker-target delivery path when an endpoint uses `EndpointTarget::Worker`. The caller-facing Nexus
HTTP routes and HTTP-to-worker synchronous dispatch are owned by the sibling
`edge-nexus-http-dispatch` spec; both paths share this broker and task-token contract.

The Nexus task broker follows the same `Notify`-based long-poll pattern as `InMemoryBroker` (workflow tasks) and `InMemoryActivityBroker` (activity tasks). Tasks are keyed by (namespace, task_queue) matching the `EndpointTarget::Worker` configuration.

The initial implementation landed, but its JSON task-token representation is not compatible with
v1.31.0. This revision is a conformance correction: the worker-visible token becomes the protobuf
`temporal.server.api.token.v1.NexusTask` shape and opaque workflow/run correlation moves into the
broker. The v1.31.0 source establishes the observable separation between the public token and
server-private result correlation; Tokeira realizes that contract through its own disposable runtime
delivery broker rather than adopting Temporal's service topology.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **NexusTaskBroker**: The in-memory broker (in `tokeira-runtime`) that queues Nexus tasks for worker polling, keyed by (namespace, task_queue). Follows the same `Notify`-based long-poll pattern as `InMemoryBroker` and `InMemoryActivityBroker`.
- **NexusTask**: A unit of work delivered to a Nexus worker via polling. Contains a `Request` (start or cancel operation) and a task token for completion correlation.
- **TaskToken**: The opaque bytes returned to a worker, encoded as the v1.31.0 protobuf
  `temporal.server.api.token.v1.NexusTask` with `namespace_id`, `task_queue`, and UUID `task_id`.
- **TaskCorrelation**: Server-side broker state keyed by `task_id`. It contains the private routing
  information needed to deliver the worker response; it is never serialized into the TaskToken.
- **NexusRequest**: The proto `temporal.api.nexus.v1.Request` message containing either a `StartOperationRequest` or a `CancelOperationRequest`, plus headers and scheduled_time.
- **NexusResponse**: The proto `temporal.api.nexus.v1.Response` message containing either a `StartOperationResponse` or a `CancelOperationResponse`.
- **StartOperationResponse**: The proto response variant for start operations, with three outcomes: `Sync` (synchronous success with payload), `Async` (asynchronous acceptance with operation_id), or `operation_error` (unsuccessful completion).
- **HandlerError**: The proto `temporal.api.nexus.v1.HandlerError` message representing a handler-level error with an error_type and optional failure details.
- **NexusResolution**: The kernel enum representing how a Nexus operation was resolved: Started, Completed, Failed, Canceled, or TimedOut.
- **EndpointTarget_Worker**: The `EndpointTarget::Worker` variant in the Nexus endpoint configuration, specifying a target namespace and task queue for worker-based dispatch.
- **RuntimeDispatchPublisher**: The `DispatchPublisher` implementation in `tokeira-runtime` that forwards dispatch ops to the appropriate subsystem after a transition is committed.
- **Upstream_Proto**: The vendored public Temporal API protobuf definitions at v1.62.11.

## Target State

- Poll responses carry protobuf Nexus task tokens compatible with Temporal v1.31.0.
- The token exposes only `namespace_id`, `task_queue`, and a server-authored UUID `task_id`;
  workflow/run correlation remains process-local broker state.
- Completion and failure responses validate token shape and namespace before any broker or workflow
  side effect, then atomically consume the outstanding correlation by `task_id`.
- Both workflow-originated worker dispatch and caller-facing Nexus HTTP dispatch use the same token
  and broker correlation model.
- Caller-facing Nexus HTTP routing, HTTP protocol serialization, endpoint resolution, and HTTP
  admission metrics remain outside this spec and are owned by `edge-nexus-http-dispatch`.

## Evidence From Current Code

- **Public wire surface:** `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`
  defines the opaque `task_token` fields on Poll/Respond messages.
- **Token shape:** `proto/internal/temporal/server/api/token/v1/message.proto @ v1.31.0` defines
  `NexusTask { namespace_id = 1; task_queue = 2; task_id = 3; }`.
- **Serialization:** `common/tasktoken/serializer.go @ v1.31.0` uses protobuf marshal/unmarshal for
  Nexus task tokens.
- **Poll and correlation behaviour:** `service/matching/matching_engine.go:2449-2490,2530-2625 @
  v1.31.0` demonstrates that the server authors a UUID `taskID`, makes only the three-field token
  worker-visible, and consumes private result correlation by `task_id`. Tokeira implements those
  observable invariants in `NexusTaskBroker`; the cited service split is not part of Tokeira's
  architecture.
- **Frontend validation:** `service/frontend/workflow_handler.go:6035-6130 @ v1.31.0` validates the
  operation token, protobuf task token, JSON failure details, and structured failure before forwarding
  the response for server-side delivery.
- **Namespace guard:** `common/rpc/interceptor/namespace_validator.go @ v1.31.0` rejects a token whose
  namespace differs from the request with `INVALID_ARGUMENT` and the literal message
  `Operation requested with a token from a different namespace.`
- **Current Tokeira defect:** `crates/tokeira-runtime/src/nexus.rs` serializes
  `(run_key, operation_id, scheduled_event_id)` with `serde_json`, while
  `crates/tokeira-edge/src/workflow_service.rs` decodes that private shape directly.

## Task Token Field Policy

| Field | Target policy | Error if invalid | Side-effect impact |
|---|---|---|---|
| `namespace_id` (1) | Canonical namespace UUID of the worker target | Namespace mismatch with a non-empty request namespace returns `INVALID_ARGUMENT` | Checked before consuming broker correlation |
| `task_queue` (2) | Exact normal Nexus task-queue name used for poll delivery | Empty value returns `INVALID_ARGUMENT` as an invalid task token | Checked before consuming broker correlation |
| `task_id` (3) | Unique server-authored UUID identifying one outstanding dispatch | Empty value returns `INVALID_ARGUMENT`; unknown/expired value returns `NOT_FOUND` | Successful Respond atomically removes the outstanding correlation |
| Unknown protobuf fields | Ignored by protobuf decoding | No error | No additional side effect |

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
7. WHEN a dispatch is admitted, THE NexusTaskBroker SHALL author a unique UUID `task_id` and retain
   the dispatch's private TaskCorrelation before making the task visible to pollers.
8. WHEN a completion or failure consumes an outstanding `task_id`, THE NexusTaskBroker SHALL remove
   and return its TaskCorrelation atomically.
9. IF a completion or failure names an unknown, expired, or already-consumed `task_id`, THEN THE
   NexusTaskBroker SHALL leave all other outstanding correlations unchanged.

### Requirement 2: Nexus Task Token Generation and Encoding

**User Story:** As a Temporal SDK user, I want Nexus task tokens to use the v1.31.0 wire format, so
that workers and conformance clients can return tokens without knowing Tokeira's private routing state.

#### Acceptance Criteria

1. WHEN a NexusTask is returned by `poll_nexus_task_queue`, THE Edge_Layer SHALL encode its TaskToken
   as protobuf `temporal.server.api.token.v1.NexusTask` bytes.
2. THE encoded TaskToken SHALL contain the canonical worker-target `namespace_id` in field 1.
3. THE encoded TaskToken SHALL contain the polled `task_queue` in field 2.
4. THE encoded TaskToken SHALL contain the broker-authored UUID `task_id` in field 3.
5. THE encoded TaskToken SHALL NOT contain `run_key`, `workflow_id`, `run_id`, `operation_id`, or
   `scheduled_event_id`.
6. WHEN a valid TaskToken is decoded, THE Edge_Layer SHALL recover the same `namespace_id`,
   `task_queue`, and `task_id` values.
7. IF TaskToken bytes are malformed or truncated protobuf, THEN THE Edge_Layer SHALL return
   `INVALID_ARGUMENT` with `Error deserializing task token.`
8. IF a decoded TaskToken has an empty `task_queue` or `task_id`, THEN THE Edge_Layer SHALL return
   `INVALID_ARGUMENT` without consuming broker correlation.
9. FOR ALL valid `(namespace_id, task_queue, task_id)` tuples, THE task-token codec SHALL preserve all
   three fields across protobuf encode/decode.

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

1. THE Edge_Layer SHALL provide a translation function to construct a proto `temporal.api.nexus.v1.Request` from the internal Nexus task representation, including `scheduled_time` and the `variant` (start_operation or cancel_operation). For worker-dispatched tasks, `header` is empty.
2. THE Edge_Layer SHALL provide a translation function to construct a `StartOperationRequest` from
   the internal representation while preserving `service`, `operation`, `request_id`, `payload`, and
   callback data already attached under the `nexus-async-completion` contract.
3. THE Edge_Layer SHALL provide a translation function to construct a `CancelOperationRequest` from the internal representation, preserving `service`, `operation`, and `operation_id` fields.
4. WHEN a proto field contains an invalid value (e.g., empty task_queue name), THE translation function SHALL return a descriptive error rather than silently defaulting.

---

## Phase 2: Completion and Failure Handlers

### Requirement 5: respond_nexus_task_completed Handler

**User Story:** As a Temporal SDK user, I want to report successful Nexus task completion via the `respond_nexus_task_completed` gRPC endpoint, so that the originating workflow receives the operation result.

#### Acceptance Criteria

1. WHEN `respond_nexus_task_completed` receives a valid TaskToken, THE handler SHALL decode its
   `namespace_id`, `task_queue`, and `task_id` using protobuf.
2. WHEN the response contains a `StartOperationResponse::Sync` variant, THE handler SHALL call `WorkflowRuntimeApi::resolve_nexus_operation` with `NexusResolution::Completed` containing the result payload.
3. WHEN the response contains a `StartOperationResponse::Async` variant, THE handler SHALL call
   `WorkflowRuntimeApi::resolve_nexus_operation` with `NexusResolution::Started` carrying the
   handler-authored operation token (falling back to the deprecated `operation_id` field when the
   modern field is empty). THE handler SHALL NOT require that token to equal Tokeira's scheduled
   operation key. The `links` field is ignored by the workflow-resolution path.
4. WHEN the response contains a `StartOperationResponse::operation_error` variant, THE handler SHALL call `WorkflowRuntimeApi::resolve_nexus_operation` with `NexusResolution::Failed` containing the failure information serialized as a JSON-encoded kernel `Payload` via `nexus_failure_to_kernel_payload`.
5. WHEN the response contains a `CancelOperationResponse` variant, THE handler SHALL acknowledge the
   cancel task without resolving the Nexus operation; v1.31.0's cancel acknowledgement advances the
   cancellation request independently and the operation resolves only through its eventual completion.
6. WHEN the task_token is empty or cannot be decoded, THE handler SHALL return `INVALID_ARGUMENT`
   with the v1.31.0 token error.
7. WHEN the response is absent or has no variant set, THE handler SHALL NOT reject it during
   frontend validation; after correlation consumption, the correlation owner SHALL handle it as an
   empty worker outcome. `RespondNexusTaskCompleted` itself SHALL acknowledge delivery rather than
   inventing a `response is required` validation error
   (`service/frontend/workflow_handler.go:6032-6092 @ v1.31.0`).
8. IF the kernel rejects the `NexusOperationResolved` command (e.g., operation already resolved or run closed), THEN THE handler SHALL return a successful response (idempotent completion — the operation was already resolved).
9. IF the request namespace resolves to an ID different from the token `namespace_id`, THEN THE
   handler SHALL return `INVALID_ARGUMENT` with `Operation requested with a token from a different namespace.`
10. IF an async-success operation token exceeds 4096 bytes, THEN THE handler SHALL return
    `INVALID_ARGUMENT` with `operation token length exceeds allowed limit (<actual>/4096)`.
11. IF an operation-error failure has non-JSON `details`, THEN THE handler SHALL return
    `INVALID_ARGUMENT` with `failure details must be JSON serializable`.
12. WHEN request and response validation succeeds, THE handler SHALL atomically consume the
    TaskCorrelation addressed by `task_id`.
13. IF `task_id` is unknown, expired, or already consumed, THEN THE handler SHALL return `NOT_FOUND`
    with `Nexus task not found or already expired`.
14. WHEN the consumed TaskCorrelation belongs to a workflow-originated dispatch, THE handler SHALL
    route the translated response using its private `(run_key, operation_id, scheduled_event_id)`.
15. WHEN the consumed TaskCorrelation belongs to a caller-facing HTTP dispatch, THE handler SHALL
    return the translated worker response to the HTTP dispatch waiter.

### Requirement 6: respond_nexus_task_failed Handler

**User Story:** As a Temporal SDK user, I want to report Nexus task handler errors via the `respond_nexus_task_failed` gRPC endpoint, so that the originating workflow receives the failure information.

#### Acceptance Criteria

1. WHEN `respond_nexus_task_failed` receives a valid TaskToken, THE handler SHALL decode its
   `namespace_id`, `task_queue`, and `task_id` using protobuf.
2. WHEN the request contains a `HandlerError`, THE handler SHALL call `WorkflowRuntimeApi::resolve_nexus_operation` with `NexusResolution::Failed` containing the error information serialized as a JSON-encoded kernel `Payload` via `nexus_failure_to_kernel_payload`.
3. WHEN the task_token is empty or cannot be decoded, THE handler SHALL return `INVALID_ARGUMENT`.
4. WHEN the error is missing, THE handler SHALL return `INVALID_ARGUMENT`.
5. IF the kernel rejects the `NexusOperationResolved` command (e.g., operation already resolved or run closed), THEN THE handler SHALL return a successful response (idempotent failure — the operation was already resolved).
6. IF the request namespace resolves to an ID different from the token `namespace_id`, THEN THE
   handler SHALL return `INVALID_ARGUMENT` with `Operation requested with a token from a different namespace.`
7. IF the deprecated `error.failure.details` value is non-JSON, THEN THE handler SHALL return
   `INVALID_ARGUMENT` with `failure details must be JSON serializable`.
8. IF the modern `failure` field lacks `NexusHandlerFailureInfo`, THEN THE handler SHALL return
   `INVALID_ARGUMENT` with `request Failure must contain error or failure with NexusHandlerFailureInfo`.
9. WHEN request validation succeeds, THE handler SHALL atomically consume the TaskCorrelation
   addressed by `task_id`.
10. IF `task_id` is unknown, expired, or already consumed, THEN THE handler SHALL return `NOT_FOUND`
    with `Nexus task not found or already expired`.
11. WHEN the consumed TaskCorrelation belongs to a workflow-originated dispatch, THE handler SHALL
    route the translated failure using its private `(run_key, operation_id, scheduled_event_id)`.
12. WHEN the consumed TaskCorrelation belongs to a caller-facing HTTP dispatch, THE handler SHALL
    return the translated worker failure to the HTTP dispatch waiter.

### Requirement 7: Proto Translation for Nexus Completion Types

**User Story:** As a Tokeira developer, I want proto translation functions for Nexus completion and failure types, so that the response handlers can convert between proto messages and kernel commands.

#### Acceptance Criteria

1. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::Sync` to `NexusResolution::Completed`, extracting the result payload.
2. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::Async` to `NexusResolution::Started`.
3. THE Edge_Layer SHALL provide a translation function from proto `StartOperationResponse::operation_error` to `NexusResolution::Failed`, extracting the failure information from the `UnsuccessfulOperationError`.
4. THE Edge_Layer SHALL translate proto `CancelOperationResponse` as a delivery acknowledgement
   with no `NexusResolution`; the acknowledgement advances only the cancellation request, while the
   operation remains pending until its eventual completion.
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
4. THE NexusTask published to the broker SHALL carry the originator's `run_key`, `operation_id`, and
   `scheduled_event_id` as private TaskCorrelation rather than worker-visible task-token fields.

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

1. THE `NexusEndpointConfig` SHALL support both `External` (address-based) and `Worker` (pre-resolved NamespaceId + task_queue) target variants. Namespace name → NamespaceId resolution happens at endpoint registration time, not at dispatch time.
2. WHEN operator configuration defines a Worker endpoint using a namespace name that cannot be
   resolved to a `NamespaceId` at registration time, THE registration/configuration step SHALL fail
   with a descriptive error.
3. IF Worker endpoint namespace resolution fails, THEN THE registration/configuration step SHALL NOT
   insert the endpoint into the `NexusEndpointRegistry`.
4. WHEN the RuntimeDispatchPublisher resolves a Nexus endpoint, THE publisher SHALL inspect the target variant to determine whether to dispatch via NexusHttpClient (External) or NexusTaskBroker (Worker). The Worker target's `namespace_id` is used directly for broker publish — no runtime namespace resolution needed.
5. WHEN a `DispatchOp::ScheduleNexusOperation` references an endpoint name that is not present in the NexusEndpointRegistry, THE RuntimeDispatchPublisher SHALL submit a `Command::NexusOperationResolved` with `NexusResolution::Failed` containing a descriptive "endpoint not found" message (existing behavior preserved).
