# Requirements Document

## Introduction

This feature adds a Temporal-compatible gRPC transport layer to Tokeira, wiring the existing `tokeira-edge` service layer and `tokeira-proto` generated bindings into a real tonic gRPC server. The edge crate already contains `WorkflowService`, `OperatorService`, and `HealthService` with interceptors, routing, long-poll gating, namespace resolution, and translate DTOs. The proto crate already contains Temporal-compatible protobuf definitions with tonic server/client codegen. What is missing is the glue: tonic `Service` trait implementations that accept proto request types, translate them through the existing edge services, and return proto response types — plus the server bootstrap that binds to a TCP port and serves traffic.

The scope is deliberately narrow: wire the existing internal service layer to gRPC, add gRPC-appropriate error mapping, and make `tokeirad` start a real gRPC server. This does not add new workflow semantics, new kernel commands, or new storage capabilities.

## Glossary

- **Edge_Server**: The tonic-based gRPC server process that accepts Temporal-compatible RPCs and delegates to the existing edge service layer.
- **WorkflowService_Adapter**: The tonic `WorkflowService` server trait implementation that bridges proto request/response types to the existing `tokeira_edge::WorkflowService`.
- **OperatorService_Adapter**: The tonic `OperatorService` server trait implementation that bridges proto request/response types to the existing `tokeira_edge::OperatorService`.
- **HealthService_Adapter**: Not included in the initial gRPC transport. Health is served via `OperatorService::GetClusterInfo`. A dedicated gRPC health endpoint may be added in a future milestone.
- **Proto_Translator**: The conversion layer in `tokeira-edge::grpc::translate` that maps between proto wire types and `tokeira-edge::translate` DTOs. Lives in the edge crate (not tokeira-proto) to avoid a dependency cycle.
- **Error_Mapper**: The component that translates `EdgeError` variants into appropriate gRPC status codes and error details.
- **Server_Builder**: The bootstrap component in `tokeirad` that constructs the tonic `Server`, registers service implementations, and binds to a configurable address.

## Requirements

### Requirement 1: WorkflowService gRPC Adapter

**User Story:** As a Temporal SDK user, I want to call Tokeira's WorkflowService over gRPC using standard Temporal proto definitions, so that existing Temporal SDKs and tooling work without modification.

#### Acceptance Criteria

1. WHEN a `StartWorkflowExecution` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request into the existing edge `StartWorkflowExecutionRequest`, invoke `WorkflowService::start_workflow_execution`, and return a proto `StartWorkflowExecutionResponse`.
2. WHEN a `SignalWorkflowExecution` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request into the existing edge `SignalWorkflowExecutionRequest`, invoke `WorkflowService::signal_workflow_execution`, and return a proto `SignalWorkflowExecutionResponse`.
3. WHEN a `PollWorkflowTaskQueue` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request into the existing edge `PollWorkflowTaskQueueRequest`, invoke `WorkflowService::poll_workflow_task_queue`, and return a proto `PollWorkflowTaskQueueResponse`.
4. WHEN a `RespondWorkflowTaskCompleted` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request into the existing edge `RespondWorkflowTaskCompletedRequest`, invoke `WorkflowService::respond_workflow_task_completed`, and return a proto `RespondWorkflowTaskCompletedResponse`.
5. WHEN a `DescribeWorkflowExecution` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request, invoke `WorkflowService::describe_workflow_execution`, and return a proto `DescribeWorkflowExecutionResponse`.
6. WHEN a `ListWorkflowExecutions` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request, invoke `WorkflowService::list_workflow_executions`, and return a proto `ListWorkflowExecutionsResponse`.
7. WHEN a `CountWorkflowExecutions` gRPC request is received, THE WorkflowService_Adapter SHALL translate the proto request, invoke `WorkflowService::count_workflow_executions`, and return a proto `CountWorkflowExecutionsResponse`.

### Requirement 2: OperatorService gRPC Adapter

**User Story:** As a platform operator, I want to call Tokeira's OperatorService over gRPC, so that I can manage cluster metadata and search attributes using standard Temporal tooling.

#### Acceptance Criteria

1. WHEN a `GetClusterInfo` gRPC request is received, THE OperatorService_Adapter SHALL invoke `OperatorService::cluster_info` and return a proto `GetClusterInfoResponse`.
2. WHEN an `AddSearchAttributes` gRPC request is received, THE OperatorService_Adapter SHALL translate the proto attribute map, invoke `OperatorService::upsert_search_attribute` for each attribute, and return a proto `AddSearchAttributesResponse`.
3. WHEN a `ListSearchAttributes` gRPC request is received, THE OperatorService_Adapter SHALL invoke `OperatorService::list_search_attributes` and return a proto `ListSearchAttributesResponse` with system and custom attribute maps.

### Requirement 3: gRPC Error Mapping

**User Story:** As a Temporal SDK user, I want gRPC errors from Tokeira to use standard gRPC status codes, so that SDK retry logic and error handling work correctly.

#### Acceptance Criteria

1. WHEN an `EdgeError::BadRequest` occurs, THE Error_Mapper SHALL return gRPC status `INVALID_ARGUMENT` with the error message.
2. WHEN an `EdgeError::Unauthorized` occurs, THE Error_Mapper SHALL return gRPC status `UNAUTHENTICATED` with the error message.
3. WHEN an `EdgeError::Forbidden` occurs, THE Error_Mapper SHALL return gRPC status `PERMISSION_DENIED` with the error message.
4. WHEN an `EdgeError::NamespaceNotFound` or `EdgeError::WorkflowNotFound` occurs, THE Error_Mapper SHALL return gRPC status `NOT_FOUND` with the error message.
5. WHEN an `EdgeError::TooManyLongPolls` occurs, THE Error_Mapper SHALL return gRPC status `RESOURCE_EXHAUSTED` with the error message.
6. WHEN an `EdgeError::Internal` occurs, THE Error_Mapper SHALL return gRPC status `INTERNAL` with the error message.
7. WHEN an `EdgeError::LongPollAdmissionTimeout` occurs, THE Error_Mapper SHALL return gRPC status `DEADLINE_EXCEEDED` with the error message.
8. WHEN an `EdgeError::NamespaceDeleted` occurs, THE Error_Mapper SHALL return gRPC status `FAILED_PRECONDITION` with the error message.

### Requirement 4: gRPC Metadata Extraction

**User Story:** As a Temporal SDK user, I want my request metadata (request IDs, authorization headers) to flow through gRPC into the edge interceptor pipeline, so that authentication, authorization, and request tracing work end-to-end.

#### Acceptance Criteria

1. THE WorkflowService_Adapter SHALL extract gRPC metadata from the tonic `Request` and convert it into an `http::HeaderMap` before passing it to the edge service methods.
2. WHEN a gRPC request includes an `x-request-id` metadata entry, THE WorkflowService_Adapter SHALL preserve that value through to the edge interceptor pipeline.
3. WHEN a gRPC request includes `authorization` metadata, THE WorkflowService_Adapter SHALL include it in the `HeaderMap` passed to the edge interceptors.

### Requirement 5: Server Bootstrap and Configuration

**User Story:** As a platform operator, I want `tokeirad` to start a gRPC server on a configurable address, so that I can run Tokeira as a real service accepting Temporal SDK connections.

#### Acceptance Criteria

1. THE Server_Builder SHALL construct a tonic `Server` with the WorkflowService_Adapter and OperatorService_Adapter registered.
2. THE Server_Builder SHALL bind the gRPC server to a configurable socket address, defaulting to `[::1]:7233` (the standard Temporal port).
3. WHEN the server starts successfully, THE Server_Builder SHALL log the bound address at info level.
4. THE Server_Builder SHALL enable gRPC reflection using the file descriptor sets already generated by `tokeira-proto`, so that tools like `grpcurl` can discover available services.
5. IF the server fails to bind to the configured address, THEN THE Server_Builder SHALL return an error with a descriptive message including the attempted address.

### Requirement 6: Proto-to-Edge Translation Layer

**User Story:** As a developer extending Tokeira, I want clean bidirectional translation between proto wire types and edge DTOs, so that the gRPC adapter stays thin and the translation logic is reusable and testable.

#### Acceptance Criteria

1. THE Proto_Translator SHALL convert `workflowservice::StartWorkflowExecutionRequest` into `translate::StartWorkflowExecutionRequest` using conversion helpers owned by `tokeira-edge::grpc::translate` for payloads, memo, search attributes, and task queue.
2. THE Proto_Translator SHALL convert `translate::StartWorkflowExecutionResponse` into `workflowservice::StartWorkflowExecutionResponse`.
3. THE Proto_Translator SHALL convert `workflowservice::PollWorkflowTaskQueueRequest` into `translate::PollWorkflowTaskQueueRequest` with appropriate defaults for timeout and sticky TTL.
4. THE Proto_Translator SHALL convert `translate::PollWorkflowTaskQueueResponse` into `workflowservice::PollWorkflowTaskQueueResponse`, including task token bytes, workflow execution identity, started event ID, and attempt. History serialization is deferred to a follow-up milestone; the initial implementation returns an empty history.
5. THE Proto_Translator SHALL convert `workflowservice::RespondWorkflowTaskCompletedRequest` into `translate::RespondWorkflowTaskCompletedRequest`, translating each proto `Command` into the corresponding `WorkflowCommand` variant.
6. THE Proto_Translator SHALL convert proto `Command` messages with `schedule_activity`, `start_timer`, `complete_workflow`, `fail_workflow`, `upsert_search_attributes`, and `upsert_memo` attributes into the corresponding `WorkflowCommand` variants.
7. IF a proto `Command` message has no recognized `attributes` variant set, THEN THE Proto_Translator SHALL return a conversion error.
8. FOR ALL valid edge DTOs whose fields are in scope for the initial gRPC transport milestone, converting to proto and back to edge DTO SHALL produce an equivalent value. DTO fields explicitly deferred in this milestone, such as poll-response history payloads, are excluded from this round-trip requirement until that transport is implemented.

### Requirement 7: Long-Poll gRPC Behavior

**User Story:** As a Temporal worker, I want `PollWorkflowTaskQueue` to behave as a long-poll over gRPC, so that I receive tasks with minimal latency without busy-polling.

#### Acceptance Criteria

1. WHEN a `PollWorkflowTaskQueue` request is received and no task is immediately available, THE WorkflowService_Adapter SHALL hold the gRPC stream open until a task becomes available or the poll timeout expires.
2. WHEN the poll timeout expires without a task becoming available, THE WorkflowService_Adapter SHALL return an empty response (default `PollWorkflowTaskQueueResponse`) rather than an error.
3. WHILE the long-poll gate has reached its concurrency limit, THE WorkflowService_Adapter SHALL reject new poll requests with gRPC status `RESOURCE_EXHAUSTED`.

### Requirement 8: Runtime Adapter Wiring

**User Story:** As a developer, I want the existing `TokeiraRuntime` to be wired as the `WorkflowRuntimeApi` implementation behind the gRPC server, so that gRPC requests drive real workflow transitions through the kernel and storage.

#### Acceptance Criteria

1. THE Server_Builder SHALL create a `WorkflowRuntimeApi` adapter that delegates to the existing `TokeiraRuntime`.
2. THE Server_Builder SHALL create an `ExecutionResolver` backed by the existing storage layer's current-run and execution-summary data, so that `DescribeWorkflowExecution` and `SignalWorkflowExecution` observe authoritative workflow identity without a separate in-memory source of truth.
3. THE Server_Builder SHALL wire the `InMemoryStore`, `TokeiraRuntime`, storage-backed `ExecutionResolver`, edge services, and gRPC adapters together in `tokeirad` so that a `StartWorkflowExecution` gRPC call results in a committed workflow transition in the in-memory store and subsequent describe/signal requests resolve against that same committed state.
