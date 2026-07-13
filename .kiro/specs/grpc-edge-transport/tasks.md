# Implementation Plan: gRPC Edge Transport

## Overview

Wire the existing `tokeira-edge` service layer and `tokeira-proto` generated bindings into a Temporal-compatible tonic gRPC server. The implementation follows a bottom-up approach: dependency setup → translation layer → error mapping → metadata extraction → runtime adapter → gRPC service adapters → server bootstrap → tests.

## Tasks

- [x] 1. Add workspace and crate dependencies
  - [x] 1.1 Update workspace `Cargo.toml` with tonic and tonic-reflection
    - Add `tonic = { version = "0.11", features = ["transport"] }` and `tonic-reflection = "0.11"` to `[workspace.dependencies]`
    - _Requirements: 5.1, 5.4_
  - [x] 1.2 Update `tokeira-edge/Cargo.toml` with tokeira-proto and tonic
    - Add `tokeira-proto = { path = "../tokeira-proto" }` and `tonic = { version = "0.11", features = ["transport"] }` to dependencies
    - _Requirements: 1.1, 3.1_
  - [x] 1.3 Update `tokeirad/Cargo.toml` with tokeira-edge, tokeira-proto, tonic, and tonic-reflection
    - Add `tokeira-edge`, `tokeira-proto`, `tonic`, and `tonic-reflection` to dependencies
    - _Requirements: 5.1, 5.4, 8.3_

- [x] 2. Implement proto-to-edge translation layer (`tokeira-edge::grpc::translate`)
  - [x] 2.1 Create `crates/tokeira-edge/src/grpc/translate.rs` with proto→edge request converters
    - Implement `start_request_to_edge`, `signal_request_to_edge`, `poll_request_to_edge`, `respond_completed_request_to_edge`, `describe_request_to_edge`, `list_request_to_edge`, `count_request_to_edge`
    - Implement or colocate the payload, memo, search_attributes, and task_queue conversion helpers inside `tokeira-edge::grpc::translate` rather than depending on a `tokeira_proto::conversions` module
    - Apply default timeout (60s) and sticky_ttl (30s) for poll requests
    - _Requirements: 6.1, 6.3, 6.5, 6.6, 6.7_
  - [x] 2.2 Add edge→proto response converters to the same module
    - Implement `start_response_to_proto`, `poll_response_to_proto`, `signal_response_to_proto`, `completed_response_to_proto`, `describe_response_to_proto`, `list_response_to_proto`, `count_response_to_proto`
    - Poll response populates task token bytes, workflow execution identity, started event ID, and attempt from existing `StartedWorkflowTask` fields; history is empty (deferred)
    - _Requirements: 6.2, 6.4, 6.8_
  - [x] 2.3 Add `WorkflowCommand` ↔ proto `Command` translation
    - Implement `proto_command_to_workflow_command` and `workflow_command_to_proto`
    - Handle `schedule_activity`, `start_timer`, `complete_workflow`, `fail_workflow`, `upsert_search_attributes`, `upsert_memo` variants
    - Return `ProtoConversionError::MissingField("Command.attributes")` when no recognized variant is set
    - _Requirements: 6.5, 6.6, 6.7_
  - [x] 2.4 Register the `translate` module in `grpc/mod.rs`
    - Add `pub mod translate;` to `crates/tokeira-edge/src/grpc/mod.rs`
    - _Requirements: 6.1_

- [x] 3. Implement gRPC metadata extraction and error mapping (`tokeira-edge::grpc`)
  - [x] 3.1 Create `crates/tokeira-edge/src/grpc/metadata.rs`
    - Implement `metadata_to_header_map(metadata: &tonic::metadata::MetadataMap) -> http::HeaderMap`
    - Iterate metadata entries and insert into a fresh HeaderMap, preserving `x-request-id`, `authorization`, and all custom headers
    - _Requirements: 4.1, 4.2, 4.3_
  - [x] 3.2 Create `crates/tokeira-edge/src/grpc/errors.rs`
    - Implement `From<EdgeError> for tonic::Status` with the mapping: BadRequest→INVALID_ARGUMENT, Unauthorized→UNAUTHENTICATED, Forbidden→PERMISSION_DENIED, NamespaceNotFound/WorkflowNotFound→NOT_FOUND, NamespaceDeleted→FAILED_PRECONDITION, TooManyLongPolls→RESOURCE_EXHAUSTED, LongPollAdmissionTimeout→DEADLINE_EXCEEDED, RemoteRouteUnsupported→UNAVAILABLE, Internal→INTERNAL
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_
  - [x] 3.3 Create `crates/tokeira-edge/src/grpc/mod.rs`
    - Declare `pub mod metadata;`, `pub mod errors;`, `pub mod translate;` (and later `workflow_service`, `operator_service`, `runtime_adapter`)
    - Register `pub mod grpc;` in `crates/tokeira-edge/src/lib.rs`
    - _Requirements: 1.1, 3.1_

- [x] 4. Checkpoint — Ensure dependency and foundation modules compile
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement RuntimeAdapter (`tokeira-edge::grpc::runtime_adapter`)
  - [x] 5.1 Create `crates/tokeira-edge/src/grpc/runtime_adapter.rs`
    - Implement `RuntimeAdapter<R>` struct wrapping `Arc<TokeiraRuntime<R>>`
    - Implement `WorkflowRuntimeApi` for `RuntimeAdapter<R>` delegating `start_workflow`, `signal_workflow`, `poll_workflow_task`, `complete_workflow_task` to the runtime
    - Implement `commit_result_to_outcome` helper converting `CommitResult` → `WorkflowMutationOutcome` (using `new_state.last_event_id`, not `history_length` which does not exist)
    - _Requirements: 8.1, 8.2_
  - [x] 5.2 Register `runtime_adapter` in `grpc/mod.rs`
    - Add `pub mod runtime_adapter;`
    - _Requirements: 8.1_

- [x] 6. Implement gRPC service adapters
  - [x] 6.1 Create `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Implement `WorkflowServiceGrpc` struct wrapping `WorkflowService`
    - Implement `workflowservice::workflow_service_server::WorkflowService` tonic trait
    - Each method: extract metadata→HeaderMap, convert proto→edge DTO, call edge service, convert result→proto response or EdgeError→tonic::Status
    - For `poll_workflow_task_queue`: return default empty proto response when edge returns `None`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 7.1, 7.2, 7.3_
  - [x] 6.2 Create `crates/tokeira-edge/src/grpc/operator_service.rs`
    - Implement `OperatorServiceGrpc` struct wrapping `OperatorService`
    - Implement `operatorservice::operator_service_server::OperatorService` tonic trait
    - Handle `get_cluster_info`, `add_search_attributes` (iterate attribute map, call upsert for each), `list_search_attributes`
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 6.3 Register all service modules in `grpc/mod.rs`
    - Add `pub mod workflow_service;`, `pub mod operator_service;`
    - _Requirements: 1.1, 2.1_

- [x] 7. Update `tokeirad` server bootstrap
  - [x] 7.1 Rewrite `apps/tokeirad/src/main.rs` to start the gRPC server
    - Construct `InMemoryStore`, `TokeiraRuntime`, `RuntimeAdapter`, a storage-backed `ExecutionResolver`, `EmptyVisibilityApi`, `InMemoryOperatorApi`, `EdgeInterceptors::permissive`, `LongPollGate`, `LocalOnlyRouter` directly in `main()`
    - Construct `WorkflowService`, `OperatorService` from the above
    - Wrap in `WorkflowServiceGrpc`, `OperatorServiceGrpc`
    - Register `FILE_DESCRIPTOR_SET` with `tonic_reflection`
    - Bind `tonic::Server` to `[::1]:7233`, log bound address at info level
    - Return descriptive error if bind fails
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.1, 8.2, 8.3_

- [x] 8. Checkpoint — Ensure full stack compiles and server boots
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Property-based tests
  - [x]* 9.1 Write property test: Proto-to-edge DTO round-trip
    - **Property 1: Proto-to-edge DTO round-trip for in-scope phase-1 fields**
    - Generate arbitrary edge DTOs (StartWorkflowExecutionRequest, StartWorkflowExecutionResponse, SignalWorkflowExecutionRequest, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse, etc.), convert to proto and back, assert equality for the fields represented by the initial transport
    - Exclude fields intentionally deferred in this milestone, such as poll-response history payloads
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 6.1, 6.2, 6.3, 6.4, 6.5, 6.8**
  - [x]* 9.2 Write property test: WorkflowCommand round-trip
    - **Property 2: WorkflowCommand round-trip**
    - Generate arbitrary `WorkflowCommand` variants (ScheduleActivity, StartTimer, CompleteWorkflow, FailWorkflow, UpsertSearchAttributes, UpsertMemo), convert to proto Command and back, assert equality
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 6.5, 6.6**
  - [x]* 9.3 Write property test: EdgeError to gRPC status code mapping
    - **Property 3: EdgeError to gRPC status code mapping**
    - Generate arbitrary `EdgeError` variants with random message strings, convert to `tonic::Status`, assert correct status code and message preservation
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8**
  - [x]* 9.4 Write property test: gRPC metadata to HeaderMap preservation
    - **Property 4: gRPC metadata to HeaderMap preservation**
    - Generate arbitrary sets of ASCII key-value pairs, insert into `MetadataMap`, convert via `metadata_to_header_map`, assert all pairs present in resulting HeaderMap
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 4.1, 4.2, 4.3**

- [x] 10. Unit tests
  - [x]* 10.1 Write unit tests for proto-to-edge translation
    - Test empty poll response: edge returns `None` → adapter returns default empty proto response
    - Test command with no attributes: proto `Command` with `attributes: None` → `ProtoConversionError::MissingField`
    - Test default poll timeout: adapter applies 60s timeout and 30s sticky TTL defaults
    - _Requirements: 6.3, 6.7, 7.2_
  - [x]* 10.2 Write unit tests for error mapping and metadata extraction
    - Test specific metadata keys: `x-request-id` and `authorization` preserved through metadata extraction
    - Test each EdgeError variant maps to the correct gRPC status code
    - _Requirements: 3.1–3.8, 4.2, 4.3_
  - [x]* 10.3 Write unit tests for operator service adapter
    - Test ClusterInfo response mapping (cluster_name, server_version fields)
    - Test AddSearchAttributes with multiple attributes results in multiple upsert calls
    - _Requirements: 2.1, 2.2, 2.3_

- [x] 11. Integration test
  - [x]* 11.1 Write integration test for full gRPC round-trip
    - Boot full server stack with `InMemoryStore` (inline construction, same as tokeirad)
    - Connect a tonic client
    - Call `StartWorkflowExecution` and verify a `run_id` is returned
    - Call `DescribeWorkflowExecution` and verify the workflow exists via the storage-backed resolver path
    - Verify gRPC reflection lists available services
    - _Requirements: 1.1, 1.5, 5.1, 5.4, 8.3_

- [x] 12. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Extend proto definitions for new endpoints
  - [x] 13.1 Add activity and advanced workflow RPC definitions to `service.proto`
    - Add `PollActivityTaskQueue`, `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `RecordActivityTaskHeartbeat`, `TerminateWorkflowExecution`, `RequestCancelWorkflowExecution`, `QueryWorkflow`, `UpdateWorkflowExecution` RPCs to the `WorkflowService` service block in `crates/tokeira-proto/proto/upstream/temporal/api/workflowservice/v1/service.proto`
    - _Requirements: 12.14_
  - [x] 13.2 Add proto message types for activity endpoints
    - Add `PollActivityTaskQueueRequest`, `PollActivityTaskQueueResponse`, `RespondActivityTaskCompletedRequest`, `RespondActivityTaskCompletedResponse`, `RespondActivityTaskFailedRequest`, `RespondActivityTaskFailedResponse`, `RecordActivityTaskHeartbeatRequest`, `RecordActivityTaskHeartbeatResponse` messages
    - `PollActivityTaskQueueResponse` includes `task_token` bytes, `activity_id`, `activity_type`, `input` Payloads, `attempt` int32, optional timeout fields, and `workflow_execution`
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_
  - [x] 13.3 Add proto message types for advanced workflow endpoints
    - Add `TerminateWorkflowExecutionRequest/Response`, `RequestCancelWorkflowExecutionRequest/Response`, `QueryWorkflowRequest`, `QueryWorkflowResponse` (with `QueryRejected`), `UpdateWorkflowExecutionRequest`, `UpdateWorkflowExecutionResponse` messages
    - _Requirements: 12.7, 12.8, 12.9, 12.10, 12.11, 12.12_

- [x] 14. Add new edge DTOs and `to_internal`/`from_internal` helpers
  - [x] 14.1 Add activity edge DTOs to `crates/tokeira-edge/src/translate/mod.rs`
    - Add `PollActivityTaskQueueRequest`, `PollActivityTaskQueueResponse`, `RespondActivityTaskCompletedRequest`, `RespondActivityTaskCompletedResponse`, `RespondActivityTaskFailedRequest`, `RespondActivityTaskFailedResponse`, `RecordActivityTaskHeartbeatRequest`, `RecordActivityTaskHeartbeatResponse`
    - Follow existing DTO patterns (derive `Clone, Debug, PartialEq`)
    - _Requirements: 9.1, 9.4, 9.5, 9.6_
  - [x] 14.2 Add advanced workflow edge DTOs to `crates/tokeira-edge/src/translate/mod.rs`
    - Add `TerminateWorkflowExecutionRequest/Response`, `RequestCancelWorkflowExecutionRequest/Response`, `QueryWorkflowRequest`, `QueryWorkflowResponse`, `UpdateWorkflowExecutionRequest`, `UpdateWorkflowExecutionResponse`, `UpdateOutcomeDto`, `UpdateWaitPolicy`
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  - [x] 14.3 Add `to_internal` conversion functions for new endpoints
    - Add `poll_activity_request`, `terminate_request`, `cancel_request` to `crates/tokeira-edge/src/translate/to_internal.rs`
    - Follow existing patterns (e.g., `poll_request`, `signal_request`)
    - _Requirements: 11.9_
  - [x] 14.4 Add `from_internal` conversion functions for new endpoints
    - Add `poll_activity_response`, `terminate_response`, `cancel_response`, `query_response`, `update_response` to `crates/tokeira-edge/src/translate/from_internal.rs`
    - Follow existing patterns (e.g., `poll_response`, `signal_response`)
    - _Requirements: 11.9_

- [x] 15. Add new `Action` variants to interceptors
  - Add `PollActivityTaskQueue`, `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `RecordActivityTaskHeartbeat`, `TerminateWorkflowExecution`, `RequestCancelWorkflowExecution`, `QueryWorkflow`, `UpdateWorkflowExecution` to the `Action` enum in `crates/tokeira-edge/src/interceptors.rs`
  - Add corresponding `as_str()` match arms
  - _Requirements: 9.1, 9.4, 9.5, 9.6, 10.1, 10.2, 10.3, 10.4_

- [x] 16. Extend `WorkflowRuntimeApi` trait and `RuntimeAdapter`
  - [x] 16.1 Add 8 new methods to `WorkflowRuntimeApi` trait in `crates/tokeira-edge/src/workflow_service.rs`
    - `poll_activity_task(queue, worker_identity, timeout) -> Result<Option<StartedActivityTask>>`
    - `complete_activity_task(token, result) -> Result<WorkflowMutationOutcome>`
    - `fail_activity_task(token, failure_message, failure_error_type) -> Result<()>`
    - `record_activity_heartbeat(token) -> Result<bool>`
    - `terminate_workflow(execution, req) -> Result<WorkflowMutationOutcome>`
    - `cancel_workflow(execution, req) -> Result<WorkflowMutationOutcome>`
    - `query_workflow(execution, query_type, query_args, timeout) -> Result<QueryResult>`
    - `update_workflow(execution, update_id, update_name, input, request, timeout, wait_policy) -> Result<UpdateOutcome>`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8_
  - [x] 16.2 Add `cancel_workflow` method to `TokeiraRuntime` in `crates/tokeira-runtime/src/runtime.rs`
    - The kernel supports `Command::Cancel(CancelRequest)` but the runtime doesn't expose it yet
    - Follow the same pattern as `terminate_workflow`: resolve execution → submit `Command::Cancel`
    - _Requirements: 11.6_
  - [x] 16.3 Implement new `WorkflowRuntimeApi` methods on `RuntimeAdapter` in `crates/tokeira-edge/src/grpc/runtime_adapter.rs`
    - Delegate each method to the corresponding `TokeiraRuntime` method
    - Apply `commit_result_to_outcome` for methods returning `CommitResult`
    - Apply `execution_for_run` resolution for methods requiring `ExecutionRef`
    - _Requirements: 11.9_

- [x] 17. Add new edge service methods on `WorkflowService`
  - [x] 17.1 Add activity endpoint methods to `WorkflowService` in `crates/tokeira-edge/src/workflow_service.rs`
    - `poll_activity_task_queue`: interceptors → route_task_queue → long_poll acquire → runtime.poll_activity_task → from_internal conversion; return `None` on timeout
    - `respond_activity_task_completed`: interceptors → runtime.complete_activity_task
    - `respond_activity_task_failed`: interceptors → runtime.fail_activity_task
    - `record_activity_task_heartbeat`: interceptors → runtime.record_activity_heartbeat
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_
  - [x] 17.2 Add advanced workflow endpoint methods to `WorkflowService`
    - `terminate_workflow_execution`: interceptors → route_workflow → resolve_execution → runtime.terminate_workflow
    - `request_cancel_workflow_execution`: interceptors → route_workflow → resolve_execution → runtime.cancel_workflow
    - `query_workflow`: interceptors → route_workflow → resolve_execution → runtime.query_workflow
    - `update_workflow_execution`: interceptors → route_workflow → resolve_execution → runtime.update_workflow
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  - [x] 17.3 Add `resolve_execution` helper method to `WorkflowService`
    - Resolve `(namespace, workflow_id)` → `ExecutionRef` via `self.resolver.current_run_key`
    - Return `EdgeError::WorkflowNotFound` if not found
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

- [x] 18. Checkpoint — Ensure edge layer compiles with new methods
  - Ensure all tests pass, ask the user if questions arise.

- [x] 19. Add proto-to-edge translation for new endpoints
  - [x] 19.1 Add `ActivityTaskToken` serialization/deserialization to `crates/tokeira-edge/src/grpc/translate.rs`
    - `serialize_activity_token(token) -> Vec<u8>` using `serde_json::to_vec`
    - `deserialize_activity_token(bytes) -> Result<ActivityTaskToken, ProtoConversionError>`
    - Add `ProtoConversionError::InvalidTaskToken(String)` variant
    - _Requirements: 12.3, 12.4, 12.5, 12.13_
  - [x] 19.2 Add activity endpoint translation functions to `crates/tokeira-edge/src/grpc/translate.rs`
    - `poll_activity_request_to_edge`: extract task_queue, identity, apply 60s default timeout
    - `poll_activity_response_to_proto`: serialize task token, map activity fields, timeouts via `duration_to_proto`
    - `respond_activity_completed_to_edge`: deserialize task token, extract result payloads
    - `respond_activity_completed_to_proto`: empty response
    - `respond_activity_failed_to_edge`: deserialize task token, extract failure message/type
    - `respond_activity_failed_to_proto`: empty response
    - `record_heartbeat_to_edge`: deserialize task token
    - `record_heartbeat_to_proto`: map `cancel_requested` boolean
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_
  - [x] 19.3 Add advanced workflow endpoint translation functions to `crates/tokeira-edge/src/grpc/translate.rs`
    - `terminate_request_to_edge`: extract namespace, workflow_id, reason, details, identity
    - `terminate_response_to_proto`: empty response
    - `cancel_request_to_edge`: extract namespace, workflow_id, reason
    - `cancel_response_to_proto`: empty response
    - `query_request_to_edge`: extract namespace, workflow_id, query_type, query_args, apply 10s default timeout
    - `query_response_to_proto`: map result payloads and query_rejected
    - `update_request_to_edge`: extract namespace, workflow_id, update_id, update_name, input, wait_policy enum mapping, apply 30s default timeout
    - `update_response_to_proto`: map outcome (accepted/completed/rejected)
    - _Requirements: 12.7, 12.8, 12.9, 12.10, 12.11, 12.12_

- [x] 20. Add gRPC adapter methods on `WorkflowServiceGrpc` for new endpoints
  - [x] 20.1 Add activity endpoint adapter methods to `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - `poll_activity_task_queue`: extract metadata → translate → call edge → return default empty response on `None`
    - `respond_activity_task_completed`: extract metadata → translate (deserialize token) → call edge → translate response
    - `respond_activity_task_failed`: extract metadata → translate (deserialize token) → call edge → translate response
    - `record_activity_task_heartbeat`: extract metadata → translate (deserialize token) → call edge → translate response
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_
  - [x] 20.2 Add advanced workflow endpoint adapter methods to `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - `terminate_workflow_execution`: extract metadata → translate → call edge → translate response
    - `request_cancel_workflow_execution`: extract metadata → translate → call edge → translate response
    - `query_workflow`: extract metadata → translate → call edge → translate response
    - `update_workflow_execution`: extract metadata → translate → call edge → translate response
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

- [x] 21. Checkpoint — Ensure full stack compiles with new endpoints
  - Ensure all tests pass, ask the user if questions arise.

- [x] 22. Property-based tests for new endpoints
  - [x]* 22.1 Write property test: Proto-to-edge DTO round-trip for new endpoints
    - **Property 5: Proto-to-edge DTO round-trip for new endpoints**
    - Generate arbitrary edge DTOs for all new endpoint types (PollActivityTaskQueueRequest/Response, RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest, RecordActivityTaskHeartbeatRequest/Response, TerminateWorkflowExecutionRequest, RequestCancelWorkflowExecutionRequest, QueryWorkflowRequest/Response, UpdateWorkflowExecutionRequest/Response)
    - Convert to proto and back, assert equality; exclude deferred fields (activity heartbeat details payloads)
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 12.10, 12.11, 12.12, 12.15**
  - [x]* 22.2 Write property test: ActivityTaskToken serialization round-trip
    - **Property 6: ActivityTaskToken serialization round-trip**
    - Generate arbitrary `ActivityTaskToken` values (arbitrary `RunKey`, `String` activity_id, `i64` schedule_event_id, `u32` attempt, `ShardEpoch`)
    - Serialize to bytes via `serde_json::to_vec`, deserialize back via `serde_json::from_slice`, assert equality
    - Also test full path: serialize token → embed in proto `RespondActivityTaskCompletedRequest` → translate to edge DTO → verify token matches
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 12.3, 12.4, 12.5, 12.13**

- [x] 23. Unit tests for new endpoints
  - [x]* 23.1 Write unit tests for activity endpoint translation
    - Test empty activity poll response: edge returns `None` → adapter returns default empty `PollActivityTaskQueueResponse`
    - Test invalid task token bytes: corrupt bytes in `RespondActivityTaskCompleted.task_token` → `ProtoConversionError::InvalidTaskToken`
    - Test empty task token bytes → `ProtoConversionError::InvalidTaskToken`
    - Test heartbeat `cancel_requested` propagation: runtime returns `true` → proto response has `cancel_requested = true`
    - Test default activity poll timeout: adapter applies 60s timeout default
    - _Requirements: 9.1, 9.3, 9.6, 12.1, 12.6, 12.13_
  - [x]* 23.2 Write unit tests for advanced workflow endpoint translation
    - Test terminate with details payloads correctly translates through proto→edge path
    - Test cancel with empty reason string is handled correctly
    - Test update wait policy mapping: proto values 0 → `Accepted`, 1 → `Completed`
    - Test default query timeout: adapter applies 10s default
    - Test default update timeout: adapter applies 30s default
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 12.7, 12.8, 12.9, 12.11_
  - [x]* 23.3 Write unit test for activity poll sharing LongPollGate
    - Verify activity polls and workflow polls compete for the same semaphore by exhausting the gate with workflow polls and verifying activity polls are rejected with `RESOURCE_EXHAUSTED`
    - _Requirements: 9.2_

- [x] 24. Integration tests for new endpoints
  - [x]* 24.1 Extend integration test for activity lifecycle
    - Start a workflow with a `ScheduleActivity` command
    - Poll for an activity task via `PollActivityTaskQueue`, verify activity ID, input, and task token
    - Complete the activity via `RespondActivityTaskCompleted` with a result payload
    - Verify the workflow progresses via `DescribeWorkflowExecution`
    - _Requirements: 9.1, 9.4, 12.1, 12.2, 12.3_
  - [x]* 24.2 Extend integration test for terminate and cancel
    - Terminate a workflow via `TerminateWorkflowExecution`, verify it's terminated via `DescribeWorkflowExecution`
    - Cancel a workflow via `RequestCancelWorkflowExecution`, verify the cancellation is recorded
    - _Requirements: 10.1, 10.2_

- [x] 25. Final checkpoint — Ensure all new endpoint tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 26. Preserve lifecycle request links (Tier 5.31 correction)
  - [x] 26.1 Extend terminate and request-cancel edge DTOs and translators
    - Preserve every validated request link in order and map it to the existing kernel `Link` model.
    - Keep the shared v1.31.0 count, size, variant, and identity-field validation ahead of runtime invocation.
    - _Requirements: 10.5, 10.6, 10.7, 12.7, 12.8, 12.16_
  - [x] 26.2 Extend the pure kernel request and history variants
    - Carry lifecycle links as deterministic data on `CancelRequest`, `TerminateRequest`, `WorkflowExecutionCancelRequested`, and `WorkflowExecutionTerminated`.
    - Preserve empty-link behavior for internal cancellation and termination call sites.
    - _Requirements: 10.5, 10.6_
  - [x] 26.3 Project lifecycle links on outer history events
    - Extend the history serializer's common event-link projection for cancel-requested and terminated events.
    - _Requirements: 10.5, 10.6_
  - [x] 26.4 Required property test: lifecycle link preservation
    - **Property 7: Lifecycle link preservation**
    - Generate at least 100 valid ordered link lists and prove edge-to-kernel-to-history-to-proto equality for both operations, plus preservation for empty lists.
    - Tag the property `// Feature: grpc-edge-transport, Property 7`.
    - _Requirements: 10.1, 10.2, 10.5, 10.6, 12.7, 12.8, 12.15_
  - [x] 26.5 Functional conformance checkpoint
    - Run focused kernel and edge tests, then two clean consecutive `TestLinksTestSuite` runs against the final binary.
    - _Requirements: 10.5, 10.6, 10.7_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- The implementation language is Rust throughout, matching the existing codebase
- All crate paths are relative to the `tokeira/` workspace root
- Tasks 1–12 cover Requirements 1–8 (initial gRPC transport milestone, all completed)
- Tasks 13–25 cover Requirements 9–12 (activity endpoints, advanced workflow endpoints, extended trait, proto translation)
