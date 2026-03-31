# Implementation Plan: gRPC Edge Transport

## Overview

Wire the existing `tokeira-edge` service layer and `tokeira-proto` generated bindings into a Temporal-compatible tonic gRPC server. The implementation follows a bottom-up approach: dependency setup → translation layer → error mapping → metadata extraction → runtime adapter → gRPC service adapters → server bootstrap → tests.

## Tasks

- [ ] 1. Add workspace and crate dependencies
  - [ ] 1.1 Update workspace `Cargo.toml` with tonic and tonic-reflection
    - Add `tonic = { version = "0.11", features = ["transport"] }` and `tonic-reflection = "0.11"` to `[workspace.dependencies]`
    - _Requirements: 5.1, 5.4_
  - [ ] 1.2 Update `tokeira-edge/Cargo.toml` with tokeira-proto and tonic
    - Add `tokeira-proto = { path = "../tokeira-proto" }` and `tonic = { version = "0.11", features = ["transport"] }` to dependencies
    - _Requirements: 1.1, 3.1_
  - [ ] 1.3 Update `tokeirad/Cargo.toml` with tokeira-edge, tokeira-proto, tonic, and tonic-reflection
    - Add `tokeira-edge`, `tokeira-proto`, `tonic`, and `tonic-reflection` to dependencies
    - _Requirements: 5.1, 5.4, 8.3_

- [ ] 2. Implement proto-to-edge translation layer (`tokeira-edge::grpc::translate`)
  - [ ] 2.1 Create `crates/tokeira-edge/src/grpc/translate.rs` with proto→edge request converters
    - Implement `start_request_to_edge`, `signal_request_to_edge`, `poll_request_to_edge`, `respond_completed_request_to_edge`, `describe_request_to_edge`, `list_request_to_edge`, `count_request_to_edge`
    - Implement or colocate the payload, memo, search_attributes, and task_queue conversion helpers inside `tokeira-edge::grpc::translate` rather than depending on a `tokeira_proto::conversions` module
    - Apply default timeout (60s) and sticky_ttl (30s) for poll requests
    - _Requirements: 6.1, 6.3, 6.5, 6.6, 6.7_
  - [ ] 2.2 Add edge→proto response converters to the same module
    - Implement `start_response_to_proto`, `poll_response_to_proto`, `signal_response_to_proto`, `completed_response_to_proto`, `describe_response_to_proto`, `list_response_to_proto`, `count_response_to_proto`
    - Poll response populates task token bytes, workflow execution identity, started event ID, and attempt from existing `StartedWorkflowTask` fields; history is empty (deferred)
    - _Requirements: 6.2, 6.4, 6.8_
  - [ ] 2.3 Add `WorkflowCommand` ↔ proto `Command` translation
    - Implement `proto_command_to_workflow_command` and `workflow_command_to_proto`
    - Handle `schedule_activity`, `start_timer`, `complete_workflow`, `fail_workflow`, `upsert_search_attributes`, `upsert_memo` variants
    - Return `ProtoConversionError::MissingField("Command.attributes")` when no recognized variant is set
    - _Requirements: 6.5, 6.6, 6.7_
  - [ ] 2.4 Register the `translate` module in `grpc/mod.rs`
    - Add `pub mod translate;` to `crates/tokeira-edge/src/grpc/mod.rs`
    - _Requirements: 6.1_

- [ ] 3. Implement gRPC metadata extraction and error mapping (`tokeira-edge::grpc`)
  - [ ] 3.1 Create `crates/tokeira-edge/src/grpc/metadata.rs`
    - Implement `metadata_to_header_map(metadata: &tonic::metadata::MetadataMap) -> http::HeaderMap`
    - Iterate metadata entries and insert into a fresh HeaderMap, preserving `x-request-id`, `authorization`, and all custom headers
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ] 3.2 Create `crates/tokeira-edge/src/grpc/errors.rs`
    - Implement `From<EdgeError> for tonic::Status` with the mapping: BadRequest→INVALID_ARGUMENT, Unauthorized→UNAUTHENTICATED, Forbidden→PERMISSION_DENIED, NamespaceNotFound/WorkflowNotFound→NOT_FOUND, NamespaceDeleted→FAILED_PRECONDITION, TooManyLongPolls→RESOURCE_EXHAUSTED, LongPollAdmissionTimeout→DEADLINE_EXCEEDED, RemoteRouteUnsupported→UNAVAILABLE, Internal→INTERNAL
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_
  - [ ] 3.3 Create `crates/tokeira-edge/src/grpc/mod.rs`
    - Declare `pub mod metadata;`, `pub mod errors;`, `pub mod translate;` (and later `workflow_service`, `operator_service`, `runtime_adapter`)
    - Register `pub mod grpc;` in `crates/tokeira-edge/src/lib.rs`
    - _Requirements: 1.1, 3.1_

- [ ] 4. Checkpoint — Ensure dependency and foundation modules compile
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement RuntimeAdapter (`tokeira-edge::grpc::runtime_adapter`)
  - [ ] 5.1 Create `crates/tokeira-edge/src/grpc/runtime_adapter.rs`
    - Implement `RuntimeAdapter<R>` struct wrapping `Arc<TokeiraRuntime<R>>`
    - Implement `WorkflowRuntimeApi` for `RuntimeAdapter<R>` delegating `start_workflow`, `signal_workflow`, `poll_workflow_task`, `complete_workflow_task` to the runtime
    - Implement `commit_result_to_outcome` helper converting `CommitResult` → `WorkflowMutationOutcome` (using `new_state.last_event_id`, not `history_length` which does not exist)
    - _Requirements: 8.1, 8.2_
  - [ ] 5.2 Register `runtime_adapter` in `grpc/mod.rs`
    - Add `pub mod runtime_adapter;`
    - _Requirements: 8.1_

- [ ] 6. Implement gRPC service adapters
  - [ ] 6.1 Create `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Implement `WorkflowServiceGrpc` struct wrapping `WorkflowService`
    - Implement `workflowservice::workflow_service_server::WorkflowService` tonic trait
    - Each method: extract metadata→HeaderMap, convert proto→edge DTO, call edge service, convert result→proto response or EdgeError→tonic::Status
    - For `poll_workflow_task_queue`: return default empty proto response when edge returns `None`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 7.1, 7.2, 7.3_
  - [ ] 6.2 Create `crates/tokeira-edge/src/grpc/operator_service.rs`
    - Implement `OperatorServiceGrpc` struct wrapping `OperatorService`
    - Implement `operatorservice::operator_service_server::OperatorService` tonic trait
    - Handle `get_cluster_info`, `add_search_attributes` (iterate attribute map, call upsert for each), `list_search_attributes`
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 6.3 Register all service modules in `grpc/mod.rs`
    - Add `pub mod workflow_service;`, `pub mod operator_service;`
    - _Requirements: 1.1, 2.1_

- [ ] 7. Update `tokeirad` server bootstrap
  - [ ] 7.1 Rewrite `apps/tokeirad/src/main.rs` to start the gRPC server
    - Construct `InMemoryStore`, `TokeiraRuntime`, `RuntimeAdapter`, a storage-backed `ExecutionResolver`, `EmptyVisibilityApi`, `InMemoryOperatorApi`, `EdgeInterceptors::permissive`, `LongPollGate`, `LocalOnlyRouter` directly in `main()`
    - Construct `WorkflowService`, `OperatorService` from the above
    - Wrap in `WorkflowServiceGrpc`, `OperatorServiceGrpc`
    - Register `FILE_DESCRIPTOR_SET` with `tonic_reflection`
    - Bind `tonic::Server` to `[::1]:7233`, log bound address at info level
    - Return descriptive error if bind fails
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.1, 8.2, 8.3_

- [ ] 8. Checkpoint — Ensure full stack compiles and server boots
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Property-based tests
  - [ ]* 9.1 Write property test: Proto-to-edge DTO round-trip
    - **Property 1: Proto-to-edge DTO round-trip for in-scope phase-1 fields**
    - Generate arbitrary edge DTOs (StartWorkflowExecutionRequest, StartWorkflowExecutionResponse, SignalWorkflowExecutionRequest, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse, etc.), convert to proto and back, assert equality for the fields represented by the initial transport
    - Exclude fields intentionally deferred in this milestone, such as poll-response history payloads
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 6.1, 6.2, 6.3, 6.4, 6.5, 6.8**
  - [ ]* 9.2 Write property test: WorkflowCommand round-trip
    - **Property 2: WorkflowCommand round-trip**
    - Generate arbitrary `WorkflowCommand` variants (ScheduleActivity, StartTimer, CompleteWorkflow, FailWorkflow, UpsertSearchAttributes, UpsertMemo), convert to proto Command and back, assert equality
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 6.5, 6.6**
  - [ ]* 9.3 Write property test: EdgeError to gRPC status code mapping
    - **Property 3: EdgeError to gRPC status code mapping**
    - Generate arbitrary `EdgeError` variants with random message strings, convert to `tonic::Status`, assert correct status code and message preservation
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8**
  - [ ]* 9.4 Write property test: gRPC metadata to HeaderMap preservation
    - **Property 4: gRPC metadata to HeaderMap preservation**
    - Generate arbitrary sets of ASCII key-value pairs, insert into `MetadataMap`, convert via `metadata_to_header_map`, assert all pairs present in resulting HeaderMap
    - Use `proptest` with minimum 100 cases
    - **Validates: Requirements 4.1, 4.2, 4.3**

- [ ] 10. Unit tests
  - [ ]* 10.1 Write unit tests for proto-to-edge translation
    - Test empty poll response: edge returns `None` → adapter returns default empty proto response
    - Test command with no attributes: proto `Command` with `attributes: None` → `ProtoConversionError::MissingField`
    - Test default poll timeout: adapter applies 60s timeout and 30s sticky TTL defaults
    - _Requirements: 6.3, 6.7, 7.2_
  - [ ]* 10.2 Write unit tests for error mapping and metadata extraction
    - Test specific metadata keys: `x-request-id` and `authorization` preserved through metadata extraction
    - Test each EdgeError variant maps to the correct gRPC status code
    - _Requirements: 3.1–3.8, 4.2, 4.3_
  - [ ]* 10.3 Write unit tests for operator service adapter
    - Test ClusterInfo response mapping (cluster_name, server_version fields)
    - Test AddSearchAttributes with multiple attributes results in multiple upsert calls
    - _Requirements: 2.1, 2.2, 2.3_

- [ ] 11. Integration test
  - [ ]* 11.1 Write integration test for full gRPC round-trip
    - Boot full server stack with `InMemoryStore` (inline construction, same as tokeirad)
    - Connect a tonic client
    - Call `StartWorkflowExecution` and verify a `run_id` is returned
    - Call `DescribeWorkflowExecution` and verify the workflow exists via the storage-backed resolver path
    - Verify gRPC reflection lists available services
    - _Requirements: 1.1, 1.5, 5.1, 5.4, 8.3_

- [ ] 12. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- The implementation language is Rust throughout, matching the existing codebase
- All crate paths are relative to the `tokeira/` workspace root
