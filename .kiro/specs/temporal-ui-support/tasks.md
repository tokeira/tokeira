# Implementation Plan: Temporal UI Support

## Overview

Implement the missing gRPC endpoints, namespace management, gRPC-Web transport, and visibility wiring so the Temporal UI can connect to tokeirad. Tasks are ordered by what unblocks the UI fastest: discovery endpoints first, then namespace management, then gRPC-Web transport, then visibility and workflow management endpoints.

All new endpoints follow the existing edge-layer pattern: thin gRPC handler → edge method with interceptors → runtime/storage/cache delegation. Proto ↔ edge DTO translation lives in `grpc/translate.rs`.

## Tasks

- [x] 1. Add `NamespaceAlreadyExists` error variant and extend `NamespaceCache` trait
  - [x] 1.1 Add `NamespaceAlreadyExists(String)` variant to `EdgeError` in `crates/tokeira-edge/src/errors.rs`
    - Add the variant, update `status_code()` to return `CONFLICT`, update `action_name()` to return `"namespace_already_exists"`
    - Add `NamespaceAlreadyExists → Status::already_exists` arm in `grpc/errors.rs`
    - _Requirements: 13.2_

  - [x] 1.2 Extend `NamespaceCache` trait with `list_all()` and `insert()` methods
    - Add `async fn list_all(&self) -> Result<Vec<ResolvedNamespace>>` to the `NamespaceCache` trait
    - Promote `insert` from inherent method on `InMemoryNamespaceCache` to trait method: `async fn insert(&self, ns: ResolvedNamespace) -> Result<()>`
    - Implement both on `InMemoryNamespaceCache` — `list_all` clones all values, `insert` writes to the inner map
    - Update `main.rs` call site to use the trait method (signature changes from `async fn insert(&self, ns)` to returning `Result<()>`)
    - _Requirements: 3.4, 13.1_

  - [ ]* 1.3 Write property tests for namespace cache round-trip (Property 1)
    - **Property 1: Namespace cache round-trip**
    - Generate random sets of `ResolvedNamespace`, insert all, verify `list_all()` returns every inserted namespace with matching fields
    - **Validates: Requirements 3.1, 3.2, 3.4**

  - [ ]* 1.4 Write property test for namespace lookup correctness (Property 2)
    - **Property 2: Namespace lookup correctness**
    - Generate random namespace names, insert some, verify `get()` returns correct result for present and absent names
    - **Validates: Requirements 4.1, 4.2**

- [ ] 2. Add new `Action` variants and edge DTOs for new endpoints
  - [ ] 2.1 Add new `Action` variants to the interceptors enum
    - Add `GetSystemInfo`, `GetClusterInfo`, `ListNamespaces`, `DescribeNamespace`, `RegisterNamespace`, `GetWorkflowExecutionHistoryReverse`, `DeleteWorkflowExecution`, `ResetWorkflowExecution`, `SignalWithStartWorkflowExecution`, `DescribeTaskQueue` to `Action` enum in `interceptors.rs`
    - Update `as_str()` match arms
    - _Requirements: 1.4, 2.4, 3.5, 4.3, 7.4, 8.4, 9.4, 10.4, 11.3, 12.3, 13.4_

  - [ ] 2.2 Add new edge DTOs in `translate/mod.rs`
    - Add `SystemInfo`, `SystemCapabilities`, `RegisterNamespaceRequest`, `DeleteWorkflowExecutionRequest`, `ResetWorkflowExecutionRequest`, `ResetWorkflowExecutionResponse`, `SignalWithStartWorkflowExecutionRequest`, `DescribeTaskQueueRequest`, `DescribeTaskQueueResponse`, `PollerInfo`, `GetWorkflowExecutionHistoryReverseRequest`, `GetWorkflowExecutionHistoryReverseResponse`
    - _Requirements: 1.1, 1.2, 8.1, 9.1, 10.1, 11.1, 12.1, 13.1_

- [ ] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement Tier 1 discovery endpoints — GetSystemInfo and GetClusterInfo
  - [x] 4.1 Add `operator_api` and `namespace_cache` fields to `WorkflowService` struct
    - Add `operator_api: Arc<dyn OperatorApi>` and `namespace_cache: Arc<dyn NamespaceCache>` fields to the `WorkflowService` struct in `workflow_service.rs`
    - Update `new()` and `new_with_history_wait_registry()` constructors to accept and store the new parameters
    - Update `main.rs` to pass `Arc::new(InMemoryOperatorApi::new("tokeira-local"))` and `namespaces.clone()` (before they are moved into `EdgeInterceptors`)
    - Update all test call sites in `grpc/workflow_service.rs` tests
    - _Requirements: 2.2_

  - [x] 4.2 Implement `get_system_info` edge method and gRPC handler
    - Add `get_system_info(&self, headers: &HeaderMap) -> EdgeResult<SystemInfo>` to `WorkflowService`
    - Return static `SystemCapabilities` with values from the design table (signal_and_query_header=true, internal_error_differentiation=true, etc.)
    - Add `system_info_to_proto` translation in `grpc/translate.rs`
    - Wire the gRPC handler in `grpc/workflow_service.rs` to replace the `Status::unimplemented` stub
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 4.3 Implement `get_cluster_info` edge method and gRPC handler
    - Add `get_cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo>` to `WorkflowService`
    - Delegate to `self.operator_api.cluster_info()`, run interceptors with `Action::GetClusterInfo`
    - Add `cluster_info_to_proto` translation in `grpc/translate.rs` — populate `cluster_name`, `server_version`, `cluster_id`, and empty `supported_clients` map
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x]* 4.4 Write unit tests for GetSystemInfo and GetClusterInfo
    - Test `GetSystemInfo` returns expected capabilities struct
    - Test `GetClusterInfo` delegates to `OperatorApi::cluster_info()` and returns correct fields
    - _Requirements: 1.1, 1.2, 2.1, 2.2_

- [x] 5. Implement Tier 1 namespace endpoints — ListNamespaces and DescribeNamespace
  - [x] 5.1 Implement `list_namespaces` edge method and gRPC handler
    - Add `list_namespaces(&self, headers: &HeaderMap) -> EdgeResult<Vec<ResolvedNamespace>>` to `WorkflowService`
    - Delegate to `self.namespace_cache.list_all()`, run interceptors with `Action::ListNamespaces`
    - Add `namespaces_to_proto` translation — populate each entry with `namespace_info` (name, state, description) and `namespace_config` (retention)
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 3.1, 3.2, 3.3, 3.5_

  - [x] 5.2 Implement `describe_namespace` edge method and gRPC handler
    - Add `describe_namespace(&self, headers: &HeaderMap, name: &str) -> EdgeResult<ResolvedNamespace>` to `WorkflowService`
    - Delegate to `self.namespace_cache.get(name)`, return `EdgeError::NamespaceNotFound` if absent
    - Add `namespace_to_proto` translation — populate `namespace_info`, `namespace_config`, `is_global_namespace`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 4.1, 4.2, 4.3_

  - [x]* 5.3 Write unit tests for ListNamespaces and DescribeNamespace
    - Test `ListNamespaces` with empty cache returns empty list
    - Test `DescribeNamespace` with non-existent name returns NOT_FOUND
    - _Requirements: 3.3, 4.2_

- [x] 6. Implement gRPC-Web and CORS transport layer
  - [x] 6.1 Add `tonic-web` and `tower-http` workspace dependencies
    - Add `tonic-web = "0.11"` and `tower-http = { version = "0.5", features = ["cors"] }` to `[workspace.dependencies]` in root `Cargo.toml`
    - Add both as dependencies in `apps/tokeirad/Cargo.toml`
    - _Requirements: 7.5_

  - [x] 6.2 Wire `CorsLayer` and `GrpcWebLayer` in `tokeirad/src/main.rs`
    - Add `tower_http::cors::CorsLayer::permissive()` and `tonic_web::GrpcWebLayer::new()` to the tonic `Server::builder()` layer stack
    - Order: CORS → gRPC-Web → tonic router
    - Both native gRPC (HTTP/2) and gRPC-Web (HTTP/1.1) must work on the same port — use `.accept_http1(true)` on the server builder
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ]* 6.3 Write integration tests for gRPC-Web and CORS
    - Test gRPC-Web request to `GetSystemInfo` returns valid response
    - Test CORS preflight returns expected headers
    - _Requirements: 7.1, 7.2_

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement Tier 2 — Visibility pipeline wiring
  - [x] 8.1 Add `delete_execution` to both `VisibilityStore` and `VisibilityApi` traits
    - Add `async fn delete_execution(&self, run_key: RunKey) -> Result<()>` to the `VisibilityStore` trait in `tokeira-projection/src/store.rs`
    - Implement on `InMemoryVisibilityStore` — remove the row and associated search attribute indexes
    - Add `async fn delete_execution(&self, run_key: RunKey) -> Result<()>` to the `VisibilityApi` trait in `tokeira-edge/src/workflow_service.rs`
    - Implement on `VisibilityQueryService` — delegate to the underlying `VisibilityStore`
    - This ensures `WorkflowService` can delete via its existing `Arc<dyn VisibilityApi>` without needing a separate store handle
    - _Requirements: 9.1_

  - [x] 8.2 Wire the `ProjectionWorker` as the authoritative visibility ingestion point in `tokeirad` startup
    - The `ProjectionWorker` (not `RuntimeAdapter`) must consume `ProjectionRecord`s from the storage layer's projection log and feed them to the `VisibilitySink`
    - This is the only path that sees ALL commits — including background scanner-driven commits, activity timeout commits, and timer-fired commits
    - Start the `ProjectionWorker` as a background tokio task in `main.rs`
    - Verify `ListWorkflowExecutions` returns real data after starting and completing a workflow
    - _Requirements: 5.4_

  - [ ]* 8.3 Write property test for delete removes from visibility (Property 5)
    - **Property 5: Delete removes from visibility**
    - Generate random visibility rows, insert, delete one, verify it no longer appears in list results
    - **Validates: Requirements 9.1**

- [x] 9. Implement Tier 2 — DescribeWorkflowExecution completeness
  - [x] 9.1 Enhance `describe_response_to_proto` translation for complete field coverage
    - Ensure `workflow_execution_info` in the proto response includes all available fields: execution, type, task_queue, start_time, close_time, status, history_length, memo, search_attributes, state_transition_count
    - Add `pending_activities` population if data is available from the runtime
    - _Requirements: 6.1, 6.2_

  - [x]* 9.2 Write property test for describe workflow field preservation (Property 3)
    - **Property 3: Describe workflow execution field preservation**
    - Generate random `WorkflowExecutionDescription` values, translate to proto, verify field preservation for workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, history_length, state_transition_count
    - **Validates: Requirements 6.1**

- [x] 10. Implement Tier 2 — GetWorkflowExecutionHistoryReverse
  - [x] 10.1 Implement `get_workflow_execution_history_reverse` edge method
    - Add the edge method to `WorkflowService` — load history from repo, reverse the event order, support pagination via `next_page_token`
    - Reverse pagination token encodes `before_event_id` as big-endian i64 bytes (distinct from forward token which encodes `after_event_id`)
    - First request (empty token): start from the last event. Subsequent requests: return events with `event_id < before_event_id`. Empty token in response: no more events.
    - Return `EdgeError::WorkflowNotFound` if the execution does not exist
    - Run interceptors with `Action::GetWorkflowExecutionHistoryReverse`
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [x] 10.2 Add proto translation and wire gRPC handler for reverse history
    - Add `reverse_history_request_to_edge` and `reverse_history_response_to_proto` in `grpc/translate.rs`
    - Wire the gRPC handler in `grpc/workflow_service.rs` to replace the `Status::unimplemented` stub
    - _Requirements: 8.1, 8.2_

  - [x]* 10.3 Write property test for reverse history ordering (Property 4)
    - **Property 4: Reverse history ordering**
    - Generate random history event sequences, reverse, verify strictly descending event_id order and pagination completeness
    - **Validates: Requirements 8.1, 8.2**

- [ ] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Implement Tier 3 — RegisterNamespace
  - [x] 12.1 Implement `register_namespace` edge method and gRPC handler
    - Add `register_namespace(&self, headers: &HeaderMap, req: RegisterNamespaceRequest) -> EdgeResult<()>` to `WorkflowService`
    - Validate namespace name: non-empty, matches `^[a-zA-Z0-9_-]+$`, return `EdgeError::BadRequest` otherwise
    - Check if namespace already exists via `namespace_cache.get()`, return `EdgeError::NamespaceAlreadyExists` if so
    - Insert via `namespace_cache.insert()`
    - Add `register_namespace_to_edge` translation in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 13.1, 13.2, 13.3, 13.4_

  - [ ]* 12.2 Write property test for register namespace round-trip with duplicate detection (Property 8)
    - **Property 8: Register namespace round-trip with duplicate detection**
    - Generate random valid namespace names, register, verify round-trip via `describe_namespace`, verify duplicate returns ALREADY_EXISTS
    - **Validates: Requirements 13.1, 13.2**

  - [ ]* 12.3 Write property test for namespace name validation (Property 9)
    - **Property 9: Namespace name validation**
    - Generate random strings (valid and invalid), verify `register_namespace` accepts iff non-empty and matches `^[a-zA-Z0-9_-]+$`
    - **Validates: Requirements 13.3**

  - [x]* 12.4 Write unit tests for RegisterNamespace edge cases
    - Test empty name returns INVALID_ARGUMENT
    - Test special characters return INVALID_ARGUMENT
    - _Requirements: 13.3_

- [x] 13. Implement Tier 3 — DeleteWorkflowExecution
  - [x] 13.1 Implement `delete_workflow_execution` edge method and gRPC handler
    - Add `delete_workflow_execution(&self, headers: &HeaderMap, req: DeleteWorkflowExecutionRequest) -> EdgeResult<()>` to `WorkflowService`
    - Resolve the workflow execution, return `EdgeError::WorkflowNotFound` if absent
    - If the workflow is running, terminate it first via `runtime.terminate_workflow()`
    - Delete from visibility store via `delete_execution()`
    - Add `delete_request_to_edge` translation in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

  - [x]* 13.2 Write unit tests for DeleteWorkflowExecution
    - Test delete of non-existent workflow returns NOT_FOUND
    - _Requirements: 9.3_

- [ ] 14. Implement Tier 3 — ResetWorkflowExecution
  - [ ] 14.1 Implement `reset_workflow_execution` edge method and gRPC handler
    - Add `reset_workflow_execution(&self, headers: &HeaderMap, req: ResetWorkflowExecutionRequest) -> EdgeResult<ResetWorkflowExecutionResponse>` to `WorkflowService`
    - Resolve the workflow, return `EdgeError::WorkflowNotFound` if absent
    - Validate the `workflow_task_finish_event_id` references a `WORKFLOW_TASK_COMPLETED` event, return `EdgeError::BadRequest` otherwise
    - Create a new run replaying history up to the specified event, return the new `run_id`
    - Add `reset_request_to_edge` and `reset_response_to_proto` translations in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [ ]* 14.2 Write unit tests for ResetWorkflowExecution
    - Test reset of non-existent workflow returns NOT_FOUND
    - Test reset with non-WFT-completed event returns INVALID_ARGUMENT
    - _Requirements: 10.2, 10.3_

- [x] 15. Implement Tier 3 — SignalWithStartWorkflowExecution
  - [x] 15.1 Implement `signal_with_start_workflow_execution` edge method and gRPC handler
    - Add `signal_with_start_workflow_execution(&self, headers: &HeaderMap, req: SignalWithStartWorkflowExecutionRequest) -> EdgeResult<StartWorkflowExecutionResponse>` to `WorkflowService`
    - If target workflow is running: deliver signal, return existing `run_id`
    - If target workflow does not exist: start new execution with signal as first event after start, return new `run_id`
    - Add `signal_with_start_request_to_edge` and `signal_with_start_response_to_proto` translations in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 12.1, 12.2, 12.3_

  - [ ]* 15.2 Write property test for signal-with-start conditional behavior (Property 7)
    - **Property 7: Signal-with-start conditional behavior**
    - Generate random signal-with-start scenarios (workflow exists vs. not), verify correct branch taken and correct run_id returned
    - **Validates: Requirements 12.1, 12.2**

- [x] 16. Implement Tier 3 — DescribeTaskQueue
  - [x] 16.1 Create `PollerRegistry` in `tokeira-edge`
    - Implement `PollerRegistry` with `register()` (returns RAII `PollerGuard`) and `pollers()` methods
    - `PollerGuard` removes the poller entry on drop when the poll request completes
    - Store `HashMap<QueueKey, Vec<ActivePoller>>` with identity and registered_at timestamp
    - Add `poller_registry: PollerRegistry` field to `WorkflowService`
    - _Requirements: 11.1, 11.2_

  - [x] 16.2 Wire `PollerRegistry` into poll handlers
    - In `PollWorkflowTaskQueue` and `PollActivityTaskQueue` gRPC handlers, call `poller_registry.register()` at entry and hold the guard for the duration of the poll
    - Create `PollerRegistry` in `main.rs` and pass to `WorkflowService`
    - _Requirements: 11.1_

  - [x] 16.3 Implement `describe_task_queue` edge method and gRPC handler
    - Add `describe_task_queue(&self, headers: &HeaderMap, req: DescribeTaskQueueRequest) -> EdgeResult<DescribeTaskQueueResponse>` to `WorkflowService`
    - Delegate to `self.poller_registry.pollers()` for the specified task queue
    - Return empty pollers list when no workers are polling
    - Add `describe_task_queue_request_to_edge` and `describe_task_queue_response_to_proto` translations in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 11.1, 11.2, 11.3_

  - [ ]* 16.2 Write property test for describe task queue lists all pollers (Property 6)
    - **Property 6: Describe task queue lists all pollers**
    - Generate random poller sets, register them, verify `describe_task_queue` returns all with matching identity
    - **Validates: Requirements 11.1**

- [x] 17. Update public exports in `tokeira-edge/src/lib.rs`
  - Ensure all new DTOs, traits, and types are re-exported from `lib.rs`
  - _Requirements: all_

- [ ] 18. Final checkpoint - Ensure all tests pass
  - Run `cargo test` and `cargo lint` to verify all existing and new tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The design uses Rust throughout — all implementations target the existing `tokeira-edge`, `tokeira-projection`, and `tokeirad` crates
