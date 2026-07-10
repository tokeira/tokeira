# Implementation Plan: Temporal UI Support

## Overview

Implement the missing gRPC endpoints, namespace management, gRPC-Web transport, and visibility wiring so the Temporal UI can connect to tokeirad. Tasks are ordered by what unblocks the UI fastest: discovery endpoints first, then namespace management, then gRPC-Web transport, then visibility and workflow management endpoints.

All new endpoints follow the existing edge-layer pattern: thin gRPC handler → edge method with interceptors → runtime/storage/cache delegation. Proto ↔ edge DTO translation lives in `grpc/translate.rs`.

Tier 3.19 proved that the completed deletion slices in Tasks 8 and 13 implemented only
visibility removal: all three `TestWorkflowDeleteExecutionSuite` leaves still read the
supposedly deleted mutable state and history. Correction Tasks 19–28 supersede the
deletion portions of Tasks 8 and 13 while preserving the historical completion record.

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

  - [ ] 1.3 Write property tests for namespace cache round-trip (Property 1)
    - Generate random sets of `ResolvedNamespace`, insert all, and verify `list_all()`
      returns every inserted namespace with matching fields for at least 100 cases.
    - Tag: `// Feature: temporal-ui-support, Property 1: namespace cache round-trip`
    - _Requirements: 3.1, 3.2, 3.4_

  - [ ] 1.4 Write property test for namespace lookup correctness (Property 2)
    - Generate random namespace names, insert some, and verify `get()` for present and
      absent names for at least 100 cases.
    - Tag: `// Feature: temporal-ui-support, Property 2: namespace lookup correctness`
    - _Requirements: 4.1, 4.2_

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

  - [x] 4.4 Write unit tests for GetSystemInfo and GetClusterInfo
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

  - [x] 5.3 Write unit tests for ListNamespaces and DescribeNamespace
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

  - [ ] 6.3 Write integration tests for gRPC-Web and CORS
    - Test gRPC-Web request to `GetSystemInfo` returns valid response
    - Test CORS preflight returns expected headers
    - _Requirements: 7.1, 7.2_

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement Tier 2 — Visibility pipeline wiring (legacy deletion API superseded by Task 23)
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

- [x] 9. Implement Tier 2 — DescribeWorkflowExecution completeness
  - [x] 9.1 Enhance `describe_response_to_proto` translation for complete field coverage
    - Ensure `workflow_execution_info` in the proto response includes all available fields: execution, type, task_queue, start_time, close_time, status, history_length, memo, search_attributes, state_transition_count
    - Add `pending_activities` population if data is available from the runtime
    - _Requirements: 6.1, 6.2_

  - [x] 9.2 Write property test for describe workflow field preservation (Property 3)
    - Generate at least 100 random `WorkflowExecutionDescription` values, translate to
      proto, and verify every designed field is preserved.
    - Tag: `// Feature: temporal-ui-support, Property 3: describe field preservation`
    - _Requirements: 6.1_

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

  - [x] 10.3 Write property test for reverse history ordering (Property 4)
    - Generate at least 100 random history sequences and page sizes; verify strictly
      descending event ids and pagination completeness.
    - Tag: `// Feature: temporal-ui-support, Property 4: reverse history ordering`
    - _Requirements: 8.1, 8.2_

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

  - [ ] 12.2 Write property test for register namespace round-trip with duplicate detection (Property 8)
    - Generate at least 100 valid namespace names, verify register/describe round-trip,
      and verify duplicate registration returns ALREADY_EXISTS.
    - Tag: `// Feature: temporal-ui-support, Property 8: namespace register round-trip`
    - _Requirements: 13.1, 13.2_

  - [ ] 12.3 Write property test for namespace name validation (Property 9)
    - Generate at least 100 valid and invalid strings and verify registration accepts if
      and only if the non-empty value matches `^[a-zA-Z0-9_-]+$`.
    - Tag: `// Feature: temporal-ui-support, Property 9: namespace name validation`
    - _Requirements: 13.3_

  - [x] 12.4 Write unit tests for RegisterNamespace edge cases
    - Test empty name returns INVALID_ARGUMENT
    - Test special characters return INVALID_ARGUMENT
    - _Requirements: 13.3_

- [x] 13. Implement Tier 3 — DeleteWorkflowExecution (legacy visibility-only slice; superseded by Tasks 19–28)
  - [x] 13.1 Implement `delete_workflow_execution` edge method and gRPC handler
    - Add `delete_workflow_execution(&self, headers: &HeaderMap, req: DeleteWorkflowExecutionRequest) -> EdgeResult<()>` to `WorkflowService`
    - Resolve the workflow execution, return `EdgeError::WorkflowNotFound` if absent
    - If the workflow is running, terminate it first via `runtime.terminate_workflow()`
    - Delete from visibility store via `delete_execution()`
    - Add `delete_request_to_edge` translation in `grpc/translate.rs`
    - Wire the gRPC handler to replace the `Status::unimplemented` stub
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

  - [x] 13.2 Write unit tests for DeleteWorkflowExecution
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

  - [ ] 14.2 Write unit tests for ResetWorkflowExecution
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

  - [ ] 15.2 Write property test for signal-with-start conditional behavior (Property 7)
    - Generate at least 100 signal-with-start scenarios and verify the existing-run and
      new-run branches plus returned run id against a reference model.
    - Tag: `// Feature: temporal-ui-support, Property 7: signal-with-start conditional behavior`
    - _Requirements: 12.1, 12.2_

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

  - [ ] 16.4 Write property test for describe task queue lists all pollers (Property 6)
    - Generate at least 100 poller sets, register them, and verify
      `describe_task_queue` returns every active identity.
    - Tag: `// Feature: temporal-ui-support, Property 6: describe task queue pollers`
    - _Requirements: 11.1_

- [x] 17. Update public exports in `tokeira-edge/src/lib.rs`
  - Ensure all new DTOs, traits, and types are re-exported from `lib.rs`
  - _Requirements: all_

- [ ] 18. Legacy feature checkpoint - Ensure all tests pass
  - Run `cargo test` and `cargo lint` to verify all existing and new tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 19. Tier 3.19 bug-condition exploration — prove visibility-only deletion is insufficient
  - [x] 19.1 Add the Property 5 exploration test before changing production code
    - Generate open and closed executions with run-owned history and side-table state,
      invoke `DeleteWorkflowExecution`, and assert the exact run is absent from explicit
      resolution, mutable-state load, and history reads and that its side rows are gone.
    - Run with at least 100 cases and confirm it fails on the current implementation;
      retain the same test as the post-fix regression property.
    - Tag: `// Feature: temporal-ui-support, Property 5: authoritative workflow deletion`
    - _Requirements: 9.1, 9.2, 9.4_

- [x] 20. Add the authoritative storage deletion contract
  - [x] 20.1 Add `DeleteRunRequest`, `DeleteRunResult`, and `RunRepository::delete_run_for_bundle`
    - Define the expected-sequence and deletion-time inputs plus Deleted, NotFound, and
      Conflict outcomes in `tokeira-storage/src/api.rs`; document OCC, shard-fence, and
      atomicity guarantees on every public item.
    - Thread the method through repository forwarding wrappers and test doubles without
      weakening production implementations to an unsupported default.
    - _Requirements: 9.1, 9.2, 9.4_

  - [x] 20.2 Implement atomic deletion in `InMemoryStore`
    - Add an explicit current-execution pointer used by `find_latest_run`, updated on
      new-run creation and conditionally removed only when it still names the target.
    - Under one store lock, validate epoch and expected sequence, append a Deleted
      projection record at `expected_seq.next()`, and remove run state, history,
      execution index, dedupe, audit, activity, timer, dispatch, backlog, and shard-map
      entries.
    - _Requirements: 9.1, 9.3, 9.4, 9.5_

  - [x] 20.3 Implement atomic deletion in `DsqlRunRepository`
    - In one retried DSQL transaction, lock and validate `workflow_hot`, enforce the
      execution-home epoch and expected sequence, insert the Deleted projection record,
      conditionally delete `current_execution`, then delete every run-owned row from
      `workflow_hot`, `history_batch`, `request_dedupe`, `activity_state`,
      `timer_bucket`, `activity_dispatch`, and `dispatch_backlog`.
    - Reuse the existing DSQL operation metrics and OCC classification; do not add an
      `ALTER TABLE` migration during the pre-baseline build phase.
    - _Requirements: 9.1, 9.3, 9.4, 9.5_

  - [x] 20.4 Share projection-context construction between commit and deletion
    - Extract the complete workflow projection image builder so normal commits and
      deletion use identical identity/version fields; deletion overrides lifecycle to
      `Deleted`, update time to the admitted deletion time, and clears memo/search
      attributes.
    - _Requirements: 9.5_

- [x] 21. Checkpoint — storage deletion contract is green
  - Run formatting, `cargo test -p tokeira-storage`, and workspace linting; require the
    storage API, in-memory implementation, DSQL implementation, wrappers, and mocks to
    compile with no warnings.

- [x] 22. Implement the runtime deletion coordinator
  - [x] 22.1 Add run-scoped disposable-state cleanup helpers
    - Add run removal to workflow/activity brokers and activity timeout tracking; reuse
      existing run removal for workflow/WFT/Nexus timeouts, completion callbacks,
      buffered queries, close-attempt tracking, and updates.
    - Ensure drained query/update waiters complete with a deterministic not-found/closed
      result rather than hanging, while already-delivered worker tasks remain safely
      fenced by absent authoritative state.
    - _Requirements: 9.1, 9.2, 9.4_

  - [x] 22.2 Add `TokeiraRuntime::delete_workflow`
    - Resolve one explicit run; when open, submit the existing kernel `Terminate`
      command through its lane with reason `Delete workflow execution`, no details, and
      identity `history-service`; closed runs skip termination.
    - Reload after termination, derive the execution-home bundle and commit epoch using
      the normal lane rules, call `delete_run_for_bundle`, and retry OCC conflicts from
      a fresh load without ever retargeting another run.
    - Return the exact persisted tombstone and a typed runtime not-found error; perform
      disposable-state cleanup only after the authoritative purge succeeds.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.6_

  - [x] 22.3 Extend `WorkflowRuntimeApi` and `RuntimeAdapter`
    - Add `DeleteWorkflowRequest` and `WorkflowDeletion`, adapt the edge run key to one
      explicit execution reference, and map the typed runtime not-found error without
      string inspection.
    - Update all edge/runtime test doubles and public exports with documented defaults
      only where backward-compatible test isolation requires one.
    - _Requirements: 9.1, 9.2, 9.6_

  - [x] 22.4 Route batch deletion through the same coordinator
    - Remove the separate terminate-plus-visibility-only batch path so direct and batch
      deletion share termination identity, fencing, purge, cleanup, and tombstone
      behaviour.
    - _Requirements: 9.1, 9.2, 9.4, 9.5_

- [x] 23. Replace physical visibility deletion with versioned tombstones
  - [x] 23.1 Change the projection interfaces
    - Replace run-key-only `delete_execution` on `VisibilityApi` and `VisibilityStore`
      with `apply_deletion(ProjectionRecord)` and update forwarding implementations,
      mocks, and public documentation.
    - Make `VisibilitySink` route `VisibilityLifecycleState::Deleted` records through
      the same operation used by the synchronous edge path.
    - _Requirements: 9.5_

  - [x] 23.2 Implement in-memory tombstone application
    - Persist a Deleted high-water row only when its visibility version is newer, clear
      all search-attribute indexes, subtract prior rollups, and exclude Deleted rows
      from list, filtered count, grouped count, and rollup count results.
    - _Requirements: 9.5_

  - [x] 23.3 Implement DSQL tombstone application
    - Apply the version-guarded Deleted row and search-index cleanup transactionally;
      make every DSQL list/count query explicitly exclude lifecycle `Deleted` and keep
      rollup counters consistent.
    - _Requirements: 9.5_

  - [x] 23.4 Make rollup deltas lifecycle-aware
    - Treat visible-to-deleted as removal, absent-to-deleted as no contribution, and
      deleted-to-deleted as an idempotent no-op without changing ordinary visible-row
      transition conservation.
    - _Requirements: 9.5_

- [x] 24. Wire authoritative deletion through the edge and server
  - [x] 24.1 Complete delete request validation
    - Reject a missing execution, empty workflow id, and malformed non-empty run id as
      INVALID_ARGUMENT; preserve run-id omission as current-execution resolution and
      keep interceptor/routing checks ahead of mutation.
    - _Requirements: 9.3, 9.6, 9.7, 9.8_

  - [x] 24.2 Replace edge delete orchestration
    - Resolve the exact run once, call `runtime.delete_workflow`, synchronously apply the
      returned authoritative tombstone through `VisibilityApi`, and return the empty
      response only after both operations succeed.
    - Map initial or raced absence to typed NOT_FOUND with no response; remove direct
      repository loads, ad-hoc termination identity, and physical visibility deletion
      from the edge handler.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.8_

  - [x] 24.3 Update app wiring and test fixtures
    - Thread the new runtime and projection methods through `tokeirad`, gRPC roundtrip
      fixtures, history-wait repository wrappers, and all mocks while retaining the
      existing projection workers for durable tombstone replay.
    - _Requirements: 9.1, 9.4, 9.5_

- [x] 25. Add the remaining required Tier 3.19 property tests
  - [x] 25.1 Property 10 — current-execution pointer safety
    - Generate lineages with older runs and an optional replacement current run; delete
      the target and compare resolution against a pointer-based reference model for at
      least 100 cases.
    - Tag: `// Feature: temporal-ui-support, Property 10: current-execution pointer safety`
    - _Requirements: 9.3_

  - [x] 25.2 Property 11 — visibility tombstone monotonicity
    - Generate older snapshots, duplicates, and one newer deletion record, permute
      application order, and prove convergence to an invisible tombstone with no index
      or rollup contribution for at least 100 cases.
    - Tag: `// Feature: temporal-ui-support, Property 11: visibility tombstone monotonicity`
    - _Requirements: 9.5_

  - [x] 25.3 Property 12 — rejected deletion preserves state
    - Generate arbitrary authoritative/visibility store contents and missing, empty-id,
      or malformed-run-id delete requests; assert byte-equivalent observable state
      before and after rejection for at least 100 cases.
    - Tag: `// Feature: temporal-ui-support, Property 12: rejected deletion preserves state`
    - _Requirements: 9.6, 9.7_

- [x] 26. Add fixed and end-to-end deletion coverage
  - [x] 26.1 Add example-based storage/runtime/projection tests
    - Cover exact termination reason/identity, closed-run no-extra-termination, every
      purged table, conditional current-pointer deletion, DSQL transaction statements,
      Deleted-row query exclusion, rollup subtraction, duplicate replay, and stale
      snapshot rejection.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 26.2 Add edge and gRPC validation tests
    - Assert INVALID_ARGUMENT for missing execution, empty workflow id, and malformed
      run id; assert NOT_FOUND with no response for a missing or deleted execution; and
      verify interceptors still run before mutation.
    - _Requirements: 9.6, 9.7, 9.8_

  - [x] 26.3 Add real-stack deletion roundtrips
    - Exercise completed, running, and already-terminated runs through gRPC and assert
      Describe and History return NOT_FOUND while List returns zero rows after success.
    - _Requirements: 9.1, 9.2, 9.4, 9.5_

- [x] 27. Checkpoint — Tier 3.19 implementation is locally green
  - Run `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`,
    `cargo check --workspace`, and focused tests for storage, runtime, projection, edge,
    and tokeirad; resolve every warning or failure before functional conformance.

- [x] 28. Prove Tier 3.19 conformance and record evidence
  - [x] 28.1 Run the pinned functional suite against a real `tokeirad`
    - Run `TestWorkflowDeleteExecutionSuite` from the sibling Temporal fork with the
      pinned Go toolchain and require all three leaves to pass with no new skip.
    - Repeat the focused suite to catch deletion/projection timing flakes, then run the
      accumulated Tier 1.1–3.19 regression selection.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_

  - [x] 28.2 Update compatibility evidence after the suite is clean
    - Record Tier 3.19 in `docs/readiness/conformance.md`; update the API tracker entry
      for `DeleteWorkflowExecution`; retain the broader workflow-cancel-terminate
      feature's `Partial` state unless cancel and terminate independently have enough
      evidence, but add the deletion corpus evidence to its notes/evidence.
    - _Requirements: 9.1, 9.2, 9.4, 9.5_

  - [x] 28.3 Run the Tier 3.19 verification gate
    - Run formatting, lint, test-lint, workspace check, the focused crate suites, and
      the accumulated functional regression selection; require a clean working diff
      before committing Tier 3.19.
    - Per owner direction on 2026-07-10, reserve the full workspace test, rustdoc, and
      compatibility-invariant sweep for milestone/release gates rather than repeating
      them after every remaining conformance tier.

## Task Dependency Graph

```json
{
  "19": [],
  "20": ["19"],
  "21": ["20"],
  "22": ["21"],
  "23": ["22"],
  "24": ["23"],
  "25": ["20", "22", "23", "24"],
  "26": ["24", "25"],
  "27": ["26"],
  "28": ["27"]
}
```

## Notes

- Property tests are required and run at least 100 generated cases; none is an optional
  MVP item.
- Tasks 19–28 are the Tier 3.19 execution path. Earlier unchecked tasks remain part of
  the broader UI-support backlog but do not supersede this correction sequence.
- The kernel is intentionally absent from the deletion tasks. Only the existing
  `Terminate` command is used for an open target; purge and tombstone work belongs to
  runtime, storage, projection, edge, and server wiring.
- From Tier 3.20 onward, per-tier evidence uses focused crate gates plus the target
  corpus; full workspace and accumulated-corpus sweeps run at milestone/release gates.
