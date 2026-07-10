# Requirements Document

## Introduction

Tokeirad currently serves native gRPC on port 7233 with WorkflowService and OperatorService, and the hello-world example works end-to-end with the Rust SDK. However, the Temporal UI cannot connect because many gRPC endpoints the UI needs are unimplemented (returning `Status::UNIMPLEMENTED`), discovery endpoints are missing, and namespace management endpoints are not wired.

This feature enables the Temporal UI to connect to tokeirad, either through the standard `ui-server` Go proxy (which speaks native gRPC) or directly from the browser via gRPC-Web. The work is organized in priority tiers: discovery endpoints first (unblocks UI connection), then namespace management (unblocks the namespace picker), then gRPC-Web support (enables proxy-free browser connections), then visibility improvements and secondary workflow management endpoints.

## Glossary

- **Tokeirad**: The tokeira server binary that hosts gRPC services on port 7233.
- **UI_Server**: The Temporal `ui-server` Go binary that acts as a backend-for-frontend proxy, translating HTTP requests from the SvelteKit UI into native gRPC calls against the Temporal frontend service.
- **WorkflowServiceGrpc**: The tonic gRPC service implementation in `tokeira-edge` that implements the `temporal.api.workflowservice.v1.WorkflowService` proto interface.
- **OperatorServiceGrpc**: The tonic gRPC service implementation in `tokeira-edge` that implements the `temporal.api.operatorservice.v1.OperatorService` proto interface.
- **Namespace_Cache**: The `InMemoryNamespaceCache` in `tokeira-edge` that stores `ResolvedNamespace` entries and provides namespace lookup.
- **Visibility_Store**: The `VisibilityStore` trait and `InMemoryVisibilityStore` implementation in `tokeira-projection` that materializes workflow execution rows for list and count queries.
- **Edge_Interceptors**: The `EdgeInterceptors` in `tokeira-edge` that perform authorization and namespace validation before request dispatch.
- **gRPC-Web**: A protocol that allows browser-based JavaScript clients to communicate with gRPC services over HTTP/1.1 using protobuf-encoded bodies, supported by the `tonic-web` crate.
- **CORS**: Cross-Origin Resource Sharing headers required when a browser makes requests to a different origin than the page was served from.

## Requirements

### Requirement 1: System Discovery — GetSystemInfo

**User Story:** As a UI_Server operator, I want tokeirad to respond to GetSystemInfo requests, so that the Temporal UI can detect server capabilities and establish a connection.

#### Acceptance Criteria

1. WHEN a GetSystemInfo request is received, THE WorkflowServiceGrpc SHALL return a GetSystemInfoResponse containing the server version string and a capabilities structure.
2. THE WorkflowServiceGrpc SHALL populate the capabilities structure with boolean fields indicating which features tokeirad supports (signal_and_query_header, internal_error_differentiation, activity_failure_include_heartbeat, supports_schedules, encoded_failure_attributes, upsert_memo, eager_workflow_start, sdk_metadata, count_group_by_execution_status).
3. WHEN a capability is not yet implemented by tokeirad, THE WorkflowServiceGrpc SHALL set that capability field to false.
4. THE WorkflowServiceGrpc SHALL pass the GetSystemInfo request through Edge_Interceptors for authorization before returning the response.

### Requirement 2: Cluster Discovery — GetClusterInfo

**User Story:** As a UI_Server operator, I want tokeirad to respond to GetClusterInfo requests, so that the Temporal UI can display cluster metadata.

#### Acceptance Criteria

1. WHEN a GetClusterInfo request is received, THE WorkflowServiceGrpc SHALL return a GetClusterInfoResponse containing the cluster name, server version, and cluster identifier.
2. THE WorkflowServiceGrpc SHALL delegate to the existing `OperatorApi::cluster_info()` method to retrieve cluster metadata.
3. THE WorkflowServiceGrpc SHALL populate the `supported_clients` map with an empty map (no client version restrictions).
4. THE WorkflowServiceGrpc SHALL pass the GetClusterInfo request through Edge_Interceptors for authorization before returning the response.

### Requirement 3: Namespace Listing — ListNamespaces

**User Story:** As a UI user, I want to see all registered namespaces in the sidebar picker, so that I can switch between namespaces.

#### Acceptance Criteria

1. WHEN a ListNamespaces request is received, THE WorkflowServiceGrpc SHALL return a ListNamespacesResponse containing all namespaces from the Namespace_Cache.
2. THE WorkflowServiceGrpc SHALL populate each namespace entry with namespace_info (name, state, description) and namespace_config (retention period).
3. WHEN the Namespace_Cache contains no namespaces, THE WorkflowServiceGrpc SHALL return an empty list.
4. THE Namespace_Cache SHALL expose a method to list all stored namespaces.
5. THE WorkflowServiceGrpc SHALL pass the ListNamespaces request through Edge_Interceptors for authorization before returning the response.

### Requirement 4: Namespace Detail — DescribeNamespace

**User Story:** As a UI user, I want to view details of a specific namespace, so that I can see its configuration and state.

#### Acceptance Criteria

1. WHEN a DescribeNamespace request is received with a namespace name, THE WorkflowServiceGrpc SHALL return a DescribeNamespaceResponse with namespace_info, namespace_config, and is_global_namespace fields.
2. WHEN a DescribeNamespace request is received with a namespace name that does not exist in the Namespace_Cache, THE WorkflowServiceGrpc SHALL return a gRPC NOT_FOUND status.
3. THE WorkflowServiceGrpc SHALL pass the DescribeNamespace request through Edge_Interceptors for authorization before returning the response.

### Requirement 5: Visibility — ListWorkflowExecutions with real data

**User Story:** As a UI user, I want to see a list of workflow executions with accurate metadata, so that I can browse and search workflows in the UI.

#### Acceptance Criteria

1. WHEN a ListWorkflowExecutions request is received, THE WorkflowServiceGrpc SHALL return workflow executions from the Visibility_Store with accurate `workflow_id`, `run_id`, `workflow_type`, `task_queue`, `status`, `start_time`, `close_time`, `history_length`, `state_transition_count`, `memo`, and `search_attributes`.
2. WHEN a ListWorkflowExecutions request includes a query string, THE Visibility_Store SHALL filter results by the query (at minimum supporting `WorkflowType` and `ExecutionStatus` filters).
3. WHEN a ListWorkflowExecutions request includes a page_size and next_page_token, THE Visibility_Store SHALL paginate results correctly.
4. THE Visibility_Store SHALL be populated with execution data when workflows are started, updated, and closed — not returning empty or stale results.
5. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

### Requirement 6: Workflow Detail — DescribeWorkflowExecution completeness

**User Story:** As a UI user, I want the workflow detail view to show complete execution information, so that I can inspect workflow state.

#### Acceptance Criteria

1. WHEN a DescribeWorkflowExecution request is received, THE WorkflowServiceGrpc SHALL return a DescribeWorkflowExecutionResponse with `workflow_execution_info` containing all available fields (execution, type, task_queue, start_time, close_time, status, history_length, memo, search_attributes).
2. WHEN the workflow has pending activities, THE response SHOULD include `pending_activities` with activity_id, activity_type, state, and attempt (if the data is available from the runtime).
3. WHEN the specified workflow execution does not exist, THE WorkflowServiceGrpc SHALL return a gRPC NOT_FOUND status.
4. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

### Requirement 7: gRPC-Web Transport Layer

**User Story:** As a developer, I want tokeirad to accept gRPC-Web requests directly from the browser, so that I can use the Temporal UI without running the ui-server proxy.

#### Acceptance Criteria

1. THE Tokeirad server SHALL wrap its gRPC services with a tonic-web layer that accepts gRPC-Web requests (Content-Type: application/grpc-web, application/grpc-web+proto).
2. THE Tokeirad server SHALL include CORS headers that allow requests from any origin during development (Access-Control-Allow-Origin: *).
3. THE Tokeirad server SHALL accept both native gRPC (HTTP/2) and gRPC-Web (HTTP/1.1) requests on the same port.
4. WHEN a gRPC-Web request is received, THE tonic-web layer SHALL decode the request, dispatch it to the appropriate gRPC service, and encode the response back in gRPC-Web format.
5. THE Tokeirad server SHALL add `tonic-web` and `tower-http` (for CORS) as workspace dependencies.

### Requirement 8: Reverse History — GetWorkflowExecutionHistoryReverse

**User Story:** As a UI user, I want to view workflow execution history in reverse chronological order, so that I can see the most recent events first.

#### Acceptance Criteria

1. WHEN a GetWorkflowExecutionHistoryReverse request is received, THE WorkflowServiceGrpc SHALL return the history events in reverse order (most recent event first).
2. THE WorkflowServiceGrpc SHALL support pagination via next_page_token for reverse history traversal.
3. WHEN the specified workflow execution does not exist, THE WorkflowServiceGrpc SHALL return a gRPC NOT_FOUND status.
4. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

### Requirement 9: Workflow Deletion — DeleteWorkflowExecution

**User Story:** As a UI user, I want to delete a workflow execution from the UI, so that its authoritative state, history, and visibility record are removed.

#### Acceptance Criteria

1. WHEN a valid DeleteWorkflowExecution request identifies an existing closed workflow execution, THE WorkflowServiceGrpc SHALL arrange deletion of that run's authoritative mutable state, event history, current-execution pointer when it names that run, and derived visibility data, and SHALL return an empty successful response.
2. WHEN a valid DeleteWorkflowExecution request identifies an existing running workflow execution, THE WorkflowServiceGrpc SHALL terminate that run with the workflow-deletion reason and history-service identity before arranging the same deletion as for a closed execution.
3. WHEN a DeleteWorkflowExecution request omits `run_id`, THE WorkflowServiceGrpc SHALL target the current execution identified by the request's namespace and `workflow_id` rather than selecting an older run after the current-execution pointer has been deleted.
4. WHEN deletion of a workflow execution has completed, THE WorkflowServiceGrpc SHALL return gRPC NOT_FOUND with no response for both DescribeWorkflowExecution and GetWorkflowExecutionHistory requests that identify the deleted run.
5. WHEN deletion of a workflow execution has completed, THE Visibility_Store SHALL exclude the deleted run from ListWorkflowExecutions results, including in the presence of an older delayed visibility update for that run.
6. WHEN a DeleteWorkflowExecution request identifies a workflow execution that does not exist, THE WorkflowServiceGrpc SHALL return a gRPC NOT_FOUND status.
7. WHEN a DeleteWorkflowExecution request omits the execution, contains an empty `workflow_id`, or contains a non-empty malformed `run_id`, THE WorkflowServiceGrpc SHALL return a gRPC INVALID_ARGUMENT status.
8. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

These semantics are verified against `service/frontend/workflow_handler.go`,
`service/frontend/validators.go`, `service/history/api/deleteworkflow/api.go`,
`service/history/transfer_queue_task_executor_base.go`, and
`service/history/shard/context_impl.go` at Temporal server v1.31.0.

### Requirement 10: Workflow Reset — ResetWorkflowExecution

**User Story:** As a UI user, I want to reset a workflow execution to a previous point in its history, so that I can retry from a known-good state.

#### Acceptance Criteria

1. WHEN a ResetWorkflowExecution request is received with a valid workflow_task_finish_event_id naming a `WORKFLOW_TASK_COMPLETED`, `WORKFLOW_TASK_FAILED`, `WORKFLOW_TASK_TIMED_OUT`, or `WORKFLOW_TASK_STARTED` event, THE WorkflowServiceGrpc SHALL create a new run that replays history up to the specified event and return the new run_id.
2. WHEN a ResetWorkflowExecution request references a non-existent workflow execution, THE WorkflowServiceGrpc SHALL return a gRPC NOT_FOUND status.
3. WHEN a ResetWorkflowExecution request references an event_id that is not a `WORKFLOW_TASK_COMPLETED`, `WORKFLOW_TASK_FAILED`, `WORKFLOW_TASK_TIMED_OUT`, or `WORKFLOW_TASK_STARTED` event, THE WorkflowServiceGrpc SHALL return a gRPC INVALID_ARGUMENT status.
4. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

### Requirement 11: Task Queue Description — DescribeTaskQueue

**User Story:** As a UI user, I want to view task queue details including recently observed pollers, so that I can monitor worker health.

#### Acceptance Criteria

1. WHEN a DescribeTaskQueue request is received, THE WorkflowServiceGrpc SHALL return one poller entry per worker identity observed on the specified task queue during the preceding five minutes.
2. WHEN no worker identity has been observed on the specified task queue during the preceding five minutes, THE WorkflowServiceGrpc SHALL return an empty pollers list.
3. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.
4. WHEN the same worker identity polls repeatedly, THE WorkflowServiceGrpc SHALL return one entry whose last_access_time reflects its latest poll observation.
5. WHEN a poll returns a task or reaches its long-poll deadline, THE Edge_Layer SHALL refresh that identity's last_access_time at poll completion.
6. WHEN client or worker cancellation drops an outstanding poll, THE Edge_Layer SHALL NOT refresh last_access_time at poll completion.

### Requirement 12: Signal-With-Start — SignalWithStartWorkflowExecution

**User Story:** As a UI user, I want to signal a workflow and start it if it does not exist, so that I can trigger workflows from the UI reliably.

#### Acceptance Criteria

1. WHEN a SignalWithStartWorkflowExecution request is received and the target workflow is running, THE WorkflowServiceGrpc SHALL deliver the signal to the running workflow and return the existing run_id.
2. WHEN a SignalWithStartWorkflowExecution request is received and the target workflow does not exist, THE WorkflowServiceGrpc SHALL start a new workflow execution with the signal delivered as the first event after start, and return the new run_id.
3. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.

### Requirement 13: Namespace Registration — RegisterNamespace

**User Story:** As an operator, I want to create new namespaces via the gRPC API, so that the UI or CLI can register namespaces without restarting tokeirad.

#### Acceptance Criteria

1. WHEN a RegisterNamespace request is received with a valid namespace name, THE WorkflowServiceGrpc SHALL insert the namespace into the Namespace_Cache and return a successful response.
2. WHEN a RegisterNamespace request is received with a namespace name that already exists in the Namespace_Cache, THE WorkflowServiceGrpc SHALL return a gRPC ALREADY_EXISTS status.
3. THE WorkflowServiceGrpc SHALL validate that the namespace name is non-empty and contains only alphanumeric characters, hyphens, and underscores.
4. THE WorkflowServiceGrpc SHALL pass the request through Edge_Interceptors for authorization before returning the response.
