# Requirements Document: Edge Batch Operations Transport

## Introduction

This spec implements the Batch Operations Transport layer — the 4 gRPC handlers for Temporal's batch operations feature in the `tokeira-edge` crate, plus the backing batch operation store and execution engine. Batch operations allow operators to perform bulk actions (terminate, cancel, signal, delete, reset) on workflow executions matching a visibility query or an explicit execution list.

> **Scoped out for MVP:** `BatchOperationUpdateWorkflowExecutionOptions` is defined in the upstream proto but is not implemented. The `start_batch_operation` handler SHALL return `INVALID_ARGUMENT` if this operation variant is requested.

This is Feature 7 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features in the umbrella spec. The work covers 4 gRPC handlers across two categories:

1. **Store + Start** (Phase 1): `BatchOperationStore` in-memory store keyed by (namespace_id, job_id), and the `start_batch_operation` handler that validates the request, creates the store entry, and spawns the background execution engine.
2. **Execution Engine** (Phase 2): A background task per batch operation that queries visibility (or iterates an explicit execution list), applies the requested operation to each matching workflow, and tracks progress counts. The engine lives in `tokeira-edge` (not runtime) because it depends on `VisibilityApi` and `WorkflowService` for operation dispatch.
3. **Lifecycle Handlers** (Phase 3): `stop_batch_operation` (cooperative stop via cancellation flag), `describe_batch_operation` (query state and progress), `list_batch_operations` (paginated listing).

The batch operation store is in-memory for MVP — a `DashMap<(NamespaceId, JobId), BatchOperationEntry>` (same pattern as `VersioningRuleStore` and `ScheduleStore`). Durable persistence is deferred to the DSQL storage spec.

The execution engine applies operations using internal `WorkflowService` batch-dispatch methods: `terminate_workflow_batch_internal`, `cancel_workflow_batch_internal`, `signal_workflow_batch_internal`, `delete_workflow_batch_internal`, and `reset_workflow_batch_internal`. These methods accept the pre-validated batch dispatch context plus the exact `WorkflowExecutionRef`, including `run_id` when present, so background execution does not re-authenticate from headers and does not accidentally target the current run. Proto translation stays in `tokeira-edge`. The store lives in `tokeira-runtime`.

Currently all 4 handler stubs exist in `tokeira-edge/src/grpc/workflow_service.rs` returning `Status::unimplemented`.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **BatchOperationStore**: The in-memory store (in `tokeira-runtime`) that persists batch operation entries per (namespace_id, job_id).
- **JobId**: A string identifier for a batch operation, unique within a namespace.
- **BatchOperationEntry**: The full stored state of a batch operation: job_id, namespace_id, operation type, operation parameters, state, progress counts, start/close times, identity, reason, and a cancellation flag.
- **BatchOperationType**: The enum `temporal.api.enums.v1.BatchOperationType` identifying the bulk action: Terminate, Cancel, Signal, Delete, or Reset. `UpdateExecutionOptions` is defined in the proto but scoped out for MVP.
- **BatchOperationState**: The enum `temporal.api.enums.v1.BatchOperationState` tracking lifecycle: Running, Completed, or Failed.
- **BatchExecutionEngine**: The background task (in `tokeira-edge`) that iterates matching workflows and applies the batch operation to each one, tracking progress. Lives in edge because it depends on `VisibilityApi` and `WorkflowService`.
- **VisibilityQuery**: A string query used by `list_workflow_executions` to find matching workflow executions.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.
- **CancellationFlag**: A `CancellationToken` (from `tokio_util`) checked between iterations by the BatchExecutionEngine to support cooperative stop.

## Requirements

---

## Phase 1: Batch Operation Store and Start Handler

### Requirement 1: BatchOperationStore — In-Memory Batch Operation Storage

**User Story:** As a Tokeira developer, I want to store batch operation entries per (namespace_id, job_id), so that batch operation handlers have a backing store for state and progress tracking.

#### Acceptance Criteria

1. THE BatchOperationStore SHALL store a `BatchOperationEntry` per (namespace_id, job_id) pair.
2. THE BatchOperationStore SHALL be safe for concurrent access from multiple gRPC handler threads and background engine tasks.
3. WHEN a batch operation entry does not exist for a (namespace_id, job_id) pair, THE BatchOperationStore SHALL return a `NOT_FOUND` error for describe, stop, and list-by-id operations.
4. WHEN a `start` call is made with a job_id that already exists in the namespace, THE BatchOperationStore SHALL return an `ALREADY_EXISTS` error.
5. THE BatchOperationStore SHALL support atomic updates to progress counters (total_operation_count, complete_operation_count, failure_operation_count) from the background engine task.

### Requirement 2: start_batch_operation Handler

**User Story:** As a Temporal SDK user, I want to start a batch operation via the `start_batch_operation` gRPC endpoint, so that I can perform bulk actions on workflow executions matching a visibility query or explicit execution list.

#### Acceptance Criteria

1. WHEN the `start_batch_operation` endpoint is called with a valid namespace, job_id, visibility_query (or executions list), and exactly one operation variant, THE handler SHALL create a `BatchOperationEntry` in the store with state `Running` and `start_time` set to the current timestamp, and return a successful response.
2. WHEN the job_id is empty, THE handler SHALL return `INVALID_ARGUMENT`.
3. WHEN neither `visibility_query` nor `executions` is provided, THE handler SHALL return `INVALID_ARGUMENT`.
4. WHEN no operation variant is set in the request, THE handler SHALL return `INVALID_ARGUMENT`.
5. WHEN a batch operation with the same job_id already exists in the namespace, THE handler SHALL return `ALREADY_EXISTS`.
6. WHEN the request is valid, THE handler SHALL spawn a BatchExecutionEngine background task for the new batch operation.
7. THE handler SHALL store the `reason`, `identity` (from the request metadata), and `max_operations_per_second` alongside the batch operation entry.
8. WHEN the operation variant is `signal_operation`, THE handler SHALL store the signal name and input payloads from the `BatchOperationSignal` message. The `header` field is dropped at translation time — it is not stored or delivered. The kernel `SignalRequest` has no header field. This is documented in UNSUPPORTED_FIELDS.md.
9. WHEN the operation variant is `termination_operation`, THE handler SHALL store the termination details from the `BatchOperationTermination` message.
10. WHEN the operation variant is `reset_operation`, THE handler SHALL translate the `BatchOperationReset` message into a `BatchResetTarget` enum preserving the supported target variants: `WorkflowTaskId(i64)`, `FirstWorkflowTask`, `LastWorkflowTask`, `BuildId(String)`. The `reset_reapply_type`, `current_run_only`, and `reset_reapply_exclude_types` fields are not supported and are documented in UNSUPPORTED_FIELDS.md. The engine resolves the concrete `fork_event_id` per-workflow at dispatch time.

### Requirement 3: Proto Translation for Batch Operation Types

**User Story:** As a Tokeira developer, I want proto translation functions for batch operation request and response types, so that the gRPC handlers can convert between proto messages and internal domain types.

#### Acceptance Criteria

1. THE Edge_Layer SHALL provide translation functions between proto `StartBatchOperationRequest` and the internal `BatchOperationEntry` creation parameters.
2. THE Edge_Layer SHALL provide translation functions to construct proto `DescribeBatchOperationResponse` from the internal `BatchOperationEntry`.
3. THE Edge_Layer SHALL provide translation functions to construct proto `ListBatchOperationsResponse` (with `BatchOperationInfo` items) from internal store data.
4. WHEN a proto field contains an invalid value (e.g., empty job_id, missing operation variant), THE translation function SHALL return a descriptive error rather than silently defaulting.
5. THE translation functions SHALL map between `BatchOperationType` enum values and the internal operation type representation.
6. THE translation functions SHALL map between `BatchOperationState` enum values and the internal state representation.

---

## Phase 2: Batch Execution Engine

### Requirement 4: Batch Execution Engine — Workflow Iteration and Operation Application

**User Story:** As a Temporal operator, I want batch operations to iterate through matching workflows and apply the requested operation to each one, so that bulk actions execute automatically after being started.

#### Acceptance Criteria

1. WHEN a batch operation uses a `visibility_query`, THE BatchExecutionEngine SHALL call `WorkflowService::list_workflow_executions` with the query to discover matching workflows, following pagination to process all results.
2. WHEN a batch operation uses an explicit `executions` list, THE BatchExecutionEngine SHALL iterate through the provided workflow executions directly.
3. WHEN the operation type is `Terminate`, THE BatchExecutionEngine SHALL call `WorkflowService::terminate_workflow_batch_internal` for each matching workflow, passing the exact `WorkflowExecutionRef` and termination details from the stored operation parameters.
4. WHEN the operation type is `Cancel`, THE BatchExecutionEngine SHALL call `WorkflowService::cancel_workflow_batch_internal` for each matching workflow, passing the exact `WorkflowExecutionRef`.
5. WHEN the operation type is `Signal`, THE BatchExecutionEngine SHALL call `WorkflowService::signal_workflow_batch_internal` for each matching workflow, passing the exact `WorkflowExecutionRef`, signal name, and input from the stored operation parameters. The signal `header` field is not delivered (documented as unsupported).
6. WHEN the operation type is `Delete`, THE BatchExecutionEngine SHALL call `WorkflowService::delete_workflow_batch_internal` for each matching workflow, passing the exact `WorkflowExecutionRef`.
7. WHEN the operation type is `Reset`, THE BatchExecutionEngine SHALL resolve the concrete `fork_event_id` for each matching workflow by reading that exact workflow execution's history (including `run_id` when present), then call `WorkflowService::reset_workflow_batch_internal` with the exact `WorkflowExecutionRef`, resolved `fork_event_id`, and `reason`.
8. WHEN an individual operation succeeds, THE BatchExecutionEngine SHALL increment `complete_operation_count` in the store entry.
9. WHEN an individual operation fails, THE BatchExecutionEngine SHALL increment `failure_operation_count` in the store entry and continue processing remaining workflows.
10. THE BatchExecutionEngine SHALL update `total_operation_count` in the store entry to reflect the total number of workflows discovered (from visibility query pagination or explicit list length).

### Requirement 5: Batch Execution Engine — Completion and Rate Limiting

**User Story:** As a Temporal operator, I want batch operations to complete gracefully and respect rate limits, so that bulk operations do not overwhelm the system.

#### Acceptance Criteria

1. WHEN all matching workflows have been processed, THE BatchExecutionEngine SHALL set the batch operation state to `Completed` and record `close_time` as the current timestamp.
2. WHEN the BatchExecutionEngine encounters an unrecoverable error during visibility query iteration (not an individual operation failure), THE BatchExecutionEngine SHALL set the batch operation state to `Failed` and record `close_time`.
3. WHEN `max_operations_per_second` is set to a positive value, THE BatchExecutionEngine SHALL limit the rate of individual operation invocations to that value.
4. WHEN `max_operations_per_second` is zero or unset, THE BatchExecutionEngine SHALL apply a default rate limit to prevent system overload.

### Requirement 6: Batch Execution Engine — Cooperative Stop

**User Story:** As a Temporal operator, I want to stop an in-progress batch operation, so that I can halt a bulk action that is no longer needed or was started in error.

#### Acceptance Criteria

1. THE BatchExecutionEngine SHALL check the CancellationFlag between processing individual workflows.
2. WHEN the CancellationFlag is set, THE BatchExecutionEngine SHALL stop processing further workflows, set the batch operation state to `Completed`, and record `close_time`.
3. WHEN the CancellationFlag is set, THE BatchExecutionEngine SHALL NOT roll back operations that have already been applied to individual workflows.

---

## Phase 3: Lifecycle Handlers

### Requirement 7: stop_batch_operation Handler

**User Story:** As a Temporal SDK user, I want to stop an in-progress batch operation via the `stop_batch_operation` gRPC endpoint, so that I can halt a bulk action.

#### Acceptance Criteria

1. WHEN the `stop_batch_operation` endpoint is called with a valid namespace and job_id for a batch operation in `Running` state, THE handler SHALL set the CancellationFlag for that operation and return a successful response.
2. WHEN the batch operation does not exist, THE handler SHALL return `NOT_FOUND`.
3. WHEN the batch operation is already in a terminal state (`Completed` or `Failed`), THE handler SHALL return a successful response (idempotent stop).
4. THE handler SHALL store the `reason` and `identity` from the stop request in the batch operation entry.

### Requirement 8: describe_batch_operation Handler

**User Story:** As a Temporal SDK user, I want to describe a batch operation via the `describe_batch_operation` gRPC endpoint, so that I can inspect its state, progress, and metadata.

#### Acceptance Criteria

1. WHEN the `describe_batch_operation` endpoint is called with a valid namespace and job_id, THE handler SHALL return the operation type, job_id, state, start_time, close_time, total_operation_count, complete_operation_count, failure_operation_count, identity, and reason.
2. WHEN the batch operation does not exist, THE handler SHALL return `NOT_FOUND`.
3. WHILE the batch operation is in `Running` state, THE handler SHALL return the current progress counts reflecting operations processed so far.
4. WHEN the batch operation is in a terminal state, THE handler SHALL return the final progress counts and `close_time`.

### Requirement 9: list_batch_operations Handler

**User Story:** As a Temporal SDK user, I want to list batch operations in a namespace via the `list_batch_operations` gRPC endpoint, so that I can discover and monitor existing batch operations.

#### Acceptance Criteria

1. WHEN the `list_batch_operations` endpoint is called with a namespace, THE handler SHALL return a paginated list of `BatchOperationInfo` items for all batch operations in that namespace.
2. EACH `BatchOperationInfo` SHALL include `job_id`, `state`, `start_time`, and `close_time`.
3. WHEN the request includes `page_size`, THE handler SHALL return at most that many entries per page.
4. WHEN more entries exist beyond the page, THE handler SHALL return a `next_page_token` that can be used to fetch the next page.
5. WHEN the request includes a `next_page_token`, THE handler SHALL return the next page of results starting after the previous page's last entry.
6. WHEN no batch operations exist in the namespace, THE handler SHALL return an empty list with no next_page_token.
