# Requirements Document

## Introduction

This feature extends the `tokeira-projection` stub (currently 137 lines) into a working visibility layer that materializes SQL-queryable execution rows from committed projection operations. The kernel already emits `ProjectionOp::UpsertExecution` and `ProjectionOp::CloseExecution` on every state-changing transition, and the storage layer provides `ProjectionLog` with cursor-based pagination. The existing `InMemoryVisibilitySink` demonstrates the basic sink pattern but lacks query support, search attribute indexing, pagination, and aggregation.

The scope covers five areas: (1) a visibility sink that consumes `ProjectionOp`s and materializes denormalized execution rows with search attribute side-indexes, (2) a query planner that compiles Temporal-compatible list-filter expressions into executable queries, (3) stable page-token pagination for list queries, (4) count and group-by aggregation with optional rollup acceleration, and (5) an in-memory implementation of the full visibility contract for dev and test use. The design targets both an in-memory dev backend and a future DSQL backend behind the same trait surface.

The projection plane is explicitly NOT on the correctness path. A lagging or temporarily unavailable visibility layer does not affect workflow execution semantics. Sinks are independently checkpointed and replayable.

## Glossary

- **Visibility_Sink**: A `ProjectionSink` implementation that consumes `ProjectionRecord`s from the projection log and materializes denormalized execution rows and search attribute indexes for query use.
- **Visibility_Store**: The storage abstraction behind the visibility sink, responsible for persisting execution rows, search attribute current values, and typed index entries.
- **Query_Planner**: The component that accepts a parsed visibility filter expression and produces an executable query plan against the visibility store.
- **Page_Token**: An opaque, base64-encoded cursor that encodes the last-emitted sort tuple for stable keyset pagination across list queries.
- **Search_Attribute_Registry**: A namespace-scoped mapping from attribute names to stable identifiers and typed descriptors, used by the visibility sink to route attribute values into the correct index structures.
- **Rollup_Planner**: The component that computes signed count deltas for low-cardinality operational dimensions (execution status, workflow type, task queue) as part of each visibility apply cycle.
- **Visibility_Query_Service**: The component that implements the `VisibilityApi` trait (`list_workflows`, `count_workflows`) by delegating to the query planner and visibility store.
- **InMemory_Visibility_Store**: An in-memory implementation of the full visibility contract for dev and test use, replacing the current `InMemoryVisibilitySink` stub.
- **Execution_Row**: A denormalized row representing one workflow execution with system fields (namespace, workflow ID, run ID, workflow type, task queue, status, start time, close time, history length, transition count, memo).
- **Typed_Index**: A per-type side-index table (keyword, keyword list, int, bool, double, datetime, text token) that enables selective predicate evaluation on custom search attributes.
- **ProjectionOp**: The kernel-emitted enum with variants `UpsertExecution` (carrying status, memo patch, search attribute patch) and `CloseExecution` (carrying status and closed-at timestamp).

## Requirements

### Requirement 1: Visibility Sink Materialization

**User Story:** As a platform operator, I want committed workflow transitions to be automatically materialized into queryable execution rows, so that I can list and inspect workflow executions through the Temporal-compatible API.

#### Acceptance Criteria

1. WHEN a `ProjectionRecord` containing a `ProjectionOp::UpsertExecution` is applied, THE Visibility_Sink SHALL create or update the corresponding Execution_Row with the provided execution status, merged memo fields, and merged search attribute values.
2. WHEN a `ProjectionRecord` containing a `ProjectionOp::CloseExecution` is applied, THE Visibility_Sink SHALL update the corresponding Execution_Row to reflect the terminal execution status and the closed-at timestamp.
3. WHEN the same `ProjectionRecord` is applied more than once due to replay or retry, THE Visibility_Sink SHALL produce the same Execution_Row state as a single application (idempotent apply).
4. WHEN a `ProjectionRecord` contains multiple `ProjectionOp`s, THE Visibility_Sink SHALL apply all operations from that record in order within a single logical unit of work.
5. THE Visibility_Sink SHALL populate the Execution_Row system fields (namespace, workflow ID, run ID, workflow type, task queue, start time, execution time, history length, transition count) from the `ProjectionContext` carried on each `ProjectionRecord`. The current `ProjectionRecord` in `tokeira-storage` does not carry this context — it must be extended with a `ProjectionContext` struct (following the pattern established in the prototyping crate) that the storage layer populates from `WorkflowState` at commit time. This is an upstream change to `tokeira-storage::api::ProjectionRecord` and to `InMemoryStore::commit_transition`.

### Requirement 2: Search Attribute Indexing

**User Story:** As a platform operator, I want custom search attributes on workflow executions to be indexed by type, so that I can filter list queries by typed predicates on custom attributes.

#### Acceptance Criteria

1. WHEN a `ProjectionOp::UpsertExecution` includes a non-empty search attribute patch, THE Visibility_Sink SHALL resolve each attribute name against the Search_Attribute_Registry to obtain the attribute descriptor.
2. WHEN a search attribute value is resolved, THE Visibility_Sink SHALL write the current value to the attribute current-value store and insert corresponding entries into the Typed_Index for that attribute type.
3. WHEN a search attribute value is updated for an attribute that already has an indexed value, THE Visibility_Sink SHALL remove the previous Typed_Index entries before inserting the new entries.
4. IF a search attribute name in the patch is not found in the Search_Attribute_Registry, THEN THE Visibility_Sink SHALL return a descriptive error identifying the unknown attribute.
5. IF a search attribute value type does not match the registered attribute type, THEN THE Visibility_Sink SHALL return a descriptive error identifying the type mismatch.
6. THE Visibility_Sink SHALL support all seven search attribute types: Keyword, KeywordList, Int, Bool, Double, Datetime, and Text.
7. WHEN a Text search attribute is indexed, THE Visibility_Sink SHALL tokenize the value into lowercase alphanumeric tokens and index each token separately.

### Requirement 3: Visibility Query Compilation

**User Story:** As a Temporal SDK user, I want to filter workflow executions using Temporal-compatible list-filter expressions, so that I can find specific executions by status, workflow type, time range, or custom search attributes.

#### Acceptance Criteria

1. WHEN a list query with a filter expression is received, THE Query_Planner SHALL parse the filter into a structured expression tree supporting AND, OR, comparison (=, !=, <, <=, >, >=), IN, BETWEEN, and StartsWith operators.
2. THE Query_Planner SHALL support filtering on system fields: WorkflowId, RunId, WorkflowType, TaskQueue, ExecutionStatus, StartTime, CloseTime, HistoryLength, and StateTransitionCount.
3. THE Query_Planner SHALL support filtering on custom search attributes by resolving attribute names through the Search_Attribute_Registry and routing predicates to the appropriate Typed_Index.
4. WHEN a filter references an unknown search attribute, THE Query_Planner SHALL return a descriptive error identifying the unknown attribute name.
5. WHEN a filter literal type does not match the expected type for the target field, THE Query_Planner SHALL return a descriptive error identifying the type mismatch.
6. WHEN no filter expression is provided, THE Query_Planner SHALL return all executions in the namespace subject to pagination and sort order.

### Requirement 4: List Query Pagination

**User Story:** As a Temporal SDK user, I want to page through large result sets of workflow executions using opaque page tokens, so that I can retrieve results incrementally without missing or duplicating entries.

#### Acceptance Criteria

1. THE Visibility_Query_Service SHALL support a configurable page size per list request, capped at a maximum of 1000 results per page.
2. WHEN a list query returns more results than the requested page size, THE Visibility_Query_Service SHALL include a Page_Token in the response that encodes the sort-key tuple of the last returned row.
3. WHEN a list query includes a Page_Token from a previous response, THE Visibility_Query_Service SHALL resume results from the position immediately after the encoded cursor using keyset pagination.
4. THE Visibility_Query_Service SHALL support the following sort orders internally: default (close time descending with nulls first, then start time descending, then run key descending), start time ascending, start time descending, close time ascending, and close time descending. However, the current `ListWorkflowExecutionsRequest` DTO in `tokeira-edge` does not carry a sort-order field, so only the default sort order is selectable via the API until the edge DTO is extended in a future milestone.
5. WHEN the default sort order is used, THE Visibility_Query_Service SHALL return open executions (null close time) before closed executions, with closed executions ordered by close time descending.
6. IF an invalid or corrupted Page_Token is provided, THEN THE Visibility_Query_Service SHALL return a descriptive error indicating the token is malformed.

### Requirement 5: Count and Group-By Queries

**User Story:** As a platform operator, I want to count workflow executions matching a filter and optionally group counts by a dimension, so that I can build operational dashboards and capacity views.

#### Acceptance Criteria

1. WHEN a count query with a filter expression is received, THE Visibility_Query_Service SHALL return the total count of matching executions.
2. WHEN a count query includes a group-by field referencing a system field (ExecutionStatus, WorkflowType, TaskQueue), THE Visibility_Query_Service SHALL return per-group counts for each distinct value of that field.
3. WHEN a count query includes a group-by field referencing a custom search attribute, THE Visibility_Query_Service SHALL return per-group counts for each distinct value of that attribute.
4. WHEN no filter expression is provided for a count query, THE Visibility_Query_Service SHALL count all executions in the namespace.

### Requirement 6: Count Rollup Acceleration

**User Story:** As a platform operator, I want common count queries on low-cardinality dimensions to be fast, so that operational dashboards remain responsive under high execution volumes.

#### Acceptance Criteria

1. WHEN the Visibility_Sink applies a projection record, THE Rollup_Planner SHALL compute signed count deltas for the execution status, workflow type, and task queue dimensions.
2. WHEN an Execution_Row transitions from one status to another, THE Rollup_Planner SHALL emit a negative delta for the previous dimension value and a positive delta for the new dimension value.
3. THE Rollup_Planner SHALL bucket rollup entries by a configurable time window using the execution's close time (or start time for open executions) as the anchor.
4. WHEN a count query targets a rollup-accelerated dimension with no additional filter predicates, THE Visibility_Query_Service SHALL serve the count from rollup aggregates instead of scanning execution rows.

### Requirement 7: VisibilityApi Integration

**User Story:** As a Temporal SDK user, I want the existing `list_workflows` and `count_workflows` edge endpoints to return real results from the projection-backed visibility store, so that the gRPC API returns meaningful execution data.

#### Acceptance Criteria

1. THE Visibility_Query_Service SHALL implement the existing `VisibilityApi` trait with `list_workflows` returning `ListWorkflowExecutionsResponse` containing `WorkflowExecutionSummary` entries populated from Execution_Rows.
2. THE Visibility_Query_Service SHALL implement `count_workflows` returning `CountWorkflowExecutionsResponse` with total count and optional group counts.
3. WHEN `list_workflows` is called with a query string, THE Visibility_Query_Service SHALL parse the query string, compile it through the Query_Planner, execute the plan against the Visibility_Store, and return matching summaries with pagination.
4. WHEN `list_workflows` is called without a query string, THE Visibility_Query_Service SHALL return all executions in the namespace in default sort order with pagination.
5. THE Visibility_Query_Service SHALL map Execution_Row fields to `WorkflowExecutionSummary` fields: namespace, workflow ID, run ID, workflow type, task queue, execution status, start time, and close time.

### Requirement 8: In-Memory Visibility Store

**User Story:** As a developer, I want a fully functional in-memory visibility store for local development and testing, so that I can exercise the complete visibility pipeline without a database.

#### Acceptance Criteria

1. THE InMemory_Visibility_Store SHALL implement the Visibility_Store trait with execution rows, search attribute current values, and typed indexes stored in memory.
2. THE InMemory_Visibility_Store SHALL support the full query planner surface including filter expressions, sort orders, and keyset pagination.
3. THE InMemory_Visibility_Store SHALL support search attribute registration, resolution, and typed index queries.
4. THE InMemory_Visibility_Store SHALL support count queries with group-by on both system fields and custom search attributes.
5. THE InMemory_Visibility_Store SHALL support rollup delta accumulation and rollup-accelerated count queries.
6. WHEN the InMemory_Visibility_Store replaces the existing `InMemoryVisibilitySink`, THE InMemory_Visibility_Store SHALL preserve the existing `get(run_key)` lookup capability for backward compatibility with existing tests.

### Requirement 9: Sink Checkpoint Persistence

**User Story:** As a platform operator, I want the projection worker to persist its cursor independently per sink, so that sinks can resume from their last checkpoint after restart without reprocessing the entire log.

#### Acceptance Criteria

1. THE ProjectionWorker SHALL persist the `ProjectionCursor` to the `VisibilityStore` checkpoint methods after each successfully applied batch. The `ProjectionSink` trait only has `apply(record)` and does not see batch or cursor boundaries — checkpointing is a worker responsibility, not a sink responsibility.
2. WHEN the ProjectionWorker starts or restarts, THE ProjectionWorker SHALL load the last persisted cursor from the `VisibilityStore` and resume reading from that position.
3. WHEN no persisted cursor exists for a sink, THE ProjectionWorker SHALL start reading from the beginning of the projection log partition.
4. THE ProjectionWorker SHALL advance the checkpoint only after the batch has been fully applied to the sink, ensuring at-least-once delivery semantics.

### Requirement 10: Projection Worker Lifecycle

**User Story:** As a platform operator, I want the projection worker to run continuously with backoff and cancellation support, so that visibility stays current during normal operation and shuts down cleanly.

#### Acceptance Criteria

1. THE ProjectionWorker SHALL support a long-running loop that repeatedly reads batches from the projection log and applies them to the sink.
2. WHEN the projection log returns an empty batch, THE ProjectionWorker SHALL wait with exponential backoff before the next read attempt.
3. WHEN a cancellation signal is received, THE ProjectionWorker SHALL complete the current batch application and stop the loop.
4. IF the sink returns an error during batch application, THEN THE ProjectionWorker SHALL log the error and retry with backoff without advancing the checkpoint.
