# Requirements Document: Query Dispatch

## Introduction

This document captures the requirements for Feature 13 (Query Dispatch) of the Tokeira runtime. Queries are read-only operations that allow callers to inspect the current state of a workflow without modifying it. Unlike signals, updates, or commands, queries do NOT produce history events, transitions, or dispatch ops. They bypass the kernel entirely and are a runtime-only concern.

The query dispatch mechanism reuses the existing workflow task delivery infrastructure. The runtime creates a query task, dispatches it to the broker (preferring a sticky worker that already has the workflow state cached), and waits for the worker to return a result via a oneshot channel. A configurable timeout prevents unresponsive workers from blocking callers indefinitely.

Key architectural constraints:
- Queries bypass the kernel — they are a runtime-only concern.
- Queries do not modify authoritative state.
- Queries are routed to workers via the existing broker, preferring sticky affinity.
- Query dispatch is a request-response pattern with a configurable timeout.
- Multiple concurrent queries to the same run are independent and do not interfere with each other.

Depends on: Feature 1 (Lane OCC Retry).

The authoritative specifications are [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md) and [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md).

## Glossary

- **Runtime**: The execution shell (`TokeiraRuntime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Broker**: The in-memory workflow-task delivery subsystem (`InMemoryBroker`). Implements sticky and general tiers for task routing.
- **Query**: A read-only request to inspect the current state of a workflow execution. Identified by a query type name and serialized arguments.
- **Query_Task**: A special-purpose workflow task created by the Runtime to deliver a query to a worker. The Query_Task carries the query type, arguments, and a response channel. It does not produce history events or transitions.
- **Query_Result**: The serialized response returned by a worker after evaluating a query against the workflow's current state.
- **Response_Channel**: A oneshot channel created by the Runtime for each query dispatch. The worker sends the Query_Result (or an error) through this channel, and the Runtime awaits it.
- **Sticky_Worker**: A worker that has the target workflow's state cached in memory due to recent execution. The broker tracks sticky affinity via `sticky_preferred` on dispatchable tasks.
- **ExecutionRef**: A composite reference (`namespace_id`, `workflow_id`, optional `run_id`) used by the edge layer to identify a target workflow execution.
- **RunKey**: The durable storage key for a specific run, resolved from an ExecutionRef via the repository.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **Query_Timeout**: The configurable maximum duration the Runtime waits for a worker to return a Query_Result before returning a timeout error to the caller.

## Requirements

---

### Requirement 1: Query Dispatch Method

**User Story:** As a Tokeira developer, I want the runtime to expose a query dispatch method, so that callers can inspect workflow state without modifying it.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `query_workflow` method that accepts an ExecutionRef, a query type name (string), a serialized query arguments payload, and a Query_Timeout duration.
2. WHEN `query_workflow` is called, THE Runtime SHALL resolve the ExecutionRef to a RunKey via the repository.
3. IF the ExecutionRef cannot be resolved to a RunKey, THEN THE Runtime SHALL return an error indicating the execution was not found.
4. THE Runtime SHALL NOT submit queries as commands to the Kernel. Queries do not produce transitions, history events, or dispatch ops.
5. THE Runtime SHALL NOT produce transitions, history events, or dispatch ops as a result of query dispatch. Storage-side housekeeping (such as clearing expired sticky affinity during `load_run`) is not considered a query mutation.

---

### Requirement 2: Query Task Creation

**User Story:** As a Tokeira developer, I want the runtime to create a query task for each query dispatch, so that the query can be delivered to a worker through the existing broker infrastructure.

#### Acceptance Criteria

1. WHEN the Runtime dispatches a query, THE Runtime SHALL create a Query_Task containing the target RunKey, the query type name, the serialized query arguments, and a Response_Channel sender.
2. THE Query_Task SHALL carry the QueueKey of the target run's task queue, so that the broker can route it to a compatible worker.
3. THE Runtime SHALL create a new Response_Channel (oneshot channel) for each query dispatch. The sender half is attached to the Query_Task; the receiver half is retained by the caller.
4. THE Query_Task SHALL NOT carry a `logical_seq` or participate in the workflow task sequence numbering. Query tasks are not part of the durable task chain.

---

### Requirement 3: Sticky-Preferred Query Routing

**User Story:** As a Tokeira developer, I want queries to be routed preferentially to the sticky worker, so that queries are answered quickly by a worker that already has the workflow state cached.

#### Acceptance Criteria

1. WHEN the Runtime creates a Query_Task, THE Runtime SHALL look up the current sticky worker affinity for the target run from the repository.
2. WHEN a sticky worker affinity exists and has not expired, THE Runtime SHALL set the `sticky_preferred` field on the Query_Task to that worker identity.
3. WHEN no sticky worker affinity exists or the affinity has expired, THE Runtime SHALL create the Query_Task without a `sticky_preferred` hint, allowing the broker to route it to any compatible poller.
4. THE Broker SHALL use a dedicated query notification channel (separate `Notify`) for query task wakeups, so that query publications do not cause spurious wakeups on workflow-task long-polls and vice versa.
5. WHEN a query task has `sticky_preferred = Some(worker)` and the polling worker does not match, THE Broker SHALL skip that task (same behavior as the workflow broker's sticky tier). The task remains in the query queue for the matching worker or until the caller's timeout expires and the oneshot channel is dropped.

---

### Requirement 4: Query Result Delivery

**User Story:** As a Tokeira developer, I want the worker to return query results through a response channel, so that the runtime can deliver the result back to the caller synchronously.

#### Acceptance Criteria

1. WHEN a worker receives a Query_Task, THE worker SHALL evaluate the query against the workflow's current state and send the Query_Result through the Response_Channel sender.
2. WHEN the worker encounters an error evaluating the query, THE worker SHALL send an error through the Response_Channel sender.
3. THE Runtime SHALL await the Response_Channel receiver until either a result is received or the Query_Timeout expires.
4. WHEN the Response_Channel receiver yields a Query_Result, THE Runtime SHALL return the result to the caller.
5. WHEN the Response_Channel receiver yields an error, THE Runtime SHALL propagate the error to the caller.

---

### Requirement 5: Query Timeout Handling

**User Story:** As a Tokeira developer, I want query dispatch to enforce a configurable timeout, so that unresponsive workers do not block query callers indefinitely.

#### Acceptance Criteria

1. THE Runtime SHALL enforce the Query_Timeout duration on each query dispatch, starting from when the Query_Task is published to the broker.
2. WHEN the Query_Timeout expires before a result is received on the Response_Channel, THE Runtime SHALL return a timeout error to the caller.
3. THE Runtime SHALL NOT modify run state or create transitions as a result of a query timeout.
4. WHEN a query times out, THE Runtime SHALL drop the Response_Channel receiver. If the worker later sends a result, the send will fail silently (the oneshot sender observes a closed channel).
5. THE Query_Timeout SHALL be configurable per query dispatch call. The caller provides the timeout as a parameter to `query_workflow`.

---

### Requirement 6: Concurrent Queries to the Same Run

**User Story:** As a Tokeira developer, I want multiple concurrent queries to the same run to be independent, so that one slow query does not block or interfere with another.

#### Acceptance Criteria

1. THE Runtime SHALL support multiple concurrent `query_workflow` calls targeting the same RunKey.
2. EACH concurrent query SHALL have its own independent Response_Channel and Query_Timeout.
3. THE Runtime SHALL NOT serialize concurrent queries to the same run. Each query is dispatched independently through the broker.
4. THE Broker SHALL NOT deduplicate Query_Tasks. Unlike regular workflow tasks (which are deduplicated by `(run_key, logical_seq)`), each Query_Task is a unique dispatch.

---

### Requirement 7: Query Task Lifecycle Isolation

**User Story:** As a Tokeira developer, I want query tasks to be fully isolated from the normal workflow task lifecycle, so that queries cannot corrupt workflow state or interfere with command processing.

#### Acceptance Criteria

1. THE Runtime SHALL NOT record query dispatch or query results in the run's history.
2. THE Runtime SHALL NOT include Query_Tasks in the broker's deduplication set (`enqueued`). Query_Tasks do not participate in the `(run_key, logical_seq)` deduplication mechanism.
3. THE Runtime SHALL NOT include Query_Tasks in the durable backlog lifecycle. Query_Tasks are transient and are not persisted to Durable_Backlog by the Grace_Scanner.
4. THE Runtime SHALL NOT include Query_Tasks in the sweeper's recovery scope. If the runtime restarts, in-flight queries are lost; callers receive a channel-closed error or timeout and may retry.
5. WHEN a Query_Task is delivered to a worker, THE worker SHALL NOT produce workflow commands (schedule activity, start timer, etc.) as a result of evaluating the query. The query handler is read-only.

---

### Requirement 8: Query Dispatch for Closed Executions

**User Story:** As a Tokeira developer, I want queries to closed (completed, failed, terminated, cancelled) executions to be handled gracefully, so that callers receive a clear error rather than hanging indefinitely.

#### Acceptance Criteria

1. WHEN `query_workflow` is called with an ExecutionRef that includes a specific `run_id` and that run has reached a terminal state, THE Runtime SHALL still attempt to dispatch the query to a worker (the worker may have the final state cached).
2. IF no worker can answer the query for a closed execution within the Query_Timeout, THEN THE Runtime SHALL return a timeout error to the caller.
3. THE Runtime SHALL NOT reject queries to closed executions at the dispatch level when the run can be resolved. Whether a closed execution can be queried depends on worker cache availability, not on execution status.
4. WHEN `query_workflow` is called with an ExecutionRef that omits `run_id` (resolves by namespace + workflow_id only), THE current `resolve_execution` contract returns only the current open run. If the execution is closed and no open run exists, resolution returns `None` and the query fails with "execution not found." Querying a closed execution by workflow_id alone requires the caller to provide the specific `run_id`.
