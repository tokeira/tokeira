# Implementation Plan: Query Dispatch

## Overview

Add a read-only query dispatch mechanism to `tokeira-runtime`. The runtime creates a `QueryTask` with a oneshot response channel, publishes it to a dedicated query tier on the broker, and awaits the worker's response with a configurable timeout. Queries bypass the kernel entirely — no history events, no transitions, no dispatch ops. All changes are confined to `tokeira-runtime`.

## Tasks

- [x] 1. Define QueryTask and QueryResult types
  - [x] 1.1 Create `tokeira/crates/tokeira-runtime/src/query.rs` with `QueryTask` and `QueryResult`
    - `QueryTask` fields: `run_key: RunKey`, `query_type: String`, `query_args: Payloads`, `queue: QueueKey`, `sticky_preferred: Option<WorkerIdentity>`, `response_tx: tokio::sync::oneshot::Sender<QueryResult>`
    - `QueryTask` does NOT implement `Clone` (contains oneshot sender)
    - `QueryResult` is an enum with `Completed { result: Payloads }` and `Failed { message: String }`
    - `QueryResult` derives `Clone`, `Debug`, `PartialEq`
    - `QueryTask` has no `logical_seq` field — query tasks are not part of the durable task chain
    - _Requirements: 2.1, 2.2, 2.4_
  - [x] 1.2 Register `pub mod query;` and `pub use query::*;` in `lib.rs`
    - _Requirements: 2.1_

- [x] 2. Add query channel to the broker
  - [x] 2.1 Extend `BrokerState` with query fields
    - Add `query_ready: HashMap<QueueKey, VecDeque<QueryTask>>` to `BrokerState`
    - Add `query_waiter_counts: HashMap<QueueKey, usize>` to `BrokerState`
    - Add `query_wake: Arc<Notify>` to `InMemoryBroker` (separate from the existing `wake` used for workflow tasks)
    - No dedup set for queries — each query is unique
    - No timestamps on query tasks — no grace window
    - _Requirements: 3.4, 6.4, 7.2, 7.3_
  - [x] 2.2 Implement `publish_query_task` on `InMemoryBroker`
    - Push `QueryTask` into `query_ready` keyed by `task.queue`
    - No dedup check — every query is published
    - Wake query waiters via `query_wake` (NOT the workflow-task `wake`)
    - _Requirements: 3.4, 6.4, 7.2_
  - [x] 2.3 Implement `poll_query_task` on `InMemoryBroker`
    - Accept `queue: &QueueKey`, `worker: &WorkerIdentity`, `wait_for: Duration`
    - Return `Option<QueryTask>` (not `Result` — no fallible path needed)
    - Prefer sticky-matched tasks: scan `query_ready` for entries where `sticky_preferred == Some(worker)`, take first match
    - If no sticky match, take front of queue where `sticky_preferred.is_none()` — do NOT take tasks with a non-matching `sticky_preferred` (skip them, they stay in the queue for the matching worker)
    - Long-poll with `query_wake` + `timeout`, track `query_waiter_counts`
    - _Requirements: 3.4, 3.5, 6.4_

- [x] 3. Checkpoint — Verify broker changes compile and existing tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement `query_workflow` on `TokeiraRuntime`
  - [x] 4.1 Add the `query_workflow` method
    - Signature: `pub async fn query_workflow(&self, execution: ExecutionRef, query_type: String, query_args: Payloads, timeout: Duration) -> Result<QueryResult>`
    - Step 1: `self.repo.resolve_execution(&execution)` → `RunKey`, return `anyhow!("execution not found")` if `None`
    - Step 2: `self.repo.load_run(run_key)` → match on `LoadedRun::Existing(state)`, error if `Absent`
    - Step 3: Build `QueueKey` from `state.namespace_id`, `state.task_queue`, `TaskKind::Workflow`, `state.deployment`, `state.build_id`
    - Step 4: Read `state.sticky` — if `Some(affinity)` and `affinity.expires_at > OffsetDateTime::now_utc()`, set `sticky_preferred = Some(affinity.worker_identity)`, else `None`
    - Step 5: Create `oneshot::channel::<QueryResult>()`
    - Step 6: Build `QueryTask` and call `self.broker.publish_query_task(task)`
    - Step 7: `tokio::time::timeout(timeout, rx).await` — map `Ok(Ok(result))` → `Ok(result)`, `Ok(Err(_))` → channel-closed error, `Err(_)` → timeout error
    - Do NOT submit any `Command` to lanes. Do NOT modify run state.
    - Do NOT reject queries to closed executions — dispatch regardless of `ExecutionStatus`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 4.3, 4.4, 4.5, 5.1, 5.2, 5.5, 8.1, 8.3_

- [x] 5. Property tests for query dispatch
  - [x] 5.1 Write property test: query dispatch does not modify run state
    - **Property 1: Query dispatch produces no transitions**
    - Generate random run state, dispatch a query, assert `transition_seq` and `last_event_id` are unchanged, no `Command` was submitted to any lane, no history events appended
    - **Validates: Requirements 1.4, 1.5, 5.3, 7.1**
  - [x] 5.2 Write property test: QueryTask carries correct metadata
    - **Property 2: QueryTask carries correct metadata from run state**
    - Generate random run state with known fields, dispatch query, intercept the published `QueryTask`, assert `run_key`, `query_type`, `query_args`, and `QueueKey` fields match
    - **Validates: Requirements 2.1, 2.2**
  - [x] 5.3 Write property test: sticky affinity correctly reflected
    - **Property 3: Sticky affinity is correctly reflected on QueryTask**
    - Generate random `StickyAffinity` with `expires_at` in the future or past (or `None`), dispatch query, assert `sticky_preferred` is `Some(worker)` only when affinity is active and not expired
    - **Validates: Requirements 3.1, 3.2, 3.3**
  - [x] 5.4 Write property test: query result round-trip
    - **Property 4: Query result round-trip**
    - Generate random `QueryResult` (both `Completed` and `Failed` variants), send through oneshot channel, assert received result is identical
    - **Validates: Requirements 4.3, 4.4, 4.5**
  - [x] 5.5 Write property test: timeout enforcement
    - **Property 5: Timeout enforcement**
    - Generate random short timeout durations, dispatch query with no worker responding, assert timeout error is returned within a bounded window
    - **Validates: Requirements 5.1, 5.2, 8.2**
  - [x] 5.6 Write property test: concurrent queries independent
    - **Property 6: Concurrent queries are independent**
    - Generate random N (2–8) concurrent queries to the same `RunKey`, complete some and timeout others, assert each query's outcome is independent
    - **Validates: Requirements 6.1, 6.2, 6.3**
  - [x] 5.7 Write property test: query tasks bypass dedup
    - **Property 7: Query tasks bypass dedup**
    - Publish N query tasks for the same `RunKey` to the broker, poll all N, assert all N are delivered (none suppressed)
    - **Validates: Requirements 6.4, 7.2**
  - [x] 5.8 Write property test: queries to closed executions not rejected
    - **Property 8: Queries to closed executions are not rejected at dispatch**
    - Generate random terminal `ExecutionStatus` values, create a run with that status, dispatch query using an `ExecutionRef` with explicit `run_id`, assert no error at dispatch level (query task is published). Note: querying by workflow_id alone without `run_id` will fail at resolution for closed runs — this is expected per the storage contract.
    - **Validates: Requirements 8.1, 8.3, 8.4**

- [x] 6. Checkpoint — Verify all property tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Integration test — end-to-end query dispatch
  - [x] 7.1 Write integration test with mock worker
    - Start runtime with in-memory repo, start a workflow, dispatch a query
    - Spawn a task that polls `poll_query_task`, evaluates the query (returns a canned `QueryResult::Completed`), sends result through the oneshot channel
    - Assert caller receives the expected `QueryResult`
    - Test the timeout path: dispatch a query with no worker polling, assert timeout error
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 5.1, 5.2_

- [x] 8. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property test tasks are REQUIRED (not optional) per project guidance
- `QueryTask` does not implement `Clone` — it contains a `tokio::sync::oneshot::Sender`
- `QueryResult` implements `Clone`, `Debug`, `PartialEq`
- The broker's `query_ready` is not timestamped (no grace window for queries)
- The broker's `query_ready` is not deduplicated (each query is unique)
- `rustfmt max_width = 90` — keep lines within 90 columns
- Follow existing patterns in `broker.rs` and `runtime.rs`
- Each task references specific requirements for traceability
- Property tests use `proptest` (already used in `broker.rs`)
