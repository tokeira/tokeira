# Design: Runtime Broker Tiered Delivery

## Overview

Make `tokeira-runtime`'s broker deliver **queries on the same poll path as workflow tasks**, fixing
the demo-blocking gap where a query to a quiescent workflow is stranded in a side channel the SDK
never polls. Along the way, name the broker's delivery tiers (sticky / live / backlog) explicitly.

This is the P6 `runtime-broker-tiered-delivery` slice. The dominant, prioritized change is query
delivery (Requirements 1-4, 6, 7); the tier-naming refactor (Requirement 5) is the cleanup that
makes the query integration coherent.

## Current state (what exists)

- **Workflow-task delivery** — `broker.rs` keeps `sticky_ready` and `general_ready` maps keyed by
  `QueueKey`. `poll_workflow_task` long-polls, prefers a sticky task matching the worker, promotes
  expired-sticky / no-preferred tasks to general. This works.
- **Consistent queries (WFT in flight)** — `WorkflowService::poll_workflow_task_queue` →
  `decorate_workflow_task_response` → `attach_buffered_queries` drains `BufferedQueryRegistry` queries
  whose barrier is satisfied and attaches them to the WFT poll response's `queries` map; the worker
  answers via `RespondWorkflowTaskCompleted.query_results`, routed back through `PendingQueryStore`.
  This works.
- **Direct queries (quiescent run)** — `runtime/query.rs::query_workflow` publishes a `QueryTask` to
  the broker (`publish_query_task` → `query_ready`) when there is no pending WFT, and
  `dispatch_queries_direct` does likewise for barrier-satisfied buffered queries on quiescent runs.
  `query_ready` is drained only by `poll_query_task`.
- **`respond_query_task_completed`** + `PendingQueryStore` + `LEGACY_QUERY_ID` — the completion side
  for single (legacy) query tasks. Present and functional, but nothing currently produces a poll
  response that routes a worker to answer via it for the direct path.

## The gap

`query_ready` is drained only by `poll_query_task`. The standard Temporal SDK has **no query-poll
RPC** — it calls `PollWorkflowTaskQueue` only. So a direct query sits in `query_ready` undelivered
until the caller's deadline → `query timed out`. Confirmed in `tokeirad` logs: `num_queries=0` on
every `poll_workflow_task_queue` response while `get_turn_state` timed out, with the worker idle on a
sticky queue (`<pid>@<host>-<uuid>`).

## Ground truth (v1.31.0)

| Concern | Source | Behaviour |
|---|---|---|
| Query routing | `service/history/api/queryworkflow/api.go:350-410` | sticky-first (with `StickyTaskQueueScheduleToStartTimeout`); on sticky timeout / `StickyWorkerUnavailable`, reset sticky and fall back to normal queue |
| Query as a matched task | `service/matching/matching_engine.go:1084` `QueryWorkflow` → `DispatchQueryTask` | a query is dispatched as a task matched to a waiting workflow poller; no separate query-poll RPC |
| Poll response for a query | `matching_engine.go createPollWorkflowTaskQueueResponse` (`task.isQuery()`) | sets a **query task token** (`tokenspb.QueryTask{NamespaceId, TaskQueue, TaskId}`) and `response.Query`; carries history for replay |
| Completion | `matching_engine.go:1154` `RespondQueryTaskCompleted` | result delivered to the caller keyed by query task id |

## Architecture

### 1. Unify query delivery into the workflow-task poll

Two viable shapes; **prefer (A)** for fidelity to Temporal, fall to (B) if (A) proves too invasive:

- **(A) Broker-level (preferred):** `poll_workflow_task` returns a unified poll result that is either
  a workflow task or a query task. When no WFT is ready but a query is ready for the polled queue
  (sticky-matched or live), return the query task. This matches Temporal's single matched-task model
  and naturally fits the tier refactor (Requirement 5).
- **(B) Edge-level race:** the edge `poll_workflow_task_queue` runs `runtime.poll_workflow_task` and a
  new `runtime.poll_query(queue, worker, timeout)` (wrapping the broker's existing `poll_query_task`)
  concurrently via `tokio::select!`, returning whichever resolves first. Smaller blast radius; keeps
  the broker's WFT return type unchanged.

Either way, the edge must build a **query-task poll response** when a query wins.

### 2. Query-task poll response (Requirement 3)

Extend the edge `PollWorkflowTaskQueueResponse` DTO with a query-task variant:

```rust
// crates/tokeira-edge/src/translate/mod.rs
pub struct PollWorkflowTaskQueueResponse {
    // ... existing fields ...
    /// Present when this poll delivers a legacy single query task rather than a
    /// workflow task. Mirrors PollWorkflowTaskQueueResponse.query @ v1.31.0.
    pub query: Option<WorkflowQueryDto>,
}
```

- The gRPC layer maps `query` to the proto `PollWorkflowTaskQueueResponse.query` and the `task_token`
  to a serialized **query task token** (a tokeira analog of `tokenspb.QueryTask{namespace, task_queue,
  task_id}`). The worker answers via `RespondQueryTaskCompleted` carrying that token.
- For a **sticky** target (worker holds the run cached — the common case), the response may omit full
  history; `previous_started_event_id` + the query is sufficient (sticky query). For a **non-sticky**
  target, include history for replay (reuse the existing history-assembly used by
  `from_internal::poll_response`).

## Data Models

The only new data shape is the query-task representation on the poll response (above):

- `PollWorkflowTaskQueueResponse.query: Option<WorkflowQueryDto>` — set iff the poll delivers a query
  task; mutually exclusive with a real workflow task. The existing `queries: HashMap<..>` map stays
  for the consistent-query (buffered, on-WFT) path and is unchanged.
- **Query task token** — a serializable token identifying `(namespace_id, task_queue, query_task_id)`,
  the tokeira analog of `tokenspb.QueryTask @ v1.31.0`. It is what `RespondQueryTaskCompleted` echoes;
  `PendingQueryStore` is keyed by `(token, LEGACY_QUERY_ID)`.
- No kernel state, no history-event, and no storage schema changes: a query task carries no durable
  state and writes no history.

### 3. Completion routing (Requirement 4)

On producing the query-task response, register the caller's `response_tx` in `PendingQueryStore`
keyed by `(query_task_token, LEGACY_QUERY_ID)` — exactly as `attach_buffered_queries` does. The
existing `respond_query_task_completed` handler resolves it. No new completion plumbing.

### 4. Sticky-first with fallback (Requirement 2)

The broker's `poll_query_task`/`try_take_query` already prefer a sticky-matched query and otherwise a
non-sticky one (`broker.rs:536`). What's missing is the **fallback timer**: a query routed sticky must
become live-deliverable after a sticky schedule-to-start window if the sticky worker doesn't take it.
Model this with a per-query `sticky_deadline`; on expiry, the query's `sticky_preferred` is cleared so
any matching worker can take it (mirroring the workflow-task expired-sticky → general promotion, and
`queryworkflow/api.go` sticky→non-sticky fallback). At-most-once is preserved because a single
`query_ready` entry is removed on take.

### 5. Explicit tiers (Requirement 5)

Rename/group the broker's readiness into named tiers and fold query readiness into the same
poll/match path:

- **sticky** — `sticky_ready` (workflow tasks) + sticky-preferred queries; takeable only by the
  matching worker until the sticky deadline.
- **live** — `general_ready` (workflow tasks) + queries past their sticky deadline / with no preferred
  worker; takeable by any matching worker.
- **backlog** — the in-memory readiness awaiting a poller (no behavioural change; this names the
  current model, not a storage change).

Keep the refactor behaviour-preserving for workflow tasks; the only new delivery is queries riding the
poll. Update the broker module doc (which currently says "TODO: split into sticky/live/backlog tiers")
to describe the realized model.

## Components and Interfaces

| File | Change |
|---|---|
| `crates/tokeira-runtime/src/broker.rs` | tier naming; surface query readiness on the workflow poll (shape A) or expose query long-poll (shape B); sticky fallback timer |
| `crates/tokeira-runtime/src/runtime/workflow_task.rs` / `query.rs` | unified poll result (A) or `poll_query` wrapper (B); query-task assembly inputs (run state, history-for-replay) |
| `crates/tokeira-edge/src/workflow_service.rs` | `poll_workflow_task_queue`: deliver a query-task response; register `pending_queries`; `WorkflowRuntimeApi` poll-query method if shape B |
| `crates/tokeira-edge/src/translate/mod.rs` | `PollWorkflowTaskQueueResponse.query` (query-task variant) + query task token |
| `crates/tokeira-edge/src/grpc/translate.rs` | map the query-task variant to the proto `query` field + serialized query task token |
| `crates/tokeira-edge/src/grpc/runtime_adapter.rs` | implement any new `WorkflowRuntimeApi` method |
| (reuse) `respond_query_task_completed`, `PendingQueryStore`, `LEGACY_QUERY_ID` | unchanged |

## Correctness Properties

### Property 1: No stranded queries

**Validates: Requirements 1.1, 6.1**

With a polling worker on the matching queue, a quiescent-run query is delivered within the
poll/fallback window — never left until the caller deadline because the SDK does not poll a side
channel.

### Property 2: At-most-once answer

**Validates: Requirements 2.4, 6.2**

One `query_ready` entry → one take → one completion; a sticky→live fallback re-routes the same entry
and never duplicates it.

### Property 3: No update regression

**Validates: Requirements 6.3**

`accepted_update_is_not_redelivered_as_pending_transport` continues to pass.

### Property 4: Workflow-task delivery unchanged

**Validates: Requirements 5.3, 6.4**

Sticky-vs-live take rules for workflow tasks are preserved; no double-delivery.

## Error Handling

- **Sticky worker unavailable / sticky timeout** — clear `sticky_preferred` and fall back to live
  delivery (mirrors `StickyWorkerUnavailable` → non-sticky in `queryworkflow/api.go @ v1.31.0`); never
  fail the query for sticky absence alone.
- **No poller before the caller deadline** — the query remains the caller's existing timeout path
  (`query timed out`); the fix removes the *structural* strand (SDK never polls the side channel), not
  the legitimate no-worker timeout.
- **Worker error on a query task** — surfaced through the existing `respond_query_task_completed` /
  `QueryResult::Failed` path; the caller receives the failure rather than blocking.
- **Run closed/absent during dispatch** — return the existing not-found/closed error to the caller; do
  not enqueue a query for a gone run.

## Testing strategy

- **Runtime/broker unit + property tests** (preferred level — deterministic, no service harness):
  quiescent-run query delivered on the workflow poll; sticky-first then live fallback; at-most-once;
  tier promotion (sticky-expiry → live).
- **Edge service test**: `poll_workflow_task_queue` returns a query-task response for a quiescent run
  and `respond_query_task_completed` resolves the caller.
- **Regression**: consistent-query (buffered) path unchanged; update non-redelivery unchanged.
- No Docker/AWS/DSQL/network/OpenAI. The OpenAI sandbox sample is the *acceptance* check (run by the
  operator), not a unit test.

## Validation / definition of done

- `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint` clean.
- `cargo test -p tokeira-runtime` and `cargo test -p tokeira-edge` green, including the new tests.
- Manual acceptance: the OpenAI Agents SDK sandbox sample (local backend) drives a turn and
  `get_turn_state` polling no longer times out against `tokeirad`.
