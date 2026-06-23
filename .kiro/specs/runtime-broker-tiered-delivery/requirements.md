# Requirements Document

Feature: Runtime Broker Tiered Delivery

## Introduction

`tokeira-runtime`'s in-memory broker (`crates/tokeira-runtime/src/broker.rs`) delivers workflow
tasks through a sticky/general readiness model, and the broker module doc already flags the current
shape as provisional: *"split this into explicit sticky/live/backlog tiers once the surrounding
runtime grows"* (`broker.rs:61`). This is tracked as **P6 `runtime-broker-tiered-delivery`** in
`docs/readiness/futures.md`.

A live run of the OpenAI Agents SDK sandbox sample against `tokeirad` exposed a concrete, demo-blocking
defect in the broker's **query** tier that this spec exists to fix as its first slice, alongside
making the tier model explicit:

- The broker holds queries in a separate `query_ready` channel drained only by `poll_query_task` — a
  long-poll **the standard Temporal SDK never calls**. The SDK only calls `PollWorkflowTaskQueue`.
- When a workflow has an in-flight workflow task (WFT), a query is buffered behind a barrier and
  attached to the next WFT poll response (`attach_buffered_queries`) — this works.
- When a workflow is **quiescent** (no pending WFT — the steady state between agent turns), the query
  is published to `query_ready` (`dispatch_queries_direct`) and **never delivered**, because no SDK
  RPC drains that channel. The caller (`QueryWorkflow`) blocks until its deadline and returns
  `query timed out`. Observed in `tokeirad` logs as `num_queries=0` on every `poll_workflow_task_queue`
  response while the TUI's `get_turn_state` poll timed out.

The fix mirrors Temporal: a query to a (cached) worker is returned **on `PollWorkflowTaskQueue`** as a
query-bearing task (a query task token + the `query` field), matched to a poller exactly like a
workflow task, and answered via `RespondQueryTaskCompleted`. Sticky-first with a fallback to the
normal queue.

### Ground truth (v1.31.0)

- `service/history/api/queryworkflow/api.go:350-410 @ v1.31.0` — sticky-first dispatch: if the
  workflow has a live sticky task queue, dispatch the query to the **sticky** queue with a
  `StickyTaskQueueScheduleToStartTimeout`; on sticky timeout / `StickyWorkerUnavailable`, reset the
  sticky queue and **fall back to the normal** queue.
- `service/matching/matching_engine.go:1084 QueryWorkflow` + `createPollWorkflowTaskQueueResponse`
  (~:3055-3100) `@ v1.31.0` — a query is dispatched as a task matched to a waiting poller; the poll
  response, when `task.isQuery()`, carries a **query task token** (`tokenspb.QueryTask{NamespaceId,
  TaskQueue, TaskId}`) and sets `response.Query = task.query.request.QueryRequest.Query`. There is
  **no separate query-poll RPC**.
- Answered via `RespondQueryTaskCompleted` (`matching_engine.go:1154`), keyed by the query task id.

All behaviour claims in this spec are verified against the local checkout at `../temporal` tag
`v1.31.0`; cite the path + tag inline when implementing.

## Scope

### Delivered

- Query delivery to **quiescent** workflows via `PollWorkflowTaskQueue` (the demo-blocking fix).
- Sticky-first query routing with a schedule-to-start fallback to the normal task queue.
- A query-task representation on the edge poll response (legacy single `query` + a query task token)
  and its gRPC mapping to the proto `query` field.
- Reuse of the existing `respond_query_task_completed` + `PendingQueryStore` (`LEGACY_QUERY_ID`)
  machinery to close the loop.
- Explicit broker delivery tiers (sticky / live / backlog) with documented promotion rules,
  consolidating the current ad-hoc sticky/general maps and the separate query channel.

### Deferred / Non-goals

- Durable backlog persistence beyond the current in-memory model (the backlog tier here is the
  in-memory readiness tier, not a storage change).
- The `update`-redelivery defect — **already fixed** (`runtime: don't re-offer an accepted update on
  later workflow tasks`, commit `2565975`); listed here only so the implementer does not re-open it.
- Consistent-query (`queries` map on a real WFT) behaviour — already works via
  `attach_buffered_queries`; this spec must not regress it.
- DSQL / cross-process broker behaviour.

## Glossary

- **WFT** — workflow task.
- **Quiescent run** — a run with no pending workflow task (`state.pending_workflow_task.is_none()`).
- **Query task** — a task that carries a query for a worker to evaluate against cached/replayed state,
  identified by a query task token, answered via `RespondQueryTaskCompleted`.
- **Sticky tier** — tasks reserved for the run's cached worker (the sticky task queue).
- **Live tier** — tasks immediately pollable by any matching worker (today's "general").
- **Backlog tier** — in-memory readiness awaiting a poller / promotion (today's expired-sticky
  promotion path).

## Requirements

### Requirement 1: Deliver queries to quiescent workflows via the workflow-task poll

**User Story:** As an SDK client querying a workflow that is idle between tasks, I want my query
answered, so that consistent/idle queries do not time out.

#### Acceptance Criteria

1. WHEN a `QueryWorkflow` is dispatched for a run with no pending WFT, THE broker SHALL make the query
   available to a worker via `PollWorkflowTaskQueue` (not only via the separate `poll_query_task`
   channel).
2. WHILE a worker is long-polling `PollWorkflowTaskQueue` and a query becomes ready for that run's
   queue, THE poll SHALL wake and return a query-bearing response rather than waiting out its timeout.
3. THE query-bearing poll response SHALL carry a query task token and the query payload such that the
   worker answers via `RespondQueryTaskCompleted`.
4. WHEN the worker answers, THE original `QueryWorkflow` caller SHALL receive the result before its
   deadline (no `query timed out`).
5. THE direct (quiescent) query path SHALL NOT require the standard SDK to call any RPC other than
   `PollWorkflowTaskQueue` and `RespondQueryTaskCompleted`.

### Requirement 2: Sticky-first query routing with normal-queue fallback

**User Story:** As an operator relying on sticky execution, I want queries routed to the cached
worker, so that queries are fast and do not force full-history replays.

#### Acceptance Criteria

1. WHERE a run has a live sticky affinity (sticky worker present and not expired), THE broker SHALL
   route the query to the sticky tier first, matching only the sticky worker.
2. IF the sticky worker does not take the query within the sticky schedule-to-start window, THEN THE
   broker SHALL fall back to delivering the query on the normal (live) tier to any matching worker,
   mirroring `queryworkflow/api.go:377-410 @ v1.31.0`.
3. WHERE a run has no live sticky affinity, THE query SHALL be delivered on the normal tier directly.
4. THE fallback SHALL NOT duplicate the query (at most one worker answers a given query task id).

### Requirement 3: Query-task representation on the poll response

**User Story:** As a maintainer, I want the poll response to model a query task distinctly from a
workflow task, so the gRPC surface matches the Temporal wire contract.

#### Acceptance Criteria

1. THE edge `PollWorkflowTaskQueueResponse` DTO (`crates/tokeira-edge/src/translate/mod.rs`) SHALL
   represent a query task: a single legacy `query` plus a query task token, distinct from the existing
   `queries` map.
2. THE gRPC translation SHALL map this to the proto `PollWorkflowTaskQueueResponse.query` field with a
   serialized query task token, mirroring `createPollWorkflowTaskQueueResponse` `task.isQuery()`
   branch `@ v1.31.0`.
3. A query-task poll response SHALL NOT advance workflow history or carry a started/ scheduled
   workflow-task event id as if it were a real WFT.
4. WHERE the target worker holds the run in its sticky cache, THE response MAY omit full history
   (sticky query); WHERE it does not, THE response SHALL include the history needed for the worker to
   replay and answer.

### Requirement 4: Reuse the existing query-completion machinery

**User Story:** As a maintainer, I want the fix to reuse the built query-response plumbing, so the
change is contained and consistent.

#### Acceptance Criteria

1. THE direct query path SHALL register the caller's `response_tx` in `PendingQueryStore` keyed by the
   query task token (using `LEGACY_QUERY_ID`), consistent with `attach_buffered_queries`.
2. THE existing `respond_query_task_completed` handler SHALL resolve the caller without modification
   beyond what Requirement 3 requires.
3. THE change SHALL NOT regress the consistent-query path (`attach_buffered_queries` /
   `RespondWorkflowTaskCompleted.query_results`).

### Requirement 5: Make the broker delivery tiers explicit

**User Story:** As a maintainer, I want sticky/live/backlog tiers named and documented, so broker
behaviour is understandable without reverse-engineering ad-hoc maps.

#### Acceptance Criteria

1. THE broker SHALL expose explicit sticky / live / backlog tiers (naming the current
   `sticky_ready` / `general_ready` and the expired-sticky promotion path), with the query tier
   integrated into the same poll/match path rather than a separate channel.
2. THE promotion rules SHALL be documented inline: a sticky task whose affinity expires (or has no
   preferred worker) is promoted to the live tier; a query follows the same sticky-first / live
   fallback as Requirement 2.
3. THE refactor SHALL preserve existing workflow-task delivery semantics: no change to which worker
   may take a sticky vs live workflow task, and no double-delivery.

### Requirement 6: Correctness properties

**User Story:** As a maintainer, I want the broker's delivery invariants stated as testable
properties, so the fix is provably correct and stays correct.

#### Acceptance Criteria

1. **No stranded queries:** a query dispatched to a quiescent run with at least one polling worker on
   the matching (sticky or normal) queue SHALL be delivered within the poll/fallback window — never
   left undelivered until timeout.
2. **At-most-once answer:** a given query task id SHALL be answerable by at most one worker; sticky
   fallback to normal SHALL NOT allow two deliveries to both complete.
3. **No update regression:** the accepted-update non-redelivery property (commit `2565975`,
   `accepted_update_is_not_redelivered_as_pending_transport`) SHALL continue to hold.
4. **Ground-truthed:** sticky-first/fallback ordering and the query-task response shape SHALL match
   the cited v1.31.0 sources; deviations SHALL be documented inline with rationale.

### Requirement 7: Tests

**User Story:** As a maintainer, I want regression and property coverage, so the query-delivery fix
and tier behaviour cannot silently regress.

#### Acceptance Criteria

1. A regression test SHALL assert a query to a **quiescent** run is delivered on a `PollWorkflowTaskQueue`
   poll (the current gap) and resolved via `RespondQueryTaskCompleted`.
2. A test SHALL assert sticky-first routing and normal-queue fallback after the sticky window, with
   at-most-once delivery.
3. A test SHALL assert the consistent-query (buffered) path still works (no regression).
4. Property tests SHALL cover at-most-once query answer and tier promotion (sticky-expiry → live).
5. Tests SHALL NOT require Docker, AWS, live DSQL, network access, or the OpenAI API.
