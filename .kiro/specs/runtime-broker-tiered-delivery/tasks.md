# Implementation Plan: Runtime Broker Tiered Delivery

## Overview

Deliver queries to quiescent workflows on the `PollWorkflowTaskQueue` path (fixing the demo-blocking
gap) and make the broker's delivery tiers explicit. Prioritize the query-delivery slice
(Requirements 1-4, 6, 7); the tier-naming refactor (Requirement 5) follows. All behaviour
ground-truthed to v1.31.0 (cite path + tag inline).

> **Status: COMPLETE (2026-06-12).** Implemented by Codex per this plan; verified (see Notes). Task 9
> (operator-run acceptance) passed live as part of the `agentic-walking-skeleton` durability proof —
> `get_turn_state` against a quiescent workflow resolves instead of timing out, and the sticky→live
> fallback held across a hard worker kill mid-turn. Evidence:
> `.kiro/specs/agentic-orchestration/reference/openai-sandbox-gap.md` (Next actions).

## Tasks

- [x] 1. Reproduce the gap (write failing tests first)
  - [x] 1.1 Runtime test: a query dispatched to a **quiescent** run with a polling worker is delivered
    via the workflow-task poll (today it is not). Assert the worker receives a query-bearing poll and
    `RespondQueryTaskCompleted` resolves the caller. This fails against current code.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 7.1_
  - [x] 1.2 Confirm the consistent-query (buffered) path passes today (guard against regression).
    - _Requirements: 4.3, 7.3_

- [x] 2. Query-task representation on the poll response
  - [x] 2.1 Add a query-task variant to `PollWorkflowTaskQueueResponse` in
    `crates/tokeira-edge/src/translate/mod.rs` (single `query: Option<WorkflowQueryDto>` + a query
    task token), distinct from the `queries` map.
    - _Requirements: 3.1, 3.3_
  - [x] 2.2 Map it in `crates/tokeira-edge/src/grpc/translate.rs` to the proto
    `PollWorkflowTaskQueueResponse.query` + a serialized query task token, mirroring
    `createPollWorkflowTaskQueueResponse` `task.isQuery()` @ v1.31.0.
    - _Requirements: 3.1, 3.2_

- [x] 3. Deliver queries on the workflow-task poll
  - [x] 3.1 Choose delivery shape per design §1: (A) unified broker poll result (preferred) or
    (B) edge-level `select!` over `poll_workflow_task` + a new `poll_query`. Document the choice.
    - _Requirements: 1.1, 1.2, 1.5_
  - [x] 3.2 Implement query-task poll assembly: build the response from run state — query task token,
    `query` payload, sticky-cache-aware history (omit full history for sticky targets; include it for
    non-sticky), no started/scheduled WFT event id.
    - _Requirements: 1.3, 3.3, 3.4_
  - [x] 3.3 Register the caller's `response_tx` in `PendingQueryStore` keyed by
    `(query_task_token, LEGACY_QUERY_ID)`, mirroring `attach_buffered_queries`; rely on the existing
    `respond_query_task_completed` to resolve it.
    - _Requirements: 4.1, 4.2_

- [x] 4. Sticky-first routing with normal-queue fallback
  - [x] 4.1 Route a query to the sticky tier first when the run has live sticky affinity; add a
    per-query sticky schedule-to-start deadline.
    - _Requirements: 2.1_
  - [x] 4.2 On sticky-deadline expiry, clear `sticky_preferred` so any matching worker can take the
    query (live fallback), mirroring `queryworkflow/api.go:377-410 @ v1.31.0` and the existing
    expired-sticky → general workflow-task promotion. Preserve at-most-once.
    - _Requirements: 2.2, 2.4, 6.2_
  - [x] 4.3 No sticky affinity → deliver on the live tier directly.
    - _Requirements: 2.3_

- [x] 5. Checkpoint — query delivery green
  - Run `cargo test -p tokeira-runtime -p tokeira-edge`; the task 1.1 repro now passes; 1.2 still passes.

- [x] 6. Make broker tiers explicit (refactor)
  - [x] 6.1 Name the sticky / live / backlog tiers in `broker.rs`, folding query readiness into the
    same poll/match path; update the module doc (remove the "TODO: split into tiers" note) to describe
    the realized model and promotion rules.
    - _Requirements: 5.1, 5.2_
  - [x] 6.2 Preserve workflow-task delivery semantics: identical sticky-vs-live take rules, no
    double-delivery.
    - _Requirements: 5.3, 6.4_

- [x] 7. Property + regression tests
  - [x] 7.1 Property: at-most-once query answer (sticky→live fallback never double-delivers).
    - _Requirements: 6.2, 7.4_
  - [x] 7.2 Property: tier promotion (sticky-expiry → live) for both workflow tasks and queries.
    - _Requirements: 5.2, 7.4_
  - [x] 7.3 Regression: `accepted_update_is_not_redelivered_as_pending_transport` still passes; the
    consistent-query buffered path still passes.
    - _Requirements: 6.3, 4.3, 7.3_

- [x] 8. Checkpoint — full suite, lint, fmt
  - `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`,
    `cargo test -p tokeira-runtime -p tokeira-edge`.

- [x] 9. Acceptance (operator-run, not a unit test)
  - Boot `tokeirad`, run the OpenAI Agents SDK sandbox sample (local backend) + TUI; confirm a turn
    completes and `get_turn_state` polling no longer times out. (Owned by `agentic-orchestration`.)
    **Passed live 2026-06-12** during the walking-skeleton durability proof.

## Notes

- The `update`-redelivery defect is **already fixed** (commit `2565975`); do not re-open it — only
  keep its regression test green.
- Verified by Codex: `cargo check -p tokeira-runtime -p tokeira-edge`; `cargo +nightly fmt --all --check`;
  `cargo test -p tokeira-runtime query`; `cargo test -p tokeira-runtime
  workflow_poll_falls_back_when_sticky_query_deadline_elapses`; `cargo test -p tokeira-edge
  workflow_poll_response_projects_legacy_query_field`.
- Ground truth lives in the local checkout `../temporal` @ `v1.31.0`; cite path + tag inline for the
  query-routing and query-task-response decisions.
- Prefer runtime/broker-level tests (deterministic, no service harness) for the core properties; one
  edge service test covers the end-to-end poll→respond loop.
- No tests may require Docker, AWS, live DSQL, network, or the OpenAI API.
- Kernel stays untouched: this is `tokeira-runtime` + `tokeira-edge` only.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] },
    { "id": 5, "tasks": ["6.1", "6.2"] },
    { "id": 6, "tasks": ["7.1", "7.2", "7.3"] },
    { "id": 7, "tasks": ["8", "9"] }
  ]
}
```
