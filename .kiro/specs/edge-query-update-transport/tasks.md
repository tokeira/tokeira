# Implementation Plan: Edge Query & Update Transport

## Overview

Wire the runtime's existing query dispatch (`QueryTask`, `QueryResult`) and update lifecycle (`UpdateRegistry`, `UpdateOutcome`) through the edge/gRPC layer so that queries and updates flow end-to-end between SDK clients and workers via the standard Temporal protocol. Work spans `tokeira-edge` (primary) and `tokeira-runtime` (BufferedQueryRegistry, UpdateRegistry extension).

The implementation has two phases:
- **Phase 1 (complete):** Transport plumbing — DTOs, proto translation, PendingQueryStore, update message wiring, legacy query support, PollWorkflowExecutionUpdate.
- **Phase 2 (in progress):** Consistent query correctness — BufferedQueryRegistry with read barriers, barrier-gated piggybacking, post-completion dispatch, eager return gated by authoritative run state.

## Phase 1 Tasks (Complete)

- [x] 0. Runtime prerequisites
  - [x] 0.1 Extend `UpdateRegistryEntry` to retain `input`, `identity`, and `update_name`
    - _Requirements: 6.2_
  - [x] 0.2 Add combined query/WFT poll to the broker
    - _Requirements: 2.1_ (superseded by Phase 2 — queries no longer delivered via broker poll)
  - [x] 0.3 Add `drain_pending_updates` method to `UpdateRegistry`
    - _Requirements: 6.1, 6.4_

- [x] 1. Create `PendingQueryStore` and new edge DTO types
  - [x] 1.1 Create `pending_queries.rs` module with `PendingQueryStore`
    - _Requirements: 2.6, 3.1_
  - [x] 1.2 Add new DTO types to `translate/mod.rs`
    - _Requirements: 8.1, 8.2, 8.3, 8.4_
  - [x] 1.3 Extend `PollWorkflowTaskQueueResponse` DTO with query and message fields
    - _Requirements: 8.1, 8.2_
  - [x] 1.4 Extend `RespondWorkflowTaskCompletedRequest` DTO with query_results and messages fields
    - _Requirements: 8.3, 8.4_
  - [ ]* 1.5 Write property test for PendingQueryStore insert/take round-trip (Property 3)
    - **Validates: Requirements 2.6, 3.1**

- [x] 2. Checkpoint — Ensure all tests pass

- [x] 3. Wire query transport into poll and completion paths
  - [x] 3.1 Add `PendingQueryStore` and broker access to `WorkflowService`
    - _Requirements: 2.6_
  - [x] 3.2 Wire query draining into `poll_workflow_task_queue`
    - _Requirements: 2.1, 2.2, 2.5, 2.6_ (to be updated in Phase 2 for barrier gating)
  - [x] 3.3 Wire query result routing into `respond_workflow_task_completed`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_
  - [ ]* 3.4 Write property test for query attachment preserves fields (Property 2)
    - **Validates: Requirements 2.5, 8.1**
  - [ ]* 3.5 Write property test for query result routing delivers correct results (Property 4)
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.5**

- [x] 4. Wire proto translation for query fields
  - [x] 4.1 Extend `poll_response_to_proto` to populate `queries` map (field 14)
    - _Requirements: 10.1_
  - [x] 4.2 Extend `respond_completed_request_to_edge` to extract `query_results` (field 8)
    - _Requirements: 10.2, 10.3, 10.4_
  - [ ]* 4.3 Write property test for query proto round-trip (Property 8)
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

- [x] 5. Implement legacy `RespondQueryTaskCompleted`
  - [x] 5.1 Implement `respond_query_task_completed` edge method and gRPC handler
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [x] 6. Checkpoint — Ensure all tests pass

- [x] 7. Wire update transport into poll and completion paths
  - [x] 7.1 Wire update message construction into `poll_workflow_task_queue`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_
  - [x] 7.2 Wire update response routing into `respond_workflow_task_completed`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  - [ ]* 7.3 Write property test for update message construction preserves fields (Property 6)
    - **Validates: Requirements 6.1, 6.2**
  - [ ]* 7.4 Write property test for update response routing delivers correct resolution (Property 7)
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5**

- [x] 8. Wire proto translation for update message fields
  - [x] 8.1 Extend `poll_response_to_proto` to populate `messages` field (field 15)
    - _Requirements: 11.1_
  - [x] 8.2 Extend `respond_completed_request_to_edge` to extract `messages` (field 11)
    - _Requirements: 11.2, 11.4_
  - [ ]* 8.3 Write property test for update message proto round-trip (Property 9)
    - **Validates: Requirements 11.1, 11.2, 11.3, 11.4**

- [x] 9. Checkpoint — Ensure all tests pass

- [x] 10. Update UNSUPPORTED_FIELDS.md
  - _Requirements: 12.1, 12.2, 12.3, 12.4_

- [x] 11. Fix query-only WFT to include workflow history
  - [x] 11.1 Ensure query-only poll responses include the workflow's committed history
    - _Requirements: 13.1_ (superseded by Phase 2 — barrier-gated delivery replaces this workaround)

- [x] 12. Implement `PollWorkflowExecutionUpdate` long-poll
  - [x] 12.1 Implement `poll_workflow_execution_update` edge method and gRPC handler
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5_

## Phase 2 Tasks (Consistent Query Correctness)

- [x] 14. Implement `BufferedQueryRegistry`
  - [x] 14.1 Create `buffered_queries.rs` module in `tokeira-runtime`
    - Implement `BufferedQuery` struct with `query_id`, `query_type`, `query_args`, `required_barrier: i64`, `response_tx: oneshot::Sender<QueryResult>`
    - Implement `BufferedQueryRegistry` with `Arc<Mutex<HashMap<RunKey, VecDeque<BufferedQuery>>>>`
    - Implement `buffer(run_key, query) -> Result<(), BufferedQuery>` with per-run bounded count (256)
    - Implement `drain_satisfied(run_key, observable_barrier) -> Vec<BufferedQuery>` — returns queries where `required_barrier ≤ observable_barrier`, leaves others
    - Implement `drain_all(run_key) -> Vec<BufferedQuery>`
    - Implement `remove(run_key, query_id) -> Option<BufferedQuery>` for timeout cleanup
    - Implement `has_buffered(run_key) -> bool`
    - Export from runtime crate
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 14.2 Write property test for barrier-gated drain (Property 1)
    - **Property 1: Query barrier consistency**
    - Generate random `(required_barrier, observable_barrier)` pairs, buffer queries, drain with various barriers, verify only queries with `required_barrier ≤ observable_barrier` are returned
    - **Validates: Requirements 1.1, 2.1, 2.2, 2.3**

  - [x] 14.3 Write property test for bounded count enforcement
    - Buffer 257 queries for a single run, verify the 257th is rejected
    - **Validates: Requirement 1.4**

- [x] 15. Rewire `query_workflow` to use `BufferedQueryRegistry`
  - [x] 15.1 Update `TokeiraRuntime::query_workflow` to use the registry
    - Read authoritative run state (pending_workflow_task, last_event_id)
    - Capture `required_barrier = last_event_id`
    - If no pending/started WFT and run has completed ≥1 WFT: dispatch through direct query-only path (publish to broker query queue for matching delivery)
    - Otherwise: buffer in `BufferedQueryRegistry`, await oneshot
    - Remove `ScheduleQueryTask` call from this path
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 15.2 Add `BufferedQueryRegistry` to `WorkflowService` and `RuntimeAdapter`
    - Thread the registry through constructors
    - Update all call sites (main.rs, tests)
    - _Requirements: 1.1_

- [x] 16. Rewire poll response construction for barrier-gated piggybacking
  - [x] 16.1 Update `poll_workflow_task_queue` to drain from `BufferedQueryRegistry`
    - After building the poll response, determine `observable_barrier` = last event ID in the response history
    - Call `registry.drain_satisfied(run_key, observable_barrier)` to get queries whose barrier is met
    - For each drained query: generate UUID query ID, store `response_tx` in `PendingQueryStore`, add to `queries` map
    - Remove the broker-based query draining loop (the `poll_query_task` calls)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

- [x] 17. Rewire post-completion dispatch
  - [x] 17.1 Update `respond_workflow_task_completed` for barrier-gated dispatch
    - After committing the WFT completion, read authoritative post-completion run state
    - If run is quiescent (no pending/started WFT):
      - If `return_new_workflow_task` is true and registry has buffered queries: build eager return (empty history, started_event_id=0, attach queries with satisfied barriers)
      - Else if registry has buffered queries: dispatch through direct query-only path (publish to broker for matching)
    - If new WFT was created: queries stay buffered
    - Remove the unconditional eager return and `submit_schedule_query_task` call
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 5.4_

  - [x] 17.2 Write property test for post-completion quiescence check (Property 5)
    - **Property 5: Post-completion quiescence check**
    - Generate random completion outcomes (with/without new WFT), verify buffered queries are dispatched only when quiescent
    - **Validates: Requirements 4.1, 4.2, 4.3, 5.4**

- [x] 18. Remove broker-based query buffering
  - [x] 18.1 Remove `publish_query_task` from `query_workflow` for the buffered path
    - The broker query queue is still used for direct query-only dispatch (quiescent path), but NOT for buffering queries that need to wait for a WFT
    - _Requirements: 1.1, 1.2_

  - [x] 18.2 Remove `poll_workflow_or_query_task` from runtime
    - Replace with `poll_workflow_task` (real WFTs only)
    - Update edge layer to use `poll_workflow_task` directly
    - Clean up `PolledWorkflowTaskTransport::QueryOnly` variant if no longer needed
    - _Requirements: 2.4_

- [ ] 19. Checkpoint — Ensure all tests pass
  - Run `cargo test` in the `tokeira` workspace
  - Run `cargo lint`
  - Validate with `message_passing` example: signal(5) then query returns 5

- [ ] 20. Final checkpoint — End-to-end validation
  - Run the `message_passing` SDK example end-to-end against tokeirad
  - Verify: signal(5) → query returns 5 → update(10) returns old=5 → result=10
  - _Requirements: 13.1, 13.2, 13.3, 13.4, 14.1, 14.2, 14.3, 14.4_

## Architectural Follow-Up (Not in Scope)

- [ ]* Remove `ScheduleQueryTask` kernel command
  - Replace with direct query-only dispatch that does not create WFT history events
  - _Requirements: 16.1, 16.2, 16.3_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Phase 1 tasks are complete — transport plumbing works end-to-end
- Phase 2 is the correctness fix — without it, signal-then-query can return stale state
- The `BufferedQueryRegistry` is the key new abstraction: queries live with the run, each carries a barrier, delivery is gated by authoritative WFT state
- The `PendingQueryStore` remains for the poll-to-completion channel retention (it is NOT the buffering mechanism)
- The broker query queue remains for direct query-only dispatch (quiescent path) but is no longer used for buffering queries that need to wait for a WFT
- Property tests validate universal correctness properties from the design document
