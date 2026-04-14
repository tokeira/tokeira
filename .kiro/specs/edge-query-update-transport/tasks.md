# Implementation Plan: Edge Query & Update Transport

## Overview

Wire the runtime's existing query dispatch (`QueryTask`, `QueryResult`) and update lifecycle (`UpdateRegistry`, `UpdateOutcome`) through the edge/gRPC layer so that queries and updates flow end-to-end between SDK clients and workers via the standard Temporal protocol. Work spans `tokeira-edge` (primary) and `tokeira-runtime` (UpdateRegistry extension, broker combined poll).

Tasks follow a five-phase approach: runtime prerequisites first, then edge infrastructure, then query transport, then update transport, then cleanup and validation.

## Tasks

- [ ] 0. Runtime prerequisites
  - [ ] 0.1 Extend `UpdateRegistryEntry` to retain `input`, `identity`, and `update_name`
    - The current `UpdateRegistryEntry` only stores `complete_tx: oneshot::Sender<UpdateResolution>`
    - Add `update_name: String`, `input: Payloads`, `identity: String` fields so the edge can construct `update.v1.Request` messages
    - Update `UpdateRegistry::register()` to accept and store these fields
    - Update all call sites in the runtime that register updates
    - _Requirements: 3.2_

  - [ ] 0.2 Add combined query/WFT poll to the broker
    - The current `poll_workflow_task` only returns real WFTs. When a workflow is idle and a client issues `QueryWorkflow`, the worker poll gets no task.
    - Add a `poll_workflow_or_query_task` method (or extend the existing poll) that returns either a real WFT or a synthetic query-only task when queries are pending but no WFT exists
    - The synthetic query-only task has no history, `started_event_id = 0`, and a synthetic task token
    - _Requirements: 1.1, 1.5_

  - [ ] 0.3 Add `drain_pending_updates` method to `UpdateRegistry`
    - Add a method that returns all pending update entries for a given `run_key` without removing them (the entries stay until the worker responds)
    - Return `Vec<(String, String, Payloads, String)>` — (update_id, update_name, input, identity)
    - _Requirements: 3.1, 3.4_

- [ ] 1. Create `PendingQueryStore` and new edge DTO types
  - [ ] 1.1 Create `pending_queries.rs` module with `PendingQueryStore`
    - Create `crates/tokeira-edge/src/pending_queries.rs`
    - Implement `PendingQueryStore` with `Arc<Mutex<HashMap<Vec<u8>, HashMap<String, oneshot::Sender<QueryResult>>>>>` keyed by task token bytes, inner map keyed by query ID
    - Implement `insert(token: &[u8], query_id: String, tx: oneshot::Sender<QueryResult>)`
    - Implement `take(token: &[u8], query_id: &str) -> Option<oneshot::Sender<QueryResult>>`
    - Implement `drain(token: &[u8]) -> Vec<(String, oneshot::Sender<QueryResult>)>`
    - Export from `lib.rs`
    - _Requirements: 1.4, 2.1_

  - [ ] 1.2 Add new DTO types to `translate/mod.rs`
    - Add `WorkflowQueryDto { query_type: String, query_args: Payloads }`
    - Add `QueryResultDto` enum with `Answered { result: Payloads }` and `Failed { error_message: String }` variants
    - Add `ProtocolMessageDto { id: String, protocol_instance_id: String, body: Vec<u8>, sequencing_event_id: Option<i64> }`
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 1.3 Extend `PollWorkflowTaskQueueResponse` DTO with query and message fields
    - Add `queries: HashMap<String, WorkflowQueryDto>` field (default empty)
    - Add `messages: Vec<ProtocolMessageDto>` field (default empty)
    - Update all existing construction sites to include the new fields with defaults
    - _Requirements: 5.1, 5.2_

  - [ ] 1.4 Extend `RespondWorkflowTaskCompletedRequest` DTO with query_results and messages fields
    - Add `query_results: HashMap<String, QueryResultDto>` field (default empty)
    - Add `messages: Vec<ProtocolMessageDto>` field (default empty)
    - Update all existing construction sites to include the new fields with defaults
    - _Requirements: 5.3, 5.4_

  - [ ]* 1.5 Write property test for PendingQueryStore insert/take round-trip (Property 2)
    - **Property 2: PendingQueryStore insert/take round-trip**
    - Generate random sets of query IDs (1..8), insert oneshot senders, take each back, send a `QueryResult` on the returned sender, verify the receiver gets the correct result
    - **Validates: Requirements 1.4, 2.1**

- [ ] 2. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Wire query transport into poll and completion paths
  - [ ] 3.1 Add `PendingQueryStore` and broker access to `WorkflowService`
    - Add `pending_queries: PendingQueryStore` field to `WorkflowService`
    - Add `broker: Arc<InMemoryBroker>` field (or equivalent broker trait) to `WorkflowService` so the edge layer can drain query tasks
    - Update constructors (`new`, `new_with_history_wait_registry`) to accept and store these
    - Update all call sites (main.rs, tests)
    - _Requirements: 1.4_

  - [ ] 3.2 Wire query draining into `poll_workflow_task_queue`
    - After obtaining a `StartedWorkflowTask` from the runtime, drain pending query tasks from the broker for the same task queue using a non-blocking zero-timeout poll
    - For each `QueryTask`, generate a UUID query ID, store the `response_tx` in the `PendingQueryStore` keyed by the task token, and add the query to the response DTO's `queries` map
    - If the response carries only queries (no history advancement), set `started_event_id` to 0
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ] 3.3 Wire query result routing into `respond_workflow_task_completed`
    - Extract `query_results` from the DTO
    - For each entry, look up the query ID in the `PendingQueryStore` using the task token, send the `QueryResult` on the retained oneshot channel
    - Silently discard entries with no matching channel (caller timed out)
    - If the completion contains only `query_results` and no commands, skip command processing (query-only completion)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ]* 3.4 Write property test for query attachment preserves fields (Property 1)
    - **Property 1: Query attachment preserves fields**
    - Generate random `Vec<(String, String, Payloads)>` (query_id, query_type, query_args), build `WorkflowQueryDto` entries, verify the queries map contains all entries with matching fields
    - **Validates: Requirements 1.1, 1.3**

  - [ ]* 3.5 Write property test for query result routing delivers correct results (Property 3)
    - **Property 3: Query result routing delivers correct results**
    - Generate N random `QueryResultDto` entries (mix of Answered/Failed), insert corresponding oneshot channels in `PendingQueryStore`, route results by ID, verify each channel receives the correct variant and content, include orphaned IDs to verify silent discard
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5**

- [ ] 4. Wire proto translation for query fields
  - [ ] 4.1 Extend `poll_response_to_proto` to populate `queries` map (field 14)
    - Serialize each `WorkflowQueryDto` into a `query.v1.WorkflowQuery` proto with `query_type` and `query_args`
    - Populate the `queries` map on the proto response
    - _Requirements: 7.1_

  - [ ] 4.2 Extend `respond_completed_request_to_edge` to extract `query_results` (field 8)
    - Deserialize each `WorkflowQueryResult` proto entry into a `QueryResultDto`
    - Map `QUERY_RESULT_TYPE_ANSWERED` to `QueryResultDto::Answered` and `QUERY_RESULT_TYPE_FAILED` to `QueryResultDto::Failed`
    - Empty or absent `query_results` produces an empty HashMap
    - _Requirements: 7.2, 7.3, 7.4_

  - [ ]* 4.3 Write property test for query proto round-trip (Property 6)
    - **Property 6: Query proto round-trip**
    - Generate random `(query_id, query_type, Payloads)`, serialize to proto `WorkflowQuery` in the queries map, create a matching `WorkflowQueryResult` with `ANSWERED` and the same payloads, deserialize to `QueryResultDto`, verify all fields preserved
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4**

- [ ] 5. Implement legacy `RespondQueryTaskCompleted`
  - [ ] 5.1 Implement `respond_query_task_completed` edge method and gRPC handler
    - Add `respond_query_task_completed` method to `WorkflowService`
    - Extract the query result from the request, look up the query ID in the `PendingQueryStore`, send the result on the retained oneshot channel
    - If no matching channel exists (caller timed out), return a successful empty response
    - Wire the gRPC handler in `grpc/workflow_service.rs` to replace the `Status::unimplemented` stub
    - Add proto translation for `RespondQueryTaskCompleted` request/response
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Wire update transport into poll and completion paths
  - [ ] 7.1 Wire update message construction into `poll_workflow_task_queue`
    - After obtaining a `StartedWorkflowTask`, check the `UpdateRegistry` for pending updates on the same `run_key`
    - For each pending update, construct a `ProtocolMessageDto` wrapping an `update.v1.Request` body in a `google.protobuf.Any` envelope with type URL `type.googleapis.com/temporal.api.update.v1.Request`
    - Set `protocol_instance_id` to the update ID, `id` to `{update_id}/request`
    - Add the messages to the response DTO's `messages` field
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [ ] 7.2 Wire update response routing into `respond_workflow_task_completed`
    - Extract `messages` from the DTO
    - For each message, unpack the `google.protobuf.Any` body and determine the type (`update.v1.Acceptance`, `update.v1.Rejection`, `update.v1.Response`)
    - Extract `protocol_instance_id` as the update ID
    - Route to the `UpdateRegistry`: Rejection → `UpdateResolution::Rejected`, Response with success → `UpdateResolution::Completed`, Response with failure → `UpdateResolution::Rejected`
    - Silently discard messages with no matching registry entry (caller timed out)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [ ]* 7.3 Write property test for update message construction preserves fields (Property 4)
    - **Property 4: Update message construction preserves fields**
    - Generate random `(update_id, update_name, Payloads)`, call `build_update_request_message`, decode the body as `update.v1.Request`, verify `protocol_instance_id == update_id`, `Meta.update_id == update_id`, `Input.name == update_name`, `Input.args == payloads`
    - **Validates: Requirements 3.1, 3.2**

  - [ ]* 7.4 Write property test for update response routing delivers correct resolution (Property 5)
    - **Property 5: Update response routing delivers correct resolution**
    - Generate random update response messages (Acceptance, Rejection with random failure, Response with random success/failure), register corresponding entries in `UpdateRegistry`, route messages, verify each caller receives the correct `UpdateResolution` variant, include orphaned IDs to verify silent discard
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

- [ ] 8. Wire proto translation for update message fields
  - [ ] 8.1 Extend `poll_response_to_proto` to populate `messages` field (field 15)
    - Serialize each `ProtocolMessageDto` into a `protocol.v1.Message` proto
    - Decode the `body` bytes as a `prost_types::Any` and set it on the proto message
    - Set `sequencing_id` from `sequencing_event_id`
    - _Requirements: 8.1_

  - [ ] 8.2 Extend `respond_completed_request_to_edge` to extract `messages` (field 11)
    - Deserialize each `protocol.v1.Message` proto into a `ProtocolMessageDto`
    - Encode the `body` Any back to bytes for the DTO
    - Extract `sequencing_event_id` from the `sequencing_id` oneof
    - Empty or absent `messages` produces an empty Vec
    - _Requirements: 8.2, 8.4_

  - [ ]* 8.3 Write property test for update message proto round-trip (Property 7)
    - **Property 7: Update message proto round-trip**
    - Generate random `ProtocolMessageDto` with arbitrary `id`, `protocol_instance_id`, `body` bytes, and optional `sequencing_event_id`, serialize to proto `protocol.v1.Message`, deserialize back, verify all fields match
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4**

- [ ] 9. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 10. Update UNSUPPORTED_FIELDS.md
  - Remove `queries` and `messages` entries from the `PollWorkflowTaskQueueResponse` section
  - Remove `query_results` and `messages` entries from the `RespondWorkflowTaskCompletedRequest` section
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [ ] 11. Final checkpoint — Ensure all tests pass
  - Run `cargo test` in the `tokeira` workspace to verify all existing and new tests pass
  - Run `cargo lint` to verify no clippy warnings
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document (7 properties)
- **Task 0 is a runtime prerequisite** — `UpdateRegistryEntry` must retain input/identity/name, and the broker needs a combined poll for query-only tasks
- The `PendingQueryStore` is keyed by task token bytes, with at most one legacy query per token under key `"__legacy__"`
- Legacy and modern query paths are mutually exclusive per poll response to avoid ambiguity
- No kernel changes are needed
