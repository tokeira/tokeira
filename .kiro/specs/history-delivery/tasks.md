# Implementation Plan: History Delivery

## Overview

Implement history event serialization, poll response population, `GetWorkflowExecutionHistory` RPC, visibility wiring, and completion status propagation. Tasks are ordered by dependency: proto definitions → build config → serializer → poll path → RPC endpoint → completion status → tokeirad wiring → tests.

## Tasks

- [x] 1. Define history proto messages
  - [x] 1.1 Create `event_type.proto` with EventType enum
    - Create `tokeira-proto/proto/upstream/temporal/api/enums/v1/event_type.proto`
    - Define `temporal.api.enums.v1.EventType` enum with all 51 variants matching `HistoryEventKind` discriminants (EVENT_TYPE_UNSPECIFIED through EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)
    - _Requirements: 1.2_

  - [x] 1.2 Create `message.proto` with HistoryEvent, History, and all attributes messages
    - Create `tokeira-proto/proto/upstream/temporal/api/history/v1/message.proto`
    - Define `HistoryEvent` message with `event_id`, `event_time`, `event_type`, and `oneof attributes` covering all 51 event kinds
    - Define one `*EventAttributes` message per `HistoryEventKind` variant with fields matching the kernel variant fields
    - Define `History` wrapper message with `repeated HistoryEvent events`
    - Define `RetryPolicy` message for started attributes
    - Import `temporal.api.common.v1.message` and `temporal.api.enums.v1.event_type`
    - Use `temporal.api.common.v1.Payloads`, `Memo`, `SearchAttributes`, `TaskQueue` for domain types
    - Encode `time::Duration` fields as `int64` milliseconds, `time::OffsetDateTime` fields as `int64` unix nanos
    - _Requirements: 1.1, 1.3, 1.4_

  - [x] 1.3 Add `GetWorkflowExecutionHistory` RPC to `service.proto`
    - Add `import "temporal/api/history/v1/message.proto"` to `workflowservice/v1/service.proto`
    - Add `rpc GetWorkflowExecutionHistory(GetWorkflowExecutionHistoryRequest) returns (GetWorkflowExecutionHistoryResponse)` to the `WorkflowService` service definition
    - Define `GetWorkflowExecutionHistoryRequest` with `namespace`, `execution` (WorkflowExecution), `maximum_page_size` (int32)
    - Define `GetWorkflowExecutionHistoryResponse` with `history` field of type `temporal.api.history.v1.History`
    - _Requirements: 4.1, 4.2, 4.3, 4.6_

- [x] 2. Update build configuration and module exports
  - [x] 2.1 Add new proto files to `build.rs` compilation
    - Add `"proto/upstream/temporal/api/history/v1/message.proto"` and `"proto/upstream/temporal/api/enums/v1/event_type.proto"` to the public compile list in `tokeira-proto/build.rs`
    - _Requirements: 1.5_

  - [x] 2.2 Add `history` module to `public.rs` and re-export in `lib.rs`
    - Add `pub mod history { pub mod v1 { tonic::include_proto!("temporal.api.history.v1"); } }` inside `temporal::api` in `tokeira-proto/src/public.rs`
    - Add `pub use temporal::api::history::v1 as history;` re-export alongside existing re-exports in `public.rs`
    - _Requirements: 1.5_

- [x] 3. Checkpoint — Proto compilation
  - Ensure `cargo build -p tokeira-proto` succeeds and generated Rust types are accessible under `tokeira_proto::history`. Ask the user if questions arise.

- [x] 4. Implement history serializer module
  - [x] 4.1 Create `history_serializer.rs` in `tokeira-edge/src/translate/`
    - Create `tokeira-edge/src/translate/history_serializer.rs`
    - Add `pub mod history_serializer;` to `tokeira-edge/src/translate/mod.rs`
    - Implement `pub fn serialize_history(events: &[HistoryEvent]) -> Vec<u8>` that builds a `history::History` proto and encodes to bytes via `prost::Message::encode_to_vec`
    - Implement `pub fn history_event_to_proto(event: &HistoryEvent) -> history::HistoryEvent` converting `event_id`, `happened_at` (unix nanos), `event_type`, and attributes
    - Implement `fn event_type_for_kind(kind: &HistoryEventKind) -> i32` mapping all `HistoryEventKind` variants to `EventType` enum values
    - Implement `fn attributes_for_kind(kind: &HistoryEventKind) -> history::history_event::Attributes` with a `match` over ALL `HistoryEventKind` variants constructing the corresponding proto attributes message
    - Use existing conversion helpers: `payloads_from_domain`, `memo_from_domain`, `search_attributes_from_domain`, `task_queue_from_domain` from `tokeira_proto::conversions::common`
    - Encode `time::Duration` → `.whole_milliseconds() as i64`, `time::OffsetDateTime` → unix nanos as `i64`, `Option<T>` → default/empty when `None`
    - Respect `rustfmt max_width = 90`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x]* 4.2 Write property test: History serialization round-trip (Property 1)
    - **Property 1: History serialization round-trip**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**
    - In `history_serializer.rs` `#[cfg(test)]` module, use `proptest` with `ProptestConfig::with_cases(100)`
    - Generate arbitrary `HistoryEvent` values covering all `HistoryEventKind` variants via a proptest strategy
    - For each event: call `history_event_to_proto`, encode to bytes with `prost::Message::encode_to_vec`, decode with `history::HistoryEvent::decode`, assert decoded proto equals original proto
    - Tag: `Feature: history-delivery, Property 1: History serialization round-trip`

  - [x]* 4.3 Write unit tests for history serializer
    - Test empty history produces valid decodable `History` proto with zero events
    - Test at least one golden example per major event category (workflow lifecycle, activity, timer, child workflow, external signal, nexus, update)
    - _Requirements: 2.1, 2.6, 3.5_

- [x] 5. Checkpoint — History serializer
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Wire history into poll response path
  - [x] 6.1 Add `repo: Arc<dyn RunRepository>` to `WorkflowService`
    - Add `repo: Arc<dyn RunRepository>` field to `WorkflowService` struct in `tokeira-edge/src/workflow_service.rs`
    - Update `WorkflowService::new` constructor to accept and store the new `repo` parameter
    - Update all existing call sites of `WorkflowService::new` (in `tokeirad/src/main.rs` and test code in `grpc/workflow_service.rs`)
    - _Requirements: 3.1_

  - [x] 6.2 Update `from_internal::poll_response` to load history
    - Change `poll_response` signature to `pub async fn poll_response(started: StartedWorkflowTask, repo: &dyn RunRepository) -> Result<PollWorkflowTaskQueueResponse>`
    - Call `repo.read_history(started.run_key, 0, usize::MAX).await?` to load full history
    - Populate `WorkflowTaskPayloadDto.history` with the loaded events
    - Update all call sites of `poll_response` to pass the repository reference
    - _Requirements: 3.1, 3.2, 3.6_

  - [x] 6.3 Update `translate::poll_response_to_proto` to serialize history
    - Replace the stub `history_blob` function with a call to `history_serializer::serialize_history(&resp.payload.history)`
    - Remove the old `fn history_blob(_history: &[HistoryEvent]) -> Vec<u8>` stub
    - _Requirements: 3.3, 3.4, 3.5_

  - [ ]* 6.4 Write property test: History loader preserves event list (Property 2)
    - **Property 2: History loader preserves event list**
    - **Validates: Requirements 3.1, 3.2**
    - Use `proptest` with `ProptestConfig::with_cases(100)`
    - Generate arbitrary `Vec<HistoryEvent>` lists, create a mock `RunRepository` returning the list
    - Call `poll_response` and assert `payload.history == original_list`
    - Tag: `Feature: history-delivery, Property 2: History loader preserves event list`

- [x] 7. Implement GetWorkflowExecutionHistory endpoint
  - [x] 7.1 Add edge DTOs for GetWorkflowExecutionHistory
    - Add `GetWorkflowExecutionHistoryRequest` struct (namespace, workflow_id, run_id: Option, maximum_page_size) to `tokeira-edge/src/translate/mod.rs`
    - Add `GetWorkflowExecutionHistoryResponse` struct (history: Vec<HistoryEvent>) to `tokeira-edge/src/translate/mod.rs`
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 7.2 Add translate functions for GetWorkflowExecutionHistory
    - Implement `get_history_request_to_edge` in `tokeira-edge/src/grpc/translate.rs` converting proto request to edge DTO
    - Implement `get_history_response_to_proto` in `tokeira-edge/src/grpc/translate.rs` serializing history events via `history_serializer::serialize_history` and wrapping in proto response
    - _Requirements: 4.3, 4.4_

  - [x] 7.3 Add `get_workflow_execution_history` method to `WorkflowService`
    - Implement the method on `WorkflowService` in `tokeira-edge/src/workflow_service.rs`
    - Resolve execution to run key via `self.resolve_run_key`, read history from `self.repo`, return `GetWorkflowExecutionHistoryResponse`
    - Return NOT_FOUND when execution does not exist
    - _Requirements: 4.4, 4.5_

  - [x] 7.4 Add gRPC handler for `get_workflow_execution_history`
    - Add `async fn get_workflow_execution_history` to `WorkflowServiceGrpc` in `tokeira-edge/src/grpc/workflow_service.rs`
    - Wire translate → service → translate response
    - _Requirements: 4.4, 4.6_

  - [ ]* 7.5 Write unit tests for GetWorkflowExecutionHistory
    - Test non-existent workflow returns NOT_FOUND
    - Test valid request returns serialized history
    - _Requirements: 4.4, 4.5_

- [x] 8. Implement completion status changes
  - [x] 8.1 Extend `WorkflowMutationOutcome` with execution status
    - Add `execution_status: ExecutionStatus` and `new_run_id: Option<RunId>` fields to `WorkflowMutationOutcome` in `tokeira-edge/src/workflow_service.rs`
    - _Requirements: 6.1_

  - [x] 8.2 Update `commit_result_to_outcome` in `RuntimeAdapter`
    - In `tokeira-edge/src/grpc/runtime_adapter.rs`, extract `new_state.status` and map `ContinuedAsNew` to `new_run_id` from the committed state
    - Set `execution_status` from `new_state.status` for `Applied` variant
    - Set `execution_status: ExecutionStatus::Running` and `new_run_id: None` for `Duplicate` variant
    - _Requirements: 6.1_

  - [x] 8.3 Update `RespondWorkflowTaskCompletedResponse` DTO and `from_internal::completed_response`
    - Add `execution_status: ExecutionStatus`, `new_run_id: Option<RunId>`, and `was_duplicate: bool` fields to `RespondWorkflowTaskCompletedResponse` in `translate/mod.rs`
    - Update `from_internal::completed_response` to propagate the new fields from `WorkflowMutationOutcome`
    - _Requirements: 6.1, 6.2_

  - [x] 8.4 Update `translate::completed_response_to_proto` for completion status
    - Set `workflow_completed = !resp.was_duplicate && !resp.execution_status.is_open()`
    - Set `new_run_id` from `resp.new_run_id` (empty string when None)
    - _Requirements: 6.2, 6.3, 6.4, 6.5_

  - [x]* 8.5 Write property test: Completion status mapping (Property 3)
    - **Property 3: Completion status mapping**
    - **Validates: Requirements 6.2, 6.3, 6.5**
    - Use `proptest` with `ProptestConfig::with_cases(100)`
    - Generate arbitrary `(ExecutionStatus, bool)` pairs, construct DTO, call `completed_response_to_proto`
    - Assert `workflow_completed == (!was_duplicate && !status.is_open())`
    - Tag: `Feature: history-delivery, Property 3: Completion status mapping`

  - [x]* 8.6 Write unit tests for completion status
    - Test `ContinuedAsNew` sets `new_run_id`
    - Test `was_duplicate: true` always sets `workflow_completed: false`
    - Test each terminal status sets `workflow_completed: true`
    - _Requirements: 6.2, 6.3, 6.4, 6.5_

- [x] 9. Checkpoint — Core implementation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Wire tokeirad bootstrap
  - [x] 10.1 Replace `EmptyVisibilityApi` with `VisibilityQueryService` in `tokeirad`
    - In `apps/tokeirad/src/main.rs`, replace `Arc::new(EmptyVisibilityApi)` with `Arc::new(VisibilityQueryService::new(InMemoryVisibilityStore::default()))`
    - Add necessary imports for `VisibilityQueryService` and `InMemoryVisibilityStore` from `tokeira_projection`
    - _Requirements: 5.1, 5.2_

  - [x] 10.2 Pass `RunRepository` to `WorkflowService` constructor in `tokeirad`
    - Pass `store.clone()` as the `repo` argument to `WorkflowService::new` in `tokeirad/src/main.rs`
    - _Requirements: 3.1_

- [x] 11. Checkpoint — Full integration
  - Ensure all tests pass and `cargo build -p tokeirad` succeeds. Ask the user if questions arise.

- [ ]* 12. Write integration tests
  - [ ]* 12.1 Write integration test for poll response history
    - Start a workflow, poll a workflow task, verify `history_blob` decodes to a `History` message containing `WorkflowExecutionStarted` and `WorkflowTaskScheduled` events
    - _Requirements: 3.3, 3.4_

  - [ ]* 12.2 Write integration test for GetWorkflowExecutionHistory
    - Start and complete a workflow, call `GetWorkflowExecutionHistory`, verify the full event sequence
    - _Requirements: 4.4_

  - [ ]* 12.3 Write integration test for visibility wiring
    - After wiring `VisibilityQueryService`, start a workflow and verify `ListWorkflowExecutions` returns it
    - _Requirements: 5.3, 5.4_

- [x] 13. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- The history serializer `attributes_for_kind` match must cover ALL 51 `HistoryEventKind` variants — the Rust compiler will enforce exhaustiveness
- `rustfmt max_width = 90` must be respected in all new code
- `proptest` with minimum 100 iterations per property test
