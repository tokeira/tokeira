# Implementation Plan: Edge Nexus Task Transport

## Overview

Implement the Nexus Task Transport layer in 3 phases: (1) NexusTaskBroker + poll handler + proto translation for request types, (2) completion and failure handlers + proto translation for response types, (3) worker-targeted dispatch routing in the publisher. All code is Rust, targeting the existing `tokeira-runtime` and `tokeira-edge` crates.

## Tasks

- [ ] 1. Phase 1 — NexusTaskBroker, task token, poll handler, request translation
  - [ ] 1.1 Add NexusTaskToken, NexusTask, NexusTaskRequest, NexusLink, and NexusTaskBroker to `crates/tokeira-runtime/src/nexus.rs`
    - Add `NexusTaskToken` struct with `#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]`, fields: `run_key: RunKey`, `operation_id: String`, `scheduled_event_id: i64`
    - Implement `NexusTaskToken::encode() -> Result<Vec<u8>>` via `serde_json::to_vec` and `NexusTaskToken::decode(bytes: &[u8]) -> Result<Self>` via `serde_json::from_slice` with descriptive error on failure
    - Add `NexusTaskRequest` enum (`StartOperation` / `CancelOperation`) and `NexusLink` struct per design
    - Add `NexusTask` struct with `token: NexusTaskToken` and `request: NexusTaskRequest`
    - Add `NexusTaskBroker` with `Arc<Mutex<NexusBrokerState>>` + `Arc<Notify>`, keyed by `(NamespaceId, TaskQueueName)`
    - Implement `publish(&self, namespace_id, task_queue, task)` — enqueue and notify
    - Implement `poll(&self, namespace_id, task_queue, wait_for) -> Option<NexusTask>` — try take, if empty register Notify future, re-check, await with timeout, try take again (same pattern as `InMemoryActivityBroker`)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3, 2.4_

  - [ ]* 1.2 Write property test for task token round-trip
    - **Property 1: Task token round-trip**
    - Generate random `(RunKey, String, i64)` tuples via proptest, encode then decode `NexusTaskToken`, verify equality
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5**

  - [ ]* 1.3 Write property test for broker queue isolation
    - **Property 2: Broker queue isolation**
    - Generate random queue keys and tasks, publish to specific queues, poll each queue, verify only matching tasks returned in FIFO order
    - **Validates: Requirements 1.1, 1.4**

  - [ ] 1.4 Create proto translation module `crates/tokeira-edge/src/translate/nexus.rs` and register in `mod.rs`
    - Add `pub mod nexus;` to `crates/tokeira-edge/src/translate/mod.rs`
    - Implement `nexus_task_to_proto_request(task_request: &NexusTaskRequest) -> Result<nexus_v1::Request>` — build proto `Request` with correct variant, header, scheduled_time
    - Implement `start_operation_to_proto(...)` — build `StartOperationRequest` preserving service, operation, request_id, payload, callback, callback_header, links, header fields
    - Implement `cancel_operation_to_proto(...)` — build `CancelOperationRequest` preserving service, operation, operation_id
    - Return descriptive errors for invalid field values
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ]* 1.5 Write property test for request translation field preservation
    - **Property 3: Request translation preserves fields**
    - Generate random `NexusTaskRequest` values (both variants), translate to proto, verify all fields preserved
    - **Validates: Requirements 4.1, 4.2, 4.3**

  - [ ] 1.6 Implement `poll_nexus_task_queue` handler in `crates/tokeira-edge/src/workflow_service.rs` and wire gRPC stub
    - Add `nexus_broker: NexusTaskBroker` field to `WorkflowService` struct, thread through constructors
    - Add `pub async fn poll_nexus_task_queue(...)` method: validate namespace (non-empty) and task_queue (present, non-empty name), long-poll `NexusTaskBroker`, translate `NexusTask` to `PollNexusTaskQueueResponse` with encoded task token and proto `Request`
    - Return empty response on timeout, `INVALID_ARGUMENT` on validation failures
    - Replace the `poll_nexus_task_queue` stub in `crates/tokeira-edge/src/grpc/workflow_service.rs` to delegate to `WorkflowService`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ]* 1.7 Write unit tests for poll handler validation
    - Test empty namespace returns `INVALID_ARGUMENT` (Req 3.4)
    - Test empty task_queue returns `INVALID_ARGUMENT` (Req 3.5)
    - Test poll timeout returns empty response (Req 1.6, 3.3)
    - Test poller wakes on publish (Req 1.3)
    - Test malformed token decode returns descriptive error (Req 2.4)
    - _Requirements: 1.3, 1.6, 2.4, 3.3, 3.4, 3.5_

- [ ] 2. Checkpoint — Phase 1
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Phase 2 — Completion and failure handlers, response translation
  - [ ] 3.1 Add response translation functions to `crates/tokeira-edge/src/translate/nexus.rs`
    - Implement `proto_response_to_resolution(response: nexus_v1::Response) -> Result<NexusResolution>` — dispatch on variant
    - Implement `proto_start_response_to_resolution(...)` — `Sync` → `Completed` with payload, `Async` → `Started`, `operation_error` → `Failed`
    - Implement `proto_cancel_response_to_resolution()` → `Canceled`
    - Implement `proto_handler_error_to_resolution(error: HandlerError) -> Result<NexusResolution>` → `Failed` with error_type and failure details
    - Return descriptive errors for unrecognized variants or invalid data
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

  - [ ]* 3.2 Write property test for response translation correctness
    - **Property 4: Response translation correctness**
    - Generate random proto `Response` and `HandlerError` values across all variants, translate to `NexusResolution`, verify correct variant and field preservation
    - **Validates: Requirements 5.2, 5.3, 5.4, 5.5, 6.2, 7.1, 7.2, 7.3, 7.4, 7.5**

  - [ ] 3.3 Implement `respond_nexus_task_completed` handler in `crates/tokeira-edge/src/workflow_service.rs` and wire gRPC stub
    - Add `pub async fn respond_nexus_task_completed(...)` method: validate task_token (non-empty, decodable) and response (variant present), decode `NexusTaskToken`, translate proto `Response` to `NexusResolution`, submit `Command::NexusOperationResolved` to originating run via `LaneHandle`
    - Return `INVALID_ARGUMENT` for empty/malformed token or missing response
    - Return success even if kernel rejects the command (idempotent completion)
    - Replace the `respond_nexus_task_completed` stub in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8_

  - [ ] 3.4 Implement `respond_nexus_task_failed` handler in `crates/tokeira-edge/src/workflow_service.rs` and wire gRPC stub
    - Add `pub async fn respond_nexus_task_failed(...)` method: validate task_token (non-empty, decodable) and error (present), decode `NexusTaskToken`, translate proto `HandlerError` to `NexusResolution::Failed`, submit `Command::NexusOperationResolved`
    - Return `INVALID_ARGUMENT` for empty/malformed token or missing error
    - Return success even if kernel rejects the command (idempotent failure)
    - Replace the `respond_nexus_task_failed` stub in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ]* 3.5 Write unit tests for completion and failure handlers
    - Test empty task_token returns `INVALID_ARGUMENT` for completed handler (Req 5.6)
    - Test missing response returns `INVALID_ARGUMENT` for completed handler (Req 5.7)
    - Test kernel rejection returns success for completed handler (Req 5.8)
    - Test empty task_token returns `INVALID_ARGUMENT` for failed handler (Req 6.3)
    - Test missing error returns `INVALID_ARGUMENT` for failed handler (Req 6.4)
    - Test kernel rejection returns success for failed handler (Req 6.5)
    - _Requirements: 5.6, 5.7, 5.8, 6.3, 6.4, 6.5_

- [ ] 4. Checkpoint — Phase 2
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Phase 3 — Worker-targeted dispatch routing and endpoint registry extension
  - [ ] 5.1 Extend `NexusEndpointConfig` with `EndpointTarget` enum in `crates/tokeira-runtime/src/nexus.rs`
    - Add `EndpointTarget` enum: `External { address: String }` and `Worker { namespace_id: NamespaceId, task_queue: TaskQueueName }`
    - Replace `NexusEndpointConfig { address: String }` with `NexusEndpointConfig { target: EndpointTarget }`
    - Update `NexusEndpointRegistry` and all call sites that reference `config.address` to use `config.target` match
    - _Requirements: 11.1, 11.2_

  - [ ] 5.2 Add `NexusTaskBroker` to `RuntimeDispatchPublisher` and route Worker-targeted operations through the broker in `crates/tokeira-runtime/src/publisher.rs`
    - Add `nexus_broker: NexusTaskBroker` field to `RuntimeDispatchPublisher`, thread through constructor
    - In `handle_schedule_nexus_operation`: match on `config.target` — `External` keeps existing HTTP path, `Worker` builds `NexusTask` with `StartOperation` request and publishes to broker
    - In `handle_cancel_nexus_operation`: match on `config.target` — `External` keeps existing HTTP path, `Worker` builds `NexusTask` with `CancelOperation` request and publishes to broker
    - Preserve existing "endpoint not found" → `NexusResolution::Failed` behavior
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 9.1, 9.2, 9.3, 11.3_

  - [ ] 5.3 Wire timeout tracking for broker-dispatched operations in `crates/tokeira-runtime/src/publisher.rs`
    - When publishing a `NexusTask` with a non-None `schedule_to_close_timeout` via the Worker path, insert a `NexusTimeoutEntry` into `NexusTimeoutTrackingState` (same as the existing HTTP path)
    - Existing `NexusTimeoutScanner` and resolution removal logic handle the rest without modification
    - _Requirements: 10.1, 10.2, 10.3_

  - [ ]* 5.4 Write property test for dispatch-to-broker field preservation
    - **Property 5: Dispatch-to-broker field preservation**
    - Generate random `ScheduleNexusOperation` and `CancelNexusOperation` dispatch ops, run through publisher logic with Worker target, verify published `NexusTask` token and request fields match
    - **Validates: Requirements 8.3, 8.4, 9.2**

  - [ ]* 5.5 Write integration tests for dispatch routing
    - Test Worker-targeted ScheduleNexusOperation publishes to broker (Req 8.1)
    - Test External-targeted ScheduleNexusOperation routes to HTTP client (Req 8.2)
    - Test Worker-targeted CancelNexusOperation publishes cancel task to broker (Req 9.1)
    - Test External-targeted CancelNexusOperation routes to HTTP client (Req 9.3)
    - Test unknown endpoint produces `NexusResolution::Failed` (Req 11.3)
    - Test timeout tracking inserted for broker-dispatched task (Req 10.1)
    - _Requirements: 8.1, 8.2, 9.1, 9.3, 10.1, 11.3_

- [ ] 6. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- All property tests use `proptest` with minimum 100 iterations, tagged `// Feature: edge-nexus-task-transport, Property N: <title>`
- The `NexusTaskBroker` follows the same `Notify`-based long-poll pattern as `InMemoryBroker` and `InMemoryActivityBroker`
- Task tokens use JSON encoding via `serde_json` (same as WFT/activity tokens)
- Completion handlers are idempotent — kernel rejections are swallowed as success
