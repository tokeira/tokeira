# Implementation Tasks: Edge Failure Object Completeness

## Task 1: Kernel event model — Replace bare strings with opaque Payload on failure-bearing events
> Requirements: 1 (AC 1.1), 2 (AC 2.1, 2.2), 3 (AC 3.1), 5 (AC 5.1), 6 (AC 6.1)
> Design: Components 1, 2

- [x] 1.1 In `crates/tokeira-kernel/src/event.rs`, change `WorkflowExecutionFailed` from `{ message: String, details: Option<Payload>, retry_state: RetryState, attempt: u32 }` to `{ failure: Payload, retry_state: RetryState, attempt: u32 }`
- [x] 1.2 In `crates/tokeira-kernel/src/event.rs`, change `ActivityTaskFailed` from `{ activity_id: String, scheduled_event_id: i64, started_event_id: i64, message: String }` to `{ activity_id: String, scheduled_event_id: i64, started_event_id: i64, failure: Payload }`
- [x] 1.3 In `crates/tokeira-kernel/src/event.rs`, change `ChildWorkflowExecutionFailed` from `{ child_workflow_id: WorkflowId, failure: String }` to `{ child_workflow_id: WorkflowId, failure: Payload }`
- [x] 1.4 In `crates/tokeira-kernel/src/event.rs`, change `NexusOperationFailed` from `{ operation_id: String, scheduled_event_id: i64, failure: String }` to `{ operation_id: String, scheduled_event_id: i64, failure: Payload }`
- [x] 1.5 In `crates/tokeira-kernel/src/event.rs`, change `WorkflowExecutionUpdateRejected` from `{ update_id: String, failure: String }` to `{ update_id: String, failure: Payload }`
- [x] 1.6 In `crates/tokeira-kernel/src/event.rs`, change `ActivityResolution::Failed` from `{ message: String }` to `{ failure: Payload }`
- [x] 1.7 In `crates/tokeira-kernel/src/state.rs`, change `WorkflowState.close_failure` from `Option<String>` to `Option<Payload>` so the child resolution path has access to the full failure data

## Task 2: Kernel command model — Replace bare strings with opaque Payload on failure-bearing commands
> Requirements: 1 (AC 1.2), 3 (AC 3.2), 5 (AC 5.2), 6 (AC 6.2, 6.3)
> Design: Component 2

- [x] 2.1 In `crates/tokeira-kernel/src/command.rs`, change `WorkflowCommand::FailWorkflow` from `{ message: String, details: Option<Payload> }` to `{ failure: Payload }`
- [x] 2.2 In `crates/tokeira-kernel/src/command.rs`, change `ChildResolution::Failed` from `{ failure: String }` to `{ failure: Payload }`
- [x] 2.3 In `crates/tokeira-kernel/src/command.rs`, change `NexusResolution::Failed` from `{ failure: String }` to `{ failure: Payload }`
- [x] 2.4 In `crates/tokeira-kernel/src/command.rs`, change `UpdateProtocolBody::Rejected` from `{ update_id: String, failure: String }` to `{ update_id: String, failure: Payload }`
- [x] 2.5 In `crates/tokeira-kernel/src/command.rs`, change `WorkflowCommand::UpdateRejected` from `{ update_id: String, failure: String }` to `{ update_id: String, failure: Payload }`

## Task 3: Kernel apply methods — Thread opaque Payload through failure paths
> Requirements: 1 (AC 1.1, 1.2), 2 (AC 2.1, 2.2), 3 (AC 3.1, 3.2), 5 (AC 5.1, 5.2), 6 (AC 6.1, 6.2, 6.3)
> Design: Component 3

- [x] 3.1 In `crates/tokeira-kernel/src/kernel.rs`, update `apply_workflow_task_completed` to thread `failure: Payload` from `WorkflowCommand::FailWorkflow` into `HistoryEventKind::WorkflowExecutionFailed`
- [x] 3.2 In `crates/tokeira-kernel/src/kernel.rs`, update `apply_activity_resolved` to thread `failure: Payload` from `ActivityResolution::Failed` into `HistoryEventKind::ActivityTaskFailed`
- [x] 3.3 In `crates/tokeira-kernel/src/kernel.rs`, update `apply_child_resolved` to thread `failure: Payload` from `ChildResolution::Failed` into `HistoryEventKind::ChildWorkflowExecutionFailed`
- [x] 3.4 In `crates/tokeira-kernel/src/kernel.rs`, update `apply_nexus_operation_resolved` to thread `failure: Payload` from `NexusResolution::Failed` into `HistoryEventKind::NexusOperationFailed`
- [x] 3.5 In `crates/tokeira-kernel/src/kernel.rs`, update the `WorkflowCommand::UpdateRejected` handling to thread `failure: Payload` into `HistoryEventKind::WorkflowExecutionUpdateRejected`
- [x] 3.6 In `crates/tokeira-kernel/src/kernel.rs`, when `FailWorkflow` closes the run, store the opaque `failure: Payload` in `state.close_failure` instead of extracting `message`
- [x] 3.7 Fix all remaining pattern matches on the modified variants in `crates/tokeira-kernel/src/kernel.rs` (replay paths, close info extraction, etc.)

## Checkpoint: Kernel model changes — verify `cargo test -p tokeira-kernel` compiles (tests may fail due to generator changes needed in Task 7)

## Task 4: Edge inbound translation — Encode full Failure as Payload
> Requirements: 1 (AC 1.3), 2 (AC 2.3), 6 (AC 6.4)
> Design: Component 4

- [x] 4.1 In `crates/tokeira-edge/src/grpc/translate.rs`, update `proto_command_to_workflow_command` for `FailWorkflowExecutionCommandAttributes` to encode the entire proto `Failure` via `failure_to_payload` instead of extracting `message`
- [x] 4.2 In `crates/tokeira-edge/src/grpc/translate.rs`, add `extract_retry_classification(failure) -> (Option<String>, bool)` that extracts `ApplicationFailureInfo.type` and `non_retryable` flag (NOT `Failure.source`) for retry decisions, matching Temporal's `isRetryable` semantics
- [x] 4.3 In `crates/tokeira-edge/src/grpc/translate.rs`, update `respond_activity_failed_to_edge` to encode the entire proto `Failure` via `failure_to_payload`, extract retry classification via `extract_retry_classification`, and return all three in the DTO
- [x] 4.4 In `crates/tokeira-edge/src/grpc/translate.rs`, update `resolve_protocol_message_body` for the `Rejection` path to encode the proto `Failure` via `failure_to_payload` into `UpdateProtocolBody::Rejected`
- [x] 4.5 In `crates/tokeira-edge/src/grpc/translate.rs`, update `resolve_protocol_message_body` for the `Response` with `Failure` outcome path to encode the proto `Failure` via `failure_to_payload` into `UpdateProtocolBody::Rejected`
- [x] 4.6 In `crates/tokeira-edge/src/grpc/translate.rs`, update `workflow_command_to_proto` for `FailWorkflow` to use `payload_to_failure` to reconstruct the proto `Failure`
- [x] 4.7 In `crates/tokeira-edge/src/grpc/translate.rs`, update `workflow_command_to_proto` for `UpdateRejected` to use `payload_to_failure` to reconstruct the proto `Failure`

## Task 5: Edge DTO and runtime — Thread opaque Payload through activity fail path
> Requirements: 2 (AC 2.4, 2.5), 9 (AC 9.1, 9.2, 9.6)
> Design: Components 5, 6

- [x] 5.1 In `crates/tokeira-edge/src/translate/mod.rs`, change `RespondActivityTaskFailedRequest` to carry `failure: Payload`, `failure_error_type: Option<String>`, and `is_non_retryable: bool` (replacing `failure_message: String`)
- [x] 5.2 In `crates/tokeira-runtime/src/runtime.rs`, update `fail_activity_task` to accept `failure: Payload`, `failure_error_type: Option<String>`, and `is_non_retryable: bool`; use `failure_error_type` and `is_non_retryable` for retry decisions (replacing the current `Failure.source`-based logic); thread `failure` into `ActivityResolution::Failed { failure }`
- [x] 5.3 In `crates/tokeira-edge/src/workflow_service.rs`, update the `respond_activity_task_failed` call site to pass `req.failure`, `req.failure_error_type`, and `req.is_non_retryable`

## Task 5b: Runtime — Child resolution and Nexus failure source
> Requirements: 3 (AC 3.4, 3.5), 5 (AC 5.4)
> Design: Components 1, 3

- [x] 5b.1 In `crates/tokeira-runtime/src/lane.rs`, update the child resolution path to read `close_failure: Option<Payload>` from the child's `WorkflowState` and pass it as `ChildResolution::Failed { failure }`. If `close_failure` is `None`, construct a default `Failure { message: "child workflow failed" }` via `failure_to_payload`.
- [x] 5b.2 In `crates/tokeira-runtime/src/publisher.rs`, update the Nexus failure paths to construct a proto `Failure { message: error_string, failure_info: Some(ApplicationFailureInfo { type: "NexusOperationFailure", non_retryable: false }) }` and encode via `failure_to_payload`, replacing the bare string construction.

## Task 5c: Shared failure encoding utility
> Requirements: 3 (AC 3.4), 5 (AC 5.4)
> Design: Components 1, 3

- [x] 5c.1 Move `failure_to_payload` and `payload_to_failure` from `crates/tokeira-edge/src/grpc/translate.rs` to `crates/tokeira-proto/src/conversions/common.rs` (or a new `crates/tokeira-proto/src/conversions/failure.rs` module) so they are accessible from both the edge and runtime crates
- [x] 5c.2 Re-export the helpers from `tokeira-proto::conversions::common` and update all import sites in `tokeira-edge` to use the new location
- [x] 5c.3 Add `tokeira-proto` as a dependency of `tokeira-runtime` if not already present (check `Cargo.toml`)

## Checkpoint: Edge and runtime compilation — verify `cargo build -p tokeira-edge -p tokeira-runtime` compiles

## Task 6: History serializer — Deserialize opaque Payload back to proto Failure
> Requirements: 1 (AC 1.4, 1.5), 2 (AC 2.6), 3 (AC 3.3), 4 (AC 4.2), 5 (AC 5.3), 6 (AC 6.5), 7 (AC 7.2)
> Design: Component 7

- [x] 6.1 In `crates/tokeira-edge/src/translate/history_serializer.rs`, add `failure_payload_to_proto(payload: &Payload) -> proto_failure::Failure` helper that checks for `temporal/failure+proto` encoding metadata, decodes via `proto_failure::Failure::decode`, and falls back to UTF-8 message on failure
- [x] 6.2 Update the `WorkflowExecutionFailed` arm to use `failure_payload_to_proto` on the `failure: Payload` field
- [x] 6.3 Update the `ActivityTaskFailed` arm to use `failure_payload_to_proto` on the `failure: Payload` field
- [x] 6.4 Update the `ChildWorkflowExecutionFailed` arm to use `failure_payload_to_proto` on the `failure: Payload` field
- [x] 6.5 Update the `WorkflowTaskFailed` arm to use `failure_details.as_ref().map(failure_payload_to_proto)` instead of constructing an empty `Failure`
- [x] 6.6 Update the `NexusOperationFailed` arm to use `failure_payload_to_proto` on the `failure: Payload` field
- [x] 6.7 Update the `WorkflowExecutionUpdateRejected` arm to use `failure_payload_to_proto` on the `failure: Payload` field
- [x] 6.8 Update the `MarkerRecorded` arm to use `failure.as_ref().map(failure_payload_to_proto)` instead of constructing an empty `Failure`

## Checkpoint: History serializer — verify `cargo build -p tokeira-edge` compiles

## Task 7: Fix downstream compilation — Update all pattern matches and test generators
> Requirements: All
> Design: Component 8

- [x] 7.1 In `crates/tokeira-kernel/tests/property_tests.rs`, add `arb_failure_payload()` proptest generator that creates a proto `Failure` with random message/source/stack_trace and encodes it via proto `encode_to_vec` into a `Payload` with `temporal/failure+proto` metadata
- [x] 7.2 Update all proptest generators in `property_tests.rs` that construct `WorkflowExecutionFailed`, `ActivityResolution::Failed`, `ChildResolution::Failed`, `NexusResolution::Failed`, and `UpdateProtocolBody::Rejected` to use `arb_failure_payload()`
- [x] 7.3 Update all proptest assertions in `property_tests.rs` that match on the modified variants to destructure `failure: Payload` instead of `message: String` / `failure: String`
- [x] 7.4 In `crates/tokeira-edge/src/translate/history_serializer.rs` `mod tests`, update `arb_history_event_kind()` to generate `failure: Payload` fields using a helper that encodes a proto `Failure` as a `Payload`
- [x] 7.5 Fix all remaining pattern matches across the workspace that reference the old field names (`message`, `details`, `failure_message`) on the modified variants
- [x] 7.6 Fix all construction sites of `RespondActivityTaskFailedRequest` in test files to use the new `failure: Payload` field

## Checkpoint: Full workspace build — verify `cargo lint` passes

## Task 8: Property-based tests — failure_to_payload / payload_to_failure round-trip
> Requirements: 8 (AC 8.1)
> Design: Property 1

- [x] 8.1 [PBT] In `crates/tokeira-edge/src/grpc/translate.rs` tests (or a new test module), add `arb_proto_failure()` proptest generator that creates proto `Failure` objects with random `message`, `source`, `stack_trace`, `encoded_attributes`, `cause` chains (depth 0-3), and `failure_info` variants (ApplicationFailureInfo, TimeoutFailureInfo, CanceledFailureInfo)
- [x] 8.2 [PBT] Write property test `prop_failure_payload_round_trip`: for any proto `Failure`, `payload_to_failure(&failure_to_payload(&failure))` re-encodes to identical bytes as the original (Property 1)

## Task 9: Property-based tests — History serializer failure round-trips
> Requirements: 8 (AC 8.2, 8.3, 8.4, 8.5)
> Design: Properties 2, 3, 4, 5, 6

- [x] 9.1 [PBT] In `crates/tokeira-edge/src/translate/history_serializer.rs` tests, write property test `prop_workflow_execution_failed_preserves_failure`: generate `WorkflowExecutionFailed` events with full Opaque_Failure_Payload, serialize to proto, assert the proto `failure` field's `failure_info` matches the original when the original had one (Property 2)
- [x] 9.2 [PBT] Write property test `prop_activity_task_failed_preserves_failure`: same pattern for `ActivityTaskFailed` events (Property 3)
- [x] 9.3 [PBT] Write property test `prop_child_workflow_failed_preserves_failure`: same pattern for `ChildWorkflowExecutionFailed` events (Property 4)
- [x] 9.4 [PBT] Write property test `prop_workflow_task_failed_preserves_failure`: generate `WorkflowTaskFailed` events with `failure_details: Some(Payload)`, serialize to proto, assert the proto `failure` field is `Some` and contains the original `failure_info` (Property 5)
- [x] 9.5 [PBT] Write property test `prop_all_failure_events_preserve_failure_info`: generate events across all failure-bearing kinds with non-None `failure_info`, serialize to proto, assert `failure_info` is preserved (Property 6)

## Checkpoint: All property tests — verify `cargo test -p tokeira-edge -p tokeira-kernel` passes

## Task 10: Unit tests — Golden examples and regression tests
> Requirements: All
> Design: Testing Strategy

- [x] 10.1 Write unit test in `history_serializer.rs`: `WorkflowExecutionFailed` with `ApplicationFailureInfo` produces proto with `failure_info` populated and `message`, `stack_trace`, `encoded_attributes` preserved
- [x] 10.2 Write unit test in `history_serializer.rs`: `ActivityTaskFailed` with `ApplicationFailureInfo` and a `cause` chain produces proto with both `failure_info` and `cause` populated
- [x] 10.3 Write unit test in `history_serializer.rs`: `WorkflowTaskFailed` with `failure_details` produces proto with non-empty `Failure` (regression for current bug where an empty Failure is constructed)
- [x] 10.4 Write unit test in `history_serializer.rs`: `MarkerRecorded` with failure produces proto with non-empty `Failure` (regression for current bug)
- [x] 10.5 Write unit test in `grpc/translate.rs`: `proto_command_to_workflow_command` for `FailWorkflow` produces `Payload` with `temporal/failure+proto` encoding metadata
- [x] 10.6 Write unit test in `grpc/translate.rs`: `respond_activity_failed_to_edge` produces `Payload` with `temporal/failure+proto` encoding metadata, extracts `failure_error_type` from `ApplicationFailureInfo.type` (not `Failure.source`), and sets `is_non_retryable` from `ApplicationFailureInfo.non_retryable`
- [x] 10.7 Write unit test: corrupted `Payload` data (not valid proto) produces a `Failure` with the raw bytes interpreted as UTF-8 message

## Checkpoint: Final — verify `cargo lint` and `cargo test` pass across the workspace
