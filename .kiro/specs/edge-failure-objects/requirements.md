# Requirements Document: Edge Failure Object Completeness

## Introduction

This spec addresses the loss of structured failure information as proto `Failure` objects flow through the Tokeira system. The proto field audit (`../edge-complete-implementation/reference/proto-field-audit.md` §3) identified that every `proto_failure::Failure` construction in the history serializer uses `..Default::default()`, silently dropping `failure_info` variants, `cause` chains, `stack_trace`, `source`, and `encoded_attributes`. Without these fields, all failures appear as generic errors to the SDK — it cannot distinguish an application error from a timeout from a cancellation.

This is Feature 2 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features.

The key design insight: the edge layer already has `failure_to_payload` and `payload_to_failure` helpers that serialize the full proto `Failure` as an opaque `Payload` (with metadata encoding `temporal/failure+proto`). The fix is to carry the full serialized proto `Failure` bytes through the kernel as an opaque `Payload` rather than decomposing into `message: String`. The kernel stays pure — it treats the failure as an opaque blob. The history serializer deserializes it back to proto on the outbound path.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **History_Serializer**: The module `tokeira-edge/src/translate/history_serializer.rs` that converts kernel `HistoryEvent` values into proto `temporal.api.history.v1.History` messages.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Failure_Object**: The proto `temporal.api.failure.v1.Failure` message, which carries structured failure information including `failure_info` variants (ApplicationFailureInfo, TimeoutFailureInfo, CanceledFailureInfo, TerminatedFailureInfo, ServerFailureInfo, ResetWorkflowFailureInfo, ActivityFailureInfo, ChildWorkflowExecutionFailureInfo, NexusOperationFailureInfo), `cause` chains, `stack_trace`, `source`, and `encoded_attributes`.
- **Opaque_Failure_Payload**: A `tokeira_types::Payload` whose `data` field contains proto-encoded `Failure` bytes and whose `metadata` contains `encoding: temporal/failure+proto`. The kernel treats this as an opaque blob.
- **failure_to_payload**: The existing helper in `tokeira-edge/src/grpc/translate.rs` that serializes a proto `Failure` into an Opaque_Failure_Payload.
- **payload_to_failure**: The existing helper in `tokeira-edge/src/grpc/translate.rs` that deserializes an Opaque_Failure_Payload back into a proto `Failure`.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

### Requirement 1: Kernel failure model — opaque Payload for workflow execution failures

**User Story:** As an SDK user, I want `WorkflowExecutionFailed` events to carry the full proto `Failure` (including `failure_info`, `cause`, `stack_trace`, `encoded_attributes`), so that the SDK can distinguish failure types and present meaningful error information to workflow code.

#### Acceptance Criteria

1. THE `HistoryEventKind::WorkflowExecutionFailed` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `message: String` and `details: Option<Payload>` fields.
2. THE `WorkflowCommand::FailWorkflow` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `message: String` and `details: Option<Payload>` fields.
3. WHEN the Edge_Layer translates a `FailWorkflowExecutionCommandAttributes` proto command, THE Edge_Layer SHALL encode the entire `Failure` proto as an Opaque_Failure_Payload using the existing `failure_to_payload` helper.
4. WHEN the History_Serializer serializes a `WorkflowExecutionFailed` event, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper, producing a complete `Failure` with all original fields preserved.
5. WHEN the Opaque_Failure_Payload cannot be deserialized (corrupted or missing data), THE History_Serializer SHALL fall back to a `Failure` with the raw bytes interpreted as a UTF-8 message string.

### Requirement 2: Kernel failure model — opaque Payload for activity task failures

**User Story:** As an SDK user, I want `ActivityTaskFailed` events to carry the full proto `Failure` (including `failure_info`, `cause`, `stack_trace`), so that the SDK can present structured activity failure information to workflow code.

#### Acceptance Criteria

1. THE `HistoryEventKind::ActivityTaskFailed` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `message: String` field.
2. THE `ActivityResolution::Failed` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `message: String` field.
3. WHEN the Edge_Layer translates a `RespondActivityTaskFailedRequest` proto, THE Edge_Layer SHALL encode the entire `Failure` proto as an Opaque_Failure_Payload using the existing `failure_to_payload` helper.
4. THE `RespondActivityTaskFailedRequest` edge DTO SHALL carry a `failure: Payload` field, replacing the current `failure_message: String` and `failure_error_type: Option<String>` fields.
5. WHEN the Runtime calls `fail_activity_task`, THE Runtime SHALL pass the Opaque_Failure_Payload through to `ActivityResolution::Failed` without decomposing it.
6. WHEN the History_Serializer serializes an `ActivityTaskFailed` event, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper.

### Requirement 3: Kernel failure model — opaque Payload for child workflow execution failures

**User Story:** As an SDK user, I want `ChildWorkflowExecutionFailed` events to carry the full proto `Failure` (including `ChildWorkflowExecutionFailureInfo`, `cause`, `stack_trace`), so that the SDK can present structured child workflow failure information to workflow code.

#### Acceptance Criteria

1. THE `HistoryEventKind::ChildWorkflowExecutionFailed` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `failure: String` field.
2. THE `ChildResolution::Failed` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `failure: String` field.
3. WHEN the History_Serializer serializes a `ChildWorkflowExecutionFailed` event, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper.
4. WHEN the Runtime synthesizes a `ChildResolution::Failed` from a closed child run, THE Runtime SHALL read the child's `close_failure` from `WorkflowState` and construct an Opaque_Failure_Payload. If the child's `close_failure` is already an Opaque_Failure_Payload (i.e., the child's `WorkflowExecutionFailed` event carried full proto bytes), THE Runtime SHALL pass it through. If the child's `close_failure` is a bare string (legacy), THE Runtime SHALL wrap it in a proto `Failure { message: ... }` and encode via `failure_to_payload`.
5. TO SUPPORT criterion 4, `WorkflowState.close_failure` SHALL be changed from `Option<String>` to `Option<Payload>`, carrying the full Opaque_Failure_Payload from the `WorkflowExecutionFailed` event. This ensures the child resolution path has access to the complete failure data.

### Requirement 4: Kernel failure model — opaque Payload for workflow task failures

**User Story:** As an SDK user, I want `WorkflowTaskFailed` events to carry the full proto `Failure` (including `failure_info`, `stack_trace`), so that the SDK can present structured workflow task failure information.

#### Acceptance Criteria

1. THE `HistoryEventKind::WorkflowTaskFailed` variant SHALL continue to carry `failure_details: Option<Payload>`, which may contain proto-encoded `Failure` bytes from the inbound path.
2. WHEN the History_Serializer serializes a `WorkflowTaskFailed` event with `failure_details: Some(payload)`, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper, replacing the current empty `Failure` construction.
3. NOTE: The inbound `RespondWorkflowTaskFailed` gRPC handler is currently a logging stub that does not submit to the kernel. Full WFT failure transport is out of scope for this spec. This requirement only ensures the serializer correctly handles `failure_details` when present (e.g., from kernel-generated failures like reset or non-determinism detection).

### Requirement 5: Kernel failure model — opaque Payload for Nexus operation failures

**User Story:** As an SDK user, I want `NexusOperationFailed` events to carry a proto `Failure` object, so that the SDK can present structured Nexus failure information to workflow code.

#### Acceptance Criteria

1. THE `HistoryEventKind::NexusOperationFailed` variant SHALL carry a `failure: Payload` field containing proto-encoded `Failure` bytes, replacing the current `failure: String` field.
2. THE `NexusResolution::Failed` variant SHALL carry a `failure: Payload` field containing proto-encoded `Failure` bytes, replacing the current `failure: String` field.
3. WHEN the History_Serializer serializes a `NexusOperationFailed` event, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper.
4. NOTE: The current Nexus failure producers (`publisher.rs`) only have plain error strings — there is no upstream proto `Failure` to preserve. The Runtime SHALL construct a proto `Failure { message: error_string, failure_info: Some(ApplicationFailureInfo { type: "NexusOperationFailure", non_retryable: false }) }` and encode it via `failure_to_payload` at the Nexus publisher boundary. This is best-effort wrapping, not full preservation — full Nexus failure fidelity requires the Nexus task transport (Feature 8) to carry structured `HandlerError` responses.

### Requirement 6: Kernel failure model — opaque Payload for update rejections

**User Story:** As an SDK user, I want `WorkflowExecutionUpdateRejected` events to carry the full proto `Failure` (including `failure_info`), so that the SDK can present structured update rejection information.

#### Acceptance Criteria

1. THE `HistoryEventKind::WorkflowExecutionUpdateRejected` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `failure: String` field.
2. THE `UpdateProtocolBody::Rejected` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `failure: String` field.
3. THE `WorkflowCommand::UpdateRejected` variant SHALL carry a `failure: Payload` field containing the full proto-encoded `Failure` bytes, replacing the current `failure: String` field.
4. WHEN the Edge_Layer translates an update rejection protocol message, THE Edge_Layer SHALL encode the entire `Failure` proto as an Opaque_Failure_Payload.
5. WHEN the History_Serializer serializes a `WorkflowExecutionUpdateRejected` event, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper.

### Requirement 7: Marker failure round-trip

**User Story:** As an SDK user, I want `MarkerRecorded` events with failures to carry the full proto `Failure`, so that side-effect replay preserves complete failure information.

#### Acceptance Criteria

1. THE `MarkerRecorded` event already carries `failure: Option<Payload>` which is populated by the `RecordMarker` command using `failure_to_payload` on the inbound path.
2. WHEN the History_Serializer serializes a `MarkerRecorded` event with a failure, THE History_Serializer SHALL deserialize the Opaque_Failure_Payload back to a proto `Failure` using the existing `payload_to_failure` helper, replacing the current empty `Failure` construction.

### Requirement 8: Failure round-trip correctness

**User Story:** As a Tokeira developer, I want the failure serialization pipeline to be round-trip correct, so that no failure information is lost as it flows through the system.

#### Acceptance Criteria

1. FOR ANY proto `Failure` with all fields populated (message, source, stack_trace, encoded_attributes, cause chain, failure_info), encoding via `failure_to_payload` then decoding via `payload_to_failure` SHALL produce a `Failure` with all original fields preserved.
2. FOR ANY `WorkflowExecutionFailed` event with a full Opaque_Failure_Payload, the serialized proto `WorkflowExecutionFailedEventAttributes.failure` SHALL contain the complete `Failure` including `failure_info` and `encoded_attributes`.
3. FOR ANY `ActivityTaskFailed` event with a full Opaque_Failure_Payload, the serialized proto `ActivityTaskFailedEventAttributes.failure` SHALL contain the complete `Failure` including `failure_info`.
4. FOR ANY `ChildWorkflowExecutionFailed` event with a full Opaque_Failure_Payload, the serialized proto `ChildWorkflowExecutionFailedEventAttributes.failure` SHALL contain the complete `Failure` including `failure_info`.
5. FOR ANY `WorkflowTaskFailed` event with `failure_details`, the serialized proto `WorkflowTaskFailedEventAttributes.failure` SHALL contain the complete `Failure` including `failure_info`.

### Requirement 9: Activity retry classification from failure_info

**User Story:** As a Tokeira developer, I want activity retry decisions to use the correct failure classification from `ApplicationFailureInfo.type` and `ApplicationFailureInfo.non_retryable`, so that retry behavior matches Temporal's semantics.

#### Acceptance Criteria

1. WHEN the Runtime evaluates whether a failed activity should be retried, THE Runtime SHALL extract the error type from `ApplicationFailureInfo.type` inside the proto `Failure.failure_info`, NOT from `Failure.source`. The `source` field identifies the SDK/server origin (e.g., "GoSDK"), not the application error type.
2. WHEN the proto `Failure` has `ApplicationFailureInfo` with `non_retryable: true`, THE Runtime SHALL treat the failure as non-retryable regardless of the error type.
3. WHEN the proto `Failure` has `ServerFailureInfo` with `non_retryable: true`, THE Runtime SHALL treat the failure as non-retryable.
4. WHEN the proto `Failure` has `TerminatedFailureInfo` or `CanceledFailureInfo`, THE Runtime SHALL treat the failure as non-retryable.
5. WHEN the proto `Failure` has `TimeoutFailureInfo`, THE Runtime SHALL treat it as non-retryable unless the timeout type is `START_TO_CLOSE` or `HEARTBEAT` (which are retryable unless listed in `non_retryable_error_types` with the `Timeout:` prefix).
6. THE Edge_Layer SHALL extract the retry-relevant classification from the proto `Failure` before encoding it as an Opaque_Failure_Payload, and pass it alongside the payload to the Runtime. This avoids the Runtime needing to decode the proto `Failure` for retry decisions.
