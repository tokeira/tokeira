# Implementation Plan

## Overview

Fix the edge history serializer to emit authoritative data for all classified proto fields instead of defaulting to zero/empty values. Implementation is phased by risk and dependency: serializer-only fixes first, then pause encoding, kernel enrichment, WFT ID threading, runtime context, serializer wiring, and finally verification.

## Tasks

- [x] 1. Write bug condition exploration test
  - **Property 1: Bug Condition** — Serializer Emits Default Where Authoritative Data Exists
  - **IMPORTANT**: Write this property-based test BEFORE implementing the fix
  - **GOAL**: Surface counterexamples that demonstrate the serializer emits zero/empty values where the kernel event carries authoritative data
  - **Scoped PBT Approach**: Scope the property to concrete failing cases across all four bug classes:
    - Class 1 (serializer-only): Construct `NexusOperationCompleted { operation_id: "op-123", .. }`, serialize, assert proto `operation_id == "op-123"` (currently empty — `_` binding discards it)
    - Class 1 (serializer-only): Construct `WorkflowExecutionContinuedAsNew { workflow_execution_timeout: Some(1h), retry_policy: Some(..), .. }`, serialize, assert proto fields populated (currently defaulted — `_` bindings)
    - Pause encoding: Construct `WorkflowExecutionPaused { .. }`, serialize, assert event_type is NOT `EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED` (currently maps to canceled attributes)
    - Class 2 (kernel enrichment, post-enrichment): Construct enriched events with `workflow_task_completed_event_id > 0`, serialize, assert proto field matches (will fail until kernel enrichment is done)
    - Start metadata: construct `WorkflowExecutionStarted` with enriched identity/header, serialize, assert proto identity/header match (requirement 2.1)
    - WFT completion metadata: construct `WorkflowTaskCompleted` with `sdk_metadata` and `worker_version`, serialize, assert both are populated (requirement 2.10)
    - Activity completion identity: construct `ActivityTaskCompleted` with worker identity, serialize, assert proto identity matches (requirement 2.14)
    - Activity failure retry state: construct `ActivityTaskFailed` with worker identity and retry state, serialize, assert proto identity and retry_state match (requirement 2.15)
    - Child terminal metadata: construct a terminal child event with namespace and child run ID, serialize, assert proto namespace/run ID match (requirement 2.24)
    - External signal metadata: construct an external signal result with namespace and target run ID, serialize, assert proto namespace/run ID match (requirement 2.25)
    - Update accepted sequencing: construct `WorkflowExecutionUpdateAccepted` with `accepted_request_sequencing_event_id`, serialize, assert proto field matches (requirement 2.27)
    - Command-produced event ID: construct `ActivityTaskScheduled` with `workflow_task_completed_event_id`, serialize, assert proto field matches (requirement 2.12)
  - Run test on UNFIXED code — expect FAILURE (confirms the bug exists)
  - Document counterexamples: proto fields are zero/empty where kernel data exists; `_` bindings discard carried values; pause maps to canceled
  - Mark task complete when test is written, run, and failure is documented
  - _Requirements: 1.1, 1.2, 1.7, 1.38, 1.43, 2.1, 2.7, 2.10, 2.12, 2.14, 2.15, 2.24, 2.25, 2.26, 2.27, 2.29_

- [x] 2. Write preservation property tests (BEFORE implementing fix)
  - **Property 2: Preservation** — Existing Correct Serialization Unchanged
  - **IMPORTANT**: Follow observation-first methodology
  - Observe: serialize existing `HistoryEventKind` variants on unfixed code and record all currently-correct field values
  - Observe: `workflow_type`, `task_queue`, `input`, `result`, `failure`, `scheduled_event_id`, `started_event_id`, `activity_id`, `activity_type`, `timer_id`, `start_to_fire_timeout` all produce correct proto values today
  - Observe: `serialize_history()` produces valid protobuf bytes decodable as `temporal.api.history.v1.History`
  - Observe: deprecated v0.4 wire-compat fields (`ContinuedAsNew.failure`, `SignalExternal.control`, `NexusStarted.operation_id`) are populated
  - Write property-based test using existing `arb_history_event_kind()` strategy: for all event kinds, all fields listed in requirements 3.1–3.11 produce values matching the current serializer output
  - Write property: for any event, `serialize_history` produces bytes decodable as `History` without error
  - Verify tests pass on UNFIXED code
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11_

- [ ] 3. Phase 1 — Serializer-only fixes (wire through existing kernel data)

  - [x] 3.1 Wire `operation_id` on Nexus terminal events
    - In `history_serializer.rs`, replace `operation_id: _` bindings with `operation_id` on `NexusOperationCompleted`, `NexusOperationFailed`, `NexusOperationCanceled`, `NexusOperationTimedOut`
    - Populate proto `operation_id` field from the destructured value
    - _Bug_Condition: isBugCondition(event, field) where event is NexusOperation{Completed,Failed,Canceled,TimedOut} and field is operation_id_
    - _Expected_Behavior: proto operation_id == kernel operation_id_
    - _Preservation: All other Nexus fields (endpoint, service, operation, input, schedule_to_close_timeout, scheduled_event_id, result, failure) unchanged_
    - _Requirements: 2.26, 3.8_

  - [ ] 3.2 Wire `workflow_execution_timeout` and `retry_policy` on ContinuedAsNew
    - In `history_serializer.rs`, replace `workflow_execution_timeout: _` and `retry_policy: _` bindings with named bindings on `WorkflowExecutionContinuedAsNew`
    - Populate proto `workflow_execution_timeout` and `retry_policy` fields
    - _Bug_Condition: isBugCondition(event, field) where event is ContinuedAsNew and field is workflow_execution_timeout or retry_policy_
    - _Expected_Behavior: proto fields match kernel values when present_
    - _Preservation: All other ContinuedAsNew fields (workflow_type, task_queue, input, new_execution_run_id, failure) unchanged_
    - _Requirements: 2.7, 3.1_

  - [x] 3.3 Confirm `target_run_id` on SignalExternal and RequestCancelExternal initiated events
    - Verify `SignalExternalWorkflowExecutionInitiated` and `RequestCancelExternalWorkflowExecutionInitiated` already wire `target_run_id` correctly
    - If not wired, add the binding and populate proto field
    - _Requirements: 2.25, 3.7_

- [x] 4. Phase 2 — Pause/unpause encoding fix (MarkerRecorded instead of WorkflowExecutionCanceled)

  - [x] 4.1 Replace pause/unpause placeholder encoding with MarkerRecorded
    - In `history_serializer.rs`, change `WorkflowExecutionPaused` mapping from `WorkflowExecutionCanceledEventAttributes` to `MarkerRecordedEventAttributes` with `marker_name: "tokeira:paused"`
    - Change `WorkflowExecutionUnpaused` mapping to `MarkerRecordedEventAttributes` with `marker_name: "tokeira:unpaused"`
    - Update `event_type_for_kind` in `history_serializer.rs` to return `E::MarkerRecorded` for `WorkflowExecutionPaused` and `WorkflowExecutionUnpaused`
    - Keep the event type and attributes consistent: pause/unpause MUST NOT produce `event_type: Unspecified` with marker attributes
    - Encode identity and reason as marker details payload
    - _Bug_Condition: isBugCondition(event, field) where event is Paused/Unpaused and encoding uses CanceledEventAttributes_
    - _Expected_Behavior: event_type is MarkerRecorded, marker_name is "tokeira:paused"/"tokeira:unpaused", details contain identity+reason_
    - _Preservation: No other event types affected; SDKs skip unknown markers gracefully_
    - _Requirements: 2.29, 3.1_

  - [x] 4.2 Add unit tests for pause/unpause marker encoding
    - Test `WorkflowExecutionPaused` serializes to `MarkerRecordedEventAttributes` with correct marker_name
    - Test `WorkflowExecutionUnpaused` serializes to `MarkerRecordedEventAttributes` with correct marker_name
    - Test details payload contains identity and reason
    - Test event_type is NOT `EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED`
    - _Requirements: 2.29_

- [x] 5. Phase 3 — Kernel event enrichment (add fields to HistoryEventKind variants)

  - [x] 5.1 Add enrichment fields to workflow execution events in `event.rs`
    - Add `identity: String` and `header: Option<Headers>` to `WorkflowExecutionStarted`
    - Add `header: Option<Headers>` to `StartRequest` in `crates/tokeira-kernel/src/command.rs`
    - Thread start header from gRPC request -> `to_internal::start_request` -> `StartRequest.header` -> emitted `WorkflowExecutionStarted { header, ... }` event
    - Add `details: Option<Payloads>` to `WorkflowExecutionCanceled`
    - Add `new_execution_run_id: Option<RunId>` to `WorkflowExecutionTimedOut`
    - Add `identity: String` and `external_initiated_event_id: i64` to `WorkflowExecutionCancelRequested`
    - Add `header: Option<Headers>` to `WorkflowExecutionSignaled`
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.1, 2.4, 2.5, 2.6, 2.8_

  - [x] 5.2 Add enrichment fields to workflow task events in `event.rs`
    - Add `request_id: String` to `WorkflowTaskStarted`
    - Add `sdk_metadata: Option<Vec<u8>>` and `worker_version: Option<String>` to `WorkflowTaskCompleted`
    - Add `sdk_metadata: Option<Vec<u8>>` and `worker_version: Option<String>` to the edge DTO and kernel `WorkflowTaskCompletedRequest`
    - Read `sdk_metadata` from `RespondWorkflowTaskCompletedRequest.sdk_metadata` (`temporal.api.sdk.v1.WorkflowTaskCompletedMetadata`) and store it as raw proto bytes using `prost::Message::encode_to_vec`
    - Read `worker_version` from `RespondWorkflowTaskCompletedRequest.worker_version_stamp.build_id`
    - Thread `sdk_metadata` and worker version through `respond_completed_request_to_edge` -> `workflow_task_completed_request` -> `WorkflowTaskCompletedRequest` -> kernel event
    - Leave `WorkflowTaskFailed.worker_version` intentionally defaulted; the current WFT-failed path has no worker version metadata source
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.9, 2.10_

  - [x] 5.3 Add enrichment fields to activity task events in `event.rs`
    - Add `request_id: String` and `last_failure: Option<Payload>` to `ActivityTaskStarted`
    - Add `identity: WorkerIdentity` to `ActivityTaskCompleted`
    - Add `identity: WorkerIdentity` and `retry_state: RetryState` to `ActivityTaskFailed`
    - Add `retry_state: RetryState` to `ActivityTaskTimedOut`
    - Add `identity: WorkerIdentity` to `ActivityTaskCanceled`
    - Add source-data plumbing for activity-start metadata:
      extend the activity start request/path to carry a generated `request_id`, and persist previous-attempt failure on `ActivityState` when retrying so `last_failure` can be stamped on the next `ActivityTaskStarted`
    - Add `identity: Option<WorkerIdentity>` to the internal complete/fail activity request path so worker identity reaches the kernel:
      `RespondActivityTaskCompletedRequest`/`RespondActivityTaskFailedRequest` translation -> `WorkflowServiceRuntime` trait -> `RuntimeAdapter` -> `TokeiraRuntime::{complete,fail}_activity_task` -> `ActivityResolvedRequest`
    - Preserve `req.identity` in the edge translation layer instead of discarding it before runtime submission
    - Update `ActivityResolvedRequest` to carry `identity: Option<WorkerIdentity>` and have the kernel stamp it onto activity terminal events when present
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.13, 2.14, 2.15, 2.16, 2.17_

  - [x] 5.4 Add enrichment fields to child workflow events in `event.rs`
    - Extend `WorkflowCommand::StartChildWorkflow` and `StartChildWorkflowExecutionCommandAttributes` translation to carry SDK command attributes currently dropped by edge translation: header, memo, search attributes, workflow execution/run/task timeouts, retry policy, cron schedule, and human-readable namespace name when available
    - Add `namespace: Option<String>`, `header: Option<Headers>`, `memo: Memo`, `search_attributes: SearchAttributes`, `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `workflow_task_timeout: Duration`, `retry_policy: Option<RetryPolicy>`, `cron_schedule: Option<String>` to `StartChildWorkflowExecutionInitiated`
    - Add `workflow_type: WorkflowType` to `ChildWorkflowState` and populate it when the child-start command creates the child state
    - Persist `child_run_id` in `ChildWorkflowState` when `ChildWorkflowExecutionStarted` is processed, then reuse it when emitting terminal child events
    - Add `initiated_event_id: i64`, `namespace: Option<String>`, `workflow_type: WorkflowType` to `StartChildWorkflowExecutionFailed`
    - Add `namespace: Option<String>`, `child_run_id: RunId` to `ChildWorkflowExecutionCompleted`
    - Add `namespace: Option<String>`, `child_run_id: RunId`, `retry_state: RetryState` to `ChildWorkflowExecutionFailed`
    - Add `namespace: Option<String>`, `child_run_id: RunId`, `workflow_type: WorkflowType`, `details: Option<Payloads>` to `ChildWorkflowExecutionCanceled`
    - Add `namespace: Option<String>`, `workflow_type: WorkflowType` to `ChildWorkflowExecutionTerminated`
    - Add `namespace: Option<String>`, `workflow_type: WorkflowType`, `retry_state: RetryState` to `ChildWorkflowExecutionTimedOut`
    - In child start confirmation/failure and terminal child resolution paths, read `namespace`, `workflow_type`, and `child_run_id` from `ChildWorkflowState` rather than attempting serializer lookup
    - Preserve the namespace source contract: only populate human-readable `namespace` from explicitly threaded namespace names; keep `namespace_id` for UUID identity and do not stringify `NamespaceId` into `namespace`
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.22, 2.24_

  - [x] 5.5 Add enrichment fields to external signal/cancel events in `event.rs`
    - Extend external signal/cancel command translation to preserve human-readable target namespace names when available, in addition to existing `NamespaceId`
    - Add `namespace: Option<String>`, `header: Option<Headers>` to `SignalExternalWorkflowExecutionInitiated`
    - Add `namespace: Option<String>` to `RequestCancelExternalWorkflowExecutionInitiated`
    - Add `target_namespace: Option<String>` to `PendingExternalSignal` and `PendingExternalCancel` in `state.rs`
    - Populate pending external target namespace from explicitly threaded namespace name when available; otherwise keep it `None`
    - Add `namespace: Option<String>`, `target_run_id: Option<RunId>` to `ExternalWorkflowExecutionSignaled`
    - Add `namespace: Option<String>`, `target_run_id: Option<RunId>` to `SignalExternalWorkflowExecutionFailed`
    - Add `namespace: Option<String>`, `target_run_id: Option<RunId>` to `ExternalWorkflowExecutionCancelRequested`
    - Add `namespace: Option<String>`, `target_run_id: Option<RunId>` to `RequestCancelExternalWorkflowExecutionFailed`
    - In external signal/cancel resolution paths, read `target_namespace` and `target_run_id` from pending state and include them on result/failure events
    - Preserve the namespace source contract: leave human-readable `namespace` empty when only `NamespaceId` is available
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.25_

  - [x] 5.6 Add enrichment fields to Nexus and update events in `event.rs`
    - Add `nexus_header: Option<Headers>` and `endpoint_id: String` to `NexusOperationScheduled`
    - Add `accepted_request_sequencing_event_id: i64` to `WorkflowExecutionUpdateAccepted`
    - Add `accepted_event_id: i64` to `WorkflowExecutionUpdateCompleted`
    - Add `rejected_request_message_id: String` and `rejected_request_sequencing_event_id: i64` to `WorkflowExecutionUpdateRejected`
    - Update all constructors, test fixtures, and `Default` impls
    - _Requirements: 2.27_

  - [x] 5.7 Update kernel `apply_commands` and test fixtures for new fields
    - Ensure all kernel methods that construct enriched events populate the new fields from their source data
    - Ensure activity complete/fail worker identity is threaded from edge/runtime request DTOs into `ActivityResolvedRequest` before kernel application
    - Ensure child workflow state stores `workflow_type` and `child_run_id` before terminal child events need them
    - Ensure pending external signal/cancel state stores `target_namespace` before resolution events need it
    - Update all test helpers and golden test fixtures in `crates/tokeira-kernel/tests/` to include new fields
    - Ensure `cargo test -p tokeira-kernel` passes
    - _Requirements: 2.1, 2.4, 2.5, 2.6, 2.8, 2.9, 2.10, 2.13, 2.14, 2.15, 2.16, 2.17, 2.22, 2.24, 2.25, 2.27_

- [x] 6. Phase 4 — `workflow_task_completed_event_id` threading

  - [x] 6.1 Add `workflow_task_completed_event_id: i64` to affected HistoryEventKind variants
    - Add the field to: `WorkflowExecutionCompleted`, `WorkflowExecutionFailed`, `WorkflowExecutionCanceled`, `WorkflowExecutionContinuedAsNew`, `ActivityTaskScheduled`, `ActivityTaskCancelRequested`, `ActivityTaskCanceled`, `TimerStarted`, `TimerCanceled`, `MarkerRecorded`, `StartChildWorkflowExecutionInitiated`, `SignalExternalWorkflowExecutionInitiated`, `RequestCancelExternalWorkflowExecutionInitiated`, `NexusOperationScheduled`
    - Update all constructors and `Default` impls
    - _Requirements: 2.2, 2.3, 2.6, 2.7, 2.12, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22_

  - [x] 6.2 Capture and thread `wft_completed_event_id` inside kernel WFT completion processing
    - In `apply_workflow_task_completed`, capture the return value of `builder.emit(HistoryEventKind::WorkflowTaskCompleted { .. })`
    - Pass the captured ID into `apply_workflow_command` (or equivalent per-command processing helper)
    - Stamp that ID into each command-produced event's `workflow_task_completed_event_id` field
    - The kernel stays pure — the ID is assigned internally by `TransitionBuilder::emit` and no lane input is required
    - _Bug_Condition: isBugCondition(event, field) where field is workflow_task_completed_event_id and event is command-produced_
    - _Expected_Behavior: proto workflow_task_completed_event_id == event ID of the `WorkflowTaskCompleted` event from the same transition_
    - _Requirements: 2.2, 2.3, 2.6, 2.7, 2.12, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22_

  - [x] 6.3 Update all kernel test fixtures for `workflow_task_completed_event_id`
    - Update golden tests and property tests that construct events to include the new field
    - Ensure `cargo test -p tokeira-kernel` passes
    - _Requirements: 2.2, 2.3, 2.6, 2.7, 2.12_

- [ ] 7. Phase 5 — Runtime context enrichment

  - [ ] 7.1 Add request metadata and history-size hints to WorkflowTaskStarted
    - Add `request_id: String`, `history_size_bytes: i64`, and `suggest_continue_as_new: bool` fields to `WorkflowTaskStarted` in `event.rs`
    - Add `request_id: String`, `history_size_bytes: i64`, and `suggest_continue_as_new: bool` to `StartWorkflowTaskRequest`
    - In `crates/tokeira-runtime/src/runtime.rs`, generate a UUID v4 request ID in the `start_polled_workflow_task` path when constructing `Command::WorkflowTaskStarted(StartWorkflowTaskRequest { ... })`
    - Runtime computes `history_size_bytes` from the run's accumulated history size before submitting `Command::WorkflowTaskStarted`
    - Runtime derives `suggest_continue_as_new` from a history_size_bytes threshold and passes it through `StartWorkflowTaskRequest`
    - Kernel stamps all three values onto the `WorkflowTaskStarted` event from `StartWorkflowTaskRequest`
    - _Requirements: 2.9_

  - [x] 7.2 Add `scheduled_event_id` resolution for ActivityTaskCancelRequested
    - Add `scheduled_event_id: i64` to `ActivityTaskCancelRequested` in `event.rs`
    - Kernel resolves `activity_id -> scheduled_event_id` from `WorkflowState.activities` while processing the cancel-request command
    - Kernel stamps `scheduled_event_id` when producing the cancel-requested event
    - _Bug_Condition: isBugCondition(event, field) where event is ActivityTaskCancelRequested and field is scheduled_event_id_
    - _Expected_Behavior: proto scheduled_event_id == resolved scheduled event ID for the activity_
    - _Requirements: 2.18_

  - [x] 7.3 Add `started_event_id` resolution for TimerCanceled
    - Add `started_event_id: i64` to `TimerCanceled` in `event.rs`
    - Kernel resolves `timer_id -> started_event_id` from `WorkflowState.timers` while processing the cancel-timer command
    - Kernel stamps `started_event_id` when producing the timer-canceled event
    - _Bug_Condition: isBugCondition(event, field) where event is TimerCanceled and field is started_event_id_
    - _Expected_Behavior: proto started_event_id == resolved started event ID for the timer_
    - _Requirements: 2.20_

- [x] 8. Phase 6 — Wire all enriched fields through the serializer

  - [x] 8.1 Wire workflow execution enrichment fields in serializer
    - Wire `identity` and `header` on `WorkflowExecutionStarted`
    - Wire `details` on `WorkflowExecutionCanceled`
    - Wire `new_execution_run_id` on `WorkflowExecutionTimedOut`
    - Wire `identity` and `external_initiated_event_id` on `WorkflowExecutionCancelRequested`
    - Wire `header` on `WorkflowExecutionSignaled`
    - _Requirements: 2.1, 2.4, 2.5, 2.6, 2.8_

  - [x] 8.2 Wire workflow task enrichment fields in serializer
    - Wire `request_id` on `WorkflowTaskStarted`
    - Wire `history_size_bytes` and `suggest_continue_as_new` on `WorkflowTaskStarted`
    - Wire `sdk_metadata` and `worker_version` on `WorkflowTaskCompleted`; decode stored raw metadata bytes back into `WorkflowTaskCompletedMetadata` before assigning the proto field
    - Leave `WorkflowTaskFailed.worker_version` default in this spec
    - _Requirements: 2.9, 2.10_

  - [x] 8.3 Wire activity task enrichment fields in serializer
    - Wire `request_id` and `last_failure` on `ActivityTaskStarted`
    - Wire `identity` on `ActivityTaskCompleted`
    - Wire `identity` and `retry_state` on `ActivityTaskFailed`
    - Wire `retry_state` on `ActivityTaskTimedOut`
    - Wire `identity` on `ActivityTaskCanceled`
    - Wire `scheduled_event_id` on `ActivityTaskCancelRequested`
    - _Requirements: 2.13, 2.14, 2.15, 2.16, 2.17, 2.18_

  - [x] 8.4 Wire child workflow enrichment fields in serializer
    - Wire all new fields on `StartChildWorkflowExecutionInitiated` (namespace, header, memo, search_attributes, timeouts, retry_policy, cron_schedule)
    - Leave `ChildWorkflowExecutionStarted.header` default in this spec; the runtime child-start-confirmed path does not currently echo the child's authored header back to the parent
    - Wire `initiated_event_id`, `namespace`, `workflow_type` on `StartChildWorkflowExecutionFailed`
    - Wire `namespace`, `child_run_id` on `ChildWorkflowExecutionCompleted`
    - Wire `namespace`, `child_run_id`, `retry_state` on `ChildWorkflowExecutionFailed`
    - Wire `namespace`, `child_run_id`, `workflow_type`, `details` on `ChildWorkflowExecutionCanceled`
    - Wire `namespace`, `workflow_type` on `ChildWorkflowExecutionTerminated`
    - Wire `namespace`, `workflow_type`, `retry_state` on `ChildWorkflowExecutionTimedOut`
    - _Requirements: 2.22, 2.24_

  - [x] 8.5 Wire external signal/cancel enrichment fields in serializer
    - Wire `namespace`, `header` on `SignalExternalWorkflowExecutionInitiated`
    - Wire `namespace` on `RequestCancelExternalWorkflowExecutionInitiated`
    - Wire `namespace`, `target_run_id` on `ExternalWorkflowExecutionSignaled`
    - Wire `namespace`, `target_run_id` on `SignalExternalWorkflowExecutionFailed`
    - Wire `namespace`, `target_run_id` on `ExternalWorkflowExecutionCancelRequested`
    - Wire `namespace`, `target_run_id` on `RequestCancelExternalWorkflowExecutionFailed`
    - _Requirements: 2.25_

  - [x] 8.6 Wire Nexus, update, and `workflow_task_completed_event_id` fields in serializer
    - Wire `nexus_header` and `endpoint_id` on `NexusOperationScheduled`
    - Leave `NexusOperationStarted.operation_token` deferred until `tokeira_proto` exposes the v1.62 field; keep the existing `operation_id` mapping unchanged
    - Wire `accepted_request_sequencing_event_id` on `WorkflowExecutionUpdateAccepted`
    - Wire `accepted_event_id` on `WorkflowExecutionUpdateCompleted`
    - Wire `rejected_request_message_id` and `rejected_request_sequencing_event_id` on `WorkflowExecutionUpdateRejected`
    - Wire `workflow_task_completed_event_id` on all ~15 affected events
    - Wire `started_event_id` on `TimerCanceled`
    - _Requirements: 2.2, 2.3, 2.6, 2.7, 2.12, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22, 2.26, 2.27_

- [x] 9. Phase 7 — Verify fix and preservation

  - [x] 9.1 Verify bug condition exploration test now passes
    - **Property 1: Expected Behavior** — Serializer Emits Authoritative Data
    - **IMPORTANT**: Re-run the SAME test from task 1 — do NOT write a new test
    - The test from task 1 encodes the expected behavior for all classified fields
    - When this test passes, it confirms the expected behavior is satisfied across all four implementation classes
    - Run bug condition exploration test from step 1
    - **EXPECTED OUTCOME**: Test PASSES (confirms bug is fixed)
    - Deferred fields (Class 4) are NOT verified here; they are tracked in their owning specs
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.12, 2.13, 2.14, 2.15, 2.16, 2.17, 2.18, 2.19, 2.20, 2.21, 2.22, 2.24, 2.25, 2.26, 2.27, 2.29_

  - [x] 9.2 Verify preservation tests still pass
    - **Property 2: Preservation** — Existing Correct Serialization Unchanged
    - **IMPORTANT**: Re-run the SAME tests from task 2 — do NOT write new tests
    - Run preservation property tests from step 2
    - **EXPECTED OUTCOME**: Tests PASS (confirms no regressions)
    - Confirm all fields in requirements 3.1–3.11 produce identical output
    - Confirm `serialize_history()` still produces valid decodable proto bytes
    - Confirm deprecated v0.4 wire-compat fields still populated

  - [x] 9.3 Extend `arb_history_event_kind()` proptest strategy for new fields
    - Update the arbitrary event generator to include all new enrichment fields
    - Add property: for any event with `workflow_task_completed_event_id > 0`, proto field matches
    - Add property: for any Nexus terminal event, proto `operation_id` matches kernel `operation_id`
    - Add property: for any pause/unpause event, proto event_type is MarkerRecorded
    - _Requirements: 2.2, 2.26, 2.29, 3.10_

- [ ] 10. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-kernel` — all kernel tests pass with new fields
  - Run `cargo test -p tokeira-edge` — all serializer tests pass
  - Run `cargo test -p tokeira-runtime` — runtime context and edge/runtime identity threading tests pass
  - Run `cargo test --workspace` — full workspace green
  - Ensure all property-based tests pass (exploration + preservation)
  - Ask the user if questions arise


## Task Dependency Graph

```json
{
  "waves": [
    { "tasks": ["1", "2"], "description": "Write exploration and preservation tests BEFORE fix" },
    { "tasks": ["3"], "description": "Phase 1 — Serializer-only fixes (lowest risk)" },
    { "tasks": ["4"], "description": "Phase 2 — Pause/unpause encoding fix" },
    { "tasks": ["5"], "description": "Phase 3 — Kernel event enrichment" },
    { "tasks": ["6"], "description": "Phase 4 — workflow_task_completed_event_id threading" },
    { "tasks": ["7"], "description": "Phase 5 — Runtime context enrichment" },
    { "tasks": ["8"], "description": "Phase 6 — Wire all enriched fields through serializer" },
    { "tasks": ["9"], "description": "Phase 7 — Verify fix and preservation" },
    { "tasks": ["10"], "description": "Checkpoint — all tests pass" }
  ]
}
```

- Tasks 1 and 2 are independent and run BEFORE any fix implementation
- Tasks 3–8 are sequential phases ordered by implementation risk
- Task 9 re-runs the tests from tasks 1 and 2 to verify the fix
- Task 10 is the final checkpoint ensuring all workspace tests pass

## Notes

- Class 4 (deferred proto-sync) fields are tracked in the design but NOT implemented in this task list unless a concrete task above names the field explicitly.
- Intentionally-defaulted fields (documented in design) are NOT bugs and are not addressed here.
- The kernel stays pure — `workflow_task_completed_event_id` is captured inside WFT completion processing, values already present in `WorkflowState` are resolved inside the kernel, and runtime-only context is passed through command/request DTOs before `kernel.apply`.
- The serializer remains a pure projector — no lookup, no state, just reads fields from events.
