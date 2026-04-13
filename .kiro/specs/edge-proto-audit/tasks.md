# Implementation Plan: Edge Proto Audit

## Overview

Fix the edge/proto translation layer so every upstream Temporal API proto field is faithfully translated through the system. The work proceeds bottom-up: first thread missing data through kernel and runtime structs, then fix the command translator, then the history serializer, then response builders, then add long-poll support. Property tests validate the four correctness properties from the design.

## Tasks

- [x] 1. Thread all activity-related data through kernel and runtime structs
  - [x] 1.1 Add `activity_type: String` and `header: Option<Headers>` fields to `WorkflowCommand::ScheduleActivity` in `tokeira-kernel/src/command.rs`
    - Update all match arms and struct literals across the kernel that reference `ScheduleActivity`
    - Update the kernel's command-to-event mapping in `kernel.rs` to thread `activity_type` and `header` into the `ActivityTaskScheduled` event
    - _Requirements: 2.1, 2.2_
  - [x] 1.2 Add `activity_type: String`, `header: Option<Headers>`, and `retry_policy: Option<RetryPolicy>` fields to `HistoryEventKind::ActivityTaskScheduled` in `tokeira-kernel/src/event.rs`
    - Update all match arms and struct literals across the kernel that reference `ActivityTaskScheduled`
    - _Requirements: 2.1, 2.2_
  - [x] 1.3 Add all missing fields to `StartedActivityTask` in `tokeira-runtime/src/runtime.rs`
    - Add `activity_type: String`, `workflow_id: String`, `workflow_type: String`, `workflow_namespace: String`, `header: Option<Headers>`, `retry_policy: Option<RetryPolicy>`
    - Update all sites in the runtime that construct `StartedActivityTask` to populate every new field from kernel event data and run metadata
    - _Requirements: 2.2, 2.3_
  - [x] 1.4 Ensure all existing kernel tests (221) and runtime tests (162) still pass after struct changes
    - Run `cargo test -p tokeira-kernel` and `cargo test -p tokeira-runtime`
    - Fix any compilation errors from new fields in test fixtures and golden files
    - _Requirements: 2.1, 2.2, 2.3_

- [x] 2. Checkpoint — Verify kernel and runtime are green
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Fix command translator (`grpc/translate.rs`)
  - [x] 3.1 Fix `ScheduleActivity` field extraction in `proto_command_to_workflow_command`
    - Extract `activity_type` from `attrs.activity_type.map(|at| at.name)`
    - Extract all four timeouts from proto duration fields instead of hardcoding `None`
    - Extract `retry_policy` from proto instead of hardcoding `None`
    - _Requirements: 1.1, 1.3_
  - [x] 3.2 Fix `ScheduleActivity` field population in `workflow_command_to_proto`
    - Populate `activity_type`, all four timeouts, and `retry_policy` on the proto command attributes
    - _Requirements: 1.3_
  - [x] 3.3 Add missing command type translations in `proto_command_to_workflow_command`
    - Add match arms for: `CancelTimerCommandAttributes`, `RequestCancelActivityTaskCommandAttributes`, `ContinueAsNewWorkflowExecutionCommandAttributes`, `StartChildWorkflowExecutionCommandAttributes`, `SignalExternalWorkflowExecutionCommandAttributes`, `RequestCancelExternalWorkflowExecutionCommandAttributes`, `CancelWorkflowExecutionCommandAttributes`, `RecordMarkerCommandAttributes`, `ProtocolMessageCommandAttributes`, `ScheduleNexusOperationCommandAttributes`, `RequestCancelNexusOperationCommandAttributes`
    - Each arm extracts all proto fields and maps to the corresponding `WorkflowCommand` variant
    - Replace the catch-all `_ =>` with an explicit unsupported error for truly unknown command types
    - _Requirements: 1.3_
  - [x] 3.4 Add reverse translations in `workflow_command_to_proto` for the 11 command types currently returning errors
    - Implement proto construction for `CancelTimer`, `RequestCancelActivity`, `ContinueAsNew`, `StartChildWorkflow`, `SignalExternalWorkflowExecution`, `RequestCancelExternalWorkflowExecution`, `CancelWorkflow`, `RecordMarker`, `ProtocolMessage`, `ScheduleNexusOperation`, `CancelNexusOperation`
    - _Requirements: 1.3_
  - [ ]* 3.5 Write property test for command translation round-trip
    - **Property 1: Command translation round-trip**
    - Generate random proto commands for each supported type, translate to `WorkflowCommand` and back, assert field equality
    - **Validates: Requirements 1.3**
  - [ ]* 3.6 Write golden-example unit tests for each newly added command translation
    - One test per new command type with known input/output
    - _Requirements: 1.3_

- [x] 4. Checkpoint — Verify command translator is green
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Fix history serializer (`history_serializer.rs`)
  - [x] 5.1 Map `activity_type` in `ActivityTaskScheduled` serialization
    - Populate `activity_type` field on `ActivityTaskScheduledEventAttributes` from the new kernel field
    - _Requirements: 1.4_
  - [x] 5.2 Fix `_` patterns in activity event serialization
    - `ActivityTaskCompleted`: map `activity_id` (currently `_`) — no direct proto field for `activity_id` on completed, but ensure `scheduled_event_id` is populated if available
    - `ActivityTaskFailed`: map `activity_id` similarly
    - `ActivityTaskTimedOut`: map `activity_id` and `timeout_type` (currently both `_`)
    - `ActivityTaskCanceled`: map `activity_id` (currently `_`)
    - `ActivityTaskCancelRequested`: map `activity_id` (currently `_`) to populate `scheduled_event_id` if trackable
    - _Requirements: 1.4_
  - [x] 5.3 Fix `TimerStarted` serialization to compute `start_to_fire_timeout`
    - Compute `start_to_fire_timeout` as `fire_at - event.happened_at` and populate the proto field (currently `fire_at` is ignored with `_`)
    - _Requirements: 1.4_
  - [x] 5.4 Fix `_` patterns in update event serialization
    - `WorkflowExecutionUpdateAccepted`: map `update_name` and `input` (currently `_`) to construct `accepted_request` with `update.v1.Request`
    - `WorkflowExecutionUpdateCompleted`: map `update_id` and `result` (currently both `_`) to populate `meta` and `outcome`
    - _Requirements: 1.4_
  - [x] 5.5 Fix `_` patterns in remaining event serializations
    - `WorkflowExecutionFailed`: map `details` (currently `_`) to `failure.details`
    - `StartChildWorkflowExecutionFailed`: map `cause` (currently `_`) to `cause` enum field
    - `WorkflowExecutionOptionsUpdated`: map `versioning_override`, `completion_callbacks`, `attached_request_id` (all currently `_`)
    - `NexusOperationStarted`: map `operation_id` (currently not in proto but verify)
    - _Requirements: 1.4_
  - [x] 5.6 Update `arb_history_event_kind` proptest generator to populate ALL fields
    - Currently some fields are hardcoded to `None`/default (e.g. `ActivityTaskScheduled` timeouts, `NexusOperationScheduled` timeout)
    - Add generators for the new `activity_type` field
    - Ensure all variants with optional fields generate non-None values
    - _Requirements: 1.4_
  - [ ]* 5.7 Write property test for history serialization field completeness
    - **Property 2: History serialization field completeness**
    - For any kernel `HistoryEvent` with all fields populated, assert serialized proto has non-default values for every mapped field
    - **Validates: Requirements 1.4**
  - [ ]* 5.8 Write golden-example unit tests for each fixed `_` pattern
    - Verify previously-dropped fields are now populated in proto output
    - _Requirements: 1.4_

- [x] 6. Checkpoint — Verify history serializer is green
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Fix response field population
  - [x] 7.1 Update edge DTO `PollActivityTaskQueueResponse` in `translate/mod.rs`
    - Add missing fields: `workflow_type`, `workflow_namespace`, `header`, `retry_policy`
    - _Requirements: 1.2, 2.3_
  - [x] 7.2 Update `poll_activity_response` in `from_internal.rs` to populate all fields from `StartedActivityTask`
    - Map `activity_type` and `workflow_id` from the now-populated `StartedActivityTask` fields (replacing `String::new()`)
    - Populate any additional fields added in 7.1
    - _Requirements: 1.2, 2.1, 2.3_
  - [x] 7.3 Update `poll_activity_response_to_proto` in `grpc/translate.rs` to populate all proto response fields
    - Map `workflow_type`, `workflow_namespace`, `header`, `retry_policy`, `started_time` to proto fields
    - _Requirements: 1.2_
  - [ ]* 7.4 Write property test for activity data threading end-to-end
    - **Property 4: Activity data threading end-to-end**
    - Generate random `ScheduleActivityTaskCommandAttributes`, flow through command translation → kernel event → `StartedActivityTask` → edge DTO → proto response, assert field preservation
    - **Validates: Requirements 2.1, 2.3**

- [x] 8. Checkpoint — Verify response population is green
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement long-poll for `GetWorkflowExecutionHistory`
  - [x] 9.1 Thread `next_page_token` through the edge DTO and translation layer
    - Add `next_page_token: Vec<u8>` to `GetWorkflowExecutionHistoryRequest` and `GetWorkflowExecutionHistoryResponse` edge DTOs
    - Update `get_history_request_to_edge` to pass through `next_page_token` from the proto request
    - Update `get_history_response_to_proto` to include `next_page_token` in the proto response (encode `last_event_id` as big-endian i64 bytes)
    - Update `workflow_service.rs` to decode the caller's `next_page_token` into a `last_event_id` cursor and use it to determine what counts as "new"
    - _Requirements: 3.1, 3.2_
  - [x] 9.2 Create a `HistoryWaitHandle` with `tokio::sync::watch` channel per run
    - Add a `DashMap<RunKey, watch::Sender<i64>>` to the edge service or runtime
    - Create/retrieve the watch channel lazily on first long-poll request
    - _Requirements: 3.1, 3.2_
  - [x] 9.3 Notify the watch channel when history events are committed
    - After each `commit_transition` that appends history events, send the latest `last_event_id` on the watch channel
    - _Requirements: 3.2_
  - [x] 9.4 Implement long-poll logic in `get_workflow_execution_history`
    - When `wait_new_event=true` and no matching events exist past the caller's cursor, subscribe to the watch channel and wait with `tokio::time::timeout(60s)`
    - On new event notification, re-read history and check for matching events
    - On timeout, return current history with updated `next_page_token`
    - When `wait_new_event=false`, return immediately (current behavior)
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x]* 9.5 Write unit tests for long-poll behavior
    - Test immediate return when `wait_new_event=false`
    - Test blocking + wake when event arrives
    - Test timeout behavior
    - Test that `next_page_token` correctly tracks caller position across calls
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 10. Checkpoint — Verify long-poll is green
  - Ensure all tests pass, ask the user if questions arise.

- [ ]* 11. Write property test for timestamp/duration conversion round-trip
  - **Property 3: Timestamp and Duration conversion round-trip**
  - Generate random `OffsetDateTime` and `time::Duration` values, convert to proto and back, assert equality within nanosecond precision
  - **Validates: Requirements 1.5**

- [x] 12. Full request/response audit beyond activity/history path
  - [x] 12.1 Audit and fix `workflow_execution_info_from_summary` in `grpc/translate.rs`
    - Currently hardcodes `history_length: 0`, `state_transition_count: 0`, and empty memo/search_attributes
    - Thread real values from `WorkflowExecutionSummary` (add fields to the edge DTO if missing)
    - _Requirements: 1.2_
  - [x] 12.2 Audit and fix `start_request_to_edge` for dropped fields
    - Compare every field in upstream `StartWorkflowExecutionRequest` against the edge DTO
    - Extract `workflow_execution_timeout`, `workflow_run_timeout`, `workflow_task_timeout`, `retry_policy`, `header` from proto
    - Document unsupported fields (cron_schedule, etc.) in UNSUPPORTED_FIELDS.md
    - _Requirements: 1.1_
  - [x] 12.3 Audit and fix `describe_response_to_proto` for dropped fields
    - Compare every field in upstream `DescribeWorkflowExecutionResponse` against what we return
    - Populate `execution_config`, `pending_activities`, `pending_children` where data is available
    - Document fields requiring new storage queries as known gaps
    - _Requirements: 1.2_
  - [x] 12.4 Audit remaining request/response translation functions
    - `signal_request_to_edge`, `poll_request_to_edge`, `respond_completed_request_to_edge`, `list_response_to_proto`, `count_response_to_proto`, `query_response_to_proto`, `update_response_to_proto`
    - For each: compare proto fields against edge DTO, classify gaps, fix or document
    - _Requirements: 1.1, 1.2_
  - [x] 12.5 Create `UNSUPPORTED_FIELDS.md` in `crates/tokeira-edge/`
    - Document all proto fields that Tokeira intentionally does not support, with rationale
    - Reference from translation code comments
    - _Requirements: 1.1_

- [x] 13. Final checkpoint — Full test suite
  - Run `cargo test` across the entire workspace
  - Run `cargo lint` to verify no clippy warnings
  - Ensure all 221 kernel tests, 162 runtime tests, and edge tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Add `ActivityTaskStarted` event to the kernel and wire through the full pipeline
  - [x] 14.1 Add `HistoryEventKind::ActivityTaskStarted` variant to `tokeira-kernel/src/event.rs`
    - Fields: `activity_id: String`, `scheduled_event_id: i64`, `attempt: u32`, `identity: WorkerIdentity`
    - Update all match arms across the kernel that pattern-match on `HistoryEventKind`
    - _Requirements: 1.4, 2.1_
  - [x] 14.2 Add `apply_activity_started` operation to the kernel (`tokeira-kernel/src/kernel.rs`)
    - Accept `activity_id`, `identity`, and `now`
    - Look up the activity in `state.activities`, emit `ActivityTaskStarted` with `scheduled_event_id` from `ActivityState.schedule_event_id` and `attempt` from `ActivityState.attempt`
    - Record the `started_event_id` back into `ActivityState` (add `started_event_id: Option<i64>` field to `ActivityState`)
    - Set `ActivityState.started_at` to `now`
    - _Requirements: 1.4, 2.1_
  - [x] 14.3 Add `started_event_id: Option<i64>` field to `ActivityState` in `tokeira-kernel/src/state.rs`
    - Update all struct literals that construct `ActivityState`
    - _Requirements: 2.1_
  - [x] 14.4 Add `scheduled_event_id: i64` and `started_event_id: i64` fields to `ActivityTaskCompleted`, `ActivityTaskFailed`, `ActivityTaskTimedOut`, `ActivityTaskCanceled` event variants
    - The kernel's `apply_activity_resolved` should read these from `ActivityState` and include them in the emitted events
    - Update all match arms across the kernel
    - _Requirements: 1.4, 2.1_
  - [x] 14.5 Call `apply_activity_started` from the runtime when dispatching an activity task
    - In `tokeira-runtime/src/runtime.rs`, when `poll_activity_task` returns a task to a worker, call the kernel to emit the `ActivityTaskStarted` event
    - Pass the worker identity from the poll request
    - This requires a new kernel method or extending the existing activity dispatch path
    - _Requirements: 2.1, 2.3_
  - [x] 14.6 Ensure all kernel and runtime tests pass after the changes
    - Run `cargo test -p tokeira-kernel` and `cargo test -p tokeira-runtime`
    - Fix compilation errors from new fields in test fixtures and golden files
    - _Requirements: 2.1_

- [x] 15. Checkpoint — Verify kernel ActivityTaskStarted is green
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Wire `ActivityTaskStarted` through the history serializer and populate event-ID linkage
  - [x] 16.1 Add `ActivityTaskStarted` serialization to `history_serializer.rs`
    - Map to `history::ActivityTaskStartedEventAttributes` with `scheduled_event_id`, `identity`, `attempt`
    - Add the event type mapping in `event_type_for_kind`
    - _Requirements: 1.4_
  - [x] 16.2 Populate `scheduled_event_id` and `started_event_id` on activity completion/failure/timeout/cancel events
    - Now that the kernel events carry these IDs (from task 14.4), map them to the proto fields
    - Remove the `let _ = activity_id` suppression comments
    - _Requirements: 1.4_
  - [x] 16.3 Update the proptest generator to include `ActivityTaskStarted` events
    - Add the new variant to `arb_history_event_kind`
    - _Requirements: 1.4_
  - [x] 16.4 Ensure all edge tests pass
    - Run `cargo test -p tokeira-edge`
    - _Requirements: 1.4_

- [x] 17. Checkpoint — Verify hello-world example works end-to-end
  - Start tokeirad, worker, and starter
  - The starter should print the workflow result
  - Ask the user to verify

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The kernel is pure and deterministic — changes in tasks 1.1–1.2 and 14.x must not introduce I/O
- Adding fields to enums/structs will cause compile errors in all match arms and struct literals — tasks 1.4 and 14.6 cover fixing all of these
- The history serializer has an existing proptest (`prop_history_serialization_round_trip`) — task 5.6 updates its generators
- Task 1.3 is the authoritative data-threading task — every field the edge layer needs in a response must have a corresponding field in `StartedActivityTask`
- Task 9.1 threads `next_page_token` so the long-poll handler knows what the caller has already seen
- Task 12 covers the full request/response audit beyond the activity/history path (list responses, describe, start, etc.)
- **Tasks 14–17 are the ActivityTaskStarted event group** — the SDK's activity state machine requires `Scheduled → Started → Completed` in history. Without `ActivityTaskStarted`, the SDK cannot replay activity completions. This is the root cause of the hello-world example failure.
- Task 14.5 is the most architecturally significant — it requires the runtime to call back into the kernel when dispatching an activity, creating a new interaction pattern
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design
