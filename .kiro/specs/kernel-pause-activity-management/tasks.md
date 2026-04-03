# Implementation Plan: Kernel Pause/Unpause and Activity Management (Feature 11)

## Overview

Add six new top-level commands to `tokeira-kernel` in two groups: workflow pause/unpause (PauseWorkflow, UnpauseWorkflow) and activity management (UpdateActivityOptions, PauseActivity, UnpauseActivity, ResetActivity). Types first (`ExecutionStatus::Paused` in `tokeira-types`, `PauseInfo`/`ActivityPauseInfo` in `state.rs`, six request structs and six `Command` variants in `command.rs`, four `Reject` variants, two `HistoryEventKind` variants), then kernel logic (six `apply_*` handlers, `schedule_workflow_task` paused guard, WFT re-dispatch suppression in `apply_workflow_task_failed`/`apply_workflow_task_timed_out`, Update rejection in `apply_update`), then downstream fixes (exhaustive matches, construction sites for `WorkflowState`/`ActivityState`), workspace compile checkpoint, then tests.

## Tasks

- [x] 1. Add new types and enum variants
  - [x] 1.1 Add `ExecutionStatus::Paused` variant and update `is_open()` in `tokeira-types`
    - Add `Paused` variant to `ExecutionStatus` enum in `tokeira/crates/tokeira-types/src/execution.rs`
    - Update `is_open()` to `matches!(self, Self::Running | Self::Paused)`
    - _Requirements: 11.1.1, 11.1.2, 11.1.3_

  - [x] 1.2 Add `PauseInfo` and `ActivityPauseInfo` structs, extend `WorkflowState` and `ActivityState` in `state.rs`
    - Add `PauseInfo { pause_time: OffsetDateTime, identity: String, reason: String, request_id: String }` deriving `Clone, Debug, PartialEq`
    - Add `ActivityPauseInfo { pause_time: OffsetDateTime, identity: String, reason: String }` deriving `Clone, Debug, PartialEq`
    - Add `pause_info: Option<PauseInfo>` and `wft_stamp: u64` fields to `WorkflowState`
    - Add `pause_info: Option<ActivityPauseInfo>` and `stamp: u64` fields to `ActivityState`
    - _Requirements: 11.2.1, 11.2.2, 11.2.5, 11.7.1, 11.7.2, 11.7.3_

  - [x] 1.3 Add six new request structs and six `Command` variants in `command.rs`
    - `PauseWorkflowRequest { identity: String, reason: String, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - `UnpauseWorkflowRequest { identity: String, reason: String, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - `UpdateActivityOptionsRequest { activity_id: String, task_queue: FieldChange<TaskQueueName>, schedule_to_close_timeout: FieldChange<Option<Duration>>, schedule_to_start_timeout: FieldChange<Option<Duration>>, start_to_close_timeout: FieldChange<Option<Duration>>, heartbeat_timeout: FieldChange<Option<Duration>>, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - `PauseActivityRequest { activity_id: String, identity: String, reason: String, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - `UnpauseActivityRequest { activity_id: String, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - `ResetActivityRequest { activity_id: String, reset_heartbeat: bool, request: RequestContext, now: OffsetDateTime }` with `Clone, Debug, PartialEq`
    - Add six `Command` variants: `PauseWorkflow(PauseWorkflowRequest)`, `UnpauseWorkflow(UnpauseWorkflowRequest)`, `UpdateActivityOptions(UpdateActivityOptionsRequest)`, `PauseActivity(PauseActivityRequest)`, `UnpauseActivity(UnpauseActivityRequest)`, `ResetActivity(ResetActivityRequest)`
    - _Requirements: 11.13.1–11.13.7_

  - [x] 1.4 Add four `Reject` variants in `kernel.rs`
    - `WorkflowPaused` with `#[error("workflow is paused")]`
    - `AlreadyPaused` with `#[error("workflow is already paused")]`
    - `NotPaused` with `#[error("workflow is not paused")]`
    - `ActivityNotPaused(String)` with `#[error("activity is not paused: {0}")]`
    - _Requirements: 11.12.1–11.12.4_

  - [x] 1.5 Add two `HistoryEventKind` variants in `event.rs`
    - `WorkflowExecutionPaused { identity: String, reason: String, request_id: String }`
    - `WorkflowExecutionUnpaused { identity: String, reason: String, request_id: String }`
    - _Requirements: 11.5.1, 11.5.2_


- [x] 2. Implement kernel logic
  - [x] 2.1 Add `TransitionBuilder::schedule_workflow_task` paused guard
    - Add `if self.state.status == ExecutionStatus::Paused { return; }` at the top of `schedule_workflow_task` in `kernel.rs`
    - This single check point covers all commands that call `schedule_workflow_task` — Signal, Cancel, ActivityResolved, TimerDue, ChildStartConfirmed, ChildResolved, ExternalSignalResolved, ExternalCancelResolved, NexusOperationResolved, WorkflowTaskCompleted (force_new_workflow_task), UnpauseWorkflow (sets Running before calling)
    - _Requirements: 11.1.4, 11.6.1, 11.6.2, 11.6.7, 11.6.8, 11.6.10, 11.6.14_

  - [x] 2.2 Add `apply_pause_workflow` method and `Command::PauseWorkflow` match arm
    - New match arm in `BasicKernel::apply`: `Command::PauseWorkflow(req) => self.apply_pause_workflow(loaded, req)`
    - `apply_pause_workflow`: `expect_open` → if `status == Paused` and `pause_info.request_id == req.request.request_id.0` → noop (TransitionBuilder::new + finish, no ops) → if `status == Paused` and different request_id → `Reject::AlreadyPaused` → emit `RequestDedupeOp` → emit `WorkflowExecutionPaused` event → set `status = Paused`, set `PauseInfo`, increment `wft_stamp` → bump stamps on all pending activities with `ActivityOp::Upsert` each → emit `ProjectionOp::UpsertExecution(Paused)` → NO WFT scheduled → `finish`
    - _Requirements: 11.3.1–11.3.12, 11.5.3, 11.15.1–11.15.3_

  - [x] 2.3 Add `apply_unpause_workflow` method and `Command::UnpauseWorkflow` match arm
    - New match arm: `Command::UnpauseWorkflow(req) => self.apply_unpause_workflow(loaded, req)`
    - `apply_unpause_workflow`: `expect_open` → if `status != Paused` → `Reject::NotPaused` → emit `RequestDedupeOp` → emit `WorkflowExecutionUnpaused` event → set `status = Running`, clear `pause_info` → bump stamps on all pending activities with `ActivityOp::Upsert` each → emit `DispatchOp::EnqueueActivityTask` for each activity → emit `ProjectionOp::UpsertExecution(Running)` → `schedule_workflow_task()` (status is now Running, so guard passes; existing `pending_workflow_task.is_some()` guard handles at-most-one-WFT) → `finish`
    - _Requirements: 11.4.1–11.4.11, 11.5.4_

  - [x] 2.4 Add `apply_update_activity_options` method and `Command::UpdateActivityOptions` match arm
    - New match arm: `Command::UpdateActivityOptions(req) => self.apply_update_activity_options(loaded, req)`
    - `apply_update_activity_options`: `expect_open` → lookup activity by `activity_id` (reject `UnknownActivity`) → emit `RequestDedupeOp` → apply `FieldChange` for each timeout and `task_queue` (Set → assign, Clear → None for optionals / no-op for task_queue, Unchanged → skip) → increment `stamp` → emit `ActivityOp::Upsert` → NO history events, NO WFT → `finish`
    - _Requirements: 11.8.1–11.8.9_

  - [x] 2.5 Add `apply_pause_activity` method and `Command::PauseActivity` match arm
    - New match arm: `Command::PauseActivity(req) => self.apply_pause_activity(loaded, req)`
    - `apply_pause_activity`: `expect_open` → lookup activity (reject `UnknownActivity`) → emit `RequestDedupeOp` → set `ActivityPauseInfo` on activity → increment `stamp` → emit `ActivityOp::Upsert` → NO history events, NO WFT → `finish`
    - _Requirements: 11.9.1–11.9.9_

  - [x] 2.6 Add `apply_unpause_activity` method and `Command::UnpauseActivity` match arm
    - New match arm: `Command::UnpauseActivity(req) => self.apply_unpause_activity(loaded, req)`
    - `apply_unpause_activity`: `expect_open` → lookup activity (reject `UnknownActivity`) → if `pause_info.is_none()` → `Reject::ActivityNotPaused` → emit `RequestDedupeOp` → clear `pause_info` → increment `stamp` → emit `ActivityOp::Upsert` → if `status != Paused` emit `DispatchOp::EnqueueActivityTask` (suppress when workflow paused) → NO history events, NO WFT → `finish`
    - _Requirements: 11.10.1–11.10.11_

  - [x] 2.7 Add `apply_reset_activity` method and `Command::ResetActivity` match arm
    - New match arm: `Command::ResetActivity(req) => self.apply_reset_activity(loaded, req)`
    - `apply_reset_activity`: `expect_open` → lookup activity (reject `UnknownActivity`) → emit `RequestDedupeOp` → set `attempt = 1` → `reset_heartbeat` accepted but no-op (ActivityState has no heartbeat_details yet) → increment `stamp` → emit `ActivityOp::Upsert` → if `status != Paused` emit `DispatchOp::EnqueueActivityTask` (suppress when workflow paused) → NO history events, NO WFT → `finish`
    - _Requirements: 11.11.1–11.11.11_

- [x] 3. Modify existing handlers
  - [x] 3.1 Add Update rejection for paused workflows in `apply_update`
    - After `expect_open`, before duplicate check: `if state.status == ExecutionStatus::Paused { return Err(Reject::WorkflowPaused); }`
    - _Requirements: 11.1.5, 11.6.5_

  - [x] 3.2 Add WFT re-dispatch suppression in `apply_workflow_task_failed`
    - Wrap the existing `builder.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask { ... })` in `if builder.state.status != ExecutionStatus::Paused { ... }`
    - _Requirements: 11.6.11_

  - [x] 3.3 Add WFT re-dispatch suppression in `apply_workflow_task_timed_out`
    - Wrap the existing `builder.dispatch_ops.push(DispatchOp::EnqueueWorkflowTask { ... })` in `if builder.state.status != ExecutionStatus::Paused { ... }`
    - _Requirements: 11.6.12_

  - [x] 3.4 Update `apply_start` WorkflowState initializer
    - Add `pause_info: None, wft_stamp: 0` to the `WorkflowState` initializer in `apply_start`
    - _Requirements: 11.2.4, 11.2.5_

  - [x] 3.5 Update `ScheduleActivity` in `apply_workflow_command` ActivityState initializer
    - Add `pause_info: None, stamp: 0` to the `ActivityState` initializer in the `ScheduleActivity` arm
    - _Requirements: 11.7.4_

  - [x] 3.6 Update `TransitionBuilder::close()` to clear `pause_info`
    - Add `self.state.pause_info = None;` to the `close()` method, alongside existing clears for `pending_workflow_task`, `sticky`, and pending entity maps
    - Ensures `pause_info` is `None` whenever `status != Paused` (Requirement 11.2.4)
    - _Requirements: 11.2.4_


- [x] 4. Fix downstream breakage
  - [x] 4.1 Update all exhaustive matches on `ExecutionStatus` across the workspace
    - Add `Paused` arm to every exhaustive match on `ExecutionStatus` (projection, serialization, display, test helpers)
    - _Requirements: 11.1.1_

  - [x] 4.2 Update all exhaustive matches on `Command` across the workspace
    - Add arms for all six new `Command` variants in every exhaustive match (serialization, display, test helpers)
    - _Requirements: 11.13.1–11.13.6_

  - [x] 4.3 Update all exhaustive matches on `Reject` across the workspace
    - Add arms for `WorkflowPaused`, `AlreadyPaused`, `NotPaused`, `ActivityNotPaused` in every exhaustive match
    - _Requirements: 11.12.1–11.12.4_

  - [x] 4.4 Update all exhaustive matches on `HistoryEventKind` across the workspace
    - Add arms for `WorkflowExecutionPaused` and `WorkflowExecutionUnpaused` in every exhaustive match
    - _Requirements: 11.5.1, 11.5.2_

  - [x] 4.5 Update all `WorkflowState` construction sites across the workspace
    - Add `pause_info: None, wft_stamp: 0` to every `WorkflowState` struct literal in tests and helpers
    - _Requirements: 11.2.4, 11.2.5_

  - [x] 4.6 Update all `ActivityState` construction sites across the workspace
    - Add `pause_info: None, stamp: 0` to every `ActivityState` struct literal in tests and helpers
    - _Requirements: 11.7.4_

- [x] 5. Checkpoint — workspace compilation
  - Run `cargo check --workspace` and ensure zero errors. Fix any additional compile failures discovered. Ask the user if questions arise.

- [x] 6. Add golden tests to `golden_tests.rs`
  - [x] 6.1 Add `pause_workflow_happy_path` test
    - PauseWorkflow against running state with 2 activities. Assert: `WorkflowExecutionPaused` event with correct identity/reason/request_id, status=Paused, pause_info=Some with correct fields, wft_stamp incremented, 2 ActivityOp::Upsert with stamp=1, ProjectionOp::UpsertExecution(Paused), one RequestDedupeOp, no EnqueueWorkflowTask.
    - _Requirements: 11.3.1–11.3.8, 11.5.3_

  - [x] 6.2 Add `pause_workflow_no_activities` test
    - PauseWorkflow against running state with 0 activities. Assert: same as 6.1 but 0 ActivityOp::Upsert.
    - _Requirements: 11.3.1–11.3.4, 11.3.6, 11.3.7, 11.3.8_

  - [x] 6.3 Add `pause_workflow_idempotent_same_request_id` test
    - PauseWorkflow against paused state with matching request_id. Assert: Ok, no events, no activity ops, no dispatch ops, no projection ops, next_state identical except transition_seq.
    - _Requirements: 11.3.9, 11.15.1–11.15.3_

  - [x] 6.4 Add `pause_workflow_rejects_different_request_id` test
    - PauseWorkflow against paused state with different request_id. Assert: `Reject::AlreadyPaused`.
    - _Requirements: 11.3.10_

  - [x] 6.5 Add `pause_workflow_rejects_absent_run` test
    - Assert: `Reject::MissingRun`.
    - _Requirements: 11.3.11_

  - [x] 6.6 Add `pause_workflow_rejects_closed_run` test
    - Assert: `Reject::RunClosed`.
    - _Requirements: 11.3.12_

  - [x] 6.7 Add `unpause_workflow_happy_path` test
    - UnpauseWorkflow against paused state with 2 activities and no pending WFT. Assert: `WorkflowExecutionUnpaused` event, `WorkflowTaskScheduled` event, status=Running, pause_info=None, 2 ActivityOp::Upsert with incremented stamps, 2 EnqueueActivityTask, 1 EnqueueWorkflowTask, ProjectionOp::UpsertExecution(Running), one RequestDedupeOp.
    - _Requirements: 11.4.1–11.4.8, 11.5.4_

  - [x] 6.8 Add `unpause_workflow_no_activities` test
    - UnpauseWorkflow against paused state with 0 activities and no pending WFT. Assert: same but 0 activity ops/dispatch, still gets WFT scheduled.
    - _Requirements: 11.4.1–11.4.4, 11.4.7, 11.4.8_

  - [x] 6.9 Add `unpause_workflow_rejects_running` test
    - UnpauseWorkflow against running state. Assert: `Reject::NotPaused`.
    - _Requirements: 11.4.9_

  - [x] 6.10 Add `unpause_workflow_rejects_absent_run` test
    - Assert: `Reject::MissingRun`.
    - _Requirements: 11.4.10_

  - [x] 6.11 Add `unpause_workflow_rejects_closed_run` test
    - Assert: `Reject::RunClosed`.
    - _Requirements: 11.4.11_

  - [x] 6.12 Add `signal_paused_workflow_no_wft` test
    - Signal against paused state with no pending WFT. Assert: `WorkflowExecutionSignaled` event emitted, no `WorkflowTaskScheduled`, no `EnqueueWorkflowTask`.
    - _Requirements: 11.1.4, 11.6.1_

  - [x] 6.13 Add `cancel_paused_workflow_no_wft` test
    - Cancel against paused state with no pending WFT. Assert: event emitted, no WFT.
    - _Requirements: 11.6.2_

  - [x] 6.14 Add `update_rejects_paused_workflow` test
    - Update against paused state. Assert: `Reject::WorkflowPaused`.
    - _Requirements: 11.1.5, 11.6.5_

  - [x] 6.15 Add `terminate_paused_workflow` test
    - Terminate against paused state. Assert: closes with Terminated (Paused is open).
    - _Requirements: 11.6.3_

  - [x] 6.16 Add `activity_resolved_paused_workflow_no_wft` test
    - ActivityResolved against paused state. Assert: resolution event emitted, activity removed, no WFT.
    - _Requirements: 11.6.7_

  - [x] 6.17 Add `wft_failed_paused_workflow_no_redispatch` test
    - WorkflowTaskFailed against paused state with started WFT. Assert: event emitted, started_event_id cleared, no EnqueueWorkflowTask dispatch.
    - _Requirements: 11.6.11_

  - [x] 6.18 Add `wft_timed_out_paused_workflow_no_redispatch` test
    - WorkflowTaskTimedOut against paused state with started WFT. Assert: event emitted, started_event_id cleared, no EnqueueWorkflowTask dispatch.
    - _Requirements: 11.6.12_

  - [x] 6.19 Add `wft_completed_paused_workflow_no_force_wft` test
    - WorkflowTaskCompleted with force_new_workflow_task=true against paused state. Assert: completion proceeds, no new WFT scheduled.
    - _Requirements: 11.6.10_

  - [x] 6.20 Add `update_activity_options_happy_path` test
    - UpdateActivityOptions with Set timeouts against open run. Assert: no events, stamp incremented, fields updated, one ActivityOp::Upsert, one RequestDedupeOp, no WFT.
    - _Requirements: 11.8.1–11.8.6_

  - [x] 6.21 Add `update_activity_options_unknown_activity` test
    - Assert: `Reject::UnknownActivity`.
    - _Requirements: 11.8.7_

  - [x] 6.22 Add `pause_activity_happy_path` test
    - PauseActivity against open run. Assert: pause_info set with correct fields, stamp incremented, one ActivityOp::Upsert, one RequestDedupeOp, no events, no WFT.
    - _Requirements: 11.9.1–11.9.6_

  - [x] 6.23 Add `pause_activity_unknown_activity` test
    - Assert: `Reject::UnknownActivity`.
    - _Requirements: 11.9.7_

  - [x] 6.24 Add `unpause_activity_happy_path` test
    - UnpauseActivity on paused activity in running workflow. Assert: pause_info cleared, stamp incremented, one ActivityOp::Upsert, one EnqueueActivityTask, one RequestDedupeOp, no events, no WFT.
    - _Requirements: 11.10.1–11.10.7_

  - [x] 6.25 Add `unpause_activity_not_paused` test
    - UnpauseActivity on non-paused activity. Assert: `Reject::ActivityNotPaused`.
    - _Requirements: 11.10.8_

  - [x] 6.26 Add `unpause_activity_unknown_activity` test
    - Assert: `Reject::UnknownActivity`.
    - _Requirements: 11.10.9_

  - [x] 6.27 Add `reset_activity_happy_path` test
    - ResetActivity against open run. Assert: attempt=1, stamp incremented, one ActivityOp::Upsert, one EnqueueActivityTask, one RequestDedupeOp, no events, no WFT.
    - _Requirements: 11.11.1–11.11.8_

  - [x] 6.28 Add `reset_activity_unknown_activity` test
    - Assert: `Reject::UnknownActivity`.
    - _Requirements: 11.11.9_


- [x] 7. Add property tests to `property_tests.rs`
  - [x] 7.1 Extend `arb_valid_pair` with all six new commands
    - Add `Command::PauseWorkflow` against Running state to `arb_valid_pair`
    - Add `Command::UnpauseWorkflow` against Paused state to `arb_valid_pair`
    - Add all four activity management commands against open state with activities to `arb_valid_pair`
    - Add helper strategies: `arb_pause_workflow_request(now)`, `arb_unpause_workflow_request(now)`, `arb_update_activity_options_request(activity_id, now)`, `arb_pause_activity_request(activity_id, now)`, `arb_unpause_activity_request(activity_id, now)`, `arb_reset_activity_request(activity_id, now)`, `arb_running_state_with_activities(now, n)`, `arb_paused_state_with_activities(now, n)`, `arb_activity_management_command(state, now)`
    - Ensures existing structural property tests (event ID contiguity, transition_seq increment, at-most-one-WFT, activity op consistency) automatically cover all six new commands
    - _Requirements: 11.14.1–11.14.5_

  - [x] 7.2 Add Property 1 test: PauseWorkflow produces correct state and event
    - `proptest!` block: generate random running state with N activities, apply PauseWorkflow, assert: status=Paused, pause_info=Some with correct fields, wft_stamp incremented, exactly one WorkflowExecutionPaused event, one RequestDedupeOp, one ProjectionOp::UpsertExecution(Paused), N ActivityOp::Upsert with stamp+1, no EnqueueWorkflowTask
    - **Property 1: PauseWorkflow produces correct state and event**
    - **Validates: Requirements 11.3.1–11.3.8, 11.5.3, 11.14.1, 11.14.6, 11.14.8**

  - [x] 7.3 Add Property 2 test: UnpauseWorkflow produces correct state, events, and dispatch ops
    - `proptest!` block: generate random paused state with N activities (with and without pending WFT), apply UnpauseWorkflow, assert: status=Running, pause_info=None, one WorkflowExecutionUnpaused event, conditional WorkflowTaskScheduled event (only if no pending WFT), one RequestDedupeOp, one ProjectionOp::UpsertExecution(Running), N ActivityOp::Upsert with incremented stamps, N EnqueueActivityTask
    - **Property 2: UnpauseWorkflow produces correct state, events, and dispatch ops**
    - **Validates: Requirements 11.4.1–11.4.8, 11.5.4, 11.14.1, 11.14.7**

  - [x] 7.4 Add Property 3 test: PauseWorkflow idempotency
    - `proptest!` block: generate random paused state, apply PauseWorkflow with same request_id → assert Ok with no events/ops/projection (only seq bump); apply with different request_id → assert Err(AlreadyPaused)
    - **Property 3: PauseWorkflow idempotency**
    - **Validates: Requirements 11.3.9, 11.3.10, 11.15.1–11.15.3**

  - [x] 7.5 Add Property 4 test: WFT scheduling suppression for paused workflows
    - `proptest!` block: generate random paused state with pending entities, apply WFT-triggering commands (Signal, Cancel, ActivityResolved, TimerDue), assert: no EnqueueWorkflowTask dispatch, no WorkflowTaskScheduled event, primary event still emitted
    - **Property 4: WFT scheduling suppression for paused workflows**
    - **Validates: Requirements 11.1.4, 11.6.1, 11.6.2, 11.6.7, 11.6.8, 11.6.14**

  - [x] 7.6 Add Property 5 test: WFT re-dispatch suppression for paused workflows
    - `proptest!` block: generate random paused state with started WFT, apply WorkflowTaskFailed and WorkflowTaskTimedOut, assert: no EnqueueWorkflowTask dispatch, event still emitted, started_event_id cleared
    - **Property 5: WFT re-dispatch suppression for paused workflows**
    - **Validates: Requirements 11.6.11, 11.6.12**

  - [x] 7.7 Add Property 6 test: Activity management commands emit no history events and no WFT
    - `proptest!` block: generate random open state with activity, apply random activity management command (UpdateActivityOptions, PauseActivity, UnpauseActivity, ResetActivity), assert: zero history events, no EnqueueWorkflowTask, one RequestDedupeOp, transition_seq incremented
    - **Property 6: Activity management commands emit no history events and no WFT**
    - **Validates: Requirements 11.8.5, 11.8.6, 11.9.5, 11.9.6, 11.10.6, 11.10.7, 11.11.7, 11.11.8, 11.14.3, 11.14.4**

  - [x] 7.8 Add Property 7 test: Activity management commands produce correct ActivityOp::Upsert with incremented stamp
    - `proptest!` block: generate random open state with activity at stamp S, apply random activity management command, assert: one ActivityOp::Upsert with stamp S+1, corresponding entry in next_state.activities
    - **Property 7: Activity management commands produce correct ActivityOp::Upsert with incremented stamp**
    - **Validates: Requirements 11.8.3, 11.8.4, 11.9.3, 11.9.4, 11.10.3, 11.10.4, 11.11.4, 11.11.5, 11.14.5**

  - [x] 7.9 Add Property 8 test: UpdateActivityOptions mutates specified fields correctly
    - `proptest!` block: generate random open state with activity, apply UpdateActivityOptions with random FieldChange values, assert: Set → field equals value, Clear → optional timeout is None, Unchanged → field unchanged
    - **Property 8: UpdateActivityOptions mutates specified fields correctly**
    - **Validates: Requirements 11.8.2**

  - [x] 7.10 Add Property 9 test: PauseActivity sets ActivityPauseInfo correctly
    - `proptest!` block: generate random open state with activity, apply PauseActivity, assert: pause_info=Some with pause_time=req.now, identity=req.identity, reason=req.reason
    - **Property 9: PauseActivity sets ActivityPauseInfo correctly**
    - **Validates: Requirements 11.9.2**

  - [x] 7.11 Add Property 10 test: UnpauseActivity clears pause_info and re-dispatches
    - `proptest!` block: generate random open state with paused activity, apply UnpauseActivity, assert: pause_info=None; if workflow Running → one EnqueueActivityTask; if workflow Paused → no EnqueueActivityTask
    - **Property 10: UnpauseActivity clears pause_info and re-dispatches**
    - **Validates: Requirements 11.10.2, 11.10.5**

  - [x] 7.12 Add Property 11 test: UnpauseActivity rejects non-paused activity
    - `proptest!` block: generate random open state with non-paused activity, apply UnpauseActivity, assert: Err(Reject::ActivityNotPaused)
    - **Property 11: UnpauseActivity rejects non-paused activity**
    - **Validates: Requirements 11.10.8**

  - [x] 7.13 Add Property 12 test: ResetActivity resets attempt and re-dispatches
    - `proptest!` block: generate random open state with activity at arbitrary attempt, apply ResetActivity, assert: attempt=1; if workflow Running → one EnqueueActivityTask with attempt=1; if workflow Paused → no EnqueueActivityTask
    - **Property 12: ResetActivity resets attempt and re-dispatches**
    - **Validates: Requirements 11.11.2, 11.11.6**

  - [x] 7.14 Add Property 13 test: ScheduleActivity initializes new ActivityState fields
    - `proptest!` block: generate random ScheduleActivity in WFT completion, assert: resulting ActivityState has stamp=0 and pause_info=None
    - **Property 13: ScheduleActivity initializes new ActivityState fields**
    - **Validates: Requirements 11.7.4**

  - [x] 7.15 Add Property 14 test: UnpauseWorkflow rejects non-paused workflows
    - `proptest!` block: generate random running state, apply UnpauseWorkflow, assert: Err(Reject::NotPaused)
    - **Property 14: UnpauseWorkflow rejects non-paused workflows**
    - **Validates: Requirements 11.4.9**

- [x] 8. Update architecture and crate reference documentation
  - [x] 8.1 Update `docs/architecture/020-kernel.md`
    - Add PauseWorkflow and UnpauseWorkflow behavioral specifications to command taxonomy section
    - Add UpdateActivityOptions, PauseActivity, UnpauseActivity, ResetActivity behavioral specifications
    - Update command taxonomy table with all six new commands (origin: Operator/External, open-run requirement, request dedup)
    - Update WorkflowState section with `pause_info: Option<PauseInfo>`, `wft_stamp: u64`, and extended ActivityState fields (`pause_info`, `stamp`)
    - Document Paused status interaction with existing commands (WFT lifecycle not rejected, Updates rejected, WFT scheduling suppressed)
    - _Requirements: 11.16.1–11.16.5, 11.16.7_

  - [x] 8.2 Update `docs/crates/kernel.md`
    - Update implementation status table to reflect Feature 11 complete
    - Update command taxonomy tables with six new commands
    - Update reject taxonomy with four new variants
    - Update Temporal feature coverage section
    - _Requirements: 11.16.6, 11.16.7_

- [x] 9. Final checkpoint — all tests pass
  - Run `cargo test --workspace`. Ensure all tests pass, ask the user if questions arise.

## Notes

- All tests are required (none marked optional) per user direction
- Property tests use `proptest! { }` block style, golden tests are individual `#[test]` functions
- Tests extend existing `golden_tests.rs` and `property_tests.rs` — no new test files
- Property 15 (structural invariants) is covered by extending `arb_valid_pair` in task 7.1 — existing structural property tests automatically cover all six new commands
- `ExecutionStatus::Paused` lives in `tokeira-types`, not `tokeira-kernel` — task 1.1 modifies the types crate
- `FieldChange<T>` already exists in `command.rs` from Feature 8 — reused for `UpdateActivityOptionsRequest`
- `ResetActivity` heartbeat clearing is a no-op — accepted for API compatibility but ActivityState has no `heartbeat_details` field yet
- UnpauseActivity and ResetActivity suppress `DispatchOp::EnqueueActivityTask` when workflow is paused — dispatch deferred to UnpauseWorkflow
- UnpauseWorkflow WFT scheduling is conditional on `pending_workflow_task.is_none()` via the existing guard in `schedule_workflow_task`
- British spelling: `Cancelled` (not `Canceled`)
